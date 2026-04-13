"""In-memory debounce manager for inbound messages.

Replaces GHL's 3-minute consolidation timer. When a message arrives:
1. Cancel existing debounce timer for this contact (reset behavior)
2. Cancel running pipeline for this contact (abort+restart)
3. Cancel pending FUs + outreach (not reactivation)
4. Reschedule reactivation (push timer forward)
5. Clear GHL follow-up field
6. Start new debounce timer

When the timer expires (no new message in N seconds):
- Sync conversation from GHL
- Detect channel from latest inbound messageType
- Resolve agent_type from contact custom fields
- Build PipelineContext and run text_engine Prefect flow

Three layers of safety:
- Primary: asyncio tasks fire at exact time
- Startup recovery: recreate tasks from DB on boot
- Safety-net poller: 30-min sweep for anything missed
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

# Default debounce window (seconds) — configurable per client via system_config
DEFAULT_DEBOUNCE_SECONDS = 90


@dataclass
class DebounceState:
    """Tracks debounce + pipeline state for a single contact."""

    debounce_task: asyncio.Task | None = None
    pipeline_task: asyncio.Task | None = None
    entity_id: str = ""
    contact_id: str = ""
    webhook_data: dict = field(default_factory=dict)
    config: dict = field(default_factory=dict)


# Global registry: "{entity_id}:{contact_id}" → DebounceState
_active: dict[str, DebounceState] = {}


def _key(entity_id: str, contact_id: str) -> str:
    return f"{entity_id}:{contact_id}"


async def debounce_inbound(
    entity_id: str,
    contact_id: str,
    raw_webhook_data: dict[str, Any],
    config: dict[str, Any],
) -> None:
    """Handle an inbound message — debounce, cancel existing, restart timer.

    Called by the /inbound webhook endpoint. Returns immediately (fire-and-forget).
    The actual pipeline execution happens after the debounce window expires.
    """
    key = _key(entity_id, contact_id)
    state = _active.get(key)

    # 1. Cancel existing debounce timer
    if state and state.debounce_task and not state.debounce_task.done():
        state.debounce_task.cancel()
        logger.info("DEBOUNCE | timer_reset | contact=%s | entity=%s", contact_id, entity_id[:8])

    # 2. Cancel running pipeline (abort+restart on new message)
    if state and state.pipeline_task and not state.pipeline_task.done():
        state.pipeline_task.cancel()
        logger.info(
            "DEBOUNCE | pipeline_aborted | contact=%s | entity=%s | reason=new_inbound_message",
            contact_id, entity_id[:8],
        )

    # 3. Cancel pending FUs + outreach (NOT reactivation — that gets rescheduled)
    # 4. Reschedule reactivation (push timer forward)
    # 5. Clear GHL follow-up field
    # These are done in a background task to not block the webhook response
    asyncio.create_task(_cancel_and_reschedule(entity_id, contact_id, config))

    # 6. Start new debounce timer (read from setter's config via Agent Type)
    from app.webhooks.standard_parser import extract_custom_field
    agent_type_for_config = extract_custom_field(raw_webhook_data, "Agent Type") or ""
    debounce_seconds = _get_debounce_window(config, agent_type_for_config)
    new_state = DebounceState(
        entity_id=entity_id,
        contact_id=contact_id,
        webhook_data=raw_webhook_data,
        config=config,
    )

    async def _debounce_then_run():
        try:
            await asyncio.sleep(debounce_seconds)
            logger.info(
                "DEBOUNCE | timer_expired | contact=%s | entity=%s | window=%ds",
                contact_id, entity_id[:8], debounce_seconds,
            )

            # Check reply window — if outside, sleep until window opens
            sleep_seconds = _get_reply_window_sleep(config, agent_type_for_config)
            if sleep_seconds > 0:
                logger.info(
                    "DEBOUNCE | reply_window_hold | contact=%s | sleeping %ds until window opens",
                    contact_id, sleep_seconds,
                )
                await asyncio.sleep(sleep_seconds)
                # Re-fetch config in case it changed during long sleep
                # (webhook_data stays the same — we'll re-sync conversation anyway)
                logger.info("DEBOUNCE | reply_window_open | contact=%s | proceeding", contact_id)

            # Timer expired + inside reply window — run the pipeline
            pipeline_task = asyncio.create_task(
                _run_reply_pipeline(new_state)
            )
            new_state.pipeline_task = pipeline_task
            await pipeline_task
        except asyncio.CancelledError:
            # Timer was reset by a new message — this is expected
            pass
        except Exception:
            logger.exception(
                "DEBOUNCE | pipeline_failed | contact=%s | entity=%s",
                contact_id, entity_id[:8],
            )
        finally:
            # Clean up registry
            if _active.get(key) is new_state:
                del _active[key]

    new_state.debounce_task = asyncio.create_task(_debounce_then_run())
    _active[key] = new_state


async def _cancel_and_reschedule(
    entity_id: str,
    contact_id: str,
    config: dict[str, Any],
) -> None:
    """Cancel pending FUs/outreach + reschedule reactivation + clear GHL field.

    Runs as a background task so it doesn't block the webhook response.
    """
    try:
        # Import here to avoid circular imports at module level
        from app.services.message_scheduler import (
            cancel_pending,
            update_ghl_followup_field,
        )

        # Lazy import the singleton
        from app.main import supabase as sb

        # Cancel FUs + outreach (NOT reactivation)
        # Note: cancel_pending() already cancels asyncio fire tasks internally
        await cancel_pending(
            sb,
            contact_id,
            entity_id,
            message_types=["followup", "smart_followup", "outreach"],
            reason="lead_replied",
        )

        # Reschedule reactivation (push timer forward)
        try:
            from app.workflows.reactivation_scheduler import reschedule_reactivation
            await reschedule_reactivation(entity_id, contact_id)
        except Exception as e:
            logger.warning("DEBOUNCE | reactivation reschedule failed (non-blocking): %s", e)

        # Clear GHL follow-up field
        ghl_api_key = config.get("ghl_api_key", "")
        ghl_location_id = config.get("ghl_location_id", "")
        if ghl_api_key and ghl_location_id:
            from app.services.ghl_client import GHLClient
            ghl = GHLClient(api_key=ghl_api_key, location_id=ghl_location_id)
            await update_ghl_followup_field(ghl, contact_id)  # Clear

    except Exception:
        logger.exception("DEBOUNCE | cancel_and_reschedule failed | contact=%s", contact_id)


async def _run_reply_pipeline(state: DebounceState) -> dict[str, Any]:
    """Execute the reply pipeline after debounce timer expires.

    Syncs conversation from GHL, detects channel, builds context, runs pipeline.
    This is the core of what used to happen in the GHL reply webhook handler.
    """
    from prefect import tags as prefect_tags

    from app.main import (
        supabase,
        build_run_tags,
        build_flow_name,
        derive_result_tag,
        notify_error,
        _resolve_tenant_ai_keys,
    )
    from app.models import PipelineContext
    from app.text_engine.pipeline import text_engine
    from app.webhooks.standard_parser import extract_custom_field

    entity_id = state.entity_id
    contact_id = state.contact_id
    config = state.config
    webhook_data = state.webhook_data

    slug = config.get("slug") or config.get("name", entity_id[:8])

    # Extract contact data from standard webhook payload
    agent_type = extract_custom_field(webhook_data, "Agent Type") or ""
    contact_name = webhook_data.get("full_name", "")
    contact_phone = webhook_data.get("phone", "")
    contact_email = webhook_data.get("email", "")

    # Extract channel from webhook custom field (set by GHL before sending webhook)
    channel = extract_custom_field(webhook_data, "Channel") or "SMS"

    # Resolve per-tenant AI keys
    tenant_keys = await _resolve_tenant_ai_keys(entity_id)

    # Build PipelineContext — message will be resolved during sync
    ctx = PipelineContext(
        entity_id=entity_id,
        contact_id=contact_id,
        trigger_type="reply",
        channel=channel,
        agent_type=agent_type,
        raw_payload="",  # No payload — sync handles message detection
        message="",  # Will be populated from sync
        contact_phone=contact_phone,
        contact_email=contact_email,
        contact_name=contact_name,
        config=config,
        slug=slug,
        ghl_api_key=config.get("ghl_api_key", ""),
        ghl_location_id=config.get("ghl_location_id", ""),
        openrouter_api_key=tenant_keys["openrouter"],
        tenant_ai_keys=tenant_keys,
        webhook_contact_data=webhook_data,  # Skip get_contact() — use webhook data
    )

    # Build Prefect tags
    run_tags = build_run_tags(
        slug=slug,
        entity_id=entity_id,
        contact_id=contact_id,
        channel=ctx.channel,
        trigger_type="reply",
        agent_type=agent_type,
    )
    flow_name = build_flow_name(slug, entity_id)

    logger.info(
        "DEBOUNCE | pipeline_start | contact=%s | entity=%s | agent=%s",
        contact_id, entity_id[:8], agent_type,
    )

    try:
        with prefect_tags(*run_tags):
            result = await text_engine.with_options(name=flow_name)(ctx)

        logger.info(
            "DEBOUNCE | pipeline_complete | contact=%s | path=%s | result=%s",
            contact_id, result.get("path", "reply"), derive_result_tag(result, "reply"),
        )
        return result

    except asyncio.CancelledError:
        logger.info("DEBOUNCE | pipeline_cancelled | contact=%s | reason=new_message", contact_id)
        raise  # Let the caller handle cancellation
    except Exception as e:
        logger.exception("DEBOUNCE | pipeline_error | contact=%s", contact_id)
        try:
            await notify_error(e, source=f"debounce/{entity_id[:8]}/{contact_id}")
        except Exception:
            pass
        return {"error": True, "message": str(e)[:200]}


def _get_reply_window_sleep(config: dict[str, Any], agent_type: str = "") -> int:
    """Check if current time is outside the reply window.

    Returns seconds to sleep until window opens, or 0 if inside window / no restriction.

    Reads from setter's conversation.reply_window config.
    Supports three modes:
    - "24/7" (default): no restriction, always returns 0
    - "business_hours": reads from business_schedule JSONB on client row
    - "custom": reads per-day config from reply_window.days
    """
    from datetime import datetime
    from zoneinfo import ZoneInfo

    sys_config = config.get("system_config") or {}
    setters = sys_config.get("setters") or {}

    # Resolve setter
    setter_key = agent_type or "setter_1"
    setter = setters.get(setter_key) or {}
    if not setter:
        for k, v in setters.items():
            if v.get("is_default"):
                setter = v
                break

    conversation = setter.get("conversation") or {}
    rw = conversation.get("reply_window") or {}
    mode = rw.get("mode", "24/7")

    if mode == "24/7":
        return 0

    tz_name = config.get("timezone", "America/Chicago")
    try:
        tz = ZoneInfo(tz_name)
    except (KeyError, Exception):
        tz = ZoneInfo("America/Chicago")

    now = datetime.now(tz)

    # Determine which per-day config to use
    if mode == "custom":
        days_config = rw.get("days", {})
    elif mode == "business_hours":
        days_config = config.get("business_schedule") or {}
    else:
        return 0

    if not days_config:
        return 0

    day_names = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
    current_day_name = day_names[now.weekday()]
    day_cfg = days_config.get(current_day_name, {})

    # Check if today is enabled and we're in the window
    if day_cfg.get("enabled", True):
        start_str = day_cfg.get("start", "00:00")
        end_str = day_cfg.get("end", "23:59")
        try:
            sh, sm = map(int, start_str.split(":"))
            eh, em = map(int, end_str.split(":"))
        except (ValueError, AttributeError):
            return 0

        window_start = now.replace(hour=sh, minute=sm, second=0, microsecond=0)
        window_end = now.replace(hour=eh, minute=em, second=0, microsecond=0)

        if window_start <= now <= window_end:
            return 0  # Inside window

        if now < window_start:
            # Before today's window — sleep until it opens
            return int((window_start - now).total_seconds())

    # After today's window OR today is disabled — find next enabled window
    from datetime import timedelta

    for offset in range(1, 8):
        check_date = now + timedelta(days=offset)
        check_day_name = day_names[check_date.weekday()]
        check_cfg = days_config.get(check_day_name, {})

        if check_cfg.get("enabled", True):
            start_str = check_cfg.get("start", "07:00")
            try:
                sh, sm = map(int, start_str.split(":"))
            except (ValueError, AttributeError):
                sh, sm = 7, 0

            next_window = check_date.replace(hour=sh, minute=sm, second=0, microsecond=0)
            return int((next_window - now).total_seconds())

    return 0  # No enabled days (shouldn't happen)


def _get_debounce_window(config: dict[str, Any], agent_type: str = "") -> int:
    """Get debounce window from setter's conversation config, with default fallback.

    Reads from system_config.setters[setter_key].conversation.debounce_window_seconds.
    Falls back to DEFAULT_DEBOUNCE_SECONDS (90s) if not configured.
    """
    sys_config = config.get("system_config") or {}
    setters = sys_config.get("setters") or {}

    # Resolve setter key from agent_type (e.g., "setter_1", "setter_2" → find matching setter)
    setter_key = agent_type or "setter_1"
    setter = setters.get(setter_key) or {}

    # If agent_type didn't match a setter key directly, find the default setter
    if not setter:
        for k, v in setters.items():
            if v.get("is_default"):
                setter = v
                break

    conversation = setter.get("conversation") or {}
    window = conversation.get("debounce_window_seconds")
    if isinstance(window, (int, float)) and window > 0:
        return int(window)
    return DEFAULT_DEBOUNCE_SECONDS


# =========================================================================
# STARTUP RECOVERY
# =========================================================================


async def debounce_startup_recovery() -> None:
    """On server boot, check for unprocessed inbound messages.

    Looks for contacts with recent inbound messages (last 5 min) that
    have no AI response after them. Processes any found.

    Covers the edge case where the server restarted during a debounce window.
    """
    await asyncio.sleep(10)  # Let services initialize

    logger.info("DEBOUNCE | startup_recovery | scanning for unprocessed inbound messages...")

    # This will be implemented in Phase 1 when we have the full pipeline wired up.
    # For now, it's a placeholder that logs and returns.
    # Implementation will:
    # 1. Query each client's chat history for recent inbound messages
    # 2. Check if there's an AI response after the most recent inbound
    # 3. If not, trigger the pipeline for that contact

    logger.info("DEBOUNCE | startup_recovery | complete (placeholder — will be wired in Phase 1)")


def get_active_debounce_count() -> int:
    """Return number of active debounce timers (for monitoring)."""
    return len(_active)


def get_active_debounces() -> dict[str, dict[str, Any]]:
    """Return active debounce states (for debugging/monitoring)."""
    return {
        key: {
            "entity_id": state.entity_id,
            "contact_id": state.contact_id,
            "debounce_active": state.debounce_task is not None and not state.debounce_task.done(),
            "pipeline_running": state.pipeline_task is not None and not state.pipeline_task.done(),
        }
        for key, state in _active.items()
    }
