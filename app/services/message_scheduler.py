"""Message scheduler — schedule, cancel, query, and manage scheduled messages.

Central service for the Python-first scheduling system. All follow-ups,
outreach, smart follow-ups, reactivations, and activity resets flow through here.

The `scheduled_messages` table is the source of truth. Asyncio tasks (managed
by message_scheduler_loop.py) are the trigger mechanism.
"""

from __future__ import annotations

import logging
import random
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID
from zoneinfo import ZoneInfo

from app.services.supabase_client import SupabaseClient

logger = logging.getLogger(__name__)

# Default follow-up cadence (hours) — matches followup.py
_DEFAULT_CADENCE_HOURS = [6.0, 21.0, 24.0, 48.0, 72.0, 144.0]

# Default timing jitter percentage
_DEFAULT_JITTER_PERCENT = 10

# Default reactivation days
_DEFAULT_REACTIVATION_DAYS = 45


# =========================================================================
# CADENCE PARSING
# =========================================================================


def parse_cadence_to_hours(timings: list[str] | None) -> list[float]:
    """Parse human-readable cadence strings to hours.

    Accepts formats like: "6 hours", "2 days", "30 minutes", "1 day", "4 min".
    Falls back to _DEFAULT_CADENCE_HOURS if parsing fails or timings is None.
    """
    if not timings:
        return list(_DEFAULT_CADENCE_HOURS)

    hours: list[float] = []
    for t in timings:
        parsed = _parse_timing_string(t)
        if parsed is not None:
            hours.append(parsed)
        else:
            logger.warning("SCHEDULER | failed to parse cadence timing: %r", t)

    return hours if hours else list(_DEFAULT_CADENCE_HOURS)


def _parse_timing_string(s: str) -> float | None:
    """Parse a single timing string to hours. Returns None on failure."""
    s = s.strip().lower()
    if not s:
        return None

    # Try to extract number and unit
    parts = s.split()
    if len(parts) < 2:
        # Try formats like "6h", "2d", "30m"
        for suffix, multiplier in [("h", 1.0), ("d", 24.0), ("m", 1.0 / 60)]:
            if s.endswith(suffix):
                try:
                    return float(s[:-1]) * multiplier
                except ValueError:
                    pass
        # Try bare number (assume hours)
        try:
            return float(s)
        except ValueError:
            return None

    try:
        value = float(parts[0])
    except ValueError:
        return None

    unit = parts[1].rstrip("s")  # "hours" -> "hour", "days" -> "day"
    unit_map = {
        "minute": 1.0 / 60,
        "min": 1.0 / 60,
        "hour": 1.0,
        "hr": 1.0,
        "day": 24.0,
        "week": 168.0,
    }
    multiplier = unit_map.get(unit)
    if multiplier is None:
        return None
    return value * multiplier


# =========================================================================
# TIMING JITTER
# =========================================================================


def apply_timing_jitter(
    due_at: datetime,
    base_delay_hours: float,
    jitter_percent: float = _DEFAULT_JITTER_PERCENT,
) -> datetime:
    """Add random variation to a scheduled time.

    Args:
        due_at: The originally calculated due time.
        base_delay_hours: The cadence interval this is based on (e.g., 6.0 hours).
            Used to calculate the jitter range — jitter scales with the interval.
        jitter_percent: Variation percentage (default 10). E.g., 10% on 6h = +/-36min.

    Returns:
        Jittered datetime. Applied BEFORE send window enforcement.
    """
    if jitter_percent <= 0:
        return due_at

    jitter_hours = base_delay_hours * (jitter_percent / 100.0)
    jitter_seconds = jitter_hours * 3600
    offset = random.uniform(-jitter_seconds, jitter_seconds)
    return due_at + timedelta(seconds=offset)


# =========================================================================
# SEND WINDOW ENFORCEMENT
# =========================================================================


_DAY_NAMES = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]


def enforce_send_window(
    due_at: datetime,
    config: dict[str, Any],
    send_window: dict[str, str] | None = None,
    window_config: dict[str, Any] | None = None,
) -> datetime:
    """Push due_at into the allowed send window if it falls outside.

    Supports per-day scheduling: each day of the week can have its own
    enabled/disabled state and start/end times.

    Resolution order:
    1. Explicit send_window arg (per-position override, simple {start, end})
    2. window_config arg (three-mode config from setter's follow_up.send_window)
    3. business_schedule JSONB on entity config (fallback)

    If no send window is configured or enabled, returns due_at unchanged.

    Args:
        due_at: Proposed send time (timezone-aware).
        config: Entity config dict (from Supabase entities row).
        send_window: Optional simple override {start: "07:00", end: "22:00"} for per-position.
        window_config: Optional three-mode config {mode, days?} from setter's follow_up.send_window.

    Returns:
        Adjusted datetime within the send window, with 0-2h morning jitter
        when pushed to a new window.
    """
    tz_name = config.get("timezone", "America/Chicago")
    try:
        tz = ZoneInfo(tz_name)
    except (KeyError, Exception):
        tz = ZoneInfo("America/Chicago")

    local_dt = due_at.astimezone(tz)

    # Simple override (per-outreach-position) — not per-day, just start/end
    if send_window and send_window.get("start") and send_window.get("end"):
        return _enforce_simple_window(local_dt, send_window["start"], send_window["end"], due_at.tzinfo)

    # Three-mode routing from window_config (passed by caller from setter config)
    wc = window_config or {}
    mode = wc.get("mode", "")  # "24/7" | "business_hours" | "custom"

    if mode == "24/7":
        return due_at  # No restriction

    if mode == "custom" and wc.get("days"):
        return _enforce_per_day_window(local_dt, wc["days"], due_at.tzinfo)

    if mode == "business_hours" or not mode:
        # Read from business_schedule JSONB on client/bot row
        biz_schedule = config.get("business_schedule")
        if biz_schedule and isinstance(biz_schedule, dict):
            return _enforce_per_day_window(local_dt, biz_schedule, due_at.tzinfo)

    return due_at  # No window configured


def _enforce_simple_window(
    local_dt: datetime,
    start_str: str,
    end_str: str,
    original_tzinfo: Any,
) -> datetime:
    """Enforce a simple start/end window (no per-day logic)."""
    try:
        start_hour, start_min = map(int, start_str.split(":"))
        end_hour, end_min = map(int, end_str.split(":"))
    except (ValueError, AttributeError):
        logger.warning("SCHEDULER | invalid window format: %s - %s", start_str, end_str)
        return local_dt.astimezone(original_tzinfo) if original_tzinfo else local_dt

    window_start = local_dt.replace(hour=start_hour, minute=start_min, second=0, microsecond=0)
    window_end = local_dt.replace(hour=end_hour, minute=end_min, second=0, microsecond=0)

    if window_start <= local_dt <= window_end:
        return local_dt.astimezone(original_tzinfo) if original_tzinfo else local_dt

    if local_dt < window_start:
        # Before window today
        jitter_secs = random.uniform(0, 7200)
        result = window_start + timedelta(seconds=jitter_secs)
    else:
        # After window today — next day
        next_start = window_start + timedelta(days=1)
        jitter_secs = random.uniform(0, 7200)
        result = next_start + timedelta(seconds=jitter_secs)

    return result.astimezone(original_tzinfo) if original_tzinfo else result


def _enforce_per_day_window(
    local_dt: datetime,
    days_config: dict[str, Any],
    original_tzinfo: Any,
) -> datetime:
    """Enforce per-day send window. Each day has enabled + start + end.

    If the current day is disabled or outside its window, find the next
    enabled day's window and push there with 0-2h morning jitter.
    Searches up to 7 days ahead to avoid infinite loops.
    """
    current_day_name = _DAY_NAMES[local_dt.weekday()]
    day_cfg = days_config.get(current_day_name, {})

    # Current day is enabled — check if we're in the window
    if day_cfg.get("enabled", True):
        start_str = day_cfg.get("start", "00:00")
        end_str = day_cfg.get("end", "23:59")

        try:
            start_hour, start_min = map(int, start_str.split(":"))
            end_hour, end_min = map(int, end_str.split(":"))
        except (ValueError, AttributeError):
            return local_dt.astimezone(original_tzinfo) if original_tzinfo else local_dt

        window_start = local_dt.replace(hour=start_hour, minute=start_min, second=0, microsecond=0)
        window_end = local_dt.replace(hour=end_hour, minute=end_min, second=0, microsecond=0)

        if window_start <= local_dt <= window_end:
            # Inside window — no change
            return local_dt.astimezone(original_tzinfo) if original_tzinfo else local_dt

        if local_dt < window_start:
            # Before today's window — push to today's window start + jitter
            jitter_secs = random.uniform(0, 7200)
            result = window_start + timedelta(seconds=jitter_secs)
            return result.astimezone(original_tzinfo) if original_tzinfo else result

        # After today's window — fall through to find next enabled day

    # Find the next enabled day (search up to 7 days ahead)
    for offset in range(1, 8):
        check_date = local_dt + timedelta(days=offset)
        check_day_name = _DAY_NAMES[check_date.weekday()]
        check_cfg = days_config.get(check_day_name, {})

        if check_cfg.get("enabled", True):
            start_str = check_cfg.get("start", "07:00")
            try:
                start_hour, start_min = map(int, start_str.split(":"))
            except (ValueError, AttributeError):
                start_hour, start_min = 7, 0

            next_window = check_date.replace(
                hour=start_hour, minute=start_min, second=0, microsecond=0,
            )
            jitter_secs = random.uniform(0, 7200)
            result = next_window + timedelta(seconds=jitter_secs)
            return result.astimezone(original_tzinfo) if original_tzinfo else result

    # No enabled days found (shouldn't happen) — return unchanged
    logger.warning("SCHEDULER | no enabled days in send window config")
    return local_dt.astimezone(original_tzinfo) if original_tzinfo else local_dt


# =========================================================================
# SCHEDULE / CANCEL / QUERY
# =========================================================================


async def schedule_message(
    supabase: SupabaseClient,
    *,
    entity_id: str | UUID,
    contact_id: str,
    message_type: str,
    position: int | None = None,
    channel: str = "SMS",
    due_at: datetime,
    source: str = "cadence",
    triggered_by: str | None = None,
    smart_reason: str | None = None,
    metadata: dict | None = None,
    to_phone: str | None = None,
) -> dict[str, Any] | None:
    """Insert or update a scheduled message in the DB.

    Uses SELECT-then-INSERT/UPDATE pattern (not PostgREST UPSERT) because
    our unique index is partial (WHERE status='pending') which PostgREST
    doesn't support for UPSERT via REST API.

    If a pending message already exists for this contact/type/position,
    it gets updated with the new due_at. This is the core mechanism for
    the reactivation push-forward pattern.

    Returns the inserted/updated row dict, or None on failure.
    """
    entity_id_str = str(entity_id)
    pos_val = position if position is not None else 0

    # Check for existing pending row
    check_params: dict[str, str] = {
        "entity_id": f"eq.{entity_id_str}",
        "contact_id": f"eq.{contact_id}",
        "message_type": f"eq.{message_type}",
        "status": "eq.pending",
        "limit": "1",
    }

    check_resp = await supabase._request(
        supabase.main_client,
        "GET",
        "/scheduled_messages",
        params=check_params,
        label="schedule_check_existing",
    )

    existing = check_resp.json() if check_resp.status_code < 400 else []
    # Filter by position match (COALESCE logic from the unique index)
    existing = [
        r for r in existing
        if (r.get("position") or 0) == pos_val
    ] if existing else []

    if existing:
        # UPDATE existing row with new due_at and metadata
        existing_id = existing[0]["id"]
        # Cancel the old asyncio task before updating
        from app.workflows.message_scheduler_loop import cancel_fire_task
        cancel_fire_task(str(existing_id))

        update_body: dict[str, Any] = {
            "due_at": due_at.isoformat(),
            "source": source,
            "triggered_by": triggered_by,
            "smart_reason": smart_reason,
            "metadata": metadata or {},
            "channel": channel,
            "fired_at": None,
            "result": None,
            "result_reason": None,
        }
        if to_phone:
            update_body["to_phone"] = to_phone

        resp = await supabase._request(
            supabase.main_client,
            "PATCH",
            "/scheduled_messages",
            params={"id": f"eq.{existing_id}"},
            json=update_body,
            headers={"Prefer": "return=representation"},
            label="schedule_update_existing",
        )
    else:
        # INSERT new row
        tenant_id = await supabase.resolve_tenant_id(entity_id_str)
        body: dict[str, Any] = {
            "entity_id": entity_id_str,
            **({"tenant_id": tenant_id} if tenant_id else {}),
            "contact_id": contact_id,
            "message_type": message_type,
            "position": position,
            "channel": channel,
            "status": "pending",
            "due_at": due_at.isoformat(),
            "source": source,
            "triggered_by": triggered_by,
            "smart_reason": smart_reason,
            "metadata": metadata or {},
        }
        if to_phone:
            body["to_phone"] = to_phone
        resp = await supabase._request(
            supabase.main_client,
            "POST",
            "/scheduled_messages",
            json=body,
            headers={"Prefer": "return=representation"},
            label="schedule_insert_new",
        )

    if resp.status_code >= 400:
        logger.error(
            "SCHEDULER | schedule_message failed | status=%d | body=%s | type=%s pos=%s contact=%s",
            resp.status_code, resp.text[:200], message_type, position, contact_id,
        )
        return None

    rows = resp.json()
    row = rows[0] if isinstance(rows, list) and rows else rows
    logger.info(
        "SCHEDULER | scheduled | id=%s | type=%s | pos=%s | due=%s | source=%s | triggered_by=%s | contact=%s",
        row.get("id", "?"), message_type, position, due_at.isoformat(),
        source, triggered_by, contact_id,
    )

    # Create asyncio fire task (Layer 1 — exact-time execution)
    from app.workflows.message_scheduler_loop import create_fire_task
    create_fire_task(row)

    return row


async def cancel_pending(
    supabase: SupabaseClient,
    contact_id: str,
    entity_id: str | UUID,
    *,
    message_types: list[str] | None = None,
    reason: str = "lead_replied",
) -> list[dict[str, Any]]:
    """Cancel all pending messages for a contact.

    Args:
        message_types: If provided, only cancel these types. Otherwise cancel ALL.
        reason: Why cancelled (lead_replied, booking_confirmed, stop_bot, manual, superseded).

    Returns list of cancelled row dicts (for asyncio task cleanup).
    """
    # Build filter
    params: dict[str, str] = {
        "contact_id": f"eq.{contact_id}",
        "entity_id": f"eq.{entity_id}",
        "status": "eq.pending",
        "select": "id,message_type,position",
    }
    if message_types:
        types_csv = ",".join(message_types)
        params["message_type"] = f"in.({types_csv})"

    # First, get the IDs of rows to cancel (for asyncio task cleanup)
    get_resp = await supabase._request(
        supabase.main_client,
        "GET",
        "/scheduled_messages",
        params=params,
        label="cancel_pending_get",
    )

    if get_resp.status_code >= 400:
        logger.error("SCHEDULER | cancel_pending GET failed | status=%d", get_resp.status_code)
        return []

    rows = get_resp.json()
    if not rows:
        return []

    # Now cancel them
    cancel_params: dict[str, str] = {
        "contact_id": f"eq.{contact_id}",
        "entity_id": f"eq.{entity_id}",
        "status": "eq.pending",
    }
    if message_types:
        cancel_params["message_type"] = f"in.({','.join(message_types)})"

    patch_resp = await supabase._request(
        supabase.main_client,
        "PATCH",
        "/scheduled_messages",
        params=cancel_params,
        json={
            "status": "cancelled",
            "cancelled_at": datetime.now(timezone.utc).isoformat(),
            "cancel_reason": reason,
        },
        headers={"Prefer": "return=representation"},
        label="cancel_pending",
    )

    cancelled_count = len(rows)
    types_cancelled = {r.get("message_type") for r in rows}
    logger.info(
        "SCHEDULER | cancelled %d messages | types=%s | reason=%s | contact=%s",
        cancelled_count, types_cancelled, reason, contact_id,
    )

    # Cancel asyncio fire tasks for cancelled messages (Layer 1 cleanup)
    from app.workflows.message_scheduler_loop import cancel_fire_task
    for row in rows:
        cancel_fire_task(str(row.get("id", "")))

    return rows


async def get_pending_for_contact(
    supabase: SupabaseClient,
    contact_id: str,
    entity_id: str | UUID,
    *,
    message_type: str | None = None,
) -> list[dict[str, Any]]:
    """Query pending messages for a contact (used by smart FU check in reply path)."""
    params: dict[str, str] = {
        "contact_id": f"eq.{contact_id}",
        "entity_id": f"eq.{entity_id}",
        "status": "eq.pending",
        "order": "due_at.asc",
    }
    if message_type:
        params["message_type"] = f"eq.{message_type}"

    resp = await supabase._request(
        supabase.main_client,
        "GET",
        "/scheduled_messages",
        params=params,
        label="get_pending",
    )

    if resp.status_code >= 400:
        logger.warning("SCHEDULER | get_pending failed | status=%d", resp.status_code)
        return []

    return resp.json()


async def update_message_status(
    supabase: SupabaseClient,
    message_id: str | UUID,
    *,
    status: str,
    result: str | None = None,
    result_reason: str | None = None,
    error: str | None = None,
) -> bool:
    """Update a scheduled message's status after firing."""
    body: dict[str, Any] = {"status": status}
    if status == "processing":
        body["fired_at"] = datetime.now(timezone.utc).isoformat()
    if result:
        body["result"] = result
    if result_reason:
        body["result_reason"] = result_reason
    # 'error' is not a column — store in result_reason for failures
    if error and not result_reason:
        body["result_reason"] = error

    resp = await supabase._request(
        supabase.main_client,
        "PATCH",
        "/scheduled_messages",
        params={"id": f"eq.{message_id}"},
        json=body,
        label="update_status",
    )
    return resp.status_code < 400


async def claim_message(
    supabase: SupabaseClient,
    message_id: str | UUID,
) -> dict[str, Any] | None:
    """Atomically claim a pending message for processing.

    Sets status='processing' + fired_at only if still pending.
    Returns the updated row, or None if already claimed/cancelled.
    """
    resp = await supabase._request(
        supabase.main_client,
        "PATCH",
        "/scheduled_messages",
        params={
            "id": f"eq.{message_id}",
            "status": "eq.pending",  # Only claim if still pending
        },
        json={
            "status": "processing",
            "fired_at": datetime.now(timezone.utc).isoformat(),
        },
        headers={"Prefer": "return=representation"},
        label="claim_message",
    )

    if resp.status_code >= 400:
        return None

    rows = resp.json()
    if not rows:
        return None  # Was already claimed or cancelled

    return rows[0] if isinstance(rows, list) else rows


# =========================================================================
# GHL FOLLOW-UP FIELD UPDATE
# =========================================================================


async def update_ghl_followup_field(
    ghl: Any,
    contact_id: str,
    message_type: str | None = None,
    position: int | None = None,
    due_at: datetime | None = None,
    tz_name: str | None = None,
) -> None:
    """Update the GHL 'AI Follow Up' custom field on a contact.

    Non-blocking — logs warning on failure, never raises.
    This field is informational only (visible in GHL contact view).

    Display format:
    - Scheduled: "#3 · Mar 28, 10:30 AM" (position + date in client timezone)
    - Not scheduled: "Not scheduled"

    Args:
        ghl: GHLClient instance.
        contact_id: GHL contact ID.
        message_type: "followup" or "smart_followup", or None for not scheduled.
        position: Follow-up position number (e.g. 1, 2, 3).
        due_at: When the follow-up is scheduled for (UTC).
        tz_name: IANA timezone string for display formatting (e.g. "America/Chicago").
    """
    try:
        if message_type is None or due_at is None:
            await ghl.set_custom_field(contact_id, "next_ai_follow_up", "Not scheduled")
            return

        pos = position or 1
        display = f"#{pos} · {_format_due(due_at, tz_name)}"
        await ghl.set_custom_field(contact_id, "next_ai_follow_up", display)
    except Exception as e:
        logger.warning("SCHEDULER | GHL field update failed (non-blocking): %s", e)


def _format_due(dt: datetime, tz_name: str | None = None) -> str:
    """Format a datetime in client timezone for GHL display."""
    try:
        from zoneinfo import ZoneInfo
        try:
            tz = ZoneInfo(tz_name) if tz_name else ZoneInfo("America/Chicago")
        except (KeyError, Exception):
            tz = ZoneInfo("America/Chicago")
        local_dt = dt.astimezone(tz)
        return local_dt.strftime("%b %-d, %I:%M %p")
    except Exception:
        return str(dt)[:16]


# =========================================================================
# FOLLOW-UP CHAIN SCHEDULING
# =========================================================================


async def schedule_followup_chain(
    ctx: Any,
    *,
    position: int = 1,
    triggered_by: str = "reply",
) -> dict[str, Any] | None:
    """Schedule the next follow-up in the cadence chain.

    Called from:
    - reply_pipeline (after reply sent) → schedules FU#1
    - _fire_followup (after FU sent) → schedules FU#N+1

    Reads cadence + jitter config from the matched setter's conversation.follow_up.
    Applies timing jitter + send window enforcement.
    Updates GHL follow-up field.

    Returns the scheduled row, or None if no more positions in cadence.
    """
    from app.text_engine.followup_compiler import get_cadence_timing

    # Lazy import supabase singleton
    from app.main import supabase

    entity_id = ctx.entity_id
    contact_id = ctx.contact_id
    channel = ctx.channel

    # Read cadence + jitter from matched setter's follow_up config
    setter = ctx.compiled.get("_matched_setter") or {}
    fu_config = setter.get("conversation", {}).get("follow_up") or {}
    fu_sections = fu_config.get("sections") or fu_config  # Handle both nested and flat

    # Parse cadence timings
    custom_timings = get_cadence_timing(fu_config)
    cadence_hours = parse_cadence_to_hours(custom_timings)

    # Check if position is within cadence
    if position > len(cadence_hours):
        logger.info(
            "SCHEDULER | chain_complete | no more positions (pos=%d, cadence_len=%d) | contact=%s",
            position, len(cadence_hours), contact_id,
        )
        # Clear GHL field — chain is done
        if not ctx.is_test_mode:
            try:
                await update_ghl_followup_field(ctx.ghl, contact_id)
            except Exception:
                pass
        return None

    # Calculate due_at
    delay_hours = cadence_hours[position - 1]  # 0-indexed
    now = datetime.now(timezone.utc)
    due_at = now + timedelta(hours=delay_hours)

    # Apply timing jitter (if enabled)
    jitter_enabled = fu_sections.get("timing_jitter_enabled", True)  # Default on
    jitter_percent = fu_sections.get("timing_jitter_percent", _DEFAULT_JITTER_PERCENT) if jitter_enabled else 0
    if jitter_percent > 0:
        due_at = apply_timing_jitter(due_at, delay_hours, jitter_percent)

    # Apply send window enforcement
    # Pass the setter's follow_up send_window config so enforce_send_window reads the right source
    fu_send_window = fu_config.get("send_window") or {}
    due_at = enforce_send_window(due_at, ctx.config, window_config=fu_send_window)

    # Schedule
    row = await schedule_message(
        supabase,
        entity_id=entity_id,
        contact_id=contact_id,
        message_type="followup",
        position=position,
        channel=channel,
        due_at=due_at,
        source="cadence",
        triggered_by=triggered_by,
        to_phone=getattr(ctx, "contact_phone", None),
    )

    if row and not ctx.is_test_mode:
        try:
            await update_ghl_followup_field(
                ctx.ghl, contact_id,
                message_type="followup",
                position=position,
                due_at=due_at,
                tz_name=ctx.config.get("timezone"),
            )
        except Exception:
            pass

    logger.info(
        "SCHEDULER | chain_scheduled | FU#%d | due=%s | delay=%.1fh | jitter=%d%% | contact=%s",
        position, due_at.isoformat(), delay_hours, jitter_percent, contact_id,
    )

    return row


# =========================================================================
# MISSED CALL TEXTBACK → FOLLOW-UP SCHEDULING
# =========================================================================


async def schedule_missed_call_followup(
    *,
    entity_id: str,
    contact_id: str,
    channel: str,
    contact_phone: str | None,
    config: dict,
    setter: dict,
    ghl: Any,
    followup_config: dict,
) -> dict[str, Any] | None:
    """Schedule FU#1 after a missed call textback delivery.

    Modes:
    - "no_followup": returns None (default / current behavior)
    - "normal_cadence": uses setter's FU#1 timing from cadence config
    - "custom_delay": uses followup_config["custom_delay_hours"]

    Only schedules position=1. When FU#1 fires, the normal cadence chain
    takes over (FU#2, FU#3, etc.) via _fire_followup → schedule_followup_chain.
    """
    from app.text_engine.followup_compiler import get_cadence_timing
    from app.main import supabase

    mode = followup_config.get("mode", "normal_cadence")
    if mode == "no_followup":
        return None

    # Determine delay hours
    if mode == "custom_delay":
        delay_hours = float(followup_config.get("custom_delay_hours", 3))
    else:
        # normal_cadence — read FU#1 timing from setter
        fu_config = setter.get("conversation", {}).get("follow_up") or {}
        custom_timings = get_cadence_timing(fu_config)
        cadence_hours = parse_cadence_to_hours(custom_timings)
        delay_hours = cadence_hours[0] if cadence_hours else 6.0

    now = datetime.now(timezone.utc)
    due_at = now + timedelta(hours=delay_hours)

    # Apply jitter from setter's follow-up config
    fu_config = setter.get("conversation", {}).get("follow_up") or {}
    fu_sections = fu_config.get("sections") or fu_config
    jitter_enabled = fu_sections.get("timing_jitter_enabled", True)
    jitter_percent = fu_sections.get("timing_jitter_percent", _DEFAULT_JITTER_PERCENT) if jitter_enabled else 0
    if jitter_percent > 0:
        due_at = apply_timing_jitter(due_at, delay_hours, jitter_percent)

    # Apply send window enforcement
    fu_send_window = fu_config.get("send_window") or {}
    due_at = enforce_send_window(due_at, config, window_config=fu_send_window)

    # Schedule FU#1
    row = await schedule_message(
        supabase,
        entity_id=entity_id,
        contact_id=contact_id,
        message_type="followup",
        position=1,
        channel=channel,
        due_at=due_at,
        source="cadence",
        triggered_by="missed_call_textback",
        to_phone=contact_phone,
    )

    if row:
        try:
            await update_ghl_followup_field(
                ghl, contact_id,
                message_type="followup",
                position=1,
                due_at=due_at,
                tz_name=config.get("timezone"),
            )
        except Exception:
            pass

    logger.info(
        "SCHEDULER | missed_call_fu | FU#1 scheduled | mode=%s | delay=%.1fh | due=%s | contact=%s",
        mode, delay_hours, due_at.isoformat(), contact_id,
    )

    return row
