"""Smart follow-up scheduling logic.

Shared helpers for clearing and setting smart follow-up timers. Lives here
(not in pipeline.py, post_processing.py, or followup.py) to avoid circular
imports — multiple modules import from here.

MIGRATED: Now uses scheduled_messages DB table instead of GHL custom fields/tags.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from app.models import PipelineContext
from app.text_engine.utils import get_timezone

logger = logging.getLogger(__name__)


# Timeframe → days mapping for date calculation (used by set_smart_followup
# and the smart scheduler agent in followup.py)
TIMEFRAME_DAYS: dict[str, int] = {
    "tomorrow": 1,
    "2_days": 2,
    "3_days": 3,
    "4-6_days": 5,
    "1_week": 7,
    "2_weeks": 14,
    "3_weeks": 21,
    "1_month": 30,
    "2_months": 60,
    "3_months": 90,
    "4_months": 120,
}


async def clear_smart_followup(ctx: PipelineContext) -> None:
    """Cancel any pending smart follow-up for this contact.

    Replaces the old GHL custom field/tag clearing.
    Now just cancels the DB row in scheduled_messages.
    Safe to call even if no smart FU exists.
    """
    if ctx.is_test_mode:
        return

    try:
        from app.services.message_scheduler import cancel_pending
        from app.main import supabase

        cancelled = await cancel_pending(
            supabase,
            ctx.contact_id,
            ctx.entity_id,
            message_types=["smart_followup"],
            reason="superseded",
        )
        if cancelled:
            logger.info("Cleared smart follow-up (cancelled %d pending rows)", len(cancelled))
    except Exception as e:
        logger.warning("Clear smart follow-up failed: %s", e)


async def set_smart_followup(
    ctx: PipelineContext, timeframe: str, reason: str
) -> dict[str, Any]:
    """Schedule a smart follow-up via the scheduled_messages table.

    Called from the follow-up path when the determination agent decides
    to reschedule instead of sending a follow-up now.

    Replaces the old approach of writing to GHL custom fields + tags + webhook.
    Now just inserts/upserts a row in scheduled_messages.

    In test mode, logs intent but skips DB writes.
    Returns timer metadata for artifact visibility.
    """
    tz = ctx.tz or get_timezone(ctx.config)
    now = datetime.now(tz)
    days = TIMEFRAME_DAYS.get(timeframe, 14)
    due = now + timedelta(days=days)

    timer_data = {
        "timeframe": timeframe,
        "reason": reason,
        "due": due.isoformat(),
        "scheduled_at": now.isoformat(),
    }

    if ctx.is_test_mode:
        logger.info("TEST MODE — would set smart follow-up: %s (due %s)", timeframe, due.isoformat())
        return timer_data

    try:
        from app.services.message_scheduler import (
            schedule_message,
            enforce_send_window,
            update_ghl_followup_field,
        )
        from app.main import supabase

        # Apply send window (don't schedule a smart FU at 3 AM)
        due_utc = due.astimezone(timezone.utc)
        # Read send window from the setter's follow_up config
        _setter = ctx.compiled.get("_matched_setter") or {}
        _fu_sw = _setter.get("conversation", {}).get("follow_up", {}).get("send_window") or {}
        due_utc = enforce_send_window(due_utc, ctx.config, window_config=_fu_sw)

        row = await schedule_message(
            supabase,
            entity_id=ctx.entity_id,
            contact_id=ctx.contact_id,
            message_type="smart_followup",
            position=None,
            channel=ctx.channel,
            due_at=due_utc,
            source="smart_reschedule",
            smart_reason=reason,
            metadata=timer_data,
        )

        # Update GHL display field
        if row:
            try:
                await update_ghl_followup_field(
                    ctx.ghl, ctx.contact_id,
                    message_type="smart_followup",
                    position=1,
                    due_at=due_utc,
                    tz_name=ctx.config.get("timezone"),
                )
            except Exception:
                pass

        logger.info("Set smart follow-up: %s (due %s) — DB row", timeframe, due.isoformat())

    except Exception as e:
        logger.warning("Set smart follow-up failed: %s", e)

    return timer_data
