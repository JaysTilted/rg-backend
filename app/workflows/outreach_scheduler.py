"""Outreach Scheduler — schedule all outreach positions for a new lead.

Called when a form submission webhook arrives. Resolves the outreach template,
then schedules each position as a row in scheduled_messages with the configured
timing (delay + send window + jitter).

Replaces GHL's 9-step drip workflow with wait steps.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from app.services.message_scheduler import (
    schedule_message,
    apply_timing_jitter,
    enforce_send_window,
    parse_cadence_to_hours,
)
from app.services.workflow_tracker import WorkflowTracker

logger = logging.getLogger(__name__)

# Default jitter for outreach (same as follow-ups by default)
_DEFAULT_OUTREACH_JITTER = 10


async def schedule_outreach_positions(
    *,
    entity_id: str,
    contact_id: str,
    channel: str,
    resolved_positions: list[dict[str, Any]],
    timing_config: dict[str, Any],
    template_name: str,
    config: dict[str, Any],
    ghl: Any | None = None,
) -> int:
    """Schedule all outreach positions into scheduled_messages.

    Args:
        resolved_positions: List of resolved positions from outreach_resolver.
            Each has: position, delay, send_window, sms, email_subject, email_body,
            media_url, media_type, media_transcript.
        timing_config: Per-template timing config (default_send_window, jitter).
        template_name: form_service_interest (for logging/display).
        config: Entity config (for timezone, business_schedule).
        ghl: GHLClient for updating GHL field (optional).

    Returns number of positions scheduled.
    """
    from app.main import supabase

    tracker = WorkflowTracker(
        "outreach_scheduler",
        entity_id=entity_id,
        ghl_contact_id=contact_id,
        trigger_source="triggered_by_system",
    )

    try:
        if not resolved_positions:
            logger.warning("OUTREACH_SCHED | no positions to schedule | entity=%s contact=%s", entity_id[:8], contact_id)
            tracker.set_status("skipped")
            tracker.set_decisions({"reason": "no_positions"})
            return 0

        # Read jitter config from timing_config (per-template) or default
        jitter_percent = timing_config.get("jitter_percent", _DEFAULT_OUTREACH_JITTER)
        jitter_enabled = timing_config.get("jitter_enabled", True)
        default_send_window = timing_config.get("default_send_window")

        now = datetime.now(timezone.utc)
        scheduled_count = 0
        first_position_due: datetime | None = None

        for pos in resolved_positions:
            position_num = pos.get("position", scheduled_count + 1)
            delay_str = pos.get("delay", "0 minutes")

            # Parse delay to hours
            delay_hours_list = parse_cadence_to_hours([delay_str])
            delay_hours = delay_hours_list[0] if delay_hours_list else 0

            # Calculate due_at
            due_at = now + timedelta(hours=delay_hours)

            # Apply jitter (if enabled and delay > 0)
            if jitter_enabled and jitter_percent > 0 and delay_hours > 0:
                due_at = apply_timing_jitter(due_at, delay_hours, jitter_percent)

            # Apply send window (per-position override > template default > business_schedule)
            pos_send_window = pos.get("send_window")
            if pos_send_window:
                # Per-position override with custom mode
                due_at = enforce_send_window(due_at, config, window_config={"mode": "custom", "days": pos_send_window})
            elif default_send_window:
                # Template-level default
                due_at = enforce_send_window(due_at, config, window_config=default_send_window)
            else:
                # No outreach-specific window — use entity business_schedule if available
                due_at = enforce_send_window(due_at, config, window_config={"mode": "business_hours"})

            metadata = {
                "sms": pos.get("sms"),
                "email_subject": pos.get("email_subject"),
                "email_body": pos.get("email_body"),
                "media_url": pos.get("media_url"),
                "media_type": pos.get("media_type"),
                "media_transcript": pos.get("media_transcript"),
                "template_name": template_name,
            }
            msg_type = "outreach"

            # Schedule the position
            row = await schedule_message(
                supabase,
                entity_id=entity_id,
                contact_id=contact_id,
                message_type=msg_type,
                position=position_num,
                channel=channel,
                due_at=due_at,
                source="outreach",
                triggered_by="form_submission",
                metadata=metadata,
            )

            if row:
                scheduled_count += 1
                if first_position_due is None:
                    first_position_due = due_at

            logger.info(
                "OUTREACH_SCHED | pos=%d | type=%s | delay=%s | due=%s | sms=%s email=%s media=%s | contact=%s",
                position_num, pos_type, delay_str, due_at.isoformat(),
                bool(pos.get("sms")), bool(pos.get("email_body")), bool(pos.get("media_url")),
                contact_id,
            )

        logger.info(
            "OUTREACH_SCHED | complete | scheduled=%d/%d positions | template=%s | contact=%s",
            scheduled_count, len(resolved_positions), template_name, contact_id,
        )

        tracker.set_decisions({
            "scheduled": scheduled_count,
            "total_positions": len(resolved_positions),
            "template": template_name,
        })

        return scheduled_count
    except Exception as e:
        tracker.set_error(str(e))
        raise
    finally:
        await tracker.save()
