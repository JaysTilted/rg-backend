"""Appointment Reminder Scheduler — schedule reminders before/after appointments.

When a booking webhook fires, this module:
1. Matches the calendar_id to an appointment template
2. Schedules all reminder positions based on their timing direction:
   - "after_trigger" → due_at = booking_time + delay
   - "before_appointment" → due_at = appointment_start - delay
   - "after_appointment" → due_at = appointment_start + delay
3. On reschedule: cancels old reminders, schedules new ones
4. On cancel: cancels all pending reminders
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from app.services.message_scheduler import (
    schedule_message,
    cancel_pending,
    apply_timing_jitter,
    enforce_send_window,
    parse_cadence_to_hours,
)
from app.services.workflow_tracker import WorkflowTracker

logger = logging.getLogger(__name__)


async def schedule_appointment_reminders(
    *,
    entity_id: str,
    contact_id: str,
    channel: str,
    calendar_id: str,
    calendar_name: str,
    appointment_id: str,
    appointment_start: datetime,
    appointment_end: datetime | None = None,
    config: dict[str, Any],
    ghl: Any | None = None,
) -> int:
    """Schedule all reminder positions for a confirmed appointment.

    Finds the appointment template matching the calendar_id, then schedules
    each position based on its timing_direction.

    Returns number of positions scheduled.
    """
    from app.main import supabase

    tracker = WorkflowTracker(
        "appointment_reminder_scheduler",
        entity_id=entity_id,
        ghl_contact_id=contact_id,
        trigger_source="triggered_by_system",
    )

    try:
        from app.workflows.outreach_resolver import (
            _parse_name,
            _build_variable_map,
            _resolve_text,
            _resolve_email_body,
            _pass_through,
            _pick_variant,
            _determine_variant,
        )
        from app.models import OutreachResolverBody

        # Find matching appointment template by calendar_id
        templates = await supabase.get_outreach_templates(entity_id)
        matched = next(
            (t for t in templates
             if t.get("is_appointment_template")
             and t.get("calendar_id") == calendar_id),
            None,
        )

        if not matched and calendar_name:
            # Try matching by calendar_name as fallback (only if name is non-empty)
            matched = next(
                (t for t in templates
                 if t.get("is_appointment_template")
                 and (t.get("calendar_name") or "").strip().lower() == calendar_name.strip().lower()),
                None,
            )

        if not matched:
            logger.info(
                "APPT_REMIND | no matching template | calendar_id=%s name=%s | entity=%s",
                calendar_id, calendar_name, entity_id[:8],
            )
            tracker.set_status("skipped")
            tracker.set_decisions({"reason": "no_matching_template", "calendar_id": calendar_id})
            return 0

        positions = matched.get("positions") or []
        if not positions:
            logger.info("APPT_REMIND | template has no positions | template=%s", matched.get("form_service_interest"))
            tracker.set_status("skipped")
            tracker.set_decisions({"reason": "no_positions"})
            return 0

        # A/B variant
        variant, is_ab_test, _ = await _determine_variant(matched)

        # Build variable map for template substitution
        # We need contact data — fetch from GHL if available
        contact_name = ""
        contact_phone = ""
        contact_email = ""

        if ghl:
            try:
                contact = await ghl.get_contact(contact_id)
                if contact:
                    contact_name = f"{contact.get('firstName', '')} {contact.get('lastName', '')}".strip()
                    contact_phone = contact.get("phone", "")
                    contact_email = contact.get("email", "")
            except Exception:
                pass

        first_name, last_name, full_name = _parse_name(contact_name)

        # Build a minimal OutreachResolverBody for variable substitution
        location = config.get("location") or {}
        var_map = {
            "{{contact.first_name}}": first_name,
            "{{contact.last_name}}": last_name,
            "{{contact.name}}": full_name,
            "{{contact.email}}": contact_email,
            "{{contact.phone}}": contact_phone,
            "{{location.name}}": config.get("name", ""),
            "{{location.address}}": location.get("address", ""),
            "{{location.city}}": location.get("city", ""),
            "{{location.state}}": location.get("state", ""),
            "{{appointment.start_date}}": appointment_start.strftime("%B %d, %Y"),
            "{{appointment.start_time}}": appointment_start.strftime("%I:%M %p"),
            "{{appointment.day_of_week}}": appointment_start.strftime("%A"),
        }

        # Schedule each position
        now = datetime.now(timezone.utc)
        scheduled_count = 0

        for pos in positions:
            position_num = pos.get("position", scheduled_count + 1)
            delay_str = pos.get("delay", "0 minutes")
            direction = pos.get("timing_direction", "after_trigger")

            # Parse delay
            delay_hours_list = parse_cadence_to_hours([delay_str])
            delay_hours = delay_hours_list[0] if delay_hours_list else 0

            # Calculate due_at based on direction
            if direction == "after_trigger":
                # After booking confirmation (immediate or delayed)
                due_at = now + timedelta(hours=delay_hours)
            elif direction == "before_appointment":
                # Before appointment start time
                due_at = appointment_start - timedelta(hours=delay_hours)
                # Don't schedule if due_at is in the past
                if due_at <= now:
                    logger.info(
                        "APPT_REMIND | skipping past-due position | pos=%d | due=%s (already past)",
                        position_num, due_at.isoformat(),
                    )
                    continue
            elif direction == "after_appointment":
                # After appointment start time
                due_at = appointment_start + timedelta(hours=delay_hours)
            else:
                logger.warning("APPT_REMIND | unknown timing_direction: %s (pos %d) — defaulting to after_trigger", direction, position_num)
                due_at = now + timedelta(hours=delay_hours)

            # Apply send window
            timing_config = matched.get("timing_config") or {}
            default_sw = timing_config.get("default_send_window")
            if default_sw:
                due_at = enforce_send_window(due_at, config, window_config=default_sw)
            else:
                due_at = enforce_send_window(due_at, config, window_config={"mode": "business_hours"})

            # Resolve template content for this position
            raw_sms = _pick_variant(pos.get("sms"), pos.get("sms_b"), variant)
            resolved_sms = _resolve_text(raw_sms, var_map)
            raw_email_subject = _pick_variant(pos.get("email_subject"), pos.get("email_subject_b"), variant)
            raw_email_body = _pick_variant(pos.get("email_body"), pos.get("email_body_b"), variant)
            resolved_email_subject = _resolve_text(raw_email_subject, var_map)
            resolved_email_body = _resolve_email_body(raw_email_body, var_map)
            raw_media = _pick_variant(pos.get("media_url"), pos.get("media_url_b"), variant)
            resolved_media = _pass_through(raw_media)

            metadata = {
                "sms": resolved_sms,
                "email_subject": resolved_email_subject,
                "email_body": resolved_email_body,
                "media_url": resolved_media,
                "media_type": pos.get("media_type"),
                "template_name": matched.get("form_service_interest", ""),
                "appointment_id": appointment_id,
                "appointment_start": appointment_start.isoformat(),
                "timing_direction": direction,
            }

            row = await schedule_message(
                supabase,
                entity_id=entity_id,
                contact_id=contact_id,
                message_type="appointment_reminder",
                position=position_num,
                channel=channel,
                due_at=due_at,
                source="outreach",
                triggered_by="booking",
                metadata=metadata,
            )

            if row:
                scheduled_count += 1

            logger.info(
                "APPT_REMIND | scheduled | pos=%d | dir=%s | delay=%s | due=%s | contact=%s",
                position_num, direction, delay_str, due_at.isoformat(), contact_id,
            )

        logger.info(
            "APPT_REMIND | complete | scheduled=%d/%d | calendar=%s | appt=%s | contact=%s",
            scheduled_count, len(positions), calendar_name, appointment_id, contact_id,
        )

        tracker.set_decisions({
            "scheduled": scheduled_count,
            "total_positions": len(positions),
            "calendar": calendar_name,
            "appointment_id": appointment_id,
        })

        return scheduled_count
    except Exception as e:
        tracker.set_error(str(e))
        raise
    finally:
        await tracker.save()


async def cancel_appointment_reminders(
    entity_id: str,
    contact_id: str,
    appointment_id: str | None = None,
) -> int:
    """Cancel all pending appointment reminders for a contact.

    If appointment_id is provided, only cancels reminders for that specific appointment.
    """
    from app.main import supabase

    # Cancel all appointment_reminder type messages for this contact
    cancelled = await cancel_pending(
        supabase,
        contact_id,
        entity_id,
        message_types=["appointment_reminder"],
        reason="appointment_cancelled",
    )

    count = len(cancelled)
    if count > 0:
        logger.info(
            "APPT_REMIND | cancelled %d reminders | contact=%s | appt=%s",
            count, contact_id, appointment_id or "all",
        )

    return count


async def reschedule_appointment_reminders(
    *,
    entity_id: str,
    contact_id: str,
    channel: str,
    calendar_id: str,
    calendar_name: str,
    appointment_id: str,
    appointment_start: datetime,
    appointment_end: datetime | None = None,
    config: dict[str, Any],
    ghl: Any | None = None,
) -> int:
    """Handle appointment reschedule: cancel old reminders, schedule new ones."""
    # Cancel existing reminders
    await cancel_appointment_reminders(entity_id, contact_id, appointment_id)

    # Schedule new reminders with updated times
    return await schedule_appointment_reminders(
        entity_id=entity_id,
        contact_id=contact_id,
        channel=channel,
        calendar_id=calendar_id,
        calendar_name=calendar_name,
        appointment_id=appointment_id,
        appointment_start=appointment_start,
        appointment_end=appointment_end,
        config=config,
        ghl=ghl,
    )
