"""Booking Logger — logs GHL appointment events into Supabase.

Migrated from n8n workflow "GHL Bookings into SupaBase" (zNrJpSDmEaGo1bZ0).

GHL calls this when an appointment is created, confirmed, or cancelled.
The workflow:
  1. Resolves entity (client vs personal bot)
  2. Syncs GHL conversation history
  3. Checks if AI booked the appointment (via tool_executions)
  4. Determines after-hours status
  5. Inserts or updates the booking record
  6. Returns metadata for GHL opportunity custom fields
"""

import asyncio
import json
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from prefect import flow

from app.models import BookingWebhookBody
from app.services.ghl_client import GHLClient
from app.services.supabase_client import supabase
from app.services.postgres_client import postgres
from app.services.workflow_tracker import WorkflowTracker
from app.text_engine.conversation_sync import run_conversation_sync

logger = logging.getLogger(__name__)

# Channel names returned to GHL for opportunity custom fields
_CHANNEL_MAP = {
    "sms": "SMS",
    "direct": "Direct",
    "ai_assisted": "AI Assisted",
    "email": "Email",
    "whatsapp": "WhatsApp",
    "facebook_messenger": "Facebook",
    "instagram_dm": "Instagram",
    "live_chat": "Web Chat",
    "imessage": "iMessage",
}


def _configured_calendar_length_minutes(
    config: dict,
    calendar_name: str,
    calendar_id: str = "",
) -> int | None:
    """Resolve configured appointment length from the default setter calendar config."""
    try:
        from app.text_engine.agent_compiler import resolve_setter

        sc = config.get("system_config") or {}
        setter = resolve_setter(sc, "")
        if not setter:
            return None

        calendars = setter.get("booking", {}).get("calendars", []) or []
        for cal in calendars:
            if not isinstance(cal, dict):
                continue
            cal_id = cal.get("calendar_id") or cal.get("id")
            if calendar_id and cal_id == calendar_id:
                value = cal.get("appointment_length_minutes")
                return int(value) if value not in (None, "") else None
            if calendar_name and cal.get("name") == calendar_name:
                value = cal.get("appointment_length_minutes")
                return int(value) if value not in (None, "") else None
    except Exception:
        logger.warning("BOOKING_LOG | configured calendar length lookup failed", exc_info=True)
    return None


# ============================================================================
# AFTER-HOURS CHECK
# ============================================================================


def _check_after_hours(config: dict) -> bool:
    """Determine if current time is outside business hours.

    Reads from business_schedule JSONB. Returns False if not configured.
    """
    from app.text_engine.utils import extract_business_hours

    hours_start, hours_end, business_days = extract_business_hours(config)
    tz_name = config.get("timezone")

    if (
        not hours_start
        or not hours_end
        or not business_days
        or len(business_days) == 0
        or not tz_name
    ):
        return False

    try:
        tz = ZoneInfo(tz_name)
    except (KeyError, Exception):
        return False

    now = datetime.now(tz)
    day_names = [
        "Monday", "Tuesday", "Wednesday", "Thursday",
        "Friday", "Saturday", "Sunday",
    ]
    current_day = day_names[now.weekday()]

    if current_day not in business_days:
        return True

    current_time = now.strftime("%H:%M:%S")
    return current_time < hours_start or current_time > hours_end


# ============================================================================
# AI DETECTION
# ============================================================================


async def _check_ai_booked(
    contact_id: str, entity_id: str, appointment_id: str
) -> tuple[bool, str, str]:
    """Check if AI booked this appointment via tool_executions.

    Two-tier approach:
      1. Primary: Match appointment ID in tool_output (exact, no time window)
      2. Fallback: 5-minute recency window (catches edge cases)

    Returns (ai_booked: bool, channel: str, detection_tier: str).
    """
    if not appointment_id:
        return False, "direct", "none"

    # Query recent book_appointment executions for this contact+entity
    rows = await postgres.chat_pool.fetch(
        "SELECT id, channel, tool_output "
        "FROM tool_executions "
        "WHERE session_id = $1 "
        "AND client_id = $2 "
        "AND tool_name = 'book_appointment' "
        "ORDER BY created_at DESC "
        "LIMIT 10",
        contact_id,
        entity_id,
    )

    if not rows:
        return False, "direct", "none"

    # Tier 1: Match by appointment ID in tool_output
    for row in rows:
        tool_output = row["tool_output"]
        if not tool_output:
            continue

        if isinstance(tool_output, str):
            try:
                tool_output = json.loads(tool_output)
            except (json.JSONDecodeError, TypeError):
                continue

        # Successful bookings have "Appointment Details" as a JSON string
        details_str = tool_output.get("Appointment Details")
        if not details_str:
            continue

        try:
            details = json.loads(details_str) if isinstance(details_str, str) else details_str
            if details.get("id") == appointment_id:
                channel = row["channel"] or "direct"
                return True, channel, "appointment_id_match"
        except (json.JSONDecodeError, TypeError):
            continue

    # Tier 2: Fallback — 5-minute recency window
    fallback_row = await postgres.chat_pool.fetchrow(
        "SELECT id, channel "
        "FROM tool_executions "
        "WHERE session_id = $1 "
        "AND client_id = $2 "
        "AND tool_name = 'book_appointment' "
        "AND created_at > now() - interval '5 minutes' "
        "ORDER BY created_at DESC "
        "LIMIT 1",
        contact_id,
        entity_id,
    )

    if fallback_row:
        return True, fallback_row["channel"] or "direct", "5min_recency_window"

    return False, "direct", "none"


# ============================================================================
# MAIN WORKFLOW
# ============================================================================


@flow(name="booking-logger", retries=0)
async def process_booking_log(
    client_id: str, body: BookingWebhookBody
) -> dict:
    """Log a GHL appointment event into Supabase.

    Returns JSON response for GHL opportunity custom field updates.
    """
    contact_id = body.resolved_contact_id

    # Workflow tracker
    tracker = WorkflowTracker(
        "booking_logger",
        entity_id=client_id,
        ghl_contact_id=contact_id,
        trigger_source="webhook",
    )

    # ── 1. Entity resolution ──
    try:
        config = await supabase.resolve_entity(client_id)
    except ValueError:
        logger.error("BOOKING_LOG | entity not found: %s", client_id)
        tracker.set_error(f"Entity {client_id} not found")
        await tracker.save()
        return {"error": f"Entity {client_id} not found"}

    entity_id = config.get("id", client_id)
    tracker.entity_id = entity_id  # Re-sync after resolution
    slug = config.get("slug") or config.get("name", client_id[:8])
    existing_booking = None
    ai_booked = False
    booking_channel_display = "direct"
    booking_type = "manual"
    after_hours = False
    logger.info(
        "BOOKING_LOG | entity=%s | contact=%s | appt=%s | status=%s",
        slug, contact_id, body.appointmentID, body.status,
    )
    try:

        # ── 2. Parallel fetch: lead, existing booking, AI detection, conv sync ──
        ghl = GHLClient(
            api_key=config.get("ghl_api_key", ""),
            location_id=config.get("ghl_location_id", ""),
        )

        async def _conv_sync():
            chat_table = config.get("chat_history_table_name", "")
            if not chat_table:
                return None
            try:
                return await run_conversation_sync(
                    contact_id=contact_id,
                    ghl=ghl,
                    chat_table=chat_table,
                    entity_id=entity_id,
                    config=config,
                    workflow_run_id=tracker.run_id,
                )
            except Exception:
                logger.warning("BOOKING_LOG | conv sync failed — continuing", exc_info=True)
                return None

        async def _noop():
            return None

        lead_task = supabase.get_lead(contact_id, entity_id)
        existing_task = (
            supabase.find_booking_by_appointment(body.appointmentID, entity_id)
            if body.appointmentID
            else _noop()
        )
        ai_task = _check_ai_booked(contact_id, entity_id, body.appointmentID)
        sync_task = _conv_sync()

        lead, existing_booking, (ai_booked, raw_channel, detection_tier), sync_result = await asyncio.gather(
            lead_task, existing_task, ai_task, sync_task,
        )
        _conv_sync_result = sync_result

        if lead and lead.get("id"):
            tracker.set_lead_id(lead["id"])

        # ── 3. Compute derived fields ──
        after_hours = _check_after_hours(config)
        fk = "entity_id"

        # Determine booking channel:
        #   - AI booked directly → channel from tool_execution (sms, email, etc.)
        #   - Not AI booked, but lead replied (AI assisted) → "ai_assisted"
        #   - Not AI booked, no lead interaction → "direct"
        if ai_booked:
            booking_channel = raw_channel
        elif lead and lead.get("last_reply_datetime"):
            booking_channel = "ai_assisted"
        else:
            booking_channel = "direct"

        booking_channel_display = _CHANNEL_MAP.get(booking_channel, booking_channel)

        # Billing attribution (single source of truth)
        if ai_booked:
            booking_type = "ai_direct"
        elif lead and lead.get("last_reply_datetime"):
            booking_type = "ai_assisted"
        else:
            booking_type = "manual"

        # ── 4. Insert or update ──
        _reminders_result = None
        _appt_end_str = body.appointmentEndTime or ""
        cal_id = ""
        cal_name = body.calendarName if hasattr(body, "calendarName") else ""
        appt_start_str = body.appointmentDateTime or ""

        if existing_booking:
            # Update existing booking (reschedule / status change)
            updates = {
                "status": body.status or "Other",
                "appointment_datetime": body.appointmentDateTime or None,
                "appointment_timezone": (
                    body.appointmentTimezone or config.get("timezone")
                ),
            }
            await supabase.update_booking(existing_booking["id"], updates)
            logger.info(
                "BOOKING_LOG | UPDATED | booking=%s | status=%s",
                existing_booking["id"], body.status,
            )
        else:
            # New booking
            booking_data = {
                fk: entity_id,
                "ghl_contact_id": contact_id,
                "ghl_appointment_id": body.appointmentID or None,
                "ghl_calendar_name": body.calendarName or None,
                "lead_source": body.leadSource or "Other",
                "appointment_datetime": body.appointmentDateTime or None,
                "status": body.status or "Other",
                "after_hours": after_hours,
                "appointment_timezone": (
                    body.appointmentTimezone or config.get("timezone")
                ),
                "booking_channel": booking_channel,
                "ai_booked_directly": ai_booked,
                "booking_type": booking_type,
            }
            if lead:
                booking_data["lead_id"] = lead.get("id")
            if body.interest:
                booking_data["interest"] = body.interest

            _new_booking = await supabase.create_booking(booking_data)
            _booking_record_id = _new_booking.get("id") if isinstance(_new_booking, dict) else None
            logger.info(
                "BOOKING_LOG | CREATED | contact=%s | ai=%s | channel=%s | type=%s | after_hours=%s",
                contact_id, ai_booked, booking_channel, booking_type, after_hours,
            )

            # Cancel pending follow-ups when booking is confirmed (lead doesn't need FUs anymore)
            _fu_cancelled = False
            try:
                from app.services.message_scheduler import cancel_pending, update_ghl_followup_field
                await cancel_pending(
                    supabase, contact_id, entity_id,
                    message_types=["followup", "smart_followup"],
                    reason="booking_confirmed",
                )
                await update_ghl_followup_field(ghl, contact_id)  # Clear GHL field
                _fu_cancelled = True
            except Exception as _cancel_err:
                logger.warning("BOOKING_LOG | FU cancel failed (non-blocking): %s", _cancel_err)

            # Reschedule reactivation (booking is an activity event)
            _react_rescheduled = False
            try:
                from app.workflows.reactivation_scheduler import reschedule_reactivation
                await reschedule_reactivation(entity_id, contact_id)
                _react_rescheduled = True
            except Exception as _react_err:
                logger.warning("BOOKING_LOG | reactivation reschedule failed (non-blocking): %s", _react_err)

            # Schedule appointment reminders (if template exists for this calendar)
            try:
                from app.workflows.appointment_reminder_scheduler import (
                    schedule_appointment_reminders,
                    cancel_appointment_reminders,
                    reschedule_appointment_reminders,
                )
                from app.webhooks.standard_parser import extract_calendar
                if body.appointmentID and not cal_id:
                    try:
                        appt_data = await ghl.get_appointment(body.appointmentID)
                        if appt_data:
                            cal_id = appt_data.get("calendarId", "")
                            if not cal_id:
                                logger.warning("BOOKING_LOG | no calendarId in GHL appointment | appt=%s", body.appointmentID)
                            if not cal_name:
                                # Fetch calendar name from the calendars list
                                try:
                                    all_cals = await ghl.get_calendars()
                                    matched_cal = next((c for c in all_cals if c.get("id") == cal_id), None)
                                    if matched_cal:
                                        cal_name = matched_cal.get("name", "")
                                except Exception:
                                    pass
                            if not appt_start_str:
                                appt_start_str = appt_data.get("startTime", "")
                            if not _appt_end_str:
                                _appt_end_str = appt_data.get("endTime", "")
                    except Exception:
                        pass

                if appt_start_str and cal_id:
                    # Parse appointment start time
                    from datetime import datetime as dt
                    try:
                        appt_start = dt.fromisoformat(appt_start_str.replace("Z", "+00:00"))
                    except (ValueError, AttributeError):
                        appt_start = None

                    if appt_start:
                        status_lower = (body.status or "").lower()

                        if status_lower in ("confirmed", "booked", "showed"):
                            # Schedule reminders (or reschedule if already exist)
                            await reschedule_appointment_reminders(
                                entity_id=entity_id,
                                contact_id=contact_id,
                                channel="SMS",
                                calendar_id=cal_id,
                                calendar_name=cal_name,
                                appointment_id=body.appointmentID,
                                appointment_start=appt_start,
                                config=config,
                                ghl=ghl,
                            )
                            _reminders_result = "scheduled"
                        elif status_lower in ("cancelled", "no_show", "noshow"):
                            # Cancel all reminders
                            await cancel_appointment_reminders(entity_id, contact_id, body.appointmentID)
                            _reminders_result = "cancelled"
                        else:
                            _reminders_result = f"unhandled_status:{body.status}"
                            logger.info("BOOKING_LOG | unhandled status for reminders: %s", body.status)
            except Exception as _remind_err:
                _reminders_result = f"error:{_remind_err}"
                logger.warning("BOOKING_LOG | appointment reminder scheduling failed (non-blocking): %s", _remind_err)

        # ── 4b. Post-appointment automation — cancel/schedule (runs for ALL bookings) ──
        if existing_booking and _reminders_result is None:
            try:
                from app.workflows.appointment_reminder_scheduler import (
                    cancel_appointment_reminders,
                    reschedule_appointment_reminders,
                )
                if body.appointmentID and not cal_id:
                    try:
                        appt_data = await ghl.get_appointment(body.appointmentID)
                        if appt_data:
                            cal_id = appt_data.get("calendarId", "")
                            if not cal_id:
                                logger.warning("BOOKING_LOG | no calendarId in GHL appointment | appt=%s", body.appointmentID)
                            if not cal_name:
                                try:
                                    all_cals = await ghl.get_calendars()
                                    matched_cal = next((c for c in all_cals if c.get("id") == cal_id), None)
                                    if matched_cal:
                                        cal_name = matched_cal.get("name", "")
                                except Exception:
                                    pass
                            if not appt_start_str:
                                appt_start_str = appt_data.get("startTime", "")
                            if not _appt_end_str:
                                _appt_end_str = appt_data.get("endTime", "")
                    except Exception:
                        pass

                if appt_start_str and cal_id:
                    from datetime import datetime as dt
                    try:
                        appt_start = dt.fromisoformat(appt_start_str.replace("Z", "+00:00"))
                    except (ValueError, AttributeError):
                        appt_start = None

                    if appt_start:
                        status_lower = (body.status or "").lower()
                        if status_lower in ("confirmed", "booked", "showed"):
                            await reschedule_appointment_reminders(
                                entity_id=entity_id,
                                contact_id=contact_id,
                                channel="SMS",
                                calendar_id=cal_id,
                                calendar_name=cal_name,
                                appointment_id=body.appointmentID,
                                appointment_start=appt_start,
                                config=config,
                                ghl=ghl,
                            )
                            _reminders_result = "scheduled"
                        elif status_lower in ("cancelled", "no_show", "noshow"):
                            await cancel_appointment_reminders(entity_id, contact_id, body.appointmentID)
                            _reminders_result = "cancelled"
                        else:
                            _reminders_result = f"unhandled_status:{body.status}"
                            logger.info("BOOKING_LOG | unhandled status for reminders: %s", body.status)
            except Exception as _remind_err:
                _reminders_result = f"error:{_remind_err}"
                logger.warning("BOOKING_LOG | appointment reminder scheduling failed (non-blocking): %s", _remind_err)

        _pa_result = None
        try:
            from app.text_engine.agent_compiler import resolve_setter
            from app.services.message_scheduler import (
                schedule_message as _pa_schedule,
                cancel_pending as _pa_cancel,
                enforce_send_window as _pa_window,
                apply_timing_jitter as _pa_jitter,
            )
            from app.main import supabase as _pa_sb

            sc = config.get("system_config") or {}
            pa_setter = resolve_setter(sc, "")
            if pa_setter:
                pb_config = (
                    pa_setter.get("conversation", {})
                    .get("reply", {})
                    .get("sections", {})
                    .get("post_booking", {})
                )
                status_lower_pa = (body.status or "").lower()

                if status_lower_pa in ("cancelled", "no_show", "noshow"):
                    # Cancel any pending post-appointment messages
                    _cancelled = await _pa_cancel(
                        _pa_sb, contact_id, entity_id,
                        message_types=["post_appointment"],
                        reason=f"booking_{status_lower_pa}",
                    )
                    _pa_result = f"cancelled:{len(_cancelled)}" if _cancelled else "cancel_noop"

                elif pb_config.get("proactive_outreach") and status_lower_pa in ("confirmed", "booked", "showed"):
                    # Resolve delay
                    delay_cfg = pb_config.get("post_appointment_delay", {"value": 1, "unit": "days"})
                    delay_val = delay_cfg.get("value", 1)
                    delay_unit = delay_cfg.get("unit", "days")
                    delay_minutes = delay_val * 1440 if delay_unit == "days" else delay_val

                    # Parse appointment end time (from GHL API data or fallback)
                    from datetime import datetime as dt
                    _pa_appt_end = None
                    _pa_appt_start_str = body.appointmentDateTime or ""
                    _pa_appt_end_str = body.appointmentEndTime or ""

                    if _pa_appt_end_str:
                        try:
                            _pa_appt_end = dt.fromisoformat(_pa_appt_end_str.replace("Z", "+00:00"))
                        except (ValueError, AttributeError):
                            _pa_appt_end = None

                    # Try GHL API for end time (appt_data may be available from reminders block)
                    if body.appointmentID and not _pa_appt_end:
                        try:
                            _pa_appt_data = await ghl.get_appointment(body.appointmentID)
                            if _pa_appt_data:
                                _pa_end_str = _pa_appt_data.get("endTime", "")
                                if _pa_end_str:
                                    _pa_appt_end = dt.fromisoformat(_pa_end_str.replace("Z", "+00:00"))
                                if not _pa_appt_start_str:
                                    _pa_appt_start_str = _pa_appt_data.get("startTime", "")
                        except Exception:
                            pass

                    # Fallback 1: configured calendar duration from setter booking config
                    if not _pa_appt_end and _pa_appt_start_str:
                        try:
                            _configured_len = _configured_calendar_length_minutes(
                                config,
                                body.calendarName or "",
                                cal_id,
                            )
                            if _configured_len:
                                _pa_start = dt.fromisoformat(_pa_appt_start_str.replace("Z", "+00:00"))
                                _pa_appt_end = _pa_start + timedelta(minutes=_configured_len)
                        except (ValueError, AttributeError):
                            pass

                    # Fallback 2: start + 60 min
                    if not _pa_appt_end and _pa_appt_start_str:
                        try:
                            _pa_start = dt.fromisoformat(_pa_appt_start_str.replace("Z", "+00:00"))
                            _pa_appt_end = _pa_start + timedelta(minutes=60)
                        except (ValueError, AttributeError):
                            pass

                    if _pa_appt_end:
                        raw_due = _pa_appt_end + timedelta(minutes=delay_minutes)

                        # Apply timing jitter (same as follow-ups)
                        delay_hours = delay_minutes / 60.0
                        fu_config = pa_setter.get("conversation", {}).get("follow_up", {})
                        fu_sections = fu_config.get("sections", {})
                        jitter_enabled = fu_sections.get("timing_jitter_enabled", True)
                        jitter_pct = fu_sections.get("timing_jitter_percent", 10) if jitter_enabled else 0
                        if jitter_pct > 0:
                            raw_due = _pa_jitter(raw_due, delay_hours, jitter_pct)

                        # Apply follow-up send window enforcement
                        fu_send_window = fu_config.get("send_window") or {}
                        due_at = _pa_window(raw_due, config, window_config=fu_send_window)

                        # Resolve interest from lead record (webhook body doesn't carry it)
                        _lead_interest = ""
                        _lead_ref = lead if 'lead' in dir() else None
                        if _lead_ref:
                            _lead_interest = _lead_ref.get("form_interest") or _lead_ref.get("interest") or ""

                        await _pa_schedule(
                            _pa_sb,
                            entity_id=entity_id,
                            contact_id=contact_id,
                            message_type="post_appointment",
                            channel="SMS",
                            due_at=due_at,
                            source="booking_confirmed",
                            triggered_by="booking_logger",
                            metadata={
                                "appointment_id": body.appointmentID or "",
                                "appointment_start": _pa_appt_start_str or "",
                                "appointment_end": (_pa_appt_end.isoformat() if _pa_appt_end else ""),
                                "calendar_name": body.calendarName or "",
                                "contact_name": body.name or "",
                                "interest": _lead_interest,
                            },
                        )
                        _pa_result = "scheduled"
                        logger.info(
                            "BOOKING_LOG | post-appointment scheduled | due=%s | delay=%d%s | contact=%s",
                            due_at.isoformat(), delay_val, delay_unit[0], contact_id,
                        )
                    else:
                        _pa_result = "no_end_time"
                else:
                    _pa_result = "disabled_or_unhandled"
        except Exception as _pa_err:
            _pa_result = f"error:{_pa_err}"
            logger.warning("BOOKING_LOG | post-appointment scheduling failed (non-blocking): %s", _pa_err)

        # ── 4c. Booking tags (from setter config) ──
        _booking_tags_added = []
        try:
            from app.text_engine.agent_compiler import resolve_setter as _bt_resolve
            _bt_sc = config.get("system_config") or {}
            _bt_setter = _bt_resolve(_bt_sc, "")
            _bt_configured = (_bt_setter or {}).get("tags", {}).get("booking") if _bt_setter else None
            if isinstance(_bt_configured, list):
                for _bt_tag in _bt_configured:
                    try:
                        await ghl.add_tag(contact_id, _bt_tag)
                        _booking_tags_added.append(_bt_tag)
                    except Exception as _bt_err:
                        logger.warning("BOOKING_LOG | Failed to add booking tag '%s': %s", _bt_tag, _bt_err)
        except Exception as _bt_outer:
            logger.warning("BOOKING_LOG | booking tag resolution failed (non-blocking): %s", _bt_outer)

        # ── 5. Booking notification ──
        _notif_sent = False
        try:
            from app.services.notifications import send_notifications
            await send_notifications(
                config=config,
                event="booking",
                context={
                    "contact_id": contact_id,
                    "contact_name": body.name or "",
                    "contact_phone": body.phone or "",
                    "status": body.status or "",
                    "ghl_location_id": config.get("ghl_location_id", ""),
                },
                entity_id=entity_id,
            )
            _notif_sent = True
        except Exception:
            logger.warning("BOOKING_LOG | notification failed", exc_info=True)

        try:
            from app.services.notifications import send_tenant_notification

            tenant_id = config.get("tenant_id")
            if tenant_id:
                await send_tenant_notification(
                    tenant_id=tenant_id,
                    event="booking",
                    context={
                        "contact_id": contact_id,
                        "contact_name": body.name or "",
                        "contact_phone": body.phone or "",
                        "status": body.status or "",
                    },
                    client_name=config.get("name") or "",
                    entity_id=entity_id,
                )
        except Exception:
            logger.warning("BOOKING_LOG | tenant notification failed", exc_info=True)

        # ── 6. Return response for GHL opportunity fields ──
        return {
            "after_hours": "Yes" if after_hours else "No",
            "booking_channel": booking_channel_display,
            "booking_type": booking_type,
            "ai_booked": "Yes" if ai_booked else "No",
            "monetary_value": config.get("average_booking_value", 0),
        }
    except Exception as e:
        tracker.set_error(str(e))
        raise
    finally:
        # Rich decisions — AI detection, booking action, side effects
        _lead = lead if 'lead' in dir() else None
        _decisions = {
            "ai_detection": {
                "ai_booked": ai_booked,
                "channel": booking_channel_display,
                "raw_channel": raw_channel if 'raw_channel' in dir() else None,
                "booking_type": booking_type,
                "detection_tier": detection_tier if 'detection_tier' in dir() else "none",
            },
            "after_hours": {"is_after_hours": after_hours},
            "booking": {
                "action": "update" if existing_booking else "create",
                "status": body.status or "",
                "appointment_id": body.appointmentID or "",
                "calendar_name": body.calendarName or "",
                "appointment_datetime": body.appointmentDateTime or "",
                "booking_record_id": _booking_record_id if '_booking_record_id' in dir() else None,
            },
            "lead": {
                "found": bool(_lead),
                "lead_id": _lead.get("id") if _lead else None,
                "source": _lead.get("source") if _lead else None,
            },
            "side_effects": {
                "followup_cancelled": _fu_cancelled if '_fu_cancelled' in dir() else None,
                "reactivation_rescheduled": _react_rescheduled if '_react_rescheduled' in dir() else None,
                "reminders_scheduled": _reminders_result if '_reminders_result' in dir() else None,
                "post_appointment_scheduled": _pa_result if '_pa_result' in dir() else None,
                "notification_sent": _notif_sent if '_notif_sent' in dir() else None,
                "booking_tags_added": _booking_tags_added if '_booking_tags_added' in dir() else [],
            },
            "conv_sync": _conv_sync_result if '_conv_sync_result' in dir() else None,
        }
        tracker.set_decisions(_decisions)

        # Runtime context — booking data and contact info
        tracker.set_runtime_context({
            "contact": {
                "name": body.name or "",
                "phone": body.phone or "",
            },
            "appointment": {
                "id": body.appointmentID or "",
                "status": body.status or "",
                "datetime": body.appointmentDateTime or "",
                "timezone": body.appointmentTimezone or config.get("timezone", ""),
                "calendar": body.calendarName or "",
                "lead_source": body.leadSource or "",
                "interest": body.interest or "",
            },
            "existing_booking": bool(existing_booking) if 'existing_booking' in dir() else False,
            "existing_booking_id": existing_booking.get("id") if ('existing_booking' in dir() and existing_booking) else None,
        })
        tracker.set_system_config(config.get("system_config") if config else None)
        await tracker.save()
