"""Tool: create an appointment via GHL calendar API.

Validates the requested slot is available before booking.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

from zoneinfo import ZoneInfo

from app.services.ghl_client import GHLClient
from app.services.postgres_client import postgres

logger = logging.getLogger(__name__)


def _to_local_time(iso_str: str, tz_name: str) -> str:
    """Convert ISO 8601 string to human-readable local time.

    GHL returns naive datetimes in the location timezone.  If the string
    has no offset we interpret it as already in ``tz_name``.
    """
    try:
        tz = ZoneInfo(tz_name)
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=tz)
        local = dt.astimezone(tz)
        return local.strftime("%A, %B %d, %Y at %I:%M %p").replace(" 0", " ")
    except (ValueError, TypeError, KeyError):
        return iso_str

BOOK_APPOINTMENT_DEF = {
    "type": "function",
    "function": {
        "name": "book_appointment",
        "description": (
            "Books an appointment at a specific time slot. "
            "The startTime must match an available slot from get_available_slots."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "calendar_id": {
                    "type": "string",
                    "description": "The calendar ID to book the appointment on.",
                },
                "startTime": {
                    "type": "string",
                    "description": (
                        "ISO 8601 datetime with timezone offset. Must match an available slot. "
                        "Example: 2026-01-05T11:00:00-05:00"
                    ),
                },
            },
            "required": ["calendar_id", "startTime"],
        },
    },
}


async def book_appointment(
    startTime: str,
    ghl: GHLClient,
    contact_id: str,
    calendar_id: str = "",
    tz_name: str = "America/Chicago",
    is_test_mode: bool = False,
    entity_id: str = "",
    channel: str = "",
    lead_id: str | None = None,
    skip_pre_validation: bool = False,
    **kwargs: Any,
) -> dict[str, Any]:
    """Book an appointment after validating slot availability.

    Args:
        skip_pre_validation: If True, skip slot availability check
            and book directly. If GHL returns error, THEN fetch alternatives.
    """

    # ── Qualification gate ──
    # If service_config has required qualifications, lead must be "qualified" to book
    service_config = kwargs.get("service_config")
    qualification_status = kwargs.get("qualification_status", "undetermined")

    if service_config:
        from app.text_engine.qualification import _has_required_qualifications
        if _has_required_qualifications(service_config) and qualification_status != "qualified":
            # Build missing criteria feedback for the agent
            qual_notes = kwargs.get("qualification_notes")
            missing = []
            if isinstance(qual_notes, dict):
                for c in qual_notes.get("criteria", []):
                    if c["status"] != "confirmed":
                        missing.append(c["name"])
            missing_str = ", ".join(missing) if missing else "unknown -- qualification not yet evaluated"
            return {
                "error": "lead_not_qualified",
                "qualification_status": qualification_status,
                "missing_criteria": missing,
                "message": f"Cannot book -- lead qualification status is '{qualification_status}'. "
                           f"Required criteria not yet confirmed: {missing_str}. "
                           f"Continue qualifying the lead before booking.",
            }

    # LLM may pass calendar_id directly; fall back to shared kwargs
    effective_cal_id = calendar_id or kwargs.get("calendar_id", "")
    tool_input = {"startTime": startTime, "calendar_id": effective_cal_id}

    try:
        # Test mode — return mock response without calling GHL API
        if is_test_mode:
            local_str = _to_local_time(startTime, tz_name)
            result = {
                "ok": True,
                "appointment": {"id": "test_appointment_id", "startTime": startTime},
                "message": f"[TEST MODE] Appointment confirmed for {local_str}",
            }
            await _log_execution(contact_id, entity_id, tool_input, result, channel, is_test_mode, lead_id)
            return result

        # Parse requested time
        req_dt = datetime.fromisoformat(startTime)

        # Pre-validation: check slot availability (skipped for voice calls)
        if not skip_pre_validation:
            day_start = req_dt.replace(hour=0, minute=0, second=0)
            day_end = req_dt.replace(hour=23, minute=59, second=59)
            start_ms = int(day_start.timestamp() * 1000)
            end_ms = int(day_end.timestamp() * 1000)

            slots_resp = await ghl.get_free_slots(effective_cal_id, start_ms, end_ms, tz_name)

            req_ts = req_dt.timestamp()
            slot_available = False
            all_slots: list[str] = []

            for date_key, date_data in slots_resp.items():
                if date_key in ("_metadata", "traceId"):
                    continue
                slot_list = date_data if isinstance(date_data, list) else date_data.get("slots", [])
                for slot in slot_list:
                    all_slots.append(slot)
                    try:
                        slot_dt = datetime.fromisoformat(slot.replace("Z", "+00:00"))
                        if abs(slot_dt.timestamp() - req_ts) < 60:
                            slot_available = True
                    except (ValueError, TypeError):
                        continue

            if not slot_available:
                result = {
                    "ok": False,
                    "reason": "slot_unavailable",
                    "requested": startTime,
                    "available_slots": all_slots[:10],
                    "message": (
                        f"The time {startTime} is not available. "
                        f"Here are some available slots: {', '.join(all_slots[:5])}"
                    ),
                }
                await _log_execution(contact_id, entity_id, tool_input, result, channel, is_test_mode, lead_id)
                return result

        # Book it
        api_result = await ghl.book_appointment({
            "calendarId": effective_cal_id,
            "locationId": ghl.location_id,
            "contactId": contact_id,
            "startTime": startTime,
            "appointmentStatus": "confirmed",
            "toNotify": True,
        })

        local_str = _to_local_time(startTime, tz_name)
        result = {
            "ok": True,
            "appointment": api_result,
            "message": f"Appointment confirmed for {local_str}",
        }
        await _log_execution(contact_id, entity_id, tool_input, result, channel, is_test_mode, lead_id)
        return result

    except Exception as e:
        logger.warning("Book appointment failed: %s", e, exc_info=True)

        # Voice path (Option C): if booking failed and we skipped validation,
        # fetch alternatives now so the AI can offer different times
        if skip_pre_validation:
            try:
                req_dt_fallback = datetime.fromisoformat(startTime)
                day_s = req_dt_fallback.replace(hour=0, minute=0, second=0)
                day_e = req_dt_fallback.replace(hour=23, minute=59, second=59)
                s_ms = int(day_s.timestamp() * 1000)
                e_ms = int(day_e.timestamp() * 1000)
                fallback_slots = await ghl.get_free_slots(effective_cal_id, s_ms, e_ms, tz_name)
                alt_slots: list[str] = []
                for dk, dd in fallback_slots.items():
                    if dk in ("_metadata", "traceId"):
                        continue
                    sl = dd if isinstance(dd, list) else dd.get("slots", [])
                    alt_slots.extend(sl[:5])
                if alt_slots:
                    result = {
                        "ok": False,
                        "reason": "slot_unavailable",
                        "requested": startTime,
                        "available_slots": alt_slots[:10],
                        "message": f"That time just got taken. Available: {', '.join(_to_local_time(s, tz_name) for s in alt_slots[:5])}",
                    }
                    await _log_execution(contact_id, entity_id, tool_input, result, channel, is_test_mode, lead_id)
                    return result
            except Exception:
                pass

        result = {"ok": False, "error": str(e), "message": "Something went wrong with the booking. Let me try again or suggest another time."}
        await _log_execution(contact_id, entity_id, tool_input, result, channel, is_test_mode, lead_id)
        return result


async def _log_execution(
    session_id: str, entity_id: str, tool_input: dict, tool_output: dict,
    channel: str, test_mode: bool, lead_id: str | None,
) -> None:
    try:
        await postgres.log_tool_execution({
            "session_id": session_id,
            "client_id": entity_id,  # DB column is client_id; value comes from entity_id
            "tool_name": "book_appointment",
            "tool_input": json.dumps(tool_input),
            "tool_output": json.dumps(tool_output),
            "channel": channel,
            "execution_id": None,
            "test_mode": test_mode,
            "lead_id": lead_id,
        })
    except Exception:
        logger.warning("Failed to log book_appointment execution", exc_info=True)
