"""Tool: reschedule an existing appointment via GHL API.

Validates new slot availability before updating.
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

UPDATE_APPOINTMENT_DEF = {
    "type": "function",
    "function": {
        "name": "update_appointment",
        "description": (
            "Reschedule an existing appointment to a new time. "
            "Use the Event ID from the Appointment Status section as the eventId. "
            "The new time must be available (check get_available_slots first)."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "eventId": {
                    "type": "string",
                    "description": "The Event ID from the Appointment Status section",
                },
                "newStartTime": {
                    "type": "string",
                    "description": "New ISO 8601 datetime with timezone offset. Must match an available slot. Example: 2026-01-06T11:30:00-05:00",
                },
            },
            "required": ["eventId", "newStartTime"],
        },
    },
}


async def update_appointment(
    eventId: str,
    newStartTime: str,
    ghl: GHLClient,
    calendar_id: str,
    contact_id: str = "",
    tz_name: str = "America/Chicago",
    is_test_mode: bool = False,
    entity_id: str = "",
    channel: str = "",
    lead_id: str | None = None,
    **kwargs: Any,
) -> dict[str, Any]:
    """Reschedule appointment after validating new slot."""
    tool_input = {"eventId": eventId, "newStartTime": newStartTime}

    try:
        # Test mode — return mock response without calling GHL API
        if is_test_mode:
            local_str = _to_local_time(newStartTime, tz_name)
            result = {
                "ok": True,
                "appointment": {"id": eventId, "startTime": newStartTime},
                "message": f"[TEST MODE] Appointment rescheduled to {local_str}",
            }
            await _log_execution(contact_id, entity_id, tool_input, result, channel, is_test_mode, lead_id)
            return result

        # Validate new slot
        req_dt = datetime.fromisoformat(newStartTime)
        day_start = req_dt.replace(hour=0, minute=0, second=0)
        day_end = req_dt.replace(hour=23, minute=59, second=59)
        start_ms = int(day_start.timestamp() * 1000)
        end_ms = int(day_end.timestamp() * 1000)

        slots_resp = await ghl.get_free_slots(calendar_id, start_ms, end_ms, tz_name)

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
                "requested": newStartTime,
                "available_slots": all_slots[:10],
                "message": (
                    f"The time {newStartTime} is not available. "
                    f"Here are some available slots: {', '.join(all_slots[:5])}"
                ),
            }
            await _log_execution(contact_id, entity_id, tool_input, result, channel, is_test_mode, lead_id)
            return result

        api_result = await ghl.update_appointment(eventId, {
            "calendarId": calendar_id,
            "startTime": newStartTime,
            "appointmentStatus": "confirmed",
        })

        local_str = _to_local_time(newStartTime, tz_name)
        result = {
            "ok": True,
            "appointment": api_result,
            "message": f"Appointment rescheduled to {local_str}",
        }
        await _log_execution(contact_id, entity_id, tool_input, result, channel, is_test_mode, lead_id)
        return result

    except Exception as e:
        logger.warning("Update appointment failed: %s", e, exc_info=True)
        result = {"ok": False, "error": str(e)}
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
            "tool_name": "update_appointment",
            "tool_input": json.dumps(tool_input),
            "tool_output": json.dumps(tool_output),
            "channel": channel,
            "execution_id": None,
            "test_mode": test_mode,
            "lead_id": lead_id,
        })
    except Exception:
        logger.warning("Failed to log update_appointment execution", exc_info=True)
