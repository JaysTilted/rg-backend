"""Tool: fetch a contact's appointments from GHL."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

from zoneinfo import ZoneInfo

from app.services.ghl_client import GHLClient
from app.services.postgres_client import postgres

logger = logging.getLogger(__name__)


def _to_local(iso_str: str, tz: ZoneInfo) -> str:
    """Convert ISO 8601 string to human-readable local time.

    GHL returns naive datetimes in the location timezone (which should
    match the client's configured timezone ``tz``).  If the string has
    no offset we interpret it as already being in ``tz``.
    """
    if not iso_str:
        return ""
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            # Naive = already in the client's configured timezone
            dt = dt.replace(tzinfo=tz)
        local = dt.astimezone(tz)
        return local.strftime("%A, %B %d, %Y at %I:%M %p").replace(" 0", " ")
    except (ValueError, TypeError):
        return iso_str

GET_APPOINTMENTS_DEF = {
    "type": "function",
    "function": {
        "name": "get_appointments",
        "description": (
            "Get all appointments for this contact. "
            "Use this to check existing bookings before rescheduling or canceling. "
            "Returns appointments (with their eventId) needed for update/cancel operations."
        ),
        "parameters": {
            "type": "object",
            "properties": {},
        },
    },
}


async def get_appointments(
    ghl: GHLClient,
    contact_id: str,
    tz_name: str = "America/Chicago",
    entity_id: str = "",
    channel: str = "",
    is_test_mode: bool = False,
    lead_id: str | None = None,
    **kwargs: Any,
) -> dict[str, Any]:
    """Fetch all appointments for the contact."""
    tool_input: dict[str, Any] = {}

    try:
        # Resolve timezone for human-readable display
        try:
            tz = ZoneInfo(tz_name)
        except (KeyError, Exception):
            tz = ZoneInfo("America/Chicago")

        events = await ghl.get_appointments(contact_id)

        if not events:
            result: dict[str, Any] = {"result": "No appointments found for this contact.", "events": []}
        else:
            formatted = []
            for e in events:
                start_raw = e.get("startTime", "")
                end_raw = e.get("endTime", "")
                formatted.append({
                    "id": e.get("id") or e.get("eventId", ""),
                    "title": e.get("title", "Appointment"),
                    "startTime": start_raw,
                    "endTime": end_raw,
                    "localStartTime": _to_local(start_raw, tz),
                    "localEndTime": _to_local(end_raw, tz),
                    "status": e.get("appointmentStatus", "unknown"),
                })

            result = {
                "result": f"Found {len(formatted)} appointment(s).",
                "events": formatted,
            }

        await _log_execution(contact_id, entity_id, tool_input, result, channel, is_test_mode, lead_id)
        return result

    except Exception as e:
        logger.warning("Get appointments failed: %s", e, exc_info=True)
        result = {"error": str(e)}
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
            "tool_name": "get_appointments",
            "tool_input": json.dumps(tool_input),
            "tool_output": json.dumps(tool_output, default=str),
            "channel": channel,
            "execution_id": None,
            "test_mode": test_mode,
            "lead_id": lead_id,
        })
    except Exception:
        logger.warning("Failed to log get_appointments execution", exc_info=True)
