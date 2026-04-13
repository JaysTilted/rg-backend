"""Tool: query GHL calendar for available appointment slots."""

from __future__ import annotations

import json
import logging
from datetime import datetime
from datetime import timezone as dt_timezone
from typing import Any

from app.services.ghl_client import GHLClient
from app.services.postgres_client import postgres

logger = logging.getLogger(__name__)

GET_AVAILABLE_SLOTS_DEF = {
    "type": "function",
    "function": {
        "name": "get_available_slots",
        "description": (
            "Get available appointment time slots from the calendar. "
            "Call this BEFORE booking to see what times are open. "
            "Returns available slots grouped by date."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "calendar_id": {
                    "type": "string",
                    "description": "The calendar ID to check availability for.",
                },
                "timezone": {
                    "type": "string",
                    "description": "IANA timezone (e.g. America/New_York).",
                },
                "start_date": {
                    "type": "string",
                    "description": (
                        "ISO 8601 datetime with offset. Inclusive. "
                        "Example: 2026-01-05T00:00:00-05:00"
                    ),
                },
                "end_date": {
                    "type": "string",
                    "description": (
                        "ISO 8601 datetime with offset. Exclusive. Must be after start_date. "
                        "Example: 2026-01-06T00:00:00-05:00"
                    ),
                },
            },
            "required": ["calendar_id", "timezone", "start_date", "end_date"],
        },
    },
}


async def get_available_slots(
    start_date: str,
    end_date: str,
    ghl: GHLClient,
    calendar_id: str = "",
    timezone: str = "",
    contact_id: str = "",
    tz_name: str = "America/Chicago",
    entity_id: str = "",
    channel: str = "",
    is_test_mode: bool = False,
    lead_id: str | None = None,
    **kwargs: Any,
) -> dict[str, Any]:
    """Fetch available calendar slots from GHL."""
    # LLM may pass calendar_id and timezone directly; fall back to shared kwargs
    effective_cal_id = calendar_id or kwargs.get("calendar_id", "")
    effective_tz = timezone or tz_name
    tool_input = {"start_date": start_date, "end_date": end_date, "calendar_id": effective_cal_id, "timezone": effective_tz}

    try:
        # Support both ISO 8601 with offset and YYYY-MM-DD formats
        try:
            start_dt = datetime.fromisoformat(start_date)
        except ValueError:
            start_dt = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=dt_timezone.utc)
        try:
            end_dt = datetime.fromisoformat(end_date)
        except ValueError:
            end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(
                hour=23, minute=59, second=59, tzinfo=dt_timezone.utc
            )
        start_ms = int(start_dt.timestamp() * 1000)
        end_ms = int(end_dt.timestamp() * 1000)

        result = await ghl.get_free_slots(effective_cal_id, start_ms, end_ms, effective_tz)

        await _log_execution(contact_id, entity_id, tool_input, result, channel, is_test_mode, lead_id)
        return result

    except ValueError as e:
        result = {"error": f"Invalid date format: {e}. Use YYYY-MM-DD."}
        await _log_execution(contact_id, entity_id, tool_input, result, channel, is_test_mode, lead_id)
        return result
    except Exception as e:
        logger.warning("Get slots failed: %s", e, exc_info=True)
        result = {"error": f"Failed to get available slots: {str(e)}"}
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
            "tool_name": "get_available_slots",
            "tool_input": json.dumps(tool_input),
            "tool_output": json.dumps(tool_output, default=str),
            "channel": channel,
            "execution_id": None,
            "test_mode": test_mode,
            "lead_id": lead_id,
        })
    except Exception:
        logger.warning("Failed to log get_available_slots execution", exc_info=True)
