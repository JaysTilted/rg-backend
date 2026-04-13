"""Tool: cancel an appointment via GHL API."""

from __future__ import annotations

import json
import logging
from typing import Any

from app.services.ghl_client import GHLClient
from app.services.postgres_client import postgres

logger = logging.getLogger(__name__)

CANCEL_APPOINTMENT_DEF = {
    "type": "function",
    "function": {
        "name": "cancel_appointment",
        "description": (
            "Cancel an existing appointment. "
            "Use the Event ID from the Appointment Status section as the eventId. "
            "Confirm with the lead before canceling."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "eventId": {
                    "type": "string",
                    "description": "The Event ID from the Appointment Status section",
                },
            },
            "required": ["eventId"],
        },
    },
}


async def cancel_appointment(
    eventId: str,
    ghl: GHLClient,
    contact_id: str = "",
    is_test_mode: bool = False,
    entity_id: str = "",
    channel: str = "",
    lead_id: str | None = None,
    **kwargs: Any,
) -> dict[str, Any]:
    """Cancel an appointment via GHL."""
    tool_input = {"eventId": eventId}

    try:
        # Test mode — return mock response without calling GHL API
        if is_test_mode:
            result = {
                "ok": True,
                "appointment": {"id": eventId, "status": "cancelled"},
                "message": "[TEST MODE] Appointment cancelled successfully.",
            }
            await _log_execution(contact_id, entity_id, tool_input, result, channel, is_test_mode, lead_id)
            return result

        api_result = await ghl.cancel_appointment(eventId)
        result = {
            "ok": True,
            "appointment": api_result,
            "message": "Appointment cancelled successfully.",
        }
        await _log_execution(contact_id, entity_id, tool_input, result, channel, is_test_mode, lead_id)
        return result

    except Exception as e:
        logger.warning("Cancel appointment failed: %s", e, exc_info=True)
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
            "tool_name": "cancel_appointment",
            "tool_input": json.dumps(tool_input),
            "tool_output": json.dumps(tool_output),
            "channel": channel,
            "execution_id": None,
            "test_mode": test_mode,
            "lead_id": lead_id,
        })
    except Exception:
        logger.warning("Failed to log cancel_appointment execution", exc_info=True)
