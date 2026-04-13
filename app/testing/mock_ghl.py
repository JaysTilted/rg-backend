"""Mock GHL client for direct pipeline testing.

Drop-in replacement for GHLClient. READ methods return configurable data,
WRITE methods no-op and return plausible success dicts.  Every call is
logged to ``self.calls_log`` for post-test inspection.
"""

from __future__ import annotations

import logging
from typing import Any
from uuid import uuid4

from app.testing.models import MockGHLConfig

logger = logging.getLogger(__name__)


class MockGHLClient:
    """Fake GHLClient that never touches the real GHL API."""

    def __init__(self, config: MockGHLConfig) -> None:
        self.config = config
        self.location_id = "mock-location"
        self.calls_log: list[dict[str, Any]] = []

    def _log(self, method: str, **kwargs: Any) -> None:
        self.calls_log.append({"method": method, **kwargs})

    # ------------------------------------------------------------------
    # Location
    # ------------------------------------------------------------------

    async def get_location_timezone(self) -> str | None:
        self._log("get_location_timezone")
        return self.config.timezone

    # ------------------------------------------------------------------
    # Contacts (READ)
    # ------------------------------------------------------------------

    async def get_contact(self, contact_id: str) -> dict[str, Any]:
        self._log("get_contact", contact_id=contact_id)
        return self.config.contact

    # Contacts (WRITE)

    async def update_contact(self, contact_id: str, data: dict[str, Any]) -> dict[str, Any]:
        self._log("update_contact", contact_id=contact_id, data=data)
        # Persist changes so subsequent get_contact calls see updated data
        self.config.contact.update(data)
        return {"contact": self.config.contact}

    # ------------------------------------------------------------------
    # Tags (WRITE)
    # ------------------------------------------------------------------

    async def add_tag(self, contact_id: str, tag: str) -> None:
        self._log("add_tag", contact_id=contact_id, tag=tag)
        tags = self.config.contact.get("tags", [])
        if tag not in tags:
            tags.append(tag)
            self.config.contact["tags"] = tags

    async def remove_tag(self, contact_id: str, tag: str) -> None:
        self._log("remove_tag", contact_id=contact_id, tag=tag)
        tags = self.config.contact.get("tags", [])
        if tag in tags:
            tags.remove(tag)
            self.config.contact["tags"] = tags

    # ------------------------------------------------------------------
    # Appointments (READ)
    # ------------------------------------------------------------------

    async def get_appointments(self, contact_id: str) -> list[dict[str, Any]]:
        self._log("get_appointments", contact_id=contact_id)
        return self.config.appointments

    async def get_free_slots(
        self,
        calendar_id: str,
        start_ms: int,
        end_ms: int,
        timezone: str = "America/Chicago",
    ) -> dict[str, Any]:
        self._log(
            "get_free_slots",
            calendar_id=calendar_id,
            start_ms=start_ms,
            end_ms=end_ms,
            timezone=timezone,
        )
        return self.config.free_slots

    # Appointments (WRITE)

    async def book_appointment(self, data: dict[str, Any]) -> dict[str, Any]:
        self._log("book_appointment", data=data)
        new_appt = {"id": str(uuid4()), "appointmentStatus": "confirmed", **data}
        self.config.appointments.append(new_appt)
        return new_appt

    async def update_appointment(self, event_id: str, data: dict[str, Any]) -> dict[str, Any]:
        self._log("update_appointment", event_id=event_id, data=data)
        # Update the matching appointment in-place so subsequent reads see the change
        for appt in self.config.appointments:
            if appt.get("id") == event_id:
                appt.update(data)
                return {"id": event_id, **appt}
        return {"id": event_id, **data}

    async def cancel_appointment(self, event_id: str) -> dict[str, Any]:
        self._log("cancel_appointment", event_id=event_id)
        # Mark the appointment as cancelled so subsequent reads see updated status
        for appt in self.config.appointments:
            if appt.get("id") == event_id:
                appt["appointmentStatus"] = "cancelled"
                return {"id": event_id, **appt}
        return {"id": event_id, "appointmentStatus": "cancelled"}

    # ------------------------------------------------------------------
    # Conversations (READ)
    # ------------------------------------------------------------------

    async def get_conversation_messages(self, contact_id: str) -> list[dict[str, Any]]:
        self._log("get_conversation_messages", contact_id=contact_id)
        return self.config.conversation_messages

    async def get_call_transcription(
        self, message_id: str, location_id: str | None = None,
    ) -> dict[str, Any] | None:
        self._log("get_call_transcription", message_id=message_id)
        return None

    async def get_email_by_id(self, message_id: str) -> dict[str, Any] | None:
        self._log("get_email_by_id", message_id=message_id)
        return None

    # Conversations (WRITE)

    async def send_email_reply(
        self,
        contact_id: str,
        subject: str,
        message: str,
        conversation_id: str | None = None,
        in_reply_to: str | None = None,
        thread_id: str | None = None,
        email_from: str | None = None,
    ) -> dict[str, Any]:
        self._log("send_email_reply", contact_id=contact_id, subject=subject)
        return {"id": str(uuid4()), "status": "sent"}

    # ------------------------------------------------------------------
    # Pipelines & Opportunities (READ)
    # ------------------------------------------------------------------

    async def get_pipelines(self) -> list[dict[str, Any]]:
        self._log("get_pipelines")
        return self.config.pipelines

    async def search_opportunities(
        self, contact_id: str, pipeline_id: str,
    ) -> list[dict[str, Any]]:
        self._log("search_opportunities", contact_id=contact_id, pipeline_id=pipeline_id)
        return []

    # Pipelines & Opportunities (WRITE)

    async def create_opportunity(self, data: dict[str, Any]) -> dict[str, Any]:
        self._log("create_opportunity", data=data)
        return {"id": str(uuid4()), **data}

    async def update_opportunity(
        self, opportunity_id: str, data: dict[str, Any],
    ) -> dict[str, Any]:
        self._log("update_opportunity", opportunity_id=opportunity_id, data=data)
        return {"id": opportunity_id, **data}

    # ------------------------------------------------------------------
    # Custom Fields (READ)
    # ------------------------------------------------------------------

    async def get_custom_field_defs(self) -> dict[str, str]:
        self._log("get_custom_field_defs")
        return self.config.custom_field_defs

    # Custom Fields (WRITE)

    async def set_custom_field(
        self, contact_id: str, field_key: str, value: str,
    ) -> None:
        self._log("set_custom_field", contact_id=contact_id, field_key=field_key, value=value)
        # Persist in contact's customFields so subsequent reads see the change
        custom_fields = self.config.contact.get("customFields", [])
        # Update existing or add new
        for cf in custom_fields:
            if cf.get("id") == field_key:
                cf["value"] = value
                return
        custom_fields.append({"id": field_key, "value": value})
        self.config.contact["customFields"] = custom_fields

    async def set_opportunity_custom_fields(
        self, opportunity_id: str, fields: list[dict[str, str]],
    ) -> None:
        self._log("set_opportunity_custom_fields", opportunity_id=opportunity_id, fields=fields)

    # ------------------------------------------------------------------
    # Safety net: _request should never be called in test mode
    # ------------------------------------------------------------------

    async def _request(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError(
            "MockGHLClient._request called — this should be unreachable in test mode. "
            "Check that test mode guards are working correctly."
        )
