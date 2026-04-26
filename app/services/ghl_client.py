"""GoHighLevel API client — async, with transient error retry.

One GHLClient instance per request (since auth is per-client).
Uses a shared httpx.AsyncClient for connection pooling.
Retries on 429 (rate limit), 500, 502, 503, 504, and timeouts.

Supports both PIT API keys (legacy) and OAuth access tokens (Marketplace).
When OAuth params are provided, auto-refreshes expired tokens before requests.
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Awaitable

import httpx

logger = logging.getLogger(__name__)

from app.utils.text_scrub import scrub_dashes

GHL_BASE = "https://services.leadconnectorhq.com"

# Shared connection pool — created at app startup
_http_client: httpx.AsyncClient | None = None


async def start_ghl_pool() -> None:
    """Create shared HTTP client. Call once at app startup."""
    global _http_client
    _http_client = httpx.AsyncClient(base_url=GHL_BASE, timeout=30.0)


async def stop_ghl_pool() -> None:
    """Close shared HTTP client. Call at app shutdown."""
    global _http_client
    if _http_client:
        await _http_client.aclose()
        _http_client = None


class GHLClient:
    """Per-request GHL API client. Wraps the shared connection pool with per-client auth.

    Dual-auth: OAuth access_token (preferred) or PIT api_key (legacy fallback).

    Usage (PIT — existing entities):
        ghl = GHLClient(api_key="pit-xxx", location_id="abc123")

    Usage (OAuth — Marketplace installs):
        ghl = GHLClient(
            location_id="abc123",
            access_token="oauth_token",
            refresh_token="refresh_token",
            token_expires_at=datetime(...),
            on_token_refresh=async_save_callback,
        )
    """

    def __init__(
        self,
        api_key: str = "",
        location_id: str = "",
        *,
        access_token: str = "",
        refresh_token: str = "",
        token_expires_at: datetime | None = None,
        on_token_refresh: Callable[[str, str, datetime], Awaitable[None]] | None = None,
    ) -> None:
        self.api_key = api_key
        self.location_id = location_id
        self._access_token = access_token
        self._refresh_token = refresh_token
        self._token_expires_at = token_expires_at
        self._on_token_refresh = on_token_refresh
        self._is_oauth = bool(access_token)

    def _bearer_token(self) -> str:
        if self._is_oauth and self._access_token:
            return self._access_token
        return self.api_key

    def _headers(self, version: str = "2021-07-28") -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._bearer_token()}",
            "Version": version,
            "Content-Type": "application/json",
        }

    _RETRYABLE_STATUSES = {429, 500, 502, 503, 504}
    _TOKEN_REFRESH_MARGIN = timedelta(minutes=5)

    async def _ensure_fresh_token(self) -> None:
        """Refresh OAuth access token if expired or close to expiry. No-op for PIT."""
        if not self._is_oauth or not self._refresh_token:
            return
        if self._token_expires_at is None:
            return

        now = datetime.now(timezone.utc)
        expires = self._token_expires_at
        if expires.tzinfo is None:
            expires = expires.replace(tzinfo=timezone.utc)

        if expires - self._TOKEN_REFRESH_MARGIN > now:
            return

        logger.info("GHL_CLIENT | refreshing OAuth token | location=%s", self.location_id)
        from app.marketplace.token_refresh import _refresh_token
        refreshed = await _refresh_token(self._refresh_token)
        self._access_token = refreshed["access_token"]
        self._refresh_token = refreshed.get("refresh_token", self._refresh_token)
        new_expires_in = int(refreshed.get("expires_in", 86400))
        self._token_expires_at = datetime.now(timezone.utc) + timedelta(seconds=new_expires_in)

        if self._on_token_refresh:
            await self._on_token_refresh(
                self._access_token, self._refresh_token, self._token_expires_at,
            )

    async def _request(
        self,
        method: str,
        path: str,
        version: str = "2021-07-28",
        params: dict | None = None,
        json_data: dict | None = None,
        retries: int = 3,
    ) -> httpx.Response:
        """Make a GHL API request with transient error retry + automatic logging.

        Retries on: 429, 500, 502, 503, 504 status codes and timeouts.
        Uses exponential backoff (2^attempt seconds).
        """
        await self._ensure_fresh_token()

        t0 = time.perf_counter()
        last_resp = None
        for attempt in range(retries):
            try:
                resp = await _http_client.request(
                    method,
                    path,
                    headers=self._headers(version),
                    params=params,
                    json=json_data,
                )
            except httpx.TimeoutException:
                if attempt < retries - 1:
                    wait = 2 ** attempt
                    logger.warning(
                        "GHL_HTTP | %s %s | timeout | retry in %ds (attempt %d/%d)",
                        method, path, wait, attempt + 1, retries,
                    )
                    await asyncio.sleep(wait)
                    continue
                elapsed_ms = int((time.perf_counter() - t0) * 1000)
                logger.error("GHL_HTTP | %s %s | timeout after %d retries | elapsed_ms=%d", method, path, retries, elapsed_ms)
                raise

            last_resp = resp
            if resp.status_code in self._RETRYABLE_STATUSES and attempt < retries - 1:
                wait = 2 ** attempt
                logger.warning(
                    "GHL_HTTP | %s %s | %d (transient) | retry in %ds (attempt %d/%d)",
                    method, path, resp.status_code, wait, attempt + 1, retries,
                )
                await asyncio.sleep(wait)
                continue

            elapsed_ms = int((time.perf_counter() - t0) * 1000)
            if resp.status_code >= 400:
                logger.warning("GHL_HTTP | %s %s | status=%d | elapsed_ms=%d", method, path, resp.status_code, elapsed_ms)
            else:
                logger.info("GHL_HTTP | %s %s | status=%d | elapsed_ms=%d", method, path, resp.status_code, elapsed_ms)
            return resp

        elapsed_ms = int((time.perf_counter() - t0) * 1000)
        logger.warning(
            "GHL_HTTP | %s %s | retries exhausted (last=%d) | elapsed_ms=%d",
            method, path, last_resp.status_code if last_resp else 0, elapsed_ms,
        )
        return last_resp

    # =========================================================================
    # LOCATION
    # =========================================================================

    async def get_location_timezone(self) -> str | None:
        """Fetch the IANA timezone of this GHL location.

        Returns e.g. ``"America/Chicago"`` or *None* on failure.
        GHL is the source-of-truth for what timezone naive appointment
        datetimes are expressed in.
        """
        try:
            resp = await self._request(
                "GET", f"/locations/{self.location_id}", version="2021-04-15",
            )
            if resp.status_code == 200:
                data = resp.json()
                loc = data.get("location", data)
                tz = loc.get("timezone") or None
                logger.info("GHL_API | get_location_timezone | location=%s | tz=%s", self.location_id, tz)
                return tz
        except Exception as exc:
            logger.warning("Failed to fetch GHL location timezone: %s", exc)
        return None

    # =========================================================================
    # CONTACTS
    # =========================================================================

    async def get_contact(self, contact_id: str) -> dict[str, Any]:
        """Get contact by ID. Returns the contact dict."""
        resp = await self._request("GET", f"/contacts/{contact_id}", version="2021-04-15")
        contact = resp.json().get("contact", {})
        logger.info("GHL_API | get_contact | contact=%s | found=%s | tags=%d", contact_id, bool(contact), len(contact.get("tags", []) or []))
        return contact

    async def update_contact(self, contact_id: str, data: dict[str, Any]) -> dict[str, Any]:
        """Update contact fields (name, email, custom fields, etc.)."""
        logger.info("GHL_API | update_contact | contact=%s | fields=%s", contact_id, list(data.keys()))
        resp = await self._request(
            "PUT", f"/contacts/{contact_id}", version="2021-04-15", json_data=data
        )
        return resp.json().get("contact", resp.json())

    async def search_contacts(self, query: str, limit: int = 1) -> list[dict[str, Any]]:
        """Search contacts by phone, email, or name. Returns list of matching contacts."""
        resp = await self._request(
            "GET", "/contacts/", version="2021-07-28",
            params={"query": query, "locationId": self.location_id, "limit": str(limit)},
        )
        contacts = resp.json().get("contacts", [])
        logger.info("GHL_API | search_contacts | query=%s | results=%d", query[:20], len(contacts))
        return contacts

    async def create_contact(self, data: dict[str, Any]) -> dict[str, Any]:
        """Create a new contact. Returns the created contact dict."""
        data["locationId"] = self.location_id
        resp = await self._request(
            "POST", "/contacts/", version="2021-07-28", json_data=data
        )
        contact = resp.json().get("contact", resp.json())
        logger.info("GHL_API | create_contact | id=%s | phone=%s", contact.get("id", ""), data.get("phone", data.get("email", "")))
        return contact

    # =========================================================================
    # TAGS
    # =========================================================================

    async def list_location_tags(self) -> list[dict[str, str]]:
        """List all tags for the GHL location."""
        resp = await self._request(
            "GET", f"/locations/{self.location_id}/tags", version="2021-07-28",
        )
        tags = resp.json().get("tags", [])
        logger.info("GHL_API | list_tags | location=%s | count=%d", self.location_id, len(tags))
        return tags

    async def add_tag(self, contact_id: str, tag: str) -> None:
        """Add a tag to a contact."""
        await self._request(
            "POST", f"/contacts/{contact_id}/tags", json_data={"tags": [tag]}
        )
        logger.info("GHL_API | add_tag | contact=%s | tag=%s", contact_id, tag)

    async def remove_tag(self, contact_id: str, tag: str) -> None:
        """Remove a tag from a contact."""
        await self._request(
            "DELETE", f"/contacts/{contact_id}/tags", json_data={"tags": [tag]}
        )
        logger.info("GHL_API | remove_tag | contact=%s | tag=%s", contact_id, tag)

    # =========================================================================
    # APPOINTMENTS
    # =========================================================================

    async def get_appointments(self, contact_id: str) -> list[dict[str, Any]]:
        """Get all appointments for a contact."""
        resp = await self._request(
            "GET", f"/contacts/{contact_id}/appointments", version="2021-04-15"
        )
        data = resp.json()
        appts = data.get("events", data.get("appointments", []))
        logger.info("GHL_API | get_appointments | contact=%s | count=%d", contact_id, len(appts) if appts else 0)
        return appts

    async def get_calendars(self) -> list[dict[str, Any]]:
        """List all calendars for this location."""
        resp = await self._request(
            "GET", "/calendars/",
            version="2021-04-15",
            params={"locationId": self.location_id},
        )
        calendars = resp.json().get("calendars", [])
        logger.info("GHL_API | get_calendars | location=%s | count=%d", self.location_id, len(calendars))
        return calendars

    async def get_appointment(self, appointment_id: str) -> dict[str, Any] | None:
        """Get a single appointment by ID."""
        resp = await self._request(
            "GET", f"/calendars/events/appointments/{appointment_id}",
            version="2021-04-15",
        )
        data = resp.json()
        return data.get("appointment") or data

    async def get_free_slots(
        self, calendar_id: str, start_ms: int, end_ms: int, timezone: str = "America/Chicago"
    ) -> dict[str, Any]:
        """Get free calendar slots. Dates are epoch milliseconds."""
        resp = await self._request(
            "GET",
            f"/calendars/{calendar_id}/free-slots/",
            version="2021-04-15",
            params={"startDate": start_ms, "endDate": end_ms, "timezone": timezone},
        )
        result = resp.json()
        slot_count = sum(len(v) for v in result.values() if isinstance(v, list))
        logger.info("GHL_API | get_free_slots | calendar=%s | tz=%s | slots=%d", calendar_id, timezone, slot_count)
        return result

    async def book_appointment(self, data: dict[str, Any]) -> dict[str, Any]:
        """Book an appointment."""
        logger.info("GHL_API | book_appointment | contact=%s | calendar=%s | start=%s", data.get("contactId"), data.get("calendarId"), data.get("startTime"))
        resp = await self._request(
            "POST", "/calendars/events/appointments", version="2021-04-15", json_data=data
        )
        return resp.json()

    async def update_appointment(self, event_id: str, data: dict[str, Any]) -> dict[str, Any]:
        """Reschedule or update an appointment."""
        logger.info("GHL_API | update_appointment | event=%s | fields=%s", event_id, list(data.keys()))
        resp = await self._request(
            "PUT", f"/calendars/events/appointments/{event_id}",
            version="2021-04-15", json_data=data,
        )
        return resp.json()

    async def cancel_appointment(self, event_id: str) -> dict[str, Any]:
        """Cancel an appointment (PUT with status change, not DELETE)."""
        logger.info("GHL_API | cancel_appointment | event=%s", event_id)
        resp = await self._request(
            "PUT", f"/calendars/events/appointments/{event_id}",
            version="2021-04-15", json_data={"appointmentStatus": "cancelled"},
        )
        return resp.json()

    # =========================================================================
    # CONVERSATIONS
    # =========================================================================

    async def list_recent_conversations(
        self, limit: int = 100, sort: str = "desc", sort_by: str = "last_message_date"
    ) -> list[dict[str, Any]]:
        """List conversations in this location sorted by last-message date.

        Used by the sync poller to discover contacts with recent activity
        (including cold leads whose first reply may never have created a
        chat-history row yet because the SH webhook was dropped).

        Returns the raw conversation objects; each has at least
        ``id``, ``contactId``, and ``lastMessageDate``.
        """
        resp = await self._request(
            "GET", "/conversations/search",
            version="2021-04-15",
            params={
                "locationId": self.location_id,
                "limit": str(max(1, min(limit, 100))),
                "sort": sort,
                "sortBy": sort_by,
            },
        )
        conversations = resp.json().get("conversations", [])
        logger.info(
            "GHL_API | list_recent_conversations | location=%s | count=%d | sort=%s/%s",
            self.location_id, len(conversations), sort_by, sort,
        )
        return conversations

    async def get_conversation_messages(
        self, contact_id: str
    ) -> list[dict[str, Any]]:
        """Get ALL conversation messages for a contact (paginated).

        Handles GHL's double-nested response and pagination.
        Returns deduped list of all messages.
        """
        # Step 1: Find conversation
        resp = await self._request(
            "GET", "/conversations/search",
            version="2021-04-15",
            params={"contactId": contact_id},
        )
        conversations = resp.json().get("conversations", [])
        if not conversations:
            logger.info("GHL_API | get_conversation_messages | contact=%s | no_conversation_found", contact_id)
            return []

        conversation_id = conversations[0]["id"]

        # Step 2: Paginate through all messages
        all_messages = []
        seen_ids = set()
        last_message_id = None

        while True:
            params = {}
            if last_message_id:
                params["lastMessageId"] = last_message_id

            resp = await self._request(
                "GET", f"/conversations/{conversation_id}/messages",
                version="2021-04-15", params=params,
            )
            msg_data = resp.json().get("messages", {})
            messages = msg_data.get("messages", [])

            for msg in messages:
                if msg["id"] not in seen_ids:
                    seen_ids.add(msg["id"])
                    all_messages.append(msg)

            if not msg_data.get("nextPage"):
                break
            last_message_id = msg_data.get("lastMessageId", "")

        logger.info("GHL_API | get_conversation_messages | contact=%s | messages=%d", contact_id, len(all_messages))
        return all_messages

    async def get_call_transcription(
        self, message_id: str, location_id: str | None = None
    ) -> dict[str, Any] | None:
        """Get call transcription (structured JSON with per-sentence data)."""
        loc = location_id or self.location_id
        resp = await self._request(
            "GET",
            f"/conversations/locations/{loc}/messages/{message_id}/transcription",
            version="2021-04-15",
        )
        if resp.status_code == 200:
            result = resp.json()
            logger.info("GHL_API | get_call_transcription | message=%s | has_data=%s", message_id, bool(result))
            return result
        logger.info("GHL_API | get_call_transcription | message=%s | status=%d | no_transcript", message_id, resp.status_code)
        return None

    async def get_email_by_id(self, message_id: str) -> dict[str, Any] | None:
        """Get full email details via dedicated email endpoint.

        Matches n8n's "Get Subject Line" step:
        GET /conversations/messages/email/{messageId}

        Returns the emailMessage dict (subject, threadId, to, etc.)
        or None if the request fails.
        """
        resp = await self._request(
            "GET", f"/conversations/messages/email/{message_id}",
            version="2021-04-15",
            params={"limit": 20},
        )
        if resp.status_code == 200:
            return resp.json().get("emailMessage")
        logger.warning("get_email_by_id failed: status=%s id=%s", resp.status_code, message_id)
        return None

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
        """Send an email reply via GHL conversations API."""
        # Final outbound boundary: em-dashes never reach the inbox.
        subject = scrub_dashes(subject)
        message = scrub_dashes(message)
        logger.info("GHL_API | send_email_reply | contact=%s | subject=%s | thread=%s", contact_id, subject[:50], bool(thread_id))
        payload: dict[str, Any] = {
            "type": "Email",
            "contactId": contact_id,
            "subject": subject,
            "html": message,
        }
        if conversation_id:
            payload["conversationId"] = conversation_id
        if in_reply_to:
            payload["replyMessageId"] = in_reply_to
        if thread_id:
            payload["threadId"] = thread_id
        if email_from:
            payload["emailFrom"] = email_from
        resp = await self._request(
            "POST", "/conversations/messages", version="2021-04-15", json_data=payload
        )
        return resp.json()

    # =========================================================================
    # INTERNAL COMMENTS
    # =========================================================================

    async def add_internal_comment(
        self,
        contact_id: str,
        message: str,
    ) -> dict[str, Any]:
        """Add an internal comment (yellow sticky note) to a GHL conversation.

        Uses POST /conversations/messages with type=InternalComment.
        Shows in the conversation timeline, visible only to staff.
        """
        payload: dict[str, Any] = {
            "type": "InternalComment",
            "contactId": contact_id,
            "message": message,
        }
        resp = await self._request(
            "POST", "/conversations/messages", version="2021-04-15", json_data=payload
        )
        if resp.status_code >= 400:
            body = resp.text[:200]
            logger.error("GHL_API | internal_comment FAILED | contact=%s | status=%d | %s", contact_id, resp.status_code, body)
            return {"ok": False, "error": body}
        result = resp.json()
        logger.info(
            "GHL_API | internal_comment | contact=%s | messageId=%s",
            contact_id, result.get("messageId"),
        )
        return result

    async def create_note(
        self,
        contact_id: str,
        body: str,
    ) -> dict[str, Any]:
        """Create a note on a GHL contact.

        Uses POST /contacts/{id}/notes. Shows in the Notes tab on the right panel.
        """
        resp = await self._request(
            "POST", f"/contacts/{contact_id}/notes", version="2021-07-28",
            json_data={"body": body},
        )
        if resp.status_code >= 400:
            body_err = resp.text[:200]
            logger.error("GHL_API | create_note FAILED | contact=%s | status=%d | %s", contact_id, resp.status_code, body_err)
            return {"ok": False, "error": body_err}
        result = resp.json()
        logger.info("GHL_API | create_note | contact=%s | note_id=%s", contact_id, result.get("note", {}).get("id"))
        return result

    async def get_notes(
        self,
        contact_id: str,
    ) -> list[dict[str, Any]]:
        """Get all notes for a GHL contact. GET /contacts/{id}/notes."""
        resp = await self._request(
            "GET", f"/contacts/{contact_id}/notes", version="2021-07-28",
        )
        if resp.status_code >= 400:
            return []
        return resp.json().get("notes", [])

    # =========================================================================
    # SMS / STANDALONE EMAIL — Direct API Delivery
    # =========================================================================

    async def send_sms(
        self,
        contact_id: str,
        message: str,
        attachments: list[str] | None = None,
    ) -> dict[str, Any]:
        """Send SMS via POST /conversations/messages.

        Returns: {conversationId, messageId, traceId} on 201.
        Raises httpx.HTTPStatusError on 4xx/5xx.
        """
        # Final outbound boundary: em-dashes never reach the carrier.
        message = scrub_dashes(message)
        payload: dict[str, Any] = {
            "type": "SMS",
            "contactId": contact_id,
            "message": message,
        }
        if attachments:
            payload["attachments"] = attachments
        resp = await self._request(
            "POST", "/conversations/messages", version="2021-04-15", json_data=payload
        )
        if resp.status_code >= 400:
            body = resp.text[:200]
            logger.error("GHL_API | send_sms FAILED | contact=%s | status=%d | %s", contact_id, resp.status_code, body)
            raise httpx.HTTPStatusError(
                f"GHL send_sms failed: {resp.status_code} — {body}",
                request=resp.request,
                response=resp,
            )
        result = resp.json()
        logger.info(
            "GHL_API | send_sms | contact=%s | messageId=%s | conversationId=%s",
            contact_id, result.get("messageId"), result.get("conversationId"),
        )
        return result

    async def get_message_status(self, message_id: str) -> dict[str, Any] | None:
        """Fetch a single message to check delivery status.

        GET /conversations/messages/{id}
        Returns message dict with 'status' field (sent, delivered, undelivered).
        """
        resp = await self._request(
            "GET", f"/conversations/messages/{message_id}", version="2021-04-15"
        )
        if resp.status_code == 200:
            return resp.json().get("message", resp.json())
        logger.warning("GHL_API | get_message_status failed | id=%s | status=%d", message_id, resp.status_code)
        return None

    async def send_standalone_email(
        self,
        contact_id: str,
        subject: str,
        html_body: str,
    ) -> dict[str, Any]:
        """Send a standalone (non-threaded) email. For outreach positions with email content."""
        # Final outbound boundary: em-dashes never reach the inbox.
        subject = scrub_dashes(subject)
        html_body = scrub_dashes(html_body)
        logger.info("GHL_API | send_standalone_email | contact=%s | subject=%s", contact_id, subject[:50])
        payload: dict[str, Any] = {
            "type": "Email",
            "contactId": contact_id,
            "subject": subject,
            "html": html_body,
        }
        resp = await self._request(
            "POST", "/conversations/messages", version="2021-04-15", json_data=payload
        )
        if resp.status_code >= 400:
            body = resp.text[:200]
            logger.error("GHL_API | send_standalone_email FAILED | contact=%s | status=%d | %s", contact_id, resp.status_code, body)
            raise httpx.HTTPStatusError(
                f"GHL send_standalone_email failed: {resp.status_code} — {body}",
                request=resp.request,
                response=resp,
            )
        return resp.json()

    # =========================================================================
    # PIPELINES & OPPORTUNITIES
    # =========================================================================

    async def get_pipelines(self) -> list[dict[str, Any]]:
        """Get all pipelines with stage definitions."""
        resp = await self._request(
            "GET", "/opportunities/pipelines",
            params={"locationId": self.location_id},
        )
        pipelines = resp.json().get("pipelines", [])
        logger.info("GHL_API | get_pipelines | location=%s | count=%d", self.location_id, len(pipelines))
        return pipelines

    async def search_opportunities(
        self, contact_id: str, pipeline_id: str
    ) -> list[dict[str, Any]]:
        """Search for opportunities (GET, not POST)."""
        resp = await self._request(
            "GET", "/opportunities/search",
            params={
                "location_id": self.location_id,
                "contact_id": contact_id,
                "pipeline_id": pipeline_id,
                "order": "added_desc",
                "limit": 1,
            },
        )
        opps = resp.json().get("opportunities", [])
        logger.info("GHL_API | search_opportunities | contact=%s | pipeline=%s | found=%d", contact_id, pipeline_id, len(opps))
        return opps

    async def create_opportunity(self, data: dict[str, Any]) -> dict[str, Any]:
        """Create a new pipeline opportunity."""
        data.setdefault("locationId", self.location_id)
        logger.info("GHL_API | create_opportunity | contact=%s | pipeline=%s", data.get("contactId"), data.get("pipelineId"))
        resp = await self._request("POST", "/opportunities/", json_data=data)
        return resp.json().get("opportunity", resp.json())

    async def update_opportunity(
        self, opportunity_id: str, data: dict[str, Any]
    ) -> dict[str, Any]:
        """Update an opportunity (e.g., move pipeline stage)."""
        logger.info("GHL_API | update_opportunity | opp=%s | fields=%s", opportunity_id, list(data.keys()))
        resp = await self._request(
            "PUT", f"/opportunities/{opportunity_id}", json_data=data
        )
        return resp.json()

    # =========================================================================
    # CUSTOM FIELDS
    # =========================================================================

    async def get_custom_field_defs(self) -> dict[str, str]:
        """Get custom field name→ID mapping for this location."""
        resp = await self._request(
            "GET", f"/locations/{self.location_id}/customFields"
        )
        fields = resp.json().get("customFields", [])
        logger.info("GHL_API | get_custom_field_defs | location=%s | fields=%d", self.location_id, len(fields))
        return {f["name"]: f["id"] for f in fields}

    async def get_custom_field_defs_full(self) -> list[dict[str, str]]:
        """Get full custom field definitions with id, name, fieldKey, dataType."""
        resp = await self._request(
            "GET", f"/locations/{self.location_id}/customFields"
        )
        fields = resp.json().get("customFields", [])
        logger.info("GHL_API | get_custom_field_defs_full | location=%s | fields=%d", self.location_id, len(fields))
        return [
            {
                "id": f["id"],
                "name": f["name"],
                "key": f.get("fieldKey", f["name"]),
                "dataType": f.get("dataType", "TEXT"),
            }
            for f in fields
        ]

    async def create_custom_field(self, name: str, data_type: str = "TEXT") -> dict[str, str]:
        """Create a new custom field in this location."""
        logger.info("GHL_API | create_custom_field | location=%s | name=%s | type=%s", self.location_id, name, data_type)
        resp = await self._request(
            "POST", f"/locations/{self.location_id}/customFields",
            json_data={"name": name, "dataType": data_type},
        )
        field = resp.json().get("customField", resp.json())
        return {
            "id": field["id"],
            "name": field["name"],
            "key": field.get("fieldKey", field["name"]),
            "dataType": field.get("dataType", data_type),
        }

    async def set_custom_field(
        self, contact_id: str, field_key: str, value: str
    ) -> None:
        """Set a custom field value on a contact.

        Uses 'field_value' (not 'value') per GHL's write API.
        Key-based: pass field key like "smartfollowup_timer" (no prefix needed).
        """
        logger.info("GHL_API | set_custom_field | contact=%s | field=%s | value=%s", contact_id, field_key, str(value)[:100])
        await self.update_contact(
            contact_id,
            {"customFields": [{"key": field_key, "field_value": value}]},
        )

    async def set_opportunity_custom_fields(
        self, opportunity_id: str, fields: list[dict[str, str]]
    ) -> None:
        """Set custom field values on an opportunity.

        Args:
            opportunity_id: GHL opportunity ID.
            fields: List of dicts with 'key' and 'field_value'.
                    Keys must be prefixed: "opportunity.field_name".
        """
        if not fields:
            return
        logger.info("GHL_API | set_opp_custom_fields | opp=%s | field_count=%d", opportunity_id, len(fields))
        await self.update_opportunity(
            opportunity_id, {"customFields": fields}
        )
