"""Delivery Service — provider-aware SMS/email delivery with confirmation + retry.

Every SMS delivery in the system goes through this service. It handles:
- Sending via GHL API
- Delivery confirmation (Signal House or GHL polling)
- One retry on failure
- Failure logging to delivery_failures table
- Slack notification on permanent failure
- Batch timeout (if one message polls too long, skip polling for the rest)
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import httpx

from app.services.ghl_client import GHLClient
from app.utils.text_scrub import scrub_dashes

logger = logging.getLogger(__name__)


@dataclass
class DeliveryResult:
    ghl_message_id: str | None = None
    status: str = "unknown"  # delivered, failed, timeout, sent, skipped
    provider: str = "unknown"
    error_message: str | None = None
    carrier: str | None = None


@dataclass
class SplitDeliveryResult:
    results: list[DeliveryResult] = field(default_factory=list)
    all_delivered: bool = False
    stopped_early: bool = False


class DeliveryService:
    """Provider-aware message delivery with confirmation and retry.

    Instantiate per delivery call with the client's GHL credentials + config.

    Usage:
        svc = DeliveryService(ghl_client, client_config)
        result = await svc.send_sms(contact_id, "hey there")
        split = await svc.send_split_messages(contact_id, ["msg1", "msg2", "msg3"])
    """

    def __init__(self, ghl: GHLClient, config: dict[str, Any]) -> None:
        self.ghl = ghl
        self.config = config
        sc = config.get("system_config") or {}
        if isinstance(sc, str):
            try:
                sc = json.loads(sc)
            except (json.JSONDecodeError, TypeError):
                sc = {}
        self.sms_provider: str = sc.get("sms_provider", "telnyx")
        self.business_phone: str = config.get("business_phone", "") or ""
        self.entity_id: str = config.get("id", "")
        self._polling_disabled: bool = False

    # =========================================================================
    # PUBLIC API
    # =========================================================================

    async def send_sms(
        self,
        contact_id: str,
        message: str,
        media_url: str | None = None,
        to_phone: str | None = None,
        message_type: str = "reply",
    ) -> DeliveryResult:
        """Send a single SMS with provider-aware delivery confirmation + retry."""

        # 0. Final-line-of-defense scrub. Em-dashes never leave this process.
        message = scrub_dashes(message)

        # 1. Send via GHL API
        attachments = [media_url] if media_url else None
        send_time = datetime.now(timezone.utc)

        try:
            ghl_result = await self.ghl.send_sms(contact_id, message, attachments)
        except httpx.HTTPStatusError as e:
            error_msg = str(e)[:200]
            logger.error("DELIVERY | send_failed | contact=%s | %s", contact_id, error_msg)
            await self._log_failure(
                contact_id=contact_id, message_type=message_type,
                status="failed", error_message=error_msg,
                to_phone=to_phone, from_phone=self.business_phone,
                message_body=message,
            )
            return DeliveryResult(
                status="failed", provider=self.sms_provider, error_message=error_msg,
            )
        except Exception as e:
            error_msg = f"Unexpected error: {e}"
            logger.error("DELIVERY | send_exception | contact=%s | %s", contact_id, error_msg)
            return DeliveryResult(
                status="failed", provider=self.sms_provider, error_message=error_msg,
            )

        ghl_message_id = ghl_result.get("messageId")

        # 2. No-poll providers: random delay between splits (human impersonation)
        # telnyx uses async delivery status via iron-bridge's webhook (no sync poll needed)
        if self.sms_provider in ("telnyx", "imessage", "other"):
            await asyncio.sleep(random.uniform(3.0, 8.0))
            return DeliveryResult(
                ghl_message_id=ghl_message_id, status="sent", provider=self.sms_provider,
            )

        # 3. Polling disabled (previous timeout in this batch): just delay
        if self._polling_disabled:
            await asyncio.sleep(random.uniform(2.0, 4.0))
            return DeliveryResult(
                ghl_message_id=ghl_message_id, status="sent", provider=self.sms_provider,
            )

        # 4. Poll for delivery confirmation
        poll_result = await self._poll_delivery(
            ghl_message_id=ghl_message_id,
            to_phone=to_phone,
            message_body=message,
            send_time=send_time,
        )

        if poll_result.status == "delivered":
            return DeliveryResult(
                ghl_message_id=ghl_message_id,
                status="delivered",
                provider=self.sms_provider,
                carrier=poll_result.carrier,
            )

        if poll_result.status == "timeout":
            self._polling_disabled = True
            logger.info("DELIVERY | timeout — disabling polling for batch | contact=%s", contact_id)
            return DeliveryResult(
                ghl_message_id=ghl_message_id, status="timeout", provider=self.sms_provider,
            )

        # 5. Failed → retry once
        logger.warning(
            "DELIVERY | first attempt failed | contact=%s | reason=%s | retrying...",
            contact_id, poll_result.error_message,
        )

        retry_result = await self._retry_once(
            contact_id=contact_id, message=message, media_url=media_url,
            to_phone=to_phone, message_type=message_type,
        )

        if retry_result.status in ("delivered", "sent"):
            # Retry succeeded
            return retry_result

        # Both attempts failed → log failure + Slack
        await self._log_failure(
            contact_id=contact_id, message_type=message_type,
            status=retry_result.status or "failed",
            error_message=retry_result.error_message or poll_result.error_message,
            ghl_message_id=retry_result.ghl_message_id or ghl_message_id,
            carrier=poll_result.carrier,
            to_phone=to_phone, from_phone=self.business_phone,
            message_body=message, retry_attempted=True,
        )
        await self._notify_slack(contact_id, retry_result, message_type)

        return DeliveryResult(
            ghl_message_id=retry_result.ghl_message_id or ghl_message_id,
            status="failed",
            provider=self.sms_provider,
            error_message=retry_result.error_message or poll_result.error_message,
            carrier=poll_result.carrier,
        )

    async def send_split_messages(
        self,
        contact_id: str,
        messages: list[str],
        media_url: str | None = None,
        to_phone: str | None = None,
    ) -> SplitDeliveryResult:
        """Send 1-3 split messages sequentially with delivery gating."""
        results: list[DeliveryResult] = []

        for i, msg in enumerate(messages):
            media = media_url if i == 0 else None  # Media on first message only
            result = await self.send_sms(
                contact_id=contact_id, message=msg, media_url=media,
                to_phone=to_phone, message_type="reply_split",
            )
            results.append(result)

            if result.status == "failed":
                logger.warning(
                    "DELIVERY | split %d/%d failed — stopping remaining | contact=%s",
                    i + 1, len(messages), contact_id,
                )
                return SplitDeliveryResult(
                    results=results, all_delivered=False, stopped_early=True,
                )

        all_ok = all(r.status in ("delivered", "sent", "timeout") for r in results)
        return SplitDeliveryResult(results=results, all_delivered=all_ok)

    async def send_standalone_email(
        self,
        contact_id: str,
        subject: str,
        html_body: str,
    ) -> DeliveryResult:
        """Send standalone email via GHL API (for outreach, not threaded)."""
        subject = scrub_dashes(subject)
        html_body = scrub_dashes(html_body)
        try:
            result = await self.ghl.send_standalone_email(contact_id, subject, html_body)
            return DeliveryResult(
                ghl_message_id=result.get("messageId"),
                status="sent",
                provider=self.sms_provider,
            )
        except httpx.HTTPStatusError as e:
            error_msg = str(e)[:200]
            logger.error("DELIVERY | standalone_email_failed | contact=%s | %s", contact_id, error_msg)
            return DeliveryResult(
                status="failed", provider=self.sms_provider, error_message=error_msg,
            )

    # =========================================================================
    # INTERNAL — Polling
    # =========================================================================

    async def _poll_delivery(
        self,
        ghl_message_id: str | None,
        to_phone: str | None,
        message_body: str,
        send_time: datetime,
    ) -> DeliveryResult:
        """Route to provider-specific polling.

        Post-Telnyx cutover (2026-04-23): signalhouse polling removed.
        Delivery status for telnyx-routed sends is written asynchronously
        by iron-bridge to sms.messages.provider_status.
        """
        if self.sms_provider == "ghl_default":
            return await self._poll_ghl(ghl_message_id)
        # Unknown provider — treat as no-poll
        return DeliveryResult(status="sent", provider=self.sms_provider)

    async def _poll_ghl(self, message_id: str | None) -> DeliveryResult:
        """Poll GHL GET /conversations/messages/{id} for delivery status."""
        if not message_id:
            return DeliveryResult(status="timeout", provider="ghl_default")

        timeout = 12.0
        poll_interval = 1.5
        t0 = time.perf_counter()

        while (time.perf_counter() - t0) < timeout:
            await asyncio.sleep(poll_interval)

            msg = await self.ghl.get_message_status(message_id)
            if not msg:
                continue

            status = (msg.get("status") or "").lower()

            if status == "delivered":
                elapsed = time.perf_counter() - t0
                logger.info("DELIVERY | GHL delivered | id=%s | %.1fs", message_id, elapsed)
                return DeliveryResult(status="delivered", provider="ghl_default")

            if status == "undelivered":
                error = msg.get("error", "")
                elapsed = time.perf_counter() - t0
                logger.warning("DELIVERY | GHL undelivered | id=%s | %.1fs | %s", message_id, elapsed, error)
                return DeliveryResult(
                    status="failed", provider="ghl_default", error_message=error,
                )

        elapsed = time.perf_counter() - t0
        logger.info("DELIVERY | GHL poll timeout | id=%s | %.1fs", message_id, elapsed)
        return DeliveryResult(status="timeout", provider="ghl_default")

    # =========================================================================
    # INTERNAL — Retry
    # =========================================================================

    async def _retry_once(
        self,
        contact_id: str,
        message: str,
        media_url: str | None,
        to_phone: str | None,
        message_type: str,
    ) -> DeliveryResult:
        """Retry a failed send one time."""
        message = scrub_dashes(message)
        attachments = [media_url] if media_url else None
        send_time = datetime.now(timezone.utc)

        try:
            ghl_result = await self.ghl.send_sms(contact_id, message, attachments)
        except httpx.HTTPStatusError as e:
            return DeliveryResult(
                status="failed", provider=self.sms_provider,
                error_message=f"Retry send failed: {e}"[:200],
            )
        except Exception as e:
            return DeliveryResult(
                status="failed", provider=self.sms_provider,
                error_message=f"Retry exception: {e}"[:200],
            )

        ghl_message_id = ghl_result.get("messageId")

        # Poll the retry
        poll_result = await self._poll_delivery(
            ghl_message_id=ghl_message_id,
            to_phone=to_phone,
            message_body=message,
            send_time=send_time,
        )

        return DeliveryResult(
            ghl_message_id=ghl_message_id,
            status=poll_result.status,
            provider=self.sms_provider,
            error_message=poll_result.error_message,
            carrier=poll_result.carrier,
        )

    # =========================================================================
    # INTERNAL — Failure Logging
    # =========================================================================

    async def _log_failure(
        self,
        contact_id: str,
        message_type: str,
        status: str,
        error_message: str | None = None,
        ghl_message_id: str | None = None,
        carrier: str | None = None,
        to_phone: str | None = None,
        from_phone: str | None = None,
        message_body: str | None = None,
        retry_attempted: bool = False,
        chat_history_table: str | None = None,
    ) -> None:
        """Insert a row into delivery_failures table."""
        try:
            from app.services import supabase
            await supabase._request(
                supabase.main_client, "POST", "/delivery_failures",
                json={
                    "entity_id": self.entity_id,
                    "contact_id": contact_id,
                    "message_type": message_type,
                    "sms_provider": self.sms_provider,
                    "ghl_message_id": ghl_message_id,
                    "status": status,
                    "error_message": (error_message or "")[:500],
                    "carrier": carrier,
                    "to_phone": to_phone,
                    "from_phone": from_phone,
                    "message_body": (message_body or "")[:500],
                    "retry_attempted": retry_attempted,
                    "chat_history_table": chat_history_table or self.config.get("chat_history_table_name"),
                },
                label="insert_delivery_failure",
            )
            logger.info("DELIVERY | failure logged | contact=%s | type=%s | status=%s", contact_id, message_type, status)
        except Exception as e:
            logger.error("DELIVERY | failed to log failure: %s", e)

    async def _notify_slack(
        self,
        contact_id: str,
        result: DeliveryResult,
        message_type: str,
    ) -> None:
        """Send Slack notification for permanent delivery failure."""
        try:
            from app.services.mattermost import post_message as post_slack_message
            company = self.config.get("name") or self.config.get("name") or self.entity_id
            text = (
                f":x: *SMS Delivery Failed* (after retry)\n"
                f"*Client:* {company}\n"
                f"*Contact:* {contact_id}\n"
                f"*Type:* {message_type}\n"
                f"*Provider:* {self.sms_provider}\n"
                f"*Error:* {result.error_message or 'Unknown'}\n"
                f"*Carrier:* {result.carrier or 'Unknown'}"
            )
            await post_slack_message("python-errors", text)
        except Exception as e:
            logger.error("DELIVERY | slack notification failed: %s", e)
