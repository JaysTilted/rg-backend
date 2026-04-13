"""Signal House API client — delivery status polling.

Polls GET /message/logs to check if an SMS was delivered or failed.
Used by DeliveryService for provider="signalhouse" delivery confirmation.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import datetime

import httpx

logger = logging.getLogger(__name__)

SH_BASE = "https://api.signalhouse.io"

# Shared connection pool
_sh_client: httpx.AsyncClient | None = None


async def start_signal_house_pool() -> None:
    global _sh_client
    _sh_client = httpx.AsyncClient(base_url=SH_BASE, timeout=15.0)


async def stop_signal_house_pool() -> None:
    global _sh_client
    if _sh_client:
        await _sh_client.aclose()
        _sh_client = None


@dataclass
class SignalHouseDeliveryResult:
    status: str  # "delivered", "failed", "timeout"
    signal_house_sid: str | None = None
    elapsed: float = 0.0
    failure_message: str | None = None
    carrier: str | None = None


class SignalHouseClient:
    """Polls Signal House for SMS delivery confirmation."""

    def __init__(self, api_key: str, auth_token: str) -> None:
        self.api_key = api_key
        self.auth_token = auth_token

    def _headers(self) -> dict[str, str]:
        return {
            "authToken": self.auth_token,
            "apiKey": self.api_key,
            "Content-Type": "application/json",
        }

    async def check_delivery(
        self,
        from_phone: str,
        to_phone: str,
        message_body: str,
        start_time: datetime,
        timeout: float = 12.0,
        poll_interval: float = 1.5,
    ) -> SignalHouseDeliveryResult:
        """Poll /message/logs until message reaches a terminal status.

        Filters by from + to + startDate + message body match.
        Terminal statuses: delivered, failed.
        Returns timeout if still in-transit after `timeout` seconds.
        """
        client = _sh_client
        if not client:
            logger.warning("SH_CLIENT | no pool — skipping delivery check")
            return SignalHouseDeliveryResult(status="timeout")

        # Strip + prefix for Signal House (expects bare numbers)
        from_clean = from_phone.lstrip("+")
        to_clean = to_phone.lstrip("+")
        start_iso = start_time.isoformat() if isinstance(start_time, datetime) else str(start_time)
        body_stripped = message_body.strip()

        t0 = time.perf_counter()

        while (time.perf_counter() - t0) < timeout:
            await asyncio.sleep(poll_interval)
            elapsed = time.perf_counter() - t0

            try:
                resp = await client.get(
                    "/message/logs",
                    headers=self._headers(),
                    params={
                        "from": from_clean,
                        "to": to_clean,
                        "startDate": start_iso,
                        "direction": "outbound",
                        "limit": 3,
                        "sortBy": "CreatedDate",
                        "sortOrder": "desc",
                    },
                )
            except Exception as e:
                logger.warning("SH_CLIENT | poll error at %.1fs: %s", elapsed, e)
                continue

            if resp.status_code != 200:
                logger.warning("SH_CLIENT | poll status=%d at %.1fs", resp.status_code, elapsed)
                continue

            records = resp.json().get("records", [])

            for rec in records:
                if rec.get("messageBody", "").strip() == body_stripped:
                    status = rec.get("latestStatus", "").lower()
                    if status == "delivered":
                        sid = rec.get("signalHouseSID")
                        carrier_list = rec.get("carrier") or []
                        carrier = carrier_list[0] if carrier_list else None
                        logger.info(
                            "SH_CLIENT | delivered | sid=%s | %.1fs | carrier=%s",
                            sid, elapsed, carrier,
                        )
                        return SignalHouseDeliveryResult(
                            status="delivered",
                            signal_house_sid=sid,
                            elapsed=elapsed,
                            carrier=carrier,
                        )
                    if status == "failed":
                        sid = rec.get("signalHouseSID")
                        failure = rec.get("failureMessage")
                        carrier_list = rec.get("carrier") or []
                        carrier = carrier_list[0] if carrier_list else None
                        logger.warning(
                            "SH_CLIENT | failed | sid=%s | %.1fs | reason=%s | carrier=%s",
                            sid, elapsed, failure, carrier,
                        )
                        return SignalHouseDeliveryResult(
                            status="failed",
                            signal_house_sid=sid,
                            elapsed=elapsed,
                            failure_message=failure,
                            carrier=carrier,
                        )
                    # Still in transit (sent, enqueued, etc.) — keep polling
                    break

        elapsed = time.perf_counter() - t0
        logger.info("SH_CLIENT | timeout after %.1fs | from=%s to=%s", elapsed, from_clean, to_clean)
        return SignalHouseDeliveryResult(status="timeout", elapsed=elapsed)
