"""OAuth token refresh — ensures a fresh access_token before GHL API calls.

Ported from iron-bridge's _ensure_fresh_token pattern.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

from app.config import settings
from app.marketplace.oauth_store import update_tokens

logger = logging.getLogger(__name__)

GHL_TOKEN_URL = "https://services.leadconnectorhq.com/oauth/token"
_TOKEN_REFRESH_MARGIN = timedelta(minutes=5)


async def ensure_fresh_token(install: dict[str, Any]) -> dict[str, Any]:
    """Return the install dict with a guaranteed-fresh access_token.

    If the token is expired or close to expiry, refreshes it via GHL's
    OAuth token endpoint and persists the new tokens. Returns the updated
    install dict (caller should use the returned access_token, not the
    original).
    """
    now = datetime.now(timezone.utc)
    expires_at = install["expires_at"]
    if isinstance(expires_at, datetime) and expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)

    if expires_at - _TOKEN_REFRESH_MARGIN > now:
        return install

    logger.info(
        "MARKETPLACE | refreshing token | location=%s | expires=%s",
        install["location_id"], expires_at.isoformat(),
    )

    refreshed = await _refresh_token(install["refresh_token"])
    new_access = refreshed["access_token"]
    new_refresh = refreshed.get("refresh_token", install["refresh_token"])
    new_expires_in = int(refreshed.get("expires_in", 86400))
    new_expires_at = datetime.now(timezone.utc) + timedelta(seconds=new_expires_in)
    new_scopes = refreshed.get("scope", install.get("scopes", ""))

    await update_tokens(
        install["location_id"],
        access_token=new_access,
        refresh_token=new_refresh,
        expires_at=new_expires_at,
        scopes=new_scopes,
    )

    install = {**install}
    install["access_token"] = new_access
    install["refresh_token"] = new_refresh
    install["expires_at"] = new_expires_at
    if new_scopes:
        install["scopes"] = new_scopes
    return install


async def _refresh_token(refresh_token: str) -> dict[str, Any]:
    """Call GHL's OAuth token endpoint to refresh an access token."""
    client_id = settings.ghl_oauth_client_id
    client_secret = settings.ghl_oauth_client_secret
    if not client_id or not client_secret:
        raise RuntimeError("GHL OAuth credentials not configured")

    async with httpx.AsyncClient(timeout=15.0) as client:
        resp = await client.post(
            GHL_TOKEN_URL,
            data={
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
                "client_id": client_id,
                "client_secret": client_secret,
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )

    if resp.status_code >= 400:
        logger.error(
            "MARKETPLACE | token refresh failed | status=%d",
            resp.status_code,
        )
        raise RuntimeError(f"GHL OAuth refresh failed: {resp.status_code}")

    payload = resp.json()
    if "access_token" not in payload:
        raise RuntimeError("GHL OAuth refresh response missing access_token")

    logger.info(
        "MARKETPLACE | token refresh OK | expires_in=%s",
        payload.get("expires_in"),
    )
    return payload
