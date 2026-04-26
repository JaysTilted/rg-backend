"""GHL Marketplace OAuth routes — install + callback.

Adapted from iron-bridge's OAuth flow for iron-setter's FastAPI app.
After OAuth exchange, issues a setup JWT and redirects to iron-setup portal.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode

import httpx
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import HTMLResponse, RedirectResponse

from app.config import settings
from app.marketplace.oauth_store import get_token, upsert_token, link_entity

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/oauth/ghl", tags=["marketplace-oauth"])

GHL_TOKEN_URL = "https://services.leadconnectorhq.com/oauth/token"
SETUP_PORTAL_URL = "https://setup.ironops.xyz"

OAUTH_SCOPES = (
    "contacts.readonly contacts.write "
    "calendars.readonly calendars.write "
    "calendars/events.readonly calendars/events.write "
    "conversations.readonly conversations.write "
    "conversations/message.readonly conversations/message.write "
    "opportunities.readonly opportunities.write "
    "locations.readonly "
    "locations/tags.readonly locations/tags.write "
    "locations/customFields.readonly locations/customFields.write"
)


def _issue_setup_jwt(location_id: str, product: str = "setter") -> str:
    """Issue a short-lived setup JWT for the iron-setup portal.

    Uses HMAC-SHA256 with the shared IRON_SETUP_JWT_SECRET (or
    portal_jwt_secret as fallback).
    """
    secret = settings.portal_jwt_secret
    if not secret:
        raise HTTPException(status_code=500, detail="JWT secret not configured")

    now = int(time.time())
    payload = {
        "sub": location_id,
        "product": product,
        "iat": now,
        "exp": now + 86400,
    }
    header = {"alg": "HS256", "typ": "JWT"}

    def _b64(data: bytes) -> str:
        return base64.urlsafe_b64encode(data).rstrip(b"=").decode()

    h = _b64(json.dumps(header).encode())
    p = _b64(json.dumps(payload).encode())
    sig = hmac.new(secret.encode(), f"{h}.{p}".encode(), hashlib.sha256).digest()
    return f"{h}.{p}.{_b64(sig)}"


@router.get("/install")
async def install() -> RedirectResponse:
    """Kick off the Marketplace install by bouncing to GHL's authorize URL."""
    if not settings.ghl_oauth_client_id:
        raise HTTPException(status_code=500, detail="GHL_OAUTH_CLIENT_ID not configured")

    params = {
        "response_type": "code",
        "client_id": settings.ghl_oauth_client_id,
        "redirect_uri": settings.ghl_oauth_redirect_uri,
        "scope": OAUTH_SCOPES,
    }
    url = f"{settings.ghl_marketplace_authorize_url}?{urlencode(params)}"
    logger.info("MARKETPLACE | oauth install redirect")
    return RedirectResponse(url=url, status_code=302)


@router.get("/callback")
async def callback(
    code: str,
    locationId: str | None = None,
) -> RedirectResponse:
    """Exchange the authorization code for tokens, then redirect to setup portal."""
    logger.info("MARKETPLACE | oauth callback | location=%s", locationId)

    if not settings.ghl_oauth_client_id or not settings.ghl_oauth_client_secret:
        raise HTTPException(status_code=500, detail="OAuth credentials not configured")

    async with httpx.AsyncClient(timeout=15.0) as client:
        resp = await client.post(
            GHL_TOKEN_URL,
            data={
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": settings.ghl_oauth_redirect_uri,
                "client_id": settings.ghl_oauth_client_id,
                "client_secret": settings.ghl_oauth_client_secret,
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )

    if resp.status_code >= 400:
        logger.error("MARKETPLACE | oauth exchange failed | status=%d", resp.status_code)
        raise HTTPException(status_code=502, detail="OAuth exchange failed")

    payload = resp.json()
    access_token = str(payload.get("access_token") or "")
    refresh_token = str(payload.get("refresh_token") or "")
    expires_in = int(payload.get("expires_in") or 86400)
    scopes = str(payload.get("scope") or OAUTH_SCOPES)
    resolved_location_id = str(payload.get("locationId") or locationId or "")
    company_id = payload.get("companyId")

    if not access_token or not refresh_token or not resolved_location_id:
        logger.error("MARKETPLACE | oauth response missing fields")
        raise HTTPException(status_code=502, detail="OAuth response missing required fields")

    existing = await get_token(resolved_location_id)
    entity_id = existing["entity_id"] if existing and existing.get("entity_id") else None

    if not entity_id:
        try:
            from app.services.supabase_client import supabase
            entity = await supabase.resolve_entity_by_location_id(resolved_location_id)
            entity_id = str(entity["id"])
            logger.info(
                "MARKETPLACE | auto-linked entity | location=%s | entity=%s",
                resolved_location_id, entity_id,
            )
        except (ValueError, RuntimeError):
            pass

    await upsert_token(
        location_id=resolved_location_id,
        access_token=access_token,
        refresh_token=refresh_token,
        expires_at=datetime.now(timezone.utc) + timedelta(seconds=expires_in),
        scopes=scopes,
        company_id=str(company_id) if company_id else None,
        entity_id=entity_id,
    )

    token = _issue_setup_jwt(resolved_location_id, "setter")

    if entity_id:
        redirect_url = f"{SETUP_PORTAL_URL}/setter/config?token={token}"
    else:
        redirect_url = f"{SETUP_PORTAL_URL}/setter?token={token}"

    logger.info(
        "MARKETPLACE | redirecting to portal | location=%s | has_entity=%s",
        resolved_location_id, bool(entity_id),
    )
    return RedirectResponse(url=redirect_url, status_code=302)
