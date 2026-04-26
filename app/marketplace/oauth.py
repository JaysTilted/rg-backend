"""GHL Marketplace OAuth routes — install + callback.

Adapted from iron-bridge's OAuth flow for iron-setter's FastAPI app.
"""

from __future__ import annotations

import logging
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


_INSTALL_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>Iron Setter — Installed</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
            max-width: 560px; margin: 4rem auto; padding: 0 1.5rem; color: #1f2937; }}
    h1 {{ color: #111827; }}
    code {{ background: #f3f4f6; padding: 0.15rem 0.4rem; border-radius: 4px; }}
    .next {{ margin-top: 1.5rem; padding: 0.75rem 1.5rem; background: #3b82f6;
             color: white; text-decoration: none; border-radius: 8px; display: inline-block; }}
  </style>
</head>
<body>
  <h1>Installation complete</h1>
  <p>Iron Setter installed for location <code>{location_id}</code>.</p>
  <p>{setup_link}</p>
</body>
</html>
"""


@router.get("/callback")
async def callback(
    code: str,
    locationId: str | None = None,
) -> HTMLResponse:
    """Exchange the authorization code for tokens and persist them."""
    logger.info("MARKETPLACE | oauth callback | location=%s", locationId)

    if not settings.ghl_oauth_client_id or not settings.ghl_oauth_client_secret:
        logger.warning("MARKETPLACE | oauth callback skipped — no credentials")
        return HTMLResponse(
            _INSTALL_HTML.format(
                location_id=locationId or "unknown",
                setup_link="OAuth credentials not configured.",
            ),
            status_code=200,
        )

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

    # Check if this location already has an entity linked
    existing = await get_token(resolved_location_id)
    entity_id = existing["entity_id"] if existing and existing.get("entity_id") else None

    # If no entity linked, try to find one by ghl_location_id in Supabase
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

    setup_link = ""
    if not entity_id:
        base = settings.ghl_app_base_url or ""
        if base:
            setup_link = (
                f'<a class="next" href="{base}/marketplace/setup?installId={resolved_location_id}">'
                f'Continue to Setup →</a>'
            )

    return HTMLResponse(
        _INSTALL_HTML.format(
            location_id=resolved_location_id,
            setup_link=setup_link or "You can close this window.",
        ),
        status_code=200,
    )
