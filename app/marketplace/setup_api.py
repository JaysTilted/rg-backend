"""Setup API routes for the iron-setup portal.

Endpoints for provisioning new clients, reading/updating config,
and fetching GHL data (calendars, users) for dropdown population.
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, HTTPException, Header
from pydantic import BaseModel, Field

from app.marketplace.ghl_assets import provision_ghl_assets
from app.marketplace.oauth_store import get_token, link_entity
from app.marketplace.token_refresh import ensure_fresh_token
from app.marketplace.templates import build_config, AVAILABLE_NICHES
from app.services.ghl_client import GHLClient
from app.services.supabase_client import supabase

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/marketplace/setter", tags=["marketplace-setup"])


# ---------------------------------------------------------------------------
# Auth helper
# ---------------------------------------------------------------------------

def _decode_setup_token(authorization: str) -> dict[str, Any]:
    """Decode a setup JWT from the Authorization header.

    Minimal validation — the portal issued this token so we trust the
    structure. We verify expiry and extract the location_id (sub).
    """
    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing Bearer token")

    token = authorization[7:]
    try:
        import base64
        parts = token.split(".")
        if len(parts) != 3:
            raise ValueError("bad format")
        payload_b64 = parts[1] + "=" * (4 - len(parts[1]) % 4)
        payload = json.loads(base64.urlsafe_b64decode(payload_b64))

        if payload.get("exp", 0) < datetime.now(timezone.utc).timestamp():
            raise HTTPException(status_code=401, detail="Token expired")

        if payload.get("product") != "setter":
            raise HTTPException(status_code=403, detail="Token not for setter product")

        return payload
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token")


async def _get_oauth_client(location_id: str) -> GHLClient:
    """Build an OAuth-authenticated GHLClient for a location."""
    install = await get_token(location_id)
    if not install:
        raise HTTPException(status_code=404, detail="No OAuth install found for this location")

    install = await ensure_fresh_token(install)

    async def _on_refresh(at: str, rt: str, exp: datetime) -> None:
        from app.marketplace.oauth_store import update_tokens
        await update_tokens(location_id, access_token=at, refresh_token=rt, expires_at=exp)

    return GHLClient(
        location_id=location_id,
        access_token=install["access_token"],
        refresh_token=install["refresh_token"],
        token_expires_at=install["expires_at"],
        on_token_refresh=_on_refresh,
    )


# ---------------------------------------------------------------------------
# Request/Response models
# ---------------------------------------------------------------------------

class ProvisionRequest(BaseModel):
    business_name: str
    niche: str = "base"
    setter_name: str = "Alex"
    tone: str = "friendly"
    services: list[dict[str, Any]] = Field(default_factory=list)
    calendar_id: str = ""
    calendar_name: str = "Discovery Call"
    booking_link: str = ""
    booking_mode: str = "conversational"
    offer_enabled: bool = False
    offer_description: str = ""
    offer_value: str = ""
    service_area: str = ""
    business_phone: str = ""
    business_email: str = ""
    business_website: str = ""
    timezone: str = "America/Chicago"
    contact_name: str = ""


class ConfigPatchRequest(BaseModel):
    system_config: dict[str, Any]


# ---------------------------------------------------------------------------
# Provision endpoint — creates entity + system_config + GHL assets
# ---------------------------------------------------------------------------

@router.post("/provision")
async def provision_setter(
    body: ProvisionRequest,
    authorization: str = Header(...),
) -> dict[str, Any]:
    payload = _decode_setup_token(authorization)
    location_id = payload["sub"]

    entities = await supabase._get_entities_by_location_id(location_id)
    if entities:
        raise HTTPException(
            status_code=409,
            detail=f"Entity already exists for location {location_id}",
        )

    client = await _get_oauth_client(location_id)

    logger.info("PROVISION | start | location=%s | business=%s", location_id, body.business_name)

    asset_manifest = await provision_ghl_assets(client)

    if body.calendar_id and asset_manifest.get("pipeline", {}).get("id"):
        pipeline = asset_manifest["pipeline"]
        body_services = body.services
    else:
        body_services = body.services

    config = build_config(
        niche=body.niche,
        business_name=body.business_name,
        setter_name=body.setter_name,
        tone=body.tone,
        services=body_services if body_services else None,
        calendar_id=body.calendar_id,
        calendar_name=body.calendar_name,
        booking_link=body.booking_link,
        booking_mode=body.booking_mode,
        offer_enabled=body.offer_enabled,
        offer_description=body.offer_description,
        offer_value=body.offer_value,
    )

    tenant_rows = await supabase.rest_get("main", "/tenants?select=id&limit=1")
    if not tenant_rows:
        raise HTTPException(status_code=500, detail="No tenant found — system not initialized")
    tenant_id = tenant_rows[0]["id"]

    entity_id = str(uuid.uuid4())
    entity_data = {
        "id": entity_id,
        "entity_type": "client",
        "name": body.business_name,
        "contact_name": body.contact_name or body.business_name,
        "contact_email": body.business_email,
        "contact_phone": body.business_phone,
        "ghl_location_id": location_id,
        "timezone": body.timezone,
        "business_phone": body.business_phone,
        "system_config": config,
        "tenant_id": tenant_id,
        "journey_stage": "Onboarding",
        "status": "active",
    }

    await supabase._request(
        supabase.main_client, "POST", "/entities",
        json=entity_data,
        label="create_entity",
    )

    await link_entity(location_id, entity_id)

    logger.info(
        "PROVISION | complete | entity=%s | location=%s | niche=%s",
        entity_id, location_id, body.niche,
    )

    return {
        "entity_id": entity_id,
        "location_id": location_id,
        "asset_manifest": asset_manifest,
        "status": "provisioned",
    }


# ---------------------------------------------------------------------------
# Config CRUD
# ---------------------------------------------------------------------------

@router.get("/config/{location_id}")
async def get_config(
    location_id: str,
    authorization: str = Header(...),
) -> dict[str, Any]:
    payload = _decode_setup_token(authorization)
    if payload["sub"] != location_id:
        raise HTTPException(status_code=403, detail="Token location mismatch")

    try:
        entity = await supabase.resolve_entity_by_location_id(location_id)
    except (ValueError, RuntimeError) as e:
        raise HTTPException(status_code=404, detail=str(e))

    return {
        "entity_id": entity["id"],
        "business_name": entity.get("name", ""),
        "system_config": entity.get("system_config", {}),
    }


@router.patch("/config/{location_id}")
async def patch_config(
    location_id: str,
    body: ConfigPatchRequest,
    authorization: str = Header(...),
) -> dict[str, Any]:
    payload = _decode_setup_token(authorization)
    if payload["sub"] != location_id:
        raise HTTPException(status_code=403, detail="Token location mismatch")

    try:
        entity = await supabase.resolve_entity_by_location_id(location_id)
    except (ValueError, RuntimeError) as e:
        raise HTTPException(status_code=404, detail=str(e))

    existing_config = entity.get("system_config", {})
    merged = _deep_merge(existing_config, body.system_config)

    await supabase.update_entity_field(entity["id"], "system_config", merged)

    logger.info(
        "CONFIG | patched | entity=%s | keys=%s",
        entity["id"], list(body.system_config.keys()),
    )

    return {"status": "updated", "entity_id": entity["id"]}


# ---------------------------------------------------------------------------
# GHL data — for portal dropdowns
# ---------------------------------------------------------------------------

@router.get("/ghl-data/{location_id}")
async def get_ghl_data(
    location_id: str,
    authorization: str = Header(...),
) -> dict[str, Any]:
    payload = _decode_setup_token(authorization)
    if payload["sub"] != location_id:
        raise HTTPException(status_code=403, detail="Token location mismatch")

    client = await _get_oauth_client(location_id)

    calendars = await client.get_calendars()
    pipelines = await client.get_pipelines()

    users: list[dict[str, Any]] = []
    try:
        users = await client.get_location_users()
    except Exception as e:
        logger.warning("GHL_DATA | users fetch failed | %s", e)

    return {
        "calendars": calendars,
        "pipelines": pipelines,
        "users": users,
    }


@router.get("/niches")
async def list_niches() -> dict[str, Any]:
    return {"niches": AVAILABLE_NICHES}


@router.post("/validate-token")
async def validate_token(
    authorization: str = Header(...),
) -> dict[str, Any]:
    payload = _decode_setup_token(authorization)
    location_id = payload["sub"]

    install = await get_token(location_id)
    has_entity = False
    if install and install.get("entity_id"):
        has_entity = True

    return {
        "valid": True,
        "location_id": location_id,
        "has_install": install is not None,
        "has_entity": has_entity,
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _deep_merge(base: dict, override: dict) -> dict:
    """Recursively merge override into base. Override wins on conflicts."""
    result = base.copy()
    for key, val in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(val, dict):
            result[key] = _deep_merge(result[key], val)
        else:
            result[key] = val
    return result
