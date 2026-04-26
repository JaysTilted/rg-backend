"""
API endpoints for GHL custom field management.

Proxies requests to GHL's API using per-client credentials stored in Supabase.
Used by the frontend Data Collection tab to list and create custom fields.
"""

import logging
from datetime import datetime
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from app.services.ghl_client import GHLClient
from app.services.supabase_client import supabase

logger = logging.getLogger(__name__)
router = APIRouter()

# System GHL custom fields that must not appear in the mapping dropdown.
# These are used internally by the pipeline/workflows — overwriting them breaks things.
# Matched against the fieldKey (e.g., "contact.smartfollowup_timer") which is stable
# and case-consistent, unlike display names which vary per location.
_SYSTEM_GHL_FIELD_KEYS = frozenset({
    # Pipeline system fields
    "contact.smartfollowup_timer",
    "contact.smartfollowup_reason",
    "contact.qualification_status",
    "contact.qualification_notes",
    "contact.ai_processing",
    "contact.chathistory",
    "contact.last_ai_channel",
    "contact.last_activity_datetime",
    # GHL workflow fields
    "contact.agent_type_new",
    "contact.channel",
    "contact.lead_source",
    "contact.form_service_interest",
    "contact.next_ai_follow_up",
    "contact.ai_bot",
    # AI reply storage
    "contact.response",
    "contact.response2",
    "contact.response3",
    "contact.response4",
    "contact.response5",
    "contact.response1_media_url",
    # Outreach template fields
    "contact.at1", "contact.at2", "contact.at3", "contact.at4", "contact.at5",
    "contact.at6", "contact.at7", "contact.at8", "contact.at9", "contact.at10",
    "contact.dm1", "contact.dm2", "contact.dm3", "contact.dm4", "contact.dm5",
    "contact.dm6", "contact.dm7", "contact.dm8", "contact.dm9", "contact.dm10",
    "contact.dmbody",
    "contact.masterattachments",
    "contact.mastermessage",
})

# Standard GHL contact fields available for extraction mapping.
# These are built-in contact properties (not custom fields) that can be
# written via PUT /contacts/{id}. Shown in a separate "Contact Fields"
# section in the frontend dropdown.
GHL_CONTACT_FIELDS = [
    {"key": "city", "name": "City", "dataType": "TEXT", "section": "contact"},
    {"key": "state", "name": "State", "dataType": "TEXT", "section": "contact"},
    {"key": "country", "name": "Country", "dataType": "TEXT", "section": "contact"},
    {"key": "postalCode", "name": "Postal Code", "dataType": "TEXT", "section": "contact"},
    {"key": "address1", "name": "Address", "dataType": "TEXT", "section": "contact"},
    {"key": "companyName", "name": "Company Name", "dataType": "TEXT", "section": "contact"},
    {"key": "website", "name": "Website", "dataType": "TEXT", "section": "contact"},
    {"key": "dateOfBirth", "name": "Date of Birth", "dataType": "DATE", "section": "contact"},
    {"key": "timezone", "name": "Timezone", "dataType": "TEXT", "section": "contact"},
    {"key": "source", "name": "Source", "dataType": "TEXT", "section": "contact"},
]


class CreateFieldRequest(BaseModel):
    name: str
    data_type: str = "TEXT"


async def _get_ghl_client(entity_id: str, is_bot: bool = False) -> GHLClient:
    """Build a GHLClient from entity credentials. OAuth preferred, PIT fallback."""
    try:
        row = await supabase.resolve_entity(entity_id)
    except ValueError:
        raise HTTPException(status_code=404, detail=f"Entity {entity_id} not found")

    if not row:
        raise HTTPException(status_code=404, detail=f"Entity {entity_id} not found")

    location_id = row.get("ghl_location_id", "")
    if not location_id:
        raise HTTPException(
            status_code=400,
            detail="GHL not configured for this entity (missing location_id)",
        )

    from app.marketplace.oauth_store import get_token_by_entity, update_tokens
    install = await get_token_by_entity(entity_id)
    if install and install.get("access_token"):
        async def _persist_refresh(access: str, refresh: str, expires: datetime) -> None:
            await update_tokens(
                install["location_id"],
                access_token=access, refresh_token=refresh, expires_at=expires,
            )
        return GHLClient(
            location_id=location_id,
            access_token=install["access_token"],
            refresh_token=install.get("refresh_token", ""),
            token_expires_at=install.get("expires_at"),
            on_token_refresh=_persist_refresh,
        )

    api_key = row.get("ghl_api_key", "")
    if not api_key:
        raise HTTPException(
            status_code=400,
            detail="GHL not configured for this entity (no OAuth install and no api_key)",
        )
    return GHLClient(api_key=api_key, location_id=location_id)


@router.get("/api/ghl/{entity_id}/custom-fields")
async def list_custom_fields(
    entity_id: str,
    is_bot: bool = Query(default=False),
) -> dict[str, Any]:
    """List GHL fields for an entity: contact fields + custom fields (system fields hidden)."""
    ghl = await _get_ghl_client(entity_id, is_bot)

    try:
        all_fields = await ghl.get_custom_field_defs_full()
    except Exception as e:
        logger.warning("GHL custom field list failed: %s", e)
        raise HTTPException(status_code=502, detail=f"GHL API error: {e}")

    # Filter out system fields by fieldKey (stable, case-consistent)
    custom_fields = [
        {**f, "section": "custom"}
        for f in all_fields
        if f["key"] not in _SYSTEM_GHL_FIELD_KEYS
    ]

    # Return contact fields first, then custom fields
    return {
        "contact_fields": GHL_CONTACT_FIELDS,
        "custom_fields": custom_fields,
        "total": len(GHL_CONTACT_FIELDS) + len(custom_fields),
    }


@router.post("/api/ghl/{entity_id}/custom-fields")
async def create_custom_field(
    entity_id: str,
    body: CreateFieldRequest,
    is_bot: bool = Query(default=False),
) -> dict[str, Any]:
    """Create a new GHL custom field for an entity."""
    ghl = await _get_ghl_client(entity_id, is_bot)

    try:
        field = await ghl.create_custom_field(name=body.name, data_type=body.data_type)
    except Exception as e:
        logger.warning("GHL custom field creation failed: %s", e)
        raise HTTPException(status_code=502, detail=f"GHL API error: {e}")

    return {"field": {**field, "section": "custom"}}


# =========================================================================
# TAGS
# =========================================================================


@router.get("/api/ghl/{entity_id}/tags")
async def list_tags(
    entity_id: str,
    is_bot: bool = Query(default=False),
) -> dict[str, Any]:
    """List all GHL tags for an entity's location."""
    ghl = await _get_ghl_client(entity_id, is_bot)

    try:
        tags = await ghl.list_location_tags()
    except Exception as e:
        logger.warning("GHL tag list failed: %s", e)
        raise HTTPException(status_code=502, detail=f"GHL API error: {e}")

    # Extract tag names from GHL response (format: [{id, name, locationId}])
    tag_names = sorted(set(t.get("name", "") for t in tags if t.get("name")))

    return {
        "tags": tag_names,
        "total": len(tag_names),
    }


# =========================================================================
# CALENDARS
# =========================================================================


@router.get("/api/ghl/{entity_id}/calendars")
async def list_calendars(
    entity_id: str,
    is_bot: bool = Query(default=False),
) -> dict[str, Any]:
    """List GHL calendars for an entity."""
    ghl = await _get_ghl_client(entity_id, is_bot)

    try:
        raw_calendars = await ghl.get_calendars()
    except Exception as e:
        logger.warning("GHL calendar list failed: %s", e)
        raise HTTPException(status_code=502, detail=f"GHL API error: {e}")

    # Return simplified calendar data for the UI
    calendars = [
        {
            "id": c.get("id", ""),
            "name": c.get("name", ""),
            "calendarType": c.get("calendarType", ""),
            "slotDuration": c.get("slotDuration", 30),
            "slotDurationUnit": c.get("slotDurationUnit", "mins"),
            "isActive": c.get("isActive", False),
            "widgetSlug": c.get("widgetSlug", ""),
        }
        for c in raw_calendars
    ]

    return {
        "calendars": calendars,
        "total": len(calendars),
    }
