"""GHL asset definitions for auto-provisioning on new client installs.

Creates custom fields, tags, and a pipeline in the client's GHL sub-account.
"""

from __future__ import annotations

import logging
from typing import Any

from app.services.ghl_client import GHLClient

logger = logging.getLogger(__name__)

CUSTOM_FIELDS = [
    ("smartfollowup_timer", "TEXT"),
    ("smartfollowup_reason", "TEXT"),
    ("qualification_status", "TEXT"),
    ("qualification_notes", "TEXT"),
    ("ai_processing", "TEXT"),
    ("chathistory", "LARGE_TEXT"),
    ("last_ai_channel", "TEXT"),
    ("last_activity_datetime", "TEXT"),
    ("agent_type_new", "TEXT"),
    ("channel", "TEXT"),
    ("lead_source", "TEXT"),
    ("form_service_interest", "TEXT"),
    ("next_ai_follow_up", "TEXT"),
    ("ai_bot", "TEXT"),
    ("response", "LARGE_TEXT"),
    ("response2", "LARGE_TEXT"),
    ("response3", "LARGE_TEXT"),
    ("response4", "LARGE_TEXT"),
    ("response5", "LARGE_TEXT"),
    ("response1_media_url", "TEXT"),
]

TAGS = [
    "stop-bot",
    "AI Test Mode",
    "human_handover",
    "DNC",
    "DNC - No Reply",
    "DNC - Stop",
    "Booked",
    "Qualified",
    "Closed Won",
]

PIPELINE_NAME = "Sales Pipeline"
PIPELINE_STAGES = ["Engaged", "Booked", "Closed Won"]


async def provision_ghl_assets(client: GHLClient) -> dict[str, Any]:
    """Create all required GHL assets in a client sub-account.

    Returns a manifest of created asset IDs for storage.
    """
    manifest: dict[str, Any] = {"custom_fields": {}, "tags": {}, "pipeline": {}}

    existing_fields = await client.get_custom_field_defs()
    existing_field_names = {v.lower(): k for k, v in existing_fields.items()}

    for field_name, data_type in CUSTOM_FIELDS:
        if field_name.lower() in existing_field_names:
            manifest["custom_fields"][field_name] = existing_field_names[field_name.lower()]
            logger.info("PROVISION | field exists | %s", field_name)
            continue
        try:
            result = await client.create_custom_field(field_name, data_type)
            manifest["custom_fields"][field_name] = result.get("id", "")
            logger.info("PROVISION | field created | %s -> %s", field_name, result.get("id"))
        except Exception as e:
            logger.warning("PROVISION | field failed | %s | %s", field_name, e)

    existing_tags = await client.list_location_tags()
    existing_tag_names = {t["name"].lower() for t in existing_tags}

    for tag_name in TAGS:
        if tag_name.lower() in existing_tag_names:
            logger.info("PROVISION | tag exists | %s", tag_name)
            continue
        try:
            result = await client.create_location_tag(tag_name)
            manifest["tags"][tag_name] = result.get("id", "")
            logger.info("PROVISION | tag created | %s", tag_name)
        except Exception as e:
            logger.warning("PROVISION | tag failed | %s | %s", tag_name, e)

    pipelines = await client.get_pipelines()
    existing_pipeline = next(
        (p for p in pipelines if p["name"].lower() == PIPELINE_NAME.lower()),
        None,
    )

    if existing_pipeline:
        manifest["pipeline"] = {
            "id": existing_pipeline["id"],
            "stages": {s["name"]: s["id"] for s in existing_pipeline.get("stages", [])},
        }
        logger.info("PROVISION | pipeline exists | %s", existing_pipeline["id"])
    else:
        try:
            pipeline = await client.create_pipeline(PIPELINE_NAME, PIPELINE_STAGES)
            manifest["pipeline"] = {
                "id": pipeline.get("id", ""),
                "stages": {s["name"]: s["id"] for s in pipeline.get("stages", [])},
            }
            logger.info("PROVISION | pipeline created | %s", pipeline.get("id"))
        except Exception as e:
            logger.warning("PROVISION | pipeline failed | %s", e)

    return manifest
