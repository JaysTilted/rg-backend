"""Clean Chat History — migrated from n8n workflow MSkPb2pJ4Cd7KYD8.

Deletes the most recent AI message from a contact's chat history.
Called from the dashboard to undo the last AI response.

Triggered by: POST /webhook/{client_id}/clean-chat-history
"""

from __future__ import annotations

import logging
from typing import Any

from prefect import flow

from app.services.postgres_client import postgres
from app.services.supabase_client import supabase

logger = logging.getLogger(__name__)


@flow(name="clean-chat-history", retries=0)
async def clean_chat_history(entity_id: str, contact_id: str) -> dict[str, Any]:
    """Delete the most recent AI message for a contact.

    Resolves the entity config to find the chat history table,
    then deletes the latest AI message by timestamp.
    """
    logger.info(
        "CLEAN_CHAT | starting | entity=%s contact=%s",
        entity_id, contact_id,
    )

    # 1. Resolve entity to get chat_history_table_name
    try:
        config = await supabase.resolve_entity(entity_id)
    except Exception:
        logger.exception("CLEAN_CHAT | resolve_entity_failed | entity=%s", entity_id)
        return {"action": "error", "reason": "entity not found"}

    table = config.get("chat_history_table_name", "")
    if not table:
        logger.warning("CLEAN_CHAT | no_table | entity=%s", entity_id)
        return {"action": "skipped", "reason": "no chat history table configured"}

    # 2. Delete the most recent AI message
    deleted = await postgres.delete_latest_ai_message(table, contact_id)

    if not deleted:
        logger.info("CLEAN_CHAT | no_ai_message | entity=%s contact=%s", entity_id, contact_id)
        return {"action": "skipped", "reason": "no AI message found"}

    logger.info(
        "CLEAN_CHAT | deleted | entity=%s contact=%s message_id=%s",
        entity_id, contact_id, deleted["id"],
    )

    return {
        "action": "deleted",
        "message_id": str(deleted["id"]),
    }
