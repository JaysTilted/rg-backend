"""Cleanup fake-UUID test data from Supabase and Postgres after a test batch."""

from __future__ import annotations

import json
import logging
from typing import Any

import httpx

from app.config import settings
from app.services.postgres_client import postgres
from app.services.supabase_client import supabase

logger = logging.getLogger(__name__)


async def cleanup_test_contacts(
    contact_ids: list[str],
    entity_id: str,
    chat_table: str,
    skip_tool_executions: bool = False,
) -> dict[str, Any]:
    """Delete all rows associated with fake test contact UUIDs.

    Args:
        skip_tool_executions: If True, keep tool_executions and attachments
            (simulator uses this — tool_executions have is_test=true and are
            valuable for the inspector. Attachments reference tool data.)

    Returns a dict with counts of deleted rows per table.
    """
    if not contact_ids:
        return {"skipped": True, "reason": "no contact_ids to clean"}

    stats: dict[str, Any] = {}

    # ------------------------------------------------------------------
    # Chat DB tables (direct SQL via asyncpg)
    # ------------------------------------------------------------------
    try:
        async with postgres.chat_pool.acquire() as conn:
            # Chat history
            if chat_table:
                tag = await conn.execute(
                    f'DELETE FROM "{chat_table}" WHERE session_id = ANY($1::text[])',
                    contact_ids,
                )
                stats["chat_history_deleted"] = _parse_delete_count(tag)

            # Tool executions (skip for simulator — kept with is_test=true)
            if not skip_tool_executions:
                tag = await conn.execute(
                    "DELETE FROM tool_executions WHERE session_id = ANY($1::text[])",
                    contact_ids,
                )
                stats["tool_executions_deleted"] = _parse_delete_count(tag)

                # Attachments — first delete storage files, then DB records
                att_rows = await conn.fetch(
                    "SELECT url FROM attachments WHERE session_id = ANY($1::text[])",
                    contact_ids,
                )
                storage_paths = []
                storage_prefix = f"{settings.supabase_chat_url}/storage/v1/object/public/attachments/"
                for row in att_rows:
                    url = row.get("url", "") or ""
                    if url.startswith(storage_prefix):
                        storage_paths.append(url[len(storage_prefix):])

                if storage_paths:
                    try:
                        async with httpx.AsyncClient(timeout=30.0) as client:
                            resp = await client.request(
                                "DELETE",
                                f"{settings.supabase_chat_url}/storage/v1/object/attachments",
                                headers={
                                    "Authorization": f"Bearer {settings.supabase_chat_key}",
                                    "Content-Type": "application/json",
                                },
                                content=json.dumps({"prefixes": storage_paths}).encode(),
                            )
                            if resp.status_code < 400:
                                stats["storage_files_deleted"] = len(storage_paths)
                            else:
                                logger.warning(
                                    "Storage cleanup HTTP %d: %s",
                                    resp.status_code, resp.text[:200],
                                )
                                stats["storage_files_deleted"] = 0
                                stats["storage_error"] = f"HTTP {resp.status_code}"
                    except Exception as e:
                        logger.warning("Storage cleanup error: %s", e, exc_info=True)
                        stats["storage_error"] = str(e)

                tag = await conn.execute(
                    "DELETE FROM attachments WHERE session_id = ANY($1::text[])",
                    contact_ids,
                )
                stats["attachments_deleted"] = _parse_delete_count(tag)
            else:
                stats["tool_executions_skipped"] = True
                stats["attachments_skipped"] = True
    except Exception as e:
        logger.warning("Chat DB cleanup error: %s", e, exc_info=True)
        stats["chat_db_error"] = str(e)

    # ------------------------------------------------------------------
    # Main DB tables (Supabase REST)
    # ------------------------------------------------------------------
    ids_csv = ",".join(contact_ids)

    for table_name in ["leads", "bookings", "call_logs"]:
        try:
            resp = await supabase._request(
                supabase.main_client,
                "DELETE",
                f"/{table_name}",
                params={"ghl_contact_id": f"in.({ids_csv})"},
                headers={"Prefer": "return=representation"},
                label=f"cleanup_{table_name}",
            )
            stats[f"{table_name}_deleted"] = (
                len(resp) if isinstance(resp, list) else 0
            )
        except Exception as e:
            logger.warning("Cleanup %s error: %s", table_name, e, exc_info=True)
            stats[f"{table_name}_error"] = str(e)

    logger.info("Test cleanup complete: %s contacts, stats=%s", len(contact_ids), stats)
    return stats


def _parse_delete_count(tag: str) -> int:
    """Parse asyncpg DELETE command tag like 'DELETE 5' -> 5."""
    try:
        return int(tag.split()[-1])
    except (ValueError, IndexError):
        return 0
