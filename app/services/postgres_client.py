"""Direct Postgres client — async connection pools for Main DB + Chat History DB.

Uses asyncpg for direct SQL queries where the REST API is insufficient
(chat history inserts, attachment queries with window functions, outreach checks).
"""

from __future__ import annotations

import logging
import ipaddress
from datetime import datetime
from typing import Any
from urllib.parse import urlsplit

import asyncpg

from app.config import settings

logger = logging.getLogger(__name__)


class PostgresClient:
    """Async Postgres connection pools for both databases.

    Created once at app startup, reused across all requests.
    """

    def __init__(self) -> None:
        self.main_pool: asyncpg.Pool | None = None
        self.chat_pool: asyncpg.Pool | None = None

    async def start(self) -> None:
        """Create connection pools. Call once at app startup."""
        import ssl

        def _ssl_for_dsn(dsn: str):
            # Local / host-routed Postgres targets should stay plain TCP.
            host = (urlsplit(dsn).hostname or "").lower()
            if host in {"host.docker.internal", "127.0.0.1", "localhost"}:
                return None
            try:
                ip = ipaddress.ip_address(host)
                if ip.is_loopback or ip.is_private or ip in ipaddress.ip_network("100.64.0.0/10"):
                    return None
            except ValueError:
                pass
            ssl_ctx = ssl.create_default_context()
            ssl_ctx.check_hostname = False
            ssl_ctx.verify_mode = ssl.CERT_NONE
            return ssl_ctx

        main_search_path = f"{settings.supabase_main_schema},public"
        chat_search_path = f"{settings.supabase_chat_schema},public"

        self.main_pool = await asyncpg.create_pool(
            settings.database_url, min_size=2, max_size=10, ssl=_ssl_for_dsn(settings.database_url),
            statement_cache_size=0,
            server_settings={"search_path": main_search_path},
        )
        self.chat_pool = await asyncpg.create_pool(
            settings.database_chat_url, min_size=2, max_size=10, ssl=_ssl_for_dsn(settings.database_chat_url),
            statement_cache_size=0,
            server_settings={"search_path": chat_search_path},
        )

    async def stop(self) -> None:
        """Close pools. Call at app shutdown."""
        if self.main_pool:
            await self.main_pool.close()
        if self.chat_pool:
            await self.chat_pool.close()

    # =========================================================================
    # CHAT HISTORY — READ
    # =========================================================================

    async def get_chat_history(
        self, table: str, contact_id: str, limit: int = 100
    ) -> list[dict[str, Any]]:
        """Get chat history for a contact, newest first.

        Returns list of {role, content, source, channel, timestamp, ghl_message_id, attachment_ids, workflow_run_id}.
        """
        rows = await self.chat_pool.fetch(
            f'SELECT role, content, source, channel, "timestamp", ghl_message_id, attachment_ids, workflow_run_id FROM "{table}" '
            f"WHERE session_id = $1 ORDER BY timestamp DESC LIMIT $2",
            contact_id,
            limit,
        )
        result = []
        for r in rows:
            row_dict: dict[str, Any] = {
                "role": r["role"],
                "content": r["content"],
                "source": r["source"],
                "channel": r["channel"],
                "timestamp": r["timestamp"],
                "ghl_message_id": r["ghl_message_id"],
                "workflow_run_id": r["workflow_run_id"],
            }
            att_ids = r["attachment_ids"]
            row_dict["attachment_ids"] = [str(uid) for uid in att_ids] if att_ids else []
            result.append(row_dict)
        logger.info("PG_READ | get_chat_history | table=%s | contact=%s | limit=%d | rows=%d", table, contact_id, limit, len(result))
        return result

    async def delete_latest_ai_message(
        self, table: str, contact_id: str
    ) -> dict[str, Any] | None:
        """Delete the most recent AI message for a contact.

        Returns the deleted row as a flat dict, or None if no AI message found.
        """
        async with self.chat_pool.acquire() as conn:
            row = await conn.fetchrow(
                f'DELETE FROM "{table}" '
                f"WHERE id = ("
                f'  SELECT id FROM "{table}" '
                f"  WHERE session_id = $1"
                f"  AND role = 'ai'"
                f"  ORDER BY timestamp DESC"
                f"  LIMIT 1"
                f") RETURNING id, session_id, role, content, source, channel, timestamp",
                contact_id,
            )
        if row:
            logger.info("PG_WRITE | delete_latest_ai | table=%s | contact=%s | deleted_id=%s", table, contact_id, row["id"])
            return dict(row)
        logger.info("PG_WRITE | delete_latest_ai | table=%s | contact=%s | no_ai_message_found", table, contact_id)
        return None

    # =========================================================================
    # CHAT HISTORY — WRITE
    # =========================================================================

    async def insert_message(
        self,
        table: str,
        session_id: str,
        message: dict[str, Any],
        lead_id: str | None = None,
        attachment_ids: list[str] | None = None,
        workflow_run_id: str | None = None,
    ) -> None:
        """Insert a single message into chat history.

        Accepts legacy message dict format {type, content, additional_kwargs}
        and decomposes into flat columns for insertion.
        """
        role = message.get("type", "unknown")
        content = message.get("content", "") or ""
        aka = message.get("additional_kwargs") or {}
        source = aka.get("source")
        channel = aka.get("channel")
        ghl_msg_id = aka.get("ghl_message_id")
        await self.chat_pool.execute(
            f'INSERT INTO "{table}" (session_id, role, content, source, channel, "timestamp", lead_id, ghl_message_id, attachment_ids, workflow_run_id) '
            f"VALUES ($1, $2, $3, $4, $5, NOW(), $6, $7, $8, $9)",
            session_id, role, content, source, channel, lead_id, ghl_msg_id,
            attachment_ids or [], workflow_run_id,
        )
        logger.info("PG_WRITE | insert_message | table=%s | contact=%s | type=%s | source=%s", table, session_id, role, source or "")

    async def insert_messages_batch(
        self,
        table: str,
        messages: list[dict[str, Any]],
    ) -> None:
        """Insert multiple messages into chat history.

        Each dict must have: session_id, message (dict with type/content/additional_kwargs), lead_id (str|None).
        Optionally: timestamp (datetime) — defaults to NOW().
        Optionally: ghl_message_id (str) — top-level key or extracted from message dict.
        Optionally: attachment_ids (list[str]) — UUIDs.
        Optionally: workflow_run_id (str) — links message to the workflow run that produced it.
        """
        logger.info("PG_WRITE | insert_messages_batch | table=%s | batch_size=%d", table, len(messages))
        async with self.chat_pool.acquire() as conn:
            async with conn.transaction():
                for msg in messages:
                    ts = msg.get("timestamp")
                    msg_dict = msg["message"]
                    aka = msg_dict.get("additional_kwargs") or {}
                    ghl_msg_id = msg.get("ghl_message_id") or aka.get("ghl_message_id")
                    role = msg_dict.get("type", "unknown")
                    content = msg_dict.get("content", "") or ""
                    source = aka.get("source")
                    channel = aka.get("channel")
                    att_ids = msg.get("attachment_ids") or []
                    wf_run_id = msg.get("workflow_run_id")
                    if ts:
                        await conn.execute(
                            f'INSERT INTO "{table}" (session_id, role, content, source, channel, "timestamp", lead_id, ghl_message_id, attachment_ids, workflow_run_id) '
                            f"VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
                            msg["session_id"], role, content, source, channel, ts,
                            msg.get("lead_id"), ghl_msg_id, att_ids, wf_run_id,
                        )
                    else:
                        await conn.execute(
                            f'INSERT INTO "{table}" (session_id, role, content, source, channel, "timestamp", lead_id, ghl_message_id, attachment_ids, workflow_run_id) '
                            f"VALUES ($1, $2, $3, $4, $5, NOW(), $6, $7, $8, $9)",
                            msg["session_id"], role, content, source, channel,
                            msg.get("lead_id"), ghl_msg_id, att_ids, wf_run_id,
                        )
        logger.info("PG_WRITE | insert_messages_batch | table=%s | inserted=%d", table, len(messages))

    async def backfill_ghl_message_id(
        self,
        table: str,
        session_id: str,
        timestamp: Any,
        ghl_message_id: str,
    ) -> bool:
        """Backfill ghl_message_id on an existing chat history row.

        Used by conversation_sync when a GHL outbound message matches
        an AI-stored message (from delivery.py) by exact text content.
        Matches by session_id + exact timestamp + ghl_message_id IS NULL.
        """
        result = await self.chat_pool.execute(
            f'UPDATE "{table}" SET ghl_message_id = $1 '
            f"WHERE session_id = $2 AND timestamp = $3 AND ghl_message_id IS NULL",
            ghl_message_id,
            session_id,
            timestamp,
        )
        updated = result == "UPDATE 1"
        logger.info(
            "PG_WRITE | backfill_ghl_message_id | table=%s | contact=%s | ghl_id=%s | updated=%s",
            table, session_id, ghl_message_id[:20] if ghl_message_id else "", updated,
        )
        return updated

    # =========================================================================
    # OUTREACH CHECK
    # =========================================================================

    async def check_outreach_exists(self, table: str, contact_id: str) -> bool:
        """Check if outreach drip messages already exist in chat history."""
        row = await self.chat_pool.fetchrow(
            f'SELECT 1 FROM "{table}" WHERE session_id = $1 AND source = \'drip_sequence\' LIMIT 1',
            contact_id,
        )
        exists = row is not None
        logger.info("PG_READ | check_outreach_exists | table=%s | contact=%s | exists=%s", table, contact_id, exists)
        return exists

    # =========================================================================
    # ATTACHMENTS
    # =========================================================================

    async def get_attachments(
        self, contact_id: str, entity_id: str
    ) -> list[dict[str, Any]]:
        """Get all attachments for a contact with type index (for timeline)."""
        rows = await self.chat_pool.fetch(
            'SELECT *, ROW_NUMBER() OVER (PARTITION BY type ORDER BY created_at) as type_index '
            'FROM attachments '
            'WHERE session_id = $1 AND "client/bot_id" = $2 '
            'ORDER BY created_at',
            contact_id,
            entity_id,
        )
        result = [dict(r) for r in rows]
        logger.info("PG_READ | get_attachments | contact=%s | rows=%d", contact_id, len(result))
        return result

    async def insert_attachment(self, data: dict[str, Any]) -> dict[str, Any]:
        """Insert a new attachment record (upsert on session_id + url)."""
        logger.info("PG_WRITE | insert_attachment | contact=%s | type=%s | ghl_msg=%s | url=%s",
                     data.get("session_id"), data.get("type"),
                     data.get("ghl_message_id", "")[:20], (data.get("url") or "")[:80])
        row = await self.chat_pool.fetchrow(
            'INSERT INTO attachments '
            '(session_id, "client/bot_id", lead_id, type, url, original_url, '
            'ghl_message_id, description, raw_analysis, message_timestamp) '
            'VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) '
            'ON CONFLICT (session_id, url) DO UPDATE SET '
            'description = EXCLUDED.description, '
            'raw_analysis = EXCLUDED.raw_analysis, '
            'message_timestamp = EXCLUDED.message_timestamp, '
            'lead_id = EXCLUDED.lead_id, '
            'ghl_message_id = EXCLUDED.ghl_message_id '
            'RETURNING *',
            data["session_id"],
            data["client_bot_id"],
            data.get("lead_id"),
            data["type"],
            data["url"],
            data.get("original_url"),
            data.get("ghl_message_id"),
            data.get("description"),
            data.get("raw_analysis"),
            data.get("message_timestamp"),
        )
        return dict(row)

    async def update_attachment_description(
        self, attachment_id: str, description: str
    ) -> None:
        """Update the description of an existing attachment (after synthesis)."""
        await self.chat_pool.execute(
            'UPDATE attachments SET description = $1 WHERE id = $2',
            description,
            attachment_id,
        )
        logger.info("PG_WRITE | update_attachment_description | id=%s | desc_len=%d", attachment_id, len(description))

    async def get_attachment_by_type_index(
        self,
        session_id: str,
        att_type: str,
        att_index: int,
        entity_id: str,
    ) -> dict[str, Any] | None:
        """Look up a specific attachment by type + index (for reanalyze tool).

        Matches n8n query: SELECT url, description, raw_analysis FROM attachments
        WHERE session_id = ? AND type = ? AND "client/bot_id" = ?
        ORDER BY created_at LIMIT 1 OFFSET (index - 1)
        """
        row = await self.chat_pool.fetchrow(
            'SELECT * FROM attachments '
            'WHERE session_id = $1 AND type = $2 AND "client/bot_id" = $3 '
            'ORDER BY created_at LIMIT 1 OFFSET $4',
            session_id,
            att_type,
            entity_id,
            att_index - 1,  # Convert 1-based index to 0-based offset
        )
        logger.info("PG_READ | get_attachment_by_type_index | contact=%s | type=%s | index=%d | found=%s", session_id, att_type, att_index, row is not None)
        return dict(row) if row else None

    # update_latest_human_message_refs — REMOVED (dead code, never called)

    async def get_processed_attachment_message_ids(
        self, contact_id: str, entity_id: str
    ) -> set[str]:
        """Get ghl_message_ids that already have attachment records."""
        rows = await self.chat_pool.fetch(
            'SELECT DISTINCT ghl_message_id FROM attachments '
            'WHERE session_id = $1 AND "client/bot_id" = $2 '
            'AND ghl_message_id IS NOT NULL',
            contact_id,
            entity_id,
        )
        result = {r["ghl_message_id"] for r in rows}
        logger.info("PG_READ | get_processed_att_msg_ids | contact=%s | count=%d", contact_id, len(result))
        return result

    async def update_message_attachment_ids(
        self, table: str, session_id: str, ghl_message_id: str, attachment_ids: list[str]
    ) -> bool:
        """Set attachment_ids (UUID[]) on a specific message by ghl_message_id."""
        logger.info("PG_WRITE | update_msg_attachment_ids | table=%s | contact=%s | ghl_id=%s | ids=%s",
                     table, session_id, ghl_message_id, attachment_ids)
        result = await self.chat_pool.execute(
            f'UPDATE "{table}" SET attachment_ids = $1::uuid[] '
            f"WHERE session_id = $2 AND ghl_message_id = $3",
            attachment_ids,
            session_id,
            ghl_message_id,
        )
        return "UPDATE 1" in result

    # =========================================================================
    # TOOL EXECUTIONS (logging)
    # =========================================================================

    async def log_tool_execution(self, data: dict[str, Any]) -> None:
        """Log a tool execution to the tool_executions table."""
        logger.info("PG_WRITE | log_tool_execution | tool=%s | contact=%s", data.get("tool_name"), data.get("session_id"))
        await self.chat_pool.execute(
            'INSERT INTO tool_executions '
            '(session_id, client_id, tool_name, tool_input, tool_output, '
            'channel, execution_id, test_mode, lead_id) '
            'VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)',
            data.get("session_id"),
            data.get("client_id"),
            data.get("tool_name"),
            data.get("tool_input"),
            data.get("tool_output"),
            data.get("channel"),
            data.get("execution_id"),
            data.get("test_mode", False),
            data.get("lead_id"),
        )


# Singleton — initialized in main.py lifespan
postgres = PostgresClient()
