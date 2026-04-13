"""Supabase client — Main DB + Chat History DB via REST API (httpx async)."""

from __future__ import annotations

import logging
import time
from typing import Any

import httpx

from app.config import settings

logger = logging.getLogger(__name__)


class SupabaseClient:
    """Async Supabase REST client for both databases.

    Uses a shared httpx.AsyncClient per database for connection pooling.
    Created once at app startup, reused across all requests.
    """

    def __init__(self) -> None:
        self.main_client: httpx.AsyncClient | None = None
        self.chat_client: httpx.AsyncClient | None = None

    @staticmethod
    def _schema_headers(schema: str) -> dict[str, str]:
        """Headers that bind PostgREST requests to a specific exposed schema."""
        return {
            "Accept-Profile": schema,
            "Content-Profile": schema,
        }

    async def start(self) -> None:
        """Create persistent HTTP clients. Call once at app startup."""
        self.main_client = httpx.AsyncClient(
            base_url=f"{settings.supabase_main_url}/rest/v1",
            headers={
                "apikey": settings.supabase_main_key,
                "Authorization": f"Bearer {settings.supabase_main_key}",
                "Content-Type": "application/json",
                **self._schema_headers(settings.supabase_main_schema),
            },
            timeout=15.0,
        )
        self.chat_client = httpx.AsyncClient(
            base_url=f"{settings.supabase_chat_url}/rest/v1",
            headers={
                "apikey": settings.supabase_chat_key,
                "Authorization": f"Bearer {settings.supabase_chat_key}",
                "Content-Type": "application/json",
                **self._schema_headers(settings.supabase_chat_schema),
            },
            timeout=15.0,
        )

    async def stop(self) -> None:
        """Close HTTP clients. Call at app shutdown."""
        if self.main_client:
            await self.main_client.aclose()
        if self.chat_client:
            await self.chat_client.aclose()

    # =========================================================================
    # CENTRAL HTTP — all Supabase REST calls flow through here
    # =========================================================================

    _RETRYABLE_STATUSES = {429, 500, 502, 503, 504}

    async def _request(
        self,
        client: httpx.AsyncClient,
        method: str,
        path: str,
        *,
        params: dict | None = None,
        json: Any = None,
        headers: dict | None = None,
        label: str = "",
        max_retries: int = 2,
    ) -> httpx.Response:
        """Central Supabase HTTP request with timing, logging, and transient retry.

        Every public method calls this instead of client.get/post/patch directly.
        Any new method added later automatically gets HTTP logging and retry for free.

        Retries on: 429, 500, 502, 503, 504 status codes and timeouts.
        """
        import asyncio

        t0 = time.perf_counter()
        last_resp = None
        for attempt in range(max_retries + 1):
            try:
                resp = await client.request(method, path, params=params, json=json, headers=headers)
            except httpx.TimeoutException:
                if attempt < max_retries:
                    wait = 1.0 * (2 ** attempt)
                    logger.warning(
                        "SB_HTTP | %s | %s %s | timeout | retry in %.1fs (attempt %d/%d)",
                        label, method, path, wait, attempt + 1, max_retries + 1,
                    )
                    await asyncio.sleep(wait)
                    continue
                elapsed_ms = int((time.perf_counter() - t0) * 1000)
                logger.error("SB_HTTP | %s | %s %s | timeout after %d retries | elapsed_ms=%d", label, method, path, max_retries + 1, elapsed_ms)
                raise

            last_resp = resp
            if resp.status_code in self._RETRYABLE_STATUSES and attempt < max_retries:
                wait = 1.0 * (2 ** attempt)
                logger.warning(
                    "SB_HTTP | %s | %s %s | %d (transient) | retry in %.1fs (attempt %d/%d)",
                    label, method, path, resp.status_code, wait, attempt + 1, max_retries + 1,
                )
                await asyncio.sleep(wait)
                continue

            elapsed_ms = int((time.perf_counter() - t0) * 1000)
            if resp.status_code >= 400:
                logger.warning("SB_HTTP | %s | %s %s | status=%d | elapsed_ms=%d", label, method, path, resp.status_code, elapsed_ms)
            else:
                logger.info("SB_HTTP | %s | %s %s | status=%d | elapsed_ms=%d", label, method, path, resp.status_code, elapsed_ms)
            return resp

        elapsed_ms = int((time.perf_counter() - t0) * 1000)
        logger.warning(
            "SB_HTTP | %s | %s %s | retries exhausted (last=%d) | elapsed_ms=%d",
            label, method, path, last_resp.status_code if last_resp else 0, elapsed_ms,
        )
        return last_resp

    # =========================================================================
    # ENTITY CONFIG
    # =========================================================================

    async def _get_entity(self, entity_id: str) -> dict[str, Any] | None:
        """Query the entities table by ID."""
        resp = await self._request(
            self.main_client, "GET", "/entities",
            params={"id": f"eq.{entity_id}", "select": "*"},
            label="get_entity",
        )
        data = resp.json()
        if isinstance(data, list) and data:
            return data[0]
        return None

    async def _get_entities_by_location_id(
        self,
        ghl_location_id: str,
        *,
        active_only: bool = True,
    ) -> list[dict[str, Any]]:
        """Query the entities table by GHL location ID."""
        params = {
            "ghl_location_id": f"eq.{ghl_location_id}",
            "select": "*",
        }
        if active_only:
            params["status"] = "eq.active"
        resp = await self._request(
            self.main_client,
            "GET",
            "/entities",
            params=params,
            label="get_entities_by_location_id",
        )
        data = resp.json()
        return data if isinstance(data, list) else []

    async def resolve_entity(self, entity_id: str) -> dict[str, Any]:
        """Fetch entity config from the entities table.

        Returns the config dict. Raises ValueError if not found.
        """
        row = await self._get_entity(entity_id)
        if row:
            logger.info("SB | resolve_entity | entity=%s | found", entity_id)
            return row
        logger.warning("SB | resolve_entity | entity=%s | NOT FOUND", entity_id)
        raise ValueError(f"Entity {entity_id} not found in entities table")

    async def resolve_entity_by_location_id(
        self,
        ghl_location_id: str,
        *,
        active_only: bool = True,
    ) -> dict[str, Any]:
        """Fetch entity config from the entities table by GHL location ID.

        Raises ValueError if not found and RuntimeError if multiple matches exist.
        """
        rows = await self._get_entities_by_location_id(
            ghl_location_id,
            active_only=active_only,
        )
        if not rows:
            logger.warning(
                "SB | resolve_entity_by_location_id | location=%s | NOT FOUND",
                ghl_location_id,
            )
            raise ValueError(
                f"Entity with ghl_location_id {ghl_location_id} not found in entities table"
            )
        if len(rows) > 1:
            logger.error(
                "SB | resolve_entity_by_location_id | location=%s | MULTIPLE MATCHES=%d",
                ghl_location_id,
                len(rows),
            )
            raise RuntimeError(
                f"Multiple active entities found for ghl_location_id {ghl_location_id}"
            )
        row = rows[0]
        logger.info(
            "SB | resolve_entity_by_location_id | location=%s | entity=%s",
            ghl_location_id,
            row.get("id", ""),
        )
        return row

    # =========================================================================
    # TENANT RESOLUTION (multi-tenant API key support)
    # =========================================================================

    _tenant_cache: dict[str, tuple[float, dict[str, Any]]] = {}
    _TENANT_CACHE_TTL = 300  # 5 minutes

    async def get_tenant(self, tenant_id: str) -> dict[str, Any]:
        """Fetch tenant config (incl. API keys) with 5-min cache.

        Returns the full tenant row. Raises ValueError if not found.
        """
        now = time.time()
        if tenant_id in self._tenant_cache:
            cached_at, data = self._tenant_cache[tenant_id]
            if now - cached_at < self._TENANT_CACHE_TTL:
                return data

        resp = await self._request(
            self.main_client, "GET", "/tenants",
            params={"id": f"eq.{tenant_id}", "select": "*"},
            label="get_tenant",
        )
        rows = resp.json()
        if not rows:
            raise ValueError(f"Tenant {tenant_id} not found")
        tenant = rows[0]
        self._tenant_cache[tenant_id] = (now, tenant)
        logger.info("SB | get_tenant | id=%s | name=%s", tenant_id, tenant.get("name"))
        return tenant

    _entity_tenant_cache: dict[str, str] = {}  # entity_id → tenant_id

    async def resolve_tenant_id(self, entity_id: str) -> str | None:
        """Resolve tenant_id from entity_id. Cached per-process.

        Returns just the UUID string (not a full tenant dict).
        For the full tenant dict, use get_tenant_for_entity() instead.
        """
        if entity_id in self._entity_tenant_cache:
            return self._entity_tenant_cache[entity_id]
        resp = await self._request(
            self.main_client, "GET", "/entities",
            params={"id": f"eq.{entity_id}", "select": "tenant_id"},
            label="resolve_tenant_id",
        )
        rows = resp.json()
        tid = rows[0]["tenant_id"] if rows else None
        if tid:
            self._entity_tenant_cache[entity_id] = tid
        return tid

    async def get_tenant_for_entity(self, entity_id: str) -> dict[str, Any]:
        """Resolve entity -> tenant config with caching.

        Looks up entity's tenant_id, then fetches the tenant row.
        """
        entity = await self.resolve_entity(entity_id)
        tenant_id = entity.get("tenant_id")
        if not tenant_id:
            raise ValueError(f"Entity {entity_id} has no tenant_id")
        return await self.get_tenant(tenant_id)

    # =========================================================================
    # LEADS
    # =========================================================================

    async def get_lead(
        self, contact_id: str, entity_id: str
    ) -> dict[str, Any] | None:
        """Get the most recent lead for a contact (newest opt-in)."""
        resp = await self._request(
            self.main_client, "GET", "/leads",
            params={
                "ghl_contact_id": f"eq.{contact_id}",
                "entity_id": f"eq.{entity_id}",
                "order": "created_at.desc",
                "limit": "1",
            },
            label="get_lead",
        )
        data = resp.json()
        result = data[0] if data else None
        logger.info("SB | get_lead | contact=%s | found=%s", contact_id, result is not None)
        return result

    async def create_lead(self, data: dict[str, Any]) -> dict[str, Any]:
        """Create a new lead record in the leads table."""
        if "tenant_id" not in data and "entity_id" in data:
            tid = await self.resolve_tenant_id(str(data["entity_id"]))
            if tid:
                data["tenant_id"] = tid
        resp = await self._request(
            self.main_client, "POST", "/leads",
            json=data,
            headers={"Prefer": "return=representation"},
            label="create_lead",
        )
        logger.info("SB | create_lead | table=leads | contact=%s", data.get("ghl_contact_id"))
        return resp.json()[0]

    async def update_lead(
        self, lead_id: str, updates: dict[str, Any]
    ) -> None:
        """Update lead fields in the leads table."""
        await self._request(
            self.main_client, "PATCH", "/leads",
            params={"id": f"eq.{lead_id}"},
            json=updates,
            label="update_lead",
        )
        logger.info("SB | update_lead | lead=%s | fields=%s", lead_id, list(updates.keys()))

    # =========================================================================
    # OUTREACH TEMPLATES
    # =========================================================================

    async def get_outreach_templates(
        self, entity_id: str
    ) -> list[dict[str, Any]]:
        """Fetch all active outreach templates for an entity."""
        resp = await self._request(
            self.main_client, "GET", "/outreach_templates",
            params={
                "entity_id": f"eq.{entity_id}",
                "is_active": "eq.true",
                "select": "*",
            },
            label="get_outreach_templates",
        )
        data = resp.json()
        result = data if isinstance(data, list) else []
        logger.info("SB | get_outreach_templates | entity=%s | count=%d", entity_id, len(result))
        return result

    # =========================================================================
    # BOOKINGS
    # =========================================================================

    async def get_bookings(
        self, contact_id: str, entity_id: str
    ) -> list[dict[str, Any]]:
        """Get active (non-cancelled, upcoming) bookings for a contact.

        Filters to appointment_datetime > now - 12h to match n8n's
        Check Booking Status query. Without this, ancient bookings
        would incorrectly flag has_upcoming_booking as True.
        """
        from datetime import datetime, timedelta, timezone as tz

        cutoff = (datetime.now(tz.utc) - timedelta(hours=12)).isoformat()
        resp = await self._request(
            self.main_client, "GET", "/bookings",
            params={
                "ghl_contact_id": f"eq.{contact_id}",
                "entity_id": f"eq.{entity_id}",
                "status": "neq.cancelled",
                "appointment_datetime": f"gt.{cutoff}",
                "order": "appointment_datetime.asc",
            },
            label="get_bookings",
        )
        result = resp.json()
        logger.info("SB | get_bookings | contact=%s | count=%d", contact_id, len(result) if isinstance(result, list) else 0)
        return result

    async def find_booking_by_appointment(
        self, appointment_id: str, entity_id: str
    ) -> dict[str, Any] | None:
        """Find an existing booking by GHL appointment ID."""
        resp = await self._request(
            self.main_client, "GET", "/bookings",
            params={
                "entity_id": f"eq.{entity_id}",
                "ghl_appointment_id": f"eq.{appointment_id}",
                "limit": "1",
            },
            label="find_booking_by_appointment",
        )
        rows = resp.json()
        return rows[0] if rows else None

    # =========================================================================
    # CALL LOGS
    # =========================================================================

    async def get_call_logs(
        self,
        contact_id: str,
        entity_id: str,
        limit: int = 5,
    ) -> list[dict[str, Any]]:
        """Get recent call logs for a contact from the call_logs table."""
        resp = await self._request(
            self.main_client, "GET", "/call_logs",
            params={
                "ghl_contact_id": f"eq.{contact_id}",
                "entity_id": f"eq.{entity_id}",
                "order": "created_at.desc",
                "limit": str(limit),
            },
            label="get_call_logs",
        )
        result = resp.json()
        logger.info("SB | get_call_logs | contact=%s | count=%d", contact_id, len(result) if isinstance(result, list) else 0)
        return result

    # =========================================================================
    # BOOKINGS (insert/update for tools)
    # =========================================================================

    async def create_booking(self, data: dict[str, Any]) -> dict[str, Any]:
        """Insert a new booking record into the bookings table."""
        if "tenant_id" not in data and "entity_id" in data:
            tid = await self.resolve_tenant_id(str(data["entity_id"]))
            if tid:
                data["tenant_id"] = tid
        resp = await self._request(
            self.main_client, "POST", "/bookings",
            json=data,
            headers={"Prefer": "return=representation"},
            label="create_booking",
        )
        logger.info("SB | create_booking | table=bookings | contact=%s", data.get("ghl_contact_id"))
        return resp.json()[0]

    async def update_booking(
        self, booking_id: str, updates: dict[str, Any]
    ) -> None:
        """Update booking fields in the bookings table."""
        await self._request(
            self.main_client, "PATCH", "/bookings",
            params={"id": f"eq.{booking_id}"},
            json=updates,
            label="update_booking",
        )
        logger.info("SB | update_booking | booking=%s | fields=%s", booking_id, list(updates.keys()))

    # =========================================================================
    # CALL LOGS (insert for GHL sync)
    # =========================================================================

    async def insert_call_log(self, data: dict[str, Any]) -> dict[str, Any]:
        """Insert a call log record into the call_logs table."""
        if "tenant_id" not in data and "entity_id" in data:
            tid = await self.resolve_tenant_id(str(data["entity_id"]))
            if tid:
                data["tenant_id"] = tid
        resp = await self._request(
            self.main_client, "POST", "/call_logs",
            json=data,
            headers={"Prefer": "return=representation"},
            label="insert_call_log",
        )
        logger.info("SB | insert_call_log | table=call_logs | contact=%s", data.get("ghl_contact_id"))
        return resp.json()[0]

    # =========================================================================
    # KNOWLEDGE BASE (vector search)
    # =========================================================================

    async def search_knowledge_base(
        self, query_embedding: list[float], entity_id: str, match_count: int = 5
    ) -> list[dict[str, Any]]:
        """Vector search against the documents table via RPC.

        Filters by entity_id column directly (no metadata filtering).
        """
        resp = await self._request(
            self.main_client, "POST", "/rpc/match_documents",
            json={
                "query_embedding": query_embedding,
                "match_count": match_count,
                "p_entity_id": entity_id,
            },
            label="search_knowledge_base",
        )
        result = resp.json()
        logger.info("SB | search_knowledge_base | entity=%s | results=%d", entity_id, len(result) if isinstance(result, list) else 0)
        return result

    async def delete_documents(
        self, entity_id: str, category: str, *, article_id: str | None = None,
    ) -> None:
        """Delete document chunks for a given article or entity + category.

        When article_id is provided, deletes by article_id (precise).
        Otherwise deletes by entity_id column + metadata category.
        """
        if article_id:
            await self._request(
                self.main_client, "DELETE", "/documents",
                params={"article_id": f"eq.{article_id}"},
                label="delete_documents",
            )
        else:
            await self._request(
                self.main_client, "DELETE", "/documents",
                params={
                    "entity_id": f"eq.{entity_id}",
                    "metadata->>Category": f"eq.{category}",
                },
                label="delete_documents",
            )

    async def insert_documents(self, rows: list[dict[str, Any]]) -> None:
        """Batch insert document chunks with embeddings to the documents table.

        Each row should have: content, embedding, entity_id, tenant_id,
        metadata ({Category, Client_ID}), and optionally article_id.
        """
        await self._request(
            self.main_client, "POST", "/documents",
            json=rows,
            label="insert_documents",
        )

    # =========================================================================
    # STORAGE — Attachment file uploads (Chat History DB project)
    # =========================================================================

    async def upload_attachment(
        self, path: str, file_data: bytes, mime_type: str
    ) -> str:
        """Upload a file to Supabase Storage and return the public URL.

        Uses the 'attachments' bucket on the Chat History Supabase project.
        Path format: {contact_id}/{type}/{timestamp}_{index}_{uuid}

        Returns the permanent public URL for the uploaded file.
        """
        logger.info("SB | upload_attachment | path=%s | mime=%s | bytes=%d", path, mime_type, len(file_data))
        upload_url = (
            f"{settings.supabase_chat_url}/storage/v1/object/attachments/{path}"
        )
        t0 = time.perf_counter()
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                upload_url,
                content=file_data,
                headers={
                    "Authorization": f"Bearer {settings.supabase_chat_key}",
                    "Content-Type": mime_type,
                },
            )
            resp.raise_for_status()
        elapsed_ms = int((time.perf_counter() - t0) * 1000)

        public_url = f"{settings.supabase_chat_url}/storage/v1/object/public/attachments/{path}"
        logger.info("SB | upload_attachment | done | elapsed_ms=%d | url=%s", elapsed_ms, public_url[:80])
        return public_url

    # =========================================================================
    # TOOL EXECUTIONS (Chat History DB)
    # =========================================================================

    async def insert_tool_execution(self, data: dict[str, Any]) -> None:
        """Log a tool execution to the chat history DB."""
        await self._request(
            self.chat_client, "POST", "/tool_executions",
            json=data,
            label="insert_tool_execution",
        )

    # =========================================================================
    # GENERIC RPC (for dashboard RPCs, etc.)
    # =========================================================================

    async def rpc(self, db: str, fn_name: str, params: dict[str, Any]) -> Any:
        """Call a Supabase RPC function.

        Args:
            db: 'main' or 'chat' — which database to target.
            fn_name: RPC function name (e.g. 'dashboard_main_stats').
            params: JSON-serializable parameters for the RPC.

        Returns the parsed JSON response (usually a dict or list).
        """
        client = self.main_client if db == "main" else self.chat_client
        resp = await self._request(
            client, "POST", f"/rpc/{fn_name}",
            json=params,
            label=f"rpc_{fn_name}",
        )
        return resp.json()

    # =========================================================================
    # DAILY REPORTS (queries used by the daily reports workflow)
    # =========================================================================

    async def get_active_clients(self) -> list[dict[str, Any]]:
        """Fetch all active client entities (journey_stage != churned) for daily reports."""
        resp = await self._request(
            self.main_client, "GET", "/entities",
            params={
                "entity_type": "eq.client",
                "journey_stage": "neq.churned",
                "status": "eq.active",
                "order": "company.asc",
                "select": (
                    "id,company,chat_history_table_name,timezone,"
                    "business_schedule,average_booking_value,"
                    "journey_stage,slack_report_channel_id,"
                    "service_config,billing_config"
                ),
            },
            label="get_active_clients",
        )
        data = resp.json()
        return data if isinstance(data, list) else []

    async def get_active_bots(self) -> list[dict[str, Any]]:
        """Fetch all active internal entities (personal bots) for daily reports."""
        resp = await self._request(
            self.main_client, "GET", "/entities",
            params={
                "entity_type": "eq.internal",
                "is_active": "eq.true",
                "status": "eq.active",
                "order": "name.asc",
                "select": "id,name,chat_history_table_name,average_booking_value",
            },
            label="get_active_bots",
        )
        data = resp.json()
        return data if isinstance(data, list) else []

    async def upsert_daily_stats(
        self,
        entity_id: str,
        snapshot_date: str,
        stats: dict[str, Any],
    ) -> None:
        """Upsert a row into client_daily_stats.

        Uses Supabase's on_conflict for dedup on (entity_id, snapshot_date).
        """
        data: dict[str, Any] = {
            "entity_id": entity_id,
            "snapshot_date": snapshot_date,
            "stats": stats,
        }
        tid = await self.resolve_tenant_id(entity_id)
        if tid:
            data["tenant_id"] = tid
        await self._request(
            self.main_client, "POST", "/client_daily_stats",
            json=data,
            headers={
                "Prefer": "return=representation,resolution=merge-duplicates",
            },
            params={"on_conflict": "entity_id,snapshot_date"},
            label="upsert_daily_stats",
        )

    async def get_daily_stats_history(
        self, entity_id: str, limit: int = 90
    ) -> list[dict[str, Any]]:
        """Fetch historical daily stats snapshots (newest first)."""
        resp = await self._request(
            self.main_client, "GET", "/client_daily_stats",
            params={
                "entity_id": f"eq.{entity_id}",
                "order": "snapshot_date.desc",
                "limit": str(limit),
            },
            label="get_daily_stats_history",
        )
        data = resp.json()
        return data if isinstance(data, list) else []

    async def update_entity_field(
        self, entity_id: str, field: str, value: Any
    ) -> None:
        """Update a single field on the entities table."""
        await self._request(
            self.main_client, "PATCH", "/entities",
            params={"id": f"eq.{entity_id}"},
            json={field: value},
            label=f"update_entity_{field}",
        )

    async def rest_get(self, db: str, path: str) -> list[dict[str, Any]]:
        """Generic REST GET on either database. Returns parsed JSON array."""
        client = self.main_client if db == "main" else self.chat_client
        resp = await self._request(
            client, "GET", path,
            label=f"rest_get_{path[:40]}",
        )
        data = resp.json()
        return data if isinstance(data, list) else []


# Singleton — initialized in main.py lifespan
supabase = SupabaseClient()
