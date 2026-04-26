"""GHL OAuth token persistence via asyncpg.

Stores one row per installed location in marketplace.ghl_installs.
access_token rotates every ~24h; refresh_token is long-lived.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from app.services.postgres_client import postgres

logger = logging.getLogger(__name__)


async def upsert_token(
    *,
    location_id: str,
    access_token: str,
    refresh_token: str,
    expires_at: datetime,
    scopes: str = "",
    company_id: str | None = None,
    entity_id: str | None = None,
) -> None:
    """Insert or update the OAuth token row for a location."""
    pool = postgres.main_pool
    if not pool:
        raise RuntimeError("Postgres main pool not initialized")

    now = datetime.now(timezone.utc)
    await pool.execute(
        """
        INSERT INTO marketplace.ghl_installs (
            location_id, company_id, entity_id,
            access_token, refresh_token, expires_at, scopes,
            installed_at, updated_at
        ) VALUES ($1, $2, $3::uuid, $4, $5, $6, $7, $8, $8)
        ON CONFLICT (location_id) DO UPDATE SET
            access_token  = EXCLUDED.access_token,
            refresh_token = EXCLUDED.refresh_token,
            expires_at    = EXCLUDED.expires_at,
            scopes        = EXCLUDED.scopes,
            company_id    = EXCLUDED.company_id,
            updated_at    = EXCLUDED.updated_at
        """,
        location_id, company_id, entity_id,
        access_token, refresh_token, expires_at, scopes,
        now,
    )
    logger.info(
        "MARKETPLACE | token upserted | location=%s | expires=%s",
        location_id, expires_at.isoformat(),
    )


async def get_token(location_id: str) -> dict[str, Any] | None:
    """Fetch the token row for a location, or None if not installed."""
    pool = postgres.main_pool
    if not pool:
        return None

    row = await pool.fetchrow(
        """
        SELECT location_id, company_id, entity_id,
               access_token, refresh_token, expires_at, scopes,
               installed_at, updated_at
        FROM marketplace.ghl_installs
        WHERE location_id = $1
        """,
        location_id,
    )
    if not row:
        return None
    return dict(row)


async def get_token_by_entity(entity_id: str) -> dict[str, Any] | None:
    """Fetch the token row linked to an entity, or None."""
    pool = postgres.main_pool
    if not pool:
        return None

    row = await pool.fetchrow(
        """
        SELECT location_id, company_id, entity_id,
               access_token, refresh_token, expires_at, scopes,
               installed_at, updated_at
        FROM marketplace.ghl_installs
        WHERE entity_id = $1::uuid
        ORDER BY updated_at DESC
        LIMIT 1
        """,
        entity_id,
    )
    if not row:
        return None
    return dict(row)


async def update_tokens(
    location_id: str,
    *,
    access_token: str,
    refresh_token: str,
    expires_at: datetime,
    scopes: str | None = None,
) -> None:
    """Update just the token fields after a refresh."""
    pool = postgres.main_pool
    if not pool:
        raise RuntimeError("Postgres main pool not initialized")

    now = datetime.now(timezone.utc)
    if scopes is not None:
        await pool.execute(
            """
            UPDATE marketplace.ghl_installs
            SET access_token = $2, refresh_token = $3, expires_at = $4,
                scopes = $5, updated_at = $6
            WHERE location_id = $1
            """,
            location_id, access_token, refresh_token, expires_at, scopes, now,
        )
    else:
        await pool.execute(
            """
            UPDATE marketplace.ghl_installs
            SET access_token = $2, refresh_token = $3, expires_at = $4,
                updated_at = $5
            WHERE location_id = $1
            """,
            location_id, access_token, refresh_token, expires_at, now,
        )
    logger.info(
        "MARKETPLACE | token refreshed | location=%s | new_expires=%s",
        location_id, expires_at.isoformat(),
    )


async def link_entity(location_id: str, entity_id: str) -> None:
    """Link an install to an existing entity."""
    pool = postgres.main_pool
    if not pool:
        raise RuntimeError("Postgres main pool not initialized")

    await pool.execute(
        """
        UPDATE marketplace.ghl_installs
        SET entity_id = $2::uuid, updated_at = NOW()
        WHERE location_id = $1
        """,
        location_id, entity_id,
    )
    logger.info(
        "MARKETPLACE | entity linked | location=%s | entity=%s",
        location_id, entity_id,
    )
