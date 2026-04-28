"""Periodic GHL conversation sync poller.

Reconciles GHL's conversation timeline with our chat-history table every
POLL_INTERVAL_SECONDS. This is the safety net for dropped Signal House
webhooks (both SH -> GHL native forwarding and SH -> rg-backend direct
webhooks can silently drop messages; polling covers both paths).

Side effects on detection of NEW rows inserted by the sync:

* New `source=manual` row (staff typed in GHL) -> apply `stop bot` tag,
  cancel pending followups, stamp `app.leads.sms_status='replied'` so
  the cold-SMS dispatcher also stops.
* New `source=lead_reply` row (inbound missed by webhook) -> stamp
  `app.leads.sms_status='replied'` and fire `debounce_inbound` so Scott
  processes the reply.

GHL's conversation timeline is treated as the authoritative source of
truth for message state. The poller reads the timeline, diffs against
what we already have, and fills the gap.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from app.services.ghl_client import GHLClient
from app.services.postgres_client import postgres
from app.text_engine.conversation_sync import run_conversation_sync

logger = logging.getLogger(__name__)

POLL_INTERVAL_SECONDS = 300
ACTIVE_WINDOW_DAYS = 2  # bootstrap fallback only (first run / process restart)
WATERMARK_SAFETY_BUFFER_MINUTES = 30  # see ~/.claude/memory/reference_watermark_sync_pattern.md
STARTUP_GRACE_SECONDS = 45

# In-memory per-entity watermark — the latest `timestamp` we've successfully
# polled. Resets on process restart, which is fine — bootstrap takes over.
# 2026-04-28 audit-loop: replaces the prior 2-day rolling-window candidate query
# that re-fetched all 48-hour-active contacts every 5 min regardless of change.
_ENTITY_WATERMARKS: dict[str, datetime] = {}
# Only trigger the reply pipeline for inbound messages detected within this
# window. Older inbound is still logged + dedupe'd, but we don't wake Scott
# up on week-old conversations that were never manually answered.
REPLY_TRIGGER_MAX_AGE_HOURS = 6
# How many most-recent GHL conversations to scan on each pass. This covers
# cold leads whose first reply may never have created a chat-history row
# because the SH webhook was dropped. Union with rg_chat-active contacts.
GHL_CONVERSATIONS_PER_PASS = 50
# Per-contact delay — at 700ms with ~150 contacts, a full pass takes ~105s,
# leaving the rest of the 5-minute interval for actual webhook traffic.
# GHL's per-location limit is ~10 req/s; we cap our poller at ~1.4 req/s.
PER_CONTACT_SLEEP_MS = 700


async def _list_active_entities() -> list[dict[str, Any]]:
    """Active entities that have a chat-history table + GHL creds."""
    assert postgres.main_pool is not None, "postgres.main_pool not initialized"
    rows = await postgres.main_pool.fetch(
        """
        SELECT id, name, chat_history_table_name, ghl_location_id, ghl_api_key,
               system_config, test_config_overrides, timezone
        FROM entities
        WHERE status = 'active'
          AND chat_history_table_name IS NOT NULL
          AND chat_history_table_name <> ''
          AND ghl_location_id IS NOT NULL
          AND ghl_location_id <> ''
          AND ghl_api_key IS NOT NULL
          AND ghl_api_key <> ''
        """
    )
    return [dict(r) for r in rows]


async def _active_contacts_for_table(
    chat_table: str,
    entity_id: str,
    bootstrap_days: int,
) -> tuple[list[str], datetime | None]:
    """Distinct session_ids (= GHL contact IDs) with chat-table activity since
    the last successful poll for *entity_id*.

    Watermark behavior (2026-04-28 audit-loop fix):
        * If we have a previous watermark for this entity, use
          ``watermark - 30min`` as the cutoff. Steady-state polls every 5
          minutes only see contacts that actually had new activity since
          the previous pass, plus a 30-min safety buffer for race conditions.
        * If no watermark yet (first run after process start), fall back to
          ``NOW() - bootstrap_days`` as the cutoff. This rebuilds the
          candidate set after restarts without losing coverage.

    Returns ``(contact_ids, latest_timestamp_seen)``. Caller persists the
    latest timestamp via ``_ENTITY_WATERMARKS`` AFTER the pass succeeds, so
    a mid-pass crash doesn't skip rows on the retry.
    """
    assert postgres.chat_pool is not None, "postgres.chat_pool not initialized"

    watermark = _ENTITY_WATERMARKS.get(entity_id)
    if watermark is None:
        cutoff_clause = f"NOW() - INTERVAL '{int(bootstrap_days)} days'"
        params: list[Any] = []
    else:
        cutoff = watermark - timedelta(minutes=WATERMARK_SAFETY_BUFFER_MINUTES)
        cutoff_clause = "$1::timestamptz"
        params = [cutoff]

    rows = await postgres.chat_pool.fetch(
        f'SELECT session_id, "timestamp" FROM "{chat_table}" '
        f'WHERE "timestamp" > {cutoff_clause} '
        f"AND session_id IS NOT NULL AND session_id <> ''",
        *params,
    )
    seen: set[str] = set()
    contacts: list[str] = []
    latest_ts: datetime | None = None
    for r in rows:
        sid = r["session_id"]
        if sid not in seen:
            seen.add(sid)
            contacts.append(sid)
        ts = r["timestamp"]
        if ts is not None and (latest_ts is None or ts > latest_ts):
            latest_ts = ts
    return contacts, latest_ts


async def _existing_ghl_ids(chat_table: str, contact_id: str) -> set[str]:
    assert postgres.chat_pool is not None, "postgres.chat_pool not initialized"
    rows = await postgres.chat_pool.fetch(
        f'SELECT ghl_message_id FROM "{chat_table}" '
        f"WHERE session_id = $1 AND ghl_message_id IS NOT NULL",
        contact_id,
    )
    return {r["ghl_message_id"] for r in rows}


async def _classify_new_rows(
    chat_table: str, new_ids: list[str]
) -> dict[str, Any]:
    """Bucket new rows by source. Also returns the newest inbound timestamp
    (so callers can gate the reply-pipeline trigger by recency)."""
    counts: dict[str, Any] = {
        "new_inbound": 0, "new_manual": 0, "new_workflow": 0, "new_ai": 0,
        "latest_inbound_ts": None,
    }
    if not new_ids:
        return counts
    assert postgres.chat_pool is not None, "postgres.chat_pool not initialized"
    rows = await postgres.chat_pool.fetch(
        f'SELECT role, source, "timestamp" FROM "{chat_table}" WHERE ghl_message_id = ANY($1::text[])',
        new_ids,
    )
    for r in rows:
        src = (r["source"] or "").lower()
        if src == "lead_reply":
            counts["new_inbound"] += 1
            ts = r["timestamp"]
            if ts and (counts["latest_inbound_ts"] is None or ts > counts["latest_inbound_ts"]):
                counts["latest_inbound_ts"] = ts
        elif src == "manual":
            counts["new_manual"] += 1
        elif src == "workflow":
            counts["new_workflow"] += 1
        elif src == "ai":
            counts["new_ai"] += 1
    return counts


async def _stamp_sms_replied(contact_id: str) -> None:
    """Mark the iron-sms lead row as replied so the cold-SMS dispatcher cancels
    remaining drips. Best-effort — logs and moves on if the update fails or if
    the role cannot write to the `app` schema."""
    try:
        assert postgres.main_pool is not None, "postgres.main_pool not initialized"
        async with postgres.main_pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE app.leads
                   SET sms_status = 'replied',
                       updated_at = NOW()
                 WHERE ghl_contact_id = $1
                   AND (sms_status IS NULL
                        OR sms_status NOT IN (
                           'replied', 'do_not_contact', 'not_interested',
                           'interested', 'automated_reply', 'delivery_failed'
                        ))
                """,
                contact_id,
            )
    except Exception as e:
        logger.warning(
            "SYNC_POLLER | stamp_sms_replied failed | contact=%s | err=%s",
            contact_id, e,
        )


async def _cancel_followups(contact_id: str, entity_id: str, reason: str) -> None:
    try:
        from app.main import supabase as sb
        from app.services.message_scheduler import cancel_pending
        await cancel_pending(
            sb, contact_id, entity_id,
            message_types=["followup", "smart_followup", "outreach"],
            reason=reason,
        )
    except Exception as e:
        logger.warning(
            "SYNC_POLLER | cancel_followups failed | contact=%s | err=%s",
            contact_id, e,
        )


async def _trigger_reply_pipeline(
    entity_id: str, contact_id: str, config: dict[str, Any]
) -> None:
    try:
        from app.services.debounce import debounce_inbound
        body: dict[str, Any] = {
            "contact_id": contact_id,
            "source": "sync_poller_backfill",
        }
        await debounce_inbound(entity_id, contact_id, body, config)
    except Exception as e:
        logger.warning(
            "SYNC_POLLER | trigger_reply_pipeline failed | contact=%s | err=%s",
            contact_id, e,
        )


async def _sync_one_contact(
    contact_id: str,
    ghl: GHLClient,
    chat_table: str,
    entity_id: str,
    config: dict[str, Any],
) -> dict[str, Any]:
    pre_ids = await _existing_ghl_ids(chat_table, contact_id)
    empty: dict[str, Any] = {
        "new_inbound": 0, "new_manual": 0, "new_workflow": 0, "new_ai": 0,
        "latest_inbound_ts": None,
    }
    try:
        await run_conversation_sync(
            contact_id=contact_id,
            ghl=ghl,
            chat_table=chat_table,
            entity_id=entity_id,
            config=config,
        )
    except Exception as e:
        logger.warning(
            "SYNC_POLLER | run_conversation_sync failed | contact=%s | err=%s",
            contact_id, e,
        )
        return empty

    post_ids = await _existing_ghl_ids(chat_table, contact_id)
    new_ids = list(post_ids - pre_ids)
    if not new_ids:
        return empty
    return await _classify_new_rows(chat_table, new_ids)


async def _sync_one_entity(entity: dict[str, Any]) -> None:
    entity_id = str(entity["id"])
    chat_table = entity["chat_history_table_name"]
    ghl_location_id = entity["ghl_location_id"]
    ghl_api_key = entity["ghl_api_key"]

    # Per-entity kill switch. Defaults ON. Set
    # system_config.ghl_conversation_sync.enabled = false to disable.
    sc = entity.get("system_config") or {}
    if isinstance(sc, str):
        import json
        try:
            sc = json.loads(sc)
        except Exception:
            sc = {}
    sync_cfg = (sc.get("ghl_conversation_sync") or {}) if isinstance(sc, dict) else {}
    if sync_cfg.get("enabled") is False:
        logger.info(
            "SYNC_POLLER | entity=%s | disabled via system_config.ghl_conversation_sync.enabled",
            entity_id[:8],
        )
        return

    ghl = GHLClient(api_key=ghl_api_key, location_id=ghl_location_id)
    config = dict(entity)
    # sync_poller previously copied the entity row verbatim via ``dict(entity)``,
    # which preserved ``system_config`` as a JSON string. Downstream consumers
    # (`_get_debounce_window`, `_get_reply_window_sleep`, etc.) then crashed
    # with ``'str' object has no attribute 'get'`` (2026-04-24 fix). The
    # debounce module now defends itself but passing the parsed dict here
    # also saves a parse on every consumer call.
    config["system_config"] = sc if isinstance(sc, dict) else {}

    # Contact discovery: union of two sources.
    #   A) rg_chat active contacts (last N days) — contacts we already track
    #   B) GHL's N most-recent conversations in this location — catches
    #      brand-new cold leads whose first reply was webhook-dropped
    #      before any chat-history row existed.
    contact_set: set[str] = set()

    from_chat, latest_chat_ts = await _active_contacts_for_table(
        chat_table, entity_id, ACTIVE_WINDOW_DAYS
    )
    contact_set.update(from_chat)

    from_ghl: list[str] = []
    try:
        convs = await ghl.list_recent_conversations(
            limit=GHL_CONVERSATIONS_PER_PASS
        )
        for c in convs:
            cid = c.get("contactId") or ""
            if cid:
                from_ghl.append(cid)
        contact_set.update(from_ghl)
    except Exception as e:
        logger.warning(
            "SYNC_POLLER | list_recent_conversations failed | entity=%s | err=%s",
            entity_id[:8], e,
        )

    if not contact_set:
        return

    logger.info(
        "SYNC_POLLER | entity=%s | table=%s | from_chat=%d from_ghl=%d union=%d",
        entity_id[:8], chat_table, len(from_chat), len(from_ghl), len(contact_set),
    )

    # Pass-level counters for summary log
    from datetime import datetime, timezone
    pass_started_at = datetime.now(timezone.utc)
    pass_counts = {
        "contacts_scanned": 0,
        "contacts_with_changes": 0,
        "new_inbound": 0,
        "new_manual": 0,
        "new_workflow": 0,
        "new_ai": 0,
        "pipelines_triggered": 0,
        "stop_bot_tags_applied": 0,
        "errors": 0,
    }

    for contact_id in sorted(contact_set):
        pass_counts["contacts_scanned"] += 1
        if PER_CONTACT_SLEEP_MS > 0:
            await asyncio.sleep(PER_CONTACT_SLEEP_MS / 1000.0)
        try:
            counts = await _sync_one_contact(
                contact_id, ghl, chat_table, entity_id, config
            )
            total_new = (
                int(counts["new_inbound"]) + int(counts["new_manual"])
                + int(counts["new_workflow"]) + int(counts["new_ai"])
            )
            if total_new == 0:
                continue

            pass_counts["contacts_with_changes"] += 1
            pass_counts["new_inbound"] += int(counts["new_inbound"])
            pass_counts["new_manual"] += int(counts["new_manual"])
            pass_counts["new_workflow"] += int(counts["new_workflow"])
            pass_counts["new_ai"] += int(counts["new_ai"])

            logger.info(
                "SYNC_POLLER | contact=%s | new_inbound=%d new_manual=%d new_workflow=%d new_ai=%d",
                contact_id, counts["new_inbound"], counts["new_manual"],
                counts["new_workflow"], counts["new_ai"],
            )

            if int(counts["new_manual"]) > 0:
                try:
                    await ghl.add_tag(contact_id, "stop bot")
                    pass_counts["stop_bot_tags_applied"] += 1
                except Exception as e:
                    logger.warning(
                        "SYNC_POLLER | add_tag(stop bot) failed | contact=%s | err=%s",
                        contact_id, e,
                    )
                await _cancel_followups(contact_id, entity_id, "manual_staff_reply")
                await _stamp_sms_replied(contact_id)
                logger.info(
                    "SYNC_POLLER | MANUAL_STAFF_REPLY | contact=%s | stop_bot_applied=true",
                    contact_id,
                )

            if int(counts["new_inbound"]) > 0:
                await _stamp_sms_replied(contact_id)
                latest_ts = counts.get("latest_inbound_ts")
                should_trigger = False
                if latest_ts is not None:
                    try:
                        from datetime import timedelta
                        if isinstance(latest_ts, datetime):
                            age = datetime.now(timezone.utc) - latest_ts
                        else:
                            age = datetime.now(timezone.utc) - datetime.fromisoformat(str(latest_ts))
                        should_trigger = age < timedelta(hours=REPLY_TRIGGER_MAX_AGE_HOURS)
                    except Exception:
                        should_trigger = True
                if should_trigger:
                    await _trigger_reply_pipeline(entity_id, contact_id, config)
                    pass_counts["pipelines_triggered"] += 1
                    logger.info(
                        "SYNC_POLLER | MISSED_INBOUND | contact=%s | pipeline_triggered=true",
                        contact_id,
                    )
                else:
                    logger.info(
                        "SYNC_POLLER | MISSED_INBOUND_STALE | contact=%s | latest_ts=%s | trigger_skipped=>age",
                        contact_id, latest_ts,
                    )
        except Exception:
            pass_counts["errors"] += 1
            logger.exception(
                "SYNC_POLLER | contact processing error | contact=%s", contact_id
            )

    elapsed = (datetime.now(timezone.utc) - pass_started_at).total_seconds()
    logger.info(
        "SYNC_POLLER | pass_complete | entity=%s | elapsed=%.1fs | scanned=%d changed=%d inbound=%d manual=%d workflow=%d ai=%d pipelines=%d stopbot=%d errors=%d",
        entity_id[:8], elapsed,
        pass_counts["contacts_scanned"], pass_counts["contacts_with_changes"],
        pass_counts["new_inbound"], pass_counts["new_manual"],
        pass_counts["new_workflow"], pass_counts["new_ai"],
        pass_counts["pipelines_triggered"], pass_counts["stop_bot_tags_applied"],
        pass_counts["errors"],
    )


async def run_conversation_sync_poller() -> None:
    """Top-level loop. Started from app.main lifespan."""
    logger.info(
        "SYNC_POLLER | started | interval=%ds | window=%dd",
        POLL_INTERVAL_SECONDS, ACTIVE_WINDOW_DAYS,
    )
    await asyncio.sleep(STARTUP_GRACE_SECONDS)

    while True:
        try:
            entities = await _list_active_entities()
            for entity in entities:
                try:
                    await _sync_one_entity(entity)
                except Exception:
                    logger.exception(
                        "SYNC_POLLER | entity processing error | entity=%s",
                        str(entity.get("id", ""))[:8],
                    )
        except Exception:
            logger.exception("SYNC_POLLER | top-level loop error")
        await asyncio.sleep(POLL_INTERVAL_SECONDS)
