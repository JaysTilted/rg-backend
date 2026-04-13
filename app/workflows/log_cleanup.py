"""Prefect log cleanup — deletes flow runs older than N days.

Runs daily via asyncio.create_task at app startup. Keeps the Prefect
database from growing indefinitely on the Hetzner server.

Default retention: 90 days.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone

from prefect import flow

from app.services.slack import notify_error
from prefect.client.orchestration import get_client
from prefect.client.schemas.filters import (
    FlowRunFilter,
    FlowRunFilterStartTime,
)

logger = logging.getLogger(__name__)

RETENTION_DAYS = 90
CLEANUP_INTERVAL_HOURS = 24
BATCH_SIZE = 200


@flow(name="log-cleanup", retries=0)
async def cleanup_old_flow_runs(days: int = RETENTION_DAYS) -> dict:
    """Delete all Prefect flow runs older than `days` days.

    Processes in batches to avoid memory spikes on large backlogs.
    Returns a summary dict with the count of deleted runs.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    total_deleted = 0

    logger.info("LOG_CLEANUP | starting | retention_days=%d | cutoff=%s", days, cutoff.isoformat())

    async with get_client() as client:
        while True:
            old_runs = await client.read_flow_runs(
                flow_run_filter=FlowRunFilter(
                    start_time=FlowRunFilterStartTime(before_=cutoff),
                ),
                limit=BATCH_SIZE,
            )

            if not old_runs:
                break

            for run in old_runs:
                await client.delete_flow_run(run.id)
                total_deleted += 1

            logger.info("LOG_CLEANUP | batch_deleted=%d | total_so_far=%d", len(old_runs), total_deleted)

    logger.info("LOG_CLEANUP | complete | total_deleted=%d", total_deleted)
    return {"deleted": total_deleted, "cutoff": cutoff.isoformat()}


async def run_cleanup_loop() -> None:
    """Background loop that runs cleanup once every 24 hours.

    Called via asyncio.create_task in the FastAPI lifespan.
    First run starts 60 seconds after boot to let Prefect connect.
    """
    await asyncio.sleep(60)  # Let Prefect server fully initialize
    while True:
        try:
            result = await cleanup_old_flow_runs(RETENTION_DAYS)
            logger.info("LOG_CLEANUP | scheduled_run_done | deleted=%d", result["deleted"])
        except Exception as e:
            logger.exception("LOG_CLEANUP | scheduled_run_failed")
            try:
                await notify_error(e, source="scheduler: log_cleanup", context={"trigger_type": "log-cleanup"})
            except Exception:
                pass
        await asyncio.sleep(CLEANUP_INTERVAL_HOURS * 3600)
