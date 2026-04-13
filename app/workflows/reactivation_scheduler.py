"""Reactivation Scheduler — schedule/reschedule reactivation based on activity.

Every activity event (reply, follow-up, outreach, booking, missed call text-back)
calls reschedule_reactivation() to push the reactivation timer forward.

The timer is a row in scheduled_messages with message_type='reactivation'.
Each call UPSERTs this row with due_at = now + X days. When activity stops,
the row's due_at passes, the scheduler fires it, and the lead gets reactivated.

No extra DB columns needed — the scheduled_messages row IS the timer.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from app.services.workflow_tracker import WorkflowTracker

logger = logging.getLogger(__name__)

# Default reactivation days if not configured
_DEFAULT_REACTIVATION_DAYS = 45


async def reschedule_reactivation(
    entity_id: str,
    contact_id: str,
) -> dict[str, Any] | None:
    """Push the reactivation timer forward. Called from every activity event.

    Reads the reactivation config from the lead's setter:
    - system_config.setters[key].auto_reactivation.enabled
    - system_config.setters[key].auto_reactivation.days (default 45)

    If auto_reactivation is disabled, does nothing (leaves any existing row as-is or paused).
    If enabled, UPSERTs a reactivation row with due_at = now + X days.

    The setter is resolved from the contact's Agent Type at call time (not stored).
    """
    from app.services.message_scheduler import schedule_message
    from app.main import supabase

    try:
        try:
            # Resolve entity config
            config = await supabase.resolve_entity(entity_id)
        except ValueError:
            logger.warning("REACTIVATION | entity not found: %s", entity_id)
            return None

        # Find the reactivation config from the setter
        sys_config = config.get("system_config") or {}
        setters = sys_config.get("setters") or {}

        # Find the default setter (or first setter with auto_reactivation)
        react_config = None
        for _key, setter in setters.items():
            ar = setter.get("auto_reactivation") or {}
            if ar.get("enabled"):
                react_config = ar
                break

        if not react_config:
            # Auto-reactivation disabled — don't log, just exit silently
            return None

        # Only create tracker after confirming the workflow is enabled
        tracker = WorkflowTracker(
            "reactivation_scheduler",
            entity_id=entity_id,
            ghl_contact_id=contact_id,
            trigger_source="chain",
        )

        days = react_config.get("days", _DEFAULT_REACTIVATION_DAYS)
        if not isinstance(days, (int, float)) or days <= 0:
            days = _DEFAULT_REACTIVATION_DAYS

        # Calculate due_at
        now = datetime.now(timezone.utc)
        due_at = now + timedelta(days=days)

        # UPSERT into scheduled_messages (replaces any existing pending reactivation)
        row = await schedule_message(
            supabase,
            entity_id=entity_id,
            contact_id=contact_id,
            message_type="reactivation",
            position=None,
            channel="SMS",  # Reactivation is always SMS
            due_at=due_at,
            source="cadence",
            triggered_by="activity",
        )

        if row:
            logger.info(
                "REACTIVATION | rescheduled | due_in=%d days | due=%s | contact=%s",
                days, due_at.isoformat(), contact_id,
            )

        tracker.set_decisions({"days": days, "scheduled": bool(row)})

        return row
    except Exception as e:
        if 'tracker' in dir() and tracker:
            tracker.set_error(str(e))
        raise
    finally:
        if 'tracker' in dir() and tracker:
            await tracker.save()
