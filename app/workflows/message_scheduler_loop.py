"""Three-layer message scheduling system.

Layer 1 (Primary): asyncio sleep tasks — exact-time firing, zero latency.
    When schedule_message() inserts a DB row, it also creates an asyncio task
    that sleeps until due_at, then fires. In-memory, fast, precise.

Layer 2 (Startup Recovery): On server boot, recreate asyncio tasks from DB.
    Queries all pending rows, creates tasks for each. Past-due rows fire
    immediately. Recovery happens in seconds, not minutes.

Layer 3 (Safety Net): Background poller every 30 minutes.
    Catches anything Layer 1+2 missed (should almost never happen).
    Only picks up rows that are 2+ minutes past due and have no active task.

Nothing is ever permanently lost — the DB is the source of truth.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from app.services.workflow_tracker import WorkflowTracker

logger = logging.getLogger(__name__)

# In-memory registry: message_id (str) → asyncio.Task
_scheduled_tasks: dict[str, asyncio.Task] = {}

# Safety-net poller interval (seconds)
_SAFETY_NET_INTERVAL = 1800  # 30 minutes
_GRACE_PERIOD_SECONDS = 120  # 2 minutes — don't pick up messages asyncio is about to fire
_ORPHAN_TIMEOUT_MINUTES = 10  # Reset 'processing' rows older than this


# =========================================================================
# LAYER 1: ASYNCIO FIRE TASKS
# =========================================================================


def create_fire_task(message_row: dict[str, Any]) -> asyncio.Task | None:
    """Create an asyncio task that sleeps until due_at, then fires the message.

    Called by:
    - schedule_message() after inserting a new DB row
    - recover_all_pending() on startup for existing pending rows
    - Safety-net poller for missed messages

    Returns the created task, or None if task couldn't be created.
    """
    message_id = str(message_row.get("id", ""))
    if not message_id:
        logger.warning("SCHEDULER_LOOP | no message_id in row, skipping")
        return None

    # Cancel any existing task for this message (idempotent)
    cancel_fire_task(message_id)

    due_at_str = message_row.get("due_at", "")
    try:
        if isinstance(due_at_str, datetime):
            due_at = due_at_str if due_at_str.tzinfo else due_at_str.replace(tzinfo=timezone.utc)
        else:
            due_at = datetime.fromisoformat(due_at_str.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        logger.error("SCHEDULER_LOOP | invalid due_at: %r | message_id=%s", due_at_str, message_id)
        return None

    async def _sleep_then_fire():
        try:
            # Calculate sleep duration
            now = datetime.now(timezone.utc)
            seconds_until_due = (due_at - now).total_seconds()

            if seconds_until_due > 0:
                logger.info(
                    "SCHEDULER_LOOP | task_created | id=%s | type=%s | due_in=%.0fs | due_at=%s",
                    message_id[:8], message_row.get("message_type"),
                    seconds_until_due, due_at.isoformat(),
                )
                await asyncio.sleep(seconds_until_due)

            # Time to fire
            await _fire_message(message_row)

        except asyncio.CancelledError:
            logger.info("SCHEDULER_LOOP | task_cancelled | id=%s", message_id[:8])
        except Exception:
            logger.exception("SCHEDULER_LOOP | task_error | id=%s", message_id[:8])
        finally:
            _scheduled_tasks.pop(message_id, None)

    task = asyncio.create_task(_sleep_then_fire())
    _scheduled_tasks[message_id] = task
    return task


def cancel_fire_task(message_id: str) -> bool:
    """Cancel the asyncio fire task for a message.

    Called when cancel_pending() cancels DB rows — also cleans up in-memory tasks.
    Returns True if a task was found and cancelled.
    """
    task = _scheduled_tasks.pop(message_id, None)
    if task and not task.done():
        task.cancel()
        return True
    return False


def get_active_task_count() -> int:
    """Number of active asyncio fire tasks (for monitoring)."""
    return sum(1 for t in _scheduled_tasks.values() if not t.done())


def get_active_task_ids() -> set[str]:
    """Set of message IDs with active asyncio tasks."""
    return {mid for mid, t in _scheduled_tasks.items() if not t.done()}


# =========================================================================
# CORE: FIRE A MESSAGE
# =========================================================================


async def _fire_message(message_row: dict[str, Any]) -> None:
    """Fire a scheduled message — claim it, execute, update status.

    Routes by message_type to the appropriate pipeline/delivery.
    """
    from app.services.message_scheduler import (
        claim_message,
        update_message_status,
    )

    # Lazy import the Supabase singleton
    from app.main import supabase

    message_id = str(message_row["id"])
    message_type = message_row.get("message_type", "")
    contact_id = message_row.get("contact_id", "")
    entity_id = message_row.get("entity_id", "")

    # Skip tracker for message types that create their own workflow_runs
    # (followup → text_engine pipeline, reactivation → reactivation workflow,
    #  activity_reset/human_takeover → just tag removal, not worth logging)
    _SKIP_TRACKER_TYPES = {"followup", "smart_followup", "reactivation", "activity_reset", "human_takeover", "post_appointment"}
    _skip_tracker = message_type in _SKIP_TRACKER_TYPES

    # Map message types to descriptive workflow_type names
    _TYPE_MAP = {
        "outreach": "outreach_delivery",
        "appointment_reminder": "appointment_reminder_delivery",
    }
    wf_type = _TYPE_MAP.get(message_type, "message_scheduler")

    tracker = None
    if not _skip_tracker:
        tracker = WorkflowTracker(
            wf_type,
            entity_id=entity_id or None,
            ghl_contact_id=contact_id or None,
            trigger_source="scheduled",
        )
        tracker.set_metadata("message_type", message_type)
        tracker.set_metadata("message_id", message_id)

    logger.info(
        "SCHEDULER_LOOP | firing | id=%s | type=%s | pos=%s | contact=%s",
        message_id[:8], message_type, message_row.get("position"), contact_id,
    )

    # Atomic claim — only proceed if still pending
    claimed = await claim_message(supabase, message_id)
    if not claimed:
        logger.info("SCHEDULER_LOOP | claim_failed (already fired/cancelled) | id=%s", message_id[:8])
        if tracker:
            tracker.set_status("skipped")
            tracker.set_decisions({"reason": "claim_failed"})
            await tracker.save()
        return

    try:
        if message_type in ("followup", "smart_followup"):
            result = await _fire_followup(claimed)
        elif message_type in ("outreach", "appointment_reminder"):
            result = await _fire_outreach(claimed)  # Same delivery logic — pre-resolved content in metadata
        elif message_type == "reactivation":
            result = await _fire_reactivation(claimed)
        elif message_type == "activity_reset":
            result = await _fire_activity_reset(claimed)
        elif message_type == "human_takeover":
            result = await _fire_human_takeover(claimed)
        elif message_type == "post_appointment":
            result = await _fire_post_appointment(claimed)
        else:
            logger.warning("SCHEDULER_LOOP | unknown message_type: %s | id=%s", message_type, message_id[:8])
            await update_message_status(supabase, message_id, status="failed", result_reason=f"Unknown type: {message_type}")
            if tracker:
                tracker.set_error(f"Unknown message_type: {message_type}")
            return

        # Update status based on result
        status = result.get("status", "sent")
        await update_message_status(
            supabase, message_id,
            status=status,
            result=result.get("result"),
            result_reason=result.get("reason"),
        )

        if tracker:
            # Parse metadata safely
            _meta = message_row.get("metadata") or {}
            if isinstance(_meta, str):
                import json as _json
                try:
                    _meta = _json.loads(_meta)
                except (ValueError, TypeError):
                    _meta = {}

            # Rich decisions for outreach/reminder
            _decisions = {
                "message_type": message_type,
                "status": status,
                "result": result.get("result"),
                "reason": result.get("reason", ""),
                "position": message_row.get("position"),
                "channel": message_row.get("channel", "SMS"),
                "source": message_row.get("source", ""),
                "triggered_by": message_row.get("triggered_by", ""),
            }

            # Content that was sent
            if message_type in ("outreach", "appointment_reminder"):
                _decisions["content"] = {
                    "sms": _meta.get("sms", ""),
                    "email_subject": _meta.get("email_subject", ""),
                    "email_body": _meta.get("email_body", ""),
                    "media_url": _meta.get("media_url", ""),
                    "media_type": _meta.get("media_type", ""),
                }
                _decisions["template"] = {
                    "name": _meta.get("template_name", ""),
                    "variant": _meta.get("variant", ""),
                    "ab_test_id": _meta.get("ab_test_id", ""),
                }
                # Appointment-specific
                if message_type == "appointment_reminder":
                    _decisions["appointment"] = {
                        "id": _meta.get("appointment_id", ""),
                        "datetime": _meta.get("appointment_datetime", ""),
                        "calendar": _meta.get("calendar_name", ""),
                    }
                # Include the full metadata for transparency
                _decisions["raw_metadata"] = _meta

            tracker.set_decisions(_decisions)

        logger.info(
            "SCHEDULER_LOOP | completed | id=%s | type=%s | status=%s | result=%s",
            message_id[:8], message_type, status, result.get("result"),
        )

    except Exception as e:
        logger.exception("SCHEDULER_LOOP | fire_error | id=%s | type=%s", message_id[:8], message_type)
        await update_message_status(
            supabase, message_id,
            status="failed",
            result_reason=str(e)[:200],
        )
        if tracker:
            tracker.set_error(str(e))
    finally:
        if tracker:
            await tracker.save()


# =========================================================================
# MESSAGE TYPE HANDLERS (stubs — wired in Phase 1-4)
# =========================================================================


async def _fire_followup(row: dict[str, Any]) -> dict[str, Any]:
    """Fire a follow-up message — build context, run pipeline, chain schedule."""
    from prefect import tags as prefect_tags

    from app.main import supabase, build_run_tags, build_flow_name, _resolve_tenant_ai_keys
    from app.models import PipelineContext
    from app.text_engine.pipeline import text_engine
    from app.services.ghl_client import GHLClient
    from app.services.message_scheduler import (
        schedule_followup_chain,
        update_ghl_followup_field,
    )

    entity_id = row["entity_id"]
    contact_id = row["contact_id"]
    channel = row.get("channel", "SMS")
    position = row.get("position", 1)
    triggered_by = row.get("triggered_by", "reply")

    # Resolve entity config
    try:
        config = await supabase.resolve_entity(entity_id)
    except ValueError:
        return {"status": "failed", "result": "error", "reason": f"Entity {entity_id} not found"}

    slug = config.get("slug") or config.get("name", entity_id[:8])

    # Resolve per-tenant AI keys
    tenant_keys = await _resolve_tenant_ai_keys(entity_id)

    # Build PipelineContext for follow-up
    ctx = PipelineContext(
        entity_id=entity_id,
        contact_id=contact_id,
        trigger_type="followup",
        channel=channel,
        agent_type="",  # Resolved during data_loading from contact
        config=config,
        slug=slug,
        ghl_api_key=config.get("ghl_api_key", ""),
        ghl_location_id=config.get("ghl_location_id", ""),
        openrouter_api_key=tenant_keys["openrouter"],
        tenant_ai_keys=tenant_keys,
        scheduled_message=row,  # So followup.py can read source, metadata, position
    )

    # Build Prefect tags
    run_tags = build_run_tags(
        slug=slug,
        entity_id=entity_id,
        contact_id=contact_id,
        channel=channel,
        trigger_type="followup",
        agent_type="",
    )
    flow_name = build_flow_name(slug, entity_id)

    try:
        with prefect_tags(*run_tags):
            result = await text_engine.with_options(name=flow_name)(ctx)

        fu_needed = result.get("followUpNeeded", False)
        result_path = result.get("path", "")

        if fu_needed:
            # Follow-up was sent — schedule next position (chain)
            try:
                await schedule_followup_chain(ctx, position=position + 1, triggered_by=triggered_by)
            except Exception as e:
                logger.warning("SCHEDULER_LOOP | chain scheduling failed: %s", e)

            # Reschedule reactivation (activity happened)
            try:
                from app.workflows.reactivation_scheduler import reschedule_reactivation
                await reschedule_reactivation(entity_id, contact_id)
            except Exception as e:
                logger.warning("SCHEDULER_LOOP | reactivation reschedule failed: %s", e)

            return {"status": "sent", "result": "sent", "reason": result_path}
        else:
            # Follow-up was skipped — clear GHL field, don't chain
            try:
                ghl = GHLClient(api_key=config.get("ghl_api_key", ""), location_id=config.get("ghl_location_id", ""))
                await update_ghl_followup_field(ghl, contact_id)  # Clear
            except Exception:
                pass
            return {"status": "skipped", "result": "skipped", "reason": result_path or result.get("reason", "")}

    except Exception as e:
        logger.exception("SCHEDULER_LOOP | followup pipeline failed | contact=%s", contact_id)
        return {"status": "failed", "result": "error", "reason": str(e)[:200]}


async def _fire_outreach(row: dict[str, Any]) -> dict[str, Any]:
    """Fire an outreach message — deliver pre-resolved content via GHL."""
    from app.main import supabase
    from app.services.ghl_client import GHLClient
    from app.services.delivery_service import DeliveryService

    entity_id = row["entity_id"]
    contact_id = row["contact_id"]
    position = row.get("position", 1)
    _raw_meta = row.get("metadata") or {}
    if isinstance(_raw_meta, str):
        import json as _json
        try:
            _raw_meta = _json.loads(_raw_meta)
        except (ValueError, TypeError):
            _raw_meta = {}
    metadata = _raw_meta
    channel = row.get("channel", "SMS")

    # Resolve entity for GHL credentials
    try:
        config = await supabase.resolve_entity(entity_id)
    except ValueError:
        return {"status": "failed", "result": "error", "reason": f"Entity {entity_id} not found"}

    # Read pre-resolved content from metadata
    sms = metadata.get("sms")
    email_subject = metadata.get("email_subject")
    email_body = metadata.get("email_body")
    media_url = metadata.get("media_url")
    media_type = metadata.get("media_type")

    if not sms and not email_body:
        logger.warning("SCHEDULER_LOOP | outreach pos %d has no SMS or email content", position)
        return {"status": "skipped", "result": "no_content", "reason": "Position has no SMS or email"}

    # Deliver via GHL API with delivery confirmation
    ghl = GHLClient(api_key=config.get("ghl_api_key", ""), location_id=config.get("ghl_location_id", ""))
    delivery_svc = DeliveryService(ghl, config)

    if sms:
        to_phone = row.get("to_phone")
        if not to_phone:
            contact = await ghl.get_contact(contact_id)
            to_phone = (contact or {}).get("phone")
        result = await delivery_svc.send_sms(
            contact_id=contact_id, message=sms, media_url=media_url,
            to_phone=to_phone, message_type="outreach",
        )
        if result.status == "failed":
            return {"status": "failed", "result": "delivery_failed", "reason": result.error_message or "SMS delivery failed"}

    if email_body and email_subject:
        email_result = await delivery_svc.send_standalone_email(contact_id, email_subject, email_body)
        if email_result.status == "failed":
            logger.warning("SCHEDULER_LOOP | outreach email failed pos %d | %s", position, email_result.error_message)

    # Reschedule reactivation (outreach delivery is an activity event)
    try:
        from app.workflows.reactivation_scheduler import reschedule_reactivation
        await reschedule_reactivation(entity_id, contact_id)
    except Exception as e:
        logger.warning("SCHEDULER_LOOP | outreach reactivation reschedule failed: %s", e)

    template_name = metadata.get("template_name", "?")
    logger.info(
        "SCHEDULER_LOOP | outreach_delivered | pos=%d | template=%s | sms=%s email=%s media=%s | contact=%s",
        position, template_name, bool(sms), bool(email_body), bool(media_url), contact_id,
    )

    return {"status": "sent", "result": "delivered", "reason": f"Outreach #{position} delivered"}


async def _fire_reactivation(row: dict[str, Any]) -> dict[str, Any]:
    """Fire a reactivation message — run reactivation pipeline, then start FU cadence."""
    from app.main import supabase, _resolve_tenant_ai_keys
    from app.services.ghl_client import GHLClient
    from app.services.message_scheduler import schedule_followup_chain
    from app.workflows.reactivation_scheduler import reschedule_reactivation

    entity_id = row["entity_id"]
    contact_id = row["contact_id"]

    # Resolve entity config
    try:
        config = await supabase.resolve_entity(entity_id)
    except ValueError:
        return {"status": "failed", "result": "error", "reason": f"Entity {entity_id} not found"}

    # Resolve per-tenant AI keys
    tenant_keys = await _resolve_tenant_ai_keys(entity_id)

    slug = config.get("slug") or config.get("name", entity_id[:8])
    ghl = GHLClient(
        api_key=config.get("ghl_api_key", ""),
        location_id=config.get("ghl_location_id", ""),
    )

    try:
        # Run the existing reactivation pipeline (generates message, doesn't deliver)
        from app.workflows.reactivation import reactivate_lead
        from app.models import ReactivateBody
        from app.services.delivery_service import DeliveryService

        body = ReactivateBody(
            id=contact_id,
            entityId=entity_id,
            ReplyChannel="SMS",
        )

        result_tuple = await reactivate_lead(entity_id=entity_id, body=body)
        # reactivate_lead returns (response_dict, http_status_code) tuple
        result = result_tuple[0] if isinstance(result_tuple, tuple) else result_tuple

        if result.get("success"):
            # Deliver the reactivation message via DeliveryService
            sms_text = result.get("sms_1")
            if sms_text:
                delivery_svc = DeliveryService(ghl, config)
                to_phone = row.get("to_phone")
                if not to_phone:
                    contact_data = await ghl.get_contact(contact_id)
                    to_phone = (contact_data or {}).get("phone")
                delivery_result = await delivery_svc.send_sms(
                    contact_id=contact_id, message=sms_text,
                    to_phone=to_phone, message_type="reactivation",
                )
                if delivery_result.status == "failed":
                    return {"status": "failed", "result": "delivery_failed", "reason": delivery_result.error_message or "Delivery failed"}
            logger.info(
                "SCHEDULER_LOOP | reactivation_sent | contact=%s | entity=%s",
                contact_id, entity_id[:8],
            )

            # Start normal follow-up cadence after reactivation
            # Fetch contact from GHL to get Agent Type for setter resolution
            agent_type = ""
            try:
                contact = await ghl.get_contact(contact_id)
                if contact:
                    # GHL contacts API returns customFields as [{id, value}] — id is a UUID.
                    # Map UUID→fieldKey via field definitions, then find agent_type_new.
                    field_defs = await ghl.get_custom_field_defs_full()  # [{id, name, key, dataType}]
                    id_to_key = {f["id"]: f["key"] for f in field_defs}

                    for cf in contact.get("customFields", []):
                        field_id = cf.get("id", "")
                        field_key = id_to_key.get(field_id, "")
                        if field_key in ("contact.agent_type_new", "contact.agent_type"):
                            agent_type = str(cf.get("value", "")).strip()
                            break
            except Exception as _e:
                logger.warning("SCHEDULER_LOOP | agent_type fetch failed (using default): %s", _e)

            from app.models import PipelineContext
            ctx = PipelineContext(
                entity_id=entity_id,
                contact_id=contact_id,
                trigger_type="reactivation",
                channel="SMS",
                agent_type=agent_type,  # From GHL contact, falls back to default setter if empty
                config=config,
                slug=slug,
                ghl_api_key=config.get("ghl_api_key", ""),
                ghl_location_id=config.get("ghl_location_id", ""),
                openrouter_api_key=tenant_keys["openrouter"],
                tenant_ai_keys=tenant_keys,
            )
            ctx.ghl = ghl

            # Compile setter config for cadence reading
            from app.text_engine.data_loading import _compile_system_config
            try:
                _compile_system_config(ctx, config.get("system_config") or {})
            except Exception:
                pass

            try:
                await schedule_followup_chain(ctx, triggered_by="reactivation")
            except Exception as e:
                logger.warning("SCHEDULER_LOOP | FU chain after reactivation failed: %s", e)

            # Schedule NEXT reactivation (in case lead doesn't reply)
            try:
                await reschedule_reactivation(entity_id, contact_id)
            except Exception as e:
                logger.warning("SCHEDULER_LOOP | next reactivation schedule failed: %s", e)

            return {"status": "sent", "result": "sent", "reason": "Reactivation message sent + FU chain started"}
        else:
            error = result.get("error", "Reactivation pipeline returned failure")
            return {"status": "failed", "result": "pipeline_failed", "reason": str(error)[:200]}

    except Exception as e:
        logger.exception("SCHEDULER_LOOP | reactivation failed | contact=%s", contact_id)
        return {"status": "failed", "result": "error", "reason": str(e)[:200]}


async def _fire_activity_reset(row: dict[str, Any]) -> dict[str, Any]:
    """Fire an activity reset (appointment start time) — just pushes reactivation timer forward.

    No message is sent. This is triggered when an appointment's start time arrives,
    so the reactivation timer resets from the appointment date, not the booking date.
    """
    entity_id = row["entity_id"]
    contact_id = row["contact_id"]

    try:
        from app.workflows.reactivation_scheduler import reschedule_reactivation
        await reschedule_reactivation(entity_id, contact_id)
        logger.info(
            "SCHEDULER_LOOP | activity_reset | reactivation pushed forward | contact=%s",
            contact_id,
        )
        return {"status": "sent", "result": "timer_reset", "reason": "Appointment started — reactivation timer pushed forward"}
    except Exception as e:
        logger.warning("SCHEDULER_LOOP | activity_reset failed: %s", e)
        return {"status": "failed", "result": "error", "reason": str(e)[:200]}


async def _fire_human_takeover(row: dict[str, Any]) -> dict[str, Any]:
    """Fire human takeover removal — remove 'stop bot' tag so AI can resume.

    Called when the human takeover timer expires. Idempotent — safe if tag
    was already removed manually by staff.
    """
    entity_id = row["entity_id"]
    contact_id = row["contact_id"]

    try:
        from app.main import supabase
        config = await supabase.resolve_entity(entity_id)
        from app.services.ghl_client import GHLClient
        ghl = GHLClient(
            api_key=config.get("ghl_api_key", ""),
            location_id=config.get("ghl_location_id", ""),
        )
        await ghl.remove_tag(contact_id, "stop bot")
        logger.info(
            "SCHEDULER_LOOP | human_takeover | stop bot tag removed | contact=%s",
            contact_id,
        )
        return {"status": "sent", "result": "tag_removed", "reason": "Human takeover expired — stop bot tag removed"}
    except Exception as e:
        logger.warning("SCHEDULER_LOOP | human_takeover failed: %s", e)
        return {"status": "failed", "result": "error", "reason": str(e)[:200]}


async def _fire_post_appointment(row: dict[str, Any]) -> dict[str, Any]:
    """Fire a post-appointment automation message — check-in, review, rebook, or referral."""
    from app.workflows.post_appointment import fire_post_appointment
    return await fire_post_appointment(row)


# =========================================================================
# LAYER 2: STARTUP RECOVERY
# =========================================================================


async def recover_all_pending() -> None:
    """On server boot, recreate asyncio tasks for all pending messages.

    Past-due rows fire immediately (sleep(0)). Future rows sleep until due_at.
    This ensures recovery happens in seconds after a restart.
    """
    await asyncio.sleep(5)  # Let Supabase client initialize

    from app.main import supabase

    logger.info("SCHEDULER_LOOP | startup_recovery | loading all pending messages...")

    try:
        resp = await supabase._request(
            supabase.main_client,
            "GET",
            "/scheduled_messages",
            params={
                "status": "eq.pending",
                "order": "due_at.asc",
                "limit": "5000",
            },
            label="recover_all_pending",
        )

        if resp.status_code >= 400:
            logger.error("SCHEDULER_LOOP | startup_recovery | DB query failed: %d", resp.status_code)
            return

        rows = resp.json()
        if not rows:
            logger.info("SCHEDULER_LOOP | startup_recovery | no pending messages found")
            return

        now = datetime.now(timezone.utc)
        past_due = 0
        future = 0

        for row in rows:
            task = create_fire_task(row)
            if task:
                try:
                    due_str = row.get("due_at", "")
                    due = datetime.fromisoformat(due_str.replace("Z", "+00:00")) if isinstance(due_str, str) else due_str
                    if due and due <= now:
                        past_due += 1
                    else:
                        future += 1
                except (ValueError, TypeError):
                    future += 1

        logger.info(
            "SCHEDULER_LOOP | startup_recovery | created %d tasks (%d past-due, %d future)",
            len(rows), past_due, future,
        )

        # Also recover orphaned 'processing' rows (crashed mid-fire)
        await _recover_orphaned_processing(supabase)

    except Exception:
        logger.exception("SCHEDULER_LOOP | startup_recovery | failed")


async def _recover_orphaned_processing(supabase: Any) -> None:
    """Reset 'processing' rows older than 10 minutes back to 'pending'."""
    cutoff = (datetime.now(timezone.utc) - timedelta(minutes=_ORPHAN_TIMEOUT_MINUTES)).isoformat()

    resp = await supabase._request(
        supabase.main_client,
        "PATCH",
        "/scheduled_messages",
        params={
            "status": "eq.processing",
            "fired_at": f"lt.{cutoff}",
        },
        json={"status": "pending", "fired_at": None},
        headers={"Prefer": "return=representation"},
        label="recover_orphaned",
    )

    if resp.status_code < 400:
        rows = resp.json()
        if rows:
            logger.warning(
                "SCHEDULER_LOOP | recovered %d orphaned 'processing' rows (crashed mid-fire)",
                len(rows),
            )
            for row in rows:
                create_fire_task(row)


# =========================================================================
# LAYER 3: SAFETY-NET POLLER
# =========================================================================


async def run_safety_net_poller() -> None:
    """Background poller — last resort, runs every 30 minutes.

    Only picks up messages that should have fired but didn't (2-min grace period).
    In normal operation, Layer 1 (asyncio tasks) handles everything.
    """
    await asyncio.sleep(120)  # Wait for startup recovery to finish first

    logger.info("SCHEDULER_LOOP | safety_net | started (interval=%ds)", _SAFETY_NET_INTERVAL)

    while True:
        try:
            await _safety_net_sweep()
        except Exception:
            logger.exception("SCHEDULER_LOOP | safety_net | sweep failed")

        await asyncio.sleep(_SAFETY_NET_INTERVAL)


async def _safety_net_sweep() -> None:
    """Single sweep: find and fire past-due messages without active tasks."""
    from app.main import supabase

    # Only pick up messages that are 2+ minutes past due
    cutoff = (datetime.now(timezone.utc) - timedelta(seconds=_GRACE_PERIOD_SECONDS)).isoformat()
    active_ids = get_active_task_ids()

    resp = await supabase._request(
        supabase.main_client,
        "GET",
        "/scheduled_messages",
        params={
            "status": "eq.pending",
            "due_at": f"lt.{cutoff}",
            "order": "due_at.asc",
            "limit": "20",
        },
        label="safety_net_sweep",
    )

    if resp.status_code >= 400:
        logger.warning("SCHEDULER_LOOP | safety_net | query failed: %d", resp.status_code)
        return

    rows = resp.json()
    if not rows:
        return

    # Filter out rows that already have active asyncio tasks
    missed = [r for r in rows if str(r.get("id", "")) not in active_ids]

    if missed:
        logger.warning(
            "SCHEDULER_LOOP | safety_net | found %d missed messages (of %d past-due)",
            len(missed), len(rows),
        )
        for row in missed:
            create_fire_task(row)

    # Also recover orphaned processing rows
    await _recover_orphaned_processing(supabase)
