"""Prefect flow — single entry point for reply + follow-up pipelines.

The text_engine flow is dynamically named per client at runtime:
    text_engine.with_options(name=f"{slug}--{client_id[:8]}")(ctx)
This makes each client appear as its own flow in the Prefect dashboard.
The slug--shortid format ensures uniqueness even when two clients share a name.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from typing import Any

from prefect import flow, task
from prefect.artifacts import create_markdown_artifact
from prefect.client.orchestration import get_client
from prefect.context import get_run_context

from app.text_engine.agent import call_agent
from app.text_engine.attachments import process_attachments
from app.text_engine.booking import load_booking_context
from app.text_engine.classification import classification_gate
from app.text_engine.conversation_sync import sync_conversation
from app.text_engine.data_loading import load_data, resolve_supported_languages
from app.text_engine.delivery import deliver_response
from app.text_engine.followup import run_followup
from app.text_engine.followup_scheduling import clear_smart_followup
from app.text_engine.offers import format_offers_for_prompt
from app.text_engine.post_processing import _extract_user_info, post_agent_processing
from app.text_engine.qualification import evaluate_qualification, compute_overall_status, apply_qualification_guard, format_services_for_followup
from app.text_engine.security import apply_security
from app.text_engine.timeline import build_timeline
from app.text_engine.utils import get_timezone
from app.models import PipelineContext
from app.services.ai_client import set_ai_context, clear_ai_context
from app.services.ghl_client import GHLClient
from app.services.ghl_links import build_ghl_contact_url
from app.services.postgres_client import postgres
from app.services.mattermost import post_message as post_slack_message
from app.services.supabase_client import supabase
from app.services.workflow_tracker import WorkflowTracker
from app.tools.transfer_to_human import execute_transfer

logger = logging.getLogger(__name__)

# Source value → GHL pipeline name mapping (3-pipeline routing)
_SOURCE_TO_PIPELINE: dict[str, str] = {
    "Auto Reactivation": "Auto Reactivation",
    "Reactivation Campaign": "Reactivation Campaign",
}
# Default (any source not in this dict) → "AI Setter"


def _cache_derived_values(ctx: PipelineContext) -> None:
    """Compute frequently-used derived values once after load_data().

    These are pure functions of ctx.config that don't change mid-pipeline.
    Caching avoids 20+ redundant recomputations across LLM call sites.
    """
    ctx.tz = get_timezone(ctx.config)
    ctx.supported_languages = resolve_supported_languages(ctx)
    # Offers and services text from compiled setter config
    ctx.offers_text = ctx.compiled.get("offers_text") or format_offers_for_prompt(ctx)
    _setter = ctx.compiled.get("_matched_setter") or {}
    service_config = _setter.get("services") or {}
    ctx.services_text = format_services_for_followup(service_config)


@flow(log_prints=True, timeout_seconds=600)
async def text_engine(ctx: PipelineContext) -> dict[str, Any]:
    """Single Prefect flow for both reply and follow-up pipelines.

    Flow name is overridden per-client via .with_options(name=slug--shortid).
    Tags (entity/name, contact, channel, trigger type, agent, result) are set
    by the webhook route via prefect.tags context manager + post-execution.
    """
    # Refresh default AI models cache if stale (no-op if <5 min old)
    from app.text_engine.model_resolver import refresh_defaults_if_stale
    await refresh_defaults_if_stale()

    # Create shared GHL client once for the entire pipeline
    ctx.ghl = GHLClient(api_key=ctx.ghl_api_key, location_id=ctx.ghl_location_id)

    # Set per-request AI context (API key + token tracker) for all nested AI calls
    tenant_keys = ctx.tenant_ai_keys or {}
    set_ai_context(
        api_key=ctx.openrouter_api_key,
        token_tracker=ctx.token_usage,
        google_key=tenant_keys.get("google", ""),
        openai_key=tenant_keys.get("openai", ""),
        deepseek_key=tenant_keys.get("deepseek", ""),
        xai_key=tenant_keys.get("xai", ""),
        anthropic_key=tenant_keys.get("anthropic", ""),
    )

    # Start timer for duration tracking
    ctx.pipeline_start_time = time.perf_counter()

    # Workflow tracker — persists run to workflow_runs table
    tracker = WorkflowTracker(
        ctx.trigger_type or "reply",
        entity_id=ctx.entity_id,
        ghl_contact_id=ctx.contact_id,
        trigger_source="webhook",
        mode="test" if ctx.is_test_mode else "production",
    )
    ctx.workflow_run_id = tracker.run_id

    logger.info(
        "Pipeline started: %s for %s (contact: %s)",
        ctx.trigger_type,
        ctx.slug,
        ctx.contact_id,
    )

    result = {}
    try:
        if ctx.trigger_type == "reply":
            result = await reply_pipeline(ctx)
        else:
            result = await followup_pipeline(ctx)

        # Add result tag + artifact to the current flow run for dashboard visibility
        await _add_result_tag(result, ctx.trigger_type)
        await _create_result_artifact(result, ctx)

        # Capture internal keys BEFORE popping (needed for tracker decisions)
        post_processing = result.get("_post_processing")
        classification_gate = result.get("_classification_gate")

        # Build workflow tracker decisions from pipeline context
        tracker.set_decisions(_build_tracker_decisions(ctx, result, post_processing, classification_gate))
        tracker.set_token_usage(ctx.token_usage)
        tracker.set_system_config(ctx.config.get("system_config") if ctx.config else None, setter_key=getattr(ctx, "agent_type", None))
        tracker.set_runtime_context({
            "timeline": ctx.timeline or "",
            "message": ctx.message or "",
            "contact": {
                "name": ctx.contact_name,
                "email": ctx.contact_email,
                "phone": ctx.contact_phone,
                "tags": getattr(ctx, "contact_tags", []),
                "source": getattr(ctx, "contact_source", ""),
                "form_interest": getattr(ctx, "form_interest", ""),
            },
            "booking_context": {
                "has_upcoming": getattr(ctx, "has_upcoming_booking", False),
                "upcoming_text": getattr(ctx, "upcoming_booking_text", ""),
                "past_text": getattr(ctx, "past_booking_text", ""),
            },
            "qualification": {
                "status": ctx.qualification_status,
                "notes": ctx.qualification_notes,
            },
            "extraction": ctx.extraction_result or {},
            "channel": ctx.channel,
            "agent_type": ctx.agent_type,
            "offers_text": getattr(ctx, "offers_text", ""),
            "services_text": getattr(ctx, "services_text", ""),
            "media_selected": {
                "url": getattr(ctx, "selected_media_url", None),
                "name": getattr(ctx, "selected_media_name", None),
                "type": getattr(ctx, "selected_media_type", None),
                "description": getattr(ctx, "selected_media_description", "")[:200] if getattr(ctx, "selected_media_description", None) else None,
            } if getattr(ctx, "selected_media_url", None) else None,
            "lead": {k: v for k, v in (ctx.lead or {}).items() if k != "extracted_data"} if ctx.lead else None,
            "supported_languages": getattr(ctx, "supported_languages", ["English"]),
            "agent_response": ctx.agent_response or "",
            "sync_stats": getattr(ctx, "sync_stats", None),
            "timeline_stats": getattr(ctx, "timeline_stats", None),
            "prompt_log": getattr(ctx, "prompt_log", []),
            "attachments_processed": len(getattr(ctx, "attachments", [])) if getattr(ctx, "attachments", None) else 0,
            "delivery_results": _serialize_delivery_results(getattr(ctx, "_delivery_results", None)),
        })
        tracker.set_metadata("channel", ctx.channel)
        tracker.set_metadata("agent_type", ctx.agent_type)
        tracker.set_metadata("result_path", result.get("path", ""))
        if not ctx.is_test_mode and ctx.lead and ctx.lead.get("id"):
            tracker.set_lead_id(ctx.lead["id"])

        # Strip internal keys before returning to webhook caller
        result.pop("_post_processing", None)
        result.pop("_classification_gate", None)
        return result
    except Exception as e:
        tracker.set_error(str(e))
        raise
    finally:
        await tracker.save()
        clear_ai_context()


async def run_pipeline_with_tracking(ctx: PipelineContext) -> dict[str, Any]:
    """Run reply or follow-up pipeline with full WorkflowTracker enrichment.

    Used by direct_runner.py for test/simulation mode. Does NOT use Prefect flow.
    Same tracker logic as text_engine() but without the @flow decorator.
    """
    from app.models import TokenUsage

    ctx.pipeline_start_time = time.perf_counter()

    tracker = WorkflowTracker(
        ctx.trigger_type or "reply",
        entity_id=ctx.entity_id,
        ghl_contact_id=ctx.contact_id,
        trigger_source="webhook",
        mode="test" if ctx.is_test_mode else "production",
    )
    ctx.workflow_run_id = tracker.run_id

    # Snapshot the current call count BEFORE this pipeline run.
    # The shared TokenUsage accumulates across all turns in run_single_test.
    # We only want THIS turn's calls in the workflow run.
    calls_before = len(ctx.token_usage.calls) if ctx.token_usage else 0

    result = {}
    try:
        if ctx.trigger_type == "reply":
            result = await reply_pipeline(ctx)
        else:
            result = await followup_pipeline(ctx)

        post_processing = result.get("_post_processing")
        classification_gate = result.get("_classification_gate")

        tracker.set_decisions(_build_tracker_decisions(ctx, result, post_processing, classification_gate))

        # Only capture THIS turn's LLM calls (not all accumulated calls)
        if ctx.token_usage:
            turn_usage = TokenUsage()
            turn_usage.calls = ctx.token_usage.calls[calls_before:]
            turn_usage.prompt_tokens = sum(c.get("prompt_tokens", 0) for c in turn_usage.calls)
            turn_usage.completion_tokens = sum(c.get("completion_tokens", 0) for c in turn_usage.calls)
            turn_usage.total_tokens = turn_usage.prompt_tokens + turn_usage.completion_tokens
            turn_usage.call_count = len(turn_usage.calls)
            tracker.set_token_usage(turn_usage)
        tracker.set_system_config(ctx.config.get("system_config") if ctx.config else None, setter_key=getattr(ctx, "agent_type", None))
        tracker.set_runtime_context({
            "timeline": ctx.timeline or "",
            "message": ctx.message or "",
            "contact": {
                "name": ctx.contact_name,
                "email": ctx.contact_email,
                "phone": ctx.contact_phone,
                "tags": getattr(ctx, "contact_tags", []),
                "source": getattr(ctx, "contact_source", ""),
            },
            "channel": ctx.channel,
            "agent_type": ctx.agent_type,
            "agent_response": ctx.agent_response or "",
            "prompt_log": getattr(ctx, "prompt_log", []),
        })
        tracker.set_metadata("channel", ctx.channel)
        tracker.set_metadata("agent_type", ctx.agent_type)
        tracker.set_metadata("result_path", result.get("path", ""))
        if not ctx.is_test_mode and ctx.lead and ctx.lead.get("id"):
            tracker.set_lead_id(ctx.lead["id"])

        result.pop("_post_processing", None)
        result.pop("_classification_gate", None)
        return result
    except Exception as e:
        tracker.set_error(str(e))
        raise
    finally:
        await tracker.save()


def _serialize_delivery_results(results: Any) -> Any:
    """Convert DeliveryResult/SplitDeliveryResult objects to JSON-serializable dicts."""
    if results is None:
        return None
    from dataclasses import asdict, is_dataclass
    if is_dataclass(results) and not isinstance(results, type):
        return asdict(results)
    if isinstance(results, list):
        return [asdict(r) if (is_dataclass(r) and not isinstance(r, type)) else r for r in results]
    return results


def _build_tracker_decisions(
    ctx: PipelineContext,
    result: dict[str, Any],
    post_processing: dict[str, Any] | None,
    classification_gate: str | None,
) -> dict[str, Any]:
    """Build structured decisions dict for the workflow_runs table."""
    decisions: dict[str, Any] = {}

    if ctx.trigger_type == "reply":
        # Classification
        decisions["classification"] = {
            "gates": ctx.classification_gates,
            "final_path": result.get("path", ""),
            "reason": result.get("reason", ""),
        }

        # Extraction
        if ctx.extraction_result:
            fields_updated = [k for k, v in ctx.extraction_result.items() if v]
            decisions["extraction"] = {
                **ctx.extraction_result,
                "fields_updated": fields_updated,
            }

        # Qualification
        decisions["qualification"] = {
            "status": ctx.qualification_status,
            "notes": ctx.qualification_notes,
        }

        # Media selection
        decisions["media_selection"] = {
            "selected": bool(getattr(ctx, "selected_media_url", None)),
        }

        # Agent
        decisions["agent"] = {
            "iterations": getattr(ctx, "agent_iterations", 0),
            "tools_used": ctx.tool_calls_log or [],
            "transfer_triggered": getattr(ctx, "transfer_triggered", False),
        }

        # Security
        _sec_result = getattr(ctx, "security_check_result", {}) or {}
        decisions["security"] = {
            "replacements_applied": getattr(ctx, "security_replacements_applied", False),
            "original_response": getattr(ctx, "security_original_response", ""),
            "llm_check": {
                "safe": _sec_result.get("safe"),
                "violations": _sec_result.get("violations", []),
                "rewritten": _sec_result.get("rewritten", False),
                "error": _sec_result.get("error"),
            },
        }

        # Delivery
        decisions["delivery"] = {
            "messages_sent": len(ctx.messages) if ctx.messages else 0,
            "channel": ctx.channel,
        }

        # Post-processing
        if post_processing and isinstance(post_processing, dict):
            followup_info = post_processing.get("followup")
            if followup_info:
                decisions["followup_scheduling"] = followup_info
            pipeline_info = post_processing.get("pipeline")
            if pipeline_info:
                decisions["pipeline_management"] = pipeline_info

    else:
        # Follow-up path
        decisions["determination"] = {
            "followup_needed": result.get("followUpNeeded", False),
            "reason": result.get("reason", ""),
            "attempt_number": (ctx.followup_stats.get("consecutive_followups", 0) if ctx.followup_stats else 0) + 1,
        }
        if ctx.followup_stats:
            decisions["media_selection"] = {
                "selected": bool(ctx.followup_stats.get("media_selected_index")),
                "media_type": ctx.followup_stats.get("media_selected_type"),
                "media_name": ctx.followup_stats.get("media_selected_name"),
                "media_library_total": ctx.followup_stats.get("media_library_total"),
                "media_library_filtered": ctx.followup_stats.get("media_library_filtered"),
            }
            decisions["generation"] = {
                "method": ctx.followup_stats.get("followup_mode", "ai_generated"),
                "text": result.get("suggestedFollowUp") or "",
                "is_smart_followup": ctx.followup_stats.get("is_smart_followup", False),
                "static_template_position": ctx.followup_stats.get("static_template_position"),
            }
        decisions["delivery"] = {
            "messages_sent": 1 if result.get("followUpNeeded") else 0,
            "channel": ctx.channel,
        }

    return decisions


async def _add_result_tag(result: dict[str, Any], trigger_type: str) -> None:
    """Derive and add a result: tag to the current Prefect flow run.

    Called at the end of the pipeline so the outcome is visible in the
    Prefect dashboard runs list without clicking into individual runs.

    Reply results:   result:replied, result:transferred, result:opted-out,
                     result:no-reply, result:bot-off
    Followup results: result:followup-sent, result:followup-skipped
    """
    # Derive the tag
    if trigger_type == "reply":
        path = result.get("path", "")
        gate = result.get("_classification_gate", "")
        if path == "Transfer To Human":
            tag = "result:transferred"
        elif path == "Opt Out":
            tag = "result:opted-out"
        elif path == "No Reply Needed" and gate == "stop_bot":
            tag = "result:bot-off"
        elif path == "No Reply Needed":
            tag = "result:no-reply"
        else:
            tag = "result:replied"
    else:
        tag = "result:followup-sent" if result.get("followUpNeeded") else "result:followup-skipped"

    # Update the flow run's tags via Prefect API
    try:
        run_ctx = get_run_context()
        async with get_client() as client:
            flow_run = await client.read_flow_run(run_ctx.flow_run.id)
            updated_tags = list(flow_run.tags) + [tag]
            await client.update_flow_run(run_ctx.flow_run.id, tags=updated_tags)
        logger.info("Added result tag: %s", tag)
    except Exception as e:
        logger.warning("Failed to add result tag: %s", e)


async def _create_result_artifact(result: dict[str, Any], ctx: PipelineContext) -> None:
    """Create a Prefect markdown artifact summarizing pipeline results.

    Shows up in the Artifacts tab of each flow run for quick visibility.
    Includes all pipeline outputs: classification, agent response, extraction,
    smart follow-up, pipeline stage, and delivery details.
    """
    try:
        if ctx.trigger_type == "reply":
            md = _build_reply_artifact(result, ctx)
        else:
            md = _build_followup_artifact(result, ctx)

        await create_markdown_artifact(
            key="pipeline-result",
            markdown=md,
            description=f"{ctx.trigger_type.title()} pipeline result",
        )
    except Exception as e:
        logger.warning("Failed to create result artifact: %s", e)


def _build_reply_artifact(result: dict[str, Any], ctx: PipelineContext) -> str:
    """Build comprehensive reply pipeline artifact."""
    path = result.get("path", "reply")
    reason = result.get("reason", "")
    msgs = [result.get(f"Message{i}", "") for i in range(1, 4)]
    msgs = [m for m in msgs if m]
    gate = result.get("_classification_gate", "")

    lines = [
        "## Reply Pipeline Result",
        "",
        "| Field | Value |",
        "|-------|-------|",
        f"| **Path** | {path} |",
        f"| **Agent Type** | {ctx.agent_type or 'setter_1'} |",
        f"| **Channel** | {ctx.channel} |",
        f"| **Test Mode** | {ctx.is_test_mode} |",
        f"| **Timezone** | {ctx.config.get('timezone', 'N/A')} |",
    ]
    if gate:
        lines.append(f"| **Classification Gate** | {gate} |")
    if reason:
        lines.append(f"| **Reason** | {reason[:300]} |")
    if msgs:
        lines.append(f"| **Messages** | {len(msgs)} |")
    if ctx.has_upcoming_booking:
        lines.append("| **Has Upcoming Booking** | Yes |")
    lines.append("")

    # Lead & Contact info
    lead_id = ctx.lead.get("id", "")[:8] if ctx.lead else ""
    lead_status = "existing" if ctx.lead else "none"
    lines.append("### Lead & Contact")
    lines.append("")
    lines.append("| Field | Value |")
    lines.append("|-------|-------|")
    if ctx.contact_name:
        lines.append(f"| **Name** | {ctx.contact_name} |")
    if ctx.contact_phone:
        lines.append(f"| **Phone** | {ctx.contact_phone} |")
    if ctx.contact_email:
        lines.append(f"| **Email** | {ctx.contact_email} |")
    if lead_id:
        lines.append(f"| **Lead** | {lead_status} ({lead_id}...) |")
    else:
        lines.append(f"| **Lead** | {lead_status} |")
    if ctx.contact_tags:
        lines.append(f"| **Tags** | {', '.join(ctx.contact_tags[:10])} |")
    lines.append("")

    # Conversation Sync stats
    if ctx.sync_stats:
        ss = ctx.sync_stats
        lines.append("### Conversation Sync")
        lines.append("")
        lines.append("| Field | Value |")
        lines.append("|-------|-------|")
        lines.append(f"| **GHL Messages Fetched** | {ss.get('ghl_messages_fetched', 0)} |")
        lines.append(f"| **New Messages Synced** | {ss.get('messages_synced', 0)} |")
        lines.append(f"| **Calls Found** | {ss.get('calls_found', 0)} |")
        lines.append(f"| **Calls Synced** | {ss.get('calls_synced', 0)} |")
        lines.append("")

    # Timeline stats
    if ctx.timeline_stats:
        ts = ctx.timeline_stats
        lines.append("### Timeline")
        lines.append("")
        lines.append("| Field | Value |")
        lines.append("|-------|-------|")
        lines.append(f"| **Messages** | {ts.get('messages', 0)} |")
        lines.append(f"| **Call Logs** | {ts.get('calls', 0)} |")
        lines.append(f"| **Attachments** | {ts.get('attachments', 0)} |")
        channels = ts.get("channels", [])
        if channels:
            lines.append(f"| **Channels** | {', '.join(channels)} |")
        source_types = ts.get("source_types", [])
        if source_types:
            lines.append(f"| **Source Types** | {', '.join(source_types)} |")
        lines.append("")

    # Full conversation timeline
    if ctx.timeline:
        lines.append("### Conversation History")
        lines.append("")
        lines.append("```")
        lines.append(ctx.timeline[:5000])
        lines.append("```")
        lines.append("")

    # Booking details
    if ctx.bookings:
        lines.append("### Upcoming Appointments")
        lines.append("")
        for apt in ctx.bookings[:3]:
            start = apt.get("start", "")
            title = apt.get("title", "Appointment")
            status = apt.get("status", "unknown")
            lines.append(f"- **{title}**: {start} ({status})")
        lines.append("")

    # Classification gates
    if ctx.classification_gates:
        lines.append("### Classification Gates")
        lines.append("")
        lines.append("| Gate | Result | Reason |")
        lines.append("|------|--------|--------|")
        for g in ctx.classification_gates:
            gate_reason = g.get("reason", "")[:100]
            lines.append(f"| {g['gate']} | {g['result']} | {gate_reason} |")
        lines.append("")

    # Inbound message
    if ctx.message:
        lines.append("### Inbound Message")
        lines.append("")
        lines.append(f"> {ctx.message[:500]}")
        lines.append("")

    # Agent tool calls
    if ctx.tool_calls_log:
        lines.append(f"### Agent Tool Calls ({ctx.agent_iterations} iteration{'s' if ctx.agent_iterations != 1 else ''})")
        lines.append("")
        for i, tc in enumerate(ctx.tool_calls_log, 1):
            lines.append(f"**{i}. {tc['name']}**({tc['args'][:80]})")
            lines.append(f"  → {tc['result_preview'][:150]}")
            lines.append("")
    elif ctx.agent_iterations:
        lines.append(f"### Agent ({ctx.agent_iterations} iteration{'s' if ctx.agent_iterations != 1 else ''}, no tool calls)")
        lines.append("")

    # Response messages
    if msgs:
        lines.append("### Response Messages")
        lines.append("")
        for i, m in enumerate(msgs, 1):
            lines.append(f"**Message {i}:** {m}")
            lines.append("")

    # Post-processing details (extraction, follow-up, pipeline)
    post = result.get("_post_processing", {})
    if post:
        # Extraction
        extraction = post.get("extraction", {})
        if extraction and isinstance(extraction, dict):
            lines.append("### User Info Extraction")
            lines.append("")
            lines.append("| Field | Value |")
            lines.append("|-------|-------|")
            name = extraction.get("name", "")
            email = extraction.get("email", "")
            phone = extraction.get("phone", "")
            if name:
                lines.append(f"| **Name** | {name} |")
            if email:
                lines.append(f"| **Email** | {email} |")
            if phone:
                lines.append(f"| **Phone** | {phone} |")
            # Qualification lives on ctx (dedicated qual agent)
            lines.append(f"| **Qualification** | {ctx.qualification_status} |")
            if ctx.qualification_notes:
                notes_str = str(ctx.qualification_notes)[:200]
                lines.append(f"| **Qualification Notes** | {notes_str} |")
            if not name and not email and not phone:
                lines.append("| *(no new info extracted)* | |")
            lines.append("")

        # Smart follow-up
        followup = post.get("followup", {})
        if followup and isinstance(followup, dict):
            action = followup.get("action", "none")
            lines.append("### Smart Follow-Up")
            lines.append("")
            lines.append("| Field | Value |")
            lines.append("|-------|-------|")
            lines.append(f"| **Action** | {action} |")
            if followup.get("timeframe"):
                lines.append(f"| **Timeframe** | {followup['timeframe']} |")
            if followup.get("due"):
                lines.append(f"| **Due** | {followup['due']} |")
            fu_reason = followup.get("stop_reason") or followup.get("keep_reason") or followup.get("reason", "")
            if fu_reason:
                lines.append(f"| **Reason** | {fu_reason[:200]} |")
            lines.append("")

        # Pipeline stage
        pipeline = post.get("pipeline", {})
        if pipeline and isinstance(pipeline, dict):
            p_action = pipeline.get("action", "none")
            lines.append("### Pipeline Stage")
            lines.append("")
            lines.append("| Field | Value |")
            lines.append("|-------|-------|")
            lines.append(f"| **Action** | {p_action} |")
            if pipeline.get("current_stage"):
                lines.append(f"| **Current Stage** | {pipeline['current_stage']} |")
            if pipeline.get("new_stage"):
                lines.append(f"| **New Stage** | {pipeline['new_stage']} |")
            if pipeline.get("reason"):
                lines.append(f"| **Reason** | {pipeline['reason'][:200]} |")
            lines.append("")

    # Token usage
    if ctx.token_usage and ctx.token_usage.call_count > 0:
        tu = ctx.token_usage
        lines.append("### AI Token Usage")
        lines.append("")
        lines.append("| Field | Value |")
        lines.append("|-------|-------|")
        lines.append(f"| **Prompt Tokens** | {tu.prompt_tokens:,} |")
        lines.append(f"| **Completion Tokens** | {tu.completion_tokens:,} |")
        lines.append(f"| **Total Tokens** | {tu.total_tokens:,} |")
        lines.append(f"| **AI Calls** | {tu.call_count} |")
        lines.append(f"| **Estimated Cost** | ${tu.estimated_cost():.4f} |")
        lines.append("")

    return "\n".join(lines)


def _build_followup_artifact(result: dict[str, Any], ctx: PipelineContext) -> str:
    """Build comprehensive follow-up pipeline artifact."""
    needed = result.get("followUpNeeded", False)
    reason = result.get("reason", "")
    suggested = result.get("suggestedFollowUp", "")
    media_type = result.get("media_type", "")
    media_url = result.get("media_url", "")
    fu_path = result.get("path", "")
    fs = ctx.followup_stats

    lines = [
        "## Follow-Up Pipeline Result",
        "",
        "| Field | Value |",
        "|-------|-------|",
        f"| **Follow-Up Needed** | {needed} |",
        f"| **Channel** | {ctx.channel} |",
        f"| **Test Mode** | {ctx.is_test_mode} |",
        f"| **Timezone** | {ctx.config.get('timezone', 'N/A')} |",
    ]
    if fu_path:
        lines.append(f"| **Path** | {fu_path} |")
    if reason:
        lines.append(f"| **Reason** | {reason[:300]} |")
    if fs:
        lines.append(f"| **Attempt #** | {fs.get('attempt_number', '?')} ({fs.get('consecutive_followups', 0)} previous with no reply) |")
        if fs.get("is_smart_followup"):
            lines.append("| **Smart Follow-Up** | Yes |")
    if ctx.has_upcoming_booking:
        lines.append("| **Has Upcoming Booking** | Yes |")
    lines.append("")

    # Lead & Contact
    lead_id = ctx.lead.get("id", "")[:8] if ctx.lead else ""
    lead_status = "existing" if ctx.lead else "none"
    lines.append("### Lead & Contact")
    lines.append("")
    lines.append("| Field | Value |")
    lines.append("|-------|-------|")
    if ctx.contact_name:
        lines.append(f"| **Name** | {ctx.contact_name} |")
    if ctx.contact_phone:
        lines.append(f"| **Phone** | {ctx.contact_phone} |")
    if ctx.contact_email:
        lines.append(f"| **Email** | {ctx.contact_email} |")
    if lead_id:
        lines.append(f"| **Lead** | {lead_status} ({lead_id}...) |")
    else:
        lines.append(f"| **Lead** | {lead_status} |")
    if ctx.contact_tags:
        lines.append(f"| **Tags** | {', '.join(ctx.contact_tags[:10])} |")
    lines.append("")

    # Conversation Sync
    if ctx.sync_stats:
        ss = ctx.sync_stats
        lines.append("### Conversation Sync")
        lines.append("")
        lines.append("| Field | Value |")
        lines.append("|-------|-------|")
        lines.append(f"| **GHL Messages Fetched** | {ss.get('ghl_messages_fetched', 0)} |")
        lines.append(f"| **New Messages Synced** | {ss.get('messages_synced', 0)} |")
        lines.append(f"| **Calls Found** | {ss.get('calls_found', 0)} |")
        lines.append(f"| **Calls Synced** | {ss.get('calls_synced', 0)} |")
        lines.append("")

    # Timeline stats
    if ctx.timeline_stats:
        ts = ctx.timeline_stats
        lines.append("### Timeline")
        lines.append("")
        lines.append("| Field | Value |")
        lines.append("|-------|-------|")
        lines.append(f"| **Messages** | {ts.get('messages', 0)} |")
        lines.append(f"| **Call Logs** | {ts.get('calls', 0)} |")
        lines.append(f"| **Attachments** | {ts.get('attachments', 0)} |")
        channels = ts.get("channels", [])
        if channels:
            lines.append(f"| **Channels** | {', '.join(channels)} |")
        lines.append("")

    # Full conversation timeline
    if ctx.timeline:
        lines.append("### Conversation History")
        lines.append("")
        lines.append("```")
        lines.append(ctx.timeline[:5000])
        lines.append("```")
        lines.append("")

    # Booking details
    if ctx.bookings:
        lines.append("### Upcoming Appointments")
        lines.append("")
        for apt in ctx.bookings[:3]:
            start = apt.get("start", "")
            title = apt.get("title", "Appointment")
            status = apt.get("status", "unknown")
            lines.append(f"- **{title}**: {start} ({status})")
        lines.append("")

    # Media selection details
    if fs and needed:
        lines.append("### Media Selection")
        lines.append("")
        lines.append("| Field | Value |")
        lines.append("|-------|-------|")
        lines.append(f"| **Library Total** | {fs.get('media_library_total', 0)} |")
        lines.append(f"| **After Filtering** | {fs.get('media_library_filtered', 0)} |")
        selected_idx = fs.get("media_selected_index", 0)
        if selected_idx and selected_idx > 0:
            lines.append(f"| **Selected** | #{selected_idx}: {fs.get('media_selected_name', '')} ({fs.get('media_selected_type', '')}) |")
            desc = fs.get("media_selected_description", "")
            if desc:
                lines.append(f"| **Description** | {desc} |")
        else:
            lines.append("| **Selected** | None (text only) |")
        lines.append(f"| **Needs Text** | {fs.get('needs_text', True)} |")
        if media_url:
            lines.append(f"| **Media URL** | {media_url} |")
        lines.append("")

    # Generated follow-up text
    if suggested:
        lines.append("### Generated Follow-Up")
        lines.append("")
        lines.append(f"> {suggested}")
        lines.append("")

    # Token usage
    if ctx.token_usage and ctx.token_usage.call_count > 0:
        tu = ctx.token_usage
        lines.append("### AI Token Usage")
        lines.append("")
        lines.append("| Field | Value |")
        lines.append("|-------|-------|")
        lines.append(f"| **Prompt Tokens** | {tu.prompt_tokens:,} |")
        lines.append(f"| **Completion Tokens** | {tu.completion_tokens:,} |")
        lines.append(f"| **Total Tokens** | {tu.total_tokens:,} |")
        lines.append(f"| **AI Calls** | {tu.call_count} |")
        lines.append(f"| **Estimated Cost** | ${tu.estimated_cost():.4f} |")
        lines.append("")

    return "\n".join(lines)


async def ensure_opportunity(ctx: PipelineContext) -> None:
    """Ensure a pipeline opportunity exists for this contact. PROD only.

    Runs early in the reply pipeline (before classification) so that even
    early-exit paths (opt-out, transfer, stop bot) get an opportunity created
    and auto-reengage/auto-booked fires.

    Populates ctx fields: opportunity_id, pipeline_id, current_pipeline_stage,
    pipeline_name_to_id, pipeline_id_to_name.
    """
    if ctx.is_test_mode:
        return

    try:
        ghl = ctx.ghl

        # 1. Get pipelines and build stage maps
        pipelines = await ghl.get_pipelines()
        if not pipelines:
            logger.warning("No pipelines found — skipping opportunity management")
            return

        lead_source = (ctx.lead.get("source") or "") if ctx.lead else ""
        pipeline_name = _SOURCE_TO_PIPELINE.get(lead_source, "AI Setter")
        pipeline = next((p for p in pipelines if p.get("name") == pipeline_name), pipelines[0])

        ctx.pipeline_id = pipeline["id"]
        for stage in pipeline.get("stages", []):
            key = stage["name"].lower().replace(" ", "_")
            ctx.pipeline_name_to_id[key] = stage["id"]
            ctx.pipeline_id_to_name[stage["id"]] = key

        # 2. Search for existing opportunity
        opps = await ghl.search_opportunities(ctx.contact_id, ctx.pipeline_id)
        opp = opps[0] if opps else None

        if opp:
            ctx.opportunity_id = opp.get("id", "")
            stage_id = opp.get("pipelineStageId", "")
            ctx.current_pipeline_stage = ctx.pipeline_id_to_name.get(stage_id, "contacted")
            logger.info("OPPORTUNITY | found=True | stage=%s | opp=%s", ctx.current_pipeline_stage, ctx.opportunity_id[:12])
        else:
            # Create opportunity
            try:
                new_opp = await ghl.create_opportunity({
                    "pipelineId": ctx.pipeline_id,
                    "pipelineStageId": ctx.pipeline_name_to_id.get("contacted", ""),
                    "contactId": ctx.contact_id,
                    "name": ctx.contact_name or "Lead",
                    "status": "open",
                })
                ctx.opportunity_id = new_opp.get("id", "")
                ctx.current_pipeline_stage = "contacted"
                logger.info("Created opportunity: %s", ctx.opportunity_id)
            except Exception as e:
                # Handle 409 race condition — retry search
                if "409" in str(e) or "conflict" in str(e).lower():
                    opps = await ghl.search_opportunities(ctx.contact_id, ctx.pipeline_id)
                    if opps:
                        ctx.opportunity_id = opps[0].get("id", "")
                        stage_id = opps[0].get("pipelineStageId", "")
                        ctx.current_pipeline_stage = ctx.pipeline_id_to_name.get(stage_id, "contacted")
                    else:
                        raise
                else:
                    raise

        # 3. Auto-booked: upcoming booking → move to "booked"
        if ctx.has_upcoming_booking and ctx.current_pipeline_stage != "booked":
            if "booked" in ctx.pipeline_name_to_id:
                await ghl.update_opportunity(
                    ctx.opportunity_id, {"pipelineStageId": ctx.pipeline_name_to_id["booked"]}
                )
                logger.info("Auto-booked: %s → booked", ctx.current_pipeline_stage)
                ctx.current_pipeline_stage = "booked"
                return

        # 4. Auto-reengage: lead replied from a negative stage → move to "engaged"
        negative_stages = {"opt_out", "not_interested", "wrong_number", "not_qualified"}
        if ctx.current_pipeline_stage in negative_stages:
            if "engaged" in ctx.pipeline_name_to_id:
                await ghl.update_opportunity(
                    ctx.opportunity_id, {"pipelineStageId": ctx.pipeline_name_to_id["engaged"]}
                )
                logger.info("Auto-reengaged: %s → engaged", ctx.current_pipeline_stage)
                ctx.current_pipeline_stage = "engaged"

    except Exception as e:
        logger.warning("ensure_opportunity failed: %s", e, exc_info=True)


async def reply_pipeline(ctx: PipelineContext) -> dict[str, Any]:
    """Reply pipeline — Steps 3.2 through 3.11.

    Each step will be implemented as a @task function for Prefect visibility.
    Steps are filled in as we complete each Phase 3 sub-step.
    """
    # Log inbound message details
    logger.info(
        "INBOUND | contact=%s | name=%s | channel=%s | agent=%s | message=%s",
        ctx.contact_id,
        ctx.contact_name or "(unknown)",
        ctx.channel,
        ctx.agent_type or "setter_1",
        ctx.message,
    )

    # Step 3.2: Data loading (GHL contact, prompts, lead)
    await load_data(ctx)
    _cache_derived_values(ctx)

    # Step 3.3–3.5: Conversation sync, storage, and timeline
    if ctx.is_test_mode:
        # Test mode (simulator/sandbox):
        # Skip GHL conversation sync, chat history reads/writes, and timeline build.
        # The test runner pre-populates ctx.timeline and ctx.chat_history from its
        # in-memory conversation array — same format, different data source.
        logger.info("PIPELINE | test_mode — skipping sync/store/build (timeline=%d chars)", len(ctx.timeline or ""))
    else:
        # Production mode: full GHL sync + chat history + timeline
        await sync_conversation(ctx)

        if not ctx.message:
            _detect_inbound_from_sync(ctx)

        await _ensure_inbound_stored(ctx)
        logger.info("PIPELINE | data_loaded | sync_done | pending_attachments=%d", len(ctx.pending_attachments))

        await process_attachments(ctx)

        if ctx.attachments:
            await _embed_attachment_ids(ctx)

        await build_timeline(ctx)

    # Step 3.6: Booking context
    await load_booking_context(ctx)

    # Step 3.6b: Ensure opportunity exists + auto-reengage/auto-booked (PROD only)
    await ensure_opportunity(ctx)

    # Step 3.7: Classification gate (stop bot, transfer, opt-out, response determination)
    # Fallback: if classification fails after all retries, default to "respond"
    # (safer to reply than to ghost the lead's message)
    try:
        await classification_gate(ctx)
    except Exception as e:
        logger.error(
            "Classification gate failed after all retries — defaulting to 'respond': %s", e,
            exc_info=True,
        )
        ctx.response_path = "respond"
        ctx.response_reason = f"Classification unavailable (error: {type(e).__name__})"
        ctx.classification_gates.append({
            "gate": "Classification Error Fallback",
            "result": "fallback_respond",
            "reason": str(e)[:200],
        })

    logger.info(
        "CLASSIFICATION | path=%s | reason=%s",
        ctx.response_path or "respond",
        ctx.response_reason[:200] if ctx.response_reason else "(none)",
    )

    # Early exit if classifier says don't proceed to agent
    if ctx.response_path and ctx.response_path != "respond":
        logger.info(
            "Classification gate exit: path=%s reason=%s",
            ctx.response_path,
            ctx.response_reason[:100] if ctx.response_reason else "",
        )

        # ----------------------------------------------------------
        # dont_respond: skip agent + delivery, but STILL run
        # extraction + post-processing so follow-up scheduler and
        # pipeline management can evaluate the lead's message.
        # ----------------------------------------------------------
        if ctx.response_path == "dont_respond":
            # Run extraction only (contact info) — skip qualification agent to save LLM call
            try:
                extraction = await _extract_user_info(ctx)
                ctx.extraction_result = extraction
                logger.info("DONT_RESPOND EXTRACTION | name=%s email=%s phone=%s",
                    bool(extraction.get("name")), bool(extraction.get("email")), bool(extraction.get("phone")))
            except Exception as e:
                logger.warning("Dont-respond extraction failed: %s", e, exc_info=True)
                ctx.extraction_result = {}

            # Mark that no agent response was generated — post-processing
            # agents (follow-up scheduler, pipeline management) can use this
            # to know that no outbound message was sent this turn.
            ctx.agent_response = ""
            ctx.no_reply_sent = True

            # Run post-processing (follow-up scheduler + pipeline management).
            # _smart_followup already handles has_booking checks internally,
            # so we don't need the separate clear_smart_followup call here.
            post_results = await post_agent_processing(ctx)

            return {
                "path": "No Reply Needed",
                "reason": ctx.response_reason,
                "_classification_gate": ctx.response_path,
                "_post_processing": post_results,
            }

        # ----------------------------------------------------------
        # All other non-respond paths (opt_out, human, stop_bot):
        # keep original early-exit behavior with their own side effects.
        # ----------------------------------------------------------
        if ctx.response_path == "opt_out":
            await clear_smart_followup(ctx)

        # Execute side effects for specific exit paths
        if ctx.response_path == "opt_out" and not ctx.is_test_mode:
            # Add opt-out tags from setter config (defaults to ["opt out"])
            opt_out_tags = ["opt out"]  # default
            try:
                sc = ctx.config.get("system_config", {}) if ctx.config else {}
                setter_key = getattr(ctx, "agent_type", None) or "setter_1"
                setter = sc.get("setters", {}).get(setter_key, {})
                configured = setter.get("tags", {}).get("opt_out")
                if isinstance(configured, list):
                    opt_out_tags = configured
            except Exception:
                pass
            for tag in opt_out_tags:
                try:
                    await ctx.ghl.add_tag(ctx.contact_id, tag)
                    logger.info("Added opt-out tag '%s' for contact", tag)
                except Exception as e:
                    logger.warning("Failed to add opt-out tag '%s': %s", tag, e)

            # Move opportunity to opt_out stage (post_processing never runs for this path)
            if ctx.opportunity_id and ctx.pipeline_name_to_id.get("opt_out"):
                try:
                    await ctx.ghl.update_opportunity(
                        ctx.opportunity_id,
                        {"pipelineStageId": ctx.pipeline_name_to_id["opt_out"]}
                    )
                    ctx.current_pipeline_stage = "opt_out"
                    logger.info("Pipeline: → opt_out (classification gate)")
                except Exception as e:
                    logger.warning("Opt-out pipeline update failed: %s", e)

        elif ctx.response_path == "human":
            # Unified transfer: tags + log + routing + notifications
            # execute_transfer handles test mode internally
            await execute_transfer(
                reason=ctx.response_reason or "Classifier-detected transfer",
                ghl=ctx.ghl,
                contact_id=ctx.contact_id,
                contact_name=ctx.contact_name,
                contact_email=ctx.contact_email or "",
                contact_phone=ctx.contact_phone or "",
                client_name=ctx.config.get("name", ""),
                ghl_location_id=ctx.ghl_location_id,
                config=ctx.config,
                is_test_mode=ctx.is_test_mode,
                channel=ctx.channel,
                entity_id=ctx.config.get("id", ctx.entity_id),
                lead_id=ctx.lead.get("id") if ctx.lead else None,
                agent_type="Classifier",
            )

        # Map internal paths to GHL-expected values
        path_map = {
            "human": "Transfer To Human",
            "opt_out": "Opt Out",
            "stop_bot": "No Reply Needed",
        }
        return {
            "path": path_map.get(ctx.response_path, ctx.response_path),
            "reason": ctx.response_reason,
            "_classification_gate": ctx.response_path,
        }

    # Step 3.7b: Pre-agent extraction + qualification (PARALLEL)
    # Extraction (name/email/phone) and qualification (service criteria) run in parallel.
    # Both evaluate what the LEAD said — the agent's response is irrelevant.
    try:
        extraction_task = _extract_user_info(ctx)
        qual_task = evaluate_qualification(ctx)

        extraction, qual_result = await asyncio.gather(extraction_task, qual_task)

        # Process extraction (name/email/phone only)
        ctx.extraction_result = extraction

        # Process qualification (from dedicated qual agent)
        if qual_result:
            new_status = compute_overall_status(qual_result, ctx.config.get("service_config", {}), agent_key=ctx.agent_type or None)
            prev_status = ctx.qualification_status
            prev_notes = ctx.qualification_notes

            final_status, final_notes = apply_qualification_guard(
                new_status, prev_status, qual_result, prev_notes,
            )

            ctx.qualification_status = final_status
            ctx.qualification_notes = final_notes

        logger.info(
            "PRE-AGENT | extraction=name:%s email:%s phone:%s | qual=%s",
            bool(extraction.get("name")),
            bool(extraction.get("email")),
            bool(extraction.get("phone")),
            ctx.qualification_status,
        )
    except Exception as e:
        logger.warning("Pre-agent extraction/qualification failed: %s", e, exc_info=True)
        ctx.extraction_result = {}

    # Step 3.7b: Reply media selection (dedicated selector — runs before reply agent)
    from app.text_engine.agent import select_reply_media
    await select_reply_media(ctx)

    # Step 3.8: Agent routing & response generation (tool-calling loop)
    await call_agent(ctx)

    # If agent triggered transfer_to_human tool, suppress response
    if ctx.transfer_triggered:
        logger.info("Agent triggered transfer_to_human — suppressing AI response")
        return {"path": "Transfer To Human", "reason": "Agent initiated transfer to human"}

    # Safety Layer 1a: Pipeline-level empty response guard
    # Agent has its own internal retry (nudge on empty content inside tool loop),
    # but can still exit with empty string. This is a second, pipeline-level retry.
    if not ctx.agent_response or not ctx.agent_response.strip():
        logger.warning(
            "Agent returned empty response (first attempt) — retrying | client=%s contact=%s",
            ctx.config.get("name", ""),
            ctx.contact_id,
        )
        ctx.agent_response = ""
        ctx.agent_iterations = 0
        ctx.tool_calls_log = []
        await call_agent(ctx)

        if not ctx.agent_response or not ctx.agent_response.strip():
            logger.error(
                "Agent returned empty response after retry — aborting delivery | client=%s contact=%s agent_type=%s",
                ctx.config.get("name", ""),
                ctx.contact_id,
                ctx.agent_type,
            )
            ghl_link = ""
            if ctx.ghl_location_id:
                ghl_url = build_ghl_contact_url(
                    ctx.ghl_location_id,
                    ctx.contact_id,
                    (ctx.config or {}).get("ghl_domain", ""),
                )
                ghl_link = f"\n*GHL Link:* <{ghl_url}|View Contact>" if ghl_url else ""
            alert_text = (
                f":warning: *Empty Agent Response*\n"
                f"*Client:* {ctx.config.get("name", "Unknown")}\n"
                f"*Contact:* {ctx.contact_name} (`{ctx.contact_id}`)\n"
                f"*Channel:* {ctx.channel}\n"
                f"*Agent Type:* {ctx.agent_type or 'setter_1'}\n"
                f"*What happened:* Agent returned empty/blank text after 2 attempts. "
                f"Delivery aborted — no message sent to lead."
                f"{ghl_link}"
            )
            try:
                await post_slack_message("#python-errors", alert_text)
            except Exception as e:
                logger.warning("Failed to send Slack alert for empty agent response: %s", e)

            # Empty-response recovery: the lead still needs to hear from us tomorrow.
            # Schedule FU#1 so a silent LLM hiccup doesn't drop them from the ladder.
            if not ctx.is_test_mode:
                try:
                    from app.services.message_scheduler import schedule_followup_chain
                    await schedule_followup_chain(ctx, position=1, triggered_by="empty_response_recovery")
                except Exception as e:
                    logger.warning("FU scheduling after empty response failed: %s", e)

            return {"path": "No Reply Needed", "reason": "Agent returned empty response after retry"}

    # Step 3.8a: Security — term replacements + LLM compliance check
    # Runs on the full response BEFORE timeline update and BEFORE message split.
    await apply_security(ctx)

    # Append AI response to timeline so post-processing (extraction, follow-up) can see it
    if ctx.agent_response:
        channel_tag = f" [{ctx.channel}]" if ctx.channel else ""
        ctx.timeline = f"[Just sent] AI{channel_tag}: {ctx.agent_response}\n\n{ctx.timeline}"

    # Step 3.8b: Re-fetch booking context after agent — if the agent booked
    # an appointment, GHL now has it. Refreshing ensures has_upcoming_booking
    # is correct for post-processing (smart follow-up auto-stop, pipeline stage).
    await load_booking_context(ctx)

    # Step 3.9: Post-agent processing (extraction, follow-up, pipeline)
    post_results = await post_agent_processing(ctx)

    # Pre-delivery stop bot re-check — close the race between agent generation
    # and actual SMS send. If staff tagged "stop bot" any time after the
    # classification gate (during LLM work, security, post-processing), suppress
    # the reply so we don't step on a human reply mid-conversation.
    if not ctx.is_test_mode:
        try:
            refreshed_contact = await ctx.ghl.get_contact(ctx.contact_id)
            if refreshed_contact:
                refreshed_tags = ", ".join(refreshed_contact.get("tags", []))
                if "stop bot" in refreshed_tags:
                    logger.info("Pre-delivery stop bot tag detected — suppressing AI response")
                    return {"path": "No Reply Needed", "reason": "Stop bot tag added before delivery"}
        except Exception as e:
            logger.warning("Pre-delivery stop bot re-check failed: %s", e)

    # Step 3.10 + 3.11: Response delivery (split, store, email) with test mode guards
    response = await deliver_response(ctx)

    # Step 3.12: Schedule FU#1 + reschedule reactivation (PROD only, only if reply was sent)
    if not ctx.is_test_mode and ctx.agent_response:
        try:
            from app.services.message_scheduler import schedule_followup_chain
            await schedule_followup_chain(ctx)
        except Exception as e:
            logger.warning("FU scheduling after reply failed (non-blocking): %s", e)

        try:
            from app.workflows.reactivation_scheduler import reschedule_reactivation
            await reschedule_reactivation(
                ctx.entity_id,
                ctx.contact_id,
            )
        except Exception as e:
            logger.warning("Reactivation reschedule after reply failed (non-blocking): %s", e)

    # Attach post-processing details for artifact visibility
    response["_post_processing"] = post_results

    logger.info(
        "Reply pipeline complete: %d messages, agent_type=%s, test_mode=%s",
        len(ctx.messages),
        ctx.agent_type,
        ctx.is_test_mode,
    )

    # Log full webhook response for debugging visibility
    logger.info("WEBHOOK RESPONSE | %s", json.dumps(response, default=str))

    return response


async def followup_pipeline(ctx: PipelineContext) -> dict[str, Any]:
    """Follow-up pipeline — Phase 5.

    Reuses Steps 3.2-3.6 (data loading, sync, timeline, booking),
    then runs follow-up-specific gates + classifier + generator.
    """
    # Log follow-up trigger
    logger.info(
        "INBOUND | contact=%s | name=%s | channel=%s | trigger=followup",
        ctx.contact_id,
        ctx.contact_name or "(unknown)",
        ctx.channel,
    )

    # Reuse shared data loading steps
    await load_data(ctx)
    _cache_derived_values(ctx)
    if ctx.is_test_mode:
        logger.info("FOLLOWUP_PIPELINE | test_mode — skipping sync (timeline=%d chars)", len(ctx.timeline or ""))
    else:
        await sync_conversation(ctx)
        await build_timeline(ctx, is_followup=True)
    await load_booking_context(ctx)

    # Follow-up specific logic (gates + classifier + generator)
    result = await run_followup(ctx)

    logger.info(
        "Follow-up pipeline complete: needed=%s",
        result.get("followUpNeeded", False),
    )

    # Log full webhook response for debugging visibility
    logger.info("WEBHOOK RESPONSE | %s", json.dumps(result, default=str))
    return result


def _detect_inbound_from_sync(ctx: PipelineContext) -> None:
    """Detect the inbound message text from conversation sync.

    Called when ctx.message is empty (standard webhook flow — no payload).
    Finds all human messages since the last AI response and concatenates them
    as the inbound message. This replaces the GHL consolidation payload.

    Channel is NOT set here — it comes from the webhook's Channel custom field.
    """
    if not ctx.chat_history:
        logger.warning("DETECT_INBOUND | no chat history — cannot detect message")
        return

    # Chat history is newest-first. Find all human messages before the first AI message.
    inbound_messages = []
    for row in ctx.chat_history:
        role = row.get("role", "")
        if role == "ai":
            break  # Stop at the most recent AI response
        if role == "human":
            content = (row.get("content") or "").strip()
            if content:
                inbound_messages.append(content)

    if not inbound_messages:
        logger.warning("DETECT_INBOUND | no new human messages found since last AI response")
        return

    # Reverse to chronological order (chat_history is newest-first)
    inbound_messages.reverse()

    # Join multiple messages (replaces GHL's ||| consolidation)
    ctx.message = " ".join(inbound_messages)
    ctx.raw_payload = ctx.message

    logger.info(
        "DETECT_INBOUND | found %d messages | combined=%d chars | preview=%s",
        len(inbound_messages), len(ctx.message), ctx.message[:100],
    )


async def _ensure_inbound_stored(ctx: PipelineContext) -> None:
    """Ensure the current inbound message is in chat history.

    After sync_conversation, the inbound message should already be stored
    as individual rows (each with a ghl_message_id). This is a safety net
    for the rare case where GHL hasn't indexed the message yet when we
    call their Conversations API.

    Checks if any recent human message has a ghl_message_id (proof that
    conversation sync is working). Only inserts a fallback if sync stored
    zero human messages with IDs — meaning GHL API likely hadn't indexed
    the message yet.
    """
    if not ctx.message:
        return

    chat_table = ctx.config.get("chat_history_table_name", "")
    if not chat_table:
        return

    try:
        recent = await postgres.get_chat_history(chat_table, ctx.contact_id, limit=5)

        for row in recent:
            if row.get("role") == "human":
                if row.get("ghl_message_id"):
                    return  # Sync stored a human message with ID — current message is there

        # No human messages with ghl_message_id found — sync missed the inbound
        lead_id = ctx.lead.get("id") if ctx.lead else None
        message = {
            "type": "human",
            "content": ctx.message,
            "additional_kwargs": {
                "source": "lead_reply",
                "channel": ctx.channel,
            },
            "response_metadata": {},
        }
        await postgres.insert_message(
            table=chat_table,
            session_id=ctx.contact_id,
            message=message,
            lead_id=lead_id,
        )
        logger.info("Inbound message safety net: stored explicitly (GHL sync missed it)")

    except Exception as e:
        logger.warning("Inbound message safety net failed: %s", e)


@task(name="embed_attachment_ids", retries=2, retry_delay_seconds=2, timeout_seconds=15)
async def _embed_attachment_ids(ctx: PipelineContext) -> None:
    """Link attachment records to chat history rows by ghl_message_id.

    Writes attachment UUIDs to the attachment_ids column.
    """
    chat_table = ctx.config.get("chat_history_table_name", "")
    if not chat_table or not ctx.attachments:
        return

    # Group attachment UUIDs by ghl_message_id
    ids_by_msg: dict[str, list[str]] = {}
    for att in ctx.attachments:
        ghl_id = att.get("ghl_message_id", "")
        if not ghl_id:
            continue
        att_id = str(att.get("id", ""))
        if att_id:
            ids_by_msg.setdefault(ghl_id, []).append(att_id)

    if not ids_by_msg:
        return

    for ghl_id, att_ids in ids_by_msg.items():
        await postgres.update_message_attachment_ids(
            chat_table, ctx.contact_id, ghl_id, att_ids
        )
    logger.info("Embedded attachment_ids for %d messages", len(ids_by_msg))
