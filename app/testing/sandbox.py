"""Sandbox — interactive conversation testing.

Each sandbox session is a sandbox_sessions row with JSONB messages.
No chat history writes, no pipeline_runs. Messages stored in JSONB.
Reuses all pipeline infrastructure from direct_runner.
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone, timedelta
from typing import Any
from uuid import uuid4

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.config import settings
from app.models import TokenUsage
from app.services.ai_client import set_ai_context, clear_ai_context, chat
from app.services.supabase_client import supabase
from app.text_engine.pipeline import run_pipeline_with_tracking

from app.testing.mock_ghl import MockGHLClient
from app.testing.models import (
    MockGHLConfig,
    LeadConfig,
    TestContext,
)
from app.testing.direct_runner import (
    _build_variable_map,
    _build_context,
    _seed_drip_messages,
    _generate_free_slots,
    _backdate_mock_appointments,
    _extract_scheduled_message_from_contact,
    _inject_smart_followup_timer,
    _resolve_setter_cadence,
)
from app.testing.sandbox_state import (
    apply_preconditions_to_mock_config,
    backdate_session_state,
    build_mock_contact,
    build_sandbox_timeline,
    clear_preconditions as clear_sandbox_preconditions,
    delete_precondition_item as delete_sandbox_precondition_item,
    ensure_sandbox_preconditions,
    get_appointments_for_display,
    get_contact_state,
    get_sandbox_lead,
    merge_session_messages,
    serialize_session,
    snapshot_runtime_state,
)

logger = logging.getLogger(__name__)

sandbox_router = APIRouter(prefix="/testing/sandbox", tags=["sandbox"])

# In-memory store for active sandbox sessions' MockGHLClients.
_active_sessions: dict[str, dict[str, Any]] = {}


# ============================================================================
# Request models
# ============================================================================

class CreateSessionRequest(BaseModel):
    entity_id: str
    label: str = ""
    channel: str = "SMS"
    agent_type: str = "setter_1"
    lead: LeadConfig = LeadConfig()


class SendMessageRequest(BaseModel):
    session_id: str
    message: str


class PreconditionsRequest(BaseModel):
    session_id: str
    context: TestContext


class FollowupRequest(BaseModel):
    session_id: str
    hours_back: float | None = None


class RevertRequest(BaseModel):
    session_id: str
    revert_to_timestamp: str


class GenerateCallLogRequest(BaseModel):
    entity_id: str
    description: str
    direction: str = "inbound"


class ClearPreconditionsRequest(BaseModel):
    session_id: str


class DeletePreconditionItemRequest(BaseModel):
    session_id: str
    item_type: str  # "call_log", "appointment", "message"
    item_id: str


# ============================================================================
# Helpers
# ============================================================================

async def _resolve_tenant_keys(entity_id: str) -> dict[str, str]:
    """Resolve tenant API keys for the entity."""
    from app.main import _resolve_tenant_ai_keys
    return await _resolve_tenant_ai_keys(entity_id)


async def _load_session(session_id: str) -> dict[str, Any]:
    """Load a sandbox session from sandbox_sessions."""
    resp = await supabase._request(
        supabase.main_client, "GET", "/sandbox_sessions",
        params={"id": f"eq.{session_id}"},
        label="load_sandbox_session",
    )
    data = resp.json()
    if not data:
        raise HTTPException(status_code=404, detail="Sandbox session not found")
    return data[0]


async def _get_or_create_mock_ghl(
    session_id: str,
    session: dict[str, Any],
    entity_config: dict[str, Any],
) -> MockGHLClient:
    """Get or create the MockGHLClient for a sandbox session."""
    if session_id in _active_sessions:
        return _active_sessions[session_id]["mock_ghl"]

    contact_id = session.get("contact_id", str(uuid4()))
    preconditions = ensure_sandbox_preconditions(session.get("preconditions"))
    lead = get_sandbox_lead(preconditions, contact_id)
    fake_contact = build_mock_contact(contact_id, preconditions)

    mock_config = MockGHLConfig(
        contact=fake_contact,
        timezone=entity_config.get("timezone", "America/Chicago"),
        free_slots=_generate_free_slots(entity_config.get("timezone", "America/Chicago")),
    )
    apply_preconditions_to_mock_config(mock_config, preconditions)
    mock_ghl = MockGHLClient(mock_config)

    _active_sessions[session_id] = {
        "mock_ghl": mock_ghl,
        "mock_config": mock_config,
        "lead": lead,
        "entity_config": entity_config,
        "contact_id": contact_id,
        "var_map": _build_variable_map(lead, entity_config),
        "preconditions": preconditions,
    }

    return mock_ghl


async def _append_messages(session_id: str, new_messages: list[dict[str, Any]], cost_delta: float = 0) -> None:
    """Append messages to a sandbox session's JSONB messages array."""
    # Fetch current messages
    resp = await supabase._request(
        supabase.main_client, "GET", "/sandbox_sessions",
        params={"id": f"eq.{session_id}", "select": "messages,total_cost,total_turns"},
        label="sandbox_get_messages",
    )
    data = resp.json()
    if not data:
        return
    session = data[0]
    messages = merge_session_messages(session.get("messages") or [], new_messages)

    update: dict[str, Any] = {"messages": messages}
    if cost_delta > 0:
        current_cost = float(session.get("total_cost") or 0)
        update["total_cost"] = round(current_cost + cost_delta, 6)

    # Count lead messages as turns
    lead_count = sum(
        1 for m in messages
        if m.get("role") == "human" and (m.get("source") or "").lower() not in {"seed", "drip", "outreach_drip"}
    )
    update["total_turns"] = lead_count

    await supabase._request(
        supabase.main_client, "PATCH", "/sandbox_sessions",
        params={"id": f"eq.{session_id}"},
        json=update,
        label="sandbox_append_messages",
    )


async def _update_session(
    session_id: str,
    *,
    messages: list[dict[str, Any]] | None = None,
    preconditions: dict[str, Any] | None = None,
    total_cost: float | None = None,
    analysis_results: dict[str, Any] | None = None,
) -> None:
    payload: dict[str, Any] = {}
    if messages is not None:
        payload["messages"] = messages
        payload["total_turns"] = sum(
            1 for msg in messages
            if msg.get("role") == "human" and (msg.get("source") or "").lower() not in {"seed", "drip", "outreach_drip"}
        )
    if preconditions is not None:
        payload["preconditions"] = preconditions
    if total_cost is not None:
        payload["total_cost"] = total_cost
    if analysis_results is not None:
        payload["analysis_results"] = analysis_results
    if not payload:
        return

    await supabase._request(
        supabase.main_client,
        "PATCH",
        "/sandbox_sessions",
        params={"id": f"eq.{session_id}"},
        json=payload,
        label="sandbox_update_session",
    )


def _normalize_appointment(appt: Any) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    raw_start = getattr(appt, "start", "") or ""
    if raw_start:
        try:
            start_dt = datetime.fromisoformat(raw_start.replace("Z", "+00:00"))
        except ValueError:
            start_dt = now + timedelta(days=2)
    else:
        start_dt = now + timedelta(days=2)
    if start_dt.tzinfo is None:
        start_dt = start_dt.replace(tzinfo=timezone.utc)

    end_dt = start_dt + timedelta(hours=1)
    return {
        "id": str(uuid4()),
        "calendar_id": getattr(appt, "calendar_id", "") or "",
        "calendar_name": getattr(appt, "calendar_name", "") or "Appointment",
        "status": getattr(appt, "status", "confirmed") or "confirmed",
        "start": start_dt.isoformat(),
        "end": end_dt.isoformat(),
        "timezone": getattr(appt, "timezone", "") or "",
    }


def _normalize_call_log(call_log: Any) -> dict[str, Any]:
    created_at = getattr(call_log, "created_at", "") or ""
    if created_at:
        try:
            created_dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
        except ValueError:
            created_dt = datetime.now(timezone.utc) - timedelta(hours=24)
    else:
        created_dt = datetime.now(timezone.utc) - timedelta(hours=24)
    if created_dt.tzinfo is None:
        created_dt = created_dt.replace(tzinfo=timezone.utc)

    return {
        "id": str(uuid4()),
        "direction": getattr(call_log, "direction", "inbound") or "inbound",
        "summary": getattr(call_log, "summary", "") or "",
        "status": getattr(call_log, "status", "answered") or "answered",
        "user_sentiment": getattr(call_log, "sentiment", "Neutral") or "Neutral",
        "duration_seconds": getattr(call_log, "duration_seconds", 120) or 120,
        "after_hours": bool(getattr(call_log, "after_hours", False)),
        "transcript": getattr(call_log, "transcript", "") or "",
        "created_at": created_dt.isoformat(),
    }


def _normalize_seed_entry(
    role: str,
    content: str,
    source: str,
    channel: str,
    timestamp: datetime,
) -> tuple[dict[str, Any], dict[str, Any]]:
    message_role = "human" if role == "lead" else "ai"
    msg_id = str(uuid4())
    precondition_entry = {
        "id": msg_id,
        "role": message_role,
        "content": content,
        "source": source,
        "channel": channel,
        "timestamp": timestamp.isoformat(),
    }
    return precondition_entry, dict(precondition_entry)


def _hydrate_active_session(
    session_id: str,
    entity_config: dict[str, Any],
    contact_id: str,
    preconditions: dict[str, Any],
) -> None:
    if session_id not in _active_sessions:
        return

    sess_data = _active_sessions[session_id]
    lead = get_sandbox_lead(preconditions, contact_id)
    sess_data["lead"] = lead
    sess_data["preconditions"] = preconditions
    sess_data["var_map"] = _build_variable_map(lead, entity_config)
    apply_preconditions_to_mock_config(sess_data["mock_config"], preconditions)
    sess_data["mock_config"].contact["firstName"] = lead.first_name
    sess_data["mock_config"].contact["lastName"] = lead.last_name
    sess_data["mock_config"].contact["email"] = lead.email
    sess_data["mock_config"].contact["phone"] = lead.phone


def _count_consecutive_sandbox_followups(messages: list[dict[str, Any]]) -> int:
    count = 0
    ordered = sorted(messages or [], key=lambda item: str(item.get("timestamp", "")), reverse=True)
    for message in ordered:
        role = message.get("role", "")
        if role == "ai":
            source = (message.get("source") or "").lower()
            if source in {"follow_up", "followup", "follow up"}:
                count += 1
        elif role == "human":
            break
    return count


def _resolve_sandbox_followup_preview(
    session: dict[str, Any],
    entity_config: dict[str, Any],
) -> dict[str, Any]:
    messages = session.get("messages") or []
    consecutive_followups = _count_consecutive_sandbox_followups(messages)
    cadence = _resolve_setter_cadence(
        entity_config,
        session.get("setter_key", "setter_1"),
        consecutive_followups + 1,
    )
    hours_back = cadence[consecutive_followups] if cadence else 6.0
    return {
        "consecutive_followups": consecutive_followups,
        "next_followup_number": consecutive_followups + 1,
        "hours_back": hours_back,
    }


# ============================================================================
# Endpoints
# ============================================================================

@sandbox_router.post("/sessions")
async def create_session(body: CreateSessionRequest) -> dict[str, Any]:
    """Create a new sandbox session."""
    try:
        config = await supabase.resolve_entity(body.entity_id)
    except ValueError:
        raise HTTPException(status_code=404, detail=f"Entity {body.entity_id} not found")

    entity_name = config.get("name") or config.get("bot_name") or "Unknown"
    contact_id = str(uuid4())

    # Resolve tenant
    tenant_id = config.get("tenant_id")
    if not tenant_id:
        entity_resp = await supabase._request(
            supabase.main_client, "GET", "/entities",
            params={"id": f"eq.{body.entity_id}", "select": "tenant_id"},
            label="sandbox_get_tenant",
        )
        ed = entity_resp.json()
        tenant_id = ed[0]["tenant_id"] if ed else None

    lead = body.lead or LeadConfig()
    full_name = f"{lead.first_name} {lead.last_name}".strip()
    preconditions = ensure_sandbox_preconditions({}, lead=lead)

    row = {
        "tenant_id": tenant_id,
        "entity_id": body.entity_id,
        "entity_name": entity_name,
        "setter_key": body.agent_type,
        "name": body.label or full_name or f"Sandbox {datetime.now(timezone.utc).strftime('%m/%d %H:%M')}",
        "contact_id": contact_id,
        "channel": body.channel,
        "messages": [],
        "preconditions": preconditions,
        "total_cost": 0,
        "total_turns": 0,
    }

    resp = await supabase._request(
        supabase.main_client, "POST", "/sandbox_sessions",
        json=row,
        headers={"Prefer": "return=representation"},
        label="create_sandbox_session",
    )
    if resp.status_code >= 400:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)

    data = resp.json()
    return serialize_session(data[0]) if data else {}


@sandbox_router.post("/message")
async def send_message(body: SendMessageRequest) -> dict[str, Any]:
    """Send a lead message through the pipeline and get AI response."""
    session = await _load_session(body.session_id)
    entity_id = session["entity_id"]
    contact_id = session.get("contact_id", "")
    channel = session.get("channel", "SMS")
    agent_type = session.get("setter_key", "setter_1")

    try:
        entity_config = await supabase.resolve_entity(entity_id)
    except ValueError:
        raise HTTPException(status_code=404, detail=f"Entity {entity_id} not found")

    preconditions = ensure_sandbox_preconditions(session.get("preconditions"))
    mock_ghl = await _get_or_create_mock_ghl(body.session_id, session, entity_config)
    sess_data = _active_sessions[body.session_id]
    lead = sess_data["lead"]
    contact_state = get_contact_state(preconditions)

    # Resolve tenant keys
    tenant_keys = await _resolve_tenant_keys(entity_id)

    ctx = _build_context(
        entity_config=entity_config,
        entity_id=entity_id,
        contact_id=contact_id,
        agent_type=agent_type,
        channel=channel,
        message=body.message,
        mock_ghl=mock_ghl,
        lead_config=lead,
    )
    ctx.timeline, ctx.chat_history = build_sandbox_timeline(
        session.get("messages") or [],
        preconditions,
        entity_config,
        current_message=body.message,
        channel=channel,
    )
    ctx.qualification_status = contact_state.get("qualification_status") or ""
    ctx.qualification_notes = contact_state.get("qualification_notes")
    scheduled_message = _extract_scheduled_message_from_contact(sess_data["mock_config"])
    if scheduled_message:
        ctx.scheduled_message = scheduled_message

    token_usage = TokenUsage()
    ctx.openrouter_api_key = tenant_keys.get("openrouter") or settings.openrouter_testing_key
    ctx.token_usage = token_usage
    set_ai_context(
        api_key=ctx.openrouter_api_key,
        token_tracker=ctx.token_usage,
        google_key=tenant_keys.get("google", ""),
        anthropic_key=tenant_keys.get("anthropic", ""),
        openai_key=tenant_keys.get("openai", ""),
        deepseek_key=tenant_keys.get("deepseek", ""),
        xai_key=tenant_keys.get("xai", ""),
    )

    start = time.perf_counter()
    try:
        result = await run_pipeline_with_tracking(ctx)
        elapsed = time.perf_counter() - start
    except Exception as e:
        logger.error("Sandbox pipeline error: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Pipeline error: {e}")
    finally:
        clear_ai_context()

    _inject_smart_followup_timer(sess_data["mock_config"], result, 0)

    # Collect AI messages
    ai_messages = []
    for i in range(1, 4):
        for key in (f"Message_{i}", f"Message{i}"):
            msg = result.get(key)
            if msg:
                ai_messages.append(msg)
                break
    ai_text = " | ".join(ai_messages) if ai_messages else None
    path = result.get("path", "reply" if ai_messages else "unknown")

    # Build messages for JSONB
    now = datetime.now(timezone.utc)
    new_messages = []

    # Lead message
    new_messages.append({
        "id": str(uuid4()),
        "role": "human",
        "content": body.message,
        "source": "lead_reply",
        "channel": channel,
        "timestamp": now.isoformat(),
    })

    # AI response messages
    for i, msg_text in enumerate(ai_messages):
        msg_data: dict[str, Any] = {
            "id": str(uuid4()),
            "role": "ai",
            "content": msg_text,
            "source": "AI",
            "channel": channel,
            "timestamp": (now + timedelta(seconds=2 + i)).isoformat(),
        }
        if getattr(ctx, "workflow_run_id", None):
            msg_data["workflow_run_id"] = ctx.workflow_run_id
        # Media on first message
        if i == 0 and ctx.selected_media_url:
            msg_data["media"] = {
                "url": ctx.selected_media_url,
                "type": ctx.selected_media_type or "image",
                "name": ctx.selected_media_name or "",
            }
        new_messages.append(msg_data)

    # Append to session
    cost = token_usage.total_cost() if hasattr(token_usage, "total_cost") else 0
    await _append_messages(body.session_id, new_messages, cost_delta=cost)

    preconditions["runtime_state"] = snapshot_runtime_state(
        sess_data["mock_config"],
        preconditions,
        ctx.qualification_status,
        ctx.qualification_notes,
    )
    await _update_session(body.session_id, preconditions=preconditions)
    _hydrate_active_session(body.session_id, entity_config, contact_id, preconditions)

    # Build structured decisions for response
    tools = [{"name": t.get("name", ""), "args": t.get("args"), "result": t.get("result")} for t in (ctx.tool_calls_log or [])]
    variables = [{"label": p.get("label", ""), "variables": p.get("variables")} for p in (ctx.prompt_log or [])]
    post_proc = result.get("_post_processing", {})
    decisions = {
        "classification": {"path": ctx.response_path, "reason": ctx.response_reason, "gates": ctx.classification_gates},
        "extraction": post_proc.get("extraction", {}),
        "qualification": {"status": ctx.qualification_status, "notes": ctx.qualification_notes},
    }

    return {
        "ai_response": ai_text,
        "path": path,
        "duration_seconds": round(elapsed, 1),
        "token_usage": token_usage.summary(),
        "decisions": decisions,
        "tools": tools,
        "variables": variables,
    }


@sandbox_router.post("/preconditions")
async def setup_preconditions(body: PreconditionsRequest) -> dict[str, Any]:
    """Set up world state before chatting."""
    session = await _load_session(body.session_id)
    entity_id = session["entity_id"]
    contact_id = session.get("contact_id", "")

    try:
        entity_config = await supabase.resolve_entity(entity_id)
    except ValueError:
        raise HTTPException(status_code=404, detail=f"Entity {entity_id} not found")

    mock_ghl = await _get_or_create_mock_ghl(body.session_id, session, entity_config)
    sess_data = _active_sessions[body.session_id]
    lead = sess_data["lead"]
    var_map = sess_data["var_map"]
    preconditions = ensure_sandbox_preconditions(session.get("preconditions"), lead=lead)
    ctx_config = body.context
    results: list[str] = []
    new_messages: list[dict[str, Any]] = []
    now = datetime.now(timezone.utc)

    if ctx_config.tags or ctx_config.custom_fields:
        preconditions["tags"] = list(dict.fromkeys([*preconditions["tags"], *(ctx_config.tags or [])]))
        preconditions["custom_fields"].update(ctx_config.custom_fields or {})
        results.append(f"Applied {len(ctx_config.tags)} tags, {len(ctx_config.custom_fields)} custom fields")

    if ctx_config.appointments:
        normalized_appointments = [_normalize_appointment(appt) for appt in ctx_config.appointments]
        preconditions["appointments"].extend(normalized_appointments)
        results.append(f"Created {len(ctx_config.appointments)} appointments")

    if ctx_config.call_logs:
        normalized_call_logs = [_normalize_call_log(call_log) for call_log in ctx_config.call_logs]
        preconditions["call_logs"].extend(normalized_call_logs)
        results.append(f"Created {len(ctx_config.call_logs)} call logs")

    if ctx_config.qualification_status or ctx_config.qualification_notes:
        if ctx_config.qualification_status:
            preconditions["qualification_status"] = ctx_config.qualification_status
        if ctx_config.qualification_notes is not None:
            preconditions["qualification_notes"] = ctx_config.qualification_notes
        results.append("Updated contact state")

    if ctx_config.drip_messages:
        entries = await _seed_drip_messages(entity_config, entity_id, contact_id, ctx_config.drip_messages, lead, var_map)
        for entry in entries:
            raw_timestamp = entry.get("timestamp")
            timestamp = datetime.fromisoformat(str(raw_timestamp).replace("Z", "+00:00")) if raw_timestamp else now - timedelta(hours=float(entry.get("hours_ago", 0) or 0))
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=timezone.utc)
            pre_entry, message_entry = _normalize_seed_entry(
                role="ai",
                content=entry.get("content", ""),
                source="drip",
                channel=session.get("channel", "SMS"),
                timestamp=timestamp,
            )
            preconditions["drip_messages"].append(pre_entry)
            new_messages.append(message_entry)
        results.append(f"Seeded {len(entries)} drip messages")

    if ctx_config.seed_messages:
        sorted_seed_messages = sorted(
            ctx_config.seed_messages,
            key=lambda item: (
                datetime.fromisoformat(str(item.timestamp).replace("Z", "+00:00")).timestamp()
                if item.timestamp else
                (now - timedelta(hours=float(item.hours_ago or 0))).timestamp()
            ),
        )
        for seed in sorted_seed_messages:
            content = seed.content.strip()
            if not content:
                continue
            if seed.timestamp:
                timestamp = datetime.fromisoformat(str(seed.timestamp).replace("Z", "+00:00"))
            else:
                timestamp = now - timedelta(hours=float(seed.hours_ago or 0))
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=timezone.utc)
            source = seed.source or "seed"
            pre_entry, message_entry = _normalize_seed_entry(
                role=seed.role,
                content=content,
                source=source,
                channel=seed.channel or session.get("channel", "SMS"),
                timestamp=timestamp,
            )
            preconditions["seed_messages"].append(pre_entry)
            new_messages.append(message_entry)
        results.append(f"Seeded {len(ctx_config.seed_messages)} messages")

    preconditions["runtime_state"] = None
    merged_messages = merge_session_messages(session.get("messages") or [], new_messages)
    await _update_session(body.session_id, messages=merged_messages, preconditions=preconditions)
    _hydrate_active_session(body.session_id, entity_config, contact_id, preconditions)

    return {"status": "ok", "results": results}


@sandbox_router.post("/followup")
async def trigger_followup(body: FollowupRequest) -> dict[str, Any]:
    """Trigger a follow-up by simulating time passing."""
    session = await _load_session(body.session_id)
    entity_id = session["entity_id"]
    contact_id = session.get("contact_id", "")
    channel = session.get("channel", "SMS")
    agent_type = session.get("setter_key", "setter_1")

    try:
        entity_config = await supabase.resolve_entity(entity_id)
    except ValueError:
        raise HTTPException(status_code=404, detail=f"Entity {entity_id} not found")

    followup_preview = _resolve_sandbox_followup_preview(session, entity_config)
    hours_back = body.hours_back if body.hours_back is not None else float(followup_preview["hours_back"])

    preconditions = ensure_sandbox_preconditions(session.get("preconditions"))
    aged_messages, aged_preconditions = backdate_session_state(
        session.get("messages") or [],
        preconditions,
        hours_back,
    )
    await _update_session(body.session_id, messages=aged_messages, preconditions=aged_preconditions)

    mock_ghl = await _get_or_create_mock_ghl(body.session_id, session, entity_config)
    sess_data = _active_sessions[body.session_id]
    apply_preconditions_to_mock_config(sess_data["mock_config"], aged_preconditions)
    _backdate_mock_appointments(sess_data["mock_config"], hours_back)
    tenant_keys = await _resolve_tenant_keys(entity_id)

    lead = _active_sessions.get(body.session_id, {}).get("lead", LeadConfig())
    contact_state = get_contact_state(aged_preconditions)

    ctx = _build_context(
        entity_config=entity_config,
        entity_id=entity_id,
        contact_id=contact_id,
        agent_type=agent_type,
        channel=channel,
        message="",
        mock_ghl=mock_ghl,
        lead_config=lead,
    )
    ctx.trigger_type = "followup"
    ctx.timeline, ctx.chat_history = build_sandbox_timeline(
        aged_messages,
        aged_preconditions,
        entity_config,
        channel=channel,
        is_followup=True,
    )
    ctx.qualification_status = contact_state.get("qualification_status") or ""
    ctx.qualification_notes = contact_state.get("qualification_notes")
    scheduled_message = _extract_scheduled_message_from_contact(sess_data["mock_config"])
    if scheduled_message:
        ctx.scheduled_message = scheduled_message

    token_usage = TokenUsage()
    ctx.openrouter_api_key = tenant_keys.get("openrouter") or settings.openrouter_testing_key
    ctx.token_usage = token_usage
    set_ai_context(
        api_key=ctx.openrouter_api_key,
        token_tracker=ctx.token_usage,
        google_key=tenant_keys.get("google", ""),
        anthropic_key=tenant_keys.get("anthropic", ""),
        openai_key=tenant_keys.get("openai", ""),
        deepseek_key=tenant_keys.get("deepseek", ""),
        xai_key=tenant_keys.get("xai", ""),
    )

    start = time.perf_counter()
    try:
        result = await run_pipeline_with_tracking(ctx)
        elapsed = time.perf_counter() - start
    except Exception as e:
        logger.error("Sandbox followup error: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Follow-up error: {e}")
    finally:
        clear_ai_context()

    fu_needed = result.get("followUpNeeded", False)
    fu_message = result.get("suggestedFollowUp") or result.get("followup_message")
    fu_path = result.get("path", "followup" if fu_needed else "skipped")

    cost = token_usage.total_cost() if hasattr(token_usage, "total_cost") else 0

    # Append follow-up message to JSONB
    if fu_message and fu_needed:
        now = datetime.now(timezone.utc)
        msg_data: dict[str, Any] = {
            "id": str(uuid4()),
            "role": "ai",
            "content": fu_message,
            "source": "follow_up",
            "channel": channel,
            "timestamp": now.isoformat(),
        }
        if getattr(ctx, "workflow_run_id", None):
            msg_data["workflow_run_id"] = ctx.workflow_run_id
        if result.get("media_url"):
            msg_data["media"] = {
                "url": result["media_url"],
                "type": result.get("media_type", ""),
                "name": result.get("media_name", ""),
            }
        await _append_messages(body.session_id, [msg_data], cost_delta=cost)
    elif cost > 0:
        current_cost = float(session.get("total_cost") or 0)
        await _update_session(body.session_id, total_cost=round(current_cost + cost, 6))

    aged_preconditions["runtime_state"] = snapshot_runtime_state(
        sess_data["mock_config"],
        aged_preconditions,
        ctx.qualification_status,
        ctx.qualification_notes,
    )
    await _update_session(body.session_id, preconditions=aged_preconditions)
    _hydrate_active_session(body.session_id, entity_config, contact_id, aged_preconditions)

    return {
        "followup_message": fu_message,
        "path": fu_path,
        "followup_needed": fu_needed,
        "reason": result.get("reason", ""),
        "hours_back_used": hours_back,
        "followup_number": followup_preview["next_followup_number"],
        "duration_seconds": round(elapsed, 1),
        "token_usage": token_usage.summary(),
        "media_url": result.get("media_url", ""),
        "media_type": result.get("media_type", ""),
        "media_name": result.get("media_name", ""),
    }


@sandbox_router.post("/delete-precondition-item")
async def delete_precondition_item(body: DeletePreconditionItemRequest) -> dict[str, Any]:
    """Delete a single precondition item."""
    session = await _load_session(body.session_id)
    contact_id = session.get("contact_id", "")
    entity_id = session["entity_id"]

    if body.item_type == "call_log":
        preconditions, deleted = delete_sandbox_precondition_item(
            session.get("preconditions"),
            "call_log",
            body.item_id,
        )
        if not deleted:
            raise HTTPException(status_code=404, detail="Call log not found")
        await _update_session(body.session_id, preconditions=preconditions)
        if body.session_id in _active_sessions:
            _hydrate_active_session(body.session_id, _active_sessions[body.session_id]["entity_config"], contact_id, preconditions)
        return {"status": "deleted", "type": "call_log", "id": body.item_id}

    elif body.item_type == "appointment":
        preconditions, deleted = delete_sandbox_precondition_item(
            session.get("preconditions"),
            "appointment",
            body.item_id,
        )
        if not deleted:
            raise HTTPException(status_code=404, detail="Appointment not found")
        await _update_session(body.session_id, preconditions=preconditions)
        if body.session_id in _active_sessions:
            _hydrate_active_session(body.session_id, _active_sessions[body.session_id]["entity_config"], contact_id, preconditions)
        return {"status": "deleted", "type": "appointment", "id": body.item_id}

    elif body.item_type == "message":
        # Remove message from JSONB by filtering it out
        messages = session.get("messages") or []
        updated = [m for m in messages if m.get("id") != body.item_id]
        await _update_session(body.session_id, messages=updated)
        return {"status": "deleted", "type": "message", "id": body.item_id}

    raise HTTPException(status_code=400, detail=f"Unknown item type: {body.item_type}")


@sandbox_router.post("/clear-preconditions")
async def clear_preconditions(body: ClearPreconditionsRequest) -> dict[str, Any]:
    """Clear all seeded preconditions and messages."""
    session = await _load_session(body.session_id)
    contact_id = session.get("contact_id", "")
    entity_id = session["entity_id"]

    results = []
    cleared_preconditions = clear_sandbox_preconditions(session.get("preconditions"))
    await supabase._request(
        supabase.main_client,
        "PATCH",
        "/sandbox_sessions",
        params={"id": f"eq.{body.session_id}"},
        json={
            "messages": [],
            "preconditions": cleared_preconditions,
            "total_turns": 0,
            "total_cost": 0,
            "analysis_results": None,
        },
        label="sandbox_clear_messages",
    )
    results.append("Cleared messages")

    try:
        await supabase._request(
            supabase.main_client,
            "DELETE",
            "/tool_executions",
            params={
                "ghl_contact_id": f"eq.{contact_id}",
                "is_test": "eq.true",
            },
            label="sandbox_clear_tool_executions",
        )
        results.append("Cleared tool executions")
    except Exception as e:
        logger.warning("Failed to clear sandbox tool executions: %s", e)

    # Reset in-memory mock GHL state
    _active_sessions.pop(body.session_id, None)
    results.append("Reset mock state")

    return {"status": "cleared", "results": results}


@sandbox_router.post("/revert")
async def revert(body: RevertRequest) -> dict[str, Any]:
    """Revert a sandbox session to a previous point in time."""
    session = await _load_session(body.session_id)

    # Truncate messages JSONB — keep only messages before the revert timestamp
    messages = session.get("messages") or []
    cutoff = datetime.fromisoformat(body.revert_to_timestamp.replace("Z", "+00:00"))
    reverted = [
        m for m in messages
        if datetime.fromisoformat(m["timestamp"].replace("Z", "+00:00")) <= cutoff
    ]

    lead_count = sum(
        1 for m in reverted
        if m.get("role") == "human" and (m.get("source") or "").lower() not in {"seed", "drip", "outreach_drip"}
    )
    preconditions = ensure_sandbox_preconditions(session.get("preconditions"))
    preconditions["runtime_state"] = None

    await supabase._request(
        supabase.main_client,
        "PATCH",
        "/sandbox_sessions",
        params={"id": f"eq.{body.session_id}"},
        json={
            "messages": reverted,
            "preconditions": preconditions,
            "total_turns": lead_count,
            "analysis_results": None,
        },
        label="sandbox_revert_messages",
    )

    try:
        await supabase._request(
            supabase.main_client,
            "DELETE",
            "/tool_executions",
            params={
                "ghl_contact_id": f"eq.{session.get('contact_id', '')}",
                "is_test": "eq.true",
                "created_at": f"gt.{cutoff.isoformat()}",
            },
            label="sandbox_revert_tool_executions",
        )
    except Exception as e:
        logger.warning("Failed to delete reverted sandbox tool executions: %s", e)

    # Clear in-memory mock GHL state (will be rebuilt on next message)
    _active_sessions.pop(body.session_id, None)

    return {"status": "reverted", "revert_to": body.revert_to_timestamp, "messages_kept": len(reverted)}


@sandbox_router.post("/generate-call-log")
async def generate_call_log(body: GenerateCallLogRequest) -> dict[str, Any]:
    """AI-generate a realistic call transcript from a description."""
    try:
        entity_config = await supabase.resolve_entity(body.entity_id)
    except ValueError:
        raise HTTPException(status_code=404, detail=f"Entity {body.entity_id} not found")

    business_name = entity_config.get("name") or "the business"

    system_prompt = (
        "You generate realistic phone call transcripts for testing purposes. "
        "The business is a service provider. Generate a natural conversation between "
        "a customer and a staff member."
    )

    user_prompt = (
        f"Business: {business_name}\n"
        f"Call direction: {body.direction}\n"
        f"Scenario: {body.description}\n\n"
        f"Generate a realistic call transcript with 'Staff:' and 'Customer:' labels, "
        f"a brief summary, estimated duration in seconds, and customer sentiment."
    )

    schema = {
        "type": "object",
        "properties": {
            "transcript": {"type": "string"},
            "summary": {"type": "string"},
            "duration_seconds": {"type": "integer"},
            "sentiment": {"type": "string", "enum": ["Positive", "Neutral", "Negative"]},
        },
        "required": ["transcript", "summary", "duration_seconds", "sentiment"],
    }

    try:
        resp = await chat(
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            model="google/gemini-2.5-flash",
            temperature=0.7,
            label="call_simulator",
            response_format={
                "type": "json_schema",
                "json_schema": {"name": "call_log", "strict": True, "schema": schema},
            },
        )
        content = resp.choices[0].message.content or "{}"
        result = json.loads(content)
        return result
    except Exception as e:
        logger.error("Call log generation failed: %s", e)
        raise HTTPException(status_code=500, detail=f"Generation failed: {e}")


@sandbox_router.get("/sessions")
async def list_sessions(
    tenant_id: str | None = None,
    entity_id: str | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    """List sandbox sessions."""
    params: dict[str, str] = {
        "select": "id,entity_id,entity_name,setter_key,channel,created_at,"
                  "total_cost,total_turns,contact_id,name,preconditions",
        "order": "created_at.desc",
        "limit": str(limit),
    }
    if tenant_id:
        params["tenant_id"] = f"eq.{tenant_id}"
    if entity_id:
        params["entity_id"] = f"eq.{entity_id}"

    resp = await supabase._request(
        supabase.main_client, "GET", "/sandbox_sessions",
        params=params,
        label="list_sandbox_sessions",
    )
    return [serialize_session(item) for item in resp.json()]


@sandbox_router.get("/sessions/{session_id}")
async def get_session(session_id: str) -> dict[str, Any]:
    """Get a sandbox session with its messages."""
    session = await _load_session(session_id)
    try:
        entity_config = await supabase.resolve_entity(session["entity_id"])
    except ValueError:
        entity_config = {}
    payload = serialize_session(session)
    payload["followup_preview"] = _resolve_sandbox_followup_preview(session, entity_config) if entity_config else {
        "consecutive_followups": 0,
        "next_followup_number": 1,
        "hours_back": 6.0,
    }
    return payload


@sandbox_router.get("/sessions/{session_id}/chat-history")
async def get_chat_history(session_id: str) -> dict[str, Any]:
    """Get chat history, call logs, and appointments for a sandbox session."""
    session = await _load_session(session_id)
    preconditions = ensure_sandbox_preconditions(session.get("preconditions"))

    result: dict[str, Any] = {
        "messages": session.get("messages") or [],
        "call_logs": [],
        "appointments": [],
    }

    result["call_logs"] = list(preconditions.get("call_logs") or [])
    result["appointments"] = [
        {
            "id": appointment.get("id"),
            "ghl_calendar_name": appointment.get("calendar_name", "Appointment"),
            "status": appointment.get("status", "confirmed"),
            "appointment_datetime": appointment.get("start", ""),
            "appointment_timezone": appointment.get("timezone", ""),
            "created_at": appointment.get("start", ""),
        }
        for appointment in get_appointments_for_display(preconditions)
    ]

    return result


@sandbox_router.delete("/sessions/{session_id}")
async def delete_session(session_id: str) -> dict[str, str]:
    """Delete a sandbox session and clean up test data."""
    session = await _load_session(session_id)
    contact_id = session.get("contact_id", "")
    entity_id = session["entity_id"]

    try:
        await supabase._request(
            supabase.main_client,
            "DELETE",
            "/tool_executions",
            params={
                "ghl_contact_id": f"eq.{contact_id}",
                "is_test": "eq.true",
            },
            label="sandbox_delete_tool_executions",
        )
    except Exception as e:
        logger.warning("Sandbox tool execution cleanup failed: %s", e)

    # Delete the session row
    await supabase._request(
        supabase.main_client, "DELETE", "/sandbox_sessions",
        params={"id": f"eq.{session_id}"},
        label="delete_sandbox_session",
    )

    # Clean up in-memory state
    _active_sessions.pop(session_id, None)

    return {"status": "deleted", "id": session_id}


# ============================================================================
# Analyze — run compliance + deep analysis on current conversation
# ============================================================================

class AnalyzeRequest(BaseModel):
    session_id: str


@sandbox_router.post("/analyze")
async def analyze_session(req: AnalyzeRequest) -> dict[str, Any]:
    """Run compliance + deep analysis on the current sandbox conversation."""
    from app.testing.compliance import run_compliance
    from app.testing.analysis import run_analysis

    session = await _load_session(req.session_id)
    entity_id = session["entity_id"]
    setter_key = session.get("setter_key", "setter_1")
    messages = session.get("messages", [])

    if not messages:
        raise HTTPException(status_code=400, detail="No messages to analyze")

    # Count turns (lead messages)
    total_turns = sum(
        1 for m in messages
        if m.get("role") == "human" and (m.get("source") or "").lower() not in {"seed", "drip", "outreach_drip"}
    )

    # Load entity config
    entity_resp = await supabase._request(
        supabase.main_client, "GET", "/entities",
        params={"id": f"eq.{entity_id}", "select": "system_config"},
        label="sandbox_analyze_entity",
    )
    entity_data = entity_resp.json()
    system_config = entity_data[0].get("system_config", {}) if entity_data else {}

    # Resolve tenant keys
    tenant_keys = await _resolve_tenant_keys(entity_id)

    # Wrap messages into conversation_results format expected by compliance/analysis
    conversation_results = [{
        "id": req.session_id,
        "messages": messages,
        "scenario_type": "sandbox",
        "outcome": "completed",
    }]

    # Run compliance + analysis in parallel
    import asyncio
    compliance_task = run_compliance(
        simulation_id=req.session_id,
        conversation_results=conversation_results,
        system_config=system_config,
        setter_key=setter_key,
        tenant_keys=tenant_keys,
    )
    analysis_task = run_analysis(
        simulation_id=req.session_id,
        conversation_results=conversation_results,
        system_config=system_config,
        setter_key=setter_key,
        reviews={},
        tenant_keys=tenant_keys,
        entity_id=entity_id,
    )

    compliance_results, analysis_results = await asyncio.gather(
        compliance_task, analysis_task, return_exceptions=True
    )

    # Handle exceptions gracefully
    if isinstance(compliance_results, Exception):
        logger.error("Compliance failed: %s", compliance_results)
        compliance_results = {"per_conversation": {}, "summary": {"total_passed": 0, "total_warned": 0, "total_failed": 0}}
    if isinstance(analysis_results, Exception):
        logger.error("Analysis failed: %s", analysis_results)
        analysis_results = {"per_conversation": {}}

    result = {
        "analyzed_at_turn": total_turns,
        "total_turns_at_analysis": total_turns,
        "compliance": compliance_results,
        "analysis": analysis_results,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    # Store in sandbox_sessions
    await supabase._request(
        supabase.main_client, "PATCH", "/sandbox_sessions",
        params={"id": f"eq.{req.session_id}"},
        json={"analysis_results": result},
        label="sandbox_store_analysis",
    )

    return result
