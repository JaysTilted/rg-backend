"""Ask AI approved action execution."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
import re
from typing import Any
from uuid import uuid4

from app.services.supabase_client import supabase


ALLOWED_SETTER_PREFIXES = (
    "bot_persona",
    "services",
    "offers_deployment",
    "case_studies",
    "prep_instructions_prompt",
    "case_study_deployment",
    "conversation",
    "booking",
    "transfer",
    "security",
    "ai_models",
    "ai_temperatures",
    "missed_call_textback",
    "auto_reactivation",
)

ALLOWED_CLIENT_PATHS = {
    "journey_stage",
    "timezone",
    "business_phone",
    "notes",
    "average_booking_value",
    "system_config.sms_provider",
    "system_config.pause_bot_on_human_activity",
    "system_config.human_takeover_minutes",
}

ALLOWED_CLIENT_PREFIXES = (
    "system_config.notifications.recipients",
)

ALLOWED_OUTREACH_TOP_LEVEL_PATHS = {
    "form_service_interest",
    "calendar_id",
    "calendar_name",
}

ALLOWED_OUTREACH_POSITION_FIELDS = {
    "delay",
    "delay_value",
    "delay_unit",
    "timing_direction",
    "sms",
    "sms_b",
    "email_subject",
    "email_body",
    "email_subject_b",
    "email_body_b",
    "media_url",
    "media_url_b",
    "media_type",
    "media_transcript",
    "media_transcript_b",
}


def _tokenize_path(path: str) -> list[str | int]:
    tokens: list[str | int] = []
    for part in [segment for segment in path.split(".") if segment]:
        for match in re.finditer(r"([^\[\]]+)|\[(\d+)\]", part):
            key, index = match.groups()
            if key is not None:
                tokens.append(key)
            elif index is not None:
                tokens.append(int(index))
    return tokens


def _deep_set(obj: dict[str, Any], path: str, value: Any) -> dict[str, Any]:
    current: Any = obj
    parts = _tokenize_path(path)
    if not parts:
        return obj
    for index, part in enumerate(parts[:-1]):
        next_part = parts[index + 1]
        if isinstance(part, str):
            if not isinstance(current, dict):
                raise RuntimeError("Invalid dict path segment")
            next_value = current.get(part)
            if isinstance(next_part, int):
                if not isinstance(next_value, list):
                    next_value = []
                    current[part] = next_value
            else:
                if not isinstance(next_value, dict):
                    next_value = {}
                    current[part] = next_value
            current = next_value
            continue

        if not isinstance(current, list):
            raise RuntimeError("Invalid list path segment")
        while len(current) <= part:
            current.append({} if not isinstance(next_part, int) else [])
        next_value = current[part]
        if isinstance(next_part, int):
            if not isinstance(next_value, list):
                next_value = []
                current[part] = next_value
        else:
            if not isinstance(next_value, dict):
                next_value = {}
                current[part] = next_value
        current = next_value

    final_part = parts[-1]
    if isinstance(final_part, str):
        if not isinstance(current, dict):
            raise RuntimeError("Invalid final dict path segment")
        current[final_part] = value
    else:
        if not isinstance(current, list):
            raise RuntimeError("Invalid final list path segment")
        while len(current) <= final_part:
            current.append(None)
        current[final_part] = value
    return obj


def _path_exists(obj: dict[str, Any], path: str) -> bool:
    current: Any = obj
    parts = _tokenize_path(path)
    for part in parts:
        if isinstance(part, str):
            if not isinstance(current, dict) or part not in current:
                return False
            current = current[part]
            continue
        if not isinstance(current, list) or part >= len(current):
            return False
        current = current[part]
    return True


def _default_setter_key(system_config: dict[str, Any]) -> str | None:
    setters = (system_config or {}).get("setters") or {}
    if not isinstance(setters, dict) or not setters:
        return None
    for key, value in setters.items():
        if isinstance(value, dict) and value.get("is_default"):
            return key
    return next(iter(setters.keys()), None)


def _is_allowed_setter_path(path: str) -> bool:
    return any(path == prefix or path.startswith(f"{prefix}.") for prefix in ALLOWED_SETTER_PREFIXES)


def _is_allowed_client_path(path: str) -> bool:
    return path in ALLOWED_CLIENT_PATHS or any(
        path == prefix
        or path.startswith(f"{prefix}.")
        or path.startswith(f"{prefix}[")
        for prefix in ALLOWED_CLIENT_PREFIXES
    )


def _is_allowed_outreach_path(path: str) -> bool:
    if path in ALLOWED_OUTREACH_TOP_LEVEL_PATHS:
        return True
    match = re.fullmatch(r"positions\[(\d+)\]\.([A-Za-z0-9_]+)", path)
    if not match:
        return False
    return match.group(2) in ALLOWED_OUTREACH_POSITION_FIELDS


async def create_action_request(
    *,
    session_id: str,
    tenant_id: str,
    entity_id: str,
    origin: str,
    action_payload: dict[str, Any],
) -> str:
    resp = await supabase._request(
        supabase.main_client,
        "POST",
        "/assistant_action_requests",
        json={
            "session_id": session_id,
            "tenant_id": tenant_id,
            "entity_id": entity_id,
            "origin": origin,
            "action_type": action_payload.get("action_type"),
            "action_payload": action_payload,
            "status": "proposed",
        },
        headers={"Prefer": "return=representation"},
        label="data_chat_create_action_request",
    )
    rows = resp.json() if isinstance(resp.json(), list) else []
    if not rows:
        raise RuntimeError("Failed to create action request")
    return rows[0]["id"]


async def _load_action_request(action_request_id: str) -> dict[str, Any] | None:
    resp = await supabase._request(
        supabase.main_client,
        "GET",
        "/assistant_action_requests",
        params={"id": f"eq.{action_request_id}", "select": "*"},
        label="data_chat_load_action_request",
    )
    rows = resp.json() if isinstance(resp.json(), list) else []
    return rows[0] if rows else None


async def _append_action_message(session_id: str, content: str) -> dict[str, Any]:
    now = datetime.now(timezone.utc).isoformat()
    resp = await supabase._request(
        supabase.main_client,
        "GET",
        "/ai_chat_sessions",
        params={"id": f"eq.{session_id}", "select": "messages,message_count,total_cost"},
        label="data_chat_load_session_for_action",
    )
    rows = resp.json() if isinstance(resp.json(), list) else []
    if not rows:
        raise RuntimeError("Chat session not found")

    session = rows[0]
    messages = list(session.get("messages") or [])
    assistant_msg = {
        "id": str(uuid4()),
        "role": "assistant",
        "content": content,
        "timestamp": now,
        "cost": 0,
        "tokens_in": 0,
        "tokens_out": 0,
        "provider": "system",
    }
    messages.append(assistant_msg)

    await supabase._request(
        supabase.main_client,
        "PATCH",
        "/ai_chat_sessions",
        params={"id": f"eq.{session_id}"},
        json={
            "messages": messages,
            "message_count": int(session.get("message_count") or 0) + 1,
            "updated_at": now,
        },
        label="data_chat_append_action_message",
    )
    return assistant_msg


async def _update_action_request_status(
    action_request_id: str,
    *,
    status: str,
    approved_by: str | None = None,
    result_summary: str | None = None,
    error_message: str | None = None,
) -> None:
    payload: dict[str, Any] = {"status": status}
    if approved_by:
        payload["approved_by"] = approved_by
        payload["approved_at"] = datetime.now(timezone.utc).isoformat()
    if status == "executed":
        payload["executed_at"] = datetime.now(timezone.utc).isoformat()
    if result_summary is not None:
        payload["result_summary"] = result_summary
    if error_message is not None:
        payload["error_message"] = error_message

    await supabase._request(
        supabase.main_client,
        "PATCH",
        "/assistant_action_requests",
        params={"id": f"eq.{action_request_id}"},
        json=payload,
        label="data_chat_update_action_request",
    )


async def _load_entity_for_action(entity_id: str) -> dict[str, Any]:
    resp = await supabase._request(
        supabase.main_client,
        "GET",
        "/entities",
        params={"id": f"eq.{entity_id}", "select": "*"},
        label="data_chat_load_entity_for_action",
    )
    rows = resp.json() if isinstance(resp.json(), list) else []
    if not rows:
        raise RuntimeError("Entity not found")
    return rows[0]


async def _execute_setter_action(entity: dict[str, Any], payload: dict[str, Any]) -> str:
    entity_id = entity["id"]
    system_config = deepcopy(entity.get("system_config") or {})
    setters = system_config.get("setters") or {}
    if not isinstance(setters, dict) or not setters:
        raise RuntimeError("Entity has no setters configured")

    setter_key = payload.get("setter_key") or _default_setter_key(system_config)
    if not setter_key or setter_key not in setters:
        raise RuntimeError("Setter not found")

    path = str(payload.get("path") or "").strip()
    if not _is_allowed_setter_path(path):
        raise RuntimeError("Setter path is not allowed")

    setter = deepcopy(setters.get(setter_key) or {})
    if not _path_exists(setter, path) and not path.startswith("ai_models.") and not path.startswith("ai_temperatures."):
        raise RuntimeError("Setter path does not exist on the current configuration")
    _deep_set(setter, path, payload.get("value"))
    setters[setter_key] = setter
    system_config["setters"] = setters

    await supabase._request(
        supabase.main_client,
        "PATCH",
        "/entities",
        params={"id": f"eq.{entity_id}"},
        json={"system_config": system_config},
        label="data_chat_execute_setter_action",
    )

    return payload.get("summary") or f"Updated {entity.get('name')}"


async def _execute_booking_length_action(entity: dict[str, Any], payload: dict[str, Any]) -> str:
    entity_id = entity["id"]
    system_config = deepcopy(entity.get("system_config") or {})
    setters = system_config.get("setters") or {}
    setter_key = payload.get("setter_key") or _default_setter_key(system_config)
    if not setter_key or setter_key not in setters:
        raise RuntimeError("Setter not found")

    setter = deepcopy(setters.get(setter_key) or {})
    booking = setter.get("booking") or {}
    calendars = list(booking.get("calendars") or [])
    if not calendars:
        raise RuntimeError("No calendars configured for this setter")

    target_calendar_id = payload.get("calendar_id")
    minutes = int(payload.get("value"))
    updated = False
    for calendar in calendars:
        if not isinstance(calendar, dict):
            continue
        cal_id = calendar.get("calendar_id") or calendar.get("id")
        if target_calendar_id and cal_id != target_calendar_id:
            continue
        calendar["appointment_length_minutes"] = minutes
        updated = True
        if target_calendar_id:
            break

    if not updated:
        raise RuntimeError("Target calendar not found")

    booking["calendars"] = calendars
    setter["booking"] = booking
    setters[setter_key] = setter
    system_config["setters"] = setters

    await supabase._request(
        supabase.main_client,
        "PATCH",
        "/entities",
        params={"id": f"eq.{entity_id}"},
        json={"system_config": system_config},
        label="data_chat_execute_booking_length_action",
    )

    return payload.get("summary") or f"Updated booking length for {entity.get('name')}"


async def _execute_bot_identity_rename(entity: dict[str, Any], payload: dict[str, Any]) -> str:
    entity_id = entity["id"]
    system_config = deepcopy(entity.get("system_config") or {})
    setters = system_config.get("setters") or {}
    setter_key = payload.get("setter_key") or _default_setter_key(system_config)
    if not setter_key or setter_key not in setters:
        raise RuntimeError("Setter not found")

    setter = deepcopy(setters.get(setter_key) or {})
    identity = (((setter.get("bot_persona") or {}).get("sections") or {}).get("identity") or {})
    if not isinstance(identity, dict):
        raise RuntimeError("Bot identity settings not found")

    old_name = str(payload.get("old_name") or identity.get("name") or "").strip()
    new_name = str(payload.get("new_name") or "").strip()
    if not new_name:
        raise RuntimeError("New bot name is required")

    identity["name"] = new_name
    for field in ("role", "extra_prompt"):
        value = identity.get(field)
        if isinstance(value, str) and old_name:
            identity[field] = value.replace(old_name, new_name)

    bot_persona = deepcopy(setter.get("bot_persona") or {})
    sections = deepcopy(bot_persona.get("sections") or {})
    sections["identity"] = identity
    bot_persona["sections"] = sections
    setter["bot_persona"] = bot_persona
    setters[setter_key] = setter
    system_config["setters"] = setters

    await supabase._request(
        supabase.main_client,
        "PATCH",
        "/entities",
        params={"id": f"eq.{entity_id}"},
        json={"system_config": system_config},
        label="data_chat_execute_bot_identity_rename",
    )

    return payload.get("summary") or f"Renamed the bot for {entity.get('name')}"


async def _execute_outreach_template_action(entity: dict[str, Any], payload: dict[str, Any]) -> str:
    template_id = str(payload.get("template_id") or "").strip()
    path = str(payload.get("path") or "").strip()
    if not template_id:
        raise RuntimeError("Outreach template is required")
    if not _is_allowed_outreach_path(path):
        raise RuntimeError("Outreach template path is not allowed")

    resp = await supabase._request(
        supabase.main_client,
        "GET",
        "/outreach_templates",
        params={"id": f"eq.{template_id}", "select": "*"},
        label="data_chat_load_outreach_template",
    )
    rows = resp.json() if isinstance(resp.json(), list) else []
    if not rows:
        raise RuntimeError("Outreach template not found")

    template = rows[0]
    if template.get("entity_id") != entity.get("id"):
        raise RuntimeError("Outreach template does not belong to this entity")

    updated_template = deepcopy(template)
    if not _path_exists(updated_template, path):
        raise RuntimeError("Outreach template path does not exist on the current configuration")
    _deep_set(updated_template, path, payload.get("value"))

    patch: dict[str, Any] = {}
    for key in ("form_service_interest", "calendar_id", "calendar_name", "positions"):
        if key in updated_template:
            patch[key] = updated_template[key]

    await supabase._request(
        supabase.main_client,
        "PATCH",
        "/outreach_templates",
        params={"id": f"eq.{template_id}"},
        json=patch,
        label="data_chat_execute_outreach_template_action",
    )

    return payload.get("summary") or f"Updated outreach template for {entity.get('name')}"


async def _execute_client_action(entity: dict[str, Any], payload: dict[str, Any]) -> str:
    entity_id = entity["id"]
    path = str(payload.get("path") or "").strip()
    if not _is_allowed_client_path(path):
        raise RuntimeError("Client settings path is not allowed")

    updates: dict[str, Any] = {}
    if path.startswith("system_config."):
        system_config = deepcopy(entity.get("system_config") or {})
        _deep_set(system_config, path.removeprefix("system_config."), payload.get("value"))
        updates["system_config"] = system_config
    else:
        updates[path] = payload.get("value")

    await supabase._request(
        supabase.main_client,
        "PATCH",
        "/entities",
        params={"id": f"eq.{entity_id}"},
        json=updates,
        label="data_chat_execute_client_action",
    )

    return payload.get("summary") or f"Updated {entity.get('name')}"


async def reject_action_request(action_request_id: str) -> dict[str, Any]:
    action_request = await _load_action_request(action_request_id)
    if not action_request:
        raise RuntimeError("Action request not found")
    session_id = action_request.get("session_id")
    await _update_action_request_status(action_request_id, status="rejected", result_summary="User canceled the action")
    message = await _append_action_message(session_id, "Okay, I did not change anything.")
    return {"session_id": session_id, "message": message}


async def execute_action_request(action_request_id: str, approved_by: str | None = None) -> dict[str, Any]:
    action_request = await _load_action_request(action_request_id)
    if not action_request:
        raise RuntimeError("Action request not found")
    if action_request.get("status") != "proposed":
        raise RuntimeError("Action request is no longer pending")

    payload = action_request.get("action_payload") or {}
    entity_id = action_request.get("entity_id")
    session_id = action_request.get("session_id")
    entity = await _load_entity_for_action(entity_id)

    try:
        action_type = payload.get("action_type")
        if action_type == "set_setter_value":
            result_summary = await _execute_setter_action(entity, payload)
        elif action_type == "set_client_value":
            result_summary = await _execute_client_action(entity, payload)
        elif action_type == "set_booking_appointment_length":
            result_summary = await _execute_booking_length_action(entity, payload)
        elif action_type == "rename_bot_identity":
            result_summary = await _execute_bot_identity_rename(entity, payload)
        elif action_type == "set_outreach_template_value":
            result_summary = await _execute_outreach_template_action(entity, payload)
        else:
            raise RuntimeError("Unsupported action type")

        await _update_action_request_status(
            action_request_id,
            status="executed",
            approved_by=approved_by,
            result_summary=result_summary,
        )
        message = await _append_action_message(session_id, f"{result_summary}\n\nThe requested change was applied.")
        return {"session_id": session_id, "message": message}
    except Exception as exc:
        await _update_action_request_status(
            action_request_id,
            status="failed",
            approved_by=approved_by,
            result_summary="Action failed",
            error_message=str(exc),
        )
        message = await _append_action_message(session_id, f"I could not apply that change.\n\nReason: {str(exc)}")
        return {"session_id": session_id, "message": message}
