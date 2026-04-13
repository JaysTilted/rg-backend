from __future__ import annotations

import json
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

from app.testing.models import LeadConfig, MockGHLConfig
from app.text_engine.timeline import format_timeline
from app.text_engine.utils import get_timezone


def ensure_sandbox_preconditions(
    raw: Any,
    lead: LeadConfig | dict[str, Any] | None = None,
) -> dict[str, Any]:
    data = deepcopy(raw) if isinstance(raw, dict) else {}
    lead_identity = data.get("lead_identity")
    if not isinstance(lead_identity, dict):
        if isinstance(lead, LeadConfig):
            lead_identity = lead.model_dump()
        elif isinstance(lead, dict):
            lead_identity = dict(lead)
        else:
            lead_identity = LeadConfig().model_dump()

    data["lead_identity"] = {
        "first_name": lead_identity.get("first_name", "Sandbox"),
        "last_name": lead_identity.get("last_name", "Lead"),
        "email": lead_identity.get("email", ""),
        "phone": lead_identity.get("phone", ""),
        "source": lead_identity.get("source", "Sandbox"),
    }
    data["seed_messages"] = list(data.get("seed_messages") or [])
    data["drip_messages"] = list(data.get("drip_messages") or [])
    data["call_logs"] = list(data.get("call_logs") or [])
    data["appointments"] = list(data.get("appointments") or [])
    data["tags"] = list(data.get("tags") or [])
    data["custom_fields"] = dict(data.get("custom_fields") or {})
    data["qualification_status"] = data.get("qualification_status") or ""
    data["qualification_notes"] = data.get("qualification_notes")
    runtime_state = data.get("runtime_state")
    data["runtime_state"] = deepcopy(runtime_state) if isinstance(runtime_state, dict) else None
    return data


def get_sandbox_lead(
    preconditions: dict[str, Any],
    contact_id: str,
) -> LeadConfig:
    data = ensure_sandbox_preconditions(preconditions)
    lead_data = data["lead_identity"]
    email = lead_data.get("email") or f"sandbox_{contact_id[:8]}@test.local"
    phone = lead_data.get("phone") or "+15550000000"
    return LeadConfig(
        first_name=lead_data.get("first_name", "Sandbox"),
        last_name=lead_data.get("last_name", "Lead"),
        email=email,
        phone=phone,
        source=lead_data.get("source", "Sandbox"),
    )


def build_mock_contact(
    contact_id: str,
    preconditions: dict[str, Any],
) -> dict[str, Any]:
    data = ensure_sandbox_preconditions(preconditions)
    lead = get_sandbox_lead(data, contact_id)
    state = get_contact_state(data)
    return {
        "id": contact_id,
        "firstName": lead.first_name,
        "lastName": lead.last_name,
        "email": lead.email,
        "phone": lead.phone,
        "tags": list(state["tags"]) or ["ai test mode"],
        "source": state.get("source") or lead.source or "Sandbox",
        "customFields": _custom_fields_to_list(state["custom_fields"]),
    }


def get_contact_state(preconditions: dict[str, Any]) -> dict[str, Any]:
    data = ensure_sandbox_preconditions(preconditions)
    runtime_state = data.get("runtime_state") or {}
    if runtime_state:
        return {
            "tags": list(runtime_state.get("tags") or []),
            "custom_fields": dict(runtime_state.get("custom_fields") or {}),
            "qualification_status": runtime_state.get("qualification_status") or "",
            "qualification_notes": runtime_state.get("qualification_notes"),
            "source": runtime_state.get("source") or data["lead_identity"].get("source", "Sandbox"),
        }

    return {
        "tags": list(data.get("tags") or []),
        "custom_fields": dict(data.get("custom_fields") or {}),
        "qualification_status": data.get("qualification_status") or "",
        "qualification_notes": data.get("qualification_notes"),
        "source": data["lead_identity"].get("source", "Sandbox"),
    }


def get_appointments_for_display(preconditions: dict[str, Any]) -> list[dict[str, Any]]:
    data = ensure_sandbox_preconditions(preconditions)
    runtime_state = data.get("runtime_state") or {}
    runtime_appts = runtime_state.get("appointments") or []
    return deepcopy(runtime_appts if runtime_appts else data.get("appointments") or [])


def apply_preconditions_to_mock_config(
    mock_config: MockGHLConfig,
    preconditions: dict[str, Any],
) -> None:
    data = ensure_sandbox_preconditions(preconditions)
    state = get_contact_state(data)

    tags = list(state["tags"]) or ["ai test mode"]
    if "ai test mode" not in [tag.lower() for tag in tags]:
        tags.append("ai test mode")
    mock_config.contact["tags"] = tags
    mock_config.contact["source"] = state.get("source") or mock_config.contact.get("source") or "Sandbox"
    mock_config.contact["customFields"] = _custom_fields_to_list(state["custom_fields"])

    mock_config.appointments = []
    for appointment in get_appointments_for_display(data):
        start = appointment.get("start")
        end = appointment.get("end") or _plus_one_hour(start)
        if not start:
            continue
        mock_config.appointments.append({
            "id": appointment.get("id") or str(uuid4()),
            "title": appointment.get("calendar_name") or appointment.get("title") or "Appointment",
            "startTime": start,
            "endTime": end,
            "appointmentStatus": appointment.get("status", "confirmed"),
            "calendarId": appointment.get("calendar_id", ""),
        })


def build_sandbox_timeline(
    session_messages: list[dict[str, Any]],
    preconditions: dict[str, Any],
    entity_config: dict[str, Any],
    current_message: str = "",
    channel: str = "SMS",
    is_followup: bool = False,
) -> tuple[str, list[dict[str, Any]]]:
    tz = get_timezone(entity_config)
    chat_rows: list[dict[str, Any]] = []

    for msg in session_messages:
        timestamp = _parse_iso(msg.get("timestamp"))
        if not timestamp:
            continue
        role = msg.get("role", "ai")
        if role not in ("human", "ai"):
            continue
        chat_rows.append({
            "role": role,
            "content": msg.get("content", ""),
            "source": msg.get("source", "AI"),
            "channel": msg.get("channel") or channel,
            "timestamp": timestamp,
            "ghl_message_id": None,
            "attachment_ids": [],
        })

    if current_message and not is_followup:
        chat_rows.append({
            "role": "human",
            "content": current_message,
            "source": "lead_reply",
            "channel": channel,
            "timestamp": datetime.now(timezone.utc),
            "ghl_message_id": None,
            "attachment_ids": [],
        })

    chat_rows.sort(key=lambda row: row["timestamp"], reverse=True)
    timeline, _stats = format_timeline(
        chat_rows,
        list(ensure_sandbox_preconditions(preconditions).get("call_logs") or []),
        [],
        tz,
        is_followup=is_followup,
    )
    return timeline, chat_rows


def snapshot_runtime_state(
    mock_config: MockGHLConfig,
    preconditions: dict[str, Any],
    qualification_status: str = "",
    qualification_notes: dict[str, Any] | None = None,
) -> dict[str, Any]:
    data = ensure_sandbox_preconditions(preconditions)
    contact = mock_config.contact or {}

    state = {
        "appointments": [],
        "tags": list(contact.get("tags") or []),
        "custom_fields": _custom_fields_to_dict(contact.get("customFields") or []),
        "qualification_status": qualification_status or data.get("qualification_status") or "",
        "qualification_notes": qualification_notes if qualification_notes is not None else data.get("qualification_notes"),
        "source": contact.get("source") or data["lead_identity"].get("source", "Sandbox"),
    }

    for appt in mock_config.appointments:
        state["appointments"].append({
            "id": appt.get("id") or str(uuid4()),
            "calendar_id": appt.get("calendarId", ""),
            "calendar_name": appt.get("title", "Appointment"),
            "status": appt.get("appointmentStatus", "confirmed"),
            "start": appt.get("startTime", ""),
            "end": appt.get("endTime", ""),
            "timezone": appt.get("timezone", ""),
        })

    return state


def merge_session_messages(
    existing: list[dict[str, Any]],
    new_messages: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    seen_ids: set[str] = set()

    for raw in [*(existing or []), *(new_messages or [])]:
        msg = dict(raw)
        msg["id"] = msg.get("id") or str(uuid4())
        if msg["id"] in seen_ids:
            continue
        seen_ids.add(msg["id"])
        merged.append(msg)

    merged.sort(key=lambda msg: str(msg.get("timestamp", "")))
    return merged


def clear_preconditions(
    preconditions: dict[str, Any],
) -> dict[str, Any]:
    data = ensure_sandbox_preconditions(preconditions)
    return {
        **data,
        "seed_messages": [],
        "drip_messages": [],
        "call_logs": [],
        "appointments": [],
        "tags": [],
        "custom_fields": {},
        "qualification_status": "",
        "qualification_notes": None,
        "runtime_state": None,
    }


def delete_precondition_item(
    preconditions: dict[str, Any],
    item_type: str,
    item_id: str,
) -> tuple[dict[str, Any], bool]:
    data = ensure_sandbox_preconditions(preconditions)
    deleted = False

    if item_type == "call_log":
        before = len(data["call_logs"])
        data["call_logs"] = [item for item in data["call_logs"] if item.get("id") != item_id]
        deleted = len(data["call_logs"]) != before
    elif item_type == "appointment":
        before = len(data["appointments"])
        data["appointments"] = [item for item in data["appointments"] if item.get("id") != item_id]
        deleted = len(data["appointments"]) != before
        runtime_state = data.get("runtime_state") or {}
        if runtime_state.get("appointments"):
            runtime_state["appointments"] = [
                item for item in runtime_state["appointments"]
                if item.get("id") != item_id
            ]
            data["runtime_state"] = runtime_state

    return data, deleted


def backdate_session_state(
    session_messages: list[dict[str, Any]],
    preconditions: dict[str, Any],
    hours_back: float,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if hours_back <= 0:
        return session_messages, ensure_sandbox_preconditions(preconditions)

    updated_messages = [dict(msg) for msg in session_messages]
    for msg in updated_messages:
        msg["timestamp"] = _shift_iso(msg.get("timestamp"), hours_back)

    data = ensure_sandbox_preconditions(preconditions)
    for key in ("seed_messages", "drip_messages"):
        shifted = []
        for item in data.get(key, []):
            entry = dict(item)
            entry["timestamp"] = _shift_iso(entry.get("timestamp"), hours_back)
            shifted.append(entry)
        data[key] = shifted

    shifted_logs = []
    for call_log in data.get("call_logs", []):
        entry = dict(call_log)
        entry["created_at"] = _shift_iso(entry.get("created_at"), hours_back)
        shifted_logs.append(entry)
    data["call_logs"] = shifted_logs

    data["appointments"] = _shift_appointments(data.get("appointments", []), hours_back)

    runtime_state = data.get("runtime_state") or None
    if runtime_state:
        runtime_state["appointments"] = _shift_appointments(runtime_state.get("appointments", []), hours_back)
        custom_fields = dict(runtime_state.get("custom_fields") or {})
        if "smartfollowup_timer" in custom_fields:
            custom_fields["smartfollowup_timer"] = _shift_timer_payload(custom_fields["smartfollowup_timer"], hours_back)
        runtime_state["custom_fields"] = custom_fields
        data["runtime_state"] = runtime_state
    elif "smartfollowup_timer" in data["custom_fields"]:
        data["custom_fields"]["smartfollowup_timer"] = _shift_timer_payload(
            data["custom_fields"]["smartfollowup_timer"],
            hours_back,
        )

    return updated_messages, data


def serialize_session(session: dict[str, Any]) -> dict[str, Any]:
    payload = dict(session)
    preconditions = ensure_sandbox_preconditions(payload.get("preconditions"))
    lead = preconditions["lead_identity"]
    payload["preconditions"] = preconditions
    payload["lead"] = {
        "first_name": lead.get("first_name", ""),
        "last_name": lead.get("last_name", ""),
        "email": lead.get("email", ""),
        "phone": lead.get("phone", ""),
        "source": lead.get("source", ""),
    }
    return payload


def _custom_fields_to_list(custom_fields: dict[str, Any]) -> list[dict[str, Any]]:
    result = []
    for key, value in (custom_fields or {}).items():
        result.append({
            "key": key,
            "field_value": value if isinstance(value, str) else json.dumps(value),
        })
    return result


def _custom_fields_to_dict(custom_fields: list[dict[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for field in custom_fields:
        key = field.get("key") or field.get("id")
        if not key:
            continue
        value = field.get("field_value")
        if value is None:
            value = field.get("value")
        if isinstance(value, str):
            try:
                result[key] = json.loads(value)
                continue
            except Exception:
                pass
        result[key] = value
    return result


def _parse_iso(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def _shift_iso(value: Any, hours_back: float) -> str:
    dt = _parse_iso(value)
    if not dt:
        return str(value or "")
    return (dt - timedelta(hours=hours_back)).isoformat()


def _shift_timer_payload(value: Any, hours_back: float) -> Any:
    data = value
    if isinstance(value, str):
        try:
            data = json.loads(value)
        except Exception:
            return value
    if not isinstance(data, dict):
        return value

    shifted = dict(data)
    for key in ("due", "scheduled_at"):
        if shifted.get(key):
            shifted[key] = _shift_iso(shifted.get(key), hours_back)
    return shifted if not isinstance(value, str) else json.dumps(shifted)


def _shift_appointments(appointments: list[dict[str, Any]], hours_back: float) -> list[dict[str, Any]]:
    shifted = []
    for appointment in appointments or []:
        entry = dict(appointment)
        entry["start"] = _shift_iso(entry.get("start"), hours_back)
        if entry.get("end"):
            entry["end"] = _shift_iso(entry.get("end"), hours_back)
        shifted.append(entry)
    return shifted


def _plus_one_hour(start: str | None) -> str:
    dt = _parse_iso(start)
    if not dt:
        return ""
    return (dt + timedelta(hours=1)).isoformat()
