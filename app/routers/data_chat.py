"""
AI Data Chat — API router.

Endpoints for sending messages, managing sessions, and portal access.
Admin endpoints use global Bearer auth. Portal endpoint uses JWT.
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from app.services.data_chat import handle_message, revert_session, _load_session
from app.services.data_chat_actions import execute_action_request, reject_action_request
from app.services.supabase_client import supabase

logger = logging.getLogger(__name__)

data_chat_router = APIRouter(prefix="/ai/data-chat", tags=["data-chat"])


# ── Request/Response Models ──

class SendMessageRequest(BaseModel):
    session_id: str | None = None
    message: str
    entity_id: str | None = None
    tenant_id: str
    user_id: str | None = None
    model_override: str | None = None
    origin: str = "admin"


class PortalMessageRequest(BaseModel):
    session_id: str | None = None
    message: str
    entity_id: str
    portal_token: str


class RevertRequest(BaseModel):
    revert_to_timestamp: str


class UpdateSessionRequest(BaseModel):
    title: str | None = None
    status: str | None = None
    model_override: str | None = None


class ActionExecuteRequest(BaseModel):
    approved_by: str | None = None


# ── Admin Endpoints (Bearer auth via global dependency) ──

@data_chat_router.post("/message")
async def send_message(body: SendMessageRequest) -> dict[str, Any]:
    """Send a message to the AI Data Chat agent."""
    if not body.message.strip():
        raise HTTPException(status_code=400, detail="Message cannot be empty")
    if not body.tenant_id:
        raise HTTPException(status_code=400, detail="tenant_id is required")

    try:
        result = await handle_message(
            tenant_id=body.tenant_id,
            user_id=body.user_id,
            entity_id=body.entity_id,
            message=body.message.strip(),
            session_id=body.session_id,
            model_override=body.model_override,
            origin=body.origin if body.origin in ("admin", "portal") else "admin",
        )
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception("Data chat message error")
        raise HTTPException(status_code=500, detail=f"Chat error: {str(e)}")


@data_chat_router.get("/sessions")
async def list_sessions(
    tenant_id: str = Query(...),
    entity_id: str | None = Query(None),
    origin: str | None = Query(None),
    status: str = Query("active"),
) -> list[dict[str, Any]]:
    """List chat sessions for a tenant."""
    params: dict[str, str] = {
        "tenant_id": f"eq.{tenant_id}",
        "status": f"eq.{status}",
        "select": "id,tenant_id,entity_id,user_id,origin,title,model_override,"
                  "total_cost,message_count,status,created_at,updated_at",
        "order": "updated_at.desc",
        "limit": "100",
    }
    if entity_id:
        params["entity_id"] = f"eq.{entity_id}"
    if origin:
        params["origin"] = f"eq.{origin}"

    resp = await supabase._request(
        supabase.main_client, "GET", "/ai_chat_sessions",
        params=params,
        label="data_chat_list_sessions",
    )
    raw = resp.json()
    sessions = raw if isinstance(raw, list) else []

    # Enrich with entity names
    entity_ids = list(set(s.get("entity_id") for s in sessions if isinstance(s, dict) and s.get("entity_id")))
    entity_names = {}
    if entity_ids:
        eresp = await supabase._request(
            supabase.main_client, "GET", "/entities",
            params={"id": f"in.({','.join(entity_ids)})", "select": "id,name"},
            label="data_chat_entity_names",
        )
        for e in (eresp.json() or []):
            entity_names[e["id"]] = e["name"]

    for s in sessions:
        s["entity_name"] = entity_names.get(s.get("entity_id"), None)

    return sessions


@data_chat_router.get("/sessions/{session_id}")
async def get_session(session_id: str) -> dict[str, Any]:
    """Get a session with all messages."""
    session = await _load_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    # Enrich with entity name
    if session.get("entity_id"):
        resp = await supabase._request(
            supabase.main_client, "GET", "/entities",
            params={"id": f"eq.{session['entity_id']}", "select": "name"},
            label="data_chat_entity_name",
        )
        edata = resp.json()
        session["entity_name"] = edata[0]["name"] if edata else None

    return session


@data_chat_router.patch("/sessions/{session_id}")
async def update_session(session_id: str, body: UpdateSessionRequest) -> dict[str, Any]:
    """Update session title, status, or model override."""
    update: dict[str, Any] = {}
    if body.title is not None:
        update["title"] = body.title[:100]
    if body.status is not None:
        if body.status not in ("active", "archived"):
            raise HTTPException(status_code=400, detail="Invalid status")
        update["status"] = body.status
    if body.model_override is not None:
        update["model_override"] = body.model_override or None

    if not update:
        raise HTTPException(status_code=400, detail="No fields to update")

    await supabase._request(
        supabase.main_client, "PATCH", "/ai_chat_sessions",
        params={"id": f"eq.{session_id}"},
        json=update,
        label="data_chat_update_session",
    )
    return {"success": True}


@data_chat_router.delete("/sessions/{session_id}")
async def delete_session(session_id: str) -> dict[str, Any]:
    """Delete a session permanently."""
    await supabase._request(
        supabase.main_client, "DELETE", "/ai_chat_sessions",
        params={"id": f"eq.{session_id}"},
        label="data_chat_delete_session",
    )
    return {"success": True}


@data_chat_router.post("/sessions/{session_id}/revert")
async def revert(session_id: str, body: RevertRequest) -> dict[str, Any]:
    """Revert a session to a previous timestamp."""
    try:
        result = await revert_session(session_id, body.revert_to_timestamp)
        return result
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.exception("Data chat revert error")
        raise HTTPException(status_code=500, detail=str(e))


@data_chat_router.post("/actions/{action_request_id}/execute")
async def execute_action(action_request_id: str, body: ActionExecuteRequest) -> dict[str, Any]:
    try:
        return await execute_action_request(action_request_id, approved_by=body.approved_by)
    except Exception as e:
        logger.exception("Data chat action execute error")
        raise HTTPException(status_code=500, detail=str(e))


@data_chat_router.post("/actions/{action_request_id}/reject")
async def reject_action(action_request_id: str) -> dict[str, Any]:
    try:
        return await reject_action_request(action_request_id)
    except Exception as e:
        logger.exception("Data chat action reject error")
        raise HTTPException(status_code=500, detail=str(e))


# ── Portal Endpoint (JWT auth, not Bearer) ──

@data_chat_router.post("/portal-message")
async def portal_message(body: PortalMessageRequest) -> dict[str, Any]:
    """Send a message from a portal user. Uses JWT auth instead of Bearer."""
    if not body.message.strip():
        raise HTTPException(status_code=400, detail="Message cannot be empty")

    # Validate portal JWT
    from app.main import _decode_portal_token
    payload = _decode_portal_token(body.portal_token)
    jwt_entity_id = payload.get("sub", "")

    if jwt_entity_id != body.entity_id:
        raise HTTPException(status_code=403, detail="Entity mismatch")

    # Resolve tenant from entity
    tenant_id = await supabase.resolve_tenant_id(body.entity_id)
    if not tenant_id:
        raise HTTPException(status_code=400, detail="Could not resolve tenant")

    try:
        result = await handle_message(
            tenant_id=tenant_id,
            user_id=None,  # Portal users don't have Supabase auth user IDs
            entity_id=body.entity_id,
            message=body.message.strip(),
            session_id=body.session_id,
            origin="portal",
        )
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception("Portal data chat error")
        raise HTTPException(status_code=500, detail=f"Chat error: {str(e)}")
