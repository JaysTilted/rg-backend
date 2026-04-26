"""FastAPI app — webhook routes for the Text Engine."""

import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from prefect import tags as prefect_tags

from app.text_engine.logging import setup_logging, set_request_context, clear_request_context, get_request_context
from app.text_engine.pipeline import text_engine
from app.models import (
    BookingWebhookBody,
    CleanChatHistoryBody,
    ManualLearningTestBody,
    MissedCallWebhookBody,
    UpdateKBBody,
)
from app.workflows.outreach_resolver import resolve_outreach
# reactivate_lead import removed — Python scheduler handles reactivation
from app.workflows.missed_call_textback import process_missed_call
from app.workflows.booking_logger import process_booking_log
from app.auth import verify_auth
from app.services.mattermost import notify_error
from app.services.supabase_client import supabase
from app.services.postgres_client import postgres
from app.services.ghl_client import start_ghl_pool, stop_ghl_pool
from app.services.ai_client import start_ai_clients
from app.config import settings
from app.workflows.log_cleanup import run_cleanup_loop
from app.workflows.daily_reports import run_daily_reports, run_reports_loop
from app.workflows.manual_message_learning import (
    run_manual_learning, run_learning_loop, _run_learning_agent,
    _validate_findings, WorkflowTokenTracker,
)
from app.workflows.update_kb import process_kb_update
from app.workflows.clean_chat_history import clean_chat_history
from app.testing import testing_router
from app.testing.sandbox import sandbox_router
from app.api.token_map import router as token_map_router
from app.api.ghl_proxy import router as ghl_proxy_router
from app.routers.data_chat import data_chat_router
from app.marketplace.oauth import router as marketplace_oauth_router

# Configure structured JSON logging before anything else
setup_logging()
logger = logging.getLogger(__name__)


class MissingTenantAIKeyError(Exception):
    """Raised when a non-platform tenant tries to use AI without its own OpenRouter key."""

    def __init__(self, entity_id: str, detail: str | None = None):
        self.entity_id = entity_id
        self.detail = detail or "No usable AI provider key is configured for this tenant. Add an OpenRouter or direct-provider key in Settings, or configure the shared Azure runtime key."
        super().__init__(self.detail)


# ============================================================================
# PREFECT TAG HELPERS
# ============================================================================


def build_flow_name(slug: str, entity_id: str) -> str:
    """Build the Prefect flow name: slug--shortid for uniqueness + readability.

    Example: 'dr-k-dental--a1b2c3d4'
    """
    return f"{slug}--{entity_id[:8]}"


def build_run_tags(
    *,
    slug: str,
    entity_id: str,
    contact_id: str,
    channel: str,
    trigger_type: str,
    agent_type: str = "",
) -> list[str]:
    """Build Prefect tags for a flow run.

    Tag format uses key:value prefixes for alphabetical grouping in the dashboard.

    Tags:
        entity:<slug--shortid>                          — entity identifier
        contact:<contact_id>                            — lead identifier
        channel:<channel>                               — sms, email, etc.
        type:<trigger_type>                             — reply or followup
        agent:<agent_type>                              — conversion-agent, etc. (reply only)
    """
    flow_name = build_flow_name(slug, entity_id)

    tags = [
        f"entity:{flow_name}",
        f"contact:{contact_id}",
        f"channel:{channel.lower()}",
        f"type:{trigger_type}",
    ]
    if agent_type:
        tags.append(f"agent:{agent_type.lower().replace(' ', '-')}")
    return tags


def derive_result_tag(result: dict, trigger_type: str) -> str:
    """Derive a result: tag from the pipeline's return dict.

    Reply paths:
        result:replied          — normal agent response delivered
        result:transferred      — transfer to human (classifier or agent tool)
        result:opted-out        — lead opted out
        result:bot-off          — contact has "stop bot" tag, skipped all AI
        result:no-reply         — dont_respond, form submission, post-agent stop
    Followup paths:
        result:followup-sent    — follow-up generated and sent
        result:followup-skipped — classifier said no, or early gate blocked it
    """
    if trigger_type == "reply":
        path = result.get("path", "")
        gate = result.get("_classification_gate", "")
        if path == "Transfer To Human":
            return "result:transferred"
        elif path == "Opt Out":
            return "result:opted-out"
        elif path == "No Reply Needed" and gate == "stop_bot":
            return "result:bot-off"
        elif path == "No Reply Needed":
            return "result:no-reply"
        else:
            # Normal reply — agent responded
            return "result:replied"
    else:
        # Followup
        if result.get("followUpNeeded"):
            return "result:followup-sent"
        else:
            return "result:followup-skipped"


def _classify_error_stage(exc: Exception) -> str:
    """Classify which pipeline stage likely failed, for structured error responses."""
    msg = str(exc).lower()
    exc_type = type(exc).__name__.lower()
    if "entity" in msg and "not found" in msg:
        return "config_lookup"
    if "timeout" in msg or "timeout" in exc_type:
        return "timeout"
    if any(k in msg for k in ("openrouter", "gemini", "openai", "genai")):
        return "ai_provider"
    if any(k in msg for k in ("ghl", "leadconnector", "429")):
        return "ghl_api"
    if any(k in msg for k in ("supabase", "postgres", "asyncpg", "pool")):
        return "database"
    return "unknown"


def _resolve_openrouter_key() -> str:
    """Return the single production OpenRouter API key (DEPRECATED — use _resolve_tenant_ai_keys)."""
    return settings.openrouter_api_key


async def _resolve_tenant_ai_keys(entity_id: str) -> dict[str, str]:
    """Resolve all AI API keys for an entity's tenant.

    Returns dict with keys: openrouter, google, openai, anthropic, deepseek, xai.
    """
    try:
        tenant = await supabase.get_tenant_for_entity(entity_id)
        is_platform_owner = bool(tenant.get("is_platform_owner"))
        openrouter_key = (tenant.get("openrouter_api_key") or "").strip()

        has_shared_ai = bool(settings.azure_openai_api_key or settings.openrouter_api_key)
        has_direct_tenant_ai = bool(
            (tenant.get("openai_key") or "").strip()
            or (tenant.get("google_ai_key") or "").strip()
            or (tenant.get("anthropic_key") or "").strip()
            or (tenant.get("deepseek_key") or "").strip()
            or (tenant.get("xai_key") or "").strip()
        )

        if not is_platform_owner and not (openrouter_key or has_direct_tenant_ai or has_shared_ai):
            logger.warning("TENANT | entity=%s | blocked | no usable AI provider key", entity_id)
            raise MissingTenantAIKeyError(entity_id)

        keys = {
            "openrouter": openrouter_key or (settings.openrouter_api_key if is_platform_owner else ""),
            "google": (tenant.get("google_ai_key") or "").strip(),
            "openai": (tenant.get("openai_key") or "").strip(),
            "anthropic": (tenant.get("anthropic_key") or "").strip(),
            "deepseek": (tenant.get("deepseek_key") or "").strip(),
            "xai": (tenant.get("xai_key") or "").strip(),
        }
        return keys
    except MissingTenantAIKeyError:
        raise
    except Exception as e:
        logger.error("TENANT | entity=%s | Failed to resolve tenant keys: %s", entity_id, e)
        raise


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup/shutdown: create and destroy connection pools."""
    # Startup
    await supabase.start()
    try:
        await postgres.start()
    except Exception as e:
        logger.warning("Postgres pools failed to connect (will retry on first use): %s", e)
    await start_ghl_pool()
    start_ai_clients()
    # Load default AI models from Supabase (cached with 5-min TTL)
    from app.text_engine.model_resolver import refresh_defaults_if_stale
    await refresh_defaults_if_stale()
    asyncio.create_task(run_cleanup_loop())
    # asyncio.create_task(run_reports_loop())    # Disabled — daily reports
    # asyncio.create_task(run_learning_loop())   # Disabled — manual message learning
    # Python-first scheduling: recover pending messages + start safety-net poller
    from app.workflows.message_scheduler_loop import recover_all_pending, run_safety_net_poller
    from app.services.debounce import debounce_startup_recovery
    from app.workflows.ghl_conversation_sync_poller import run_conversation_sync_poller
    asyncio.create_task(recover_all_pending())
    asyncio.create_task(run_safety_net_poller())
    asyncio.create_task(debounce_startup_recovery())
    asyncio.create_task(run_conversation_sync_poller())
    # Simulator: mark stale running/generating/analyzing simulations as failed
    from app.testing.simulator import recover_stale_simulations
    asyncio.create_task(recover_stale_simulations())
    yield
    # Shutdown
    await supabase.stop()
    await postgres.stop()
    await stop_ghl_pool()


app = FastAPI(title="Text Engine", version="0.1.0", lifespan=lifespan, dependencies=[Depends(verify_auth)])

# CORS — allow frontend (localhost dev + production) to call the API
from fastapi.middleware.cors import CORSMiddleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://localhost:5173",
        "https://app.ironautomations.com",
        "https://app.bookmyleads.ai",
        "https://app.refinedgrowth.net",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(testing_router)
app.include_router(sandbox_router)
app.include_router(token_map_router)
app.include_router(ghl_proxy_router)
app.include_router(data_chat_router)
app.include_router(marketplace_oauth_router)


@app.exception_handler(MissingTenantAIKeyError)
async def missing_tenant_ai_key_handler(_: Request, exc: MissingTenantAIKeyError):
    return JSONResponse(
        status_code=412,
        content={
            "error": "tenant_ai_locked",
            "detail": exc.detail,
            "entity_id": exc.entity_id,
        },
    )


@app.middleware("http")
async def error_notification_middleware(request: Request, call_next):
    """Safety net — catches exceptions that escape endpoint handlers."""
    try:
        return await call_next(request)
    except Exception as exc:
        source = f"{request.method} {request.url.path}"
        try:
            await notify_error(exc, source=source, stage=_classify_error_stage(exc), context=get_request_context())
        except Exception:
            pass
        raise


@app.get("/health")
async def health():
    return {"status": "ok"}


# ============================================================================
# TENANT TIER ENFORCEMENT
# ============================================================================


@app.get("/tenants/{tenant_id}/can-create-client")
async def can_create_client(tenant_id: str):
    """Check if a tenant can create another client (tier limit check).

    Auth handled globally by Depends(verify_auth) on the FastAPI app.
    """
    # Get tenant
    try:
        tenant = await supabase.get_tenant(tenant_id)
    except ValueError:
        return JSONResponse(status_code=404, content={"error": "Tenant not found"})

    # Count current clients (always — even for unlimited tenants)
    resp = await supabase._request(
        supabase.main_client, "GET", "/entities",
        params={
            "tenant_id": f"eq.{tenant_id}",
            "entity_type": "eq.client",
            "status": "eq.active",
            "select": "id",
        },
        label="count_tenant_clients",
    )
    current = len(resp.json())

    max_clients = tenant.get("max_clients")
    return {
        "allowed": max_clients is None or current < max_clients,
        "current": current,
        "max": max_clients,
    }


# ============================================================================
# PORTAL SESSION AUTH
# ============================================================================

from pydantic import BaseModel

class PortalVerifyRequest(BaseModel):
    token: str


class PortalChangeRequestBody(BaseModel):
    entity_id: str
    topic: str
    description: str
    token: str | None = None


def _decode_portal_token(token: str) -> dict:
    import jwt

    if not settings.portal_jwt_secret:
        raise HTTPException(status_code=503, detail="Portal auth not configured")

    try:
        return jwt.decode(token, settings.portal_jwt_secret, algorithms=["HS256"])
    except jwt.ExpiredSignatureError as exc:
        raise HTTPException(status_code=401, detail="Portal session expired") from exc
    except jwt.InvalidTokenError as exc:
        raise HTTPException(status_code=401, detail="Invalid portal session") from exc


def _verify_api_auth_header(request: Request) -> None:
    if not settings.api_auth_token:
        return

    auth_header = request.headers.get("Authorization", "")
    parts = auth_header.split(" ", 1)
    if len(parts) != 2 or parts[0] != "Bearer" or parts[1] != settings.api_auth_token:
        raise HTTPException(status_code=401, detail="Invalid authentication token")


def _extract_location_id_from_payload(body: dict | None) -> str:
    """Extract a GHL location ID from a standard webhook payload."""
    if not isinstance(body, dict):
        return ""
    location = body.get("location")
    if isinstance(location, dict):
        value = str(location.get("id", "")).strip()
        if value:
            return value
    for key in ("location_id", "locationId", "lid"):
        value = str(body.get(key, "")).strip()
        if value:
            return value
    return ""


async def _resolve_entity_reference(entity_ref: str) -> dict:
    """Resolve an entity reference from either UUID or GHL location ID."""
    ref = (entity_ref or "").strip()
    if not ref:
        raise ValueError("Missing entity reference")
    try:
        return await supabase.resolve_entity(ref)
    except ValueError:
        return await supabase.resolve_entity_by_location_id(ref)


async def _resolve_entity_for_webhook(
    *,
    entity_ref: str = "",
    body: dict | None = None,
    source: str,
) -> tuple[str, dict]:
    """Resolve entity config for a webhook from path ref and/or payload location."""
    config = None
    location_id = _extract_location_id_from_payload(body)

    if entity_ref:
        config = await _resolve_entity_reference(entity_ref)
        expected_location_id = (config.get("ghl_location_id") or "").strip()
        if location_id and expected_location_id and location_id != expected_location_id:
            logger.warning(
                "WEBHOOK | location_mismatch | source=%s | ref=%s | expected=%s | got=%s",
                source,
                entity_ref,
                expected_location_id,
                location_id,
            )
            raise HTTPException(status_code=403, detail="Location ID does not match entity")
        return str(config.get("id") or entity_ref), config

    if location_id:
        config = await supabase.resolve_entity_by_location_id(location_id)
        return str(config.get("id") or ""), config

    raise HTTPException(
        status_code=400,
        detail="Missing location.id in webhook payload",
    )

@app.get("/portal/session")
async def portal_session(request: Request, cid: str = "", lid: str = ""):
    """Create a signed JWT session for portal access.

    GHL iframe hits this URL with location ID, and may include entity ID during
    the migration period. Verifies the location matches, signs a JWT, and
    redirects to the frontend portal.
    """
    import jwt
    from datetime import datetime, timezone, timedelta

    if not settings.portal_jwt_secret:
        return HTMLResponse(
            "<h2>Portal auth not configured</h2><p>Contact support.</p>",
            status_code=503,
        )

    if not lid:
        return HTMLResponse(
            "<h2>Missing parameters</h2><p>Location ID is required.</p>",
            status_code=400,
        )

    # Resolve entity (client or personal bot)
    try:
        if cid:
            config = await _resolve_entity_reference(cid)
        else:
            config = await supabase.resolve_entity_by_location_id(lid)
    except ValueError:
        return HTMLResponse(
            "<h2>Not found</h2><p>No client or bot matches this ID.</p>",
            status_code=404,
        )
    except RuntimeError:
        logger.exception("PORTAL | ambiguous_location | lid=%s", lid)
        return HTMLResponse(
            "<h2>Configuration error</h2><p>Multiple active entities match this location ID.</p>",
            status_code=409,
        )
    except Exception:
        logger.exception("PORTAL | resolve_entity_failed | cid=%s | lid=%s", cid, lid)
        return HTMLResponse(
            "<h2>Service unavailable</h2><p>Please try again.</p>",
            status_code=502,
        )

    entity_id = str(config.get("id") or cid)

    # Verify location ID matches
    entity_lid = config.get("ghl_location_id", "")
    if not entity_lid:
        return HTMLResponse(
            "<h2>GHL integration not configured</h2>"
            "<p>This account has no GHL location ID set.</p>",
            status_code=403,
        )

    if lid != entity_lid:
        logger.warning(
            "PORTAL | location_mismatch | cid=%s | expected=%s | got=%s",
            entity_id, entity_lid, lid,
        )
        return HTMLResponse(
            "<h2>Access denied</h2><p>Location ID does not match.</p>",
            status_code=403,
        )

    # Log iframe signal (info only, not a gate)
    sec_fetch_dest = request.headers.get("sec-fetch-dest", "unknown")
    logger.info(
        "PORTAL | session_created | entity=%s | sec_fetch_dest=%s",
        entity_id, sec_fetch_dest,
    )

    # Sign JWT
    now = datetime.now(timezone.utc)
    payload = {
        "sub": entity_id,
        "lid": lid,
        "iat": now,
        "exp": now + timedelta(hours=4),
    }
    token = jwt.encode(payload, settings.portal_jwt_secret, algorithm="HS256")

    redirect_url = f"{settings.frontend_url}/portal/{entity_id}?session={token}"
    return RedirectResponse(url=redirect_url, status_code=302)


@app.post("/portal/verify")
async def portal_verify(body: PortalVerifyRequest):
    """Verify a portal JWT and return payload if valid."""
    try:
        payload = _decode_portal_token(body.token)
        return {"valid": True, "entity_id": payload["sub"], "exp": payload["exp"]}
    except HTTPException as exc:
        if exc.status_code == 401 and exc.detail == "Portal session expired":
            return JSONResponse(
                {"valid": False, "error": "expired"}, status_code=401
            )
        return JSONResponse(
            {"valid": False, "error": "invalid"}, status_code=401
        )


# ============================================================================
# REPLY / INBOUND WEBHOOK (Standard GHL webhook — debounce + pipeline)
# ============================================================================


@app.post("/portal/change-request")
async def portal_change_request(request: Request, body: PortalChangeRequestBody):
    """Create a portal change request and send tenant SMS/email via the Python backend."""
    entity_id = (body.entity_id or "").strip()
    topic = (body.topic or "").strip()
    description = (body.description or "").strip()

    if not entity_id:
        raise HTTPException(status_code=400, detail="Missing entity_id")
    if not topic:
        raise HTTPException(status_code=400, detail="Missing topic")
    if not description:
        raise HTTPException(status_code=400, detail="Missing description")

    if body.token:
        payload = _decode_portal_token(body.token)
        if payload.get("sub") != entity_id:
            raise HTTPException(status_code=403, detail="Portal session does not match entity")
    else:
        _verify_api_auth_header(request)

    try:
        entity = await supabase.resolve_entity(entity_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail="Entity not found") from exc

    tenant_id = entity.get("tenant_id")
    if not tenant_id:
        raise HTTPException(status_code=400, detail="Entity has no tenant")

    insert_resp = await supabase._request(
        supabase.main_client,
        "POST",
        "/change_requests",
        json={
            "entity_id": entity_id,
            "tenant_id": tenant_id,
            "topic": topic,
            "description": description,
            "status": "pending",
        },
        headers={"Prefer": "return=representation"},
        label="create_change_request",
    )
    if insert_resp.status_code >= 400:
        logger.warning(
            "PORTAL | change_request_insert_failed | entity=%s | status=%s | body=%s",
            entity_id,
            insert_resp.status_code,
            insert_resp.text[:200],
        )
        raise HTTPException(status_code=502, detail="Failed to create change request")

    rows = insert_resp.json()
    created_row = rows[0] if isinstance(rows, list) and rows else {}

    notification_results = []
    try:
        from app.services.notifications import send_tenant_notification

        notification_results = await send_tenant_notification(
            tenant_id=tenant_id,
            event="change_request",
            context={"topic": topic, "description": description},
            client_name=entity.get("name") or "",
            entity_id=entity_id,
            allow_in_app=False,
        )
    except Exception:
        logger.exception(
            "PORTAL | change_request_notify_failed | entity=%s | tenant=%s",
            entity_id,
            tenant_id,
        )

    return {
        "ok": True,
        "change_request_id": created_row.get("id"),
        "notification_results": notification_results,
    }


def _extract_webhook_tags(body: dict) -> list[str]:
    """GHL webhook payloads carry contact tags in one of several shapes
    depending on workflow trigger. Normalize to a list[str], lowercase.
    """
    tags: list = []
    raw = body.get("tags") or body.get("contact_tags") or body.get("contact", {}).get("tags", []) or []
    if isinstance(raw, str):
        tags = [t.strip() for t in raw.split(",") if t.strip()]
    elif isinstance(raw, list):
        tags = [str(t).strip() for t in raw if t]
    return [t.lower() for t in tags]


def _is_contact_allowed(
    contact_id: str,
    config: dict,
    contact_tags: list[str] | None = None,
) -> tuple[bool, str]:
    """Check if a contact is allowed to trigger AI responses.

    Reads `system_config.test_mode` block:
      - `enabled: bool` — if true, only allowlisted contacts are processed
      - `allowed_contact_ids: list[str]` — contacts that bypass the gate
      - `allowed_tags: list[str]` — any contact carrying one of these tags
        (case-insensitive) bypasses the gate. Lets Jay tag real leads with
        `ai-test` in GHL to opt them in one-by-one without a redeploy.

    Returns (allowed, reason). If test_mode is disabled, all contacts pass.
    """
    sys_config = (config or {}).get("system_config") or {}
    test_mode = sys_config.get("test_mode") or {}
    if not test_mode.get("enabled"):
        return True, ""

    allowed_ids = test_mode.get("allowed_contact_ids") or []
    if contact_id in allowed_ids:
        return True, "allowlisted_id"

    allowed_tags_raw = test_mode.get("allowed_tags") or []
    if allowed_tags_raw and contact_tags:
        allowed_tags = {str(t).strip().lower() for t in allowed_tags_raw if t}
        tags_lower = {t.lower() for t in contact_tags}
        if allowed_tags & tags_lower:
            matched = sorted(allowed_tags & tags_lower)[0]
            return True, f"allowlisted_tag:{matched}"

    return False, f"test_mode enabled, contact {contact_id} not in allowlist"


@app.post("/webhook/reply")
@app.post("/webhook/{entity_ref}/reply")
async def reply_webhook(request: Request, entity_ref: str = ""):
    """Inbound message handler — receives standard GHL webhook.

    GHL fires this on EVERY inbound message (no consolidation timer).
    Python handles debouncing, cancellation, and pipeline execution.

    Returns immediately (fire-and-forget). The debounce manager:
    1. Resets the consolidation timer on each message
    2. Cancels any running pipeline for this contact
    3. Cancels pending FUs + outreach
    4. When timer expires: syncs conversation, runs reply pipeline
    """
    try:
        body = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"error": "Invalid JSON"})

    contact_id = body.get("contact_id", "")
    if not contact_id:
        return JSONResponse(status_code=400, content={"error": "Missing contact_id"})

    try:
        entity_id, config = await _resolve_entity_for_webhook(
            entity_ref=entity_ref,
            body=body,
            source="reply",
        )
    except ValueError:
        raise HTTPException(status_code=404, detail=f"Entity {entity_ref} not found")
    except RuntimeError:
        raise HTTPException(status_code=409, detail="Multiple active entities match this location ID")

    slug = config.get("slug") or config.get("name", entity_id[:8])

    set_request_context(entity_id=entity_id, entity_slug=slug, trigger_type="reply", contact_id=contact_id)

    allowed, reason = _is_contact_allowed(contact_id, config, _extract_webhook_tags(body))
    if not allowed:
        logger.info("INBOUND | blocked by test_mode | contact=%s | reason=%s", contact_id, reason)
        clear_request_context()
        return {"status": "skipped", "reason": "test_mode", "contact_id": contact_id}

    try:
        from app.services.debounce import debounce_inbound
        await debounce_inbound(entity_id, contact_id, body, config)

        logger.info("INBOUND | accepted | contact=%s | entity=%s | reason=%s", contact_id, entity_id[:8], reason)
        return {"status": "accepted", "contact_id": contact_id}

    except Exception as e:
        logger.exception("INBOUND | error | contact=%s", contact_id)
        return JSONResponse(status_code=500, content={"error": True, "message": str(e)[:200]})
    finally:
        clear_request_context()


# Form submission, call event, booking event endpoints removed — merged into
# /resolve-outreach, /missed-call, /log-booking respectively (same URLs, new logic).


if False:  # Dead code — form_submission_webhook merged into /resolve-outreach
  async def form_submission_webhook(entity_id: str, request: Request):
    """Handle form submission — resolve outreach template and schedule all positions.

    GHL fires this standard webhook when a lead submits a form.
    Python resolves the template, schedules all positions with timing/jitter/windows,
    and creates the lead record.
    """
    try:
        body = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"error": "Invalid JSON"})

    contact_id = body.get("contact_id", "")
    if not contact_id:
        return JSONResponse(status_code=400, content={"error": "Missing contact_id"})

    # Resolve entity
    try:
        config = await supabase.resolve_entity(entity_id)
    except ValueError:
        raise HTTPException(status_code=404, detail=f"Entity {entity_id} not found")

    slug = config.get("slug") or config.get("name", entity_id[:8])

    set_request_context(
        entity_id=entity_id,
        entity_slug=slug,
        trigger_type="form_submission",
        contact_id=contact_id,
    )

    try:
        from app.webhooks.standard_parser import extract_custom_field, extract_location
        from app.models import OutreachResolverBody
        from app.workflows.outreach_resolver import resolve_outreach
        from app.workflows.outreach_scheduler import schedule_outreach_positions
        from app.services.ghl_client import GHLClient

        # Build OutreachResolverBody from standard webhook payload
        location = extract_location(body)
        resolver_body = OutreachResolverBody(
            id=contact_id,
            entityId=entity_id,
            name=body.get("full_name", ""),
            phone=body.get("phone", ""),
            email=body.get("email", ""),
            source=extract_custom_field(body, "Lead Source") or "Other",
            form_service_interest=extract_custom_field(body, "Form Service Interest") or extract_custom_field(body, "\ud83d\udcdc Form Service Interest") or "",
            location_name=location.get("name", ""),
            location_address=location.get("address", ""),
            location_city=location.get("city", ""),
            location_state=location.get("state", ""),
        )

        if not resolver_body.form_service_interest:
            return JSONResponse(status_code=400, content={"error": "Missing Form Service Interest"})

        # Resolve outreach template
        resolve_result, status_code = await resolve_outreach(entity_id, resolver_body)

        if not resolve_result.get("success"):
            return JSONResponse(status_code=status_code, content=resolve_result)

        # Schedule all positions
        resolved_positions = resolve_result.get("resolved_positions", [])
        timing_config = resolve_result.get("timing_config", {})
        channel = extract_custom_field(body, "Channel") or "SMS"

        ghl = GHLClient(
            api_key=config.get("ghl_api_key", ""),
            location_id=config.get("ghl_location_id", ""),
        )

        scheduled_count = await schedule_outreach_positions(
            entity_id=entity_id,
            contact_id=contact_id,
            channel=channel,
            resolved_positions=resolved_positions,
            timing_config=timing_config,
            template_name=resolve_result.get("form_service_interest", ""),
            config=config,
            ghl=ghl,
        )

        # Reschedule reactivation (form submission is an activity event)
        try:
            from app.workflows.reactivation_scheduler import reschedule_reactivation
            await reschedule_reactivation(entity_id, contact_id)
        except Exception as e:
            logger.warning("Form submission: reactivation reschedule failed: %s", e)

        logger.info(
            "FORM_SUBMISSION | scheduled %d outreach positions | template=%s | contact=%s",
            scheduled_count, resolve_result.get("form_service_interest"), contact_id,
        )

        return {
            "status": "scheduled",
            "positions": scheduled_count,
            "template": resolve_result.get("form_service_interest"),
            "variant": resolve_result.get("variant"),
            "lead_id": resolve_result.get("lead_id"),
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Form submission failed")
        return JSONResponse(status_code=500, content={"error": True, "message": str(e)[:200]})
    finally:
        clear_request_context()


# Call event + booking event endpoints removed — merged into /missed-call and /log-booking.


# Follow-up webhook removed — Python scheduler handles follow-ups via scheduled_messages.


# ============================================================================
# OUTREACH / FORM SUBMISSION WEBHOOK (Standard GHL webhook — schedule outreach)
# ============================================================================


@app.post("/webhook/resolve-outreach")
@app.post("/webhook/{entity_ref}/resolve-outreach")
async def resolve_outreach_webhook(request: Request, entity_ref: str = ""):
    """Handle form submission — resolve outreach template and schedule all positions.

    GHL fires this standard webhook when a lead submits a form.
    Python resolves the template, schedules all positions, creates the lead record.
    """
    try:
        body = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"error": "Invalid JSON"})

    contact_id = body.get("contact_id", "")
    if not contact_id:
        return JSONResponse(status_code=400, content={"error": "Missing contact_id"})

    try:
        entity_id, config = await _resolve_entity_for_webhook(
            entity_ref=entity_ref,
            body=body,
            source="resolve-outreach",
        )
    except ValueError:
        raise HTTPException(status_code=404, detail=f"Entity {entity_ref} not found")
    except RuntimeError:
        raise HTTPException(status_code=409, detail="Multiple active entities match this location ID")

    slug = config.get("slug") or config.get("name", entity_id[:8])
    set_request_context(entity_id=entity_id, entity_slug=slug, trigger_type="form_submission", contact_id=contact_id)

    allowed, reason = _is_contact_allowed(contact_id, config, _extract_webhook_tags(body))
    if not allowed:
        logger.info("RESOLVE_OUTREACH | blocked by test_mode | contact=%s | reason=%s", contact_id, reason)
        clear_request_context()
        return {"status": "skipped", "reason": "test_mode", "contact_id": contact_id}

    try:
        from app.webhooks.standard_parser import extract_custom_field, extract_location
        from app.models import OutreachResolverBody
        from app.workflows.outreach_resolver import resolve_outreach
        from app.workflows.outreach_scheduler import schedule_outreach_positions
        from app.services.ghl_client import GHLClient

        location = extract_location(body)
        resolver_body = OutreachResolverBody(
            id=contact_id,
            entityId=entity_id,
            name=body.get("full_name", ""),
            phone=body.get("phone", ""),
            email=body.get("email", ""),
            source=extract_custom_field(body, "Lead Source") or "Other",
            form_service_interest=extract_custom_field(body, "Form Service Interest") or extract_custom_field(body, "\ud83d\udcdc Form Service Interest") or "",
            location_name=location.get("name", ""),
            location_address=location.get("address", ""),
            location_city=location.get("city", ""),
            location_state=location.get("state", ""),
        )

        if not resolver_body.form_service_interest:
            return JSONResponse(status_code=400, content={"error": "Missing Form Service Interest"})

        resolve_result, status_code = await resolve_outreach(entity_id, resolver_body)

        if not resolve_result.get("success"):
            return JSONResponse(status_code=status_code, content=resolve_result)

        resolved_positions = resolve_result.get("resolved_positions", [])
        timing_config = resolve_result.get("timing_config", {})
        channel = extract_custom_field(body, "Channel") or "SMS"

        ghl = GHLClient(api_key=config.get("ghl_api_key", ""), location_id=config.get("ghl_location_id", ""))

        scheduled_count = await schedule_outreach_positions(
            entity_id=entity_id,
            contact_id=contact_id,
            channel=channel,
            resolved_positions=resolved_positions,
            timing_config=timing_config,
            template_name=resolve_result.get("form_service_interest", ""),
            config=config,
            ghl=ghl,
        )

        try:
            from app.workflows.reactivation_scheduler import reschedule_reactivation
            await reschedule_reactivation(entity_id, contact_id)
        except Exception:
            pass

        # New lead notification
        try:
            from app.services.notifications import send_notifications
            await send_notifications(
                config=config,
                event="new_lead",
                context={
                    "contact_id": contact_id,
                    "contact_name": resolver_body.name or "",
                    "contact_phone": resolver_body.phone or "",
                    "service": resolver_body.form_service_interest or "",
                    "ghl_location_id": config.get("ghl_location_id", ""),
                },
                entity_id=entity_id,
            )
        except Exception as e:
            logger.warning("FORM_SUBMISSION | notification failed: %s", e)

        try:
            from app.services.notifications import send_tenant_notification

            tenant_id = config.get("tenant_id")
            if tenant_id:
                await send_tenant_notification(
                    tenant_id=tenant_id,
                    event="new_lead",
                    context={
                        "contact_id": contact_id,
                        "contact_name": resolver_body.name or "",
                        "contact_phone": resolver_body.phone or "",
                        "service": resolver_body.form_service_interest or "",
                    },
                    client_name=config.get("name") or "",
                    entity_id=entity_id,
                )
        except Exception as e:
            logger.warning("FORM_SUBMISSION | tenant notification failed: %s", e)

        logger.info("FORM_SUBMISSION | scheduled %d positions | contact=%s", scheduled_count, contact_id)
        return {
            "status": "scheduled",
            "positions": scheduled_count,
            "template": resolve_result.get("form_service_interest"),
            "variant": resolve_result.get("variant"),
            "lead_id": resolve_result.get("lead_id"),
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Form submission failed")
        return JSONResponse(status_code=500, content={"error": True, "message": str(e)[:200]})
    finally:
        clear_request_context()



# Reactivation webhook removed — Python scheduler handles reactivation via scheduled_messages.


# ============================================================================
# BOOKING LOGGER WEBHOOK (Standard GHL webhook — logs booking + schedules reminders)
# ============================================================================


@app.post("/webhook/log-booking")
@app.post("/webhook/{entity_ref}/log-booking")
async def log_booking_webhook(request: Request, entity_ref: str = ""):
    """Handle booking events via standard GHL webhook.

    Parses calendar data from the standard webhook payload and routes
    to the booking logger. Also schedules/cancels appointment reminders.
    """
    try:
        body = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"error": "Invalid JSON"})

    contact_id = body.get("contact_id", "")
    if not contact_id:
        return JSONResponse(status_code=400, content={"error": "Missing contact_id"})

    try:
        entity_id, config = await _resolve_entity_for_webhook(
            entity_ref=entity_ref,
            body=body,
            source="log-booking",
        )
    except ValueError:
        raise HTTPException(status_code=404, detail=f"Entity {entity_ref} not found")
    except RuntimeError:
        raise HTTPException(status_code=409, detail="Multiple active entities match this location ID")

    slug = config.get("slug") or config.get("name", entity_id[:8])
    set_request_context(entity_id=entity_id, entity_slug=slug, trigger_type="booking_event", contact_id=contact_id)

    try:
        from app.webhooks.standard_parser import extract_calendar
        from app.models import BookingWebhookBody

        calendar = extract_calendar(body) or {}

        booking_body = BookingWebhookBody(
            contactId=contact_id,
            appointmentID=calendar.get("appointmentId", ""),
            calendarName=calendar.get("calendarName", ""),
            appointmentDateTime=calendar.get("startTime", ""),
            appointmentEndTime=calendar.get("endTime", ""),
            appointmentTimezone=calendar.get("selectedTimezone", ""),
            status=calendar.get("appoinmentStatus") or calendar.get("status", ""),
            entityId=entity_id,
            id=contact_id,
            name=body.get("full_name", ""),
            email=body.get("email", ""),
            phone=body.get("phone", ""),
        )

        result = await process_booking_log(entity_id, booking_body)

        logger.info(
            "BOOKING_EVENT | processed | contact=%s | appt=%s | status=%s | entity=%s",
            contact_id, calendar.get("appointmentId", "?"), calendar.get("status", "?"), entity_id[:8],
        )
        return result

    except Exception as e:
        logger.exception("BOOKING_EVENT | error | contact=%s", contact_id)
        return JSONResponse(status_code=500, content={"error": True, "message": str(e)[:200]})
    finally:
        clear_request_context()


# ============================================================================
# CALL EVENT WEBHOOK (Standard GHL webhook — routes answered vs missed calls)
# ============================================================================

_ANSWERED_CALL_STATUSES = frozenset({"completed", "answered", "connected"})


@app.post("/webhook/call-event")
@app.post("/webhook/{entity_ref}/call-event")
async def call_event_webhook(entity_ref: str = "", request: Request = None, test: bool = Query(False)):
    """Handle ALL call events via standard GHL webhook.

    Routes based on call status after conversation sync:
    - Answered/completed → pause bot (if toggle enabled), sync, push reactivation
    - Missed/no-answer/voicemail → missed call text-back + notification

    Pass ?test=true for synchronous dry-run processing (missed call path only).
    """
    try:
        body = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"error": "Invalid JSON"})

    contact_id = body.get("contact_id", "")
    if not contact_id:
        return JSONResponse(status_code=400, content={"error": "Missing contact_id"})

    try:
        entity_id, config = await _resolve_entity_for_webhook(
            entity_ref=entity_ref,
            body=body,
            source="call-event",
        )
    except ValueError:
        raise HTTPException(status_code=404, detail=f"Entity {entity_ref} not found")
    except RuntimeError:
        raise HTTPException(status_code=409, detail="Multiple active entities match this location ID")

    slug = config.get("slug") or config.get("name", entity_id[:8])
    set_request_context(entity_id=entity_id, entity_slug=slug, trigger_type="call_event", contact_id=contact_id)

    allowed, reason = _is_contact_allowed(contact_id, config, _extract_webhook_tags(body))
    if not allowed:
        logger.info("CALL_EVENT | blocked by test_mode | contact=%s | reason=%s", contact_id, reason)
        clear_request_context()
        return {"status": "skipped", "reason": "test_mode", "contact_id": contact_id}

    from app.services.workflow_tracker import WorkflowTracker

    tracker = WorkflowTracker(
        "call_event",
        entity_id=entity_id,
        ghl_contact_id=contact_id,
        trigger_source="webhook",
    )

    last_call_status = None
    sync_stats = None
    bot_paused = False
    action = "unknown"
    notification_sent = False
    reactivation_rescheduled = False

    try:
        from app.services.ghl_client import GHLClient
        ghl = GHLClient(api_key=config.get("ghl_api_key", ""), location_id=config.get("ghl_location_id", ""))

        # Small delay to let GHL index the call record
        await asyncio.sleep(3)

        # Sync conversation to get call metadata + last_call_status
        chat_table = config.get("chat_history_table_name", "")
        if chat_table:
            from app.text_engine.conversation_sync import run_conversation_sync
            try:
                sync_result = await run_conversation_sync(
                    contact_id=contact_id, ghl=ghl, chat_table=chat_table,
                    entity_id=config.get("id", entity_id), config=config,
                    workflow_run_id=tracker.run_id,
                )
                last_call_status = (sync_result or {}).get("last_call_status")
                sync_stats = sync_result
            except Exception as e:
                logger.warning("CALL_EVENT | sync failed: %s", e)

        logger.info("CALL_EVENT | status=%s | contact=%s | entity=%s", last_call_status, contact_id, entity_id[:8])

        # ---- ANSWERED CALL: pause bot (if toggle enabled) ----
        if last_call_status and last_call_status in _ANSWERED_CALL_STATUSES:
            action = "bot_paused"
            try:
                bot_paused = await _pause_bot(contact_id, config, ghl, source="answered_call")
            except Exception as e:
                logger.warning("CALL_EVENT | pause_bot failed: %s", e)

            # Push reactivation timer forward
            try:
                from app.workflows.reactivation_scheduler import reschedule_reactivation
                await reschedule_reactivation(entity_id, contact_id)
                reactivation_rescheduled = True
            except Exception:
                pass

            logger.info("CALL_EVENT | answered_call | contact=%s | status=%s", contact_id, last_call_status)
            return {"status": "accepted", "action": "bot_paused", "call_status": last_call_status, "contact_id": contact_id}

        # ---- MISSED/NO-ANSWER/VOICEMAIL/UNKNOWN: text-back flow ----
        action = "missed_call_textback"

        # Build MissedCallWebhookBody for the handler
        from app.models import MissedCallWebhookBody
        mc_body = MissedCallWebhookBody(
            id=contact_id,
            entityId=entity_id,
            name=body.get("full_name", ""),
            email=body.get("email", ""),
            phone=body.get("phone", ""),
        )

        # Missed call notification (fires regardless of textback status)
        try:
            from app.services.notifications import send_notifications
            sys_cfg = config.get("system_config") or {}
            setters = sys_cfg.get("setters") or {}
            textback_active = any(
                s.get("missed_call_textback") for s in setters.values() if isinstance(s, dict)
            )
            await send_notifications(
                config=config,
                event="missed_call",
                context={
                    "contact_id": contact_id,
                    "contact_name": body.get("full_name", ""),
                    "contact_phone": body.get("phone", ""),
                    "textback_active": "true" if textback_active else "false",
                },
                entity_id=entity_id,
            )
            notification_sent = True
        except Exception as e:
            logger.warning("CALL_EVENT | notification failed: %s", e)

        try:
            from app.services.notifications import send_tenant_notification

            tenant_id = config.get("tenant_id")
            if tenant_id:
                await send_tenant_notification(
                    tenant_id=tenant_id,
                    event="missed_call",
                    context={
                        "contact_id": contact_id,
                        "contact_name": body.get("full_name", ""),
                        "contact_phone": body.get("phone", ""),
                        "textback_active": "true" if textback_active else "false",
                    },
                    client_name=config.get("name") or "",
                    entity_id=entity_id,
                )
        except Exception as e:
            logger.warning("CALL_EVENT | tenant notification failed: %s", e)

        if test:
            result = await process_missed_call(entity_id, mc_body, dry_run=True, skip_delay=True)
            return result
        else:
            asyncio.create_task(_run_missed_call(entity_id, mc_body))

        # Reschedule reactivation
        try:
            from app.workflows.reactivation_scheduler import reschedule_reactivation
            await reschedule_reactivation(entity_id, contact_id)
            reactivation_rescheduled = True
        except Exception:
            pass

        logger.info("CALL_EVENT | missed_call | contact=%s | status=%s", contact_id, last_call_status)
        return {"status": "accepted", "action": "missed_call_textback", "call_status": last_call_status, "contact_id": contact_id}

    except Exception as e:
        tracker.set_error(str(e))
        logger.exception("CALL_EVENT | error | contact=%s", contact_id)
        return JSONResponse(status_code=500, content={"error": True, "message": str(e)[:200]})
    finally:
        # Workflow tracker — decisions + runtime context
        tracker.set_decisions({
            "call_status": last_call_status,
            "action": action,
            "bot_paused": bot_paused,
            "pause_toggle_enabled": (config.get("system_config") or {}).get("pause_bot_on_human_activity", True),
            "takeover_minutes": (config.get("system_config") or {}).get("human_takeover_minutes", _DEFAULT_HUMAN_TAKEOVER_MINUTES),
            "notification_sent": notification_sent,
            "reactivation_rescheduled": reactivation_rescheduled,
            "sync": {
                "messages_synced": (sync_stats or {}).get("messages_synced", 0),
                "calls_synced": (sync_stats or {}).get("calls_synced", 0),
            },
        })
        tracker.set_runtime_context({
            "contact": {
                "name": body.get("full_name", ""),
                "phone": body.get("phone", ""),
                "email": body.get("email", ""),
            },
            "call_status": last_call_status,
            "call_direction": body.get("direction", "unknown"),
            "test_mode": test,
        })
        tracker.set_system_config(config.get("system_config"), setter_key=None)
        tracker.set_metadata("channel", "Call")
        tracker.set_metadata("result_path", action)
        tracker.set_metadata("call_direction", body.get("direction", "unknown"))
        await tracker.save()
        clear_request_context()


@app.post("/webhook/missed-call")
async def missed_call_webhook_static_alias(request: Request, test: bool = Query(False)):
    """Static alias for /call-event using payload location.id resolution."""
    return await call_event_webhook("", request, test)


# Backward-compat alias for old GHL workflows still pointing to /missed-call
@app.post("/webhook/{entity_id}/missed-call")
async def missed_call_webhook_alias(entity_id: str, request: Request, test: bool = Query(False)):
    """Alias for /call-event — kept for backward compatibility."""
    return await call_event_webhook(entity_id, request, test)


async def _run_missed_call(entity_id: str, body: MissedCallWebhookBody) -> None:
    """Background task for missed call processing."""
    contact_id = body.resolved_contact_id
    set_request_context(
        entity_id=entity_id,
        trigger_type="missed-call",
        contact_id=contact_id,
    )
    try:
        run_tags = [
            f"entity:{entity_id[:8]}",
            f"contact:{contact_id}",
            "type:missed-call",
        ]
        with prefect_tags(*run_tags):
            result = await process_missed_call.with_options(
                name=f"missed-call--{entity_id[:8]}"
            )(entity_id, body)
        logger.info(
            "Missed call processed: action=%s, reason=%s",
            result.get("action", "send"),
            result.get("reason", ""),
        )
    except Exception as e:
        logger.exception("Missed call processing failed")
        try:
            await notify_error(e, source="background: _run_missed_call", stage=_classify_error_stage(e), context=get_request_context())
        except Exception:
            pass
    finally:
        clear_request_context()


# ============================================================================
# SHARED: Bot pause helper (used by human-activity + call-event)
# ============================================================================

_DEFAULT_HUMAN_TAKEOVER_MINUTES = 15


async def _pause_bot(
    contact_id: str,
    config: dict,
    ghl,
    source: str = "human_activity",
) -> bool:
    """Add stop_bot tag + schedule timed removal. Returns False if toggle is off."""
    from datetime import datetime, timedelta, timezone
    from app.services.message_scheduler import schedule_message

    sys_config = config.get("system_config") or {}
    if not sys_config.get("pause_bot_on_human_activity", True):
        logger.info("PAUSE_BOT | disabled by toggle | contact=%s | source=%s", contact_id, source)
        return False

    # Add stop bot tag (idempotent)
    try:
        await ghl.add_tag(contact_id, "stop bot")
    except Exception as e:
        logger.warning("PAUSE_BOT | add_tag failed: %s", e)

    # Update AI Bot field to "Off - Auto Resumes When Staff Is Inactive"
    try:
        field_defs = await ghl.get_custom_field_defs_full()
        ai_bot_field = next((f for f in field_defs if f.get("key") == "contact.ai_bot"), None)
        if ai_bot_field:
            await ghl.update_contact(contact_id, {
                "customFields": [{"id": ai_bot_field["id"], "field_value": "\u274cOff - Auto Resumes When Staff Is Inactive"}]
            })
        else:
            logger.warning("PAUSE_BOT | ai_bot field not found in location")
    except Exception as e:
        logger.warning("PAUSE_BOT | ai_bot field update failed: %s", e)

    # Schedule (or reset) tag removal
    takeover_minutes = sys_config.get("human_takeover_minutes", _DEFAULT_HUMAN_TAKEOVER_MINUTES)
    if not isinstance(takeover_minutes, (int, float)) or takeover_minutes <= 0:
        takeover_minutes = _DEFAULT_HUMAN_TAKEOVER_MINUTES

    due_at = datetime.now(timezone.utc) + timedelta(minutes=takeover_minutes)

    await schedule_message(
        supabase,
        entity_id=config.get("id"),
        contact_id=contact_id,
        message_type="human_takeover",
        position=0,
        channel="SMS",
        due_at=due_at,
        source="activity",
        triggered_by=source,
    )
    logger.info("PAUSE_BOT | scheduled | contact=%s | due_in=%dm | source=%s", contact_id, takeover_minutes, source)
    return True


# ============================================================================
# HUMAN ACTIVITY (manual messages only → pause bot, sync, schedule resume)
# ============================================================================


@app.post("/webhook/human-activity")
@app.post("/webhook/{entity_ref}/human-activity")
async def human_activity_webhook(request: Request, entity_ref: str = ""):
    """Handle human staff manual messages.

    GHL fires this when a staff member sends a manual message.
    Python adds 'stop bot' tag (if toggle enabled), schedules timed removal,
    syncs conversations, and pushes reactivation timer forward.

    Call events are routed to /call-event instead.
    """
    try:
        body = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"error": "Invalid JSON"})

    contact_id = body.get("contact_id", "")
    if not contact_id:
        return JSONResponse(status_code=400, content={"error": "Missing contact_id"})

    try:
        entity_id, config = await _resolve_entity_for_webhook(
            entity_ref=entity_ref,
            body=body,
            source="human-activity",
        )
    except ValueError:
        raise HTTPException(status_code=404, detail=f"Entity {entity_ref} not found")
    except RuntimeError:
        raise HTTPException(status_code=409, detail="Multiple active entities match this location ID")

    slug = config.get("slug") or config.get("name", entity_id[:8])
    set_request_context(entity_id=entity_id, entity_slug=slug, trigger_type="human_activity", contact_id=contact_id)

    try:
        asyncio.create_task(_handle_human_activity(entity_id, contact_id, config, body))
        logger.info("HUMAN_ACTIVITY | accepted | contact=%s | entity=%s", contact_id, entity_id[:8])
        return {"status": "accepted", "contact_id": contact_id}
    except Exception as e:
        logger.exception("HUMAN_ACTIVITY | error | contact=%s", contact_id)
        return JSONResponse(status_code=500, content={"error": True, "message": str(e)[:200]})
    finally:
        clear_request_context()


async def _handle_human_activity(
    entity_id: str,
    contact_id: str,
    config: dict,
    body: dict | None = None,
) -> None:
    """Background task: pause bot (if toggle enabled), sync, push reactivation."""
    from app.services.workflow_tracker import WorkflowTracker

    set_request_context(entity_id=entity_id, trigger_type="human_activity", contact_id=contact_id)

    tracker = WorkflowTracker(
        "human_activity",
        entity_id=entity_id,
        ghl_contact_id=contact_id,
        trigger_source="webhook",
    )

    bot_paused = False
    sync_stats = None
    reactivation_rescheduled = False

    try:
        from app.services.ghl_client import GHLClient
        ghl = GHLClient(api_key=config.get("ghl_api_key", ""), location_id=config.get("ghl_location_id", ""))

        # 1. Pause bot (checks toggle internally)
        try:
            bot_paused = await _pause_bot(contact_id, config, ghl, source="manual_message")
        except Exception as e:
            logger.warning("HUMAN_ACTIVITY | pause_bot failed: %s", e)

        # 2. Sync conversations from GHL
        chat_table = config.get("chat_history_table_name", "")
        if chat_table:
            try:
                from app.text_engine.conversation_sync import run_conversation_sync
                sync_stats = await run_conversation_sync(
                    contact_id=contact_id, ghl=ghl, chat_table=chat_table,
                    entity_id=config.get("id", entity_id), config=config,
                    workflow_run_id=tracker.run_id,
                )
                logger.info("HUMAN_ACTIVITY | conversation synced | contact=%s", contact_id)
            except Exception as e:
                logger.warning("HUMAN_ACTIVITY | sync failed: %s", e)

        # 3. Push reactivation timer forward
        try:
            from app.workflows.reactivation_scheduler import reschedule_reactivation
            await reschedule_reactivation(entity_id, contact_id)
            reactivation_rescheduled = True
        except Exception:
            pass

    except Exception as e:
        tracker.set_error(str(e))
        logger.exception("HUMAN_ACTIVITY | background task failed")
        try:
            await notify_error(e, source="background: _handle_human_activity", stage=_classify_error_stage(e), context=get_request_context())
        except Exception:
            pass
    finally:
        # Workflow tracker — decisions + runtime context
        tracker.set_decisions({
            "bot_paused": bot_paused,
            "pause_toggle_enabled": (config.get("system_config") or {}).get("pause_bot_on_human_activity", True),
            "takeover_minutes": (config.get("system_config") or {}).get("human_takeover_minutes", _DEFAULT_HUMAN_TAKEOVER_MINUTES),
            "reactivation_rescheduled": reactivation_rescheduled,
            "sync": {
                "messages_synced": (sync_stats or {}).get("messages_synced", 0),
                "calls_synced": (sync_stats or {}).get("calls_synced", 0),
            },
        })
        tracker.set_runtime_context({
            "contact": {
                "name": (body or {}).get("full_name", ""),
                "phone": (body or {}).get("phone", ""),
                "email": (body or {}).get("email", ""),
            },
            "source": "manual_message",
        })
        tracker.set_system_config(config.get("system_config"), setter_key=None)
        tracker.set_metadata("channel", "SMS")
        tracker.set_metadata("result_path", "bot_paused" if bot_paused else "toggle_off")
        await tracker.save()
        clear_request_context()


# ============================================================================
# DAILY REPORTS (scheduled + manual trigger)
# ============================================================================


@app.post("/workflows/daily-reports")
async def daily_reports_trigger(dry_run: bool = False):
    """Manual trigger for daily reports.

    Runs the full daily reports workflow synchronously and returns the summary.
    Useful for testing or re-running after a failure.
    Pass ?dry_run=true to skip Slack notifications.

    Migrated from n8n workflow LYGmrAGdeAIZMrJr (33 executions).
    Normally runs automatically at 8:45 AM America/Chicago via the scheduler.
    """
    set_request_context(entity_id="system", trigger_type="daily-reports")
    try:
        run_tags = ["type:daily-reports"]
        with prefect_tags(*run_tags):
            result = await run_daily_reports.with_options(
                name="daily-reports"
            )(dry_run=dry_run)
        return result
    except Exception as e:
        logger.exception("Daily reports failed")
        try:
            await notify_error(e, source="POST /workflows/daily-reports", stage=_classify_error_stage(e), context=get_request_context())
        except Exception:
            pass
        raise HTTPException(status_code=500, detail="Daily reports failed")
    finally:
        clear_request_context()


# ── Manual Message Learning ───────────────────────────────────────────────
# Daily knowledge gap detection from staff messages and call transcripts.
# Normally runs automatically at 9:00 AM America/Chicago via the scheduler.


@app.post("/workflows/manual-message-learning")
async def manual_message_learning_trigger(
    dry_run: bool = False,
    call_mode: str = "transcript",
    model: str = "google/gemini-2.5-flash",
    entity_id: str | None = None,
):
    """Analyze staff messages/calls for knowledge gaps.

    Runs the manual message learning workflow and returns findings summary.
    Pass ?dry_run=true to skip Slack notifications and include detailed results.
    Pass ?call_mode=summary|transcript|both to control call data sent to agent.
    Pass ?model=anthropic/claude-sonnet-4-6 (or other) to override the default model.
    Pass ?entity_id=xxx to process only one client.
    """
    set_request_context(entity_id=entity_id or "system", trigger_type="manual-message-learning")
    try:
        run_tags = ["type:manual-message-learning"]
        with prefect_tags(*run_tags):
            result = await run_manual_learning.with_options(
                name="manual-message-learning"
            )(dry_run=dry_run, call_mode=call_mode, model=model, entity_id=entity_id)
        return result
    except Exception as e:
        logger.exception("Manual message learning failed")
        try:
            await notify_error(e, source="POST /workflows/manual-message-learning", stage=_classify_error_stage(e), context=get_request_context())
        except Exception:
            pass
        raise HTTPException(status_code=500, detail="Manual message learning failed")
    finally:
        clear_request_context()


@app.post("/workflows/manual-message-learning/test")
async def manual_message_learning_test(body: ManualLearningTestBody):
    """Test the learning agent with inline synthetic data — no database writes.

    Accepts messages and calls directly in the request body. Loads real prompts
    and searches the real KB for the given entity_id. Returns findings + token usage.
    """
    from app.text_engine.data_loading import _build_prompts

    set_request_context(entity_id=body.client_id, trigger_type="ml-test")
    try:
        # Load real client prompts
        full_config = await supabase._get_from_clients(body.client_id)
        if not full_config:
            raise HTTPException(status_code=404, detail=f"Client {body.client_id} not found")

        prompts = _build_prompts(full_config, is_test_mode=False)
        client_name = full_config.get("name", "Unknown")

        # Format messages to match expected structure
        formatted_msgs = [
            {
                "content": m.get("content", ""),
                "timestamp": m.get("timestamp", ""),
                "session_id": m.get("session_id", "test"),
                "channel": m.get("channel", "SMS"),
            }
            for m in body.messages
        ]
        formatted_calls = [
            {
                "summary": c.get("summary", ""),
                "transcript": c.get("transcript", ""),
                "direction": c.get("direction", "inbound"),
                "status": c.get("status", "completed"),
                "duration_seconds": c.get("duration_seconds", 0),
                "user_sentiment": c.get("user_sentiment", ""),
                "created_at": c.get("created_at", ""),
            }
            for c in body.calls
        ]

        tracker = WorkflowTokenTracker()
        findings, kb_searches = await _run_learning_agent(
            manual_messages=formatted_msgs,
            call_logs=formatted_calls,
            prompts=prompts,
            entity_id=body.client_id,
            client_name=client_name,
            call_mode=body.call_mode,
            model=body.model,
            tracker=tracker,
        )

        # Deterministic KB content filter + LLM validation
        from app.workflows.manual_message_learning import (
            _get_kb_articles, _deterministic_kb_content_filter,
        )
        kb_articles = await _get_kb_articles(body.client_id)
        findings, kb_removed = _deterministic_kb_content_filter(findings, kb_articles)

        # Second-pass LLM validation — filter out remaining false positives
        pre_validation = len(findings) + kb_removed
        findings = await _validate_findings(
            findings, prompts, model=body.model, tracker=tracker,
        )

        return {
            "client": client_name,
            "findings": findings,
            "findings_count": len(findings),
            "pre_validation_count": pre_validation,
            "validation_removed": pre_validation - len(findings),
            "kb_searches": kb_searches,
            "model": body.model,
            "messages_count": len(formatted_msgs),
            "calls_count": len(formatted_calls),
            "token_usage": tracker.summary(),
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Manual learning test failed")
        try:
            await notify_error(e, source="POST /workflows/manual-message-learning/test", stage=_classify_error_stage(e), context=get_request_context())
        except Exception:
            pass
        raise HTTPException(status_code=500, detail="Manual learning test failed")
    finally:
        clear_request_context()


# ── Update Knowledge Base ──────────────────────────────────────────────────
# Migrated from n8n workflow cKtsf87a7Cmwb8lf (20 executions).
# Handles the embedding pipeline: chunk content → OpenAI embeddings → insert
# into documents vector store table. Called by dashboard after KB article upsert.


@app.post("/webhook/{entity_id}/update-kb")
async def update_kb_webhook(entity_id: str, body: UpdateKBBody):
    """Update knowledge base embeddings for a client.

    Payload: {title, content, tag, action?}
    - action "delete": removes embeddings for tag + client
    - action "upsert" (default): deletes old + chunks + embeds + inserts new
    """
    set_request_context(entity_id=entity_id, trigger_type="update-kb")
    try:
        run_tags = ["type:update-kb", f"client:{entity_id}"]
        with prefect_tags(*run_tags):
            result = await process_kb_update.with_options(
                name="update-kb"
            )(
                entity_id=entity_id,
                title=body.title,
                content=body.content,
                tag=body.tag,
                action=body.action,
                article_id=body.kb_id or None,
            )
        return result
    except Exception as e:
        logger.exception("Update KB failed")
        try:
            await notify_error(e, source=f"POST /webhook/{entity_id}/update-kb", stage=_classify_error_stage(e), context=get_request_context())
        except Exception:
            pass
        raise HTTPException(status_code=500, detail="Update KB failed")
    finally:
        clear_request_context()


# ── Clean Chat History ─────────────────────────────────────────────────────
# Migrated from n8n workflow MSkPb2pJ4Cd7KYD8 (13 executions).
# Deletes the most recent AI message from a contact's chat history.
# Called from the dashboard to undo the last AI response.


@app.post("/webhook/{entity_id}/clean-chat-history")
async def clean_chat_history_webhook(entity_id: str, body: CleanChatHistoryBody):
    """Delete the most recent AI message for a contact.

    Payload: {id, name?, email?, phone?, entityId?}
    - id: GHL contact ID whose latest AI message will be deleted
    - entityId: entity UUID (body preferred, URL path {entity_id} is fallback)
    """
    resolved_client = body.entityId or entity_id
    contact_id = body.id
    set_request_context(entity_id=resolved_client, trigger_type="clean-chat-history")
    try:
        run_tags = ["type:clean-chat-history", f"client:{resolved_client}", f"contact:{contact_id}"]
        with prefect_tags(*run_tags):
            result = await clean_chat_history.with_options(
                name="clean-chat-history"
            )(entity_id=resolved_client, contact_id=contact_id)
        return result
    except Exception as e:
        logger.exception("Clean chat history failed")
        try:
            await notify_error(e, source=f"POST /webhook/{resolved_client}/clean-chat-history", stage=_classify_error_stage(e), context=get_request_context())
        except Exception:
            pass
        raise HTTPException(status_code=500, detail="Clean chat history failed")
    finally:
        clear_request_context()


# ── AI Prompt Generator ──────────────────────────────────────────


@app.post("/ai/generate-prompt")
async def ai_generate_prompt(body: dict):
    """Generate or refine a prompt section using AI (sparkle button)."""
    from app.services.prompt_generator import (
        GeneratePromptRequest,
        generate_prompt,
    )

    req = GeneratePromptRequest(**body)
    result = await generate_prompt(req)
    return {"generated_prompt": result.generated_prompt}
