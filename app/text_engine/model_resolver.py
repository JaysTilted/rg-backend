"""
Model & temperature resolver — resolves which AI model and temperature to use for each LLM call.

Resolution order (same for both model and temperature):
1. Client's system_config overrides (ai_models / ai_temperatures)
2. Tenant defaults from tenants.default_setter_config (cached with 5-min TTL)
3. Hardcoded fallback (CALL_DEFAULTS / TEMP_DEFAULTS)
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.models import PipelineContext

logger = logging.getLogger(__name__)

# ── Hardcoded fallback — full OpenRouter model IDs ──

CALL_DEFAULTS: dict[str, str] = {
    # All defaults route to openai/gpt-4.1 — the only model deployed on
    # ironclaw.openai.azure.com (Azure free tier). Any non-gpt-4.1 model
    # falls through to OpenRouter, which is not funded.
    # Text Engine — Reply
    "reply_agent": "openai/gpt-4.1",
    "reply_media": "openai/gpt-4.1",
    "transfer_detection": "openai/gpt-4.1",
    "response_determination": "openai/gpt-4.1",
    "contact_extraction": "openai/gpt-4.1",
    "stop_detection": "openai/gpt-4.1",
    "pipeline_management": "openai/gpt-4.1",
    "reply_security": "openai/gpt-4.1",
    "qualification": "openai/gpt-4.1",
    "email_formatting": "openai/gpt-4.1",
    "media_analysis": "openai/gpt-4.1",
    "link_synthesis": "openai/gpt-4.1",
    "call_summarization": "openai/gpt-4.1",

    # Text Engine — Follow-up
    "followup_text": "openai/gpt-4.1",
    "followup_media": "openai/gpt-4.1",
    "followup_determination": "openai/gpt-4.1",
    "smart_scheduler": "openai/gpt-4.1",
    "followup_security": "openai/gpt-4.1",
    "followup_email_formatting": "openai/gpt-4.1",

    # Missed Call Text-Back
    "missed_call_text": "openai/gpt-4.1",
    "missed_call_gate": "openai/gpt-4.1",

    # Reactivation
    "reactivation_sms": "openai/gpt-4.1",
    "reactivation_p1_only": "openai/gpt-4.1",
    "reactivation_qual": "openai/gpt-4.1",
    "service_matcher": "openai/gpt-4.1",
    "reactivation_security": "openai/gpt-4.1",
    "message_splitter": "openai/gpt-4.1",

    # Post-Appointment
    "post_appointment_determination": "openai/gpt-4.1",
    "post_appointment_generation": "openai/gpt-4.1",

    # AI Data Chat
    "data_chat": "openai/gpt-4.1",
}

# ── Hardcoded fallback — default temperatures ──

TEMP_DEFAULTS: dict[str, float] = {
    # Text Engine — Reply
    "reply_agent": 0.7,
    "reply_media": 0.2,
    "transfer_detection": 0.3,
    "response_determination": 0.3,
    "contact_extraction": 0.3,
    "stop_detection": 0.3,
    "pipeline_management": 0.3,
    "reply_security": 0.1,
    "qualification": 0.2,
    "email_formatting": 0.3,
    "media_analysis": 0.3,
    "link_synthesis": 0.4,
    "call_summarization": 0.3,

    # Text Engine — Follow-up
    "followup_text": 0.7,
    "followup_media": 0.3,
    "followup_determination": 0.3,
    "smart_scheduler": 0.3,
    "followup_security": 0.1,
    "followup_email_formatting": 0.3,

    # Missed Call Text-Back
    "missed_call_text": 1.2,
    "missed_call_gate": 0.3,

    # Reactivation
    "reactivation_sms": 0.5,
    "reactivation_p1_only": 0.5,
    "reactivation_qual": 0.1,
    "service_matcher": 0.1,
    "reactivation_security": 0.1,
    "message_splitter": 0.3,

    # Post-Appointment
    "post_appointment_determination": 0.3,
    "post_appointment_generation": 0.7,

    # AI Data Chat
    "data_chat": 0.3,
}

# ── Tenant defaults cache (keyed by tenant_id) ──
_tenant_models: dict[str, dict[str, str]] = {}  # tenant_id → {call_key: model_id}
_tenant_temps: dict[str, dict[str, float]] = {}  # tenant_id → {call_key: temperature}
_tenant_cache_ts: float = 0
_TENANT_CACHE_TTL = 300  # 5 minutes


async def refresh_defaults_if_stale() -> None:
    """Fetch tenant default AI models from tenants.default_setter_config (5-min TTL)."""
    global _tenant_models, _tenant_temps, _tenant_cache_ts

    if time.time() - _tenant_cache_ts < _TENANT_CACHE_TTL:
        return  # cache is fresh

    try:
        from app.services.supabase_client import supabase

        resp = await supabase.main_client.get(
            "/tenants",
            params={
                "select": "id,default_setter_config",
                "default_setter_config": "not.is.null",
            },
        )
        if resp.status_code == 200:
            rows = resp.json()
            new_models: dict[str, dict[str, str]] = {}
            new_temps: dict[str, dict[str, float]] = {}
            for row in rows:
                tid = row["id"]
                config = row.get("default_setter_config") or {}
                if config.get("ai_models"):
                    new_models[tid] = config["ai_models"]
                if config.get("ai_temperatures"):
                    new_temps[tid] = {
                        k: float(v) for k, v in config["ai_temperatures"].items()
                    }
            _tenant_models = new_models
            _tenant_temps = new_temps
            _tenant_cache_ts = time.time()
            logger.info(
                "Loaded tenant AI model defaults for %d tenant(s)", len(new_models)
            )
        else:
            logger.warning("Failed to load tenant defaults: %s", resp.status_code)
    except Exception:
        logger.exception("Error loading tenant AI model defaults")


def resolve_model_standalone(
    call_key: str,
    ai_models: dict[str, str] | None = None,
    tenant_id: str | None = None,
) -> str:
    """Resolve model without PipelineContext — for standalone workflows.

    Resolution: entity override → tenant default → hardcoded fallback.
    """
    if ai_models and call_key in ai_models:
        return ai_models[call_key]
    if tenant_id and tenant_id in _tenant_models:
        tenant_defaults = _tenant_models[tenant_id]
        if call_key in tenant_defaults:
            return tenant_defaults[call_key]
    return CALL_DEFAULTS.get(call_key, "google/gemini-2.5-flash")


def resolve_temperature_standalone(
    call_key: str,
    ai_temperatures: dict[str, float] | None = None,
    tenant_id: str | None = None,
) -> float:
    """Resolve temperature without PipelineContext."""
    if ai_temperatures and call_key in ai_temperatures:
        return float(ai_temperatures[call_key])
    if tenant_id and tenant_id in _tenant_temps:
        tenant_defaults = _tenant_temps[tenant_id]
        if call_key in tenant_defaults:
            return tenant_defaults[call_key]
    return TEMP_DEFAULTS.get(call_key, 0.3)


def resolve_model(ctx: "PipelineContext", call_key: str) -> str:
    """Resolve the model for a specific LLM call.

    Resolution: client override → tenant default → hardcoded fallback.
    """
    overrides = ctx.compiled.get("ai_models") or {}
    if call_key in overrides:
        return overrides[call_key]
    tenant_id = ctx.config.get("tenant_id")
    if tenant_id and tenant_id in _tenant_models:
        tenant_defaults = _tenant_models[tenant_id]
        if call_key in tenant_defaults:
            return tenant_defaults[call_key]
    return CALL_DEFAULTS.get(call_key, "google/gemini-2.5-flash")


def resolve_temperature(ctx: "PipelineContext", call_key: str) -> float:
    """Resolve the temperature for a specific LLM call.

    Resolution: client override → tenant default → hardcoded fallback.
    """
    overrides = ctx.compiled.get("ai_temperatures") or {}
    if call_key in overrides:
        return float(overrides[call_key])
    tenant_id = ctx.config.get("tenant_id")
    if tenant_id and tenant_id in _tenant_temps:
        tenant_defaults = _tenant_temps[tenant_id]
        if call_key in tenant_defaults:
            return tenant_defaults[call_key]
    return TEMP_DEFAULTS.get(call_key, 0.3)
