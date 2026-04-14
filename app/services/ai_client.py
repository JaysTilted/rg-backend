"""AI client — Azure/OpenAI direct for `openai/*` models, OpenRouter fallback.

Uses Azure GPT-4.1 as the primary route for `openai/*` models.
Uses OpenRouter for non-OpenAI models and as a fallback path.
Falls back to direct provider APIs when OpenRouter fails:
  - google/* → Google GenAI SDK (existing)
  - openai/* → OpenAI direct
  - anthropic/* → Anthropic SDK
  - deepseek/* → DeepSeek API (OpenAI-compatible)
  - x-ai/* → xAI API (OpenAI-compatible)
All classifiers use function calling / structured output for guaranteed valid JSON.
"""

from __future__ import annotations

import asyncio
import contextvars
import json
import logging
import time
from typing import Any

import httpx
import openai
from google import genai
from google.genai import types as genai_types

from app.config import settings

logger = logging.getLogger(__name__)

# Per-request context variables (set by pipeline, read by classify/chat/etc.)
_current_api_key: contextvars.ContextVar[str] = contextvars.ContextVar("_current_api_key", default="")
_current_token_tracker: contextvars.ContextVar[Any] = contextvars.ContextVar("_current_token_tracker", default=None)

# Per-request backup provider keys (multi-tenant support)
_current_google_key: contextvars.ContextVar[str] = contextvars.ContextVar("_current_google_key", default="")
_current_openai_key: contextvars.ContextVar[str] = contextvars.ContextVar("_current_openai_key", default="")
_current_deepseek_key: contextvars.ContextVar[str] = contextvars.ContextVar("_current_deepseek_key", default="")
_current_xai_key: contextvars.ContextVar[str] = contextvars.ContextVar("_current_xai_key", default="")
_current_anthropic_key: contextvars.ContextVar[str] = contextvars.ContextVar("_current_anthropic_key", default="")

# Retry config for classify() — matches n8n's retryOnFail pattern
_CLASSIFY_MAX_RETRIES = 3
_CLASSIFY_RETRY_DELAYS = [2, 5, 10]  # seconds between retries (exponential-ish)

# Google fallback — maps full OpenRouter model IDs to Google Gemini direct model IDs.
# Only Google-compatible models have entries. Non-Google models skip this fallback.
GOOGLE_FALLBACK: dict[str, str] = {
    "google/gemini-2.5-flash": "gemini-2.5-flash",
    "google/gemini-2.5-pro": "gemini-2.5-pro",
    "google/gemini-3.1-pro-preview": "gemini-3.1-pro-preview",
    "google/gemini-3.1-flash-lite-preview": "gemini-3.1-flash-lite-preview",
    "google/gemini-3-flash-preview": "gemini-3-flash-preview",
}

# Special Google model IDs used directly by media analysis (not configurable)
_GOOGLE_MEDIA_MODEL = "gemini-3.1-pro-preview"
_GOOGLE_MEDIA_FALLBACK = "gemini-3-flash-preview"

# Clients — initialized at startup
_openrouter: openai.AsyncOpenAI | None = None
_google: genai.Client | None = None
_openai_direct: openai.AsyncOpenAI | None = None
_deepseek_direct: openai.AsyncOpenAI | None = None
_xai_direct: openai.AsyncOpenAI | None = None
_anthropic_direct: Any | None = None  # anthropic.AsyncAnthropic


def start_ai_clients() -> None:
    """Initialize AI SDK clients. Call once at app startup."""
    global _openrouter, _google, _openai_direct, _deepseek_direct, _xai_direct, _anthropic_direct
    if settings.openrouter_api_key:
        _openrouter = openai.AsyncOpenAI(
            api_key=settings.openrouter_api_key,
            base_url="https://openrouter.ai/api/v1",
            timeout=60.0,
        )
    else:
        _openrouter = None
    if settings.google_gemini_api_key:
        _google = genai.Client(api_key=settings.google_gemini_api_key)
    else:
        _google = None
    if settings.openai_api_key:
        _openai_direct = openai.AsyncOpenAI(
            api_key=settings.openai_api_key,
            timeout=60.0,
        )
    if settings.deepseek_api_key:
        _deepseek_direct = openai.AsyncOpenAI(
            api_key=settings.deepseek_api_key,
            base_url="https://api.deepseek.com",
            timeout=60.0,
        )
    if settings.xai_api_key:
        _xai_direct = openai.AsyncOpenAI(
            api_key=settings.xai_api_key,
            base_url="https://api.x.ai/v1",
            timeout=60.0,
        )
    if settings.anthropic_api_key:
        try:
            import anthropic
            _anthropic_direct = anthropic.AsyncAnthropic(
                api_key=settings.anthropic_api_key,
                timeout=60.0,
            )
        except ImportError:
            logger.warning("anthropic SDK not installed — Anthropic direct fallback disabled")


# ── Direct provider model ID mappings ──
# Maps OpenRouter model IDs to direct provider model IDs

def _get_direct_model_id(model: str) -> str | None:
    """Convert OpenRouter model ID to direct provider model ID.
    Returns None if no direct mapping exists.
    """
    # Full OpenRouter ID → direct provider ID
    _DIRECT_MAPPINGS = {
        # OpenAI
        "openai/gpt-5.4": "gpt-5.4",
        "openai/gpt-5.4-mini": "gpt-5.4-mini",
        "openai/gpt-5.4-nano": "gpt-5.4-nano",
        "openai/gpt-4.1": "gpt-4.1",
        "openai/gpt-4.1-mini": "gpt-4.1-mini",
        "openai/gpt-4o": "gpt-4o",
        "openai/gpt-4o-mini": "gpt-4o-mini",
        # Anthropic
        "anthropic/claude-opus-4.6": "claude-opus-4-6",
        "anthropic/claude-sonnet-4.6": "claude-sonnet-4-6",
        "anthropic/claude-haiku-4.5": "claude-haiku-4-5-20251001",
        # DeepSeek
        "deepseek/deepseek-r1": "deepseek-reasoner",
        "deepseek/deepseek-v3.2": "deepseek-chat",
        # xAI
        "x-ai/grok-4": "grok-4.20-0309-reasoning",
        "x-ai/grok-4.1-fast": "grok-4-1-fast-non-reasoning",
    }
    return _DIRECT_MAPPINGS.get(model)


def _should_prefer_azure(model: str) -> bool:
    """Whether this model should execute against Azure before any fallback path.

    Only routes to Azure when the resolved model matches a model actually
    DEPLOYED on the Azure resource. On ironclaw.openai.azure.com only
    `gpt-4.1` is deployed; routing anything else (mini/nano variants, 4o,
    5.4) wastes ~8s per call in 404 retries before falling back.
    """
    if not settings.azure_openai_api_key:
        return False
    deployed = settings.azure_openai_model
    if not deployed:
        return False
    # Direct match (e.g. "gpt-4.1") or openrouter-style prefix ("openai/gpt-4.1")
    return model == deployed or model == f"openai/{deployed}"


async def _azure_chat_completion(
    *,
    model: str,
    messages: list[dict[str, Any]],
    temperature: float,
    label: str,
    tools: list[dict[str, Any]] | None = None,
    response_format: dict[str, Any] | None = None,
    max_tokens: int | None = None,
) -> openai.types.chat.ChatCompletion:
    """Call Azure OpenAI's v1 chat completions endpoint and parse to OpenAI types."""
    model_id = _get_direct_model_id(model) or model
    url = settings.azure_openai_base_url.rstrip("/") + "/chat/completions"
    payload: dict[str, Any] = {
        "model": model_id,
        "messages": messages,
        "temperature": temperature,
    }
    if tools:
        payload["tools"] = tools
    if response_format:
        payload["response_format"] = response_format
    if max_tokens is not None:
        payload["max_completion_tokens"] = max_tokens

    async with httpx.AsyncClient(timeout=60.0) as client:
        resp = await client.post(
            url,
            json=payload,
            headers={
                "api-key": settings.azure_openai_api_key,
                "Content-Type": "application/json",
            },
        )
        resp.raise_for_status()
        parsed = openai.types.chat.ChatCompletion.model_validate(resp.json())

    _track_usage(parsed, model=model_id, label=label, provider="azure_openai")
    return parsed


def _get_direct_client(model: str) -> tuple[openai.AsyncOpenAI | None, str | None]:
    """Get the direct provider client and model ID for a given OpenRouter model.

    Checks per-request ContextVar keys first (multi-tenant), falls back to global clients.
    Returns (client, direct_model_id) or (None, None) if unavailable.
    """
    direct_id = _get_direct_model_id(model)
    if not direct_id:
        return None, None

    provider = model.split("/")[0] if "/" in model else ""

    if provider == "openai":
        tenant_key = _current_openai_key.get("")
        if tenant_key and tenant_key != (settings.openai_api_key or ""):
            return _get_or_create_client(tenant_key, "https://api.openai.com/v1"), direct_id
        if _openai_direct:
            return _openai_direct, direct_id
    elif provider == "deepseek":
        tenant_key = _current_deepseek_key.get("")
        if tenant_key and tenant_key != (settings.deepseek_api_key or ""):
            return _get_or_create_client(tenant_key, "https://api.deepseek.com"), direct_id
        if _deepseek_direct:
            return _deepseek_direct, direct_id
    elif provider == "x-ai":
        tenant_key = _current_xai_key.get("")
        if tenant_key and tenant_key != (settings.xai_api_key or ""):
            return _get_or_create_client(tenant_key, "https://api.x.ai/v1"), direct_id
        if _xai_direct:
            return _xai_direct, direct_id
    # Anthropic handled separately (different SDK)
    return None, None


# Cache for per-tenant provider clients (keyed by prefix+key_start)
_provider_client_cache: dict[str, Any] = {}

# Cache for OpenRouter + direct provider clients (keyed by API key, avoids creating new clients per request)
_client_cache: dict[str, openai.AsyncOpenAI] = {}


def _get_google() -> genai.Client:
    """Get a Google GenAI client for the current request (tenant-aware).

    Uses per-request ContextVar key if set and different from global.
    """
    tenant_key = _current_google_key.get("")
    if tenant_key and tenant_key != (settings.google_gemini_api_key or ""):
        cache_key = f"google_{tenant_key[:8]}"
        if cache_key not in _provider_client_cache:
            _provider_client_cache[cache_key] = genai.Client(api_key=tenant_key)
        return _provider_client_cache[cache_key]
    if _google is None:
        raise RuntimeError("Google Gemini client not configured")
    return _google


def _get_anthropic() -> Any:
    """Get an Anthropic client for the current request (tenant-aware)."""
    tenant_key = _current_anthropic_key.get("")
    if tenant_key and tenant_key != (settings.anthropic_api_key or ""):
        cache_key = f"anthropic_{tenant_key[:8]}"
        if cache_key not in _provider_client_cache:
            try:
                import anthropic
                _provider_client_cache[cache_key] = anthropic.AsyncAnthropic(
                    api_key=tenant_key, timeout=60.0,
                )
            except ImportError:
                return _anthropic_direct
        return _provider_client_cache[cache_key]
    return _anthropic_direct


def _get_or_create_client(api_key: str, base_url: str | None = None) -> openai.AsyncOpenAI:
    """Get or create a cached OpenAI-compatible client for a given API key."""
    cache_key = f"{api_key[:8]}_{base_url or 'default'}"
    if cache_key not in _client_cache:
        kwargs: dict[str, Any] = {"api_key": api_key, "timeout": 60.0}
        if base_url:
            kwargs["base_url"] = base_url
        _client_cache[cache_key] = openai.AsyncOpenAI(**kwargs)
    return _client_cache[cache_key]


# =========================================================================
# PER-REQUEST AI CONTEXT (API key + token tracking)
# =========================================================================

def set_ai_context(
    api_key: str = "",
    token_tracker: Any = None,
    *,
    google_key: str = "",
    openai_key: str = "",
    deepseek_key: str = "",
    xai_key: str = "",
    anthropic_key: str = "",
) -> None:
    """Set per-request AI context. Called at pipeline entry point.

    api_key: OpenRouter key (primary)
    google_key..anthropic_key: backup provider keys (optional, for multi-tenant)
    """
    if api_key:
        _current_api_key.set(api_key)
    if token_tracker is not None:
        _current_token_tracker.set(token_tracker)
    # Backup provider keys (empty string = use global fallback)
    if google_key:
        _current_google_key.set(google_key)
    if openai_key:
        _current_openai_key.set(openai_key)
    if deepseek_key:
        _current_deepseek_key.set(deepseek_key)
    if xai_key:
        _current_xai_key.set(xai_key)
    if anthropic_key:
        _current_anthropic_key.set(anthropic_key)


def clear_ai_context() -> None:
    """Clear per-request AI context. Called at pipeline exit."""
    _current_api_key.set("")
    _current_token_tracker.set(None)
    _current_google_key.set("")
    _current_openai_key.set("")
    _current_deepseek_key.set("")
    _current_xai_key.set("")
    _current_anthropic_key.set("")


def _get_openrouter() -> openai.AsyncOpenAI:
    """Get an OpenRouter client for the current request's API key (cached).

    Resolution: contextvar key > global default client.
    """
    key = _current_api_key.get("")
    if not key or key == settings.openrouter_api_key:
        if _openrouter is None:
            raise RuntimeError("OpenRouter client not configured")
        return _openrouter

    if key not in _client_cache:
        _client_cache[key] = openai.AsyncOpenAI(
            api_key=key,
            base_url="https://openrouter.ai/api/v1",
            timeout=60.0,
        )
    return _client_cache[key]


def _extract_response_text(resp: Any) -> str:
    """Extract response text from an OpenAI-compatible response object.

    For tool-calling responses where content is empty, captures the tool call details.
    """
    try:
        if resp and resp.choices:
            msg = resp.choices[0].message
            content = msg.content or ""
            if content:
                return content
            # If no text content, check for tool calls
            tool_calls = getattr(msg, "tool_calls", None)
            if tool_calls:
                parts = []
                for tc in tool_calls:
                    fn = getattr(tc, "function", None)
                    if fn:
                        parts.append(f"[tool_call: {fn.name}({fn.arguments})]")
                return " ".join(parts) if parts else ""
    except Exception:
        pass
    return ""


def _track_usage(resp: Any, model: str = "", label: str = "", provider: str = "openrouter") -> None:
    """Record token usage from an OpenAI response if a tracker is active."""
    tracker = _current_token_tracker.get(None)
    if tracker is None:
        return
    usage = getattr(resp, "usage", None)
    if usage is None:
        return
    # OpenRouter returns exact billed cost in usage.cost (non-standard extension)
    actual_cost = getattr(usage, "cost", None)
    tracker.add(
        prompt=getattr(usage, "prompt_tokens", 0) or 0,
        completion=getattr(usage, "completion_tokens", 0) or 0,
        total=getattr(usage, "total_tokens", 0) or 0,
        model=model,
        label=label,
        actual_cost=actual_cost,
        provider=provider,
        response=_extract_response_text(resp),
    )


def _track_usage_google(resp: Any, model: str = "", label: str = "") -> None:
    """Record token usage from a Google Gemini response (calculated cost, no markup)."""
    tracker = _current_token_tracker.get(None)
    if tracker is None:
        return
    meta = getattr(resp, "usage_metadata", None)
    if meta is None:
        return
    prompt = getattr(meta, "prompt_token_count", 0) or 0
    completion = getattr(meta, "candidates_token_count", 0) or 0
    # Google model name without google/ prefix for _MODEL_PRICING lookup
    full_model = f"google/{model}" if not model.startswith("google/") else model
    # Extract Google response text
    response_text = ""
    try:
        response_text = resp.text or ""
    except Exception:
        pass
    tracker.add(
        prompt=prompt,
        completion=completion,
        total=prompt + completion,
        model=full_model,
        label=f"{label} (google_fallback)",
        provider="google_direct",
        response=response_text,
        # actual_cost=None → uses _MODEL_PRICING without OpenRouter markup
    )


# =========================================================================
# CLASSIFY — structured output via function calling
# =========================================================================


async def classify(
    prompt: str,
    schema: dict[str, Any],
    model: str = "flash",
    temperature: float = 0.3,
    system_prompt: str | None = None,
    label: str = "classify",
) -> dict[str, Any]:
    """Run a classification prompt and return structured JSON.

    Uses function calling to guarantee valid JSON matching the schema.
    Retries each provider up to _CLASSIFY_MAX_RETRIES before falling back.

    Args:
        prompt: The user/input prompt to classify.
        schema: JSON schema for the expected response structure.
        model: Model alias ("flash" or "pro").
        temperature: Sampling temperature.
        system_prompt: Optional system prompt.
        label: Human-readable label for logging (e.g. "extraction", "stop_followup").

    Returns:
        Parsed dict matching the schema.
    """
    # Log the full prompt so we can see exactly what the LLM receives
    logger.info(
        "CLASSIFY [%s] | model=%s temp=%s | system_prompt_len=%d | prompt_len=%d",
        label, model, temperature,
        len(system_prompt) if system_prompt else 0,
        len(prompt),
    )
    logger.debug("CLASSIFY [%s] SYSTEM:\n%s", label, system_prompt or "(none)")
    logger.debug("CLASSIFY [%s] PROMPT:\n%s", label, prompt)

    last_error: Exception | None = None

    if _should_prefer_azure(model):
        for attempt in range(_CLASSIFY_MAX_RETRIES):
            try:
                messages = []
                if system_prompt:
                    messages.append({"role": "system", "content": system_prompt})
                messages.append({"role": "user", "content": prompt})
                resp = await _azure_chat_completion(
                    model=model,
                    messages=messages,
                    temperature=temperature,
                    label=label,
                    response_format={
                        "type": "json_schema",
                        "json_schema": {
                            "name": "classify",
                            "strict": True,
                            "schema": schema,
                        },
                    },
                )
                content = resp.choices[0].message.content or "{}"
                result = json.loads(content)
                logger.info("CLASSIFY [%s] RESULT (Azure, attempt %d): %s", label, attempt + 1, result)
                return result
            except Exception as e:
                last_error = e
                if attempt < _CLASSIFY_MAX_RETRIES - 1:
                    delay = _CLASSIFY_RETRY_DELAYS[min(attempt, len(_CLASSIFY_RETRY_DELAYS) - 1)]
                    logger.warning(
                        "CLASSIFY [%s] Azure attempt %d/%d failed: %s — retrying in %ds",
                        label, attempt + 1, _CLASSIFY_MAX_RETRIES, e, delay,
                    )
                    await asyncio.sleep(delay)
                else:
                    logger.warning(
                        "CLASSIFY [%s] Azure exhausted %d retries: %s — falling back to OpenRouter/direct",
                        label, _CLASSIFY_MAX_RETRIES, e,
                    )

    # Try OpenRouter with retries
    for attempt in range(_CLASSIFY_MAX_RETRIES):
        try:
            result = await _classify_openrouter(prompt, schema, model, temperature, system_prompt, label)
            logger.info("CLASSIFY [%s] RESULT (OpenRouter, attempt %d): %s", label, attempt + 1, result)
            return result
        except Exception as e:
            last_error = e
            if attempt < _CLASSIFY_MAX_RETRIES - 1:
                delay = _CLASSIFY_RETRY_DELAYS[min(attempt, len(_CLASSIFY_RETRY_DELAYS) - 1)]
                logger.warning(
                    "CLASSIFY [%s] OpenRouter attempt %d/%d failed: %s — retrying in %ds",
                    label, attempt + 1, _CLASSIFY_MAX_RETRIES, e, delay,
                )
                await asyncio.sleep(delay)
            else:
                logger.warning(
                    "CLASSIFY [%s] OpenRouter exhausted %d retries: %s — falling back to Google",
                    label, _CLASSIFY_MAX_RETRIES, e,
                )

    # Fallback: try Google (for Google models) or direct provider (for others)
    resolved_model = model  # Resolve alias to full ID

    if model in GOOGLE_FALLBACK:
        # Google Gemini fallback (existing path)
        for attempt in range(_CLASSIFY_MAX_RETRIES):
            try:
                result = await _classify_google(prompt, schema, model, temperature, system_prompt, label)
                logger.info("CLASSIFY [%s] RESULT (Google, attempt %d): %s", label, attempt + 1, result)
                return result
            except Exception as e:
                last_error = e
                if attempt < _CLASSIFY_MAX_RETRIES - 1:
                    delay = _CLASSIFY_RETRY_DELAYS[min(attempt, len(_CLASSIFY_RETRY_DELAYS) - 1)]
                    logger.warning(
                        "CLASSIFY [%s] Google attempt %d/%d failed: %s — retrying in %ds",
                        label, attempt + 1, _CLASSIFY_MAX_RETRIES, e, delay,
                    )
                    await asyncio.sleep(delay)
                else:
                    logger.error("CLASSIFY [%s] ALL retries exhausted (OpenRouter + Google): %s", label, e)
    else:
        # Direct provider fallback (OpenAI, DeepSeek, xAI via OpenAI SDK; Anthropic via own SDK)
        direct_client, direct_id = _get_direct_client(resolved_model)
        if direct_client and direct_id:
            logger.warning("CLASSIFY [%s] falling back to direct provider for %s", label, resolved_model)
            for attempt in range(_CLASSIFY_MAX_RETRIES):
                try:
                    result = await _classify_direct(direct_client, direct_id, prompt, schema, temperature, system_prompt, label)
                    logger.info("CLASSIFY [%s] RESULT (direct %s, attempt %d): %s", label, resolved_model, attempt + 1, result)
                    return result
                except Exception as e:
                    last_error = e
                    if attempt < _CLASSIFY_MAX_RETRIES - 1:
                        delay = _CLASSIFY_RETRY_DELAYS[min(attempt, len(_CLASSIFY_RETRY_DELAYS) - 1)]
                        logger.warning("CLASSIFY [%s] direct attempt %d/%d failed: %s — retrying in %ds", label, attempt + 1, _CLASSIFY_MAX_RETRIES, e, delay)
                        await asyncio.sleep(delay)
                    else:
                        logger.error("CLASSIFY [%s] ALL retries exhausted (OpenRouter + direct): %s", label, e)
        elif resolved_model.startswith("anthropic/") and _get_anthropic():
            logger.warning("CLASSIFY [%s] falling back to Anthropic direct for %s", label, resolved_model)
            for attempt in range(_CLASSIFY_MAX_RETRIES):
                try:
                    result = await _classify_anthropic(prompt, schema, resolved_model, temperature, system_prompt, label)
                    logger.info("CLASSIFY [%s] RESULT (Anthropic direct, attempt %d): %s", label, attempt + 1, result)
                    return result
                except Exception as e:
                    last_error = e
                    if attempt < _CLASSIFY_MAX_RETRIES - 1:
                        delay = _CLASSIFY_RETRY_DELAYS[min(attempt, len(_CLASSIFY_RETRY_DELAYS) - 1)]
                        logger.warning("CLASSIFY [%s] Anthropic attempt %d/%d failed: %s — retrying in %ds", label, attempt + 1, _CLASSIFY_MAX_RETRIES, e, delay)
                        await asyncio.sleep(delay)
                    else:
                        logger.error("CLASSIFY [%s] ALL retries exhausted (OpenRouter + Anthropic): %s", label, e)
        else:
            logger.warning("CLASSIFY [%s] No direct fallback for model '%s' — raising last error", label, resolved_model)

    raise last_error or RuntimeError(f"classify [{label}] failed with no error captured")


async def _classify_openrouter(
    prompt: str,
    schema: dict[str, Any],
    model: str,
    temperature: float,
    system_prompt: str | None,
    label: str = "classify",
) -> dict[str, Any]:
    """Classify via OpenRouter using response_format (structured JSON output).

    Uses response_format instead of function calling to avoid string truncation
    that occurs intermittently with Gemini models via tool call arguments.
    """
    messages = []
    if system_prompt:
        messages.append({"role": "system", "content": system_prompt})
    messages.append({"role": "user", "content": prompt})

    resp = await _get_openrouter().chat.completions.create(
        model=model,
        messages=messages,
        temperature=temperature,
        response_format={
            "type": "json_schema",
            "json_schema": {
                "name": "classify",
                "strict": True,
                "schema": schema,
            },
        },
    )
    _track_usage(resp, model=model, label=label)

    content = resp.choices[0].message.content or ""
    return json.loads(content)


def _strip_additional_properties(schema: dict[str, Any]) -> dict[str, Any]:
    """Deep-strip additionalProperties from a schema for Google Gemini compatibility.

    Google Gemini's response_schema rejects additionalProperties. OpenRouter
    requires it for strict mode. This strips it only for the Google path.
    """
    cleaned = {k: v for k, v in schema.items() if k != "additionalProperties"}
    if "properties" in cleaned:
        cleaned["properties"] = {
            k: _strip_additional_properties(v) if isinstance(v, dict) else v
            for k, v in cleaned["properties"].items()
        }
    if "items" in cleaned and isinstance(cleaned["items"], dict):
        cleaned["items"] = _strip_additional_properties(cleaned["items"])
    return cleaned


async def _classify_google(
    prompt: str,
    schema: dict[str, Any],
    model: str,
    temperature: float,
    system_prompt: str | None,
    label: str = "classify",
) -> dict[str, Any]:
    """Classify via Google Gemini using response_schema (structured JSON output).

    Uses response_schema instead of function calling to match the OpenRouter path
    and avoid string truncation. Strips additionalProperties which Google rejects.
    """
    clean_schema = _strip_additional_properties(schema)

    config = genai_types.GenerateContentConfig(
        temperature=temperature,
        response_mime_type="application/json",
        response_schema=clean_schema,
    )

    contents = []
    if system_prompt:
        config.system_instruction = system_prompt
    contents.append(prompt)

    resp = await _get_google().aio.models.generate_content(
        model=GOOGLE_FALLBACK.get(model, model),
        contents=contents,
        config=config,
    )
    _track_usage_google(resp, model=GOOGLE_FALLBACK.get(model, model), label=label)

    content = resp.candidates[0].content.parts[0].text
    return json.loads(content)


async def _classify_direct(
    client: openai.AsyncOpenAI,
    model_id: str,
    prompt: str,
    schema: dict[str, Any],
    temperature: float,
    system_prompt: str | None,
    label: str = "classify",
) -> dict[str, Any]:
    """Classify via direct provider API (OpenAI, DeepSeek, xAI — all OpenAI-compatible)."""
    messages = []
    if system_prompt:
        messages.append({"role": "system", "content": system_prompt})
    messages.append({"role": "user", "content": prompt})

    resp = await client.chat.completions.create(
        model=model_id,
        messages=messages,
        temperature=temperature,
        response_format={
            "type": "json_schema",
            "json_schema": {
                "name": "classify",
                "strict": True,
                "schema": schema,
            },
        },
    )
    _track_usage(resp, model=model_id, label=label, provider="direct_provider")

    content = resp.choices[0].message.content or "{}"
    return json.loads(content)


async def _classify_anthropic(
    prompt: str,
    schema: dict[str, Any],
    model: str,
    temperature: float,
    system_prompt: str | None,
    label: str = "classify",
) -> dict[str, Any]:
    """Classify via Anthropic direct API."""
    direct_id = _get_direct_model_id(model) or "claude-sonnet-4-6-20250514"

    messages = [{"role": "user", "content": prompt}]
    kwargs: dict[str, Any] = {
        "model": direct_id,
        "max_tokens": 8192,  # Required by Anthropic API (cannot be omitted)
        "temperature": temperature,
        "messages": messages,
    }
    if system_prompt:
        kwargs["system"] = system_prompt

    resp = await _get_anthropic().messages.create(**kwargs)

    # Extract text content
    content = ""
    for block in resp.content:
        if hasattr(block, "text"):
            content = block.text
            break

    # Track usage
    tracker = _current_token_tracker.get()
    if tracker and hasattr(resp, "usage"):
        tracker.add(
            prompt=getattr(resp.usage, "input_tokens", 0) or 0,
            completion=getattr(resp.usage, "output_tokens", 0) or 0,
            total=(getattr(resp.usage, "input_tokens", 0) or 0) + (getattr(resp.usage, "output_tokens", 0) or 0),
            model=direct_id,
            label=label,
            provider="anthropic_direct",
            response=content,
        )

    return json.loads(content)


# =========================================================================
# CHAT — multi-turn agent response with tool calling
# =========================================================================


async def chat(
    messages: list[dict[str, Any]],
    tools: list[dict[str, Any]] | None = None,
    model: str = "pro",
    temperature: float = 0.7,
    response_format: dict[str, Any] | None = None,
    label: str = "chat",
) -> openai.types.chat.ChatCompletion:
    """Multi-turn chat completion with optional tool calling.

    Used by the agent tool-calling loop where the full message history
    (system + user + assistant + tool results) needs to be sent each turn.

    Primary: OpenRouter with 3 retries + exponential backoff.
    Fallback: Google Gemini with 3 retries (initial call only — mid-loop
    failures re-raise for Prefect task retry).
    """
    t0 = time.perf_counter()
    msg_count = len(messages)
    tool_count = len(tools) if tools else 0
    logger.info("CHAT | model=%s | temp=%s | messages=%d | tools=%d", model, temperature, msg_count, tool_count)

    last_error: Exception | None = None

    if _should_prefer_azure(model):
        for attempt in range(_CLASSIFY_MAX_RETRIES):
            try:
                resp = await _azure_chat_completion(
                    model=model,
                    messages=messages,
                    temperature=temperature,
                    label=label,
                    tools=tools,
                    response_format=response_format,
                )
                elapsed_ms = int((time.perf_counter() - t0) * 1000)
                finish = resp.choices[0].finish_reason if resp.choices else "unknown"
                has_tool_calls = bool(resp.choices and resp.choices[0].message.tool_calls)
                logger.info(
                    "CHAT | done | provider=azure_openai | attempt=%d | elapsed_ms=%d | finish=%s | has_tool_calls=%s",
                    attempt + 1, elapsed_ms, finish, has_tool_calls,
                )
                return resp
            except Exception as e:
                last_error = e
                if attempt < _CLASSIFY_MAX_RETRIES - 1:
                    delay = _CLASSIFY_RETRY_DELAYS[min(attempt, len(_CLASSIFY_RETRY_DELAYS) - 1)]
                    logger.warning(
                        "CHAT | Azure attempt %d/%d failed: %s — retrying in %ds",
                        attempt + 1, _CLASSIFY_MAX_RETRIES, e, delay,
                    )
                    await asyncio.sleep(delay)
                else:
                    logger.warning(
                        "CHAT | Azure exhausted %d retries: %s",
                        _CLASSIFY_MAX_RETRIES, e,
                    )

    # Try OpenRouter with retries
    for attempt in range(_CLASSIFY_MAX_RETRIES):
        try:
            kwargs: dict[str, Any] = {
                "model": model,
                "messages": messages,
                "temperature": temperature,
            }
            if tools:
                kwargs["tools"] = tools
            if response_format:
                kwargs["response_format"] = response_format
            resp = await _get_openrouter().chat.completions.create(**kwargs)
            _track_usage(resp, model=model, label=label)
            elapsed_ms = int((time.perf_counter() - t0) * 1000)
            finish = resp.choices[0].finish_reason if resp.choices else "unknown"
            has_tool_calls = bool(resp.choices and resp.choices[0].message.tool_calls)
            logger.info("CHAT | done | provider=openrouter | attempt=%d | elapsed_ms=%d | finish=%s | has_tool_calls=%s", attempt + 1, elapsed_ms, finish, has_tool_calls)
            return resp
        except Exception as e:
            last_error = e
            if attempt < _CLASSIFY_MAX_RETRIES - 1:
                delay = _CLASSIFY_RETRY_DELAYS[min(attempt, len(_CLASSIFY_RETRY_DELAYS) - 1)]
                logger.warning(
                    "CHAT | OpenRouter attempt %d/%d failed: %s — retrying in %ds",
                    attempt + 1, _CLASSIFY_MAX_RETRIES, e, delay,
                )
                await asyncio.sleep(delay)
            else:
                logger.warning(
                    "CHAT | OpenRouter exhausted %d retries: %s",
                    _CLASSIFY_MAX_RETRIES, e,
                )

    # Google fallback only for initial call (system + user = 2 messages)
    # Mid-loop failures re-raise — converting full tool-call message history
    # between providers is fragile, so we let the Prefect task restart instead.
    if len(messages) > 2:
        raise last_error or RuntimeError("chat() OpenRouter failed with no error captured")

    # Try direct provider fallback for non-Google models
    resolved_model = model
    if model not in GOOGLE_FALLBACK:
        direct_client, direct_id = _get_direct_client(resolved_model)
        if direct_client and direct_id:
            logger.warning("CHAT | falling back to direct provider for %s (initial call)", resolved_model)
            system = next((m["content"] for m in messages if m["role"] == "system"), "")
            user = next((m["content"] for m in messages if m["role"] == "user"), "")
            for attempt in range(_CLASSIFY_MAX_RETRIES):
                try:
                    # Use OpenAI-compatible chat (same SDK, different base URL)
                    resp = await direct_client.chat.completions.create(
                        model=direct_id,
                        messages=messages[:2],  # system + user only
                        tools=tools if tools else openai.NOT_GIVEN,
                        temperature=temperature,
                        **({"response_format": response_format} if response_format else {}),
                    )
                    _prov = (resolved_model.split("/")[0] + "_direct") if "/" in resolved_model else (resolved_model + "_direct")
                    _track_usage(resp, model=direct_id, label=label, provider=_prov)
                    return resp
                except Exception as e:
                    last_error = e
                    if attempt < _CLASSIFY_MAX_RETRIES - 1:
                        delay = _CLASSIFY_RETRY_DELAYS[min(attempt, len(_CLASSIFY_RETRY_DELAYS) - 1)]
                        logger.warning("CHAT | direct %s attempt %d/%d failed: %s — retrying in %ds", resolved_model, attempt + 1, _CLASSIFY_MAX_RETRIES, e, delay)
                        await asyncio.sleep(delay)
        logger.warning("CHAT | No fallback available for model '%s' — raising last error", resolved_model)
        raise last_error or RuntimeError("chat() OpenRouter failed with no error captured")

    logger.warning("CHAT | falling back to Google Gemini (initial call)")
    system = next((m["content"] for m in messages if m["role"] == "system"), "")
    user = next((m["content"] for m in messages if m["role"] == "user"), "")

    for attempt in range(_CLASSIFY_MAX_RETRIES):
        try:
            resp = await _generate_google(system, user, tools, model, temperature, response_format, label=label)
            elapsed_ms = int((time.perf_counter() - t0) * 1000)
            finish = resp.choices[0].finish_reason if resp.choices else "unknown"
            has_tool_calls = bool(resp.choices and resp.choices[0].message.tool_calls)
            logger.info("CHAT | done | provider=google_fallback | attempt=%d | elapsed_ms=%d | finish=%s | has_tool_calls=%s", attempt + 1, elapsed_ms, finish, has_tool_calls)
            return resp
        except Exception as e:
            last_error = e
            if attempt < _CLASSIFY_MAX_RETRIES - 1:
                delay = _CLASSIFY_RETRY_DELAYS[min(attempt, len(_CLASSIFY_RETRY_DELAYS) - 1)]
                logger.warning(
                    "CHAT | Google attempt %d/%d failed: %s — retrying in %ds",
                    attempt + 1, _CLASSIFY_MAX_RETRIES, e, delay,
                )
                await asyncio.sleep(delay)
            else:
                logger.error("CHAT | ALL retries exhausted (OpenRouter + Google): %s", e)

    raise last_error or RuntimeError("chat() all providers failed with no error captured")


async def _generate_openrouter(
    system_prompt: str,
    user_prompt: str,
    tools: list[dict[str, Any]] | None,
    model: str,
    temperature: float,
) -> openai.types.chat.ChatCompletion:
    """Generate via OpenRouter."""
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]

    kwargs: dict[str, Any] = {
        "model": model,
        "messages": messages,
        "temperature": temperature,
    }
    if tools:
        kwargs["tools"] = tools

    resp = await _get_openrouter().chat.completions.create(**kwargs)
    _track_usage(resp, model=model, label="chat_generate")
    return resp


async def _generate_google(
    system_prompt: str,
    user_prompt: str,
    tools: list[dict[str, Any]] | None,
    model: str,
    temperature: float,
    response_format: dict[str, Any] | None = None,
    label: str = "chat_generate",
) -> openai.types.chat.ChatCompletion:
    """Generate via Google Gemini, returning OpenAI-compatible format.

    Wraps Google's response into an OpenAI ChatCompletion-like structure
    so the agent loop doesn't need provider-specific code.
    """
    config = genai_types.GenerateContentConfig(
        temperature=temperature,
        system_instruction=system_prompt,
    )

    if response_format:
        # Convert OpenAI json_schema format to Google's response_schema
        schema = response_format.get("json_schema", {}).get("schema", {})
        if schema:
            config.response_mime_type = "application/json"
            config.response_schema = schema

    if tools:
        # Convert OpenAI tool format to Google format
        declarations = []
        for tool in tools:
            func = tool["function"]
            declarations.append(
                genai_types.FunctionDeclaration(
                    name=func["name"],
                    description=func.get("description", ""),
                    parameters=func.get("parameters", {}),
                )
            )
        config.tools = [genai_types.Tool(function_declarations=declarations)]

    resp = await _get_google().aio.models.generate_content(
        model=GOOGLE_FALLBACK.get(model, model),
        contents=user_prompt,
        config=config,
    )
    _track_usage_google(resp, model=GOOGLE_FALLBACK.get(model, model), label=label)

    # Convert Google response to OpenAI-compatible structure
    candidate = resp.candidates[0]
    part = candidate.content.parts[0]

    if hasattr(part, "function_call") and part.function_call:
        # Tool call response
        fc = part.function_call
        tool_call = openai.types.chat.ChatCompletionMessageToolCall(
            id=f"call_{fc.name}",
            type="function",
            function=openai.types.chat.chat_completion_message_tool_call.Function(
                name=fc.name,
                arguments=json.dumps(dict(fc.args)),
            ),
        )
        message = openai.types.chat.ChatCompletionMessage(
            role="assistant", content=None, tool_calls=[tool_call]
        )
    else:
        # Text response
        message = openai.types.chat.ChatCompletionMessage(
            role="assistant", content=part.text
        )

    choice = openai.types.chat.chat_completion.Choice(
        index=0, message=message, finish_reason="stop"
    )
    return openai.types.chat.ChatCompletion(
        id="google-fallback",
        choices=[choice],
        created=0,
        model=GOOGLE_FALLBACK.get(model, model),
        object="chat.completion",
    )


# =========================================================================
# ANALYZE MEDIA — multimodal analysis (image, audio, video, document)
# =========================================================================


async def analyze_media(
    file_data: bytes,
    mime_type: str,
    prompt: str,
    temperature: float = 0.3,
    label: str = "media_analysis",
) -> str:
    """Analyze media file (image/audio/video/document) and return text.

    3-tier fallback strategy:
      1. Google Gemini 3.1 Pro directly (3 retries) — all media types
      2. Google Gemini 3 Flash directly (3 retries) — all media types, different model
      3. Non-Google fallback routed by media type (3 retries each):
         - Images + documents → Claude Opus 4.6 via OpenRouter
         - Audio → GPT-4o Audio via direct OpenAI API
         - Video → Amazon Nova 2 Lite via OpenRouter

    Args:
        file_data: Raw binary file data.
        mime_type: MIME type (e.g., "image/jpeg", "audio/mpeg").
        prompt: Analysis instructions.
        temperature: Sampling temperature.

    Returns:
        Text analysis/description from the model.
    """
    import base64

    t0 = time.perf_counter()
    logger.info(
        "ANALYZE_MEDIA | mime=%s | bytes=%d | prompt_len=%d",
        mime_type, len(file_data), len(prompt),
    )

    # --- Tier 1: Google Gemini 3.1 Pro (primary) ---
    last_error: Exception | None = None
    google_model = _GOOGLE_MEDIA_MODEL  # gemini-3.1-pro-preview
    for attempt in range(_CLASSIFY_MAX_RETRIES):
        try:
            result_text = await _analyze_google(file_data, mime_type, prompt, google_model, temperature)
            elapsed_ms = int((time.perf_counter() - t0) * 1000)
            logger.info("ANALYZE_MEDIA | done | provider=google | model=%s | attempt=%d | elapsed_ms=%d | result_len=%d", google_model, attempt + 1, elapsed_ms, len(result_text))
            return result_text
        except Exception as e:
            last_error = e
            if attempt < _CLASSIFY_MAX_RETRIES - 1:
                delay = _CLASSIFY_RETRY_DELAYS[min(attempt, len(_CLASSIFY_RETRY_DELAYS) - 1)]
                logger.warning(
                    "ANALYZE_MEDIA | Google %s attempt %d/%d failed: %s — retrying in %ds",
                    google_model, attempt + 1, _CLASSIFY_MAX_RETRIES, e, delay,
                )
                await asyncio.sleep(delay)
            else:
                logger.warning(
                    "ANALYZE_MEDIA | Google %s exhausted %d retries: %s — trying Flash",
                    google_model, _CLASSIFY_MAX_RETRIES, e,
                )

    # --- Tier 2: Google Gemini 3 Flash (different model, same provider) ---
    flash_model = _GOOGLE_MEDIA_FALLBACK  # gemini-3-flash-preview
    for attempt in range(_CLASSIFY_MAX_RETRIES):
        try:
            result_text = await _analyze_google(file_data, mime_type, prompt, flash_model, temperature)
            elapsed_ms = int((time.perf_counter() - t0) * 1000)
            logger.info("ANALYZE_MEDIA | done | provider=google_flash | model=%s | attempt=%d | elapsed_ms=%d | result_len=%d", flash_model, attempt + 1, elapsed_ms, len(result_text))
            return result_text
        except Exception as e:
            last_error = e
            if attempt < _CLASSIFY_MAX_RETRIES - 1:
                delay = _CLASSIFY_RETRY_DELAYS[min(attempt, len(_CLASSIFY_RETRY_DELAYS) - 1)]
                logger.warning(
                    "ANALYZE_MEDIA | Google %s attempt %d/%d failed: %s — retrying in %ds",
                    flash_model, attempt + 1, _CLASSIFY_MAX_RETRIES, e, delay,
                )
                await asyncio.sleep(delay)
            else:
                logger.warning(
                    "ANALYZE_MEDIA | Google %s exhausted %d retries: %s",
                    flash_model, _CLASSIFY_MAX_RETRIES, e,
                )

    # --- Tier 3: Non-Google fallback, routed by media type ---
    is_image = mime_type.startswith("image/")
    is_document = mime_type.startswith("application/") or mime_type.startswith("text/")
    is_audio = mime_type.startswith("audio/")

    if is_image or is_document:
        # --- Tier 3A: Claude Opus 4.6 via OpenRouter (images + documents) ---
        logger.warning("ANALYZE_MEDIA | falling back to Claude Opus 4.6 via OpenRouter (%s)", mime_type)
        b64 = base64.b64encode(file_data).decode()
        data_url = f"data:{mime_type};base64,{b64}"
        messages = [{"role": "user", "content": [
            {"type": "image_url", "image_url": {"url": data_url}},
            {"type": "text", "text": prompt},
        ]}]

        tier3_model = "anthropic/claude-opus-4.6"
        tier3_label = "openrouter_opus"
        for attempt in range(_CLASSIFY_MAX_RETRIES):
            try:
                resp = await _get_openrouter().chat.completions.create(
                    model=tier3_model,
                    messages=messages,
                    temperature=temperature,
                )
                _track_usage(resp, model=tier3_model, label=label)
                result_text = resp.choices[0].message.content or ""
                elapsed_ms = int((time.perf_counter() - t0) * 1000)
                logger.info("ANALYZE_MEDIA | done | provider=%s | attempt=%d | elapsed_ms=%d | result_len=%d", tier3_label, attempt + 1, elapsed_ms, len(result_text))
                return result_text
            except Exception as e:
                last_error = e
                if attempt < _CLASSIFY_MAX_RETRIES - 1:
                    delay = _CLASSIFY_RETRY_DELAYS[min(attempt, len(_CLASSIFY_RETRY_DELAYS) - 1)]
                    logger.warning(
                        "ANALYZE_MEDIA | Opus attempt %d/%d failed: %s — retrying in %ds",
                        attempt + 1, _CLASSIFY_MAX_RETRIES, e, delay,
                    )
                    await asyncio.sleep(delay)
                else:
                    logger.error("ANALYZE_MEDIA | ALL retries exhausted (Google Pro + Flash + Opus): %s", e)

    elif is_audio:
        # --- Tier 3B: GPT-4o Audio via direct OpenAI API (audio files) ---
        if not _openai_direct:
            logger.error("ANALYZE_MEDIA | No OpenAI API key — cannot fall back for audio")
            raise last_error or RuntimeError("analyze_media() Google failed and no OpenAI key for audio fallback")

        logger.warning("ANALYZE_MEDIA | falling back to GPT-4o Audio via direct OpenAI (%s)", mime_type)
        b64 = base64.b64encode(file_data).decode()
        audio_format_map = {
            "audio/wav": "wav", "audio/x-wav": "wav", "audio/wave": "wav",
            "audio/mpeg": "mp3", "audio/mp3": "mp3",
            "audio/mp4": "mp4", "audio/m4a": "mp4",
            "audio/ogg": "wav",  # best-effort fallback
            "audio/webm": "wav",
        }
        audio_fmt = audio_format_map.get(mime_type, "wav")
        messages = [{"role": "user", "content": [
            {"type": "input_audio", "input_audio": {"data": b64, "format": audio_fmt}},
            {"type": "text", "text": prompt},
        ]}]

        tier3_model = "gpt-4o-audio-preview"
        tier3_label = "openai_direct_gpt4o_audio"
        for attempt in range(_CLASSIFY_MAX_RETRIES):
            try:
                resp = await _openai_direct.chat.completions.create(
                    model=tier3_model,
                    messages=messages,
                    temperature=temperature,
                )
                result_text = resp.choices[0].message.content or ""
                elapsed_ms = int((time.perf_counter() - t0) * 1000)
                logger.info("ANALYZE_MEDIA | done | provider=%s | attempt=%d | elapsed_ms=%d | result_len=%d", tier3_label, attempt + 1, elapsed_ms, len(result_text))
                return result_text
            except Exception as e:
                last_error = e
                if attempt < _CLASSIFY_MAX_RETRIES - 1:
                    delay = _CLASSIFY_RETRY_DELAYS[min(attempt, len(_CLASSIFY_RETRY_DELAYS) - 1)]
                    logger.warning(
                        "ANALYZE_MEDIA | GPT-4o Audio attempt %d/%d failed: %s — retrying in %ds",
                        attempt + 1, _CLASSIFY_MAX_RETRIES, e, delay,
                    )
                    await asyncio.sleep(delay)
                else:
                    logger.error("ANALYZE_MEDIA | ALL retries exhausted (Google Pro + Flash + GPT-4o Audio): %s", e)

    else:
        # --- Tier 3C: Amazon Nova 2 Lite via OpenRouter (video + any remaining types) ---
        logger.warning("ANALYZE_MEDIA | falling back to Amazon Nova 2 Lite via OpenRouter (%s)", mime_type)
        b64 = base64.b64encode(file_data).decode()
        data_url = f"data:{mime_type};base64,{b64}"
        messages = [{"role": "user", "content": [
            {"type": "video_url", "video_url": {"url": data_url}},
            {"type": "text", "text": prompt},
        ]}]

        tier3_model = "amazon/nova-2-lite-v1"
        tier3_label = "openrouter_nova"
        for attempt in range(_CLASSIFY_MAX_RETRIES):
            try:
                resp = await _get_openrouter().chat.completions.create(
                    model=tier3_model,
                    messages=messages,
                    temperature=temperature,
                )
                _track_usage(resp, model=tier3_model, label=label)
                result_text = resp.choices[0].message.content or ""
                elapsed_ms = int((time.perf_counter() - t0) * 1000)
                logger.info("ANALYZE_MEDIA | done | provider=%s | attempt=%d | elapsed_ms=%d | result_len=%d", tier3_label, attempt + 1, elapsed_ms, len(result_text))
                return result_text
            except Exception as e:
                last_error = e
                if attempt < _CLASSIFY_MAX_RETRIES - 1:
                    delay = _CLASSIFY_RETRY_DELAYS[min(attempt, len(_CLASSIFY_RETRY_DELAYS) - 1)]
                    logger.warning(
                        "ANALYZE_MEDIA | Nova attempt %d/%d failed: %s — retrying in %ds",
                        attempt + 1, _CLASSIFY_MAX_RETRIES, e, delay,
                    )
                    await asyncio.sleep(delay)
                else:
                    logger.error("ANALYZE_MEDIA | ALL retries exhausted (Google Pro + Flash + Nova): %s", e)

    raise last_error or RuntimeError("analyze_media() all providers failed with no error captured")


async def _analyze_google(
    file_data: bytes,
    mime_type: str,
    prompt: str,
    model: str,
    temperature: float,
) -> str:
    """Analyze media via Google Gemini directly. Supports all media types."""
    media_part = genai_types.Part.from_bytes(data=file_data, mime_type=mime_type)
    config = genai_types.GenerateContentConfig(temperature=temperature)
    resp = await _get_google().aio.models.generate_content(
        model=model,
        contents=[media_part, prompt],
        config=config,
    )
    _track_usage_google(resp, model=model, label="analyze_media")
    return resp.candidates[0].content.parts[0].text


# =========================================================================
# EXTRACT — structured extraction (convenience wrapper around classify)
# =========================================================================


async def extract(
    prompt: str,
    schema: dict[str, Any],
    model: str = "flash",
    temperature: float = 0.3,
) -> dict[str, Any]:
    """Extract structured data from text. Alias for classify with extraction semantics."""
    return await classify(prompt, schema, model, temperature)
