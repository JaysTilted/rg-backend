"""
API endpoint: GET /api/prompt-token-map

Returns token counts for all LLM calls in the pipeline, separating hardcoded
prompt tokens from variable config keys. The frontend combines these with
live client config data to compute accurate per-client cost estimates.

Response is deterministic per deploy — hardcoded prompts don't change at runtime.
Cache aggressively (24h recommended).
"""

import logging
from datetime import datetime, timezone

from fastapi import APIRouter, Query

from app.text_engine.llm_registry import REGISTRY
from app.services.tokenizer import count_tokens

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/api/prompt-token-map")
async def get_prompt_token_map(
    model: str = Query(
        default="openai/gpt-4o",
        description="Model ID to use for tokenization (affects token count accuracy per provider)",
    ),
):
    """Return token counts for all registered LLM calls.

    The response maps each call key to:
    - hardcoded_tokens: token count of all hardcoded prompt text
    - variable_keys: list of config keys whose content is injected at runtime
    - service_config_dependent: whether service_config JSON is formatted and injected
    - history_avg: estimated average conversation history tokens
    - output_avg: estimated average output tokens
    - default_model: the model alias used by default (pro, flash, etc.)
    """
    calls = {}

    for key, entry in REGISTRY.items():
        # Count hardcoded tokens by concatenating all referenced prompt texts
        hardcoded_text = "\n".join(
            t for t in entry.get("hardcoded_texts", []) if isinstance(t, str)
        )
        hardcoded_tokens = count_tokens(hardcoded_text, model) if hardcoded_text else 0

        calls[key] = {
            "hardcoded_tokens": hardcoded_tokens,
            "variable_keys": entry.get("variable_keys", []),
            "service_config_dependent": entry.get("service_config", False),
            "history_avg": entry.get("history_avg_tokens", 0),
            "output_avg": entry.get("output_avg_tokens", 200),
            "default_model": entry.get("default_model", "google/gemini-2.5-flash"),
            "default_temperature": entry.get("default_temperature", 0.3),
        }

    return {
        "model_used": model,
        "tokenizer": _tokenizer_name(model),
        "calls": calls,
        "call_count": len(calls),
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


def _tokenizer_name(model_id: str) -> str:
    """Return human-readable tokenizer name for the model provider."""
    provider = model_id.split("/")[0] if "/" in model_id else "openai"
    return {
        "openai": "tiktoken-o200k_base",
        "google": "google-countTokens",
        "anthropic": "anthropic-count_tokens",
    }.get(provider, f"tiktoken-fallback ({provider})")
