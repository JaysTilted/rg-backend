"""
Multi-provider token counting service.

Uses native tokenizers where available, falls back to tiktoken (OpenAI's o200k_base).
Accuracy: ~98-100% for OpenAI, ~90-95% for others via tiktoken fallback.

Provider-specific tokenizers (Google countTokens, Anthropic count_tokens) are called
lazily and cached. If any provider API is unavailable, tiktoken is used as fallback.
"""

import logging
from functools import lru_cache

import tiktoken

logger = logging.getLogger(__name__)

# ── Lazy-loaded tiktoken encoder ──
_tiktoken_enc: tiktoken.Encoding | None = None


def _get_tiktoken() -> tiktoken.Encoding:
    global _tiktoken_enc
    if _tiktoken_enc is None:
        _tiktoken_enc = tiktoken.get_encoding("o200k_base")
    return _tiktoken_enc


def count_tokens(text: str, model_id: str = "openai/gpt-4o") -> int:
    """Count tokens using the appropriate tokenizer for the model provider.

    Args:
        text: The text to count tokens for.
        model_id: Full model ID (e.g., "google/gemini-2.5-pro", "openai/gpt-5.4").

    Returns:
        Token count (int). Returns 0 for empty text.
    """
    if not text:
        return 0

    provider = model_id.split("/")[0] if "/" in model_id else "openai"

    try:
        if provider == "openai":
            return _count_tiktoken(text)
        elif provider == "google":
            return _count_gemini(text)
        elif provider == "anthropic":
            return _count_anthropic(text)
        else:
            # DeepSeek, xAI, etc. — tiktoken fallback (~90-95% accurate for English)
            return _count_tiktoken(text)
    except Exception as e:
        logger.warning(f"Tokenizer failed for {provider}, falling back to tiktoken: {e}")
        return _count_tiktoken(text)


def _count_tiktoken(text: str) -> int:
    """Count tokens using OpenAI's tiktoken (o200k_base). Exact for OpenAI models."""
    return len(_get_tiktoken().encode(text))


def _count_gemini(text: str) -> int:
    """Count tokens using Google's countTokens API (free, exact for Gemini models).
    Falls back to tiktoken if API unavailable.
    """
    try:
        import google.generativeai as genai
        import os

        api_key = os.getenv("GOOGLE_AI_API_KEY") or os.getenv("GEMINI_API_KEY")
        if not api_key:
            return _count_tiktoken(text)

        genai.configure(api_key=api_key)
        model = genai.GenerativeModel("gemini-2.5-flash")
        result = model.count_tokens(text)
        return result.total_tokens
    except Exception as e:
        logger.debug(f"Gemini countTokens failed, using tiktoken: {e}")
        return _count_tiktoken(text)


def _count_anthropic(text: str) -> int:
    """Count tokens using Anthropic's count_tokens API (free, exact for Claude models).
    Falls back to tiktoken if API unavailable.
    """
    try:
        import anthropic
        import os

        api_key = os.getenv("ANTHROPIC_API_KEY")
        if not api_key:
            return _count_tiktoken(text)

        client = anthropic.Anthropic(api_key=api_key)
        result = client.messages.count_tokens(
            model="claude-sonnet-4-6-20250514",
            messages=[{"role": "user", "content": text}],
        )
        return result.input_tokens
    except Exception as e:
        logger.debug(f"Anthropic count_tokens failed, using tiktoken: {e}")
        return _count_tiktoken(text)


@lru_cache(maxsize=256)
def count_tokens_cached(text: str, model_id: str = "openai/gpt-4o") -> int:
    """Cached version of count_tokens. Use for repeated counting of the same text."""
    return count_tokens(text, model_id)


def count_chars_fallback(text: str) -> int:
    """Last-resort fallback: chars / 4. ~75-85% accurate for English."""
    if not text:
        return 0
    return max(1, len(text) // 4)
