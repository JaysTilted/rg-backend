"""Tool: vector search against Supabase knowledge base.

Uses OpenAI embeddings (same model as n8n KB pipeline) to generate query
vector, then searches Supabase documents table via match_documents RPC.

Includes similarity threshold filtering, keyword boost re-ranking, and
score exposure so the agent can judge retrieval confidence.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any

import openai

from app.config import settings
from app.services.postgres_client import postgres
from app.services.supabase_client import supabase

logger = logging.getLogger(__name__)

KNOWLEDGE_BASE_SEARCH_DEF = {
    "type": "function",
    "function": {
        "name": "knowledge_base_search",
        "description": (
            "Search the knowledge base for detailed information about services, "
            "treatments, procedures, pricing, policies, and FAQs. ALWAYS use this "
            "tool when a lead asks about a specific service or treatment you are not "
            "100% certain about — the Services & Pricing section in your context is a "
            "summary, but the knowledge base contains additional details, treatment "
            "descriptions, and related services. Never tell a lead you don't offer or "
            "aren't familiar with something without searching here first."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "The search query — what information you need",
                },
            },
            "required": ["query"],
        },
    },
}

# OpenAI client for embeddings (initialized lazily)
_openai_client: openai.AsyncOpenAI | None = None
EMBEDDING_MODEL = "text-embedding-3-small"

# Minimum cosine similarity to include a chunk in results.
# With text-embedding-3-small on a small domain-specific KB:
#   > 0.50 = strong topical match
#   0.30-0.50 = related, worth including
#   < 0.30 = noise (off-topic queries score 0.10-0.20)
KB_SIMILARITY_THRESHOLD = 0.0  # TEMP: disabled for testing (was 0.30)

# Max keyword boost added to similarity score (when all query keywords match)
_KEYWORD_BOOST = 0.0  # TEMP: disabled for testing (was 0.08)

# Stop words excluded from keyword matching
_STOPWORDS = frozenset({
    "a", "an", "the", "is", "in", "it", "of", "to", "and", "or",
    "for", "with", "do", "does", "what", "how", "can", "are", "you",
    "your", "my", "i", "we", "this", "that", "at", "by", "from",
    "as", "be", "have", "has", "on", "any", "about", "tell", "me",
})


def _get_openai() -> openai.AsyncOpenAI:
    global _openai_client
    if _openai_client is None:
        _openai_client = openai.AsyncOpenAI(api_key=settings.openai_api_key)
    return _openai_client


def _keyword_boost(results: list[dict], query: str) -> list[dict]:
    """Boost and re-rank results that contain query keywords.

    Extracts meaningful words from the query (3+ chars, stop words removed),
    checks each chunk for keyword presence, adds up to _KEYWORD_BOOST to the
    similarity score, re-sorts, and caps at 7 results.
    """
    tokens = re.findall(r"\b[a-z]{3,}\b", query.lower())
    keywords = [t for t in tokens if t not in _STOPWORDS]

    if not keywords:
        return results[:7]

    total_kw = len(keywords)
    boosted = []

    for r in results:
        content_lower = r.get("content", "").lower()
        matches = sum(1 for kw in keywords if kw in content_lower)
        keyword_ratio = matches / total_kw
        original_sim = r.get("similarity", 0.0)
        adjusted_sim = original_sim + _KEYWORD_BOOST * keyword_ratio

        boosted.append({
            **r,
            "similarity": adjusted_sim,
            "_original_similarity": original_sim,
            "_keyword_matches": matches,
        })

    boosted.sort(key=lambda x: x["similarity"], reverse=True)
    return boosted[:7]


async def knowledge_base_search(
    query: str,
    entity_id: str,
    contact_id: str = "",
    channel: str = "",
    is_test_mode: bool = False,
    lead_id: str | None = None,
    match_count: int = 15,
    **kwargs: Any,
) -> dict[str, Any]:
    """Search knowledge base via vector similarity with threshold filtering and keyword boost."""
    # Resolve tenant's OpenAI key if available, fall back to global
    openai_key = settings.openai_api_key
    if entity_id:
        try:
            tenant = await supabase.get_tenant_for_entity(entity_id)
            tenant_key = tenant.get("openai_key")
            if tenant_key:
                openai_key = tenant_key
        except Exception:
            pass  # Fall back to global key

    if not openai_key:
        return {"error": "Knowledge base search unavailable (no OpenAI API key)"}

    try:
        # 1. Generate query embedding with tenant-aware key
        client = openai.AsyncOpenAI(api_key=openai_key)
        resp = await client.embeddings.create(
            model=EMBEDDING_MODEL,
            input=query,
        )
        embedding = resp.data[0].embedding

        # 2. Vector search in Supabase (fetch extra candidates for re-ranking)
        results = await supabase.search_knowledge_base(
            query_embedding=embedding,
            entity_id=entity_id,
            match_count=match_count,
        )

        # 3. Keyword boost + re-rank (caps at 7 results)
        if results:
            results = _keyword_boost(results, query)

        # 4. Similarity threshold filter
        if not results:
            result = {"result": "No relevant information found in the knowledge base.", "match_count": 0}
        else:
            filtered = [
                r for r in results
                if r.get("similarity", 0.0) >= KB_SIMILARITY_THRESHOLD
            ]

            if not filtered:
                best = results[0]
                best_score = best.get("similarity", 0.0)
                logger.info(
                    "KB search: all %d results below threshold %.2f (best=%.3f) | entity=%s | query=%s",
                    len(results), KB_SIMILARITY_THRESHOLD, best_score, entity_id, query[:80],
                )
                result = {
                    "result": "No relevant information found in the knowledge base.",
                    "match_count": 0,
                    "best_score": round(best_score, 3),
                }
            else:
                articles = []
                scores = []
                for r in filtered:
                    content = r.get("content", "")
                    if content:
                        articles.append(content)
                        scores.append(round(r.get("similarity", 0.0), 3))

                top_kw = filtered[0].get("_keyword_matches", 0) if filtered else 0

                result = {
                    "result": "\n\n---\n\n".join(articles) if articles else "No relevant information found.",
                    "match_count": len(articles),
                    "scores": scores,
                    "top_score": scores[0] if scores else 0.0,
                    "top_keyword_matches": top_kw,
                }

        # 5. Log to tool_executions
        try:
            await postgres.log_tool_execution({
                "session_id": contact_id,
                "client_id": entity_id,
                "tool_name": "knowledge_base_search",
                "tool_input": json.dumps({"query": query}),
                "tool_output": json.dumps(result),
                "channel": channel,
                "execution_id": None,
                "test_mode": is_test_mode,
                "lead_id": lead_id,
            })
        except Exception:
            logger.warning("Failed to log KB search tool execution", exc_info=True)

        return result

    except Exception as e:
        logger.warning("KB search failed: %s", e, exc_info=True)
        return {"error": f"Knowledge base search failed: {str(e)}"}
