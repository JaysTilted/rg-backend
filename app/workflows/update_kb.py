"""Update Knowledge Base — migrated from n8n workflow cKtsf87a7Cmwb8lf.

Handles the embedding pipeline for client knowledge bases:
1. Deletes existing vector embeddings for a given tag + client
2. Chunks new content (1500 chars, 50 overlap)
3. Generates OpenAI embeddings for each chunk
4. Inserts chunks + embeddings + metadata into the documents table

Triggered by: POST /webhook/{entity_id}/update-kb
"""

from __future__ import annotations

import logging
from typing import Any

from prefect import flow

from app.services.supabase_client import supabase
from app.services.workflow_tracker import WorkflowTracker
from app.tools.knowledge_base_search import EMBEDDING_MODEL, _get_openai

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Semantic text chunking
# ---------------------------------------------------------------------------

import re as _re

_MAX_CHUNK_SIZE = 1400   # chars — leaves buffer for title prefix
_MIN_CHUNK_SIZE = 80     # chars — discard stub fragments

# Structural separators tried in order (most specific first)
_STRUCTURAL_PATTERNS = [
    r"\n#{1,6}\s",          # Markdown headers (## Services, ### Pricing)
    r"\n\n+",               # Double newlines (paragraph breaks)
    r"\n(?=[-*•]\s)",       # Newline before bullet/list items
    r"\n",                  # Single newlines
]


def _split_on_pattern(text: str, pattern: str) -> list[str]:
    """Split text on regex pattern, keeping delimiter at start of each piece."""
    parts = _re.split(f"({pattern})", text)
    if len(parts) <= 1:
        return [text]
    result = [parts[0]]
    for i in range(1, len(parts), 2):
        delimiter = parts[i]
        content = parts[i + 1] if i + 1 < len(parts) else ""
        result.append(delimiter + content)
    return [p for p in result if p.strip()]


def _char_split_at_word(text: str, max_size: int, overlap: int = 50) -> list[str]:
    """Last-resort character split at word boundaries."""
    chunks: list[str] = []
    start = 0
    while start < len(text):
        end = start + max_size
        if end >= len(text):
            chunks.append(text[start:])
            break
        boundary = text.rfind(" ", start, end)
        if boundary == -1 or boundary <= start:
            boundary = end
        chunks.append(text[start:boundary])
        start = boundary - overlap
    return [c.strip() for c in chunks if c.strip()]


def _merge_and_split(pieces: list[str], max_size: int) -> list[str]:
    """Merge small adjacent pieces, recursively split large ones."""
    result: list[str] = []
    current = ""

    for piece in pieces:
        piece = piece.strip()
        if not piece:
            continue

        candidate = (current + "\n\n" + piece).strip() if current else piece

        if len(candidate) <= max_size:
            current = candidate
        else:
            if current:
                result.append(current)
            if len(piece) > max_size:
                result.extend(_split_into_sections(piece, max_size))
                current = ""
            else:
                current = piece

    if current:
        result.append(current)
    return result


def _split_into_sections(text: str, max_size: int) -> list[str]:
    """Recursively split text into sections no larger than max_size."""
    text = text.strip()
    if not text:
        return []
    if len(text) <= max_size:
        return [text]

    for pattern in _STRUCTURAL_PATTERNS:
        pieces = _split_on_pattern(text, pattern)
        if len(pieces) > 1:
            return _merge_and_split(pieces, max_size)

    return _char_split_at_word(text, max_size)


def _chunk_text_semantic(title: str, content: str) -> list[str]:
    """Semantic chunker: split on document structure, prepend title to every chunk.

    Splits on headers > paragraphs > bullets > newlines > word boundaries.
    Every chunk gets "Article: {title}" so the agent always knows the source.
    """
    title_prefix = f"Article: {title}\n\n"
    effective_max = _MAX_CHUNK_SIZE - len(title_prefix)

    sections = _split_into_sections(content, effective_max)
    return [
        f"{title_prefix}{s}"
        for s in sections
        if len(s.strip()) >= _MIN_CHUNK_SIZE
    ]


# ---------------------------------------------------------------------------
# Legacy chunker (kept as safety net — rename back to _chunk_text to revert)
# ---------------------------------------------------------------------------

_SEPARATORS = ["\n\n", "\n", " ", ""]


def _chunk_text_legacy(
    text: str,
    chunk_size: int = 1500,
    overlap: int = 50,
) -> list[str]:
    """Split text into overlapping chunks using recursive character splitting.

    Tries separators in order: double-newline, newline, space, character.
    Mirrors n8n's RecursiveCharacterTextSplitter with chunk_size=1500, overlap=50.
    """
    if len(text) <= chunk_size:
        return [text] if text.strip() else []

    return _split_recursive(text, _SEPARATORS, chunk_size, overlap)


def _split_recursive(
    text: str,
    separators: list[str],
    chunk_size: int,
    overlap: int,
) -> list[str]:
    """Recursively split text on the best separator."""
    if not text.strip():
        return []

    sep = ""
    remaining_seps = []
    for i, s in enumerate(separators):
        if s == "" or s in text:
            sep = s
            remaining_seps = separators[i + 1:]
            break

    pieces = text.split(sep) if sep else list(text)

    chunks: list[str] = []
    current = ""

    for piece in pieces:
        candidate = current + sep + piece if current else piece

        if len(candidate) <= chunk_size:
            current = candidate
        else:
            if current:
                chunks.append(current)
            if len(piece) > chunk_size and remaining_seps:
                sub_chunks = _split_recursive(piece, remaining_seps, chunk_size, overlap)
                chunks.extend(sub_chunks)
                current = ""
            else:
                current = piece

    if current:
        chunks.append(current)

    if overlap > 0 and len(chunks) > 1:
        overlapped: list[str] = [chunks[0]]
        for i in range(1, len(chunks)):
            prev_tail = chunks[i - 1][-overlap:]
            overlapped.append(prev_tail + chunks[i])
        chunks = overlapped

    return [c for c in chunks if c.strip()]


# ---------------------------------------------------------------------------
# Embedding generation
# ---------------------------------------------------------------------------


async def _generate_embeddings(chunks: list[str]) -> list[list[float]]:
    """Generate embeddings for all chunks in a single OpenAI API call.

    Uses the same model as the KB search tool (text-embedding-3-small).
    """
    client = _get_openai()
    resp = await client.embeddings.create(model=EMBEDDING_MODEL, input=chunks)
    return [d.embedding for d in resp.data]


# ---------------------------------------------------------------------------
# Main workflow
# ---------------------------------------------------------------------------


@flow(name="update-kb", retries=0)
async def process_kb_update(
    entity_id: str,
    title: str,
    content: str,
    tag: str,
    action: str = "upsert",
    article_id: str | None = None,
) -> dict[str, Any]:
    """Process a knowledge base update: delete old embeddings, chunk, embed, insert.

    Args:
        entity_id: The entity UUID.
        title: Article title.
        content: Article content.
        tag: Category tag (used as metadata key for filtering).
        action: "delete" to only remove, "upsert" (default) to replace.
        article_id: knowledge_base_articles.id — stored as FK on document rows.
    """
    tracker = WorkflowTracker(
        "update_kb",
        entity_id=entity_id,
        trigger_source="webhook",
    )

    try:
        logger.info(
            "UPDATE_KB | starting | client=%s tag=%s action=%s",
            entity_id, tag, action,
        )

        # --- Delete path ---
        if action == "delete":
            await supabase.delete_documents(entity_id, tag, article_id=article_id)
            logger.info("UPDATE_KB | deleted | client=%s tag=%s article=%s", entity_id, tag, article_id)
            tracker.set_decisions({"action": "deleted", "tag": tag, "chunks": 0})
            return {"action": "deleted", "category": tag, "chunks": 0}

        # --- Upsert path ---

        # 1. Delete existing embeddings for this article (or tag+client fallback)
        await supabase.delete_documents(entity_id, tag, article_id=article_id)

        # 2. Handle empty content (treat as delete)
        if not content or not content.strip():
            logger.info("UPDATE_KB | empty_content | client=%s tag=%s", entity_id, tag)
            tracker.set_decisions({"action": "deleted", "tag": tag, "reason": "empty_content", "chunks": 0})
            return {"action": "deleted", "category": tag, "chunks": 0}

        # 3 + 4. Semantic chunk with title prepended to every chunk
        chunks = _chunk_text_semantic(title, content)
        logger.info(
            "UPDATE_KB | chunked | client=%s tag=%s chunks=%d total_chars=%d",
            entity_id, tag, len(chunks), len(content),
        )

        if not chunks:
            tracker.set_decisions({"action": "upserted", "tag": tag, "chunks": 0})
            return {"action": "upserted", "category": tag, "chunks": 0}

        # 5. Generate embeddings (single batch API call)
        embeddings = await _generate_embeddings(chunks)
        logger.info(
            "UPDATE_KB | embedded | client=%s tag=%s model=%s",
            entity_id, tag, EMBEDDING_MODEL,
        )

        # 6. Insert all chunks with entity/tenant columns
        tenant_id = await supabase.resolve_tenant_id(entity_id)
        rows = [
            {
                "content": chunk,
                "embedding": emb,
                "entity_id": entity_id,
                **({"tenant_id": tenant_id} if tenant_id else {}),
                "metadata": {"Category": tag},
                **({"article_id": article_id} if article_id else {}),
            }
            for chunk, emb in zip(chunks, embeddings)
        ]
        await supabase.insert_documents(rows)

        logger.info(
            "UPDATE_KB | complete | client=%s tag=%s chunks=%d",
            entity_id, tag, len(chunks),
        )

        tracker.set_decisions({"action": "upserted", "tag": tag, "chunks": len(chunks)})

        return {"action": "upserted", "category": tag, "chunks": len(chunks)}

    except Exception as e:
        tracker.set_error(str(e))
        raise
    finally:
        await tracker.save()
