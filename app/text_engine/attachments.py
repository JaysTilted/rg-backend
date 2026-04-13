"""Step 3.4: Attachment processing — download, classify, analyze, store.

Replaces n8n's Parse Attachments → Download → Classify by MIME →
Media Analysis Subworkflow → Synthesis Agent → Insert Attachments chain.

Flow: Download from GHL → Upload to Supabase Storage (permanent URL) →
Analyze with Gemini (detailed extraction prompts matching n8n) →
Synthesize context-aware descriptions → Store in attachments table.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any

import httpx
from prefect import task

from app.models import PipelineContext
from app.services.ai_client import analyze_media, classify
from app.text_engine.model_resolver import resolve_model, resolve_temperature
from app.services.postgres_client import postgres
from app.services.supabase_client import supabase
from app.text_engine.utils import get_timezone, parse_datetime

logger = logging.getLogger(__name__)

# Magic bytes → MIME type mapping for when HTTP headers lie
_MAGIC_BYTES: list[tuple[bytes, str, int]] = [
    # (signature, mime_type, offset)
    (b"\x89PNG\r\n\x1a\n", "image/png", 0),
    (b"\xff\xd8\xff", "image/jpeg", 0),
    (b"GIF87a", "image/gif", 0),
    (b"GIF89a", "image/gif", 0),
    (b"RIFF", "image/webp", 0),        # WebP — RIFF at 0, WEBP at 8 (checked below)
    (b"%PDF", "application/pdf", 0),
    (b"ID3", "audio/mpeg", 0),          # MP3 with ID3 tag
    (b"\xff\xfb", "audio/mpeg", 0),     # MP3 frame sync
    (b"\xff\xf3", "audio/mpeg", 0),     # MP3 frame sync (MPEG 2.5)
    (b"\xff\xf2", "audio/mpeg", 0),     # MP3 frame sync (MPEG 2.5)
    (b"OggS", "audio/ogg", 0),
    (b"fLaC", "audio/flac", 0),
    (b"#!AMR", "audio/amr", 0),
    (b"PK\x03\x04", "application/zip", 0),  # ZIP (also docx/xlsx)
]


def _detect_mime_from_bytes(data: bytes) -> str | None:
    """Detect MIME type from file magic bytes. Returns None if unrecognized."""
    if len(data) < 12:
        return None

    for sig, mime, offset in _MAGIC_BYTES:
        if data[offset:offset + len(sig)] == sig:
            # WebP needs extra check: RIFF....WEBP
            if sig == b"RIFF" and data[8:12] != b"WEBP":
                continue
            return mime

    # MP4/MOV family — "ftyp" at offset 4
    if data[4:8] == b"ftyp":
        return "video/mp4"

    return None


# MIME type classification — maps MIME prefix/suffix to attachment type
_MIME_CATEGORIES = {
    "image/": "image",
    "video/": "video",
    "audio/": "audio",
    "application/pdf": "document",
    "application/msword": "document",
    "application/vnd.openxmlformats-officedocument": "document",
    "application/vnd.ms-excel": "document",
    "application/vnd.ms-powerpoint": "document",
    "text/": "document",
    "application/rtf": "document",
    "application/csv": "document",
}

# Extension-based fallback (when MIME is generic like application/octet-stream)
_EXT_CATEGORIES = {
    ".jpg": "image", ".jpeg": "image", ".png": "image", ".gif": "image",
    ".webp": "image", ".heic": "image", ".heif": "image", ".bmp": "image",
    ".tiff": "image", ".svg": "image",
    ".mp4": "video", ".mov": "video", ".avi": "video", ".mkv": "video",
    ".webm": "video", ".m4v": "video", ".3gp": "video",
    ".mp3": "audio", ".wav": "audio", ".m4a": "audio", ".ogg": "audio",
    ".aac": "audio", ".flac": "audio", ".opus": "audio", ".amr": "audio",
    ".pdf": "document", ".doc": "document", ".docx": "document",
    ".xls": "document", ".xlsx": "document", ".txt": "document",
    ".csv": "document", ".rtf": "document",
}

# Detailed extraction prompts by type — matches n8n Media Analysis Subworkflow exactly
_ANALYSIS_PROMPTS = {
    "image": (
        "Extract ALL information from this image. Be thorough and precise.\n\n"
        "**IMAGE TYPE:** State what kind of image this is (photo, screenshot, document scan, etc.)\n\n"
        "**ALL TEXT & NUMBERS:** Extract exactly as shown:\n"
        "- Every word, number, price, date, name visible\n"
        "- Error messages, notifications, labels\n"
        "- Fine print, watermarks, headers/footers\n"
        "- Handwritten text if any\n\n"
        "**VISUAL CONTENT:** Describe in detail:\n"
        "- People: appearance, what they're showing/doing\n"
        "- Objects: items, products, their condition\n"
        "- Screenshots: app/website name, which screen, UI elements, buttons\n"
        "- Documents: layout, sections, tables, structure\n"
        "- Photos: setting, lighting, what's in frame\n\n"
        "**LOGOS & BRANDING:** Any company names, logos, brand identifiers\n\n"
        "**CONDITION/QUALITY:** Is anything blurry, cut off, or partially visible?\n\n"
        "Be factual. Extract everything. Do not interpret or summarize."
    ),
    "video": (
        "Extract ALL information from this video. Be thorough and precise.\n\n"
        "**VIDEO TYPE:** What kind of video is this? (screen recording, selfie video, "
        "voice message, product demo, etc.)\n\n"
        "**SPOKEN CONTENT:** Transcribe or summarize everything said:\n"
        "- Use quotes for exact important phrases\n"
        "- Note speaker tone (calm, rushed, frustrated, happy, etc.)\n"
        "- Include [pause], [unclear], [background noise] where relevant\n\n"
        "**VISUAL CONTENT:** Describe what's shown:\n"
        "- Scene, setting, people, actions\n"
        "- Any text visible on screen\n"
        "- Changes throughout the video (what happens when)\n\n"
        "**TEXT & NUMBERS:** Extract all visible:\n"
        "- Prices, dates, names, phone numbers\n"
        "- On-screen text, captions, UI elements\n"
        "- Error messages, notifications\n\n"
        "**LOGOS & BRANDING:** Any company names, logos visible\n\n"
        "**AUDIO QUALITY:** Clear? Background noise? Multiple speakers?\n\n"
        "Be factual. Extract everything. Do not interpret meaning."
    ),
    "audio": (
        "Transcribe and extract ALL information from this audio. Be thorough and precise.\n\n"
        "**TRANSCRIPTION:**\n"
        "Write out everything said, as accurately as possible.\n"
        "- Use [unclear] for words you can't make out\n"
        "- Use [pause], [sigh], [laugh], [background noise] for non-speech\n"
        "- Preserve natural phrasing\n\n"
        "**SPEAKER INFO:**\n"
        "- How many speakers?\n"
        "- Tone of voice (calm, rushed, frustrated, excited, nervous, friendly, etc.)\n"
        "- Speaking pace (fast, slow, hesitant)\n\n"
        "**EXTRACTED DETAILS:** List any mentioned:\n"
        "- Names (people, businesses, products)\n"
        "- Numbers (prices, phone numbers, quantities, dates, times)\n"
        "- Specific requests or questions asked\n"
        "- Places or locations mentioned\n\n"
        "**AUDIO QUALITY:** Clear recording? Background noise? Any parts hard to hear?\n\n"
        "Be factual. Transcribe accurately. Do not interpret intent or meaning."
    ),
    "document": (
        "Extract ALL information from this document. Be thorough and precise.\n\n"
        "**DOCUMENT TYPE:** What kind of document is this? (quote, invoice, contract, "
        "form, receipt, medical record, etc.)\n\n"
        "**SOURCE:**\n"
        "- Company/organization name\n"
        "- Logo or letterhead\n"
        "- Address, phone, email, website if shown\n"
        "- Document date and reference numbers\n\n"
        "**ALL TEXT CONTENT:** Extract everything:\n"
        "- Headers, titles, section names\n"
        "- Body text and paragraphs\n"
        "- Line items with descriptions and amounts\n"
        "- Terms, conditions, fine print\n"
        "- Signatures, dates, stamps\n\n"
        "**NUMBERS & PRICES:** List all with context:\n"
        "- Individual line items and costs\n"
        "- Subtotals, taxes, discounts, totals\n"
        "- Dates, validity periods, deadlines\n"
        "- Reference numbers, account numbers\n\n"
        "**TABLES & STRUCTURE:** Describe layout:\n"
        "- How information is organized\n"
        "- What columns/rows contain\n"
        "- Any highlighted or emphasized sections\n\n"
        "**HANDWRITTEN CONTENT:** Notes, signatures, checkmarks, modifications\n\n"
        "**CONDITION:** Fully visible? Any parts cut off, blurry, or obscured?\n\n"
        "Be factual. Extract everything. Do not summarize or interpret."
    ),
}

# =========================================================================
# LINK ATTACHMENT PROCESSING — fetch URLs, summarize, store as type="link"
# =========================================================================

_MAX_LINK_TEXT = 8000  # Truncate fetched page text


def _is_private_ip(hostname: str) -> bool:
    """Check if a hostname resolves to a private/loopback IP."""
    import ipaddress
    import socket
    try:
        addr_info = socket.getaddrinfo(hostname, None)
        for _family, _, _, _, sockaddr in addr_info:
            ip = ipaddress.ip_address(sockaddr[0])
            if ip.is_private or ip.is_loopback or ip.is_link_local:
                return True
    except (socket.gaierror, ValueError):
        pass
    return False


async def _fetch_link_content(url: str) -> tuple[str, str]:
    """Fetch URL via Jina Reader API. Returns (page_text, final_url).

    Primary: Jina Reader (handles JS rendering, bot protection, redirects).
    Fallback: empty string (caller uses domain/path as degraded description).
    """
    from urllib.parse import urlparse
    from app.utils.retry import with_retries, is_transient_http

    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        raise ValueError(f"Invalid URL scheme: {parsed.scheme}")

    hostname = parsed.hostname or ""
    if _is_private_ip(hostname):
        raise ValueError(f"Blocked private IP for hostname: {hostname}")

    async def _do_jina_fetch() -> tuple[str, str]:
        async with httpx.AsyncClient(
            timeout=30.0,
            follow_redirects=True,
            headers={
                "Accept": "text/plain",
                "User-Agent": "TextEngine/1.0",
            },
        ) as client:
            resp = await client.get(f"https://r.jina.ai/{url}")
            resp.raise_for_status()
            text = resp.text[:_MAX_LINK_TEXT] if resp.text else ""
            return text, url

    return await with_retries(
        _do_jina_fetch,
        max_attempts=3,
        base_delay=3.0,
        retryable=is_transient_http,
        label="link_fetch_jina",
    )


async def _process_link(
    ctx: PipelineContext,
    url: str,
    entity_id: str,
    type_counts: dict[str, int],
    ghl_message_id: str = "",
    date_added: str = "",
) -> dict[str, Any] | None:
    """Fetch a URL via Jina, store raw page text for Synthesis Agent to summarize."""
    from urllib.parse import urlparse

    parsed = urlparse(url)
    domain = (parsed.netloc or "").lstrip("www.")
    path = parsed.path or ""

    # 1. Fetch page content via Jina Reader
    page_text = ""
    try:
        page_text, _final_url = await _fetch_link_content(url)
    except Exception:
        logger.warning("LINK | fetch failed: %s", url[:80], exc_info=True)

    # 2. Update type index
    type_counts["link"] = type_counts.get("link", 0) + 1
    type_index = type_counts["link"]

    # 3. Raw analysis = page text (Synthesis Agent will summarize, same as media)
    raw_analysis = f"URL: {url}\n\n{page_text[:_MAX_LINK_TEXT]}" if page_text else ""

    # Description is set to raw_analysis for now — Synthesis Agent (step 6 in
    # process_attachments) will replace it with a context-aware version.
    if raw_analysis:
        description = f"Link {type_index}: {raw_analysis}"
    else:
        description = f"Link {type_index}: [{domain}{path}] — page could not be loaded"

    # 4. Insert to DB (no Supabase Storage upload — links don't need binary storage)
    msg_timestamp = parse_datetime(date_added) if date_added else datetime.now(timezone.utc)
    record = await postgres.insert_attachment({
        "session_id": ctx.contact_id,
        "client_bot_id": entity_id,
        "lead_id": ctx.lead.get("id") if ctx.lead else None,
        "type": "link",
        "url": url,
        "original_url": url,
        "ghl_message_id": ghl_message_id,
        "description": description,
        "raw_analysis": raw_analysis,
        "message_timestamp": msg_timestamp,
    })
    # Attach metadata for Synthesis Agent (not persisted — used in-memory only)
    record["_global_index"] = sum(type_counts.values())
    record["type_index"] = type_index
    logger.info("LINK | stored | url=%s | type_index=%d | ghl_msg=%s",
                url[:80], type_index, ghl_message_id[:12])
    return record


@task(name="process_attachments", retries=2, retry_delay_seconds=5, timeout_seconds=120)
async def process_attachments(ctx: PipelineContext) -> None:
    """Process new attachments discovered by conversation sync.

    For each pending attachment group (from ctx.pending_attachments):
    1. Skip if ghl_message_id already has attachment records (dedup)
    2. Download binary from GHL CDN URL
    3. Upload to Supabase Storage (permanent URL)
    4. Classify by MIME type (image/video/audio/document)
    5. Analyze with Gemini multimodal (detailed extraction prompts)
    6. Run Synthesis Agent to create context-aware descriptions
    7. Update DB records with synthesized descriptions

    Updates ctx.attachments with all attachment records (existing + new).
    """
    entity_id = ctx.config.get("id", ctx.entity_id)

    # Always load existing attachments into ctx (needed for embed_refs + agent context)
    existing = await postgres.get_attachments(ctx.contact_id, entity_id)

    if not ctx.pending_attachments:
        ctx.attachments = existing
        return

    total_urls = sum(len(pa["urls"]) for pa in ctx.pending_attachments)
    total_links = sum(len(pa.get("link_urls", [])) for pa in ctx.pending_attachments)
    logger.info("ATTACHMENTS | pending_groups=%d | total_urls=%d | total_links=%d | contact=%s",
                len(ctx.pending_attachments), total_urls, total_links, ctx.contact_id)

    # Build set of existing ghl_message_ids for dedup
    existing_ghl_ids = {att.get("ghl_message_id", "") for att in existing} - {""}

    # Build set of incoming ghl_message_ids for re-sent detection
    incoming_ghl_ids = {pa["ghl_message_id"] for pa in ctx.pending_attachments}

    # Identify re-sent attachments (ghl_message_ids in this batch that already exist in DB)
    resent = [att for att in existing if att.get("ghl_message_id", "") in incoming_ghl_ids]

    # Track type counts for indexing (Image 1, Image 2, etc.)
    type_counts: dict[str, int] = {}
    for att in existing:
        att_type = att.get("type", "image")
        type_counts[att_type] = type_counts.get(att_type, 0) + 1

    new_attachments: list[dict[str, Any]] = []
    for pa in ctx.pending_attachments:
        ghl_msg_id = pa["ghl_message_id"]
        date_added = pa["date_added"]

        # Skip if already processed (conversation sync should have filtered, but double-check)
        if ghl_msg_id in existing_ghl_ids:
            continue

        for url in pa["urls"]:
            try:
                record = await _process_single(ctx, url, entity_id, type_counts,
                                               ghl_msg_id, date_added)
                if record:
                    new_attachments.append(record)
            except Exception:
                logger.warning("Failed to process attachment: %s", url, exc_info=True)

        # Process link URLs (URLs extracted from message body)
        for link_url in pa.get("link_urls", []):
            try:
                record = await _process_link(ctx, link_url, entity_id, type_counts,
                                             ghl_msg_id, date_added)
                if record:
                    new_attachments.append(record)
            except Exception:
                logger.warning("Failed to process link: %s", link_url, exc_info=True)

    # 6. Synthesis Agent — rewrite descriptions with conversation context
    if new_attachments:
        # Prior attachments = existing ones NOT in this message batch
        resent_ids = {att.get("id") for att in resent}
        prior = [att for att in existing if att.get("id") not in resent_ids]

        synthesized = await _synthesize_descriptions(
            ctx, new_attachments, resent, prior
        )
        if synthesized:
            # Update DB records with synthesized descriptions
            for rec in new_attachments:
                global_idx = rec.get("_global_index")
                if global_idx is not None and global_idx in synthesized:
                    att_type = rec.get("type", "image")
                    type_idx = rec.get("type_index", 1)
                    label = f"{att_type.capitalize()} {type_idx}"
                    new_desc = f"{label}: {synthesized[global_idx]}"
                    rec["description"] = new_desc
                    # Update in DB too
                    try:
                        await postgres.update_attachment_description(
                            rec["id"], new_desc
                        )
                    except Exception:
                        logger.warning("Failed to update description for %s", rec.get("id"), exc_info=True)

    # Update ctx with all attachments
    ctx.attachments = existing + new_attachments
    if new_attachments:
        logger.info("Processed %d new attachments", len(new_attachments))


async def _process_single(
    ctx: PipelineContext,
    url: str,
    entity_id: str,
    type_counts: dict[str, int],
    ghl_message_id: str = "",
    date_added: str = "",
) -> dict[str, Any] | None:
    """Download, upload to Supabase Storage, analyze, and store a single attachment."""
    # 1. Download from GHL CDN
    file_data, mime_type = await _download_file(url)
    if not file_data:
        logger.warning("Failed to download attachment: %s", url)
        return None

    # 2. Classify
    att_type = _classify_mime(mime_type, url)
    logger.info("ATTACHMENT | mime=%s | type=%s | ghl_msg=%s | url=%s",
                mime_type, att_type, ghl_message_id[:12], url[:80])

    # 3. Update type index
    type_counts[att_type] = type_counts.get(att_type, 0) + 1
    type_index = type_counts[att_type]

    # 4. Upload to Supabase Storage (our own permanent copy)
    timestamp_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    file_uuid = uuid.uuid4().hex[:36]
    storage_path = f"{ctx.contact_id}/{att_type}/{timestamp_ms}_{type_index - 1}_{file_uuid}"

    permanent_url = url  # Fallback to GHL CDN URL if upload fails
    try:
        from app.utils.retry import with_retries, is_transient_http

        permanent_url = await with_retries(
            supabase.upload_attachment,
            args=(storage_path, file_data, mime_type),
            max_attempts=3,
            base_delay=2.0,
            retryable=is_transient_http,
            label="attachment_upload",
        )
        logger.info("Uploaded to Supabase Storage: %s", storage_path)
    except Exception:
        logger.warning(
            "Supabase Storage upload failed after retries, using GHL CDN URL: %s", url[:80], exc_info=True
        )

    # 5. Analyze with AI (using detailed extraction prompts)
    raw_analysis = ""
    try:
        prompt = _ANALYSIS_PROMPTS.get(att_type, _ANALYSIS_PROMPTS["image"])
        raw_analysis = await analyze_media(file_data, mime_type, prompt)
    except Exception:
        logger.warning("AI analysis failed for %s attachment", att_type, exc_info=True)

    # Description is set to raw_analysis for now — Synthesis Agent (step 6 in
    # process_attachments) will replace it with a context-aware version.
    type_label = att_type.capitalize()
    description = f"{type_label} {type_index}: {raw_analysis.strip() or 'Media attachment received'}"

    # 6. Insert to DB (permanent Supabase URL in `url`, original GHL CDN URL preserved)
    msg_timestamp = parse_datetime(date_added) if date_added else datetime.now(timezone.utc)
    record = await postgres.insert_attachment({
        "session_id": ctx.contact_id,
        "client_bot_id": entity_id,
        "lead_id": ctx.lead.get("id") if ctx.lead else None,
        "type": att_type,
        "url": permanent_url,
        "original_url": url.split("?")[0],
        "ghl_message_id": ghl_message_id,
        "description": description,
        "raw_analysis": raw_analysis,
        "message_timestamp": msg_timestamp,
    })
    # Attach metadata for Synthesis Agent (not persisted — used in-memory only)
    record["_global_index"] = sum(type_counts.values())
    record["type_index"] = type_index
    logger.info("Stored %s (type=%s, index=%d, ghl_msg=%s)",
                permanent_url[:60], att_type, type_index, ghl_message_id[:12])
    return record


async def _download_file(url: str) -> tuple[bytes | None, str]:
    """Download a file and return (binary_data, mime_type).

    Returns (None, "") if download fails. Retries on transient HTTP errors.
    """
    from app.utils.retry import with_retries, is_transient_http

    async def _do_download() -> tuple[bytes, str]:
        async with httpx.AsyncClient(
            timeout=30.0,
            follow_redirects=True,
            headers={"User-Agent": "TextEngine/1.0"},
        ) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            mime_type = resp.headers.get("content-type", "application/octet-stream")
            mime_type = mime_type.split(";")[0].strip()
            return resp.content, mime_type

    try:
        file_data, mime_type = await with_retries(
            _do_download,
            max_attempts=3,
            base_delay=2.0,
            retryable=is_transient_http,
            label="attachment_download",
        )
        # If server returned a useless MIME type, detect from file signature
        if file_data and mime_type in ("application/octet-stream", "binary/octet-stream", ""):
            detected = _detect_mime_from_bytes(file_data)
            if detected:
                logger.info("MAGIC_BYTES | override %s -> %s | url=%s", mime_type, detected, url[:80])
                mime_type = detected
        return file_data, mime_type
    except Exception:
        logger.warning("Download failed after retries: %s", url[:80], exc_info=True)
        return None, ""


def _classify_mime(mime_type: str, url: str) -> str:
    """Classify an attachment as image/video/audio/document.

    Uses MIME type first (authoritative), falls back to URL extension.
    Handles ambiguous container formats (Ogg, 3GP) where MIME says "video"
    but the file is actually audio — common with voice notes from GHL/WhatsApp.
    """
    mime_lower = mime_type.lower()

    # Ambiguous container MIME types — extension wins over MIME prefix
    _AUDIO_EXTENSIONS = {".ogg", ".oga", ".opus", ".amr", ".m4a", ".mp3", ".wav", ".aac", ".flac"}
    if mime_lower in ("video/ogg", "application/ogg", "video/3gpp", "video/3gpp2"):
        url_lower = url.lower().split("?")[0]
        for ext in _AUDIO_EXTENSIONS:
            if url_lower.endswith(ext):
                return "audio"
        # No audio extension match — fall through to normal MIME classification

    # MIME-based classification
    for prefix, category in _MIME_CATEGORIES.items():
        if mime_lower.startswith(prefix):
            return category

    # Extension-based fallback
    url_lower = url.lower().split("?")[0]  # Strip query params
    for ext, category in _EXT_CATEGORIES.items():
        if url_lower.endswith(ext):
            return category

    # URL pattern fallback (GHL URLs sometimes contain hints)
    if "/image/" in url_lower or "img" in url_lower:
        return "image"
    if "/video/" in url_lower or "vid" in url_lower:
        return "video"
    if "/audio/" in url_lower or "voice" in url_lower or "ptt" in url_lower:
        return "audio"

    return "image"  # Default


# =========================================================================
# SYNTHESIS AGENT — context-aware descriptions (matches n8n Synthesis Agent)
# =========================================================================


async def _fetch_chat_history_for_synthesis(ctx: PipelineContext) -> str:
    """Fetch and format chat history for the Synthesis Agent.

    Matches n8n's "Get Chat History Early" → "Format Chat History" chain.
    Queries the last 50 messages and formats as:
        [time] ROLE [source]: message text

    Called before timeline is built, so we query the DB directly.
    """
    chat_table = ctx.config.get("chat_history_table_name", "")
    if not chat_table:
        return ""

    tz = ctx.tz or get_timezone(ctx.config)

    try:
        rows = await postgres.get_chat_history(chat_table, ctx.contact_id, limit=50)
    except Exception:
        logger.warning("Failed to fetch chat history for synthesis", exc_info=True)
        return ""

    if not rows:
        return ""

    # Parse and sort by timestamp (oldest first)
    messages = []
    for row in rows:
        ts = row.get("timestamp")
        is_human = row.get("role") == "human"
        source = row.get("source") or ("unknown")

        text = ""
        content = row.get("content") or ""
        if isinstance(content, str):
            text = content
        elif isinstance(content, list):
            text = " ".join(c.get("text", "") for c in content)

        if not text:
            continue

        messages.append({"timestamp": ts, "is_human": is_human, "source": source, "text": text})

    messages.sort(key=lambda m: m["timestamp"] or datetime.min.replace(tzinfo=timezone.utc))

    # Format conversation lines
    lines = ["--- CONVERSATION ---"]
    for msg in messages:
        ts = msg["timestamp"]
        if ts:
            local_ts = ts.astimezone(tz) if hasattr(ts, "astimezone") else ts
            time_str = local_ts.strftime("%b %d, %I:%M %p")
        else:
            time_str = ""
        role = "LEAD" if msg["is_human"] else "AI"
        source_tag = f" [{msg['source']}]" if msg["source"] != "unknown" else ""
        lines.append(f"[{time_str}] {role}{source_tag}: {msg['text']}")

    return "\n".join(lines)

_SYNTHESIS_SYSTEM_PROMPT = """\
<role>
You are a Media Analysis Specialist integrated into a sales conversation system.

## Your Role
Create concise, actionable descriptions for attachments (media files and links) that leads send during conversations. These descriptions are what the AI sales agent will see - they CANNOT see the actual attachments or visit the links.
</role>

<rules>
## Critical Rules
1. Use "Lead" when referring to the person (not "Customer" or "User")
2. **NEVER MAKE THINGS UP.** Only describe what you can actually see/read. If unclear, say so.
3. **DO NOT ADD LABELS.** Never start with "Image 1:", "Link 1:", etc. Labels are added automatically by the system.
4. Only mention relationships between attachments if they're obvious (e.g., clear before/after photos)
5. Don't force connections - if items are unrelated, describe each independently
6. Include specific details the agent can reference (names, numbers, prices, dates visible in the media)
</rules>

<attachment_types>
## Attachment Types
- **Image/Video/Audio/Document:** You receive a Gemini media analysis describing the content. Summarize the key details.
- **Link:** You receive the raw text content of a web page the lead shared (fetched from the URL). Summarize what the page is about, key details (prices, services, features), and anything a sales rep needs to discuss it. The URL is included at the top of the raw text.
</attachment_types>

<output_format>
## Output Format
Output valid JSON with a descriptions array. Each object has:
- index: the global_index of the attachment (integer)
- description: factual description of the attachment content (NO LABEL PREFIX)

CORRECT examples:
{
  "descriptions": [
    {"index": 1, "description": "A photo showing the Lead's current smile with visible yellowing and crowding."},
    {"index": 2, "description": "Dr. K Beauty's Opus Plasma skin resurfacing treatment page (drkbeautylv.com/face/opus/). Fractional plasma technology for improving skin texture, addressing aging, sun damage, wrinkles, and scars. Shorter recovery than traditional CO2 laser."}
  ]
}

WRONG (never do this):
{"index": 1, "description": "Image 1: A photo showing..."}
</output_format>"""

_SYNTHESIS_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "descriptions": {
            "type": "array",
            "description": "Array of descriptions, one for each new attachment",
            "items": {
                "type": "object",
                "properties": {
                    "index": {
                        "type": "number",
                        "description": "The global_index of the attachment",
                    },
                    "description": {
                        "type": "string",
                        "description": "Factual description of the attachment content (NO label prefix)",
                    },
                },
                "required": ["index", "description"],
            },
        },
    },
    "required": ["descriptions"],
}


async def _synthesize_descriptions(
    ctx: PipelineContext,
    new_attachments: list[dict[str, Any]],
    resent: list[dict[str, Any]],
    prior: list[dict[str, Any]],
) -> dict[int, str]:
    """Run Synthesis Agent to create context-aware descriptions.

    Takes the raw Gemini analyses + conversation context and produces
    concise descriptions the sales agent can actually use.

    Fetches chat history directly (matching n8n's "Get Chat History Early"
    node) since ctx.timeline hasn't been built yet at this point in the pipeline.

    Returns a dict mapping global_index → synthesized description text.
    Returns empty dict if synthesis fails (caller uses raw analysis as fallback).
    """
    try:
        parts: list[str] = []

        # Fetch conversation context directly (timeline not built yet)
        conversation_context = await _fetch_chat_history_for_synthesis(ctx)
        parts.append("## CONVERSATION HISTORY")
        parts.append(conversation_context if conversation_context else "(No conversation history)")

        parts.append("\n---\n")

        # Re-sent attachments (already have descriptions from DB)
        if resent:
            lines = ["## ATTACHMENTS RE-SENT IN THIS MESSAGE (already in database)"]
            lines.append("(These were sent before and the lead is sending them again now)")
            for att in resent:
                desc = att.get("description", "Previously analyzed")
                lines.append(desc)
            parts.append("\n".join(lines))
        parts.append("\n---\n")

        # Prior attachments (from earlier messages, not in this message)
        if prior:
            lines = ["## OTHER PRIOR ATTACHMENTS (from earlier messages, NOT in this message)"]
            for att in prior:
                desc = att.get("description", "Not yet analyzed")
                lines.append(desc)
            parts.append("\n".join(lines))
        elif not resent:
            parts.append("## PRIOR ATTACHMENTS\n(none - this is the first message with attachments)")
        parts.append("\n---\n")

        # New attachments with raw analyses
        lines = ["## NEW ATTACHMENTS IN THIS MESSAGE (need descriptions)"]
        for att in new_attachments:
            att_type = att.get("type", "unknown")
            type_idx = att.get("type_index", "?")
            global_idx = att.get("_global_index", "?")
            raw = att.get("raw_analysis", "")
            type_cap = att_type.capitalize()
            lines.append(f"\n### {type_cap} {type_idx}")
            lines.append(f"Global index: {global_idx}")
            lines.append(f"Type: {att_type}")
            lines.append(f"\n**Raw Analysis:**")
            lines.append(raw or "(Analysis unavailable)")
        parts.append("\n".join(lines))

        parts.append("\n---\n")

        # Current message(s) — use GHL message bodies from pending attachments
        message_bodies = []
        for pa in ctx.pending_attachments:
            body_text = pa.get("body", "")
            if body_text and body_text not in message_bodies:
                message_bodies.append(body_text)

        parts.append("## CURRENT MESSAGE(S) FROM LEAD")
        if message_bodies:
            for body_text in message_bodies:
                parts.append(f'"{body_text}"')
        else:
            parts.append('"(Lead sent media without any text message)"')
        parts.append("\n---\n")
        parts.append(
            "## YOUR TASK\n\n"
            "Write a description for each NEW attachment above.\n\n"
            "Remember:\n"
            "- NO LABELS (don't start with \"Image 1:\", etc.)\n"
            "- Only describe what you can actually see\n"
            "- Don't make up intent or connections that aren't obvious"
        )

        user_prompt = "\n".join(parts)

        result = await classify(
            prompt=user_prompt,
            schema=_SYNTHESIS_SCHEMA,
            model=resolve_model(ctx, "link_synthesis"),
            temperature=resolve_temperature(ctx, "link_synthesis"),
            system_prompt=_SYNTHESIS_SYSTEM_PROMPT,
            label="link_synthesis",
        )

        descriptions = result.get("descriptions", [])
        logger.info("SYNTHESIS | descriptions=%d", len(descriptions))
        return {d["index"]: d["description"] for d in descriptions if "index" in d and "description" in d}

    except Exception:
        logger.warning("Synthesis Agent failed — using raw analysis as fallback", exc_info=True)
        return {}
