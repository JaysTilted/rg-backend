"""Step 3.10: Response delivery — split messages, email, chat history, webhook response.

For SMS/DM: Splits agent response into 1-3 human-like messages.
For Email: Formats with LLM and sends directly via GHL API.
Stores AI response in chat history table.
Builds the final webhook response dict.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from app.services.ghl_client import GHLClient

import httpx
from prefect import task

from app.models import PipelineContext
from app.services.ai_client import classify
from app.services.postgres_client import postgres
from app.text_engine.utils import format_email_html
from app.text_engine.model_resolver import resolve_model, resolve_temperature

logger = logging.getLogger(__name__)


# =========================================================================
# ORCHESTRATOR
# =========================================================================


@task(name="deliver_response", retries=2, retry_delay_seconds=5, timeout_seconds=90)
async def deliver_response(ctx: PipelineContext) -> dict[str, Any]:
    """Split, store, and build the webhook response.

    Returns the dict that becomes the HTTP response body.
    """
    ghl = ctx.ghl

    # Log full AI response before splitting
    logger.info("DELIVERY | input | channel=%s | test_mode=%s | response_len=%d", ctx.channel, ctx.is_test_mode, len(ctx.agent_response or ""))
    logger.info("DELIVERY | ai_response | %s", (ctx.agent_response or "")[:200])

    # 1. Split response into human-like messages (SMS/DM only)
    setter = ctx.compiled.get("_matched_setter") or {}
    bp_sections = setter.get("bot_persona", {}).get("sections", {})
    skip_splitting = bp_sections.get("message_split", {}).get("skip_splitting", False)

    if ctx.channel.lower() == "email":
        ctx.messages = [ctx.agent_response]
        logger.info("DELIVERY | email_channel | skipping split, single message")
    elif skip_splitting:
        ctx.messages = [ctx.agent_response]
        logger.info("DELIVERY | split_skipped | config=skip_splitting")
    else:
        await _split_response(ctx, bp_sections)

    # Log each split message
    for i, msg in enumerate(ctx.messages, 1):
        logger.info("SPLIT MESSAGE %d/%d | %s", i, len(ctx.messages), msg)

    # 2. Handle email delivery (PROD-ONLY, email channel only)
    if ctx.channel.lower() == "email" and not ctx.is_test_mode:
        logger.info("DELIVERY | sending_email | contact=%s", ctx.contact_id)
        await send_threaded_email(ctx, ctx.agent_response, media_url=ctx.selected_media_url or None, email_model=resolve_model(ctx, "email_formatting"), email_temperature=resolve_temperature(ctx, "email_formatting"))

    # 3. Build serialized chat history for webhook response
    ctx.chat_history_text = _build_chat_history_text(ctx)

    # 4. Send SMS/DM directly via GHL API with delivery confirmation (PROD-ONLY)
    # Email already sent in step 2 via GHL API (send_threaded_email).
    if not ctx.is_test_mode and ctx.channel.lower() != "email":
        from app.services.delivery_service import DeliveryService
        delivery_svc = DeliveryService(ctx.ghl, ctx.config)
        split_result = await delivery_svc.send_split_messages(
            contact_id=ctx.contact_id,
            messages=ctx.messages[:3],
            media_url=ctx.selected_media_url or None,
            to_phone=getattr(ctx, "contact_phone", None),
        )
        ctx._delivery_results = split_result.results
        if split_result.stopped_early:
            logger.warning(
                "DELIVERY | split_stopped_early | contact=%s | delivered=%d/%d",
                ctx.contact_id,
                sum(1 for r in split_result.results if r.status == "delivered"),
                len(ctx.messages),
            )

    # 5. Store AI response in chat history (AFTER delivery — no ghost messages on failure)
    await _store_ai_response(ctx)

    # 6. Build inline webhook response (always returned — for test mode + logging)
    logger.info("DELIVERY | complete | messages=%d | channel=%s | test_mode=%s", len(ctx.messages), ctx.channel, ctx.is_test_mode)
    return _build_webhook_response(ctx)


# =========================================================================
# MESSAGE SPLITTING (SMS/DM)
# =========================================================================

_DEFAULT_TYPO_RULES = """\
### Subtle Typos (natural human feel)
Introduce ONE subtle typo in roughly 1-2 out of every 10 messages (10-20%). Most messages \
should be typo-free.

**How to typo (single-letter changes ONLY — word must stay readable):**
- Swap two adjacent letters: "the" → "teh", "with" → "wiht", "just" → "jsut"
- Skip a letter: "about" → "abut", "would" → "woud", "really" → "realy"
- Double a letter: "recommend" → "reccommend", "definitely" → "definately"
- Adjacent key hit: "good" → "goof", "can" → "csn", "for" → "fir"

**IMPORTANT: Vary your typos.** Don't always pick the same word. Choose different words \
and different typo types each time.

**NEVER typo:** names, prices ($199, $500), times (3:00 PM), dates, phone numbers, \
links/URLs, medical/service terms (Botox, PDRN, EMSculpt, Fotona, collagen, filler)

**Rules:**
- Max 1 typo per entire response
- No correction message needed — the typo should be minor enough that meaning is obvious
- Skip typos on very short messages (under 15 words)"""

_DEFAULT_PUNCTUATION_RULES = """\
### Punctuation
- Single punctuation marks only: ? or ! or .
- NEVER use ??, !!, ..., ?!, or any doubled/mixed punctuation
- ~60% of messages should end with no punctuation (natural texting style)
- Questions keep their single ?
- Excitement keeps single !"""


def _build_split_prompt(bp_sections: dict) -> str:
    """Build the message splitter prompt dynamically based on bot persona config."""
    # Typo rules — toggle from bot persona
    typo_section = bp_sections.get("typos", {})
    typos_enabled = typo_section.get("enabled", True)  # default on
    typo_text = typo_section.get("prompt", "").strip() or _DEFAULT_TYPO_RULES if typos_enabled else ""

    # Punctuation rules — from selected style
    punct_section = bp_sections.get("punctuation_style", {})
    punct_selected = punct_section.get("selected", "casual")
    punct_options = punct_section.get("options", {})
    if punct_selected and punct_selected in punct_options:
        punct_text = f"### Punctuation Style: {punct_options[punct_selected].get('label', punct_selected)}\n{punct_options[punct_selected].get('prompt', '')}"
    elif punct_selected == "casual":
        punct_text = _DEFAULT_PUNCTUATION_RULES
    else:
        punct_text = _DEFAULT_PUNCTUATION_RULES

    parts = [
        """\
<role>
You are a message splitter for SMS/text conversations. You take an AI-generated reply \
and split it into natural, human-like text messages.
</role>

<splitting_rules>
## RULES

### Splitting (STRICT WORD COUNT RULES — FOLLOW EXACTLY)
**Step 1: Count the words in the input message.**
**Step 2: Apply these rules based on word count:**

| Word Count | Max Messages | Rule |
|-----------|-------------|------|
| 1-39 words | 1 | NEVER split. Output as-is. Even if it ends with a question. |
| 40-79 words | 2 | Split into 2 max. |
| 80+ words | 3 | Split into 3 max (HARD CAP). |

**CRITICAL:** Messages under 40 words MUST be 1 message. Do NOT split off a trailing \
question into its own message. "Info + question" under 40 words = 1 message.
- When in doubt, use FEWER messages — under-splitting is always better than over-splitting
</splitting_rules>

<content_preservation>
### Content Preservation (CRITICAL)
- NEVER add or remove words
- Keep all slang, abbreviations, emojis exactly as-is
- Every word from the original must appear exactly once across all messages
</content_preservation>""",
    ]

    if typo_text:
        parts.append(f"<typo_rules>\n{typo_text}\n</typo_rules>")

    parts.append(f"<punctuation_rules>\n{punct_text}\n</punctuation_rules>")

    parts.append("""\
<capitalization_rules>
### Capitalization
- Randomly vary the first letter of each message (capital or lowercase)
- Only the first character changes — rest of message stays as-is
- Mix it up naturally — don't follow a predictable pattern
</capitalization_rules>

<output_format>
## OUTPUT FORMAT
Return ONLY a JSON object:
{{"totalMessages": <1-3>, "message1": "first part", "message2": "second part (if needed)", \
"message3": "third part (if needed)"}}
</output_format>""")

    parts.append("""\
<examples>
## EXAMPLES

Input (13 words): "Hey! Thanks for reaching out. I'd love to help you with that."
Output: {{"totalMessages": 1, "message1": "Hey! Thanks for reaching out. I'd love to help you with that."}}
Reason: Under 40 words - keep as 1 message.

Input (14 words): "Sounds good, I can book you in for Thursday at 2pm. Does that work?"
Output: {{"totalMessages": 1, "message1": "Sounds good, I can book you in for Thursday at 2pm. Does that work?"}}
Reason: Under 40 words - keep as 1 message.

Input (35 words): "Hey Silvara, it's Dr. K Beauty! We just dropped a new EMSculpt bundle - 3 months free weight loss program with any package. Have you heard of EMSculpt before?"
Output: {{"totalMessages": 1, "message1": "hey Silvara, it's Dr. K Beauty! We just dropped a new EMSculpt bundle - 3 months free weight loss program with any package. Have you heard of EMSculpt before?"}}
Reason: Under 40 words - keep as 1 message even though it has multiple sentences.

WRONG (do NOT do this):
Input (38 words): "Yes, the facial is still available for $199. I have some openings for a consultation to get you booked. How about Thursday at 11 AM or Friday at 10 AM?"
Output: {{"totalMessages": 2, "message1": "Yes, the facial is still available...", "message2": "how about Thursday at 11 AM or Friday at 10 AM?"}}
Why wrong: 38 words = under 40 = MUST be 1 message. Do NOT split off time/scheduling questions.
Correct: {{"totalMessages": 1, "message1": "Yes, the facial is still available for $199. I have some openings for a consultation to get you booked. How about Thursday at 11 AM or Friday at 10 AM?"}}

Input (50 words): "This is Emily with Dr. K Beauty. We're reaching out to past contacts with a new special on our Emsculpt packages. Is body contouring something you're interested in, or are you thinking about other treatments? We have openings this week if you'd like to come in."
Output: {{"totalMessages": 2, "message1": "This is Emily with Dr. K Beauty. We're reaching out to past contacts with a new special on our Emsculpt packages", "message2": "is body contouring something you're interested in, or are you thinking about other treatments? We have openings this week if you'd like to come in"}}
Reason: 50 words - split into 2 max.

Input (90+ words): "Great news! We have a special running right now on our facial treatments. It includes a deep cleanse, exfoliation, and LED therapy. The treatment takes about 45 minutes and there's no downtime at all. Most of our clients see results after just one session, but we recommend a series of three for the best outcome. Would you like me to book you in for a consultation this week? I have openings on Tuesday and Thursday."
Output: {{"totalMessages": 3, "message1": "great news! We have a special running right now on our facial treatments. It includes a deep cleanse, exfoliation, and LED therapy", "message2": "The treatment takes about 45 minutes and there's no downtime at all. Most of our clients see results after just one session, but we recommend a series of three for the best outcome", "message3": "would you like me to book you in for a consultation this week? I have openings on Tuesday and Thursday"}}
Reason: 90+ words - split into 3 (the max).
</examples>""")

    return "\n\n".join(parts)

_SPLIT_SCHEMA = {
    "type": "object",
    "properties": {
        "totalMessages": {
            "type": "integer",
            "description": "Number of messages (1-3)",
            "minimum": 1,
            "maximum": 3,
        },
        "message1": {"type": "string", "description": "First message"},
        "message2": {"type": "string", "description": "Second message (or empty)"},
        "message3": {"type": "string", "description": "Third message (or empty)"},
        "reason": {"type": "string", "description": "Brief explanation: word count and why this split count was chosen"},
    },
    "required": ["totalMessages", "message1", "reason"],
    "additionalProperties": False,
}


_PUNCT_STRIP_RE = re.compile(r"[^\w\s]", re.UNICODE)


def _word_overlap_ratio(original: str, split_output: str) -> float:
    """Word-set overlap between original text and split output (0.0-1.0).

    Strips punctuation and lowercases before comparison so that splitter
    transformations (trailing punctuation removal, first-char capitalization)
    don't cause false negatives.
    """
    def tokenize(text: str) -> set[str]:
        cleaned = _PUNCT_STRIP_RE.sub("", text.lower())
        return set(cleaned.split())

    original_words = tokenize(original)
    split_words = tokenize(split_output)

    if not original_words:
        return 1.0

    return len(original_words & split_words) / len(original_words)


async def _split_response(ctx: PipelineContext, bp_sections: dict | None = None) -> None:
    """Split agent response into 1-3 human-like messages."""
    if not ctx.agent_response or not ctx.agent_response.strip():
        ctx.messages = []
        return

    # Short responses (≤ 10 words) skip LLM entirely — acceptable divergence from n8n
    # which always sends to LLM. Typo/lowercase transforms won't apply to very short messages.
    word_count = len(ctx.agent_response.split())
    if word_count <= 10:
        ctx.messages = [ctx.agent_response]
        return

    split_prompt = _build_split_prompt(bp_sections or {})

    try:
        result = await classify(
            prompt=f"<InputMessage>\n{ctx.agent_response}\n</InputMessage>\n\nSplit this message following the rules.",
            schema=_SPLIT_SCHEMA,
            system_prompt=split_prompt,
            model="google/gemini-2.5-flash",
            temperature=resolve_temperature(ctx, "message_splitter"),
            label="message_splitting",
        )

        total = min(result.get("totalMessages", 1), 3)
        messages = []
        for i in range(1, total + 1):
            msg = result.get(f"message{i}", "")
            if msg and msg.strip():
                messages.append(msg.strip())

        # Guard 1: inflation — split significantly longer than original (prompt leak)
        split_total = sum(len(m) for m in messages)
        if split_total > len(ctx.agent_response) * 1.3:
            logger.warning("Split output inflated (%d vs %d chars) — using original", split_total, len(ctx.agent_response))
            ctx.messages = [ctx.agent_response]
        else:
            # Guard 2: word-set overlap — splitter hallucinated different content
            combined_split = " ".join(messages)
            overlap = _word_overlap_ratio(ctx.agent_response, combined_split)
            if overlap < 0.80:
                logger.warning(
                    "Split output failed word overlap (%.1f%% < 80%%) — using original | preview: %s",
                    overlap * 100,
                    combined_split[:100],
                )
                ctx.messages = [ctx.agent_response]
            else:
                ctx.messages = messages if messages else [ctx.agent_response]

    except Exception as e:
        logger.warning("Split response failed, using original: %s", e)
        ctx.messages = [ctx.agent_response]

    msg_lengths = [len(m) for m in ctx.messages]
    logger.info("DELIVERY | split_result | messages=%d | word_count=%d | msg_lengths=%s", len(ctx.messages), word_count, msg_lengths)


# =========================================================================
# STANDALONE SPLITTING (for workflows outside the main pipeline)
# =========================================================================


async def split_message_standalone(
    message: str,
    bp_sections: dict | None = None,
    *,
    temperature: float = 0.5,
    skip_splitting: bool = False,
) -> list[str]:
    """Split a single message into 1-3 human-like SMS texts.

    Standalone version of _split_response() for use by workflows that don't
    run through the full pipeline (post-appointment, reactivation, etc.).
    Applies the same splitting logic, typos, and punctuation rules.

    Args:
        message: The full message text to split.
        bp_sections: Bot persona sections dict (for typo/punctuation config).
                     If None, uses defaults.
        temperature: Splitter LLM temperature (default 0.5).
        skip_splitting: If True, returns [message] without splitting.

    Returns:
        List of 1-3 message strings.
    """
    if not message or not message.strip():
        return []

    if skip_splitting:
        return [message]

    word_count = len(message.split())
    if word_count <= 10:
        return [message]

    split_prompt = _build_split_prompt(bp_sections or {})

    try:
        result = await classify(
            prompt=f"<InputMessage>\n{message}\n</InputMessage>\n\nSplit this message following the rules.",
            schema=_SPLIT_SCHEMA,
            system_prompt=split_prompt,
            model="google/gemini-2.5-flash",
            temperature=temperature,
            label="message_splitting",
        )

        total = min(result.get("totalMessages", 1), 3)
        messages = []
        for i in range(1, total + 1):
            msg = result.get(f"message{i}", "")
            if msg and msg.strip():
                messages.append(msg.strip())

        # Guard 1: inflation
        split_total = sum(len(m) for m in messages)
        if split_total > len(message) * 1.3:
            logger.warning("SPLIT_STANDALONE | inflated (%d vs %d chars) — using original", split_total, len(message))
            return [message]

        # Guard 2: word-set overlap
        combined_split = " ".join(messages)
        overlap = _word_overlap_ratio(message, combined_split)
        if overlap < 0.80:
            logger.warning("SPLIT_STANDALONE | word overlap %.1f%% < 80%% — using original", overlap * 100)
            return [message]

        return messages if messages else [message]

    except Exception as e:
        logger.warning("SPLIT_STANDALONE | failed, using original: %s", e)
        return [message]


# =========================================================================
# EMAIL DELIVERY
# =========================================================================




async def send_threaded_email(
    ctx: PipelineContext,
    message_text: str,
    media_url: str | None = None,
    default_subject: str = "Your inquiry",
    email_model: str = "google/gemini-2.5-flash",
    email_temperature: float = 0.3,
) -> None:
    """Send email via GHL with proper threading — matches n8n flow exactly.

    Flow:
    1. Search conversation by contact
    2. Get last message (limit=1)
    3. Extract email message ID from meta.email.messageIds
    4. Call dedicated email endpoint for subject, threadId, from address
    5. Format HTML via LLM and send

    Used by both reply pipeline (delivery.py) and follow-up pipeline (followup.py).
    """
    ghl = ctx.ghl
    try:
        # Step 1: Get conversation ID
        resp = await ghl._request(
            "GET", "/conversations/search",
            version="2021-04-15",
            params={"contactId": ctx.contact_id, "locationId": ctx.ghl_location_id, "limit": 1},
        )
        conversations = resp.json().get("conversations", [])
        conversation_id = conversations[0]["id"] if conversations else None

        subject = default_subject
        thread_id = None
        email_from = None
        reply_message_id = None

        if conversation_id:
            # Step 2: Get recent messages (limit=20, matching n8n "Get Last Message ID")
            msg_resp = await ghl._request(
                "GET", f"/conversations/{conversation_id}/messages",
                version="2021-04-15",
                params={"limit": 20},
            )
            msgs = msg_resp.json().get("messages", {}).get("messages", [])

            # Step 3: Find first message with email metadata (Extract latest email ID)
            for msg in msgs:
                meta = msg.get("meta", {})
                email_meta = meta.get("email", {})
                message_ids = email_meta.get("messageIds", [])
                if message_ids:
                    reply_message_id = message_ids[-1]
                    break

            # Step 4: Call dedicated email endpoint for subject + threading
            if reply_message_id:
                email_data = await ghl.get_email_by_id(reply_message_id)
                if email_data:
                    # Use subject AS-IS (no manual "RE:" — matches n8n exactly)
                    subject = email_data.get("subject", default_subject)
                    thread_id = email_data.get("threadId")
                    to_list = email_data.get("to", [])
                    email_from = to_list[0] if to_list else None

        # Step 5: Format email with LLM and send
        html_body = await format_email_html(
            message_text,
            bot_persona=ctx.prompts.get("bot_persona", ""),
            contact_name=ctx.contact_name,
            contact_email=ctx.contact_email,
            contact_phone=ctx.contact_phone,
            company=ctx.config.get("name", ""),
            business_phone=ctx.config.get("phone", ""),
            media_url=media_url,
            model=email_model,
            temperature=email_temperature,
        )

        from app.utils.retry import with_retries, is_transient_http

        await with_retries(
            ghl.send_email_reply,
            kwargs=dict(
                contact_id=ctx.contact_id,
                subject=subject,
                message=html_body,
                conversation_id=conversation_id,
                in_reply_to=reply_message_id,
                thread_id=thread_id,
                email_from=email_from,
            ),
            max_attempts=3,
            base_delay=2.0,
            retryable=is_transient_http,
            label="email_send",
        )
        logger.info("Email sent: subject=%s", subject[:50])

    except Exception as e:
        logger.error("Email delivery failed after retries: %s", e, exc_info=True)


# =========================================================================
# CHAT HISTORY STORAGE
# =========================================================================


async def _store_ai_response(ctx: PipelineContext) -> None:
    """Store split AI messages in chat history (matches what the lead receives).

    Stores 1-3 rows (one per split message) with 500ms timestamp offsets
    for correct sort order. Media marker goes on the first message only.
    GHL sync will backfill ghl_message_id on the next pipeline run.
    """
    # Simulator/test mode: skip chat history writes entirely.
    # Messages are captured from pipeline return data and stored in simulations JSONB.
    if ctx.is_test_mode:
        logger.info("DELIVERY | test_mode — skipping chat history write")
        return

    from datetime import datetime, timezone, timedelta

    chat_table = ctx.config.get("chat_history_table_name", "")
    if not chat_table:
        logger.warning("No chat_history_table configured — skipping AI response storage")
        return

    if not ctx.messages:
        logger.warning("No split messages to store — skipping")
        return

    lead_id = ctx.lead.get("id") if ctx.lead else None
    base_ts = datetime.now(timezone.utc)

    # Build media marker (for follow-up dedup/spacing detection)
    marker = None
    if ctx.selected_media_url:
        desc = ""
        if ctx.reply_media_list:
            idx = ctx.selected_media_idx - 1
            if 0 <= idx < len(ctx.reply_media_list):
                desc = ctx.reply_media_list[idx].get("description", "")[:80]
        display_type = "GIF" if ctx.selected_media_type == "gif" else ctx.selected_media_type
        name_desc = ctx.selected_media_name
        if desc:
            name_desc = f"{ctx.selected_media_name} - {desc}"
        marker = f"[AI sent {display_type}: {name_desc}]"

    # Create attachment record for AI-sent media (if any)
    media_attachment_id: str | None = None
    if ctx.selected_media_url:
        try:
            media_desc = ctx.selected_media_name or ""
            if ctx.reply_media_list and ctx.selected_media_idx:
                _midx = ctx.selected_media_idx - 1
                if 0 <= _midx < len(ctx.reply_media_list):
                    lib_desc = ctx.reply_media_list[_midx].get("description", "")
                    if lib_desc:
                        media_desc = f"{media_desc}: {lib_desc}" if media_desc else lib_desc
            record = await postgres.insert_attachment({
                "session_id": ctx.contact_id,
                "client_bot_id": ctx.entity_id,
                "lead_id": lead_id,
                "type": ctx.selected_media_type or "image",
                "url": ctx.selected_media_url,
                "original_url": ctx.selected_media_url,
                "ghl_message_id": None,  # Backfilled below if delivery result has it
                "description": media_desc,
                "raw_analysis": None,
                "message_timestamp": base_ts,
            })
            media_attachment_id = str(record["id"])
            logger.info("Created attachment record for AI media: id=%s, type=%s", media_attachment_id, ctx.selected_media_type)
        except Exception as e:
            logger.warning("Failed to create attachment for AI media: %s", e)

    batch = []
    for i, msg_text in enumerate(ctx.messages):
        content = msg_text

        # Media marker on first (oldest) message only
        if i == 0 and marker:
            content = f"{marker}\n{content}" if content else marker

        kwargs: dict[str, Any] = {
            "source": "AI",
            "channel": ctx.channel,
        }
        # Store media metadata on the first message (legacy compat for old-schema tables)
        if i == 0 and ctx.selected_media_url:
            kwargs["media_url"] = ctx.selected_media_url
            kwargs["media_type"] = ctx.selected_media_type or "image"
            kwargs["media_name"] = ctx.selected_media_name or ""
            if ctx.reply_media_list and ctx.selected_media_idx:
                _midx = ctx.selected_media_idx - 1
                if 0 <= _midx < len(ctx.reply_media_list):
                    kwargs["media_description"] = ctx.reply_media_list[_midx].get("description", "")

        message = {
            "type": "ai",
            "content": content,
            "additional_kwargs": kwargs,
            "response_metadata": {},
        }

        row: dict[str, Any] = {
            "session_id": ctx.contact_id,
            "message": message,
            "timestamp": base_ts + timedelta(milliseconds=500 * i),
            "lead_id": lead_id,
        }
        # Link to the workflow run that produced this message
        if ctx.workflow_run_id:
            row["workflow_run_id"] = ctx.workflow_run_id
        # Attach media attachment ID to first message only
        if i == 0 and media_attachment_id:
            row["attachment_ids"] = [media_attachment_id]
        # Attach ghl_message_id from delivery results (set immediately, no backfill needed)
        delivery_results = getattr(ctx, "_delivery_results", None)
        if delivery_results and i < len(delivery_results):
            dr = delivery_results[i]
            if dr and dr.ghl_message_id:
                row["ghl_message_id"] = dr.ghl_message_id
        batch.append(row)

    from app.utils.retry import with_retries, is_transient_db

    try:
        await with_retries(
            postgres.insert_messages_batch,
            kwargs=dict(table=chat_table, messages=batch),
            max_attempts=3,
            base_delay=1.0,
            retryable=is_transient_db,
            label="store_ai_response",
        )
        logger.info("Stored %d split AI messages in chat history", len(batch))
    except Exception as e:
        logger.error("Chat history storage failed after retries: %s", e)


# =========================================================================
# WEBHOOK RESPONSE BUILDER
# =========================================================================


def _build_chat_history_text(ctx: PipelineContext) -> str:
    """Build serialized chat history for webhook response.

    Matches n8n's JSON.stringify(messages, null, 2) format — pretty-printed
    JSON of the message dicts from chat history, with the agent's response
    appended as a final AI message.
    """
    from datetime import datetime, timezone

    messages = []
    # Chat history rows are newest-first; reverse to chronological order
    for row in reversed(ctx.chat_history or []):
        ts = row.get("timestamp")
        entry = {
            "type": row.get("role", ""),
            "content": row.get("content", ""),
        }
        if ts:
            entry["timestamp"] = ts.isoformat() if hasattr(ts, "isoformat") else str(ts)
        messages.append(entry)

    # Append agent's current response
    if ctx.agent_response:
        messages.append({
            "type": "ai",
            "content": ctx.agent_response,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

    return json.dumps(messages, indent=2, ensure_ascii=False)


def _build_webhook_response(ctx: PipelineContext) -> dict[str, Any]:
    """Build the inline webhook response (for test mode visibility + logging).

    Field names match n8n: Message1-Message3 (no underscore), Chat_History, UserID.
    """
    response: dict[str, Any] = {}

    # Add split messages (max 3 — matches Python's split cap)
    for i in range(1, 4):
        if i <= len(ctx.messages):
            response[f"Message{i}"] = ctx.messages[i - 1]
        else:
            response[f"Message{i}"] = ""

    # Add chat history text
    response["Chat_History"] = ctx.chat_history_text

    # Pass through UserID (GHL Contact ID)
    response["UserID"] = ctx.contact_id

    return response
