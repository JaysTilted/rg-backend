"""Step 3.3: GHL Conversation Sync — sync-on-reply.

Pulls all GHL messages, diffs against Supabase, inserts missing ones.
Also fetches call transcriptions and logs them to Supabase.

This replaces:
  - Manual message logging GHL workflow (2¢/msg)
  - Appointment reminder logging steps
  - Call logging workflow
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import Any, TYPE_CHECKING
from urllib.parse import urlparse

if TYPE_CHECKING:
    from app.services.ghl_client import GHLClient

from prefect import task

from app.models import PipelineContext
from app.services.postgres_client import postgres
from app.services.supabase_client import supabase
from app.text_engine.utils import get_timezone, parse_datetime

logger = logging.getLogger(__name__)

# URL detection regex — matches http(s) URLs in message bodies
_URL_RE = re.compile(r'https?://[^\s<>\"\')\]]+', re.IGNORECASE)

# Domains to never fetch — outbound links WE send, not lead-submitted content
_SKIP_DOMAINS = frozenset({
    "msgsndr.com",             # GHL short links (outbound SMS)
    "leadconnectorhq.com",     # GHL platform links (booking widgets)
    "bookmyleads.ai",          # Our own domain
    "api.bookmyleads.ai",      # Our API
    "refinedgrowth.net",       # Legacy domain
    "api.refinedgrowth.net",   # Legacy API
    "app.gohighlevel.com",     # GHL dashboard
    "calendly.com",            # Common booking tool
})

# Additional email-specific domains to skip (tracking, marketing infra, unsubscribe)
_EMAIL_SKIP_DOMAINS = frozenset({
    # Mailchimp
    "mailchimp.com", "list-manage.com", "campaign-archive.com", "mailchi.mp",
    # Major ESPs
    "constantcontact.com", "sendgrid.net", "mailgun.org", "mandrillapp.com",
    "createsend.com", "aweber.com", "getresponse.com",
    # CRM / marketing automation
    "hubspot.com", "hs-analytics.net", "hubspotlinks.com",
    "activecampaign.com", "klaviyo.com", "getdrip.com",
    "convertkit.com", "ck.page",
    "brevo.com", "sendinblue.com", "sib.li",
    "keap-link.com", "infusionsoft.com",
    # Email infrastructure
    "litmus.com", "emailprotection.link",
})

# Domain prefixes that indicate tracking/redirect links in emails
_EMAIL_TRACKING_PREFIXES = (
    "click.", "track.", "trk.", "links.", "email.", "t.",
    "open.", "url.", "go.",
)

# Patterns that mark the start of reply chains or signatures in emails
_EMAIL_CUTOFF_RE = re.compile(
    r'^(?:'
    # Reply chain markers
    r'On\s.+wrote:\s*$'                    # "On Mon, Jan 5, 2026... wrote:"
    r'|----\s*Original Message\s*----'      # Outlook-style
    r'|From:\s*\S+@\S+'                     # "From: user@domain.com"
    r'|>\s'                                 # Quoted reply line
    # Signature dividers
    r'|--\s*$'                              # Standard email signature divider "-- "
    # Sign-off phrases (must be start of line)
    r'|Best[\s,]'                           # "Best," / "Best regards,"
    r'|Thanks[\s,]'                         # "Thanks," / "Thanks!"
    r'|Thank\s+you[\s,]'                    # "Thank you,"
    r'|Regards[\s,]'                        # "Regards,"
    r'|Kind\s+regards'                      # "Kind regards,"
    r'|Warm\s+regards'                      # "Warm regards,"
    r'|Cheers[\s,]'                         # "Cheers,"
    r'|Sincerely[\s,]'                      # "Sincerely,"
    r'|V/r[\s,]'                            # Military "Very respectfully"
    # Mobile / app footers
    r'|Sent from my\s'                      # "Sent from my iPhone"
    r'|Get Outlook'                         # Outlook mobile
    # Legal / confidentiality notices
    r'|CONFIDENTIAL'                        # "CONFIDENTIALITY NOTICE"
    r'|This\s+email\s+(?:is\s+)?(?:confidential|intended|privileged)'
    # Newsletter / marketing footers
    r'|You\s+received\s+this'              # "You received this because..."
    r'|Manage\s+your\s+preferences'
    r'|View\s+in\s+browser'
    r')',
    re.IGNORECASE | re.MULTILINE,
)

_MAX_LINKS_PER_MESSAGE = 3


def _strip_email_noise(body: str) -> str:
    """Strip reply chains, signatures, and footers from email body.

    Returns only the "fresh content" — the part the lead actually typed.
    """
    lines = body.split("\n")
    clean_lines: list[str] = []
    for line in lines:
        if _EMAIL_CUTOFF_RE.search(line):
            break
        clean_lines.append(line)
    return "\n".join(clean_lines)


def _extract_urls_from_body(body: str, channel: str = "SMS") -> list[str]:
    """Extract lead-submitted URLs from message body, filtering out our own domains.

    For email channels, strips reply chains and signatures first, then applies
    additional email-specific domain filtering (tracking, marketing infra).
    """
    if not body:
        return []

    is_email = channel == "Email"

    # For email: strip signatures, reply chains, footers before extracting URLs
    text = _strip_email_noise(body) if is_email else body

    matches = _URL_RE.findall(text)
    urls: list[str] = []
    for url in matches:
        # Strip trailing punctuation that regex may have captured
        url = url.rstrip(".,;:!?")
        try:
            parsed = urlparse(url)
            domain = (parsed.netloc or "").lower().lstrip("www.")

            # Universal skip domains
            if any(domain.endswith(skip) for skip in _SKIP_DOMAINS):
                continue

            # Email-specific filters
            if is_email:
                if any(domain.endswith(skip) for skip in _EMAIL_SKIP_DOMAINS):
                    continue
                if any(domain.startswith(prefix) for prefix in _EMAIL_TRACKING_PREFIXES):
                    continue
                # Skip opt-out / preference management paths
                path_lower = (parsed.path or "").lower()
                if any(kw in path_lower for kw in (
                    "unsubscribe", "opt-out", "optout",
                    "manage-preferences", "email-preferences",
                )):
                    continue
        except Exception:
            continue
        urls.append(url)
        if len(urls) >= _MAX_LINKS_PER_MESSAGE:
            break

    return urls


# ============================================================================
# CALL SUMMARIZER PROMPT (ported from n8n AI Summarizer)
# ============================================================================

_CALL_SUMMARIZER_PROMPT = """\
<role>
You summarize phone call transcripts for an AI-powered CRM system.

## Purpose
Your summaries are critical context for AI agents that will continue automated conversations with this lead via text/email. The AI agents ONLY know what's in these summaries - they cannot listen to the original call. If you miss important details, the AI will lack context and may confuse or frustrate the lead.
</role>

<speaker_identification>
## Understanding Speaker Identity

Transcripts may not have speaker labels. Use call direction to identify speakers:

**Outbound calls (we called the lead):**
- First speaker = Staff (we initiated)
- Second speaker = Lead (they answered)

**Inbound calls (lead called us):**
- First speaker = Lead (they initiated)
- Second speaker = Staff (we answered)

**Speaker clues to watch for:**
- Staff typically: greets with business name, offers appointments, quotes prices, asks qualifying questions
- Lead typically: asks questions, expresses interest/concerns, confirms personal details, makes decisions

**If voicemail:**
- Outbound voicemail = Staff left the message
- Inbound voicemail = Lead left the message (rare, usually goes to business voicemail system)

Always attribute statements correctly in your summary ("Staff offered..." vs "Lead asked...").
</speaker_identification>

<what_to_capture>
## What to Capture

### Always Include:
- Purpose of the call (why they called or why we called them)
- Specific services/products discussed
- Prices, quotes, or estimates mentioned
- Dates, times, or timeframes discussed
- Appointments booked, rescheduled, or cancelled
- Questions the lead asked (even if unanswered)
- Concerns, objections, or hesitations expressed
- Personal details shared (spouse approval needed, budget constraints, timeline, preferences)
- Commitments made by either party ("I'll send you info", "I'll call back Tuesday")
- Next steps agreed upon
- If voicemail: exactly what message was left

### Lead Details to Capture:
- Name preferences ("Call me Mike, not Michael")
- Decision influencers ("Need to ask my wife", "Business partner decides")
- Budget mentions ("Looking to stay under $500", "Price isn't an issue")
- Timeline ("Need this done before my wedding in March")
- Prior experience ("I've had Botox before but never filler")
- Specific concerns ("I'm worried about downtime", "Last provider was rude")
- Referral source if mentioned ("My friend Sarah recommended you")

### Outcome Indicators:
- Hot: Ready to buy/book now
- Warm: Interested but needs time/info/approval
- Cold: Not interested, wrong number, or do not contact
- Callback needed: They asked us to call back at specific time

### Edge Cases to Note:
- Wrong number: "Wrong number - lead was looking for a different business"
- Do not contact: "Lead requested to be removed from contact list"
- Complaint/upset: Note what they're upset about and any resolution offered
- Payment taken: Amount charged, what for, payment method if mentioned
- Transferred call: Who they were transferred to and why
- Call disconnected: If call dropped unexpectedly, note where conversation left off
- Returning customer: If they mention prior visits/purchases, note what they reference
- Competitor mentions: If they're comparing to competitors or got other quotes
- Language barrier: If communication was difficult, note preferred language if mentioned
- Third party caller: If someone called on behalf of the lead (spouse, assistant, parent)
</what_to_capture>

<rules>
## Rules
- 2-4 sentences, but include ALL critical details even if slightly longer
- Use third person ("Lead asked..." not "You asked...")
- Be specific - "Thursday 2pm" not "later this week"
- Include exact numbers - "$2,500 package" not "discussed pricing"
- Note tone only if notable - "Lead seemed frustrated about wait time"
- If details are unclear in transcript, note that: "Lead mentioned a date but audio unclear"
</rules>

<examples>
## Examples

### Home Services
"Lead called about AC repair - unit not cooling, making grinding noise. Available for service call tomorrow morning or Friday afternoon. Quoted $89 diagnostic fee. Lead confirmed tomorrow 9-11am window works, will have someone home."

"Left voicemail for lead who requested quote for fence installation. Mentioned we received their online form, have availability next week for estimate, asked them to call back to schedule."

### Med Spa / Aesthetics
"Lead inquired about EMSculpt for abdomen and buttocks. Explained treatment process and quoted $3,000 for 4-session package. Lead interested but needs to discuss with husband first. Will call back by end of week. Also asked about financing options - mentioned we offer Cherry financing."

"Staff followed up on missed appointment from yesterday. Lead apologized, had family emergency. Rescheduled Botox appointment to Monday 3pm. Lead confirmed she wants same 40 units as discussed previously."

### Dental
"New patient called about toothache, upper right side, started 3 days ago. No current dentist. Scheduled emergency exam for tomorrow 2pm. Lead has Delta Dental PPO insurance. Reminded to arrive 15 min early for paperwork."

"Lead called to reschedule cleaning from Thursday to next week. Now booked for Tuesday 10am. Asked about teeth whitening - interested in take-home trays. Will discuss options at cleaning appointment."

### Legal Services
"Potential client called about car accident case from last month. Was rear-ended, has ongoing neck pain, other driver was cited. Currently treating with chiropractor. Lead wants to know options before medical bills pile up. Scheduled free consultation for Wednesday 4pm. Lead mentioned other driver's insurance already called them - advised not to give recorded statement."

### Real Estate
"Buyer lead called about listing on 123 Oak Street. Interested in 4-bed homes in Summerlin area, budget $500-600k, needs good schools for two kids (elementary age). Pre-approved with Wells Fargo. Available for showings weekends only. Scheduled 3 home tours for Saturday starting 10am."

### Fitness / Gym
"Lead toured facility, interested in family membership. Has spouse and 2 teens. Asked about basketball courts and pool hours. Quoted $149/month family rate, $99 enrollment (can waive if signs today). Lead wants to bring spouse back tomorrow evening to see facility. Tentatively holding the waived enrollment offer until end of week."

### Contractor / Trades
"Homeowner needs kitchen remodel - wants new cabinets, countertops, and backsplash. Budget around $25-30k. Showed photos of style they like (modern white shaker). House is 1985 build. Scheduled in-home estimate for Saturday 1pm. Lead mentioned they're getting 3 quotes."

### Voicemail Examples
"Left voicemail: Thanked lead for inquiry about roof inspection. Mentioned we have availability this week, free estimates, and asked them to call back at 555-1234 or reply to this text to schedule."

"Left voicemail: Following up on proposal sent last week for bathroom remodel. Mentioned the 10% discount expires Friday. Asked lead to call back with any questions."

### Edge Case Examples
"Wrong number - caller was looking for a pizza restaurant, not our office."

"Lead called to complain about being charged twice for last month's service. Apologized and confirmed refund will process in 3-5 business days. Lead satisfied with resolution."

"Lead's wife called to reschedule his appointment. Moved from Thursday to Monday 2pm. She mentioned he's nervous about the procedure - may want extra consultation time."

"Call disconnected mid-conversation while discussing pricing. Lead had shown interest in the premium package ($299/month). Should follow up to continue discussion."

"Lead requested to be removed from all contact lists. Do not contact again."

"Lead is comparing quotes - mentioned they got an estimate from ABC Company for $2,000 less. Asked if we can match or explain the difference in value."

"Returning customer called - referenced getting Botox here 6 months ago with 'the blonde lady.' Wants to book again, same treatment. Last time was 30 units forehead and crow's feet."

"Spanish-speaking lead called. Limited English but communicated she needs appointment for husband. Prefers Spanish-speaking staff if available. Booked consultation for Friday 11am, noted to have Maria available."
</examples>

<filtering>
## Filtering: Should This Call Be Logged?

Before summarizing, determine if this call has ANY value for future AI context.

### DO NOT LOG (should_log: false):

**System Errors:**
- "mailbox is full" / "mailbox that is full"
- "mailbox has not been set up" / "hasn't been set up"
- "cannot accept messages" / "cannot accept any messages"
- "nothing has been recorded"
- "sorry, but the voice mail belonging to" (voicemail full mid-recording)
- "I couldn't hear you. Please try again" (audio capture failed)
- "google subscriber you have called is not available" with NO message left after

**Voicemail Prompts with No Message:**
The KEY is whether actual content exists AFTER the voicemail system prompt.

DO NOT LOG if transcript ENDS with:
- "you may hang up" / "you may hang up."
- "for further options" / "press pound for further options"
- "press 1 for more options"
- "after the tone" / "record your message after the tone"
- "please leave a message" / "please leave me a message" (with nothing following)
- "goodbye." (after a system error)

Examples of NO MESSAGE (don't log):
- "Your call has been forwarded to voicemail... you may hang up." [END]
- "Telephone number 702... can't take your call... press pound for further options." [END]
- "Hey, this is Becky. Please leave me a message." [END - no message followed]
- "Google subscriber not available. Please leave a message after the tone." [END]

**Other Useless:**
- Cut-off transcripts under 5 seconds with no meaningful content
- Just "hello?" or "hello? hello?" with no actual response or interaction
- Random wrong-context calls with no business relevance (e.g., "mommy, who is that?" with no follow-up)

### ALWAYS LOG (should_log: true):

**Voicemails WITH Messages Left:**
If there's actual content AFTER the voicemail prompt, LOG IT:
- "...please leave a message after the tone. Hello, this is Doctor Kay Beauty's office..." = LOG (message was left!)
- "...press pound for further options. Hi, good morning, this is Doctor Kay..." = LOG (message was left!)

**Any Human Interaction:**
- Brief exchanges: "wrong number", "call me later", "not interested", "I'm busy"
- Language barriers: "do you speak Spanish?" - valuable context!
- Callback requests: "can you call me back tomorrow?"
- Hang-ups after brief conversation - still context

**Business Context:**
- Any mention of: appointments, pricing, services, scheduling, interest, questions, names
- Any sentiment indicator: frustration, excitement, confusion
- Staff identifying themselves and leaving info
- Wrong number confirmations - tells AI to stop contacting

**Rule of thumb**: If the transcript contains ANY human words beyond the voicemail system, lean toward logging. Only skip pure system failures and recordings where no message was left.
</filtering>

<output_format>
## Output Format

Return a JSON object with exactly three fields:

1. **should_log**: Boolean - true if this call has any context value (see Filtering section above), false if it's pure system noise/errors
2. **summary**: Your 2-4 sentence summary following all rules above (if should_log is false, just put "System error or empty recording - no useful context")
3. **user_sentiment**: The lead's overall sentiment during the call (if should_log is false, use "neutral"):
   - "positive" - Lead was friendly, enthusiastic, agreeable, or satisfied
   - "neutral" - Lead was matter-of-fact, neither positive nor negative, just transactional
   - "negative" - Lead was frustrated, upset, annoyed, dismissive, or complained

Base sentiment on the LEAD's tone, not staff's. If mixed (started negative but ended positive), use the ending sentiment. If voicemail with no lead interaction, use "neutral".
</output_format>\
"""

_CALL_SUMMARIZER_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "should_log": {"type": "boolean"},
        "summary": {"type": "string"},
        "user_sentiment": {
            "type": "string",
            "enum": ["positive", "neutral", "negative"],
        },
    },
    "required": ["should_log", "summary", "user_sentiment"],
    "additionalProperties": False,
}


def _format_services_for_summarizer(config: dict[str, Any]) -> str:
    """Extract qualification criteria from service_config for summarizer context.

    Tells the summarizer what to pay attention to without making it think
    it's the qualification agent. Returns empty string if no service_config.
    """
    _sc = config.get("system_config") or {}
    from app.text_engine.agent_compiler import resolve_setter
    _setter = resolve_setter(_sc, "") or {}
    service_config = _setter.get("services")
    if not service_config or not isinstance(service_config, dict):
        return ""

    qualifications = service_config.get("global_qualifications", [])
    if not qualifications:
        return ""

    lines = [
        "\n## Qualification Criteria (for context only)",
        "This business evaluates leads on the following criteria.",
        "You are NOT the qualification agent — do not evaluate these.",
        "Just capture any details the lead mentioned that relate to these.\n",
    ]
    for q in qualifications:
        name = q.get("name", "")
        qualified_desc = q.get("qualified", "")
        if name:
            lines.append(f"- {name}: {qualified_desc}" if qualified_desc else f"- {name}")

    return "\n".join(lines)


# GHL messageType → friendly channel name (matches timeline legend + agency interface)
_GHL_CHANNEL_MAP: dict[str, str] = {
    "TYPE_SMS": "SMS",
    "TYPE_PHONE": "SMS",
    "TYPE_CUSTOM_SMS": "SMS",
    "TYPE_EMAIL": "Email",
    "TYPE_CUSTOM_EMAIL": "Email",
    "TYPE_CALL": "Call",
    "TYPE_FACEBOOK": "FB",
    "TYPE_INSTAGRAM": "IG",
    "TYPE_WHATSAPP": "WhatsApp",
    "TYPE_LIVE_CHAT": "Live Chat",
    "TYPE_GMB": "GMB",
}


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================


@task(name="sync_conversation", retries=2, retry_delay_seconds=3, timeout_seconds=60)
async def sync_conversation(ctx: PipelineContext) -> None:
    """Sync GHL conversation history into Supabase chat history.

    Runs before the AI processes anything so the conversation context
    is complete and up-to-date. Thin wrapper around run_conversation_sync()
    that unpacks PipelineContext.
    """
    chat_table = ctx.config.get("chat_history_table_name", "")
    if not chat_table:
        logger.warning("No chat_history_table_name in config — skipping sync")
        return

    entity_id = ctx.config.get("id", ctx.entity_id)

    stats = await run_conversation_sync(
        contact_id=ctx.contact_id,
        ghl=ctx.ghl,
        chat_table=chat_table,
        entity_id=entity_id,
        config=ctx.config,
        lead=ctx.lead,
        workflow_run_id=ctx.workflow_run_id or None,
    )
    ctx.sync_stats = stats
    ctx.pending_attachments = stats.get("pending_attachments", [])


# ============================================================================
# GHL CONVERSATION SYNC
# ============================================================================


async def run_conversation_sync(
    contact_id: str,
    ghl: GHLClient,
    chat_table: str,
    entity_id: str,
    config: dict[str, Any],
    lead: dict[str, Any] | None = None,
    workflow_run_id: str | None = None,
) -> dict[str, Any]:
    """Pull GHL messages, diff against Supabase, insert missing ones.

    Decoupled from PipelineContext so any workflow can call this directly
    (text engine, missed call text-back, etc.).

    Categorization logic:
    - direction=inbound → lead_reply (safety net)
    - source=app → manual (replaces manual logging workflow)
    - source=workflow → workflow (drips, reminders, campaigns, etc.)
    - messageType=TYPE_CALL → fetch transcription → call_logs (only if transcript exists)
    """
    # 1. Pull all GHL messages
    ghl_messages = await ghl.get_conversation_messages(contact_id)
    if not ghl_messages:
        logger.info("CONV_SYNC | no GHL messages found for contact=%s", contact_id)
        return {"ghl_messages_fetched": 0, "messages_synced": 0, "calls_found": 0, "calls_synced": 0}

    logger.info("CONV_SYNC | ghl_messages_fetched=%d | contact=%s", len(ghl_messages), contact_id)

    # 2. Pull existing Supabase messages and extract GHL message IDs for dedup
    existing = await postgres.get_chat_history(chat_table, contact_id, limit=500)
    existing_ghl_ids: set[str] = set()
    existing_timestamps: set[str] = set()
    for row in existing:
        ghl_id = row.get("ghl_message_id") or ""
        if ghl_id:
            existing_ghl_ids.add(ghl_id)
        ts = row.get("timestamp")
        if ts:
            if isinstance(ts, datetime):
                existing_timestamps.add(ts.isoformat())
            else:
                existing_timestamps.add(str(ts))

    logger.info("CONV_SYNC | existing_messages=%d | existing_ghl_ids=%d", len(existing), len(existing_ghl_ids))

    # 2b. Build lookup for text-match backfill: AI messages stored by delivery.py
    # that don't have a ghl_message_id yet (sync will backfill from GHL)
    _ai_content_lookup: dict[str, list[dict[str, Any]]] = {}
    for row in existing:
        if row.get("ghl_message_id"):
            continue
        if row.get("role") != "ai":
            continue
        if (row.get("source") or "") != "AI":
            continue
        content = (row.get("content") or "").strip()
        if not content:
            continue
        normalized = _strip_media_marker(content)
        _ai_content_lookup.setdefault(normalized, []).append(row)

    # 3. Process each GHL message
    to_insert: list[dict[str, Any]] = []
    call_messages: list[dict[str, Any]] = []
    pending_attachments: list[dict[str, Any]] = []
    skipped_activity = 0
    skipped_empty = 0
    skipped_dedup = 0
    backfilled = 0
    categorized = {"inbound": 0, "manual": 0, "workflow": 0}

    for msg in ghl_messages:
        ghl_msg_id = msg.get("id", "")
        msg_type = msg.get("messageType", "")
        direction = msg.get("direction", "")
        source = msg.get("source", "")
        date_added = msg.get("dateAdded", "")
        msg_attachments = msg.get("attachments", [])

        # Collect attachment data BEFORE dedup — attachments may need
        # re-processing even when the message row already exists.
        # The downstream filter (get_processed_attachment_message_ids)
        # handles skipping already-processed attachments.
        body = msg.get("body", "")
        if msg_attachments and ghl_msg_id:
            pending_attachments.append({
                "ghl_message_id": ghl_msg_id,
                "urls": msg_attachments,
                "body": body,
                "date_added": date_added,
            })

        # Extract URLs from inbound message bodies (treat as link attachments)
        if direction == "inbound" and body and ghl_msg_id:
            channel = _GHL_CHANNEL_MAP.get(msg_type, "SMS")
            link_urls = _extract_urls_from_body(body, channel=channel)
            if link_urls:
                existing_pa = next(
                    (pa for pa in pending_attachments if pa["ghl_message_id"] == ghl_msg_id),
                    None,
                )
                if existing_pa:
                    existing_pa["link_urls"] = link_urls
                else:
                    pending_attachments.append({
                        "ghl_message_id": ghl_msg_id,
                        "urls": [],
                        "link_urls": link_urls,
                        "body": body,
                        "date_added": date_added,
                    })

        # Skip if already in Supabase (by GHL message ID or timestamp proximity)
        if ghl_msg_id and ghl_msg_id in existing_ghl_ids:
            skipped_dedup += 1
            continue
        if _timestamp_exists(date_added, existing_timestamps):
            skipped_dedup += 1
            continue

        # Categorize
        if msg_type == "TYPE_CALL":
            call_messages.append(msg)
            continue

        # Skip activity messages (opportunity events, appointment events, etc.)
        if msg_type.startswith("TYPE_ACTIVITY"):
            skipped_activity += 1
            continue

        # Skip if no body AND no attachments
        if not body and not msg_attachments:
            skipped_empty += 1
            continue

        if direction == "inbound":
            # Lead reply — individual bubble from GHL conversation
            to_insert.append(_make_sync_msg(
                contact_id, lead, body, "human", "lead_reply", msg, date_added,
            ))
            categorized["inbound"] += 1

        elif source == "app":
            # Manual message from staff
            msg_dir = "ai" if direction == "outbound" else "human"
            to_insert.append(_make_sync_msg(
                contact_id, lead, body, msg_dir, "manual", msg, date_added,
            ))
            categorized["manual"] += 1

        elif source == "workflow":
            # Outbound workflow messages: could be AI replies (already stored
            # by delivery.py as split messages) or genuine workflow messages
            # (drips, reminders, campaigns). Try text-match backfill first.
            body_stripped = body.strip()
            matched_rows = _ai_content_lookup.get(body_stripped, [])
            if matched_rows and ghl_msg_id:
                # Backfill: this GHL message matches a delivery.py-stored row
                match_row = matched_rows.pop(0)
                await postgres.backfill_ghl_message_id(
                    chat_table, contact_id, match_row["timestamp"], ghl_msg_id,
                )
                existing_ghl_ids.add(ghl_msg_id)
                backfilled += 1
                logger.info("CONV_SYNC | backfilled ghl_message_id=%s via text match", ghl_msg_id[:20])
            else:
                # No match — genuine workflow message, insert as before
                to_insert.append(_make_sync_msg(
                    contact_id, lead, body, "ai", "workflow", msg, date_added,
                ))
                categorized["workflow"] += 1

        # Other sources (e.g., direct API) → skip for now

    logger.info(
        "CONV_SYNC | skipped_dedup=%d | backfilled=%d | skipped_activity=%d | skipped_empty=%d | inbound=%d | manual=%d | workflow=%d | calls=%d | pending_atts=%d",
        skipped_dedup, backfilled, skipped_activity, skipped_empty,
        categorized["inbound"], categorized["manual"], categorized["workflow"],
        len(call_messages), len(pending_attachments),
    )

    # 4. Insert messages (grouping happens in Step 3.5 timeline)
    if to_insert:
        # Tag all synced messages with the workflow run that triggered this sync
        if workflow_run_id:
            for msg in to_insert:
                msg["workflow_run_id"] = workflow_run_id
        await postgres.insert_messages_batch(chat_table, to_insert)
        logger.info("CONV_SYNC | inserted=%d messages", len(to_insert))

    # 5. Filter already-processed attachments
    if pending_attachments:
        existing_att_ids = await postgres.get_processed_attachment_message_ids(
            contact_id, entity_id
        )
        pending_attachments = [
            pa for pa in pending_attachments
            if pa["ghl_message_id"] not in existing_att_ids
        ]
        logger.info("CONV_SYNC | pending_attachments=%d (after dedup)", len(pending_attachments))

    # 6. Process call transcriptions
    calls_synced = 0
    if call_messages:
        # Resolve call summarization model + temperature from matched setter
        _sc = config.get("system_config") or {}
        from app.text_engine.agent_compiler import resolve_setter
        _setter = resolve_setter(_sc, "") or {}  # use default setter
        _ai_models = _setter.get("ai_models") or {}
        _ai_temps = _setter.get("ai_temperatures") or {}
        _call_sum_model = _ai_models.get("call_summarization", "google/gemini-2.5-flash")
        _call_sum_temp = float(_ai_temps.get("call_summarization", 0.3))
        calls_synced = await _sync_call_logs(contact_id, ghl, call_messages, entity_id, config, lead, call_summarization_model=_call_sum_model, call_summarization_temperature=_call_sum_temp)

    # Extract last call status from raw call messages (before transcript filtering)
    # Important: missed calls often have no transcript and get skipped by _sync_call_logs,
    # but we still need their status for the /call-event endpoint routing.
    last_call_status = None
    if call_messages:
        sorted_calls = sorted(call_messages, key=lambda m: m.get("dateAdded", ""), reverse=True)
        last_call_meta = sorted_calls[0].get("meta", {}).get("call", {})
        last_call_status = last_call_meta.get("status", "unknown")

    return {
        "ghl_messages_fetched": len(ghl_messages),
        "messages_synced": len(to_insert),
        "calls_found": len(call_messages),
        "calls_synced": calls_synced,
        "pending_attachments": pending_attachments,
        "last_call_status": last_call_status,
    }


# ============================================================================
# CALL TRANSCRIPTION SYNC
# ============================================================================


async def _sync_call_logs(
    contact_id: str,
    ghl: GHLClient,
    call_messages: list[dict[str, Any]],
    entity_id: str,
    config: dict[str, Any],
    lead: dict[str, Any] | None = None,
    call_summarization_model: str = "google/gemini-2.5-flash",
    call_summarization_temperature: float = 0.3,
) -> int:
    """Fetch transcriptions for calls, summarize with AI, and log to Supabase.

    Uses a single classify() call with the full summarizer prompt to produce
    should_log, summary, and user_sentiment in one shot. Junk calls (system
    errors, empty voicemails) are filtered via should_log=false.
    """
    from app.services.ai_client import classify

    # Build system prompt: base + optional qualification criteria
    system_prompt = _CALL_SUMMARIZER_PROMPT
    qual_section = _format_services_for_summarizer(config)
    if qual_section:
        system_prompt += qual_section

    # Fetch existing call logs once (not per-message)
    existing_logs = await supabase.get_call_logs(
        contact_id, entity_id, limit=50
    )

    synced_count = 0
    filtered_count = 0
    for msg in call_messages:
        meta = msg.get("meta", {}).get("call", {})
        message_id = msg.get("id", "")
        date_added = msg.get("dateAdded", "")

        # Two-tier dedup: exact ghl_message_id match first, timestamp fallback second
        already_logged = any(
            log.get("ghl_message_id") == message_id
            for log in existing_logs
            if message_id
        ) or any(
            _timestamps_close(log.get("created_at", ""), date_added, seconds=60)
            for log in existing_logs
        )
        if already_logged:
            continue

        # Fetch transcription (may be empty for missed/short calls)
        transcript_text = ""
        transcription = await ghl.get_call_transcription(message_id)
        if transcription:
            transcript_text = _format_transcription(transcription)

        if not transcript_text:
            logger.info("No transcription for call %s (status: %s) — skipping",
                        message_id, meta.get("status", "unknown"))
            continue

        # Single AI call: summary + sentiment + should_log
        # Direction may be in meta.call OR top-level msg (GHL inconsistency)
        call_direction = meta.get("direction") or msg.get("direction") or "unknown"
        call_status = meta.get("status", "unknown")
        call_duration = meta.get("duration", 0)
        direction_hint = "Lead called us" if call_direction == "inbound" else "We called the lead"

        summary = ""
        user_sentiment = ""
        should_log = True  # default to logging on failure

        try:
            result = await classify(
                system_prompt=system_prompt,
                prompt=(
                    f"Call Direction: {call_direction} ({direction_hint})\n"
                    f"Call Status: {call_status}\n"
                    f"Duration: {call_duration}s\n\n"
                    f"Transcript:\n{transcript_text}"
                ),
                schema=_CALL_SUMMARIZER_SCHEMA,
                model=call_summarization_model,
                temperature=call_summarization_temperature,
                label="call_summarization",
            )
            should_log = result.get("should_log", True)
            summary = result.get("summary", "")
            user_sentiment = result.get("user_sentiment", "")
        except Exception:
            logger.warning("Failed to summarize call %s — logging with empty summary",
                           message_id, exc_info=True)

        # Skip junk calls
        if not should_log:
            filtered_count += 1
            logger.info("Filtered junk call %s (status: %s, duration: %ds)",
                        message_id, call_status, call_duration)
            continue

        # Determine after_hours flag (only for calls we're keeping)
        after_hours = _is_after_hours(date_added, config)

        # Insert call log
        await supabase.insert_call_log(
            {
                "ghl_contact_id": contact_id,
                "entity_id": entity_id,
                "lead_id": lead.get("id") if lead else None,
                "ghl_message_id": message_id,
                "transcript": transcript_text,
                "summary": summary,
                "duration_seconds": call_duration,
                "direction": call_direction,
                "status": call_status,
                "from_number": meta.get("from", ""),
                "to_number": meta.get("to", ""),
                "after_hours": after_hours,
                "user_sentiment": user_sentiment,
            },
        )
        synced_count += 1
        logger.info("Synced call log for message %s (status: %s, duration: %ds, after_hours=%s, sentiment=%s)",
                     message_id, call_status, call_duration, after_hours, user_sentiment)

    if filtered_count:
        logger.info("CALL_SYNC | filtered_junk=%d | synced=%d", filtered_count, synced_count)

    return synced_count


# ============================================================================
# HELPERS
# ============================================================================


def _is_after_hours(date_added: str, config: dict[str, Any]) -> bool:
    """Check if a call timestamp falls outside business hours.

    Uses the client's business_schedule JSONB and timezone from Supabase.
    """
    try:
        from app.text_engine.utils import extract_business_hours

        tz = get_timezone(config)
        call_dt = parse_datetime(date_added).astimezone(tz)

        hours_start, hours_end, enabled_days = extract_business_hours(config)

        # Check business days
        if enabled_days:
            day_name = call_dt.strftime("%A")
            if day_name not in enabled_days:
                return True

        # Check business hours
        if hours_start and hours_end:
            start_parts = [int(p) for p in str(hours_start).split(":")[:2]]
            end_parts = [int(p) for p in str(hours_end).split(":")[:2]]
            start_minutes = start_parts[0] * 60 + start_parts[1]
            end_minutes = end_parts[0] * 60 + end_parts[1]
            call_minutes = call_dt.hour * 60 + call_dt.minute
            if call_minutes < start_minutes or call_minutes >= end_minutes:
                return True

        return False
    except Exception:
        return False


def _make_sync_msg(
    contact_id: str,
    lead: dict[str, Any] | None,
    body: str,
    msg_type: str,
    source: str,
    ghl_msg: dict[str, Any],
    date_added: str,
) -> dict[str, Any]:
    """Build a sync message dict for batch insert."""
    ts = parse_datetime(date_added)
    ghl_msg_id = ghl_msg.get("id", "")
    return {
        "session_id": contact_id,
        "message": {
            "type": msg_type,
            "content": body,
            "additional_kwargs": {
                "source": source,
                "channel": _GHL_CHANNEL_MAP.get(ghl_msg.get("messageType", ""), "SMS"),
                "ghl_message_id": ghl_msg_id,
            },
            "response_metadata": {},
        },
        "timestamp": ts,
        "lead_id": lead.get("id") if lead else None,
        "ghl_message_id": ghl_msg_id,
    }


_MEDIA_MARKER_RE = re.compile(r"^\[AI sent [^\]]+\]\n?")


def _strip_media_marker(content: str) -> str:
    """Strip [AI sent TYPE: NAME] marker prefix for text-match dedup."""
    return _MEDIA_MARKER_RE.sub("", content).strip()


def _timestamp_exists(ghl_date: str, existing: set[str], tolerance_seconds: int = 2) -> bool:
    """Check if a GHL timestamp is within tolerance of any existing timestamp."""
    if not ghl_date:
        return False
    try:
        ghl_dt = parse_datetime(ghl_date)
        for ts_str in existing:
            try:
                existing_dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                if abs((ghl_dt - existing_dt).total_seconds()) < tolerance_seconds:
                    return True
            except (ValueError, TypeError):
                continue
    except (ValueError, TypeError):
        pass
    return False


def _timestamps_close(ts1: str, ts2: str, seconds: int = 60) -> bool:
    """Check if two timestamp strings are within N seconds of each other."""
    try:
        dt1 = datetime.fromisoformat(str(ts1).replace("Z", "+00:00"))
        dt2 = datetime.fromisoformat(str(ts2).replace("Z", "+00:00"))
        return abs((dt1 - dt2).total_seconds()) < seconds
    except (ValueError, TypeError):
        return False


def _format_transcription(data: Any) -> str:
    """Format GHL call transcription JSON into flat transcript text.

    GHL returns per-sentence data with speaker channels and timestamps.
    We format it as: "Speaker: text" lines.

    Handles both formats: {"transcription": [...]} (dict wrapper) and
    direct list [...] (raw array from GHL API).
    """
    if not data:
        return ""

    # GHL may return {"transcription": [...]}} or just [...]
    if isinstance(data, list):
        sentences = data
    else:
        sentences = data.get("transcription", [])
    if not sentences:
        return ""

    lines = []
    for s in sentences:
        # GHL uses two field naming conventions:
        #   Old: {"channel": 0/1, "text": "..."}  (0=Staff, 1=Lead)
        #   Current: {"mediaChannel": 1/2, "transcript": "..."}  (2=Staff, 1=Lead)
        if "mediaChannel" in s:
            speaker = "Staff" if s.get("mediaChannel") == 2 else "Lead"
        else:
            speaker = "Staff" if s.get("channel", 0) == 0 else "Lead"
        text = (s.get("transcript") or s.get("text") or "").strip()
        if text:
            lines.append(f"{speaker}: {text}")

    return "\n".join(lines)
