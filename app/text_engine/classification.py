"""Step 3.7: Classification gate — stop bot, transfer/opt-out, response determination.

Three sequential gates before agent routing:
1. Stop Bot tag check (no LLM — pure tag lookup)
2. Transfer To Human / Opt Out classifier (Gemini Flash)
3. Response Determination classifier (Gemini Flash)

Only if all three pass does the pipeline proceed to agent routing.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Any

from prefect import task

from app.models import PipelineContext
from app.services.ai_client import classify
from app.text_engine.model_resolver import resolve_model, resolve_temperature

# Precompiled patterns for the keyword-fallback gate so we don't recompile
# them on every inbound.
_PUNCT_RE = re.compile(r"[^\w\s]")
_WS_RE = re.compile(r"\s+")

# Window for the recent-call gate. Any call_log row created within this window
# (post-call) means the lead just spoke to a human; SMS auto-replies during
# this window race with the live conversation. setter.call_logs only stores
# completed calls, so the upper-bound is also a proxy for "call may still be
# active" since rows aren't written until the call ends.
_RECENT_CALL_GATE_SECONDS = 180

logger = logging.getLogger(__name__)

# ============================================================================
# SCHEMAS
# ============================================================================

_TTH_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "path": {
            "type": "string",
            "enum": ["human", "opt_out", "continue"],
            "description": "Classification: human (escalate), opt_out (DNC), continue (proceed)",
        },
        "reason": {
            "type": "string",
            "description": "Brief explanation for the classification decision",
        },
    },
    "required": ["path", "reason"],
    "additionalProperties": False,
}

_RD_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "path": {
            "type": "string",
            "enum": ["respond", "dont_respond"],
            "description": "Whether the AI should respond to this message",
        },
        "reason": {
            "type": "string",
            "description": "Brief explanation for the decision",
        },
    },
    "required": ["path", "reason"],
    "additionalProperties": False,
}


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================


@task(name="classification_gate", retries=3, retry_delay_seconds=5, timeout_seconds=45)
async def classification_gate(ctx: PipelineContext) -> None:
    """Run the three classification gates in sequence.

    Sets ctx.response_path and ctx.response_reason.
    If any gate triggers early exit, the pipeline should check response_path
    before proceeding to agent routing.

    Possible paths:
    - "stop_bot": Contact has stop_bot tag, skip all AI
    - "transfer": Transfer to human staff
    - "opt_out": Lead opted out / is a solicitor
    - "dont_respond": No response needed (conversation concluded)
    - "respond": Proceed to agent routing
    """
    logger.info(
        "CLASSIFICATION | contact=%s | tags=%d | has_booking=%s | timeline_len=%d",
        ctx.contact_id, len(ctx.contact_tags), ctx.has_upcoming_booking,
        len(ctx.timeline) if ctx.timeline else 0,
    )

    # Gate 1: Stop Bot tag check (free — no LLM call)
    if _check_stop_bot(ctx):
        ctx.classification_gates.append({"gate": "Stop Bot", "result": "BLOCKED", "reason": "Contact has stop bot tag"})
        return

    ctx.classification_gates.append({"gate": "Stop Bot", "result": "passed", "reason": ""})

    # Gate 1b: Reply-intent tag check (free — no LLM call).
    # If the upstream Python classifier already bucketed this reply as DNC /
    # not-interested / automated and wrote the corresponding tag to GHL, we
    # should NOT rebut. 2026-04-24 fix: previously iron-setter happily sent
    # "Got it, totally get it. Most people who end up with us said the same
    # at first..." to a lead that replied "No thank you" — because the
    # downstream LLM gates defaulted to `respond` when the signal wasn't
    # loud enough for the TTH gate's opt-out definition.
    if _check_reply_intent_tags(ctx):
        ctx.classification_gates.append({
            "gate": "Reply Intent Tag",
            "result": "dont_respond",
            "reason": ctx.response_reason[:200],
        })
        return
    ctx.classification_gates.append({"gate": "Reply Intent Tag", "result": "passed", "reason": ""})

    # Gate 1b2: Empty / tapback inbound (free — no LLM call). Lead messages
    # with empty bodies, whitespace-only bodies, or iOS/Android tapback
    # reactions ("Liked '...'", "👍 to '...'") are not real conversational
    # turns and should never trigger an AI reply. Catches a class of bug
    # where MMS/sticker delivery notifications produced full AAA replies.
    if _check_empty_or_tapback(ctx):
        ctx.classification_gates.append({
            "gate": "Empty / Tapback",
            "result": "dont_respond",
            "reason": ctx.response_reason[:200],
        })
        return
    ctx.classification_gates.append({"gate": "Empty / Tapback", "result": "passed", "reason": ""})

    # Gate 1c: Keyword fallback (free — no LLM call).
    # If the inbound body matches a DNC / not-interested keyword (STOP,
    # "no thank you", "not interested", curse words), short-circuit to
    # dont_respond instead of falling through to the LLM gates. Protects us
    # when the LLM is misconfigured or down — we fail CLOSED, not "respond".
    if _check_keyword_fallback(ctx):
        ctx.classification_gates.append({
            "gate": "Keyword Fallback",
            "result": ctx.response_path or "dont_respond",
            "reason": ctx.response_reason[:200],
        })
        return
    ctx.classification_gates.append({"gate": "Keyword Fallback", "result": "passed", "reason": ""})

    # Gate 1d: Recent-call gate (free — uses ctx.call_logs already loaded by
    # build_timeline). If a call ended in the last few minutes, the lead is
    # still in "just got off the phone" state and an automated SMS during this
    # window collides with the live conversation. Fail closed → dont_respond.
    if _check_recent_call(ctx):
        ctx.classification_gates.append({
            "gate": "Recent Call",
            "result": "dont_respond",
            "reason": ctx.response_reason[:200],
        })
        return
    ctx.classification_gates.append({"gate": "Recent Call", "result": "passed", "reason": ""})

    # Gate 2: Transfer To Human / Opt Out (LLM classifier)
    tth_result = await _classify_transfer(ctx)
    ctx.classification_gates.append({
        "gate": "Transfer/Opt-Out",
        "result": tth_result["path"],
        "reason": tth_result["reason"][:200],
    })
    if tth_result["path"] != "continue":
        ctx.response_path = tth_result["path"]  # "human" or "opt_out"
        ctx.response_reason = tth_result["reason"]
        logger.info(
            "TTH classifier: path=%s reason=%s",
            tth_result["path"],
            tth_result["reason"][:100],
        )
        return

    # Gate 3: Response Determination (LLM classifier)
    rd_result = await _classify_response(ctx)
    ctx.classification_gates.append({
        "gate": "Response Determination",
        "result": rd_result["path"],
        "reason": rd_result["reason"][:200],
    })
    ctx.response_path = rd_result["path"]  # "respond" or "dont_respond"
    ctx.response_reason = rd_result["reason"]
    logger.info(
        "RD classifier: path=%s reason=%s",
        rd_result["path"],
        rd_result["reason"][:100],
    )


# ============================================================================
# GATE 1: STOP BOT TAG CHECK
# ============================================================================


def _check_stop_bot(ctx: PipelineContext) -> bool:
    """Check if contact has 'stop bot' tag. Returns True if should stop.

    Matches n8n behavior: caseSensitive=true, contains operation.
    Checks raw tags joined with ', ' for substring 'stop bot'.
    """
    tags_joined = ", ".join(ctx.contact_tags)
    if "stop bot" in tags_joined:
        ctx.response_path = "stop_bot"
        ctx.response_reason = "Contact has stop bot tag"
        logger.info("Stop bot tag detected — skipping all AI processing")
        return True
    return False


# Tags written by the upstream Python classifier (iron-sms). When any of
# these are present on the contact we should NOT rebut — the reply was
# already classified and the drip gate logic is owned elsewhere.
_REPLY_INTENT_SKIP_TAGS: frozenset[str] = frozenset({
    "replied-do-not-contact",
    "replied-not-interested",
    "replied-automated",
    "opt-out", "opt_out",
    "do-not-contact", "do_not_contact",
    "tcpa-risk", "tcpa_risk", "tcpa-threat",
})


def _check_reply_intent_tags(ctx: PipelineContext) -> bool:
    """Skip rebut when the upstream classifier already bucketed the reply.

    Returns True if response_path was set to ``dont_respond``.
    """
    tags_lower = {(t or "").strip().lower() for t in (ctx.contact_tags or [])}
    hit = _REPLY_INTENT_SKIP_TAGS & tags_lower
    if hit:
        ctx.response_path = "dont_respond"
        ctx.response_reason = (
            f"Upstream classifier already set DNC/not-interested intent "
            f"(tags: {sorted(hit)})"
        )
        logger.info(
            "Reply-intent tag hit — skipping rebut (tags=%s)", sorted(hit)
        )
        return True
    return False


# Cheap keyword fallbacks for when the LLM is unavailable or slow. Matches
# the iron-sms classifier's `_DNC_EXACT` + `_DNC_CONTAINS` intent set plus
# common "not interested" phrases (the Gemini gate can miss these when the
# body is tiny and context is sparse).
# Audit-cycle-1 P1-2 (2026-04-24): added "yes stop" / "ok stop" / "please
# stop" / "just stop" compound phrases. These pass an agreement token
# before the stop keyword (e.g. reply to "do you want to continue?") and
# were reaching the LLM where they could be miscategorized as ``continue``.
_KEYWORD_DNC_EXACT: frozenset[str] = frozenset({
    "stop", "end", "cancel", "quit", "unsubscribe",
    "yes stop", "ok stop", "okay stop", "just stop", "please stop",
    "yes remove", "yes unsubscribe", "ok unsubscribe",
})
_KEYWORD_DNC_CONTAINS: tuple[str, ...] = (
    "stop texting", "stop messaging", "stop contacting",
    "stop the texts", "stop the messages", "stop the drip",
    "stop sending", "stop calling",
    # Audit-cycle-3 P2 fix (2026-04-24): the punctuation-stripping normalizer
    # turns "don't" → "don t" (apostrophe → space), so we include both the
    # apostrophe form and the post-normalize space form of each DNC token.
    "do not contact", "don't contact", "dont contact", "don t contact",
    "don't text", "dont text", "don t text",
    "leave me alone",
    "remove me", "remove my number", "take me off",
    # Audit-cycle-4 P1-A fix (2026-04-24): removed `"can't text"` / `"cant text"`
    # / `"can t text"` — substring-contains against these phrases false-positives
    # on temporary-unavailability replies like "can t text back right now, call
    # me instead" which is a call preference, NOT a DNC request. Let the TTH
    # LLM gate decide on ambiguous "can't text" variants.
)
_KEYWORD_NOT_INTERESTED_EXACT: frozenset[str] = frozenset({
    "no", "nope", "nah", "no thanks", "no thank you", "not interested",
    "not at this time", "not now", "all set", "we're good", "we are good",
    # 2026-05-02: short-form no replies that pre-patch missed and re-pitched.
    # See setter.entities.system_config.accept_no for the full taxonomy.
    "no i don t", "no i dont", "no i do not",
    "no i m good", "no i am good", "no im good",
    "no i m not", "no i am not", "no im not",
    "no i won t", "no i wont", "no i will not",
    "no can do", "hard pass", "no thanks man", "nah i m good",
    "no money", "no budget",
    "no longer in business", "no longer doing this", "shut down",
})
_KEYWORD_NOT_INTERESTED_CONTAINS: tuple[str, ...] = (
    "not interested", "no thank you", "all set", "not a fit",
    "already have", "already use", "pass", "i'll pass", "i pass",
    # 2026-05-02: cover negative-polarity short-form replies that bypass
    # exact-match because they have extra trailing words/context.
    "no money up front", "no money for that", "i don t pay for leads",
    "i dont pay for leads", "don t pay for leads", "dont pay for leads",
    "no longer in", "no longer doing", "no longer running",
    # 2026-05-03: production incident — DeCoursey Company sent "Nope not at
    # all", LLM RD classifier routed to respond, agent leaked classification
    # jargon as the SMS body. Catching multi-word "Nope X" / "Nah X" /
    # explicit refusals at the deterministic gate eliminates the LLM
    # round-trip risk for unambiguous nos.
    "nope not", "nope nope", "nope i", "nope we", "nope dont",
    "nah not", "nah we", "nah i",
    "not at all", "absolutely not", "definitely not", "hell no", "hard no",
    "no chance", "not a chance", "not interested at all", "not even close",
    "no way", "no f", "no f ing",
)


def _last_inbound_body(ctx: PipelineContext) -> str:
    """Best-effort pull of the most recent inbound message body.

    Primary source is ``ctx.message`` (the consolidated inbound text set by
    ``pipeline.py:_consolidate_inbound`` — it joins all unanswered inbound
    bodies since the last AI/staff turn). Falls back to walking
    ``ctx.chat_history`` for the most recent ``role == 'human'`` row, in
    case the gate runs before consolidation populated ``ctx.message``.
    """
    msg = getattr(ctx, "message", "") or ""
    if isinstance(msg, str) and msg.strip():
        return msg.strip()
    history = getattr(ctx, "chat_history", None) or []
    for row in reversed(history):
        if not isinstance(row, dict):
            continue
        if str(row.get("role") or "").lower() != "human":
            continue
        content = row.get("content")
        if isinstance(content, str) and content.strip():
            return content.strip()
    return ""


_TAPBACK_RE = re.compile(
    r'^\s*(liked|loved|laughed at|emphasized|disliked|questioned|👍|❤️|😂|‼️|❓|🥰)\s+["“]',
    re.IGNORECASE,
)


def _check_empty_or_tapback(ctx: PipelineContext) -> bool:
    """Free gate: dont_respond for empty bodies and iOS/Android tapback reactions.

    Returns True if response_path was set.
    """
    body = _last_inbound_body(ctx)
    if not body or not body.strip():
        ctx.response_path = "dont_respond"
        ctx.response_reason = "Empty inbound body — nothing to respond to"
        logger.info("Empty inbound body — skipping AI reply")
        return True
    if _TAPBACK_RE.match(body):
        ctx.response_path = "dont_respond"
        ctx.response_reason = f"Tapback reaction (not a new message): {body[:80]!r}"
        logger.info("Tapback reaction detected — skipping AI reply")
        return True
    return False


def _check_recent_call(ctx: PipelineContext) -> bool:
    """Free gate: dont_respond if a call ended within the recent-call window.

    Uses ``ctx.call_logs`` populated upstream by ``build_timeline`` (limit=5,
    most-recent-first). A row in ``setter.call_logs`` is only written when a
    call completes, so a recent row is a strong signal that either (a) the
    lead just hung up with a human, or (b) a live call is ending right now —
    in both cases an automated SMS reply collides with the live conversation
    and should be suppressed.

    Window: ``_RECENT_CALL_GATE_SECONDS`` (default 180s).

    Returns True if response_path was set to ``dont_respond``.
    """
    call_logs = getattr(ctx, "call_logs", None) or []
    if not call_logs:
        return False
    latest = call_logs[0] if isinstance(call_logs[0], dict) else None
    if not latest:
        return False
    raw_ts = latest.get("created_at")
    if not raw_ts:
        return False
    try:
        if isinstance(raw_ts, str):
            ts = datetime.fromisoformat(raw_ts.replace("Z", "+00:00"))
        elif isinstance(raw_ts, datetime):
            ts = raw_ts
        else:
            return False
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return False
    age = datetime.now(timezone.utc) - ts
    if age <= timedelta(seconds=_RECENT_CALL_GATE_SECONDS):
        ctx.response_path = "dont_respond"
        ctx.response_reason = (
            f"Recent call activity ({int(age.total_seconds())}s ago, "
            f"status={latest.get('status', 'unknown')!r}) — "
            f"suppressing AI reply to avoid colliding with live conversation"
        )
        logger.info(
            "Recent-call gate fired — call ended %.0fs ago, status=%s",
            age.total_seconds(),
            latest.get("status", "unknown"),
        )
        return True
    return False


def _check_keyword_fallback(ctx: PipelineContext) -> bool:
    """Free keyword gate. Fail-closed for DNC / not-interested bodies.

    Returns True if response_path was set.
    """
    body = _last_inbound_body(ctx)
    if not body:
        return False
    # Audit-cycle-2 P2-C fix (2026-04-24): strip ALL punctuation before the
    # exact-match check so "Yes, stop." / "Ok, stop!" normalize to
    # "yes stop" / "ok stop" and hit `_KEYWORD_DNC_EXACT`. Previously only
    # trailing `.!? ` was stripped, leaving commas embedded — the TCPA
    # compound-phrase gate was being bypassed by the most common real-world
    # punctuation.
    normalized = body.lower().strip()
    normalized = _PUNCT_RE.sub(" ", normalized)   # punctuation → space
    normalized = _WS_RE.sub(" ", normalized).strip()
    if normalized in _KEYWORD_DNC_EXACT or any(
        token in normalized for token in _KEYWORD_DNC_CONTAINS
    ):
        ctx.response_path = "opt_out"
        ctx.response_reason = f"Keyword DNC match on inbound body: {body[:80]!r}"
        logger.info("Keyword DNC fallback — routing to opt_out")
        return True
    if normalized in _KEYWORD_NOT_INTERESTED_EXACT or any(
        token in normalized for token in _KEYWORD_NOT_INTERESTED_CONTAINS
    ):
        ctx.response_path = "dont_respond"
        ctx.response_reason = (
            f"Keyword not-interested match on inbound body: {body[:80]!r}"
        )
        logger.info("Keyword not-interested fallback — skipping rebut")
        return True
    return False


# ============================================================================
# GATE 2: TRANSFER TO HUMAN / OPT OUT
# ============================================================================

# Full TTH prompt — matches n8n "Transfer To Human / Opt out" node (line 3988)
# Client-specific transfer rules are injected at the top via {transfer_to_human_prompt}
_TTH_SYSTEM_PROMPT = """\
<transfer_criteria>
## Transfer to Human Criteria and rules
{transfer_to_human_prompt}
</transfer_criteria>

<call_history_context>
## Call History Context

Use call history to inform classification:
- If lead was already transferred during a call AND is now asking for human again → likely escalation, classify as "Human"
- If Staff already resolved their issue on a call → they may not need transfer, classify as "Continue" unless clearly requesting human
- If lead had negative call experience (transferred multiple times, long holds) and sounds frustrated → lean toward "Human"
- Call history alone does NOT trigger transfer - only use it as supporting context
</call_history_context>

<opt_out_criteria>
## Opt Out / Solicitor Criteria and Rules

**Analyze the COMPLETE conversation history. Context is critical.**

### Classify as "Opt Out" if user requests to stop ALL communication:

**Explicit opt-out phrases:**
- "stop", "stop texting me", "stop messaging", "stop contacting me"
- "unsubscribe", "remove me", "take me off your list"
- "don't contact me again", "don't text me anymore", "do not contact"
- "leave me alone", "stop bothering me"
- "I'm not interested and don't want to be contacted"
- "remove my number", "delete my information"

**Contextual opt-out indicators (ALL must include an explicit request to stop contact):**
- Explicitly requests removal from contact list
- States they want no future messages
- Frustration + explicit request to cease contact entirely
- The lead must be asking to STOP BEING CONTACTED — not just saying they don't need the service

### Classify as "Opt Out" if user is a SOLICITOR/SALESPERSON:

**Solicitor indicators - someone trying to SELL to us, not buy from us:**
- Offering their services unprompted ("I can help with your marketing", "We offer SEO services")
- Cold pitch language ("I noticed your business", "I came across your company")
- Representing another company to sell ("I'm with [company]", "I represent [company]")
- Asking to speak to owner/decision maker for sales purposes
- Dropping links with sales intent
- B2B sales pitches ("Are you the person who handles...", "Who manages your...")
- Recruitment/job seeking ("Are you hiring?", "I'm looking for work", "any openings?", "position available?", "looking to join")
- Marketing/agency pitches ("We can get you more leads", "We specialize in...")
- Software/tool sales ("Have you tried [product]?", "Our platform can...")

**Key distinction:**
- CUSTOMER = asking about OUR services, wanting to BUY from us → Continue
- SOLICITOR = offering THEIR services, wanting to SELL to us → Opt Out

### EMAIL SIGNATURE EXCEPTION (CRITICAL)

When Reply Channel is "Email", messages typically contain email signatures with:
- Name, phone number, social media links (LinkedIn, etc.)
- Professional taglines, business promotions, website links
- "Check out my...", "Follow me on...", promotional text
- Company name, job title, disclaimer text

These are standard email signatures, NOT solicitation. Do NOT classify as Opt Out based on email signature content. Only evaluate the BODY of the email (the message text before the signature block) for solicitor indicators.

A lead asking about our services who has a promotional email signature is a CUSTOMER, not a solicitor.

### DO NOT classify as "Opt Out":

**Topic-specific dismissal:**
- "stop talking about that" → Topic change, not opt-out
- "stop asking about my budget" → Topic boundary, not opt-out

**Temporary disinterest:**
- "not interested right now", "maybe later", "not at this time"
- "call me back in a few months"
- "I need to think about it"

**Soft responses:**
- "I'm busy right now" → Timing issue
- "I already have someone" → Not requesting removal
- "too expensive" → Price objection
- "I'll reach out if I need you" → On hold, not opt-out

**Lead disqualifies themselves (NOT an opt-out — these need a polite reply):**
- "I rent" / "I'm a renter" / "my landlord handles it" → Not a fit, but not requesting removal
- "I'm in [wrong city/state]" / "that's too far" → Outside service area, not opting out
- "nvm" / "never mind" / "actually forget it" → Changed their mind, not requesting DNC
- "I don't need that anymore" / "we went with someone else" → Lost interest, not opt-out
- "not what I was looking for" → Mismatch, not removal request
A lead who discovers they can't use the service is NOT opting out of communication.
They deserve a polite closing reply, not silence. Only classify as opt-out if they
explicitly ask to stop receiving messages or be removed from the contact list.

**Contextual "stop":**
- "stop the service" (discussing canceling something)
- "stop at the corner" (giving directions)
- "can't stop thinking about it" (expression)

**Mid-conversation frequency complaints (engaged leads):**
If the lead has been ACTIVELY ENGAGED in the conversation (asked questions, discussed services, attempted to book, shared preferences) and their "stop" message is about follow-up FREQUENCY rather than wanting ZERO contact, this is NOT an opt-out. Classify as **Continue**.

- "Stop texting me so much" → Frequency complaint from engaged lead, NOT opt-out
- "Can you stop texting me three times a day?" → Wants less contact, not zero contact
- "Stop messaging me every few hours" → Frustrated about pacing, still interested
- "I said I'd research it, stop messaging" → Gave clear intent + frequency complaint
- "Please stop with the constant texts" → Wants reduced frequency, not removal
- "You're texting too much" → Feedback about pacing

**Key test:** Has this lead been ENGAGED earlier in the conversation (asked questions, tried to book, discussed services/pricing)?
- YES + complaint about texting frequency → **Continue** (not opt-out — they're frustrated about pacing, not requesting removal)
- NO engagement + "stop texting" from initial outreach with no prior engagement → **Opt Out** (true opt-out from cold contact)

**Why:** A lead who tried to book, asked questions, or discussed services is NOT opting out — they're telling us to back off on frequency. Marking them DNC loses a warm lead permanently.

**Legitimate customer inquiries:**
- Asking about pricing, services, availability
- Complaining about service (they're a customer)
- Asking questions, even tough ones
</opt_out_criteria>

<continue_criteria>
## Continue Category

**Use "Continue" when:**
- Normal conversation, no transfer or opt-out needed
- Questions about services, pricing, availability
- Objections that can be addressed
- Interested but needs more information
- Any conversation that should keep going
- Legitimate customer or lead behavior
</continue_criteria>

<decision_tree>
## Classification Path Rules

**Follow this decision tree:**

1. **First check: Solicitor?**
   - Trying to sell THEIR services to us? → **Opt Out**
   - B2B sales pitch or job seeker? → **Opt Out**
   - Otherwise, go to step 2

2. **Second check: Opt Out?**
   Ask yourself: "Did the lead explicitly ask to STOP BEING CONTACTED or be REMOVED?"
   - YES, they said stop/unsubscribe/remove/don't contact → **Opt Out**
   - NO, they just said they don't need the service, changed their mind, aren't a fit,
     said "nvm", or revealed they can't use us → **Continue** (not an opt-out, the AI
     should send a polite closing reply)
   - Otherwise, go to step 3

3. **Third check: Transfer to Human?**
   - Matches transfer criteria above? → **Human**
   - Otherwise, go to step 4

4. **Default: Continue**
   - If none of the above, classify as → **Continue**

**When uncertain between categories:**
- Not sure if solicitor? Look for: Are they offering something or asking for something?
  - Offering their service = Solicitor = **Opt Out**
  - Asking about our service = Customer = **Continue**
- Uncertain if opt-out? → Classify as **Continue** (false opt-out is worse)
- Uncertain if needs human? → Classify as **Human** (safer to transfer)
- Truly ambiguous? → Classify as **Continue** and let conversation proceed
</decision_tree>

<output_format>
## OUTPUT FORMAT

You MUST respond with valid JSON only. No other text.

Valid path values: "human", "opt_out", "continue"

Example:
{{"path": "human", "reason": "Lead explicitly asked to speak with a real person"}}
{{"path": "opt_out", "reason": "Lead said stop texting me, clear opt-out request"}}
{{"path": "continue", "reason": "Normal conversation, no transfer or opt-out needed"}}
</output_format>
"""


async def _classify_transfer(ctx: PipelineContext) -> dict[str, Any]:
    """Run Transfer To Human / Opt Out classifier."""
    # Build system prompt with client-specific rules
    # Compiled path: transfer prompt includes agent capabilities + philosophy + scenarios + DNT
    transfer_rules = ctx.compiled.get("transfer_prompt", "")
    # Opt-out sections replace the hardcoded opt_out_criteria
    opt_out = ctx.compiled.get("opt_out_sections", "")
    if opt_out:
        transfer_rules += f"\n\n{opt_out}"
    system = _TTH_SYSTEM_PROMPT.format(transfer_to_human_prompt=transfer_rules)

    # Build user prompt matching n8n: timeline + Reply Channel + appointments
    user_prompt = _build_tth_context(ctx)

    from app.models import log_prompt
    _tc_vars: dict[str, Any] = {
        "transfer_to_human_prompt": transfer_rules,
    }
    if ctx.timeline:
        _tc_vars["timeline"] = ctx.timeline
    if ctx.upcoming_booking_text:
        _tc_vars["upcoming_booking_text"] = ctx.upcoming_booking_text
    if ctx.past_booking_text:
        _tc_vars["past_booking_text"] = ctx.past_booking_text
    _tc_vars["channel"] = ctx.channel or ""
    log_prompt(ctx, "Transfer Classifier", system, user_prompt, variables=_tc_vars)

    return await classify(
        prompt=user_prompt,
        schema=_TTH_SCHEMA,
        model=resolve_model(ctx, "transfer_detection"),
        temperature=resolve_temperature(ctx, "transfer_detection"),
        system_prompt=system,
        label="transfer_detection",
    )


# ============================================================================
# GATE 3: RESPONSE DETERMINATION
# ============================================================================

# Full RD prompt — matches n8n "Response Determination" node (line 4117)
_RD_SYSTEM_PROMPT = """\
<role>
You are analyzing whether the AI should send a response to the lead's latest message.
</role>

<pending_actions_check>
## CRITICAL FIRST CHECK: PENDING ACTIONS

Before anything else, determine if the AI's LAST message:
- Asked a question (with or without punctuation - includes "which", "would you", "do you want", "does that work", "what time", "let me know", etc.)
- Offered options for the lead to choose from (times, services, dates)
- Is waiting for the lead to select, confirm, or provide information
- Said it would do something that requires lead input ("I can book that for you", "I'll send that over", etc.)
- Requires any action to be taken (booking, sending info, following up)

**If ANY of the above → ALWAYS "Respond"**

Even if the lead's message sounds like a closing ("perfect thanks", "ok see you then", "sounds good ty"), if the AI is waiting for input or needs to take action, it MUST respond.

The lead answering a question or selecting an option is NOT a closing - it's the lead providing input that requires ACTION from the AI.

**Examples of PENDING ACTIONS requiring response:**
- AI: "Did you want 10:00 AM or 10:30 AM" → Lead: "10am perfect ty" → RESPOND (must book)
- AI: "Which service interests you" → Lead: "the facial thanks" → RESPOND (must continue)
- AI: "Does Tuesday work" → Lead: "yes perfect" → RESPOND (must book)
- AI: "Let me know which works best for you" → Lead: "the first one" → RESPOND (must proceed)
- AI: "I can get you scheduled for Saturday" → Lead: "yes please thanks" → RESPOND (must book)
- AI: "What's your email" → Lead: "john@email.com thanks" → RESPOND (must proceed)
</pending_actions_check>

<auto_responder_detection>
## AUTO-RESPONDER DETECTION (Check Early)

Before analyzing conversation context, check if the lead's message is an automated/system response rather than a human reply. Auto-responders should NOT receive AI responses.

**Auto-Responder Indicators:**

1. **Multi-part SMS fragments** - Messages starting with or containing:
   - "(2/2)", "(2/3)", "(3/3)", etc. (continuation of a blast)
   - "(1/2)" followed immediately by booking link or hours info

2. **Generic away/hours messages:**
   - "Thanks for your message. We're off for the day..."
   - "We're currently closed..."
   - "Our hours are..."
   - "We'll get back to you soon"
   - "This is an automated response"
   - "Thank you for contacting [business name]"

3. **Unprompted booking link blasts:**
   - Message contains booking URL (calendly, vagaro, acuity, as.me, etc.)
   - AI did NOT ask them to book or send a link
   - Appears to be an auto-reply to any incoming text

4. **Business info dumps:**
   - Hours of operation
   - Address/location info
   - "For appointments call..."
   - Service menus or pricing lists

**If message matches auto-responder pattern → "Dont Respond"**

The lead's business has an auto-reply system. Responding would create an awkward AI-to-autoresponder loop. Wait for actual human engagement.

**Examples - Dont Respond (Auto-Responder):**
- Lead: "(2/2)more feel free to book one below https://serenitysdream..." → Dont Respond
- Lead: "Thanks for your message! We're closed but will respond soon." → Dont Respond
- Lead: "Our hours are Mon-Fri 9-5. Book here: [link]" → Dont Respond
- Lead: "(1/2) Thank you for reaching out to ABC Med Spa!" → Dont Respond

**Examples - RESPOND (Human, not auto-responder):**
- Lead: "here's my booking link if you want to see availability [link]" → RESPOND (contextual human reply)
- Lead: "we're closed today but what's this about?" → RESPOND (question asked)
- Lead: "thanks for reaching out, what exactly is this?" → RESPOND (human engagement)

**Key distinction:** Auto-responders are generic and triggered by ANY incoming message. Human replies reference the conversation context or ask questions.
</auto_responder_detection>

<booking_safety_check>
## BOOKING SAFETY CHECK (Using Appointments Data)

If upcoming appointments data is provided, use it to verify booking status.

**IMPORTANT: This check only applies when:**
- AI offered APPOINTMENT TIMES for the lead to select (not just mentioned hours/availability info)
- AI books directly (not when sending Vagaro/external booking link)
- Conversation is about booking a NEW appointment (or rescheduling to a new time)

**If AI offered appointment times AND lead selected one AND that specific time is NOT in upcoming appointments:**
→ "Respond" - Booking hasn't happened yet

**What to check in appointments data:**
- Look for UPCOMING appointments (future dates, not past)
- Look for CONFIRMED status (not cancelled)
- Check if the SPECIFIC time discussed exists (not just any appointment)

**Do NOT apply this check when:**
- AI sent an external booking link (Vagaro, Calendly, etc.) - lead books themselves
- Conversation was about general info (hours, pricing) not booking appointment times
- Lead already has OTHER appointments but is booking something NEW

**Examples:**

AI offered times, lead selected, NO matching appointment:
- AI: "I have Saturday at 10am or Monday at 2pm"
- Lead: "Saturday 10am perfect thanks"
- Appointments: empty OR only shows different appointments like Tuesday 3pm consultation
→ RESPOND (the Saturday 10am booking hasn't happened!)

AI confirmed booking, matching appointment EXISTS:
- AI: "You're all booked for Saturday at 10:00 AM!"
- Lead: "thank you!"
- Appointments: shows Saturday 10:00 AM confirmed
→ Dont Respond (booking confirmed)

AI sent external link (doesn't book directly):
- AI: "Book here: vagaro.com/..."
- Lead: "ok thanks I'll book now"
- Appointments: empty (expected - they book themselves)
→ Dont Respond (true closing, AI doesn't book these)

Lead has existing appointment, booking NEW one:
- Appointments: shows Tuesday 3pm consultation only
- AI: "For your follow-up visit, I have Thursday at 11am"
- Lead: "Thursday works thanks"
→ RESPOND (Thursday follow-up not booked yet)

Lead didn't select specific time:
- AI: "I have 10am or 2pm Saturday"
- Lead: "Saturday works see you then"
→ RESPOND (didn't specify WHICH time - need to clarify)
</booking_safety_check>

<incomplete_info_check>
## INCOMPLETE INFORMATION CHECK

If lead provides partial information that needs clarification:
- "I want the 10am" (but which DAY?) → Respond (need to clarify)
- "Saturday works" (but which TIME?) → Respond (need to clarify)
- "Yes book it" (but haven't confirmed details) → Respond (need to confirm specifics)
- "The second option" (but AI offered options days ago) → Respond (need to confirm which)

Incomplete information requires follow-up, not closing.
</incomplete_info_check>

<call_history_context>
## Call History Context
- If lead references something from a call (booking, info, conversation), use call logs to understand context
- If call outcome was "booked" and lead texts acknowledgment → likely Dont Respond
- If call was unresolved/transferred and lead follows up → likely Respond
</call_history_context>

<delayed_action_requests>
## DELAYED ACTION REQUESTS

If lead asks to be contacted later or delays action:
- "Can you call me tomorrow?" → Respond (acknowledge, confirm timing)
- "I'll get back to you next week" → Respond (acknowledge, keep door open)
- "Text me in an hour" → Respond (confirm you will)
- "Let me check my schedule and get back to you" → Respond (acknowledge)
- "I need to ask my husband first" → Respond (acknowledge, offer to help when ready)

These sound like closings but require acknowledgment of the request.
</delayed_action_requests>

<path_consequences>
## WHAT HAPPENS ON EACH PATH

Understanding the consequences helps make the right choice:

- **"Respond" path**: AI generates a response, can book appointments, extract information, move conversation forward
- **"Dont Respond" path**: Conversation ends silently. No booking happens. No information captured. No follow-up.

This context is NOT to make you over-choose "Respond" - simple closing acknowledgments after completed conversations should still be "Dont Respond". But if there's genuinely an action the AI needs to take, choosing "Dont Respond" means that action never happens.
</path_consequences>

<classification_criteria>
## PRIMARY PURPOSE: Prevent Endless Response Loops

This node stops unnecessary back-and-forth when the conversation is naturally concluding AND all actions are complete. The AI should stop responding when:
1. The conversation has reached a natural ending point
2. All actions have been completed (booking made, info sent, etc.)
3. No questions or offers are pending from the AI

## CRITICAL RULE: When in doubt, ALWAYS choose "Respond"

Only use "Dont Respond" when ALL of these are true:
1. AI's last message was NOT a question (with or without punctuation)
2. AI's last message was NOT offering options
3. AI is NOT waiting for any input or selection
4. No actions are left to take (goal already achieved)
5. Conversation is clearly wrapping up with acknowledgments

## Understanding SMS Reactions

Leads can react to messages with reactions that appear as:
- "Liked [message]", "Loved [message]", "Laughed at [message]"
- "Emphasized [message]", "Questioned [message]", "Disliked [message]"
- Emoji reactions: 👍, 😊, ✓, etc.

**Reaction ALONE (no text added):**
- If AI asked a question → "Respond" (reaction isn't an answer)
- If AI gave closing statement → "Dont Respond" (reaction = acknowledgment)
- If booking incomplete → "Respond" (need to continue)

**Reaction + Text:**
- Analyze the TEXT, not just the reaction
- "Liked 'message' thanks so much" → evaluate "thanks so much"
- "Loved 'message' can I reschedule?" → evaluate "can I reschedule?" → Respond
- "Liked 'message' see you then" → evaluate "see you then" in context

**Examples:**
- AI: "You're booked for Tuesday!" → Lead: "Liked 'You're booked for Tuesday!'" → Dont Respond
- AI: "Does 10am work?" → Lead: "Liked 'Does 10am work?'" → Respond (reaction isn't an answer)
- AI: "See you Saturday!" → Lead: "Loved 'See you Saturday!' can't wait" → Dont Respond

## Classification Categories:

### "Respond" - Send AI response (DEFAULT)
Use this for:
- AI's last message asked a question or offered options (ALWAYS respond to answers)
- AI needs to take an action (book, send link, provide info)
- AI said it would do something and needs confirmation
- Booking discussed but appointment not yet in data
- ANY actual question from the lead (contains a question mark, asks "do you", "how does", "what is", etc.) — ALWAYS respond, even if the AI's previous message already covered the topic. The lead may want clarification, may not have noticed, or may be asking a related follow-up
- New topics or requests for information
- Lead providing information that moves conversation forward
- Anything that progresses toward booking/conversion
- Middle of conversation (not closing yet)
- Unclear reactions or ambiguous responses
- Lead answered AI's question (even with "thanks" or "perfect" attached)
- Lead provided incomplete information needing clarification
- Lead requested delayed action or callback
- OFFER OPPORTUNITY: If an <offers> section is present and the lead has a confirmed booking and shows positive sentiment, classify as "Respond" so the agent can engage and use relevant offers if appropriate. Simple closers ("ok", "thanks", "see you then") with no conversational opening are still "Dont Respond."
- APPOINTMENT CONFIRMATION REQUESTS: If the lead is asking about their appointment ("is my appointment still on", "just checking on my booking", "confirming my appointment"), ALWAYS classify as "Respond" -- even if the appointment data shows it's confirmed. The lead asked a question and deserves a response confirming the details.
- DECLINED OFFER/UPSELL: If the lead declines an offer, upsell, or suggestion (e.g., "not right now", "I'm good", "just the botox for now", "maybe later"), classify as "Respond". The agent should acknowledge their decision gracefully and close warmly rather than going silent. Going silent after a lead declines something feels dismissive.

### "Dont Respond" - Skip AI response (RARE)
ONLY use when ALL conditions are met:

**No pending actions:**
- AI's last message was purely a closing statement
- AI was NOT asking anything or offering options
- AI is NOT waiting for a selection or confirmation
- No booking or action needs to be taken
- Conversation goal is genuinely COMPLETE
- If booking was discussed, appointment EXISTS in data

**AND conversation is in TRUE CLOSING PHASE:**
- AI: "You're all set! See you Tuesday!" → Lead: "thanks"
- AI: "Perfect, your appointment is confirmed!" → Lead: "sounds good"
- AI: "Great! Let me know if you need anything" → Lead: "will do"
- AI: "No problem!" → Lead: "Liked 'No problem!'"
- AI: "Have a great day!" → Lead: "you too"

## What This Node Is NOT For:

**DO NOT classify as "Dont Respond" just because:**
- Lead said "thanks" or "perfect" (check if AI was waiting for input first)
- Lead's response is short (short answers to questions still need action)
- Lead answered a question (that's input requiring action, not closing)
- Lead selected an option the AI offered (must act on their selection)
- Lead reacted positively (might have more questions, or AI needs to act)
- The response sounds final but the goal isn't actually complete
- Lead said "see you then" but no appointment exists in data yet
- Lead gave partial info (still need to clarify missing details)
- Lead asked to be contacted later (need to acknowledge)
- Lead shares their situation or constraints (requires handling, not closing)
- Lead mentions they already have something (explore opportunity)
- Lead gives an objection (must handle before giving up)
</classification_criteria>

<examples>
## Examples - RESPOND (Pending Action):

**AI asked/offered something - lead answered:**
- AI: "Perfect, did you want 10:00 AM or 10:30 AM" → Lead: "10am Saturday Ok perfect ty see you on Saturday" → RESPOND (booking not made yet!)
- AI: "Does that time work for you" → Lead: "yes thanks" → RESPOND
- AI: "What areas are you interested in" → Lead: "just my face thanks" → RESPOND
- AI: "Would next Tuesday work" → Lead: "perfect" → RESPOND
- AI: "I have Monday at 2pm or Wednesday at 10am" → Lead: "Monday works ty" → RESPOND
- AI: "Let me know which option works best" → Lead: "the second one please" → RESPOND
- AI: "I can book you in for a consultation" → Lead: "yes please" → RESPOND

**Incomplete information:**
- AI: "I have 10am or 2pm available" → Lead: "morning works" → RESPOND (which day?)
- AI: "What day works for you?" → Lead: "next week sometime" → RESPOND (need specific day)

**Delayed action requests:**
- AI: "Want me to book that for you?" → Lead: "let me check and get back to you" → RESPOND
- AI: "I have openings tomorrow" → Lead: "can you text me in the morning?" → RESPOND

**Appointment confirmation questions (lead has booking but is asking about it):**
- Lead: "hey just making sure my appointment is still on" → RESPOND (confirm their details)
- Lead: "is my appointment this week" → RESPOND (confirm date/time)
- Lead: "wanted to check on my booking" → RESPOND (this is a question, not a closing)



## Examples - RESPOND (Lead Sharing Situation - NOT a Closing):

**Lead provides info that requires follow-up or objection handling:**
- Lead: "we already have something for that" → RESPOND (must explore if it's working)
- Lead: "we already use something like this" → RESPOND (must explore satisfaction)
- Lead: "we got that covered" → RESPOND (must ask how it's going)
- Lead: "already have a system" → RESPOND (must explore)
- Lead: "we're not really looking right now" → RESPOND (soft objection, not opt-out)
- Lead: "we tried this before" → RESPOND (explore what happened)
- Lead: "we don't really need this" → RESPOND (probe deeper)
- Lead: "we're too busy right now" → RESPOND (timing objection, handle it)
- Lead: "I need to talk to my partner first" → RESPOND (acknowledge, offer to help)

**Why these require response:**
These are NOT closings - the lead is sharing their situation. This is valuable intel that requires:
- Exploring dissatisfaction (already have something → ask how it's working)
- Handling the objection before giving up
- Acknowledging constraints and offering alternatives

The conversation is just beginning, not ending. Choosing "Dont Respond" here kills a potential opportunity.

## Examples - DONT RESPOND (True Closing):

**Goal COMPLETE, AI gave closing statement, NOT waiting for anything, appointment confirmed in data:**
- AI: "You're all booked for Saturday at 10:00 AM! You'll get a confirmation shortly. See you then!" → Lead: "thank you so much" → Dont Respond
- AI: "Perfect, your appointment is confirmed for Tuesday at 2pm!" → Lead: "sounds good thanks" → Dont Respond
- AI: "Great, I've sent that info to your email!" → Lead: "got it thanks" → Dont Respond
- AI: "No problem! Have a great day!" → Lead: "you too 👍" → Dont Respond

**Reaction-only responses after closing:**
- AI: "See you Saturday at 10am!" → Lead: "Liked 'See you Saturday at 10am!'" → Dont Respond
- AI: "You're all set!" → Lead: "❤️" → Dont Respond
</examples>

<decision_flow>
## Key Decision Flow:

1. Did AI's last message ask a question (with or without punctuation)? → **RESPOND**
2. Did AI's last message offer options to choose from? → **RESPOND**
3. Is AI waiting for lead to confirm or select something? → **RESPOND**
4. Does AI need to take an action based on lead's response? → **RESPOND**
5. Was booking discussed but NO appointment in data yet? → **RESPOND**
6. Did lead provide incomplete information needing clarification? → **RESPOND**
7. Did lead request delayed action or callback? → **RESPOND**
8. Is the lead asking a question? → **RESPOND**
9. Is the goal INCOMPLETE (booking not made, info not sent)? → **RESPOND**
10. Is goal genuinely COMPLETE + AI gave closing + appointment in data (if applicable) + simple acknowledgment? → **Dont Respond**
11. Uncertain about any of the above? → **RESPOND**

## When Uncertain:

- Not sure if AI was asking something → "Respond"
- Not sure if there's a pending action → "Respond"
- Not sure if the goal is complete → "Respond"
- Not sure if booking was made → "Respond"
- Not sure if lead gave complete info → "Respond"
- ANY possibility the AI needs to do something → "Respond"

**Default to "Respond" when uncertain.** It's better to send an unnecessary response than to leave a lead hanging or miss a booking.

This node prevents "thanks" → "no problem" → "great" loops, but ONLY after all actions are genuinely complete.
</decision_flow>

<output_format>
## OUTPUT FORMAT

You MUST respond with valid JSON only. No other text.

Valid path values: "dont_respond", "respond"

Example:
{"path": "dont_respond", "reason": "Conversation complete, booking confirmed, lead just said thanks"}
{"path": "respond", "reason": "AI asked a question and lead answered, need to take action"}
</output_format>
"""


async def _classify_response(ctx: PipelineContext) -> dict[str, Any]:
    """Run Response Determination classifier."""
    # Build user prompt matching n8n: timeline + appointments only (no contact info)
    user_prompt = _build_rd_context(ctx)

    # Inject supported languages context (cached on ctx)
    langs = ctx.supported_languages
    langs_str = ", ".join(langs)
    system = (
        _RD_SYSTEM_PROMPT
        + f"\n\n<supported_languages>\n## SUPPORTED LANGUAGES\n"
        f"This business supports: {langs_str}\n\n"
        "- If the lead communicates in an unsupported language AND the AI has already "
        "informed them of the language limitation AND the lead's latest message has no "
        "other actionable content (questions, booking intent, etc.) → dont_respond\n"
        "- Otherwise, language alone does NOT justify dont_respond — let the reply agent "
        "handle the language communication\n"
        "</supported_languages>\n\n"
        "<short_positive_reactions>\n## SHORT POSITIVE REACTIONS AFTER A QUESTION\n"
        "If the AI's last message asked the lead a yes/no or commitment question (e.g. "
        "'Want to see it?', 'Want me to show you?', 'Which works?'), and the lead replied "
        "with a short positive token — 'ok' / 'k' / 'sure' / 'yeah' / 'yep' / '👍' / '✅' / "
        "'🆗' / '👌' / 'Y' / 'OK!' / 'sounds good' — that is a green-light response that "
        "REQUIRES the AI to respond (book a slot, deliver the next step, etc.). DO NOT "
        "classify these as dont_respond. They are commitment signals, not conversation "
        "closures. Always RESPOND in this case.\n"
        "</short_positive_reactions>\n\n"
        "<auto_reply_detection>\n## AUTO-REPLY DETECTION\n"
        "If the lead's latest message is clearly an automated/canned response from a "
        "customer-service bot — patterns like 'Thanks for your message, we will get back "
        "to you shortly', 'Auto-reply: Out of office', 'We received your inquiry and will "
        "respond within X hours', 'Thank you for texting [Business]. We will respond...', "
        "or any message signed by the business itself thanking the SENDER for reaching out "
        "— classify as dont_respond with reason 'auto_reply_detected'. There is no human "
        "engaging with this thread; replying just adds noise. Only classify as dont_respond "
        "if the message reads as automated/templated, not as a personal message that "
        "happens to be polite.\n"
        "</auto_reply_detection>\n\n"
        "<wrong_number_or_wrong_contact>\n## WRONG NUMBER / WRONG PERSON\n"
        "If the lead says 'wrong number', 'wrong person', 'you have the wrong person', "
        "'I'm not the owner', 'this isn't [business name]', or any signal that the "
        "message reached someone other than the intended recipient — ALWAYS classify as "
        "RESPOND. The reply path has a wrong_number_pivot script that sends one polite "
        "SMS asking them to forward our info to the actual decision-maker (a receptionist "
        "or family member often will). Treating these as dont_respond loses real-world "
        "referral upside. Never classify these as dont_respond.\n"
        "</wrong_number_or_wrong_contact>"
    )

    from app.models import log_prompt
    _rd_vars: dict[str, Any] = {}
    if ctx.timeline:
        _rd_vars["timeline"] = ctx.timeline
    if ctx.upcoming_booking_text:
        _rd_vars["upcoming_booking_text"] = ctx.upcoming_booking_text
    if ctx.past_booking_text:
        _rd_vars["past_booking_text"] = ctx.past_booking_text
    if ctx.offers_text:
        _rd_vars["offers_config"] = ctx.offers_text
    _rd_vars["supported_languages"] = langs_str
    log_prompt(ctx, "Response Determination", system, user_prompt, variables=_rd_vars)

    return await classify(
        prompt=user_prompt,
        schema=_RD_SCHEMA,
        model=resolve_model(ctx, "response_determination"),
        temperature=resolve_temperature(ctx, "response_determination"),
        system_prompt=system,
        label="response_determination",
    )


# ============================================================================
# SHARED: Build classifier context
# ============================================================================


def _build_tth_context(ctx: PipelineContext) -> str:
    """Build TTH classifier user prompt matching n8n exactly.

    n8n format: timeline + Reply Channel + appointments.
    No Contact Information block, no Current Message block.
    Timeline includes full legend (no stripping).
    """
    parts: list[str] = []

    # Timeline (full, no legend stripping — fix 2.4)
    if ctx.timeline:
        parts.append(ctx.timeline)
    else:
        parts.append("*No conversation history available.*")

    # Reply Channel — standalone line matching n8n (fix 2.3)
    parts.append(f"\n**Reply Channel:** {ctx.channel}")

    # Appointments
    parts.append("\n---\n")
    if ctx.upcoming_booking_text:
        parts.append(ctx.upcoming_booking_text)
    if ctx.past_booking_text:
        parts.append(f"\n{ctx.past_booking_text}")

    return "\n".join(parts)


def _build_rd_context(ctx: PipelineContext) -> str:
    """Build RD classifier user prompt.

    Includes timeline + appointments + upsell prompt (if present).
    No Contact Information, no Reply Channel, no Current Message.
    Timeline includes full legend (no stripping).
    """
    parts: list[str] = []

    # Timeline (full, no legend stripping — fix 2.4)
    if ctx.timeline:
        parts.append(ctx.timeline)
    else:
        parts.append("*No conversation history available.*")

    # Appointments
    if ctx.upcoming_booking_text:
        parts.append(f"\n{ctx.upcoming_booking_text}")
    if ctx.past_booking_text:
        parts.append(f"\n{ctx.past_booking_text}")

    # Offers context — lets classifier know proactive offer opportunities exist
    if ctx.offers_text:
        parts.append(f"\n<offers>\n{ctx.offers_text}\n</offers>")

    return "\n".join(parts)
