"""Phase 5: Follow-up pipeline — determine if follow-up needed, generate message.

Shares Steps 3.2-3.6 with reply pipeline (data loading, sync, timeline, booking).
Then runs follow-up-specific logic:
1. Early gates (stop bot, no lead, has booking)
2. Follow-up needed classifier (should we send a follow-up?)
3. Follow-up message generator (what to say)
4. Store + return response
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from prefect import task

from app.text_engine.followup_scheduling import clear_smart_followup, set_smart_followup
from app.text_engine.utils import extract_bot_name, get_custom_field
from app.models import PipelineContext
from app.services.ai_client import classify
from app.text_engine.model_resolver import resolve_model, resolve_temperature
from app.services.postgres_client import postgres
from app.services.ghl_links import build_ghl_contact_url
from app.services.slack import post_slack_message

logger = logging.getLogger(__name__)


# =========================================================================
# FOLLOW-UP NEEDED CLASSIFIER
# =========================================================================

_DETERMINATION_PROMPT_BASE = """\
<role>
## YOUR ROLE

You are a follow-up decision expert. You analyze conversation history and appointment data to decide \
if this lead needs a follow-up message.
</role>

<cadence_info>
## HOW FOLLOW-UPS WORK

When a lead goes silent, a 6-follow-up cadence begins: 6h → 21h → 24h → 48h → 72h → 144h.
The cadence resets every time the lead replies. You run at each follow-up in the cadence.

Your job: decide if the follow-up should send.
</cadence_info>

<pre_filter_context>
## PRE-FILTER CONTEXT

These leads are blocked BEFORE reaching you:
- Stop bot tag → blocked
- Confirmed upcoming appointment → blocked
- Disqualified per qualification criteria → blocked

If a lead reached you, none of the above apply.
</pre_filter_context>

<decision_criteria>
## IS FOLLOW-UP NEEDED?

This is your primary decision. Most of the time the answer is YES — the lead went silent and needs re-engagement.

### FOLLOW-UP IS NEEDED IF:
- AI's last message is hanging without user response — this is the most important signal
- AI asked questions, made offers, or shared information and got silence
- Previous follow-up(s) were sent but user still hasn't acknowledged
- Lead requested specific timing and that time has now arrived
- Lead has recent cancelled/no-show appointment(s) and hasn't rebooked (within last 3 months)
- User gave a soft/non-committal response like "maybe later", "not right now", "I'll think about it", "let me talk to my [spouse/partner]", "let me check my schedule" — these are warm leads who haven't committed yet. They ALWAYS need follow-up. These are NOT conversation closures.
- User was hostile or dismissive WITHOUT explicitly opting out (e.g., "ugh", "whatever", "leave me alone", rude responses) — hostility alone is NOT an opt-out. The lead hasn't said "stop", "not interested", or "remove me." They still get follow-up.
- The conversation has not progressed beyond initial contact or basic logistics (e.g., the lead only asked "who is this?" or "where are you located?" and the AI answered, but no service discussion, booking attempt, or meaningful engagement has occurred yet) — the conversation hasn't really started and needs a follow-up to re-engage with the original topic
- Lead said goodbye, "I'm all set", or "have a good day" WITHOUT having actually booked an appointment — this is just a polite close to the info-gathering phase. The lead hasn't converted yet. Follow up.
- Lead said they would take action themselves ("I'll call you", "I'll book online", "I'll come by") — these promises often don't materialize. If no booking appeared in the system (the "Booking Exists" gate didn't block this follow-up), the lead didn't follow through. Follow up.
- Lead gave a vague future time reference ("next month", "after the holidays", "when I get paid", "sometime next week") — these are soft deferrals, NOT firm commitments. Treat them the same as "maybe later". Follow up.

### FOLLOW-UP IS NOT NEEDED IF:
- User explicitly opted out ("stop", "unsubscribe", "remove me", "don't contact me", "do not text me")
- User explicitly said "not interested" or clearly stated they do not want the service
- Conversation shows user rebooked after cancellation
- Only appointment data is 6+ months old with no recent conversation activity

### CRITICAL: WHAT MATTERS MOST
Look at the LAST message in the conversation. If the AI's last message is a question or offer hanging without a response, follow-up is needed — regardless of what the lead said earlier in the conversation. A lead who said "ok" or "thanks" mid-conversation but then ghosted the AI's follow-up question still needs follow-up.

Remember: if a lead had truly converted (booked, called, came in), the booking gate would have already stopped this follow-up before it reached you. The fact that you're seeing this conversation means NO booking exists — the lead hasn't followed through on any promises they made.
</decision_criteria>

<call_history_context>
## CALL HISTORY CONTEXT
- If recent call outcome was "booked" → likely no follow-up needed
- If recent call outcome was "not_interested" → no follow-up needed
- If recent call outcome was "transferred" and Staff handled → check if resolved
- If recent call was unresolved/callback scheduled → follow-up may be appropriate
- If lead called recently (within 24h), they're engaged — weigh this in decision
</call_history_context>

<appointment_rules>
## APPOINTMENT DATA RULES

**No appointments** → Decide based on conversation alone

**Cancelled/No-Show appointment(s)** → Context matters:

### Recency Rules (based on dateUpdated — when status changed):
- **Within last 7 days**: Hot lead — strong follow-up signal, they recently showed intent
- **8-30 days ago**: Warm lead — still worth following up, reference the appointment
- **1-3 months ago**: Lukewarm — consider it context but weigh conversation more heavily
- **3+ months ago**: Stale — treat as historical context only, decide based on conversation
- **6+ months ago**: Ignore appointment data — too old to be relevant, decide purely on conversation

### Pattern Recognition:
- **Single cancellation**: Normal — life happens, follow up
- **Multiple cancellations**: Flaky lead pattern — still follow up but note the pattern
- **No-show**: Flakier than cancellation — they didn't even communicate, still follow up but note this
- **Multiple no-shows**: Very unreliable lead — still follow up but temper expectations

### Scheduled Time Context:
- **Was scheduled within 48 hours of cancellation**: Urgent intent shown — strong follow-up signal
- **Was scheduled weeks/months out**: Less urgent but still showed intent

### Exception:
- If conversation clearly shows they rebooked, resolved the issue, or moved forward after the cancellation → no follow-up needed
</appointment_rules>

<previous_followup_language>
## PREVIOUS FOLLOW-UP LANGUAGE

Previous follow-ups may contain de-escalation phrases like "no worries", "whenever you're ready", or "no pressure." These are tone choices by the follow-up generator, NOT conversation closures. The lead still hasn't responded. Evaluate whether follow-up is needed based on the lead's actions (or lack thereof) and any manual/call activity since — not on the wording of previous AI follow-ups.
</previous_followup_language>

<sms_reactions>
## SMS REACTIONS

Leads can react to messages (iMessage tapbacks) that appear in chat history as:
- "Liked [message]", "Loved [message]", "Laughed at [message]"
- "Emphasized [message]", "Questioned [message]", "Disliked [message]"

**A reaction ALONE is NOT a real reply.** It is a soft acknowledgment — like a head nod.

**Reaction without any text reply:**
- If the AI asked a question and the lead only reacted → **NEEDED** (reaction does not answer the question)
- If the AI made an offer or suggested booking and the lead only reacted → **NEEDED** (they showed interest but did not commit or respond)
- A reaction is NOT a conversation closure — it is a non-committal engagement signal, similar to "maybe later"

**Reaction with text reply:**
- Ignore the reaction, analyze the text reply normally
</sms_reactions>

<timeline_reading>
## READING THE TIMELINE

Messages are displayed MOST RECENT FIRST (newest at top, oldest at bottom). The LAST message in the list is the FIRST message ever sent. To determine if a follow-up is needed, check who sent the most recent message (top of the list). If the AI/DRIP sent the last message and the lead hasn't replied, a follow-up is likely needed.
</timeline_reading>

<rules>
## CRITICAL RULES
- DEFAULT BEHAVIOR: Send follow-up unless conversation clearly closed by user
- RECENT CANCELLED APPOINTMENTS (within 3 months): Strong signal to follow up — they showed intent
- OLD APPOINTMENT DATA (6+ months): Ignore it — decide based on conversation only
- CONTEXT AWARENESS: Multiple follow-ups in history are normal. Your job is only to assess the current conversation state
- DATE AWARENESS: Always consider how recent the appointment activity is before weighing it in your decision
</rules>
"""

# --- Reschedule section: ONLY included on follow-up #1 ---
_DETERMINATION_RESCHEDULE_SECTION = """
<reschedule_option>
## RESCHEDULE OPTION

You have a **reschedule** option. Setting reschedule=true means "don't send a follow-up now, \
but schedule one for a more appropriate time later." A scheduling agent will pick the exact timeframe.

reschedule=true is the RIGHT choice when:
- Lead said they need to consult someone ("talk to my wife", "ask my doctor")
- Lead is traveling or temporarily unavailable ("on vacation", "out of town")
- Lead mentioned a future timeframe ("maybe next month", "after the holidays", "in a few weeks")
- Lead has a budget/financial constraint ("saving up", "not right now financially")
- Lead is checking their schedule or finances ("let me check my calendar", "need to look at my budget")
- The conversation suggests this isn't the right time but the lead IS still interested

reschedule=true is WRONG when:
- The lead IS ready and responsive — just send the follow-up (followUpNeeded=true)
- The lead explicitly opted out ("stop", "not interested", "remove me") — no reschedule, no follow-up (both false)
- The lead ghosted with no timing signal — standard cadence handles this (followUpNeeded=true)

### ACTIVE TIMER (if present)

If you see "**ACTIVE TIMER:**" in the context, this means a previous follow-up was already \
rescheduled for a specific reason. The scheduled time has now arrived.

- Default: **followUpNeeded: true** — the timer arrived, honor it and send the follow-up
- Reschedule again ONLY if the original blocker is clearly still unresolved (e.g., "traveling 2 weeks" but lead extended their trip)
- Do NOT follow up if the lead opted out since the timer was set
</reschedule_option>
"""

# --- Output format sections (conditional) ---
_OUTPUT_FORMAT_WITH_RESCHEDULE = """
<output_format>
## OUTPUT FORMAT
Return valid JSON:
{{"followUpNeeded": true, "reschedule": false, "reason": "Brief analysis..."}}
{{"followUpNeeded": false, "reschedule": true, "reason": "Lead traveling for 2 weeks, reschedule."}}
{{"followUpNeeded": false, "reschedule": false, "reason": "Lead opted out permanently."}}

Note: If followUpNeeded=true, reschedule MUST be false. Reschedule only applies when followUpNeeded=false — it means "don't send now, but come back later."

The reason field is REQUIRED and must NEVER be empty. Always explain your decision in 1-2 sentences.
</output_format>
"""

_OUTPUT_FORMAT_SIMPLE = """
<output_format>
## OUTPUT FORMAT
Return valid JSON:
{{"followUpNeeded": true, "reason": "Brief analysis of why follow-up is needed"}}
{{"followUpNeeded": false, "reason": "Specific explanation of why no follow-up is needed"}}

The reason field is REQUIRED and must NEVER be empty. Always explain your decision in 1-2 sentences.
</output_format>
"""

# --- Schema variants ---
_SCHEMA_WITH_RESCHEDULE = {
    "type": "object",
    "properties": {
        "followUpNeeded": {"type": "boolean"},
        "reschedule": {
            "type": "boolean",
            "description": "If followUpNeeded=false, should we reschedule for later? Only true when followUpNeeded=false.",
        },
        "reason": {"type": "string", "description": "Required explanation of why follow-up is or is not needed. Must not be empty."},
    },
    "required": ["followUpNeeded", "reschedule", "reason"],
    "additionalProperties": False,
}

_SCHEMA_SIMPLE = {
    "type": "object",
    "properties": {
        "followUpNeeded": {"type": "boolean"},
        "reason": {"type": "string", "description": "Required explanation of why follow-up is or is not needed. Must not be empty."},
    },
    "required": ["followUpNeeded", "reason"],
    "additionalProperties": False,
}


def _build_determination_prompt(followup_count: int) -> tuple[str, dict]:
    """Build the determination prompt and schema based on follow-up position.

    Follow-up #1 (followup_count=0): Full prompt with reschedule option + smart timer section.
    Follow-up #2+ (followup_count>0): Simplified prompt — no reschedule, no smart timer.
    """
    if followup_count == 0:
        prompt = _DETERMINATION_PROMPT_BASE + _DETERMINATION_RESCHEDULE_SECTION + _OUTPUT_FORMAT_WITH_RESCHEDULE
        schema = _SCHEMA_WITH_RESCHEDULE
    else:
        prompt = _DETERMINATION_PROMPT_BASE + _OUTPUT_FORMAT_SIMPLE
        schema = _SCHEMA_SIMPLE
    return prompt, schema


# =========================================================================
# SMART FOLLOW-UP SCHEDULER (runs only when determination says reschedule)
# =========================================================================

# Production follow-up cadence: 6h → 21h → 24h → 48h → 72h → 144h
_DEFAULT_CADENCE_HOURS = [6.0, 21.0, 24.0, 48.0, 72.0, 144.0]

_SMART_SCHEDULER_PROMPT = """\
<role>
## SMART FOLLOW-UP SCHEDULER

You are a follow-up scheduling specialist. The determination agent decided this lead needs a \
custom follow-up time instead of the standard cadence. Your job: pick the right timeframe \
based on the conversation context.
</role>

<timeline_reading>
Messages are displayed MOST RECENT FIRST (newest at top, oldest at bottom).
</timeline_reading>

<available_timeframes>
## AVAILABLE TIMEFRAMES

tomorrow, 2_days, 3_days, 4-6_days, 1_week, 2_weeks, 3_weeks, 1_month, 2_months, 3_months, 4_months
</available_timeframes>

<lead_signaled_timing>
## LEAD-SIGNALED TIMING (takes priority)

If the lead gave a timing signal, honor it.

**Explicit timeframe:**
- "in 2 weeks" / "after my trip in 2 weeks" → 2_weeks
- "next month" / "in a month" → 1_month
- "after the holidays" → estimate from current date
- "in a few days" / "later this week" → 3_days
- "tomorrow" / "I'll get back to you tomorrow" → tomorrow
- "give me a couple days" → 2_days

**Consult someone** (spouse, partner, parent, doctor): 3_days default, match timing if stated.

**Checking schedule / finances** (calendar, bank account, budget): 2_days default, match timing if stated.

**Medical / health constraint:**
- Long-term (months) → 3_months to 4_months
- Medium-term (weeks, e.g. surgery recovery) → 1_month to 3_months
- Short-term (days/weeks, e.g. illness) → 2_weeks to 1_month

**Travel / temporary absence:**
- "Back next week" → 1_week
- "On vacation for 2 weeks" → 2_weeks
- "Moving this month" → 1_month
- No specific return date → 2_weeks

**Budget constraint (stalled):**
- AI already offered cheaper alternatives → 3_days to 1_week
- Just "too expensive", no alternatives discussed → 2_weeks to 1_month
- Saving up / "not right now financially" → 1_month to 2_months

**Seasonal / event-based:**
- Tied to a specific event ("after my wedding", "after summer") → estimate from current date
- "When I get my tax refund" → estimate (Feb-April typically)
- No clear date → 1_month
</lead_signaled_timing>

<no_explicit_signal>
## NO EXPLICIT TIMING SIGNAL

If the lead didn't mention a specific time but the conversation suggests the standard cadence \
(next follow-up in ~21 hours) is too aggressive:
- Conversation ended tensely → tomorrow or 2_days (cool off period)
- Thorough conversation, everything was covered → tomorrow or 2_days
- Lead is clearly busy but didn't say when → tomorrow or 2_days
- Lead showed interest but needs breathing room → 2_days to 3_days
</no_explicit_signal>

<output_format>
## OUTPUT FORMAT
Return valid JSON:
{{"timeframe": "2_weeks", "reason": "Lead traveling for 2 weeks, will follow up after return."}}

Valid timeframe values: "tomorrow", "2_days", "3_days", "4-6_days", "1_week", "2_weeks", "3_weeks", "1_month", "2_months", "3_months", "4_months"

CURRENT DATE AND TIME: {current_datetime}
</output_format>
"""

_SMART_SCHEDULER_SCHEMA = {
    "type": "object",
    "properties": {
        "timeframe": {
            "type": "string",
            "enum": [
                "tomorrow", "2_days", "3_days", "4-6_days",
                "1_week", "2_weeks", "3_weeks",
                "1_month", "2_months", "3_months", "4_months",
            ],
        },
        "reason": {"type": "string", "description": "Brief explanation of why this timeframe was chosen"},
    },
    "required": ["timeframe", "reason"],
    "additionalProperties": False,
}


_CONTEXT_REACTIVE_SCHEMA = {
    "type": "object",
    "properties": {
        "timeframe": {
            "type": "string",
            "enum": [
                "no_reschedule",
                "tomorrow", "2_days", "3_days", "4-6_days",
                "1_week", "2_weeks", "3_weeks",
                "1_month", "2_months", "3_months", "4_months",
            ],
        },
        "reason": {"type": "string", "description": "Brief explanation — timing signal found or no signal detected"},
    },
    "required": ["timeframe", "reason"],
    "additionalProperties": False,
}

_CONTEXT_REACTIVE_PROMPT = """\
<role>
## CONTEXT-REACTIVE TIMING DETECTOR

You evaluate whether a lead gave a specific timing signal for when to follow up.
This is NOT the normal scheduling mode — you are ONLY looking for explicit timing requests.
</role>

<timeline_reading>
Messages are displayed MOST RECENT FIRST (newest at top, oldest at bottom).
</timeline_reading>

<rules>
## RULES

1. Search the conversation for any explicit timing signal from the lead:
   - "reach out next week" / "call me in 2 weeks" → match that timeframe
   - "I'm on vacation until Friday" / "traveling for a month" → match return date
   - "after the holidays" / "when I get paid" → estimate from current date
   - "give me a couple days" / "let me think about it" → 2_days
   - "not right now" / "maybe later" → NOT a timing signal (too vague)

2. If the lead gave a clear timing signal → return the matching timeframe
3. If there is NO explicit timing signal → return "no_reschedule"
4. When in doubt, return "no_reschedule" — the standard cadence will handle it

CURRENT DATE AND TIME: {current_datetime}
</rules>

<output_format>
Return valid JSON:
{{"timeframe": "2_weeks", "reason": "Lead said they're traveling for 2 weeks"}}
or
{{"timeframe": "no_reschedule", "reason": "No explicit timing signal from lead"}}
</output_format>
"""


def _build_cadence_context(followup_count: int) -> str:
    """Build a human-readable cadence position string for the scheduler.

    Shows which follow-up number this is, when the next standard follow-up
    would fire, and the full cadence sequence with a position marker.
    """
    cadence = _DEFAULT_CADENCE_HOURS
    # followup_count is 0-based (number of PREVIOUS consecutive follow-ups)
    # So this is follow-up #(followup_count + 1)
    current_num = followup_count + 1
    total = len(cadence)

    # Current interval (what triggered this follow-up)
    current_idx = min(followup_count, total - 1)
    current_hours = cadence[current_idx]

    # Next interval (what would fire next in standard cadence)
    next_idx = min(followup_count + 1, total - 1)
    next_hours = cadence[next_idx]

    # Format cadence with position marker
    cadence_parts = []
    for i, h in enumerate(cadence):
        label = f"{h}h" if h < 24 else f"{h/24:.0f}d"
        if i == current_idx:
            cadence_parts.append(f"[{label}]")  # current position
        else:
            cadence_parts.append(label)

    cadence_str = " → ".join(cadence_parts)

    # Format next interval as human-readable
    if next_hours < 24:
        next_label = f"{next_hours:.0f} hours"
    else:
        next_label = f"{next_hours / 24:.0f} days"

    return (
        f"## Follow-Up Cadence Position\n"
        f"This is follow-up #{current_num} of {total}.\n"
        f"Next standard follow-up would fire in {next_label}.\n"
        f"Cadence: {cadence_str}\n"
    )


async def _smart_schedule_followup(
    ctx: PipelineContext,
    followup_count: int,
    context_reactive: bool = False,
) -> dict[str, Any]:
    """Run the smart scheduler agent to pick a follow-up timeframe.

    Normal mode: called when determination says reschedule=true. Always picks a timeframe.
    Context-reactive mode: called when skip_determination is on. Only picks a timeframe
    if the lead gave an explicit timing signal. Returns "no_reschedule" if no signal found.
    """
    from app.text_engine.utils import get_custom_field, get_timezone

    tz = ctx.tz or get_timezone(ctx.config)
    now = datetime.now(tz)

    if context_reactive:
        system = _CONTEXT_REACTIVE_PROMPT.format(current_datetime=now.isoformat())
        schema = _CONTEXT_REACTIVE_SCHEMA
        label = "context_reactive_scheduler"
    else:
        system = _SMART_SCHEDULER_PROMPT.format(current_datetime=now.isoformat())
        schema = _SMART_SCHEDULER_SCHEMA
        label = "smart_scheduler"

    cadence_context = _build_cadence_context(followup_count)

    # Include original timer context if this is a re-reschedule (read from scheduled_message metadata)
    timer_section = ""
    if ctx.scheduled_message:
        sm_meta = ctx.scheduled_message.get("metadata") or {}
        prev_reason = sm_meta.get("reason") or ctx.scheduled_message.get("smart_reason", "")
        if prev_reason:
            prev_timeframe = sm_meta.get("timeframe", "unknown")
            prev_due = sm_meta.get("due", "unknown")
            timer_section = f"## Previous Timer\nThis lead was already rescheduled before. Original timer: {prev_timeframe} (due: {prev_due}) | Reason: {prev_reason}\n\n"

    user_prompt = (
        f"## Conversation History\n{ctx.timeline}\n\n"
        f"{timer_section}"
        f"{cadence_context}\n"
        f"{'Check if the lead gave a timing signal.' if context_reactive else 'Pick the best follow-up timeframe for this lead.'}"
    )

    from app.models import log_prompt
    _ss_vars: dict[str, Any] = {"current_datetime": now.isoformat(), "context_reactive": context_reactive}
    if ctx.timeline:
        _ss_vars["timeline"] = ctx.timeline
    if timer_value:
        _ss_vars["previous_timer"] = timer_value
    log_prompt(ctx, f"{'Context-Reactive' if context_reactive else 'Smart'} Scheduler", system, user_prompt, variables=_ss_vars)

    result = await classify(
        prompt=user_prompt,
        schema=schema,
        system_prompt=system,
        model=resolve_model(ctx, "smart_scheduler"),
        temperature=resolve_temperature(ctx, "smart_scheduler"),
        label=label,
    )

    logger.info(
        "SMART SCHEDULER | timeframe=%s | reason=%s",
        result.get("timeframe", "unknown"),
        result.get("reason", "")[:200],
    )

    return result


# =========================================================================
# FOLLOW-UP MESSAGE GENERATOR
# =========================================================================

_FOLLOWUP_AGENT_SYSTEM = """\
<role>
## YOUR ROLE
You are a human sales rep following up with leads via text message. Your messages should \
sound like a real person texting — casual, natural, and conversational. You are NOT an AI \
chatbot. You are NOT a reply agent. You GENERATE FOLLOW-UP MESSAGES.

SMS style rules: never use dashes (em dashes, en dashes, or hyphens used as dashes), \
never use bullet points, never use markdown formatting. Real people don't text with dashes.
</role>

{compliance_section}\
{supported_languages_section}\

<call_history_context>
## CALL HISTORY CONTEXT

If the conversation includes call logs:
- Reference calls naturally if relevant: "following up on your call"
- Don't repeat questions they already answered on the call
- If call discussed specific service/topic, tie follow-up to that
- If Staff handled the call, don't contradict what they said
</call_history_context>

<appointment_context>
## APPOINTMENT CONTEXT
{appointment_context}

### How to Use Appointment Data

The data above covers the last 30 days only.

**Cancelled appointment:**
- Acknowledge it briefly — "I know things come up" or "I know life gets busy"
- Offer to reschedule naturally — "want to find a new time?"
- Do NOT pretend the appointment never existed
- Do NOT say "you cancelled" — use neutral framing like "that slot didn't work out"
- Do NOT guilt-trip

**No-show (missed appointment):**
- Use a warm "we missed you" approach
- Do NOT say the appointment was never scheduled (it was — they just didn't show)
- Do NOT ask "does that time still work?" if the appointment already passed
- Offer to reschedule: "want to find a new time that works better?"
- Keep it brief, no guilt, no pressure

**No appointment data:**
- Ignore this section, focus on conversation only
</appointment_context>

<sms_reactions>
## Understanding Reactions (iMessage Tapbacks)

When reading the conversation timeline, you will sometimes see lines like:
- "Liked [original message text]"
- "Loved [original message text]"
- "Laughed at [original message text]"
- "Emphasized [original message text]"

These are iMessage tapback reactions — the lead tapped a button on a previous message. \
They did NOT type a reply. The content they reacted to can hint at their interest \
(e.g., liking a "mornings?" question suggests they prefer mornings), so use that as \
context for your topic choice. But a reaction is not a conversation turn — the lead \
has not actually responded.

Write your follow-up as if the lead went quiet after the last real message. Never \
acknowledge, reference, or comment on the reaction itself. No "glad you liked that", \
no "caught your eye", no "sounds like you're interested", no "stood out to you" — \
just continue the conversation naturally using whatever context you have.
</sms_reactions>

<timing_followups>
## TIMING-BASED FOLLOW-UPS

If the lead previously requested a specific follow-up timeframe (e.g., "call me in 2 weeks"), \
you're generating this follow-up BECAUSE that requested time has arrived.

- Acknowledge you're following their requested timing
- Reference the timeframe naturally
- Don't use generic check-in language
- Examples: "wanted to reach out like you asked", "hope now works better for you"
</timing_followups>

<tone_rules>
## TONE RULE: NO FINALITY LANGUAGE

Never use phrases that signal you're giving up or closing the door:
- "no worries if the timing isn't right"
- "we're here whenever you're ready"
- "just wanted to let you know we're here"
- "no pressure at all"
- "if you ever change your mind"
- "this is my last message"
- "I won't bother you again"

These de-escalate the conversation and make the next follow-up feel contradictory. Instead, \
always leave an open thread — a question, a new angle, a reason for them to respond.
</tone_rules>

<uniqueness_requirement>
## UNIQUENESS REQUIREMENT

Before generating, review ALL previous follow-ups in the conversation:
- NEVER repeat previous wording — each follow-up must be completely different
- Different angles on same topics — approach from new perspective each time
- Rephrase rather than repeat — same info, different words
- Different hooks to re-engage based on conversation context
</uniqueness_requirement>

<contextual_specificity>
## CONTEXTUAL SPECIFICITY
Reference specific details from the conversation — NOT generic templates.

- Mention the SPECIFIC service, product, or topic the lead discussed
- If the lead asked a specific question, reference it
- If the lead mentioned a personal detail, weave it in naturally
- If the AI made a specific offer, reference it
- Avoid generic "still interested?" messages when you have specific context
- The more specific, the more it feels like a real person remembering the conversation
</contextual_specificity>

<banned_phrases>
## BANNED PHRASES
Never start a message with: "just checking in", "circling back", "following up", \
"just wanted to check", "I wanted to reach out", "I hope this message finds you well", \
"I understand you're busy", "I hope all is well"

Never use anywhere in a follow-up: "just curious", "curious what you think", "I was curious", \
"was curious", "curious if", "quick question", "quick thought", "quick one"

Never use filler transitions that add no value — go straight to the point. \
No "was thinking about," "had a thought about," "been meaning to ask," or similar padding.

Never start consecutive follow-ups with the same opening. If the previous follow-up \
started one way, this one MUST open differently. Vary your openings naturally and \
respect the configured voice/style rules above — sometimes lead with the topic directly, \
sometimes use a softer transition, sometimes reference the context immediately. \
Check the conversation history and make sure your opening is different from the last follow-up.
</banned_phrases>

{banned_phrases_section}\
{media_rules}\
<output_format>
## OUTPUT FORMAT

Return valid JSON: {{"suggestedFollowUp": "your message text here"}}
</output_format>

<rules>
## CRITICAL RULES

**ALWAYS:**
- Follow style rules from the Follow Up Preferences section below (it has authority)
- Review ALL previous follow-ups — never repeat wording
- Reference timing requests if they exist
- Use the services reference for accurate service/price mentions

**NEVER:**
- Output placeholders like [Service] — always use real values from context
- Ignore previous follow-up attempts
- Contradict what was said in calls or previous messages
</rules>

<services_reference>
### Services Reference:
{services_prompt}
</services_reference>

{offers_section}\

{urgency_scarcity_section}\
{bot_persona_section}\
{case_studies_section}\
<followup_preferences>
### Follow Up Preferences & Style Rules:
{followup_agent_prompt}
</followup_preferences>
"""

_FOLLOWUP_GEN_SCHEMA = {
    "type": "object",
    "properties": {
        "suggestedFollowUp": {
            "type": "string",
            "description": "Follow-up message text",
        },
    },
    "required": ["suggestedFollowUp"],
}


# =========================================================================
# STATIC TEMPLATE RESOLVER (lightweight LLM call for variable resolution)
# =========================================================================

_STATIC_TEMPLATE_SYSTEM = """\
<role>
You are resolving a follow-up message template for an SMS conversation. \
Your job is to replace variables like {{name}}, {{service}}, {{last_topic}} with real values \
from the conversation context, and lightly personalize the template per the instructions below. \
Do NOT rewrite the message structure. Do NOT add content beyond what the template specifies.
</role>

<instructions>
## Processing Instructions
{static_instructions}
</instructions>

<rules>
## RULES
- Your output MUST be the template with variables filled in — nothing else
- Do NOT rephrase, reword, add sentences, remove sentences, or change the message structure in any way
- Replace all variables (e.g. {{name}}, {{service}}, {{last_topic}}) with real values from the conversation
- If a variable cannot be resolved (no data available), follow the Processing Instructions \
above for how to handle missing values
- Preserve the exact punctuation, capitalization style, and emoji usage of the original template
- Output ONLY the resolved message text — no quotes, no markdown, no explanation
- If media is attached, do NOT mention or reference the media in your text
</rules>

{media_rules}\
{supported_languages_section}\

<output_format>
Return valid JSON: {{"suggestedFollowUp": "your resolved message here"}}
</output_format>
"""


# =========================================================================
# MEDIA SELECTOR (dedicated LLM agent — fix 3.17, matches n8n Media Selector node)
# =========================================================================

_MEDIA_SELECTOR_PROMPT = """\
<role>
## YOUR ROLE
You decide whether a follow-up message should include media (GIF, image, or voice note) \
or be text-only. You do NOT write text messages. You ONLY decide on media.
</role>

<context>
## CONTEXT

### Follow Up Preferences & Style Rules:
-{followup_agent_prompt}

### Services Reference:
-{services_prompt}

{bot_persona}\
{urgency_scarcity}\
</context>

<timeline_reading>
## READING THE TIMELINE
Messages in the conversation history are displayed MOST RECENT FIRST (newest at top, oldest at bottom). \
The lead's most recent message is the one closest to the top labeled LEAD.
</timeline_reading>

<available_media>
## AVAILABLE MEDIA

{media_library}

The list above has been pre-filtered — spacing, reuse, channel compatibility, and phone \
validation are already handled. Every item shown is safe to use.
</available_media>

<selection_rules>
## SELECTION RULES

- 0 = no media (a text-only follow-up will be generated separately).

**Matching principle:** Think about what a human sales rep would actually text \
this lead. Consider how engaged the lead was, what they're specifically \
discussing, and what tone fits the conversation.

- **Voice notes** require a strong, specific signal — the lead must have \
expressed a clear concern, intent, or emotion that directly matches the \
voice note's content. Low-engagement leads (1-2 casual messages, \
"maybe sometime", cold outreach) should NOT get voice notes — that's \
too personal for someone who barely engaged.
- **Images** should match what the conversation is currently about, not \
just the general topic. Read each image's description and pick the one \
that fits what the lead is asking about RIGHT NOW in the conversation.
- **GIFs** are a light, casual touch that work in most situations. When \
engagement is low, the tone is casual, or no voice note/image is a clear \
match, a GIF is usually the right call.
- **Different angle:** If the AI already discussed something specific in the \
conversation (a deal, results, pricing, etc.), don't just re-send media about \
the same thing. Try a different angle to give the lead a new reason to engage. \
Exception: if the lead was actively trying to book or showed clear intent to \
come in, prioritize media that helps close that — don't pivot away from booking.
- If nothing fits naturally, pick 0.
- If the list says "No media available" — you MUST output 0.
</selection_rules>

<uniqueness>
## UNIQUENESS
The list has been pre-filtered: recently used items are already removed. Every item \
shown is safe to use — no dedup needed on your part.
</uniqueness>

<output_format>
## OUTPUT FORMAT

Return valid JSON with one field:
{{"media_selection": N}}

0 = no media. Otherwise, the number of the item you want to select from the list above.

**Return valid JSON.**
</output_format>
"""

_MEDIA_SELECTOR_SCHEMA = {
    "type": "object",
    "properties": {
        "media_selection": {
            "type": "integer",
            "description": "Media item number (0 = no media)",
        },
    },
    "required": ["media_selection"],
    "additionalProperties": False,
}


# =========================================================================
# MAIN PIPELINE
# =========================================================================


@task(name="followup_pipeline_steps", retries=1, retry_delay_seconds=5, timeout_seconds=120)
async def run_followup(ctx: PipelineContext) -> dict[str, Any]:
    """Run follow-up pipeline after data loading + timeline are complete.

    Called from pipeline.py after Steps 3.2-3.6 have run.
    """

    # === CLEAR SMART FOLLOWUP (every run) ===
    # If a follow-up was triggered, the timer has fired — clear it regardless of outcome.
    # Matches n8n: timer cleanup happens early, not on specific exit paths.
    await clear_smart_followup(ctx)

    logger.info("FOLLOWUP | start | contact=%s | channel=%s | tags=%d | has_lead=%s",
                ctx.contact_id, ctx.channel, len(ctx.contact_tags), ctx.lead is not None)

    # === EARLY GATES ===

    # Read follow-up gate config from matched setter's conversation.follow_up
    _setter = ctx.compiled.get("_matched_setter") or {}
    _fu_config = _setter.get("conversation", {}).get("follow_up") or {}
    _fu_sections = _fu_config

    # Gate 1: Stop bot tag (case-sensitive "stop bot" check) — ALWAYS ON, not configurable
    if any("stop bot" in t for t in ctx.contact_tags):
        logger.info("FOLLOWUP | gate_blocked | gate=stop_bot")
        # Cancel all pending scheduled messages + clear GHL field
        if not ctx.is_test_mode:
            try:
                from app.services.message_scheduler import cancel_pending, update_ghl_followup_field
                from app.main import supabase as _sb
                await cancel_pending(_sb, ctx.contact_id, ctx.entity_id, message_types=["followup", "smart_followup"], reason="stop_bot")
                await update_ghl_followup_field(ctx.ghl, ctx.contact_id)
            except Exception as _e:
                logger.warning("Gate cancel failed: %s", _e)
        return {
            "followUpNeeded": False,
            "path": "Bot Is Turned Off",
            "reason": "Contact has stop_bot tag",
        }

    # Gate 2: No-lead does NOT block follow-ups (matches n8n behavior).

    # Gate 3: Lead is permanently disqualified (configurable — default ON)
    _skip_disqualified = _fu_sections.get("skip_if_disqualified", True)
    if _skip_disqualified and ctx.lead and ctx.lead.get("qualification_status") == "disqualified":
        logger.info("FOLLOWUP | gate_blocked | gate=disqualified")
        if not ctx.is_test_mode:
            try:
                from app.services.message_scheduler import cancel_pending, update_ghl_followup_field
                from app.main import supabase as _sb
                await cancel_pending(_sb, ctx.contact_id, ctx.entity_id, message_types=["followup", "smart_followup"], reason="disqualified")
                await update_ghl_followup_field(ctx.ghl, ctx.contact_id)
            except Exception as _e:
                logger.warning("Gate cancel failed: %s", _e)
        return {
            "followUpNeeded": False,
            "path": "Disqualified",
            "reason": "Lead is permanently disqualified per qualification criteria",
        }

    # Gate 4: Booking exists (configurable — default ON)
    _skip_upcoming = _fu_sections.get("skip_if_upcoming_appointment", True)
    if _skip_upcoming and ctx.has_upcoming_booking:
        logger.info("FOLLOWUP | gate_blocked | gate=booking_exists")
        if not ctx.is_test_mode:
            try:
                from app.services.message_scheduler import cancel_pending, update_ghl_followup_field
                from app.main import supabase as _sb
                await cancel_pending(_sb, ctx.contact_id, ctx.entity_id, message_types=["followup", "smart_followup"], reason="booking_confirmed")
                await update_ghl_followup_field(ctx.ghl, ctx.contact_id)
            except Exception as _e:
                logger.warning("Gate cancel failed: %s", _e)
        return {
            "followUpNeeded": False,
            "path": "Booking Exists",
            "reason": "Contact has confirmed upcoming appointment",
        }

    # === FOLLOW-UP NEEDED CLASSIFIER ===

    # Count consecutive AI/follow-up messages with no lead response
    followup_count = _count_consecutive_followups(ctx.chat_history)

    # Skip determination mode: bypass the classifier entirely, always follow up
    _skip_determination = _fu_sections.get("skip_determination", False)
    if _skip_determination:
        logger.info("FOLLOWUP | skip_determination | always_followup=true | followup_count=%d", followup_count)

        # Context-reactive scheduler on FU#1 only — check if lead gave timing signal
        if followup_count == 0:
            sched_result = await _smart_schedule_followup(ctx, followup_count, context_reactive=True)
            timeframe = sched_result.get("timeframe", "no_reschedule")
            if timeframe != "no_reschedule":
                reason = sched_result.get("reason", "")
                timer_data = await set_smart_followup(ctx, timeframe, reason)
                logger.info("FOLLOWUP | context_reactive_reschedule | timeframe=%s | reason=%s", timeframe, reason[:200])
                return {
                    "followUpNeeded": False,
                    "path": "Rescheduled (context-reactive)",
                    "reschedule_timeframe": timeframe,
                    "reschedule_reason": reason,
                    "timer_data": timer_data,
                }

        # No reschedule — fall through to text generation (skip to GENERATE section)
        decision = {"followUpNeeded": True, "reason": "skip_determination mode — always follow up"}

    else:
        # === NORMAL DETERMINATION FLOW ===

        # Build context for classifier — use ALL appointments (including cancelled/no-show)
        all_appts = ctx.all_bookings or ctx.bookings or []
        logger.info("FOLLOWUP | classifier_input | followup_count=%d | appointments=%d | timeline_len=%d",
                    followup_count, len(all_appts), len(ctx.timeline or ""))
        appointment_text = ""
        if all_appts:
            appointment_text = "\n".join(
                f"- {b.get('title', 'Appointment')}: {b.get('start', b.get('startTime', ''))} "
                f"(status: {b.get('status', b.get('appointmentStatus', 'unknown'))})"
                for b in all_appts
            )

        now = datetime.now(timezone.utc).isoformat()
        if followup_count > 0:
            followup_context = (
                f"## Follow-up Context\n"
                f"This is consecutive follow-up #{followup_count + 1} in the current cycle "
                f"(the lead has not responded to the last {followup_count} follow-up(s) in a row). "
                f"This count resets when the lead responds.\n\n"
            )
        else:
            followup_context = (
                f"## Follow-up Context\n"
                f"This is the FIRST follow-up. The lead went silent after the AI's last reply. "
                f"Hours have passed. You are NOT replying to their message — that reply already "
                f"happened above in the conversation. You are re-engaging a lead who went quiet. "
                f"Your message should feel like a new touchpoint, not a continuation of the "
                f"previous exchange.\n\n"
            )

        # Smart follow-up detection — check if this was fired from a smart_followup scheduled row
        is_smart_followup = (
            ctx.scheduled_message is not None
            and ctx.scheduled_message.get("source") == "smart_reschedule"
        )

        # Timer context ONLY on follow-up #1 (followup_count == 0).
        timer_context = ""
        if followup_count == 0 and ctx.scheduled_message:
            sm_metadata = ctx.scheduled_message.get("metadata") or {}
            smart_reason = ctx.scheduled_message.get("smart_reason") or sm_metadata.get("reason", "")
            if smart_reason:
                timeframe = sm_metadata.get("timeframe", "unknown")
                due = sm_metadata.get("due", "unknown")
                timer_context = f"## Smart Follow-Up Timer\n**ACTIVE TIMER:** {timeframe} (due: {due}) | Reason: {smart_reason}\n\n"

        # Build determination prompt conditionally — #1 gets reschedule, #2+ does not
        determination_prompt, determination_schema = _build_determination_prompt(followup_count)

        # Inject supported languages (cached on ctx)
        langs = ctx.supported_languages
        langs_str = ", ".join(langs)
        determination_prompt += (
            f"\n\n<supported_languages>\n## SUPPORTED LANGUAGES\n"
            f"This business supports: {langs_str}\n\n"
            "- If the lead's messages are entirely in an unsupported language, "
            "do NOT follow up (followUpNeeded: false) — the business cannot serve them\n"
            "- If the lead used a supported language at any point in the conversation, "
            "follow-up is appropriate regardless of their latest message language\n"
            "- Analyze the conversation in whatever language it's conducted in\n"
            "</supported_languages>"
        )

        # Inject agent goals + lead source for threshold decisions
        goals = ctx.compiled.get("agent_goals_summary", "")
        if goals:
            determination_prompt += (
                f"\n\n<agent_context>\n## Agent Goals\n{goals}\n</agent_context>"
            )
        lead_source = ctx.compiled.get("agent_lead_source", "")
        if lead_source:
            determination_prompt += (
                f"\n\n<lead_source>\n## Lead Source\n{lead_source}\n</lead_source>"
            )
        positions = ctx.compiled.get("followup_positions_determination", "")
        if positions:
            determination_prompt += (
                f"\n\n<follow_up_positions>\n{positions}\n</follow_up_positions>"
            )

        # Build cadence context for determination agent
        cadence_context = _build_cadence_context(followup_count)

        user_prompt = (
            f"## Conversation History\n{ctx.timeline}\n\n"
            f"{followup_context}"
            f"{cadence_context}\n"
            f"{timer_context}"
            f"## Appointment Data\n{ctx.past_booking_text or appointment_text or 'No past appointments'}\n\n"
            f"## Current Date\n{now}\n\n"
            f"Should a follow-up message be sent?"
        )

        from app.models import log_prompt
        _fd_vars: dict[str, Any] = {}
        if ctx.timeline:
            _fd_vars["timeline"] = ctx.timeline
        if ctx.past_booking_text:
            _fd_vars["past_booking_text"] = ctx.past_booking_text
        if timer_context:
            _fd_vars["timer_context"] = timer_value or ""
        _fd_vars["supported_languages"] = langs_str
        log_prompt(ctx, "Follow-Up Determination", determination_prompt, user_prompt, variables=_fd_vars)

        # In text_generator_only mode, skip the determination LLM call entirely
        # and force followUpNeeded=True so we fall through to the text generator.
        if ctx.text_generator_only:
            logger.info("FOLLOWUP | text_generator_only | skipping determination agent")
            decision = {"followUpNeeded": True, "reason": "text_generator_only mode"}
        else:
            decision = await classify(
                prompt=user_prompt,
                schema=determination_schema,
                system_prompt=determination_prompt,
                model=resolve_model(ctx, "followup_determination"),
                temperature=resolve_temperature(ctx, "followup_determination"),
                label="followup_determination",
            )

    logger.info(
        "FOLLOWUP CLASSIFIER | needed=%s | reschedule=%s | followup_num=%d | reason=%s",
        decision.get("followUpNeeded", False),
        decision.get("reschedule", "N/A"),
        followup_count + 1,
        decision.get("reason", "")[:200],
    )

    if not decision.get("followUpNeeded", False):
        # Reschedule path — only possible on follow-up #1.
        # On #2+, the schema doesn't even include reschedule, so this can't fire.
        if decision.get("reschedule", False) and followup_count == 0:
            # Route to smart scheduler agent
            sched_result = await _smart_schedule_followup(
                ctx, followup_count,
            )
            timeframe = sched_result.get("timeframe", "2_weeks")
            reason = sched_result.get("reason", "")

            # Set new smart follow-up timer
            timer_data = await set_smart_followup(ctx, timeframe, reason)

            logger.info(
                "FOLLOWUP | rescheduled | timeframe=%s | reason=%s",
                timeframe, reason[:200],
            )

            return {
                "followUpNeeded": False,
                "path": "Rescheduled",
                "reschedule_timeframe": timeframe,
                "reschedule_reason": reason,
                "timer_data": timer_data,
                "determination_reason": decision.get("reason", ""),
            }

        # Standard "not needed" path — no reschedule
        # Update GHL field so clients see current status (fixes stale data)
        if not ctx.is_test_mode:
            try:
                from app.services.message_scheduler import update_ghl_followup_field
                await update_ghl_followup_field(ctx.ghl, ctx.contact_id)
            except Exception:
                pass

        return {
            "followUpNeeded": False,
            "reason": (decision.get("reason") or "").strip() or "No follow-up needed",
        }

    # === DETERMINATION-ONLY SHORT-CIRCUIT (test mode) ===
    # When determination_only is set, return the classifier decision without
    # running media selector or text generator — saves 2 LLM calls per follow-up.
    if ctx.determination_only:
        logger.info("FOLLOWUP | determination_only | short-circuit after classifier")
        return {
            "followUpNeeded": True,
            "reason": decision.get("reason", ""),
            "path": "determination_only",
            "reschedule": decision.get("reschedule", False),
            "determination_decision": decision,
        }

    # === GENERATE FOLLOW-UP MESSAGE ===

    # Resolve follow-up prompt from compiled config
    followup_prompt = ctx.compiled.get("followup_preferences", "")

    # === TEXT-GENERATOR-ONLY SHORT-CIRCUIT (test mode) ===
    # Skip media cap + filter + selector, set up mock media variables,
    # then fall through to STEP 2 (text generator).
    if ctx.text_generator_only:
        logger.info("FOLLOWUP | text_generator_only | skipping media pipeline")
        _mock = ctx.mock_media or {}
        media_idx = 1 if _mock else 0
        media_name = _mock.get("name", "")
        media_type = _mock.get("type", "")
        media_description = _mock.get("description", "")
        media_url = _mock.get("url", "https://mock-media.test/mock") if _mock else None
        needs_text = True
        filtered_media = [_mock] if _mock else []

        _services_text = ctx.services_text

    else:
        # Services text (cached on ctx after load_data)
        _services_text = ctx.services_text

        # === PROBABILISTIC MEDIA CAP ===
        # Target ~1.5 media per 6-FU cadence. Count media already sent in this
        # follow-up sequence and gate further selections probabilistically.
        import random as _rng
        _cap_media_markers = (
            "[AI sent GIF:", "[AI sent gif:", "[AI sent image:",
            "[AI sent voice note:", "[AI sent media:",
        )
        _media_sent_count = 0
        for _row in ctx.chat_history:
            if _row.get("role") != "ai":
                continue
            _src = (_row.get("source") or "").lower()
            if _src not in _FOLLOWUP_SOURCES:
                continue
            _content = _row.get("content") or ""
            if any(m in _content for m in _cap_media_markers):
                _media_sent_count += 1
        _media_capped = False
        if _media_sent_count >= 3:
            _media_capped = True
        elif _media_sent_count >= 2:
            _media_capped = _rng.random() > 0.20  # 80% chance of blocking

        if _media_capped:
            # Skip media selector LLM call entirely — save tokens/cost
            filtered_media = []
            media_url = None
            media_type = None
            media_name = None
            media_description = ""
            media_idx = 0
            media_selection = {"media_selection": 0}
            logger.info("FOLLOWUP | media_cap | blocked | media_already_sent=%d", _media_sent_count)
        else:
            # Filter media library (pre-filter, channel gating, phone validation, spacing, dedup)
            # Test/production source already resolved in data_loading.py
            # Use follow-up-specific media library when available (system_config path)
            _fu_media_source = ctx.followup_media_library if ctx.followup_media_library else ctx.media_library
            filtered_media = _filter_media_library(
                _fu_media_source, ctx.chat_history, ctx.channel, ctx.contact_tags,
                is_smart_followup=is_smart_followup,
            )

            # Build media library prompt section for Media Selector
            if filtered_media:
                _rng.shuffle(filtered_media)   # randomize order to avoid positional bias
                media_lines = []
                for i, item in enumerate(filtered_media, 1):
                    if isinstance(item, dict):
                        name = item.get("name", item.get("url", f"Item {i}"))
                        mtype = item.get("type", "gif")
                        desc = item.get("description", "")
                        media_lines.append(f'{i}. "{name}" ({mtype})\n   {desc}')
                    else:
                        media_lines.append(f"{i}. {item}")
                media_library_text = (
                    "Select by NUMBER, or 0 for none:\n\n"
                    + "\n\n---\n\n".join(media_lines)
                )
            else:
                media_library_text = "No media available for this follow-up. You MUST output media_selection: 0."

            # === STEP 1: MEDIA SELECTOR (fix 3.17 — dedicated LLM call matching n8n Media Selector) ===
            # _services_text already defined above (before media cap check)

            # Use compiled services for media selector
            _media_svc = ctx.compiled.get("services_names", _services_text)
            media_selector_system = _MEDIA_SELECTOR_PROMPT.format(
                followup_agent_prompt=followup_prompt,
                services_prompt=_media_svc,
                media_library=media_library_text,
                bot_persona=ctx.compiled.get("bot_persona_media", ""),
                urgency_scarcity=ctx.compiled.get("agent_urgency_scarcity", ""),
            )

            # Compute time since last lead message for media selector context
            _time_since_label = ""
            for _row in ctx.chat_history:
                if _row.get("role") == "human":
                    _ts = _row.get("timestamp")
                    if isinstance(_ts, str):
                        from app.text_engine.timeline import parse_datetime
                        _ts = parse_datetime(_ts)
                    if _ts:
                        _delta = datetime.now(timezone.utc) - _ts
                        _days = _delta.days
                        _hours = _delta.seconds // 3600
                        if _days >= 1:
                            _time_since_label = f"{_days} day{'s' if _days != 1 else ''}"
                        else:
                            _time_since_label = f"{_hours} hour{'s' if _hours != 1 else ''}"
                    break  # chat_history is most-recent-first, so first human = last lead msg

            _time_line = f"\n## Time Since Last Contact\n{_time_since_label}\n" if _time_since_label else ""

            media_selector_prompt = (
                f"## Conversation History\n{ctx.timeline}\n\n"
                f"## Appointment Data\n{ctx.past_booking_text or 'No past appointments'}\n"
                f"{_time_line}\n"
                f"Should this follow-up include media? Pick an item number if something fits, otherwise 0."
            )

            from app.models import log_prompt
            _ms_vars: dict[str, Any] = {
                "followup_agent_prompt": followup_prompt,
                "services": _services_text,
                "media_library": media_library_text,
            }
            if ctx.timeline:
                _ms_vars["timeline"] = ctx.timeline
            if ctx.past_booking_text:
                _ms_vars["past_booking_text"] = ctx.past_booking_text
            log_prompt(ctx, "Media Selector", media_selector_system, media_selector_prompt, variables=_ms_vars)

            media_selection = await classify(
                prompt=media_selector_prompt,
                schema=_MEDIA_SELECTOR_SCHEMA,
                system_prompt=media_selector_system,
                model=resolve_model(ctx, "followup_media"),
                temperature=resolve_temperature(ctx, "followup_media"),
                label="followup_media",
            )

            # === RESOLVE MEDIA URL + TYPE (matches n8n Resolve Media URL node) ===
            media_url = None
            media_type = None
            media_name = None
            media_description = ""
            media_idx = media_selection.get("media_selection", 0)
            logger.info("FOLLOWUP | media_selector | selected_index=%d | filtered_count=%d", media_idx, len(filtered_media))
            if media_idx and media_idx > 0:
                if isinstance(filtered_media, list) and media_idx <= len(filtered_media):
                    media_item = filtered_media[media_idx - 1]
                    if isinstance(media_item, dict):
                        media_url = media_item.get("url", "")
                        media_type = media_item.get("type", "gif")
                        media_name = media_item.get("name", "")
                        media_description = media_item.get("description", "")
                    else:
                        media_url = str(media_item)
                        media_type = "gif"
                        media_description = ""

        # === NEEDS TEXT? ===
        # No media → always text. Images → always companion text (standalone image
        # looks like a mass-blast, not a personal follow-up).
        # GIF/voice_note → companion text only when BOTH conditions are true:
        #   1. Client is iMessage-enabled (imessage=true in config)
        #   2. Channel is SMS or iMessage (these are the only channels where
        #      iMessage rendering applies — Instagram, Facebook, WhatsApp etc.
        #      handle media natively and don't need companion text)
        if not media_idx or media_idx == 0:
            needs_text = True  # No media selected — text-only follow-up
        elif media_type == "image":
            needs_text = True  # Images always get companion text
        else:
            # GIF, voice_note — companion text only for iMessage clients on SMS channel
            _ch = (ctx.channel or "").lower().strip()
            _is_imessage_channel = _ch in ("sms", "imessage", "")
            _sc = ctx.config.get("system_config") or {}
            if isinstance(_sc, str):
                import json as _json
                try:
                    _sc = _json.loads(_sc)
                except (ValueError, TypeError):
                    _sc = {}
            _provider = _sc.get("sms_provider", "signalhouse")
            needs_text = (_provider == "imessage") and _is_imessage_channel

        logger.info("FOLLOWUP | media_resolved | type=%s | url=%s | needs_text=%s",
                    media_type or "none", bool(media_url), needs_text)

        # === MEDIA-ONLY SHORT-CIRCUIT (test mode) ===
        # When media_only is set, return after media selection without running text generator.
        # Store in chat history first so multi-FU sequences can see prior follow-ups
        # (needed for Rule 0 counting, Rule 1 spacing, Rule 2 dedup).
        if ctx.media_only:
            if not ctx.is_test_mode:
                if media_url or media_name:
                    await _store_followup(
                        ctx, "", media_url=media_url, media_type=media_type,
                        media_name=media_name, media_description=media_description,
                    )
                else:
                    await _store_followup(ctx, "(media-only test: no media selected)")
            logger.info("FOLLOWUP | media_only | short-circuit after media selector")
            return {
                "followUpNeeded": True,
                "reason": decision.get("reason", ""),
                "path": "media_only",
                "media_url": media_url or "",
                "media_type": media_type or "",
                "media_name": media_name or "",
                "media_description": media_description,
                "media_selection_index": media_idx,
                "media_selection_reasoning": media_selection.get("reasoning", ""),
                "filtered_media_count": len(filtered_media),
                "needs_text": needs_text,
                "suggestedFollowUp": "",
            }

    # === STEP 2: TEXT GENERATOR (only if needs_text — matches n8n routing) ===
    suggested = ""
    _using_static = False
    _static_template_position = 0
    if needs_text:
        # --- Static template mode check ---
        _fu_mode = _fu_config.get("mode", "ai_generated")
        _static_templates = _fu_config.get("templates") or []

        if _fu_mode == "static_templates" and _static_templates:
            # Position is 1-based (template.position), followup_count is 0-based
            _target_position = followup_count + 1
            _matched_template = None
            for _t in _static_templates:
                if isinstance(_t, dict) and _t.get("position") == _target_position and (_t.get("message") or "").strip():
                    _matched_template = _t
                    break

            if _matched_template:
                _template_msg = _matched_template["message"]
                _timing_label = _matched_template.get("timing_label", "")
                _static_instructions = (_fu_config.get("static_instructions") or "").strip()

                logger.info(
                    "FOLLOWUP | static_template | position=%d | timing=%s | template_len=%d",
                    _target_position, _timing_label, len(_template_msg),
                )

                # Build media context (same logic as AI-gen path)
                _st_media_context = ""
                if media_idx and media_idx > 0 and media_name:
                    _mt = (media_type or "gif").lower().strip()
                    _desc_label = "Transcript" if _mt == "voice_note" else "Description"
                    _st_media_context = (
                        f"\n\n## Attached Media\n"
                        f"- Name: {media_name}\n"
                        f"- Type: {media_type or 'gif'}\n"
                        f"- {_desc_label}: {media_description}\n"
                    )

                # Build media rules for static template (voice note needs short teaser override)
                _st_media_rules = ""
                _st_selected_type = (media_type or "").lower().strip()
                if _st_selected_type == "voice_note":
                    _st_media_rules = (
                        "## MEDIA RULES (Voice Note)\n"
                        "A voice note is attached. OVERRIDE the template: instead output a short "
                        "personalized teaser of 10 words or less related to the conversation topic. "
                        "Do NOT mention the voice note.\n\n---\n\n"
                    )
                elif _st_selected_type == "image":
                    _st_media_rules = (
                        "## MEDIA RULES (Image)\n"
                        "An image is attached with your text. Resolve the template normally — "
                        "the image complements the message.\n\n---\n\n"
                    )
                elif _st_selected_type == "gif":
                    _st_media_rules = (
                        "## MEDIA RULES (GIF)\n"
                        "A GIF is attached with your text. Resolve the template normally — "
                        "the GIF adds personality. Do not reference the GIF.\n\n---\n\n"
                    )

                # Build language section
                _st_langs = ctx.supported_languages
                _st_langs_str = ", ".join(_st_langs)
                _st_lang_section = (
                    "<supported_languages>\n"
                    f"This business supports: {_st_langs_str}\n"
                    f"Resolve the template in the same language the lead used. Default: {_st_langs[0]}\n"
                    "</supported_languages>\n"
                )

                static_system = _STATIC_TEMPLATE_SYSTEM.format(
                    static_instructions=_static_instructions or "(No additional instructions provided.)",
                    media_rules=_st_media_rules,
                    supported_languages_section=_st_lang_section,
                )

                static_prompt = (
                    f"**Timeline reading order:** Most recent first.\n\n"
                    f"## Conversation History\n{ctx.timeline}\n\n"
                    f"## Template to Resolve\n{_template_msg}\n\n"
                    f"Resolve the variables in the template above using the conversation context. "
                    f"Return the result as JSON."
                    f"{_st_media_context}"
                )

                from app.models import log_prompt
                _st_vars: dict[str, Any] = {
                    "mode": "static_templates",
                    "position": str(_target_position),
                    "timing_label": _timing_label,
                    "template_message": _template_msg,
                    "static_instructions": _static_instructions or "(none)",
                }
                if ctx.timeline:
                    _st_vars["timeline"] = ctx.timeline
                log_prompt(ctx, "Follow-Up Static Template", static_system, static_prompt, variables=_st_vars)

                generation = await classify(
                    prompt=static_prompt,
                    schema=_FOLLOWUP_GEN_SCHEMA,
                    system_prompt=static_system,
                    model=resolve_model(ctx, "followup_text"),
                    temperature=resolve_temperature(ctx, "followup_text"),
                    label="followup_text",
                )

                suggested = generation.get("suggestedFollowUp", "")

                if suggested and suggested.strip():
                    _using_static = True
                    _static_template_position = _target_position
                    logger.info(
                        "FOLLOWUP | static_template_resolved | position=%d | result_len=%d",
                        _target_position, len(suggested),
                    )
                else:
                    logger.warning(
                        "FOLLOWUP | static_template_empty | position=%d | falling back to AI generation",
                        _target_position,
                    )
            else:
                logger.info(
                    "FOLLOWUP | static_template_miss | position=%d | available=%s | falling back to AI generation",
                    _target_position,
                    [t.get("position") for t in _static_templates if isinstance(t, dict)],
                )

        # --- AI generation (default path, OR fallback from static template miss/empty) ---
        if not _using_static:
            # Build media context for text generator — full details for all types
            if media_idx and media_idx > 0 and media_name:
                _mt = (media_type or "gif").lower().strip()
                _desc_label = "Transcript" if _mt == "voice_note" else "Description"
                media_context = (
                    f"MEDIA CONTEXT:\n"
                    f"- Name: {media_name}\n"
                    f"- Type: {media_type or 'gif'}\n"
                    f"- {_desc_label}: {media_description}\n"
                    f"Follow the media rules in the system prompt for this type."
                )
            else:
                media_context = ""

            # Build appointment context for generator — past-only (Gate 4 pre-filters upcoming)
            gen_appointment_text = ctx.past_booking_text or appointment_text or "No appointment data"

            # Build offers section
            _offers_text = ctx.compiled.get("offers_text", "")
            if _offers_text:
                _offers_section = (
                    "### Current Offers & Specials\n\n"
                    "IMPORTANT: Only mention an offer below if it directly relates to the service the lead was "
                    "discussing. If no offer matches the conversation topic, don't mention any. Never pivot to "
                    "an unrelated promotion.\n\n"
                    f"{_offers_text}"
                )
            else:
                _offers_section = ""

            # Build compliance section (right after YOUR ROLE — high priority)
            _compliance = ctx.compiled.get("compliance_rules", "")
            if _compliance:
                _compliance_section = (
                    "# COMPLIANCE RULES — MUST FOLLOW\n\n"
                    f"{_compliance}\n\n"
                    "These rules are non-negotiable. Always follow them when composing your message.\n\n---\n\n"
                )
            else:
                _compliance_section = ""

            # Build bot persona section
            _persona = ctx.compiled.get("bot_persona_followup", "")
            if _persona:
                _persona_section = f"### Bot Persona:\n{_persona}\n\n"
            else:
                _persona_section = ""

            # Build banned phrases section
            _banned_section = ""
            _banned = ctx.compiled.get("followup_banned_phrases", "")
            if _banned:
                _banned_section = f"\n## CLIENT-SPECIFIC BANNED PHRASES\n{_banned}\n\n"

            # Build case studies section
            _cs_section = ""
            _cs = ctx.compiled.get("case_studies_followup", "")
            if _cs:
                _cs_section = f"<case_studies>\n## Case Studies (reference material)\n{_cs}\n</case_studies>\n\n"

            # Build urgency/scarcity section
            _urgency_section = ""
            _us = ctx.compiled.get("agent_urgency_scarcity", "")
            if _us:
                _urgency_section = f"\n## Urgency & Scarcity Context\n{_us}\n\n"

            # Build dynamic media rules — only show rules for the selected media type
            _selected_type = (media_type or "").lower().strip()
            if _selected_type == "voice_note":
                _media_rules = (
                    "## MEDIA RULES (Voice Note)\n"
                    "A voice note is attached alongside your text. Keep your text to 10 words "
                    "or less — a short personalized teaser related to the conversation topic. "
                    "Do NOT mention the voice note (no \"sent you a voice note\", no \"just sent\", "
                    "no \"voice note\", no \"voice message\", no \"quick message\"). The lead sees "
                    "the voice note right next to your text so announcing it is redundant. "
                    "Instead write a brief topical nudge that connects what the lead was "
                    "discussing with what the voice note transcript covers. Use both the "
                    "conversation context and the transcript to craft a short teaser that "
                    "bridges the two. "
                    "Follow these rules even if the voice note topic doesn't perfectly match "
                    "the conversation.\n\n---\n\n"
                )
            elif _selected_type == "image":
                _media_rules = (
                    "## MEDIA RULES (Image)\n"
                    "An image is being sent with your text. Write a full companion message that "
                    "references and complements the image. Images are typically before/after photos, "
                    "promo graphics, or portfolio work. Your text should describe or react to what "
                    "the image shows and tie it back to the conversation. "
                    "Follow these rules even if the image topic doesn't perfectly match "
                    "the conversation.\n\n---\n\n"
                )
            elif _selected_type == "gif":
                _media_rules = (
                    "## MEDIA RULES (GIF)\n"
                    "A GIF is being sent with your text. Write a normal follow-up message. The GIF "
                    "adds personality but your text carries the conversation. Do not reference the "
                    "GIF directly — your text should work on its own. "
                    "Follow these rules even if the GIF topic doesn't perfectly match "
                    "the conversation.\n\n---\n\n"
                )
            else:
                _media_rules = ""

            _langs = ctx.supported_languages
            _langs_str = ", ".join(_langs)
            _lang_section = (
                "<supported_languages>\n"
                "## SUPPORTED LANGUAGES\n"
                f"This business supports: {_langs_str}\n\n"
                f"- Generate the follow-up in the same language the lead has been communicating in\n"
                f"- If no prior conversation exists, use {_langs[0]}\n"
                "- Never switch languages from what the lead was using\n"
                "</supported_languages>\n"
            )

            system = _FOLLOWUP_AGENT_SYSTEM.format(
                compliance_section=_compliance_section,
                followup_agent_prompt=followup_prompt,
                services_prompt=_services_text,  # From service_config
                offers_section=_offers_section,
                bot_persona_section=_persona_section,
                appointment_context=gen_appointment_text,
                media_rules=_media_rules,
                supported_languages_section=_lang_section,
                banned_phrases_section=_banned_section,
                case_studies_section=_cs_section,
                urgency_scarcity_section=_urgency_section,
            )

            # Compute time since last lead message for text generator context
            _gen_time_label = ""
            for _row in ctx.chat_history:
                if _row.get("role") == "human":
                    _ts = _row.get("timestamp")
                    if isinstance(_ts, str):
                        from app.text_engine.timeline import parse_datetime
                        _ts = parse_datetime(_ts)
                    if _ts:
                        _delta = datetime.now(timezone.utc) - _ts
                        _days = _delta.days
                        _hours = _delta.seconds // 3600
                        if _days >= 1:
                            _gen_time_label = f"{_days} day{'s' if _days != 1 else ''}"
                        else:
                            _gen_time_label = f"{_hours} hour{'s' if _hours != 1 else ''}"
                    break
            _gen_time_line = (
                f"\n## Time Since Last Lead Message\n"
                f"The lead has been silent for {_gen_time_label}. Your message is a re-engagement "
                f"after this silence, not a reply to their last message.\n"
            ) if _gen_time_label else ""

            gen_prompt = (
                f"**Timeline reading order:** Messages are displayed MOST RECENT FIRST "
                f"(newest at top, oldest at bottom). The lead's most recent message is "
                f"the one closest to the top labeled LEAD.\n\n"
                f"## Conversation History\n{ctx.timeline}\n\n"
                f"{followup_context}"
                f"{_gen_time_line}"
                f"{media_context}\n\n"
                f"Generate a follow-up message for this lead. "
                f"Review all previous follow-ups and make this one unique."
            )

            from app.text_engine.data_loading import resolve_supported_languages as _resolve_langs
            _fg_vars: dict[str, Any] = {
                "followup_agent_prompt": followup_prompt,
                "offers_config": _offers_text or "",
                "services": _services_text,
                "appointment_context": gen_appointment_text,
                "bot_persona": ctx.compiled.get("bot_persona_followup", ""),
                "compliance_rules": ctx.compiled.get("compliance_rules", ""),
                "supported_languages": ", ".join(_resolve_langs(ctx)),
            }
            if ctx.timeline:
                _fg_vars["timeline"] = ctx.timeline
            log_prompt(ctx, "Follow-Up Text Generator", system, gen_prompt, variables=_fg_vars)

            generation = await classify(
                prompt=gen_prompt,
                schema=_FOLLOWUP_GEN_SCHEMA,
                system_prompt=system,
                model=resolve_model(ctx, "followup_text"),
                temperature=resolve_temperature(ctx, "followup_text"),
                label="followup_text",
            )

            suggested = generation.get("suggestedFollowUp", "")

            # Safety Layer 1b: Empty text generator guard
            if not suggested or not suggested.strip():
                logger.warning(
                    "Follow-up text generator returned empty (first attempt) — retrying | client=%s contact=%s",
                    ctx.config.get("name", ""),
                    ctx.contact_id,
                )
                generation = await classify(
                    prompt=gen_prompt,
                    schema=_FOLLOWUP_GEN_SCHEMA,
                    system_prompt=system,
                    model=resolve_model(ctx, "followup_text"),
                    temperature=resolve_temperature(ctx, "followup_text"),
                    label="followup_text",
                )
                suggested = generation.get("suggestedFollowUp", "")

                if not suggested or not suggested.strip():
                    logger.error(
                        "Follow-up text generator returned empty after retry | client=%s contact=%s",
                        ctx.config.get("name", ""),
                        ctx.contact_id,
                    )
                    ghl_link = ""
                    if ctx.ghl_location_id:
                        ghl_url = build_ghl_contact_url(
                            ctx.ghl_location_id,
                            ctx.contact_id,
                            (ctx.config or {}).get("ghl_domain", ""),
                        )
                        ghl_link = f"\n*GHL Link:* <{ghl_url}|View Contact>" if ghl_url else ""
                    alert_text = (
                        f":warning: *Empty Follow-Up Text*\n"
                        f"*Client:* {ctx.config.get("name", "Unknown")}\n"
                        f"*Contact:* {ctx.contact_name} (`{ctx.contact_id}`)\n"
                        f"*Channel:* {ctx.channel}\n"
                        f"*Follow-Up #:* {followup_count + 1}\n"
                        f"*What happened:* Follow-up text generator returned empty after 2 attempts."
                        f"{ghl_link}"
                    )
                    try:
                        await post_slack_message("#python-errors", alert_text)
                    except Exception as e:
                        logger.warning("Failed to send Slack alert for empty follow-up text: %s", e)
                    suggested = ""

    # === SECURITY (dash cleanup + term replacements + optional LLM compliance check) ===
    if suggested:
        from app.text_engine.security import (
            strip_punctuation_dashes,
            apply_term_replacements_standalone,
            run_security_check_standalone,
        )

        # Step 0: Dash cleanup (always runs)
        suggested = strip_punctuation_dashes(suggested)

        term_replacements = ctx.compiled.get("term_replacements", [])
        if term_replacements and isinstance(term_replacements, list):
            suggested = apply_term_replacements_standalone(suggested, term_replacements)

        compliance_rules = ctx.compiled.get("compliance_rules", "")
        suggested = await run_security_check_standalone(
            text=suggested,
            compliance_rules=compliance_rules,
            conversation_context=ctx.timeline[:2000] if ctx.timeline else "",
            ctx=ctx,
        )

    # === DELIVERY ===

    # Send email follow-up directly (PROD-ONLY, email channel only)
    if ctx.channel.lower() == "email" and suggested and not ctx.is_test_mode:
        await _send_followup_email(ctx, suggested, media_url=media_url)

    # Send SMS/DM directly via GHL API with delivery confirmation (PROD-ONLY)
    if not ctx.is_test_mode and ctx.channel.lower() != "email" and (suggested or media_url):
        from app.services.delivery_service import DeliveryService
        delivery_svc = DeliveryService(ctx.ghl, ctx.config)
        fu_result = await delivery_svc.send_sms(
            contact_id=ctx.contact_id,
            message=suggested or "",
            media_url=media_url or None,
            to_phone=getattr(ctx, "contact_phone", None),
            message_type="followup",
        )
        if fu_result.status == "failed":
            logger.warning("FOLLOWUP | delivery_failed | contact=%s | error=%s", ctx.contact_id, fu_result.error_message)

    logger.info("FOLLOWUP | delivery | sms=%s | email=%s",
                not ctx.is_test_mode and ctx.channel.lower() != "email" and (suggested or media_url),
                ctx.channel.lower() == "email" and suggested and not ctx.is_test_mode)

    # Store follow-up in chat history AFTER delivery (no ghost messages on failure)
    # Skip in test mode — simulator stores results in simulations JSONB, not chat DB
    if (suggested or media_url) and not ctx.is_test_mode:
        await _store_followup(
            ctx, suggested, media_url=media_url, media_type=media_type,
            media_name=media_name, media_description=media_description,
        )

    # Build inline response (always returned — for test mode visibility + logging)
    result: dict[str, Any] = {
        "followUpNeeded": True,
        "reason": decision.get("reason", ""),
        "suggestedFollowUp": suggested,
        "media_url": media_url or "",
        "filtered_media_count": len(filtered_media),
        "followup_mode": "static_templates" if _using_static else "ai_generated",
    }
    if _using_static:
        result["static_template_position"] = _static_template_position
    if media_type:
        result["media_type"] = media_type
    if media_name:
        result["media_name"] = media_name
    if media_description:
        result["media_description"] = media_description

    # Populate artifact diagnostics
    ctx.followup_stats = {
        "consecutive_followups": followup_count,
        "attempt_number": followup_count + 1,
        "is_smart_followup": is_smart_followup,
        "media_library_total": len(ctx.media_library or []),
        "media_library_filtered": len(filtered_media),
        "media_selected_index": media_idx,
        "media_selected_name": media_name or "",
        "media_selected_type": media_type or "",
        "media_selected_description": media_description[:100] if media_description else "",
        "needs_text": needs_text,
        "followup_mode": "static_templates" if _using_static else "ai_generated",
        "static_template_position": _static_template_position if _using_static else 0,
    }

    logger.info(
        "FOLLOWUP GENERATED | text=%s | media_name=%s | media_type=%s | media_url=%s",
        suggested or "(media only)",
        media_name or "(none)",
        media_type or "(none)",
        media_url or "(none)",
    )
    return result


async def _store_followup(
    ctx: PipelineContext,
    message: str,
    media_url: str | None = None,
    media_type: str | None = None,
    media_name: str | None = None,
    media_description: str = "",
) -> None:
    """Store follow-up message in chat history with media markers."""
    chat_table = ctx.config.get("chat_history_table_name", "")
    if not chat_table:
        return

    lead_id = ctx.lead.get("id") if ctx.lead else None

    # Prepend media marker so future follow-ups know what was sent
    # Uses granular format [AI sent TYPE: NAME - DESC] for dedup matching
    content = message
    marker = None

    if media_type == "voice_note":
        name_desc = media_name or "voice note"
        if media_name and media_description:
            name_desc = f"{media_name} - {media_description}"
        marker = f"[AI sent voice note: {name_desc}]"
    elif media_url:
        # GIF / image — standard media (requires media_url)
        display_type = "GIF" if (media_type or "gif") == "gif" else media_type or "gif"
        if display_type == "image":
            display_type = "image"
        name_desc = media_name or media_url
        if media_name and media_description:
            name_desc = f"{media_name} - {media_description}"
        marker = f"[AI sent {display_type}: {name_desc}]"

    if marker:
        content = f"{marker}\n{message}" if message else marker

    fu_kwargs: dict[str, Any] = {
        "source": "follow_up",
        "channel": ctx.channel,
    }
    # Legacy compat: store media metadata in additional_kwargs for old-schema tables
    if media_url:
        fu_kwargs["media_url"] = media_url
        fu_kwargs["media_type"] = media_type or "image"
        fu_kwargs["media_name"] = media_name or ""
        fu_kwargs["media_description"] = media_description or ""

    # Create attachment record for AI follow-up media (new schema uses attachment_ids)
    attachment_ids: list[str] = []
    if media_url:
        try:
            media_desc = media_name or ""
            if media_description:
                media_desc = f"{media_desc}: {media_description}" if media_desc else media_description
            record = await postgres.insert_attachment({
                "session_id": ctx.contact_id,
                "client_bot_id": ctx.entity_id,
                "lead_id": lead_id,
                "type": media_type or "image",
                "url": media_url,
                "original_url": media_url,
                "ghl_message_id": None,
                "description": media_desc,
                "raw_analysis": None,
                "message_timestamp": datetime.now(timezone.utc),
            })
            attachment_ids.append(str(record["id"]))
            logger.info("Created attachment record for follow-up media: id=%s", record["id"])
        except Exception as e:
            logger.warning("Failed to create attachment for follow-up media: %s", e)

    msg = {
        "type": "ai",
        "content": content,
        "additional_kwargs": fu_kwargs,
        "response_metadata": {},
    }

    try:
        await postgres.insert_message(
            table=chat_table,
            session_id=ctx.contact_id,
            message=msg,
            lead_id=lead_id,
            attachment_ids=attachment_ids or None,
            workflow_run_id=ctx.workflow_run_id or None,
        )
    except Exception as e:
        logger.warning("Follow-up history storage failed: %s", e)


async def _send_followup_email(
    ctx: PipelineContext,
    message: str,
    media_url: str | None = None,
) -> None:
    """Send follow-up email — delegates to shared send_threaded_email."""
    from app.text_engine.delivery import send_threaded_email

    await send_threaded_email(
        ctx, message, media_url=media_url, default_subject="Following up",
        email_model=resolve_model(ctx, "followup_email_formatting"),
        email_temperature=resolve_temperature(ctx, "followup_email_formatting"),
    )


# =========================================================================
# HELPERS
# =========================================================================


_FOLLOWUP_SOURCES = frozenset(("follow_up", "followup", "follow up"))


def _count_consecutive_followups(chat_history: list[dict[str, Any]]) -> int:
    """Count consecutive follow-up messages with no lead response.

    Reads from most recent to oldest. Only counts AI messages with
    a follow-up source — regular AI replies (source="AI") don't count.
    Accepts all source variants: "follow_up", "followup", "follow up".
    """
    count = 0
    for row in chat_history:
        role = row.get("role", "")
        if role == "ai":
            source = (row.get("source") or "").lower()
            if source in _FOLLOWUP_SOURCES:
                count += 1
            # Non-follow_up AI messages (e.g. source="AI") — skip, keep looking
        elif role == "human":
            break
    return count


def _filter_media_library(
    media_library: list,
    chat_history: list[dict[str, Any]],
    channel: str,
    contact_tags: list[str] | None = None,
    is_smart_followup: bool = False,
) -> list:
    """Filter media library matching n8n's Filter Media Library node.

    Test/production source selection is handled upstream in data_loading.py.
    This function only applies filtering rules:
    - Pre-filter: Remove items with enabled=false or empty/missing description
    - Rule 3 (Channel gating): Remove voice_note items on email channel
    - Rule 0 (First follow-up gate): Block ALL media on 1st standard follow-up
      (allow on smart follow-ups with smartfollowup tag, or subsequent follow-ups)
    - Rule 1 (Spacing): Block ALL media if the most recent AI message included media
    - Rule 2 (Dedup): Remove items already used in the last 20 messages
    """
    if not media_library or not isinstance(media_library, list):
        return []

    contact_tags = contact_tags or []
    tags_lower = [t.lower() for t in contact_tags]

    # --- Pre-filter: remove disabled items, items without description, and non-followup items ---
    library = []
    for item in media_library:
        if isinstance(item, dict):
            if item.get("enabled") is False:
                continue
            desc = item.get("description", "")
            if not desc or not str(desc).strip():
                continue
            if "followup" not in item.get("path", ["followup"]):
                continue
            library.append(item)
        else:
            library.append(item)

    if not library:
        return []

    # --- Rule 3 (Channel gating): Remove voice_note on email channel ---
    library = [
        item for item in library
        if not (channel.lower() == "email" and isinstance(item, dict) and item.get("type") == "voice_note")
    ]

    # --- Count existing follow-ups from chat history ---
    followup_count = 0
    ai_sources: list[str] = []  # Debug: track all AI message sources
    for row in chat_history:
        if row.get("role") == "ai":
            source = (row.get("source") or "").lower()
            ai_sources.append(source)
            if source in _FOLLOWUP_SOURCES:
                followup_count += 1

    # is_smart_followup passed as parameter from the caller (detected from ctx.scheduled_message)

    logger.info(
        "MEDIA FILTER | pre_filter=%d | followup_count=%d | smart=%s | ai_sources=%s",
        len(library), followup_count, is_smart_followup, ai_sources,
    )

    # --- Rule 0 (First follow-up gate): Block ALL media on 1st standard follow-up ---
    if followup_count == 0 and not is_smart_followup:
        logger.info("MEDIA FILTER | Rule 0 BLOCKED: first standard follow-up, no prior follow_up messages")
        return []

    # --- Rule 1 (Spacing): Block ALL media if most recent AI response included media ---
    # AI replies are stored as 1-3 split messages (separate rows). The media
    # marker lives on the first (oldest) row. Check all consecutive AI messages
    # within 10 seconds of the newest one to catch the full split group.
    _media_markers = (
        "[AI sent GIF:", "[AI sent gif:", "[AI sent image:", "[AI sent voice note:",
        "[AI sent media:",
    )
    _first_ai_ts = None
    for row in chat_history:
        if row.get("role") == "ai":
            ts = row.get("timestamp")
            if _first_ai_ts is None:
                _first_ai_ts = ts
            elif ts and _first_ai_ts:
                if abs((_first_ai_ts - ts).total_seconds()) > 10:
                    break  # Different AI response group — stop checking
            content = row.get("content") or ""
            if any(marker in content for marker in _media_markers):
                logger.info("MEDIA FILTER | Rule 1 BLOCKED: most recent AI response had media marker")
                return []
        else:
            break  # Non-AI message — end of most recent AI response group

    # --- Rule 2 (Dedup): Remove items already used in last 20 messages ---
    # Extract media names from granular markers like [AI sent TYPE: NAME - DESC]
    recently_used_names: set[str] = set()
    for row in chat_history[:20]:
        content = row.get("content") or ""
        if not isinstance(content, str):
            continue
        for line in content.split("\n"):
            line = line.strip()
            if not line.startswith("[AI sent "):
                continue
            # Format: [AI sent TYPE: NAME - DESC] or [AI sent TYPE: URL]
            colon_idx = line.find(": ", 9)
            if colon_idx < 0:
                continue
            rest = line[colon_idx + 2:].rstrip("]").strip()
            # Try to extract name (before " - " separator)
            dash_idx = rest.find(" - ")
            if dash_idx > 0:
                name = rest[:dash_idx].strip()
            else:
                name = rest
            if name:
                recently_used_names.add(name.lower())

    filtered = []
    for item in library:
        if isinstance(item, dict):
            item_name = (item.get("name", "") or "").lower()
            if item_name and item_name in recently_used_names:
                continue
        filtered.append(item)

    # Sort alphabetically by name (matches n8n media library ordering)
    filtered.sort(key=lambda x: (x.get("name", "") if isinstance(x, dict) else str(x)).lower())

    return filtered
