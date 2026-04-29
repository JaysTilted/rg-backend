"""Step 3.9: Post-agent processing — follow-up + pipeline management.

Two operations run after the agent responds:
1. Smart Follow-Up — detect/schedule/cancel follow-up timers
2. Pipeline Management — classify and update GHL opportunity stage

Extraction (name/email/phone/qualification) now runs PRE-AGENT at
pipeline.py step 3.7b. Results are stored on ctx and reused here.
All GHL writes are PROD-ONLY (skipped in test mode).
"""

from __future__ import annotations

import asyncio
import copy
import json
import logging
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from app.services.ghl_client import GHLClient

from prefect import task

from app.models import PipelineContext
from app.text_engine.utils import get_custom_field
from app.services.ai_client import classify
from app.text_engine.model_resolver import resolve_model, resolve_temperature
from app.services.supabase_client import supabase

logger = logging.getLogger(__name__)


# =========================================================================
# ORCHESTRATOR
# =========================================================================


@task(name="post_agent_processing", retries=2, retry_delay_seconds=3, timeout_seconds=60)
async def post_agent_processing(ctx: PipelineContext) -> dict[str, Any]:
    """Run post-agent operations: smart follow-up + pipeline management.

    Extraction already ran pre-agent (pipeline.py step 3.7b) — results are
    on ctx.extraction_result, ctx.qualification_status, ctx.qualification_notes.
    """
    ghl = ctx.ghl

    # Extraction already ran pre-agent — use stored results
    extraction = ctx.extraction_result

    # Phase 1: Smart follow-up (must be post-agent — checks agent's commitments)
    followup: dict[str, Any] = {}
    try:
        followup = await _smart_followup(ctx, ghl)
    except Exception as e:
        logger.warning("Follow-up failed: %s", e, exc_info=True)

    # Phase 2: Pipeline management (uses qualification from pre-agent extraction)
    qual_status = ctx.qualification_status
    qual_notes = ctx.qualification_notes

    pipeline_result = {}
    try:
        pipeline_result = await _manage_pipeline(ctx, ghl, qual_status, qual_notes)
    except Exception as e:
        logger.warning("Pipeline management failed: %s", e, exc_info=True)

    # Phase 3: Apply GHL updates (PROD-ONLY)
    if not ctx.is_test_mode:
        await _apply_ghl_updates(ctx, ghl, extraction, followup, pipeline_result)
    else:
        logger.info("TEST MODE — skipping GHL updates")

    results = {
        "extraction": extraction,
        "followup": followup,
        "pipeline": pipeline_result,
    }

    # Detailed post-processing logs
    if extraction:
        logger.info(
            "EXTRACTION | name=%s | email=%s | phone=%s",
            extraction.get("name", "") or "(none)",
            extraction.get("email", "") or "(none)",
            extraction.get("phone", "") or "(none)",
        )
    if followup:
        logger.info(
            "SMART FOLLOWUP | action=%s | timeframe=%s | reason=%s",
            followup.get("action", "none"),
            followup.get("timeframe", "") or "(none)",
            (followup.get("stop_reason") or followup.get("keep_reason") or followup.get("reason", ""))[:150],
        )
    if pipeline_result:
        logger.info(
            "PIPELINE | action=%s | %s → %s | reason=%s",
            pipeline_result.get("action", "none"),
            pipeline_result.get("current_stage", "?"),
            pipeline_result.get("new_stage", "?"),
            pipeline_result.get("reason", "")[:100],
        )

    return results


# =========================================================================
# 1. EXTRACT USER INFORMATION
# =========================================================================

_EXTRACT_SYSTEM_PROMPT = """\
<role>
YOU ARE A HIGHLY ACCURATE AI ANALYST TASKED WITH EXTRACTING USER CONTACT INFORMATION FROM A GIVEN CHAT HISTORY.

## YOUR GOAL

Analyze the chat history and extract contact data ONLY when it adds value. You are deciding WHETHER to update existing data, not blindly extracting everything you see.

**Key principle:** If we already have data for a field, leave it alone unless the lead explicitly CORRECTS it. Repeating the same info is not a correction.
</role>

<existing_data>
## EXISTING CONTACT DATA

You have access to the contact's current data below. Use this to decide what needs updating.
</existing_data>

<extraction_rules>
## EXTRACTION RULES (Apply to ALL fields — name, email, phone, AND custom fields)

**RULE 1 - NEW DATA**
When: We have no data for this field (existing is empty/none) and the lead provides it.
Action: Extract the value.

**RULE 2 - CORRECTION**
When: The lead explicitly corrects existing data (e.g., "Actually my name is Smith, not Pork" or "my budget changed, it's closer to $8,000 now").
Action: Extract the CORRECTED value. For partial corrections, combine intelligently with existing data.

**RULE 3 - SAME OR REPEATED DATA**
When: The lead mentions data that matches or is equivalent to what we already have.
Action: Return "" — no update needed. This is the most common case. If the lead says their name and we already have it, that is NOT a correction — it's just conversation.

**RULE 4 - PARTIAL / DOWNGRADE**
When: You can only extract partial info and we already have fuller info (e.g., existing "John Smith", lead says "I'm John").
Action: Return "" — never replace complete data with partial data.
</extraction_rules>

<examples>
## EXTRACTION EXAMPLES

**Name — New data:**
Existing: "(none)", Lead says: "I'm Sarah"
Extract: "Sarah" ← new data, we had nothing

**Name — Correction:**
Existing: "John Pork", Lead says: "It's actually Smith, not Pork"
Extract: "John Smith" ← correction, combine first name with corrected last name

**Name — Upgrade:**
Existing: "John", Lead says: "Full name is John Smith"
Extract: "John Smith" ← upgrade from partial to fuller info

**Name — Same data (DO NOT extract):**
Existing: "Sarah Johnson", Lead says: "Yeah this is Sarah"
Extract: "" ← we already know her name, she's just confirming — not a correction

**Name — Downgrade (DO NOT extract):**
Existing: "Sarah Johnson", Lead says: "Call me Sarah"
Extract: "" ← don't replace full name with just first name

**Email — New data:**
Existing: "(none)", Lead says: "My email is sarah@gmail.com"
Extract: "sarah@gmail.com" ← new data

**Email — Same data (DO NOT extract):**
Existing: "sarah@gmail.com", Lead says: "You can reach me at sarah@gmail.com"
Extract: "" ← exact same email, no update needed

**Email — Correction:**
Existing: "old@gmail.com", Lead says: "Actually use new@gmail.com instead"
Extract: "new@gmail.com" ← explicit correction

**Phone — New data:**
Existing: "(none)", Lead says: "Call me at 555-987-6543"
Extract: "555-987-6543" ← new data

**Phone — Same data (DO NOT extract):**
Existing: "555-987-6543", Lead says: "My number is 555-987-6543"
Extract: "" ← same number, no update

**Custom field — New data:**
Field: "budget_range", Existing: "(none)", Lead says: "I'm thinking around $5,000"
Extract: "$5,000" ← new data for empty field

**Custom field — Same data (DO NOT extract):**
Field: "budget_range", Existing: "$5,000", Lead says: "Yeah like I said around $5,000"
Extract: "" ← repeating what we already have

**Custom field — Correction:**
Field: "budget_range", Existing: "$5,000", Lead says: "Actually my budget went up, closer to $8,000 now"
Extract: "$8,000" ← explicit correction with new value

**Custom field — New data (timeline):**
Field: "timeline", Existing: "(none)", Lead says: "Looking to get this done next month"
Extract: "next month" ← new data

**Custom field — Correction (timeline):**
Field: "timeline", Existing: "next month", Lead says: "Actually we pushed it to June"
Extract: "June" ← explicit correction
</examples>

<step_by_step>
## STEP-BY-STEP PROCESS

For EACH field (name, email, phone, and any custom fields):
1. Is this data mentioned in the conversation?
2. If not mentioned → return ""
3. If mentioned → compare to existing data for this field
4. Is it NEW data (field was empty)? → Extract it
5. Is it a CORRECTION (lead explicitly changes existing value)? → Extract the corrected value
6. Is it the SAME as existing data? → Return "" (no update)
7. Is it a DOWNGRADE (partial replacing fuller data)? → Return "" (keep existing)
</step_by_step>

<prohibited_actions>
## WHAT NOT TO DO

- DO NOT guess data that was not stated in the conversation
- DO NOT fabricate or infer information
- DO NOT return partial or malformed emails/phones
- DO NOT include the AI agent's own contact info
- DO NOT re-extract data that matches what we already have — return "" instead
- DO NOT downgrade existing data with partial info — return "" instead
- DO NOT treat the lead repeating known info as a "correction" — it's just conversation
</prohibited_actions>
"""

_EXTRACT_SCHEMA = {
    "type": "object",
    "properties": {
        "name": {
            "type": "string",
            "description": "Extracted name (or empty string if no update needed)",
        },
        "email": {
            "type": "string",
            "description": "Extracted email (or empty string if no update needed)",
        },
        "phone": {
            "type": "string",
            "description": "Extracted phone (or empty string if no update needed)",
        },
    },
    "required": ["name", "email", "phone"],
    "additionalProperties": False,
}


async def _extract_user_info(ctx: PipelineContext) -> dict[str, Any]:
    """Extract user contact info (name/email/phone) + custom data collection fields.

    Qualification is handled separately by the dedicated qualification agent
    in qualification.py — this function handles all data extraction.
    """
    # Build existing data display — exclude channel-protected fields entirely
    existing_parts = [f"Name: {ctx.contact_name or '(none)'}"]
    if ctx.channel != "Email":
        existing_parts.append(f"Email: {ctx.contact_email or '(none)'}")
    if ctx.channel != "SMS":
        existing_parts.append(f"Phone: {ctx.contact_phone or '(none)'}")
    existing = "\n".join(existing_parts)

    system = _EXTRACT_SYSTEM_PROMPT

    # --- Custom data collection fields (dynamic extension) ---
    custom_fields = ctx.compiled.get("data_collection_fields", [])
    # Filter out any keys that collide with built-in fields
    custom_fields = [f for f in custom_fields if f.get("key") not in ("name", "email", "phone")]

    if custom_fields:
        existing_ed = (ctx.lead.get("extracted_data") or {}) if ctx.lead else {}

        custom_section = "\n<custom_fields>\n## CUSTOM DATA COLLECTION\n\n"
        custom_section += "In addition to name/email/phone, extract these custom fields.\n"
        custom_section += 'Return "" if data not mentioned, unclear, or matches existing value.\n'
        custom_section += "Apply the same rules as name/email/phone: only extract NEW data or CORRECTIONS.\n\n"
        for f in custom_fields:
            current_val = existing_ed.get(f["key"], "(none)")
            custom_section += f"- **{f['key']}** ({f.get('data_type', 'TEXT')}): {f.get('instructions', '')}\n"
            custom_section += f'  Current value in system: "{current_val}"\n'
        custom_section += "</custom_fields>"
        system += custom_section

    # --- Build dynamic schema ---
    schema = copy.deepcopy(_EXTRACT_SCHEMA)

    # Channel protection at code level — remove field from schema entirely
    if ctx.channel == "SMS":
        schema["properties"].pop("phone", None)
        schema["required"] = [r for r in schema["required"] if r != "phone"]
    elif ctx.channel == "Email":
        schema["properties"].pop("email", None)
        schema["required"] = [r for r in schema["required"] if r != "email"]

    if custom_fields:
        for f in custom_fields:
            schema["properties"][f["key"]] = {
                "type": "string",
                "description": f"{f.get('label', f['key'])} - extract from conversation or return empty string",
            }
            schema["required"].append(f["key"])

    user_prompt = (
        f"{ctx.timeline}\n\n"
        f"**Current Contact Details in System:**\n{existing}\n\n"
        f"Extract user information following the rules above."
    )

    from app.models import log_prompt
    _ex_vars: dict[str, Any] = {
        "existing_contact": existing,
        "reply_channel": ctx.channel,
    }
    if ctx.timeline:
        _ex_vars["timeline"] = ctx.timeline
    if custom_fields:
        _ex_vars["custom_fields"] = [f["key"] for f in custom_fields]
    log_prompt(ctx, "Extraction", system, user_prompt, variables=_ex_vars)

    result = await classify(
        prompt=user_prompt,
        schema=schema,
        system_prompt=system,
        model=resolve_model(ctx, "contact_extraction"),
        temperature=resolve_temperature(ctx, "contact_extraction"),
        label="contact_extraction",
    )

    # Log built-in + custom field results
    custom_extracted = {f["key"]: bool(result.get(f["key"])) for f in custom_fields}
    logger.info(
        "Extraction: name=%s email=%s phone=%s custom=%s",
        bool(result.get("name")),
        bool(result.get("email")),
        bool(result.get("phone")),
        custom_extracted if custom_extracted else "none",
    )
    return result


# =========================================================================
# 2. SMART FOLLOW-UP MANAGEMENT
# =========================================================================


# NOTE: The smart follow-up SCHEDULER has been moved to the follow-up path
# (followup.py → _SMART_SCHEDULER_PROMPT). The reply path now only runs the
# stop-checker below. TIMEFRAME_DAYS moved to followup_scheduling.py.
#
# See followup.py for the new architecture:
# - Determination agent decides: send now / reschedule / done
# - Smart scheduler agent picks timeframe (only when reschedule=true)
_SCHEDULER_REMOVED = True  # Marker for code search — safe to delete later

_STOP_FOLLOWUP_PROMPT = """\
<role>
## STOP SMART FOLLOW-UP CLASSIFIER

A lead is currently in SMART FOLLOW-UP (timer is set). They just replied. Your job: Should the sequence CONTINUE or STOP?
</role>

<how_it_works>
## HOW THIS WORKS

Lead has smart follow-up timer → Lead replies → YOU DECIDE → continue or stop?

If STOP → Timer cleared → Scheduler re-evaluates
If CONTINUE → Timer stays, scheduled follow-up fires as planned

**Important:** If you output "stop_follow_up", the timer is cleared and the lead goes back through the Smart Follow-Up Scheduler. The scheduler will then decide if a NEW timer should be set based on the latest conversation. You don't need to worry about setting the new timing - just decide if the CURRENT timer should stop.

</how_it_works>

<timer_categories>
## WHY LEADS ARE IN SMART FOLLOW-UP

The scheduler uses two types of triggers. Understanding which type matters for your decision:

### Section A — LEAD-SIGNALED (Reactive)
The lead or agent explicitly communicated something about timing, availability, or status. The lead KNOWS they're waiting.

| # | Category | Example | Typical Timer |
|---|----------|---------|---------------|
| 1 | Agent committed to follow-up | "We'll reach back out closer to your date" | `2_weeks` default |
| 2 | Lead stated unavailability | "I'm out of town till the 12th", "I have the flu", "Dealing with a family emergency", "I need to save up" | Calculated |
| 3 | Lead requested specific timing | "Call me back next week" | As requested |
| 4 | Booking window conflict | Wanted date too far out | When date enters window |
| 5 | Lead said they'll initiate | "I'll reach out when ready" | `2_weeks` safety net |
| 6 | Disqualified - competitor/medical/long-term | "I booked with someone else", "I have a medical issue that prevents this" | `4_months` |
| 6b | Recently had service/procedure | "I just had this done last week" | `3_months` |
| 7 | Cancelled with vague reschedule | "Cancel for now, I'll rebook later" | `1_week` |

### Section B — CONTEXT-BASED (Proactive)
The lead did NOT explicitly request delayed follow-up. WE proactively delayed because a 4-hour follow-up would feel pushy or hurt conversion. The lead may not know they're on a timer.

| # | Category | Example | Typical Timer |
|---|----------|---------|---------------|
| 8 | Scheduling constraints blocked | Only available times that don't exist | `3_days` to `1_week` |
| 9 | Lead needs time to research/decide | "I need to check with my husband", "Let me research the technology", "I want to compare prices" | `2_days` to `3_days` |
| 9b | Frequency complaint (engaged lead) | "Stop texting me so much", "Can you stop texting three times a day?" | `3_days` cooldown |

### Why Section A vs B Matters

**Section A (Reactive):** The lead explicitly asked for future contact. Their new message is either confirming the plan (→ continue) or changing it (→ stop).

**Section B (Proactive):** WE decided to delay, not the lead. The bar for STOP is LOWER — any genuine re-engagement signal should stop the timer because we imposed the delay, not them.

</timer_categories>

<decision_outcomes>
## THE DECISION

| Output | What happens |
|--------|------------|
| `continue` | Timer stays active. Scheduled follow-up fires at planned time. |
| `stop_follow_up` | Timer cleared. Lead goes back through scheduler for re-evaluation. |

</decision_outcomes>

<timer_info>
## CURRENT TIMER INFO

The lead's timer is provided as JSON with:
- `value` - timeframe string (e.g., "2_weeks", "4_months")
- `scheduled_at` - when timer was set
- `scheduled_for` - when follow-up will fire

Use `value` to detect timeline CHANGES:
- Different timeframe mentioned → stop (let scheduler reset)
- Same timeframe confirmed → continue
- No timing mentioned → use other signals

</timer_info>

<continue_criteria>
## WHEN TO CONTINUE

The lead's reply confirms the plan or is just a passive acknowledgment.

### Simple Acknowledgments → CONTINUE
- "ok", "thanks", "perfect", "sounds good", "got it"
- "ok thanks", "sounds good thanks"
- Emojis: "👍", "✓", "😊", "🙏"
- Brief confirmations under 25 characters with no questions

### Greetings, Lone Emojis & Non-Committal Words → CONTINUE
These are NOT substantive engagement. The lead isn't changing the plan, asking anything, or signaling readiness:
- Greetings without follow-up: "hey", "hi", "hello", "hey! 👋", "yo", "what's up"
- Lone emojis (no accompanying text with intent): "🎉", "😂", "❤️", "🔥", "😎", "👀"
- Non-committal responses: "maybe", "perhaps", "idk", "we'll see", "not sure yet", "possibly"
- Ambient chatter: "haha", "lol", "nice", "cool"

→ These could be accidental, reactions to something else, or ambient noise. If the lead truly wants something, they'll follow up with substance.
→ A greeting or emoji alone does NOT override a timer — wait for a clear signal (question, request, decision, booking intent).
→ "Maybe" and "idk" indicate the lead is still undecided — the original reason for the timer still applies.

### Timeline Confirmations → CONTINUE
- "See you in 2 weeks" (when scheduled for 2_weeks)
- "Perfect, reach out when it gets closer"
- "Sounds good, talk then"
- "Ok, looking forward to hearing from you"
- "Great, check in around the 12th"

### Light Contact Requests → CONTINUE
- "Stay in touch though"
- "Keep me posted"
- "Check in occasionally"
- "Touch base before then"

These are NOT re-engagement - they're saying "yes to the plan, maybe ping me lightly."

### "But/Though" Adding Minor Request → CONTINUE
- "Sounds good, but keep me posted" (adds request, not a change)
- "Yes, stay in touch though" (minor add-on)
- "Ok, though check in occasionally" (light contact)

### Still In Progress (Section B leads) → CONTINUE
- "Still looking into it" (research in progress)
- "Haven't talked to my husband yet" (consultation pending)
- "Still comparing options" (decision not made)
- "Still saving up" (financial constraint unchanged)
- "Still feeling sick" (illness ongoing)

These confirm the original reason for the timer still applies.

</continue_criteria>

<stop_criteria>
## WHEN TO STOP

The lead is re-engaging NOW or changing the plan.

### Questions (Any "?") → STOP
- "What's the price?"
- "Where are you located?"
- "How does this work?"
- "Can we do sooner?"

### Booking/Ready Signals → STOP
- "I'm ready to book"
- "Let's schedule"
- "I want to make an appointment"
- "When can we do this?"

### Timeline Changes → STOP
- "Actually, let's do sooner"
- "Changed my mind"
- "Nevermind the wait"
- "How about this week instead"
- Different timeframe than scheduled (e.g., scheduled 2_weeks, says "4 days works better")

### Already Spoke to Staff → STOP
- "I already talked to someone at your office"
- "Someone helped me already"
- "I spoke with [name] about this"

→ Trust staff is handling it. Clear the automated follow-up.

### Research/Decision Completed → STOP
If the lead was on timer for research/consultation (Category 9) and now has an answer:
- "My husband said go for it!"
- "I've done my research, let's do it"
- "I compared prices and I want to go with you"
- "I looked into it and I'm interested"
- "Talked to my spouse, we're in"
- "I checked with my insurance, we're covered"

→ They completed what they needed to do. Stop and let them re-engage.

### Financial Constraint Cleared → STOP
If the lead was on timer for financial reasons (Category 2) and is now ready:
- "I got my tax refund, let's book!"
- "I saved up enough"
- "I got approved for financing"
- "Budget looks good now"

### Illness/Crisis Recovery → STOP
If the lead was on timer for illness or personal crisis (Category 2) and is now better:
- "Feeling better now, can we schedule?"
- "I'm recovered, let's do it"
- "Things have settled down"
- "Back on my feet"

### Disqualified Lead Re-engaging → STOP
If they were set to `3_months` or `4_months` for competitor/medical/procedure and now say:
- "Changed my mind, want to book with you"
- "My medical issue is resolved now"
- "The other place didn't work out"
- "I'm available now"
- "I'm due for another session"
- "It's been a while, I'm ready to come back"

→ They're ready NOW. Stop and let scheduler re-evaluate.

### Frequency Complaint Lead Re-engaging → STOP
If lead was on `3_days` cooldown for a frequency complaint (Category 9b) and now sends:
- Any question
- Any booking signal
- Any new topic or engagement

→ They've cooled off and are re-engaging. Stop the cooldown timer.

### Urgency → STOP
- "urgent"
- "asap"
- "right away"
- "need this quickly"

### Stop Requests → STOP
- "Don't follow up"
- "I'll call you instead"
- "Stop texting me"
- "Cancel"

### "But/Though" Introducing Change → STOP
- "Sounds good, but what's the cost?" (question)
- "Ok, though I changed my mind" (change)
- "Yes, but actually can we do sooner?" (timeline change)

### Any Engagement from Section B (Proactive) Leads → STOP
If the lead was placed on a proactive timer (Categories 8, 9, 9b — scheduling blocked, research/decide, frequency complaint) and sends ANY substantive message beyond a simple acknowledgment:

→ We imposed the delay. If they're engaging, honor it. STOP and let the system respond naturally.

</stop_criteria>

<decision_flowchart>
## DECISION FLOWCHART

1. Did lead say they already spoke with staff?
   YES → stop_follow_up

2. Contains "?"
   YES → stop_follow_up

3. Contains timeline change words? ("actually", "sooner", "now", "changed my mind", "nevermind")
   YES → stop_follow_up

4. Mentions DIFFERENT timeframe than scheduled?
   YES → stop_follow_up

5. Contains booking words? ("book", "schedule", "appointment", "ready")
   YES → stop_follow_up

6. Contains pricing/detail words? ("price", "cost", "how much", "what", "where", "explain")
   YES → stop_follow_up

7. Contains stop request? ("don't follow up", "stop", "cancel", "I'll call you")
   YES → stop_follow_up

8. Research/decision completed? ("husband said", "I've researched", "compared", "checked with", "talked to my spouse")
   YES → stop_follow_up

9. Constraint cleared? ("feeling better", "saved up", "budget good", "recovered", "back on my feet")
   YES → stop_follow_up

10. Is the message ONLY a greeting ("hey", "hi"), lone emoji, or non-committal word ("maybe", "idk", "we'll see") with NO question, request, decision, or booking intent?
    YES → continue (not substantive engagement — wait for a clear signal)

11. Is it a Section B (proactive timer) lead sending a substantive message?
    YES → stop_follow_up (we imposed the delay, honor their engagement)

12. Is it a simple acknowledgment? (under 25 chars, no questions)
    YES → continue

13. Is it a timeline confirmation? (same timeframe as scheduled)
    YES → continue

14. Is it a light contact request? ("stay in touch", "keep me posted")
    YES → continue

15. Is it a "still in progress" update? ("still researching", "haven't decided yet")
    YES → continue

16. "But/though" context?
    + minor request → continue
    + question or change → stop_follow_up

17. Uncertain?
    → continue (don't ghost the lead)

</decision_flowchart>

<default_behavior>
## WHY DEFAULT TO CONTINUE

| If you... | Result |
|-----------|--------|
| STOP when should CONTINUE | Timer cleared, scheduler might set same timer again. Wasteful but not harmful. |
| CONTINUE when should STOP | Lead gets scheduled follow-up they didn't want. Could annoy them. |

**But consider:** The lead ASKED for future follow-up originally (Section A), or we proactively delayed for their benefit (Section B). If their new message is ambiguous, they probably still want the plan. Better to send one scheduled message than ghost them.

**Exception for Section B:** If a proactive timer lead sends anything substantive, lean toward STOP — we imposed the delay, so be responsive to their engagement.

**Default when uncertain:** CONTINUE

</default_behavior>

<rules>
## CRITICAL RULES

1. **Any "?" = stop** - Questions mean they're engaging now
2. **"Already spoke to staff" = stop** - Trust staff is handling it
3. **Different timeframe = stop** - Let scheduler reset with new timing
4. **Simple acknowledgments = continue** - They're confirming the plan
5. **Light contact requests = continue** - "Keep me posted" ≠ re-engagement
6. **Disqualified re-engaging = stop** - They're ready now (competitor, medical, or procedure)
7. **Research/decision completed = stop** - They finished what they needed to do
8. **Constraint cleared = stop** - Financial, illness, or crisis resolved
9. **Section B (proactive) leads re-engaging = stop** - We imposed the delay, honor their engagement
10. **"Still in progress" updates = continue** - Original reason still applies
11. **Focus on MOST RECENT message** - Earlier messages already processed
12. **When uncertain = continue** - Don't ghost the lead
13. **Substance required for STOP** - Greetings alone ("hey"), lone emojis, or non-committal words ("maybe") are NOT re-engagement. Need a question, request, decision, or booking intent to trigger STOP
</rules>

<examples>
## EXAMPLES

### CONTINUE

| Lead says | Timer | Category | Reason |
|-----------|-------|----------|--------|
| "ok" | any | any | Simple acknowledgment |
| "sounds good thanks" | any | any | Simple acknowledgment |
| "👍" | any | any | Emoji acknowledgment |
| "See you in 2 weeks" | 2_weeks | 1-7 | Confirms same timeframe |
| "Perfect, reach out around then" | any | 1-7 | Timeline confirmation |
| "Yes, stay in touch though" | any | any | Light contact request |
| "Sounds good, keep me posted" | any | any | Light contact request |
| "Ok, check in occasionally" | any | any | Light contact request |
| "Will do, talk then" | any | any | Confirms future contact |
| "Still looking into it" | 3_days | 9 | Research still in progress |
| "Haven't talked to my husband yet" | 2_days | 9 | Consultation still pending |
| "Still saving up" | 1_month | 2 | Financial constraint unchanged |
| "Still feeling under the weather" | 1_week | 2 | Illness ongoing |
| "Hey! 👋" | 2_weeks | 5 | Greeting alone — not substantive engagement |
| "🎉" | 4_months | 6 | Lone emoji — no question, request, or intent |
| "maybe" | 2_weeks | 5 | Non-committal — still undecided, timer stays |
| "haha" | 1_week | any | Ambient chatter — not re-engagement |

### STOP

| Lead says | Timer | Category | Reason |
|-----------|-------|----------|--------|
| "What's the price?" | any | any | Question |
| "I'm ready to book" | any | any | Booking intent |
| "Actually let's do sooner" | any | any | Timeline change |
| "4 days works better" | 2_weeks | 1-7 | Different timeframe |
| "I already spoke to someone" | any | any | Staff handling it |
| "Changed my mind, I want to book with you" | 4_months | 6 | Disqualified re-engaging |
| "My medical issue is resolved now" | 4_months | 6 | Medical cleared |
| "The other place didn't work out" | 4_months | 6 | Competitor didn't work |
| "I'm due for another session" | 3_months | 6b | Returning for repeat service |
| "Don't bother following up" | any | any | Stop request |
| "Where are you located?" | any | any | Question |
| "Sounds good, but what's the cost?" | any | any | "But" + question |
| "My husband said go for it!" | 2_days | 9 | Research/consultation done |
| "I've done my research, let's book" | 3_days | 9 | Research completed |
| "I compared prices, going with you" | 3_days | 9 | Decision made |
| "Talked to my spouse, we're in" | 2_days | 9 | Spousal approval received |
| "I got my tax refund!" | 1_month | 2 | Financial constraint cleared |
| "Feeling better, can we schedule?" | 1_week | 2 | Illness recovered |
| "Things have settled down now" | 2_months | 2 | Crisis passed |
| "Can I get more info on the treatment?" | 3_days | 9b | Frequency complaint lead re-engaging |
</examples>

<output_format>
## OUTPUT FORMAT

Valid JSON only:

{{"path": "continue", "reason": "Simple acknowledgment, timer stays active"}}
{{"path": "stop_follow_up", "reason": "Lead asked a question, clearing timer for scheduler re-evaluation"}}

Valid path values: "continue", "stop_follow_up"

Current timer: {timer_info}
</output_format>
"""

_STOP_FOLLOWUP_SCHEMA = {
    "type": "object",
    "properties": {
        "path": {"type": "string", "enum": ["continue", "stop_follow_up"]},
        "reason": {"type": "string"},
    },
    "required": ["path", "reason"],
    "additionalProperties": False,
}

async def _smart_followup(
    ctx: PipelineContext, ghl: GHLClient
) -> dict[str, Any]:
    """Check existing smart follow-up timer — stop or keep. No scheduling.

    Scheduling has moved to the follow-up path (followup.py). The reply path
    now only checks whether an EXISTING timer should be stopped when the lead
    replies, or kept active.

    MIGRATED: Now reads from scheduled_messages DB table instead of GHL tags/fields.
    """
    result: dict[str, Any] = {"action": "none"}

    # Check for pending smart follow-up in DB (replaces GHL tag + custom field check)
    pending_smart_fus: list[dict] = []
    try:
        from app.services.message_scheduler import get_pending_for_contact
        from app.main import supabase
        pending_smart_fus = await get_pending_for_contact(
            supabase, ctx.contact_id, ctx.entity_id, message_type="smart_followup"
        )
    except Exception as e:
        logger.warning("SMART_FOLLOWUP | DB query failed: %s", e)

    has_timer = len(pending_smart_fus) > 0
    timer_metadata = pending_smart_fus[0].get("metadata", {}) if pending_smart_fus else {}

    logger.info("SMART_FOLLOWUP | has_timer=%s | has_booking=%s", has_timer, ctx.has_upcoming_booking)

    if has_timer:
        # Build timer display string from DB metadata (same format the LLM expects)
        timer_display = (
            f"{timer_metadata.get('timeframe', 'unknown')} "
            f"(due: {timer_metadata.get('due', 'unknown')}) | "
            f"Reason: {timer_metadata.get('reason', 'N/A')}"
        )

        # Auto-stop if lead has confirmed booking
        if ctx.has_upcoming_booking:
            result["action"] = "stopped_existing"
            result["stop_reason"] = "Lead has confirmed upcoming appointment"
            result["timer_stopped"] = True
            return result

        # Ask LLM whether to stop the timer
        stop_result = await _check_stop_followup(ctx, timer_display)
        if stop_result.get("path") == "stop_follow_up":
            result["action"] = "stopped_existing"
            result["stop_reason"] = stop_result.get("reason", "")
            result["timer_stopped"] = True
        else:
            result["action"] = "kept_existing"
            result["keep_reason"] = stop_result.get("reason", "")
    else:
        result["action"] = "none"

    logger.info("Smart follow-up: %s", result.get("action"))
    return result


async def _check_stop_followup(
    ctx: PipelineContext, timer_display: str
) -> dict[str, Any]:
    """Check if existing smart follow-up timer should be stopped."""
    system = _STOP_FOLLOWUP_PROMPT.format(timer_info=timer_display)
    _fu_user = f"{ctx.timeline}\n\n---\n\nScheduled timeframe: {timer_display}"

    from app.models import log_prompt
    _sfc_vars: dict[str, Any] = {
        "timer_info": timer_display,
    }
    if ctx.timeline:
        _sfc_vars["timeline"] = ctx.timeline
    log_prompt(ctx, "Stop Follow-Up Checker", system, _fu_user, variables=_sfc_vars)

    # Match n8n: send full timeline (no truncation), lead's message is already
    # in the timeline but we highlight it for the classifier
    result = await classify(
        prompt=_fu_user,
        schema=_STOP_FOLLOWUP_SCHEMA,
        system_prompt=system,
        model=resolve_model(ctx, "stop_detection"),
        temperature=resolve_temperature(ctx, "stop_detection"),
        label="stop_followup",
    )
    logger.info("STOP_FOLLOWUP | path=%s | reason=%s", result.get("path"), (result.get("reason", ""))[:100])
    return result


# =========================================================================
# 3. PIPELINE MANAGEMENT
# =========================================================================

# ── Slim negative-signal LLM prompt (only call for not_interested / wrong_number) ──

_SLIM_NEGATIVE_PROMPT = """\
Analyze the lead's latest message in the context of the conversation.

Is this lead:
1. Clearly and definitively NOT INTERESTED — explicit, final refusal of the service
2. Saying this is the WRONG NUMBER or wrong person

"Not interested" means the lead explicitly and definitively refused. This is NOT:
- Hesitation ("maybe later", "I need to think about it")
- Objections ("too expensive", "bad timing")
- Being busy ("I'm at work", "call me later")
- Cooling off or going silent
- Asking questions (even skeptical ones)

Only classify as not_interested if the lead clearly, permanently refuses.

Output: not_interested, wrong_number, or no_change.
"""

_SLIM_NEGATIVE_SCHEMA = {
    "type": "object",
    "properties": {
        "path": {
            "type": "string",
            "enum": ["not_interested", "wrong_number", "no_change"],
            "description": "Lead disposition or no_change",
        },
        "reason": {
            "type": "string",
            "description": "Brief explanation",
        },
    },
    "required": ["path", "reason"],
    "additionalProperties": False,
}

# ── Negative keyword gate — skip slim LLM if message has no negative signals ──

_NEGATIVE_KEYWORDS = frozenset({
    "not interested", "stop", "unsubscribe", "wrong number", "wrong person",
    "don't text", "dont text", "remove me", "take me off", "no thanks",
    "not the right", "who is this", "never signed up", "leave me alone",
    "stop texting", "stop messaging", "don't contact", "dont contact",
    "no soliciting", "do not contact",
})


def _has_negative_signals(ctx: PipelineContext) -> bool:
    """Gate: only call slim LLM if latest message contains negative keywords."""
    msg = (ctx.message or "").lower()
    return any(kw in msg for kw in _NEGATIVE_KEYWORDS)


# ── Deterministic progression logic ──

_PROGRESSION = ["contacted", "engaged", "qualified", "appointment_requested"]


_TERMINAL_NEGATIVE_STAGES = {"not_interested", "wrong_number", "opt_out", "not_qualified"}


def _deterministic_progression(
    ctx: PipelineContext,
    current_stage: str,
    qual_status: str,
) -> tuple[str, str]:
    """Determine highest applicable progression stage. Returns (stage, reason)."""
    # Audit-fix 2026-04-29: don't auto-promote out of negative terminal stages.
    # Exterior Illuminations bug: lead at not_interested said "Now it's fine"
    # (no negative keyword match), fell through to default "engaged" promotion.
    # _PROGRESSION's backward-guard only protects forward stages, not terminals.
    # If the lead is currently in a negative terminal, requires an explicit DNC
    # tag check upstream (replied-not-interested, etc.) before we'd consider
    # reactivating — and that's the auto-reengage path in ensure_opportunity,
    # not this default progression.
    if current_stage in _TERMINAL_NEGATIVE_STAGES:
        return "no_change", f"At terminal negative stage {current_stage}; reply doesn't auto-reactivate"

    has_qual = bool((ctx.compiled.get("_matched_setter") or {}).get("services"))
    slots_called = any(
        t.get("name") == "get_available_slots" for t in (ctx.tool_calls_log or [])
    )

    # Priority 5: appointment_requested — get_available_slots was called
    if slots_called:
        if has_qual and qual_status != "qualified":
            target = "engaged"
            reason = "Booking tool called but not yet qualified"
        else:
            target = "appointment_requested"
            reason = "get_available_slots tool was called"
    # Priority 6: qualified
    elif has_qual and qual_status == "qualified":
        target = "qualified"
        reason = "Qualification criteria met"
    # Priority 7: engaged — default for any reply
    else:
        target = "engaged"
        reason = "Lead replied"

    # Backward movement guard
    if current_stage in _PROGRESSION and target in _PROGRESSION:
        if _PROGRESSION.index(target) <= _PROGRESSION.index(current_stage):
            return "no_change", f"Already at {current_stage} (>= {target})"

    if current_stage == target:
        return "no_change", f"Already at {target}"

    return target, reason


_VALID_STAGES = {
    "contacted", "engaged", "qualified", "appointment_requested", "booked",
    "not_qualified", "not_interested", "wrong_number", "opt_out", "no_change",
}


async def _manage_pipeline(
    ctx: PipelineContext,
    ghl: GHLClient,
    qual_status: str,
    qual_notes: str,
) -> dict[str, Any]:
    """Deterministic pipeline stage classification with slim LLM for negatives.

    Priority chain (first match wins):
    1. booked       — has_upcoming_booking (safety net)
    2. not_qualified — qualification agent said disqualified
    3. not_interested / wrong_number — slim LLM (gated by keyword check)
    4. appointment_requested — get_available_slots tool was called
    5. qualified    — qualification agent said qualified
    6. engaged      — default for any reply (promotes from contacted)

    In PROD mode, ensure_opportunity() already ran and populated ctx fields.
    In TEST mode, falls back to fetching pipelines + searching opportunities.
    """
    result: dict[str, Any] = {"action": "none"}

    # Use ctx fields if ensure_opportunity already ran, otherwise fetch fresh
    if ctx.pipeline_id and ctx.opportunity_id:
        # PROD path — reuse pre-populated fields
        pipeline_id = ctx.pipeline_id
        name_to_id = ctx.pipeline_name_to_id
        opp_id = ctx.opportunity_id
        current_stage = ctx.current_pipeline_stage
    else:
        # TEST mode fallback — full pipeline fetch + opportunity search
        from app.text_engine.pipeline import _SOURCE_TO_PIPELINE

        pipelines = await ghl.get_pipelines()
        if not pipelines:
            result["error"] = "No pipelines found"
            return result

        lead_source = (ctx.lead.get("source") or "") if ctx.lead else ""
        pipeline_name = _SOURCE_TO_PIPELINE.get(lead_source, "AI Setter")
        pipeline = next((p for p in pipelines if p.get("name") == pipeline_name), pipelines[0])

        pipeline_id = pipeline["id"]
        name_to_id: dict[str, str] = {}
        id_to_name: dict[str, str] = {}
        for stage in pipeline.get("stages", []):
            key = stage["name"].lower().replace(" ", "_")
            name_to_id[key] = stage["id"]
            id_to_name[stage["id"]] = key

        opps = await ghl.search_opportunities(ctx.contact_id, pipeline_id)
        opp = opps[0] if opps else None

        if opp:
            opp_id = opp.get("id", "")
            current_stage_id = opp.get("pipelineStageId", "")
            current_stage = id_to_name.get(current_stage_id, "contacted")
        else:
            opp_id = ""
            current_stage = "contacted"

    result["pipeline_id"] = pipeline_id
    result["opportunity_id"] = opp_id
    result["current_stage"] = current_stage

    has_qual = bool((ctx.compiled.get("_matched_setter") or {}).get("services"))

    logger.info(
        "PIPELINE_CLASSIFY | current_stage=%s | qual=%s | has_qual=%s | opp=%s",
        current_stage, qual_status, has_qual, bool(opp_id),
    )

    # Terminal — already booked
    if current_stage == "booked":
        result["action"] = "no_change"
        result["reason"] = "Already booked"
        return result

    # --- Priority 1: booked (safety net — ensure_opportunity handles this too) ---
    if ctx.has_upcoming_booking:
        new_stage = "booked"
        reason = "Has upcoming booking"

    # --- Priority 2: not_qualified (deterministic from qualification agent) ---
    elif has_qual and qual_status == "disqualified":
        new_stage = "not_qualified"
        reason = "Qualification criteria not met"

    # --- Priority 3-4: not_interested / wrong_number (slim LLM, gated) ---
    elif _has_negative_signals(ctx):
        slim_result = await _slim_negative_check(ctx)
        slim_stage = slim_result.get("path", "no_change")
        if slim_stage in ("not_interested", "wrong_number"):
            new_stage = slim_stage
            reason = slim_result.get("reason", slim_stage)
        else:
            new_stage, reason = _deterministic_progression(ctx, current_stage, qual_status)

    # --- Priority 5-7: deterministic progression ---
    else:
        new_stage, reason = _deterministic_progression(ctx, current_stage, qual_status)

    # Validate
    if new_stage not in _VALID_STAGES:
        result["action"] = "no_change"
        result["reason"] = f"Invalid stage: {new_stage}"
        return result

    if new_stage == "no_change" or new_stage == current_stage:
        result["action"] = "no_change"
        result["reason"] = reason
        return result

    # Backward movement guard (progression stages are forward-only)
    if current_stage in _PROGRESSION and new_stage in _PROGRESSION:
        if _PROGRESSION.index(new_stage) <= _PROGRESSION.index(current_stage):
            result["action"] = "no_change"
            result["reason"] = f"Backward movement blocked: {current_stage} → {new_stage}"
            return result

    result["new_stage"] = new_stage
    result["stage_id"] = name_to_id.get(new_stage, "")
    result["action"] = "update"
    result["reason"] = reason

    logger.info("Pipeline: %s → %s (%s)", current_stage, new_stage, reason[:80])
    return result


async def _slim_negative_check(ctx: PipelineContext) -> dict[str, Any]:
    """Slim LLM call — only checks for not_interested or wrong_number."""
    from app.models import log_prompt

    # Use last ~500 chars of timeline for context, plus the latest message
    recent_context = ctx.timeline[-500:] if ctx.timeline else "(no history)"
    user_prompt = (
        f"Lead's latest message: {ctx.message}\n\n"
        f"Recent conversation context:\n{recent_context}"
    )

    log_prompt(ctx, "Pipeline Slim Negative", _SLIM_NEGATIVE_PROMPT, user_prompt, variables={
        "message": ctx.message,
        "current_stage": ctx.current_pipeline_stage,
    })

    result = await classify(
        prompt=user_prompt,
        schema=_SLIM_NEGATIVE_SCHEMA,
        system_prompt=_SLIM_NEGATIVE_PROMPT,
        model=resolve_model(ctx, "pipeline_management"),
        temperature=resolve_temperature(ctx, "pipeline_management"),
        label="pipeline_management",
    )
    logger.info("PIPELINE_SLIM | result=%s | reason=%s", result.get("path"), result.get("reason", "")[:80])
    return result


# =========================================================================
# GHL UPDATES (PROD-ONLY)
# =========================================================================


async def _apply_ghl_updates(
    ctx: PipelineContext,
    ghl: GHLClient,
    extraction: dict[str, Any],
    followup: dict[str, Any],
    pipeline_result: dict[str, Any],
) -> None:
    """Apply all GHL updates. Only called in PROD mode."""

    # 1. Update contact + qualification from extraction
    if extraction:
        has_new_contact_info = any(
            extraction.get(k) for k in ("name", "email", "phone")
        )

        # --- Contact basic fields (name, email, phone) ---
        if has_new_contact_info:
            contact_update: dict[str, Any] = {}
            if extraction.get("name"):
                parts = extraction["name"].strip().split(None, 1)
                contact_update["firstName"] = parts[0]
                if len(parts) > 1:
                    contact_update["lastName"] = parts[1]
            if extraction.get("email"):
                contact_update["email"] = extraction["email"]
            if extraction.get("phone"):
                contact_update["phone"] = extraction["phone"]

            try:
                await ghl.update_contact(ctx.contact_id, contact_update)
                logger.info("Updated contact fields: %s", list(contact_update.keys()))
            except Exception as e:
                logger.warning("Contact update failed: %s", e)

        # --- Custom data collection fields → GHL fields ---
        custom_fields = ctx.compiled.get("data_collection_fields", [])
        # Filter collisions with built-in keys
        custom_fields = [f for f in custom_fields if f.get("key") not in ("name", "email", "phone")]
        if custom_fields:
            # Standard GHL contact properties (city, state, companyName, etc.)
            _CONTACT_PROPS = frozenset({
                "city", "state", "country", "postalCode", "address1",
                "companyName", "website", "dateOfBirth", "timezone", "source",
            })
            ghl_contact_props: dict[str, str] = {}
            ghl_custom_writes = []
            for field in custom_fields:
                val = extraction.get(field["key"], "")
                ghl_key = field.get("ghl_field_key")
                ghl_id = field.get("ghl_field_id")
                if val and (ghl_key or ghl_id):
                    if ghl_key in _CONTACT_PROPS:
                        ghl_contact_props[ghl_key] = val
                    else:
                        # Use field ID for custom field writes (key-based writes silently fail)
                        write_key = ghl_id or ghl_key
                        ghl_custom_writes.append({
                            "id" if ghl_id else "key": write_key,
                            "field_value": val,
                        })
            # Write contact properties (top-level update)
            if ghl_contact_props:
                try:
                    await ghl.update_contact(ctx.contact_id, ghl_contact_props)
                    logger.info("Updated GHL contact props: %s", list(ghl_contact_props.keys()))
                except Exception as e:
                    logger.warning("GHL contact prop update failed: %s", e)
            # Write custom fields
            if ghl_custom_writes:
                try:
                    await ghl.update_contact(ctx.contact_id, {"customFields": ghl_custom_writes})
                    logger.info("Updated GHL custom fields: %s", [f["key"] for f in ghl_custom_writes])
                except Exception as e:
                    logger.warning("GHL custom field update failed: %s", e)

        # --- Qualification: read from ctx (set by dedicated qual agent), write if changed ---
        from app.text_engine.qualification import format_qual_for_ghl

        new_qual = ctx.qualification_status
        old_qual = (ctx.lead.get("qualification_status", "") or "") if ctx.lead else ""
        qual_changed = new_qual and new_qual != old_qual

        if qual_changed:
            ghl_notes = format_qual_for_ghl(ctx.qualification_notes)
            # Update GHL contact custom fields
            try:
                await ghl.update_contact(ctx.contact_id, {
                    "customFields": [
                        {"key": "qualification_status", "field_value": new_qual},
                        {"key": "qualification_notes", "field_value": ghl_notes},
                    ]
                })
                logger.info("Updated contact qualification: %s -> %s", old_qual or "(none)", new_qual)
            except Exception as e:
                logger.warning("Contact qualification update failed: %s", e)

            # --- Qualification tags: add from setter config based on new status ---
            _qual_tag_key = "qualified" if new_qual == "qualified" else (
                "disqualified" if new_qual == "disqualified" else None
            )
            if _qual_tag_key:
                _qual_tags = []
                try:
                    sc = ctx.config.get("system_config", {}) if ctx.config else {}
                    setter_key = getattr(ctx, "agent_type", None) or "setter_1"
                    setter = sc.get("setters", {}).get(setter_key, {})
                    configured = setter.get("tags", {}).get(_qual_tag_key)
                    if isinstance(configured, list):
                        _qual_tags = configured
                except Exception:
                    pass
                for _qt in _qual_tags:
                    try:
                        await ghl.add_tag(ctx.contact_id, _qt)
                        logger.info("Added %s tag '%s' for contact", _qual_tag_key, _qt)
                    except Exception as _qt_err:
                        logger.warning("Failed to add %s tag '%s': %s", _qual_tag_key, _qt, _qt_err)

        # --- Supabase lead update ---
        if ctx.lead and ctx.lead.get("id"):
            lead_updates: dict[str, Any] = {}
            if extraction.get("name"):
                lead_updates["contact_name"] = extraction["name"]
            if extraction.get("email"):
                lead_updates["contact_email"] = extraction["email"]
            if extraction.get("phone"):
                lead_updates["contact_phone"] = extraction["phone"]
            if qual_changed:
                lead_updates["qualification_status"] = new_qual
                lead_updates["qualification_notes"] = ctx.qualification_notes  # JSONB dict

            # --- Merge extracted_data (built-in + custom fields) ---
            extracted_updates: dict[str, Any] = {}
            for key in ("name", "email", "phone"):
                if extraction.get(key):
                    extracted_updates[key] = extraction[key]
            for field in custom_fields:
                val = extraction.get(field["key"], "")
                if val:
                    extracted_updates[field["key"]] = val
            if extracted_updates:
                existing_ed = ctx.lead.get("extracted_data") or {}
                lead_updates["extracted_data"] = {**existing_ed, **extracted_updates}

            if lead_updates:
                try:
                    await supabase.update_lead(ctx.lead["id"], lead_updates)
                    logger.info("Updated Supabase lead: %s", list(lead_updates.keys()))
                except Exception as e:
                    logger.warning("Supabase lead update failed: %s", e)

    # 2. Smart follow-up updates
    action = followup.get("action", "none")

    # Cancel smart follow-up in DB when LLM says to stop
    if followup.get("timer_stopped"):
        try:
            from app.services.message_scheduler import cancel_pending, update_ghl_followup_field
            from app.main import supabase as sb
            await cancel_pending(
                sb, ctx.contact_id, ctx.entity_id,
                message_types=["smart_followup"],
                reason="stopped_by_reply",
            )
            # Clear GHL display field
            await update_ghl_followup_field(ghl, ctx.contact_id)
            logger.info("Cancelled smart follow-up (stop confirmed by LLM)")
        except Exception as e:
            logger.warning("Cancel smart follow-up failed: %s", e)

    # 3. Pipeline stage update
    if pipeline_result.get("action") in ("update", "auto_booked", "auto_reengaged"):
        opp_id = pipeline_result.get("opportunity_id", "")
        stage_id = pipeline_result.get("stage_id", "")
        if opp_id and stage_id:
            try:
                await ghl.update_opportunity(opp_id, {"pipelineStageId": stage_id})
                logger.info(
                    "Updated pipeline: %s → %s",
                    pipeline_result.get("current_stage"),
                    pipeline_result.get("new_stage"),
                )
            except Exception as e:
                logger.warning("Pipeline update failed: %s", e)
