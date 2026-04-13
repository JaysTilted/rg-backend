"""Manual Message Learning — daily knowledge gap detection from staff activity.

Runs once daily (9:00 AM America/Chicago). For every active client:
1. Collects manual (staff-sent) messages from the last 48 hours (overlapping window for reliability)
2. Collects call logs with transcripts from the last 48 hours
3. Runs an AI agent that analyzes the data, searches the knowledge base as needed,
   and compares findings against the client's current prompts
4. Posts actionable findings to client Slack channel

The agent has access to the knowledge_base_search tool and decides what to search,
how many results to fetch, and when to do follow-up searches.

Triggered by: internal scheduler (run_learning_loop) or manual POST /workflows/manual-message-learning
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Any

from prefect import flow

from app.config import settings
from app.services.slack import create_slack_channel, invite_to_channel, notify_error, post_slack_message
from app.services.supabase_client import supabase
from app.services.postgres_client import postgres
from app.text_engine.data_loading import _build_prompts
from app.tools.knowledge_base_search import knowledge_base_search

logger = logging.getLogger(__name__)

# Schedule: 9:00 AM America/Chicago daily
SCHEDULE_HOUR = 9
SCHEDULE_MINUTE = 0
SCHEDULE_TZ = ZoneInfo("America/Chicago")
RANDY_SLACK_USER_ID = "U089A7K9DTL"


# ============================================================================
# AGENT SYSTEM PROMPT
# ============================================================================

LEARNING_AGENT_SYSTEM_PROMPT = """\
You are a knowledge gap analyst. You review staff activity — manual messages and \
call transcripts — from a business, then verify each factual claim against the \
business's knowledge base and prompts to find what's missing or wrong.

<role>
The business uses an AI sales rep that impersonates a human via SMS. The AI needs \
complete, accurate knowledge to respond correctly. When staff manually send messages \
or handle phone calls, they often share information the AI doesn't have — pricing, \
policies, services, procedures, aftercare instructions, etc. Your job is to find \
those gaps so they can be closed.
</role>

<ai_scope>
The AI sales rep's capabilities are LIMITED to:
- Answering questions about services, pricing, policies, aftercare, and the business
- Qualifying leads and handling objections via text conversation
- Booking appointments conversationally (checking availability, proposing times, booking \
when the lead agrees) — this is the primary booking method
- Sending booking links or payment links depending on client setup (some links require \
payment before booking, others allow direct booking)
- Sending follow-up messages to re-engage leads

The AI DOES NOT and CANNOT:
- Process actual payments, refunds, or billing adjustments via text — the AI never \
directly handles money. It can only send links where the lead completes payment themselves.
- Hold, reserve, or release appointment slots manually
- Handle billing disputes or account-level financial actions (credits, chargebacks, etc.)

RELEVANCE FILTER — apply this to EVERY potential finding before including it:
Ask: "If a lead asked the AI about this topic, would the AI give a wrong or incomplete \
answer without this information?" If the answer is no — because it is an internal \
operational detail, a staff-only process, or something the AI would never discuss with \
a lead — do NOT flag it.

SCHEDULING LOGISTICS — use this distinction:
- FIXED POLICY → flag it (e.g., "PDO threads require a consultation before treatment", \
"appointments must be booked 48 hours in advance", "this service can't be done same day")
- VARIABLE/SITUATIONAL → skip it (e.g., "could be next day or 3 days later depending \
on scheduling", "she has a 2pm opening", "that books out about a week right now")
The test: is this ALWAYS true regardless of the day, or does it depend on current \
availability/staffing? If it depends, skip it.

Examples of what to SKIP (AI would never need this):
- "Deposit can be paid over the phone or via invoice" → staff-only payment logistics
- "We can hold an appointment for 24 hours without a deposit" → internal slot management
- "The $75 was refunded" → one-off billing action
- "That service books out 1-3 days" → variable scheduling, not a fixed policy
- "I'll send you an invoice" → staff-only action
- "Let me check the schedule" → internal process, not knowledge

Examples of what to FLAG (AI would give wrong/missing answers):
- "PDO threads can't be done same day as consultation" → fixed policy a lead would ask about
- "Botox is $12 per unit" → pricing a lead would ask about
- "Consultation required even if you've had threads before" → fixed policy that prevents \
the AI from incorrectly waiving the requirement
- "Results last 6 months" → outcome info a lead would ask about
</ai_scope>

<workflow>
1. READ all staff messages and call transcripts carefully — extract EVERY specific \
   fact (see <what_to_extract>). Do NOT summarize or generalize. List each fact.
2. SEARCH the knowledge base for each topic using the knowledge_base_search tool
3. COMPARE search results + the AI's current prompts against the facts you found
4. REPORT each missing/wrong fact as its own finding in structured JSON
</workflow>

<what_to_extract>
Extract EVERY concrete, specific fact. Each bullet below is a separate category — \
a single conversation may contain dozens of facts across multiple categories. \
Capture ALL of them, not just the main topic.

PRICING (each is a separate fact):
- Per-unit costs, per-session costs, package pricing, bundle pricing
- Discounts, promotions, new client offers, seasonal deals
- Deposit amounts and payment deadlines

FINANCING (each term is a separate fact):
- Which financing providers are accepted (e.g., CareCredit, Affirm, Klarna, GreenSky, etc.)
- Specific APR rates, interest terms, and repayment periods for each provider
- In-house payment plan details (minimum amount, deposit %, payment schedule)
- Application process details (how to apply, approval time, where to apply)
- Loyalty programs and rewards apps (names, how they work, what they earn)

POLICIES:
- Cancellation windows, refund rules, rescheduling rules
- Age limits, ID requirements, consent requirements
- Consultation requirements (who needs one, how long, cost)
- Notice periods (e.g., "7 days notice before cancellation")

SERVICES:
- What services/products are offered and their descriptions
- Combinations allowed (e.g., "can add paint correction to a ceramic coating appointment", \
  "roof inspection includes gutter check at no extra cost", "bundle grooming + nail trim")
- Add-ons and their pricing
- Comparison details between similar services (speed, longevity, coverage area, quality, etc.)

POST-SERVICE CARE (EACH instruction is its own fact — never batch these):
- Product/usage restrictions with timeframes (e.g., "no washing for 48 hours", \
  "avoid direct sunlight for 72 hours", "don't mow new sod for 2 weeks")
- Activity restrictions with timeframes
- Recommended products (brand names and duration of use)
- Expected side effects or normal conditions with duration (e.g., "redness for 2-3 days", \
  "odor fades within 24 hours", "settling cracks are normal for 30 days")
- Care tips (e.g., "apply supplied ointment twice daily", "water new turf daily for 2 weeks")

SERVICE PROTOCOLS (ALWAYS separate from pricing — these are their own findings):
- Recommended number of sessions/visits/phases (e.g., "3-session series", "2-coat process", \
  "4-visit training program")
- Spacing between sessions/visits (e.g., "4-6 weeks apart", "allow 48 hours between coats") \
  — ALWAYS its own finding
- Maintenance schedule after initial service (e.g., "every 6-12 months", "annual re-seal \
  recommended", "quarterly pest treatment") — ALWAYS its own finding
- Self-care or self-administration instructions (e.g., "apply product at home daily", \
  "run system for 30 min weekly")
Even when session count, spacing, and maintenance appear in the same sentence as pricing, \
extract each protocol detail as a SEPARATE finding from the price.

RESULTS AND OUTCOMES:
- Expected results with timeframes (e.g., "see full results in 4-6 weeks", \
  "energy savings of 20-30% within first billing cycle", "coat lasts 2-5 years")
- Time until results are noticeable
- How long results last

HEALTH, SAFETY AND ELIGIBILITY:
- Who should NOT use a service/product and why (e.g., "not safe for pregnant women", \
  "not recommended for homes built before 1978 without lead test", "dogs under 6 months \
  cannot be groomed")
- Side effects or risks with management instructions
- Substance or activity restrictions with timeframes (e.g., "no alcohol for 24 hours", \
  "keep pets off treated lawn for 48 hours", "no heavy lifting for 72 hours")
- Eligibility or prerequisite requirements

CONTACT AND LOGISTICS:
- Phone numbers, emails, addresses, websites
- Hours, availability windows
- How to book (online, phone, app)
</what_to_extract>

<what_to_ignore>
Skip anything that adds no factual knowledge or is not relevant to the AI:
- Pleasantries ("See you tomorrow!", "Sounds good!", "You're welcome")
- Scheduling confirmations with no factual content
- Questions without answers ("What time works?")
- Emotional responses ("So excited for your appointment!")
- Messages with only a name, date, or time
- One-off financial transactions (refunds, credits, adjustments for specific clients)
- Internal staff-to-client logistics that don't represent fixed policies \
(e.g., "I'll email you an invoice", "let me check the schedule", "I'll hold \
that spot for you", "we can do the deposit over the phone")
- Variable scheduling details that change daily (e.g., "could be next day or \
3 days later depending on scheduling") — only flag if it's a FIXED policy \
(e.g., "this procedure cannot be done same day", "appointments require 48 hours notice")

EPHEMERAL vs PERMANENT — classify every finding:
Mark each finding as "ephemeral" or "permanent" in the durability field.
EPHEMERAL (changes daily/weekly — useful context but not KB-worthy):
- Specific staff availability on specific days ("she's off Monday", "he has a 3:45 opening tomorrow")
- Who is or isn't working today/tomorrow/this week, who called in sick
- Specific appointment slot availability ("we have a 2pm open", "next available is Thursday")
- Temporary closures or schedule changes ("we're closed early today", "the owner is out this week")
- Waitlist or cancellation-based openings ("we just had a cancellation at 4pm")
- One-time operational issues ("we're running behind today", "there might be a wait today")
PERMANENT (durable knowledge that belongs in KB/prompts):
- Pricing, policies, services, aftercare, financing — always permanent
- Permanent business hours ("we're open Monday-Saturday 9am-6pm")
- Staff capabilities/specializations ("she also does that service", "he handles insurance claims")
- General scheduling policies ("appointments must be booked 24 hours in advance")
- Typical lead times and booking windows ("that service books out 2-3 weeks in advance")
- How to book ("you can book online at our website", "call us to schedule")
</what_to_ignore>

<search_strategy>
You control the knowledge_base_search tool. Use it strategically:

BATCHING — group related claims into fewer searches when it makes sense:
- Same service: roof repair pricing + warranty + timeline → search "roof repair"
- Same category: deposit and cancellation policies → search "deposit cancellation policy"
- Cross-source: staff SMS and phone call both mention same topic → one search
- Related add-ons: base service + add-on → search together

DO NOT BATCH these — keep them as separate searches:
- Unrelated services (grooming and boarding are separate searches)
- Topics from completely different domains (pricing vs post-service care)

CONTACT INFO — phone numbers, addresses, emails, and hours often live in a "contact" \
or "about us" KB article. If staff shares any contact detail, search specifically for it:
- Phone number → search "phone number contact" (match_count=5)
- Address → search "address location" (match_count=5)
- Hours → search "hours of operation" (match_count=5)
These are commonly missed by topic-based searches, so always do a dedicated search.

MATCH COUNT — you control how many results to return per search:
- Use match_count=2-3 for specific facts (exact price, specific policy)
- Use match_count=4-5 for moderate topics (a service overview)
- Use match_count=6-7 for broad topics (general aftercare, full service menu)

FOLLOW-UP SEARCHES — if a broad search doesn't cover specific details you need to \
verify, do a targeted follow-up search. Thoroughness beats efficiency.

You may search as many times as needed. Do not limit yourself.
</search_strategy>

<comparison_rules>
After searching, compare what you found against BOTH the KB results AND the AI's \
current prompts (provided in the user message).

TYPE DECISION TREE — follow this EXACTLY for every finding:
1. Does the KB or prompts mention the SAME topic with a DIFFERENT value?
   → YES = "contradictory" (even if the KB is outdated or staff has newer info)
   Examples of "contradictory":
   - KB says "$500" but staff said "$450" → contradictory
   - KB says "open 9:30-5" but staff said "open 10-6" → contradictory
   - KB says "24-hour cancellation" but staff said "48-hour cancellation" → contradictory
   - KB says "results in 3-7 days" but staff said "results in 2 weeks" → contradictory
   - KB says "lasts 3-4 months" but staff said "lasts 6 months" → contradictory
2. Does the KB or prompts mention the topic but LACK specific details staff provided?
   → YES = "incomplete"
   Examples of "incomplete":
   - KB mentions the service exists but has no pricing → incomplete
   - KB mentions financing but no APR rate or terms → incomplete
   - KB mentions a program but not the full price breakdown → incomplete
3. Is the topic completely ABSENT from KB and prompts?
   → YES = "missing"
   Examples of "missing":
   - Staff mentioned a service not listed anywhere → missing
   - Staff quoted a deposit policy with no equivalent in KB → missing
   - Staff mentioned a new add-on service → missing

IMPORTANT: If two numbers for the same thing differ, it is ALWAYS "contradictory", \
never "missing". The test is: "does the KB already say something about this specific \
topic?" If yes and the values differ → contradictory. If yes but it lacks detail → \
incomplete. If no → missing.

DO NOT FLAG:
- Information already present in KB results or prompts, even if worded differently
- Vague or non-specific claims ("we can help with that")
- One-off situational responses ("I'll make an exception this time")
- Information reasonably implied by existing content
- Operational procedures already covered in the conversion_agent_prompt \
  (booking flows, routing logic, question hierarchy, etc.)
- A staff message that CONFIRMS what the KB already says. If staff says "$500" and the \
  KB also says "$500" — that is NOT a finding. Skip it entirely.
- IMPORTANT: When multiple staff messages mention the same topic with DIFFERENT values \
  (e.g., one says "$100" and another says "$75"), ONLY flag the message whose value \
  DIFFERS from the KB. The message that MATCHES the KB is not a finding. Evaluate each \
  message independently against the KB, not against other staff messages.
</comparison_rules>

<output>
After completing ALL your searches, provide your final analysis as JSON.
Return ONLY the JSON — no markdown fences, no explanation before or after.

{
  "findings": [
    {
      "type": "missing|contradictory|incomplete",
      "durability": "permanent|ephemeral",
      "topic": "Short label (e.g. 'roof repair pricing', 'ceramic coating aftercare', 'puppy grooming age limit')",
      "detail": "What specifically is missing or wrong — be precise, include all numbers",
      "source_quote": "The staff message or call excerpt that revealed this, max 150 chars",
      "source_type": "message|call",
      "source_timestamp": "The timestamp from the source (copy the [time: ...] value exactly)",
      "contact_id": "The contact ID from the source (copy the [contact: ...] value exactly)",
      "suggested_action": "Specific action to close the gap — include exact numbers, rates, \
timeframes from the source",
      "priority": "high|medium|low"
    }
  ],
  "kb_searches_performed": <number of searches you made>,
  "summary": "One-sentence overall assessment"
}

BEFORE assigning priority, apply the AI RELEVANCE TEST from <ai_scope>:
"Would a lead realistically ask the AI about this, AND would the AI give a wrong \
or incomplete answer without this information?"
- If NO → do NOT include the finding at all (not low priority — EXCLUDE it entirely)
- If YES → assign priority as below

Priority rules (only for findings that PASSED the relevance test):
- HIGH: Pricing, contact info, financing terms, or policies that would cause the AI \
  to give wrong answers or miss sales opportunities
- MEDIUM: Services, procedures, aftercare, or treatment protocols the AI doesn't know
- LOW: Minor supplementary details

If no gaps found, return: {"findings": [], "kb_searches_performed": N, "summary": "..."}

CRITICAL VERIFICATION — do this BEFORE writing your JSON:
Re-read the CURRENT AI PROMPTS section (services_prompt, offers_config, etc.) from the \
user message. For EVERY finding you are about to include, verify the information is not \
already stated in those prompts. The prompts are the DEFINITIVE source of truth for what \
the AI already knows. If a price, service, policy, or detail appears in ANY prompt, that \
finding must be REMOVED — it is not missing, contradictory, or incomplete. \
KB search results may be incomplete, but the prompts are always complete.
</output>

<rules>
- Only flag genuinely new or conflicting information — when in doubt, don't flag it
- "Contradictory" requires the KB/prompts to clearly state something different
- Be specific in suggested_action — "add to KB" is too vague. Actionable examples:
  "Add roof inspection pricing ($150 for first visit, free with signed contract) to KB"
  "Add ceramic coating cure time (no water for 48 hours, no wax for 30 days) to KB"
  "Add puppy grooming age requirement (minimum 12 weeks with vaccines) to KB"
- GRANULARITY IS CRITICAL: Every individual fact gets its own finding. Never combine \
  unrelated facts into one finding. Examples of separate findings:
  - "no parking on new driveway for 72 hours" = one finding
  - "GreenSky financing 0% APR for 12 months" = one finding
  - "GreenSky approval takes 5 minutes online" = one finding
  - "full results visible in 4-6 weeks" = one finding
  - "can bundle gutter cleaning with roof inspection" = one finding
  - "dogs must be current on rabies vaccine" = one finding
  - "$75 deposit required to hold appointment" = one finding
  - "keep pets off treated lawn for 48 hours" = one finding
  A single call transcript may produce 10+ separate findings. That is expected.
  CRITICAL EXAMPLE — pricing + protocol in same sentence must be split:
  If staff says "detail package is $350, 3-stage process, allow 48 hours between stages, \
  re-apply sealant every 6-12 months" — that's FOUR separate findings:
  1. $350 package pricing
  2. 3-stage process recommendation
  3. 48 hours spacing between stages
  4. 6-12 month sealant reapplication schedule
  Another example: "training program is $200/session, 4-session series, 1 week apart, \
  maintenance session every quarter" — FOUR separate findings:
  1. $200/session pricing
  2. 4-session series recommendation
  3. 1 week spacing between sessions
  4. Quarterly maintenance schedule
- COMPLETENESS CHECK: Before writing your final JSON, review all the source data one \
  more time. For each conversation or call, ask: "Did I capture every specific number, \
  percentage, timeframe, rate, brand name, and condition mentioned?" If you find any \
  you missed, add them.
- SOURCE ISOLATION: Every finding must come from ONE specific source message or call. \
  Never merge information from multiple messages into a single finding, even if they \
  discuss similar topics. If two messages mention the same service with different numbers, \
  that's TWO separate findings (one per message). The "source_quote" must be a verbatim \
  substring copy-pasted from ONE source — not paraphrased, not combined from multiple sources.
- VERBATIM QUOTES: The "source_quote" field must contain the EXACT text from the source \
  message or call transcript. Do not paraphrase, reword, or combine quotes from different \
  sources. If you cannot find the exact text, quote the closest relevant substring.
</rules>"""


# ============================================================================
# KB SEARCH TOOL DEFINITION
# ============================================================================

_KB_SEARCH_TOOL_DEF = {
    "type": "function",
    "function": {
        "name": "knowledge_base_search",
        "description": (
            "Search the client's knowledge base to check if specific information "
            "already exists. Returns relevant articles ranked by similarity. "
            "Use this to verify whether facts from staff messages/calls are already "
            "in the AI's knowledge."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": (
                        "What to search for — use natural language. Broader queries "
                        "find more context, specific queries verify exact facts."
                    ),
                },
                "match_count": {
                    "type": "integer",
                    "description": (
                        "Number of results to return (1-10). Use 2-3 for specific "
                        "facts, 4-5 for moderate topics, 6-7 for broad overviews."
                    ),
                },
            },
            "required": ["query"],
        },
    },
}


# ============================================================================
# HELPERS
# ============================================================================


def _parse_ai_json(raw: str) -> dict | list | None:
    """Parse JSON from AI response (may have markdown fences)."""
    if not raw:
        return None

    # Strip markdown code fences if present
    cleaned = raw.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```\w*\s*\n?", "", cleaned)
        cleaned = re.sub(r"\n?```\s*$", "", cleaned)
        cleaned = cleaned.strip()

    # Try direct parse first (fastest path)
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        pass

    # Try object extraction (findings response)
    match = re.search(r"\{[\s\S]*\}", cleaned)
    if match:
        try:
            return json.loads(match.group(0))
        except json.JSONDecodeError as e:
            logger.debug("MANUAL_LEARNING | json_object_parse_error | %s", e)

    # Try array extraction
    match = re.search(r"\[[\s\S]*\]", cleaned)
    if match:
        try:
            return json.loads(match.group(0))
        except json.JSONDecodeError as e:
            logger.debug("MANUAL_LEARNING | json_array_parse_error | %s", e)

    return None


# Model pricing per 1M tokens (input, output) — from OpenRouter
_DEFAULT_MODEL = "google/gemini-2.5-flash"
_PRICING: dict[str, tuple[float, float]] = {
    "anthropic/claude-sonnet-4-6": (3.00, 15.00),
    "anthropic/claude-3.5-haiku": (0.80, 4.00),
    "google/gemini-2.5-flash": (0.30, 2.50),
    "google/gemini-2.5-pro": (1.25, 10.00),
    "google/gemini-3-flash-preview": (0.50, 3.00),
    "openai/gpt-4.1-mini": (0.40, 1.60),
}
_OPENROUTER_MARKUP = 1.055  # OpenRouter charges 5.5% on top


@dataclass
class _LLMUsage:
    """Token usage from a single LLM call."""
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0
    cost_usd: float = 0.0


@dataclass
class WorkflowTokenTracker:
    """Accumulates token usage across all LLM calls in a workflow run."""
    calls: list[dict[str, Any]] = field(default_factory=list)
    total_prompt_tokens: int = 0
    total_completion_tokens: int = 0
    total_tokens: int = 0
    total_cost_usd: float = 0.0

    def add(self, usage: _LLMUsage, label: str = "", provider: str = "openrouter") -> None:
        self.total_prompt_tokens += usage.prompt_tokens
        self.total_completion_tokens += usage.completion_tokens
        self.total_tokens += usage.total_tokens
        self.total_cost_usd += usage.cost_usd
        self.calls.append({
            "label": label,
            "provider": provider,
            "prompt_tokens": usage.prompt_tokens,
            "completion_tokens": usage.completion_tokens,
            "total_tokens": usage.total_tokens,
            "cost_usd": round(usage.cost_usd, 6),
        })

    def summary(self) -> dict[str, Any]:
        return {
            "prompt_tokens": self.total_prompt_tokens,
            "completion_tokens": self.total_completion_tokens,
            "total_tokens": self.total_tokens,
            "call_count": len(self.calls),
            "estimated_cost_usd": round(self.total_cost_usd, 4),
            "calls": self.calls,
        }


def _extract_usage(resp: Any, model: str = _DEFAULT_MODEL) -> _LLMUsage:
    """Extract token usage and cost from an OpenRouter response."""
    usage = _LLMUsage()
    if resp.usage:
        usage.prompt_tokens = resp.usage.prompt_tokens or 0
        usage.completion_tokens = resp.usage.completion_tokens or 0
        usage.total_tokens = resp.usage.total_tokens or 0
        # Prefer actual cost from OpenRouter response
        actual_cost = getattr(resp.usage, "cost", None)
        if actual_cost is not None:
            usage.cost_usd = actual_cost
        else:
            inp_rate, out_rate = _PRICING.get(model, (3.00, 15.00))
            usage.cost_usd = (
                (usage.prompt_tokens * inp_rate + usage.completion_tokens * out_rate)
                / 1_000_000
                * _OPENROUTER_MARKUP
            )
    return usage


# ============================================================================
# DATA COLLECTION
# ============================================================================


async def _get_manual_messages(
    chat_table: str,
    day_start: datetime,
    day_end: datetime,
) -> list[dict[str, Any]]:
    """Query chat history for manual (staff-sent) messages in the time window."""
    if not chat_table or not postgres.chat_pool:
        return []

    try:
        rows = await postgres.chat_pool.fetch(
            f'SELECT content, "timestamp", session_id, channel '
            f'FROM "{chat_table}" '
            f"WHERE role = 'ai' AND source = 'manual' "
            f'AND "timestamp" >= $1 AND "timestamp" < $2 '
            f'ORDER BY "timestamp" DESC',
            day_start,
            day_end,
        )
    except Exception as e:
        logger.warning("MANUAL_LEARNING | manual_messages_query_failed | error=%s", e)
        return []

    result = [
        {
            "content": r["content"] or "",
            "timestamp": r["timestamp"].isoformat() if r["timestamp"] else "",
            "session_id": r["session_id"],
            "channel": r["channel"] or "SMS",
        }
        for r in rows
        if r["content"]
    ]

    logger.info(
        "MANUAL_LEARNING | manual_messages | table=%s | count=%d",
        chat_table[:30],
        len(result),
    )
    return result


async def _get_call_logs(
    client_id: str,
    day_start: datetime,
    day_end: datetime,
) -> list[dict[str, Any]]:
    """Query client_call_logs for calls in the time window."""
    if not postgres.main_pool:
        return []

    try:
        rows = await postgres.main_pool.fetch(
            "SELECT summary, transcript, direction, status, duration_seconds, "
            "user_sentiment, created_at, ghl_contact_id "
            "FROM client_call_logs "
            "WHERE client_id = $1 "
            "AND created_at >= $2 AND created_at < $3 "
            "ORDER BY created_at DESC",
            client_id,
            day_start,
            day_end,
        )
    except Exception as e:
        logger.warning("MANUAL_LEARNING | call_logs_query_failed | error=%s", e)
        return []

    result = [
        {
            "summary": r["summary"] or "",
            "transcript": r["transcript"] or "",
            "direction": r["direction"] or "",
            "status": r["status"] or "",
            "duration_seconds": r["duration_seconds"] or 0,
            "user_sentiment": r["user_sentiment"] or "",
            "created_at": r["created_at"].isoformat() if r["created_at"] else "",
            "ghl_contact_id": r["ghl_contact_id"] or "",
        }
        for r in rows
        if r["summary"] or r["transcript"]
    ]

    logger.info(
        "MANUAL_LEARNING | call_logs | client=%s | count=%d",
        client_id[:8],
        len(result),
    )
    return result


async def _get_kb_articles(client_id: str) -> list[dict[str, str]]:
    """Load knowledge base articles for a client (for validator context)."""
    try:
        resp = await supabase._request(
            supabase.main_client, "GET", "/knowledge_base_articles",
            params={
                "client_id": f"eq.{client_id}",
                "select": "title,content,tag",
                "order": "created_at.asc",
            },
            label="get_kb_articles",
        )
        articles = resp.json()
        if isinstance(articles, list):
            return [
                {"title": a.get("title", ""), "content": a.get("content", ""), "tag": a.get("tag", "")}
                for a in articles
            ]
    except Exception as e:
        logger.warning("MANUAL_LEARNING | kb_articles_load_failed | error=%s", e)
    return []


# ============================================================================
# AGENT LOOP
# ============================================================================


def _build_agent_user_prompt(
    manual_messages: list[dict[str, Any]],
    call_logs: list[dict[str, Any]],
    prompts: dict[str, str],
    client_name: str,
    call_mode: str = "transcript",
) -> str:
    """Build the user prompt with all data for the learning agent."""
    parts = [f"## Client: {client_name}\n"]

    # Staff messages
    if manual_messages:
        parts.append("## STAFF MESSAGES (last 48h)")
        for msg in manual_messages[:100]:
            ts = msg.get("timestamp", "")[:19]  # trim to seconds
            sid = msg.get("session_id", "")
            parts.append(f"- [{msg['channel']}] [time: {ts}] [contact: {sid}] {msg['content']}")

    # Call logs
    if call_logs:
        parts.append("\n## CALL LOGS (last 48h)")
        for call in call_logs:
            ts = call.get("created_at", "")[:19]
            cid = call.get("ghl_contact_id", "")
            header = f"- [{call['direction']}, {call['status']}, {call['duration_seconds']}s"
            if call["user_sentiment"]:
                header += f", sentiment: {call['user_sentiment']}"
            header += f", time: {ts}, contact: {cid}]"

            call_parts = []
            if call_mode in ("summary", "both") and call["summary"]:
                call_parts.append(f"Summary: {call['summary']}")
            if call_mode in ("transcript", "both") and call["transcript"]:
                transcript = call["transcript"][:3000]
                call_parts.append(f"Transcript: {transcript}")

            if call_parts:
                parts.append(f"{header} {call_parts[0]}")
                for extra in call_parts[1:]:
                    parts.append(f"  {extra}")

    # Current AI prompts
    parts.append("\n## CURRENT AI PROMPTS")
    parts.append("These are the prompts the AI sales rep currently uses. "
                 "Information already here does NOT need to be flagged. "
                 "These prompts are the DEFINITIVE source of truth.\n")
    for key in ["services_prompt", "offers_config", "qualification_criteria_prompt",
                "conversion_agent_prompt"]:
        value = prompts.get(key, "")
        if value and value != "Not Configured":
            parts.append(f"### {key}:\n{value}\n")

    # Reminder at end of message (closest to where model writes output)
    parts.append("\n## REMINDER")
    parts.append("Before flagging any finding, re-read the CURRENT AI PROMPTS above. "
                 "Any price, service, policy, or detail that appears in ANY prompt is "
                 "already known — do NOT flag it as missing, contradictory, or incomplete.")

    return "\n".join(parts)


async def _run_learning_agent(
    manual_messages: list[dict[str, Any]],
    call_logs: list[dict[str, Any]],
    prompts: dict[str, str],
    client_id: str,
    client_name: str,
    call_mode: str = "transcript",
    model: str = _DEFAULT_MODEL,
    tracker: WorkflowTokenTracker | None = None,
) -> tuple[list[dict[str, Any]], int]:
    """Run the learning agent with KB search tool access.

    Uses OpenRouter as primary provider with Google Gemini direct as fallback.
    Caps KB searches to prevent infinite search loops.
    Returns (findings, kb_searches_performed).
    """
    import openai as openai_lib

    # Build provider clients: OpenRouter primary, Google direct fallback
    providers: list[tuple[str, openai_lib.AsyncOpenAI]] = []
    if settings.openrouter_api_key:
        providers.append(("openrouter", openai_lib.AsyncOpenAI(
            base_url="https://openrouter.ai/api/v1",
            api_key=settings.openrouter_api_key,
            timeout=120.0,
        )))
    if settings.google_gemini_api_key:
        providers.append(("google", openai_lib.AsyncOpenAI(
            base_url="https://generativelanguage.googleapis.com/v1beta/openai/",
            api_key=settings.google_gemini_api_key,
            timeout=120.0,
        )))
    if not providers:
        logger.warning("MANUAL_LEARNING | no_api_keys_configured")
        return [], 0

    # Google direct uses different model IDs (no provider prefix)
    _google_model_map = {
        "google/gemini-2.5-flash": "gemini-2.5-flash",
        "google/gemini-2.5-pro": "gemini-2.5-pro",
        "google/gemini-3-flash-preview": "gemini-3-flash-preview",
    }

    user_prompt = _build_agent_user_prompt(
        manual_messages, call_logs, prompts, client_name, call_mode,
    )

    tools = [_KB_SEARCH_TOOL_DEF]
    max_iterations = 15  # Safety limit — typically finishes in 2-3
    max_kb_searches = 35  # Prevent infinite search loops (normal is ~25)

    # Try each provider in order — on failure, fall back to next
    for provider_name, client in providers:
        messages: list[dict[str, Any]] = [
            {"role": "system", "content": LEARNING_AGENT_SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ]
        kb_searches = 0
        active_model = (
            _google_model_map.get(model, model) if provider_name == "google" else model
        )

        logger.info(
            "MANUAL_LEARNING | agent_start | provider=%s | model=%s | client=%s",
            provider_name, active_model, client_name,
        )

        agent_failed = False
        for iteration in range(max_iterations):
            try:
                resp = await client.chat.completions.create(
                    model=active_model,
                    messages=messages,
                    tools=tools,
                    temperature=0,
                    max_tokens=16384,
                )
            except Exception as e:
                logger.warning(
                    "MANUAL_LEARNING | agent_call_failed | provider=%s | iteration=%d | error=%s",
                    provider_name, iteration, e,
                )
                agent_failed = True
                break

            # Track token usage
            usage = _extract_usage(resp, model=model)
            if tracker:
                tracker.add(usage, f"{provider_name}_turn_{iteration}", provider=provider_name)

            choice = resp.choices[0]

            # If model wants to call tools
            if choice.message.tool_calls:
                messages.append(choice.message)

                for tool_call in choice.message.tool_calls:
                    fn_name = tool_call.function.name
                    try:
                        args = json.loads(tool_call.function.arguments)
                    except json.JSONDecodeError:
                        args = {}

                    if fn_name == "knowledge_base_search":
                        # Check KB search cap
                        if kb_searches >= max_kb_searches:
                            logger.warning(
                                "MANUAL_LEARNING | kb_search_cap_hit | client=%s | cap=%d",
                                client_name, max_kb_searches,
                            )
                            messages.append({
                                "role": "tool",
                                "tool_call_id": tool_call.id,
                                "content": json.dumps({
                                    "result": "Search limit reached. You have done enough searches. "
                                    "Proceed to write your final JSON output now.",
                                    "match_count": 0,
                                }),
                            })
                            continue

                        query = args.get("query", "")
                        match_count = args.get("match_count", 5)
                        match_count = max(1, min(10, match_count))

                        try:
                            kb_result = await knowledge_base_search(
                                query=query,
                                entity_id=client_id,
                                contact_id="",
                                channel="learning_workflow",
                                is_test_mode=False,
                                match_count=match_count,
                            )
                        except Exception as e:
                            logger.warning(
                                "MANUAL_LEARNING | agent_kb_search_failed | query=%s | error=%s",
                                query[:50], e,
                            )
                            kb_result = {"result": "Search failed", "match_count": 0}

                        kb_searches += 1
                        logger.info(
                            "MANUAL_LEARNING | agent_kb_search | provider=%s | iter=%d | "
                            "query=%s | match_count=%d | matches=%d",
                            provider_name, iteration, query[:60], match_count,
                            kb_result.get("match_count", 0),
                        )

                        messages.append({
                            "role": "tool",
                            "tool_call_id": tool_call.id,
                            "content": json.dumps(kb_result),
                        })
                    else:
                        messages.append({
                            "role": "tool",
                            "tool_call_id": tool_call.id,
                            "content": json.dumps({"error": f"Unknown tool: {fn_name}"}),
                        })

            else:
                # Final response — no more tool calls
                content = choice.message.content or ""

                logger.info(
                    "MANUAL_LEARNING | agent_complete | provider=%s | client=%s | "
                    "iterations=%d | kb_searches=%d | cost=$%.4f",
                    provider_name, client_name, iteration + 1, kb_searches,
                    tracker.total_cost_usd if tracker else 0,
                )

                parsed = _parse_ai_json(content)
                if isinstance(parsed, dict) and "findings" in parsed:
                    findings = parsed["findings"]
                    search_count = parsed.get("kb_searches_performed", kb_searches)
                    logger.info(
                        "MANUAL_LEARNING | findings=%d | reported_searches=%d",
                        len(findings), search_count,
                    )
                    return findings, kb_searches
                if isinstance(parsed, list):
                    return parsed, kb_searches

                logger.warning(
                    "MANUAL_LEARNING | agent_response_parse_failed | provider=%s | raw=%s",
                    provider_name, content[:300],
                )
                # Parse failure — try next provider
                agent_failed = True
                break

        if not agent_failed:
            # Hit max_iterations without finishing
            logger.warning(
                "MANUAL_LEARNING | agent_max_iterations | provider=%s | client=%s | "
                "iterations=%d | kb_searches=%d",
                provider_name, client_name, max_iterations, kb_searches,
            )
            agent_failed = True

        if agent_failed and provider_name != providers[-1][0]:
            logger.warning(
                "MANUAL_LEARNING | falling_back | from=%s | client=%s",
                provider_name, client_name,
            )
            await asyncio.sleep(2)  # Brief pause before fallback

    # All providers failed
    logger.error("MANUAL_LEARNING | all_providers_failed | client=%s", client_name)
    return [], kb_searches


# ============================================================================
# SECOND-PASS VALIDATION
# ============================================================================


VALIDATION_SYSTEM_PROMPT = """\
You are a validation filter. You receive a list of "findings" from a knowledge gap analysis \
and the business's current AI prompts. Your ONLY job: remove findings that are already covered \
by the prompts.

<rules>
For EACH finding, check:
1. Is the specific information (price, policy, service, detail) already stated in ANY prompt?
   → YES = REMOVE it (it's a false positive)
   → NO = KEEP it (it's a real gap)

"Already stated" means the prompt contains the same fact, even if worded differently:
- Finding says "Botox $12/unit" and prompt says "$12 per unit for Botox" → REMOVE (same fact)
- Finding says "Dysport $4/unit" and prompt says "Dysport" with no price → KEEP (price is new)
- Finding says "open 9-5" and prompt says "hours: 9am-5pm" → REMOVE (same fact)
- Finding says "$75 deposit" and no prompt mentions deposits → KEEP (genuinely missing)

For "contradictory" findings: Check if the core fact (price, policy, number) actually DIFFERS \
from the prompts. If the numbers/values MATCH (e.g., finding says "$500" and prompt says "$500"), \
REMOVE it — that's a false contradiction, not a real one. Only KEEP if the values genuinely differ.

For "incomplete" findings: KEEP only if the specific missing detail is truly absent from ALL \
prompts. If the detail exists in any prompt, REMOVE it. Subjective descriptions ("amazing results", \
"very popular", "great for skin") are NOT actionable knowledge gaps — REMOVE these.

For "missing" findings: KEEP only if no prompt mentions the topic at all. If any prompt covers \
the same topic with the same information, REMOVE it.

EPHEMERAL findings (durability="ephemeral"): Do NOT remove these. They are kept intentionally \
for the business owner to review. Only remove a finding if it is a false positive (already \
covered by the prompts). Preserve the "durability" field as-is on all findings you keep.

AI RELEVANCE filter: Also REMOVE findings about things the AI would never discuss with a lead:
- Internal payment logistics (how deposits are collected, invoice methods, payment processing)
- Internal slot management (holding appointments, releasing slots, manual scheduling)
- One-off financial actions (specific refunds, credits, billing adjustments)
- Variable scheduling details that depend on daily availability, not fixed policies
The AI books appointments conversationally and may send booking/payment links, but it does \
NOT process payments, hold slots, or handle billing. If a finding is about a staff-only \
operational process that the AI would never need to know, REMOVE it.
</rules>

<output>
Return ONLY a JSON array of findings that passed validation (real gaps). Same format as input.
If ALL findings are false positives, return an empty array: []
Do not add new findings. Do not modify existing findings. Only filter.
Return ONLY the JSON — no markdown fences, no explanation.
</output>"""


def _deterministic_fp_filter(
    findings: list[dict[str, Any]],
    prompts: dict[str, str],
) -> tuple[list[dict[str, Any]], int]:
    """Remove findings whose service+price combo already exists in the services_prompt.

    Uses line-based matching: a finding is a false positive if ANY single line in the
    services_prompt contains BOTH a keyword from the finding AND the same dollar amount.
    This is fully deterministic — no LLM needed.

    Returns (filtered_findings, removed_count).
    """
    services_prompt = prompts.get("services_prompt", "")
    if not services_prompt:
        return findings, 0

    sp_lower = services_prompt.lower()
    sp_lines = [line.strip() for line in sp_lower.split("\n") if line.strip()]

    # Common words to exclude from service name matching
    _STOP_WORDS = {
        "this", "that", "with", "from", "about", "your", "their", "they",
        "have", "also", "just", "like", "it's", "it is", "we're", "were",
        "session", "treatment", "treatments", "price", "pricing", "starts",
        "starting", "costs", "cost", "offer", "available", "special", "right",
        "currently", "only", "month", "year", "week", "series", "package",
    }

    filtered = []
    removed = 0

    for f in findings:
        sq = f.get("source_quote", "").lower()

        # Extract dollar amounts from source_quote
        prices = re.findall(r"\$\d[\d,]*", sq)
        if not prices:
            filtered.append(f)
            continue

        # Extract meaningful words (service identifiers)
        sq_no_price = re.sub(r"\$\d[\d,]*(?:\.\d+)?", "", sq)
        words = [
            w for w in re.findall(r"\b[a-z][a-z.]+\b", sq_no_price)
            if len(w) > 3 and w not in _STOP_WORDS
        ]
        if not words:
            filtered.append(f)
            continue

        # Check if any SP line contains BOTH a service word AND the price(s).
        # When a finding has multiple prices (a range like "$150-$300"),
        # ALL prices must appear on the SAME line to be a true FP.
        # This prevents partial range overlaps from being filtered
        # (e.g., staff says "$5-$8" vs SP "$8-$12" — only $8 overlaps).
        is_fp = False
        for line in sp_lines:
            # Count how many distinct prices exist on this SP line.
            # Lines with 3+ prices indicate tiered pricing (e.g.,
            # "$150-$250 (single), $250-$400 (two-story)") where the
            # deterministic filter can't distinguish which tier the
            # finding refers to. Skip these — let the LLM handle it.
            line_prices = re.findall(r"\$\d[\d,]*(?:\.\d+)?", line)
            if len(line_prices) >= 3:
                continue

            if len(prices) >= 2:
                # Range: ALL prices must match the same line
                if not all(p in line for p in prices):
                    continue
            else:
                # Single price: must appear on line
                if prices[0] not in line:
                    continue
            # Price(s) matched — check for 2+ service words on same line.
            # Requiring 2+ words prevents cross-service matches where a
            # generic word like "home" matches a different service's line.
            matched_words = sum(1 for word in words if word in line)
            if matched_words >= 2:
                is_fp = True
            if is_fp:
                break

        if is_fp:
            removed += 1
        else:
            filtered.append(f)

    if removed:
        logger.info(
            "MANUAL_LEARNING | deterministic_fp_filter | removed=%d | remaining=%d",
            removed, len(filtered),
        )

    return filtered, removed


def _deterministic_kb_content_filter(
    findings: list[dict[str, Any]],
    kb_articles: list[dict[str, str]],
) -> tuple[list[dict[str, Any]], int]:
    """Remove non-contradictory findings whose topic is confirmed by KB content.

    Uses 3-gram matching: 2+ distinct 3-word phrases from source_quote found
    in KB article text.

    Safety rails:
    - Contradictions are NEVER filtered (they differ from KB by definition).
    - 3-word phrases required to avoid coincidental matches on large KBs.
    """
    if not kb_articles:
        return findings, 0

    # Build lowercased KB text for matching
    kb_text = "\n".join(a.get("content", "") for a in kb_articles).lower()

    _word_re = re.compile(r"\b[a-z0-9][a-z0-9.+/*#%-]*\b")
    kb_words = _word_re.findall(kb_text)

    # Pre-compute 3-gram set
    kb_3grams: set[tuple[str, str, str]] = set()
    for i in range(len(kb_words) - 2):
        kb_3grams.add((kb_words[i], kb_words[i + 1], kb_words[i + 2]))

    filtered = []
    removed = 0

    for f in findings:
        # Never filter contradictions — they differ from KB/prompts by definition
        if f.get("type", "") == "contradictory":
            filtered.append(f)
            continue

        sq = f.get("source_quote", "").lower()
        sq_words = _word_re.findall(sq)

        if len(sq_words) < 3:
            filtered.append(f)
            continue

        # 3-gram matching: 2+ distinct 3-word phrases from quote found in KB
        match_count = 0
        for i in range(len(sq_words) - 2):
            if (sq_words[i], sq_words[i + 1], sq_words[i + 2]) in kb_3grams:
                match_count += 1
                if match_count >= 2:
                    break

        if match_count >= 2:
            removed += 1
        else:
            filtered.append(f)

    if removed:
        logger.info(
            "MANUAL_LEARNING | kb_content_filter | removed=%d | remaining=%d",
            removed, len(filtered),
        )

    return filtered, removed


async def _validate_findings(
    findings: list[dict[str, Any]],
    prompts: dict[str, str],
    model: str = _DEFAULT_MODEL,
    tracker: WorkflowTokenTracker | None = None,
) -> list[dict[str, Any]]:
    """Two-stage validation: deterministic pre-filter + LLM validation.

    Stage 1: Deterministic filter removes findings whose service+price combo
    already exists on the same line in the services_prompt (zero variance).
    Stage 2: LLM call catches remaining nuanced false positives.
    """
    if not findings:
        return []

    # Stage 1: Deterministic false positive filter
    findings, det_removed = _deterministic_fp_filter(findings, prompts)
    if not findings:
        return []

    # Stage 1.5: Remove self-identified non-issues
    # The agent sometimes produces findings but marks them "no action needed"
    # in suggested_action — these are self-identified false positives.
    _NO_ACTION_PHRASES = ("no action needed", "information is consistent")
    before_self = len(findings)
    findings = [
        f for f in findings
        if not any(
            phrase in f.get("suggested_action", "").lower()
            for phrase in _NO_ACTION_PHRASES
        )
    ]
    self_removed = before_self - len(findings)
    if self_removed:
        logger.info(
            "MANUAL_LEARNING | self_identified_filter | removed=%d | remaining=%d",
            self_removed, len(findings),
        )
    if not findings:
        return []

    import openai as openai_lib

    # Build provider clients — Google direct first (more reliable for validation),
    # OpenRouter as fallback
    providers: list[tuple[str, openai_lib.AsyncOpenAI]] = []
    if settings.google_gemini_api_key:
        providers.append(("google", openai_lib.AsyncOpenAI(
            base_url="https://generativelanguage.googleapis.com/v1beta/openai/",
            api_key=settings.google_gemini_api_key,
            timeout=60.0,
        )))
    if settings.openrouter_api_key:
        providers.append(("openrouter", openai_lib.AsyncOpenAI(
            base_url="https://openrouter.ai/api/v1",
            api_key=settings.openrouter_api_key,
            timeout=60.0,
        )))
    if not providers:
        return findings  # Can't validate, return as-is

    _google_model_map = {
        "google/gemini-2.5-flash": "gemini-2.5-flash",
        "google/gemini-2.5-pro": "gemini-2.5-pro",
        "google/gemini-3-flash-preview": "gemini-3-flash-preview",
    }

    # Build compact user prompt with findings + prompts
    user_parts = ["## FINDINGS TO VALIDATE\n"]
    user_parts.append(json.dumps(findings, indent=2))

    user_parts.append("\n\n## CURRENT AI PROMPTS (source of truth)\n")
    for key in ["services_prompt", "offers_config", "qualification_criteria_prompt",
                "conversion_agent_prompt"]:
        value = prompts.get(key, "")
        if value and value != "Not Configured":
            user_parts.append(f"### {key}:\n{value}\n")

    user_prompt = "\n".join(user_parts)

    for provider_name, client in providers:
        active_model = (
            _google_model_map.get(model, model) if provider_name == "google" else model
        )

        try:
            resp = await client.chat.completions.create(
                model=active_model,
                messages=[
                    {"role": "system", "content": VALIDATION_SYSTEM_PROMPT},
                    {"role": "user", "content": user_prompt},
                ],
                temperature=0,
            )
        except Exception as e:
            logger.warning(
                "MANUAL_LEARNING | validation_failed | provider=%s | error=%s",
                provider_name, e,
            )
            continue

        # Track usage
        usage = _extract_usage(resp, model=model)
        if tracker:
            tracker.add(usage, f"validation_{provider_name}", provider=provider_name)

        # Check for truncation
        finish_reason = resp.choices[0].finish_reason or ""
        if finish_reason == "length":
            logger.warning(
                "MANUAL_LEARNING | validation_truncated | provider=%s | "
                "finish_reason=length | output too long",
                provider_name,
            )
            continue

        content = resp.choices[0].message.content or ""
        parsed = _parse_ai_json(content)

        if isinstance(parsed, list):
            removed = len(findings) - len(parsed)
            logger.info(
                "MANUAL_LEARNING | validation_complete | provider=%s | "
                "input=%d | output=%d | removed=%d",
                provider_name, len(findings), len(parsed), removed,
            )
            return parsed
        elif isinstance(parsed, dict) and "findings" in parsed:
            validated = parsed["findings"]
            removed = len(findings) - len(validated)
            logger.info(
                "MANUAL_LEARNING | validation_complete | provider=%s | "
                "input=%d | output=%d | removed=%d",
                provider_name, len(findings), len(validated), removed,
            )
            return validated

        logger.warning(
            "MANUAL_LEARNING | validation_parse_failed | provider=%s | raw=%s",
            provider_name, content[:200],
        )
        continue

    # All providers failed — return unfiltered findings
    logger.warning("MANUAL_LEARNING | validation_all_providers_failed | returning_unfiltered")
    return findings


# ============================================================================
# SLACK FORMATTING
# ============================================================================


def _format_slack_message(
    client_name: str,
    findings: list[dict[str, Any]],
    stats: dict[str, Any],
) -> str:
    """Format Slack message with permanent findings by priority, then ephemeral section."""
    div = "─" * 36

    permanent = [f for f in findings if f.get("durability") != "ephemeral"]
    ephemeral = [f for f in findings if f.get("durability") == "ephemeral"]

    lines = [
        f"*Knowledge Gap Report — {client_name}*",
        f"Analyzed {stats['manual_messages']} staff messages and "
        f"{stats['call_logs']} call logs",
        f"{stats['kb_searches']} KB searches performed, "
        f"{len(permanent)} gaps found"
        + (f", {len(ephemeral)} temporary notes" if ephemeral else ""),
        div,
    ]

    def _fmt(f: dict) -> str:
        type_label = {
            "missing": "MISSING",
            "contradictory": "CONFLICT",
            "incomplete": "INCOMPLETE",
        }.get(f.get("type", ""), "GAP")
        parts = [f"[{type_label}] *{f.get('topic', 'Unknown')}*"]
        # Source attribution line
        src_type = f.get("source_type", "")
        src_ts = f.get("source_timestamp", "")
        src_contact = f.get("contact_id", "")
        if src_type or src_ts:
            src_label = "Call" if src_type == "call" else "Message"
            src_parts = [f"Source: {src_label}"]
            if src_ts:
                src_parts.append(src_ts)
            if src_contact:
                src_parts.append(f"contact: {src_contact}")
            parts.append(f"  {', '.join(src_parts)}")
        if f.get("detail"):
            parts.append(f"  {f['detail']}")
        if f.get("source_quote"):
            parts.append(f'  Staff said: "{f["source_quote"][:150]}"')
        if f.get("suggested_action"):
            parts.append(f"  Action: {f['suggested_action']}")
        return "\n".join(parts)

    # ── Permanent findings grouped by priority ──
    if permanent:
        high = [f for f in permanent if f.get("priority") == "high"]
        medium = [f for f in permanent if f.get("priority") == "medium"]
        low = [f for f in permanent if f.get("priority") == "low"]

        if high:
            lines.append("HIGH PRIORITY:")
            for f in high:
                lines.append(_fmt(f))
                lines.append("")

        if medium:
            lines.append("MEDIUM PRIORITY:")
            for f in medium:
                lines.append(_fmt(f))
                lines.append("")

        if low:
            lines.append("LOW PRIORITY:")
            for f in low:
                lines.append(_fmt(f))
                lines.append("")

        lines.append(div)
        lines.append("Update KB or prompts to close these gaps. Tomorrow's run will verify.")
    else:
        lines.append("No permanent knowledge gaps found.")

    # ── Ephemeral findings in separate section ──
    if ephemeral:
        lines.append("")
        lines.append(div)
        lines.append("*TEMPORARY INFO* (may change daily — verify with staff before acting)")
        for f in ephemeral:
            lines.append(f"• *{f.get('topic', 'Unknown')}*: {f.get('detail', '')}")
            if f.get("source_quote"):
                lines.append(f'  Staff said: "{f["source_quote"][:150]}"')
            lines.append("")

    return "\n".join(lines)


# ============================================================================
# SLACK CHANNEL CREATION
# ============================================================================


async def _ensure_slack_channel(client: dict[str, Any]) -> str | None:
    """Ensure a Slack channel exists for a client. Creates if needed.

    Uses the same channel naming as daily_reports so both workflows share a channel.
    Returns the channel ID, or None if creation failed.
    """
    client_id = client["id"]
    client_name = client.get("name") or "unknown"

    slug = re.sub(r"[^a-z0-9\s-]", "", client_name.lower())
    slug = re.sub(r"\s+", "-", slug).strip("-")[:60]
    channel_name = f"{slug}-{client_id}"[:80]

    channel_id = await create_slack_channel(channel_name)
    if channel_id:
        await invite_to_channel(channel_id, RANDY_SLACK_USER_ID)
        try:
            await supabase.update_client_field(client_id, "slack_report_channel_id", channel_id)
        except Exception:
            logger.warning("MANUAL_LEARNING | failed to store channel_id | client=%s", client_name)
    return channel_id


# ============================================================================
# PER-CLIENT PROCESSING
# ============================================================================


async def _process_client(
    client: dict[str, Any],
    day_start: datetime,
    day_end: datetime,
    dry_run: bool = False,
    call_mode: str = "transcript",
    model: str = _DEFAULT_MODEL,
) -> dict[str, Any]:
    """Process a single client: collect data, run agent, post to Slack."""
    client_id = client["id"]
    client_name = client.get("name", "Unknown")
    chat_table = client.get("chat_history_table_name", "")
    tracker = WorkflowTokenTracker()

    try:
        logger.info("MANUAL_LEARNING | processing | client=%s", client_name)

        # 1. Collect manual messages + call logs in parallel
        manual_messages, call_logs = await asyncio.gather(
            _get_manual_messages(chat_table, day_start, day_end),
            _get_call_logs(client_id, day_start, day_end),
        )

        # 2. Skip if both empty
        if not manual_messages and not call_logs:
            logger.info("MANUAL_LEARNING | skipped | client=%s | no_data", client_name)
            return {"client": client_name, "status": "skipped", "reason": "no_data"}

        logger.info(
            "MANUAL_LEARNING | data_collected | client=%s | messages=%d calls=%d",
            client_name,
            len(manual_messages),
            len(call_logs),
        )

        # 3. Load current prompts
        full_config = await supabase._get_from_clients(client_id)
        if not full_config:
            logger.warning("MANUAL_LEARNING | config_not_found | client=%s", client_name)
            return {"client": client_name, "status": "error", "reason": "config_not_found"}

        prompts = _build_prompts(full_config, is_test_mode=False)

        # Resolve default setter for config
        _sc = full_config.get("system_config") or {}
        from app.text_engine.agent_compiler import resolve_setter
        _setter = resolve_setter(_sc, "") or {}

        # Derive offers text from setter's services config for gap detection
        from app.text_engine.offers import get_active_offers, render_offers_text
        from app.text_engine.services_compiler import compile_all_offers
        _offers_text = compile_all_offers(_setter.get("services"))
        if _offers_text:
            prompts["offers_config"] = _offers_text

        # Derive services and qualification text from setter's services config
        service_config = _setter.get("services") or {}
        from app.text_engine.qualification import format_services_for_followup
        prompts["services_prompt"] = format_services_for_followup(service_config)
        qual_parts = []
        for q in service_config.get("global_qualifications", []):
            qual_parts.append(f"{q['name']}: qualified={q.get('qualified','')}, disqualified={q.get('disqualified','')}")
        for svc in service_config.get("services", []):
            for q in svc.get("qualifications", []):
                qual_parts.append(f"{svc['name']} - {q['name']}: qualified={q.get('qualified','')}, disqualified={q.get('disqualified','')}")
        prompts["qualification_criteria_prompt"] = "\n".join(qual_parts) if qual_parts else ""

        # 4. Run the learning agent with retry (exponential backoff)
        data_count = len(manual_messages) + len(call_logs)
        max_attempts = 3 if data_count >= 5 else 1
        retry_delays = [5, 15]  # Seconds between retries
        findings: list[dict[str, Any]] = []
        kb_searches = 0

        for attempt in range(max_attempts):
            attempt_findings, attempt_kb = await _run_learning_agent(
                manual_messages=manual_messages,
                call_logs=call_logs,
                prompts=prompts,
                client_id=client_id,
                client_name=client_name,
                call_mode=call_mode,
                model=model,
                tracker=tracker,
            )
            kb_searches += attempt_kb

            if attempt_findings:
                findings = attempt_findings
                break

            if attempt < max_attempts - 1:
                delay = retry_delays[min(attempt, len(retry_delays) - 1)]
                logger.warning(
                    "MANUAL_LEARNING | 0_findings_retry | client=%s | attempt=%d/%d | "
                    "data_count=%d | waiting=%ds",
                    client_name, attempt + 1, max_attempts, data_count, delay,
                )
                await asyncio.sleep(delay)

        if not findings:
            logger.info("MANUAL_LEARNING | no_gaps | client=%s", client_name)
            return {
                "client": client_name,
                "status": "no_gaps",
                "manual_messages": len(manual_messages),
                "call_logs": len(call_logs),
                "kb_searches": kb_searches,
                "findings": 0,
                "token_usage": tracker.summary(),
            }

        # 5. Second-pass validation — filter out false positives
        kb_articles = await _get_kb_articles(client_id)
        findings, kb_removed = _deterministic_kb_content_filter(findings, kb_articles)
        pre_validation_count = len(findings)
        findings = await _validate_findings(
            findings, prompts, model=model, tracker=tracker,
        )
        pre_validation_count += kb_removed  # Include in total removed count
        if pre_validation_count != len(findings):
            logger.info(
                "MANUAL_LEARNING | validation_filtered | client=%s | before=%d after=%d",
                client_name, pre_validation_count, len(findings),
            )

        if not findings:
            logger.info("MANUAL_LEARNING | no_gaps_after_validation | client=%s", client_name)
            return {
                "client": client_name,
                "status": "no_gaps",
                "manual_messages": len(manual_messages),
                "call_logs": len(call_logs),
                "kb_searches": kb_searches,
                "findings": 0,
                "token_usage": tracker.summary(),
            }

        logger.info(
            "MANUAL_LEARNING | gaps_found | client=%s | count=%d",
            client_name,
            len(findings),
        )

        # 6. Format Slack message
        stats = {
            "manual_messages": len(manual_messages),
            "call_logs": len(call_logs),
            "kb_searches": kb_searches,
            "call_mode": call_mode,
        }
        slack_message = _format_slack_message(client_name, findings, stats)

        # 7. Post to Slack (or log in dry_run)
        posted = False
        if dry_run:
            logger.info(
                "MANUAL_LEARNING | dry_run | would_post | client=%s findings=%d",
                client_name,
                len(findings),
            )
            logger.info("MANUAL_LEARNING | dry_run | message:\n%s", slack_message)
        else:
            channel_id = client.get("slack_report_channel_id")
            if not channel_id:
                channel_id = await _ensure_slack_channel(client)
            if channel_id:
                try:
                    await post_slack_message(channel_id, slack_message)
                    posted = True
                    logger.info("MANUAL_LEARNING | slack_posted | client=%s", client_name)
                except Exception:
                    logger.warning(
                        "MANUAL_LEARNING | slack_failed | client=%s",
                        client_name,
                        exc_info=True,
                    )
            else:
                logger.warning("MANUAL_LEARNING | no_slack_channel | client=%s", client_name)

        return {
            "client": client_name,
            "status": "posted" if posted else ("dry_run" if dry_run else "no_channel"),
            "manual_messages": len(manual_messages),
            "call_logs": len(call_logs),
            "kb_searches": kb_searches,
            "call_mode": call_mode,
            "findings": len(findings),
            "findings_detail": findings if dry_run else [],
            "slack_posted": posted,
            "token_usage": tracker.summary(),
        }
    except Exception as e:
        logger.error("MANUAL_LEARNING | error | client=%s: %s", client_name, e)
        raise


# ============================================================================
# MAIN FLOW
# ============================================================================


@flow(name="manual-message-learning", retries=0)
async def run_manual_learning(
    dry_run: bool = False,
    call_mode: str = "transcript",
    model: str = _DEFAULT_MODEL,
    client_id: str | None = None,
) -> dict[str, Any]:
    """Main entry point — processes active clients (or a single client if client_id given).

    Called by the scheduler loop or manually via POST /workflows/manual-message-learning.
    If dry_run=True, skips Slack posts but logs what would be sent.
    call_mode: "summary", "transcript", or "both" — controls call data sent to the agent.
    model: OpenRouter model ID (default: anthropic/claude-sonnet-4-6).
    client_id: If set, only process this specific client.
    """
    now = datetime.now(timezone.utc)
    day_end = now
    day_start = now - timedelta(days=2)

    logger.info(
        "MANUAL_LEARNING | starting | window=%s to %s | dry_run=%s | call_mode=%s | model=%s | client_id=%s",
        day_start.isoformat(),
        day_end.isoformat(),
        dry_run,
        call_mode,
        model,
        client_id or "all",
    )

    # Fetch active clients only
    try:
        clients = await supabase.get_active_clients()
    except Exception:
        logger.exception("MANUAL_LEARNING | failed to fetch clients")
        return {"error": "Failed to fetch clients"}

    # Filter to single client if specified
    if client_id:
        clients = [c for c in clients if c["id"] == client_id]
        if not clients:
            return {"error": f"Client {client_id} not found or not active"}

    # Deduplicate clients sharing the same chat table (e.g., two entries for one business).
    # Keep the first entry per chat table to avoid processing the same messages twice.
    seen_tables: set[str] = set()
    deduped: list[dict[str, Any]] = []
    for c in clients:
        table = c.get("chat_history_table_name", "")
        if table and table in seen_tables:
            logger.info(
                "MANUAL_LEARNING | skipping_duplicate_table | client=%s | table=%s",
                c.get("name", "?"), table[:40],
            )
            continue
        if table:
            seen_tables.add(table)
        deduped.append(c)
    clients = deduped

    logger.info("MANUAL_LEARNING | clients_found | count=%d", len(clients))

    if dry_run:
        logger.info("MANUAL_LEARNING | dry_run=True — Slack posts will be skipped")

    # Process clients concurrently (max 5 at a time to avoid rate limits)
    MAX_CONCURRENT = 5
    semaphore = asyncio.Semaphore(MAX_CONCURRENT)

    async def _process_with_limit(client: dict[str, Any]) -> dict[str, Any]:
        async with semaphore:
            try:
                return await _process_client(
                    client, day_start, day_end,
                    dry_run=dry_run, call_mode=call_mode, model=model,
                )
            except Exception:
                name = client.get("name", client.get("id", "unknown"))
                logger.exception("MANUAL_LEARNING | client_failed | client=%s", name)
                return {"client": name, "status": "error"}

    results = await asyncio.gather(*[_process_with_limit(c) for c in clients])

    # Aggregate token usage across all clients
    total_prompt = sum(r.get("token_usage", {}).get("prompt_tokens", 0) for r in results)
    total_completion = sum(r.get("token_usage", {}).get("completion_tokens", 0) for r in results)
    total_tokens = sum(r.get("token_usage", {}).get("total_tokens", 0) for r in results)
    total_cost = sum(r.get("token_usage", {}).get("estimated_cost_usd", 0) for r in results)
    total_kb = sum(r.get("kb_searches", 0) for r in results)

    summary = {
        "clients_processed": len(results),
        "clients_with_data": sum(1 for r in results if r.get("status") != "skipped"),
        "clients_with_gaps": sum(1 for r in results if r.get("findings", 0) > 0),
        "total_findings": sum(r.get("findings", 0) for r in results),
        "total_kb_searches": total_kb,
        "slack_posted": sum(1 for r in results if r.get("slack_posted")),
        "errors": sum(1 for r in results if r.get("status") == "error"),
        "call_mode": call_mode,
        "model": model,
        "token_usage": {
            "prompt_tokens": total_prompt,
            "completion_tokens": total_completion,
            "total_tokens": total_tokens,
            "estimated_cost_usd": round(total_cost, 4),
        },
        "results": results if dry_run else [],
    }

    logger.info(
        "MANUAL_LEARNING | complete | processed=%d with_data=%d with_gaps=%d "
        "findings=%d kb_searches=%d posted=%d errors=%d cost=$%.4f",
        summary["clients_processed"],
        summary["clients_with_data"],
        summary["clients_with_gaps"],
        summary["total_findings"],
        summary["total_kb_searches"],
        summary["slack_posted"],
        summary["errors"],
        total_cost,
    )

    return summary


# ============================================================================
# SCHEDULER LOOP
# ============================================================================


async def run_learning_loop() -> None:
    """Background loop that runs manual message learning at 7:00 AM CT daily.

    Called via asyncio.create_task in the FastAPI lifespan.
    """
    await asyncio.sleep(90)  # Let services fully initialize

    while True:
        try:
            now = datetime.now(SCHEDULE_TZ)
            target = now.replace(hour=SCHEDULE_HOUR, minute=SCHEDULE_MINUTE, second=0, microsecond=0)
            if target <= now:
                target += timedelta(days=1)

            wait_seconds = (target - now).total_seconds()
            logger.info(
                "MANUAL_LEARNING | scheduler | next_run=%s wait_hours=%.1f",
                target.isoformat(),
                wait_seconds / 3600,
            )
            await asyncio.sleep(wait_seconds)

            result = await run_manual_learning()
            logger.info("MANUAL_LEARNING | scheduled_run_done | result=%s", result)

        except Exception as e:
            logger.exception("MANUAL_LEARNING | scheduled_run_failed")
            try:
                await notify_error(e, source="scheduler: manual_learning", context={"trigger_type": "manual-message-learning"})
            except Exception:
                pass
            await asyncio.sleep(3600)
