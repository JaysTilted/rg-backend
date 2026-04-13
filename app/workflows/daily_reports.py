"""Daily Client Reports — migrated from n8n workflow LYGmrAGdeAIZMrJr.

Runs once daily (8:45 AM America/Chicago). For every active client and personal bot:
1. Calls 4 Supabase RPCs + cross-DB queries to collect yesterday's stats
2. Upserts snapshot to client_daily_stats
3. Uses Claude (via OpenRouter) for AI digest (Mondays) and daily alerts
4. Posts to per-client Slack channels (creates channel if needed)
5. Personal bots all post to #personal-bots-reports

Triggered by: internal scheduler (run_reports_loop) or manual POST /workflows/daily-reports
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import re
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Any

from prefect import flow

from app.config import settings
from app.services.mattermost import (
    create_channel as create_slack_channel,
    find_channel_by_name,
    invite_to_channel,
    notify_error,
    post_message as post_slack_message,
)
from app.services.supabase_client import supabase

logger = logging.getLogger(__name__)

# TODO: Replace with Mattermost user ID for channel invites
RANDY_SLACK_USER_ID = ""

# Schedule: 8:45 AM America/Chicago daily
SCHEDULE_HOUR = 8
SCHEDULE_MINUTE = 45
SCHEDULE_TZ = ZoneInfo("America/Chicago")


# ============================================================================
# AI SYSTEM PROMPTS (copied verbatim from n8n workflow)
# ============================================================================

DIGEST_SYSTEM_PROMPT = """\
<role>
You are an AI agency owner's daily briefing analyst. Your job: look at pre-computed metric comparisons (yesterday vs 7-day average) and produce an actionable morning briefing.
</role>

<data_schema>
## DATA YOU RECEIVE
Each metric in `comparisons` has: metric name, yesterday's value, 7-day average, 30-day average, and % changes vs both (pre-computed — DO NOT recalculate).
You also get `last_7_days` with day-by-day values to spot multi-day trends.
</data_schema>

<metric_context>
## METRIC CONTEXT (important — understand what these measure)
- responseRate: The % of leads that REPLIED BACK to us after we contacted them. This is NOT our reply rate — it's the lead engagement rate. Higher = better (leads are responding). Lower = leads are ignoring us.
- transfer_count: Number of conversations where the AI FAILED to handle the lead and had to escalate to a human. Higher = worse (AI couldn't close or answer). An increase in transfers is ALWAYS bad (ALERT/WATCH). A decrease is neutral — do not highlight it as a WIN (it's the expected baseline).
- ai_booked_count: Bookings the AI closed directly without any human help. Higher = better.
- aiAssisted: Bookings where the AI engaged the lead but a human ultimately closed. Higher = better (AI did its job warming the lead).
- manual: Bookings where a human handled the lead without meaningful AI involvement. Higher = worse (means AI isn't doing its job).
- ai_involvement_pct: (ai_booked_count + aiAssisted) / total_bookings. Higher = better. Below 50% means the AI isn't contributing enough.
- stale_leads_14d: Leads that haven't been contacted in 14+ days. Higher = worse (leads going cold).
- cancelled_bookings: Bookings that were cancelled. Higher = worse.
- after_hours_sessions: Chat sessions that happened outside business hours. Higher = good (AI handles leads 24/7 so the business doesn't miss them).
- kb_sessions: Number of chat sessions where the AI used the knowledge base to answer questions. Higher = good (AI is using the KB). If transfers are high but KB usage is low, the KB probably needs updating.
- mctb_total: Total missed call text-backs sent yesterday. These are automatic texts sent to callers who called but nobody picked up. Higher = more call recovery happening.
- mctb_new_callers: How many of the missed call text-backs went to NEW callers (potential new leads). Higher = good.
- mctb_voicemails: How many missed callers left voicemails. Higher = more engaged callers.
- mctb_after_hours: How many missed calls happened after business hours. Higher = more 24/7 lead capture.
</metric_context>

<data_sufficiency>
## DATA SUFFICIENCY
- If `days_of_history` < 3: Return health_score: null, empty arrays, health_summary: "Building baseline — need 3+ days for analysis"
- If `days_of_history` 3-6: Only flag zero-activity on weekdays or extreme outliers. No trend analysis.
- If `days_of_history` >= 7: Full analysis.
</data_sufficiency>

<output_format>
## OUTPUT FORMAT
Return ONLY valid JSON (no markdown, no code fences):
{
  "health_score": <1-10 or null>,
  "health_trend": "<up|down|stable>",
  "health_summary": "<One punchy sentence — what's the headline?>",
  "alerts": [{"metric": "<name>", "detail": "<concise — include the pre-computed numbers>"}],
  "watch": [{"metric": "<name>", "detail": "<concise>"}],
  "wins": [{"metric": "<name>", "detail": "<concise>"}],
  "action_items": ["<specific actionable thing to check or do>"]
}
</output_format>

<classification_rules>
## CLASSIFICATION RULES (use the pre-computed pct_change)
- ALERT (red): pct_change <= -30%, OR zero on a weekday when avg > 0, OR 4+ day consecutive decline in last_7_days
- WATCH (yellow): pct_change between -15% and -30%, OR 3-day declining trend
- WIN (green): pct_change >= +25% AND the metric is meaningful (not trivially small). Only flag clear wins — if borderline, omit.
- Prefer fewer, high-confidence items over many borderline ones. If you're unsure whether something qualifies, leave it out.
- EXCEPTION: transfer_count and cancelled_bookings are INVERTED — increases are BAD (ALERT/WATCH), decreases are neutral (NOT a win — it's the expected baseline).
- EXCEPTION: stale_leads_14d increases are BAD, decreases are GOOD
- EXCEPTION: manual booking increases are BAD (means AI isn't closing)
</classification_rules>

<health_score>
## HEALTH SCORE (weight volume metrics 2x)
Primary metrics (2x weight): total_leads, total_bookings
Secondary metrics (1x weight): responseRate, ai_involvement_pct, after_hours_sessions

Scoring guide:
- 1-3: Multiple primary metrics down >30%, or zero weekday activity
- 4-5: Primary metrics significantly below avg (1+ alert on volume)
- 6: Mixed — some metrics up, some down, net neutral
- 7: Most metrics near or above avg
- 8-9: Primary metrics all above avg, no alerts
- 10: New highs on multiple primary metrics
</health_score>

<health_summary>
## HEALTH SUMMARY
Write like a headline — be specific, not generic.
BAD: "Consistent performance with moderate lead volume" (says nothing)
GOOD: "Leads and bookings at weekly lows — only 1 booking on a Friday"
GOOD: "Strong booking day offset by declining lead engagement"
The summary should make the owner immediately know: was yesterday good or bad?
</health_summary>

<action_items>
## ACTION ITEMS
0-3 specific things the owner should check or do TODAY based on the data.
Examples:
- "Check why lead engagement dropped to 25% — leads aren't responding to the AI's outreach"
- "Review the 3 manual bookings — were these leads the AI should have handled?"
- "Lead source shifted to Staff Call only — verify Meta Ads are running"
- "No action needed — solid day across the board"
Only suggest actions that are supported by the data. Never suggest generic things like "monitor trends" or "keep up the good work".
</action_items>

<rules>
## RULES
- Use the pre-computed pct_change values. Do NOT recalculate averages yourself.
- Include numbers in every alert/watch/win: "Down 50% vs 7d avg (7 vs 13.9)"
- 0-3 items per category. Empty arrays are preferred over weak observations.
- If `hasQualification` is false: NEVER mention qualified leads.
- Weekend context: if isWeekend is true, zero/low activity is WATCH not ALERT. On weekends, health_trend should be "stable" unless activity is anomalously high or low even by weekend standards. The 7d avg includes weekdays, so naturally everything will be lower on a weekend — don't penalize for that.
- For AI metrics: low ai_involvement_pct (< 50%) with manual bookings high = the AI isn't converting, flag this.
- KB + transfers: if transfer_count is high AND kb_sessions is low compared to total_sessions, flag that the knowledge base needs updating (AI is escalating because it doesn't have answers).
- Lead sources: `lead_sources` shows where leads came from yesterday (e.g., Meta Ads, Staff Call, Website). If a major source drops to zero or shifts dramatically, flag it. If one source dominates, mention it.
- Service interests: `service_interests` shows which services leads are interested in (e.g., "Lips", "Botox", "PDRN Facial"). Each has leads, replied, and bookings counts. If one service has high leads but low bookings, or if interest distribution shifts, flag it. This helps the owner understand which services are driving conversions.
- MCTB (Missed Call Text-Back): if mctb_total > 0, comment on call recovery. High new callers = good lead capture. High after-hours = system working when humans aren't. If mctb_total is zero and it was previously active, that's worth noting.
- NEVER use the word "revenue" anywhere in your output. Do not analyze, mention, or reference revenue in any form — not as a metric, not colloquially, not in descriptions. Say "bookings" or "closed deals" instead.
</rules>"""

ALERT_SYSTEM_PROMPT = """\
<role>
You are a silent watchdog for an AI lead conversion agency. You monitor daily client metrics and ONLY speak up when something is genuinely wrong — something the agency owner needs to act on TODAY.

Your default state is SILENCE. Most days, nothing is wrong. Do NOT notify for:
- Minor dips (under 30% below average)
- Normal daily variance
- Metrics that are slightly below average but within a reasonable range
- Weekends — ANY level of reduced activity on weekends is normal (see WEEKEND section)
- New clients with less than 5 days of history (not enough baseline)
- A single secondary metric dipping while primary metrics (leads, bookings) are healthy (see HIERARCHY)
</role>

<metric_hierarchy>
## METRIC HIERARCHY (this is critical)
PRIMARY metrics — the ones that actually matter for the business:
  → total_leads: Are leads coming in?
  → total_bookings: Are bookings happening?

SECONDARY metrics — these only matter if PRIMARY metrics are ALSO bad:
  → responseRate, ai_booked_count, ai_involvement_pct, transfer_count, manual, kb_sessions, etc.

RULE: If leads AND bookings are at or near their averages, do NOT alert on any secondary metric. A response rate drop with healthy bookings means leads are still converting — no action needed. Only flag secondary metrics when they coincide with primary metric problems.

IMPORTANT: Each primary metric is evaluated independently. A 50%+ drop in leads alone IS alert-worthy even if bookings are fine — the pipeline is drying up. A 50%+ drop in bookings alone IS alert-worthy even if leads are fine — conversion is broken. The hierarchy rule ONLY suppresses secondary metrics, never a primary metric.
</metric_hierarchy>

<alert_criteria>
## WHEN TO NOTIFY (weekdays only, unless stated otherwise)
- A primary metric (leads or bookings) dropped 50%+ vs BOTH the 7d and 30d averages
- Zero leads or zero bookings when the client normally has activity
- The AI system is failing: transfers spiked 3x+ AND (AI booked 0 when it normally books several OR ai_involvement_pct dropped below 30%)
- A multi-day trend: 3+ consecutive days of declining leads or bookings (visible in last_7_days)
- Lead sources completely shifted or disappeared (e.g., primary ad source went to zero)
- Stale leads exploded (doubled or more vs average)
</alert_criteria>

<metric_context>
## METRIC CONTEXT
- responseRate: % of leads that replied back. Secondary metric — only matters if bookings are also down.
- transfer_count: AI failed and escalated to human. INVERTED. Only alarming at 3x+ normal AND bookings down.
- ai_booked_count: AI closed bookings without human help. Higher = better.
- manual: Human closed without AI. INVERTED.
- ai_involvement_pct: (ai_booked + aiAssisted) / total_bookings. Below 30% with multiple bookings = AI not working.
- stale_leads_14d: Leads not contacted in 14+ days. INVERTED.
- cancelled_bookings: INVERTED.
- kb_sessions: AI using knowledge base. If transfers high + KB low = KB needs updating.
</metric_context>

<data_schema>
## DATA YOU RECEIVE
- `comparisons`: Each metric has yesterday's value, 7d average, 30d average, and % change vs both.
- `last_7_days`: Day-by-day values for trend detection.
- `lead_sources`: Where leads came from yesterday.
- `service_interests`: What services leads are interested in.
- `dayOfWeek`, `isWeekend`: Context for expected activity level.
- `days_of_history`: How much baseline data exists.
</data_schema>

<weekend_handling>
## WEEKEND HANDLING
On weekends (Saturday/Sunday), ALWAYS return notify: false. Zero activity, low activity, reduced metrics — all completely normal on weekends. There is nothing actionable about weekend numbers. The owner cannot and should not act on weekend data. Save alerts for weekdays when the data is meaningful.

The ONLY exception: if there is strong evidence of a technical system failure that also appeared on the preceding weekdays (e.g., the last_7_days show 3+ weekdays of decline culminating in the weekend). A weekend-only dip is NEVER worth alerting.
</weekend_handling>

<output_format>
## OUTPUT FORMAT
Return ONLY valid JSON (no markdown, no code fences):
{
  "notify": true/false,
  "reason": "<One concise sentence explaining what's wrong. Only include if notify is true. If notify is false, set to null.>"
}
</output_format>

<rules>
## CRITICAL RULES
- Default to notify: false. The bar for notification is HIGH.
- Never notify about positive things (wins, improvements, good days).
- Never use the word "revenue" in any form.
- If you're unsure whether to notify, DON'T. Err on the side of silence.
- The reason must be specific and actionable — include the key numbers.
- Keep the reason under 150 characters. It should read like a text message alert.
- On weekends, notify: false (unless preceding weekdays confirm systemic failure).
</rules>"""


# ============================================================================
# METRIC LABELS (for Slack formatting)
# ============================================================================

_METRIC_LABELS: dict[str, str] = {
    "total_leads": "Leads",
    "total_bookings": "Bookings",
    "ai_booked_count": "AI Bookings",
    "cancelled_bookings": "Cancellations",
    "stale_leads_14d": "Stale Leads (14d)",
    "responseRate": "Lead Engagement",
    "repliedCount": "Replies",
    "aiAssisted": "AI-Assisted",
    "manual": "Manual Bookings",
    "total_sessions": "Chat Sessions",
    "follow_up_count": "Follow-Ups",
    "after_hours_sessions": "After-Hours Sessions",
    "transfer_count": "Transfers to Human",
    "ai_involvement_pct": "AI Involvement",
    "kb_sessions": "KB Usage",
    "mctb_total": "Missed Call Text-Backs",
    "mctb_new_callers": "MCTB New Callers",
    "mctb_voicemails": "MCTB Voicemails",
    "mctb_after_hours": "MCTB After-Hours",
}


# ============================================================================
# HELPERS
# ============================================================================


def _pct(n: float, d: float) -> str:
    """Format percentage, returning '—' if denominator is zero."""
    return f"{n / d * 100:.1f}%" if d > 0 else "\u2014"


def _fmt_time(h: float | None) -> str:
    """Format hours into human-readable time string."""
    if h is None:
        return "\u2014"
    if h < 1:
        return f"{round(h * 60)}m"
    if h < 24:
        return f"{h:.1f}h"
    return f"{h / 24:.1f}d"


def _ml(key: str) -> str:
    """Get human-readable metric label."""
    return _METRIC_LABELS.get(key, key)


def _get_field(stats: dict, path: str) -> float:
    """Navigate a dot-separated path in a nested dict, returning 0 if missing."""
    parts = path.split(".")
    v: Any = stats
    for p in parts:
        if isinstance(v, dict):
            v = v.get(p)
        else:
            return 0
    return v if isinstance(v, (int, float)) else 0


def _pct_change(yesterday: float, avg: float) -> float:
    """Calculate percentage change from average."""
    if avg == 0:
        return 100.0 if yesterday > 0 else 0.0
    return round(((yesterday - avg) / avg) * 1000) / 10


def _parse_ai_json(raw: str) -> dict | None:
    """Parse JSON from AI response (may have markdown fences)."""
    match = re.search(r"\{[\s\S]*\}", raw)
    if match:
        try:
            return json.loads(match.group(0))
        except json.JSONDecodeError:
            return None
    return None


# ============================================================================
# STATS COLLECTION
# ============================================================================


async def _collect_entity_stats(
    entity: dict[str, Any],
    cfg: dict[str, Any],
    is_client: bool,
) -> dict[str, Any]:
    """Collect yesterday's stats for a single entity via 4 Supabase RPCs.

    Returns the combined stats dict: {main, chat, tools, missedCall, crossDb}.
    """
    entity_id = entity["id"]
    table_name = entity.get("chat_history_table_name", "")

    # Parse business hours for chat metrics
    biz_start = 9
    biz_end = 17
    if entity.get("business_hours_start"):
        try:
            biz_start = int(str(entity["business_hours_start"]).split(":")[0])
        except (ValueError, IndexError):
            pass
    if entity.get("business_hours_end"):
        try:
            biz_end = int(str(entity["business_hours_end"]).split(":")[0])
        except (ValueError, IndexError):
            pass

    # RPC 1: dashboard_main_stats (Main DB)
    main_task = supabase.rpc("main", "dashboard_main_stats", {
        "p_client_ids": [entity_id],
        "p_start": cfg["day_start"],
        "p_end": cfg["day_end"],
        "p_sources": None,
        "p_reply_channels": None,
        "p_has_booking": None,
        "p_reply_status": None,
        "p_qualification": None,
        "p_form_interest": None,
        "p_repeat_unique": None,
        "p_contact_ids": None,
        "p_booking_channels": None,
        "p_booking_status": None,
        "p_after_hours": None,
    })

    # RPC 2: dashboard_chat_metrics (Chat DB) — only if table exists
    async def _chat_metrics() -> dict:
        if not table_name:
            return {}
        return await supabase.rpc("chat", "dashboard_chat_metrics", {
            "p_table_configs": [{
                "table_name": table_name,
                "timezone": entity.get("timezone", "America/New_York"),
                "biz_start": biz_start,
                "biz_end": biz_end,
            }],
            "p_start": cfg["day_start"],
            "p_end": cfg["day_end"],
            "p_channels": None,
            "p_contact_ids": None,
            "p_lead_ids": None,
        })

    # RPC 3: dashboard_tool_stats (Chat DB)
    tool_task = supabase.rpc("chat", "dashboard_tool_stats", {
        "p_client_ids": [entity_id],
        "p_start": cfg["day_start"],
        "p_end": cfg["day_end"],
        "p_channels": None,
        "p_contact_ids": None,
        "p_lead_ids": None,
    })

    # RPC 4: dashboard_missed_call_stats (Chat DB)
    mc_task = supabase.rpc("chat", "dashboard_missed_call_stats", {
        "p_client_ids": [entity_id],
        "p_start": cfg["day_start"],
        "p_end": cfg["day_end"],
        "p_lead_ids": None,
    })

    main_stats, chat_metrics, tool_stats, mc_stats = await asyncio.gather(
        main_task, _chat_metrics(), tool_task, mc_task,
        return_exceptions=True,
    )

    # Handle exceptions from gather
    if isinstance(main_stats, Exception):
        logger.warning("DAILY_REPORTS | rpc failed | entity=%s rpc=dashboard_main_stats error=%s", entity_id, main_stats)
        main_stats = {}
    if isinstance(chat_metrics, Exception):
        logger.warning("DAILY_REPORTS | rpc failed | entity=%s rpc=dashboard_chat_metrics error=%s", entity_id, chat_metrics)
        chat_metrics = {}
    if isinstance(tool_stats, Exception):
        logger.warning("DAILY_REPORTS | rpc failed | entity=%s rpc=dashboard_tool_stats error=%s", entity_id, tool_stats)
        tool_stats = {}
    if isinstance(mc_stats, Exception):
        logger.warning("DAILY_REPORTS | rpc failed | entity=%s rpc=dashboard_missed_call_stats error=%s", entity_id, mc_stats)
        mc_stats = {}

    # Cross-DB metrics (only for entities with a chat table)
    cross_db = await _compute_cross_db_metrics(entity, cfg, is_client)

    return {
        "main": main_stats or {},
        "chat": chat_metrics or {},
        "tools": tool_stats or {},
        "missedCall": mc_stats or {},
        "crossDb": cross_db,
    }


async def _compute_cross_db_metrics(
    entity: dict[str, Any],
    cfg: dict[str, Any],
    is_client: bool,
) -> dict[str, Any]:
    """Compute cross-database metrics: response rate, booking attribution, etc.

    Mirrors n8n's cross-DB section in "Collect Client Stats".
    """
    defaults: dict[str, Any] = {
        "responseRate": 0, "repliedCount": 0, "repliedAndBookedCount": 0,
        "qualifiedAndBookedCount": 0, "aiAssisted": 0, "manual": 0,
        "msgsPerBooking": None, "msgsPerNonBooking": None,
        "channelLeads": {}, "channelReplied": {},
    }

    table_name = entity.get("chat_history_table_name", "")
    if not table_name:
        return defaults

    # Old client_leads / client_bookings tables only have client_id FK
    if not is_client:
        return defaults

    entity_id = entity["id"]
    fk = "client_id"

    # Step A: Get leads and bookings from Main DB (parallel)
    leads_path = (
        f"/client_leads?select=id,ghl_contact_id,qualification_status,source,"
        f"form_interest,last_reply_datetime&{fk}=eq.{entity_id}"
        f"&created_at=gte.{cfg['day_start']}&created_at=lte.{cfg['day_end']}"
    )
    bookings_path = (
        f"/client_bookings?select=ghl_contact_id,lead_id,ai_booked_directly,booking_type"
        f"&{fk}=eq.{entity_id}&created_at=gte.{cfg['day_start']}"
        f"&created_at=lte.{cfg['day_end']}&lead_id=not.is.null"
    )

    try:
        lead_rows, booking_rows = await asyncio.gather(
            supabase.rest_get("main", leads_path),
            supabase.rest_get("main", bookings_path),
        )
    except Exception as e:
        logger.warning("DAILY_REPORTS | cross_db main queries failed | entity=%s error=%s", entity_id, e)
        return defaults

    # Build sets
    lead_id_array = [r["id"] for r in lead_rows if r.get("id")]
    lead_contacts = list({r["ghl_contact_id"] for r in lead_rows if r.get("ghl_contact_id")})
    booked_lead_ids = [r["lead_id"] for r in booking_rows if r.get("lead_id")]
    booked_contacts = list({r["ghl_contact_id"] for r in booking_rows if r.get("ghl_contact_id")})
    booked_lead_id_set = set(booked_lead_ids)
    non_booked_lead_ids = [lid for lid in lead_id_array if lid not in booked_lead_id_set]
    non_booked_contacts = [c for c in lead_contacts if c not in set(booked_contacts)]

    # AI-Assisted vs Manual attribution
    # Prefer booking_type when set (new rows), fall back to cross-DB derivation (old rows)
    ai_assisted = 0
    manual = 0

    typed_rows = [r for r in booking_rows if r.get("booking_type")]
    untyped_non_ai = [
        r for r in booking_rows
        if not r.get("booking_type") and not r.get("ai_booked_directly")
    ]

    # New rows: read directly from booking_type
    for row in typed_rows:
        bt = row["booking_type"]
        if bt == "ai_assisted":
            ai_assisted += 1
        elif bt == "manual":
            manual += 1

    # Old rows (NULL booking_type): cross-DB derivation
    non_ai_lead_ids = [r["lead_id"] for r in untyped_non_ai if r.get("lead_id")]
    if non_ai_lead_ids:
        try:
            replied_leads = await supabase.rest_get(
                "main",
                f"/client_leads?select=id&id=in.({','.join(non_ai_lead_ids)})"
                f"&last_reply_datetime=not.is.null",
            )
            replied_lead_ids = {r["id"] for r in replied_leads}
        except Exception:
            replied_lead_ids = set()

        for row in untyped_non_ai:
            if row.get("lead_id") and row["lead_id"] in replied_lead_ids:
                ai_assisted += 1
            else:
                manual += 1

    # Step B: Chat DB RPCs (parallel)
    engagement_task = (
        supabase.rpc("chat", "dashboard_contact_engagement", {
            "p_contact_ids": lead_contacts,
            "p_table_names": [table_name],
            "p_lead_ids": lead_id_array,
        }) if lead_id_array else _noop_dict()
    )
    booked_engagement_task = (
        supabase.rpc("chat", "dashboard_contact_engagement", {
            "p_contact_ids": booked_contacts,
            "p_table_names": [table_name],
            "p_lead_ids": booked_lead_ids,
        }) if booked_lead_ids else _noop_dict()
    )
    booking_msgs_task = (
        supabase.rpc("chat", "dashboard_booking_msgs", {
            "p_booked_ids": booked_contacts,
            "p_non_booked_ids": non_booked_contacts,
            "p_table_names": [table_name],
            "p_booked_lead_ids": booked_lead_ids,
            "p_non_booked_lead_ids": non_booked_lead_ids,
        }) if (booked_lead_ids or non_booked_lead_ids) else _noop_dict()
    )

    try:
        engagement, booked_engagement, booking_msgs = await asyncio.gather(
            engagement_task, booked_engagement_task, booking_msgs_task,
            return_exceptions=True,
        )
    except Exception as e:
        logger.warning("DAILY_REPORTS | cross_db chat RPCs failed | entity=%s error=%s", entity_id, e)
        return defaults

    # Safely extract from results (may be Exception objects)
    if isinstance(engagement, Exception):
        engagement = {}
    if isinstance(booked_engagement, Exception):
        booked_engagement = {}
    if isinstance(booking_msgs, Exception):
        booking_msgs = {}

    replied_count = (engagement.get("matched_count") or 0) if isinstance(engagement, dict) else 0
    response_rate = round((replied_count / len(lead_rows)) * 1000) / 10 if lead_rows else 0
    replied_and_booked = (booked_engagement.get("matched_count") or 0) if isinstance(booked_engagement, dict) else 0

    # Qualified leads that booked
    qualified_and_booked = sum(
        1 for r in lead_rows
        if r.get("id") and r.get("qualification_status") == "qualified" and r["id"] in booked_lead_id_set
    )

    return {
        "responseRate": response_rate,
        "repliedCount": replied_count,
        "repliedAndBookedCount": replied_and_booked,
        "qualifiedAndBookedCount": qualified_and_booked,
        "aiAssisted": ai_assisted,
        "manual": manual,
        "msgsPerBooking": (booking_msgs.get("msgs_per_booking") if isinstance(booking_msgs, dict) else None),
        "msgsPerNonBooking": (booking_msgs.get("msgs_per_non_booking") if isinstance(booking_msgs, dict) else None),
        "channelLeads": (engagement.get("by_channel") or {}) if isinstance(engagement, dict) else {},
        "channelReplied": (engagement.get("by_channel_replied") or {}) if isinstance(engagement, dict) else {},
    }


async def _noop_dict() -> dict:
    return {}


# ============================================================================
# COMPARISONS (7-day and 30-day averages)
# ============================================================================

# Metrics to track (path into stats dict)
_METRIC_PATHS: list[tuple[str, str]] = [
    ("total_leads", "main.total_leads"),
    ("total_bookings", "main.total_bookings"),
    ("ai_booked_count", "main.ai_booked_count"),
    ("cancelled_bookings", "main.cancelled_bookings"),
    ("stale_leads_14d", "main.stale_leads_14d"),
    ("responseRate", "crossDb.responseRate"),
    ("repliedCount", "crossDb.repliedCount"),
    ("aiAssisted", "crossDb.aiAssisted"),
    ("manual", "crossDb.manual"),
    ("total_sessions", "chat.total_sessions"),
    ("follow_up_count", "chat.follow_up_count"),
    ("after_hours_sessions", "chat.after_hours_sessions"),
    ("transfer_count", "tools.transfer_count"),
    ("kb_sessions", "tools.kb_unique_sessions"),
    ("mctb_total", "missedCall.total"),
    ("mctb_new_callers", "missedCall.newCallers"),
    ("mctb_voicemails", "missedCall.voicemails"),
    ("mctb_after_hours", "missedCall.afterHours"),
]


def _build_comparisons(
    yesterday_stats: dict[str, Any],
    history: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Build metric comparisons with 7-day and 30-day averages.

    Returns (comparisons, last_7_summary) where comparisons is a list of dicts
    with metric, yesterday, avg_7d, pct_change_7d, avg_30d, pct_change_30d.
    """
    last_7 = list(reversed(history[:7]))  # chronological order
    last_30 = history[:30]

    def avg_n(records: list[dict], path: str) -> float:
        if not records:
            return 0
        total = sum(_get_field(r.get("stats", {}), path) for r in records)
        return round((total / len(records)) * 10) / 10

    comparisons = []
    for metric_name, path in _METRIC_PATHS:
        y_val = _get_field(yesterday_stats, path)
        a7 = avg_n(last_7, path)
        a30 = avg_n(last_30, path)
        comparisons.append({
            "metric": metric_name,
            "yesterday": y_val,
            "avg_7d": a7,
            "pct_change_7d": _pct_change(y_val, a7),
            "avg_30d": a30,
            "pct_change_30d": _pct_change(y_val, a30),
        })

    # AI involvement percentage (computed metric)
    main = yesterday_stats.get("main", {})
    total_bookings = main.get("total_bookings", 0) or 0
    ai_booked = main.get("ai_booked_count", 0) or 0
    xdb = yesterday_stats.get("crossDb", {})
    ai_assisted = xdb.get("aiAssisted", 0) or 0
    ai_involvement = ai_booked + ai_assisted
    ai_pct = round((ai_involvement / total_bookings) * 100) if total_bookings > 0 else 0
    comparisons.append({
        "metric": "ai_involvement_pct",
        "yesterday": ai_pct,
        "avg_7d": None,
        "pct_change_7d": None,
        "avg_30d": None,
        "pct_change_30d": None,
    })

    # Day-by-day summary for trend detection
    last_7_summary = [
        {
            "date": r.get("snapshot_date", ""),
            "leads": _get_field(r.get("stats", {}), "main.total_leads"),
            "bookings": _get_field(r.get("stats", {}), "main.total_bookings"),
            "responseRate": _get_field(r.get("stats", {}), "crossDb.responseRate"),
            "sessions": _get_field(r.get("stats", {}), "chat.total_sessions"),
            "mctb": _get_field(r.get("stats", {}), "missedCall.total"),
        }
        for r in last_7
    ]

    return comparisons, last_7_summary


# ============================================================================
# AI CALLS (Azure GPT-4.1 primary, OpenRouter fallback)
# ============================================================================


async def _call_llm(
    system_prompt: str,
    user_prompt: str,
    max_tokens: int,
) -> tuple[str, float]:
    """Call Azure GPT-4.1 first, then OpenRouter fallback. Returns (content, cost_usd)."""
    import httpx
    import openai

    from app.config import settings

    if settings.azure_openai_api_key:
        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                resp = await client.post(
                    settings.azure_openai_base_url.rstrip("/") + "/chat/completions",
                    json={
                        "model": settings.azure_openai_model,
                        "messages": [
                            {"role": "system", "content": system_prompt},
                            {"role": "user", "content": user_prompt},
                        ],
                        "temperature": 0,
                        "max_completion_tokens": max_tokens,
                    },
                    headers={
                        "api-key": settings.azure_openai_api_key,
                        "Content-Type": "application/json",
                    },
                )
                resp.raise_for_status()
                result = resp.json()
                content = result.get("choices", [{}])[0].get("message", {}).get("content", "") or ""
                return content, 0.0
        except Exception as e:
            logger.warning("DAILY_REPORTS | Azure call failed | error=%s", e)

    if not settings.openrouter_api_key:
        logger.warning("DAILY_REPORTS | No Azure or OpenRouter key configured")
        return "", 0.0

    try:
        client = openai.AsyncOpenAI(
            base_url="https://openrouter.ai/api/v1",
            api_key=settings.openrouter_api_key,
        )
        resp = await client.chat.completions.create(
            model="anthropic/claude-sonnet-4-6",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            max_tokens=max_tokens,
            temperature=0,
        )
        content = resp.choices[0].message.content or ""
        cost = getattr(resp.usage, "cost", 0.0) or 0.0 if resp.usage else 0.0
        return content, cost
    except Exception as e:
        logger.warning("DAILY_REPORTS | OpenRouter call failed | error=%s", e)
        return "", 0.0


async def _run_digest_ai(data_payload: str) -> tuple[dict[str, Any], float]:
    """Run the weekly digest AI analysis. Returns (parsed_result, cost_usd)."""
    raw, cost = await _call_llm(DIGEST_SYSTEM_PROMPT, data_payload, max_tokens=1024)
    result = _parse_ai_json(raw)
    if result:
        return result, cost
    return {
        "health_score": None,
        "health_trend": "stable",
        "health_summary": "Analysis unavailable",
        "alerts": [],
        "watch": [],
        "wins": [],
        "action_items": [],
    }, cost


async def _run_alert_ai(data_payload: str) -> tuple[dict[str, Any], float]:
    """Run the daily alert check. Returns ({notify: bool, reason: str|None}, cost_usd)."""
    raw, cost = await _call_llm(ALERT_SYSTEM_PROMPT, data_payload, max_tokens=256)
    result = _parse_ai_json(raw)
    if result:
        return result, cost
    return {"notify": False, "reason": None}, cost


# ============================================================================
# SLACK FORMATTING (matches n8n's Analyze & Format node exactly)
# ============================================================================


def _format_digest_message(
    entity: dict[str, Any],
    stats: dict[str, Any],
    ai: dict[str, Any],
    cfg: dict[str, Any],
    is_client: bool,
) -> str:
    """Format the full Slack digest message (Monday only)."""
    entity_name = entity.get("name") or "Unknown"
    entity_id = entity["id"]
    has_qualification = bool(entity.get("service_config"))
    journey_stage = (entity.get("journey_stage") or "").lower()

    main = stats.get("main", {})
    tools = stats.get("tools", {})
    chat = stats.get("chat", {})
    xdb = stats.get("crossDb", {})
    mc = stats.get("missedCall", {})

    # Format date
    d = datetime.strptime(cfg["snapshot_date"], "%Y-%m-%d")
    months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    formatted_date = f"{months[d.month - 1]} {d.day}, {d.year}"

    # Health display
    trend_icon = {"up": "\u2191", "down": "\u2193"}.get(ai.get("health_trend", ""), "\u2192")
    health_score = f"{ai['health_score']}/10" if ai.get("health_score") is not None else "N/A"
    div = "\u2500" * 36

    msg = f"\U0001f4cb *Weekly Digest \u2014 {entity_name} \u2014 {formatted_date}*\n"
    msg += f"Health: *{health_score}* ({trend_icon}) \u2014 {ai.get('health_summary', 'N/A')}\n"

    # SECTION 1: AI INSIGHTS
    alerts = ai.get("alerts", [])
    watch = ai.get("watch", [])
    wins = ai.get("wins", [])
    actions = ai.get("action_items", [])

    if alerts or watch or wins:
        msg += f"\n{div}\n"
        if alerts:
            msg += "\U0001f534 *ALERTS*\n"
            for a in alerts:
                msg += f"\u2022 *{_ml(a.get('metric', ''))}:* {a.get('detail', '')}\n"
            if watch or wins:
                msg += "\n"
        if watch:
            msg += "\U0001f7e1 *WATCH*\n"
            for w in watch:
                msg += f"\u2022 *{_ml(w.get('metric', ''))}:* {w.get('detail', '')}\n"
            if wins:
                msg += "\n"
        if wins:
            msg += "\U0001f7e2 *WINS*\n"
            for w in wins:
                msg += f"\u2022 *{_ml(w.get('metric', ''))}:* {w.get('detail', '')}\n"

    # SECTION 1b: ACTION ITEMS
    if actions:
        msg += f"\n{div}\n"
        msg += "\u2705 *ACTION ITEMS*\n"
        for action in actions:
            msg += f"\u2022 {action}\n"

    # SECTION 2: BILLING (clients only)
    if is_client:
        ai_direct_count = main.get("ai_booked_count", 0) or 0
        total_bookings = main.get("total_bookings", 0) or 0
        ai_assisted_count = xdb.get("aiAssisted", 0) or 0
        manual_count = xdb.get("manual", 0) or 0

        msg += f"\n{div}\n"
        msg += "\U0001f4b0 *BILLING*\n\n"

        billing = entity.get("billing_config") or {}
        model = billing.get("model", "tiered")

        if journey_stage == "performance":
            if model == "flat":
                rate = billing.get("rate", 0) or 0
                total_pay = total_bookings * rate
                msg += f"Total Bookings: {total_bookings} (*${total_pay}*)\n"
                msg += f"\u2003Rate: ${rate}/booking (flat)\n"
            else:  # tiered
                ai_direct_rate = billing.get("ai_direct_rate", 0) or 0
                ai_assisted_rate = billing.get("ai_assisted_rate", 0) or 0
                manual_rate = billing.get("manual_rate", 0) or 0
                ai_direct_pay = ai_direct_count * ai_direct_rate
                ai_assisted_pay = ai_assisted_count * ai_assisted_rate
                manual_pay = manual_count * manual_rate
                total_pay = ai_direct_pay + ai_assisted_pay + manual_pay
                msg += f"Total Bookings: {total_bookings} (*${total_pay}*)\n"
                msg += f"\u2003AI Direct: {ai_direct_count} (${ai_direct_pay} @ ${ai_direct_rate}/ea)\n"
                msg += f"\u2003AI Assisted: {ai_assisted_count} (${ai_assisted_pay} @ ${ai_assisted_rate}/ea)\n"
                if manual_rate > 0:
                    msg += f"\u2003Manual: {manual_count} (${manual_pay} @ ${manual_rate}/ea)\n"
                else:
                    msg += f"\u2003Manual: {manual_count} ($0)\n"
        elif journey_stage == "retainer":
            amount = billing.get("amount", 0) or 0
            msg += f"Retainer: *${amount}/mo*\n"
            msg += f"Total Bookings: {total_bookings}\n"
        elif journey_stage == "trial":
            ts = (billing.get("trial_start") or "N/A").split("T")[0]
            te = (billing.get("trial_end") or "N/A").split("T")[0]
            msg += f"Trial: {ts} \u2192 {te}\n"
            msg += f"Total Bookings: {total_bookings}\n"
        else:
            msg += f"Model: {entity.get('journey_stage', 'Unknown')} | Bookings: {total_bookings}\n"

    # SECTION 3: VOLUME & LEADS
    leads = main.get("total_leads", 0) or 0
    total_bookings = main.get("total_bookings", 0) or 0
    lead_linked = main.get("lead_linked_bookings", 0) or 0
    unique_leads = main.get("unique_lead_contacts", 0) or leads
    est_revenue = main.get("est_revenue", 0) or 0
    ai_direct_count = main.get("ai_booked_count", 0) or 0
    ai_assisted_count = xdb.get("aiAssisted", 0) or 0
    manual_count = xdb.get("manual", 0) or 0

    msg += f"\n{div}\n"
    msg += "\U0001f4ca *VOLUME & LEADS*\n\n"
    msg += f"Leads: *{leads}* | Bookings: *{total_bookings}* | Conversion: *{_pct(lead_linked, leads)}*\n"
    if est_revenue > 0:
        msg += f"Client Revenue (est): ${round(est_revenue):,}\n"
    if has_qualification:
        msg += f"Qualified: {main.get('qualified_leads', 0) or 0}\n"
    msg += f"Returning: {main.get('returning_leads', 0) or 0} | Stale (14d): {main.get('stale_leads_14d', 0) or 0}\n"
    msg += f"Cancelled: {main.get('cancelled_bookings', 0) or 0} | Repeat: {main.get('repeat_bookings', 0) or 0} | After-Hours Bookings: {main.get('after_hours_bookings', 0) or 0}\n"

    ai_involvement_total = ai_direct_count + ai_assisted_count
    msg += f"\n_Attribution:_ AI Direct {ai_direct_count} | AI Assisted {ai_assisted_count} | Manual {manual_count} ({_pct(ai_involvement_total, total_bookings)} AI)\n"

    sources = main.get("lead_sources", []) or []
    if sources:
        msg += "\n_Sources:_\n"
        for s in sources:
            msg += f"\u2003{s.get('source', 'Unknown')}: {s.get('leads', 0)} leads \u2192 {s.get('bookings', 0)} bookings ({_pct(s.get('bookings', 0), s.get('leads', 0))})\n"

    interests = main.get("service_interests", []) or []
    if interests:
        msg += "\n_Interests:_\n"
        for s in interests:
            msg += f"\u2003{s.get('service', 'Unknown')}: {s.get('leads', 0)} leads \u2192 {s.get('replied', 0)} replied \u2192 {s.get('bookings', 0)} booked\n"

    # SECTION 4: AI PERFORMANCE
    sessions = chat.get("total_sessions", 0) or 0
    msg += f"\n{div}\n"
    msg += "\U0001f916 *AI PERFORMANCE*\n\n"
    msg += f"Lead Engagement: {xdb.get('responseRate', 0)}% ({xdb.get('repliedCount', 0)}/{unique_leads} replied)\n"
    msg += f"Transfers: {tools.get('transfer_count', 0) or 0} ({_pct(tools.get('transfer_count', 0) or 0, sessions)} of sessions)\n"
    fu_sessions = chat.get("follow_up_sessions", 0) or 0
    avg_fu = chat.get("avg_follow_ups")
    msg += f"Follow-Up Rate: {_pct(fu_sessions, sessions)} ({fu_sessions}/{sessions}) | Avg: {f'{avg_fu:.1f}' if avg_fu else '\u2014'}/convo\n"
    kb_sessions = tools.get("kb_unique_sessions", 0) or 0
    msg += f"KB Usage: {_pct(kb_sessions, sessions)} ({kb_sessions}/{sessions})\n"
    msg += f"Cancellation Rate: {_pct(main.get('cancelled_bookings', 0) or 0, total_bookings)}\n"
    drip_pos = chat.get("drip_reply_avg_position")
    if drip_pos is not None:
        msg += f"Avg Reply Drip #: {drip_pos:.1f}\n"

    # SECTION 5: CONVERSION
    msg += f"\n{div}\n"
    msg += "\U0001f4c8 *CONVERSION*\n\n"
    msg += f"Reply \u2192 Booking: {_pct(xdb.get('repliedAndBookedCount', 0), xdb.get('repliedCount', 0))} ({xdb.get('repliedAndBookedCount', 0)}/{xdb.get('repliedCount', 0)})\n"
    if has_qualification:
        msg += f"Qualified \u2192 Booking: {_pct(xdb.get('qualifiedAndBookedCount', 0), main.get('qualified_leads', 0) or 0)} ({xdb.get('qualifiedAndBookedCount', 0)}/{main.get('qualified_leads', 0) or 0})\n"
    msg += f"Avg Time to Book: {_fmt_time(main.get('avg_time_to_book_hours'))}\n"
    mpb = xdb.get("msgsPerBooking")
    mpnb = xdb.get("msgsPerNonBooking")
    mpb_str = f"{mpb:.1f}" if mpb is not None else "\u2014"
    mpnb_str = f"{mpnb:.1f}" if mpnb is not None else "\u2014"
    msg += f"Msgs/Booking: {mpb_str} | Msgs/Non-Booking: {mpnb_str}\n"
    msg += f"After-Hours Responses: {chat.get('after_hours_responses', 0) or 0}\n"

    # SECTION 6: CHAT & TOOLS
    msg += f"\n{div}\n"
    msg += "\U0001f4ac *CHAT & TOOLS*\n\n"
    msg += f"Sessions: {sessions} | Follow-Ups: {chat.get('follow_up_count', 0) or 0} drips | After-Hours: {chat.get('after_hours_sessions', 0) or 0}\n"

    tools_arr = tools.get("tools", []) or []
    total_tool_calls = tools.get("total_calls", 0) or 0
    if total_tool_calls > 0:
        msg += f"\nTools ({total_tool_calls} calls):\n"
        for t in tools_arr[:8]:
            msg += f"\u2003{t.get('tool_name', 'unknown')}: {t.get('count', 0)} ({_pct(t.get('count', 0), total_tool_calls)})\n"

    # SECTION 7: MISSED CALL TEXT-BACK
    mctb_total = mc.get("total", 0) or 0
    if mctb_total > 0 or (mc.get("newCallers", 0) or 0) > 0:
        msg += f"\n{div}\n"
        msg += "\U0001f4f1 *MISSED CALL TEXT-BACK*\n\n"
        msg += f"Text-Backs Sent: *{mctb_total}*\n"
        msg += f"New Callers: {mc.get('newCallers', 0) or 0} | Existing: {mc.get('existingCallers', 0) or 0}\n"
        msg += f"Voicemails: {mc.get('voicemails', 0) or 0} | After-Hours: {mc.get('afterHours', 0) or 0}\n"

    # DASHBOARD LINK
    msg += f"\n{div}\n"
    msg += f"\U0001f517 <https://app.bookmyleads.ai/clients/{entity_id}|View Dashboard>\n"

    return msg


def _format_alert_message(
    entity_name: str,
    reason: str,
    formatted_date: str,
) -> str:
    """Format a daily alert Slack message."""
    return f"\U0001f6a8 *Alert \u2014 {entity_name} \u2014 {formatted_date}*\n{reason}"


# ============================================================================
# SLACK CHANNEL MANAGEMENT
# ============================================================================


async def _ensure_slack_channel(
    entity: dict[str, Any],
    is_client: bool,
) -> str | None:
    """Ensure a Slack channel exists for a client. Creates if needed.

    Returns the channel ID, or None if Slack is not configured.
    """
    entity_id = entity["id"]
    entity_name = entity.get("name") or "unknown"

    # Build channel name: slug-entityid (max 80 chars)
    slug = re.sub(r"[^a-z0-9\s-]", "", entity_name.lower())
    slug = re.sub(r"\s+", "-", slug).strip("-")[:60]
    channel_name = f"{slug}-{entity_id}"[:80]

    channel_id = await create_slack_channel(channel_name)
    if channel_id:
        await invite_to_channel(channel_id, RANDY_SLACK_USER_ID)
        if is_client:
            try:
                await supabase.update_client_field(entity_id, "slack_report_channel_id", channel_id)
            except Exception:
                logger.warning("DAILY_REPORTS | failed to store channel_id | entity=%s", entity_id)
    return channel_id


# ============================================================================
# PER-ENTITY PROCESSING
# ============================================================================


async def _process_entity(
    entity: dict[str, Any],
    cfg: dict[str, Any],
    is_client: bool,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Process a single entity: collect stats, AI analysis, Slack posting."""
    entity_id = entity["id"]
    entity_name = entity.get("name") or "Unknown"
    entity_type = "client" if is_client else "internal"

    try:
        logger.info("DAILY_REPORTS | processing | entity=%s type=%s", entity_name, entity_type)

        # 1. Collect stats
        stats = await _collect_entity_stats(entity, cfg, is_client)

        logger.info(
            "DAILY_REPORTS | stats_collected | entity=%s leads=%s bookings=%s sessions=%s",
            entity_name,
            _get_field(stats, "main.total_leads"),
            _get_field(stats, "main.total_bookings"),
            _get_field(stats, "chat.total_sessions"),
        )

        # 2. Store snapshot
        try:
            await supabase.upsert_daily_stats(
                entity_id=entity_id,
                snapshot_date=cfg["snapshot_date"],
                stats=stats,
            )
        except Exception:
            logger.warning("DAILY_REPORTS | upsert failed | entity=%s", entity_name, exc_info=True)

        # 3. Historical context + comparisons
        history = await supabase.get_daily_stats_history(entity_id)
        days_of_history = len(history)
        comparisons, last_7_summary = _build_comparisons(stats, history)

        # Lead sources + service interests for AI
        main = stats.get("main", {})
        lead_sources = [
            {"source": s.get("source", ""), "leads": s.get("leads", 0), "bookings": s.get("bookings", 0)}
            for s in (main.get("lead_sources") or [])
        ]
        service_interests = [
            {"service": s.get("service", ""), "leads": s.get("leads", 0),
             "replied": s.get("replied", 0), "bookings": s.get("bookings", 0)}
            for s in (main.get("service_interests") or [])
        ]

        # Shared data payload for AI
        data_payload = json.dumps({
            "entity": {"name": entity_name, "type": entity_type},
            "hasQualification": bool(entity.get("service_config")),
            "dayOfWeek": cfg["day_of_week"],
            "isWeekend": cfg["is_weekend"],
            "days_of_history": days_of_history,
            "comparisons": comparisons,
            "lead_sources": lead_sources,
            "service_interests": service_interests,
            "last_7_days": last_7_summary,
        })

        # Format date for Slack
        d = datetime.strptime(cfg["snapshot_date"], "%Y-%m-%d")
        months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
        formatted_date = f"{months[d.month - 1]} {d.day}, {d.year}"

        # 4. AI Digest (Monday only)
        slack_message: str | None = None
        total_ai_cost = 0.0
        if cfg["is_monday"]:
            ai_analysis, digest_cost = await _run_digest_ai(data_payload)
            total_ai_cost += digest_cost
            slack_message = _format_digest_message(entity, stats, ai_analysis, cfg, is_client)
            logger.info(
                "DAILY_REPORTS | digest_generated | entity=%s health=%s trend=%s",
                entity_name,
                ai_analysis.get("health_score"),
                ai_analysis.get("health_trend"),
            )

        # 5. AI Alert (every day)
        alert_message: str | None = None
        alert_result, alert_cost = await _run_alert_ai(data_payload)
        total_ai_cost += alert_cost
        if alert_result.get("notify"):
            alert_message = _format_alert_message(entity_name, alert_result["reason"], formatted_date)
            logger.info("DAILY_REPORTS | alert_triggered | entity=%s reason=%s", entity_name, alert_result["reason"])

        # 6. Resolve Slack channel (skip in dry_run)
        channel_id: str | None = None
        if not dry_run:
            if is_client:
                channel_id = entity.get("slack_report_channel_id")
                if not channel_id:
                    channel_id = await _ensure_slack_channel(entity, is_client)
            else:
                # Personal bots: hardcoded #personal-bots-reports
                channel_id = await find_channel_by_name("personal-bots-reports")

        # 7. Post to Slack (skip in dry_run)
        posted_digest = False
        posted_alert = False
        if dry_run:
            if slack_message:
                logger.info("DAILY_REPORTS | dry_run | would_post_digest | entity=%s len=%d", entity_name, len(slack_message))
            if alert_message:
                logger.info("DAILY_REPORTS | dry_run | would_post_alert | entity=%s reason=%s", entity_name, alert_result.get("reason", ""))
        elif channel_id:
            if slack_message:
                try:
                    await post_slack_message(channel_id, slack_message)
                    posted_digest = True
                except Exception:
                    logger.warning("DAILY_REPORTS | slack_digest_failed | entity=%s", entity_name, exc_info=True)
            if alert_message:
                try:
                    await post_slack_message(channel_id, alert_message)
                    posted_alert = True
                except Exception:
                    logger.warning("DAILY_REPORTS | slack_alert_failed | entity=%s", entity_name, exc_info=True)
        else:
            if slack_message or alert_message:
                logger.warning("DAILY_REPORTS | no_slack_channel | entity=%s", entity_name)

        logger.info(
            "DAILY_REPORTS | entity_done | entity=%s digest=%s alert=%s channel=%s",
            entity_name, posted_digest, posted_alert, bool(channel_id),
        )

        return {
            "entity": entity_name,
            "type": entity_type,
            "stats_collected": True,
            "digest_posted": posted_digest,
            "alert_posted": posted_alert,
            "alert_notify": alert_result.get("notify", False),
        }

    except Exception as e:
        logger.error("DAILY_REPORTS | entity_error | entity=%s: %s", entity_name, e)
        raise


# ============================================================================
# MAIN FLOW
# ============================================================================


@flow(name="daily-reports", retries=0)
async def run_daily_reports(dry_run: bool = False) -> dict[str, Any]:
    """Main entry point — processes all clients and bots.

    Called by the scheduler loop or manually via POST /workflows/daily-reports.
    If dry_run=True, skips Slack posts and channel creation (for testing).
    """
    now = datetime.now(timezone.utc)
    yesterday = now - timedelta(days=1)
    snapshot_date = yesterday.strftime("%Y-%m-%d")

    cfg = {
        "snapshot_date": snapshot_date,
        "day_start": snapshot_date + "T00:00:00.000Z",
        "day_end": snapshot_date + "T23:59:59.999Z",
        "day_of_week": yesterday.strftime("%A"),
        "is_weekend": yesterday.weekday() in (5, 6),
        "is_monday": now.weekday() == 0,
    }

    logger.info(
        "DAILY_REPORTS | starting | snapshot=%s day=%s is_monday=%s is_weekend=%s",
        snapshot_date, cfg["day_of_week"], cfg["is_monday"], cfg["is_weekend"],
    )

    # Fetch all entities
    try:
        clients, bots = await asyncio.gather(
            supabase.get_active_clients(),
            supabase.get_active_bots(),
        )
    except Exception:
        logger.exception("DAILY_REPORTS | failed to fetch entities")
        return {"error": "Failed to fetch entities"}

    logger.info("DAILY_REPORTS | entities_found | clients=%d bots=%d", len(clients), len(bots))

    if dry_run:
        logger.info("DAILY_REPORTS | dry_run=True — Slack posts will be skipped")

    # Process all entities concurrently (semaphore limits parallel work)
    MAX_CONCURRENT = 5
    sem = asyncio.Semaphore(MAX_CONCURRENT)

    async def _safe_process(entity: dict, is_client: bool) -> dict:
        async with sem:
            try:
                return await _process_entity(entity, cfg, is_client=is_client, dry_run=dry_run)
            except Exception:
                name = entity.get("name", entity.get("id", "unknown"))
                logger.exception("DAILY_REPORTS | entity_failed | entity=%s", name)
                return {"entity": name, "type": "client" if is_client else "internal", "error": True}

    tasks = [_safe_process(c, is_client=True) for c in clients]
    tasks += [_safe_process(b, is_client=False) for b in bots]
    results = await asyncio.gather(*tasks)

    summary = {
        "snapshot_date": snapshot_date,
        "clients_processed": sum(1 for r in results if r.get("type") == "client" and not r.get("error")),
        "bots_processed": sum(1 for r in results if r.get("type") == "internal" and not r.get("error")),
        "errors": sum(1 for r in results if r.get("error")),
        "digests_posted": sum(1 for r in results if r.get("digest_posted")),
        "alerts_posted": sum(1 for r in results if r.get("alert_posted")),
    }

    logger.info(
        "DAILY_REPORTS | complete | clients=%d bots=%d errors=%d digests=%d alerts=%d",
        summary["clients_processed"], summary["bots_processed"],
        summary["errors"], summary["digests_posted"], summary["alerts_posted"],
    )

    return summary


# ============================================================================
# SCHEDULER LOOP
# ============================================================================


async def run_reports_loop() -> None:
    """Background loop that runs daily reports at SCHEDULE_HOUR:SCHEDULE_MINUTE America/Chicago.

    Called via asyncio.create_task in the FastAPI lifespan.
    Calculates time until next scheduled run, sleeps, then executes.
    """
    await asyncio.sleep(60)  # Let services fully initialize

    while True:
        try:
            now = datetime.now(SCHEDULE_TZ)
            # Next scheduled run in Chicago time
            target = now.replace(hour=SCHEDULE_HOUR, minute=SCHEDULE_MINUTE, second=0, microsecond=0)
            if target <= now:
                target += timedelta(days=1)

            wait_seconds = (target - now).total_seconds()
            logger.info(
                "DAILY_REPORTS | scheduler | next_run=%s wait_hours=%.1f",
                target.isoformat(), wait_seconds / 3600,
            )
            await asyncio.sleep(wait_seconds)

            # Run the reports
            result = await run_daily_reports()
            logger.info("DAILY_REPORTS | scheduled_run_done | result=%s", result)

        except Exception as e:
            logger.exception("DAILY_REPORTS | scheduled_run_failed")
            try:
                await notify_error(e, source="scheduler: daily_reports", context={"trigger_type": "daily-reports"})
            except Exception:
                pass
            # Wait an hour before retrying on failure
            await asyncio.sleep(3600)
