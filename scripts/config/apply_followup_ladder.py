"""Generate + apply the 8-day followup ladder + 56-day LT nurture for Iron Automations.

Target audience: leads who responded positively to the cold opener but didn't book.
- Positions 1-8  : aggressive daily ladder to push the Discovery Call booking.
- Positions 9-64 : soft daily nurture (7 weekly themes × 8 weeks) keeping the thread warm.

The chain stops automatically on any of:
- Appointment booked (skip_if_upcoming_appointment=true)
- Lead disqualified / opt-out / stop-bot (skip_if_disqualified=true)
- Position > cadence length (chain_complete)

Runs as a dry-run printer by default — pipe the SQL into psql or pass --apply
to run it via Supabase MCP pattern (prints the UPDATE; caller executes).

Usage:
    python3 scripts/config/apply_followup_ladder.py                 # prints JSON + SQL
    python3 scripts/config/apply_followup_ladder.py --json-only     # just dump JSON patch
"""

from __future__ import annotations

import argparse
import json
from typing import Any

ENTITY_ID = "55b37690-0d29-4283-85c0-f1f8f5858788"

# ---------------------------------------------------------------------------
# 8-day ladder — warm lead, went silent after positive engagement.
# Goal: book the 10-min Discovery Call.
# ---------------------------------------------------------------------------
LADDER_POSITIONS: list[dict[str, Any]] = [
    {
        "position": 1,
        "timing_label": "Day 1",
        "goal": "Bump — re-engage without assuming opener variant.",
        "approach": (
            "Short, casual, properly capitalized. Re-engage neutrally without referring "
            "to website-vs-results pitch (we don't know which opener variant the lead saw). "
            "Reference [business] by name. "
            "Example: 'Hey — still want to see how this''d work for [business]? Takes 10 mins.'"
        ),
    },
    {
        "position": 2,
        "timing_label": "Day 2",
        "goal": "Proof — drop a concrete win from a similar business.",
        "approach": (
            "One sentence of proof + one sentence invite. No links. "
            "Example: 'Just wrapped one for another [industry] shop last week — site + funnel live in 3 days. Want to see yours?'"
        ),
    },
    {
        "position": 3,
        "timing_label": "Day 3",
        "goal": "Preempt the 'too busy' objection.",
        "approach": (
            "Acknowledge they're slammed, reframe the call as time-saving, not time-costing. "
            "Example: 'If you're slammed I get it — this actually saves you time once it's running. 10 mins to walk you through it.'"
        ),
    },
    {
        "position": 4,
        "timing_label": "Day 4",
        "goal": "Pain dig — reopen discovery with a specific question.",
        "approach": (
            "Pick ONE: lead flow, follow-up speed, or booking friction. Ask which is the biggest bottleneck. "
            "Example: 'What's the biggest bottleneck right now — lead flow, follow-up speed, or booking?'"
        ),
    },
    {
        "position": 5,
        "timing_label": "Day 5",
        "goal": "Outcome framing — paint the ROI in one line.",
        "approach": (
            "Name a real result number and tie it back to their business. "
            "Example: 'For another [industry] shop this added 6-8 extra jobs a month. Want me to show you how it'd run for [business]?'"
        ),
    },
    {
        "position": 6,
        "timing_label": "Day 6",
        "goal": "Lower the commitment — reduce friction to the call.",
        "approach": (
            "Reframe as a zero-pitch demo. Emphasize brevity. Do NOT ask a discovery question here — that's Day 4's job. "
            "Example: '10 min screen share, no pitch. You'll see exactly how it'd run for your shop.'"
        ),
    },
    {
        "position": 7,
        "timing_label": "Day 7",
        "goal": "Direct interest check — is this still on their radar.",
        "approach": (
            "Ask plainly if tightening up leads/follow-up is still something they care about. "
            "Example: 'Is tightening up leads / follow-up still on your radar?'"
        ),
    },
    {
        "position": 8,
        "timing_label": "Day 8",
        "goal": "Graceful out — invite them to re-engage on their timing.",
        "approach": (
            "Close with dignity. No pressure. Offer a clean door to come back through. "
            "Example: 'I'll stop bugging you unless you say otherwise. If timing's not right, just text me when it is.'"
        ),
    },
]

# ---------------------------------------------------------------------------
# LT nurture — 7 weekly themes × 8 weeks = 56 positions (9..64).
# Day-of-week rotation (Mon..Sun mapped to offset 0..6 within each week).
# Each position gets a theme + a week number — the agent produces fresh copy.
# ---------------------------------------------------------------------------
LT_THEMES = [
    ("Stat / Industry Benchmark", "Drop one punchy industry stat or benchmark that lands for service-business owners. No links."),
    ("Mini Case", "One sentence naming a similar business + the outcome. Keep it concrete and believable."),
    ("Question", "Ask a thoughtful question about their business — not generic, tied to what they told you before."),
    ("Workflow Tip", "Share a tiny actionable tip they can apply today even without us. Builds goodwill."),
    ("Offer Reminder", "Lightly remind them the $297/mo offer exists and the Discovery Call is still 10 mins."),
    ("Soft Check", "A genuine pulse check — 'how's it going' style, no ask."),
    ("Content / Reset", "Share the video link or a fresh angle to restart the conversation."),
]

# Per-theme placeholder examples, reused across all 8 weeks. Aggregate volume
# isn't high enough to justify 56 hand-written scripts — the LLM riffs from
# the approach string + current lead context.
LT_PLACEHOLDERS = {
    0: "Example: 'Quick one — 72% of service leads never get a reply in under 5 mins. Costs the business real money. Want to see how we close that gap?'",
    1: "Example: 'An HVAC shop we set up in Austin went from 6 jobs/mo to 14 in ~60 days. No ad-spend increase. Want to see his setup?'",
    2: "Example: 'Curious — what's the #1 thing you'd fix in your lead flow if you had a magic wand?'",
    3: "Example: 'Tip — if your missed-call textback is longer than 2 lines, reply rate drops hard. Keep it short.'",
    4: "Example: 'Site + automation stack is still $297/mo, no contract. 10 mins on a call and I can show you exactly what it'd run like for you.'",
    5: "Example: 'Hey — how's things going? Any movement on the lead-flow side?'",
    6: "Example: 'Dropped a short walkthrough video if you're curious: https://grow.ironautomations.com/watch — no sign-up, no pitch.'",
}


def build_lt_positions() -> list[dict[str, Any]]:
    """Generate 56 LT-nurture positions (weeks 1-8, daily Mon-Sun).

    Weeks 1-4 are value-only (no offer pushes — build trust first). The
    Friday position (day_idx=4) gets swapped to a Workflow Tip instead of
    Offer Reminder for those weeks. Weeks 5-8 keep the standard rotation
    where Friday = Offer Reminder (4 total offer pushes across the LT phase).
    """
    positions: list[dict[str, Any]] = []
    pos_num = 9
    for week in range(1, 9):  # Week 1..8
        for day_idx, (theme_label, approach) in enumerate(LT_THEMES):
            # Suppress offer pushes in weeks 1-4 — replace Friday's Offer
            # Reminder (day_idx=4) with a value-only Workflow Tip variant.
            if day_idx == 4 and week <= 4:
                effective_label = "Workflow Tip (no-offer week)"
                effective_approach = (
                    "Share a tiny actionable tip the lead can apply this week even "
                    "without us. Builds goodwill. NO mention of $297 or our offer "
                    "— this is a trust-building week. "
                    "Example: 'Tip — if your booking confirmation doesn''t include "
                    "the calendar invite, no-show rate jumps. Auto-attach it on send.'"
                )
            else:
                effective_label = theme_label
                effective_approach = f"{approach} {LT_PLACEHOLDERS[day_idx]}"
            positions.append({
                "position": pos_num,
                "timing_label": f"Week {week} {_DOW_LABELS[day_idx]}",
                "goal": f"{effective_label} — week {week}. Keep it alive without pressuring.",
                "approach": effective_approach,
            })
            pos_num += 1
    return positions


_DOW_LABELS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


# ---------------------------------------------------------------------------
# Re-engagement angles — a short library the agent can also draw on.
# ---------------------------------------------------------------------------
RE_ENGAGEMENT_ANGLES = [
    {"context": "Lead went silent after showing initial interest",
     "approach": "Refer back to the specific thing they reacted to. No generic nudges."},
    {"context": "Lead mentioned being busy / slammed",
     "approach": "Reframe the call as net time-saving. Emphasize 10-min duration and zero-pitch demo."},
    {"context": "Lead asked about price and stalled",
     "approach": "Acknowledge the price, restate the $297/mo starting point, and point to outcome not cost."},
    {"context": "Lead said they have a site already",
     "approach": "Do NOT re-pitch the site. Pivot to follow-up automation + booking speed — that's where most of the leak is."},
    {"context": "Lead is in LT nurture phase (cold for 8+ days)",
     "approach": "No hard ask. Share value (stat, tip, mini-case) and leave the door open. Re-open conversation on their terms."},
    {"context": "Lead asked a specific question we already answered",
     "approach": "Acknowledge we covered it, summarize in one line, ask if anything else is holding them back."},
    {"context": "Seasonal / timing sensitivity",
     "approach": "Tie the nudge to a plausible business rhythm (end of month, slow season, post-holiday) without being formulaic."},
]


def build_patch() -> dict[str, Any]:
    ladder = LADDER_POSITIONS
    lt = build_lt_positions()
    positions = ladder + lt
    assert len(positions) == 64, f"expected 64 positions, got {len(positions)}"

    timings = ["1 day"] * 64

    return {
        "positions_enabled": True,
        "follow_up_positions": positions,
        "re_engagement_angles": RE_ENGAGEMENT_ANGLES,
        "cadence_timing": {
            "customized": True,
            "timings": timings,
        },
        "max_offer_pushes": {
            "enabled": True,
            "max": 3,
            "prompt": (
                "During the 8-day ladder you may push the Discovery Call up to {max} times. "
                "During LT nurture (Week 1+) push the offer at most once per week and only on the "
                "Offer Reminder day."
            ),
        },
        "skip_if_upcoming_appointment": True,
        "skip_if_disqualified": True,
        "no_finality_language": {
            "enabled": True,
            "prompt": "Never sound like you're closing the door. Always leave an opening to reply.",
        },
        "timing_followup_rules": {
            "enabled": True,
            "prompt": (
                "Respect the lead's timezone (America/Chicago default) and the setter's send_window. "
                "If the due_at falls outside the window, the scheduler pushes it forward automatically — "
                "write copy that still reads naturally at the adjusted send time."
            ),
        },
    }


def build_sql(patch: dict[str, Any]) -> str:
    # Nest jsonb_set calls so adjacent follow_up.sections fields
    # (followup_context, tone_overrides, etc.) are preserved.
    expr = "system_config"
    for key, value in patch.items():
        lit = json.dumps(value).replace("'", "''")
        expr = (
            f"jsonb_set({expr}, "
            f"'{{setters,setter_1,conversation,follow_up,sections,{key}}}'::text[], "
            f"'{lit}'::jsonb, true)"
        )
    return (
        "UPDATE rg_backend.entities\n"
        f"SET system_config = {expr}\n"
        f"WHERE id = '{ENTITY_ID}';"
    )


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--json-only", action="store_true", help="print only the JSON patch")
    args = ap.parse_args()

    patch = build_patch()

    if args.json_only:
        print(json.dumps(patch, indent=2))
        return 0

    print("-- Generated patch applies 8-day ladder + 56-day LT nurture (64 positions)")
    print(f"-- Target entity: {ENTITY_ID}")
    print(f"-- Positions: {len(patch['follow_up_positions'])}")
    print(f"-- Cadence:   {len(patch['cadence_timing']['timings'])} × 1 day")
    print(f"-- Angles:    {len(patch['re_engagement_angles'])}")
    print()
    print(build_sql(patch))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
