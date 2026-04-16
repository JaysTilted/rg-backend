"""Curated E2E scenarios for the Iron Automations conversation agent.

Each scenario seeds a conversation history via TestContext.seed_messages
and then issues a fresh lead turn. Scott's reply + classification are
captured via POST /testing/run against the live backend, which uses
MockGHLClient so nothing touches real GHL/Signal House.

The goal is not pass/fail assertions — it's fast human review of how
Scott handles the 10 classes of message we expect in production.
"""

from __future__ import annotations

from typing import Any

ENTITY_ID = "55b37690-0d29-4283-85c0-f1f8f5858788"  # Iron Automations

# Common seed: Scott's standard cold opener, picked up ~2h ago
_BAIT_OPENER_SEED = [
    {
        "role": "ai",
        "content": "Hey it's Scott, I build websites for HVAC in Texas. Noticed Blue Wood Heating and Air doesn't have one.",
        "hours_ago": 2.0,
    },
    {
        "role": "ai",
        "content": "I know it sounds crazy but I threw one together real quick. Want to see it?",
        "hours_ago": 2.0,
    },
]

_POR_OPENER_SEED = [
    {
        "role": "ai",
        "content": "If Blue Wood Heating and Air wants more HVAC jobs in Texas, I can help. Pay on results.",
        "hours_ago": 2.0,
    },
]


def _lead(first: str = "Test", last: str = "Lead", phone: str = "+15555551212") -> dict[str, Any]:
    return {
        "first_name": first,
        "last_name": last,
        "email": f"{first.lower()}.{last.lower()}@example.com",
        "phone": phone,
        "source": "E2E Framework",
    }


SCENARIOS: list[dict[str, Any]] = [
    {
        "id": "01-open-reply-yes",
        "category": "opener-reply",
        "description": "Bait opener → lead says 'yeah' → should send demo + move forward",
        "lead": _lead("Mike", "Hvac"),
        "context": {
            "seed_messages": _BAIT_OPENER_SEED,
            "tags": ["src:cold-sms", "hvac"],
        },
        "conversation": [{"message": "yeah", "expect_path": "reply"}],
    },
    {
        "id": "02-open-reply-i-have-one",
        "category": "opener-reply",
        "description": "Bait opener → lead says 'I have one' → pivot to improvement value",
        "lead": _lead("Sandra", "Plumbing", "+15555551213"),
        "context": {
            "seed_messages": _BAIT_OPENER_SEED,
            "tags": ["src:cold-sms", "plumbing"],
        },
        "conversation": [{"message": "i have one", "expect_path": "reply"}],
    },
    {
        "id": "03-price-question-early",
        "category": "pricing",
        "description": "Lead asks price right away — should answer directly ($297/mo)",
        "lead": _lead("Carla", "Hvac", "+15555551214"),
        "context": {
            "seed_messages": _BAIT_OPENER_SEED + [
                {"role": "lead", "content": "yeah sure", "hours_ago": 1.5},
                {"role": "ai", "content": "Cool. Quick question — are you looking for just the site or the full automation stack with follow-up + booking?", "hours_ago": 1.4},
            ],
            "tags": ["src:cold-sms", "hvac"],
        },
        "conversation": [{"message": "how much is this", "expect_path": "reply"}],
    },
    {
        "id": "04-ready-to-book",
        "category": "booking",
        "description": "Qualified lead says 'let's do it' — should use calendar tool",
        "lead": _lead("Ramon", "Roofing", "+15555551215"),
        "context": {
            "seed_messages": _BAIT_OPENER_SEED + [
                {"role": "lead", "content": "yeah", "hours_ago": 1.5},
                {"role": "ai", "content": "Nice. Quick question — roughly how many leads/mo are you getting now?", "hours_ago": 1.4},
                {"role": "lead", "content": "maybe 20 from word of mouth", "hours_ago": 1.3},
                {"role": "ai", "content": "Got it. I can show you how to 2-3x that. Want to hop on a 15-min call so I can walk you through it?", "hours_ago": 1.2},
            ],
            "tags": ["src:cold-sms", "roofing"],
        },
        "conversation": [{"message": "yeah sure, when works", "expect_path": "reply"}],
    },
    {
        "id": "05-objection-expensive",
        "category": "objection",
        "description": "Objection: too expensive — should handle without being pushy",
        "lead": _lead("Deb", "Electric", "+15555551216"),
        "context": {
            "seed_messages": _BAIT_OPENER_SEED + [
                {"role": "lead", "content": "whats the cost", "hours_ago": 1.5},
                {"role": "ai", "content": "Starts at $297/mo for site + follow-up + booking automation.", "hours_ago": 1.4},
            ],
            "tags": ["src:cold-sms", "electrician"],
        },
        "conversation": [{"message": "too expensive for me", "expect_path": "reply"}],
    },
    {
        "id": "06-objection-not-ready",
        "category": "objection",
        "description": "Soft no — 'not right now' — should nurture without dead-ending",
        "lead": _lead("Tom", "Painting", "+15555551217"),
        "context": {
            "seed_messages": _BAIT_OPENER_SEED,
            "tags": ["src:cold-sms", "painting"],
        },
        "conversation": [{"message": "not looking right now maybe later", "expect_path": "reply"}],
    },
    {
        "id": "07-opt-out-stop",
        "category": "opt-out",
        "description": "Lead says STOP — should trigger opt-out path, no further messages",
        "lead": _lead("Al", "Hvac", "+15555551218"),
        "context": {
            "seed_messages": _POR_OPENER_SEED,
            "tags": ["src:cold-sms", "hvac"],
        },
        "conversation": [{"message": "STOP", "expect_path": "opt_out"}],
    },
    {
        "id": "08-hostile",
        "category": "opt-out",
        "description": "Hostile reply — should transfer to human, not engage",
        "lead": _lead("Rick", "Landscape", "+15555551219"),
        "context": {
            "seed_messages": _POR_OPENER_SEED,
            "tags": ["src:cold-sms", "landscaping"],
        },
        "conversation": [{"message": "fuck off", "expect_path": "opt_out"}],
    },
    {
        "id": "09-wrong-fit-seo",
        "category": "qualification",
        "description": "Lead asks for SEO — we don't do that. Should scope or disqualify",
        "lead": _lead("Pat", "Hvac", "+15555551220"),
        "context": {
            "seed_messages": _BAIT_OPENER_SEED + [
                {"role": "lead", "content": "oh hey yeah", "hours_ago": 1.5},
            ],
            "tags": ["src:cold-sms", "hvac"],
        },
        "conversation": [{"message": "do you do seo too? thats what i really need", "expect_path": "reply"}],
    },
    {
        "id": "10-what-do-you-do",
        "category": "discovery",
        "description": "Vague lead — 'what do you do exactly' — should explain + invite",
        "lead": _lead("Ben", "Roofing", "+15555551221"),
        "context": {
            "seed_messages": _BAIT_OPENER_SEED,
            "tags": ["src:cold-sms", "roofing"],
        },
        "conversation": [{"message": "what do you do exactly", "expect_path": "reply"}],
    },
    {
        "id": "11-are-you-a-bot",
        "category": "ai-disclosure",
        "description": "Lead asks if Scott is a bot — ai_disclosure=reveal_when_asked should kick in",
        "lead": _lead("Joel", "Plumbing", "+15555551222"),
        "context": {
            "seed_messages": _BAIT_OPENER_SEED + [
                {"role": "lead", "content": "interesting", "hours_ago": 1.5},
            ],
            "tags": ["src:cold-sms", "plumbing"],
        },
        "conversation": [{"message": "wait are you a real person or a bot", "expect_path": "reply"}],
    },
    {
        "id": "12-followup-ghost",
        "category": "followup",
        "description": "Lead went silent after opener — Scott should send a followup (not a reply)",
        "lead": _lead("Quinn", "Hvac", "+15555551223"),
        "context": {
            "seed_messages": _BAIT_OPENER_SEED,
            "tags": ["src:cold-sms", "hvac"],
        },
        "conversation": [{"intent": "no response — ghost", "followups": 1, "followup_cadence": [6.0], "expect_path": "followup"}],
    },
]


def build_request() -> dict[str, Any]:
    """Return the POST /testing/run payload."""
    return {
        "entity_id": ENTITY_ID,
        "test_scenarios": SCENARIOS,
    }
