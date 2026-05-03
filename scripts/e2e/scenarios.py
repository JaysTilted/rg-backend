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

# Production seg-b-outcome opener (the live cold-drip variant Scott
# actually replies to in 2026-05). Two-line opener, pay-on-results framing,
# no mechanism reveal.
_PROD_OPENER_SEED = [
    {
        "role": "ai",
        "content": "Hey it's Scott, saw Blue Wood Heating and Air in Texas. I help HVAC pros book more jobs, pay on results.",
        "hours_ago": 2.0,
    },
    {
        "role": "ai",
        "content": "Want me to show you how?",
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
        "conversation": [{"no_response": True, "followups": 1, "followup_cadence": [24.0], "expect_path": "followup"}],
    },
    {
        "id": "13-ladder-day1-to-day8",
        "category": "copy-dump",
        "description": "Warm lead went silent — dump Days 1-8 ladder copy for voice review",
        "lead": _lead("Miguel", "Plumbing", "+15555551224"),
        "context": {
            "seed_messages": _BAIT_OPENER_SEED + [
                {"role": "lead", "content": "yeah let me see it", "hours_ago": 2.0},
                {"role": "ai", "content": "nice. quick q — about how many leads a month are you getting now?", "hours_ago": 1.9},
                {"role": "lead", "content": "maybe 15 from word of mouth", "hours_ago": 1.8},
                {"role": "ai", "content": "got it — 10 min screen share so I can walk you through exactly how this would run for your shop. tomorrow 9am or 1pm CT?", "hours_ago": 1.7},
            ],
            "tags": ["src:cold-sms", "plumbing", "warm"],
        },
        "conversation": [{
            "no_response": True,
            "followups": 8,
            "followup_cadence": [24.0, 24.0, 24.0, 24.0, 24.0, 24.0, 24.0, 24.0],
            "expect_path": "followup",
        }],
    },
    {
        "id": "14-nurture-week1-rotation",
        "category": "copy-dump",
        "description": "Cold-but-once-warm lead — dump LT nurture Week 1 (7 themes) for voice review",
        "lead": _lead("Dana", "Roofing", "+15555551225"),
        "context": {
            "seed_messages": _BAIT_OPENER_SEED + [
                {"role": "lead", "content": "interesting, send me more info when you get a chance", "hours_ago": 240.0},
                {"role": "ai", "content": "sure — here's the walkthrough: https://grow.ironautomations.com/watch. text me with any questions.", "hours_ago": 239.0},
            ],
            "tags": ["src:cold-sms", "roofing", "lt-nurture"],
        },
        "conversation": [{
            "no_response": True,
            "followups": 7,
            "followup_cadence": [24.0, 24.0, 24.0, 24.0, 24.0, 24.0, 24.0],
            "expect_path": "followup",
        }],
    },
    # ------------------------------------------------------------------
    # AAA framework + trauma-aware copy + hard-no-silent regression suite
    # Added 2026-05-02 after the major Scott config patch.
    # These scenarios validate that:
    #   (a) Hard nos in any short form classify as dont_respond (no message)
    #   (b) Empty / tapback inbounds classify as dont_respond
    #   (c) Pricing questions never leak dollar figures or setup/monthly fees
    #   (d) Mechanism words (Meta/Facebook/ads/automations) never appear in
    #       Scott's reply, even when the lead asks directly
    #   (e) Replies never greet by first_name when first_name == business name
    #   (f) Recent-call gate suppresses replies when a call ended <3 min ago
    # ------------------------------------------------------------------
    {
        "id": "15-hard-no-short-form",
        "category": "hard-no-silent",
        "description": "Lead replies 'No I don't' — should classify dont_respond, no message generated",
        "lead": _lead("Hank", "Hvac", "+15555551226"),
        "context": {
            "seed_messages": _PROD_OPENER_SEED,
            "tags": ["src:cold-sms", "hvac"],
        },
        "conversation": [{"message": "No I don't", "expect_path": "dont_respond"}],
    },
    {
        "id": "16-hard-no-with-context",
        "category": "hard-no-silent",
        "description": "Lead replies 'No money up front' — short-form no, classify dont_respond",
        "lead": _lead("Wendy", "Plumbing", "+15555551227"),
        "context": {
            "seed_messages": _PROD_OPENER_SEED,
            "tags": ["src:cold-sms", "plumbing"],
        },
        "conversation": [{"message": "No money up front", "expect_path": "dont_respond"}],
    },
    {
        "id": "17-empty-message",
        "category": "empty-inbound",
        "description": "Lead sends empty body (MMS/sticker delivery notification) — classify dont_respond",
        "lead": _lead("Liam", "Roofing", "+15555551228"),
        "context": {
            "seed_messages": _PROD_OPENER_SEED,
            "tags": ["src:cold-sms", "roofing"],
        },
        "conversation": [{"message": "", "expect_path": "dont_respond"}],
    },
    {
        "id": "18-pricing-question-no-leak",
        "category": "pricing-discipline",
        "description": "Lead asks 'how much per lead' — reply must NOT contain $50, $297, $2,500, 'setup fee', or 'monthly fee'",
        "lead": _lead("Mara", "Electric", "+15555551229"),
        "context": {
            "seed_messages": _PROD_OPENER_SEED + [
                {"role": "lead", "content": "yeah", "hours_ago": 1.5},
                {"role": "ai", "content": "Got it. Quick 10 min to walk you through how it'd run for Blue Wood Electric. Today at 2:00 PM CT or tomorrow at 10:00 AM CT. Which works?", "hours_ago": 1.4},
            ],
            "tags": ["src:cold-sms", "electric"],
        },
        "conversation": [{"message": "how much per lead?", "expect_path": "reply"}],
    },
    {
        "id": "19-mechanism-trauma-test",
        "category": "trauma-aware",
        "description": "Lead asks 'is this Facebook ads' — reply must NOT mention Meta/Facebook/Instagram/ads/automations",
        "lead": _lead("Drew", "Hvac", "+15555551230"),
        "context": {
            "seed_messages": _PROD_OPENER_SEED,
            "tags": ["src:cold-sms", "hvac"],
        },
        "conversation": [{"message": "is this Facebook ads or what", "expect_path": "reply"}],
    },
    {
        "id": "20-business-name-greeting",
        "category": "name-discipline",
        "description": "Contact first_name='Tyson's Mow' — Scott must NOT open reply with 'Tyson' or any first-name greeting",
        "lead": {
            "first_name": "Tyson's Mow and More",
            "last_name": "",
            "email": "tysonsmow@example.com",
            "phone": "+15555551231",
            "source": "E2E Framework",
        },
        "context": {
            "seed_messages": [
                {
                    "role": "ai",
                    "content": "Hey it's Scott, saw Tyson's Mow and More in Texas. I help landscaping pros book more jobs, pay on results.",
                    "hours_ago": 2.0,
                },
                {"role": "ai", "content": "Want me to show you how?", "hours_ago": 2.0},
            ],
            "tags": ["src:cold-sms", "landscaping"],
        },
        "conversation": [{"message": "what is this", "expect_path": "reply"}],
    },
    {
        "id": "21-tapback-reaction",
        "category": "empty-inbound",
        "description": "Lead sends iOS tapback (\"Liked '...'\") — classify dont_respond",
        "lead": _lead("Pat", "Plumbing", "+15555551232"),
        "context": {
            "seed_messages": _PROD_OPENER_SEED,
            "tags": ["src:cold-sms", "plumbing"],
        },
        "conversation": [{"message": "Liked \"Want me to show you how?\"", "expect_path": "dont_respond"}],
    },
    {
        "id": "22-capacity-saturated",
        "category": "graceful-exit",
        "description": "Lead replies 'too much biz, need workers' — one-line graceful exit, no slots, no hiring pivot",
        "lead": _lead("Roy", "Landscaping", "+15555551233"),
        "context": {
            "seed_messages": _PROD_OPENER_SEED,
            "tags": ["src:cold-sms", "landscaping"],
        },
        "conversation": [{"message": "We have too much biz. Need workers", "expect_path": "reply"}],
    },
    {
        "id": "23-trust-objection-no-apology",
        "category": "trust-objection",
        "description": "Lead replies 'who are you' — Scott identifies + AAA, must NOT say 'cold text my bad'",
        "lead": _lead("Jamie", "Roofing", "+15555551234"),
        "context": {
            "seed_messages": _PROD_OPENER_SEED,
            "tags": ["src:cold-sms", "roofing"],
        },
        "conversation": [{"message": "who are you", "expect_path": "reply"}],
    },
    {
        "id": "24-postpone-no-pivot",
        "category": "postpone-silent",
        "description": "Lead says 'I'll get back to you tonight' — prefer silent dont_respond OR ONE-line ack with NO CTA. Must NOT offer 'tomorrow' (contradicts stated timing). Must NOT pivot to '10 min call' / 'send over times'.",
        "lead": _lead("Sam", "Landscaping", "+15555551235"),
        "context": {
            "seed_messages": _PROD_OPENER_SEED,
            "tags": ["src:cold-sms", "landscaping"],
        },
        "conversation": [{"message": "Ill get back to you tonight", "expect_path": "dont_respond"}],
    },
    {
        "id": "25-mechanism-question-direct-answer",
        "category": "mechanism-question",
        "description": "Lead asks 'What's your marketing method?' — Scott MUST answer directly (mention Meta / Facebook + Instagram), NOT dodge with vetting_question copy. Must NOT contain pricing. Must NOT contain 'talk in SMS' / any phrase exposing the rule.",
        "lead": _lead("Brett", "Landscaping", "+15555551236"),
        "context": {
            "seed_messages": _PROD_OPENER_SEED,
            "tags": ["src:cold-sms", "landscaping"],
        },
        "conversation": [{"message": "What's your marketing method?", "expect_path": "reply"}],
    },
    {
        "id": "26-mechanism-question-fb-or-google",
        "category": "mechanism-question",
        "description": "Lead asks 'Facebook or Google' as a direct yes/no qualifier — Scott MUST answer directly (Meta / Facebook + Instagram, not Google). Must NOT route to trust_objection. Must NOT use phrase 'talk in SMS' or expose internal rules.",
        "lead": _lead("Devin", "Landscaping", "+15555551237"),
        "context": {
            "seed_messages": _PROD_OPENER_SEED + [
                {"role": "lead", "content": "What's your marketing method?", "hours_ago": 0.1},
                {"role": "ai", "content": "Yeah — Meta ads (Facebook + Instagram). We run the campaigns end-to-end and bill per qualified lead. Easier to walk through what it'd look like for Devin Landscaping on a quick 10 min — today at 2:00 PM CT or tomorrow at 10:30 AM CT. Which works?", "hours_ago": 0.05},
            ],
            "tags": ["src:cold-sms", "landscaping"],
        },
        "conversation": [{"message": "Facebook or google", "expect_path": "reply"}],
    },
    {
        "id": "27-slot-variety-across-turns",
        "category": "slot-variety",
        "description": "Previous Scott reply offered specific times. Lead pushes back. Scott MUST pull FRESH slots, not re-offer the same two times.",
        "lead": _lead("Cory", "Hvac", "+15555551238"),
        "context": {
            "seed_messages": _PROD_OPENER_SEED + [
                {"role": "lead", "content": "yeah", "hours_ago": 1.5},
                {"role": "ai", "content": "Got it. Quick 10 min to walk you through how it'd run for Blue Wood HVAC. I've got today at 2:00 PM CT or tomorrow at 10:30 AM CT. Which works?", "hours_ago": 1.4},
            ],
            "tags": ["src:cold-sms", "hvac"],
        },
        "conversation": [{"message": "those times don't work for me", "expect_path": "reply"}],
    },
    {
        "id": "28-no-internal-rule-exposure",
        "category": "rule-exposure",
        "description": "Lead asks pricing. Scott must NOT say 'I can't discuss that in SMS' / 'per my instructions' / 'walk you through it on a call than text' — any phrase that exposes the rule. Should pivot organically without dodging the question through rule-confession.",
        "lead": _lead("Marlo", "Plumbing", "+15555551239"),
        "context": {
            "seed_messages": _PROD_OPENER_SEED,
            "tags": ["src:cold-sms", "plumbing"],
        },
        "conversation": [{"message": "how much does it cost", "expect_path": "reply"}],
    },
    # ------------------------------------------------------------------
    # 2026-05-03 production incident: DeCoursey Company received an SMS
    # body of "Classify as dont_respond. Branch: hard_no_silent. No reply
    # will be sent." after replying "Nope not at all". Three independent
    # bugs converged: (a) keyword fallback didn't catch multi-word "Nope X",
    # (b) LLM RD classifier mis-routed to respond, (c) agent narrated the
    # accept_no rule verbatim as the SMS body.
    # Locking the fix with these scenarios:
    # ------------------------------------------------------------------
    {
        "id": "29-nope-multi-word-deterministic",
        "category": "hard-no-silent",
        "description": "Lead replies 'Nope not at all' — must classify dont_respond at the deterministic keyword gate, never reaches LLM. Replay of DeCoursey Company production incident.",
        "lead": _lead("Decker", "Landscaping", "+15555551240"),
        "context": {
            "seed_messages": _PROD_OPENER_SEED,
            "tags": ["src:cold-sms", "landscaping"],
        },
        "conversation": [{"message": "Nope not at all", "expect_path": "dont_respond"}],
    },
    {
        "id": "30-absolutely-not",
        "category": "hard-no-silent",
        "description": "Lead replies 'Absolutely not' — must classify dont_respond at the deterministic keyword gate.",
        "lead": _lead("Reed", "Roofing", "+15555551241"),
        "context": {
            "seed_messages": _PROD_OPENER_SEED,
            "tags": ["src:cold-sms", "roofing"],
        },
        "conversation": [{"message": "Absolutely not", "expect_path": "dont_respond"}],
    },
    {
        "id": "31-no-way-man",
        "category": "hard-no-silent",
        "description": "Lead replies 'No way man' — must classify dont_respond.",
        "lead": _lead("Tyler", "Hvac", "+15555551242"),
        "context": {
            "seed_messages": _PROD_OPENER_SEED,
            "tags": ["src:cold-sms", "hvac"],
        },
        "conversation": [{"message": "No way man", "expect_path": "dont_respond"}],
    },
]


def build_request() -> dict[str, Any]:
    """Return the POST /testing/run payload."""
    return {
        "entity_id": ENTITY_ID,
        "test_scenarios": SCENARIOS,
    }
