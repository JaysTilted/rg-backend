"""Test: P1-Only Reactivation with Better Model (Sonnet)

Compares current Flash Lite P1 vs Sonnet P1 for the "single message + follow-up handoff" approach.
P1 here is a beefier single message that combines personal callback + offer/price mention.

This is a TEST ONLY — no production code is modified. Delete this file to undo everything.

Usage: docker compose exec rg-backend python -m app.test_p1_only
"""

import asyncio
import json
import os
import time

from app.services.ai_client import start_ai_clients, set_ai_context, clear_ai_context, chat, MODELS
from app.models import TokenUsage
from app.workflows.reactivation import (
    _ROLE_SMS, _VOICE, _ANTI_HALLUCINATION, _BANNED_PATTERNS, _GLOBAL_RULES,
    check_banned_patterns, scan_sensitivity,
)

# ── Enhanced P1 prompt for standalone use ──
# Combines: personal callback + offer awareness + price mention (optional)
# This is the ONE message that needs to get a reply.

_P1_STANDALONE = """\
## YOUR TASK: SINGLE REACTIVATION MESSAGE

You are writing ONE message to someone who hasn't been in touch for over a month. \
This is your only shot to get them to reply. Make it count.

Your message should do TWO things:
1. Reference something SPECIFIC from the conversation — what the LEAD said, asked, \
or was dealing with. This proves you actually remember them.
2. Give them a reason to reply NOW — a relevant offer, a new option they haven't \
heard about, availability, or a genuine question about their situation.

THINGS TO NEVER REFERENCE:
- Price objections ("too expensive") — skip it
- Bad experiences or complaints — skip it
- Family health, treatments, or costs — skip it
If the only details are sensitive, use Service Interest or the general treatment area.

HOW TO USE OFFERS:
- If Current Offers has something relevant to this lead's interest, weave it in \
naturally. Don't announce it like a sale — mention it like "oh by the way, we \
actually have a [offer] right now."
- If no offers are relevant, mention the service price casually instead.
- If neither fits, just ask a compelling question about their situation.

GOOD EXAMPLES (diverse industries):
- "You mentioned the shingles blew off in that storm — did your insurance end up \
covering anything? We're doing free inspections right now if you want someone to \
come take a look." (roofing — references conversation + weaves in offer)
- "You were asking about whitening for the coffee stains a while back. We actually \
have the take-home kits on sale for $149 right now, way gentler than those strips \
you tried. Want me to set one aside for you?" (dental — references concern + offer + question)
- "Last time we talked you said you wanted to start after the holidays — how's \
that going? We've got private training sessions if you want to ease into it without \
the gym crowd." (fitness — references timeline + addresses concern + option)
- "You were looking into the neck treatment a while back. We've been getting some \
really good results with Morpheus8 lately — $800 per session and honestly the \
before and afters speak for themselves. Is that area still something you're trying \
to address?" (med spa — references interest + price + question)

BAD EXAMPLES (do NOT write like this):
- "We have a special offer on teeth whitening." (generic, no conversation reference)
- "Are you still interested in our services?" (could be sent to anyone)
- "Hi Maria! We miss you at Dr. K Beauty!" (corporate, no substance)
- "The Morpheus8 treatment is a great option for skin tightening." (product catalog)

CONSTRAINTS:
- 2-4 sentences (more substantial than a quick text, but not a paragraph)
- End with a question that's easy to reply to
- Don't start with their name
- Never start with "I was", "I remember", "I noticed", "I saw", "Checking in", \
"Looking back" — just say the thing
- The offer/price should feel like a natural aside, not the main pitch
- The conversation reference should feel like the main reason you're texting"""

# Same test scenarios from Phase 4
SCENARIOS = [
    {
        "name": "MED SPA — Neck treatment, said too expensive",
        "first_name": "Maria",
        "service_interest": "Turkey Neck Treatment",
        "business_name": "Dr. K Beauty in LV",
        "bot_persona": "You're Sarah, the patient coordinator. Warm, professional, texts like a friendly coworker.",
        "timeline": """## CONVERSATION HISTORY

[Oct 10, 2025 — 3:01 PM]
LEAD: yes how much is the neck treatment

[Oct 10, 2025 — 3:03 PM]
AI: our Morpheus8 RF Microneedling for the neck area starts at $800 per session. Want me to check what day works for a free consultation?

[Oct 10, 2025 — 3:15 PM]
LEAD: thats too expensive for me right now""",
        "booking_history": "No appointment history.",
        "services": "- Morpheus8 RF Microneedling (Neck): $800/session\n- Smooth Eye Treatment: $99 (INTRO)\n- PDO Thread Lift (Neck): $1,200\n- Body Contouring: $350/session",
        "offers": "[PROMO] Smooth Eye Treatment — $99 intro\n[PROMO] Body Contouring — $199 first session",
        "security_prompt": "", "supported_languages": "English", "detected_language": "English", "channel": "sms",
    },
    {
        "name": "DENTAL — Whitening, nervous about pain from strips",
        "first_name": "Mike",
        "service_interest": "Teeth Whitening",
        "business_name": "Bright Smile Dental",
        "bot_persona": "You're Jake, the office coordinator. Casual and friendly, texts like a buddy who works at a dental office.",
        "timeline": """## CONVERSATION HISTORY

[Sep 28, 2025 — 11:20 AM]
LEAD: yeah my teeth are really yellow from coffee. had a bad experience with whitening strips though, they hurt my gums

[Sep 28, 2025 — 11:22 AM]
AI: professional whitening is way different. our Zoom treatment is controlled and much more effective. $450 and up to 8 shades brighter.

[Sep 28, 2025 — 11:35 AM]
LEAD: maybe in a few weeks. kind of nervous about it""",
        "booking_history": "No appointment history.",
        "services": "- Zoom Teeth Whitening: $450\n- Take-Home Whitening Kit: $199\n- Dental Cleaning: $89\n- Dental Exam + X-Rays: $49 (NEW PATIENT)\n- Veneers: $1,200/tooth",
        "offers": "[PROMO] New Patient — Exam + X-Rays $49\n[PROMO] Take-Home Kit — $149 (reg $199)",
        "security_prompt": "", "supported_languages": "English", "detected_language": "English", "channel": "sms",
    },
    {
        "name": "ROOFING — Storm damage, waiting on insurance",
        "first_name": "Tom",
        "service_interest": "Roof Repair",
        "business_name": "Summit Roofing Co",
        "bot_persona": "You're Dave, the project manager. Down to earth, talks like a contractor, keeps it simple.",
        "timeline": """## CONVERSATION HISTORY

[Sep 15, 2025 — 9:30 AM]
LEAD: yeah we had some shingles blow off in the storm last week. need someone to come look at it

[Sep 15, 2025 — 9:32 AM]
AI: we can send someone out for a free inspection this week. most storm damage repairs run $500-2500.

[Sep 15, 2025 — 10:00 AM]
LEAD: let me check with my insurance first. they said an adjuster is coming out next thursday""",
        "booking_history": "No appointment history.",
        "services": "- Storm Damage Repair: $500-$2,500\n- Full Roof Replacement: $8,000-$15,000\n- Gutter Installation: $800-$1,500\n- Gutter Cleaning: $149\n- Free Roof Inspection\n- Attic Insulation: $1,200-$2,500",
        "offers": "[PROMO] Gutter Cleaning — $99 (reg $149)\n[PROMO] Free Roof Inspection",
        "security_prompt": "", "supported_languages": "English", "detected_language": "English", "channel": "sms",
    },
    {
        "name": "FITNESS — Weight loss, too embarrassed for gym",
        "first_name": "Jessica",
        "service_interest": "Personal Training",
        "business_name": "Iron Forge Fitness",
        "bot_persona": "You're Coach Alex. Motivating but chill. Talk like a trainer who texts clients.",
        "timeline": """## CONVERSATION HISTORY

[Oct 5, 2025 — 6:00 PM]
LEAD: yes i really need to lose weight. ive gained like 40 pounds and nothing fits anymore

[Oct 5, 2025 — 6:02 PM]
AI: personal training is built around your goals and pace. 12-session package for $600.

[Oct 5, 2025 — 6:15 PM]
LEAD: im too embarrassed to go to a gym honestly

[Oct 5, 2025 — 6:17 PM]
AI: our PT sessions are private, just you and the trainer in a separate room.

[Oct 5, 2025 — 6:20 PM]
LEAD: maybe after new years""",
        "booking_history": "No appointment history.",
        "services": "- Personal Training (12 sessions): $600\n- Personal Training (24 sessions): $1,000\n- Group Fitness Classes: $99/month\n- Yoga: $79/month\n- Nutrition Coaching: $200/month\n- Gym Membership: $49/month",
        "offers": "[PROMO] Yoga — $29 first month\n[PROMO] Gym Membership — $29 first month",
        "security_prompt": "", "supported_languages": "English", "detected_language": "English", "channel": "sms",
    },
    {
        "name": "LAW FIRM — Personal injury, rear-ended",
        "first_name": "Desiree",
        "service_interest": "Car Accident Consultation",
        "business_name": "Martinez & Associates",
        "bot_persona": "You're Ana, the intake coordinator. Professional, empathetic, never pushy.",
        "timeline": """## CONVERSATION HISTORY

[Oct 1, 2025 — 4:00 PM]
LEAD: i got rear ended at a red light last week. my back and neck are killing me. do i have a case?

[Oct 1, 2025 — 4:05 PM]
AI: I'm sorry to hear that. Rear-end accidents almost always favor the person who was hit. A free consultation would help us understand your situation better.

[Oct 1, 2025 — 4:15 PM]
LEAD: ok but i dont have money for a lawyer right now

[Oct 1, 2025 — 4:17 PM]
AI: We work on contingency, meaning you don't pay unless we win your case.

[Oct 1, 2025 — 4:30 PM]
LEAD: let me think about it. im still dealing with the pain and havent been able to work""",
        "booking_history": "No appointment history.",
        "services": "- Free Case Consultation\n- Personal Injury Representation (contingency)\n- Car Accident Claims\n- Slip and Fall Claims\n- Workers' Compensation",
        "offers": "",
        "security_prompt": "", "supported_languages": "English", "detected_language": "English", "channel": "sms",
    },
    {
        "name": "PET GROOMING — Dog matting",
        "first_name": "Ashley",
        "service_interest": "Dog Grooming",
        "business_name": "Pawfect Grooming Studio",
        "bot_persona": "You're Bri, the grooming coordinator. Bubbly, loves dogs, texts casually.",
        "timeline": """## CONVERSATION HISTORY

[Sep 10, 2025 — 1:00 PM]
LEAD: hi i have a goldendoodle and his fur is getting really matted. how much for a full groom?

[Sep 10, 2025 — 1:05 PM]
AI: for a goldendoodle a full groom is usually $85-$120 depending on the matting. We'd need to see him to give you an exact price.

[Sep 10, 2025 — 1:15 PM]
LEAD: ok his name is Benny. he's about 60 pounds. when can i bring him in?

[Sep 10, 2025 — 1:17 PM]
AI: we have openings this Saturday morning. Want me to save a spot for Benny?

[Sep 10, 2025 — 1:30 PM]
LEAD: let me check my schedule and get back to you""",
        "booking_history": "No appointment history.",
        "services": "- Full Dog Groom (small): $55-$75\n- Full Dog Groom (medium): $75-$95\n- Full Dog Groom (large): $85-$120\n- Bath & Brush: $40-$60\n- Nail Trim: $15\n- De-matting Treatment: $25 add-on\n- Puppy First Groom: $45",
        "offers": "[PROMO] First groom 20% off for new clients\n[PROMO] Nail trim free with full groom",
        "security_prompt": "", "supported_languages": "English", "detected_language": "English", "channel": "sms",
    },
    {
        "name": "HVAC — AC broken, asked about financing",
        "first_name": "Carlos",
        "service_interest": "AC Repair",
        "business_name": "CoolAir HVAC",
        "bot_persona": "You're Brandon, the service coordinator. Professional but friendly.",
        "timeline": """## CONVERSATION HISTORY

[Aug 20, 2025 — 2:00 PM]
LEAD: my ac went out yesterday and its 110 degrees here. how fast can you get someone out

[Aug 20, 2025 — 2:05 PM]
AI: we can usually get someone out same day or next day. diagnostic is $89 and that gets applied to the repair cost.

[Aug 20, 2025 — 2:15 PM]
LEAD: do you guys do financing? if its a big repair i might need a payment plan

[Aug 20, 2025 — 2:17 PM]
AI: yes we offer 0% financing for 12 months on repairs over $500.

[Aug 20, 2025 — 2:30 PM]
LEAD: ok let me talk to my wife and get back to you""",
        "booking_history": "No appointment history.",
        "services": "- AC Diagnostic: $89 (applied to repair)\n- AC Repair: $150-$2,000\n- AC Unit Replacement: $3,500-$7,000\n- Duct Cleaning: $299\n- Annual Maintenance Plan: $149/year\n- Furnace Tune-Up: $99",
        "offers": "[PROMO] Free diagnostic with any repair over $500\n[PROMO] Annual Maintenance Plan — $99 first year (reg $149)",
        "security_prompt": "", "supported_languages": "English", "detected_language": "English", "channel": "sms",
    },
    {
        "name": "REAL ESTATE — First-time buyer, school district",
        "first_name": "James",
        "service_interest": "Home Buying",
        "business_name": "Realty One Group",
        "bot_persona": "You're Nicole, a real estate agent. Professional, knowledgeable.",
        "timeline": """## CONVERSATION HISTORY

[Sep 22, 2025 — 10:00 AM]
LEAD: we're looking to buy our first home. budget is around 350-400k. need to be in a good school district for our kids

[Sep 22, 2025 — 10:05 AM]
AI: Congratulations! There are some great options in that range. The Henderson and Summerlin areas have top-rated schools. Would you like to set up a time to go over what's available?

[Sep 22, 2025 — 10:15 AM]
LEAD: yeah that would be great. we're flexible on weekends

[Sep 22, 2025 — 10:17 AM]
AI: Perfect, how about this Saturday at 11am?

[Sep 22, 2025 — 10:30 AM]
LEAD: let me confirm with my wife and ill get back to you""",
        "booking_history": "No appointment history.",
        "services": "- Buyer Consultation (free)\n- Home Search + Showing Tours\n- First-Time Buyer Program\n- Pre-Approval Assistance\n- Investment Property Advisory",
        "offers": "",
        "security_prompt": "", "supported_languages": "English", "detected_language": "English", "channel": "sms",
    },
]


def _build_system_prompt(scenario: dict) -> str:
    """Build the system prompt for standalone P1 generation."""
    sections = [_ROLE_SMS, _VOICE, _P1_STANDALONE, _ANTI_HALLUCINATION, _BANNED_PATTERNS, _GLOBAL_RULES]

    if scenario.get("bot_persona"):
        sections.append(f"## BOT PERSONA\n\n{scenario['bot_persona']}")

    return "\n\n".join(sections)


def _build_user_prompt(scenario: dict) -> str:
    """Build the user message with all context."""
    parts = [
        f"Contact Name: {scenario['first_name']}",
        f"Service Interest: {scenario['service_interest']}",
        f"Business: {scenario['business_name']}",
        "",
        scenario["timeline"],
        "",
        scenario["booking_history"],
        "",
        f"## Available Services\n{scenario['services']}",
    ]

    if scenario.get("offers"):
        parts.append(f"\n## Current Offers\n{scenario['offers']}")

    warnings = scan_sensitivity(scenario["timeline"])
    if warnings:
        parts.append(f"\n## SENSITIVITY WARNINGS\n" + "\n".join(f"- {w}" for w in warnings))

    parts.append("\nGenerate ONE reactivation message for this lead.")
    return "\n".join(parts)


async def run_tests():
    start_ai_clients()
    testing_key = os.getenv("OPENROUTER_TESTING_KEY") or os.getenv("OPENROUTER_API_KEY", "")

    # Add Sonnet to model mapping for this test only
    MODELS["sonnet"] = "anthropic/claude-sonnet-4-6"

    models = {
        "flash-lite": "Gemini 3.1 Flash Lite (current)",
        "sonnet": "Claude Sonnet 4.6",
    }

    for model_name, label in models.items():
        print(f"\n{'='*70}")
        print(f"MODEL: {label}")
        print(f"{'='*70}")

        token_tracker = TokenUsage()
        set_ai_context(api_key=testing_key, token_tracker=token_tracker)
        total_time = 0
        banned_count = 0

        for s_idx, scenario in enumerate(SCENARIOS, 1):
            system_prompt = _build_system_prompt(scenario)
            user_prompt = _build_user_prompt(scenario)

            t0 = time.perf_counter()
            result = await chat(
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                model=model_name,
                temperature=0.9,
                label="reactivation_sms_test",
            )
            elapsed = time.perf_counter() - t0
            total_time += elapsed

            message = result.choices[0].message.content.strip()
            violations = check_banned_patterns(message)
            if violations:
                banned_count += 1

            flag = f" [BANNED: {violations}]" if violations else ""
            print(f"\n  [{s_idx}] {scenario['name']}")
            print(f"  {message}{flag}")
            print(f"  ({elapsed:.1f}s)")

        clear_ai_context()

        print(f"\n  --- {label} Summary ---")
        print(f"  Total cost: ${token_tracker.estimated_cost():.4f}")
        print(f"  Avg cost/lead: ${token_tracker.estimated_cost()/len(SCENARIOS):.4f}")
        print(f"  Total time: {total_time:.1f}s ({total_time/len(SCENARIOS):.1f}s avg)")
        print(f"  Banned leaks: {banned_count}")


if __name__ == "__main__":
    asyncio.run(run_tests())
