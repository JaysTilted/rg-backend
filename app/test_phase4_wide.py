"""Phase 4b: Wide Cross-Industry Test — 8 industries, full 6-position generation.

Each scenario has a realistic conversation with specific details the model should
reference. Output is formatted for easy manual review.

Usage: docker compose exec rg-backend python -m app.test_phase4_wide
"""

import asyncio
import json
import os
import time

from app.services.ai_client import start_ai_clients, set_ai_context, clear_ai_context
from app.models import TokenUsage
from app.workflows.reactivation import (
    _generate_sms_lane,
    check_banned_patterns,
    scan_sensitivity,
)

SCENARIOS = [
    # ── 1. MED SPA (neck, price objection) ──
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
    # ── 2. DENTAL (whitening, nervous about pain) ──
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
    # ── 3. ROOFING (storm damage, waiting on insurance) ──
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
    # ── 4. FITNESS (weight loss, embarrassed about gym) ──
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
    # ── 5. HVAC (AC broken, summer, asked about financing) ──
    {
        "name": "HVAC — AC broken in summer, asked about financing",
        "first_name": "Carlos",
        "service_interest": "AC Repair",
        "business_name": "CoolAir HVAC",
        "bot_persona": "You're Brandon, the service coordinator. Professional but friendly. Texts like a helpful neighbor who happens to do HVAC.",
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
    # ── 6. LAW FIRM (personal injury, car accident) ──
    {
        "name": "LAW FIRM — Personal injury, rear-ended at a light",
        "first_name": "Desiree",
        "service_interest": "Car Accident Consultation",
        "business_name": "Martinez & Associates",
        "bot_persona": "You're Ana, the intake coordinator. Professional, empathetic, never pushy. Texts like a helpful office assistant.",
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
    # ── 7. PET GROOMING (dog grooming, mentioned matting) ──
    {
        "name": "PET GROOMING — Dog matting, asked about pricing",
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
    # ── 8. REAL ESTATE (home buyer, mentioned school district) ──
    {
        "name": "REAL ESTATE — First-time buyer, needs good school district",
        "first_name": "James",
        "service_interest": "Home Buying",
        "business_name": "Realty One Group",
        "bot_persona": "You're Nicole, a real estate agent. Professional, knowledgeable, texts like a friendly advisor.",
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


async def run_tests():
    start_ai_clients()
    testing_key = os.getenv("OPENROUTER_TESTING_KEY") or os.getenv("OPENROUTER_API_KEY", "")
    token_tracker = TokenUsage()
    set_ai_context(api_key=testing_key, token_tracker=token_tracker)

    total_banned = 0
    total_time = 0

    for s_idx, scenario in enumerate(SCENARIOS, 1):
        t0 = time.perf_counter()

        # Sensitivity scan
        warnings = scan_sensitivity(scenario["timeline"])

        # Generate
        messages = await _generate_sms_lane(scenario)

        elapsed = time.perf_counter() - t0
        total_time += elapsed

        # Check for violations
        banned_in_scenario = 0
        for i in range(1, 7):
            v = check_banned_patterns(messages.get(f"sms_{i}", ""))
            if v:
                banned_in_scenario += 1
        total_banned += banned_in_scenario

        # Output
        print(f"\n{'='*70}")
        print(f"[{s_idx}/{len(SCENARIOS)}] {scenario['name']}")
        print(f"Lead: {scenario['first_name']} | Interest: {scenario['service_interest']}")
        print(f"Business: {scenario['business_name']}")
        if warnings:
            print(f"Sensitivity: {', '.join(w.split(':')[0] for w in warnings)}")
        print(f"Time: {elapsed:.1f}s")
        print(f"{'='*70}")

        for i in range(1, 7):
            text = messages.get(f"sms_{i}", "(empty)")
            violations = check_banned_patterns(text)
            flag = f" [BANNED: {violations}]" if violations else ""
            print(f"\n  P{i}: {text}{flag}")

    clear_ai_context()

    # Summary
    print(f"\n\n{'='*70}")
    print(f"SUMMARY — {len(SCENARIOS)} scenarios, {len(SCENARIOS)*6} messages")
    print(f"{'='*70}")
    print(f"Total cost: ${token_tracker.estimated_cost():.4f}")
    print(f"Avg cost/lead: ${token_tracker.estimated_cost()/len(SCENARIOS):.4f}")
    print(f"Total time: {total_time:.1f}s ({total_time/len(SCENARIOS):.1f}s avg)")
    print(f"Banned patterns leaked: {total_banned}")
    print(f"{'='*70}")


if __name__ == "__main__":
    asyncio.run(run_tests())
