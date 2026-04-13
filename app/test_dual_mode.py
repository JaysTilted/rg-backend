"""Stress Test: Dual-Mode Reactivation (P1-Only vs Full Drip)

Tests both modes across 8 industries, 3 runs each for consistency.
Outputs formatted for manual review — no automated pass/fail.

Usage: docker compose exec rg-backend python -m app.test_dual_mode
"""

import asyncio
import json
import os
import time

from app.services.ai_client import start_ai_clients, set_ai_context, clear_ai_context
from app.models import TokenUsage
from app.workflows.reactivation import (
    _generate_p1_only,
    _generate_sms_lane,
    _build_context,
    _build_response,
    _has_media_library,
    check_banned_patterns,
    scan_sensitivity,
)

# 8 industry scenarios (same as Phase 4 tests)
SCENARIOS = [
    {
        "name": "MED SPA — Neck treatment, said too expensive",
        "first_name": "Maria", "service_interest": "Turkey Neck Treatment",
        "business_name": "Dr. K Beauty in LV",
        "bot_persona": "You're Sarah, the patient coordinator. Warm, professional, texts like a friendly coworker.",
        "timeline": "## CONVERSATION HISTORY\n\n[Oct 10, 2025 — 3:01 PM]\nLEAD: yes how much is the neck treatment\n\n[Oct 10, 2025 — 3:03 PM]\nAI: our Morpheus8 RF Microneedling for the neck area starts at $800 per session. Want me to check what day works for a free consultation?\n\n[Oct 10, 2025 — 3:15 PM]\nLEAD: thats too expensive for me right now",
        "booking_history": "No appointment history.",
        "services": "- Morpheus8 RF Microneedling (Neck): $800/session\n- Smooth Eye Treatment: $99 (INTRO)\n- PDO Thread Lift (Neck): $1,200\n- Body Contouring: $350/session",
        "offers": "[PROMO] Smooth Eye Treatment — $99 intro\n[PROMO] Body Contouring — $199 first session",
        "security_prompt": "", "supported_languages": "English", "detected_language": "English", "channel": "sms",
    },
    {
        "name": "DENTAL — Whitening, nervous about pain",
        "first_name": "Mike", "service_interest": "Teeth Whitening",
        "business_name": "Bright Smile Dental",
        "bot_persona": "You're Jake, the office coordinator. Casual and friendly.",
        "timeline": "## CONVERSATION HISTORY\n\n[Sep 28, 2025 — 11:20 AM]\nLEAD: yeah my teeth are really yellow from coffee. had a bad experience with whitening strips though, they hurt my gums\n\n[Sep 28, 2025 — 11:22 AM]\nAI: professional whitening is way different. our Zoom treatment is controlled and much more effective. $450 and up to 8 shades brighter.\n\n[Sep 28, 2025 — 11:35 AM]\nLEAD: maybe in a few weeks. kind of nervous about it",
        "booking_history": "No appointment history.",
        "services": "- Zoom Teeth Whitening: $450\n- Take-Home Whitening Kit: $199\n- Dental Cleaning: $89\n- Dental Exam + X-Rays: $49 (NEW PATIENT)\n- Veneers: $1,200/tooth",
        "offers": "[PROMO] New Patient — Exam + X-Rays $49\n[PROMO] Take-Home Kit — $149 (reg $199)",
        "security_prompt": "", "supported_languages": "English", "detected_language": "English", "channel": "sms",
    },
    {
        "name": "ROOFING — Storm damage, waiting on insurance",
        "first_name": "Tom", "service_interest": "Roof Repair",
        "business_name": "Summit Roofing Co",
        "bot_persona": "You're Dave, the project manager. Down to earth, keeps it simple.",
        "timeline": "## CONVERSATION HISTORY\n\n[Sep 15, 2025 — 9:30 AM]\nLEAD: yeah we had some shingles blow off in the storm last week. need someone to come look at it\n\n[Sep 15, 2025 — 9:32 AM]\nAI: we can send someone out for a free inspection this week. most storm damage repairs run $500-2500.\n\n[Sep 15, 2025 — 10:00 AM]\nLEAD: let me check with my insurance first. they said an adjuster is coming out next thursday",
        "booking_history": "No appointment history.",
        "services": "- Storm Damage Repair: $500-$2,500\n- Full Roof Replacement: $8,000-$15,000\n- Gutter Installation: $800-$1,500\n- Gutter Cleaning: $149\n- Free Roof Inspection\n- Attic Insulation: $1,200-$2,500",
        "offers": "[PROMO] Gutter Cleaning — $99 (reg $149)\n[PROMO] Free Roof Inspection",
        "security_prompt": "", "supported_languages": "English", "detected_language": "English", "channel": "sms",
    },
    {
        "name": "FITNESS — Weight loss, embarrassed about gym",
        "first_name": "Jessica", "service_interest": "Personal Training",
        "business_name": "Iron Forge Fitness",
        "bot_persona": "You're Coach Alex. Motivating but chill.",
        "timeline": "## CONVERSATION HISTORY\n\n[Oct 5, 2025 — 6:00 PM]\nLEAD: yes i really need to lose weight. ive gained like 40 pounds and nothing fits anymore\n\n[Oct 5, 2025 — 6:02 PM]\nAI: personal training is built around your goals and pace. 12-session package for $600.\n\n[Oct 5, 2025 — 6:15 PM]\nLEAD: im too embarrassed to go to a gym honestly\n\n[Oct 5, 2025 — 6:17 PM]\nAI: our PT sessions are private, just you and the trainer in a separate room.\n\n[Oct 5, 2025 — 6:20 PM]\nLEAD: maybe after new years",
        "booking_history": "No appointment history.",
        "services": "- Personal Training (12 sessions): $600\n- Personal Training (24 sessions): $1,000\n- Group Fitness Classes: $99/month\n- Yoga: $79/month\n- Nutrition Coaching: $200/month\n- Gym Membership: $49/month",
        "offers": "[PROMO] Yoga — $29 first month\n[PROMO] Gym Membership — $29 first month",
        "security_prompt": "", "supported_languages": "English", "detected_language": "English", "channel": "sms",
    },
    {
        "name": "LAW FIRM — Personal injury, rear-ended",
        "first_name": "Desiree", "service_interest": "Car Accident Consultation",
        "business_name": "Martinez & Associates",
        "bot_persona": "You're Ana, the intake coordinator. Professional, empathetic.",
        "timeline": "## CONVERSATION HISTORY\n\n[Oct 1, 2025 — 4:00 PM]\nLEAD: i got rear ended at a red light last week. my back and neck are killing me. do i have a case?\n\n[Oct 1, 2025 — 4:05 PM]\nAI: I'm sorry to hear that. Rear-end accidents almost always favor the person who was hit. A free consultation would help us understand your situation better.\n\n[Oct 1, 2025 — 4:15 PM]\nLEAD: ok but i dont have money for a lawyer right now\n\n[Oct 1, 2025 — 4:17 PM]\nAI: We work on contingency, meaning you don't pay unless we win your case.\n\n[Oct 1, 2025 — 4:30 PM]\nLEAD: let me think about it. im still dealing with the pain and havent been able to work",
        "booking_history": "No appointment history.",
        "services": "- Free Case Consultation\n- Personal Injury Representation (contingency)\n- Car Accident Claims\n- Slip and Fall Claims\n- Workers' Compensation",
        "offers": "",
        "security_prompt": "", "supported_languages": "English", "detected_language": "English", "channel": "sms",
    },
    {
        "name": "PET GROOMING — Dog matting",
        "first_name": "Ashley", "service_interest": "Dog Grooming",
        "business_name": "Pawfect Grooming Studio",
        "bot_persona": "You're Bri, the grooming coordinator. Bubbly, loves dogs.",
        "timeline": "## CONVERSATION HISTORY\n\n[Sep 10, 2025 — 1:00 PM]\nLEAD: hi i have a goldendoodle and his fur is getting really matted. how much for a full groom?\n\n[Sep 10, 2025 — 1:05 PM]\nAI: for a goldendoodle a full groom is usually $85-$120 depending on the matting. We'd need to see him to give you an exact price.\n\n[Sep 10, 2025 — 1:15 PM]\nLEAD: ok his name is Benny. he's about 60 pounds. when can i bring him in?\n\n[Sep 10, 2025 — 1:17 PM]\nAI: we have openings this Saturday morning. Want me to save a spot for Benny?\n\n[Sep 10, 2025 — 1:30 PM]\nLEAD: let me check my schedule and get back to you",
        "booking_history": "No appointment history.",
        "services": "- Full Dog Groom (small): $55-$75\n- Full Dog Groom (medium): $75-$95\n- Full Dog Groom (large): $85-$120\n- Bath & Brush: $40-$60\n- Nail Trim: $15\n- De-matting Treatment: $25 add-on\n- Puppy First Groom: $45",
        "offers": "[PROMO] First groom 20% off for new clients\n[PROMO] Nail trim free with full groom",
        "security_prompt": "", "supported_languages": "English", "detected_language": "English", "channel": "sms",
    },
    {
        "name": "HVAC — AC broken, asked about financing",
        "first_name": "Carlos", "service_interest": "AC Repair",
        "business_name": "CoolAir HVAC",
        "bot_persona": "You're Brandon, the service coordinator. Professional but friendly.",
        "timeline": "## CONVERSATION HISTORY\n\n[Aug 20, 2025 — 2:00 PM]\nLEAD: my ac went out yesterday and its 110 degrees here. how fast can you get someone out\n\n[Aug 20, 2025 — 2:05 PM]\nAI: we can usually get someone out same day or next day. diagnostic is $89 and that gets applied to the repair cost.\n\n[Aug 20, 2025 — 2:15 PM]\nLEAD: do you guys do financing? if its a big repair i might need a payment plan\n\n[Aug 20, 2025 — 2:17 PM]\nAI: yes we offer 0% financing for 12 months on repairs over $500.\n\n[Aug 20, 2025 — 2:30 PM]\nLEAD: ok let me talk to my wife and get back to you",
        "booking_history": "No appointment history.",
        "services": "- AC Diagnostic: $89 (applied to repair)\n- AC Repair: $150-$2,000\n- AC Unit Replacement: $3,500-$7,000\n- Duct Cleaning: $299\n- Annual Maintenance Plan: $149/year\n- Furnace Tune-Up: $99",
        "offers": "[PROMO] Free diagnostic with any repair over $500\n[PROMO] Annual Maintenance Plan — $99 first year (reg $149)",
        "security_prompt": "", "supported_languages": "English", "detected_language": "English", "channel": "sms",
    },
    {
        "name": "REAL ESTATE — First-time buyer, school district",
        "first_name": "James", "service_interest": "Home Buying",
        "business_name": "Realty One Group",
        "bot_persona": "You're Nicole, a real estate agent. Professional, knowledgeable.",
        "timeline": "## CONVERSATION HISTORY\n\n[Sep 22, 2025 — 10:00 AM]\nLEAD: we're looking to buy our first home. budget is around 350-400k. need to be in a good school district for our kids\n\n[Sep 22, 2025 — 10:05 AM]\nAI: Congratulations! There are some great options in that range. The Henderson and Summerlin areas have top-rated schools. Would you like to set up a time to go over what's available?\n\n[Sep 22, 2025 — 10:15 AM]\nLEAD: yeah that would be great. we're flexible on weekends\n\n[Sep 22, 2025 — 10:17 AM]\nAI: Perfect, how about this Saturday at 11am?\n\n[Sep 22, 2025 — 10:30 AM]\nLEAD: let me confirm with my wife and ill get back to you",
        "booking_history": "No appointment history.",
        "services": "- Buyer Consultation (free)\n- Home Search + Showing Tours\n- First-Time Buyer Program\n- Pre-Approval Assistance\n- Investment Property Advisory",
        "offers": "",
        "security_prompt": "", "supported_languages": "English", "detected_language": "English", "channel": "sms",
    },
]


async def run_tests():
    start_ai_clients()
    testing_key = os.getenv("OPENROUTER_TESTING_KEY") or os.getenv("OPENROUTER_API_KEY", "")

    num_runs = 3

    # ══════ P1-ONLY MODE (Sonnet) ══════
    print(f"\n{'='*70}")
    print("MODE: P1-ONLY (Sonnet) — 3 RUNS")
    print(f"{'='*70}")

    p1_total_cost = 0
    p1_total_banned = 0

    for run in range(1, num_runs + 1):
        print(f"\n{'─'*50}")
        print(f"RUN {run}/{num_runs}")
        print(f"{'─'*50}")

        token_tracker = TokenUsage()
        set_ai_context(api_key=testing_key, token_tracker=token_tracker)

        for s_idx, scenario in enumerate(SCENARIOS, 1):
            t0 = time.perf_counter()
            messages = await _generate_p1_only(scenario)
            elapsed = time.perf_counter() - t0

            sms = messages.get("sms_1", "(empty)")
            violations = check_banned_patterns(sms)
            flag = f" [BANNED: {violations}]" if violations else ""
            if violations:
                p1_total_banned += 1

            print(f"\n  [{s_idx}] {scenario['name']}")
            print(f"  {sms}{flag}")
            print(f"  ({elapsed:.1f}s)")

        clear_ai_context()
        run_cost = token_tracker.estimated_cost()
        p1_total_cost += run_cost
        print(f"\n  Run {run} cost: ${run_cost:.4f} (${run_cost/len(SCENARIOS):.4f}/lead)")

    print(f"\n{'─'*50}")
    print(f"P1-ONLY SUMMARY: 3 runs x 8 scenarios = 24 messages")
    print(f"Total cost: ${p1_total_cost:.4f} (avg ${p1_total_cost/24:.4f}/lead)")
    print(f"Banned leaks: {p1_total_banned}/24")
    print(f"{'─'*50}")

    # ══════ FULL DRIP MODE (Flash Lite) ══════
    print(f"\n\n{'='*70}")
    print("MODE: FULL DRIP (Flash Lite) — 1 RUN (P1 only for comparison)")
    print(f"{'='*70}")

    token_tracker = TokenUsage()
    set_ai_context(api_key=testing_key, token_tracker=token_tracker)
    drip_banned = 0

    for s_idx, scenario in enumerate(SCENARIOS, 1):
        t0 = time.perf_counter()
        messages = await _generate_sms_lane(scenario)
        elapsed = time.perf_counter() - t0

        sms1 = messages.get("sms_1", "(empty)")
        violations = check_banned_patterns(sms1)
        flag = f" [BANNED: {violations}]" if violations else ""
        if violations:
            drip_banned += 1

        print(f"\n  [{s_idx}] {scenario['name']}")
        print(f"  P1: {sms1}{flag}")
        print(f"  ({elapsed:.1f}s for all 6)")

    clear_ai_context()
    print(f"\n  Full drip cost: ${token_tracker.estimated_cost():.4f} (${token_tracker.estimated_cost()/len(SCENARIOS):.4f}/lead)")
    print(f"  P1 banned leaks: {drip_banned}/8")

    # ══════ MEDIA CHECK TEST ══════
    print(f"\n\n{'='*70}")
    print("MEDIA LIBRARY DETECTION TEST")
    print(f"{'='*70}")

    test_configs = [
        ({"media_library": None}, False, "None"),
        ({"media_library": []}, False, "Empty list"),
        ({"media_library": [{"name": "test.gif", "url": "...", "enabled": True}]}, True, "One enabled item"),
        ({"media_library": [{"name": "test.gif", "url": "...", "enabled": False}]}, False, "One disabled item"),
        ({"media_library": [{"name": "a.gif", "enabled": False}, {"name": "b.mp3", "enabled": True}]}, True, "Mixed enabled/disabled"),
        ({}, False, "No media_library key"),
    ]

    all_pass = True
    for config, expected, label in test_configs:
        result = _has_media_library(config)
        status = "PASS" if result == expected else "FAIL"
        if status == "FAIL":
            all_pass = False
        print(f"  {status} | {label}: expected={expected}, got={result}")

    print(f"\n  Media detection: {'ALL PASS' if all_pass else 'FAILURES DETECTED'}")


if __name__ == "__main__":
    asyncio.run(run_tests())
