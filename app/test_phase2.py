"""Phase 2 Test: Full Message Generation with Enforcement

Generates complete 6-position SMS drips for 3 diverse fake scenarios.
Tests banned pattern enforcement and sensitivity pre-scan in action.
Outputs everything for manual review — no auto-grading.

Usage: docker compose exec iron-setter python -m app.test_phase2
"""

import asyncio
import json
import os

from app.services.ai_client import start_ai_clients, set_ai_context, clear_ai_context
from app.models import TokenUsage
from app.workflows.reactivation import (
    _generate_sms_lane,
    check_banned_patterns,
    scan_sensitivity,
)

# Three scenarios designed to trigger the original failure modes:
# 1. Beauty (neck) with cheap eye promo trap + price sensitivity
# 2. Dental with exam trap + bad experience sensitivity
# 3. Fitness with yoga trap + body image sensitivity

SCENARIOS = [
    {
        "name": "BEAUTY SPA — Neck concern + price objection in history",
        "first_name": "Maria",
        "service_interest": "Turkey Neck Treatment",
        "business_name": "Dr. K Beauty in LV",
        "bot_persona": "You're Sarah, the patient coordinator at Dr. K Beauty. You text like a friendly professional — warm but not over-the-top.",
        "timeline": """## CONVERSATION HISTORY

[Oct 10, 2025 — 2:30 PM]
DRIP: hi Maria! we have some amazing treatments for turkey neck at Dr. K Beauty. would you like to learn more?

[Oct 10, 2025 — 3:01 PM]
LEAD: yes how much is the neck treatment

[Oct 10, 2025 — 3:03 PM]
AI: our Morpheus8 RF Microneedling for the neck area starts at $800 per session. we also have some intro pricing available right now. want me to check what day works for a free consultation?

[Oct 10, 2025 — 3:15 PM]
LEAD: thats too expensive for me right now. my mom is sick and ive been spending a lot on her treatments

[Oct 10, 2025 — 3:17 PM]
AI: totally understand Maria. we do have payment plans available and sometimes run specials. would it help if i let you know when we have a promotion on neck treatments?

[Oct 10, 2025 — 3:20 PM]
LEAD: sure thanks""",
        "booking_history": "No appointment history.",
        "services": """- Morpheus8 RF Microneedling (Neck): $800/session
- Morpheus8 RF Microneedling (Face): $750/session
- Smooth Eye Treatment: $99/session (INTRO PRICE)
- Lip Filler: $450
- Botox (per unit): $12
- PDO Thread Lift (Neck): $1,200
- Body Contouring: $350/session""",
        "offers": """[PROMO] Smooth Eye Treatment — $99 intro price (regular $250)
[PROMO] Body Contouring — $199 first session (regular $350)""",
        "security_prompt": "",
        "supported_languages": "English",
        "detected_language": "English",
        "channel": "sms",
    },
    {
        "name": "DENTAL — Whitening interest + bad past experience",
        "first_name": "Mike",
        "service_interest": "Teeth Whitening",
        "business_name": "Bright Smile Dental",
        "bot_persona": "You're Jake, the office coordinator at Bright Smile Dental. Casual and friendly, like texting a buddy who happens to work at a dental office.",
        "timeline": """## CONVERSATION HISTORY

[Sep 28, 2025 — 11:00 AM]
DRIP: hey Mike! saw you were interested in teeth whitening at Bright Smile Dental. we have some great options. want to hear more?

[Sep 28, 2025 — 11:20 AM]
LEAD: yeah my teeth are really yellow from coffee and smoking. had a bad experience with whitening strips though, they hurt my gums and didnt really work

[Sep 28, 2025 — 11:22 AM]
AI: oh man yeah those strips can be rough. professional whitening is way different. our Zoom treatment is done by the dentist so its controlled and much more effective. $450 and your teeth can be up to 8 shades brighter. want to come in?

[Sep 28, 2025 — 11:35 AM]
LEAD: maybe in a few weeks. kind of nervous about it honestly""",
        "booking_history": "No appointment history.",
        "services": """- Zoom Teeth Whitening: $450
- Take-Home Whitening Kit: $199
- Dental Cleaning: $89
- Dental Exam + X-Rays: $49 (NEW PATIENT SPECIAL)
- Dental Implants: $3,500/tooth
- Invisalign: $4,500
- Veneers: $1,200/tooth""",
        "offers": """[PROMO] New Patient Special — Exam + X-Rays $49 (regular $150)
[PROMO] Take-Home Whitening Kit — $149 (regular $199)""",
        "security_prompt": "",
        "supported_languages": "English",
        "detected_language": "English",
        "channel": "sms",
    },
    {
        "name": "FITNESS — Weight loss + body image sensitivity",
        "first_name": "Jessica",
        "service_interest": "Personal Training",
        "business_name": "Iron Forge Fitness",
        "bot_persona": "You're Coach Alex at Iron Forge Fitness. Motivating but chill. You talk like a trainer who texts clients — direct, encouraging, no corporate speak.",
        "timeline": """## CONVERSATION HISTORY

[Oct 5, 2025 — 5:45 PM]
DRIP: hey Jessica! still interested in personal training at Iron Forge Fitness? we'd love to help you hit your goals

[Oct 5, 2025 — 6:00 PM]
LEAD: yes i really need to lose weight. ive gained like 40 pounds and i hate how i look. nothing fits anymore

[Oct 5, 2025 — 6:02 PM]
AI: i hear you and youre not alone in feeling that way. personal training is great because its built around your specific goals and pace. we have a 12-session package for $600. want to come in for a free fitness assessment?

[Oct 5, 2025 — 6:15 PM]
LEAD: im too embarrassed to go to a gym honestly

[Oct 5, 2025 — 6:17 PM]
AI: totally get it. just so you know our PT sessions are private, its just you and the trainer in a separate room. no one watching. a lot of our members started feeling the same way. want me to save you a spot for an assessment?

[Oct 5, 2025 — 6:20 PM]
LEAD: maybe after new years""",
        "booking_history": "No appointment history.",
        "services": """- Personal Training (12 sessions): $600
- Personal Training (24 sessions): $1,000
- Group Fitness Classes: $99/month
- Yoga Membership: $79/month
- Nutrition Coaching: $200/month
- Gym Membership: $49/month""",
        "offers": """[PROMO] Yoga — First month $29 (regular $79)
[PROMO] Gym Membership — $29 first month (regular $49)""",
        "security_prompt": "",
        "supported_languages": "English",
        "detected_language": "English",
        "channel": "sms",
    },
]


async def run_tests():
    start_ai_clients()

    testing_key = os.getenv("OPENROUTER_TESTING_KEY") or os.getenv("OPENROUTER_API_KEY", "")
    token_tracker = TokenUsage()
    set_ai_context(api_key=testing_key, token_tracker=token_tracker)

    print("=" * 70)
    print("PHASE 2 TEST: Full 6-Position SMS Generation")
    print("Manual review required — check each message for:")
    print("  - Correct service selection (P2, P5)")
    print("  - No banned openers")
    print("  - No sensitivity violations")
    print("  - Human-sounding, not robotic")
    print("=" * 70)

    for s_idx, scenario in enumerate(SCENARIOS, 1):
        print(f"\n{'='*70}")
        print(f"SCENARIO {s_idx}: {scenario['name']}")
        print(f"{'='*70}")
        print(f"Lead: {scenario['first_name']}")
        print(f"Interest: {scenario['service_interest']}")

        # Show sensitivity scan results
        warnings = scan_sensitivity(scenario["timeline"])
        if warnings:
            print(f"\nSensitivity warnings detected ({len(warnings)}):")
            for w in warnings:
                print(f"  >> {w.split(':')[0]}")
        else:
            print("\nNo sensitivity warnings detected.")

        # Generate all 6 messages
        print("\nGenerating 6 SMS positions...")
        messages = await _generate_sms_lane(scenario)

        # Display each message with banned pattern check
        print(f"\n--- GENERATED MESSAGES ---")
        for i in range(1, 7):
            key = f"sms_{i}"
            text = messages.get(key, "(empty)")
            violations = check_banned_patterns(text)
            violation_flag = f" !! BANNED: {violations}" if violations else ""
            print(f"\n  P{i}: {text}{violation_flag}")

        print()

    clear_ai_context()

    print(f"\n{'='*70}")
    print(f"Total cost: ${token_tracker.estimated_cost():.4f}")
    print(f"{'='*70}")


if __name__ == "__main__":
    asyncio.run(run_tests())
