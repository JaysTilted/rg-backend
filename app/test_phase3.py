"""Phase 3: Model Comparison — Same scenarios, different models.

Runs all 3 scenarios on each model and outputs messages for manual review.
Also tracks cost per lead.

Usage: docker compose exec iron-setter python -m app.test_phase3
"""

import asyncio
import json
import os
import time

from app.services.ai_client import start_ai_clients, set_ai_context, clear_ai_context
from app.models import TokenUsage
from app.workflows import reactivation
from app.workflows.reactivation import (
    _generate_sms_lane,
    check_banned_patterns,
    scan_sensitivity,
)

MODELS_TO_TEST = ["flash", "gemini-3-flash", "flash-lite"]

SCENARIOS = [
    {
        "name": "BEAUTY — Neck concern + price objection + family health",
        "first_name": "Maria",
        "service_interest": "Turkey Neck Treatment",
        "business_name": "Dr. K Beauty in LV",
        "bot_persona": "You're Sarah, the patient coordinator at Dr. K Beauty. Warm but not over-the-top.",
        "timeline": """## CONVERSATION HISTORY

[Oct 10, 2025 — 2:30 PM]
DRIP: hi Maria! we have some amazing treatments for turkey neck at Dr. K Beauty. would you like to learn more?

[Oct 10, 2025 — 3:01 PM]
LEAD: yes how much is the neck treatment

[Oct 10, 2025 — 3:03 PM]
AI: our Morpheus8 RF Microneedling for the neck area starts at $800 per session. we also have some intro pricing available right now. want me to check what day works for a free consultation?

[Oct 10, 2025 — 3:15 PM]
LEAD: thats too expensive for me right now. my mom is sick and ive been spending a lot on her treatments

[Oct 10, 2025 — 3:20 PM]
LEAD: sure thanks""",
        "booking_history": "No appointment history.",
        "services": """- Morpheus8 RF Microneedling (Neck): $800/session
- Morpheus8 RF Microneedling (Face): $750/session
- Smooth Eye Treatment: $99/session (INTRO PRICE)
- Lip Filler: $450
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
        "name": "DENTAL — Whitening + bad past experience",
        "first_name": "Mike",
        "service_interest": "Teeth Whitening",
        "business_name": "Bright Smile Dental",
        "bot_persona": "You're Jake, office coordinator at Bright Smile Dental. Casual, like texting a buddy.",
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
        "bot_persona": "You're Coach Alex at Iron Forge Fitness. Motivating but chill.",
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


async def run_model(model_name: str, scenarios: list[dict]) -> dict:
    """Run all scenarios for one model. Returns results dict."""
    # Swap the model
    reactivation._REACTIVATION_MODEL = model_name

    testing_key = os.getenv("OPENROUTER_TESTING_KEY") or os.getenv("OPENROUTER_API_KEY", "")
    token_tracker = TokenUsage()
    set_ai_context(api_key=testing_key, token_tracker=token_tracker)

    results = []
    t0 = time.perf_counter()

    for scenario in scenarios:
        messages = await _generate_sms_lane(scenario)
        banned_count = 0
        for i in range(1, 7):
            v = check_banned_patterns(messages.get(f"sms_{i}", ""))
            if v:
                banned_count += 1
        results.append({"name": scenario["name"], "messages": messages, "banned_remaining": banned_count})

    elapsed = time.perf_counter() - t0
    cost = token_tracker.estimated_cost()
    clear_ai_context()

    return {
        "model": model_name,
        "results": results,
        "total_cost": cost,
        "cost_per_lead": cost / len(scenarios),
        "elapsed_s": round(elapsed, 1),
    }


async def main():
    start_ai_clients()

    all_results = {}
    for model in MODELS_TO_TEST:
        print(f"\n{'='*70}")
        print(f"Running model: {model}")
        print(f"{'='*70}")
        data = await run_model(model, SCENARIOS)
        all_results[model] = data

        for r in data["results"]:
            print(f"\n--- {r['name']} ---")
            for i in range(1, 7):
                text = r["messages"].get(f"sms_{i}", "(empty)")
                print(f"  P{i}: {text}")
            if r["banned_remaining"] > 0:
                print(f"  !! {r['banned_remaining']} banned patterns slipped through")

        print(f"\n  Cost: ${data['total_cost']:.4f} (${data['cost_per_lead']:.4f}/lead) | Time: {data['elapsed_s']}s")

    # Reset model back to default
    reactivation._REACTIVATION_MODEL = "flash"

    # Summary table
    print(f"\n{'='*70}")
    print("MODEL COMPARISON SUMMARY")
    print(f"{'='*70}")
    print(f"{'Model':<20} {'Cost/lead':<12} {'Total':<10} {'Time':<10} {'Banned leaked'}")
    print(f"{'-'*70}")
    for model, data in all_results.items():
        total_banned = sum(r["banned_remaining"] for r in data["results"])
        print(f"{model:<20} ${data['cost_per_lead']:.4f}      ${data['total_cost']:.4f}    {data['elapsed_s']}s     {total_banned}")


if __name__ == "__main__":
    asyncio.run(main())
