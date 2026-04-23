"""Phase 1 Test: Service Matcher Accuracy

Tests _match_service_for_position across diverse fake business scenarios.
Each scenario has a lead conversation + services list + expected correct service.
We check if the matcher picks the RIGHT service (not the cheapest irrelevant one).

Usage: docker compose exec iron-setter python -m app.test_phase1
"""

import asyncio
import json
import os

from app.services.ai_client import classify, start_ai_clients, set_ai_context, clear_ai_context
from app.models import TokenUsage
from app.workflows.reactivation import _match_service_for_position

# ── Test Scenarios ──
# Each has: services, offers, lead conversation, service_interest,
# expected_service (correct), trap_service (cheapest promo that's WRONG)

SCENARIOS = [
    # === BEAUTY / MEDICAL SPA ===
    {
        "name": "Beauty: Neck concern → trap is $99 eye promo",
        "service_interest": "Turkey Neck Treatment",
        "timeline": """## CONVERSATION HISTORY

[Oct 15, 2025 — 3:42 PM]
DRIP: hi Maria! we have some amazing treatments for turkey neck at Dr. K Beauty. would you like to learn more?

[Oct 15, 2025 — 4:01 PM]
LEAD: yes how much is the neck treatment

[Oct 15, 2025 — 4:03 PM]
AI: our Morpheus8 RF Microneedling for the neck area starts at $800 per session. we also have some intro pricing available right now. want me to check what day works for a free consultation?

[Oct 15, 2025 — 4:15 PM]
LEAD: thats a lot let me think about it""",
        "services": """- Morpheus8 RF Microneedling (Neck): $800/session
- Morpheus8 RF Microneedling (Face): $750/session
- Smooth Eye Treatment: $99/session (INTRO PRICE)
- Lip Filler: $450
- Botox (per unit): $12
- PDO Thread Lift (Neck): $1,200
- Body Contouring: $350/session""",
        "offers": """[PROMO] Smooth Eye Treatment — $99 intro price (regular $250)
[PROMO] Body Contouring — $199 first session (regular $350)""",
        "expected_keywords": ["morpheus", "neck", "thread lift", "pdo"],
        "trap_keywords": ["eye", "smooth eye"],
    },
    {
        "name": "Beauty: Eye concern → correct is eye treatment",
        "service_interest": "Under Eye Treatment",
        "timeline": """## CONVERSATION HISTORY

[Sep 20, 2025 — 10:15 AM]
DRIP: hey Lisa! noticed you were looking at our under-eye treatments. still thinking about it?

[Sep 20, 2025 — 10:30 AM]
LEAD: yes i have really dark circles and bags under my eyes. what can you do for that

[Sep 20, 2025 — 10:32 AM]
AI: we have the Smooth Eye Treatment which is great for dark circles and under-eye bags. its $99 for your first session right now. want to come in for a consultation?

[Sep 20, 2025 — 10:45 AM]
LEAD: maybe next month""",
        "services": """- Smooth Eye Treatment: $250/session
- Morpheus8 RF Microneedling (Neck): $800/session
- PDO Thread Lift (Neck): $1,200
- Lip Filler: $450
- Body Contouring: $350/session""",
        "offers": """[PROMO] Smooth Eye Treatment — $99 intro price (regular $250)
[PROMO] PDO Thread Lift — $899 (regular $1,200)""",
        "expected_keywords": ["eye", "smooth eye"],
        "trap_keywords": ["neck", "thread lift", "pdo"],
    },
    {
        "name": "Beauty: Jowls/face → trap is eye promo",
        "service_interest": "Face Rejuvenation",
        "timeline": """## CONVERSATION HISTORY

[Aug 5, 2025 — 2:00 PM]
DRIP: hi Sandra! Dr. K Beauty has some great options for face rejuvenation. interested in learning more?

[Aug 5, 2025 — 2:20 PM]
LEAD: yes my jowls are really bothering me. what do you recommend

[Aug 5, 2025 — 2:22 PM]
AI: for jowls we'd recommend Morpheus8 RF Microneedling for the face area or a PDO Thread Lift. want to come in for a free consultation to see which would be best?

[Aug 5, 2025 — 2:30 PM]
LEAD: ok ill think about it""",
        "services": """- Morpheus8 RF Microneedling (Face): $750/session
- PDO Thread Lift (Face): $1,500
- Smooth Eye Treatment: $99/session (INTRO PRICE)
- Lip Filler: $450
- Body Contouring: $350/session
- Botox (per unit): $12""",
        "offers": """[PROMO] Smooth Eye Treatment — $99 intro price (regular $250)
[PROMO] Body Contouring — $199 first session (regular $350)""",
        "expected_keywords": ["morpheus", "face", "thread lift", "pdo", "botox"],
        "trap_keywords": ["eye", "smooth eye", "body contouring"],
    },

    # === DENTAL ===
    {
        "name": "Dental: Whitening interest → trap is $49 exam special",
        "service_interest": "Teeth Whitening",
        "timeline": """## CONVERSATION HISTORY

[Oct 1, 2025 — 11:00 AM]
DRIP: hey Mike! saw you were interested in teeth whitening at Bright Smile Dental. still thinking about it?

[Oct 1, 2025 — 11:30 AM]
LEAD: yeah my teeth are pretty yellow from coffee. how much is it

[Oct 1, 2025 — 11:32 AM]
AI: we offer Zoom whitening for $450 which can brighten your teeth up to 8 shades. we also have take-home kits for $199. want to schedule a consultation?

[Oct 1, 2025 — 12:00 PM]
LEAD: ill let you know""",
        "services": """- Zoom Teeth Whitening: $450
- Take-Home Whitening Kit: $199
- Dental Cleaning: $89
- Dental Exam + X-Rays: $49 (NEW PATIENT SPECIAL)
- Dental Implants: $3,500/tooth
- Invisalign: $4,500
- Veneers: $1,200/tooth""",
        "offers": """[PROMO] New Patient Special — Exam + X-Rays $49 (regular $150)
[PROMO] Free Whitening with Invisalign signup""",
        "expected_keywords": ["whitening", "zoom", "take-home"],
        "trap_keywords": ["exam", "cleaning", "implant"],
    },

    # === ROOFING ===
    {
        "name": "Roofing: Storm damage → trap is $99 gutter cleaning",
        "service_interest": "Roof Repair",
        "timeline": """## CONVERSATION HISTORY

[Sep 15, 2025 — 9:00 AM]
DRIP: hi Tom! I see you reached out about roof repair after the storm. everything ok with your roof?

[Sep 15, 2025 — 9:30 AM]
LEAD: yeah we had some shingles blow off in the storm last week. need someone to come look at it

[Sep 15, 2025 — 9:32 AM]
AI: sorry to hear that. we can send someone out for a free inspection this week. most storm damage repairs run $500-2500 depending on the extent. want me to get you on the schedule?

[Sep 15, 2025 — 10:00 AM]
LEAD: let me check with my insurance first""",
        "services": """- Storm Damage Repair: $500-$2,500
- Full Roof Replacement: $8,000-$15,000
- Gutter Installation: $800-$1,500
- Gutter Cleaning: $149
- Free Roof Inspection
- Attic Insulation: $1,200-$2,500""",
        "offers": """[PROMO] Gutter Cleaning — $99 (regular $149) through end of month
[PROMO] Free Roof Inspection — no obligation""",
        "expected_keywords": ["storm", "repair", "roof", "inspection"],
        "trap_keywords": ["gutter cleaning", "insulation"],
    },

    # === FITNESS ===
    {
        "name": "Fitness: Weight loss / PT → trap is $29 yoga promo",
        "service_interest": "Personal Training",
        "timeline": """## CONVERSATION HISTORY

[Oct 10, 2025 — 6:00 PM]
DRIP: hey Jessica! still interested in personal training at Iron Forge Fitness?

[Oct 10, 2025 — 6:15 PM]
LEAD: yes i need to lose about 30 pounds. ive been putting it off

[Oct 10, 2025 — 6:17 PM]
AI: totally understand. our personal training packages are designed for exactly that. we have a 12-session package for $600 or 24 sessions for $1,000. want to come in for a free fitness assessment?

[Oct 10, 2025 — 6:30 PM]
LEAD: maybe after the holidays""",
        "services": """- Personal Training (12 sessions): $600
- Personal Training (24 sessions): $1,000
- Group Fitness Classes: $99/month
- Yoga Membership: $79/month
- Nutrition Coaching: $200/month
- Gym Membership: $49/month""",
        "offers": """[PROMO] Yoga — First month $29 (regular $79)
[PROMO] Gym Membership — $29 first month (regular $49)""",
        "expected_keywords": ["personal training", "nutrition"],
        "trap_keywords": ["yoga", "gym membership"],
    },

    # === VAGUE CONVERSATION (fall back to Service Interest) ===
    {
        "name": "Beauty: Minimal convo, rely on Service Interest (Lip Filler)",
        "service_interest": "Lip Filler",
        "timeline": """## CONVERSATION HISTORY

[Nov 1, 2025 — 3:00 PM]
DRIP: hey Ashley! interested in learning about our services at Glow Med Spa?

[Nov 1, 2025 — 3:15 PM]
LEAD: yes

[Nov 1, 2025 — 3:17 PM]
AI: great! what are you most interested in? we have a range of treatments from lip filler to body contouring to skin rejuvenation.

[No further replies]""",
        "services": """- Lip Filler: $450
- Botox (per unit): $12
- Chemical Peel: $150
- Microdermabrasion: $99
- Body Contouring: $350/session
- Laser Hair Removal: $75/session""",
        "offers": """[PROMO] Laser Hair Removal — $49 first session (regular $75)
[PROMO] Chemical Peel — $99 (regular $150)""",
        "expected_keywords": ["lip filler"],
        "trap_keywords": ["laser", "hair removal", "chemical peel"],
    },

    # === LEAD REJECTED A SERVICE ===
    {
        "name": "Beauty: Lead hates needles (no Botox) → pick non-needle option",
        "service_interest": "Anti-Aging Treatments",
        "timeline": """## CONVERSATION HISTORY

[Sep 5, 2025 — 1:00 PM]
DRIP: hi Karen! Dr. K Beauty has some amazing anti-aging options. interested?

[Sep 5, 2025 — 1:20 PM]
LEAD: yes but i dont want botox. i dont like needles

[Sep 5, 2025 — 1:22 PM]
AI: totally understand! we have non-needle options like our Morpheus8 RF Microneedling which uses tiny pins with radiofrequency. its great for tightening and rejuvenation. $750 per session. want to learn more?

[Sep 5, 2025 — 1:30 PM]
LEAD: maybe""",
        "services": """- Botox (per unit): $12
- Morpheus8 RF Microneedling (Face): $750/session
- Chemical Peel: $150
- Smooth Eye Treatment: $99/session (INTRO PRICE)
- PDO Thread Lift: $1,500
- LED Light Therapy: $89/session""",
        "offers": """[PROMO] Smooth Eye Treatment — $99 intro price (regular $250)
[PROMO] LED Light Therapy — $49 first session (regular $89)""",
        "expected_keywords": ["morpheus", "chemical peel", "led", "thread lift"],
        "trap_keywords": ["botox"],  # lead explicitly rejected this
    },
]


def grade_result(selected: str, scenario: dict) -> tuple[str, str]:
    """Grade: PASS if matches expected, TRAP if picked the trap, WRONG otherwise."""
    sel = selected.lower()
    for kw in scenario["expected_keywords"]:
        if kw in sel:
            return "PASS", "matched expected"
    for kw in scenario["trap_keywords"]:
        if kw in sel:
            return "TRAP", f"fell for trap: {kw}"
    return "WRONG", "unexpected selection"


async def run_tests():
    """Run all scenarios and grade results."""
    start_ai_clients()

    testing_key = os.getenv("OPENROUTER_TESTING_KEY") or os.getenv("OPENROUTER_API_KEY", "")
    token_tracker = TokenUsage()
    set_ai_context(api_key=testing_key, token_tracker=token_tracker)

    print("=" * 70)
    print("PHASE 1 TEST: Service Matcher Accuracy (P2)")
    print("=" * 70)
    print()

    results = []

    for i, scenario in enumerate(SCENARIOS, 1):
        print(f"--- {i}/{len(SCENARIOS)}: {scenario['name']} ---")

        context = {
            "first_name": "TestLead",
            "service_interest": scenario["service_interest"],
            "timeline": scenario["timeline"],
            "services": scenario["services"],
            "offers": scenario.get("offers", ""),
        }

        result = await _match_service_for_position(context, position=2)
        selected = result.get("service_name", "")
        price = result.get("price", "")
        reasoning = result.get("reasoning", "")

        status, detail = grade_result(selected, scenario)
        icon = "PASS" if status == "PASS" else "FAIL"

        print(f"  [{icon}] Selected: {selected} ({price})")
        print(f"         Reason:   {reasoning}")
        if status != "PASS":
            print(f"         Issue:    {detail}")
        print()

        results.append({
            "scenario": scenario["name"],
            "selected": selected,
            "price": price,
            "status": status,
            "detail": detail,
            "reasoning": reasoning,
        })

    # P5 differentiation test
    print("--- P5 Differentiation Test ---")
    context_p5 = {
        "first_name": "TestLead",
        "service_interest": SCENARIOS[0]["service_interest"],
        "timeline": SCENARIOS[0]["timeline"],
        "services": SCENARIOS[0]["services"],
        "offers": SCENARIOS[0].get("offers", ""),
    }
    p5_result = await _match_service_for_position(
        context_p5, position=5, p2_service="Morpheus8 RF Microneedling (Neck)",
    )
    p5_selected = p5_result.get("service_name", "")
    p5_diff = "morpheus8 rf microneedling (neck)" not in p5_selected.lower()
    p5_not_eye = not any(kw in p5_selected.lower() for kw in ["eye", "smooth eye"])
    print(f"  P2 was: Morpheus8 RF Microneedling (Neck)")
    print(f"  P5 picked: {p5_selected} ({p5_result.get('price', '')})")
    print(f"  Different from P2: {'PASS' if p5_diff else 'FAIL'}")
    print(f"  Not irrelevant eye promo: {'PASS' if p5_not_eye else 'FAIL'}")
    print(f"  Reason: {p5_result.get('reasoning', '')}")
    print()

    clear_ai_context()

    # Summary
    total = len(results)
    passed = sum(1 for r in results if r["status"] == "PASS")
    trapped = sum(1 for r in results if r["status"] == "TRAP")
    wrong = sum(1 for r in results if r["status"] == "WRONG")

    print("=" * 70)
    print(f"P2 RESULTS: {passed}/{total} PASS ({passed/total*100:.0f}%)")
    if trapped:
        print(f"  TRAPPED (picked cheap irrelevant promo): {trapped}")
    if wrong:
        print(f"  WRONG (other mismatch): {wrong}")
    print(f"  P5 differentiation: {'PASS' if p5_diff else 'FAIL'}")
    print(f"  P5 not eye trap: {'PASS' if p5_not_eye else 'FAIL'}")
    print(f"  Total cost: ${token_tracker.estimated_cost():.4f}")
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(run_tests())
