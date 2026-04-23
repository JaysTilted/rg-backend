"""Test the reactivation qualification gate with temporal vs permanent disqualification.

5 scenarios:
1. Budget disqualification (temporary) → should reactivate
2. Outside service area (permanent) → should skip
3. Not ready yet (temporary) → should reactivate
4. Wrong customer type (permanent) → should skip
5. No qualification data → should reactivate (default)

Usage: docker compose exec iron-setter python -m app.test_qual_gate
"""

import asyncio
import os

from app.services.ai_client import start_ai_clients, set_ai_context, clear_ai_context
from app.models import TokenUsage
from app.workflows.reactivation import _qualification_gate

SCENARIOS = [
    {
        "name": "TEMPORARY — Budget too low for premium service",
        "expected": True,
        "context": {
            "contact_name": "Mike Johnson",
            "first_name": "Mike",
            "service_interest": "Full Kitchen Remodel",
            "business_name": "Elite Home Renovations",
            "timeline": """## CONVERSATION HISTORY

[Sep 15, 2025 — 2:00 PM]
LEAD: yeah i want to redo my kitchen but my budget is only about 10k

[Sep 15, 2025 — 2:05 PM]
AI: our full kitchen remodels start at $25,000. we also have a cabinet refacing option for $8,000-$12,000.

[Sep 15, 2025 — 2:15 PM]
LEAD: yeah thats way out of my range right now. maybe in a few months""",
            "booking_history": "No appointment history.",
            "services": "- Full Kitchen Remodel: $25,000-$50,000\n- Cabinet Refacing: $8,000-$12,000\n- Bathroom Remodel: $15,000-$30,000\n- Countertop Replacement: $3,000-$6,000",
            "offers": "",
            "bot_persona": "",
            "security_prompt": "",
            "supported_languages": "English",
            "detected_language": "English",
            "channel": "sms",
            "qualification_status": "disqualified",
            "qualification_notes": {
                "Budget": {
                    "status": "disqualified",
                    "evidence": "Lead said budget is only 10k, minimum for full remodel is 25k"
                }
            },
            "qualification_criteria": """### Global Criteria (apply to ALL services)
- **Budget** (REQUIRED)
  Qualified: Budget of $15,000 or more
  Disqualified: Budget under $15,000
  Undetermined: Budget not discussed""",
        },
    },
    {
        "name": "PERMANENT — Lives outside service area (different state)",
        "expected": False,
        "context": {
            "contact_name": "Sarah Williams",
            "first_name": "Sarah",
            "service_interest": "Roof Repair",
            "business_name": "Summit Roofing (Las Vegas)",
            "timeline": """## CONVERSATION HISTORY

[Oct 1, 2025 — 10:00 AM]
LEAD: hi i need my roof fixed. some shingles came off

[Oct 1, 2025 — 10:05 AM]
AI: we can definitely help with that. where are you located?

[Oct 1, 2025 — 10:10 AM]
LEAD: im in phoenix arizona

[Oct 1, 2025 — 10:12 AM]
AI: unfortunately we only service the Las Vegas metro area. I'm sorry we can't help!""",
            "booking_history": "No appointment history.",
            "services": "- Storm Damage Repair: $500-$2,500\n- Full Roof Replacement: $8,000-$15,000\n- Free Roof Inspection",
            "offers": "",
            "bot_persona": "",
            "security_prompt": "",
            "supported_languages": "English",
            "detected_language": "English",
            "channel": "sms",
            "qualification_status": "disqualified",
            "qualification_notes": {
                "Service Area": {
                    "status": "disqualified",
                    "evidence": "Lead is in Phoenix, Arizona. Business only serves Las Vegas metro area."
                }
            },
            "qualification_criteria": """### Global Criteria (apply to ALL services)
- **Service Area** (REQUIRED)
  Qualified: Located in Las Vegas, Henderson, North Las Vegas, or surrounding Clark County
  Disqualified: Located outside Clark County, Nevada
  Undetermined: Location not discussed""",
        },
    },
    {
        "name": "TEMPORARY — Needed to check with spouse",
        "expected": True,
        "context": {
            "contact_name": "James Park",
            "first_name": "James",
            "service_interest": "Home Buying",
            "business_name": "Realty One Group",
            "timeline": """## CONVERSATION HISTORY

[Sep 22, 2025 — 10:00 AM]
LEAD: we want to buy a home in henderson. budget 350-400k

[Sep 22, 2025 — 10:05 AM]
AI: there are some great options in that range. want to set up a showing?

[Sep 22, 2025 — 10:15 AM]
LEAD: let me check with my wife first and get back to you""",
            "booking_history": "No appointment history.",
            "services": "- Buyer Consultation (free)\n- Home Search + Showing Tours\n- First-Time Buyer Program",
            "offers": "",
            "bot_persona": "",
            "security_prompt": "",
            "supported_languages": "English",
            "detected_language": "English",
            "channel": "sms",
            "qualification_status": "undetermined",
            "qualification_notes": {
                "Spouse Approval": {
                    "status": "undetermined",
                    "evidence": "Lead said he needs to check with his wife before proceeding"
                }
            },
            "qualification_criteria": "",
        },
    },
    {
        "name": "PERMANENT — Commercial client, business is residential only",
        "expected": False,
        "context": {
            "contact_name": "Dave Martinez",
            "first_name": "Dave",
            "service_interest": "HVAC Repair",
            "business_name": "CoolAir Residential HVAC",
            "timeline": """## CONVERSATION HISTORY

[Aug 20, 2025 — 2:00 PM]
LEAD: i need hvac work done on my warehouse. its a 15000 sq ft commercial building

[Aug 20, 2025 — 2:05 PM]
AI: we only service residential properties. for commercial HVAC you'd need a commercial contractor.

[Aug 20, 2025 — 2:10 PM]
LEAD: oh ok. do you know anyone?""",
            "booking_history": "No appointment history.",
            "services": "- AC Repair: $150-$2,000\n- AC Unit Replacement: $3,500-$7,000\n- Annual Maintenance Plan: $149/year",
            "offers": "",
            "bot_persona": "",
            "security_prompt": "",
            "supported_languages": "English",
            "detected_language": "English",
            "channel": "sms",
            "qualification_status": "disqualified",
            "qualification_notes": {
                "Property Type": {
                    "status": "disqualified",
                    "evidence": "Lead has a 15,000 sq ft commercial warehouse. Business is residential only."
                }
            },
            "qualification_criteria": """### Global Criteria (apply to ALL services)
- **Property Type** (REQUIRED)
  Qualified: Residential property (house, condo, townhome, apartment)
  Disqualified: Commercial, industrial, or multi-unit property (4+ units)
  Undetermined: Property type not discussed""",
        },
    },
    {
        "name": "NO QUALIFICATION DATA — Should default to reactivate",
        "expected": True,
        "context": {
            "contact_name": "Ashley Brown",
            "first_name": "Ashley",
            "service_interest": "Dog Grooming",
            "business_name": "Pawfect Grooming Studio",
            "timeline": """## CONVERSATION HISTORY

[Sep 10, 2025 — 1:00 PM]
LEAD: how much for a full groom for a goldendoodle?

[Sep 10, 2025 — 1:05 PM]
AI: for a goldendoodle a full groom is usually $85-$120. when can you bring him in?

[Sep 10, 2025 — 1:15 PM]
LEAD: let me check my schedule""",
            "booking_history": "No appointment history.",
            "services": "- Full Dog Groom (large): $85-$120\n- Bath & Brush: $40-$60\n- Nail Trim: $15",
            "offers": "",
            "bot_persona": "",
            "security_prompt": "",
            "supported_languages": "English",
            "detected_language": "English",
            "channel": "sms",
            "qualification_status": "",
            "qualification_notes": None,
            "qualification_criteria": "",
        },
    },
]


async def run_tests():
    start_ai_clients()
    testing_key = os.getenv("OPENROUTER_TESTING_KEY") or os.getenv("OPENROUTER_API_KEY", "")
    token_tracker = TokenUsage()
    set_ai_context(api_key=testing_key, token_tracker=token_tracker)

    results = []
    for i, scenario in enumerate(SCENARIOS, 1):
        result = await _qualification_gate(scenario["context"])
        reactivate = result.get("reactivate", True)
        reason = result.get("reason", "")
        expected = scenario["expected"]
        passed = reactivate == expected

        results.append(passed)

        status = "PASS" if passed else "FAIL"
        decision = "REACTIVATE" if reactivate else "SKIP"
        expected_str = "REACTIVATE" if expected else "SKIP"

        print(f"\n{'='*60}")
        print(f"[{i}] {scenario['name']}")
        print(f"    Expected: {expected_str}")
        print(f"    Got:      {decision}")
        print(f"    Reason:   {reason}")
        print(f"    Status:   {status}")

    clear_ai_context()

    print(f"\n{'='*60}")
    print(f"RESULTS: {sum(results)}/{len(results)} passed")
    if all(results):
        print("ALL TESTS PASSED")
    else:
        for i, (passed, scenario) in enumerate(zip(results, SCENARIOS), 1):
            if not passed:
                print(f"  FAILED: [{i}] {scenario['name']}")


if __name__ == "__main__":
    asyncio.run(run_tests())
