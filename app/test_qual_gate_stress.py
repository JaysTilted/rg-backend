"""Deep stress test for reactivation qualification gate.

30 scenarios across 10+ industries covering every edge case:
- Temporary vs permanent disqualification
- Ambiguous cases
- Hostile/stop leads
- Multiple disqualifications
- Partial qualification
- Edge cases (competitor, wrong number, one-time service completed)

Usage: docker compose exec rg-backend python -m app.test_qual_gate_stress
"""

import asyncio
import os
import time

from app.services.ai_client import start_ai_clients, set_ai_context, clear_ai_context
from app.models import TokenUsage
from app.workflows.reactivation import _qualification_gate

SCENARIOS = [
    # ═══════════════════════════════════════════════════════════
    # TEMPORARY DISQUALIFICATIONS — Should all REACTIVATE
    # ═══════════════════════════════════════════════════════════

    {
        "name": "TEMP — Budget too low (med spa)",
        "expected": True,
        "context": {
            "contact_name": "Maria Garcia", "first_name": "Maria",
            "service_interest": "Morpheus8 RF Microneedling",
            "business_name": "Dr. K Beauty",
            "timeline": "[Oct 10] LEAD: how much is the neck treatment\n[Oct 10] AI: Morpheus8 starts at $800/session\n[Oct 10] LEAD: thats too expensive for me right now",
            "booking_history": "No appointment history.",
            "services": "- Morpheus8 RF Microneedling: $800/session\n- Smooth Eye Treatment: $99",
            "offers": "", "bot_persona": "", "security_prompt": "",
            "supported_languages": "English", "detected_language": "English", "channel": "sms",
            "qualification_status": "disqualified",
            "qualification_notes": {"Budget": {"status": "disqualified", "evidence": "Said $800 is too expensive right now"}},
            "qualification_criteria": "- **Budget** (REQUIRED)\n  Qualified: Can afford $200+\n  Disqualified: Cannot afford minimum service price",
        },
    },
    {
        "name": "TEMP — Scheduling conflict (fitness)",
        "expected": True,
        "context": {
            "contact_name": "Tom Reed", "first_name": "Tom",
            "service_interest": "Personal Training",
            "business_name": "Iron Forge Fitness",
            "timeline": "[Sep 5] LEAD: i work nights so im only free after 10pm\n[Sep 5] AI: our latest session is 8pm unfortunately\n[Sep 5] LEAD: that doesnt work for me",
            "booking_history": "No appointment history.",
            "services": "- Personal Training (12 sessions): $600\n- Group Classes: $99/month",
            "offers": "", "bot_persona": "", "security_prompt": "",
            "supported_languages": "English", "detected_language": "English", "channel": "sms",
            "qualification_status": "disqualified",
            "qualification_notes": {"Availability": {"status": "disqualified", "evidence": "Only available after 10pm, latest session is 8pm"}},
            "qualification_criteria": "- **Availability** (REQUIRED)\n  Qualified: Available during business hours (6am-8pm)\n  Disqualified: Only available outside business hours",
        },
    },
    {
        "name": "TEMP — Waiting on insurance (roofing)",
        "expected": True,
        "context": {
            "contact_name": "Carlos Mendez", "first_name": "Carlos",
            "service_interest": "Roof Repair",
            "business_name": "Summit Roofing",
            "timeline": "[Sep 15] LEAD: shingles blew off need someone to look at it\n[Sep 15] AI: we can send someone for a free inspection\n[Sep 15] LEAD: let me check with insurance first. adjuster coming next week",
            "booking_history": "No appointment history.",
            "services": "- Storm Damage Repair: $500-$2,500\n- Free Roof Inspection",
            "offers": "", "bot_persona": "", "security_prompt": "",
            "supported_languages": "English", "detected_language": "English", "channel": "sms",
            "qualification_status": "undetermined",
            "qualification_notes": {"Insurance": {"status": "undetermined", "evidence": "Waiting for insurance adjuster"}},
            "qualification_criteria": "",
        },
    },
    {
        "name": "TEMP — Needs spouse approval (real estate)",
        "expected": True,
        "context": {
            "contact_name": "James Park", "first_name": "James",
            "service_interest": "Home Buying",
            "business_name": "Realty One Group",
            "timeline": "[Sep 22] LEAD: looking for 350-400k home in henderson\n[Sep 22] AI: great options in that range. want to see some?\n[Sep 22] LEAD: need to talk to my wife first",
            "booking_history": "No appointment history.",
            "services": "- Buyer Consultation (free)\n- Home Search + Showings",
            "offers": "", "bot_persona": "", "security_prompt": "",
            "supported_languages": "English", "detected_language": "English", "channel": "sms",
            "qualification_status": "undetermined",
            "qualification_notes": {"Decision Maker": {"status": "undetermined", "evidence": "Needs to check with wife"}},
            "qualification_criteria": "",
        },
    },
    {
        "name": "TEMP — Seasonal timing (landscaping)",
        "expected": True,
        "context": {
            "contact_name": "Beth Turner", "first_name": "Beth",
            "service_interest": "Lawn Renovation",
            "business_name": "Green Valley Landscaping",
            "timeline": "[Nov 1] LEAD: i want to redo my whole front yard\n[Nov 1] AI: we can do a full lawn renovation starting at $2,500\n[Nov 1] LEAD: can we do this in the spring? its too cold now to plant anything",
            "booking_history": "No appointment history.",
            "services": "- Full Lawn Renovation: $2,500-$5,000\n- Tree Trimming: $200-$500\n- Weekly Maintenance: $150/month",
            "offers": "", "bot_persona": "", "security_prompt": "",
            "supported_languages": "English", "detected_language": "English", "channel": "sms",
            "qualification_status": "undetermined",
            "qualification_notes": {"Timing": {"status": "undetermined", "evidence": "Wants to wait until spring for planting"}},
            "qualification_criteria": "",
        },
    },
    {
        "name": "TEMP — Medical clearance needed (med spa)",
        "expected": True,
        "context": {
            "contact_name": "Donna Lewis", "first_name": "Donna",
            "service_interest": "Botox",
            "business_name": "Glow Aesthetics",
            "timeline": "[Oct 5] LEAD: i want botox but im on blood thinners right now\n[Oct 5] AI: you may need to pause blood thinners before treatment. we recommend consulting your doctor first\n[Oct 5] LEAD: ok ill ask my doctor at my next appointment",
            "booking_history": "No appointment history.",
            "services": "- Botox: $12/unit\n- Dermal Fillers: $600/syringe\n- Chemical Peel: $150",
            "offers": "", "bot_persona": "", "security_prompt": "",
            "supported_languages": "English", "detected_language": "English", "channel": "sms",
            "qualification_status": "undetermined",
            "qualification_notes": {"Medical Clearance": {"status": "undetermined", "evidence": "On blood thinners, needs doctor clearance before Botox"}},
            "qualification_criteria": "- **Medical Clearance** (REQUIRED)\n  Qualified: No contraindicated medications\n  Disqualified: On contraindicated medications without doctor clearance",
        },
    },
    {
        "name": "TEMP — Credit check pending (auto)",
        "expected": True,
        "context": {
            "contact_name": "Ray Thompson", "first_name": "Ray",
            "service_interest": "Used Car Purchase",
            "business_name": "Metro Auto Sales",
            "timeline": "[Sep 20] LEAD: i want to look at that 2022 camry\n[Sep 20] AI: great choice, want to come in for a test drive?\n[Sep 20] LEAD: yeah but my credit isnt great. can you guys do financing?\n[Sep 20] AI: we work with all credit levels. come in and we'll see what we can do\n[Sep 20] LEAD: let me check my credit score first",
            "booking_history": "No appointment history.",
            "services": "- Used Cars: $10,000-$35,000\n- Financing Available\n- Trade-In Appraisal (free)",
            "offers": "", "bot_persona": "", "security_prompt": "",
            "supported_languages": "English", "detected_language": "English", "channel": "sms",
            "qualification_status": "undetermined",
            "qualification_notes": {"Financing": {"status": "undetermined", "evidence": "Concerned about credit, checking score first"}},
            "qualification_criteria": "",
        },
    },
    {
        "name": "TEMP — Price objection on specific service, other services exist (dental)",
        "expected": True,
        "context": {
            "contact_name": "Linda Chen", "first_name": "Linda",
            "service_interest": "Veneers",
            "business_name": "Bright Smile Dental",
            "timeline": "[Oct 1] LEAD: how much are veneers?\n[Oct 1] AI: porcelain veneers are $1,200 per tooth\n[Oct 1] LEAD: holy crap thats way too much. forget it",
            "booking_history": "No appointment history.",
            "services": "- Porcelain Veneers: $1,200/tooth\n- Zoom Whitening: $450\n- Dental Cleaning: $89\n- Composite Bonding: $300/tooth",
            "offers": "[PROMO] Whitening $299 (reg $450)", "bot_persona": "", "security_prompt": "",
            "supported_languages": "English", "detected_language": "English", "channel": "sms",
            "qualification_status": "disqualified",
            "qualification_notes": {"Budget": {"status": "disqualified", "evidence": "Said $1,200/tooth for veneers is way too much"}},
            "qualification_criteria": "- **Budget** (REQUIRED)\n  Qualified: Budget of $500+\n  Disqualified: Budget under $200",
        },
    },

    # ═══════════════════════════════════════════════════════════
    # PERMANENT DISQUALIFICATIONS — Should all SKIP
    # ═══════════════════════════════════════════════════════════

    {
        "name": "PERM — Wrong state (plumbing)",
        "expected": False,
        "context": {
            "contact_name": "Greg Harris", "first_name": "Greg",
            "service_interest": "Pipe Repair",
            "business_name": "Vegas Plumbing Co (Las Vegas NV)",
            "timeline": "[Oct 5] LEAD: i have a leaking pipe under my sink\n[Oct 5] AI: we can send a plumber today. where are you located?\n[Oct 5] LEAD: im in seattle washington\n[Oct 5] AI: sorry we only serve the Las Vegas area",
            "booking_history": "No appointment history.",
            "services": "- Emergency Pipe Repair: $150-$500\n- Drain Cleaning: $99",
            "offers": "", "bot_persona": "", "security_prompt": "",
            "supported_languages": "English", "detected_language": "English", "channel": "sms",
            "qualification_status": "disqualified",
            "qualification_notes": {"Service Area": {"status": "disqualified", "evidence": "Located in Seattle, WA. Business only serves Las Vegas, NV."}},
            "qualification_criteria": "- **Service Area** (REQUIRED)\n  Qualified: Within Las Vegas metro area\n  Disqualified: Outside Nevada",
        },
    },
    {
        "name": "PERM — Commercial property, residential only (pest control)",
        "expected": False,
        "context": {
            "contact_name": "Frank Russo", "first_name": "Frank",
            "service_interest": "Pest Control",
            "business_name": "HomeGuard Pest Control (Residential Only)",
            "timeline": "[Sep 25] LEAD: we have a roach problem in our restaurant kitchen\n[Sep 25] AI: we only handle residential pest control. for commercial you'd need a commercial service\n[Sep 25] LEAD: oh ok thanks",
            "booking_history": "No appointment history.",
            "services": "- General Pest Treatment: $150\n- Termite Inspection: $99\n- Monthly Pest Plan: $49/month",
            "offers": "", "bot_persona": "", "security_prompt": "",
            "supported_languages": "English", "detected_language": "English", "channel": "sms",
            "qualification_status": "disqualified",
            "qualification_notes": {"Property Type": {"status": "disqualified", "evidence": "Has a restaurant (commercial). Business is residential only."}},
            "qualification_criteria": "- **Property Type** (REQUIRED)\n  Qualified: Residential property\n  Disqualified: Commercial property",
        },
    },
    {
        "name": "PERM — Too young for service (tattoo)",
        "expected": False,
        "context": {
            "contact_name": "Tyler Scott", "first_name": "Tyler",
            "service_interest": "Tattoo",
            "business_name": "Ink Masters Studio",
            "timeline": "[Oct 10] LEAD: i want to get a sleeve done\n[Oct 10] AI: awesome, we can do a consultation. how old are you?\n[Oct 10] LEAD: im 16\n[Oct 10] AI: sorry we can only tattoo clients 18 and older, even with parental consent",
            "booking_history": "No appointment history.",
            "services": "- Custom Tattoo: $150/hour\n- Touch-Up: $75\n- Piercing: $50-$100",
            "offers": "", "bot_persona": "", "security_prompt": "",
            "supported_languages": "English", "detected_language": "English", "channel": "sms",
            "qualification_status": "disqualified",
            "qualification_notes": {"Age": {"status": "disqualified", "evidence": "Lead is 16 years old. Must be 18+."}},
            "qualification_criteria": "- **Age** (REQUIRED)\n  Qualified: 18 years or older\n  Disqualified: Under 18",
        },
    },
    {
        "name": "PERM — Needs service business doesn't offer (chiropractor wants surgery)",
        "expected": False,
        "context": {
            "contact_name": "Nancy White", "first_name": "Nancy",
            "service_interest": "Back Surgery",
            "business_name": "Align Chiropractic",
            "timeline": "[Oct 8] LEAD: my doctor said i need spinal surgery. do you do that?\n[Oct 8] AI: we're a chiropractic office, we don't perform surgery. we offer adjustments and physical therapy.\n[Oct 8] LEAD: no i specifically need surgery. my disc is herniated badly",
            "booking_history": "No appointment history.",
            "services": "- Chiropractic Adjustment: $75\n- Physical Therapy: $100/session\n- Massage Therapy: $90/hour",
            "offers": "", "bot_persona": "", "security_prompt": "",
            "supported_languages": "English", "detected_language": "English", "channel": "sms",
            "qualification_status": "disqualified",
            "qualification_notes": {"Service Match": {"status": "disqualified", "evidence": "Needs spinal surgery. Business only offers chiropractic care, not surgical procedures."}},
            "qualification_criteria": "- **Service Match** (REQUIRED)\n  Qualified: Needs non-surgical musculoskeletal care\n  Disqualified: Needs surgical intervention",
        },
    },
    {
        "name": "PERM — Pet too large for facility (grooming)",
        "expected": False,
        "context": {
            "contact_name": "Kim Patel", "first_name": "Kim",
            "service_interest": "Dog Grooming",
            "business_name": "Tiny Paws Grooming (Small Dogs Only)",
            "timeline": "[Sep 30] LEAD: can you groom my great dane? hes about 150 pounds\n[Sep 30] AI: sorry we only groom dogs under 30 pounds. we're a small dog specialty salon.\n[Sep 30] LEAD: oh ok",
            "booking_history": "No appointment history.",
            "services": "- Small Dog Groom: $45-$65\n- Puppy First Groom: $35\n- Nail Trim: $15",
            "offers": "", "bot_persona": "", "security_prompt": "",
            "supported_languages": "English", "detected_language": "English", "channel": "sms",
            "qualification_status": "disqualified",
            "qualification_notes": {"Pet Size": {"status": "disqualified", "evidence": "Great Dane, 150 lbs. Facility only handles dogs under 30 lbs."}},
            "qualification_criteria": "- **Pet Size** (REQUIRED)\n  Qualified: Dog under 30 pounds\n  Disqualified: Dog over 30 pounds",
        },
    },

    # ═══════════════════════════════════════════════════════════
    # HARD STOPS — Should all SKIP (explicit stop/hostile/spam)
    # ═══════════════════════════════════════════════════════════

    {
        "name": "STOP — Lead said stop texting me",
        "expected": False,
        "context": {
            "contact_name": "Mark Davis", "first_name": "Mark",
            "service_interest": "AC Repair",
            "business_name": "CoolAir HVAC",
            "timeline": "[Aug 20] LEAD: my ac is broken\n[Aug 20] AI: we can send someone today\n[Aug 25] AI: hey just following up on your AC\n[Aug 25] LEAD: stop texting me. i already got it fixed elsewhere. do not contact me again.",
            "booking_history": "No appointment history.",
            "services": "- AC Repair: $150-$2,000",
            "offers": "", "bot_persona": "", "security_prompt": "",
            "supported_languages": "English", "detected_language": "English", "channel": "sms",
            "qualification_status": "",
            "qualification_notes": None,
            "qualification_criteria": "",
        },
    },
    {
        "name": "STOP — Hostile/threatening lead",
        "expected": False,
        "context": {
            "contact_name": "Unknown", "first_name": "",
            "service_interest": "Teeth Whitening",
            "business_name": "Bright Smile Dental",
            "timeline": "[Oct 1] LEAD: who is this\n[Oct 1] AI: hi this is Bright Smile Dental following up on your whitening inquiry\n[Oct 1] LEAD: i never asked about whitening. if you text me again im reporting you for harassment and calling my lawyer. this is your only warning.",
            "booking_history": "No appointment history.",
            "services": "- Zoom Whitening: $450",
            "offers": "", "bot_persona": "", "security_prompt": "",
            "supported_languages": "English", "detected_language": "English", "channel": "sms",
            "qualification_status": "",
            "qualification_notes": None,
            "qualification_criteria": "",
        },
    },
    {
        "name": "STOP — Wrong number / gibberish",
        "expected": False,
        "context": {
            "contact_name": "Unknown", "first_name": "",
            "service_interest": "",
            "business_name": "Summit Roofing",
            "timeline": "[Sep 10] AI: hi is this the homeowner? we noticed storm damage in your area\n[Sep 10] LEAD: asjdfklasdjf\n[Sep 12] AI: just following up\n[Sep 12] LEAD: wrong number dude",
            "booking_history": "No appointment history.",
            "services": "- Storm Damage Repair: $500-$2,500",
            "offers": "", "bot_persona": "", "security_prompt": "",
            "supported_languages": "English", "detected_language": "English", "channel": "sms",
            "qualification_status": "",
            "qualification_notes": None,
            "qualification_criteria": "",
        },
    },
    {
        "name": "STOP — Competitor fishing for info",
        "expected": False,
        "context": {
            "contact_name": "Steve", "first_name": "Steve",
            "service_interest": "Roof Replacement",
            "business_name": "Summit Roofing",
            "timeline": "[Oct 5] LEAD: how much do you charge for a full roof replacement?\n[Oct 5] AI: typically $8,000-$15,000 depending on size\n[Oct 5] LEAD: what materials do you use? whats your cost per square? what underlayment? who is your supplier?\n[Oct 5] AI: those are pretty specific questions. are you a homeowner looking for a quote?\n[Oct 5] LEAD: yeah totally just curious lol. so what shingle brand and whats your markup?",
            "booking_history": "No appointment history.",
            "services": "- Full Roof Replacement: $8,000-$15,000",
            "offers": "", "bot_persona": "", "security_prompt": "",
            "supported_languages": "English", "detected_language": "English", "channel": "sms",
            "qualification_status": "",
            "qualification_notes": None,
            "qualification_criteria": "",
        },
    },

    # ═══════════════════════════════════════════════════════════
    # DEFAULT REACTIVATE — Various ghosting/vague scenarios
    # ═══════════════════════════════════════════════════════════

    {
        "name": "DEFAULT — Ghosted after first message",
        "expected": True,
        "context": {
            "contact_name": "Amy Nelson", "first_name": "Amy",
            "service_interest": "Facial Treatment",
            "business_name": "Glow Aesthetics",
            "timeline": "[Oct 1] AI: hi Amy, thanks for your interest in our facial treatments. what are you looking to address?\n(no response)",
            "booking_history": "No appointment history.",
            "services": "- HydraFacial: $199\n- Chemical Peel: $150",
            "offers": "", "bot_persona": "", "security_prompt": "",
            "supported_languages": "English", "detected_language": "English", "channel": "sms",
            "qualification_status": "",
            "qualification_notes": None,
            "qualification_criteria": "",
        },
    },
    {
        "name": "DEFAULT — Said maybe later",
        "expected": True,
        "context": {
            "contact_name": "Chris Taylor", "first_name": "Chris",
            "service_interest": "Window Tinting",
            "business_name": "ShadePro Tinting",
            "timeline": "[Sep 15] LEAD: how much for tinting my car windows\n[Sep 15] AI: depends on the vehicle. usually $200-$400. want to come in for a quote?\n[Sep 15] LEAD: maybe later",
            "booking_history": "No appointment history.",
            "services": "- Car Window Tint: $200-$400\n- Home Window Film: $8/sqft",
            "offers": "", "bot_persona": "", "security_prompt": "",
            "supported_languages": "English", "detected_language": "English", "channel": "sms",
            "qualification_status": "",
            "qualification_notes": None,
            "qualification_criteria": "",
        },
    },
    {
        "name": "DEFAULT — Cancelled appointment, never rebooked",
        "expected": True,
        "context": {
            "contact_name": "Jen Morris", "first_name": "Jen",
            "service_interest": "Massage",
            "business_name": "Serenity Spa",
            "timeline": "[Oct 1] LEAD: id like to book a deep tissue massage\n[Oct 1] AI: great, how about Thursday at 2pm?\n[Oct 1] LEAD: perfect\n[Oct 3] LEAD: hey i need to cancel something came up\n[Oct 3] AI: no problem, want to reschedule?\n[Oct 3] LEAD: ill let you know",
            "booking_history": "Cancelled: Oct 3 — Deep Tissue Massage",
            "services": "- Deep Tissue Massage: $120/hr\n- Swedish Massage: $100/hr\n- Hot Stone: $140/hr",
            "offers": "", "bot_persona": "", "security_prompt": "",
            "supported_languages": "English", "detected_language": "English", "channel": "sms",
            "qualification_status": "",
            "qualification_notes": None,
            "qualification_criteria": "",
        },
    },
    {
        "name": "DEFAULT — Already booked and completed (repeatable service)",
        "expected": True,
        "context": {
            "contact_name": "Pat Kim", "first_name": "Pat",
            "service_interest": "Teeth Cleaning",
            "business_name": "Bright Smile Dental",
            "timeline": "[Aug 15] LEAD: i need a cleaning\n[Aug 15] AI: we have openings this week\n[Aug 15] LEAD: thursday works",
            "booking_history": "Completed: Aug 17 — Dental Cleaning",
            "services": "- Dental Cleaning: $89\n- Whitening: $450\n- Exam: $49",
            "offers": "", "bot_persona": "", "security_prompt": "",
            "supported_languages": "English", "detected_language": "English", "channel": "sms",
            "qualification_status": "",
            "qualification_notes": None,
            "qualification_criteria": "",
        },
    },
    {
        "name": "DEFAULT — No conversation at all",
        "expected": True,
        "context": {
            "contact_name": "Lisa Hernandez", "first_name": "Lisa",
            "service_interest": "Hair Color",
            "business_name": "Studio 77 Salon",
            "timeline": "(no conversation history)",
            "booking_history": "No appointment history.",
            "services": "- Full Color: $150\n- Highlights: $200\n- Balayage: $250",
            "offers": "", "bot_persona": "", "security_prompt": "",
            "supported_languages": "English", "detected_language": "English", "channel": "sms",
            "qualification_status": "",
            "qualification_notes": None,
            "qualification_criteria": "",
        },
    },
    {
        "name": "DEFAULT — Said not interested but no hostility",
        "expected": True,
        "context": {
            "contact_name": "Bob Wright", "first_name": "Bob",
            "service_interest": "Gym Membership",
            "business_name": "PowerFit Gym",
            "timeline": "[Sep 10] AI: hey Bob, still thinking about the gym membership?\n[Sep 10] LEAD: nah not really interested right now",
            "booking_history": "No appointment history.",
            "services": "- Monthly Membership: $49/month\n- Annual: $399/year",
            "offers": "[PROMO] First month $19", "bot_persona": "", "security_prompt": "",
            "supported_languages": "English", "detected_language": "English", "channel": "sms",
            "qualification_status": "",
            "qualification_notes": None,
            "qualification_criteria": "",
        },
    },

    # ═══════════════════════════════════════════════════════════
    # TRICKY EDGE CASES
    # ═══════════════════════════════════════════════════════════

    {
        "name": "EDGE — Disqualified on one criterion, qualified on another",
        "expected": True,
        "context": {
            "contact_name": "Rachel Green", "first_name": "Rachel",
            "service_interest": "Kitchen Remodel",
            "business_name": "Elite Home Renovations",
            "timeline": "[Oct 1] LEAD: i want to redo my kitchen. budget is about 8k\n[Oct 1] AI: our remodels start at $25k but we have cabinet refacing for $8-12k\n[Oct 1] LEAD: hmm the refacing sounds interesting. im in henderson",
            "booking_history": "No appointment history.",
            "services": "- Full Kitchen Remodel: $25,000-$50,000\n- Cabinet Refacing: $8,000-$12,000",
            "offers": "", "bot_persona": "", "security_prompt": "",
            "supported_languages": "English", "detected_language": "English", "channel": "sms",
            "qualification_status": "disqualified",
            "qualification_notes": {
                "Budget": {"status": "disqualified", "evidence": "Budget is $8k, minimum for full remodel is $25k"},
                "Service Area": {"status": "confirmed", "evidence": "Located in Henderson, NV"}
            },
            "qualification_criteria": "- **Budget** (REQUIRED)\n  Qualified: $15,000+\n  Disqualified: Under $15,000\n- **Service Area** (REQUIRED)\n  Qualified: Clark County NV\n  Disqualified: Outside Clark County",
        },
    },
    {
        "name": "EDGE — One-time service completed, no other services (wedding photography)",
        "expected": False,
        "context": {
            "contact_name": "Mia Johnson", "first_name": "Mia",
            "service_interest": "Wedding Photography",
            "business_name": "Captured Moments (Wedding Photography Only)",
            "timeline": "[Jun 1] LEAD: we need a photographer for our wedding\n[Jun 5] AI: congratulations! we'd love to capture your day\n[Jun 10] LEAD: booked!",
            "booking_history": "Completed: Jul 15 — Wedding Photography Package ($3,500)",
            "services": "- Wedding Photography Package: $3,500",
            "offers": "", "bot_persona": "", "security_prompt": "",
            "supported_languages": "English", "detected_language": "English", "channel": "sms",
            "qualification_status": "",
            "qualification_notes": None,
            "qualification_criteria": "",
        },
    },
    {
        "name": "EDGE — Completed service but business has other services",
        "expected": True,
        "context": {
            "contact_name": "Dan Cooper", "first_name": "Dan",
            "service_interest": "Oil Change",
            "business_name": "QuickLube Auto",
            "timeline": "[Aug 1] LEAD: need an oil change\n[Aug 1] AI: we can get you in today. $49 for conventional\n[Aug 1] LEAD: ill be there in an hour",
            "booking_history": "Completed: Aug 1 — Oil Change ($49)",
            "services": "- Oil Change: $49\n- Brake Service: $200\n- Tire Rotation: $30\n- Full Inspection: $89",
            "offers": "[PROMO] Free tire rotation with oil change", "bot_persona": "", "security_prompt": "",
            "supported_languages": "English", "detected_language": "English", "channel": "sms",
            "qualification_status": "",
            "qualification_notes": None,
            "qualification_criteria": "",
        },
    },
    {
        "name": "EDGE — Spanish-speaking lead in English-only business",
        "expected": True,
        "context": {
            "contact_name": "Rosa Gutierrez", "first_name": "Rosa",
            "service_interest": "House Cleaning",
            "business_name": "Sparkle Clean Services",
            "timeline": "[Sep 20] LEAD: hola necesito limpieza de casa\n[Sep 20] AI: hi Rosa, we'd be happy to help with house cleaning. our deep clean starts at $200.\n[Sep 20] LEAD: ok cuanto cuesta? sorry my english not very good",
            "booking_history": "No appointment history.",
            "services": "- Deep Clean: $200-$400\n- Weekly Cleaning: $120/visit\n- Move-Out Clean: $300-$500",
            "offers": "", "bot_persona": "", "security_prompt": "",
            "supported_languages": "English", "detected_language": "Spanish", "channel": "sms",
            "qualification_status": "",
            "qualification_notes": None,
            "qualification_criteria": "",
        },
    },
    {
        "name": "EDGE — Lead has upcoming appointment (should NOT reactivate)",
        "expected": False,
        "context": {
            "contact_name": "Tina Brooks", "first_name": "Tina",
            "service_interest": "Facial",
            "business_name": "Glow Aesthetics",
            "timeline": "[Oct 10] LEAD: i want to book a hydrafacial\n[Oct 10] AI: how about next Friday at 3pm?\n[Oct 10] LEAD: sounds good!",
            "booking_history": "Upcoming: Oct 20 — HydraFacial (confirmed)",
            "services": "- HydraFacial: $199\n- Chemical Peel: $150",
            "offers": "", "bot_persona": "", "security_prompt": "",
            "supported_languages": "English", "detected_language": "English", "channel": "sms",
            "qualification_status": "",
            "qualification_notes": None,
            "qualification_criteria": "",
        },
    },
    {
        "name": "EDGE — No-showed but expressed interest in rebooking",
        "expected": True,
        "context": {
            "contact_name": "Kevin Zhao", "first_name": "Kevin",
            "service_interest": "Personal Training",
            "business_name": "Iron Forge Fitness",
            "timeline": "[Sep 15] LEAD: i want to start PT\n[Sep 15] AI: great, how about Monday at 10am?\n[Sep 15] LEAD: perfect\n[Sep 18] AI: hey Kevin you missed your appointment. want to reschedule?\n[Sep 18] LEAD: yeah sorry something came up. can we do next week?\n(no further response)",
            "booking_history": "No-Show: Sep 18 — Personal Training Assessment",
            "services": "- Personal Training (12 sessions): $600\n- Group Classes: $99/month",
            "offers": "", "bot_persona": "", "security_prompt": "",
            "supported_languages": "English", "detected_language": "English", "channel": "sms",
            "qualification_status": "",
            "qualification_notes": None,
            "qualification_criteria": "",
        },
    },

    # ═══════════════════════════════════════════════════════════
    # ADDITIONAL SCENARIOS (30-50) — Mixed categories
    # ═══════════════════════════════════════════════════════════

    # --- More temporary disqualifications ---
    {
        "name": "TEMP — Pregnant, can't do treatment now (med spa)",
        "expected": True,
        "context": {
            "contact_name": "Samantha Reed", "first_name": "Samantha",
            "service_interest": "Botox",
            "business_name": "Glow Aesthetics",
            "timeline": "[Oct 5] LEAD: i want botox but im pregnant right now\n[Oct 5] AI: congratulations! we recommend waiting until after pregnancy and breastfeeding to do botox\n[Oct 5] LEAD: ok ill come back after the baby is born",
            "booking_history": "No appointment history.",
            "services": "- Botox: $12/unit\n- Dermal Fillers: $600/syringe\n- HydraFacial: $199",
            "offers": "", "bot_persona": "", "security_prompt": "",
            "supported_languages": "English", "detected_language": "English", "channel": "sms",
            "qualification_status": "disqualified",
            "qualification_notes": {"Medical": {"status": "disqualified", "evidence": "Currently pregnant, cannot receive Botox"}},
            "qualification_criteria": "- **Medical Clearance** (REQUIRED)\n  Qualified: Not pregnant or breastfeeding\n  Disqualified: Currently pregnant or breastfeeding",
        },
    },
    {
        "name": "TEMP — Landlord approval needed (home improvement)",
        "expected": True,
        "context": {
            "contact_name": "Alex Nguyen", "first_name": "Alex",
            "service_interest": "Window Replacement",
            "business_name": "ClearView Windows",
            "timeline": "[Sep 28] LEAD: i want to replace the windows in my apartment\n[Sep 28] AI: we can do a free estimate. are you the homeowner?\n[Sep 28] LEAD: no im renting. i need to ask my landlord if theyd pay for it",
            "booking_history": "No appointment history.",
            "services": "- Window Replacement: $300-$800/window\n- Storm Windows: $150/window",
            "offers": "", "bot_persona": "", "security_prompt": "",
            "supported_languages": "English", "detected_language": "English", "channel": "sms",
            "qualification_status": "undetermined",
            "qualification_notes": {"Decision Maker": {"status": "undetermined", "evidence": "Renter, needs landlord approval and funding"}},
            "qualification_criteria": "",
        },
    },
    {
        "name": "TEMP — Recovering from surgery, wants to wait (fitness)",
        "expected": True,
        "context": {
            "contact_name": "Derek Mills", "first_name": "Derek",
            "service_interest": "Personal Training",
            "business_name": "Iron Forge Fitness",
            "timeline": "[Oct 1] LEAD: i want to start training but i just had knee surgery 2 weeks ago\n[Oct 1] AI: totally understand. we can work around that when you're cleared by your doctor\n[Oct 1] LEAD: yeah doc said 6 weeks. ill reach out then",
            "booking_history": "No appointment history.",
            "services": "- Personal Training (12 sessions): $600\n- Physical Rehab Program: $400",
            "offers": "", "bot_persona": "", "security_prompt": "",
            "supported_languages": "English", "detected_language": "English", "channel": "sms",
            "qualification_status": "undetermined",
            "qualification_notes": {"Medical": {"status": "undetermined", "evidence": "Recovering from knee surgery, cleared in 6 weeks"}},
            "qualification_criteria": "",
        },
    },
    {
        "name": "TEMP — Wants to finish current contract first (cleaning)",
        "expected": True,
        "context": {
            "contact_name": "Heather Fox", "first_name": "Heather",
            "service_interest": "Weekly Cleaning",
            "business_name": "Sparkle Clean Services",
            "timeline": "[Sep 15] LEAD: im interested but im locked into a contract with another cleaning company until december\n[Sep 15] AI: no problem, reach out when your contract ends and we'll get you set up\n[Sep 15] LEAD: will do thanks",
            "booking_history": "No appointment history.",
            "services": "- Weekly Cleaning: $120/visit\n- Deep Clean: $200-$400\n- Move-Out Clean: $300-$500",
            "offers": "[PROMO] First clean 50% off", "bot_persona": "", "security_prompt": "",
            "supported_languages": "English", "detected_language": "English", "channel": "sms",
            "qualification_status": "undetermined",
            "qualification_notes": {"Availability": {"status": "undetermined", "evidence": "Under contract with another company until December"}},
            "qualification_criteria": "",
        },
    },

    # --- More permanent disqualifications ---
    {
        "name": "PERM — Allergic to all products used (salon)",
        "expected": False,
        "context": {
            "contact_name": "Wendy Clark", "first_name": "Wendy",
            "service_interest": "Hair Color",
            "business_name": "Studio 77 Salon",
            "timeline": "[Oct 8] LEAD: i want to color my hair but im severely allergic to PPD and all hair dyes\n[Oct 8] AI: unfortunately all our color products contain PPD or similar compounds. we dont carry hypoallergenic alternatives\n[Oct 8] LEAD: thats what i figured. thanks anyway",
            "booking_history": "No appointment history.",
            "services": "- Full Color: $150\n- Highlights: $200\n- Balayage: $250",
            "offers": "", "bot_persona": "", "security_prompt": "",
            "supported_languages": "English", "detected_language": "English", "channel": "sms",
            "qualification_status": "disqualified",
            "qualification_notes": {"Allergy": {"status": "disqualified", "evidence": "Severely allergic to PPD and all hair dye compounds. Salon has no hypoallergenic options."}},
            "qualification_criteria": "- **Product Compatibility** (REQUIRED)\n  Qualified: No allergies to salon products\n  Disqualified: Allergic to all available color products",
        },
    },
    {
        "name": "PERM — Wants to rent, business is sales only (real estate)",
        "expected": False,
        "context": {
            "contact_name": "Omar Hassan", "first_name": "Omar",
            "service_interest": "Home Rental",
            "business_name": "Realty One Group (Sales Only — No Rentals)",
            "timeline": "[Sep 30] LEAD: im looking for a 2 bedroom apartment to rent in henderson\n[Sep 30] AI: we actually only handle home sales, not rentals. you might want to try a property management company\n[Sep 30] LEAD: oh ok. know any good ones?",
            "booking_history": "No appointment history.",
            "services": "- Buyer Consultation (free)\n- Home Search + Showings\n- First-Time Buyer Program",
            "offers": "", "bot_persona": "", "security_prompt": "",
            "supported_languages": "English", "detected_language": "English", "channel": "sms",
            "qualification_status": "disqualified",
            "qualification_notes": {"Service Match": {"status": "disqualified", "evidence": "Looking for rental. Business only does sales."}},
            "qualification_criteria": "- **Service Match** (REQUIRED)\n  Qualified: Looking to buy a home\n  Disqualified: Looking to rent",
        },
    },
    {
        "name": "PERM — Vehicle too old for service (auto detailing with warranty)",
        "expected": False,
        "context": {
            "contact_name": "Phil Barrett", "first_name": "Phil",
            "service_interest": "Ceramic Coating",
            "business_name": "ProShine Detailing (Vehicles 2015+ Only)",
            "timeline": "[Oct 3] LEAD: how much to ceramic coat my 2003 honda civic\n[Oct 3] AI: we only apply ceramic coating to vehicles 2015 or newer due to our warranty requirements. older paint may not bond properly.\n[Oct 3] LEAD: well thats all i got so i guess not then",
            "booking_history": "No appointment history.",
            "services": "- Ceramic Coating (2015+): $800-$1,500\n- Paint Correction: $500-$1,000\n- Interior Detail: $200",
            "offers": "", "bot_persona": "", "security_prompt": "",
            "supported_languages": "English", "detected_language": "English", "channel": "sms",
            "qualification_status": "disqualified",
            "qualification_notes": {"Vehicle Year": {"status": "disqualified", "evidence": "2003 Honda Civic. Service requires 2015 or newer."}},
            "qualification_criteria": "- **Vehicle Year** (REQUIRED)\n  Qualified: 2015 or newer\n  Disqualified: Older than 2015",
        },
    },

    # --- More hard stops ---
    {
        "name": "STOP — Lead said they reported business to BBB",
        "expected": False,
        "context": {
            "contact_name": "Janet Stone", "first_name": "Janet",
            "service_interest": "Roof Repair",
            "business_name": "Summit Roofing",
            "timeline": "[Sep 1] LEAD: your guys scratched my car when they were on the roof\n[Sep 1] AI: im sorry to hear that, let me have my manager reach out\n[Sep 5] LEAD: nobody called me. ive filed a complaint with the BBB. dont contact me again.",
            "booking_history": "Completed: Aug 28 — Roof Repair ($1,200)",
            "services": "- Storm Damage Repair: $500-$2,500",
            "offers": "", "bot_persona": "", "security_prompt": "",
            "supported_languages": "English", "detected_language": "English", "channel": "sms",
            "qualification_status": "",
            "qualification_notes": None,
            "qualification_criteria": "",
        },
    },
    {
        "name": "STOP — Lead is a minor asking for adult service (tattoo variant)",
        "expected": False,
        "context": {
            "contact_name": "Jayden", "first_name": "Jayden",
            "service_interest": "Tattoo",
            "business_name": "Ink Masters Studio",
            "timeline": "[Oct 10] LEAD: can i get a small tattoo on my wrist?\n[Oct 10] AI: sure! are you 18 or older?\n[Oct 10] LEAD: im 14 but my mom said its ok\n[Oct 10] AI: sorry, we require all clients to be 18+, even with parental consent. its state law.",
            "booking_history": "No appointment history.",
            "services": "- Custom Tattoo: $150/hour\n- Piercing: $50-$100",
            "offers": "", "bot_persona": "", "security_prompt": "",
            "supported_languages": "English", "detected_language": "English", "channel": "sms",
            "qualification_status": "disqualified",
            "qualification_notes": {"Age": {"status": "disqualified", "evidence": "Lead is 14 years old. Must be 18+ by state law."}},
            "qualification_criteria": "- **Age** (REQUIRED)\n  Qualified: 18 years or older\n  Disqualified: Under 18",
        },
    },

    # --- More default reactivate ---
    {
        "name": "DEFAULT — Asked detailed questions then disappeared (solar)",
        "expected": True,
        "context": {
            "contact_name": "Marcus Bell", "first_name": "Marcus",
            "service_interest": "Solar Panels",
            "business_name": "SunPower Solar NV",
            "timeline": "[Sep 20] LEAD: how much for a 10kw system\n[Sep 20] AI: typically $20,000-$28,000 before the 30% federal tax credit\n[Sep 20] LEAD: what panels do you use? whats the warranty?\n[Sep 20] AI: we use REC Alpha panels with a 25-year warranty\n[Sep 20] LEAD: ok let me think about it\n(no further response)",
            "booking_history": "No appointment history.",
            "services": "- Residential Solar (6kW): $15,000-$18,000\n- Residential Solar (10kW): $20,000-$28,000\n- Battery Storage: $10,000-$15,000",
            "offers": "[PROMO] Free battery with 10kW+ system", "bot_persona": "", "security_prompt": "",
            "supported_languages": "English", "detected_language": "English", "channel": "sms",
            "qualification_status": "",
            "qualification_notes": None,
            "qualification_criteria": "",
        },
    },
    {
        "name": "DEFAULT — Multiple no-shows but keeps expressing interest (dental)",
        "expected": True,
        "context": {
            "contact_name": "Tony Reeves", "first_name": "Tony",
            "service_interest": "Dental Cleaning",
            "business_name": "Bright Smile Dental",
            "timeline": "[Aug 10] LEAD: i need a cleaning\n[Aug 10] AI: how about Wednesday?\n[Aug 10] LEAD: sure\n[Aug 14] AI: hey you missed your appointment. reschedule?\n[Aug 14] LEAD: yeah sorry. next week?\n[Aug 20] AI: you missed again. want to try one more time?\n[Aug 20] LEAD: ugh yeah im sorry im terrible at this. ill call when im ready",
            "booking_history": "No-Show: Aug 12\nNo-Show: Aug 19",
            "services": "- Dental Cleaning: $89\n- Exam: $49",
            "offers": "", "bot_persona": "", "security_prompt": "",
            "supported_languages": "English", "detected_language": "English", "channel": "sms",
            "qualification_status": "",
            "qualification_notes": None,
            "qualification_criteria": "",
        },
    },
    {
        "name": "DEFAULT — Sent just an emoji response (ambiguous)",
        "expected": True,
        "context": {
            "contact_name": "Nicki Tran", "first_name": "Nicki",
            "service_interest": "Eyelash Extensions",
            "business_name": "Lash Bar Studio",
            "timeline": "[Oct 1] AI: hi Nicki! still interested in lash extensions? we have openings this week\n[Oct 1] LEAD: 👍",
            "booking_history": "No appointment history.",
            "services": "- Classic Lashes: $150\n- Volume Lashes: $200\n- Lash Lift: $75",
            "offers": "", "bot_persona": "", "security_prompt": "",
            "supported_languages": "English", "detected_language": "English", "channel": "sms",
            "qualification_status": "",
            "qualification_notes": None,
            "qualification_criteria": "",
        },
    },
    {
        "name": "DEFAULT — Booked, completed, left a 5-star review (repeatable)",
        "expected": True,
        "context": {
            "contact_name": "Lori Adams", "first_name": "Lori",
            "service_interest": "Massage",
            "business_name": "Serenity Spa",
            "timeline": "[Jul 15] LEAD: id like to book a swedish massage\n[Jul 15] AI: great, how about Saturday at noon?\n[Jul 15] LEAD: perfect\n[Jul 20] LEAD: that was amazing!! left you a 5 star review",
            "booking_history": "Completed: Jul 17 — Swedish Massage ($100)",
            "services": "- Swedish Massage: $100/hr\n- Deep Tissue: $120/hr\n- Hot Stone: $140/hr\n- Couples Massage: $220",
            "offers": "[PROMO] 20% off second visit", "bot_persona": "", "security_prompt": "",
            "supported_languages": "English", "detected_language": "English", "channel": "sms",
            "qualification_status": "",
            "qualification_notes": None,
            "qualification_criteria": "",
        },
    },

    # --- More tricky edge cases ---
    {
        "name": "EDGE — Lead asked for a refund, got it, conversation ended neutral",
        "expected": True,
        "context": {
            "contact_name": "Peter Grant", "first_name": "Peter",
            "service_interest": "Window Tinting",
            "business_name": "ShadePro Tinting",
            "timeline": "[Aug 5] LEAD: the tint you did is peeling already\n[Aug 5] AI: sorry about that. we can redo it for free or issue a refund\n[Aug 5] LEAD: just give me the refund\n[Aug 5] AI: refund processed. sorry for the trouble\n[Aug 5] LEAD: thanks",
            "booking_history": "Completed: Jul 20 — Car Window Tint ($350) — Refunded",
            "services": "- Car Window Tint: $200-$400\n- Home Window Film: $8/sqft\n- Paint Protection Film: $500-$1,500",
            "offers": "", "bot_persona": "", "security_prompt": "",
            "supported_languages": "English", "detected_language": "English", "channel": "sms",
            "qualification_status": "",
            "qualification_notes": None,
            "qualification_criteria": "",
        },
    },
    {
        "name": "EDGE — Lead mentioned they're moving to the area soon",
        "expected": True,
        "context": {
            "contact_name": "Victoria Lane", "first_name": "Victoria",
            "service_interest": "Gym Membership",
            "business_name": "PowerFit Gym",
            "timeline": "[Sep 10] LEAD: im moving to vegas next month. do you have month to month?\n[Sep 10] AI: yes $49/month no contract. come check us out when you get here\n[Sep 10] LEAD: sweet will do!",
            "booking_history": "No appointment history.",
            "services": "- Monthly Membership: $49/month\n- Annual: $399/year\n- Day Pass: $15",
            "offers": "[PROMO] First month $19", "bot_persona": "", "security_prompt": "",
            "supported_languages": "English", "detected_language": "English", "channel": "sms",
            "qualification_status": "",
            "qualification_notes": None,
            "qualification_criteria": "",
        },
    },
    {
        "name": "EDGE — Lead was rude but not hostile (no stop request)",
        "expected": True,
        "context": {
            "contact_name": "Brad Foster", "first_name": "Brad",
            "service_interest": "AC Repair",
            "business_name": "CoolAir HVAC",
            "timeline": "[Aug 25] LEAD: my ac is broken again. your guys were just here last month and it still doesnt work\n[Aug 25] AI: im sorry about that. we can send someone out again at no charge\n[Aug 25] LEAD: yeah whatever. your service kinda sucks honestly\n[Aug 25] AI: i understand your frustration. when works best for us to come back?\n(no response)",
            "booking_history": "Completed: Jul 25 — AC Repair ($450)",
            "services": "- AC Repair: $150-$2,000\n- Annual Maintenance: $149/year",
            "offers": "", "bot_persona": "", "security_prompt": "",
            "supported_languages": "English", "detected_language": "English", "channel": "sms",
            "qualification_status": "",
            "qualification_notes": None,
            "qualification_criteria": "",
        },
    },
    {
        "name": "EDGE — Multiple criteria, all undetermined (new lead, no convo)",
        "expected": True,
        "context": {
            "contact_name": "Diane Young", "first_name": "Diane",
            "service_interest": "Full Kitchen Remodel",
            "business_name": "Elite Home Renovations",
            "timeline": "(no conversation history)",
            "booking_history": "No appointment history.",
            "services": "- Full Kitchen Remodel: $25,000-$50,000\n- Cabinet Refacing: $8,000-$12,000",
            "offers": "", "bot_persona": "", "security_prompt": "",
            "supported_languages": "English", "detected_language": "English", "channel": "sms",
            "qualification_status": "undetermined",
            "qualification_notes": {
                "Budget": {"status": "undetermined", "evidence": "Not discussed"},
                "Service Area": {"status": "undetermined", "evidence": "Not discussed"},
                "Homeowner": {"status": "undetermined", "evidence": "Not discussed"}
            },
            "qualification_criteria": "- **Budget** (REQUIRED)\n  Qualified: $15,000+\n  Disqualified: Under $15,000\n- **Service Area** (REQUIRED)\n  Qualified: Clark County NV\n  Disqualified: Outside Clark County\n- **Homeowner** (REQUIRED)\n  Qualified: Owns the property\n  Disqualified: Renter without landlord approval",
        },
    },
    {
        "name": "EDGE — Disqualified on required + confirmed on optional",
        "expected": False,
        "context": {
            "contact_name": "Steve Kim", "first_name": "Steve",
            "service_interest": "Roof Replacement",
            "business_name": "Summit Roofing (Las Vegas Only)",
            "timeline": "[Oct 1] LEAD: i need a new roof. im in san diego\n[Oct 1] AI: sorry we only serve Las Vegas\n[Oct 1] LEAD: oh. well i own my home and have good insurance if that matters",
            "booking_history": "No appointment history.",
            "services": "- Full Roof Replacement: $8,000-$15,000",
            "offers": "", "bot_persona": "", "security_prompt": "",
            "supported_languages": "English", "detected_language": "English", "channel": "sms",
            "qualification_status": "disqualified",
            "qualification_notes": {
                "Service Area": {"status": "disqualified", "evidence": "Located in San Diego, CA"},
                "Homeowner": {"status": "confirmed", "evidence": "Owns the property"},
                "Insurance": {"status": "confirmed", "evidence": "Has good insurance"}
            },
            "qualification_criteria": "- **Service Area** (REQUIRED)\n  Qualified: Las Vegas metro\n  Disqualified: Outside Nevada\n- **Homeowner** (optional)\n  Qualified: Owns property\n- **Insurance** (optional)\n  Qualified: Has homeowner's insurance",
        },
    },
    {
        "name": "EDGE — Lead explicitly said STOP then came back and said nevermind",
        "expected": True,
        "context": {
            "contact_name": "Cindy Marks", "first_name": "Cindy",
            "service_interest": "Hair Color",
            "business_name": "Studio 77 Salon",
            "timeline": "[Sep 1] LEAD: stop texting me\n[Sep 5] LEAD: hey actually sorry about that. i was having a bad day. im still interested in getting my hair done\n[Sep 5] AI: no worries at all! we'd love to have you in. any specific color you're thinking?\n(no response)",
            "booking_history": "No appointment history.",
            "services": "- Full Color: $150\n- Highlights: $200",
            "offers": "", "bot_persona": "", "security_prompt": "",
            "supported_languages": "English", "detected_language": "English", "channel": "sms",
            "qualification_status": "",
            "qualification_notes": None,
            "qualification_criteria": "",
        },
    },
    {
        "name": "EDGE — One-time service completed but business has many other services",
        "expected": True,
        "context": {
            "contact_name": "Yuki Tanaka", "first_name": "Yuki",
            "service_interest": "Bridal Makeup",
            "business_name": "Glamour Studio (Full Service Salon + Spa)",
            "timeline": "[Jun 15] LEAD: i need bridal makeup for my wedding\n[Jun 20] LEAD: everything was perfect thank you!",
            "booking_history": "Completed: Jul 1 — Bridal Makeup ($500)",
            "services": "- Bridal Makeup: $500\n- HydraFacial: $199\n- Lash Extensions: $150\n- Massage: $120\n- Hair Color: $200\n- Waxing: $50-$150",
            "offers": "[PROMO] 20% off for returning clients", "bot_persona": "", "security_prompt": "",
            "supported_languages": "English", "detected_language": "English", "channel": "sms",
            "qualification_status": "",
            "qualification_notes": None,
            "qualification_criteria": "",
        },
    },
]


NUM_ROUNDS = 3


async def run_single_scenario(scenario: dict) -> dict:
    """Run one scenario and return result dict."""
    result = await _qualification_gate(scenario["context"])
    reactivate = result.get("reactivate", True)
    return {
        "name": scenario["name"],
        "expected": scenario["expected"],
        "got": reactivate,
        "passed": reactivate == scenario["expected"],
        "reason": result.get("reason", ""),
    }


async def run_round(round_num: int) -> list[dict]:
    """Run all scenarios concurrently for one round."""
    tasks = [run_single_scenario(s) for s in SCENARIOS]
    return await asyncio.gather(*tasks)


async def run_tests():
    start_ai_clients()
    testing_key = os.getenv("OPENROUTER_TESTING_KEY") or os.getenv("OPENROUTER_API_KEY", "")
    token_tracker = TokenUsage()
    set_ai_context(api_key=testing_key, token_tracker=token_tracker)

    print(f"Running {len(SCENARIOS)} scenarios x {NUM_ROUNDS} rounds (async)\n")
    t0 = time.perf_counter()

    all_rounds: list[list[dict]] = []
    for r in range(1, NUM_ROUNDS + 1):
        rt0 = time.perf_counter()
        results = await run_round(r)
        elapsed_r = time.perf_counter() - rt0
        passed = sum(1 for x in results if x["passed"])
        print(f"Round {r}: {passed}/{len(results)} passed ({elapsed_r:.1f}s)")
        all_rounds.append(results)

    elapsed = time.perf_counter() - t0
    clear_ai_context()

    # Consistency analysis
    print(f"\n{'='*70}")
    print(f"CONSISTENCY ANALYSIS — {len(SCENARIOS)} scenarios x {NUM_ROUNDS} rounds")
    print(f"{'='*70}")

    inconsistent = []
    all_failed = []

    for i, scenario in enumerate(SCENARIOS):
        decisions = [all_rounds[r][i]["got"] for r in range(NUM_ROUNDS)]
        expected = scenario["expected"]
        all_correct = all(d == expected for d in decisions)
        all_same = len(set(decisions)) == 1

        if not all_correct:
            correct_count = sum(1 for d in decisions if d == expected)
            reasons = [all_rounds[r][i]["reason"] for r in range(NUM_ROUNDS)]

            if not all_same:
                inconsistent.append((i + 1, scenario["name"], expected, decisions, reasons))
            else:
                # Consistently wrong
                all_failed.append((i + 1, scenario["name"], expected, decisions[0], reasons[0]))

    # Print per-scenario results
    for i, scenario in enumerate(SCENARIOS):
        decisions = [all_rounds[r][i]["got"] for r in range(NUM_ROUNDS)]
        expected = scenario["expected"]
        marks = []
        for d in decisions:
            if d == expected:
                marks.append("✓")
            else:
                marks.append("✗")

        status_str = " ".join(marks)
        exp_str = "REACTIVATE" if expected else "SKIP"
        print(f"[{i+1:2d}] {status_str} | {exp_str:10s} | {scenario['name']}")

    # Summary
    total_checks = len(SCENARIOS) * NUM_ROUNDS
    total_passed = sum(sum(1 for x in r if x["passed"]) for r in all_rounds)

    print(f"\n{'='*70}")
    print(f"TOTAL: {total_passed}/{total_checks} checks passed")
    print(f"Time: {elapsed:.1f}s | Cost: ${token_tracker.estimated_cost():.4f}")

    if inconsistent:
        print(f"\nINCONSISTENT ({len(inconsistent)}) — different answers across rounds:")
        for num, name, expected, decisions, reasons in inconsistent:
            exp_str = "REACTIVATE" if expected else "SKIP"
            dec_strs = ["REACTIVATE" if d else "SKIP" for d in decisions]
            print(f"\n  [{num}] {name}")
            print(f"       Expected: {exp_str}")
            for r, (d, reason) in enumerate(zip(dec_strs, reasons), 1):
                marker = "✓" if decisions[r-1] == expected else "✗"
                print(f"       Round {r}: {marker} {d} — {reason}")

    if all_failed:
        print(f"\nCONSISTENTLY WRONG ({len(all_failed)}) — same wrong answer every round:")
        for num, name, expected, got, reason in all_failed:
            print(f"  [{num}] {name}")
            print(f"       Expected: {'REACTIVATE' if expected else 'SKIP'}")
            print(f"       Got:      {'REACTIVATE' if got else 'SKIP'}")
            print(f"       Reason:   {reason}")

    if not inconsistent and not all_failed:
        print("\nALL TESTS PASSED — 100% CONSISTENT ACROSS ALL ROUNDS")


if __name__ == "__main__":
    asyncio.run(run_tests())
