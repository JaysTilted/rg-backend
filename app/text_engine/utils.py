"""Shared helpers for the text engine pipeline.

Deduplicates code that was previously copy-pasted across multiple
text engine files.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import Any

from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)


# =========================================================================
# DATETIME & TIMEZONE HELPERS
# =========================================================================


def parse_datetime(ts: str | datetime, fallback_tz: ZoneInfo | None = None) -> datetime:
    """Parse an ISO 8601 timestamp string into a timezone-aware datetime.

    Accepts both str and datetime inputs. Falls back to UTC now on failure.
    Replaces _parse_ts (timeline.py) and _parse_ghl_timestamp (conversation_sync.py).

    When ``fallback_tz`` is provided, naive datetimes are interpreted in
    that timezone (GHL returns naive times in the location timezone).
    Otherwise UTC is assumed.
    """
    if isinstance(ts, datetime):
        if ts.tzinfo is None:
            return ts.replace(tzinfo=fallback_tz or timezone.utc)
        return ts
    if not ts:
        return datetime.now(timezone.utc)
    try:
        dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=fallback_tz or timezone.utc)
        return dt
    except (ValueError, TypeError):
        return datetime.now(timezone.utc)


def get_timezone(config: dict) -> ZoneInfo:
    """Resolve the client's timezone from config with America/Chicago fallback."""
    tz_name = config.get("timezone", "America/Chicago")
    try:
        return ZoneInfo(tz_name)
    except (KeyError, Exception):
        return ZoneInfo("America/Chicago")


_DAY_ORDER = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
_DEFAULT_DAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]


def extract_business_hours(config: dict) -> tuple[str, str, list[str]]:
    """Extract (start_time, end_time, enabled_day_names) from business_schedule JSONB.

    Falls back to defaults if not configured.
    Returns: ("09:00", "17:00", ["Monday", "Tuesday", ...])
    """
    schedule = config.get("business_schedule")
    if not schedule or not isinstance(schedule, dict):
        return ("09:00", "17:00", _DEFAULT_DAYS)

    enabled_days: list[str] = []
    starts: list[str] = []
    ends: list[str] = []

    for day_key in _DAY_ORDER:
        day_cfg = schedule.get(day_key)
        if not day_cfg or not isinstance(day_cfg, dict):
            continue
        if day_cfg.get("enabled"):
            enabled_days.append(day_key.capitalize())
            if day_cfg.get("start"):
                starts.append(day_cfg["start"])
            if day_cfg.get("end"):
                ends.append(day_cfg["end"])

    if not enabled_days:
        return ("09:00", "17:00", _DEFAULT_DAYS)

    # Use the most common start/end times (or first if all different)
    start_time = max(set(starts), key=starts.count) if starts else "09:00"
    end_time = max(set(ends), key=ends.count) if ends else "17:00"

    return (start_time, end_time, enabled_days)


# =========================================================================
# CONTACT DETAILS FORMATTING
# =========================================================================

_PERSONALIZATION_NOTE = "(Make sure to reference these details of the user in your conversation to make it personalised for the user)"


def format_contact_details(ctx: Any) -> str:
    """Build the contact details block used in agent prompts.

    Returns Name/Email/Phone/Source lines (only fields that have values),
    prefixed with a personalization note.  Falls back to a placeholder
    when nothing is available.
    """
    parts: list[str] = []
    if ctx.contact_name:
        parts.append(f"Name: {ctx.contact_name}")
    if ctx.contact_email:
        parts.append(f"Email: {ctx.contact_email}")
    if ctx.contact_phone:
        parts.append(f"Phone: {ctx.contact_phone}")
    if ctx.contact_source:
        parts.append(f"Source: {ctx.contact_source}")
    if not parts:
        return "No contact details available"
    return f"{_PERSONALIZATION_NOTE}\n" + "\n".join(parts)


# =========================================================================
# BOT NAME EXTRACTION
# =========================================================================

# Common names for bot name validation (ported from n8n Bot Name Extractor1 node)
_COMMON_NAMES = {
    "emily", "emma", "olivia", "ava", "sophia", "isabella", "mia", "charlotte", "amelia", "harper",
    "sarah", "jessica", "ashley", "amanda", "stephanie", "jennifer", "elizabeth", "nicole", "rachel", "michelle",
    "david", "james", "john", "michael", "robert", "william", "joseph", "thomas", "charles", "daniel",
    "alex", "sam", "taylor", "jordan", "casey", "riley", "morgan", "jamie", "drew", "cameron",
    "lisa", "karen", "nancy", "betty", "helen", "sandra", "donna", "carol", "ruth", "sharon",
    "amy", "anna", "kate", "katie", "kelly", "kim", "laura", "linda", "maria", "mary",
    "matt", "matthew", "mark", "steve", "steven", "paul", "peter", "brian", "chris", "christopher",
    "ryan", "kevin", "jason", "justin", "brandon", "andrew", "josh", "joshua", "tyler", "aaron",
    "grace", "lily", "zoe", "chloe", "natalie", "hannah", "leah", "victoria", "audrey", "claire",
    "jack", "luke", "ethan", "noah", "mason", "logan", "lucas", "liam", "owen", "henry",
}

# Regex patterns for extracting bot name from persona text (ported from n8n)
_BOT_NAME_PATTERNS = [
    re.compile(r"Your name is ([A-Za-z]+)[\s,.]", re.IGNORECASE),
    re.compile(r"Your name is ([A-Za-z]+)$", re.IGNORECASE),
    re.compile(r"You're ([A-Za-z]+)[\s,.]", re.IGNORECASE),
    re.compile(r"You're ([A-Za-z]+)$", re.IGNORECASE),
    re.compile(r"You are ([A-Za-z]+)[\s,.]", re.IGNORECASE),
    re.compile(r"You are ([A-Za-z]+)$", re.IGNORECASE),
    re.compile(r"I'm ([A-Za-z]+)[\s,.]", re.IGNORECASE),
    re.compile(r"I am ([A-Za-z]+)[\s,.]", re.IGNORECASE),
    re.compile(r"My name is ([A-Za-z]+)[\s,.]", re.IGNORECASE),
    re.compile(r"My name is ([A-Za-z]+)$", re.IGNORECASE),
    re.compile(r"Call me ([A-Za-z]+)[\s,.]", re.IGNORECASE),
    re.compile(r"Call me ([A-Za-z]+)$", re.IGNORECASE),
    re.compile(r"Name:\s*([A-Za-z]+)", re.IGNORECASE),
    re.compile(r"Name\s*-\s*([A-Za-z]+)", re.IGNORECASE),
    re.compile(r"This is ([A-Za-z]+)[\s,.]", re.IGNORECASE),
    re.compile(r"This is ([A-Za-z]+)$", re.IGNORECASE),
    re.compile(r"^([A-Za-z]+) here[\s,.]", re.IGNORECASE | re.MULTILINE),
    re.compile(r"\n([A-Za-z]+) here[\s,.]", re.IGNORECASE),
    re.compile(r"Respond as ([A-Za-z]+)[\s,.]", re.IGNORECASE),
    re.compile(r"Act as ([A-Za-z]+)[\s,.]", re.IGNORECASE),
    re.compile(r"Playing as ([A-Za-z]+)[\s,.]", re.IGNORECASE),
    re.compile(r"Play the role of ([A-Za-z]+)[\s,.]", re.IGNORECASE),
    re.compile(r"Introduce yourself as ([A-Za-z]+)[\s,.]", re.IGNORECASE),
    re.compile(r"Introduce yourself as ([A-Za-z]+)$", re.IGNORECASE),
    re.compile(r"Known as ([A-Za-z]+)[\s,.]", re.IGNORECASE),
    re.compile(r"Go by ([A-Za-z]+)[\s,.]", re.IGNORECASE),
    re.compile(r"Persona:\s*([A-Za-z]+)", re.IGNORECASE),
    re.compile(r"Character:\s*([A-Za-z]+)", re.IGNORECASE),
    re.compile(r"Bot Name:\s*([A-Za-z]+)", re.IGNORECASE),
    re.compile(r"Agent Name:\s*([A-Za-z]+)", re.IGNORECASE),
    re.compile(r'^#+ ([A-Za-z]+)\s*$', re.IGNORECASE | re.MULTILINE),
    re.compile(r'# Team Member Persona "([A-Za-z]+)"', re.IGNORECASE),
    re.compile(r"# Bot Persona - ([A-Za-z]+)", re.IGNORECASE),
    re.compile(r"You'll be ([A-Za-z]+)[\s,.]", re.IGNORECASE),
    re.compile(r"You will be ([A-Za-z]+)[\s,.]", re.IGNORECASE),
    re.compile(r"^As ([A-Za-z]+),", re.IGNORECASE | re.MULTILINE),
    re.compile(r"\nAs ([A-Za-z]+),", re.IGNORECASE),
    re.compile(r"([A-Za-z]+) is your name", re.IGNORECASE),
    re.compile(r"([A-Za-z]+),?\s+(?:the\s+)?(?:front desk|receptionist|assistant|coordinator|representative|team member)", re.IGNORECASE),
    re.compile(r"(?:front desk|receptionist|assistant|coordinator|representative|team member)\s+(?:named\s+)?([A-Za-z]+)", re.IGNORECASE),
]


def extract_bot_name(persona_prompt: str) -> str:
    """Extract the bot persona name from the bot_persona prompt text.

    Uses 30+ regex patterns (matching n8n's Bot Name Extractor) with a
    100-name validation list and "The Team" fallback.
    """
    if not persona_prompt:
        return "The Team"

    # Try each regex pattern in order
    for pattern in _BOT_NAME_PATTERNS:
        m = pattern.search(persona_prompt)
        if m and m.group(1):
            name = m.group(1).strip()
            if 2 <= len(name) <= 15:
                return name[0].upper() + name[1:].lower()

    # Fallback: search for any common name in the text
    for name in _COMMON_NAMES:
        if re.search(rf"\b{name}\b", persona_prompt, re.IGNORECASE):
            return name[0].upper() + name[1:].lower()

    return "The Team"


# =========================================================================
# CUSTOM FIELD EXTRACTION
# =========================================================================


def get_custom_field(contact: dict, field_key: str) -> str:
    """Get a custom field value from a GHL contact dict."""
    for cf in contact.get("customFields", contact.get("customField", [])):
        key = cf.get("key", "") or cf.get("name", "")
        if key.lower() == field_key.lower():
            return cf.get("field_value", cf.get("value", ""))
    return ""


# =========================================================================
# EMAIL FORMATTING
# =========================================================================

_EMAIL_FORMAT_PROMPT = """\
<role>
You are an email formatter. Your job is to take SMS-style messages and reformat them for email delivery without changing the content, meaning, or length.

## Your Tasks

1. **Greeting** - Ensure appropriate email greeting exists
2. **Body** - Add line breaks for email readability
3. **Links** - Convert URLs to clean hyperlinks
4. **Sign-off** - Add professional signature
5. **HTML** - Wrap everything in proper HTML structure
</role>

<task_greeting>
## Task 1: Greeting

Check if the message starts with a greeting (Hey, Hi, Hello, Good morning/afternoon/evening).

**If greeting exists with name:**
Keep as-is.
Example: "Hey John," → "Hey John,"

**If greeting exists without name and first_name is available:**
Add the name.
Example: "Hey," → "Hey John,"

**If no greeting and first_name is available:**
Add greeting.
Example: "Checking in about..." → "Hey John,\\n\\nChecking in about..."

**If no greeting and no first_name:**
Add generic greeting.
Example: "Checking in about..." → "Hi there,\\n\\nChecking in about..."

**Never duplicate greetings.** If message already has "Hey John," do not add another.
</task_greeting>

<task_body>
## Task 2: Body Line Breaks

Add line breaks to improve email readability. The content stays identical - you're only adding whitespace.

**Rules:**
- Blank line after greeting
- Blank line between distinct thoughts or topics
- Blank line before questions
- Blank line before sign-off
- Keep related sentences together (same paragraph)

**Example Input:**
"Hey John, checking in about your landscaping project. We have openings Thursday and Friday. Let me know what works!"

**Example Output:**
"Hey John,

Checking in about your landscaping project. We have openings Thursday and Friday.

Let me know what works!"

**Do NOT:**
- Change any words
- Add or remove content
- Extend the message
- Add filler phrases
- Change the tone
</task_body>

<task_links>
## Task 3: Links

Detect URLs and convert to clean hyperlinks.

**Pattern A - Context phrase before URL:**
Input: "Book here: https://calendly.com/example"
Output: "<a href="https://calendly.com/example">Book here</a>"

Input: "Schedule at https://calendly.com/example"
Output: "<a href="https://calendly.com/example">Schedule here</a>"

**Pattern B - Standalone URL:**
Input: "https://calendly.com/example"
Output: "<a href="https://calendly.com/example">Schedule Here</a>"

**Pattern C - URL mid-sentence:**
Input: "You can book at https://calendly.com/example whenever you're ready"
Output: "You can book <a href="https://calendly.com/example">here</a> whenever you're ready"

**Rules:**
- Remove the raw URL from visible text
- Use context phrase as anchor text when available
- Default anchor text: "Schedule Here" for calendar links, "Click Here" for other links
- Preserve surrounding punctuation
</task_links>

<task_signoff>
## Task 4: Sign-off

Add signature block after the message body.

**Format:**
- Bot name
Business name
(phone number)

**Example:**
- Emily
ABC Landscaping
(555) 123-4567

**Rules:**
- Always include all three lines
- Use the exact bot_name, business_name, and business_phone provided
- Blank line before signature
- Hyphen before bot name
</task_signoff>

<task_html>
## Task 5: HTML Wrapping

Wrap the complete formatted email in HTML.

**Structure:**
<html>
<body>
{formatted_content_with_<br>_tags_for_line_breaks}
</body>
</html>

**Rules:**
- Convert all line breaks to <br> tags
- Blank lines become <br><br>
- Links use <a href=""> tags
- No inline styles
- No colors
- No font specifications
- Plain HTML structure only
</task_html>

<complete_example>
## Complete Example

**Inputs:**
- agent_response: "Hey, checking in about your Botox consultation. We have openings this Thursday at 2pm or Friday at 10am. Book here: https://calendly.com/drk/botox Let me know if either works!"
- first_name: "Sarah"
- bot_name: "Emily"
- business_name: "Dr. K Beauty"
- business_phone: "(702) 555-0123"

**Output:**
{{
  "emailBody": "<html><body>Hey Sarah,<br><br>Checking in about your Botox consultation. We have openings this Thursday at 2pm or Friday at 10am.<br><br><a href=\\"https://calendly.com/drk/botox\\">Book here</a> - let me know if either works!<br><br>- Emily<br>Dr. K Beauty<br>(702) 555-0123</body></html>"
}}
</complete_example>

<rules>
## Critical Rules

1. **Never change the message content** - Same words, same meaning, same length
2. **Never add content** - No "Hope this helps!", no "Looking forward to hearing from you!"
3. **Never remove content** - Every word from original must appear
4. **Always output valid HTML** - Properly closed tags
5. **Always include signature** - Even if message seems complete
6. **Handle missing inputs gracefully:**
   - No first_name → Use "Hi there," greeting
   - No bot_name → Use "The Team" in signature
   - No business_phone → Omit phone line from signature
</rules>
"""

# Task 6 appended when media_url is provided
_EMAIL_MEDIA_SECTION = """
---

## Task 6: Media

If a media URL is provided (not "None" or empty):
- Add an `<img>` tag BEFORE the sign-off section
- Use this exact format: `<img src="MEDIA_URL" alt="Follow-up media" \
style="max-width: 480px; border-radius: 8px; margin: 12px 0;" />`
- GIFs will animate automatically in most email clients
- Place it after the message body, before the signature
- If no media URL is provided or it says "None", skip this step entirely — do NOT add any image tag
"""

_EMAIL_FORMAT_SCHEMA = {
    "type": "object",
    "properties": {
        "emailBody": {"type": "string", "description": "The formatted HTML email body"},
    },
    "required": ["emailBody"],
    "additionalProperties": False,
}


async def format_email_html(
    message: str,
    *,
    bot_persona: str,
    contact_name: str,
    contact_email: str,
    contact_phone: str,
    company: str,
    business_phone: str,
    media_url: str | None = None,
    model: str = "google/gemini-2.5-flash",
    temperature: float = 0.3,
) -> str:
    """Format a message as email HTML using LLM.

    Works for both reply emails (no media_url) and follow-up emails
    (with optional media_url for embedded GIFs/images).
    """
    from app.services.ai_client import classify

    bot_name = extract_bot_name(bot_persona)
    first_name = (contact_name or "").split()[0] if contact_name else ""

    # Build user details
    user_details_parts = []
    if contact_name:
        user_details_parts.append(f"Name: {contact_name}")
    if contact_email:
        user_details_parts.append(f"Email: {contact_email}")
    if contact_phone:
        user_details_parts.append(f"Phone: {contact_phone}")
    user_details = "\n".join(user_details_parts) if user_details_parts else f"First name: {first_name or '(unknown)'}"

    prompt = f"Format this message for email delivery to the lead.\n\n"
    prompt += f"**Message to Format:**\n{message}\n\n---\n\n"
    prompt += f"**Lead Details:**\n{user_details}\n\n---\n\n"
    prompt += f"**Signature Details:**\n"
    prompt += f"- Bot Name: {bot_name}\n"
    prompt += f"- Business Name: {company or '(unknown)'}\n"
    prompt += f"- Business Phone: {business_phone or '(none)'}\n\n---\n\n"
    if media_url:
        prompt += f"**Media to Include:**\n{media_url}\n\n"
    prompt += "Return the formatted HTML email."

    # Use the base prompt + media section if media_url is provided
    system = _EMAIL_FORMAT_PROMPT
    if media_url:
        system += _EMAIL_MEDIA_SECTION

    try:
        result = await classify(
            prompt=prompt,
            schema=_EMAIL_FORMAT_SCHEMA,
            system_prompt=system,
            model=model,
            temperature=temperature,
            label="email_formatting",
        )
        html = result.get("emailBody", "")
        if html:
            return html
    except Exception as e:
        logger.warning("Email formatting LLM failed, falling back to simple format: %s", e)

    # Fallback: simple HTML
    return message.replace("\n", "<br>")
