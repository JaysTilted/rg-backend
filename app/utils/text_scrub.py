"""Final-line-of-defense text sanitizer for outbound messages.

Em-dashes and en-dashes are obvious AI tells in SMS. LLMs (especially Gemini)
reflexively insert them even when prompted not to. Apply scrub_dashes() at
EVERY outbound boundary so it is structurally impossible for either character
to leave the system, regardless of which code path generated the text.
"""
from __future__ import annotations

import re

EM_DASH = "\u2014"
EN_DASH = "\u2013"

_DOUBLE_COMMA_RE = re.compile(r",\s*,")
_SPACE_BEFORE_COMMA_RE = re.compile(r"\s+,")
_DOUBLE_SPACE_RE = re.compile(r"  +")
_COMMA_BEFORE_SENTENCE_END_RE = re.compile(r",(\s*[.?!])")


def scrub_dashes(text: str | None) -> str:
    """Replace every em-dash / en-dash with natural punctuation.

    Idempotent: safe to run multiple times.

    Replacement rules (applied in order):
      ' DASH '  -> ', '   (most common case: space-dash-space)
      'DASH '   -> ', '   (line-start dash with trailing space)
      ' DASH'   -> ','    (trailing dash with leading space)
      'DASH'    -> ', '   (no spaces — between words)

    Then collapse double commas, trim space-before-comma, collapse double
    spaces, and strip stray commas that ended up before sentence terminators.
    """
    if not text:
        return text or ""
    s = text
    if EM_DASH not in s and EN_DASH not in s:
        return s
    for dash in (EM_DASH, EN_DASH):
        s = s.replace(f" {dash} ", ", ")
        s = s.replace(f"{dash} ", ", ")
        s = s.replace(f" {dash}", ",")
        s = s.replace(dash, ", ")
    s = _DOUBLE_COMMA_RE.sub(",", s)
    s = _SPACE_BEFORE_COMMA_RE.sub(",", s)
    s = _DOUBLE_SPACE_RE.sub(" ", s)
    s = _COMMA_BEFORE_SENTENCE_END_RE.sub(r"\1", s)
    return s
