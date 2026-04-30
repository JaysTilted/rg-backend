"""Regression: single-calendar setter must resolve calendar_id deterministically.

Bug: live system_config (and the iron-setup portal) writes calendar entries
with the field name `calendar_id`, but several readers used `.get("id", "")`.
For a single-calendar entity:
  - the LLM-facing calendar list (booking_compiler.compile_booking_config)
    serialized `id: ""`, leaving the LLM with no ID to pass.
  - the deterministic _build_tool_kwargs fallback in agent.py wrote
    calendar_id="", and the "force calendar_id" override then clobbered any
    ID the LLM tried to pass anyway.
Net effect: get_available_slots hit `/calendars//free-slots/` and 404'd,
so the agent never offered specific times.
"""

from __future__ import annotations

import json
import sys
import types

# Stub prefect for local test runs (iron-setter Docker image has the real module).
# _build_tool_kwargs doesn't actually use @task at runtime, so a no-op decorator
# is sufficient.
if "prefect" not in sys.modules:
    _stub = types.ModuleType("prefect")

    def _task(*args, **kwargs):  # type: ignore[no-redef]
        if args and callable(args[0]) and not kwargs:
            return args[0]

        def _wrap(fn):
            return fn

        return _wrap

    _stub.task = _task  # type: ignore[attr-defined]
    sys.modules["prefect"] = _stub

from app.models import PipelineContext  # noqa: E402
from app.text_engine.agent import _build_tool_kwargs  # noqa: E402
from app.text_engine.booking_compiler import compile_booking_config  # noqa: E402


_PORTAL_CALENDAR = {
    "name": "Discovery Call",
    "is_default": True,
    "calendar_id": "guMoK9aY3lVSkF6Ilo9z",
}

_LEGACY_CALENDAR = {
    "name": "Legacy",
    "id": "legacyCalId123",
}


def _ctx_with_setter(setter: dict) -> PipelineContext:
    ctx = PipelineContext()
    ctx.compiled = {"_matched_setter": setter}
    return ctx


def test_build_tool_kwargs_resolves_portal_schema():
    """Portal writes `calendar_id`. Fallback must pick that up."""
    setter = {"booking": {"calendars": [_PORTAL_CALENDAR]}}
    kwargs = _build_tool_kwargs(_ctx_with_setter(setter))
    assert kwargs.get("calendar_id") == "guMoK9aY3lVSkF6Ilo9z"


def test_build_tool_kwargs_resolves_legacy_id_schema():
    """Defensive: legacy entries that used `id` still resolve."""
    setter = {"booking": {"calendars": [_LEGACY_CALENDAR]}}
    kwargs = _build_tool_kwargs(_ctx_with_setter(setter))
    assert kwargs.get("calendar_id") == "legacyCalId123"


def test_build_tool_kwargs_skips_when_no_id():
    """Empty calendar entry should NOT inject an empty calendar_id (the
    'force calendar_id' override at agent.py would then clobber an LLM-supplied
    value with an empty string)."""
    setter = {"booking": {"calendars": [{"name": "Broken", "is_default": True}]}}
    kwargs = _build_tool_kwargs(_ctx_with_setter(setter))
    assert "calendar_id" not in kwargs


def test_build_tool_kwargs_multi_calendar_no_fallback():
    """Multi-calendar entities must let the LLM pick — no fallback injected."""
    setter = {"booking": {"calendars": [_PORTAL_CALENDAR, _LEGACY_CALENDAR]}}
    kwargs = _build_tool_kwargs(_ctx_with_setter(setter))
    assert "calendar_id" not in kwargs


def test_compile_booking_config_serializes_calendar_id_for_llm():
    """The calendar list shown to the LLM must carry the actual ID, keyed
    `calendar_id` so the param name matches the get_available_slots tool def."""
    booking = {"calendars": [_PORTAL_CALENDAR], "booking_window_days": 10}
    text = compile_booking_config(booking)
    # Extract the JSON block after the heading
    block_start = text.find("[")
    block_end = text.rfind("]") + 1
    cal_data = json.loads(text[block_start:block_end])
    assert cal_data[0]["calendar_id"] == "guMoK9aY3lVSkF6Ilo9z"
    assert cal_data[0]["name"] == "Discovery Call"


def test_compile_booking_config_legacy_id_falls_back():
    booking = {"calendars": [_LEGACY_CALENDAR], "booking_window_days": 10}
    text = compile_booking_config(booking)
    block_start = text.find("[")
    block_end = text.rfind("]") + 1
    cal_data = json.loads(text[block_start:block_end])
    assert cal_data[0]["calendar_id"] == "legacyCalId123"
