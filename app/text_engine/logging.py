"""Structured JSON logging with async-safe request context.

Usage:
    # At app startup (main.py):
    from app.text_engine.logging import setup_logging
    setup_logging()

    # At webhook entry point:
    from app.text_engine.logging import set_request_context, clear_request_context
    set_request_context(entity_id="abc", trigger_type="reply", entity_slug="dr-k")
    ...
    clear_request_context()

    # In any module — context fields are automatically included:
    import logging
    logger = logging.getLogger(__name__)
    logger.info("Processing message")
    # Output: {"timestamp": "...", "level": "INFO", "logger": "app.text_engine.reply",
    #          "message": "Processing message", "entity_id": "abc",
    #          "trigger_type": "reply", "entity_slug": "dr-k"}
"""

from __future__ import annotations

import json
import logging
from contextvars import ContextVar
from datetime import datetime, timezone
from typing import Any

# Async-safe context vars — set once per request, readable from any async task
_entity_id: ContextVar[str] = ContextVar("entity_id", default="")
_entity_slug: ContextVar[str] = ContextVar("entity_slug", default="")
_trigger_type: ContextVar[str] = ContextVar("trigger_type", default="")
_contact_id: ContextVar[str] = ContextVar("contact_id", default="")


def set_request_context(
    *,
    entity_id: str = "",
    entity_slug: str = "",
    trigger_type: str = "",
    contact_id: str = "",
) -> None:
    """Set request-scoped context. Call at webhook entry point."""
    if entity_id:
        _entity_id.set(entity_id)
    if entity_slug:
        _entity_slug.set(entity_slug)
    if trigger_type:
        _trigger_type.set(trigger_type)
    if contact_id:
        _contact_id.set(contact_id)


def clear_request_context() -> None:
    """Clear request context. Call after request completes."""
    _entity_id.set("")
    _entity_slug.set("")
    _trigger_type.set("")
    _contact_id.set("")


def get_request_context() -> dict[str, str]:
    """Get current request context as a dict."""
    ctx = {}
    if v := _entity_id.get():
        ctx["entity_id"] = v
    if v := _entity_slug.get():
        ctx["entity_slug"] = v
    if v := _trigger_type.get():
        ctx["trigger_type"] = v
    if v := _contact_id.get():
        ctx["contact_id"] = v
    return ctx


class JSONFormatter(logging.Formatter):
    """JSON log formatter that includes request context from contextvars."""

    def format(self, record: logging.LogRecord) -> str:
        log_entry: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Add request context from contextvars
        log_entry.update(get_request_context())

        # Add exception info if present
        if record.exc_info and record.exc_info[0] is not None:
            log_entry["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_entry, default=str)


def setup_logging(level: int = logging.INFO) -> None:
    """Configure structured JSON logging for the entire app.

    Call once at app startup before any logging happens.
    """
    root = logging.getLogger()
    root.setLevel(level)

    # Remove existing handlers (uvicorn adds its own)
    root.handlers.clear()

    handler = logging.StreamHandler()
    handler.setFormatter(JSONFormatter())
    root.addHandler(handler)

    # Quiet down noisy libraries
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("asyncpg").setLevel(logging.WARNING)
    logging.getLogger("prefect").setLevel(logging.WARNING)
