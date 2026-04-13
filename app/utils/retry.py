"""Shared async retry utility with exponential backoff.

Usage:
    from app.utils.retry import with_retries, is_transient_http

    result = await with_retries(
        some_async_func,
        args=(arg1, arg2),
        max_attempts=3,
        retryable=is_transient_http,
        label="webhook_post",
    )
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Awaitable, Callable

logger = logging.getLogger(__name__)


async def with_retries(
    fn: Callable[..., Awaitable[Any]],
    *,
    args: tuple = (),
    kwargs: dict | None = None,
    max_attempts: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
    retryable: Callable[[Exception], bool] | None = None,
    label: str = "",
) -> Any:
    """Call an async function with exponential backoff retry on transient errors.

    Args:
        fn: Async function to call.
        args/kwargs: Arguments to pass through.
        max_attempts: Total attempts (1 = no retry).
        base_delay: Initial delay in seconds (doubles each attempt).
        max_delay: Cap on delay between attempts.
        retryable: Predicate — returns True if the exception is worth retrying.
                   Defaults to retrying all exceptions.
        label: Human-readable label for log messages.
    """
    kwargs = kwargs or {}
    last_error: Exception | None = None

    for attempt in range(1, max_attempts + 1):
        try:
            return await fn(*args, **kwargs)
        except Exception as e:
            last_error = e
            if retryable and not retryable(e):
                raise  # Not a retryable error — fail immediately

            if attempt < max_attempts:
                delay = min(base_delay * (2 ** (attempt - 1)), max_delay)
                logger.warning(
                    "RETRY [%s] attempt %d/%d failed: %s — retrying in %.1fs",
                    label or fn.__name__, attempt, max_attempts, e, delay,
                )
                await asyncio.sleep(delay)
            else:
                logger.error(
                    "RETRY [%s] all %d attempts exhausted: %s",
                    label or fn.__name__, max_attempts, e,
                )

    raise last_error  # type: ignore[misc]


def is_transient_http(exc: Exception) -> bool:
    """Predicate: True for transient HTTP errors worth retrying."""
    import httpx

    if isinstance(exc, (httpx.TimeoutException, httpx.ConnectError)):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code in {429, 500, 502, 503, 504}
    if isinstance(exc, (ConnectionError, OSError)):
        return True
    return False


def is_transient_db(exc: Exception) -> bool:
    """Predicate: True for transient Postgres errors worth retrying."""
    import asyncpg

    if isinstance(exc, (asyncpg.InterfaceError, asyncpg.ConnectionDoesNotExistError)):
        return True
    if isinstance(exc, (ConnectionError, OSError)):
        return True
    # Connection pool exhaustion
    if "pool" in str(exc).lower():
        return True
    return False
