"""Bearer token authentication for all API routes."""

import httpx

from fastapi import HTTPException, Request, WebSocket
from starlette.status import HTTP_401_UNAUTHORIZED

from app.config import settings

_PUBLIC_PATHS: frozenset[str] = frozenset({
    "/health", "/portal/session", "/portal/verify", "/portal/change-request",
    "/ai/data-chat/portal-message",
})


async def _is_valid_supabase_token(token: str) -> bool:
    """Accept a logged-in Supabase user JWT from the admin frontend."""
    if not token or not settings.supabase_main_url or not settings.supabase_main_key:
        return False

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(
                f"{settings.supabase_main_url}/auth/v1/user",
                headers={
                    "Authorization": f"Bearer {token}",
                    "apikey": settings.supabase_main_key,
                },
            )
        return resp.status_code == 200
    except Exception:
        return False


async def verify_auth(request: Request = None, websocket: WebSocket = None) -> None:
    """Validate Authorization: Bearer <token> header.

    Skips public paths and all WebSocket connections.
    Accepts both Request and WebSocket since FastAPI applies global
    dependencies to both route types.
    """
    # WebSocket connections bypass HTTP auth entirely
    if websocket is not None:
        return

    if request is None:
        return

    path = request.url.path
    if path in _PUBLIC_PATHS:
        return

    if not settings.api_auth_token:
        return

    auth_header = request.headers.get("Authorization", "")
    if not auth_header:
        raise HTTPException(
            status_code=HTTP_401_UNAUTHORIZED,
            detail="Missing Authorization header",
        )

    parts = auth_header.split(" ", 1)
    if len(parts) != 2 or parts[0] != "Bearer":
        raise HTTPException(
            status_code=HTTP_401_UNAUTHORIZED,
            detail="Invalid Authorization header format. Expected: Bearer <token>",
        )

    token = parts[1]

    if token == settings.api_auth_token:
        return

    if await _is_valid_supabase_token(token):
        return

    if token != settings.api_auth_token:
        raise HTTPException(
            status_code=HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication token",
        )
