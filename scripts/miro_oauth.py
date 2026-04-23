#!/usr/bin/env python3
"""Helpers for Miro REST OAuth.

Official Miro REST access tokens come from a Miro Developer app using the
OAuth 2.0 authorization code flow, not from the MCP login token.
"""

from __future__ import annotations

import argparse
import json
import os
import secrets
from pathlib import Path
from urllib.parse import urlencode

import httpx


AUTHORIZE_URL = "https://miro.com/oauth/authorize"
TOKEN_URL = "https://api.miro.com/v1/oauth/token"
TOKEN_CONTEXT_URL = "https://api.miro.com/v1/oauth-token"
ROOT = Path(__file__).resolve().parent.parent
ENV_PATH = ROOT / ".env"


def load_local_env(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if value and len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        os.environ.setdefault(key, value)


def require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise SystemExit(f"Missing required environment variable: {name}")
    return value


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Miro OAuth helper for REST API automation.")
    sub = parser.add_subparsers(dest="command", required=True)

    auth = sub.add_parser("auth-url", help="Generate the Miro OAuth authorization URL.")
    auth.add_argument("--client-id", default=os.getenv("MIRO_CLIENT_ID", ""), help="Miro app client ID.")
    auth.add_argument("--redirect-uri", default=os.getenv("MIRO_REDIRECT_URI", ""), help="Configured redirect URI.")
    auth.add_argument(
        "--scopes",
        default=os.getenv("MIRO_SCOPES", "boards:read boards:write"),
        help="Space-separated scopes.",
    )
    auth.add_argument("--state", default="", help="Optional state value. Random if omitted.")

    exchange = sub.add_parser("exchange", help="Exchange an authorization code for access tokens.")
    exchange.add_argument("--code", required=True, help="Authorization code from the redirect URL.")
    exchange.add_argument("--client-id", default=os.getenv("MIRO_CLIENT_ID", ""), help="Miro app client ID.")
    exchange.add_argument("--client-secret", default=os.getenv("MIRO_CLIENT_SECRET", ""), help="Miro app client secret.")
    exchange.add_argument("--redirect-uri", default=os.getenv("MIRO_REDIRECT_URI", ""), help="Configured redirect URI.")
    exchange.add_argument("--json-only", action="store_true", help="Print raw JSON only.")

    refresh = sub.add_parser("refresh", help="Refresh an expiring Miro REST access token.")
    refresh.add_argument("--refresh-token", default=os.getenv("MIRO_REFRESH_TOKEN", ""), help="Current Miro refresh token.")
    refresh.add_argument("--client-id", default=os.getenv("MIRO_CLIENT_ID", ""), help="Miro app client ID.")
    refresh.add_argument("--client-secret", default=os.getenv("MIRO_CLIENT_SECRET", ""), help="Miro app client secret.")
    refresh.add_argument("--json-only", action="store_true", help="Print raw JSON only.")

    info = sub.add_parser("token-info", help="Inspect the current REST access token.")
    info.add_argument("--access-token", default=os.getenv("MIRO_ACCESS_TOKEN", ""), help="Miro REST access token.")

    return parser.parse_args()


def generate_auth_url(client_id: str, redirect_uri: str, scopes: str, state: str) -> str:
    if not client_id:
        raise SystemExit("Missing client ID. Set MIRO_CLIENT_ID or pass --client-id.")
    if not redirect_uri:
        raise SystemExit("Missing redirect URI. Set MIRO_REDIRECT_URI or pass --redirect-uri.")
    actual_state = state or secrets.token_urlsafe(24)
    query = urlencode(
        {
            "response_type": "code",
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "scope": scopes,
            "state": actual_state,
        }
    )
    return f"{AUTHORIZE_URL}?{query}"


def token_request(params: dict[str, str]) -> dict:
    response = httpx.post(TOKEN_URL, params=params, timeout=30.0)
    if response.status_code not in (200, 201):
        raise SystemExit(f"Miro token request failed: {response.status_code} {response.text[:800]}")
    return response.json()


def print_token_result(payload: dict, json_only: bool) -> None:
    if json_only:
        print(json.dumps(payload, indent=2))
        return

    print(json.dumps(payload, indent=2))
    access = payload.get("access_token", "")
    refresh = payload.get("refresh_token", "")
    expires = payload.get("expires_in", "")
    print("\n# Shell exports")
    if access:
        print(f"export MIRO_ACCESS_TOKEN='{access}'")
    if refresh:
        print(f"export MIRO_REFRESH_TOKEN='{refresh}'")
    if expires:
        print(f"# expires_in={expires}")


def exchange_code(args: argparse.Namespace) -> None:
    payload = token_request(
        {
            "grant_type": "authorization_code",
            "code": args.code,
            "client_id": args.client_id or require_env("MIRO_CLIENT_ID"),
            "client_secret": args.client_secret or require_env("MIRO_CLIENT_SECRET"),
            "redirect_uri": args.redirect_uri or require_env("MIRO_REDIRECT_URI"),
        }
    )
    print_token_result(payload, args.json_only)


def refresh_token(args: argparse.Namespace) -> None:
    refresh = args.refresh_token or require_env("MIRO_REFRESH_TOKEN")
    payload = token_request(
        {
            "grant_type": "refresh_token",
            "refresh_token": refresh,
            "client_id": args.client_id or require_env("MIRO_CLIENT_ID"),
            "client_secret": args.client_secret or require_env("MIRO_CLIENT_SECRET"),
        }
    )
    print_token_result(payload, args.json_only)


def token_info(args: argparse.Namespace) -> None:
    token = args.access_token or require_env("MIRO_ACCESS_TOKEN")
    response = httpx.get(
        TOKEN_CONTEXT_URL,
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
        timeout=30.0,
    )
    if response.status_code != 200:
        raise SystemExit(f"Miro token info failed: {response.status_code} {response.text[:800]}")
    print(json.dumps(response.json(), indent=2))


def main() -> int:
    load_local_env(ENV_PATH)
    args = parse_args()
    if args.command == "auth-url":
        print(generate_auth_url(args.client_id, args.redirect_uri, args.scopes, args.state))
        return 0
    if args.command == "exchange":
        exchange_code(args)
        return 0
    if args.command == "refresh":
        refresh_token(args)
        return 0
    if args.command == "token-info":
        token_info(args)
        return 0
    raise SystemExit(f"Unknown command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
