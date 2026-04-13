#!/usr/bin/env python3
"""Operator-grade validation for the backend-only one-project setup.

This script is intentionally stdlib-only so it can run before dependencies are
installed. It validates the repo handoff assets, the local `.env`, and optional
online/runtime checks for Supabase and Docker Compose.
"""

from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parent.parent
ENV_PATH = ROOT / ".env"


REQUIRED_FILES = (
    ROOT / "CLAUDE_CODE_MASTER_BACKEND_HANDOFF.md",
    ROOT / "BACKEND_ONLY_ONE_PROJECT_BOOTSTRAP.sql",
    ROOT / "STARTER_SYSTEM_CONFIG_TEMPLATE.json",
)

REQUIRED_ENV_KEYS = (
    "SUPABASE_MAIN_URL",
    "SUPABASE_MAIN_KEY",
    "SUPABASE_MAIN_SCHEMA",
    "SUPABASE_CHAT_URL",
    "SUPABASE_CHAT_KEY",
    "SUPABASE_CHAT_SCHEMA",
    "DATABASE_URL",
    "DATABASE_CHAT_URL",
    "API_AUTH_TOKEN",
    "PREFECT_API_URL",
    "AZURE_OPENAI_API_KEY",
)

OPTIONAL_ENV_KEYS = (
    "AZURE_OPENAI_BASE_URL",
    "AZURE_OPENAI_MODEL",
    "OPENROUTER_TESTING_KEY",
    "OPENROUTER_API_KEY",
    "OPENAI_API_KEY",
    "GOOGLE_GEMINI_API_KEY",
    "ANTHROPIC_API_KEY",
    "DEEPSEEK_API_KEY",
    "XAI_API_KEY",
    "SLACK_BOT_TOKEN",
    "GIPHY_API_KEY",
    "SIGNAL_HOUSE_API_KEY",
    "SIGNAL_HOUSE_AUTH_TOKEN",
    "GHL_SNAPSHOT_API_KEY",
    "GHL_SNAPSHOT_LOCATION_ID",
    "RESEND_API_KEY",
    "RESEND_FROM_EMAIL",
    "FRONTEND_URL",
    "PORTAL_JWT_SECRET",
)

PLACEHOLDER_SNIPPETS = (
    "change-me",
    "your-project",
    "your_project_ref",
    "password@aws-1-your-region",
    "YOUR_SERVICE_ROLE_KEY",
)


@dataclass
class Issue:
    level: str
    message: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate rg-backend setup.")
    parser.add_argument(
        "--online",
        action="store_true",
        help="Probe Supabase REST and PostgreSQL URLs using the values in .env.",
    )
    parser.add_argument(
        "--docker",
        action="store_true",
        help="Run `docker compose config` to validate the local compose stack.",
    )
    return parser.parse_args()


def parse_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if value and len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        values[key] = value
    return values


def check_required_files() -> list[Issue]:
    issues: list[Issue] = []
    for path in REQUIRED_FILES:
        if not path.exists():
            issues.append(Issue("error", f"Missing required handoff file: {path.name}"))

    starter = ROOT / "STARTER_SYSTEM_CONFIG_TEMPLATE.json"
    if starter.exists():
        try:
            json.loads(starter.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            issues.append(Issue("error", f"Starter system config JSON is invalid: {exc}"))

    return issues


def check_env(values: dict[str, str]) -> list[Issue]:
    issues: list[Issue] = []
    if not ENV_PATH.exists():
        issues.append(Issue("error", "Missing .env file. Copy .env.example to .env first."))
        return issues

    for key in REQUIRED_ENV_KEYS:
        value = values.get(key, "")
        if not value:
            issues.append(Issue("error", f"Required env var missing or empty: {key}"))
        elif any(snippet in value for snippet in PLACEHOLDER_SNIPPETS):
            issues.append(Issue("error", f"Required env var still uses a placeholder value: {key}"))

    for key in ("SUPABASE_MAIN_URL", "SUPABASE_CHAT_URL", "FRONTEND_URL", "PREFECT_API_URL"):
        value = values.get(key, "")
        if value:
            parsed = urlparse(value)
            if parsed.scheme not in {"http", "https"} or not parsed.netloc:
                issues.append(Issue("error", f"Invalid URL for {key}: {value}"))

    for key in ("DATABASE_URL", "DATABASE_CHAT_URL"):
        value = values.get(key, "")
        if value:
            parsed = urlparse(value)
            if not parsed.scheme.startswith("postgres"):
                issues.append(Issue("error", f"Invalid Postgres URL for {key}: {value}"))

    one_project_pairs = (
        ("SUPABASE_MAIN_URL", "SUPABASE_CHAT_URL"),
        ("SUPABASE_MAIN_KEY", "SUPABASE_CHAT_KEY"),
        ("DATABASE_URL", "DATABASE_CHAT_URL"),
    )
    for left, right in one_project_pairs:
        left_value = values.get(left, "")
        right_value = values.get(right, "")
        if left_value and right_value and left_value != right_value:
            issues.append(
                Issue(
                    "warning",
                    f"One-project handoff expects {left} and {right} to match exactly.",
                )
            )

    for key in ("SUPABASE_MAIN_SCHEMA", "SUPABASE_CHAT_SCHEMA"):
        value = values.get(key, "")
        if value and not value.replace("_", "").isalnum():
            issues.append(Issue("error", f"Schema name contains unsupported characters: {key}={value}"))

    main_schema = values.get("SUPABASE_MAIN_SCHEMA", "")
    chat_schema = values.get("SUPABASE_CHAT_SCHEMA", "")
    if main_schema and chat_schema and main_schema == chat_schema:
        issues.append(
            Issue(
                "info",
                f"MAIN and CHAT are sharing the same schema ({main_schema}). This is valid, but separate schemas give cleaner isolation.",
            )
        )
    elif main_schema and chat_schema:
        issues.append(
            Issue(
                "info",
                f"Shared Supabase mode enabled with separate schemas: main={main_schema}, chat={chat_schema}",
            )
        )

    if values.get("SIGNAL_HOUSE_API_KEY") and not values.get("SIGNAL_HOUSE_AUTH_TOKEN"):
        issues.append(Issue("warning", "Signal House API key is set but SIGNAL_HOUSE_AUTH_TOKEN is empty."))
    if values.get("SIGNAL_HOUSE_AUTH_TOKEN") and not values.get("SIGNAL_HOUSE_API_KEY"):
        issues.append(Issue("warning", "Signal House auth token is set but SIGNAL_HOUSE_API_KEY is empty."))

    missing_optional = [key for key in OPTIONAL_ENV_KEYS if not values.get(key)]
    if missing_optional:
        issues.append(
            Issue(
                "info",
                "Optional env vars still empty: " + ", ".join(sorted(missing_optional)),
            )
        )

    return issues


def check_online(values: dict[str, str]) -> list[Issue]:
    issues: list[Issue] = []
    main_url = values.get("SUPABASE_MAIN_URL", "")
    main_key = values.get("SUPABASE_MAIN_KEY", "")
    main_schema = values.get("SUPABASE_MAIN_SCHEMA", "")
    db_url = values.get("DATABASE_URL", "")

    if not main_url or not main_key or not main_schema or not db_url:
        issues.append(Issue("warning", "Skipping online checks because required env vars are missing."))
        return issues

    try:
        result = subprocess.run(
            [
                "curl",
                "-sS",
                "-o",
                "/tmp/rg_backend_doctor_rest.out",
                "-w",
                "%{http_code}",
                "-H",
                f"apikey: {main_key}",
                "-H",
                f"Authorization: Bearer {main_key}",
                "-H",
                f"Accept-Profile: {main_schema}",
                "-H",
                f"Content-Profile: {main_schema}",
                main_url.rstrip("/") + "/rest/v1/tenants?select=id&limit=1",
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            issues.append(Issue("error", f"Supabase REST probe failed: {result.stderr.strip() or result.stdout.strip()}"))
        else:
            status_code = int((result.stdout or "0").strip() or "0")
            if status_code != 200:
                body = Path("/tmp/rg_backend_doctor_rest.out").read_text(encoding="utf-8", errors="ignore")[:300]
                issues.append(Issue("error", f"Supabase REST probe failed with HTTP {status_code}: {body}"))
    except Exception as exc:
        issues.append(Issue("error", f"Supabase REST probe failed: {exc}"))

    parsed = urlparse(db_url)
    if not parsed.hostname or not parsed.path:
        issues.append(Issue("error", "DATABASE_URL is not parseable for online checks."))
    else:
        issues.append(Issue("info", f"Postgres host parsed as {parsed.hostname}:{parsed.port or '(default)'}"))

    return issues


def check_docker() -> list[Issue]:
    issues: list[Issue] = []
    try:
        result = subprocess.run(
            ["docker", "compose", "config"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError:
        issues.append(Issue("warning", "Docker is not installed or not on PATH; skipped compose validation."))
        return issues

    if result.returncode != 0:
        stderr = result.stderr.strip() or result.stdout.strip()
        issues.append(Issue("error", f"`docker compose config` failed: {stderr}"))
    else:
        issues.append(Issue("info", "Docker Compose configuration parses cleanly."))
    return issues


def print_issues(issues: Iterable[Issue]) -> int:
    exit_code = 0
    for issue in issues:
        prefix = {
            "error": "ERROR",
            "warning": "WARN ",
            "info": "INFO ",
        }.get(issue.level, issue.level.upper())
        print(f"[{prefix}] {issue.message}")
        if issue.level == "error":
            exit_code = 1
    return exit_code


def main() -> int:
    args = parse_args()
    env_values = parse_env_file(ENV_PATH)

    issues: list[Issue] = []
    issues.extend(check_required_files())
    issues.extend(check_env(env_values))
    if args.online:
        issues.extend(check_online(env_values))
    if args.docker:
        issues.extend(check_docker())

    print(f"Repo root: {ROOT}")
    print(f"Env file:  {ENV_PATH}")
    print()
    return print_issues(issues)


if __name__ == "__main__":
    raise SystemExit(main())
