"""End-to-end test driver for the Iron Automations conversation agent.

Posts curated scenarios from scenarios.py to the live backend's
POST /testing/run endpoint, parses the response (with MockGHL ensuring
no real SMS/GHL writes), and writes a reviewable markdown report.

Usage:
    export API_AUTH_TOKEN="$(grep '^API_AUTH_TOKEN=' .env | cut -d= -f2-)"
    python -m scripts.e2e.run

    # Or a single scenario by ID:
    python -m scripts.e2e.run --only 03-price-question-early

    # Or against a local backend:
    python -m scripts.e2e.run --base-url http://localhost:8000
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import pathlib
import sys
import urllib.error
import urllib.request

# Allow `python scripts/e2e/run.py` as well as `-m`.
_HERE = pathlib.Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

from scenarios import SCENARIOS, ENTITY_ID, build_request  # noqa: E402


DEFAULT_BASE_URL = "https://rg-backend.23.88.127.9.sslip.io"
REPORT_DIR = pathlib.Path("/tmp/iron-setter-e2e")


def post_run(base_url: str, token: str, payload: dict) -> dict:
    body = json.dumps(payload).encode()
    req = urllib.request.Request(
        f"{base_url}/testing/run",
        data=body,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=300) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        print(f"HTTP {e.code}: {e.read().decode()[:500]}", file=sys.stderr)
        raise


def _last_ai_reply(conversation: list[dict]) -> str:
    """Extract Scott's final outbound text from a test's conversation log."""
    for entry in reversed(conversation or []):
        if entry.get("role") == "turn" and entry.get("ai_response"):
            return entry["ai_response"]
        if entry.get("role") == "followup" and entry.get("message"):
            return entry["message"]
    return "(no reply captured)"


def _first_turn_path(test: dict) -> str:
    for entry in test.get("conversation", []):
        if entry.get("role") == "turn":
            return entry.get("path") or entry.get("classification", {}).get("path", "")
    return ""


def _tool_calls_summary(test: dict) -> str:
    tools = []
    for entry in test.get("conversation", []):
        for tc in entry.get("tool_calls_log", []) or entry.get("tools_used", []):
            name = tc.get("tool_name") or tc.get("name") or tc
            if name:
                tools.append(str(name))
    return ", ".join(tools) if tools else "—"


def write_report(scenarios: list[dict], response: dict, report_path: pathlib.Path) -> None:
    report_path.parent.mkdir(parents=True, exist_ok=True)
    tests_by_id = {t.get("test_id"): t for t in response.get("tests", [])}

    lines: list[str] = []
    lines.append(f"# Iron Automations E2E run — {response.get('timestamp','')}\n")
    lines.append(f"Entity: **{response.get('entity_name')}**  |  Agent: `{response.get('agent_type')}`  |  Channel: `{response.get('channel')}`\n")
    lines.append(
        f"**{response.get('passed',0)}/{response.get('test_count',0)} passed**, "
        f"{response.get('duration_seconds',0)}s, "
        f"{response.get('token_usage',{}).get('total_tokens',0):,} tokens, "
        f"${response.get('token_usage',{}).get('estimated_cost_usd',0)}\n"
    )
    lines.append("---\n")

    for sc in scenarios:
        tid = sc["id"]
        t = tests_by_id.get(tid, {})
        turn_in = next(iter(sc["conversation"]), {})
        lines.append(f"## `{tid}` — {sc.get('description','')}")
        lines.append(f"**category**: `{sc.get('category','')}`  "
                     f"**status**: `{t.get('status','no-result')}`  "
                     f"**path**: `{_first_turn_path(t) or '—'}`  "
                     f"**expected**: `{turn_in.get('expect_path','—')}`  "
                     f"**tools**: `{_tool_calls_summary(t)}`\n")
        if turn_in.get("message"):
            lines.append(f"> **Lead:** {turn_in['message']}\n")
        elif turn_in.get("intent"):
            lines.append(f"> **Lead intent:** _{turn_in['intent']}_\n")
        if t.get("issues"):
            lines.append(f"⚠️ issues: {', '.join(t['issues'])}\n")
        reply = _last_ai_reply(t.get("conversation", []))
        lines.append(f"**Scott:**\n\n> {reply.replace(chr(10), chr(10)+'> ')}\n")
        lines.append("")

    report_path.write_text("\n".join(lines))
    print(f"\nReport written → {report_path}", file=sys.stderr)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--base-url", default=DEFAULT_BASE_URL)
    ap.add_argument("--only", help="Run just one scenario by id")
    ap.add_argument("--out", default=None, help="Report output path")
    args = ap.parse_args()

    token = os.environ.get("API_AUTH_TOKEN")
    if not token:
        print("ERROR: API_AUTH_TOKEN not set. Export it from .env in a non-streamed shell.", file=sys.stderr)
        return 2

    payload = build_request()
    if args.only:
        selected = [s for s in payload["test_scenarios"] if s["id"] == args.only]
        if not selected:
            print(f"No scenario with id='{args.only}'. Options: {[s['id'] for s in payload['test_scenarios']]}", file=sys.stderr)
            return 2
        payload["test_scenarios"] = selected

    print(f"Posting {len(payload['test_scenarios'])} scenarios to {args.base_url}/testing/run …", file=sys.stderr)
    response = post_run(args.base_url, token, payload)

    # Short stdout summary
    print(
        f"\n{response.get('passed',0)}/{response.get('test_count',0)} passed  "
        f"{response.get('duration_seconds',0)}s  "
        f"{response.get('token_usage',{}).get('total_tokens',0):,} toks  "
        f"${response.get('token_usage',{}).get('estimated_cost_usd',0)}",
        file=sys.stderr,
    )
    for t in response.get("tests", []):
        status = t.get("status", "?")
        flag = "✅" if status == "passed" else ("⚠️" if status == "error" else "❌")
        print(f"  {flag} {t.get('test_id','?'):<30} path={_first_turn_path(t) or '-':<16} status={status}", file=sys.stderr)

    report_path = pathlib.Path(args.out) if args.out else REPORT_DIR / f"run-{dt.datetime.utcnow().strftime('%Y%m%dT%H%M%S')}.md"
    scenarios = payload["test_scenarios"]
    write_report(scenarios, response, report_path)
    return 0 if response.get("failed", 1) == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
