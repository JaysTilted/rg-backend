#!/usr/bin/env python3
"""Sync a rendered Mermaid asset to a Miro board via the REST API.

This script treats Mermaid as the structural source of truth and Miro as the
human-facing canvas. It uploads a rendered image asset (PNG preferred for
presentation, SVG acceptable for archival) as an image item to a target board
and records the sync metadata in `docs/mermaid-miro-sync-manifest.yaml`.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
import yaml


ROOT = Path(__file__).resolve().parent.parent
DEFAULT_MANIFEST = ROOT / "docs" / "mermaid-miro-sync-manifest.yaml"
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync a rendered Mermaid asset to Miro via REST API.")
    parser.add_argument("--asset", required=True, help="Path to the rendered image asset (.png preferred, .svg supported).")
    parser.add_argument("--slug", help="Manifest slug to read/update.")
    parser.add_argument("--manifest", default=str(DEFAULT_MANIFEST), help="Path to the sync manifest YAML.")
    parser.add_argument("--board-id", help="Target Miro board ID.")
    parser.add_argument("--board-url", help="Target Miro board URL.")
    parser.add_argument("--x", type=float, default=0.0, help="Board X coordinate for the image center.")
    parser.add_argument("--y", type=float, default=0.0, help="Board Y coordinate for the image center.")
    parser.add_argument("--width", type=float, default=1600.0, help="Image width in board units.")
    parser.add_argument(
        "--replace-existing",
        action="store_true",
        help="Delete the manifest's existing miro_item_id before creating the new image.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Resolve inputs and print the intended action without calling the Miro API or updating the manifest.",
    )
    return parser.parse_args()


def parse_board_id(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    if raw.startswith("http://") or raw.startswith("https://"):
        match = re.search(r"/app/board/([A-Za-z0-9=_-]+)", raw)
        if not match:
            raise ValueError(f"Could not parse board ID from URL: {raw}")
        return match.group(1)
    return raw


def load_manifest(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"visuals": []}
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if "visuals" not in data or not isinstance(data["visuals"], list):
        data["visuals"] = []
    return data


def save_manifest(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8")


def find_manifest_entry(data: dict[str, Any], slug: str) -> dict[str, Any] | None:
    for entry in data.get("visuals", []):
        if entry.get("slug") == slug:
            return entry
    return None


def resolve_board_id(args: argparse.Namespace, entry: dict[str, Any] | None) -> tuple[str, str]:
    board_url = args.board_url or os.getenv("MIRO_BOARD_URL", "")
    if not board_url and entry:
        board_url = entry.get("miro_board_url", "") or ""

    board_id = args.board_id or os.getenv("MIRO_BOARD_ID", "")
    if not board_id and board_url:
        board_id = parse_board_id(board_url)

    if not board_id:
        raise ValueError("Missing target board. Provide --board-id, --board-url, MIRO_BOARD_ID, or MIRO_BOARD_URL.")

    if not board_url:
        board_url = f"https://miro.com/app/board/{board_id}/"

    return board_id, board_url


def build_headers(token: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }


def delete_existing_item(client: httpx.Client, board_id: str, item_id: str) -> bool:
    response = client.delete(f"https://api.miro.com/v2/boards/{board_id}/items/{item_id}")
    if response.status_code in (200, 204):
        return True
    # Older/manual widget IDs can fail here. Keep going and create the new item.
    print(
        f"WARNING delete_failed item_id={item_id} status={response.status_code} body={response.text[:200]}",
        file=sys.stderr,
    )
    return False


def update_image_item(
    client: httpx.Client,
    board_id: str,
    item_id: str,
    asset_path: Path,
    mime_type: str,
    x: float,
    y: float,
    width: float,
) -> dict[str, Any] | None:
    data = {
        "position": json.dumps({"origin": "center", "x": x, "y": y}),
        "geometry": json.dumps({"width": width}),
    }
    with asset_path.open("rb") as fh:
        files = {"resource": (asset_path.name, fh, mime_type)}
        response = client.patch(
            f"https://api.miro.com/v2/boards/{board_id}/images/{item_id}",
            data=data,
            files=files,
        )
    if response.status_code in (200, 201):
        return response.json()
    print(
        f"WARNING update_failed item_id={item_id} status={response.status_code} body={response.text[:200]}",
        file=sys.stderr,
    )
    return None


def create_image_item(
    client: httpx.Client,
    board_id: str,
    asset_path: Path,
    mime_type: str,
    x: float,
    y: float,
    width: float,
) -> dict[str, Any]:
    data = {
        "position": json.dumps({"origin": "center", "x": x, "y": y}),
        "geometry": json.dumps({"width": width}),
    }
    with asset_path.open("rb") as fh:
        files = {"resource": (asset_path.name, fh, mime_type)}
        response = client.post(f"https://api.miro.com/v2/boards/{board_id}/images", data=data, files=files)
    if response.status_code not in (200, 201):
        raise RuntimeError(f"Failed to create image item: {response.status_code} {response.text[:400]}")
    return response.json()


def main() -> int:
    load_local_env(ENV_PATH)
    args = parse_args()
    asset_path = Path(args.asset).expanduser().resolve()
    if not asset_path.exists():
        raise SystemExit(f"Asset not found: {asset_path}")
    asset_suffix = asset_path.suffix.lower()
    mime_type = "image/png" if asset_suffix == ".png" else "image/svg+xml"

    manifest_path = Path(args.manifest).expanduser().resolve()
    manifest = load_manifest(manifest_path)
    entry = find_manifest_entry(manifest, args.slug) if args.slug else None

    board_id, board_url = resolve_board_id(args, entry)
    token = os.getenv("MIRO_ACCESS_TOKEN", "").strip()
    existing_item_id = (entry or {}).get("miro_item_id", "") if entry else ""

    if args.dry_run:
        print("DRY_RUN")
        print(f"asset={asset_path}")
        print(f"manifest={manifest_path}")
        print(f"slug={args.slug or ''}")
        print(f"board_id={board_id}")
        print(f"board_url={board_url}")
        print(f"replace_existing={args.replace_existing}")
        print(f"existing_item_id={existing_item_id}")
        print(f"width={args.width}")
        print(f"position=({args.x}, {args.y})")
        print("token_present=yes" if token else "token_present=no")
        return 0

    if not token:
        raise SystemExit("MIRO_ACCESS_TOKEN is required for a live sync.")

    with httpx.Client(headers=build_headers(token), timeout=60.0) as client:
        created = None

        # Preferred path: update the existing image item in place so the board link stays stable.
        if existing_item_id and not args.replace_existing:
            created = update_image_item(client, board_id, existing_item_id, asset_path, mime_type, args.x, args.y, args.width)

        # Explicit replace path: delete old item if possible, then create a new one.
        if args.replace_existing and existing_item_id:
            delete_existing_item(client, board_id, existing_item_id)

        if created is None:
            created = create_image_item(client, board_id, asset_path, mime_type, args.x, args.y, args.width)

    item_id = created.get("id", "")
    item_type = created.get("type", "image")
    final_board_url = f"https://miro.com/app/board/{board_id}/?focusWidget={item_id}"
    image_url = ""
    if isinstance(created.get("data"), dict):
        image_url = created["data"].get("imageUrl") or created["data"].get("url") or ""

    if args.slug:
        if not entry:
            entry = {"slug": args.slug}
            manifest.setdefault("visuals", []).append(entry)
        entry["svg_artifact"] = str(asset_path.relative_to(ROOT))
        entry["miro_item_id"] = item_id
        entry["miro_board_url"] = final_board_url
        entry["status"] = "synced"
        entry["last_synced_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        notes = (entry.get("notes") or "").strip()
        sync_note = "Synced via scripts/sync_mermaid_to_miro.py."
        entry["notes"] = f"{notes} {sync_note}".strip() if notes else sync_note
        save_manifest(manifest_path, manifest)

    print("SYNC_OK")
    print(f"board_id={board_id}")
    print(f"board_url={final_board_url}")
    print(f"item_id={item_id}")
    print(f"item_type={item_type}")
    if image_url:
        print(f"image_url={image_url}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(130)
