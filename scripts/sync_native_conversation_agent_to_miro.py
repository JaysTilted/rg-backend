#!/usr/bin/env python3
"""Create or refresh a polished native Miro overview for the RG Backend conversation agent."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
import yaml


ROOT = Path(__file__).resolve().parent.parent
ENV_PATH = ROOT / ".env"
MANIFEST_PATH = ROOT / "docs" / "mermaid-miro-sync-manifest.yaml"
SLUG = "conversation-agent-native-overview"
BOARD_ID = "uXjVLer1WJo="  # cleaner board for the human-facing native view


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


def load_manifest() -> dict[str, Any]:
    if not MANIFEST_PATH.exists():
        return {"visuals": []}
    data = yaml.safe_load(MANIFEST_PATH.read_text(encoding="utf-8")) or {}
    if "visuals" not in data or not isinstance(data["visuals"], list):
        data["visuals"] = []
    return data


def save_manifest(data: dict[str, Any]) -> None:
    MANIFEST_PATH.write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8")


def get_entry(data: dict[str, Any]) -> dict[str, Any]:
    for entry in data["visuals"]:
        if entry.get("slug") == SLUG:
            return entry
    entry = {"slug": SLUG, "title": "Iron Setter Conversation Agent - Native Overview"}
    data["visuals"].append(entry)
    return entry


def headers() -> dict[str, str]:
    token = os.getenv("MIRO_ACCESS_TOKEN", "").strip()
    if not token:
        raise SystemExit("Missing MIRO_ACCESS_TOKEN")
    return {"Authorization": f"Bearer {token}", "Accept": "application/json", "Content-Type": "application/json"}


def delete_item(client: httpx.Client, item_id: str) -> None:
    if not item_id:
        return
    r = client.delete(f"https://api.miro.com/v2/boards/{BOARD_ID}/items/{item_id}", headers=headers(), timeout=30)
    if r.status_code not in (200, 204, 404):
        print(f"WARNING delete_failed {item_id} {r.status_code} {r.text[:160]}")


def create_frame(client: httpx.Client, title: str, x: float, y: float, width: float, height: float) -> str:
    payload = {
        "data": {"title": title},
        "position": {"x": x, "y": y, "origin": "center"},
        "geometry": {"width": width, "height": height},
    }
    r = client.post(f"https://api.miro.com/v2/boards/{BOARD_ID}/frames", headers=headers(), json=payload, timeout=30)
    r.raise_for_status()
    return r.json()["id"]


def create_shape(
    client: httpx.Client,
    frame_id: str,
    content: str,
    x: float,
    y: float,
    width: float,
    height: float,
    fill: str,
    border: str,
    color: str,
    font_size: int = 28,
    shape: str = "round_rectangle",
) -> str:
    payload = {
        "data": {"shape": shape, "content": f"<p>{content}</p>"},
        "style": {
            "fillColor": fill,
            "borderColor": border,
            "color": color,
            "fontSize": str(font_size),
            "textAlign": "center",
            "textAlignVertical": "middle",
            "borderWidth": "2",
        },
        "parent": {"id": frame_id},
        "position": {"x": x, "y": y, "origin": "center"},
        "geometry": {"width": width, "height": height},
    }
    r = client.post(f"https://api.miro.com/v2/boards/{BOARD_ID}/shapes", headers=headers(), json=payload, timeout=30)
    r.raise_for_status()
    return r.json()["id"]


def create_connector(
    client: httpx.Client,
    start_id: str,
    end_id: str,
    start_snap: str = "bottom",
    end_snap: str = "top",
    label: str | None = None,
    stroke_color: str = "#0f172a",
    stroke_width: str = "4",
    shape: str = "elbowed",
) -> str:
    payload: dict[str, Any] = {
        "startItem": {"id": start_id, "snapTo": start_snap},
        "endItem": {"id": end_id, "snapTo": end_snap},
        "shape": shape,
        "style": {"strokeColor": stroke_color, "strokeWidth": stroke_width, "strokeStyle": "normal"},
    }
    if label:
        payload["captions"] = [{"content": f"<p>{label}</p>"}]
    r = client.post(f"https://api.miro.com/v2/boards/{BOARD_ID}/connectors", headers=headers(), json=payload, timeout=30)
    r.raise_for_status()
    return r.json()["id"]


def main() -> int:
    load_local_env(ENV_PATH)
    manifest = load_manifest()
    entry = get_entry(manifest)

    old_frame_ids = entry.get("miro_frame_ids", []) or []
    if not old_frame_ids and entry.get("miro_item_id"):
        old_frame_ids = [entry["miro_item_id"]]
    old_child_ids = entry.get("miro_child_item_ids", []) or []

    with httpx.Client(timeout=30.0) as client:
        for item_id in old_child_ids:
            delete_item(client, item_id)
        for frame_id in old_frame_ids:
            delete_item(client, frame_id)

        frame_id = create_frame(
            client,
            "Iron Setter Conversation Agent — Human Overview",
            x=8200,
            y=0,
            width=2600,
            height=3400,
        )

        ids: list[str] = []

        # Board title and legend
        title = create_shape(client, frame_id, "Iron Setter Conversation Agent", 1300, 180, 1200, 120, "#ffffff", "#0f172a", "#0f172a", 42, "rectangle")
        subtitle = create_shape(client, frame_id, "human-facing overview / Mermaid remains source of truth", 1300, 300, 1400, 90, "#ffffff", "#cbd5e1", "#475569", 24, "rectangle")
        ids.extend([title, subtitle])

        # Main vertical flow
        w = create_shape(client, frame_id, "Inbound webhook", 1300, 560, 520, 120, "#dbeafe", "#1d4ed8", "#1e3a8a", 32)
        d = create_shape(client, frame_id, "Debounce manager", 1300, 800, 560, 120, "#dbeafe", "#1d4ed8", "#1e3a8a", 32)
        c = create_shape(client, frame_id, "Load contact / lead / setter config", 1300, 1060, 860, 130, "#dcfce7", "#166534", "#14532d", 30)
        s = create_shape(client, frame_id, "Sync GHL messages into chat history", 1300, 1340, 920, 130, "#dcfce7", "#166534", "#14532d", 30)
        t = create_shape(client, frame_id, "Build timeline / attachments / booking context", 1300, 1640, 980, 140, "#dcfce7", "#166534", "#14532d", 30)
        g = create_shape(client, frame_id, "Classification gate", 1300, 1960, 620, 120, "#fef3c7", "#b45309", "#78350f", 34)
        a = create_shape(client, frame_id, "Reply agent", 1300, 2360, 520, 120, "#ede9fe", "#6d28d9", "#4c1d95", 34)
        p = create_shape(client, frame_id, "Security + post-process", 1300, 2740, 820, 130, "#dcfce7", "#166534", "#14532d", 30)
        o = create_shape(client, frame_id, "Delivery + scheduling", 1300, 3080, 760, 130, "#dcfce7", "#166534", "#14532d", 30)

        # Branch outcomes from classification
        n = create_shape(client, frame_id, "No reply", 520, 2160, 360, 110, "#fef3c7", "#b45309", "#78350f", 30)
        h = create_shape(client, frame_id, "Human / opt-out", 2080, 2160, 420, 110, "#fef3c7", "#b45309", "#78350f", 30)

        # Side reference panels
        cfg = create_shape(
            client,
            frame_id,
            "Current behavior\nAlex persona\nconversational booking\n24/7 replies\n90 second debounce",
            420,
            1060,
            520,
            260,
            "#fee2e2",
            "#b91c1c",
            "#7f1d1d",
            26,
        )
        tool = create_shape(
            client,
            frame_id,
            "Core tools\nknowledge base\nbooking actions\nattachment analysis\ntransfer to human",
            2180,
            2520,
            560,
            300,
            "#ede9fe",
            "#6d28d9",
            "#4c1d95",
            26,
        )

        ids.extend([w, d, c, s, t, g, a, p, o, n, h, cfg, tool])

        # Main flow connectors
        ids.extend(
            [
                create_connector(client, w, d),
                create_connector(client, d, c),
                create_connector(client, c, s),
                create_connector(client, s, t),
                create_connector(client, t, g),
                create_connector(client, g, a),
                create_connector(client, a, p),
                create_connector(client, p, o),
            ]
        )

        # Branch connectors from the gate
        ids.extend(
            [
                create_connector(client, g, n, "left", "top"),
                create_connector(client, g, h, "right", "top"),
            ]
        )

        # Dotted reference connectors
        ids.extend(
            [
                create_connector(client, c, cfg, "left", "right", None, "#64748b", "3", "elbowed"),
                create_connector(client, a, tool, "right", "left", None, "#64748b", "3", "elbowed"),
            ]
        )

    focus_item_id = a
    entry["mermaid_source"] = "docs/visuals/conversation-agent-current.mmd"
    entry["svg_artifact"] = "docs/visuals/conversation-agent-current.direct.png"
    entry["miro_board_url"] = f"https://miro.com/app/board/{BOARD_ID}/?focusWidget={focus_item_id}"
    entry["miro_item_id"] = frame_id
    entry["miro_focus_item_id"] = focus_item_id
    entry["miro_frame_ids"] = [frame_id]
    entry["miro_frame_name"] = "Iron Setter Conversation Agent — Human Overview"
    entry["miro_child_item_ids"] = ids
    entry["status"] = "synced_native"
    entry["last_synced_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    entry["notes"] = "Native Miro single-frame human overview synced from script."
    save_manifest(manifest)

    print("SYNC_OK")
    print(f"board_id={BOARD_ID}")
    print(f"frame_id={frame_id}")
    print(f"focus_item_id={focus_item_id}")
    print(f"board_url={entry['miro_board_url']}")
    print(f"child_count={len(ids)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
