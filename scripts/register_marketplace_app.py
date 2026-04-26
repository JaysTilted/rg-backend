#!/usr/bin/env python3
"""Register Iron Setter as a GHL Marketplace Custom App.

Uses the same MarketplaceClient SDK that published iron-bridge and
iron-instantly-bridge. Automates:
  1. Upload logo + preview assets (if available)
  2. PATCH listing metadata (basicInfo, appProfiles, supportDetails, listingConfiguration)
  3. Set free pricing
  4. Optionally publish

Prerequisites:
  - Comet running on port 9222 with marketplace.gohighlevel.com open
  - Run from automation-platform repo root (for SDK import):
      cd /home/jay/automation-platform
      .venv/bin/python /home/jay/iron-setter/scripts/register_marketplace_app.py

Usage:
  # First time — create the app manually in the dev portal (no API for this),
  # then run this script with the app ID:
  python scripts/register_marketplace_app.py --app-id <APP_ID>

  # With assets:
  python scripts/register_marketplace_app.py --app-id <APP_ID> --assets /home/jay/iron-brand/iron-setter/marketplace

  # Publish (only if logo + preview images are uploaded):
  python scripts/register_marketplace_app.py --app-id <APP_ID> --publish
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# Ensure automation-platform is importable
sys.path.insert(0, "/home/jay/automation-platform")

from tools.ghl_private_sdk.marketplace import MarketplaceClient  # noqa: E402

DESCRIPTION = (
    "Iron Setter is an AI conversation agent for home-services contractors. "
    "It handles inbound SMS replies with intelligent, persona-driven responses "
    "that qualify leads, book discovery calls, and nurture prospects — all "
    "automatically. Supports custom personas, multi-service qualification, "
    "smart follow-up scheduling, and seamless human handover."
)


def upload_assets(client: MarketplaceClient, app_id: str, asset_dir: Path) -> dict[str, str]:
    """Upload all PNGs in asset_dir, return {filename: url} manifest."""
    manifest: dict[str, str] = {}
    for f in sorted(asset_dir.glob("*.png")):
        print(f"  Uploading {f.name}...", end=" ", flush=True)
        url = client.upload_marketplace_asset(f, app_id=app_id)
        manifest[f.name] = url
        print(f"OK -> {url[:80]}...")
    return manifest


def patch_listing(
    client: MarketplaceClient,
    app_id: str,
    manifest: dict[str, str],
) -> None:
    """PATCH all 4 mandatory listing sections."""
    logo_url = manifest.get("logo.png", "")
    preview_urls = [v for k, v in manifest.items() if k.startswith("preview-")]

    print("  PATCH basicInfo...", end=" ", flush=True)
    payload: dict = {
        "name": "Iron Setter",
        "tagLine": "AI conversation agent for home-services contractors",
        "allowedUserTypes": ["Location"],
    }
    if logo_url:
        payload["logoUrl"] = logo_url
    if preview_urls:
        payload["previewImageUrls"] = preview_urls
    client._request("PATCH", f"/marketplace/app/{app_id}", json=payload)
    print("OK")

    print("  PATCH appProfiles...", end=" ", flush=True)
    client._request("PATCH", f"/marketplace/app/{app_id}", json={
        "description": DESCRIPTION,
        "isPrivate": True,
        "hasSubAccountProfile": True,
    })
    print("OK")

    print("  PATCH supportDetails...", end=" ", flush=True)
    client._request("PATCH", f"/marketplace/app/{app_id}", json={
        "supportConfig": {
            "supportEmail": "support@ironautomations.xyz",
        },
    })
    print("OK")

    print("  PATCH listingConfiguration...", end=" ", flush=True)
    client._request("PATCH", f"/marketplace/app/{app_id}", json={
        "allowedUserTypes": ["Location"],
    })
    print("OK")


def set_free_pricing(client: MarketplaceClient, app_id: str) -> None:
    print("  PATCH pricing (Free)...", end=" ", flush=True)
    client._request("PATCH", f"/marketplace/app/{app_id}", json={
        "paymentPlans": [],
    })
    print("OK")


def main() -> None:
    parser = argparse.ArgumentParser(description="Register Iron Setter GHL Marketplace app")
    parser.add_argument("--app-id", required=True, help="GHL Marketplace app ID (create in dev portal first)")
    parser.add_argument("--assets", type=Path, help="Directory with logo.png + preview-*.png")
    parser.add_argument("--publish", action="store_true", help="Publish the app after patching")
    args = parser.parse_args()

    client = MarketplaceClient()

    print("\n=== Iron Setter Marketplace App ===")
    print(f"App ID: {args.app_id}")

    # Check current state
    app_data = client.get_app(args.app_id)
    print(f"  Current status: {app_data.get('status', 'unknown')}")
    print(f"  Current name: {app_data.get('name', 'unnamed')}")
    version_id = app_data.get("versionId") or app_data.get("_id", "")

    # Upload assets if provided
    manifest: dict[str, str] = {}
    if args.assets and args.assets.is_dir() and list(args.assets.glob("*.png")):
        print("\nUploading assets...")
        manifest = upload_assets(client, args.app_id, args.assets)
        Path("/tmp/iron-setter-marketplace-assets.json").write_text(
            json.dumps(manifest, indent=2)
        )
        print(f"  Manifest: /tmp/iron-setter-marketplace-assets.json")
    else:
        print("\nNo assets directory provided — patching listing without images.")

    # Patch listing
    print("\nPatching listing...")
    patch_listing(client, args.app_id, manifest)
    set_free_pricing(client, args.app_id)

    # Publish
    if args.publish and version_id:
        print("\nPublishing...")
        client.publish_version(args.app_id, version_id, "1.0.0")
        refreshed = client.get_app(args.app_id)
        print(f"  -> status = {refreshed.get('status')}")
    elif args.publish:
        print("\nERROR: could not determine version ID for publish.")

    # Print OAuth credentials hint
    print("\n=== Next Steps ===")
    print("1. In the GHL dev portal, go to the app's Auth tab")
    print("2. Copy the Client ID and Client Secret")
    print("3. Add to iron-setter .env:")
    print("   GHL_OAUTH_CLIENT_ID=<client_id>")
    print("   GHL_OAUTH_CLIENT_SECRET=<client_secret>")
    print(f"4. Set OAuth redirect URI to: https://setter.ironops.xyz/oauth/ghl/callback")
    print(f"5. Set scopes (in dev portal Auth tab):")
    print("   contacts.readonly contacts.write calendars.readonly calendars.write")
    print("   calendars/events.readonly calendars/events.write")
    print("   conversations.readonly conversations.write")
    print("   conversations/message.readonly conversations/message.write")
    print("   opportunities.readonly opportunities.write locations.readonly")
    print("   locations/tags.readonly locations/tags.write")
    print("   locations/customFields.readonly locations/customFields.write")


if __name__ == "__main__":
    main()
