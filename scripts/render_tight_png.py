#!/usr/bin/env python3
"""Render a Mermaid SVG to a tightly cropped PNG for presentation surfaces."""

from __future__ import annotations

import argparse
import subprocess
import tempfile
from pathlib import Path

from PIL import Image, ImageChops


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Render and crop an SVG into a tight PNG.")
    parser.add_argument("--input", required=True, help="Input SVG path.")
    parser.add_argument("--output", required=True, help="Output PNG path.")
    parser.add_argument("--scale", type=float, default=2.0, help="Raster scale multiplier.")
    parser.add_argument("--pad", type=int, default=24, help="Padding pixels around cropped content.")
    parser.add_argument("--bg", default="white", help="Background color for rasterization.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    input_path = Path(args.input).expanduser().resolve()
    output_path = Path(args.output).expanduser().resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
        tmp_path = Path(tmp.name)

    try:
        subprocess.run(
            [
                "rsvg-convert",
                str(input_path),
                "-o",
                str(tmp_path),
                "-f",
                "png",
                "-b",
                args.bg,
                "-z",
                str(args.scale),
            ],
            check=True,
        )

        img = Image.open(tmp_path).convert("RGBA")
        # Crop based on non-white content instead of non-transparent pixels.
        bg = Image.new("RGBA", img.size, (255, 255, 255, 255))
        diff = ImageChops.difference(img, bg)
        bbox = diff.getbbox()
        if bbox is None:
            img.save(output_path)
            return 0

        left, top, right, bottom = bbox
        left = max(left - args.pad, 0)
        top = max(top - args.pad, 0)
        right = min(right + args.pad, img.width)
        bottom = min(bottom + args.pad, img.height)
        cropped = img.crop((left, top, right, bottom))
        cropped.save(output_path)
        return 0
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


if __name__ == "__main__":
    raise SystemExit(main())
