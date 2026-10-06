#!/usr/bin/env python3
"""Inline every SVG in candidates/ into gallery.html for side-by-side comparison."""
from __future__ import annotations

import html
import re
from pathlib import Path

HERE = Path(__file__).resolve().parent
CANDIDATES = HERE / "candidates"
OUT = HERE / "gallery.html"
SIZES = [16, 24, 32, 48, 96]
TILE = "#ff2323"  # matches the current webapp favicon background


def load() -> list[tuple[str, str, str]]:
    items = []
    for path in sorted(CANDIDATES.glob("*.svg")):
        svg = path.read_text()
        label = re.search(r'aria-label="([^"]+)"', svg)
        items.append((path.stem, label.group(1) if label else path.stem, svg))
    return items


def cell(svg: str, size: int) -> str:
    return f'<span class="ic" style="width:{size}px;height:{size}px">{svg}</span>'


def row(stem: str, label: str, svg: str) -> str:
    sizes = "".join(cell(svg, s) for s in SIZES)
    return f"""
<section class="cand" id="{html.escape(stem)}">
  <h2>{html.escape(stem)} <small>{html.escape(label)}</small></h2>
  <div class="strip light"><div class="sizes">{sizes}</div><span class="tile light" style="color:#111">{cell(svg, 40)}</span></div>
  <div class="strip dark"><div class="sizes">{sizes}</div><span class="tile red" style="color:#000">{cell(svg, 40)}</span><span class="tile red" style="color:#fff">{cell(svg, 40)}</span></div>
  <details><summary>source</summary><pre>{html.escape(svg)}</pre></details>
</section>"""


def main() -> None:
    items = load()
    body = "".join(row(*it) for it in items)
    OUT.write_text(f"""<!doctype html>
<meta charset="utf-8">
<title>netaudio icon candidates</title>
<style>
  :root {{ color-scheme: light dark; font: 14px/1.4 system-ui, sans-serif; }}
  body {{ margin: 0; padding: 24px; background: #f4f4f5; color: #111; }}
  @media (prefers-color-scheme: dark) {{ body {{ background: #18181b; color: #eee; }} }}
  header {{ margin-bottom: 24px; }}
  .grid {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(560px, 1fr)); gap: 20px; }}
  .cand {{ background: #fff; color: #111; border-radius: 12px; overflow: hidden; box-shadow: 0 1px 3px rgba(0,0,0,.15); }}
  @media (prefers-color-scheme: dark) {{ .cand {{ background: #27272a; color: #eee; }} }}
  h2 {{ margin: 0; padding: 12px 16px; font-size: 15px; }}
  h2 small {{ font-weight: normal; opacity: .6; margin-left: 8px; }}
  .strip {{ display: flex; align-items: center; gap: 16px; padding: 14px 16px; }}
  .strip.light {{ background: #fff; color: #111; }}
  .strip.dark {{ background: #111; color: #f4f4f5; }}
  .sizes {{ display: flex; align-items: flex-end; gap: 18px; flex: 1; }}
  .ic {{ display: inline-block; vertical-align: bottom; }}
  .ic svg {{ width: 100%; height: 100%; display: block; }}
  .tile {{ display: inline-flex; align-items: center; justify-content: center; width: 56px; height: 56px; border-radius: 12px; }}
  .tile.light {{ background: #e4e4e7; }}
  .tile.red {{ background: {TILE}; }}
  details {{ padding: 8px 16px 12px; font-size: 12px; opacity: .8; }}
  pre {{ white-space: pre-wrap; margin: 6px 0 0; }}
</style>
<header>
  <h1>netaudio icon candidates</h1>
  <p>{len(items)} candidates, each at {", ".join(f"{s}px" for s in SIZES)} on light and dark, plus app-tile variants on the current favicon red.
  All use <code>currentColor</code> so they take the surrounding text colour.</p>
</header>
<div class="grid">{body}</div>
""")
    print(f"wrote {OUT} with {len(items)} candidates")


if __name__ == "__main__":
    main()
