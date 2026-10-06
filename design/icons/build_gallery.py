#!/usr/bin/env python3
"""Build gallery.html: the proposed icon set (proposal/) first, then the exploration (candidates/)."""
from __future__ import annotations

import base64
import html
import re
from pathlib import Path

HERE = Path(__file__).resolve().parent
CANDIDATES = HERE / "candidates"
PROPOSAL = HERE / "proposal"
OUT = HERE / "gallery.html"
SIZES = [16, 24, 32, 48, 96]
TILES = [("dark", "#18181b", "#fff"), ("white", "#fff", "#111"), ("slate", "#334155", "#fff"), ("zinc", "#e4e4e7", "#111")]


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
    tiles = "".join(
        f'<span class="tile" style="background:{bg};color:{fg}">{cell(svg, 40)}</span>'
        for _, bg, fg in TILES
    )
    return f"""
<section class="cand" id="{html.escape(stem)}">
  <h2>{html.escape(stem)} <small>{html.escape(label)}</small></h2>
  <div class="strip light"><div class="sizes">{sizes}</div></div>
  <div class="strip dark"><div class="sizes">{sizes}</div></div>
  <div class="strip tiles">{tiles}</div>
  <details><summary>source</summary><pre>{html.escape(svg)}</pre></details>
</section>"""


LIGHT_INK, DARK_INK = "#18181b", "#e4e4e7"


def img(svg: str, size: int, ink: str, pixelated: bool = False) -> str:
    """Render an SVG as an isolated <img>, resolving currentColor and the favicon's own ink."""
    svg = re.sub(r"@media \(prefers-color-scheme:dark\)\{[^}]*\}\}", "", svg)
    svg = svg.replace("currentColor", ink).replace(LIGHT_INK, ink)
    uri = "data:image/svg+xml;base64," + base64.b64encode(svg.encode()).decode()
    style = f"width:{size}px;height:{size}px" + (";image-rendering:pixelated" if pixelated else "")
    return f'<img src="{uri}" style="{style}" alt="">'


def proposal() -> str:
    read = lambda n: (PROPOSAL / n).read_text()
    mark, mono = read("netaudio-mark.svg"), read("netaudio-mark-mono.svg")
    f16, f32, app = read("netaudio-16.svg"), read("netaudio-32.svg"), read("netaudio-app-icon.svg")

    def pair(fn) -> str:
        return (f'<div class="strip light"><div class="sizes">{fn(LIGHT_INK)}</div></div>'
                f'<div class="strip dark"><div class="sizes">{fn(DARK_INK)}</div></div>')

    def piece(title: str, note: str, inner: str) -> str:
        return f'<section class="cand"><h2>{title} <small>{note}</small></h2>{inner}</section>'

    pieces = [
        piece("netaudio-mark.svg", "primary mark, 48px and up",
              pair(lambda ink: "".join(img(mark, s, ink) for s in (48, 64, 96, 128)))),
        piece("netaudio-16.svg", "pixel-snapped favicon, shown at 1x, 2x, 4x",
              pair(lambda ink: "".join(img(f16, s, ink, s > 16) for s in (16, 32, 64)))),
        piece("netaudio-32.svg", "pixel-snapped 32px, shown at 1x, 2x, 3x",
              pair(lambda ink: "".join(img(f32, s, ink, s > 32) for s in (32, 64, 96)))),
        piece("netaudio-mark-mono.svg", "single colour, for print, terminals, embossing",
              pair(lambda ink: "".join(img(mono, s, ink) for s in (24, 48, 96)))),
        piece("netaudio-app-icon.svg", "iOS and desktop app tile",
              '<div class="strip light"><div class="sizes">'
              + "".join(img(app, s, LIGHT_INK) for s in (29, 40, 60, 120, 180)) + "</div></div>"),
    ]

    def tab(bg: str, fg: str, ink: str, active: str) -> str:
        return (f'<div class="browser" style="background:{bg};color:{fg}">'
                f'<div class="tabx" style="background:{active}">{img(f16, 16, ink)}<span>netaudio · Routing</span><span class="x">×</span></div>'
                f'<div class="tabx dim">{img(f16, 16, ink)}<span>netaudio · Devices</span></div></div>')

    context = f"""
<section class="cand ctx">
  <h2>In context</h2>
  <div class="ctxrow">
    {tab("#dee1e6", "#202124", LIGHT_INK, "#fff")}
    {tab("#202124", "#e8eaed", DARK_INK, "#35363a")}
  </div>
  <div class="ctxrow">
    <div class="home">
      <div class="app">{img(app, 60, LIGHT_INK)}<span>NetAudio</span></div>
      <div class="app"><span class="ghost"></span><span>Settings</span></div>
      <div class="app"><span class="ghost"></span><span>Music</span></div>
      <div class="app"><span class="ghost"></span><span>Files</span></div>
    </div>
    <div class="readme">
      <div class="readme-h">{img(mark, 40, LIGHT_INK)}<span>netaudio</span></div>
      <p>Unofficial Dante Controller alternative for discovering, routing, configuring and monitoring Dante devices.</p>
    </div>
    <div class="readme dark">
      <div class="readme-h">{img(mark, 40, DARK_INK)}<span>netaudio</span></div>
      <p>Unofficial Dante Controller alternative for discovering, routing, configuring and monitoring Dante devices.</p>
    </div>
  </div>
</section>"""

    return f"""
<h1>Proposal</h1>
<p class="lede">A Dante routing matrix: transmitter labels across the top, receivers down the side, and three
green crosspoints routed one-to-one. Square cells and hairline borders keep it a table rather than a button
grid; the diagonal reads as signal flowing through. The small sizes are hand-snapped to the pixel grid instead
of scaled, and drop the checkmarks where they would turn to mush.</p>
<div class="grid">{''.join(pieces)}</div>
{context}
<h1 class="explore">Exploration</h1>
"""


def main() -> None:
    items = load()
    body = "".join(row(*it) for it in items)
    OUT.write_text(f"""<!doctype html>
<meta charset="utf-8">
<title>netaudio icons</title>
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
  .strip.tiles {{ background: #f4f4f5; border-top: 1px solid #ddd; }}
  @media (prefers-color-scheme: dark) {{ .strip.tiles {{ background: #1f1f23; border-color: #333; }} }}
  details {{ padding: 8px 16px 12px; font-size: 12px; opacity: .8; }}
  pre {{ white-space: pre-wrap; margin: 6px 0 0; }}
  h1 {{ margin: 8px 0 8px; }}
  h1.explore {{ margin-top: 48px; }}
  .lede {{ max-width: 760px; margin: 0 0 20px; opacity: .85; }}
  .strip .sizes img {{ display: block; }}
  .ctx {{ margin-top: 20px; }}
  .ctxrow {{ display: flex; flex-wrap: wrap; gap: 16px; padding: 16px; }}
  .browser {{ display: flex; gap: 2px; padding: 8px 8px 0; border-radius: 8px 8px 0 0; flex: 1; min-width: 320px; }}
  .tabx {{ display: flex; align-items: center; gap: 8px; padding: 8px 12px; border-radius: 8px 8px 0 0; font-size: 12px; min-width: 150px; }}
  .tabx.dim {{ opacity: .75; }}
  .tabx .x {{ margin-left: auto; opacity: .6; }}
  .home {{ display: flex; gap: 18px; padding: 22px; border-radius: 16px; background: linear-gradient(135deg, #3b4a6b, #8a5a7a); }}
  .app {{ display: flex; flex-direction: column; align-items: center; gap: 6px; color: #fff; font-size: 11px; }}
  .ghost {{ width: 60px; height: 60px; border-radius: 13px; background: rgba(255,255,255,.25); }}
  .readme {{ flex: 1; min-width: 260px; padding: 16px 20px; border-radius: 8px; background: #fff; color: #1f2328; border: 1px solid #d0d7de; }}
  .readme.dark {{ background: #0d1117; color: #e6edf3; border-color: #30363d; }}
  .readme-h {{ display: flex; align-items: center; gap: 12px; font-size: 26px; font-weight: 600; padding-bottom: 8px; border-bottom: 1px solid currentColor; border-color: rgba(127,127,127,.3); }}
  .readme p {{ font-size: 13px; margin: 10px 0 0; }}
</style>
{proposal()}
<header>
  <p>Earlier rounds. {len(items)} candidates, each at {", ".join(f"{s}px" for s in SIZES)} on light and dark, plus app-tile variants on neutral backgrounds.
  All use <code>currentColor</code> so they take the surrounding text colour.</p>
</header>
<div class="grid">{body}</div>
""")
    print(f"wrote {OUT} with {len(items)} candidates")


if __name__ == "__main__":
    main()
