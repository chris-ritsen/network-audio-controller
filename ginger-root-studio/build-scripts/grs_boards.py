# -*- coding: utf-8 -*-
"""Concept boards: every print becomes a tiled 'repeat concept' swatch built from the motif library.

A swatch is a periodic lattice (straight / brick / half-drop) of one drawn cell, clipped to the
swatch rectangle, so the drawing is seamless by construction. Blenders and textures are drawn
directly as periodic line families. Boards: 5x2 grid (ten prints) or 4x3 (O Holy Night) above a
cream band of isolated motif studies.
"""
import math
import random
from reportlab.lib.colors import Color
import grs_motifs as M
from grs_data import HEX, BY_CODE


def C(name):
    return M.hexc(HEX[name])


CREAM = C("Warm Cream")
COCOA = C("Soft Cocoa")

# ------------------------------------------------------------------ colour schemes (role -> palette name)
BASE = dict(petal="Dusty Blush", petal2="Peach", center="Warm Gold", leaf="Jade", leaf2="Sage", berry="Warm Berry",
            bird="Blue Willow", bird2="Warm Taupe", wing="Peach", wing2="Buttercream", object="Warm Gold",
            object2="Old Parchment", cream="Warm Cream", water="Blue Willow", mountain="Sage", mountain2="Blue Willow",
            accent="Warm Coral", ground="Warm Cream", line="Soft Cocoa")

SCHEMES = {
    "HG": dict(petal="Warm Coral", petal2="Peach", center="Ochre", leaf="Moss", leaf2="Deep Olive", berry="Terracotta Rose",
               object="Ochre", object2="Peach", accent="Terracotta Rose", wing="Peach", wing2="Warm Cream", bird2="Deep Olive"),
    "CG": dict(petal="Warm Berry", petal2="Terracotta Rose", center="Warm Gold", leaf="Deep Garden Green", leaf2="Jade",
               berry="Warm Berry", object="Warm Gold", object2="Old Parchment", accent="Warm Berry", cream="Warm Cream"),
    "WG": dict(petal="Dusty Blush", petal2="Soft Oat", center="Soft Oat", leaf="Jade", leaf2="Sage", berry="Blue Willow",
               object="Soft Oat", object2="Sage", accent="Dusty Blush", bird="Blue Willow", water="Blue Willow"),
    "SG": dict(petal="Peach", petal2="Dusty Blush", center="Warm Coral", leaf="Jade", leaf2="Sage", berry="Warm Coral",
               bird="Blue Willow", bird2="Dusty Blush", wing="Peach", wing2="Buttercream", object="Buttercream", accent="Warm Coral"),
    "GP": dict(petal="Warm Coral", petal2="Peach", center="Warm Gold", leaf="Jade", leaf2="Sage", berry="Warm Coral",
               bird="Blue Willow", bird2="Sage", wing="Peach", wing2="Buttercream", object="Warm Gold", accent="Warm Coral"),
    "JG": dict(petal="Dusty Blush", petal2="Old Parchment", center="Warm Gold", leaf="Jade", leaf2="Moss", berry="Warm Gold",
               bird="Blue Willow", bird2="Moss", wing="Dusty Blush", wing2="Old Parchment", object="Warm Gold", object2="Old Parchment",
               accent="Dusty Blush", line="Soft Charcoal"),
    "OH": dict(petal="Warm Cream", petal2="Old Parchment", center="Warm Gold", leaf="Deep Garden Green", leaf2="Deep Olive",
               berry="Warm Berry", object="Warm Gold", object2="Old Parchment", cream="Warm Cream", wing="Old Parchment",
               wing2="Warm Cream", water="Blue Willow", accent="Warm Gold", bird="Blue Willow"),
    "CF": dict(petal="Peach", petal2="Buttercream", center="Warm Gold", leaf="Jade", leaf2="Sage", berry="Warm Coral",
               bird="Blue Willow", bird2="Sage", wing="Peach", wing2="Buttercream", water="Blue Willow", object="Warm Gold",
               object2="Buttercream", accent="Warm Coral"),
    "SP": dict(petal="Peach", petal2="Buttercream", center="Warm Coral", leaf="Jade", leaf2="Sage", berry="Warm Coral",
               bird="Blue Willow", bird2="Buttercream", wing="Blue Willow", wing2="Buttercream", object="Warm Gold", accent="Warm Coral"),
    "BV": dict(petal="Terracotta Rose", petal2="Warm Gold", center="Warm Gold", leaf="Jade", leaf2="Deep Garden Green",
               berry="Terracotta Rose", object="Warm Gold", object2="Old Parchment", accent="Warm Gold", ground="Old Parchment",
               cream="Old Parchment"),
    "GA": dict(petal="Peach", petal2="Terracotta Rose", center="Warm Gold", leaf="Moss", leaf2="Jade", berry="Terracotta Rose",
               bird="Blue Willow", bird2="Moss", mountain="Blue Willow", mountain2="Jade", object="Warm Gold", accent="Terracotta Rose"),
    "IW": dict(petal="Dusty Blush", petal2="Warm Cream", center="Warm Gold", leaf="Jade", leaf2="Sage", water="Blue Willow",
               bird="Blue Willow", bird2="Sage", object="Warm Gold", object2="Sage", accent="Dusty Blush", wing="Blue Willow", wing2="Sage"),
}
# dark / coloured ground options: ground, line colour, and role overrides for legibility
DARK = {
    "HG": ("Deep Olive", "Warm Cream", dict(leaf="Sage", leaf2="Moss", petal="Peach", petal2="Warm Coral", object="Buttercream", berry="Terracotta Rose")),
    "CG": ("Deep Garden Green", "Old Parchment", dict(leaf="Jade", leaf2="Sage", petal="Warm Cream", petal2="Old Parchment", object="Buttercream", object2="Soft Oat", berry="Warm Berry", center="Warm Gold")),
    "WG": ("Blue Willow", "Warm Cream", dict(leaf="Sage", leaf2="Warm Cream", petal="Warm Cream", petal2="Soft Oat", object="Soft Oat", berry="Dusty Blush", bird="Warm Cream")),
    "SG": ("Jade", "Warm Cream", dict(leaf="Sage", leaf2="Buttercream", petal="Peach", petal2="Dusty Blush", bird="Buttercream", bird2="Peach", wing="Peach", wing2="Buttercream")),
    "GP": ("Jade", "Warm Cream", dict(leaf="Sage", leaf2="Buttercream", petal="Peach", petal2="Buttercream", bird="Buttercream", bird2="Peach", wing="Peach", wing2="Buttercream")),
    "JG": ("Jade", "Warm Cream", dict(leaf="Moss", leaf2="Old Parchment", petal="Dusty Blush", petal2="Old Parchment", bird="Old Parchment", bird2="Dusty Blush", wing="Dusty Blush", wing2="Old Parchment")),
    "OH": ("Deep Garden Green", "Old Parchment", dict(leaf="Deep Olive", leaf2="Blue Willow", petal="Warm Cream", petal2="Old Parchment", object="Warm Gold", object2="Old Parchment", cream="Warm Cream", berry="Warm Berry", wing="Old Parchment", wing2="Warm Cream")),
    "CF": ("Jade", "Warm Cream", dict(leaf="Sage", leaf2="Buttercream", petal="Peach", petal2="Buttercream", water="Warm Cream", bird="Buttercream", object2="Buttercream")),
    "SP": ("Jade", "Warm Cream", dict(leaf="Sage", leaf2="Buttercream", petal="Peach", petal2="Buttercream", bird="Buttercream", bird2="Peach", wing="Buttercream", wing2="Peach")),
    "BV": ("Deep Garden Green", "Old Parchment", dict(leaf="Jade", leaf2="Blue Willow", petal="Terracotta Rose", petal2="Warm Gold", object="Warm Gold", object2="Old Parchment")),
    "GA": ("Deep Garden Green", "Old Parchment", dict(leaf="Moss", leaf2="Blue Willow", petal="Peach", petal2="Warm Cream", mountain="Blue Willow", mountain2="Jade", object="Warm Gold", bird="Warm Cream")),
    "IW": ("Blue Willow", "Warm Cream", dict(leaf="Jade", leaf2="Sage", petal="Warm Cream", petal2="Dusty Blush", water="Warm Cream", bird="Warm Cream", bird2="Sage")),
}


def scheme_colors(code, ground=None, line=None, roles=None):
    sch = dict(BASE)
    sch.update(SCHEMES[code])
    if ground == "dark":
        g, l, ov = DARK[code]
        sch.update(ov)
        sch["ground"] = g
        sch["line"] = l
    elif ground:
        sch["ground"] = ground
        if line:
            sch["line"] = line
    if roles:
        sch.update(roles)
    return {k: C(v) for k, v in sch.items()}


# ------------------------------------------------------------------ placements
def ent(e):
    if isinstance(e, str):
        return (e, {}, None, 1.0)
    e = tuple(e)
    return (e[0], e[1] if len(e) > 1 else {}, e[2] if len(e) > 2 else None, e[3] if len(e) > 3 else 1.0)


def pl(name, kw, x, y, size, rotd=0, flip=False, mode=None, seed=0):
    return dict(fn=M.MOTIFS[name], kw=kw, x=x, y=y, size=size, rot=rotd, flip=flip, mode=mode, seed=seed)


def gen_hero(spec, w, h, rng):
    S = w * spec.get("size", 0.2)
    cw, ch = w * spec.get("cell", 0.84), w * spec.get("cell", 0.84) * spec.get("ratio", 1.08)
    blooms = [ent(e) for e in spec["blooms"]]
    foliage = [ent(e) for e in spec.get("foliage", ["leaf_simple"])]
    fillers = [ent(e) for e in spec.get("fillers", [])]
    vine = spec.get("vine", "vine_s")
    anchors = spec.get("anchors", [(0.30, 0.66, 1.0), (0.74, 0.3, 0.92), (0.84, 0.86, 0.64), (0.16, 0.16, 0.6)])
    out = []
    # vines
    if vine:
        pairs = [(0, 1), (1, 2)] if len(anchors) >= 3 else [(0, 1)]
        for (a, b) in pairs:
            ax, ay, _ = anchors[a]
            bx, by, _ = anchors[b]
            mx, my = (ax + bx) / 2 * cw, (ay + by) / 2 * ch
            ang = math.degrees(math.atan2((by - ay) * ch, (bx - ax) * cw))
            out.append(pl(vine, {}, mx, my, S * spec.get("vinesize", 0.9), ang + rng.uniform(-8, 8), seed=rng.random()))
    # leaves around anchors
    fi = 0
    for k, (ax, ay, mult) in enumerate(anchors[:len(blooms)]):
        nleaf = spec.get("leaves", 4) if k < 2 else 2
        base_a = rng.uniform(0, 360)
        for i in range(nleaf):
            a = base_a + 360.0 * i / nleaf + rng.uniform(-18, 18)
            r = S * mult * 0.92
            name, kw, mode, sm = foliage[fi % len(foliage)]
            fi += 1
            out.append(pl(name, kw, ax * cw + r * math.cos(math.radians(a)), ay * ch + r * math.sin(math.radians(a)),
                          S * 0.48 * sm, a + rng.uniform(-20, 20), mode=mode, seed=rng.random()))
    # fillers in the gaps
    fpos = spec.get("fpos", [(0.52, 0.5), (0.06, 0.42), (0.5, 0.05), (0.96, 0.56), (0.4, 0.92)])
    for i, f in enumerate(fillers):
        name, kw, mode, sm = f
        fx, fy = fpos[i % len(fpos)]
        out.append(pl(name, kw, fx * cw, fy * ch, S * 0.42 * sm, rng.uniform(-30, 30), mode=mode, seed=rng.random()))
    # blooms last (on top)
    for k, (ax, ay, mult) in enumerate(anchors[:len(blooms)]):
        name, kw, mode, sm = blooms[k % len(blooms)]
        out.append(pl(name, kw, ax * cw, ay * ch, S * mult * sm, rng.uniform(-25, 25), mode=mode, seed=rng.random()))
    return out, cw, ch


def gen_toss(spec, w, h, rng):
    S = w * spec.get("size", 0.08)
    cw = w * spec.get("cell", 0.5)
    ch = cw * spec.get("ratio", 1.0)
    items = [ent(e) for e in spec["items"]]
    n = spec.get("n", len(items))
    cols = int(math.ceil(math.sqrt(n)))
    rows = int(math.ceil(n / float(cols)))
    cells = [(i, j) for i in range(cols) for j in range(rows)]
    rng.shuffle(cells)
    rotr = spec.get("rot", 360)
    out = []
    jit = spec.get("jit", 0.22)
    order = list(range(n))
    for idx in order:
        i, j = cells[idx % len(cells)]
        name, kw, mode, sm = items[idx % len(items)]
        x = (i + 0.5 + rng.uniform(-jit, jit)) / cols * cw
        y = (j + 0.5 + rng.uniform(-jit, jit)) / rows * ch
        rotd = rng.uniform(-rotr / 2.0, rotr / 2.0)
        flip = rng.random() < 0.5 if spec.get("flip", True) else False
        out.append(pl(name, kw, x, y, S * sm * rng.uniform(0.9, 1.1), rotd, flip, mode, seed=rng.random()))
    # bigger items last so they sit on top
    out.sort(key=lambda p: p["size"])
    return out, cw, ch


def gen_specimen(spec, w, h, rng):
    S = w * spec.get("size", 0.13)
    cw = w * spec.get("cell", 0.55)
    ch = cw * spec.get("ratio", 1.1)
    items = [ent(e) for e in spec["items"]]
    pos = spec.get("pos") or ([(0.28, 0.3), (0.76, 0.78)] if len(items) <= 2 else [(0.25, 0.28), (0.72, 0.5), (0.4, 0.86)])
    rotr = spec.get("rot", 24)
    out = []
    for i, (px, py) in enumerate(pos):
        name, kw, mode, sm = items[i % len(items)]
        rotd = rng.uniform(-rotr / 2.0, rotr / 2.0)
        if spec.get("alt"):
            rotd = (spec.get("alt") if i % 2 == 0 else -spec.get("alt")) + rng.uniform(-4, 4)
        out.append(pl(name, kw, px * cw, py * ch, S * sm, rotd, flip=(i % 2 == 1 and spec.get("flip", False)), mode=mode, seed=rng.random()))
    return out, cw, ch


def gen_trail(spec, w, h, rng):
    """A long trailing motif on the cell diagonal with small items along it (half-drop links the trails)."""
    S = w * spec.get("size", 0.07)
    cw = w * spec.get("cell", 0.6)
    ch = cw * spec.get("ratio", 1.0)
    trail = ent(spec.get("trail", "vine_s"))
    along = [ent(e) for e in spec.get("along", [])]
    ang = spec.get("angle", 38)
    out = []
    tsize = spec.get("trailsize", 0.5) * cw
    out.append(pl(trail[0], trail[1], cw * 0.5, ch * 0.5, tsize, ang, mode=trail[2], seed=rng.random()))
    for i, t in enumerate(spec.get("ts", (0.2, 0.5, 0.8))):
        if not along:
            break
        name, kw, mode, sm = along[i % len(along)]
        dx = (t - 0.5) * tsize * 2.0
        x = cw * 0.5 + dx * math.cos(math.radians(ang))
        y = ch * 0.5 + dx * math.sin(math.radians(ang))
        side = 1 if i % 2 else -1
        off = S * 0.9 * side
        x += -off * math.sin(math.radians(ang))
        y += off * math.cos(math.radians(ang))
        out.append(pl(name, kw, x, y, S * sm, ang + side * 40 + rng.uniform(-15, 15), mode=mode, seed=rng.random()))
    return out, cw, ch


def gen_scatter(spec, w, h, rng):
    S = w * spec.get("size", 0.06)
    cw = w * spec.get("cell", 0.4)
    ch = cw * spec.get("ratio", 1.0)
    items = [ent(e) for e in spec["items"]]
    rows, cols = spec.get("rows", 2), spec.get("cols", 2)
    jit = spec.get("jit", 0.1)
    rotr = spec.get("rot", 40)
    out = []
    k = 0
    for j in range(rows):
        for i in range(cols):
            name, kw, mode, sm = items[k % len(items)]
            k += 1
            x = (i + 0.5 + (0.5 if (j % 2 and spec.get("stagger", True)) else 0) + rng.uniform(-jit, jit)) / cols * cw
            y = (j + 0.5 + rng.uniform(-jit, jit)) / rows * ch
            x = x % cw
            out.append(pl(name, kw, x, y, S * sm * rng.uniform(0.85, 1.15), rng.uniform(-rotr / 2.0, rotr / 2.0),
                          flip=rng.random() < 0.5 if spec.get("flip", True) else False, mode=mode, seed=rng.random()))
    return out, cw, ch


def gen_micro(spec, w, h, rng):
    s = dict(spec)
    s.setdefault("size", 0.026)
    s.setdefault("cell", 0.26)
    s.setdefault("rows", 3)
    s.setdefault("cols", 3)
    s.setdefault("rot", 360)
    s.setdefault("jit", 0.18)
    return gen_scatter(s, w, h, rng)


def gen_medallion(spec, w, h, rng):
    S = w * spec.get("size", 0.25)
    cw = w * spec.get("cell", 0.55)
    ch = cw * spec.get("ratio", 1.0)
    item = ent(spec["item"])
    out = []
    for i, e in enumerate(spec.get("satellites", [])):
        name, kw, mode, sm = ent(e)
        spos = spec.get("spos", [(0.08, 0.1), (0.92, 0.9), (0.9, 0.12), (0.1, 0.9)])
        px, py = spos[i % len(spos)]
        out.append(pl(name, kw, px * cw, py * ch, S * 0.3 * sm, rng.uniform(-30, 30), mode=mode, seed=rng.random()))
    out.append(pl(item[0], item[1], cw * 0.5, ch * 0.5, S * item[3], spec.get("rot", 0), mode=item[2], seed=rng.random()))
    return out, cw, ch


def gen_bands(spec, w, h, rng):
    """Horizontal bands of vignettes; each band row is offset (brick) so clusters stagger."""
    S = w * spec.get("size", 0.2)
    cw = w * spec.get("cell", 0.6)
    ch = h * spec.get("band", 0.34)
    items = [ent(e) for e in spec["items"]]
    out = []
    for i, e in enumerate(items):
        name, kw, mode, sm = e
        px, py = spec.get("pos", [(0.5, 0.5), (0.15, 0.6), (0.85, 0.4)])[i % 3]
        out.append(pl(name, kw, px * cw, py * ch, S * sm, rng.uniform(-4, 4), mode=mode, seed=rng.random()))
    return out, cw, ch


GENERATORS = dict(hero=gen_hero, toss=gen_toss, specimen=gen_specimen, trail=gen_trail, scatter=gen_scatter,
                  micro=gen_micro, medallion=gen_medallion, bands=gen_bands)


# ------------------------------------------------------------------ periodic textures drawn directly
def _wobble_line(pen, pts, color, lw, amp, rng):
    q = [(x + rng.uniform(-amp, amp), y + rng.uniform(-amp, amp)) for (x, y) in pts]
    pen.curve(q, line=color, lw=lw, tension=0.9)


def tex_stripe(pen, x, y, w, h, spec, cols, rng):
    sp = w * spec.get("spacing", 0.07)
    names = spec.get("colors", ["line"])
    contrast = spec.get("contrast", 0.5)
    amp = spec.get("amp", 0.0) * w
    wave = spec.get("wave", 0.0) * w
    xx = x + sp * 0.5
    k = 0
    while xx < x + w + sp:
        col = M.mix(cols.get(names[k % len(names)], pen.line), cols["ground"], contrast)
        pts = []
        yy = y - 10
        while yy <= y + h + 10:
            pts.append((xx + wave * math.sin((yy - y) / h * 2 * math.pi * spec.get("freq", 1.0) + k), yy))
            yy += 9
        _wobble_line(pen, pts, col, spec.get("lw", 0.9), amp, rng)
        xx += sp
        k += 1


def tex_lattice(pen, x, y, w, h, spec, cols, rng):
    sp = w * spec.get("spacing", 0.16)
    col = M.mix(cols.get(spec.get("color", "line"), pen.line), cols["ground"], spec.get("contrast", 0.55))
    amp = spec.get("amp", 0.012) * w
    variant = spec.get("variant", "diag")
    lw = spec.get("lw", 0.8)
    if variant == "arches":
        rowh = sp
        yy = y - rowh
        r = 0
        while yy < y + h + rowh:
            xx = x - sp + (sp * 0.5 if r % 2 else 0)
            while xx < x + w + sp:
                p = pen.arc(xx, yy, sp * 0.5, rowh * 0.95, 0, 180)
                pen.stroke(p, line=col, lw=lw)
                xx += sp
            yy += rowh
            r += 1
        return
    L = math.hypot(w, h) + 2 * sp
    for d in (1, -1):
        off = -L
        while off < L:
            pts = []
            for t in range(-2, int(L / 9) + 3):
                s = t * 9
                px = x + w / 2 + off / math.sqrt(2) + s / math.sqrt(2) * (1 if d == 1 else 1)
                py = y + h / 2 - off / math.sqrt(2) * d + s / math.sqrt(2) * d
                pts.append((px, py))
            _wobble_line(pen, pts, col, lw, amp if variant != "twig" else amp * 0.4, rng)
            off += sp


def tex_scallop(pen, x, y, w, h, spec, cols, rng):
    sp = w * spec.get("spacing", 0.18)
    rowh = sp * spec.get("rowratio", 0.9)
    col = M.mix(cols.get(spec.get("color", "line"), pen.line), cols["ground"], spec.get("contrast", 0.45))
    gold = M.mix(cols.get("object", pen.line), cols["ground"], spec.get("contrast", 0.45) * 0.6)
    yy = y - rowh
    r = 0
    while yy < y + h + rowh:
        xx = x - sp + (sp * 0.5 if r % 2 else 0)
        while xx < x + w + sp:
            p = pen.arc(xx, yy, sp * 0.5, rowh * 0.55, 0, 180)
            pen.stroke(p, line=col, lw=spec.get("lw", 0.8))
            if spec.get("feather"):
                # two small feather strokes under the apex
                pen.line_to(xx, yy + rowh * 0.55, xx - sp * 0.12, yy + rowh * 0.3, line=col, lw=0.6)
                pen.line_to(xx, yy + rowh * 0.55, xx + sp * 0.12, yy + rowh * 0.3, line=col, lw=0.6)
            if spec.get("leaves"):
                pen.c.saveState()
                pen.cols = dict(pen.cols)
                M.Pen.place(pen, M.leaf_pair, xx + sp * 0.5, yy, sp * 0.12, rotd=0, mode="light")
                pen.c.restoreState()
            if spec.get("gold") and r % 2 == 0:
                p = pen.arc(xx, yy - rowh * 0.12, sp * 0.3, rowh * 0.25, 20, 160)
                pen.stroke(p, line=gold, lw=0.6)
            xx += sp
        yy += rowh
        r += 1


def tex_zigzag(pen, x, y, w, h, spec, cols, rng):
    sp = w * spec.get("spacing", 0.22)
    rowh = h * spec.get("rowh", 0.11)
    amp = rowh * spec.get("amp", 0.35)
    col = M.mix(cols.get(spec.get("color", "line"), pen.line), cols["ground"], spec.get("contrast", 0.55))
    yy = y - rowh
    r = 0
    while yy < y + h + rowh:
        pts = []
        xx = x - sp
        k = 0
        while xx < x + w + sp:
            pts.append((xx + (sp * 0.5 if r % 2 else 0), yy + (amp if k % 2 == 0 else -amp) * rng.uniform(0.8, 1.1)))
            xx += sp * 0.5
            k += 1
        pen.curve(pts, line=col, lw=spec.get("lw", 0.8), tension=0.6)
        yy += rowh
        r += 1


def tex_waves(pen, x, y, w, h, spec, cols, rng):
    rowh = h * spec.get("rowh", 0.09)
    col = M.mix(cols.get(spec.get("color", "water"), pen.line), cols["ground"], spec.get("contrast", 0.5))
    yy = y - rowh * 0.5
    r = 0
    while yy < y + h + rowh:
        xx = x - w * 0.2 + (w * 0.13 if r % 2 else 0)
        while xx < x + w + w * 0.1:
            seg = w * rng.uniform(0.18, 0.4)
            p = pen.arc(xx + seg / 2, yy, seg / 2, rowh * 0.35, 200, 340)
            pen.stroke(p, line=col, lw=spec.get("lw", 0.7))
            xx += seg + w * rng.uniform(0.04, 0.12)
        yy += rowh
        r += 1


def tex_flow(pen, x, y, w, h, spec, cols, rng):
    """Vertical shallow S-curves (Flowing Vine blender)."""
    sp = w * spec.get("spacing", 0.14)
    col = M.mix(cols.get(spec.get("color", "leaf"), pen.line), cols["ground"], spec.get("contrast", 0.55))
    xx = x - sp
    k = 0
    while xx < x + w + sp:
        pts = []
        yy = y - h * 0.2
        while yy < y + h * 1.2:
            pts.append((xx + sp * 0.35 * math.sin((yy - y) / h * 2 * math.pi + k * 0.9), yy))
            yy += h * 0.08
        pen.curve(pts, line=col, lw=spec.get("lw", 0.8), tension=1.0)
        xx += sp
        k += 1


def tex_weave(pen, x, y, w, h, spec, cols, rng):
    s = w * spec.get("spacing", 0.07)
    col = M.mix(cols.get("object2", pen.line), cols["ground"], spec.get("contrast", 0.2))
    col2 = M.mix(cols.get("line", pen.line), cols["ground"], 0.7)
    j = 0
    yy = y - s
    while yy < y + h + s:
        i = 0
        xx = x - s
        while xx < x + w + s:
            if (i + j) % 2 == 0:
                for k in range(3):
                    off = (k - 1) * s * 0.28
                    pen.line_to(xx + s * 0.1, yy + off + s * 0.5, xx + s * 0.9, yy + off + s * 0.5, line=col, lw=0.8)
            else:
                for k in range(3):
                    off = (k - 1) * s * 0.28
                    pen.line_to(xx + off + s * 0.5, yy + s * 0.1, xx + off + s * 0.5, yy + s * 0.9, line=col, lw=0.8)
            if (i * 7 + j * 3) % 5 == 0:
                pen.line_to(xx + s * 0.2, yy + s * 0.2, xx + s * 0.6, yy + s * 0.75, line=col2, lw=0.5)
            xx += s
            i += 1
        yy += s
        j += 1


def tex_cloud(pen, x, y, w, h, spec, cols, rng):
    """Rows of soft scalloped cloud forms."""
    cw = w * spec.get("spacing", 0.3)
    rowh = cw * 0.55
    col = M.mix(cols.get("line", pen.line), cols["ground"], spec.get("contrast", 0.35))
    fill = M.mix(cols.get("leaf2", pen.line), cols["ground"], 0.7)
    yy = y - rowh
    r = 0
    while yy < y + h + rowh:
        xx = x - cw + (cw * 0.5 if r % 2 else 0)
        while xx < x + w + cw:
            # three overlapping humps on a flat base
            p = pen.path()
            p.moveTo(xx - cw * 0.38, yy)
            p.curveTo(xx - cw * 0.5, yy + rowh * 0.45, xx - cw * 0.18, yy + rowh * 0.55, xx - cw * 0.14, yy + rowh * 0.3)
            p.curveTo(xx - cw * 0.12, yy + rowh * 0.75, xx + cw * 0.12, yy + rowh * 0.75, xx + cw * 0.14, yy + rowh * 0.3)
            p.curveTo(xx + cw * 0.18, yy + rowh * 0.55, xx + cw * 0.5, yy + rowh * 0.45, xx + cw * 0.38, yy)
            p.close()
            pen.c.setFillColor(fill)
            pen.c.setStrokeColor(col)
            pen.c.setLineWidth(pen.lw * 0.8 / pen.scale)
            pen.c.drawPath(p, stroke=1, fill=1)
            # inner echo contour
            p = pen.arc(xx, yy + rowh * 0.05, cw * 0.22, rowh * 0.3, 0, 180)
            pen.stroke(p, line=col, lw=0.6)
            xx += cw
        yy += rowh
        r += 1


def tex_ribbon(pen, x, y, w, h, spec, cols, rng):
    sp = w * spec.get("spacing", 0.09)
    gold = M.mix(cols["object"], cols["ground"], 0.25)
    parch = M.mix(cols["object2"], cols["ground"], 0.1)
    xx = x + sp * 0.5
    k = 0
    while xx < x + w + sp:
        col = gold if k % 2 == 0 else M.mix(cols["line"], cols["ground"], 0.6)
        for off in (0, sp * 0.14):
            pts = []
            yy = y - h * 0.1
            while yy < y + h * 1.1:
                pts.append((xx + off + sp * 0.2 * math.sin((yy - y) / h * 2 * math.pi * 1.5 + k), yy))
                yy += h * 0.06
            pen.curve(pts, line=col, lw=0.75, tension=1.0)
        if k % 3 == 1:
            pen.cols = cols
            M.Pen.place(pen, M.star4, xx + sp * 0.07, y + h * (0.2 + 0.3 * (k % 2)), sp * 0.22, mode="paint")
        xx += sp
        k += 1


TEXTURES = dict(stripe=tex_stripe, lattice=tex_lattice, scallop=tex_scallop, zigzag=tex_zigzag, waves=tex_waves,
                flow=tex_flow, weave=tex_weave, cloud=tex_cloud, ribbon=tex_ribbon)


# ------------------------------------------------------------------ swatch renderer
def draw_swatch(c, x, y, w, h, code, pid, spec, seed):
    rng = random.Random(seed)
    cols = scheme_colors(code, spec.get("ground"), spec.get("line"), spec.get("roles"))
    c.saveState()
    p = c.beginPath()
    p.rect(x, y, w, h)
    c.clipPath(p, stroke=0, fill=0)
    c.setFillColor(cols["ground"])
    c.rect(x - 1, y - 1, w + 2, h + 2, stroke=0, fill=1)
    pen = M.Pen(c, rng)
    pen.cols = cols
    pen.ground = cols["ground"]
    pen.line = cols["line"]
    pen.lw = spec.get("lw", 0.55)
    pen.mode = spec.get("mode", "paint")
    gen = spec["gen"]
    if gen in TEXTURES:
        TEXTURES[gen](pen, x, y, w, h, spec, cols, rng)
        c.restoreState()
        return
    placements, cw, ch = GENERATORS[gen](spec, w, h, rng)
    lattice = spec.get("lattice", "halfdrop")
    nx = int(math.ceil(w / cw)) + 2
    ny = int(math.ceil(h / ch)) + 2
    for j in range(-1, ny):
        for i in range(-1, nx):
            ox, oy = i * cw, j * ch
            if lattice == "halfdrop":
                oy += (i % 2) * ch * 0.5
            elif lattice == "brick":
                ox += (j % 2) * cw * 0.5
            # skip cells entirely outside (with margin)
            if ox + cw + w * 0.3 < 0 or ox - w * 0.3 > w or oy + ch + h * 0.3 < 0 or oy - h * 0.3 > h:
                continue
            for pm in placements:
                pen.rng = random.Random(pm["seed"])
                pen.place(pm["fn"], x + ox + pm["x"], y + oy + pm["y"], pm["size"], pm["rot"], pm["flip"], pm["mode"], **pm["kw"])
    c.restoreState()


# ------------------------------------------------------------------ the 122 swatch specs
L = "light"
LN = "line"
SPECS = {
    # ---------------- HG Harvest Garden
    "HG01": dict(gen="hero", blooms=["dahlia", ("dahlia", {"view": "side"}), ("chrysanthemum", {}, None, 0.85), ("dahlia", {"view": "bud"}, None, 0.8)],
                 foliage=[("leaf_simple", {"kind": "pointed"}), ("oak_leaf", {}, None, 0.9)], fillers=["berry_cluster", ("leaf_simple", {}, L)], size=0.2),
    "HG02": dict(gen="toss", n=10, size=0.2, cell=1.0, ground="dark", items=["dahlia", ("acorn", {}, None, 0.4), ("berry_cluster", {}, None, 0.5), ("seed_pod", {"opened": True}, None, 0.55),
                 ("bare_twig", {}, None, 0.8), ("oak_leaf", {}, None, 0.55), ("chrysanthemum", {}, None, 0.85), ("apple", {}, None, 0.4), ("seed_pod", {}, None, 0.5), ("leaf_simple", {}, None, 0.5)]),
    "HG03": dict(gen="specimen", size=0.13, cell=0.6, lattice="brick", items=["dahlia", ("dahlia", {"view": "side"}), ("dahlia", {"view": "bud"}, None, 0.8)], pos=[(0.25, 0.28), (0.72, 0.5), (0.4, 0.86)]),
    "HG04": dict(gen="trail", size=0.1, cell=0.62, trail="vine_s", trailsize=0.6, along=["oak_leaf", ("berry_cluster", {}, None, 0.6), "oak_leaf"], ground="Peach"),
    "HG05": dict(gen="scatter", size=0.07, cell=0.42, items=[("seed_pod", {"opened": True}), "seed_pod"], mode=L, rot=60),
    "HG06": dict(gen="toss", n=6, size=0.065, cell=0.42, items=[("leaf_simple", {}), ("leaf_simple", {"kind": "pointed"}), ("oak_leaf", {}, None, 0.9)], roles=dict(leaf="Ochre", leaf2="Terracotta Rose"), lattice="brick", jit=0.32, rot=360),
    "HG07": dict(gen="trail", size=0.055, cell=0.5, trail=("vine_s", {"leaves": 2, "tendril": False}, LN), trailsize=0.55, along=[("berry_cluster", {"n": 3}), ("berry_cluster", {"n": 2}), ("berry_cluster", {"n": 3})], angle=42),
    "HG08": dict(gen="specimen", size=0.075, cell=0.45, alt=22, items=[("apple", {}, None, 0.9), ("leaf_pair", {}, L, 1.1)], pos=[(0.28, 0.3), (0.76, 0.78)], lattice="brick"),
    "HG09": dict(gen="lattice", spacing=0.2, contrast=0.6, amp=0.02, color="leaf"),
    "HG10": dict(gen="micro", size=0.026, items=["acorn", ("dot_mark", {"role": "berry"}, None, 0.35), ("leaf_simple", {}, None, 0.9), ("dot_mark", {"role": "leaf"}, None, 0.3)], cols=3, rows=3),
    # ---------------- CG Christmas at the Garden
    "CG01": dict(gen="hero", blooms=["poinsettia", ("camellia", {}, None, 0.9), ("poinsettia", {}, None, 0.75), ("camellia", {"view": "half"}, None, 0.7)],
                 foliage=[("leaf_simple", {"kind": "pointed"}), ("pine_sprig", {}, None, 1.1)], fillers=["berry_cluster", ("holly_leaf", {}, None, 1.1)], size=0.2,
                 roles=dict(petal="Warm Berry", petal2="Terracotta Rose")),
    "CG02": dict(gen="toss", n=9, size=0.2, cell=1.0, ground="dark", rot=90, items=[("pine_sprig", {"cone": True}, None, 1.1), ("berry_cluster", {}, None, 0.45), ("hellebore", {}, None, 0.6), ("pinecone", {}, None, 0.45),
                 ("pine_sprig", {}, None, 1.0), ("berry_cluster", {"n": 2}, None, 0.4), ("hellebore", {}, None, 0.5), ("holly_leaf", {}, None, 0.6), ("pine_sprig", {"cone": True}, None, 0.9)]),
    "CG03": dict(gen="toss", n=5, size=0.12, cell=0.62, items=[("camellia", {}), ("holly_leaf", {}, None, 0.75), ("camellia", {"view": "three-quarter"}), ("holly_leaf", {"berries": False}, None, 0.7), ("camellia", {"view": "half"}, None, 0.85)],
                 roles=dict(petal="Terracotta Rose", petal2="Dusty Blush"), rot=60),
    "CG04": dict(gen="specimen", size=0.14, cell=0.6, ground="Old Parchment", items=[("pine_sprig", {"cone": True}), ("pinecone", {}, None, 0.6), ("berry_cluster", {}, None, 0.55)], alt=18),
    "CG05": dict(gen="trail", size=0.06, cell=0.5, trail=("vine_s", {"leaves": 0, "tendril": False}, LN), trailsize=0.55, along=["holly_leaf", ("holly_leaf", {"berries": False}), "holly_leaf"], angle=40),
    "CG06": dict(gen="scatter", size=0.06, cell=0.42, items=["poinsettia"], rot=60, jit=0.2, roles=dict(petal="Warm Berry", petal2="Warm Berry")),
    "CG07": dict(gen="toss", n=4, size=0.06, cell=0.4, items=[("berry_cluster", {"n": 2}), ("berry_cluster", {"n": 2}), ("berry_cluster", {"n": 3})], rot=80),
    "CG08": dict(gen="specimen", size=0.065, cell=0.42, alt=30, items=[("pine_sprig", {"cone": True}), ("pinecone", {}, L, 0.7)], pos=[(0.28, 0.3), (0.76, 0.78)], lattice="brick"),
    "CG09": dict(gen="stripe", spacing=0.075, colors=["object", "leaf"], contrast=0.45, amp=0.006, lw=0.9),
    "CG10": dict(gen="micro", size=0.024, items=[("snow_mark", {}, LN), ("dot_mark", {"role": "berry"}, None, 0.35), ("dot_mark", {"role": "berry"}, None, 0.3), ("snow_mark", {}, LN, 0.9)], cols=3, rows=3, lw=0.45),
    # ---------------- WG Winter Garden
    "WG01": dict(gen="hero", blooms=[("camellia", {}, L), ("camellia", {"view": "three-quarter"}, L), ("bud", {}, L, 0.8), ("camellia", {"view": "half"}, L, 0.7)],
                 foliage=[("leaf_simple", {"kind": "pointed"}), ("pine_sprig", {}, None, 1.0)], fillers=[("berry_cluster", {}, L), ("leaf_simple", {}, LN)], size=0.21),
    "WG02": dict(gen="toss", n=9, size=0.2, cell=1.0, ground="Soft Oat", rot=120, items=[("bare_twig", {}, LN, 1.3), ("berry_cluster", {"n": 3}, None, 0.45), ("five_petal", {}, L, 0.45), ("pine_sprig", {}, None, 0.9),
                 ("bare_twig", {"buds": True}, LN, 1.1), ("berry_cluster", {"n": 2}, None, 0.4), ("five_petal", {}, L, 0.4), ("bare_twig", {}, LN, 1.0), ("pine_sprig", {}, None, 0.8)]),
    "WG03": dict(gen="specimen", size=0.13, cell=0.6, items=[("camellia", {}, L), ("camellia", {"view": "three-quarter"}, L), ("camellia", {"view": "half"}, L)], pos=[(0.25, 0.28), (0.72, 0.5), (0.4, 0.86)]),
    "WG04": dict(gen="specimen", size=0.14, cell=0.6, ground="dark", alt=25, items=[("pine_sprig", {"cone": True}), ("pinecone", {}, None, 0.65)]),
    "WG05": dict(gen="trail", size=0.055, cell=0.5, trail=("bare_twig", {}, LN), trailsize=0.6, along=[("berry_cluster", {"n": 2, "stems": False}), ("berry_cluster", {"n": 3, "stems": False})], angle=35, roles=dict(berry="Dusty Blush")),
    "WG06": dict(gen="toss", n=6, size=0.06, cell=0.42, items=[("leaf_simple", {}, L), ("leaf_simple", {"kind": "pointed"}, L)], lattice="brick", jit=0.32, rot=360),
    "WG07": dict(gen="scatter", size=0.045, cell=0.4, items=[("fir_tip", {}, None, 1.0)], rot=70, rows=2, cols=2),
    "WG08": dict(gen="specimen", size=0.075, cell=0.44, alt=20, items=[("bare_twig", {"buds": True}, LN)], pos=[(0.28, 0.3), (0.76, 0.78)], lattice="brick", roles=dict(petal="Dusty Blush")),
    "WG09": dict(gen="lattice", spacing=0.17, contrast=0.6, amp=0.004, color="line", variant="twig", lw=0.7),
    "WG10": dict(gen="micro", size=0.02, items=[("dot_mark", {"role": "berry"}), ("dot_mark", {"role": "leaf2"}, None, 0.5), ("berry_cluster", {"n": 2, "stems": False}, None, 1.2), ("dot_mark", {"role": "leaf2"}, None, 0.45)], jit=0.08, cols=3, rows=3, roles=dict(berry="Dusty Blush")),
    # ---------------- SG The Secret Garden
    "SG01": dict(gen="hero", blooms=["peony", ("camellia", {}, None, 0.9), ("wildflower", {}, None, 1.1), ("bud", {}, None, 0.7)],
                 foliage=[("leaf_simple", {}), ("leaf_pair", {}, None, 1.1)], fillers=[("five_petal", {}, None, 0.8), ("leaf_simple", {"kind": "pointed"}, L)], size=0.2),
    "SG02": dict(gen="toss", n=9, size=0.19, cell=1.0, ground="Buttercream", rot=40, items=[("five_petal", {}, None, 0.75), ("bird", {}, None, 0.7), ("camellia", {"view": "half"}, None, 0.8), ("butterfly", {}, None, 0.55),
                 ("leaf_pair", {}, None, 0.9), ("bird", {"pose": "turned"}, None, 0.65), ("wildflower", {"kind": "spike"}, None, 1.0), ("butterfly", {"pose": "side"}, None, 0.45), ("five_petal", {}, L, 0.6)]),
    "SG03": dict(gen="specimen", size=0.15, cell=0.6, items=[("peony", {}), ("leaf_pair", {}, L, 0.8), ("peony", {}, L)], pos=[(0.25, 0.28), (0.72, 0.5), (0.4, 0.86)]),
    "SG04": dict(gen="toss", n=7, size=0.15, cell=0.7, rot=50, items=[("wildflower", {}), ("wildflower", {"kind": "spike"}), ("wildflower", {"kind": "bell"}), ("wildflower", {}, L), ("wildflower", {"kind": "spike"}, L), ("grass_tuft", {}, LN, 0.8), ("wildflower", {"kind": "bell"})]),
    "SG05": dict(gen="toss", n=5, size=0.06, cell=0.42, items=[("butterfly", {}), ("bud", {}, L, 0.8), ("butterfly", {"pose": "side"}), ("bud", {}, L, 0.7), ("butterfly", {}, L)], rot=60),
    "SG06": dict(gen="scatter", size=0.06, cell=0.4, items=[("leaf_pair", {})], rot=40, lattice="brick"),
    "SG07": dict(gen="trail", size=0.07, cell=0.52, trail=("vine_s", {"leaves": 3}), trailsize=0.6, along=[("bud", {}, None, 0.9), ("bud", {}, L, 0.8), ("bud", {}, None, 0.85)], angle=36),
    "SG08": dict(gen="scatter", size=0.05, cell=0.38, items=[("five_petal", {}), ("five_petal", {}, None, 0.65)], rot=40, cols=2, rows=2),
    "SG09": dict(gen="lattice", variant="arches", spacing=0.2, contrast=0.6, color="leaf", lw=0.7),
    "SG10": dict(gen="micro", size=0.025, items=[("petal_loose", {}), ("petal_loose", {}, L, 0.8), ("petal_loose", {}, None, 0.7)], jit=0.3, cols=3, rows=3, roles=dict(petal="Dusty Blush")),
    # ---------------- GP The Garden Party
    "GP01": dict(gen="hero", blooms=[("five_petal", {}, None, 1.05), ("hellebore", {}, None, 0.95), ("strawberry", {}, None, 0.6), ("bird", {}, None, 0.65)],
                 foliage=[("leaf_simple", {}), ("leaf_pair", {}, None, 1.0)], fillers=[("butterfly", {}, None, 0.8), ("strawberry", {}, None, 0.65), ("five_petal", {}, L, 0.6)], size=0.2,
                 roles=dict(petal="Warm Coral", petal2="Peach")),
    "GP02": dict(gen="toss", n=9, size=0.22, cell=1.0, rot=30, items=[("wildflower", {}), ("wildflower", {"kind": "spike"}), ("grass_tuft", {}, LN), ("wildflower", {"kind": "bell"}), ("wildflower", {}, L), ("grass_tuft", {}, LN, 0.8), ("wildflower", {"kind": "spike"}, L), ("wildflower", {"kind": "bell"}, L), ("wildflower", {})]),
    "GP03": dict(gen="trail", size=0.11, cell=0.65, ground="Buttercream", trail=("vine_s", {"leaves": 4}), trailsize=0.62, along=[("strawberry", {}), ("five_petal", {}, None, 0.7), ("strawberry", {"seeds": 3}, L, 0.75)], angle=34),
    "GP04": dict(gen="specimen", size=0.13, cell=0.6, items=[("five_petal", {}), ("hellebore", {}), ("starflower", {})], pos=[(0.25, 0.28), (0.72, 0.5), (0.4, 0.86)]),
    "GP05": dict(gen="toss", n=5, size=0.055, cell=0.4, items=[("strawberry", {"seeds": 3})], rot=70),
    "GP06": dict(gen="scatter", size=0.065, cell=0.42, ground="Sage", line="Soft Cocoa", items=[("butterfly", {}), ("butterfly", {"pose": "side"})], rot=50, roles=dict(wing="Peach", wing2="Buttercream", bird2="Soft Cocoa")),
    "GP07": dict(gen="toss", n=4, size=0.075, cell=0.42, items=[("wildflower", {}), ("wildflower", {"kind": "spike"}), ("wildflower", {"kind": "bell"}), ("wildflower", {}, L)], rot=40, flip=True),
    "GP08": dict(gen="scatter", size=0.06, cell=0.4, items=[("leaf_pair", {})], rot=180, lattice="brick"),
    "GP09": dict(gen="stripe", spacing=0.065, colors=["petal", "petal2"], contrast=0.45, amp=0.008, wave=0.004, freq=1.5, lw=0.9),
    "GP10": dict(gen="micro", size=0.024, items=[("petal_loose", {}), ("dot_mark", {"role": "accent"}, None, 0.45), ("petal_loose", {}, L), ("dot_mark", {"role": "center"}, None, 0.35)], jit=0.25, cols=3, rows=3),
    # ---------------- JG Jade Garden
    "JG01": dict(gen="hero", blooms=["camellia", ("camellia", {"view": "three-quarter"}), ("bell_flower", {}, None, 1.0), ("camellia", {"view": "half"}, None, 0.72)],
                 foliage=[("leaf_simple", {"kind": "pointed"}), ("leaf_simple", {}, L)], fillers=[("bud", {}, None, 0.8), ("ginkgo", {}, L, 0.7)], size=0.2, ground="Old Parchment"),
    "JG02": dict(gen="toss", n=9, size=0.19, cell=1.0, rot=40, items=[("camellia", {}, None, 0.8), ("bird", {}, None, 0.65), ("butterfly", {}, None, 0.55), ("dragonfly", {}, None, 0.6), ("five_petal", {}, None, 0.6),
                 ("leaf_pair", {}, None, 0.9), ("camellia", {"view": "half"}, L, 0.7), ("bird", {"pose": "turned"}, None, 0.6), ("bell_flower", {}, None, 0.8)]),
    "JG03": dict(gen="toss", n=6, size=0.11, cell=0.55, ground="dark", rot=40, jit=0.1, items=[("bell_flower", {}), ("five_petal", {}), ("wildflower", {"kind": "bell"}), ("leaf_pair", {}), ("five_petal", {}, L, 0.8), ("bell_flower", {}, L)]),
    "JG04": dict(gen="specimen", size=0.13, cell=0.58, items=[("camellia", {}), ("bud", {}, None, 0.75)], pos=[(0.28, 0.3), (0.76, 0.78)], lattice="brick"),
    "JG05": dict(gen="trail", size=0.07, cell=0.55, trail=("vine_s", {"leaves": 5, "tendril": False}), trailsize=0.65, along=[("leaf_simple", {"kind": "narrow"}, L)], ts=(0.3, 0.7), angle=40),
    "JG06": dict(gen="toss", n=5, size=0.06, cell=0.42, ground="Old Parchment", items=[("ginkgo", {}), ("ginkgo", {}, L)], rot=90),
    "JG07": dict(gen="cloud", spacing=0.32, contrast=0.3),
    "JG08": dict(gen="scatter", size=0.055, cell=0.4, items=[("bud", {}, L)], rot=50, rows=2, cols=2, jit=0.16),
    "JG09": dict(gen="lattice", spacing=0.18, contrast=0.6, amp=0.02, color="leaf"),
    "JG10": dict(gen="micro", size=0.024, items=[("seed_mark", {}), ("petal_loose", {}, None, 0.9), ("seed_mark", {}, L, 0.8)], jit=0.22, cols=3, rows=3),
    # ---------------- OH O Holy Night (12)
    "OH01": dict(gen="medallion", size=0.22, cell=0.5, ratio=1.3, ground="dark", item="manger_scene", satellites=[("olive_sprig", {}, None, 1.1), ("star4", {}, None, 0.25), ("olive_sprig", {}, None, 1.0), ("star4", {}, None, 0.2)],
                 spos=[(0.08, 0.12), (0.9, 0.9), (0.92, 0.2), (0.1, 0.9)]),
    "OH02": dict(gen="specimen", size=0.2, cell=0.55, ratio=1.25, items=[("angel", {}), ("angel", {})], pos=[(0.3, 0.32), (0.78, 0.8)], alt=6, flip=True, lattice="halfdrop"),
    "OH03": dict(gen="hero", blooms=[("hellebore", {}, None, 0.9), ("holly_leaf", {}, None, 0.95), ("hellebore", {}, None, 0.75), ("fir_tip", {}, None, 0.95)],
                 foliage=[("holly_leaf", {"berries": False}, None, 0.9), ("olive_sprig", {}, None, 1.1)], fillers=[("berry_cluster", {}, None, 0.9), ("star4", {}, None, 0.45)], size=0.19, ground="dark", vine="olive_sprig"),
    "OH04": dict(gen="bands", size=0.2, cell=0.55, band=0.5, lattice="brick", ground="Blue Willow", line="Soft Cocoa", roles=dict(cream="Warm Cream", object="Warm Gold", leaf="Deep Garden Green", leaf2="Deep Olive", object2="Old Parchment", ground="Blue Willow"),
                 items=[("village", {})], pos=[(0.5, 0.5)]),
    "OH05": dict(gen="toss", n=5, size=0.14, cell=0.6, rot=8, items=[("candle", {}), ("olive_sprig", {}, None, 0.5), ("candle", {}, None, 0.8), ("dot_mark", {"role": "object"}, None, 0.08), ("candle", {}, None, 0.9)], flip=False),
    "OH06": dict(gen="specimen", size=0.15, cell=0.62, ground="Old Parchment", items=[("gifts", {}), ("star8", {}, None, 0.25), ("gifts", {}, None, 0.9)], pos=[(0.3, 0.3), (0.7, 0.58), (0.78, 0.82)], alt=5, lattice="halfdrop"),
    "OH07": dict(gen="specimen", size=0.2, cell=0.62, items=[("leaf_arch", {}), ("leaf_arch", {"center": "candle"})], pos=[(0.3, 0.32), (0.8, 0.82)], rot=0, lattice="halfdrop"),
    "OH08": dict(gen="toss", n=5, size=0.13, cell=0.6, rot=40, items=[("bell", {}), ("bell", {"bow": False, "sprig": False}, None, 0.85), ("fir_tip", {}, None, 0.6), ("bell", {}, None, 0.9), ("bell", {"bow": False, "sprig": False}, None, 0.8)], jit=0.12),
    "OH09": dict(gen="scatter", size=0.045, cell=0.36, ground="dark", items=[("star8", {}), ("star8", {}, None, 0.55), ("dot_mark", {"role": "cream"}, None, 0.15), ("star8", {}, LN, 0.6)], rot=0, cols=2, rows=2, flip=False),
    "OH10": dict(gen="scallop", spacing=0.2, contrast=0.4, feather=True, gold=True, color="line", lw=0.7),
    "OH11": dict(gen="ribbon", spacing=0.1),
    "OH12": dict(gen="weave", spacing=0.06, contrast=0.15),
    # ---------------- CF Come Thou Fount
    "CF01": dict(gen="hero", blooms=[("wildflower", {}, None, 1.1), ("wildflower", {"kind": "spike"}, None, 1.1), ("five_petal", {}, None, 0.8), ("wildflower", {"kind": "bell"}, None, 0.9)],
                 foliage=[("leaf_simple", {"kind": "narrow"}, L)], fillers=[("petal_loose", {}, None, 0.5), ("five_petal", {}, L, 0.5)], size=0.19, vine="stream", vinesize=1.5, leaves=2),
    "CF02": dict(gen="toss", n=9, size=0.19, cell=1.0, ground="Buttercream", rot=30, items=[("ripples", {}, None, 1.0), ("five_petal", {}, None, 0.7), ("wildflower", {}, None, 1.0), ("bird", {}, None, 0.6), ("butterfly", {}, None, 0.5),
                 ("ripples", {}, None, 0.9), ("wildflower", {"kind": "bell"}, None, 0.9), ("five_petal", {}, L, 0.6), ("butterfly", {"pose": "side"}, None, 0.45)]),
    "CF03": dict(gen="specimen", size=0.15, cell=0.6, items=[("wildflower", {}), ("wildflower", {"kind": "spike"}), ("wildflower", {"kind": "bell"})], pos=[(0.25, 0.28), (0.72, 0.5), (0.4, 0.86)]),
    "CF04": dict(gen="medallion", size=0.17, cell=0.62, ratio=1.1, ground="Buttercream", item="fountain", satellites=[("five_petal", {}, None, 0.7), ("wildflower", {}, None, 1.2), ("five_petal", {}, L, 0.6), ("wildflower", {"kind": "spike"}, None, 1.1)]),
    "CF05": dict(gen="toss", n=5, size=0.07, cell=0.42, items=[("wildflower", {}), ("wildflower", {"kind": "bell"}), ("wildflower", {}, L)], rot=30, flip=True),
    "CF06": dict(gen="trail", size=0.07, cell=0.55, trail=("stream", {}, None), trailsize=0.42, along=[("leaf_pair", {}, L), ("leaf_simple", {}, L), ("leaf_pair", {}, L)], angle=20),
    "CF07": dict(gen="scatter", size=0.06, cell=0.44, ground="Sage", line="Soft Cocoa", items=[("butterfly", {}), ("bud", {}, L, 0.7), ("butterfly", {"pose": "side"})], rot=50, roles=dict(wing="Peach", wing2="Buttercream", bird2="Soft Cocoa", petal="Peach")),
    "CF08": dict(gen="toss", n=6, size=0.05, cell=0.4, items=[("petal_loose", {}), ("petal_loose", {}, L)], rot=360),
    "CF09": dict(gen="flow", spacing=0.15, contrast=0.6, color="leaf"),
    "CF10": dict(gen="micro", size=0.022, items=[("raindrop", {}, L), ("petal_loose", {}, None, 0.9), ("dot_mark", {"role": "water"}, None, 0.3), ("raindrop", {}, None, 0.8)], jit=0.3, cols=3, rows=3),
    # ---------------- SP His Eye Is on the Sparrow
    "SP01": dict(gen="hero", blooms=[("bird", {}, None, 0.85), ("five_petal", {}, None, 0.9), ("bird", {"pose": "turned"}, None, 0.8), ("five_petal", {}, None, 0.7)],
                 foliage=[("leaf_simple", {"kind": "pointed"}), ("leaf_pair", {}, None, 1.0)], fillers=[("berry_cluster", {}, None, 0.8), ("bud", {}, L, 0.7)], size=0.2, vine="bare_twig", vinesize=1.2),
    "SP02": dict(gen="toss", n=8, size=0.21, cell=1.0, ground="dark", rot=40, items=[("willow_branch", {}, None, 1.2), ("bird", {}, None, 0.6), ("leaf_pair", {}, None, 0.9), ("willow_branch", {}, None, 1.1), ("bird", {"pose": "turned"}, None, 0.55), ("leaf_simple", {}, None, 0.6), ("willow_branch", {}, None, 1.0), ("leaf_pair", {}, L, 0.8)]),
    "SP03": dict(gen="specimen", size=0.13, cell=0.6, items=[("bird", {}), ("bare_twig", {}, LN, 1.1), ("bird", {"pose": "turned"})], pos=[(0.25, 0.3), (0.55, 0.55), (0.78, 0.82)], flip=True),
    "SP04": dict(gen="toss", n=6, size=0.11, cell=0.58, ground="Buttercream", rot=30, items=[("bird", {}, None, 0.9), ("five_petal", {}, None, 0.8), ("berry_cluster", {}, None, 0.7), ("bird", {"pose": "turned"}, None, 0.85), ("leaf_pair", {}, L, 0.9), ("five_petal", {}, L, 0.7)]),
    "SP05": dict(gen="toss", n=4, size=0.06, cell=0.4, items=[("bird_tiny", {})], rot=20, flip=True),
    "SP06": dict(gen="trail", size=0.06, cell=0.55, trail=("vine_s", {"leaves": 3, "tendril": False}), trailsize=0.6, along=[("bird_tiny", {}), ("leaf_simple", {}, L, 0.8), ("bird_tiny", {}, None, 0.9)], angle=32),
    "SP07": dict(gen="toss", n=6, size=0.07, cell=0.42, ground="Sage", line="Soft Cocoa", items=[("feather", {}), ("leaf_simple", {"kind": "pointed"}), ("feather", {}, L), ("leaf_simple", {}, L)], rot=90, roles=dict(wing="Buttercream", leaf="Jade")),
    "SP08": dict(gen="specimen", size=0.07, cell=0.42, alt=25, items=[("berry_cluster", {}), ("bare_twig", {}, LN, 1.0)], pos=[(0.28, 0.3), (0.76, 0.78)], lattice="brick"),
    "SP09": dict(gen="scallop", spacing=0.22, contrast=0.6, feather=True, color="line", lw=0.7, rowratio=0.8),
    "SP10": dict(gen="micro", size=0.026, items=[("bird_tiny", {}), ("leaf_simple", {}, L, 0.7), ("bird_tiny", {}, None, 0.9), ("leaf_simple", {"kind": "pointed"}, L, 0.6)], rot=30, cols=3, rows=3),
    # ---------------- BV Be Thou My Vision
    "BV01": dict(gen="hero", blooms=[("vine_knot", {}, None, 1.0), ("five_petal", {}, None, 0.6), ("vine_knot", {}, None, 0.8), ("star8", {}, None, 0.3)],
                 foliage=[("oak_leaf", {}, None, 0.9), ("leaf_simple", {"kind": "pointed"}, L)], fillers=[("star8", {}, None, 0.35), ("berry_cluster", {}, None, 0.6)], size=0.19, vine="vine_s", leaves=3),
    "BV02": dict(gen="medallion", size=0.22, cell=0.62, ratio=1.1, ground="dark", item=("vine_knot", {}), satellites=[("oak_leaf", {}, None, 0.8), ("rose_small", {}, None, 0.6), ("oak_leaf", {}, None, 0.75), ("berry_cluster", {}, None, 0.6)],
                 spos=[(0.12, 0.12), (0.88, 0.88), (0.88, 0.14), (0.12, 0.86)]),
    "BV03": dict(gen="specimen", size=0.14, cell=0.58, items=[("rose_frame", {}), ("rose_frame", {})], pos=[(0.28, 0.3), (0.76, 0.78)], rot=0, lattice="halfdrop"),
    "BV04": dict(gen="specimen", size=0.12, cell=0.58, alt=30, items=[("oak_leaf", {}), ("star8", {}, None, 0.35), ("oak_leaf", {}, L, 0.9)], pos=[(0.25, 0.28), (0.6, 0.55), (0.78, 0.82)]),
    "BV05": dict(gen="scatter", size=0.07, cell=0.42, items=[("vine_knot", {"leaves": False})], rot=30, rows=2, cols=2, flip=False),
    "BV06": dict(gen="scatter", size=0.05, cell=0.36, items=[("star8", {}), ("star8", {}, None, 0.55)], rot=0, rows=2, cols=2, flip=False),
    "BV07": dict(gen="toss", n=5, size=0.07, cell=0.42, ground="dark", items=[("oak_leaf", {}), ("oak_leaf", {}, L)], rot=120),
    "BV08": dict(gen="scatter", size=0.05, cell=0.38, items=[("starflower", {}), ("five_petal", {}, L, 0.8)], rot=40, rows=2, cols=2),
    "BV09": dict(gen="lattice", spacing=0.2, contrast=0.6, amp=0.03, color="leaf"),
    "BV10": dict(gen="micro", size=0.022, items=[("star4", {}), ("curl", {}, None, 1.2), ("star4", {}, None, 0.7), ("curl", {}, None, 1.0)], jit=0.25, cols=3, rows=3),
    # ---------------- GA How Great Thou Art
    "GA01": dict(gen="medallion", size=0.27, cell=0.72, ratio=1.0, item=("mountains", {}), satellites=[("conifer", {}, None, 0.5), ("wildflower", {}, None, 0.55), ("sun_disk", {}, None, 0.28), ("wildflower", {"kind": "spike"}, None, 0.5), ("bird_tiny", {}, None, 0.22), ("star8", {}, None, 0.14)],
                 spos=[(0.78, 0.28), (0.22, 0.2), (0.78, 0.86), (0.4, 0.14), (0.3, 0.8), (0.62, 0.9)]),
    "GA02": dict(gen="hero", blooms=[("hellebore", {}, None, 0.9), ("five_petal", {}, None, 0.85), ("conifer", {}, None, 0.8), ("mountains", {}, L, 0.8)],
                 foliage=[("leaf_simple", {}), ("leaf_simple", {"kind": "pointed"}, L)], fillers=[("bird", {}, None, 0.7), ("star8", {}, None, 0.3), ("wildflower", {}, None, 0.9)], size=0.19),
    "GA03": dict(gen="bands", size=0.2, cell=0.8, band=0.5, lattice="brick", items=[("mountains", {}, L, 1.0), ("wildflower", {}, None, 0.5), ("wildflower", {"kind": "spike"}, None, 0.45)], pos=[(0.5, 0.68), (0.28, 0.22), (0.72, 0.22)]),
    "GA04": dict(gen="toss", n=7, size=0.12, cell=0.6, ground="dark", rot=40, items=[("star8", {}, None, 0.35), ("wildflower", {}, LN), ("five_petal", {}, L, 0.8), ("star8", {}, None, 0.25), ("wildflower", {"kind": "bell"}, LN), ("dot_mark", {"role": "object"}, None, 0.08), ("five_petal", {}, L, 0.7)]),
    "GA05": dict(gen="toss", n=4, size=0.075, cell=0.42, items=[("wildflower", {}), ("wildflower", {"kind": "spike"})], rot=20, flip=True),
    "GA06": dict(gen="bands", size=0.1, cell=0.5, band=0.3, lattice="brick", items=[("mountains", {}, L)], pos=[(0.5, 0.5)]),
    "GA07": dict(gen="scatter", size=0.04, cell=0.34, items=[("star8", {}), ("star8", {}, None, 0.6)], rot=0, rows=2, cols=2, flip=False),
    "GA08": dict(gen="toss", n=6, size=0.06, cell=0.42, items=[("leaf_simple", {}, L), ("leaf_simple", {"kind": "pointed"}, L), ("leaf_pair", {}, L, 0.9)], rot=360),
    "GA09": dict(gen="zigzag", spacing=0.3, rowh=0.12, amp=0.3, contrast=0.6, color="mountain"),
    "GA10": dict(gen="micro", size=0.026, items=[("starflower", {}), ("starflower", {}, L, 0.8)], jit=0.15, cols=3, rows=3),
    # ---------------- IW It Is Well
    "IW01": dict(gen="hero", blooms=[("water_lily", {}, None, 0.95), ("water_lily", {"view": "side"}, None, 0.95), ("lily_pad", {}, None, 0.75), ("water_lily", {}, L, 0.6)],
                 foliage=[("lily_pad", {}, None, 1.1), ("lily_pad", {}, L, 0.9)], fillers=[("ripples", {}, None, 1.0), ("ripples", {}, None, 0.8)], size=0.2, vine=None, leaves=2),
    "IW02": dict(gen="bands", size=0.2, cell=0.75, band=0.5, lattice="brick", ground="dark", items=[("water_lily", {"view": "side"}, None, 0.9), ("reeds", {}, None, 1.0), ("willow_branch", {}, None, 1.1)], pos=[(0.5, 0.45), (0.15, 0.55), (0.85, 0.6)]),
    "IW03": dict(gen="specimen", size=0.14, cell=0.6, items=[("water_lily", {}), ("water_lily", {"view": "side"}), ("lily_pad", {}, L, 0.9)], pos=[(0.25, 0.28), (0.72, 0.5), (0.4, 0.86)]),
    "IW04": dict(gen="specimen", size=0.17, cell=0.6, ground="Sage", line="Soft Cocoa", items=[("willow_branch", {}), ("willow_branch", {}, L, 0.8)], pos=[(0.28, 0.3), (0.76, 0.78)], alt=12, flip=True, roles=dict(leaf="Jade", leaf2="Warm Cream")),
    "IW05": dict(gen="scatter", size=0.06, cell=0.42, items=[("lily_pad", {}), ("lily_pad", {}, L, 0.8)], rot=360, rows=2, cols=2),
    "IW06": dict(gen="scatter", size=0.085, cell=0.44, items=[("reeds", {})], rot=10, rows=2, cols=2, flip=True),
    "IW07": dict(gen="bands", size=0.08, cell=0.5, band=0.28, lattice="brick", items=[("ripples", {}), ("leaf_simple", {}, L, 0.8), ("ripples", {}, None, 0.8)], pos=[(0.5, 0.5), (0.1, 0.4), (0.85, 0.65)]),
    "IW08": dict(gen="scatter", size=0.05, cell=0.4, items=[("water_lily", {"simple": True}, L)], rot=40, rows=2, cols=2),
    "IW09": dict(gen="waves", rowh=0.09, contrast=0.55, color="water"),
    "IW10": dict(gen="micro", size=0.024, items=[("water_lily", {"simple": True}, L), ("dot_mark", {"role": "water"}, None, 0.3), ("dot_mark", {"role": "water"}, None, 0.25), ("water_lily", {"simple": True}, L, 0.85)], jit=0.25, cols=3, rows=3),
}

# ------------------------------------------------------------------ isolated motif studies per collection (8 each, mixed line/paint)
STUDIES = {
    "HG": [("dahlia", {}, "paint"), ("dahlia", {"view": "side"}, "line"), ("dahlia", {"view": "bud"}, "paint"), ("oak_leaf", {}, "line"), ("oak_leaf", {}, "paint"), ("vine_s", {}, "line"), ("berry_cluster", {}, "paint"), ("acorn", {}, "line")],
    "CG": [("poinsettia", {}, "paint"), ("camellia", {}, "line"), ("holly_leaf", {}, "paint"), ("holly_leaf", {"berries": False}, "line"), ("pine_sprig", {}, "line"), ("pinecone", {}, "paint"), ("berry_cluster", {}, "paint"), ("ribbon", {}, "line")],
    "WG": [("camellia", {}, "line"), ("camellia", {"view": "three-quarter"}, "light"), ("camellia", {"view": "half"}, "line"), ("bare_twig", {"buds": True}, "line"), ("pinecone", {}, "paint"), ("pine_sprig", {}, "line"), ("bud", {}, "light"), ("berry_cluster", {}, "paint")],
    "SG": [("peony", {}, "paint"), ("camellia", {}, "line"), ("camellia", {"view": "half"}, "paint"), ("wildflower", {}, "line"), ("wildflower", {"kind": "spike"}, "paint"), ("bud", {}, "line"), ("bird", {}, "paint"), ("butterfly", {}, "line")],
    "GP": [("five_petal", {}, "paint"), ("hellebore", {}, "line"), ("wildflower", {}, "line"), ("strawberry", {}, "paint"), ("butterfly", {}, "paint"), ("bird", {}, "line"), ("petal_loose", {}, "paint"), ("leaf_pair", {}, "line")],
    "JG": [("camellia", {}, "paint"), ("camellia", {"view": "half"}, "line"), ("camellia", {"view": "three-quarter"}, "paint"), ("bell_flower", {}, "line"), ("vine_s", {}, "line"), ("ginkgo", {}, "paint"), ("bird", {}, "paint"), ("dragonfly", {}, "line")],
    "OH": [("manger_scene", {}, "line"), ("angel_wing", {}, "paint"), ("trumpet", {}, "paint"), ("bell", {}, "line"), ("candle", {}, "paint"), ("olive_sprig", {}, "line"), ("star8", {}, "paint"), ("angel", {}, "line")],
    "CF": [("wildflower", {}, "line"), ("wildflower", {"kind": "spike"}, "paint"), ("wildflower", {"kind": "bell"}, "line"), ("bird", {}, "paint"), ("butterfly", {}, "line"), ("ripples", {}, "paint"), ("fountain", {}, "line"), ("petal_loose", {}, "paint")],
    "SP": [("bird", {}, "paint"), ("bird", {"pose": "turned"}, "line"), ("angel_wing", {}, "line"), ("five_petal", {}, "paint"), ("berry_cluster", {}, "paint"), ("feather", {}, "line"), ("leaf_simple", {}, "paint"), ("leaf_simple", {"kind": "pointed"}, "line")],
    "BV": [("vine_knot", {}, "line"), ("oak_leaf", {}, "paint"), ("oak_leaf", {}, "line"), ("rose_small", {}, "paint"), ("star8", {}, "paint"), ("berry_cluster", {}, "line"), ("ornament_border", {}, "paint"), ("rose_frame", {}, "line")],
    "GA": [("mountains", {}, "line"), ("mountains", {}, "paint"), ("conifer", {}, "paint"), ("wildflower", {}, "line"), ("wildflower", {"kind": "spike"}, "paint"), ("bird", {}, "line"), ("sun_disk", {}, "paint"), ("star8", {}, "line")],
    "IW": [("water_lily", {}, "paint"), ("water_lily", {"view": "side"}, "line"), ("lily_pad", {}, "paint"), ("lily_pad", {}, "line"), ("reeds", {}, "line"), ("willow_branch", {}, "paint"), ("bird", {}, "line"), ("ripples", {}, "paint")],
}


def draw_studies(c, x, y, w, h, code, seed=5):
    rng = random.Random(seed)
    cols = scheme_colors(code)
    cols["ground"] = CREAM
    pen = M.Pen(c, rng)
    pen.cols = cols
    pen.ground = CREAM
    pen.line = cols["line"]
    pen.lw = 0.6
    items = STUDIES[code]
    n = len(items)
    size = min(h * 0.34, (w / n) * 0.36)
    for i, (name, kw, mode) in enumerate(items):
        cx = x + w * (i + 0.5) / n
        cy = y + h * 0.52
        pen.rng = random.Random(seed * 100 + i)
        pen.place(M.MOTIFS[name], cx, cy, size, 0, False, mode, **kw)


def draw_board(c, x, y, w, h, code, seed=11, label_h=0):
    """Draw a full concept board; returns list of (print_id, sx, sy, sw, sh) swatch rectangles.
    label_h reserves cream space beneath every swatch row for labels drawn by the page layout."""
    col = BY_CODE[code]
    prints = col["prints"]
    twelve = len(prints) == 12
    ncol, nrow = (4, 3) if twelve else (5, 2)
    gf = 0.72 if twelve else 0.64
    gap = 2.6
    c.saveState()
    c.setFillColor(CREAM)
    c.rect(x, y, w, h, stroke=0, fill=1)
    gh = h * gf
    gy = y + h - gh
    sw = (w - gap * (ncol + 1)) / ncol
    sh = (gh - gap * (nrow + 1) - label_h * nrow) / nrow
    rects = []
    for k, p in enumerate(prints):
        i = k % ncol
        j = k // ncol
        sx = x + gap + i * (sw + gap)
        sy = gy + gh - gap - (j + 1) * sh - j * (gap + label_h)
        draw_swatch(c, sx, sy, sw, sh, code, p["id"], SPECS[p["id"]], seed * 1000 + k)
        rects.append((p["id"], sx, sy, sw, sh))
    draw_studies(c, x, y, w, h - gh, code, seed)
    c.restoreState()
    return rects
