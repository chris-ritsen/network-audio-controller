# -*- coding: utf-8 -*-
"""Vector botanical motif library for the Ginger Root Studio concept boards.

Every motif draws into a ReportLab canvas around the origin inside roughly a unit
radius; Pen.place() handles position, rotation, scale and absolute line width.
Modes: 'paint' (flat gouache-like fills), 'light' (tinted fills), 'line' (contour only).
"""
import math
import random
from reportlab.lib.colors import Color, HexColor

# ------------------------------------------------------------------ colors

def hexc(h):
    return HexColor(h)


def mix(c1, c2, t):
    """Mix color c1 toward c2 by t (0..1)."""
    return Color(c1.red + (c2.red - c1.red) * t,
                 c1.green + (c2.green - c1.green) * t,
                 c1.blue + (c2.blue - c1.blue) * t)


def darker(c, t=0.2):
    return Color(c.red * (1 - t), c.green * (1 - t), c.blue * (1 - t))


# ------------------------------------------------------------------ geometry helpers

def rot(x, y, ang):
    a = math.radians(ang)
    ca, sa = math.cos(a), math.sin(a)
    return (x * ca - y * sa, x * sa + y * ca)


def bezier_arc(cx, cy, rx, ry, a0, a1, steps=None):
    """Return list of cubic segments approximating an elliptical arc from a0 to a1 (degrees)."""
    if steps is None:
        steps = max(1, int(math.ceil(abs(a1 - a0) / 90.0)))
    segs = []
    da = (a1 - a0) / steps
    for i in range(steps):
        t0 = math.radians(a0 + da * i)
        t1 = math.radians(a0 + da * (i + 1))
        k = 4.0 / 3.0 * math.tan((t1 - t0) / 4.0)
        p0 = (cx + rx * math.cos(t0), cy + ry * math.sin(t0))
        p3 = (cx + rx * math.cos(t1), cy + ry * math.sin(t1))
        p1 = (cx + rx * (math.cos(t0) - k * math.sin(t0)), cy + ry * (math.sin(t0) + k * math.cos(t0)))
        p2 = (cx + rx * (math.cos(t1) + k * math.sin(t1)), cy + ry * (math.sin(t1) - k * math.cos(t1)))
        segs.append((p0, p1, p2, p3))
    return segs


class Pen:
    """Drawing context: canvas + rng + style + current scale."""

    def __init__(self, c, rng=None):
        self.c = c
        self.rng = rng or random.Random(1)
        self.scale = 1.0
        self.lw = 0.55          # absolute stroke width in points
        self.mode = "paint"
        self.cols = {}
        self.line = hexc("#76563F")
        self.ground = hexc("#F7EEDC")
        self.c.setLineJoin(1)
        self.c.setLineCap(1)

    # ---- randomness
    def j(self, a):
        return self.rng.uniform(-a, a)

    def u(self, a, b):
        return self.rng.uniform(a, b)

    # ---- colors
    def col(self, role):
        return self.cols.get(role, self.line)

    def fill_for(self, role):
        if role is None:
            return None
        if self.mode == "line":
            return self.ground
        c = self.col(role)
        if self.mode == "light":
            return mix(c, self.ground, 0.5)
        return c

    def _apply(self, role, line=None, lw=1.0):
        c = self.c
        f = self.fill_for(role)
        if f is not None:
            c.setFillColor(f)
        c.setStrokeColor(line or self.line)
        c.setLineWidth(self.lw * lw / self.scale)
        return f is not None

    # ---- primitives
    def path(self):
        return self.c.beginPath()

    def draw(self, p, role=None, stroke=True, line=None, lw=1.0, close=True):
        hasfill = self._apply(role, line, lw)
        if close:
            p.close()
        self.c.drawPath(p, stroke=1 if stroke else 0, fill=1 if (hasfill and role is not None) else 0)

    def stroke(self, p, line=None, lw=1.0):
        self._apply(None, line, lw)
        self.c.drawPath(p, stroke=1, fill=0)

    def smooth(self, pts, closed=True, tension=1.0):
        """Catmull-Rom smoothing through pts -> path."""
        n = len(pts)
        p = self.path()
        p.moveTo(*pts[0])
        rng = range(n) if closed else range(n - 1)
        for i in rng:
            if closed:
                p0, p1, p2, p3 = pts[(i - 1) % n], pts[i], pts[(i + 1) % n], pts[(i + 2) % n]
            else:
                p0, p1, p2, p3 = pts[max(i - 1, 0)], pts[i], pts[i + 1], pts[min(i + 2, n - 1)]
            c1 = (p1[0] + (p2[0] - p0[0]) / 6.0 * tension, p1[1] + (p2[1] - p0[1]) / 6.0 * tension)
            c2 = (p2[0] - (p3[0] - p1[0]) / 6.0 * tension, p2[1] - (p3[1] - p1[1]) / 6.0 * tension)
            p.curveTo(c1[0], c1[1], c2[0], c2[1], p2[0], p2[1])
        return p

    def add_smooth(self, p, pts, tension=1.0):
        """Append Catmull-Rom segments through pts (open) to an existing path positioned at pts[0]."""
        n = len(pts)
        for i in range(n - 1):
            p0, p1, p2, p3 = pts[max(i - 1, 0)], pts[i], pts[i + 1], pts[min(i + 2, n - 1)]
            c1 = (p1[0] + (p2[0] - p0[0]) / 6.0 * tension, p1[1] + (p2[1] - p0[1]) / 6.0 * tension)
            c2 = (p2[0] - (p3[0] - p1[0]) / 6.0 * tension, p2[1] - (p3[1] - p1[1]) / 6.0 * tension)
            p.curveTo(c1[0], c1[1], c2[0], c2[1], p2[0], p2[1])
        return p

    def arc(self, cx, cy, rx, ry, a0, a1, p=None, move=True):
        segs = bezier_arc(cx, cy, rx, ry, a0, a1)
        if p is None:
            p = self.path()
        if move:
            p.moveTo(*segs[0][0])
        for s in segs:
            p.curveTo(s[1][0], s[1][1], s[2][0], s[2][1], s[3][0], s[3][1])
        return p

    def circle(self, cx, cy, r, role=None, line=None, lw=1.0, stroke=True):
        p = self.arc(cx, cy, r, r, 0, 360)
        self.draw(p, role, stroke=stroke, line=line, lw=lw)

    def ellipse(self, cx, cy, rx, ry, role=None, ang=0, line=None, lw=1.0, stroke=True):
        self.c.saveState()
        self.c.translate(cx, cy)
        self.c.rotate(ang)
        p = self.arc(0, 0, rx, ry, 0, 360)
        self.draw(p, role, stroke=stroke, line=line, lw=lw)
        self.c.restoreState()

    def dot(self, cx, cy, r, role="accent"):
        """Small filled dot without outline (reads as paint mark)."""
        c = self.c
        f = self.fill_for(role)
        if self.mode == "line":
            f = self.line
        c.setFillColor(f)
        p = self.arc(cx, cy, r, r, 0, 360)
        p.close()
        c.drawPath(p, stroke=0, fill=1)

    def line_to(self, x0, y0, x1, y1, line=None, lw=1.0):
        p = self.path()
        p.moveTo(x0, y0)
        p.lineTo(x1, y1)
        self.stroke(p, line, lw)

    def curve(self, pts, line=None, lw=1.0, tension=1.0):
        """Open smooth curve through points."""
        p = self.smooth(pts, closed=False, tension=tension)
        self.stroke(p, line, lw)

    def bez(self, p0, c1, c2, p1, line=None, lw=1.0):
        p = self.path()
        p.moveTo(*p0)
        p.curveTo(c1[0], c1[1], c2[0], c2[1], p1[0], p1[1])
        self.stroke(p, line, lw)

    # ---- petal / leaf shapes
    def petal_path(self, cx, cy, ang, L, W, roundness=0.6, jit=0.06, tip_curl=0.0):
        """Closed petal: base at (cx,cy), tip at distance L along ang. roundness 0 = pointed, 1 = blunt."""
        j = jit
        r = max(0.0, min(1.0, roundness))
        w1 = W * (0.62 + self.j(j))
        w2 = W * (0.62 + self.j(j))
        ex = 0.55 + 0.45 * r
        tipw = 0.05 + 0.52 * r
        pts = [
            (0, 0),
            (L * (0.22 + self.j(j)), w1), (L * ex, W * tipw * (1 + self.j(j))), (L * (1 + self.j(j * 0.5)), L * tip_curl),
            (L * ex, -W * tipw * (1 + self.j(j))), (L * (0.22 + self.j(j)), -w2), (0, 0),
        ]
        out = []
        for (x, y) in pts:
            rx, ry = rot(x, y, ang)
            out.append((cx + rx, cy + ry))
        p = self.path()
        p.moveTo(*out[0])
        p.curveTo(out[1][0], out[1][1], out[2][0], out[2][1], out[3][0], out[3][1])
        p.curveTo(out[4][0], out[4][1], out[5][0], out[5][1], out[6][0], out[6][1])
        return p, out[3]

    def petal(self, cx, cy, ang, L, W, role="petal", roundness=0.6, jit=0.06, vein=False, line=None, lw=1.0, stroke=True):
        p, tip = self.petal_path(cx, cy, ang, L, W, roundness, jit)
        self.draw(p, role, stroke=stroke, line=line, lw=lw)
        if vein:
            vx, vy = rot(L * 0.82, 0, ang)
            self.line_to(cx, cy, cx + vx, cy + vy, line=line, lw=lw * 0.8)
        return tip

    def leaf(self, cx, cy, ang, L, W, role="leaf", roundness=0.25, veins=0, jit=0.06, line=None, lw=1.0):
        p, tip = self.petal_path(cx, cy, ang, L, W, roundness, jit)
        self.draw(p, role, line=line, lw=lw)
        vx, vy = rot(L * 0.85, 0, ang)
        self.line_to(cx, cy, cx + vx, cy + vy, line=line, lw=lw * 0.75)
        for i in range(veins):
            t = (i + 1) / (veins + 1.0)
            bx, by = rot(L * t, 0, ang)
            for s in (1, -1):
                ex_, ey_ = rot(L * (t + 0.14), s * W * 0.33 * (1 - t * 0.5), ang)
                self.line_to(cx + bx, cy + by, cx + ex_, cy + ey_, line=line, lw=lw * 0.6)
        return tip

    def ring(self, n, r_base, L, W, ang0=0, role="petal", roundness=0.6, jit=0.08, squash=1.0, backshort=0.0, vein=False):
        """Ring of petals radiating from radius r_base."""
        for i in range(n):
            a = ang0 + 360.0 * i / n + self.j(360.0 / n * 0.12)
            Lf = 1.0 + self.j(0.08)
            if backshort:
                Lf *= 1.0 - backshort * max(0.0, math.sin(math.radians(a)))
            bx, by = rot(r_base, 0, a)
            self.c.saveState()
            self.c.scale(1, squash)
            self.petal(bx, by / squash, a, L * Lf, W * (1 + self.j(0.08)), role=role, roundness=roundness, jit=jit, vein=vein)
            self.c.restoreState()

    # ---- placement
    def place(self, fn, x, y, size, rotd=0, flip=False, mode=None, **kw):
        c = self.c
        c.saveState()
        c.translate(x, y)
        if rotd:
            c.rotate(rotd)
        if flip:
            c.scale(-1, 1)
        c.scale(size, size)
        old = self.scale
        oldmode = self.mode
        if mode:
            self.mode = mode
        self.scale = old * size
        c.setLineWidth(self.lw / self.scale)
        fn(self, **kw)
        self.scale = old
        self.mode = oldmode
        c.restoreState()


# ================================================================== FLOWERS

def camellia(pen, view="front"):
    """Rounded layered camellia. view: front | three-quarter | half"""
    if view == "three-quarter":
        pen.ring(7, 0.22, 0.78, 0.6, ang0=pen.u(0, 50), roundness=0.9, squash=0.72, backshort=0.3)
        pen.ring(6, 0.12, 0.55, 0.44, ang0=pen.u(0, 60), roundness=0.9, squash=0.72, backshort=0.3, role="petal2")
        pen.ring(4, 0.04, 0.32, 0.3, ang0=pen.u(0, 90), roundness=0.95, squash=0.72, role="petal")
        for i in range(3):
            pen.dot(pen.j(0.07), -0.02 + pen.j(0.05), 0.035, "center")
        return
    if view == "half":
        pen.ring(6, 0.2, 0.75, 0.6, ang0=pen.u(0, 60), roundness=0.9)
        pen.ring(5, 0.08, 0.45, 0.42, ang0=pen.u(0, 60), roundness=0.95, role="petal2")
        # a small closed inner cup
        pen.petal(0, -0.05, 90, 0.36, 0.34, role="petal", roundness=0.95)
        pen.petal(-0.04, -0.05, 110, 0.3, 0.26, role="petal2", roundness=0.95)
        pen.petal(0.05, -0.05, 70, 0.3, 0.26, role="petal2", roundness=0.95)
        return
    pen.ring(7, 0.24, 0.78, 0.62, ang0=pen.u(0, 50), roundness=0.92)
    pen.ring(6, 0.14, 0.54, 0.46, ang0=pen.u(0, 60), roundness=0.92, role="petal2")
    pen.ring(5, 0.05, 0.33, 0.3, ang0=pen.u(0, 70), roundness=0.95, role="petal")
    for i in range(5):
        a = pen.u(0, 360)
        r = pen.u(0.02, 0.08)
        pen.dot(r * math.cos(math.radians(a)), r * math.sin(math.radians(a)), 0.03, "center")


def bud(pen, role="petal"):
    """Closed flower bud with two sepals and a short stem."""
    pen.line_to(0, -0.45, 0.03, -1.0, lw=0.9)
    pen.petal(0, -0.45, 90, 1.0, 0.55, role=role, roundness=0.75)
    pen.petal(0, -0.45, 90 + 28, 0.72, 0.3, role="leaf", roundness=0.2)
    pen.petal(0, -0.45, 90 - 28, 0.72, 0.3, role="leaf", roundness=0.2)
    pen.petal(0, -0.45, 90, 0.55, 0.22, role="leaf", roundness=0.2)


def dahlia(pen, view="front"):
    squash = 0.7 if view == "side" else 1.0
    back = 0.35 if view == "side" else 0.0
    if view == "bud":
        pen.petal(0, -0.5, 90, 1.0, 0.6, role="petal", roundness=0.3)
        pen.petal(-0.08, -0.5, 100, 0.8, 0.35, role="petal2", roundness=0.3)
        pen.petal(0.08, -0.5, 80, 0.8, 0.35, role="petal2", roundness=0.3)
        pen.petal(0, -0.5, 90 + 35, 0.55, 0.25, role="leaf", roundness=0.2)
        pen.petal(0, -0.5, 90 - 35, 0.55, 0.25, role="leaf", roundness=0.2)
        return
    pen.ring(18, 0.3, 0.7, 0.2, ang0=pen.u(0, 20), roundness=0.15, squash=squash, backshort=back)
    pen.ring(14, 0.18, 0.62, 0.2, ang0=pen.u(0, 25), roundness=0.15, role="petal2", squash=squash, backshort=back)
    pen.ring(10, 0.09, 0.46, 0.19, ang0=pen.u(0, 36), roundness=0.15, squash=squash, backshort=back)
    pen.ring(7, 0.03, 0.28, 0.16, ang0=pen.u(0, 50), roundness=0.2, role="petal2", squash=squash)
    pen.circle(0, 0, 0.07, role="center", lw=0.7)


def chrysanthemum(pen):
    pen.ring(22, 0.22, 0.78, 0.12, ang0=pen.u(0, 16), roundness=0.1, jit=0.1)
    pen.ring(16, 0.1, 0.55, 0.12, ang0=pen.u(0, 22), roundness=0.1, jit=0.1, role="petal2")
    pen.ring(10, 0.03, 0.3, 0.11, ang0=pen.u(0, 36), roundness=0.2, jit=0.1)
    pen.dot(0, 0, 0.06, "center")


def peony(pen):
    pen.ring(7, 0.2, 0.82, 0.74, ang0=pen.u(0, 50), roundness=0.8, jit=0.16)
    pen.ring(6, 0.1, 0.6, 0.55, ang0=pen.u(0, 60), roundness=0.8, jit=0.18, role="petal2")
    pen.ring(5, 0.04, 0.38, 0.38, ang0=pen.u(0, 70), roundness=0.85, jit=0.18)
    for i in range(6):
        a = pen.u(0, 360)
        pen.line_to(0, 0, 0.12 * math.cos(math.radians(a)), 0.12 * math.sin(math.radians(a)), lw=0.7)
        pen.dot(0.14 * math.cos(math.radians(a)), 0.14 * math.sin(math.radians(a)), 0.025, "center")


def poinsettia(pen):
    pen.ring(6, 0.12, 1.0, 0.4, ang0=pen.u(0, 60), roundness=0.08, vein=True)
    pen.ring(5, 0.06, 0.68, 0.34, ang0=pen.u(0, 72), roundness=0.08, role="petal2", vein=True)
    for i in range(5):
        a = math.radians(i * 72 + pen.j(10))
        pen.circle(0.07 * math.cos(a), 0.07 * math.sin(a), 0.045, role="center", lw=0.6)


def five_petal(pen, n=5):
    pen.ring(n, 0.1, 0.62, 0.52, ang0=pen.u(0, 72), roundness=0.9)
    pen.circle(0, 0, 0.12, role="center", lw=0.7)


def hellebore(pen):
    pen.ring(5, 0.12, 0.85, 0.66, ang0=pen.u(0, 72), roundness=0.55, vein=False)
    pen.circle(0, 0, 0.14, role="center", lw=0.7)
    for i in range(8):
        a = math.radians(i * 45 + pen.j(8))
        pen.dot(0.22 * math.cos(a), 0.22 * math.sin(a), 0.028, "leaf")


def rose_small(pen):
    pen.ring(5, 0.18, 0.7, 0.6, ang0=pen.u(0, 72), roundness=0.9)
    pen.circle(0, 0, 0.3, role="petal2")
    # spiral
    pts = []
    for i in range(14):
        t = i / 13.0
        r = 0.27 * (1 - t) + 0.02
        a = t * 3.2 * math.pi
        pts.append((r * math.cos(a), r * math.sin(a)))
    pen.curve(pts, lw=0.75)


def starflower(pen):
    pen.ring(5, 0.08, 0.62, 0.5, ang0=pen.u(0, 72), roundness=0.85)
    star8(pen, size=0.22, role="center")


def water_lily(pen, view="top", simple=False):
    if simple:
        pen.ring(7, 0.2, 0.75, 0.3, ang0=pen.u(0, 40), roundness=0.1, role="petal")
        pen.ring(5, 0.05, 0.45, 0.26, ang0=pen.u(0, 60), roundness=0.1, role="petal2")
        pen.dot(0, 0, 0.07, "center")
        return
    if view == "side":
        # floating pad under a side-facing cup of pointed petals
        pen.ellipse(0, -0.55, 0.9, 0.22, role="leaf")
        for i, (a, L) in enumerate([(165, 0.8), (140, 0.95), (112, 1.0), (84, 1.0), (56, 0.95), (28, 0.8)]):
            pen.petal(0, -0.5, a, L, 0.3, role="petal" if i % 2 else "petal2", roundness=0.1)
        pen.petal(0, -0.5, 96, 0.72, 0.3, role="petal", roundness=0.1)
        return
    pen.ring(9, 0.3, 0.7, 0.26, ang0=pen.u(0, 40), roundness=0.08, role="petal")
    pen.ring(8, 0.15, 0.55, 0.25, ang0=pen.u(0, 45), roundness=0.08, role="petal2")
    pen.ring(6, 0.05, 0.36, 0.22, ang0=pen.u(0, 60), roundness=0.1, role="petal")
    for i in range(6):
        a = math.radians(i * 60 + pen.j(10))
        pen.dot(0.07 * math.cos(a), 0.07 * math.sin(a), 0.03, "center")


def bell_flower(pen):
    """Hanging bell-shaped flower on a curved stem."""
    pen.bez((-0.1, 1.0), (0.0, 0.7), (0.3, 0.7), (0.3, 0.45), lw=0.9)
    pen.c.saveState()
    pen.c.translate(0.3, 0.05)
    pen.c.rotate(-8)
    pen.c.scale(0.42, 0.42)
    pen.scale *= 0.42
    pen.c.setLineWidth(pen.lw / pen.scale)
    bell_cup(pen)
    pen.line_to(0, 0.4, 0, -0.45, lw=0.6)
    pen.dot(0, -0.55, 0.08, "center")
    pen.scale /= 0.42
    pen.c.restoreState()
    pen.leaf(-0.1, 1.0, -125, 0.5, 0.2, roundness=0.2)
    pen.leaf(0.05, 0.75, -40, 0.4, 0.16, roundness=0.2)


def wildflower(pen, kind="daisy"):
    """Single fine flowering stem."""
    pen.bez((0, -1.0), (0.05, -0.5), (-0.08, -0.1), (0, 0.3), lw=0.9)
    if kind == "spike":
        for i in range(7):
            t = 0.3 + i * 0.1
            s = 1 if i % 2 else -1
            pen.ellipse(s * 0.07, t, 0.07, 0.045, role="petal", ang=s * 30, lw=0.8)
        pen.ellipse(0, 1.02, 0.05, 0.07, role="petal2", lw=0.8)
    elif kind == "bell":
        for i, t in enumerate([0.35, 0.6, 0.85]):
            s = 1 if i % 2 else -1
            pen.c.saveState()
            pen.c.translate(s * 0.05, t)
            pen.c.scale(0.28, 0.28)
            pen.c.rotate(s * 25)
            pen.scale *= 0.28
            pen.c.setLineWidth(pen.lw / pen.scale)
            bell_cup(pen)
            pen.scale /= 0.28
            pen.c.restoreState()
    else:
        pen.c.saveState()
        pen.c.translate(0, 0.55)
        pen.c.scale(0.45, 0.45)
        pen.scale *= 0.45
        pen.c.setLineWidth(pen.lw / pen.scale)
        pen.ring(10, 0.12, 0.85, 0.3, ang0=pen.u(0, 36), roundness=0.5)
        pen.circle(0, 0, 0.2, role="center", lw=0.7)
        pen.scale /= 0.45
        pen.c.restoreState()
    pen.leaf(0.0, -0.6, 30, 0.42, 0.14, roundness=0.15)
    pen.leaf(-0.02, -0.3, 150, 0.36, 0.13, roundness=0.15)


def bell_cup(pen):
    p = pen.path()
    p.moveTo(-0.2, 0.9)
    p.curveTo(-0.3, 0.3, -0.75, 0.1, -0.85, -0.5)
    p.curveTo(-0.55, -0.65, -0.3, -0.5, 0, -0.65)
    p.curveTo(0.3, -0.5, 0.55, -0.65, 0.85, -0.5)
    p.curveTo(0.75, 0.1, 0.3, 0.3, 0.2, 0.9)
    pen.draw(p, "petal")


# ================================================================== FOLIAGE

def leaf_simple(pen, kind="oval", veins=2):
    rnd = {"oval": 0.55, "pointed": 0.15, "narrow": 0.1}[kind]
    W = {"oval": 0.62, "pointed": 0.5, "narrow": 0.3}[kind]
    pen.leaf(-0.5, 0, 0, 1.0, W, roundness=rnd, veins=veins)


def leaf_pair(pen):
    pen.bez((-0.6, -0.45), (-0.3, -0.2), (0.1, 0.1), (0.55, 0.55), lw=0.85)
    pen.leaf(-0.2, -0.12, 120, 0.55, 0.26, roundness=0.3, veins=1)
    pen.leaf(0.12, 0.18, -40, 0.55, 0.26, roundness=0.3, veins=1)
    pen.leaf(0.4, 0.4, 70, 0.4, 0.2, roundness=0.3)


def oak_leaf(pen):
    pts = []
    n = 40
    lobes = 4.0
    for side in (1, -1):
        rng_ = range(n + 1) if side == 1 else range(n, -1, -1)
        for i in rng_:
            t = i / float(n)
            x = -0.95 + 1.9 * t
            w = 0.42 * math.sin(math.pi * (0.05 + 0.9 * t)) ** 0.5
            ph = 0.3 if side == 1 else 1.4
            lobe = 0.45 + 0.55 * abs(math.sin(lobes * math.pi * t + ph)) ** 1.3
            pts.append((x, side * w * lobe * (1 + pen.j(0.04))))
    p = pen.smooth(pts, closed=True, tension=0.6)
    pen.draw(p, "leaf")
    pen.line_to(-1.1, 0, 0.9, 0, lw=0.8)
    for t in (0.22, 0.45, 0.68):
        x = -0.95 + 1.9 * t
        pen.line_to(x, 0, x + 0.16, 0.26, lw=0.5)
        pen.line_to(x + 0.12, 0, x + 0.28, -0.26, lw=0.5)


def ginkgo(pen):
    cx, cy = 0.0, -0.55
    pen.line_to(0, -1.05, 0, cy, lw=0.9)
    pts = [(cx, cy)]
    n = 22
    for i in range(n + 1):
        a = 18 + (180 - 36) * i / float(n)
        d = abs(a - 90)
        r = 1.0 - 0.22 * math.exp(-(d / 7.0) ** 2) + 0.03 * math.sin(a * 0.9) + pen.j(0.015)
        pts.append((cx + r * math.cos(math.radians(a)), cy + r * 0.95 * math.sin(math.radians(a))))
    pts.append((cx, cy))
    p = pen.path()
    p.moveTo(*pts[0])
    p.lineTo(*pts[1])
    pen.add_smooth(p, pts[1:-1], tension=0.7)
    p.lineTo(*pts[-1])
    pen.draw(p, "leaf")
    for a in (30, 50, 70, 110, 130, 150):
        ex = math.cos(math.radians(a))
        ey = math.sin(math.radians(a)) * 0.95
        pen.line_to(cx, cy, cx + ex * 0.82, cy + ey * 0.82, lw=0.45)


def holly_leaf(pen, berries=True):
    pts = []
    n = 7
    for i in range(n):
        t = i / float(n - 1)
        x = -1.0 + 2.0 * t
        w = 0.5 * math.sin(math.pi * (0.1 + 0.8 * t))
        spike = 1.0 if i % 2 else 0.45
        pts.append((x, w * spike))
    for i in range(n - 1, -1, -1):
        t = i / float(n - 1)
        x = -1.0 + 2.0 * t
        w = 0.5 * math.sin(math.pi * (0.1 + 0.8 * t))
        spike = 1.0 if i % 2 else 0.45
        pts.append((x, -w * spike))
    p = pen.smooth(pts, closed=True, tension=0.5)
    pen.draw(p, "leaf")
    pen.line_to(-0.95, 0, 0.85, 0, lw=0.75)
    if berries:
        for (bx, by) in ((-0.95, 0.2), (-1.12, 0.05), (-1.0, -0.15)):
            pen.circle(bx, by, 0.12, role="berry", lw=0.7)


def pine_sprig(pen, cone=False, dense=False):
    pen.bez((-1.0, -0.1), (-0.4, -0.05), (0.3, 0.05), (1.0, 0.1), lw=0.9)
    step = 0.12 if dense else 0.17
    x = -0.85
    while x < 0.95:
        y = -0.1 + 0.2 * (x + 1.0) / 2.0
        for s in (1, -1):
            a = s * (38 + pen.j(8))
            L = 0.3 + pen.j(0.05)
            ex, ey = rot(L, 0, a)
            pen.line_to(x, y, x + ex * 0.55, y + ey, lw=0.6)
        x += step
    if cone:
        pen.place(pinecone, 0.55, -0.42, 0.28, rotd=-30)


def fir_tip(pen):
    pine_sprig(pen, dense=True)


def willow_branch(pen):
    pts = [(-1.0, 0.6), (-0.5, 0.45), (-0.05, 0.1), (0.3, -0.35), (0.5, -0.9)]
    pen.curve(pts, lw=0.85)
    for i, t in enumerate([0.15, 0.3, 0.45, 0.6, 0.75, 0.9]):
        # approximate along polyline
        idx = min(int(t * (len(pts) - 1)), len(pts) - 2)
        f = t * (len(pts) - 1) - idx
        x = pts[idx][0] + (pts[idx + 1][0] - pts[idx][0]) * f
        y = pts[idx][1] + (pts[idx + 1][1] - pts[idx][1]) * f
        s = 1 if i % 2 else -1
        pen.leaf(x, y, -70 + s * 35 + pen.j(10), 0.34, 0.1, roundness=0.1)


def vine_s(pen, leaves=5, tendril=True):
    p = pen.path()
    p.moveTo(-1.0, -0.6)
    p.curveTo(-0.6, -0.1, -0.3, 0.6, 0.0, 0.0)
    p.curveTo(0.3, -0.6, 0.6, 0.1, 1.0, 0.6)
    pen.stroke(p, lw=0.9)
    for i in range(leaves):
        t = (i + 0.5) / leaves
        # evaluate the two-segment bezier
        if t < 0.5:
            u = t * 2
            P = [(-1.0, -0.6), (-0.6, -0.1), (-0.3, 0.6), (0.0, 0.0)]
        else:
            u = (t - 0.5) * 2
            P = [(0.0, 0.0), (0.3, -0.6), (0.6, 0.1), (1.0, 0.6)]
        x = sum(c * (1 - u) ** (3 - k) * u ** k * (1, 3, 3, 1)[k] for k, (c, _) in enumerate(P))
        y = sum(c * (1 - u) ** (3 - k) * u ** k * (1, 3, 3, 1)[k] for k, (_, c) in enumerate(P))
        s = 1 if i % 2 else -1
        pen.leaf(x, y, 60 * s + 20 + pen.j(15), 0.36, 0.17, roundness=0.25, veins=0)
    if tendril:
        pts = []
        for i in range(10):
            u = i / 9.0
            r = 0.16 * (1 - u) + 0.02
            a = u * 2.6 * math.pi
            pts.append((1.0 + r * math.cos(a) - 0.1, 0.6 + r * math.sin(a) + 0.1))
        pen.curve(pts, lw=0.6)


def bare_twig(pen, buds=False):
    pen.bez((-1.0, -0.5), (-0.4, -0.3), (0.0, 0.1), (0.55, 0.55), lw=0.9)
    pen.bez((-0.35, -0.2), (-0.2, 0.1), (-0.1, 0.3), (-0.05, 0.6), lw=0.7)
    pen.bez((0.1, 0.2), (0.3, 0.2), (0.5, 0.15), (0.75, 0.05), lw=0.7)
    pen.bez((-0.7, -0.4), (-0.6, -0.5), (-0.55, -0.65), (-0.5, -0.8), lw=0.6)
    if buds:
        for (x, y, a) in ((-0.05, 0.6, 85), (0.55, 0.55, 45), (0.75, 0.05, -10)):
            pen.petal(x, y, a, 0.17, 0.1, role="petal", roundness=0.7)


def reeds(pen):
    for i, (x, h, lean) in enumerate([(-0.5, 0.9, -8), (-0.2, 1.1, -3), (0.1, 0.8, 4), (0.4, 1.0, 9), (0.65, 0.6, 14)]):
        ex, ey = rot(0, h, lean)
        pen.bez((x, -0.6), (x, -0.2), (x + ex * 0.5, -0.6 + ey * 0.5), (x + ex, -0.6 + ey), lw=0.8)
        if i in (1, 3):
            pen.ellipse(x + ex * 0.9, -0.6 + ey * 0.88, 0.045, 0.16, role="leaf", ang=lean, lw=0.7)


def lily_pad(pen):
    pts = []
    n = 26
    for i in range(n + 1):
        a = 28 + (360 - 56) * i / float(n)
        r = 1.0 + pen.j(0.035)
        pts.append((r * math.cos(math.radians(a)), r * math.sin(math.radians(a)) * 0.82))
    p = pen.path()
    p.moveTo(0.0, 0.0)
    p.lineTo(*pts[0])
    pen.add_smooth(p, pts, tension=0.8)
    p.lineTo(0.0, 0.0)
    pen.draw(p, "leaf")
    for a in (70, 110, 150, 200, 250, 290):
        pen.line_to(0, 0, 0.72 * math.cos(math.radians(a)), 0.6 * math.sin(math.radians(a)), lw=0.45)


def grass_tuft(pen):
    for a in (-30, -15, 0, 12, 28):
        ex, ey = rot(0, 1.0 + pen.j(0.2), a)
        pen.bez((0, -0.8), (0, -0.3), (ex * 0.2, -0.8 + ey * 0.6), (ex, -0.8 + ey), lw=0.7)


# ================================================================== FRUIT & OBJECTS

def berry_cluster(pen, n=3, stems=True):
    pos = [(-0.45, 0.3), (0.05, 0.55), (0.45, 0.2), (0.0, 0.05), (-0.2, 0.75)][:n]
    if stems:
        for (x, y) in pos:
            pen.bez((0, -0.9), (0, -0.4), (x * 0.5, y * 0.3), (x, y), lw=0.7)
    for (x, y) in pos:
        pen.circle(x, y, 0.2 + pen.j(0.03), role="berry", lw=0.8)


def acorn(pen):
    pen.petal(0, 0.25, -90, 0.95, 0.72, role="object", roundness=0.95)
    p = pen.path()
    p.moveTo(-0.45, 0.2)
    p.curveTo(-0.5, 0.55, 0.5, 0.55, 0.45, 0.2)
    p.lineTo(0.42, 0.1)
    p.curveTo(0.2, 0.0, -0.2, 0.0, -0.42, 0.1)
    pen.draw(p, "leaf")
    for x in (-0.3, -0.1, 0.1, 0.3):
        pen.line_to(x, 0.42, x + 0.08, 0.15, lw=0.5)
    pen.line_to(0, 0.5, 0.08, 0.72, lw=0.8)


def pinecone(pen):
    p = pen.path()
    p.moveTo(0, 1.0)
    p.curveTo(0.55, 0.9, 0.62, 0.2, 0.3, -0.5)
    p.curveTo(0.2, -0.8, -0.2, -0.8, -0.3, -0.5)
    p.curveTo(-0.62, 0.2, -0.55, 0.9, 0, 1.0)
    pen.draw(p, "object")
    rows = [(0.65, 0.3, 2), (0.35, 0.45, 3), (0.05, 0.5, 3), (-0.25, 0.4, 3), (-0.52, 0.25, 2)]
    for (y, half, n) in rows:
        for i in range(n):
            x = -half + (2 * half) * (i + 0.5) / n
            p = pen.arc(x, y - 0.1, half / n * 0.9, 0.14, 200, 340)
            pen.stroke(p, lw=0.6)


def strawberry(pen, seeds=6):
    pts = [(0, 0.55), (0.5, 0.42), (0.58, -0.05), (0.2, -0.7), (0, -0.85), (-0.2, -0.7), (-0.58, -0.05), (-0.5, 0.42)]
    p = pen.smooth(pts, closed=True, tension=0.9)
    pen.draw(p, "berry")
    for i in range(seeds):
        a = pen.u(0, 360)
        r = pen.u(0.15, 0.42)
        pen.dot(r * math.cos(math.radians(a)) * 0.9, r * math.sin(math.radians(a)) * 0.9 - 0.1, 0.03, "ground" if pen.mode == "paint" else "leaf")
    for a in (150, 110, 70, 30):
        pen.petal(0, 0.45, a, 0.38, 0.16, role="leaf", roundness=0.2)
    pen.line_to(0, 0.5, 0.05, 0.85, lw=0.8)


def apple(pen):
    pts = [(0, 0.55), (0.5, 0.7), (0.78, 0.2), (0.6, -0.5), (0.2, -0.72), (0, -0.62), (-0.2, -0.72), (-0.6, -0.5), (-0.78, 0.2), (-0.5, 0.7)]
    p = pen.smooth(pts, closed=True, tension=0.9)
    pen.draw(p, "berry")
    pen.line_to(0, 0.55, 0.06, 0.95, lw=0.8)
    pen.leaf(0.05, 0.75, 20, 0.5, 0.22, roundness=0.3)


def seed_pod(pen, opened=False):
    pen.line_to(0, -1.0, 0, -0.55, lw=0.8)
    pen.petal(0, -0.55, 90, 1.1, 0.5, role="object", roundness=0.7)
    if opened:
        pen.line_to(0, -0.45, 0, 0.45, lw=0.6)
        for y in (-0.2, 0.05, 0.3):
            pen.dot(-0.08, y, 0.035, "berry")
            pen.dot(0.08, y - 0.1, 0.035, "berry")
    else:
        pen.line_to(-0.02, 0.5, 0.04, 0.72, lw=0.6)


# ================================================================== CREATURES

def bird(pen, pose="perched", tiny=False):
    flip_head = pose == "turned"
    # tail
    pen.petal(-0.5, -0.05, 200, 0.6, 0.22, role="bird2", roundness=0.2)
    # body
    pen.ellipse(0, 0, 0.6, 0.36, role="bird", ang=12)
    # head
    hx = 0.52 if not flip_head else 0.4
    pen.circle(hx, 0.3, 0.24, role="bird")
    # beak
    p = pen.path()
    if not flip_head:
        p.moveTo(0.72, 0.33)
        p.lineTo(0.95, 0.27)
        p.lineTo(0.72, 0.2)
    else:
        p.moveTo(0.22, 0.42)
        p.lineTo(0.0, 0.4)
        p.lineTo(0.22, 0.3)
    pen.draw(p, "center")
    # wing
    pen.petal(0.12, 0.05, 195, 0.6, 0.32, role="bird2", roundness=0.5)
    if not tiny:
        pen.dot(hx + (0.08 if not flip_head else -0.06), 0.36, 0.035, "line")
        pen.line_to(0.05, -0.34, 0.1, -0.62, lw=0.6)
        pen.line_to(0.2, -0.32, 0.28, -0.6, lw=0.6)
        pen.line_to(-0.1, -0.62, 0.3, -0.6, lw=0.6)


def bird_tiny(pen):
    pen.petal(-0.4, -0.02, 195, 0.5, 0.2, role="bird2", roundness=0.2)
    pen.ellipse(0, 0, 0.5, 0.3, role="bird", ang=10)
    pen.circle(0.42, 0.26, 0.2, role="bird")
    p = pen.path()
    p.moveTo(0.58, 0.28)
    p.lineTo(0.78, 0.24)
    p.lineTo(0.58, 0.18)
    pen.draw(p, "center")


def butterfly(pen, pose="open"):
    if pose == "side":
        pen.petal(0, -0.2, 70, 1.0, 0.7, role="wing", roundness=0.85)
        pen.petal(0, -0.2, 120, 0.75, 0.55, role="wing2", roundness=0.85)
        pen.ellipse(0, -0.25, 0.08, 0.35, role="bird2", ang=-10)
        pen.bez((0, 0.05), (0.1, 0.35), (0.2, 0.45), (0.35, 0.5), lw=0.5)
        return
    for s in (1, -1):
        pen.petal(0, 0.1, 90 - s * 55, 0.95, 0.78, role="wing", roundness=0.85)
        pen.petal(0, -0.05, -90 + s * 40, 0.7, 0.55, role="wing2", roundness=0.85)
        pen.dot(s * 0.45, 0.42, 0.06, "center")
    pen.ellipse(0, 0, 0.09, 0.45, role="bird2")
    pen.circle(0, 0.5, 0.09, role="bird2")
    for s in (1, -1):
        pen.bez((0, 0.55), (s * 0.05, 0.75), (s * 0.15, 0.85), (s * 0.28, 0.92), lw=0.5)


def dragonfly(pen):
    pen.ellipse(0, -0.35, 0.07, 0.65, role="bird2")
    pen.circle(0, 0.35, 0.1, role="bird")
    for s in (1, -1):
        pen.petal(0, 0.2, 90 - s * 70, 1.0, 0.24, role="wing", roundness=0.8)
        pen.petal(0, 0.05, 90 - s * 100, 0.85, 0.22, role="wing", roundness=0.8)
    for y in (-0.3, -0.5, -0.7, -0.85):
        pen.line_to(-0.06, y, 0.06, y, lw=0.45)


# ================================================================== SACRED & STORY

def star8(pen, size=1.0, role="object", longer=1.15):
    pts = []
    for i in range(16):
        a = math.radians(90 + i * 22.5)
        r = (1.0 if i % 2 == 0 else 0.38) * size
        if i == 0:
            r *= longer
        if i == 8:
            r *= 1.05
        pts.append((r * math.cos(a), r * math.sin(a)))
    p = pen.path()
    p.moveTo(*pts[0])
    for q in pts[1:]:
        p.lineTo(*q)
    pen.draw(p, role, lw=0.8)


def star4(pen, size=1.0, role="object"):
    pts = []
    for i in range(8):
        a = math.radians(90 + i * 45)
        r = (1.0 if i % 2 == 0 else 0.3) * size
        pts.append((r * math.cos(a), r * math.sin(a)))
    p = pen.path()
    p.moveTo(*pts[0])
    for q in pts[1:]:
        p.lineTo(*q)
    pen.draw(p, role, lw=0.7)


def sparkle(pen, size=1.0):
    pen.line_to(-size, 0, size, 0, lw=0.6)
    pen.line_to(0, -size, 0, size, lw=0.6)


def bell(pen, bow=True, sprig=True):
    p = pen.path()
    p.moveTo(-0.14, 0.78)
    p.curveTo(-0.3, 0.75, -0.3, 0.2, -0.42, -0.1)
    p.curveTo(-0.5, -0.3, -0.7, -0.4, -0.72, -0.5)
    p.lineTo(0.72, -0.5)
    p.curveTo(0.7, -0.4, 0.5, -0.3, 0.42, -0.1)
    p.curveTo(0.3, 0.2, 0.3, 0.75, 0.14, 0.78)
    pen.draw(p, "object")
    pen.circle(0, 0.86, 0.1, role="object", lw=0.7)
    pen.circle(0, -0.58, 0.1, role="object2", lw=0.7)
    pen.line_to(-0.45, -0.3, 0.45, -0.3, lw=0.5)
    pen.line_to(-0.36, -0.18, 0.36, -0.18, lw=0.5)
    if bow:
        for s in (1, -1):
            pen.petal(0, 0.95, 90 + s * 70, 0.32, 0.22, role="berry", roundness=0.9)
            pen.petal(0, 0.9, -90 + s * 20, 0.3, 0.1, role="berry", roundness=0.2)
    if sprig:
        pen.place(fir_tip, -0.75, 0.55, 0.38, rotd=35)


def candle(pen, holder=True, arcs=True):
    p = pen.path()
    p.moveTo(-0.11, -0.5)
    p.lineTo(0.11, -0.5)
    p.lineTo(0.09, 0.45)
    p.lineTo(-0.09, 0.45)
    pen.draw(p, "cream")
    pen.line_to(0, 0.45, 0, 0.55, lw=0.7)
    pen.petal(0, 0.53, 90, 0.36, 0.2, role="object", roundness=0.4)
    if holder:
        p = pen.path()
        p.moveTo(-0.18, -0.5)
        p.lineTo(0.18, -0.5)
        p.lineTo(0.14, -0.68)
        p.lineTo(-0.14, -0.68)
        pen.draw(p, "object2")
        pen.ellipse(0, -0.78, 0.36, 0.1, role="object2")
    if arcs:
        for s in (1, -1):
            pq = pen.arc(0, 0.62, 0.45 * s, 0.38, 20, 70)
            pen.stroke(pq, lw=0.5)


def trumpet(pen):
    p = pen.path()
    p.moveTo(-1.0, 0.06)
    p.curveTo(-0.3, 0.1, 0.3, 0.12, 0.55, 0.16)
    p.curveTo(0.75, 0.3, 0.9, 0.4, 1.0, 0.45)
    p.lineTo(1.0, -0.33)
    p.curveTo(0.9, -0.28, 0.75, -0.18, 0.55, -0.04)
    p.curveTo(0.3, 0.0, -0.3, -0.02, -1.0, -0.06)
    pen.draw(p, "object")
    pen.circle(-1.0, 0, 0.09, role="object", lw=0.7)
    pen.line_to(-0.4, 0.09, -0.4, -0.05, lw=0.5)
    pen.line_to(-0.2, 0.1, -0.2, -0.04, lw=0.5)


def angel_wing(pen, rows=3):
    """Feathered wing sweeping up to the right."""
    spine = [(-0.9, -0.5), (-0.5, 0.1), (0.1, 0.55), (0.95, 0.85)]
    for r in range(rows):
        n = 7 - r
        for i in range(n):
            t = (i + 0.5) / n
            idx = min(int(t * 3), 2)
            f = t * 3 - idx
            x = spine[idx][0] + (spine[idx + 1][0] - spine[idx][0]) * f
            y = spine[idx][1] + (spine[idx + 1][1] - spine[idx][1]) * f
            L = (0.55 - r * 0.12) * (0.65 + 0.5 * t)
            pen.petal(x - r * 0.12, y - r * 0.18 - 0.1, -70 + 35 * t + r * 6, L, 0.18, role="wing" if r % 2 == 0 else "wing2", roundness=0.75)
    pen.curve(spine, lw=0.8)


def angel(pen, trumpet_up=True):
    # wings behind
    pen.place(angel_wing, -0.35, 0.05, 0.55, rotd=20)
    pen.place(angel_wing, 0.15, 0.05, 0.4, rotd=35, flip=True)
    # gown
    p = pen.path()
    p.moveTo(-0.18, 0.5)
    p.curveTo(-0.3, 0.1, -0.5, -0.5, -0.45, -0.95)
    p.curveTo(-0.1, -1.02, 0.2, -1.02, 0.45, -0.95)
    p.curveTo(0.5, -0.5, 0.3, 0.1, 0.18, 0.5)
    pen.draw(p, "cream")
    pen.line_to(-0.1, -0.1, -0.18, -0.9, lw=0.5)
    pen.line_to(0.12, -0.1, 0.2, -0.9, lw=0.5)
    # head and hair
    pen.circle(0, 0.68, 0.19, role="cream")
    pq = pen.arc(0, 0.7, 0.2, 0.2, 10, 170)
    pen.stroke(pq, lw=0.8)
    # arm and trumpet
    pen.bez((0.1, 0.3), (0.3, 0.35), (0.45, 0.45), (0.6, 0.6), lw=0.7)
    pen.place(trumpet, 0.65, 0.68, 0.42, rotd=30)
    # halo ring (light)
    pq = pen.arc(0, 0.72, 0.28, 0.1, 0, 360)
    pen.stroke(pq, lw=0.5)


def manger_scene(pen):
    """Shelter arch, star, manger and two bowed figures."""
    # star + rays
    for a in range(0, 360, 30):
        ex, ey = rot(0, 0.5, a)
        sx, sy = rot(0, 0.28, a)
        pen.line_to(sx, 0.75 + sy, ex, 0.75 + ey, lw=0.45)
    pen.place(star8, 0, 0.75, 0.2)
    # shelter
    p = pen.path()
    p.moveTo(-0.95, -0.2)
    p.lineTo(-0.95, 0.1)
    p.curveTo(-0.8, 0.35, -0.5, 0.45, 0, 0.45)
    p.curveTo(0.5, 0.45, 0.8, 0.35, 0.95, 0.1)
    p.lineTo(0.95, -0.2)
    pen.stroke(p, lw=0.9)
    # manger
    p = pen.path()
    p.moveTo(-0.3, -0.55)
    p.lineTo(0.3, -0.55)
    p.lineTo(0.22, -0.78)
    p.lineTo(-0.22, -0.78)
    pen.draw(p, "object2")
    pen.line_to(-0.25, -0.78, 0.0, -1.0, lw=0.7)
    pen.line_to(0.25, -0.78, 0.0, -1.0, lw=0.7)
    pen.ellipse(0, -0.5, 0.22, 0.09, role="cream")
    # figures
    for s in (-1, 1):
        p = pen.path()
        p.moveTo(s * 0.4, -1.0)
        p.curveTo(s * 0.4, -0.5, s * 0.55, -0.3, s * 0.62, -0.1)
        p.curveTo(s * 0.75, -0.3, s * 0.85, -0.6, s * 0.8, -1.0)
        pen.draw(p, "cream")
        pen.circle(s * 0.55, -0.02, 0.12, role="cream")


def village(pen):
    """Cluster of simple stone houses with a dome, cypress and olive tree."""
    houses = [(-0.9, -0.55, 0.45, 0.45), (-0.45, -0.55, 0.5, 0.65), (0.05, -0.55, 0.45, 0.5), (0.5, -0.55, 0.4, 0.4)]
    for (x, y, w, h) in houses:
        p = pen.path()
        p.moveTo(x, y)
        p.lineTo(x + w, y)
        p.lineTo(x + w, y + h)
        p.lineTo(x, y + h)
        pen.draw(p, "cream")
    # dome on the second
    pq = pen.arc(-0.2, 0.1, 0.22, 0.2, 0, 180)
    pen.draw(pq, "object2", close=True)
    # windows
    for (x, y, lit) in ((-0.72, -0.3, 1), (-0.3, -0.25, 0), (-0.15, -0.4, 1), (0.25, -0.3, 1), (0.68, -0.35, 0)):
        p = pen.path()
        p.moveTo(x - 0.05, y - 0.08)
        p.lineTo(x - 0.05, y + 0.03)
        p.curveTo(x - 0.05, y + 0.1, x + 0.05, y + 0.1, x + 0.05, y + 0.03)
        p.lineTo(x + 0.05, y - 0.08)
        pen.draw(p, "object" if lit else "ground", lw=0.6)
    # cypress
    p = pen.path()
    p.moveTo(0.98, -0.55)
    p.curveTo(0.9, -0.2, 0.9, 0.3, 1.02, 0.75)
    p.curveTo(1.14, 0.3, 1.14, -0.2, 1.06, -0.55)
    pen.draw(p, "leaf")
    # olive tree
    pen.line_to(-1.1, -0.55, -1.1, -0.25, lw=0.8)
    pen.circle(-1.1, -0.08, 0.2, role="leaf2")
    pen.line_to(-1.2, -0.55, 1.1, -0.55, lw=0.7)
    pen.place(star8, 0.3, 0.85, 0.09)


def lantern(pen, hanging=True):
    if hanging:
        pen.line_to(0, 1.0, 0, 0.6, lw=0.6)
    pen.circle(0, 0.55, 0.06, lw=0.6)
    p = pen.path()
    p.moveTo(-0.22, 0.45)
    p.lineTo(0.22, 0.45)
    p.lineTo(0.3, 0.3)
    p.lineTo(0.3, -0.3)
    p.lineTo(0.22, -0.45)
    p.lineTo(-0.22, -0.45)
    p.lineTo(-0.3, -0.3)
    p.lineTo(-0.3, 0.3)
    pen.draw(p, "object2")
    pen.line_to(-0.1, 0.3, -0.1, -0.3, lw=0.45)
    pen.line_to(0.1, 0.3, 0.1, -0.3, lw=0.45)
    pen.petal(0, -0.22, 90, 0.3, 0.16, role="object", roundness=0.4)
    pen.line_to(-0.12, -0.55, 0.12, -0.55, lw=0.6)


def leaf_arch(pen, center="lantern"):
    """Open arch of small leaves with a hanging lantern or candle inside."""
    cx, cy, rx, ry = 0.0, -0.35, 0.9, 1.1
    p = pen.arc(cx, cy, rx, ry, 0, 180)
    pen.stroke(p, lw=0.8)
    n = 16
    for i in range(n + 1):
        a = 180.0 * i / n
        x = cx + rx * math.cos(math.radians(a))
        y = cy + ry * math.sin(math.radians(a))
        side = 1 if i % 2 else -1
        pen.leaf(x, y, a + 90 + side * 60 + pen.j(10), 0.28, 0.11, role="leaf" if i % 3 else "leaf2", roundness=0.2)
        if i % 4 == 2:
            pen.circle(x * 1.02, y + 0.03, 0.045, role="berry", lw=0.6)
    if center == "lantern":
        pen.place(lantern, 0, 0.22, 0.5)
    else:
        pen.place(candle, 0, -0.05, 0.55)
    for s in (1, -1):
        pen.bez((s * rx, cy), (s * (rx + 0.05), cy - 0.3), (s * (rx - 0.1), cy - 0.45), (s * (rx + 0.05), cy - 0.7), lw=0.6)
        pen.bez((s * rx, cy), (s * (rx - 0.08), cy - 0.25), (s * (rx - 0.2), cy - 0.35), (s * (rx - 0.15), cy - 0.6), lw=0.6)


def gifts(pen):
    # casket
    p = pen.path()
    p.moveTo(-1.0, -0.5)
    p.lineTo(-0.35, -0.5)
    p.lineTo(-0.35, -0.1)
    p.lineTo(-1.0, -0.1)
    pen.draw(p, "object")
    p = pen.path()
    p.moveTo(-1.03, -0.1)
    p.curveTo(-1.0, 0.12, -0.38, 0.12, -0.32, -0.1)
    pen.draw(p, "object2")
    pen.line_to(-0.9, -0.3, -0.45, -0.3, lw=0.5)
    # incense vessel with wisp
    p = pen.path()
    p.moveTo(-0.25, -0.5)
    p.curveTo(-0.35, -0.2, 0.0, 0.1, 0.0, 0.25)
    p.lineTo(0.3, 0.25)
    p.curveTo(0.3, 0.1, 0.65, -0.2, 0.55, -0.5)
    pen.draw(p, "object2")
    pen.ellipse(0.15, 0.27, 0.18, 0.05, role="object")
    pen.bez((0.15, 0.32), (0.05, 0.5), (0.35, 0.6), (0.18, 0.85), lw=0.5)
    # myrrh jar
    p = pen.path()
    p.moveTo(0.7, -0.5)
    p.curveTo(0.62, -0.2, 0.68, 0.05, 0.78, 0.12)
    p.lineTo(0.78, 0.3)
    p.lineTo(0.96, 0.3)
    p.lineTo(0.96, 0.12)
    p.curveTo(1.06, 0.05, 1.12, -0.2, 1.04, -0.5)
    pen.draw(p, "berry")
    pen.line_to(-1.1, -0.52, 1.1, -0.52, lw=0.5)
    pen.place(star8, 0.0, 0.75, 0.1)


def fountain(pen):
    pen.ellipse(0, -0.85, 0.95, 0.14, role="water")
    p = pen.path()
    p.moveTo(-0.16, -0.85)
    p.lineTo(-0.1, -0.35)
    p.lineTo(0.1, -0.35)
    p.lineTo(0.16, -0.85)
    pen.draw(p, "object2")
    pq = pen.arc(0, -0.2, 0.6, 0.22, 180, 360)
    pen.draw(pq, "object2", close=True)
    pen.ellipse(0, -0.2, 0.6, 0.1, role="water")
    pen.line_to(0, -0.2, 0, 0.15, lw=0.8)
    pq = pen.arc(0, 0.2, 0.28, 0.12, 180, 360)
    pen.draw(pq, "object2", close=True)
    pen.ellipse(0, 0.2, 0.28, 0.06, role="water")
    pen.line_to(0, 0.2, 0, 0.5, lw=0.8)
    for s in (1, -1):
        pen.bez((0, 0.55), (s * 0.15, 0.7), (s * 0.5, 0.5), (s * 0.55, 0.05), lw=0.55)
        pen.bez((0, 0.15), (s * 0.3, 0.2), (s * 0.7, 0.0), (s * 0.75, -0.35), lw=0.5)


def mountains(pen, ridges=2):
    """Layered ridge silhouettes; the back ridge is lighter and taller."""
    for r in range(ridges - 1, -1, -1):
        base = -0.55
        peaks = [(-0.75 + pen.j(0.1), 0.55 + pen.j(0.1)), (-0.1 + pen.j(0.12), 0.95 - r * 0.15 + pen.j(0.1)), (0.6 + pen.j(0.1), 0.6 + pen.j(0.15))]
        if r == 1:
            peaks = [(-0.45 + pen.j(0.1), 0.8), (0.3 + pen.j(0.1), 1.05), (0.95, 0.55)]
        pts = [(-1.15, base)]
        prev = pts[0]
        for (px, ph) in peaks:
            vx = (prev[0] + px) / 2.0
            pts.append((vx, base + 0.15 + pen.j(0.05)))
            pts.append((px, base + ph))
            prev = (px, base + ph)
        pts.append((1.15, base))
        p = pen.path()
        p.moveTo(*pts[0])
        for q in pts[1:]:
            p.lineTo(*q)
        role = "mountain" if r == 0 else "mountain2"
        pen.draw(p, role)
        if r == 0:
            # one or two interior ridge lines
            px, ph = peaks[1]
            pen.line_to(px, base + ph, px + 0.25, base + 0.2, lw=0.5)


def conifer(pen):
    pen.line_to(0, -0.95, 0, -0.6, lw=0.9)
    for i, (y, w) in enumerate([(-0.6, 0.55), (-0.25, 0.42), (0.1, 0.3)]):
        p = pen.path()
        p.moveTo(-w, y)
        p.curveTo(-w * 0.4, y + 0.05, w * 0.4, y + 0.05, w, y)
        p.lineTo(0, y + 0.55)
        pen.draw(p, "leaf" if i % 2 == 0 else "leaf2")


def sun_disk(pen):
    pen.circle(0, 0, 0.5, role="object")
    for a in range(0, 360, 30):
        sx, sy = rot(0.62, 0, a)
        ex, ey = rot(0.85 + (0.1 if a % 60 == 0 else 0), 0, a)
        pen.line_to(sx, sy, ex, ey, lw=0.6)


def ripples(pen):
    for i, (w, y) in enumerate([(1.0, 0.3), (0.7, 0.0), (0.85, -0.3)]):
        pq = pen.arc(0, y, w, 0.12, 190, 300)
        pen.stroke(pq, line=pen.col("water"), lw=0.6)
        pq = pen.arc(0, y, w, 0.12, 320, 355)
        pen.stroke(pq, line=pen.col("water"), lw=0.6)


def feather(pen):
    pen.petal(-0.9, -0.3, 30, 1.9, 0.5, role="wing", roundness=0.4, jit=0.1)
    pen.line_to(-1.0, -0.35, 0.65, 0.6, lw=0.7)
    for t in (0.3, 0.45, 0.6, 0.75):
        x = -0.9 + 1.6 * t
        y = -0.3 + 0.92 * t
        pen.line_to(x, y, x - 0.1, y + 0.22, lw=0.45)
        pen.line_to(x + 0.05, y, x + 0.18, y - 0.18, lw=0.45)


def _trefoil(t, s=0.95):
    return ((math.sin(t) + 2 * math.sin(2 * t)) / 3.0 * s, (math.cos(t) - 2 * math.cos(2 * t)) / 3.0 * s)


def _trefoil_crossings():
    """Numerically locate the three double points: returns list of (t_a, t_b) pairs."""
    N = 720
    pts = [_trefoil(2 * math.pi * i / N) for i in range(N)]
    found = []
    for i in range(N):
        for jx in range(i + 60, N - (60 if i < 60 else 0)):
            dx = pts[i][0] - pts[jx][0]
            dy = pts[i][1] - pts[jx][1]
            if dx * dx + dy * dy < 0.0004:
                ta, tb = 2 * math.pi * i / N, 2 * math.pi * jx / N
                if all(abs(ta - fa) > 0.3 for fa, _ in found):
                    found.append((ta, tb))
    return found[:3]


_CROSS = None


def vine_knot(pen, leaves=True):
    """Simple trefoil loop with alternating over/under crossings."""
    global _CROSS
    if _CROSS is None:
        _CROSS = _trefoil_crossings()
    pts = [_trefoil(2 * math.pi * i / 72.0) for i in range(72)]
    p = pen.smooth(pts, closed=True, tension=1.0)
    pen.stroke(p, lw=0.9)
    # alternate: at each crossing the strand with the lower parameter goes under for k even, over for k odd
    for k, (ta, tb) in enumerate(_CROSS):
        under, over = (ta, tb) if k % 2 == 0 else (tb, ta)
        for (tc, gap) in ((under, True), (over, False)):
            seg = [_trefoil(tc + d) for d in (-0.14, -0.07, 0, 0.07, 0.14)]
            q = pen.path()
            q.moveTo(*seg[0])
            for s_ in seg[1:]:
                q.lineTo(*s_)
            if gap:
                pen.c.setStrokeColor(pen.ground)
                pen.c.setLineWidth(pen.lw * 4.0 / pen.scale)
                pen.c.drawPath(q, stroke=1, fill=0)
            else:
                pen.stroke(q, lw=0.9)
    if leaves:
        for (x, y, a) in ((0.0, 0.95, 90), (-0.82, -0.47, 210), (0.82, -0.47, -30)):
            pen.leaf(x * 0.9, y * 0.9, a, 0.32, 0.14, roundness=0.3)


def ornament_border(pen):
    pen.line_to(-1.0, 0.15, 1.0, 0.15, lw=0.6)
    pen.line_to(-1.0, -0.15, 1.0, -0.15, lw=0.6)
    x = -0.9
    i = 0
    while x < 0.95:
        pq = pen.arc(x, -0.15, 0.1, 0.18, 0, 180)
        pen.draw(pq, "object2" if i % 2 else "ground", close=True, lw=0.5)
        if i % 2:
            pen.dot(x, 0.26, 0.03, "object")
        x += 0.2
        i += 1


def snow_mark(pen):
    for a in (0, 60, 120):
        ex, ey = rot(0.9, 0, a)
        pen.line_to(-ex, -ey, ex, ey, lw=0.55)
        for s in (1, -1):
            bx, by = rot(0.55, 0, a)
            tx, ty = rot(0.25, 0, a + s * 50)
            pen.line_to(bx, by, bx + tx, by + ty, lw=0.45)
            pen.line_to(-bx, -by, -bx - tx, -by - ty, lw=0.45)


def raindrop(pen):
    pen.petal(0, -0.5, 90, 1.0, 0.5, role="water", roundness=0.95)


def petal_loose(pen):
    pen.petal(-0.5, 0, 0 + pen.j(10), 1.0, 0.55, role="petal", roundness=0.8, jit=0.12)


def seed_mark(pen):
    pen.ellipse(0, 0, 0.5, 0.26, role="object", ang=pen.u(-30, 30), lw=0.6)


def dot_mark(pen, role="accent"):
    pen.dot(0, 0, 0.5, role)


def curl(pen):
    pen.bez((-0.8, -0.3), (-0.4, 0.5), (0.3, 0.5), (0.8, -0.2), lw=0.6)


def olive_sprig(pen):
    pen.bez((-1.0, -0.25), (-0.4, -0.1), (0.3, 0.05), (1.0, 0.25), lw=0.85)
    for i, t in enumerate((0.1, 0.28, 0.46, 0.64, 0.82)):
        x = -1.0 + 2.0 * t
        y = -0.25 + 0.5 * t
        for s in (1, -1):
            pen.leaf(x, y, 20 + s * 48 + pen.j(8), 0.36, 0.12, role="leaf" if (i + s) % 2 else "leaf2", roundness=0.1)
    pen.leaf(1.0, 0.25, 22, 0.32, 0.11, role="leaf", roundness=0.1)


def stream(pen):
    """Long shallow double wave used as a 'botanical river'."""
    for off in (0.0, 0.1):
        p = pen.path()
        p.moveTo(-1.6, -0.3 + off)
        p.curveTo(-1.0, 0.5 + off, -0.6, -0.6 + off, 0.0, 0.0 + off)
        p.curveTo(0.6, 0.6 + off, 1.0, -0.5 + off, 1.6, 0.3 + off)
        pen.stroke(p, line=pen.col("water"), lw=0.7)


def ribbon(pen):
    p = pen.path()
    p.moveTo(-1.0, 0.3)
    p.curveTo(-0.5, 0.9, -0.2, -0.6, 0.3, -0.1)
    p.curveTo(0.6, 0.2, 0.8, 0.5, 1.0, 0.0)
    pen.stroke(p, lw=0.8)
    p = pen.path()
    p.moveTo(-1.0, 0.18)
    p.curveTo(-0.5, 0.78, -0.2, -0.72, 0.3, -0.22)
    p.curveTo(0.6, 0.08, 0.8, 0.38, 1.0, -0.12)
    pen.stroke(p, lw=0.8)


def rose_frame(pen):
    for s in (1, -1):
        pq = pen.arc(0, 0, 0.95, 0.95, 90 - s * 20 + (0 if s == 1 else 180), 90 + s * 70 + (0 if s == 1 else 180))
        pen.stroke(pq, line=pen.col("object"), lw=0.7)
        pen.dot(0.95 * math.cos(math.radians(90 + s * 70 + (0 if s == 1 else 180))), 0.95 * math.sin(math.radians(90 + s * 70 + (0 if s == 1 else 180))), 0.05, "object")
    pen.place(rose_small, 0, 0, 0.55)
    pen.leaf(-0.4, -0.35, 215, 0.4, 0.16, roundness=0.3)
    pen.leaf(0.4, -0.35, -35, 0.4, 0.16, roundness=0.3)


def window_arch(pen):
    p = pen.path()
    p.moveTo(-0.4, -0.6)
    p.lineTo(-0.4, 0.2)
    p.curveTo(-0.4, 0.75, 0.4, 0.75, 0.4, 0.2)
    p.lineTo(0.4, -0.6)
    pen.draw(p, "object")


MOTIFS = {k: v for k, v in globals().items() if callable(v) and k[0].islower() and k not in (
    "hexc", "mix", "darker", "rot", "bezier_arc")}
