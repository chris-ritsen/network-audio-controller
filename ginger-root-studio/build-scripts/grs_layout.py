# -*- coding: utf-8 -*-
"""Page layout system for the Ginger Root Studio books (US Letter, 42pt margins)."""
import random
from reportlab.pdfgen import canvas
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.enums import TA_LEFT, TA_CENTER
from reportlab.platypus import Paragraph, Table, TableStyle
from reportlab.lib.colors import HexColor, Color
import grs_motifs as M
import grs_boards as B
from grs_data import HEX

PAGE_W, PAGE_H = 612.0, 792.0
MARGIN = 42.0
CONTENT_W = PAGE_W - 2 * MARGIN   # 528
TOP = PAGE_H - MARGIN             # 750
BOTTOM = MARGIN + 18              # 60 (footer sits below)

PAPER = HexColor("#FFFDF8")
HEAD = HexColor("#3F5540")
BODY = HexColor("#4B4940")
ACCENT = HexColor("#9A653C")
RULE = HexColor("#DFD3BF")
RULE_SOFT = HexColor("#EDE4D3")
CREAM = HexColor("#F7EEDC")
PARCH = HexColor("#E9D8B8")

F = "/usr/share/fonts/truetype/crosextra/"
_registered = False


def register_fonts():
    global _registered
    if _registered:
        return
    pdfmetrics.registerFont(TTFont("Caladea", F + "Caladea-Regular.ttf"))
    pdfmetrics.registerFont(TTFont("Caladea-Italic", F + "Caladea-Italic.ttf"))
    pdfmetrics.registerFont(TTFont("Caladea-Bold", F + "Caladea-Bold.ttf"))
    pdfmetrics.registerFont(TTFont("Caladea-BoldItalic", F + "Caladea-BoldItalic.ttf"))
    pdfmetrics.registerFont(TTFont("Carlito", F + "Carlito-Regular.ttf"))
    pdfmetrics.registerFont(TTFont("Carlito-Bold", F + "Carlito-Bold.ttf"))
    pdfmetrics.registerFont(TTFont("Carlito-Italic", F + "Carlito-Italic.ttf"))
    pdfmetrics.registerFont(TTFont("Carlito-BoldItalic", F + "Carlito-BoldItalic.ttf"))
    pdfmetrics.registerFontFamily("Carlito", normal="Carlito", bold="Carlito-Bold", italic="Carlito-Italic", boldItalic="Carlito-BoldItalic")
    pdfmetrics.registerFontFamily("Caladea", normal="Caladea", bold="Caladea-Bold", italic="Caladea-Italic", boldItalic="Caladea-BoldItalic")
    _registered = True


register_fonts()
from reportlab import rl_config
rl_config.canvas_basefontname = "Carlito"

ST = dict(
    body=ParagraphStyle("body", fontName="Carlito", fontSize=9.6, leading=14, textColor=BODY),
    bodyb=ParagraphStyle("bodyb", fontName="Carlito-Bold", fontSize=9.6, leading=14, textColor=BODY),
    small=ParagraphStyle("small", fontName="Carlito", fontSize=8.6, leading=12, textColor=BODY),
    smallb=ParagraphStyle("smallb", fontName="Carlito-Bold", fontSize=8.6, leading=12, textColor=BODY),
    tiny=ParagraphStyle("tiny", fontName="Carlito", fontSize=7.6, leading=10, textColor=BODY),
    tinyb=ParagraphStyle("tinyb", fontName="Carlito-Bold", fontSize=7.6, leading=10, textColor=HEAD),
    note=ParagraphStyle("note", fontName="Carlito-Italic", fontSize=8.6, leading=12, textColor=BODY),
    h1=ParagraphStyle("h1", fontName="Caladea", fontSize=21, leading=25, textColor=HEAD),
    h1big=ParagraphStyle("h1big", fontName="Caladea", fontSize=34, leading=38, textColor=HEAD),
    deck=ParagraphStyle("deck", fontName="Caladea-Italic", fontSize=11, leading=15, textColor=BODY),
    quote=ParagraphStyle("quote", fontName="Caladea-Italic", fontSize=12.5, leading=18, textColor=HEAD),
    sec=ParagraphStyle("sec", fontName="Carlito-Bold", fontSize=8.4, leading=11, textColor=HEAD),
    th=ParagraphStyle("th", fontName="Carlito-Bold", fontSize=8.0, leading=10, textColor=HEAD),
    td=ParagraphStyle("td", fontName="Carlito", fontSize=8.6, leading=11.8, textColor=BODY),
    tdb=ParagraphStyle("tdb", fontName="Carlito-Bold", fontSize=8.6, leading=11.8, textColor=BODY),
    tds=ParagraphStyle("tds", fontName="Carlito", fontSize=8.0, leading=10.6, textColor=BODY),
    tdsb=ParagraphStyle("tdsb", fontName="Carlito-Bold", fontSize=8.0, leading=10.6, textColor=BODY),
    tdx=ParagraphStyle("tdx", fontName="Carlito", fontSize=7.5, leading=9.4, textColor=BODY),
    serif=ParagraphStyle("serif", fontName="Caladea", fontSize=10.5, leading=15, textColor=BODY),
    serifc=ParagraphStyle("serifc", fontName="Caladea", fontSize=10.5, leading=15, textColor=BODY, alignment=TA_CENTER),
    center=ParagraphStyle("center", fontName="Carlito", fontSize=9.6, leading=14, textColor=BODY, alignment=TA_CENTER),
)


import re
_AMP = re.compile(r"&(?!#?\w+;)")


def esc(text):
    return _AMP.sub("&amp;", text)


def P(text, style="body", **kw):
    text = esc(text)
    st = ST[style] if isinstance(style, str) else style
    if kw:
        st = ParagraphStyle("x", parent=st, **kw)
    return Paragraph(text, st)


def spaced(c, x, y, text, font="Carlito-Bold", size=7.4, spacing=1.3, align="left", color=None):
    """Draw letter-spaced text with a text object; returns the advance width."""
    w = pdfmetrics.stringWidth(text, font, size) + spacing * max(len(text) - 1, 0)
    if align == "center":
        x = x - w / 2.0
    elif align == "right":
        x = x - w
    t = c.beginText()
    t.setTextOrigin(x, y)
    t.setFont(font, size)
    t.setCharSpace(spacing)
    if color is not None:
        t.setFillColor(color)
    t.textOut(text)
    t.setCharSpace(0)
    c.drawText(t)
    return w


class Doc:
    def __init__(self, path, book_label, title=None, author="Ginger Root Studio / working edition"):
        self.c = canvas.Canvas(path, pagesize=(PAGE_W, PAGE_H), initialFontName="Carlito", initialFontSize=9.6)
        self.c.setTitle(title or book_label)
        self.c.setAuthor(author)
        self.c.setSubject("Ginger Root Studio working edition, October 2026")
        self.book_label = book_label
        self.page = 0
        self.warnings = []
        self.keys = set()

    # ------------------------------------------------------------ page frame
    def new_page(self, outline=None, key=None, level=0):
        if self.page > 0:
            self.c.showPage()
        self.page += 1
        c = self.c
        c.setFillColor(PAPER)
        c.rect(0, 0, PAGE_W, PAGE_H, stroke=0, fill=1)
        if key:
            c.bookmarkPage(key)
            self.keys.add(key)
            if outline:
                c.addOutlineEntry(outline, key, level=level, closed=False)
        return TOP

    def footer(self, right_text=None):
        c = self.c
        c.setStrokeColor(RULE)
        c.setLineWidth(0.5)
        c.line(MARGIN, MARGIN - 4, PAGE_W - MARGIN, MARGIN - 4)
        spaced(c, MARGIN, MARGIN - 15, self.book_label.upper(), font="Carlito", size=6.8, spacing=1.0, color=ACCENT)
        c.setFont("Carlito-Bold", 8)
        c.setFillColor(HEAD)
        c.drawRightString(PAGE_W - MARGIN, MARGIN - 15, "%02d" % self.page)
        if right_text:
            c.setFont("Carlito", 6.8)
            c.setFillColor(ACCENT)
            c.drawRightString(PAGE_W - MARGIN - 22, MARGIN - 15, right_text)

    def kicker(self, text, x, y, color=ACCENT, size=7.4, spacing=1.3):
        text = text.replace("&amp;", "&").replace("&gt;", ">").replace("&lt;", "<")
        spaced(self.c, x, y, text.upper(), font="Carlito-Bold", size=size, spacing=spacing, color=color)
        return y - size - 4

    def header(self, kicker, title, deck=None, y=None, rule=True):
        """Standard page header; returns the y cursor below it."""
        y = TOP if y is None else y
        y = self.kicker(kicker, MARGIN, y - 7)
        y -= 14
        h = self.para(title, MARGIN, y + 4, CONTENT_W, "h1")
        y -= h - 2
        if deck:
            h = self.para(deck, MARGIN, y, CONTENT_W, "deck")
            y -= h
        if rule:
            y -= 6
            self.rule(MARGIN, y, CONTENT_W)
            y -= 12
        return y

    def rule(self, x, y, w, color=RULE, lw=0.6):
        self.c.setStrokeColor(color)
        self.c.setLineWidth(lw)
        self.c.line(x, y, x + w, y)

    # ------------------------------------------------------------ text
    def para(self, text, x, y, w, style="body", **kw):
        p = P(text, style, **kw)
        _, h = p.wrap(w, 10000)
        p.drawOn(self.c, x, y - h)
        return h

    def section(self, title, x, y, w, gap_after=5):
        y = self.kicker(title, x, y - 6, color=HEAD, size=7.6, spacing=1.1)
        self.rule(x, y + 1, w, color=RULE_SOFT, lw=0.5)
        return y - gap_after

    def block(self, title, text, x, y, w, style="body", gap=9):
        y = self.section(title, x, y, w)
        h = self.para(text, x, y, w, style)
        return y - h - gap

    # ------------------------------------------------------------ tables
    def table(self, rows, x, y, widths, header=True, style="td", hstyle="th", zebra=False, pad=4, rules=True,
              valign="TOP", boldfirst=False, extra=None, leading=None):
        """rows: list of lists of str/Paragraph. Draws at (x, y-top). Returns (height, Table)."""
        data = []
        for ri, r in enumerate(rows):
            row = []
            for ci, cell in enumerate(r):
                if isinstance(cell, str):
                    st = hstyle if (header and ri == 0) else (("tdb" if boldfirst and ci == 0 else style))
                    cell = P(cell, st)
                row.append(cell)
            data.append(row)
        t = Table(data, colWidths=widths, hAlign="LEFT")
        ts = [("VALIGN", (0, 0), (-1, -1), valign), ("FONTNAME", (0, 0), (-1, -1), "Carlito"),
              ("LEFTPADDING", (0, 0), (-1, -1), pad), ("RIGHTPADDING", (0, 0), (-1, -1), pad),
              ("TOPPADDING", (0, 0), (-1, -1), pad), ("BOTTOMPADDING", (0, 0), (-1, -1), pad)]
        if rules:
            ts.append(("LINEBELOW", (0, 0), (-1, -1), 0.5, RULE))
        if header:
            ts.append(("LINEBELOW", (0, 0), (-1, 0), 0.9, HEAD))
            ts.append(("BOTTOMPADDING", (0, 0), (-1, 0), 5))
        if zebra:
            for i in range(1, len(rows)):
                if i % 2 == 0:
                    ts.append(("BACKGROUND", (0, i), (-1, i), HexColor("#FBF7EE")))
        if extra:
            ts.extend(extra)
        t.setStyle(TableStyle(ts))
        _, h = t.wrapOn(self.c, sum(widths), 10000)
        t.drawOn(self.c, x, y - h)
        return h, t

    # ------------------------------------------------------------ worksheet furniture
    def box(self, x, y, w, h, label=None, ruled=0, dots=False, fill=None, label_inside=True, corner=False):
        """Sketch / note box with the top-left corner at (x, y). Returns bottom y."""
        c = self.c
        c.setStrokeColor(RULE)
        c.setLineWidth(0.7)
        if fill:
            c.setFillColor(fill)
            c.rect(x, y - h, w, h, stroke=1, fill=1)
        else:
            c.rect(x, y - h, w, h, stroke=1, fill=0)
        if label:
            self.kicker(label, x + 7, y - 12, color=ACCENT, size=6.8, spacing=1.1)
        if ruled:
            c.setStrokeColor(RULE_SOFT)
            c.setLineWidth(0.5)
            top = y - (22 if label else 10)
            yy = top - ruled
            while yy > y - h + 6:
                c.line(x + 8, yy, x + w - 8, yy)
                yy -= ruled
        if dots:
            c.setFillColor(RULE)
            step = 14
            yy = y - 24
            while yy > y - h + 8:
                xx = x + 12
                while xx < x + w - 8:
                    c.circle(xx, yy, 0.45, stroke=0, fill=1)
                    xx += step
                yy -= step
        return y - h

    def checkbox(self, x, y, size=8, checked=False):
        c = self.c
        c.setStrokeColor(HEAD)
        c.setLineWidth(0.7)
        c.rect(x, y - size, size, size, stroke=1, fill=0)
        if checked:
            c.line(x + 2, y - size / 2, x + size / 2 - 0.5, y - size + 2)
            c.line(x + size / 2 - 0.5, y - size + 2, x + size - 1.5, y - 1.5)

    def check_item(self, text, x, y, w, style="body"):
        self.checkbox(x, y - 2)
        h = self.para(text, x + 14, y, w - 14, style)
        return max(h, 12)

    def field(self, label, x, y, w, value_w=None):
        """Inline label followed by a writing line."""
        c = self.c
        self.kicker(label, x, y - 7, color=HEAD, size=6.8, spacing=1.0)
        lw_ = pdfmetrics.stringWidth(label.upper(), "Carlito-Bold", 6.8) + len(label) * 1.0
        c.setStrokeColor(RULE)
        c.setLineWidth(0.6)
        c.line(x + lw_ + 8, y - 9, x + w, y - 9)
        return y - 18

    def swatch(self, x, y, w, h, hexv, name=None, label_hex=True, label_size=7.4):
        c = self.c
        c.setFillColor(HexColor(hexv))
        c.setStrokeColor(RULE)
        c.setLineWidth(0.4)
        c.roundRect(x, y - h, w, h, 3, stroke=1, fill=1)
        if name:
            c.setFillColor(BODY)
            c.setFont("Carlito-Bold", label_size)
            c.drawString(x, y - h - 9, name)
            if label_hex:
                c.setFont("Carlito", label_size - 0.6)
                c.drawString(x, y - h - 18, hexv)

    def chips(self, names, x, y, size=16, gap=4, labels=True, label_size=6.6, max_w=None):
        """Row of small colour chips with names beneath; returns height used."""
        c = self.c
        xx = x
        for n in names:
            c.setFillColor(HexColor(HEX[n]))
            c.setStrokeColor(RULE)
            c.setLineWidth(0.4)
            c.roundRect(xx, y - size, size, size, 2, stroke=1, fill=1)
            if labels:
                c.setFillColor(BODY)
                c.setFont("Carlito", label_size)
                words = n.split(" ")
                ly = y - size - 8
                for wd in words:
                    c.drawString(xx, ly, wd)
                    ly -= label_size + 1
            xx += size + gap + (22 if labels else 0)
        return size + (26 if labels else 0)

    # ------------------------------------------------------------ navigation
    def link_to(self, key, x, y, w, h):
        if key in self.keys or True:
            self.c.linkRect("", key, (x, y - h, x + w, y), relative=0, thickness=0)

    def save(self):
        self.c.save()


def cluster(c, cx, cy, size, code, items, seed=3, mode=None):
    """Draw a free cluster of motifs (for covers): items = [(name, kw, dx, dy, s, rot, mode)] in unit coords."""
    rng = random.Random(seed)
    cols = B.scheme_colors(code)
    pen = M.Pen(c, rng)
    pen.cols = cols
    pen.ground = PAPER
    pen.line = cols["line"]
    pen.lw = 0.6
    for (name, kw, dx, dy, s, rot, md) in items:
        pen.rng = random.Random(seed + int(dx * 100 + dy * 37))
        pen.place(M.MOTIFS[name], cx + dx * size, cy + dy * size, s * size, rot, False, md or mode, **kw)
