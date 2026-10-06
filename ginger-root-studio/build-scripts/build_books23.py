# -*- coding: utf-8 -*-
"""Books 2 and 3 - collection guides (cover, map, six 3-page sections, review tracker; Book 3 adds the O Holy Night appendix)."""
import sys
from reportlab.lib.colors import HexColor
from grs_layout import Doc, P, ST, spaced, MARGIN, CONTENT_W, TOP, BOTTOM, PAGE_W, PAGE_H, HEAD, BODY, ACCENT, RULE, RULE_SOFT, CREAM, PARCH, PAPER, cluster
from grs_data import HEX, SEASONAL, SACRED, OH_SONGS, OH_DETAIL, OH_SHORT, OLD_CHRISTMAS_PALETTE, role_counts, ROLE_ORDER
import grs_boards as B
import grs_motifs as M

family = sys.argv[1] if len(sys.argv) > 1 else "seasonal"
OUT = sys.argv[2] if len(sys.argv) > 2 else ("../out/Ginger_Root_Studio_Seasonal_Collections.pdf" if family == "seasonal" else "../out/Ginger_Root_Studio_Sacred_Seasons.pdf")
EDITION_DATE = "October 6, 2026"
X = MARGIN
W = CONTENT_W
GUTTER = 18
COL = (W - GUTTER) / 2.0

if family == "seasonal":
    COLS = SEASONAL
    BOOK = "Book 2"
    TITLE = "Botanical & Seasonal Collections"
    LABEL = "Ginger Root Studio / Book 2 / Botanical & Seasonal"
    TOTAL = 60
    FAMILY_NAME = "Botanical & Seasonal"
else:
    COLS = SACRED
    BOOK = "Book 3"
    TITLE = "Sacred Seasons"
    LABEL = "Ginger Root Studio / Book 3 / Sacred Seasons"
    TOTAL = 62
    FAMILY_NAME = "Sacred Seasons"

d = Doc(OUT, LABEL, title="Ginger Root Studio - %s" % TITLE)


def check(y, label):
    if y < BOTTOM:
        d.warnings.append("page %d (%s) overflow by %.0f pt" % (d.page, label, BOTTOM - y))


OPEN_PAGE = {c["code"]: 3 + 3 * i for i, c in enumerate(COLS)}

# =============================================================== 01 COVER
d.new_page("Cover", "cover")
d.kicker("Prepared for Alisha Bruton", X, TOP - 10)
y = TOP - 48
y -= d.para("Ginger Root Studio", X, y, W, "h1big")
y -= 2
y -= d.para(TITLE, X, y, W, "deck", fontSize=15, leading=19)
y -= 2
d.kicker("%s / collection guide / six ranges, %d print briefs / working edition, %s" % (BOOK, TOTAL, EDITION_DATE), X, y - 8)
y -= 24
cy = y - 150
if family == "seasonal":
    cluster(d.c, PAGE_W / 2, cy, 150, "HG", [
        ("vine_s", {}, -0.7, 0.1, 0.5, 20, None),
        ("vine_s", {}, 0.7, 0.05, 0.45, -160, None),
        ("oak_leaf", {}, -0.5, 0.45, 0.26, 150, None),
        ("holly_leaf", {"berries": True}, 0.55, 0.45, 0.26, 20, None),
        ("ginkgo", {}, -0.05, -0.62, 0.22, -8, "light"),
        ("pine_sprig", {"cone": True}, 0.72, -0.4, 0.3, -30, None),
        ("strawberry", {}, -0.72, -0.4, 0.17, 15, None),
        ("acorn", {}, 0.3, -0.58, 0.14, 0, None),
        ("berry_cluster", {}, -0.3, -0.5, 0.2, 0, None),
        ("peony", {}, -0.42, 0.05, 0.3, 10, None),
        ("poinsettia", {}, 0.45, 0.12, 0.3, 0, None),
        ("dahlia", {}, 0.0, 0.08, 0.4, 0, None),
        ("bird", {}, 0.9, -0.68, 0.2, 0, None),
        ("butterfly", {}, -0.9, 0.5, 0.17, 15, None),
    ], seed=21)
else:
    cluster(d.c, PAGE_W / 2, cy, 150, "OH", [
        ("olive_sprig", {}, -0.62, 0.3, 0.42, 25, None),
        ("olive_sprig", {}, 0.62, 0.3, 0.42, 155, None),
        ("star8", {}, 0.0, 0.62, 0.22, 0, None),
        ("star4", {}, -0.3, 0.72, 0.07, 0, None),
        ("star4", {}, 0.32, 0.7, 0.06, 0, None),
        ("angel_wing", {}, -0.42, -0.05, 0.3, 10, None),
        ("bell", {}, 0.42, -0.05, 0.24, 8, None),
        ("candle", {}, 0.0, 0.05, 0.3, 0, None),
        ("water_lily", {}, -0.72, -0.5, 0.22, 0, None),
        ("oak_leaf", {}, 0.72, -0.52, 0.22, -20, None),
        ("bird", {}, -0.25, -0.55, 0.2, 0, None),
        ("hellebore", {}, 0.28, -0.55, 0.22, 0, None),
        ("mountains", {}, 0.0, -0.82, 0.3, 0, "light"),
    ], seed=22)
y = cy - 185
if family == "seasonal":
    tag = "Six gardens through the year, drawn with one hand and one palette."
else:
    tag = "Six hymns translated into gardens, stars, water and light."
h = d.para(tag, X, y, W, "quote", alignment=1)
y -= h + 16
spaced(d.c, PAGE_W / 2, y, "  /  ".join(c["name"].upper() for c in COLS), size=6.6, spacing=1.2, align="center", color=ACCENT)
y -= 24
if family == "seasonal":
    body = "Six ranges and 60 print briefs: Harvest Garden, Christmas at the Garden, Winter Garden, The Secret Garden, The Garden Party and the signature Jade Garden. Each range has a story, a named palette subset, a concept board, a complete lineup and a drawing page."
else:
    body = "Six ranges and 62 print briefs. O Holy Night keeps its original twelve-print structure; the five companion hymns use the studio’s standard ten. Each range translates meaning, symbolism and imagery into botanical pattern; no lyrics are printed."
h = d.para(body, X + 40, y, W - 80, "serifc")
y -= h + 22
d.rule(X + 120, y, W - 240)
y -= 16
d.para("The concept boards and motif studies are original vector drawings made in code for this planning set. They show direction and hierarchy; they are not Alisha’s artwork and not verified seamless tiles. The written briefs are authoritative wherever a drawing differs.", X + 40, y, W - 80, "small", alignment=1)
d.footer()

# =============================================================== 02 COLLECTION MAP
y = d.new_page("Collection map", "map")
y = d.header("Collection map", "Six ranges, %d print briefs" % TOTAL, "How this book is organized and how the design count is reached")
rows = [["Code", "Collection", "Theme / season", "Lead form", "Prints", "Page"]]
lead = {"HG": "Dahlia and oak", "CG": "Poinsettia and camellia", "WG": "Pale camellia and bare twig", "SG": "Peony and camellia", "GP": "Open summer bloom and strawberry", "JG": "Camellia and S-curve vine",
        "OH": "Star of Bethlehem, nativity, angels", "CF": "Flowing stems and meadow flowers", "SP": "Sparrow and flowering branch", "BV": "Interlaced vine and oak", "GA": "Mountain ridge and wildflower", "IW": "Water lily and willow"}
for c in COLS:
    rows.append([c["code"], c["name"], c["season"], lead[c["code"]], str(len(c["prints"])), str(OPEN_PAGE[c["code"]])])
h, t = d.table(rows, X, y, [42, 150, 110, 150, 40, 36], boldfirst=True)
# links on rows
rowh = t._rowHeights
yy = y - rowh[0]
for i, c in enumerate(COLS):
    d.link_to("col_" + c["code"], X, yy, W, rowh[i + 1])
    yy -= rowh[i + 1]
y -= h + 10
if family == "seasonal":
    cnt = "Six collections x ten designs = 60 print briefs. Every range uses the studio’s standard structure: 2 heroes + 2 secondaries + 4 coordinates + 1 blender + 1 micro. Harvest Garden and Christmas at the Garden were each one design short in the original conversation; Orchard Sprig (HG08) and Pinecone Sprig (CG08) were added in this edition to complete them and are marked as proposals in their briefs."
else:
    cnt = "O Holy Night keeps the twelve-print structure requested in the original Christmas brief: 3 heroes + 5 secondaries + 4 coordinates = 12. The five other hymns use the studio’s standard ten: 2 heroes + 2 secondaries + 4 coordinates + 1 blender + 1 micro. 12 + 5 x 10 = 62 print briefs. O Holy Night has no blender or micro print because its four coordinates already supply the quiet textures (Bethlehem Starlight, Gloria Garland, Carol Ribbon, Manger Linen)."
y = d.block("How the count is reached", cnt, X, y, W)
rows = [["Role", "Per standard range", "What it does", "Starting scale study"],
        ["Hero", "2", "Carries the largest visual story and the most complex arrangement. The second hero has a genuinely different composition, not a recoloring.", "3-5 in motifs; 18-24 in repeat"],
        ["Secondary", "2", "Simplifies or isolates a motif family at medium scale.", "1.5-3 in motifs; 12-16 in repeat"],
        ["Coordinate", "4", "Reduces scale, complexity or density and pairs with the hero.", "0.4-1.2 in motifs; 6-12 in repeat"],
        ["Blender", "1", "Distinguished by low contrast and visual quietness.", "0.2-0.6 in rhythm; 4-8 in repeat"],
        ["Micro", "1", "Very small, readable marks for tags, liners and small goods.", "0.15-0.35 in marks; 4-6 in repeat"]]
if family == "sacred":
    rows[1][1] = "2 (OH: 3)"
    rows[2][1] = "2 (OH: 5)"
    rows[4][1] = "1 (OH: 0)"
    rows[5][1] = "1 (OH: 0)"
yy = d.section("The role hierarchy", X, y, W)
h, _ = d.table(rows, X, yy, [70, 90, 230, W - 390], style="tds", hstyle="th", boldfirst=True)
y = yy - h - 10
h = d.para("Scale studies are exploratory textile and paper starting points, not wallpaper specifications; the final size depends on the product and manufacturer (Book 1, page 14). O Holy Night briefs carry their own more specific studies.", X, y, W, "note")
y -= h + 10
rows = [["Page", "What each three-page collection section contains"],
        ["A", "Collection story, named palette swatches, the full concept board with every print labeled, the signature and a pairing to test."],
        ["B", "Complete print-by-print lineup: ID, name, role, motifs, composition and repeat direction, one drawing decision and a starting scale study."],
        ["C", "Isolated motif references drawn from the board, what to draw first, three development steps, a silhouette-study box and a repeat-thumbnail box."]]
yy = d.section("How to use each section", X, y, W)
h, _ = d.table(rows, X, yy, [40, W - 40], style="td", hstyle="th", boldfirst=True)
y = yy - h - 10
last = "Page 21 compares the six ranges and holds the decision about which one to finish first."
if family == "sacred":
    last += " Pages 22-24 preserve the original twelve-print O Holy Night detail (song themes, drawing inspiration, pitfalls and product ideas) from the earlier standalone Christmas brief."
h = d.para(last, X, y, W, "body")
y -= h
check(y, "map")
d.footer()

# =============================================================== collection sections
ROLE_CODE = {"Hero": "Hero", "Secondary": "Secondary", "Coordinate": "Coordinate", "Blender": "Blender", "Micro": "Micro"}
for ci, col in enumerate(COLS):
    code = col["code"]
    n = len(col["prints"])
    # ---------------------------------------------------- A: story + board
    y = d.new_page("%s / %s" % (code, col["name"]), "col_" + code)
    y = d.header("Collection %d of 6  /  %s  /  %s" % (ci + 1, code, col["season"]), col["name"], None, rule=False)
    y += 2
    h = d.para(col["story"], X, y, W, "deck", fontSize=10.5, leading=14.5)
    y -= h + 8
    d.rule(X, y, W)
    y -= 10
    d.kicker("Named palette subset", X, y - 6, color=HEAD, size=6.8, spacing=1.0)
    d.chips(col["palette"], X + 108, y + 2, size=14, gap=2, label_size=6.2)
    y -= 46
    bw = W
    bh = bw * 2 / 3.0
    rects = B.draw_board(d.c, X, y - bh, bw, bh, code, seed=11 + ci, label_h=9)
    # thin frame
    d.c.setStrokeColor(RULE)
    d.c.setLineWidth(0.5)
    d.c.rect(X, y - bh, bw, bh, stroke=1, fill=0)
    # labels under each swatch (inside the cream board area, just below the grid row)
    d.c.setFillColor(BODY)
    for k, (pid, sx, sy, sw, sh) in enumerate(rects):
        p = col["prints"][k]
        lab = "%02d  %s  /  %s" % (k + 1, p["name"], p["role"])
        d.c.setFont("Carlito", 5.9)
        # fit label into swatch width
        while d.c.stringWidth(lab, "Carlito", 5.9) > sw - 3 and len(lab) > 10:
            lab = lab[:-4] + "..."
        d.c.setFillColor(HexColor("#5A5146"))
        d.c.drawString(sx + 1.5, sy - 6.8, lab)
    y -= bh + 8
    h = d.para("Concept board drawn for this edition: the %s swatches are repeat concepts in true lattice tilings (half-drop, brick or straight), read left to right by row in lineup order; the cream band holds isolated motif studies. Not verified seamless tiles and not Alisha’s artwork." % ("twelve" if n == 12 else "ten"), X, y, W, "tiny")
    y -= h + 8
    ytop = y
    yy = d.section("Signature", X, ytop, COL)
    h1 = d.para(col["signature"], X, yy, COL, "small")
    yy2 = d.section("Pairing to test first", X + COL + GUTTER, ytop, COL)
    h2 = d.para(col["pairing"] + ". Put the hero beside one secondary and one quiet support at their relative scales before drawing anything else.", X + COL + GUTTER, yy2, COL, "small")
    y = ytop - max(ytop - yy + h1, ytop - yy2 + h2) - 10
    if y - BOTTOM > 40:
        d.box(X, y, W, y - BOTTOM - 2, label="What I notice on the board / what I would change in my own version", ruled=16)
    check(y, code + " story")
    d.footer()

    # ---------------------------------------------------- B: lineup
    y = d.new_page("%s lineup" % code, "lineup_" + code, level=1)
    rc = role_counts(col)
    PL = {"Hero": "heroes", "Secondary": "secondaries", "Coordinate": "coordinates", "Blender": "blenders", "Micro": "micro"}
    hier = " + ".join("%d %s" % (rc[r], (PL[r] if rc[r] > 1 else r.lower())) for r in ROLE_ORDER if rc[r])
    y = d.header("%s  /  print-by-print lineup  /  %s" % (code, hier), "%s, design by design" % col["name"], None if code == "OH" else "Every brief: motifs, composition and repeat direction, one drawing decision and a starting scale study")
    rows = [["ID / name / role", "Motifs", "Composition / repeat", "Drawing decision", "Scale study"]]
    if code == "OH":
        rows[0][0] = "ID / name / role / song"
    for p in col["prints"]:
        if code == "OH":
            mo, re_, de = OH_SHORT[p["id"]]
            first = P("<b>%s %s</b><br/>%s<br/><i>%s</i>" % (p["id"], p["name"], p["role"], OH_SONGS[p["id"]]), "tdx")
            rows.append([first, mo, re_, de, p["scale"]])
        else:
            first = P("<b>%s %s</b><br/>%s" % (p["id"], p["name"], p["role"]), "tds")
            rows.append([first, p["motifs"], p["repeat"], p["decision"], p["scale"]])
    widths = [104, 112, 126, 118, W - 460]
    pad = 2.5 if code == "OH" else 4
    h, t = d.table(rows, X, y, widths, style=("tdx" if code == "OH" else "tds"), hstyle="th", pad=pad, zebra=True)
    y -= h + 8
    if code == "OH":
        h = d.para("Briefs are condensed here to fit one page; the full original twelve-print detail, including song themes, drawing inspiration, steps, pitfalls and product ideas, is on pages 22-24. All twelve prints come from the original Christmas brief; nothing was added or removed.", X, y, W, "note")
        y -= h
    elif code in ("HG", "CG"):
        h = d.para("%s is a proposed addition from this edition, completing a list that was one design short in the original conversation." % ("HG08 Orchard Sprig" if code == "HG" else "CG08 Pinecone Sprig"), X, y, W, "note")
        y -= h
    if y - BOTTOM > 44:
        d.box(X, y - 6, W, y - 6 - BOTTOM, label="Order I will draw them in / what two prints share one drawing", ruled=16)
    check(y, code + " lineup")
    d.footer()

    # ---------------------------------------------------- C: sketch page
    y = d.new_page("%s drawing page" % code, "sketch_" + code, level=1)
    y = d.header("%s  /  drawing studies" % code, "Draw first, then arrange", "Isolated motif references from the board, what to draw first, and room for your own studies")
    d.kicker("Isolated motif references drawn for this range", X, y - 6, color=HEAD, size=6.8, spacing=1.0)
    y -= 12
    bandh = 104
    d.box(X, y, W, bandh, fill=CREAM)
    B.draw_studies(d.c, X, y - bandh + 16, W, bandh - 20, code, seed=5 + ci)
    d.c.setFont("Carlito", 5.7)
    d.c.setFillColor(ACCENT)
    names = B.STUDIES[code]
    for i, (nm, kw, md) in enumerate(names):
        lab = nm.replace("_", " ")
        extra = [str(v) for v in kw.values() if not isinstance(v, bool)]
        if extra:
            lab += ", " + " ".join(extra)
        if kw.get("berries") is False:
            lab += ", no berries"
        cx_ = X + W * (i + 0.5) / len(names)
        d.c.setFillColor(ACCENT)
        d.c.drawCentredString(cx_, y - bandh + 12, lab.upper())
        d.c.setFillColor(BODY)
        d.c.drawCentredString(cx_, y - bandh + 5, {"line": "contour only", "paint": "painted", "light": "lightly painted"}[md])
    y -= bandh + 12
    ytop = y
    yy = d.section("What to draw first", X, ytop, COL)
    h1 = d.para(col["library"] + " Draw each on paper at a comfortable size, line only, before any color or repeat work.", X, yy, COL, "body")
    yy2 = d.section("Three development steps", X + COL + GUTTER, ytop, COL)
    steps_txt = "<br/>".join("<b>%d.</b>  %s" % (i + 1, s) for i, s in enumerate(col["steps"]))
    h2 = d.para(steps_txt, X + COL + GUTTER, yy2, COL, "body")
    y = ytop - max(ytop - yy + h1, ytop - yy2 + h2) - 10
    h = d.para("Scale check: hero motifs about 3-5 in, secondaries 1.5-3 in, coordinates 0.4-1.2 in, blender rhythm 0.2-0.6 in, micro marks 0.15-0.35 in. These are starting studies; the product decides the final size." if code != "OH" else "Scale check: use the individual studies in the lineup (vignettes 3.5-5 in, angels 3-4.5 in, bouquets 2.5-4 in, down to 0.06-0.15 in linen marks). These are starting studies; the product decides the final size.", X, y, W, "note")
    y -= h + 10
    bh = y - BOTTOM - 2
    d.box(X, y, COL, bh, label="Silhouette study  /  one motif, solid fill, no interior lines")
    d.box(X + COL + GUTTER, y, COL, bh, label="Repeat thumbnail  /  hero cluster in a 3 x 3 half-drop", dots=True)
    d.footer()

# =============================================================== 21 REVIEW TRACKER
y = d.new_page("Comparison and review tracker", "tracker")
y = d.header("Collection comparison", "Compare, choose, finish", "Score each range honestly, then commit to one finished ten-print range" + (" (twelve for O Holy Night)" if family == "sacred" else ""))
rows = [["Collection", "Drawing momentum (1-5)", "Buyer fit I can name", "Motifs already drawn", "Prints proofed", "Decision"]]
for c in COLS:
    rows.append([P("<b>%s</b><br/>%s / %d prints" % (c["name"], c["code"], len(c["prints"])), "tds"), "", "", "", "", ""])
h, t = d.table(rows, X, y, [130, 80, 110, 80, 60, W - 460], style="tds", hstyle="th", extra=[("TOPPADDING", (0, 1), (-1, -1), 9), ("BOTTOMPADDING", (0, 1), (-1, -1), 9)])
y -= h + 12
y = d.block("Decision rule", "Choose the range you can return to for several weeks with drawings you already have. Momentum beats novelty: " + ("Jade Garden has camellia practice behind it and Harvest Garden follows the current fall and Christmas interest; both share the cream ground, the line quality and the approach to space, so the first motif library is reused either way." if family == "seasonal" else "O Holy Night already has the most developed brief and shares its star, olive sprig and sparrow language with the other five, so it is the natural first Sacred Seasons range after a Botanical &amp; Seasonal collection exists.") + " Keep the other ranges as reference folders until the first range is proofed and presented.", X, y, W)
yy = d.section("Release check for the chosen range", X, y, W)
items = ["A recognizable hero and a genuinely different second hero.", "Quiet supports that pair with the hero at their intended scales.", "One named palette subset used consistently across all prints.", "Clean repeat boundaries tested on an exported tile, not only the live preview.", "Readable detail at the intended product size.", "A concise presentation with IDs, roles and the palette (Book 1, page 18)."]
y = yy
for it in items:
    hh = d.check_item(it, X, y, W, "small")
    y -= hh + 4
y -= 8
bh = (y - BOTTOM - 10) / 2.0
d.box(X, y, W, bh, label="The range I will finish first / why / by the end of week", ruled=16)
y -= bh + 10
d.box(X, y, W, bh, label="What its first ten drawings will be", ruled=16)
d.footer()

# =============================================================== Book 3 appendix: original O Holy Night detail
if family == "sacred":
    oh = [c for c in COLS if c["code"] == "OH"][0]
    prints = oh["prints"]
    for pg in range(3):
        y = d.new_page("Appendix: O Holy Night original detail %d/3" % (pg + 1), "oh_app_%d" % pg)
        y = d.header("Appendix  /  O Holy Night  /  original twelve-print detail  /  %d of 3" % (pg + 1), "The earlier Christmas design branch, preserved", "Song themes, drawing inspiration, steps, pitfalls and product ideas from the original standalone brief")
        if pg == 0:
            h = d.para("The first standalone Christmas PDF used a darker seven-color palette. The Sacred Seasons guide adapts those prints into the studio’s 23-color palette (Warm Cream, Old Parchment, garden greens, Warm Berry, Warm Gold, Soft Cocoa and Blue Willow) while keeping the religious imagery and hierarchy. The seven earlier colors are shown here for history only; they are not in the .ase file and should not be mixed into the master palette.", X, y, W, "small")
            y -= h + 6
            xx = X
            d.c.setFont("Carlito", 6.4)
            d.c.setFillColor(BODY)
            for nme, hx in OLD_CHRISTMAS_PALETTE:
                d.c.setFillColor(HexColor(hx))
                d.c.setStrokeColor(RULE)
                d.c.roundRect(xx, y - 13, 13, 13, 2, stroke=1, fill=1)
                d.c.setFillColor(BODY)
                d.c.drawString(xx + 16, y - 10, "%s %s" % (nme, hx))
                xx += 16 + d.c.stringWidth("%s %s" % (nme, hx), "Carlito", 6.4) + 10
            y -= 22
            h = d.para("The nativity star is the Star of Bethlehem. The original brief’s “North Star” wording has been corrected: the biblical guiding star is not Polaris, and a stylized eight-point star serves the collection’s visual language.", X, y, W, "note")
            y -= h + 8
        group = prints[pg * 4:(pg + 1) * 4]
        card_w = COL
        ytop = y
        for k, p in enumerate(group):
            det = OH_DETAIL[p["id"]]
            cx = X + (k % 2) * (COL + GUTTER)
            if k == 2:
                ytop = y_row_bottom - 10
            cy = ytop
            txt = ("<b>%s  %s</b>  /  %s  /  inspired by <i>%s</i><br/>"
                   "<b>Theme.</b> %s<br/>"
                   "<b>Drawing inspiration.</b> %s<br/>"
                   "<b>Steps.</b> 1. %s  2. %s  3. %s<br/>"
                   "<b>Pitfalls.</b> %s<br/>"
                   "<b>Original palette / alternate.</b> %s; %s<br/>"
                   "<b>Pair with.</b> %s<br/>"
                   "<b>Product ideas.</b> %s") % (p["id"], p["name"], p["role"], OH_SONGS[p["id"]], det["theme"], det["inspiration"], det["steps"][0], det["steps"][1], det["steps"][2], det["pitfalls"], det["orig"], det["alt"], det["pair"], det["products"])
            pp = P(txt, "tds")
            _, hh = pp.wrap(card_w - 16, 10000)
            d.c.setStrokeColor(RULE)
            d.c.setLineWidth(0.6)
            d.c.setFillColor(HexColor("#FBF7EE"))
            d.c.rect(cx, cy - hh - 14, card_w, hh + 14, stroke=1, fill=1)
            pp.drawOn(d.c, cx + 8, cy - hh - 7)
            if k % 2 == 0:
                y_row_bottom = cy - hh - 14
            else:
                y_row_bottom = min(y_row_bottom, cy - hh - 14)
        y = y_row_bottom - 4
        check(y, "appendix %d" % pg)
        d.footer()

d.save()
for w_ in d.warnings:
    print("WARNING:", w_)
print("%s pages: %d -> %s" % (BOOK, d.page, OUT))
