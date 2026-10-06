# -*- coding: utf-8 -*-
"""Book 1 - Ginger Root Studio Brand & Business Workbook (25 pages)."""
import sys
from reportlab.lib.colors import HexColor
from reportlab.platypus import Paragraph
from grs_layout import Doc, P, ST, spaced, MARGIN, CONTENT_W, TOP, BOTTOM, PAGE_W, PAGE_H, HEAD, BODY, ACCENT, RULE, RULE_SOFT, CREAM, PARCH, PAPER, cluster
from grs_data import PALETTE, PALETTE_GROUPS, SUBSETS, HEX, SEASONAL, SACRED
import grs_motifs as M

OUT = sys.argv[1] if len(sys.argv) > 1 else "../out/Ginger_Root_Studio_Brand_Business_Workbook.pdf"
EDITION_DATE = "October 6, 2026"
X = MARGIN
W = CONTENT_W
GUTTER = 18
COL = (W - GUTTER) / 2.0

d = Doc(OUT, "Ginger Root Studio / Book 1 / Studio Workbook", title="Ginger Root Studio - Brand & Business Workbook")


def check(y, label):
    if y < BOTTOM:
        d.warnings.append("page %d (%s) overflow by %.0f pt" % (d.page, label, BOTTOM - y))


# =============================================================== 01 COVER
d.new_page("Cover", "p01")
d.kicker("Prepared for Alisha Bruton", X, TOP - 10)
y = TOP - 48
y -= d.para("Ginger Root Studio", X, y, W, "h1big")
y -= 2
y -= d.para("Brand &amp; Business Workbook", X, y, W, "deck", fontSize=15, leading=19)
y -= 2
d.kicker("Working edition / %s" % EDITION_DATE, X, y - 8)
y -= 24
# cover illustration
cy = y - 150
cluster(d.c, PAGE_W / 2, cy, 150, "JG", [
    ("vine_s", {}, -0.62, 0.18, 0.5, 28, None),
    ("vine_s", {}, 0.6, -0.1, 0.45, -150, None),
    ("leaf_simple", {"kind": "pointed"}, -0.3, 0.38, 0.28, 120, None),
    ("leaf_simple", {}, 0.32, 0.42, 0.26, 40, "light"),
    ("leaf_simple", {"kind": "pointed"}, 0.38, -0.5, 0.26, -60, None),
    ("leaf_simple", {}, -0.42, -0.42, 0.26, 215, "light"),
    ("ginkgo", {}, 0.02, -0.62, 0.24, -12, "light"),
    ("berry_cluster", {}, -0.72, -0.28, 0.2, 10, None),
    ("bud", {}, 0.7, 0.28, 0.22, -20, None),
    ("camellia", {"view": "half"}, -0.5, 0.0, 0.3, 15, None),
    ("camellia", {"view": "three-quarter"}, 0.48, 0.12, 0.3, -10, None),
    ("camellia", {}, 0.0, 0.05, 0.42, 8, None),
    ("bird", {}, 0.82, -0.42, 0.2, 0, None),
    ("butterfly", {}, -0.82, 0.5, 0.17, 20, None),
], seed=12)
y = cy - 185
h = d.para("A thoughtful pattern business, rooted in a world you love to draw.", X, y, W, "quote", alignment=1)
y -= h + 16
spaced(d.c, PAGE_W / 2, y, "STYLE  /  COLLECTIONS  /  DRAWING  /  PRODUCTION  /  LICENSING", size=7.4, spacing=1.6, align="center", color=ACCENT)
y -= 24
h = d.para("A practical foundation for a hand-drawn botanical studio: a recognizable visual language, a connected collection library of twelve ranges and 122 print briefs, and a realistic path from a sketchbook to a licensing portfolio.", X + 40, y, W - 80, "serifc")
y -= h + 22
d.rule(X + 120, y, W - 240)
y -= 16
d.para("Built from your shared Surface Pattern Design Roadmap and Christmas collection brief. New recommendations and the drawn visual references in these three books are starting points for your own artwork and decisions, not finished designs.", X + 40, y, W - 80, "small", alignment=1)
d.footer()

# =============================================================== 02 START HERE
y = d.new_page("Start here", "p02")
y = d.header("Start here", "Three books, one studio", "Use the collection guides as a library of possibilities, not a production deadline")
cols3 = (W - 2 * 14) / 3.0
texts = [
    ("Book 1 / Foundation and action", "This workbook holds the brand decisions, palette, drawing workflow, production checks and business plan. Pages 3-7 define the studio; pages 8-14 develop the artwork; pages 15-24 turn the work into a manageable practice. Page 25 lists sources and editorial decisions."),
    ("Book 2 / Botanical &amp; Seasonal", "Six illustrated ranges and 60 print briefs: Harvest Garden, Christmas at the Garden, Winter Garden, The Secret Garden, The Garden Party and Jade Garden. Begin with one range and use its motif library to build related prints."),
    ("Book 3 / Sacred Seasons", "Six hymn-inspired ranges and 62 print briefs. O Holy Night keeps its earlier twelve-print structure; the other five contain ten prints each. The emotional theme shapes each design without printing lyrics."),
]
ytop = y
hmax = 0
for i, (t, tx) in enumerate(texts):
    xx = X + i * (cols3 + 14)
    yy = d.section(t, xx, ytop, cols3)
    h = d.para(tx, xx, yy, cols3, "small")
    hmax = max(hmax, ytop - yy + h)
y = ytop - hmax - 16
# chosen vs proposed
ytop = y
yy = d.section("Already chosen in your conversation", X, ytop, COL)
h1 = d.para("Ginger Root Studio, spelled with spaces. Warm botanical color and flowing hand-drawn detail. Wallpaper, textiles and stationery as the primary products. Adobe Illustrator. Fifteen hours a week. Camellia practice already under way. Half-drop for the lead botanical, a richer but warm palette and finer linework. A recent emphasis on fall and Christmas, with Jade Garden still the signature range.", X, yy, COL, "small")
yy2 = d.section("Proposed in this edition", X + COL + GUTTER, ytop, COL)
h2 = d.para("The 12-week schedule, business worksheets, collection role assignments, logo directions and visual studies are recommendations. Orchard Sprig and Pinecone Sprig complete two lists that were one design short. The palette has 23 named colors, including Deep Olive, although the original thread called it a 24-color palette. Revenue figures are hypothetical planning inputs, never forecasts.", X + COL + GUTTER, yy2, COL, "small")
y = ytop - max(ytop - yy + h1, ytop - yy2 + h2) - 18
# how to read a brief
yy = d.section("How to read a print brief in Books 2 and 3", X, y, W)
h = d.para("Every design has a stable ID (for example HG01), a working name, a role, its motifs, a composition or repeat direction, one drawing decision and a starting scale study. The standard range is ten designs: 2 heroes + 2 secondaries + 4 coordinates + 1 blender + 1 micro. A hero carries the largest visual story; the second hero needs a genuinely different composition, not a recoloring. A secondary simplifies or isolates a motif family. Coordinates reduce scale, complexity or density. A blender is quiet and low in contrast. A micro uses very small readable marks. O Holy Night is the deliberate exception with 3 heroes, 5 secondaries and 4 coordinates.", X, yy, W, "small")
y = yy - h - 16
# first useful session box
d.box(X, y, W, 86, label="Your first useful session", fill=CREAM)
d.para("Choose Harvest Garden or Jade Garden (page 17). Draw one flower from three angles, add two leaf shapes, then make three small composition thumbnails. Use the first repeat test to discover what you need to draw next. Record the session in the project tracker on page 24.", X + 10, y - 26, W - 20, "body")
y -= 86 + 14
yy = d.section("A note on the illustrations", X, y, W)
h = d.para("The concept boards and motif studies in all three books are original vector drawings produced in code for this planning set. They show motif families, hierarchy and repeat structure in the studio palette; they are intentionally simpler than your hand-drawn work will be, and they are not finished or seamless production artwork. Where a drawing differs from a written brief, the brief is correct.", X, yy, W, "small")
y = yy - h
check(y, "start here")
d.footer()

# =============================================================== 03 BRAND FOUNDATION
y = d.new_page("Brand foundation", "p03")
y = d.header("Brand foundation", "The feeling of Ginger Root Studio", "Warm, botanical, graceful, nostalgic and quietly playful")
d.box(X, y, W, 54, fill=CREAM)
d.para("“Hand-drawn botanical patterns for homes and everyday objects, filled with warm color, graceful movement and small stories from nature.”", X + 16, y - 14, W - 32, "quote", alignment=1)
y -= 54 + 12
y = d.block("A working positioning statement", "Use the line above as draft language for a portfolio introduction. It connects the style to wallpaper, textiles and stationery without making the brand depend on a single season or age group. It is draft copy, not an approved tagline.", X, y, W)
rows = [["Visual pillar", "How it appears in the work"],
        ["Botanical character", "Rounded flowers, buds, fine stems, layered leaves and carefully observed small creatures: camellia, dahlia, peony, bellflower, sparrow, butterfly, dragonfly."],
        ["Graceful movement", "S-curves, climbing stems, open arches and interlaced growth. Keep a clear route for the eye through a busy hero; cluster, then release space."],
        ["Warm color", "Cream and parchment grounds; moss, jade and olive foliage; peach, coral, ochre and berry accents; Blue Willow as the one small cool note."],
        ["Human touch", "Slightly uneven petals, tapered strokes and restrained painted texture. Some motifs line-only, some lightly painted, some fully painted. Preserve the character of the original drawing."],
        ["Quiet storytelling", "A bird on a branch, a fallen petal, a star or a lantern. Use the story motif with enough space around it to be noticed."],
        ["Influences, held lightly", "Art Nouveau movement and color; Korean and East Asian garden structure such as lattice, cloud forms and fans; a slight cottage atmosphere; vintage botanical books and old textiles. Felt, not literal."]]
h, _ = d.table(rows, X, y, [118, W - 118], boldfirst=True)
y -= h + 12
y = d.block("The style test", "Would this still feel like Ginger Root Studio in one color? Does the silhouette work before the shading? Does the palette feel warm? Is there room for a viewer to rest? Could it live in a home for ten years? If a design feels loud, busy or novelty-driven, cut it.", X, y, W)
y = d.block("What the studio is not", "Not generic flat clip art, neon seasonal art, heavy distressing, extremely dark ornate engraving, a highly rustic farmhouse look, or a trendy logo exercise detached from the patterns. The sacred Christmas direction stays heirloom, romantic, reverent and old-world.", X, y, W)
d.box(X, y, W, max(y - BOTTOM - 2, 40), label="My own words for the studio feeling", ruled=16)
d.footer()

# =============================================================== 04 BRAND ARCHITECTURE
y = d.new_page("Brand architecture", "p04")
y = d.header("Brand architecture", "One name, three connected families", "Organize the portfolio by the story a buyer is looking for")
rows = [["Family", "Contents", "Purpose"],
        ["Botanical &amp; Seasonal", "Harvest Garden; Christmas at the Garden; Winter Garden; The Secret Garden; The Garden Party; Jade Garden. Six ranges, 60 prints (Book 2).", "Everyday and seasonal home, fabric and paper collections. Jade Garden is the signature evergreen range."],
        ["Sacred Seasons", "O Holy Night; Come Thou Fount; His Eye Is on the Sparrow; Be Thou My Vision; How Great Thou Art; It Is Well. Six ranges, 62 prints (Book 3).", "Faith-inspired ranges whose symbols and emotional themes remain connected to nature; translated through meaning and imagery, never printed lyrics."],
        ["Children &amp; Story", "Future adaptations of selected botanicals, birds, butterflies and quiet narrative motifs from the two families above.", "A product direction to test with lighter contrast, open spacing and readable small-scale stories. Not another required set of six collections."]]
h, _ = d.table(rows, X, y, [100, 228, W - 328], boldfirst=True)
y -= h + 14
y = d.block("What holds the families together", "Reuse the same line quality, core greens, warm neutrals and approach to negative space. A bird can move from Jade Garden into a gentler children’s colorway; a vine can support both a seasonal floral and a Sacred Seasons ornament. Stars, sparrows, vines, small shared flowers, sparing gold and a light ground option recur across Sacred Seasons so the six ranges read as one series.", X, y, W)
y = d.block("What makes each collection different", "Give each range one lead flower or landscape form, one clear emotional word and one distinct movement. Change the motif relationships and rhythm along with the palette, so that each collection has its own story rather than the same flower in a different color.", X, y, W)
rows = [["Range", "Lead form", "Emotional word", "Movement"],
        ["Jade Garden", "Camellia", "Collected", "S-curve vines"],
        ["Harvest Garden", "Dahlia", "Gathered", "Flowing, interrupted clusters"],
        ["Christmas at the Garden", "Poinsettia and camellia", "Festive", "Sweeping branch diagonals"],
        ["Winter Garden", "Pale camellia and bare twig", "Quiet", "Open branch network"],
        ["The Secret Garden", "Peony", "Discovered", "Curved paths and pockets"],
        ["The Garden Party", "Open summer bloom", "Cheerful", "Buoyant, airy meadow"],
        ["O Holy Night", "Star of Bethlehem", "Wonder", "Radiating light, vignettes"]]
h, _ = d.table(rows, X, y, [128, 140, 100, W - 368], style="tds", hstyle="th", boldfirst=False)
y -= h + 12
d.box(X, y, W, max(y - BOTTOM - 2, 40), label="My first family / first collection / intended product", ruled=16)
d.footer()

# =============================================================== 05 MASTER PALETTE
y = d.new_page("Master palette", "p05")
y = d.header("Master palette", "Twenty-three colors, one warm world", "RGB / HEX working values; use the companion Adobe Swatch Exchange file")
sw_w = (W - 6 * 8) / 7.0
sw_h = 34
for gname, names in PALETTE_GROUPS:
    d.kicker(gname, X, y - 6, color=HEAD, size=6.8, spacing=1.0)
    y -= 14
    for i, n in enumerate(names):
        d.swatch(X + i * (sw_w + 8), y, sw_w, sw_h, HEX[n], name=n, label_size=7.2)
    y -= sw_h + 30
y -= 2
h = d.para("The original conversation calls this a 24-color palette but names 23 distinct colors. This edition preserves those 23, including Deep Olive, which one repeated listing dropped. No twenty-fourth color has been invented to make the arithmetic work. The golds are flat color simulations; metallic ink or foil would need separate production instructions.", X, y, W, "body")
y -= h + 8
h = d.para("Treat these numbers as the studio’s digital starting point, not a promise of a printed result. Proof the actual material and printing process before approving a production color, and ask each buyer or printer which color mode and profile they want. No CMYK or Pantone equivalents are prescribed here.", X, y, W, "body")
y -= h + 10
d.box(X, y, W, max(y - BOTTOM - 2, 36), label="Colors I reach for most / colors to use sparingly", ruled=16)
d.footer()

# =============================================================== 06 COLOR IN PRACTICE
y = d.new_page("Color in practice", "p06")
y = d.header("Color in practice", "Build a small palette for each range", "A focused subset makes related patterns easier to combine")
d.kicker("Direction", X, y - 6, color=HEAD, size=6.8, spacing=1.0)
d.kicker("Starting subset", X + 118, y - 6, color=HEAD, size=6.8, spacing=1.0)
y -= 12
d.rule(X, y, W, color=HEAD, lw=0.8)
y -= 8
for label, names in SUBSETS:
    d.c.setFillColor(BODY)
    d.c.setFont("Carlito-Bold", 8.8)
    d.c.drawString(X, y - 12, label)
    d.chips(names, X + 118, y - 2, size=15, gap=3, label_size=6.4)
    y -= 46
    d.rule(X, y + 4, W, color=RULE_SOFT, lw=0.5)
y -= 6
y = d.block("A simple balance study", "Start with a dominant ground, two foliage colors, two flower or story colors, and one small accent. As a composition exercise, try roughly 60% ground and calm areas, 30% supporting color and 10% emphasis; adjust by eye rather than treating the ratio as a rule. Every collection in Books 2 and 3 names its own seven- or eight-color subset.", X, y, W)
y = d.block("Load the included .ase file", "In Illustrator, open the Swatches panel menu, choose Open Swatch Library &gt; Other Library, then select Ginger_Root_Studio_Master_Palette.ase. Add the colors you need to the current document. The file contains 23 named RGB process swatches with exactly the HEX values on page 5. [A4]", X, y, W)
y = d.block("Color approval workflow", "Keep a master palette and record every collection’s subset. Confirm the buyer’s requested color mode and profile before export. Order a sample when the process allows it; compare the printed sample with the design intent under consistent light and note what shifted.", X, y, W)
d.box(X, y, W, max(y - BOTTOM - 2, 36), label="Palette adjustment after the first proof", ruled=16)
d.footer()

# =============================================================== 07 IDENTITY DIRECTIONS
y = d.new_page("Identity directions", "p07")
y = d.header("Identity directions", "A wordmark with room to grow", "Five concepts to sketch; these are design directions, not approved or finished logos")
rows = [["", "Direction", "Drawing brief"],
        ["", "01 / Botanical serif", "An elegant, readable serif wordmark with a small hand-drawn leaf. The best starting direction for a warm, established studio feeling."],
        ["", "02 / Climbing stem", "A restrained serif name with one fine vine rising beside it. Keep the stem independent so the name remains legible at small size."],
        ["", "03 / Root seal", "A circular studio stamp with a simple root-and-leaf drawing. Explore as a secondary mark for tags, packaging and the back of a card."],
        ["", "04 / Garden frame", "A loose oval botanical border around the name. Use selectively on presentation covers and labels where there is enough space."],
        ["", "05 / Lettered signature", "A simple custom hand-lettered name with a small, plain STUDIO line. Aim for readable character rather than elaborate flourishes."]]
h, t = d.table(rows, X, y, [118, 104, W - 222], boldfirst=False, extra=[("TOPPADDING", (0, 1), (-1, -1), 10), ("BOTTOMPADDING", (0, 1), (-1, -1), 10)])
# thumbnails in the first column
rowh = t._rowHeights
yy = y - rowh[0]
c = d.c
for i in range(1, 6):
    cy = yy - rowh[i] / 2.0
    cx = X + 59
    c.saveState()
    if i == 1:
        c.setFillColor(HEAD)
        c.setFont("Caladea", 9.5)
        c.drawCentredString(cx - 6, cy - 3, "Ginger Root Studio")
        cluster(c, cx + 44, cy + 2, 10, "JG", [("leaf_simple", {}, 0, 0, 1.0, 35, None)], seed=1)
    elif i == 2:
        c.setFillColor(HEAD)
        c.setFont("Caladea", 9)
        c.drawCentredString(cx + 4, cy - 3, "Ginger Root Studio")
        cluster(c, cx - 48, cy, 22, "JG", [("vine_s", {"leaves": 4, "tendril": True}, 0, 0, 1.0, 95, None)], seed=2)
    elif i == 3:
        c.setStrokeColor(HEAD)
        c.setLineWidth(0.8)
        c.circle(cx, cy, 18, stroke=1, fill=0)
        c.circle(cx, cy, 15.5, stroke=1, fill=0)
        cluster(c, cx, cy + 6, 9, "JG", [("leaf_simple", {}, -0.3, 0.3, 1.0, 120, None), ("leaf_simple", {}, 0.3, 0.3, 1.0, 60, None)], seed=3)
        c.setStrokeColor(HEAD)
        c.setLineWidth(0.7)
        c.line(cx, cy + 8, cx, cy - 2)
        c.line(cx, cy - 2, cx - 6, cy - 10)
        c.line(cx, cy - 2, cx + 5, cy - 11)
        c.line(cx - 3, cy - 6, cx - 7, cy - 4)
        c.line(cx + 2, cy - 7, cx + 7, cy - 6)
        spaced(c, cx, cy - 15.5, "GINGER ROOT STUDIO", size=3.6, spacing=0.6, align="center", color=HEAD)
    elif i == 4:
        c.setFillColor(HEAD)
        c.setFont("Caladea", 8)
        c.drawCentredString(cx, cy - 3, "Ginger Root Studio")
        import math
        pen = M.Pen(c, __import__("random").Random(4))
        pen.cols = __import__("grs_boards").scheme_colors("JG")
        pen.ground = PAPER
        pen.lw = 0.5
        for k in range(18):
            a = 360.0 * k / 18
            px = cx + 48 * math.cos(math.radians(a))
            py = cy + 16 * math.sin(math.radians(a))
            pen.rng = __import__("random").Random(k)
            pen.place(M.leaf_simple, px, py, 4.2, a + 90 + (40 if k % 2 else -40), False, "light" if k % 2 else "paint")
    elif i == 5:
        c.setFillColor(HEAD)
        c.setFont("Caladea-Italic", 11.5)
        c.drawCentredString(cx, cy + 1, "Ginger Root")
        spaced(c, cx + 1, cy - 9, "STUDIO", font="Carlito", size=5, spacing=1.8, align="center", color=HEAD)
    c.restoreState()
    yy -= rowh[i]
y -= h + 4
h = d.para("The thumbnails in the first column are rough placement sketches made for this page, not drawn logos.", X, y, W, "note")
y -= h + 10
y = d.block("Recommended first test", "Sketch directions 01 and 03 in one color. Print each at about 1 inch and 3 inches wide. Keep the spelling and spacing consistent: Ginger Root Studio. Judge the name first; the botanical symbol should support it. Earlier logo images from your conversation were not available to this edition, so nothing here claims to match them.", X, y, W)
bh = y - BOTTOM - 2
d.box(X, y, COL, bh, label="Wordmark sketch")
d.box(X + COL + GUTTER, y, COL, bh, label="Secondary seal sketch")
d.footer()

# =============================================================== 08 DRAWING LIBRARY
y = d.new_page("Drawing library", "p08")
y = d.header("Drawing library", "A small set of motifs can go a long way", "Collect related shapes before building finished repeats")
rows = [["Motif bank", "Possibilities across the collection library"],
        ["Flowers", "Camellia, dahlia, peony, chrysanthemum, poinsettia, bellflower, hellebore, simple wildflower, five-petal bloom, water lily, small rose."],
        ["Foliage &amp; structure", "S-curve vine, broad leaf, narrow leaf, ginkgo fan, oak leaf, holly, evergreen sprig, olive sprig, willow, berry stem, bare branch, reeds, lily pad."],
        ["Story details", "Garden bird, sparrow, butterfly, dragonfly, strawberry, acorn, pinecone, seed head, eight-point star, small lantern, bell, candle, mountain ridge."]]
h, _ = d.table(rows, X, y, [118, W - 118], boldfirst=True)
y -= h + 12
y = d.block("First session: draw 8-10 useful pieces", "Choose one lead flower in three views, two leaf types, two curved stems, one bud and one small filler. Draw at a comfortable size on paper first; line only, no color. Save the first version even if it is imperfect; the repeat test will reveal which shapes are missing.", X, y, W)
y = d.block("Variation that feels natural", "Change the silhouette, opening angle and stem bend. Use mirrored copies only when the lighting and form still make sense. Two independently drawn flower heads usually give a hero more life than many copies of one detailed head. Draw the same camellia once, then let it become a winter, spring, Christmas and garden camellia through color, spacing and companions.", X, y, W)
y = d.block("Reference gathering", "Use your own flower photographs and direct observation for structure. Study historical ornament for rhythm and spacing. For the Korean and East Asian garden influence in your thread, choose specific plants, arrangements or documented references rather than treating a broad region as a single visual style. Generated or drawn references, including the boards in Books 2 and 3, show possibilities; they are not proof of exact botany.", X, y, W)
y = d.block("Line, then color", "Fine, slightly imperfect, tapered lines first. Then decide per motif whether it stays line-only, lightly painted, fully painted, or painted and detailed with ink. That variation is what makes a pattern feel hand-created rather than vector-generated.", X, y, W)
d.box(X, y, W, max(y - BOTTOM - 2, 36), label="The next motif my repeat actually needs", ruled=16)
d.footer()

# =============================================================== 09 CAMELLIA PRACTICE
y = d.new_page("Practice: camellia", "p09")
y = d.header("Practice / camellia", "Continue the flower you already started", "Jade Garden / a signature motif that can also support winter and Christmas")
ytop = y
yy = d.section("Observe the large shapes first", X, ytop, COL)
h1 = d.para("Use a real camellia reference to locate the center, outer cup and overlapping petal groups. Draw the silhouette with five or six broad changes in direction. Add inner petals in loose layers; vary their edges and leave some petals almost unmarked.", X, yy, COL, "body")
yy2 = d.section("Three studies", X + COL + GUTTER, ytop, COL)
h2 = d.para("Front view: a rounded flower with an off-center cluster of stamens. Three-quarter view: a cup with shorter far-side petals. Bud: a small oval interrupted by overlapping sepals. Add one broad leaf and one gently curling leaf.", X + COL + GUTTER, yy2, COL, "body")
y = ytop - max(ytop - yy + h1, ytop - yy2 + h2) - 12
y = d.block("Translate it into your style", "Use the darkest line where petals overlap; lighten the outer edges. Keep veins selective and shading on one side. At thumbnail size, the flower should read as a soft cup before the viewer notices the detail. Tapered stroke ends, consistent weight, no thick outlines, no heavy contouring.", X, y, W)
# reference studies row
d.kicker("Reference studies drawn for this page / front, three-quarter, bud, two leaves", X, y - 6, color=HEAD, size=6.8, spacing=1.0)
y -= 14
rh = 96
d.box(X, y, W, rh, fill=CREAM)
items = [("camellia", {}, "line"), ("camellia", {"view": "three-quarter"}, "line"), ("bud", {}, "line"), ("leaf_simple", {}, "line"), ("leaf_simple", {"kind": "pointed"}, "line")]
for i, (n, kw, md) in enumerate(items):
    cluster(d.c, X + W * (i + 0.5) / 5, y - rh / 2, 34 if i < 3 else 30, "JG", [(n, kw, 0, 0, 1.0, 0 if i < 3 else 30, md)], seed=20 + i)
labels = ["FRONT", "THREE-QUARTER", "BUD", "BROAD LEAF", "CURLING LEAF"]
d.c.setFont("Carlito-Bold", 6.4)
d.c.setFillColor(ACCENT)
for i, lab in enumerate(labels):
    d.c.drawCentredString(X + W * (i + 0.5) / 5, y - rh + 7, lab)
y -= rh + 12
bh = y - BOTTOM - 2
cw3 = (W - 2 * 10) / 3.0
for i, lab in enumerate(["My front view", "My three-quarter view", "My bud and leaves"]):
    d.box(X + i * (cw3 + 10), y, cw3, bh, label=lab)
d.footer()

# =============================================================== 10 DAHLIA PRACTICE
y = d.new_page("Practice: dahlia", "p10")
y = d.header("Practice / dahlia", "Make Harvest Garden feel like autumn", "Begin with structure and rhythm, then add the fine detail")
ytop = y
yy = d.section("Three useful dahlia drawings", X, ytop, COL)
h1 = d.para("Front view: a loose circle with staggered petal rings. Side view: a shallow dome with the underside visible. Bud: a compact oval with small layered tips. Keep the petal tips slightly different so the flower feels grown rather than mechanically assembled.", X, yy, COL, "body")
yy2 = d.section("First repeat idea", X + COL + GUTTER, ytop, COL)
h2 = d.para("Place a large dahlia low on one side and a smaller bloom higher on the other. Connect them with a curved stem. Leave a clear diagonal pocket of background and use an oak leaf or berry cluster to balance it. Build this as a half-drop (page 12).", X + COL + GUTTER, yy2, COL, "body")
y = ytop - max(ytop - yy + h1, ytop - yy2 + h2) - 12
yy = d.section("A 90-minute first session", X, y, W)
rows = [["Minutes", "Do this", "Outcome"],
        ["10", "Observe two or three real dahlia references; notice the nested petal rings and the uneven center.", "A mental map of the structure"],
        ["30", "Three flower studies: front, side and bud. Line only.", "Three usable silhouettes"],
        ["15", "Three oak leaves (silhouette first, then the strongest veins), one seed head.", "Supporting shapes"],
        ["20", "Three composition thumbnails, each about 2 inches square.", "A rhythm to test"],
        ["15", "Choose one thumbnail. Write down the next drawing the repeat needs.", "Next session planned"]]
h, _ = d.table(rows, X, yy, [52, W - 52 - 150, 150], style="tds", hstyle="th")
y = yy - h - 12
d.kicker("Reference studies drawn for this page / front, side, bud, oak leaf, acorn", X, y - 6, color=HEAD, size=6.8, spacing=1.0)
y -= 14
rh = 90
d.box(X, y, W, rh, fill=CREAM)
items = [("dahlia", {}, "line"), ("dahlia", {"view": "side"}, "line"), ("dahlia", {"view": "bud"}, "line"), ("oak_leaf", {}, "line"), ("acorn", {}, "line")]
for i, (n, kw, md) in enumerate(items):
    cluster(d.c, X + W * (i + 0.5) / 5, y - rh / 2, 32 if i < 4 else 24, "HG", [(n, kw, 0, 0, 1.0, 0, md)], seed=30 + i)
d.c.setFont("Carlito-Bold", 6.4)
d.c.setFillColor(ACCENT)
for i, lab in enumerate(["FRONT", "SIDE", "BUD", "OAK LEAF", "ACORN"]):
    d.c.drawCentredString(X + W * (i + 0.5) / 5, y - rh + 7, lab)
y -= rh + 12
bh = y - BOTTOM - 2
d.box(X, y, W * 0.5, bh, label="Flower structure")
tw = W * 0.5 - 10
sm = (tw - 2 * 6) / 3.0
for i in range(3):
    d.box(X + W * 0.5 + 10 + i * (sm + 6), y, sm, sm, label="Thumbnail %d" % (i + 1))
d.box(X + W * 0.5 + 10, y - sm - 8, tw, bh - sm - 8, label="Chosen thumbnail / next drawing needed", ruled=16)
d.footer()

# =============================================================== 11 ILLUSTRATOR WORKFLOW
y = d.new_page("Illustrator workflow", "p11")
y = d.header("Illustrator workflow", "Keep the hand in the artwork", "Choose the conversion method that preserves the drawing’s character")
rows = [["Source", "Practical approach"],
        ["Clean line drawings", "Place a scan or photograph on a locked reference layer. Redraw the important contours with editable paths. Use fewer, more deliberate anchor points, rounded caps and joins, and test the silhouette with a solid fill."],
        ["Image Trace", "Place the raster artwork, open Window &gt; Image Trace and try a suitable preset. Inspect corners, small gaps and thin strokes before expanding a copy. Trace can speed conversion, but the result still needs visual judgment and cleanup. [A3]"],
        ["Painted artwork", "Preserve useful watercolor or gouache texture as raster artwork when the intended workflow supports it. Keep a high-resolution source and check effective resolution at the final output size. Vector conversion is a choice, not a requirement for every style."]]
h, _ = d.table(rows, X, y, [118, W - 118], boldfirst=True)
y -= h + 12
y = d.block("A tidy working file", "Use separate layers for source reference, clean motifs, color studies and repeat development, for example: Background; Florals large; Florals medium; Florals small; Foliage and vines; Details; Repeat guide (locked). Name related objects or groups. Keep an untouched original before recoloring, tracing or expanding strokes.", X, y, W)
y = d.block("From motifs to a pattern swatch", "Select the artwork and use Object &gt; Pattern &gt; Make. Adjust the tile in Pattern Options, review enough copies to reveal gaps, then choose Done to save the pattern to Swatches. Test it in a large filled rectangle and export a tile to check, because the live preview is not the final proof. [A1]", X, y, W)
y = d.block("Delivery copies", "Expand strokes or flatten only a delivery copy, and only when a buyer needs it; the master stays editable. Ask the actual printer or buyer for color mode, profile, size, resolution and file format instead of assuming one universal export setting.", X, y, W)
d.box(X, y, W, 58, label="Stop and fix before moving on", fill=CREAM)
d.para("Dense tracing noise, pinhole gaps, doubled strokes, tiny detached shapes, or shading that becomes muddy at the intended print size.", X + 10, y - 24, W - 20, "body")
y -= 58 + 12
d.box(X, y, W, max(y - BOTTOM - 2, 36), label="My conversion test / what the scan lost and what I redrew", ruled=16)
d.footer()

# =============================================================== 12 REPEAT CONSTRUCTION
y = d.new_page("Repeat construction", "p12")
y = d.header("Repeat construction", "A true half-drop repeat", "Adjacent columns move vertically by half the repeat height")
h = d.para("For a tile of width W and height H, the lattice is generated by two translations: the vertical step (0, H) and the column step (W, H/2). Place the next column W to the right and half a tile height up or down. The offset belongs to the entire repeated tile, including all edge-crossing motifs; a half-drop is not made by shifting an arbitrary half of the motifs inside a straight tile.", X, y, W, "body")
y -= h + 10
# ---- diagram
dg_top = y
tile = 54
cols_n, rows_n = 4, 3
dx0 = X + 46
dy0 = y - 20 - tile * rows_n - tile * 0.5
c = d.c
import random as _r
pen = M.Pen(c, _r.Random(5))
pen.cols = __import__("grs_boards").scheme_colors("JG")
pen.ground = PAPER
pen.lw = 0.5
for i in range(cols_n):
    off = (tile * 0.5) if i % 2 else 0
    for j in range(rows_n):
        tx = dx0 + i * tile
        ty = dy0 + j * tile + off
        c.setStrokeColor(RULE)
        c.setLineWidth(0.6)
        c.setFillColor(CREAM if i % 2 == 0 else PARCH)
        c.rect(tx, ty, tile, tile, stroke=1, fill=1)
        # the same two shapes in every tile
        pen.rng = _r.Random(7)
        pen.place(M.camellia, tx + tile * 0.36, ty + tile * 0.6, tile * 0.19, 0, False, "paint")
        pen.rng = _r.Random(8)
        pen.place(M.leaf_simple, tx + tile * 0.72, ty + tile * 0.28, tile * 0.16, 35, False, "light")
c.setFillColor(BODY)
c.setFont("Carlito-Bold", 7)
for i in range(cols_n):
    c.drawCentredString(dx0 + i * tile + tile / 2, dy0 - 11, "Column %d" % (i + 1))
# dimension arrows
def dim_h(x1, x2, yy, label):
    c.setStrokeColor(HEAD)
    c.setLineWidth(0.6)
    c.line(x1, yy, x2, yy)
    c.line(x1, yy - 3, x1, yy + 3)
    c.line(x2, yy - 3, x2, yy + 3)
    c.setFillColor(HEAD)
    c.setFont("Carlito-Bold", 7.5)
    c.drawCentredString((x1 + x2) / 2, yy + 4, label)
def dim_v(xx, y1, y2, label, side=1):
    c.setStrokeColor(HEAD)
    c.setLineWidth(0.6)
    c.line(xx, y1, xx, y2)
    c.line(xx - 3, y1, xx + 3, y1)
    c.line(xx - 3, y2, xx + 3, y2)
    c.setFillColor(HEAD)
    c.setFont("Carlito-Bold", 7.5)
    if side > 0:
        c.drawString(xx + 5, (y1 + y2) / 2 - 2.5, label)
    else:
        c.drawRightString(xx - 5, (y1 + y2) / 2 - 2.5, label)
dim_h(dx0, dx0 + tile, dy0 + rows_n * tile + 8, "W")
dim_v(dx0 - 8, dy0, dy0 + tile, "H", side=-1)
dim_v(dx0 + 2 * tile - 8 + 0, dy0 + 0, dy0 + tile * 0.5, "H/2", side=-1)
dim_v(dx0 + cols_n * tile + 8, dy0 + tile * 0.5, dy0 + tile * 0.5 + tile, "H", side=1)
# straight-repeat export rectangle on the right
ex0 = dx0 + cols_n * tile + 70
ey0 = dy0 + 20
et = 46
c.setStrokeColor(RULE)
c.setLineWidth(0.6)
for i in range(2):
    for j in range(2):
        c.setFillColor(CREAM if i % 2 == 0 else PARCH)
        c.rect(ex0 + i * et, ey0 + j * et + (et * 0.5 if i % 2 else 0), et, et, stroke=1, fill=1)
c.setStrokeColor(HEAD)
c.setLineWidth(1.0)
c.setDash(3, 2)
c.rect(ex0, ey0 + et * 0.5, 2 * et, et, stroke=1, fill=0)
c.setDash()
c.setFillColor(HEAD)
c.setFont("Carlito-Bold", 7.5)
c.drawCentredString(ex0 + et, ey0 + 2 * et + et * 0.5 + 8, "2W x H straight tile")
c.setFont("Carlito", 7)
c.setFillColor(BODY)
c.drawCentredString(ex0 + et, ey0 - 10, "dashed: 48 x 24 in export")
y = dy0 - 26
h = d.para("The same two shapes repeat in every tile. Every other column is shifted by half a tile height. The diagram shows placement geometry, not a finished pattern; it was drawn with exact vector tools, not generated.", X, y, W, "note")
y -= h + 10
ytop = y
yy = d.section("Example: 24 x 24 inch base tile", X, ytop, COL)
h1 = d.para("One column step is 24 inches across and 12 inches vertically. One vertical step is 24 inches. A two-column straight-repeat export can use a 48 x 24 inch bounding rectangle, with all boundary-crossing artwork duplicated correctly, then tested as its own tile.", X, yy, COL, "body")
yy2 = d.section("In Illustrator", X + COL + GUTTER, ytop, COL)
h2 = d.para("In Pattern Options, choose Brick by Column and a 1/2 brick offset, then set the width and height. Inspect the actual full preview. Duplicate edge-crossing artwork to the opposite edge of the tile and test the exported tile, not only the live preview. [A2]", X + COL + GUTTER, yy2, COL, "body")
y = ytop - max(ytop - yy + h1, ytop - yy2 + h2) - 10
d.box(X, y, W, 50, label="Proof", fill=CREAM)
d.para("Inspect at least a 3 x 3 repeat area for accidental diagonals, gaps, clumps, motif collisions and edge mismatches. For a buyer who needs a straight rectangular tile, export and test that tile separately.", X + 10, y - 24, W - 20, "body")
y -= 50 + 12
d.box(X, y, W, max(y - BOTTOM - 2, 36), label="My tile size / offset / what the 3 x 3 proof revealed", ruled=16)
check(y, "repeat")
d.footer()

# =============================================================== 13 PRODUCTION CHECKLIST
y = d.new_page("Production checklist", "p13")
y = d.header("Production checklist", "Make the artwork easy to review", "Keep editable masters and prepare only the deliverables a buyer needs")
items = ["The pattern fills a large rectangle without seams, clipped strokes or missing edge elements; the exported tile was tested, not only the live preview.",
         "Motif scale and direction suit the proposed product; a ruler or labeled dimension appears in the proof.",
         "Small details remain readable at actual print size. Fine lines have been tested on the intended material.",
         "The collection has clear design IDs, names and colorway labels that match the presentation.",
         "An editable master is saved with organized layers, linked assets and a version number.",
         "The export uses the buyer’s requested dimensions, color mode, profile and resolution; nothing was assumed.",
         "A separate expanded or flattened copy is supplied only when requested; the master remains editable.",
         "The presentation includes the repeat preview, palette, pattern roles and selected product applications.",
         "Every preview is labeled honestly: concept, mockup or finished artwork."]
for it in items:
    h = d.check_item(it, X, y, W)
    y -= h + 7
y -= 6
yy = d.section("Suggested working filenames", X, y, W)
d.box(X, yy, W, 50, fill=CREAM)
d.c.setFont("Carlito", 9)
d.c.setFillColor(BODY)
for i, fn in enumerate(["GRS_HG01_AutumnGarden_Cream_v01.ai", "GRS_HG01_AutumnGarden_Cream_RepeatProof.pdf", "GRS_HG_CollectionOverview_v01.pdf"]):
    d.c.drawString(X + 12, yy - 16 - i * 13, fn)
y = yy - 50 - 10
h = d.para("Studio prefix, print ID, working name, colorway and version: a name a buyer can read without opening the file. Keep an untouched master beside every delivery copy.", X, y, W, "note")
y -= h + 10
d.box(X, y, W, max(y - BOTTOM - 2, 36), label="Product / repeat size / color profile / export requirements for this buyer", ruled=16)
d.footer()

# =============================================================== 14 PRODUCT ADAPTATION
y = d.new_page("Product adaptation", "p14")
y = d.header("Product adaptation", "Show the same story at different scales", "Choose one primary product before creating every possible variation")
rows = [["Application", "What to test"],
        ["Wallpaper", "Test a full wall view and a close crop. Check whether large motifs repeat too obviously and whether vertical forms feel natural across seams. Confirm the manufacturer’s repeat, panel width and match requirements before fixing the tile size."],
        ["Textiles", "Test the fabric in the intended use: a dress, quilt, cushion or curtain. Pair a hero with quieter supports. Check directional motifs, cutting loss and whether fine lines survive the fabric texture and dye process."],
        ["Stationery", "Test the actual item size. A flower that works on a wall may overwhelm a small card. Try a small repeat on an envelope liner, a cropped hero on a notebook, and a micro print on a tag."],
        ["Children &amp; story", "Try open spacing, gentler contrast and clear bird or butterfly silhouettes. Keep the studio’s sophisticated linework so the design grows with the child. Let the product and buyer determine scale instead of assuming every children’s design must be tiny."]]
h, _ = d.table(rows, X, y, [118, W - 118], boldfirst=True)
y -= h + 12
rows = [["Role", "Starting motif size study", "Typical first repeat study"],
        ["Hero", "3-5 in", "18-24 in"], ["Secondary", "1.5-3 in", "12-16 in"], ["Coordinate", "0.4-1.2 in", "6-12 in"],
        ["Blender", "0.2-0.6 in rhythm", "4-8 in"], ["Micro", "0.15-0.35 in marks", "4-6 in"]]
yy = d.section("Starting scale studies used in Books 2 and 3", X, y, W)
h, _ = d.table(rows, X, yy, [118, 200, W - 318], style="tds", hstyle="th")
y = yy - h - 8
h = d.para("All motif dimensions in the collection guides are exploratory textile and paper studies, not universal wallpaper specifications. Final repeat size and production specifications depend on the selected product and manufacturer; the O Holy Night briefs carry their own more specific studies.", X, y, W, "note")
y -= h + 12
d.box(X, y, W, max(y - BOTTOM - 2, 36), label="Primary product / intended motif size / first proof to make", ruled=16)
d.footer()

# =============================================================== 15 TIME AND CAPACITY
y = d.new_page("Time and capacity", "p15")
y = d.header("Time and capacity", "A fifteen-hour studio week", "Make progress repeatable before making the production target bigger")
rows = [["Work block", "Time", "Purpose"],
        ["Drawing &amp; design", "7 h", "Two focused sessions for motif development and the current hero or supporting print."],
        ["Refinement &amp; proofing", "3 h", "Repeat edges, scale tests, palette cleanup, file organization and a small printed proof."],
        ["Buyer research &amp; outreach", "3 h", "Study relevant product ranges, identify the right submission route, tailor a pitch or follow up."],
        ["Planning &amp; administration", "2 h", "Update the tracker, record expenses and income, back up work and choose the next week’s deliverable."],
        ["Total", "15 h", "A starting allocation to revise after two recorded weeks."]]
h, _ = d.table(rows, X, y, [140, 50, W - 190], boldfirst=True, extra=[("FONTNAME", (0, 5), (-1, 5), "Carlito-Bold")])
y -= h + 12
y = d.block("Protect a minimum finished outcome", "Name one concrete deliverable each week: three cleaned camellias, a tested hero repeat, or a collection pitch sheet. Use a “next” list for ideas that arrive while you are finishing the current work. One finished pattern a day and several complete collections in two weeks are not realistic at fifteen hours; the earlier plan that implied them has been replaced.", X, y, W)
y = d.block("A realistic first collection", "A ten-print range uses shared drawings. The two heroes need the most attention; coordinates extract smaller ideas from that motif library. Treat the first 12 weeks as a test of your actual pace, then revise the schedule using recorded hours rather than hopes.", X, y, W)
yy = d.section("A sample week to adapt", X, y, W)
rows = [["Day", "Block", "Hours"], ["Mon", "Drawing: motifs for the current print", "2"], ["Wed", "Drawing: hero or supporting print", "2"], ["Thu", "Drawing, then refinement and proofing", "3 + 1"],
        ["Sat", "Proofing, file cleanup, product test", "2"], ["Sun", "Buyer research, outreach or follow-up", "3"], ["Flexible", "Planning, tracker, backups", "2"]]
h, _ = d.table(rows, X, yy, [70, W - 70 - 60, 60], style="tds", hstyle="th")
y = yy - h - 12
d.box(X, y, W, max(y - BOTTOM - 2, 36), label="This week’s finished outcome / my actual time blocks", ruled=16)
d.footer()

# =============================================================== 16 TWELVE-WEEK ROADMAP
y = d.new_page("Twelve-week roadmap", "p16")
y = d.header("Twelve-week roadmap", "One finished collection, ready to show", "180 planned hours at 15 hours per week; revise after the first two weeks")
rows = [["Weeks", "Main work", "Completion evidence", "Done"],
        ["1-2", "Choose Harvest Garden or Jade Garden. Draw and digitize a compact 8-10 piece motif library and confirm the palette subset. Build the first hero thumbnails.", "Approved direction, palette and 8-10 useful motifs.", ""],
        ["3-4", "Finish hero 1 and build hero 2 using a genuinely different composition. Start a targeted buyer-fit research list.", "Two readable hero repeats and initial product tests.", ""],
        ["5-6", "Develop two secondary prints and the first two coordinates. Reuse motifs with changed rhythm and scale.", "Six related designs with clear roles.", ""],
        ["7-8", "Finish the remaining coordinates, the blender and the micro. Proof the whole range at two scales.", "Ten finished designs with clean repeat proofs.", ""],
        ["9-10", "Prepare a concise collection presentation, a portfolio page and product applications. Verify submission routes for ten carefully researched prospects.", "A shareable portfolio and 10 researched prospects.", ""],
        ["11-12", "If the artwork is ready, send a small tailored batch; record responses and schedule appropriate follow-up. Improve the portfolio from actual evidence.", "Recorded outreach and one clear next improvement.", ""]]
h, t = d.table(rows, X, y, [46, 272, W - 46 - 272 - 36, 36], boldfirst=True)
# checkboxes in the last column
rowh = t._rowHeights
yy = y - rowh[0]
for i in range(1, len(rows)):
    d.checkbox(X + W - 36 + 12, yy - 6)
    yy -= rowh[i]
y -= h + 12
h = d.para("Aim for five tailored first contacts in the initial outreach batch if the collection is ready. That is a controllable activity target, not a promise of sales or of a deal within any number of days. If the artwork needs longer, adjust the dates and keep the scope focused on one finished range.", X, y, W, "body")
y -= h + 12
yy = d.section("Hours actually recorded", X, y, W)
rows = [["Week"] + [str(i) for i in range(1, 13)], ["Hours"] + [""] * 12, ["Main output"] + [""] * 12]
h, _ = d.table(rows, X, yy, [64] + [(W - 64) / 12.0] * 12, style="tds", hstyle="th", extra=[("TOPPADDING", (0, 1), (-1, -1), 12), ("BOTTOMPADDING", (0, 1), (-1, -1), 12)])
y = yy - h - 12
d.box(X, y, W, max(y - BOTTOM - 2, 36), label="What I will change after week two", ruled=16)
d.footer()

# =============================================================== 17 CHOOSE THE FIRST RANGE
y = d.new_page("Choose the first range", "p17")
y = d.header("Choose the first range", "Use momentum to make the decision", "The latest thread favors Harvest Garden; Jade Garden already has sketching momentum")
rows = [["Starting option", "Why it may be right now", "Immediate action"],
        ["Harvest Garden (Book 2, page 3)", "Choose this if you want to follow the recent fall and Christmas direction and are excited to draw dahlias, autumn leaves and seed heads. It leads naturally into Christmas at the Garden, which shares the cream ground and berry accents.", "First session: one dahlia in three views, two oak leaves and three layout thumbnails (page 10)."],
        ["Jade Garden (Book 2, page 18)", "Choose this if your camellia drawings are already developing and you want the studio’s evergreen signature range first. The camellia then carries into Winter Garden and Camellia Noel without new drawing.", "First session: select the best camellia study, add two buds and a flowing stem, then test a hero layout (page 9)."]]
h, _ = d.table(rows, X, y, [118, 250, W - 368], boldfirst=True)
y -= h + 12
y = d.block("Decision rule", "Choose the range you can return to for several weeks. A seasonal theme does not guarantee a near-term buying opportunity; many companies plan seasonal ranges a year or more ahead, so ask each prospect about its development calendar rather than assuming the current season is open. Keep the other range as the next project, with only a small reference folder for now.", X, y, W)
y = d.block("Either way", "Both ranges use the same line quality, the same cream ground and the same approach to space, so the motif library you build first will be reused. Finishing one ten-print range well is worth more to a buyer than two half-finished ranges.", X, y, W)
bh = (y - BOTTOM - 10) / 2.0
d.box(X, y, W, bh, label="I will finish / because / by the end of week", ruled=16)
y -= bh + 10
d.box(X, y, W, bh, label="What I will postpone until this range is ready", ruled=16)
d.footer()

# =============================================================== 18 OFFER AND PORTFOLIO
y = d.new_page("Offer and portfolio", "p18")
y = d.header("Offer and portfolio", "Make the next conversation easy", "A buyer should understand the artwork, product fit and next step quickly")
y = d.block("The initial offer", "Present a coherent collection available for a defined licensing discussion; that is the emphasis you chose. A custom adaptation or commissioned design can be a separate offer with its own scope, timeline, revisions and fee. Keep print-on-demand experiments optional until the core portfolio is ready. Licensing is your chosen starting point, not a claim that it is universally the easiest or fastest route.", X, y, W)
yy = d.section("A compact collection presentation", X, y, W)
rows = [["Page", "Contents"],
        ["1", "Collection name, a large hero, a short story and your contact details."],
        ["2", "All designs with IDs and roles, plus the named palette subset."],
        ["3", "Two or three credible product applications and close views that show the drawing quality."],
        ["4 (optional)", "A second colorway or product-specific variation if it answers a buyer’s need."]]
h, _ = d.table(rows, X, yy, [80, W - 80], style="td", hstyle="th", boldfirst=True)
y = yy - h - 12
y = d.block("A portfolio home", "Start with a small, organized set of finished work. Give each collection its own overview. Include a short artist introduction, a clear contact route, and an invitation to request the current availability sheet. Label concepts, mockups and finished artwork accurately. A simple site with Home, Collections, About, Licensing and Contact is enough.", X, y, W)
y = d.block("Before you show a range", "Replace the drawn concept boards in Books 2 and 3 with your own finished artwork. Check every preview against the actual repeat file. Keep a record of which designs and product categories are available so the portfolio does not promise rights already committed elsewhere.", X, y, W)
d.box(X, y, W, max(y - BOTTOM - 2, 36), label="My collection presentation will include", ruled=16)
d.footer()

# =============================================================== 19 BUYER RESEARCH
y = d.new_page("Buyer research", "p19")
y = d.header("Buyer research", "Look for a specific product fit", "Research the work a company actually publishes and how it receives submissions")
ytop = y
yy = d.section("A useful prospect record", X, ytop, COL)
h1 = d.para("Record the company, product category, visual fit, current submission instructions, verified contact or official form, relevant collection, date contacted and next action. Save the source URL and date checked. Prefer a small relevant list to an unfiltered list of names. No real buyer contacts are supplied in this workbook; each entry must come from your own verified research.", X, yy, COL, "body")
yy2 = d.section("Three questions before a first contact", X + COL + GUTTER, ytop, COL)
h2 = d.para("Does this company use original surface artwork in my product category? Does my collection complement its visual range without duplicating it? Can I identify a legitimate submission route and explain the fit in one sentence?", X + COL + GUTTER, yy2, COL, "body")
y = ytop - max(ytop - yy + h1, ytop - yy2 + h2) - 12
rows = [["Prospect / source", "Product + fit", "Collection / ID", "Stage", "Next action / date"]] + [[""] * 5 for _ in range(9)]
h, _ = d.table(rows, X, y, [128, 130, 90, 60, W - 408], style="tds", hstyle="th", extra=[("TOPPADDING", (0, 1), (-1, -1), 11), ("BOTTOMPADDING", (0, 1), (-1, -1), 11)])
y -= h + 10
h = d.para("Stages: researching, ready to contact, sent, follow-up due, conversation, closed or future. A lack of reply is a result to record, not a reason to abandon the style after one batch.", X, y, W, "note")
y -= h + 6
h = d.para("Review submission guidelines before sending files. Tailor the first message and use the requested file size, link or form. Respect each company’s stated calendar. No outreach has been sent as part of creating this workbook.", X, y, W, "note")
y -= h
check(y, "buyer research")
d.footer()

# =============================================================== 20 OUTREACH DRAFTS
y = d.new_page("Outreach drafts", "p20")
y = d.header("Outreach drafts", "A short, specific introduction", "Replace every bracketed field and use the recipient’s submission instructions; nothing here has been sent")
email1 = ("<b>Subject:</b> [Collection name] botanical patterns for [product category]<br/><br/>"
          "Hello [name],<br/><br/>"
          "I’m Alisha, the artist behind Ginger Root Studio. I create hand-drawn botanical patterns with warm color and graceful, nature-inspired details.<br/><br/>"
          "I thought my [collection name] range could suit [specific product or reason connected to their work]. It includes [number] coordinating designs featuring [two or three key motifs].<br/><br/>"
          "You can view a short collection presentation here: [portfolio link]. If this is relevant to an upcoming range, I’d be happy to share current availability and discuss your artwork needs.<br/><br/>"
          "Thank you for your time,<br/>Alisha<br/>Ginger Root Studio | [website] | [email]")
p = P(email1, "body")
_, eh = p.wrap(W - 24, 10000)
d.box(X, y, W, eh + 34, label="Initial email draft", fill=CREAM)
p.drawOn(d.c, X + 12, y - 26 - eh)
y -= eh + 34 + 12
email2 = ("Hello [name],<br/><br/>"
          "I’m following up on the [collection name] presentation I shared on [date]. Here is the link again: [portfolio link].<br/><br/>"
          "If you are reviewing artwork for [product category], I’d welcome the chance to learn what themes or timelines you are considering. If someone else handles submissions, please let me know the appropriate route.<br/><br/>"
          "Thank you,<br/>Alisha")
p = P(email2, "body")
_, eh = p.wrap(W - 24, 10000)
d.box(X, y, W, eh + 34, label="Follow-up draft", fill=CREAM)
p.drawOn(d.c, X + 12, y - 26 - eh)
y -= eh + 34 + 12
h = d.para("Follow the company’s stated response window. If none is given, choose a reasonable follow-up date and record it on page 19. Keep the message brief and stop if the recipient declines or asks not to be contacted. The placeholders in brackets are intentional; fill them from your own research.", X, y, W, "body")
y -= h + 10
d.box(X, y, W, max(y - BOTTOM - 2, 36), label="My one-sentence fit statement for this prospect", ruled=16)
d.footer()

# =============================================================== 21 OPPORTUNITY WORKSHEET
y = d.new_page("Opportunity worksheet", "p21")
y = d.header("Opportunity worksheet", "Define the arrangement before delivery", "Questions to prepare a licensing or custom-work discussion; a planning worksheet, not a contract")
rows = [["Topic", "Questions to resolve"],
        ["Artwork", "Which exact design IDs and colorways are included? Are adaptations or new designs part of the scope?"],
        ["Permitted use", "Which products, channels and territories are covered? Is the arrangement exclusive, and if so, within what limits and for how long?"],
        ["Time", "When does use begin and end? What happens at renewal, termination and the end of a sell-off period?"],
        ["Payment", "Is there a fixed fee, royalty, advance or combination? What precisely is the royalty base: retail, wholesale or net sales, and with what deductions? When are statements and payments due? Is an advance recoupable?"],
        ["Production", "Who may alter color or scale? What approvals, samples, file formats and delivery dates are needed?"],
        ["Ownership &amp; credit", "Who owns the original artwork and any new commissioned work? Is any transfer of copyright being requested, and is that intended? What may appear in the artist’s portfolio, and when?"],
        ["Changes &amp; records", "How are revisions, cancellations and additional uses handled? Who keeps the signed agreement and the reports?"]]
h, _ = d.table(rows, X, y, [118, W - 118], boldfirst=True)
y -= h + 12
d.box(X, y, W, max(y - 86 - BOTTOM, 60), label="Questions for this particular opportunity", ruled=16)
y -= max(y - 86 - BOTTOM, 60) + 10
h = d.para("This is a planning worksheet, not contract language. Have material rights and payment terms documented and reviewed before committing. For U.S. copyright background, including what a transfer or assignment means, see the official resources listed on page 25. [C1] [C2]", X, y, W, "note")
y -= h
check(y, "opportunity")
d.footer()

# =============================================================== 22 REVENUE PLANNING
y = d.new_page("Revenue planning", "p22")
y = d.header("Revenue planning", "Use the $100,000 goal as a model", "Illustrative annual gross revenue; not an earnings forecast, a rate benchmark or a deadline for 2026")
h = d.para("The goal in your conversation, $100,000 “by the end of the year”, is ambitious. This page translates it into assumptions you can test with real opportunities. Keep gross revenue, business expenses, taxes and personal take-home pay separate. The figures below are invented planning inputs, not typical market fees and not a probable result.", X, y, W, "body")
y -= h + 10
rows = [["Hypothetical source", "Assumption", "Annual gross"],
        ["Collection licenses", "12 agreements x $5,000", "$60,000"],
        ["Custom projects", "4 projects x $7,500", "$30,000"],
        ["Renewals / extensions", "4 agreements x $2,500", "$10,000"],
        ["Illustrative total", "20 paid agreements / projects", "$100,000"]]
h, _ = d.table(rows, X, y, [180, 220, W - 400], boldfirst=True, extra=[("FONTNAME", (0, 4), (-1, 4), "Carlito-Bold")])
y -= h + 12
y = d.block("What the example actually asks of the business", "Twenty paid agreements in a year from a studio that is still building its first collection. Replace the prices and volumes with the terms of real offers. Count each agreement once; check whether an advance is recoupable before adding it to later royalties as separate revenue.", X, y, W)
rows = [["The time constraint", "Arithmetic"],
        ["Working hours a year", "15 hours x 52 weeks = 780 hours"],
        ["Gross per total working hour", "$100,000 / 780 = about $128.21"],
        ["If half the hours are paid-production capacity", "390 hours; $100,000 / 390 = about $256.41 per production hour"],
        ["A royalty illustration", "At an assumed 5% royalty, $100,000 requires $2,000,000 in the contract’s defined royalty base before other adjustments."]]
h, _ = d.table(rows, X, y, [200, W - 200], style="td", hstyle="th", boldfirst=True)
y -= h + 10
h = d.para("These ratios describe the required business output; they are not suggested hourly prices. Retail sales, wholesale sales and net sales are not interchangeable royalty bases, and the actual rate, base, exclusions and sales volume would need to come from a real agreement.", X, y, W, "body")
y -= h + 10
d.box(X, y, W, 48, label="First milestone", fill=CREAM)
d.para("Finish a strong range, contact relevant buyers and learn from actual responses. Use those facts to revise the revenue model on this page.", X + 10, y - 24, W - 20, "body")
y -= 48 + 10
d.box(X, y, W, max(y - BOTTOM - 2, 30), label="My own assumptions to test", ruled=16)
d.footer()

# =============================================================== 23 MONEY WORKSHEET
y = d.new_page("Money worksheet", "p23")
y = d.header("Money worksheet", "Track evidence month by month", "Choose any starting month; enter actual amounts as they become known")
h = d.para("Use “goal” for the planned figure, “contracted” for agreed fees, “invoiced” for bills issued, and “received” for cash collected. Royalties may arrive later than the product sale, and a recoupable advance is not new income when the royalties that recoup it arrive. Track expenses separately and review the timing of payments as well as the total.", X, y, W, "body")
y -= h + 10
rows = [["Month", "Goal", "Contracted", "Invoiced", "Received", "Expenses"]] + [[str(i)] + [""] * 5 for i in range(1, 13)] + [["Total"] + [""] * 5]
cw = (W - 60) / 5.0
h, _ = d.table(rows, X, y, [60] + [cw] * 5, style="tds", hstyle="th", boldfirst=True, extra=[("TOPPADDING", (0, 1), (-1, -1), 8), ("BOTTOMPADDING", (0, 1), (-1, -1), 8), ("FONTNAME", (0, 13), (-1, 13), "Carlito-Bold"), ("LINEABOVE", (0, 13), (-1, 13), 0.9, HEAD)])
y -= h + 10
d.box(X, y, W, max(y - 40 - BOTTOM, 40), label="What the actual numbers changed in my plan", ruled=16)
y -= max(y - 40 - BOTTOM, 40) + 8
h = d.para("This worksheet is for planning and recordkeeping. The example on page 22 is an annual aspiration; it is not a promise for the remaining months of 2026.", X, y, W, "note")
y -= h
check(y, "money")
d.footer()

# =============================================================== 24 PROJECT TRACKER
y = d.new_page("Project tracker", "p24")
y = d.header("Project tracker", "Finish the ten-print collection", "One row per design; tick each stage only when it is proofed, not when it is started")
y = d.field("Collection", X, y, COL)
d.field("Primary product", X + COL + GUTTER, y + 18, COL)
y -= 6
rows = [["ID", "Role / working name", "Drawn", "Color", "Repeat", "Proof"]]
roles = ["Hero 1", "Hero 2", "Secondary 1", "Secondary 2", "Coordinate 1", "Coordinate 2", "Coordinate 3", "Coordinate 4", "Blender", "Micro"]
for i, r in enumerate(roles):
    rows.append(["%02d" % (i + 1), r, "", "", "", ""])
h, t = d.table(rows, X, y, [40, W - 40 - 4 * 62, 62, 62, 62, 62], style="td", hstyle="th", extra=[("TOPPADDING", (0, 1), (-1, -1), 9), ("BOTTOMPADDING", (0, 1), (-1, -1), 9)])
rowh = t._rowHeights
yy = y - rowh[0]
for i in range(1, len(rows)):
    for k in range(4):
        cx = X + W - 4 * 62 + k * 62 + 31 - 4
        d.checkbox(cx, yy - rowh[i] / 2 + 4)
    yy -= rowh[i]
y -= h + 10
d.box(X, y, W, 86, label="Weekly review: finished / learned / next useful action", ruled=16)
y -= 86 + 10
d.box(X, y, W, max(y - BOTTOM - 2, 40), label="Release check", fill=CREAM)
d.para("A recognizable hero, a distinct second hero, quiet supports, consistent color, clean boundaries, readable scale and a concise presentation. For the expanded O Holy Night range, use the twelve-design lineup in Book 3 (page 4) as the tracker: 3 heroes, 5 secondaries and 4 coordinates.", X + 10, y - 24, W - 20, "body")
d.footer()

# =============================================================== 25 SOURCES AND DECISIONS
y = d.new_page("Sources and decisions", "p25")
y = d.header("Sources and decisions", "What this edition is built on", "Prepared %s / working recommendations for Alisha Bruton" % EDITION_DATE)
rows = [["Ref", "Source", "Used for"],
        ["S1", "Your shared conversation, “Surface Pattern Design Roadmap” (chatgpt.com/share/6ac3b39b-f4cc-83ea-a2f7-72cc6b6d1a59), plus the original O Holy Night brief.", "Studio name, style, palette, collections, Illustrator and time budget; confirmed preferences."],
        ["A1", "Adobe, Create patterns: helpx.adobe.com/illustrator/desktop/paint-and-fill/create-and-edit-patterns/create-patterns.html", "Pattern creation and saving to Swatches."],
        ["A2", "Adobe, Edit patterns: helpx.adobe.com/illustrator/desktop/paint-and-fill/create-and-edit-patterns/edit-patterns.html", "Pattern Options controls, brick offset and tile settings."],
        ["A3", "Adobe, Trace raster artwork: helpx.adobe.com/illustrator/desktop/manage-objects/traces-mockups-symbols/trace-images-to-convert-raster-into-vector-artwork.html", "Image Trace workflow and editable vector conversion."],
        ["A4", "Adobe, Create and open swatch libraries: helpx.adobe.com/in/illustrator/desktop/manage-colors/use-swatches/create-and-open-swatch-libraries.html", "Loading the .ase palette."],
        ["A5", "Adobe, Share swatches between applications: helpx.adobe.com/in/illustrator/desktop/manage-colors/use-swatches/share-swatches-between-applications.html", "Swatch exchange background."],
        ["C1", "U.S. Copyright Office, Visual artists: copyright.gov/engage/visual-artists/", "Official starting point for copyright background."],
        ["C2", "U.S. Copyright Office, Transfers and assignment FAQ: copyright.gov/help/faq/faq-assignment.html", "What a transfer or assignment of rights means."]]
h, _ = d.table(rows, X, y, [30, 330, W - 360], style="tds", hstyle="th", boldfirst=True)
y -= h + 12
y = d.block("Editorial decisions", "This edition uses 23 distinct palette colors; completes the two short collection lists with Orchard Sprig and Pinecone Sprig; retains the earlier twelve-print O Holy Night range inside Sacred Seasons; corrects the half-drop construction; uses the Star of Bethlehem rather than the North Star for the nativity motif; and replaces compressed production promises with a 12-week, 180-hour plan. Revenue examples are explicitly hypothetical. Assistant suggestions in the historical conversation were treated as proposals, not approvals.", X, y, W, "small")
y = d.block("Illustration and artwork status", "The concept boards and motif studies in Books 2 and 3, the studies on pages 9 and 10 and the cover drawings are original vector illustrations produced in code for this planning set; no photographic or AI-painted imagery was available in the production environment, so the boards are deliberately simple line-and-fill drawings. They visualize direction and hierarchy. They are not Alisha’s existing artwork, not verified seamless tiles and not production-ready licensed collections. Sketch pages invite her own interpretation. The written briefs take priority where an image differs in motif, count or color. Earlier style and logo images from the original conversation were not available and are not reproduced or imitated.", X, y, W, "small")
y = d.block("Palette and next decisions", "The .ase file encodes the 23 supplied HEX values as named RGB process colors and was decoded and checked after writing. The earlier seven-color standalone Christmas palette is documented in Book 3 as history only and is not included in the file. Choose the first collection, approve its palette and develop original artwork before using any presentation with buyers. No contact information from the screenshots has been reproduced, and no email, pitch or publication has been sent or made.", X, y, W, "small")
check(y, "sources")
d.footer()

d.save()
for w_ in d.warnings:
    print("WARNING:", w_)
print("book1 pages:", d.page, "->", OUT)
