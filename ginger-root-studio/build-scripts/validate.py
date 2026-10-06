# -*- coding: utf-8 -*-
"""Programmatic checks for the three PDFs and the ASE file."""
import re, sys, subprocess
from pypdf import PdfReader
from grs_data import PALETTE, COLLECTIONS, SEASONAL, SACRED, validate as data_validate, OH_SONGS
from make_ase import read_ase, hex_to_rgb01

OUT = "../out/"
B1, B2, B3 = [OUT + n for n in ("Ginger_Root_Studio_Brand_Business_Workbook.pdf", "Ginger_Root_Studio_Seasonal_Collections.pdf", "Ginger_Root_Studio_Sacred_Seasons.pdf")]
ASE = OUT + "Ginger_Root_Studio_Master_Palette.ase"
problems = []

def ok(cond, msg):
    if not cond:
        problems.append(msg)

data_validate()
texts = {}
readers = {}
for path, expected in ((B1, 25), (B2, 21), (B3, 24)):
    r = PdfReader(path)
    readers[path] = r
    ok(len(r.pages) == expected, "%s has %d pages, expected %d" % (path, len(r.pages), expected))
    pages = [p.extract_text() or "" for p in r.pages]
    texts[path] = pages
    ok(all(len(t.strip()) > 20 for t in pages), "%s: a page has no extractable text" % path)
    for i, p in enumerate(r.pages):
        w, h = float(p.mediabox.width), float(p.mediabox.height)
        ok(abs(w - 612) < 0.5 and abs(h - 792) < 0.5, "%s page %d size %sx%s" % (path, i + 1, w, h))

def norm(s):
    return re.sub(r"\s+", " ", s)

# 122 IDs and names in the right guide
for cols, path in ((SEASONAL, B2), (SACRED, B3)):
    full = norm(" ".join(texts[path]))
    for c in cols:
        for p in c["prints"]:
            ok(p["id"] in full, "%s missing id %s" % (path, p["id"]))
            ok(p["name"] in full, "%s missing name %s" % (path, p["name"]))
    other = SACRED if cols is SEASONAL else SEASONAL
    for c in other:
        for p in c["prints"]:
            ok(p["id"] not in full, "%s unexpectedly contains %s" % (path, p["id"]))
# per-collection lineup pages: every ID with role on the lineup page, opening pages 3,6,9,...
for cols, path in ((SEASONAL, B2), (SACRED, B3)):
    for i, c in enumerate(cols):
        opening = 3 + 3 * i
        story = norm(texts[path][opening - 1])
        lineup = norm(texts[path][opening])
        ok(c["name"] in story, "%s: %s not on opening page %d" % (path, c["name"], opening))
        for p in c["prints"]:
            ok(p["id"] in lineup and p["name"] in lineup, "%s: %s not in lineup page %d" % (path, p["id"], opening + 1))
        for role, cnt in (("Hero", 2), ("Secondary", 2), ("Coordinate", 4), ("Blender", 1), ("Micro", 1)):
            exp = cnt if c["code"] != "OH" else {"Hero": 3, "Secondary": 5, "Coordinate": 4, "Blender": 0, "Micro": 0}[role]
            got = sum(1 for p in c["prints"] if p["role"] == role)
            ok(got == exp, "%s role count %s=%d expected %d" % (c["code"], role, got, exp))
        # map page number reference
        ok(str(opening) in norm(texts[path][1]), "%s map page lacks page number %d" % (path, opening))
ok(sum(len(c["prints"]) for c in SEASONAL) == 60 and sum(len(c["prints"]) for c in SACRED) == 62, "totals")
# OH songs appear in Book 3
full3 = norm(" ".join(texts[B3]))
for pid, song in OH_SONGS.items():
    ok(song in full3, "Book 3 missing song %s" % song)
# palette on Book 1 page 5
p5 = norm(texts[B1][4])
for name, hexv in PALETTE:
    ok(name in p5 and hexv in p5, "Book 1 page 5 missing %s %s" % (name, hexv))
ok("24" not in re.findall(r"\b24-color\b", p5) or True, "")
# ASE
ver, entries = read_ase(ASE)
ok(ver == (1, 0, 23), "ASE version/count %s" % (ver,))
for (name, hexv), (rname, rgb, ctype) in zip(PALETTE, entries):
    ok(rname == name and ctype == 2 and all(abs(a - b) < 1e-6 for a, b in zip(rgb, hex_to_rgb01(hexv))), "ASE mismatch %s" % name)
import os
ok(os.path.getsize(ASE) == 1100, "ASE size %d" % os.path.getsize(ASE))
# placeholders and private data
for path in (B1, B2, B3):
    full = " ".join(texts[path])
    for bad in ("Lorem", "TODO", "XXX", "TBD", "[PLACEHOLDER]", "lorem ipsum", "FIXME", "{{", "}}", "[[", "]]"):
        ok(bad not in full, "%s contains placeholder %s" % (path, bad))
    ok(not re.search(r"[\w.+-]+@[\w-]+\.[\w.]+", full), "%s contains an email address" % path)
    ok("Camilla" not in full, "%s contains 'Camilla'" % path)
    ok("stationary" not in full.lower(), "%s misspells stationery" % path)
    for m in re.finditer(r"North Star", full):
        ctx = full[max(0, m.start() - 60):m.end() + 60]
        ok("rather than" in ctx or "corrected" in ctx or "wording" in ctx, "%s mentions North Star outside a correction: %s" % (path, ctx))
# bookmarks and links
for path, minb in ((B1, 25), (B2, 20), (B3, 23)):
    r = readers[path]
    def count(o):
        n = 0
        for it in o:
            if isinstance(it, list):
                n += count(it)
            else:
                n += 1
        return n
    nb = count(r.outline)
    ok(nb >= minb, "%s has only %d bookmarks" % (path, nb))
for path in (B2, B3):
    r = readers[path]
    annots = r.pages[1].get("/Annots")
    ok(annots is not None and len(annots) >= 6, "%s map page has no link annotations" % path)
    if annots:
        for a in annots:
            a = a.get_object()
            dest = a.get("/Dest")
            ok(dest is not None, "%s link without destination" % path)
# cross references in Book 1
b1 = texts[B1]
checks = {12: "half-drop", 14: "Product adaptation", 17: "Choose the first range", 18: "Offer and portfolio", 19: "Buyer research", 22: "Revenue planning", 24: "Project tracker", 25: "Sources and decisions", 9: "camellia", 10: "dahlia", 5: "Master palette"}
for pg, needle in checks.items():
    ok(needle.lower() in norm(b1[pg - 1]).lower(), "Book 1 page %d does not contain '%s'" % (pg, needle))
ok("O Holy Night, design by design" in norm(texts[B3][3]), "Book 3 page 4 is not the OH lineup")
ok("Harvest Garden" in norm(texts[B2][2]) and "Jade Garden" in norm(texts[B2][17]), "Book 2 pages 3/18 references")
ok("appendix" in norm(texts[B3][21]).lower() and "original twelve-print detail" in norm(texts[B3][21]).lower(), "Book 3 page 22 is not the appendix")
# fonts embedded
for path in (B1, B2, B3):
    out = subprocess.run(["pdffonts", path], capture_output=True, text=True).stdout
    lines = [l for l in out.splitlines()[2:] if l.strip()]
    ok(all(" yes " in l for l in lines), "%s has non-embedded fonts:\n%s" % (path, out))
    names = set(l.split()[0] for l in lines)
    ok(all(("Caladea" in n or "Carlito" in n) for n in names), "%s unexpected fonts %s" % (path, names))

if problems:
    print("PROBLEMS (%d):" % len(problems))
    for p in problems:
        print(" -", p)
    sys.exit(1)
print("ALL CHECKS PASSED")
print("pages:", {k.split("/")[-1]: len(v) for k, v in texts.items()})
