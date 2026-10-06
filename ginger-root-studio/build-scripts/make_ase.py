# -*- coding: utf-8 -*-
"""Write and verify Ginger_Root_Studio_Master_Palette.ase (Adobe Swatch Exchange).

Format: 'ASEF' + version 1.0 + big-endian uint32 block count, then colour-entry blocks:
  uint16 type 0x0001, uint32 payload length, uint16 name length (UTF-16 code units incl. terminator),
  UTF-16BE name + 0x0000, 4-byte model 'RGB ', three big-endian float32 channels, uint16 colour type (2 = normal/process).
"""
import struct
import sys
from grs_data import PALETTE


def hex_to_rgb01(h):
    h = h.lstrip("#")
    return tuple(int(h[i:i + 2], 16) / 255.0 for i in (0, 2, 4))


def write_ase(path, palette):
    blocks = b""
    for name, hexv in palette:
        r, g, b = hex_to_rgb01(hexv)
        name_utf16 = name.encode("utf-16-be") + b"\x00\x00"
        payload = struct.pack(">H", len(name) + 1) + name_utf16 + b"RGB " + struct.pack(">fff", r, g, b) + struct.pack(">H", 2)
        blocks += struct.pack(">HI", 0x0001, len(payload)) + payload
    data = b"ASEF" + struct.pack(">HH", 1, 0) + struct.pack(">I", len(palette)) + blocks
    with open(path, "wb") as f:
        f.write(data)
    return len(data)


def read_ase(path):
    data = open(path, "rb").read()
    assert data[:4] == b"ASEF", "bad signature"
    major, minor, count = struct.unpack(">HHI", data[4:12])
    pos = 12
    out = []
    for _ in range(count):
        btype, blen = struct.unpack(">HI", data[pos:pos + 6])
        pos += 6
        payload = data[pos:pos + blen]
        pos += blen
        assert btype == 0x0001, "not a colour entry"
        nlen = struct.unpack(">H", payload[:2])[0]
        name = payload[2:2 + nlen * 2].decode("utf-16-be").rstrip("\x00")
        q = 2 + nlen * 2
        model = payload[q:q + 4]
        assert model == b"RGB ", model
        r, g, b = struct.unpack(">fff", payload[q + 4:q + 16])
        ctype = struct.unpack(">H", payload[q + 16:q + 18])[0]
        out.append((name, (r, g, b), ctype))
    assert pos == len(data), "trailing bytes"
    return (major, minor, count), out


if __name__ == "__main__":
    out = sys.argv[1] if len(sys.argv) > 1 else "../out/Ginger_Root_Studio_Master_Palette.ase"
    n = write_ase(out, PALETTE)
    ver, entries = read_ase(out)
    assert ver == (1, 0, 23), ver
    assert len(entries) == 23
    for (name, hexv), (rname, rgb, ctype) in zip(PALETTE, entries):
        exp = hex_to_rgb01(hexv)
        assert rname == name, (rname, name)
        assert ctype == 2
        back = "#%02X%02X%02X" % tuple(int(round(ch * 255)) for ch in rgb)
        assert back == hexv, (back, hexv)
        assert all(abs(a - b) < 1e-6 for a, b in zip(rgb, exp))
    print("ASE OK: %d bytes, version %d.%d, %d swatches, all names/values round-trip" % (n, ver[0], ver[1], ver[2]))
    for name, rgb, ctype in entries:
        print("  %-18s #%02X%02X%02X  rgb01=(%.4f, %.4f, %.4f) type=%d" % ((name,) + tuple(int(round(ch * 255)) for ch in rgb) + rgb + (ctype,)))
