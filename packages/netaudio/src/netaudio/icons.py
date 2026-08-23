from __future__ import annotations


NERD_FONT_ICONS = {
    "device": "\U000f04c3",
    "tx": "\uf093",
    "rx": "\uf019",
    "ip": "\U000f0a60",
    "mac": "\U000f0237",
    "connected": "\U000f05e0",
    "error": "\U000f0159",
    "warning": "\U000f0026",
    "clock": "\uf017",
    "server": "\U000f048b",
    "channel": "\U000f062e",
    "subscription": "\uf0c1",
    "bug": "\ueaaf",
    "config": "\uf013",
    "capture": "\U000f0100",
    "reboot": "\U000f0709",
    "identify": "\U000f0241",
    "factory_reset": "\U000f020f",
    "sample_rate": "\U000f1479",
    "latency": "\U000f04c5",
    "meter": "\U000f029a",
    "model": "\U000f061a",
    "name": "\U000f0455",
    "success": "\uf00c",
    "fail": "\uf00d",
    "info": "\U000f02fc",
    "bluetooth": "\uf293",
    "firmware": "\U000f061a",
    "encoding": "\U000f0169",
    "aes67": "\U000f0003",
    "session": "\uea83",
    "marker": "\uf041",
    "packet": "\U000f03d3",
    "open": "\uf2fc",
    "closed": "\uf023",
    "reopened": "\U000f006f",
    "lock": "\uf023",
    "unlock": "\uf2fc",
    "online": "\U000f0003",
    "offline": "\U000f05aa",
    "version": "\uf02b",
    "manufacturer": "\U000f020f",
    "board": "\U000f08ae",
    "software": "\U000f08c6",
    "bit_depth": "\U000f03a0",
    "flow": "\U000f04e1",
    "gain": "\U000f057e",
    "status": "\U000f02fc",
    "direction": "\uf061",
    "remove": "\U000f0376",
    "add": "\U000f0417",
    "last_seen": "\uf06e",
    "role": "\U000f0017",
    "grandmaster": "\U000f01a5",
    "tag": "\uf02b",
    "history": "\U000f02da",
    "context": "\U000f0328",
    "reported": "\uf073",
    "updated": "\U000f06b0",
    "summary": "\uf15c",
    "label": "\U000f0315",
    "friendly_name": "\uebcf",
    "number": "\U000f03a0",
    "timeout": "\U000f051f",
    "receiving": "\U000f1119",
    "level": "\uf012",
    "wall_time": "\uf017",
    "diagnostic": "\U000f04d9",
    "volume_high": "\U000f057e",
    "volume_off": "\U000f0581",
    "volume_mute": "\U000f075f",
    "speaker": "\U000f04c3",
    "speaker_off": "\U000f04c4",
}


def icon(key: str) -> str:
    from netaudio.cli import state

    if not state.icons:
        return ""
    glyph = NERD_FONT_ICONS.get(key, "")
    return f"{glyph} " if glyph else ""


def icon_only(key: str) -> str:
    from netaudio.cli import state

    if not state.icons:
        return ""
    return NERD_FONT_ICONS.get(key, "")


SEVERITY_PRESENTATION = {
    "none": {"icon": None, "shape": "", "color": None},
    "ok": {"icon": "connected", "shape": "●", "color": "32"},
    "info": {"icon": "info", "shape": "•", "color": "36"},
    "progress": {"icon": "clock", "shape": "◷", "color": "33"},
    "warning": {"icon": "warning", "shape": "⚠", "color": "33"},
    "error": {"icon": "error", "shape": "⊘", "color": "31"},
}


def severity_icon(severity: str) -> str:
    from netaudio.cli import state

    presentation = SEVERITY_PRESENTATION.get(severity)
    if presentation is None:
        return ""

    if state.icons:
        glyph = NERD_FONT_ICONS.get(presentation["icon"], "") if presentation["icon"] else ""
        return glyph

    if state.no_color:
        return ""

    shape = presentation["shape"]
    color = presentation["color"]
    if not shape or not color:
        return shape
    return f"\033[{color}m{shape}\033[0m"
