from __future__ import annotations


NERD_FONT_ICONS = {
    "device": "\U000f04c3",       # nf-md-speaker
    "tx": "\uf093",               # nf-fa-upload
    "rx": "\uf019",               # nf-fa-download
    "ip": "\U000f0a60",           # nf-md-ip_network
    "mac": "\U000f0237",          # nf-md-fingerprint
    "connected": "\U000f05e0",    # nf-md-check_circle
    "error": "\U000f0159",        # nf-md-close_circle
    "warning": "\U000f0026",      # nf-md-alert
    "clock": "\uf017",            # nf-fa-clock
    "server": "\U000f048b",       # nf-md-server
    "channel": "\U000f062e",      # nf-md-tune
    "subscription": "\uf0c1",     # nf-fa-link
    "bug": "\ueaaf",              # nf-cod-bug
    "config": "\uf013",           # nf-fa-cog
    "capture": "\U000f0100",      # nf-md-camera
    "reboot": "\U000f0709",       # nf-md-restart
    "identify": "\U000f0241",     # nf-md-flash
    "factory_reset": "\U000f020f", # nf-md-factory
    "sample_rate": "\U000f1479",  # nf-md-cosine_wave
    "latency": "\U000f04c5",      # nf-md-speedometer
    "meter": "\U000f029a",        # nf-md-gauge
    "model": "\U000f061a",        # nf-md-chip
    "name": "\U000f0455",         # nf-md-rename_box
    "success": "\uf00c",          # nf-fa-check
    "fail": "\uf00d",             # nf-fa-close
    "info": "\U000f02fc",         # nf-md-information
    "bluetooth": "\uf293",        # nf-fa-bluetooth
    "firmware": "\U000f061a",     # nf-md-chip
    "encoding": "\U000f0169",     # nf-md-code_braces
    "aes67": "\U000f0003",        # nf-md-access_point
    "session": "\uea83",          # nf-cod-folder
    "marker": "\uf041",           # nf-fa-map_marker
    "packet": "\U000f03d3",       # nf-md-package
    "open": "\uf2fc",             # nf-fa-lock_open
    "closed": "\uf023",           # nf-fa-lock
    "reopened": "\U000f006f",     # nf-md-backup_restore
    "lock": "\uf023",              # nf-fa-lock
    "unlock": "\uf2fc",            # nf-fa-lock_open
    "online": "\U000f0003",       # nf-md-access_point
    "offline": "\U000f05aa",      # nf-md-wifi_off
    "version": "\uf02b",          # nf-fa-tag
    "manufacturer": "\U000f020f", # nf-md-factory
    "board": "\U000f08ae",        # nf-md-expansion_card
    "software": "\U000f08c6",     # nf-md-application
    "bit_depth": "\U000f03a0",    # nf-md-numeric
    "flow": "\U000f04e1",         # nf-md-swap_horizontal
    "gain": "\U000f057e",         # nf-md-volume_high
    "status": "\U000f02fc",       # nf-md-information
    "direction": "\uf061",        # nf-fa-arrow_right
    "remove": "\U000f0376",       # nf-md-minus_circle
    "add": "\U000f0417",          # nf-md-plus_circle
    "last_seen": "\uf06e",        # nf-fa-eye
    "role": "\U000f0017",         # nf-md-account_star
    "grandmaster": "\U000f01a5",  # nf-md-crown
    "tag": "\uf02b",              # nf-fa-tag
    "history": "\U000f02da",      # nf-md-history
    "context": "\U000f0328",      # nf-md-layers
    "reported": "\uf073",         # nf-fa-calendar
    "updated": "\U000f06b0",      # nf-md-update
    "summary": "\uf15c",          # nf-fa-file_text
    "label": "\U000f0315",        # nf-md-label
    "friendly_name": "\uebcf",    # nf-cod-wand
    "number": "\U000f03a0",       # nf-md-numeric
    "timeout": "\U000f051f",      # nf-md-timer_sand
    "receiving": "\U000f1119",    # nf-md-antenna
    "level": "\uf012",            # nf-fa-signal
    "wall_time": "\uf017",        # nf-fa-clock
    "diagnostic": "\U000f04d9",   # nf-md-stethoscope
    "volume_high": "\U000f057e",  # nf-md-volume_high
    "volume_off": "\U000f0581",   # nf-md-volume_off
    "volume_mute": "\U000f075f",  # nf-md-volume_mute
    "speaker": "\U000f04c3",      # nf-md-speaker
    "speaker_off": "\U000f04c4",  # nf-md-speaker_off
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
