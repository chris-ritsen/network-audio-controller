from __future__ import annotations

import math

from netaudio.commands.meter_models import (
    DETAILED_SOURCE,
    DETAILED_STALE_SECONDS,
    METER_DISPLAY_FLOOR_DBFS,
    PASSIVE_SOURCE,
    PASSIVE_STALE_SECONDS,
    MeterFilterDialog,
    MeterRow,
    MeterViewport,
    _clean_terminal_text,
    _fit_cell,
    _fit_render_cell,
    _METER_PLACEHOLDER,
    _STATE_PLACEHOLDER,
)
from netaudio.dante.metering import metering_value_dbfs


def _ansi(code: str, text: str, enabled: bool) -> str:
    return f"\x1b[{code}m{text}\x1b[0m" if enabled else text


def _state_label(row: MeterRow) -> str:
    if row.level is None:
        return "waiting"
    return {
        "clipping": "CLIP",
        "signal_present": "signal",
        "below_threshold": "quiet",
        "muted": "MUTED",
        "unknown": "UNKNOWN",
    }.get(row.indication, _clean_terminal_text(row.indication))


def _dbfs_label(level: int | None) -> str:
    if level is None:
        return "--"
    value = metering_value_dbfs(level)
    if value is not None:
        return f"{value:.1f}"
    return {0x00: "clip", 0xFE: "mute", 0xFF: "invalid"}.get(level, "--")


def _source_label(source: str | None) -> str:
    if source == PASSIVE_SOURCE:
        return "passive"
    if source == DETAILED_SOURCE:
        return "detail"
    return "--"


def _is_stale(row: MeterRow) -> bool:
    if row.age is None:
        return False
    stale_after = PASSIVE_STALE_SECONDS if row.metering_source == PASSIVE_SOURCE else DETAILED_STALE_SECONDS
    return row.age > stale_after


def _state_appearance(row: MeterRow) -> tuple[str, str, str]:
    """Return a Controller-style lamp, text label, and ANSI color."""
    if _is_stale(row):
        return "!", "STALE", "93;1"
    if row.level is None:
        return " ", "waiting", "90"
    return {
        "clipping": ("●", "CLIP", "91;1"),
        "signal_present": ("●", "signal", "92;1"),
        "below_threshold": (" ", "quiet", "90"),
        "muted": ("○", "MUTED", "90"),
        "unknown": ("?", "UNKNOWN", "93;1"),
    }.get(row.indication, ("?", _state_label(row), "93;1"))


def _meter_bar(row: MeterRow, width: int) -> tuple[str, str]:
    """Return a fixed-width level bar and its ANSI color."""
    width = max(0, width)
    blank = " " * width
    if width == 0 or _is_stale(row) or row.level is None or row.level in (0xFE, 0xFF):
        return blank, ""
    if row.level == 0x00:
        return "█" * width, "91;1"

    dbfs = metering_value_dbfs(row.level)
    if dbfs is None or dbfs <= METER_DISPLAY_FLOOR_DBFS:
        return blank, ""
    fraction = min(1.0, max(0.0, (dbfs - METER_DISPLAY_FLOOR_DBFS) / -METER_DISPLAY_FLOOR_DBFS))
    filled = min(width, max(1, math.ceil(fraction * width)))
    return "█" * filled + " " * (width - filled), "92;1"


def _replace_state_placeholder(line: str, token: str, display: str, ansi_code: str, color: bool) -> str:
    if color and ansi_code:
        display = f"\x1b[{ansi_code}m{display}\x1b[22;39m"
    return line.replace(token, display, 1)


def meter_page_size(height: int) -> int:
    return max(0, height - 3)


def render_meter_filter_prompt(dialog: MeterFilterDialog) -> str:
    device_value = dialog.device_query
    if dialog.selected_field == 0:
        return f"filter  device contains: {device_value}█   direction: {dialog.direction_label}"
    return f"filter  device contains: {device_value or 'all'}   direction: {dialog.direction_label}█"


def render_meter_frame(
    viewport: MeterViewport,
    *,
    width: int,
    height: int,
    mode: str,
    connection_status: str,
    no_color: bool,
    detailed_status: str | None = None,
    filter_status: str | None = None,
    search_prompt: str | None = None,
    filter_prompt: str | None = None,
) -> str:
    width = max(1, width)
    height = max(1, height)
    line_width = max(1, width - 1)
    color = not no_color
    page_size = meter_page_size(height)
    total = len(viewport.rows)
    if total and page_size:
        visible_start = viewport.top + 1
        visible_end = min(total, viewport.top + page_size)
        position = f"{visible_start}-{visible_end}/{total}"
    else:
        position = f"0/{total}"

    if mode == "passive":
        mode_label = "PASSIVE/no-requests"
    elif mode == "detailed":
        mode_label = f"DETAILED/{detailed_status or 'active'}"
    else:
        mode_label = f"MIXED/{detailed_status or 'starting'}"
    filter_label = f"  {filter_status}" if filter_status else ""
    title_text = f"Meter {mode_label}{filter_label}  {connection_status}  {position}"
    title = _fit_cell(title_text, line_width)

    if search_prompt is not None:
        footer = _fit_cell(f"/{search_prompt}█", line_width)
    elif filter_prompt is not None:
        footer = _fit_cell(filter_prompt, line_width)
    else:
        footer = _fit_cell(
            "j/k move  ^B/^F page  ^U/^D half  gg/G ends  f filter  / search  Enter device  Esc clear  q quit",
            line_width,
        )
    if height == 1:
        return _ansi("1", title, color)
    if height == 2:
        return "\n".join((_ansi("1", title, color), _ansi("90", footer, color)))

    if line_width < 39:
        header = " D  Ch  State Raw"
        compact = "tiny"
        device_width = 0
        channel_width = 0
        meter_width = 0
    elif line_width < 74:
        device_width = max(6, min(14, (line_width - 32) // 2))
        channel_width = max(1, line_width - device_width - 32)
        header = (
            f" {_fit_cell('Device', device_width)} "
            f"D {'Ch':>3} {_fit_cell('Channel', channel_width)} "
            f"  {_fit_cell('State', 7)} {_fit_cell('Raw', 4)} Src"
        )
        compact = "compact"
        meter_width = 0
    else:
        device_width = max(12, min(24, width // 5))
        available_meter_width = line_width - device_width - 44 - 18
        meter_width = min(20, available_meter_width) if available_meter_width >= 6 else 0
        if meter_width:
            channel_width = max(1, line_width - device_width - 44 - meter_width)
            header = (
                f" {_fit_cell('Device', device_width)} "
                f"{'Dir':<3} {'Ch':>3} {_fit_cell('Channel', channel_width)} "
                f"  {_fit_cell('State', 8)} Raw   dBFS {_fit_cell('Level', meter_width)} Source"
            )
        else:
            channel_width = max(1, line_width - device_width - 43)
            header = (
                f" {_fit_cell('Device', device_width)} "
                f"{'Dir':<3} {'Ch':>3} {_fit_cell('Channel', channel_width)} "
                f"  {_fit_cell('State', 8)} Raw   dBFS     Source"
            )
        compact = "wide"

    lines = [_ansi("1", title, color), _ansi("90", _fit_cell(header, line_width), color)]
    visible = viewport.visible_rows(page_size)
    for index, row in visible:
        stale = _is_stale(row)
        state_icon, state, state_color = _state_appearance(row)
        raw = f"0x{row.level:02X}" if row.level is not None else "--"
        source = "stale" if stale else _source_label(row.metering_source)
        marker = ">" if index == viewport.selected else " "
        meter_token = _METER_PLACEHOLDER * meter_width
        if compact == "tiny":
            state_text = _fit_render_cell(state, 5)
            state_token = f"{_STATE_PLACEHOLDER} {state_text}"
            line = f"{marker}{row.key.direction[0]} {row.key.channel_number:>3} {state_token} {raw:>4}"
        elif compact == "compact":
            state_text = _fit_render_cell(state, 7)
            state_token = f"{_STATE_PLACEHOLDER} {state_text}"
            line = (
                f"{marker}{_fit_render_cell(row.device_name, device_width)} "
                f"{row.key.direction[0]} {row.key.channel_number:>3} "
                f"{_fit_render_cell(row.channel_name, channel_width)} "
                f"{state_token} {raw:>4} {source:<7}"
            )
        else:
            state_text = _fit_render_cell(state, 8)
            state_token = f"{_STATE_PLACEHOLDER} {state_text}"
            if meter_width:
                line = (
                    f"{marker}{_fit_render_cell(row.device_name, device_width)} "
                    f"{row.key.direction:<3} {row.key.channel_number:>3} "
                    f"{_fit_render_cell(row.channel_name, channel_width)} "
                    f"{state_token} {raw:>4} {_dbfs_label(row.level):>8} "
                    f"{meter_token} {source:<7}"
                )
            else:
                line = (
                    f"{marker}{_fit_render_cell(row.device_name, device_width)} "
                    f"{row.key.direction:<3} {row.key.channel_number:>3} "
                    f"{_fit_render_cell(row.channel_name, channel_width)} "
                    f"{state_token} {raw:>4} {_dbfs_label(row.level):>8} "
                    f"{source:<7}"
                )
        line = _fit_cell(line, line_width)
        state_display = f"{state_icon} {state_text}"
        line = _replace_state_placeholder(line, state_token, state_display, state_color, color)
        if meter_width:
            meter_display, meter_color = _meter_bar(row, meter_width)
            line = _replace_state_placeholder(line, meter_token, meter_display, meter_color, color)
        if index == viewport.selected:
            line = _ansi("7", line, color)
        lines.append(line)

    while len(lines) < height - 1:
        lines.append("")
    lines.append(_ansi("90", footer, color))
    return "\n".join(lines[:height])
