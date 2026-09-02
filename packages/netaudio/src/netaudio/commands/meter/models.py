from __future__ import annotations

import math
import time
from dataclasses import dataclass
from typing import Callable

from rich.cells import cell_len, set_cell_size

from netaudio.commands.device.display import _channel_matches
from netaudio.dante.device_serializer import DanteDeviceSerializer
from netaudio.dante.metering import classify_signal_presence

PASSIVE_SOURCE = "signal_presence"
DETAILED_SOURCE = "detailed"
PASSIVE_STALE_SECONDS = 2.5
DETAILED_STALE_SECONDS = 1.0
AUTO_DETAILED_DEVICE_IDENTIFIERS = frozenset({"lx-dante"})
AUTO_DETAILED_DANTE_MODELS = frozenset({"a32 dante ad/da converter"})
DETAILED_ESCALATION_SECONDS = 3.0
SEARCH_QUERY_LIMIT = 256
METER_DISPLAY_FLOOR_DBFS = -61.0
MOUSE_WHEEL_ROWS = 3
_STATE_PLACEHOLDER = "¤"
_METER_PLACEHOLDER = "§"


def automatic_detailed_metering_targets(devices: dict) -> list[str]:
    targets = []
    for server_name, device in devices.items():
        if not getattr(device, "online", True) or not getattr(device, "ipv4", None):
            continue
        model_id = str(getattr(device, "model_id", "") or "").strip().casefold()
        dante_model = str(getattr(device, "dante_model", "") or "").strip().casefold()
        if model_id in AUTO_DETAILED_DEVICE_IDENTIFIERS or dante_model in AUTO_DETAILED_DANTE_MODELS:
            targets.append(server_name)
    return sorted(targets)


@dataclass(frozen=True)
class MeterRowKey:
    server_name: str
    direction: str
    channel_number: int


@dataclass(frozen=True)
class MeterRow:
    key: MeterRowKey
    device_name: str
    channel_name: str
    level: int | None
    indication: str
    metering_source: str | None
    age: float | None


def _clean_terminal_text(value: object) -> str:
    text = str(value or "")
    return "".join(character if character.isprintable() and character != "\x1b" else "?" for character in text)


def _fit_cell(value: object, width: int) -> str:
    if width <= 0:
        return ""
    text = _clean_terminal_text(value)
    if cell_len(text) > width:
        if width == 1:
            return "…"
        text = set_cell_size(text, width - 1) + "…"
    return set_cell_size(text, width)


def _fit_render_cell(value: object, width: int) -> str:
    """Fit untrusted row text without allowing ANSI marker collisions."""
    text = _clean_terminal_text(value).replace(_STATE_PLACEHOLDER, "?").replace(_METER_PLACEHOLDER, "?")
    return _fit_cell(text, width)


def _normalize_level_map(value: object) -> dict[int, int] | None:
    if not isinstance(value, dict):
        return None
    result: dict[int, int] = {}
    try:
        for channel_key, level in value.items():
            channel_number = int(channel_key)
            if channel_number <= 0 or isinstance(level, bool) or not isinstance(level, int) or not 0 <= level <= 0xFF:
                return None
            result[channel_number] = level
    except (TypeError, ValueError):
        return None
    return result


def _normalize_indication_map(value: object) -> dict[int, str] | None:
    if value is None:
        return {}
    if not isinstance(value, dict):
        return None
    result: dict[int, str] = {}
    try:
        for channel_key, indication in value.items():
            channel_number = int(channel_key)
            if channel_number <= 0 or not isinstance(indication, str):
                return None
            result[channel_number] = indication
    except (TypeError, ValueError):
        return None
    return result


def format_meter_sample(device, sample: object) -> dict | None:
    """Attach channel names to a validated cache sample for plain/JSON output."""
    if not isinstance(sample, dict):
        return None
    tx = _normalize_level_map(sample.get("tx", {}))
    rx = _normalize_level_map(sample.get("rx", {}))
    tx_indications = _normalize_indication_map(sample.get("tx_signal_presence"))
    rx_indications = _normalize_indication_map(sample.get("rx_signal_presence"))
    if tx is None or rx is None or tx_indications is None or rx_indications is None:
        return None

    result = {
        "tx": {},
        "rx": {},
        "wall_time": sample.get("wall_time"),
        "source_ip": sample.get("source_ip"),
        "source_port": sample.get("source_port"),
        "metering_source": sample.get("metering_source"),
    }
    for direction, levels, indications, inventory in (
        ("tx", tx, tx_indications, getattr(device, "tx_channels", {}) or {}),
        ("rx", rx, rx_indications, getattr(device, "rx_channels", {}) or {}),
    ):
        for channel_number, level in levels.items():
            channel = inventory.get(channel_number)
            channel_name = ""
            if channel is not None:
                channel_name = channel.friendly_name or channel.name or ""
            result[direction][channel_number] = {
                "name": channel_name,
                "level": level,
                "signal_presence": indications.get(channel_number) or classify_signal_presence(level),
            }
    return result


class MeterViewModel:
    """Pure meter state shared by the terminal UI and tests."""

    def __init__(
        self,
        devices: dict,
        *,
        show_tx: bool = True,
        show_rx: bool = True,
        channel_patterns: list[str] | None = None,
    ):
        self.devices = dict(devices)
        self.show_tx = show_tx
        self.show_rx = show_rx
        self.channel_patterns = list(channel_patterns or [])
        self.device_filter: str | None = None
        self.device_query = ""
        self.search_query = ""
        self.samples: dict[str, dict] = {}
        self.connection_status = "connecting"

    def set_direction_filter(self, direction: str) -> None:
        if direction == "tx":
            self.show_tx, self.show_rx = True, False
        elif direction == "rx":
            self.show_tx, self.show_rx = False, True
        elif direction == "both":
            self.show_tx = self.show_rx = True
        else:
            raise ValueError(f"unknown direction filter: {direction}")

    def set_device_filter(self, server_name: str | None) -> None:
        if server_name is not None and server_name not in self.devices:
            raise ValueError(f"unknown device filter: {server_name}")
        self.device_filter = server_name

    def set_device_query(self, query: str) -> None:
        self.device_query = query[:SEARCH_QUERY_LIMIT]

    def set_search_query(self, query: str) -> None:
        self.search_query = query[:SEARCH_QUERY_LIMIT]

    def undo_content_filter(self) -> bool:
        if self.device_filter is not None:
            self.device_filter = None
            return True
        if self.device_query:
            self.device_query = ""
            return True
        if self.search_query:
            self.search_query = ""
            return True
        return False

    @property
    def filter_status(self) -> str:
        if self.device_filter is not None:
            device = self.devices.get(self.device_filter)
            device_label = f"device={(device.name if device else None) or self.device_filter}"
        elif self.device_query:
            device_label = f"devices~{self.device_query}"
        else:
            device_label = "all devices"
        if self.show_tx and self.show_rx:
            direction_label = "TX+RX"
        elif self.show_tx:
            direction_label = "TX"
        else:
            direction_label = "RX"
        parts = [device_label, direction_label]
        if self.search_query:
            parts.append(f"/{self.search_query}")
        return " · ".join(parts)

    @staticmethod
    def row_matches_search(row: MeterRow, query: str) -> bool:
        normalized_query = query.casefold()
        if not normalized_query:
            return False
        return any(
            normalized_query in value.casefold()
            for value in (
                row.device_name,
                row.key.server_name,
                row.channel_name,
            )
        )

    def apply_sample(self, server_name: str, sample: object) -> bool:
        """Validate a complete sample before replacing any visible device state."""
        if server_name not in self.devices or not isinstance(sample, dict):
            return False

        tx = _normalize_level_map(sample.get("tx", {}))
        rx = _normalize_level_map(sample.get("rx", {}))
        tx_indications = _normalize_indication_map(sample.get("tx_signal_presence"))
        rx_indications = _normalize_indication_map(sample.get("rx_signal_presence"))
        if None in (tx, rx, tx_indications, rx_indications):
            return False

        source = sample.get("metering_source")
        if source is not None and not isinstance(source, str):
            return False

        topology: dict[str, int] = {}
        for key in (
            "tx_count",
            "rx_count",
            "tx_first_channel_index",
            "rx_first_channel_index",
        ):
            value = sample.get(key)
            if value is None:
                continue
            if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= 0xFFFF:
                return False
            topology[key] = value

        wall_time = sample.get("wall_time")
        if wall_time is None:
            wall_time = time.time()
        if (
            isinstance(wall_time, bool)
            or not isinstance(wall_time, (int, float))
            or not math.isfinite(wall_time)
            or wall_time > time.time() + 5.0
        ):
            return False

        initial_age = max(0.0, time.time() - float(wall_time))
        previous = self.samples.get(server_name)
        if previous is not None and float(wall_time) < previous["wall_time"]:
            return False

        self.samples[server_name] = {
            "tx": tx,
            "rx": rx,
            "tx_signal_presence": tx_indications,
            "rx_signal_presence": rx_indications,
            "metering_source": source,
            "wall_time": float(wall_time),
            "received_monotonic": time.monotonic() - initial_age,
            "source_ip": sample.get("source_ip"),
            "source_port": sample.get("source_port"),
            **topology,
        }
        return True

    def _replace_device(self, server_name: str, device_json: dict) -> bool:
        previous = self.devices.get(server_name)
        if previous is None:
            return False
        try:
            replacement = DanteDeviceSerializer.device_from_json(device_json)
        except (TypeError, ValueError):
            return False

        previous_counts = (
            getattr(previous, "tx_count", None),
            getattr(previous, "rx_count", None),
        )
        replacement_counts = (
            getattr(replacement, "tx_count", None),
            getattr(replacement, "rx_count", None),
        )
        self.devices[server_name] = replacement
        if previous_counts != replacement_counts:
            # Meter values are scoped to the channel topology that produced
            # them.  Never carry a pre-rate-change sample into a new active
            # inventory; wait for the next passive or detailed frame.
            self.samples.pop(server_name, None)
        return True

    def apply_event(self, event: object) -> bool:
        if not isinstance(event, dict):
            return False
        event_name = event.get("event")
        if event_name == "snapshot":
            changed = False
            devices = event.get("devices")
            if isinstance(devices, dict):
                for server_name, device_json in devices.items():
                    if server_name not in self.devices or not isinstance(device_json, dict):
                        continue
                    changed = self._replace_device(server_name, device_json) or changed
            metering = event.get("metering")
            if isinstance(metering, dict):
                for server_name, sample in metering.items():
                    changed = self.apply_sample(server_name, sample) or changed
            return changed
        if event_name == "meter_values":
            server_name = event.get("server_name")
            return isinstance(server_name, str) and self.apply_sample(server_name, event)
        if event_name in ("device_discovered", "device_updated"):
            server_name = event.get("server_name")
            device_json = event.get("device")
            if not isinstance(server_name, str) or server_name not in self.devices or not isinstance(device_json, dict):
                return False
            return self._replace_device(server_name, device_json)
        if event_name == "device_removed":
            server_name = event.get("server_name")
            if not isinstance(server_name, str):
                return False
            device = self.devices.get(server_name)
            if not device:
                return False
            device.online = False
            self.samples.pop(server_name, None)
            return True
        return False

    def rows(self, now: float | None = None) -> list[MeterRow]:
        result: list[MeterRow] = []
        ordered_devices = sorted(
            self.devices.items(),
            key=lambda item: ((item[1].name or item[0]).lower(), item[0]),
        )
        for server_name, device in ordered_devices:
            if self.device_filter is not None and server_name != self.device_filter:
                continue
            query = self.device_query.casefold()
            device_matches_query = not query or any(
                query in str(value or "").casefold()
                for value in (
                    getattr(device, "name", None),
                    server_name,
                )
            )
            if not device_matches_query:
                continue
            sample = self.samples.get(server_name)
            source = sample.get("metering_source") if sample else None
            if sample is None:
                age = None
            elif now is not None:
                age = max(0.0, now - sample["wall_time"])
            else:
                age = max(0.0, time.monotonic() - sample["received_monotonic"])
            for direction, enabled, channels_attribute, indication_key in (
                ("TX", self.show_tx, "tx_channels", "tx_signal_presence"),
                ("RX", self.show_rx, "rx_channels", "rx_signal_presence"),
            ):
                if not enabled:
                    continue
                inventory = getattr(device, channels_attribute, {}) or {}
                sample_key = direction.lower()
                levels = sample.get(sample_key, {}) if sample else {}
                indications = sample.get(indication_key, {}) if sample else {}
                channel_numbers = sorted(set(inventory) | set(levels))
                active_count = getattr(device, f"{sample_key}_count", None)
                first_index = sample.get(f"{sample_key}_first_channel_index") if sample else None
                sample_count = sample.get(f"{sample_key}_count") if sample else None
                if first_index == 0 and isinstance(sample_count, int):
                    active_count = sample_count
                if isinstance(active_count, int) and active_count >= 0:
                    channel_numbers = [number for number in channel_numbers if 1 <= number <= active_count]
                for channel_number in channel_numbers:
                    channel = inventory.get(channel_number)
                    channel_name = ""
                    if channel is not None:
                        channel_name = channel.friendly_name or channel.name or ""
                    if self.channel_patterns and not _channel_matches(
                        channel_number,
                        channel_name,
                        self.channel_patterns,
                    ):
                        continue
                    level = levels.get(channel_number)
                    indication = indications.get(channel_number)
                    if indication is None and level is not None:
                        indication = classify_signal_presence(level)
                    result.append(
                        MeterRow(
                            key=MeterRowKey(server_name, direction, channel_number),
                            device_name=device.name or server_name,
                            channel_name=channel_name or f"Ch {channel_number}",
                            level=level,
                            indication=indication or "waiting",
                            metering_source=source,
                            age=age,
                        )
                    )
        return result


class MeterFilterDialog:
    """Typed device filter plus an explicit direction field."""

    _DIRECTIONS = (
        ("both", "Both (TX + RX)"),
        ("tx", "TX only"),
        ("rx", "RX only"),
    )

    def __init__(self, model: MeterViewModel):
        self.original_device_filter = model.device_filter
        self.original_device_query = model.device_query
        self.original_direction = "both" if model.show_tx and model.show_rx else "tx" if model.show_tx else "rx"
        self.device_query = model.device_query
        self.direction_index = next(
            index for index, (direction, _label) in enumerate(self._DIRECTIONS) if direction == self.original_direction
        )
        self.selected_field = 0

    @property
    def device_label(self) -> str:
        return self.device_query or "all"

    @property
    def direction_label(self) -> str:
        return self._DIRECTIONS[self.direction_index][1]

    def move_field(self, amount: int) -> None:
        self.selected_field = (self.selected_field + amount) % 2

    def change_value(self, amount: int) -> None:
        if self.selected_field == 1:
            self.direction_index = (self.direction_index + amount) % len(self._DIRECTIONS)

    def append_text(self, text: str) -> None:
        self.device_query = (self.device_query + text)[:SEARCH_QUERY_LIMIT]

    def backspace(self) -> None:
        self.device_query = self.device_query[:-1]

    def apply(self, model: MeterViewModel) -> None:
        model.set_device_query(self.device_query)
        model.set_device_filter(None)
        model.set_direction_filter(self._DIRECTIONS[self.direction_index][0])

    def cancel(self, model: MeterViewModel) -> None:
        model.set_device_filter(self.original_device_filter)
        model.set_device_query(self.original_device_query)
        model.set_direction_filter(self.original_direction)


class MeterViewport:
    def __init__(self):
        self.rows: list[MeterRow] = []
        self.selected = 0
        self.top = 0

    @property
    def selected_key(self) -> MeterRowKey | None:
        if not self.rows:
            return None
        return self.rows[self.selected].key

    def replace_rows(self, rows: list[MeterRow], page_size: int) -> None:
        previous_key = self.selected_key
        self.rows = rows
        if not rows:
            self.selected = 0
            self.top = 0
            return
        if previous_key is not None:
            for index, row in enumerate(rows):
                if row.key == previous_key:
                    self.selected = index
                    break
            else:
                self.selected = min(self.selected, len(rows) - 1)
        else:
            self.selected = min(self.selected, len(rows) - 1)
        self._reveal(page_size)

    def move(self, amount: int, page_size: int) -> None:
        if not self.rows:
            return
        self.selected = max(0, min(len(self.rows) - 1, self.selected + amount))
        self._reveal(page_size)

    def home(self, page_size: int) -> None:
        if self.rows:
            self.selected = 0
            self._reveal(page_size)

    def end(self, page_size: int) -> None:
        if self.rows:
            self.selected = len(self.rows) - 1
            self._reveal(page_size)

    def align_selected(self, placement: str, page_size: int) -> None:
        if not self.rows:
            return
        page_size = max(1, page_size)
        if placement == "top":
            desired_top = self.selected
        elif placement == "center":
            desired_top = self.selected - page_size // 2
        elif placement == "bottom":
            desired_top = self.selected - page_size + 1
        else:
            raise ValueError(f"unknown viewport placement: {placement}")
        self.top = max(0, min(len(self.rows) - 1, desired_top))

    def select_key(self, key: MeterRowKey | None, page_size: int) -> bool:
        if key is None:
            return False
        for index, row in enumerate(self.rows):
            if row.key == key:
                self.selected = index
                self._reveal(page_size)
                return True
        return False

    def move_to_match(
        self,
        predicate: Callable[[MeterRow], bool],
        *,
        direction: int,
        page_size: int,
        include_current: bool = False,
    ) -> bool:
        if not self.rows:
            return False
        start_step = 0 if include_current else 1
        for step in range(start_step, len(self.rows) + start_step):
            index = (self.selected + direction * step) % len(self.rows)
            if predicate(self.rows[index]):
                self.selected = index
                self._reveal(page_size)
                return True
        return False

    def _reveal(self, page_size: int) -> None:
        page_size = max(1, page_size)
        if self.selected < self.top:
            self.top = self.selected
        elif self.selected >= self.top + page_size:
            self.top = self.selected - page_size + 1
        self.top = max(0, min(len(self.rows) - 1, self.top))

    def visible_rows(self, page_size: int) -> list[tuple[int, MeterRow]]:
        page_size = max(0, page_size)
        return list(enumerate(self.rows[self.top : self.top + page_size], start=self.top))
