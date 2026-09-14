from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Iterator

DIRECT_BATCH_LIMIT = 16
MANAGED_BATCH_LIMIT = 32
MODERN_ARC_BATCH_LIMIT = 32
ROUTE_LIMIT = 4096


@dataclass(frozen=True)
class Route:
    rx_device: str
    rx_channel: int
    tx_device: str | None = None
    tx_channel: str | None = None

    @property
    def clears(self) -> bool:
        return self.tx_device is None

    def to_dict(self) -> dict:
        return {
            "action": "clear" if self.clears else "set",
            "rx_channel": self.rx_channel,
            "rx_device": self.rx_device,
            "tx_channel": self.tx_channel,
            "tx_device": self.tx_device,
        }


@dataclass(frozen=True)
class DeviceBatch:
    rx_device: str
    action: str
    routes: tuple[Route, ...]


def parse_routes(payload: Any) -> list[Route]:
    if not isinstance(payload, list) or not payload:
        raise ValueError("routes must be a non-empty list")
    if len(payload) > ROUTE_LIMIT:
        raise ValueError(f"routes must contain at most {ROUTE_LIMIT} entries")
    routes: list[Route] = []
    seen: dict[tuple[str, int], int] = {}
    for index, entry in enumerate(payload):
        if not isinstance(entry, dict):
            raise ValueError(f"routes[{index}] must be an object")
        rx_device = entry.get("rx_device")
        rx_channel = entry.get("rx_channel")
        if not isinstance(rx_device, str) or not rx_device.strip():
            raise ValueError(f"routes[{index}].rx_device must be a device name")
        if isinstance(rx_channel, bool) or not isinstance(rx_channel, int) or rx_channel < 1:
            raise ValueError(f"routes[{index}].rx_channel must be a positive channel number")
        tx_device = entry.get("tx_device") or None
        tx_channel = entry.get("tx_channel") or None
        if (tx_device is None) != (tx_channel is None):
            raise ValueError(f"routes[{index}] must give both tx_device and tx_channel, or neither to clear")
        if tx_device is not None and (not isinstance(tx_device, str) or not isinstance(tx_channel, str)):
            raise ValueError(f"routes[{index}].tx_device and tx_channel must be strings")
        key = (rx_device.strip(), rx_channel)
        if key in seen:
            raise ValueError(f"routes[{index}] repeats {rx_device} channel {rx_channel} from routes[{seen[key]}]")
        seen[key] = index
        routes.append(Route(rx_device.strip(), rx_channel, tx_device, tx_channel))
    return routes


def chunked(items: Iterable[Route], size: int) -> Iterator[tuple[Route, ...]]:
    batch: list[Route] = []
    for item in items:
        batch.append(item)
        if len(batch) == size:
            yield tuple(batch)
            batch = []
    if batch:
        yield tuple(batch)


def plan_batches(routes: list[Route], limit_for_device) -> dict[str, list[DeviceBatch]]:
    grouped: dict[str, list[Route]] = {}
    for route in routes:
        grouped.setdefault(route.rx_device, []).append(route)
    plan: dict[str, list[DeviceBatch]] = {}
    for rx_device, device_routes in grouped.items():
        limit = limit_for_device(rx_device)
        batches: list[DeviceBatch] = []
        for action, selected in (
            ("clear", [route for route in device_routes if route.clears]),
            ("set", [route for route in device_routes if not route.clears]),
        ):
            batches.extend(DeviceBatch(rx_device, action, chunk) for chunk in chunked(selected, limit))
        plan[rx_device] = batches
    return plan
