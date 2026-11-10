from __future__ import annotations

import ipaddress

ARC_SERVICE_TYPE = "_netaudio-arc._udp.local."


class Record:
    renamed: dict[str, str] = {}

    def __init__(self, record: dict) -> None:
        self.record = record

    def __getattr__(self, name: str):
        if name.startswith("_"):
            raise AttributeError(name)
        return self.record.get(self.renamed.get(name, name))


class Channel(Record):
    def __init__(self, record: dict, number: int, channel_type: str) -> None:
        super().__init__(record)
        self.channel_type = channel_type
        self.number = number


class Device(Record):
    renamed = {
        "active_latency": "active_latency_ms",
        "configured_latency": "configured_latency_ms",
        "default_latency": "default_latency_ms",
        "latency": "latency_ms",
        "max_latency": "max_latency_ms",
        "min_latency": "min_latency_ms",
        "sample_rate": "sample_rate_hz",
        "standard_latency_choices": "standard_latency_choices_ms",
        "supported_sample_rates": "supported_sample_rates_hz",
    }

    def __init__(self, record: dict) -> None:
        super().__init__(record)
        channels = record.get("channels") or {}
        self.rx_channels = self._channels(channels.get("receivers"), "rx")
        self.tx_channels = self._channels(channels.get("transmitters"), "tx")

    @staticmethod
    def _channels(records: dict | None, channel_type: str) -> dict[int, Channel]:
        return {int(number): Channel(record, int(number), channel_type) for number, record in (records or {}).items()}

    @property
    def ipv4(self):
        address = self.record.get("ipv4")
        return ipaddress.ip_address(address) if address and address != "None" else None

    @property
    def is_licensed(self) -> bool | None:
        for service in self.services.values():
            properties = service.get("properties")
            if service.get("type") == ARC_SERVICE_TYPE and isinstance(properties, dict) and "unlicensed" in properties:
                return False
        return None

    @property
    def kind(self) -> str:
        if self.record.get("kind"):
            return self.record["kind"]
        return "managed" if self.record.get("direct_control_available") is False else ""

    @property
    def name(self) -> str:
        return self.record.get("name") or ""

    @property
    def online(self) -> bool:
        return self.record.get("online", True)

    @property
    def server_name(self) -> str:
        return self.record.get("server_name") or ""

    @property
    def services(self) -> dict:
        return self.record.get("services") or {}

    def gain_level_label_for_channel(self, channel_number: int) -> str | None:
        levels = self.record.get("gain_levels") or []
        if not 0 < channel_number <= len(levels):
            return None
        labels = {choice["value"]: choice["label"] for choice in self.record.get("gain_level_choices") or []}
        return labels.get(levels[channel_number - 1])
