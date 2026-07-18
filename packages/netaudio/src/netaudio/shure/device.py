from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class ShureDeviceType(str, Enum):
    ad4d = "ad4d"
    p10t = "p10t"


class BatteryType(str, Enum):
    lithium_ion = "LION"
    alkaline = "ALKA"
    nimh = "NIMH"
    unknown = "UNKN"


@dataclass
class ShureTransmitter:
    model: str | None = None
    device_id: str | None = None
    battery_minutes: int | None = None
    battery_type: BatteryType | None = None
    battery_charge_percent: int | None = None
    battery_bars: int | None = None
    battery_cycle_count: int | None = None
    battery_temp_f: int | None = None
    power_level: int | None = None
    mute_status: str | None = None

    @property
    def battery_hours(self) -> float | None:
        if self.battery_minutes is None or self.battery_minutes >= 65530:
            return None
        return self.battery_minutes / 60.0

    @property
    def connected(self) -> bool:
        return self.model is not None and self.model != "UNKNOWN"


@dataclass
class ShureChannel:
    number: int
    name: str | None = None
    frequency: int | None = None
    audio_gain: int | None = None
    audio_mute: bool = False
    audio_level_peak: int | None = None
    audio_level_rms: int | None = None
    signal_quality: int | None = None
    antenna_status: str | None = None
    encryption_status: str | None = None
    interference_status: str | None = None
    fd_mode: bool = False
    group_channel: str | None = None
    transmitter: ShureTransmitter | None = None

    @property
    def active(self) -> bool:
        if not self.transmitter or not self.transmitter.connected:
            return False
        return self.antenna_status is not None and self.antenna_status != "XX"


@dataclass
class ShureP10TChannel:
    number: int
    name: str | None = None
    frequency: int | None = None
    audio_in_level: int | None = None
    audio_in_level_l: int | None = None
    audio_in_level_r: int | None = None
    rf_tx_level: int | None = None
    rf_mute: bool = False
    audio_tx_mode: int | None = None
    audio_in_line_level: int | None = None
    group_channel: str | None = None


@dataclass
class ShureDeviceInfo:
    ip: str
    mac: str
    device_type: ShureDeviceType
    name: str
    port: int = 2202
    model: str | None = None
    firmware_version: str | None = None
    rf_band: str | None = None
    transmission_mode: str | None = None
    quadversity_mode: str | None = None
    encryption_mode: str | None = None
    channels: dict[int, ShureChannel | ShureP10TChannel] = field(default_factory=dict)
    dante_mac: str | None = None

    def to_json(self) -> dict:
        result: dict = {
            "ip": self.ip,
            "mac": self.mac,
            "device_type": self.device_type.value,
            "name": self.name,
        }

        optional_values = {
            "model": self.model,
            "firmware_version": self.firmware_version,
            "rf_band": self.rf_band,
            "transmission_mode": self.transmission_mode,
            "quadversity_mode": self.quadversity_mode,
            "encryption_mode": self.encryption_mode,
            "dante_mac": self.dante_mac,
        }
        for field_name, field_value in optional_values.items():
            if field_value is not None:
                result[field_name] = field_value

        if self.channels:
            result["channels"] = {}
            for channel_number, channel in sorted(self.channels.items()):
                result["channels"][channel_number] = _channel_to_json(channel)

        return result


def _channel_to_json(channel):
    if isinstance(channel, ShureChannel):
        return _ad4d_channel_to_json(channel)
    return _p10t_channel_to_json(channel)


def _ad4d_channel_to_json(channel: ShureChannel):
    result: dict[str, object] = {"name": channel.name, "active": channel.active}

    optional_values = {
        "frequency": channel.frequency,
        "audio_gain": channel.audio_gain,
        "audio_mute": channel.audio_mute,
        "audio_level_peak": channel.audio_level_peak,
        "audio_level_rms": channel.audio_level_rms,
        "signal_quality": channel.signal_quality,
        "antenna_status": channel.antenna_status,
        "encryption_status": channel.encryption_status,
        "interference_status": channel.interference_status,
        "fd_mode": channel.fd_mode,
        "group_channel": channel.group_channel,
    }
    for field_name, field_value in optional_values.items():
        if field_value is not None and field_value is not False:
            result[field_name] = field_value

    if channel.transmitter and channel.transmitter.connected:
        tx = channel.transmitter
        tx_json: dict[str, object] = {"model": tx.model}
        if tx.device_id:
            tx_json["device_id"] = tx.device_id
        if tx.battery_hours is not None:
            tx_json["battery_hours"] = round(tx.battery_hours, 1)
            tx_json["battery_minutes"] = tx.battery_minutes
        if tx.battery_type and tx.battery_type != BatteryType.unknown:
            tx_json["battery_type"] = tx.battery_type.value
        if tx.battery_charge_percent is not None:
            tx_json["battery_charge_percent"] = tx.battery_charge_percent
        if tx.battery_bars is not None:
            tx_json["battery_bars"] = tx.battery_bars
        if tx.battery_cycle_count is not None:
            tx_json["battery_cycle_count"] = tx.battery_cycle_count
        if tx.battery_temp_f is not None:
            tx_json["battery_temp_f"] = tx.battery_temp_f
        if tx.power_level is not None and tx.power_level < 255:
            tx_json["power_level"] = tx.power_level
        if tx.mute_status:
            tx_json["mute_status"] = tx.mute_status
        result["transmitter"] = tx_json

    return result


def _p10t_channel_to_json(channel: ShureP10TChannel):
    result: dict[str, object] = {"name": channel.name}

    optional_values = {
        "frequency": channel.frequency,
        "audio_in_level": channel.audio_in_level,
        "audio_in_level_l": channel.audio_in_level_l,
        "audio_in_level_r": channel.audio_in_level_r,
        "rf_tx_level": channel.rf_tx_level,
        "rf_mute": channel.rf_mute,
        "audio_tx_mode": channel.audio_tx_mode,
        "audio_in_line_level": channel.audio_in_line_level,
        "group_channel": channel.group_channel,
    }
    for field_name, field_value in optional_values.items():
        if field_value is not None and field_value is not False:
            result[field_name] = field_value

    return result


def _safe_int(value, default=None):
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


def parse_ad4d(raw_response: dict, ip: str, mac: str) -> ShureDeviceInfo:
    device = ShureDeviceInfo(
        ip=ip,
        mac=mac,
        device_type=ShureDeviceType.ad4d,
        name=raw_response.get("DEVICE_ID", ""),
        model=raw_response.get("MODEL"),
        firmware_version=raw_response.get("FW_VER"),
        rf_band=raw_response.get("RF_BAND"),
        transmission_mode=raw_response.get("TRANSMISSION_MODE"),
        quadversity_mode=raw_response.get("QUADVERSITY_MODE"),
        encryption_mode=raw_response.get("ENCRYPTION_MODE"),
    )

    for channel_number in (1, 2, 3, 4):
        channel_data = raw_response.get(channel_number, {})
        if not channel_data:
            continue

        tx = ShureTransmitter(
            model=channel_data.get("TX_MODEL"),
            device_id=channel_data.get("TX_DEVICE_ID") or None,
            battery_minutes=_safe_int(channel_data.get("TX_BATT_MINS")),
            battery_type=BatteryType(channel_data["TX_BATT_TYPE"])
            if channel_data.get("TX_BATT_TYPE") in BatteryType._value2member_map_
            else None,
            battery_charge_percent=_safe_int(channel_data.get("TX_BATT_CHARGE_PERCENT")),
            battery_bars=_safe_int(channel_data.get("TX_BATT_BARS")),
            battery_cycle_count=_safe_int(channel_data.get("TX_BATT_CYCLE_COUNT")),
            battery_temp_f=_safe_int(channel_data.get("TX_BATT_TEMP_F")),
            power_level=_safe_int(channel_data.get("TX_POWER_LEVEL")),
            mute_status=channel_data.get("TX_MUTE_MODE_STATUS"),
        )

        frequency = _safe_int(channel_data.get("FREQUENCY"))

        channel = ShureChannel(
            number=channel_number,
            name=channel_data.get("CHAN_NAME"),
            frequency=frequency,
            audio_gain=_safe_int(channel_data.get("AUDIO_GAIN")),
            audio_mute=channel_data.get("AUDIO_MUTE") == "ON",
            audio_level_peak=_safe_int(channel_data.get("AUDIO_LEVEL_PEAK")),
            audio_level_rms=_safe_int(channel_data.get("AUDIO_LEVEL_RMS")),
            signal_quality=_safe_int(channel_data.get("CHAN_QUALITY")),
            antenna_status=channel_data.get("ANTENNA_STATUS"),
            encryption_status=channel_data.get("ENCRYPTION_STATUS"),
            interference_status=channel_data.get("INTERFERENCE_STATUS"),
            fd_mode=channel_data.get("FD_MODE") == "ON",
            group_channel=channel_data.get("GROUP_CHANNEL"),
            transmitter=tx,
        )

        device.channels[channel_number] = channel

    return device


def parse_p10t(raw_response: dict, ip: str, mac: str) -> ShureDeviceInfo:
    device = ShureDeviceInfo(
        ip=ip,
        mac=mac,
        device_type=ShureDeviceType.p10t,
        name=raw_response.get("DEVICE_NAME", ""),
    )

    for channel_number in (1, 2):
        channel_data = raw_response.get(channel_number, {})
        if not channel_data:
            continue

        channel = ShureP10TChannel(
            number=channel_number,
            name=channel_data.get("CHAN_NAME"),
            frequency=_safe_int(channel_data.get("FREQUENCY")),
            audio_in_level=_safe_int(channel_data.get("AUDIO_IN_LVL")),
            audio_in_level_l=_safe_int(channel_data.get("AUDIO_IN_LVL_L")),
            audio_in_level_r=_safe_int(channel_data.get("AUDIO_IN_LVL_R")),
            rf_tx_level=_safe_int(channel_data.get("RF_TX_LVL")),
            rf_mute=channel_data.get("RF_MUTE") == "1",
            audio_tx_mode=_safe_int(channel_data.get("AUDIO_TX_MODE")),
            audio_in_line_level=_safe_int(channel_data.get("AUDIO_IN_LINE_LVL")),
            group_channel=channel_data.get("GROUP_CHAN"),
        )

        device.channels[channel_number] = channel

    return device
