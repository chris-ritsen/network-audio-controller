from __future__ import annotations

import logging
import ipaddress
from typing import Any

from netaudio.commands.shure_transport import PROTOCOL_CONFIGS, Protocol

logger = logging.getLogger("netaudio")

PROTOCOLS = {"ad4d": Protocol.rep, "p10t": Protocol.report}
MANAGED_KEYS = frozenset({"METER_RATE"})
SETTING_KEYS = {
    "ad4d": {
        "name": "CHAN_NAME",
        "channel_name": "CHAN_NAME",
        "gain": "AUDIO_GAIN",
        "mute": "AUDIO_MUTE",
        "frequency": "FREQUENCY",
        "group_channel": "GROUP_CHANNEL",
        "identify": "FLASH",
        "flash": "FLASH",
        "device_name": "DEVICE_ID",
        "frequency2": "FREQUENCY2",
        "group_channel2": "GROUP_CHANNEL2",
        "network": "NET_SETTINGS",
        "net_settings": "NET_SETTINGS",
        "slot_input_pad": "SLOT_INPUT_PAD",
        "slot_offset": "SLOT_OFFSET",
        "slot_polarity": "SLOT_POLARITY",
        "slot_rf_output": "SLOT_RF_OUTPUT",
        "slot_rf_power_mode": "SLOT_RF_POWER_MODE",
        "slot_device_id": "SLOT_TX_DEVICE_ID",
    },
    "p10t": {
        "name": "CHAN_NAME",
        "channel_name": "CHAN_NAME",
        "input_level": "AUDIO_IN_LVL",
        "rf_power": "RF_TX_LVL",
        "rf_mute": "RF_MUTE",
        "mute": "RF_MUTE",
        "frequency": "FREQUENCY",
        "group_channel": "GROUP_CHAN",
        "transmit_mode": "AUDIO_TX_MODE",
        "line_level": "AUDIO_IN_LINE_LVL",
        "audio_level": "AUDIO_IN_LVL",
        "device_name": "DEVICE_NAME",
    },
}
ON_WORDS = frozenset({"on", "true", "yes", "1", "mute", "muted", "enable", "enabled"})
OFF_WORDS = frozenset({"off", "false", "no", "0", "unmute", "unmuted", "disable", "disabled"})
AD4D_GAIN_OFFSET_DB = 18


def _normalize_mac(value: str) -> str:
    return "".join(character for character in value.lower() if character in "0123456789abcdef")[:12]


def find_wireless_device(devices: dict, selector: Any) -> tuple[str, Any] | None:
    if not isinstance(selector, str) or not selector.strip():
        return None
    wanted = selector.strip().casefold()
    for mac, device in devices.items():
        if wanted in {str(device.name).casefold(), str(device.ip), mac.casefold()}:
            return mac, device
        if (
            _normalize_mac(selector)
            and _normalize_mac(selector) == _normalize_mac(mac)
            and len(_normalize_mac(selector)) == 12
        ):
            return mac, device
    return None


def channel_number(device: Any, channel: Any) -> int | None:
    if channel is None or channel == "":
        return None
    if isinstance(channel, int) or (isinstance(channel, str) and channel.strip().isdigit()):
        number = int(channel)
        if number in device.channels:
            return number
        raise ValueError(f"{device.name} has channels {', '.join(str(key) for key in sorted(device.channels))}")
    wanted = str(channel).strip().casefold()
    matches = [
        number for number, entry in device.channels.items() if str(entry.name or "").strip().casefold() == wanted
    ]
    if len(matches) == 1:
        return matches[0]
    names = ", ".join(f"{number} {entry.name}" for number, entry in sorted(device.channels.items()))
    raise ValueError(f"{device.name} has no single channel named {channel!r}; its channels are {names}")


def setting_key(device_type: str, setting: str) -> str:
    folded = setting.strip().lower().replace(" ", "_").replace("-", "_")
    key = SETTING_KEYS.get(device_type, {}).get(folded, setting.strip().upper())
    configuration = PROTOCOL_CONFIGS[PROTOCOLS[device_type]]
    writable = configuration["device_rw_keys"] + configuration["channel_rw_keys"]
    if key in MANAGED_KEYS:
        raise ValueError(f"{key} is managed by the server for metering")
    if key not in writable:
        friendly = ", ".join(sorted(SETTING_KEYS.get(device_type, {})))
        raise ValueError(f"{setting!r} cannot be set on a {device_type.upper()}; settings: {friendly}")
    return key


def is_channel_key(device_type: str, key: str) -> bool:
    return key in PROTOCOL_CONFIGS[PROTOCOLS[device_type]]["channel_rw_keys"]


SLOT_CHOICES = {
    "SLOT_POLARITY": ("POSITIVE", "NEGATIVE"),
    "SLOT_RF_OUTPUT": ("RF_ON", "RF_MUTE"),
    "SLOT_RF_POWER_MODE": ("LOW", "NORMAL", "HIGH"),
}
DEVICE_LEVEL_OPTIONAL = frozenset({"FLASH"})


def slot_number(slot: Any) -> int:
    if isinstance(slot, bool) or slot is None or str(slot).strip() == "":
        raise ValueError("this is a transmitter slot setting; give the slot, 1 to 8")
    text = str(slot).strip()
    if not text.isdigit() or not 1 <= int(text) <= 8:
        raise ValueError("slot must be 1 to 8")
    return int(text)


def _offset_value(text: str, low: int, high: int, name: str) -> str:
    try:
        decibels = int(text)
    except ValueError as exception:
        raise ValueError(f"{name} takes whole decibels from {low} to {high}") from exception
    if not low <= decibels <= high:
        raise ValueError(f"{name} takes whole decibels from {low} to {high}")
    return f"{decibels + 12:03d}"


def network_value(value: Any) -> str:
    if not isinstance(value, dict):
        raise ValueError("network settings take interface, mode, address, subnet mask and gateway")
    interface = str(value.get("interface", "")).upper()
    if interface not in {"SC", "D1", "D2"}:
        raise ValueError("interface must be SC, D1 or D2")
    mode = str(value.get("mode", "")).upper()
    if mode == "AUTO":
        return f"{interface} AUTO na na na"
    if mode != "MANUAL":
        raise ValueError("mode must be AUTO or MANUAL")
    fields = []
    for name in ("address", "subnet_mask", "gateway"):
        text = str(value.get(name, "")).strip()
        try:
            ipaddress.IPv4Address(text)
        except ipaddress.AddressValueError as exception:
            raise ValueError(f"{name.replace('_', ' ')} must be an IPv4 address") from exception
        fields.append(text)
    return " ".join([interface, "MANUAL", *fields])


def comparable(value: Any) -> str:
    return " ".join(str(value).replace("{", " ").replace("}", " ").split()).casefold()


def formatted_value(device_type: str, key: str, value: Any) -> str:
    text = str(value).strip() if not isinstance(value, bool) else ("on" if value else "off")
    if key == "AUDIO_MUTE":
        folded = text.casefold()
        if folded in ON_WORDS:
            return "ON"
        if folded in OFF_WORDS:
            return "OFF"
        if folded == "toggle":
            return "TOGGLE"
        raise ValueError("mute takes on or off")
    if key == "RF_MUTE":
        folded = text.casefold()
        if folded in ON_WORDS:
            return "1"
        if folded in OFF_WORDS:
            return "0"
        raise ValueError("rf_mute takes on or off")
    if key == "FLASH":
        return "ON"
    if key == "SLOT_INPUT_PAD":
        folded = text.casefold()
        if folded in ON_WORDS or folded == "-12":
            return "0"
        if folded in OFF_WORDS:
            return "12"
        raise ValueError("input pad takes on (-12 dB) or off (0 dB)")
    if key == "SLOT_OFFSET":
        return _offset_value(text, -12, 21, "offset")
    if key in SLOT_CHOICES:
        upper = text.upper()
        if upper not in SLOT_CHOICES[key]:
            raise ValueError(f"{key.lower()} takes {', '.join(SLOT_CHOICES[key])}")
        return upper
    if key == "AUDIO_TX_MODE":
        modes = {
            "mono": "1",
            "1": "1",
            "point_to_point": "2",
            "point to point": "2",
            "ptp": "2",
            "2": "2",
            "stereo": "3",
            "3": "3",
        }
        if text.casefold() not in modes:
            raise ValueError("transmit mode takes mono, point to point or stereo")
        return modes[text.casefold()]
    if key == "AUDIO_IN_LINE_LVL":
        levels = {"line": "1", "1": "1", "on": "1", "aux": "0", "0": "0", "off": "0"}
        if text.casefold() not in levels:
            raise ValueError("input takes line (+4 dBu) or aux (-10 dBV)")
        return levels[text.casefold()]
    if key == "RF_TX_LVL":
        if text not in {"10", "50", "100"}:
            raise ValueError("RF power takes 10, 50 or 100 mW")
        return text
    if key in {"FREQUENCY", "FREQUENCY2"}:
        try:
            number = float(text)
        except ValueError as exception:
            raise ValueError("frequency takes MHz, such as 511.125, or kHz, such as 511125") from exception
        kilohertz = round(number * 1000) if number < 10000 else round(number)
        return f"{kilohertz:06d}"
    if key == "AUDIO_GAIN" and device_type == "ad4d":
        if not text.lstrip("-").isdigit():
            raise ValueError("gain takes the receiver's 0-60 setting, where 18 is 0 dB")
        return f"{int(text):03d}"
    forbidden = {"<", ">", "\r", "\n", "\x00"}
    if any(character in text for character in forbidden):
        raise ValueError("values cannot contain angle brackets, newlines or NUL bytes")
    if key in PROTOCOL_CONFIGS[PROTOCOLS[device_type]]["brace_keys"]:
        if "{" in text or "}" in text:
            raise ValueError("this value cannot contain braces")
        return "{" + text + "}"
    return text


def described_value(device_type: str, key: str, value: Any) -> Any:
    if value is None:
        return None
    text = str(value).strip().strip("{}").strip()
    if key == "AUDIO_GAIN" and device_type == "ad4d" and text.lstrip("-").isdigit():
        return f"{int(text)} ({int(text) - AD4D_GAIN_OFFSET_DB:+d} dB)"
    if key == "FREQUENCY" and text.isdigit():
        return f"{int(text) / 1000:.3f} MHz"
    if key == "RF_MUTE":
        return "on" if text == "1" else "off" if text == "0" else text
    return text


class DaemonShureHandlers:
    shure: Any

    def wireless_change(self, params: dict) -> dict:
        if not self.shure:
            raise ValueError("Shure support is not running")
        found = find_wireless_device(self.shure.devices, params.get("device"))
        if found is None:
            names = ", ".join(sorted(str(device.name) for device in self.shure.devices.values())) or "none"
            raise ValueError(f"no Shure device {params.get('device')!r}; known devices: {names}")
        mac, device = found
        device_type = device.device_type.value
        key = setting_key(device_type, str(params.get("setting") or params.get("key") or ""))
        channel = channel_number(device, params.get("channel")) if is_channel_key(device_type, key) else None
        if is_channel_key(device_type, key) and channel is None and key not in DEVICE_LEVEL_OPTIONAL:
            raise ValueError(f"{key} is a channel setting; give the channel")
        if key == "NET_SETTINGS":
            value = network_value(params.get("value"))
        else:
            value = formatted_value(device_type, key, params.get("value"))
        if key.startswith("SLOT_"):
            value = f"{slot_number(params.get('slot'))} {value}"
        current = self.shure.current_value(mac, key, channel)
        return {
            "mac": mac,
            "device": device.name,
            "device_type": device_type,
            "channel": channel,
            "channel_name": device.channels[channel].name if channel is not None else None,
            "key": key,
            "value": value,
            "current": current,
            "current_described": described_value(device_type, key, current),
            "requested_described": described_value(device_type, key, value),
        }

    async def _handle_shure_set(self, writer, params) -> None:
        try:
            change = self.wireless_change(params)
        except ValueError as exception:
            await self._send_json(writer, {"error": str(exception)}, 400)
            return
        try:
            reported = await self.shure.set_value(change["mac"], change["key"], change["value"], change["channel"])
        except ConnectionError as exception:
            await self._send_json(writer, {"error": str(exception)}, 503)
            return
        except ValueError as exception:
            await self._send_json(writer, {**change, "error": str(exception)}, 409)
            return
        verified = change["key"] in {"FLASH", "NET_SETTINGS"} or (
            isinstance(reported, str) and comparable(reported) == comparable(change["value"])
        )
        logger.info(
            "Shure %s %s set %s to %s (device reports %s)",
            change["device"],
            f"channel {change['channel']}" if change["channel"] is not None else "device",
            change["key"],
            change["value"],
            reported,
        )
        await self._send_json(
            writer,
            {
                **change,
                "reported": reported,
                "reported_described": described_value(change["device_type"], change["key"], reported),
                "verified": verified,
            },
            200 if verified else 502,
        )
