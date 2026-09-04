from __future__ import annotations

import asyncio
import contextlib
import io
from dataclasses import dataclass, field
from types import SimpleNamespace

import click

from netaudio.asynchronous_primitives import DeferredAsyncioLock
from netaudio.dante.application import CapabilityProbeTimeout


@dataclass
class CommandResult:
    exit_code: int
    output: str
    exception: BaseException | None = None


def invoke(run, application, devices, *arguments, **options) -> CommandResult:
    buffer = io.StringIO()
    exit_code = 0
    exception = None
    with contextlib.redirect_stdout(buffer), contextlib.redirect_stderr(buffer):
        try:
            asyncio.run(run(application, devices, *arguments, **options))
        except click.exceptions.Exit as exit_exception:
            exit_code = exit_exception.exit_code
        except SystemExit as exit_exception:
            exit_code = exit_exception.code or 0
        except Exception as raised:
            exception = raised
            exit_code = 1
    return CommandResult(exit_code=exit_code, output=buffer.getvalue(), exception=exception)


def next_value(value):
    if isinstance(value, list):
        if len(value) > 1:
            item = value.pop(0)
        elif value:
            item = value[0]
        else:
            item = None
    else:
        item = value
    if isinstance(item, BaseException):
        raise item
    return item


class FakeDevice:
    def __init__(
        self,
        name,
        *,
        name_reads=None,
        settings=None,
        aes67=None,
        ipv4="192.0.2.10",
        encoding=None,
        supported_sample_rates=None,
        supported_encodings=None,
        aes67_supported=None,
        min_latency=None,
        max_latency=None,
        server_name=None,
    ):
        self.name = name
        self.server_name = server_name or f"{name.lower()}.local."
        self.ipv4 = ipv4
        self.mac_address = "00:1D:C1:00:00:01"
        self.model_id = "fake"
        self.services = {}
        self.online = True
        self.sample_rate = None
        self.supported_sample_rates = supported_sample_rates
        self.sample_rate_pullup_raw_value = None
        self.requested_sample_rate_pullup_raw_value = None
        self.supported_sample_rate_pullup_raw_values = None
        self.encoding = encoding
        self.supported_encodings = supported_encodings
        self.topology_mutation_lock = DeferredAsyncioLock()
        self.gain_device_type = None
        self.gain_levels = None
        self.supported_gain_levels = None
        self.min_latency = min_latency
        self.max_latency = max_latency
        self.aes67_current = None
        self.aes67_configured = None
        self.aes67_supported = aes67_supported
        self.aes67_multicast_prefix = None
        self.settings_properties = None
        self.clock_subdomain = None
        self.clock_source_code = None
        self.preferred_leader = None
        self.interfaces = None
        self.interface_pending_config = None
        self.tx_channels = {}
        self.rx_channels = {}
        self.subscriptions = []
        self.settings = settings
        self.aes67 = aes67
        self.settings_calls = 0
        self.aes67_calls = 0
        self._name_reads = name_reads
        self.name_read_calls = 0

    async def fetch_device_name(self):
        self.name_read_calls += 1
        return next_value(self._name_reads)


class FakeChannelDevice(FakeDevice):
    def __init__(
        self,
        *,
        channel_reads,
        channel_type="tx",
        gain_status=("input", [5]),
        gain_write_status="applied",
    ):
        super().__init__("AVIO")
        channel = SimpleNamespace(number=1, name="Input-1", friendly_name="Old", volume=2, factory_name=None)
        self.tx_channels = {1: channel} if channel_type == "tx" else {}
        self.rx_channels = {1: channel} if channel_type == "rx" else {}
        self.receiver_channel_name_protocol_identifier = None
        self.transmitter_channel_name_protocol_identifier = 0x2729
        self._channel_reads = channel_reads
        self.channel_read_calls = 0
        self.gain_probe_status = gain_status
        self.gain_write_status = gain_write_status
        if gain_status is not None:
            self.gain_device_type, self.gain_levels = gain_status
            self.supported_gain_levels = [1, 2, 3, 4, 5]

    async def get_tx_channels(self):
        self.channel_read_calls += 1
        self.tx_channels[1].friendly_name = next_value(self._channel_reads)

    async def get_rx_channels(self):
        self.channel_read_calls += 1
        self.rx_channels[1].name = next_value(self._channel_reads)

    def gain_level_for_channel(self, channel_number, channel_type):
        if self.gain_device_type != "input" or channel_type != "tx" or self.gain_levels is None:
            return None
        return self.gain_levels[channel_number - 1] if 0 < channel_number <= len(self.gain_levels) else None


@dataclass
class Sent:
    operation: str
    device: object
    arguments: tuple = ()


def _success_response(result_code: int = 0x0001) -> bytes:
    return bytes.fromhex("2809000a00003400") + result_code.to_bytes(2, "big")


@dataclass
class FakeApplication:
    devices: dict = field(default_factory=dict)
    send_error_for: str | None = None
    responses: list | None = None
    sent: list = field(default_factory=list)

    def _device_by_ip(self, device_ip_address):
        for device in self.devices.values():
            if str(device.ipv4) == str(device_ip_address):
                return device
        return None

    def _device(self, target):
        return target if hasattr(target, "ipv4") else self._device_by_ip(target)

    def _record(self, operation: str, device, *arguments):
        if str(device.ipv4) == self.send_error_for:
            raise OSError("send failed")
        self.sent.append(Sent(operation, device, arguments))
        if self.responses is not None:
            return next_value(self.responses)
        return _success_response()

    async def get_aes67_configured(self, device):
        device.aes67_calls += 1
        return next_value(device.aes67)

    async def get_device_settings(self, device):
        device.settings_calls += 1
        return next_value(device.settings)

    async def get_latency_settings(self, device):
        device.settings_calls += 1
        return next_value(device.settings)

    async def resolve_channel_name_protocol_identifier(self, device, channel_type):
        return 0x2809

    async def identify(self, device):
        self._record("identify", device)

    async def reboot(self, device):
        self._record("reboot", device)

    async def factory_reset(self, device):
        self._record("factory_reset", device)

    async def set_device_name(self, device, name):
        return self._record("set_device_name", device, name)

    async def reset_device_name(self, device):
        return self._record("reset_device_name", device)

    async def set_channel_name(self, device, channel_type, channel_number, name):
        return self._record("set_channel_name", device, channel_type, channel_number, name)

    async def reset_channel_name(self, device, channel_type, channel_number):
        return self._record("reset_channel_name", device, channel_type, channel_number)

    async def set_latency(self, device, milliseconds):
        return self._record("set_latency", device, milliseconds)

    async def set_encoding(self, device, encoding):
        self._record("set_encoding", device, encoding)
        return await self.probe_encoding_status(device)

    async def set_sample_rate_pullup(self, device, raw_value):
        return self._record("set_sample_rate_pullup", device, raw_value)

    async def set_aes67_enabled(self, device, is_enabled):
        return self._record("set_aes67_enabled", device, is_enabled)

    async def set_aes67_multicast_prefix(self, device, prefix):
        self._record("set_aes67_multicast_prefix", device, prefix)
        await self.get_aes67_configured(device)
        return device.aes67_multicast_prefix

    async def set_preferred_leader(self, device, is_preferred):
        return self._record("set_preferred_leader", device, is_preferred)

    async def set_clock_subdomain(self, device, subdomain):
        return self._record("set_clock_subdomain", device, subdomain)

    async def set_clock_source(self, device, source):
        self._record("set_clock_source", device, source)
        device.clock_source_code = source
        return source

    async def set_interface(self, device, mode, static_configuration=None):
        return self._record("set_interface", device, mode, static_configuration)

    async def add_subscriptions(self, device, records):
        return self._record("add_subscriptions", device, tuple(records))

    async def remove_subscriptions(self, device, channel_numbers):
        return self._record("remove_subscriptions", device, tuple(channel_numbers))

    async def clear_configuration(self, target, preserve_internet_protocol_settings):
        device = self._device(target)
        self._record("clear_configuration", device, preserve_internet_protocol_settings)
        return {
            "available_actions_mask": 3,
            "action_result_code": 2 if preserve_internet_protocol_settings else 1,
        }

    async def probe_sample_rate_status(self, target, timeout=2.0):
        device = self._device(target)
        settings = await self.get_device_settings(device)
        if not isinstance(settings, dict) or settings.get("sample_rate") is None:
            raise RuntimeError("sample rate status unavailable")
        current_sample_rate = settings["sample_rate"]
        return current_sample_rate, device.supported_sample_rates or [current_sample_rate]

    async def probe_encoding_status(self, target, timeout=2.0):
        device = self._device(target)
        if device.encoding is None or device.supported_encodings is None:
            raise RuntimeError("encoding status unavailable")
        return device.encoding, device.supported_encodings

    async def probe_sample_rate_pullup_status(self, target, timeout=2.0):
        device = self._device(target)
        if device.supported_sample_rate_pullup_raw_values is None:
            raise CapabilityProbeTimeout("sample rate pull-up readback timed out")
        return device.sample_rate_pullup_raw_value, device.supported_sample_rate_pullup_raw_values

    async def probe_gain_status(self, target, timeout=2.0):
        device = self._device(target)
        status = device.gain_probe_status
        if status is None:
            raise CapabilityProbeTimeout("gain status readback timed out")
        return status

    async def set_gain_level(self, device, channel_number, gain_level, device_type):
        if device.gain_write_status == "applied":
            channel_levels = list(device.gain_levels or [gain_level])
            channel_levels[channel_number - 1] = gain_level
            device.gain_device_type = device_type
            device.gain_levels = channel_levels
            device.supported_gain_levels = [1, 2, 3, 4, 5]
            return device_type, channel_levels
        return device.gain_write_status

    async def set_sample_rate(self, device, sample_rate, confirm_destructive=False, timeout=4.0):
        from tests.sample_rate_test_support import fake_sample_rate_change

        return await fake_sample_rate_change(self, device, sample_rate, confirm_destructive=confirm_destructive)

    async def probe_clocking_status(self, target, timeout=3.0):
        device = self._device(target)
        if device.clock_source_code is None and device.clock_subdomain is None:
            raise RuntimeError("clock status readback was unavailable")
        return {
            "clock_source_code": device.clock_source_code,
            "clock_subdomain": device.clock_subdomain,
        }

    async def apply_modern_arc_status_pages(self, device):
        return None
