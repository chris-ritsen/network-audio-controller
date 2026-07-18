import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest
from typer.testing import CliRunner

from netaudio._common import _make_core_sender, readback_after_notification
from netaudio.commands import channel as channel_commands
from netaudio.commands import config as config_commands
from netaudio.commands import device as device_commands
from netaudio.dante.device_operations import DanteDeviceOperations
from netaudio.dante.events import DanteEventDispatcher
from netaudio.dante.services.notification import DanteNotificationService


runner = CliRunner()


class FakeOperations:
    def __init__(self, *, settings=None, aes67=None):
        self.settings = settings
        self.aes67 = aes67
        self.settings_calls = 0
        self.aes67_calls = 0

    async def get_device_settings(self):
        self.settings_calls += 1
        return _next_value(self.settings)

    async def get_aes67_configured(self):
        self.aes67_calls += 1
        return _next_value(self.aes67)


class FakeDevice:
    def __init__(self, name, *, name_reads=None, settings=None, aes67=None, ipv4="192.0.2.10"):
        self.name = name
        self.ipv4 = ipv4
        self.mac_address = "00:1D:C1:00:00:01"
        self.model_id = "fake"
        self.services = {}
        self.aes67_current = None
        self.aes67_configured = None
        self.operations = FakeOperations(settings=settings, aes67=aes67)
        self._name_reads = name_reads
        self.name_read_calls = 0

    async def fetch_device_name(self):
        self.name_read_calls += 1
        return _next_value(self._name_reads)


class FakeChannelDevice(FakeDevice):
    def __init__(self, *, channel_reads):
        super().__init__("AVIO")
        self.tx_channels = {1: SimpleNamespace(number=1, name="Input-1", friendly_name="Old", volume=2)}
        self.rx_channels = {}
        self._channel_reads = channel_reads
        self.channel_read_calls = 0

    async def get_tx_channels(self):
        self.channel_read_calls += 1
        self.tx_channels[1].friendly_name = _next_value(self._channel_reads)


def _next_value(value):
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


def _install_context(monkeypatch, module, devices, *, send_error_for=None):
    sent = []

    async def send(packet, ipv4, port, **kwargs):
        sent.append((str(ipv4), port, packet, kwargs))
        if str(ipv4) == send_error_for:
            raise OSError("send failed")
        return b"an ACK is deliberately not authoritative"

    @asynccontextmanager
    async def command_context():
        yield devices, send

    monkeypatch.setattr(module, "_command_context", command_context)
    return sent


@pytest.fixture(autouse=True)
def reset_cli_state():
    from netaudio.cli import state

    original = (
        list(state.names),
        list(state.hosts),
        list(state.server_names),
        list(state.macs),
        state.sort_field,
        state.sort_reverse,
    )
    state.names = []
    state.hosts = []
    state.server_names = []
    state.macs = []
    state.sort_field = "mac"
    state.sort_reverse = False
    try:
        yield
    finally:
        (
            state.names,
            state.hosts,
            state.server_names,
            state.macs,
            state.sort_field,
            state.sort_reverse,
        ) = original


@pytest.mark.asyncio
async def test_readback_after_notification_reads_once():
    reads = []

    async def read():
        reads.append(True)
        return "wanted"

    result = await readback_after_notification(read, "wanted")

    assert result.matched is True
    assert result.observed == "wanted"
    assert reads == [True]


@pytest.mark.asyncio
async def test_readback_after_notification_distinguishes_mismatch_from_unavailable():
    async def mismatch():
        return "old"

    mismatch_result = await readback_after_notification(mismatch, "new")
    assert mismatch_result.matched is False
    assert mismatch_result.observed_available is True
    assert mismatch_result.observed == "old"
    assert mismatch_result.error is None

    async def unavailable():
        raise TimeoutError("no reply")

    unavailable_result = await readback_after_notification(unavailable, "new")
    assert unavailable_result.matched is False
    assert unavailable_result.observed_available is False
    assert isinstance(unavailable_result.error, TimeoutError)


@pytest.mark.asyncio
async def test_notification_waiters_are_device_scoped_and_concurrent():
    service = DanteNotificationService(DanteEventDispatcher())
    first = service.register_notification_waiter("192.0.2.10", (128,))
    second = service.register_notification_waiter("192.0.2.10", (128, 262))
    other_device = service.register_notification_waiter("192.0.2.11", (128,))

    service._notify_notification_waiters("192.0.2.10", 128)

    await asyncio.wait_for(first.event.wait(), timeout=1)
    await asyncio.wait_for(second.event.wait(), timeout=1)
    assert not other_device.event.is_set()
    assert first.notification_id == 128
    service.unregister_notification_waiter(first)
    service.unregister_notification_waiter(second)
    service.unregister_notification_waiter(other_device)


@pytest.mark.asyncio
async def test_core_sender_forwards_fire_and_repeat_options(monkeypatch):
    from netaudio import core

    calls = []

    class FakeClient:
        observer = None

        def __init__(self, device_ip):
            self.device_ip = device_ip

        def set_host_mac(self, _mac):
            return None

        def request(self, packet, port, expect_response, repeat, interval_ms):
            calls.append((packet, port, expect_response, repeat, interval_ms))
            return None

    monkeypatch.setattr(core, "CoreClient", FakeClient)
    monkeypatch.setattr(core, "host_mac", lambda: None)
    send = _make_core_sender()

    await send(
        b"request",
        "192.0.2.10",
        8700,
        expect_response=False,
        repeat=3,
        interval_ms=100,
    )

    assert calls == [(b"request", 8700, False, 3, 100)]


@pytest.mark.asyncio
async def test_operations_aes67_getter_updates_configured_state():
    class FakeClient:
        def get_aes67_configured(self):
            return True

    class Device:
        aes67_configured = None

        def _core_client(self):
            return FakeClient()

    device = Device()
    operations = DanteDeviceOperations(device)

    assert await operations.get_aes67_configured() is True
    assert device.aes67_configured is True


@pytest.mark.asyncio
async def test_reboot_operation_uses_core_repeat_sender_when_discovery_application_is_stopped():
    calls = []

    class FakeClient:
        def request(self, packet, port, expect_response, repeat, interval_milliseconds):
            calls.append((packet, port, expect_response, repeat, interval_milliseconds))

    class Commands:
        def command_reboot(self, host_mac):
            assert host_mac == b"\x01\x02\x03\x04\x05\x06"
            return b"reboot", None, 8700

    class Device:
        commands = Commands()

        def _core_client(self):
            return FakeClient()

    operations = DanteDeviceOperations(Device())

    await operations.reboot(
        host_mac=b"\x01\x02\x03\x04\x05\x06",
        retries=3,
        retry_delay=0.1,
    )

    assert calls == [(b"reboot", 8700, False, 3, 100)]


def test_device_name_only_reports_success_after_matching_readback(monkeypatch):
    device = FakeDevice("Old", name_reads="New")
    _install_context(monkeypatch, device_commands, {"old.local.": device})

    result = runner.invoke(device_commands.app, ["name", "New"])

    assert result.exit_code == 0
    assert "Set name: New (verified)" in result.output
    assert device.name_read_calls == 1


def test_device_name_mismatch_is_not_reported_as_success(monkeypatch):
    device = FakeDevice("Old", name_reads="Old")
    _install_context(monkeypatch, device_commands, {"old.local.": device})

    result = runner.invoke(device_commands.app, ["name", "New"])

    assert result.exit_code == 1
    assert "device reports 'Old' instead of 'New'" in result.output
    assert "Set name:" not in result.output


def test_device_name_unavailable_readback_is_honest(monkeypatch):
    device = FakeDevice("Old", name_reads=TimeoutError("no reply"))
    _install_context(monkeypatch, device_commands, {"old.local.": device})

    result = runner.invoke(device_commands.app, ["name", "New"])

    assert result.exit_code == 1
    assert "readback was unavailable" in result.output
    assert "change was not verified" in result.output


def test_device_name_send_failure_is_clean_and_skips_readback(monkeypatch):
    device = FakeDevice("Old", name_reads="New")
    _install_context(
        monkeypatch,
        device_commands,
        {"old.local.": device},
        send_error_for="192.0.2.10",
    )

    result = runner.invoke(device_commands.app, ["name", "New"])

    assert result.exit_code == 1
    assert "could not send name change to Old: send failed" in result.output
    assert device.name_read_calls == 0


def test_device_name_reset_is_requested_but_not_claimed_verified(monkeypatch):
    device = FakeDevice("Old", name_reads="unexpected")
    _install_context(monkeypatch, device_commands, {"old.local.": device})

    result = runner.invoke(device_commands.app, ["name", ""])

    assert result.exit_code == 0
    assert "Name reset requested for old.local.; not verified" in result.output
    assert device.name_read_calls == 0


def test_channel_name_only_reports_success_after_matching_readback(monkeypatch):
    device = FakeChannelDevice(channel_reads="New")
    _install_context(monkeypatch, channel_commands, {"avio.local.": device})

    result = runner.invoke(channel_commands.app, ["name", "1", "New", "--type", "tx"])

    assert result.exit_code == 0
    assert "Set channel name: New (verified)" in result.output
    assert device.channel_read_calls == 1


def test_channel_name_mismatch_is_not_reported_as_success(monkeypatch):
    device = FakeChannelDevice(channel_reads="Old")
    _install_context(monkeypatch, channel_commands, {"avio.local.": device})

    result = runner.invoke(channel_commands.app, ["name", "1", "New", "--type", "tx"])

    assert result.exit_code == 1
    assert "device reports 'Old' instead of 'New'" in result.output
    assert "Set channel name:" not in result.output


def test_channel_name_reset_and_gain_use_unverified_requested_language(monkeypatch):
    device = FakeChannelDevice(channel_reads="unused")
    _install_context(monkeypatch, channel_commands, {"avio.local.": device})

    reset_result = runner.invoke(channel_commands.app, ["name", "1", "", "--type", "tx"])
    gain_result = runner.invoke(channel_commands.app, ["gain", "1", "3", "--type", "tx"])

    assert reset_result.exit_code == 0
    assert "Channel name reset requested" in reset_result.output
    assert "not verified" in reset_result.output
    assert gain_result.exit_code == 0
    assert "Gain change requested: 3; not verified" in gain_result.output
    assert device.channel_read_calls == 0


def test_gain_rejects_fractional_level_before_discovery(monkeypatch):
    async def should_not_run():
        raise AssertionError("fractional gain must be rejected before command execution")

    monkeypatch.setattr(channel_commands, "_command_context", should_not_run)

    result = runner.invoke(channel_commands.app, ["gain", "1", "1.9", "--type", "tx"])

    assert result.exit_code != 0
    assert "not a valid integer" in result.output


def test_channel_commands_reject_unknown_channel_type_before_sending(monkeypatch):
    device = FakeChannelDevice(channel_reads="unused")
    sent = _install_context(monkeypatch, channel_commands, {"avio.local.": device})

    result = runner.invoke(channel_commands.app, ["name", "1", "New", "--type", "banana"])

    assert result.exit_code == 1
    assert "channel type must be 'tx' or 'rx'" in result.output
    assert sent == []


def test_sample_rate_all_aggregates_readback_failures(monkeypatch):
    good = FakeDevice("Good", settings={"sample_rate": 48000}, ipv4="192.0.2.10")
    stale = FakeDevice("Stale", settings={"sample_rate": 44100}, ipv4="192.0.2.11")
    sent = _install_context(
        monkeypatch,
        config_commands,
        {"good.local.": good, "stale.local.": stale},
    )
    result = runner.invoke(config_commands.app, ["sample-rate", "48000", "--all"])

    assert result.exit_code == 1
    assert len(sent) == 2
    assert "Set sample rate for Good: 48000 Hz (verified)" in result.output
    assert "sample rate change sent to Stale" in result.output
    assert "44100 instead of 48000" in result.output


def test_fractional_latency_verifies_rounded_nanoseconds(monkeypatch):
    device = FakeDevice("AVIO", settings={"latency_ns": 150_000})
    _install_context(monkeypatch, config_commands, {"avio.local.": device})
    result = runner.invoke(config_commands.app, ["latency", "0.15"])

    assert result.exit_code == 0
    assert "Set latency for AVIO: 0.15 ms (verified)" in result.output


def test_latency_get_uses_active_device_readback(monkeypatch):
    device = FakeDevice("AVIO", settings={"latency_ns": 150_000})
    device.latency = 99.0
    _install_context(monkeypatch, config_commands, {"avio.local.": device})

    result = runner.invoke(config_commands.app, ["latency"])

    assert result.exit_code == 0
    assert result.output.strip() == "0.15"
    assert device.operations.settings_calls == 1


@pytest.mark.parametrize("value", ["-0.1", "nan", "inf"])
def test_latency_rejects_nonfinite_or_negative_values_before_sending(monkeypatch, value):
    device = FakeDevice("AVIO", settings={"latency_ns": 0})
    sent = _install_context(monkeypatch, config_commands, {"avio.local.": device})

    result = runner.invoke(config_commands.app, ["latency", "--", value])

    assert result.exit_code == 1
    assert "finite, nonnegative" in result.output
    assert sent == []


def test_aes67_verifies_configured_state_not_current_state(monkeypatch):
    device = FakeDevice("AVIO", aes67=True)
    device.aes67_current = False
    sent = _install_context(monkeypatch, config_commands, {"avio.local.": device})
    result = runner.invoke(config_commands.app, ["aes67", "on"])

    assert result.exit_code == 0
    assert device.operations.aes67_calls == 1
    assert "AES67 configured state for AVIO: on (verified)" in result.output
    assert sent[0][3] == {"expect_response": False, "repeat": 3, "interval_ms": 100}


@pytest.mark.parametrize(
    ("arguments", "expected", "send_kwargs"),
    [
        (["encoding", "24"], "Encoding change requested for AVIO: 24-bit; not verified", {}),
        (
            ["preferred-leader", "on"],
            "Preferred leader change requested for AVIO: on; not verified",
            {"expect_response": False, "repeat": 3, "interval_ms": 500},
        ),
        (
            ["interface", "dhcp"],
            "Interface change requested for AVIO: dhcp; not verified",
            {"expect_response": False},
        ),
    ],
)
def test_commands_without_readback_use_requested_language(monkeypatch, arguments, expected, send_kwargs):
    device = FakeDevice("AVIO")
    sent = _install_context(monkeypatch, config_commands, {"avio.local.": device})

    result = runner.invoke(config_commands.app, arguments)

    assert result.exit_code == 0
    assert expected in result.output
    assert sent[0][3] == send_kwargs
