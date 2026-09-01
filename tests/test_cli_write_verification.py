import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from typer.testing import CliRunner

from netaudio.asynchronous_primitives import DeferredAsyncioLock
from netaudio._common import CoreCommandSender, _make_core_sender, readback_after_notification
from netaudio.commands import channel as channel_commands
from netaudio.commands import device as device_commands
from netaudio.dante.device_operations import DanteDeviceOperations
from netaudio.dante.events import DanteEventDispatcher
from netaudio.dante.services.notification import (
    DanteNotificationService,
    mutate_and_wait_for_capability_value,
)


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

    async def get_latency_settings(self):
        self.settings_calls += 1
        return _next_value(self.settings)

    async def get_aes67_configured(self):
        self.aes67_calls += 1
        return _next_value(self.aes67)


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
    ):
        self.name = name
        self.ipv4 = ipv4
        self.mac_address = "00:1D:C1:00:00:01"
        self.model_id = "fake"
        self.services = {}
        self.sample_rate = None
        self.supported_sample_rates = supported_sample_rates
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
        self.operations = FakeOperations(settings=settings, aes67=aes67)
        self._name_reads = name_reads
        self.name_read_calls = 0

    async def fetch_device_name(self):
        self.name_read_calls += 1
        return _next_value(self._name_reads)


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
        channel = SimpleNamespace(number=1, name="Input-1", friendly_name="Old", volume=2)
        self.tx_channels = {1: channel} if channel_type == "tx" else {}
        self.rx_channels = {1: channel} if channel_type == "rx" else {}
        self.receiver_channel_name_protocol_identifier = None
        self.transmitter_channel_name_protocol_identifier = 0x2729
        self._channel_reads = channel_reads
        self.channel_read_calls = 0
        self._gain_probe_status = gain_status
        self._gain_write_status = gain_write_status
        if gain_status is not None:
            self.gain_device_type, self.gain_levels = gain_status
            self.supported_gain_levels = [1, 2, 3, 4, 5]

    async def get_tx_channels(self):
        self.channel_read_calls += 1
        self.tx_channels[1].friendly_name = _next_value(self._channel_reads)

    async def get_rx_channels(self):
        self.channel_read_calls += 1
        self.rx_channels[1].name = _next_value(self._channel_reads)

    def gain_level_for_channel(self, channel_number, channel_type):
        if self.gain_device_type != "input" or channel_type != "tx" or self.gain_levels is None:
            return None
        return self.gain_levels[channel_number - 1] if 0 < channel_number <= len(self.gain_levels) else None


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


def _install_context(
    monkeypatch,
    module,
    devices,
    *,
    send_error_for=None,
    notification_timeout=False,
    send_responses=None,
):
    sent = []

    async def send(packet, ipv4, port, **kwargs):
        sent.append((str(ipv4), port, packet, kwargs))
        if str(ipv4) == send_error_for:
            raise OSError("send failed")
        if send_responses is not None:
            return _next_value(send_responses)
        if len(packet) >= 10 and int.from_bytes(packet[0:2], "big") in {0x2729, 0x27FF, 0x2801, 0x2809}:
            response = bytearray(packet)
            response[8:10] = b"\x00\x01"
            return bytes(response)
        return b"an ACK is deliberately not authoritative"

    async def probe_sample_rate_status(ipv4):
        device = next(device for device in devices.values() if str(device.ipv4) == str(ipv4))
        settings = await device.operations.get_device_settings()
        if not isinstance(settings, dict) or settings.get("sample_rate") is None:
            raise RuntimeError("sample rate status unavailable")
        current_sample_rate = settings["sample_rate"]
        supported_sample_rates = device.supported_sample_rates or [current_sample_rate]
        return current_sample_rate, supported_sample_rates

    async def probe_encoding_status(ipv4):
        device = next(device for device in devices.values() if str(device.ipv4) == str(ipv4))
        if device.encoding is None or device.supported_encodings is None:
            raise RuntimeError("encoding status unavailable")
        return device.encoding, device.supported_encodings

    async def probe_gain_status(ipv4):
        device = next(device for device in devices.values() if str(device.ipv4) == str(ipv4))
        return device._gain_probe_status

    async def set_gain_level(ipv4, channel_number, gain_level, device_type):
        device = next(device for device in devices.values() if str(device.ipv4) == str(ipv4))
        if device._gain_write_status == "applied":
            channel_levels = list(device.gain_levels or [gain_level])
            channel_levels[channel_number - 1] = gain_level
            device.gain_device_type = device_type
            device.gain_levels = channel_levels
            device.supported_gain_levels = [1, 2, 3, 4, 5]
            return device_type, channel_levels
        return device._gain_write_status

    send.probe_sample_rate_status = probe_sample_rate_status
    send.probe_encoding_status = probe_encoding_status
    send.probe_gain_status = probe_gain_status
    send.set_gain_level = set_gain_level

    async def send_and_wait_for_capability_value(
        packet,
        ipv4,
        port,
        _capability_name,
        _expected_value,
        probe_status,
        **kwargs,
    ):
        await send(packet, ipv4, port, **kwargs)
        return await probe_status()

    send.send_and_wait_for_capability_value = send_and_wait_for_capability_value
    if notification_timeout:

        async def send_and_wait_for_notification(packet, ipv4, port, _notification_ids, **kwargs):
            await send(packet, ipv4, port, **kwargs)
            raise TimeoutError("device sent no mutation notification")

        send.send_and_wait_for_notification = send_and_wait_for_notification

    @asynccontextmanager
    async def command_context():
        yield devices, send

    monkeypatch.setattr(module, "_command_context", command_context)
    if hasattr(module, "change_sample_rate_with_command_sender"):
        from tests.sample_rate_test_support import make_fake_sample_rate_change

        monkeypatch.setattr(
            module,
            "change_sample_rate_with_command_sender",
            make_fake_sample_rate_change(send, probe_sample_rate_status),
        )
    return sent


def _make_audio_capability_operations(command_method_name, packet, supported_values_field, supported_values):
    command_builder = MagicMock(return_value=(packet, None, 8700))
    commands = SimpleNamespace(**{command_method_name: command_builder})
    device = SimpleNamespace(
        commands=commands,
        dante_send_command=AsyncMock(),
    )
    setattr(device, supported_values_field, supported_values)
    return DanteDeviceOperations(device), commands, device


def _make_encoding_operations(supported_encodings):
    return _make_audio_capability_operations(
        "command_set_encoding",
        b"encoding",
        "supported_encodings",
        supported_encodings,
    )


async def _assert_encoding_operation_sends(supported_encodings):
    operations, commands, device = _make_encoding_operations(supported_encodings)

    result = await operations.set_encoding(32)

    assert result is None
    commands.command_set_encoding.assert_called_once_with(32)
    device.dante_send_command.assert_awaited_once_with(b"encoding", None, 8700)


@pytest.fixture(autouse=True)
def _reset_cli_state_for_module(reset_cli_state):
    yield


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
async def test_core_sender_starts_one_notification_service_for_concurrent_probes(monkeypatch):
    notification_start_entered = asyncio.Event()
    allow_notification_start = asyncio.Event()
    notification_start_calls = []

    async def start_notification_service(notification_service):
        notification_start_calls.append(notification_service)
        notification_start_entered.set()
        await allow_notification_start.wait()

    monkeypatch.setattr(DanteNotificationService, "start", start_notification_service)
    send = _make_core_sender()

    first_start = asyncio.create_task(send._ensure_notifications())
    await notification_start_entered.wait()
    second_start = asyncio.create_task(send._ensure_notifications())
    allow_notification_start.set()
    first_service, second_service = await asyncio.gather(first_start, second_start)

    assert first_service is second_service
    assert notification_start_calls == [first_service]
    await send.close()


@pytest.mark.asyncio
async def test_core_sender_capability_verification_ignores_old_and_unrelated_status(monkeypatch):
    notifications = DanteNotificationService(DanteEventDispatcher())
    sender = CoreCommandSender()
    sender._ensure_notifications = AsyncMock(return_value=notifications)
    mutation_sent = asyncio.Event()
    old_status_observed = asyncio.Event()

    async def send_packet(
        _sender,
        _packet,
        _device_ip_address,
        _port,
        **_send_options,
    ):
        mutation_sent.set()

    async def probe_status():
        notifications._notify_capability_value_waiters(
            "sample_rate",
            "192.0.2.10",
            48_000,
            [48_000, 96_000],
        )
        old_status_observed.set()
        return 48_000, [48_000, 96_000]

    monkeypatch.setattr(CoreCommandSender, "__call__", send_packet)
    verification_task = asyncio.create_task(
        sender.send_and_wait_for_capability_value(
            b"set sample rate",
            "192.0.2.10",
            8700,
            "sample_rate",
            96_000,
            probe_status,
            capability_timeout=1,
            expect_response=False,
        )
    )
    await mutation_sent.wait()
    await old_status_observed.wait()

    assert not verification_task.done()
    notifications._notify_capability_value_waiters(
        "sample_rate",
        "192.0.2.11",
        96_000,
        [48_000, 96_000],
    )
    notifications._notify_capability_value_waiters(
        "encoding",
        "192.0.2.10",
        96_000,
        [48_000, 96_000],
    )
    assert not verification_task.done()
    notifications._notify_capability_value_waiters(
        "sample_rate",
        "192.0.2.10",
        96_000,
        [48_000, 96_000],
    )

    assert await verification_task == (96_000, [48_000, 96_000])


@pytest.mark.asyncio
async def test_capability_value_waiter_unregisters_after_timeout():
    notifications = DanteNotificationService(DanteEventDispatcher())

    async def mutate():
        return None

    async def probe_status():
        return 48_000, [48_000, 96_000]

    status = await mutate_and_wait_for_capability_value(
        notifications,
        "sample_rate",
        "192.0.2.10",
        96_000,
        mutate,
        probe_status,
        0.01,
    )

    assert status == (48_000, [48_000, 96_000])
    assert notifications._capability_value_waiters == {}


@pytest.mark.asyncio
async def test_operations_aes67_getter_updates_configured_state():
    class FakeClient:
        _arc_port = 4440

        def request(self, packet, port):
            return bytes.fromhex(
                "28090094180011000001171702010001820400688205006c021000100211001000008218000082198301007083020074830600780310001003110010030300028021007c000000f08060008c002200010063000300000064000000650222138c0212003083210090000f4240000f4240000f42400135f1b4000f424000000000000000000000000000000000ef450000001e8480"
            )

    class Device:
        aes67_configured = None
        aes67_multicast_prefix = None

        def _core_client(self):
            return FakeClient()

    device = Device()
    operations = DanteDeviceOperations(device)

    assert await operations.get_aes67_configured() is True
    assert device.aes67_configured is True
    assert device.aes67_multicast_prefix == "239.69.0.0"


@pytest.mark.asyncio
async def test_reboot_operation_registers_with_active_application_before_sending():
    calls = []

    class FakeClient:
        def request(self, packet, port, expect_response, repeat, interval_milliseconds):
            calls.append(("reboot", packet, port, expect_response, repeat, interval_milliseconds))

    class Commands:
        def command_reboot(self, host_mac):
            assert host_mac == b"\x01\x02\x03\x04\x05\x06"
            return b"reboot", None, 8700

    class Device:
        commands = Commands()
        ipv4 = "192.168.1.61"

        def _core_client(self):
            return FakeClient()

    application_service = SimpleNamespace(is_started=True)

    async def require_registration(device_ip_address, host_media_access_control_address):
        calls.append(("registration", device_ip_address, host_media_access_control_address))

    application_service.require_registration = require_registration
    device = Device()
    device._app = SimpleNamespace(cmc=application_service)
    operations = DanteDeviceOperations(device)

    await operations.reboot(host_mac=b"\x01\x02\x03\x04\x05\x06")

    assert calls == [
        ("registration", "192.168.1.61", b"\x01\x02\x03\x04\x05\x06"),
        ("reboot", b"reboot", 8700, False, 1, 0),
    ]


@pytest.mark.asyncio
async def test_factory_reset_operation_registers_before_sending():
    calls = []

    class FakeClient:
        def request(self, packet, port, expect_response, repeat, interval_milliseconds):
            calls.append(("factory_reset", packet, port, expect_response, repeat, interval_milliseconds))

    class Commands:
        def command_factory_reset(self, host_mac):
            assert host_mac == b"\x01\x02\x03\x04\x05\x06"
            return b"factory-reset", None, 8700

    class Device:
        commands = Commands()
        ipv4 = "192.168.1.61"

        def _core_client(self):
            return FakeClient()

    application_service = SimpleNamespace(is_started=True)

    async def require_registration(device_ip_address, host_media_access_control_address):
        calls.append(("registration", device_ip_address, host_media_access_control_address))

    application_service.require_registration = require_registration
    device = Device()
    device._app = SimpleNamespace(cmc=application_service)
    operations = DanteDeviceOperations(device)

    await operations.factory_reset(host_mac=b"\x01\x02\x03\x04\x05\x06")

    assert calls == [
        ("registration", "192.168.1.61", b"\x01\x02\x03\x04\x05\x06"),
        ("factory_reset", b"factory-reset", 8700, False, 1, 0),
    ]


@pytest.mark.asyncio
async def test_reboot_operation_owns_temporary_registration_service_without_active_application(monkeypatch):
    calls = []

    class FakeClient:
        def request(self, packet, port, expect_response, repeat, interval_milliseconds):
            calls.append(("reboot", packet, port, expect_response, repeat, interval_milliseconds))

    class Commands:
        def command_reboot(self, host_mac):
            return b"reboot", None, 8700

    class Device:
        commands = Commands()
        ipv4 = "192.168.1.61"
        _app = None

        def _core_client(self):
            return FakeClient()

    class OperationService:
        def __init__(self, host_media_access_control_address):
            calls.append(("created", host_media_access_control_address))

        async def start(self):
            calls.append(("started",))

        async def require_registration(self, device_ip_address, host_media_access_control_address):
            calls.append(("registration", device_ip_address, host_media_access_control_address))

        async def stop(self):
            calls.append(("stopped",))

    from netaudio.dante.services import cmc as control_monitoring_module

    monkeypatch.setattr(control_monitoring_module, "DanteCMCService", OperationService)
    operations = DanteDeviceOperations(Device())

    await operations.reboot(host_mac=b"\x01\x02\x03\x04\x05\x06")

    assert calls == [
        ("created", b"\x01\x02\x03\x04\x05\x06"),
        ("started",),
        ("registration", "192.168.1.61", b"\x01\x02\x03\x04\x05\x06"),
        ("stopped",),
        ("reboot", b"reboot", 8700, False, 1, 0),
    ]


@pytest.mark.asyncio
async def test_reboot_operation_does_not_send_when_registration_fails():
    class Commands:
        def command_reboot(self, host_mac):
            raise AssertionError("reboot command must not be built after failed registration")

    class Device:
        commands = Commands()
        ipv4 = "192.168.1.61"

        def _core_client(self):
            raise AssertionError("reboot command must not be sent after failed registration")

    application_service = SimpleNamespace(
        is_started=True,
        require_registration=AsyncMock(side_effect=RuntimeError("CMC registration failed for 192.168.1.61")),
    )
    device = Device()
    device._app = SimpleNamespace(cmc=application_service)
    operations = DanteDeviceOperations(device)

    with pytest.raises(RuntimeError, match="CMC registration failed for 192.168.1.61"):
        await operations.reboot(host_mac=b"\x01\x02\x03\x04\x05\x06")


def test_identify_is_sent_without_waiting_for_a_response(monkeypatch):
    device = FakeDevice("AVIO")
    sent = _install_context(monkeypatch, device_commands, {"avio.local.": device})

    result = runner.invoke(device_commands.app, ["identify"])

    assert result.exit_code == 0
    assert len(sent) == 1
    assert sent[0][0:2] == ("192.0.2.10", 8700)
    assert sent[0][3] == {"expect_response": False}
    assert "Identified: AVIO" in result.output


def test_identify_refuses_multiple_devices_without_all(monkeypatch):
    first = FakeDevice("One")
    second = FakeDevice("Two")
    sent = _install_context(
        monkeypatch,
        device_commands,
        {"one.local.": first, "two.local.": second},
    )

    result = runner.invoke(device_commands.app, ["identify"])

    assert result.exit_code == 1
    assert sent == []
    assert "multiple devices matched" in result.output


def test_identify_all_sends_to_every_matched_device(monkeypatch):
    first = FakeDevice("One")
    second = FakeDevice("Two")
    sent = _install_context(
        monkeypatch,
        device_commands,
        {"one.local.": first, "two.local.": second},
    )

    result = runner.invoke(device_commands.app, ["identify", "--all"])

    assert result.exit_code == 0
    assert len(sent) == 2
    assert "Identified: One" in result.output
    assert "Identified: Two" in result.output


@pytest.mark.asyncio
async def test_encoding_operation_rejects_known_unsupported_value_without_sending():
    operations, commands, device = _make_encoding_operations([24])

    with pytest.raises(ValueError, match="requested encoding 32 is not supported"):
        await operations.set_encoding(32)

    commands.command_set_encoding.assert_not_called()
    device.dante_send_command.assert_not_awaited()


@pytest.mark.asyncio
async def test_encoding_operation_sends_known_supported_value():
    await _assert_encoding_operation_sends([16, 24, 32])


@pytest.mark.asyncio
async def test_encoding_operation_preserves_send_when_capabilities_are_unknown():
    await _assert_encoding_operation_sends(None)


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

    result = runner.invoke(channel_commands.app, ["name", "tx:1", "New"])

    assert result.exit_code == 0
    assert "Set channel name: New (verified)" in result.output
    assert device.channel_read_calls == 1


def test_channel_name_mismatch_is_not_reported_as_success(monkeypatch):
    device = FakeChannelDevice(channel_reads="Old")
    _install_context(monkeypatch, channel_commands, {"avio.local.": device})

    result = runner.invoke(channel_commands.app, ["name", "tx:1", "New"])

    assert result.exit_code == 1
    assert "device reports 'Old' instead of 'New'" in result.output
    assert "Set channel name:" not in result.output


def test_channel_name_reset_remains_explicitly_unverified(monkeypatch):
    device = FakeChannelDevice(channel_reads="unused")
    _install_context(monkeypatch, channel_commands, {"avio.local.": device})

    reset_result = runner.invoke(channel_commands.app, ["name", "tx:1", ""])

    assert reset_result.exit_code == 0
    assert "Channel name reset requested" in reset_result.output
    assert "not verified" in reset_result.output
    assert device.channel_read_calls == 0
