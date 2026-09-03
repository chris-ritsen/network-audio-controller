import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from netaudio.cli_support.execution import readback_after_notification
from netaudio.cli_support.selection import parse_channel_reference
from netaudio.commands import channel as channel_commands
from netaudio.commands.device import cli as device_commands
from netaudio.dante.application import DanteApplication
from netaudio.dante.core_transport import CoreTransport
from netaudio.dante.events import DanteEventDispatcher
from netaudio.dante.services.notification import (
    DanteNotificationService,
    mutate_and_wait_for_capability_value,
)

from tests.cli_test_support import FakeApplication, FakeChannelDevice, FakeDevice, invoke


class RecordingDevice:
    def __init__(self, ipv4="192.168.1.61", supported_encodings=None):
        self.ipv4 = ipv4
        self.executed = []
        self.supported_encodings = supported_encodings
        self.aes67_configured = None
        self.aes67_multicast_prefix = None
        self.application = None
        self.transport = CoreTransport()
        self.responses = []

    def _require_address(self):
        return str(self.ipv4)

    async def execute(self, specification):
        self.executed.append(specification)
        if self.responses:
            return self.responses.pop(0)
        return None


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

    service.notify_waiters("notification", "192.0.2.10", 128)

    await asyncio.wait_for(first.wait(), timeout=1)
    await asyncio.wait_for(second.wait(), timeout=1)
    assert not other_device.is_set()
    assert first.latest_result == 128
    service.unregister_waiter(first)
    service.unregister_waiter(second)
    service.unregister_waiter(other_device)
    assert service._waiters == {}


@pytest.mark.asyncio
async def test_transport_executes_specifications_through_one_cached_client(monkeypatch):
    from netaudio import core

    created = []

    class FakeClient:
        def __init__(self, device_ip, arc_port=4440, timeout_ms=1000, attempts=3):
            self.device_ip = device_ip
            self.arc_port = arc_port
            self.executed = []
            created.append(self)

        def set_host_mac(self, _mac):
            return None

        def execute(self, specification):
            self.executed.append(specification)
            return b"response"

        def close(self):
            return None

    monkeypatch.setattr(core, "CoreClient", FakeClient)
    monkeypatch.setattr(core, "host_mac", lambda: None)
    transport = CoreTransport()

    first = await transport.execute("192.0.2.10", {"command": "identify"}, arc_port=8700)
    second = await transport.execute("192.0.2.10", {"command": "reboot"}, arc_port=8700)
    transport.close()

    assert first == b"response"
    assert second == b"response"
    assert len(created) == 1
    assert created[0].arc_port == 8700
    assert created[0].executed == [{"command": "identify"}, {"command": "reboot"}]


@pytest.mark.asyncio
async def test_capability_verification_ignores_old_and_unrelated_status():
    application = DanteApplication()
    notifications = application.notifications
    device = SimpleNamespace(ipv4="192.0.2.10")
    mutation_sent = asyncio.Event()
    old_status_observed = asyncio.Event()

    async def mutate():
        mutation_sent.set()

    async def probe_status():
        notifications.notify_waiters("sample_rate", "192.0.2.10", (48_000, [48_000, 96_000]))
        old_status_observed.set()
        return 48_000, [48_000, 96_000]

    verification_task = asyncio.create_task(
        application.mutate_and_wait_for_capability_value(
            device,
            mutate,
            "sample_rate",
            96_000,
            probe_status,
            timeout=1,
        )
    )
    await mutation_sent.wait()
    await old_status_observed.wait()

    assert not verification_task.done()
    notifications.notify_waiters("sample_rate", "192.0.2.11", (96_000, [48_000, 96_000]))
    notifications.notify_waiters("encoding", "192.0.2.10", (96_000, [48_000, 96_000]))
    assert not verification_task.done()
    notifications.notify_waiters("sample_rate", "192.0.2.10", (96_000, [48_000, 96_000]))

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
    assert notifications._waiters == {}


@pytest.mark.asyncio
async def test_operations_aes67_getter_updates_configured_state():
    device = RecordingDevice()
    device.responses.append(
        bytes.fromhex(
            "28090094180011000001171702010001820400688205006c021000100211001000008218000082198301007083020074830600780310001003110010030300028021007c000000f08060008c002200010063000300000064000000650222138c0212003083210090000f4240000f4240000f42400135f1b4000f424000000000000000000000000000000000ef450000001e8480"
        )
    )
    application = DanteApplication()

    assert await application.get_aes67_configured(device) is True
    assert device.aes67_configured is True
    assert device.aes67_multicast_prefix == "239.69.0.0"
    assert device.executed == [{"command": "query_latency_config"}]


@pytest.mark.asyncio
async def test_reboot_operation_registers_with_active_application_before_sending():
    calls = []
    device = RecordingDevice()

    async def require_registration(device_ip_address, host_media_access_control_address):
        calls.append(("registration", device_ip_address, host_media_access_control_address))

    application = DanteApplication()
    application.cmc = SimpleNamespace(require_registration=require_registration)

    await application.reboot(device, host_mac=b"\x01\x02\x03\x04\x05\x06")

    assert calls == [("registration", "192.168.1.61", b"\x01\x02\x03\x04\x05\x06")]
    assert device.executed == [{"command": "reboot", "host_mac": "010203040506"}]


@pytest.mark.asyncio
async def test_factory_reset_operation_registers_before_sending():
    calls = []
    device = RecordingDevice()

    async def require_registration(device_ip_address, host_media_access_control_address):
        calls.append(("registration", device_ip_address, host_media_access_control_address))

    application = DanteApplication()
    application.cmc = SimpleNamespace(require_registration=require_registration)

    await application.factory_reset(device, host_mac=b"\x01\x02\x03\x04\x05\x06")

    assert calls == [("registration", "192.168.1.61", b"\x01\x02\x03\x04\x05\x06")]
    assert device.executed == [{"command": "factory_reset", "host_mac": "010203040506"}]


@pytest.mark.asyncio
async def test_reboot_operation_does_not_send_when_registration_fails():
    device = RecordingDevice()
    application = DanteApplication()
    application.cmc = SimpleNamespace(
        require_registration=AsyncMock(side_effect=RuntimeError("CMC registration failed for 192.168.1.61"))
    )

    with pytest.raises(RuntimeError, match="CMC registration failed for 192.168.1.61"):
        await application.reboot(device, host_mac=b"\x01\x02\x03\x04\x05\x06")

    assert device.executed == []


def test_identify_is_sent_without_waiting_for_a_response():
    device = FakeDevice("AVIO")
    application = FakeApplication({"avio.local.": device})

    result = invoke(device_commands.run_identify, application, application.devices, False)

    assert result.exit_code == 0
    assert [sent.operation for sent in application.sent] == ["identify"]
    assert application.sent[0].device is device
    assert "Identified: AVIO" in result.output


def test_identify_refuses_multiple_devices_without_all():
    application = FakeApplication({"one.local.": FakeDevice("One"), "two.local.": FakeDevice("Two")})

    result = invoke(device_commands.run_identify, application, application.devices, False)

    assert result.exit_code == 1
    assert application.sent == []
    assert "multiple devices matched: One, Two; narrow the filter or pass --all" in result.output


def test_identify_all_sends_to_every_matched_device():
    application = FakeApplication({"one.local.": FakeDevice("One"), "two.local.": FakeDevice("Two")})

    result = invoke(device_commands.run_identify, application, application.devices, True)

    assert result.exit_code == 0
    assert len(application.sent) == 2
    assert "Identified: One" in result.output
    assert "Identified: Two" in result.output


@pytest.mark.asyncio
async def test_encoding_operation_rejects_known_unsupported_value_without_sending():
    device = RecordingDevice(supported_encodings=[24])
    application = DanteApplication()
    application.transport = SimpleNamespace(execute=AsyncMock(return_value=None))

    with pytest.raises(ValueError, match="requested encoding 32 is not supported"):
        await application.send_set_encoding(device, 32)

    application.transport.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("supported_encodings", [[16, 24, 32], None])
async def test_encoding_operation_sends_supported_or_unknown_value(supported_encodings):
    device = RecordingDevice(supported_encodings=supported_encodings)
    application = DanteApplication()
    application.transport = SimpleNamespace(execute=AsyncMock(return_value=None))

    assert await application.send_set_encoding(device, 32) is None
    [(address, specification)] = [call.args for call in application.transport.execute.await_args_list]
    assert address == "192.168.1.61"
    assert specification["command"] == "set_encoding"
    assert specification["encoding"] == 32
    assert 1 <= specification["sequence"] <= 0xFFFF


def test_device_name_only_reports_success_after_matching_readback():
    device = FakeDevice("Old", name_reads="New")
    application = FakeApplication({"old.local.": device})

    result = invoke(device_commands.run_name, application, application.devices, "New")

    assert result.exit_code == 0
    assert "Set name: New (verified)" in result.output
    assert device.name_read_calls == 1
    assert application.sent[0].arguments == ("New",)


def test_single_device_name_read_is_labeled():
    device = FakeDevice("AVIO")
    application = FakeApplication({"avio.local.": device})

    result = invoke(device_commands.run_name, application, application.devices, None)

    assert result.exit_code == 0
    assert result.output == "Device name: AVIO\n"


def test_device_name_mismatch_is_not_reported_as_success():
    device = FakeDevice("Old", name_reads="Old")
    application = FakeApplication({"old.local.": device})

    result = invoke(device_commands.run_name, application, application.devices, "New")

    assert result.exit_code == 1
    assert "device reports 'Old' instead of 'New'" in result.output
    assert "Set name:" not in result.output


def test_device_name_unavailable_readback_is_honest():
    device = FakeDevice("Old", name_reads=TimeoutError("no reply"))
    application = FakeApplication({"old.local.": device})

    result = invoke(device_commands.run_name, application, application.devices, "New")

    assert result.exit_code == 1
    assert "readback was unavailable" in result.output
    assert "change was not verified" in result.output


def test_device_name_send_failure_is_clean_and_skips_readback():
    device = FakeDevice("Old", name_reads="New")
    application = FakeApplication({"old.local.": device}, send_error_for="192.0.2.10")

    result = invoke(device_commands.run_name, application, application.devices, "New")

    assert result.exit_code == 1
    assert "could not send name change to Old: send failed" in result.output
    assert device.name_read_calls == 0


def test_device_name_reset_is_requested_but_not_claimed_verified():
    device = FakeDevice("Old", name_reads="unexpected")
    application = FakeApplication({"old.local.": device})

    result = invoke(device_commands.run_name, application, application.devices, "")

    assert result.exit_code == 0
    assert "Name reset requested for old.local.; not verified" in result.output
    assert device.name_read_calls == 0


def test_channel_name_only_reports_success_after_matching_readback():
    device = FakeChannelDevice(channel_reads="New")
    application = FakeApplication({"avio.local.": device})

    result = invoke(
        channel_commands.run_channel_name,
        application,
        application.devices,
        parse_channel_reference("tx:1"),
        "New",
    )

    assert result.exit_code == 0
    assert "Set channel name: New (verified)" in result.output
    assert device.channel_read_calls == 1


def test_channel_name_mismatch_is_not_reported_as_success():
    device = FakeChannelDevice(channel_reads="Old")
    application = FakeApplication({"avio.local.": device})

    result = invoke(
        channel_commands.run_channel_name,
        application,
        application.devices,
        parse_channel_reference("tx:1"),
        "New",
    )

    assert result.exit_code == 1
    assert "device reports 'Old' instead of 'New'" in result.output
    assert "Set channel name:" not in result.output


def test_channel_name_reset_remains_explicitly_unverified():
    device = FakeChannelDevice(channel_reads="unused")
    application = FakeApplication({"avio.local.": device})

    reset_result = invoke(
        channel_commands.run_channel_name,
        application,
        application.devices,
        parse_channel_reference("tx:1"),
        "",
    )

    assert reset_result.exit_code == 0
    assert "Channel name reset requested" in reset_result.output
    assert "not verified" in reset_result.output
    assert device.channel_read_calls == 0
