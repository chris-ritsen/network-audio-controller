from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from netaudio.common.managed_api import ManagedAPIConfiguration
from netaudio.dante.application import DanteApplication
from netaudio.dante.device import DanteDevice
from netaudio.ddm import device_transport


API_KEY = "00000000-0000-4000-8000-000000000000"


class FakeClient:
    def __init__(self, result=None):
        self.result = result
        self.calls = []

    def read_credential(self):
        return API_KEY

    async def execute_async(self, query, variables, operation_name):
        self.calls.append((query, variables, operation_name))
        return self.result


def _configuration(**overrides):
    values = {
        "url": "http://ddm.example/graphql",
        "credential": API_KEY,
        "credential_file": None,
        "refresh_interval": 10.0,
    }
    values.update(overrides)
    return ManagedAPIConfiguration(**values)


def _device(*, enrolled=True, direct=True):
    device = DanteDevice("managed-device")
    device.name = "managed-device"
    device.ipv4 = "192.0.2.10"
    device.ddm_device_id = "001dc1fffe50692e:0"
    device.ddm_server_profile = "manager"
    device.ddm_context = "manager-main"
    device.ddm_domain_id = "11" * 16
    device.ddm_enrolment_state = "ENROLLED" if enrolled else "UNENROLLED"
    device.management_state = "managed" if enrolled else "unenrolled"
    device.direct_control_available = direct
    return device


def test_enrolment_metadata_selects_managed_control_even_when_direct_ip_exists():
    assert device_transport.device_requires_managed_control(_device(enrolled=True, direct=True)) is True
    assert device_transport.device_requires_managed_control(_device(enrolled=False, direct=True)) is False

    enrolled_without_id = _device(enrolled=True, direct=True)
    enrolled_without_id.ddm_device_id = None
    assert device_transport.device_requires_managed_control(enrolled_without_id) is True


@pytest.mark.asyncio
async def test_managed_device_without_an_id_fails_closed_instead_of_using_direct_control():
    device = _device()
    device.ddm_device_id = None
    transport = device_transport.ManagedDeviceTransport(_configuration(), client=FakeClient())

    with pytest.raises(device_transport.ManagedDeviceControlError, match="no DDM device ID"):
        await transport.execute(device, {"command": "identify"})


def test_application_routes_each_managed_device_to_its_originating_server_profile(monkeypatch, tmp_path):
    config_path = tmp_path / "config.toml"
    east_credential = tmp_path / "east.credential"
    west_credential = tmp_path / "west.credential"
    east_credential.write_text("00000000-0000-4000-8000-000000000001\n", encoding="ascii")
    west_credential.write_text("00000000-0000-4000-8000-000000000002\n", encoding="ascii")
    config_path.write_text(
        """
[ddm.servers.east]
url = "http://east.example/graphql"
credential_file = "east.credential"

[ddm.servers.west]
url = "http://west.example/graphql"
credential_file = "west.credential"

[ddm.contexts.east-main]
server = "east"
domain_id = "11111111111111111111111111111111"

[ddm.contexts.west-main]
server = "west"
domain_id = "22222222222222222222222222222222"
""".lstrip(),
        encoding="utf-8",
    )
    monkeypatch.setenv("NETAUDIO_CONFIG", str(config_path))
    east = _device()
    east.ddm_server_profile = "east"
    east.ddm_context = "east-main"
    east.ddm_domain_id = "11" * 16
    west = _device()
    west.ddm_server_profile = "west"
    west.ddm_context = "west-main"
    west.ddm_domain_id = "22" * 16
    application = DanteApplication()

    east_transport = application.managed_transport(east)
    west_transport = application.managed_transport(west)

    assert east_transport.configuration.name == "east"
    assert east_transport.configuration.url == "http://east.example/graphql"
    assert west_transport.configuration.name == "west"
    assert west_transport.configuration.url == "http://west.example/graphql"
    assert application.managed_transport(east) is east_transport

    west.ddm_context = "east-main"
    with pytest.raises(RuntimeError, match="belongs to server 'east', not 'west'"):
        application.managed_transport(west)


@pytest.mark.asyncio
async def test_managed_arc_normalizes_latency_to_2809_and_uses_controller_service(monkeypatch):
    built = []
    sent = []
    response = bytes.fromhex("2809000a123411010001")
    transport = device_transport.ManagedDeviceTransport(_configuration(), client=FakeClient())

    def build(specification):
        built.append(specification)
        return bytes.fromhex("2809000a123411010000")

    def query(server, credential, device_id, packet, **options):
        sent.append((server, credential, device_id, packet, options))
        return response

    monkeypatch.setattr(device_transport.core, "next_message_id", lambda: 0x1234)
    monkeypatch.setattr(device_transport.core, "build_command", build)
    monkeypatch.setattr(device_transport, "query_managed_arc_with_api_key", query)

    result = await transport.execute(_device(), {"command": "set_latency", "latency": 2.0})

    assert result == response
    assert built == [
        {
            "command": "set_latency",
            "latency": 2.0,
            "message_id": 0x1234,
            "protocol_id": 0x2809,
        }
    ]
    assert sent == [
        (
            "ddm.example",
            API_KEY,
            "001dc1fffe50692e:0",
            bytes.fromhex("2809000a123411010000"),
            {"expected_domain_id": "11" * 16},
        )
    ]


@pytest.mark.asyncio
async def test_private_managed_action_requires_the_device_records_domain(monkeypatch):
    captured = {}
    device = _device()
    device.ddm_domain_id = "11" * 16
    transport = device_transport.ManagedDeviceTransport(_configuration(), client=FakeClient())
    monkeypatch.setattr(device_transport.core, "host_mac", lambda: bytes.fromhex("001122334455"))

    def identify(server, credential, device_id, host_mac, **options):
        captured.update(options)

    monkeypatch.setattr(device_transport, "identify_managed_device_with_api_key", identify)

    await transport.execute(device, {"command": "identify"})

    assert captured == {"expected_domain_id": "11" * 16}


@pytest.mark.asyncio
async def test_managed_arc_normalizes_receiver_port_range_query_to_2809(monkeypatch):
    built = []
    transport = device_transport.ManagedDeviceTransport(_configuration(), client=FakeClient())

    def build(specification):
        built.append(specification)
        return bytes.fromhex("2809000a123433000000")

    monkeypatch.setattr(device_transport.core, "next_message_id", lambda: 0x1234)
    monkeypatch.setattr(device_transport.core, "build_command", build)
    monkeypatch.setattr(
        device_transport,
        "query_managed_arc_with_api_key",
        lambda *_args, **_kwargs: bytes.fromhex("2809000a123433000001"),
    )

    await transport.execute(_device(), {"command": "query_receiver_port_ranges"})

    assert built == [
        {
            "command": "query_receiver_port_ranges",
            "message_id": 0x1234,
            "protocol_id": 0x2809,
        }
    ]


@pytest.mark.asyncio
async def test_managed_settings_query_injects_host_mac_and_correlates_the_publication(monkeypatch):
    built = []
    sent = []
    publication = bytes.fromhex("ffff001c00010000001dc1fffe50692e417564696e61746507380080")
    transport = device_transport.ManagedDeviceTransport(_configuration(), client=FakeClient())

    monkeypatch.setattr(device_transport.core, "host_mac", lambda: bytes.fromhex("001122334455"))
    monkeypatch.setattr(device_transport.core, "next_message_id", lambda: 0x4321)

    def build(specification):
        built.append(specification)
        return bytes.fromhex("ffff0024002d7e3f0011223344550000417564696e617465073a00810000000000000000")

    def query(server, credential, device_id, packet, expected_opcode, **options):
        sent.append((server, credential, device_id, expected_opcode, options))
        return publication

    monkeypatch.setattr(device_transport.core, "build_command", build)
    monkeypatch.setattr(device_transport, "query_managed_settings_with_api_key", query)

    result = await transport.execute(_device(), {"command": "probe_sample_rate"})

    assert result == publication
    assert built == [
        {
            "command": "probe_sample_rate",
            "host_mac": "001122334455",
            "message_id": 0x4321,
        }
    ]
    assert sent[0][3] == 0x0080


@pytest.mark.asyncio
async def test_managed_transport_fails_closed_before_sending_unsupported_or_unverified_actions(monkeypatch):
    sent = MagicMock()
    transport = device_transport.ManagedDeviceTransport(_configuration(), client=FakeClient())
    monkeypatch.setattr(device_transport.core, "next_message_id", lambda: 1)
    monkeypatch.setattr(
        device_transport.core, "build_command", lambda specification: bytes.fromhex("27ff000a000110020000")
    )
    monkeypatch.setattr(device_transport, "query_managed_arc_with_api_key", sent)

    with pytest.raises(device_transport.ManagedDeviceControlError, match="not available through DDM"):
        await transport.execute(_device(), {"command": "device_name"})

    sent.assert_not_called()


@pytest.mark.asyncio
async def test_managed_subscription_changes_use_the_documented_graphql_mutation():
    result = SimpleNamespace(data={"DeviceRxChannelsSubscriptionSet": {"ok": True}}, errors=())
    client = FakeClient(result)
    transport = device_transport.ManagedDeviceTransport(_configuration(), client=client)

    accepted = await transport.set_subscriptions(_device(), [(2, "Left", "stagebox")])
    removed = await transport.remove_subscriptions(_device(), [2])

    assert accepted.successful is True
    assert removed.successful is True
    assert client.calls[0][1] == {
        "input": {
            "deviceId": "001dc1fffe50692e:0",
            "subscriptions": [{"rxChannelIndex": 2, "subscribedChannel": "Left", "subscribedDevice": "stagebox"}],
        }
    }
    assert client.calls[1][1]["input"]["subscriptions"] == [
        {"rxChannelIndex": 2, "subscribedChannel": "", "subscribedDevice": ""}
    ]


@pytest.mark.asyncio
async def test_managed_device_name_changes_use_the_documented_graphql_mutation():
    result = SimpleNamespace(data={"DeviceNameSet": {"ok": True}}, errors=())
    client = FakeClient(result)
    transport = device_transport.ManagedDeviceTransport(_configuration(), client=client)

    renamed = await transport.set_device_name(_device(), "Stage-IO")
    reset = await transport.reset_device_name(_device())

    assert renamed.successful is True
    assert reset.successful is True
    assert client.calls[0][1] == {"input": {"deviceId": "001dc1fffe50692e:0", "name": "Stage-IO"}}
    assert client.calls[1][1] == {"input": {"deviceId": "001dc1fffe50692e:0", "name": ""}}


@pytest.mark.asyncio
async def test_managed_preferred_leader_change_uses_the_documented_graphql_mutation():
    result = SimpleNamespace(data={"DeviceClockingPreferredLeaderSet": {"ok": True}}, errors=())
    client = FakeClient(result)
    transport = device_transport.ManagedDeviceTransport(_configuration(), client=client)

    accepted = await transport.set_preferred_leader(_device(), True)

    assert accepted.successful is True
    assert client.calls[0][1] == {"input": {"deviceId": "001dc1fffe50692e:0", "enabled": True}}


@pytest.mark.asyncio
async def test_application_device_name_change_uses_managed_transport_for_enrolled_device():
    managed = SimpleNamespace(set_device_name=AsyncMock(return_value=SimpleNamespace(successful=True)))
    application = DanteApplication(managed_transport=managed)
    device = _device()
    application.attach_devices({device.server_name: device})

    result = await application.set_device_name(device, "Stage-IO")

    assert result.successful is True
    managed.set_device_name.assert_awaited_once_with(device, "Stage-IO")


@pytest.mark.asyncio
async def test_application_preferred_leader_change_uses_managed_mutation_and_inventory_readback():
    fresh = SimpleNamespace(clock_preferences=SimpleNamespace(leader=True))
    managed = SimpleNamespace(
        set_preferred_leader=AsyncMock(return_value=SimpleNamespace(successful=True)),
        fetch_device=AsyncMock(return_value=fresh),
    )
    application = DanteApplication(managed_transport=managed)
    device = _device()
    application.attach_devices({device.server_name: device})

    observed = await application.set_preferred_leader(device, True)

    assert observed is True
    assert device.preferred_leader is True
    managed.set_preferred_leader.assert_awaited_once_with(device, True)
    managed.fetch_device.assert_awaited_once_with(device)


@pytest.mark.asyncio
async def test_device_execute_automatically_prefers_ddm_for_an_enrolled_device():
    managed = SimpleNamespace(execute=AsyncMock(return_value=b"managed"))
    application = DanteApplication(managed_transport=managed)
    application.transport.execute = AsyncMock(return_value=b"direct")
    device = _device(enrolled=True, direct=True)
    application.attach_devices({device.server_name: device})

    result = await device.execute({"command": "query_latency_config"})

    assert result == b"managed"
    managed.execute.assert_awaited_once_with(device, {"command": "query_latency_config"})
    application.transport.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_ordinary_device_identify_automatically_uses_ddm_for_an_enrolled_device():
    managed = SimpleNamespace(execute=AsyncMock(return_value=None))
    application = DanteApplication(managed_transport=managed)
    application.transport.execute = AsyncMock()
    device = _device(enrolled=True, direct=True)
    application.attach_devices({device.server_name: device})

    await application.identify(device)

    specification = managed.execute.await_args.args[1]
    assert specification["command"] == "identify"
    application.transport.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_managed_control_fails_closed_when_the_originating_context_is_unknown():
    managed = SimpleNamespace(execute=AsyncMock(return_value=None))
    application = DanteApplication(managed_transport=managed)
    device = _device()
    device.ddm_context = None
    application.attach_devices({device.server_name: device})

    with pytest.raises(RuntimeError, match="managed but has no context"):
        await application.identify(device)

    managed.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_managed_identify_routes_by_device_identity_when_contexts_reuse_an_ip():
    managed = SimpleNamespace(execute=AsyncMock(return_value=None))
    application = DanteApplication(managed_transport=managed)
    east = _device()
    east.server_name = "east.local."
    east.ddm_device_id = "001dc1fffe50692e:0"
    east.ddm_domain_id = "11" * 16
    west = _device()
    west.server_name = "west.local."
    west.ddm_device_id = "001dc1fffe50692f:0"
    west.ddm_domain_id = "22" * 16
    application.attach_devices({east.server_name: east, west.server_name: west})
    application.transport.execute = AsyncMock()

    await application.identify(west)

    managed.execute.assert_awaited_once()
    assert managed.execute.await_args.args[0] is west
    application.transport.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_ambiguous_raw_ip_control_is_rejected_when_contexts_reuse_an_ip():
    application = DanteApplication(managed_transport=SimpleNamespace(execute=AsyncMock()))
    east = _device()
    east.server_name = "east.local."
    west = _device()
    west.server_name = "west.local."
    west.ddm_device_id = "001dc1fffe50692f:0"
    application.attach_devices({east.server_name: east, west.server_name: west})
    application.transport.execute = AsyncMock()

    with pytest.raises(RuntimeError, match="ambiguous across devices"):
        await application.send_probe_sample_rate("192.0.2.10")

    application.transport.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_managed_device_without_an_ip_uses_ddm_for_identify_channels_and_name():
    fresh = SimpleNamespace(
        name="managed-device",
        identity=SimpleNamespace(actual_name="Managed Device", default_name="factory-name"),
    )
    managed = SimpleNamespace(execute=AsyncMock(return_value=None), fetch_device=AsyncMock(return_value=fresh))
    application = DanteApplication(managed_transport=managed)
    application.query_receiver_channel_status_2809 = AsyncMock(
        return_value={"reported_channel_count": 0, "records": []}
    )
    application.query_transmitter_channel_status_2809 = AsyncMock(
        return_value={"reported_channel_count": 0, "records": []}
    )
    application.transport.execute = AsyncMock()
    device = _device()
    device.ipv4 = None
    application.attach_devices({device.server_name: device})

    await application.identify(device)
    await device.get_rx_channels()
    await device.get_tx_channels()
    name = await device.fetch_device_name()

    assert name == "Managed Device"
    assert managed.execute.await_args.args[0] is device
    application.query_receiver_channel_status_2809.assert_awaited_once_with(device)
    application.query_transmitter_channel_status_2809.assert_awaited_once_with(device)
    managed.fetch_device.assert_awaited_once_with(device)
    application.transport.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_managed_device_without_an_ip_uses_exact_device_for_clear_configuration():
    application = DanteApplication(managed_transport=SimpleNamespace(execute=AsyncMock()))
    device = _device()
    device.ipv4 = None
    application.attach_devices({device.server_name: device})
    key = application._control_key(device)

    async def clear(target):
        assert target is device
        application.notifications.notify_waiters(
            "clear_configuration_status",
            key,
            {"action_result_code": 1, "available_actions_mask": 3},
        )

    application.send_clear_all_configuration = clear

    status = await application.clear_configuration(device, preserve_internet_protocol_settings=False)

    assert status["action_result_code"] == 1


@pytest.mark.asyncio
async def test_managed_settings_publication_is_fed_into_the_normal_status_pipeline():
    publication = bytes.fromhex("ffff001c00010000001dc1fffe50692e417564696e61746507380080")
    managed = SimpleNamespace(execute=AsyncMock(return_value=publication))
    application = DanteApplication(managed_transport=managed)
    application.notifications.receive_settings_response = MagicMock()
    device = _device()
    application.attach_devices({device.server_name: device})

    await application.send_probe_sample_rate(device)

    application.notifications.receive_settings_response.assert_called_once_with(
        publication,
        "ddm:manager:manager-main:11111111111111111111111111111111:001dc1fffe50692e:0",
    )


@pytest.mark.asyncio
async def test_managed_control_population_uses_ddm_reads_without_a_local_arc_service():
    fresh = SimpleNamespace(
        name="managed-device",
        identity=SimpleNamespace(actual_name="Managed Device", default_name="factory-name"),
        clock_preferences=SimpleNamespace(leader=False),
    )
    managed = SimpleNamespace(fetch_device=AsyncMock(return_value=fresh))
    application = DanteApplication(managed_transport=managed)
    application.get_latency_settings = AsyncMock(return_value={"configured_latency": 1.0})
    application.apply_avio_status_pages = AsyncMock()
    application.probe_interface_status = AsyncMock(return_value=[])
    device = _device()
    device.services = {}
    application.attach_devices({device.server_name: device})

    await application._populate_device_controls(device)

    assert device.name == "Managed Device"
    assert device.preferred_leader is False
    managed.fetch_device.assert_awaited_once_with(device)
    application.get_latency_settings.assert_awaited_once_with(device)
    application.apply_avio_status_pages.assert_awaited_once_with(device)
    application.probe_interface_status.assert_awaited_once_with(device)


@pytest.mark.asyncio
async def test_managed_subscription_application_path_skips_direct_arc_and_multicast_wait():
    accepted = device_transport.ManagedOperationResult("DeviceRxChannelsSubscriptionSet")
    managed = SimpleNamespace(set_subscriptions=AsyncMock(return_value=accepted))
    application = DanteApplication(managed_transport=managed)
    device = _device()
    application.attach_devices({device.server_name: device})

    result = await application.add_subscriptions(device, [(1, "Left", "stagebox")])

    assert result == accepted
    managed.set_subscriptions.assert_awaited_once_with(device, [(1, "Left", "stagebox")])
