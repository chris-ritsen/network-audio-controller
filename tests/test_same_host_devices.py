import asyncio
import logging
import socket
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio

from netaudio import core
from netaudio.dante.application import DanteApplication
from netaudio.dante.const import PROTOCOL_CMC, SERVICE_ARC, SERVICE_CMC
from netaudio.dante.device import DanteDevice
from netaudio.dante.events import EventType
from netaudio.dante.services.heartbeat import DanteHeartbeatService
from netaudio.dante.virtual_device import VirtualDevice, VirtualDeviceConfig, _ARCProtocol
from tests.status_test_support import receive_packets, status_events

HOST = "127.0.0.1"
TEST_HOST_MAC = bytes.fromhex("020000000001")


class _HostedProcess(VirtualDevice):
    """One device process on HOST answering ARC, CMC and settings on its own port, like Inferno with ALT_PORT."""

    def __init__(self, name: str, process_id: int, tx_channels: list[str], rx_channels: list[str]):
        super().__init__(VirtualDeviceConfig(name=name, tx_channels=tx_channels, rx_channels=rx_channels))
        self._local_ip = HOST
        self.device_id = f"00007f000001{process_id:04x}"
        self.port = 0
        self.received: list[str] = []

    def _handle_request(self, data: bytes, addr, *, settings_only: bool = False):
        if data[:2] == b"\xff\xff":
            self.received.append("settings")
            return None
        self.received.append(core.parse_response("device_request", data)["operation"])
        return super()._handle_request(data, addr, settings_only=settings_only)

    def _handle_cmc_registration(self, transaction_id: int) -> bytes:
        response = {"kind": "cmc_registration", "device_ip": HOST, "settings_port": self.port}
        return self._build_response(transaction_id, response, PROTOCOL_CMC)


async def _start_device(name: str, process_id: int, tx_channels: list[str], rx_channels: list[str]):
    device = _HostedProcess(name, process_id, tx_channels, rx_channels)
    transport, _ = await asyncio.get_running_loop().create_datagram_endpoint(
        lambda: _ARCProtocol(device),
        local_addr=(HOST, 0),
        family=socket.AF_INET,
    )
    device._transports.append(transport)
    device.port = transport.get_extra_info("sockname")[1]
    return device


def _discover(application: DanteApplication, process: _HostedProcess):
    name = process.config.name

    def service(service_type: str, properties: dict) -> tuple[str, dict]:
        service_name = f"{name}.{service_type}"
        return service_name, {
            "ipv4": HOST,
            "name": service_name,
            "port": process.port,
            "properties": properties,
            "server_name": f"{name}.local.",
            "type": service_type,
        }

    return application._apply_discovered_services(
        f"{name}.local.",
        dict([service(SERVICE_ARC, {}), service(SERVICE_CMC, {"id": process.device_id, "process": "1"})]),
    )


@pytest_asyncio.fixture
async def same_host_devices():
    # Two processes on one host, each advertising its own ARC and CMC ports (as Inferno does with ALT_PORT).
    player = await _start_device("player", 1, ["TX 1", "TX 2"], [])
    recorder = await _start_device("recorder", 2, [], ["RX 1", "RX 2", "RX 3"])
    application = DanteApplication(verify_mutations=False)
    client_for_key = application.transport._client_for_key

    prepared_clients = set()

    def client_with_host_mac(key):
        # Loopback has no MAC address to put in settings and CMC requests.
        client = client_for_key(key)
        if id(client) not in prepared_clients:
            prepared_clients.add(id(client))
            client.set_host_mac(TEST_HOST_MAC)
        return client

    application.transport._client_for_key = client_with_host_mac
    devices = {
        "player": _discover(application, player),
        "recorder": _discover(application, recorder),
    }
    try:
        yield application, devices, {"player": player, "recorder": recorder}
    finally:
        for virtual_device in (player, recorder):
            for transport in virtual_device._transports:
                transport.close()
        application.transport.close()


@pytest.mark.asyncio
async def test_application_execute_reaches_each_device_on_a_shared_ip_at_its_advertised_arc_port(same_host_devices):
    application, devices, processes = same_host_devices

    counts = {}
    for name, device in devices.items():
        response = await application.execute(device, {"command": "channel_count"})
        parsed = core.parse_response("channel_count", response)
        counts[name] = (parsed["tx_count"], parsed["rx_count"])

    assert counts == {"player": (2, 0), "recorder": (0, 3)}


@pytest.mark.asyncio
async def test_populating_devices_that_share_an_ip_reads_each_device_without_ambiguity(same_host_devices, caplog):
    application, devices, processes = same_host_devices
    # Make and model queries need a MAC-shaped device ID; Inferno IDs with a process ID are not (unrelated here).
    application._query_conmon_all = AsyncMock()

    with caplog.at_level(logging.WARNING, logger="netaudio"):
        await application.populate_devices(dict(application.devices), timeout=0.5)

    assert caplog.messages == []
    assert (devices["player"].name, devices["player"].tx_count, devices["player"].rx_count) == ("player", 2, 0)
    assert (devices["recorder"].name, devices["recorder"].tx_count, devices["recorder"].rx_count) == (
        "recorder",
        0,
        3,
    )


@pytest.mark.asyncio
async def test_cmc_registration_reaches_each_device_at_its_advertised_cmc_port(same_host_devices):
    application, devices, processes = same_host_devices

    await application.cmc.register_all(list(devices.values()))

    for name, process in processes.items():
        assert "cmc_registration" in process.received, name
        assert devices[name].settings_port == process.port


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "probe_phase",
    [
        "_probe_interface_status",
        "_probe_preferred_leader_all",
        "_probe_aes67_all",
        "_probe_sample_rates_all",
        "_probe_encodings_all",
        "_probe_codec_status_all",
        "_probe_sample_rate_pullups_all",
    ],
)
async def test_status_probes_reach_each_device_at_its_reported_settings_port(same_host_devices, caplog, probe_phase):
    application, devices, processes = same_host_devices
    await application.cmc.register_all(list(devices.values()))

    with caplog.at_level(logging.WARNING, logger="netaudio"):
        await getattr(application, probe_phase)(timeout=0.05)

    assert caplog.messages == []
    for name, process in processes.items():
        assert "settings" in process.received, name


@pytest.mark.asyncio
async def test_settings_message_to_a_host_shared_by_direct_devices_goes_to_that_host(same_host_devices):
    application, _, _ = same_host_devices
    application.transport.execute = AsyncMock(return_value=None)

    await application.send_probe_sample_rate(HOST)

    application.transport.execute.assert_awaited_once()
    assert application.transport.execute.await_args.args[0] == HOST


@pytest.mark.asyncio
async def test_raw_arc_request_to_a_shared_ip_is_rejected_because_the_device_is_ambiguous(same_host_devices):
    application, _, _ = same_host_devices
    application.transport.execute = AsyncMock()

    with pytest.raises(RuntimeError, match="ambiguous across devices: player.local., recorder.local."):
        await application.execute(HOST, {"command": "channel_count"})

    application.transport.execute.assert_not_awaited()


SAMPLE_RATE_STATUS_PACKET = bytes.fromhex(
    "ffff004816310000001dc10812580000417564696e6174650724008000000000"
    "001800060000ac4400000000000200000000ac440000bb800001588800017700"
    "0002b1100002ee00"
)
RX_CHANNEL_CHANGE_PACKET = bytes.fromhex("ffff0023000100000000000000000000417564696e617465072a010200000000000100")
COHOSTED_IP = "172.19.0.5"


def _cohosted_application():
    application = DanteApplication()
    devices = {}
    for name, process_id in (("player", 1), ("recorder", 2)):
        device = DanteDevice(server_name=f"{name}.local.")
        device.name = name
        device.ipv4 = COHOSTED_IP
        device.mac_address = f"0000ac130005{process_id:04x}"
        devices[name] = device
    application.attach_devices({device.server_name: device for device in devices.values()})
    return application, devices


def _from_process(packet: bytes, process_id: int) -> bytes:
    # Inferno stamps its PROCESS_ID at offset 6 and its device ID (0000, IPv4, process) at offset 8.
    stamped = bytearray(packet)
    stamped[6:8] = process_id.to_bytes(2, "big")
    stamped[8:16] = bytes.fromhex(f"0000ac130005{process_id:04x}")
    return bytes(stamped)


def test_status_from_a_shared_ip_is_applied_to_the_device_named_in_the_packet():
    application, devices = _cohosted_application()

    events = receive_packets(application, [_from_process(SAMPLE_RATE_STATUS_PACKET, 2)], (COHOSTED_IP, 10203))

    [status_event] = status_events(events)
    assert status_event.server_name == "recorder.local."
    assert devices["recorder"].sample_rate == 44_100
    assert devices["player"].sample_rate is None


def test_notification_from_a_shared_ip_names_the_device_in_the_packet():
    application, _ = _cohosted_application()

    events = receive_packets(application, [_from_process(RX_CHANNEL_CHANGE_PACKET, 1)], (COHOSTED_IP, 10103))

    [event] = [event for event in events if event.type is EventType.NOTIFICATION_RECEIVED]
    assert event.server_name == "player.local."
    assert event.data["notification_id"] == 258


def test_heartbeat_from_a_shared_ip_marks_the_device_named_in_the_packet():
    application, devices = _cohosted_application()
    service = DanteHeartbeatService(device_by_ip=application._device_by_source)
    heartbeat = core.build_publication(
        {"source_ip": COHOSTED_IP, "message_id": 1, "publication": {"kind": "heartbeat", "tx_count": 0, "rx_count": 3}}
    )

    service._on_packet(_from_process(heartbeat, 2), (COHOSTED_IP, 10203))

    assert devices["recorder"].last_seen is not None
    assert devices["player"].last_seen is None
