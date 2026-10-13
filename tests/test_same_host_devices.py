import asyncio
import logging
import socket
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio

from netaudio import core
from netaudio.dante.application import DanteApplication
from netaudio.dante.const import SERVICE_ARC
from netaudio.dante.virtual_device import VirtualDevice, VirtualDeviceConfig, _ARCProtocol

HOST = "127.0.0.1"


async def _start_device(name: str, tx_channels: list[str], rx_channels: list[str]):
    device = VirtualDevice(VirtualDeviceConfig(name=name, tx_channels=tx_channels, rx_channels=rx_channels))
    device._local_ip = HOST
    transport, _ = await asyncio.get_running_loop().create_datagram_endpoint(
        lambda: _ARCProtocol(device),
        local_addr=(HOST, 0),
        family=socket.AF_INET,
    )
    device._transports.append(transport)
    return device, transport.get_extra_info("sockname")[1]


def _discover(application: DanteApplication, name: str, arc_port: int):
    service_name = f"{name}._netaudio-arc._udp.local."
    return application._apply_discovered_services(
        f"{name}.local.",
        {
            service_name: {
                "ipv4": HOST,
                "name": service_name,
                "port": arc_port,
                "properties": {},
                "server_name": f"{name}.local.",
                "type": SERVICE_ARC,
            }
        },
    )


@pytest_asyncio.fixture
async def same_host_devices():
    # Two processes on one host, each advertising its own ARC port (as Inferno does with ALT_PORT).
    player, player_port = await _start_device("player", ["TX 1", "TX 2"], [])
    recorder, recorder_port = await _start_device("recorder", [], ["RX 1", "RX 2", "RX 3"])
    application = DanteApplication(verify_mutations=False)
    devices = {
        "player": _discover(application, "player", player_port),
        "recorder": _discover(application, "recorder", recorder_port),
    }
    try:
        yield application, devices
    finally:
        for virtual_device in (player, recorder):
            for transport in virtual_device._transports:
                transport.close()
        application.transport.close()


@pytest.mark.asyncio
async def test_application_execute_reaches_each_device_on_a_shared_ip_at_its_advertised_arc_port(same_host_devices):
    application, devices = same_host_devices

    counts = {}
    for name, device in devices.items():
        response = await application.execute(device, {"command": "channel_count"})
        parsed = core.parse_response("channel_count", response)
        counts[name] = (parsed["tx_count"], parsed["rx_count"])

    assert counts == {"player": (2, 0), "recorder": (0, 3)}


def _unanswered_host_ports(application: DanteApplication) -> list[str]:
    arc_execute = application.transport.execute
    host_messages = []

    async def execute(address, specification, **options):
        if "arc_port" in options:
            return await arc_execute(address, specification, **options)
        # Settings and CMC messages go to the host's fixed ports; nothing answers them on loopback.
        host_messages.append(address)
        return None

    application.transport.execute = execute
    return host_messages


@pytest.mark.asyncio
async def test_populating_devices_that_share_an_ip_reads_each_device_without_ambiguity(same_host_devices, caplog):
    application, devices = same_host_devices
    host_messages = _unanswered_host_ports(application)

    with caplog.at_level(logging.WARNING, logger="netaudio"):
        await application.populate_devices(dict(application.devices), timeout=0.5)

    assert caplog.messages == []
    assert host_messages and set(host_messages) == {HOST}
    assert (devices["player"].name, devices["player"].tx_count, devices["player"].rx_count) == ("player", 2, 0)
    assert (devices["recorder"].name, devices["recorder"].tx_count, devices["recorder"].rx_count) == (
        "recorder",
        0,
        3,
    )


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
async def test_status_probes_reach_a_host_shared_by_devices_without_failing(same_host_devices, caplog, probe_phase):
    application, _ = same_host_devices
    host_messages = _unanswered_host_ports(application)

    with caplog.at_level(logging.WARNING, logger="netaudio"):
        await getattr(application, probe_phase)(timeout=0.05)

    assert caplog.messages == []
    assert host_messages and set(host_messages) == {HOST}


@pytest.mark.asyncio
async def test_settings_message_to_a_host_shared_by_direct_devices_goes_to_that_host(same_host_devices):
    application, _ = same_host_devices
    application.transport.execute = AsyncMock(return_value=None)

    await application.send_probe_sample_rate(HOST)

    application.transport.execute.assert_awaited_once()
    assert application.transport.execute.await_args.args[0] == HOST


@pytest.mark.asyncio
async def test_raw_arc_request_to_a_shared_ip_is_rejected_because_the_device_is_ambiguous(same_host_devices):
    application, _ = same_host_devices
    application.transport.execute = AsyncMock()

    with pytest.raises(RuntimeError, match="ambiguous across devices: player.local., recorder.local."):
        await application.execute(HOST, {"command": "channel_count"})

    application.transport.execute.assert_not_awaited()
