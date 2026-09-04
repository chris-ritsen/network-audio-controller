import hashlib
import json
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from netaudio import core
from netaudio.dante.application import DanteApplication
from netaudio.dante.browser import DanteBrowser
from netaudio.dante.channel import DanteChannel
from netaudio.dante.const import SERVICE_ARC, SERVICE_CMC, SERVICE_DBC, SERVICE_VIDEO, SERVICES
from netaudio.dante.device import DanteDevice


FIXTURE_PATH = Path(__file__).parent / "fixtures" / "studio_video_modern_arc.json"


def _fixture():
    return json.loads(FIXTURE_PATH.read_text())


def _packet(name):
    return bytes.fromhex(_fixture()["packets"][name]["payload"])


def _studio_service(service_type, server_name, ipv4, port):
    return {
        "ipv4": ipv4,
        "name": f"studio-media-b.{service_type}",
        "port": port,
        "properties": {"arcp_vers": "2.8.15"} if service_type == SERVICE_ARC else {},
        "server_name": server_name,
        "type": service_type,
    }


def test_promoted_packets_are_bound_to_the_recorded_payload_hashes():
    fixture = _fixture()
    assert fixture["_provenance"]["source_pcap"] == "controller-studio-bound.pcapng"
    assert fixture["_provenance"]["source_pcap_sha256"] == (
        "c91f705a4c46b0a30da58d116853e11b9fdd56a1928e0a493b675a2fd018dddd"
    )
    for packet in fixture["packets"].values():
        assert hashlib.sha256(bytes.fromhex(packet["payload"])).hexdigest() == packet["sha256"]


def test_video_channels_and_flows_parse_without_audio_labels():
    transmitter_channel = core.parse_response(
        "modern_arc_transmitter_channel_status_page",
        _packet("transmitter_channel_response"),
    )["records"][0]
    receiver_channel = core.parse_response(
        "modern_arc_receiver_channel_status_page",
        _packet("receiver_channel_response"),
    )["records"][0]
    transmitter_flow = core.parse_response(
        "transmitter_flow_status_page",
        _packet("transmitter_flow_response"),
    )["flows"][0]
    receiver_flow = core.parse_response(
        "modern_arc_receiver_flow_status_page",
        _packet("receiver_flow_response"),
    )["flows"][0]

    for record in (transmitter_channel, receiver_channel, transmitter_flow, receiver_flow):
        assert record["media_type_code"] == 4
        assert record["format_descriptor_hexadecimal"] == "02080000060000000000008200000000"
        assert record["sample_rate"] is None
        assert record["encoding"] is None
    assert receiver_channel["source_device_name"] == "studio-media-b"
    assert receiver_channel["subscription_status_code"] == 9
    assert receiver_flow["global_flow_id"] == 1
    assert receiver_flow["latency_nanoseconds"] is None


def test_status_pages_create_video_inventory_even_when_scalar_counts_are_zero():
    device = DanteDevice("studio-media-b.local.")
    device.name = "studio-media-b"
    device.rx_count = 0
    device.tx_count = 0
    device.apply_transmitter_channel_status_page(
        core.parse_response("modern_arc_transmitter_channel_status_page", _packet("transmitter_channel_response"))
    )
    device.apply_receiver_channel_status_page(
        core.parse_response("modern_arc_receiver_channel_status_page", _packet("receiver_channel_response"))
    )

    assert device.tx_count == device.rx_count == 1
    assert device.media_types == ["video"]
    assert device.tx_channels[1].media_type == "video"
    assert device.rx_channels[1].format_descriptor_hexadecimal == "02080000060000000000008200000000"
    assert len(device.subscriptions) == 1
    assert device.subscriptions[0].tx_device_name == "studio-media-b"


@pytest.mark.asyncio
async def test_direct_channel_refresh_uses_the_explicitly_advertised_modern_protocol():
    application = DanteApplication()
    application.query_modern_arc_transmitter_channel_status = AsyncMock(
        return_value=core.parse_response(
            "modern_arc_transmitter_channel_status_page",
            _packet("transmitter_channel_response"),
        )
    )
    application.query_modern_arc_receiver_channel_status = AsyncMock(
        return_value=core.parse_response(
            "modern_arc_receiver_channel_status_page",
            _packet("receiver_channel_response"),
        )
    )
    device = DanteDevice("studio-media-b.local.", app=application)
    device.services = {
        "arc": _studio_service(SERVICE_ARC, "W.local.", "192.168.1.38", 4540),
    }

    await device.get_tx_channels()
    await device.get_rx_channels()

    application.query_modern_arc_transmitter_channel_status.assert_awaited_once_with(device)
    application.query_modern_arc_receiver_channel_status.assert_awaited_once_with(device)
    assert device.tx_channels[1].media_type == "video"
    assert device.rx_channels[1].media_type == "video"


@pytest.mark.asyncio
async def test_direct_channel_refresh_fails_closed_on_an_unknown_advertised_protocol():
    application = DanteApplication()
    device = DanteDevice("future-device.local.", app=application)
    device.services = {
        "arc": {
            **_studio_service(SERVICE_ARC, "future.local.", "192.168.1.39", 4540),
            "properties": {"arcp_vers": "2.8.14"},
        }
    }

    with pytest.raises(RuntimeError, match="unsupported ARC protocol version"):
        await device.get_rx_channels()


@pytest.mark.asyncio
async def test_normal_subscription_path_selects_the_captured_280f_video_shape():
    application = DanteApplication()
    device = DanteDevice("studio-media-b.local.", app=application)
    device.name = "studio-media-b"
    device.services = {
        "arc": _studio_service(SERVICE_ARC, "W.local.", "192.168.1.38", 4540),
    }
    channel = DanteChannel()
    channel.number = 1
    channel.media_type_code = 4
    device.rx_channels = {1: channel}
    device.execute = AsyncMock(return_value=b"ack")

    assert await application.send_add_subscriptions(device, [(1, "01", "studio-media-b")]) == b"ack"
    specification = device.execute.await_args.args[0]
    assert specification == {
        "command": "modern_arc_subscription_page",
        "protocol_id": 0x280F,
        "page_capacity": 1,
        "media_type_code": 4,
        "records": [
            {
                "action": "set",
                "rx_channel": 1,
                "tx_channel": "01",
                "tx_device": "studio-media-b",
            }
        ],
    }
    assert core.build_command({**specification, "transaction_id": 0x05D9}) == _packet("subscription_set_request")

    device.execute.reset_mock()
    assert await application.send_remove_subscriptions(device, [1]) == b"ack"
    clear_specification = device.execute.await_args.args[0]
    assert clear_specification["records"] == [{"action": "clear", "rx_channel": 1}]
    assert core.build_command({**clear_specification, "transaction_id": 0x05DC}) == _packet(
        "subscription_clear_request"
    )


def test_shared_host_services_are_split_into_logical_devices_and_arc_owns_the_control_address():
    services = [
        {
            "ipv4": "192.168.1.38",
            "name": f"Windows-PC.{SERVICE_ARC}",
            "port": 4440,
            "properties": {"arcp_vers": "2.8.15"},
            "server_name": "W.local.",
            "type": SERVICE_ARC,
        },
        _studio_service(SERVICE_ARC, "W.local.", "192.168.1.38", 4540),
        _studio_service(SERVICE_DBC, "W.local.", "192.168.1.38", 4555),
        _studio_service(SERVICE_CMC, "www.local.", "192.168.1.107", 8802),
    ]
    grouped = DanteBrowser._group_device_services(services)

    assert set(grouped) == {"Windows-PC.local.", "studio-media-b.local."}
    application = DanteApplication()
    studio = application._apply_discovered_services("studio-media-b.local.", grouped["studio-media-b.local."])
    assert str(studio.ipv4) == "192.168.1.38"
    assert studio.get_service(SERVICE_CMC)["ipv4"] == "192.168.1.107"


def test_video_source_discovery_attaches_the_dbc_endpoint_to_the_transmitter_channel():
    assert SERVICE_VIDEO in SERVICES
    application = DanteApplication()
    device = DanteDevice("studio-media-b.local.")
    device.name = "studio-media-b"
    channel = DanteChannel()
    channel.name = "01"
    device.tx_channels = {1: channel}
    application.media_services = {
        "video": {
            "ipv4": "192.168.1.38",
            "name": f"01@studio-media-b.{SERVICE_VIDEO}",
            "port": 4555,
            "properties": {},
            "server_name": "W.local.",
            "type": SERVICE_VIDEO,
        }
    }

    application._attach_media_services(device)
    assert channel.media_service["type"] == SERVICE_VIDEO
    assert channel.media_service["port"] == 4555
