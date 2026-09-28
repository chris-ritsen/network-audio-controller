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
from netaudio.dante.subscription_operations import reconcile_receiver_subscriptions, read_subscription_readback
from netaudio.dante.subscription import DanteSubscription


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
    assert receiver_channel["receiver_capability_flags"] == 6
    assert receiver_channel["can_subscribe_self"] is False
    assert receiver_flow["flow_number"] == 1
    assert receiver_flow["latency_nanoseconds"] is None


@pytest.mark.parametrize("direction,kind", [("tx", "transmitter"), ("rx", "receiver")])
@pytest.mark.parametrize("duplicate", [False, True])
def test_native_channel_inventory_preserves_surviving_identity_and_rejects_ambiguity(direction, kind, duplicate):
    device = DanteDevice("studio-media-b.local.")
    target = DanteChannel()
    target.number = 1
    other = DanteChannel()
    other.number = 1 if duplicate else 2
    other.name = "Untouched"
    previous = {"cached-label": target, 1: other}
    setattr(device, f"{direction}_channels", previous)
    page = core.parse_response(f"modern_arc_{kind}_channel_status_page", _packet(f"{kind}_channel_response"))
    apply = getattr(device, f"apply_{kind}_channel_inventory")

    if duplicate:
        with pytest.raises(RuntimeError, match="conflicting identities"):
            apply(page)

        assert getattr(device, f"{direction}_channels") is previous
    else:
        apply(page)
        assert getattr(device, f"{direction}_channels") == {1: target}

    assert other.name == "Untouched"


@pytest.mark.asyncio
@pytest.mark.parametrize("direction,kind", [("rx", "receiver"), ("tx", "transmitter")])
async def test_complete_native_inventory_retires_disappeared_channels_and_routes(direction, kind):
    application = DanteApplication()
    device = DanteDevice("receiver.local.", app=application)
    device.services = {"arc": _studio_service(SERVICE_ARC, device.server_name, "192.0.2.10", 4440)}
    survivor = DanteChannel()
    survivor.number = 1
    survivor.managed_can_subscribe_self = False
    survivor.managed_can_subscribe_self_fresh = True
    retired = DanteChannel()
    retired.number = 2
    setattr(device, f"{direction}_channels", {1: survivor, 2: retired})
    subscription = DanteSubscription()
    subscription.rx_channel = retired
    subscription.tx_channel_name = "Out"
    subscription.tx_device_name = "Source"
    subscription.status_code = 9
    device.subscriptions = [subscription] if direction == "rx" else []
    device.execute = AsyncMock(return_value=_packet(f"{kind}_channel_response"))

    if direction == "rx":
        with pytest.raises(core.NetaudioCoreError, match="unavailable"):
            await read_subscription_readback(device, {2: ("Out", "Source")})

        assert all(entry.rx_channel.number != 2 for entry in device.subscriptions)
    else:
        await device.get_tx_channels()

    assert getattr(device, f"{direction}_channels") == {1: survivor}
    assert getattr(device, f"{direction}_count") == 1
    assert survivor.managed_can_subscribe_self is False


def test_status_pages_create_video_inventory_even_when_scalar_counts_are_zero():
    device = DanteDevice("studio-media-b.local.")
    device.name = "studio-media-b"
    device.rx_count = 0
    device.tx_count = 0
    device.apply_transmitter_channel_inventory(
        core.parse_response("modern_arc_transmitter_channel_status_page", _packet("transmitter_channel_response"))
    )
    device.apply_receiver_channel_inventory(
        core.parse_response("modern_arc_receiver_channel_status_page", _packet("receiver_channel_response"))
    )

    assert device.tx_count == device.rx_count == 1
    assert device.media_types == ["video"]
    assert device.tx_channels[1].media_type == "video"
    assert device.rx_channels[1].format_descriptor_hexadecimal == "02080000060000000000008200000000"
    assert device.rx_channels[1].receiver_capability_flags == 6
    assert device.rx_channels[1].receiver_status_flags is None
    assert device.rx_channels[1].can_subscribe_self is False
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
    channel.can_subscribe_self = True
    device.rx_channels = {1: channel}
    device.get_rx_channels = AsyncMock()
    device.execute = AsyncMock(return_value=b"ack")

    assert await application.send_add_subscriptions(device, [(1, "01", "studio-media-b")]) == b"ack"
    specification = device.execute.await_args.args[0]
    assert core.build_command({**specification, "message_id": 0x05D9}) == _packet("subscription_set_request")
    device.get_rx_channels.assert_awaited_once_with()

    device.execute.reset_mock()
    assert await application.send_remove_subscriptions(device, [1]) == b"ack"
    clear_specification = device.execute.await_args.args[0]
    assert core.build_command({**clear_specification, "message_id": 0x05DC}) == _packet("subscription_clear_request")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "responses,expected_calls,expected",
    [
        (
            [bytes.fromhex("280f000a000134100030"), bytes.fromhex("280f000a000234100001")],
            1,
            bytes.fromhex("280f000a000134100030"),
        ),
        ([None, bytes.fromhex("280f000a000234100001")], 1, None),
        ([b"invalid", bytes.fromhex("280f000a000234100001")], 1, b"invalid"),
        (
            [bytes.fromhex("280f000a000134100001"), bytes.fromhex("280f000a000234100030")],
            2,
            bytes.fromhex("280f000a000234100030"),
        ),
        (
            [bytes.fromhex("280f000a000134100001"), bytes.fromhex("280f000a000234100001")],
            2,
            bytes.fromhex("280f000a000234100001"),
        ),
    ],
)
async def test_subscription_pages_stop_at_the_first_unconfirmed_reply(responses, expected_calls, expected):
    application = DanteApplication()
    device = DanteDevice("receiver.local.", app=application)
    device.name = "Receiver"
    device.services = {"arc": _studio_service(SERVICE_ARC, "receiver.local.", "192.0.2.10", 4440)}
    device.rx_channels = {}

    for number in range(1, 34):
        channel = DanteChannel()
        channel.number = number
        channel.media_type_code = 3
        device.rx_channels[number] = channel

    device.execute = AsyncMock(side_effect=responses)
    result = await application.send_add_subscriptions(device, [(number, "Mic", "Source") for number in range(1, 34)])

    assert result == expected
    assert device.execute.await_count == expected_calls
    assert len(device.execute.await_args_list[0].args[0]["records"]) == 32


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["late_invalid_name", "duplicate_receiver", "missing_receiver", "unknown_media"])
@pytest.mark.parametrize("version", ["2.7.255", "2.8.9", "2.8.15"])
async def test_subscription_plan_validates_every_page_before_any_send(damage, version):
    application = DanteApplication()
    device = DanteDevice("receiver.local.", app=application)
    device.name = "Receiver"
    device.services = {"arc": _studio_service(SERVICE_ARC, "receiver.local.", "192.0.2.10", 4440)}
    device.services["arc"]["properties"]["arcp_vers"] = version
    device.rx_channels = {}

    for number in range(1, 34):
        channel = DanteChannel()
        channel.number = number
        channel.media_type_code = 3
        device.rx_channels[number] = channel

    records = [(number, "Mic", "Source") for number in range(1, 34)]

    if damage == "late_invalid_name":
        records[-1] = (33, "Mic", "x" * 100)
    elif damage == "duplicate_receiver":
        records[-1] = records[0]
    elif damage == "missing_receiver":
        records[-1] = (34, "Mic", "Source")
    elif version == "2.8.15":
        device.rx_channels[33].media_type_code = None
    else:
        device.rx_channels[33].number = 1

    async def execute(spec):
        core.build_command(spec)
        return bytes.fromhex("280f000a000134100001")

    device.execute = AsyncMock(side_effect=execute)

    with pytest.raises(RuntimeError):
        await application.send_add_subscriptions(device, records)

    device.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("version", ["2.7.255", "2.8.9", "2.8.15"])
@pytest.mark.parametrize("clear_first", [False, True])
async def test_reconciliation_validates_last_batch_before_writing_first(version, clear_first):
    application = DanteApplication()
    device = DanteDevice("receiver.local.", app=application)
    device.name = "Receiver"
    device.services = {"arc": {"type": SERVICE_ARC, "properties": {"arcp_vers": version}}}
    device.get_rx_channels = AsyncMock()

    for number in range(1, 34):
        channel = DanteChannel()
        channel.number = number
        channel.media_type_code = 3
        device.rx_channels[number] = channel

    async def send_without_notification(_device, send, *_args):
        return await send()

    application.mutate_and_wait_for_notification = send_without_notification
    device.execute = AsyncMock(return_value=bytes.fromhex("280f000a000134100001"))
    desired = {number: ("Mic", "Source") for number in range(1, 34)}
    desired[33] = ("Mic", "x" * 100)

    if clear_first:
        device.apply_receiver_channel_inventory(
            {
                "records": [
                    {
                        "channel_number": 1,
                        "media_type_code": 3,
                        "source_device_name": "Source",
                        "source_channel_name": "Mic",
                    }
                ]
            }
        )
        desired[1] = None

    with pytest.raises(RuntimeError):
        await reconcile_receiver_subscriptions(application, device, desired)

    device.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("version", [None, "2.8.16", "bad"])
@pytest.mark.parametrize("action", ["set", "clear"])
async def test_subscription_writes_never_guess_an_unknown_revision(version, action):
    application = DanteApplication()
    device = DanteDevice("receiver.local.", app=application)
    device.name = "Receiver"
    device.services = {"arc": _studio_service(SERVICE_ARC, "receiver.local.", "192.0.2.10", 4440)}
    device.services["arc"]["properties"]["arcp_vers"] = version
    device.execute = AsyncMock(return_value=b"ack")

    with pytest.raises(RuntimeError):
        if action == "set":
            await application.send_add_subscriptions(device, [(1, "Mic", "Source")])
        else:
            await application.send_remove_subscriptions(device, [1])

    device.execute.assert_not_awaited()


@pytest.mark.parametrize("protocol_id,capacity", [(0x27FF, 16), (0x2809, 32), (0x280C, 32), (0x280F, 32)])
@pytest.mark.parametrize("action", ["set", "clear"])
def test_native_subscription_plan_selects_revision_and_bounds_every_command(protocol_id, capacity, action):
    records = [
        {
            "action": action,
            "rx_channel": number,
            **({"tx_channel": "Mic", "tx_device": "Source"} if action == "set" else {}),
        }
        for number in range(1, 34)
    ]
    commands = core.plan_subscription_commands(
        {
            "protocol_id": protocol_id,
            "channels": [{"number": number, "media_type_code": 3} for number in range(1, 34)],
            "records": records,
        }
    )
    field = "records" if capacity == 32 else "subscriptions" if action == "set" else "rx_channels"

    assert [len(command[field]) for command in commands] == ([32, 1] if capacity == 32 else [16, 16, 1])
    assert all(core.build_command(command) for command in commands)


def test_native_subscription_plan_rejects_unknown_revision():
    with pytest.raises(RuntimeError):
        core.plan_subscription_commands(
            {"protocol_id": 0x2900, "channels": [{"number": 1}], "records": [{"action": "clear", "rx_channel": 1}]}
        )


def test_native_subscription_plan_groups_media_without_mixing_records():
    records = [
        {"action": "clear", "rx_channel": 3},
        {"action": "set", "rx_channel": 1, "tx_channel": "Mic", "tx_device": "Source"},
        {"action": "clear", "rx_channel": 2},
    ]
    pages = core.plan_subscription_commands(
        {
            "protocol_id": 0x280F,
            "channels": [
                {"number": 1, "media_type_code": 3},
                {"number": 2, "media_type_code": 3},
                {"number": 3, "media_type_code": 4},
            ],
            "records": records,
        }
    )

    assert [(page["media_type_code"], page["records"]) for page in pages] == [(4, records[:1]), (3, records[1:])]
    assert all(page["page_capacity"] == 3 for page in pages)
    assert all(core.build_command(page) for page in pages)


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
