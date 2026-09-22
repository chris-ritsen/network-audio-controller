import hashlib

import pytest

from netaudio import core
from netaudio.dante.services.heartbeat import parse_signal_presence_records
from netaudio.dante.virtual_device import _McastInfoProtocol, VirtualDevice, VirtualDeviceConfig


class RecordingTransport:
    def __init__(self):
        self.sent = []

    def sendto(self, packet, destination):
        self.sent.append((packet, destination))


@pytest.mark.asyncio
@pytest.mark.parametrize("encoding,supported,pcm", [(24, [16, 24, 32], b"3 0xe"), (20, [20, 24], None)])
async def test_discovery_registration_preserves_device_and_channel_capabilities(monkeypatch, encoding, supported, pcm):
    from netaudio.dante import virtual_device

    registered = []

    class Registrar:
        def __init__(self, **kwargs):
            assert kwargs["interfaces"] == ["192.0.2.10"]

        async def async_register_service(self, info):
            registered.append(info)

    monkeypatch.setattr(virtual_device, "AsyncZeroconf", Registrar)
    device = VirtualDevice(
        VirtualDeviceConfig(
            name="Desk",
            tx_channels=["Left", "Right"],
            encoding=encoding,
            supported_encodings=supported,
            sample_rate=96000,
            configured_latency_ns=500000,
        )
    )
    device._local_ip = "192.0.2.10"
    device._arc_port = 4442

    await device._register_mdns()

    assert [info.name for info in registered] == [
        "Desk._netaudio-arc._udp.local.",
        "Desk._netaudio-cmc._udp.local.",
        "Left@Desk._netaudio-chan._udp.local.",
        "Right@Desk._netaudio-chan._udp.local.",
    ]
    assert [info.port for info in registered] == [4442, 8800, 4455, 4455]
    assert all(info.addresses == [bytes([192, 0, 2, 10])] and info.server == "Desk.local." for info in registered)
    assert registered[0].properties[b"arcp_vers"] == b"2.7.41"
    assert registered[1].properties[b"id"] == b"0000c000020a0000"

    for channel_id, info in enumerate(registered[2:], 1):
        expected = {
            b"txtvers": b"2",
            b"dbcp1": b"0x1102",
            b"dbcp": b"0x1004",
            b"id": str(channel_id).encode(),
            b"rate": b"96000",
            b"enc": str(encoding).encode(),
            b"en": str(encoding).encode(),
            b"latency_ns": b"500000",
            b"fpp": b"32,2",
            b"nchan": b"8",
        }
        if pcm is not None:
            expected[b"pcm"] = pcm

        assert info.properties == expected


@pytest.mark.parametrize(
    "operation,publication,decoder,field,expected,digest",
    [
        (
            "board_info",
            {"kind": "board_info", "name": "Desk"},
            "dante_model",
            "platform_model_name",
            "Desk",
            "21e12239e7c8645d812e67df221f63c9e71afb405238dfb3bb6da5e046df0f0b",
        ),
        (
            "product_info",
            {"kind": "product_info", "name": "Desk", "manufacturer": "NetAudio", "model": "Virtual"},
            "make_model",
            "product_name",
            "Virtual",
            "f6f54e89697e08f0976fb51c7a502d4908b3b1dee46822a018f1b1758ba14589",
        ),
        (
            "clock_stats",
            {"kind": "clock_status", "mac_address": [2, 0, 0, 0, 0, 1]},
            "ptp_clock_status",
            "ptpv1_device_uuid",
            [2, 0, 0, 0, 0, 1],
            "3f52933e1f81e98e3e314fbabfede253d66734d9c8d6b4f4b0930eef77b294c0",
        ),
    ],
)
def test_virtual_status_publications_use_native_encoding(operation, publication, decoder, field, expected, digest):
    device = VirtualDevice(VirtualDeviceConfig(name="Desk", manufacturer="NetAudio", model="Virtual"))
    device._mac = "02:00:00:00:00:01"
    device._local_ip = "192.0.2.10"
    device._mcast_transport = RecordingTransport()

    getattr(device, f"_send_mcast_{operation}")()
    [(packet, destination)] = device._mcast_transport.sent
    native = core.build_publication({"source_ip": "192.0.2.10", "message_id": 1, "publication": publication})

    assert packet == native
    assert hashlib.sha256(packet).hexdigest() == digest
    assert core.parse_response(decoder, packet)[field] == expected
    assert destination == ("224.0.0.231", 8702)


def test_virtual_publications_wrap_per_sender_without_changing_packet_contents():
    device = VirtualDevice()
    device._local_ip = "192.0.2.10"
    device._mcast_seqnum = 65535
    publication = {"kind": "board_info", "name": "Desk"}

    for expected_id in (65535, 0, 1):
        assert device._build_publication(publication) == core.build_publication(
            {"source_ip": "192.0.2.10", "message_id": expected_id, "publication": publication}
        )

    independent = VirtualDevice()
    assert independent._mcast_seqnum == 1


def test_virtual_interface_publication_round_trips_the_running_interface():
    device = VirtualDevice()
    device._mac = "02:00:00:00:00:01"
    device._local_ip = "192.0.2.10"
    device._mcast_transport = RecordingTransport()

    device._send_mcast_network_info()
    [(packet, _)] = device._mcast_transport.sent
    status = core.parse_response("interface_status", packet)

    assert status["link_speed_mbps"] == 1000
    [interface] = status["interfaces"]
    assert interface["ip_address"] == "192.0.2.10"
    assert interface["mac_address"] == "02:00:00:00:00:01"
    assert interface["mode"] == "dynamic"
    assert interface["netmask"] == "255.255.255.0"
    assert status["reboot_required"] is False


@pytest.mark.parametrize(
    "kind,command,decoder",
    [
        ("property_directory", "property_directory", "property_directory"),
        ("receiver_port_ranges", "query_receiver_port_ranges", "receiver_port_ranges"),
    ],
)
def test_virtual_directory_and_port_replies_use_typed_native_responses(kind, command, decoder):
    request = core.build_command({"command": command, "protocol_id": 0x2809, "message_id": 7})
    response = VirtualDevice()._handle_request(request, ("192.0.2.10", 4440))
    native = core.build_response(
        {"protocol_id": 0x2809, "transaction_id": 7, "result_code": 1, "response": {"kind": kind}}
    )

    assert response == native
    parsed = core.parse_response(decoder, native)
    if kind == "property_directory":
        assert len(parsed["properties"]) == 31
        assert parsed["properties"][0] == {"property_id": 0x8020, "flags": 1}
    else:
        assert parsed["first_port_range_start"] == 14336
        assert parsed["first_port_range_end"] == 14589
        assert parsed["second_port_range_start"] == 14590
        assert parsed["second_port_range_end"] == 14591


@pytest.mark.parametrize("command", ["dante_model", "make_model"])
@pytest.mark.parametrize("damage", [None, "revision", "reserved", "truncated", "extra_bytes"])
def test_virtual_identity_requests_require_a_complete_supported_envelope(command, damage):
    device = VirtualDevice()
    transport = RecordingTransport()
    device._mcast_transport = transport
    packet = bytearray(core.build_command({"command": command, "mac": "020000000001"}))

    if damage == "revision":
        packet[25] = 0x99
    elif damage == "reserved":
        packet[6] = 1
    elif damage == "truncated":
        del packet[-1]
    elif damage == "extra_bytes":
        packet.extend(b"\x00\x00")

    packet[2:4] = len(packet).to_bytes(2, "big")
    device._handle_request(bytes(packet), ("192.0.2.11", 49152))

    if damage is None:
        assert len(transport.sent) == 2
        assert transport.sent[-1][1] == ("192.0.2.11", 49152)
        assert transport.sent[0][0][24:] == transport.sent[1][0][24:]
    else:
        assert transport.sent == []


@pytest.mark.parametrize(
    "command",
    ["clear_all_configuration", "clear_all_configuration_preserving_internet_protocol_settings"],
)
def test_virtual_device_does_not_confirm_unimplemented_configuration_clear(command):
    device = VirtualDevice()
    transport = RecordingTransport()
    device._mcast_transport = transport
    packet = core.build_command({"command": command, "host_mac": "020000000001", "message_id": 7})

    device._handle_request(packet, ("192.0.2.11", 49152))

    assert transport.sent == []


@pytest.mark.parametrize("accepted,result", [(True, "0001"), (False, "0030")])
def test_native_acknowledgement_reply_uses_request_identity_and_named_outcome(accepted, result):
    request = core.build_command({"command": "remove_subscriptions", "rx_channels": [1], "message_id": 7})
    reply = core.build_response(
        {
            "protocol_id": 10239,
            "transaction_id": 7,
            "result_code": 1,
            "response": {"kind": "acknowledgement", "request": list(request), "accepted": accepted},
        }
    )

    assert reply == bytes.fromhex("27ff000a00073014" + result)
    assert core.command_acknowledgement(reply)["accepted"] is accepted


@pytest.mark.parametrize("damage", ["transaction", "protocol", "response", "length"])
def test_native_acknowledgement_cannot_reply_to_mismatched_or_invalid_request(damage):
    request = bytearray(core.build_command({"command": "remove_subscriptions", "rx_channels": [1], "message_id": 7}))
    specification = {"protocol_id": 10239, "transaction_id": 7, "result_code": 1}

    if damage == "transaction":
        specification["transaction_id"] = 8
    elif damage == "protocol":
        specification["protocol_id"] = 10249
    elif damage == "response":
        request[8:10] = bytes.fromhex("0001")
    else:
        request.extend(b"extra")

    specification["response"] = {"kind": "acknowledgement", "request": list(request), "accepted": True}

    with pytest.raises(core.NetaudioCoreError):
        core.build_response(specification)


@pytest.mark.parametrize(
    "opcode,response",
    [
        (0x2200, {"kind": "empty_flows", "channel_type": "tx"}),
        (0x3200, {"kind": "empty_flows", "channel_type": "rx"}),
        (0x2204, {"kind": "empty_transmitter_flow_labels"}),
    ],
)
def test_virtual_empty_flow_replies_use_native_response_codec(opcode, response):
    request = bytes.fromhex("27ff000a0042") + opcode.to_bytes(2, "big") + bytes.fromhex("0000")
    expected = bytes.fromhex("27ff000c0042") + opcode.to_bytes(2, "big") + bytes.fromhex("00010200")
    specification = {"protocol_id": 0x27FF, "transaction_id": 66, "result_code": 1, "response": response}

    assert VirtualDevice()._handle_request(request, ("192.0.2.10", 4440)) == expected
    assert core.build_response(specification) == expected

    with pytest.raises(core.NetaudioCoreError):
        core.build_response({**specification, "protocol_id": 0x1200})


@pytest.mark.parametrize("damage", ["length", "trailing", "response", "revision", "short"])
def test_virtual_device_drops_invalid_control_envelopes(damage):
    device = VirtualDevice()
    request = bytearray(core.build_command({"command": "device_name", "message_id": 7}))

    if damage == "length":
        request[2:4] = (len(request) + 1).to_bytes(2, "big")
    elif damage == "trailing":
        request.extend(b"extra")
    elif damage == "response":
        request[8:10] = bytes.fromhex("0001")
    elif damage == "revision":
        request[:2] = bytes.fromhex("280e")
    else:
        del request[8:]

    assert device._handle_request(bytes(request), ("192.0.2.10", 4440)) is None


def test_virtual_cmc_registration_reply_uses_native_codec_and_preserves_address():
    device = VirtualDevice(VirtualDeviceConfig())
    device._local_ip = "192.0.2.10"
    request = core.build_command({"command": "cmc_register", "message_id": 66, "host_mac": "001122334455"})
    expected = bytes.fromhex("1200002000421001000100000000c000020a000000010000c000020a21fc0000")
    specification = {
        "protocol_id": 0x1200,
        "transaction_id": 66,
        "result_code": 1,
        "response": {"kind": "cmc_registration", "device_ip": "192.0.2.10", "settings_port": 8700},
    }

    assert device._handle_request(request, ("192.0.2.20", 8800)) == expected
    assert core.build_response(specification) == expected
    assert core.parse_response("cmc_registration", expected) == {"sequence": 66, "accepted": True}

    with pytest.raises(core.NetaudioCoreError):
        core.build_response({**specification, "protocol_id": 0x27FF})


def test_native_response_codec_preserves_channel_inventory_and_name():
    envelope = {"protocol_id": 0x27FF, "transaction_id": 7, "result_code": 1}
    count = core.build_response({**envelope, "response": {"kind": "channel_count", "tx_count": 3, "rx_count": 2}})
    name = core.build_response({**envelope, "response": {"kind": "device_name", "name": "Console"}})

    assert count == bytes.fromhex(
        "27ff002c00071000000100300003000200040008000800200020000500010001000000000000000000000000"
    )
    parsed = core.parse_response("channel_count", count)
    assert (parsed["tx_count"], parsed["rx_count"]) == (3, 2)
    assert core.parse_response("device_name", name) == "Console"


@pytest.mark.parametrize(
    "handler",
    [
        "_handle_device_name",
        "_handle_channel_count",
        "_handle_tx_channel_names",
        "_handle_device_info",
        "_handle_device_settings",
        "_handle_rx_channels",
        "_handle_tx_channels",
    ],
)
def test_virtual_arc_replies_reject_cmc_protocol(handler):
    device = VirtualDevice()

    with pytest.raises(core.NetaudioCoreError):
        getattr(device, handler)(7, b"", protocol_id=0x1200)


@pytest.mark.parametrize(
    "names", [[], ["Left"], ["Left", "Right"], ["One", "Two", "Three"], [f"TX{i}" for i in range(32)]]
)
def test_native_transmitter_names_round_trip_through_virtual_device(names):
    packet = core.build_response(
        {
            "protocol_id": 0x27FF,
            "transaction_id": 7,
            "result_code": 1,
            "response": {"kind": "transmitter_names", "names": names},
        }
    )
    device = VirtualDevice(VirtualDeviceConfig(tx_channels=names))

    assert device._handle_tx_channel_names(7, b"") == packet
    assert core.parse_page("tx_friendly", packet, 1) == [[index, name] for index, name in enumerate(names, 1)]


@pytest.mark.parametrize("names", [["Left\0Other"], ["a" * 65535], ["TX"] * 33, ["TX"] * 256])
def test_virtual_transmitter_names_reject_unrepresentable_records(names):
    device = VirtualDevice(VirtualDeviceConfig(tx_channels=names))

    with pytest.raises(core.NetaudioCoreError):
        device._handle_tx_channel_names(7, b"")


@pytest.mark.parametrize(
    "direction,count", [("tx", 0), ("tx", 1), ("tx", 3), ("tx", 32), ("rx", 0), ("rx", 1), ("rx", 3), ("rx", 16)]
)
def test_virtual_channel_records_round_trip_with_native_identity_audio_and_sources(direction, count):
    names = [f"Channel {i}" for i in range(1, count + 1)]
    device = VirtualDevice(VirtualDeviceConfig(tx_channels=names, rx_channels=names, sample_rate=96000))

    if direction == "rx" and count:
        device._subscriptions[1] = ("Source", "Sender")

    packet = getattr(device, f"_handle_{direction}_channels")(7, b"")
    records = core.parse_page("tx_info" if direction == "tx" else "rx", packet, 1)
    assert [record["number"] for record in records] == list(range(1, count + 1))
    assert [record["name" if direction == "tx" else "rx_channel_name"] for record in records] == names

    if count:
        assert core.parse_response("channel_audio_metadata", packet)["sample_rate"] == 96000

    if direction == "rx" and count:
        assert records[0]["tx_channel_name"] == "Source"
        assert records[0]["tx_device_name"] == "Sender"
        assert records[0]["subscription_status_code"] == 9
        assert all(record["tx_device_name"] is None for record in records[1:])


@pytest.mark.parametrize("direction", ["tx", "rx"])
def test_virtual_channel_records_reject_embedded_terminators(direction):
    device = VirtualDevice(VirtualDeviceConfig(tx_channels=["A\0B"], rx_channels=["A\0B"]))

    with pytest.raises(core.NetaudioCoreError):
        getattr(device, f"_handle_{direction}_channels")(7, b"")


def test_virtual_device_info_reports_the_configured_model_and_display_name():
    device = VirtualDevice(VirtualDeviceConfig(name="Console", model="Virtual Mixer"))

    parsed = core.parse_response("device_info", device._handle_device_info(7, b""))

    assert parsed["model_name"] == "Virtual Mixer"
    assert parsed["display_name"] == "Console"
    assert parsed["model_code"] == "Virtual Mixer"
    assert parsed["port"] == ""


def test_native_response_codec_round_trips_device_settings():
    response = {
        "kind": "device_settings",
        "sample_rate": 96000,
        "configured_latency_ns": 250000,
        "active_latency_ns": 1000000,
        "default_latency_ns": 500000,
        "minimum_latency_ns": 125000,
        "maximum_latency_ns": 2000000,
    }
    packet = core.build_response({"protocol_id": 0x27FF, "transaction_id": 7, "result_code": 1, "response": response})
    parsed = core.parse_response("device_settings", packet)

    assert parsed["sample_rate"] == 96000
    assert parsed["configured_latency_ns"] == 250000
    assert parsed["active_latency_ns"] == 1000000
    assert parsed["default_latency_ns"] == 500000
    assert parsed["min_latency_ns"] == 125000
    assert parsed["max_latency_ns"] == 2000000


@pytest.mark.parametrize(
    "response",
    [
        {"kind": "channel_count", "tx_count": 65535, "rx_count": 1},
        {"kind": "device_name", "name": "Console\u0000Other"},
        {"kind": "device_name", "name": "Desk", "opcod": 0x1003},
        {
            "kind": "device_info",
            "model_name": "Model",
            "display_name": "Console\u0000Other",
            "model_code": "Model",
            "port": "",
        },
    ],
)
def test_native_response_codec_rejects_unrepresentable_or_ambiguous_payload(response):
    with pytest.raises(core.NetaudioCoreError):
        core.build_response({"protocol_id": 0x27FF, "transaction_id": 7, "result_code": 1, "response": response})


def test_native_response_codec_rejects_unknown_protocol():
    with pytest.raises(core.NetaudioCoreError):
        core.build_response(
            {
                "protocol_id": 0x2810,
                "transaction_id": 7,
                "result_code": 1,
                "response": {"kind": "device_name", "name": "Desk"},
            }
        )


@pytest.mark.parametrize("protocol", [0x2729, 0x2809])
@pytest.mark.parametrize("direction", ["rx", "tx"])
def test_virtual_rename_changes_only_the_requested_channel(protocol, direction):
    device = VirtualDevice(VirtualDeviceConfig(rx_channels=["RX One", "RX Two"], tx_channels=["TX One", "TX Two"]))
    request = core.build_command(
        {
            "command": "set_channel_name",
            "channel_type": direction,
            "channel_number": 2,
            "name": "Console",
            "protocol_id": protocol,
            "message_id": 7,
        }
    )

    response = device._handle_request(request, ("192.0.2.10", 4440))

    assert device.config.rx_channels == (["RX One", "Console"] if direction == "rx" else ["RX One", "RX Two"])
    assert device.config.tx_channels == (["TX One", "Console"] if direction == "tx" else ["TX One", "TX Two"])
    assert response[:2] == protocol.to_bytes(2, "big")
    assert response[4:8] == request[4:8]
    assert response[8:10] == bytes.fromhex("0001")


@pytest.mark.parametrize(
    "damage", ["length", "prefix", "pointer", "unterminated", "utf8", "zero_channel", "missing_channel"]
)
def test_virtual_rename_rejects_invalid_requests_without_changing_names(damage):
    device = VirtualDevice(VirtualDeviceConfig(tx_channels=["One", "Two"]))
    request = bytearray(
        core.build_command(
            {
                "command": "set_channel_name",
                "channel_type": "tx",
                "channel_number": 2,
                "name": "Console",
                "protocol_id": 0x2729,
                "message_id": 7,
            }
        )
    )

    if damage == "length":
        request[2:4] = (len(request) + 1).to_bytes(2, "big")
    elif damage == "prefix":
        request[11] = 2
    elif damage == "pointer":
        request[16:18] = (8).to_bytes(2, "big")
    elif damage == "unterminated":
        request[-1] = 65
    elif damage == "utf8":
        request[24] = 255
    elif damage == "zero_channel":
        request[14:16] = bytes(2)
    else:
        request[14:16] = (3).to_bytes(2, "big")

    response = device._handle_request(bytes(request), ("192.0.2.10", 4440))

    assert device.config.tx_channels == ["One", "Two"]

    if damage == "length":
        assert response is None
    else:
        assert response[8:10] == bytes.fromhex("0030")


@pytest.mark.parametrize("channels", [[2], [2, 3]])
def test_virtual_disconnect_removes_requested_channels_and_preserves_other_routes(channels):
    device = VirtualDevice(VirtualDeviceConfig(rx_channels=["One", "Two", "Three"]))
    device._subscriptions = {number: ("Source", "Transmitter") for number in (1, 2, 3)}
    request = core.build_command({"command": "remove_subscriptions", "rx_channels": channels, "message_id": 7})

    response = device._handle_request(request, ("192.0.2.10", 4440))

    assert device._subscriptions == {
        number: ("Source", "Transmitter") for number in (1, 2, 3) if number not in channels
    }
    assert response == bytes.fromhex("27ff000a000730140001")


@pytest.mark.parametrize("damage", ["length", "count", "zero_channel", "unknown_channel", "truncated", "protocol"])
def test_virtual_disconnect_rejects_invalid_batch_without_removing_any_route(damage):
    device = VirtualDevice(VirtualDeviceConfig(rx_channels=["One", "Two", "Three"]))
    before = {number: ("Source", "Transmitter") for number in (1, 2, 3)}
    device._subscriptions = dict(before)
    request = bytearray(core.build_command({"command": "remove_subscriptions", "rx_channels": [1, 2], "message_id": 7}))

    if damage == "length":
        request[2:4] = (len(request) + 1).to_bytes(2, "big")
    elif damage == "count":
        request[8:12] = (3).to_bytes(4, "big")
    elif damage == "zero_channel":
        request[-4:] = bytes(4)
    elif damage == "unknown_channel":
        request[-4:] = (4).to_bytes(4, "big")
    elif damage == "protocol":
        request[:2] = bytes.fromhex("2729")
    else:
        request = request[:-1]

    response = device._handle_request(bytes(request), ("192.0.2.10", 4440))

    assert device._subscriptions == before

    if damage in {"length", "truncated"}:
        assert response is None
    else:
        assert response[8:10] == bytes.fromhex("0030")


def test_native_disconnect_decoder_preserves_full_channel_identifiers():
    request = core.build_command({"command": "remove_subscriptions", "rx_channels": [257, 65537], "message_id": 7})

    assert core.parse_response("remove_subscriptions_request", request) == [257, 65537]


def test_virtual_subscribe_applies_native_command_batch_and_preserves_other_routes():
    device = VirtualDevice(VirtualDeviceConfig(rx_channels=["One", "Two", "Three"]))
    device._subscriptions = {1: ("Existing", "Transmitter")}
    records = [
        {"rx_channel": 2, "tx_channel": "Left", "tx_device": "Source"},
        {"rx_channel": 3, "tx_channel": "Right", "tx_device": "Source"},
    ]
    request = core.build_command({"command": "add_subscriptions", "subscriptions": records, "message_id": 9})

    response = device._handle_request(request, ("192.0.2.10", 4440))

    assert device._subscriptions == {1: ("Existing", "Transmitter"), 2: ("Left", "Source"), 3: ("Right", "Source")}
    assert response == bytes.fromhex("27ff000a000930100001")
    assert core.parse_response("add_subscriptions_request", request) == records


@pytest.mark.parametrize(
    "damage", ["length", "count", "pointer", "zero_channel", "unknown_channel", "unterminated", "protocol", "prefix"]
)
def test_virtual_subscribe_rejects_whole_invalid_batch_before_changing_routes(damage):
    device = VirtualDevice(VirtualDeviceConfig(rx_channels=["One", "Two", "Three"]))
    before = {1: ("Existing", "Transmitter")}
    device._subscriptions = dict(before)
    request = bytearray(
        core.build_command(
            {
                "command": "add_subscriptions",
                "subscriptions": [
                    {"rx_channel": 1, "tx_channel": "Left", "tx_device": "Source"},
                    {"rx_channel": 2, "tx_channel": "Right", "tx_device": "Source"},
                ],
                "message_id": 9,
            }
        )
    )

    if damage == "length":
        request[2:4] = (len(request) + 1).to_bytes(2, "big")
    elif damage == "count":
        request[11] = 17
    elif damage == "pointer":
        request[20:22] = (8).to_bytes(2, "big")
    elif damage == "zero_channel":
        request[18:20] = bytes(2)
    elif damage == "unknown_channel":
        request[18:20] = (4).to_bytes(2, "big")
    elif damage == "protocol":
        request[:2] = bytes.fromhex("2729")
    elif damage == "prefix":
        request[10] = 3
    else:
        request[-1] = 65

    response = device._handle_request(bytes(request), ("192.0.2.10", 4440))

    assert device._subscriptions == before

    if damage == "length":
        assert response is None
    else:
        assert response[8:10] == bytes.fromhex("0030")


def test_native_audio_publication_matches_synthetic_wire_contract():
    packet = core.build_publication(
        {
            "source_ip": "192.0.2.10",
            "message_id": 7,
            "publication": {
                "kind": "audio",
                "capability": "encoding",
                "current_value": 32,
                "supported_values": [16, 24, 32],
            },
        }
    )
    assert packet == bytes.fromhex(
        "ffff003c000700000000c000020a0000"
        "417564696e6174650724008200000000"
        "00180003000000200000000000020000000000100000001800000020"
    )
    assert core.parse_response("encoding_status", packet)["available_values"] == [16, 24, 32]


@pytest.mark.parametrize(
    "override",
    [
        {"publication": {"kind": "unknown"}},
        {"source_ip": "::1"},
        {"message_id": 65536},
        {"publication": {"kind": "audio", "capability": "encoding", "current_value": -1, "supported_values": [24]}},
        {"publication": {"kind": "audio", "capability": "encoding", "current_value": 24, "supported_values": [True]}},
        {
            "publication": {
                "kind": "audio",
                "capability": "encoding",
                "current_value": 24,
                "supported_values": [24] * 16372,
            }
        },
        {"unused_field": 1},
    ],
)
def test_native_audio_publication_rejects_unrepresentable_or_unknown_input(override):
    spec = {
        "source_ip": "192.0.2.10",
        "message_id": 0,
        "publication": {"kind": "audio", "capability": "encoding", "current_value": 24, "supported_values": [24]},
        **override,
    }

    with pytest.raises(core.NetaudioCoreError):
        core.build_publication(spec)


@pytest.mark.parametrize(
    "publication,expected",
    [
        (
            {"kind": "heartbeat", "tx_count": 2, "rx_count": 1},
            "fffe004c000700000000c000020a0000417564696e6174650008000110000000"
            "00108001000400040007000000000000"
            "001c80020004001000070000000200000001000000180000ffffff00",
        ),
    ],
)
def test_native_publication_matches_synthetic_multicast_wire_contract(publication, expected):
    assert core.build_publication(
        {"source_ip": "192.0.2.10", "message_id": 7, "publication": publication}
    ) == bytes.fromhex(expected)


@pytest.mark.parametrize(
    "publication",
    [
        {"kind": "board_info", "name": "Desk\u0000Other"},
        {"kind": "board_info", "name": "Desk", "protocol_id": 10239},
        {"kind": "clock_status", "mac_address": [0] * 5},
        {"kind": "interface_status", "mac_address": [0] * 7},
        {"kind": "heartbeat", "tx_count": 65535, "rx_count": 1},
        {"kind": "heartbeat", "tx_count": True, "rx_count": 1},
    ],
)
def test_publication_rejects_invalid_framing_and_impossible_channel_counts(publication):
    with pytest.raises(core.NetaudioCoreError):
        core.build_publication({"source_ip": "192.0.2.10", "message_id": 7, "publication": publication})


def test_virtual_device_reports_configured_encoding_capabilities():
    device = VirtualDevice(
        VirtualDeviceConfig(
            encoding=20,
            supported_encodings=[20, 24, 32],
        )
    )

    packet = device._build_audio_capability_status_packet(
        "encoding",
        device.config.encoding,
        device.config.supported_encodings,
    )

    assert core.parse_response("encoding_status", packet) == {
        "record_protocol_version": 0x0724,
        "current_value": 20,
        "requested_value": 0,
        "update_mode": 2,
        "available_values": [20, 24, 32],
        "flags": None,
    }


@pytest.mark.parametrize("encoding,supported", [(16, [16]), (24, [24]), (32, [32, 16, 24, 16])])
def test_virtual_channel_response_preserves_pcm_capabilities(encoding, supported):
    device = VirtualDevice(VirtualDeviceConfig(encoding=encoding, supported_encodings=supported))

    response = device._handle_tx_channels(7, b"")
    parsed = core.parse_response("channel_audio_metadata", response)

    assert parsed["current_encoding"] == encoding
    assert parsed["supported_encodings"] == sorted(set(supported))
    assert parsed["sample_rate"] == 48_000


@pytest.mark.parametrize(
    "direction,encoding,supported", [("tx", 20, [20, 24]), ("rx", 20, [20, 24]), ("tx", 24, [20, 24])]
)
def test_virtual_device_does_not_advertise_inconsistent_pcm_capabilities(direction, encoding, supported):
    device = VirtualDevice(VirtualDeviceConfig(encoding=encoding, supported_encodings=supported))

    assert getattr(device, f"_handle_{direction}_channels")(7, b"")[8:10] == bytes.fromhex("0030")


@pytest.mark.parametrize("encoding,supported", [(24, [16]), (24, [])])
def test_native_pcm_publication_declines_unknown_or_inconsistent_capabilities(encoding, supported):
    assert (
        core.channel_audio_publication({"sample_rate": 48_000, "encoding": encoding, "supported_encodings": supported})
        is None
    )


def test_virtual_device_reports_configured_sample_rate_capabilities():
    device = VirtualDevice(
        VirtualDeviceConfig(
            sample_rate=384_000,
            supported_sample_rates=[48_000, 96_000, 384_000],
        )
    )

    packet = device._build_audio_capability_status_packet(
        "sample_rate",
        device.config.sample_rate,
        device.config.supported_sample_rates,
    )

    assert core.parse_response("sample_rate_status", packet) == {
        "record_protocol_version": 0x0724,
        "current_value": 384_000,
        "requested_value": 0,
        "update_mode": 2,
        "available_values": [48_000, 96_000, 384_000],
        "flags": None,
    }


@pytest.mark.parametrize("protocol", [0x27FF, 0x2809])
@pytest.mark.parametrize("latency_ms", [0.25, 1.000001])
def test_virtual_latency_write_is_confirmed_by_native_settings_readback(protocol, latency_ms):
    device = VirtualDevice()
    request = core.build_command(
        {"command": "set_latency", "latency": latency_ms, "protocol_id": protocol, "message_id": 7}
    )

    response = device._handle_request(request, ("192.0.2.10", 4440))

    value = {0.25: "0003d090", 1.000001: "000f4241"}[latency_ms]
    assert response == bytes.fromhex(
        f"{protocol:04x}002400071101000104048205001c021100048301002003100004{value}{value}"
    )
    assert core.command_acknowledgement(response)["accepted"] is True
    status = core.parse_response("device_settings", device._handle_device_settings(8, b"", protocol))
    assert status["configured_latency_ns"] == round(latency_ms * 1_000_000)
    assert status["active_latency_ns"] == status["configured_latency_ns"]


@pytest.mark.parametrize("protocol", [0x2729, 0x1200, 0xFFFF])
def test_latency_publication_rejects_protocols_without_the_supported_property_layout(protocol):
    with pytest.raises(core.NetaudioCoreError):
        core.build_response(
            {
                "protocol_id": protocol,
                "transaction_id": 7,
                "result_code": 1,
                "response": {"kind": "latency_applied", "latency_ns": 250000},
            }
        )


def test_native_latency_publication_preserves_full_nanosecond_value():
    packet = core.build_response(
        {
            "protocol_id": 0x2809,
            "transaction_id": 7,
            "result_code": 1,
            "response": {"kind": "latency_applied", "latency_ns": 20312500},
        }
    )
    assert packet == bytes.fromhex("2809002400071101000104048205001c0211000483010020031000040135f1b40135f1b4")


@pytest.mark.parametrize(
    "damage", ["length", "count", "record_size", "unknown_property", "frames_per_packet", "mismatch"]
)
def test_virtual_latency_rejects_unsupported_layout_without_changing_state(damage):
    device = VirtualDevice(VirtualDeviceConfig(configured_latency_ns=1_000_000, active_latency_ns=1_000_000))
    request = bytearray(core.build_command({"command": "set_latency", "latency": 0.25, "message_id": 7}))

    if damage == "length":
        request[2:4] = (len(request) + 1).to_bytes(2, "big")
    elif damage == "count":
        request[10] = 3
    elif damage == "record_size":
        request[11] = 5
    elif damage == "unknown_property":
        request[16:18] = bytes.fromhex("ffff")
    elif damage == "frames_per_packet":
        request[18:20] = (8).to_bytes(2, "big")
    else:
        request[-4:] = (500_000).to_bytes(4, "big")

    response = device._handle_request(bytes(request), ("192.0.2.10", 4440))

    if damage == "length":
        assert response is None
    else:
        assert core.command_acknowledgement(response)["accepted"] is False
    assert device.config.configured_latency_ns == 1_000_000
    assert device.config.active_latency_ns == 1_000_000


def test_virtual_device_settings_distinguish_configured_and_active_latency():
    device = VirtualDevice(
        VirtualDeviceConfig(
            configured_latency_ns=250_000,
            active_latency_ns=1_000_000,
        )
    )

    packet = device._handle_device_settings(7, b"")
    parsed = core.parse_response("device_settings", packet)

    assert parsed["configured_latency_ns"] == 250_000
    assert parsed["active_latency_ns"] == 1_000_000
    assert parsed["default_latency_ns"] == 1_000_000
    assert parsed["min_latency_ns"] == 150_000
    assert parsed["max_latency_ns"] == 21_333_334


@pytest.mark.parametrize(("kind", "value"), [("encoding", 16), ("sample_rate", 96000)])
@pytest.mark.parametrize("write", [False, True])
def test_virtual_audio_configuration_round_trips_native_commands_and_readback(kind, value, write):
    device = VirtualDevice(VirtualDeviceConfig(supported_sample_rates=[48000, 96000]))
    device._local_ip = "192.0.2.10"
    transport = RecordingTransport()
    device._mcast_transport = transport
    specification = {"command": f"set_{kind}" if write else f"probe_{kind}", "message_id": 7}

    if write:
        specification[kind] = value
    else:
        specification["host_mac"] = "020000000001"

    packet = core.build_command(specification)
    assert core.parse_response("device_request", packet) == {
        "family": "settings",
        "operation": "audio",
        "kind": kind,
        "value": value if write else None,
    }

    device._handle_request(packet, ("192.0.2.11", 49152))

    assert len(transport.sent) == 1
    status = core.parse_response(f"{kind}_status", transport.sent[0][0])
    assert status["current_value"] == (value if write else {"encoding": 24, "sample_rate": 48000}[kind])
    assert getattr(device.config, kind) == status["current_value"]


@pytest.mark.parametrize(("kind", "value"), [("encoding", 16), ("sample_rate", 96000)])
@pytest.mark.parametrize("damage", ["revision", "reserved", "extra_bytes", "truncated", "mode"])
def test_virtual_audio_configuration_rejects_malformed_writes_without_mutation(kind, value, damage):
    device = VirtualDevice(VirtualDeviceConfig(supported_sample_rates=[48000, 96000]))
    transport = RecordingTransport()
    device._mcast_transport = transport
    packet = bytearray(core.build_command({"command": f"set_{kind}", kind: value, "message_id": 7}))

    if damage == "revision":
        packet[25] = 0x99
    elif damage == "reserved":
        packet[6] = 1
    elif damage == "extra_bytes":
        packet.extend(b"\x00\x00")
        packet[2:4] = len(packet).to_bytes(2, "big")
    elif damage == "truncated":
        del packet[-1]
        packet[2:4] = len(packet).to_bytes(2, "big")
    else:
        packet[32:36] = (2).to_bytes(4, "big")

    device._handle_request(bytes(packet), ("192.0.2.11", 49152))

    assert device.config.sample_rate == 48000
    assert device.config.encoding == 24
    assert transport.sent == []


def test_same_host_encoding_probe_receives_status_response():
    device = VirtualDevice()
    device._local_ip = "192.0.2.10"
    recording_transport = RecordingTransport()
    device._mcast_transport = recording_transport
    protocol = _McastInfoProtocol(device)
    packet = core.build_command({"command": "probe_encoding", "host_mac": "020000000001", "message_id": 0x83})

    protocol.datagram_received(packet, ("192.0.2.10", 49152))

    assert len(recording_transport.sent) == 1
    response, destination = recording_transport.sent[0]
    assert destination == ("224.0.0.231", 8702)
    assert core.parse_response("encoding_status", response)["available_values"] == [24, 16, 32]


def test_virtual_device_heartbeat_round_trips_odd_signal_presence_count():
    device = VirtualDevice(
        VirtualDeviceConfig(
            tx_channels=["TX 1", "TX 2"],
            rx_channels=["RX 1"],
        )
    )
    recording_transport = RecordingTransport()
    device._mcast_transport = recording_transport

    device._send_heartbeat()

    assert len(recording_transport.sent) == 1
    packet, destination = recording_transport.sent[0]
    assert destination == ("224.0.0.233", 8708)

    signal_record = packet[0x20 + 0x10 :]
    assert len(signal_record) == 0x1C
    assert signal_record[:8] == bytes.fromhex("001c800200040010")
    assert signal_record[0x18:] == bytes.fromhex("ffffff00")

    [parsed] = parse_signal_presence_records(packet)
    assert parsed["tx_count"] == 2
    assert parsed["rx_count"] == 1
    assert parsed["tx_levels"] == [0xFF, 0xFF]
    assert parsed["rx_levels"] == [0xFF]
    assert parsed["padding_length"] == 1
