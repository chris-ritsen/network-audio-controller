"""Synthetic specification vectors exercised through the native public interface."""

import json
import subprocess
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from netaudio import core
from netaudio.dante.metering import classify_signal_presence, metering_value_dbfs
from tests.test_flow_inventory import receiver_pages
from tests.test_clock_port_records import CLOCK_STATUS_PACKET
from tests.status_test_support import application_with_device


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ["/refresh-clock", "/set-clock-configuration"])
async def test_http_does_not_silently_ignore_obsolete_clock_revision(path):
    from tests.http_api_test_support import FakeWriter, make_http_server

    _, device = application_with_device("clock.local.", "192.0.2.1")
    server = make_http_server({device.server_name: device})
    server.application.probe_clocking_status = AsyncMock()
    server.application.set_clock_configuration = AsyncMock()
    writer = FakeWriter()
    body = json.dumps({"device": device.server_name, "record_revision": 0x0734, "changes": {}}).encode()
    await server._route("POST", path, body, writer, None)
    code, result = writer.response()
    assert code == 400
    assert "control_profile" in result["error"]
    server.application.probe_clocking_status.assert_not_awaited()
    server.application.set_clock_configuration.assert_not_awaited()


@pytest.mark.asyncio
async def test_incomplete_receiver_query_keeps_last_complete_inventory_and_diagnostics():
    app, device = application_with_device("receiver.local.", "192.0.2.1")
    device.services = {"arc": {"type": "_netaudio-arc._udp.local.", "properties": {"arcp_vers": "2.8.9"}}}
    first, terminal, _, _ = receiver_pages(0x2809)
    device.apply_receiver_flow_status_page(core.parse_response("modern_arc_receiver_flow_status_page", terminal))
    baseline = device.receiver_flows.copy()
    device.execute = AsyncMock(side_effect=[first, None])

    with pytest.raises(RuntimeError, match="no response"):
        await app.query_modern_arc_receiver_flow_status(device)

    assert device.receiver_flows == baseline
    assert device.receiver_flow_completeness == "partial"
    assert device.receiver_flow_partial_inventory["raw_pages"] == [list(first)]
    assert [call.args[0]["starting_flow"] for call in device.execute.await_args_list] == [1, 16]


def extended_clock_packet(*, validity=0x7F, offset=92, stride=64, count=2):
    record = bytearray(220)
    record[:4] = bytes.fromhex("073a0020")
    record[46:48] = (48).to_bytes(2, "big")
    record[58:60] = bytes.fromhex("0804")
    record[86:92] = stride.to_bytes(2, "big") + offset.to_bytes(2, "big") + count.to_bytes(2, "big")
    record[92:96] = bytes.fromhex("007e0001")
    record[96:105] = bytes.fromhex("40010100fd01f98002")
    record[124:128] = bytes.fromhex("deadbeef")
    record[128:132] = validity.to_bytes(4, "big")
    record[140:146] = bytes([2, 1, 7, 248, 128, 129])
    record[152] = 46
    record[158:160] = (2).to_bytes(2, "big")
    header = bytearray(CLOCK_STATUS_PACKET[:24])
    header[2:4] = (24 + len(record)).to_bytes(2, "big")
    return bytes(header + record)


def test_extended_clock_readback_preserves_raw_and_nullable_values():
    status = core.parse_response("ptp_clock_status", extended_clock_packet())
    assert status["follower_only"] is True
    assert status["extended_capabilities"] == 0xDEADBEEF
    assert status["extended_validity"] == 0x7F
    assert status["ptpv2_priority1"] == 128
    assert status["multicast_dscp"] == 46
    first, second = status["extended_ports"]
    assert first["port_id"] is None
    assert first["record_index"] == 0
    assert first["sync_interval"] == -3 and first["sync_interval_raw"] == 253
    assert first["peer_delay_interval"] == -128
    assert first["follower_only"] is True
    assert second["ttl"] is None and second["follower_only"] is None
    assert len(first["raw_record"]) == 64
    unavailable = core.parse_response("ptp_clock_status", extended_clock_packet(validity=0))
    assert unavailable["ptpv2_priority1"] is None and unavailable["multicast_dscp"] is None


def paired_clock_packet(port_ids=(17, 3), *, interface_valid=True):
    packet = bytearray(extended_clock_packet())
    record = bytearray(packet[24:])
    record[80:84] = bytes.fromhex("00dc0001")
    record[92:96] = (0x7F if interface_valid else 0x7E).to_bytes(2, "big") + bytes.fromhex("002a")
    record.extend(len(port_ids).to_bytes(2, "big") + bytes.fromhex("000000e40010"))

    for port_id in port_ids:
        record.extend(bytes(2) + port_id.to_bytes(2, "big") + bytes(12))

    packet = packet[:24] + record
    packet[2:4] = len(packet).to_bytes(2, "big")
    return bytes(packet)


@pytest.mark.parametrize("interface_valid", [True, False])
def test_clock_port_identity_survives_parse_plan_encode_and_readback(interface_valid):
    from netaudio.dante.clock_control import observed_clock_configuration

    status = core.parse_response("ptp_clock_status", paired_clock_packet(interface_valid=interface_valid))
    assert [port["port_id"] for port in status["extended_ports"]] == [17, 3]
    assert status["extended_ports"][0]["network_interface_index"] == (42 if interface_valid else None)
    assert status["clock_port_records"][0]["network_interface_index"] == (42 if interface_valid else None)
    assert observed_clock_configuration(status)["ports"][0]["port_id"] == 17
    request = {"ports": [{"port_id": 17, "ttl": 63, "sync_interval": -3}]}
    plan = core.plan_clock_configuration({"status": status, "changes": request})
    assert plan["changes"] == {"ports": [{"port_id": 17, "ttl": 63}]}
    packet = core.build_command({"command": "clock_control", "host_mac": "020000000001", "control": plan["control"]})
    assert packet[24 + 68 : 24 + 74] == bytes.fromhex("000300113f00")
    assert not core.clock_configuration_matches(status, request)
    updated = bytearray(paired_clock_packet(interface_valid=interface_valid))
    updated[24 + 96] = 63
    readback = core.parse_response("ptp_clock_status", bytes(updated))
    assert core.clock_configuration_matches(readback, request)


@pytest.mark.parametrize("port_ids", [(), (17,), (17, 3, 4), (17, 17), (0, 3), (65, 3)])
def test_unpaired_or_invalid_clock_port_identity_cannot_author_mutations(port_ids):
    status = core.parse_response("ptp_clock_status", paired_clock_packet(port_ids))
    assert status["extended_ports"][0]["port_id"] is None

    for candidate in (1, 17, 42):
        with pytest.raises(core.NetaudioCoreError):
            core.plan_clock_configuration({"status": status, "changes": {"ports": [{"port_id": candidate, "ttl": 63}]}})


def test_missing_clock_base_vector_keeps_port_unavailable():
    status = core.parse_response("ptp_clock_status", extended_clock_packet())

    with pytest.raises(core.NetaudioCoreError):
        core.plan_clock_configuration({"status": status, "changes": {"ports": [{"port_id": 1, "ttl": 63}]}})


@pytest.mark.parametrize(
    "geometry",
    [
        {"offset": 48},
        {"offset": 219},
        {"stride": 11},
        {"stride": 65535},
        {"count": 65535},
        {"offset": 0},
        {"count": 0},
    ],
)
def test_clock_descriptor_rejects_overlap_truncation_and_inconsistent_geometry(geometry):
    with pytest.raises(core.NetaudioCoreError):
        core.parse_response("ptp_clock_status", extended_clock_packet(**geometry))


def test_clock_planning_uses_local_profile_and_fresh_valid_fields():
    status = core.parse_response("ptp_clock_status", extended_clock_packet())
    plan = core.plan_clock_configuration(
        {
            "status": status,
            "changes": {"ptpv2_priority1": 100, "multicast_dscp": 46},
        }
    )
    assert plan["control"]["control_profile"] == 0x073A
    assert plan["changes"] == {"ptpv2_priority1": 100}
    assert not core.clock_configuration_matches(status, plan["requested"])
    status["ptpv2_priority1"] = 100
    assert core.clock_configuration_matches(status, plan["requested"])

    for changes in ({"ptpv1_enabled": True}, {"multicast_dscp": 64}):
        with pytest.raises(core.NetaudioCoreError):
            core.plan_clock_configuration({"status": status, "changes": changes})

    status["ptpv2_priority1"] = None
    with pytest.raises(core.NetaudioCoreError):
        core.plan_clock_configuration({"status": status, "changes": {"ptpv2_priority1": 1}})


@pytest.mark.parametrize("protocol", [0x2809, 0x280C, 0x280F])
@pytest.mark.parametrize("first", [1, 16])
def test_receiver_range_keeps_media_and_flow_identifiers_separate(protocol, first):
    packet = core.build_command(
        {
            "command": "query_modern_arc_receiver_flow_status",
            "protocol_id": protocol,
            "starting_flow": first,
            "message_id": 0x2856,
        }
    )
    expected = bytes.fromhex("000000000000000000010001") + first.to_bytes(2, "big") + bytes(6)
    tail = bytes.fromhex("830283060310") if protocol == 0x2809 else bytes(6)
    assert packet == protocol.to_bytes(2, "big") + bytes.fromhex("002228563600") + expected + tail


@pytest.mark.parametrize("protocol", [0x2809, 0x280C, 0x280F])
@pytest.mark.parametrize("capacity", [16, 64, 255])
def test_partial_receiver_inventory_produces_one_correct_continuation(protocol, capacity):
    first, terminal, _, _ = receiver_pages(0x2809)
    next_id, count = 16, 16
    if protocol != 0x2809:
        fixture = json.loads((Path(__file__).parent / "fixtures/arc_280c_capture.json").read_text())
        page = bytearray.fromhex(fixture["shure_mxa920"]["receiver_flow_status"]["response"])
        assert page[17] == 1
        pointer = int.from_bytes(page[18:20], "big")
        page[8:10] = bytes.fromhex("8112")
        page[pointer + 2 : pointer + 4] = (1).to_bytes(2, "big")
        first = bytes(page)
        page[8:10] = bytes.fromhex("0001")
        page[pointer + 2 : pointer + 4] = (2).to_bytes(2, "big")
        terminal = bytes(page)
        next_id, count = 2, 2
    first = protocol.to_bytes(2, "big") + first[2:16] + bytes([capacity]) + first[17:]
    terminal = protocol.to_bytes(2, "big") + terminal[2:16] + bytes([capacity]) + terminal[17:]

    with core.ReceiverFlowInventory(protocol) as inventory:
        inventory.accept(first)
        assert inventory.state()["inventory"] is None
        assert inventory.state()["partial_inventory"]["complete"] is False
        assert inventory.state()["partial_inventory"]["raw_pages"] == [list(first)]
        command = inventory.state()["next_command"]
        assert command["starting_flow"] == next_id
        packet = core.build_command(command)
        assert packet[16:24] == bytes.fromhex("00010001") + next_id.to_bytes(2, "big") + bytes(2)
        inventory.accept(terminal)
        assert inventory.state()["next_command"] is None
        assert len(inventory.state()["inventory"]["flows"]) == count


@pytest.mark.parametrize("media", [3, 4])
def test_modern_subscription_set_and_clear_preserve_protocol(media):
    commands = core.plan_subscription_commands(
        {
            "protocol_id": 0x280C,
            "channels": [{"number": n, "media_type_code": media} for n in (1, 2, 3)],
            "records": [
                {"action": "set", "rx_channel": 1, "tx_channel": "Left", "tx_device": "Sender"},
                {"action": "clear", "rx_channel": 2},
            ],
        }
    )
    assert len(commands) == 1
    packet = core.build_command(commands[0])
    assert packet[:2] == bytes.fromhex("280c")
    assert packet[6:20] == bytes.fromhex("3410000000000000000008000302")
    assert packet[20:24] == bytes([0, 1, 0, media])
    assert packet[28:36] == bytes([0, 2, 0, media, 0, 0, 0, 0])
    assert packet[36:44] == bytes(8)
    assert packet[44:] == b"Left\0Sender\0"


@pytest.mark.parametrize("profile,length", [(0x0734, 40), (0x073A, 68)])
def test_clock_query_uses_local_profile_without_status(profile, length):
    packet = core.build_command(
        {"command": "refresh_clock_status", "host_mac": "020000000001", "control_profile": profile}
    )
    assert packet[24:] == profile.to_bytes(2, "big") + bytes.fromhex("002100000064") + bytes(length - 8)


@pytest.mark.parametrize("profile", [0, 0x0730, 0x0738, 0x073B, 0x0740, 65535])
def test_unknown_clock_profiles_fail_closed(profile):
    with pytest.raises(core.NetaudioCoreError):
        core.build_command({"command": "refresh_clock_status", "host_mac": "020000000001", "control_profile": profile})


def test_clock_port_changes_share_one_record_and_use_signed_intervals():
    packet = core.build_command(
        {
            "command": "clock_control",
            "host_mac": "020000000001",
            "control": {
                "control_profile": 0x073A,
                "advanced": True,
                "follower_only": True,
                "ptpv1_enabled": False,
                "ptpv2_enabled": True,
                "ptpv2_domain": 7,
                "multicast_dscp": 46,
                "ports": [
                    {"port_id": 2, "ttl": 64, "sync_interval": -3, "announce_interval": 1, "follower_only": False},
                    {"port_id": 1, "delay_request_interval": -7, "peer_delay_interval": -128, "delay_mechanism": 2},
                ],
            },
        }
    )
    record = packet[24:]
    assert len(record) == 92
    assert record[32:36] == bytes.fromhex("001c0014")
    assert record[40:44] == bytes.fromhex("00000048")
    assert record[54] == 7 and record[64] == 46
    assert record[58:64] == bytes.fromhex("00020044000c")
    assert record[68:80] == bytes.fromhex("00710001000000f980020000")
    assert record[80:92] == bytes.fromhex("000f000240fd010000000001")


@pytest.mark.parametrize(
    "changes",
    [
        {"ports": [{"port_id": 0, "ttl": 1}]},
        {"ports": [{"port_id": 65, "ttl": 1}]},
        {"ports": [{"port_id": 1, "ttl": 1}, {"port_id": 1, "ttl": 2}]},
        {"ports": [{"port_id": 1, "sync_interval": -129}]},
        {"multicast_dscp": 64},
        {"subdomain": [65] * 16},
        {"ptpv1_enabled": True, "advanced": False},
        {"control_profile": 0x0734, "ptpv2_domain": 0},
    ],
)
def test_invalid_clock_writes_fail_before_transport(changes):
    with pytest.raises(core.NetaudioCoreError):
        core.build_command(
            {
                "command": "clock_control",
                "host_mac": "020000000001",
                "control": {
                    "control_profile": 0x073A,
                    "advanced": True,
                    **changes,
                },
            }
        )


def test_source_normalization_agrees_with_browser_at_every_edge():
    raw_values = [0, 1, 120, 121, 122, 193, 252, 253, 254, 255]
    scales = core.metering_scale()
    expected = []

    for source in ("detailed", "signal_presence"):
        for raw in raw_values:
            finite = 1 <= raw <= (253 if source == "detailed" else 252)
            dbfs = -(raw - (1 if source == "detailed" else 0)) / 2 if finite else None
            if raw == 0:
                state = "clipping"
            elif finite:
                state = "signal_present" if raw <= (121 if source == "detailed" else 120) else "below_threshold"
            elif source == "signal_presence":
                state = "mute_or_floor"
            else:
                state = "muted" if raw == 254 else "framing_marker"

            value = scales[source][raw]
            assert value["raw"] == raw and value["source"] == source
            assert value["dbfs"] == dbfs and value["state"] == state
            assert metering_value_dbfs(raw, source) == dbfs
            assert classify_signal_presence(raw, source) == state
            expected.append([source, raw, dbfs, state])

    module = (Path(__file__).parents[1] / "packages/netaudio/src/netaudio/daemon/http/webapp/format.js").as_uri()
    script = f"""
        import {{setMeteringScale, meteringDecibelsFullScale, meteringSignalPresence}} from {json.dumps(module)};
        let input = ''; for await (const chunk of process.stdin) input += chunk;
        const {{scales, expected}} = JSON.parse(input); setMeteringScale(scales);
        console.log(JSON.stringify(expected.map(([source, raw]) =>
          [source, raw, meteringDecibelsFullScale(raw, source), meteringSignalPresence(raw, source)])));
    """
    result = subprocess.run(
        ["node", "--input-type=module", "-e", script],
        input=json.dumps({"scales": scales, "expected": expected}),
        text=True,
        capture_output=True,
        check=True,
    )
    assert json.loads(result.stdout) == expected
