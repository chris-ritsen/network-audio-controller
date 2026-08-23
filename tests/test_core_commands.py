import ctypes
import json
from pathlib import Path

import pytest

from netaudio import core
from tests.protocol_test_fixtures import load_protocol_packet

if not core.available():
    pytest.skip("netaudio-core library not available", allow_module_level=True)

FIXTURES_DIR = Path(__file__).parent / "fixtures"
GOLDEN = json.loads((FIXTURES_DIR / "core_commands_golden.json").read_text())


@pytest.mark.parametrize("case_id", list(GOLDEN))
def test_build_command_matches_golden(case_id):
    entry = GOLDEN[case_id]
    assert core.build_command(entry["spec"]) == bytes.fromhex(entry["hex"])


def test_reboot_without_host_mac_is_rejected():
    with pytest.raises(core.NetaudioCoreError) as exc_info:
        core.build_command({"command": "reboot"})
    assert exc_info.value.status == 14


def test_reboot_zero_sequence_is_rejected():
    with pytest.raises(core.NetaudioCoreError) as exc_info:
        core.build_command(
            {
                "command": "reboot",
                "host_mac": "001dc1502368",
                "sequence": 0,
            }
        )
    assert exc_info.value.status == 31


@pytest.mark.parametrize(
    "specification",
    [
        {
            "command": "set_interface_dhcp",
            "host_mac": "001dc1502368",
            "sequence": 0,
        },
        {
            "command": "set_interface_static",
            "ip": "192.168.1.36",
            "netmask": "255.255.255.0",
            "dns": "8.8.8.8",
            "gateway": "192.168.1.1",
            "host_mac": "001dc1502368",
            "sequence": 0,
        },
    ],
)
def test_interface_write_zero_sequence_is_rejected(specification):
    with pytest.raises(core.NetaudioCoreError) as exc_info:
        core.build_command(specification)
    assert exc_info.value.status == 31


def _controller_interface_configuration_member(member):
    return load_protocol_packet("interface_configuration", member)


def test_interface_dhcp_matches_controller_capture():
    captured = _controller_interface_configuration_member("protocol_FFFF_message_0013_id_4672878.bin")
    built = core.build_command(
        {
            "command": "set_interface_dhcp",
            "host_mac": captured[8:14].hex(),
            "sequence": int.from_bytes(captured[4:6], "big"),
        }
    )
    assert built == captured


def test_interface_static_matches_controller_capture():
    captured = _controller_interface_configuration_member("protocol_FFFF_message_0013_id_4684352.bin")
    built = core.build_command(
        {
            "command": "set_interface_static",
            "ip": "192.168.1.36",
            "netmask": "255.255.255.0",
            "dns": "8.8.8.8",
            "gateway": "192.168.1.1",
            "host_mac": captured[8:14].hex(),
            "sequence": int.from_bytes(captured[4:6], "big"),
        }
    )
    assert built == captured


def _controller_preset_member(member):
    return load_protocol_packet("preset", member)


def _controller_subscription_member(member):
    return load_protocol_packet("subscription", member)


def _controller_transmit_channel_rename_member(member):
    return load_protocol_packet("transmitter_channel_rename", member)


def _controller_device_rename_member(member):
    return load_protocol_packet("device_rename", member)


def _controller_reboot_member(member):
    return load_protocol_packet("reboot", member)


def _receive_channel_name_page_spec(captured):
    records = []
    for index in range(32):
        record_offset = 12 + index * 4
        rx_channel = int.from_bytes(captured[record_offset : record_offset + 2], "big")
        name_offset = int.from_bytes(captured[record_offset + 2 : record_offset + 4], "big")
        name_end = captured.index(0, name_offset)
        records.append(
            {
                "rx_channel": rx_channel,
                "name": captured[name_offset:name_end].decode("ascii"),
            }
        )
    return {
        "command": "receive_channel_name_page_2729",
        "records": records,
        "transaction_id": int.from_bytes(captured[4:6], "big"),
    }


@pytest.mark.parametrize("packet_id", [962844, 962875, 962888, 962904])
def test_receive_channel_name_page_2729_matches_controller_capture(packet_id):
    captured = _controller_preset_member(f"protocol_2729_opcode_3001_id_{packet_id}.bin")
    built = core.build_command(_receive_channel_name_page_spec(captured))
    referenced_end = max(
        captured.index(0, int.from_bytes(captured[offset + 2 : offset + 4], "big")) + 1 for offset in range(12, 140, 4)
    )
    assert built[:2] == captured[:2]
    assert built[4:] == captured[4:referenced_end]
    assert captured[referenced_end:] in (b"", f"{int.from_bytes(captured[136:138], 'big') + 1}\0".encode())


def test_subscription_page_2729_matches_controller_assignment_capture():
    captured = _controller_preset_member("protocol_2729_opcode_3010_id_947064.bin")
    records = [
        {
            "action": "set",
            "rx_channel": channel,
            "tx_channel": "128-renamed" if channel == 128 else str(channel),
            "tx_device": ".",
        }
        for channel in range(97, 129)
    ]
    built = core.build_command(
        {
            "command": "subscription_page_2729",
            "records": records,
            "transaction_id": int.from_bytes(captured[4:6], "big"),
        }
    )
    assert built == captured


def test_subscription_page_2729_excludes_unreferenced_controller_tail():
    captured = _controller_preset_member("protocol_2729_opcode_3010_id_947041.bin")
    built = core.build_command(
        {
            "command": "subscription_page_2729",
            "records": [
                {
                    "action": "set",
                    "rx_channel": channel,
                    "tx_channel": str(channel),
                    "tx_device": ".",
                }
                for channel in range(65, 97)
            ],
            "transaction_id": int.from_bytes(captured[4:6], "big"),
        }
    )
    assert built[:2] == captured[:2]
    assert built[4:] == captured[4 : len(built)]
    assert captured[len(built) :] == b"97\0"


def test_subscription_page_2729_matches_controller_clear_capture():
    captured = _controller_preset_member("protocol_2729_opcode_3010_id_964736.bin")
    built = core.build_command(
        {
            "command": "subscription_page_2729",
            "records": [
                {
                    "action": "clear",
                    "rx_channel": channel,
                }
                for channel in range(1, 33)
            ],
            "transaction_id": 0x0768,
        }
    )
    assert built == captured


def test_subscription_page_2729_matches_controller_single_assignment_capture():
    captured = _controller_subscription_member("protocol_2729_opcode_3010_id_29606.bin")
    built = core.build_command(
        {
            "command": "subscription_page_2729",
            "records": [
                {
                    "action": "set",
                    "rx_channel": 1,
                    "tx_channel": "bluetooth:left",
                    "tx_device": "avio-bt-1",
                }
            ],
            "transaction_id": 0x29AB,
        }
    )
    assert built == captured


def test_subscription_page_2729_matches_controller_single_removal_capture():
    captured = _controller_subscription_member("protocol_2729_opcode_3010_id_29441.bin")
    built = core.build_command(
        {
            "command": "subscription_page_2729",
            "records": [{"action": "clear", "rx_channel": 1}],
            "transaction_id": 0x297A,
        }
    )
    assert built == captured


def test_set_transmit_channel_name_matches_controller_capture():
    captured = _controller_transmit_channel_rename_member("protocol_2729_opcode_2013_id_1.bin")
    built = core.build_command(
        {
            "command": "set_channel_name",
            "channel_type": "tx",
            "channel_number": 1,
            "name": "tett",
            "transaction_id": 0x49A4,
        }
    )
    assert built == captured


def test_set_device_name_matches_controller_capture():
    captured = _controller_device_rename_member("protocol_2809_opcode_1001_id_27507.bin")
    built = core.build_command(
        {
            "command": "set_name",
            "name": "avio-bt-11",
            "transaction_id": 0x261B,
        }
    )
    assert built == captured


def test_reboot_matches_controller_capture():
    captured = _controller_reboot_member("protocol_FFFF_message_0090_id_27657.bin")
    built = core.build_command(
        {
            "command": "reboot",
            "host_mac": captured[8:14].hex(),
            "sequence": int.from_bytes(captured[4:6], "big"),
        }
    )
    assert built == captured


class TestSpecErrors:
    def _status(self, spec):
        with pytest.raises(core.NetaudioCoreError) as exc_info:
            core.build_command(spec)
        return exc_info.value.status

    def test_invalid_json(self):
        lib = core.require()
        out = (ctypes.c_uint8 * 64)()
        length = ctypes.c_size_t(0)
        status = lib.netaudio_build_command(b"not even valid", out, 64, ctypes.byref(length))
        assert status != 0

    def test_unknown_command(self):
        assert self._status({"command": "frobnicate"}) == 13

    def test_invalid_mac(self):
        assert self._status({"command": "make_model", "mac": "xyz"}) == 14

    def test_invalid_name_propagates(self):
        assert self._status({"command": "set_name", "name": "-bad-"}) == 4

    def test_subscription_count_zero(self):
        assert self._status({"command": "add_subscriptions", "subscriptions": []}) == 12

    @pytest.mark.parametrize("latency", [-1, 1e99])
    def test_invalid_latency(self, latency):
        assert self._status({"command": "set_latency", "latency": latency}) == 25

    def test_zero_sample_rate_is_rejected(self):
        assert self._status({"command": "set_sample_rate", "sample_rate": 0}) == 26

    @pytest.mark.parametrize("sample_rate", [32_000, 123_456, 0x01000000])
    def test_nonzero_sample_rate_preserves_full_advertised_value(self, sample_rate):
        packet = core.build_command({"command": "set_sample_rate", "sample_rate": sample_rate})
        assert packet[36:40] == sample_rate.to_bytes(4, "big")

    def test_sample_rate_write_accepts_a_changing_transaction_identifier(self):
        packet = core.build_command({"command": "set_sample_rate", "sample_rate": 48_000, "sequence": 0x18B1})
        assert packet[4:6] == bytes.fromhex("18b1")

    def test_sample_rate_write_rejects_zero_transaction_identifier(self):
        assert self._status({"command": "set_sample_rate", "sample_rate": 48_000, "sequence": 0}) == 31

    def test_zero_encoding_is_rejected(self):
        assert self._status({"command": "set_encoding", "encoding": 0}) == 27

    @pytest.mark.parametrize("encoding", [16, 20, 24, 32, 256, 0xFFFFFFFF])
    def test_nonzero_encoding_preserves_advertised_value(self, encoding):
        packet = core.build_command({"command": "set_encoding", "encoding": encoding})
        assert packet[36:40] == encoding.to_bytes(4, "big")

    def test_channel_zero_is_rejected(self):
        assert (
            self._status(
                {
                    "command": "set_gain_level",
                    "channel_number": 0,
                    "gain_level": 1,
                    "device_type": "input",
                    "host_mac": "001122334455",
                }
            )
            == 24
        )
        assert (
            self._status(
                {
                    "command": "create_tx_flow",
                    "flow_protocol_id": 0x2729,
                    "flow_slot": 1,
                    "channels": [],
                }
            )
            == 24
        )

    @pytest.mark.parametrize("gain_level", [0, 6, 255])
    def test_invalid_gain_level(self, gain_level):
        assert (
            self._status(
                {
                    "command": "set_gain_level",
                    "channel_number": 1,
                    "gain_level": gain_level,
                    "device_type": "output",
                    "host_mac": "001122334455",
                }
            )
            == 28
        )

    @pytest.mark.parametrize("field", ["tx_channel", "tx_device"])
    def test_subscription_strings_reject_embedded_nul(self, field):
        subscription = {"rx_channel": 1, "tx_channel": "tx-a", "tx_device": "dev-a"}
        subscription[field] = "bad\0value"
        assert (
            self._status(
                {
                    "command": "add_subscriptions",
                    "subscriptions": [subscription],
                }
            )
            == 5
        )

    def test_volume_name_overflow_is_rejected_without_constructing_a_packet(self):
        assert (
            self._status(
                {
                    "command": "volume_start",
                    "device_name": "a" * 65_521,
                    "mac": "001122334455",
                    "port": 9999,
                }
            )
            == 3
        )

    @pytest.mark.parametrize("slot", [0, 33, 65535])
    @pytest.mark.parametrize("command", ["create_tx_flow", "delete_tx_flow"])
    def test_invalid_flow_slot(self, command, slot):
        spec = {
            "command": command,
            "flow_protocol_id": 0x2729,
            "flow_slot": slot,
        }
        if command == "create_tx_flow":
            spec["channels"] = [1]
        assert self._status(spec) == 29

    @pytest.mark.parametrize("protocol", [0, 0x2728, 0x2800, 0x2808, 0xFFFF])
    @pytest.mark.parametrize("command", ["query_tx_flows", "create_tx_flow", "delete_tx_flow"])
    def test_invalid_flow_protocol(self, command, protocol):
        spec = {
            "command": command,
            "flow_protocol_id": protocol,
        }
        if command == "create_tx_flow":
            spec.update(flow_slot=1, channels=[1])
        elif command == "delete_tx_flow":
            spec["flow_slot"] = 1
        assert self._status(spec) == 30
