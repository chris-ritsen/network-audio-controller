import ctypes
import json
from pathlib import Path

import pytest

from netaudio import core

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
