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


def test_reboot_without_host_mac_builds():
    assert len(core.build_command({"command": "reboot"})) > 0


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
