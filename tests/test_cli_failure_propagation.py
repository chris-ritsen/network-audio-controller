import json
import logging
from unittest.mock import AsyncMock, patch

import pytest
from typer.testing import CliRunner

from netaudio.cli import app
from netaudio.core.binding import NetaudioCoreError
from netaudio.dante.application import CapabilityProbeTimeout, DanteApplication
from netaudio.dante.device import DanteDevice

PROBES = (
    "get_aes67_configured",
    "get_latency_settings",
    "probe_clocking_status",
    "probe_encoding_status",
    "probe_gain_status",
    "probe_lock_status",
    "probe_sample_rate_status",
    "query_modern_arc_receiver_channel_status",
    "query_modern_arc_receiver_flow_status",
    "query_modern_arc_transmitter_channel_status",
    "query_modern_arc_transmitter_flow_status",
)


def _device(inventory_error=None):
    device = DanteDevice(server_name="receiver.local.")
    device.name = "receiver"
    device.ipv4 = "192.0.2.10"
    device.online = True
    device.last_seen = 1789128000.0
    device.rx_count = 16
    device.tx_count = 16
    device.services = {
        "receiver._netaudio-arc._udp.local.": {"type": "_netaudio-arc._udp.local.", "port": 4440, "ipv4": "192.0.2.10"}
    }
    if inventory_error is None:
        device.populate_from_core = AsyncMock(return_value=True)
    else:
        device.populate_from_core = AsyncMock(side_effect=inventory_error)
    return device


@pytest.fixture(autouse=True)
def _restore_cli_state():
    from netaudio.cli import state

    snapshot = dict(vars(state))
    yield
    vars(state).clear()
    vars(state).update(snapshot)


def _invoke(arguments, devices):
    runner = CliRunner()
    with (
        patch("netaudio.cli_support.execution.get_devices_from_daemon", AsyncMock(return_value=devices)),
        patch("netaudio.daemon.client.daemon_is_accessible", return_value=False),
        patch.object(DanteApplication, "startup", AsyncMock()),
        patch.object(DanteApplication, "shutdown", AsyncMock()),
    ):
        with patch.multiple(
            DanteApplication,
            **{name: AsyncMock(side_effect=CapabilityProbeTimeout("read timed out")) for name in PROBES},
        ):
            logging.getLogger("netaudio").handlers.clear()
            return runner.invoke(app, ["--no-color", *arguments])


@pytest.mark.parametrize("arguments", [["subscription", "list"], ["channel", "list"]])
def test_inventory_read_failure_fails_the_listing(arguments):
    device = _device(NetaudioCoreError(10, "receiver inventory"))

    result = _invoke(arguments, {"receiver.local.": device})

    assert result.exit_code == 1
    assert "could not read" in result.output
    assert "receiver.local. (192.0.2.10)" in result.output


def test_inventory_read_failure_does_not_mark_the_device_offline():
    device = _device(NetaudioCoreError(10, "receiver inventory"))

    result = _invoke(["-j", "status"], {"receiver.local.": device})

    assert result.exit_code == 0
    assert device.online is True
    assert json.loads(result.stdout)["dante"]["receiver.local."]["online"] is True


def test_empty_subscription_list_is_json_when_json_is_requested():
    result = _invoke(["-j", "subscription", "list"], {"receiver.local.": _device()})

    assert result.exit_code == 0
    assert json.loads(result.stdout) == []


def test_empty_subscription_list_is_a_sentence_in_plain_output():
    result = _invoke(["subscription", "list"], {})

    assert result.exit_code == 0
    assert result.stdout.strip() == "No active subscriptions."


def test_gain_query_timeout_is_an_error_not_unsupported():
    result = _invoke(["-n", "receiver", "channel", "gain", "rx:1"], {"receiver.local.": _device()})

    assert result.exit_code == 1
    assert "could not read gain status" in result.output
    assert "unsupported" not in result.stdout
