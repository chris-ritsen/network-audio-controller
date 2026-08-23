from __future__ import annotations

import json
import sys
import time
from unittest.mock import AsyncMock
from types import SimpleNamespace

from typer.testing import CliRunner

from netaudio.cli import OutputFormat, app, state
from netaudio.commands.device import meter_callback
from netaudio.dante.channel import DanteChannel
from netaudio.dante.device import DanteDevice


runner = CliRunner()


def _device() -> DanteDevice:
    device = DanteDevice("a32.local.")
    device.name = "A32"
    channel = DanteChannel()
    channel.number = 17
    channel.name = "tx-17"
    channel.friendly_name = "Shelford"
    device.tx_channels[17] = channel
    return device


def _sample(source: str = "signal_presence") -> dict:
    return {
        "tx": {17: 0x00},
        "rx": {},
        "tx_signal_presence": {17: "clipping"},
        "rx_signal_presence": {},
        "metering_source": source,
        "wall_time": time.time(),
        "source_ip": "192.0.2.17",
        "source_port": 8700,
    }


def _patch_daemon(monkeypatch, *, source: str = "signal_presence"):
    device = _device()
    get_devices = AsyncMock(return_value={device.server_name: device})
    get_cache = AsyncMock(return_value={device.server_name: _sample(source)})
    start = AsyncMock(return_value=True)
    stop = AsyncMock(return_value=True)
    monkeypatch.setattr("netaudio.daemon.client.get_devices_from_daemon", get_devices)
    monkeypatch.setattr("netaudio.daemon.client.meter_cache_from_daemon", get_cache)
    monkeypatch.setattr("netaudio.daemon.client.meter_start_on_daemon", start)
    monkeypatch.setattr("netaudio.daemon.client.meter_stop_on_daemon", stop)
    return device, get_cache, start, stop


def test_passive_json_snapshot_reads_cache_without_start_or_stop(monkeypatch):
    device, get_cache, start, stop = _patch_daemon(monkeypatch)

    result = runner.invoke(app, ["--json", "meter", "--timeout", "0"])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload[device.server_name]["tx"]["17"] == {
        "name": "Shelford",
        "level": 0x00,
        "signal_presence": "clipping",
    }
    assert payload[device.server_name]["metering_source"] == "signal_presence"
    get_cache.assert_awaited_once_with()
    start.assert_not_awaited()
    stop.assert_not_awaited()


def test_detailed_snapshot_uses_unique_balanced_reference(monkeypatch):
    device, _get_cache, start, stop = _patch_daemon(monkeypatch, source="detailed")

    result = runner.invoke(app, ["--no-color", "meter", "--snapshot", "--detailed", "--timeout", "0"])

    assert result.exit_code == 0, result.output
    assert "[detailed]" in result.output
    start.assert_awaited_once()
    stop.assert_awaited_once()
    start_server_name, start_client_id = start.await_args.args
    stop_server_name, stop_client_id = stop.await_args.args
    assert start_server_name == stop_server_name == device.server_name
    assert start_client_id == stop_client_id
    assert start_client_id.startswith("meter_snapshot:")


def test_detailed_snapshot_ignores_passive_cache_until_detailed_arrives(monkeypatch):
    device, get_cache, start, stop = _patch_daemon(monkeypatch)
    get_cache.side_effect = [
        {device.server_name: _sample("signal_presence")},
        {device.server_name: _sample("detailed")},
    ]

    result = runner.invoke(app, ["--json", "meter", "--detailed", "--timeout", "0.2"])

    assert result.exit_code == 0, result.output
    assert json.loads(result.output)[device.server_name]["metering_source"] == "detailed"
    assert get_cache.await_count == 2
    start.assert_awaited_once()
    stop.assert_awaited_once()


def test_json_snapshot_applies_direction_and_channel_filters(monkeypatch):
    device, get_cache, start, stop = _patch_daemon(monkeypatch)
    rx_channel = DanteChannel()
    rx_channel.number = 3
    rx_channel.name = "rx-3"
    rx_channel.friendly_name = "Return"
    device.rx_channels[3] = rx_channel
    sample = _sample()
    sample["rx"] = {3: 0x7B}
    sample["rx_signal_presence"] = {3: "signal_present"}
    get_cache.return_value = {device.server_name: sample}

    result = runner.invoke(app, ["--json", "meter", "--rx", "--channel", "3", "--timeout", "0"])

    assert result.exit_code == 0, result.output
    levels = json.loads(result.output)[device.server_name]
    assert levels["tx"] == {}
    assert levels["rx"] == {
        "3": {
            "name": "Return",
            "level": 0x7B,
            "signal_presence": "signal_present",
        }
    }
    start.assert_not_awaited()
    stop.assert_not_awaited()


def test_interactive_meter_rejects_non_tty_without_starting_metering(monkeypatch):
    _device_value, _get_cache, start, stop = _patch_daemon(monkeypatch)
    run_tui = AsyncMock()
    monkeypatch.setattr("netaudio.commands.meter_tui.run_meter_tui", run_tui)

    result = runner.invoke(app, ["meter"])

    assert result.exit_code == 1
    assert "Interactive meter requires a TTY" in result.output
    run_tui.assert_not_awaited()
    start.assert_not_awaited()
    stop.assert_not_awaited()


def test_interactive_meter_forwards_simple_view_options(monkeypatch):
    device, get_cache, start, stop = _patch_daemon(monkeypatch)
    run_tui = AsyncMock()
    monkeypatch.setattr("netaudio.commands.meter_tui.run_meter_tui", run_tui)
    monkeypatch.setattr(sys.stdin, "isatty", lambda: True)
    monkeypatch.setattr(sys.stdout, "isatty", lambda: True)
    for field_name in ("names", "hosts", "server_names", "macs"):
        monkeypatch.setattr(state, field_name, [])
    monkeypatch.setattr(state, "output_format", OutputFormat.plain)
    monkeypatch.setattr(state, "no_color", True)

    meter_callback(
        SimpleNamespace(invoked_subcommand=None),
        timeout=0,
        tx=True,
        rx=False,
        channel=["Shel*"],
        snapshot=False,
        detailed=True,
    )

    run_tui.assert_awaited_once_with(
        {device.server_name: device},
        show_tx=True,
        show_rx=False,
        channel_patterns=["Shel*"],
        detailed=True,
        no_color=True,
    )
    get_cache.assert_not_awaited()
    start.assert_not_awaited()
    stop.assert_not_awaited()


def test_passive_cache_timeout_never_starts_or_stops(monkeypatch):
    _device_value, get_cache, start, stop = _patch_daemon(monkeypatch)
    get_cache.return_value = {}

    result = runner.invoke(app, ["--json", "meter", "--timeout", "0"])

    assert result.exit_code == 1
    assert "No fresh metering data" in result.output
    get_cache.assert_awaited_once()
    start.assert_not_awaited()
    stop.assert_not_awaited()
