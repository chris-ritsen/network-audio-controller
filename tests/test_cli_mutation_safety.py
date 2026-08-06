from contextlib import asynccontextmanager
import json
from types import SimpleNamespace

import pytest
from typer.testing import CliRunner

from netaudio import _common
from netaudio.commands import flow as flow_commands
from netaudio.commands import preset as preset_commands
from netaudio.commands import subscription as subscription_commands
from netaudio.dante import flows


runner = CliRunner()


@pytest.fixture(autouse=True)
def reset_cli_state(monkeypatch):
    from netaudio.cli import state

    original = (
        list(state.names),
        list(state.hosts),
        list(state.server_names),
        list(state.macs),
        state.output_format,
    )
    state.names = []
    state.hosts = []
    state.server_names = []
    state.macs = []
    from netaudio.cli import OutputFormat

    state.output_format = OutputFormat.plain

    async def unavailable_preset_readback():
        raise RuntimeError("synthetic readback service unavailable")

    monkeypatch.setattr(
        preset_commands,
        "_start_preset_readback_application",
        unavailable_preset_readback,
    )
    try:
        yield state
    finally:
        state.names, state.hosts, state.server_names, state.macs, state.output_format = original


def _channel(number, name):
    return SimpleNamespace(
        number=number,
        name=name,
        friendly_name=None,
    )


def _subscription(rx_name, tx_name, tx_device):
    return SimpleNamespace(
        rx_channel_name=rx_name,
        tx_channel_name=tx_name,
        tx_device_name=tx_device,
    )


def _subscription_devices(refresh_rx=None):
    tx = SimpleNamespace(
        name="TX",
        server_name="tx.local.",
        ipv4="192.0.2.10",
        tx_channels={1: _channel(1, "Tx1"), 2: _channel(2, "Tx2")},
        rx_channels={},
        subscriptions=[],
        services={},
    )
    rx = SimpleNamespace(
        name="RX",
        server_name="rx.local.",
        ipv4="192.0.2.20",
        tx_channels={},
        rx_channels={1: _channel(1, "Rx1"), 2: _channel(2, "Rx2")},
        subscriptions=[],
        services={},
    )

    async def get_rx_channels():
        if refresh_rx is not None:
            refresh_rx(rx)

    rx.get_rx_channels = get_rx_channels
    return {"tx.local.": tx, "rx.local.": rx}, tx, rx


def _install_subscription_context(monkeypatch, devices, send):
    @asynccontextmanager
    async def command_context():
        yield devices, send

    monkeypatch.setattr(subscription_commands, "_command_context", command_context)


def test_subscription_rejects_negative_ranges_before_discovery(monkeypatch):
    entered = False

    @asynccontextmanager
    async def command_context():
        nonlocal entered
        entered = True
        yield {}, None

    monkeypatch.setattr(subscription_commands, "_command_context", command_context)

    result = runner.invoke(
        subscription_commands.app,
        ["add", "--tx", "TX", "--rx", "RX", "--offset-tx", "-1"],
    )

    assert result.exit_code != 0
    assert not entered


def test_subscription_rejects_count_beyond_available_pairs(monkeypatch):
    devices, _, _ = _subscription_devices()

    async def send(*_args, **_kwargs):
        raise AssertionError("validation should finish before sending")

    _install_subscription_context(monkeypatch, devices, send)

    result = runner.invoke(
        subscription_commands.app,
        ["add", "--tx", "TX", "--rx", "RX", "--count", "3"],
    )

    assert result.exit_code == 1
    assert "exceeds the 2 channel pairs" in result.output


def test_subscription_bulk_send_failure_returns_nonzero(monkeypatch):
    devices, _, _ = _subscription_devices()

    async def send(*_args, **_kwargs):
        raise OSError("synthetic send failure")

    _install_subscription_context(monkeypatch, devices, send)

    result = runner.invoke(
        subscription_commands.app,
        ["add", "--tx", "TX", "--rx", "RX"],
    )

    assert result.exit_code == 1
    assert "FAILED" in result.output
    assert "synthetic send failure" in result.output


def test_subscription_bulk_skips_already_satisfied_pairs(monkeypatch):
    devices, _, rx = _subscription_devices()
    rx.subscriptions = [_subscription("Rx1", "Tx1", "TX")]
    sends = []

    async def send(*args, **kwargs):
        sends.append((args, kwargs))

    _install_subscription_context(monkeypatch, devices, send)

    result = runner.invoke(
        subscription_commands.app,
        ["add", "--tx", "TX", "--rx", "RX", "--count", "1"],
    )

    assert result.exit_code == 0
    assert sends == []
    assert "UNCHANGED Rx1@RX <- Tx1@TX (already subscribed)" in result.output
    assert "MODIFIED" not in result.output


def test_subscription_bulk_reports_unchanged_and_modified_exactly(monkeypatch):
    refresh_count = 0

    def refresh_rx(device):
        nonlocal refresh_count
        refresh_count += 1
        if refresh_count >= 2:
            device.subscriptions = [
                _subscription("Rx1", "Tx1", "TX"),
                _subscription("Rx2", "Tx2", "TX"),
            ]

    devices, _, rx = _subscription_devices(refresh_rx=refresh_rx)
    rx.subscriptions = [_subscription("Rx1", "Tx1", "TX")]
    sends = []
    encoded = []

    def build_subscriptions(_self, subscriptions):
        encoded.append(subscriptions)
        return b"packet", None

    monkeypatch.setattr(
        subscription_commands.DanteDeviceCommands,
        "command_add_subscriptions",
        build_subscriptions,
    )

    async def send(*args, **kwargs):
        sends.append((args, kwargs))

    _install_subscription_context(monkeypatch, devices, send)

    result = runner.invoke(
        subscription_commands.app,
        ["add", "--tx", "TX", "--rx", "RX"],
    )

    assert result.exit_code == 0
    assert len(sends) == 1
    assert encoded == [[(2, "Tx2", "TX")]]
    assert "UNCHANGED Rx1@RX <- Tx1@TX" in result.output
    assert "MODIFIED Rx2@RX <- Tx2@TX (verified)" in result.output


def test_subscription_bulk_reports_partial_readback_per_channel(monkeypatch):
    refresh_count = 0

    def refresh_rx(device):
        nonlocal refresh_count
        refresh_count += 1
        if refresh_count >= 2:
            device.subscriptions = [_subscription("Rx1", "Tx1", "TX")]

    devices, _, _ = _subscription_devices(refresh_rx=refresh_rx)

    async def send(*_args, **_kwargs):
        return None

    _install_subscription_context(monkeypatch, devices, send)

    result = runner.invoke(
        subscription_commands.app,
        ["add", "--tx", "TX", "--rx", "RX"],
    )

    assert result.exit_code == 1
    assert "MODIFIED Rx1@RX <- Tx1@TX (verified)" in result.output
    assert "FAILED Rx2@RX <- Tx2@TX" in result.output
    assert "fresh readback was None" in result.output


def test_subscription_add_requires_fresh_readback(monkeypatch):
    devices, _, _ = _subscription_devices()
    sends = []

    async def send(*args, **kwargs):
        sends.append((args, kwargs))

    _install_subscription_context(monkeypatch, devices, send)

    result = runner.invoke(
        subscription_commands.app,
        ["add", "--tx", "Tx1@TX", "--rx", "Rx1@RX"],
    )

    assert result.exit_code == 1
    assert sends[0][1]["expect_response"] is False
    assert "fresh readback reports" in result.output
    assert "(verified)" not in result.output


def test_subscription_readback_uses_channel_identity_when_labels_collide():
    device = SimpleNamespace(
        rx_channels={1: _channel(1, "Duplicate"), 2: _channel(2, "Duplicate")},
        subscriptions=[
            _subscription("Duplicate", "Tx1", "TX"),
            _subscription("Duplicate", "Tx2", "TX"),
        ],
    )
    subscription_commands._index_fresh_subscriptions(device)

    assert subscription_commands._subscription_signature(device, 2) == ("Tx2", "TX")


def test_subscription_remove_all_uses_global_filters_and_verifies(monkeypatch, reset_cli_state):
    removal_requested = False

    def refresh_rx(device):
        if removal_requested:
            device.subscriptions = []

    devices, _, rx = _subscription_devices(refresh_rx=refresh_rx)
    rx.subscriptions = [_subscription("Rx1", "Tx1", "TX")]
    reset_cli_state.names = ["RX"]

    async def send(*_args, **kwargs):
        nonlocal removal_requested
        assert kwargs["expect_response"] is False
        removal_requested = True

    _install_subscription_context(monkeypatch, devices, send)

    result = runner.invoke(subscription_commands.app, ["remove", "--all"])

    assert result.exit_code == 0
    assert removal_requested
    assert "Removed: Rx1@RX (verified)" in result.output


def test_subscription_remove_rejects_bare_device_spec(monkeypatch):
    devices, _, _ = _subscription_devices()
    sent = False

    async def send(*_args, **_kwargs):
        nonlocal sent
        sent = True

    _install_subscription_context(monkeypatch, devices, send)

    result = runner.invoke(
        subscription_commands.app,
        ["remove", "--rx", "RX"],
    )

    assert result.exit_code == 1
    assert "expected channel@device" in result.output
    assert not sent


def test_subscription_remove_refreshes_before_claiming_channel_is_subscribed(monkeypatch):
    def refresh_rx(device):
        device.subscriptions = []

    devices, _, rx = _subscription_devices(refresh_rx=refresh_rx)
    rx.subscriptions = [_subscription("Rx1", "Tx1", "TX")]
    sent = False

    async def send(*_args, **_kwargs):
        nonlocal sent
        sent = True

    _install_subscription_context(monkeypatch, devices, send)

    result = runner.invoke(
        subscription_commands.app,
        ["remove", "--rx", "Rx1@RX"],
    )

    assert result.exit_code == 1
    assert "is not subscribed" in result.output
    assert not sent


class _FakeApplication:
    def __init__(self):
        self.shutdown_calls = 0

    async def shutdown(self):
        self.shutdown_calls += 1


def _flow_device():
    return SimpleNamespace(
        name="Flow Device",
        ipv4="192.0.2.30",
        flow_protocol_id=0x2729,
        tx_channels={1: _channel(1, "Tx1"), 2: _channel(2, "Tx2")},
    )


def test_flow_create_rejects_malformed_channels_before_discovery(monkeypatch):
    calls = 0

    async def get_device(*_args):
        nonlocal calls
        calls += 1

    monkeypatch.setattr(flow_commands, "_get_device_and_app", get_device)

    result = runner.invoke(
        flow_commands.app,
        ["create", "device", "--slot", "17", "--channels", "1,"],
    )

    assert result.exit_code == 1
    assert "comma-separated list of integers" in result.output
    assert calls == 0


def test_empty_flow_list_preserves_structured_output(monkeypatch):
    from netaudio.cli import OutputFormat, state

    application = _FakeApplication()
    device = _flow_device()
    flow_inventory = {"max_flow_slots": 16, "flows": []}

    async def get_device(*_args):
        return application, device, 4440

    async def query(*_args):
        return flow_inventory

    monkeypatch.setattr(flow_commands, "_get_device_and_app", get_device)
    monkeypatch.setattr(flows, "query_tx_flow_inventory", query)
    state.output_format = OutputFormat.json

    result = runner.invoke(flow_commands.app, ["list", "device"])

    assert result.exit_code == 0
    assert json.loads(result.output) == flow_inventory
    assert application.shutdown_calls == 1


def test_flow_create_refuses_occupied_slot(monkeypatch):
    application = _FakeApplication()
    device = _flow_device()
    create_calls = 0

    async def get_device(*_args):
        return application, device, 4440

    async def query(*_args):
        return {"max_flow_slots": 32, "flows": [{"flow_number": 17, "flow_type": "multicast"}]}

    async def create(*_args):
        nonlocal create_calls
        create_calls += 1

    monkeypatch.setattr(flow_commands, "_get_device_and_app", get_device)
    monkeypatch.setattr(flows, "query_tx_flow_inventory", query)
    monkeypatch.setattr(flows, "create_tx_flow", create)

    result = runner.invoke(
        flow_commands.app,
        ["create", "device", "--slot", "17", "--channels", "1"],
    )

    assert result.exit_code == 1
    assert "already in use" in result.output
    assert create_calls == 0
    assert application.shutdown_calls == 1


def test_flow_create_confirms_success(monkeypatch):
    application = _FakeApplication()
    device = _flow_device()

    async def get_device(*_args):
        return application, device, 4440

    async def query(*_args):
        return {"max_flow_slots": 32, "flows": []}

    async def create(*_args):
        return 1

    monkeypatch.setattr(flow_commands, "_get_device_and_app", get_device)
    monkeypatch.setattr(flows, "query_tx_flow_inventory", query)
    monkeypatch.setattr(flows, "create_tx_flow", create)

    result = runner.invoke(
        flow_commands.app,
        ["create", "device", "--slot", "17", "--channels", "1,2"],
    )

    assert result.exit_code == 0
    assert "Created multicast TX flow" in result.output
    assert "device confirmed" in result.output


def test_flow_create_refuses_slot_above_device_capacity(monkeypatch):
    application = _FakeApplication()
    device = _flow_device()
    create_calls = 0

    async def get_device(*_args):
        return application, device, 4440

    async def query(*_args):
        return {"max_flow_slots": 16, "flows": []}

    async def create(*_args):
        nonlocal create_calls
        create_calls += 1

    monkeypatch.setattr(flow_commands, "_get_device_and_app", get_device)
    monkeypatch.setattr(flows, "query_tx_flow_inventory", query)
    monkeypatch.setattr(flows, "create_tx_flow", create)

    result = runner.invoke(
        flow_commands.app,
        ["create", "device", "--slot", "17", "--channels", "1"],
    )

    assert result.exit_code == 1
    assert "exceeds the device capacity of 16" in result.output
    assert create_calls == 0


def test_flow_delete_requires_confirmation_before_discovery(monkeypatch):
    calls = 0

    async def get_device(*_args):
        nonlocal calls
        calls += 1

    monkeypatch.setattr(flow_commands, "_get_device_and_app", get_device)

    result = runner.invoke(
        flow_commands.app,
        ["delete", "device", "--slot", "17"],
    )

    assert result.exit_code == 1
    assert "without --yes" in result.output
    assert calls == 0


def test_flow_delete_refuses_non_multicast_flow(monkeypatch):
    application = _FakeApplication()
    device = _flow_device()
    delete_calls = 0

    async def get_device(*_args):
        return application, device, 4440

    async def query(*_args):
        return {"max_flow_slots": 32, "flows": [{"flow_number": 17, "flow_type": "unicast"}]}

    async def delete(*_args):
        nonlocal delete_calls
        delete_calls += 1

    monkeypatch.setattr(flow_commands, "_get_device_and_app", get_device)
    monkeypatch.setattr(flows, "query_tx_flow_inventory", query)
    monkeypatch.setattr(flows, "delete_tx_flow", delete)

    result = runner.invoke(
        flow_commands.app,
        ["delete", "device", "--slot", "17", "--yes"],
    )

    assert result.exit_code == 1
    assert "is not multicast" in result.output
    assert delete_calls == 0


def _write_preset(path, devices):
    device_xml = []
    for device in devices:
        fields = [
            f"<friendly_name>{device['name']}</friendly_name>",
        ]
        if "sample_rate" in device:
            fields.append(f"<samplerate>{device['sample_rate']}</samplerate>")
        if "encoding" in device:
            fields.append(f"<encoding>{device['encoding']}</encoding>")
        if "latency_us" in device:
            fields.append(f"<unicast_latency>{device['latency_us']}</unicast_latency>")
        if "preferred" in device:
            fields.append(f'<preferred_master value="{"true" if device["preferred"] else "false"}" />')
        if "interface" in device:
            fields.append(device["interface"])
        device_xml.append(f"<device>{''.join(fields)}</device>")
    path.write_text(f'<?xml version="1.0"?><preset><name>test</name>{"".join(device_xml)}</preset>')


def _preset_device(
    name,
    sample_rate=48000,
    supported_sample_rates=None,
    encoding=24,
    supported_encodings=None,
    active_latency_ns=1_000_000,
):
    operations = SimpleNamespace()

    async def get_device_settings():
        return {"sample_rate": sample_rate, "active_latency_ns": active_latency_ns}

    operations.get_device_settings = get_device_settings
    return SimpleNamespace(
        name=name,
        server_name=f"{name.lower()}.local.",
        ipv4="192.0.2.40",
        services={},
        supported_sample_rates=supported_sample_rates,
        encoding=encoding,
        supported_encodings=supported_encodings if supported_encodings is not None else [16, 24, 32],
        interface_pending_config=None,
        operations=operations,
    )


def _install_preset_context(monkeypatch, devices, send):
    @asynccontextmanager
    async def command_context():
        yield devices, send

    monkeypatch.setattr(_common, "_command_context", command_context)


def test_preset_save_refuses_overwrite_before_discovery(monkeypatch, tmp_path):
    output = tmp_path / "existing.xml"
    output.write_text("original")
    entered = False

    @asynccontextmanager
    async def command_context():
        nonlocal entered
        entered = True
        yield {}, None

    monkeypatch.setattr(_common, "_command_context", command_context)

    result = runner.invoke(preset_commands.app, ["save", str(output)])

    assert result.exit_code == 1
    assert "refusing to overwrite existing file" in result.output
    assert output.read_text() == "original"
    assert not entered


def test_preset_force_save_failure_preserves_existing_file(monkeypatch, tmp_path):
    output = tmp_path / "existing.xml"
    output.write_text("original")
    devices = {"device.local.": SimpleNamespace(name="Device")}

    async def send(*_args, **_kwargs):
        return None

    _install_preset_context(monkeypatch, devices, send)
    monkeypatch.setattr(
        _common,
        "format_devices_xml",
        lambda *_args, **_kwargs: "replacement",
    )
    monkeypatch.setattr(
        preset_commands.os,
        "replace",
        lambda *_args: (_ for _ in ()).throw(OSError("synthetic replace failure")),
    )

    result = runner.invoke(
        preset_commands.app,
        ["save", str(output), "--force"],
    )

    assert result.exit_code == 1
    assert "synthetic replace failure" in result.output
    assert output.read_text() == "original"
    assert list(tmp_path.glob(".*.tmp")) == []


def test_preset_force_replaces_existing_file(monkeypatch, tmp_path):
    output = tmp_path / "existing.xml"
    output.write_text("original")
    devices = {"device.local.": SimpleNamespace(name="Device")}

    async def send(*_args, **_kwargs):
        return None

    _install_preset_context(monkeypatch, devices, send)
    monkeypatch.setattr(
        _common,
        "format_devices_xml",
        lambda *_args, **_kwargs: "replacement",
    )

    result = runner.invoke(
        preset_commands.app,
        ["save", str(output), "--force"],
    )

    assert result.exit_code == 0
    assert output.read_text() == "replacement"
    assert list(tmp_path.glob(".*.tmp")) == []


def test_preset_save_publishes_complete_file_atomically(monkeypatch, tmp_path):
    output = tmp_path / "new.xml"
    devices = {"device.local.": SimpleNamespace(name="Device")}

    async def send(*_args, **_kwargs):
        return None

    _install_preset_context(monkeypatch, devices, send)
    monkeypatch.setattr(
        _common,
        "format_devices_xml",
        lambda *_args, **_kwargs: "complete preset",
    )

    result = runner.invoke(preset_commands.app, ["save", str(output)])

    assert result.exit_code == 0
    assert output.read_text() == "complete preset"
    assert list(tmp_path.glob(".*.tmp")) == []


def test_preset_converts_dc_latency_microseconds_for_display(tmp_path):
    preset = tmp_path / "latency.xml"
    _write_preset(preset, [{"name": "Device", "latency_us": 150}])

    result = runner.invoke(preset_commands.app, ["load", str(preset), "--dry-run"])

    assert result.exit_code == 0
    assert "latency: 0.15 ms" in result.output
    assert "unsupported for load" not in result.output


def test_preset_rejects_duplicate_friendly_names(tmp_path):
    preset = tmp_path / "duplicates.xml"
    _write_preset(
        preset,
        [
            {"name": "Duplicate", "sample_rate": 48000},
            {"name": "Duplicate", "sample_rate": 96000},
        ],
    )

    result = runner.invoke(preset_commands.app, ["load", str(preset), "--dry-run"])

    assert result.exit_code == 1
    assert "duplicate preset device name" in result.output


def test_preset_rejects_invalid_preferred_master_value(tmp_path):
    preset = tmp_path / "invalid-preferred.xml"
    preset.write_text(
        '<?xml version="1.0"?><preset><name>test</name><device>'
        "<friendly_name>Device</friendly_name>"
        '<preferred_master value="definitely" />'
        "</device></preset>"
    )

    result = runner.invoke(preset_commands.app, ["load", str(preset), "--dry-run"])

    assert result.exit_code == 1
    assert "preferred_master value must be true or false" in result.output


def test_preset_marks_additional_interfaces_unsupported(monkeypatch, tmp_path):
    preset = tmp_path / "interfaces.xml"
    interfaces = (
        '<interface network="0"><ipv4_address mode="dynamic" /></interface>'
        '<interface network="1"><ipv4_address mode="dynamic" /></interface>'
    )
    _write_preset(preset, [{"name": "Device", "interface": interfaces}])
    devices = {"device.local.": _preset_device("Device")}
    sends = []

    async def send(*args, **kwargs):
        sends.append((args, kwargs))

    _install_preset_context(monkeypatch, devices, send)

    result = runner.invoke(preset_commands.app, ["load", str(preset)])

    assert result.exit_code == 1
    assert "unsupported fields: additional network interfaces" in result.output
    assert sends == []


def test_preset_preflights_all_matches_before_any_send(monkeypatch, tmp_path):
    preset = tmp_path / "partial.xml"
    _write_preset(
        preset,
        [
            {"name": "First", "sample_rate": 48000},
            {"name": "Second", "encoding": 24},
        ],
    )
    devices = {
        "first.local.": _preset_device("First"),
        "second.local.": _preset_device("Second", supported_encodings=[16]),
    }
    sends = []

    async def send(*args, **kwargs):
        sends.append((args, kwargs))

    _install_preset_context(monkeypatch, devices, send)

    result = runner.invoke(preset_commands.app, ["load", str(preset)])

    assert result.exit_code == 1
    assert "refused before sending any changes" in result.output
    assert "Second: device reports supported encodings [16]; 24 is not supported" in result.output
    assert sends == []


def test_preset_load_applies_and_verifies_encoding_and_latency(monkeypatch, tmp_path):
    preset = tmp_path / "audio-settings.xml"
    _write_preset(preset, [{"name": "Device", "encoding": 24, "latency_us": 150}])
    device = _preset_device("Device", encoding=24, active_latency_ns=150_000)
    sends = []

    async def send(*args, **kwargs):
        sends.append((args, kwargs))

    async def probe_encoding_status(_ipv4):
        return device.encoding, device.supported_encodings

    send.probe_encoding_status = probe_encoding_status
    _install_preset_context(monkeypatch, {"device.local.": device}, send)

    result = runner.invoke(preset_commands.app, ["load", str(preset)])

    assert result.exit_code == 0
    assert "encoding 24-bit (verified)" in result.output
    assert "latency 0.15 ms (verified)" in result.output
    assert len(sends) == 2


def test_preset_refuses_unmatched_devices_without_explicit_filter(monkeypatch, tmp_path):
    preset = tmp_path / "offline.xml"
    _write_preset(
        preset,
        [
            {"name": "Online", "sample_rate": 48000},
            {"name": "Offline", "sample_rate": 48000},
        ],
    )
    devices = {"online.local.": _preset_device("Online")}
    sends = []

    async def send(*args, **kwargs):
        sends.append((args, kwargs))

    _install_preset_context(monkeypatch, devices, send)

    result = runner.invoke(preset_commands.app, ["load", str(preset)])

    assert result.exit_code == 1
    assert "these preset devices were not found" in result.output
    assert "Offline" in result.output
    assert sends == []


def test_preset_filters_devices_before_unsupported_preflight(
    monkeypatch,
    tmp_path,
    reset_cli_state,
):
    preset = tmp_path / "filtered.xml"
    _write_preset(
        preset,
        [
            {"name": "Selected", "sample_rate": 48000},
            {"name": "Excluded", "encoding": 24},
        ],
    )
    devices = {
        "selected.local.": _preset_device("Selected"),
        "excluded.local.": _preset_device("Excluded"),
    }
    sends = []
    reset_cli_state.names = ["Selected"]

    async def send(*args, **kwargs):
        sends.append((args, kwargs))

    _install_preset_context(monkeypatch, devices, send)

    result = runner.invoke(preset_commands.app, ["load", str(preset)])

    assert result.exit_code == 0
    assert len(sends) == 1
    assert sends[0][1]["expect_response"] is False
    assert "sample rate 48000 Hz (verified)" in result.output
    assert "Excluded" not in result.output


def test_preset_accepts_nonstandard_sample_rate_advertised_by_device(monkeypatch, tmp_path):
    preset = tmp_path / "future-rate.xml"
    _write_preset(preset, [{"name": "Device", "sample_rate": 384000}])
    devices = {
        "device.local.": _preset_device(
            "Device",
            sample_rate=384000,
            supported_sample_rates=[48000, 384000],
        )
    }
    sends = []

    async def send(*args, **kwargs):
        sends.append((args, kwargs))

    _install_preset_context(monkeypatch, devices, send)

    result = runner.invoke(preset_commands.app, ["load", str(preset)])

    assert result.exit_code == 0
    assert len(sends) == 1
    assert "sample rate 384000 Hz (verified)" in result.output


def test_preset_rejects_sample_rate_missing_from_device_capabilities(monkeypatch, tmp_path):
    preset = tmp_path / "unsupported-rate.xml"
    _write_preset(preset, [{"name": "Device", "sample_rate": 96000}])
    devices = {
        "device.local.": _preset_device(
            "Device",
            supported_sample_rates=[48000],
        )
    }
    sends = []

    async def send(*args, **kwargs):
        sends.append((args, kwargs))

    _install_preset_context(monkeypatch, devices, send)

    result = runner.invoke(preset_commands.app, ["load", str(preset)])

    assert result.exit_code == 1
    assert "device reports supported sample rates [48000]" in result.output
    assert sends == []


def test_preset_preflight_rejects_incomplete_static_interface(monkeypatch, tmp_path):
    preset = tmp_path / "static.xml"
    interface = '<interface><ipv4_address mode="static"><ip_address>192.0.2.9</ip_address></ipv4_address></interface>'
    _write_preset(preset, [{"name": "Device", "interface": interface}])
    devices = {"device.local.": _preset_device("Device")}
    sends = []

    async def send(*args, **kwargs):
        sends.append((args, kwargs))

    _install_preset_context(monkeypatch, devices, send)

    result = runner.invoke(preset_commands.app, ["load", str(preset)])

    assert result.exit_code == 1
    assert "static interface is missing" in result.output
    assert sends == []


def test_preset_reports_unverified_requests_honestly(monkeypatch, tmp_path):
    preset = tmp_path / "requested.xml"
    interface = '<interface><ipv4_address mode="dynamic" /></interface>'
    _write_preset(
        preset,
        [{"name": "Device", "preferred": True, "interface": interface}],
    )
    devices = {"device.local.": _preset_device("Device")}
    sends = []

    async def send(*args, **kwargs):
        sends.append((args, kwargs))

    _install_preset_context(monkeypatch, devices, send)
    monkeypatch.setattr(
        "netaudio.dante.services.cmc._get_host_mac",
        lambda *_args: b"\x02\x00\x00\x00\x00\x01",
    )

    result = runner.invoke(preset_commands.app, ["load", str(preset)])

    assert result.exit_code == 0
    assert len(sends) == 2
    assert all(kwargs["expect_response"] is False for _, kwargs in sends)
    assert "preferred leader on requested; not verified" in result.output
    assert "interface dynamic requested; not verified" in result.output
    assert "applied" not in result.output.lower()


@pytest.mark.parametrize(
    ("pending_interface_config", "expects_reboot"),
    [
        (None, False),
        ({"mode": "dynamic"}, True),
    ],
)
def test_preset_verifies_preferred_leader_and_interface_when_available(
    monkeypatch,
    tmp_path,
    pending_interface_config,
    expects_reboot,
):
    preset = tmp_path / "verified-state.xml"
    interface = '<interface><ipv4_address mode="dynamic" /></interface>'
    _write_preset(
        preset,
        [{"name": "Device", "preferred": True, "interface": interface}],
    )
    device = _preset_device("Device")
    device.interface_pending_config = pending_interface_config
    devices = {"device.local.": device}

    class ReadbackApplication:
        shutdown_called = False

        async def probe_preferred_leader_state(self, _device_ip, timeout):
            assert timeout == 1.0
            return True

        async def probe_interface_status(self, _device_ip, timeout):
            assert timeout == 1.0
            return [{"mode": "dynamic", "ip_address": "192.0.2.40"}]

        async def shutdown(self):
            self.shutdown_called = True

    readback = ReadbackApplication()

    async def start_readback():
        return readback

    async def send(*_args, **_kwargs):
        return None

    _install_preset_context(monkeypatch, devices, send)
    monkeypatch.setattr(
        preset_commands,
        "_start_preset_readback_application",
        start_readback,
    )
    monkeypatch.setattr(
        "netaudio.dante.services.cmc._get_host_mac",
        lambda *_args: b"\x02\x00\x00\x00\x00\x01",
    )

    result = runner.invoke(preset_commands.app, ["load", str(preset)])

    assert result.exit_code == 0
    assert "preferred leader on (verified)" in result.output
    assert "interface dynamic (verified)" in result.output
    assert "requested; not verified" not in result.output
    assert ("Reboot required: Device" in result.output) is expects_reboot
    assert readback.shutdown_called


def test_preset_summary_reports_partial_failure_and_continues(
    monkeypatch,
    tmp_path,
):
    preset = tmp_path / "partial-runtime.xml"
    _write_preset(
        preset,
        [{"name": "Device", "sample_rate": 48000, "preferred": True}],
    )
    devices = {"device.local.": _preset_device("Device")}
    send_count = 0

    async def send(*_args, **_kwargs):
        nonlocal send_count
        send_count += 1
        if send_count == 1:
            raise OSError("synthetic sample-rate send failure")

    _install_preset_context(monkeypatch, devices, send)

    result = runner.invoke(preset_commands.app, ["load", str(preset)])

    assert result.exit_code == 1
    assert send_count == 2
    assert "Preset load summary:" in result.output
    assert "sample rate: FAILED to send request: synthetic sample-rate send failure" in result.output
    assert "preferred leader on requested; not verified" in result.output
