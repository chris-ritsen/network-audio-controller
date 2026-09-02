import asyncio
from types import SimpleNamespace

import pytest
from netaudio.asynchronous_primitives import DeferredAsyncioLock
from netaudio.cli_support import output as output_module
from netaudio.commands.preset import cli as preset_commands
from typer.testing import CliRunner

from tests.cli_test_support import FakeApplication
from tests.test_cli_mutation_safety import _channel, _subscription

runner = CliRunner()


class PresetApplication(FakeApplication):
    async def probe_preferred_leader_state(self, device_ip_address, timeout=2.0):
        raise RuntimeError("synthetic readback service unavailable")

    async def probe_interface_status(self, device_ip_address, timeout=2.0):
        raise RuntimeError("synthetic readback service unavailable")


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
    try:
        yield state
    finally:
        state.names = original[0]
        state.hosts = original[1]
        state.server_names = original[2]
        state.macs = original[3]
        state.output_format = original[4]


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
        for transmitter_channel in device.get("transmitter_channels", []):
            fields.append(
                f'<txchannel danteId="{transmitter_channel["number"]}" mediaType="audio">'
                f"<label>{transmitter_channel['name']}</label></txchannel>"
            )
        for receiver_channel in device.get("receiver_channels", []):
            receiver_fields = [f"<name>{receiver_channel['name']}</name>"]
            if receiver_channel.get("transmitter_channel"):
                receiver_fields.append(
                    f"<subscribed_channel>{receiver_channel['transmitter_channel']}</subscribed_channel>"
                )
                receiver_fields.append(
                    f"<subscribed_device>{receiver_channel.get('transmitter_device', '.')}</subscribed_device>"
                )
            fields.append(
                f'<rxchannel danteId="{receiver_channel["number"]}" mediaType="audio">'
                f"{''.join(receiver_fields)}</rxchannel>"
            )
        device_xml.append(f"<device>{''.join(fields)}</device>")
    path.write_text(f'<?xml version="1.0"?><preset><name>test</name>{"".join(device_xml)}</preset>')


def _write_transmitter_preset(path, channels):
    _write_preset(path, [{"name": "Transmitter", "transmitter_channels": channels}])


def _preset_device(
    name,
    sample_rate=48000,
    supported_sample_rates=None,
    encoding=24,
    supported_encodings=None,
    active_latency_ns=1_000_000,
):
    return SimpleNamespace(
        name=name,
        server_name=f"{name.lower()}.local.",
        ipv4="192.0.2.40",
        services={},
        supported_sample_rates=supported_sample_rates,
        encoding=encoding,
        supported_encodings=supported_encodings if supported_encodings is not None else [16, 24, 32],
        interface_pending_config=None,
        settings={"sample_rate": sample_rate, "active_latency_ns": active_latency_ns},
        settings_calls=0,
        topology_mutation_lock=DeferredAsyncioLock(),
    )


def _preset_receiver_device(name, source_state):
    device = _preset_device(name)
    device.rx_channels = {
        receiver_channel_number: _channel(receiver_channel_number, f"Rx{receiver_channel_number}")
        for receiver_channel_number in source_state
    }

    def refresh_subscriptions():
        device.subscriptions = []
        for receiver_channel_number, source in source_state.items():
            receiver_channel_name = device.rx_channels[receiver_channel_number].name
            if source is None:
                device.subscriptions.append(_subscription(receiver_channel_name, None, None))
            else:
                device.subscriptions.append(_subscription(receiver_channel_name, source[0], source[1]))

    async def get_receiver_channels():
        refresh_subscriptions()

    refresh_subscriptions()
    device.get_rx_channels = get_receiver_channels
    return device


def _preset_transmitter_device(name, routing_name_state):
    device = _preset_device(name)
    device.transmitter_channel_name_protocol_identifier = None

    async def get_transmitter_channels():
        device.tx_channels = {
            channel_number: SimpleNamespace(
                number=channel_number,
                name=f"Factory-{channel_number}",
                friendly_name=routing_name,
            )
            for channel_number, routing_name in routing_name_state.items()
        }

    device.get_tx_channels = get_transmitter_channels
    return device


def _install_preset_context(monkeypatch, devices, application=None):
    application = PresetApplication(devices) if application is None else application

    def run_command(run, *arguments, **options):
        return asyncio.run(run(application, devices, *arguments, **options))

    monkeypatch.setattr(preset_commands, "run_command", run_command)
    return application


def _sent_operations(application):
    return [sent.operation for sent in application.sent]


def test_preset_save_refuses_overwrite_before_discovery(monkeypatch, tmp_path):
    output = tmp_path / "existing.xml"
    output.write_text("original")
    entered = False

    def run_command(run, *arguments, **options):
        nonlocal entered
        entered = True

    monkeypatch.setattr(preset_commands, "run_command", run_command)

    result = runner.invoke(preset_commands.app, ["save", str(output)])

    assert result.exit_code == 1
    assert "refusing to overwrite existing file" in result.output
    assert output.read_text() == "original"
    assert not entered


def test_preset_force_save_failure_preserves_existing_file(monkeypatch, tmp_path):
    output = tmp_path / "existing.xml"
    output.write_text("original")
    devices = {"device.local.": SimpleNamespace(name="Device")}

    _install_preset_context(monkeypatch, devices)
    monkeypatch.setattr(
        output_module,
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

    _install_preset_context(monkeypatch, devices)
    monkeypatch.setattr(
        output_module,
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

    _install_preset_context(monkeypatch, devices)
    monkeypatch.setattr(
        output_module,
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

    application = _install_preset_context(monkeypatch, devices)

    result = runner.invoke(preset_commands.app, ["load", str(preset)])

    assert result.exit_code == 1
    assert "unsupported fields: additional network interfaces" in result.output
    assert application.sent == []


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

    application = _install_preset_context(monkeypatch, devices)

    result = runner.invoke(preset_commands.app, ["load", str(preset)])

    assert result.exit_code == 1
    assert "refused before sending any changes" in result.output
    assert "Second: device reports supported encodings [16]; 24 is not supported" in result.output
    assert application.sent == []


def test_preset_load_applies_and_verifies_encoding_and_latency(monkeypatch, tmp_path):
    preset = tmp_path / "audio-settings.xml"
    _write_preset(preset, [{"name": "Device", "encoding": 24, "latency_us": 150}])
    device = _preset_device("Device", encoding=24, active_latency_ns=150_000)
    application = _install_preset_context(monkeypatch, {"device.local.": device})

    result = runner.invoke(preset_commands.app, ["load", str(preset)])

    assert result.exit_code == 0
    assert "encoding 24-bit (verified)" in result.output
    assert "latency 0.15 ms (verified)" in result.output
    assert _sent_operations(application) == ["set_encoding", "set_latency"]


def test_preset_load_reconciles_receiver_subscriptions(monkeypatch, tmp_path):
    preset = tmp_path / "subscriptions.xml"
    _write_preset(
        preset,
        [
            {
                "name": "Receiver",
                "receiver_channels": [
                    {
                        "number": 1,
                        "name": "Rx1",
                        "transmitter_channel": "NewTx",
                        "transmitter_device": "Transmitter",
                    },
                    {"number": 2, "name": "Rx2"},
                ],
            }
        ],
    )
    source_state = {
        1: ("OldTx", "OldTransmitter"),
        2: ("Tx2", "Transmitter"),
    }
    receiver_device = _preset_receiver_device("Receiver", source_state)

    class ReconcilingApplication(PresetApplication):
        async def remove_subscriptions(self, device, channel_numbers):
            assert list(channel_numbers) == [2]
            source_state[2] = None
            return self._record("remove_subscriptions", device, tuple(channel_numbers))

        async def add_subscriptions(self, device, records):
            assert list(records) == [(1, "NewTx", "Transmitter")]
            source_state[1] = ("NewTx", "Transmitter")
            return self._record("add_subscriptions", device, tuple(records))

    application = _install_preset_context(
        monkeypatch,
        {"receiver.local.": receiver_device},
        ReconcilingApplication({"receiver.local.": receiver_device}),
    )

    result = runner.invoke(preset_commands.app, ["load", str(preset)])

    assert result.exit_code == 0
    assert _sent_operations(application) == ["remove_subscriptions", "add_subscriptions"]
    assert "receiver channel 1 <- NewTx@Transmitter (verified)" in result.output
    assert "receiver channel 2 unsubscribed (verified)" in result.output


def test_preset_load_reconciles_transmitter_channel_names(monkeypatch, tmp_path):
    preset = tmp_path / "transmitter-channel-names.xml"
    _write_transmitter_preset(
        preset,
        [
            {"number": 1, "name": "New-1"},
            {"number": 2, "name": "Current-2"},
        ],
    )
    routing_name_state = {1: "Old-1", 2: "Current-2"}
    transmitter_device = _preset_transmitter_device("Transmitter", routing_name_state)
    probes = []

    class RenamingApplication(PresetApplication):
        async def resolve_channel_name_protocol_identifier(self, device, channel_type):
            probes.append(channel_type)
            transmitter_device.transmitter_channel_name_protocol_identifier = 0x2809
            return 0x2809

        async def set_channel_name(self, device, channel_type, channel_number, name):
            routing_name_state[channel_number] = name
            self._record("set_channel_name", device, channel_type, channel_number, name)
            return bytes.fromhex("2809000c0302201300010000")

    application = _install_preset_context(
        monkeypatch,
        {"transmitter.local.": transmitter_device},
        RenamingApplication({"transmitter.local.": transmitter_device}),
    )

    result = runner.invoke(preset_commands.app, ["load", str(preset)])

    assert result.exit_code == 0
    assert probes == ["tx"]
    assert [(sent.operation, sent.arguments) for sent in application.sent] == [
        ("set_channel_name", ("tx", 1, "New-1")),
    ]
    assert "transmitter channel 1: New-1 (verified)" in result.output


def test_preset_transmitter_name_preflight_rejects_missing_channel(monkeypatch, tmp_path):
    preset = tmp_path / "missing-transmitter-channel.xml"
    _write_transmitter_preset(
        preset,
        [{"number": 3, "name": "Missing"}],
    )
    transmitter_device = _preset_transmitter_device("Transmitter", {1: "One", 2: "Two"})

    application = _install_preset_context(monkeypatch, {"transmitter.local.": transmitter_device})

    result = runner.invoke(preset_commands.app, ["load", str(preset)])

    assert result.exit_code == 1
    assert "transmitter channel 3 is unavailable" in result.output
    assert "refused before sending any changes" in result.output
    assert application.sent == []


def test_preset_subscription_preflight_rejects_missing_receiver_channel(monkeypatch, tmp_path):
    preset = tmp_path / "missing-receiver-channel.xml"
    _write_preset(
        preset,
        [
            {
                "name": "Receiver",
                "receiver_channels": [
                    {
                        "number": 3,
                        "name": "Rx3",
                        "transmitter_channel": "Tx1",
                        "transmitter_device": "Transmitter",
                    }
                ],
            }
        ],
    )
    receiver_device = _preset_receiver_device(
        "Receiver",
        {1: None, 2: None},
    )

    application = _install_preset_context(monkeypatch, {"receiver.local.": receiver_device})

    result = runner.invoke(preset_commands.app, ["load", str(preset)])

    assert result.exit_code == 1
    assert "receiver channel 3 is unavailable" in result.output
    assert "refused before sending any changes" in result.output
    assert application.sent == []


def test_preset_subscription_reconciliation_is_idempotent(monkeypatch, tmp_path):
    preset = tmp_path / "matching-subscriptions.xml"
    _write_preset(
        preset,
        [
            {
                "name": "Receiver",
                "receiver_channels": [
                    {
                        "number": 1,
                        "name": "Rx1",
                        "transmitter_channel": "Tx1",
                        "transmitter_device": "Transmitter",
                    },
                    {"number": 2, "name": "Rx2"},
                ],
            }
        ],
    )
    receiver_device = _preset_receiver_device(
        "Receiver",
        {1: ("Tx1", "Transmitter"), 2: None},
    )

    application = _install_preset_context(monkeypatch, {"receiver.local.": receiver_device})

    result = runner.invoke(preset_commands.app, ["load", str(preset)])

    assert result.exit_code == 0
    assert application.sent == []
    assert "receiver subscriptions already match (2 channels)" in result.output


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

    application = _install_preset_context(monkeypatch, devices)

    result = runner.invoke(preset_commands.app, ["load", str(preset)])

    assert result.exit_code == 1
    assert "these preset devices were not found" in result.output
    assert "Offline" in result.output
    assert application.sent == []


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

    application = _install_preset_context(monkeypatch, devices)

    result = runner.invoke(preset_commands.app, ["load", str(preset)])

    assert result.exit_code == 0
    assert application.sent == []
    assert "sample rate already 48000 Hz (verified; no write sent)" in result.output
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

    application = _install_preset_context(monkeypatch, devices)

    result = runner.invoke(preset_commands.app, ["load", str(preset)])

    assert result.exit_code == 0
    assert application.sent == []
    assert "sample rate already 384000 Hz (verified; no write sent)" in result.output


def test_preset_routes_sample_rate_through_shared_safe_operation(monkeypatch, tmp_path):
    preset = tmp_path / "shared-sample-rate-operation.xml"
    _write_preset(preset, [{"name": "Device", "sample_rate": 48000}])
    device = _preset_device("Device")
    devices = {"device.local.": device}
    calls = []

    async def change_sample_rate(target_device, sample_rate, confirm_destructive=False, timeout=4.0):
        calls.append((target_device, sample_rate, confirm_destructive, timeout))
        preflight = SimpleNamespace()
        return SimpleNamespace(
            changed=False,
            preflight=preflight,
            observed_sample_rate_hertz=48_000,
        )

    application = _install_preset_context(monkeypatch, devices)
    application.set_sample_rate = change_sample_rate

    result = runner.invoke(
        preset_commands.app,
        ["load", str(preset), "--confirm-destructive"],
    )

    assert result.exit_code == 0
    assert calls == [(device, 48_000, True, 4.0)]
    assert application.sent == []
    assert "sample rate already 48000 Hz (verified; no write sent)" in result.output


def test_preset_rejects_sample_rate_missing_from_device_capabilities(monkeypatch, tmp_path):
    preset = tmp_path / "unsupported-rate.xml"
    _write_preset(preset, [{"name": "Device", "sample_rate": 96000}])
    devices = {
        "device.local.": _preset_device(
            "Device",
            supported_sample_rates=[48000],
        )
    }

    application = _install_preset_context(monkeypatch, devices)

    result = runner.invoke(preset_commands.app, ["load", str(preset)])

    assert result.exit_code == 1
    assert "device reports supported sample rates [48000]" in result.output
    assert application.sent == []


def test_preset_preflight_rejects_incomplete_static_interface(monkeypatch, tmp_path):
    preset = tmp_path / "static.xml"
    interface = '<interface><ipv4_address mode="static"><ip_address>192.0.2.9</ip_address></ipv4_address></interface>'
    _write_preset(preset, [{"name": "Device", "interface": interface}])
    devices = {"device.local.": _preset_device("Device")}

    application = _install_preset_context(monkeypatch, devices)

    result = runner.invoke(preset_commands.app, ["load", str(preset)])

    assert result.exit_code == 1
    assert "static interface is missing" in result.output
    assert application.sent == []


def test_preset_reports_unverified_requests_honestly(monkeypatch, tmp_path):
    preset = tmp_path / "requested.xml"
    interface = '<interface><ipv4_address mode="dynamic" /></interface>'
    _write_preset(
        preset,
        [{"name": "Device", "preferred": True, "interface": interface}],
    )
    devices = {"device.local.": _preset_device("Device")}
    application = _install_preset_context(monkeypatch, devices)

    result = runner.invoke(preset_commands.app, ["load", str(preset)])

    assert result.exit_code == 0
    assert [(sent.operation, sent.arguments) for sent in application.sent] == [
        ("set_preferred_leader", (True,)),
        ("set_interface", ("dhcp", None)),
    ]
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

    class ReadbackApplication(PresetApplication):
        async def probe_preferred_leader_state(self, _device_ip, timeout):
            assert timeout == 1.0
            return True

        async def probe_interface_status(self, _device_ip, timeout):
            assert timeout == 1.0
            return [{"mode": "dynamic", "ip_address": "192.0.2.40"}]

    _install_preset_context(monkeypatch, devices, ReadbackApplication(devices))

    result = runner.invoke(preset_commands.app, ["load", str(preset)])

    assert result.exit_code == 0
    assert "preferred leader on (verified)" in result.output
    assert "interface dynamic (verified)" in result.output
    assert "requested; not verified" not in result.output
    assert ("Reboot required: Device" in result.output) is expects_reboot


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

    async def fail_sample_rate(*_args, **_kwargs):
        from netaudio.dante.sample_rate_topology import SampleRateTopologyMutationOutcomeUnknownError

        preflight = SimpleNamespace(to_dict=lambda: {})
        raise SampleRateTopologyMutationOutcomeUnknownError(
            "sample-rate mutation failed after it was attempted; device state is unknown: synthetic send failure",
            preflight,
        )

    application = _install_preset_context(monkeypatch, devices)
    application.set_sample_rate = fail_sample_rate

    result = runner.invoke(preset_commands.app, ["load", str(preset)])

    assert result.exit_code == 1
    assert _sent_operations(application) == ["set_preferred_leader"]
    assert "Preset load summary:" in result.output
    assert "sample rate: MUTATION OUTCOME UNKNOWN" in result.output
    assert "synthetic send failure" in result.output
    assert "preferred leader on requested; not verified" in result.output
