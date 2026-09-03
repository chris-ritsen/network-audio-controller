import json
from types import SimpleNamespace

import pytest
from netaudio.cli_support.selection import parse_channel_reference
from netaudio.commands import channel as channel_commands
from netaudio.commands.config import cli as config_commands
from netaudio.commands.config import interface as config_network_commands
from netaudio.commands.device import lock as lock_commands
from netaudio.dante.application import CapabilityProbeTimeout
from netaudio.dante.lock_status import LockStatusObservation
from typer.testing import CliRunner

from tests.cli_test_support import FakeApplication, FakeChannelDevice, FakeDevice, invoke

runner = CliRunner()


def _gain(application, channel, level=None):
    return invoke(
        channel_commands.run_channel_gain,
        application,
        application.devices,
        parse_channel_reference(channel),
        level,
    )


def _operations(application):
    return [sent.operation for sent in application.sent]


def test_gain_getter_reports_configured_reference_level():
    device = FakeChannelDevice(channel_reads="unused", gain_status=("input", [5]))
    application = FakeApplication({"avio.local.": device})

    result = _gain(application, "tx:1")

    assert result.exit_code == 0
    assert "-10 dBV (level 5)" in result.output
    assert "level 2" not in result.output


def test_gain_getter_uses_the_device_reported_channel_side():
    device = FakeChannelDevice(channel_reads="unused", gain_status=("input", [5]))
    application = FakeApplication({"avio.local.": device})

    result = _gain(application, "1")

    assert result.exit_code == 0
    assert "-10 dBV (level 5)" in result.output


def test_gain_setter_reports_success_only_after_matching_readback():
    device = FakeChannelDevice(channel_reads="unused", gain_status=("input", [5]))
    application = FakeApplication({"avio.local.": device})

    result = _gain(application, "tx:1", 3)

    assert result.exit_code == 0
    assert "0 dBu (verified)" in result.output
    assert device.gain_levels == [3]


def test_gain_setter_rejects_mismatched_readback():
    device = FakeChannelDevice(
        channel_reads="unused",
        gain_status=("input", [5]),
        gain_write_status=("input", [5]),
    )
    application = FakeApplication({"avio.local.": device})

    result = _gain(application, "tx:1", 3)

    assert result.exit_code == 1
    assert "gain change was not applied" in result.output
    assert "Set input reference level" not in result.output


def test_gain_setter_rejects_missing_readback():
    device = FakeChannelDevice(channel_reads="unused", gain_status=("input", [5]), gain_write_status=None)
    application = FakeApplication({"avio.local.": device})

    result = _gain(application, "tx:1", 3)

    assert result.exit_code == 1
    assert "readback was unavailable" in result.output


def test_gain_rejects_fractional_level_before_discovery(monkeypatch, reset_cli_state):
    def should_not_run(*_arguments, **_options):
        raise AssertionError("fractional gain must be rejected before command execution")

    monkeypatch.setattr(channel_commands, "run_command", should_not_run)

    result = runner.invoke(channel_commands.app, ["gain", "tx:1", "1.9"])

    assert result.exit_code != 0
    assert "not a valid integer" in result.output


def test_channel_commands_reject_unknown_channel_type_before_sending(monkeypatch, reset_cli_state):
    def should_not_run(*_arguments, **_options):
        raise AssertionError("an unknown channel direction must be rejected before command execution")

    monkeypatch.setattr(channel_commands, "run_command", should_not_run)

    result = runner.invoke(channel_commands.app, ["name", "banana:1", "New"])

    assert result.exit_code == 1
    assert "unknown channel direction 'banana'" in result.output


def _sample_rate(application, rate, all_devices=False):
    return invoke(config_commands.run_sample_rate, application, application.devices, rate, all_devices, False)


def test_single_device_sample_rate_read_is_labeled():
    device = FakeDevice("AVIO")
    device.sample_rate = 48_000
    application = FakeApplication({"avio.local.": device})

    result = _sample_rate(application, None)

    assert result.exit_code == 0
    assert result.output == "Sample rate: 48000 Hz\n"


def test_single_device_sample_rate_json_uses_unit_labeled_key(monkeypatch):
    from netaudio.cli import OutputFormat, state

    device = FakeDevice("AVIO")
    device.sample_rate = 48_000
    application = FakeApplication({"avio.local.": device})
    monkeypatch.setattr(state, "output_format", OutputFormat.json)

    result = _sample_rate(application, None)

    assert result.exit_code == 0
    assert json.loads(result.output) == {"sample_rate_hz": 48_000}


def test_sample_rate_all_aggregates_readback_failures():
    good = FakeDevice("Good", settings={"sample_rate": 48000}, ipv4="192.0.2.10")
    stale = FakeDevice(
        "Stale",
        settings={"sample_rate": 44100},
        ipv4="192.0.2.11",
        supported_sample_rates=[44_100, 48_000],
    )
    application = FakeApplication({"good.local.": good, "stale.local.": stale})

    result = _sample_rate(application, 48000, all_devices=True)

    assert result.exit_code == 1
    assert _operations(application) == ["set_sample_rate"]
    assert "Good" in result.output
    assert "Unchanged" in result.output
    assert "Stale" in result.output
    assert "Changed but unverified" in result.output
    assert "device reports 44100 Hz instead of 48000 Hz" in result.output


def test_sample_rate_refuses_advertised_rate_without_proven_topology_capacity():
    from netaudio.dante.sample_rate_topology import SampleRateTopologyUnsupportedError

    device = FakeDevice(
        "Future",
        settings={"sample_rate": 384_000},
        supported_sample_rates=[48_000, 384_000],
    )
    application = FakeApplication({"future.local.": device})

    async def refuse_unproven_capacity(*_arguments, **_options):
        raise SampleRateTopologyUnsupportedError("no proven Ferrofish A32 channel capacity is available for 384000 Hz")

    application.set_sample_rate = refuse_unproven_capacity

    result = _sample_rate(application, 384000)

    assert result.exit_code == 1
    assert "no proven Ferrofish A32 channel capacity" in result.output
    assert application.sent == []


def test_sample_rate_uses_active_readback_when_mutation_notification_is_absent():
    device = FakeDevice(
        "Quiet Device",
        settings={"sample_rate": 96000},
        supported_sample_rates=[48000, 96000],
    )
    application = FakeApplication({"quiet.local.": device})

    result = _sample_rate(application, 96000)

    assert result.exit_code == 0
    assert "fresh readback already reports 96000 Hz; no write sent" in result.output
    assert application.sent == []


def _encoding(application, bits, all_devices=False):
    return invoke(config_commands.run_encoding, application, application.devices, bits, all_devices)


def test_single_device_encoding_read_is_labeled():
    device = FakeDevice("AVIO", encoding=24)
    application = FakeApplication({"avio.local.": device})

    result = _encoding(application, None)

    assert result.exit_code == 0
    assert result.output == "Encoding: 24-bit\n"


def test_encoding_rejects_value_missing_from_advertised_capabilities():
    device = FakeDevice("AVIO", encoding=24, supported_encodings=[24])
    application = FakeApplication({"avio.local.": device})

    result = _encoding(application, 16)

    assert result.exit_code == 1
    assert "reports supported encoding values [24]" in result.output
    assert application.sent == []


def test_encoding_uses_advertised_nonstandard_future_value():
    device = FakeDevice("Future", encoding=20, supported_encodings=[20, 24])
    application = FakeApplication({"future.local.": device})

    result = _encoding(application, 20)

    assert result.exit_code == 0
    assert "20-bit (verified)" in result.output
    assert [(sent.operation, sent.arguments) for sent in application.sent] == [("set_encoding", (20,))]


def _latency(application, value, all_devices=False):
    return invoke(config_commands.run_latency, application, application.devices, value, all_devices)


def test_fractional_latency_verifies_rounded_nanoseconds():
    device = FakeDevice("AVIO", settings={"active_latency_ns": 150_000})
    application = FakeApplication({"avio.local.": device})

    result = _latency(application, 0.15)

    assert result.exit_code == 0
    assert "Set latency for AVIO: 0.15 ms (verified)" in result.output


def test_latency_get_uses_active_device_readback():
    device = FakeDevice(
        "AVIO",
        settings={
            "default_latency_ns": 1_000_000,
            "configured_latency_ns": 250_000,
            "active_latency_ns": 150_000,
            "min_latency_ns": 150_000,
            "max_latency_ns": 21_333_334,
        },
    )
    device.latency = 99.0
    application = FakeApplication({"avio.local.": device})

    result = _latency(application, None)

    assert result.exit_code == 0
    assert result.output.splitlines() == [
        "Active latency: 0.15 ms",
        "Configured latency: 0.25 ms",
        "Default latency: 1 ms",
        "Reported latency range: 0.15-21.3333 ms",
        "Latency options: 0.15, 0.25, 0.5, 1, 2, 5 ms",
    ]
    assert device.settings_calls == 1


def test_latency_get_json_labels_milliseconds_and_raw_nanoseconds(monkeypatch):
    from netaudio.cli import OutputFormat, state

    device = FakeDevice(
        "AVIO",
        settings={
            "default_latency_ns": 1_000_000,
            "configured_latency_ns": 250_000,
            "active_latency_ns": 150_000,
            "min_latency_ns": 150_000,
            "max_latency_ns": 21_333_334,
        },
    )
    application = FakeApplication({"avio.local.": device})
    monkeypatch.setattr(state, "output_format", OutputFormat.json)

    result = _latency(application, None)

    assert result.exit_code == 0
    assert json.loads(result.output) == {
        "active_latency_ms": 0.15,
        "active_latency_ns": 150_000,
        "configured_latency_ms": 0.25,
        "configured_latency_ns": 250_000,
        "default_latency_ms": 1.0,
        "default_latency_ns": 1_000_000,
        "min_latency_ms": 0.15,
        "min_latency_ns": 150_000,
        "max_latency_ms": 21.333334,
        "max_latency_ns": 21_333_334,
        "latency_options_ms": [0.15, 0.25, 0.5, 1.0, 2.0, 5.0],
        "latency_options_ns": [150_000, 250_000, 500_000, 1_000_000, 2_000_000, 5_000_000],
        "latency_options_source": "controller_fixed_set_filtered_by_reported_range",
        "active_latency_is_standard_choice": True,
        "active_latency_within_reported_range": True,
        "configured_latency_is_standard_choice": True,
        "configured_latency_within_reported_range": True,
    }


def test_latency_get_xml_labels_milliseconds_and_raw_nanoseconds(monkeypatch):
    import xml.etree.ElementTree as ET

    from netaudio.cli import OutputFormat, state

    device = FakeDevice(
        "AVIO",
        settings={
            "configured_latency_ns": 250_000,
            "active_latency_ns": 150_000,
            "min_latency_ns": 150_000,
            "max_latency_ns": 5_000_000,
        },
    )
    application = FakeApplication({"avio.local.": device})
    monkeypatch.setattr(state, "output_format", OutputFormat.xml)

    result = _latency(application, None)

    assert result.exit_code == 0
    root = ET.fromstring(result.output)
    assert root.tag == "netaudio"
    assert root.findtext("active_latency_ms") == "0.15"
    assert root.findtext("active_latency_ns") == "150000"
    assert [item.text for item in root.findall("latency_options_ms/item")] == [
        "0.15",
        "0.25",
        "0.5",
        "1.0",
        "2.0",
        "5.0",
    ]


def test_latency_get_csv_is_a_labeled_field_value_report(monkeypatch):
    import csv
    import io

    from netaudio.cli import OutputFormat, state

    device = FakeDevice(
        "AVIO",
        settings={
            "configured_latency_ns": 250_000,
            "active_latency_ns": 150_000,
            "min_latency_ns": 150_000,
            "max_latency_ns": 5_000_000,
        },
    )
    application = FakeApplication({"avio.local.": device})
    monkeypatch.setattr(state, "output_format", OutputFormat.csv)

    result = _latency(application, None)

    assert result.exit_code == 0
    rows = list(csv.reader(io.StringIO(result.output)))
    assert rows[0] == ["Field", "Value"]
    assert ["active_latency_ms", "0.15"] in rows
    assert ["active_latency_ns", "150000"] in rows


def test_latency_get_surfaces_retained_configured_value_when_active_is_zero():
    device = FakeDevice(
        "AVIO",
        settings={
            "configured_latency_ns": 250_000,
            "active_latency_ns": 0,
            "min_latency_ns": 1_000_000,
            "max_latency_ns": 20_312_500,
        },
    )
    application = FakeApplication({"avio.local.": device})

    result = _latency(application, None)

    assert result.exit_code == 0
    assert result.output.splitlines() == [
        "Active latency: 0 ms",
        "Configured latency: 0.25 ms",
        "Reported latency range: 1-20.3125 ms",
        "Latency options: 1, 2, 5 ms",
    ]


def test_latency_get_json_surfaces_retained_configured_value(monkeypatch):
    from netaudio.cli import OutputFormat, state

    device = FakeDevice(
        "AVIO",
        settings={
            "configured_latency_ns": 250_000,
            "active_latency_ns": 0,
            "min_latency_ns": 1_000_000,
            "max_latency_ns": 20_312_500,
        },
    )
    application = FakeApplication({"avio.local.": device})
    monkeypatch.setattr(state, "output_format", OutputFormat.json)

    result = _latency(application, None)

    assert result.exit_code == 0
    assert json.loads(result.output) == {
        "active_latency_ms": 0.0,
        "active_latency_ns": 0,
        "configured_latency_ms": 0.25,
        "configured_latency_ns": 250_000,
        "min_latency_ms": 1.0,
        "min_latency_ns": 1_000_000,
        "max_latency_ms": 20.3125,
        "max_latency_ns": 20_312_500,
        "latency_options_ms": [1.0, 2.0, 5.0],
        "latency_options_ns": [1_000_000, 2_000_000, 5_000_000],
        "latency_options_source": "controller_fixed_set_filtered_by_reported_range",
        "active_latency_is_standard_choice": False,
        "active_latency_within_reported_range": False,
        "configured_latency_is_standard_choice": False,
        "configured_latency_within_reported_range": False,
    }


def test_latency_get_json_preserves_an_off_list_current_value(monkeypatch):
    from netaudio.cli import OutputFormat, state

    device = FakeDevice(
        "Synthetic",
        settings={
            "configured_latency_ns": 750_000,
            "active_latency_ns": 750_000,
            "min_latency_ns": 150_000,
            "max_latency_ns": 5_000_000,
        },
    )
    application = FakeApplication({"synthetic.local.": device})
    monkeypatch.setattr(state, "output_format", OutputFormat.json)

    result = _latency(application, None)

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["active_latency_ms"] == 0.75
    assert payload["active_latency_ns"] == 750_000
    assert payload["active_latency_within_reported_range"] is True
    assert payload["active_latency_is_standard_choice"] is False
    assert 0.75 not in payload["latency_options_ms"]


def test_latency_get_fails_when_every_reported_latency_value_is_unavailable():
    device = FakeDevice(
        "Unavailable",
        settings={
            "active_latency_ns": None,
            "configured_latency_ns": None,
            "default_latency_ns": None,
            "min_latency_ns": None,
            "max_latency_ns": None,
        },
    )
    application = FakeApplication({"unavailable.local.": device})

    result = _latency(application, None)

    assert result.exit_code == 1
    assert "latency readback was unavailable" in result.output


def test_clock_source_get_is_not_implemented(monkeypatch, reset_cli_state):
    def should_not_run(*_arguments, **_options):
        raise AssertionError("clock source must not discover devices")

    monkeypatch.setattr(config_commands, "run_command", should_not_run)

    result = runner.invoke(config_commands.app, ["clock-source"])

    assert result.exit_code == 0
    assert result.output.strip() == "not implemented"


def test_clock_source_set_is_not_implemented(monkeypatch, reset_cli_state):
    def should_not_run(*_arguments, **_options):
        raise AssertionError("clock source must not discover devices")

    monkeypatch.setattr(config_commands, "run_command", should_not_run)

    result = runner.invoke(config_commands.app, ["clock-source", "1"])

    assert result.exit_code == 1
    assert "not implemented" in result.output


def test_latency_does_not_treat_configured_value_as_applied():
    device = FakeDevice(
        "AVIO",
        settings={
            "configured_latency_ns": 150_000,
            "active_latency_ns": 1_000_000,
        },
    )
    application = FakeApplication({"avio.local.": device})

    result = _latency(application, 0.15)

    assert result.exit_code == 1
    assert "1000000 instead of 150000" in result.output
    assert "Set latency for AVIO" not in result.output


def test_latency_range_does_not_block_device_verified_nonstandard_value():
    device = FakeDevice(
        "AVIO",
        min_latency=1.0,
        max_latency=5.0,
        settings={"active_latency_ns": 250_000},
    )
    application = FakeApplication({"avio.local.": device})

    result = _latency(application, 0.25)

    assert result.exit_code == 0
    assert "Set latency for AVIO: 0.25 ms (verified)" in result.output
    assert _operations(application) == ["set_latency"]


@pytest.mark.parametrize("value", [-0.1, float("nan"), float("inf")])
def test_latency_rejects_nonfinite_or_negative_values_before_sending(value):
    device = FakeDevice("AVIO", settings={"active_latency_ns": 0})
    application = FakeApplication({"avio.local.": device})

    result = _latency(application, value)

    assert result.exit_code == 1
    assert "finite, nonnegative" in result.output
    assert application.sent == []


def _aes67(application, enabled, multicast_prefix=None, all_devices=False):
    return invoke(
        config_commands.run_aes67,
        application,
        application.devices,
        enabled,
        multicast_prefix,
        all_devices,
    )


def test_aes67_verifies_configured_state_not_current_state():
    device = FakeDevice("AVIO", aes67=True)
    device.aes67_current = False
    application = FakeApplication({"avio.local.": device})

    result = _aes67(application, "on")

    assert result.exit_code == 0
    assert device.aes67_calls == 1
    assert "AES67 configured state for AVIO: on (verified)" in result.output
    assert [(sent.operation, sent.arguments) for sent in application.sent] == [("set_aes67_enabled", (True,))]


def test_aes67_rejects_known_unsupported_device_without_sending():
    device = FakeDevice("LX-DANTE", aes67_supported=False)
    application = FakeApplication({"lx.local.": device})

    result = _aes67(application, "on")

    assert result.exit_code == 1
    assert "does not support AES67 configuration" in result.output
    assert application.sent == []


def test_encoding_is_verified_from_reported_status():
    device = FakeDevice("AVIO", encoding=24, supported_encodings=[24])
    application = FakeApplication({"avio.local.": device})

    result = _encoding(application, 24)

    assert result.exit_code == 0
    assert "Set encoding for AVIO: 24-bit (verified)" in result.output
    assert [(sent.operation, sent.arguments) for sent in application.sent] == [("set_encoding", (24,))]


def test_preferred_leader_write_is_requested_but_not_verified():
    device = FakeDevice("AVIO")
    application = FakeApplication({"avio.local.": device})

    result = invoke(config_commands.run_preferred_leader, application, application.devices, "on", False)

    assert result.exit_code == 0
    assert "Preferred leader change requested for AVIO: on; not verified" in result.output
    assert [(sent.operation, sent.arguments) for sent in application.sent] == [("set_preferred_leader", (True,))]


def test_single_device_preferred_leader_read_is_labeled():
    device = FakeDevice("AVIO")
    device.preferred_leader = False
    application = FakeApplication({"avio.local.": device})

    result = invoke(config_commands.run_preferred_leader, application, application.devices, None, False)

    assert result.exit_code == 0
    assert result.output == "Preferred leader: off\n"


def test_interface_write_is_requested_but_not_verified():
    device = FakeDevice("AVIO")
    application = FakeApplication({"avio.local.": device})

    result = invoke(config_network_commands.run_interface, application, application.devices, "dhcp", None, False)

    assert result.exit_code == 0
    assert "Interface change requested for AVIO: dhcp; not verified" in result.output
    assert [(sent.operation, sent.arguments) for sent in application.sent] == [("set_interface", ("dhcp", None))]


@pytest.mark.parametrize(
    ("locking", "action"),
    [(True, "lock"), (False, "unlock")],
)
def test_daemon_lock_failure_reports_protocol_status(monkeypatch, reset_cli_state, locking, action):
    async def failed_daemon_request(device_name, pin, requested_action):
        assert device_name == "AVIO"
        assert pin == "1234"
        assert requested_action == action
        return {"success": False, "status": 0x1101}

    monkeypatch.setattr(lock_commands, "_lock_via_daemon", failed_daemon_request)
    device = FakeDevice("AVIO")

    result = invoke(
        lock_commands.run_lock_operation, SimpleNamespace(), {device.server_name: device}, "1234", locking=locking
    )

    assert result.exit_code == 1
    assert f"Error: {action} failed (status 0x1101)" in result.output
    assert "unknown" not in result.output


def _lock_observation(is_locked):
    return LockStatusObservation(
        lock_reset_status={
            "lock_state_code": 1 if is_locked else 0,
            "is_locked": is_locked,
            "status_code": 0,
        },
        observed_at="2026-08-21T20:57:35.396345+00:00",
    )


async def _async_value(value):
    return value


def _lock(application, locking, device=None):
    device = device or FakeDevice("AVIO")
    return invoke(lock_commands.run_lock_operation, application, {device.server_name: device}, "1234", locking=locking)


@pytest.mark.parametrize(
    ("locking", "operation_name"),
    [(True, "core_lock_device"), (False, "core_unlock_device")],
)
def test_standalone_lock_fallback_requires_matching_post_request_observation(
    monkeypatch,
    reset_cli_state,
    locking,
    operation_name,
):
    calls = []

    async def unavailable_daemon(device_name, pin, action):
        calls.append(("daemon", action))
        return None

    async def operation(device_ip, pin, key):
        calls.append(("operation", operation_name, device_ip, pin, key))
        return {
            "success": True,
            "already": False,
            "status": 0,
            "lock_state": 1 if locking else 0,
        }

    async def probe(device_ip, timeout=None):
        calls.append(("probe", device_ip))
        return _lock_observation(locking)

    monkeypatch.setattr(lock_commands, "_lock_via_daemon", unavailable_daemon)
    monkeypatch.setattr(lock_commands, "_get_lock_key", lambda: b"x" * 32)
    monkeypatch.setattr(lock_commands, operation_name, operation)

    result = _lock(SimpleNamespace(probe_lock_status=probe), locking)

    assert result.exit_code == 0, result.output
    assert calls == [
        ("daemon", "lock" if locking else "unlock"),
        ("operation", operation_name, "192.0.2.10", "1234", b"x" * 32),
        ("probe", "192.0.2.10"),
    ]


@pytest.mark.parametrize(
    ("locking", "operation_name", "action"),
    [(True, "core_lock_device", "lock"), (False, "core_unlock_device", "unlock")],
)
def test_standalone_lock_fallback_fails_closed_when_readback_is_missing(
    monkeypatch,
    reset_cli_state,
    locking,
    operation_name,
    action,
):
    async def operation(device_ip, pin, key):
        return {"success": True, "already": False, "status": 0, "lock_state": 1}

    async def probe(device_ip, timeout=None):
        raise CapabilityProbeTimeout("lock status readback timed out")

    monkeypatch.setattr(lock_commands, "_lock_via_daemon", lambda device_name, pin, action: _async_value(None))
    monkeypatch.setattr(lock_commands, "_get_lock_key", lambda: b"x" * 32)
    monkeypatch.setattr(lock_commands, operation_name, operation)

    result = _lock(SimpleNamespace(probe_lock_status=probe), locking)

    assert result.exit_code == 1
    assert f"Error: {action} failed: lock status readback was not reported" in result.output


def test_standalone_lock_fallback_rejects_opposite_observation(monkeypatch, reset_cli_state):
    async def operation(device_ip, pin, key):
        return {"success": True, "already": True, "status": 0x1102, "lock_state": 1}

    async def probe(device_ip, timeout=None):
        return _lock_observation(False)

    monkeypatch.setattr(lock_commands, "_lock_via_daemon", lambda device_name, pin, action: _async_value(None))
    monkeypatch.setattr(lock_commands, "_get_lock_key", lambda: b"x" * 32)
    monkeypatch.setattr(lock_commands, "core_lock_device", operation)

    result = _lock(SimpleNamespace(probe_lock_status=probe), True)

    assert result.exit_code == 1
    assert "lock operation did not reach the requested state" in result.output
    assert "already locked" not in result.output


def test_standalone_already_result_is_reported_only_after_matching_observation(monkeypatch, reset_cli_state):
    probed = False

    async def operation(device_ip, pin, key):
        return {"success": True, "already": True, "status": 0x1102, "lock_state": 1}

    async def probe(device_ip, timeout=None):
        nonlocal probed
        probed = True
        return _lock_observation(True)

    monkeypatch.setattr(lock_commands, "_lock_via_daemon", lambda device_name, pin, action: _async_value(None))
    monkeypatch.setattr(lock_commands, "_get_lock_key", lambda: b"x" * 32)
    monkeypatch.setattr(lock_commands, "core_lock_device", operation)

    result = _lock(SimpleNamespace(probe_lock_status=probe), True)

    assert result.exit_code == 0, result.output
    assert probed is True
    assert "already locked" in result.output
