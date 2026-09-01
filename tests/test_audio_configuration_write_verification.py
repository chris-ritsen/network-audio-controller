import json

import pytest

from netaudio.commands import channel as channel_commands
from netaudio.commands import config as config_commands
from netaudio.commands import config_network as config_network_commands
from netaudio.commands import device as device_commands
from netaudio.dante.lock_status import LockStatusObservation
from tests.test_cli_write_verification import (
    FakeChannelDevice,
    FakeDevice,
    _install_context,
    runner,
)


def test_gain_getter_reports_configured_reference_level(monkeypatch):
    device = FakeChannelDevice(channel_reads="unused", gain_status=("input", [5]))
    _install_context(monkeypatch, channel_commands, {"avio.local.": device})

    result = runner.invoke(channel_commands.app, ["gain", "tx:1"])

    assert result.exit_code == 0
    assert "-10 dBV (level 5)" in result.output
    assert "level 2" not in result.output


def test_gain_getter_uses_the_device_reported_channel_side(monkeypatch):
    device = FakeChannelDevice(channel_reads="unused", gain_status=("input", [5]))
    _install_context(monkeypatch, channel_commands, {"avio.local.": device})

    result = runner.invoke(channel_commands.app, ["gain", "1"])

    assert result.exit_code == 0
    assert "-10 dBV (level 5)" in result.output


def test_gain_setter_reports_success_only_after_matching_readback(monkeypatch):
    device = FakeChannelDevice(channel_reads="unused", gain_status=("input", [5]))
    _install_context(monkeypatch, channel_commands, {"avio.local.": device})

    result = runner.invoke(channel_commands.app, ["gain", "tx:1", "3"])

    assert result.exit_code == 0
    assert "0 dBu (verified)" in result.output
    assert device.gain_levels == [3]


def test_gain_setter_rejects_mismatched_readback(monkeypatch):
    device = FakeChannelDevice(
        channel_reads="unused",
        gain_status=("input", [5]),
        gain_write_status=("input", [5]),
    )
    _install_context(monkeypatch, channel_commands, {"avio.local.": device})

    result = runner.invoke(channel_commands.app, ["gain", "tx:1", "3"])

    assert result.exit_code == 1
    assert "gain change was not applied" in result.output
    assert "Set input reference level" not in result.output


def test_gain_setter_rejects_missing_readback(monkeypatch):
    device = FakeChannelDevice(channel_reads="unused", gain_status=("input", [5]), gain_write_status=None)
    _install_context(monkeypatch, channel_commands, {"avio.local.": device})

    result = runner.invoke(channel_commands.app, ["gain", "tx:1", "3"])

    assert result.exit_code == 1
    assert "readback was unavailable" in result.output


def test_gain_rejects_fractional_level_before_discovery(monkeypatch):
    async def should_not_run():
        raise AssertionError("fractional gain must be rejected before command execution")

    monkeypatch.setattr(channel_commands, "_command_context", should_not_run)

    result = runner.invoke(channel_commands.app, ["gain", "tx:1", "1.9"])

    assert result.exit_code != 0
    assert "not a valid integer" in result.output


def test_channel_commands_reject_unknown_channel_type_before_sending(monkeypatch):
    device = FakeChannelDevice(channel_reads="unused")
    sent = _install_context(monkeypatch, channel_commands, {"avio.local.": device})

    result = runner.invoke(channel_commands.app, ["name", "banana:1", "New"])

    assert result.exit_code == 1
    assert "unknown channel direction 'banana'" in result.output
    assert sent == []


def test_sample_rate_all_aggregates_readback_failures(monkeypatch):
    good = FakeDevice("Good", settings={"sample_rate": 48000}, ipv4="192.0.2.10")
    stale = FakeDevice(
        "Stale",
        settings={"sample_rate": 44100},
        ipv4="192.0.2.11",
        supported_sample_rates=[44_100, 48_000],
    )
    sent = _install_context(
        monkeypatch,
        config_commands,
        {"good.local.": good, "stale.local.": stale},
    )
    result = runner.invoke(config_commands.app, ["sample-rate", "48000", "--all"])

    assert result.exit_code == 1
    assert len(sent) == 1
    assert "Good" in result.output
    assert "Unchanged" in result.output
    assert "Stale" in result.output
    assert "Changed but unverified" in result.output
    assert "device reports 44100 Hz instead of 48000 Hz" in result.output


def test_sample_rate_refuses_advertised_rate_without_proven_topology_capacity(monkeypatch):
    from netaudio.dante.sample_rate_topology import SampleRateTopologyUnsupportedError

    device = FakeDevice(
        "Future",
        settings={"sample_rate": 384_000},
        supported_sample_rates=[48_000, 384_000],
    )
    sent = _install_context(monkeypatch, config_commands, {"future.local.": device})

    async def refuse_unproven_capacity(*_arguments, **_options):
        raise SampleRateTopologyUnsupportedError("no proven Ferrofish A32 channel capacity is available for 384000 Hz")

    monkeypatch.setattr(
        config_commands,
        "change_sample_rate_with_command_sender",
        refuse_unproven_capacity,
    )

    result = runner.invoke(config_commands.app, ["sample-rate", "384000"])

    assert result.exit_code == 1
    assert "no proven Ferrofish A32 channel capacity" in result.output
    assert sent == []


def test_sample_rate_uses_active_readback_when_mutation_notification_is_absent(monkeypatch):
    device = FakeDevice(
        "Quiet Device",
        settings={"sample_rate": 96000},
        supported_sample_rates=[48000, 96000],
    )
    sent = _install_context(
        monkeypatch,
        config_commands,
        {"quiet.local.": device},
        notification_timeout=True,
    )

    result = runner.invoke(config_commands.app, ["sample-rate", "96000"])

    assert result.exit_code == 0
    assert "fresh readback already reports 96000 Hz; no write sent" in result.output
    assert sent == []


def test_encoding_rejects_value_missing_from_advertised_capabilities(monkeypatch):
    device = FakeDevice("AVIO", encoding=24, supported_encodings=[24])
    sent = _install_context(monkeypatch, config_commands, {"avio.local.": device})

    result = runner.invoke(config_commands.app, ["encoding", "16"])

    assert result.exit_code == 1
    assert "reports supported encoding values [24]" in result.output
    assert sent == []


def test_encoding_uses_advertised_nonstandard_future_value(monkeypatch):
    device = FakeDevice("Future", encoding=20, supported_encodings=[20, 24])
    sent = _install_context(monkeypatch, config_commands, {"future.local.": device})

    result = runner.invoke(config_commands.app, ["encoding", "20"])

    assert result.exit_code == 0
    assert "20-bit (verified)" in result.output
    assert sent[0][3] == {"expect_response": False}


def test_fractional_latency_verifies_rounded_nanoseconds(monkeypatch):
    device = FakeDevice("AVIO", settings={"active_latency_ns": 150_000})
    _install_context(monkeypatch, config_commands, {"avio.local.": device})
    result = runner.invoke(config_commands.app, ["latency", "0.15"])

    assert result.exit_code == 0
    assert "Set latency for AVIO: 0.15 ms (verified)" in result.output


def test_latency_get_uses_active_device_readback(monkeypatch):
    device = FakeDevice(
        "AVIO",
        settings={
            "default_latency_ns": 1_000_000,
            "configured_latency_ns": 250_000,
            "active_latency_ns": 150_000,
            "latency_ns": 150_000,
            "min_latency_ns": 150_000,
            "max_latency_ns": 21_333_334,
        },
    )
    device.latency = 99.0
    _install_context(monkeypatch, config_commands, {"avio.local.": device})

    result = runner.invoke(config_commands.app, ["latency"])

    assert result.exit_code == 0
    assert result.output.splitlines() == [
        "Active latency: 0.15 ms",
        "Configured latency: 0.25 ms",
        "Default latency: 1 ms",
        "Reported latency range: 0.15-21.3333 ms",
        "Latency options: 0.15, 0.25, 0.5, 1, 2, 5 ms",
    ]
    assert device.operations.settings_calls == 1


def test_latency_get_json_labels_milliseconds_and_raw_nanoseconds(monkeypatch):
    from netaudio.cli import OutputFormat, state

    device = FakeDevice(
        "AVIO",
        settings={
            "default_latency_ns": 1_000_000,
            "configured_latency_ns": 250_000,
            "active_latency_ns": 150_000,
            "latency_ns": 150_000,
            "min_latency_ns": 150_000,
            "max_latency_ns": 21_333_334,
        },
    )
    _install_context(monkeypatch, config_commands, {"avio.local.": device})
    monkeypatch.setattr(state, "output_format", OutputFormat.json)

    result = runner.invoke(config_commands.app, ["latency"])

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


def test_latency_get_surfaces_retained_configured_value_when_active_is_zero(monkeypatch):
    device = FakeDevice(
        "AVIO",
        settings={
            "configured_latency_ns": 250_000,
            "active_latency_ns": 0,
            "latency_ns": 0,
            "min_latency_ns": 1_000_000,
            "max_latency_ns": 20_312_500,
        },
    )
    _install_context(monkeypatch, config_commands, {"avio.local.": device})

    result = runner.invoke(config_commands.app, ["latency"])

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
            "latency_ns": 0,
            "min_latency_ns": 1_000_000,
            "max_latency_ns": 20_312_500,
        },
    )
    _install_context(monkeypatch, config_commands, {"avio.local.": device})
    monkeypatch.setattr(state, "output_format", OutputFormat.json)

    result = runner.invoke(config_commands.app, ["latency"])

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
    _install_context(monkeypatch, config_commands, {"synthetic.local.": device})
    monkeypatch.setattr(state, "output_format", OutputFormat.json)

    result = runner.invoke(config_commands.app, ["latency"])

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["active_latency_ms"] == 0.75
    assert payload["active_latency_ns"] == 750_000
    assert payload["active_latency_within_reported_range"] is True
    assert payload["active_latency_is_standard_choice"] is False
    assert 0.75 not in payload["latency_options_ms"]


def test_latency_get_fails_when_every_reported_latency_value_is_unavailable(monkeypatch):
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
    _install_context(monkeypatch, config_commands, {"unavailable.local.": device})

    result = runner.invoke(config_commands.app, ["latency"])

    assert result.exit_code == 1
    assert "latency readback was unavailable" in result.output


def test_clock_source_get_is_not_implemented(monkeypatch):
    sent = _install_context(monkeypatch, config_commands, {"avio.local.": FakeDevice("AVIO")})

    result = runner.invoke(config_commands.app, ["clock-source"])

    assert result.exit_code == 0
    assert result.output.strip() == "not implemented"
    assert sent == []


def test_clock_source_set_is_not_implemented(monkeypatch):
    sent = _install_context(monkeypatch, config_commands, {"avio.local.": FakeDevice("AVIO")})

    result = runner.invoke(config_commands.app, ["clock-source", "1"])

    assert result.exit_code == 1
    assert "not implemented" in result.output
    assert sent == []


def test_latency_does_not_treat_configured_value_as_applied(monkeypatch):
    device = FakeDevice(
        "AVIO",
        settings={
            "configured_latency_ns": 150_000,
            "active_latency_ns": 1_000_000,
            "latency_ns": 1_000_000,
        },
    )
    _install_context(monkeypatch, config_commands, {"avio.local.": device})

    result = runner.invoke(config_commands.app, ["latency", "0.15"])

    assert result.exit_code == 1
    assert "1000000 instead of 150000" in result.output
    assert "Set latency for AVIO" not in result.output


def test_latency_range_does_not_block_device_verified_nonstandard_value(monkeypatch):
    device = FakeDevice(
        "AVIO",
        min_latency=1.0,
        max_latency=5.0,
        settings={"active_latency_ns": 250_000},
    )
    sent = _install_context(monkeypatch, config_commands, {"avio.local.": device})

    result = runner.invoke(config_commands.app, ["latency", "0.25"])

    assert result.exit_code == 0
    assert "Set latency for AVIO: 0.25 ms (verified)" in result.output
    assert len(sent) == 1


@pytest.mark.parametrize("value", ["-0.1", "nan", "inf"])
def test_latency_rejects_nonfinite_or_negative_values_before_sending(monkeypatch, value):
    device = FakeDevice("AVIO", settings={"active_latency_ns": 0})
    sent = _install_context(monkeypatch, config_commands, {"avio.local.": device})

    result = runner.invoke(config_commands.app, ["latency", "--", value])

    assert result.exit_code == 1
    assert "finite, nonnegative" in result.output
    assert sent == []


def test_aes67_verifies_configured_state_not_current_state(monkeypatch):
    device = FakeDevice("AVIO", aes67=True)
    device.aes67_current = False
    sent = _install_context(monkeypatch, config_commands, {"avio.local.": device})
    result = runner.invoke(config_commands.app, ["aes67", "on"])

    assert result.exit_code == 0
    assert device.operations.aes67_calls == 1
    assert "AES67 configured state for AVIO: on (verified)" in result.output
    assert sent[0][3] == {"expect_response": False, "repeat": 3, "interval_ms": 100}


def test_aes67_rejects_known_unsupported_device_without_sending(monkeypatch):
    device = FakeDevice("LX-DANTE", aes67_supported=False)
    sent = _install_context(monkeypatch, config_commands, {"lx.local.": device})

    result = runner.invoke(config_commands.app, ["aes67", "on"])

    assert result.exit_code == 1
    assert "does not support AES67 configuration" in result.output
    assert sent == []


def test_encoding_is_verified_from_reported_status(monkeypatch):
    device = FakeDevice("AVIO", encoding=24, supported_encodings=[24])
    sent = _install_context(monkeypatch, config_commands, {"avio.local.": device})

    result = runner.invoke(config_commands.app, ["encoding", "24"])

    assert result.exit_code == 0
    assert "Set encoding for AVIO: 24-bit (verified)" in result.output
    assert sent[0][3] == {"expect_response": False}


@pytest.mark.parametrize(
    ("arguments", "expected", "send_kwargs"),
    [
        (
            ["preferred-leader", "on"],
            "Preferred leader change requested for AVIO: on; not verified",
            {"expect_response": False, "repeat": 3, "interval_ms": 500},
        ),
        (
            ["interface", "dhcp"],
            "Interface change requested for AVIO: dhcp; not verified",
            {"expect_response": False},
        ),
    ],
)
def test_commands_without_readback_use_requested_language(monkeypatch, arguments, expected, send_kwargs):
    device = FakeDevice("AVIO")
    command_module = config_network_commands if arguments[0] == "interface" else config_commands
    sent = _install_context(monkeypatch, command_module, {"avio.local.": device})

    result = runner.invoke(config_commands.app, arguments)

    assert result.exit_code == 0
    assert expected in result.output
    assert sent[0][3] == send_kwargs


@pytest.mark.parametrize(
    ("command", "action"),
    [("set", "lock"), ("clear", "unlock")],
)
def test_daemon_lock_failure_reports_protocol_status(monkeypatch, command, action):
    async def failed_daemon_request(pin, requested_action):
        assert pin == "1234"
        assert requested_action == action
        return {"success": False, "status": 0x1101}

    monkeypatch.setattr(device_commands, "_lock_via_daemon", failed_daemon_request)

    result = runner.invoke(device_commands.lock_app, [command, "1234"])

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


@pytest.mark.parametrize(
    ("command", "operation_name", "expected_is_locked"),
    [("set", "core_lock_device", True), ("clear", "core_unlock_device", False)],
)
def test_standalone_lock_fallback_requires_matching_post_request_observation(
    monkeypatch,
    command,
    operation_name,
    expected_is_locked,
):
    calls = []

    async def unavailable_daemon(pin, action):
        calls.append(("daemon", action))
        return None

    async def operation(device_ip, pin, key):
        calls.append(("operation", operation_name, device_ip, pin, key))
        return {
            "success": True,
            "already": False,
            "status": 0,
            "lock_state": 1 if expected_is_locked else 0,
        }

    async def probe(device_ip):
        calls.append(("probe", device_ip))
        return _lock_observation(expected_is_locked)

    monkeypatch.setattr(device_commands, "_lock_via_daemon", unavailable_daemon)
    monkeypatch.setattr(device_commands, "_get_lock_key", lambda: b"x" * 32)
    monkeypatch.setattr(device_commands, "_resolve_lock_ip", lambda: _async_value("192.0.2.10"))
    monkeypatch.setattr(device_commands, operation_name, operation)
    monkeypatch.setattr(device_commands, "_probe_lock_status_once", probe)

    result = runner.invoke(device_commands.lock_app, [command, "1234"])

    assert result.exit_code == 0
    assert calls == [
        ("daemon", "lock" if expected_is_locked else "unlock"),
        ("operation", operation_name, "192.0.2.10", "1234", b"x" * 32),
        ("probe", "192.0.2.10"),
    ]


@pytest.mark.parametrize(
    ("command", "operation_name", "action"),
    [("set", "core_lock_device", "lock"), ("clear", "core_unlock_device", "unlock")],
)
def test_standalone_lock_fallback_fails_closed_when_readback_is_missing(
    monkeypatch,
    command,
    operation_name,
    action,
):
    async def operation(device_ip, pin, key):
        return {"success": True, "already": False, "status": 0, "lock_state": 1}

    monkeypatch.setattr(device_commands, "_lock_via_daemon", lambda pin, requested_action: _async_value(None))
    monkeypatch.setattr(device_commands, "_get_lock_key", lambda: b"x" * 32)
    monkeypatch.setattr(device_commands, "_resolve_lock_ip", lambda: _async_value("192.0.2.10"))
    monkeypatch.setattr(device_commands, operation_name, operation)
    monkeypatch.setattr(device_commands, "_probe_lock_status_once", lambda device_ip: _async_value(None))

    result = runner.invoke(device_commands.lock_app, [command, "1234"])

    assert result.exit_code == 1
    assert f"Error: {action} failed: lock status readback was not reported" in result.output


def test_standalone_lock_fallback_rejects_opposite_observation(monkeypatch):
    async def operation(device_ip, pin, key):
        return {"success": True, "already": True, "status": 0x1102, "lock_state": 1}

    monkeypatch.setattr(device_commands, "_lock_via_daemon", lambda pin, action: _async_value(None))
    monkeypatch.setattr(device_commands, "_get_lock_key", lambda: b"x" * 32)
    monkeypatch.setattr(device_commands, "_resolve_lock_ip", lambda: _async_value("192.0.2.10"))
    monkeypatch.setattr(device_commands, "core_lock_device", operation)
    monkeypatch.setattr(
        device_commands,
        "_probe_lock_status_once",
        lambda device_ip: _async_value(_lock_observation(False)),
    )

    result = runner.invoke(device_commands.lock_app, ["set", "1234"])

    assert result.exit_code == 1
    assert "lock operation did not reach the requested state" in result.output
    assert "already locked" not in result.output


def test_standalone_already_result_is_reported_only_after_matching_observation(monkeypatch):
    probed = False

    async def operation(device_ip, pin, key):
        return {"success": True, "already": True, "status": 0x1102, "lock_state": 1}

    async def probe(device_ip):
        nonlocal probed
        probed = True
        return _lock_observation(True)

    monkeypatch.setattr(device_commands, "_lock_via_daemon", lambda pin, action: _async_value(None))
    monkeypatch.setattr(device_commands, "_get_lock_key", lambda: b"x" * 32)
    monkeypatch.setattr(device_commands, "_resolve_lock_ip", lambda: _async_value("192.0.2.10"))
    monkeypatch.setattr(device_commands, "core_lock_device", operation)
    monkeypatch.setattr(device_commands, "_probe_lock_status_once", probe)

    result = runner.invoke(device_commands.lock_app, ["set", "1234"])

    assert result.exit_code == 0
    assert probed is True
    assert "already locked" in result.output
