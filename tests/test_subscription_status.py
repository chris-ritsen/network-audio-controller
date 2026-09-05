from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from netaudio.dante.const import subscription_status_entry
from netaudio.dante.device_serializer import DanteDeviceSerializer
from netaudio.dante.subscription import DanteSubscription


FIXTURE = Path(__file__).parent / "fixtures/subscription/status-observations.json"
OBSERVATIONS = json.loads(FIXTURE.read_text())
MAPPINGS = [(code, group["api"]) for group in OBSERVATIONS["groups"] for code in group["codes"]]


def test_fixture_digest_and_sweep_coverage():
    provenance = json.loads(FIXTURE.with_suffix(".provenance.json").read_text())
    assert hashlib.sha256(FIXTURE.read_bytes()).hexdigest() == provenance["fixture_sha256"]
    assert sorted(code for code, _ in MAPPINGS) == list(range(256))


@pytest.mark.parametrize("code,api", MAPPINGS)
def test_every_observed_numeric_mapping(code, api):
    entry = subscription_status_entry(code, OBSERVATIONS["receiver_status_code"])
    assert entry["code"] == code
    assert entry["receiver_status_code"] == 257
    assert entry["status"] == api["status"]
    if api["status"] is None:
        assert entry["state"] == "unknown"
        assert entry["interpretation"] == "unknown"
    else:
        assert entry["observed_summary"] == api["summary"]
        assert entry["interpretation"] == "observed"
        assert entry["detail"] != api["statusMessage"]
        if api["summary"] == "CONNECTED":
            assert entry["state"] == "connected"
        if api["summary"] == "WARNING":
            assert entry["severity"] == "warning"
        if api["summary"] == "ERROR":
            assert entry["severity"] == "error"


@pytest.mark.parametrize("record", OBSERVATIONS["context_records"])
def test_code_one_cleanup_context(record):
    entry = subscription_status_entry(record["wire"]["subscription_status_code"], record["wire"]["rx_status_code"])
    assert entry["status"] == record["api"]["status"]
    assert entry["observed_summary"] == record["api"]["summary"]


@pytest.mark.parametrize("receiver", [None, 1, 0x0100, 0x0102, 0xFFFF])
def test_code_one_has_no_invented_precedence(receiver):
    entry = subscription_status_entry(1, receiver)
    assert entry["code"] == 1
    assert entry["receiver_status_code"] == receiver
    assert entry["status"] is None
    assert entry["interpretation"] == "receiver_context_required"


@pytest.mark.parametrize("code", [0x0100, 0x0101, 0x0109, 0x0140, 0x0171, 0x01FF, 0xFFFF])
def test_unknown_high_values_preserved_through_json_roundtrip(code):
    subscription = DanteSubscription()
    subscription.status_code = code
    subscription.rx_channel_status_code = 0x8123
    result = subscription.to_json()
    assert result["status"]["code"] == code
    assert result["status"]["status"] is None
    assert result["status"]["state"] == "unknown"
    restored = DanteDeviceSerializer._subscription_from_json(result)
    assert restored.status_code == code
    assert restored.rx_channel_status_code == 0x8123


@pytest.mark.parametrize("code", [4, 9, 10, 14])
def test_subscription_success_preserves_additional_warning(code):
    subscription = DanteSubscription()
    subscription.status_code = code
    subscription.rx_channel_status_code = 257
    subscription.status_message = ["The source channel name changed; check restoration after reboot."]
    result = subscription.to_json()
    assert result["status"]["state"] == "connected"
    assert result["status"]["severity"] == "ok"
    assert result["status_message"] == subscription.status_message
    assert result["rx_channel_status"]["code"] == 257
    restored = DanteDeviceSerializer._subscription_from_json(result)
    assert restored.status_message == subscription.status_message
    assert subscription.status_message[0] in restored.status_text()


def test_equal_raw_fields_are_still_separate():
    subscription = DanteSubscription()
    subscription.status_code = 1
    subscription.rx_channel_status_code = 1
    result = subscription.to_json()
    assert result["rx_channel_status"]["code"] == 1
    assert result["status"]["status"] is None


def test_absent_receiver_health_is_not_copied_from_subscription():
    subscription = DanteDeviceSerializer._subscription_from_json({"status": {"code": 1}})
    assert subscription.rx_channel_status_code is None
    assert subscription.to_json()["status"]["interpretation"] == "receiver_context_required"


@pytest.mark.parametrize("value", [-1, 65536, True, "9", 9.0, None])
def test_binding_rejects_values_that_would_be_truncated(value):
    with pytest.raises(ValueError):
        subscription_status_entry(value)
    if value is not None:
        with pytest.raises(ValueError):
            subscription_status_entry(9, value)


@pytest.mark.parametrize(
    "severity,expected",
    [("ok", "\U000f05e0"), ("error", "\U000f0159"), ("none", ""), ("unknown", "")],
)
def test_severity_icon_nerd_font_mode(severity, expected):
    from netaudio.cli import state
    from netaudio.icons import severity_icon

    previous = (state.icons, state.no_color)
    try:
        state.icons = True
        state.no_color = False
        assert severity_icon(severity) == expected
    finally:
        state.icons, state.no_color = previous


def test_severity_icon_color_shape_mode():
    from netaudio.cli import state
    from netaudio.icons import severity_icon

    previous = (state.icons, state.no_color)
    try:
        state.icons = False
        state.no_color = False
        rendered = severity_icon("warning")
        assert rendered == "\033[33m⚠\033[0m"
        assert severity_icon("none") == ""
    finally:
        state.icons, state.no_color = previous


def test_severity_icon_plain_mode_is_empty():
    from netaudio.cli import state
    from netaudio.icons import severity_icon

    previous = (state.icons, state.no_color)
    try:
        state.icons = False
        state.no_color = True
        for severity in ("ok", "info", "progress", "warning", "error", "none"):
            assert severity_icon(severity) == ""
    finally:
        state.icons, state.no_color = previous


def test_status_to_json_includes_status_severity_and_icon():
    from netaudio.cli import state
    from netaudio.dante.device_serializer import DanteDeviceSerializer

    previous = (state.icons, state.no_color)
    try:
        state.icons = False
        state.no_color = True
        rendered = DanteDeviceSerializer._status_to_json(0x09)
        assert rendered["code"] == 0x09
        assert rendered["status"] == "DYNAMIC"
        assert rendered["state"] == "connected"
        assert rendered["severity"] == "ok"
        assert rendered["label"] == "Subscribed (automatic flow)"
        assert rendered["icon"] == ""
        assert DanteDeviceSerializer._status_to_json(None) is None
    finally:
        state.icons, state.no_color = previous


@pytest.mark.parametrize("code,api", [(code, api) for code, api in MAPPINGS if api["status"] is not None])
def test_managed_identifier_classification_reuses_numeric_definitions(code, api):
    from netaudio.core import subscription_state_for_identifier

    entry = subscription_status_entry(code, 257)
    assert subscription_state_for_identifier(api["status"]) == entry["state"]


def test_managed_unknown_identifier_is_preserved_as_unknown():
    from netaudio.core import subscription_state_for_identifier

    assert subscription_state_for_identifier("FUTURE_STATUS") == "unknown"
    assert subscription_state_for_identifier(None) == "unknown"
    with pytest.raises(ValueError):
        subscription_state_for_identifier("DYNAMIC\0FUTURE_STATUS")


@pytest.mark.asyncio
async def test_cli_displays_success_label_and_additional_warning(monkeypatch, capsys):
    from netaudio.commands import subscription as commands
    from netaudio.dante.device import DanteDevice
    from netaudio.cli import state

    device = DanteDevice(server_name="receiver.local.")
    device.name = "receiver"
    subscription = DanteSubscription()
    subscription.status_code = 9
    subscription.rx_channel_status_code = 257
    subscription.rx_channel_name = "Input"
    subscription.rx_device_name = "receiver"
    subscription.tx_device_name = "source"
    subscription.tx_channel_name = "Output"
    subscription.status_message = ["Source name changed"]
    device.subscriptions = [subscription]
    monkeypatch.setattr(commands, "filter_devices", lambda devices: devices)
    monkeypatch.setattr(state, "no_color", True)
    from netaudio.cli import OutputFormat

    monkeypatch.setattr(state, "output_format", OutputFormat.plain)
    await commands.run_subscription_list(None, {device.server_name: device}, False)
    output = capsys.readouterr().out
    assert "Subscribed (automatic flow)" in output
    assert "Source name changed" in output


def test_managed_success_with_warning_survives_roundtrip_and_text():
    subscription = DanteSubscription()
    subscription.ddm_status = "DYNAMIC"
    subscription.ddm_summary = "WARNING"
    subscription.ddm_status_message = "Source name changed"
    restored = DanteDeviceSerializer._subscription_from_json(subscription.to_json())
    assert restored.status_text() == ("DYNAMIC", "WARNING", "Source name changed")


@pytest.mark.parametrize(
    "code,expected",
    [
        (9, "Subscribed (automatic flow)"),
        (0x1B, "Clock domain mismatch"),
        (0xFFFF, "Unknown subscription status"),
        (None, "status:unknown"),
    ],
)
def test_subscription_format_includes_status_only_when_verbose(code, expected):
    subscription = DanteSubscription()
    subscription.rx_channel_name = "Speaker"
    subscription.rx_device_name = "Receiver"
    subscription.tx_channel_name = "Mic"
    subscription.tx_device_name = "Transmitter"
    subscription.status_code = code

    assert subscription.format(verbose=True) == f"Speaker@Receiver <- Mic@Transmitter [{expected}]"
    assert subscription.format(verbose=False) == "Speaker@Receiver <- Mic@Transmitter"
