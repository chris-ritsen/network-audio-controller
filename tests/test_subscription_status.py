from __future__ import annotations

import pytest

from netaudio.dante.const import (
    SUBSCRIPTION_STATUS_ACTIVE,
    SUBSCRIPTION_STATUS_SEVERITY,
    subscription_status_entry,
)


def test_active_set_is_connected_states_only():
    assert sorted(SUBSCRIPTION_STATUS_ACTIVE) == [0x04, 0x09, 0x0A, 0x0E]


@pytest.mark.parametrize(
    "code,status,label,severity",
    [
        (0x00, "NOT_SUBSCRIBED", "Not subscribed", "none"),
        (0x01, "UNRESOLVED", "Unresolved", "warning"),
        (0x02, "RESOLVED", "Resolved", "progress"),
        (0x03, "RESOLVE_FAILED", "Resolve failed", "error"),
        (0x04, "SUBSCRIBED_SELF", "Subscribed (self)", "ok"),
        (0x07, "UNLABELED", "", "none"),
        (0x09, "SUBSCRIBED_UNICAST", "Subscribed (unicast)", "ok"),
        (0x0A, "SUBSCRIBED_MULTICAST", "Subscribed (multicast)", "ok"),
        (0x0E, "MANUALLY_CONFIGURED", "Manually configured", "ok"),
        (0x1B, "CLOCK_DOMAIN_MISMATCH", "Clock domain mismatch", "error"),
        (0x26, "RX_ENCRYPTION_UNSUPPORTED", "Encryption unsupported (Rx)", "error"),
    ],
)
def test_known_codes_have_expected_status_label_and_severity(code, status, label, severity):
    entry = subscription_status_entry(code)
    assert entry["status"] == status
    assert entry["label"] == label
    assert entry["severity"] == severity
    assert SUBSCRIPTION_STATUS_SEVERITY[code] == severity


def test_unknown_range_falls_back():
    for code in range(0x28, 0x40):
        entry = subscription_status_entry(code)
        assert entry["state"] == "unknown"
        assert entry["severity"] == "error"
        assert entry["label"] == "Subscription failed"


def test_high_byte_falls_back_to_low_byte():
    entry = subscription_status_entry(0x0109)
    assert entry["label"] == "Subscribed (unicast)"

    unknown = subscription_status_entry(0x0140)
    assert unknown["label"] == "Subscription failed"


@pytest.mark.parametrize(
    "code,expected",
    [(9, "Subscribed (unicast)"), (0x1B, "Clock domain mismatch"), (0xFFFF, "status:65535"), (None, "status:unknown")],
)
def test_subscription_format_includes_status_only_when_verbose(code, expected):
    from netaudio.dante.subscription import DanteSubscription

    subscription = DanteSubscription()
    subscription.rx_channel_name = "Speaker"
    subscription.rx_device_name = "Receiver"
    subscription.tx_channel_name = "Mic"
    subscription.tx_device_name = "Transmitter"
    subscription.status_code = code

    assert subscription.format(verbose=True) == f"Speaker@Receiver <- Mic@Transmitter [{expected}]"
    assert subscription.format(verbose=False) == "Speaker@Receiver <- Mic@Transmitter"


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
        assert rendered["status"] == "SUBSCRIBED_UNICAST"
        assert rendered["state"] == "connected"
        assert rendered["severity"] == "ok"
        assert rendered["label"] == "Subscribed (unicast)"
        assert rendered["icon"] == ""
        assert DanteDeviceSerializer._status_to_json(None) is None
    finally:
        state.icons, state.no_color = previous
