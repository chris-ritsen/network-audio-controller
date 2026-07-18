import pytest
import typer

from netaudio.commands.report import _filter_report_diagnostics, _format_report, report_create


SAMPLE_DIAGNOSTICS = {
    "version": "1.2.3",
    "python": "3.12.2",
    "platform": "Darwin 24 arm64",
    "device_count": 2,
    "devices": [
        {
            "name": "Alpha Box",
            "server_name": "alpha-box.local.",
            "ipv4": "192.168.1.10",
            "mac_address": "AA:BB:CC:DD:EE:01",
            "manufacturer": "Audinate",
            "dante_model": "AVIO USB",
            "model": "AVIO USB",
            "dante_model_id": "0x1001",
            "firmware_version": "1.0.0",
            "software_version": "1.0.1",
            "sample_rate": 48000,
            "encoding": "pcm24",
            "aes67_current": False,
            "aes67_configured": False,
            "preferred_leader": False,
            "is_locked": True,
            "interfaces": [
                {
                    "mode": "dynamic",
                    "ip_address": "192.168.1.10",
                    "netmask": "255.255.255.0",
                    "gateway": "192.168.1.1",
                    "dns_server": "192.168.1.1",
                }
            ],
            "channels": {
                "transmitters": {"1": {"name": "Alpha TX 1"}},
                "receivers": {"1": {"name": "Alpha RX 1"}},
            },
            "subscriptions": [],
        },
        {
            "name": "Bravo Unit",
            "server_name": "bravo-unit.local.",
            "ipv4": "192.168.1.20",
            "mac_address": "AA:BB:CC:DD:EE:02",
            "manufacturer": "Focusrite",
            "dante_model": "RedNet",
            "model": "RedNet",
            "dante_model_id": "0x1002",
            "firmware_version": "2.0.0",
            "software_version": "2.0.1",
            "sample_rate": 96000,
            "encoding": "pcm24",
            "aes67_current": True,
            "aes67_configured": True,
            "preferred_leader": True,
            "is_locked": True,
            "interfaces": [
                {
                    "mode": "static",
                    "ip_address": "192.168.1.20",
                    "netmask": "255.255.255.0",
                    "gateway": "192.168.1.1",
                    "dns_server": "192.168.1.1",
                }
            ],
            "channels": {
                "transmitters": {"1": {"name": "Bravo TX 1"}},
                "receivers": {"1": {"name": "Bravo RX 1"}},
            },
            "subscriptions": [
                {
                    "rx_channel_name": "Bravo RX 1",
                    "rx_device_name": "Bravo Unit",
                    "tx_channel_name": "Alpha TX 1",
                    "tx_device_name": "Alpha Box",
                }
            ],
        },
    ],
}


def test_filter_report_diagnostics_matches_name_server_ip_and_mac():
    assert _filter_report_diagnostics(SAMPLE_DIAGNOSTICS, "bravo")["device_count"] == 1
    assert _filter_report_diagnostics(SAMPLE_DIAGNOSTICS, "bravo")["devices"][0]["name"] == "Bravo Unit"

    assert _filter_report_diagnostics(SAMPLE_DIAGNOSTICS, "alpha-box.local")["device_count"] == 1
    assert _filter_report_diagnostics(SAMPLE_DIAGNOSTICS, "192.168.1.20")["devices"][0]["name"] == "Bravo Unit"

    filtered = _filter_report_diagnostics(SAMPLE_DIAGNOSTICS, "aabbccddee01")
    assert filtered["device_count"] == 1
    assert filtered["devices"][0]["name"] == "Alpha Box"


def test_format_report_applies_device_filter_to_body_and_json():
    body = _format_report(
        SAMPLE_DIAGNOSTICS,
        "full",
        "Routing failure while testing",
        device_filter="bravo",
    )

    assert "Devices: 1" in body
    assert "**Bravo Unit**" in body
    assert "**Alpha Box**" not in body
    assert '"name": "Bravo Unit"' in body
    assert '"name": "Alpha Box"' not in body


def test_report_create_rejects_empty_description(capsys):
    with pytest.raises(typer.Exit) as exception:
        report_create(
            title="Routing issue",
            description="",
            level="minimal",
            device_filter="",
            session=None,
            dry_run=True,
        )

    assert exception.value.exit_code == 1
    captured = capsys.readouterr()
    assert "description must not be empty" in captured.err


def test_report_create_rejects_unknown_level(capsys):
    with pytest.raises(typer.Exit) as exception:
        report_create(
            title="Routing issue",
            description="Something broke",
            level="everything",
            device_filter="",
            session=None,
            dry_run=True,
        )

    assert exception.value.exit_code == 1
    captured = capsys.readouterr()
    assert "level must be one of" in captured.err
