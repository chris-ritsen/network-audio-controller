import os
import stat
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import pytest
from typer.testing import CliRunner

import netaudio.common.key_extract as key_extract_module
import netaudio.common.lock_key_qr as lock_key_qr_module
from netaudio.commands.key import app
from netaudio.common.config_loader import get_config_value
from netaudio.common.lock_key_qr import lock_key_import_uri, normalize_lock_key, write_lock_key_qr

VALID_LOCK_KEY = "0123456789ABCDEF0123456789ABCDEF"
NORMALIZED_LOCK_KEY = VALID_LOCK_KEY.lower()


def test_lock_key_import_uri_is_versioned_and_action_specific():
    import_uri = lock_key_import_uri(VALID_LOCK_KEY)
    parsed_uri = urlsplit(import_uri)

    assert import_uri == ("netaudio://lock-key/import?version=1&key=0123456789abcdef0123456789abcdef")
    assert parsed_uri.scheme == "netaudio"
    assert parsed_uri.netloc == "lock-key"
    assert parsed_uri.path == "/import"
    assert parse_qs(parsed_uri.query) == {"version": ["1"], "key": [NORMALIZED_LOCK_KEY]}


@pytest.mark.parametrize(
    "invalid_lock_key",
    ["", "0" * 31, "0" * 33, "g" * 32, "0" * 16 + "-" + "0" * 16],
)
def test_lock_key_validation_rejects_non_wire_values(invalid_lock_key):
    with pytest.raises(ValueError, match="32-character hex"):
        normalize_lock_key(invalid_lock_key)


def test_write_lock_key_qr_creates_private_png(tmp_path):
    output_path = tmp_path / "lock-key.png"

    result_path = write_lock_key_qr(VALID_LOCK_KEY, output_path)

    assert result_path == output_path.resolve()
    assert output_path.read_bytes().startswith(b"\x89PNG\r\n\x1a\n")
    if os.name != "nt":
        assert stat.S_IMODE(output_path.stat().st_mode) == 0o600


def test_write_lock_key_qr_uses_png_for_temporary_output(monkeypatch, tmp_path):
    temporary_path = tmp_path / "generated-lock-key.png"
    monkeypatch.setattr(
        lock_key_qr_module.tempfile,
        "mkstemp",
        lambda prefix, suffix: (
            os.open(temporary_path, os.O_CREAT | os.O_RDWR, 0o600),
            str(temporary_path),
        ),
    )

    output_path = write_lock_key_qr(VALID_LOCK_KEY)

    assert output_path.suffix == ".png"
    assert output_path.read_bytes().startswith(b"\x89PNG\r\n\x1a\n")
    if os.name != "nt":
        assert stat.S_IMODE(output_path.stat().st_mode) == 0o600


def test_write_lock_key_qr_allows_explicit_svg(tmp_path):
    output_path = tmp_path / "lock-key.svg"

    result_path = write_lock_key_qr(VALID_LOCK_KEY, output_path)

    assert result_path == output_path.resolve()
    assert output_path.read_text().startswith("<?xml")
    assert "<svg" in output_path.read_text()


def test_write_lock_key_qr_rejects_misleading_extension(tmp_path):
    with pytest.raises(ValueError, match=r"must end in \.png or \.svg"):
        write_lock_key_qr(VALID_LOCK_KEY, tmp_path / "lock-key.jpg")


@pytest.mark.parametrize(
    ("platform", "executable"),
    [("darwin", "open"), ("linux", "xdg-open"), ("win32", "explorer")],
)
def test_open_path_uses_platform_viewer(monkeypatch, tmp_path, platform, executable):
    output_path = tmp_path / "lock-key.svg"
    completed_commands = []
    monkeypatch.setattr(lock_key_qr_module.sys, "platform", platform)
    monkeypatch.setattr(
        lock_key_qr_module.subprocess,
        "run",
        lambda command, check: completed_commands.append((command, check)),
    )

    lock_key_qr_module.open_path(output_path)

    assert completed_commands == [([executable, str(output_path)], True)]


def test_qr_command_uses_configured_key(monkeypatch, tmp_path):
    config_path = tmp_path / "config.toml"
    config_path.write_text(f'device_lock_key = "{NORMALIZED_LOCK_KEY}"\n')
    monkeypatch.setenv("NETAUDIO_CONFIG", str(config_path))
    output_path = tmp_path / "configured-key.png"

    result = CliRunner().invoke(app, ["qr", "--output", str(output_path)])

    assert result.exit_code == 0
    assert result.stdout == f"{output_path}\n"
    assert output_path.exists()


def test_qr_command_requires_configured_key(monkeypatch, tmp_path):
    config_path = tmp_path / "missing.toml"
    monkeypatch.setenv("NETAUDIO_CONFIG", str(config_path))

    result = CliRunner().invoke(app, ["qr"])

    assert result.exit_code == 1
    assert f"No device_lock_key in {config_path}" in result.output


def test_extract_can_save_generate_and_open_qr_code(monkeypatch, tmp_path):
    binary_path = tmp_path / "libDanteController.dylib"
    binary_path.write_bytes(b"Dante Controller")
    config_path = tmp_path / "config.toml"
    output_path = tmp_path / "extracted-key.png"
    monkeypatch.setenv("NETAUDIO_CONFIG", str(config_path))
    monkeypatch.setattr(
        key_extract_module,
        "extract_key_from_binary",
        lambda requested_path: NORMALIZED_LOCK_KEY.encode("ascii") if requested_path == binary_path else None,
    )
    opened_paths = []
    monkeypatch.setattr(lock_key_qr_module, "open_path", opened_paths.append)

    result = CliRunner().invoke(
        app,
        [
            "extract",
            "--path",
            str(binary_path),
            "--save",
            "--qr",
            "--output",
            str(output_path),
            "--open",
        ],
    )

    assert result.exit_code == 0
    assert NORMALIZED_LOCK_KEY in result.output
    assert f"Saved to {config_path}" in result.output
    assert f"QR code: {output_path}" in result.output
    assert output_path.exists()
    assert opened_paths == [output_path.resolve()]
    assert get_config_value("device_lock_key") == (NORMALIZED_LOCK_KEY, config_path)
