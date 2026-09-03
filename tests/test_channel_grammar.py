import json
from types import SimpleNamespace

import pytest
import typer
from netaudio.cli import app as root_app
from netaudio.cli_support.selection import (
    ChannelReference,
    parse_channel_reference,
    parse_qualified_channel,
    resolve_channel,
)
from netaudio.commands.config import cli as config_commands
from netaudio.commands.preset import cli as preset_commands
from typer.testing import CliRunner

runner = CliRunner()


def _channel(number, name):
    return SimpleNamespace(number=number, name=name, friendly_name=None)


def _device():
    return SimpleNamespace(
        ipv4="192.0.2.5",
        name="AVIO",
        rx_channels={1: _channel(1, "Input-1"), 2: _channel(2, "Input-2")},
        server_name="avio.local.",
        tx_channels={1: _channel(1, "Output-1")},
    )


def test_config_show_prints_path_first_and_hides_secrets(monkeypatch, tmp_path):
    config_path = tmp_path / "config.toml"
    config_path.write_text('device_lock_key = "0123456789abcdef0123456789abcdef"\n\n[ui]\nicons = true\n')
    monkeypatch.setenv("NETAUDIO_CONFIG", str(config_path))

    result = runner.invoke(config_commands.top_app, ["show"])

    assert result.exit_code == 0
    lines = result.output.splitlines()
    assert lines[0] == f"path = {config_path.resolve()}"
    assert "device_lock_key = (set; hidden)" in lines
    assert "ui.icons = true" in lines
    assert "0123456789abcdef" not in result.output


def test_config_show_reports_missing_file(monkeypatch, tmp_path):
    config_path = tmp_path / "missing.toml"
    monkeypatch.setenv("NETAUDIO_CONFIG", str(config_path))

    result = runner.invoke(config_commands.top_app, ["show"])

    assert result.exit_code == 0
    assert result.output.splitlines()[0] == f"path = {config_path.resolve()}"
    assert "no configuration file" in result.output


def test_config_path_remains_available_when_ddm_configuration_is_invalid(monkeypatch, tmp_path):
    config_path = tmp_path / "config.toml"
    config_path.write_text("[ddm]\nservers = []\n")
    monkeypatch.setenv("NETAUDIO_CONFIG", str(config_path))

    result = runner.invoke(root_app, ["config", "path"])

    assert result.exit_code == 0, result.output
    assert result.output.strip() == str(config_path.resolve())


def test_parse_channel_reference_accepts_direction_prefix():
    assert parse_channel_reference("tx:1") == ChannelReference("tx", "1")
    assert parse_channel_reference("RX:Input-1") == ChannelReference("rx", "Input-1")
    assert parse_channel_reference(" rx: 2 ") == ChannelReference("rx", "2")


def test_parse_channel_reference_bare_name_uses_default_direction():
    assert parse_channel_reference("1") == ChannelReference(None, "1")
    assert parse_channel_reference("1", "rx") == ChannelReference("rx", "1")


@pytest.mark.parametrize("value", ["", "banana:1", "tx:", ":1"])
def test_parse_channel_reference_rejects_malformed_values(value, capsys):
    with pytest.raises(typer.Exit):
        parse_channel_reference(value)
    assert "accepted forms are tx:NUMBER, rx:NUMBER" in capsys.readouterr().err


def test_parse_qualified_channel_splits_device_and_applies_default_direction():
    assert parse_qualified_channel("tx:1@AVIO", "rx") == (ChannelReference("tx", "1"), "AVIO")
    assert parse_qualified_channel("1@AVIO", "rx") == (ChannelReference("rx", "1"), "AVIO")
    assert parse_qualified_channel("Input-1@avio.local.") == (ChannelReference(None, "Input-1"), "avio.local.")


@pytest.mark.parametrize("value", ["AVIO", "@AVIO", "1@"])
def test_parse_qualified_channel_rejects_missing_parts(value, capsys):
    with pytest.raises(typer.Exit):
        parse_qualified_channel(value)
    assert "tx:1@DEVICE, rx:1@DEVICE, or CHANNEL-NAME@DEVICE" in capsys.readouterr().err


def test_preset_list_reports_name_device_count_and_saved_time(monkeypatch, tmp_path):
    from netaudio.cli import OutputFormat, state

    preset_path = tmp_path / "stage.xml"
    preset_path.write_text(
        "<preset><name>Stage</name><device><name>A</name></device><device><name>B</name></device></preset>"
    )
    (tmp_path / "notes.txt").write_text("ignored")
    monkeypatch.setattr(preset_commands, "preset_directory", lambda: tmp_path)
    monkeypatch.setattr(state, "output_format", OutputFormat.json)

    result = runner.invoke(preset_commands.app, ["list"])

    assert result.exit_code == 0
    listing = json.loads(result.output)
    assert listing["stage"]["name"] == "Stage"
    assert listing["stage"]["device_count"] == 2
    assert listing["stage"]["devices"] == ["A", "B"]
    assert listing["stage"]["path"] == str(preset_path)
    assert len(listing["stage"]["saved"]) == 19


def test_preset_list_reports_empty_directory(monkeypatch, tmp_path):
    monkeypatch.setattr(preset_commands, "preset_directory", lambda: tmp_path / "presets")

    result = runner.invoke(preset_commands.app, ["list"])

    assert result.exit_code == 0
    assert "No presets in" in result.output


def test_resolve_channel_bare_name_searches_both_directions():
    assert resolve_channel(_device(), ChannelReference(None, "Input-2"))[0] == "rx"
    assert resolve_channel(_device(), ChannelReference(None, "Output-1"))[0] == "tx"


def test_resolve_channel_bare_number_is_ambiguous_when_both_directions_match(capsys):
    with pytest.raises(typer.Exit):
        resolve_channel(_device(), ChannelReference(None, "1"))
    assert "use tx:1 or rx:1" in capsys.readouterr().err


def test_resolve_channel_with_direction_reports_missing_channel(capsys):
    with pytest.raises(typer.Exit):
        resolve_channel(_device(), ChannelReference("tx", "2"))
    assert "TX channel '2' not found on AVIO" in capsys.readouterr().err


def test_resolve_preset_path_uses_directory_for_bare_names(monkeypatch, tmp_path):
    monkeypatch.setattr(preset_commands, "preset_directory", lambda: tmp_path)

    assert preset_commands.resolve_preset_path("stage", for_write=True) == tmp_path / "stage.xml"
    assert preset_commands.resolve_preset_path("stage.xml", for_write=True).name == "stage.xml"
    assert preset_commands.resolve_preset_path("/tmp/x.xml", for_write=False).is_absolute()
    assert preset_commands.resolve_preset_path("sub/stage", for_write=False).parts[-2:] == ("sub", "stage")
