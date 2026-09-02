import sys

import pytest

from netaudio import _common as common_module
from netaudio.cli import error_message, main
from netaudio.common.app_config import settings


def _install_failing_daemon(monkeypatch):
    async def get_devices_from_daemon():
        raise RuntimeError("ConMon export is unavailable on 192.0.2.10: device returned an empty response")

    monkeypatch.setattr(common_module, "get_devices_from_daemon", get_devices_from_daemon)


def test_unhandled_command_failure_renders_as_plain_error_line(monkeypatch, capsys):
    _install_failing_daemon(monkeypatch)
    monkeypatch.setattr(settings, "debug", False)
    monkeypatch.setattr(sys, "argv", ["netaudio", "-n", "avio-usb-1", "device", "name"])

    with pytest.raises(SystemExit) as exit_info:
        main()

    captured = capsys.readouterr()
    assert exit_info.value.code == 1
    assert captured.err.strip().splitlines()[-1] == (
        "Error: ConMon export is unavailable on 192.0.2.10: device returned an empty response"
    )
    assert "Traceback" not in captured.err
    assert "RuntimeError" not in captured.err


def test_debug_flag_lets_the_traceback_through(monkeypatch):
    _install_failing_daemon(monkeypatch)
    monkeypatch.setattr(settings, "debug", False)
    monkeypatch.setattr(sys, "argv", ["netaudio", "--debug", "-n", "avio-usb-1", "device", "name"])

    with pytest.raises(RuntimeError, match="ConMon export is unavailable"):
        main()


def test_error_message_falls_back_to_the_exception_class_name():
    assert error_message(RuntimeError("")) == "RuntimeError"
    assert error_message(ValueError("bad value")) == "bad value"
