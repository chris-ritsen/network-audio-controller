import asyncio

import pytest

from netaudio.daemon.server import _record_jack_levels_from_config
from netaudio.host_audio.manager import HostAudioManager
from netaudio.monitoring.level_history import LevelHistory, history_text


class _Component:
    def __init__(self, name):
        self.name = name
        self.available = True
        self.reason = None
        self.connected_at = object()

    async def start(self):
        return None

    async def stop(self):
        return None

    async def shutdown_executor(self):
        return None


class _Watch:
    def start(self):
        return False

    def stop(self):
        return None


class _Recorder:
    def __init__(self):
        self.starts = []
        self.stops = 0

    def start(self, connection_key=None):
        self.starts.append(connection_key)

    def stop(self):
        self.stops += 1


class _Peers:
    async def stop(self):
        return None


def _manager(**options):
    manager = HostAudioManager(LevelHistory(), **options)
    manager.jack = _Component("jack")
    manager.pulse = _Component("pulse")
    manager._watch = _Watch()
    manager.recorder = _Recorder()
    manager.peers = _Peers()
    return manager


def test_jack_levels_are_not_recorded_unless_turned_on():
    manager = _manager()

    async def run():
        await manager.start()
        manager._changed("jack")

    asyncio.run(run())
    assert manager.recorder.starts == []


def test_turning_on_jack_level_recording_follows_the_jack_server():
    manager = _manager(record_jack_levels=True)

    async def run():
        await manager.start()
        manager.jack.connected_at = "reconnected"
        manager._changed("jack")
        await manager.stop()

    asyncio.run(run())
    assert len(manager.recorder.starts) == 2
    assert manager.recorder.starts[1] == "reconnected"
    assert manager.recorder.stops == 1


def test_the_recording_setting_must_be_true_or_false():
    assert _record_jack_levels_from_config({}) is False
    assert _record_jack_levels_from_config({"record_jack_levels": False}) is False
    assert _record_jack_levels_from_config({"record_jack_levels": True}) is True
    for value in ("yes", 1, "true"):
        with pytest.raises(ValueError, match="record_jack_levels"):
            _record_jack_levels_from_config({"record_jack_levels": value})


def test_history_says_recording_is_off_instead_of_reporting_silence():
    manager = _manager()
    summary = manager.level_history.summary([("jack", "system:capture_1")], 1000.0, 1060.0)
    note = manager.jack_history_note()
    assert note is not None
    summary["not_recording"] = note
    text = history_text(summary)
    assert "record_jack_levels" in text
    assert "no levels recorded" not in text


def test_history_recorded_before_recording_was_turned_off_is_still_shown():
    manager = _manager()
    manager.level_history.record("jack", 1000, {"system:capture_1": (-12.0, True)}, ["system:capture_1"])
    summary = manager.level_history.summary([("jack", "system:capture_1")], 990.0, 1010.0)
    summary["not_recording"] = manager.jack_history_note()
    assert history_text(summary).startswith("signal in 1 of")


def test_no_note_while_recording_is_on():
    assert _manager(record_jack_levels=True).jack_history_note() is None
