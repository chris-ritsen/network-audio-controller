import os

import pytest

collect_ignore = [] if os.environ.get("NETAUDIO_LAB") else ["lab"]


@pytest.fixture(autouse=True)
def isolate_default_configuration(monkeypatch, tmp_path):
    monkeypatch.setenv("NETAUDIO_CONFIG", str(tmp_path / "config.toml"))
