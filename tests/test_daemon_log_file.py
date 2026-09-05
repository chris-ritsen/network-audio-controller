from __future__ import annotations

import tempfile
from pathlib import Path

from netaudio.daemon import service_install
from netaudio.daemon.log_file import LOG_SIZE_LIMIT_BYTES, daemon_log_path, truncate_when_oversized


class TestDaemonLogPath:
    def test_log_lives_under_the_temporary_directory(self):
        assert daemon_log_path().parent.parent == Path(tempfile.gettempdir())

    def test_service_install_spawns_into_the_temporary_directory(self):
        assert service_install.spawn_log_path() == daemon_log_path()

    def test_no_log_is_written_under_the_home_directory(self):
        assert Path.home() not in daemon_log_path().parents


class TestTruncation:
    def test_missing_log_is_not_truncated(self, tmp_path):
        assert truncate_when_oversized(tmp_path / "absent.log") is False

    def test_small_log_is_left_alone(self, tmp_path):
        path = tmp_path / "daemon.log"
        path.write_bytes(b"x" * 64)
        assert truncate_when_oversized(path, limit_bytes=128) is False
        assert path.stat().st_size == 64

    def test_oversized_log_is_emptied_in_place(self, tmp_path):
        path = tmp_path / "daemon.log"
        path.write_bytes(b"x" * 256)
        inode = path.stat().st_ino
        assert truncate_when_oversized(path, limit_bytes=128) is True
        assert path.stat().st_size == 0
        assert path.stat().st_ino == inode

    def test_default_limit_is_bounded(self):
        assert 0 < LOG_SIZE_LIMIT_BYTES <= 32 * 1024 * 1024
