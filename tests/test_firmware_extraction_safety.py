import os
import stat
import struct
import zlib
from pathlib import Path

import pytest

from netaudio.commands.firmware import (
    _CramfsExtractionError,
    _cramfs_walk,
    _safe_cramfs_destination,
    _safe_cramfs_symlink_target,
)


class _RootDirectory:
    def __init__(self, size):
        self.data_offset = 0
        self.size = size

    def is_dir(self):
        return True


def _directory_record(name: str, mode: int = stat.S_IFREG | 0o644) -> bytes:
    encoded_name = name.encode("ascii")
    padded_name = encoded_name + bytes((-len(encoded_name)) % 4)
    name_words = len(padded_name) // 4
    inode = struct.pack("<III", mode, 0, name_words)
    return inode + padded_name


def _directory_record_with_payload(name: str, mode: int, payload: bytes) -> bytes:
    encoded_name = name.encode("ascii")
    padded_name = encoded_name + bytes((-len(encoded_name)) % 4)
    name_words = len(padded_name) // 4
    pointer_offset = 12 + len(padded_name)
    compressed = zlib.compress(payload)
    block_end = pointer_offset + 4 + len(compressed)
    inode = struct.pack(
        "<III",
        mode,
        len(payload),
        (pointer_offset // 4) << 6 | name_words,
    )
    return inode + padded_name + struct.pack("<I", block_end) + compressed


@pytest.mark.parametrize(
    "name",
    [
        ".",
        "..",
        "/abs",
        "../x",
        "a/b",
        r"a\b",
        "C:escape",
        "bad\x00name",
    ],
)
def test_cramfs_walk_rejects_unsafe_inode_names(tmp_path, name):
    output = tmp_path / "rootfs"
    output.mkdir()
    data = _directory_record(name)

    with pytest.raises(_CramfsExtractionError):
        _cramfs_walk(data, _RootDirectory(len(data)), "", output)

    assert list(output.iterdir()) == []


def test_cramfs_walk_preserves_empty_regular_files(tmp_path):
    output = tmp_path / "rootfs"
    output.mkdir()
    data = _directory_record("empty")

    _cramfs_walk(data, _RootDirectory(len(data)), "", output)

    extracted = output / "empty"
    assert extracted.is_file()
    assert extracted.read_bytes() == b""


def test_cramfs_destination_rejects_symlinked_parent_escape(tmp_path):
    output = tmp_path / "rootfs"
    outside = tmp_path / "outside"
    output.mkdir()
    outside.mkdir()
    (output / "linked").symlink_to(outside, target_is_directory=True)

    with pytest.raises(_CramfsExtractionError, match="escapes extraction root"):
        _safe_cramfs_destination(output.resolve(), ("linked", "payload"))


def test_cramfs_symlink_target_rejects_relative_escape(tmp_path):
    output = tmp_path / "rootfs"
    output.mkdir()
    destination = output / "link"

    with pytest.raises(_CramfsExtractionError, match="escapes extraction root"):
        _safe_cramfs_symlink_target(output.resolve(), destination, "../outside")


def test_cramfs_walk_rejects_escaping_symlink_from_image(tmp_path):
    output = tmp_path / "rootfs"
    output.mkdir()
    data = _directory_record_with_payload(
        "escape",
        stat.S_IFLNK | 0o777,
        b"../outside",
    )
    directory_size = 12 + len("escape".encode("ascii") + b"\x00\x00")

    with pytest.raises(_CramfsExtractionError, match="escapes extraction root"):
        _cramfs_walk(data, _RootDirectory(directory_size), "", output)

    assert list(output.iterdir()) == []


def test_cramfs_symlink_target_rewrites_absolute_rootfs_path(tmp_path):
    output = tmp_path / "rootfs"
    destination = output / "usr" / "bin" / "tool"
    destination.parent.mkdir(parents=True)

    target = _safe_cramfs_symlink_target(
        output.resolve(),
        destination,
        "/bin/busybox",
    )

    assert not os.path.isabs(target)
    assert (destination.parent / target).resolve() == output / "bin" / "busybox"


@pytest.mark.parametrize("target", ["bad\x00target", r"C:\outside", r"..\outside"])
def test_cramfs_symlink_target_rejects_ambiguous_platform_paths(tmp_path, target):
    output = tmp_path / "rootfs"
    output.mkdir()

    with pytest.raises(_CramfsExtractionError):
        _safe_cramfs_symlink_target(output.resolve(), output / "link", target)


def test_cramfs_walk_rejects_symlink_extraction_root(tmp_path):
    real_output = tmp_path / "real-rootfs"
    linked_output = tmp_path / "linked-rootfs"
    real_output.mkdir()
    linked_output.symlink_to(real_output, target_is_directory=True)

    with pytest.raises(_CramfsExtractionError, match="must not be a symlink"):
        _cramfs_walk(b"", _RootDirectory(0), "", linked_output)
