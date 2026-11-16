from __future__ import annotations

import os
import stat
import struct
import zlib
from pathlib import Path, PurePosixPath, PureWindowsPath

import typer

from netaudio.commands.firmware.constants import CRAMFS_MAGIC_BE, CRAMFS_MAGIC_LE
from netaudio.commands.firmware.parser import _parse_sections

PAGE_SIZE = 4096


def _le32(data, offset):
    return struct.unpack("<I", data[offset : offset + 4])[0]


class _CramfsInode:
    def __init__(self, data, offset):
        first_word = _le32(data, offset)
        second_word = _le32(data, offset + 4)
        third_word = _le32(data, offset + 8)
        self.mode = first_word & 0xFFFF
        self.uid = (first_word >> 16) & 0xFFFF
        self.size = second_word & 0xFFFFFF
        self.gid = (second_word >> 24) & 0xFF
        self.namelen = third_word & 0x3F
        self.offset = (third_word >> 6) & 0x3FFFFFF
        self.data_offset = self.offset * 4

    def is_dir(self):
        return stat.S_ISDIR(self.mode)

    def is_reg(self):
        return stat.S_ISREG(self.mode)

    def is_lnk(self):
        return stat.S_ISLNK(self.mode)

    def type_char(self):
        if self.is_dir():
            return "d"
        if self.is_reg():
            return "-"
        if self.is_lnk():
            return "l"
        if stat.S_ISCHR(self.mode):
            return "c"
        if stat.S_ISBLK(self.mode):
            return "b"
        if stat.S_ISFIFO(self.mode):
            return "p"
        return "?"

    def mode_str(self):
        chars = self.type_char()
        for shift in (6, 3, 0):
            bits = (self.mode >> shift) & 7
            chars += "r" if bits & 4 else "-"
            chars += "w" if bits & 2 else "-"
            chars += "x" if bits & 1 else "-"
        return chars


class _CramfsExtractionError(ValueError):
    pass


def _validate_cramfs_name(name: str) -> str:
    if not name:
        raise _CramfsExtractionError("empty inode name")
    if "\x00" in name:
        raise _CramfsExtractionError("inode name contains a NUL byte")
    if name in (".", ".."):
        raise _CramfsExtractionError(f"unsafe inode name: {name!r}")
    if "/" in name or "\\" in name:
        raise _CramfsExtractionError(f"inode name contains a path separator: {name!r}")
    if PurePosixPath(name).is_absolute() or PureWindowsPath(name).drive:
        raise _CramfsExtractionError(f"absolute inode name is not allowed: {name!r}")
    return name


def _decode_cramfs_name(raw_name: bytes) -> str:
    raw_name = raw_name.rstrip(b"\x00")
    if b"\x00" in raw_name:
        raise _CramfsExtractionError("inode name contains a NUL byte")
    try:
        name = raw_name.decode("ascii")
    except UnicodeDecodeError as exception:
        raise _CramfsExtractionError("inode name is not valid ASCII") from exception
    return _validate_cramfs_name(name)


def _require_cramfs_containment(root: Path, candidate: Path, description: str) -> Path:
    try:
        resolved = candidate.resolve(strict=False)
    except (OSError, RuntimeError) as exception:
        raise _CramfsExtractionError(f"could not resolve {description}: {exception}") from exception

    try:
        resolved.relative_to(root)
    except ValueError as exception:
        raise _CramfsExtractionError(f"{description} escapes extraction root: {candidate}") from exception
    return resolved


def _safe_cramfs_destination(root: Path, components: tuple[str, ...]) -> Path:
    for component in components:
        _validate_cramfs_name(component)
    destination = root.joinpath(*components)
    _require_cramfs_containment(root, destination, "inode path")
    return destination


def _safe_cramfs_symlink_target(root: Path, destination: Path, target: str) -> str:
    if not target:
        raise _CramfsExtractionError(f"empty symlink target for {destination}")
    if "\x00" in target:
        raise _CramfsExtractionError(f"symlink target contains a NUL byte: {destination}")
    posix_target = PurePosixPath(target)
    has_windows_drive = PureWindowsPath(target).drive or any(PureWindowsPath(part).drive for part in posix_target.parts)
    if "\\" in target or has_windows_drive:
        raise _CramfsExtractionError(f"symlink target uses an unsafe platform-specific path: {target!r}")

    if posix_target.is_absolute():
        candidate = root.joinpath(*posix_target.parts[1:])
        _require_cramfs_containment(root, candidate, "symlink target")
        return os.path.relpath(candidate, start=destination.parent)

    candidate = destination.parent.joinpath(*posix_target.parts)
    _require_cramfs_containment(root, candidate, "symlink target")
    return target


def _cramfs_extract_file(data, inode):
    if inode.size == 0:
        return b""
    block_count = (inode.size + PAGE_SIZE - 1) // PAGE_SIZE
    pointer_start = inode.data_offset
    if pointer_start + block_count * 4 > len(data):
        return None
    block_ends = [_le32(data, pointer_start + index * 4) for index in range(block_count)]
    previous_end = pointer_start + block_count * 4
    result = b""
    for block_end in block_ends:
        if block_end > len(data) or block_end <= previous_end:
            break
        chunk = data[previous_end:block_end]
        try:
            result += zlib.decompress(chunk)
        except zlib.error:
            result += chunk
        previous_end = block_end
    return result[: inode.size]


def _read_cramfs_directory_entry(data, position, end, inode):
    child = _CramfsInode(data, position)
    position += 12
    name_bytes = child.namelen * 4
    if name_bytes > 0 and position + name_bytes <= end and position + name_bytes <= len(data):
        name = _decode_cramfs_name(data[position : position + name_bytes])
        return child, name, position + name_bytes
    raise _CramfsExtractionError(f"invalid inode name length at directory offset 0x{inode.data_offset:X}")


def _require_unused_cramfs_destination(destination, full):
    if os.path.lexists(destination):
        raise _CramfsExtractionError(f"duplicate inode path: {full}")


def _extract_cramfs_directory(data, child, child_components, full, destination, root, verbose, visited):
    if verbose:
        typer.echo(f"{child.mode_str()} {full}/")
    _require_unused_cramfs_destination(destination, full)
    try:
        destination.mkdir()
    except OSError as exception:
        raise _CramfsExtractionError(f"could not create directory {full}: {exception}") from exception
    _cramfs_walk_directory(data, child, child_components, root, verbose, visited)


def _extract_cramfs_symlink(data, child, full, destination, root, verbose):
    target_data = _cramfs_extract_file(data, child)
    if target_data is None or len(target_data) != child.size:
        raise _CramfsExtractionError(f"invalid symlink data for {full}")
    try:
        target = target_data.decode("ascii")
    except UnicodeDecodeError as exception:
        raise _CramfsExtractionError(f"symlink target is not valid ASCII: {full}") from exception
    safe_target = _safe_cramfs_symlink_target(root, destination, target)
    if verbose:
        typer.echo(f"{child.mode_str()} {full} -> {safe_target}")
    _require_unused_cramfs_destination(destination, full)
    try:
        os.symlink(safe_target, destination)
    except OSError as exception:
        raise _CramfsExtractionError(f"could not create symlink {full}: {exception}") from exception


def _extract_cramfs_regular_file(data, child, full, destination, verbose):
    file_data = _cramfs_extract_file(data, child)
    if file_data is None or len(file_data) != child.size:
        raise _CramfsExtractionError(f"invalid file data for {full}")
    if verbose:
        typer.echo(f"{child.mode_str()} {full}  ({child.size} -> {len(file_data)} bytes)")
    _require_unused_cramfs_destination(destination, full)
    try:
        with open(destination, "xb") as output_file:
            output_file.write(file_data)
        if child.mode & 0o111:
            os.chmod(destination, child.mode & 0o7777)
    except OSError as exception:
        raise _CramfsExtractionError(f"could not write file {full}: {exception}") from exception


def _cramfs_walk_directory(data, inode, components, root, verbose, visited):
    if not inode.is_dir():
        return

    directory_key = (inode.data_offset, inode.size)
    if directory_key in visited:
        raise _CramfsExtractionError(f"recursive directory inode at offset 0x{inode.data_offset:X}")
    visited.add(directory_key)

    position = inode.data_offset
    end = position + inode.size
    while position + 12 <= end and position + 12 <= len(data):
        child, name, position = _read_cramfs_directory_entry(data, position, end, inode)
        child_components = (*components, name)
        full = "/" + "/".join(child_components)
        destination = _safe_cramfs_destination(root, child_components)

        if child.is_dir():
            _extract_cramfs_directory(data, child, child_components, full, destination, root, verbose, visited)
        elif child.is_lnk():
            _extract_cramfs_symlink(data, child, full, destination, root, verbose)
        elif child.is_reg():
            _extract_cramfs_regular_file(data, child, full, destination, verbose)
        elif verbose:
            typer.echo(f"{child.type_char()} {full}  (special)")

    visited.remove(directory_key)


def _cramfs_walk(data, inode, path, output_directory, verbose=False):
    output = Path(output_directory)
    if output.is_symlink():
        raise _CramfsExtractionError(f"extraction root must not be a symlink: {output}")
    try:
        root = output.resolve(strict=True)
    except (OSError, RuntimeError) as exception:
        raise _CramfsExtractionError(f"could not resolve extraction root {output}: {exception}") from exception
    if not root.is_dir():
        raise _CramfsExtractionError(f"extraction root is not a directory: {root}")

    if path:
        raw_components = tuple(component for component in path.split("/") if component)
        components = tuple(_validate_cramfs_name(component) for component in raw_components)
    else:
        components = ()
    _cramfs_walk_directory(data, inode, components, root, verbose, set())


def _cramfs_find_file(data, root_inode, target_path):
    parts = [part for part in target_path.strip("/").split("/") if part]
    current = root_inode
    for index, part in enumerate(parts):
        if not current.is_dir():
            return None
        position = current.data_offset
        end = position + current.size
        found = False
        while position + 12 <= end and position + 12 <= len(data):
            child = _CramfsInode(data, position)
            position += 12
            name_bytes = child.namelen * 4
            if name_bytes > 0 and position + name_bytes <= len(data):
                name = data[position : position + name_bytes].rstrip(b"\x00").decode("ascii", errors="replace")
                position += name_bytes
            else:
                break
            if name == part:
                current = child
                found = True
                break
        if not found:
            return None
    if current.is_reg():
        return _cramfs_extract_file(data, current)
    return None


def _find_cramfs_in_dnt(data):
    sections = []
    if len(data) >= 0x50 and data[:4] == b"AUDI":
        header_length = struct.unpack(">I", data[4:8])[0]
        sections = _parse_sections(data, header_length)

    for section in sections:
        blob = data[section["file_offset"] : section["file_offset"] + section["size"]]
        for magic_bytes in (CRAMFS_MAGIC_LE, CRAMFS_MAGIC_BE):
            position = blob.find(magic_bytes)
            if position != -1:
                absolute_offset = section["file_offset"] + position
                is_big_endian = magic_bytes == CRAMFS_MAGIC_BE
                endian = ">" if is_big_endian else "<"
                filesystem_size = struct.unpack(f"{endian}I", data[absolute_offset + 4 : absolute_offset + 8])[0]
                return absolute_offset, filesystem_size, is_big_endian

    for magic_bytes, is_big_endian in [(CRAMFS_MAGIC_LE, False), (CRAMFS_MAGIC_BE, True)]:
        position = data.find(magic_bytes)
        if position != -1:
            endian = ">" if is_big_endian else "<"
            filesystem_size = struct.unpack(f"{endian}I", data[position + 4 : position + 8])[0]
            return position, filesystem_size, is_big_endian

    return None, None, None


def _cramfs_to_le(data, cramfs_offset, cramfs_size, is_big_endian):
    blob = bytearray(data[cramfs_offset : cramfs_offset + cramfs_size])
    if is_big_endian:
        for offset in range(0, len(blob) - 3, 4):
            blob[offset], blob[offset + 1], blob[offset + 2], blob[offset + 3] = (
                blob[offset + 3],
                blob[offset + 2],
                blob[offset + 1],
                blob[offset],
            )
    return bytes(blob)


def _prepare_rootfs_output(output: Path, force: bool = False) -> None:
    output_exists = output.exists() or output.is_symlink()
    if output_exists and not force:
        typer.echo(
            f"Error: output path already exists: {output}. Use --force to replace it.",
            err=True,
        )
        raise typer.Exit(code=1)

    if output_exists:
        resolved = output.expanduser().resolve()
        home = Path.home().resolve()
        working_directory = Path.cwd().resolve()
        protected_paths = {
            Path(resolved.anchor),
            home,
            working_directory,
            *home.parents,
            *working_directory.parents,
        }
        if resolved in protected_paths:
            typer.echo(f"Error: refusing to replace unsafe output path: {resolved}", err=True)
            raise typer.Exit(code=1)

        if output.is_symlink() or not output.is_dir():
            output.unlink()
        else:
            import shutil

            shutil.rmtree(output)

    output.mkdir(parents=True, exist_ok=False)


def firmware_rootfs(
    path: Path = typer.Argument(..., help=".dnt file containing a CramFS rootfs."),
    output: Path = typer.Argument(..., help="Output directory for extracted filesystem."),
    verbose: bool = typer.Option(False, "-v", "--verbose", help="Print each extracted file."),
    force: bool = typer.Option(
        False,
        "--force",
        help="Delete and replace an existing output path.",
    ),
):
    """Extract the Linux root filesystem from a .dnt firmware image."""
    with open(path, "rb") as file:
        data = file.read()

    cramfs_offset, cramfs_size, is_big_endian = _find_cramfs_in_dnt(data)
    if cramfs_offset is None:
        typer.echo("No CramFS filesystem found in this firmware.", err=True)
        raise typer.Exit(code=1)

    endian_description = "big-endian" if is_big_endian else "little-endian"
    typer.echo(
        f"CramFS at offset 0x{cramfs_offset:X}, {cramfs_size:,} bytes ({endian_description})",
        err=True,
    )

    cramfs_data = _cramfs_to_le(data, cramfs_offset, cramfs_size, is_big_endian)

    magic = _le32(cramfs_data, 0)
    if magic != 0x28CD3D45:
        typer.echo(f"CramFS magic mismatch after conversion: 0x{magic:08X}", err=True)
        raise typer.Exit(code=1)

    filesystem_size = _le32(cramfs_data, 4)
    file_count = _le32(cramfs_data, 44)
    typer.echo(f"Filesystem: {filesystem_size:,} bytes, {file_count} files", err=True)

    root = _CramfsInode(cramfs_data, 0x40)

    _prepare_rootfs_output(output, force=force)

    try:
        _cramfs_walk(cramfs_data, root, "", output, verbose=verbose)
    except _CramfsExtractionError as exception:
        typer.echo(f"Error: could not safely extract CramFS: {exception}", err=True)
        raise typer.Exit(code=1) from exception
    typer.echo(f"Extracted to {output}", err=True)


def firmware_password(
    path: Path = typer.Argument(..., help=".dnt file containing a CramFS rootfs."),
):
    """Extract the root password hash from a .dnt firmware image.

    Reads /etc/passwd and /etc/shadow from the embedded CramFS filesystem.
    Brooklyn II devices use DES crypt (hashcat mode 1500, max 8 characters).
    """
    with open(path, "rb") as file:
        data = file.read()

    cramfs_offset, cramfs_size, is_big_endian = _find_cramfs_in_dnt(data)
    if cramfs_offset is None:
        typer.echo("No CramFS filesystem found.", err=True)
        raise typer.Exit(code=1)

    cramfs_data = _cramfs_to_le(data, cramfs_offset, cramfs_size, is_big_endian)
    root = _CramfsInode(cramfs_data, 0x40)

    for account_file_path in ("etc/passwd", "etc/shadow"):
        content = _cramfs_find_file(cramfs_data, root, account_file_path)
        if content is None:
            continue
        for line in content.decode("ascii", errors="replace").splitlines():
            if line.startswith("root:"):
                fields = line.split(":")
                password_hash = fields[1]
                if password_hash and password_hash not in ("*", "!", "x", "!!"):
                    typer.echo(password_hash)
                    return

    typer.echo("No root password hash found.", err=True)
    raise typer.Exit(code=1)
