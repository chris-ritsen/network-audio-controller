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


def _le32(data, off):
    return struct.unpack("<I", data[off : off + 4])[0]


class _CramfsInode:
    def __init__(self, data, offset):
        w0 = _le32(data, offset)
        w1 = _le32(data, offset + 4)
        w2 = _le32(data, offset + 8)
        self.mode = w0 & 0xFFFF
        self.uid = (w0 >> 16) & 0xFFFF
        self.size = w1 & 0xFFFFFF
        self.gid = (w1 >> 24) & 0xFF
        self.namelen = w2 & 0x3F
        self.offset = (w2 >> 6) & 0x3FFFFFF
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
    num_blocks = (inode.size + PAGE_SIZE - 1) // PAGE_SIZE
    ptr_start = inode.data_offset
    if ptr_start + num_blocks * 4 > len(data):
        return None
    block_ends = [_le32(data, ptr_start + i * 4) for i in range(num_blocks)]
    prev = ptr_start + num_blocks * 4
    result = b""
    for bend in block_ends:
        if bend > len(data) or bend <= prev:
            break
        chunk = data[prev:bend]
        try:
            result += zlib.decompress(chunk)
        except zlib.error:
            result += chunk
        prev = bend
    return result[: inode.size]


def _read_cramfs_directory_entry(data, position, end, inode):
    child = _CramfsInode(data, position)
    position += 12
    name_bytes = child.namelen * 4
    if name_bytes > 0 and position + name_bytes <= end and position + name_bytes <= len(data):
        name = _decode_cramfs_name(data[position : position + name_bytes])
        return child, name, position + name_bytes
    raise _CramfsExtractionError(f"invalid inode name length at directory offset 0x{inode.data_offset:X}")


def _require_unused_cramfs_destination(dest, full):
    if os.path.lexists(dest):
        raise _CramfsExtractionError(f"duplicate inode path: {full}")


def _extract_cramfs_directory(data, child, child_components, full, dest, root, verbose, visited):
    if verbose:
        typer.echo(f"{child.mode_str()} {full}/")
    _require_unused_cramfs_destination(dest, full)
    try:
        dest.mkdir()
    except OSError as exception:
        raise _CramfsExtractionError(f"could not create directory {full}: {exception}") from exception
    _cramfs_walk_directory(data, child, child_components, root, verbose, visited)


def _extract_cramfs_symlink(data, child, full, dest, root, verbose):
    target_data = _cramfs_extract_file(data, child)
    if target_data is None or len(target_data) != child.size:
        raise _CramfsExtractionError(f"invalid symlink data for {full}")
    try:
        target = target_data.decode("ascii")
    except UnicodeDecodeError as exception:
        raise _CramfsExtractionError(f"symlink target is not valid ASCII: {full}") from exception
    safe_target = _safe_cramfs_symlink_target(root, dest, target)
    if verbose:
        typer.echo(f"{child.mode_str()} {full} -> {safe_target}")
    _require_unused_cramfs_destination(dest, full)
    try:
        os.symlink(safe_target, dest)
    except OSError as exception:
        raise _CramfsExtractionError(f"could not create symlink {full}: {exception}") from exception


def _extract_cramfs_regular_file(data, child, full, dest, verbose):
    file_data = _cramfs_extract_file(data, child)
    if file_data is None or len(file_data) != child.size:
        raise _CramfsExtractionError(f"invalid file data for {full}")
    if verbose:
        typer.echo(f"{child.mode_str()} {full}  ({child.size} -> {len(file_data)} bytes)")
    _require_unused_cramfs_destination(dest, full)
    try:
        with open(dest, "xb") as output_file:
            output_file.write(file_data)
        if child.mode & 0o111:
            os.chmod(dest, child.mode & 0o7777)
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
        dest = _safe_cramfs_destination(root, child_components)

        if child.is_dir():
            _extract_cramfs_directory(data, child, child_components, full, dest, root, verbose, visited)
        elif child.is_lnk():
            _extract_cramfs_symlink(data, child, full, dest, root, verbose)
        elif child.is_reg():
            _extract_cramfs_regular_file(data, child, full, dest, verbose)
        elif verbose:
            typer.echo(f"{child.type_char()} {full}  (special)")

    visited.remove(directory_key)


def _cramfs_walk(data, inode, path, outdir, verbose=False):
    output = Path(outdir)
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
    parts = [p for p in target_path.strip("/").split("/") if p]
    current = root_inode
    for i, part in enumerate(parts):
        if not current.is_dir():
            return None
        pos = current.data_offset
        end = pos + current.size
        found = False
        while pos + 12 <= end and pos + 12 <= len(data):
            child = _CramfsInode(data, pos)
            pos += 12
            name_bytes = child.namelen * 4
            if name_bytes > 0 and pos + name_bytes <= len(data):
                name = data[pos : pos + name_bytes].rstrip(b"\x00").decode("ascii", errors="replace")
                pos += name_bytes
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
        hdr_len = struct.unpack(">I", data[4:8])[0]
        sections = _parse_sections(data, hdr_len)

    for sec in sections:
        blob = data[sec["file_offset"] : sec["file_offset"] + sec["size"]]
        for magic_bytes in (CRAMFS_MAGIC_LE, CRAMFS_MAGIC_BE):
            pos = blob.find(magic_bytes)
            if pos != -1:
                abs_off = sec["file_offset"] + pos
                is_be = magic_bytes == CRAMFS_MAGIC_BE
                endian = ">" if is_be else "<"
                fs_size = struct.unpack(f"{endian}I", data[abs_off + 4 : abs_off + 8])[0]
                return abs_off, fs_size, is_be

    for magic_bytes, is_be in [(CRAMFS_MAGIC_LE, False), (CRAMFS_MAGIC_BE, True)]:
        pos = data.find(magic_bytes)
        if pos != -1:
            endian = ">" if is_be else "<"
            fs_size = struct.unpack(f"{endian}I", data[pos + 4 : pos + 8])[0]
            return pos, fs_size, is_be

    return None, None, None


def _cramfs_to_le(data, cramfs_off, cramfs_size, is_be):
    blob = bytearray(data[cramfs_off : cramfs_off + cramfs_size])
    if is_be:
        for i in range(0, len(blob) - 3, 4):
            blob[i], blob[i + 1], blob[i + 2], blob[i + 3] = (
                blob[i + 3],
                blob[i + 2],
                blob[i + 1],
                blob[i],
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
        cwd = Path.cwd().resolve()
        protected_paths = {
            Path(resolved.anchor),
            home,
            cwd,
            *home.parents,
            *cwd.parents,
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
    with open(path, "rb") as f:
        data = f.read()

    cramfs_off, cramfs_size, is_be = _find_cramfs_in_dnt(data)
    if cramfs_off is None:
        typer.echo("No CramFS filesystem found in this firmware.", err=True)
        raise typer.Exit(code=1)

    endian_str = "big-endian" if is_be else "little-endian"
    typer.echo(
        f"CramFS at offset 0x{cramfs_off:X}, {cramfs_size:,} bytes ({endian_str})",
        err=True,
    )

    cramfs_data = _cramfs_to_le(data, cramfs_off, cramfs_size, is_be)

    magic = _le32(cramfs_data, 0)
    if magic != 0x28CD3D45:
        typer.echo(f"CramFS magic mismatch after conversion: 0x{magic:08X}", err=True)
        raise typer.Exit(code=1)

    fs_size = _le32(cramfs_data, 4)
    file_count = _le32(cramfs_data, 44)
    typer.echo(f"Filesystem: {fs_size:,} bytes, {file_count} files", err=True)

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
    with open(path, "rb") as f:
        data = f.read()

    cramfs_off, cramfs_size, is_be = _find_cramfs_in_dnt(data)
    if cramfs_off is None:
        typer.echo("No CramFS filesystem found.", err=True)
        raise typer.Exit(code=1)

    cramfs_data = _cramfs_to_le(data, cramfs_off, cramfs_size, is_be)
    root = _CramfsInode(cramfs_data, 0x40)

    for passwd_path in ("etc/passwd", "etc/shadow"):
        content = _cramfs_find_file(cramfs_data, root, passwd_path)
        if content is None:
            continue
        for line in content.decode("ascii", errors="replace").splitlines():
            if line.startswith("root:"):
                fields = line.split(":")
                pw_hash = fields[1]
                if pw_hash and pw_hash not in ("*", "!", "x", "!!"):
                    typer.echo(pw_hash)
                    return

    typer.echo("No root password hash found.", err=True)
    raise typer.Exit(code=1)
