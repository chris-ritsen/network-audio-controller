from __future__ import annotations

import argparse
import platform as host_platform
import re
import struct
import sys
import zipfile
from pathlib import Path


NATIVE_LIBRARY_NAMES = {
    "linux": "libnetaudio_core.so",
    "macos": "libnetaudio_core.dylib",
    "windows": "netaudio_core.dll",
}

MACHINE_CODES = {
    ("linux", "x86_64"): 62,
    ("linux", "aarch64"): 183,
    ("macos", "x86_64"): 0x01000007,
    ("macos", "arm64"): 0x0100000C,
    ("windows", "x86_64"): 0x8664,
}


def _host_policy() -> tuple[str, str]:
    platform_name = {"darwin": "macos", "linux": "linux", "win32": "windows"}.get(sys.platform)
    architecture = {
        "amd64": "x86_64",
        "x86_64": "x86_64",
        "aarch64": "aarch64",
        "arm64": "arm64",
    }.get(host_platform.machine().lower())
    if platform_name is None or architecture is None:
        raise RuntimeError(f"unsupported build host: {sys.platform}/{host_platform.machine()}")
    return platform_name, architecture


def _expected_tag(platform_name: str, architecture: str) -> str:
    if platform_name == "linux":
        return f"manylinux_2_28_{architecture}"
    if platform_name == "macos":
        return f"macosx_11_0_{architecture}"
    if platform_name == "windows" and architecture == "x86_64":
        return "win_amd64"
    raise ValueError(f"unsupported artifact policy: {platform_name}/{architecture}")


def _elf_machine(data: bytes) -> int:
    if data[:4] != b"\x7fELF" or len(data) < 20:
        raise RuntimeError("Linux native library is not an ELF file")
    byte_order = {1: "<", 2: ">"}.get(data[5])
    if byte_order is None:
        raise RuntimeError("ELF file has an invalid byte-order marker")
    return struct.unpack_from(f"{byte_order}H", data, 18)[0]


def _macho_details(data: bytes) -> tuple[int, tuple[int, int, int]]:
    if len(data) < 32:
        raise RuntimeError("macOS native library has a truncated Mach-O header")
    magic = data[:4]
    if magic == b"\xcf\xfa\xed\xfe":
        byte_order = "<"
    elif magic == b"\xfe\xed\xfa\xcf":
        byte_order = ">"
    elif magic in (b"\xca\xfe\xba\xbe", b"\xbe\xba\xfe\xca"):
        raise RuntimeError("wheel policy requires one thin Mach-O architecture per artifact")
    else:
        raise RuntimeError("macOS native library is not a 64-bit Mach-O file")

    cpu_type = struct.unpack_from(f"{byte_order}I", data, 4)[0]
    command_count = struct.unpack_from(f"{byte_order}I", data, 16)[0]
    offset = 32
    minimum_version: tuple[int, int, int] | None = None
    for _ in range(command_count):
        if offset + 8 > len(data):
            raise RuntimeError("Mach-O load-command table is truncated")
        command, size = struct.unpack_from(f"{byte_order}II", data, offset)
        if size < 8 or offset + size > len(data):
            raise RuntimeError("Mach-O load command has an invalid size")
        if command == 0x32 and size >= 16:
            encoded = struct.unpack_from(f"{byte_order}I", data, offset + 12)[0]
            minimum_version = _decode_apple_version(encoded)
        elif command == 0x24 and size >= 12:
            encoded = struct.unpack_from(f"{byte_order}I", data, offset + 8)[0]
            minimum_version = _decode_apple_version(encoded)
        offset += size

    if minimum_version is None:
        raise RuntimeError("Mach-O library does not declare a minimum macOS version")
    return cpu_type, minimum_version


def _decode_apple_version(encoded: int) -> tuple[int, int, int]:
    return (encoded >> 16, (encoded >> 8) & 0xFF, encoded & 0xFF)


def _pe_machine(data: bytes) -> int:
    if data[:2] != b"MZ" or len(data) < 64:
        raise RuntimeError("Windows native library is not a PE file")
    pe_offset = struct.unpack_from("<I", data, 0x3C)[0]
    if pe_offset + 6 > len(data) or data[pe_offset : pe_offset + 4] != b"PE\0\0":
        raise RuntimeError("Windows native library has an invalid PE header")
    return struct.unpack_from("<H", data, pe_offset + 4)[0]


def _verify_native_binary(data: bytes, platform_name: str, architecture: str) -> None:
    expected_machine = MACHINE_CODES[(platform_name, architecture)]
    if platform_name == "linux":
        actual_machine = _elf_machine(data)
        required_versions = {(int(major), int(minor)) for major, minor in re.findall(rb"GLIBC_(\d+)\.(\d+)", data)}
        if required_versions and max(required_versions) > (2, 28):
            version = ".".join(str(part) for part in max(required_versions))
            raise RuntimeError(f"Linux library requires GLIBC_{version}, above the manylinux 2.28 policy")
    elif platform_name == "macos":
        actual_machine, minimum_version = _macho_details(data)
        if minimum_version > (11, 0, 0):
            version = ".".join(str(part) for part in minimum_version)
            raise RuntimeError(f"macOS library requires macOS {version}, above the 11.0 wheel tag")
    else:
        actual_machine = _pe_machine(data)

    if actual_machine != expected_machine:
        raise RuntimeError(
            f"native library machine 0x{actual_machine:x} does not match "
            f"{platform_name}/{architecture} (0x{expected_machine:x})"
        )


def verify_wheel(wheel: Path, platform_name: str, architecture: str) -> None:
    expected_tag = _expected_tag(platform_name, architecture)
    filename_match = re.fullmatch(r".+-py3-none-([A-Za-z0-9_.]+)\.whl", wheel.name)
    if filename_match is None:
        raise RuntimeError(f"wheel does not have the expected py3-none platform filename: {wheel.name}")
    if filename_match.group(1) != expected_tag:
        raise RuntimeError(f"wheel platform tag is {filename_match.group(1)}, expected {expected_tag}")

    expected_library = f"netaudio/core/{NATIVE_LIBRARY_NAMES[platform_name]}"
    with zipfile.ZipFile(wheel) as archive:
        names = archive.namelist()
        native_members = [name for name in names if name.endswith((".so", ".dylib", ".dll", ".a", ".lib", ".rlib"))]
        if native_members != [expected_library]:
            raise RuntimeError(
                f"wheel must contain only {expected_library} as a native artifact; found {native_members}"
            )

        wheel_metadata = [name for name in names if name.endswith(".dist-info/WHEEL")]
        if len(wheel_metadata) != 1:
            raise RuntimeError(f"wheel must contain exactly one WHEEL metadata file; found {wheel_metadata}")
        metadata = archive.read(wheel_metadata[0]).decode("utf-8")
        if "Root-Is-Purelib: false" not in metadata:
            raise RuntimeError("native wheel incorrectly declares itself as a pure-Python wheel")
        if f"Tag: py3-none-{expected_tag}" not in metadata:
            raise RuntimeError("WHEEL metadata tag does not match the filename/platform policy")

        entry_points = [name for name in names if name.endswith(".dist-info/entry_points.txt")]
        if len(entry_points) != 1 or "netaudio = netaudio.cli:main" not in archive.read(entry_points[0]).decode(
            "utf-8"
        ):
            raise RuntimeError("wheel does not declare the netaudio console script")

        _verify_native_binary(archive.read(expected_library), platform_name, architecture)


def main() -> None:
    host_platform_name, host_architecture = _host_policy()
    parser = argparse.ArgumentParser()
    parser.add_argument("wheel", type=Path)
    parser.add_argument("--platform", choices=tuple(NATIVE_LIBRARY_NAMES), default=host_platform_name)
    parser.add_argument(
        "--architecture",
        choices=("x86_64", "aarch64", "arm64"),
        default=host_architecture,
    )
    args = parser.parse_args()
    verify_wheel(args.wheel, args.platform, args.architecture)
    print(f"verified {args.wheel.name}: {args.platform}/{args.architecture}")


if __name__ == "__main__":
    main()
