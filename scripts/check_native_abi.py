from __future__ import annotations

import argparse
import ctypes
import re
import shutil
import subprocess
import sys
from pathlib import Path


def _exported_netaudio_symbols(library: Path) -> set[str] | None:
    if sys.platform == "win32":
        command = shutil.which("dumpbin")
        arguments = [command, "/NOLOGO", "/EXPORTS", str(library)] if command else []
    elif sys.platform == "darwin":
        command = shutil.which("nm")
        arguments = [command, "-gU", str(library)] if command else []
    else:
        command = shutil.which("nm")
        arguments = [command, "-D", "--defined-only", str(library)] if command else []
    if not arguments:
        return None
    result = subprocess.run(arguments, capture_output=True, text=True, check=True, timeout=30)
    return {symbol for symbol in re.findall(r"\b_?(netaudio_[a-z0-9_]+)\b", result.stdout) if symbol != "netaudio_core"}


def _source_abi(path: Path, pattern: str) -> int:
    match = re.search(pattern, path.read_text(encoding="utf-8"))
    if match is None:
        raise RuntimeError(f"could not find the ABI version in {path}")
    return int(match.group(1))


def check_abi(library_path: Path, header_path: Path, expected_abi: int) -> None:
    header = header_path.read_text(encoding="utf-8")
    declared_symbols = set(re.findall(r"\b(netaudio_[a-z0-9_]+)\s*\(", header))
    if not declared_symbols:
        raise RuntimeError("public header does not declare any netaudio functions")

    library = ctypes.CDLL(str(library_path.resolve()))
    missing = sorted(symbol for symbol in declared_symbols if not hasattr(library, symbol))
    if missing:
        raise RuntimeError(f"native library is missing public header symbols: {missing}")

    exported_symbols = _exported_netaudio_symbols(library_path)
    if exported_symbols is not None and exported_symbols != declared_symbols:
        missing_exports = sorted(declared_symbols - exported_symbols)
        undocumented_exports = sorted(exported_symbols - declared_symbols)
        raise RuntimeError(
            f"native/header symbol mismatch; missing={missing_exports}, undocumented={undocumented_exports}"
        )

    library.netaudio_abi_version.argtypes = []
    library.netaudio_abi_version.restype = ctypes.c_uint32
    actual_abi = library.netaudio_abi_version()
    if actual_abi != expected_abi:
        raise RuntimeError(f"native ABI is {actual_abi}, expected {expected_abi}")

    library.netaudio_status_name.argtypes = [ctypes.c_int32]
    library.netaudio_status_name.restype = ctypes.c_char_p
    statuses = [(name, int(value)) for name, value in re.findall(r"NETAUDIO_STATUS_([A-Z0-9_]+)\s*=\s*(\d+)", header)]
    if not statuses:
        raise RuntimeError("public header does not declare any status values")
    for status_name, value in statuses:
        native_name = library.netaudio_status_name(value)
        if native_name is None or native_name == b"unknown":
            raise RuntimeError(f"status {status_name}={value} has no native name")
    for invalid in (-1, max(value for _, value in statuses) + 1, 2**31 - 1):
        if library.netaudio_status_name(invalid) != b"unknown":
            raise RuntimeError(f"out-of-range status {invalid} did not map to unknown")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("library", type=Path)
    parser.add_argument(
        "--header",
        type=Path,
        default=Path("packages/netaudio-core/include/netaudio_core.h"),
    )
    parser.add_argument("--abi", type=int)
    parser.add_argument(
        "--python-binding",
        type=Path,
        default=Path("packages/netaudio/src/netaudio/core/binding.py"),
    )
    parser.add_argument(
        "--rust-source",
        type=Path,
        default=Path("packages/netaudio-core/src/ffi.rs"),
    )
    args = parser.parse_args()
    python_abi = _source_abi(args.python_binding, r"(?m)^ABI_VERSION\s*=\s*(\d+)\s*$")
    rust_abi = _source_abi(args.rust_source, r"NETAUDIO_ABI_VERSION:\s*u32\s*=\s*(\d+)")
    if python_abi != rust_abi:
        raise RuntimeError(f"Python expects ABI {python_abi}, but Rust declares ABI {rust_abi}")
    expected_abi = python_abi if args.abi is None else args.abi
    if expected_abi != python_abi:
        raise RuntimeError(f"requested ABI {expected_abi}, but sources declare ABI {python_abi}")
    check_abi(args.library, args.header, expected_abi)
    print(f"verified {args.library}: ABI {expected_abi}")


if __name__ == "__main__":
    main()
