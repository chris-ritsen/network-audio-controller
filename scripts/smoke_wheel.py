from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

import netaudio
from netaudio import core
from netaudio.core import binding


def main() -> None:
    if os.environ.get("NETAUDIO_CORE_LIB"):
        raise RuntimeError("NETAUDIO_CORE_LIB must be unset so the bundled library is actually tested")

    core_dir = Path(binding.__file__).resolve().parent
    bundled_libraries = [core_dir / name for name in binding._library_names() if (core_dir / name).is_file()]
    if len(bundled_libraries) != 1:
        found = ", ".join(str(path) for path in bundled_libraries) or "none"
        raise RuntimeError(f"wheel must contain exactly one native library for this platform; found {found}")

    if not core.available():
        raise RuntimeError("installed wheel could not load its bundled netaudio-core library")

    library = binding.require()
    loaded_path = Path(library._name).resolve()
    if loaded_path != bundled_libraries[0].resolve():
        raise RuntimeError(f"loaded native library {loaded_path}, expected bundled library {bundled_libraries[0]}")

    abi_version = library.netaudio_abi_version()
    if abi_version != binding.ABI_VERSION:
        raise RuntimeError(f"wheel native ABI mismatch: library={abi_version}, Python={binding.ABI_VERSION}")

    packet = core.build_command({"command": "channel_count", "transaction_id": 0x1234})
    expected = bytes.fromhex("27ff000a123410000000")
    if packet != expected:
        raise RuntimeError(f"installed native builder returned {packet.hex()}, expected {expected.hex()}")

    executable = shutil.which("netaudio")
    if executable is None:
        raise RuntimeError("installed wheel did not provide the netaudio console script")
    cli = subprocess.run(
        [executable, "--help"],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    if cli.returncode != 0:
        output = (cli.stdout + cli.stderr).strip()
        raise RuntimeError(f"installed netaudio --help failed with status {cli.returncode}: {output}")
    if "Usage:" not in cli.stdout:
        raise RuntimeError("installed netaudio --help did not produce a usage message")

    print(f"netaudio {netaudio.__version__}: native ABI {abi_version}, packet {packet.hex()}")


if __name__ == "__main__":
    main()
