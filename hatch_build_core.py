from __future__ import annotations

import json
import os
import platform
import shutil
import subprocess
import sys
from pathlib import Path

from hatchling.builders.hooks.plugin.interface import BuildHookInterface
from packaging.tags import platform_tags


def package_library_destination(root: Path, library_name: str) -> Path:
    return Path(root) / "packages" / "netaudio" / "src" / "netaudio" / "core" / library_name


def git_directory(root: Path) -> Path | None:
    marker = Path(root) / ".git"
    if marker.is_dir():
        return marker
    if marker.is_file():
        content = marker.read_text().strip()
        if content.startswith("gitdir:"):
            return (Path(root) / content.removeprefix("gitdir:").strip()).resolve()
    return None


def git_revision(root: Path) -> str | None:
    directory = git_directory(root)
    if directory is None:
        return None
    head = (directory / "HEAD").read_text().strip()
    if not head.startswith("ref:"):
        return head
    reference = head.removeprefix("ref:").strip()
    common = directory
    common_marker = directory / "commondir"
    if common_marker.is_file():
        common = (directory / common_marker.read_text().strip()).resolve()
    for base in (directory, common):
        loose = base / reference
        if loose.is_file():
            return loose.read_text().strip()
    packed = common / "packed-refs"
    if packed.is_file():
        for line in packed.read_text().splitlines():
            if line.endswith(f" {reference}"):
                return line.split(" ", 1)[0]
    return None


def write_build_information(root: Path) -> Path:
    destination = Path(root) / "packages" / "netaudio" / "src" / "netaudio" / "_build_info.json"
    information = {"git_revision": git_revision(Path(root))}
    destination.write_text(json.dumps({key: value for key, value in information.items() if value}) + "\n")
    return destination


def install_built_library(root: Path, built_library: Path) -> Path:
    destination = package_library_destination(root, built_library.name)
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(built_library, destination)
    return destination


class CoreLibraryBuildHook(BuildHookInterface):
    PLUGIN_NAME = "custom"

    def initialize(self, version, build_data):
        crate_dir = Path(self.root) / "packages" / "netaudio-core"
        cargo = shutil.which("cargo")
        if cargo is not None:
            self._build_core(crate_dir)
        library_path = self._find_library(crate_dir)
        if library_path is None:
            if cargo is None:
                raise RuntimeError(
                    "netaudio-core library is not built and cargo is not installed; "
                    "install Rust (https://rustup.rs) or use a prebuilt wheel"
                )
            raise RuntimeError(
                f"netaudio-core library not found in {crate_dir / 'target' / 'release'} after cargo build"
            )
        installed_library = install_built_library(self.root, library_path)
        build_data["force_include"][str(installed_library)] = f"netaudio/core/{installed_library.name}"
        build_data["force_include"][str(write_build_information(self.root))] = "netaudio/_build_info.json"
        build_data["pure_python"] = False
        build_data["tag"] = f"py3-none-{self._platform_tag()}"

    def _find_library(self, crate_dir):
        release_dir = crate_dir / "target" / "release"
        library_path = release_dir / self._library_name()
        return library_path if library_path.exists() else None

    @staticmethod
    def _library_name():
        if sys.platform == "darwin":
            return "libnetaudio_core.dylib"
        if sys.platform == "win32":
            return "netaudio_core.dll"
        return "libnetaudio_core.so"

    def _build_core(self, crate_dir):
        cargo = shutil.which("cargo")
        if cargo is None:
            raise RuntimeError(
                "netaudio-core library is not built and cargo is not installed; "
                "install Rust (https://rustup.rs) or use a prebuilt wheel"
            )
        subprocess.run([cargo, "build", "--release"], cwd=crate_dir, check=True)

    def _platform_tag(self):
        if sys.platform == "darwin":
            target = os.environ.get("MACOSX_DEPLOYMENT_TARGET", "11.0")
            major, _, minor = target.partition(".")
            return f"macosx_{major}_{minor or '0'}_{platform.machine()}"
        return next(iter(platform_tags()))
