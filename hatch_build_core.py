import os
import platform
import shutil
import subprocess
import sys
from pathlib import Path

from hatchling.builders.hooks.plugin.interface import BuildHookInterface
from packaging.tags import platform_tags


class CoreLibraryBuildHook(BuildHookInterface):
    PLUGIN_NAME = "custom"

    def initialize(self, version, build_data):
        crate_dir = Path(self.root) / "packages" / "netaudio-core"
        library_path = self._find_library(crate_dir)
        if library_path is None:
            self._build_core(crate_dir)
            library_path = self._find_library(crate_dir)
        if library_path is None:
            raise RuntimeError(
                f"netaudio-core library not found in {crate_dir / 'target' / 'release'} after cargo build"
            )
        build_data["force_include"][str(library_path)] = f"netaudio/core/{library_path.name}"
        build_data["pure_python"] = False
        build_data["tag"] = f"py3-none-{self._platform_tag()}"

    def _find_library(self, crate_dir):
        release_dir = crate_dir / "target" / "release"
        for library_name in ("libnetaudio_core.so", "libnetaudio_core.dylib", "netaudio_core.dll"):
            library_path = release_dir / library_name
            if library_path.exists():
                return library_path
        return None

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
