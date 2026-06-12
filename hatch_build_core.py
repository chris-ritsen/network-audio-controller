from pathlib import Path

from hatchling.builders.hooks.plugin.interface import BuildHookInterface


class CoreLibraryBuildHook(BuildHookInterface):
    PLUGIN_NAME = "custom"

    def initialize(self, version, build_data):
        release_dir = Path(self.root) / "packages" / "netaudio-core" / "target" / "release"
        for library_name in ("libnetaudio_core.so", "libnetaudio_core.dylib", "netaudio_core.dll"):
            library_path = release_dir / library_name
            if library_path.exists():
                build_data["force_include"][str(library_path)] = f"netaudio/core/{library_name}"
                return
        raise RuntimeError(
            f"netaudio-core library not found in {release_dir}; build it first with 'make core'"
        )
