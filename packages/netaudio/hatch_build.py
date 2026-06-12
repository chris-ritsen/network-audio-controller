import subprocess
import os
import sys
from pathlib import Path

from hatchling.builders.hooks.plugin.interface import BuildHookInterface


class ManPageBuildHook(BuildHookInterface):
    PLUGIN_NAME = "man-pages"

    def initialize(self, version, build_data):
        man_dir = Path(self.root) / "man"
        man_dir.mkdir(exist_ok=True)
        env = {
            **os.environ,
            "PYTHONPATH": str(Path(self.root) / "src"),
        }
        subprocess.check_call(
            [sys.executable, str(Path(self.root) / "generate_man.py"), str(man_dir)],
            env=env,
        )

        shared_data = build_data.setdefault("shared_data", {})
        for man_file in sorted(man_dir.glob("*.1")):
            shared_data[str(man_file)] = f"share/man/man1/{man_file.name}"
