import importlib.util
import struct
import sys
import types
import zipfile
from pathlib import Path

import pytest

from netaudio.daemon import service_install


REPO_ROOT = Path(__file__).resolve().parents[1]


def _load_artifact_verifier():
    spec = importlib.util.spec_from_file_location(
        "netaudio_test_verify_wheel_artifact",
        REPO_ROOT / "scripts" / "verify_wheel_artifact.py",
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _load_build_hook(monkeypatch):
    interface = types.ModuleType("hatchling.builders.hooks.plugin.interface")
    interface.BuildHookInterface = object
    monkeypatch.setitem(sys.modules, "hatchling", types.ModuleType("hatchling"))
    monkeypatch.setitem(sys.modules, "hatchling.builders", types.ModuleType("hatchling.builders"))
    monkeypatch.setitem(sys.modules, "hatchling.builders.hooks", types.ModuleType("hatchling.builders.hooks"))
    monkeypatch.setitem(
        sys.modules,
        "hatchling.builders.hooks.plugin",
        types.ModuleType("hatchling.builders.hooks.plugin"),
    )
    monkeypatch.setitem(sys.modules, "hatchling.builders.hooks.plugin.interface", interface)

    spec = importlib.util.spec_from_file_location("netaudio_test_hatch_build_core", REPO_ROOT / "hatch_build_core.py")
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize(
    ("platform_name", "expected_name"),
    [
        ("darwin", "libnetaudio_core.dylib"),
        ("linux", "libnetaudio_core.so"),
        ("win32", "netaudio_core.dll"),
    ],
)
def test_build_hook_selects_only_the_current_platform_library(monkeypatch, tmp_path, platform_name, expected_name):
    module = _load_build_hook(monkeypatch)
    monkeypatch.setattr(module.sys, "platform", platform_name)
    release_dir = tmp_path / "target" / "release"
    release_dir.mkdir(parents=True)
    for name in ("libnetaudio_core.so", "libnetaudio_core.dylib", "netaudio_core.dll"):
        (release_dir / name).touch()

    hook = module.CoreLibraryBuildHook()

    assert hook._find_library(tmp_path) == release_dir / expected_name


def test_build_hook_rebuilds_before_including_the_library(monkeypatch, tmp_path):
    module = _load_build_hook(monkeypatch)
    hook = module.CoreLibraryBuildHook()
    hook.root = str(tmp_path)
    crate_dir = tmp_path / "packages" / "netaudio-core"
    library_path = crate_dir / "target" / "release" / hook._library_name()
    calls = []

    def build_core(received_crate_dir):
        calls.append(received_crate_dir)
        library_path.parent.mkdir(parents=True)
        library_path.touch()

    monkeypatch.setattr(hook, "_build_core", build_core)
    monkeypatch.setattr(hook, "_platform_tag", lambda: "test_platform")
    build_data = {"force_include": {}}

    hook.initialize("0.0.0", build_data)

    assert calls == [crate_dir]
    assert build_data["force_include"] == {str(library_path): f"netaudio/core/{library_path.name}"}
    assert build_data["pure_python"] is False
    assert build_data["tag"] == "py3-none-test_platform"


class _Object:
    pass


class _Actions:
    def Create(self, _action_type):
        self.action = _Object()
        return self.action


class _Triggers:
    def Create(self, _trigger_type):
        return _Object()


class _Scheduler:
    def NewTask(self, _flags):
        definition = _Object()
        definition.RegistrationInfo = _Object()
        definition.Triggers = _Triggers()
        definition.Actions = _Actions()
        definition.Settings = _Object()
        return definition


def test_windows_task_runs_foreground_without_a_time_limit(monkeypatch):
    monkeypatch.setattr(service_install, "executable_path", lambda: r"C:\Tools\netaudio.exe")

    definition = service_install._windows_build_definition(_Scheduler())

    assert definition.Actions.action.Path == r"C:\Tools\netaudio.exe"
    assert definition.Actions.action.Arguments == "daemon run"
    assert definition.Settings.ExecutionTimeLimit == "PT0S"


def test_supported_python_floor_is_consistent():
    try:
        import tomllib
    except ModuleNotFoundError:
        import tomli as tomllib

    project = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text())
    release_workflow = (REPO_ROOT / ".github" / "workflows" / "release.yml").read_text()
    quality_workflow = (REPO_ROOT / ".github" / "workflows" / "quality.yml").read_text()

    assert project["project"]["requires-python"] == ">=3.9"
    assert project["tool"]["ruff"]["target-version"] == "py39"
    assert 'python-version: ["3.9", "3.10", "3.11", "3.12", "3.13"]' in quality_workflow
    for version in ("3.9", "3.10", "3.11", "3.12", "3.13"):
        assert version in release_workflow
        assert version in quality_workflow
    assert (REPO_ROOT / "scripts" / "smoke_wheel.py").is_file()
    assert "scripts/smoke_wheel_install.py" in release_workflow
    assert "scripts/smoke_wheel_install.py" in quality_workflow
    assert "scripts/verify_wheel_artifact.py" in release_workflow
    assert "scripts/verify_wheel_artifact.py" in quality_workflow
    assert "scripts/check_native_abi.py" in quality_workflow
    assert "uv run --isolated --no-project" in release_workflow
    assert "uv run --isolated --no-project" in quality_workflow
    assert 'uv build --wheel --out-dir "$wheel_dir" "${archive[0]}"' in release_workflow


def test_release_artifact_portability_policy_is_explicit():
    release_workflow = (REPO_ROOT / ".github" / "workflows" / "release.yml").read_text()
    quality_workflow = (REPO_ROOT / ".github" / "workflows" / "quality.yml").read_text()

    for workflow in (release_workflow, quality_workflow):
        assert "quay.io/pypa/manylinux_2_28_x86_64" in workflow
        assert "quay.io/pypa/manylinux_2_28_aarch64" in workflow
        assert "auditwheel show" in workflow
        assert "ubuntu-24.04-arm" in workflow
        assert "macos-15-intel" in workflow
        assert "architecture: arm64" in workflow
        assert "architecture: x86_64" in workflow
    assert "macos-14" not in release_workflow
    assert "cbindgen --verify" in quality_workflow
    assert "rename_channel.c" in quality_workflow
    assert "rename_channel.swift" in quality_workflow


def _elf(machine: int = 62, glibc: bytes = b"GLIBC_2.28") -> bytes:
    data = bytearray(64)
    data[:6] = b"\x7fELF\x02\x01"
    struct.pack_into("<H", data, 18, machine)
    return bytes(data) + glibc


def _write_test_wheel(
    tmp_path: Path,
    *,
    native_data: bytes,
    native_name: str = "libnetaudio_core.so",
    platform_tag: str = "manylinux_2_28_x86_64",
) -> Path:
    wheel = tmp_path / f"netaudio-0.0.0-py3-none-{platform_tag}.whl"
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.writestr(f"netaudio/core/{native_name}", native_data)
        archive.writestr(
            "netaudio-0.0.0.dist-info/WHEEL",
            f"Wheel-Version: 1.0\nRoot-Is-Purelib: false\nTag: py3-none-{platform_tag}\n",
        )
        archive.writestr(
            "netaudio-0.0.0.dist-info/entry_points.txt",
            "[console_scripts]\nnetaudio = netaudio.cli:main\n",
        )
    return wheel


def _macho(machine: int = 0x0100000C, minimum: tuple[int, int, int] = (11, 0, 0)) -> bytes:
    data = bytearray(56)
    data[:4] = b"\xcf\xfa\xed\xfe"
    struct.pack_into("<I", data, 4, machine)
    struct.pack_into("<II", data, 16, 1, 24)
    struct.pack_into("<II", data, 32, 0x32, 24)
    encoded_version = minimum[0] << 16 | minimum[1] << 8 | minimum[2]
    struct.pack_into("<I", data, 44, encoded_version)
    return bytes(data)


def _pe(machine: int = 0x8664) -> bytes:
    data = bytearray(128)
    data[:2] = b"MZ"
    struct.pack_into("<I", data, 0x3C, 64)
    data[64:68] = b"PE\0\0"
    struct.pack_into("<H", data, 68, machine)
    return bytes(data)


def test_wheel_policy_accepts_matching_manylinux_artifact(tmp_path):
    wheel = _write_test_wheel(tmp_path, native_data=_elf())

    _load_artifact_verifier().verify_wheel(wheel, "linux", "x86_64")


def test_wheel_policy_rejects_glibc_newer_than_claimed_floor(tmp_path):
    wheel = _write_test_wheel(tmp_path, native_data=_elf(glibc=b"GLIBC_2.29"))

    with pytest.raises(RuntimeError, match="above the manylinux 2.28 policy"):
        _load_artifact_verifier().verify_wheel(wheel, "linux", "x86_64")


def test_wheel_policy_rejects_foreign_native_artifacts(tmp_path):
    wheel = _write_test_wheel(tmp_path, native_data=_elf(), native_name="libnetaudio_core.dylib")

    with pytest.raises(RuntimeError, match="must contain only"):
        _load_artifact_verifier().verify_wheel(wheel, "linux", "x86_64")


def test_wheel_policy_accepts_matching_macos_artifact(tmp_path):
    wheel = _write_test_wheel(
        tmp_path,
        native_data=_macho(),
        native_name="libnetaudio_core.dylib",
        platform_tag="macosx_11_0_arm64",
    )

    _load_artifact_verifier().verify_wheel(wheel, "macos", "arm64")


def test_wheel_policy_rejects_macos_binary_newer_than_tag(tmp_path):
    wheel = _write_test_wheel(
        tmp_path,
        native_data=_macho(minimum=(12, 0, 0)),
        native_name="libnetaudio_core.dylib",
        platform_tag="macosx_11_0_arm64",
    )

    with pytest.raises(RuntimeError, match="above the 11.0 wheel tag"):
        _load_artifact_verifier().verify_wheel(wheel, "macos", "arm64")


def test_wheel_policy_accepts_matching_windows_artifact(tmp_path):
    wheel = _write_test_wheel(
        tmp_path,
        native_data=_pe(),
        native_name="netaudio_core.dll",
        platform_tag="win_amd64",
    )

    _load_artifact_verifier().verify_wheel(wheel, "windows", "x86_64")


def test_aur_published_package_matches_pure_python_release():
    package_build = (REPO_ROOT / "aur" / "netaudio" / "PKGBUILD").read_text()
    dependencies = next(line for line in package_build.splitlines() if line.startswith("depends="))
    build_dependencies = next(line for line in package_build.splitlines() if line.startswith("makedepends="))

    assert "pkgver=0.2.4" in package_build
    assert "arch=(any)" in package_build
    assert "'rust'" not in build_dependencies
    assert "'python-pynacl'" in dependencies


def test_aur_git_package_builds_native_core_and_declares_linux_runtime_dependencies():
    package_build = (REPO_ROOT / "aur" / "netaudio-git" / "PKGBUILD").read_text()
    dependencies = next(line for line in package_build.splitlines() if line.startswith("depends="))
    build_dependencies = next(line for line in package_build.splitlines() if line.startswith("makedepends="))

    assert "arch=('x86_64' 'aarch64')" in package_build
    assert "'rust'" in build_dependencies
    assert "'python-redis'" in dependencies
    assert "'python-dbus-fast'" in dependencies


def test_xcframework_build_requires_clean_committed_source_and_writes_provenance():
    build_script = (REPO_ROOT / "scripts" / "build_xcframework.sh").read_text()

    assert 'git -C "$root" rev-parse --verify HEAD' in build_script
    assert 'git -C "$root" status --porcelain=v1 --untracked-files=all --' in build_script
    assert "packages/netaudio-core" in build_script
    assert "scripts/build_xcframework.sh" in build_script
    assert 'if [[ -n "$repository_changes" ]]' in build_script
    assert '"$framework_path/PROVENANCE.md"' in build_script
    assert "Source commit:" in build_script
    assert "ABI version:" in build_script
