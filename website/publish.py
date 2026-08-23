import argparse
import hashlib
import json
import os
import re
import shutil
from pathlib import Path
from typing import Optional


DEPLOYMENT_ROOT = Path("/srv/http/netaudio")
DEFAULT_SOURCE_DIRECTORY = Path(__file__).resolve().parent / "public"
RELEASE_IDENTIFIER_PATTERN = re.compile(r"^[0-9a-f]{64}$")


def file_digest(file_path: Path) -> str:
    digest = hashlib.sha256()
    with file_path.open("rb") as file_handle:
        for chunk in iter(lambda: file_handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def release_identifier(files: dict[str, str]) -> str:
    serialized_files = json.dumps(files, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(serialized_files).hexdigest()


def source_file_manifest(source_directory: Path) -> dict[str, str]:
    resolved_source = source_directory.expanduser().resolve(strict=True)
    if resolved_source.is_symlink() or not resolved_source.is_dir():
        raise ValueError("source directory must be a real directory")
    files: dict[str, str] = {}
    for path in sorted(resolved_source.rglob("*")):
        relative_path = path.relative_to(resolved_source)
        if path.is_symlink() or any(part.startswith(".") for part in relative_path.parts):
            raise ValueError(f"source contains a forbidden path: {relative_path}")
        if path.is_file():
            files[relative_path.as_posix()] = file_digest(path)
        elif not path.is_dir():
            raise ValueError(f"source contains an unsupported path: {relative_path}")
    required_files = {
        "index.html",
        "support.html",
        "privacy.html",
        "robots.txt",
        "sitemap.xml",
        "assets/site.css",
        "assets/contents-navigation.js",
        "assets/favicon.svg",
        "assets/favicon.ico",
        "assets/apple-touch-icon.png",
    }
    missing_files = required_files - set(files)
    if missing_files:
        raise ValueError(f"source is missing required files: {sorted(missing_files)}")
    return files


def release_manifest(source_directory: Path) -> dict:
    files = source_file_manifest(source_directory)
    return {
        "format_version": 1,
        "site_name": "NetAudio",
        "base_url": "https://netaudio.app",
        "release_identifier": release_identifier(files),
        "files": files,
    }


def verify_release_directory(release_directory: Path, expected_manifest: Optional[dict] = None) -> dict:
    manifest_path = release_directory / "manifest.json"
    public_directory = release_directory / "public"
    if not manifest_path.is_file() or manifest_path.is_symlink() or not public_directory.is_dir():
        raise ValueError("release directory is incomplete")
    stored_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if set(stored_manifest) != {"format_version", "site_name", "base_url", "release_identifier", "files"}:
        raise ValueError("release manifest has an unexpected structure")
    actual_manifest = release_manifest(public_directory)
    if stored_manifest != actual_manifest:
        raise ValueError("release contents do not match the stored manifest")
    if expected_manifest is not None and stored_manifest != expected_manifest:
        raise ValueError("existing release differs from the requested source")
    return stored_manifest


def set_release_permissions(release_directory: Path, owner_identifier: int, group_identifier: int) -> None:
    for path in [release_directory, *sorted(release_directory.rglob("*"))]:
        os.chown(path, owner_identifier, group_identifier)
        os.chmod(path, 0o755 if path.is_dir() else 0o644)


def synchronize_directory(directory: Path) -> None:
    directory_descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory_descriptor)
    finally:
        os.close(directory_descriptor)


def activate_release(deployment_root: Path, release_identifier_value: str) -> tuple[Optional[str], str]:
    if not RELEASE_IDENTIFIER_PATTERN.fullmatch(release_identifier_value):
        raise ValueError("invalid release identifier")
    release_directory = deployment_root / "releases" / release_identifier_value
    verify_release_directory(release_directory)
    current_link = deployment_root / "current"
    if current_link.exists() and not current_link.is_symlink():
        raise ValueError("deployment current path must be a symlink")
    previous_target = os.readlink(current_link) if current_link.is_symlink() else None
    temporary_link = deployment_root / f".current.{os.getpid()}"
    if temporary_link.exists() or temporary_link.is_symlink():
        raise ValueError("temporary activation path already exists")
    relative_target = Path("releases") / release_identifier_value / "public"
    os.symlink(relative_target, temporary_link)
    os.replace(temporary_link, current_link)
    synchronize_directory(deployment_root)
    return previous_target, relative_target.as_posix()


def publish_source(
    source_directory: Path,
    deployment_root: Path,
    owner_identifier: int,
    group_identifier: int,
) -> tuple[str, Optional[str], str]:
    source_directory = source_directory.expanduser().resolve(strict=True)
    requested_manifest = release_manifest(source_directory)
    release_identifier_value = requested_manifest["release_identifier"]
    if deployment_root.is_symlink() or not deployment_root.is_dir():
        raise ValueError("deployment root must be an existing real directory")
    releases_directory = deployment_root / "releases"
    releases_directory.mkdir(mode=0o755, exist_ok=True)
    if releases_directory.is_symlink() or not releases_directory.is_dir():
        raise ValueError("releases path must be a real directory")
    os.chown(releases_directory, owner_identifier, group_identifier)
    final_release_directory = releases_directory / release_identifier_value
    if final_release_directory.exists():
        verify_release_directory(final_release_directory, requested_manifest)
    else:
        staging_release_directory = releases_directory / f".{release_identifier_value}.{os.getpid()}"
        if staging_release_directory.exists():
            raise ValueError("staging release path already exists")
        try:
            staging_public_directory = staging_release_directory / "public"
            shutil.copytree(source_directory, staging_public_directory, symlinks=False)
            (staging_release_directory / "manifest.json").write_text(
                json.dumps(requested_manifest, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            verify_release_directory(staging_release_directory, requested_manifest)
            set_release_permissions(staging_release_directory, owner_identifier, group_identifier)
            synchronize_directory(staging_release_directory)
            os.replace(staging_release_directory, final_release_directory)
            synchronize_directory(releases_directory)
        except Exception:
            if staging_release_directory.exists():
                shutil.rmtree(staging_release_directory)
            raise
    previous_target, current_target = activate_release(deployment_root, release_identifier_value)
    return release_identifier_value, previous_target, current_target


def main() -> None:
    if os.name != "posix":
        raise SystemExit("publish.py requires a POSIX operating system")
    if os.geteuid() != 0:
        raise SystemExit("publish.py must run as root")
    import grp

    argument_parser = argparse.ArgumentParser()
    operation_group = argument_parser.add_mutually_exclusive_group(required=True)
    operation_group.add_argument("--source", type=Path)
    operation_group.add_argument("--activate")
    arguments = argument_parser.parse_args()
    caddy_group_identifier = grp.getgrnam("caddy").gr_gid
    if arguments.source is not None:
        release_identifier_value, previous_target, current_target = publish_source(
            arguments.source,
            DEPLOYMENT_ROOT,
            0,
            caddy_group_identifier,
        )
    else:
        release_identifier_value = arguments.activate
        previous_target, current_target = activate_release(DEPLOYMENT_ROOT, release_identifier_value)
    print(
        json.dumps(
            {
                "release_identifier": release_identifier_value,
                "previous_target": previous_target,
                "current_target": current_target,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
