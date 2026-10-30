import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tarfile
import urllib.request


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def run(arguments, directory, environment, log):
    with log.open("wb") as output:
        subprocess.run(
            arguments,
            cwd=directory,
            env=environment,
            stdout=output,
            stderr=subprocess.STDOUT,
            check=True,
        )


def main():
    arguments = argparse.ArgumentParser(description="Build the isolated H.264 configuration-acceptance provider")
    arguments.add_argument("--directory", type=Path, required=True)
    arguments.add_argument("--source-cache", type=Path)
    arguments.add_argument("--jobs", type=int, default=4)
    selected = arguments.parse_args()
    if not 1 <= selected.jobs <= 64:
        arguments.error("jobs must be between 1 and 64")
    repository = Path(__file__).resolve().parent.parent
    package = repository / "native/video-provider"
    manifest = json.loads((package / "sources.json").read_text())
    directory = selected.directory.resolve()
    if directory.is_relative_to(repository):
        arguments.error("build directory must be outside the product checkout")
    directory.mkdir(parents=True, exist_ok=False)
    prefix = directory / "prefix"
    environment = os.environ.copy()
    environment.pop("LD_PRELOAD", None)
    environment["PKG_CONFIG_PATH"] = str(prefix / "lib/pkgconfig")
    environment["LD_LIBRARY_PATH"] = str(prefix / "lib")
    for source in manifest["sources"]:
        archive = directory / source["file"]
        if selected.source_cache:
            shutil.copyfile(selected.source_cache / source["file"], archive)
        else:
            with urllib.request.urlopen(source["url"], timeout=60) as response:
                with archive.open("xb") as output:
                    shutil.copyfileobj(response, output)
        if archive.stat().st_size != source["bytes"] or digest(archive) != source["sha256"]:
            raise RuntimeError(f"source archive does not match its pin: {source['file']}")
        patch = package / source["patch"]
        if digest(patch) != source["patch_sha256"]:
            raise RuntimeError(f"provider patch does not match its pin: {patch.name}")
        with tarfile.open(archive) as contents:
            for member in contents:
                if Path(member.name).parts[0] != source["directory"]:
                    raise RuntimeError("source archive contains an unexpected root")
            contents.extractall(directory, filter="data")
        run(
            ["patch", "--batch", "--forward", "--fuzz=0", "-p1", "-i", str(patch)],
            directory / source["directory"],
            environment,
            directory / (source["directory"] + "-patch.log"),
        )
    native = directory / "ffmpeg-build"
    native.mkdir()
    run(
        [
            str(directory / "ffmpeg-9.0.2/configure"),
            f"--prefix={prefix}",
            "--disable-programs",
            "--disable-doc",
            "--disable-autodetect",
            "--disable-everything",
            "--enable-decoder=h264",
            "--enable-shared",
            "--disable-static",
            "--disable-avdevice",
            "--extra-version=netaudio-parameter-configuration-v1",
        ],
        native,
        environment,
        directory / "ffmpeg-configure.log",
    )
    run(["make", f"-j{selected.jobs}"], native, environment, directory / "ffmpeg-build.log")
    run(["make", "install"], native, environment, directory / "ffmpeg-stage.log")
    run(
        [
            "meson",
            "setup",
            "gstreamer-build",
            "gst-libav-1.28.8",
            f"--prefix={prefix}",
            "--libdir=lib",
            "--buildtype=release",
            "--wrap-mode=nofallback",
            "-Dtests=disabled",
            "-Ddoc=disabled",
            "-Dpackage-name=netaudio-parameter-configuration-v1",
        ],
        directory,
        environment,
        directory / "gstreamer-configure.log",
    )
    run(
        ["ninja", "-C", "gstreamer-build", f"-j{selected.jobs}"],
        directory,
        environment,
        directory / "gstreamer-build.log",
    )
    run(
        ["meson", "install", "-C", "gstreamer-build"],
        directory,
        environment,
        directory / "gstreamer-stage.log",
    )
    artifacts = [
        {"path": str(path.relative_to(directory)), "sha256": digest(path)}
        for path in sorted((prefix / "lib").rglob("*.so*"))
        if path.is_file() and not path.is_symlink()
    ]
    result = {
        "contract": manifest["contract"],
        "sources": manifest["sources"],
        "artifacts": artifacts,
        "environment": {
            "LD_LIBRARY_PATH": str(prefix / "lib"),
            "GST_PLUGIN_PATH": str(prefix / "lib/gstreamer-1.0"),
            "GST_REGISTRY": str(directory / "registry.bin"),
        },
        "scope": "Private staged provider; no system installation or runtime qualification",
    }
    (directory / "build.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
