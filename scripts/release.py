import argparse
import json
import os
import pathlib
import re
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request

ROOT = pathlib.Path(__file__).resolve().parents[1]
CHECKS = (
    ("check-local", ["make", "check-local"]),
    (
        "browser",
        [
            "./node_modules/.bin/playwright",
            "test",
            "--project=chromium",
            "--workers=1",
            "--retries=0",
            "--reporter=line",
        ],
    ),
)
GITHUB_REPOSITORY = "chris-ritsen/network-audio-controller"
GITHUB_URL = f"https://github.com/{GITHUB_REPOSITORY}.git"
GITOLITE_REMOTE = "local"
OUTPUT = ROOT / "build" / "release"
POLL_SECONDS = 60
PYPI_URL = "https://pypi.org/pypi/netaudio/json"
PYPI_WAIT_SECONDS = 1800
RELEASE_WORKFLOW = "release.yml"
RUN_DISCOVERY_SECONDS = 300
STATUS = OUTPUT / "status.json"
STEPS = ("preflight", "version", "checks", "tag", "publish", "pypi")


class ReleaseError(Exception):
    pass


def timestamp():
    return time.strftime("%Y-%m-%d %H:%M:%S")


def read_status():
    try:
        return json.loads(STATUS.read_text())
    except FileNotFoundError:
        return None


def write_status(**values):
    status = read_status() or {}
    status.update(values, updated=timestamp())
    OUTPUT.mkdir(parents=True, exist_ok=True)
    temporary = STATUS.with_suffix(".tmp")
    temporary.write_text(json.dumps(status, indent=1, sort_keys=True) + "\n")
    temporary.replace(STATUS)


def process_alive(identifier):
    try:
        os.kill(identifier, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def running_release():
    status = read_status()
    if status and status.get("state") == "running" and process_alive(status["pid"]):
        return status
    return None


def report(message):
    print(f"[{timestamp()}] {message}", flush=True)
    write_status(detail=message)


def run(arguments):
    print(f"$ {' '.join(arguments)}", flush=True)
    subprocess.run(arguments, cwd=ROOT, check=True)


def output(arguments):
    return subprocess.run(arguments, cwd=ROOT, check=True, capture_output=True, text=True).stdout.strip()


def github_git(arguments):
    return ["git", "-c", "credential.helper=", "-c", "credential.helper=!gh auth git-credential", *arguments]


def project_version():
    match = re.search(r'^version = "([^"]+)"$', (ROOT / "pyproject.toml").read_text(), re.MULTILINE)
    if not match:
        raise ReleaseError("pyproject.toml declares no version.")
    return match.group(1)


def published_versions():
    with urllib.request.urlopen(PYPI_URL, timeout=30) as response:
        return set(json.load(response)["releases"])


def preflight():
    if output(["git", "rev-parse", "--abbrev-ref", "HEAD"]) != "master":
        raise ReleaseError("Release from master.")
    if output(["git", "status", "--porcelain", "--untracked-files=no"]):
        raise ReleaseError("Commit all source changes before releasing.")
    output(["git", "fetch", "--quiet", GITOLITE_REMOTE])
    if (
        subprocess.run(["git", "merge-base", "--is-ancestor", f"{GITOLITE_REMOTE}/master", "HEAD"], cwd=ROOT).returncode
        != 0
    ):
        raise ReleaseError("master is behind gitolite; pull before releasing.")
    output(github_git(["fetch", "--quiet", GITHUB_URL, "master"]))
    if subprocess.run(["git", "merge-base", "--is-ancestor", "FETCH_HEAD", "HEAD"], cwd=ROOT).returncode != 0:
        raise ReleaseError("GitHub master has commits that master lacks.")


def prepare_version(requested):
    current = project_version()
    version = requested or current
    if not re.fullmatch(r"\d+\.\d+\.\d+", version):
        raise ReleaseError(f"{version} is not a release version.")
    if version in published_versions():
        raise ReleaseError(f"netaudio {version} is already on PyPI; pass VERSION= with the next version.")
    tag = f"v{version}"
    if output(github_git(["ls-remote", "--tags", GITHUB_URL, f"refs/tags/{tag}"])):
        raise ReleaseError(f"GitHub already has tag {tag}.")
    if output(["git", "tag", "--list", tag]) and output(["git", "rev-parse", f"{tag}^{{commit}}"]) != output(
        ["git", "rev-parse", "HEAD"]
    ):
        raise ReleaseError(f"Tag {tag} already names another commit.")
    if version != current:
        path = ROOT / "pyproject.toml"
        path.write_text(
            re.sub(
                rf'^version = "{re.escape(current)}"$',
                f'version = "{version}"',
                path.read_text(),
                count=1,
                flags=re.MULTILINE,
            )
        )
        run(["uv", "lock", "--offline"])
        run(["git", "add", "pyproject.toml", "uv.lock"])
        run(["git", "-c", "commit.gpgsign=false", "commit", "--quiet", "-m", f"NetAudio {version}"])
    report(f"Releasing netaudio {version}.")
    return version


def check():
    for name, command in CHECKS:
        report(f"Running {name}.")
        run(command)


def tag(version):
    name = f"v{version}"
    if not output(["git", "tag", "--list", name]):
        run(["git", "-c", "tag.gpgsign=false", "tag", "-a", name, "-m", f"NetAudio {version}"])
    run(["git", "push", "--quiet", GITOLITE_REMOTE, "master", f"refs/tags/{name}"])
    return name


def workflow_state():
    return output(["gh", "api", f"repos/{GITHUB_REPOSITORY}/actions/workflows/{RELEASE_WORKFLOW}", "--jq", ".state"])


def find_run(tag_name):
    runs = json.loads(
        output(
            [
                "gh",
                "run",
                "list",
                "--repo",
                GITHUB_REPOSITORY,
                "--workflow",
                RELEASE_WORKFLOW,
                "--limit",
                "20",
                "--json",
                "databaseId,headBranch,url",
            ]
        )
    )
    return next((workflow_run for workflow_run in runs if workflow_run["headBranch"] == tag_name), None)


def wait_for_run(tag_name):
    deadline = time.monotonic() + RUN_DISCOVERY_SECONDS
    dispatched = False
    while not (workflow_run := find_run(tag_name)):
        if time.monotonic() >= deadline:
            if dispatched:
                raise ReleaseError(f"No {RELEASE_WORKFLOW} run started for {tag_name}.")
            run(["gh", "workflow", "run", RELEASE_WORKFLOW, "--repo", GITHUB_REPOSITORY, "--ref", tag_name])
            dispatched = True
            deadline = time.monotonic() + RUN_DISCOVERY_SECONDS
        time.sleep(15)
    report(f"Release workflow running: {workflow_run['url']}")
    while True:
        state = json.loads(
            output(
                [
                    "gh",
                    "run",
                    "view",
                    str(workflow_run["databaseId"]),
                    "--repo",
                    GITHUB_REPOSITORY,
                    "--json",
                    "status,conclusion,jobs",
                ]
            )
        )
        if state["status"] == "completed":
            if state["conclusion"] != "success":
                failed = ", ".join(
                    job["name"] for job in state["jobs"] if job["conclusion"] not in ("success", "skipped")
                )
                raise ReleaseError(
                    f"The release workflow ended with {state['conclusion']} ({failed}): {workflow_run['url']}"
                )
            return workflow_run
        finished = sum(1 for job in state["jobs"] if job["status"] == "completed")
        write_status(
            detail=f"Release workflow: {finished} of {len(state['jobs'])} jobs finished. {workflow_run['url']}"
        )
        time.sleep(POLL_SECONDS)


def publish(tag_name):
    enabled_here = False
    if workflow_state() != "active":
        run(["gh", "workflow", "enable", RELEASE_WORKFLOW, "--repo", GITHUB_REPOSITORY])
        enabled_here = True
    try:
        run(github_git(["push", "--quiet", GITHUB_URL, "master", f"refs/tags/{tag_name}"]))
        return wait_for_run(tag_name)
    finally:
        if enabled_here:
            run(["gh", "workflow", "disable", RELEASE_WORKFLOW, "--repo", GITHUB_REPOSITORY])


def wait_for_pypi(version):
    deadline = time.monotonic() + PYPI_WAIT_SECONDS
    while version not in published_versions():
        if time.monotonic() >= deadline:
            raise ReleaseError(f"netaudio {version} has not appeared on PyPI.")
        time.sleep(POLL_SECONDS)


def release(arguments):
    write_status(step="preflight")
    preflight()
    write_status(step="version")
    version = prepare_version(arguments.version)
    write_status(version=version)
    write_status(step="checks")
    check()
    write_status(step="tag")
    tag_name = tag(version)
    write_status(tag=tag_name)
    write_status(step="publish")
    publish(tag_name)
    write_status(step="pypi")
    wait_for_pypi(version)
    report(
        f"Published netaudio {version}: https://pypi.org/project/netaudio/{version}/ and https://github.com/{GITHUB_REPOSITORY}/releases/tag/{tag_name}"
    )


def handle_termination(signal_number, frame):
    raise SystemExit(128 + signal_number)


def execute(arguments):
    signal.signal(signal.SIGTERM, handle_termination)
    write_status(
        completed=False,
        error=None,
        pid=os.getpid(),
        started=timestamp(),
        state="running",
        step=None,
        tag=None,
        version=None,
    )
    try:
        release(arguments)
    except (ReleaseError, OSError, subprocess.CalledProcessError, urllib.error.URLError) as error:
        report(f"Failed: {error}")
        write_status(error=str(error), state="failed")
        return 1
    except SystemExit:
        write_status(error="Stopped before finishing.", state="stopped")
        raise
    write_status(completed=True, state="finished")
    return 0


def start(arguments):
    if running := running_release():
        raise ReleaseError(
            f"A release is already running as process {running['pid']}; make release-status shows its progress."
        )
    preflight()
    OUTPUT.mkdir(parents=True, exist_ok=True)
    log = (OUTPUT / "release.log").open("w")
    command = [sys.executable, __file__, "--run"]
    if arguments.version:
        command += ["--version", arguments.version]
    process = subprocess.Popen(
        command, cwd=ROOT, stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT, start_new_session=True
    )
    print(
        f"Release started as process {process.pid}. make release-status shows its progress; build/release/release.log has the full output."
    )


def show_status():
    status = read_status()
    if not status:
        print("No release has run; start one with make release.")
        return
    state = status["state"]
    if state == "running" and not process_alive(status["pid"]):
        state = f"running, but process {status['pid']} is no longer running"
    print(f"State: {state}")
    print(f"Started: {status['started']}; last update: {status['updated']}")
    if status.get("step"):
        position = STEPS.index(status["step"]) + 1 if status["step"] in STEPS else "?"
        print(f"Step: {status['step']} ({position} of {len(STEPS)})")
    if status.get("version"):
        print(f"Version: {status['version']}")
    if status.get("detail"):
        print(status["detail"])
    print(f"Log: {OUTPUT / 'release.log'}")


def main():
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--run", action="store_true")
    mode.add_argument("--status", action="store_true")
    parser.add_argument("--version")
    arguments = parser.parse_args()
    if arguments.status:
        show_status()
        return 0
    if arguments.run:
        return execute(arguments)
    try:
        start(arguments)
    except (ReleaseError, subprocess.CalledProcessError, urllib.error.URLError) as error:
        print(error, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
