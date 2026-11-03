from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import sys
import time
import urllib.request
from dataclasses import dataclass
from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib

LAB_VARIABLE = "NETAUDIO_LAB"
DAEMON_HOST_NAME = "netaudio-lab"
VIRTUAL_DEVICE_NAME = "netaudio-lab"
VIRTUAL_CHANNEL_COUNT = 4
HOST_NAME_LAUNCHER = "import os, socket, sys; socket.sethostname(sys.argv[1]); os.execv(sys.argv[2], sys.argv[2:])"
LAB_KEYS = {"devices", "interface", "timeout", "virtual_devices"}
DEVICE_KEYS = {"name", "writable"}


class LabError(RuntimeError):
    pass


@dataclass(frozen=True)
class LabDevice:
    name: str
    virtual: bool = False
    writable: bool = False


@dataclass(frozen=True)
class Lab:
    devices: tuple[LabDevice, ...]
    interface: str | None
    timeout: float
    virtual_devices: int

    def named(self, name: str) -> LabDevice:
        for device in self.devices:
            if device.name == name:
                return device
        raise LabError(f"{name} is not a lab device")

    @property
    def writable(self) -> tuple[LabDevice, ...]:
        return tuple(device for device in self.devices if device.writable)


def load_lab() -> Lab:
    if not os.environ.get(LAB_VARIABLE):
        raise LabError(f"Set {LAB_VARIABLE} to a lab file naming the devices to test.")
    path = Path(os.environ[LAB_VARIABLE]).expanduser()
    with path.open("rb") as file:
        document = tomllib.load(file)
    unknown = set(document) - LAB_KEYS
    if unknown:
        raise LabError(f"{path} has unknown keys: {', '.join(sorted(unknown))}")
    devices = []
    for entry in document.get("devices", []):
        unknown = set(entry) - DEVICE_KEYS
        if unknown:
            raise LabError(f"{path} device {entry.get('name')} has unknown keys: {', '.join(sorted(unknown))}")
        devices.append(LabDevice(name=entry["name"], writable=bool(entry.get("writable", False))))
    virtual_devices = int(document.get("virtual_devices", 0))
    for index in range(virtual_devices):
        devices.append(LabDevice(name=virtual_device_name(index), virtual=True, writable=True))
    names = [device.name for device in devices]
    if len(names) != len(set(names)):
        raise LabError(f"{path} names a device more than once")
    if not devices:
        raise LabError(f"{path} names no devices")
    return Lab(
        devices=tuple(devices),
        interface=document.get("interface"),
        timeout=float(document.get("timeout", 5.0)),
        virtual_devices=virtual_devices,
    )


def virtual_device_name(index: int) -> str:
    return VIRTUAL_DEVICE_NAME if index == 0 else f"{VIRTUAL_DEVICE_NAME}-{index + 1}"


def free_port(kind: int = socket.SOCK_STREAM) -> int:
    with socket.socket(socket.AF_INET, kind) as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def interface_address(name: str) -> str:
    import ifaddr

    for adapter in ifaddr.get_adapters():
        if name in (adapter.name, adapter.nice_name):
            for address in adapter.ips:
                if isinstance(address.ip, str):
                    return address.ip
    raise LabError(f"interface {name} has no IPv4 address")


def isolated_environment(home: Path, daemon_port: int, interface: str | None) -> dict[str, str]:
    home.mkdir(parents=True, exist_ok=True)
    (home / "config.toml").touch()
    environment = {key: value for key, value in os.environ.items() if not key.startswith("NETAUDIO_")}
    environment.update(
        {
            "HOME": str(home),
            "NETAUDIO_CONFIG": str(home / "config.toml"),
            "NETAUDIO_DAEMON_PORT": str(daemon_port),
            "NETAUDIO_NO_COLOR": "1",
            "XDG_CACHE_HOME": str(home / "cache"),
            "XDG_CONFIG_HOME": str(home / "config"),
            "XDG_DATA_HOME": str(home / "data"),
            "XDG_STATE_HOME": str(home / "state"),
        }
    )
    if interface:
        environment["NETAUDIO_INTERFACE"] = interface
    return environment


class NetAudio:
    def __init__(self, home: Path, daemon_port: int, lab: Lab):
        self.environment = isolated_environment(home, daemon_port, lab.interface)
        self.timeout = lab.timeout

    def run(self, *arguments: str, check: bool = True, timeout: float = 180) -> subprocess.CompletedProcess[str]:
        command = [sys.executable, "-m", "netaudio", "--timeout", str(self.timeout), *arguments]
        result = subprocess.run(command, env=self.environment, capture_output=True, text=True, timeout=timeout)
        if check and result.returncode != 0:
            raise LabError(
                f"netaudio {' '.join(arguments)} exited with {result.returncode}\n{result.stdout}\n{result.stderr}"
            )
        return result

    def json(self, *arguments: str) -> object:
        return json.loads(self.run("--json", *arguments).stdout)

    def devices(self) -> dict[str, dict]:
        listing = self.json("device", "list")
        return {device["name"]: device for device in listing.get("dante", {}).values() if device.get("name")}

    def device(self, name: str) -> dict:
        devices = self.devices()
        if name not in devices:
            raise LabError(f"{name} was not discovered; found {', '.join(sorted(devices))}")
        return devices[name]

    def subscriptions(self, name: str) -> list[dict]:
        return [
            subscription
            for subscription in self.json("-n", name, "subscription", "list")
            if subscription.get("rx_device") == name
        ]


class Supervised:
    def __init__(self, name: str, command: list[str], environment: dict[str, str], log: Path):
        self.name = name
        self.log = log
        self.log_file = log.open("w")
        self.process = subprocess.Popen(
            command,
            env=environment,
            stdout=self.log_file,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )

    def require_running(self) -> None:
        if self.process.poll() is not None:
            raise LabError(f"{self.name} exited with {self.process.returncode}:\n{self.log.read_text()[-4000:]}")

    def stop(self) -> None:
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=15)
        self.log_file.close()


def wait_until(description: str, condition, deadline_seconds: float, owner: Supervised | None = None):
    deadline = time.monotonic() + deadline_seconds
    last_error = None
    while time.monotonic() < deadline:
        if owner is not None:
            owner.require_running()
        try:
            result = condition()
        except (LabError, OSError, ValueError) as error:
            last_error = error
            result = None
        if result:
            return result
        time.sleep(1)
    raise LabError(f"{description} did not happen within {deadline_seconds:.0f} seconds; last error: {last_error}")


def start_virtual_devices(lab: Lab, home: Path) -> Supervised:
    environment = isolated_environment(home, free_port(), lab.interface)
    command = [
        sys.executable,
        "-m",
        "netaudio",
        "virtual",
        "start",
        "--foreground",
        "--name",
        VIRTUAL_DEVICE_NAME,
        "--count",
        str(lab.virtual_devices),
        "--tx",
        str(VIRTUAL_CHANNEL_COUNT),
        "--rx",
        str(VIRTUAL_CHANNEL_COUNT),
        "--no-detailed-metering",
    ]
    if lab.interface:
        command.extend(["--interface", interface_address(lab.interface)])
    return Supervised("virtual devices", command, environment, home / "virtual.log")


class PrivateDaemon(Supervised):
    def __init__(self, lab: Lab, home: Path):
        if not sys.platform.startswith("linux") or shutil.which("unshare") is None:
            raise LabError("the private daemon needs Linux user and UTS namespaces")
        self.port = free_port()
        environment = isolated_environment(home, self.port, lab.interface)
        environment["NETAUDIO_METERING_PORT"] = str(free_port(socket.SOCK_DGRAM))
        environment["NETAUDIO_NO_SSL"] = "1"
        command = [
            "unshare",
            "--user",
            "--map-root-user",
            "--uts",
            "--",
            sys.executable,
            "-c",
            HOST_NAME_LAUNCHER,
            DAEMON_HOST_NAME,
            sys.executable,
            "-m",
            "netaudio",
            "daemon",
            "run",
            "--port",
            str(self.port),
        ]
        super().__init__("private daemon", command, environment, home / "daemon.log")

    def device_names(self) -> set[str]:
        with urllib.request.urlopen(f"http://127.0.0.1:{self.port}/devices", timeout=5) as response:
            devices = json.load(response)
        return {device.get("name") for device in devices.values() if isinstance(device, dict)}
