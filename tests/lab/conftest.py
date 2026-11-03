from __future__ import annotations

import pytest

from tests.lab.lab_support import (
    LabError,
    NetAudio,
    PrivateDaemon,
    free_port,
    load_lab,
    start_virtual_devices,
    wait_until,
)

TRANSPORTS = ("daemon", "direct")
DISCOVERY_DEADLINE_SECONDS = 90


def pytest_generate_tests(metafunc):
    lab = load_lab()
    if "transport" in metafunc.fixturenames:
        metafunc.parametrize("transport", TRANSPORTS, scope="session")
    if "lab_device" in metafunc.fixturenames:
        metafunc.parametrize("lab_device", [device.name for device in lab.devices], scope="session")
    if "writable_device" in metafunc.fixturenames:
        metafunc.parametrize("writable_device", [device.name for device in lab.writable] or [None], scope="session")


@pytest.fixture(scope="session")
def lab():
    return load_lab()


@pytest.fixture(scope="session")
def lab_home(tmp_path_factory):
    return tmp_path_factory.mktemp("lab")


@pytest.fixture(scope="session")
def virtual_devices(lab, lab_home):
    if not lab.virtual_devices:
        yield None
        return
    devices = start_virtual_devices(lab, lab_home / "virtual")
    try:
        yield devices
    finally:
        devices.stop()


@pytest.fixture(scope="session")
def direct_netaudio(lab, lab_home, virtual_devices):
    netaudio = NetAudio(lab_home / "direct", free_port(), lab)
    expected = {device.name for device in lab.devices}
    wait_until(
        f"direct discovery of {', '.join(sorted(expected))}",
        lambda: expected <= set(netaudio.devices()),
        DISCOVERY_DEADLINE_SECONDS,
        virtual_devices,
    )
    return netaudio


@pytest.fixture(scope="session")
def private_daemon(lab, lab_home, virtual_devices):
    try:
        daemon = PrivateDaemon(lab, lab_home / "daemon")
    except LabError as error:
        pytest.skip(str(error))
    try:
        expected = {device.name for device in lab.devices}
        wait_until(
            f"daemon discovery of {', '.join(sorted(expected))}",
            lambda: expected <= daemon.device_names(),
            DISCOVERY_DEADLINE_SECONDS,
            daemon,
        )
        yield daemon
    finally:
        daemon.stop()


@pytest.fixture(scope="session")
def daemon_netaudio(lab, lab_home, private_daemon):
    return NetAudio(lab_home / "daemon-client", private_daemon.port, lab)


@pytest.fixture(scope="session")
def netaudio(request, transport):
    return request.getfixturevalue(f"{transport}_netaudio")


@pytest.fixture(scope="session")
def daemon_inventory(daemon_netaudio):
    return daemon_netaudio.devices()


@pytest.fixture(scope="session")
def direct_inventory(direct_netaudio):
    return direct_netaudio.devices()


@pytest.fixture(scope="session")
def inventory(request, transport):
    return request.getfixturevalue(f"{transport}_inventory")
