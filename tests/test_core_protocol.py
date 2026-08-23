import ctypes
import json
import shutil
import socket
import subprocess
import sys
import threading
from pathlib import Path

import pytest

from netaudio.dante.const import SERVICE_ARC
from netaudio.dante.device_commands import DanteDeviceCommands
from netaudio.dante.device_operations import validate_dante_name
from tests.protocol_test_fixtures import load_protocol_packet

FIXTURES_DIR = Path(__file__).parent / "fixtures"

CRATE_DIR = Path(__file__).parent.parent / "packages" / "netaudio-core"

NETAUDIO_OK = 0
NETAUDIO_NULL_POINTER = 1
NETAUDIO_INVALID_UTF8 = 2
NETAUDIO_NAME_TOO_LONG = 3
NETAUDIO_NAME_INVALID_HYPHEN = 4
NETAUDIO_NAME_INVALID_CHARS = 5
NETAUDIO_BUFFER_TOO_SMALL = 6
NETAUDIO_INVALID_ADDRESS = 7
NETAUDIO_IO_ERROR = 8
NETAUDIO_TIMEOUT = 9
NETAUDIO_MALFORMED_RESPONSE = 10
NETAUDIO_SERIALIZATION_ERROR = 11

VALID_NAMES = [
    "a",
    "A1",
    "AVIO",
    "Studio-AVIO",
    "x-1-y",
    "device-with-many-hyphens-here",
    "0",
    "9starts-with-digit",
    "a" * 31,
]

INVALID_NAMES = {
    "a" * 32: NETAUDIO_NAME_TOO_LONG,
    "ü" * 32: NETAUDIO_NAME_TOO_LONG,
    "-leading": NETAUDIO_NAME_INVALID_HYPHEN,
    "trailing-": NETAUDIO_NAME_INVALID_HYPHEN,
    "-both-": NETAUDIO_NAME_INVALID_HYPHEN,
    "": NETAUDIO_NAME_INVALID_CHARS,
    "under_score": NETAUDIO_NAME_INVALID_CHARS,
    "has space": NETAUDIO_NAME_INVALID_CHARS,
    "über": NETAUDIO_NAME_INVALID_CHARS,
    "dot.name": NETAUDIO_NAME_INVALID_CHARS,
}


def _library_name():
    if sys.platform == "darwin":
        return "libnetaudio_core.dylib"
    if sys.platform == "win32":
        return "netaudio_core.dll"
    return "libnetaudio_core.so"


def _find_or_build_library():
    if not shutil.which("cargo"):
        pytest.skip("cargo is required to build netaudio-core for protocol tests")
    subprocess.run(["cargo", "build"], cwd=CRATE_DIR, check=True, capture_output=True)
    library_path = CRATE_DIR / "target" / "debug" / _library_name()
    if library_path.exists():
        return library_path
    pytest.skip("netaudio-core build produced no loadable library")


@pytest.fixture(scope="module")
def core():
    library = ctypes.CDLL(str(_find_or_build_library()))
    library.netaudio_build_set_device_name.argtypes = [
        ctypes.c_char_p,
        ctypes.c_uint16,
        ctypes.POINTER(ctypes.c_uint8),
        ctypes.c_size_t,
        ctypes.POINTER(ctypes.c_size_t),
    ]
    library.netaudio_build_set_device_name.restype = ctypes.c_int
    library.netaudio_service_arc.argtypes = []
    library.netaudio_service_arc.restype = ctypes.c_char_p
    library.netaudio_client_new.argtypes = [
        ctypes.c_char_p,
        ctypes.c_uint16,
        ctypes.c_uint32,
        ctypes.c_uint32,
        ctypes.POINTER(ctypes.c_void_p),
    ]
    library.netaudio_client_new.restype = ctypes.c_int
    library.netaudio_client_set_device_name.argtypes = [ctypes.c_void_p, ctypes.c_char_p]
    library.netaudio_client_set_device_name.restype = ctypes.c_int
    library.netaudio_client_get_channel_count.argtypes = [
        ctypes.c_void_p,
        ctypes.POINTER(ctypes.c_uint16),
        ctypes.POINTER(ctypes.c_uint16),
        ctypes.POINTER(ctypes.c_int32),
    ]
    library.netaudio_client_get_channel_count.restype = ctypes.c_int
    for name in (
        "netaudio_client_get_rx_channels_json",
        "netaudio_client_get_tx_channels_json",
        "netaudio_client_get_property_directory_json",
    ):
        function = getattr(library, name)
        function.argtypes = [
            ctypes.c_void_p,
            ctypes.POINTER(ctypes.c_uint8),
            ctypes.c_size_t,
            ctypes.POINTER(ctypes.c_size_t),
        ]
        function.restype = ctypes.c_int
    library.netaudio_client_get_rx_inventory_json.argtypes = [
        ctypes.c_void_p,
        ctypes.c_uint16,
        ctypes.POINTER(ctypes.c_uint8),
        ctypes.c_size_t,
        ctypes.POINTER(ctypes.c_size_t),
    ]
    library.netaudio_client_get_rx_inventory_json.restype = ctypes.c_int
    library.netaudio_client_free.argtypes = [ctypes.c_void_p]
    library.netaudio_client_free.restype = None
    return library


def rust_client_json(core, function_name, client, capacity=65536):
    buffer = (ctypes.c_uint8 * capacity)()
    length = ctypes.c_size_t(0)
    status = getattr(core, function_name)(client, buffer, capacity, ctypes.byref(length))
    if status != NETAUDIO_OK:
        return status, None
    return status, json.loads(bytes(buffer[: length.value]))


def rust_rx_inventory_json(core, client, rx_count, capacity=65536):
    buffer = (ctypes.c_uint8 * capacity)()
    length = ctypes.c_size_t(0)
    status = core.netaudio_client_get_rx_inventory_json(
        client,
        rx_count,
        buffer,
        capacity,
        ctypes.byref(length),
    )
    if status != NETAUDIO_OK:
        return status, None
    return status, json.loads(bytes(buffer[: length.value]))


class FakeReplayDevice:
    def __init__(self, responses):
        self.responses = list(responses)
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind(("127.0.0.1", 0))
        self.socket.settimeout(3.0)
        self.requests = []
        self.thread = threading.Thread(target=self._serve)

    @property
    def port(self):
        return self.socket.getsockname()[1]

    def _serve(self):
        for response in self.responses:
            try:
                request, source = self.socket.recvfrom(2048)
            except socket.timeout:
                return
            self.requests.append(request)
            patched = bytearray(response)
            patched[4:6] = request[4:6]
            self.socket.sendto(bytes(patched), source)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *exc_info):
        self.thread.join()
        self.socket.close()


def load_fixture(name):
    return (FIXTURES_DIR / name).read_bytes()


class FakeDanteDevice:
    def __init__(self, respond=True, result_code=0x0001):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind(("127.0.0.1", 0))
        self.socket.settimeout(2.0)
        self.respond = respond
        self.result_code = result_code
        self.requests = []
        self.stop_requested = threading.Event()
        self.thread = threading.Thread(target=self._serve)

    @property
    def port(self):
        return self.socket.getsockname()[1]

    def _serve(self):
        try:
            request, source = self.socket.recvfrom(2048)
        except socket.timeout:
            return
        if self.stop_requested.is_set():
            return
        self.requests.append(request)
        if self.respond:
            response = request[0:2] + (10).to_bytes(2, "big") + request[4:8] + self.result_code.to_bytes(2, "big")
            self.socket.sendto(response, source)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *exc_info):
        self.stop_requested.set()
        if self.thread.is_alive():
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as wake_socket:
                wake_socket.sendto(b"", self.socket.getsockname())
        self.thread.join()
        self.socket.close()


@pytest.fixture
def client_factory(core):
    created = []

    def _create(port, timeout_milliseconds=500, attempts=1):
        handle = ctypes.c_void_p()
        status = core.netaudio_client_new(b"127.0.0.1", port, timeout_milliseconds, attempts, ctypes.byref(handle))
        assert status == NETAUDIO_OK
        created.append(handle)
        return handle

    yield _create

    for handle in created:
        core.netaudio_client_free(handle)


def rust_build_set_device_name(core, name, transaction_id=0, capacity=64):
    buffer = (ctypes.c_uint8 * capacity)()
    length = ctypes.c_size_t(0)
    status = core.netaudio_build_set_device_name(
        name.encode("utf-8"), transaction_id, buffer, capacity, ctypes.byref(length)
    )
    return status, bytes(buffer[: length.value])


def _control_packet(opcode, payload, transaction_id, protocol_id=0x27FF):
    import struct

    header = struct.pack(">HH", protocol_id, 8 + len(payload))
    header += struct.pack(">HH", transaction_id, opcode)
    return header + payload


class TestSetDeviceNameParity:
    @pytest.mark.parametrize("name", VALID_NAMES)
    def test_packet_bytes_match_python(self, core, name):
        python_packet, python_service = DanteDeviceCommands().command_set_name(name)
        status, rust_packet = rust_build_set_device_name(core, name)
        assert status == NETAUDIO_OK
        assert rust_packet == python_packet

    @pytest.mark.parametrize("transaction_id", [0x0001, 0x1234, 0xFFFF])
    def test_transaction_id_matches_python(self, core, transaction_id):
        python_packet = _control_packet(
            0x1001,
            b"\x00\x00" + b"Studio-AVIO" + b"\x00",
            transaction_id=transaction_id,
            protocol_id=0x2809,
        )
        status, rust_packet = rust_build_set_device_name(core, "Studio-AVIO", transaction_id=transaction_id)
        assert status == NETAUDIO_OK
        assert rust_packet == python_packet

    def test_service_matches_python(self, core):
        assert core.netaudio_service_arc().decode() == SERVICE_ARC

    @pytest.mark.parametrize("name,expected_status", INVALID_NAMES.items())
    def test_validation_rejects_what_python_rejects(self, core, name, expected_status):
        assert validate_dante_name(name) is not None
        status, packet = rust_build_set_device_name(core, name)
        assert status == expected_status
        assert packet == b""

    @pytest.mark.parametrize("name", VALID_NAMES)
    def test_validation_accepts_what_python_accepts(self, core, name):
        assert validate_dante_name(name) is None
        status, packet = rust_build_set_device_name(core, name)
        assert status == NETAUDIO_OK

    def test_buffer_too_small(self, core):
        status, packet = rust_build_set_device_name(core, "Studio-AVIO", capacity=4)
        assert status == NETAUDIO_BUFFER_TOO_SMALL

    def test_length_field_matches_packet_length(self, core):
        status, packet = rust_build_set_device_name(core, "Studio-AVIO")
        assert status == NETAUDIO_OK
        assert int.from_bytes(packet[2:4], "big") == len(packet)


class TestClientSetDeviceName:
    def test_wire_request_matches_python_builder(self, core, client_factory):
        with FakeDanteDevice() as device:
            client = client_factory(device.port)
            status = core.netaudio_client_set_device_name(client, b"Studio-AVIO")

        assert status == NETAUDIO_OK
        expected = _control_packet(
            0x1001,
            b"\x00\x00" + b"Studio-AVIO" + b"\x00",
            transaction_id=1,
            protocol_id=0x2809,
        )
        assert device.requests == [expected]

    def test_transaction_id_increments_across_calls(self, core, client_factory):
        with FakeDanteDevice() as first_device:
            client = client_factory(first_device.port)
            core.netaudio_client_set_device_name(client, b"First")

        assert int.from_bytes(first_device.requests[0][4:6], "big") == 1

        with FakeDanteDevice() as second_device:
            second_client = client_factory(second_device.port)
            core.netaudio_client_set_device_name(second_client, b"Second")

        assert int.from_bytes(second_device.requests[0][4:6], "big") == 1

    def test_failure_acknowledgement_is_not_reported_as_success(self, core, client_factory):
        with FakeDanteDevice(result_code=0x0600) as device:
            client = client_factory(device.port)
            status = core.netaudio_client_set_device_name(client, b"Studio-AVIO")

        assert status == NETAUDIO_MALFORMED_RESPONSE

    def test_timeout_when_device_silent(self, core, client_factory):
        with FakeDanteDevice(respond=False) as device:
            client = client_factory(device.port, timeout_milliseconds=100)
            status = core.netaudio_client_set_device_name(client, b"Studio-AVIO")

        assert status == NETAUDIO_TIMEOUT
        assert len(device.requests) == 1

    def test_invalid_name_rejected_before_sending(self, core, client_factory):
        with FakeDanteDevice(respond=False) as device:
            client = client_factory(device.port, timeout_milliseconds=100)
            status = core.netaudio_client_set_device_name(client, b"-bad")

        assert status == NETAUDIO_NAME_INVALID_HYPHEN
        assert device.requests == []

    def test_invalid_address_rejected(self, core):
        handle = ctypes.c_void_p()
        status = core.netaudio_client_new(b"not-an-ip", 4440, 100, 1, ctypes.byref(handle))
        assert status == NETAUDIO_INVALID_ADDRESS

    def test_null_client_rejected(self, core):
        status = core.netaudio_client_set_device_name(None, b"Studio-AVIO")
        assert status == NETAUDIO_NULL_POINTER


CHANNEL_FIXTURES = {
    "avio-usb-2": (
        "20250517_200646_478965_avio-usb-2_get_channel_count_response.bin",
        "20250517_200646_499097_avio-usb-2_get_receivers_response.bin",
    ),
    "avio-usb-1": (
        "20250517_200646_445946_avio-usb-1_get_channel_count_response.bin",
        "20250517_200646_463580_avio-usb-1_get_receivers_response.bin",
    ),
    "avio-aes3-1": (
        "20250517_200646_416392_avio-aes3-1_get_channel_count_response.bin",
        "20250517_200646_429145_avio-aes3-1_get_receivers_response.bin",
    ),
}


class TestChannelCountBuilderAndParse:
    def test_channel_count_request_and_parse_match_python(self, core, client_factory):
        count_fixture = load_fixture(CHANNEL_FIXTURES["avio-usb-2"][0])
        with FakeReplayDevice([count_fixture]) as device:
            client = client_factory(device.port)
            tx = ctypes.c_uint16(0)
            rx = ctypes.c_uint16(0)
            locked = ctypes.c_int32(-2)
            status = core.netaudio_client_get_channel_count(
                client, ctypes.byref(tx), ctypes.byref(rx), ctypes.byref(locked)
            )

        assert status == NETAUDIO_OK
        expected_request = DanteDeviceCommands().command_channel_count(transaction_id=1)[0]
        assert device.requests == [expected_request]
        assert tx.value == int.from_bytes(count_fixture[12:14], "big")
        assert rx.value == int.from_bytes(count_fixture[14:16], "big")
        assert locked.value == -1


GOLDEN_RX_CHANNELS = {
    "avio-usb-2": [
        {
            "number": 1,
            "rx_channel_name": "mic-mix-1",
            "rx_status_code": 257,
            "tx_channel_name": "mic-mix-high",
            "tx_device_name": "lx-dante",
            "subscription_status_code": 9,
        },
        {
            "number": 2,
            "rx_channel_name": "mic-mix-2",
            "rx_status_code": 257,
            "tx_channel_name": "mic-mix-high",
            "tx_device_name": "lx-dante",
            "subscription_status_code": 9,
        },
    ],
    "avio-usb-1": [
        {
            "number": 1,
            "rx_channel_name": "mic-mix-1",
            "rx_status_code": 257,
            "tx_channel_name": "mic-mix-high",
            "tx_device_name": "lx-dante",
            "subscription_status_code": 9,
        },
        {
            "number": 2,
            "rx_channel_name": "mic-mix-2",
            "rx_status_code": 257,
            "tx_channel_name": "mic-mix-high",
            "tx_device_name": "lx-dante",
            "subscription_status_code": 9,
        },
    ],
    "avio-aes3-1": [
        {
            "number": 1,
            "rx_channel_name": "unused-1",
            "rx_status_code": 0,
            "tx_channel_name": "linux-mic-mix:high",
            "tx_device_name": "lx-dante",
            "subscription_status_code": 1,
        },
        {
            "number": 2,
            "rx_channel_name": "unused-2",
            "rx_status_code": 0,
            "tx_channel_name": "linux-mic-mix:high",
            "tx_device_name": "lx-dante",
            "subscription_status_code": 1,
        },
    ],
}


class TestRxChannelsGolden:
    @pytest.mark.parametrize("device_name", list(CHANNEL_FIXTURES))
    def test_rx_channels_match_golden(self, core, client_factory, device_name):
        count_fixture = load_fixture(CHANNEL_FIXTURES[device_name][0])
        receivers_fixture = load_fixture(CHANNEL_FIXTURES[device_name][1])

        with FakeReplayDevice([count_fixture, receivers_fixture]) as device:
            client = client_factory(device.port)
            status, rust_channels = rust_client_json(core, "netaudio_client_get_rx_channels_json", client)

        assert status == NETAUDIO_OK

        expected = GOLDEN_RX_CHANNELS[device_name]
        assert len(rust_channels) == len(expected)
        for rust_channel, expected_channel in zip(rust_channels, expected):
            for key, value in expected_channel.items():
                assert rust_channel[key] == value, (
                    f"{device_name} ch{expected_channel['number']} {key}: {rust_channel[key]!r} != {value!r}"
                )

    def test_request_sequence_matches_python_builders(self, core, client_factory):
        count_fixture = load_fixture(CHANNEL_FIXTURES["avio-usb-2"][0])
        receivers_fixture = load_fixture(CHANNEL_FIXTURES["avio-usb-2"][1])

        with FakeReplayDevice([count_fixture, receivers_fixture]) as device:
            client = client_factory(device.port)
            rust_client_json(core, "netaudio_client_get_rx_channels_json", client)

        commands = DanteDeviceCommands()
        assert device.requests[0] == commands.command_channel_count(transaction_id=1)[0]
        assert device.requests[1] == commands.command_receivers(0, transaction_id=2)[0]

    def test_combined_inventory_uses_single_receivers_response(self, core, client_factory):
        receivers_fixture = load_fixture("20250517_200646_289003_lx-dante_get_receivers_response.bin")

        with FakeReplayDevice([receivers_fixture]) as device:
            client = client_factory(device.port)
            status, inventory = rust_rx_inventory_json(core, client, 16)

        assert status == NETAUDIO_OK
        assert len(inventory["channels"]) == 16
        assert inventory["channels"][0]["number"] == 1
        assert inventory["channel_audio_metadata"] == {
            "sample_rate": 48_000,
            "current_encoding": 24,
            "encoding_capability_bitmap": 4,
            "supported_encodings": [24],
        }
        assert len(device.requests) == 1
        assert device.requests[0] == DanteDeviceCommands().command_receivers(0, transaction_id=1)[0]


def test_property_directory_getter_uses_capture_backed_empty_query(core, client_factory):
    response = load_protocol_packet(
        "property_directory",
        "protocol_2729_opcode_1102_id_12358036.bin",
    )

    with FakeReplayDevice([response]) as device:
        client = client_factory(device.port)
        status, directory = rust_client_json(core, "netaudio_client_get_property_directory_json", client)

    assert status == NETAUDIO_OK
    assert directory["aes67_supported"] is False
    assert directory["properties"][0] == {"property_id": 0x8020, "flags": 0x0001}
    assert device.requests == [bytes.fromhex("27ff000a000111020000")]
