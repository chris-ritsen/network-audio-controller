import ctypes
import json
import socket
import threading
from pathlib import Path

import pytest

from netaudio.dante.commands import validate_dante_name
from netaudio import core as native_core
from netaudio.core import _abi as abi
from tests.protocol_test_fixtures import load_protocol_packet

FIXTURES_DIR = Path(__file__).parent / "fixtures"

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
    "a" * 32: abi.STATUS_NAME_TOO_LONG,
    "ü" * 32: abi.STATUS_NAME_TOO_LONG,
    "-leading": abi.STATUS_NAME_INVALID_HYPHEN,
    "trailing-": abi.STATUS_NAME_INVALID_HYPHEN,
    "-both-": abi.STATUS_NAME_INVALID_HYPHEN,
    "": abi.STATUS_NAME_INVALID_CHARS,
    "under_score": abi.STATUS_NAME_INVALID_CHARS,
    "has space": abi.STATUS_NAME_INVALID_CHARS,
    "über": abi.STATUS_NAME_INVALID_CHARS,
    "dot.name": abi.STATUS_NAME_INVALID_CHARS,
    "Studio\n": abi.STATUS_NAME_INVALID_CHARS,
    "Studio\r": abi.STATUS_NAME_INVALID_CHARS,
    "Studio\x00": abi.STATUS_NAME_INVALID_CHARS,
}


@pytest.fixture(scope="module")
def core():
    return native_core.require()


def rust_client_json(core, function_name, client, capacity=65536):
    buffer = (ctypes.c_uint8 * capacity)()
    length = ctypes.c_size_t(0)
    status = getattr(core, function_name)(client, buffer, capacity, ctypes.byref(length))
    if status != abi.STATUS_OK:
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
    if status != abi.STATUS_OK:
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
        status = core.netaudio_client_new(
            b"127.0.0.1", None, port, timeout_milliseconds, attempts, ctypes.byref(handle)
        )
        assert status == abi.STATUS_OK
        created.append(handle)
        return handle

    yield _create

    for handle in created:
        core.netaudio_client_free(handle)


def rust_build_set_device_name(core, name, transaction_id=0, capacity=64):
    buffer = (ctypes.c_uint8 * capacity)()
    length = ctypes.c_size_t(0)
    specification = json.dumps({"command": "set_name", "name": name, "message_id": transaction_id})
    status = core.netaudio_build_command(specification.encode("utf-8"), buffer, capacity, ctypes.byref(length))
    return status, bytes(buffer[: length.value])


def _control_packet(opcode, payload, transaction_id, protocol_id=0x27FF):
    import struct

    header = struct.pack(">HH", protocol_id, 8 + len(payload))
    header += struct.pack(">HH", transaction_id, opcode)
    return header + payload


class TestDeviceNames:
    @pytest.mark.parametrize("transaction_id", [0x0001, 0x1234, 0xFFFF])
    def test_transaction_id_matches_python(self, core, transaction_id):
        python_packet = _control_packet(
            0x1001,
            b"\x00\x00" + b"Studio-AVIO" + b"\x00",
            transaction_id=transaction_id,
            protocol_id=0x2809,
        )
        status, rust_packet = rust_build_set_device_name(core, "Studio-AVIO", transaction_id=transaction_id)
        assert status == abi.STATUS_OK
        assert rust_packet == python_packet

    @pytest.mark.parametrize("name,expected_status", INVALID_NAMES.items())
    def test_invalid_names_rejected_by_native_and_frontend(self, core, name, expected_status):
        assert validate_dante_name(name) is not None
        status, packet = rust_build_set_device_name(core, name)
        assert status == expected_status
        assert packet == b""

    @pytest.mark.parametrize("name", VALID_NAMES)
    def test_valid_names_accepted_by_native_and_frontend(self, core, name):
        assert validate_dante_name(name) is None
        status, packet = rust_build_set_device_name(core, name)
        assert status == abi.STATUS_OK

    def test_buffer_too_small(self, core):
        status, packet = rust_build_set_device_name(core, "Studio-AVIO", capacity=4)
        assert status == abi.STATUS_BUFFER_TOO_SMALL


class TestClientSetDeviceName:
    def test_wire_request_matches_python_builder(self, core, client_factory):
        with FakeDanteDevice() as device:
            client = client_factory(device.port)
            status = core.netaudio_client_set_device_name(client, b"Studio-AVIO")

        assert status == abi.STATUS_OK
        expected = _control_packet(
            0x1001,
            b"\x00\x00" + b"Studio-AVIO" + b"\x00",
            transaction_id=1,
            protocol_id=0x2809,
        )
        assert device.requests == [expected]

    def test_transaction_id_increments_across_calls(self, core, client_factory):
        acknowledgement = _control_packet(0x1001, b"\x00\x01", 1, protocol_id=0x2809)

        with FakeReplayDevice([acknowledgement, acknowledgement]) as device:
            client = client_factory(device.port)
            assert core.netaudio_client_set_device_name(client, b"First") == abi.STATUS_OK
            assert core.netaudio_client_set_device_name(client, b"Second") == abi.STATUS_OK

        assert [int.from_bytes(request[4:6], "big") for request in device.requests] == [1, 2]

    def test_failure_acknowledgement_is_not_reported_as_success(self, core, client_factory):
        with FakeDanteDevice(result_code=0x0600) as device:
            client = client_factory(device.port)
            status = core.netaudio_client_set_device_name(client, b"Studio-AVIO")

        assert status == abi.STATUS_MALFORMED_RESPONSE

    def test_timeout_when_device_silent(self, core, client_factory):
        with FakeDanteDevice(respond=False) as device:
            client = client_factory(device.port, timeout_milliseconds=100)
            status = core.netaudio_client_set_device_name(client, b"Studio-AVIO")

        assert status == abi.STATUS_TIMEOUT
        assert len(device.requests) == 1

    def test_invalid_name_rejected_before_sending(self, core, client_factory):
        with FakeDanteDevice(respond=False) as device:
            client = client_factory(device.port, timeout_milliseconds=100)
            status = core.netaudio_client_set_device_name(client, b"-bad")

        assert status == abi.STATUS_NAME_INVALID_HYPHEN
        assert device.requests == []

    def test_invalid_address_rejected(self, core):
        handle = ctypes.c_void_p()
        status = core.netaudio_client_new(b"not-an-ip", None, 4440, 100, 1, ctypes.byref(handle))
        assert status == abi.STATUS_INVALID_ADDRESS

    def test_null_client_rejected(self, core):
        status = core.netaudio_client_set_device_name(None, b"Studio-AVIO")
        assert status == abi.STATUS_NULL_POINTER


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
            transmit_flow_authoring_capability_word = ctypes.c_uint16(0)
            locked = ctypes.c_int32(-2)
            status = core.netaudio_client_get_channel_count(
                client,
                ctypes.byref(tx),
                ctypes.byref(rx),
                ctypes.byref(transmit_flow_authoring_capability_word),
                ctypes.byref(locked),
            )

        assert status == abi.STATUS_OK
        expected_request = native_core.build_command({"command": "channel_count", "message_id": 1})
        assert device.requests == [expected_request]
        assert tx.value == int.from_bytes(count_fixture[12:14], "big")
        assert rx.value == int.from_bytes(count_fixture[14:16], "big")
        assert transmit_flow_authoring_capability_word.value == int.from_bytes(count_fixture[10:12], "big")
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

        assert status == abi.STATUS_OK

        expected = GOLDEN_RX_CHANNELS[device_name]
        assert len(rust_channels) == len(expected)
        for rust_channel, expected_channel in zip(rust_channels, expected):
            for key, value in expected_channel.items():
                assert rust_channel[key] == value, (
                    f"{device_name} ch{expected_channel['number']} {key}: {rust_channel[key]!r} != {value!r}"
                )

    def test_request_sequence_matches_native_commands(self, core, client_factory):
        count_fixture = load_fixture(CHANNEL_FIXTURES["avio-usb-2"][0])
        receivers_fixture = load_fixture(CHANNEL_FIXTURES["avio-usb-2"][1])

        with FakeReplayDevice([count_fixture, receivers_fixture]) as device:
            client = client_factory(device.port)
            rust_client_json(core, "netaudio_client_get_rx_channels_json", client)

        assert device.requests[0] == native_core.build_command({"command": "channel_count", "message_id": 1})
        assert device.requests[1] == native_core.build_command({"command": "receivers", "page": 0, "message_id": 2})

    def test_combined_inventory_uses_single_receivers_response(self, core, client_factory):
        receivers_fixture = load_fixture("20250517_200646_289003_lx-dante_get_receivers_response.bin")

        with FakeReplayDevice([receivers_fixture]) as device:
            client = client_factory(device.port)
            status, inventory = rust_rx_inventory_json(core, client, 16)

        assert status == abi.STATUS_OK
        assert len(inventory["channels"]) == 16
        assert inventory["channels"][0]["number"] == 1
        assert inventory["channel_audio_metadata"] == {
            "sample_rate": 48_000,
            "current_encoding": 24,
            "encoding_capability_bitmap": 4,
            "supported_encodings": [24],
        }
        assert len(device.requests) == 1
        assert device.requests[0] == native_core.build_command({"command": "receivers", "page": 0, "message_id": 1})


def test_property_directory_getter_uses_capture_backed_empty_query(core, client_factory):
    response = load_protocol_packet(
        "property_directory",
        "protocol_2729_opcode_1102_id_12358036.bin",
    )

    with FakeReplayDevice([response]) as device:
        client = client_factory(device.port)
        status, directory = rust_client_json(core, "netaudio_client_get_property_directory_json", client)

    assert status == abi.STATUS_OK
    assert directory["aes67_configured_property_advertised"] is False
    assert directory["properties"][0] == {"property_id": 0x8020, "flags": 0x0001}
    assert device.requests == [bytes.fromhex("27ff000a000111020000")]
