import ctypes
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from netaudio.core import binding
from netaudio.core.binding import CoreClient, NetaudioCoreLibraryMissing, lock_token


class ExecuteLibrary:
    def __init__(self, response):
        self.response = response
        self.spec = None

    def netaudio_client_execute(self, _handle, encoded_spec, out, capacity, out_length):
        self.spec = json.loads(encoded_spec)
        assert len(self.response) <= capacity
        for index, byte in enumerate(self.response):
            out[index] = byte
        out_length._obj.value = len(self.response)
        return 0

    def netaudio_client_free(self, _handle):
        pass


@pytest.mark.parametrize(
    ("response", "expected"),
    [
        (b"\x27\xff\x00\x0a\x00\x01\x10\x00\x00\x00", b"\x27\xff\x00\x0a\x00\x01\x10\x00\x00\x00"),
        (b"", None),
    ],
)
def test_execute_returns_raw_protocol_response_or_none(response, expected):
    library = ExecuteLibrary(response)
    client = CoreClient.__new__(CoreClient)
    client._lib = library
    client._device_ip = "192.0.2.1"
    client.observer = None
    client._handle = ctypes.c_void_p(1)
    client._native_lock = threading.RLock()

    try:
        assert client.execute({"command": "channel_count"}) == expected
        assert library.spec == {"command": "channel_count"}
    finally:
        client.close()


@pytest.mark.parametrize("nonce_length", [0, 23, 25])
def test_lock_token_rejects_invalid_nonce_lengths_before_ffi(nonce_length):
    with pytest.raises(ValueError, match="nonce must be exactly 24 bytes"):
        lock_token("1234", b"n" * nonce_length, b"k" * 32)


@pytest.mark.parametrize("key_length", [0, 31, 33])
def test_lock_token_rejects_invalid_key_lengths_before_ffi(key_length):
    with pytest.raises(ValueError, match="key must be exactly 32 bytes"):
        lock_token("1234", b"n" * 24, b"k" * key_length)


@pytest.mark.parametrize("method_name", ["lock", "unlock"])
def test_client_lock_operations_reject_invalid_key_before_ffi(method_name):
    client = CoreClient.__new__(CoreClient)
    client._handle = ctypes.c_void_p()

    with pytest.raises(ValueError, match="key must be exactly 32 bytes"):
        getattr(client, method_name)("1234", b"short")


class ConcurrentChannelCountLibrary:
    def __init__(self):
        self._state_lock = threading.Lock()
        self.active_calls = 0
        self.maximum_active_calls = 0

    def netaudio_client_get_channel_count(self, _handle, tx, rx, locked):
        with self._state_lock:
            self.active_calls += 1
            self.maximum_active_calls = max(self.maximum_active_calls, self.active_calls)
        try:
            time.sleep(0.01)
            tx._obj.value = 260
            rx._obj.value = 520
            locked._obj.value = 1
            return 0
        finally:
            with self._state_lock:
                self.active_calls -= 1

    def netaudio_client_free(self, _handle):
        pass


def test_client_serializes_concurrent_native_calls_per_instance():
    library = ConcurrentChannelCountLibrary()
    client = CoreClient.__new__(CoreClient)
    client._lib = library
    client._handle = ctypes.c_void_p(1)
    client._native_lock = threading.RLock()

    try:
        with ThreadPoolExecutor(max_workers=16) as executor:
            results = list(executor.map(lambda _index: client.get_channel_count(), range(32)))
    finally:
        client.close()

    assert results == [(260, 520, True)] * 32
    assert library.maximum_active_calls == 1


def test_require_reports_missing_library_without_io_error_status(monkeypatch):
    missing = Path("/tmp/netaudio-core-missing.so")
    monkeypatch.setattr(binding, "_library", None)
    monkeypatch.setattr(binding, "_load_attempted", False)
    monkeypatch.setattr(binding, "_load_failures", [])
    monkeypatch.setattr(binding, "_candidate_paths", lambda: (missing,))

    with pytest.raises(NetaudioCoreLibraryMissing) as exception:
        binding.require()

    message = str(exception.value)
    assert "io error" not in message
    assert "ABI-incompatible" not in message
    assert "make core" in message
    assert f"{missing}: not found" in message


@pytest.mark.parametrize(
    ("status", "category", "label"),
    [
        (10, "binary_response", "malformed binary response"),
        (8, "transport", "io error"),
        (9, "transport", "device did not respond"),
        (11, "api_serialization", "FFI/API serialization failure"),
        (13, "json_input", "invalid command json"),
    ],
)
def test_core_errors_distinguish_binary_transport_and_serialization(monkeypatch, status, category, label):
    monkeypatch.setattr(binding, "last_error_message", lambda: "")
    error = binding.NetaudioCoreError(status, "transmitter names")
    assert error.category == category
    assert error.context == "transmitter names"
    assert label in str(error)
    assert "malformed JSON" not in str(error)


@pytest.mark.parametrize("payload", [b"{", b"not JSON", b"\xff"])
def test_invalid_ffi_json_has_a_distinct_decoding_error(payload):
    with pytest.raises(binding.NetaudioCoreJsonError, match="JSON decoding failed") as caught:
        binding._decode_json_output(payload, "test getter")
    assert caught.value.category == "json_decoding"
    assert caught.value.context == "test getter"
    assert repr(payload) not in str(caught.value)


def test_invalid_command_object_is_a_json_encoding_error():
    with pytest.raises(binding.NetaudioCoreJsonError, match="JSON encoding failed") as caught:
        binding._encode_command_spec({"command": object()})
    assert caught.value.category == "json_encoding"


def test_exact_issue_59_names_parse_through_python_ffi():
    from tests.issue_59_fixtures import packet

    data = packet("transmitter_names.bin")
    assert len(data) == 199
    assert binding.parse_page("tx_friendly", data, 1) == [[number, f"TX {number}"] for number in range(1, 17)]
