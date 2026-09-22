import ctypes
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeout
from pathlib import Path

import pytest

from netaudio.core import binding
from netaudio.core.binding import CoreClient, NetaudioCoreLibraryMissing, lock_token
from tests.test_core_protocol import FakeDanteDevice, FakeReplayDevice, load_fixture


@pytest.mark.parametrize("result_code", [1, 1536])
def test_execute_preserves_native_acceptance_and_rejection_responses(result_code):
    with FakeDanteDevice(result_code=result_code) as device:
        with CoreClient("127.0.0.1", arc_port=device.port, local_ip="127.0.0.1", attempts=1) as client:
            response = client.execute({"command": "set_name", "name": "Studio-AVIO"})

    assert device.requests == [bytes.fromhex("2809001600011001000053747564696f2d4156494f00")]
    assert response == bytes.fromhex("2809000a00011001") + result_code.to_bytes(2, "big")


@pytest.mark.parametrize("nonce_length", [0, 23, 25])
def test_lock_token_preserves_native_nonce_validation(nonce_length):
    with pytest.raises(binding.NetaudioCoreError, match="nonce must be exactly 24 bytes"):
        lock_token("1234", b"n" * nonce_length, b"k" * 32)


@pytest.mark.parametrize("pin", ["١٢٣٤", "1234\x00ignored", "12", "abcd"])
def test_lock_token_rejects_invalid_pin_without_coercion(pin):
    with pytest.raises(binding.NetaudioCoreError, match="pin must be exactly 4 digits"):
        lock_token(pin, b"n" * 24, b"k" * 32)


@pytest.mark.parametrize("operation", ["lock", "unlock"])
def test_client_rejects_embedded_null_pin_before_network(operation):
    with CoreClient("127.0.0.1", local_ip="127.0.0.1", timeout_ms=10, attempts=1) as client:
        with pytest.raises(binding.NetaudioCoreError, match="pin must be exactly 4 digits"):
            getattr(client, operation)("1234\x00ignored", b"k" * 32)


@pytest.mark.parametrize("case", ["rx_only", "tx_fallback", "unavailable", "empty"])
def test_channel_audio_probe_fallback_uses_real_native_transport(case):
    reply = load_fixture("20250517_200646_289003_lx-dante_get_receivers_response.bin")
    rejected = bytes.fromhex("27ff000a000020000030")
    responses = {
        "rx_only": [reply],
        "tx_fallback": [rejected, reply],
        "unavailable": [rejected, rejected],
        "empty": [],
    }[case]

    with FakeReplayDevice(responses) as device:
        with CoreClient("127.0.0.1", arc_port=device.port, local_ip="127.0.0.1", attempts=1) as client:
            result = client.get_channel_audio_metadata(
                0 if case in {"rx_only", "empty"} else 1, 0 if case == "empty" else 1
            )

    assert [int.from_bytes(request[6:8], "big") for request in device.requests] == {
        "rx_only": [0x3000],
        "tx_fallback": [0x2000, 0x3000],
        "unavailable": [0x2000, 0x3000],
        "empty": [],
    }[case]
    assert result == (
        {"sample_rate": 48000, "current_encoding": 24, "encoding_capability_bitmap": 4, "supported_encodings": [24]}
        if case in {"rx_only", "tx_fallback"}
        else None
    )


@pytest.mark.parametrize("value", [True, -1, 65536])
@pytest.mark.parametrize("direction", ["tx", "rx"])
def test_channel_audio_probe_rejects_counts_that_cannot_cross_ffi(value, direction):
    with CoreClient("127.0.0.1", local_ip="127.0.0.1", timeout_ms=10, attempts=1) as client:
        with pytest.raises(ValueError, match="channel count"):
            client.get_channel_audio_metadata(value if direction == "tx" else 0, value if direction == "rx" else 0)


@pytest.mark.parametrize("key_length", [0, 31, 33])
def test_lock_token_preserves_native_key_validation(key_length):
    with pytest.raises(binding.NetaudioCoreError, match="key must be exactly 32 bytes"):
        lock_token("1234", b"n" * 24, b"k" * key_length)


@pytest.mark.parametrize("method_name", ["lock", "unlock"])
def test_client_lock_operations_preserve_native_key_validation(method_name):
    with CoreClient("127.0.0.1", local_ip="127.0.0.1", timeout_ms=10, attempts=1) as client:
        with pytest.raises(binding.NetaudioCoreError, match="key must be exactly 32 bytes"):
            getattr(client, method_name)("1234", b"short")


class ConcurrentChannelCountLibrary:
    def __init__(self):
        self._state_lock = threading.Lock()
        self.active_calls = 0
        self.maximum_active_calls = 0

    def netaudio_client_get_channel_count(self, _handle, tx, rx, capability_word, locked):
        with self._state_lock:
            self.active_calls += 1
            self.maximum_active_calls = max(self.maximum_active_calls, self.active_calls)
        try:
            time.sleep(0.01)
            tx._obj.value = 260
            rx._obj.value = 520
            capability_word._obj.value = 0x1030
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

    assert results == [(260, 520, True, 0x1030)] * 32
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


def test_concurrent_first_use_waits_for_native_library_configuration(monkeypatch):
    path = Path(binding.require()._name)
    configure = binding._configure
    entered = threading.Event()
    release = threading.Event()
    second_started = threading.Event()
    monkeypatch.setattr(binding, "_library", None)
    monkeypatch.setattr(binding, "_load_attempted", False)
    monkeypatch.setattr(binding, "_load_failures", [])
    monkeypatch.setattr(binding, "_candidate_paths", lambda: (path,))

    def blocked_configure(library):
        entered.set()
        assert release.wait(2)
        return configure(library)

    def second_load():
        second_started.set()
        return binding.require()

    monkeypatch.setattr(binding, "_configure", blocked_configure)

    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(binding.require)

        try:
            assert entered.wait(1)
            second = executor.submit(second_load)
            assert second_started.wait(1)

            with pytest.raises(FutureTimeout):
                second.result(timeout=0.05)
        finally:
            release.set()

        assert first.result(timeout=1) is second.result(timeout=1)


def test_missing_native_export_does_not_hide_a_usable_candidate(monkeypatch):
    path = Path(binding.require()._name)
    configure = binding._configure
    attempts = []
    monkeypatch.setattr(binding, "_library", None)
    monkeypatch.setattr(binding, "_load_attempted", False)
    monkeypatch.setattr(binding, "_load_failures", [])
    monkeypatch.setattr(binding, "_candidate_paths", lambda: (path, path))

    def configure_candidate(library):
        attempts.append(library)

        if len(attempts) == 1:
            raise AttributeError("missing native export")

        return configure(library)

    monkeypatch.setattr(binding, "_configure", configure_candidate)

    assert binding.require() is attempts[1]
    assert "missing native export" in binding._load_failures[0][1]


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
