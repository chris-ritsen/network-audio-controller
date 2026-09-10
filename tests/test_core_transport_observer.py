import ctypes
import json
import threading

from netaudio import core
from netaudio.dante.core_transport import CoreTransport


class _Function:
    def __init__(self, behavior):
        self.behavior = behavior
        self.calls = []

    def __call__(self, *arguments):
        self.calls.append(arguments)
        return self.behavior(*arguments)


class _Library:
    def __init__(self, captures):
        payload = json.dumps(captures).encode()

        def read(handle, out, capacity, length_reference):
            length_reference._obj.value = len(payload)
            if capacity < len(payload):
                return 6
            out[: len(payload)] = payload
            return core.STATUS_OK

        self.netaudio_client_clear_wire_captures = _Function(lambda handle: core.STATUS_OK)
        self.netaudio_client_get_wire_captures_json = _Function(read)


class _Client:
    def __init__(self, library):
        self._device_ip = "192.0.2.10"
        self._handle = ctypes.c_void_p(1)
        self._library = library

    def _require_library(self):
        return self._library


def test_core_module_exports_status_constants_used_by_transport():
    assert core.STATUS_OK == 0
    assert core.STATUS_TIMEOUT == 9
    assert core.STATUS_INVALID_SEQUENCE == 31


def test_observer_receives_wire_captures_around_operation():
    library = _Library(
        [
            {"payload_hex": "ffff0010", "port": 4440, "direction": "sent"},
            {"payload_hex": "ffff0020", "port": 4440, "direction": "received"},
        ]
    )
    client = _Client(library)
    observed = []
    transport = CoreTransport(
        observer=lambda payload, ip, port, direction: observed.append((payload, ip, port, direction))
    )

    result = transport._call_and_observe(client, threading.Lock(), lambda active: ("done", active))

    assert result == ("done", client)
    assert len(library.netaudio_client_clear_wire_captures.calls) == 1
    assert observed == [
        (bytes.fromhex("ffff0010"), "192.0.2.10", 4440, "sent"),
        (bytes.fromhex("ffff0020"), "192.0.2.10", 4440, "received"),
    ]


def test_operation_without_observer_skips_wire_capture_functions():
    library = _Library([])
    client = _Client(library)
    transport = CoreTransport()

    assert transport._call_and_observe(client, threading.Lock(), lambda active: "plain") == "plain"
    assert library.netaudio_client_clear_wire_captures.calls == []
    assert library.netaudio_client_get_wire_captures_json.calls == []


def test_core_package_exports_every_public_binding_name():
    from netaudio.core import binding

    public_names = {
        name
        for name, value in vars(binding).items()
        if not name.startswith("_")
        and ((name.isupper() and isinstance(value, int)) or getattr(value, "__module__", None) == binding.__name__)
    }
    assert public_names <= set(core.__all__)
    assert set(core.__all__) <= set(vars(core))
