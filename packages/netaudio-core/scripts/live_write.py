import ctypes
import json
import sys
from pathlib import Path

CRATE_DIR = Path(__file__).resolve().parent.parent
LIBRARY = CRATE_DIR / "target" / "release" / "libnetaudio_core.so"

NETAUDIO_OK = 0


def load():
    lib = ctypes.CDLL(str(LIBRARY))
    lib.netaudio_client_new.argtypes = [
        ctypes.c_char_p,
        ctypes.c_uint16,
        ctypes.c_uint32,
        ctypes.c_uint32,
        ctypes.POINTER(ctypes.c_void_p),
    ]
    lib.netaudio_client_new.restype = ctypes.c_int
    lib.netaudio_client_execute.argtypes = [
        ctypes.c_void_p,
        ctypes.c_char_p,
        ctypes.POINTER(ctypes.c_uint8),
        ctypes.c_size_t,
        ctypes.POINTER(ctypes.c_size_t),
    ]
    lib.netaudio_client_execute.restype = ctypes.c_int
    lib.netaudio_client_get_rx_channels_json.argtypes = [
        ctypes.c_void_p,
        ctypes.POINTER(ctypes.c_uint8),
        ctypes.c_size_t,
        ctypes.POINTER(ctypes.c_size_t),
    ]
    lib.netaudio_client_get_rx_channels_json.restype = ctypes.c_int
    lib.netaudio_host_mac.argtypes = [ctypes.POINTER(ctypes.c_uint8)]
    lib.netaudio_host_mac.restype = ctypes.c_int
    lib.netaudio_client_free.argtypes = [ctypes.c_void_p]
    lib.netaudio_client_free.restype = None
    return lib


def execute(lib, client, spec):
    buf = (ctypes.c_uint8 * 4096)()
    length = ctypes.c_size_t(0)
    status = lib.netaudio_client_execute(client, json.dumps(spec).encode(), buf, 4096, ctypes.byref(length))
    return status, bytes(buf[: length.value])


def rx_channels(lib, client):
    buf = (ctypes.c_uint8 * 262144)()
    length = ctypes.c_size_t(0)
    status = lib.netaudio_client_get_rx_channels_json(client, buf, 262144, ctypes.byref(length))
    assert status == NETAUDIO_OK, status
    return json.loads(bytes(buf[: length.value]))


def main():
    device_ip = sys.argv[1]
    lib = load()

    mac = (ctypes.c_uint8 * 6)()
    if lib.netaudio_host_mac(mac) == NETAUDIO_OK:
        print("host_mac:", ":".join(f"{b:02x}" for b in mac))
    else:
        print("host_mac: discovery failed")

    handle = ctypes.c_void_p()
    assert lib.netaudio_client_new(device_ip.encode(), 4440, 1000, 3, ctypes.byref(handle)) == NETAUDIO_OK

    before = rx_channels(lib, handle)
    original = before[0]["rx_channel_name"]
    print(f"rx channel 1 original name: {original!r}")

    status, response = execute(
        lib, handle, {"command": "set_channel_name", "channel_type": "rx", "channel_number": 1, "name": "rusttest-1"}
    )
    print(f"rename -> status {status}, {len(response)} byte response")

    after = rx_channels(lib, handle)
    print(f"rx channel 1 name after rename: {after[0]['rx_channel_name']!r}")

    status, _ = execute(
        lib, handle, {"command": "set_channel_name", "channel_type": "rx", "channel_number": 1, "name": original}
    )
    restored = rx_channels(lib, handle)
    print(f"rx channel 1 name after restore: {restored[0]['rx_channel_name']!r}")

    status, _ = execute(lib, handle, {"command": "identify", "sequence": 1})
    print(f"identify -> status {status} (LED should blink)")

    lib.netaudio_client_free(handle)


if __name__ == "__main__":
    main()
