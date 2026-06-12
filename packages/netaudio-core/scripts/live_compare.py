import ctypes
import json
import sys
from pathlib import Path

CRATE_DIR = Path(__file__).resolve().parent.parent
LIBRARY = CRATE_DIR / "target" / "release" / "libnetaudio_core.so"

NETAUDIO_OK = 0


def load_library():
    library = ctypes.CDLL(str(LIBRARY))
    library.netaudio_client_new.argtypes = [
        ctypes.c_char_p,
        ctypes.c_uint16,
        ctypes.c_uint32,
        ctypes.c_uint32,
        ctypes.POINTER(ctypes.c_void_p),
    ]
    library.netaudio_client_new.restype = ctypes.c_int
    library.netaudio_client_get_channel_count.argtypes = [
        ctypes.c_void_p,
        ctypes.POINTER(ctypes.c_uint16),
        ctypes.POINTER(ctypes.c_uint16),
        ctypes.POINTER(ctypes.c_int32),
    ]
    library.netaudio_client_get_channel_count.restype = ctypes.c_int
    for name in ("netaudio_client_get_rx_channels_json", "netaudio_client_get_tx_channels_json"):
        function = getattr(library, name)
        function.argtypes = [
            ctypes.c_void_p,
            ctypes.POINTER(ctypes.c_uint8),
            ctypes.c_size_t,
            ctypes.POINTER(ctypes.c_size_t),
        ]
        function.restype = ctypes.c_int
    library.netaudio_client_free.argtypes = [ctypes.c_void_p]
    library.netaudio_client_free.restype = None
    return library


def get_json(library, function_name, client, capacity=262144):
    buffer = (ctypes.c_uint8 * capacity)()
    length = ctypes.c_size_t(0)
    status = getattr(library, function_name)(client, buffer, capacity, ctypes.byref(length))
    if status != NETAUDIO_OK:
        raise RuntimeError(f"{function_name} returned status {status}")
    return json.loads(bytes(buffer[: length.value]))


def main():
    device_ip = sys.argv[1]
    arc_port = int(sys.argv[2]) if len(sys.argv) > 2 else 4440
    library = load_library()

    handle = ctypes.c_void_p()
    status = library.netaudio_client_new(
        device_ip.encode(), arc_port, 1000, 3, ctypes.byref(handle)
    )
    if status != NETAUDIO_OK:
        raise SystemExit(f"client_new failed: {status}")

    tx = ctypes.c_uint16(0)
    rx = ctypes.c_uint16(0)
    locked = ctypes.c_int32(-2)
    library.netaudio_client_get_channel_count(
        handle, ctypes.byref(tx), ctypes.byref(rx), ctypes.byref(locked)
    )

    result = {
        "device_ip": device_ip,
        "tx_count": tx.value,
        "rx_count": rx.value,
        "locked": locked.value,
        "rx_channels": get_json(library, "netaudio_client_get_rx_channels_json", handle),
        "tx_channels": get_json(library, "netaudio_client_get_tx_channels_json", handle),
    }
    library.netaudio_client_free(handle)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
