import ctypes
import shutil
import struct
import subprocess
from pathlib import Path

import nacl.bindings
import pytest

CRATE_DIR = Path(__file__).parent.parent / "packages" / "netaudio-core"

NETAUDIO_OK = 0
NETAUDIO_INVALID_PIN = 18

LOCK_DDP_HEADER = struct.pack(">HHHH", 8, 0x0001, 0x1000, 0x0200)


def _find_or_build_library():
    for profile in ("release", "debug"):
        library_path = CRATE_DIR / "target" / profile / "libnetaudio_core.so"
        if library_path.exists():
            return library_path
    if not shutil.which("cargo"):
        pytest.skip("netaudio-core not built and cargo not available")
    subprocess.run(["cargo", "build"], cwd=CRATE_DIR, check=True, capture_output=True)
    return CRATE_DIR / "target" / "debug" / "libnetaudio_core.so"


@pytest.fixture(scope="module")
def lib():
    library = ctypes.CDLL(str(_find_or_build_library()))
    library.netaudio_lock_token.argtypes = [
        ctypes.c_char_p,
        ctypes.POINTER(ctypes.c_uint8),
        ctypes.POINTER(ctypes.c_uint8),
        ctypes.POINTER(ctypes.c_uint8),
        ctypes.c_size_t,
        ctypes.POINTER(ctypes.c_size_t),
    ]
    library.netaudio_lock_token.restype = ctypes.c_int
    return library


def rust_token(lib, pin, nonce, key, capacity=256):
    nonce_buf = (ctypes.c_uint8 * len(nonce)).from_buffer_copy(nonce)
    key_buf = (ctypes.c_uint8 * len(key)).from_buffer_copy(key)
    out = (ctypes.c_uint8 * capacity)()
    length = ctypes.c_size_t(0)
    status = lib.netaudio_lock_token(
        pin.encode(), nonce_buf, key_buf, out, capacity, ctypes.byref(length)
    )
    return status, bytes(out[: length.value])


def python_token(pin, nonce, key):
    return nacl.bindings.crypto_secretbox(pin.encode("ascii"), nonce, key)


KEYS = [
    b"0123456789abcdef0123456789abcdef",
    b"\x00" * 32,
    bytes(range(32)),
]
NONCES = [
    b"\x11" * 24,
    bytes(range(24)),
    bytes(range(100, 124)),
]
PINS = ["1234", "0000", "9876"]


class TestLockTokenParity:
    @pytest.mark.parametrize("key", KEYS)
    @pytest.mark.parametrize("nonce", NONCES)
    @pytest.mark.parametrize("pin", PINS)
    def test_token_matches_pynacl(self, lib, key, nonce, pin):
        status, rust = rust_token(lib, pin, nonce, key)
        assert status == NETAUDIO_OK
        assert rust == python_token(pin, nonce, key)


class TestLockFraming:
    def test_challenge_request_framing_agrees(self):
        python_packet = LOCK_DDP_HEADER + struct.pack(
            ">HHHHHH", 0x000C, 0x2FFE, 0x0004, 0x0000, 1, 0x0000
        )
        rust_unit_expected = bytes(
            [0x00, 0x08, 0x00, 0x01, 0x10, 0x00, 0x02, 0x00, 0x00, 0x0C, 0x2F, 0xFE, 0x00, 0x04,
             0x00, 0x00, 0x00, 0x01, 0x00, 0x00]
        )
        assert python_packet == rust_unit_expected

    def test_auth_header_framing_agrees(self):
        auth_header = struct.pack(
            ">HHHHHHHHH", 0x0028, 0x2FFF, 0x0004, 0x0008, 1, 0x0000, 1, 0x0014, 0x0014
        )
        python_prefix = LOCK_DDP_HEADER + auth_header + struct.pack(">H", 0x0000)
        rust_unit_expected = bytes(
            [0x00, 0x08, 0x00, 0x01, 0x10, 0x00, 0x02, 0x00, 0x00, 0x28, 0x2F, 0xFF, 0x00, 0x04,
             0x00, 0x08, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x14, 0x00, 0x14, 0x00, 0x00]
        )
        assert python_prefix == rust_unit_expected
