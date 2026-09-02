import ctypes
import shutil
import struct
import subprocess
import sys
from pathlib import Path

import nacl.bindings
import pytest

from netaudio.core.binding import lock_token as binding_lock_token

CRATE_DIR = Path(__file__).parent.parent / "packages" / "netaudio-core"

NETAUDIO_OK = 0
NETAUDIO_INVALID_KEY = 17
NETAUDIO_INVALID_PIN = 18
NETAUDIO_CRYPTO_ERROR = 19
NETAUDIO_INVALID_LENGTH = 35

LOCK_DDP_HEADER = struct.pack(">HHHH", 8, 0x0001, 0x1000, 0x0200)


def _library_name():
    if sys.platform == "darwin":
        return "libnetaudio_core.dylib"
    if sys.platform == "win32":
        return "netaudio_core.dll"
    return "libnetaudio_core.so"


def _find_or_build_library():
    if not shutil.which("cargo"):
        pytest.skip("cargo is required to build netaudio-core for lock parity tests")
    subprocess.run(["cargo", "build"], cwd=CRATE_DIR, check=True, capture_output=True)
    library_path = CRATE_DIR / "target" / "debug" / _library_name()
    if library_path.exists():
        return library_path
    pytest.skip("netaudio-core build produced no loadable library")


@pytest.fixture(scope="module")
def lib():
    library = ctypes.CDLL(str(_find_or_build_library()))
    library.netaudio_lock_token.argtypes = [
        ctypes.c_char_p,
        ctypes.POINTER(ctypes.c_uint8),
        ctypes.c_size_t,
        ctypes.POINTER(ctypes.c_uint8),
        ctypes.c_size_t,
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
        pin.encode(),
        nonce_buf,
        len(nonce),
        key_buf,
        len(key),
        out,
        capacity,
        ctypes.byref(length),
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

    def test_python_binding_preserves_valid_token_parity(self, lib):
        assert lib is not None
        assert binding_lock_token(PINS[0], NONCES[0], KEYS[0]) == python_token(PINS[0], NONCES[0], KEYS[0])

    @pytest.mark.parametrize("nonce_length", [0, 1, 23, 25, 64])
    def test_rejects_invalid_nonce_lengths(self, lib, nonce_length):
        status, token = rust_token(lib, "1234", b"n" * nonce_length, KEYS[0])
        assert status == NETAUDIO_INVALID_LENGTH
        assert token == b""

    @pytest.mark.parametrize("key_length", [0, 1, 31, 33, 64])
    def test_rejects_invalid_key_lengths(self, lib, key_length):
        status, token = rust_token(lib, "1234", NONCES[0], b"k" * key_length)
        assert status == NETAUDIO_INVALID_KEY
        assert token == b""


class TestLockFraming:
    def test_challenge_request_framing_agrees(self):
        python_packet = LOCK_DDP_HEADER + struct.pack(">HHHHHH", 0x000C, 0x2FFE, 0x0004, 0x0000, 1, 0x0000)
        rust_unit_expected = bytes(
            [
                0x00,
                0x08,
                0x00,
                0x01,
                0x10,
                0x00,
                0x02,
                0x00,
                0x00,
                0x0C,
                0x2F,
                0xFE,
                0x00,
                0x04,
                0x00,
                0x00,
                0x00,
                0x01,
                0x00,
                0x00,
            ]
        )
        assert python_packet == rust_unit_expected

    def test_auth_header_framing_agrees(self):
        auth_header = struct.pack(">HHHHHHHHH", 0x0028, 0x2FFF, 0x0004, 0x0008, 1, 0x0000, 1, 0x0014, 0x0014)
        python_prefix = LOCK_DDP_HEADER + auth_header + struct.pack(">H", 0x0000)
        rust_unit_expected = bytes(
            [
                0x00,
                0x08,
                0x00,
                0x01,
                0x10,
                0x00,
                0x02,
                0x00,
                0x00,
                0x28,
                0x2F,
                0xFF,
                0x00,
                0x04,
                0x00,
                0x08,
                0x00,
                0x01,
                0x00,
                0x00,
                0x00,
                0x01,
                0x00,
                0x14,
                0x00,
                0x14,
                0x00,
                0x00,
            ]
        )
        assert python_prefix == rust_unit_expected
