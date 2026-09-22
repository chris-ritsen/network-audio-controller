import ctypes

import nacl.bindings
import pytest

from netaudio.core.binding import lock_token as binding_lock_token
from netaudio import core

NETAUDIO_INVALID_KEY = 17
NETAUDIO_INVALID_LENGTH = 35


@pytest.fixture(scope="module")
def lib():
    return core.require()


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
    @pytest.mark.parametrize("pin,nonce,key", zip(PINS, NONCES, KEYS))
    def test_token_matches_pynacl_through_python_binding(self, key, nonce, pin):
        assert binding_lock_token(pin, nonce, key) == python_token(pin, nonce, key)

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
