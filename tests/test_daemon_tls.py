from __future__ import annotations

import shutil
import ssl
import subprocess
import asyncio
import json
import os
from datetime import timedelta
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from cryptography import x509

from netaudio.daemon.http.tls import (
    DEFAULT_TLS_PORT,
    TLSConfigurationError,
    TLSSettings,
    build_ssl_context,
    certificate_fingerprint,
    tls_settings_from_config,
)
from tests.http_api_test_support import make_http_server


def write_certificate(directory: Path) -> tuple[Path, Path]:
    if shutil.which("openssl") is None:
        pytest.skip("openssl is not available to generate a test certificate")
    certificate = directory / "daemon.crt"
    key = directory / "daemon.key"
    subprocess.run(
        [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            str(key),
            "-out",
            str(certificate),
            "-days",
            "2",
            "-subj",
            "/CN=netaudio-test",
        ],
        check=True,
        capture_output=True,
    )
    return certificate, key


class TestSettingsParsing:
    def test_default_certificate_is_persistent_and_private(self, tmp_path):
        settings = tls_settings_from_config({}, base_directory=tmp_path)
        assert settings is not None
        first = settings.certificate.read_bytes()
        assert tls_settings_from_config({}, base_directory=tmp_path) == settings
        assert settings.certificate.read_bytes() == first
        build_ssl_context(settings)
        if os.name != "nt":
            assert settings.key.stat().st_mode & 0o777 == 0o600
        (tmp_path / "tls-result.json").write_text(json.dumps({"persistent": True, "port": settings.port}))

    def test_explicit_plaintext_does_not_generate_a_key(self, tmp_path):
        assert tls_settings_from_config({"no_ssl": True}, base_directory=tmp_path) is None
        assert list(tmp_path.iterdir()) == []

    @pytest.mark.parametrize("config", [{"no_ssl": "false"}, {"no_ssl": True, "tls_port": 9443}])
    def test_invalid_or_conflicting_plaintext_settings_fail(self, tmp_path, config):
        with pytest.raises(TLSConfigurationError):
            tls_settings_from_config(config, base_directory=tmp_path)

    def test_generated_certificate_supports_custom_port(self, tmp_path):
        assert tls_settings_from_config({"tls_port": 10443}, base_directory=tmp_path).port == 10443

    def test_corrupt_identity_is_not_silently_replaced(self, tmp_path):
        settings = tls_settings_from_config({}, base_directory=tmp_path)
        settings.certificate.write_bytes(b"broken identity")
        with pytest.raises(TLSConfigurationError):
            tls_settings_from_config({}, base_directory=tmp_path)
        assert settings.certificate.read_bytes() == b"broken identity"

    def test_certificate_without_key_is_rejected(self, tmp_path):
        with pytest.raises(TLSConfigurationError):
            tls_settings_from_config({"tls_certificate": "daemon.crt"}, base_directory=tmp_path)

    def test_missing_files_are_rejected(self, tmp_path):
        with pytest.raises(TLSConfigurationError):
            tls_settings_from_config({"tls_certificate": "a.crt", "tls_key": "a.key"}, base_directory=tmp_path)

    def test_relative_paths_resolve_against_the_config_directory(self, tmp_path):
        certificate, key = write_certificate(tmp_path)
        settings = tls_settings_from_config(
            {"tls_certificate": certificate.name, "tls_key": key.name}, base_directory=tmp_path
        )
        assert settings == TLSSettings(certificate=certificate.resolve(), key=key.resolve(), port=DEFAULT_TLS_PORT)

    def test_invalid_port_is_rejected(self, tmp_path):
        certificate, key = write_certificate(tmp_path)
        with pytest.raises(TLSConfigurationError):
            tls_settings_from_config(
                {"tls_certificate": str(certificate), "tls_key": str(key), "tls_port": 70000}, base_directory=tmp_path
            )


class TestContext:
    def test_context_loads_the_chain_and_requires_tls_1_2(self, tmp_path):
        certificate, key = write_certificate(tmp_path)
        context = build_ssl_context(TLSSettings(certificate=certificate, key=key, port=9443))
        assert context.minimum_version == ssl.TLSVersion.TLSv1_2

    def test_mismatched_key_is_rejected(self, tmp_path):
        certificate, _ = write_certificate(tmp_path)
        other = tmp_path / "other"
        other.mkdir()
        _, other_key = write_certificate(other)
        with pytest.raises(TLSConfigurationError):
            build_ssl_context(TLSSettings(certificate=certificate, key=other_key, port=9443))

    def test_fingerprint_is_colon_separated_sha256(self, tmp_path):
        certificate, _ = write_certificate(tmp_path)
        fingerprint = certificate_fingerprint(certificate)
        assert len(fingerprint.split(":")) == 32
        assert fingerprint == fingerprint.upper()


class TestServerWiring:
    def test_tls_port_must_differ_from_the_http_port(self, tmp_path):
        certificate, key = write_certificate(tmp_path)
        with pytest.raises(TLSConfigurationError):
            make_http_server(tls=TLSSettings(certificate=certificate, key=key, port=9000))

    def test_bonjour_advertises_the_tls_port(self, tmp_path):
        certificate, key = write_certificate(tmp_path)
        server = make_http_server(tls=TLSSettings(certificate=certificate, key=key, port=9443))
        info = server._build_service_info(["192.168.1.2"])
        assert info.properties[b"tls_port"] == b"9443"
        assert info.port == 9443
        assert info.properties[b"scheme"] == b"https"

    def test_bonjour_omits_tls_port_without_tls(self):
        info = make_http_server()._build_service_info(["192.168.1.2"])
        assert b"tls_port" not in info.properties


@pytest.mark.asyncio
@pytest.mark.parametrize("no_ssl", [False, True])
async def test_listener_mode_and_real_https_request(tmp_path, monkeypatch, no_ssl):
    settings = tls_settings_from_config({"no_ssl": no_ssl}, base_directory=tmp_path)
    if settings is not None:
        certificate = x509.load_pem_x509_certificate(settings.certificate.read_bytes())
        lifetime = certificate.not_valid_after_utc - certificate.not_valid_before_utc
        assert timedelta(days=364) < lifetime <= timedelta(days=398)

    server = make_http_server(tls=settings)
    server._reconcile_bonjour = AsyncMock()
    server._bonjour_monitor_loop = AsyncMock()
    start_server = asyncio.start_server
    listeners = []

    async def bind_ephemeral(callback, host, port, **kwargs):
        listeners.append((host, port, "ssl" in kwargs))
        return await start_server(callback, "127.0.0.1", 0, **kwargs)

    monkeypatch.setattr(asyncio, "start_server", bind_ephemeral)
    await server.start()
    try:
        if no_ssl:
            assert listeners == [("0.0.0.0", 9000, False)]
            listener, context = server.tcp_server, None
        else:
            assert listeners == [("127.0.0.1", 9000, False), ("0.0.0.0", 9443, True)]
            listener = server.tls_server
            context = ssl.create_default_context(cafile=str(settings.certificate))

        port = listener.sockets[0].getsockname()[1]
        reader, writer = await asyncio.open_connection("127.0.0.1", port, ssl=context)
        writer.write(b"GET /server-info HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        await writer.drain()
        response = await asyncio.wait_for(reader.read(), 3)
        assert b"200 OK" in response
        payload = json.loads(response.split(b"\r\n\r\n", 1)[1])
        (tmp_path / "https-server-info.json").write_text(json.dumps(payload))
        writer.close()
        await writer.wait_closed()

        reader, writer = await asyncio.open_connection("127.0.0.1", port, ssl=context)
        writer.write(b"GET /.well-known/oauth-authorization-server HTTP/1.1\r\nHost: localhost\r\n\r\n")
        await writer.drain()
        response = await asyncio.wait_for(reader.read(), 3)
        metadata = json.loads(response.split(b"\r\n\r\n", 1)[1])
        assert metadata["issuer"] == ("http://localhost" if no_ssl else "https://localhost")
        writer.close()
        await writer.wait_closed()
    finally:
        await server.stop()
