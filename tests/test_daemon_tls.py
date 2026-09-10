from __future__ import annotations

import shutil
import ssl
import subprocess
from pathlib import Path

import pytest

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
    def test_absent_keys_disable_tls(self):
        assert tls_settings_from_config({}) is None

    def test_port_alone_is_rejected(self):
        with pytest.raises(TLSConfigurationError):
            tls_settings_from_config({"tls_port": 9443})

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

    def test_bonjour_omits_tls_port_without_tls(self):
        info = make_http_server()._build_service_info(["192.168.1.2"])
        assert b"tls_port" not in info.properties
