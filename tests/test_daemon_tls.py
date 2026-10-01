from __future__ import annotations

import asyncio
import hashlib
import ipaddress
import json
import os
import shutil
import socket
import ssl
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock

import ifaddr
import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID

import netaudio.daemon.http.api as api_module
from netaudio.common.app_config import DEFAULT_DAEMON_PORT
import netaudio.daemon.http.tls as tls_module
from netaudio.daemon.http.tls import (
    DEFAULT_TLS_PORT,
    TLSConfigurationError,
    TLSSettings,
    build_ssl_context,
    certificate_fingerprint,
    tls_settings_from_config,
)
from tests.http_api_test_support import make_http_server

HOST_NAMES = ("localhost", "studio", "studio.local")
HOST_ADDRESSES = ("127.0.0.1", "192.0.2.10")


@pytest.fixture
def host(monkeypatch):
    state = {"addresses": ["192.0.2.10", ("fe80::1", 0, 4)], "hostname": "studio"}

    def adapters():
        return [
            ifaddr.Adapter("en0", "en0", [ifaddr.IP(address, 24, "en0") for address in state["addresses"]]),
        ]

    monkeypatch.setattr(ifaddr, "get_adapters", adapters)
    monkeypatch.setattr(socket, "gethostname", lambda: state["hostname"])
    return state


def write_identity(path, *, days_left=200, names=HOST_NAMES, addresses=HOST_ADDRESSES, mismatched_key=False):
    key = ec.generate_private_key(ec.SECP256R1())
    not_after = datetime.now(timezone.utc) + timedelta(days=days_left)
    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "NetAudio")])
    certificate = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(not_after - timedelta(days=365))
        .not_valid_after(not_after)
        .add_extension(
            x509.SubjectAlternativeName(
                [x509.DNSName(name) for name in names]
                + [x509.IPAddress(ipaddress.ip_address(address)) for address in addresses]
            ),
            critical=False,
        )
        .sign(key, hashes.SHA256())
    )
    signing_key = ec.generate_private_key(ec.SECP256R1()) if mismatched_key else key
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    path.write_bytes(
        certificate.public_bytes(serialization.Encoding.PEM)
        + signing_key.private_bytes(
            serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
        )
    )
    path.chmod(0o600)


def identity_path(directory: Path) -> Path:
    return directory / "tls" / "daemon.pem"


def subject_names(path: Path) -> tuple[set[str], set[str]]:
    certificate = x509.load_pem_x509_certificate(path.read_bytes())
    names = certificate.extensions.get_extension_for_class(x509.SubjectAlternativeName).value
    return set(names.get_values_for_type(x509.DNSName)), {
        str(address) for address in names.get_values_for_type(x509.IPAddress)
    }


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


class TestGeneratedIdentity:
    def test_current_identity_is_reused(self, tmp_path, host):
        write_identity(identity_path(tmp_path))
        before = identity_path(tmp_path).read_bytes()
        settings = tls_settings_from_config({}, base_directory=tmp_path)
        assert settings.certificate.read_bytes() == before

    @pytest.mark.parametrize(
        "identity",
        [
            {"corrupt": True},
            {"days_left": -1},
            {"days_left": 10},
            {"mismatched_key": True},
            {"addresses": ("127.0.0.1",)},
            {"names": ("localhost", "old-name", "old-name.local")},
        ],
        ids=["corrupt", "expired", "expiring", "mismatched-key", "new-address", "renamed-host"],
    )
    def test_stale_identity_renews(self, tmp_path, host, identity):
        path = identity_path(tmp_path)
        if identity.pop("corrupt", False):
            path.parent.mkdir(parents=True)
            path.write_bytes(b"broken identity")
        else:
            write_identity(path, **identity)
        before = path.read_bytes()
        settings = tls_settings_from_config({}, base_directory=tmp_path)
        assert settings.certificate == path
        assert path.read_bytes() != before
        build_ssl_context(settings)
        names, addresses = subject_names(path)
        assert set(HOST_NAMES) <= names
        assert set(HOST_ADDRESSES) <= addresses
        certificate = x509.load_pem_x509_certificate(path.read_bytes())
        assert certificate.not_valid_after_utc - datetime.now(timezone.utc) > timedelta(days=300)
        if os.name != "nt":
            assert path.stat().st_mode & 0o777 == 0o600
        assert [entry.name for entry in path.parent.iterdir()] == ["daemon.pem"]

    def test_ipv6_changes_keep_identity(self, tmp_path, host):
        write_identity(identity_path(tmp_path))
        before = identity_path(tmp_path).read_bytes()
        host["addresses"].append(("2001:db8::20", 0, 4))
        tls_settings_from_config({}, base_directory=tmp_path)
        assert identity_path(tmp_path).read_bytes() == before

    def test_configured_certificate_is_never_renewed(self, tmp_path, host):
        path = tmp_path / "custom.pem"
        write_identity(path, days_left=-1)
        before = path.read_bytes()
        settings = tls_settings_from_config(
            {"tls_certificate": str(path), "tls_key": str(path)}, base_directory=tmp_path
        )
        assert settings.certificate == path.resolve()
        assert path.read_bytes() == before
        assert not identity_path(tmp_path).exists()


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
            make_http_server(tls=TLSSettings(certificate=certificate, key=key, port=DEFAULT_DAEMON_PORT))

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
            assert listeners == [("0.0.0.0", DEFAULT_DAEMON_PORT, False)]
            listener, context = server.tcp_server, None
        else:
            assert listeners == [("127.0.0.1", DEFAULT_DAEMON_PORT, False), ("0.0.0.0", DEFAULT_TLS_PORT, True)]
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


async def start_tls_server(settings, monkeypatch):
    monkeypatch.setattr(api_module, "BONJOUR_MONITOR_INTERVAL_SECONDS", 0.01)
    server = make_http_server(tls=settings)
    server._reconcile_bonjour = AsyncMock()
    start_server = asyncio.start_server

    async def bind_ephemeral(callback, host, port, **kwargs):
        return await start_server(callback, "127.0.0.1", 0, **kwargs)

    monkeypatch.setattr(asyncio, "start_server", bind_ephemeral)
    await server.start()
    return server, server.tls_server.sockets[0].getsockname()[1]


async def served_certificate(port, context=None, server_hostname=None):
    if context is None:
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
    reader, writer = await asyncio.open_connection("127.0.0.1", port, ssl=context, server_hostname=server_hostname)
    der = writer.get_extra_info("ssl_object").getpeercert(binary_form=True)
    writer.close()
    await writer.wait_closed()
    digest = hashlib.sha256(der).hexdigest().upper()
    return ":".join(digest[index : index + 2] for index in range(0, len(digest), 2))


async def next_served_certificate(port, previous):
    for _ in range(300):
        current = await served_certificate(port)
        if current != previous:
            return current
        await asyncio.sleep(0.01)
    return previous


@pytest.mark.asyncio
@pytest.mark.parametrize("trigger", ["address", "expiry", "hostname"])
async def test_running_daemon_renews_identity(tmp_path, monkeypatch, host, trigger):
    settings = tls_settings_from_config({}, base_directory=tmp_path)
    server, port = await start_tls_server(settings, monkeypatch)
    try:
        before = await served_certificate(port)
        assert before == certificate_fingerprint(settings.certificate)
        if trigger == "address":
            host["addresses"].append("198.51.100.20")
        elif trigger == "hostname":
            host["hostname"] = "stage"
        else:
            later = datetime.now(timezone.utc) + timedelta(days=340)
            monkeypatch.setattr(tls_module, "_now", lambda: later)

        after = await next_served_certificate(port, before)
        assert after != before
        assert after == certificate_fingerprint(settings.certificate)
        await asyncio.sleep(0.1)
        assert await served_certificate(port) == after
        names, addresses = subject_names(settings.certificate)
        if trigger == "address":
            assert "198.51.100.20" in addresses
            trusted = ssl.create_default_context(cafile=str(settings.certificate))
            assert await served_certificate(port, trusted, "studio.local") == after
        elif trigger == "hostname":
            assert {"stage", "stage.local"} <= names
        (tmp_path / f"renewal-{trigger}.json").write_text(json.dumps({"after": after, "before": before}))
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_running_daemon_adopts_external_renewal(tmp_path, monkeypatch, host):
    settings = tls_settings_from_config({}, base_directory=tmp_path)
    server, port = await start_tls_server(settings, monkeypatch)
    try:
        before = await served_certificate(port)
        write_identity(settings.certificate, addresses=(*HOST_ADDRESSES, "198.51.100.20"))
        external = certificate_fingerprint(settings.certificate)
        host["addresses"].append("198.51.100.20")
        assert await next_served_certificate(port, before) == external
        assert certificate_fingerprint(settings.certificate) == external
    finally:
        await server.stop()


@pytest.mark.asyncio
@pytest.mark.skipif(os.name == "nt" or os.geteuid() == 0, reason="needs POSIX permissions without root")
async def test_failed_renewal_keeps_serving(tmp_path, monkeypatch, host, caplog):
    settings = tls_settings_from_config({}, base_directory=tmp_path)
    server, port = await start_tls_server(settings, monkeypatch)
    directory = settings.certificate.parent
    try:
        before = await served_certificate(port)
        directory.chmod(0o500)
        host["addresses"].append("198.51.100.20")
        await asyncio.sleep(0.2)
        assert await served_certificate(port) == before
        warnings = [record for record in caplog.records if "TLS certificate renewal failed" in record.getMessage()]
        assert len(warnings) == 1
    finally:
        directory.chmod(0o700)
        await server.stop()
