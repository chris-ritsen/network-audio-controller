from __future__ import annotations

import hashlib
import ipaddress
import os
import socket
import ssl
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import ifaddr
from cryptography import x509
from cryptography.exceptions import UnsupportedAlgorithm
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

DEFAULT_TLS_PORT = 4781
GENERATED_IDENTITY_LIFETIME = timedelta(days=365)
GENERATED_IDENTITY_RENEWAL_WINDOW = timedelta(days=30)
TLS_CERTIFICATE_KEY = "tls_certificate"
TLS_KEY_KEY = "tls_key"
TLS_PORT_KEY = "tls_port"


class TLSConfigurationError(ValueError):
    pass


@dataclass(frozen=True)
class TLSSettings:
    certificate: Path
    key: Path
    port: int
    generated: bool = False


@dataclass(frozen=True)
class IdentityCoverage:
    addresses: frozenset[ipaddress.IPv4Address | ipaddress.IPv6Address]
    names: frozenset[str]
    not_after: datetime
    not_before: datetime


def _resolve_path(value: object, base_directory: Path | None, name: str) -> Path:
    if not isinstance(value, str) or not value.strip():
        raise TLSConfigurationError(f"[daemon] {name} must be a file path")
    path = Path(value).expanduser()
    if not path.is_absolute() and base_directory is not None:
        path = base_directory / path
    path = path.resolve()
    if not path.is_file():
        raise TLSConfigurationError(f"[daemon] {name} does not exist: {path}")
    return path


TLS_ENVIRONMENT = {
    TLS_CERTIFICATE_KEY: "NETAUDIO_TLS_CERTIFICATE",
    TLS_KEY_KEY: "NETAUDIO_TLS_KEY",
    TLS_PORT_KEY: "NETAUDIO_TLS_PORT",
    "no_ssl": "NETAUDIO_NO_SSL",
}


def _environment_overrides() -> dict:
    overrides = {}
    for key, name in TLS_ENVIRONMENT.items():
        value = os.environ.get(name)
        if not value:
            continue
        if key == TLS_PORT_KEY:
            overrides[key] = int(value) if value.isdigit() else value
        elif key == "no_ssl":
            overrides[key] = value.lower() in ("1", "true", "yes")
        else:
            overrides[key] = value
    return overrides


def tls_settings_from_config(daemon_config: Mapping, base_directory: Path | None = None) -> TLSSettings | None:
    daemon_config = {**daemon_config, **_environment_overrides()}
    certificate = daemon_config.get(TLS_CERTIFICATE_KEY)
    key = daemon_config.get(TLS_KEY_KEY)
    port = daemon_config.get(TLS_PORT_KEY)
    no_ssl = daemon_config.get("no_ssl", False)
    if not isinstance(no_ssl, bool):
        raise TLSConfigurationError("[daemon] no_ssl must be a boolean")

    if no_ssl:
        if any(value is not None for value in (certificate, key, port)):
            raise TLSConfigurationError("[daemon] no_ssl cannot be combined with TLS settings")
        return None

    if (certificate is None) != (key is None):
        raise TLSConfigurationError(f"[daemon] {TLS_CERTIFICATE_KEY} and {TLS_KEY_KEY} must be configured together")
    if port is None:
        resolved_port = DEFAULT_TLS_PORT
    elif isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
        raise TLSConfigurationError(f"[daemon] {TLS_PORT_KEY} must be an integer from 1 through 65535")
    else:
        resolved_port = port

    if certificate is None:
        if base_directory is None:
            from netaudio.common.config_loader import default_config_path

            base_directory = default_config_path().parent

        identity = ensure_generated_identity(base_directory / "tls" / "daemon.pem")
        settings = TLSSettings(certificate=identity, key=identity, port=resolved_port, generated=True)
        build_ssl_context(settings)
        return settings

    return TLSSettings(
        certificate=_resolve_path(certificate, base_directory, TLS_CERTIFICATE_KEY),
        key=_resolve_path(key, base_directory, TLS_KEY_KEY),
        port=resolved_port,
    )


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _host_names() -> frozenset[str]:
    hostname = socket.gethostname().rstrip(".")
    names = set()
    for name in ("localhost", hostname, f"{hostname.removesuffix('.local')}.local"):
        try:
            names.add(name.encode("idna").decode("ascii"))
        except UnicodeError:
            continue
    return frozenset(names)


def _host_addresses() -> frozenset[ipaddress.IPv4Address | ipaddress.IPv6Address]:
    addresses = {ipaddress.ip_address("127.0.0.1"), ipaddress.ip_address("::1")}
    for adapter in ifaddr.get_adapters():
        for address in adapter.ips:
            value = address.ip[0] if isinstance(address.ip, tuple) else address.ip
            addresses.add(ipaddress.ip_address(value.split("%", 1)[0]))
    return frozenset(addresses)


def identity_coverage(identity: Path) -> IdentityCoverage | None:
    try:
        pem = identity.read_bytes()
        certificate = x509.load_pem_x509_certificate(pem)
        key = serialization.load_pem_private_key(pem, password=None)
        alternative_names = certificate.extensions.get_extension_for_class(x509.SubjectAlternativeName).value
    except (OSError, TypeError, ValueError, UnsupportedAlgorithm, x509.ExtensionNotFound):
        return None

    public_format = (serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo)
    if key.public_key().public_bytes(*public_format) != certificate.public_key().public_bytes(*public_format):
        return None

    return IdentityCoverage(
        addresses=frozenset(alternative_names.get_values_for_type(x509.IPAddress)),
        names=frozenset(alternative_names.get_values_for_type(x509.DNSName)),
        not_after=certificate.not_valid_after_utc,
        not_before=certificate.not_valid_before_utc,
    )


def identity_is_current(coverage: IdentityCoverage | None) -> bool:
    if coverage is None:
        return False
    now = _now()
    if not coverage.not_before <= now < coverage.not_after - GENERATED_IDENTITY_RENEWAL_WINDOW:
        return False
    required_addresses = {address for address in _host_addresses() if address.version == 4}
    return _host_names() <= coverage.names and required_addresses <= coverage.addresses


def ensure_generated_identity(identity: Path) -> Path:
    if identity_is_current(identity_coverage(identity)):
        return identity

    temporary = None
    try:
        identity.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        key = ec.generate_private_key(ec.SECP256R1())
        addresses = _host_addresses()
        subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "NetAudio")])
        now = _now()
        certificate = (
            x509.CertificateBuilder()
            .subject_name(subject)
            .issuer_name(subject)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now - timedelta(minutes=5))
            .not_valid_after(now + GENERATED_IDENTITY_LIFETIME)
            .add_extension(
                x509.SubjectAlternativeName(
                    [x509.DNSName(name) for name in sorted(_host_names())]
                    + [x509.IPAddress(address) for address in sorted(addresses, key=str)]
                ),
                critical=False,
            )
            .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
            .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]), critical=False)
            .sign(key, hashes.SHA256())
        )
        pem = certificate.public_bytes(serialization.Encoding.PEM) + key.private_bytes(
            serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
        )
        descriptor, temporary = tempfile.mkstemp(prefix=".identity-", dir=identity.parent)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(pem)
            stream.flush()
            os.fsync(stream.fileno())

        os.replace(temporary, identity)
        temporary = None
    except (OSError, ValueError) as error:
        raise TLSConfigurationError(f"Cannot create the daemon TLS identity at {identity}: {error}") from error
    finally:
        if temporary is not None:
            Path(temporary).unlink(missing_ok=True)

    return identity


def build_ssl_context(settings: TLSSettings) -> ssl.SSLContext:
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    try:
        context.load_cert_chain(certfile=str(settings.certificate), keyfile=str(settings.key))
    except (ssl.SSLError, OSError) as exception:
        raise TLSConfigurationError(f"TLS certificate or key could not be loaded: {exception}") from exception
    return context


def certificate_fingerprint(certificate: Path) -> str:
    try:
        pem = certificate.read_text(encoding="ascii")
        der = x509.load_pem_x509_certificate(pem.encode("ascii")).public_bytes(serialization.Encoding.DER)
    except (OSError, UnicodeError, ValueError) as exception:
        raise TLSConfigurationError(f"TLS certificate could not be read: {exception}") from exception
    digest = hashlib.sha256(der).hexdigest().upper()
    return ":".join(digest[index : index + 2] for index in range(0, len(digest), 2))


def daemon_tls_settings() -> TLSSettings | None:
    from netaudio.common.config_loader import default_config_path, load_daemon_config

    return tls_settings_from_config(load_daemon_config(), base_directory=default_config_path().parent)
