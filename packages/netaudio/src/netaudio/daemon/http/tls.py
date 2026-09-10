from __future__ import annotations

import hashlib
import ssl
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

DEFAULT_TLS_PORT = 9443
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


def tls_settings_from_config(daemon_config: Mapping, base_directory: Path | None = None) -> TLSSettings | None:
    certificate = daemon_config.get(TLS_CERTIFICATE_KEY)
    key = daemon_config.get(TLS_KEY_KEY)
    port = daemon_config.get(TLS_PORT_KEY)
    if certificate is None and key is None:
        if port is not None:
            raise TLSConfigurationError(f"[daemon] {TLS_PORT_KEY} requires {TLS_CERTIFICATE_KEY} and {TLS_KEY_KEY}")
        return None
    if certificate is None or key is None:
        raise TLSConfigurationError(f"[daemon] {TLS_CERTIFICATE_KEY} and {TLS_KEY_KEY} must be configured together")
    if port is None:
        resolved_port = DEFAULT_TLS_PORT
    elif isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
        raise TLSConfigurationError(f"[daemon] {TLS_PORT_KEY} must be an integer from 1 through 65535")
    else:
        resolved_port = port
    return TLSSettings(
        certificate=_resolve_path(certificate, base_directory, TLS_CERTIFICATE_KEY),
        key=_resolve_path(key, base_directory, TLS_KEY_KEY),
        port=resolved_port,
    )


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
        der = ssl.PEM_cert_to_DER_cert(pem)
    except (OSError, UnicodeError, ValueError) as exception:
        raise TLSConfigurationError(f"TLS certificate could not be read: {exception}") from exception
    digest = hashlib.sha256(der).hexdigest().upper()
    return ":".join(digest[index : index + 2] for index in range(0, len(digest), 2))


def daemon_tls_settings() -> TLSSettings | None:
    from netaudio.common.config_loader import default_config_path, load_daemon_config

    return tls_settings_from_config(load_daemon_config(), base_directory=default_config_path().parent)
