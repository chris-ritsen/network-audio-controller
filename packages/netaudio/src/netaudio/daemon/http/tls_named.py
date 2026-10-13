from __future__ import annotations

import logging
import ssl
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from cryptography import x509

from netaudio.common.clock_text import clock_text

logger = logging.getLogger("netaudio")


@dataclass(frozen=True)
class LoadedNamedCertificate:
    context: ssl.SSLContext
    names: frozenset[str]
    not_after: datetime


def certificate_names(certificate: x509.Certificate) -> frozenset[str]:
    try:
        alternative_names = certificate.extensions.get_extension_for_class(x509.SubjectAlternativeName).value
    except x509.ExtensionNotFound:
        return frozenset()
    return frozenset(name.rstrip(".").casefold() for name in alternative_names.get_values_for_type(x509.DNSName))


def names_cover(names: frozenset[str], server_name: str) -> bool:
    name = server_name.rstrip(".").casefold()
    if name in names:
        return True
    label, _, parent = name.partition(".")
    return bool(label) and "." in parent and f"*.{parent}" in names


def load_named_certificate(certificate: Path, key: Path) -> LoadedNamedCertificate:
    leaf = x509.load_pem_x509_certificate(certificate.read_bytes())
    names = certificate_names(leaf)
    if not names:
        raise ValueError(f"{certificate} names no DNS names")
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.load_cert_chain(certfile=str(certificate), keyfile=str(key))
    return LoadedNamedCertificate(context=context, names=names, not_after=leaf.not_valid_after_utc)


class NamedCertificate:
    def __init__(self, certificate: Path, key: Path) -> None:
        self.certificate = certificate
        self.key = key
        self._loaded: LoadedNamedCertificate | None = None
        self._loaded_signature: tuple | None = None
        self._failed_signature: tuple | None = None

    def _signature(self) -> tuple:
        signature = []
        for path in (self.certificate, self.key):
            status = path.stat()
            signature.append((status.st_dev, status.st_ino, status.st_size, status.st_mtime_ns))
        return tuple(signature)

    def refresh(self) -> LoadedNamedCertificate | None:
        try:
            signature = self._signature()
        except OSError as error:
            self._fail(("missing", str(error)), error)
            return self._loaded
        if signature in (self._loaded_signature, self._failed_signature):
            return self._loaded
        try:
            loaded = load_named_certificate(self.certificate, self.key)
        except (OSError, ValueError, ssl.SSLError) as error:
            self._fail(signature, error)
            return self._loaded
        self._loaded = loaded
        self._loaded_signature = signature
        self._failed_signature = None
        logger.info(
            f"Serving the TLS certificate for {', '.join(sorted(loaded.names))} to clients that ask for those names, "
            f"valid until {clock_text(loaded.not_after)}"
        )
        return loaded

    def _fail(self, signature: tuple, error: Exception) -> None:
        if signature == self._failed_signature:
            return
        self._failed_signature = signature
        kept = " Still serving the one loaded before." if self._loaded is not None else ""
        logger.warning(f"TLS certificate {self.certificate} or key {self.key} could not be loaded: {error}.{kept}")

    def select(self, connection: ssl.SSLObject, server_name: str | None, context: ssl.SSLContext) -> None:
        if not server_name:
            return None
        loaded = self.refresh()
        if loaded is not None and names_cover(loaded.names, server_name):
            connection.context = loaded.context
        return None


def serve_named_certificate(
    context: ssl.SSLContext, certificate: Path | None, key: Path | None
) -> NamedCertificate | None:
    if certificate is None or key is None:
        return None
    named = NamedCertificate(certificate, key)
    named.refresh()
    context.sni_callback = named.select
    return named


def named_certificate_status(certificate: Path | None, key: Path | None) -> dict | None:
    if certificate is None or key is None:
        return None
    status = {"certificate": str(certificate), "key": str(key)}
    try:
        loaded = load_named_certificate(certificate, key)
    except (OSError, ValueError, ssl.SSLError) as error:
        return {**status, "error": str(error)}
    return {**status, "names": sorted(loaded.names), "valid_until": loaded.not_after.isoformat()}
