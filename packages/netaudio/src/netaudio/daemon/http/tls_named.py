from __future__ import annotations

import ssl
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from cryptography import x509




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






def named_certificate_status(certificate: Path | None, key: Path | None) -> dict | None:
    if certificate is None or key is None:
        return None
    status = {"certificate": str(certificate), "key": str(key)}
    try:
        loaded = load_named_certificate(certificate, key)
    except (OSError, ValueError, ssl.SSLError) as error:
        return {**status, "error": str(error)}
    return {**status, "names": sorted(loaded.names), "valid_until": loaded.not_after.isoformat()}
