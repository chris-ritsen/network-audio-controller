# Pin the managed controller certificate

Status: proposed October 29, 2026; awaiting user acceptance.

Native managed control opens TLS to the DDM controller service on port 8443
and its advertised service port. The daemon accepted only certificates that
chain to a system or configured root and match the configured host. A DDM 1.9
appliance presents a leaf for `ddm.audinate.com` issued by `ca.audinate.com`,
without the issuer, valid from 2022 to 2072, with no subject alternative names.
Servers are normally configured by IP address. Both checks fail, so every
managed controller operation failed with a transport error. The earlier
Python server disabled verification for this channel.

A DDM server entry may set `controller_certificate_sha256` to the SHA-256
fingerprint of the controller's leaf certificate, in hexadecimal with or
without colons. When set, a peer presenting exactly that leaf is accepted for
that server's managed transports, regardless of host name. Handshake
signatures are still verified, against the public key of the pinned leaf. The
leaf is an X.509 version 1 certificate, which the certificate path validator
rejects, so a pinned leaf is verified as a raw public key. Any other
certificate must pass ordinary chain and host name verification. Without the
setting, behavior is unchanged. Changing a server's address removes the pin
with its credential and trust file. The Python launcher accepts the same
server settings, so it does not reject a configuration the daemon reads.

The appliance offers TLS 1.2 only. A probe from www on October 29, 2026,
accepted the pinned leaf over TLS 1.2, returned `HTTP/1.1 200 OK` for
`GET /dapi`, and rejected an all-zero pin.

This matches the iOS app, which accepts a system-trusted chain or a leaf the
user pinned for the profile. It fails closed on certificate replacement until
the operator reviews and pins the new leaf.

The certificate predates the appliance installation, so it is very likely
shared by every DDM installation. A pin proves possession of that controller
key, not the identity of one appliance. It still rejects unrelated
certificates, which disabled verification did not.

Alternatives considered: a configured root plus a TLS server-name override
(the issuer is not sent and the name is shared across installations, so it
adds no identity); a host-name mapping to `ddm.audinate.com` (machine-wide
side effects); disabled verification (accepts any peer).
