from __future__ import annotations

import ipaddress

from zeroconf import DNSOutgoing, DNSQuestion

from netaudio.dante.const import SERVICES


def discovery_destination(address: str | None) -> str | None:
    if address is None:
        return None
    if not isinstance(address, str):
        raise ValueError("discovery address must be an IPv4 address")
    try:
        destination = ipaddress.IPv4Address(address)
    except ipaddress.AddressValueError as exception:
        raise ValueError("discovery address must be an IPv4 address") from exception
    if destination.is_unspecified or destination.is_multicast or int(destination) == 0xFFFFFFFF:
        raise ValueError("directed discovery requires a unicast IPv4 address")
    return str(destination)


def request_discovery(zeroconf, address: str | None = None) -> dict:
    """Request DNS-SD PTR records once; existing discovery listeners handle replies."""
    destination = discovery_destination(address)
    if zeroconf is None or not zeroconf.started:
        raise RuntimeError("mDNS discovery is not running")
    query = DNSOutgoing(0, multicast=destination is None)
    # RFC 6762 sections 5.4 and 5.5: direct unicast implies QU response behavior.
    question_class = 0x8001 if destination is None else 1
    for service_type in SERVICES:
        query.add_question(DNSQuestion(service_type, 12, question_class))
    zeroconf.async_send(query, addr=destination, port=5353)
    return {
        "query_requested": True,
        "destination": destination or "224.0.0.251",
        "service_types": list(SERVICES),
    }
