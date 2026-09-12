from __future__ import annotations

import hashlib
import json
import socket
from pathlib import Path

import pytest

from netaudio.dante.sap import (
    SAP_CONTENT_TYPE,
    SAP_MULTICAST_ADDRESS,
    SAP_PORT,
    SapFlowInventory,
    SapInventoryChangeKind,
    SapParseError,
    parse_sap_packet,
)
from netaudio.dante.sdp import SdpParseError, parse_sdp
from netaudio.dante.services.sap import SapDiscoveryService, SapInterface

FIXTURE_DIRECTORY = Path(__file__).parent / "fixtures" / "sap_sdp"

ROUTABLE_SDP = """v=0\r
o=studio 123456789012 7 IN IP4 192.0.2.44\r
s=Studio Feed\r
i=Session information\r
c=IN IP4 239.69.1.10/32\r
c=IN IP4 239.69.1.11/32\r
t=0 0\r
a=sendonly\r
a=ts-refclk:ptp=IEEE1588-2008:00-1D-C1-FF-FE-12-34-56:37\r
a=mediaclk:direct=17\r
a=x-dante:source\r
m=audio 5004 RTP/AVP 96 97\r
i=Main Mix\r
a=rtpmap:96 L24/48000/2\r
a=rtpmap:97 opus/48000/2\r
a=ptime:0.25\r
"""


def sap_packet(
    sdp: str = ROUTABLE_SDP,
    *,
    message_hash: int = 0x1234,
    origin: str = "192.0.2.44",
    authentication: bytes = b"",
    delete: bool = False,
    flags: int = 0,
) -> bytes:
    assert len(authentication) % 4 == 0
    header_flags = 0x20 | (0x04 if delete else 0) | flags
    return (
        bytes([header_flags, len(authentication) // 4])
        + message_hash.to_bytes(2, "big")
        + socket.inet_aton(origin)
        + authentication
        + SAP_CONTENT_TYPE
        + sdp.encode("utf-8")
    )


def test_synthetic_sap_fixture_matches_recorded_digest():
    provenance = json.loads((FIXTURE_DIRECTORY / "provenance.json").read_text())
    fixture_name = "synthetic-aes67-announcement.bin"
    payload = (FIXTURE_DIRECTORY / fixture_name).read_bytes()

    assert hashlib.sha256(payload).hexdigest() == provenance["fixtures"][fixture_name]["sha256"]


def test_sdp_parses_structural_and_routable_audio_fields():
    parsed = parse_sdp(ROUTABLE_SDP)

    assert parsed.version == 0
    assert parsed.origin_username == "studio"
    assert parsed.session_id == 123456789012
    assert parsed.session_version == 7
    assert parsed.session_name == "Studio Feed"
    assert parsed.session_information == "Session information"
    assert parsed.routable is True
    audio = parsed.routable_audio
    assert audio is not None
    assert audio.media_title == "Main Mix"
    assert audio.primary_destination_address == "239.69.1.10"
    assert audio.secondary_destination_address == "239.69.1.11"
    assert audio.destination_port == 5004
    assert audio.encoding == "L24"
    assert audio.sample_rate == 48000
    assert audio.channel_count == 2
    assert audio.packet_time_microseconds == 250
    assert audio.direction == "sendonly"
    assert audio.clock_offset == 17
    assert audio.ptp_domain_token == "37"
    assert audio.dante_origin is True


def test_sdp_keeps_structural_validity_separate_from_routability():
    parsed = parse_sdp("v=0\no=user 123 1 IN IP4 192.0.2.1\ns=Metadata only\nt=0 0\n")

    assert parsed.routable is False
    assert parsed.routable_audio is None
    assert parsed.routability_errors == ("SDP has no audio media description",)


@pytest.mark.parametrize(
    "sdp",
    [
        "o=user 1 1 IN IP4 192.0.2.1\ns=x\nt=0 0\n",
        "v=0\ns=x\nt=0 0\n",
        "v=0\no=user 1 1 IN IP4 192.0.2.1\nt=0 0\n",
        "v=0\no=user 1 1 IN IP4 192.0.2.1\ns=x\n",
    ],
)
def test_sdp_rejects_each_missing_structural_line(sdp):
    with pytest.raises(SdpParseError, match="missing required lines"):
        parse_sdp(sdp)


@pytest.mark.parametrize("ptime", ["0", "-1", "0.0001", "NaN", "word"])
def test_sdp_rejects_packet_times_that_do_not_resolve_to_positive_microseconds(ptime):
    with pytest.raises(SdpParseError, match="ptime"):
        parse_sdp(ROUTABLE_SDP.replace("a=ptime:0.25", f"a=ptime:{ptime}"))


def test_sap_parser_respects_authentication_length_and_preserves_bytes():
    parsed = parse_sap_packet(sap_packet(authentication=bytes.fromhex("0102030405060708")))

    assert parsed.version == 1
    assert parsed.authentication_length_words == 2
    assert parsed.authentication_data_hexadecimal == "0102030405060708"
    assert parsed.message_hash == 0x1234
    assert parsed.origin_address == "192.0.2.44"
    assert parsed.sdp.routable is True


@pytest.mark.parametrize(
    ("packet", "message"),
    [
        (sap_packet(flags=0x02), "encrypted"),
        (sap_packet(flags=0x01), "compressed"),
        (bytes([0x40]) + sap_packet()[1:], "version 1"),
        (sap_packet(message_hash=0), "hash"),
        (sap_packet(origin="0.0.0.0"), "origin"),
        (sap_packet().replace(SAP_CONTENT_TYPE, b"text/plain\x00", 1), "application/sdp"),
        (sap_packet().replace(SAP_CONTENT_TYPE, b"application/sdp ", 1), "application/sdp"),
    ],
)
def test_sap_parser_rejects_unsupported_or_invalid_headers(packet, message):
    with pytest.raises(SapParseError, match=message):
        parse_sap_packet(packet)


def test_sap_parser_rejects_authentication_length_beyond_packet():
    packet = bytearray(sap_packet())
    packet[1] = 0xFF

    with pytest.raises(SapParseError, match="authentication length"):
        parse_sap_packet(bytes(packet))


def test_inventory_add_refresh_replace_delete_and_expiry():
    inventory = SapFlowInventory(expiry_seconds=3600)
    packet = sap_packet()
    added = inventory.ingest(
        packet,
        announcement_interface="eth0",
        packet_source_ipv4="192.0.2.44",
        received_monotonic=10,
        wall_time=1000,
    )
    assert added is not None and added.kind is SapInventoryChangeKind.ADDED
    identity = ("192.0.2.44", 123456789012)
    original = inventory.get(*identity)
    assert original is not None

    refreshed = inventory.ingest(
        packet,
        announcement_interface="eth1",
        packet_source_ipv4="192.0.2.45",
        received_monotonic=20,
        wall_time=1010,
    )
    assert refreshed is not None and refreshed.kind is SapInventoryChangeKind.REFRESHED
    assert refreshed.flow.discovered_at == original.discovered_at
    assert refreshed.flow.refreshed_monotonic == 20
    assert refreshed.flow.announcement_interface == "eth1"

    changed_packet = sap_packet(ROUTABLE_SDP.replace("Studio Feed", "Replacement Feed"), message_hash=0x5678)
    replaced = inventory.ingest(
        changed_packet,
        announcement_interface="eth1",
        packet_source_ipv4="192.0.2.44",
        received_monotonic=30,
        wall_time=1020,
    )
    assert replaced is not None and replaced.kind is SapInventoryChangeKind.REPLACED
    assert replaced.flow.flow_name == "Replacement Feed"
    assert replaced.flow.discovered_at == original.discovered_at

    unmatched_delete = inventory.ingest(
        sap_packet(delete=True, message_hash=0x9999),
        announcement_interface="eth0",
        packet_source_ipv4="192.0.2.44",
        received_monotonic=31,
        wall_time=1021,
    )
    assert unmatched_delete is None
    assert inventory.get(*identity) is not None

    deleted = inventory.ingest(
        sap_packet(ROUTABLE_SDP.replace("Studio Feed", "Replacement Feed"), delete=True, message_hash=0x5678),
        announcement_interface="eth0",
        packet_source_ipv4="192.0.2.44",
        received_monotonic=32,
        wall_time=1022,
    )
    assert deleted is not None and deleted.kind is SapInventoryChangeKind.DELETED
    assert inventory.get(*identity) is None

    inventory.ingest(
        packet,
        announcement_interface="eth0",
        packet_source_ipv4="192.0.2.44",
        received_monotonic=100,
        wall_time=2000,
    )
    assert inventory.expire(3699.999) == []
    [expired] = inventory.expire(3700)
    assert expired.kind is SapInventoryChangeKind.EXPIRED
    assert inventory.to_dict() == {}


class FakeSocket:
    def __init__(self, *arguments) -> None:
        self.arguments = arguments
        self.options = []
        self.bound = None
        self.blocking = True
        self.closed = False

    def setsockopt(self, *arguments) -> None:
        self.options.append(arguments)

    def bind(self, address) -> None:
        self.bound = address

    def setblocking(self, value) -> None:
        self.blocking = value

    def close(self) -> None:
        self.closed = True


class FakeTransport:
    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True


@pytest.mark.asyncio
async def test_service_joins_sap_group_separately_on_every_active_ipv4_interface():
    interfaces = [SapInterface("eth0", "192.0.2.10"), SapInterface("eth1", "198.51.100.20")]
    sockets = []
    endpoints = []
    changes = []

    def socket_factory(*arguments):
        value = FakeSocket(*arguments)
        sockets.append(value)
        return value

    async def endpoint_factory(protocol_factory, *, sock):
        transport = FakeTransport()
        protocol = protocol_factory()
        endpoints.append((transport, protocol, sock))
        return transport, protocol

    service = SapDiscoveryService(
        interface_provider=lambda: interfaces,
        socket_factory=socket_factory,
        endpoint_factory=endpoint_factory,
        on_change=changes.append,
        refresh_seconds=60,
    )
    await service.start()
    try:
        assert service.listening_interfaces == tuple(interfaces)
        assert len(sockets) == 2
        for interface, multicast_socket in zip(interfaces, sockets):
            assert multicast_socket.bound == ("", SAP_PORT)
            assert multicast_socket.blocking is False
            membership = socket.inet_aton(SAP_MULTICAST_ADDRESS) + socket.inet_aton(interface.address)
            assert (socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, membership) in multicast_socket.options

        endpoints[1][1].datagram_received(sap_packet(), ("192.0.2.44", SAP_PORT))
        assert changes[0].kind is SapInventoryChangeKind.ADDED
        assert changes[0].flow.announcement_interface == "eth1"

        removed_transport = endpoints[0][0]
        interfaces.pop(0)
        await service.refresh_interfaces()
        assert removed_transport.closed is True
        assert service.listening_interfaces == (SapInterface("eth1", "198.51.100.20"),)
    finally:
        await service.stop()
    assert all(transport.closed for transport, _, _ in endpoints)
