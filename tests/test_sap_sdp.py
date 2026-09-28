from __future__ import annotations

import asyncio
import hashlib
import json
import socket
from pathlib import Path

import pytest

from netaudio.dante.sap import (
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
SAP_CONTENT_TYPE = b"application/sdp\x00"

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


@pytest.mark.parametrize("fixture_name", ["pipewire-shaped-announcement.bin", "synthetic-aes67-announcement.bin"])
def test_sap_fixtures_decode_to_recorded_identity(fixture_name):
    provenance = json.loads((FIXTURE_DIRECTORY / "provenance.json").read_text())
    payload = (FIXTURE_DIRECTORY / fixture_name).read_bytes()

    assert hashlib.sha256(payload).hexdigest() == provenance["fixtures"][fixture_name]["sha256"]
    parsed = parse_sap_packet(payload)
    expected = provenance["fixtures"][fixture_name]

    assert parsed.origin_address == expected["source_ipv4"]
    assert parsed.message_hash == int(expected["message_hash"], 16)
    assert parsed.sdp["session_id"] == expected["session_id"]
    assert parsed.sdp["routable"]


def test_pipewire_announcement_is_routable_recvonly_audio():
    parsed = parse_sap_packet((FIXTURE_DIRECTORY / "pipewire-shaped-announcement.bin").read_bytes())
    audio = parsed.sdp["routable_audio"]

    assert parsed.sdp["routable"] is True
    assert parsed.sdp["routability_errors"] == []
    assert audio["direction"] == "recvonly"
    assert audio["media_title"] == "2 channels: AUX1, AUX2"
    assert audio["primary_destination_address"] == "239.69.150.243"
    assert audio["destination_port"] == 5004
    assert audio["encoding"] == "L24"
    assert audio["sample_rate"] == 48000
    assert audio["channel_count"] == 2
    assert audio["packet_time_microseconds"] == 1000
    assert audio["clock_offset"] == 0
    assert audio["dante_origin"] is False


DIRECTION_SDP = (
    "v=0\no=user 1 1 IN IP4 192.0.2.1\ns=Direction\nc=IN IP4 239.69.1.10/32\nt=0 0\n{session}"
    "m=audio 5004 RTP/AVP 96\na=rtpmap:96 L24/48000/2\n{media}"
)


@pytest.mark.parametrize(
    "session,media,direction,routable",
    [
        ("", "", None, True),
        ("", "a=inactive\n", "inactive", False),
        ("", "a=recvonly\n", "recvonly", True),
        ("", "a=sendrecv\n", "sendrecv", True),
        ("a=inactive\n", "", "inactive", False),
        ("a=inactive\n", "a=recvonly\n", "recvonly", True),
        ("a=recvonly\n", "", "recvonly", True),
        ("a=sendonly\n", "a=inactive\n", "inactive", False),
    ],
)
def test_sdp_direction_eligibility(session, media, direction, routable):
    parsed = parse_sdp(DIRECTION_SDP.format(session=session, media=media))

    assert parsed["routable_audio"]["direction"] == direction
    assert parsed["routable"] is routable
    assert ("audio media direction is inactive" in parsed["routability_errors"]) is not routable


def test_sdp_parses_structural_and_routable_audio_fields():
    parsed = parse_sdp(ROUTABLE_SDP)

    assert parsed["version"] == 0
    assert parsed["origin_username"] == "studio"
    assert parsed["session_id"] == 123456789012
    assert parsed["session_version"] == 7
    assert parsed["session_name"] == "Studio Feed"
    assert parsed["session_information"] == "Session information"
    assert parsed["routable"] is True
    audio = parsed["routable_audio"]
    assert audio is not None
    assert audio["media_title"] == "Main Mix"
    assert audio["primary_destination_address"] == "239.69.1.10"
    assert audio["secondary_destination_address"] == "239.69.1.11"
    assert audio["destination_port"] == 5004
    assert audio["encoding"] == "L24"
    assert audio["sample_rate"] == 48000
    assert audio["channel_count"] == 2
    assert audio["packet_time_microseconds"] == 250
    assert audio["direction"] == "sendonly"
    assert audio["clock_offset"] == 17
    assert audio["ptp_domain_token"] == "37"
    assert audio["dante_origin"] is True


def test_sdp_keeps_structural_validity_separate_from_routability():
    parsed = parse_sdp("v=0\no=user 123 1 IN IP4 192.0.2.1\ns=Metadata only\nt=0 0\n")

    assert parsed["routable"] is False
    assert parsed["routable_audio"] is None
    assert parsed["routability_errors"] == ["SDP has no audio media description"]


def test_sdp_media_scope_overrides_session_defaults_and_preserves_unknown_lines():
    sdp = (
        ROUTABLE_SDP
        + "c=IN IP4 239.70.2.3/64\r\na=recvonly\r\na=mediaclk:direct=31\r\na=ts-refclk:ptp=IEEE1588-2008:clock:42\r\nz=unrecognized\r\n"
    )
    parsed = parse_sdp(sdp)
    audio = parsed["routable_audio"]

    assert audio is not None
    assert audio["primary_destination_address"] == "239.70.2.3"
    assert audio["secondary_destination_address"] is None
    assert audio["direction"] == "recvonly"
    assert audio["clock_offset"] == 31
    assert audio["ptp_domain_token"] == "42"
    assert parsed["unknown_lines"] == ["z=unrecognized"]
    assert parsed["raw_sdp"] == sdp


@pytest.mark.parametrize("ptime,expected", [("0.001", 1), ("0.2500", 250), ("1e-3", 1), ("4294967.295", 4294967295)])
def test_sdp_packet_time_preserves_exact_microseconds(ptime, expected):
    parsed = parse_sdp(ROUTABLE_SDP.replace("a=ptime:0.25", f"a=ptime:{ptime}"))
    assert parsed["routable_audio"]["packet_time_microseconds"] == expected


@pytest.mark.parametrize(
    "old,new",
    [
        ("123456789012", "0"),
        ("s=Studio Feed", "s="),
        ("5004 RTP/AVP", "0 RTP/AVP"),
        ("RTP/AVP", "TCP"),
        ("L24/48000/2", "opus/48000/2"),
        ("239.69.1.10/32", "127.0.0.1"),
    ],
)
def test_sdp_unusable_announcements_do_not_become_routes(old, new):
    sdp = ROUTABLE_SDP.replace("c=IN IP4 239.69.1.11/32\r\n", "").replace(old, new)
    parsed = parse_sdp(sdp)

    assert not parsed["routable"]
    assert parsed["routable_audio"] is None
    assert parsed["routability_errors"]


@pytest.mark.parametrize(
    "addition", ["v=0\r\n", "a=recvonly\r\na=inactive\r\n", "a=rtpmap:128 L24/48000\r\n", "c=IN IP4 300.1.1.1\r\n"]
)
def test_sdp_rejects_ambiguous_or_invalid_fields(addition):
    with pytest.raises(SdpParseError):
        parse_sdp(ROUTABLE_SDP + addition)


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


@pytest.mark.parametrize("ptime", ["0", "-1", "0.0001", "NaN", "word", "4294967.296", "1e99999"])
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
    assert parsed.sdp["routable"] is True


@pytest.mark.parametrize(
    ("packet", "message"),
    [
        (b"", "shorter"),
        (sap_packet()[:7], "shorter"),
        (sap_packet(flags=0x10), "IPv6"),
        (sap_packet(flags=0x02), "encrypted"),
        (sap_packet(flags=0x01), "compressed"),
        (bytes([0x40]) + sap_packet()[1:], "version 1"),
        (sap_packet(message_hash=0), "hash"),
        (sap_packet(origin="0.0.0.0"), "origin"),
        (sap_packet().replace(SAP_CONTENT_TYPE, b"text/plain\x00", 1), "application/sdp"),
        (sap_packet().replace(SAP_CONTENT_TYPE, b"application/sdp ", 1), "application/sdp"),
        (sap_packet(""), "no SDP payload"),
        (sap_packet("") + b"\xff", "UTF-8"),
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


@pytest.mark.parametrize("same_hash,same_content", [(True, True), (True, False), (False, True), (False, False)])
def test_inventory_refresh_requires_both_message_identity_and_content(same_hash, same_content):
    inventory = SapFlowInventory()
    transport = {"announcement_interface": "eth0", "packet_source_ipv4": "192.0.2.10"}
    original = inventory.ingest(sap_packet(message_hash=123), **transport, received_monotonic=1, wall_time=1)
    changed = inventory.ingest(
        sap_packet(
            ROUTABLE_SDP if same_content else ROUTABLE_SDP.replace("Studio Feed", "Replacement Feed"),
            message_hash=123 if same_hash else 456,
        ),
        **transport,
        received_monotonic=2,
        wall_time=2,
    )

    assert original is not None and changed is not None
    expected = SapInventoryChangeKind.REFRESHED if same_hash and same_content else SapInventoryChangeKind.REPLACED
    assert changed.kind is expected
    assert changed.flow.discovered_at == original.flow.discovered_at


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


def test_duplicate_sap_retains_reception_interfaces_without_duplicate_sources():
    inventory = SapFlowInventory()
    for interface in ("eth0", "eth1", "eth0"):
        inventory.ingest(sap_packet(), announcement_interface=interface, packet_source_ipv4="192.0.2.44")
    assert len(inventory.flows()) == 1
    assert inventory.flows()[0].to_dict()["announcement_interfaces"] == ["eth0", "eth1"]


@pytest.mark.asyncio
async def test_partial_sap_membership_failure_keeps_healthy_group_and_reports_error():
    class PartialSocket(FakeSocket):
        def setsockopt(self, *arguments):
            if arguments[:2] == (socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP) and arguments[2][:4] == socket.inet_aton(
                "224.0.0.56"
            ):
                raise OSError("membership denied")
            super().setsockopt(*arguments)

    async def endpoint_factory(protocol_factory, *, sock):
        return FakeTransport(), protocol_factory()

    interface = SapInterface("eth0", "192.0.2.10")
    service = SapDiscoveryService(
        interface_provider=lambda: [interface],
        socket_factory=PartialSocket,
        endpoint_factory=endpoint_factory,
        network_changes=FakeNetworkChanges(),
    )
    await service.start()
    try:
        assert service.listening_interfaces == (interface,)
        assert service.diagnostics()["listener_errors"] == {"eth0/192.0.2.10/224.0.0.56": "membership denied"}
    finally:
        await service.stop()


class FakeTransport:
    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True


class FakeNetworkChanges:
    def __init__(self) -> None:
        self.callback = None

    def subscribe(self, callback):
        self.callback = callback

        def unsubscribe():
            self.callback = None

        return unsubscribe

    def emit(self) -> None:
        assert self.callback is not None
        self.callback()


@pytest.mark.asyncio
async def test_service_joins_sap_group_separately_on_every_active_ipv4_interface():
    interfaces = [SapInterface("eth0", "192.0.2.10"), SapInterface("eth1", "198.51.100.20")]
    sockets = []
    endpoints = []
    changes = []
    network_changes = FakeNetworkChanges()

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
        expiry_check_seconds=60,
        network_changes=network_changes,
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
            pipewire = socket.inet_aton("224.0.0.56") + socket.inet_aton(interface.address)
            assert (socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, pipewire) in multicast_socket.options

        endpoints[1][1].datagram_received(sap_packet(), ("192.0.2.44", SAP_PORT))
        assert changes[0].kind is SapInventoryChangeKind.ADDED
        assert changes[0].flow.announcement_interface == "eth1"

        removed_transport = endpoints[0][0]
        interfaces.pop(0)
        network_changes.emit()
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert removed_transport.closed is True
        assert service.listening_interfaces == (SapInterface("eth1", "198.51.100.20"),)
    finally:
        await service.stop()
    assert all(transport.closed for transport, _, _ in endpoints)
