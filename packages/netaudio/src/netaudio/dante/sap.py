from __future__ import annotations

import hashlib
import time
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from enum import Enum
from ipaddress import AddressValueError, IPv4Address

from netaudio.dante.sdp import SdpDocument, SdpParseError, parse_sdp

SAP_MULTICAST_ADDRESS = "239.255.255.255"
SAP_PORT = 9875
SAP_EXPIRY_SECONDS = 3600.0
SAP_CONTENT_TYPE = b"application/sdp\x00"


class SapParseError(ValueError):
    pass


def _timestamp(wall_time: float) -> str:
    return datetime.fromtimestamp(wall_time, timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


@dataclass(frozen=True)
class SapPacket:
    version: int
    delete: bool
    reserved: bool
    authentication_length_words: int
    message_hash: int
    origin_address: str
    authentication_data_hexadecimal: str
    content_type: str
    raw_sdp: str
    sdp: SdpDocument

    def to_dict(self) -> dict:
        return {
            "version": self.version,
            "delete": self.delete,
            "reserved": self.reserved,
            "authentication_length_words": self.authentication_length_words,
            "message_hash": self.message_hash,
            "origin_address": self.origin_address,
            "authentication_data_hexadecimal": self.authentication_data_hexadecimal,
            "content_type": self.content_type,
            "raw_sdp": self.raw_sdp,
            "sdp": self.sdp.to_dict(),
        }


def parse_sap_packet(data: bytes) -> SapPacket:
    if not isinstance(data, bytes):
        raise SapParseError("SAP packet must be bytes")
    if len(data) < 8:
        raise SapParseError("SAP packet is shorter than its IPv4 header")
    flags = data[0]
    version = flags >> 5
    address_type_ipv6 = bool(flags & 0x10)
    reserved = bool(flags & 0x08)
    delete = bool(flags & 0x04)
    encrypted = bool(flags & 0x02)
    compressed = bool(flags & 0x01)
    if version != 1:
        raise SapParseError("only SAP version 1 is supported")
    if address_type_ipv6:
        raise SapParseError("SAP IPv6 origin addresses are not supported")
    if encrypted:
        raise SapParseError("encrypted SAP packets are not supported")
    if compressed:
        raise SapParseError("compressed SAP packets are not supported")

    authentication_length_words = data[1]
    message_hash = int.from_bytes(data[2:4], "big")
    if message_hash == 0:
        raise SapParseError("SAP message hash must be nonzero")
    origin = IPv4Address(data[4:8])
    if origin.is_unspecified:
        raise SapParseError("SAP origin address must be nonzero")
    authentication_size = authentication_length_words * 4
    payload_offset = 8 + authentication_size
    if payload_offset > len(data):
        raise SapParseError("SAP authentication length exceeds the packet")
    payload = data[payload_offset:]
    if not payload.startswith(SAP_CONTENT_TYPE):
        raise SapParseError("SAP payload type must be exactly NUL-terminated application/sdp")
    raw_sdp_bytes = payload[len(SAP_CONTENT_TYPE) :]
    if not raw_sdp_bytes:
        raise SapParseError("SAP packet has no SDP payload")
    try:
        raw_sdp = raw_sdp_bytes.decode("utf-8")
    except UnicodeDecodeError as exception:
        raise SapParseError("SAP SDP payload is not valid UTF-8") from exception
    try:
        sdp = parse_sdp(raw_sdp)
    except SdpParseError as exception:
        raise SapParseError(f"invalid SAP SDP payload: {exception}") from exception
    return SapPacket(
        version=version,
        delete=delete,
        reserved=reserved,
        authentication_length_words=authentication_length_words,
        message_hash=message_hash,
        origin_address=str(origin),
        authentication_data_hexadecimal=data[8:payload_offset].hex(),
        content_type="application/sdp",
        raw_sdp=raw_sdp,
        sdp=sdp,
    )


@dataclass(frozen=True)
class DiscoveredExternalFlow:
    source_ipv4: str
    session_id: int
    message_hash: int
    content_sha256: str
    flow_name: str
    media_title: str | None
    origin_username: str
    ptp_domain_token: str | None
    channel_count: int | None
    clock_offset: int | None
    primary_destination_address: str | None
    primary_destination_port: int | None
    secondary_destination_address: str | None
    secondary_destination_port: int | None
    encoding: str | None
    sample_rate: int | None
    packet_time_microseconds: int | None
    direction: str | None
    dante_origin: bool
    routable: bool
    routability_errors: tuple[str, ...]
    announcement_interface: str
    packet_source_ipv4: str
    packet_source_port: int
    discovered_at: str
    refreshed_at: str
    expires_at: str
    discovered_monotonic: float
    refreshed_monotonic: float
    expires_monotonic: float
    authentication_length_words: int
    authentication_data_hexadecimal: str
    raw_sdp: str
    sdp: SdpDocument

    @property
    def identity(self) -> tuple[str, int]:
        return self.source_ipv4, self.session_id

    @classmethod
    def from_packet(
        cls,
        packet: SapPacket,
        *,
        announcement_interface: str,
        packet_source_ipv4: str,
        packet_source_port: int,
        received_monotonic: float,
        wall_time: float,
        expiry_seconds: float,
    ) -> DiscoveredExternalFlow:
        audio = packet.sdp.routable_audio
        expires_wall_time = wall_time + expiry_seconds
        return cls(
            source_ipv4=packet.origin_address,
            session_id=packet.sdp.session_id,
            message_hash=packet.message_hash,
            content_sha256=hashlib.sha256(packet.raw_sdp.encode("utf-8")).hexdigest(),
            flow_name=packet.sdp.session_name,
            media_title=audio.media_title if audio else None,
            origin_username=packet.sdp.origin_username,
            ptp_domain_token=audio.ptp_domain_token if audio else None,
            channel_count=audio.channel_count if audio else None,
            clock_offset=audio.clock_offset if audio else None,
            primary_destination_address=audio.primary_destination_address if audio else None,
            primary_destination_port=audio.destination_port if audio else None,
            secondary_destination_address=audio.secondary_destination_address if audio else None,
            secondary_destination_port=audio.destination_port
            if audio and audio.secondary_destination_address
            else None,
            encoding=audio.encoding if audio else None,
            sample_rate=audio.sample_rate if audio else None,
            packet_time_microseconds=audio.packet_time_microseconds if audio else None,
            direction=audio.direction if audio else None,
            dante_origin=audio.dante_origin if audio else False,
            routable=packet.sdp.routable,
            routability_errors=packet.sdp.routability_errors,
            announcement_interface=announcement_interface,
            packet_source_ipv4=packet_source_ipv4,
            packet_source_port=packet_source_port,
            discovered_at=_timestamp(wall_time),
            refreshed_at=_timestamp(wall_time),
            expires_at=_timestamp(expires_wall_time),
            discovered_monotonic=received_monotonic,
            refreshed_monotonic=received_monotonic,
            expires_monotonic=received_monotonic + expiry_seconds,
            authentication_length_words=packet.authentication_length_words,
            authentication_data_hexadecimal=packet.authentication_data_hexadecimal,
            raw_sdp=packet.raw_sdp,
            sdp=packet.sdp,
        )

    def refreshed(
        self,
        *,
        announcement_interface: str,
        packet_source_ipv4: str,
        packet_source_port: int,
        received_monotonic: float,
        wall_time: float,
        expiry_seconds: float,
    ) -> DiscoveredExternalFlow:
        return replace(
            self,
            announcement_interface=announcement_interface,
            packet_source_ipv4=packet_source_ipv4,
            packet_source_port=packet_source_port,
            refreshed_at=_timestamp(wall_time),
            expires_at=_timestamp(wall_time + expiry_seconds),
            refreshed_monotonic=received_monotonic,
            expires_monotonic=received_monotonic + expiry_seconds,
        )

    def to_dict(self) -> dict:
        return {
            "source_ipv4": self.source_ipv4,
            "session_id": self.session_id,
            "message_hash": self.message_hash,
            "content_sha256": self.content_sha256,
            "flow_name": self.flow_name,
            "media_title": self.media_title,
            "origin_username": self.origin_username,
            "ptp_domain_token": self.ptp_domain_token,
            "channel_count": self.channel_count,
            "clock_offset": self.clock_offset,
            "primary_destination": (
                {"address": self.primary_destination_address, "port": self.primary_destination_port}
                if self.primary_destination_address and self.primary_destination_port
                else None
            ),
            "secondary_destination": (
                {"address": self.secondary_destination_address, "port": self.secondary_destination_port}
                if self.secondary_destination_address and self.secondary_destination_port
                else None
            ),
            "encoding": self.encoding,
            "sample_rate": self.sample_rate,
            "packet_time_microseconds": self.packet_time_microseconds,
            "direction": self.direction,
            "dante_origin": self.dante_origin,
            "routable": self.routable,
            "routability_errors": list(self.routability_errors),
            "advertisement_supports_multiple_interfaces": self.secondary_destination_address is not None,
            "announcement_interface": self.announcement_interface,
            "packet_source_ipv4": self.packet_source_ipv4,
            "packet_source_port": self.packet_source_port,
            "discovered_at": self.discovered_at,
            "refreshed_at": self.refreshed_at,
            "expires_at": self.expires_at,
            "authentication_length_words": self.authentication_length_words,
            "authentication_data_hexadecimal": self.authentication_data_hexadecimal,
            "raw_sdp": self.raw_sdp,
            "sdp": self.sdp.to_dict(),
        }


class SapInventoryChangeKind(str, Enum):
    ADDED = "added"
    REFRESHED = "refreshed"
    REPLACED = "replaced"
    DELETED = "deleted"
    EXPIRED = "expired"


@dataclass(frozen=True)
class SapInventoryChange:
    kind: SapInventoryChangeKind
    identity: tuple[str, int]
    flow: DiscoveredExternalFlow | None
    previous: DiscoveredExternalFlow | None


class SapFlowInventory:
    def __init__(self, *, expiry_seconds: float = SAP_EXPIRY_SECONDS) -> None:
        if expiry_seconds <= 0:
            raise ValueError("SAP expiry interval must be positive")
        self.expiry_seconds = float(expiry_seconds)
        self._flows: dict[tuple[str, int], DiscoveredExternalFlow] = {}

    @staticmethod
    def identity_key(identity: tuple[str, int]) -> str:
        return f"{identity[0]}/{identity[1]}"

    def get(self, source_ipv4: str, session_id: int) -> DiscoveredExternalFlow | None:
        try:
            source_ipv4 = str(IPv4Address(source_ipv4))
        except AddressValueError as exception:
            raise ValueError("external-flow source must be an IPv4 address") from exception
        if not isinstance(session_id, int) or isinstance(session_id, bool) or not 0 <= session_id <= (1 << 64) - 1:
            raise ValueError("external-flow session ID must be an unsigned 64-bit integer")
        return self._flows.get((source_ipv4, session_id))

    def flows(self) -> tuple[DiscoveredExternalFlow, ...]:
        return tuple(self._flows[identity] for identity in sorted(self._flows))

    def to_dict(self) -> dict[str, dict]:
        return {self.identity_key(flow.identity): flow.to_dict() for flow in self.flows()}

    def ingest(
        self,
        data: bytes,
        *,
        announcement_interface: str,
        packet_source_ipv4: str,
        packet_source_port: int = SAP_PORT,
        received_monotonic: float | None = None,
        wall_time: float | None = None,
    ) -> SapInventoryChange | None:
        if not isinstance(announcement_interface, str) or not announcement_interface:
            raise ValueError("SAP announcement interface is required")
        try:
            packet_source_ipv4 = str(IPv4Address(packet_source_ipv4))
        except AddressValueError as exception:
            raise ValueError("SAP packet source must be an IPv4 address") from exception
        if (
            not isinstance(packet_source_port, int)
            or isinstance(packet_source_port, bool)
            or not 0 <= packet_source_port <= 65535
        ):
            raise ValueError("SAP packet source port is invalid")
        received_monotonic = time.monotonic() if received_monotonic is None else received_monotonic
        wall_time = time.time() if wall_time is None else wall_time
        packet = parse_sap_packet(data)
        identity = (packet.origin_address, packet.sdp.session_id)
        existing = self._flows.get(identity)
        if existing is not None and received_monotonic >= existing.expires_monotonic:
            del self._flows[identity]
            existing = None
        if packet.delete:
            if existing is None or existing.message_hash != packet.message_hash:
                return None
            del self._flows[identity]
            return SapInventoryChange(SapInventoryChangeKind.DELETED, identity, None, existing)

        candidate = DiscoveredExternalFlow.from_packet(
            packet,
            announcement_interface=announcement_interface,
            packet_source_ipv4=packet_source_ipv4,
            packet_source_port=packet_source_port,
            received_monotonic=received_monotonic,
            wall_time=wall_time,
            expiry_seconds=self.expiry_seconds,
        )
        if (
            existing is not None
            and existing.message_hash == candidate.message_hash
            and existing.content_sha256 == candidate.content_sha256
        ):
            refreshed = existing.refreshed(
                announcement_interface=announcement_interface,
                packet_source_ipv4=packet_source_ipv4,
                packet_source_port=packet_source_port,
                received_monotonic=received_monotonic,
                wall_time=wall_time,
                expiry_seconds=self.expiry_seconds,
            )
            self._flows[identity] = refreshed
            return SapInventoryChange(SapInventoryChangeKind.REFRESHED, identity, refreshed, existing)

        kind = SapInventoryChangeKind.ADDED if existing is None else SapInventoryChangeKind.REPLACED
        if existing is not None:
            candidate = replace(
                candidate,
                discovered_at=existing.discovered_at,
                discovered_monotonic=existing.discovered_monotonic,
            )
        self._flows[identity] = candidate
        return SapInventoryChange(kind, identity, candidate, existing)

    def expire(self, observed_monotonic: float | None = None) -> list[SapInventoryChange]:
        observed_monotonic = time.monotonic() if observed_monotonic is None else observed_monotonic
        changes = []
        for identity, flow in tuple(self._flows.items()):
            if observed_monotonic < flow.expires_monotonic:
                continue
            del self._flows[identity]
            changes.append(SapInventoryChange(SapInventoryChangeKind.EXPIRED, identity, None, flow))
        return changes


__all__ = [
    "DiscoveredExternalFlow",
    "SAP_CONTENT_TYPE",
    "SAP_EXPIRY_SECONDS",
    "SAP_MULTICAST_ADDRESS",
    "SAP_PORT",
    "SapFlowInventory",
    "SapInventoryChange",
    "SapInventoryChangeKind",
    "SapPacket",
    "SapParseError",
    "parse_sap_packet",
]
