from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from ipaddress import AddressValueError, IPv4Address


class SdpParseError(ValueError):
    pass


@dataclass(frozen=True)
class SdpConnection:
    network_type: str
    address_type: str
    address: str
    time_to_live: int | None
    address_count: int | None
    raw_value: str

    def to_dict(self) -> dict:
        return {
            "network_type": self.network_type,
            "address_type": self.address_type,
            "address": self.address,
            "time_to_live": self.time_to_live,
            "address_count": self.address_count,
            "raw_value": self.raw_value,
        }


@dataclass(frozen=True)
class SdpRtpMap:
    payload_type: int
    encoding: str
    sample_rate: int
    channels: int
    raw_value: str

    def to_dict(self) -> dict:
        return {
            "payload_type": self.payload_type,
            "encoding": self.encoding,
            "sample_rate": self.sample_rate,
            "channels": self.channels,
            "raw_value": self.raw_value,
        }


@dataclass(frozen=True)
class SdpMediaDescription:
    media_type: str
    port: int | None
    port_count: int | None
    protocol: str
    payload_types: tuple[int, ...]
    information: str | None
    connections: tuple[SdpConnection, ...]
    attributes: tuple[str, ...]
    raw_value: str

    def to_dict(self) -> dict:
        return {
            "media_type": self.media_type,
            "port": self.port,
            "port_count": self.port_count,
            "protocol": self.protocol,
            "payload_types": list(self.payload_types),
            "information": self.information,
            "connections": [connection.to_dict() for connection in self.connections],
            "attributes": list(self.attributes),
            "raw_value": self.raw_value,
        }


@dataclass(frozen=True)
class SdpRoutableAudio:
    media_title: str | None
    primary_destination_address: str
    secondary_destination_address: str | None
    destination_port: int
    payload_type: int
    encoding: str
    sample_rate: int
    channel_count: int
    packet_time_microseconds: int | None
    direction: str | None
    media_clock: str | None
    clock_offset: int | None
    ptp_reference: str | None
    ptp_domain_token: str | None
    dante_origin: bool

    def to_dict(self) -> dict:
        return {
            "media_title": self.media_title,
            "primary_destination_address": self.primary_destination_address,
            "secondary_destination_address": self.secondary_destination_address,
            "destination_port": self.destination_port,
            "payload_type": self.payload_type,
            "encoding": self.encoding,
            "sample_rate": self.sample_rate,
            "channel_count": self.channel_count,
            "packet_time_microseconds": self.packet_time_microseconds,
            "direction": self.direction,
            "media_clock": self.media_clock,
            "clock_offset": self.clock_offset,
            "ptp_reference": self.ptp_reference,
            "ptp_domain_token": self.ptp_domain_token,
            "dante_origin": self.dante_origin,
        }


@dataclass(frozen=True)
class SdpDocument:
    version: int
    origin_username: str
    session_id: int
    session_version: int
    origin_network_type: str
    origin_address_type: str
    origin_address: str
    session_name: str
    session_information: str | None
    start_time: int
    stop_time: int
    session_connections: tuple[SdpConnection, ...]
    session_attributes: tuple[str, ...]
    media_descriptions: tuple[SdpMediaDescription, ...]
    rtp_maps: tuple[SdpRtpMap, ...]
    routable_audio: SdpRoutableAudio | None
    routability_errors: tuple[str, ...]
    unknown_lines: tuple[str, ...]
    raw_sdp: str

    @property
    def routable(self) -> bool:
        return self.routable_audio is not None

    def to_dict(self) -> dict:
        return {
            "version": self.version,
            "origin_username": self.origin_username,
            "session_id": self.session_id,
            "session_version": self.session_version,
            "origin_network_type": self.origin_network_type,
            "origin_address_type": self.origin_address_type,
            "origin_address": self.origin_address,
            "session_name": self.session_name,
            "session_information": self.session_information,
            "start_time": self.start_time,
            "stop_time": self.stop_time,
            "session_connections": [connection.to_dict() for connection in self.session_connections],
            "session_attributes": list(self.session_attributes),
            "media_descriptions": [media.to_dict() for media in self.media_descriptions],
            "rtp_maps": [rtp_map.to_dict() for rtp_map in self.rtp_maps],
            "routable": self.routable,
            "routable_audio": self.routable_audio.to_dict() if self.routable_audio else None,
            "routability_errors": list(self.routability_errors),
            "unknown_lines": list(self.unknown_lines),
            "raw_sdp": self.raw_sdp,
        }


@dataclass
class _MutableMedia:
    media_type: str
    port: int | None
    port_count: int | None
    protocol: str
    payload_types: tuple[int, ...]
    raw_value: str
    information: str | None = None
    connections: list[SdpConnection] | None = None
    attributes: list[str] | None = None

    def __post_init__(self) -> None:
        self.connections = []
        self.attributes = []

    def freeze(self) -> SdpMediaDescription:
        return SdpMediaDescription(
            media_type=self.media_type,
            port=self.port,
            port_count=self.port_count,
            protocol=self.protocol,
            payload_types=self.payload_types,
            information=self.information,
            connections=tuple(self.connections or ()),
            attributes=tuple(self.attributes or ()),
            raw_value=self.raw_value,
        )


def _unsigned_integer(value: str, description: str, maximum: int = (1 << 64) - 1) -> int:
    if not value.isascii() or not value.isdecimal():
        raise SdpParseError(f"{description} must be an unsigned decimal integer")
    result = int(value)
    if result > maximum:
        raise SdpParseError(f"{description} exceeds its supported range")
    return result


def _connection(value: str) -> SdpConnection:
    fields = value.split()
    if len(fields) != 3:
        raise SdpParseError("c= must contain network type, address type, and address")
    network_type, address_type, address_field = fields
    address_parts = address_field.split("/")
    if len(address_parts) > 3 or not address_parts[0]:
        raise SdpParseError("c= has an invalid connection address")
    address = address_parts[0]
    ttl = _unsigned_integer(address_parts[1], "c= time to live", 255) if len(address_parts) >= 2 else None
    address_count = _unsigned_integer(address_parts[2], "c= address count", 65535) if len(address_parts) == 3 else None
    if address_count == 0:
        raise SdpParseError("c= address count must be positive")
    if address_type.upper() == "IP4":
        try:
            parsed_address = IPv4Address(address)
        except AddressValueError as exception:
            raise SdpParseError("c= contains an invalid IPv4 address") from exception
        address = str(parsed_address)
    return SdpConnection(
        network_type=network_type,
        address_type=address_type,
        address=address,
        time_to_live=ttl,
        address_count=address_count,
        raw_value=value,
    )


def _media(value: str) -> _MutableMedia:
    fields = value.split()
    if len(fields) < 4:
        raise SdpParseError("m= must contain media, port, protocol, and a payload type")
    port_parts = fields[1].split("/")
    if len(port_parts) > 2:
        raise SdpParseError("m= has an invalid port field")
    try:
        port = _unsigned_integer(port_parts[0], "m= port", 65535)
        port_count = _unsigned_integer(port_parts[1], "m= port count", 65535) if len(port_parts) == 2 else None
        payload_types = tuple(_unsigned_integer(field, "m= payload type", 127) for field in fields[3:])
    except SdpParseError:
        port = None
        port_count = None
        payload_types = ()
    return _MutableMedia(
        media_type=fields[0],
        port=port,
        port_count=port_count,
        protocol=fields[2],
        payload_types=payload_types,
        raw_value=value,
    )


def _attribute_value(attributes: tuple[str, ...], name: str) -> str | None:
    prefix = f"{name}:"
    for attribute in attributes:
        if attribute == name:
            return ""
        if attribute.startswith(prefix):
            return attribute[len(prefix) :]
    return None


def _rtp_maps(attributes: tuple[str, ...]) -> tuple[SdpRtpMap, ...]:
    result = []
    for attribute in attributes:
        if not attribute.lower().startswith("rtpmap:"):
            continue
        value = attribute.split(":", 1)[1]
        fields = value.split(None, 1)
        if len(fields) != 2:
            raise SdpParseError("a=rtpmap must contain a payload type and encoding")
        payload_type = _unsigned_integer(fields[0], "a=rtpmap payload type", 127)
        encoding_fields = fields[1].split("/")
        if len(encoding_fields) not in {2, 3} or not encoding_fields[0]:
            raise SdpParseError("a=rtpmap has an invalid encoding")
        sample_rate = _unsigned_integer(encoding_fields[1], "a=rtpmap sample rate", (1 << 32) - 1)
        channels = (
            _unsigned_integer(encoding_fields[2], "a=rtpmap channel count", 65535) if len(encoding_fields) == 3 else 1
        )
        result.append(
            SdpRtpMap(
                payload_type=payload_type,
                encoding=encoding_fields[0].upper(),
                sample_rate=sample_rate,
                channels=channels,
                raw_value=value,
            )
        )
    return tuple(result)


def _packet_time_microseconds(attributes: tuple[str, ...]) -> int | None:
    value = _attribute_value(attributes, "ptime")
    if value is None:
        return None
    try:
        microseconds = Decimal(value) * 1000
    except InvalidOperation as exception:
        raise SdpParseError("a=ptime must be a decimal number of milliseconds") from exception
    if not microseconds.is_finite() or microseconds <= 0 or microseconds != microseconds.to_integral_value():
        raise SdpParseError("a=ptime must resolve to a positive whole number of microseconds")
    result = int(microseconds)
    if result > (1 << 32) - 1:
        raise SdpParseError("a=ptime exceeds its supported range")
    return result


def _direction(attributes: tuple[str, ...]) -> str | None:
    directions = [value for value in attributes if value in {"sendrecv", "sendonly", "recvonly", "inactive"}]
    if len(directions) > 1:
        raise SdpParseError("SDP scope contains more than one direction attribute")
    return directions[0] if directions else None


def _clock_fields(attributes: tuple[str, ...]) -> tuple[str | None, int | None, str | None, str | None]:
    media_clock = _attribute_value(attributes, "mediaclk")
    clock_offset = None
    if media_clock is not None:
        tokens = media_clock.split()
        direct = next((token.split("=", 1)[1] for token in tokens if token.startswith("direct=")), None)
        if direct is not None:
            clock_offset = _unsigned_integer(direct, "a=mediaclk direct offset", (1 << 32) - 1)
    ptp_reference = _attribute_value(attributes, "ts-refclk")
    ptp_domain_token = None
    if ptp_reference is not None and ptp_reference.lower().startswith("ptp="):
        ptp_fields = ptp_reference.split(":")
        if len(ptp_fields) >= 3 and ptp_fields[-1]:
            ptp_domain_token = ptp_fields[-1]
    return media_clock, clock_offset, ptp_reference, ptp_domain_token


def _is_dante_origin(attributes: tuple[str, ...]) -> bool:
    for attribute in attributes:
        name = attribute.split(":", 1)[0].lower()
        if name in {"dante", "x-dante"}:
            return True
    return False


def _routable_audio(
    media: SdpMediaDescription,
    session_connections: tuple[SdpConnection, ...],
    session_attributes: tuple[str, ...],
) -> tuple[SdpRoutableAudio | None, tuple[str, ...], tuple[SdpRtpMap, ...]]:
    errors = []
    if media.port is None or not 1 <= media.port <= 65535:
        errors.append("audio media port is not a valid UDP port")
    if media.protocol.upper() not in {"RTP/AVP", "RTP/AVPF"}:
        errors.append("audio media protocol is not supported RTP over UDP")

    connections = media.connections or session_connections
    ipv4_connections = []
    for connection in connections:
        if connection.network_type.upper() != "IN" or connection.address_type.upper() != "IP4":
            continue
        address = IPv4Address(connection.address)
        if address.is_unspecified or address.is_loopback or int(address) == 0xFFFFFFFF:
            continue
        if connection.address not in ipv4_connections:
            ipv4_connections.append(connection.address)
    if not ipv4_connections:
        errors.append("audio media has no usable IPv4 connection address")

    attributes = session_attributes + media.attributes
    maps = _rtp_maps(attributes)
    supported_maps = {
        value.payload_type: value
        for value in maps
        if value.encoding in {"L16", "L24", "L32"} and value.sample_rate > 0 and value.channels > 0
    }
    selected_map = next((supported_maps[payload] for payload in media.payload_types if payload in supported_maps), None)
    if selected_map is None:
        errors.append("audio media has no matching L16, L24, or L32 rtpmap")

    packet_time_attributes = (
        media.attributes if _attribute_value(media.attributes, "ptime") is not None else session_attributes
    )
    packet_time = _packet_time_microseconds(packet_time_attributes)
    direction = _direction(media.attributes) or _direction(session_attributes)
    clock_attributes = (
        media.attributes if _attribute_value(media.attributes, "mediaclk") is not None else session_attributes
    )
    media_clock, clock_offset, _, _ = _clock_fields(clock_attributes)
    ptp_attributes = (
        media.attributes if _attribute_value(media.attributes, "ts-refclk") is not None else session_attributes
    )
    _, _, ptp_reference, ptp_domain_token = _clock_fields(ptp_attributes)
    dante_origin = _is_dante_origin(attributes)
    if errors or selected_map is None or media.port is None or not ipv4_connections:
        return None, tuple(errors), maps
    return (
        SdpRoutableAudio(
            media_title=media.information,
            primary_destination_address=ipv4_connections[0],
            secondary_destination_address=ipv4_connections[1] if len(ipv4_connections) > 1 else None,
            destination_port=media.port,
            payload_type=selected_map.payload_type,
            encoding=selected_map.encoding,
            sample_rate=selected_map.sample_rate,
            channel_count=selected_map.channels,
            packet_time_microseconds=packet_time,
            direction=direction,
            media_clock=media_clock,
            clock_offset=clock_offset,
            ptp_reference=ptp_reference,
            ptp_domain_token=ptp_domain_token,
            dante_origin=dante_origin,
        ),
        (),
        maps,
    )


def parse_sdp(raw_sdp: str) -> SdpDocument:
    if not isinstance(raw_sdp, str):
        raise SdpParseError("SDP must be text")
    if "\x00" in raw_sdp:
        raise SdpParseError("SDP must not contain NUL bytes")
    lines = raw_sdp.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    while lines and lines[-1] == "":
        lines.pop()
    if not lines or any(len(line) < 2 or line[1] != "=" or not line[0].isalpha() for line in lines):
        raise SdpParseError("SDP contains a malformed line")

    singletons: dict[str, str] = {}
    session_information = None
    session_connections = []
    session_attributes = []
    media_values: list[_MutableMedia] = []
    current_media: _MutableMedia | None = None
    unknown_lines = []
    for line in lines:
        kind, value = line[0], line[2:]
        if kind in {"v", "o", "s", "t"}:
            if kind in singletons:
                raise SdpParseError(f"SDP contains more than one {kind}= line")
            singletons[kind] = value
        elif kind == "m":
            current_media = _media(value)
            media_values.append(current_media)
        elif kind == "c":
            target = current_media.connections if current_media is not None else session_connections
            target.append(_connection(value))
        elif kind == "i":
            if current_media is None:
                if session_information is not None:
                    raise SdpParseError("SDP contains more than one session i= line")
                session_information = value
            else:
                if current_media.information is not None:
                    raise SdpParseError("SDP media contains more than one i= line")
                current_media.information = value
        elif kind == "a":
            target = current_media.attributes if current_media is not None else session_attributes
            target.append(value)
        else:
            unknown_lines.append(line)

    missing = [kind for kind in ("v", "o", "s", "t") if kind not in singletons]
    if missing:
        raise SdpParseError(f"SDP is missing required lines: {', '.join(f'{kind}=' for kind in missing)}")
    version = _unsigned_integer(singletons["v"], "v= value", 255)
    if version != 0:
        raise SdpParseError("only SDP version 0 is supported")
    origin = singletons["o"].split()
    if len(origin) != 6:
        raise SdpParseError("o= must contain six fields")
    session_id = _unsigned_integer(origin[1], "o= session ID")
    session_version = _unsigned_integer(origin[2], "o= session version")
    timing = singletons["t"].split()
    if len(timing) != 2:
        raise SdpParseError("t= must contain start and stop times")
    start_time = _unsigned_integer(timing[0], "t= start time")
    stop_time = _unsigned_integer(timing[1], "t= stop time")

    media_descriptions = tuple(value.freeze() for value in media_values)
    audio_media = [media for media in media_descriptions if media.media_type.lower() == "audio"]
    routable = None
    routability_errors: tuple[str, ...] = ("SDP has no audio media description",)
    all_maps: list[SdpRtpMap] = []
    for media in media_descriptions:
        all_maps.extend(_rtp_maps(tuple(session_attributes) + media.attributes))
    if audio_media:
        per_media_errors = []
        for media in audio_media:
            candidate, errors, _ = _routable_audio(media, tuple(session_connections), tuple(session_attributes))
            if candidate is not None:
                routable = candidate
                routability_errors = ()
                break
            per_media_errors.extend(errors)
        if routable is None:
            routability_errors = tuple(dict.fromkeys(per_media_errors))
    if session_id == 0:
        routable = None
        routability_errors = tuple(dict.fromkeys((*routability_errors, "SDP session ID is zero")))
    if not singletons["s"]:
        routable = None
        routability_errors = tuple(dict.fromkeys((*routability_errors, "SDP session name is empty")))

    return SdpDocument(
        version=version,
        origin_username=origin[0],
        session_id=session_id,
        session_version=session_version,
        origin_network_type=origin[3],
        origin_address_type=origin[4],
        origin_address=origin[5],
        session_name=singletons["s"],
        session_information=session_information,
        start_time=start_time,
        stop_time=stop_time,
        session_connections=tuple(session_connections),
        session_attributes=tuple(session_attributes),
        media_descriptions=media_descriptions,
        rtp_maps=tuple(dict.fromkeys(all_maps)),
        routable_audio=routable,
        routability_errors=routability_errors,
        unknown_lines=tuple(unknown_lines),
        raw_sdp=raw_sdp,
    )


__all__ = [
    "SdpConnection",
    "SdpDocument",
    "SdpMediaDescription",
    "SdpParseError",
    "SdpRoutableAudio",
    "SdpRtpMap",
    "parse_sdp",
]
