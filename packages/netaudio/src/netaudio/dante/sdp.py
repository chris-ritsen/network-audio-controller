from netaudio import core
from netaudio.core._types import SdpDocument


class SdpParseError(ValueError):
    pass


def parse_sdp(raw_sdp: str) -> SdpDocument:
    if not isinstance(raw_sdp, str):
        raise SdpParseError("SDP must be text")

    try:
        return core.parse_sdp(raw_sdp)
    except (core.NetaudioCoreError, UnicodeEncodeError) as exception:
        raise SdpParseError(str(exception)) from exception
