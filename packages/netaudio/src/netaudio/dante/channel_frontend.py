from __future__ import annotations

from netaudio import core
from netaudio.dante.channel import channel_by_number


class ChannelFrontendError(RuntimeError):
    pass


class ChannelRenameCapabilityError(ValueError):
    def __init__(self, message: str, *, prohibited: bool = False):
        super().__init__(message)
        self.prohibited = prohibited


def require_channel_rename_supported(device, channel_type: str, channel_number: int) -> None:
    if channel_type != "rx" or getattr(device, "requires_managed_control", False):
        return

    channels = getattr(device, "rx_channels", None)

    try:
        channel = channel_by_number(channels.values(), channel_number) if isinstance(channels, dict) else None
    except RuntimeError as error:
        raise ChannelRenameCapabilityError(str(error)) from error

    if channel is None:
        raise ChannelRenameCapabilityError(f"receiver channel {channel_number} is unavailable")

    capability = getattr(channel, "can_rename", None)

    if capability is False:
        raise ChannelRenameCapabilityError(
            f"receiver channel {channel_number} rename capability prohibits renaming", prohibited=True
        )

    if capability is not True:
        raise ChannelRenameCapabilityError(f"receiver channel {channel_number} rename capability is unavailable")


def require_channel_acknowledgement(response: bytes | None, operation: str) -> None:
    acknowledgement = core.command_acknowledgement(response)

    if acknowledgement is None:
        raise ChannelFrontendError(f"{operation} did not receive a response")

    if acknowledgement.get("parseable") is not True:
        raise ChannelFrontendError(f"{operation} returned an invalid response")

    if acknowledgement.get("accepted") is not True:
        raise ChannelFrontendError(f"{operation} was not acknowledged by the device")


def _channel_name_protocol_identifier_from_probe(
    response: bytes | None,
    operation: str,
    response_kind: str,
) -> int:
    if response is None:
        raise ChannelFrontendError(f"{operation} did not receive a response")

    try:
        return core.parse_response(response_kind, response)
    except core.NetaudioCoreError as exception:
        raise ChannelFrontendError(f"{operation} returned an invalid response") from exception


def receiver_channel_name_protocol_identifier_from_probe(response: bytes | None) -> int:
    return _channel_name_protocol_identifier_from_probe(
        response,
        "receiver channel frontend probe",
        "receiver_channel_name_protocol",
    )


def transmitter_channel_name_protocol_identifier_from_probe(response: bytes | None) -> int:
    return _channel_name_protocol_identifier_from_probe(
        response,
        "transmitter channel frontend probe",
        "transmitter_channel_name_protocol",
    )
