from __future__ import annotations

from netaudio.dante.const import (
    PROTOCOL_ARC_2729,
    PROTOCOL_ARC_2809,
    PROTOCOL_ARC_280F,
    RESULT_CODE_FRONTEND_UNAVAILABLE,
    RESULT_CODE_SUCCESS,
    RESULT_CODE_SUCCESS_EXTENDED,
)


class ChannelFrontendError(RuntimeError):
    pass


def channel_result_code(response: bytes | None, operation: str) -> int:
    if response is None:
        raise ChannelFrontendError(f"{operation} did not receive a response")

    from netaudio import core

    try:
        result_code = core.parse_response("result_code", response)
    except core.NetaudioCoreError as exception:
        raise ChannelFrontendError(f"{operation} returned an invalid response") from exception
    if not isinstance(result_code, int):
        raise ChannelFrontendError(f"{operation} returned an invalid response")
    return result_code


def _channel_name_protocol_identifier_from_probe(
    response: bytes | None,
    operation: str,
    response_kind: str,
) -> int:
    result_code = channel_result_code(response, operation)
    if result_code == RESULT_CODE_FRONTEND_UNAVAILABLE:
        return PROTOCOL_ARC_2729
    if result_code not in (RESULT_CODE_SUCCESS, RESULT_CODE_SUCCESS_EXTENDED):
        raise ChannelFrontendError(f"{operation} failed with result 0x{result_code:04X}")

    from netaudio import core

    try:
        page = core.parse_response(response_kind, response)
    except core.NetaudioCoreError as exception:
        raise ChannelFrontendError(f"{operation} returned an invalid status page") from exception
    if not isinstance(page, dict):
        raise ChannelFrontendError(f"{operation} returned an invalid status page")
    protocol_id = page.get("protocol_id")
    if protocol_id not in (PROTOCOL_ARC_2809, PROTOCOL_ARC_280F):
        raise ChannelFrontendError(f"{operation} returned an unsupported protocol identifier")
    return protocol_id


def receiver_channel_name_protocol_identifier_from_probe(response: bytes | None) -> int:
    return _channel_name_protocol_identifier_from_probe(
        response,
        "receiver channel frontend probe",
        "modern_arc_receiver_channel_status_page",
    )


def transmitter_channel_name_protocol_identifier_from_probe(response: bytes | None) -> int:
    return _channel_name_protocol_identifier_from_probe(
        response,
        "transmitter channel frontend probe",
        "modern_arc_transmitter_channel_status_page",
    )
