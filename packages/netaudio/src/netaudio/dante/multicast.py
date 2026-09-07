from __future__ import annotations

from netaudio import core
from netaudio.dante import flows
from netaudio.dante.channel_status_paging import advertised_arc_protocol_identifier_for_device
from netaudio.dante.const import PROTOCOL_ARC_2809


async def create_multicast_flow_2809(device, channels: list[int], request_options_word: int = 0) -> dict:
    """Allocate an ARC 2.8.9 multicast flow once, then verify fresh device inventory."""
    channels = flows.validate_flow_channels(channels)
    if getattr(device, "requires_managed_control", False):
        raise flows.FlowValidationError("multicast allocation is not verified over managed control", status=409)
    if advertised_arc_protocol_identifier_for_device(device) != PROTOCOL_ARC_2809:
        raise flows.FlowValidationError("multicast allocation requires advertised ARC 2.8.9", status=409)
    if type(request_options_word) is not int or request_options_word not in (0, 0x0001, 0x0071):
        raise flows.FlowValidationError("request_options_word must be 0, 1 or 113")
    flows.require_available_tx_channels(channels, {int(number) for number in (device.tx_channels or {})})
    specification = {
        "command": "create_multicast_flow_2809",
        "channels": channels,
        "request_options_word": request_options_word,
    }
    # Validate the Rust encoder's size and channel constraints before any I/O.
    core.build_command(specification)

    async with device.topology_mutation_lock:
        before = await flows.query_tx_flow_inventory(
            str(device.ipv4), device._arc_port(), PROTOCOL_ARC_2809, device=device
        )
        if before is None:
            raise flows.FlowValidationError("could not read existing flows; no allocation was sent", status=504)
        if len(before["flows"]) >= before["max_flow_slots"]:
            raise flows.FlowValidationError("all transmitter flow slots are in use", status=409)
        if any(flow["media_type_code"] == 3 and flow["media_local_flow_id"] == 2 for flow in before["flows"]):
            raise flows.FlowValidationError("audio media-local flow 2 is already in use", status=409)

        # Allocation is not a safe retry: a lost acknowledgment leaves the outcome uncertain.
        response = await device.call_core(lambda client: client.execute(specification), request_attempts=1)
        if response is None:
            raise flows.FlowValidationError(
                "allocation outcome is unknown; no retry was sent; inspect flows", status=504
            )
        result_code = core.parse_response("result_code", response)
        if result_code != 1:
            raise flows.FlowValidationError(
                f"device rejected multicast allocation with result 0x{result_code:04X}", status=409
            )
        try:
            allocation = core.parse_response("multicast_flow_creation_2809", response)
        except core.NetaudioCoreError as exception:
            raise flows.FlowValidationError(
                "allocation acknowledgment is unsupported; inspect flows before retrying", status=502
            ) from exception
        if allocation["channels"] != channels:
            raise flows.FlowValidationError("allocation acknowledged different channels; inspect flows", status=502)

        after = await flows.query_tx_flow_inventory(
            str(device.ipv4), device._arc_port(), PROTOCOL_ARC_2809, device=device
        )
        if after is None:
            raise flows.FlowValidationError("allocation acknowledged but readback failed; inspect flows", status=502)
        previous_identifiers = {flow["global_flow_id"] for flow in before["flows"]}
        after_by_identifier = {flow["global_flow_id"]: flow for flow in after["flows"]}
        for previous in before["flows"]:
            current = after_by_identifier.get(previous["global_flow_id"])
            stable_fields = ("media_type_code", "media_local_flow_id", "flow_type", "populated_transmitter_channel_ids")
            if current is None or any(current.get(field) != previous.get(field) for field in stable_fields):
                raise flows.FlowValidationError(
                    "existing flow state changed during allocation; inspect flows", status=502
                )
        new_flows = [flow for flow in after["flows"] if flow["global_flow_id"] not in previous_identifiers]
        if len(new_flows) != 1:
            raise flows.FlowValidationError("allocation readback did not identify exactly one new flow", status=502)
        flow = new_flows[0]
        if (
            flow["global_flow_id"] != allocation["global_flow_id"]
            or flow["media_type_code"] != allocation["media_type_code"]
            or flow["media_local_flow_id"] != allocation["media_local_flow_id"]
            or flow.get("flow_type") != "multicast"
            or flow.get("populated_transmitter_channel_ids") != channels
        ):
            raise flows.FlowValidationError(
                "allocation readback does not match acknowledgment and requested channels", status=502
            )
        return {"flow_protocol_id": PROTOCOL_ARC_2809, "flow": flow, "verified": True}
