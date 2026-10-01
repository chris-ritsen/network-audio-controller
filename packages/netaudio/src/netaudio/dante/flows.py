from __future__ import annotations

import asyncio
import copy
import logging
import time
from collections.abc import Awaitable, Callable
from typing import cast

from netaudio import core
from netaudio.core import _requests
from netaudio.common.app_config import settings as app_settings
from netaudio.network_path import source_address_for
from netaudio.dante.arc_protocol import (
    ArcProtocolError,
    arc_protocol_for_device,
    advertised_arc_protocol_identifier_for_device,
    flow_inventory_protocol_identifier_for_device,
)
from netaudio.dante.channel import channel_by_number
from netaudio.dante.channel_capability import apply_channel_capability
from netaudio.dante.flow_preconditions import refresh_flow_state

logger = logging.getLogger("netaudio")


class FlowValidationError(ValueError):
    def __init__(self, message: str, *, status: int = 400):
        super().__init__(message)
        self.status = status


def external_receiver_subscription_specification(
    device,
    flow,
    receiver_channel_ids,
    flow_slot_assignments,
    *,
    receiver_supports_multiple_interfaces: bool,
) -> _requests.ExternalSubscriptionCommand:
    if not getattr(flow, "routable", False):
        reasons = "; ".join(getattr(flow, "routability_errors", ()) or ("flow is not routable",))
        raise FlowValidationError(f"external flow is not routable: {reasons}")

    if not isinstance(receiver_channel_ids, (list, tuple)) or not isinstance(flow_slot_assignments, (list, tuple)):
        raise FlowValidationError("receiver channels and flow-slot assignments must be lists")

    device_protocol = advertised_arc_protocol_identifier_for_device(device)

    if device_protocol is None:
        raise FlowValidationError("device has no ARC protocol metadata")

    if getattr(device, "requires_managed_control", False):
        raise FlowValidationError(
            "direct external RTP subscription is unavailable for managed-only devices", status=409
        )

    try:
        specification = core.plan_external_subscription(
            {
                "device_protocol": device_protocol,
                "receiver": {
                    "locked": getattr(device, "is_locked", None),
                    "aes67_supported": getattr(device, "aes67_configuration_supported", None),
                    "aes67_enabled": getattr(device, "aes67_current", None),
                    "sample_rate": getattr(device, "sample_rate", None),
                    "encoding": getattr(device, "encoding", None),
                    "redundancy_supported": getattr(device, "switch_redundancy_supported", None),
                },
                "source_sample_rate": flow.sample_rate,
                "source_encoding": flow.encoding,
                "source_direction": flow.direction,
                "receiver_channel_ids": list(receiver_channel_ids),
                "flow_slot_assignments": list(flow_slot_assignments),
                "advertised_flow_slot_count": flow.channel_count,
                "source_address": flow.source_ipv4,
                "session_id": flow.session_id,
                "clock_offset": flow.clock_offset,
                "primary_destination": {
                    "address": flow.primary_destination_address,
                    "port": flow.primary_destination_port,
                },
                "secondary_address": flow.secondary_destination_address,
                "secondary_port": flow.secondary_destination_port,
                "receiver_supports_multiple_interfaces": receiver_supports_multiple_interfaces,
                "message_id": core.next_message_id(),
            }
        )
    except core.NetaudioCoreError as error:
        raise FlowValidationError(str(error)) from error

    receiver_channels = getattr(device, "rx_channels", None)

    if not isinstance(receiver_channels, dict):
        raise FlowValidationError("receiver channel inventory is unavailable", status=409)

    try:
        unavailable = [
            number for number in receiver_channel_ids if channel_by_number(receiver_channels.values(), number) is None
        ]
    except RuntimeError as error:
        raise FlowValidationError(str(error), status=409) from error

    if unavailable:
        raise FlowValidationError(f"receiver channel not found: {', '.join(map(str, unavailable))}", status=404)

    return specification


async def subscribe_external_rtp(
    application,
    device,
    flow,
    receiver_channel_ids,
    flow_slot_assignments,
    *,
    receiver_supports_multiple_interfaces: bool,
) -> dict:
    specification = external_receiver_subscription_specification(
        device,
        flow,
        receiver_channel_ids,
        flow_slot_assignments,
        receiver_supports_multiple_interfaces=receiver_supports_multiple_interfaces,
    )
    pending = core.external_subscription_readback(
        {"kind": "command", "specification": specification, "inventory": None}
    )

    async with device.topology_mutation_lock:
        before = await query_preferred_receiver_flow_inventory(device)

        if before is None:
            return {
                "result_code": None,
                "request_acknowledged": False,
                "arc_effective_state_confirmed": None,
                "sdp_correlation_confirmed": None,
                "rtp_packet_reception_confirmed": None,
                "clock_lock_confirmed": None,
                "persistence_confirmed": None,
                "decoded_audio_confirmed": None,
                "mutation_sent": False,
                "message": "complete fresh receiver-flow baseline was unavailable; no request was sent",
                "requested_effective_identities": pending["requested_effective_identities"],
                "receiver_flow_before": None,
                "receiver_flow_after": None,
            }

        current = application.external_flows.get(flow.source_ipv4, flow.session_id)
        if (
            current is None
            or current.content_sha256 != flow.content_sha256
            or current.expires_monotonic <= time.monotonic()
        ):
            raise FlowValidationError("source announcement expired or changed; refresh the source", status=409)

        reason = await refresh_flow_state(device, rtp=True)
        if reason is not None:
            raise FlowValidationError(f"{reason}; no request was sent", status=409)

        current = application.external_flows.get(flow.source_ipv4, flow.session_id)
        if (
            current is None
            or current.content_sha256 != flow.content_sha256
            or current.expires_monotonic <= time.monotonic()
        ):
            raise FlowValidationError("source announcement expired or changed; refresh the source", status=409)

        specification = external_receiver_subscription_specification(
            device,
            current,
            receiver_channel_ids,
            flow_slot_assignments,
            receiver_supports_multiple_interfaces=receiver_supports_multiple_interfaces,
        )
        response = await device.execute(specification)
        acknowledgement = core.command_acknowledgement(response)
        result_code = acknowledgement.get("result_code") if acknowledgement is not None else None
        acknowledged = acknowledgement is not None and acknowledgement.get("accepted") is True
        after = None
        if acknowledged:
            deadline = asyncio.get_running_loop().time() + 2.0
            while True:
                after = await query_preferred_receiver_flow_inventory(device)
                evidence = core.external_subscription_readback(
                    {"kind": "command", "specification": specification, "inventory": after}
                )
                if evidence["arc_effective_state_confirmed"] is True or asyncio.get_running_loop().time() >= deadline:
                    break
                await asyncio.sleep(0.1)

    readback = core.external_subscription_readback(
        {"kind": "command", "specification": specification, "inventory": after}
    )
    arc_confirmed = readback["arc_effective_state_confirmed"]

    if after is not None:
        apply_page = getattr(device, "apply_receiver_flow_status_page", None)

        if apply_page is not None:
            apply_page(after)

    return {
        "result_code": result_code,
        "request_acknowledged": acknowledged,
        **readback,
        "rtp_packet_reception_confirmed": None,
        "clock_lock_confirmed": None,
        "persistence_confirmed": None,
        "decoded_audio_confirmed": None,
        "mutation_sent": True,
        "message": (
            "external receiver subscription confirmed by complete fresh ARC readback"
            if arc_confirmed is True
            else (
                "request acknowledged but complete fresh ARC readback was unavailable"
                if acknowledged and arc_confirmed is None
                else (
                    "request acknowledged but fresh ARC readback contradicted the requested identity"
                    if acknowledged
                    else "external receiver subscription was not acknowledged"
                )
            )
        ),
        "flow_identity": {"source_ipv4": flow.source_ipv4, "session_id": flow.session_id},
        "receiver_channel_ids": list(receiver_channel_ids),
        "flow_slot_assignments": list(flow_slot_assignments),
        "receiver_flow_before": before,
        "receiver_flow_after": after,
    }


def validate_flow_identifier(flow_id) -> int:
    if isinstance(flow_id, bool) or not isinstance(flow_id, int) or flow_id < 1:
        raise FlowValidationError("flow identifier must be a positive integer")

    return flow_id


def validate_flow_channels(channel_numbers) -> list[int]:
    if not isinstance(channel_numbers, list) or not channel_numbers:
        raise FlowValidationError("channels must be a non-empty list")
    if any(isinstance(number, bool) or not isinstance(number, int) or number < 1 for number in channel_numbers):
        raise FlowValidationError("channels must contain positive integers")
    if len(set(channel_numbers)) != len(channel_numbers):
        raise FlowValidationError("channels must not contain duplicates")
    return channel_numbers


def _parsed_response(kind: str, response: bytes):

    try:
        return core.parse_response(kind, response)
    except core.NetaudioCoreError as exception:
        logger.debug(f"Discarding unparseable {kind} response: {exception}")
        return None


async def _request(
    device_ip: str,
    arc_port: int,
    command_specification: dict,
    timeout_ms: int,
    attempts: int,
    device=None,
) -> bytes | None:
    if device is not None:
        return await device.execute(command_specification)

    def _send():
        local_ip = source_address_for(device_ip, app_settings.interface)
        client = core.CoreClient(
            device_ip, arc_port=arc_port, timeout_ms=timeout_ms, attempts=attempts, local_ip=local_ip
        )
        try:
            packet = core.build_command(command_specification)
            return client.request(packet, arc_port)
        finally:
            client.close()

    try:
        return await asyncio.to_thread(_send)
    except core.NetaudioCoreError:
        return None


async def detect_flow_protocol(device_ip: str, arc_port: int, *, device) -> int | None:
    try:
        protocol = arc_protocol_for_device(device)
    except ArcProtocolError:
        return None

    if protocol is None:
        return None

    for flow_protocol_id in protocol["flow_query_protocol_ids"]:
        try:
            with core.TransmitFlowInventory(flow_protocol_id) as inventory:
                command = inventory.state()["next_command"]
                assert command is not None, "new native inventory must request its first page"

                response = await _request(
                    device_ip,
                    arc_port,
                    command,
                    timeout_ms=500,
                    attempts=1,
                    device=device,
                )

                if not response:
                    continue

                inventory.accept(response)
        except core.NetaudioCoreError:
            continue

        return flow_protocol_id

    return None


async def query_tx_flow_inventory(device_ip: str, arc_port: int, flow_protocol_id: int, *, device=None) -> dict | None:

    try:
        with core.TransmitFlowInventory(flow_protocol_id) as inventory:
            state = inventory.state()

            while state["next_command"] is not None:
                response = await _request(
                    device_ip,
                    arc_port,
                    state["next_command"],
                    timeout_ms=1000,
                    attempts=2,
                    device=device,
                )

                if not response:
                    return None

                inventory.accept(response)
                state = inventory.state()

            return state["inventory"]
    except core.NetaudioCoreError as error:
        logger.debug("Transmitter flow inventory unavailable: %s", error)

        return None


async def query_preferred_tx_flow_inventory(
    device_ip: str,
    arc_port: int,
    mutation_protocol_id: int,
    *,
    device,
) -> dict | None:
    try:
        protocol = arc_protocol_for_device(device)

        if protocol is None:
            return None

        candidates = core.transmit_flow_inventory_protocols(protocol["protocol_id"], mutation_protocol_id)
    except (ArcProtocolError, core.NetaudioCoreError, ValueError):
        return None

    for protocol_id in candidates:
        inventory = await query_tx_flow_inventory(device_ip, arc_port, protocol_id, device=device)

        if inventory is not None:
            return {**inventory, "flow_protocol_id": protocol_id}

    return None


def correlate_receiver_flow_inventory(inventory: dict, sap_inventory) -> dict:
    correlated = copy.deepcopy(inventory)
    for flow in correlated.get("flows") or []:
        if not isinstance(flow, dict):
            continue
        identity = flow.get("external_identity")
        source_ipv4 = identity.get("source_ipv4") if isinstance(identity, dict) else None
        session_id = identity.get("session_id") if isinstance(identity, dict) else None
        if not isinstance(source_ipv4, str) or isinstance(session_id, bool) or not isinstance(session_id, int):
            continue
        advertised = None
        if sap_inventory is not None:
            try:
                advertised = sap_inventory.get(source_ipv4, session_id)
            except ValueError:
                advertised = None
        flow["sdp_correlation"] = {
            "matched": advertised is not None,
            "source_ipv4": source_ipv4,
            "session_id": session_id,
            "advertisement": advertised.to_dict() if advertised is not None else None,
        }
        for effective_identity in flow.get("effective_subscription_identities") or ():
            if isinstance(effective_identity, dict):
                effective_identity["sdp_correlation_confirmed"] = advertised is not None
    return correlated


def effective_external_subscription_index(inventory: dict) -> dict[int, list[dict]]:
    index: dict[int, list[dict]] = {}
    for flow in inventory.get("flows") or []:
        if not isinstance(flow, dict):
            continue
        for identity in flow.get("effective_subscription_identities") or []:
            receiver_channel = identity.get("receiver_channel") if isinstance(identity, dict) else None
            if isinstance(receiver_channel, int) and not isinstance(receiver_channel, bool):
                index.setdefault(receiver_channel, []).append(copy.deepcopy(identity))
    return index


def receiver_flow_query_family(device) -> str | None:
    capability_word = getattr(device, "transmit_flow_authoring_capability_word", None)
    if not isinstance(capability_word, bool) and isinstance(capability_word, int):
        return core.flow_authoring_capabilities(capability_word)["receiver_flow_query_family"]
    if not getattr(device, "requires_managed_control", False):
        return None
    try:
        protocol_id = flow_inventory_protocol_identifier_for_device(device)
        if protocol_id is None:
            return None
        core.ReceiverFlowInventory(protocol_id, family="segmented").close()
    except (ArcProtocolError, core.NetaudioCoreError):
        return None
    return "segmented"


def receiver_flow_inventory_family(device) -> str | None:
    cached = getattr(device, "receiver_flow_inventory_family", None)
    if cached in ("legacy", "modern"):
        return cached
    family = receiver_flow_query_family(device)
    if family is None:
        return None
    return {"fixed": "legacy", "segmented": "modern"}.get(family)


async def _read_channel_capability(device) -> None:
    if not callable(getattr(device, "execute", None)) or getattr(device, "requires_managed_control", False):
        return
    try:
        response = await device.execute({"command": "channel_count"})
        counts = core.parse_response("channel_count", response) if response else None
    except (OSError, RuntimeError, TimeoutError, core.NetaudioCoreError) as exception:
        logger.debug(f"{getattr(device, 'name', device)}: channel capability read failed: {exception!r}")
        return
    if not isinstance(counts, dict):
        return
    capability_word = counts["transmit_flow_authoring_capability_word"]
    device.transmit_flow_authoring_capability_word = capability_word
    for name, value in core.flow_authoring_capabilities(capability_word).items():
        setattr(device, name, value)
    apply_channel_capability(device, counts)


async def query_preferred_receiver_flow_inventory(device) -> dict | None:
    application = device.application
    inventory_family = receiver_flow_inventory_family(device)

    if inventory_family is None:
        await _read_channel_capability(device)
        inventory_family = receiver_flow_inventory_family(device)

    if inventory_family == "modern":
        modern_query = getattr(application, "query_modern_arc_receiver_flow_status", None)

        if not callable(modern_query):
            return None

        try:
            query = cast("Callable[[object], Awaitable[dict | None]]", modern_query)
            status_page = await query(device)
        except RuntimeError:
            return None

        if status_page is None:
            return None

        apply_page = getattr(device, "apply_receiver_flow_status_page", None)

        if apply_page is not None:
            apply_page(status_page)

        if status_page.get("page_disposition") != "complete":
            return None

        device.receiver_flow_inventory_family = "modern"

        return correlate_receiver_flow_inventory(
            status_page,
            getattr(application, "external_flows", None),
        )

    if inventory_family != "legacy" or getattr(device, "requires_managed_control", False):
        return None

    inventory = await query_receiver_flow_inventory(
        str(device.ipv4),
        device._arc_port(),
        protocol_id=flow_inventory_protocol_identifier_for_device(device),
        device=device,
        family="fixed",
    )

    if inventory is None:
        return None

    device.receiver_flow_inventory_family = "legacy"

    return correlate_receiver_flow_inventory(inventory, getattr(application, "external_flows", None))


async def query_receiver_flow_inventory(
    device_ip: str, arc_port: int, *, protocol_id: int | None, device=None, family: str | None = None
) -> dict | None:
    if protocol_id is None:
        return None

    try:
        with core.ReceiverFlowInventory(protocol_id, family=family) as inventory:
            state = inventory.state()

            while state["next_command"] is not None:
                response = await _request(
                    device_ip,
                    arc_port,
                    state["next_command"],
                    timeout_ms=1000,
                    attempts=2,
                    device=device,
                )

                if not response:
                    return None

                inventory.accept(response)
                state = inventory.state()

                partial = state.get("partial_inventory")
                if device is not None and isinstance(partial, dict):
                    device.receiver_flow_partial_inventory = partial
                    device.receiver_flow_completeness = "partial"

            return state["inventory"]
    except core.NetaudioCoreError as error:
        logger.debug("Receiver flow inventory failed: %s", error)
        return None


async def query_receiver_port_ranges(device_ip: str, arc_port: int, *, device=None) -> dict | None:
    response = await _request(
        device_ip,
        arc_port,
        {"command": "query_receiver_port_ranges"},
        timeout_ms=1000,
        attempts=2,
        device=device,
    )

    if not response:
        return None

    port_ranges = _parsed_response("receiver_port_ranges", response)
    return port_ranges if isinstance(port_ranges, dict) else None


async def query_transmit_channel_capabilities(
    device_ip: str,
    arc_port: int,
    starting_channel_identifier: int = 1,
    maximum_channel_count: int = 0,
    *,
    device=None,
) -> dict | None:
    response = await _request(
        device_ip,
        arc_port,
        {
            "command": "query_transmit_channel_capabilities",
            "starting_channel_identifier": starting_channel_identifier,
            "maximum_channel_count": maximum_channel_count,
        },
        timeout_ms=1000,
        attempts=2,
        device=device,
    )

    if not response:
        return None

    capabilities = _parsed_response("transmit_channel_capabilities", response)
    return capabilities if isinstance(capabilities, dict) else None
