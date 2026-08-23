from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Awaitable, Callable

from netaudio.dante import flows


A32_CHANNEL_CAPACITIES = {
    44_100: (64, 64),
    48_000: (64, 64),
    88_200: (32, 32),
    96_000: (32, 32),
    176_400: (16, 16),
    192_000: (16, 16),
}


class SampleRateTopologyError(RuntimeError):
    def __init__(self, message: str, preflight=None):
        super().__init__(message)
        self.preflight = preflight


class SampleRateTopologyUnsupportedError(SampleRateTopologyError):
    pass


class SampleRateTopologyConfirmationRequired(SampleRateTopologyError):
    pass


class SampleRateTopologyVerificationError(SampleRateTopologyError):
    pass


class SampleRateTopologyReadbackError(SampleRateTopologyError):
    pass


class SampleRateTopologyMutationOutcomeUnknownError(SampleRateTopologyError):
    pass


class SampleRateTopologyChangedButUnverifiedError(SampleRateTopologyError):
    def __init__(
        self,
        message: str,
        preflight,
        observed_sample_rate_hertz: int | None = None,
        resulting_snapshot=None,
    ):
        super().__init__(message, preflight)
        self.observed_sample_rate_hertz = observed_sample_rate_hertz
        self.resulting_snapshot = resulting_snapshot


@dataclass(frozen=True)
class SampleRateChannelCapacity:
    sample_rate_hertz: int
    receive_channel_count: int
    transmit_channel_count: int

    def to_dict(self) -> dict:
        return {
            "sample_rate_hertz": self.sample_rate_hertz,
            "receive_channel_count": self.receive_channel_count,
            "transmit_channel_count": self.transmit_channel_count,
        }


@dataclass(frozen=True)
class ReceiverSubscriptionState:
    receiver_channel_number: int
    receiver_channel_name: str
    transmitter_channel_name: str
    transmitter_device_name: str

    def to_dict(self) -> dict:
        return {
            "receiver_channel_number": self.receiver_channel_number,
            "receiver_channel_name": self.receiver_channel_name,
            "transmitter_channel_name": self.transmitter_channel_name,
            "transmitter_device_name": self.transmitter_device_name,
        }


@dataclass(frozen=True)
class TransmitterFlowState:
    flow_number: int
    flow_type: str
    channel_count: int
    channel_members: tuple[int, ...]
    sample_rate_hertz: int
    encoding: int
    frames_per_packet: int

    def to_dict(self) -> dict:
        return {
            "flow_number": self.flow_number,
            "flow_type": self.flow_type,
            "channel_count": self.channel_count,
            "channel_members": list(self.channel_members),
            "sample_rate_hertz": self.sample_rate_hertz,
            "encoding": self.encoding,
            "frames_per_packet": self.frames_per_packet,
        }


@dataclass(frozen=True)
class SampleRateTopologySnapshot:
    capacity: SampleRateChannelCapacity
    receiver_subscriptions: tuple[ReceiverSubscriptionState, ...]
    transmitter_flows: tuple[TransmitterFlowState, ...]
    flow_protocol_identifier: int

    def to_dict(self) -> dict:
        return {
            "capacity": self.capacity.to_dict(),
            "receiver_subscriptions": [state.to_dict() for state in self.receiver_subscriptions],
            "transmitter_flows": [state.to_dict() for state in self.transmitter_flows],
            "flow_protocol_identifier": self.flow_protocol_identifier,
        }


@dataclass(frozen=True)
class TransmitterFlowMembershipLoss:
    flow_number: int
    flow_type: str
    retained_channel_members: tuple[int, ...]
    removed_channel_members: tuple[int, ...]

    def to_dict(self) -> dict:
        return {
            "flow_number": self.flow_number,
            "flow_type": self.flow_type,
            "retained_channel_members": list(self.retained_channel_members),
            "removed_channel_members": list(self.removed_channel_members),
        }


@dataclass(frozen=True)
class UncharacterizedTransmitterFlow:
    flow_number: int
    flow_type: str
    channel_count: int
    reason: str

    def to_dict(self) -> dict:
        return {
            "flow_number": self.flow_number,
            "flow_type": self.flow_type,
            "channel_count": self.channel_count,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class SampleRateTopologyPreflight:
    device_name: str
    current_sample_rate_hertz: int
    target_sample_rate_hertz: int
    current_snapshot: SampleRateTopologySnapshot | None
    target_capacity: SampleRateChannelCapacity | None
    reversible_receiver_clipping: tuple[ReceiverSubscriptionState, ...]
    destructive_transmitter_membership_loss: tuple[TransmitterFlowMembershipLoss, ...]
    uncharacterized_transmitter_flows: tuple[UncharacterizedTransmitterFlow, ...]

    @property
    def requires_destructive_confirmation(self) -> bool:
        return bool(self.destructive_transmitter_membership_loss)

    @property
    def is_classified(self) -> bool:
        return not self.uncharacterized_transmitter_flows

    @property
    def topology_characterized(self) -> bool:
        return self.current_snapshot is not None and self.target_capacity is not None

    def to_dict(self) -> dict:
        return {
            "device_name": self.device_name,
            "current_sample_rate_hertz": self.current_sample_rate_hertz,
            "target_sample_rate_hertz": self.target_sample_rate_hertz,
            "current_topology": self.current_snapshot.to_dict() if self.current_snapshot is not None else None,
            "target_capacity": self.target_capacity.to_dict() if self.target_capacity is not None else None,
            "reversible_receiver_clipping": [state.to_dict() for state in self.reversible_receiver_clipping],
            "destructive_transmitter_membership_loss": [
                state.to_dict() for state in self.destructive_transmitter_membership_loss
            ],
            "uncharacterized_transmitter_flows": [state.to_dict() for state in self.uncharacterized_transmitter_flows],
            "requires_destructive_confirmation": self.requires_destructive_confirmation,
            "is_classified": self.is_classified,
            "topology_characterized": self.topology_characterized,
        }


@dataclass(frozen=True)
class SampleRateTopologyChangeResult:
    changed: bool
    preflight: SampleRateTopologyPreflight
    observed_sample_rate_hertz: int
    observed_supported_sample_rates_hertz: tuple[int, ...]
    resulting_snapshot: SampleRateTopologySnapshot | None

    def to_dict(self) -> dict:
        return {
            "success": True,
            "changed": self.changed,
            "preflight": self.preflight.to_dict(),
            "readback": {
                "sample_rate_hertz": self.observed_sample_rate_hertz,
                "supported_sample_rates_hertz": list(self.observed_supported_sample_rates_hertz),
                "topology": self.resulting_snapshot.to_dict() if self.resulting_snapshot is not None else None,
            },
        }


def _positive_integer(value, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise SampleRateTopologyUnsupportedError(f"{field_name} must be a positive integer")
    return value


def _nonnegative_integer(value, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise SampleRateTopologyUnsupportedError(f"{field_name} must be a nonnegative integer")
    return value


def _device_label(device) -> str:
    return device.name or device.server_name or str(device.ipv4)


def _is_ferrofish_a32(device) -> bool:
    model_values = (
        getattr(device, "dante_model", None),
        getattr(device, "model", None),
        getattr(device, "board_name", None),
    )
    return any(
        isinstance(value, str) and value.casefold().startswith("a32 dante ad/da converter") for value in model_values
    )


def _reported_capacity_table(device) -> dict[int, SampleRateChannelCapacity]:
    capacities = getattr(device, "sample_rate_channel_capacities", None)
    if capacities is None:
        return {}
    if not isinstance(capacities, list):
        raise SampleRateTopologyUnsupportedError("device sample-rate channel capacities are malformed")
    table = {}
    for entry in capacities:
        if not isinstance(entry, dict):
            raise SampleRateTopologyUnsupportedError("device sample-rate channel capacities are malformed")
        sample_rate = _positive_integer(entry.get("sample_rate_hertz"), "sample_rate_hertz")
        receive_count = _nonnegative_integer(entry.get("receive_channel_count"), "receive_channel_count")
        transmit_count = _nonnegative_integer(entry.get("transmit_channel_count"), "transmit_channel_count")
        capacity = SampleRateChannelCapacity(sample_rate, receive_count, transmit_count)
        if sample_rate in table and table[sample_rate] != capacity:
            raise SampleRateTopologyUnsupportedError(
                f"device reports conflicting channel capacities for {sample_rate} Hz"
            )
        table[sample_rate] = capacity
    return table


def _capacity_for_rate(device, sample_rate_hertz: int) -> SampleRateChannelCapacity:
    if not _is_ferrofish_a32(device):
        raise SampleRateTopologyUnsupportedError(
            "topology-safe sample-rate changes are currently proven only for Ferrofish A32 devices"
        )
    reported = _reported_capacity_table(device).get(sample_rate_hertz)
    if reported is not None:
        return reported
    counts = A32_CHANNEL_CAPACITIES.get(sample_rate_hertz)
    if counts is None:
        raise SampleRateTopologyUnsupportedError(
            f"no proven Ferrofish A32 channel capacity is available for {sample_rate_hertz} Hz"
        )
    receive_count, transmit_count = counts
    return SampleRateChannelCapacity(sample_rate_hertz, receive_count, transmit_count)


def _validated_sample_rate_status(status) -> tuple[int, tuple[int, ...]]:
    if status is None:
        raise SampleRateTopologyReadbackError("sample-rate readback was unavailable")
    if not isinstance(status, tuple) or len(status) != 2:
        raise SampleRateTopologyVerificationError("sample-rate readback was unavailable")
    current_sample_rate, supported_sample_rates = status
    current_sample_rate = _positive_integer(current_sample_rate, "current sample rate")
    if not isinstance(supported_sample_rates, list) or not supported_sample_rates:
        raise SampleRateTopologyVerificationError("supported sample-rate readback was unavailable")
    normalized_supported = tuple(_positive_integer(value, "supported sample rate") for value in supported_sample_rates)
    if len(set(normalized_supported)) != len(normalized_supported):
        raise SampleRateTopologyVerificationError("supported sample-rate readback contains duplicates")
    if current_sample_rate not in normalized_supported:
        raise SampleRateTopologyVerificationError(
            "current sample rate is absent from the device's supported sample-rate list"
        )
    return current_sample_rate, normalized_supported


def _receiver_subscription_states(device) -> tuple[ReceiverSubscriptionState, ...]:
    states = []
    for subscription in device.subscriptions:
        if not subscription.has_configured_source:
            continue
        receiver_channel = subscription.rx_channel
        if receiver_channel is None or not isinstance(receiver_channel.number, int):
            raise SampleRateTopologyVerificationError(
                "fresh receiver inventory did not preserve subscription channel identity"
            )
        states.append(
            ReceiverSubscriptionState(
                receiver_channel_number=receiver_channel.number,
                receiver_channel_name=subscription.rx_channel_name or "",
                transmitter_channel_name=subscription.tx_channel_name or "",
                transmitter_device_name=subscription.tx_device_name or "",
            )
        )
    return tuple(sorted(states, key=lambda state: state.receiver_channel_number))


def _transmitter_flow_states(inventory: dict) -> tuple[TransmitterFlowState, ...]:
    raw_flows = inventory.get("flows")
    if not isinstance(raw_flows, list):
        raise SampleRateTopologyVerificationError("fresh transmitter-flow inventory is malformed")
    states = []
    for flow in raw_flows:
        if not isinstance(flow, dict):
            raise SampleRateTopologyVerificationError("fresh transmitter-flow inventory is malformed")
        flow_number = _positive_integer(flow.get("flow_number"), "transmitter flow number")
        flow_type = flow.get("flow_type")
        channel_count = _positive_integer(flow.get("channel_count"), "transmitter flow channel count")
        channel_members = flow.get("channels")
        sample_rate_hertz = _positive_integer(flow.get("sample_rate"), "transmitter flow sample rate")
        encoding = _positive_integer(flow.get("encoding"), "transmitter flow encoding")
        frames_per_packet = _positive_integer(
            flow.get("frames_per_packet"),
            "transmitter flow frames per packet",
        )
        if not isinstance(flow_type, str) or flow_type not in ("multicast", "unicast"):
            raise SampleRateTopologyVerificationError("fresh transmitter-flow type is uncharacterized")
        if not isinstance(channel_members, list) or any(
            isinstance(member, bool) or not isinstance(member, int) or member < 0 for member in channel_members
        ):
            raise SampleRateTopologyVerificationError("fresh transmitter-flow members are malformed")
        if flow_type == "multicast" and len(channel_members) != channel_count:
            raise SampleRateTopologyVerificationError(
                f"multicast flow {flow_number} member count does not match its channel count"
            )
        if flow_type == "unicast" and channel_members:
            raise SampleRateTopologyVerificationError(
                f"unicast flow {flow_number} unexpectedly contains decoded channel members"
            )
        states.append(
            TransmitterFlowState(
                flow_number=flow_number,
                flow_type=flow_type,
                channel_count=channel_count,
                channel_members=tuple(channel_members),
                sample_rate_hertz=sample_rate_hertz,
                encoding=encoding,
                frames_per_packet=frames_per_packet,
            )
        )
    return tuple(sorted(states, key=lambda state: state.flow_number))


async def capture_sample_rate_topology(
    device,
    capacity: SampleRateChannelCapacity,
) -> SampleRateTopologySnapshot:
    try:
        await device.get_rx_channels()
    except Exception as exception:
        raise SampleRateTopologyReadbackError(f"fresh receiver inventory failed: {exception}") from exception
    receiver_channel_numbers = set(device.rx_channels)
    expected_receiver_channel_numbers = set(range(1, capacity.receive_channel_count + 1))
    if receiver_channel_numbers != expected_receiver_channel_numbers:
        raise SampleRateTopologyVerificationError(
            "fresh receiver inventory does not match the proven active channel capacity"
        )
    flow_protocol_identifier = await flows.detect_flow_protocol(str(device.ipv4), device._arc_port())
    if flow_protocol_identifier is None:
        raise SampleRateTopologyReadbackError("transmitter-flow protocol did not respond")
    flow_inventory = await flows.query_tx_flow_inventory(
        str(device.ipv4),
        device._arc_port(),
        flow_protocol_identifier,
    )
    if flow_inventory is None:
        raise SampleRateTopologyReadbackError("fresh transmitter-flow inventory did not respond")
    transmitter_flows = _transmitter_flow_states(flow_inventory)
    mismatched_flow_rates = [
        state.flow_number for state in transmitter_flows if state.sample_rate_hertz != capacity.sample_rate_hertz
    ]
    if mismatched_flow_rates:
        flow_labels = ", ".join(str(flow_number) for flow_number in mismatched_flow_rates)
        raise SampleRateTopologyVerificationError(
            f"fresh transmitter flows report a different sample rate: {flow_labels}"
        )
    return SampleRateTopologySnapshot(
        capacity=capacity,
        receiver_subscriptions=_receiver_subscription_states(device),
        transmitter_flows=transmitter_flows,
        flow_protocol_identifier=flow_protocol_identifier,
    )


def _classify_transmitter_flows(
    snapshot: SampleRateTopologySnapshot,
    target_capacity: SampleRateChannelCapacity,
) -> tuple[tuple[TransmitterFlowMembershipLoss, ...], tuple[UncharacterizedTransmitterFlow, ...]]:
    if target_capacity.transmit_channel_count >= snapshot.capacity.transmit_channel_count:
        return (), ()
    destructive = []
    uncharacterized = []
    for flow in snapshot.transmitter_flows:
        if flow.flow_type == "unicast":
            uncharacterized.append(
                UncharacterizedTransmitterFlow(
                    flow_number=flow.flow_number,
                    flow_type=flow.flow_type,
                    channel_count=flow.channel_count,
                    reason="the proven unicast inventory does not expose transmitter channel members",
                )
            )
            continue
        retained_members = tuple(
            member for member in flow.channel_members if 1 <= member <= target_capacity.transmit_channel_count
        )
        removed_members = tuple(
            member for member in flow.channel_members if member > target_capacity.transmit_channel_count
        )
        if not removed_members:
            continue
        if not retained_members:
            uncharacterized.append(
                UncharacterizedTransmitterFlow(
                    flow_number=flow.flow_number,
                    flow_type=flow.flow_type,
                    channel_count=flow.channel_count,
                    reason="all active members fall outside the target capacity and that transition is unproven",
                )
            )
            continue
        destructive.append(
            TransmitterFlowMembershipLoss(
                flow_number=flow.flow_number,
                flow_type=flow.flow_type,
                retained_channel_members=retained_members,
                removed_channel_members=removed_members,
            )
        )
    return tuple(destructive), tuple(uncharacterized)


async def preflight_sample_rate_change(
    device,
    target_sample_rate_hertz: int,
    probe_sample_rate_status: Callable[[], Awaitable[tuple[int, list[int]] | None]],
) -> SampleRateTopologyPreflight:
    target_sample_rate_hertz = _positive_integer(target_sample_rate_hertz, "target sample rate")
    status = _validated_sample_rate_status(await probe_sample_rate_status())
    current_sample_rate_hertz, supported_sample_rates_hertz = status
    device.sample_rate = current_sample_rate_hertz
    device.supported_sample_rates = list(supported_sample_rates_hertz)
    if target_sample_rate_hertz not in supported_sample_rates_hertz:
        raise SampleRateTopologyUnsupportedError(
            f"requested sample rate {target_sample_rate_hertz} is not supported; "
            f"device reports {list(supported_sample_rates_hertz)}"
        )
    if target_sample_rate_hertz == current_sample_rate_hertz:
        return SampleRateTopologyPreflight(
            device_name=_device_label(device),
            current_sample_rate_hertz=current_sample_rate_hertz,
            target_sample_rate_hertz=target_sample_rate_hertz,
            current_snapshot=None,
            target_capacity=None,
            reversible_receiver_clipping=(),
            destructive_transmitter_membership_loss=(),
            uncharacterized_transmitter_flows=(),
        )
    current_capacity = _capacity_for_rate(device, current_sample_rate_hertz)
    target_capacity = _capacity_for_rate(device, target_sample_rate_hertz)
    current_snapshot = await capture_sample_rate_topology(device, current_capacity)
    reversible_receiver_clipping = tuple(
        state
        for state in current_snapshot.receiver_subscriptions
        if state.receiver_channel_number > target_capacity.receive_channel_count
    )
    destructive, uncharacterized = _classify_transmitter_flows(current_snapshot, target_capacity)
    return SampleRateTopologyPreflight(
        device_name=_device_label(device),
        current_sample_rate_hertz=current_sample_rate_hertz,
        target_sample_rate_hertz=target_sample_rate_hertz,
        current_snapshot=current_snapshot,
        target_capacity=target_capacity,
        reversible_receiver_clipping=reversible_receiver_clipping,
        destructive_transmitter_membership_loss=destructive,
        uncharacterized_transmitter_flows=uncharacterized,
    )


def _verify_resulting_topology(
    preflight: SampleRateTopologyPreflight,
    resulting_snapshot: SampleRateTopologySnapshot,
) -> None:
    if preflight.current_snapshot is None or preflight.target_capacity is None:
        raise SampleRateTopologyVerificationError("sample-rate topology verification lacks a characterized preflight")
    if resulting_snapshot.flow_protocol_identifier != preflight.current_snapshot.flow_protocol_identifier:
        raise SampleRateTopologyVerificationError(
            "transmitter-flow protocol changed during the sample-rate operation",
            preflight,
        )
    expected_subscriptions = {
        state.receiver_channel_number: state
        for state in preflight.current_snapshot.receiver_subscriptions
        if state.receiver_channel_number <= preflight.target_capacity.receive_channel_count
    }
    resulting_subscriptions = {
        state.receiver_channel_number: state for state in resulting_snapshot.receiver_subscriptions
    }
    if resulting_subscriptions != expected_subscriptions:
        raise SampleRateTopologyVerificationError(
            "receiver subscriptions did not reach the exact expected in-capacity state",
            preflight,
        )
    expected_flows = {
        state.flow_number: replace(
            state,
            channel_members=tuple(
                member if member == 0 or member <= preflight.target_capacity.transmit_channel_count else 0
                for member in state.channel_members
            ),
            sample_rate_hertz=preflight.target_sample_rate_hertz,
        )
        for state in preflight.current_snapshot.transmitter_flows
    }
    resulting_flows = {state.flow_number: state for state in resulting_snapshot.transmitter_flows}
    if resulting_flows != expected_flows:
        raise SampleRateTopologyVerificationError(
            "transmitter flows did not reach the exact expected membership and metadata state",
            preflight,
        )


async def change_sample_rate_topology_safe(
    device,
    target_sample_rate_hertz: int,
    probe_sample_rate_status: Callable[[], Awaitable[tuple[int, list[int]] | None]],
    mutate: Callable[[], Awaitable[None]],
    confirm_destructive: bool = False,
) -> SampleRateTopologyChangeResult:
    if not isinstance(confirm_destructive, bool):
        raise ValueError("confirm_destructive must be a boolean")
    preflight = await preflight_sample_rate_change(
        device,
        target_sample_rate_hertz,
        probe_sample_rate_status,
    )
    if preflight.current_sample_rate_hertz == preflight.target_sample_rate_hertz:
        return SampleRateTopologyChangeResult(
            changed=False,
            preflight=preflight,
            observed_sample_rate_hertz=preflight.current_sample_rate_hertz,
            observed_supported_sample_rates_hertz=tuple(device.supported_sample_rates),
            resulting_snapshot=None,
        )
    if preflight.uncharacterized_transmitter_flows:
        raise SampleRateTopologyUnsupportedError(
            "sample-rate contraction affects transmitter flows whose membership transition is not proven",
            preflight,
        )
    if preflight.requires_destructive_confirmation and not confirm_destructive:
        raise SampleRateTopologyConfirmationRequired(
            "sample-rate change would permanently remove transmitter flow members; explicit confirmation is required",
            preflight,
        )
    try:
        await mutate()
    except Exception as exception:
        raise SampleRateTopologyMutationOutcomeUnknownError(
            f"sample-rate mutation failed after it was attempted; device state is unknown: {exception}",
            preflight,
        ) from exception
    observed_sample_rate_hertz = None
    resulting_snapshot = None
    try:
        observed_status = _validated_sample_rate_status(await probe_sample_rate_status())
        observed_sample_rate_hertz, observed_supported_sample_rates_hertz = observed_status
        device.sample_rate = observed_sample_rate_hertz
        device.supported_sample_rates = list(observed_supported_sample_rates_hertz)
        if observed_sample_rate_hertz != preflight.target_sample_rate_hertz:
            raise SampleRateTopologyVerificationError(
                f"device reports {observed_sample_rate_hertz} Hz instead of {preflight.target_sample_rate_hertz} Hz",
                preflight,
            )
        resulting_capacity = _capacity_for_rate(device, observed_sample_rate_hertz)
        resulting_snapshot = await capture_sample_rate_topology(device, resulting_capacity)
        _verify_resulting_topology(preflight, resulting_snapshot)
    except Exception as exception:
        raise SampleRateTopologyChangedButUnverifiedError(
            f"sample-rate change was sent, but complete post-write verification failed: {exception}",
            preflight,
            observed_sample_rate_hertz,
            resulting_snapshot,
        ) from exception
    return SampleRateTopologyChangeResult(
        changed=True,
        preflight=preflight,
        observed_sample_rate_hertz=observed_sample_rate_hertz,
        observed_supported_sample_rates_hertz=observed_supported_sample_rates_hertz,
        resulting_snapshot=resulting_snapshot,
    )


async def change_sample_rate_with_command_sender(
    sender,
    device,
    sample_rate_hertz: int,
    confirm_destructive: bool = False,
    timeout: float = 4.0,
) -> SampleRateTopologyChangeResult:
    device_ip_address = str(device.ipv4)

    async def probe():
        return await sender.probe_sample_rate_status(device_ip_address, timeout=timeout)

    async def mutate():
        packet, _, port = device.commands.command_set_sample_rate(sample_rate_hertz)
        await sender(packet, device_ip_address, port, expect_response=False)

    async with device.topology_mutation_lock:
        return await change_sample_rate_topology_safe(
            device,
            sample_rate_hertz,
            probe,
            mutate,
            confirm_destructive=confirm_destructive,
        )
