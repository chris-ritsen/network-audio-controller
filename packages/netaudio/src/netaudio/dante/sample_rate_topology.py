from __future__ import annotations

from dataclasses import dataclass
from typing import Awaitable, Callable

from netaudio import core
from netaudio.core import _requests
from netaudio.core.binding import NetaudioCoreError
from netaudio.dante import flows
from netaudio.dante.operation_availability import require_writable


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

    def to_dict(self) -> _requests.ChannelCapacity:
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

    def to_dict(self) -> _requests.ReceiverSubscription:
        return {
            "receiver_channel_number": self.receiver_channel_number,
            "receiver_channel_name": self.receiver_channel_name,
            "transmitter_channel_name": self.transmitter_channel_name,
            "transmitter_device_name": self.transmitter_device_name,
        }


@dataclass(frozen=True)
class TransmitterFlowState:
    flow_number: int
    flow_type: _requests.FlowType
    channel_count: int
    channel_members: tuple[int, ...]
    sample_rate_hertz: int
    encoding: int
    frames_per_packet: int | None
    may_retire_after_sample_rate_change: bool

    def to_dict(self) -> _requests.FlowTopology:
        return {
            "flow_number": self.flow_number,
            "flow_type": self.flow_type,
            "channel_count": self.channel_count,
            "channel_members": list(self.channel_members),
            "sample_rate_hertz": self.sample_rate_hertz,
            "encoding": self.encoding,
            "frames_per_packet": self.frames_per_packet,
            "may_retire_after_sample_rate_change": self.may_retire_after_sample_rate_change,
        }


@dataclass(frozen=True)
class SampleRateTopologySnapshot:
    capacity: SampleRateChannelCapacity
    receiver_subscriptions: tuple[ReceiverSubscriptionState, ...]
    transmitter_flows: tuple[TransmitterFlowState, ...]
    flow_protocol_identifier: int

    def to_dict(self) -> _requests.TopologySnapshot:
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
    requires_destructive_confirmation: bool

    @property
    def capacity_known(self) -> bool:
        return self.target_capacity is not None

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
            "capacity_known": self.capacity_known,
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


def _device_label(device) -> str:
    return device.name or device.server_name or str(device.ipv4)


def _capacity_for_rate(device, sample_rate_hertz: int) -> SampleRateChannelCapacity | None:
    try:
        capacity = core.sample_rate_capacity(getattr(device, "sample_rate_channel_capacities", None), sample_rate_hertz)
    except NetaudioCoreError as exception:
        raise SampleRateTopologyUnsupportedError(str(exception)) from exception

    return SampleRateChannelCapacity(**capacity) if capacity is not None else None


def _validated_sample_rate_status(status) -> tuple[int, tuple[int, ...]]:
    if status is None:
        raise SampleRateTopologyReadbackError("sample-rate readback was unavailable")

    try:
        evidence = core.sample_rate_status_evidence(status)
    except NetaudioCoreError as exception:
        raise SampleRateTopologyVerificationError(str(exception)) from exception

    return evidence["current_value"], tuple(evidence["available_values"])


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


def _transmitter_flow_states(inventory: dict, *, protocol_id: int) -> tuple[TransmitterFlowState, ...]:
    raw_flows = inventory.get("flows")

    if not isinstance(raw_flows, list):
        raise SampleRateTopologyVerificationError("fresh transmitter-flow inventory is malformed")

    states = []

    for flow in raw_flows:
        try:
            state = core.transmit_flow_topology(flow, protocol_id=protocol_id)
        except NetaudioCoreError as exception:
            raise SampleRateTopologyVerificationError(str(exception)) from exception

        states.append(
            TransmitterFlowState(
                flow_number=state["flow_number"],
                flow_type=state["flow_type"],
                channel_count=state["channel_count"],
                channel_members=tuple(state["channel_members"]),
                sample_rate_hertz=state["sample_rate_hertz"],
                encoding=state["encoding"],
                frames_per_packet=state["frames_per_packet"],
                may_retire_after_sample_rate_change=state["may_retire_after_sample_rate_change"],
            )
        )

    return tuple(sorted(states, key=lambda state: state.flow_number))


async def _fresh_channel_counts(device) -> tuple[int, int] | None:
    response = await device.execute({"command": "channel_count"})
    counts = core.parse_response("channel_count", response) if response else None
    if not isinstance(counts, dict):
        return None
    receive_count = counts.get("rx_count")
    transmit_count = counts.get("tx_count")
    if isinstance(receive_count, bool) or isinstance(transmit_count, bool):
        return None
    if not isinstance(receive_count, int) or not isinstance(transmit_count, int):
        return None
    return receive_count, transmit_count


async def _observed_capacity(device, sample_rate_hertz: int, managed: bool) -> SampleRateChannelCapacity:
    if managed:
        counts = await _fresh_channel_counts(device)
        if counts is None:
            raise SampleRateTopologyReadbackError("fresh channel counts were not reported")
        return SampleRateChannelCapacity(sample_rate_hertz, *counts)
    try:
        await device.get_rx_channels()
        await device.get_tx_channels()
    except (OSError, RuntimeError, TimeoutError, ValueError, NetaudioCoreError) as exception:
        raise SampleRateTopologyReadbackError(f"fresh channel inventory failed: {exception}") from exception
    return SampleRateChannelCapacity(sample_rate_hertz, len(device.rx_channels), len(device.tx_channels))


async def capture_sample_rate_topology(
    device,
    capacity: SampleRateChannelCapacity | None,
    sample_rate_hertz: int | None = None,
) -> SampleRateTopologySnapshot:
    managed = getattr(device, "requires_managed_control", False)
    if capacity is None:
        if sample_rate_hertz is None:
            raise ValueError("sample_rate_hertz is required when the channel capacity is unknown")
        capacity = await _observed_capacity(device, sample_rate_hertz, managed)
    elif managed:
        counts = await _fresh_channel_counts(device)
        if counts != (capacity.receive_channel_count, capacity.transmit_channel_count):
            raise SampleRateTopologyVerificationError(
                "fresh channel counts do not match the expected sample-rate capacity"
            )
    try:
        if managed and capacity.receive_channel_count == 0:
            device.rx_channels = {}
            device.subscriptions = []
        else:
            await device.get_rx_channels()
    except (OSError, RuntimeError, TimeoutError, ValueError, NetaudioCoreError) as exception:
        raise SampleRateTopologyReadbackError(f"fresh receiver inventory failed: {exception}") from exception
    receiver_channel_numbers = [channel.number for channel in device.rx_channels.values()]

    try:
        core.verify_sample_rate_receiver_inventory(receiver_channel_numbers, capacity.receive_channel_count)
    except NetaudioCoreError as exception:
        raise SampleRateTopologyVerificationError(str(exception)) from exception

    if managed and capacity.transmit_channel_count == 0:
        from netaudio.dante.arc_protocol import modern_arc_protocol_identifier_for_device

        return SampleRateTopologySnapshot(
            capacity=capacity,
            receiver_subscriptions=_receiver_subscription_states(device),
            transmitter_flows=(),
            flow_protocol_identifier=modern_arc_protocol_identifier_for_device(device),
        )
    managed_option = {"device": device} if managed else {}
    flow_protocol_identifier = await flows.detect_flow_protocol(str(device.ipv4), device._arc_port(), device=device)
    if flow_protocol_identifier is None:
        raise SampleRateTopologyReadbackError("transmitter-flow protocol did not respond")
    flow_inventory = await flows.query_tx_flow_inventory(
        str(device.ipv4),
        device._arc_port(),
        flow_protocol_identifier,
        **managed_option,
    )
    if flow_inventory is None:
        raise SampleRateTopologyReadbackError("fresh transmitter-flow inventory did not respond")
    transmitter_flows = _transmitter_flow_states(flow_inventory, protocol_id=flow_protocol_identifier)
    return SampleRateTopologySnapshot(
        capacity=capacity,
        receiver_subscriptions=_receiver_subscription_states(device),
        transmitter_flows=transmitter_flows,
        flow_protocol_identifier=flow_protocol_identifier,
    )


async def preflight_sample_rate_change(
    device,
    target_sample_rate_hertz: int,
    probe_sample_rate_status: Callable[[], Awaitable[dict | None]],
    load_channel_capacities: Callable[[], Awaitable[None]] | None = None,
) -> SampleRateTopologyPreflight:
    target_sample_rate_hertz = _positive_integer(target_sample_rate_hertz, "target sample rate")
    status = await probe_sample_rate_status()

    if status is None:
        raise SampleRateTopologyReadbackError("sample-rate readback was unavailable")

    current_sample_rate_hertz, supported_sample_rates_hertz = _validated_sample_rate_status(status)
    device.sample_rate = current_sample_rate_hertz
    device.supported_sample_rates = list(supported_sample_rates_hertz)
    device.sample_rate_update_mode = status.get("update_mode")
    restrictions = core.audio_capability_control(
        device.sample_rate_update_mode, list(supported_sample_rates_hertz), target_sample_rate_hertz
    )

    if "value_not_advertised" in restrictions:
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
            requires_destructive_confirmation=False,
        )

    if "fixed" in restrictions:
        raise SampleRateTopologyUnsupportedError("device reports a fixed sample-rate update mode")

    if "update_mode_unknown" in restrictions:
        raise SampleRateTopologyReadbackError("device did not report a known writable sample-rate update mode")

    if (
        load_channel_capacities is not None
        and getattr(device, "sample_rate_channel_capacities", None) is None
        and _capacity_for_rate(device, target_sample_rate_hertz) is None
    ):
        await load_channel_capacities()
    current_capacity = _capacity_for_rate(device, current_sample_rate_hertz)
    target_capacity = _capacity_for_rate(device, target_sample_rate_hertz)
    current_snapshot = await capture_sample_rate_topology(device, current_capacity, current_sample_rate_hertz)
    try:
        impact = core.sample_rate_topology_impact(
            current_snapshot.to_dict(), target_capacity.to_dict() if target_capacity is not None else None
        )
    except NetaudioCoreError as exception:
        raise SampleRateTopologyVerificationError(str(exception)) from exception

    reversible_receiver_clipping = tuple(
        ReceiverSubscriptionState(**state) for state in impact["reversible_receiver_clipping"]
    )
    destructive = tuple(
        TransmitterFlowMembershipLoss(
            **{
                **state,
                "retained_channel_members": tuple(state["retained_channel_members"]),
                "removed_channel_members": tuple(state["removed_channel_members"]),
            }
        )
        for state in impact["destructive_transmitter_membership_loss"]
    )
    uncharacterized = tuple(
        UncharacterizedTransmitterFlow(**state) for state in impact["uncharacterized_transmitter_flows"]
    )
    return SampleRateTopologyPreflight(
        device_name=_device_label(device),
        current_sample_rate_hertz=current_sample_rate_hertz,
        target_sample_rate_hertz=target_sample_rate_hertz,
        current_snapshot=current_snapshot,
        target_capacity=target_capacity,
        reversible_receiver_clipping=reversible_receiver_clipping,
        destructive_transmitter_membership_loss=destructive,
        uncharacterized_transmitter_flows=uncharacterized,
        requires_destructive_confirmation=impact["requires_destructive_confirmation"],
    )


def _verify_resulting_topology(
    preflight: SampleRateTopologyPreflight,
    resulting_snapshot: SampleRateTopologySnapshot,
) -> None:
    if preflight.current_snapshot is None:
        raise SampleRateTopologyVerificationError("sample-rate topology verification lacks a characterized preflight")

    try:
        core.verify_sample_rate_topology(
            preflight.current_snapshot.to_dict(),
            resulting_snapshot.to_dict(),
            preflight.target_capacity.to_dict() if preflight.target_capacity is not None else None,
            preflight.target_sample_rate_hertz,
        )
    except NetaudioCoreError as exception:
        raise SampleRateTopologyVerificationError(str(exception), preflight) from exception


async def change_sample_rate_topology_safe(
    device,
    target_sample_rate_hertz: int,
    probe_sample_rate_status: Callable[[], Awaitable[dict | None]],
    mutate: Callable[[], Awaitable[None]],
    confirm_destructive: bool = False,
    load_channel_capacities: Callable[[], Awaitable[None]] | None = None,
) -> SampleRateTopologyChangeResult:
    if not isinstance(confirm_destructive, bool):
        raise ValueError("confirm_destructive must be a boolean")
    preflight = await preflight_sample_rate_change(
        device,
        target_sample_rate_hertz,
        probe_sample_rate_status,
        load_channel_capacities,
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
        if preflight.capacity_known:
            message = "sample-rate change would permanently remove transmitter flow members; explicit confirmation is required"
        else:
            message = (
                "the device did not report its channel capacity at the target sample rate, so the effect on its "
                "transmitter flows cannot be predicted; explicit confirmation is required"
            )
        raise SampleRateTopologyConfirmationRequired(message, preflight)
    require_writable(device, "sample_rate", target_sample_rate_hertz)

    try:
        await mutate()
    except (OSError, RuntimeError, TimeoutError, ValueError, NetaudioCoreError) as exception:
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
        resulting_snapshot = await capture_sample_rate_topology(device, resulting_capacity, observed_sample_rate_hertz)
        _verify_resulting_topology(preflight, resulting_snapshot)
    except (OSError, RuntimeError, TimeoutError, ValueError, NetaudioCoreError) as exception:
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
