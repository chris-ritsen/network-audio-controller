from __future__ import annotations

from dataclasses import dataclass

from netaudio.dante.readback import readback_after_notification
from netaudio.dante.channel import channel_by_number
from netaudio.dante.channel_frontend import (
    ChannelFrontendError,
    require_channel_acknowledgement,
)


@dataclass(frozen=True)
class TransmitterChannelNameReconciliationResult:
    unchanged: dict[int, str]
    verified: dict[int, str]
    failures: dict[int, str]


def _routing_name(device, channel_number):
    channels = getattr(device, "tx_channels", None) or {}
    channel = channel_by_number(channels.values(), channel_number)

    if channel is None:
        raise RuntimeError(f"TX channel {channel_number} was unavailable during readback")

    return channel.friendly_name or channel.name


async def _read_routing_names(device, channel_numbers):
    await device.get_tx_channels()
    return {channel_number: _routing_name(device, channel_number) for channel_number in channel_numbers}


async def reconcile_transmitter_channel_names(
    application,
    device,
    desired_names: dict[int, str],
) -> TransmitterChannelNameReconciliationResult:
    from netaudio import core

    channel_numbers = tuple(desired_names)
    current_names = await _read_routing_names(device, channel_numbers)
    unchanged = {
        channel_number: desired_name
        for channel_number, desired_name in desired_names.items()
        if current_names[channel_number] == desired_name
    }
    pending = {
        channel_number: desired_name
        for channel_number, desired_name in desired_names.items()
        if current_names[channel_number] != desired_name
    }
    if not pending:
        return TransmitterChannelNameReconciliationResult(
            unchanged=unchanged,
            verified={},
            failures={},
        )

    try:
        await application.resolve_channel_name_protocol_identifier(device, "tx")
    except (core.NetaudioCoreError, ChannelFrontendError, OSError, RuntimeError) as exception:
        return TransmitterChannelNameReconciliationResult(
            unchanged=unchanged,
            verified={},
            failures={channel_number: f"frontend probe failed: {exception}" for channel_number in pending},
        )

    verified: dict[int, str] = {}
    failures: dict[int, str] = {}
    for channel_number, desired_name in pending.items():
        try:
            async with device.topology_mutation_lock:
                response = await application.set_channel_name(device, "tx", channel_number, desired_name)
            require_channel_acknowledgement(response, "transmitter channel name change")
        except (core.NetaudioCoreError, ChannelFrontendError, OSError, RuntimeError) as exception:
            failures[channel_number] = f"request failed: {exception}"
            continue

        readback = await readback_after_notification(
            lambda channel_number=channel_number: _read_routing_names(device, (channel_number,)),
            {channel_number: desired_name},
        )
        if readback.matched:
            verified[channel_number] = desired_name
        elif readback.observed_available:
            observed = readback.observed if isinstance(readback.observed, dict) else {}
            failures[channel_number] = f"fresh readback reports {observed.get(channel_number)!r}"
        else:
            failures[channel_number] = f"fresh readback unavailable: {readback.error}"

    return TransmitterChannelNameReconciliationResult(
        unchanged=unchanged,
        verified=verified,
        failures=failures,
    )
