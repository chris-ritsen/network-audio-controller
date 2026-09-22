from __future__ import annotations

from dataclasses import dataclass

from netaudio import core
from netaudio.core import _requests
from netaudio.dante.arc_protocol import require_arc_protocol_for_device
from netaudio.dante.readback import MUTATION_ERRORS


@dataclass(frozen=True)
class SubscriptionReconciliationResult:
    unchanged: dict[int, tuple[str, str] | None]
    verified: dict[int, tuple[str, str] | None]
    failures: dict[int, str]


def plan_receiver_subscription_commands(device, records):
    return core.plan_subscription_commands(
        {
            "protocol_id": require_arc_protocol_for_device(device)["protocol_id"],
            "channels": [
                {"number": channel.number, "media_type_code": getattr(channel, "media_type_code", None)}
                for channel in device.rx_channels.values()
            ],
            "records": records,
        }
    )


def _subscription_evidence(device, expected) -> _requests.SubscriptionReadbackRequest:
    return {
        "channels": [channel.number for channel in device.rx_channels.values()],
        "subscriptions": [
            {
                "number": getattr(subscription.rx_channel, "number", None),
                "tx_channel": subscription.tx_channel_name,
                "tx_device": subscription.tx_device_name,
                "status_code": getattr(subscription, "status_code", None),
                "receiver_status_code": getattr(subscription, "rx_channel_status_code", None),
                "managed_status": getattr(subscription, "ddm_status", None),
            }
            for subscription in device.subscriptions
        ],
        "expected": [
            {"number": number, "source": list(source) if source is not None else None}
            for number, source in expected.items()
        ],
    }


def evaluate_subscription_readback(device, expected):
    return core.subscription_readback(_subscription_evidence(device, expected))


def _source_tuple(source: list[str] | None) -> tuple[str, str] | None:
    return (source[0], source[1]) if source is not None else None


def subscription_sources(device, channel_numbers):
    result = evaluate_subscription_readback(device, dict.fromkeys(channel_numbers))

    return {channel["number"]: _source_tuple(channel["source"]) for channel in result["channels"]}


def subscriptions_settled(device, expected):
    """Stop polling a confirmed configuration once connection status is terminal."""
    try:
        return evaluate_subscription_readback(device, expected)["settled"]
    except MUTATION_ERRORS:
        return False


async def read_subscription_readback(device, expected):
    await device.get_rx_channels()

    return evaluate_subscription_readback(device, expected)


async def reconcile_receiver_subscriptions(
    application,
    device,
    desired_sources: dict[int, tuple[str, str] | None],
) -> SubscriptionReconciliationResult:
    await device.get_rx_channels()
    evidence = _subscription_evidence(device, desired_sources)
    plan = core.plan_subscription_reconciliation(
        {
            **evidence,
            "protocol_id": require_arc_protocol_for_device(device)["protocol_id"],
            "managed": bool(getattr(device, "requires_managed_control", False)),
            "channels": [
                {"number": channel.number, "media_type_code": getattr(channel, "media_type_code", None)}
                for channel in device.rx_channels.values()
            ],
        }
    )
    unchanged = {entry["number"]: _source_tuple(entry["source"]) for entry in plan["unchanged"]}
    verified: dict[int, tuple[str, str] | None] = {}
    failures: dict[int, str] = {}

    for batch in plan["batches"]:
        expected = {entry["number"]: _source_tuple(entry["source"]) for entry in batch["expected"]}

        try:
            if batch["action"] == "clear":
                await application.remove_subscriptions(device, list(expected))
            else:
                records = []

                for number, source in expected.items():
                    assert source is not None, "core returned an empty source in a set batch"
                    records.append((number, *source))

                await application.add_subscriptions(device, records)
        except MUTATION_ERRORS as exception:
            for number in expected:
                failures[number] = f"request failed: {exception}"

            continue

        try:
            readback = await read_subscription_readback(device, expected)
        except MUTATION_ERRORS as exception:
            for number in expected:
                failures[number] = f"fresh readback unavailable: {exception}"

            continue

        for channel in readback["channels"]:
            number = channel["number"]

            if channel["matched"]:
                verified[number] = expected[number]
            else:
                failures[number] = f"fresh readback reports {channel['source']!r}"

    return SubscriptionReconciliationResult(
        unchanged=unchanged,
        verified=verified,
        failures=failures,
    )
