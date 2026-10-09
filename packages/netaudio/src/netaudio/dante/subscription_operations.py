from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field

from netaudio import core
from netaudio.core import _requests
from netaudio.dante.arc_protocol import require_arc_protocol_for_device
from netaudio.dante.readback import MUTATION_ERRORS

READBACK_INTERVAL_SECONDS = 0.5
READBACK_TIMEOUT_SECONDS = 5.0


@dataclass(frozen=True)
class SubscriptionReconciliationResult:
    unchanged: dict[int, tuple[str, str] | None]
    verified: dict[int, tuple[str, str] | None]
    failures: dict[int, str]
    sent: dict[int, tuple[str, str] | None] = field(default_factory=dict)
    unconfirmed: dict[int, str] = field(default_factory=dict)


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


def request_rejection(response) -> str | None:
    successful = getattr(response, "successful", None)
    if successful is not None:
        return None if successful else "the managing server rejected the request"
    if not isinstance(response, (bytes, bytearray)):
        return None
    receipt = core.command_acknowledgement(bytes(response))
    if not receipt or not receipt["parseable"] or receipt.get("accepted") is not False:
        return None
    code = receipt.get("result_code")
    return "device rejected the request" if code is None else f"device rejected the request (result 0x{code:04x})"


async def reconcile_receiver_subscriptions(
    application,
    device,
    desired_sources: dict[int, tuple[str, str] | None],
    verify: bool = True,
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
    sent: dict[int, tuple[str, str] | None] = {}
    unconfirmed: dict[int, str] = {}

    for batch in plan["batches"]:
        expected = {entry["number"]: _source_tuple(entry["source"]) for entry in batch["expected"]}

        try:
            if batch["action"] == "clear":
                response = await application.remove_subscriptions(device, list(expected))
            else:
                records = []

                for number, source in expected.items():
                    assert source is not None, "core returned an empty source in a set batch"
                    records.append((number, *source))

                response = await application.add_subscriptions(device, records)
        except MUTATION_ERRORS as exception:
            for number in expected:
                failures[number] = f"request failed: {exception}"

            continue

        rejection = request_rejection(response)
        if rejection is not None:
            for number in expected:
                failures[number] = rejection
            continue

        if not verify:
            sent.update(expected)
            continue

        observed, readback_error = await _poll_subscription_readback(device, expected)

        for number, source in expected.items():
            if number in observed and observed[number]["matched"]:
                verified[number] = source
            elif number in observed:
                failures[number] = f"fresh readback reports {observed[number]['source']!r}"
            else:
                unconfirmed[number] = f"readback unavailable: {readback_error}"

    return SubscriptionReconciliationResult(
        unchanged=unchanged,
        verified=verified,
        failures=failures,
        sent=sent,
        unconfirmed=unconfirmed,
    )


async def _poll_subscription_readback(device, expected):
    deadline = time.monotonic() + READBACK_TIMEOUT_SECONDS
    observed: dict[int, dict] = {}
    readback_error = None

    while True:
        try:
            readback = await read_subscription_readback(device, expected)
        except MUTATION_ERRORS as exception:
            readback_error = exception
        else:
            observed = {channel["number"]: channel for channel in readback["channels"]}
            if all(channel["matched"] for channel in observed.values()) and len(observed) == len(expected):
                return observed, None

        if time.monotonic() + READBACK_INTERVAL_SECONDS > deadline:
            return observed, readback_error

        await asyncio.sleep(READBACK_INTERVAL_SECONDS)
