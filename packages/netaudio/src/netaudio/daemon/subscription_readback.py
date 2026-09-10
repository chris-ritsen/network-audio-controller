from __future__ import annotations

import asyncio
import logging

logger = logging.getLogger("netaudio")

READBACK_WINDOW_SECONDS = 10
READBACK_INTERVAL_SECONDS = 0.5
READBACK_TIMEOUT_SECONDS = 5


def subscriptions_settled(device, expected):
    subscriptions = {
        subscription.rx_channel.number: subscription
        for subscription in device.subscriptions
        if subscription.rx_channel is not None
    }
    for number, (channel, transmitter) in expected.items():
        if number not in device.rx_channels:
            return False
        subscription = subscriptions.get(number)
        if not transmitter:
            if subscription is not None and subscription.tx_device_name:
                return False
            continue
        if subscription is None or (subscription.tx_channel_name, subscription.tx_device_name) != (
            channel,
            transmitter,
        ):
            return False
        status = subscription.to_json().get("status") or {}
        if status.get("severity") not in {"ok", "error", "warning"}:
            return False
    return True


class SubscriptionReadback:
    def __init__(self, publish, application=None):
        self.publish = publish
        self.application = application
        self.pending = {}
        self.tasks = {}

    def request(self, device, records):
        key = device.server_name
        expected, _ = self.pending.get(key, ({}, 0))
        expected.update({number: (channel, transmitter) for number, channel, transmitter in records})
        self.pending[key] = (expected, asyncio.get_running_loop().time() + READBACK_WINDOW_SECONDS)
        if key not in self.tasks:
            self.tasks[key] = asyncio.create_task(self._run(device), name=f"subscription-readback:{key}")

    async def _read(self, device):
        async with device.topology_mutation_lock:
            if getattr(device, "requires_managed_control", False):
                from netaudio.daemon.managed_controls import refresh_managed_subscriptions

                await refresh_managed_subscriptions(self.application, device)
            else:
                await device.get_rx_channels()

    async def _run(self, device):
        key = device.server_name
        loop = asyncio.get_running_loop()
        try:
            while device.online and key in self.pending:
                expected, deadline = self.pending[key]
                remaining = deadline - loop.time()
                if remaining <= 0:
                    break
                try:
                    await asyncio.wait_for(self._read(device), min(READBACK_TIMEOUT_SECONDS, remaining))
                    self.publish(device)
                    if subscriptions_settled(device, expected):
                        break
                except (RuntimeError, OSError, asyncio.TimeoutError) as error:
                    logger.debug("Subscription readback unavailable for %s: %s", key, error)
                await asyncio.sleep(min(READBACK_INTERVAL_SECONDS, max(0, deadline - loop.time())))
        finally:
            self.pending.pop(key, None)
            self.tasks.pop(key, None)

    async def stop(self):
        tasks = list(self.tasks.values())
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self.tasks.clear()
        self.pending.clear()
