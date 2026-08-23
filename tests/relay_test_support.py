import asyncio
import json
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from netaudio.asynchronous_primitives import DeferredAsyncioLock
from netaudio.daemon.relay import RelayServer
from netaudio.dante.events import DanteEvent, EventType
from netaudio.dante.lock_status import LockStatusObservation
from netaudio.dante.services.notification import DanteNotificationService


class FakeWriter:
    def __init__(self, peer=("127.0.0.1", 40000)):
        self.data = bytearray()
        self.closed = False
        self.peer = peer

    def get_extra_info(self, name):
        if name == "peername":
            return self.peer
        return None

    def write(self, payload):
        self.data.extend(payload)

    async def drain(self):
        pass

    def close(self):
        self.closed = True

    async def wait_closed(self):
        pass

    def response(self):
        raw = bytes(self.data).decode()
        header, _, body = raw.partition("\r\n\r\n")
        status = int(header.split(" ")[1])
        return status, json.loads(body) if body else None

    def response_headers(self):
        raw = bytes(self.data).decode()
        header, _, _ = raw.partition("\r\n\r\n")
        return {
            name.strip().lower(): value.strip()
            for line in header.split("\r\n")[1:]
            for name, value in [line.split(":", 1)]
        }


def make_device(server_name="dev1", name="Device1", ipv4="192.168.1.50"):
    device = SimpleNamespace(
        server_name=server_name,
        name=name,
        ipv4=ipv4,
        rx_channels={},
        tx_channels={},
        online=True,
        interfaces=[],
        link_speed_mbps=None,
        flow_protocol_id=None,
        is_locked=False,
        interface_reboot_required=False,
        interface_pending_config=None,
        sample_rate=None,
        supported_sample_rates=None,
        aes67_supported=None,
        aes67_multicast_prefix=None,
        settings_properties=None,
        sample_rate_pullup_raw_value=None,
        requested_sample_rate_pullup_raw_value=None,
        supported_sample_rate_pullup_raw_values=None,
        encoding=None,
        supported_encodings=None,
        gain_device_type=None,
        gain_levels=None,
        supported_gain_levels=None,
        _arc_port=MagicMock(return_value=4440),
        operations=MagicMock(),
        topology_mutation_lock=DeferredAsyncioLock(),
    )
    for method in (
        "add_subscription_by_name",
        "add_subscriptions_by_name",
        "remove_subscription",
        "remove_subscriptions",
        "identify",
        "set_name",
        "reset_name",
        "set_channel_name",
        "reset_channel_name",
        "set_latency",
        "set_sample_rate",
        "set_encoding",
        "enable_aes67",
        "reboot",
        "lock_device",
        "unlock_device",
    ):
        setattr(device.operations, method, AsyncMock(return_value=None))
    arc_success = bytes.fromhex("27ff000a000010010001")
    for method in (
        "add_subscription_by_name",
        "add_subscriptions_by_name",
        "remove_subscription",
        "remove_subscriptions",
        "set_name",
        "reset_name",
        "set_channel_name",
        "reset_channel_name",
        "set_latency",
    ):
        setattr(device.operations, method, AsyncMock(return_value=arc_success))
    device.operations.set_gain_level = AsyncMock(return_value=("input", [3]))
    device.operations.lock_device = AsyncMock(return_value={"success": True, "lock_state": 1})
    device.operations.unlock_device = AsyncMock(return_value={"success": True, "lock_state": 0})
    return device


def make_relay(devices=None, metering=None, on_shutdown=None):
    notifications = DanteNotificationService(dispatcher=MagicMock())

    def sample_rate_change_result(_device, sample_rate, **_options):
        return SimpleNamespace(
            to_dict=lambda: {
                "success": True,
                "changed": True,
                "preflight": {"target_sample_rate_hertz": sample_rate},
                "readback": {"sample_rate_hertz": sample_rate},
            }
        )

    application = SimpleNamespace(
        devices=devices or {},
        dispatcher=MagicMock(),
        notifications=notifications,
        settings=MagicMock(),
        mark_device_offline=MagicMock(),
        probe_sample_rate_status=AsyncMock(return_value=(48000, [48000, 96000])),
        set_sample_rate_state=AsyncMock(side_effect=sample_rate_change_result),
        probe_encoding_status=AsyncMock(return_value=(24, [16, 24, 32])),
        probe_gain_status=AsyncMock(return_value=("input", [3])),
        probe_interface_status=AsyncMock(return_value=[{"mode": "dynamic", "ip_address": "192.168.1.50"}]),
        probe_lock_status=AsyncMock(
            return_value=LockStatusObservation(
                lock_reset_status={
                    "lock_state_code": 0,
                    "is_locked": False,
                    "status_code": 0,
                },
                observed_at=datetime(2026, 8, 21, 20, 57, 35, 396345, tzinfo=timezone.utc).isoformat(
                    timespec="microseconds"
                ),
            )
        ),
        set_preferred_leader_state=AsyncMock(side_effect=lambda _address, expected: expected),
        set_aes67_state=AsyncMock(side_effect=lambda _device, expected: (False, expected)),
        set_aes67_multicast_prefix_state=AsyncMock(side_effect=lambda _device, prefix: prefix),
        set_sample_rate_pullup_state=AsyncMock(side_effect=lambda _device, raw_value: (raw_value, [0, 1, 2, 3, 4])),
        set_clock_source_state=AsyncMock(side_effect=lambda _device, clock_source: clock_source),
        set_clock_subdomain_state=AsyncMock(side_effect=lambda _device, subdomain: subdomain),
        probe_clocking_status=AsyncMock(
            return_value={
                "clock_source_code": 0,
                "clock_subdomain": bytes(16),
                "preferred_leader": False,
                "clock_role": "Follower",
            }
        ),
        set_interface_dhcp=AsyncMock(return_value=[{"mode": "dynamic"}]),
        set_interface_static=AsyncMock(
            side_effect=lambda _address, ip_address, netmask, dns_server, gateway: [
                {
                    "mode": "static",
                    "ip_address": ip_address,
                    "netmask": netmask,
                    "dns_server": dns_server,
                    "gateway": gateway,
                }
            ]
        ),
    )
    state = SimpleNamespace(
        refresh_device=AsyncMock(),
        refresh_all_devices=AsyncMock(),
    )
    relay = RelayServer(application, state, metering=metering, on_shutdown=on_shutdown)
    relay.audio_capability_verification_timeout = 0.05
    return relay


async def post(relay, path, body):
    writer = FakeWriter()
    if isinstance(body, (dict, list)):
        body = json.dumps(body).encode()
    await relay._dispatch("POST", path, body, writer)
    return writer.response()


async def get(relay, path):
    writer = FakeWriter()
    await relay._dispatch("GET", path, None, writer)
    return writer.response()
