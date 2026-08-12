import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from netaudio.daemon.relay import RelayServer
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
        encoding=None,
        supported_encodings=None,
        gain_device_type=None,
        gain_levels=None,
        supported_gain_levels=None,
        _arc_port=MagicMock(return_value=4440),
        operations=MagicMock(),
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
    application = SimpleNamespace(
        devices=devices or {},
        dispatcher=MagicMock(),
        notifications=notifications,
        settings=MagicMock(),
        mark_device_offline=MagicMock(),
        probe_sample_rate_status=AsyncMock(return_value=(48000, [48000, 96000])),
        probe_encoding_status=AsyncMock(return_value=(24, [16, 24, 32])),
        probe_gain_status=AsyncMock(return_value=("input", [3])),
        probe_interface_status=AsyncMock(return_value=[{"mode": "dynamic", "ip_address": "192.168.1.50"}]),
        set_preferred_leader_state=AsyncMock(side_effect=lambda _address, expected: expected),
        set_aes67_state=AsyncMock(side_effect=lambda _device, expected: (False, expected)),
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


class TestRouting:
    @pytest.mark.asyncio
    async def test_unknown_path_returns_404(self):
        relay = make_relay()
        status, body = await post(relay, "/nonexistent", {"device": "x"})
        assert status == 404
        assert body == {"error": "not found"}

    @pytest.mark.asyncio
    async def test_unknown_method_returns_404(self):
        relay = make_relay()
        writer = FakeWriter()
        await relay._dispatch("DELETE", "/devices", None, writer)
        status, body = writer.response()
        assert status == 404

    @pytest.mark.asyncio
    async def test_missing_body_returns_400(self):
        relay = make_relay()
        status, body = await post(relay, "/subscribe", None)
        assert status == 400
        assert body == {"error": "missing body"}

    @pytest.mark.asyncio
    async def test_malformed_json_returns_400(self):
        relay = make_relay()
        status, body = await post(relay, "/subscribe", b"{not json")
        assert status == 400
        assert "invalid json" in body["error"]

    @pytest.mark.asyncio
    async def test_non_object_body_returns_400(self):
        relay = make_relay()
        status, body = await post(relay, "/subscribe", b'["a", "b"]')
        assert status == 400
        assert body == {"error": "body must be a json object"}

    @pytest.mark.asyncio
    async def test_refresh_allows_missing_body(self):
        relay = make_relay()
        status, body = await post(relay, "/refresh", None)
        assert status == 200
        assert body == {"success": True}
        relay.state.refresh_all_devices.assert_awaited_once()


class TestErrorMapping:
    @pytest.mark.asyncio
    async def test_handler_exception_returns_500(self):
        device = make_device()
        device.operations.identify.side_effect = RuntimeError("socket exploded")
        relay = make_relay({"dev1": device})
        writer = FakeWriter()
        await relay._route("POST", "/identify", json.dumps({"device": "dev1"}).encode(), writer, None)
        status, body = writer.response()
        assert status == 500
        assert body == {"error": "socket exploded"}

    @pytest.mark.asyncio
    async def test_handler_timeout_returns_504(self):
        device = make_device()
        device.operations.set_latency.side_effect = TimeoutError()
        relay = make_relay({"dev1": device})
        writer = FakeWriter()
        await relay._route("POST", "/set-latency", json.dumps({"device": "dev1", "latency": 5}).encode(), writer, None)
        status, body = writer.response()
        assert status == 504
        assert body == {"error": "device did not respond"}


class TestMutationVerification:
    @pytest.mark.asyncio
    async def test_arc_mutations_report_device_rejection(self):
        device = make_device()
        device.operations.set_latency.return_value = bytes.fromhex("27ff000a000010010600")
        relay = make_relay({"dev1": device})

        status, response = await post(relay, "/set-latency", {"device": "dev1", "latency": 1.0})

        assert status == 409
        assert response["result_code"] == 0x0600

    @pytest.mark.asyncio
    async def test_gain_mutation_requires_matching_multicast_readback(self):
        device = make_device()
        relay = make_relay({"dev1": device})

        status, response = await post(
            relay,
            "/set-gain",
            {"device": "dev1", "channel_number": 1, "gain_level": 3, "device_type": "input"},
        )

        assert status == 200
        assert response == {"success": True}
        device.operations.set_gain_level.assert_awaited_once_with(1, 3, "input")

    @pytest.mark.asyncio
    async def test_gain_mutation_reports_mismatched_readback(self):
        device = make_device()
        device.operations.set_gain_level.return_value = ("input", [5])
        relay = make_relay({"dev1": device})

        status, response = await post(
            relay,
            "/set-gain",
            {"device": "dev1", "channel_number": 1, "gain_level": 3, "device_type": "input"},
        )

        assert status == 409
        assert response == {
            "error": "gain change was not applied",
            "observed_device_type": "input",
            "observed_level": 5,
        }

    @pytest.mark.asyncio
    async def test_gain_mutation_reports_missing_readback(self):
        device = make_device()
        device.operations.set_gain_level.return_value = None
        relay = make_relay({"dev1": device})

        status, response = await post(
            relay,
            "/set-gain",
            {"device": "dev1", "channel_number": 1, "gain_level": 3, "device_type": "input"},
        )

        assert status == 504
        assert response == {"error": "gain readback was unavailable"}

    @pytest.mark.parametrize(
        ("path", "body", "method_name", "probe_name", "expected_status"),
        [
            (
                "/set-sample-rate",
                {"device": "dev1", "sample_rate": 48000},
                "set_sample_rate",
                "probe_sample_rate_status",
                (48000, [48000, 96000]),
            ),
            (
                "/set-encoding",
                {"device": "dev1", "encoding": 24},
                "set_encoding",
                "probe_encoding_status",
                (24, [16, 24, 32]),
            ),
        ],
    )
    @pytest.mark.asyncio
    async def test_audio_capability_mutations_require_matching_readback(
        self,
        path,
        body,
        method_name,
        probe_name,
        expected_status,
    ):
        device = make_device()
        relay = make_relay({"dev1": device})
        getattr(relay.application, probe_name).return_value = expected_status

        status, response = await post(relay, path, body)

        assert status == 200
        assert response == {"success": True}
        requested_value = next(value for field_name, value in body.items() if field_name != "device")
        getattr(device.operations, method_name).assert_awaited_once_with(requested_value)
        getattr(relay.application, probe_name).assert_awaited_once_with("192.168.1.50")

    @pytest.mark.parametrize(
        ("path", "body", "method_name", "probe_name", "capability_name", "old_status", "requested_status"),
        [
            (
                "/set-sample-rate",
                {"device": "dev1", "sample_rate": 96000},
                "set_sample_rate",
                "probe_sample_rate_status",
                "sample_rate",
                (48000, [48000, 96000]),
                (96000, [48000, 96000]),
            ),
            (
                "/set-encoding",
                {"device": "dev1", "encoding": 32},
                "set_encoding",
                "probe_encoding_status",
                "encoding",
                (24, [16, 24, 32]),
                (32, [16, 24, 32]),
            ),
        ],
    )
    @pytest.mark.asyncio
    async def test_audio_capability_mutation_ignores_old_status_until_requested_value_arrives(
        self,
        path,
        body,
        method_name,
        probe_name,
        capability_name,
        old_status,
        requested_status,
    ):
        device = make_device()
        relay = make_relay({"dev1": device})
        old_status_observed = asyncio.Event()

        async def probe_status(_device_ip_address):
            relay.application.notifications._notify_capability_value_waiters(
                capability_name,
                "192.168.1.50",
                old_status[0],
                old_status[1],
            )
            old_status_observed.set()
            return old_status

        setattr(relay.application, probe_name, AsyncMock(side_effect=probe_status))
        request_task = asyncio.create_task(post(relay, path, body))
        await old_status_observed.wait()

        assert not request_task.done()
        relay.application.notifications._notify_capability_value_waiters(
            capability_name,
            "192.168.1.99",
            requested_status[0],
            requested_status[1],
        )
        relay.application.notifications._notify_capability_value_waiters(
            "encoding" if capability_name == "sample_rate" else "sample_rate",
            "192.168.1.50",
            requested_status[0],
            requested_status[1],
        )
        assert not request_task.done()
        relay.application.notifications._notify_capability_value_waiters(
            capability_name,
            "192.168.1.50",
            requested_status[0],
            requested_status[1],
        )

        status, response = await request_task

        assert status == 200
        assert response == {"success": True}
        requested_value = next(value for field_name, value in body.items() if field_name != "device")
        getattr(device.operations, method_name).assert_awaited_once_with(requested_value)

    @pytest.mark.parametrize(
        ("path", "body", "probe_name", "observed", "supported"),
        [
            (
                "/set-sample-rate",
                {"device": "dev1", "sample_rate": 96000},
                "probe_sample_rate_status",
                48000,
                [48000, 96000],
            ),
            (
                "/set-encoding",
                {"device": "dev1", "encoding": 32},
                "probe_encoding_status",
                24,
                [16, 24, 32],
            ),
        ],
    )
    @pytest.mark.asyncio
    async def test_audio_capability_mutation_mismatch_is_conflict(
        self,
        path,
        body,
        probe_name,
        observed,
        supported,
    ):
        device = make_device()
        relay = make_relay({"dev1": device})
        getattr(relay.application, probe_name).return_value = (observed, supported)

        status, response = await post(relay, path, body)

        assert status == 409
        assert response["observed"] == observed
        assert response["supported"] == supported

    @pytest.mark.parametrize(
        ("path", "body", "probe_name", "description"),
        [
            (
                "/set-sample-rate",
                {"device": "dev1", "sample_rate": 48000},
                "probe_sample_rate_status",
                "sample rate",
            ),
            (
                "/set-encoding",
                {"device": "dev1", "encoding": 24},
                "probe_encoding_status",
                "encoding",
            ),
        ],
    )
    @pytest.mark.asyncio
    async def test_audio_capability_mutation_missing_readback_is_gateway_timeout(
        self,
        path,
        body,
        probe_name,
        description,
    ):
        device = make_device()
        relay = make_relay({"dev1": device})
        getattr(relay.application, probe_name).return_value = None

        status, response = await post(relay, path, body)

        assert status == 504
        assert response == {"error": f"{description} readback was unavailable"}

    @pytest.mark.parametrize(
        ("path", "body", "method_name", "probe_name"),
        [
            (
                "/set-sample-rate",
                {"device": "dev1", "sample_rate": 96000},
                "set_sample_rate",
                "probe_sample_rate_status",
            ),
            (
                "/set-encoding",
                {"device": "dev1", "encoding": 32},
                "set_encoding",
                "probe_encoding_status",
            ),
        ],
    )
    @pytest.mark.asyncio
    async def test_audio_capability_rejection_is_conflict_without_readback(
        self,
        path,
        body,
        method_name,
        probe_name,
    ):
        device = make_device()
        getattr(device.operations, method_name).side_effect = ValueError("requested value is not supported")
        relay = make_relay({"dev1": device})

        status, response = await post(relay, path, body)

        assert status == 409
        assert response == {"error": "requested value is not supported"}
        getattr(relay.application, probe_name).assert_not_awaited()

    @pytest.mark.parametrize(
        ("path", "body"),
        [
            ("/set-sample-rate", {"device": "dev1", "sample_rate": 0}),
            ("/set-sample-rate", {"device": "dev1", "sample_rate": True}),
            ("/set-encoding", {"device": "dev1", "encoding": "24"}),
            ("/set-encoding", {"device": "dev1", "encoding": 0x100000000}),
        ],
    )
    @pytest.mark.asyncio
    async def test_audio_capability_mutation_rejects_invalid_wire_values(self, path, body):
        device = make_device()
        relay = make_relay({"dev1": device})

        status, response = await post(relay, path, body)

        assert status == 400
        assert "must be an integer" in response["error"]
        device.operations.set_sample_rate.assert_not_awaited()
        device.operations.set_encoding.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_subscription_rejection_is_not_reported_as_success(self):
        device = make_device()
        device.operations.add_subscription_by_name.return_value = bytes.fromhex("27ff000a000010010600")
        relay = make_relay({"dev1": device})

        status, response = await post(
            relay,
            "/subscribe",
            {
                "rx_device": "dev1",
                "rx_channel": 1,
                "tx_channel": "Out1",
                "tx_device": "Mixer",
            },
        )

        assert status == 409
        assert response["result_code"] == 0x0600

    @pytest.mark.asyncio
    async def test_unsubscribe_timeout_is_not_reported_as_success(self):
        device = make_device()
        channel = SimpleNamespace(number=1)
        device.rx_channels = {1: channel}
        device.operations.remove_subscription.return_value = None
        relay = make_relay({"dev1": device})

        status, response = await post(relay, "/unsubscribe", {"rx_device": "dev1", "rx_channel": 1})

        assert status == 504
        assert response == {"error": "device did not respond"}

    @pytest.mark.asyncio
    async def test_preferred_leader_mismatch_is_conflict(self):
        device = make_device()
        relay = make_relay({"dev1": device})
        relay.application.set_preferred_leader_state.side_effect = None
        relay.application.set_preferred_leader_state.return_value = False

        status, response = await post(
            relay,
            "/set-preferred-leader",
            {"device": "dev1", "preferred": True},
        )

        assert status == 409
        assert response["observed"] is False

    @pytest.mark.asyncio
    async def test_aes67_missing_readback_is_gateway_timeout(self):
        device = make_device()
        relay = make_relay({"dev1": device})
        relay.application.set_aes67_state.side_effect = None
        relay.application.set_aes67_state.return_value = None

        status, response = await post(relay, "/set-aes67", {"device": "dev1", "enabled": True})

        assert status == 504
        assert response == {"error": "AES67 readback was unavailable"}

    @pytest.mark.asyncio
    async def test_aes67_rejects_device_without_directory_property(self):
        device = make_device()
        device.aes67_supported = False
        relay = make_relay({"dev1": device})

        status, response = await post(relay, "/set-aes67", {"device": "dev1", "enabled": True})

        assert status == 409
        assert response == {"error": "device does not support AES67 configuration"}
        relay.application.set_aes67_state.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_reboot_is_reported_as_accepted_but_unverified(self):
        device = make_device()
        relay = make_relay({"dev1": device})

        status, response = await post(relay, "/reboot", {"device": "dev1"})

        assert status == 202
        assert response == {"accepted": True, "verified": False}
        device.operations.reboot.assert_awaited_once_with()

    @pytest.mark.asyncio
    async def test_interface_mismatch_is_conflict(self):
        device = make_device()
        relay = make_relay({"dev1": device})
        relay.application.set_interface_dhcp.return_value = [{"mode": "static"}]

        status, response = await post(relay, "/interface", {"device": "dev1", "mode": "dhcp"})

        assert status == 409
        assert response["interfaces"] == [{"mode": "static"}]

    @pytest.mark.asyncio
    async def test_applied_interface_reports_no_reboot(self):
        device = make_device()
        relay = make_relay({"dev1": device})

        status, response = await post(relay, "/interface", {"device": "dev1", "mode": "dhcp"})

        assert status == 200
        assert response == {
            "success": True,
            "reboot_required": False,
            "interfaces": [{"mode": "dynamic"}],
        }

    @pytest.mark.asyncio
    async def test_pending_interface_reports_reboot(self):
        device = make_device()
        device.interface_reboot_required = True
        device.interface_pending_config = {"mode": "dynamic"}
        relay = make_relay({"dev1": device})
        relay.application.set_interface_dhcp.return_value = [{"mode": "static"}]

        status, response = await post(relay, "/interface", {"device": "dev1", "mode": "dhcp"})

        assert status == 200
        assert response == {
            "success": True,
            "reboot_required": True,
            "interfaces": [{"mode": "static"}],
        }

    @pytest.mark.asyncio
    async def test_failed_lock_result_uses_conflict_status(self, monkeypatch):
        device = make_device()
        device.operations.lock_device.return_value = {"success": False, "error": "locked"}
        relay = make_relay({"dev1": device})
        monkeypatch.setattr(relay, "_get_lock_key", lambda: b"key")

        status, response = await post(relay, "/lock", {"device": "dev1", "pin": "1234"})

        assert status == 409
        assert response == {"success": False, "error": "locked"}


class TestDeviceLookup:
    @pytest.mark.asyncio
    async def test_interface_status_is_probed_on_demand(self):
        device = make_device()
        device.link_speed_mbps = 100
        relay = make_relay({"dev1": device})

        status, body = await get(relay, "/interfaces/Device1")

        assert status == 200
        assert body == {
            "device": "dev1",
            "interfaces": [{"mode": "dynamic", "ip_address": "192.168.1.50"}],
            "link_speed_mbps": 100,
            "reboot_required": False,
            "pending_config": None,
        }
        relay.application.probe_interface_status.assert_awaited_once_with("192.168.1.50")
        assert device.interfaces == body["interfaces"]

    @pytest.mark.asyncio
    async def test_interface_status_refuses_offline_device_without_waiting(self):
        device = make_device()
        device.online = False
        relay = make_relay({"dev1": device})

        status, body = await get(relay, "/interfaces/dev1")

        assert status == 409
        assert body == {"error": "device is offline"}
        relay.application.probe_interface_status.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_interface_status_timeout_is_not_reported_as_empty_success(self):
        device = make_device()
        relay = make_relay({"dev1": device})
        relay.application.probe_interface_status.return_value = None

        status, body = await get(relay, "/interfaces/dev1")

        assert status == 504
        assert body == {"error": "interface status was not reported"}

    @pytest.mark.asyncio
    async def test_unknown_device_returns_404(self):
        relay = make_relay()
        status, body = await post(relay, "/identify", {"device": "ghost"})
        assert status == 404
        assert body == {"error": "device not found"}

    @pytest.mark.asyncio
    async def test_lookup_by_friendly_name_case_insensitive(self):
        device = make_device(server_name="dev1", name="Studio-AVIO")
        relay = make_relay({"dev1": device})
        status, body = await post(relay, "/identify", {"device": "studio-avio"})
        assert status == 202
        assert body == {"accepted": True, "verified": False}
        device.operations.identify.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_lookup_by_ip_address(self):
        device = make_device(ipv4="192.168.1.50")
        relay = make_relay({"dev1": device})
        status, body = await post(relay, "/identify", {"device": "192.168.1.50"})
        assert status == 202
        assert body == {"accepted": True, "verified": False}


class TestRenameReset:
    @pytest.mark.asyncio
    async def test_blank_device_name_uses_protocol_reset(self):
        device = make_device()
        relay = make_relay({"dev1": device})

        status, body = await post(relay, "/rename-device", {"device": "dev1", "name": " \t"})

        assert status == 200
        assert body == {"success": True}
        device.operations.reset_name.assert_awaited_once_with()
        device.operations.set_name.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_nonblank_device_name_is_preserved(self):
        device = make_device()
        relay = make_relay({"dev1": device})

        status, _ = await post(relay, "/rename-device", {"device": "dev1", "name": "  Stage Rack  "})

        assert status == 200
        device.operations.set_name.assert_awaited_once_with("  Stage Rack  ")
        device.operations.reset_name.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_blank_channel_name_uses_protocol_reset(self):
        device = make_device()
        relay = make_relay({"dev1": device})

        status, body = await post(
            relay, "/rename-channel", {"device": "dev1", "channel_type": "tx", "channel_number": 3, "name": ""}
        )

        assert status == 200
        assert body == {"success": True}
        device.operations.reset_channel_name.assert_awaited_once_with("tx", 3)
        device.operations.set_channel_name.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_locked_name_reset_reports_device_rejection(self):
        device = make_device()
        device.operations.reset_name.return_value = bytes.fromhex("27ff000a000010010600")
        relay = make_relay({"dev1": device})

        status, body = await post(relay, "/rename-device", {"device": "dev1", "name": ""})

        assert status == 409
        assert body["result_code"] == 0x0600
        assert "0x0600" in body["error"]

    @pytest.mark.asyncio
    async def test_name_reset_timeout_is_not_reported_as_success(self):
        device = make_device()
        device.operations.reset_name.return_value = None
        relay = make_relay({"dev1": device})

        status, body = await post(relay, "/rename-device", {"device": "dev1", "name": ""})

        assert status == 504
        assert body == {"error": "device did not respond"}

    @pytest.mark.asyncio
    async def test_malformed_name_reset_response_is_not_reported_as_success(self):
        device = make_device()
        device.operations.reset_name.return_value = b"short"
        relay = make_relay({"dev1": device})

        status, body = await post(relay, "/rename-device", {"device": "dev1", "name": ""})

        assert status == 500
        assert "invalid device response" in body["error"]


class TestRename:
    @pytest.mark.asyncio
    async def test_rename_device_sets_name(self):
        device = make_device()
        relay = make_relay({"dev1": device})
        status, body = await post(relay, "/rename-device", {"device": "dev1", "name": "Desk-IO"})
        assert status == 200
        assert body == {"success": True}
        device.operations.set_name.assert_awaited_once_with("Desk-IO")
        device.operations.reset_name.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_rename_device_with_empty_name_resets_name(self):
        device = make_device()
        relay = make_relay({"dev1": device})
        status, response = await post(
            relay, "/rename-device", {"device": "dev1", "name": ""}
        )
        assert status == 200
        assert response == {"success": True}
        device.operations.reset_name.assert_awaited_once()
        device.operations.set_name.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("body", [{"device": "dev1"}, {"device": "dev1", "name": None}])
    async def test_rename_device_requires_string_name(self, body):
        device = make_device()
        relay = make_relay({"dev1": device})
        status, response = await post(relay, "/rename-device", body)
        assert status == 400
        assert response == {"error": "name must be a string"}
        device.operations.reset_name.assert_not_awaited()
        device.operations.set_name.assert_not_awaited()


class TestSubscribe:
    @pytest.mark.asyncio
    async def test_single_subscription(self):
        device = make_device()
        relay = make_relay({"dev1": device})
        status, body = await post(
            relay,
            "/subscribe",
            {
                "rx_device": "dev1",
                "rx_channel": 3,
                "tx_channel": "Out1",
                "tx_device": "Mixer",
            },
        )
        assert status == 200
        assert body == {"success": True}
        device.operations.add_subscription_by_name.assert_awaited_once_with(3, "Out1", "Mixer")

    @pytest.mark.asyncio
    async def test_single_subscription_missing_fields_returns_400(self):
        device = make_device()
        relay = make_relay({"dev1": device})
        status, body = await post(relay, "/subscribe", {"rx_device": "dev1", "rx_channel": 3})
        assert status == 400
        device.operations.add_subscription_by_name.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_bulk_subscriptions(self):
        device = make_device()
        relay = make_relay({"dev1": device})
        status, body = await post(
            relay,
            "/subscribe",
            {
                "rx_device": "dev1",
                "subscriptions": [
                    {"rx_channel": 1, "tx_channel": "Out1", "tx_device": "Mixer"},
                    {"rx_channel": 2, "tx_channel": "Out2", "tx_device": "Mixer"},
                ],
            },
        )
        assert status == 200
        assert body == {"success": True, "count": 2}
        device.operations.add_subscriptions_by_name.assert_awaited_once_with(
            [(1, "Out1", "Mixer"), (2, "Out2", "Mixer")]
        )

    @pytest.mark.asyncio
    async def test_bulk_subscription_malformed_entry_returns_400(self):
        device = make_device()
        relay = make_relay({"dev1": device})
        status, body = await post(
            relay,
            "/subscribe",
            {
                "rx_device": "dev1",
                "subscriptions": [{"rx_channel": 1, "tx_channel": "Out1"}],
            },
        )
        assert status == 400
        assert "invalid subscription entry" in body["error"]
        device.operations.add_subscriptions_by_name.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_bulk_subscription_empty_list_returns_400(self):
        device = make_device()
        relay = make_relay({"dev1": device})
        status, body = await post(relay, "/subscribe", {"rx_device": "dev1", "subscriptions": []})
        assert status == 400
        assert body == {"error": "subscriptions list is empty"}

    @pytest.mark.asyncio
    async def test_unknown_rx_device_returns_404(self):
        relay = make_relay()
        status, body = await post(
            relay,
            "/subscribe",
            {
                "rx_device": "ghost",
                "rx_channel": 1,
                "tx_channel": "Out1",
                "tx_device": "Mixer",
            },
        )
        assert status == 404
        assert body == {"error": "rx device not found"}


class TestUnsubscribe:
    @pytest.mark.asyncio
    async def test_single_unsubscribe(self):
        device = make_device()
        channel = SimpleNamespace(number=3)
        device.rx_channels = {3: channel}
        relay = make_relay({"dev1": device})
        status, body = await post(relay, "/unsubscribe", {"rx_device": "dev1", "rx_channel": 3})
        assert status == 200
        device.operations.remove_subscription.assert_awaited_once_with(channel)

    @pytest.mark.asyncio
    async def test_bulk_unsubscribe(self):
        device = make_device()
        channels = {1: SimpleNamespace(number=1), 2: SimpleNamespace(number=2)}
        device.rx_channels = channels
        relay = make_relay({"dev1": device})
        status, body = await post(relay, "/unsubscribe", {"rx_device": "dev1", "rx_channels": [1, 2]})
        assert status == 200
        assert body == {"success": True, "count": 2}
        device.operations.remove_subscriptions.assert_awaited_once_with([channels[1], channels[2]])

    @pytest.mark.asyncio
    async def test_bulk_unsubscribe_unknown_channel_returns_404(self):
        device = make_device()
        device.rx_channels = {1: SimpleNamespace(number=1)}
        relay = make_relay({"dev1": device})
        status, body = await post(relay, "/unsubscribe", {"rx_device": "dev1", "rx_channels": [1, 9]})
        assert status == 404
        assert body == {"error": "rx channel 9 not found"}
        device.operations.remove_subscriptions.assert_not_awaited()


class TestShutdown:
    @pytest.mark.asyncio
    async def test_shutdown_from_loopback_succeeds(self):
        on_shutdown = MagicMock()
        relay = make_relay(on_shutdown=on_shutdown)
        writer = FakeWriter(peer=("127.0.0.1", 40000))
        await relay._dispatch("POST", "/shutdown", None, writer)
        status, body = writer.response()
        assert status == 200
        assert body == {"success": True}
        on_shutdown.assert_called_once()

    @pytest.mark.asyncio
    async def test_shutdown_from_remote_peer_forbidden(self):
        on_shutdown = MagicMock()
        relay = make_relay(on_shutdown=on_shutdown)
        writer = FakeWriter(peer=("192.168.1.50", 40000))
        await relay._dispatch("POST", "/shutdown", None, writer)
        status, body = writer.response()
        assert status == 403
        on_shutdown.assert_not_called()


class TestReportUnresponsive:
    @pytest.mark.asyncio
    async def test_marks_online_device_offline(self):
        device = make_device()
        device.online = True
        relay = make_relay({"dev1": device})
        status, body = await post(relay, "/report-unresponsive", {"device": "dev1"})
        assert status == 200
        relay.application.mark_device_offline.assert_called_once_with("dev1")

    @pytest.mark.asyncio
    async def test_unknown_device_returns_404(self):
        relay = make_relay()
        status, body = await post(relay, "/report-unresponsive", {"device": "ghost"})
        assert status == 404


class TestMetering:
    @pytest.mark.asyncio
    async def test_status_without_metering_returns_empty(self):
        relay = make_relay()
        writer = FakeWriter()
        await relay._dispatch("GET", "/metering/status", None, writer)
        status, body = writer.response()
        assert status == 200
        assert body == {}

    @pytest.mark.asyncio
    async def test_status_returns_manager_status(self):
        metering = SimpleNamespace(get_status=MagicMock(return_value={"active": ["dev1"]}))
        relay = make_relay(metering=metering)
        writer = FakeWriter()
        await relay._dispatch("GET", "/metering/status", None, writer)
        status, body = writer.response()
        assert status == 200
        assert body == {"active": ["dev1"]}

    @pytest.mark.asyncio
    async def test_snapshot_unknown_device_returns_404(self):
        relay = make_relay()
        writer = FakeWriter()
        await relay._dispatch("GET", "/metering/snapshot/ghost", None, writer)
        status, body = writer.response()
        assert status == 404

    @pytest.mark.asyncio
    async def test_snapshot_without_metering_returns_503(self):
        device = make_device()
        relay = make_relay({"dev1": device})
        writer = FakeWriter()
        await relay._dispatch("GET", "/metering/snapshot/dev1", None, writer)
        status, body = writer.response()
        assert status == 503

    @pytest.mark.asyncio
    async def test_snapshot_returns_named_levels(self):
        device = make_device()
        channel = SimpleNamespace(number=1, name="ch1", friendly_name="Mic")
        device.tx_channels = {1: channel}
        device.rx_channels = {}
        metering = SimpleNamespace(
            snapshot=AsyncMock(
                return_value={"tx": {1: -20.0}, "rx": {}, "wall_time": 123.0, "source_ip": "192.168.1.50"}
            )
        )
        relay = make_relay({"dev1": device}, metering=metering)
        writer = FakeWriter()
        await relay._dispatch("GET", "/metering/snapshot/dev1", None, writer)
        status, body = writer.response()
        assert status == 200
        assert body["tx"]["1"] == {"name": "Mic", "level": -20.0}
        assert body["wall_time"] == 123.0


class TestSetInterface:
    @pytest.mark.asyncio
    async def test_invalid_mode_returns_400(self):
        device = make_device()
        relay = make_relay({"dev1": device})
        status, body = await post(relay, "/interface", {"device": "dev1", "mode": "bogus"})
        assert status == 400
        assert body == {"error": "mode must be 'dhcp' or 'static'"}

    @pytest.mark.asyncio
    async def test_static_mode_requires_ip_and_netmask(self):
        device = make_device()
        relay = make_relay({"dev1": device})
        status, body = await post(relay, "/interface", {"device": "dev1", "mode": "static"})
        assert status == 400
        assert body == {"error": "static mode requires ip, netmask"}


class BlockingReader:
    def __init__(self):
        self.disconnected = asyncio.Event()

    async def read(self, _size):
        await self.disconnected.wait()
        return b""


class NeverDrainsWriter(FakeWriter):
    async def drain(self):
        await asyncio.Event().wait()


class TestSseLifecycle:
    @pytest.mark.asyncio
    async def test_real_tcp_peer_disconnect_removes_sse_client(self, monkeypatch):
        relay = make_relay()
        relay.port = 0
        monkeypatch.setattr(relay, "_reconcile_bonjour", AsyncMock())

        await relay.start()
        try:
            port = relay.tcp_server.sockets[0].getsockname()[1]
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            writer.write(b"GET /events HTTP/1.1\r\nHost: localhost\r\n\r\n")
            await writer.drain()

            header = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=1)
            snapshot = await asyncio.wait_for(reader.readuntil(b"\n\n"), timeout=1)
            assert b"200 OK" in header
            assert b'"event": "snapshot"' in snapshot
            assert len(relay.sse_clients) == 1

            writer.close()
            await writer.wait_closed()
            for _ in range(100):
                if not relay.sse_clients:
                    break
                await asyncio.sleep(0.01)
            assert relay.sse_clients == {}
        finally:
            await relay.stop()

    @pytest.mark.asyncio
    async def test_bounded_timeout_drains_cancelled_child_finally(self):
        import netaudio.daemon.relay as relay_module

        started = asyncio.Event()
        finalized = asyncio.Event()

        async def child():
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                finalized.set()

        with pytest.raises(asyncio.TimeoutError):
            await relay_module._bounded(child(), 0.01)

        assert started.is_set()
        assert finalized.is_set()

    @pytest.mark.asyncio
    async def test_bounded_outer_cancellation_drains_cancelled_child_finally(self):
        import netaudio.daemon.relay as relay_module

        started = asyncio.Event()
        finalized = asyncio.Event()

        async def child():
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                finalized.set()

        parent = asyncio.create_task(relay_module._bounded(child(), 30))
        await started.wait()
        parent.cancel()
        with pytest.raises(asyncio.CancelledError):
            await parent

        assert finalized.is_set()

    @pytest.mark.asyncio
    async def test_snapshot_and_events_are_fifo_per_client(self):
        relay = make_relay()
        writer = FakeWriter()
        reader = BlockingReader()
        handler = asyncio.create_task(relay._handle_sse(writer, reader))

        for _ in range(20):
            if relay.sse_clients:
                break
            await asyncio.sleep(0)
        assert len(relay.sse_clients) == 1

        await relay._broadcast_sse({"event": "update", "sequence": 1})
        await relay._broadcast_sse({"event": "update", "sequence": 2})
        for _ in range(20):
            if b'"sequence": 2' in writer.data:
                break
            await asyncio.sleep(0)

        reader.disconnected.set()
        await handler

        payload = bytes(writer.data)
        assert payload.index(b'"event": "snapshot"') < payload.index(b'"sequence": 1')
        assert payload.index(b'"sequence": 1') < payload.index(b'"sequence": 2')
        assert relay.sse_clients == {}
        assert writer.closed is True

    @pytest.mark.asyncio
    async def test_full_client_queue_disconnects_only_that_client(self):
        import netaudio.daemon.relay as relay_module

        relay = make_relay()
        slow_writer = FakeWriter()
        fast_writer = FakeWriter()
        slow = relay_module._SseClient(slow_writer, queue=asyncio.Queue(maxsize=1))
        fast = relay_module._SseClient(fast_writer, queue=asyncio.Queue(maxsize=2))
        slow.queue.put_nowait(b"already queued")
        relay.sse_clients = {slow_writer: slow, fast_writer: fast}

        await relay._broadcast_sse({"event": "device_updated"})

        assert slow_writer not in relay.sse_clients
        assert slow.closed.is_set()
        assert slow_writer.closed is True
        assert relay.sse_clients[fast_writer] is fast
        assert b"device_updated" in fast.queue.get_nowait()

    @pytest.mark.asyncio
    async def test_timed_out_writer_is_removed_without_blocking_broadcast(self, monkeypatch):
        import netaudio.daemon.relay as relay_module

        monkeypatch.setattr(relay_module, "SSE_DRAIN_TIMEOUT_SECONDS", 0.01)
        relay = make_relay()
        writer = NeverDrainsWriter()
        client = relay_module._SseClient(writer)
        relay.sse_clients[writer] = client
        client.sender_task = asyncio.create_task(relay._sse_sender(client))

        await relay._broadcast_sse({"event": "meter_values"})
        await asyncio.wait_for(client.closed.wait(), timeout=0.2)

        assert writer not in relay.sse_clients
        assert writer.closed is True
        await asyncio.gather(client.sender_task, return_exceptions=True)

    @pytest.mark.asyncio
    async def test_cancelled_handler_cleans_reader_sender_and_writer(self):
        relay = make_relay()
        writer = FakeWriter()
        reader = BlockingReader()
        handler = asyncio.create_task(relay._handle_sse(writer, reader))
        for _ in range(20):
            if relay.sse_clients:
                break
            await asyncio.sleep(0)

        handler.cancel()
        with pytest.raises(asyncio.CancelledError):
            await handler

        assert relay.sse_clients == {}
        assert writer.closed is True

    @pytest.mark.asyncio
    async def test_stop_is_idempotent_and_closes_client_tasks(self):
        import netaudio.daemon.relay as relay_module

        relay = make_relay()
        relay.tcp_server = FakeTcpServer()
        writer = NeverDrainsWriter()
        client = relay_module._SseClient(writer)
        relay.sse_clients[writer] = client
        client.sender_task = asyncio.create_task(relay._sse_sender(client))
        client.queue.put_nowait(b"pending")

        await relay.stop()
        await relay.stop()

        assert relay.tcp_server is None
        assert relay.sse_clients == {}
        assert writer.closed is True
        assert client.sender_task.done()


class FakeTcpServer:
    def __init__(self):
        self.closed = False
        self.wait_closed_called = False

    def close(self):
        self.closed = True

    async def wait_closed(self):
        self.wait_closed_called = True


class FakeAsyncZeroconf:
    instances = []
    fail_next_register = False
    rename_next_register = False

    def __init__(self, **kwargs):
        self.options = kwargs
        self.registered = []
        self.register_options = []
        self.updated = []
        self.unregistered = []
        self.closed = False
        FakeAsyncZeroconf.instances.append(self)

    async def async_register_service(self, service_info, **kwargs):
        self.register_options.append(kwargs)
        if FakeAsyncZeroconf.fail_next_register:
            FakeAsyncZeroconf.fail_next_register = False
            raise RuntimeError("register failed")
        if FakeAsyncZeroconf.rename_next_register:
            FakeAsyncZeroconf.rename_next_register = False
            service_info.name = f"netaudio-relay conflict-2.{service_info.type}"
        self.registered.append(service_info)

    async def async_update_service(self, service_info):
        self.updated.append(service_info)

    async def async_unregister_service(self, service_info):
        self.unregistered.append(service_info)

    async def async_close(self):
        self.closed = True


def _adapter(*addresses, name="en0"):
    return SimpleNamespace(
        nice_name=name,
        ips=[SimpleNamespace(ip=address) for address in addresses],
    )


@pytest.fixture
def bonjour_relay(monkeypatch):
    import netaudio.daemon.relay as relay_module

    FakeAsyncZeroconf.instances = []
    FakeAsyncZeroconf.fail_next_register = False
    FakeAsyncZeroconf.rename_next_register = False

    async def fake_start_server(*_args, **_kwargs):
        return FakeTcpServer()

    monkeypatch.setattr(relay_module.asyncio, "start_server", fake_start_server)
    monkeypatch.setattr(relay_module, "AsyncZeroconf", FakeAsyncZeroconf)
    monkeypatch.setattr(relay_module.app_settings, "_interface", None, raising=False)
    monkeypatch.setattr(relay_module.app_settings, "_interface_ip", None, raising=False)

    return make_relay()


class TestBonjourReconcile:
    @pytest.mark.asyncio
    async def test_start_registers_all_non_loopback_ipv4_addresses(self, bonjour_relay, monkeypatch):
        import netaudio.daemon.relay as relay_module

        monkeypatch.setattr(
            relay_module.ifaddr,
            "get_adapters",
            lambda: [
                _adapter("127.0.0.1"),
                _adapter("192.168.1.44", "169.254.9.10"),
            ],
        )

        await bonjour_relay.start()

        assert FakeAsyncZeroconf.instances[0].registered
        service_info = FakeAsyncZeroconf.instances[0].registered[0]
        assert set(service_info.parsed_addresses()) == {"192.168.1.44", "169.254.9.10"}
        assert FakeAsyncZeroconf.instances[0].options == {
            "interfaces": ["169.254.9.10", "192.168.1.44"],
            "ip_version": relay_module.IPVersion.V4Only,
        }
        assert FakeAsyncZeroconf.instances[0].register_options == [{"allow_name_change": True}]
        assert service_info.port == bonjour_relay.port
        assert service_info.server.endswith(".local.")

        await bonjour_relay.stop()

    @pytest.mark.asyncio
    async def test_reregisters_when_advertised_addresses_change(self, bonjour_relay, monkeypatch):
        monkeypatch.setattr(bonjour_relay, "_get_advertisement_addresses", lambda: ("192.168.1.44",))
        await bonjour_relay._reconcile_bonjour(force=True)

        original_zeroconf = bonjour_relay.zeroconf
        original_service_info = bonjour_relay.service_info

        monkeypatch.setattr(
            bonjour_relay,
            "_get_advertisement_addresses",
            lambda: ("10.0.0.20", "192.168.1.44"),
        )

        await bonjour_relay._reconcile_bonjour()

        assert original_zeroconf.unregistered == [original_service_info]
        assert original_zeroconf.closed is True
        assert bonjour_relay.service_info is not None
        assert set(bonjour_relay.service_info.parsed_addresses()) == {"192.168.1.44", "10.0.0.20"}
        assert len(FakeAsyncZeroconf.instances) == 2

        await bonjour_relay._close_bonjour()

    @pytest.mark.asyncio
    async def test_address_loss_unpublishes_and_later_recovers(self, bonjour_relay, monkeypatch):
        addresses = ["192.168.1.44"]
        monkeypatch.setattr(bonjour_relay, "_get_advertisement_addresses", lambda: tuple(addresses))

        await bonjour_relay._reconcile_bonjour(force=True)
        first_zeroconf = bonjour_relay.zeroconf
        first_service = bonjour_relay.service_info

        addresses.clear()
        await bonjour_relay._reconcile_bonjour()

        assert first_zeroconf.unregistered == [first_service]
        assert first_zeroconf.closed is True
        assert bonjour_relay.zeroconf is None
        assert bonjour_relay.service_info is None

        addresses.append("10.0.0.20")
        await bonjour_relay._reconcile_bonjour()

        assert bonjour_relay.zeroconf is not None
        assert bonjour_relay.zeroconf is not first_zeroconf
        assert bonjour_relay.service_info.parsed_addresses() == ["10.0.0.20"]
        await bonjour_relay._close_bonjour()

    @pytest.mark.asyncio
    async def test_selected_interface_limits_registration_addresses(self, bonjour_relay, monkeypatch):
        import netaudio.daemon.relay as relay_module

        monkeypatch.setattr(relay_module.app_settings, "_interface", "en7", raising=False)
        monkeypatch.setattr(
            relay_module.ifaddr,
            "get_adapters",
            lambda: [
                _adapter("192.168.1.44", name="en0"),
                _adapter("10.0.0.20", "127.0.0.1", name="en7"),
            ],
        )

        await bonjour_relay._reconcile_bonjour(force=True)

        assert bonjour_relay._bonjour_addresses == ("10.0.0.20",)
        assert FakeAsyncZeroconf.instances[0].options["interfaces"] == ["10.0.0.20"]
        await bonjour_relay._close_bonjour()

    @pytest.mark.asyncio
    async def test_conflict_renamed_service_keeps_registered_name_on_update(self, bonjour_relay, monkeypatch):
        monkeypatch.setattr(bonjour_relay, "_get_advertisement_addresses", lambda: ("192.168.1.44",))
        FakeAsyncZeroconf.rename_next_register = True

        await bonjour_relay._reconcile_bonjour(force=True)
        registered_name = bonjour_relay.service_info.name
        assert "conflict-2" in registered_name

        await bonjour_relay._publish_bonjour(
            ("192.168.1.44",),
            reason="periodic refresh",
            recreate_service=False,
        )

        assert FakeAsyncZeroconf.instances[0].updated[0].name == registered_name
        assert bonjour_relay.service_info.name == registered_name
        assert len(FakeAsyncZeroconf.instances) == 1
        await bonjour_relay._close_bonjour()

    @pytest.mark.asyncio
    async def test_start_failure_closes_tcp_server_and_unregisters_listeners(self, bonjour_relay, monkeypatch):
        import netaudio.daemon.relay as relay_module

        monkeypatch.setattr(bonjour_relay, "_get_advertisement_addresses", lambda: ("192.168.1.44",))
        monkeypatch.setattr(
            relay_module,
            "AsyncZeroconf",
            lambda **kwargs: (_ for _ in ()).throw(RuntimeError("constructor failed")),
        )

        with pytest.raises(RuntimeError, match="constructor failed"):
            await bonjour_relay.start()

        assert bonjour_relay.tcp_server is None
        assert bonjour_relay._events_registered is False
        assert bonjour_relay.application.dispatcher.off.call_count == 8


class TestTxFlows:
    def test_flow_detection_excludes_incompatible_2809_layout(self):
        from netaudio.dante.const import FLOW_PROTOCOL_IDS

        assert FLOW_PROTOCOL_IDS == (0x2729, 0x2801)

    @staticmethod
    def _flow(slot=17, flow_type="multicast", channels=None):
        return {
            "flow_number": slot,
            "flow_type": flow_type,
            "sample_rate": 48000,
            "encoding": 24,
            "frames_per_packet": 48,
            "channel_count": len(channels or [1, 2]),
            "channels": channels or [1, 2],
        }

    @staticmethod
    def _mock_api(monkeypatch, device_flows=None, max_flow_slots=32):
        import netaudio.daemon.relay as relay_module

        detect = AsyncMock(return_value=0x2729)
        query = AsyncMock(return_value={"max_flow_slots": max_flow_slots, "flows": device_flows or []})
        create = AsyncMock(return_value=0x0001)
        delete = AsyncMock(return_value=0x0001)
        monkeypatch.setattr(relay_module.flows, "detect_flow_protocol", detect)
        monkeypatch.setattr(relay_module.flows, "query_tx_flow_inventory", query)
        monkeypatch.setattr(relay_module.flows, "create_tx_flow", create)
        monkeypatch.setattr(relay_module.flows, "delete_tx_flow", delete)
        return detect, query, create, delete

    @pytest.mark.asyncio
    async def test_lists_device_tx_flows(self, monkeypatch):
        device = make_device()
        flow = self._flow()
        detect, query, _, _ = self._mock_api(monkeypatch, [flow])
        relay = make_relay({"dev1": device})

        status, body = await get(relay, "/flows/dev1")

        assert status == 200
        assert body == {
            "device": "dev1",
            "flow_protocol_id": 0x2729,
            "max_flow_slots": 32,
            "flows": [flow],
        }
        detect.assert_awaited_once_with("192.168.1.50", 4440)
        query.assert_awaited_once_with("192.168.1.50", 4440, 0x2729)

    @pytest.mark.asyncio
    async def test_create_requires_explicit_confirmation(self, monkeypatch):
        device = make_device()
        device.tx_channels = {1: SimpleNamespace(number=1)}
        _, query, create, _ = self._mock_api(monkeypatch)
        relay = make_relay({"dev1": device})

        status, body = await post(relay, "/flows/create", {"device": "dev1", "flow_slot": 17, "channels": [1]})

        assert status == 400
        assert body == {"error": "confirmed must be true"}
        query.assert_not_awaited()
        create.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_create_rejects_unknown_channel_and_occupied_slot(self, monkeypatch):
        device = make_device()
        device.tx_channels = {1: SimpleNamespace(number=1)}
        _, _, create, _ = self._mock_api(monkeypatch, [self._flow(slot=17, channels=[1])])
        relay = make_relay({"dev1": device})

        status, body = await post(
            relay, "/flows/create", {"device": "dev1", "flow_slot": 18, "channels": [2], "confirmed": True}
        )
        assert status == 404
        assert body == {"error": "tx channel not found: 2"}

        status, body = await post(
            relay, "/flows/create", {"device": "dev1", "flow_slot": 17, "channels": [1], "confirmed": True}
        )
        assert status == 409
        assert body == {"error": "flow slot 17 is already in use"}
        create.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_create_rejects_slot_above_device_capacity(self, monkeypatch):
        device = make_device()
        device.tx_channels = {1: SimpleNamespace(number=1)}
        _, _, create, _ = self._mock_api(monkeypatch, max_flow_slots=4)
        relay = make_relay({"dev1": device})

        status, body = await post(
            relay, "/flows/create", {"device": "dev1", "flow_slot": 5, "channels": [1], "confirmed": True}
        )

        assert status == 409
        assert body == {"error": "flow slot 5 exceeds the device capacity of 4"}
        create.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_create_sends_guarded_channel_selection(self, monkeypatch):
        device = make_device()
        device.tx_channels = {
            1: SimpleNamespace(number=1),
            2: SimpleNamespace(number=2),
        }
        _, _, create, _ = self._mock_api(monkeypatch)
        relay = make_relay({"dev1": device})

        status, body = await post(
            relay, "/flows/create", {"device": "dev1", "flow_slot": 17, "channels": [1, 2], "confirmed": True}
        )

        assert status == 200
        assert body["success"] is True
        create.assert_awaited_once_with("192.168.1.50", 4440, 0x2729, 17, [1, 2])

    @pytest.mark.asyncio
    async def test_delete_only_allows_an_existing_multicast_flow(self, monkeypatch):
        device = make_device()
        _, _, _, delete = self._mock_api(monkeypatch, [self._flow(flow_type="0x0001")])
        relay = make_relay({"dev1": device})

        status, body = await post(relay, "/flows/delete", {"device": "dev1", "flow_slot": 17, "confirmed": True})

        assert status == 409
        assert body == {"error": "flow slot 17 is not multicast"}
        delete.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_delete_sends_confirmed_multicast_slot(self, monkeypatch):
        device = make_device()
        _, _, _, delete = self._mock_api(monkeypatch, [self._flow()])
        relay = make_relay({"dev1": device})

        status, body = await post(relay, "/flows/delete", {"device": "dev1", "flow_slot": 17, "confirmed": True})

        assert status == 200
        assert body["success"] is True
        delete.assert_awaited_once_with("192.168.1.50", 4440, 0x2729, 17)


class TestBonjourRecovery:
    @pytest.mark.asyncio
    async def test_retries_after_failed_bonjour_registration(self, bonjour_relay, monkeypatch):
        monkeypatch.setattr(bonjour_relay, "_get_advertisement_addresses", lambda: ("192.168.1.44",))

        FakeAsyncZeroconf.fail_next_register = True
        await bonjour_relay._reconcile_bonjour(force=True)

        assert bonjour_relay.zeroconf is None
        assert bonjour_relay.service_info is None
        assert len(FakeAsyncZeroconf.instances[0].unregistered) == 1
        assert FakeAsyncZeroconf.instances[0].closed is True

        await bonjour_relay._reconcile_bonjour()

        assert bonjour_relay.zeroconf is not None
        assert bonjour_relay.service_info is not None
        assert bonjour_relay.service_info.parsed_addresses() == ["192.168.1.44"]

        await bonjour_relay._close_bonjour()

    @pytest.mark.asyncio
    async def test_reregisters_after_wake_gap_even_when_address_is_unchanged(self, bonjour_relay, monkeypatch):
        monkeypatch.setattr(bonjour_relay, "_get_advertisement_addresses", lambda: ("192.168.1.44",))
        await bonjour_relay._reconcile_bonjour(force=True)

        original_zeroconf = bonjour_relay.zeroconf
        original_service_info = bonjour_relay.service_info

        await bonjour_relay._reconcile_bonjour(woke_from_sleep=True)

        assert original_zeroconf.unregistered == [original_service_info]
        assert original_zeroconf.closed is True
        assert bonjour_relay.zeroconf is not original_zeroconf
        assert bonjour_relay.service_info is not None
        assert bonjour_relay.service_info.parsed_addresses() == ["192.168.1.44"]

        await bonjour_relay._close_bonjour()
