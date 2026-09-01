import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from tests.http_api_test_support import FakeWriter, get, make_device, make_http_server, post


class TestRouting:
    @pytest.mark.asyncio
    async def test_unknown_path_returns_404(self):
        http_server = make_http_server()
        status, body = await post(http_server, "/nonexistent", {"device": "x"})
        assert status == 404
        assert body == {"error": "not found"}

    @pytest.mark.asyncio
    async def test_unknown_method_returns_404(self):
        http_server = make_http_server()
        writer = FakeWriter()
        await http_server._dispatch("DELETE", "/devices", None, writer)
        status, body = writer.response()
        assert status == 404

    @pytest.mark.asyncio
    async def test_missing_body_returns_400(self):
        http_server = make_http_server()
        status, body = await post(http_server, "/subscribe", None)
        assert status == 400
        assert body == {"error": "missing body"}

    @pytest.mark.asyncio
    async def test_malformed_json_returns_400(self):
        http_server = make_http_server()
        status, body = await post(http_server, "/subscribe", b"{not json")
        assert status == 400
        assert "invalid json" in body["error"]

    @pytest.mark.asyncio
    async def test_non_object_body_returns_400(self):
        http_server = make_http_server()
        status, body = await post(http_server, "/subscribe", b'["a", "b"]')
        assert status == 400
        assert body == {"error": "body must be a json object"}

    @pytest.mark.asyncio
    async def test_refresh_allows_missing_body(self):
        http_server = make_http_server()
        status, body = await post(http_server, "/refresh", None)
        assert status == 200
        assert body == {"success": True}
        http_server.state.refresh_all_devices.assert_awaited_once()


class TestErrorMapping:
    @pytest.mark.asyncio
    async def test_handler_exception_returns_500(self):
        device = make_device()
        device.operations.identify.side_effect = RuntimeError("socket exploded")
        http_server = make_http_server({"dev1": device})
        writer = FakeWriter()
        await http_server._route("POST", "/identify", json.dumps({"device": "dev1"}).encode(), writer, None)
        status, body = writer.response()
        assert status == 500
        assert body == {"error": "socket exploded"}

    @pytest.mark.asyncio
    async def test_handler_timeout_returns_504(self):
        device = make_device()
        device.operations.set_latency.side_effect = TimeoutError()
        http_server = make_http_server({"dev1": device})
        writer = FakeWriter()
        await http_server._route(
            "POST", "/set-latency", json.dumps({"device": "dev1", "latency": 5}).encode(), writer, None
        )
        status, body = writer.response()
        assert status == 504
        assert body == {"error": "device did not respond"}


class TestMutationVerification:
    @pytest.mark.asyncio
    async def test_arc_mutations_report_device_rejection(self):
        device = make_device()
        device.operations.set_latency.return_value = bytes.fromhex("27ff000a000010010600")
        http_server = make_http_server({"dev1": device})

        status, response = await post(http_server, "/set-latency", {"device": "dev1", "latency": 1.0})

        assert status == 409
        assert response["result_code"] == 0x0600

    @pytest.mark.asyncio
    async def test_gain_mutation_requires_matching_multicast_readback(self):
        device = make_device()
        http_server = make_http_server({"dev1": device})

        status, response = await post(
            http_server,
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
        http_server = make_http_server({"dev1": device})

        status, response = await post(
            http_server,
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
        http_server = make_http_server({"dev1": device})

        status, response = await post(
            http_server,
            "/set-gain",
            {"device": "dev1", "channel_number": 1, "gain_level": 3, "device_type": "input"},
        )

        assert status == 504
        assert response == {"error": "gain readback was unavailable"}

    @pytest.mark.parametrize(
        ("path", "body", "method_name", "probe_name", "expected_status"),
        [
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
        http_server = make_http_server({"dev1": device})
        getattr(http_server.application, probe_name).return_value = expected_status

        status, response = await post(http_server, path, body)

        assert status == 200
        assert response == {"success": True}
        requested_value = next(value for field_name, value in body.items() if field_name != "device")
        getattr(device.operations, method_name).assert_awaited_once_with(requested_value)
        getattr(http_server.application, probe_name).assert_awaited_once_with("192.168.1.50")

    @pytest.mark.parametrize(
        ("path", "body", "method_name", "probe_name", "capability_name", "old_status", "requested_status"),
        [
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
        http_server = make_http_server({"dev1": device})
        old_status_observed = asyncio.Event()

        async def probe_status(_device_ip_address):
            http_server.application.notifications._notify_capability_value_waiters(
                capability_name,
                "192.168.1.50",
                old_status[0],
                old_status[1],
            )
            old_status_observed.set()
            return old_status

        setattr(http_server.application, probe_name, AsyncMock(side_effect=probe_status))
        request_task = asyncio.create_task(post(http_server, path, body))
        await old_status_observed.wait()

        assert not request_task.done()
        http_server.application.notifications._notify_capability_value_waiters(
            capability_name,
            "192.168.1.99",
            requested_status[0],
            requested_status[1],
        )
        http_server.application.notifications._notify_capability_value_waiters(
            "encoding" if capability_name == "sample_rate" else "sample_rate",
            "192.168.1.50",
            requested_status[0],
            requested_status[1],
        )
        assert not request_task.done()
        http_server.application.notifications._notify_capability_value_waiters(
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
        http_server = make_http_server({"dev1": device})
        getattr(http_server.application, probe_name).return_value = (observed, supported)

        status, response = await post(http_server, path, body)

        assert status == 409
        assert response["observed"] == observed
        assert response["supported"] == supported

    @pytest.mark.parametrize(
        ("path", "body", "probe_name", "description"),
        [
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
        http_server = make_http_server({"dev1": device})
        getattr(http_server.application, probe_name).return_value = None

        status, response = await post(http_server, path, body)

        assert status == 504
        assert response == {"error": f"{description} readback was unavailable"}

    @pytest.mark.parametrize(
        ("path", "body", "method_name", "probe_name"),
        [
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
        http_server = make_http_server({"dev1": device})

        status, response = await post(http_server, path, body)

        assert status == 409
        assert response == {"error": "requested value is not supported"}
        getattr(http_server.application, probe_name).assert_not_awaited()

    @pytest.mark.asyncio
    async def test_sample_rate_change_uses_topology_safe_application_operation(self):
        device = make_device()
        http_server = make_http_server({"dev1": device})

        status, response = await post(
            http_server,
            "/set-sample-rate",
            {"device": "dev1", "sample_rate": 96_000, "confirm_destructive": True},
        )

        assert status == 200
        assert response == {
            "success": True,
            "changed": True,
            "preflight": {"target_sample_rate_hertz": 96_000},
            "readback": {"sample_rate_hertz": 96_000},
        }
        http_server.application.set_sample_rate_state.assert_awaited_once_with(
            device,
            96_000,
            confirm_destructive=True,
            timeout=http_server.audio_capability_verification_timeout,
        )
        device.operations.set_sample_rate.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_sample_rate_change_returns_preflight_when_confirmation_is_required(self):
        from netaudio.dante.sample_rate_topology import SampleRateTopologyConfirmationRequired

        device = make_device()
        http_server = make_http_server({"dev1": device})
        preflight = SimpleNamespace(to_dict=lambda: {"destructive_transmitter_membership_loss": [{"flow": 7}]})
        http_server.application.set_sample_rate_state.side_effect = SampleRateTopologyConfirmationRequired(
            "explicit confirmation is required",
            preflight,
        )

        status, response = await post(
            http_server,
            "/set-sample-rate",
            {"device": "dev1", "sample_rate": 192_000},
        )

        assert status == 409
        assert response == {
            "error": "explicit confirmation is required",
            "preflight": {"destructive_transmitter_membership_loss": [{"flow": 7}]},
        }

    @pytest.mark.asyncio
    async def test_sample_rate_change_maps_unavailable_fresh_inventory_to_gateway_timeout(self):
        from netaudio.dante.sample_rate_topology import SampleRateTopologyReadbackError

        device = make_device()
        http_server = make_http_server({"dev1": device})
        http_server.application.set_sample_rate_state.side_effect = SampleRateTopologyReadbackError(
            "fresh transmitter-flow inventory did not respond"
        )

        status, response = await post(
            http_server,
            "/set-sample-rate",
            {"device": "dev1", "sample_rate": 96_000},
        )

        assert status == 504
        assert response == {"error": "fresh transmitter-flow inventory did not respond"}

    @pytest.mark.asyncio
    async def test_sample_rate_change_rejects_non_boolean_confirmation(self):
        device = make_device()
        http_server = make_http_server({"dev1": device})

        status, response = await post(
            http_server,
            "/set-sample-rate",
            {"device": "dev1", "sample_rate": 96_000, "confirm_destructive": "yes"},
        )

        assert status == 400
        assert response == {"error": "confirm_destructive must be a boolean"}
        http_server.application.set_sample_rate_state.assert_not_awaited()

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
        http_server = make_http_server({"dev1": device})

        status, response = await post(http_server, path, body)

        assert status == 400
        assert "must be an integer" in response["error"]
        device.operations.set_sample_rate.assert_not_awaited()
        device.operations.set_encoding.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_subscription_rejection_is_not_reported_as_success(self):
        device = make_device()
        device.operations.add_subscription_by_name.return_value = bytes.fromhex("27ff000a000010010600")
        http_server = make_http_server({"dev1": device})

        status, response = await post(
            http_server,
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
        http_server = make_http_server({"dev1": device})

        status, response = await post(http_server, "/unsubscribe", {"rx_device": "dev1", "rx_channel": 1})

        assert status == 504
        assert response == {"error": "device did not respond"}

    @pytest.mark.asyncio
    async def test_preferred_leader_mismatch_is_conflict(self):
        device = make_device()
        http_server = make_http_server({"dev1": device})
        http_server.application.set_preferred_leader_state.side_effect = None
        http_server.application.set_preferred_leader_state.return_value = False

        status, response = await post(
            http_server,
            "/set-preferred-leader",
            {"device": "dev1", "preferred": True},
        )

        assert status == 409
        assert response["observed"] is False

    @pytest.mark.asyncio
    async def test_aes67_missing_readback_is_gateway_timeout(self):
        device = make_device()
        http_server = make_http_server({"dev1": device})
        http_server.application.set_aes67_state.side_effect = None
        http_server.application.set_aes67_state.return_value = None

        status, response = await post(http_server, "/set-aes67", {"device": "dev1", "enabled": True})

        assert status == 504
        assert response == {"error": "AES67 readback was unavailable"}

    @pytest.mark.asyncio
    async def test_aes67_rejects_device_without_directory_property(self):
        device = make_device()
        device.aes67_supported = False
        http_server = make_http_server({"dev1": device})

        status, response = await post(http_server, "/set-aes67", {"device": "dev1", "enabled": True})

        assert status == 409
        assert response == {"error": "device does not support AES67 configuration"}
        http_server.application.set_aes67_state.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_aes67_multicast_prefix_write_is_verified(self):
        device = make_device()
        device.aes67_multicast_prefix = "239.69.0.0"
        http_server = make_http_server({"dev1": device})

        status, response = await post(
            http_server,
            "/set-aes67-multicast-prefix",
            {"device": "dev1", "prefix": "239.238.0.0"},
        )

        assert status == 200
        assert response == {"success": True, "prefix": "239.238.0.0"}
        http_server.application.set_aes67_multicast_prefix_state.assert_awaited_once_with(device, "239.238.0.0")

    @pytest.mark.asyncio
    async def test_aes67_multicast_prefix_rejects_device_without_property(self):
        device = make_device()
        http_server = make_http_server({"dev1": device})

        status, response = await post(
            http_server,
            "/set-aes67-multicast-prefix",
            {"device": "dev1", "prefix": "239.238.0.0"},
        )

        assert status == 409
        assert response == {"error": "device does not advertise an AES67 multicast prefix"}
        http_server.application.set_aes67_multicast_prefix_state.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_sample_rate_pullup_write_is_verified(self):
        device = make_device()
        device.supported_sample_rate_pullup_raw_values = [0, 1, 2, 3, 4]
        http_server = make_http_server({"dev1": device})

        status, response = await post(
            http_server,
            "/set-sample-rate-pullup",
            {"device": "dev1", "value": "+4.1667%"},
        )

        assert status == 200
        assert response == {"success": True, "raw_value": 1, "supported": [0, 1, 2, 3, 4]}
        http_server.application.set_sample_rate_pullup_state.assert_awaited_once_with(device, 1)

    @pytest.mark.asyncio
    async def test_sample_rate_pullup_missing_readback_is_gateway_timeout(self):
        device = make_device()
        http_server = make_http_server({"dev1": device})
        http_server.application.set_sample_rate_pullup_state.side_effect = None
        http_server.application.set_sample_rate_pullup_state.return_value = None

        status, response = await post(
            http_server,
            "/set-sample-rate-pullup",
            {"device": "dev1", "raw_value": 0},
        )

        assert status == 504
        assert response == {"error": "sample-rate pull-up readback was unavailable"}

    @pytest.mark.asyncio
    async def test_clock_source_write_is_verified(self):
        device = make_device()
        http_server = make_http_server({"dev1": device})

        status, response = await post(
            http_server,
            "/set-clock-source",
            {"device": "dev1", "clock_source": 57044},
        )

        assert status == 200
        assert response == {"success": True, "clock_source": 57044}
        http_server.application.set_clock_source_state.assert_awaited_once_with(device, 57044)

    @pytest.mark.asyncio
    async def test_clock_subdomain_write_is_verified(self):
        device = make_device()
        http_server = make_http_server({"dev1": device})

        status, response = await post(
            http_server,
            "/set-clock-subdomain",
            {"device": "dev1", "subdomain": "_DFLT"},
        )

        assert status == 200
        assert response == {"success": True, "subdomain": "_DFLT"}
        http_server.application.set_clock_subdomain_state.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_refresh_clock_returns_raw_clock_source(self):
        device = make_device()
        http_server = make_http_server({"dev1": device})

        status, response = await post(http_server, "/refresh-clock", {"device": "dev1"})

        assert status == 200
        assert response["clock_source_code"] == 0
        assert response["clock_subdomain_label"] == "unset"
        http_server.application.probe_clocking_status.assert_awaited_once_with(device)

    @pytest.mark.asyncio
    async def test_reboot_is_reported_as_accepted_but_unverified(self):
        device = make_device()
        http_server = make_http_server({"dev1": device})

        status, response = await post(http_server, "/reboot", {"device": "dev1"})

        assert status == 202
        assert response == {"accepted": True, "verified": False}
        device.operations.reboot.assert_awaited_once_with()

    @pytest.mark.asyncio
    async def test_interface_mismatch_is_conflict(self):
        device = make_device()
        http_server = make_http_server({"dev1": device})
        http_server.application.set_interface_dhcp.return_value = [{"mode": "static"}]

        status, response = await post(http_server, "/interface", {"device": "dev1", "mode": "dhcp"})

        assert status == 409
        assert response["interfaces"] == [{"mode": "static"}]

    @pytest.mark.asyncio
    async def test_applied_interface_reports_no_reboot(self):
        device = make_device()
        http_server = make_http_server({"dev1": device})

        status, response = await post(http_server, "/interface", {"device": "dev1", "mode": "dhcp"})

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
        http_server = make_http_server({"dev1": device})
        http_server.application.set_interface_dhcp.return_value = [{"mode": "static"}]

        status, response = await post(http_server, "/interface", {"device": "dev1", "mode": "dhcp"})

        assert status == 200
        assert response == {
            "success": True,
            "reboot_required": True,
            "interfaces": [{"mode": "static"}],
        }


class TestDeviceLookup:
    @pytest.mark.asyncio
    async def test_interface_status_is_probed_on_demand(self):
        device = make_device()
        device.link_speed_mbps = 100
        http_server = make_http_server({"dev1": device})

        status, body = await get(http_server, "/interfaces/Device1")

        assert status == 200
        assert body == {
            "device": "dev1",
            "interfaces": [{"mode": "dynamic", "ip_address": "192.168.1.50"}],
            "link_speed_mbps": 100,
            "reboot_required": False,
            "pending_config": None,
        }
        http_server.application.probe_interface_status.assert_awaited_once_with("192.168.1.50")
        assert device.interfaces == body["interfaces"]

    @pytest.mark.asyncio
    async def test_interface_status_refuses_offline_device_without_waiting(self):
        device = make_device()
        device.online = False
        http_server = make_http_server({"dev1": device})

        status, body = await get(http_server, "/interfaces/dev1")

        assert status == 409
        assert body == {"error": "device is offline"}
        http_server.application.probe_interface_status.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_interface_status_timeout_is_not_reported_as_empty_success(self):
        device = make_device()
        http_server = make_http_server({"dev1": device})
        http_server.application.probe_interface_status.return_value = None

        status, body = await get(http_server, "/interfaces/dev1")

        assert status == 504
        assert body == {"error": "interface status was not reported"}

    @pytest.mark.asyncio
    async def test_unknown_device_returns_404(self):
        http_server = make_http_server()
        status, body = await post(http_server, "/identify", {"device": "ghost"})
        assert status == 404
        assert body == {"error": "device not found"}

    @pytest.mark.asyncio
    async def test_lookup_by_friendly_name_case_insensitive(self):
        device = make_device(server_name="dev1", name="Studio-AVIO")
        http_server = make_http_server({"dev1": device})
        status, body = await post(http_server, "/identify", {"device": "studio-avio"})
        assert status == 202
        assert body == {"accepted": True, "verified": False}
        device.operations.identify.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_lookup_by_ip_address(self):
        device = make_device(ipv4="192.168.1.50")
        http_server = make_http_server({"dev1": device})
        status, body = await post(http_server, "/identify", {"device": "192.168.1.50"})
        assert status == 202
        assert body == {"accepted": True, "verified": False}


class TestRenameReset:
    @pytest.mark.asyncio
    async def test_blank_device_name_uses_protocol_reset(self):
        device = make_device()
        http_server = make_http_server({"dev1": device})

        status, body = await post(http_server, "/rename-device", {"device": "dev1", "name": " \t"})

        assert status == 200
        assert body == {"success": True}
        device.operations.reset_name.assert_awaited_once_with()
        device.operations.set_name.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_nonblank_device_name_is_preserved(self):
        device = make_device()
        http_server = make_http_server({"dev1": device})

        status, _ = await post(http_server, "/rename-device", {"device": "dev1", "name": "  Stage Rack  "})

        assert status == 200
        device.operations.set_name.assert_awaited_once_with("  Stage Rack  ")
        device.operations.reset_name.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_blank_channel_name_uses_protocol_reset(self):
        device = make_device()
        http_server = make_http_server({"dev1": device})

        status, body = await post(
            http_server, "/rename-channel", {"device": "dev1", "channel_type": "tx", "channel_number": 3, "name": ""}
        )

        assert status == 200
        assert body == {"success": True}
        device.operations.reset_channel_name.assert_awaited_once_with("tx", 3)
        device.operations.set_channel_name.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_locked_name_reset_reports_device_rejection(self):
        device = make_device()
        device.operations.reset_name.return_value = bytes.fromhex("27ff000a000010010600")
        http_server = make_http_server({"dev1": device})

        status, body = await post(http_server, "/rename-device", {"device": "dev1", "name": ""})

        assert status == 409
        assert body["result_code"] == 0x0600
        assert "0x0600" in body["error"]

    @pytest.mark.asyncio
    async def test_name_reset_timeout_is_not_reported_as_success(self):
        device = make_device()
        device.operations.reset_name.return_value = None
        http_server = make_http_server({"dev1": device})

        status, body = await post(http_server, "/rename-device", {"device": "dev1", "name": ""})

        assert status == 504
        assert body == {"error": "device did not respond"}

    @pytest.mark.asyncio
    async def test_malformed_name_reset_response_is_not_reported_as_success(self):
        device = make_device()
        device.operations.reset_name.return_value = b"short"
        http_server = make_http_server({"dev1": device})

        status, body = await post(http_server, "/rename-device", {"device": "dev1", "name": ""})

        assert status == 500
        assert "invalid device response" in body["error"]
