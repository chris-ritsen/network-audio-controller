from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from tests.http_api_test_support import make_device, make_http_server, post


def _result(state="confirmed", effective=True, *, persistence_acknowledgement=None):
    return SimpleNamespace(
        state=state,
        message="typed operation result",
        to_dict=lambda: {
            "operation": "test",
            "state": state,
            "requested_properties": {},
            "effective_properties": {},
            "request_acknowledgement": None,
            "device_confirmation": None,
            "effective_state_confirmation": effective,
            "persistence_request_acknowledgement": persistence_acknowledgement,
            "persistence_confirmation": None,
            "message": "typed operation result",
            "verification_observations": [],
        },
    )


@pytest.fixture
def server():
    device = make_device(server_name="device.local.", name="Desk")
    device.online = True
    instance = make_http_server({"device.local.": device})
    instance.application.set_receive_flow_performance = AsyncMock(return_value=_result())
    instance.application.set_transmit_flow_performance = AsyncMock(return_value=_result())
    instance.application.set_unicast_performance = AsyncMock(return_value=_result())
    instance.application.set_receive_flow_default_slots = AsyncMock(return_value=_result())
    instance.application.store_current_configuration = AsyncMock(
        return_value=_result(
            "request_acknowledged",
            None,
            persistence_acknowledgement={"accepted": True},
        )
    )
    return instance


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("path", "method", "body", "arguments"),
    [
        (
            "/set-receive-flow-performance",
            "set_receive_flow_performance",
            {"latency_microseconds": 250, "frames_per_packet": 8},
            (250, 8),
        ),
        (
            "/set-transmit-flow-performance",
            "set_transmit_flow_performance",
            {"latency_microseconds": 500, "frames_per_packet": 4},
            (500, 4),
        ),
        (
            "/set-unicast-performance",
            "set_unicast_performance",
            {"latency_microseconds": 1_000, "frames_per_packet": 16},
            (1_000, 16),
        ),
        (
            "/set-receive-flow-default-slots",
            "set_receive_flow_default_slots",
            {"default_slots": 4},
            (4,),
        ),
    ],
)
async def test_typed_performance_routes(server, path, method, body, arguments):
    status, payload = await post(server, path, {"device": "device.local.", **body})

    assert status == 200
    assert payload["effective_state_confirmation"] is True
    getattr(server.application, method).assert_awaited_once_with(
        server.application.devices["device.local."], *arguments
    )


@pytest.mark.asyncio
async def test_storage_route_returns_acknowledged_without_persistence_confirmation(server):
    status, payload = await post(
        server,
        "/store-current-configuration",
        {"device": "device.local."},
    )

    assert status == 202
    assert payload["persistence_request_acknowledgement"] == {"accepted": True}
    assert payload["persistence_confirmation"] is None


@pytest.mark.asyncio
async def test_invalid_performance_input_is_a_client_error(server):
    server.application.set_receive_flow_performance.side_effect = ValueError("latency must be an integer")
    status, payload = await post(
        server,
        "/set-receive-flow-performance",
        {"device": "device.local.", "latency_microseconds": "250", "frames_per_packet": 8},
    )
    assert status == 400
    assert payload == {"error": "latency must be an integer"}
