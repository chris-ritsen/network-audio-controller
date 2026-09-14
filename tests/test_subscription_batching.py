from unittest.mock import AsyncMock

import pytest

from netaudio.daemon.subscription_batching import Route, parse_routes, plan_batches
from tests.http_api_test_support import make_device, make_http_server, post

SUCCESS = bytes.fromhex("27ff000a000010010001")
REJECTED = bytes.fromhex("27ff000a000010010600")


def test_parse_routes_validates_shape_and_duplicates():
    routes = parse_routes(
        [
            {"rx_device": "a", "rx_channel": 1, "tx_device": "b", "tx_channel": "01"},
            {"rx_device": "a", "rx_channel": 2},
            {"rx_device": "a", "rx_channel": 3, "tx_device": "", "tx_channel": ""},
        ]
    )
    assert routes[0] == Route("a", 1, "b", "01") and not routes[0].clears
    assert routes[1].clears and routes[2].clears
    for payload, message in (
        ([], "non-empty"),
        ([{"rx_device": "a"}], "rx_channel"),
        ([{"rx_device": "a", "rx_channel": 0}], "rx_channel"),
        ([{"rx_device": "a", "rx_channel": 1, "tx_device": "b"}], "both"),
        (
            [{"rx_device": "a", "rx_channel": 1}, {"rx_device": "a", "rx_channel": 1}],
            "repeats",
        ),
    ):
        with pytest.raises(ValueError, match=message):
            parse_routes(payload)


def test_plan_batches_groups_by_device_and_splits_by_limit():
    routes = [Route("a", number, "b", f"{number:02d}") for number in range(1, 35)]
    routes += [Route("a", 40), Route("c", 1, "b", "01")]
    plan = plan_batches(routes, lambda name: 16 if name == "a" else 32)
    assert [(batch.action, len(batch.routes)) for batch in plan["a"]] == [
        ("clear", 1),
        ("set", 16),
        ("set", 16),
        ("set", 2),
    ]
    assert [(batch.action, len(batch.routes)) for batch in plan["c"]] == [("set", 1)]


@pytest.mark.asyncio
async def test_apply_subscriptions_batches_per_device_and_reports_each_route():
    first = make_device(server_name="first.local.", name="first")
    second = make_device(server_name="second.local.", name="second")
    second.requires_managed_control = True
    server = make_http_server({first.server_name: first, second.server_name: second})
    server.application.add_subscriptions = AsyncMock(return_value=SUCCESS)
    server.application.remove_subscriptions = AsyncMock(return_value=SUCCESS)
    server.application._uses_modern_arc_280f = lambda device: False
    routes = [
        {"rx_device": "first", "rx_channel": number, "tx_device": "src", "tx_channel": "01"} for number in range(1, 21)
    ]
    routes += [
        {"rx_device": "first", "rx_channel": 30},
        {"rx_device": "second", "rx_channel": 1, "tx_device": "src", "tx_channel": "02"},
    ]
    routes += [{"rx_device": "ghost", "rx_channel": 1, "tx_device": "src", "tx_channel": "03"}]

    status, body = await post(server, "/subscriptions/apply", {"routes": routes})

    assert status == 200
    assert body["applied"] == 22 and body["failed"] == 1 and body["success"] is False
    assert body["batches"] == {"first": 3, "second": 1}
    assert server.application.add_subscriptions.await_count == 3
    sizes = sorted(len(call.args[1]) for call in server.application.add_subscriptions.await_args_list)
    assert sizes == [1, 4, 16]
    server.application.remove_subscriptions.assert_awaited_once_with(first, [30])
    assert body["routes"][-1] == {
        "action": "set",
        "error": "rx device not found",
        "ok": False,
        "rx_channel": 1,
        "rx_device": "ghost",
        "tx_channel": "03",
        "tx_device": "src",
    }
    assert body["routes"][20]["action"] == "clear" and body["routes"][20]["ok"] is True


@pytest.mark.asyncio
async def test_apply_subscriptions_reports_rejections_and_rejects_bad_input():
    device = make_device()
    server = make_http_server({"dev1": device})
    server.application.add_subscriptions = AsyncMock(return_value=REJECTED)
    server.application._uses_modern_arc_280f = lambda device: False

    status, body = await post(
        server,
        "/subscriptions/apply",
        {"routes": [{"rx_device": "dev1", "rx_channel": 1, "tx_device": "src", "tx_channel": "01"}]},
    )
    assert status == 409
    assert body["routes"][0]["error"] == "device rejected subscription set (result code 0x0600)"

    status, body = await post(server, "/subscriptions/apply", {"routes": []})
    assert status == 400 and "non-empty" in body["error"]
