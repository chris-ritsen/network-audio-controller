from unittest.mock import AsyncMock, Mock
from types import SimpleNamespace

import pytest

from tests.http_api_test_support import make_device, make_http_server, post
from netaudio.dante.const import SERVICE_ARC
from netaudio.dante.arc_protocol import ArcProtocolError

SUCCESS = bytes.fromhex("27ff000a000010010001")
REJECTED = bytes.fromhex("27ff000a000010010600")


@pytest.mark.asyncio
@pytest.mark.parametrize("batch", [False, True])
@pytest.mark.parametrize("inventory", ["rekeyed", "wrong_channel", "duplicate"])
async def test_unsubscribe_resolves_receiver_identity_before_sending(batch, inventory):
    device = make_device()
    device.rx_channels = {"Input": SimpleNamespace(number=1)}

    if inventory == "wrong_channel":
        device.rx_channels = {1: SimpleNamespace(number=2)}
    elif inventory == "duplicate":
        device.rx_channels[1] = SimpleNamespace(number=1)

    server = make_http_server({"dev1": device})
    server.application.remove_subscriptions = AsyncMock(return_value=SUCCESS)
    server.subscription_readback.request = Mock()
    fields = {"rx_channels": [1]} if batch else {"rx_channel": 1}
    status, body = await post(server, "/unsubscribe", {"rx_device": "dev1", **fields})

    if inventory == "rekeyed":
        assert status == 200 and body["success"] is True
        server.application.remove_subscriptions.assert_awaited_once_with(device, [1])
        server.subscription_readback.request.assert_called_once_with(device, [(1, "", "")])
    else:
        assert status == (404 if inventory == "wrong_channel" else 409)
        server.application.remove_subscriptions.assert_not_awaited()
        server.subscription_readback.request.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("numbers", [[], [1, True], [1, 1], [1, "2"], "12", None])
async def test_unsubscribe_invalid_batch_never_falls_back_to_a_single_channel(numbers):
    device = make_device()
    device.rx_channels = {1: SimpleNamespace(number=1), 2: SimpleNamespace(number=2)}
    server = make_http_server({"dev1": device})
    server.application.remove_subscriptions = AsyncMock(return_value=SUCCESS)
    server.subscription_readback.request = Mock()

    status, body = await post(server, "/unsubscribe", {"rx_device": "dev1", "rx_channel": 1, "rx_channels": numbers})

    assert status == 400 and body["error"]
    server.application.remove_subscriptions.assert_not_awaited()
    server.subscription_readback.request.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("batch", [False, True])
@pytest.mark.parametrize("damage", ["boolean_channel", "numeric_source", "missing_source", "duplicate"])
async def test_subscribe_validates_complete_intent_before_application_calls(batch, damage):
    server = make_http_server({"dev1": make_device()})
    server.application.add_subscriptions = AsyncMock(return_value=SUCCESS)
    server.subscription_readback.request = Mock()
    entry = {"rx_channel": 1, "tx_channel": "Mic", "tx_device": "Source"}

    if damage == "boolean_channel":
        entry["rx_channel"] = True
    elif damage == "numeric_source":
        entry["tx_device"] = 42
    elif damage == "missing_source":
        entry.pop("tx_channel")

    if batch:
        entries = [{"rx_channel": 2, "tx_channel": "Other", "tx_device": "Source"}, entry]

        if damage == "duplicate":
            entries.append(dict(entry))

        fields = {"subscriptions": entries}
    elif damage == "duplicate":
        fields = {"subscriptions": [], **entry}
    else:
        fields = entry

    status, body = await post(server, "/subscribe", {"rx_device": "dev1", **fields})

    assert status == 400 and body["error"]
    server.application.add_subscriptions.assert_not_awaited()
    server.subscription_readback.request.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("result_code,status", [(1, 200), (0x8112, 200), (0, 409), (1536, 409)])
async def test_http_subscription_uses_native_acknowledgement_semantics(result_code, status):
    device = make_device()
    server = make_http_server({"dev1": device})
    server.subscription_readback.request = Mock()
    server.application.add_subscriptions.return_value = bytes.fromhex("27ff000a000130100000")[
        :8
    ] + result_code.to_bytes(2, "big")

    actual_status, body = await post(
        server,
        "/subscribe",
        {"rx_device": "dev1", "rx_channel": 1, "tx_device": "Source", "tx_channel": "Mic"},
    )

    assert actual_status == status
    assert bool(body.get("success")) == (status == 200)
    assert server.subscription_readback.request.called == (status == 200)


@pytest.mark.asyncio
@pytest.mark.parametrize("value", [False, 0, [], {}, "", "   "])
async def test_invalid_source_values_never_clear_routes_or_apply_an_earlier_valid_route(value):
    device = make_device()
    server = make_http_server({"dev1": device})
    server.application.add_subscriptions = AsyncMock(return_value=SUCCESS)
    server.application.remove_subscriptions = AsyncMock(return_value=SUCCESS)
    routes = [
        {"rx_device": "dev1", "rx_channel": 1, "tx_device": "Source", "tx_channel": "Mic"},
        {"rx_device": "dev1", "rx_channel": 2, "tx_device": value, "tx_channel": value},
    ]

    status, body = await post(server, "/subscriptions/apply", {"routes": routes})

    assert status == 400
    assert "tx_device" in body["error"]
    server.application.add_subscriptions.assert_not_awaited()
    server.application.remove_subscriptions.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "routes",
    [
        [],
        [{"rx_device": "dev1"}],
        [{"rx_device": "dev1", "rx_channel": False}],
        [{"rx_device": "dev1", "rx_channel": 0}],
        [{"rx_device": "dev1", "rx_channel": 1, "tx_device": "Source"}],
        [{"rx_device": "dev1", "rx_channel": 1}, {"rx_device": "dev1", "rx_channel": 1}],
        [{"rx_device": "dev1", "rx_channel": 1, "txDevice": "Source", "txChannel": "Mic"}],
    ],
)
async def test_invalid_route_batches_are_rejected_before_any_device_write(routes):
    server = make_http_server({"dev1": make_device()})
    server.application.add_subscriptions = AsyncMock(return_value=SUCCESS)
    server.application.remove_subscriptions = AsyncMock(return_value=SUCCESS)

    status, body = await post(server, "/subscriptions/apply", {"routes": routes})

    assert status == 400 and body["error"]
    server.application.add_subscriptions.assert_not_awaited()
    server.application.remove_subscriptions.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("alias", ["Device1", "device1", "192.168.1.50"])
async def test_receiver_aliases_cannot_write_the_same_channel_twice(alias):
    device = make_device()
    device.services = {"arc": {"type": SERVICE_ARC, "properties": {"arcp_vers": "2.7.255"}}}
    device.rx_channels = {1: SimpleNamespace(number=1)}
    server = make_http_server({"dev1": device})
    server.subscription_readback.request = Mock()

    status, body = await post(
        server,
        "/subscriptions/apply",
        {
            "routes": [
                {"rx_device": "dev1", "rx_channel": 1},
                {"rx_device": alias, "rx_channel": 1, "tx_device": "Source", "tx_channel": "Mic"},
            ]
        },
    )

    assert status == 400 and "same receiver channel" in body["error"]
    server.application.add_subscriptions.assert_not_awaited()
    server.application.remove_subscriptions.assert_not_awaited()
    server.subscription_readback.request.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("version", ["2.7.255", "2.8.9", "2.8.15"])
@pytest.mark.parametrize("damage", ["invalid_source", "missing_receiver"])
async def test_apply_preflights_complete_receiver_intent_before_clears_or_sets(version, damage):
    device = make_device()
    device.services = {"arc": {"type": SERVICE_ARC, "properties": {"arcp_vers": version}}}
    device.rx_channels = {number: SimpleNamespace(number=number, media_type_code=3) for number in range(1, 34)}
    server = make_http_server({"dev1": device})
    server.subscription_readback.request = Mock()
    routes = [{"rx_device": "dev1", "rx_channel": 1}]
    routes.extend(
        {"rx_device": "dev1", "rx_channel": number, "tx_device": "Source", "tx_channel": "Mic"}
        for number in range(2, 34)
    )

    if damage == "invalid_source":
        routes[-1]["tx_device"] = "x" * 100
    else:
        routes[-1]["rx_channel"] = 34

    status, body = await post(server, "/subscriptions/apply", {"routes": routes})

    assert status == 409 and body["error"]
    server.application.add_subscriptions.assert_not_awaited()
    server.application.remove_subscriptions.assert_not_awaited()
    server.subscription_readback.request.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("clear_fields", [{}, {"tx_device": None, "tx_channel": None}])
async def test_apply_subscriptions_batches_per_device_and_reports_each_route(clear_fields):
    first = make_device(server_name="first.local.", name="first")
    first.rx_channels = {number: SimpleNamespace(number=number) for number in range(1, 31)}
    second = make_device(server_name="second.local.", name="second")
    second.requires_managed_control = True
    server = make_http_server({first.server_name: first, second.server_name: second})
    server.subscription_readback.request = Mock()
    server.application.add_subscriptions = AsyncMock(return_value=SUCCESS)
    server.application.remove_subscriptions = AsyncMock(return_value=SUCCESS)
    first.services = {"arc": {"type": SERVICE_ARC, "properties": {"arcp_vers": "2.7.255"}}}
    routes = [
        {"rx_device": "first", "rx_channel": number, "tx_device": "src", "tx_channel": "01"} for number in range(1, 21)
    ]
    routes += [
        {"rx_device": "first", "rx_channel": 30, **clear_fields},
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
    device.rx_channels = {1: SimpleNamespace(number=1)}
    server = make_http_server({"dev1": device})
    server.application.add_subscriptions = AsyncMock(return_value=REJECTED)
    device.services = {"arc": {"type": SERVICE_ARC, "properties": {"arcp_vers": "2.7.255"}}}

    status, body = await post(
        server,
        "/subscriptions/apply",
        {"routes": [{"rx_device": "dev1", "rx_channel": 1, "tx_device": "src", "tx_channel": "01"}]},
    )
    assert status == 409
    assert body["routes"][0]["error"] == "device rejected subscription set"

    status, body = await post(server, "/subscriptions/apply", {"routes": []})
    assert status == 400 and "non-empty" in body["error"]


@pytest.mark.asyncio
@pytest.mark.parametrize("version", [None, "2.8.16", "bad"])
async def test_batch_rejects_unknown_protocol_before_any_write(version):
    device = make_device()
    device.services = {"arc": {"type": SERVICE_ARC, "properties": {"arcp_vers": version}}}
    server = make_http_server({"dev1": device})
    server.application.add_subscriptions = AsyncMock(return_value=SUCCESS)
    server.application.remove_subscriptions = AsyncMock(return_value=SUCCESS)

    status, body = await post(
        server,
        "/subscriptions/apply",
        {"routes": [{"rx_device": "dev1", "rx_channel": 1, "tx_device": "src", "tx_channel": "01"}]},
    )

    assert status == 409 and body["error"]
    server.application.add_subscriptions.assert_not_awaited()
    server.application.remove_subscriptions.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "path,fields",
    [
        ("/subscribe", {"rx_channel": 1, "tx_device": "src", "tx_channel": "01"}),
        ("/subscribe", {"subscriptions": [{"rx_channel": 1, "tx_device": "src", "tx_channel": "01"}]}),
        ("/unsubscribe", {"rx_channel": 1}),
        ("/unsubscribe", {"rx_channels": [1]}),
    ],
)
async def test_subscription_endpoints_report_unavailable_protocol_as_conflict(path, fields):
    device = make_device()
    device.rx_channels = {1: SimpleNamespace(number=1)}
    server = make_http_server({"dev1": device})
    server.application.add_subscriptions = AsyncMock(side_effect=ArcProtocolError("unsupported ARC protocol version"))
    server.application.remove_subscriptions = AsyncMock(
        side_effect=ArcProtocolError("unsupported ARC protocol version")
    )

    status, body = await post(server, path, {"rx_device": "dev1", **fields})

    assert status == 409
    assert body["error"] == "unsupported ARC protocol version"
