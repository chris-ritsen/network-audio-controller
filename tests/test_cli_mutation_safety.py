import json
from types import SimpleNamespace

import pytest
from typer.testing import CliRunner

from netaudio.asynchronous_primitives import DeferredAsyncioLock
from netaudio.commands import flow as flow_commands
from netaudio.commands import subscription as subscription_commands
from netaudio.dante import flows
from tests.cli_test_support import FakeApplication, invoke

runner = CliRunner()


@pytest.fixture(autouse=True)
def reset_cli_state():
    from netaudio.cli import OutputFormat, state

    original = (
        list(state.names),
        list(state.hosts),
        list(state.server_names),
        list(state.macs),
        state.output_format,
    )
    state.names = []
    state.hosts = []
    state.server_names = []
    state.macs = []
    state.output_format = OutputFormat.plain
    try:
        yield state
    finally:
        state.names, state.hosts, state.server_names, state.macs, state.output_format = original


def _channel(number, name):
    return SimpleNamespace(
        number=number,
        name=name,
        friendly_name=None,
    )


def _subscription(rx_name, tx_name, tx_device):
    return SimpleNamespace(
        rx_channel_name=rx_name,
        tx_channel_name=tx_name,
        tx_device_name=tx_device,
    )


def _subscription_devices(refresh_rx=None):
    tx = SimpleNamespace(
        name="TX",
        server_name="tx.local.",
        ipv4="192.0.2.10",
        mac_address="00:1d:c1:00:00:10",
        tx_channels={1: _channel(1, "Tx1"), 2: _channel(2, "Tx2")},
        rx_channels={},
        subscriptions=[],
        services={},
        topology_mutation_lock=DeferredAsyncioLock(),
    )
    rx = SimpleNamespace(
        name="RX",
        server_name="rx.local.",
        ipv4="192.0.2.20",
        mac_address="00:1d:c1:00:00:20",
        tx_channels={},
        rx_channels={1: _channel(1, "Rx1"), 2: _channel(2, "Rx2")},
        subscriptions=[],
        services={},
        topology_mutation_lock=DeferredAsyncioLock(),
    )

    async def get_rx_channels():
        if refresh_rx is not None:
            refresh_rx(rx)

    rx.get_rx_channels = get_rx_channels
    return {"tx.local.": tx, "rx.local.": rx}, tx, rx


def _operations(application):
    return [sent.operation for sent in application.sent]


def _add_bulk(application, tx="TX", rx="RX", count=0, offset_tx=0, offset_rx=0):
    return invoke(
        subscription_commands.run_subscription_add_bulk,
        application,
        application.devices,
        tx,
        rx,
        count,
        offset_tx,
        offset_rx,
    )


def test_subscription_rejects_negative_ranges_before_discovery(monkeypatch):
    entered = False

    def run_command(*_arguments, **_options):
        nonlocal entered
        entered = True

    monkeypatch.setattr(subscription_commands, "run_command", run_command)

    result = runner.invoke(
        subscription_commands.app,
        ["add", "--tx", "TX", "--rx", "RX", "--offset-tx", "-1"],
    )

    assert result.exit_code != 0
    assert not entered


def test_subscription_rejects_count_beyond_available_pairs():
    devices, _, _ = _subscription_devices()
    application = FakeApplication(devices)

    result = _add_bulk(application, count=3)

    assert result.exit_code == 1
    assert "exceeds the 2 channel pairs" in result.output
    assert application.sent == []


def test_subscription_bulk_send_failure_returns_nonzero():
    devices, _, rx = _subscription_devices()
    application = FakeApplication(devices, send_error_for=str(rx.ipv4))

    result = _add_bulk(application)

    assert result.exit_code == 1
    assert "FAILED" in result.output
    assert "send failed" in result.output


def test_subscription_bulk_skips_already_satisfied_pairs():
    devices, _, rx = _subscription_devices()
    rx.subscriptions = [_subscription("Rx1", "Tx1", "TX")]
    application = FakeApplication(devices)

    result = _add_bulk(application, count=1)

    assert result.exit_code == 0
    assert application.sent == []
    assert "UNCHANGED Rx1@RX <- Tx1@TX (already subscribed)" in result.output
    assert "MODIFIED" not in result.output


def test_subscription_bulk_reports_unchanged_and_modified_exactly():
    refresh_count = 0

    def refresh_rx(device):
        nonlocal refresh_count
        refresh_count += 1
        if refresh_count >= 2:
            device.subscriptions = [
                _subscription("Rx1", "Tx1", "TX"),
                _subscription("Rx2", "Tx2", "TX"),
            ]

    devices, _, rx = _subscription_devices(refresh_rx=refresh_rx)
    rx.subscriptions = [_subscription("Rx1", "Tx1", "TX")]
    application = FakeApplication(devices)

    result = _add_bulk(application)

    assert result.exit_code == 0
    assert [(sent.operation, sent.device, sent.arguments) for sent in application.sent] == [
        ("add_subscriptions", rx, (((2, "Tx2", "TX"),),)),
    ]
    assert "UNCHANGED Rx1@RX <- Tx1@TX" in result.output
    assert "MODIFIED Rx2@RX <- Tx2@TX (verified)" in result.output


def test_subscription_bulk_reports_partial_readback_per_channel():
    refresh_count = 0

    def refresh_rx(device):
        nonlocal refresh_count
        refresh_count += 1
        if refresh_count >= 2:
            device.subscriptions = [_subscription("Rx1", "Tx1", "TX")]

    devices, _, _ = _subscription_devices(refresh_rx=refresh_rx)
    application = FakeApplication(devices)

    result = _add_bulk(application)

    assert result.exit_code == 1
    assert "MODIFIED Rx1@RX <- Tx1@TX (verified)" in result.output
    assert "FAILED Rx2@RX <- Tx2@TX" in result.output
    assert "fresh readback was None" in result.output


def test_subscription_add_requires_fresh_readback():
    devices, _, rx = _subscription_devices()
    application = FakeApplication(devices)

    result = invoke(
        subscription_commands.run_subscription_add_single,
        application,
        devices,
        "Tx1@TX",
        "Rx1@RX",
    )

    assert result.exit_code == 1
    assert [(sent.operation, sent.device, sent.arguments) for sent in application.sent] == [
        ("add_subscriptions", rx, (((1, "Tx1", "TX"),),)),
    ]
    assert "fresh readback reports" in result.output
    assert "(verified)" not in result.output


def test_subscription_readback_uses_channel_identity_when_labels_collide():
    device = SimpleNamespace(
        rx_channels={1: _channel(1, "Duplicate"), 2: _channel(2, "Duplicate")},
        subscriptions=[
            _subscription("Duplicate", "Tx1", "TX"),
            _subscription("Duplicate", "Tx2", "TX"),
        ],
    )
    subscription_commands._index_fresh_subscriptions(device)

    assert subscription_commands._subscription_signature(device, 2) == ("Tx2", "TX")


def test_subscription_remove_all_uses_global_filters_and_verifies(reset_cli_state):
    removal_requested = False

    def refresh_rx(device):
        if removal_requested:
            device.subscriptions = []

    devices, _, rx = _subscription_devices(refresh_rx=refresh_rx)
    rx.subscriptions = [_subscription("Rx1", "Tx1", "TX")]
    reset_cli_state.names = ["RX"]

    class RemovingApplication(FakeApplication):
        async def remove_subscriptions(self, device, channel_numbers):
            nonlocal removal_requested
            removal_requested = True
            return self._record("remove_subscriptions", device, tuple(channel_numbers))

    application = RemovingApplication(devices)

    result = invoke(subscription_commands.run_subscription_remove, application, devices, None, True)

    assert result.exit_code == 0
    assert removal_requested
    assert "Removed: Rx1@RX (verified)" in result.output


def test_subscription_remove_rejects_bare_device_spec():
    devices, _, _ = _subscription_devices()
    application = FakeApplication(devices)

    result = invoke(subscription_commands.run_subscription_remove, application, devices, ["RX"], False)

    assert result.exit_code == 1
    assert "expected CHANNEL@DEVICE" in result.output
    assert application.sent == []


def test_subscription_remove_refreshes_before_claiming_channel_is_subscribed():
    def refresh_rx(device):
        device.subscriptions = []

    devices, _, rx = _subscription_devices(refresh_rx=refresh_rx)
    rx.subscriptions = [_subscription("Rx1", "Tx1", "TX")]
    application = FakeApplication(devices)

    result = invoke(subscription_commands.run_subscription_remove, application, devices, ["Rx1@RX"], False)

    assert result.exit_code == 1
    assert "is not subscribed" in result.output
    assert application.sent == []


def _flow_device():
    return SimpleNamespace(
        name="Flow Device",
        server_name="flow.local.",
        ipv4="192.0.2.30",
        mac_address="00:1d:c1:00:00:30",
        flow_protocol_id=0x2729,
        services={},
        tx_channels={1: _channel(1, "Tx1"), 2: _channel(2, "Tx2")},
        topology_mutation_lock=DeferredAsyncioLock(),
    )


def _flow_context(device=None):
    device = _flow_device() if device is None else device
    devices = {device.server_name: device}
    return FakeApplication(devices), devices, device


def test_flow_create_rejects_malformed_channels_before_discovery(monkeypatch):
    calls = 0

    def run_command(*_arguments, **_options):
        nonlocal calls
        calls += 1

    monkeypatch.setattr(flow_commands, "run_command", run_command)

    result = runner.invoke(
        flow_commands.app,
        ["create", "--slot", "17", "--channels", "1,"],
    )

    assert result.exit_code == 1
    assert "comma-separated list of integers" in result.output
    assert calls == 0


def test_empty_flow_list_preserves_structured_output(monkeypatch):
    from netaudio.cli import OutputFormat, state

    application, devices, _ = _flow_context()
    flow_inventory = {"max_flow_slots": 16, "flows": []}

    async def query(*_args):
        return flow_inventory

    monkeypatch.setattr(flows, "query_tx_flow_inventory", query)
    state.output_format = OutputFormat.json

    result = invoke(flow_commands.run_flow_list, application, devices)

    assert result.exit_code == 0
    assert json.loads(result.output) == flow_inventory


RECEIVER_FLOW_INVENTORY = {
    "maximum_flow_slots": 16,
    "flows": [
        {
            "flow_number": 3,
            "flow_type": "unicast",
            "receiver_channel_numbers_by_flow_channel": [[1, 21], [22]],
            "subscription_status_code": 0x0015,
            "destination_internet_protocol_version_four_address": "192.0.2.30",
            "destination_user_datagram_port": 0x3801,
            "sample_rate": 48_000,
            "encoding": 24,
            "frames_per_packet": 1,
            "latency_nanoseconds": 1_000_000,
        }
    ],
}


def test_receiver_flow_list_preserves_structured_output(monkeypatch):
    from netaudio.cli import OutputFormat, state

    application, devices, _ = _flow_context()

    async def query(*_args):
        return RECEIVER_FLOW_INVENTORY

    monkeypatch.setattr(flows, "query_preferred_receiver_flow_inventory", query)
    state.output_format = OutputFormat.json

    result = invoke(flow_commands.run_receiver_flow_list, application, devices)

    assert result.exit_code == 0
    assert json.loads(result.output) == RECEIVER_FLOW_INVENTORY


def test_receiver_flow_list_displays_endpoint_type_and_port(monkeypatch):
    from netaudio.cli import OutputFormat, state

    application, devices, _ = _flow_context()

    async def query(*_args):
        return RECEIVER_FLOW_INVENTORY

    monkeypatch.setattr(flows, "query_preferred_receiver_flow_inventory", query)
    state.output_format = OutputFormat.plain

    result = invoke(flow_commands.run_receiver_flow_list, application, devices)

    assert result.exit_code == 0
    assert "unicast" in result.output
    assert "192.0.2.30" in result.output
    assert "14337" in result.output


def test_receiver_port_ranges_preserve_structured_output(monkeypatch):
    from netaudio.cli import OutputFormat, state

    application, devices, _ = _flow_context()
    port_ranges = {
        "first_port_range_start": 0x3800,
        "first_port_range_end": 0x397F,
        "second_port_range_start": 0x3980,
        "second_port_range_end": 0x39FF,
    }

    async def query(*_args):
        return port_ranges

    monkeypatch.setattr(flows, "query_receiver_port_ranges", query)
    state.output_format = OutputFormat.json

    result = invoke(flow_commands.run_receiver_port_ranges, application, devices)

    assert result.exit_code == 0
    assert json.loads(result.output) == port_ranges


def test_transmit_channel_capabilities_fail_closed_without_traceback(monkeypatch):
    application, devices, _ = _flow_context()

    async def query(*_args):
        return None

    monkeypatch.setattr(flows, "query_transmit_channel_capabilities", query)

    result = invoke(flow_commands.run_transmit_channel_capabilities, application, devices, 1, 0)

    assert result.exit_code == 1
    assert "does not report transmitter channel capabilities" in result.output
    assert "Traceback" not in result.output


def _subscription_list_device(subscriptions):
    return SimpleNamespace(
        name="lx-dante",
        server_name="lx.local.",
        mac_address="001dc1081258",
        ipv4="192.0.2.10",
        model_id="LX-DANTE",
        subscriptions=subscriptions,
    )


def _configured_subscription():
    from netaudio.dante.subscription import DanteSubscription

    configured = DanteSubscription()
    configured.rx_channel_name = "bluetooth:left"
    configured.rx_device_name = "lx-dante"
    configured.tx_channel_name = "bluetooth:left"
    configured.tx_device_name = "avio-bt-1"
    configured.status_code = 0x0009
    return configured


def _unused_subscription():
    from netaudio.dante.subscription import DanteSubscription

    unused = DanteSubscription()
    unused.rx_channel_name = "unused-rx"
    unused.rx_device_name = "lx-dante"
    unused.tx_channel_name = "unused-rx"
    unused.tx_device_name = ""
    unused.status_code = 0x0000
    return unused


def test_subscription_list_hides_unused_channels_by_default():
    device = _subscription_list_device([_configured_subscription(), _unused_subscription()])
    devices = {"lx.local.": device}

    result = invoke(subscription_commands.run_subscription_list, FakeApplication(devices), devices, False)

    assert result.exit_code == 0
    assert "bluetooth:left" in result.output
    assert "avio-bt-1" in result.output
    assert "unused-rx" not in result.output


def test_subscription_list_all_includes_unused_without_placeholder_source():
    device = _subscription_list_device([_unused_subscription()])
    devices = {"lx.local.": device}

    result = invoke(subscription_commands.run_subscription_list, FakeApplication(devices), devices, True)

    assert result.exit_code == 0
    assert "unused-rx" in result.output
    assert "lx-dante" in result.output
    lines = [line for line in result.output.splitlines() if "unused-rx" in line]
    assert lines
    assert lines[0].count("unused-rx") == 1


def test_transmit_channel_capabilities_preserve_structured_output(monkeypatch):
    from netaudio.cli import OutputFormat, state

    application, devices, _ = _flow_context()
    capabilities = {
        "format_identifier": 1,
        "starting_channel_identifier": 1,
        "channel_count": 128,
        "capability_flags": 0x7FFF,
    }

    async def query(*_args):
        return capabilities

    monkeypatch.setattr(flows, "query_transmit_channel_capabilities", query)
    state.output_format = OutputFormat.json

    result = invoke(flow_commands.run_transmit_channel_capabilities, application, devices, 1, 0)

    assert result.exit_code == 0
    assert json.loads(result.output) == capabilities


def test_flow_create_refuses_occupied_slot(monkeypatch):
    application, devices, _ = _flow_context()
    create_calls = 0

    async def query(*_args):
        return {"max_flow_slots": 32, "flows": [{"flow_number": 17, "flow_type": "multicast"}]}

    async def create(*_args):
        nonlocal create_calls
        create_calls += 1

    monkeypatch.setattr(flows, "query_tx_flow_inventory", query)
    monkeypatch.setattr(flows, "create_tx_flow", create)

    result = invoke(flow_commands.run_flow_create, application, devices, 17, [1])

    assert result.exit_code == 1
    assert "already in use" in result.output
    assert create_calls == 0


def test_flow_create_confirms_success(monkeypatch):
    application, devices, device = _flow_context()

    async def query(*_args):
        return {"max_flow_slots": 32, "flows": []}

    async def create(*_args):
        assert device.topology_mutation_lock.locked()
        return 1

    monkeypatch.setattr(flows, "query_tx_flow_inventory", query)
    monkeypatch.setattr(flows, "create_tx_flow", create)

    result = invoke(flow_commands.run_flow_create, application, devices, 17, [1, 2])

    assert result.exit_code == 0
    assert "Created multicast TX flow" in result.output
    assert "device confirmed" in result.output


def test_flow_create_refuses_slot_above_device_capacity(monkeypatch):
    application, devices, _ = _flow_context()
    create_calls = 0

    async def query(*_args):
        return {"max_flow_slots": 16, "flows": []}

    async def create(*_args):
        nonlocal create_calls
        create_calls += 1

    monkeypatch.setattr(flows, "query_tx_flow_inventory", query)
    monkeypatch.setattr(flows, "create_tx_flow", create)

    result = invoke(flow_commands.run_flow_create, application, devices, 17, [1])

    assert result.exit_code == 1
    assert "exceeds the device capacity of 16" in result.output
    assert create_calls == 0


def test_flow_delete_requires_confirmation_before_discovery(monkeypatch):
    calls = 0

    def run_command(*_arguments, **_options):
        nonlocal calls
        calls += 1

    monkeypatch.setattr(flow_commands, "run_command", run_command)

    result = runner.invoke(
        flow_commands.app,
        ["delete", "--slot", "17"],
    )

    assert result.exit_code == 1
    assert "without --yes" in result.output
    assert calls == 0


def test_flow_delete_refuses_non_multicast_flow(monkeypatch):
    application, devices, _ = _flow_context()
    delete_calls = 0

    async def query(*_args):
        return {"max_flow_slots": 32, "flows": [{"flow_number": 17, "flow_type": "unicast"}]}

    async def delete(*_args):
        nonlocal delete_calls
        delete_calls += 1

    monkeypatch.setattr(flows, "query_tx_flow_inventory", query)
    monkeypatch.setattr(flows, "delete_tx_flow", delete)

    result = invoke(flow_commands.run_flow_delete, application, devices, 17)

    assert result.exit_code == 1
    assert "is not multicast" in result.output
    assert delete_calls == 0
