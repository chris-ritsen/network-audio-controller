import asyncio
from types import SimpleNamespace

import pytest

from netaudio.asynchronous_primitives import DeferredAsyncioLock
from netaudio.dante import flows
from netaudio.dante.sample_rate_topology import (
    SampleRateTopologyChangedButUnverifiedError,
    SampleRateTopologyConfirmationRequired,
    SampleRateTopologyMutationOutcomeUnknownError,
    SampleRateTopologyUnsupportedError,
    change_sample_rate_with_command_sender,
    change_sample_rate_topology_safe,
    preflight_sample_rate_change,
)


class FakeA32:
    def __init__(self, phases):
        self.name = "A32"
        self.server_name = "a32.local."
        self.ipv4 = "192.0.2.10"
        self.dante_model = "A32 Dante AD/DA Converter"
        self.model = ""
        self.board_name = None
        self.sample_rate = None
        self.supported_sample_rates = None
        self.sample_rate_channel_capacities = None
        self.rx_channels = {}
        self.subscriptions = []
        self.phases = phases
        self.phase_index = 0
        self.topology_mutation_lock = DeferredAsyncioLock()

    def _arc_port(self):
        return 4440

    async def get_rx_channels(self):
        phase = self.phases[self.phase_index]
        receive_channel_count = phase["receive_channel_count"]
        self.rx_channels = {
            number: SimpleNamespace(number=number, name=f"Input {number}")
            for number in range(1, receive_channel_count + 1)
        }
        self.subscriptions = []
        for receiver_channel_number, transmitter_channel_name, transmitter_device_name in phase.get(
            "subscriptions", ()
        ):
            channel = self.rx_channels[receiver_channel_number]
            self.subscriptions.append(
                SimpleNamespace(
                    has_configured_source=True,
                    rx_channel=channel,
                    rx_channel_name=channel.name,
                    tx_channel_name=transmitter_channel_name,
                    tx_device_name=transmitter_device_name,
                )
            )


def _phase(receive_channel_count, flows_state, subscriptions=()):
    return {
        "receive_channel_count": receive_channel_count,
        "flows": flows_state,
        "subscriptions": subscriptions,
    }


def _multicast_flow(flow_number, members, sample_rate=48_000):
    return {
        "flow_number": flow_number,
        "flow_type": "multicast",
        "channel_count": len(members),
        "channels": list(members),
        "sample_rate": sample_rate,
        "encoding": 24,
        "frames_per_packet": 1,
    }


def _unicast_flow(flow_number, channel_count=1):
    return {
        "flow_number": flow_number,
        "flow_type": "unicast",
        "channel_count": channel_count,
        "channels": [],
        "sample_rate": 48_000,
        "encoding": 24,
        "frames_per_packet": 1,
    }


@pytest.fixture
def install_flow_inventory(monkeypatch):
    def install(device):
        async def detect_flow_protocol(device_ip, arc_port):
            assert device_ip == "192.0.2.10"
            assert arc_port == 4440
            return 0x2729

        async def query_tx_flow_inventory(device_ip, arc_port, flow_protocol_identifier):
            assert device_ip == "192.0.2.10"
            assert arc_port == 4440
            assert flow_protocol_identifier == 0x2729
            return {
                "max_flow_slots": 32,
                "flows": device.phases[device.phase_index]["flows"],
            }

        monkeypatch.setattr(flows, "detect_flow_protocol", detect_flow_protocol)
        monkeypatch.setattr(flows, "query_tx_flow_inventory", query_tx_flow_inventory)

    return install


@pytest.mark.asyncio
async def test_preflight_distinguishes_reversible_receiver_clipping_from_destructive_flow_loss(
    install_flow_inventory,
):
    device = FakeA32(
        [
            _phase(
                64,
                [_multicast_flow(32, range(16, 24))],
                subscriptions=[(64, "left", "avio-bt-1")],
            )
        ]
    )
    install_flow_inventory(device)

    async def probe():
        return 48_000, [44_100, 48_000, 88_200, 96_000, 176_400, 192_000]

    preflight = await preflight_sample_rate_change(device, 192_000, probe)

    assert preflight.target_capacity.to_dict() == {
        "sample_rate_hertz": 192_000,
        "receive_channel_count": 16,
        "transmit_channel_count": 16,
    }
    assert [state.receiver_channel_number for state in preflight.reversible_receiver_clipping] == [64]
    assert [state.to_dict() for state in preflight.destructive_transmitter_membership_loss] == [
        {
            "flow_number": 32,
            "flow_type": "multicast",
            "retained_channel_members": [16],
            "removed_channel_members": [17, 18, 19, 20, 21, 22, 23],
        }
    ]
    assert preflight.uncharacterized_transmitter_flows == ()


@pytest.mark.asyncio
async def test_destructive_change_is_refused_before_mutation_without_confirmation(install_flow_inventory):
    device = FakeA32([_phase(64, [_multicast_flow(4, [16, 17])])])
    install_flow_inventory(device)
    mutation_called = False

    async def probe():
        return 48_000, [48_000, 192_000]

    async def mutate():
        nonlocal mutation_called
        mutation_called = True

    with pytest.raises(SampleRateTopologyConfirmationRequired) as raised:
        await change_sample_rate_topology_safe(device, 192_000, probe, mutate)

    assert mutation_called is False
    assert raised.value.preflight.destructive_transmitter_membership_loss[0].removed_channel_members == (17,)


@pytest.mark.asyncio
async def test_confirmed_change_verifies_rate_receiver_clipping_and_exact_flow_reduction(install_flow_inventory):
    device = FakeA32(
        [
            _phase(
                64,
                [
                    _multicast_flow(7, [1, 2]),
                    _multicast_flow(32, range(16, 24)),
                ],
                subscriptions=[
                    (1, "retained", "avio-input-2"),
                    (64, "left", "avio-bt-1"),
                ],
            ),
            _phase(
                16,
                [
                    _multicast_flow(7, [1, 2], sample_rate=192_000),
                    _multicast_flow(32, [16, 0, 0, 0, 0, 0, 0, 0], sample_rate=192_000),
                ],
                subscriptions=[(1, "retained", "avio-input-2")],
            ),
        ]
    )
    install_flow_inventory(device)

    async def probe():
        return (48_000 if device.phase_index == 0 else 192_000), [48_000, 192_000]

    async def mutate():
        device.phase_index = 1

    result = await change_sample_rate_topology_safe(
        device,
        192_000,
        probe,
        mutate,
        confirm_destructive=True,
    )

    assert result.changed is True
    assert result.observed_sample_rate_hertz == 192_000
    assert result.resulting_snapshot.capacity.receive_channel_count == 16
    assert [state.receiver_channel_number for state in result.resulting_snapshot.receiver_subscriptions] == [1]
    assert [state.channel_members for state in result.resulting_snapshot.transmitter_flows] == [
        (1, 2),
        (16, 0, 0, 0, 0, 0, 0, 0),
    ]


@pytest.mark.asyncio
async def test_unicast_flow_blocks_capacity_reduction_even_with_destructive_confirmation(install_flow_inventory):
    device = FakeA32([_phase(64, [_unicast_flow(3)])])
    install_flow_inventory(device)
    mutation_called = False

    async def probe():
        return 48_000, [48_000, 192_000]

    async def mutate():
        nonlocal mutation_called
        mutation_called = True

    with pytest.raises(SampleRateTopologyUnsupportedError) as raised:
        await change_sample_rate_topology_safe(
            device,
            192_000,
            probe,
            mutate,
            confirm_destructive=True,
        )

    assert mutation_called is False
    assert raised.value.preflight.uncharacterized_transmitter_flows[0].flow_number == 3


@pytest.mark.asyncio
async def test_all_out_of_range_multicast_flow_blocks_unproven_transition(install_flow_inventory):
    device = FakeA32([_phase(64, [_multicast_flow(5, [17, 18])])])
    install_flow_inventory(device)

    async def probe():
        return 48_000, [48_000, 192_000]

    async def mutate():
        raise AssertionError("uncharacterized topology must not be mutated")

    with pytest.raises(SampleRateTopologyUnsupportedError) as raised:
        await change_sample_rate_topology_safe(
            device,
            192_000,
            probe,
            mutate,
            confirm_destructive=True,
        )

    assert "all active members" in raised.value.preflight.uncharacterized_transmitter_flows[0].reason


@pytest.mark.asyncio
async def test_post_write_readback_rejects_unexpected_loss_of_retained_member(install_flow_inventory):
    device = FakeA32(
        [
            _phase(64, [_multicast_flow(9, [16, 17])]),
            _phase(16, [_multicast_flow(9, [0, 0], sample_rate=192_000)]),
        ]
    )
    install_flow_inventory(device)

    async def probe():
        return (48_000 if device.phase_index == 0 else 192_000), [48_000, 192_000]

    async def mutate():
        device.phase_index = 1

    with pytest.raises(
        SampleRateTopologyChangedButUnverifiedError,
        match="complete post-write verification failed",
    ) as raised:
        await change_sample_rate_topology_safe(
            device,
            192_000,
            probe,
            mutate,
            confirm_destructive=True,
        )
    assert "exact expected membership" in str(raised.value.__cause__)


@pytest.mark.asyncio
async def test_post_write_readback_rejects_lost_in_capacity_receiver_subscription(install_flow_inventory):
    device = FakeA32(
        [
            _phase(64, [], subscriptions=[(1, "retained", "avio-input-2")]),
            _phase(32, []),
        ]
    )
    install_flow_inventory(device)

    async def probe():
        return (48_000 if device.phase_index == 0 else 96_000), [48_000, 96_000]

    async def mutate():
        device.phase_index = 1

    with pytest.raises(SampleRateTopologyChangedButUnverifiedError) as raised:
        await change_sample_rate_topology_safe(device, 96_000, probe, mutate)

    assert "exact expected in-capacity state" in str(raised.value.__cause__)


@pytest.mark.asyncio
async def test_post_write_readback_rejects_disappeared_unaffected_transmitter_flow(install_flow_inventory):
    device = FakeA32(
        [
            _phase(64, [_multicast_flow(7, [1, 2])]),
            _phase(32, []),
        ]
    )
    install_flow_inventory(device)

    async def probe():
        return (48_000 if device.phase_index == 0 else 96_000), [48_000, 96_000]

    async def mutate():
        device.phase_index = 1

    with pytest.raises(SampleRateTopologyChangedButUnverifiedError) as raised:
        await change_sample_rate_topology_safe(device, 96_000, probe, mutate)

    assert "exact expected membership and metadata state" in str(raised.value.__cause__)


@pytest.mark.asyncio
async def test_mutation_exception_reports_unknown_outcome_instead_of_pre_send_refusal(install_flow_inventory):
    device = FakeA32([_phase(64, [])])
    install_flow_inventory(device)

    async def probe():
        return 48_000, [48_000, 96_000]

    async def mutate():
        raise OSError("synthetic transport failure")

    with pytest.raises(SampleRateTopologyMutationOutcomeUnknownError, match="device state is unknown"):
        await change_sample_rate_topology_safe(device, 96_000, probe, mutate)


@pytest.mark.asyncio
async def test_post_write_rate_mismatch_reports_changed_but_unverified(install_flow_inventory):
    device = FakeA32([_phase(64, [])])
    install_flow_inventory(device)

    async def probe():
        return 48_000, [48_000, 96_000]

    async def mutate():
        return None

    with pytest.raises(SampleRateTopologyChangedButUnverifiedError) as raised:
        await change_sample_rate_topology_safe(device, 96_000, probe, mutate)

    assert raised.value.observed_sample_rate_hertz == 48_000
    assert "device reports 48000 Hz instead of 96000 Hz" in str(raised.value)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("current_receive_count", "current_transmit_count", "target_receive_count", "target_transmit_count"),
    [
        (64, 0, 32, 0),
        (0, 64, 0, 32),
    ],
)
async def test_preflight_accepts_proven_zero_directional_capacities(
    install_flow_inventory,
    current_receive_count,
    current_transmit_count,
    target_receive_count,
    target_transmit_count,
):
    device = FakeA32([_phase(current_receive_count, [])])
    device.sample_rate_channel_capacities = [
        {
            "sample_rate_hertz": 48_000,
            "receive_channel_count": current_receive_count,
            "transmit_channel_count": current_transmit_count,
        },
        {
            "sample_rate_hertz": 96_000,
            "receive_channel_count": target_receive_count,
            "transmit_channel_count": target_transmit_count,
        },
    ]
    install_flow_inventory(device)

    async def probe():
        return 48_000, [48_000, 96_000]

    preflight = await preflight_sample_rate_change(device, 96_000, probe)

    assert preflight.current_snapshot.capacity.receive_channel_count == current_receive_count
    assert preflight.current_snapshot.capacity.transmit_channel_count == current_transmit_count
    assert preflight.target_capacity.receive_channel_count == target_receive_count
    assert preflight.target_capacity.transmit_channel_count == target_transmit_count


@pytest.mark.asyncio
async def test_non_a32_device_is_refused_without_querying_topology(install_flow_inventory):
    device = FakeA32([_phase(64, [])])
    device.dante_model = "Different Device"
    install_flow_inventory(device)

    async def probe():
        return 48_000, [48_000, 96_000]

    with pytest.raises(SampleRateTopologyUnsupportedError, match="currently proven only"):
        await preflight_sample_rate_change(device, 96_000, probe)


@pytest.mark.asyncio
async def test_unknown_family_authoritative_same_rate_is_a_no_op_without_building_a_write():
    device = FakeA32([_phase(1, [])])
    device.dante_model = "Different Device"

    def fail_command_builder(_sample_rate):
        raise AssertionError("a same-rate no-op must not build a write")

    device.commands = SimpleNamespace(command_set_sample_rate=fail_command_builder)

    class Sender:
        async def probe_sample_rate_status(self, device_ip_address, timeout):
            assert device_ip_address == "192.0.2.10"
            assert timeout == 4.0
            return 96_000, [48_000, 96_000]

        async def __call__(self, *_arguments, **_options):
            raise AssertionError("a same-rate no-op must not send a write")

    result = await change_sample_rate_with_command_sender(Sender(), device, 96_000)

    assert result.changed is False
    assert result.observed_sample_rate_hertz == 96_000
    assert result.preflight.topology_characterized is False
    assert result.resulting_snapshot is None


@pytest.mark.asyncio
async def test_command_sender_wrapper_uses_notification_readback_and_per_device_lock(install_flow_inventory):
    device = FakeA32(
        [
            _phase(64, []),
            _phase(32, []),
        ]
    )
    device.commands = SimpleNamespace(command_set_sample_rate=lambda sample_rate: (b"sample-rate-write", None, 8700))
    install_flow_inventory(device)
    calls = []

    class Sender:
        async def probe_sample_rate_status(self, device_ip_address, timeout):
            assert device.topology_mutation_lock.locked()
            calls.append(("probe", device_ip_address, timeout))
            return (48_000 if device.phase_index == 0 else 96_000), [48_000, 96_000]

        async def __call__(
            self,
            packet,
            device_ip_address,
            port,
            **options,
        ):
            assert device.topology_mutation_lock.locked()
            calls.append(("mutate", packet, device_ip_address, port, options))
            device.phase_index = 1

    result = await change_sample_rate_with_command_sender(Sender(), device, 96_000)

    assert result.changed is True
    assert result.observed_sample_rate_hertz == 96_000
    assert calls == [
        ("probe", "192.0.2.10", 4.0),
        (
            "mutate",
            b"sample-rate-write",
            "192.0.2.10",
            8700,
            {"expect_response": False},
        ),
        ("probe", "192.0.2.10", 4.0),
    ]
