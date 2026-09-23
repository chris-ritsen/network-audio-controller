from types import SimpleNamespace

import pytest

from netaudio import core
from netaudio.asynchronous_primitives import DeferredAsyncioLock
from netaudio.dante import flows
from netaudio.dante.sample_rate_topology import (
    SampleRateTopologyChangedButUnverifiedError,
    SampleRateTopologyConfirmationRequired,
    SampleRateTopologyMutationOutcomeUnknownError,
    SampleRateTopologyUnsupportedError,
    SampleRateTopologyReadbackError,
    SampleRateTopologyVerificationError,
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
        self.sample_rate_update_mode = 2
        self.sample_rate_configuration_supported = True
        self.is_locked = False
        self.diagnostic_log_export_supported = False
        self.sample_rate_channel_capacities = [
            {"sample_rate_hertz": rate, "receive_channel_count": count, "transmit_channel_count": count}
            for rate, count in [(44100, 64), (48000, 64), (88200, 32), (96000, 32), (176400, 16), (192000, 16)]
        ]
        self.rx_channels = {}
        self.tx_channels = {}
        self.subscriptions = []
        self.phases = phases
        self.phase_index = 0
        self.topology_mutation_lock = DeferredAsyncioLock()

    def _arc_port(self):
        return 4440

    async def get_tx_channels(self):
        phase = self.phases[self.phase_index]
        transmit_channel_count = phase.get("transmit_channel_count", phase["receive_channel_count"])
        self.tx_channels = {
            number: SimpleNamespace(number=number, name=f"Output {number}")
            for number in range(1, transmit_channel_count + 1)
        }

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


def _phase(receive_channel_count, flows_state, subscriptions=(), transmit_channel_count=None):
    phase = {
        "receive_channel_count": receive_channel_count,
        "flows": flows_state,
        "subscriptions": subscriptions,
    }
    if transmit_channel_count is not None:
        phase["transmit_channel_count"] = transmit_channel_count
    return phase


def _sample_rate_status(current_value, available_values):
    return {
        "current_value": current_value,
        "requested_value": current_value,
        "update_mode": 2,
        "available_values": available_values,
        "flags": None,
    }


@pytest.mark.parametrize(
    "current,available",
    [(0, [0]), (True, [1]), (48_000, []), (48_000, [96_000]), (48_000, [48_000, 48_000]), (48_000, [48_000, -1])],
)
def test_native_sample_rate_evidence_rejects_unusable_status(current, available):
    with pytest.raises(core.NetaudioCoreError):
        core.sample_rate_status_evidence(_sample_rate_status(current, available))


def test_native_sample_rate_evidence_preserves_advertised_order():
    assert core.sample_rate_status_evidence(_sample_rate_status(48_000, [96_000, 48_000])) == {
        "current_value": 48_000,
        "available_values": [96_000, 48_000],
    }


def test_native_capacity_evidence_distinguishes_unknown_from_conflicting_reports():
    capacity = {"sample_rate_hertz": 48_000, "receive_channel_count": 8, "transmit_channel_count": 8}
    assert core.sample_rate_capacity(None, 48_000) is None
    assert core.sample_rate_capacity([capacity], 96_000) is None
    assert core.sample_rate_capacity([capacity, capacity], 48_000) == capacity

    with pytest.raises(core.NetaudioCoreError, match="conflicting"):
        core.sample_rate_capacity([capacity, {**capacity, "transmit_channel_count": 4}], 48_000)


@pytest.mark.parametrize("count", [True, -1, 65536])
def test_native_capacity_evidence_does_not_coerce_invalid_channel_counts(count):
    with pytest.raises(core.NetaudioCoreError):
        core.sample_rate_capacity(
            [{"sample_rate_hertz": 48_000, "receive_channel_count": count, "transmit_channel_count": 8}], 48_000
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("mode,error", [(0, SampleRateTopologyUnsupportedError), (77, SampleRateTopologyReadbackError)])
async def test_sample_rate_preflight_uses_fresh_update_mode_before_any_write(mode, error, install_flow_inventory):
    device = FakeA32([_phase(64, [])])
    install_flow_inventory(device)
    writes = []

    async def probe():
        status = _sample_rate_status(48_000, [48_000, 96_000])
        status["update_mode"] = mode
        return status

    async def mutate():
        writes.append(96_000)

    with pytest.raises(error, match="update mode"):
        await change_sample_rate_topology_safe(device, 96_000, probe, mutate)

    assert writes == []


@pytest.mark.asyncio
@pytest.mark.parametrize("inventory", ["rekeyed", "duplicate_identity", "boolean_identity"])
async def test_sample_rate_inventory_checks_channel_identity_not_mapping_keys(inventory, install_flow_inventory):
    device = FakeA32([_phase(64, [])])
    install_flow_inventory(device)
    read_channels = device.get_rx_channels

    async def read_rekeyed_channels():
        await read_channels()

        if inventory == "rekeyed":
            device.rx_channels = {f"channel-{key}": channel for key, channel in device.rx_channels.items()}
        elif inventory == "duplicate_identity":
            device.rx_channels[2].number = 1
        else:
            device.rx_channels[1].number = True

    device.get_rx_channels = read_rekeyed_channels

    async def probe():
        return _sample_rate_status(48_000, [48_000, 96_000])

    if inventory == "rekeyed":
        result = await preflight_sample_rate_change(device, 96_000, probe)
        assert result.current_snapshot["capacity"]["receive_channel_count"] == 64
    else:
        with pytest.raises(SampleRateTopologyVerificationError, match="receiver inventory"):
            await preflight_sample_rate_change(device, 96_000, probe)


@pytest.mark.asyncio
async def test_sample_rate_preflight_rejects_conflicting_flow_identities(install_flow_inventory):
    device = FakeA32([_phase(64, [_multicast_flow(3, [1, 33]), _multicast_flow(3, [2, 34])])])
    install_flow_inventory(device)

    async def probe():
        return _sample_rate_status(48_000, [48_000, 96_000])

    with pytest.raises(SampleRateTopologyVerificationError, match="duplicate.*flow"):
        await preflight_sample_rate_change(device, 96_000, probe)


@pytest.mark.asyncio
async def test_sample_rate_readback_rejects_duplicate_flow_identity(install_flow_inventory):
    device = FakeA32(
        [
            _phase(64, [_multicast_flow(3, [1, 2])]),
            _phase(32, [_multicast_flow(3, [1, 2], 96_000), _multicast_flow(3, [1, 2], 96_000)]),
        ]
    )
    install_flow_inventory(device)

    async def probe():
        return _sample_rate_status(48_000 if device.phase_index == 0 else 96_000, [48_000, 96_000])

    async def mutate():
        device.phase_index = 1

    with pytest.raises(SampleRateTopologyChangedButUnverifiedError, match="duplicate.*flow"):
        await change_sample_rate_topology_safe(device, 96_000, probe, mutate)


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


def _native_topology_snapshot(rate=48_000):
    return {
        "capacity": {"sample_rate_hertz": rate, "receive_channel_count": 64, "transmit_channel_count": 64},
        "receiver_subscriptions": [],
        "transmitter_flows": [],
        "flow_protocol_identifier": 0x2729,
    }


@pytest.mark.parametrize(
    "members,target_count,confirmation",
    [([], None, False), ([1, 32], None, True), ([1, 32], 16, True), ([1, 32], 64, False)],
)
def test_native_topology_owns_destructive_confirmation_requirement(members, target_count, confirmation):
    snapshot = _native_topology_snapshot()

    if members:
        snapshot["transmitter_flows"] = [core.transmit_flow_topology(_multicast_flow(1, members), protocol_id=0x2729)]

    target = (
        {"sample_rate_hertz": 96_000, "receive_channel_count": target_count, "transmit_channel_count": target_count}
        if target_count is not None
        else None
    )
    impact = core.sample_rate_topology_impact(snapshot, target)

    assert impact["requires_destructive_confirmation"] is confirmation


@pytest.mark.parametrize("known_capacity", [True, False])
def test_native_topology_readback_requires_target_rate_even_without_flows(known_capacity):
    before = _native_topology_snapshot()
    after = _native_topology_snapshot()
    target = _native_topology_snapshot(96_000)["capacity"] if known_capacity else None

    with pytest.raises(core.NetaudioCoreError, match="sample rate"):
        core.verify_sample_rate_topology(before, after, target, 96_000)


@pytest.mark.parametrize("field", ["sample_rate_hertz", "receive_channel_count", "transmit_channel_count"])
def test_native_topology_readback_requires_exact_target_capacity(field):
    before = _native_topology_snapshot()
    after = _native_topology_snapshot(96_000)
    target = {**after["capacity"], field: 32}

    with pytest.raises(core.NetaudioCoreError, match="capacity"):
        core.verify_sample_rate_topology(before, after, target, 96_000)


@pytest.mark.parametrize("operation", ["impact", "readback"])
def test_native_topology_rejects_flow_rate_inconsistent_with_snapshot(operation):
    snapshot = _native_topology_snapshot()
    snapshot["transmitter_flows"] = [core.transmit_flow_topology(_multicast_flow(1, [1], 96_000), protocol_id=0x2729)]

    with pytest.raises(core.NetaudioCoreError, match="sample rate"):
        if operation == "impact":
            core.sample_rate_topology_impact(snapshot, None)
        else:
            core.verify_sample_rate_topology(snapshot, snapshot, snapshot["capacity"], 48_000)


def test_native_topology_cannot_verify_lost_multicast_flow_using_caller_retirement_flag():
    before = _native_topology_snapshot()
    flow = core.transmit_flow_topology(_multicast_flow(1, [1]), protocol_id=0x2729)
    flow["may_retire_after_sample_rate_change"] = True
    before["transmitter_flows"] = [flow]
    after = _native_topology_snapshot(96_000)

    with pytest.raises(core.NetaudioCoreError, match="retirement"):
        core.verify_sample_rate_topology(before, after, after["capacity"], 96_000)


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
        async def detect_flow_protocol(device_ip, arc_port, *, device):
            assert device_ip == "192.0.2.10"
            assert arc_port == 4440
            return 0x2729

        async def query_tx_flow_inventory(device_ip, arc_port, flow_protocol_identifier):
            assert device_ip == "192.0.2.10"
            assert arc_port == 4440
            assert flow_protocol_identifier == 0x2729
            return {
                "maximum_flow_slots": 32,
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
        return _sample_rate_status(48_000, [44_100, 48_000, 88_200, 96_000, 176_400, 192_000])

    preflight = await preflight_sample_rate_change(device, 192_000, probe)

    assert preflight.target_capacity == {
        "sample_rate_hertz": 192_000,
        "receive_channel_count": 16,
        "transmit_channel_count": 16,
    }
    assert [state["receiver_channel_number"] for state in preflight.reversible_receiver_clipping] == [64]
    assert preflight.destructive_transmitter_membership_loss == [
        {
            "flow_number": 32,
            "flow_type": "multicast",
            "retained_channel_members": [16],
            "removed_channel_members": [17, 18, 19, 20, 21, 22, 23],
        }
    ]
    assert preflight.uncharacterized_transmitter_flows == []


@pytest.mark.asyncio
async def test_destructive_change_is_refused_before_mutation_without_confirmation(install_flow_inventory):
    device = FakeA32([_phase(64, [_multicast_flow(4, [16, 17])])])
    install_flow_inventory(device)
    mutation_called = False

    async def probe():
        return _sample_rate_status(48_000, [48_000, 192_000])

    async def mutate():
        nonlocal mutation_called
        mutation_called = True

    with pytest.raises(SampleRateTopologyConfirmationRequired) as raised:
        await change_sample_rate_topology_safe(device, 192_000, probe, mutate)

    assert mutation_called is False
    assert raised.value.preflight.destructive_transmitter_membership_loss[0]["removed_channel_members"] == [17]


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
        return _sample_rate_status(48_000 if device.phase_index == 0 else 192_000, [48_000, 192_000])

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
    assert result.resulting_snapshot["capacity"]["receive_channel_count"] == 16
    assert [state["receiver_channel_number"] for state in result.resulting_snapshot["receiver_subscriptions"]] == [1]
    assert [state["channel_members"] for state in result.resulting_snapshot["transmitter_flows"]] == [
        [1, 2],
        [16, 0, 0, 0, 0, 0, 0, 0],
    ]


@pytest.mark.asyncio
async def test_unicast_flow_blocks_capacity_reduction_even_with_destructive_confirmation(install_flow_inventory):
    device = FakeA32([_phase(64, [_unicast_flow(3)])])
    install_flow_inventory(device)
    mutation_called = False

    async def probe():
        return _sample_rate_status(48_000, [48_000, 192_000])

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
    assert raised.value.preflight.uncharacterized_transmitter_flows[0]["flow_number"] == 3


@pytest.mark.asyncio
async def test_all_out_of_range_multicast_flow_blocks_unproven_transition(install_flow_inventory):
    device = FakeA32([_phase(64, [_multicast_flow(5, [17, 18])])])
    install_flow_inventory(device)

    async def probe():
        return _sample_rate_status(48_000, [48_000, 192_000])

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

    assert "all active members" in raised.value.preflight.uncharacterized_transmitter_flows[0]["reason"]


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
        return _sample_rate_status(48_000 if device.phase_index == 0 else 192_000, [48_000, 192_000])

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
        return _sample_rate_status(48_000 if device.phase_index == 0 else 96_000, [48_000, 96_000])

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
        return _sample_rate_status(48_000 if device.phase_index == 0 else 96_000, [48_000, 96_000])

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
        return _sample_rate_status(48_000, [48_000, 96_000])

    async def mutate():
        raise OSError("synthetic transport failure")

    with pytest.raises(SampleRateTopologyMutationOutcomeUnknownError, match="device state is unknown"):
        await change_sample_rate_topology_safe(device, 96_000, probe, mutate)


@pytest.mark.asyncio
async def test_post_write_rate_mismatch_reports_changed_but_unverified(install_flow_inventory):
    device = FakeA32([_phase(64, [])])
    install_flow_inventory(device)

    async def probe():
        return _sample_rate_status(48_000, [48_000, 96_000])

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
        return _sample_rate_status(48_000, [48_000, 96_000])

    preflight = await preflight_sample_rate_change(device, 96_000, probe)

    assert preflight.current_snapshot["capacity"]["receive_channel_count"] == current_receive_count
    assert preflight.current_snapshot["capacity"]["transmit_channel_count"] == current_transmit_count
    assert preflight.target_capacity["receive_channel_count"] == target_receive_count
    assert preflight.target_capacity["transmit_channel_count"] == target_transmit_count


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "model", ["Different Device", "A32 Dante AD/DA Converter", "AVIO-DAI2", "AVIO-DAO2", "WING-DANTE64"]
)
async def test_model_name_cannot_replace_capacity_readback(install_flow_inventory, model):
    device = FakeA32([_phase(8, [], transmit_channel_count=8), _phase(4, [], transmit_channel_count=4)])
    device.dante_model = model
    device.sample_rate_channel_capacities = None
    install_flow_inventory(device)
    loads = []

    async def load_channel_capacities():
        loads.append(True)

    async def probe():
        return _sample_rate_status(48_000 if device.phase_index == 0 else 96_000, [48_000, 96_000])

    async def mutate():
        device.phase_index = 1

    result = await change_sample_rate_topology_safe(
        device, 96_000, probe, mutate, load_channel_capacities=load_channel_capacities
    )

    assert loads == [True]
    assert result.changed is True
    assert result.preflight.capacity_known is False
    assert result.preflight.current_snapshot["capacity"]["receive_channel_count"] == 8
    assert result.preflight.current_snapshot["capacity"]["transmit_channel_count"] == 8
    assert result.preflight.target_capacity is None
    assert result.observed_sample_rate_hertz == 96_000
    assert result.resulting_snapshot["capacity"]["receive_channel_count"] == 4
    assert result.resulting_snapshot["capacity"]["transmit_channel_count"] == 4


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "model", ["Different Device", "A32 Dante AD/DA Converter", "AVIO-DAI2", "AVIO-DAO2", "WING-DANTE64"]
)
async def test_unknown_capacity_with_transmitter_flows_requires_confirmation(install_flow_inventory, model):
    device = FakeA32([_phase(8, [_multicast_flow(1, [1, 2])], transmit_channel_count=8)])
    device.dante_model = model
    device.sample_rate_channel_capacities = None
    install_flow_inventory(device)

    async def probe():
        return _sample_rate_status(48_000, [48_000, 96_000])

    async def refuse_write():
        raise AssertionError("an unconfirmed change must not send a write")

    with pytest.raises(SampleRateTopologyConfirmationRequired, match="did not report its channel capacity"):
        await change_sample_rate_topology_safe(device, 96_000, probe, refuse_write)


@pytest.mark.asyncio
async def test_reported_capacity_table_is_used_for_any_model(install_flow_inventory):
    device = FakeA32([_phase(8, [_multicast_flow(1, [1, 2, 7])], transmit_channel_count=8)])
    device.dante_model = "Different Device"
    device.sample_rate_channel_capacities = [
        {"sample_rate_hertz": 48_000, "receive_channel_count": 8, "transmit_channel_count": 8},
        {"sample_rate_hertz": 96_000, "receive_channel_count": 4, "transmit_channel_count": 4},
    ]
    install_flow_inventory(device)

    async def probe():
        return _sample_rate_status(48_000, [48_000, 96_000])

    preflight = await preflight_sample_rate_change(device, 96_000, probe)

    assert preflight.capacity_known is True
    assert preflight.target_capacity["transmit_channel_count"] == 4
    assert [loss["removed_channel_members"] for loss in preflight.destructive_transmitter_membership_loss] == [[7]]


@pytest.mark.asyncio
async def test_unknown_family_authoritative_same_rate_is_a_no_op_without_sending_a_write():
    from netaudio.dante.application import DanteApplication

    device = FakeA32([_phase(1, [])])
    device.dante_model = "Different Device"
    application = DanteApplication()

    async def probe_sample_rate_status(target, timeout):
        assert target is device
        assert timeout == 4.0
        return {
            "current_value": 96_000,
            "requested_value": 96_000,
            "update_mode": 2,
            "available_values": [48_000, 96_000],
            "flags": None,
        }

    async def refuse_write(*_arguments, **_options):
        raise AssertionError("a same-rate no-op must not send a write")

    application.probe_sample_rate_status = probe_sample_rate_status
    application.send_set_sample_rate = refuse_write

    result = await application.set_sample_rate(device, 96_000)

    assert result.changed is False
    assert result.observed_sample_rate_hertz == 96_000
    assert result.preflight.topology_characterized is False
    assert result.resulting_snapshot is None


@pytest.mark.asyncio
@pytest.mark.parametrize("cached_mode,cached_choices", [(2, [48_000, 96_000]), (1, [48_000]), (None, None)])
async def test_application_sample_rate_write_uses_notification_readback_and_per_device_lock(
    install_flow_inventory, cached_mode, cached_choices
):
    from netaudio.dante.application import DanteApplication

    device = FakeA32(
        [
            _phase(64, []),
            _phase(32, []),
        ]
    )
    install_flow_inventory(device)
    device.sample_rate_update_mode = cached_mode
    device.supported_sample_rates = cached_choices
    application = DanteApplication()
    calls = []

    async def probe_sample_rate_status(target, timeout):
        assert device.topology_mutation_lock.locked()
        calls.append(("probe", target, timeout))
        current = 48_000 if device.phase_index == 0 else 96_000
        return {
            "current_value": current,
            "requested_value": current,
            "update_mode": 2,
            "available_values": [48_000, 96_000],
            "flags": None,
        }

    async def set_sample_rate(target, sample_rate):
        assert device.topology_mutation_lock.locked()
        calls.append(("mutate", target, sample_rate))
        device.phase_index = 1

    application.probe_sample_rate_status = probe_sample_rate_status
    application.send_set_sample_rate = set_sample_rate

    result = await application.set_sample_rate(device, 96_000)

    assert result.changed is True
    assert result.observed_sample_rate_hertz == 96_000
    assert calls == [
        ("probe", device, 4.0),
        ("mutate", device, 96_000),
        ("probe", device, 4.0),
    ]


@pytest.mark.asyncio
async def test_sample_rate_permission_rejection_is_not_reported_as_attempted_mutation(install_flow_inventory):
    from netaudio.dante.application import DanteApplication

    device = FakeA32([_phase(64, [])])
    install_flow_inventory(device)
    application = DanteApplication()

    async def probe(target, timeout):
        target.is_locked = True
        return _sample_rate_status(48_000, [48_000, 96_000])

    async def refuse_write(*args):
        raise AssertionError("a locked device must not receive a write")

    application.probe_sample_rate_status = probe
    application.send_set_sample_rate = refuse_write

    with pytest.raises(RuntimeError, match="not writable: device_locked") as error:
        await application.set_sample_rate(device, 96_000)

    assert not isinstance(error.value, SampleRateTopologyMutationOutcomeUnknownError)
