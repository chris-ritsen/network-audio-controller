from unittest.mock import AsyncMock
from types import SimpleNamespace

import pytest

from netaudio.dante.application import DanteApplication
from netaudio import core
from netaudio.dante.channel import DanteChannel
from netaudio.dante.device import DanteDevice
from netaudio.dante.const import SERVICE_ARC
from netaudio.dante.self_connection import (
    SelfConnectionCapabilityUnavailableError,
    SelfConnectionUnsupportedError,
    is_self_connection_request,
    receiver_self_connection_support,
    same_canonical_device,
)


def _receiver_device(server_name="receiver.local.", name="Receiver"):
    application = DanteApplication()
    device = DanteDevice(server_name, app=application)
    device.name = name
    device.services = {"arc": {"type": SERVICE_ARC, "properties": {"arcp_vers": "2.7.255"}}}
    channel = DanteChannel()
    channel.channel_type = "rx"
    channel.device = device
    channel.number = 1
    channel.name = "Input"
    device.rx_channels = {1: channel}
    application.attach_devices({server_name: device})
    device.execute = AsyncMock(return_value=b"acknowledged")
    return application, device, channel


def _managed_receiver(capability):
    fresh_channel = SimpleNamespace(index=1, can_subscribe_self=capability)
    managed = SimpleNamespace(
        fetch_device=AsyncMock(return_value=SimpleNamespace(rx_channels=(fresh_channel,))),
        set_subscriptions=AsyncMock(return_value="accepted"),
    )
    application = DanteApplication(managed_transport=managed)
    device = DanteDevice("ddm:receiver", app=application)
    device.name = "Managed Receiver"
    device.ddm_enrolment_state = "ENROLLED"
    device.ddm_device_id = "managed-device-id"
    device.ddm_server_profile = "manager"
    device.ddm_context = "main"
    device.ddm_domain_id = "domain"
    channel = DanteChannel()
    channel.channel_type = "rx"
    channel.device = device
    channel.number = 1
    channel.name = "Input"
    device.rx_channels = {1: channel}
    application.attach_devices({device.server_name: device})
    return application, managed, device, channel


@pytest.mark.parametrize(
    ("values", "expected"),
    [
        ([True, True], "supported"),
        ([False, False], "unsupported"),
        ([True, False], "mixed"),
        ([True, None], "partial"),
        ([False, None], "partial"),
        ([None, None], "unknown"),
        ([], "unknown"),
    ],
)
def test_receiver_self_connection_summary(values, expected):
    channels = []
    for value in values:
        channel = DanteChannel()
        channel.can_subscribe_self = value
        channels.append(channel)
    assert receiver_self_connection_support(channels) == expected


@pytest.mark.parametrize(
    "authority,direct,managed,fresh,supported,conflict",
    [
        ("direct", True, False, True, None, True),
        ("direct", True, False, False, True, False),
        ("direct", None, True, True, None, False),
        ("managed", True, False, True, None, True),
        ("managed", True, None, True, None, False),
        ("managed", True, True, False, None, False),
        ("managed", None, False, True, False, False),
        ("observed", None, True, True, True, False),
        ("observed", True, None, True, True, False),
        ("observed", None, True, False, None, False),
        ("observed", False, True, True, None, True),
        ("observed", True, True, True, True, False),
    ],
)
def test_native_self_connection_evidence_respects_authority_and_freshness(
    authority, direct, managed, fresh, supported, conflict
):
    result = core.receiver_self_connection_capabilities(
        {
            "authority": authority,
            "channels": [{"direct": direct, "managed": managed, "managed_fresh": fresh}],
        }
    )

    assert result["channels"] == [{"supported": supported, "conflict": conflict}]
    assert result["support"] == ("unknown" if supported is None else "supported" if supported else "unsupported")


@pytest.mark.parametrize("damage", ["authority", "direct", "freshness", "unknown_field"])
def test_native_self_connection_evidence_rejects_guessed_inputs(damage):
    request = {"authority": "direct", "channels": [{"direct": True, "managed": None, "managed_fresh": False}]}

    if damage == "authority":
        request["authority"] = "guess"
    elif damage == "direct":
        request["channels"][0]["direct"] = 1
    elif damage == "freshness":
        del request["channels"][0]["managed_fresh"]
    else:
        request["channels"][0]["model"] = "assumed supported"

    with pytest.raises(core.NetaudioCoreError):
        core.receiver_self_connection_capabilities(request)


def test_canonical_identity_survives_rename_and_display_name_is_not_identity():
    receiver = DanteDevice("stable-receiver.local.")
    receiver.name = "Renamed Receiver"
    receiver.mac_address = "00:11:22:33:44:55"
    correlated = DanteDevice("other-service.local.")
    correlated.name = "Renamed Receiver"
    correlated.mac_address = "0011223344550000"
    same_name = DanteDevice("different-device.local.")
    same_name.name = "Renamed Receiver"
    same_name.mac_address = "00:11:22:33:44:66"

    assert same_canonical_device(receiver, correlated) is True
    assert same_canonical_device(receiver, same_name) is False
    assert is_self_connection_request(receiver, "stable-receiver.local.", [receiver, same_name]) is True
    assert is_self_connection_request(receiver, "Renamed Receiver", [receiver, correlated]) is True
    assert is_self_connection_request(receiver, "Renamed Receiver", [receiver, same_name]) is False


@pytest.mark.parametrize("address", ["0011.2233.4455", "00-11-22-33-44-55", "001122fffe334455", "0011223344550000"])
def test_self_connection_identity_uses_the_same_native_address_as_display(address):
    from netaudio.commands.device.display import format_mac_address

    first = SimpleNamespace(mac_address=address)
    second = SimpleNamespace(mac_address="00:11:22:33:44:55")

    assert same_canonical_device(first, second)
    assert core.canonical_device_mac(address) == "001122334455"
    assert format_mac_address(address) == "00:11:22:33:44:55"


@pytest.mark.parametrize("address", ["000000000000", "000000fffe000000", "zz001122334455", "", None])
def test_unavailable_or_invalid_mac_is_not_proof_of_self_connection(address):
    assert not same_canonical_device(SimpleNamespace(mac_address=address), SimpleNamespace(mac_address=address))


@pytest.mark.asyncio
async def test_fresh_false_refuses_self_connection_before_sending():
    application, device, channel = _receiver_device(name="Renamed Receiver")

    async def refresh():
        channel.receiver_flags = 0x0006
        channel.can_subscribe_self = False

    device.get_rx_channels = AsyncMock(side_effect=refresh)

    with pytest.raises(SelfConnectionUnsupportedError, match="does not advertise"):
        await application.send_add_subscriptions(device, [(1, "Output", "receiver.local.")])

    device.get_rx_channels.assert_awaited_once_with()
    device.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_fresh_true_permits_normal_subscription_without_claiming_effective_state():
    application, device, channel = _receiver_device(name="Renamed-Receiver")

    async def refresh():
        channel.receiver_flags = 0x000F
        channel.can_subscribe_self = True
        device.rx_channels = {"Input": channel}

    device.get_rx_channels = AsyncMock(side_effect=refresh)

    response = await application.send_add_subscriptions(device, [(1, "Output", device.name)])

    assert response == b"acknowledged"
    assert device.subscriptions == []
    device.get_rx_channels.assert_awaited_once_with()
    device.execute.assert_awaited_once()


@pytest.mark.asyncio
async def test_unknown_fresh_capability_refuses_self_connection_before_sending():
    application, device, channel = _receiver_device()

    async def refresh():
        channel.receiver_flags = None
        channel.receiver_capability_flags = None
        channel.can_subscribe_self = None

    device.get_rx_channels = AsyncMock(side_effect=refresh)

    with pytest.raises(SelfConnectionCapabilityUnavailableError, match="capability is unavailable"):
        await application.send_add_subscriptions(device, [(1, "Output", ".")])

    device.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("capability", "exception"),
    [
        (False, SelfConnectionUnsupportedError),
        (None, SelfConnectionCapabilityUnavailableError),
    ],
)
async def test_managed_preflight_requires_fresh_conclusive_support(capability, exception):
    application, managed, device, channel = _managed_receiver(capability)

    with pytest.raises(exception):
        await application.add_subscriptions(device, [(1, "Output", ".")])

    managed.fetch_device.assert_awaited_once_with(device)
    managed.set_subscriptions.assert_not_awaited()
    assert channel.can_subscribe_self is capability


@pytest.mark.asyncio
async def test_managed_fresh_true_permits_subscription_processing():
    application, managed, device, channel = _managed_receiver(True)

    result = await application.add_subscriptions(device, [(1, "Output", ".")])

    assert result == "accepted"
    assert channel.can_subscribe_self is True
    managed.set_subscriptions.assert_awaited_once_with(device, [(1, "Output", ".")])


@pytest.mark.asyncio
async def test_serialized_capability_conflict_still_blocks_managed_self_connection():
    from netaudio.dante.device_serializer import DanteDeviceSerializer

    application, managed, device, channel = _managed_receiver(True)
    channel.direct_can_subscribe_self = False
    channel.managed_can_subscribe_self = True
    channel.managed_can_subscribe_self_fresh = True
    channel.can_subscribe_self = None
    channel.can_subscribe_self_conflict = True
    restored = DanteDeviceSerializer.device_from_json(DanteDeviceSerializer.to_json(device))
    device.rx_channels = restored.rx_channels

    with pytest.raises(SelfConnectionCapabilityUnavailableError):
        await application.add_subscriptions(device, [(1, "Output", ".")])

    managed.set_subscriptions.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("inventory", ["rekeyed", "wrong_channel", "duplicate"])
async def test_managed_self_connection_permission_uses_receiver_identity(inventory):
    application, managed, device, channel = _managed_receiver(True)

    if inventory == "rekeyed":
        device.rx_channels = {"Input": channel}
    elif inventory == "wrong_channel":
        channel.number = 2
    else:
        duplicate = DanteChannel()
        duplicate.number = 1
        device.rx_channels[2] = duplicate

    if inventory == "rekeyed":
        assert await application.add_subscriptions(device, [(1, "Output", ".")]) == "accepted"
        managed.set_subscriptions.assert_awaited_once()
    else:
        with pytest.raises(SelfConnectionCapabilityUnavailableError):
            await application.add_subscriptions(device, [(1, "Output", ".")])

        managed.set_subscriptions.assert_not_awaited()


@pytest.mark.asyncio
async def test_conflicting_fresh_managed_and_direct_observations_are_unavailable(load_fixture):
    application, managed, device, channel = _managed_receiver(False)
    records = core.parse_page("rx", load_fixture("20250517_200646_289003_lx-dante_get_receivers_response.bin"), 1)
    device.rx_channels, _ = device._build_rx_from_records(records)
    channel = device.rx_channels[1]

    # The parsed capability remains authoritative without the diagnostic wire flags.
    channel.receiver_flags = None

    with pytest.raises(SelfConnectionCapabilityUnavailableError):
        await application.add_subscriptions(device, [(1, "Output", ".")])

    assert channel.can_subscribe_self is None
    assert channel.can_subscribe_self_conflict is True
    managed.set_subscriptions.assert_not_awaited()

    managed.fetch_device.return_value = SimpleNamespace(
        rx_channels=(SimpleNamespace(index=1, can_subscribe_self=True),)
    )
    assert await application.add_subscriptions(device, [(1, "Output", ".")]) == "accepted"
    assert channel.can_subscribe_self is True
    assert channel.can_subscribe_self_conflict is None


@pytest.mark.asyncio
@pytest.mark.parametrize("ambiguous", [False, True])
async def test_legacy_refresh_cannot_lose_managed_denial_when_channel_keys_change(load_fixture, ambiguous):
    application, device, channel = _receiver_device()
    channel.managed_can_subscribe_self = False
    channel.managed_can_subscribe_self_fresh = True
    device.rx_channels = {"Input": channel}

    if ambiguous:
        duplicate = DanteChannel()
        duplicate.number = 1
        device.rx_channels["Duplicate"] = duplicate

    records = core.parse_page("rx", load_fixture("20250517_200646_289003_lx-dante_get_receivers_response.bin"), 1)

    async def refresh():
        device.rx_channels, device.subscriptions = device._build_rx_from_records(records)

    device.get_rx_channels = AsyncMock(side_effect=refresh)

    with pytest.raises(SelfConnectionCapabilityUnavailableError):
        await application.add_subscriptions(device, [(1, "Output", ".")])

    device.execute.assert_not_awaited()

    if not ambiguous:
        assert device.rx_channels[1].can_subscribe_self is None
        assert device.rx_channels[1].can_subscribe_self_conflict is True


@pytest.mark.asyncio
async def test_routing_uses_only_the_target_receiver_channel_capability():
    application, device, first = _receiver_device()
    second = DanteChannel()
    second.channel_type = "rx"
    second.device = device
    second.number = 2
    second.name = "Input 2"
    device.rx_channels[2] = second

    async def refresh():
        first.receiver_flags = 0x000F
        first.can_subscribe_self = True
        second.receiver_flags = 0x0006
        second.can_subscribe_self = False

    device.get_rx_channels = AsyncMock(side_effect=refresh)

    assert await application.send_add_subscriptions(device, [(1, "Output", ".")]) == b"acknowledged"


@pytest.mark.asyncio
async def test_identical_display_names_on_different_devices_do_not_trigger_self_preflight():
    application, receiver, _channel = _receiver_device(name="Duplicate")
    transmitter = DanteDevice("transmitter.local.")
    transmitter.name = "Duplicate"
    transmitter.mac_address = "00:11:22:33:44:99"
    application.attach_devices({transmitter.server_name: transmitter})
    receiver.get_rx_channels = AsyncMock()

    assert await application.send_add_subscriptions(receiver, [(1, "Output", "Duplicate")]) == b"acknowledged"

    receiver.get_rx_channels.assert_not_awaited()
    receiver.execute.assert_awaited_once()


def test_subscription_status_readback_does_not_overwrite_advertised_capability():
    device = DanteDevice("receiver.local.")
    device.name = "Receiver"
    for status_code in (0x0004, 0x0022):
        device.apply_receiver_channel_inventory(
            {
                "records": [
                    {
                        "channel_number": 1,
                        "local_channel_name": "Input",
                        "source_device_name": "Receiver" if status_code == 0x0004 else ".",
                        "source_channel_name": "Output",
                        "is_self_connection": True,
                        "receiver_capability_flags": 0x0000_0008,
                        "can_subscribe_self": True,
                        "subscription_status_code": status_code,
                        "receiver_status_code": 0x0101,
                        "status_flags": 0x0202,
                    }
                ]
            }
        )
        status = device.subscriptions[0].to_json()["status"]
        assert device.subscriptions[0].is_self_connection is True
        assert device.rx_channels[1].can_subscribe_self is True
        assert device.rx_channels[1].receiver_capability_flags == 0x0000_0008
        assert device.rx_channels[1].receiver_status_flags == 0x0202
        assert status["state"] == ("connected" if status_code == 0x0004 else "error")

    device.name = "Renamed Receiver"
    assert device.subscriptions[0].tx_device_name == "Renamed Receiver"
    assert device.subscriptions[0].is_self_connection is True
