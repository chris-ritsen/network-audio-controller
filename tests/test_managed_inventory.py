from collections import deque
from dataclasses import replace

import pytest

from netaudio.common.managed_api import ManagedAPIConfiguration
from netaudio.daemon.managed_inventory import (
    ManagedDeviceObservation,
    ManagedInventoryService,
    merge_device_inventory,
)
from netaudio.dante.channel import DanteChannel
from netaudio.dante.device import DanteDevice
from netaudio.dante.device_serializer import DanteDeviceSerializer
from netaudio.ddm import (
    Device,
    DeviceConnection,
    DeviceIdentity,
    Domain,
    GraphQLIssue,
    Inventory,
    InventoryResult,
    NetworkInterface,
    RxChannel,
    SignalPresence,
    TxChannel,
)


class FakeClient:
    def __init__(self, *results):
        self.results = deque(results)

    async def inventory_async(self):
        result = self.results.popleft()
        if isinstance(result, Exception):
            raise result
        return result


class FakeClock:
    def __init__(self, value=1000.0):
        self.value = value

    def __call__(self):
        return self.value


def _direct_device(
    server_name,
    *,
    name="direct-device",
    address="192.0.2.10",
    mac="001122334455",
):
    device = DanteDevice(server_name=server_name)
    device.name = name
    device.ipv4 = address
    device.mac_address = mac
    device.sample_rate = 48000
    device.supported_sample_rates = [44100, 48000, 96000]
    device.encoding = 24
    device.supported_encodings = [16, 24, 32]
    device.latency = 0.25
    device.online = True

    channel = DanteChannel()
    channel.channel_type = "tx"
    channel.device = device
    channel.number = 1
    channel.name = "direct-tx"
    device.tx_channels[1] = channel
    return device


def _managed_device(
    device_id="managed-1",
    *,
    name="managed-device",
    domain_id="domain-1",
    address="192.0.2.10",
    mac="00:11:22:33:44:55",
    connection_state="READY",
    signal_status="CLIPPING",
):
    identity = DeviceIdentity(
        id=f"identity-{device_id}",
        instance_id=f"instance-{device_id}",
        default_name=f"default-{device_id}",
        actual_name=name,
        product_model_id="model-id",
        product_model_name="Managed Adapter",
        product_version="1.2.3",
        product_software_version="4.5.6",
        dante_version="4.2.4.1",
        dante_hardware_version="4.2.3.4",
    )
    interface = NetworkInterface(
        id=f"interface-{device_id}",
        mac_address=mac,
        address=address,
        netmask=24,
        subnet="192.0.2.0",
    )
    presence = SignalPresence(id=f"signal-{device_id}", level_dbfs=-0.5, status=signal_status)
    rx_channel = RxChannel(
        id=f"rx-{device_id}",
        index=1,
        enabled=True,
        name="managed-rx",
        subscribed_device="source-device",
        subscribed_channel="source-channel",
        status="DYNAMIC",
        status_message="Subscription active",
        summary="CONNECTED",
        media_type="AUDIO",
        encryption_scheme="NONE",
        can_subscribe_self=False,
        signal_presence=presence,
    )
    tx_channel = TxChannel(
        id=f"tx-{device_id}",
        index=1,
        name="managed-tx",
        media_type="AUDIO",
        encryption_policy="OPTIONAL",
        signal_presence=presence,
    )
    return Device(
        id=device_id,
        name=name,
        domain_id=domain_id,
        type="MANAGED",
        enrolment_state="ENROLLED" if domain_id else "UNENROLLED",
        identity=identity,
        manufacturer=None,
        platform=None,
        product=None,
        interfaces=(interface,),
        connection=DeviceConnection(
            id=f"connection-{device_id}",
            state=connection_state,
            last_changed="2026-08-26T12:00:00Z",
        ),
        clock_preferences=None,
        capabilities=None,
        clocking_state=None,
        status=None,
        rx_channels=(rx_channel,),
        tx_channels=(tx_channel,),
        parameters=None,
        inputs=None,
        outputs=None,
    )


def _observation(device, *, domain_id="domain-1", domain_name="test"):
    return ManagedDeviceObservation(device=device, domain_id=domain_id, domain_name=domain_name)


def _result(*, domains=(), unenrolled=(), errors=()):
    return InventoryResult(
        data=Inventory(domains=domains, unenrolled_devices=unenrolled),
        errors=errors,
        raw_data={},
    )


def _domain(domain_id, *devices):
    return Domain(id=domain_id, name=domain_id, status=None, devices=devices)


def _service(client, clock):
    configuration = ManagedAPIConfiguration(
        url="http://manager.example/graphql",
        credential="test-key",
        credential_file=None,
        refresh_interval=10.0,
    )
    return ManagedInventoryService(configuration, client=client, clock=clock)


def test_mac_correlation_accepts_direct_16_hex_suffix_and_preserves_direct_audio_values():
    direct = _direct_device("direct.local.", mac="0011223344550000")
    managed = _managed_device(mac="00:11:22:33:44:55")

    merged = merge_device_inventory(
        {direct.server_name: direct},
        (_observation(managed),),
        synced_at=1234.0,
        fresh=True,
    )

    assert list(merged) == ["direct.local."]
    record = merged["direct.local."]
    assert record["inventory_sources"] == ["direct", "ddm"]
    assert record["sample_rate_hz"] == 48000
    assert record["supported_sample_rates_hz"] == [44100, 48000, 96000]
    assert record["encoding"] == 24
    assert record["supported_encodings"] == [16, 24, 32]
    assert record["latency_ms"] == 0.25
    assert record["field_sources"]["audio_configuration"] == "direct"
    assert record["channels"]["transmitters"][1]["name"] == "direct-tx"
    assert record["channels"]["transmitters"][1]["ddm_signal_presence"] == {
        "id": "signal-managed-1",
        "level_dbfs": -0.5,
        "status": "CLIPPING",
    }


def test_mac_correlation_accepts_dante_eui64_with_inserted_fffe():
    direct = _direct_device("direct.local.", mac="001122fffe334455")
    managed = _managed_device(mac="00:11:22:33:44:55")

    merged = merge_device_inventory(
        {direct.server_name: direct},
        (_observation(managed),),
        synced_at=1234.0,
        fresh=True,
    )

    assert list(merged) == ["direct.local."]
    assert merged["direct.local."]["inventory_sources"] == ["direct", "ddm"]


def test_matching_name_and_ip_do_not_override_conflicting_macs():
    direct = _direct_device(
        "same.local.",
        name="same-name",
        address="192.0.2.20",
        mac="001122334455",
    )
    managed = _managed_device(
        name="same-name",
        address="192.0.2.20",
        mac="00:aa:bb:cc:dd:ee",
    )

    merged = merge_device_inventory(
        {direct.server_name: direct},
        (_observation(managed),),
        synced_at=1234.0,
        fresh=True,
    )

    assert set(merged) == {"same.local.", "ddm:managed-1"}
    assert merged["same.local."]["inventory_sources"] == ["direct"]
    assert merged["ddm:managed-1"]["inventory_sources"] == ["ddm"]


def test_missing_managed_mac_allows_unique_ip_fallback():
    direct = _direct_device("direct.local.", address="192.0.2.30")
    managed = _managed_device(address="192.0.2.30", mac=None)

    merged = merge_device_inventory(
        {direct.server_name: direct},
        (_observation(managed),),
        synced_at=1234.0,
        fresh=True,
    )

    assert list(merged) == ["direct.local."]
    assert merged["direct.local."]["ddm_device_id"] == "managed-1"


def test_ip_fallback_refuses_ambiguous_direct_candidates():
    first = _direct_device("first.local.", address="192.0.2.40", mac="001122334401")
    second = _direct_device("second.local.", address="192.0.2.40", mac="001122334402")
    managed = _managed_device(address="192.0.2.40", mac=None)

    merged = merge_device_inventory(
        {first.server_name: first, second.server_name: second},
        (_observation(managed),),
        synced_at=1234.0,
        fresh=True,
    )

    assert set(merged) == {"first.local.", "second.local.", "ddm:managed-1"}
    assert all(merged[key]["inventory_sources"] == ["direct"] for key in ("first.local.", "second.local."))


def test_ddm_only_device_has_normalized_channels_raw_signal_and_subscription_status():
    managed = _managed_device(address="192.0.2.50", mac="00:11:22:33:44:99")

    merged = merge_device_inventory(
        {},
        (_observation(managed),),
        synced_at=1234.0,
        fresh=True,
    )

    record = merged["ddm:managed-1"]
    assert record["name"] == "managed-device"
    assert record["ipv4"] == "192.0.2.50"
    assert record["mac_address"] == "00:11:22:33:44:99"
    assert record["management_state"] == "managed"
    assert record["direct_control_available"] is False
    assert record["online"] is True
    assert record["product_version"] == "1.2.3"
    assert record["software_version"] == "4.5.6"
    assert record["firmware_version"] == "4.2.4.1"
    assert record["ddm_identity"]["dante_hardware_version"] == "4.2.3.4"
    assert record["channels"]["receivers"]["1"] == {
        "name": "managed-rx",
        "ddm_channel_id": "rx-managed-1",
        "ddm_media_type": "AUDIO",
        "ddm_signal_presence": {
            "id": "signal-managed-1",
            "level_dbfs": -0.5,
            "status": "CLIPPING",
        },
        "ddm_enabled": True,
        "ddm_status": "DYNAMIC",
        "ddm_status_message": "Subscription active",
        "ddm_summary": "CONNECTED",
        "ddm_encryption_scheme": "NONE",
        "ddm_can_subscribe_self": False,
    }
    assert record["subscriptions"] == [
        {
            "rx_channel": "managed-rx",
            "rx_device": "managed-device",
            "tx_channel": "source-channel",
            "tx_device": "source-device",
            "status": {
                "code": None,
                "detail": "Subscription active",
                "icon": "",
                "label": "CONNECTED",
                "severity": "ok",
                "state": "connected",
                "status": "DYNAMIC",
            },
            "ddm_status": "DYNAMIC",
            "ddm_status_message": "Subscription active",
            "ddm_summary": "CONNECTED",
        }
    ]


@pytest.mark.parametrize("status", [None, "NONE", "none", "NoNe"])
def test_ddm_idle_receiver_does_not_create_fake_subscription(status):
    managed = _managed_device()
    idle_channel = replace(
        managed.rx_channels[0],
        subscribed_device=None,
        subscribed_channel=None,
        status=status,
    )
    managed = replace(managed, rx_channels=(idle_channel,))

    merged = merge_device_inventory(
        {},
        (_observation(managed),),
        synced_at=1234.0,
        fresh=True,
    )

    assert merged["ddm:managed-1"]["subscriptions"] == []


@pytest.mark.parametrize("status", ["UNRESOLVED", "PENDING", "ERROR"])
def test_ddm_receiver_without_source_retains_non_idle_status(status):
    managed = _managed_device()
    state_channel = replace(
        managed.rx_channels[0],
        subscribed_device=None,
        subscribed_channel=None,
        status=status,
    )
    managed = replace(managed, rx_channels=(state_channel,))

    merged = merge_device_inventory(
        {},
        (_observation(managed),),
        synced_at=1234.0,
        fresh=True,
    )

    subscriptions = merged["ddm:managed-1"]["subscriptions"]
    assert len(subscriptions) == 1
    assert subscriptions[0]["ddm_status"] == status


def test_ddm_named_subscription_is_retained_when_status_is_none_state():
    managed = _managed_device()
    named_channel = replace(managed.rx_channels[0], status="NONE")
    managed = replace(managed, rx_channels=(named_channel,))

    merged = merge_device_inventory(
        {},
        (_observation(managed),),
        synced_at=1234.0,
        fresh=True,
    )

    subscriptions = merged["ddm:managed-1"]["subscriptions"]
    assert len(subscriptions) == 1
    assert subscriptions[0]["tx_channel"] == "source-channel"
    assert subscriptions[0]["tx_device"] == "source-device"


def test_ddm_only_device_omits_malformed_compatibility_address():
    managed = _managed_device(address="not-an-ip")

    merged = merge_device_inventory(
        {},
        (_observation(managed),),
        synced_at=1234.0,
        fresh=True,
    )

    record = merged["ddm:managed-1"]
    assert record["ipv4"] == "None"
    assert DanteDeviceSerializer.device_from_json(record).ipv4 is None


@pytest.mark.parametrize(
    ("connection_state", "expected_state"),
    [("DISCONNECTED", "offline"), ("ESTABLISHED", "unknown"), (None, "unknown")],
)
def test_ddm_availability_preserves_non_ready_states(connection_state, expected_state):
    managed = _managed_device(connection_state=connection_state)

    merged = merge_device_inventory(
        {},
        (_observation(managed),),
        synced_at=1234.0,
        fresh=True,
    )

    record = merged["ddm:managed-1"]
    assert record["online"] is False
    assert record["availability_state"] == expected_state


@pytest.mark.asyncio
async def test_complete_refresh_replaces_both_inventory_roots():
    first_domain_device = _managed_device("domain-old")
    first_unenrolled = _managed_device("unmanaged-old", domain_id=None)
    second_domain_device = _managed_device("domain-new")
    client = FakeClient(
        _result(domains=(_domain("old-domain", first_domain_device),), unenrolled=(first_unenrolled,)),
        _result(domains=(_domain("new-domain", second_domain_device),), unenrolled=()),
    )
    service = _service(client, FakeClock())

    assert await service.refresh() is True
    assert await service.refresh() is True

    observations = service.observations()
    assert [(item.device.id, item.domain_id) for item in observations] == [("domain-new", "new-domain")]
    assert service.status()["domain_count"] == 1
    assert service.status()["unenrolled_device_count"] == 0


@pytest.mark.asyncio
async def test_partial_refresh_replaces_present_root_and_preserves_omitted_root():
    retained = _managed_device("retained")
    old_unenrolled = _managed_device("unmanaged-old", domain_id=None)
    added = _managed_device("added")
    new_unenrolled = _managed_device("unmanaged-new", domain_id=None)
    issue = GraphQLIssue(message="one inventory branch failed", raw={"message": "one inventory branch failed"})
    client = FakeClient(
        _result(domains=(_domain("retained-domain", retained),), unenrolled=(old_unenrolled,)),
        _result(domains=(_domain("added-domain", added),), unenrolled=None, errors=(issue,)),
        _result(domains=None, unenrolled=(new_unenrolled,), errors=(issue,)),
    )
    service = _service(client, FakeClock())

    await service.refresh()
    service.clock.value += 1
    await service.refresh()

    second_records = service.serialize_devices({})
    assert second_records["ddm:added"]["ddm_last_sync"] == 1001.0
    assert second_records["ddm:added"]["availability_state"] == "online"
    assert second_records["ddm:unmanaged-old"]["ddm_last_sync"] == 1000.0
    assert second_records["ddm:unmanaged-old"]["availability_state"] == "unknown"

    service.clock.value += 1
    await service.refresh()

    observations = {(item.device.id, item.domain_id) for item in service.observations()}
    assert observations == {
        ("added", "added-domain"),
        ("unmanaged-new", None),
    }
    assert service.status()["state"] == "degraded"
    assert service.status()["graphql_errors"] == ["one inventory branch failed"]
    assert service.status()["domains_fresh"] is False
    assert service.status()["unenrolled_fresh"] is True


@pytest.mark.asyncio
async def test_fresh_unenrolled_root_beats_preserved_stale_domain_record():
    managed = _managed_device("moving-device")
    unenrolled = _managed_device("moving-device", domain_id=None)
    issue = GraphQLIssue(message="domains unavailable", raw={"message": "domains unavailable"})
    client = FakeClient(
        _result(domains=(_domain("old-domain", managed),), unenrolled=()),
        _result(domains=None, unenrolled=(unenrolled,), errors=(issue,)),
    )
    service = _service(client, FakeClock())

    await service.refresh()
    service.clock.value += 1
    await service.refresh()

    observations = service.observations()
    assert [(item.device.id, item.domain_id, item.fresh) for item in observations] == [("moving-device", None, True)]
    assert service.serialize_devices({})["ddm:moving-device"]["management_state"] == "unenrolled"


@pytest.mark.asyncio
async def test_failed_graphql_refresh_preserves_last_inventory_and_reports_degraded():
    managed = _managed_device("known-device")
    issue = GraphQLIssue(message="inventory unavailable", raw={"message": "inventory unavailable"})
    failed = InventoryResult(data=None, errors=(issue,), raw_data=None)
    clock = FakeClock()
    service = _service(FakeClient(_result(domains=(_domain("known-domain", managed),)), failed), clock)

    assert await service.refresh() is True
    clock.value += 1
    assert await service.refresh() is False

    assert [(item.device.id, item.domain_id) for item in service.observations()] == [("known-device", "known-domain")]
    assert service.status()["state"] == "degraded"
    assert service.status()["last_error"] == "inventory unavailable"
    assert service.status()["last_success"] == 1000.0
