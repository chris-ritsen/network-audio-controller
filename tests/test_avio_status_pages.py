from netaudio.dante.channel import DanteChannel
from netaudio.dante.device import DanteDevice
from netaudio.dante.device_serializer import DanteDeviceSerializer
from netaudio.dante.flows import inventory_from_receiver_flow_status_page


def test_receiver_flow_status_page_becomes_receiver_flow_inventory():
    inventory = inventory_from_receiver_flow_status_page(
        {
            "maximum_flow_slots": 8,
            "flows": [
                {
                    "global_flow_id": 1,
                    "media_type_code": 3,
                    "media_local_flow_id": 1,
                    "flow_type_code": 17,
                    "local_receiver_channel_count": 1,
                    "status_code": 9,
                    "destination_internet_protocol_version_four_address": "239.255.0.1",
                    "destination_user_datagram_port": 5004,
                    "sample_rate": 48000,
                    "encoding": 24,
                    "latency_nanoseconds": 1000000,
                }
            ],
        }
    )
    assert inventory["maximum_flow_slots"] == 8
    assert inventory["flows"][0]["flow_type"] == "0x0011"
    assert inventory["flows"][0]["status_code"] == 9
    assert "subscription_status_code" not in inventory["flows"][0]
    assert inventory["flows"][0]["sample_rate"] == 48000


def test_receiver_flow_status_page_stores_configured_latency():
    device = DanteDevice()
    device.apply_receiver_flow_status_page(
        {
            "result_code": 1,
            "page_disposition": "complete",
            "reported_flow_count": 1,
            "flows": [{"latency_nanoseconds": 4000000}],
        }
    )
    assert device.rx_flow_count == 1
    assert device.receiver_flow_latency_nanoseconds == 4000000


def test_receiver_flow_status_page_clears_latency_when_empty():
    device = DanteDevice()
    device.receiver_flow_latency_nanoseconds = 1000000
    device.apply_receiver_flow_status_page(
        {"result_code": 1, "page_disposition": "complete", "reported_flow_count": 0, "flows": []}
    )
    assert device.rx_flow_count == 0
    assert device.receiver_flow_latency_nanoseconds is None
    assert device.receiver_flows == []


def test_receiver_flow_status_page_preserves_complete_records_for_serialization():
    device = DanteDevice()
    flow = {
        "flow_number": 1,
        "flow_type": "unicast",
        "latency_nanoseconds": 1000000,
        "status_code_at_record_offset_62": 7,
    }
    device.apply_receiver_flow_status_page({"result_code": 1, "page_disposition": "complete", "flows": [flow]})

    assert device.rx_flow_count == 1
    assert device.receiver_flows == [flow]
    assert DanteDeviceSerializer.to_json(device)["receiver_flows"] == [flow]


def test_transmitter_channel_status_page_sets_controller_and_factory_names():
    device = DanteDevice()
    channel = DanteChannel()
    channel.number = 1
    channel.name = "bluetooth:left"
    channel.friendly_name = "bluetooth:left"
    device.tx_channels = {1: channel}

    device.apply_transmitter_channel_status_page(
        {
            "records": [
                {
                    "channel_number": 1,
                    "channel_name": "bluetooth:left",
                    "friendly_channel_name": "Left",
                }
            ]
        }
    )

    assert channel.name == "bluetooth:left"
    assert channel.friendly_name == "Left"
    assert channel.factory_name == "Left"
    assert DanteDeviceSerializer.channel_to_json(channel)["factory_name"] == "Left"


def test_transmitter_flow_status_page_stores_destination_and_subscriber():
    device = DanteDevice()
    device.apply_transmitter_flow_status_page(
        {
            "reported_flow_count": 1,
            "flows": [
                {
                    "flow_number": 1,
                    "flow_type": "unicast",
                    "flow_type_code": 17,
                    "channel_count": 1,
                    "sample_rate": 48000,
                    "encoding": 24,
                    "destination_internet_protocol_version_four_address": "192.168.1.108",
                    "destination_user_datagram_port": 14355,
                    "subscriber_device_name": "lx-dante",
                    "subscriber_flow_name": "1",
                }
            ],
        }
    )
    assert device.tx_flow_count == 1
    assert device.transmitter_flows[0]["destination_internet_protocol_version_four_address"] == "192.168.1.108"
    assert device.transmitter_flows[0]["subscriber_device_name"] == "lx-dante"
    assert DanteDeviceSerializer.to_json(device)["transmitter_flows"][0]["subscriber_device_name"] == "lx-dante"


def test_receiver_channel_status_page_fills_empty_subscription_and_factory_name():
    from netaudio.dante.subscription import DanteSubscription

    device = DanteDevice()
    device.name = "AVIOAES3-53ef37"
    channel = DanteChannel()
    channel.number = 1
    channel.name = "01"
    channel.channel_type = "rx"
    device.rx_channels = {1: channel}
    subscription = DanteSubscription()
    subscription.rx_channel_name = "01"
    subscription.rx_device_name = device.name
    device.subscriptions = [subscription]

    device.apply_receiver_channel_status_page(
        {
            "records": [
                {
                    "channel_number": 1,
                    "friendly_channel_name": "CH1",
                    "source_device_name": "lx-dante",
                    "source_channel_name": "01",
                    "subscription_status_code": 9,
                    "receiver_status_code": 257,
                }
            ]
        }
    )

    assert channel.factory_name == "CH1"
    assert channel.status_code == 9
    assert subscription.tx_device_name == "lx-dante"
    assert subscription.tx_channel_name == "01"
    assert subscription.status_code == 9
    assert subscription.rx_channel_status_code == 257


def test_receiver_channel_status_page_replaces_stale_source_with_authoritative_readback():
    from netaudio.dante.subscription import DanteSubscription

    device = DanteDevice()
    channel = DanteChannel()
    channel.number = 1
    channel.name = "mic-mix-1"
    device.rx_channels = {1: channel}
    subscription = DanteSubscription()
    subscription.rx_channel_name = "mic-mix-1"
    subscription.tx_device_name = "lx-dante"
    subscription.tx_channel_name = "mic-mix:high"
    device.subscriptions = [subscription]

    device.apply_receiver_channel_status_page(
        {
            "records": [
                {
                    "channel_number": 1,
                    "friendly_channel_name": "Left",
                    "source_device_name": "other-device",
                    "source_channel_name": "other-channel",
                    "subscription_status_code": 9,
                }
            ]
        }
    )

    assert channel.factory_name == "Left"
    assert subscription.tx_device_name == "other-device"
    assert subscription.tx_channel_name == "other-channel"
    assert subscription.status_code == 9


def test_partial_receiver_flow_page_preserves_last_complete_inventory_and_survives_reload(tmp_path):
    import json
    from netaudio import core
    from tests.issue_59_fixtures import packet

    device = DanteDevice()
    complete = {
        "page_disposition": "complete",
        "result_code": 1,
        "reported_flow_count": 16,
        "flows": [{"global_flow_id": number, "latency_nanoseconds": 1000000} for number in range(1, 17)],
    }
    device.apply_receiver_flow_status_page(complete)
    partial = core.parse_response("modern_arc_receiver_flow_status_page", packet("receiver_flow_partial.bin"))
    device.apply_receiver_flow_status_page(partial)
    assert device.rx_flow_count == 16
    assert device.receiver_flows == complete["flows"]
    assert device.receiver_flow_latency_nanoseconds == 1000000
    assert device.receiver_flow_completeness == "partial"
    assert device.receiver_flow_status_page == complete
    saved = tmp_path / "device.json"
    saved.write_text(json.dumps(DanteDeviceSerializer.to_json(device)))
    snapshot = json.loads(saved.read_text())
    reloaded = DanteDeviceSerializer.device_from_json(snapshot)
    assert reloaded.receiver_flow_completeness == "complete"
    assert reloaded.receiver_flows == complete["flows"]
    assert reloaded.receiver_flow_status_page == complete
    assert reloaded.receiver_flow_status_page["result_code"] == 1
    assert len(reloaded.receiver_flow_status_page["flows"]) == 16


def test_receiver_flow_state_without_completeness_cannot_clear_inventory():
    device = DanteDevice()
    device.receiver_flows = [{"global_flow_id": 16}]
    device.rx_flow_count = 1
    device.apply_receiver_flow_status_page({"flows": [], "reported_flow_count": 0})
    assert device.receiver_flows == [{"global_flow_id": 16}]
    assert device.rx_flow_count == 1
    assert device.receiver_flow_completeness == "unknown"
