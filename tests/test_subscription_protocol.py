import pytest

from netaudio.dante.packet_store import PacketRecord

FIXTURES_DIR = "subscription"


@pytest.fixture
def load_sub_fixture(load_fixture):
    def _load(name):
        return load_fixture(f"{FIXTURES_DIR}/{name}")

    return _load


class TestSubscriptionCorrelation:
    """Feed real captured packets through PacketStore and verify
    request/response correlation by transaction_id."""

    def test_remove_request_response_correlated(self, load_sub_fixture, tmp_path):
        from netaudio.dante.packet_store import PacketRecord, PacketStore

        store = PacketStore(db_path=str(tmp_path / "test.sqlite"))

        req_data = load_sub_fixture("subscription_remove_request.bin")
        resp_data = load_sub_fixture("subscription_remove_response.bin")

        req_id = store.store_packet(
            PacketRecord(
                payload=req_data,
                source_type="tshark",
                device_ip="192.168.1.94",
                direction="request",
                timestamp_ns=1000,
            )
        )
        resp_id = store.store_packet(
            PacketRecord(
                payload=resp_data,
                source_type="tshark",
                device_ip="192.168.1.94",
                direction="response",
                timestamp_ns=2000,
            )
        )

        req_row = store.get_packet(req_id)
        resp_row = store.get_packet(resp_id)

        assert req_row["correlated_packet_id"] == resp_id
        assert resp_row["correlated_packet_id"] == req_id
        store.close()

    def test_nearby_unmapped_status_is_not_correlated_to_a_request(self, load_sub_fixture, tmp_path):
        from netaudio.dante.packet_store import PacketStore

        store = PacketStore(db_path=str(tmp_path / "test.sqlite"))

        req_data = load_sub_fixture("subscription_remove_request.bin")
        mc_data = load_sub_fixture("multicast_rx_channel_change.bin")

        now = 1_000_000_000_000

        store.store_packet(
            PacketRecord(
                payload=req_data,
                source_type="tshark",
                device_ip="192.168.1.94",
                direction="request",
                timestamp_ns=now,
            )
        )
        mc_id = store.store_packet(
            PacketRecord(
                payload=mc_data,
                source_type="multicast",
                src_ip="192.168.1.94",
                device_ip="192.168.1.94",
                timestamp_ns=now + 80_000_000,  # 80ms later
            )
        )

        mc_row = store.get_packet(mc_id)
        assert mc_row["correlated_packet_id"] is None
        assert store.get_status_request_candidates(mc_id) == []
        store.close()
