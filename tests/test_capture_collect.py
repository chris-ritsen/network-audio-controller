import json

from typer.testing import CliRunner

from netaudio.commands.capture import cli as capture_commands
from netaudio.commands.capture import collection
from netaudio.dante.packet_store import PacketStore
from tests.test_capture_workflow import build_arc_packet

runner = CliRunner()


class FakeRedisClient:
    def __init__(self, entries):
        self.entries = entries
        self.published = []
        self.reads = 0

    def xadd(self, stream, event, maxlen=None, approximate=False):
        self.published.append((stream, event))
        return f"{len(self.published)}-0"

    def xread(self, streams, count=None, block=None):
        self.reads += 1
        if self.reads > 1:
            return []
        stream = next(iter(streams))
        return [(stream, self.entries)]


def _packet_fields(payload: bytes, source_host: str, timestamp_ns: int, **overrides) -> dict:
    fields = {
        "direction": "request",
        "dst_ip": "192.168.1.20",
        "dst_port": "4440",
        "event": "packet",
        "payload_hex": payload.hex(),
        "source_host": source_host,
        "src_ip": "192.168.1.10",
        "src_port": "40000",
        "timestamp_ns": str(timestamp_ns),
    }
    fields.update(overrides)
    return fields


def _install_client(monkeypatch, client):
    monkeypatch.setattr(collection, "_load_capture_profile", lambda _config, _profile: ({}, None))
    monkeypatch.setattr(collection, "_resolve_redis_for_capture", lambda **_kwargs: client)


def test_collect_imports_dedupes_and_publishes(tmp_path, monkeypatch):
    database_path = tmp_path / "capture.sqlite"
    packet_store = PacketStore(db_path=str(database_path))
    session_id = packet_store.start_session(name="active")
    packet_store.close()

    payload = build_arc_packet(0x1002)
    other_payload = build_arc_packet(0x1003)
    client = FakeRedisClient(
        [
            ("1-0", _packet_fields(payload, "host-a", 1_000_000_000)),
            ("2-0", _packet_fields(payload, "host-b", 1_000_000_000 + 5_000_000)),
            ("3-0", _packet_fields(other_payload, "host-a", 1_100_000_000)),
            (
                "4-0",
                {
                    "data_json": json.dumps({"fact": "arc_opcode:0x1002"}),
                    "event": "marker",
                    "label": "Observed Reply",
                    "marker_type": "observation",
                    "note": "seen",
                    "source_host": "host-a",
                    "timestamp_ns": str(1_200_000_000),
                },
            ),
        ]
    )
    _install_client(monkeypatch, client)

    result = runner.invoke(
        capture_commands.app,
        [
            "collect",
            "--stream",
            "netaudio:ingress",
            "--publish-stream",
            "netaudio:unified",
            "--db",
            str(database_path),
            "--once",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "Capture: Session routing mode active" in result.output
    assert "Capture: Imported 2 packet(s), 1 marker(s), deduped=1" in result.output
    assert "Capture: Published unified 2 packet(s), 1 marker(s) to netaudio:unified" in result.output

    packet_store = PacketStore(db_path=str(database_path))
    try:
        assert packet_store.get_session_packet_count(session_id) == 2
        markers = packet_store.get_markers(session_id, marker_types=["observation"])
    finally:
        packet_store.close()
    assert [marker["label"] for marker in markers] == ["observed_reply"]
    assert markers[0]["data"] == {"fact": "arc_opcode:0x1002"}

    published_events = [event["event"] for _, event in client.published]
    assert published_events == ["packet", "packet", "marker"]
    assert all(stream == "netaudio:unified" for stream, _ in client.published)
    assert client.published[0][1]["session_id"] == str(session_id)


def test_collect_without_dedupe_keeps_cross_host_duplicates(tmp_path, monkeypatch):
    database_path = tmp_path / "capture.sqlite"
    payload = build_arc_packet(0x1002)
    client = FakeRedisClient(
        [
            ("1-0", _packet_fields(payload, "host-a", 1_000_000_000)),
            ("2-0", _packet_fields(payload, "host-b", 1_000_000_000 + 5_000_000)),
        ]
    )
    _install_client(monkeypatch, client)

    result = runner.invoke(
        capture_commands.app,
        [
            "collect",
            "--stream",
            "netaudio:ingress",
            "--db",
            str(database_path),
            "--once",
            "--no-dedupe",
            "--session",
            "none",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "Capture: Session routing mode none" in result.output
    assert "Capture: Imported 2 packet(s), 0 marker(s), deduped=0" in result.output
    assert client.published == []


def test_collect_rejects_conflicting_session_options(tmp_path, monkeypatch):
    _install_client(monkeypatch, FakeRedisClient([]))

    result = runner.invoke(
        capture_commands.app,
        [
            "collect",
            "--stream",
            "netaudio:ingress",
            "--db",
            str(tmp_path / "capture.sqlite"),
            "--session-id",
            "1",
            "--session",
            "latest",
        ],
    )

    assert result.exit_code != 0
    assert "Use either --session-id or --session, not both." in result.output
