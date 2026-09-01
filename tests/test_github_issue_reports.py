import hashlib
import json
from pathlib import Path


FIXTURE_PATH = Path(__file__).parent / "fixtures" / "github_issue_reports.json"


def _fixture() -> dict:
    return json.loads(FIXTURE_PATH.read_text())


def test_promoted_issue_payloads_match_their_recorded_derived_digests():
    fixture = _fixture()
    records = [*fixture["issue_52"].values(), *fixture["issue_53"].values()]

    assert len(records) == 4
    for record in records:
        payload = bytes.fromhex(record["payload"])
        assert hashlib.sha256(payload).hexdigest() == record["derived_sha256"]


def test_issue_fixture_records_public_source_archives_and_packet_hashes():
    fixture = _fixture()
    provenance = fixture["_provenance"]

    assert provenance["issues"]["52"]["archive_sha256"] == (
        "3acaf3cb08052aa62830c08fdad16a852e04d6edb0c450c4e6e05299b67813db"
    )
    assert provenance["issues"]["53"]["archive_sha256"] == (
        "126bc54a950b4d1954889ef7d0b2779b3d93eff692d0dad2533fd6913eb115f9"
    )
    assert fixture["issue_52"]["tesira_receiver_channel_partial_page"]["source_sha256"] == (
        "c94e51fa44d0e2049ec3a33f6414a049dbb3849fce2b20b417f4259849e220ef"
    )
    assert fixture["issue_53"]["tesira_transmitter_flow_status"]["source_sha256"] == (
        "098196b45bc673ca7fd141490cbbad81f1a8568b265e403231f64867ae0de056"
    )
