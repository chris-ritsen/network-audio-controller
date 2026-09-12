import hashlib
import json
from pathlib import Path

FIXTURES = Path(__file__).parent / "fixtures" / "issue_59"


def packet(name: str) -> bytes:
    metadata = json.loads((FIXTURES / "provenance.json").read_text())["packets"][name]
    data = (FIXTURES / name).read_bytes()
    assert hashlib.sha256(data).hexdigest() == metadata["fixture_sha256"]
    return data


def later_pages() -> list[dict]:
    entries = json.loads(packet("later_pages.json"))
    for entry in entries:
        assert hashlib.sha256(bytes.fromhex(entry["response"])).hexdigest() == entry["fixture_sha256"]
        assert hashlib.sha256(bytes.fromhex(entry["request"])).hexdigest() == entry["request_sha256"]
    return entries
