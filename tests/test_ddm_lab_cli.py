from __future__ import annotations

from argparse import Namespace
from collections import deque
import json
from pathlib import Path

import pytest

from tools.ddm_lab import cli
from tools.ddm_lab.graphql import GraphQLResult, ReadOnlyOperation
from tools.ddm_lab.lifecycle import LabError


MAC_ADDRESS = "02:00:00:00:00:01"
GUEST_IPV4_ADDRESS = "192.0.2.10"


def _result(operation: ReadOnlyOperation, data, *, partial: bool = False) -> GraphQLResult:
    errors = ({"message": "partial"},) if partial else ()
    return GraphQLResult(operation=operation, data=data, errors=errors)


def _inventory(*devices, partial: bool = False) -> GraphQLResult:
    return _result(
        ReadOnlyOperation.INVENTORY,
        {"domains": [], "unenrolledDevices": list(devices)},
        partial=partial,
    )


def _device(state: str, last_changed: str) -> dict:
    return {
        "id": "device-1",
        "name": "A32-001",
        "enrolmentState": "UNENROLLED",
        "interfaces": [{"macAddress": MAC_ADDRESS, "address": GUEST_IPV4_ADDRESS}],
        "connection": {"state": state, "lastChanged": last_changed},
    }


class FakeClient:
    def __init__(self, inventories, events: list[str], *, health=None, schema=None):
        self.inventories = deque(inventories)
        self.events = events
        self.health_result = health or _result(ReadOnlyOperation.HEALTH, {"__typename": "Query"})
        self.schema_result = schema or _result(
            ReadOnlyOperation.SCHEMA,
            {"__schema": {"types": [], "mutationType": None}},
        )

    def health(self):
        self.events.append("health")
        return self.health_result

    def schema(self):
        self.events.append("schema")
        return self.schema_result

    def inventory(self):
        self.events.append("inventory")
        if len(self.inventories) > 1:
            return self.inventories.popleft()
        return self.inventories[0]


class FakeHarness:
    def __init__(
        self,
        tmp_path: Path,
        events: list[str],
        *,
        capture_size: int = 25,
        capture_path: bool = False,
        stop_error: BaseException | None = None,
        start_error: BaseException | None = None,
    ):
        self.state_root = tmp_path / "state"
        self.events = events
        self.capture_size = capture_size
        self.capture_path = capture_path
        self.stop_error = stop_error
        self.start_error = start_error
        self.started = False
        self.stop_calls = 0
        self.capture_stop_calls = 0
        self.prune_calls = 0
        self.unresolved_state = None

    def initialize_state_root(self):
        self.state_root.mkdir(parents=True, exist_ok=True)

    def validate(self, identity):
        self.events.append("validate")
        return {"identity": {"suffix": identity, "mac_address": MAC_ADDRESS}}

    def start(self, identity, **kwargs):
        self.events.append("start")
        self.started = True
        state = {
            "run_id": kwargs["run_id"],
            "purpose": kwargs.get("purpose", "interactive"),
            "identity": {"suffix": identity, "mac_address": MAC_ADDRESS},
            "lan_ipv4_addresses": [GUEST_IPV4_ADDRESS],
            "capture": None,
        }
        if self.start_error is not None:
            self.unresolved_state = {**state, "status": "failed"}
            raise self.start_error
        return state

    def status(self, identity):
        return {
            "identity": {"suffix": identity, "mac_address": MAC_ADDRESS},
            "lan_ipv4_addresses": [GUEST_IPV4_ADDRESS],
        }

    def failed_state_for_run(self, run_id):
        if self.unresolved_state is None or self.unresolved_state["run_id"] != run_id:
            raise LabError("run not found")
        return self.unresolved_state

    def prune_smoke_failures(self, current_run_id, *, apply=False):
        self.prune_calls += 1
        assert apply is True
        assert current_run_id is None or isinstance(current_run_id, str)
        return {"removed": [], "retained_latest_failure": None, "refused": []}

    def capture_stop(self, identity):
        self.events.append("capture_stop")
        self.capture_stop_calls += 1
        record = {"size": self.capture_size, "sha256": "a" * 64}
        if self.capture_path:
            path = self.state_root / "capture.pcap"
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b"x" * self.capture_size)
            record["path"] = str(path)
        return record

    def stop(self, identity):
        self.events.append("stop")
        self.stop_calls += 1
        if self.stop_error is not None:
            raise self.stop_error
        return {"outcome": "completed", "identity": identity}


class FakeClock:
    def __init__(self):
        self.value = 0.0

    def monotonic(self):
        return self.value

    def sleep(self, seconds):
        self.value += seconds


def _arguments(**overrides) -> Namespace:
    values = {
        "identity": "001",
        "wait_seconds": 90.0,
        "ddm_discovery_timeout": 2.0,
        "capture_seconds": 30,
        "capture_mib": 4,
    }
    values.update(overrides)
    return Namespace(**values)


def _run_smoke(monkeypatch, tmp_path, inventories, *, capture_size=25, arguments=None):
    events: list[str] = []
    client = FakeClient(inventories, events)
    harness = FakeHarness(tmp_path, events, capture_size=capture_size)
    clock = FakeClock()
    monkeypatch.setattr(cli, "_graphql_client", lambda unused: client)
    monkeypatch.setattr(cli.time, "monotonic", clock.monotonic)
    monkeypatch.setattr(cli.time, "sleep", clock.sleep)
    monkeypatch.setattr(cli, "utc_now", lambda: "2026-08-26T12:00:00Z")
    result = cli._smoke(arguments or _arguments(), harness)
    return result, harness, events


@pytest.mark.parametrize("value", [True, 0, -1, 601, float("inf"), float("nan"), "30"])
def test_smoke_discovery_timeout_is_finite_and_bounded(tmp_path, value):
    harness = FakeHarness(tmp_path, [])

    with pytest.raises(LabError, match="discovery timeout must be between"):
        cli._smoke(_arguments(ddm_discovery_timeout=value), harness)

    assert harness.started is False
    assert harness.stop_calls == 0


def test_smoke_takes_complete_baseline_before_validating_or_starting(monkeypatch, tmp_path):
    events: list[str] = []
    client = FakeClient([_inventory(partial=True)], events)
    harness = FakeHarness(tmp_path, events)
    monkeypatch.setattr(cli, "_graphql_client", lambda unused: client)

    with pytest.raises(LabError, match="baseline DDM inventory was partial"):
        cli._smoke(_arguments(), harness)

    assert events == ["health", "schema", "inventory"]
    assert harness.started is False
    assert harness.stop_calls == 0


@pytest.mark.parametrize("phase", ["health", "schema"])
def test_smoke_rejects_partial_health_and_schema_before_start(monkeypatch, tmp_path, phase):
    events: list[str] = []
    health = _result(ReadOnlyOperation.HEALTH, {"__typename": "Query"}, partial=phase == "health")
    schema = _result(
        ReadOnlyOperation.SCHEMA,
        {"__schema": {"types": [], "mutationType": None}},
        partial=phase == "schema",
    )
    client = FakeClient([_inventory()], events, health=health, schema=schema)
    harness = FakeHarness(tmp_path, events)
    monkeypatch.setattr(cli, "_graphql_client", lambda unused: client)

    with pytest.raises(LabError, match=f"DDM {phase} result was partial"):
        cli._smoke(_arguments(), harness)

    assert harness.started is False
    assert harness.stop_calls == 0


def test_smoke_waits_for_non_partial_ready_inventory_and_cleans_up(monkeypatch, tmp_path):
    result, harness, events = _run_smoke(
        monkeypatch,
        tmp_path,
        [
            _inventory(),
            _inventory(_device("CONNECTING", "2026-08-26T12:00:00Z")),
            _inventory(_device("READY", "2026-08-26T12:00:01Z")),
        ],
    )

    assert events[:5] == ["health", "schema", "inventory", "validate", "start"]
    assert result["virtual_device"]["connection"]["state"] == "READY"
    assert result["capture"]["size"] == 25
    assert events[-2:] == ["capture_stop", "stop"]
    assert harness.stop_calls == 1


def test_smoke_rejects_partial_post_start_inventory_and_still_stops(monkeypatch, tmp_path):
    events: list[str] = []
    client = FakeClient([_inventory(), _inventory(_device("READY", "2026-08-26T12:00:01Z"), partial=True)], events)
    harness = FakeHarness(tmp_path, events)
    monkeypatch.setattr(cli, "_graphql_client", lambda unused: client)

    with pytest.raises(LabError, match="post-start DDM inventory was partial"):
        cli._smoke(_arguments(), harness)

    assert harness.started is True
    assert harness.capture_stop_calls == 0
    assert harness.stop_calls == 1
    assert events[-1] == "stop"


def test_smoke_does_not_accept_unchanged_preexisting_ready_device(monkeypatch, tmp_path):
    unchanged = _device("READY", "2099-08-26T12:00:00Z")
    events: list[str] = []
    client = FakeClient([_inventory(unchanged), _inventory(unchanged)], events)
    harness = FakeHarness(tmp_path, events)
    clock = FakeClock()
    monkeypatch.setattr(cli, "_graphql_client", lambda unused: client)
    monkeypatch.setattr(cli.time, "monotonic", clock.monotonic)
    monkeypatch.setattr(cli.time, "sleep", clock.sleep)
    monkeypatch.setattr(cli, "utc_now", lambda: "2026-08-26T12:00:00Z")

    with pytest.raises(LabError, match="READY record was not fresh"):
        cli._smoke(_arguments(ddm_discovery_timeout=0.1), harness)

    assert harness.capture_stop_calls == 0
    assert harness.stop_calls == 1


def test_smoke_accepts_ready_device_only_after_last_changed_moves(monkeypatch, tmp_path):
    baseline = _device("READY", "2026-08-26T12:00:00Z")
    changed = _device("READY", "2026-08-26T12:00:01Z")

    result, harness, _ = _run_smoke(
        monkeypatch,
        tmp_path,
        [_inventory(baseline), _inventory(changed)],
    )

    assert (
        result["virtual_device_baseline"]["connection"]["lastChanged"]
        != result["virtual_device"]["connection"]["lastChanged"]
    )
    assert harness.stop_calls == 1


def test_changed_but_old_last_changed_is_not_fresh():
    baseline = _device("READY", "2026-08-26T11:59:58Z")
    observed = _device("READY", "2026-08-26T11:59:59Z")

    assert (
        cli._ready_observation_is_fresh(
            baseline,
            observed,
            not_before="2026-08-26T12:00:00Z",
        )
        is False
    )


def test_absent_baseline_still_requires_a_fresh_last_changed_timestamp():
    observed = _device("READY", "2026-08-26T11:59:59Z")

    assert (
        cli._ready_observation_is_fresh(
            None,
            observed,
            not_before="2026-08-26T12:00:00Z",
        )
        is False
    )


@pytest.mark.parametrize("capture_size", [0, 24])
def test_smoke_rejects_header_only_capture_and_still_stops(monkeypatch, tmp_path, capture_size):
    events: list[str] = []
    client = FakeClient([_inventory(), _inventory(_device("READY", "2026-08-26T12:00:01Z"))], events)
    harness = FakeHarness(tmp_path, events, capture_size=capture_size)
    monkeypatch.setattr(cli, "_graphql_client", lambda unused: client)
    monkeypatch.setattr(cli, "utc_now", lambda: "2026-08-26T12:00:00Z")

    with pytest.raises(LabError, match="capture contained no packets"):
        cli._smoke(_arguments(), harness)

    assert harness.capture_stop_calls == 1
    assert harness.stop_calls == 1
    assert events[-1] == "stop"


def test_failed_compact_result_keeps_last_observation_and_confirms_cleanup(monkeypatch, tmp_path):
    connecting = _device("CONNECTING", "2026-08-26T12:00:01Z")
    events: list[str] = []
    client = FakeClient([_inventory(), _inventory(connecting)], events)
    harness = FakeHarness(tmp_path, events)
    clock = FakeClock()
    monkeypatch.setattr(cli, "_graphql_client", lambda unused: client)
    monkeypatch.setattr(cli.time, "monotonic", clock.monotonic)
    monkeypatch.setattr(cli.time, "sleep", clock.sleep)

    with pytest.raises(LabError, match="as READY"):
        cli._smoke(_arguments(ddm_discovery_timeout=0.1), harness)

    compact = json.loads((harness.state_root / "latest-smoke.json").read_text())
    assert compact["run_id"]
    assert compact["last_observed_virtual_device"]["connection"]["state"] == "CONNECTING"
    assert compact["capture"]["retained"] is False
    assert harness.stop_calls == 1


def test_cleanup_failure_never_claims_capture_was_discarded(monkeypatch, tmp_path):
    events: list[str] = []
    client = FakeClient([_inventory(), _inventory(_device("READY", "2026-08-26T12:00:01Z"))], events)
    harness = FakeHarness(
        tmp_path,
        events,
        capture_path=True,
        stop_error=LabError("stop failed"),
    )
    monkeypatch.setattr(cli, "_graphql_client", lambda unused: client)
    monkeypatch.setattr(cli, "utc_now", lambda: "2026-08-26T12:00:00Z")

    with pytest.raises(LabError, match="stop failed"):
        cli._smoke(_arguments(), harness)

    compact = json.loads((harness.state_root / "latest-smoke.json").read_text())
    assert compact["capture"]["retained"] is True
    assert compact["cleanup"] is None


def test_unresolved_start_records_only_the_exact_attempted_run(monkeypatch, tmp_path):
    events: list[str] = []
    client = FakeClient([_inventory()], events)
    harness = FakeHarness(tmp_path, events, start_error=LabError("start failed after creating state"))
    monkeypatch.setattr(cli, "_graphql_client", lambda unused: client)

    with pytest.raises(LabError, match="start failed"):
        cli._smoke(_arguments(), harness)

    compact = json.loads((harness.state_root / "latest-smoke.json").read_text())
    assert compact["run_id"] == harness.unresolved_state["run_id"]
    assert compact["capture"]["retained"] is None


def test_failed_start_uses_last_capture_history_record(monkeypatch, tmp_path):
    class FailedStartWithCaptureHarness(FakeHarness):
        def start(self, identity, **kwargs):
            try:
                return super().start(identity, **kwargs)
            except LabError:
                capture = self.state_root / "failed-start.pcap"
                capture.parent.mkdir(parents=True, exist_ok=True)
                capture.write_bytes(b"x" * 31)
                self.unresolved_state["capture"] = None
                self.unresolved_state["captures"] = [{"path": str(capture), "size": 31, "sha256": "b" * 64}]
                raise

    events: list[str] = []
    client = FakeClient([_inventory()], events)
    harness = FailedStartWithCaptureHarness(
        tmp_path,
        events,
        start_error=LabError("start failed after capture cleanup"),
    )
    monkeypatch.setattr(cli, "_graphql_client", lambda unused: client)

    with pytest.raises(LabError, match="start failed"):
        cli._smoke(_arguments(), harness)

    compact = json.loads((harness.state_root / "latest-smoke.json").read_text())
    assert compact["capture"] == {
        "retained": True,
        "scope": "guest TAP only",
        "sha256": "b" * 64,
        "size": 31,
    }


def test_failed_start_can_recover_run_id_from_exact_retained_lease(monkeypatch, tmp_path):
    class LeaseOnlyFailureHarness(FakeHarness):
        def _lease_path(self, identity):
            return self.state_root / "leases" / f"identity-{identity}.lease"

        def start(self, identity, **kwargs):
            self.events.append("start")
            self.started = True
            lease = self._lease_path(identity)
            lease.mkdir(parents=True)
            (lease / "owner.json").write_text(
                json.dumps({"identity": identity, "run_id": kwargs["run_id"]}),
                encoding="utf-8",
            )
            raise LabError("start failed with retained lease")

        def failed_state_for_run(self, run_id):
            raise LabError("state was not written")

    events: list[str] = []
    client = FakeClient([_inventory()], events)
    harness = LeaseOnlyFailureHarness(tmp_path, events)
    monkeypatch.setattr(cli, "_graphql_client", lambda unused: client)

    with pytest.raises(LabError, match="retained lease"):
        cli._smoke(_arguments(), harness)

    compact = json.loads((harness.state_root / "latest-smoke.json").read_text())
    owner = json.loads(next((harness.state_root / "leases").glob("*.lease/owner.json")).read_text())
    assert compact["run_id"] == owner["run_id"]
