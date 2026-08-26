"""Command-line interface for the bounded DDM clean-room lab harness."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .graphql import DDMGraphQLClient, DDMGraphQLError, GraphQLResult
from .inventory import matching_inventory_device as _matching_inventory_device
from .lifecycle import (
    DEFAULT_BRIDGE,
    DEFAULT_CAPTURE_MIB,
    DEFAULT_CAPTURE_SECONDS,
    DEFAULT_IDENTITIES,
    DEFAULT_IMAGE,
    DEFAULT_IMAGE_MANIFEST_SHA256,
    DEFAULT_MAX_ACTIVE_GUESTS,
    DEFAULT_PROMOTION_ROOT,
    DEFAULT_QEMU,
    DEFAULT_QEMU_SHA256,
    DEFAULT_STATE_ROOT,
    LabConfiguration,
    LabError,
    LabHarness,
    atomic_write_json,
    sha256_file,
    utc_now,
)
from .topology import select_identities, start_many, status_all, stop_all


DEFAULT_DDM_URL = "http://192.168.1.217/graphql"
DEFAULT_DDM_KEY_FILE = Path("/mnt/optane/vms/ddm/MANAGED_API_KEY.txt")
MAX_DDM_DISCOVERY_SECONDS = 600.0


def _write_json(value: Any) -> None:
    print(json.dumps(value, indent=2, sort_keys=True))


def _result_mapping(result: GraphQLResult) -> dict[str, Any]:
    return {"data": result.data, "errors": result.errors, "partial": result.partial}


def _schema_summary(result: GraphQLResult) -> dict[str, Any]:
    data = result.data
    schema = data.get("__schema", {}) if isinstance(data, dict) else {}
    types = schema.get("types", []) if isinstance(schema, dict) else []
    mutation_type = schema.get("mutationType") if isinstance(schema, dict) else None
    mutation_name = mutation_type.get("name") if isinstance(mutation_type, dict) else None
    mutation_fields: list[str] = []
    if mutation_name:
        for record in types if isinstance(types, list) else []:
            if isinstance(record, dict) and record.get("name") == mutation_name:
                mutation_fields = sorted(
                    field["name"]
                    for field in record.get("fields", [])
                    if isinstance(field, dict) and isinstance(field.get("name"), str)
                )
                break
    encoded = json.dumps(data, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return {
        "schema_sha256": hashlib.sha256(encoded).hexdigest(),
        "type_count": len(types) if isinstance(types, list) else 0,
        "mutation_type": mutation_name,
        "enrollment_mutations_present": {
            "DevicesEnroll": "DevicesEnroll" in mutation_fields,
            "DevicesUnenroll": "DevicesUnenroll" in mutation_fields,
        },
        "graphql_errors": result.errors,
        "partial": result.partial,
    }


def _validate_discovery_timeout(value: Any) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or not 0 < value <= MAX_DDM_DISCOVERY_SECONDS
    ):
        raise LabError(f"ddm discovery timeout must be between 0 and {MAX_DDM_DISCOVERY_SECONDS:g} seconds")
    return float(value)


def _complete_inventory(result: GraphQLResult, phase: str) -> dict[str, Any]:
    _require_non_partial(result, f"{phase} DDM inventory")
    data = result.data
    if not isinstance(data, dict):
        raise LabError(f"{phase} DDM inventory data was not an object")
    for root in ("domains", "unenrolledDevices"):
        if not isinstance(data.get(root), list):
            raise LabError(f"{phase} DDM inventory omitted the complete {root} list")
    return data


def _require_non_partial(result: GraphQLResult, phase: str) -> GraphQLResult:
    if result.partial:
        raise LabError(f"{phase} was partial")
    return result


def _connection(value: dict[str, Any] | None) -> dict[str, Any]:
    connection = value.get("connection") if isinstance(value, dict) else None
    return connection if isinstance(connection, dict) else {}


def _parse_graphql_timestamp(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    encoded = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(encoded)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(timezone.utc)


def _ready_observation_is_fresh(
    baseline: dict[str, Any] | None,
    observed: dict[str, Any],
    *,
    not_before: str,
) -> bool:
    observed_connection = _connection(observed)
    if observed_connection.get("state") != "READY":
        return False
    observed_changed = observed_connection.get("lastChanged")
    if not isinstance(observed_changed, str) or not observed_changed:
        return False
    observed_timestamp = _parse_graphql_timestamp(observed_changed)
    boundary_timestamp = _parse_graphql_timestamp(not_before)
    if observed_timestamp is None or boundary_timestamp is None or observed_timestamp < boundary_timestamp:
        return False
    if baseline is None:
        return True
    return observed_changed != _connection(baseline).get("lastChanged")


def _require_packet_bearing_capture(record: Any) -> dict[str, Any]:
    if not isinstance(record, dict):
        raise LabError("smoke capture did not stop with an artifact record")
    size = record.get("size")
    if isinstance(size, bool) or not isinstance(size, int) or size <= 24:
        raise LabError("smoke capture contained no packets")
    return record


def _new_smoke_run_id(identity: str) -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"smoke-{timestamp}-i{identity}-{uuid.uuid4().hex[:6]}"


def _known_capture_retention(record: dict[str, Any] | None, *, cleanup_confirmed: bool) -> bool | None:
    if cleanup_confirmed:
        return False
    path = record.get("path") if isinstance(record, dict) else None
    if isinstance(path, str):
        try:
            if Path(path).is_file():
                return True
        except OSError:
            pass
    return None


def _capture_from_state(state: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(state, dict):
        return None
    current = state.get("capture")
    if isinstance(current, dict):
        return current
    captures = state.get("captures")
    if isinstance(captures, list):
        for record in reversed(captures):
            if isinstance(record, dict):
                return record
    return None


def _retained_attempted_lease_run_id(
    harness: LabHarness,
    identity: str,
    attempted_run_id: str | None,
) -> str | None:
    if attempted_run_id is None:
        return None
    lease_resolver = getattr(harness, "_lease_path", None)
    if not callable(lease_resolver):
        return None
    try:
        owner_path = Path(lease_resolver(identity)) / "owner.json"
        owner = json.loads(owner_path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        return None
    if isinstance(owner, dict) and owner.get("identity") == identity and owner.get("run_id") == attempted_run_id:
        return attempted_run_id
    return None


def _configuration(arguments: argparse.Namespace) -> LabConfiguration:
    return LabConfiguration(
        state_root=arguments.state_root,
        promotion_root=arguments.promotion_root,
        qemu=arguments.qemu,
        qemu_sha256=arguments.qemu_sha256,
        image_directory=arguments.image_directory,
        image_manifest_sha256=arguments.image_manifest_sha256,
        identities_directory=arguments.identities_directory,
        bridge=arguments.bridge,
        max_active_guests=arguments.max_active_guests,
    )


def _graphql_client(arguments: argparse.Namespace) -> DDMGraphQLClient:
    return DDMGraphQLClient(
        arguments.ddm_url,
        arguments.ddm_key_file,
        timeout=arguments.ddm_timeout,
    )


def _state_summary(state: dict[str, Any]) -> dict[str, Any]:
    return {
        "run_id": state.get("run_id"),
        "status": state.get("status"),
        "identity": state.get("identity"),
        "tap": state.get("tap"),
        "lan_ready_at": state.get("lan_ready_at"),
        "process_running": state.get("process_running"),
        "tap_present": state.get("tap_present"),
        "lan_mac_seen": state.get("lan_mac_seen"),
        "lan_ipv4_addresses": state.get("lan_ipv4_addresses"),
        "qmp_status": state.get("qmp_status"),
        "capture": state.get("capture"),
        "capture_running": state.get("capture_running"),
    }


def _selected_domain(inventory: dict[str, Any], selector: str) -> dict[str, Any]:
    matches = [
        domain
        for domain in inventory.get("domains", [])
        if isinstance(domain, dict) and (domain.get("id") == selector or domain.get("name") == selector)
    ]
    if len(matches) != 1:
        raise LabError(f"DDM domain selector {selector!r} matched {len(matches)} domains")
    return matches[0]


def _accepted_mutation(result: GraphQLResult, field: str) -> dict[str, Any]:
    _require_non_partial(result, f"DDM {field} mutation")
    payload = result.data.get(field) if isinstance(result.data, dict) else None
    if not isinstance(payload, dict) or payload.get("ok") is not True:
        raise LabError(f"DDM did not accept {field}")
    return payload


def _managed_enrollment(arguments: argparse.Namespace, harness: LabHarness, *, enroll: bool) -> dict[str, Any]:
    timeout = _validate_discovery_timeout(arguments.readback_timeout)
    state = harness.status(arguments.identity)
    if state.get("process_running") is not True:
        raise LabError("the disposable virtual device is not running")
    if state.get("capture_running") is not True:
        raise LabError("start a bounded guest-TAP capture before changing enrollment")
    mac_address = state["identity"]["mac_address"]
    ipv4_addresses = set(state.get("lan_ipv4_addresses", []))
    if not ipv4_addresses:
        raise LabError("the running virtual identity has no verified IPv4 neighbor binding")

    client = _graphql_client(arguments)
    baseline_deadline = time.monotonic() + timeout
    baseline_inventory: dict[str, Any] | None = None
    baseline: dict[str, Any] | None = None
    while time.monotonic() < baseline_deadline:
        baseline_inventory = _complete_inventory(client.inventory(), "pre-mutation")
        candidate = _matching_inventory_device(baseline_inventory, mac_address, ipv4_addresses)
        if candidate is not None and _connection(candidate).get("state") == "READY":
            baseline = candidate
            break
        remaining = baseline_deadline - time.monotonic()
        if remaining > 0:
            time.sleep(min(1.0, remaining))
    if baseline is None or baseline_inventory is None:
        raise LabError("DDM did not report the running virtual identity as READY before the mutation deadline")
    device_id = baseline.get("id")
    if not isinstance(device_id, str) or not device_id:
        raise LabError("DDM did not report an ID for the running virtual identity")

    domain: dict[str, Any] | None = None
    expected_state = "ENROLLED" if enroll else "UNENROLLED"
    requested_at = utc_now()
    if enroll:
        domain = _selected_domain(baseline_inventory, arguments.domain)
        domain_id = domain.get("id")
        if not isinstance(domain_id, str) or not domain_id:
            raise LabError("selected DDM domain has no ID")
        mutation = client.enroll_devices(domain_id, [device_id], clear_config=False)
        mutation_field = "DevicesEnroll"
    else:
        domain_id = None
        mutation = client.unenroll_devices([device_id], clear_config=False)
        mutation_field = "DevicesUnenroll"
    _accepted_mutation(mutation, mutation_field)
    accepted_at = utc_now()

    deadline = time.monotonic() + timeout
    observed: dict[str, Any] | None = None
    while time.monotonic() < deadline:
        inventory = _complete_inventory(client.inventory(), "post-mutation")
        candidate = _matching_inventory_device(
            inventory,
            mac_address,
            ipv4_addresses,
            expected_device_id=device_id,
        )
        if candidate is not None and candidate.get("enrolmentState") == expected_state:
            if not enroll or candidate.get("domainId") == domain_id:
                observed = candidate
                break
        remaining = deadline - time.monotonic()
        if remaining > 0:
            time.sleep(min(1.0, remaining))
    if observed is None:
        raise LabError(f"DDM accepted {mutation_field}, but fresh readback did not reach {expected_state}")

    result = {
        "boundary": "public Managed API action on the active leased synthetic virtual identity",
        "clearConfig": False,
        "requested_at": requested_at,
        "accepted_at": accepted_at,
        "observed_at": utc_now(),
        "action": "enroll" if enroll else "unenroll",
        "domain": {"id": domain.get("id"), "name": domain.get("name")} if domain else None,
        "baseline": baseline,
        "mutation": _result_mapping(mutation),
        "observed": observed,
    }
    session = harness.state_root / "sessions" / state["run_id"]
    record_path = session / f"managed-api-{result['action']}-{uuid.uuid4().hex[:8]}.json"
    atomic_write_json(record_path, result)
    result["record"] = {
        "path": str(record_path),
        "size": record_path.stat().st_size,
        "sha256": sha256_file(record_path),
    }
    return result


def _managed_enrollment_many(
    arguments: argparse.Namespace,
    harness: LabHarness,
    *,
    enroll: bool,
) -> dict[str, Any]:
    identities = arguments.identities or harness.active_identities()
    if not identities:
        raise LabError("no active virtual identities")
    identities = select_identities(identities, count=None, first=1)
    inventory = _complete_inventory(_graphql_client(arguments).inventory(), "batch preflight")
    device_ids: dict[str, str] = {}
    for identity in identities:
        state = harness.status(identity)
        if state.get("process_running") is not True or state.get("capture_running") is not True:
            raise LabError(f"virtual identity {identity} is not running under an active capture")
        addresses = set(state.get("lan_ipv4_addresses", []))
        candidate = _matching_inventory_device(
            inventory,
            state["identity"]["mac_address"],
            addresses,
        )
        device_id = candidate.get("id") if isinstance(candidate, dict) else None
        if (
            not addresses
            or not isinstance(device_id, str)
            or not device_id
            or _connection(candidate).get("state") != "READY"
        ):
            raise LabError(f"DDM did not uniquely report virtual identity {identity} as READY")
        device_ids[identity] = device_id
    if len(set(device_ids.values())) != len(device_ids):
        raise LabError("multiple virtual identities resolved to the same DDM device ID")
    results: dict[str, Any] = {}
    for identity in identities:
        per_device = argparse.Namespace(**vars(arguments))
        per_device.identity = identity
        try:
            result = _managed_enrollment(per_device, harness, enroll=enroll)
            if result["observed"].get("id") != device_ids[identity]:
                raise LabError("DDM device ID changed after batch preflight")
            results[identity] = result
        except BaseException as error:
            raise LabError(f"managed enrollment failed for virtual identity {identity}: {error}") from error
    return {
        "action": "enroll" if enroll else "unenroll",
        "identities": identities,
        "results": results,
    }


def _smoke(arguments: argparse.Namespace, harness: LabHarness) -> dict[str, Any]:
    discovery_timeout = _validate_discovery_timeout(arguments.ddm_discovery_timeout)
    client = _graphql_client(arguments)
    started_at = utc_now()
    health: GraphQLResult | None = None
    schema_summary: dict[str, Any] | None = None
    baseline_device: dict[str, Any] | None = None
    state: dict[str, Any] | None = None
    unresolved_state: dict[str, Any] | None = None
    attempted_run_id: str | None = None
    capture_record: dict[str, Any] | None = None
    device: dict[str, Any] | None = None
    last_observed_device: dict[str, Any] | None = None
    stale_ready_observed = False
    error: BaseException | None = None
    try:
        health = client.health()
        _require_non_partial(health, "DDM health result")
        schema = client.schema()
        schema_summary = _schema_summary(schema)
        _require_non_partial(schema, "DDM schema result")
        baseline_inventory = _complete_inventory(client.inventory(), "baseline")
        validated = harness.validate(arguments.identity)
        mac_address = validated["identity"]["mac_address"]
        baseline_device = _matching_inventory_device(baseline_inventory, mac_address)
        guest_start_requested_at = utc_now()
        attempted_run_id = _new_smoke_run_id(arguments.identity)
        state = harness.start(
            arguments.identity,
            run_id=attempted_run_id,
            purpose="smoke",
            wait_seconds=arguments.wait_seconds,
            capture=True,
            capture_seconds=arguments.capture_seconds,
            capture_mib=arguments.capture_mib,
        )
        started_mac_address = state["identity"]["mac_address"]
        if str(started_mac_address).lower() != str(mac_address).lower():
            raise LabError("validated and started virtual identities did not match")
        deadline = time.monotonic() + discovery_timeout
        guest_ipv4_addresses = set(state.get("lan_ipv4_addresses", []))
        while time.monotonic() < deadline:
            if not guest_ipv4_addresses:
                live_state = harness.status(arguments.identity)
                guest_ipv4_addresses.update(live_state.get("lan_ipv4_addresses", []))
            inventory = _complete_inventory(client.inventory(), "post-start")
            observed = _matching_inventory_device(
                inventory,
                started_mac_address,
                guest_ipv4_addresses,
            )
            if observed is not None:
                last_observed_device = observed
                if _connection(observed).get("state") == "READY":
                    if _ready_observation_is_fresh(
                        baseline_device,
                        observed,
                        not_before=guest_start_requested_at,
                    ):
                        device = observed
                        break
                    stale_ready_observed = True
            remaining = deadline - time.monotonic()
            if remaining > 0:
                time.sleep(min(1.0, remaining))
        if device is None:
            if stale_ready_observed:
                raise LabError("DDM READY record was not fresh relative to the pre-start baseline")
            if last_observed_device is not None:
                raise LabError("DDM did not report the virtual identity as READY before the discovery deadline")
            raise LabError("DDM did not report the virtual identity before the discovery deadline")
        capture_record = _require_packet_bearing_capture(harness.capture_stop(arguments.identity))
    except BaseException as caught:
        error = caught
        if state is None and attempted_run_id is not None:
            try:
                candidate = harness.failed_state_for_run(attempted_run_id)
            except (LabError, OSError, ValueError):
                pass
            else:
                if isinstance(candidate, dict) and candidate.get("run_id") == attempted_run_id:
                    unresolved_state = candidate
    finally:
        cleanup: dict[str, Any] | None = None
        cleanup_confirmed = False
        try:
            if state is not None:
                cleanup = harness.stop(arguments.identity)
                cleanup_confirmed = True
        except BaseException as cleanup_error:
            if error is None:
                error = cleanup_error
            else:
                error = LabError(f"{error}; cleanup also failed: {cleanup_error}")
        retained_capture = capture_record
        if retained_capture is None:
            for candidate_state in (state, unresolved_state):
                candidate_capture = _capture_from_state(candidate_state)
                if candidate_capture is not None:
                    retained_capture = candidate_capture
                    break
        run_id = None
        for candidate_state in (state, unresolved_state):
            candidate_run_id = candidate_state.get("run_id") if isinstance(candidate_state, dict) else None
            if isinstance(candidate_run_id, str):
                run_id = candidate_run_id
                break
        if run_id is None:
            run_id = _retained_attempted_lease_run_id(
                harness,
                arguments.identity,
                attempted_run_id,
            )
        result = {
            "schema_version": 1,
            "run_id": run_id,
            "started_at": started_at,
            "finished_at": utc_now(),
            "outcome": "passed" if error is None else "failed",
            "boundary": "harness validation only; no protocol semantics or device mutation",
            "ddm_health": _result_mapping(health) if health is not None else None,
            "schema": schema_summary,
            "virtual_device_baseline": baseline_device,
            "virtual_device": device,
            "last_observed_virtual_device": last_observed_device,
            "capture": {
                "size": retained_capture.get("size") if retained_capture else None,
                "sha256": retained_capture.get("sha256") if retained_capture else None,
                "retained": _known_capture_retention(
                    retained_capture,
                    cleanup_confirmed=cleanup_confirmed,
                ),
                "scope": "guest TAP only",
            },
            "cleanup": cleanup,
            "error": str(error) if error is not None else None,
        }
        harness.initialize_state_root()
        try:
            retention_prune = harness.prune_smoke_failures(run_id, apply=True)
        except (LabError, OSError, ValueError) as prune_error:
            result["retention_prune"] = {"ok": False, "error": str(prune_error)}
            if error is None:
                error = LabError(f"smoke retention pruning failed: {prune_error}")
                result["outcome"] = "failed"
                result["error"] = str(error)
        else:
            result["retention_prune"] = {
                "ok": True,
                "removed": len(retention_prune.get("removed", [])),
                "retained_latest_failure": retention_prune.get("retained_latest_failure"),
                "refused": retention_prune.get("refused", []),
            }
        atomic_write_json(harness.state_root / "latest-smoke.json", result)
    if error is not None:
        raise error
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m tools.ddm_lab",
        description="Bounded clean-room DDM/A32 lab lifecycle (no protocol semantics)",
    )
    parser.add_argument("--state-root", type=Path, default=DEFAULT_STATE_ROOT)
    parser.add_argument("--promotion-root", type=Path, default=DEFAULT_PROMOTION_ROOT)
    parser.add_argument("--qemu", type=Path, default=DEFAULT_QEMU)
    parser.add_argument("--qemu-sha256", default=DEFAULT_QEMU_SHA256)
    parser.add_argument("--image-directory", type=Path, default=DEFAULT_IMAGE)
    parser.add_argument("--image-manifest-sha256", default=DEFAULT_IMAGE_MANIFEST_SHA256)
    parser.add_argument("--identities-directory", type=Path, default=DEFAULT_IDENTITIES)
    parser.add_argument("--bridge", default=DEFAULT_BRIDGE)
    parser.add_argument("--max-active-guests", type=int, default=DEFAULT_MAX_ACTIVE_GUESTS)
    parser.add_argument("--ddm-url", default=DEFAULT_DDM_URL)
    parser.add_argument("--ddm-key-file", type=Path, default=DEFAULT_DDM_KEY_FILE)
    parser.add_argument("--ddm-timeout", type=float, default=5.0)
    subparsers = parser.add_subparsers(dest="command", required=True)

    validate = subparsers.add_parser("validate", help="validate hashes and host prerequisites")
    validate.add_argument("--identity", default="001")

    graphql = subparsers.add_parser("graphql", help="run a predefined read-only public API query")
    graphql_subparsers = graphql.add_subparsers(dest="graphql_command", required=True)
    graphql_subparsers.add_parser("health")
    schema = graphql_subparsers.add_parser("schema")
    schema.add_argument("--output", type=Path)
    inventory = graphql_subparsers.add_parser("inventory")
    inventory.add_argument("--output", type=Path)

    start = subparsers.add_parser("start", help="start one leased virtual A32 on br0")
    start.add_argument("--identity", default="001")
    start.add_argument("--run-id")
    start.add_argument("--wait-seconds", type=float, default=90.0)
    start.add_argument("--capture", action="store_true")
    start.add_argument("--capture-seconds", type=int, default=DEFAULT_CAPTURE_SECONDS)
    start.add_argument("--capture-mib", type=int, default=DEFAULT_CAPTURE_MIB)

    start_many_parser = subparsers.add_parser(
        "start-many",
        help="start a resource-bounded set of independently leased virtual A32s",
    )
    identity_selection = start_many_parser.add_mutually_exclusive_group()
    identity_selection.add_argument("--identity", dest="identities", action="append")
    identity_selection.add_argument("--count", type=int)
    start_many_parser.add_argument("--name")
    start_many_parser.add_argument("--first", type=int, default=1)
    start_many_parser.add_argument("--wait-seconds", type=float, default=90.0)
    start_many_parser.add_argument("--capture", action="store_true")
    start_many_parser.add_argument("--capture-seconds", type=int, default=DEFAULT_CAPTURE_SECONDS)
    start_many_parser.add_argument("--capture-mib", type=int, default=16)

    status_all_parser = subparsers.add_parser(
        "status-all", help="show active virtual identities, optionally for one persistent topology"
    )
    status_all_parser.add_argument("--topology")

    stop_all_parser = subparsers.add_parser("stop-all", help="stop every virtual identity leased by this harness")
    stop_all_parser.add_argument("--retain-session", action="store_true")
    stop_all_parser.add_argument("--topology")

    for name in ("status", "pause", "resume", "capture-stop", "capture-discard"):
        command = subparsers.add_parser(name)
        command.add_argument("--identity", default="001")

    snapshot = subparsers.add_parser("snapshot")
    snapshot.add_argument("--identity", default="001")
    snapshot.add_argument("--name", default="snapshot")

    capture_start = subparsers.add_parser("capture-start")
    capture_start.add_argument("--identity", default="001")
    capture_start.add_argument("--seconds", type=int, default=DEFAULT_CAPTURE_SECONDS)
    capture_start.add_argument("--maximum-mib", type=int, default=DEFAULT_CAPTURE_MIB)

    promote = subparsers.add_parser("promote")
    promote.add_argument("--identity", default="001")
    promote.add_argument("--claim", required=True)
    promote.add_argument(
        "--evidence-class",
        required=True,
        choices=("documented", "observed", "causal", "inferred", "unknown"),
    )
    promote.add_argument("--artifact", action="append", required=True)

    stop = subparsers.add_parser("stop", help="cleanly stop and discard raw session by default")
    stop.add_argument("--identity", default="001")
    stop.add_argument("--retain-session", action="store_true")

    prune = subparsers.add_parser("prune", help="prune only harness-owned inactive sessions")
    prune.add_argument("--apply", action="store_true")

    smoke = subparsers.add_parser("smoke", help="boot, observe in DDM, and cleanly discard")
    smoke.add_argument("--identity", default="001")
    smoke.add_argument("--wait-seconds", type=float, default=90.0)
    smoke.add_argument("--ddm-discovery-timeout", type=float, default=120.0)
    smoke.add_argument("--capture-seconds", type=int, default=DEFAULT_CAPTURE_SECONDS)
    smoke.add_argument("--capture-mib", type=int, default=DEFAULT_CAPTURE_MIB)

    enroll_virtual = subparsers.add_parser(
        "enroll-virtual",
        help="enroll the active captured leased virtual identity through DDM",
    )
    enroll_virtual.add_argument("--identity", default="001")
    enroll_virtual.add_argument("--domain", default="test")
    enroll_virtual.add_argument("--readback-timeout", type=float, default=120.0)

    unenroll_virtual = subparsers.add_parser(
        "unenroll-virtual",
        help="unenroll the active captured leased virtual identity through DDM",
    )
    unenroll_virtual.add_argument("--identity", default="001")
    unenroll_virtual.add_argument("--readback-timeout", type=float, default=120.0)

    enroll_all = subparsers.add_parser(
        "enroll-all-virtual",
        help="enroll all active captured virtual identities through DDM",
    )
    enroll_all.add_argument("--identity", dest="identities", action="append")
    enroll_all.add_argument("--domain", default="test")
    enroll_all.add_argument("--readback-timeout", type=float, default=120.0)

    unenroll_all = subparsers.add_parser(
        "unenroll-all-virtual",
        help="unenroll all active captured virtual identities through DDM",
    )
    unenroll_all.add_argument("--identity", dest="identities", action="append")
    unenroll_all.add_argument("--readback-timeout", type=float, default=120.0)
    return parser


def run(arguments: argparse.Namespace) -> Any:
    harness = LabHarness(_configuration(arguments))
    if arguments.command == "validate":
        return harness.validate(arguments.identity)
    if arguments.command == "graphql":
        client = _graphql_client(arguments)
        if arguments.graphql_command == "health":
            return _result_mapping(client.health())
        if arguments.graphql_command == "schema":
            result = client.schema()
            if arguments.output:
                atomic_write_json(arguments.output, _result_mapping(result))
            return _schema_summary(result)
        result = client.inventory()
        mapping = _result_mapping(result)
        if arguments.output:
            atomic_write_json(arguments.output, mapping)
        return mapping
    if arguments.command == "start":
        return _state_summary(
            harness.start(
                arguments.identity,
                run_id=arguments.run_id,
                wait_seconds=arguments.wait_seconds,
                capture=arguments.capture,
                capture_seconds=arguments.capture_seconds,
                capture_mib=arguments.capture_mib,
            )
        )
    if arguments.command == "start-many":
        identities = select_identities(arguments.identities, arguments.count, arguments.first)
        result = start_many(
            harness,
            identities,
            wait_seconds=arguments.wait_seconds,
            capture=arguments.capture,
            capture_seconds=arguments.capture_seconds,
            capture_mib=arguments.capture_mib,
            name=arguments.name,
        )
        result["guests"] = {identity: _state_summary(state) for identity, state in result["guests"].items()}
        return result
    if arguments.command == "status-all":
        result = status_all(harness, topology_id=arguments.topology)
        result["guests"] = {identity: _state_summary(state) for identity, state in result["guests"].items()}
        return result
    if arguments.command == "status":
        return _state_summary(harness.status(arguments.identity))
    if arguments.command == "pause":
        return _state_summary(harness.pause(arguments.identity))
    if arguments.command == "resume":
        return _state_summary(harness.resume(arguments.identity))
    if arguments.command == "snapshot":
        return harness.snapshot(arguments.identity, arguments.name)
    if arguments.command == "capture-start":
        return harness.capture_start(arguments.identity, seconds=arguments.seconds, maximum_mib=arguments.maximum_mib)
    if arguments.command == "capture-stop":
        return harness.capture_stop(arguments.identity)
    if arguments.command == "capture-discard":
        return harness.capture_discard(arguments.identity)
    if arguments.command == "promote":
        return harness.promote(
            arguments.identity,
            claim=arguments.claim,
            evidence_class=arguments.evidence_class,
            artifacts=arguments.artifact,
        )
    if arguments.command == "stop":
        return harness.stop(arguments.identity, retain_session=arguments.retain_session)
    if arguments.command == "stop-all":
        return stop_all(
            harness,
            retain_session=arguments.retain_session,
            topology_id=arguments.topology,
        )
    if arguments.command == "prune":
        return harness.prune(apply=arguments.apply)
    if arguments.command == "smoke":
        return _smoke(arguments, harness)
    if arguments.command == "enroll-virtual":
        with harness.managed_api_action():
            return _managed_enrollment(arguments, harness, enroll=True)
    if arguments.command == "unenroll-virtual":
        with harness.managed_api_action():
            return _managed_enrollment(arguments, harness, enroll=False)
    if arguments.command == "enroll-all-virtual":
        with harness.managed_api_action():
            return _managed_enrollment_many(arguments, harness, enroll=True)
    if arguments.command == "unenroll-all-virtual":
        with harness.managed_api_action():
            return _managed_enrollment_many(arguments, harness, enroll=False)
    raise LabError(f"unsupported command: {arguments.command}")


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    arguments = parser.parse_args(argv)
    try:
        result = run(arguments)
    except (DDMGraphQLError, LabError, OSError, ValueError) as error:
        _write_json({"ok": False, "error": str(error), "type": type(error).__name__})
        return 1
    _write_json({"ok": True, "result": result})
    return 0


if __name__ == "__main__":
    sys.exit(main())
