"""Resource-bounded orchestration for multiple DDM lab guests."""

from __future__ import annotations

import math
import re
import uuid
from typing import Any

from ._lifecycle_support import LabError, MAX_IDENTITY_NUMBER, validate_identity


MAX_TOPOLOGY_GUESTS = 64
MAX_CAPTURE_RESERVATION_MIB = 256
TOPOLOGY_NAME_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,31}")


def _bounded_integer(value: Any, *, minimum: int, maximum: int, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise LabError(f"{label} must be an integer between {minimum} and {maximum}")
    return value


def _validated_identities(identities: list[str]) -> list[str]:
    if not isinstance(identities, list) or not identities:
        raise LabError("at least one topology identity is required")
    if len(identities) > MAX_TOPOLOGY_GUESTS:
        raise LabError(f"a topology batch is limited to {MAX_TOPOLOGY_GUESTS} guests")
    if any(not isinstance(identity, str) for identity in identities):
        raise LabError("topology identities must be strings")
    validated = [validate_identity(identity) for identity in identities]
    if len(set(validated)) != len(validated):
        raise LabError("topology identities must be unique")
    return validated


def select_identities(explicit: list[str] | None, count: int | None, first: int) -> list[str]:
    """Select one explicit identity list or a bounded consecutive range."""

    if explicit is not None:
        if count is not None:
            raise LabError("explicit identities and a generated identity count are mutually exclusive")
        return _validated_identities(explicit)

    selected_count = (
        2
        if count is None
        else _bounded_integer(
            count,
            minimum=1,
            maximum=MAX_TOPOLOGY_GUESTS,
            label="identity count",
        )
    )
    first_number = _bounded_integer(
        first,
        minimum=1,
        maximum=MAX_IDENTITY_NUMBER,
        label="first identity",
    )
    last_number = first_number + selected_count - 1
    if last_number > MAX_IDENTITY_NUMBER:
        raise LabError(f"identity range exceeds {MAX_IDENTITY_NUMBER}")
    return _validated_identities(
        [validate_identity(f"{number:03d}") for number in range(first_number, last_number + 1)]
    )


def _capture_reservation(
    identities: list[str],
    *,
    capture: bool,
    capture_seconds: int,
    capture_mib: int,
) -> int:
    if not isinstance(capture, bool):
        raise LabError("capture must be a boolean")
    if not capture:
        return 0
    _bounded_integer(capture_seconds, minimum=1, maximum=3600, label="capture duration")
    per_guest_mib = _bounded_integer(capture_mib, minimum=1, maximum=256, label="per-guest capture limit")
    reservation = len(identities) * per_guest_mib
    if reservation > MAX_CAPTURE_RESERVATION_MIB:
        raise LabError(
            f"requested captures reserve {reservation} MiB; topology limit is {MAX_CAPTURE_RESERVATION_MIB} MiB"
        )
    return reservation


def _validate_wait_seconds(wait_seconds: Any) -> None:
    if (
        isinstance(wait_seconds, bool)
        or not isinstance(wait_seconds, (int, float))
        or not math.isfinite(wait_seconds)
        or not 0 < wait_seconds <= 600
    ):
        raise LabError("wait_seconds must be a finite number between 0 and 600")


def _new_topology_id(name: str | None) -> str:
    if name is None:
        return f"topology-{uuid.uuid4().hex}"
    if not TOPOLOGY_NAME_PATTERN.fullmatch(name):
        raise LabError(f"invalid topology name: {name!r}")
    return f"topology-{name}-{uuid.uuid4().hex[:8]}"


def _topology_from_state(identity: str, state: dict[str, Any]) -> str | None:
    run_id = state.get("run_id")
    suffix = f"-i{identity}"
    if not isinstance(run_id, str) or not run_id.startswith("topology-") or not run_id.endswith(suffix):
        return None
    return run_id[: -len(suffix)]


def _selected_active_states(harness: Any, topology_id: str | None) -> dict[str, dict[str, Any]]:
    states: dict[str, dict[str, Any]] = {}
    for identity in harness.active_identities():
        state = harness.state_for_identity(identity)
        if topology_id is None or _topology_from_state(identity, state) == topology_id:
            states[identity] = state
    if topology_id is not None and not states:
        raise LabError(f"no active guests belong to topology {topology_id}")
    return states


def start_many(
    harness: Any,
    identities: list[str],
    wait_seconds: float,
    capture: bool,
    capture_seconds: int,
    capture_mib: int,
    name: str | None = None,
) -> dict[str, Any]:
    """Start a bounded guest set sequentially and roll it back atomically on failure."""

    selected = _validated_identities(identities)
    _validate_wait_seconds(wait_seconds)
    maximum_active = getattr(getattr(harness, "configuration", None), "max_active_guests", None)
    if isinstance(maximum_active, bool) or not isinstance(maximum_active, int) or maximum_active < 1:
        raise LabError("harness max_active_guests configuration is invalid")
    if len(selected) > maximum_active:
        raise LabError(f"selected topology has {len(selected)} guests; harness limit is {maximum_active}")
    capture_reservation_mib = _capture_reservation(
        selected,
        capture=capture,
        capture_seconds=capture_seconds,
        capture_mib=capture_mib,
    )
    topology_id = _new_topology_id(name)
    started: list[str] = []
    guests: dict[str, Any] = {}

    with harness.topology_action():
        active = harness.active_identities()
        if set(active) & set(selected):
            raise LabError("one or more selected topology identities are already active")
        if len(active) + len(selected) > maximum_active:
            raise LabError(
                f"starting {len(selected)} guests with {len(active)} already active exceeds "
                f"the harness limit of {maximum_active}"
            )
        for identity in selected:
            harness.validate(identity)
        try:
            for identity in selected:
                guests[identity] = harness.start(
                    identity,
                    run_id=f"{topology_id}-i{identity}",
                    purpose="topology",
                    wait_seconds=wait_seconds,
                    capture=capture,
                    capture_seconds=capture_seconds,
                    capture_mib=capture_mib,
                )
                started.append(identity)
        except BaseException as error:
            rollback_errors: list[str] = []
            for identity in reversed(started):
                try:
                    harness.stop(identity, retain_session=False)
                except BaseException as rollback_error:
                    rollback_errors.append(f"{identity}: {rollback_error}")
            if rollback_errors:
                raise LabError(
                    f"topology {topology_id} start failed: {error}; rollback was incomplete: "
                    f"{'; '.join(rollback_errors)}"
                ) from error
            raise

    return {
        "topology_id": topology_id,
        "name": name,
        "identities": selected,
        "guest_count": len(selected),
        "capture_reservation_mib": capture_reservation_mib,
        "persistent": True,
        "guests": guests,
    }


def status_all(harness: Any, topology_id: str | None = None) -> dict[str, Any]:
    """Return status for every identity currently leased by the harness."""

    with harness.topology_action():
        states = _selected_active_states(harness, topology_id)
        guests = {identity: harness.status(identity) for identity in states}
        identities = list(guests)
    topologies: dict[str, list[str]] = {}
    ungrouped: list[str] = []
    for identity, state in states.items():
        member_of = _topology_from_state(identity, state)
        if member_of is None:
            ungrouped.append(identity)
        else:
            topologies.setdefault(member_of, []).append(identity)
    return {
        "topology_id": topology_id,
        "identities": identities,
        "guest_count": len(identities),
        "topologies": topologies,
        "ungrouped_identities": ungrouped,
        "guests": guests,
    }


def stop_all(
    harness: Any,
    retain_session: bool = False,
    topology_id: str | None = None,
) -> dict[str, Any]:
    """Stop every currently leased identity, attempting all exact guests once."""

    if not isinstance(retain_session, bool):
        raise LabError("retain_session must be a boolean")
    stopped: dict[str, Any] = {}
    errors: list[str] = []
    with harness.topology_action():
        identities = list(_selected_active_states(harness, topology_id))
        for identity in reversed(identities):
            try:
                stopped[identity] = harness.stop(identity, retain_session=retain_session)
            except BaseException as error:
                errors.append(f"{identity}: {error}")
    if errors:
        raise LabError(f"failed to stop all topology guests: {'; '.join(errors)}")
    return {
        "topology_id": topology_id,
        "identities": identities,
        "guest_count": len(identities),
        "stopped": stopped,
        "retain_session": retain_session,
    }


__all__ = [
    "MAX_CAPTURE_RESERVATION_MIB",
    "MAX_TOPOLOGY_GUESTS",
    "select_identities",
    "start_many",
    "status_all",
    "stop_all",
]
