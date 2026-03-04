import json
import time
from pathlib import Path


DEFAULT_BUGS_DIR = Path("tests/fixtures/provenance/bugs")


def _bug_path(bugs_dir: Path, label: str) -> Path:
    return bugs_dir / f"{label}.json"


def _iso_from_ns(timestamp_ns: int) -> str:
    from datetime import datetime, timezone

    dt = datetime.fromtimestamp(timestamp_ns / 1e9, tz=timezone.utc)
    return dt.astimezone().isoformat()


def report_bug(
    bugs_dir: Path,
    label: str,
    summary: str,
    note: str | None = None,
    tags: list[str] | None = None,
    context: dict | None = None,
    session_id: int | None = None,
    marker_id: int | None = None,
) -> dict:
    bugs_dir.mkdir(parents=True, exist_ok=True)
    path = _bug_path(bugs_dir, label)

    if path.exists():
        raise ValueError(f"Bug already exists: {label}")

    now_ns = time.time_ns()

    bug = {
        "label": label,
        "summary": summary,
        "status": "open",
        "tags": tags or [],
        "context": context or {},
        "reported_ns": now_ns,
        "reported_iso": _iso_from_ns(now_ns),
        "updated_ns": now_ns,
        "updated_iso": _iso_from_ns(now_ns),
        "history": [
            _event("reported", note=note, session_id=session_id, marker_id=marker_id, timestamp_ns=now_ns),
        ],
    }

    _save_bug(bug, path)
    return bug


def close_bug(
    bugs_dir: Path,
    label: str,
    note: str | None = None,
    session_id: int | None = None,
    marker_id: int | None = None,
) -> dict:
    bug = get_bug(bugs_dir, label)

    if bug is None:
        raise ValueError(f"Bug not found: {label}")

    if bug["status"] == "closed":
        raise ValueError(f"Bug already closed: {label}")

    now_ns = time.time_ns()
    bug["status"] = "closed"
    bug["updated_ns"] = now_ns
    bug["updated_iso"] = _iso_from_ns(now_ns)
    bug["history"].append(
        _event("closed", note=note, session_id=session_id, marker_id=marker_id, timestamp_ns=now_ns)
    )

    _save_bug(bug, _bug_path(bugs_dir, label))
    return bug


def reopen_bug(
    bugs_dir: Path,
    label: str,
    note: str | None = None,
    session_id: int | None = None,
    marker_id: int | None = None,
) -> dict:
    bug = get_bug(bugs_dir, label)

    if bug is None:
        raise ValueError(f"Bug not found: {label}")

    if bug["status"] != "closed":
        raise ValueError(f"Bug is not closed: {label} (status={bug['status']})")

    now_ns = time.time_ns()
    bug["status"] = "reopened"
    bug["updated_ns"] = now_ns
    bug["updated_iso"] = _iso_from_ns(now_ns)
    bug["history"].append(
        _event("reopened", note=note, session_id=session_id, marker_id=marker_id, timestamp_ns=now_ns)
    )

    _save_bug(bug, _bug_path(bugs_dir, label))
    return bug


def get_bug(bugs_dir: Path, label: str) -> dict | None:
    path = _bug_path(bugs_dir, label)

    if not path.exists():
        return None

    with open(path) as f:
        return json.load(f)


def list_bugs(
    bugs_dir: Path,
    status: str | None = None,
    tag: str | None = None,
    reported_after_ns: int | None = None,
    reported_before_ns: int | None = None,
    updated_after_ns: int | None = None,
    updated_before_ns: int | None = None,
) -> list[dict]:
    if not bugs_dir.exists():
        return []

    bugs = []

    for path in sorted(bugs_dir.glob("*.json")):
        with open(path) as f:
            bug = json.load(f)

        if status is not None and bug["status"] != status:
            continue

        if tag is not None and tag not in bug.get("tags", []):
            continue

        if reported_after_ns is not None and bug.get("reported_ns", 0) < reported_after_ns:
            continue

        if reported_before_ns is not None and bug.get("reported_ns", 0) > reported_before_ns:
            continue

        if updated_after_ns is not None and bug.get("updated_ns", 0) < updated_after_ns:
            continue

        if updated_before_ns is not None and bug.get("updated_ns", 0) > updated_before_ns:
            continue

        bugs.append(bug)

    bugs.sort(key=lambda b: b.get("reported_ns", 0))
    return bugs


def _event(
    action: str,
    note: str | None = None,
    session_id: int | None = None,
    marker_id: int | None = None,
    timestamp_ns: int | None = None,
) -> dict:
    ts = timestamp_ns or time.time_ns()
    event = {
        "action": action,
        "timestamp_ns": ts,
        "timestamp_iso": _iso_from_ns(ts),
    }

    if note:
        event["note"] = note

    if session_id is not None:
        event["session_id"] = session_id

    if marker_id is not None:
        event["marker_id"] = marker_id

    return event


def _save_bug(bug: dict, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)

    with open(path, "w") as f:
        json.dump(bug, f, indent=2, sort_keys=False)
        f.write("\n")
