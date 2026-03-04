import json
import tarfile
import time
from pathlib import Path


DEFAULT_FACTS_PATH = Path("tests/fixtures/provenance/facts.json")


def _load_facts(path: Path) -> dict:
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return {"facts": {}, "meta": {"created_ns": time.time_ns()}}


def _save_facts(data: dict, path: Path):
    data["meta"]["updated_ns"] = time.time_ns()
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=2, sort_keys=False)
        f.write("\n")


def _fact_key(category: str, key: str) -> str:
    return f"{category}:{key}"


def add_fact(
    path: Path,
    category: str,
    key: str,
    name: str,
    note: str | None = None,
    body: str | None = None,
    fields: list[dict] | None = None,
    evidence: list[str] | None = None,
    confidence: str = "verified",
    supersedes: str | None = None,
) -> dict:
    data = _load_facts(path)
    fk = _fact_key(category, key)

    existing = data["facts"].get(fk)

    fact = {
        "category": category,
        "key": key,
        "name": name,
        "note": note,
        "fields": fields or [],
        "evidence": evidence or [],
        "confidence": confidence,
        "added_ns": time.time_ns(),
    }

    if body is not None:
        fact["body"] = body
    elif existing and "body" in existing:
        fact["body"] = existing["body"]

    if supersedes:
        fact["supersedes"] = supersedes

    if existing:
        fact["evidence"] = _merge_evidence(existing.get("evidence", []), evidence or [])
        fact["history"] = existing.get("history", [])
        fact["history"].append({
            "replaced_ns": time.time_ns(),
            "previous_name": existing.get("name"),
            "previous_confidence": existing.get("confidence"),
            "previous_note": existing.get("note"),
        })

    data["facts"][fk] = fact
    _save_facts(data, path)
    return fact


def _merge_evidence(existing: list[str], new: list[str]) -> list[str]:
    seen = set(existing)
    merged = list(existing)
    for item in new:
        if item not in seen:
            merged.append(item)
            seen.add(item)
    return merged


def get_fact(path: Path, category: str, key: str) -> dict | None:
    data = _load_facts(path)
    return data["facts"].get(_fact_key(category, key))


def list_facts(path: Path, category: str | None = None) -> list[dict]:
    data = _load_facts(path)
    facts = list(data["facts"].values())

    if category:
        facts = [f for f in facts if f["category"] == category]

    facts.sort(key=lambda f: (f["category"], f["key"]))
    return facts


def get_categories(path: Path) -> list[str]:
    data = _load_facts(path)
    categories = sorted(set(f["category"] for f in data["facts"].values()))
    return categories


def remove_fact(path: Path, category: str, key: str) -> bool:
    data = _load_facts(path)
    fk = _fact_key(category, key)
    if fk in data["facts"]:
        del data["facts"][fk]
        _save_facts(data, path)
        return True
    return False


def disprove_fact(
    path: Path,
    category: str,
    key: str,
    reason: str,
    device_ip: str | None = None,
    response_size: int | None = None,
    field_mismatches: list[dict] | None = None,
) -> dict | None:
    data = _load_facts(path)
    fk = _fact_key(category, key)
    fact = data["facts"].get(fk)

    if fact is None:
        return None

    fact.setdefault("history", []).append({
        "replaced_ns": time.time_ns(),
        "previous_confidence": fact.get("confidence"),
        "previous_note": fact.get("note"),
        "action": "disproved",
    })

    fact["confidence"] = "disproved"

    disproval = {
        "reason": reason,
        "timestamp_ns": time.time_ns(),
    }
    if device_ip:
        disproval["device_ip"] = device_ip
    if response_size is not None:
        disproval["response_size"] = response_size
    if field_mismatches:
        disproval["field_mismatches"] = field_mismatches

    fact.setdefault("disprovals", []).append(disproval)

    data["facts"][fk] = fact
    _save_facts(data, path)
    return fact


def reinstate_fact(
    path: Path,
    category: str,
    key: str,
    confidence: str = "verified",
    note: str | None = None,
) -> dict | None:
    data = _load_facts(path)
    fk = _fact_key(category, key)
    fact = data["facts"].get(fk)

    if fact is None:
        return None

    fact.setdefault("history", []).append({
        "replaced_ns": time.time_ns(),
        "previous_confidence": fact.get("confidence"),
        "previous_note": fact.get("note"),
        "action": "reinstated",
    })

    fact["confidence"] = confidence
    if note is not None:
        fact["note"] = note

    data["facts"][fk] = fact
    _save_facts(data, path)
    return fact


def check_facts(path: Path, provenance_dir: Path | None = None) -> list[dict]:
    if provenance_dir is None:
        provenance_dir = path.parent

    data = _load_facts(path)
    results = []

    for fk, fact in data["facts"].items():
        result = {
            "fact_key": fk,
            "name": fact["name"],
            "category": fact["category"],
            "key": fact["key"],
            "confidence": fact.get("confidence", "unknown"),
            "evidence_count": len(fact.get("evidence", [])),
            "errors": [],
            "verified_fields": [],
        }

        evidence_refs = fact.get("evidence", [])
        if not evidence_refs:
            result["errors"].append("no evidence references")
            results.append(result)
            continue

        for ref in evidence_refs:
            session_ref, packet_id_str = _parse_evidence_ref(ref)
            if session_ref is None:
                result["errors"].append(f"invalid evidence ref: {ref}")
                continue

            bundle_path = _find_bundle(provenance_dir, session_ref)
            if bundle_path is None:
                result["errors"].append(f"bundle not found for {session_ref}")
                continue

            if packet_id_str is not None:
                packet_id = int(packet_id_str)
                manifest, files = _load_bundle(bundle_path)
                sample_by_id = {s.get("packet_id"): s for s in manifest.get("samples", [])}
                sample = sample_by_id.get(packet_id)

                if sample is None:
                    result["errors"].append(f"packet #{packet_id} not in bundle {session_ref}")
                    continue

                filename = sample.get("file", "")
                payload = files.get(filename)
                if payload is None:
                    result["errors"].append(f"payload file missing for packet #{packet_id}")
                    continue

                for field in fact.get("fields", []):
                    field_result = _verify_field(payload, field)
                    if field_result["ok"]:
                        result["verified_fields"].append(field_result)
                    else:
                        result["errors"].append(field_result["error"])

        results.append(result)

    return results


def _parse_evidence_ref(ref: str) -> tuple[str | None, str | None]:
    parts = ref.split(":")
    if len(parts) == 1:
        return parts[0], None
    if len(parts) == 2:
        return parts[0], parts[1]
    return None, None


def _find_bundle(provenance_dir: Path, session_ref: str) -> Path | None:
    dir_path = provenance_dir / session_ref
    if dir_path.is_dir() and (dir_path / "manifest.json").exists():
        return dir_path

    tar_path = provenance_dir / f"{session_ref}.tar.gz"
    if tar_path.exists():
        return tar_path

    for item in provenance_dir.iterdir():
        name = item.name.replace(".tar.gz", "")
        if session_ref in name:
            if item.is_dir() and (item / "manifest.json").exists():
                return item
            if not item.is_dir():
                return item

    return None


def _load_bundle(bundle_path: Path) -> tuple[dict, dict[str, bytes]]:
    manifest = {}
    files = {}

    if bundle_path.suffix == ".gz" and bundle_path.stem.endswith(".tar"):
        with tarfile.open(bundle_path, "r:gz") as tar:
            for member in tar.getmembers():
                if member.name.endswith("manifest.json"):
                    f = tar.extractfile(member)
                    if f:
                        manifest = json.loads(f.read())
                elif member.name.endswith(".bin"):
                    f = tar.extractfile(member)
                    if f:
                        files[Path(member.name).name] = f.read()
    elif bundle_path.is_dir():
        manifest_path = bundle_path / "manifest.json"
        if manifest_path.exists():
            with open(manifest_path) as f:
                manifest = json.load(f)
        for bin_file in bundle_path.glob("*.bin"):
            files[bin_file.name] = bin_file.read_bytes()

    return manifest, files


def _extract_field_value(payload: bytes, field: dict) -> tuple[object, str]:
    import struct

    offset = field.get("offset", 0)
    length = field.get("length", 0)
    dtype = field.get("dtype", "")

    raw = payload[offset : offset + length]

    if dtype == "uint8" and length == 1:
        value = raw[0]
    elif dtype == "uint16_be" and length == 2:
        value = struct.unpack(">H", raw)[0]
    elif dtype == "uint32_be" and length == 4:
        value = struct.unpack(">I", raw)[0]
    elif dtype == "int32_be" and length == 4:
        value = struct.unpack(">i", raw)[0]
    elif dtype == "ascii":
        value = raw.rstrip(b"\x00").decode("ascii", errors="replace")
    elif dtype == "ipv4" and length == 4:
        value = f"{raw[0]}.{raw[1]}.{raw[2]}.{raw[3]}"
    elif dtype == "hex":
        value = raw.hex()
    else:
        value = raw.hex()

    if isinstance(value, int) and dtype in ("uint16_be", "uint32_be", "uint8"):
        display = f"0x{value:04X}" if dtype == "uint16_be" else f"0x{value:08X}" if dtype == "uint32_be" else f"0x{value:02X}"
    else:
        display = str(value)

    return value, display


def _verify_field(payload: bytes, field: dict) -> dict:
    name = field.get("name", "?")
    offset = field.get("offset", 0)
    length = field.get("length", 0)
    expected_value = field.get("value")

    if offset + length > len(payload):
        return {
            "ok": False,
            "name": name,
            "bounds": True,
            "error": f"field {name}: offset {offset}+{length} exceeds payload length {len(payload)}",
        }

    try:
        actual, actual_display = _extract_field_value(payload, field)
    except Exception as exc:
        return {
            "ok": False,
            "name": name,
            "error": f"field {name}: parse error: {exc}",
        }

    if expected_value is None:
        return {"ok": True, "name": name, "expected": None, "actual": actual_display, "value": actual}

    expected_str = str(expected_value)
    actual_str = str(actual)

    if expected_str.startswith("0x"):
        try:
            expected_int = int(expected_str, 16)
            match = int(actual_str) == expected_int if actual_str.isdigit() else actual_str == expected_str
        except ValueError:
            match = actual_str == expected_str
    else:
        match = actual_str == expected_str

    if match:
        return {"ok": True, "name": name, "expected": expected_str, "actual": actual_display, "value": actual}

    return {
        "ok": False,
        "name": name,
        "error": f"field {name}: expected {expected_str}, got {actual_display} at offset {offset}",
        "expected": expected_str,
        "actual": actual_display,
        "value": actual,
    }
