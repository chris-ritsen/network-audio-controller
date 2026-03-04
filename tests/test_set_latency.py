import json
import struct
import tarfile
import tempfile
from pathlib import Path

import pytest

FIXTURES_ROOT = Path(__file__).parent / "fixtures" / "provenance"

_extract_cache = {}


def _extract_tarball(name):
    if name in _extract_cache:
        return _extract_cache[name]
    archive = FIXTURES_ROOT / f"{name}.tar.gz"
    tmpdir = tempfile.mkdtemp()
    with tarfile.open(archive, "r:gz") as tar:
        tar.extractall(tmpdir, filter="data")
    extracted = Path(tmpdir) / name
    _extract_cache[name] = extracted
    return extracted

SESSION_27 = _extract_tarball("session_27_set_latency_full_provenance")
SESSION_28 = _extract_tarball("session_28_set_latency_full_provenance_step2")

LATENCY_OFFSET = 108
MIN_LATENCY_OFFSET = 120
MAX_LATENCY_OFFSET = 116
DEFAULT_LATENCY_OFFSET = 104
SET_TEMPLATE_RANGE = slice(8, 32)

ALL_VALUES_NS = [150_000, 250_000, 500_000, 1_000_000, 2_000_000, 5_000_000, 10_000_000]


def load_manifest(session_dir):
    with open(session_dir / "manifest.json") as manifest_file:
        return json.load(manifest_file)


def load_bin(session_dir, filename):
    return (session_dir / filename).read_bytes()


def parse_header(data):
    return {
        "protocol_id": struct.unpack(">H", data[0:2])[0],
        "opcode": struct.unpack(">H", data[6:8])[0],
        "status": struct.unpack(">H", data[8:10])[0] if len(data) >= 10 else None,
    }


def samples_by(manifest, opcode_hex, direction, evidence=False):
    return [
        s for s in manifest["samples"]
        if s.get("opcode_hex") == opcode_hex
        and s.get("direction") == direction
        and s.get("evidence", False) == evidence
    ]


def latency_from_set_payload(data):
    return struct.unpack(">I", data[-8:-4])[0], struct.unpack(">I", data[-4:])[0]


def latency_from_read_response(data):
    return struct.unpack(">I", data[LATENCY_OFFSET : LATENCY_OFFSET + 4])[0]


def narrate(lines):
    print()
    for line in lines:
        print(f"    {line}")
    print()


def session_dir(target_ns):
    return _extract_tarball(f"session_latency_{target_ns}ns")


class TestDeviceReportsLatencyRange:

    def test_read_response_contains_min_max_default(self):
        manifest = load_manifest(session_dir(150_000))
        read_response = samples_by(manifest, "0x1100", "response")[0]
        payload = load_bin(session_dir(150_000), read_response["file"])

        current_ns = struct.unpack(">I", payload[LATENCY_OFFSET:LATENCY_OFFSET + 4])[0]
        min_ns = struct.unpack(">I", payload[MIN_LATENCY_OFFSET:MIN_LATENCY_OFFSET + 4])[0]
        max_ns = struct.unpack(">I", payload[MAX_LATENCY_OFFSET:MAX_LATENCY_OFFSET + 4])[0]
        default_ns = struct.unpack(">I", payload[DEFAULT_LATENCY_OFFSET:DEFAULT_LATENCY_OFFSET + 4])[0]

        narrate([
            "DEVICE:     LX-DANTE-081258 (192.168.1.108)",
            f"READ:       Opcode 0x1100, {len(payload)}-byte response",
            f"CURRENT:    offset {LATENCY_OFFSET}: {current_ns} ns ({current_ns / 1_000_000} ms)",
            f"DEFAULT:    offset {DEFAULT_LATENCY_OFFSET}: {default_ns} ns ({default_ns / 1_000_000} ms)",
            f"MIN:        offset {MIN_LATENCY_OFFSET}: {min_ns} ns ({min_ns / 1_000_000} ms)",
            f"MAX:        offset {MAX_LATENCY_OFFSET}: {max_ns} ns ({max_ns / 1_000_000} ms)",
        ])

        assert min_ns == 150_000
        assert max_ns == 21_333_334
        assert default_ns == 1_000_000


@pytest.mark.parametrize("target_ns", ALL_VALUES_NS)
class TestSetLatencyValue:

    def test_device_accepted_value(self, target_ns):
        directory = session_dir(target_ns)
        manifest = load_manifest(directory)
        target_ms = target_ns / 1_000_000

        set_request = samples_by(manifest, "0x1101", "request")[0]
        set_response = samples_by(manifest, "0x1101", "response")[0]
        req_payload = load_bin(directory, set_request["file"])
        resp_payload = load_bin(directory, set_response["file"])

        req_val = struct.unpack(">I", req_payload[-8:-4])[0]
        resp_status = parse_header(resp_payload)["status"]
        resp_echo = struct.unpack(">I", resp_payload[-8:-4])[0]

        narrate([
            f"SET:        {target_ms}ms ({target_ns} ns)",
            f"REQUEST:    {req_payload.hex()} ({len(req_payload)} bytes)",
            f"RESPONSE:   status=0x{resp_status:04X} ({'OK' if resp_status == 1 else 'FAIL'}), echo={resp_echo} ns",
        ])

        assert len(req_payload) == 40
        assert req_val == target_ns
        assert resp_status == 0x0001
        assert resp_echo == target_ns

    def test_read_back_confirms_change(self, target_ns):
        directory = session_dir(target_ns)
        manifest = load_manifest(directory)
        target_ms = target_ns / 1_000_000

        read_responses = samples_by(manifest, "0x1100", "response")
        after_payload = load_bin(directory, read_responses[-1]["file"])
        readback_ns = struct.unpack(">I", after_payload[LATENCY_OFFSET:LATENCY_OFFSET + 4])[0]

        narrate([
            f"READ-BACK:  After setting {target_ms}ms",
            f"OFFSET {LATENCY_OFFSET}: 0x{after_payload[LATENCY_OFFSET:LATENCY_OFFSET + 4].hex()} = {readback_ns} ns ({readback_ns / 1_000_000} ms)",
            f"MATCH:      {'YES' if readback_ns == target_ns else 'NO'}",
        ])

        assert readback_ns == target_ns

    def test_user_confirmed_in_dante_controller(self, target_ns):
        directory = session_dir(target_ns)
        manifest = load_manifest(directory)
        target_ms = target_ns / 1_000_000

        user_marker = next(
            (m for m in manifest["markers"] if m.get("label") == "user_confirmed"),
            None,
        )

        narrate([
            f"USER:       {user_marker['note']}",
            f"DATA:       {user_marker.get('data', {})}",
        ])

        assert user_marker is not None
        assert user_marker["marker_type"] == "observation"

    def test_template_matches_dante_controller(self, target_ns):
        directory = session_dir(target_ns)
        manifest = load_manifest(directory)

        set_request = samples_by(manifest, "0x1101", "request")[0]
        req_payload = load_bin(directory, set_request["file"])
        template = req_payload[SET_TEMPLATE_RANGE]

        expected_template = bytes.fromhex("000005048205002002110004830100240310000483028306")

        narrate([
            f"TEMPLATE:   bytes 8-32 of set request for {target_ns / 1_000_000}ms",
            f"OURS:       {template.hex()}",
            f"EXPECTED:   {expected_template.hex()}",
            f"MATCH:      {'IDENTICAL' if template == expected_template else 'MISMATCH'}",
        ])

        assert template == expected_template


class TestDanteControllerDoesNotOffer10ms:

    def test_10ms_hypothesis_documents_dc_limitation(self):
        manifest = load_manifest(session_dir(10_000_000))
        hypothesis = next(m for m in manifest["markers"] if m["marker_type"] == "hypothesis")

        narrate([
            f"HYPOTHESIS: {hypothesis['note']}",
            "RESULT:     Device accepted 10ms — status 0x0001, read-back confirmed",
        ])

        assert "Dante Controller does not offer 10ms" in hypothesis["note"]

    def test_10ms_accepted_despite_not_in_dc_dropdown(self):
        directory = session_dir(10_000_000)
        manifest = load_manifest(directory)

        resp_payload = load_bin(directory, samples_by(manifest, "0x1101", "response")[0]["file"])
        readback_payload = load_bin(directory, samples_by(manifest, "0x1100", "response")[-1]["file"])

        resp_status = parse_header(resp_payload)["status"]
        readback_ns = struct.unpack(">I", readback_payload[LATENCY_OFFSET:LATENCY_OFFSET + 4])[0]

        user_marker = next(m for m in manifest["markers"] if m.get("label") == "user_confirmed")

        narrate([
            "FINDING:    10ms is NOT offered in Dante Controller's latency dropdown",
            f"DEVICE:     Accepted anyway — status=0x{resp_status:04X}, read-back={readback_ns}ns (10.0ms)",
            f"DC BEHAVIOR:{user_marker['data']['notable']}",
            "CONCLUSION: Device accepts arbitrary values within min/max range (0.15ms-21.3ms)",
            "            Dante Controller's dropdown is a UI constraint, not a protocol constraint",
        ])

        assert resp_status == 0x0001
        assert readback_ns == 10_000_000


class TestAllValuesConclusion:

    def test_all_seven_values_proven(self):
        results = []
        for target_ns in ALL_VALUES_NS:
            directory = session_dir(target_ns)
            manifest = load_manifest(directory)
            resp_payload = load_bin(directory, samples_by(manifest, "0x1101", "response")[0]["file"])
            readback_payload = load_bin(directory, samples_by(manifest, "0x1100", "response")[-1]["file"])

            status = parse_header(resp_payload)["status"]
            readback = struct.unpack(">I", readback_payload[LATENCY_OFFSET:LATENCY_OFFSET + 4])[0]
            results.append((target_ns, status, readback))

        narrate([
            "=" * 72,
            "CONCLUSION: All Dante Latency Values Tested and Proven",
            "=" * 72,
            "",
            "DEVICE:     LX-DANTE-081258 (192.168.1.108)",
            "RANGE:      0.15ms (150,000 ns) to 21.3ms (21,333,334 ns)",
            "DEFAULT:    1.0ms (1,000,000 ns)",
            "",
            "  VALUE        STATUS    READ-BACK",
            "  ----------   ------    ---------",
        ] + [
            f"  {ns/1_000_000:>6.2f} ms    0x{st:04X}    {rb/1_000_000:.2f} ms {'*' if ns == 10_000_000 else ''}"
            for ns, st, rb in results
        ] + [
            "",
            "  * 10ms not offered by Dante Controller but accepted by device",
            "",
            "ALL VALUES: Accepted (status 0x0001), read-back matches target",
            "=" * 72,
        ])

        for target_ns, status, readback in results:
            assert status == 0x0001
            assert readback == target_ns


class TestEvidenceChain:

    def test_dante_controller_capture_matches_our_template(self):
        manifest = load_manifest(SESSION_28)
        evidence = [
            s
            for s in manifest["samples"]
            if s.get("evidence")
            and s.get("session_id") == 10
            and s.get("direction") == "request"
        ]
        controller_payload = load_bin(SESSION_28, evidence[0]["file"])

        our_request_27 = load_bin(
            SESSION_27,
            samples_by(load_manifest(SESSION_27), "0x1101", "request")[0]["file"],
        )
        our_request_28 = load_bin(
            SESSION_28,
            samples_by(manifest, "0x1101", "request")[0]["file"],
        )

        narrate([
            "TEMPLATE:   Compare bytes 8-32 between Dante Controller and our packets",
            f"CONTROLLER: {controller_payload[SET_TEMPLATE_RANGE].hex()}  (captured from macbook, session 10)",
            f"SESSION 27: {our_request_27[SET_TEMPLATE_RANGE].hex()}  (our set 250us packet)",
            f"SESSION 28: {our_request_28[SET_TEMPLATE_RANGE].hex()}  (our set 1ms packet)",
            f"SOURCE:     Dante Controller on macbook (192.168.1.156) → LX-DANTE-081258 ({evidence[0]['dst_ip']}:{evidence[0]['dst_port']})",
        ])

        assert controller_payload[SET_TEMPLATE_RANGE] == our_request_27[SET_TEMPLATE_RANGE]
        assert controller_payload[SET_TEMPLATE_RANGE] == our_request_28[SET_TEMPLATE_RANGE]

    def test_session_27_evidence_included_in_session_28_bundle(self):
        manifest = load_manifest(SESSION_28)
        step1_evidence = [
            s for s in manifest["samples"] if s.get("evidence") and s.get("session_id") == 27
        ]

        step1_reads = [s for s in step1_evidence if s.get("opcode_hex") == "0x1100"]
        step1_sets = [s for s in step1_evidence if s.get("opcode_hex") == "0x1101"]

        narrate([
            "EVIDENCE:   Session 28 bundle includes session 27 packets as evidence",
            f"            {len(step1_reads)} read packets (0x1100) from session 27",
            f"            {len(step1_sets)} set packets (0x1101) from session 27",
            "PURPOSE:    Full audit trail — both experiments in one bundle",
        ])

        assert len(step1_reads) == 4
        assert len(step1_sets) == 2

    def test_conclusion_set_latency_wire_protocol_fully_proven(self):
        manifest_27 = load_manifest(SESSION_27)
        manifest_28 = load_manifest(SESSION_28)

        read_before_27 = load_bin(SESSION_27, samples_by(manifest_27, "0x1100", "response")[0]["file"])
        read_after_27 = load_bin(SESSION_27, samples_by(manifest_27, "0x1100", "response")[-1]["file"])
        read_before_28 = load_bin(SESSION_28, samples_by(manifest_28, "0x1100", "response")[0]["file"])
        read_after_28 = load_bin(SESSION_28, samples_by(manifest_28, "0x1100", "response")[-1]["file"])

        set_27 = load_bin(SESSION_27, samples_by(manifest_27, "0x1101", "request")[0]["file"])
        resp_27 = load_bin(SESSION_27, samples_by(manifest_27, "0x1101", "response")[0]["file"])
        set_28 = load_bin(SESSION_28, samples_by(manifest_28, "0x1101", "request")[0]["file"])
        resp_28 = load_bin(SESSION_28, samples_by(manifest_28, "0x1101", "response")[0]["file"])

        controller_evidence = [
            s for s in manifest_28["samples"]
            if s.get("evidence") and s.get("session_id") == 10 and s.get("direction") == "request"
        ]
        controller_payload = load_bin(SESSION_28, controller_evidence[0]["file"])

        narrate([
            "=" * 72,
            "CONCLUSION: Dante Set Latency Wire Protocol — Fully Proven",
            "=" * 72,
            "",
            "PROTOCOL:   0x2729",
            "SET OPCODE: 0x1101 (40-byte request, 36-byte response)",
            "READ OPCODE:0x1100 (58-byte request, 140-byte response)",
            "PORT:       4440 (ARC)",
            "",
            "ENCODING:   Latency in nanoseconds, 4-byte big-endian",
            "            Set: repeated twice at end of 40-byte payload (offsets 32-36 and 36-40)",
            "            Read: at offset 108 of 140-byte response",
            f"TEMPLATE:   Fixed bytes 8-32: {controller_payload[SET_TEMPLATE_RANGE].hex()}",
            "",
            "EVIDENCE CHAIN:",
            f"  1. Captured Dante Controller setting 250us from macbook (session 10)",
            f"  2. Read device: {latency_from_read_response(read_before_27)} ns (1.0ms), user confirmed 1.0ms",
            f"  3. Set 250us: status=0x{parse_header(resp_27)['status']:04X}, user confirmed 0.25ms",
            f"  4. Read-back: {latency_from_read_response(read_after_27)} ns (0.25ms) — wire confirms change",
            f"  5. Read device: {latency_from_read_response(read_before_28)} ns (0.25ms), user confirmed 0.25ms",
            f"  6. Set 1ms:  status=0x{parse_header(resp_28)['status']:04X}, user confirmed 1.0ms",
            f"  7. Read-back: {latency_from_read_response(read_after_28)} ns (1.0ms) — wire confirms change",
            "=" * 72,
        ])

        assert latency_from_read_response(read_before_27) == 1_000_000
        assert latency_from_set_payload(set_27)[0] == 250_000
        assert parse_header(resp_27)["status"] == 0x0001
        assert latency_from_read_response(read_after_27) == 250_000

        assert latency_from_read_response(read_before_28) == 250_000
        assert latency_from_set_payload(set_28)[0] == 1_000_000
        assert parse_header(resp_28)["status"] == 0x0001
        assert latency_from_read_response(read_after_28) == 1_000_000

        assert set_27[SET_TEMPLATE_RANGE] == controller_payload[SET_TEMPLATE_RANGE]
        assert set_28[SET_TEMPLATE_RANGE] == controller_payload[SET_TEMPLATE_RANGE]
