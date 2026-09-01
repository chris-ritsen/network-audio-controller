from __future__ import annotations

import asyncio
import socket
import struct
import sys
import time
from pathlib import Path
from typing import Optional

import typer

from netaudio.commands.capture_helpers import (
    _load_capture_profile,
    _normalize_marker_label,
    _require_positive_session_id,
    _resolve_db_from_config,
    _resolve_provenance_bundle_path,
    _resolve_session_reference,
)
from netaudio.capture.packets import _compact_hexdump, _hexdump, _label_packet
from netaudio.commands.provenance_app import app
from netaudio.dante.packet_store import PacketStore


@app.command("send")
def provenance_send(
    device_ip: str = typer.Option(..., "--device-ip", help="Target device IP address."),
    port: int = typer.Option(4440, "--port", help="Target UDP port."),
    payload_hex: Optional[str] = typer.Option(None, "--payload-hex", help="Raw payload as hex string."),
    packet_id: Optional[int] = typer.Option(
        None, "--packet-id", help="Replay an existing packet's payload (to a new target)."
    ),
    label: str = typer.Option(..., "--label", help="Label for this send (used in evidence marker)."),
    note: Optional[str] = typer.Option(None, "--note", help="Descriptive note."),
    session_id: Optional[int] = typer.Option(None, "--session-id", help="Session ID."),
    session: Optional[str] = typer.Option(
        None,
        "--session",
        help="Session reference (ID, name, latest, active). Defaults to active.",
    ),
    timeout: float = typer.Option(2.0, "--timeout", help="Response timeout in seconds."),
    dump: bool = typer.Option(False, "--dump", help="Dump packet payloads as hex + ASCII."),
    db: Optional[str] = typer.Option(None, "--db", help="SQLite database path."),
    config: Optional[str] = typer.Option(None, "--config", help="Capture config TOML path."),
    profile: Optional[str] = typer.Option(None, "--profile", help="Capture config profile name."),
):
    if payload_hex is None and packet_id is None:
        print("Error: Provide --payload-hex or --packet-id.", file=sys.stderr)
        raise typer.Exit(1)

    _require_positive_session_id(session_id, "--session-id")
    profile_cfg, _ = _load_capture_profile(config, profile)
    resolved_db = _resolve_db_from_config(db, profile_cfg)

    store = PacketStore(db_path=resolved_db)
    try:
        resolved_session_id, _ = _resolve_session_reference(
            store,
            session_id=session_id,
            session=session,
            default_selector="active",
        )

        if payload_hex is not None:
            payload = bytes.fromhex(payload_hex.replace(" ", "").replace(":", ""))
        else:
            source_packet = store.get_packet(packet_id)
            if not source_packet:
                print(f"Error: Packet #{packet_id} not found.", file=sys.stderr)
                raise typer.Exit(1)
            payload = source_packet["payload"]
            if isinstance(payload, str):
                payload = bytes.fromhex(payload)

        _do_send(
            payload=payload,
            device_ip=device_ip,
            port=port,
            label=label,
            note=note,
            session_id=resolved_session_id,
            store=store,
            timeout=timeout,
            dump=dump,
        )
    finally:
        store.close()


def _do_send(
    payload: bytes,
    device_ip: str,
    port: int,
    label: str,
    note: str | None,
    session_id: int,
    store: PacketStore,
    timeout: float,
    dump: bool,
):
    normalized_label = _normalize_marker_label(label)
    source_host = socket.gethostname()

    probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    probe.connect((device_ip, port))
    local_ip = probe.getsockname()[0]
    probe.close()

    send_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    send_socket.settimeout(timeout)

    send_timestamp = time.time_ns()
    send_socket.sendto(payload, (device_ip, port))
    local_port = send_socket.getsockname()[1]

    sent_id = store.store_packet(
        payload=payload,
        source_type="provenance_send",
        src_ip=local_ip,
        src_port=local_port,
        dst_ip=device_ip,
        dst_port=port,
        device_ip=device_ip,
        direction="request",
        timestamp_ns=send_timestamp,
        session_id=session_id,
        source_host=source_host,
    )

    info = _label_packet(payload)
    print(f"Sent: #{sent_id}  {local_ip}:{local_port} -> {device_ip}:{port}  {len(payload)}B  {info or ''}")

    if dump:
        print(_hexdump(payload))

    tagged_packet_ids = []
    if sent_id:
        tagged_packet_ids.append(sent_id)

    try:
        reply_data, reply_addr = send_socket.recvfrom(4096)
        reply_timestamp = time.time_ns()
        reply_ip, reply_port = reply_addr

        reply_id = store.store_packet(
            payload=reply_data,
            source_type="provenance_send",
            src_ip=reply_ip,
            src_port=reply_port,
            dst_ip=local_ip,
            dst_port=local_port,
            device_ip=reply_ip,
            direction="response",
            timestamp_ns=reply_timestamp,
            session_id=session_id,
            source_host=source_host,
        )

        reply_info = _label_packet(reply_data)
        print(
            f"Recv: #{reply_id}  {reply_ip}:{reply_port} -> {local_ip}:{local_port}  {len(reply_data)}B  {reply_info or ''}"
        )

        if dump:
            print(_hexdump(reply_data))

        if reply_id:
            tagged_packet_ids.append(reply_id)
    except socket.timeout:
        print("  (no unicast reply)")

    send_socket.close()

    if tagged_packet_ids:
        marker_id = store.add_marker(
            session_id=session_id,
            marker_type="evidence",
            label=normalized_label,
            note=note or f"provenance send: {info or label}",
            data={
                "packet_ids": tagged_packet_ids,
                "device_ip": device_ip,
                "port": port,
                "payload_size": len(payload),
            },
        )
        print(f"\nEvidence marker #{marker_id}: {normalized_label} ({len(tagged_packet_ids)} packets)")


@app.command("replay")
def provenance_replay(
    bundle: str = typer.Argument(..., help="Path to provenance bundle (.tar.gz or directory)."),
    device_ip: Optional[str] = typer.Option(None, "--device-ip", help="Override target device IP."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show what would be sent without sending."),
    timeout: float = typer.Option(2.0, "--timeout", help="Response timeout per packet in seconds."),
    session_name: Optional[str] = typer.Option(None, "--session-name", help="Override replay session name."),
    db: Optional[str] = typer.Option(None, "--db", help="SQLite database path."),
    config: Optional[str] = typer.Option(None, "--config", help="Capture config TOML path."),
    profile: Optional[str] = typer.Option(None, "--profile", help="Capture config profile name."),
):
    from netaudio.dante.fact_store import _load_bundle as lib_load_bundle

    bundle_path = _resolve_provenance_bundle_path(bundle)
    if not bundle_path.exists():
        print(f"Bundle not found: {bundle}", file=sys.stderr)
        raise typer.Exit(1)

    manifest, files = lib_load_bundle(bundle_path)
    if not manifest:
        print(f"Empty or invalid bundle: {bundle_path}", file=sys.stderr)
        raise typer.Exit(1)

    samples = manifest.get("samples", [])
    requests = [s for s in samples if s.get("direction") == "request" and not s.get("evidence", False)]
    responses_by_opcode_seq = {}
    request_idx = 0
    for sample in samples:
        if sample.get("evidence", False):
            continue
        if sample.get("direction") == "request":
            request_idx += 1
        elif sample.get("direction") == "response":
            responses_by_opcode_seq[request_idx] = sample

    if not requests:
        print("No request packets found in bundle.", file=sys.stderr)
        raise typer.Exit(1)

    original_name = manifest.get("session", {}).get("name", bundle_path.stem)
    replay_name = session_name or f"replay_{original_name}"

    original_device_ip = manifest.get("device", {}).get("ip")
    for sample in samples:
        if sample.get("device_ip"):
            original_device_ip = sample["device_ip"]
            break

    target_ip = device_ip or original_device_ip
    if not target_ip:
        print("Cannot determine target device IP. Use --device-ip.", file=sys.stderr)
        raise typer.Exit(1)

    print(f"Bundle: {bundle_path.name}")
    print(f"  Original session: {original_name}")
    print(f"  Target device: {target_ip}")
    print(f"  Requests to replay: {len(requests)}")
    print(f"  Original responses available: {len(responses_by_opcode_seq)}")
    print()

    request_response_pairs = []
    req_counter = 0
    for sample in samples:
        if sample.get("evidence", False):
            continue
        if sample.get("direction") == "request":
            req_counter += 1
            payload = files.get(sample["file"])
            if payload is None:
                continue
            original_response_sample = responses_by_opcode_seq.get(req_counter)
            original_response = None
            if original_response_sample:
                original_response = files.get(original_response_sample["file"])
            port = sample.get("dst_port") or 4440
            request_response_pairs.append(
                {
                    "sample": sample,
                    "payload": payload,
                    "port": port,
                    "original_response": original_response,
                    "original_response_sample": original_response_sample,
                }
            )

    if dry_run:
        for idx, pair in enumerate(request_response_pairs, 1):
            sample = pair["sample"]
            opcode_hex = sample.get("opcode_hex", f"0x{sample.get('opcode', 0):04X}")
            print(
                f"  [{idx}/{len(request_response_pairs)}] {opcode_hex} -> {target_ip}:{pair['port']}  {len(pair['payload'])}B"
            )
            for line in _compact_hexdump(pair["payload"], max_lines=4):
                print(line)
            if pair["original_response"]:
                print(f"    expected response: {len(pair['original_response'])}B")
        print(f"\nDry run: {len(request_response_pairs)} packets would be sent.")
        return

    asyncio.run(
        _run_replay(
            request_response_pairs=request_response_pairs,
            target_ip=target_ip,
            replay_name=replay_name,
            original_name=original_name,
            bundle_path=bundle_path,
            timeout=timeout,
            config=config,
            profile=profile,
            db_override=db,
        )
    )


async def _run_replay(
    request_response_pairs: list[dict],
    target_ip: str,
    replay_name: str,
    original_name: str,
    bundle_path: Path,
    timeout: float,
    config: str | None,
    profile: str | None,
    db_override: str | None,
):
    from netaudio.dante.protocol_verifier import ProtocolVerifier

    async with ProtocolVerifier(
        device_ip=target_ip,
        session_name=replay_name,
        config=config,
        profile=profile,
        db=db_override,
    ) as verifier:
        verifier.marker(
            "replay_started",
            marker_type="system",
            note=f"Replaying {len(request_response_pairs)} requests from {bundle_path.name}",
            data={"source_bundle": str(bundle_path), "original_session": original_name},
        )

        results = []
        total = len(request_response_pairs)

        for idx, pair in enumerate(request_response_pairs, 1):
            sample = pair["sample"]
            opcode = sample.get("opcode", 0)
            opcode_hex = sample.get("opcode_hex", f"0x{opcode:04X}")
            port = pair["port"]
            payload = pair["payload"]

            if len(payload) >= 6:
                original_txn = struct.unpack(">H", payload[4:6])[0]
                new_txn = (original_txn + 0x8000 + idx) & 0xFFFF
                payload = payload[:4] + struct.pack(">H", new_txn) + payload[6:]

            label = f"replay_{opcode_hex}_{idx}"
            print(f"  [{idx}/{total}] {opcode_hex} -> {target_ip}:{port}  {len(payload)}B  ", end="", flush=True)

            response = await verifier.send(payload, port=port, timeout=timeout, label=label)

            original_response = pair["original_response"]

            if response is None:
                print("TIMEOUT")
                results.append({"opcode_hex": opcode_hex, "status": "timeout", "idx": idx})
                verifier.observation(
                    f"replay_{opcode_hex}_{idx}_timeout",
                    note=f"No response for {opcode_hex}",
                )
                continue

            response_status = struct.unpack(">H", response[8:10])[0] if len(response) >= 10 else 0

            if original_response is None:
                print(f"{len(response)}B  status=0x{response_status:04X}  (no original to compare)")
                results.append({"opcode_hex": opcode_hex, "status": "ok_no_baseline", "idx": idx})
                continue

            size_match = len(response) == len(original_response)

            orig_comparable = original_response
            resp_comparable = response
            if len(orig_comparable) >= 6 and len(resp_comparable) >= 6:
                orig_comparable = orig_comparable[:4] + b"\x00\x00" + orig_comparable[6:]
                resp_comparable = resp_comparable[:4] + b"\x00\x00" + resp_comparable[6:]

            bytes_match = resp_comparable == orig_comparable

            if bytes_match:
                print(f"{len(response)}B  MATCH")
                results.append({"opcode_hex": opcode_hex, "status": "match", "idx": idx})
            elif size_match:
                diff_count = sum(1 for a, b in zip(resp_comparable, orig_comparable) if a != b)
                print(f"{len(response)}B  DIFF ({diff_count} bytes differ)")
                results.append({"opcode_hex": opcode_hex, "status": "diff", "idx": idx, "diff_bytes": diff_count})
                verifier.observation(
                    f"replay_{opcode_hex}_{idx}_diff",
                    note=f"{opcode_hex} response differs: {diff_count} bytes changed, same size ({len(response)}B)",
                    data={"diff_bytes": diff_count, "response_len": len(response)},
                )
            else:
                print(f"{len(response)}B  SIZE_DIFF (expected {len(original_response)}B)")
                results.append(
                    {
                        "opcode_hex": opcode_hex,
                        "status": "size_diff",
                        "idx": idx,
                        "got_len": len(response),
                        "expected_len": len(original_response),
                    }
                )
                verifier.observation(
                    f"replay_{opcode_hex}_{idx}_size_diff",
                    note=f"{opcode_hex} response size differs: got {len(response)}B, expected {len(original_response)}B",
                )

        print()
        match_count = sum(1 for r in results if r["status"] == "match")
        diff_count = sum(1 for r in results if r["status"] in ("diff", "size_diff"))
        timeout_count = sum(1 for r in results if r["status"] == "timeout")
        no_baseline = sum(1 for r in results if r["status"] == "ok_no_baseline")

        summary_parts = []
        if match_count:
            summary_parts.append(f"{match_count} matched")
        if diff_count:
            summary_parts.append(f"{diff_count} differed")
        if timeout_count:
            summary_parts.append(f"{timeout_count} timed out")
        if no_baseline:
            summary_parts.append(f"{no_baseline} no baseline")

        summary = ", ".join(summary_parts)
        print(f"Replay complete: {total} packets — {summary}")

        verifier.marker(
            "replay_finished",
            marker_type="system",
            note=f"Replay complete: {summary}",
            data={"results": results},
        )
