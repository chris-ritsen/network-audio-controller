from __future__ import annotations

import logging
import struct
from pathlib import Path
from typing import Optional

from netaudio.core.binding import NetaudioCoreError

logger = logging.getLogger("netaudio")


_CATEGORY_TITLES = {
    "protocol_structure": "Protocol Structure",
    "arc_opcode": "ARC Protocol (Port 4440)",
    "cmc_opcode": "CMC Protocol (Port 8800)",
    "conmon_message": "Conmon Protocol (Port 8700/8702)",
}

_CATEGORY_ORDER = ["protocol_structure", "arc_opcode", "cmc_opcode", "conmon_message"]


def _category_sort_key(category: str) -> tuple[int, str]:
    try:
        return (_CATEGORY_ORDER.index(category), category)
    except ValueError:
        return (len(_CATEGORY_ORDER), category)


def _format_field_table(fields: list[dict]) -> str:
    lines = []
    has_direction = any(field.get("direction") for field in fields)
    if has_direction:
        lines.append("| Direction | Offset | Length | Type | Field | Value |")
        lines.append("|-----------|--------|--------|------|-------|-------|")
    else:
        lines.append("| Offset | Length | Type | Field | Value |")
        lines.append("|--------|--------|------|-------|-------|")
    for field in sorted(fields, key=lambda f: f.get("offset", 0)):
        value = field.get("value", "")
        if has_direction:
            direction = field.get("direction") or "both"
            lines.append(
                f"| {direction} | {field['offset']} | {field['length']} | {field['dtype']} | "
                f"{field['name']} | {value} |"
            )
        else:
            lines.append(f"| {field['offset']} | {field['length']} | {field['dtype']} | {field['name']} | {value} |")
    return "\n".join(lines)


def _spec_overview() -> list[str]:
    """Generate the Overview section with transport fundamentals and constants."""
    lines = []
    lines.append("## Overview")
    lines.append("")
    lines.append(
        "This documents the Dante **control protocol** — device discovery, configuration, "
        "routing, and monitoring. It does not cover the audio transport (RTP/AES67). "
        "All control traffic is **UDP** with **big-endian** (network byte order) encoding. "
        "Strings are **null-terminated ASCII**."
    )
    lines.append("")
    lines.append("### Discovery")
    lines.append("")
    lines.append("Devices are discovered via mDNS (Bonjour). Browse these service types:")
    lines.append("")
    lines.append("| Service | Type |")
    lines.append("|---------|------|")
    lines.append("| ARC (Audio Routing & Control) | `_netaudio-arc._udp.local.` |")
    lines.append("| Channel | `_netaudio-chan._udp.local.` |")
    lines.append("| CMC (Control & Monitoring) | `_netaudio-cmc._udp.local.` |")
    lines.append("| DBC (Device Browsing) | `_netaudio-dbc._udp.local.` |")
    lines.append("")
    lines.append(
        "The mDNS TXT record for `_netaudio-arc` contains the device's ARC port "
        "(usually 4440 but can vary). The resolved IP address is the device's control address."
    )
    lines.append("")
    lines.append("### Ports")
    lines.append("")
    lines.append("| Port | Protocol | Direction | Description |")
    lines.append("|------|----------|-----------|-------------|")
    lines.append("| 4440 | ARC | Unicast | Audio routing, channel queries, subscriptions, device naming, latency |")
    lines.append("| 8700 | Conmon | Unicast | Device settings: sample rate, reboot, identify, gain, encoding |")
    lines.append("| 8702 | Conmon | Multicast | Notifications: device announcements, sample rate, config changes |")
    lines.append("| 8708 | Heartbeat | Multicast | Device heartbeat / presence (not used for control) |")
    lines.append("| 8800 | CMC | Unicast | Control & monitoring: registration, subscription status polling |")
    lines.append("| 8751 | Metering | Multicast | Audio level metering (device-configurable port) |")
    lines.append("")
    lines.append("### Multicast Groups")
    lines.append("")
    lines.append("| Address | Usage |")
    lines.append("|---------|-------|")
    lines.append("| 224.0.0.231 | Control/monitoring notifications (port 8702) |")
    lines.append("| 224.0.0.233 | Device heartbeat (port 8708) |")
    lines.append("")
    lines.append("### Request/Response Pattern")
    lines.append("")
    lines.append(
        "Most ARC and CMC commands follow a request/response pattern over unicast UDP. "
        "The response echoes the request's `transaction_id`. Some Conmon commands "
        "(set_sample_rate, reboot, identify) are **fire-and-forget** — confirmation "
        "arrives as a multicast notification burst on 224.0.0.231:8702."
    )
    lines.append("")
    lines.append("---")
    lines.append("")
    return lines


def _build_spec_data(
    facts_path: Path,
    category_filter: Optional[str] = None,
    include_provenance: bool = False,
) -> dict:
    from netaudio.dante.fact_store import get_categories, get_confidence, list_facts

    categories = get_categories(facts_path)
    if category_filter:
        categories = [category_filter]
    categories = sorted(categories, key=_category_sort_key)

    all_facts = list_facts(facts_path)
    publishable_facts = [f for f in all_facts if get_confidence(f) != "disproved"]

    confidence_counts = {"verified": 0, "observed": 0, "inferred": 0, "uncertain": 0}
    for fact in publishable_facts:
        conf = get_confidence(fact)
        if conf in confidence_counts:
            confidence_counts[conf] += 1

    seen_message_types: set[str] = set()

    spec_categories = []
    for cat in categories:
        category_facts = [f for f in publishable_facts if f["category"] == cat]
        if not category_facts:
            continue

        title = _CATEGORY_TITLES.get(cat, cat.replace("_", " ").title())
        entries = []
        for fact in category_facts:
            dedup_key = f"{fact['name']}:{fact['key']}"
            if dedup_key in seen_message_types:
                continue
            seen_message_types.add(dedup_key)

            confidence = get_confidence(fact)
            entry = {
                "key": fact["key"],
                "name": fact["name"],
                "category": cat,
                "confidence": confidence,
            }
            if fact.get("note"):
                entry["note"] = fact["note"]
            if fact.get("body"):
                entry["body"] = fact["body"]
            fields = fact.get("fields", [])
            if fields:
                entry["fields"] = [
                    {
                        "offset": field["offset"],
                        "length": field["length"],
                        "dtype": field["dtype"],
                        "name": field["name"],
                        "value": field.get("value", ""),
                        **({"direction": field["direction"]} if field.get("direction") else {}),
                    }
                    for field in sorted(fields, key=lambda f: f.get("offset", 0))
                ]

            if include_provenance:
                evidence_list = fact.get("evidence", [])
                if evidence_list:
                    entry["evidence"] = evidence_list

            entries.append(entry)

        spec_categories.append(
            {
                "category": cat,
                "title": title,
                "facts": entries,
            }
        )

    return {
        "title": "Dante Control Protocol Reference",
        "total": sum(confidence_counts.values()),
        "confidence": {k: v for k, v in confidence_counts.items() if v},
        "categories": spec_categories,
    }


def _spec_to_markdown(spec_data: dict) -> str:
    lines = []
    lines.append("# Dante Control Protocol Reference")
    lines.append("")

    total = spec_data["total"]
    confidence = spec_data["confidence"]
    num_categories = len(spec_data["categories"])
    summary_parts = [f"{level}: {count}" for level, count in confidence.items()]
    lines.append(f"**{total} documented protocol elements** across {num_categories} categories.")
    if summary_parts:
        lines.append(" | ".join(summary_parts))
    lines.append("")
    lines.append("---")
    lines.append("")

    lines.extend(_spec_overview())

    for cat_data in spec_data["categories"]:
        lines.append(f"## {cat_data['title']}")
        lines.append("")

        for fact in cat_data["facts"]:
            lines.append(f"### {fact['key']} — {fact['name']}")
            lines.append("")

            if fact["confidence"] in ("inferred", "observed", "uncertain"):
                lines.append(f"*Status: {fact['confidence']}*")
                lines.append("")

            if fact.get("note"):
                lines.append(fact["note"])
                lines.append("")

            if fact.get("body"):
                for body_line in fact["body"].splitlines():
                    if body_line.startswith("#"):
                        body_line = "##" + body_line
                    lines.append(body_line)
                lines.append("")

            fields = fact.get("fields", [])
            if fields and not fact.get("body"):
                lines.append("#### Fields")
                lines.append("")
                lines.append(_format_field_table(fields))
                lines.append("")

    return "\n".join(lines)


_prove_cache: dict[str, str | None] = {}


def _dissect_evidence_packet(provenance_dir, session_ref: str, packet_id: int) -> str | None:
    from netaudio.dante.fact_store import _find_bundle, _load_bundle

    bundle_path = _find_bundle(provenance_dir, session_ref)
    if bundle_path is None:
        return None

    manifest, files = _load_bundle(bundle_path)
    if not manifest:
        return None

    for sample in manifest.get("samples", []):
        if sample.get("packet_id") == packet_id:
            filename = sample.get("file", "")
            payload = files.get(filename)
            if payload:
                try:
                    from netaudio.common.app_config import settings as app_settings
                    from netaudio.dante.dissection.rendering import dissect_and_render

                    color = not app_settings.no_color
                    rendered = dissect_and_render(payload, indent="        ", color=color)
                    source_endpoint = f"{sample.get('src_ip', '')}:{sample.get('src_port', '')}"
                    destination_endpoint = f"{sample.get('dst_ip', '')}:{sample.get('dst_port', '')}"
                    direction = sample.get("direction", "")
                    header = f"{source_endpoint} {'->' if direction == 'request' else '<-' if direction == 'response' else '**'} {destination_endpoint}  {len(payload)}B"
                    return f"{header}\n{rendered}"
                except (LookupError, ValueError, NetaudioCoreError, struct.error):
                    logger.exception("Failed to dissect provenance packet %s", packet_id)
                    return f"{len(payload)}B (dissection failed)"

    return None


_CONFIDENCE_COLORS = {
    "inferred": "90",
    "observed": "33",
    "uncertain": "91",
    "verified": "32",
}


def _spec_wrapped_lines(text: str, terminal_width: int) -> str:
    import textwrap

    return textwrap.fill(text, width=terminal_width, initial_indent="    ", subsequent_indent="    ")


def _spec_field_lines(fields: list[dict]) -> list[str]:
    from netaudio.cli_support.execution import ansi

    max_name_len = max(len(field["name"]) for field in fields)
    max_type_len = max(len(field["dtype"]) for field in fields)
    lines = [""]
    for field in fields:
        value_str = f"  {ansi('36', field['value'])}" if field.get("value") else ""
        offset_str = f"{field['offset']:3d}:{field['offset'] + field['length']:<3d}"
        direction_str = f"[{field['direction']}] " if field.get("direction") else ""
        lines.append(
            f"    {ansi('90', offset_str)}"
            f"  {field['dtype']:<{max_type_len}s}"
            f"  {direction_str}{field['name']:<{max_name_len}s}"
            f"{value_str}"
        )
    return lines


def _spec_body_lines(body: str, terminal_width: int) -> list[str]:
    from netaudio.cli_support.execution import ansi

    lines = [""]
    for body_line in body.splitlines():
        if body_line.startswith("#") or body_line.startswith("|"):
            continue
        stripped = body_line.strip()
        if stripped:
            lines.append(ansi("90", _spec_wrapped_lines(stripped, terminal_width)))
    return lines


def _spec_evidence_reference_lines(evidence_ref: str, facts_path: Path, full_dissect: bool) -> list[str]:
    from netaudio.cli_support.execution import ansi

    if ":" not in evidence_ref:
        return [f"      {ansi('90', evidence_ref)}"]
    session_ref, packet_id_str = evidence_ref.rsplit(":", 1)
    try:
        packet_id = int(packet_id_str)
    except ValueError:
        return [f"      {ansi('90', evidence_ref)}"]

    dissection = _prove_cache.get(evidence_ref)
    if dissection is None:
        dissection = _dissect_evidence_packet(facts_path.parent, session_ref, packet_id)
        _prove_cache[evidence_ref] = dissection

    heading = f"      {ansi('33', f'#{packet_id}')} {ansi('90', f'({session_ref})')}"
    if not dissection:
        return [heading]
    dissection_lines = dissection.split("\n")
    lines = [f"{heading} {dissection_lines[0]}"]
    if full_dissect:
        lines.extend(f"      {dissect_line}" for dissect_line in dissection_lines[1:])
    return lines


def _spec_evidence_lines(evidence: list, facts_path: Path) -> list[str]:
    from netaudio.cli import state as cli_state
    from netaudio.cli_support.execution import ansi

    lines = ["", f"    {ansi('33', f'Evidence ({len(evidence)} packets):')}"]
    for evidence_ref in evidence:
        if isinstance(evidence_ref, str):
            lines.extend(_spec_evidence_reference_lines(evidence_ref, facts_path, cli_state.dissect))
    return lines


def _spec_fact_lines(fact: dict, terminal_width: int, facts_path: Path | None) -> list[str]:
    from netaudio.cli_support.execution import ansi

    confidence_val = fact["confidence"]
    confidence_tag = ""
    if confidence_val in ("inferred", "observed", "uncertain"):
        confidence_tag = f"  {ansi(_CONFIDENCE_COLORS.get(confidence_val, ''), confidence_val)}"
    lines = [f"  {ansi('1', fact['key'])}  {fact['name']}{confidence_tag}"]
    if fact.get("note"):
        lines.extend(["", ansi("90", _spec_wrapped_lines(fact["note"], terminal_width))])
    fields = fact.get("fields", [])
    if fields and not fact.get("body"):
        lines.extend(_spec_field_lines(fields))
    if fact.get("body"):
        lines.extend(_spec_body_lines(fact["body"], terminal_width))
    evidence = fact.get("evidence", [])
    if evidence and facts_path is not None:
        lines.extend(_spec_evidence_lines(evidence, facts_path))
    return lines


def _spec_to_plain(spec_data: dict, terminal_width: int = 120, facts_path: Path | None = None) -> str:
    import shutil

    from netaudio.cli_support.execution import ansi

    terminal_width = shutil.get_terminal_size((120, 24)).columns

    lines = [ansi("1", "Dante Control Protocol Reference"), ""]
    total = spec_data["total"]
    confidence = spec_data["confidence"]
    summary_parts = [f"{ansi('1', str(count))} {level}" for level, count in confidence.items()]
    lines.append(f"{total} documented protocol elements across {len(spec_data['categories'])} categories")
    lines.append("  " + "  ".join(summary_parts))
    lines.append("")

    for cat_index, cat_data in enumerate(spec_data["categories"]):
        if cat_index > 0:
            lines.append("")
        lines.append(ansi("1;4", cat_data["title"]))
        lines.append("")
        for fact_index, fact in enumerate(cat_data["facts"]):
            if fact_index > 0:
                lines.append("")
            lines.extend(_spec_fact_lines(fact, terminal_width, facts_path))
        lines.append("")

    return "\n".join(lines)
