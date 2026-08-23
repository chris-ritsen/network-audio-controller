from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

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
    from netaudio.dante.fact_store import list_facts, get_categories, get_confidence

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
                    from netaudio.dante.packet_dissector import dissect_and_render
                    from netaudio.common.app_config import settings as app_settings

                    color = not app_settings.no_color
                    rendered = dissect_and_render(payload, indent="        ", color=color)
                    source_endpoint = f"{sample.get('src_ip', '')}:{sample.get('src_port', '')}"
                    destination_endpoint = f"{sample.get('dst_ip', '')}:{sample.get('dst_port', '')}"
                    direction = sample.get("direction", "")
                    header = f"{source_endpoint} {'->' if direction == 'request' else '<-' if direction == 'response' else '**'} {destination_endpoint}  {len(payload)}B"
                    return f"{header}\n{rendered}"
                except Exception:
                    logger.exception("Failed to dissect provenance packet %s", packet_id)
                    return f"{len(payload)}B (dissection failed)"

    return None


def _spec_to_plain(spec_data: dict, terminal_width: int = 120, facts_path: Path | None = None) -> str:
    import shutil
    import textwrap
    from netaudio._common import ansi

    terminal_width = shutil.get_terminal_size((120, 24)).columns

    lines = []
    lines.append(ansi("1", "Dante Control Protocol Reference"))
    lines.append("")

    total = spec_data["total"]
    confidence = spec_data["confidence"]
    summary_parts = [f"{ansi('1', str(count))} {level}" for level, count in confidence.items()]
    lines.append(f"{total} documented protocol elements across {len(spec_data['categories'])} categories")
    lines.append("  " + "  ".join(summary_parts))
    lines.append("")

    _CONFIDENCE_COLORS = {
        "verified": "32",
        "observed": "33",
        "inferred": "90",
        "uncertain": "91",
    }

    for cat_index, cat_data in enumerate(spec_data["categories"]):
        if cat_index > 0:
            lines.append("")
        lines.append(ansi("1;4", cat_data["title"]))
        lines.append("")

        for fact_index, fact in enumerate(cat_data["facts"]):
            if fact_index > 0:
                lines.append("")

            confidence_val = fact["confidence"]
            confidence_color = _CONFIDENCE_COLORS.get(confidence_val, "")
            confidence_tag = ""
            if confidence_val in ("inferred", "observed", "uncertain"):
                confidence_tag = f"  {ansi(confidence_color, confidence_val)}"

            lines.append(f"  {ansi('1', fact['key'])}  {fact['name']}{confidence_tag}")

            if fact.get("note"):
                lines.append("")
                note_wrapped = textwrap.fill(
                    fact["note"],
                    width=terminal_width,
                    initial_indent="    ",
                    subsequent_indent="    ",
                )
                lines.append(ansi("90", note_wrapped))

            fields = fact.get("fields", [])
            if fields and not fact.get("body"):
                lines.append("")
                max_name_len = max(len(field["name"]) for field in fields)
                max_type_len = max(len(field["dtype"]) for field in fields)
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

            if fact.get("body"):
                lines.append("")
                body_lines = fact["body"].splitlines()
                for body_line in body_lines:
                    if body_line.startswith("#") or body_line.startswith("|"):
                        continue
                    stripped = body_line.strip()
                    if stripped:
                        wrapped = textwrap.fill(
                            stripped,
                            width=terminal_width,
                            initial_indent="    ",
                            subsequent_indent="    ",
                        )
                        lines.append(ansi("90", wrapped))

            evidence = fact.get("evidence", [])
            if evidence and facts_path is not None:
                from netaudio.cli import state as cli_state

                full_dissect = cli_state.dissect

                lines.append("")
                lines.append(f"    {ansi('33', f'Evidence ({len(evidence)} packets):')}")
                for evidence_ref in evidence:
                    if isinstance(evidence_ref, str) and ":" in evidence_ref:
                        session_ref, packet_id_str = evidence_ref.rsplit(":", 1)
                        try:
                            packet_id = int(packet_id_str)
                        except ValueError:
                            lines.append(f"      {ansi('90', evidence_ref)}")
                            continue

                        dissection = _prove_cache.get(evidence_ref)
                        if dissection is None:
                            dissection = _dissect_evidence_packet(facts_path.parent, session_ref, packet_id)
                            _prove_cache[evidence_ref] = dissection

                        if dissection:
                            header_line = dissection.split("\n")[0] if dissection else ""
                            lines.append(
                                f"      {ansi('33', f'#{packet_id}')} {ansi('90', f'({session_ref})')} {header_line}"
                            )
                            if full_dissect:
                                for dissect_line in dissection.split("\n")[1:]:
                                    lines.append(f"      {dissect_line}")
                        else:
                            lines.append(f"      {ansi('33', f'#{packet_id}')} {ansi('90', f'({session_ref})')}")
                    elif isinstance(evidence_ref, str):
                        lines.append(f"      {ansi('90', evidence_ref)}")

        lines.append("")

    return "\n".join(lines)
