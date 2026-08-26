"""Fail-closed correlation of leased virtual guests with public DDM inventory."""

from __future__ import annotations

import json
from typing import Any

from ._lifecycle_support import LabError


def _normalized_mac(value: Any) -> str:
    return value.lower() if isinstance(value, str) else ""


def matching_inventory_device(
    value: Any,
    mac_address: str,
    ipv4_addresses: set[str] | None = None,
    expected_device_id: str | None = None,
) -> dict[str, Any] | None:
    """Resolve one device, preferring exact MAC and rejecting ambiguous IP reuse."""

    target = mac_address.lower()
    target_addresses = ipv4_addresses or set()
    candidates: dict[str, dict[str, Any]] = {}

    def visit(candidate: Any) -> None:
        if isinstance(candidate, list):
            for nested in candidate:
                visit(nested)
            return
        if not isinstance(candidate, dict):
            return
        interfaces = candidate.get("interfaces")
        if isinstance(interfaces, list):
            exact_mac = any(
                isinstance(interface, dict) and _normalized_mac(interface.get("macAddress")) == target
                for interface in interfaces
            )
            matching_address = any(
                isinstance(interface, dict) and str(interface.get("address", "")) in target_addresses
                for interface in interfaces
            )
            address_mac_conflict = any(
                isinstance(interface, dict)
                and str(interface.get("address", "")) in target_addresses
                and _normalized_mac(interface.get("macAddress")) not in {"", target}
                for interface in interfaces
            )
            if exact_mac or matching_address:
                device_id = candidate.get("id")
                key = (
                    device_id
                    if isinstance(device_id, str) and device_id
                    else json.dumps(
                        candidate,
                        sort_keys=True,
                        default=str,
                    )
                )
                record = candidates.setdefault(
                    key,
                    {
                        "id": device_id,
                        "name": candidate.get("name"),
                        "domainId": candidate.get("domainId"),
                        "enrolmentState": candidate.get("enrolmentState"),
                        "interfaces": [
                            {
                                "macAddress": interface.get("macAddress"),
                                "address": interface.get("address"),
                            }
                            for interface in interfaces
                            if isinstance(interface, dict)
                        ],
                        "connection": candidate.get("connection"),
                        "_matched_mac": False,
                        "_matched_address": False,
                        "_address_mac_conflict": False,
                    },
                )
                record["_matched_mac"] = record["_matched_mac"] or exact_mac
                record["_matched_address"] = record["_matched_address"] or matching_address
                record["_address_mac_conflict"] = record["_address_mac_conflict"] or address_mac_conflict
        for nested in candidate.values():
            visit(nested)

    visit(value)
    mac_matches = [candidate for candidate in candidates.values() if candidate["_matched_mac"]]
    address_matches = [candidate for candidate in candidates.values() if candidate["_matched_address"]]
    if any(candidate["_address_mac_conflict"] for candidate in address_matches):
        raise LabError("DDM IPv4 fallback exposed a MAC that conflicts with the virtual identity")
    if len(mac_matches) > 1:
        raise LabError("DDM inventory matched the virtual MAC to more than one device")
    if mac_matches:
        selected = mac_matches[0]
        if any(candidate.get("id") != selected.get("id") for candidate in address_matches):
            raise LabError("DDM MAC and live IPv4 evidence identify different devices")
    else:
        if len(address_matches) > 1:
            raise LabError("DDM inventory matched the virtual IPv4 address to more than one device")
        selected = address_matches[0] if address_matches else None
    if selected is None or (expected_device_id is not None and selected.get("id") != expected_device_id):
        return None
    return {key: item for key, item in selected.items() if not key.startswith("_")}


__all__ = ["matching_inventory_device"]
