from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ApiFeature:
    name: str
    since: int
    description: str


API_VERSION = 2

API_FEATURES = (
    ApiFeature(
        "events.patches",
        1,
        "GET /events?patches=1 sends device_patch events carrying only the fields that changed.",
    ),
    ApiFeature(
        "events.telemetry_off",
        1,
        "GET /events?telemetry=0 leaves telemetry fields out of device records and sends field-name notices instead.",
    ),
    ApiFeature(
        "events.telemetry_fields",
        2,
        "GET /events?telemetry=name,name sends only the named telemetry fields; the rest arrive as notices.",
    ),
    ApiFeature(
        "events.opt_out",
        2,
        "GET /events?meters=0 leaves out meter_values and shure_meter_values; telemetry=none leaves out telemetry "
        "fields and their notices. The server skips building events no connected client wants.",
    ),
    ApiFeature(
        "events.managed_status",
        2,
        "Dante Domain Manager changes arrive as a managed_status event plus events for the devices that changed, "
        "not as a full snapshot.",
    ),
    ApiFeature(
        "events.discovery_grace",
        2,
        "For 10 seconds after the server starts, devices that Dante Domain Manager sees but that are not enrolled "
        "appear only once direct discovery finds them, under their direct key.",
    ),
    ApiFeature(
        "devices.receiver_health_summary",
        2,
        "receiver_flow_connection_health in device records carries compact path evidence and one source_records copy "
        "of the heartbeat bytes; /diagnostics returns the full record.",
    ),
    ApiFeature(
        "device_controls.analog_level",
        2,
        "POST /device-controls plans and applies the analog_level category, including on managed AVIOs.",
    ),
    ApiFeature(
        "device_controls.bluetooth_name_bytes",
        2,
        "Bluetooth custom names are limited to 32 bytes of UTF-8. A stored name that is not valid UTF-8 is reported "
        "with custom_name_raw_hexadecimal.",
    ),
)


def api_contract() -> dict:
    return {
        "version": API_VERSION,
        "features": {
            feature.name: {"since": feature.since, "description": feature.description} for feature in API_FEATURES
        },
    }
