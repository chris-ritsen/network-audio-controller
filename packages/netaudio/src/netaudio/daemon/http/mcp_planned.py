from __future__ import annotations

from dataclasses import dataclass

from netaudio.daemon.http.mcp_schema import device_property, object_schema, with_confirmation

PROTOCOL_RESEARCH = "protocol_research"
IMPLEMENTATION = "implementation"

CHANNEL_PROPERTY = {"type": ["integer", "string"], "description": "Channel number or unique label."}
CHANNEL_TYPE_PROPERTY = {"type": "string", "enum": ["rx", "tx"]}


@dataclass(frozen=True)
class PlannedOperation:
    name: str
    description: str
    properties: dict
    required: tuple[str, ...]
    features: tuple[str, ...]
    blocker: str
    blocked_by: str
    read_only: bool = False

    @property
    def input_schema(self) -> dict:
        schema = object_schema(self.properties, list(self.required))
        return schema if self.read_only else with_confirmation(schema)

    def not_implemented(self) -> dict:
        return {
            "error": "not_implemented",
            "operation": self.name,
            "blocker": self.blocker,
            "blocked_by": self.blocked_by,
            "features": list(self.features),
        }


PLANNED_OPERATIONS: tuple[PlannedOperation, ...] = (
    PlannedOperation(
        name="preview_lock",
        description=(
            "Show what locking a device would affect before locking it: connections that would remain and "
            "receivers that would lose audio."
        ),
        properties={"device": device_property()},
        required=("device",),
        features=("NA-IDENT-LOCKPREFLIGHT",),
        blocker=IMPLEMENTATION,
        blocked_by="The read-only residual-connection assessment has not been written.",
        read_only=True,
    ),
    PlannedOperation(
        name="recover_locked_device",
        description="Recover a locked device whose PIN is unknown, using the manufacturer's supported recovery workflow.",
        properties={"device": device_property()},
        required=("device",),
        features=("NA-IDENT-LOCKRECOVERY",),
        blocker=PROTOCOL_RESEARCH,
        blocked_by="The authorized recovery action and the devices it supports are not yet established.",
    ),
    PlannedOperation(
        name="set_channel_mute",
        description="Mute or unmute one transmit or receive channel on a device.",
        properties={
            "device": device_property(),
            "channel_type": CHANNEL_TYPE_PROPERTY,
            "channel": CHANNEL_PROPERTY,
            "muted": {"type": "boolean"},
        },
        required=("device", "channel_type", "channel", "muted"),
        features=("NA-ROUTE-CHANNELMUTE",),
        blocker=PROTOCOL_RESEARCH,
        blocked_by="The channel-addressed mute request layout and its readback are not yet established.",
    ),
    PlannedOperation(
        name="set_transmit_channel_enabled",
        description="Enable or disable one transmit channel on a device.",
        properties={"device": device_property(), "channel": CHANNEL_PROPERTY, "enabled": {"type": "boolean"}},
        required=("device", "channel", "enabled"),
        features=("NA-ROUTE-TXENABLE",),
        blocker=PROTOCOL_RESEARCH,
        blocked_by="The request layout and exact enable/disable effects are not yet established.",
    ),
    PlannedOperation(
        name="set_channel_read_only",
        description="Make a channel or transmit flow read-only so other controllers cannot change it, or writable again.",
        properties={
            "device": device_property(),
            "target_type": {"type": "string", "enum": ["rx_channel", "tx_channel", "tx_flow"]},
            "target": CHANNEL_PROPERTY,
            "read_only": {"type": "boolean"},
        },
        required=("device", "target_type", "target", "read_only"),
        features=("NA-ROUTE-READONLY",),
        blocker=PROTOCOL_RESEARCH,
        blocked_by="The selector layout, response and precedence with domain permissions are not yet established.",
    ),
    PlannedOperation(
        name="get_device_groups",
        description="Channel display groups that devices supply, with the channels in each group.",
        properties={"device": device_property()},
        required=(),
        features=("NA-ROUTE-DEVICEGROUPS",),
        blocker=IMPLEMENTATION,
        blocked_by="Typed device-group decoding has not been written.",
        read_only=True,
    ),
    PlannedOperation(
        name="get_receive_templates",
        description="Receiver flow and channel templates stored on a device.",
        properties={"device": device_property()},
        required=("device",),
        features=("NA-ROUTE-RXTEMPLATES",),
        blocker=PROTOCOL_RESEARCH,
        blocked_by="The template record layouts and query semantics are not yet established.",
        read_only=True,
    ),
    PlannedOperation(
        name="create_unicast_transmit_flow",
        description="Create an explicit unicast transmit flow from a device to one receiver.",
        properties={
            "device": device_property(),
            "receiver": {"type": "string", "description": "Receiving device name."},
            "channels": {"type": "array", "items": CHANNEL_PROPERTY, "minItems": 1},
        },
        required=("device", "receiver", "channels"),
        features=("NA-FLOW-UNICAST",),
        blocker=PROTOCOL_RESEARCH,
        blocked_by="The create/remove request, receiver association and flow lifetime are not yet established.",
    ),
    PlannedOperation(
        name="create_video_transmit_flow",
        description="Create a video transmit flow on a Dante AV device.",
        properties={"device": device_property(), "specification": {"type": "object"}},
        required=("device", "specification"),
        features=("NA-FLOW-VIDEO",),
        blocker=PROTOCOL_RESEARCH,
        blocked_by="The video flow create/delete layout, format constraints and readback are not yet established.",
    ),
    PlannedOperation(
        name="delete_receive_flow",
        description="Delete a receive flow on a device, separately from unsubscribing its channels.",
        properties={"device": device_property(), "flow_id": {"type": "integer", "minimum": 1}},
        required=("device", "flow_id"),
        features=("NA-FLOW-RXDELETE",),
        blocker=PROTOCOL_RESEARCH,
        blocked_by="The receive-flow selector encoding and its effect on subscriptions are not yet established.",
    ),
    PlannedOperation(
        name="set_switch_vlan",
        description="Select one of the switch VLAN configurations a device advertises.",
        properties={"device": device_property(), "configuration": {"type": "string"}},
        required=("device", "configuration"),
        features=("NA-CONFIG-VLAN",),
        blocker=IMPLEMENTATION,
        blocked_by="The variable-mask configuration parser and selection have not been written.",
    ),
    PlannedOperation(
        name="set_audio_sync_delay",
        description="Set a device's additional audio synchronization delay.",
        properties={"device": device_property(), "delay_nanoseconds": {"type": "integer", "minimum": 0}},
        required=("device", "delay_nanoseconds"),
        features=("NA-CONFIG-AUDIOSYNC",),
        blocker=IMPLEMENTATION,
        blocked_by="Typed current and maximum delay handling has not been written.",
    ),
    PlannedOperation(
        name="set_igmp_version",
        description="Set the IGMP version a device uses for multicast membership.",
        properties={"device": device_property(), "version": {"type": "integer"}},
        required=("device", "version"),
        features=("NA-CONFIG-IGMP",),
        blocker=PROTOCOL_RESEARCH,
        blocked_by="The IGMP record fields, accepted values and device applicability are not yet established.",
    ),
    PlannedOperation(
        name="set_rtp_defaults",
        description="Change a device's RTP defaults, such as the default receive latency for RTP streams.",
        properties={"device": device_property(), "changes": {"type": "object", "minProperties": 1}},
        required=("device", "changes"),
        features=("NA-CONFIG-RTPDEFAULTS",),
        blocker=PROTOCOL_RESEARCH,
        blocked_by="Accepted value ranges, permission gates and persistence are not yet established.",
    ),
    PlannedOperation(
        name="set_advertisement_scope",
        description="Set how widely a device or one of its transmitters is advertised on the network.",
        properties={"device": device_property(), "scope": {"type": "string"}, "transmitter": CHANNEL_PROPERTY},
        required=("device", "scope"),
        features=("NA-CONFIG-ADVERTSCOPE",),
        blocker=PROTOCOL_RESEARCH,
        blocked_by="The meaning of each scope value and its write gates are not yet established.",
    ),
    PlannedOperation(
        name="set_network_loopback",
        description="Turn a device's network loopback setting on or off.",
        properties={"device": device_property(), "enabled": {"type": "boolean"}},
        required=("device", "enabled"),
        features=("NA-CONFIG-LOOPBACK",),
        blocker=PROTOCOL_RESEARCH,
        blocked_by="Permission gates, effects and persistence are not yet established.",
    ),
    PlannedOperation(
        name="set_signal_reference",
        description="Set the signal reference level metadata for one transmit channel.",
        properties={"device": device_property(), "channel": CHANNEL_PROPERTY, "reference": {"type": "number"}},
        required=("device", "channel", "reference"),
        features=("NA-CONFIG-SIGNALREFERENCE",),
        blocker=PROTOCOL_RESEARCH,
        blocked_by="The physical scale and accepted write range are not yet established.",
    ),
    PlannedOperation(
        name="get_multicast_bandwidth",
        description="Estimated multicast audio bandwidth per device and flow, compared with what the network can carry.",
        properties={"device": device_property()},
        required=(),
        features=("NA-MON-MULTICASTBW",),
        blocker=IMPLEMENTATION,
        blocked_by="The shared multicast bandwidth estimator has not been written.",
        read_only=True,
    ),
    PlannedOperation(
        name="get_receive_errors",
        description="Receiver error reports for a device: dropped, late and out-of-order packets per flow.",
        properties={"device": device_property()},
        required=("device",),
        features=("NA-MON-RXERRORS",),
        blocker=PROTOCOL_RESEARCH,
        blocked_by="The side effects of the error query and the capability bit meanings are not yet established.",
        read_only=True,
    ),
    PlannedOperation(
        name="get_latency_history",
        description="Receive latency history and distribution per network for a device, against its configured latency.",
        properties={"device": device_property()},
        required=("device",),
        features=("NA-MON-LATENCY", "NA-MON-HISTOGRAM"),
        blocker=IMPLEMENTATION,
        blocked_by="Latency history is collected but not yet presented per device.",
        read_only=True,
    ),
    PlannedOperation(
        name="set_ddm_user_role",
        description="Grant or change a user's role in a Dante Domain Manager domain.",
        properties={
            "server": {"type": "string", "description": "DDM server profile name."},
            "domain": {"type": "string", "description": "Domain name or ID."},
            "user": {"type": "string"},
            "role": {"type": "string"},
        },
        required=("server", "user", "role"),
        features=("NA-MANAGED-ADMIN",),
        blocker=PROTOCOL_RESEARCH,
        blocked_by="Human-role administration and permission mappings are not established by the Managed API.",
    ),
    PlannedOperation(
        name="get_director_sites",
        description="Dante Director accounts and remote sites linked to the configured DDM servers.",
        properties={},
        required=(),
        features=("NA-MANAGED-DIRECTOR",),
        blocker=PROTOCOL_RESEARCH,
        blocked_by="Account enumeration and the Director service contract are not yet established.",
        read_only=True,
    ),
    PlannedOperation(
        name="set_discovery_interfaces",
        description="Choose which network interfaces the server uses to discover and control devices.",
        properties={"interfaces": {"type": "array", "items": {"type": "string"}, "minItems": 1}},
        required=("interfaces",),
        features=("NA-DISC-OVERRIDES",),
        blocker=IMPLEMENTATION,
        blocked_by="Typed interface override policy has not been written.",
    ),
    PlannedOperation(
        name="get_recovery_devices",
        description="Devices advertising safe mode or recovery mode, with why they are in it.",
        properties={},
        required=(),
        features=("NA-DISC-SAFEMODE",),
        blocker=IMPLEMENTATION,
        blocked_by="Dedicated safe-mode service discovery has not been written.",
        read_only=True,
    ),
    PlannedOperation(
        name="export_diagnostic_report",
        description="Collect a diagnostic report for some or all devices at a chosen privacy level.",
        properties={
            "devices": {"type": "array", "items": {"type": "string"}},
            "privacy": {"type": "string"},
        },
        required=(),
        features=("NA-TOOLS-REPORT",),
        blocker=IMPLEMENTATION,
        blocked_by="The CLI creates reports; the daemon has no endpoint for them yet.",
        read_only=True,
    ),
    PlannedOperation(
        name="get_firmware_update_status",
        description="A device's firmware update state and last update error.",
        properties={"device": device_property()},
        required=("device",),
        features=("NA-TOOLS-UPDATE",),
        blocker=IMPLEMENTATION,
        blocked_by="Read-only update-state observation has not been written.",
        read_only=True,
    ),
    PlannedOperation(
        name="update_firmware",
        description="Update a device's firmware from an image file.",
        properties={"device": device_property(), "image": {"type": "string"}},
        required=("device", "image"),
        features=("NA-TOOLS-UPDATE",),
        blocker=PROTOCOL_RESEARCH,
        blocked_by="Image eligibility, transfer and recovery are not yet established.",
    ),
    PlannedOperation(
        name="get_virtual_device_status",
        description="Whether netaudio's virtual Dante device is running, with its name and channels.",
        properties={},
        required=(),
        features=(),
        blocker=IMPLEMENTATION,
        blocked_by="The CLI manages the virtual device; the daemon has no endpoint for it yet.",
        read_only=True,
    ),
    PlannedOperation(
        name="set_virtual_device_running",
        description="Start or stop netaudio's virtual Dante device.",
        properties={"running": {"type": "boolean"}},
        required=("running",),
        features=(),
        blocker=IMPLEMENTATION,
        blocked_by="The CLI manages the virtual device; the daemon has no endpoint for it yet.",
    ),
)

PLANNED_BY_NAME = {operation.name: operation for operation in PLANNED_OPERATIONS}
