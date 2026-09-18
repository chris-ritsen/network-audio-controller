from netaudio.dante.network_configuration import network_configuration_modes


DEVICE_SCALAR_FIELDS = (
    "active_latency",
    "aes67_configured",
    "aes67_configured_property_advertised",
    "aes67_current",
    "aes67_multicast_prefix",
    "aes67_configuration_supported",
    "availability_state",
    "bit_depth",
    "device_controls",
    "virtual_panel_supported",
    "video_transmission_supported",
    "video_reception_supported",
    "codec_status",
    "codec_observed_at",
    "bluetooth_connected",
    "bluetooth_device",
    "platform_model_name",
    "platform_model_identifier",
    "platform_versions_record",
    "clear_configuration_status",
    "clock_frequency_offset_parts_per_billion",
    "ptpv1_device_uuid",
    "clock_monitoring_supported",
    "clock_port_records",
    "clock_status",
    "clock_diagnostics",
    "supported_clock_sources",
    "clock_observed_at",
    "ptpv1_grandmaster_uuid",
    "clock_port_state_code",
    "clock_role",
    "clock_source_code",
    "clock_subdomain",
    "configured_latency",
    "control_transports",
    "dante_model",
    "dante_model_record_protocol_version",
    "dante_model_primary_capabilities",
    "dante_model_read_only_capabilities",
    "dante_model_id",
    "dante_model_monitoring_capabilities",
    "dante_model_secondary_capabilities",
    "dante_model_domain_capability_values",
    "dante_model_domain_capability_validity",
    "identify_supported",
    "sample_rate_configuration_supported",
    "encoding_configuration_supported",
    "sample_rate_pullup_configuration_supported",
    "switch_redundancy_supported",
    "redundancy_advertised_support_source",
    "static_ipv4_configuration_supported",
    "device_locking_supported",
    "external_word_clock_read_only",
    "switch_redundancy_read_only",
    "redundancy_read_only_source",
    "static_ipv4_configuration_read_only",
    "generic_codec_control_supported",
    "ddm_capabilities",
    "ddm_clock_preferences",
    "ddm_clocking_state",
    "ddm_connection_last_changed",
    "ddm_connection_state",
    "ddm_context",
    "ddm_device_id",
    "ddm_domain_id",
    "ddm_domain_name",
    "ddm_enrolment_state",
    "ddm_identity",
    "ddm_inputs",
    "ddm_last_sync",
    "ddm_outputs",
    "ddm_parameters",
    "ddm_server_profile",
    "ddm_status",
    "default_latency",
    "detailed_metering_supported",
    "diagnostic_log_export_supported",
    "direct_control_available",
    "encoding",
    "requested_encoding",
    "encoding_update_mode",
    "field_sources",
    "friendly_product_version",
    "platform_software_version",
    "platform_hardware_version",
    "platform_api_version",
    "rom_boot_version",
    "manufacturer_software_version",
    "manufacturer_firmware_version",
    "manufacturer_versions_record",
    "cmc_server_version",
    "router_protocol_version",
    "ddm_product_version",
    "ddm_product_software_version",
    "ddm_dante_version",
    "ddm_dante_hardware_version",
    "gain_device_type",
    "gain_levels",
    "gain_adapter",
    "codec_parameters",
    "interface_status_protocol",
    "interface_statistics_supported",
    "interface_statistics",
    "dante_redundancy",
    "redundancy_probe_outcomes",
    "interfaces",
    "inventory_id",
    "inventory_sources",
    "is_locked",
    "last_seen",
    "latency",
    "ptpv1_master_uuid",
    "license_signature_length_bytes",
    "licensed_receive_channel_count",
    "licensed_redundancy_enabled",
    "licensed_transmit_channel_count",
    "link_speed_mbps",
    "lock_reset_status",
    "mac_address",
    "management_state",
    "managed_operation_permissions",
    "manufacturer",
    "media_types",
    "max_latency",
    "min_latency",
    "model",
    "model_id",
    "network_interface_traffic",
    "num_networks",
    "per_channel_signal_presence_supported",
    "performance_settings",
    "preferred_leader",
    "product_version",
    "product_name",
    "receiver_flow_connection_health",
    "receiver_flow_completeness",
    "receiver_flow_latency_nanoseconds",
    "receiver_flow_status_page",
    "receiver_flows",
    "receiver_flow_inventory_opcode",
    "requested_sample_rate_pullup_raw_value",
    "routing_capacity_receive_channel_count",
    "routing_capacity_transmit_channel_count",
    "routing_ready",
    "routing_ready_state_code",
    "rx_count",
    "rx_count_raw",
    "rx_flow_count",
    "rx_flow_late_packet_monitoring_supported",
    "rx_flow_maximum_latency_monitoring_supported",
    "sample_rate",
    "requested_sample_rate",
    "sample_rate_update_mode",
    "sample_rate_channel_capacities",
    "sample_rate_pullup_raw_value",
    "sample_rate_pullup_update_mode",
    "sample_rate_pullup_flags",
    "settings_properties",
    "supported_encodings",
    "supported_gain_levels",
    "supported_sample_rate_pullup_raw_values",
    "supported_sample_rates",
    "transmitter_flows",
    "transmit_flow_authoring_capability_word",
    "transmit_flow_authoring_opcode",
    "transmit_flow_authoring_protocol_id",
    "tx_count",
    "tx_count_raw",
    "tx_flow_count",
)

CHANNEL_OPTIONAL_FIELDS = (
    "bit_depth",
    "can_subscribe_self",
    "can_subscribe_self_conflict",
    "can_rename",
    "ddm_channel_id",
    "ddm_enabled",
    "ddm_encryption_policy",
    "ddm_encryption_scheme",
    "ddm_media_type",
    "ddm_signal_presence",
    "ddm_status",
    "ddm_status_message",
    "ddm_summary",
    "factory_name",
    "friendly_name",
    "format_descriptor_hexadecimal",
    "media_local_id",
    "media_type",
    "media_type_code",
    "media_service",
    "managed_can_subscribe_self",
    "managed_can_subscribe_self_fresh",
    "muted",
    "receiver_capability_flags",
    "receiver_flags",
    "receiver_status_flags",
    "encoding",
    "sample_rate",
    "samples_per_frame",
    "status_text",
    "volume",
)

SUBSCRIPTION_MANAGED_FIELDS = ("ddm_status", "ddm_status_message", "ddm_summary")

DEVICE_JSON_FIELD_NAMES = {
    "active_latency": "active_latency_ms",
    "configured_latency": "configured_latency_ms",
    "default_latency": "default_latency_ms",
    "latency": "latency_ms",
    "max_latency": "max_latency_ms",
    "min_latency": "min_latency_ms",
    "receiver_flow_latency_nanoseconds": "receiver_flow_latency_ns",
    "sample_rate": "sample_rate_hz",
    "supported_sample_rates": "supported_sample_rates_hz",
}


def device_json_field_name(field_name: str) -> str:
    return DEVICE_JSON_FIELD_NAMES.get(field_name, field_name)


class DanteDeviceSerializer:
    @staticmethod
    def to_json(device):
        from netaudio.dante.self_connection import receiver_self_connection_support

        rx_channels = {
            k: DanteDeviceSerializer.channel_to_json(v)
            for k, v in sorted(device.rx_channels.items(), key=lambda x: x[1].number)
        }
        tx_channels = {
            k: DanteDeviceSerializer.channel_to_json(v)
            for k, v in sorted(device.tx_channels.items(), key=lambda x: x[1].number)
        }

        as_json = {
            "channels": {"receivers": rx_channels, "transmitters": tx_channels},
            "ipv4": str(device.ipv4),
            "kind": device.kind,
            "name": device.name,
            "online": device.online,
            "server_name": device.server_name,
            "services": device.services,
            "self_connection_support": receiver_self_connection_support(device.rx_channels.values()),
            "subscriptions": [DanteDeviceSerializer.subscription_to_json(s) for s in device.subscriptions],
        }

        for field_name in DEVICE_SCALAR_FIELDS:
            field_value = getattr(device, field_name, None)
            if field_value is None and field_name != "is_locked":
                continue
            if isinstance(field_value, (bytes, bytearray)):
                field_value = list(field_value)
            as_json[device_json_field_name(field_name)] = field_value

        from netaudio.dante.panel_state import panel_snapshot

        as_json["device_controls"] = panel_snapshot(device)
        connection = as_json["device_controls"].get("observations", {}).get("bluetooth_connection")
        if connection and not connection["fresh"]:
            as_json["bluetooth_connected"] = None
        if isinstance(as_json.get("clock_status"), dict):
            from netaudio.dante.clock_control import clock_status_fresh

            clock = dict(as_json["clock_status"])
            fresh = clock_status_fresh(as_json)
            clock["observation_state"] = (
                "fresh" if fresh else "stale" if clock.get("status_supported") else "unavailable"
            )
            if not fresh:
                clock["observed_synchronization"] = clock.get("synchronization", "unknown")
                clock["synchronization"] = "unknown"
            as_json["clock_status"] = clock

        if device.is_licensed is not None:
            as_json["is_licensed"] = device.is_licensed

        standard_latency_choices = device.standard_latency_choices
        if standard_latency_choices is not None:
            as_json["standard_latency_choices_ms"] = standard_latency_choices

        encoding_configurable = device.encoding_configurable
        if encoding_configurable is not None:
            as_json["encoding_configurable"] = encoding_configurable

        gain_configurable = device.gain_configurable
        if gain_configurable is not None:
            as_json["gain_configurable"] = gain_configurable

        gain_level_choices = device.gain_level_choices
        if gain_level_choices is not None:
            as_json["gain_level_choices"] = gain_level_choices

        if device.interfaces is not None:
            as_json["interface_configuration_modes"] = network_configuration_modes(device)

        from netaudio.dante.operation_availability import operation_availability_map

        as_json["operation_availability"] = operation_availability_map(device)
        from netaudio.dante.network_configuration import redundancy_snapshot

        as_json["network_redundancy"] = redundancy_snapshot(device)
        from netaudio.dante.performance_configuration import performance_operation_availability

        as_json["performance_operation_availability"] = performance_operation_availability(device)

        if device.interface_reboot_required:
            as_json["interface_reboot_required"] = device.interface_reboot_required

        return {key: as_json[key] for key in sorted(as_json.keys())}

    @staticmethod
    def device_from_json(data):
        from netaudio.dante.device import DanteDevice

        device = DanteDevice(server_name=data.get("server_name", ""))

        if data.get("ipv4") and data["ipv4"] != "None":
            device.ipv4 = data["ipv4"]
        device.name = data.get("name", "")
        device.online = data.get("online", True)
        device.services = data.get("services") or {}

        for field_name in DEVICE_SCALAR_FIELDS:
            json_field_name = device_json_field_name(field_name)
            if json_field_name in data:
                setattr(device, field_name, data[json_field_name])

        if isinstance(device.receiver_flow_status_page, dict):
            device.apply_receiver_flow_status_page(device.receiver_flow_status_page)

        if data.get("interface_reboot_required"):
            device.interface_reboot_required = True

        channels = data.get("channels") or {}
        device.rx_channels = DanteDeviceSerializer._channels_from_json(channels.get("receivers") or {}, "rx", device)
        device.tx_channels = DanteDeviceSerializer._channels_from_json(channels.get("transmitters") or {}, "tx", device)

        device.subscriptions = [
            DanteDeviceSerializer._subscription_from_json(entry) for entry in data.get("subscriptions") or []
        ]
        for subscription in device.subscriptions:
            channels = [
                channel for channel in device.rx_channels.values() if channel.name == subscription.rx_channel_name
            ]
            if len(channels) == 1:
                subscription.rx_channel = channels[0]
                subscription.rx_device = device
                subscription._netaudio_rx_channel_number = channels[0].number
            if subscription.is_self_connection:
                subscription.tx_device = device

        return device

    @staticmethod
    def _channels_from_json(channels_json, channel_type, device):
        from netaudio.dante.channel import DanteChannel

        channels = {}
        for number_key, channel_json in channels_json.items():
            channel = DanteChannel()
            channel.channel_type = channel_type
            channel.device = device
            channel.number = int(number_key)
            channel.name = channel_json.get("name")
            for field_name in CHANNEL_OPTIONAL_FIELDS:
                setattr(channel, field_name, channel_json.get(field_name))
            channels[channel.number] = channel
        return channels

    @staticmethod
    def _subscription_from_json(entry):
        from netaudio.dante.subscription import DanteSubscription

        subscription = DanteSubscription()
        subscription.rx_channel_name = entry.get("rx_channel")
        subscription.rx_device_name = entry.get("rx_device")
        subscription.tx_channel_name = entry.get("tx_channel")
        subscription.tx_device_name = entry.get("tx_device")
        subscription._is_self_connection = entry.get("self_connection") is True
        for field_name in SUBSCRIPTION_MANAGED_FIELDS:
            setattr(subscription, field_name, entry.get(field_name))
        status = entry.get("status")
        if status:
            subscription.status_code = status.get("code")
        rx_channel_status = entry.get("rx_channel_status")
        if rx_channel_status:
            subscription.rx_channel_status_code = rx_channel_status.get("code")
        subscription.status_message = list(entry.get("status_message", []))
        return subscription

    @staticmethod
    def channel_to_json(channel):
        as_json = {"name": channel.name}

        if channel.channel_type == "rx":
            as_json.update(
                {
                    "can_subscribe_self": channel.can_subscribe_self,
                    "receiver_flags": channel.receiver_flags,
                    "receiver_capability_flags": channel.receiver_capability_flags,
                }
            )

        gain_level = None
        gain_level_label = None
        if channel.device is not None:
            gain_level = channel.device.gain_level_for_channel(channel.number, channel.channel_type)
            gain_level_label = channel.device.gain_level_label_for_channel(channel.number, channel.channel_type)

        required_receiver_fields = {"can_subscribe_self", "receiver_flags", "receiver_capability_flags"}
        optional_fields = [
            *(
                (field_name, getattr(channel, field_name))
                for field_name in CHANNEL_OPTIONAL_FIELDS
                if field_name not in required_receiver_fields
            ),
            ("gain_level", gain_level),
            ("gain_level_label", gain_level_label),
        ]

        for field_name, field_value in optional_fields:
            if field_value is not None:
                as_json[field_name] = field_value

        return {key: as_json[key] for key in sorted(as_json.keys())}

    @staticmethod
    def _status_to_json(code, receiver_status_code=None):
        from netaudio.dante.const import subscription_status_entry
        from netaudio.icons import severity_icon

        if code is None:
            return None
        from netaudio.dante.subscription_status import MANAGED_STATUS_PRESENTATION

        entry = subscription_status_entry(code, receiver_status_code)
        entry.pop("labels")
        entry["icon"] = severity_icon(entry["severity"])
        presentation = MANAGED_STATUS_PRESENTATION.get(entry.get("status") or "")
        if presentation is not None:
            label, detail = presentation
            entry["label"] = label
            entry["detail"] = detail or entry.get("detail")
        return entry

    @staticmethod
    def _receiver_status_to_json(code):
        if code is None:
            return None
        return {
            "code": code,
            "detail": None,
            "label": f"Receiver status 0x{code:04X}",
            "state": "uncharacterized",
        }

    @staticmethod
    def subscription_to_json(subscription):
        from netaudio.dante.subscription import managed_subscription_status

        status = DanteDeviceSerializer._status_to_json(subscription.status_code, subscription.rx_channel_status_code)
        if status is None and subscription.ddm_status is not None:
            status = managed_subscription_status(
                subscription.ddm_status, subscription.ddm_status_message, subscription.ddm_summary
            )
        as_json = {
            "rx_channel": subscription.rx_channel_name,
            "rx_device": subscription.rx_device_name,
            "status": status,
            "tx_channel": subscription.tx_channel_name,
            "tx_device": subscription.tx_device_name,
        }
        if subscription.is_self_connection:
            as_json["self_connection"] = True
        for field_name in SUBSCRIPTION_MANAGED_FIELDS:
            field_value = getattr(subscription, field_name)
            if field_value is not None:
                as_json[field_name] = field_value

        if subscription.status_message:
            as_json["status_message"] = list(subscription.status_message)

        if subscription.rx_channel_status_code is not None:
            as_json["rx_channel_status"] = DanteDeviceSerializer._receiver_status_to_json(
                subscription.rx_channel_status_code
            )

        return as_json
