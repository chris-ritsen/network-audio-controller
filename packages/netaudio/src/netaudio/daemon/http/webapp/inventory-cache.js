const KEY = "netaudio.inventory.v1";
const FIELDS = [
  "name",
  "server_name",
  "online",
  "ipv4",
  "mac_address",
  "model",
  "product_name",
  "platform_model_name",
  "platform_model_identifier",
  "manufacturer",
  "tx_count",
  "rx_count",
  "channels",
  "subscriptions",
  "sample_rate_hz",
  "encoding",
  "latency_ms",
  "clock_role",
  "preferred_leader",
  "is_locked",
  "product_version",
  "friendly_product_version",
  "platform_software_version",
  "platform_hardware_version",
  "platform_api_version",
  "rom_boot_version",
  "manufacturer_software_version",
  "manufacturer_firmware_version",
  "cmc_server_version",
  "router_protocol_version",
  "ddm_product_version",
  "ddm_product_software_version",
  "ddm_dante_version",
  "ddm_dante_hardware_version",
  "last_seen",
  "kind",
  "inventory_sources",
  "ddm_domain_name",
  "ddm_context",
  "ddm_server_profile",
  "ddm_device_id",
  "ddm_domain_id",
  "management_state",
  "availability_state",
  "control_transports",
  "supported_sample_rates_hz",
  "supported_encodings",
  "gain_device_type",
  "gain_level_choices",
  "supported_sample_rate_pullup_raw_values",
  "sample_rate_pullup_raw_value",
  "active_latency_ms",
  "configured_latency_ms",
  "default_latency_ms",
  "min_latency_ms",
  "max_latency_ms",
  "standard_latency_choices_ms",
  "aes67_configuration_supported",
  "aes67_current",
  "aes67_configured",
  "aes67_multicast_prefix",
  "ptpv1_device_uuid",
  "ptpv1_master_uuid",
  "ptpv1_grandmaster_uuid",
  "clock_status",
  "clock_observed_at",
  "clock_source_code",
  "clock_subdomain",
  "dante_model",
  "interfaces",
  "interface_configuration_modes",
  "link_speed_mbps",
  "dante_redundancy",
  "network_redundancy",
  "interface_reboot_required",
  "ddm_capabilities",
  "ddm_clock_preferences",
  "ddm_clocking_state",
  "ddm_connection_state",
  "ddm_enrolment_state",
  "ddm_status",
  "operation_availability",
];

export function readInventoryCache() {
  try {
    const cached = JSON.parse(window.localStorage.getItem(KEY));
    if (
      !cached ||
      !Number.isFinite(cached.savedAt) ||
      cached.savedAt > Date.now()
    )
      return {};
    if (
      !cached.devices ||
      typeof cached.devices !== "object" ||
      Array.isArray(cached.devices)
    )
      return {};
    if (
      Object.values(cached.devices).some(
        (device) =>
          !device ||
          typeof device !== "object" ||
          Array.isArray(device) ||
          typeof device.server_name !== "string",
      )
    )
      return {};
    return cached.devices;
  } catch {
    return {};
  }
}

export function writeInventoryCache(devices) {
  try {
    const snapshot = Object.fromEntries(
      Object.entries(devices).map(([key, device]) => [
        key,
        Object.fromEntries(
          FIELDS.filter((field) => device[field] !== undefined).map((field) => [
            field,
            device[field],
          ]),
        ),
      ]),
    );
    window.localStorage.setItem(
      KEY,
      JSON.stringify({ savedAt: Date.now(), devices: snapshot }),
    );
  } catch {
    // Storage may be disabled or full; live updates remain available.
  }
}
