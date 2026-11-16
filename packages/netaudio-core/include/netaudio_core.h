#ifndef NETAUDIO_CORE_H
#define NETAUDIO_CORE_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

typedef enum {
  NETAUDIO_STATUS_OK = 0,
  NETAUDIO_STATUS_NULL_POINTER = 1,
  NETAUDIO_STATUS_INVALID_UTF8 = 2,
  NETAUDIO_STATUS_NAME_TOO_LONG = 3,
  NETAUDIO_STATUS_NAME_INVALID_HYPHEN = 4,
  NETAUDIO_STATUS_NAME_INVALID_CHARS = 5,
  NETAUDIO_STATUS_BUFFER_TOO_SMALL = 6,
  NETAUDIO_STATUS_INVALID_ADDRESS = 7,
  NETAUDIO_STATUS_IO_ERROR = 8,
  NETAUDIO_STATUS_TIMEOUT = 9,
  NETAUDIO_STATUS_MALFORMED_RESPONSE = 10,
  NETAUDIO_STATUS_SERIALIZATION_ERROR = 11,
  NETAUDIO_STATUS_SUBSCRIPTION_COUNT = 12,
  NETAUDIO_STATUS_INVALID_JSON = 13,
  NETAUDIO_STATUS_INVALID_MAC = 14,
  NETAUDIO_STATUS_INVALID_IP = 15,
  NETAUDIO_STATUS_INVALID_CHANNEL_TYPE = 16,
  NETAUDIO_STATUS_INVALID_KEY = 17,
  NETAUDIO_STATUS_INVALID_PIN = 18,
  NETAUDIO_STATUS_CRYPTO_ERROR = 19,
  NETAUDIO_STATUS_INVALID_PAGE = 20,
  NETAUDIO_STATUS_INVALID_SUBSCRIPTION_CHANNEL = 21,
  NETAUDIO_STATUS_INVALID_DEVICE_TYPE = 22,
  NETAUDIO_STATUS_PACKET_TOO_LARGE = 23,
  NETAUDIO_STATUS_INVALID_CHANNEL = 24,
  NETAUDIO_STATUS_INVALID_LATENCY = 25,
  NETAUDIO_STATUS_INVALID_SAMPLE_RATE = 26,
  NETAUDIO_STATUS_INVALID_ENCODING = 27,
  NETAUDIO_STATUS_INVALID_GAIN_LEVEL = 28,
  NETAUDIO_STATUS_INVALID_FLOW_SLOT = 29,
  NETAUDIO_STATUS_INVALID_FLOW_PROTOCOL = 30,
  NETAUDIO_STATUS_INVALID_SEQUENCE = 31,
  NETAUDIO_STATUS_UNSUPPORTED_PROTOCOL_OPERATION = 32,
  NETAUDIO_STATUS_INTERNAL_PANIC = 33,
  NETAUDIO_STATUS_UNKNOWN_KIND = 34,
  NETAUDIO_STATUS_INVALID_LENGTH = 35,
  NETAUDIO_STATUS_INVALID_DESTINATION = 36,
  NETAUDIO_STATUS_INVALID_FLOW_IDENTITY = 37,
  NETAUDIO_STATUS_INVALID_RECEIVER_MAPPING = 38,
} NetaudioStatus;

enum NetaudioProtocol
#if defined(__cplusplus) || __STDC_VERSION__ >= 202311L
  : uint16_t
#endif // defined(__cplusplus) || __STDC_VERSION__ >= 202311L
 {
  NETAUDIO_PROTOCOL_DEFAULT_ARC = 10239,
  NETAUDIO_PROTOCOL_ARC2729 = 10025,
  NETAUDIO_PROTOCOL_ARC2801 = 10241,
  NETAUDIO_PROTOCOL_ARC2809 = 10249,
  NETAUDIO_PROTOCOL_ARC280_C = 10252,
  NETAUDIO_PROTOCOL_ARC280_F = 10255,
  NETAUDIO_PROTOCOL_CMC = 4608,
  NETAUDIO_PROTOCOL_SETTINGS = 65535,
};
#ifndef __cplusplus
#if __STDC_VERSION__ >= 202311L
typedef enum NetaudioProtocol NetaudioProtocol;
#else
typedef uint16_t NetaudioProtocol;
#endif // __STDC_VERSION__ >= 202311L
#endif // __cplusplus

enum NetaudioResultCode
#if defined(__cplusplus) || __STDC_VERSION__ >= 202311L
  : uint16_t
#endif // defined(__cplusplus) || __STDC_VERSION__ >= 202311L
 {
  NETAUDIO_RESULT_CODE_REQUEST = 0,
  NETAUDIO_RESULT_CODE_SUCCESS = 1,
  NETAUDIO_RESULT_CODE_ERROR = 34,
  NETAUDIO_RESULT_CODE_FRONTEND_UNAVAILABLE = 48,
  NETAUDIO_RESULT_CODE_MORE_PAGES = 33042,
};
#ifndef __cplusplus
#if __STDC_VERSION__ >= 202311L
typedef enum NetaudioResultCode NetaudioResultCode;
#else
typedef uint16_t NetaudioResultCode;
#endif // __STDC_VERSION__ >= 202311L
#endif // __cplusplus

enum NetaudioPort
#if defined(__cplusplus) || __STDC_VERSION__ >= 202311L
  : uint16_t
#endif // defined(__cplusplus) || __STDC_VERSION__ >= 202311L
 {
  NETAUDIO_PORT_ARC = 4440,
  NETAUDIO_PORT_ARC_SECONDARY = 4455,
  NETAUDIO_PORT_SETTINGS = 8700,
  NETAUDIO_PORT_INFO = 8702,
  NETAUDIO_PORT_HEARTBEAT = 8708,
  NETAUDIO_PORT_CONTROL = 8800,
  NETAUDIO_PORT_CONTROLLER_METERING = 8751,
  NETAUDIO_PORT_MULTICAST_METERING = 8752,
};
#ifndef __cplusplus
#if __STDC_VERSION__ >= 202311L
typedef enum NetaudioPort NetaudioPort;
#else
typedef uint16_t NetaudioPort;
#endif // __STDC_VERSION__ >= 202311L
#endif // __cplusplus

enum NetaudioNotification
#if defined(__cplusplus) || __STDC_VERSION__ >= 202311L
  : uint16_t
#endif // defined(__cplusplus) || __STDC_VERSION__ >= 202311L
 {
  NETAUDIO_NOTIFICATION_TOPOLOGY_CHANGE = 16,
  NETAUDIO_NOTIFICATION_INTERFACE_STATUS = 17,
  NETAUDIO_NOTIFICATION_CLOCKING_STATUS = 32,
  NETAUDIO_NOTIFICATION_VERSIONS_STATUS = 96,
  NETAUDIO_NOTIFICATION_CLEAR_CONFIG_STATUS = 120,
  NETAUDIO_NOTIFICATION_SAMPLE_RATE_STATUS = 128,
  NETAUDIO_NOTIFICATION_ENCODING_STATUS = 130,
  NETAUDIO_NOTIFICATION_SAMPLE_RATE_PULLUP_STATUS = 132,
  NETAUDIO_NOTIFICATION_DEVICE_REBOOT = 146,
  NETAUDIO_NOTIFICATION_MANF_VERSIONS_STATUS = 192,
  NETAUDIO_NOTIFICATION_ROUTING_READY = 256,
  NETAUDIO_NOTIFICATION_TX_CHANNEL_CHANGE = 257,
  NETAUDIO_NOTIFICATION_RX_CHANNEL_CHANGE = 258,
  NETAUDIO_NOTIFICATION_TX_LABEL_CHANGE = 259,
  NETAUDIO_NOTIFICATION_TX_FLOW_CHANGE = 260,
  NETAUDIO_NOTIFICATION_RX_FLOW_CHANGE = 261,
  NETAUDIO_NOTIFICATION_PROPERTY_CHANGE = 262,
  NETAUDIO_NOTIFICATION_ROUTING_DEVICE_CHANGE = 288,
  NETAUDIO_NOTIFICATION_AES67_STATUS = 4103,
  NETAUDIO_NOTIFICATION_CODEC_STATUS = 4107,
  NETAUDIO_NOTIFICATION_SETTINGS_CHANGE = 4110,
};
#ifndef __cplusplus
#if __STDC_VERSION__ >= 202311L
typedef enum NetaudioNotification NetaudioNotification;
#else
typedef uint16_t NetaudioNotification;
#endif // __STDC_VERSION__ >= 202311L
#endif // __cplusplus

typedef struct NamedDomain NamedDomain;

typedef struct NetaudioClient NetaudioClient;

typedef struct NetaudioInventory NetaudioInventory;

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus

/**
 * Decide analog read/write access from advertised support and current transport/lock facts.
 */
NetaudioStatus netaudio_analog_access(const char *json,
                                      uint8_t *out_buffer,
                                      uintptr_t out_capacity,
                                      uintptr_t *out_length);

/**
 * Return analog reference-level labels and channel directions.
 */
NetaudioStatus netaudio_gain_metadata(uint8_t *out_buffer,
                                      uintptr_t out_capacity,
                                      uintptr_t *out_length);

/**
 * Resolve an analog level request against a decoded gain adapter.
 * Input contains adapter, channel, level, and optional direction.
 */
NetaudioStatus netaudio_analog_level_control(const char *json,
                                             uint8_t *out_buffer,
                                             uintptr_t out_capacity,
                                             uintptr_t *out_length);

/**
 * Evaluate operation availability from explicit capability, lock, transport,
 * and permission facts. Unknown support remains distinct from a denied write.
 */
NetaudioStatus netaudio_operation_availability(const char *json,
                                               uint8_t *out_buffer,
                                               uintptr_t out_capacity,
                                               uintptr_t *out_length);

uintptr_t netaudio_lock_key_length(void);

/**
 * Validate a JSON PIN string without truncating embedded null characters.
 */
NetaudioStatus netaudio_validate_lock_pin(const char *json);

NetaudioStatus netaudio_client_execute(NetaudioClient *client,
                                       const char *json,
                                       uint8_t *out_buffer,
                                       uintptr_t out_capacity,
                                       uintptr_t *out_length);

NetaudioStatus netaudio_client_lock(NetaudioClient *client,
                                    const char *pin,
                                    const uint8_t *key,
                                    uintptr_t key_len,
                                    uint8_t *out_buffer,
                                    uintptr_t out_capacity,
                                    uintptr_t *out_length);

NetaudioStatus netaudio_client_unlock(NetaudioClient *client,
                                      const char *pin,
                                      const uint8_t *key,
                                      uintptr_t key_len,
                                      uint8_t *out_buffer,
                                      uintptr_t out_capacity,
                                      uintptr_t *out_length);

NetaudioStatus netaudio_client_request(NetaudioClient *client,
                                       const uint8_t *packet,
                                       uintptr_t packet_len,
                                       uint16_t target_port,
                                       bool expect_response,
                                       uint32_t repeat,
                                       uint64_t interval_ms,
                                       uint8_t *out_buffer,
                                       uintptr_t out_capacity,
                                       uintptr_t *out_length);

NetaudioStatus netaudio_client_clear_wire_captures(NetaudioClient *client);

NetaudioStatus netaudio_client_get_wire_captures_json(NetaudioClient *client,
                                                      uint8_t *out_buffer,
                                                      uintptr_t out_capacity,
                                                      uintptr_t *out_length);

NetaudioStatus netaudio_client_set_host_mac(NetaudioClient *client, const uint8_t *host_mac);

/**
 * Send CMC (control) and settings commands to these ports instead of the well-known 8800 and 8700.
 */
NetaudioStatus netaudio_client_set_service_ports(NetaudioClient *client,
                                                 uint16_t control_port,
                                                 uint16_t settings_port);

NetaudioStatus netaudio_host_mac(uint8_t *out_mac);

void netaudio_client_free(NetaudioClient *client);

/**
 * Create an IPv4 control client. A null local_ip resolves and pins the source
 * selected by the OS for device_ip; a non-null local_ip must identify a
 * specific local unicast IPv4 address.
 */
NetaudioStatus netaudio_client_new(const char *device_ip,
                                   const char *local_ip,
                                   uint16_t arc_port,
                                   uint32_t timeout_milliseconds,
                                   uint32_t attempts,
                                   NetaudioClient **out_client);

NetaudioStatus netaudio_client_set_device_name(NetaudioClient *client, const char *name);

NetaudioStatus netaudio_client_get_channel_count(NetaudioClient *client,
                                                 uint16_t *out_tx_count,
                                                 uint16_t *out_rx_count,
                                                 uint16_t *out_transmit_flow_authoring_capability_word,
                                                 int32_t *out_locked);

NetaudioStatus netaudio_client_get_rx_channels_json(NetaudioClient *client,
                                                    uint8_t *out_buffer,
                                                    uintptr_t out_capacity,
                                                    uintptr_t *out_length);

NetaudioStatus netaudio_client_get_rx_inventory_json(NetaudioClient *client,
                                                     uint16_t rx_count,
                                                     uint8_t *out_buffer,
                                                     uintptr_t out_capacity,
                                                     uintptr_t *out_length);

NetaudioStatus netaudio_client_get_tx_channels_json(NetaudioClient *client,
                                                    uint8_t *out_buffer,
                                                    uintptr_t out_capacity,
                                                    uintptr_t *out_length);

NetaudioStatus netaudio_client_get_device_name_json(NetaudioClient *client,
                                                    uint8_t *out_buffer,
                                                    uintptr_t out_capacity,
                                                    uintptr_t *out_length);

NetaudioStatus netaudio_client_get_device_info_json(NetaudioClient *client,
                                                    uint8_t *out_buffer,
                                                    uintptr_t out_capacity,
                                                    uintptr_t *out_length);

NetaudioStatus netaudio_client_get_device_settings_json(NetaudioClient *client,
                                                        uint8_t *out_buffer,
                                                        uintptr_t out_capacity,
                                                        uintptr_t *out_length);

NetaudioStatus netaudio_client_get_property_directory_json(NetaudioClient *client,
                                                           uint8_t *out_buffer,
                                                           uintptr_t out_capacity,
                                                           uintptr_t *out_length);

NetaudioStatus netaudio_client_get_aes67_configured(NetaudioClient *client, int32_t *out_state);

NetaudioStatus netaudio_parse_page(const char *kind,
                                   const uint8_t *data,
                                   uintptr_t data_len,
                                   uint16_t starting_channel,
                                   uint8_t *out_buffer,
                                   uintptr_t out_capacity,
                                   uintptr_t *out_length);

/**
 * Present a validated subdomain without exposing unrecognized bytes as a name.
 */
NetaudioStatus netaudio_clock_subdomain_presentation(const char *json,
                                                     uint8_t *out_buffer,
                                                     uintptr_t out_capacity,
                                                     uintptr_t *out_length);

/**
 * Derive allowed clock controls using the command encoder's capability rules.
 */
NetaudioStatus netaudio_clock_control_availability(const char *json,
                                                   uint8_t *out_buffer,
                                                   uintptr_t out_capacity,
                                                   uintptr_t *out_length);

/**
 * Resolve current clock-source name and advertised choices from current and supported JSON facts.
 * Unknown sources have no label and are not offered as choices. Internal clock is always a choice.
 */
NetaudioStatus netaudio_clock_sources(const char *json,
                                      uint8_t *out_buffer,
                                      uintptr_t out_capacity,
                                      uintptr_t *out_length);

/**
 * Normalize and validate clock changes; return requested, before, changes, and control as JSON.
 * Input: status, changes, optional control_profile and supported_clock_sources.
 * Null changes are omitted; subdomain accepts a Latin-1 string or
 * byte array and normalizes to 16 bytes with a NUL terminator and zero padding.
 * Pass control to the clock_control command and requested to the readback comparison.
 */
NetaudioStatus netaudio_plan_clock_configuration(const char *json,
                                                 uint8_t *out_buffer,
                                                 uintptr_t out_capacity,
                                                 uintptr_t *out_length);

/**
 * Compare {status, requested} JSON and return a JSON boolean.
 * Requested settings must use normalized names and values; unknown fields are errors.
 */
NetaudioStatus netaudio_clock_configuration_matches(const char *json,
                                                    uint8_t *out_buffer,
                                                    uintptr_t out_capacity,
                                                    uintptr_t *out_length);

/**
 * Validate configuration intent or derive restorable settings from fresh panel observations.
 * No device revision is selected and no commands are sent.
 */
NetaudioStatus netaudio_configuration(const char *json,
                                      uint8_t *out_buffer,
                                      uintptr_t out_capacity,
                                      uintptr_t *out_length);

/**
 * Validate a credential without echoing its contents in error messages.
 */
NetaudioStatus netaudio_dapi_validate_credential(const char *json,
                                                 uint8_t *out_buffer,
                                                 uintptr_t out_capacity,
                                                 uintptr_t *out_length);

/**
 * Advance a transport-independent managed operation. The input state is not mutated,
 * so output buffer sizing and retries do not send or consume protocol messages.
 */
NetaudioStatus netaudio_dapi_advance_session(const char *json,
                                             uint8_t *out_buffer,
                                             uintptr_t out_capacity,
                                             uintptr_t *out_length);

/**
 * Build a managed-control packet and its transport/completion plan from {specification, host_mac, message_id}.
 * The packet is a JSON byte array; host_mac is an optional default, and explicit command identities are preserved.
 */
NetaudioStatus netaudio_build_managed_command(const char *json,
                                              uint8_t *out_buffer,
                                              uintptr_t out_capacity,
                                              uintptr_t *out_length);

NetaudioStatus netaudio_build_command(const char *json,
                                      uint8_t *out_buffer,
                                      uintptr_t out_capacity,
                                      uintptr_t *out_length);

/**
 * Require complete inventory and a supported, representable flow before deletion.
 */
NetaudioStatus netaudio_flow_delete_preflight(const char *json,
                                              uint8_t *out_buffer,
                                              uintptr_t out_capacity,
                                              uintptr_t *out_length);

/**
 * Check fresh transmitter inventory before allocating a flow slot.
 */
NetaudioStatus netaudio_flow_create_preflight(const char *json,
                                              uint8_t *out_buffer,
                                              uintptr_t out_capacity,
                                              uintptr_t *out_length);

/**
 * Select and validate a transmit-flow deletion command without sending it.
 */
NetaudioStatus netaudio_plan_transmit_flow_delete(const char *json,
                                                  uint8_t *out_buffer,
                                                  uintptr_t out_capacity,
                                                  uintptr_t *out_length);

/**
 * Select and validate a transmit-flow creation command without sending it.
 */
NetaudioStatus netaudio_plan_transmit_flow_create(const char *json,
                                                  uint8_t *out_buffer,
                                                  uintptr_t out_capacity,
                                                  uintptr_t *out_length);

NetaudioStatus netaudio_flow_authoring_capabilities(uint16_t capability_word,
                                                    uint8_t *out_buffer,
                                                    uintptr_t out_capacity,
                                                    uintptr_t *out_length);

/**
 * Plan transmitter inventory queries from advertised and observed revisions.
 */
NetaudioStatus netaudio_transmit_flow_inventory_protocols(uint16_t advertised_protocol,
                                                          uint16_t observed_protocol,
                                                          uint8_t *out_buffer,
                                                          uintptr_t out_capacity,
                                                          uintptr_t *out_length);

/**
 * Create an inventory operation for an explicitly selected protocol revision.
 * Kind must be "rx_channels", "tx_channels", "rx_flows", or "tx_flows".
 * The caller owns the returned handle and must free it exactly once.
 */
NetaudioStatus netaudio_inventory_new(const char *kind,
                                      uint16_t protocol_id,
                                      uintptr_t maximum_pages,
                                      NetaudioInventory **out_inventory);

/**
 * Free an inventory handle. No concurrent calls may use the handle.
 * Passing null is allowed.
 */
void netaudio_inventory_free(NetaudioInventory *inventory);

/**
 * Accept one complete wire response. Failure leaves the operation unchanged.
 */
NetaudioStatus netaudio_inventory_accept(NetaudioInventory *inventory,
                                         const uint8_t *data,
                                         uintptr_t data_len);

/**
 * Return JSON with next_command and inventory. Exactly one is non-null.
 * Reading state does not advance pagination, including buffer-size retries.
 */
NetaudioStatus netaudio_inventory_state(NetaudioInventory *inventory,
                                        uint8_t *out_buffer,
                                        uintptr_t out_capacity,
                                        uintptr_t *out_length);

NetaudioStatus netaudio_lock_key_search(const char *json,
                                        uint8_t *out_buffer,
                                        uintptr_t out_capacity,
                                        uintptr_t *out_length);

/**
 * Accept source-specific clock observations and compute bounded statistics.
 */
NetaudioStatus netaudio_clock_observation_update(const char *json,
                                                 uint8_t *out_buffer,
                                                 uintptr_t out_capacity,
                                                 uintptr_t *out_length);

/**
 * Return detailed and signal_presence scales, each indexed by the raw byte value.
 * Each entry contains dbfs (null for special values) and state. Callers must select
 * the scale matching the sample source; heartbeat dBFS values are estimates.
 */
NetaudioStatus netaudio_metering_scale(uint8_t *out_buffer,
                                       uintptr_t out_capacity,
                                       uintptr_t *out_length);

/**
 * Interpret parsed connection-health records and prior stream observations.
 * Returns null for replayed or absent updates. Invalid evidence changes no state.
 */
NetaudioStatus netaudio_connection_health_update(const char *json,
                                                 uint8_t *out_buffer,
                                                 uintptr_t out_capacity,
                                                 uintptr_t *out_length);

/**
 * Validate interface configuration and return normalized configured-state fields.
 * Input is {mode: "dhcp"} or {mode: "static", ip_address, netmask, dns_server?, gateway?}.
 */
NetaudioStatus netaudio_interface_configuration(const char *json,
                                                uint8_t *out_buffer,
                                                uintptr_t out_capacity,
                                                uintptr_t *out_length);

NetaudioStatus netaudio_parse_response(const char *kind,
                                       const uint8_t *data,
                                       uintptr_t data_len,
                                       uint8_t *out_buffer,
                                       uintptr_t out_capacity,
                                       uintptr_t *out_length);

/**
 * Select a supported flow-inventory revision from observed or advertised evidence.
 */
NetaudioStatus netaudio_flow_inventory_protocol(const char *json,
                                                uint8_t *out_buffer,
                                                uintptr_t out_capacity,
                                                uintptr_t *out_length);

/**
 * Normalize an explicitly identified MAC, clock, or managed identity.
 * Returns a JSON string, or null when the supplied value is invalid for its kind.
 */
NetaudioStatus netaudio_device_identity(const char *json,
                                        uint8_t *out_buffer,
                                        uintptr_t out_capacity,
                                        uintptr_t *out_length);

/**
 * Resolve advertised ARC version metadata. Null means unadvertised, not a
 * default revision. Managed control selects its observed transport revision.
 */
NetaudioStatus netaudio_arc_protocol(const char *version,
                                     bool managed,
                                     uint8_t *out_buffer,
                                     uintptr_t out_capacity,
                                     uintptr_t *out_length);

/**
 * Allocate from the library's shared nonzero transaction counter, wrapping after 65535.
 */
uint16_t netaudio_next_message_id(void);

uint32_t netaudio_abi_version(void);

const char *netaudio_status_name(int32_t status);

const char *netaudio_status_description(int32_t status);

const char *netaudio_status_category(int32_t status);

NetaudioStatus netaudio_last_error_message(uint8_t *out_buffer,
                                           uintptr_t out_capacity,
                                           uintptr_t *out_length);

NetaudioStatus netaudio_subscription_status(uint16_t code,
                                            uint16_t receiver_status_code,
                                            bool has_receiver_status,
                                            uint8_t *out_buffer,
                                            uintptr_t out_capacity,
                                            uintptr_t *out_length);

NetaudioStatus netaudio_subscription_classification_for_identifier(const char *identifier,
                                                                   uint8_t *out_buffer,
                                                                   uintptr_t out_capacity,
                                                                   uintptr_t *out_length);

/**
 * Validate {protocol_id, channels, records} and return ordered subscription commands.
 * Channels contain number and media_type_code; records use the subscription command schema.
 * All pages are validated before any commands are returned.
 */
NetaudioStatus netaudio_plan_subscription_commands(const char *json,
                                                   uint8_t *out_buffer,
                                                   uintptr_t out_capacity,
                                                   uintptr_t *out_length);

#ifdef __cplusplus
}  // extern "C"
#endif  // __cplusplus

#endif  /* NETAUDIO_CORE_H */
