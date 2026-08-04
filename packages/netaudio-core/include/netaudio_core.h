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
} NetaudioStatus;

typedef struct NetaudioClient NetaudioClient;

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus

uint32_t netaudio_abi_version(void);

const char *netaudio_service_arc(void);

const char *netaudio_status_name(int32_t status);

uintptr_t netaudio_lock_nonce_length(void);

uintptr_t netaudio_lock_key_length(void);

NetaudioStatus netaudio_build_set_device_name(const char *name,
                                              uint16_t transaction_id,
                                              uint8_t *out_buffer,
                                              uintptr_t out_capacity,
                                              uintptr_t *out_length);

NetaudioStatus netaudio_build_command(const char *json,
                                      uint8_t *out_buffer,
                                      uintptr_t out_capacity,
                                      uintptr_t *out_length);

NetaudioStatus netaudio_client_new(const char *device_ip,
                                   uint16_t arc_port,
                                   uint32_t timeout_milliseconds,
                                   uint32_t attempts,
                                   NetaudioClient **out_client);

NetaudioStatus netaudio_client_set_device_name(NetaudioClient *client, const char *name);

NetaudioStatus netaudio_client_get_channel_count(NetaudioClient *client,
                                                 uint16_t *out_tx_count,
                                                 uint16_t *out_rx_count,
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

NetaudioStatus netaudio_parse_response(const char *kind,
                                       const uint8_t *data,
                                       uintptr_t data_len,
                                       uint8_t *out_buffer,
                                       uintptr_t out_capacity,
                                       uintptr_t *out_length);

NetaudioStatus netaudio_client_execute(NetaudioClient *client,
                                       const char *json,
                                       uint8_t *out_buffer,
                                       uintptr_t out_capacity,
                                       uintptr_t *out_length);

NetaudioStatus netaudio_lock_token(const char *pin,
                                   const uint8_t *nonce,
                                   uintptr_t nonce_len,
                                   const uint8_t *key,
                                   uintptr_t key_len,
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

NetaudioStatus netaudio_host_mac(uint8_t *out_mac);

void netaudio_client_free(NetaudioClient *client);

#ifdef __cplusplus
}  // extern "C"
#endif  // __cplusplus

#endif  /* NETAUDIO_CORE_H */
