#ifndef NETAUDIO_ENGINE_H
#define NETAUDIO_ENGINE_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

typedef enum {
  NETAUDIO_ENGINE_STATUS_OK = 0,
  NETAUDIO_ENGINE_STATUS_INVALID_ARGUMENT = 1,
  NETAUDIO_ENGINE_STATUS_FAILED = 2,
} NetaudioEngineStatus;

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus

NetaudioEngineStatus netaudio_engine_start(const char *directory,
                                           uint16_t *port,
                                           char *message,
                                           uintptr_t capacity);

void netaudio_engine_stop(void);

bool netaudio_engine_running(void);

void netaudio_engine_network_changed(void);

#ifdef __cplusplus
}  // extern "C"
#endif  // __cplusplus

#endif  /* NETAUDIO_ENGINE_H */
