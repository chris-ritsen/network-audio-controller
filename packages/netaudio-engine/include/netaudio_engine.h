#ifndef NETAUDIO_ENGINE_H
#define NETAUDIO_ENGINE_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

enum NetaudioEngineStatus
#if defined(__cplusplus) || __STDC_VERSION__ >= 202311L
  : int32_t
#endif // defined(__cplusplus) || __STDC_VERSION__ >= 202311L
 {
  NETAUDIO_ENGINE_STATUS_OK = 0,
  NETAUDIO_ENGINE_STATUS_INVALID_ARGUMENT = 1,
  NETAUDIO_ENGINE_STATUS_FAILED = 2,
};
#ifndef __cplusplus
#if __STDC_VERSION__ >= 202311L
typedef enum NetaudioEngineStatus NetaudioEngineStatus;
#else
typedef int32_t NetaudioEngineStatus;
#endif // __STDC_VERSION__ >= 202311L
#endif // __cplusplus

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
