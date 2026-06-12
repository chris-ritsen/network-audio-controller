#include <stdio.h>

#include "netaudio_core.h"

int main(int argc, char **argv) {
    if (argc != 4) {
        fprintf(stderr, "usage: %s <device_ip> <tx_channel_number> <new_name>\n", argv[0]);
        return 2;
    }

    printf("netaudio-core ABI %u\n", netaudio_abi_version());

    NetaudioClient *client = NULL;
    NetaudioStatus status = netaudio_client_new(argv[1], 4440, 1000, 3, &client);
    if (status != NETAUDIO_STATUS_OK) {
        fprintf(stderr, "client_new failed: %d\n", status);
        return 1;
    }

    uint8_t buffer[65536];
    uintptr_t length = 0;

    status = netaudio_client_get_device_name_json(client, buffer, sizeof(buffer), &length);
    if (status == NETAUDIO_STATUS_OK) {
        printf("device name: %.*s\n", (int)length, buffer);
    }

    char spec[512];
    snprintf(spec, sizeof(spec),
             "{\"command\": \"set_channel_name\", \"channel_type\": \"tx\", \"channel_number\": %s, \"name\": \"%s\"}",
             argv[2], argv[3]);

    length = 0;
    status = netaudio_client_execute(client, spec, buffer, sizeof(buffer), &length);
    netaudio_client_free(client);

    if (status != NETAUDIO_STATUS_OK) {
        fprintf(stderr, "set_channel_name failed: %d\n", status);
        return 1;
    }

    printf("renamed tx channel %s to \"%s\"\n", argv[2], argv[3]);
    return 0;
}
