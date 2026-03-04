import logging
import socket
import sys
import traceback
import warnings

from netaudio_lib.dante.const import (
    BLUETOOTH_MODEL_IDS,
    DEVICE_CONTROL_PORT,
    DEVICE_SETTINGS_PORT,
    FEATURE_METERING_UNSUPPORTED,
    PORTS,
    SERVICE_ARC,
    SERVICE_CHAN,
)

logger = logging.getLogger("netaudio")
sockets = {}


class DanteDeviceNetwork:
    def __init__(self, device):
        warnings.warn(
            "DanteDeviceNetwork is deprecated. Use DanteApplication with service classes instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        self.device = device

    async def get_controls(self):
        try:
            from netaudio_lib.common.app_config import settings as app_settings

            source_ip = app_settings.interface_ip or ""

            if source_ip:
                print(
                    f"Using interface IP {source_ip} for device {self.device.server_name} connections",
                    file=sys.stderr,
                )

            for _, service in self.device.services.items():
                if service["type"] == SERVICE_CHAN:
                    continue

                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                sock.bind((source_ip, 0))
                sock.settimeout(0.3)
                sock.connect((str(self.device.ipv4), service["port"]))
                self.device.sockets[service["port"]] = sock

            for port in PORTS:
                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                sock.bind((source_ip, 0))
                sock.settimeout(0.01)
                sock.connect((str(self.device.ipv4), port))
                self.device.sockets[port] = sock
        except Exception as e:
            self.device.error = e
            print(e)
            traceback.print_exc()

        try:
            if not self.device.name:
                device_name_args = self.device.commands.command_device_name()
                response = await self.device.dante_command(
                    *device_name_args, logical_command_name="get_device_name"
                )

                if response:
                    self.device.name = response[10:-1].decode("ascii")
                else:
                    logger.warning("Failed to get Dante device name")

            if self.device.rx_count is None or self.device.tx_count is None:
                channel_count_args = self.device.commands.command_channel_count()
                channel_count_response = await self.device.dante_command(
                    *channel_count_args, logical_command_name="get_channel_count"
                )
                if channel_count_response:
                    self.device.rx_count_raw = self.device.rx_count = int.from_bytes(
                        channel_count_response[15:16], "big"
                    )
                    self.device.tx_count_raw = self.device.tx_count = int.from_bytes(
                        channel_count_response[13:14], "big"
                    )
                else:
                    logger.debug("Failed to get Dante channel counts")

            if self.device.aes67_enabled is None:
                try:
                    aes67_config_args = self.device.commands.command_get_aes67_config()
                    aes67_config_response = await self.device.dante_command(
                        *aes67_config_args, logical_command_name="get_aes67_config"
                    )
                    if aes67_config_response:
                        if b'\x63\x00\x03' in aes67_config_response:
                            self.device.aes67_enabled = True
                        elif b'\x63\x00\x01' in aes67_config_response:
                            self.device.aes67_enabled = False
                        else:
                            logger.debug(
                                f"Unknown AES67 status pattern in response"
                            )
                    else:
                        logger.debug("Failed to get AES67 configuration")
                except Exception as e:
                    logger.debug(f"Error getting AES67 configuration: {e}")

            if not self.device.tx_channels and self.device.tx_count:
                await self.device.get_tx_channels()

            if not self.device.rx_channels and self.device.rx_count:
                await self.device.get_rx_channels()

            if self.device.model_id in BLUETOOTH_MODEL_IDS:
                try:
                    await self.device.get_bluetooth_status()
                except Exception as e:
                    logger.debug(f"Error getting bluetooth status: {e}")

            self.device.error = None
        except Exception as e:
            self.device.error = e
            print(e)
            traceback.print_exc()

    async def get_volume(self, ipv4, mac, port):
        try:
            if self.device.software or (
                self.device.model_id in FEATURE_METERING_UNSUPPORTED
            ):
                return

            if port in sockets:
                sock = sockets[port]
            else:
                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                sock.settimeout(0.1)
                sock.bind((str(ipv4), port))
                sockets[port] = sock

            metering_start_args = self.device.commands.command_metering_start(
                self.device.name, ipv4, mac, port
            )
            metering_start_response = await self.device.dante_command(
                *metering_start_args, logical_command_name="metering_start"
            )

            if metering_start_response[15:16] == b"\xff":
                logger.debug(
                    f"Metering command is unsupported on {self.device.name}"
                )

                return

            while True:
                try:
                    data, addr = sock.recvfrom(2048)

                    if addr[0] == str(self.device.ipv4):
                        await self.device.dante_send_command(
                            *self.device.commands.command_metering_stop(
                                self.device.name, ipv4, mac, port
                            )
                        )
                        self.device.parse_volume(data)

                    break
                except socket.timeout:
                    break
                except Exception as e:
                    print(e)
                    traceback.print_exc()
                    break

        except Exception as e:
            traceback.print_exc()
            print(e)

    def get_service(self, service_type):
        service = None

        try:
            service_item = next(
                filter(
                    lambda x: x
                    and x[1]
                    and "type" in x[1]
                    and x[1]["type"] == service_type,
                    self.device.services.items(),
                )
            )

            service = service_item[1]

        except StopIteration:
            logger.debug(
                f"Failed to get a service by type '{service_type}' for device '{self.device.server_name}'. Services map: {self.device.services if self.device.services else 'is empty/None'}"
            )

            self.device.error = LookupError(f"Service type {service_type} not found")
            service = None
        except Exception as e:
            logger.debug(
                f"Error during get_service for type '{service_type}' on device '{self.device.server_name}': {e}. Services map: {self.device.services if self.device.services else 'is empty/None'}"
            )

            self.device.error = e
            service = None

        return service
