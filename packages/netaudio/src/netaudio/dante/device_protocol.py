import asyncio
import datetime
import logging
import os
import socket
import traceback
import warnings

from netaudio.dante.debug_formatter import format_request, format_response

logger = logging.getLogger("netaudio")


class DanteDeviceProtocol:
    def __init__(self, dump_payloads=False, debug=False, packet_store=None):
        warnings.warn(
            "DanteDeviceProtocol is deprecated. Use DanteApplication with service classes instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        self._dump_payloads = dump_payloads
        self._debug = debug
        self._packet_store = packet_store

    async def dante_command(
        self,
        command,
        sock,
        device_name="unknown_device",
        device_ipv4=None,
        logical_command_name: str = "unknown",
    ):
        if not sock:
            return None

        if self._debug:
            format_request(command, device_name, logical_command_name)

        if self._dump_payloads:
            payload_dir = os.path.join(os.getcwd(), "netaudio_device_payloads")
            try:
                if not os.path.exists(payload_dir):
                    os.makedirs(payload_dir)
            except OSError as e:
                logger.error(f"Error creating payload directory {payload_dir}: {e}")

            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S_%f")
            device_identifier = (
                device_name
                if device_name and device_name != "unknown_device"
                else str(device_ipv4) if device_ipv4 else "unknown_device"
            )
            safe_device_identifier = "".join(
                c if c.isalnum() or c in ("_", "-") else "_" for c in device_identifier
            )
            safe_logical_command_name = "".join(
                c if c.isalnum() or c in ("_", "-") else "_" for c in logical_command_name
            )
            filename_prefix = os.path.join(
                payload_dir,
                f"{timestamp}_{safe_device_identifier}_{safe_logical_command_name}",
            )

            request_filename = f"{filename_prefix}_request.bin"
            try:
                with open(request_filename, "wb") as f:
                    f.write(command)
            except IOError as e:
                logger.error(f"Error writing request payload {request_filename}: {e}")

        response_data = None

        def blocking_send_recv():
            sock.send(command)
            return sock.recvfrom(2048)[0]

        try:
            response_data = await asyncio.to_thread(blocking_send_recv)
        except TimeoutError:
            logger.debug(
                f"Timeout receiving response for {logical_command_name} on {device_name}"
            )
        except socket.error as e:
            logger.error(f"Socket error on {device_name}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error on {device_name}: {e}")
            traceback.print_exc()

        if self._dump_payloads and response_data is not None:
            response_filename = f"{filename_prefix}_response.bin"
            try:
                with open(response_filename, "wb") as f:
                    f.write(response_data)
            except IOError as e:
                logger.error(f"Error writing response payload {response_filename}: {e}")

        if self._packet_store:
            device_ip = str(device_ipv4) if device_ipv4 else None
            try:
                self._packet_store.store_packet(
                    payload=command,
                    source_type="netaudio_request",
                    device_name=device_name,
                    device_ip=device_ip,
                    direction="request",
                )
                if response_data is not None:
                    self._packet_store.store_packet(
                        payload=response_data,
                        source_type="netaudio_response",
                        device_name=device_name,
                        device_ip=device_ip,
                        direction="response",
                    )
            except Exception as e:
                logger.debug(f"PacketStore error: {e}")

        if self._debug and response_data is not None:
            format_response(response_data, device_name, logical_command_name)

        return response_data

    async def dante_send_command(self, command, sock):
        if not sock:
            return

        try:
            await asyncio.to_thread(sock.send, command)
        except Exception as e:
            print(e)
            traceback.print_exc()

    def dante_command_new(self, command, control):
        return control.sendMessage(command)
