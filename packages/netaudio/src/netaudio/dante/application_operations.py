from __future__ import annotations

import asyncio
import logging
import time

from netaudio.dante.capability_partition import (
    CapabilityPartitionExport,
    parse_capability_partition_export,
)
from netaudio.dante.conmon_export import ConmonExport, ConmonExportUnavailableError
from netaudio.dante.const import BLUETOOTH_MODEL_IDS, DEVICE_SETTINGS_PORT
from netaudio.dante.diagnostic_logs import (
    DeviceLogExport,
    apply_device_audio_capabilities,
    parse_device_log_export,
)
from netaudio.dante.gain import SUPPORTED_GAIN_LEVELS
from netaudio.dante.lock_status import LockStatusObservation
from netaudio.dante.link_status import LinkStatusObservation
from netaudio.dante.services.notification import (
    mutate_and_wait_for_capability_value,
    send_and_wait_for_gain_status,
)


logger = logging.getLogger("netaudio")


class DanteApplicationOperations:
    async def probe_link_status(
        self,
        device_ip_address: str,
        timeout: float = 2.0,
    ) -> LinkStatusObservation | None:
        async with self._capability_probe_lock("link_status", device_ip_address):
            waiter = self.notifications.register_link_status_waiter(device_ip_address)
            try:
                self.settings.probe_link_status(device_ip_address)
                await asyncio.wait_for(waiter.wait(), timeout=timeout)
                return self.notifications.get_link_status_result(device_ip_address)
            except asyncio.TimeoutError:
                logger.debug(f"Link status probe timeout for {device_ip_address}")
                return self.notifications.get_link_status_result(device_ip_address)
            finally:
                self.notifications.unregister_link_status_waiter(device_ip_address)

    async def probe_switch_configuration(
        self,
        device_ip_address: str,
        timeout: float = 2.0,
    ) -> dict | None:
        async with self._capability_probe_lock("switch_configuration", device_ip_address):
            waiter = self.notifications.register_switch_configuration_waiter(device_ip_address)
            try:
                self.settings.probe_switch_configuration(device_ip_address)
                await asyncio.wait_for(waiter.wait(), timeout=timeout)
                return self.notifications.get_switch_configuration_result(device_ip_address)
            except asyncio.TimeoutError:
                logger.debug(f"Switch configuration probe timeout for {device_ip_address}")
                return self.notifications.get_switch_configuration_result(device_ip_address)
            finally:
                self.notifications.unregister_switch_configuration_waiter(device_ip_address)

    async def probe_interface_status(self, device_ip: str, timeout: float = 2.0) -> list[dict] | None:
        waiter = self.notifications.register_interface_waiter(device_ip)
        try:
            self.settings.probe_interface_status(device_ip)
            await asyncio.wait_for(waiter.wait(), timeout=timeout)
            return self.notifications.get_interface_result(device_ip)
        except asyncio.TimeoutError:
            logger.debug(f"Interface status probe timeout for {device_ip}")
            return self.notifications.get_interface_result(device_ip)
        finally:
            self.notifications.unregister_interface_waiter(device_ip)

    async def probe_clear_configuration_status(
        self,
        device_ip_address: str,
        timeout: float = 2.0,
    ) -> dict | None:
        async with self._capability_probe_lock("clear_configuration_status", device_ip_address):
            waiter = self.notifications.register_clear_configuration_status_waiter(device_ip_address)
            try:
                self.settings.probe_clear_configuration_status(device_ip_address)
                await asyncio.wait_for(waiter.wait(), timeout=timeout)
                return self.notifications.get_clear_configuration_status_result(device_ip_address)
            except asyncio.TimeoutError:
                logger.debug(f"Clear-configuration status probe timeout for {device_ip_address}")
                return self.notifications.get_clear_configuration_status_result(device_ip_address)
            finally:
                self.notifications.unregister_clear_configuration_status_waiter(device_ip_address)

    async def probe_lock_status(
        self,
        device_ip_address: str,
        timeout: float = 2.0,
    ) -> LockStatusObservation | None:
        async with self._capability_probe_lock("lock_status", device_ip_address):
            waiter = self.notifications.register_lock_status_waiter(device_ip_address)
            try:
                self.settings.probe_lock_reset_status(device_ip_address)
                await asyncio.wait_for(waiter.wait(), timeout=timeout)
                return self.notifications.get_lock_status_result(device_ip_address)
            except asyncio.TimeoutError:
                logger.debug(f"Lock status probe timeout for {device_ip_address}")
                return None
            finally:
                self.notifications.unregister_lock_status_waiter(device_ip_address)

    async def clear_configuration(
        self,
        device_ip_address: str,
        preserve_internet_protocol_settings: bool,
        timeout: float = 2.0,
    ) -> dict:
        from netaudio.dante.services.notification import mutate_and_wait_for_clear_configuration_status

        expected_action_result_code = 2 if preserve_internet_protocol_settings else 1
        command = (
            self.settings.clear_all_configuration_preserving_internet_protocol_settings
            if preserve_internet_protocol_settings
            else self.settings.clear_all_configuration
        )

        async def mutate() -> None:
            command(device_ip_address)

        async with self._capability_probe_lock("clear_configuration_action", device_ip_address):
            status = await mutate_and_wait_for_clear_configuration_status(
                self.notifications,
                device_ip_address,
                expected_action_result_code,
                mutate,
                timeout,
            )
        if status is None:
            raise RuntimeError(f"clear-configuration status timed out for {device_ip_address}")
        if status["action_result_code"] != expected_action_result_code:
            raise RuntimeError(
                f"clear-configuration returned result {status['action_result_code']} "
                f"instead of {expected_action_result_code} for {device_ip_address}"
            )
        return status

    async def export_device_logs(
        self,
        device_ip_address: str,
        timeout: float = 15.0,
    ) -> DeviceLogExport:
        async def request() -> None:
            self.settings.request_device_log_export(device_ip_address)

        try:
            export = await self._export_conmon_data(
                device_ip_address,
                b"LOGS",
                1,
                request,
                timeout,
                "device log export",
            )
        except ConmonExportUnavailableError:
            device = self._device_by_ip(device_ip_address)
            if device is not None:
                device.diagnostic_log_export_supported = False
            raise
        result = parse_device_log_export(export)
        device = self._device_by_ip(device_ip_address)
        if device is not None:
            apply_device_audio_capabilities(device, result.audio_capabilities)
        return result

    async def export_capability_partition(
        self,
        device_ip_address: str,
        timeout: float = 15.0,
    ) -> CapabilityPartitionExport:
        async def request() -> None:
            self.settings.request_capability_partition_export(device_ip_address)

        export = await self._export_conmon_data(
            device_ip_address,
            b"CAP1",
            2,
            request,
            timeout,
            "CAP1 partition export",
        )
        return parse_capability_partition_export(export)

    async def _export_conmon_data(
        self,
        device_ip_address: str,
        expected_echoed_tag: bytes,
        expected_selector_value: int,
        request,
        timeout: float,
        operation_name: str,
    ) -> ConmonExport:
        from netaudio.dante.services.notification import request_and_wait_for_conmon_export

        async with self._capability_probe_lock("conmon_export", device_ip_address):
            result = await request_and_wait_for_conmon_export(
                self.notifications,
                device_ip_address,
                expected_echoed_tag,
                expected_selector_value,
                request,
                timeout,
            )
        if result is None:
            raise RuntimeError(f"{operation_name} timed out for {device_ip_address}")
        return result

    async def probe_sample_rate_status(
        self, device_ip_address: str, timeout: float = 2.0
    ) -> tuple[int, list[int]] | None:
        return await self._probe_capability_status(
            "sample_rate",
            device_ip_address,
            self.notifications.register_sample_rate_waiter,
            self.settings.probe_sample_rate,
            self.notifications.get_sample_rate_result,
            self.notifications.unregister_sample_rate_waiter,
            "Sample rate",
            timeout,
        )

    async def set_sample_rate_state(
        self,
        device,
        sample_rate_hertz: int,
        confirm_destructive: bool = False,
        timeout: float = 4.0,
    ):
        from netaudio.dante.sample_rate_topology import change_sample_rate_topology_safe

        device_ip_address = str(device.ipv4)

        async def probe():
            return await self.probe_sample_rate_status(device_ip_address, timeout=timeout)

        async def mutate() -> None:
            await device.operations._request_sample_rate_change(sample_rate_hertz)

        async with device.topology_mutation_lock:
            return await change_sample_rate_topology_safe(
                device,
                sample_rate_hertz,
                probe,
                mutate,
                confirm_destructive=confirm_destructive,
            )

    async def probe_encoding_status(self, device_ip_address: str, timeout: float = 2.0) -> tuple[int, list[int]] | None:
        return await self._probe_capability_status(
            "encoding",
            device_ip_address,
            self.notifications.register_encoding_waiter,
            self.settings.probe_encoding,
            self.notifications.get_encoding_result,
            self.notifications.unregister_encoding_waiter,
            "Encoding",
            timeout,
        )

    async def probe_sample_rate_pullup_status(
        self,
        device_ip_address: str,
        timeout: float = 2.0,
    ) -> tuple[int, list[int]] | None:
        return await self._probe_capability_status(
            "sample_rate_pullup",
            device_ip_address,
            self.notifications.register_sample_rate_pullup_waiter,
            self.settings.probe_sample_rate_pullup,
            self.notifications.get_sample_rate_pullup_result,
            self.notifications.unregister_sample_rate_pullup_waiter,
            "Sample rate pull-up",
            timeout,
        )

    async def set_sample_rate_pullup_state(
        self,
        device,
        raw_value: int,
        timeout: float = 4.0,
    ) -> tuple[int, list[int]] | None:
        if isinstance(raw_value, bool) or not isinstance(raw_value, int) or not 0 <= raw_value <= 0xFFFFFFFF:
            raise ValueError("raw_value must be an integer from 0 through 4294967295")
        supported_raw_values = device.supported_sample_rate_pullup_raw_values
        if supported_raw_values is not None and raw_value not in supported_raw_values:
            raise ValueError(
                f"requested sample rate pull-up value {raw_value} is not supported; "
                f"device reports {supported_raw_values}"
            )
        device_ip_address = str(device.ipv4)

        async def mutate() -> None:
            self.settings.set_sample_rate_pullup(device_ip_address, raw_value)

        result = await mutate_and_wait_for_capability_value(
            self.notifications,
            "sample_rate_pullup_raw_value",
            device_ip_address,
            raw_value,
            mutate,
            lambda: self.probe_sample_rate_pullup_status(device_ip_address, timeout=timeout),
            timeout,
        )
        if result is not None:
            current_raw_value, observed_supported_raw_values = result
            self._apply_sample_rate_pullup_capability(
                device,
                current_raw_value,
                observed_supported_raw_values,
            )
        return result

    async def probe_gain_status(
        self,
        device_ip_address: str,
        timeout: float = 2.0,
    ) -> tuple[str, list[int]] | None:
        async with self._capability_probe_lock("gain", device_ip_address):
            result = await send_and_wait_for_gain_status(
                self.notifications,
                device_ip_address,
                lambda: self.settings.probe_gain_level(device_ip_address),
                timeout,
            )
            if result is None:
                logger.debug(f"Gain status probe timeout for {device_ip_address}")
            return result

    async def set_gain_level_state(
        self,
        device,
        channel_number: int,
        gain_level: int,
        device_type: str,
        timeout: float = 4.0,
    ) -> tuple[str, list[int]] | None:
        if device_type not in ("input", "output"):
            raise ValueError("device_type must be 'input' or 'output'")
        if isinstance(channel_number, bool) or not isinstance(channel_number, int) or not 1 <= channel_number <= 0xFFFF:
            raise ValueError("channel_number must be an integer from 1 through 65535")
        if isinstance(gain_level, bool) or not isinstance(gain_level, int) or gain_level not in SUPPORTED_GAIN_LEVELS:
            raise ValueError("gain_level must be an integer from 1 through 5")
        if device.gain_device_type is not None and device.gain_device_type != device_type:
            raise ValueError(f"device reports {device.gain_device_type} gain controls, not {device_type}")
        if device.supported_gain_levels is not None and gain_level not in device.supported_gain_levels:
            raise ValueError(
                f"requested gain level {gain_level} is not supported; device reports {device.supported_gain_levels}"
            )

        device_ip_address = str(device.ipv4)
        async with self._capability_probe_lock("gain", device_ip_address):
            result = await send_and_wait_for_gain_status(
                self.notifications,
                device_ip_address,
                lambda: self.settings.set_gain_level(
                    device_ip_address,
                    channel_number,
                    gain_level,
                    device_type,
                ),
                timeout,
                expected_device_type=device_type,
                channel_number=channel_number,
                expected_level=gain_level,
            )
            if result is not None:
                observed_device_type, channel_levels = result
                self._apply_gain_capability(device, observed_device_type, channel_levels)
            return result

    async def _probe_capability_status(
        self,
        capability_name: str,
        device_ip_address: str,
        register_waiter,
        send_probe,
        get_result,
        unregister_waiter,
        capability_description: str,
        timeout: float,
    ) -> tuple[int, list[int]] | None:
        async with self._capability_probe_lock(capability_name, device_ip_address):
            waiter = register_waiter(device_ip_address)
            try:
                event_loop = asyncio.get_running_loop()
                deadline = event_loop.time() + timeout
                attempt_count = 3
                for attempt_number in range(attempt_count):
                    send_probe(device_ip_address)
                    remaining_time = max(0.0, deadline - event_loop.time())
                    if remaining_time == 0:
                        break
                    if attempt_number == attempt_count - 1:
                        attempt_timeout = remaining_time
                    else:
                        attempt_timeout = min(remaining_time, timeout / 4)
                    try:
                        await asyncio.wait_for(waiter.wait(), timeout=attempt_timeout)
                    except asyncio.TimeoutError:
                        continue
                    result = get_result(device_ip_address)
                    if result is not None:
                        return result
                    waiter.clear()
                logger.debug(f"{capability_description} probe timeout for {device_ip_address}")
                return get_result(device_ip_address)
            finally:
                unregister_waiter(device_ip_address)

    async def set_interface_dhcp(self, device_ip: str, timeout: float = 2.0) -> list[dict] | None:
        waiter = self.notifications.register_interface_waiter(device_ip)
        try:
            self.settings.set_interface_dhcp(device_ip)
            await asyncio.wait_for(waiter.wait(), timeout=timeout)
            return self.notifications.get_interface_result(device_ip)
        except asyncio.TimeoutError:
            logger.debug(f"Set interface DHCP timeout for {device_ip}")
            return self.notifications.get_interface_result(device_ip)
        finally:
            self.notifications.unregister_interface_waiter(device_ip)

    async def set_interface_static(
        self, device_ip: str, ip_address: str, netmask: str, dns_server: str, gateway: str, timeout: float = 2.0
    ) -> list[dict] | None:
        waiter = self.notifications.register_interface_waiter(device_ip)
        try:
            self.settings.set_interface_static(device_ip, ip_address, netmask, dns_server, gateway)
            await asyncio.wait_for(waiter.wait(), timeout=timeout)
            return self.notifications.get_interface_result(device_ip)
        except asyncio.TimeoutError:
            logger.debug(f"Set interface static timeout for {device_ip}")
            return self.notifications.get_interface_result(device_ip)
        finally:
            self.notifications.unregister_interface_waiter(device_ip)

    async def set_preferred_leader_state(
        self,
        device_ip: str,
        is_preferred: bool,
        timeout: float = 2.0,
    ) -> bool | None:
        waiter = self.notifications.register_preferred_leader_waiter(device_ip)
        try:
            await self.settings.set_preferred_leader(device_ip, is_preferred)
            self.settings.probe_preferred_leader(device_ip)
            try:
                await asyncio.wait_for(waiter.wait(), timeout=timeout)
            except asyncio.TimeoutError:
                logger.debug(f"Preferred leader write verification timeout for {device_ip}")
            return self.notifications.get_preferred_leader_result(device_ip)
        finally:
            self.notifications.unregister_preferred_leader_waiter(device_ip)

    async def probe_clocking_status(self, device, timeout: float = 3.0) -> dict | None:
        device_ip_address = str(device.ipv4)
        waiter = self.notifications.register_preferred_leader_waiter(device_ip_address)
        try:
            self.settings.refresh_clock_status(device_ip_address)
            try:
                await asyncio.wait_for(waiter.wait(), timeout=timeout)
            except asyncio.TimeoutError:
                logger.debug(f"Clock status probe timeout for {device_ip_address}")
        finally:
            self.notifications.unregister_preferred_leader_waiter(device_ip_address)
        if device.clock_source_code is None:
            return None
        return {
            "clock_source_code": device.clock_source_code,
            "clock_subdomain": device.clock_subdomain,
            "preferred_leader": device.preferred_leader,
            "clock_role": device.clock_role,
            "clock_identity": device.clock_identity,
            "leader_clock_identity": device.leader_clock_identity,
            "clock_port_state_code": device.clock_port_state_code,
            "clock_port_records": device.clock_port_records,
            "clock_frequency_offset_parts_per_billion": device.clock_frequency_offset_parts_per_billion,
        }

    async def set_clock_source_state(self, device, clock_source: int, timeout: float = 4.0) -> int | None:
        if isinstance(clock_source, bool) or not isinstance(clock_source, int) or not 0 <= clock_source <= 0xFFFF:
            raise ValueError("clock_source must be an integer from 0 through 65535")
        await self.settings.set_clock_source(str(device.ipv4), clock_source)
        parsed = await self.probe_clocking_status(device, timeout=timeout)
        if parsed is None:
            return None
        return parsed["clock_source_code"]

    async def set_clock_subdomain_state(self, device, subdomain, timeout: float = 4.0) -> bytes | None:
        from netaudio.dante.clock_config import clock_subdomain_bytes

        normalized = clock_subdomain_bytes(subdomain)
        if normalized is None:
            raise ValueError("clock subdomain must be at most 16 bytes")
        await self.settings.set_clock_subdomain(str(device.ipv4), normalized)
        parsed = await self.probe_clocking_status(device, timeout=timeout)
        if parsed is None:
            return None
        clock_subdomain = parsed.get("clock_subdomain")
        return bytes(clock_subdomain) if clock_subdomain is not None else None

    async def set_aes67_state(self, device, is_enabled: bool, timeout: float = 2.0):
        device_ip_address = str(device.ipv4)
        waiter = self.notifications.register_aes67_waiter(device_ip_address)
        try:
            await device.operations.enable_aes67(is_enabled, retries=1)
            self.settings.probe_aes67(device_ip_address)
            try:
                await asyncio.wait_for(waiter.wait(), timeout=timeout)
            except asyncio.TimeoutError:
                logger.debug(f"AES67 write verification timeout for {device_ip_address}")
            return self.notifications.get_aes67_result(device_ip_address)
        finally:
            self.notifications.unregister_aes67_waiter(device_ip_address)

    async def set_aes67_multicast_prefix_state(self, device, prefix: str) -> str | None:
        import ipaddress

        from netaudio.dante.device import device_advertises_aes67_multicast_prefix

        try:
            normalized_prefix = str(ipaddress.IPv4Address(prefix))
        except (ipaddress.AddressValueError, ValueError) as exception:
            raise ValueError("AES67 multicast prefix must be an IPv4 address") from exception
        if not device_advertises_aes67_multicast_prefix(device):
            raise ValueError("device does not advertise an AES67 multicast prefix")
        await device.operations.set_aes67_multicast_prefix(normalized_prefix)
        await device.operations.get_aes67_configured()
        return device.aes67_multicast_prefix

    async def _query_settings_fields(self, devices: dict | None = None) -> None:
        target_devices = self.devices if devices is None else devices
        host_mac = self.cmc.host_media_access_control_address
        tasks = []

        for device in target_devices.values():
            if not device.ipv4:
                continue

            if device.model_id in BLUETOOTH_MODEL_IDS:
                tasks.append(device.get_bluetooth_status(host_mac=host_mac))

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _query_conmon_all(self, timeout: float = 10.0, devices: dict | None = None) -> None:
        target_devices = self.devices if devices is None else devices
        deadline = time.monotonic() + timeout

        incomplete_devices = []

        for device in target_devices.values():
            remaining = deadline - time.monotonic()

            if remaining <= 0:
                logger.debug("Conmon query timeout reached, skipping remaining devices")
                break

            device_ip = str(device.ipv4) if device.ipv4 else None

            if not device_ip or not device.mac_address:
                continue

            waiter = self.notifications.register_conmon_waiter(device_ip)

            try:
                self._send_conmon_query_for_device(device, "make_model")
                self._send_conmon_query_for_device(device, "dante_model")

                per_device_timeout = min(remaining, 1.0)

                try:
                    await asyncio.wait_for(waiter.wait(), timeout=per_device_timeout)
                    logger.debug(f"Conmon responses received for {device.server_name}")
                except asyncio.TimeoutError:
                    logger.debug(f"Conmon query partial/timeout for {device.server_name}")
                    received = self.notifications._conmon_received.get(device_ip, set())

                    if len(received) < 2:
                        incomplete_devices.append(device)
            finally:
                self.notifications.unregister_conmon_waiter(device_ip)

        for retry in range(2):
            if not incomplete_devices:
                break

            remaining = deadline - time.monotonic()

            if remaining <= 0:
                break

            still_incomplete = []

            for device in incomplete_devices:
                remaining = deadline - time.monotonic()

                if remaining <= 0:
                    break

                device_ip = str(device.ipv4)
                needs_make_model = not device.dante_model
                needs_dante_model = not device.dante_model_id
                expected_count = int(needs_make_model) + int(needs_dante_model)

                if expected_count == 0:
                    continue

                waiter = self.notifications.register_conmon_waiter(device_ip, expected_count=expected_count)

                try:
                    if needs_make_model:
                        self._send_conmon_query_for_device(device, "make_model")

                    if needs_dante_model:
                        self._send_conmon_query_for_device(device, "dante_model")

                    per_device_timeout = min(remaining, 2.0)

                    try:
                        await asyncio.wait_for(waiter.wait(), timeout=per_device_timeout)
                        logger.debug(f"Conmon retry {retry + 1} succeeded for {device.server_name}")
                    except asyncio.TimeoutError:
                        logger.debug(f"Conmon retry {retry + 1} timeout for {device.server_name}")

                        if not device.dante_model_id:
                            still_incomplete.append(device)
                finally:
                    self.notifications.unregister_conmon_waiter(device_ip)

            incomplete_devices = still_incomplete

    def _send_conmon_query_for_device(self, device, opcode: str = "make_model") -> None:
        from netaudio.dante.device_commands import DanteDeviceCommands

        if not device.ipv4 or not device.mac_address:
            return

        mac_hex = device.mac_address.replace(":", "").replace("-", "")

        if len(mac_hex) == 16 and mac_hex[6:10].upper() == "FFFE":
            mac_hex = mac_hex[:6] + mac_hex[10:]
        elif len(mac_hex) == 16 and mac_hex.upper().endswith("0000"):
            mac_hex = mac_hex[:12]

        try:
            commands = DanteDeviceCommands()

            if opcode == "make_model":
                packet = commands.command_make_model(mac_hex)
            elif opcode == "dante_model":
                packet = commands.command_dante_model(mac_hex)
            else:
                return

            self.settings.send(packet, str(device.ipv4), DEVICE_SETTINGS_PORT)
        except Exception:
            logger.warning(f"Failed to send conmon {opcode} to {device.server_name}", exc_info=True)

    async def probe_preferred_leader_state(self, device_ip: str, timeout: float = 2.0) -> bool | None:
        waiter = self.notifications.register_preferred_leader_waiter(device_ip)
        try:
            self.settings.probe_preferred_leader(device_ip)
            await asyncio.wait_for(waiter.wait(), timeout=timeout)
            return self.notifications.get_preferred_leader_result(device_ip)
        except asyncio.TimeoutError:
            logger.debug(f"Preferred leader probe timeout for {device_ip}")
            return self.notifications.get_preferred_leader_result(device_ip)
        finally:
            self.notifications.unregister_preferred_leader_waiter(device_ip)

    async def probe_aes67_state(self, device_ip: str, timeout: float = 2.0) -> tuple[bool | None, bool | None] | None:
        waiter = self.notifications.register_aes67_waiter(device_ip)
        try:
            self.settings.probe_aes67(device_ip)
            await asyncio.wait_for(waiter.wait(), timeout=timeout)
            return self.notifications.get_aes67_result(device_ip)
        except asyncio.TimeoutError:
            logger.debug(f"AES67 probe timeout for {device_ip}")
            return self.notifications.get_aes67_result(device_ip)
        finally:
            self.notifications.unregister_aes67_waiter(device_ip)

    def _device_by_ip(self, ip_str: str):
        for device in self.devices.values():
            if device.ipv4 and str(device.ipv4) == ip_str:
                return device
        return None
