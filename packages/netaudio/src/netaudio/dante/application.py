from __future__ import annotations

import asyncio
import logging

from netaudio.dante.application_operations import DanteApplicationOperations
from netaudio.dante.const import (
    SERVICE_ARC,
    SERVICE_CMC,
    SERVICE_DBC,
    SERVICES,
)
from netaudio.dante.events import DanteEvent, DanteEventDispatcher, EventType
from netaudio.dante.gain import SUPPORTED_GAIN_LEVELS
from netaudio.dante.latency import nanoseconds_to_milliseconds
from netaudio.dante.services.cmc import DanteCMCService
from netaudio.dante.services.notification import (
    DanteNotificationService,
    NOTIFICATION_NAMES,
)
from netaudio.dante.services.settings import DanteSettingsService

logger = logging.getLogger("netaudio")


class DanteApplication(DanteApplicationOperations):
    def __init__(self, packet_store=None, dissect=False):
        self.devices: dict = {}
        self.dispatcher = DanteEventDispatcher()
        self.settings = DanteSettingsService(packet_store=packet_store, dissect=dissect)
        from netaudio.common.app_config import settings as app_settings

        self.cmc = DanteCMCService(packet_store=packet_store, interface_name=app_settings.interface, dissect=dissect)
        self.notifications = DanteNotificationService(
            dispatcher=self.dispatcher,
            device_lookup=self._device_by_ip,
            packet_store=packet_store,
            interface_ip=app_settings.interface_ip,
            dissect=dissect,
        )
        self._browser = None
        self._started = False
        self._notification_handlers: dict[int, list] = {}
        self._packet_store = packet_store
        self._dissect = dissect
        self.capture_session_id: int | None = None
        self.core_observer = self._record_core_traffic if (packet_store is not None or dissect) else None
        self._capture_queue: asyncio.Queue | None = None
        self._capture_loop: asyncio.AbstractEventLoop | None = None
        self._capture_writer_task: asyncio.Task | None = None
        self._capability_probe_locks: dict[tuple[str, str], asyncio.Lock] = {}

    def _capability_probe_lock(self, capability_name: str, device_ip_address: str) -> asyncio.Lock:
        lock_key = (capability_name, device_ip_address)
        lock = self._capability_probe_locks.get(lock_key)
        if lock is None:
            lock = asyncio.Lock()
            self._capability_probe_locks[lock_key] = lock
        return lock

    def _record_core_traffic(self, packet, response, device_ip, port):
        loop = self._capture_loop
        queue = self._capture_queue
        if loop is None or queue is None:
            return
        loop.call_soon_threadsafe(queue.put_nowait, (packet, device_ip, port, "request", "netaudio_request"))
        if response is not None:
            loop.call_soon_threadsafe(queue.put_nowait, (response, device_ip, port, "response", "netaudio_response"))

    async def _capture_writer(self) -> None:
        from netaudio._capture import _dissect, _record

        while True:
            item = await self._capture_queue.get()
            if item is None:
                return
            payload, device_ip, port, direction, source_type = item
            if self._dissect:
                _dissect(payload, device_ip, port, direction)
            if self._packet_store is not None:
                _record(self._packet_store, self.capture_session_id, payload, device_ip, port, direction, source_type)

    def on_notification(self, notification_id: int, callback) -> None:
        if notification_id not in self._notification_handlers:
            self._notification_handlers[notification_id] = []
        self._notification_handlers[notification_id].append(callback)

    async def _dispatch_notification(self, event) -> None:
        notification_id = event.data.get("notification_id")
        if notification_id is None:
            return

        handlers = self._notification_handlers.get(notification_id)
        if handlers:
            for handler in handlers:
                try:
                    await handler(event)
                except Exception:
                    notification_name = NOTIFICATION_NAMES.get(notification_id, f"0x{notification_id:04X}")
                    logger.exception(f"Error in notification handler for {notification_name}")
        else:
            notification_name = event.data.get("notification_name", f"0x{notification_id:04X}")
            logger.debug(f"Unhandled notification: {notification_name} from {event.server_name}")

    async def startup(self) -> None:
        if self._started:
            return

        self._started = True
        if self.core_observer is not None:
            self._capture_loop = asyncio.get_running_loop()
            self._capture_queue = asyncio.Queue()
            self._capture_writer_task = asyncio.create_task(self._capture_writer())

        try:
            self.dispatcher.on(EventType.NOTIFICATION_RECEIVED, self._dispatch_notification)
            await self.dispatcher.start()
            await self.notifications.start()
            await self.settings.start()
            await self.cmc.start()
        except BaseException:
            await self.shutdown()
            raise
        logger.info("DanteApplication started")

    async def shutdown(self) -> None:
        if not self._started:
            return

        await self.notifications.stop()
        await self.cmc.stop()
        await self.settings.stop()
        await self.dispatcher.stop()

        if self._capture_writer_task is not None:
            self._capture_queue.put_nowait(None)
            try:
                await asyncio.wait_for(self._capture_writer_task, timeout=5)
            except asyncio.TimeoutError:
                logger.warning("Capture writer did not drain within 5s")
            self._capture_writer_task = None
            self._capture_queue = None
            self._capture_loop = None

        if self._browser:
            try:
                await self._browser.async_close()
            except Exception:
                logger.exception("Failed to close discovery browser")
            self._browser = None

        self._started = False
        logger.info("DanteApplication shut down")

    async def wait_for_discovery(self, timeout: float = 5.0) -> dict:
        from netaudio.dante.browser import DanteBrowser

        browser = DanteBrowser(mdns_timeout=timeout, app=self)
        self._browser = browser
        try:
            devices = await asyncio.wait_for(browser.get_devices(), timeout=timeout)
        except asyncio.TimeoutError:
            logger.debug(f"mDNS discovery reached its {timeout}s deadline")
            devices = browser.devices
        finally:
            await browser.async_close()
            if self._browser is browser:
                self._browser = None

        if devices:
            self.devices.update(devices)

        return self.devices

    def _apply_discovered_services(self, server_name: str, device_services: dict):
        if server_name in self.devices:
            device = self.devices[server_name]
        else:
            from netaudio.dante.device import DanteDevice

            device = DanteDevice(server_name=server_name, app=self)
            self.register_device(server_name, device)

        device.services = dict(sorted(device_services.items()))
        for service in device_services.values():
            if not device.ipv4:
                device.ipv4 = service["ipv4"]
            service_properties = service.get("properties", {})
            if "id" in service_properties and service["type"] == SERVICE_CMC:
                device.mac_address = service_properties["id"]
            if "model" in service_properties:
                device.model_id = service_properties["model"]
            if "mf" in service_properties:
                device.manufacturer_mdns = service_properties["mf"]
                if not device.manufacturer:
                    device.manufacturer = service_properties["mf"]
            if "server_vers" in service_properties and service["type"] == SERVICE_CMC:
                device.software_version = service_properties["server_vers"]
            if "router_vers" in service_properties:
                device.firmware_version = service_properties["router_vers"]
            if "router_info" in service_properties and service_properties["router_info"] == '"Dante Via"':
                device.software = "Dante Via"
            if "rate" in service_properties:
                device.sample_rate = int(service_properties["rate"])
            if "latency_ns" in service_properties:
                device.latency = nanoseconds_to_milliseconds(service_properties["latency_ns"])
        return device

    async def discover_named_device(self, device_name: str, timeout: float = 2.0) -> dict:
        from zeroconf import ServiceStateChange
        from zeroconf.asyncio import AsyncServiceBrowser, AsyncZeroconf

        from netaudio.dante.browser import DanteBrowser

        browser = DanteBrowser(mdns_timeout=0, app=self)
        self._browser = browser
        browser.aio_zc = AsyncZeroconf(**browser.get_zeroconf_kwargs())
        event_loop = asyncio.get_running_loop()
        deadline = event_loop.time() + timeout
        expected_arc_service_name = f"{device_name}.{SERVICE_ARC}"
        arc_service_future = event_loop.create_future()
        arc_resolution_tasks = set()

        async def resolve_arc_service(zeroconf, service_type, service_name):
            try:
                service = await browser.async_parse_netaudio_service(zeroconf, service_type, service_name)
            except Exception as exception:
                logger.warning(f"Failed to resolve {service_name}: {exception}")
                return
            if service is not None and not arc_service_future.done():
                arc_service_future.set_result(service)

        def handle_arc_service(zeroconf, service_type, name, state_change):
            if state_change is ServiceStateChange.Removed:
                return
            if name.casefold() != expected_arc_service_name.casefold():
                return

            resolution_task = asyncio.create_task(resolve_arc_service(zeroconf, service_type, name))
            arc_resolution_tasks.add(resolution_task)
            resolution_task.add_done_callback(arc_resolution_tasks.discard)

        browser.aio_browser = AsyncServiceBrowser(
            browser.aio_zc.zeroconf,
            [SERVICE_ARC],
            handlers=[handle_arc_service],
        )
        direct_resolution_task = asyncio.create_task(
            resolve_arc_service(
                browser.aio_zc.zeroconf,
                SERVICE_ARC,
                expected_arc_service_name,
            )
        )
        arc_resolution_tasks.add(direct_resolution_task)
        direct_resolution_task.add_done_callback(arc_resolution_tasks.discard)
        optional_service_tasks = {}
        completed_optional_tasks = set()
        try:
            try:
                arc_service = await asyncio.wait_for(arc_service_future, timeout=timeout)
            except asyncio.TimeoutError:
                return {}

            remaining_time = max(0.0, deadline - event_loop.time())
            optional_service_tasks = {
                asyncio.create_task(
                    browser.async_parse_netaudio_service(
                        browser.aio_zc.zeroconf,
                        service_type,
                        f"{device_name}.{service_type}",
                    )
                ): service_type
                for service_type in (SERVICE_CMC, SERVICE_DBC)
            }
            if remaining_time > 0:
                completed_optional_tasks, _ = await asyncio.wait(optional_service_tasks, timeout=remaining_time)
        finally:
            unfinished_tasks = [task for task in (*arc_resolution_tasks, *optional_service_tasks) if not task.done()]
            for task in unfinished_tasks:
                task.cancel()
            if unfinished_tasks:
                await asyncio.gather(*unfinished_tasks, return_exceptions=True)
            await browser.async_close()
            self._browser = None

        services = [arc_service]
        for task in completed_optional_tasks:
            exception = task.exception()
            if exception is not None:
                logger.warning(f"Failed to resolve {device_name} {optional_service_tasks[task]} service: {exception}")
                continue
            service = task.result()
            if service is not None:
                services.append(service)

        server_name = arc_service["server_name"]
        matching_services = {service["name"]: service for service in services if service["server_name"] == server_name}
        device = self._apply_discovered_services(server_name, matching_services)
        return {server_name: device}

    async def discover_and_populate(self, timeout: float = 5.0) -> dict:
        from zeroconf.asyncio import AsyncServiceBrowser, AsyncZeroconf

        from netaudio.dante.browser import DanteBrowser

        discovery_time = min(timeout * 0.4, 2.0)
        populate_time = timeout - discovery_time

        browser = DanteBrowser(mdns_timeout=0, app=self)
        self._browser = browser

        browser.aio_zc = AsyncZeroconf(**browser.get_zeroconf_kwargs())
        browser.aio_browser = AsyncServiceBrowser(
            browser.aio_zc.zeroconf,
            SERVICES,
            handlers=[browser.async_on_service_state_change],
        )

        await asyncio.sleep(discovery_time)

        if browser.services:
            await asyncio.gather(*browser.services, return_exceptions=True)

        browser._assemble_completed_services()
        await browser.async_close()
        self._browser = None

        device_ips = [str(device.ipv4) for device in self.devices.values() if device.ipv4]
        if device_ips:
            await self.cmc.register_all(device_ips)

        populate_tasks = []
        for device in self.devices.values():
            if self.get_arc_port(device):
                populate_tasks.append(self._populate_device_controls(device))

        if populate_tasks:
            done, pending = await asyncio.wait(
                [asyncio.create_task(task) for task in populate_tasks],
                timeout=populate_time,
            )
            for task in pending:
                task.cancel()

        await self._query_settings_fields()

        await self._query_conmon_all()

        await self._probe_interface_status()
        await self._probe_preferred_leader_all()
        await asyncio.gather(
            self._probe_aes67_all(),
            self._probe_sample_rates_all(),
            self._probe_encodings_all(),
            self._probe_gain_levels_all(),
            self._probe_sample_rate_pullups_all(),
        )

        return self.devices

    def register_device(self, server_name: str, device) -> None:
        existing = self.devices.get(server_name)

        if existing is not None:
            if not existing.online:
                existing.online = True
                existing.update_last_seen()
                if device.ipv4:
                    existing.ipv4 = device.ipv4
                if device.services:
                    existing.services = device.services

            self.devices[server_name] = existing
            self.notifications.apply_pending_for_device(existing)
            self.dispatcher.emit_nowait(
                DanteEvent(
                    type=EventType.DEVICE_UPDATED,
                    device_name=existing.name,
                    server_name=server_name,
                )
            )
        else:
            device._app = self
            device.update_last_seen()
            self.devices[server_name] = device
            self.notifications.apply_pending_for_device(device)
            self.dispatcher.emit_nowait(
                DanteEvent(
                    type=EventType.DEVICE_DISCOVERED,
                    device_name=device.name,
                    server_name=server_name,
                )
            )

    def unregister_device(self, server_name: str) -> None:
        device = self.devices.pop(server_name, None)
        if device:
            self.dispatcher.emit_nowait(
                DanteEvent(
                    type=EventType.DEVICE_REMOVED,
                    device_name=device.name,
                    server_name=server_name,
                )
            )

    def mark_device_offline(self, server_name: str) -> None:
        device = self.devices.get(server_name)
        if device and device.online:
            device.online = False
            device.supported_sample_rates = None
            device.supported_encodings = None
            device.aes67_supported = None
            device.aes67_multicast_prefix = None
            device.settings_properties = None
            device.sample_rate_pullup_raw_value = None
            device.requested_sample_rate_pullup_raw_value = None
            device.supported_sample_rate_pullup_raw_values = None
            device.transmitter_flows = None
            device.tx_flow_count = None
            device.receiver_flows = None
            device.rx_flow_count = None
            device.gain_device_type = None
            device.gain_levels = None
            device.supported_gain_levels = None
            self.dispatcher.emit_nowait(
                DanteEvent(
                    type=EventType.DEVICE_REMOVED,
                    device_name=device.name,
                    server_name=server_name,
                )
            )

    def get_arc_port(self, device) -> int | None:
        if not device.services:
            return None

        for service_data in device.services.values():
            if service_data.get("type") == SERVICE_ARC:
                return service_data.get("port")

        return None

    async def populate_controls(self, devices: dict | None = None, include_channels: bool = True) -> None:
        if devices is None:
            devices = self.devices

        tasks = []
        for device in devices.values():
            if self.get_arc_port(device):
                tasks.append(self._populate_device_controls(device, include_channels=include_channels))
            else:
                logger.debug(f"No ARC port for {device.server_name}, skipping controls")

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def populate_device_names(
        self,
        devices: dict,
        request_timeout_milliseconds: int | None = None,
        request_attempts: int | None = None,
    ) -> None:
        name_tasks = {
            asyncio.create_task(
                device.fetch_device_name(
                    request_timeout_milliseconds=request_timeout_milliseconds,
                    request_attempts=request_attempts,
                )
            ): device
            for device in devices.values()
            if self.get_arc_port(device)
        }
        if not name_tasks:
            return

        name_results = await asyncio.gather(*name_tasks, return_exceptions=True)
        for task, result in zip(name_tasks, name_results):
            device = name_tasks[task]
            if isinstance(result, Exception):
                logger.debug(f"Failed to read device name from {device.server_name}: {result}")
            elif result:
                device.name = result

    async def populate_devices(
        self,
        devices: dict,
        timeout: float = 2.0,
        include_channels: bool = True,
    ) -> None:
        device_ip_addresses = [str(device.ipv4) for device in devices.values() if device.ipv4]
        request_timeout_milliseconds = None if include_channels else 500
        request_attempts = None if include_channels else 1

        phase_coroutines = [
            ("settings", self._query_settings_fields(devices)),
            ("ConMon", self._query_conmon_all(timeout=timeout, devices=devices)),
            ("interfaces", self._probe_interface_status(timeout=timeout, devices=devices)),
            ("preferred leader", self._probe_preferred_leader_all(timeout=timeout, devices=devices)),
            ("AES67", self._probe_aes67_all(timeout=timeout, devices=devices)),
            ("sample rates", self._probe_sample_rates_all(timeout=timeout, devices=devices)),
            ("encodings", self._probe_encodings_all(timeout=timeout, devices=devices)),
            ("gain levels", self._probe_gain_levels_all(timeout=timeout, devices=devices)),
            ("sample rate pull-ups", self._probe_sample_rate_pullups_all(timeout=timeout, devices=devices)),
        ]
        if device_ip_addresses:
            phase_coroutines.append(("CMC registration", self.cmc.register_all(device_ip_addresses)))

        control_coroutines = []
        for device in devices.values():
            if self.get_arc_port(device):
                control_coroutines.append(
                    self._populate_device_controls(
                        device,
                        include_channels=include_channels,
                        request_timeout_milliseconds=request_timeout_milliseconds,
                        request_attempts=request_attempts,
                    )
                )

        phase_tasks = {
            asyncio.create_task(phase_coroutine): phase_name for phase_name, phase_coroutine in phase_coroutines
        }
        control_tasks = [asyncio.create_task(control_coroutine) for control_coroutine in control_coroutines]
        try:
            completed_tasks, pending_tasks = await asyncio.wait(phase_tasks, timeout=timeout)
        finally:
            unfinished_tasks = [task for task in phase_tasks if not task.done()]
            for task in unfinished_tasks:
                task.cancel()
            if unfinished_tasks:
                await asyncio.gather(*unfinished_tasks, return_exceptions=True)
            if control_tasks:
                await asyncio.gather(*control_tasks, return_exceptions=True)
        if pending_tasks:
            pending_names = ", ".join(sorted(phase_tasks[task] for task in pending_tasks))
            logger.debug(f"Device detail deadline reached while waiting for: {pending_names}")
        for task in completed_tasks:
            exception = task.exception()
            if exception is not None:
                logger.warning(f"Failed to populate {phase_tasks[task]}: {exception}")

    async def _populate_device_controls(
        self,
        device,
        include_channels: bool = True,
        request_timeout_milliseconds: int | None = None,
        request_attempts: int | None = None,
    ) -> None:
        try:
            await device.populate_from_core(
                include_channels=include_channels,
                request_timeout_milliseconds=request_timeout_milliseconds,
                request_attempts=request_attempts,
            )
            if include_channels:
                await self._apply_avio_status_pages(device)
        except Exception as exception:
            device.error = exception
            logger.debug(f"Error populating controls for {device.server_name}: {exception}")

    async def _apply_avio_status_pages(self, device) -> None:
        try:
            receiver_flow_page = await device.operations.query_receiver_flow_status_2809()
        except RuntimeError:
            receiver_flow_page = None
        if receiver_flow_page is not None:
            device.apply_receiver_flow_status_page(receiver_flow_page)
        try:
            transmitter_channel_page = await device.operations.query_transmitter_channel_status_2809()
        except RuntimeError:
            transmitter_channel_page = None
        if transmitter_channel_page is not None:
            device.apply_transmitter_channel_status_page(transmitter_channel_page)
        try:
            transmitter_flow_page = await device.operations.query_transmitter_flow_status_2809()
        except RuntimeError:
            transmitter_flow_page = None
        if transmitter_flow_page is not None:
            device.apply_transmitter_flow_status_page(transmitter_flow_page)
        try:
            receiver_channel_page = await device.operations.query_receiver_channel_status_2809()
        except RuntimeError:
            receiver_channel_page = None
        if receiver_channel_page is not None:
            device.apply_receiver_channel_status_page(receiver_channel_page)

    async def _probe_interface_status(self, timeout: float = 3.0, devices: dict | None = None) -> None:
        target_devices = self.devices if devices is None else devices
        waiters = {}
        for device in target_devices.values():
            device_ip = str(device.ipv4) if device.ipv4 else None
            if device_ip:
                waiter = self.notifications.register_interface_waiter(device_ip)
                waiters[device_ip] = waiter
                self.settings.probe_interface_status(device_ip)

        if not waiters:
            return

        logger.debug(f"Probed interface status for {len(waiters)} devices")

        try:
            await asyncio.wait_for(
                asyncio.gather(*(waiter.wait() for waiter in waiters.values()), return_exceptions=True),
                timeout=timeout,
            )
        except asyncio.TimeoutError:
            logger.debug("Interface status probe did not receive every response")
        finally:
            for device_ip in waiters:
                self.notifications.unregister_interface_waiter(device_ip)

        populated = sum(1 for device in target_devices.values() if device.interfaces)
        logger.debug(f"Interface status: {populated}/{len(waiters)} devices responded")

    async def _probe_preferred_leader_all(self, timeout: float = 3.0, devices: dict | None = None) -> None:
        target_devices = self.devices if devices is None else devices
        waiters = {}
        for device in target_devices.values():
            if device.preferred_leader is not None:
                continue
            device_ip = str(device.ipv4) if device.ipv4 else None
            if device_ip:
                waiter = self.notifications.register_preferred_leader_waiter(device_ip)
                waiters[device_ip] = waiter
                self.settings.probe_preferred_leader(device_ip)

        if not waiters:
            return

        logger.debug(f"Probed preferred leader for {len(waiters)} devices")

        try:
            await asyncio.wait_for(
                asyncio.gather(*(waiter.wait() for waiter in waiters.values()), return_exceptions=True),
                timeout=timeout,
            )
        except asyncio.TimeoutError:
            logger.debug("Preferred leader probe did not receive every response")
        finally:
            for device_ip in waiters:
                result = self.notifications.get_preferred_leader_result(device_ip)
                if result is not None:
                    device = self._device_by_ip(device_ip)
                    if device:
                        device.preferred_leader = result
                self.notifications.unregister_preferred_leader_waiter(device_ip)

        populated = sum(1 for device in target_devices.values() if device.preferred_leader is not None)
        logger.debug(f"Preferred leader: {populated}/{len(target_devices)} devices have data")

    async def _probe_aes67_all(self, timeout: float = 3.0, devices: dict | None = None) -> None:
        target_devices = self.devices if devices is None else devices
        waiters = {}
        for device in target_devices.values():
            if device.aes67_current is not None:
                continue
            device_ip = str(device.ipv4) if device.ipv4 else None
            if device_ip:
                waiter = self.notifications.register_aes67_waiter(device_ip)
                waiters[device_ip] = waiter
                self.settings.probe_aes67(device_ip)

        if not waiters:
            return

        logger.debug(f"Probed AES67 for {len(waiters)} devices")

        try:
            await asyncio.wait_for(
                asyncio.gather(*(waiter.wait() for waiter in waiters.values()), return_exceptions=True),
                timeout=timeout,
            )
        except asyncio.TimeoutError:
            logger.debug("AES67 probe did not receive every response")
        finally:
            for device_ip in waiters:
                result = self.notifications.get_aes67_result(device_ip)
                if result is not None:
                    aes67_current, aes67_configured = result
                    device = self._device_by_ip(device_ip)
                    if device:
                        if aes67_current is not None:
                            device.aes67_current = aes67_current
                        if aes67_configured is not None:
                            device.aes67_configured = aes67_configured
                self.notifications.unregister_aes67_waiter(device_ip)

        populated = sum(1 for device in target_devices.values() if device.aes67_current is not None)
        logger.debug(f"AES67: {populated}/{len(target_devices)} devices have data")

    async def _probe_capabilities_all(
        self,
        capability_is_known,
        apply_capability,
        probe_status,
        capability_description: str,
        timeout: float,
        devices: dict | None = None,
    ) -> None:
        target_devices = self.devices if devices is None else devices
        probe_tasks = {}
        target_devices_by_ip_address = {}
        for device in target_devices.values():
            if not device.online or not device.ipv4 or capability_is_known(device):
                continue
            device_ip_address = str(device.ipv4)
            target_devices_by_ip_address.setdefault(device_ip_address, []).append(device)
            if device_ip_address in probe_tasks:
                continue
            probe_tasks[device_ip_address] = asyncio.create_task(probe_status(device_ip_address, timeout=timeout))

        if not probe_tasks:
            return

        logger.debug(f"Probed {capability_description} for {len(probe_tasks)} device addresses")

        probe_results = await asyncio.gather(*probe_tasks.values(), return_exceptions=True)
        response_count = 0
        for device_ip_address, result in zip(probe_tasks, probe_results):
            if isinstance(result, Exception):
                logger.warning(f"Failed to probe {capability_description} for {device_ip_address}: {result}")
                continue
            if result is None:
                continue
            response_count += 1
            current_value, supported_values = result
            for device in target_devices_by_ip_address[device_ip_address]:
                if device.online:
                    apply_capability(device, current_value, supported_values)

        logger.debug(
            f"{capability_description.capitalize()}: {response_count}/{len(probe_tasks)} device addresses responded"
        )

    async def _probe_sample_rates_all(self, timeout: float = 3.0, devices: dict | None = None) -> None:
        await self._probe_capabilities_all(
            lambda device: device.supported_sample_rates is not None,
            self._apply_sample_rate_capability,
            self.probe_sample_rate_status,
            "sample rates",
            timeout,
            devices,
        )

    async def _probe_encodings_all(self, timeout: float = 3.0, devices: dict | None = None) -> None:
        await self._probe_capabilities_all(
            lambda device: device.supported_encodings is not None,
            self._apply_encoding_capability,
            self.probe_encoding_status,
            "encodings",
            timeout,
            devices,
        )

    async def _probe_sample_rate_pullups_all(self, timeout: float = 3.0, devices: dict | None = None) -> None:
        await self._probe_capabilities_all(
            lambda device: device.supported_sample_rate_pullup_raw_values is not None,
            self._apply_sample_rate_pullup_capability,
            self.probe_sample_rate_pullup_status,
            "sample rate pull-ups",
            timeout,
            devices,
        )

    async def _probe_gain_levels_all(self, timeout: float = 3.0, devices: dict | None = None) -> None:
        await self._probe_capabilities_all(
            lambda device: device.supported_gain_levels is not None,
            self._apply_gain_capability,
            self.probe_gain_status,
            "gain levels",
            timeout,
            devices,
        )

    @staticmethod
    def _apply_sample_rate_capability(device, current_sample_rate: int, supported_sample_rates: list[int]) -> None:
        device.sample_rate = current_sample_rate
        device.supported_sample_rates = supported_sample_rates

    @staticmethod
    def _apply_encoding_capability(device, current_encoding: int, supported_encodings: list[int]) -> None:
        device.encoding = current_encoding
        device.supported_encodings = supported_encodings

    @staticmethod
    def _apply_sample_rate_pullup_capability(
        device,
        current_raw_value: int,
        supported_raw_values: list[int],
    ) -> None:
        device.sample_rate_pullup_raw_value = current_raw_value
        device.supported_sample_rate_pullup_raw_values = supported_raw_values

    @staticmethod
    def _apply_gain_capability(device, device_type: str, channel_levels: list[int]) -> None:
        device.gain_device_type = device_type
        device.gain_levels = channel_levels
        device.supported_gain_levels = list(SUPPORTED_GAIN_LEVELS)
