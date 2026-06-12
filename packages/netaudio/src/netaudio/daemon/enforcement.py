from __future__ import annotations

import asyncio
import json
import logging
import os

from netaudio.dante.events import DanteEvent, EventType

logger = logging.getLogger("netaudio")

SOCKET_PATH = "/tmp/netaudio-enforce.sock"
ENFORCE_DELAY = 2.0
ENFORCE_RETRY_DELAY = 10.0


class EnforcementManager:
    def __init__(self, daemon):
        self.daemon = daemon
        self.desired_states: dict = {}
        self.shure_desired: dict[str, dict] = {}
        self._socket_server = None
        self._enforce_tasks: dict[str, asyncio.Task] = {}
        self._running = False

    async def start(self):
        self._running = True

        if os.path.exists(SOCKET_PATH):
            os.unlink(SOCKET_PATH)

        self._socket_server = await asyncio.start_unix_server(
            self._handle_client, path=SOCKET_PATH
        )
        os.chmod(SOCKET_PATH, 0o660)

        dispatcher = self.daemon.application.dispatcher
        dispatcher.on(EventType.DEVICE_DISCOVERED, self._on_device_event)
        dispatcher.on(EventType.DEVICE_UPDATED, self._on_device_event)
        dispatcher.on(EventType.SHURE_DEVICE_DISCOVERED, self._on_shure_device_event)
        dispatcher.on(EventType.SHURE_DEVICE_UPDATED, self._on_shure_device_event)

        logger.info("EnforcementManager started")

    async def stop(self):
        self._running = False

        for task in self._enforce_tasks.values():
            task.cancel()
        self._enforce_tasks.clear()

        if self._socket_server:
            self._socket_server.close()
            await self._socket_server.wait_closed()

        if os.path.exists(SOCKET_PATH):
            os.unlink(SOCKET_PATH)

        logger.info("EnforcementManager stopped")

    async def _handle_client(self, reader, writer):
        try:
            data = await asyncio.wait_for(reader.read(4096), timeout=5.0)
            if not data:
                return

            request = json.loads(data.decode("utf-8"))
            response = self._process_request(request)

            writer.write(json.dumps(response).encode("utf-8"))
            await writer.drain()
        except Exception as exc:
            try:
                error = {"status": "error", "message": str(exc)}
                writer.write(json.dumps(error).encode("utf-8"))
                await writer.drain()
            except Exception:
                pass
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

    def _process_request(self, request):
        handlers = {
            "set_subscription": self._req_set_subscription,
            "set_sample_rate": self._req_set_sample_rate,
            "set_channel_name": self._req_set_channel_name,
            "remove_channel_name": self._req_remove_channel_name,
            "set_device_name": self._req_set_device_name,
            "remove_device_name": self._req_remove_device_name,
            "enforce_now": self._req_enforce_now,
            "get_status": self._req_get_status,
            "set_shure": self._req_set_shure,
            "remove_shure": self._req_remove_shure,
            "shure_command": self._req_shure_command,
        }

        action = request.get("action")
        handler = handlers.get(action)
        if not handler:
            return {"status": "error", "message": f"Unknown action: {action}"}

        return handler(request)

    # ── Dante request handlers ──

    def _req_set_subscription(self, request):
        rx_device = request["rx_device"]
        rx_channel = request["rx_channel"]
        tx_device = request["tx_device"]
        tx_channel = request["tx_channel"]
        state = request["state"]

        desired = self.desired_states.setdefault(rx_device, {})
        subs = desired.setdefault("subscriptions", {})

        if state == "connected":
            subs[rx_channel] = f"{tx_device}:{tx_channel}"
        else:
            subs[rx_channel] = None

        logger.info(
            f"Enforcement rule: subscription {tx_device}:{tx_channel} -> "
            f"{rx_device}:{rx_channel} ({state})"
        )

        self._schedule_enforce(rx_device)
        return {"status": "accepted", "message": "State change queued for enforcement"}

    def _req_set_sample_rate(self, request):
        device = request["device"]
        sample_rate = request["sample_rate"]

        desired = self.desired_states.setdefault(device, {})
        desired["sample_rate"] = sample_rate

        logger.info(f"Enforcement rule: sample rate {device} -> {sample_rate}Hz")

        self._schedule_enforce(device)
        return {"status": "accepted", "message": "Sample rate change queued for enforcement"}

    def _req_set_channel_name(self, request):
        device = request["device"]
        channel_type = request["channel_type"]
        channel_number = request["channel_number"]
        channel_name = request["channel_name"]

        desired = self.desired_states.setdefault(device, {})
        names = desired.setdefault("channel_names", {})
        names[f"{channel_type}:{channel_number}"] = channel_name

        logger.info(
            f"Enforcement rule: channel name {device} "
            f"{channel_type.upper()} {channel_number} -> '{channel_name}'"
        )

        self._schedule_enforce(device)
        return {"status": "accepted", "message": "Channel name change queued for enforcement"}

    def _req_remove_channel_name(self, request):
        device = request["device"]
        channel_type = request["channel_type"]
        channel_number = request["channel_number"]

        key = f"{channel_type}:{channel_number}"
        if device in self.desired_states and "channel_names" in self.desired_states[device]:
            self.desired_states[device]["channel_names"].pop(key, None)

        logger.info(
            f"Enforcement rule removed: channel name {device} "
            f"{channel_type.upper()} {channel_number}"
        )
        return {"status": "accepted", "message": "Channel name rule removed"}

    def _req_set_device_name(self, request):
        device_identifier = request["device_identifier"]
        target_name = request["target_name"]

        device_names = self.desired_states.setdefault("device_names", {})
        device_names[device_identifier] = target_name

        logger.info(
            f"Enforcement rule: device name {device_identifier} -> '{target_name}'"
        )

        self._schedule_enforce_all()
        return {"status": "accepted", "message": "Device name change queued for enforcement"}

    def _req_remove_device_name(self, request):
        device_identifier = request["device_identifier"]

        if "device_names" in self.desired_states:
            self.desired_states["device_names"].pop(device_identifier, None)

        logger.info(f"Enforcement rule removed: device name {device_identifier}")
        return {"status": "accepted", "message": "Device name rule removed"}

    def _req_enforce_now(self, _request):
        logger.info("Immediate enforcement requested")
        self._schedule_enforce_all()
        self._schedule_shure_enforce_all()
        return {"status": "accepted", "message": "Enforcement cycle triggered"}

    def _req_get_status(self, _request):
        device_states = {}
        for server_name, device in self.daemon.application.devices.items():
            if not device.online:
                continue

            subs = {}
            for sub in device.subscriptions:
                rx_name = getattr(sub, "rx_channel_name", None)
                tx_dev = getattr(sub, "device_name", None) or getattr(sub, "tx_device_name", None)
                tx_ch = getattr(sub, "channel_name", None) or getattr(sub, "tx_channel_name", None)
                if rx_name and tx_dev and tx_ch:
                    subs[rx_name] = f"{tx_dev}:{tx_ch}"

            channel_names = {}
            for ch_num, ch in (device.tx_channels or {}).items():
                fn = getattr(ch, "friendly_name", None)
                if ch_num and fn:
                    channel_names[f"tx:{ch_num}"] = fn
            for ch_num, ch in (device.rx_channels or {}).items():
                n = getattr(ch, "name", None)
                if ch_num and n:
                    channel_names[f"rx:{ch_num}"] = n

            device_states[device.name or server_name] = {
                "present": True,
                "ip": str(device.ipv4) if device.ipv4 else "",
                "sample_rate": device.sample_rate,
                "subscriptions": subs,
                "channel_names": channel_names,
            }

        shure_states = {}
        if hasattr(self.daemon, "shure") and self.daemon.shure:
            for mac, dev in self.daemon.shure.devices.items():
                shure_states[mac] = dev.to_json()

        return {
            "status": "ok",
            "desired_states": self.desired_states,
            "shure_desired": self.shure_desired,
            "device_states": device_states,
            "shure_states": shure_states,
        }

    # ── Shure request handlers ──

    def _req_set_shure(self, request):
        mac = request["mac"]
        settings = request.get("settings", {})

        desired = self.shure_desired.setdefault(mac, {})

        if "device_name" in settings:
            desired["device_name"] = settings["device_name"]

        if "channels" in settings:
            ch_desired = desired.setdefault("channels", {})
            for ch_str, ch_settings in settings["channels"].items():
                ch_num = str(ch_str)
                ch_d = ch_desired.setdefault(ch_num, {})
                for key in ("channel_name", "audio_gain", "audio_mute", "rf_mute", "rf_tx_level", "audio_in_level"):
                    if key in ch_settings:
                        ch_d[key] = ch_settings[key]

        logger.info(f"Enforcement rule: shure {mac} -> {settings}")

        self._schedule_shure_enforce(mac)
        return {"status": "accepted", "message": "Shure enforcement queued"}

    def _req_shure_command(self, request):
        mac = request["mac"]
        settings = request.get("settings", {})

        enforced_keys = set()
        if mac in self.shure_desired:
            desired = self.shure_desired[mac]
            if "device_name" in desired and "device_name" in settings:
                enforced_keys.add("device_name")
            ch_desired = desired.get("channels", {})
            for ch_str, ch_settings in settings.get("channels", {}).items():
                ch_enforced = ch_desired.get(str(ch_str), {})
                for key in ch_settings:
                    if key in ch_enforced:
                        enforced_keys.add(f"ch{ch_str}.{key}")

        if enforced_keys:
            return {
                "status": "enforced",
                "message": f"Blocked: {', '.join(sorted(enforced_keys))} currently enforced",
                "enforced_keys": sorted(enforced_keys),
            }

        self._schedule_shure_command(mac, settings)
        return {"status": "accepted", "message": "Command sent"}

    def _schedule_shure_command(self, mac, settings):
        asyncio.create_task(self._execute_shure_command(mac, settings))

    async def _execute_shure_command(self, mac, settings):
        shure = self.daemon.shure
        if not shure:
            return

        device = shure.devices.get(mac)
        if not device:
            return

        is_ad4d = device.device_type.value == "ad4d"

        if "device_name" in settings:
            key = "DEVICE_ID" if is_ad4d else "DEVICE_NAME"
            await shure.send_command(mac, f"SET {key} {settings['device_name']}")

        for ch_str, ch_settings in settings.get("channels", {}).items():
            if "channel_name" in ch_settings:
                await shure.send_command(mac, f"SET {ch_str} CHAN_NAME {ch_settings['channel_name']}")
            if "audio_gain" in ch_settings and is_ad4d:
                await shure.send_command(mac, f"SET {ch_str} AUDIO_GAIN {ch_settings['audio_gain']}")
            if "audio_mute" in ch_settings and is_ad4d:
                val = "ON" if ch_settings["audio_mute"] else "OFF"
                await shure.send_command(mac, f"SET {ch_str} AUDIO_MUTE {val}")
            if "rf_mute" in ch_settings and not is_ad4d:
                val = "1" if ch_settings["rf_mute"] else "0"
                await shure.send_command(mac, f"SET {ch_str} RF_MUTE {val}")
            if "rf_tx_level" in ch_settings and not is_ad4d:
                await shure.send_command(mac, f"SET {ch_str} RF_TX_LVL {ch_settings['rf_tx_level']}")
            if "audio_in_level" in ch_settings and not is_ad4d:
                await shure.send_command(mac, f"SET {ch_str} AUDIO_IN_LVL {ch_settings['audio_in_level']}")
            if "audio_in_line_level" in ch_settings and not is_ad4d:
                val = "1" if ch_settings["audio_in_line_level"] else "0"
                await shure.send_command(mac, f"SET {ch_str} AUDIO_IN_LINE_LVL {val}")

    def _req_remove_shure(self, request):
        mac = request["mac"]
        key = request.get("key")

        if key and mac in self.shure_desired:
            if key == "device_name":
                self.shure_desired[mac].pop("device_name", None)
            elif key.startswith("channel:"):
                parts = key.split(":", 2)
                if len(parts) == 3:
                    ch_num, setting = parts[1], parts[2]
                    channels = self.shure_desired[mac].get("channels", {})
                    if ch_num in channels:
                        channels[ch_num].pop(setting, None)
        elif not key:
            self.shure_desired.pop(mac, None)

        logger.info(f"Enforcement rule removed: shure {mac} {key or 'all'}")
        return {"status": "accepted", "message": "Shure enforcement rule removed"}

    # ── Scheduling ──

    def _schedule_enforce(self, device_name, delay=ENFORCE_DELAY):
        key = f"dante:{device_name}"
        existing = self._enforce_tasks.pop(key, None)
        if existing and not existing.done():
            existing.cancel()

        self._enforce_tasks[key] = asyncio.create_task(
            self._delayed_enforce(device_name, delay)
        )

    def _schedule_enforce_all(self, delay=ENFORCE_DELAY):
        for device in self.daemon.application.devices.values():
            if device.online and device.name:
                self._schedule_enforce(device.name, delay)

    def _schedule_shure_enforce(self, mac, delay=ENFORCE_DELAY):
        key = f"shure:{mac}"
        existing = self._enforce_tasks.pop(key, None)
        if existing and not existing.done():
            existing.cancel()

        self._enforce_tasks[key] = asyncio.create_task(
            self._delayed_shure_enforce(mac, delay)
        )

    def _schedule_shure_enforce_all(self, delay=ENFORCE_DELAY):
        if not hasattr(self.daemon, "shure") or not self.daemon.shure:
            return
        for mac in self.daemon.shure.devices:
            if mac in self.shure_desired:
                self._schedule_shure_enforce(mac, delay)

    async def _delayed_enforce(self, device_name, delay):
        try:
            await asyncio.sleep(delay)
            await self._enforce_device(device_name)
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            logger.warning(f"Enforcement failed for {device_name}: {exc}")
            if self._running:
                self._schedule_enforce(device_name, ENFORCE_RETRY_DELAY)

    async def _delayed_shure_enforce(self, mac, delay):
        try:
            await asyncio.sleep(delay)
            await self._enforce_shure_device(mac)
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            logger.warning(f"Shure enforcement failed for {mac}: {exc}")
            if self._running:
                self._schedule_shure_enforce(mac, ENFORCE_RETRY_DELAY)

    # ── Event handlers ──

    async def _on_device_event(self, event: DanteEvent):
        device = self.daemon.application.devices.get(event.server_name)
        if not device or not device.online:
            return

        device_name = device.name
        if not device_name:
            return

        has_rules = device_name in self.desired_states
        has_name_rule = False
        if "device_names" in self.desired_states:
            for ident, target in self.desired_states["device_names"].items():
                if ident == event.server_name or ident == device_name or target == device_name:
                    has_name_rule = True
                    break

        if has_rules or has_name_rule:
            self._schedule_enforce(device_name)

    async def _on_shure_device_event(self, event: DanteEvent):
        mac = event.device_name
        if mac in self.shure_desired:
            self._schedule_shure_enforce(mac)

    # ── Dante enforcement ──

    async def _enforce_device(self, device_name):
        device = self._find_device(device_name)
        if not device or not device.online:
            return

        app = self.daemon.application
        arc_port = app.get_arc_port(device)
        if not arc_port:
            logger.debug(f"No ARC port for {device_name}, skipping enforcement")
            return

        if not device.tx_channels and not device.rx_channels:
            try:
                await app.arc.get_controls(device, arc_port)
            except Exception as exc:
                logger.debug(f"Failed to get controls for {device_name}: {exc}")
                return

        await self._enforce_device_name(device)
        await self._enforce_sample_rate(device)
        await self._enforce_channel_names(device)
        await self._enforce_subscriptions(device)

    async def _enforce_device_name(self, device):
        device_names = self.desired_states.get("device_names", {})
        target_name = None

        for ident, name in device_names.items():
            if ident == device.server_name or ident == device.name:
                target_name = name
                break

        if target_name is None or device.name == target_name:
            return

        logger.info(f"Enforcing device name: '{device.name}' -> '{target_name}'")
        try:
            await device.operations.set_name(target_name)
        except Exception as exc:
            logger.warning(f"Failed to set device name for {device.name}: {exc}")

    async def _enforce_sample_rate(self, device):
        desired = self.desired_states.get(device.name, {})
        target_rate = desired.get("sample_rate")
        if target_rate is None:
            return

        if device.sample_rate == target_rate:
            return

        logger.info(
            f"Enforcing sample rate on {device.name}: "
            f"{device.sample_rate} -> {target_rate}"
        )
        try:
            await device.operations.set_sample_rate(target_rate)
        except Exception as exc:
            logger.warning(f"Failed to set sample rate for {device.name}: {exc}")

    async def _enforce_channel_names(self, device):
        desired = self.desired_states.get(device.name, {})
        target_names = desired.get("channel_names", {})
        if not target_names:
            return

        current_names = {}
        for ch_num, ch in (device.tx_channels or {}).items():
            fn = getattr(ch, "friendly_name", None)
            if fn:
                current_names[f"tx:{ch_num}"] = fn
        for ch_num, ch in (device.rx_channels or {}).items():
            n = getattr(ch, "name", None)
            if n:
                current_names[f"rx:{ch_num}"] = n

        for key, target_name in target_names.items():
            current_name = current_names.get(key)
            if current_name == target_name:
                continue

            channel_type, ch_num_str = key.split(":", 1)
            ch_num = int(ch_num_str)

            logger.info(
                f"Enforcing channel name on {device.name} "
                f"{channel_type.upper()} {ch_num}: '{current_name}' -> '{target_name}'"
            )
            try:
                await device.operations.set_channel_name(channel_type, ch_num, target_name)
            except Exception as exc:
                logger.warning(
                    f"Failed to set channel name {device.name} "
                    f"{channel_type} {ch_num}: {exc}"
                )

    async def _enforce_subscriptions(self, device):
        desired = self.desired_states.get(device.name, {})
        target_subs = desired.get("subscriptions", {})
        if not target_subs:
            return

        current_subs = {}
        for sub in device.subscriptions:
            rx_name = getattr(sub, "rx_channel_name", None)
            tx_dev = getattr(sub, "device_name", None) or getattr(sub, "tx_device_name", None)
            tx_ch = getattr(sub, "channel_name", None) or getattr(sub, "tx_channel_name", None)
            if rx_name and tx_dev and tx_ch:
                current_subs[rx_name] = f"{tx_dev}:{tx_ch}"

        for rx_channel_name, desired_tx in target_subs.items():
            current_tx = current_subs.get(rx_channel_name)

            if desired_tx and current_tx == desired_tx:
                continue
            if desired_tx is None and current_tx is None:
                continue

            if desired_tx:
                tx_dev_name, tx_ch_name = desired_tx.split(":", 1)

                logger.info(
                    f"Enforcing subscription on {device.name}: "
                    f"{rx_channel_name} <- {desired_tx}"
                )

                rx_ch_obj = None
                for ch in (device.rx_channels or {}).values():
                    if getattr(ch, "name", None) == rx_channel_name:
                        rx_ch_obj = ch
                        break

                tx_dev_obj = self._find_device(tx_dev_name)
                tx_ch_obj = None
                if tx_dev_obj:
                    for ch in (tx_dev_obj.tx_channels or {}).values():
                        fn = getattr(ch, "friendly_name", None)
                        n = getattr(ch, "name", None)
                        if tx_ch_name == fn or (tx_ch_name == n and not fn):
                            tx_ch_obj = ch
                            break

                if rx_ch_obj and tx_ch_obj and tx_dev_obj:
                    try:
                        await device.operations.add_subscription(
                            rx_ch_obj, tx_ch_obj, tx_dev_obj
                        )
                    except Exception as exc:
                        logger.warning(
                            f"Failed to add subscription {device.name}:"
                            f"{rx_channel_name} <- {desired_tx}: {exc}"
                        )
                else:
                    missing = []
                    if not rx_ch_obj:
                        missing.append(f"rx_ch '{rx_channel_name}'")
                    if not tx_dev_obj:
                        missing.append(f"tx_dev '{tx_dev_name}'")
                    if not tx_ch_obj:
                        missing.append(f"tx_ch '{tx_ch_name}'")
                    logger.debug(f"Cannot enforce subscription, missing: {', '.join(missing)}")
            else:
                if not current_tx:
                    continue

                logger.info(
                    f"Enforcing disconnection on {device.name}: "
                    f"{rx_channel_name} (was {current_tx})"
                )

                rx_ch_obj = None
                for ch in (device.rx_channels or {}).values():
                    if getattr(ch, "name", None) == rx_channel_name:
                        rx_ch_obj = ch
                        break

                if rx_ch_obj:
                    try:
                        await device.operations.remove_subscription(rx_ch_obj)
                    except Exception as exc:
                        logger.warning(
                            f"Failed to remove subscription {device.name}:"
                            f"{rx_channel_name}: {exc}"
                        )

    # ── Shure enforcement ──

    async def _enforce_shure_device(self, mac):
        shure = self.daemon.shure
        if not shure:
            return

        device = shure.devices.get(mac)
        if not device:
            return

        desired = self.shure_desired.get(mac, {})
        if not desired:
            return

        is_ad4d = device.device_type.value == "ad4d"

        if "device_name" in desired:
            target = desired["device_name"]
            if device.name != target:
                key = "DEVICE_ID" if is_ad4d else "DEVICE_NAME"
                logger.info(f"Enforcing Shure device name {mac}: '{device.name}' -> '{target}'")
                await shure.send_command(mac, f"SET {key} {target}")

        ch_desired = desired.get("channels", {})
        for ch_str, ch_settings in ch_desired.items():
            ch = device.channels.get(int(ch_str))

            if "channel_name" in ch_settings:
                target = ch_settings["channel_name"]
                current = ch.name if ch else None
                if current != target:
                    logger.info(f"Enforcing Shure ch{ch_str} name {mac}: '{current}' -> '{target}'")
                    await shure.send_command(mac, f"SET {ch_str} CHAN_NAME {target}")

            if "audio_gain" in ch_settings and is_ad4d:
                target = ch_settings["audio_gain"]
                current = ch.audio_gain if ch else None
                if current != target:
                    logger.info(f"Enforcing Shure ch{ch_str} gain {mac}: {current} -> {target}")
                    await shure.send_command(mac, f"SET {ch_str} AUDIO_GAIN {target}")

            if "audio_mute" in ch_settings and is_ad4d:
                target = ch_settings["audio_mute"]
                current = ch.audio_mute if ch else None
                target_val = "ON" if target else "OFF"
                if current != target:
                    logger.info(f"Enforcing Shure ch{ch_str} mute {mac}: {current} -> {target}")
                    await shure.send_command(mac, f"SET {ch_str} AUDIO_MUTE {target_val}")

            if "rf_mute" in ch_settings and not is_ad4d:
                target = ch_settings["rf_mute"]
                current = ch.rf_mute if ch else None
                target_val = "1" if target else "0"
                if current != target:
                    logger.info(f"Enforcing Shure ch{ch_str} RF mute {mac}: {current} -> {target}")
                    await shure.send_command(mac, f"SET {ch_str} RF_MUTE {target_val}")

            if "rf_tx_level" in ch_settings and not is_ad4d:
                target = ch_settings["rf_tx_level"]
                current = ch.rf_tx_level if ch else None
                if current != target:
                    logger.info(f"Enforcing Shure ch{ch_str} RF TX level {mac}: {current} -> {target}")
                    await shure.send_command(mac, f"SET {ch_str} RF_TX_LVL {target}")

            if "audio_in_level" in ch_settings and not is_ad4d:
                target = ch_settings["audio_in_level"]
                current = ch.audio_in_level if ch else None
                if current != target:
                    logger.info(f"Enforcing Shure ch{ch_str} audio in level {mac}: {current} -> {target}")
                    await shure.send_command(mac, f"SET {ch_str} AUDIO_IN_LVL {target}")

    def _find_device(self, name):
        for server_name, device in self.daemon.application.devices.items():
            if device.name == name:
                return device
            if server_name == name:
                return device
        return None
