from netaudio.dante.const import (
    DEVICE_CONTROL_PORT,
    DEVICE_SETTINGS_PORT,
    SERVICE_ARC,
)


def _mac_to_hex(value):
    if value is None:
        return None
    if isinstance(value, bytes):
        return value.hex()
    return value


class DanteDeviceCommands:
    def __init__(self, host_mac=None):
        self._host_mac = host_mac

    def _resolve_host_mac(self, host_mac):
        resolved = host_mac if host_mac is not None else self._host_mac
        if resolved is None:
            from netaudio.common.app_config import settings
            from netaudio.dante.services.cmc import _get_host_mac

            resolved = _get_host_mac(settings.interface) if settings.interface else _get_host_mac()

        if not isinstance(resolved, bytes) or len(resolved) != 6 or resolved == b"\x00" * 6:
            raise ValueError("a non-zero 6-byte host MAC is required")
        return resolved

    def _with_host_mac(self, spec, host_mac):
        spec["host_mac"] = _mac_to_hex(self._resolve_host_mac(host_mac))
        return spec

    def _build(self, spec):
        from netaudio import core

        return core.build_command(spec)

    def _arc(self, spec):
        return (self._build(spec), SERVICE_ARC)

    def _settings(self, spec):
        return (self._build(spec), None, DEVICE_SETTINGS_PORT)

    def _control(self, spec):
        return (self._build(spec), None, DEVICE_CONTROL_PORT)

    def command_device_info(self):
        return self._arc({"command": "device_info"})

    def command_device_name(self, transaction_id=0):
        return self._arc({"command": "device_name", "transaction_id": transaction_id})

    def command_channel_count(self, transaction_id=0):
        return self._arc({"command": "channel_count", "transaction_id": transaction_id})

    def command_device_settings(self):
        return self._arc({"command": "device_settings"})

    def command_set_name(self, name):
        return self._arc({"command": "set_name", "name": name})

    def command_reset_name(self):
        return self._arc({"command": "reset_name"})

    def command_receivers(self, page=0, transaction_id=0):
        return self._arc({"command": "receivers", "page": page, "transaction_id": transaction_id})

    def command_transmitters(self, page=0, transaction_id=0):
        return self._arc({"command": "transmitters", "page": page, "transaction_id": transaction_id})

    def command_transmitter_names(self, channel_count, transaction_id=0):
        return self._arc(
            {
                "command": "transmitter_names",
                "channel_count": channel_count,
                "transaction_id": transaction_id,
            }
        )

    def command_reset_channel_name(self, channel_type, channel_number):
        return self._arc(
            {
                "command": "reset_channel_name",
                "channel_type": channel_type,
                "channel_number": channel_number,
            }
        )

    def command_set_channel_name(self, channel_type, channel_number, new_channel_name):
        return self._arc(
            {
                "command": "set_channel_name",
                "channel_type": channel_type,
                "channel_number": channel_number,
                "name": new_channel_name,
            }
        )

    def command_add_subscription(self, rx_channel_number, tx_channel_name, tx_device_name):
        return self._arc(
            {
                "command": "add_subscriptions",
                "subscriptions": [
                    {"rx_channel": rx_channel_number, "tx_channel": tx_channel_name, "tx_device": tx_device_name}
                ],
            }
        )

    def command_add_subscriptions(self, subscriptions):
        return self._arc(
            {
                "command": "add_subscriptions",
                "subscriptions": [
                    {"rx_channel": rx, "tx_channel": txc, "tx_device": txd} for rx, txc, txd in subscriptions
                ],
            }
        )

    def command_remove_subscription(self, rx_channel):
        return self._arc({"command": "remove_subscriptions", "rx_channels": [rx_channel]})

    def command_remove_subscriptions(self, rx_channels):
        return self._arc({"command": "remove_subscriptions", "rx_channels": list(rx_channels)})

    def command_set_latency(self, latency):
        return self._arc({"command": "set_latency", "latency": latency})

    def command_query_latency_config(self, transaction_id=0):
        return (self._build({"command": "query_latency_config", "transaction_id": transaction_id}), SERVICE_ARC, None)

    def command_reboot(self, host_mac=None):
        spec = self._with_host_mac({"command": "reboot"}, host_mac)
        return self._settings(spec)

    def command_identify(self):
        return self._settings({"command": "identify"})

    def command_set_encoding(self, encoding):
        return self._settings({"command": "set_encoding", "encoding": encoding})

    def command_set_sample_rate(self, sample_rate):
        return self._settings({"command": "set_sample_rate", "sample_rate": sample_rate})

    def command_probe_sample_rate(self, host_mac=None, sequence=0x0081):
        command_specification = self._with_host_mac(
            {"command": "probe_sample_rate", "sequence": sequence},
            host_mac,
        )
        return self._settings(command_specification)

    def command_set_gain_level(self, channel_number, gain_level, device_type):
        return self._settings(
            {
                "command": "set_gain_level",
                "channel_number": channel_number,
                "gain_level": gain_level,
                "device_type": device_type,
            }
        )

    def command_enable_aes67(self, is_enabled: bool, host_mac=None):
        spec = self._with_host_mac(
            {"command": "enable_aes67", "enabled": bool(is_enabled)},
            host_mac,
        )
        return self._settings(spec)

    def command_probe_interface_status(self, host_mac=None):
        spec = self._with_host_mac({"command": "probe_interface_status"}, host_mac)
        return self._settings(spec)

    def command_set_interface_dhcp(self, host_mac=None):
        spec = self._with_host_mac({"command": "set_interface_dhcp"}, host_mac)
        return self._settings(spec)

    def command_set_interface_static(self, ip_address, netmask, dns_server, gateway, host_mac=None):
        spec = {
            "command": "set_interface_static",
            "ip": ip_address,
            "netmask": netmask,
            "dns": dns_server,
            "gateway": gateway,
        }
        self._with_host_mac(spec, host_mac)
        return self._settings(spec)

    def command_probe_aes67(self, host_mac=None, sequence=0x1007):
        spec = self._with_host_mac(
            {"command": "probe_aes67", "sequence": sequence},
            host_mac,
        )
        return self._settings(spec)

    def command_set_preferred_leader(self, is_preferred: bool, clock_source: int = 0, host_mac=None, sequence=0x0021):
        spec = {
            "command": "set_preferred_leader",
            "preferred": bool(is_preferred),
            "clock_source": clock_source,
            "sequence": sequence,
        }
        self._with_host_mac(spec, host_mac)
        return self._settings(spec)

    def command_probe_preferred_leader(self, clock_source: int = 0, host_mac=None, sequence=0x0021):
        spec = self._with_host_mac(
            {
                "command": "probe_preferred_leader",
                "clock_source": clock_source,
                "sequence": sequence,
            },
            host_mac,
        )
        return self._settings(spec)

    def command_bluetooth_status(self, host_mac=None):
        spec = self._with_host_mac({"command": "bluetooth_status"}, host_mac)
        return self._settings(spec)

    def command_cmc_register(self, sequence, host_mac=None):
        return self._build(
            {
                "command": "cmc_register",
                "sequence": sequence,
                "host_mac": _mac_to_hex(self._resolve_host_mac(host_mac)),
            }
        )

    def command_volume_start(self, device_name, ipv4, mac, port, timeout=True, transaction_id=0):
        return self._control(
            {
                "command": "volume_start",
                "device_name": device_name,
                "ipv4": str(ipv4) if ipv4 else "",
                "mac": _mac_to_hex(mac),
                "port": port,
                "timeout": timeout,
                "transaction_id": transaction_id,
            }
        )

    def command_volume_stop(self, device_name, ipv4, mac, port):
        return self._control({"command": "volume_stop", "device_name": device_name, "mac": _mac_to_hex(mac)})

    def command_metering_start(self, device_name, ipv4, mac, port, timeout=True, transaction_id=0):
        return self._control(
            {
                "command": "metering_start",
                "device_name": device_name,
                "ipv4": str(ipv4) if ipv4 else "",
                "mac": _mac_to_hex(mac),
                "port": port,
                "timeout": timeout,
                "transaction_id": transaction_id,
            }
        )

    def command_metering_stop(self, device_name, ipv4, mac, port):
        return self._control({"command": "metering_stop", "device_name": device_name, "mac": _mac_to_hex(mac)})

    def command_make_model(self, mac):
        return self._build({"command": "make_model", "mac": _mac_to_hex(mac)})

    def command_dante_model(self, mac):
        return self._build({"command": "dante_model", "mac": _mac_to_hex(mac)})
