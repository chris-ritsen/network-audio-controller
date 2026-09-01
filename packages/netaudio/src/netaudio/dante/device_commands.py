import secrets

from netaudio.dante.const import (
    DEVICE_CONTROL_PORT,
    DEVICE_SETTINGS_PORT,
    PROTOCOL_ARC_2809,
    SERVICE_ARC,
)


def _mac_to_hex(value):
    if value is None:
        return None
    if isinstance(value, bytes):
        return value.hex()
    return value


class DanteDeviceCommands:
    def __init__(self, host_mac=None, *, settings_sequence=None):
        self._host_mac = host_mac
        if settings_sequence is None:
            self._settings_sequence = secrets.randbelow(0xFFFF)
        elif isinstance(settings_sequence, bool) or not isinstance(settings_sequence, int):
            raise TypeError("settings_sequence must be an integer")
        elif not 0 <= settings_sequence <= 0xFFFF:
            raise ValueError("settings_sequence must fit in 16 bits")
        else:
            self._settings_sequence = settings_sequence

    def _next_settings_sequence(self):
        self._settings_sequence = (self._settings_sequence + 1) & 0xFFFF
        if self._settings_sequence == 0:
            self._settings_sequence = 1
        return self._settings_sequence

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

    def command_set_channel_name(
        self,
        channel_type,
        channel_number,
        new_channel_name,
        protocol_id=None,
        transaction_id=0,
    ):
        specification = {
            "command": "set_channel_name",
            "channel_type": channel_type,
            "channel_number": channel_number,
            "name": new_channel_name,
            "transaction_id": transaction_id,
        }
        if protocol_id is not None:
            specification["protocol_id"] = protocol_id
        return self._arc(specification)

    def command_query_receiver_channel_status(
        self,
        protocol_id=PROTOCOL_ARC_2809,
        media_type=1,
        starting_channel_identifier=1,
        ending_channel_identifier=0,
        transaction_id=0,
    ):
        return self._arc(
            {
                "command": "query_receiver_channel_status_2809",
                "protocol_id": protocol_id,
                "media_type": media_type,
                "starting_channel_identifier": starting_channel_identifier,
                "ending_channel_identifier": ending_channel_identifier,
                "transaction_id": transaction_id,
            }
        )

    def command_query_receiver_channel_status_2809(self, transaction_id=0):
        return self.command_query_receiver_channel_status(transaction_id=transaction_id)

    def command_query_transmitter_channel_status(
        self,
        protocol_id=PROTOCOL_ARC_2809,
        media_type=1,
        starting_channel_identifier=1,
        ending_channel_identifier=0,
        transaction_id=0,
    ):
        return self._arc(
            {
                "command": "query_transmitter_channel_status_2809",
                "protocol_id": protocol_id,
                "media_type": media_type,
                "starting_channel_identifier": starting_channel_identifier,
                "ending_channel_identifier": ending_channel_identifier,
                "transaction_id": transaction_id,
            }
        )

    def command_query_transmitter_channel_status_2809(self, transaction_id=0):
        return self.command_query_transmitter_channel_status(transaction_id=transaction_id)

    def command_query_receiver_flow_status_2809(self, transaction_id=0):
        return self._arc(
            {
                "command": "query_receiver_flow_status_2809",
                "transaction_id": transaction_id,
            }
        )

    def command_query_transmitter_flow_status_2809(self, transaction_id=0):
        return self._arc(
            {
                "command": "query_tx_flows",
                "flow_protocol_id": 0x2809,
                "starting_flow": 1,
                "transaction_id": transaction_id,
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

    def command_reboot(self, host_mac=None, sequence=None):
        spec = self._with_host_mac(
            {
                "command": "reboot",
                "sequence": self._next_settings_sequence() if sequence is None else sequence,
            },
            host_mac,
        )
        return self._settings(spec)

    def command_factory_reset(self, host_mac=None, sequence=None):
        spec = self._with_host_mac(
            {
                "command": "factory_reset",
                "sequence": self._next_settings_sequence() if sequence is None else sequence,
            },
            host_mac,
        )
        return self._settings(spec)

    def command_identify(self, sequence=None):
        return self._settings(
            {
                "command": "identify",
                "sequence": self._next_settings_sequence() if sequence is None else sequence,
            }
        )

    def command_set_encoding(self, encoding, sequence=None):
        return self._settings(
            {
                "command": "set_encoding",
                "encoding": encoding,
                "sequence": self._next_settings_sequence() if sequence is None else sequence,
            }
        )

    def command_set_sample_rate(self, sample_rate, sequence=None):
        return self._settings(
            {
                "command": "set_sample_rate",
                "sample_rate": sample_rate,
                "sequence": self._next_settings_sequence() if sequence is None else sequence,
            }
        )

    def command_probe_sample_rate(self, host_mac=None, sequence=0x0081):
        command_specification = self._with_host_mac(
            {"command": "probe_sample_rate", "sequence": sequence},
            host_mac,
        )
        return self._settings(command_specification)

    def command_probe_encoding(self, host_mac=None, sequence=0x0083):
        command_specification = self._with_host_mac(
            {"command": "probe_encoding", "sequence": sequence},
            host_mac,
        )
        return self._settings(command_specification)

    def command_probe_sample_rate_pullup(self, host_mac=None, sequence=0x0085):
        command_specification = self._with_host_mac(
            {"command": "probe_sample_rate_pullup", "sequence": sequence},
            host_mac,
        )
        return self._settings(command_specification)

    def command_set_sample_rate_pullup(self, raw_value, host_mac=None, sequence=None):
        command_specification = self._with_host_mac(
            {
                "command": "set_sample_rate_pullup",
                "raw_value": raw_value,
                "sequence": self._next_settings_sequence() if sequence is None else sequence,
            },
            host_mac,
        )
        return self._settings(command_specification)

    def command_probe_gain_level(self, host_mac=None, sequence=None):
        command_specification = self._with_host_mac(
            {
                "command": "probe_gain_level",
                "sequence": self._next_settings_sequence() if sequence is None else sequence,
            },
            host_mac,
        )
        return self._settings(command_specification)

    def command_set_gain_level(self, channel_number, gain_level, device_type, host_mac=None, sequence=None):
        command_specification = {
            "command": "set_gain_level",
            "channel_number": channel_number,
            "gain_level": gain_level,
            "device_type": device_type,
            "sequence": self._next_settings_sequence() if sequence is None else sequence,
        }
        self._with_host_mac(command_specification, host_mac)
        return self._settings(command_specification)

    def command_set_aes67_multicast_prefix(self, prefix: str, transaction_id=0):
        return (
            self._build(
                {
                    "command": "set_aes67_multicast_prefix",
                    "prefix": prefix,
                    "transaction_id": transaction_id,
                }
            ),
            SERVICE_ARC,
            None,
        )

    def command_enable_aes67(self, is_enabled: bool, host_mac=None, sequence=None):
        spec = self._with_host_mac(
            {
                "command": "enable_aes67",
                "enabled": bool(is_enabled),
                "sequence": self._next_settings_sequence() if sequence is None else sequence,
            },
            host_mac,
        )
        return self._settings(spec)

    def command_probe_interface_status(self, host_mac=None):
        spec = self._with_host_mac({"command": "probe_interface_status"}, host_mac)
        return self._settings(spec)

    def command_probe_link_status(self, host_mac=None, sequence=None):
        spec = self._with_host_mac(
            {
                "command": "probe_link_status",
                "sequence": self._next_settings_sequence() if sequence is None else sequence,
            },
            host_mac,
        )
        return self._settings(spec)

    def command_probe_switch_configuration(self, host_mac=None, sequence=None):
        spec = self._with_host_mac(
            {
                "command": "probe_switch_configuration",
                "sequence": self._next_settings_sequence() if sequence is None else sequence,
            },
            host_mac,
        )
        return self._settings(spec)

    def command_set_interface_dhcp(self, host_mac=None, sequence=None):
        spec = self._with_host_mac(
            {
                "command": "set_interface_dhcp",
                "sequence": self._next_settings_sequence() if sequence is None else sequence,
            },
            host_mac,
        )
        return self._settings(spec)

    def command_set_interface_static(
        self,
        ip_address,
        netmask,
        dns_server,
        gateway,
        host_mac=None,
        sequence=None,
    ):
        spec = {
            "command": "set_interface_static",
            "ip": ip_address,
            "netmask": netmask,
            "dns": dns_server,
            "gateway": gateway,
            "sequence": self._next_settings_sequence() if sequence is None else sequence,
        }
        self._with_host_mac(spec, host_mac)
        return self._settings(spec)

    def command_probe_aes67(self, host_mac=None, sequence=None):
        spec = self._with_host_mac(
            {
                "command": "probe_aes67",
                "sequence": self._next_settings_sequence() if sequence is None else sequence,
            },
            host_mac,
        )
        return self._settings(spec)

    def command_probe_lock_reset_status(self, host_mac=None, sequence=None, request_value=100):
        spec = self._with_host_mac(
            {
                "command": "probe_lock_reset_status",
                "sequence": self._next_settings_sequence() if sequence is None else sequence,
                "request_value": request_value,
            },
            host_mac,
        )
        return self._settings(spec)

    def command_device_log_export(self, host_mac=None, sequence=None):
        spec = self._with_host_mac(
            {
                "command": "device_log_export",
                "sequence": self._next_settings_sequence() if sequence is None else sequence,
            },
            host_mac,
        )
        return self._settings(spec)

    def command_capability_partition_export(self, host_mac=None, sequence=None):
        spec = self._with_host_mac(
            {
                "command": "capability_partition_export",
                "sequence": self._next_settings_sequence() if sequence is None else sequence,
            },
            host_mac,
        )
        return self._settings(spec)

    def command_probe_clear_configuration_status(self, host_mac=None, sequence=None):
        spec = self._with_host_mac(
            {
                "command": "probe_clear_configuration_status",
                "sequence": self._next_settings_sequence() if sequence is None else sequence,
            },
            host_mac,
        )
        return self._settings(spec)

    def command_clear_all_configuration(self, host_mac=None, sequence=None):
        spec = self._with_host_mac(
            {
                "command": "clear_all_configuration",
                "sequence": self._next_settings_sequence() if sequence is None else sequence,
            },
            host_mac,
        )
        return self._settings(spec)

    def command_clear_all_configuration_preserving_internet_protocol_settings(
        self,
        host_mac=None,
        sequence=None,
    ):
        spec = self._with_host_mac(
            {
                "command": "clear_all_configuration_preserving_internet_protocol_settings",
                "sequence": self._next_settings_sequence() if sequence is None else sequence,
            },
            host_mac,
        )
        return self._settings(spec)

    def command_set_clock_subdomain(self, subdomain, host_mac=None, sequence=None):
        if isinstance(subdomain, str):
            subdomain_bytes = subdomain.encode("ascii")
        else:
            subdomain_bytes = bytes(subdomain)
        if len(subdomain_bytes) > 16:
            raise ValueError("clock subdomain is longer than 16 bytes")
        field = list(subdomain_bytes.ljust(16, b"\x00"))
        spec = {
            "command": "set_clock_subdomain",
            "subdomain": field,
            "sequence": self._next_settings_sequence() if sequence is None else sequence,
        }
        self._with_host_mac(spec, host_mac)
        return self._settings(spec)

    def command_set_clock_source(self, clock_source: int, host_mac=None, sequence=None):
        spec = {
            "command": "set_clock_source",
            "clock_source": clock_source,
            "sequence": self._next_settings_sequence() if sequence is None else sequence,
        }
        self._with_host_mac(spec, host_mac)
        return self._settings(spec)

    def command_set_preferred_leader(self, is_preferred: bool, clock_source: int = 0, host_mac=None, sequence=None):
        spec = {
            "command": "set_preferred_leader",
            "preferred": bool(is_preferred),
            "clock_source": clock_source,
            "sequence": self._next_settings_sequence() if sequence is None else sequence,
        }
        self._with_host_mac(spec, host_mac)
        return self._settings(spec)

    def command_probe_preferred_leader(self, clock_source: int = 0, host_mac=None, sequence=None):
        spec = self._with_host_mac(
            {
                "command": "probe_preferred_leader",
                "clock_source": clock_source,
                "sequence": self._next_settings_sequence() if sequence is None else sequence,
            },
            host_mac,
        )
        return self._settings(spec)

    def command_refresh_clock_status(self, host_mac=None, sequence=None):
        spec = self._with_host_mac(
            {
                "command": "refresh_clock_status",
                "sequence": self._next_settings_sequence() if sequence is None else sequence,
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
                "sequence": self._next_settings_sequence() if sequence is None else sequence,
                "host_mac": _mac_to_hex(self._resolve_host_mac(host_mac)),
            }
        )

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
