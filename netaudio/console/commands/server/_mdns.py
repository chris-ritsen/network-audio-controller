import asyncio
import ipaddress
import json
import logging
import os
import random
import signal
import socket
import struct
import sys
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from json import JSONEncoder
from queue import Empty, Queue
from threading import Event, Thread

import typer
from redis import Redis
from redis.exceptions import ConnectionError as RedisConnectionError
from sqlitedict import SqliteDict

from netaudio.common.app_config import settings as app_settings
from netaudio.dante.browser import DanteBrowser
from netaudio.dante.const import (
    DEFAULT_MULTICAST_METERING_PORT,
    DEVICE_CONTROL_PORT,
    DEVICE_HEARTBEAT_PORT,
    DEVICE_INFO_PORT,
    DEVICE_INFO_SRC_PORT2,
    DEVICE_SETTINGS_PORT,
    MESSAGE_TYPE_ACCESS_STATUS,
    MESSAGE_TYPE_AES67_STATUS,
    MESSAGE_TYPE_AUDIO_INTERFACE_STATUS,
    MESSAGE_TYPE_CHANGE,
    MESSAGE_TYPE_CHANNEL_COUNTS_QUERY,
    MESSAGE_TYPE_CLEAR_CONFIG_STATUS,
    MESSAGE_TYPE_CLOCKING_STATUS,
    MESSAGE_TYPE_CODEC_STATUS,
    MESSAGE_TYPE_CONTROL,
    MESSAGE_TYPE_ENCODING_STATUS,
    MESSAGE_TYPE_IFSTATS_STATUS,
    MESSAGE_TYPE_INTERFACE_STATUS,
    MESSAGE_TYPE_LOCK_STATUS,
    MESSAGE_TYPE_MANF_VERSIONS_STATUS,
    MESSAGE_TYPE_MONITORING_STRINGS,
    MESSAGE_TYPE_NAME_QUERY,
    MESSAGE_TYPE_PROPERTY_CHANGE,
    MESSAGE_TYPE_QUERY,
    MESSAGE_TYPE_ROUTING_DEVICE_CHANGE,
    MESSAGE_TYPE_ROUTING_READY,
    MESSAGE_TYPE_RX_CHANNEL_CHANGE,
    MESSAGE_TYPE_RX_CHANNEL_QUERY,
    MESSAGE_TYPE_RX_FLOW_CHANGE,
    MESSAGE_TYPE_SAMPLE_RATE_PULLUP_STATUS,
    MESSAGE_TYPE_SAMPLE_RATE_STATUS,
    MESSAGE_TYPE_STATUS,
    MESSAGE_TYPE_STRINGS,
    MESSAGE_TYPE_SWITCH_VLAN_STATUS,
    MESSAGE_TYPE_TX_CHANNEL_FRIENDLY_NAMES_QUERY,
    MESSAGE_TYPE_TX_CHANNEL_QUERY,
    MESSAGE_TYPE_TX_FLOW_CHANGE,
    MESSAGE_TYPE_UNICAST_CLOCKING_STATUS,
    MESSAGE_TYPE_UPGRADE_STATUS,
    MESSAGE_TYPE_VERSIONS_STATUS,
    MESSAGE_TYPE_VOLUME_LEVELS,
    MESSAGE_TYPES,
    MULTICAST_GROUP_CONTROL_MONITORING,
    MULTICAST_GROUP_HEARTBEAT,
    PORTS,
    SERVICE_ARC,
    SERVICE_CHAN,
    SERVICE_CMC,
    SERVICE_DBC,
    SERVICES,
    SUBSCRIPTION_STATUS_LABELS,
    SUBSCRIPTION_STATUS_NONE,
)
from netaudio.dante.device import DanteDevice


def _default(self, obj):
    return getattr(obj.__class__, "to_json", _default.default)(obj)


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

_default.default = JSONEncoder().default
JSONEncoder.default = _default

sockets = {}
redis_client = None

redis_socket_path = os.environ.get("REDIS_SOCKET")
redis_host = os.environ.get("REDIS_HOST") or "localhost"
redis_port = os.environ.get("REDIS_PORT") or 6379
redis_db = os.environ.get("REDIS_DB") or 0

try:
    if redis_socket_path:
        redis_client = Redis(
            db=redis_db,
            decode_responses=False,
            socket_timeout=0.1,
            unix_socket_path=redis_socket_path,
        )
    elif os.environ.get("REDIS_PORT") or os.environ.get("REDIS_HOST"):
        redis_client = Redis(
            db=redis_db,
            decode_responses=False,
            host=redis_host,
            socket_timeout=0.1,
            port=redis_port,
        )
    if redis_client:
        redis_client.ping()
except RedisConnectionError:
    redis_client = None


def get_name_lengths(device_name):
    name_len = len(device_name)
    offset = (name_len & 1) - 2
    padding = 10 - (name_len + offset)
    name_len1 = (len(device_name) * 2) + padding
    name_len2 = name_len1 + 2
    name_len3 = name_len2 + 4

    return (name_len1, name_len2, name_len3)


def volume_level_query(device_name, ipv4, mac, port, timeout=True):
    data_len = 0
    device_name_hex = device_name.encode().hex()
    ip_hex = ipv4.packed.hex()

    name_len1, name_len2, name_len3 = get_name_lengths(device_name)

    if len(device_name) % 2 == 0:
        device_name_hex = f"{device_name_hex}00"

    if len(device_name) < 2:
        data_len = 54
    elif len(device_name) < 4:
        data_len = 56
    else:
        data_len = len(device_name) + (len(device_name) & 1) + 54

    unknown_arg = "16"
    message_hex = f"120000{data_len:02x}ffff301000000000{mac}0000000400{name_len1:02x}000100{name_len2:02x}000a{device_name_hex}{unknown_arg}0001000100{name_len3:02x}0001{port:04x}{timeout:04x}0000{ip_hex}{port:04x}0000"

    return bytes.fromhex(message_hex)


def parse_volume_level_status(message, server_name):
    redis_device_key = ":".join(["netaudio", "dante", "device", server_name])
    cached_device = redis_decode(redis_client.hgetall(redis_device_key))
    volume_levels = {"rx": {}, "tx": {}}
    rx_channel_count_raw = tx_channel_count_raw = None

    if "rx_channel_count" in cached_device:
        rx_channel_count_raw = int(cached_device["rx_channel_count"])

    if "tx_channel_count" in cached_device:
        tx_channel_count_raw = int(cached_device["tx_channel_count"])

    if not rx_channel_count_raw and not tx_channel_count_raw:
        print(f"Need channel counts to parse this request for {server_name}")
        return volume_levels

    dante_message = bytes.fromhex(message["message_hex"])
    rx_channels = dante_message[-1 - rx_channel_count_raw : -1]
    tx_channels = dante_message[
        -1 - rx_channel_count_raw - tx_channel_count_raw : -1 - rx_channel_count_raw
    ]

    for index in range(0, rx_channel_count_raw - 1):
        volume_levels["rx"][index + 1] = rx_channels[index]

    for index in range(0, tx_channel_count_raw - 1):
        volume_levels["tx"][index + 1] = tx_channels[index]

    return volume_levels


def parse_message_type_access_status(message):
    return {"access_status": None}


def parse_message_type_codec_status(message):
    return {"codec_status": None}


def parse_message_type_upgrade_status(message):
    return {"upgrade_status": None}


def parse_message_type_switch_vlan_status(message):
    return {"switch_vlan_status": None}


def parse_message_type_sample_rate_pullup_status(message):
    return {"sample_rate_pullup_status": None}


def parse_message_type_clear_config_status(message):
    return {"clear_config_status": None}


def parse_message_type_encoding_status(message):
    return {"encoding_status": None}


def parse_message_type_sample_rate_status(message):
    return {"sample_rate_status": None}


def parse_message_type_aes67_status(message):
    return {"aes67_status": None}


def parse_message_type_lock_status(message):
    return {"lock_status": None}


def parse_message_type_clocking_status(message):
    return {"clocking_status": None}


def parse_message_type_interface_status(message):
    return {"interface_status": None}


def parse_message_type_versions_status(message):
    model = message[88:].partition(b"\x00")[0].decode("utf-8")
    model_id = message[43:].partition(b"\x00")[0].decode("utf-8").replace("\u0003", "")

    return {
        "model": model,
        "model_id": model_id,
    }


def parse_message_type_manf_versions_status(message):
    manufacturer = message[76:].partition(b"\x00")[0].decode("utf-8")
    model = message[204:].partition(b"\x00")[0].decode("utf-8")

    return {
        "manufacturer": manufacturer,
        "model": model,
    }


def parse_message_type_audio_interface_status(message):
    return {"audio_interface_status": None}


def parse_message_type_ifstats_status(message):
    return {"ifstats_status": None}


def parse_message_type_routing_ready(message):
    return {"routing_ready": None}


def parse_message_type_tx_flow_change(message):
    return {"tx_flow_change": None}


def parse_message_type_unicast_clocking_status(message):
    return {"unicast_clocking_status": None}


def cache_device_value_json(server_name, key, value):
    redis_device_key = ":".join(["netaudio", "dante", "device", server_name])
    redis_client.hset(
        redis_device_key,
        key=None,
        value=None,
        mapping={
            key: json.dumps(value, indent=2),
        },
    )


def cache_device_value(server_name, key, value):
    redis_device_key = ":".join(["netaudio", "dante", "device", server_name])
    redis_client.hset(
        redis_device_key,
        key=None,
        value=None,
        mapping={
            key: value,
        },
    )


def redis_decode(cached_dict):
    return {
        key.decode("utf-8"): value.decode("utf-8") for key, value in cached_dict.items()
    }


def parse_dante_message(message):
    dante_message = bytes.fromhex(message["message_hex"])
    parsed_dante_message = {}

    src_host = message["src_host"]
    src_port = message["src_port"]
    timestamp = message["time"]
    server_name = None

    if "multicast_group" in message:
        multicast_group = message["multicast_group"]

    if "multicast_port" in message:
        multicast_port = message["multicast_port"]

    message_type = int.from_bytes(dante_message[26:28], "big")

    cached_host = redis_decode(
        redis_client.hgetall(":".join(["netaudio", "dante", "host", src_host]))
    )

    # Message was not parsed: 192.168.1.37:1064 -> 224.0.0.231:8702 type `224` (Metering Status) from `AD4D-fd4e13.local.`

    parsed_message = {
        "message": dante_message,
        "message_type": str(message_type),
        "parsed_message": parsed_dante_message,
        "src_host": src_host,
        "src_port": src_port,
        "time": timestamp,
    }

    parsed_message_redis_hash = {
        "message": dante_message,
        "message_type": str(message_type),
        "src_host": src_host,
        "src_port": src_port,
        "time": timestamp,
    }

    if "server_name" in cached_host:
        server_name = cached_host["server_name"]
    else:
        parsed_message["error"] = "Could not find server name for cached host"
        print(parsed_message["error"])
        return parsed_message

    if (
        message_type == MESSAGE_TYPE_AUDIO_INTERFACE_STATUS
        and multicast_group == MULTICAST_GROUP_CONTROL_MONITORING
        and multicast_port == DEVICE_INFO_PORT
    ):
        parsed_dante_message = parse_message_type_audio_interface_status(dante_message)
        parsed_message_redis_hash["message_type_string"] = parsed_message[
            "message_type_string"
        ] = MESSAGE_TYPE_STRINGS[message_type]
    elif (
        message_type == MESSAGE_TYPE_ACCESS_STATUS
        and multicast_group == MULTICAST_GROUP_CONTROL_MONITORING
        and multicast_port == DEVICE_INFO_PORT
    ):
        access_status = parse_message_type_access_status(dante_message)
        parsed_message_redis_hash["message_type_string"] = parsed_message[
            "message_type_string"
        ] = MESSAGE_TYPE_STRINGS[message_type]
    elif (
        message_type == MESSAGE_TYPE_ROUTING_READY
        and multicast_group == MULTICAST_GROUP_CONTROL_MONITORING
        and multicast_port == DEVICE_INFO_PORT
    ):
        parsed_dante_message = parse_message_type_routing_ready(dante_message)
        parsed_message_redis_hash["message_type_string"] = parsed_message[
            "message_type_string"
        ] = MESSAGE_TYPE_STRINGS[message_type]
    elif (
        message_type == MESSAGE_TYPE_TX_FLOW_CHANGE
        and multicast_group == MULTICAST_GROUP_CONTROL_MONITORING
        and multicast_port == DEVICE_INFO_PORT
    ):
        parsed_dante_message = parse_message_type_tx_flow_change(dante_message)
        parsed_message_redis_hash["message_type_string"] = parsed_message[
            "message_type_string"
        ] = MESSAGE_TYPE_STRINGS[message_type]

    elif (
        message_type == MESSAGE_TYPE_UNICAST_CLOCKING_STATUS
        and multicast_group == MULTICAST_GROUP_CONTROL_MONITORING
        and multicast_port == DEVICE_INFO_PORT
    ):
        parsed_dante_message = parse_message_type_unicast_clocking_status(dante_message)
        parsed_message_redis_hash["message_type_string"] = parsed_message[
            "message_type_string"
        ] = MESSAGE_TYPE_STRINGS[message_type]
    elif (
        message_type == MESSAGE_TYPE_IFSTATS_STATUS
        and multicast_group == MULTICAST_GROUP_CONTROL_MONITORING
        and multicast_port == DEVICE_INFO_PORT
    ):
        parsed_dante_message = parse_message_type_ifstats_status(dante_message)
        parsed_message_redis_hash["message_type_string"] = parsed_message[
            "message_type_string"
        ] = MESSAGE_TYPE_STRINGS[message_type]
    elif (
        message_type == MESSAGE_TYPE_VERSIONS_STATUS
        and multicast_group == MULTICAST_GROUP_CONTROL_MONITORING
        and multicast_port == DEVICE_INFO_PORT
    ):
        parsed_dante_message = parse_message_type_versions_status(dante_message)
        parsed_message_redis_hash["message_type_string"] = parsed_message[
            "message_type_string"
        ] = MESSAGE_TYPE_STRINGS[message_type]
        # print(
        #     f"{src_host}:{src_port} -> {multicast_group}:{multicast_port} type `{message_type}` ({MESSAGE_TYPE_STRINGS[message_type]}) from `{server_name}`"
        # )
    elif (
        message_type == MESSAGE_TYPE_MANF_VERSIONS_STATUS
        and multicast_group == MULTICAST_GROUP_CONTROL_MONITORING
        and multicast_port == DEVICE_INFO_PORT
    ):
        parsed_dante_message = parse_message_type_manf_versions_status(dante_message)
        parsed_message_redis_hash["message_type_string"] = parsed_message[
            "message_type_string"
        ] = MESSAGE_TYPE_STRINGS[message_type]
        # print(
        #     f"{src_host}:{src_port} -> {multicast_group}:{multicast_port} type `{message_type}` ({MESSAGE_TYPE_STRINGS[message_type]}) from `{server_name}`"
        # )
    elif message_type == MESSAGE_TYPE_PROPERTY_CHANGE:
        pass
    elif (
        multicast_group == MULTICAST_GROUP_CONTROL_MONITORING
        and multicast_port == DEFAULT_MULTICAST_METERING_PORT
    ):
        volume_levels = parse_volume_level_status(message, server_name)
        parsed_message_redis_hash["message_type_string"] = parsed_message[
            "message_type_string"
        ] = MESSAGE_TYPE_MONITORING_STRINGS[MESSAGE_TYPE_VOLUME_LEVELS]

        # print(
        #     f"{src_host}:{src_port} -> {multicast_group}:{multicast_port} ({MESSAGE_TYPE_MONITORING_STRINGS[MESSAGE_TYPE_VOLUME_LEVELS]}) from `{server_name}`"
        # )
        cache_device_value_json(server_name, "rx_volume_levels", volume_levels["rx"])
        cache_device_value_json(server_name, "tx_volume_levels", volume_levels["tx"])
    elif (
        message_type == MESSAGE_TYPE_SAMPLE_RATE_PULLUP_STATUS
        and multicast_group == MULTICAST_GROUP_CONTROL_MONITORING
        and multicast_port == DEVICE_INFO_PORT
    ):
        sample_rate_pullup_status = parse_message_type_sample_rate_pullup_status(
            dante_message
        )
        parsed_message_redis_hash["message_type_string"] = parsed_message[
            "message_type_string"
        ] = MESSAGE_TYPE_STRINGS[message_type]
        # print(
        #     f"{src_host}:{src_port} -> {multicast_group}:{multicast_port} type `{message_type}` ({MESSAGE_TYPE_STRINGS[message_type]}) from `{server_name}`"
        # )
        # print(sample_rate_pullup_status)
        cache_device_value_json(
            server_name,
            "sample_rate_pullup_status",
            sample_rate_pullup_status["sample_rate_pullup_status"],
        )
    elif (
        message_type == MESSAGE_TYPE_ENCODING_STATUS
        and multicast_group == MULTICAST_GROUP_CONTROL_MONITORING
        and multicast_port == DEVICE_INFO_PORT
    ):
        encoding_status = parse_message_type_encoding_status(dante_message)
        parsed_message_redis_hash["message_type_string"] = parsed_message[
            "message_type_string"
        ] = MESSAGE_TYPE_STRINGS[message_type]
        # print(
        #     f"{src_host}:{src_port} -> {multicast_group}:{multicast_port} type `{message_type}` ({MESSAGE_TYPE_STRINGS[message_type]}) from `{server_name}`"
        # )
        # print(encoding_status)
        cache_device_value_json(
            server_name, "encoding_status", encoding_status["encoding_status"]
        )
    elif (
        message_type == MESSAGE_TYPE_CLEAR_CONFIG_STATUS
        and multicast_group == MULTICAST_GROUP_CONTROL_MONITORING
        and multicast_port == DEVICE_INFO_PORT
    ):
        clear_config_status = parse_message_type_clear_config_status(dante_message)
        parsed_message_redis_hash["message_type_string"] = parsed_message[
            "message_type_string"
        ] = MESSAGE_TYPE_STRINGS[message_type]
        # print(
        #     f"{src_host}:{src_port} -> {multicast_group}:{multicast_port} type `{message_type}` ({MESSAGE_TYPE_STRINGS[message_type]}) from `{server_name}`"
        # )
        # print(clear_config_status)
        cache_device_value_json(
            server_name,
            "clear_config_status",
            clear_config_status["clear_config_status"],
        )
    elif (
        message_type == MESSAGE_TYPE_SAMPLE_RATE_STATUS
        and multicast_group == MULTICAST_GROUP_CONTROL_MONITORING
        and multicast_port == DEVICE_INFO_PORT
    ):
        sample_rate_status = parse_message_type_sample_rate_status(dante_message)
        parsed_message_redis_hash["message_type_string"] = parsed_message[
            "message_type_string"
        ] = MESSAGE_TYPE_STRINGS[message_type]
        # print(
        #     f"{src_host}:{src_port} -> {multicast_group}:{multicast_port} type `{message_type}` ({MESSAGE_TYPE_STRINGS[message_type]}) from `{server_name}`"
        # )
        # print(sample_rate_status)
        cache_device_value_json(
            server_name, "sample_rate_status", sample_rate_status["sample_rate_status"]
        )
    elif (
        message_type == MESSAGE_TYPE_SWITCH_VLAN_STATUS
        and multicast_group == MULTICAST_GROUP_CONTROL_MONITORING
        and multicast_port == DEVICE_INFO_PORT
    ):
        switch_vlan_status = parse_message_type_switch_vlan_status(dante_message)
        parsed_message_redis_hash["message_type_string"] = parsed_message[
            "message_type_string"
        ] = MESSAGE_TYPE_STRINGS[message_type]
        # print(
        #     f"{src_host}:{src_port} -> {multicast_group}:{multicast_port} type `{message_type}` ({MESSAGE_TYPE_STRINGS[message_type]}) from `{server_name}`"
        # )
        # print(switch_vlan_status)
        cache_device_value_json(
            server_name, "switch_vlan_status", switch_vlan_status["switch_vlan_status"]
        )
    elif (
        message_type == MESSAGE_TYPE_UPGRADE_STATUS
        and multicast_group == MULTICAST_GROUP_CONTROL_MONITORING
        and multicast_port == DEVICE_INFO_PORT
    ):
        upgrade_status = parse_message_type_upgrade_status(dante_message)
        parsed_message_redis_hash["message_type_string"] = parsed_message[
            "message_type_string"
        ] = MESSAGE_TYPE_STRINGS[message_type]
        # print(
        #     f"{src_host}:{src_port} -> {multicast_group}:{multicast_port} type `{message_type}` ({MESSAGE_TYPE_STRINGS[message_type]}) from `{server_name}`"
        # )
        # print(upgrade_status)
        cache_device_value_json(
            server_name, "upgrade_status", upgrade_status["upgrade_status"]
        )
    elif (
        message_type == MESSAGE_TYPE_INTERFACE_STATUS
        and multicast_group == MULTICAST_GROUP_CONTROL_MONITORING
        and multicast_port == DEVICE_INFO_PORT
    ):
        interface_status = parse_message_type_interface_status(dante_message)
        parsed_message_redis_hash["message_type_string"] = parsed_message[
            "message_type_string"
        ] = MESSAGE_TYPE_STRINGS[message_type]
        # print(
        #     f"{src_host}:{src_port} -> {multicast_group}:{multicast_port} type `{message_type}` ({MESSAGE_TYPE_STRINGS[message_type]}) from `{server_name}`"
        # )
        # print(interface_status)
        cache_device_value_json(
            server_name, "interface_status", interface_status["interface_status"]
        )
    elif (
        message_type == MESSAGE_TYPE_CLOCKING_STATUS
        and multicast_group == MULTICAST_GROUP_CONTROL_MONITORING
        and multicast_port == DEVICE_INFO_PORT
    ):
        clocking_status = parse_message_type_clocking_status(dante_message)
        parsed_message_redis_hash["message_type_string"] = parsed_message[
            "message_type_string"
        ] = MESSAGE_TYPE_STRINGS[message_type]
        # print(
        #     f"{src_host}:{src_port} -> {multicast_group}:{multicast_port} type `{message_type}` ({MESSAGE_TYPE_STRINGS[message_type]}) from `{server_name}`"
        # )
        cache_device_value_json(
            server_name, "clocking_status", clocking_status["clocking_status"]
        )
    elif (
        multicast_group == MULTICAST_GROUP_CONTROL_MONITORING
        and src_port in [DEVICE_SETTINGS_PORT, DEVICE_INFO_SRC_PORT2]
        and message_type
        in [
            MESSAGE_TYPE_ROUTING_DEVICE_CHANGE,
            MESSAGE_TYPE_RX_CHANNEL_CHANGE,
            MESSAGE_TYPE_RX_FLOW_CHANGE,
        ]
    ):
        print("Rx change for", server_name, message_type)
        parsed_message_redis_hash["message_type_string"] = parsed_message[
            "message_type_string"
        ] = MESSAGE_TYPE_STRINGS[message_type]
        parsed_rx_channels = get_rx_channels(server_name)
        cache_device_value_json(
            server_name, "rx_channels", parsed_rx_channels["rx_channels"]
        )
        cache_device_value_json(
            server_name, "subscriptions", parsed_rx_channels["subscriptions"]
        )
    elif (
        message_type == MESSAGE_TYPE_LOCK_STATUS
        and multicast_group == MULTICAST_GROUP_CONTROL_MONITORING
        and multicast_port == DEVICE_INFO_PORT
    ):
        lock_status = parse_message_type_lock_status(dante_message)
        parsed_message_redis_hash["message_type_string"] = parsed_message[
            "message_type_string"
        ] = MESSAGE_TYPE_STRINGS[message_type]
        # print(
        #     f"{src_host}:{src_port} -> {multicast_group}:{multicast_port} type `{message_type}` ({MESSAGE_TYPE_STRINGS[message_type]}) from `{server_name}`"
        # )
        cache_device_value_json(server_name, "lock_status", lock_status["lock_status"])
    elif (
        message_type == MESSAGE_TYPE_CODEC_STATUS
        and multicast_group == MULTICAST_GROUP_CONTROL_MONITORING
        and multicast_port == DEVICE_INFO_PORT
    ):
        codec_status = parse_message_type_codec_status(dante_message)
        parsed_message_redis_hash["message_type_string"] = parsed_message[
            "message_type_string"
        ] = MESSAGE_TYPE_STRINGS[message_type]
        # print(
        #     f"{src_host}:{src_port} -> {multicast_group}:{multicast_port} type `{message_type}` ({MESSAGE_TYPE_STRINGS[message_type]}) from `{server_name}`"
        # )
        cache_device_value_json(
            server_name, "codec_status", codec_status["codec_status"]
        )
    elif (
        message_type == MESSAGE_TYPE_AES67_STATUS
        and multicast_group == MULTICAST_GROUP_CONTROL_MONITORING
        and multicast_port == DEVICE_INFO_PORT
    ):
        aes67_status = parse_message_type_aes67_status(dante_message)
        parsed_message_redis_hash["message_type_string"] = parsed_message[
            "message_type_string"
        ] = MESSAGE_TYPE_STRINGS[message_type]
        # print(
        #     f"{src_host}:{src_port} -> {multicast_group}:{multicast_port} type `{message_type}` ({MESSAGE_TYPE_STRINGS[message_type]}) from `{server_name}`"
        # )
        cache_device_value_json(
            server_name, "aes67_status", aes67_status["aes67_status"]
        )
    elif (
        multicast_group == MULTICAST_GROUP_CONTROL_MONITORING
        and src_port in [DEVICE_SETTINGS_PORT, DEVICE_INFO_SRC_PORT2]
        and message_type
        in [
            MESSAGE_TYPE_ROUTING_DEVICE_CHANGE,
            MESSAGE_TYPE_RX_CHANNEL_CHANGE,
            MESSAGE_TYPE_RX_FLOW_CHANGE,
        ]
    ):
        print("Rx change for", server_name, message_type)
        parsed_message_redis_hash["message_type_string"] = parsed_message[
            "message_type_string"
        ] = MESSAGE_TYPE_STRINGS[message_type]
        parsed_rx_channels = get_rx_channels(server_name)
        cache_device_value_json(
            server_name, "rx_channels", parsed_rx_channels["rx_channels"]
        )
        cache_device_value_json(
            server_name, "subscriptions", parsed_rx_channels["subscriptions"]
        )
    else:
        if message_type in MESSAGE_TYPE_STRINGS:
            parsed_message_redis_hash["message_type_string"] = parsed_message[
                "message_type_string"
            ] = MESSAGE_TYPE_STRINGS[message_type]
        else:
            # print(f"Unknown message type: {message_type} from `{server_name}`")
            parsed_message_redis_hash["message_type_string"] = parsed_message[
                "message_type_string"
            ] = "Unknown"

        # print(
        #     f"Message was not parsed: {src_host}:{src_port} -> {multicast_group}:{multicast_port} type `{message_type}` ({parsed_message['message_type_string']}) from `{server_name}`"
        # )

    # if parsed_dante_message:
    #     redis_device_key = ":".join(["netaudio", "dante", "device", server_name])
    #     for key in parsed_dante_message.items():
    #         print(
    #             {
    #                 key: json.dumps(parsed_dante_message, indent=2),
    #             }
    #         )
    #         redis_client.hset(
    #             redis_device_key,
    #             key=None,
    #             value=None,
    #             mapping={
    #                 key: json.dumps(parsed_dante_message[key], indent=2),
    #             },
    #         )

    parsed_message["parsed_message"] = parsed_dante_message
    parsed_message_redis_hash["parsed_message"] = json.dumps(
        parsed_dante_message, indent=2
    )

    if multicast_group:
        parsed_message["multicast_group"] = parsed_message_redis_hash[
            "multicast_group"
        ] = multicast_group

    if multicast_port:
        parsed_message["multicast_port"] = parsed_message_redis_hash[
            "multicast_port"
        ] = multicast_port

    # redis_message_key = ":".join(
    #     ["netaudio", "dante", "device", "message", "received", src_host, str(timestamp)]
    # )
    # redis_client.hset(
    #     redis_message_key,
    #     key=None,
    #     value=None,
    #     mapping=parsed_message_redis_hash,
    # )

    # if parsed_message["parsed_message"]:
    #     print(parsed_message["parsed_message"])

    # cached_message = redis_client.hgetall(redis_message_key)
    # cached_device = redis_client.hgetall(redis_device_key)
    # print("cached device:", cached_device)
    # print("cached:", cached_message)
    #
    # if multicast_group and multicast_port
    #     print(
    #         f"{src_host}:{src_port} -> {multicast_group}:{multicast_port}\n  {MESSAGE_TYPE_STRINGS[message_type]}\n  {dante_message.hex()}"
    #     )

    return parsed_message


def message_channel_counts_query():
    message_length = 10
    message_type = MESSAGE_TYPE_CHANNEL_COUNTS_QUERY
    flags = 0
    sequence_id1 = random.randint(0, 255)
    sequence_id2 = random.randint(0, 65535)
    message_hex = f"27{sequence_id1:02x}{message_length:04x}{sequence_id2:04x}{message_type:04x}{flags:04x}"

    return bytes.fromhex(message_hex)


def message_device_name_query():
    message_length = 10
    message_type = MESSAGE_TYPE_NAME_QUERY
    flags = 0
    sequence_id1 = random.randint(0, 255)
    sequence_id2 = random.randint(0, 65535)
    message_hex = f"27{sequence_id1:02x}{message_length:04x}{sequence_id2:04x}{message_type:04x}{flags:04x}"

    return bytes.fromhex(message_hex)


def message_rx_channels_query(page):
    flags = channel_pagination(page)
    message_length = 16
    message_type = MESSAGE_TYPE_RX_CHANNEL_QUERY
    sequence_id1 = random.randint(0, 255)
    sequence_id2 = random.randint(0, 65535)
    message_hex = f"27{sequence_id1:02x}{message_length:04x}{sequence_id2:04x}{message_type:04x}{flags}"

    return bytes.fromhex(message_hex)


def message_tx_channels_friendly_names_query(page):
    flags = channel_pagination(page)
    message_length = 16
    message_type = MESSAGE_TYPE_TX_CHANNEL_FRIENDLY_NAMES_QUERY
    sequence_id1 = random.randint(0, 255)
    sequence_id2 = random.randint(0, 65535)
    message_hex = f"27{sequence_id1:02x}{message_length:04x}{sequence_id2:04x}{message_type:04x}{flags}"

    return bytes.fromhex(message_hex)


def message_tx_channels_query(page):
    flags = channel_pagination(page)
    message_length = 16
    message_type = MESSAGE_TYPE_TX_CHANNEL_QUERY
    sequence_id1 = random.randint(0, 255)
    sequence_id2 = random.randint(0, 65535)
    message_hex = f"27{sequence_id1:02x}{message_length:04x}{sequence_id2:04x}{message_type:04x}{flags}"

    return bytes.fromhex(message_hex)


def parse_message_type_name_query(message):
    device_name = message[10:-1].decode("utf-8")

    return {
        "name": device_name,
    }


def parse_message_type_channel_counts_query(message):
    rx_count = int.from_bytes(message[15:16], "big")
    tx_count = int.from_bytes(message[13:14], "big")

    return {
        "rx_channel_count": rx_count,
        "tx_channel_count": tx_count,
    }


def get_label(hex_str, offset):
    parsed_get_label = None

    try:
        hex_substring = hex_str[offset * 2 :]
        partitioned_bytes = bytes.fromhex(hex_substring).partition(b"\x00")[0]
        parsed_get_label = partitioned_bytes.decode("utf-8")
    except Exception:
        pass
        #  traceback.print_exc()

    return parsed_get_label


def parse_message_type_tx_channel_friendly_names_query(
    message, name, tx_count, sample_rate
):
    tx_channels_friendly_names = {}
    tx_friendly_names = message.hex()

    for index in range(0, min(tx_count, 32)):
        str1 = tx_friendly_names[(24 + (index * 12)) : (36 + (index * 12))]
        n = 4
        channel = [str1[i : i + 4] for i in range(0, len(str1), n)]
        channel_number = int(channel[1], 16)
        channel_offset = channel[2]
        tx_channel_friendly_name = get_label(tx_friendly_names, channel_offset)

        if tx_channel_friendly_name:
            tx_channels_friendly_names[channel_number] = tx_channel_friendly_name

    return {"tx_channels_friendly_names": tx_channels_friendly_names}


def parse_message_type_tx_channel_query(message, name, tx_count, sample_rate):
    # has_disabled_channels = False
    tx_channels = {}
    transmitters = message.hex()

    # if sample_rate:
    #     has_disabled_channels = transmitters.count(f"{sample_rate:06x}") == 2

    # first_channel = []

    for index in range(0, min(tx_count, 32)):
        str1 = transmitters[(24 + (index * 16)) : (40 + (index * 16))]
        n = 4
        channel = [str1[i : i + 4] for i in range(0, len(str1), n)]

        # if index == 0:
        #     first_channel = channel

        if channel:
            o1 = (int(channel[2], 16) * 2) + 2
            o2 = o1 + 6
            sample_rate_hex = transmitters[o1:o2]

            if sample_rate_hex != "000000":
                sample_rate = int(sample_rate_hex, 16)

            channel_number = int(channel[0], 16)
            #  channel_status = channel[1][2:]
            # channel_group = channel[2]
            channel_offset = int(channel[3], 16)

            # channel_enabled = channel_group == first_channel[2]
            # channel_disabled = channel_group != first_channel[2]

            # if channel_disabled:
            #     break

            tx_channel_name = get_label(transmitters, channel_offset)

            if tx_channel_name is None or channel_number == 0:
                break

            tx_channel = {}
            tx_channel["channel_type"] = "tx"
            tx_channel["number"] = channel_number
            tx_channel["device"] = name
            tx_channel["name"] = tx_channel_name

            # if channel_number in tx_friendly_channel_names:
            #     tx_channel.friendly_name = tx_friendly_channel_names[channel_number]

            tx_channels[channel_number] = tx_channel

    # if has_disabled_channels:
    #     break

    return {"tx_channels": tx_channels}


def parse_message_type_rx_channel_query(message, name, rx_count):
    hex_rx_response = message.hex()
    rx_channels = {}
    subscriptions = {}

    for index in range(0, min(rx_count, 16)):
        n = 4
        str1 = hex_rx_response[(24 + (index * 40)) : (56 + (index * 40))]
        channel = [str1[i : i + n] for i in range(0, len(str1), n)]

        channel_number = int(channel[0], 16)
        channel_offset = int(channel[3], 16)
        device_offset = int(channel[4], 16)
        rx_channel_offset = int(channel[5], 16)
        rx_channel_status_code = int(channel[6], 16)
        subscription_status_code = int(channel[7], 16)

        rx_channel_name = get_label(hex_rx_response, rx_channel_offset)
        tx_device_name = get_label(hex_rx_response, device_offset)

        if channel_offset != 0:
            tx_channel_name = get_label(hex_rx_response, channel_offset)
        else:
            tx_channel_name = rx_channel_name

        channel_status_text = None
        subscription = {}
        rx_channel = {}

        rx_channel["channel_type"] = "rx"
        rx_channel["device_name"] = name
        rx_channel["name"] = rx_channel_name
        rx_channel["number"] = channel_number
        rx_channel["status_code"] = rx_channel_status_code

        if channel_status_text:
            rx_channel["status_text"] = channel_status_text

        rx_channels[channel_number] = rx_channel

        subscription["rx_channel_name"] = rx_channel_name
        subscription["rx_channel_number"] = channel_number
        subscription["rx_device_name"] = name

        subscription["status_code"] = subscription_status_code
        subscription["rx_channel_status_code"] = rx_channel_status_code

        if rx_channel_status_code in SUBSCRIPTION_STATUS_LABELS:
            subscription["rx_channel_status_text"] = SUBSCRIPTION_STATUS_LABELS[
                rx_channel_status_code
            ]

        if subscription_status_code == SUBSCRIPTION_STATUS_NONE:
            subscription["tx_device_name"] = None
            subscription["tx_channel_name"] = None
        elif tx_device_name == ".":
            subscription["tx_channel_name"] = tx_channel_name
            subscription["tx_device_name"] = name
        else:
            subscription["tx_channel_name"] = tx_channel_name
            subscription["tx_device_name"] = tx_device_name

        subscription["status_message"] = SUBSCRIPTION_STATUS_LABELS[
            subscription_status_code
        ]
        subscriptions[channel_number] = subscription

    return {"rx_channels": rx_channels, "subscriptions": subscriptions}


def parse_dante_arc_message(dante_message):
    parsed_dante_message = {}

    message_type = int.from_bytes(dante_message[6:8], "big")

    if message_type == MESSAGE_TYPE_NAME_QUERY:
        parsed_dante_message = parse_message_type_name_query(dante_message)
    elif message_type == MESSAGE_TYPE_CHANNEL_COUNTS_QUERY:
        parsed_dante_message = parse_message_type_channel_counts_query(dante_message)
    else:
        print(f"Message type {message_type} was not parsed")

    return parsed_dante_message


def channel_pagination(page):
    message_args = f"0000000100{page:x}10000"

    return message_args


def get_tx_channels(server_name):
    tx_channels = {}
    # tx_channels_friendly_names = {}

    redis_service_key = ":".join(
        ["netaudio", "dante", "service", server_name, SERVICE_ARC]
    )
    cached_service = redis_decode(redis_client.hgetall(redis_service_key))
    port = int(cached_service["port"])
    sock = sockets[server_name][port]

    redis_device_key = ":".join(["netaudio", "dante", "device", server_name])

    cached_device = redis_decode(redis_client.hgetall(redis_device_key))

    if "tx_channel_count" in cached_device:
        tx_count = int(cached_device["tx_channel_count"])

    if "name" in cached_device:
        name = cached_device["name"]

    try:
        for page in range(0, max(int(tx_count / 16), 1)):
            query = message_tx_channels_query(page)
            sock.send(query)
            tx_channels_message = sock.recvfrom(2048)[0]
            parsed_tx_channels_query = parse_message_type_tx_channel_query(
                tx_channels_message, name, tx_count, None
            )
            tx_channels = tx_channels | parsed_tx_channels_query["tx_channels"]

            # query = message_tx_channels_friendly_names_query(page)
            # sock.send(query)
            # tx_channels_friendly_names_message = sock.recvfrom(2048)[0]
            # parsed_tx_channels_friendly_names_query = (
            #     parse_message_type_tx_channel_friendly_names_query(
            #         tx_channels_friendly_names_message, name, tx_count, None
            #     )
            # )
            # tx_channels_friendly_names = (
            #     tx_channels_friendly_names
            #     | parsed_tx_channels_friendly_names_query["tx_channels_friendly_names"]
            # )
    except Exception:
        traceback.print_exc()

    return {
        "tx_channels": tx_channels,
        # "tx_channels_friendly_names": tx_channels_friendly_names,
    }


def get_rx_channels(server_name):
    rx_channels = {}
    subscriptions = {}

    redis_service_key = ":".join(
        ["netaudio", "dante", "service", server_name, SERVICE_ARC]
    )
    cached_service = redis_decode(redis_client.hgetall(redis_service_key))
    port = int(cached_service["port"])
    sock = sockets[server_name][port]

    redis_device_key = ":".join(["netaudio", "dante", "device", server_name])

    cached_device = redis_decode(redis_client.hgetall(redis_device_key))

    if "rx_channel_count" in cached_device:
        rx_count = int(cached_device["rx_channel_count"])

    if "name" in cached_device:
        name = cached_device["name"]

    try:
        for page in range(0, max(int(rx_count / 16), 1)):
            query = message_rx_channels_query(page)
            sock.send(query)
            rx_channels_message = sock.recvfrom(2048)[0]
            parsed_rx_channels_query = parse_message_type_rx_channel_query(
                rx_channels_message, name, rx_count
            )
            rx_channels = rx_channels | parsed_rx_channels_query["rx_channels"]
            subscriptions = subscriptions | parsed_rx_channels_query["subscriptions"]
    except Exception:
        traceback.print_exc()

    return {
        "rx_channels": rx_channels,
        "subscriptions": subscriptions,
    }


def device_initialize_arc(server_name):
    redis_service_key = ":".join(
        ["netaudio", "dante", "service", server_name, SERVICE_ARC]
    )
    cached_service = redis_decode(redis_client.hgetall(redis_service_key))
    port = int(cached_service["port"])

    try:
        sock = sockets[server_name][port]
        sock.send(message_device_name_query())
        device_name_message = sock.recvfrom(2048)[0]
        parsed_name_query = parse_dante_arc_message(device_name_message)
        device_name = parsed_name_query["name"]

        redis_device_key = ":".join(["netaudio", "dante", "device", server_name])
        redis_client.hset(
            redis_device_key,
            key=None,
            value=None,
            mapping={
                "name": device_name,
            },
        )

        sock.send(message_channel_counts_query())
        channel_count_message = sock.recvfrom(2048)[0]
        parsed_channel_count_query = parse_dante_arc_message(channel_count_message)
        rx_count = parsed_channel_count_query["rx_channel_count"]
        tx_count = parsed_channel_count_query["tx_channel_count"]

        redis_client.hset(
            redis_device_key,
            key=None,
            value=None,
            mapping={
                "rx_channel_count": rx_count,
                "tx_channel_count": tx_count,
            },
        )

        parsed_rx_channels = get_rx_channels(server_name)
        cache_device_value_json(
            server_name, "rx_channels", parsed_rx_channels["rx_channels"]
        )
        cache_device_value_json(
            server_name, "subscriptions", parsed_rx_channels["subscriptions"]
        )

        parsed_tx_channels = get_tx_channels(server_name)
        cache_device_value_json(
            server_name, "tx_channels", parsed_tx_channels["tx_channels"]
        )

        cached_device = redis_decode(redis_client.hgetall(redis_device_key))

        rx_channels = json.loads(cached_device["rx_channels"])
        tx_channels = json.loads(cached_device["tx_channels"])

        print(f"{device_name} rx:{len(rx_channels)} tx:{len(tx_channels)}")

    except Exception:
        traceback.print_exc()

    redis_client.hset(
        redis_device_key,
        key=None,
        value=None,
        mapping={
            "device_name": device_name,
            "ipv4": cached_service["ipv4"],
            "rx_channel_count": rx_count,
            "server_name": server_name,
            "tx_channel_count": tx_count,
        },
    )

    redis_client.sadd(":".join(["netaudio", "dante", "devices"]), device_name)


def parse_dante_service_change(message):
    service = message["service"]
    server_name = service["server_name"]
    ipv4 = service["ipv4"]

    if not server_name in sockets:
        sockets[server_name] = {}

    state_change = message["state_change"]

    if state_change["name"] == "Added":
        redis_client.sadd(":".join(["netaudio", "dante", "hosts"]), service["ipv4"])
        redis_client.sadd(":".join(["netaudio", "dante", "servers"]), server_name)
        redis_client.sadd(":".join(["netaudio", "dante", "services"]), service["name"])

        for port in PORTS:
            if port in sockets[server_name]:
                continue

            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.bind(("", 0))
            sock.settimeout(0.01)
            sock.connect((ipv4, port))
            sockets[server_name][port] = sock

        redis_host_key = ":".join(["netaudio", "dante", "host", service["ipv4"]])
        redis_client.hset(
            redis_host_key,
            key=None,
            value=None,
            mapping={"ipv4": service["ipv4"], "server_name": server_name},
        )

        key = ":".join(["netaudio", "dante", "server", server_name])
        redis_client.hset(
            key,
            key=None,
            value=None,
            mapping={
                "name": server_name,
                "ipv4": ipv4,
            },
        )

        key = ":".join(["netaudio", "dante", "service", server_name, service["type"]])
        redis_client.hset(
            key,
            key=None,
            value=None,
            mapping={
                "ipv4": ipv4,
                "name": service["name"],
                "port": service["port"],
                "server_name": server_name,
                "type": service["type"],
            },
        )

        if service["properties"]:
            key = ":".join(
                [
                    "netaudio",
                    "dante",
                    "service",
                    "properties",
                    server_name,
                    service["type"],
                ]
            )
            redis_client.hset(key, key=None, value=None, mapping=service["properties"])

        if (
            not service["port"] in sockets[server_name]
            and service["type"] == SERVICE_ARC
        ):
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.bind(("", 0))
            sock.settimeout(1)
            sock.connect((ipv4, service["port"]))
            sockets[server_name][service["port"]] = sock

            device_initialize_arc(server_name)

        # print(
        #     f"Service added:\n  {service['name']}\n  {service['ipv4']}:{service['port']}"
        # )
    elif state_change["name"] == "Updated":
        pass
        # print(
        #     f"Service updated:\n  {service['name']}\n  {service['ipv4']}:{service['port']}"
        # )
    elif state_change["name"] == "Removed":
        # redis_client.srem("hosts", service["ipv4"])
        # redis_client.srem("servers", service["server_name"])
        redis_client.srem("services", service["name"])
        print(
            f"Service removed: {service['name']}\n  {service['ipv4']}:{service['port']}"
        )

    # redis_service_key = ":".join(["netaudio", "dante", "service", service["name"]])
    # cached_service = redis_client.hgetall(redis_service_key)
    # print("cached:", cached_service)


def multicast(group, port):
    from netaudio.common.app_config import settings as app_settings

    # Get the source IP for socket binding (None means use default)
    source_ip = app_settings.interface_ip or ""
    server_address = (source_ip, port)
    mc_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    try:
        mc_sock.bind(server_address)
        print(
            f"Multicast bound to {source_ip or 'default'}:{port} for group {group}",
            file=sys.stderr,
        )
    except OSError as e:
        print(e)
        print(
            f"Failed to bind to multicast port {port} on interface {source_ip or 'default'}"
        )
        return

    group_bin = socket.inet_aton(group)
    mreq = struct.pack("4sL", group_bin, socket.INADDR_ANY)
    mc_sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

    while True:
        try:
            data, addr = mc_sock.recvfrom(2048)
            timestamp = time.time_ns()

            src_host, src_port = addr

            message = {
                "message_hex": data.hex(),
                "multicast_group": group,
                "multicast_port": port,
                "src_host": src_host,
                "src_port": src_port,
                "time": timestamp,
            }

            if group == MULTICAST_GROUP_HEARTBEAT and port == DEVICE_HEARTBEAT_PORT:
                # print("heartbeat from", addr[0])

                cached_host = redis_decode(
                    redis_client.hgetall(
                        ":".join(["netaudio", "dante", "host", addr[0]])
                    )
                )

                if "server_name" in cached_host:
                    server_name = cached_host["server_name"]
                    cache_device_value(server_name, "last_seen_at", timestamp)
                    redis_device_key = ":".join(
                        ["netaudio", "dante", "device", server_name]
                    )
                    redis_client.expire(redis_device_key, 5)

                    redis_server_key = ":".join(
                        ["netaudio", "dante", "server", server_name]
                    )
                    redis_client.expire(redis_server_key, 5)

                redis_host_key = ":".join(["netaudio", "dante", "host", addr[0]])
                redis_client.expire(redis_host_key, 5)
            else:
                parse_dante_message(message)

        except Exception:
            traceback.print_exc()


class ServerMdnsDaemon:
    def __init__(self):
        self.stop_event = Event()
        self.threads = []
        # Ensure redis_client is initialized before this class is used if it's critical for constructor
        self.redis_client = redis_client  # Using the global one for now

    def _parse_services_thread_target(self, queue: Queue):
        print("MDNS Daemon: Starting service parsing thread...")
        while not self.stop_event.is_set():
            try:
                message = queue.get(timeout=1)  # Timeout to allow checking stop_event
                if message is None:  # Sentinel for shutdown
                    break
                parse_dante_service_change(message)  # Uses global redis_client
                queue.task_done()
            except Empty:
                continue
            except Exception as e:
                print(f"MDNS Daemon: Error in service parsing thread: {e}")
                traceback.print_exc()
        print("MDNS Daemon: Service parsing thread stopped.")

    def _zeroconf_browser_thread_target(self, queue: Queue):
        print("MDNS Daemon: Starting Zeroconf browser thread...")
        # DanteBrowser needs to be instantiated carefully to handle stop_event
        # For now, assuming a conceptual DanteBrowser that can be stopped.
        # The actual DanteBrowser(0, queue) and its sync_run() might need modification
        # to respect self.stop_event for graceful shutdown.
        # This is a placeholder for a more robust implementation.
        try:
            from netaudio.common.app_config import settings as app_settings

            dante_browser = DanteBrowser(
                mdns_timeout=app_settings.mdns_timeout, queue=queue
            )
            # The following is a simplified loop. Real integration needs
            # DanteBrowser to be interruptible or run in a non-blocking manner
            # that respects stop_event. Or its zc instance closed.
            # dante_browser.sync_run() # This is blocking and problematic for stop_event

            # Conceptual: If DanteBrowser had start/stop or used stop_event internally:
            # dante_browser.start_sync_browsing(self.stop_event)
            # while not self.stop_event.is_set():
            #     time.sleep(0.5)
            # dante_browser.stop_sync_browsing()

            # HACK/SIMPLIFICATION for now: run sync_run in a thread that we can't easily join nicely
            # without modifying DanteBrowser or it being designed for this.
            # This part is a known issue for graceful shutdown.
            print(
                "MDNS Daemon: Zeroconf browser thread using dante_browser.sync_run() - graceful shutdown might be an issue."
            )
            # To make it somewhat daemon-like and not block startup of other things if sync_run is short:
            # For long running, this thread won't respond to stop_event well.
            try:
                dante_browser.sync_run()  # This blocks if Zeroconf is run this way without external close
            except Exception as e:
                print(f"MDNS Daemon: Zeroconf browser thread error: {e}")

        except Exception as e:
            print(f"MDNS Daemon: Error initializing/running Zeroconf browser: {e}")
        finally:
            print("MDNS Daemon: Zeroconf browser thread finished/exited.")

    def _multicast_worker_thread_target(self, group: str, port: int):
        import sys

        from netaudio.common.app_config import settings as app_settings

        # Get the source IP for socket binding (None means use default)
        source_ip = app_settings.interface_ip or ""

        print(
            f"MDNS Daemon: Starting multicast worker for {group}:{port} on interface {source_ip or 'default'}..."
        )
        mc_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            mc_sock.bind((source_ip, port))
            group_bin = socket.inet_aton(group)
            mreq = struct.pack("4sL", group_bin, socket.INADDR_ANY)
            mc_sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
            print(
                f"MDNS Daemon: Successfully bound multicast socket on {source_ip or 'default'}:{port} for {group}",
                file=sys.stderr,
            )
            mc_sock.settimeout(1.0)  # Timeout for recvfrom

            while not self.stop_event.is_set():
                try:
                    data, addr = mc_sock.recvfrom(2048)
                    timestamp = time.time_ns()
                    src_host, src_port = addr
                    message = {
                        "message_hex": data.hex(),
                        "multicast_group": group,
                        "multicast_port": port,
                        "src_host": src_host,
                        "src_port": src_port,
                        "time": timestamp,
                    }
                    # Original logic for heartbeat processing
                    if (
                        group == MULTICAST_GROUP_HEARTBEAT
                        and port == DEVICE_HEARTBEAT_PORT
                        and self.redis_client
                    ):
                        cached_host = redis_decode(
                            self.redis_client.hgetall(f"netaudio:dante:host:{addr[0]}")
                        )
                        if "server_name" in cached_host:
                            server_name = cached_host["server_name"]
                            cache_device_value(
                                server_name, "last_seen_at", timestamp
                            )  # Uses global redis_client
                            self.redis_client.expire(
                                f"netaudio:dante:device:{server_name}", 5
                            )
                            self.redis_client.expire(
                                f"netaudio:dante:server:{server_name}", 5
                            )
                        self.redis_client.expire(f"netaudio:dante:host:{addr[0]}", 5)
                    else:
                        parse_dante_message(message)  # Uses global redis_client

                except socket.timeout:
                    continue
                except (socket.error, OSError) as e:
                    if self.stop_event.is_set():
                        break
                    print(
                        f"MDNS Daemon: Socket error in multicast worker {group}:{port}: {e}"
                    )
                    time.sleep(1)  # Avoid rapid error loops
                except Exception as e:
                    print(f"MDNS Daemon: Error in multicast worker {group}:{port}: {e}")
                    traceback.print_exc()
                    time.sleep(1)
        except OSError as e:
            print(
                f"MDNS Daemon: Failed to bind multicast socket for {group}:{port}: {e}"
            )
        finally:
            mc_sock.close()
            print(f"MDNS Daemon: Multicast worker for {group}:{port} stopped.")

    async def run_async_logic(self):
        print("MDNS Daemon: Initializing async logic...")
        if not self.redis_client:
            print(
                "MDNS Daemon: Error - Redis client not available. Exiting mdns server logic."
            )
            # Consider typer.Exit(1) if this were a direct Typer command
            return

        print("MDNS Daemon: Clearing existing netaudio:dante:* keys from Redis...")
        pattern = "netaudio:dante:*"
        try:
            for key in self.redis_client.scan_iter(match=pattern):
                self.redis_client.delete(key)
        except Exception as e:
            print(f"MDNS Daemon: Error clearing Redis keys: {e}")
            # Decide if this is fatal

        queue = Queue()
        thread_configs = [
            {
                "target": self._multicast_worker_thread_target,
                "args": (MULTICAST_GROUP_CONTROL_MONITORING, DEVICE_INFO_PORT),
            },
            {
                "target": self._multicast_worker_thread_target,
                "args": (
                    MULTICAST_GROUP_CONTROL_MONITORING,
                    DEFAULT_MULTICAST_METERING_PORT,
                ),
            },
            {
                "target": self._multicast_worker_thread_target,
                "args": (MULTICAST_GROUP_HEARTBEAT, DEVICE_HEARTBEAT_PORT),
            },
            {"target": self._parse_services_thread_target, "args": (queue,)},
            {"target": self._zeroconf_browser_thread_target, "args": (queue,)},
        ]

        for config in thread_configs:
            thread = Thread(target=config["target"], args=config["args"], daemon=True)
            self.threads.append(thread)

        print(f"MDNS Daemon: Starting {len(self.threads)} worker threads...")
        for thread in self.threads:
            thread.start()

        print("MDNS Daemon: All threads started. Monitoring for stop signal...")
        try:
            while not self.stop_event.is_set():
                await asyncio.sleep(1)  # Keep alive, check stop_event
        except (KeyboardInterrupt, SystemExit):
            print("MDNS Daemon: Stop signal received in run_async_logic.")
        finally:
            print("MDNS Daemon: Initiating shutdown sequence...")
            if not self.stop_event.is_set():
                self.stop_event.set()

            # Signal queue-based worker to stop
            if queue:
                queue.put(None)  # Sentinel value for _parse_services_thread_target

            print("MDNS Daemon: Joining threads...")
            for i, thread in enumerate(self.threads):
                if thread.is_alive():
                    print(
                        f"MDNS Daemon: Joining thread {i + 1}/{len(self.threads)} ({thread.name})..."
                    )
                    thread.join(timeout=5.0)  # Increased timeout
                    if thread.is_alive():
                        print(
                            f"MDNS Daemon: Warning - Thread {thread.name} did not terminate gracefully."
                        )
            print(
                "MDNS Daemon: All threads joined or timed out. Async logic shutdown complete."
            )


# Typer command function
def run_mdns_daemon():
    """
    Run a daemon to monitor mDNS and Dante device messages.
    This daemon listens for mDNS service advertisements (like Dante devices)
    and specific Dante multicast messages (heartbeats, control/monitoring).
    It processes these messages, potentially updating a Redis cache.
    """
    print("Initializing mDNS Daemon Typer command...")
    daemon_instance = ServerMdnsDaemon()

    # Setup signal handling for graceful shutdown
    # This might be better handled by Typer/asyncio if run directly as async,
    # but since we wrap with asyncio.run, manual setup here is an option.
    # However, the try/except KeyboardInterrupt in run_async_logic should mostly cover it.

    original_sigint_handler = signal.getsignal(signal.SIGINT)
    original_sigterm_handler = signal.getsignal(signal.SIGTERM)

    def signal_handler(sig, frame):
        print(
            f"MDNS Daemon: Signal {sig} received by Typer command handler. Triggering stop event."
        )
        daemon_instance.stop_event.set()
        # Restore original handlers to avoid issues if this is called multiple times
        # or if underlying asyncio.run has its own handlers.
        signal.signal(signal.SIGINT, original_sigint_handler)
        signal.signal(signal.SIGTERM, original_sigterm_handler)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        print("MDNS Daemon: Starting daemon's async logic...")
        asyncio.run(daemon_instance.run_async_logic())
    except Exception as e:
        print(f"MDNS Daemon: Unhandled exception in run_mdns_daemon: {e}")
        traceback.print_exc()
    finally:
        print("MDNS Daemon: Typer command run_mdns_daemon finished.")
        # Ensure original signal handlers are restored if program continues after this command
        signal.signal(signal.SIGINT, original_sigint_handler)
        signal.signal(signal.SIGTERM, original_sigterm_handler)
