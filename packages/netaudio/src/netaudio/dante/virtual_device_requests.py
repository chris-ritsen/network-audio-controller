from __future__ import annotations

import logging

from netaudio import core
from netaudio.dante.const import (
    DEVICE_SETTINGS_PORT,
    PROTOCOL_CMC,
    PROTOCOL_ID,
    RESULT_CODE_SUCCESS,
)

logger = logging.getLogger("netaudio")


class VirtualDeviceRequestHandler:
    def _handle_request(self, data: bytes, addr: tuple[str, int], *, settings_only: bool = False) -> bytes | None:
        try:
            request = core.parse_response("device_request", data)
        except core.NetaudioCoreError as error:
            if error.category != "binary_response":
                raise

            return None

        if request["family"] == "settings":
            return self._handle_settings_request(request, addr)

        if settings_only:
            return None

        if request["operation"] == "cmc_registration":
            return self._handle_cmc_registration(request["transaction_id"])

        handler = self._request_handlers.get(request["operation"])

        if handler:
            return handler(self, request["transaction_id"], data, request["protocol_id"])

        return None

    def _handle_settings_request(self, request: dict, addr: tuple[str, int]) -> None:
        operation = request["operation"]

        if operation == "board_info":
            self._send_mcast_board_info()
            self._send_unicast_from_settings(addr, {"kind": "board_info", "name": self._config.name})
        elif operation == "product_info":
            self._send_mcast_product_info()
            self._send_unicast_from_settings(
                addr,
                {
                    "kind": "product_info",
                    "name": self._config.name,
                    "manufacturer": self._config.manufacturer,
                    "model": self._config.model,
                },
            )
        elif operation == "clock_status":
            self._send_mcast_clock_stats()
        elif operation == "interface_status":
            self._send_mcast_network_info()
        elif operation == "audio":
            kind = request["kind"]
            value = request["value"]

            if kind == "sample_rate":
                supported = self._config.supported_sample_rates
                publish = self._send_sample_rate_status
            else:
                supported = self._config.supported_encodings
                publish = self._send_encoding_status

            if value is not None:
                if value not in supported:
                    return

                setattr(self._config, kind, value)

            publish()

    def _send_unicast_from_settings(self, addr: tuple[str, int], publication: dict) -> None:
        if self._mcast_transport:
            packet = self._build_publication(publication)
            self._mcast_transport.sendto(packet, addr)
            logger.debug("Sent unicast response to %s (%s bytes)", addr, len(packet))

    def _handle_cmc_registration(self, transaction_id: int) -> bytes:
        return self._build_response(
            transaction_id,
            {
                "kind": "cmc_registration",
                "device_ip": self._local_ip or "0.0.0.0",
                "settings_port": DEVICE_SETTINGS_PORT,
            },
            PROTOCOL_CMC,
        )

    def _build_response(
        self,
        transaction_id: int,
        response: dict,
        protocol_id: int,
        result_code: int = RESULT_CODE_SUCCESS,
    ) -> bytes:
        return core.build_response(
            {
                "protocol_id": protocol_id,
                "transaction_id": transaction_id,
                "result_code": result_code,
                "response": response,
            }
        )

    def _acknowledgement(self, transaction_id: int, request: bytes, protocol_id: int, *, accepted: bool) -> bytes:
        return self._build_response(
            transaction_id,
            {"kind": "acknowledgement", "request": list(request), "accepted": accepted},
            protocol_id,
        )

    def _handle_device_name(self, transaction_id: int, data: bytes, protocol_id: int = PROTOCOL_ID) -> bytes:
        return self._build_response(transaction_id, {"kind": "device_name", "name": self._config.name}, protocol_id)

    def _handle_channel_count(self, transaction_id: int, data: bytes, protocol_id: int = PROTOCOL_ID) -> bytes:
        return self._build_response(
            transaction_id,
            {
                "kind": "channel_count",
                "tx_count": len(self._config.tx_channels),
                "rx_count": len(self._config.rx_channels),
            },
            protocol_id,
        )

    def _handle_device_info(self, transaction_id: int, data: bytes, protocol_id: int = PROTOCOL_ID) -> bytes:
        return self._build_response(
            transaction_id,
            {
                "kind": "device_info",
                "model_name": self._config.model,
                "display_name": self._config.name,
                "model_code": self._config.model,
                "port": "",
            },
            protocol_id,
        )

    def _handle_tx_channels(self, transaction_id: int, data: bytes, protocol_id: int = PROTOCOL_ID) -> bytes:
        return self._channel_status_response(
            transaction_id, protocol_id, "tx", [{"name": name} for name in self._config.tx_channels]
        )

    def _channel_status_response(
        self, transaction_id: int, protocol_id: int, channel_type: str, channels: list
    ) -> bytes:
        return self._build_response(
            transaction_id,
            {
                "kind": "channel_status",
                "channel_type": channel_type,
                "channels": channels,
                "audio": {
                    "sample_rate": self._config.sample_rate,
                    "encoding": self._config.encoding,
                    "supported_encodings": self._config.supported_encodings,
                },
            },
            protocol_id,
        )

    def _handle_tx_channel_names(self, transaction_id: int, data: bytes, protocol_id: int = PROTOCOL_ID) -> bytes:
        return self._build_response(
            transaction_id, {"kind": "transmitter_names", "names": self._config.tx_channels}, protocol_id
        )

    def _handle_rx_channels(self, transaction_id: int, data: bytes, protocol_id: int = PROTOCOL_ID) -> bytes:
        channels = []

        for number, name in enumerate(self._config.rx_channels, 1):
            source = self._subscriptions.get(number)
            channels.append({"name": name, "source": {"channel": source[0], "device": source[1]} if source else None})

        return self._channel_status_response(transaction_id, protocol_id, "rx", channels)

    def _handle_property_directory(self, transaction_id: int, data: bytes, protocol_id: int = PROTOCOL_ID) -> bytes:
        return self._build_response(transaction_id, {"kind": "property_directory"}, protocol_id)

    def _handle_device_settings(self, transaction_id: int, data: bytes, protocol_id: int = PROTOCOL_ID) -> bytes:
        return self._build_response(
            transaction_id,
            {
                "kind": "device_settings",
                "sample_rate": self._config.sample_rate,
                "default_latency_ns": self._config.default_latency_ns,
                "configured_latency_ns": self._config.configured_latency_ns,
                "active_latency_ns": int(self._config.active_latency_ns),
                "maximum_latency_ns": self._config.maximum_latency_ns,
                "minimum_latency_ns": self._config.minimum_latency_ns,
            },
            protocol_id,
        )

    def _handle_device_settings_set(
        self,
        transaction_id: int,
        data: bytes,
        protocol_id: int = PROTOCOL_ID,
    ) -> bytes:
        try:
            latency_nanoseconds = core.parse_response("set_latency_request", data)
        except core.NetaudioCoreError as error:
            if error.category != "binary_response":
                raise

            return self._handle_unsupported(transaction_id, data, protocol_id)

        self._config.configured_latency_ns = latency_nanoseconds
        self._config.active_latency_ns = latency_nanoseconds
        return self._build_response(
            transaction_id, {"kind": "latency_applied", "latency_ns": latency_nanoseconds}, protocol_id
        )

    def _handle_tx_flows(self, transaction_id: int, data: bytes, protocol_id: int = PROTOCOL_ID) -> bytes:
        return self._build_response(transaction_id, {"kind": "empty_flows", "channel_type": "tx"}, protocol_id)

    def _handle_rx_flows(self, transaction_id: int, data: bytes, protocol_id: int = PROTOCOL_ID) -> bytes:
        return self._build_response(transaction_id, {"kind": "empty_flows", "channel_type": "rx"}, protocol_id)

    def _handle_rx_subscriptions(self, transaction_id: int, data: bytes, protocol_id: int = PROTOCOL_ID) -> bytes:
        return self._build_response(transaction_id, {"kind": "receiver_port_ranges"}, protocol_id)

    def _handle_tx_flow_labels(self, transaction_id: int, data: bytes, protocol_id: int = PROTOCOL_ID) -> bytes:
        return self._build_response(transaction_id, {"kind": "empty_transmitter_flow_labels"}, protocol_id)

    def _handle_channel_name_set(self, transaction_id: int, data: bytes, protocol_id: int = PROTOCOL_ID) -> bytes:
        try:
            change = core.parse_response("set_channel_name_request", data)
        except core.NetaudioCoreError as error:
            if error.category != "binary_response":
                raise

            return self._handle_unsupported(transaction_id, data, protocol_id)

        channels = self._config.rx_channels if change["channel_type"] == "rx" else self._config.tx_channels
        index = change["channel_number"] - 1

        if not 0 <= index < len(channels):
            return self._handle_unsupported(transaction_id, data, protocol_id)

        channels[index] = change["name"]
        return self._acknowledgement(transaction_id, data, protocol_id, accepted=True)

    def _handle_subscription_add(self, transaction_id: int, data: bytes, protocol_id: int = PROTOCOL_ID) -> bytes:
        try:
            subscriptions = core.parse_response("add_subscriptions_request", data)
        except core.NetaudioCoreError as error:
            if error.category != "binary_response":
                raise

            return self._handle_unsupported(transaction_id, data, protocol_id)

        if any(subscription["rx_channel"] > len(self._config.rx_channels) for subscription in subscriptions):
            return self._handle_unsupported(transaction_id, data, protocol_id)

        for subscription in subscriptions:
            channel = subscription["rx_channel"]
            source_channel = subscription["tx_channel"]
            source_device = subscription["tx_device"]
            self._subscriptions[channel] = (source_channel, source_device)
            logger.info(f"Subscribed RX ch {channel} <- {source_channel}@{source_device}")

        return self._acknowledgement(transaction_id, data, protocol_id, accepted=True)

    def _handle_subscription_remove(self, transaction_id: int, data: bytes, protocol_id: int = PROTOCOL_ID) -> bytes:
        try:
            channels = core.parse_response("remove_subscriptions_request", data)
        except core.NetaudioCoreError as error:
            if error.category != "binary_response":
                raise

            return self._handle_unsupported(transaction_id, data, protocol_id)

        if any(channel > len(self._config.rx_channels) for channel in channels):
            return self._handle_unsupported(transaction_id, data, protocol_id)

        for channel in channels:
            self._subscriptions.pop(channel, None)
            logger.info(f"Unsubscribed RX ch {channel}")

        return self._acknowledgement(transaction_id, data, protocol_id, accepted=True)

    def _handle_unsupported(self, transaction_id: int, data: bytes, protocol_id: int = PROTOCOL_ID) -> bytes:
        return self._acknowledgement(transaction_id, data, protocol_id, accepted=False)

    _request_handlers = {
        "device_name": _handle_device_name,
        "channel_count": _handle_channel_count,
        "device_info": _handle_device_info,
        "transmitter_channels": _handle_tx_channels,
        "transmitter_channel_names": _handle_tx_channel_names,
        "receiver_channels": _handle_rx_channels,
        "device_settings": _handle_device_settings,
        "set_latency": _handle_device_settings_set,
        "property_directory": _handle_property_directory,
        "transmitter_flows": _handle_tx_flows,
        "transmitter_flow_labels": _handle_tx_flow_labels,
        "set_channel_name": _handle_channel_name_set,
        "add_subscriptions": _handle_subscription_add,
        "remove_subscriptions": _handle_subscription_remove,
        "receiver_flows": _handle_rx_flows,
        "receiver_port_ranges": _handle_rx_subscriptions,
    }
