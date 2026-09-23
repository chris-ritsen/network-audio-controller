from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from netaudio.dante.channel import DanteChannel
    from netaudio.dante.device import DanteDevice

from netaudio.core import (
    subscription_status,
)


def managed_subscription_status(status, status_message, summary) -> dict:
    from netaudio import core

    return {
        **core.managed_subscription_status({"status": status, "status_message": status_message, "summary": summary}),
        "code": None,
        "icon": "",
    }


class DanteSubscription:
    def __init__(self):
        self.error = None
        self.rx_channel: DanteChannel | None = None
        self.rx_channel_name = None
        self.rx_channel_status_code: int | None = None
        self.rx_device: DanteDevice | None = None
        self._rx_device_name = None
        self.status_code: int | None = None
        self.status_message = []
        self.tx_channel: DanteChannel | None = None
        self.tx_channel_name = None
        self.tx_device: DanteDevice | None = None
        self._tx_device_name = None
        self._is_self_connection: bool | None = None
        self.ddm_status = None
        self.ddm_status_message = None
        self.ddm_summary = None

    @property
    def has_configured_source(self) -> bool:
        return bool(self.tx_device_name)

    @property
    def is_self_connection(self) -> bool:
        if self.rx_device is not None and self.tx_device is not None:
            from netaudio.dante.self_connection import same_canonical_device

            return same_canonical_device(self.rx_device, self.tx_device)
        return self._is_self_connection is True

    def __str__(self):
        return self.format(verbose=True)

    def format(self, verbose=True):
        if self.tx_channel_name and self.tx_device_name:
            text = f"{self.rx_channel_name}@{self.rx_device_name} <- {self.tx_channel_name}@{self.tx_device_name}"
        else:
            text = f"{self.rx_channel_name}@{self.rx_device_name}"

        if verbose:
            status_text = self.status_text()
            status_text = ", ".join(status_text)
            text = f"{text} [{status_text}]"

        return text

    def status_text(self):
        if self.status_code is None:
            if not any((self.ddm_status, self.ddm_summary, self.ddm_status_message)):
                return (*self.status_message,) or ("Status unavailable",)
            status = managed_subscription_status(self.ddm_status, self.ddm_status_message, self.ddm_summary)
            detail = status["detail"]
            return (status["label"], *((detail,) if detail else ()), *self.status_message)
        entry = subscription_status(self.status_code, self.rx_channel_status_code)
        return (str(entry["label"]), *self.status_message)

    def to_json(self):
        from netaudio.dante.device_serializer import DanteDeviceSerializer

        return DanteDeviceSerializer.subscription_to_json(self)

    @property
    def rx_device_name(self):
        if self._rx_device_name:
            return self._rx_device_name
        device = getattr(self.rx_channel, "device", None)
        return getattr(device, "name", None) or self._rx_device_name

    @rx_device_name.setter
    def rx_device_name(self, rx_device_name):
        self._rx_device_name = rx_device_name

    @property
    def tx_device_name(self):
        if self.tx_device is not None:
            return getattr(self.tx_device, "name", None) or self._tx_device_name
        return self._tx_device_name

    @tx_device_name.setter
    def tx_device_name(self, tx_device_name):
        self._tx_device_name = tx_device_name
