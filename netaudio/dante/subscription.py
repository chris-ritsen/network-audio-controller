from pydantic import BaseModel

from netaudio.dante.const import (
    SUBSCRIPTION_STATUS_LABELS,
)

class DanteSubscription(BaseModel):
    error: str | None = None
    rx_channel: str | None = None
    rx_channel_name: str | None = None
    rx_device: str | None = None
    rx_device_name: str | None = None
    status_code: int | None = None # From rx channel
    status_message: list[str] = []
    tx_channel: str | None = None
    tx_channel_name: str | None = None
    tx_device: str | None = None
    tx_device_name: str | None = None

    def __str__(self):
        if self.tx_channel_name and self.tx_device_name:
            text = f"{self.rx_channel_name}@{self.rx_device_name} <- {self.tx_channel_name}@{self.tx_device_name}"
        else:
            text = f"{self.rx_channel_name}@{self.rx_device_name}"

        status_text = self.status_text

        if self.status_code in SUBSCRIPTION_STATUS_LABELS:
            status_text = list(status_text)
            status_text.extend(self.status_text)
        status_text = ", ".join(status_text)
        text = f"{text} [{status_text}]"

        return text

    @property
    def status_text(self):
        return SUBSCRIPTION_STATUS_LABELS[self.status_code]

    @property
    def rx_channel_status_text(self):
        return SUBSCRIPTION_STATUS_LABELS[self.rx_channel_status_code]

    def to_json(self):
        as_json = {
            "rx_channel": self.rx_channel_name,
            "rx_channel_status_code": self.rx_channel_status_code,
            "rx_device": self.rx_device_name,
            "status_code": self.status_code,
            "status_text": self.status_text,
            "tx_channel": self.tx_channel_name,
            "tx_device": self.tx_device_name,
        }

        if self.rx_channel_status_code in SUBSCRIPTION_STATUS_LABELS:
            as_json["rx_channel_status_text"] = self.rx_channel_status_text

        return as_json