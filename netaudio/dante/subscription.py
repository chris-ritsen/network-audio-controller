from netaudio.dante.const import (
    SUBSCRIPTION_STATUS_LABELS,
)


class DanteSubscription:
    def __init__(self):
        self._error = None
        self._rx_channel = None
        self._rx_channel_name = None
        self._rx_device = None
        self._rx_device_name = None
        self._status_code = None
        self._status_message = []
        self._tx_channel = None
        self._tx_channel_name = None
        self._tx_device = None
        self._tx_device_name = None

    def __str__(self):
        if self.tx_channel_name and self.tx_device_name:
            text = f"{self.rx_channel_name}@{self.rx_device_name} <- {self.tx_channel_name}@{self.tx_device_name}"
        else:
            text = f"{self.rx_channel_name}@{self.rx_device_name}"

        status_text = self.status_text()

        if self.rx_channel_status_code in SUBSCRIPTION_STATUS_LABELS:
            status_text = list(status_text)
            status_text.extend(self.rx_channel_status_text())
        status_text = ", ".join(status_text)
        text = f"{text} [{status_text}]"

        return text

    def status_text(self):
        return SUBSCRIPTION_STATUS_LABELS[self.status_code]

    def rx_channel_status_text(self):
        return SUBSCRIPTION_STATUS_LABELS[self.rx_channel_status_code]

    def to_json(self):
        as_json = {
            "rx_channel": self.rx_channel_name,
            "rx_channel_status_code": self.rx_channel_status_code,
            "rx_device": self.rx_device_name,
            "status_code": self.status_code,
            "status_text": self.status_text(),
            "tx_channel": self.tx_channel_name,
            "tx_device": self.tx_device_name,
        }

        if self.rx_channel_status_code in SUBSCRIPTION_STATUS_LABELS:
            as_json["rx_channel_status_text"] = self.rx_channel_status_text()

        return as_json

    @property
    def error(self):
        return self._error

    @error.setter
    def error(self, error):
        self._error = error

    @property
    def rx_channel_name(self):
        return self._rx_channel_name

    @rx_channel_name.setter
    def rx_channel_name(self, rx_channel_name):
        self._rx_channel_name = rx_channel_name

    @property
    def tx_channel_name(self):
        return self._tx_channel_name

    @tx_channel_name.setter
    def tx_channel_name(self, tx_channel_name):
        self._tx_channel_name = tx_channel_name

    @property
    def rx_device_name(self):
        return self._rx_device_name

    @rx_device_name.setter
    def rx_device_name(self, rx_device_name):
        self._rx_device_name = rx_device_name

    @property
    def rx_channel_status_code(self):
        return self._rx_channel_status_code

    @rx_channel_status_code.setter
    def rx_channel_status_code(self, rx_channel_status_code):
        self._rx_channel_status_code = rx_channel_status_code

    @property
    def status_code(self):
        return self._status_code

    @status_code.setter
    def status_code(self, status_code):
        self._status_code = status_code

    @property
    def status_message(self):
        return self._status_message

    @status_message.setter
    def status_message(self, status_message):
        self._status_message = status_message

    @property
    def tx_device_name(self):
        return self._tx_device_name

    @tx_device_name.setter
    def tx_device_name(self, tx_device_name):
        self._tx_device_name = tx_device_name

    @property
    def rx_channel(self):
        return self._rx_channel

    @rx_channel.setter
    def rx_channel(self, rx_channel):
        self._rx_channel = rx_channel

    @property
    def tx_channel(self):
        return self._tx_channel

    @tx_channel.setter
    def tx_channel(self, tx_channel):
        self._tx_channel = tx_channel

    @property
    def rx_device(self):
        return self._rx_device

    @rx_device.setter
    def rx_device(self, rx_device):
        self._rx_device = rx_device

    @property
    def tx_device(self):
        return self._tx_device

    @tx_device.setter
    def tx_device(self, tx_device):
        self._tx_device = tx_device
