class DanteSubscription():
    def __init__(self):
        self._error = None
        self._rx_channel = None
        self._rx_channel_name = None
        self._rx_device = None
        self._rx_device_name = None
        self._status_codes = None
        self._status_text = None
        self._tx_channel = None
        self._tx_channel_name = None
        self._tx_device = None
        self._tx_device_name = None


    def __str__(self):
        text = f'{self.rx_channel_name}@{self.rx_device_name} <- {self.tx_channel_name}@{self.tx_device_name}'

        if self.status_text:
            text = f'{text} [{self.status_text}]'

        return text


    def to_json(self):
        as_json = {
            'rx_channel': self.rx_channel_name,
            'rx_device': self.rx_device_name,
            'tx_channel': self.tx_channel_name,
            'tx_device': self.tx_device_name,
        }

        if self.status_text:
            as_json['status_text'] = self.status_text

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
    def status_codes(self):
        return self._status_codes


    @status_codes.setter
    def status_codes(self, status_codes):
        self._status_codes = status_codes


    @property
    def status_text(self):
        return self._status_text


    @status_text.setter
    def status_text(self, status_text):
        self._status_text = status_text


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
