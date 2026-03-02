class DanteChannel:
    def __init__(self):
        self._channel_type = None
        self._device = None
        self._friendly_name = None
        self._name = None
        self._number = None
        self._status_code = None
        self._status_text = None
        self._volume = None

    def __str__(self):
        if self.friendly_name:
            name = self.friendly_name
        else:
            name = self.name

        if self.volume and self.volume != 254:
            text = f"{self.number}:{name} [{self.volume}]"
        else:
            text = f"{self.number}:{name}"

        return text

    @property
    def device(self):
        return self._device

    @device.setter
    def device(self, device):
        self._device = device

    @property
    def number(self):
        return self._number

    @number.setter
    def number(self, number):
        self._number = number

    @property
    def status_code(self):
        return self._status_code

    @status_code.setter
    def status_code(self, status_code):
        self._status_code = status_code

    @property
    def status_text(self):
        return self._status_text

    @status_text.setter
    def status_text(self, status_text):
        self._status_text = status_text

    @property
    def channel_type(self):
        return self._channel_type

    @channel_type.setter
    def channel_type(self, channel_type):
        self._channel_type = channel_type

    @property
    def friendly_name(self):
        return self._friendly_name

    @friendly_name.setter
    def friendly_name(self, friendly_name):
        self._friendly_name = friendly_name

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        self._name = name

    @property
    def volume(self):
        return self._volume

    @volume.setter
    def volume(self, volume):
        self._volume = volume

    def __getstate__(self):
        state = self.__dict__.copy()
        state["_device"] = None
        return state

    def to_json(self):
        from netaudio_lib.dante.device_serializer import DanteDeviceSerializer

        return DanteDeviceSerializer.channel_to_json(self)
