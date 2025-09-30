from .arc_service import DanteARCService
from .channel import DanteTxChannel
from .cmc_service import DanteCMCService
from .dbc_service import DanteDBCService
from .device import DanteDevice
from .discovery import DanteDiscovery
from .settings_service import DanteSettingsService
from .util import LOGGER
from .volume_service import DanteVolumeService


class DanteApplication:

    def __init__(self):

        self._arc: DanteARCService = DanteARCService(self)
        self._cmc: DanteCMCService = DanteCMCService(self)
        self._dbc: DanteDBCService = DanteDBCService(self)
        self._settings: DanteSettingsService = DanteSettingsService(self)
        self._vol: DanteVolumeService = DanteVolumeService(self)
        self._discovery: DanteDiscovery = DanteDiscovery(self)

        self._devices: list[DanteDevice] = []
        self._orphaned_tx_channels: dict[str, list[DanteTxChannel]] = {}

    def startup(self):
        self._arc.start()
        self._cmc.start()
        # ~ self._dbc.start()
        # ~ self._settings.start()
        self._vol.start()
        self._discovery.start()

    def shutdown(self):
        self._discovery.stop()
        self._arc.stop()
        self._cmc.stop()
        # ~ self._dbc.stop()
        # ~ self._settings.stop()
        self._vol.stop()

    @property
    def arc_service(self) -> DanteARCService:
        return self._arc

    @property
    def cmc_service(self) -> DanteCMCService:
        return self._cmc

    @property
    def dbc_service(self) -> DanteDBCService:
        return self._dbc

    @property
    def devices(self) -> list[DanteDevice]:
        return self._devices

    @property
    def settings_service(self) -> DanteSettingsService:
        return self._settings

    @property
    def volume_service(self) -> DanteVolumeService:
        return self._vol

    def register_device(self, device_spec):
        LOGGER.info("Discovered new Dante device at %s", device_spec['ipv4'])
        new_device = DanteDevice(self, device_spec)
        self._devices.append(new_device)

    def get_device_by_name(self, device_name: str) -> DanteDevice | None:
        if not device_name:
            return None
        # Names are unique on the network, but case-insensitive
        device_name = device_name.lower()
        try:
            return next(
                filter(
                    lambda device: device.name.lower() == device_name,
                    self._devices
                )
            )
        except StopIteration:
            return None

    def append_orphaned_tx_channel(self, tx_device_name: str, tx_channel: DanteTxChannel) -> None:
        if tx_device_name not in self._orphaned_tx_channels:
            self._orphaned_tx_channels[tx_device_name] = []
        self._orphaned_tx_channels[tx_device_name].append(tx_channel)

    def retrieve_orphaned_tx_channel(self, tx_device_name: str, tx_channel_name: str) -> DanteTxChannel | None:
        if tx_device_name not in self._orphaned_tx_channels:
            return None
        for idx in range(len(self._orphaned_tx_channels[tx_device_name])):
            channel = self._orphaned_tx_channels[tx_device_name][idx]
            if channel.name == tx_channel_name:
                return self._orphaned_tx_channels[tx_device_name].pop(idx)
        return None
