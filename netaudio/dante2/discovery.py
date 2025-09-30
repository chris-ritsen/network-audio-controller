import enum
import ipaddress

from zeroconf import (
    IPVersion,
    ServiceBrowser,
    Zeroconf,
)

from .arc_service import DanteARCService
from .cmc_service import DanteCMCService
from .dbc_service import DanteDBCService
from .util import LOGGER


class DanteDiscoveryState(enum.Enum):
    COMPLETE = enum.auto()
    DISCONNECTED = enum.auto()
    IN_PROGRESS = enum.auto()


class DanteDiscovery:

    DISCOVERABLE_SERVICE_CLASSES: list = [
        DanteARCService,
        DanteCMCService,
        DanteDBCService,
    ]

    def __init__(self, application: 'DanteApplication'):
        self._app: 'DanteApplication' = application
        self._found: dict = {}
        self._zc: Zeroconf | None = None
        self._zc_browser: ServiceBrowser | None = None

    def add_service(self, zc: Zeroconf, service_type: str, service_name: str) -> None:
        info = zc.get_service_info(service_type, service_name)
        name = info.server
        LOGGER.debug("Device %s (%s) appeared", name, service_name)

        if name not in self._found:
            self._found[name] = {
                **{service.SERVICE_TYPE_SHORT: None for service in self.DISCOVERABLE_SERVICE_CLASSES},
                'ipv4': ipaddress.IPv4Address(info.parsed_addresses()[0]),
                'status': DanteDiscoveryState.IN_PROGRESS,
            }
        elif self._found[name]['status'] == DanteDiscoveryState.DISCONNECTED:
            self._found[name]['status'] = DanteDiscoveryState.IN_PROGRESS
            for service in self.DISCOVERABLE_SERVICE_CLASSES:
                self._found[name][service.SERVICE_TYPE_SHORT] = None

        dante_service = self.get_dante_service_from_type(service_type)
        service_descriptor = dante_service.build_service_descriptor(info)
        self._found[name][dante_service.SERVICE_TYPE_SHORT] = service_descriptor

        def _all_present():
            for service in self.DISCOVERABLE_SERVICE_CLASSES:
                if self._found[name][service.SERVICE_TYPE_SHORT] is None:
                    return False
            return True

        if _all_present():
            if self._found[name]['status'] == DanteDiscoveryState.IN_PROGRESS:
                self._found[name]['status'] = DanteDiscoveryState.COMPLETE
                self._app.register_device(self._found[name])

    def get_dante_service_from_type(self, service_type: str):
        for service in self.DISCOVERABLE_SERVICE_CLASSES:
            if service.SERVICE_TYPE_MDNS == service_type:
                return service
        return None

    def remove_service(self, zc: Zeroconf, service_type: str, service_name: str) -> None:
        info = zc.get_service_info(service_type, service_name)
        name = info.server
        LOGGER.debug("Device %s (%s) disappeared", name, service_name)

    def start(self) -> None:
        if self._zc and self._zc.started:
            return
        self._zc = Zeroconf(ip_version=IPVersion.V4Only)
        service_types = [service.SERVICE_TYPE_MDNS for service in self.DISCOVERABLE_SERVICE_CLASSES]
        self._zc_browser = ServiceBrowser(self._zc, service_types, self)

    def stop(self) -> None:
        self._zc_browser.cancel()
        self._zc.close()

    def update_service(self, zc: Zeroconf, service_type: str, service_name: str) -> None:
        info = zc.get_service_info(service_type, service_name)
        name = info.server
        LOGGER.debug("Device %s (%s) updated", name, service_name)

        dante_service = self.get_dante_service_from_type(service_type)
        service_descriptor = dante_service.build_service_descriptor(info)
        self._found[name][dante_service.SERVICE_TYPE_SHORT] = service_descriptor
