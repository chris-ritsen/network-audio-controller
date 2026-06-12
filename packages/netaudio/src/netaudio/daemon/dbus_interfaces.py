import logging

from dbus_fast import PropertyAccess
from dbus_fast.service import ServiceInterface, dbus_property, signal

logger = logging.getLogger("netaudio")

RO = PropertyAccess.READ


class ManagerInterface(ServiceInterface):
    def __init__(self, daemon):
        super().__init__("com.netaudio.Manager")
        self._daemon = daemon

    @dbus_property(access=RO)
    def DanteDeviceCount(self) -> "u":
        return sum(1 for d in self._daemon.devices.values() if d.online)

    @dbus_property(access=RO)
    def ShureDeviceCount(self) -> "u":
        if not self._daemon.shure:
            return 0
        return len(self._daemon.shure.devices)

    @dbus_property(access=RO)
    def DanteDevices(self) -> "as":
        return [sn for sn, d in self._daemon.devices.items() if d.online]

    @dbus_property(access=RO)
    def ShureDevices(self) -> "as":
        if not self._daemon.shure:
            return []
        return list(self._daemon.shure.devices.keys())

    @signal()
    def DanteDeviceAdded(self, server_name) -> "s":
        return server_name

    @signal()
    def DanteDeviceRemoved(self, server_name) -> "s":
        return server_name

    @signal()
    def ShureDeviceAdded(self, mac) -> "s":
        return mac

    @signal()
    def ShureDeviceRemoved(self, mac) -> "s":
        return mac


class DanteDeviceInterface(ServiceInterface):
    def __init__(self, device):
        super().__init__("com.netaudio.DanteDevice")
        self._device = device

    @dbus_property(access=RO)
    def ServerName(self) -> "s":
        return self._device.server_name or ""

    @dbus_property(access=RO)
    def Name(self) -> "s":
        return self._device.name or ""

    @dbus_property(access=RO)
    def MacAddress(self) -> "s":
        return self._device.mac_address or ""

    @dbus_property(access=RO)
    def Ipv4(self) -> "s":
        return str(self._device.ipv4) if self._device.ipv4 else ""

    @dbus_property(access=RO)
    def Model(self) -> "s":
        return self._device.model or ""

    @dbus_property(access=RO)
    def ModelId(self) -> "s":
        return self._device.model_id or ""

    @dbus_property(access=RO)
    def Manufacturer(self) -> "s":
        return self._device.manufacturer or ""

    @dbus_property(access=RO)
    def DanteModel(self) -> "s":
        return self._device.dante_model or ""

    @dbus_property(access=RO)
    def BoardName(self) -> "s":
        return self._device.board_name or ""

    @dbus_property(access=RO)
    def SampleRate(self) -> "u":
        return self._device.sample_rate or 0

    @dbus_property(access=RO)
    def Encoding(self) -> "u":
        return self._device.encoding or 0

    @dbus_property(access=RO)
    def BitDepth(self) -> "u":
        return self._device.bit_depth or 0

    @dbus_property(access=RO)
    def Latency(self) -> "u":
        v = self._device.latency
        return int(v) if v else 0

    @dbus_property(access=RO)
    def MinLatency(self) -> "u":
        v = self._device.min_latency
        return int(v) if v else 0

    @dbus_property(access=RO)
    def MaxLatency(self) -> "u":
        v = self._device.max_latency
        return int(v) if v else 0

    @dbus_property(access=RO)
    def TxCount(self) -> "u":
        return self._device.tx_count or 0

    @dbus_property(access=RO)
    def RxCount(self) -> "u":
        return self._device.rx_count or 0

    @dbus_property(access=RO)
    def Online(self) -> "b":
        return self._device.online

    @dbus_property(access=RO)
    def IsLocked(self) -> "b":
        return bool(self._device.is_locked)

    @dbus_property(access=RO)
    def Aes67Enabled(self) -> "b":
        return bool(self._device.aes67_enabled)

    @dbus_property(access=RO)
    def LastSeen(self) -> "d":
        return self._device.last_seen or 0.0

    @dbus_property(access=RO)
    def ClockRole(self) -> "s":
        return self._device.clock_role or ""

    @dbus_property(access=RO)
    def ClockMac(self) -> "s":
        return self._device.clock_mac or ""

    @dbus_property(access=RO)
    def SoftwareVersion(self) -> "s":
        return self._device.software_version or ""

    @dbus_property(access=RO)
    def FirmwareVersion(self) -> "s":
        return self._device.firmware_version or ""

    @dbus_property(access=RO)
    def Subscriptions(self) -> "a(ssssq)":
        result = []
        for sub in (self._device.subscriptions or []):
            rx_dev = ""
            if sub.rx_device:
                rx_dev = sub.rx_device.server_name or ""
            elif sub.rx_device_name:
                rx_dev = sub.rx_device_name
            result.append((
                rx_dev,
                sub.rx_channel_name or "",
                sub.tx_device_name or "",
                sub.tx_channel_name or "",
                sub.status_code if sub.status_code is not None else 0,
            ))
        return result


class DanteChannelInterface(ServiceInterface):
    def __init__(self, channel):
        super().__init__("com.netaudio.DanteChannel")
        self._channel = channel

    @dbus_property(access=RO)
    def Number(self) -> "u":
        return self._channel.number or 0

    @dbus_property(access=RO)
    def Name(self) -> "s":
        return self._channel.name or ""

    @dbus_property(access=RO)
    def FriendlyName(self) -> "s":
        return self._channel.friendly_name or ""

    @dbus_property(access=RO)
    def StatusText(self) -> "s":
        return self._channel.status_text or ""

    @dbus_property(access=RO)
    def Volume(self) -> "u":
        return self._channel.volume or 0

    @dbus_property(access=RO)
    def Muted(self) -> "b":
        return bool(self._channel.muted)

    @dbus_property(access=RO)
    def BitDepth(self) -> "u":
        return self._channel.bit_depth or 0

    @dbus_property(access=RO)
    def SamplesPerFrame(self) -> "u":
        return self._channel.samples_per_frame or 0


class ShureDeviceInterface(ServiceInterface):
    def __init__(self, device):
        super().__init__("com.netaudio.ShureDevice")
        self._device = device

    @dbus_property(access=RO)
    def Ip(self) -> "s":
        return self._device.ip or ""

    @dbus_property(access=RO)
    def Mac(self) -> "s":
        return self._device.mac or ""

    @dbus_property(access=RO)
    def DeviceType(self) -> "s":
        return self._device.device_type.value if self._device.device_type else ""

    @dbus_property(access=RO)
    def Name(self) -> "s":
        return self._device.name or ""

    @dbus_property(access=RO)
    def Model(self) -> "s":
        return self._device.model or ""

    @dbus_property(access=RO)
    def FirmwareVersion(self) -> "s":
        return self._device.firmware_version or ""

    @dbus_property(access=RO)
    def RfBand(self) -> "s":
        return self._device.rf_band or ""

    @dbus_property(access=RO)
    def TransmissionMode(self) -> "s":
        return self._device.transmission_mode or ""

    @dbus_property(access=RO)
    def QuadversityMode(self) -> "s":
        return self._device.quadversity_mode or ""

    @dbus_property(access=RO)
    def EncryptionMode(self) -> "s":
        return self._device.encryption_mode or ""


class ShureChannelInterface(ServiceInterface):
    def __init__(self, channel):
        super().__init__("com.netaudio.ShureChannel")
        self._channel = channel

    @dbus_property(access=RO)
    def Number(self) -> "u":
        return self._channel.number or 0

    @dbus_property(access=RO)
    def Name(self) -> "s":
        return self._channel.name or ""

    @dbus_property(access=RO)
    def Frequency(self) -> "u":
        return self._channel.frequency or 0

    @dbus_property(access=RO)
    def AudioGain(self) -> "i":
        return self._channel.audio_gain or 0

    @dbus_property(access=RO)
    def AudioMute(self) -> "b":
        return bool(self._channel.audio_mute)

    @dbus_property(access=RO)
    def AudioLevelPeak(self) -> "i":
        return self._channel.audio_level_peak or 0

    @dbus_property(access=RO)
    def AudioLevelRms(self) -> "i":
        return self._channel.audio_level_rms or 0

    @dbus_property(access=RO)
    def SignalQuality(self) -> "u":
        return getattr(self._channel, "signal_quality", None) or 0

    @dbus_property(access=RO)
    def AntennaStatus(self) -> "s":
        return getattr(self._channel, "antenna_status", None) or ""

    @dbus_property(access=RO)
    def Active(self) -> "b":
        return getattr(self._channel, "active", False)

    @dbus_property(access=RO)
    def TransmitterModel(self) -> "s":
        tx = getattr(self._channel, "transmitter", None)
        if tx:
            return tx.model or ""
        return ""

    @dbus_property(access=RO)
    def TransmitterConnected(self) -> "b":
        tx = getattr(self._channel, "transmitter", None)
        if tx:
            return tx.connected
        return False

    @dbus_property(access=RO)
    def BatteryMinutes(self) -> "u":
        tx = getattr(self._channel, "transmitter", None)
        if tx and tx.battery_minutes is not None:
            return tx.battery_minutes
        return 0

    @dbus_property(access=RO)
    def BatteryChargePercent(self) -> "u":
        tx = getattr(self._channel, "transmitter", None)
        if tx and tx.battery_charge_percent is not None:
            return tx.battery_charge_percent
        return 0
