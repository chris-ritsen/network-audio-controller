import logging
from typing import Optional, Union

from dbus_fast import PropertyAccess
from dbus_fast.service import ServiceInterface, dbus_property, signal

from netaudio.daemon.dbus_state import (
    aes67_enabled,
    dbus_double,
    dbus_int32,
    dbus_string,
    dbus_uint,
    dbus_uint_list,
    latency_milliseconds,
    subscription_rows,
)
from netaudio.shure.device import ShureChannel, ShureP10TChannel

logger = logging.getLogger("netaudio")

RO = PropertyAccess.READ
DBUS_STRING_ARRAY_SIGNATURE = "as"


class ManagerInterface(ServiceInterface):
    def __init__(self, daemon):
        super().__init__("com.netaudio.Manager")
        self._daemon = daemon

    @dbus_property(access=RO)
    def DanteDeviceCount(self) -> "u":
        return sum(1 for device in self._daemon.devices.values() if device.online)

    @dbus_property(access=RO)
    def ShureDeviceCount(self) -> "u":
        if not self._daemon.shure:
            return 0
        return len(self._daemon.shure.devices)

    @dbus_property(access=RO)
    def DanteDevices(self) -> DBUS_STRING_ARRAY_SIGNATURE:
        return sorted(server_name for server_name, device in self._daemon.devices.items() if device.online)

    @dbus_property(access=RO)
    def ShureDevices(self) -> DBUS_STRING_ARRAY_SIGNATURE:
        if not self._daemon.shure:
            return []
        return sorted(self._daemon.shure.devices)

    @signal()
    def DanteDeviceAdded(self, server_name) -> "s":
        return server_name

    @signal()
    def DanteDeviceRemoved(self, server_name) -> "s":
        return server_name

    @signal()
    def ShureDeviceAdded(self, mac_address) -> "s":
        return mac_address

    @signal()
    def ShureDeviceRemoved(self, mac_address) -> "s":
        return mac_address


class DanteDeviceInterface(ServiceInterface):
    def __init__(self, device):
        super().__init__("com.netaudio.DanteDevice")
        self._device = device

    @dbus_property(access=RO)
    def ServerName(self) -> "s":
        return dbus_string(self._device.server_name)

    @dbus_property(access=RO)
    def Name(self) -> "s":
        return dbus_string(self._device.name)

    @dbus_property(access=RO)
    def MacAddress(self) -> "s":
        return dbus_string(self._device.mac_address)

    @dbus_property(access=RO)
    def Ipv4(self) -> "s":
        return dbus_string(self._device.ipv4)

    @dbus_property(access=RO)
    def Model(self) -> "s":
        return dbus_string(self._device.model)

    @dbus_property(access=RO)
    def ModelId(self) -> "s":
        return dbus_string(self._device.model_id)

    @dbus_property(access=RO)
    def Manufacturer(self) -> "s":
        return dbus_string(self._device.manufacturer)

    @dbus_property(access=RO)
    def DanteModel(self) -> "s":
        return dbus_string(self._device.dante_model)

    @dbus_property(access=RO)
    def BoardName(self) -> "s":
        return dbus_string(self._device.board_name)

    @dbus_property(access=RO)
    def LinkSpeedMbps(self) -> "u":
        return dbus_uint(self._device.link_speed_mbps)

    @dbus_property(access=RO)
    def SampleRate(self) -> "u":
        return dbus_uint(self._device.sample_rate)

    @dbus_property(access=RO)
    def SupportedSampleRates(self) -> "au":
        return dbus_uint_list(self._device.supported_sample_rates)

    @dbus_property(access=RO)
    def Encoding(self) -> "u":
        return dbus_uint(self._device.encoding)

    @dbus_property(access=RO)
    def SupportedEncodings(self) -> "au":
        return dbus_uint_list(self._device.supported_encodings)

    @dbus_property(access=RO)
    def BitDepth(self) -> "u":
        return dbus_uint(self._device.bit_depth)

    @dbus_property(access=RO)
    def Latency(self) -> "d":
        return latency_milliseconds(self._device.latency)

    @dbus_property(access=RO)
    def ActiveLatency(self) -> "d":
        return latency_milliseconds(self._device.active_latency)

    @dbus_property(access=RO)
    def ConfiguredLatency(self) -> "d":
        return latency_milliseconds(self._device.configured_latency)

    @dbus_property(access=RO)
    def DefaultLatency(self) -> "d":
        return latency_milliseconds(self._device.default_latency)

    @dbus_property(access=RO)
    def MinLatency(self) -> "d":
        return latency_milliseconds(self._device.min_latency)

    @dbus_property(access=RO)
    def MaxLatency(self) -> "d":
        return latency_milliseconds(self._device.max_latency)

    @dbus_property(access=RO)
    def TxCount(self) -> "u":
        return dbus_uint(self._device.tx_count)

    @dbus_property(access=RO)
    def RxCount(self) -> "u":
        return dbus_uint(self._device.rx_count)

    @dbus_property(access=RO)
    def Online(self) -> "b":
        return bool(self._device.online)

    @dbus_property(access=RO)
    def IsLocked(self) -> "b":
        return bool(self._device.is_locked)

    @dbus_property(access=RO)
    def Aes67Enabled(self) -> "b":
        return aes67_enabled(self._device)

    @dbus_property(access=RO)
    def LastSeen(self) -> "d":
        return dbus_double(self._device.last_seen)

    @dbus_property(access=RO)
    def ClockRole(self) -> "s":
        return dbus_string(self._device.clock_role)

    @dbus_property(access=RO)
    def ClockMac(self) -> "s":
        return dbus_string(self._device.clock_mac)

    @dbus_property(access=RO)
    def SoftwareVersion(self) -> "s":
        return dbus_string(self._device.software_version)

    @dbus_property(access=RO)
    def FirmwareVersion(self) -> "s":
        return dbus_string(self._device.firmware_version)

    @dbus_property(access=RO)
    def Subscriptions(self) -> "a(ssssq)":
        return subscription_rows(self._device)


class DanteChannelInterface(ServiceInterface):
    def __init__(self, channel):
        super().__init__("com.netaudio.DanteChannel")
        self._channel = channel

    @dbus_property(access=RO)
    def Number(self) -> "u":
        return dbus_uint(self._channel.number)

    @dbus_property(access=RO)
    def Name(self) -> "s":
        return dbus_string(self._channel.name)

    @dbus_property(access=RO)
    def FriendlyName(self) -> "s":
        return dbus_string(self._channel.friendly_name)

    @dbus_property(access=RO)
    def StatusText(self) -> "s":
        return dbus_string(self._channel.status_text)

    @dbus_property(access=RO)
    def Volume(self) -> "u":
        return dbus_uint(self._channel.volume)

    @dbus_property(access=RO)
    def Muted(self) -> "b":
        return bool(self._channel.muted)

    @dbus_property(access=RO)
    def BitDepth(self) -> "u":
        return dbus_uint(self._channel.bit_depth)

    @dbus_property(access=RO)
    def SamplesPerFrame(self) -> "u":
        return dbus_uint(self._channel.samples_per_frame)


class ShureDeviceInterface(ServiceInterface):
    def __init__(self, device):
        super().__init__("com.netaudio.ShureDevice")
        self._device = device

    @dbus_property(access=RO)
    def Ip(self) -> "s":
        return dbus_string(self._device.ip)

    @dbus_property(access=RO)
    def Mac(self) -> "s":
        return dbus_string(self._device.mac)

    @dbus_property(access=RO)
    def DeviceType(self) -> "s":
        return dbus_string(self._device.device_type.value)

    @dbus_property(access=RO)
    def Name(self) -> "s":
        return dbus_string(self._device.name)

    @dbus_property(access=RO)
    def Model(self) -> "s":
        return dbus_string(self._device.model)

    @dbus_property(access=RO)
    def FirmwareVersion(self) -> "s":
        return dbus_string(self._device.firmware_version)

    @dbus_property(access=RO)
    def RfBand(self) -> "s":
        return dbus_string(self._device.rf_band)

    @dbus_property(access=RO)
    def TransmissionMode(self) -> "s":
        return dbus_string(self._device.transmission_mode)

    @dbus_property(access=RO)
    def QuadversityMode(self) -> "s":
        return dbus_string(self._device.quadversity_mode)

    @dbus_property(access=RO)
    def EncryptionMode(self) -> "s":
        return dbus_string(self._device.encryption_mode)


class ShureChannelInterface(ServiceInterface):
    def __init__(self, channel: Union[ShureChannel, ShureP10TChannel]):
        super().__init__("com.netaudio.ShureChannel")
        self._channel = channel

    def _ad4d_channel(self) -> Optional[ShureChannel]:
        if isinstance(self._channel, ShureChannel):
            return self._channel
        return None

    @dbus_property(access=RO)
    def Number(self) -> "u":
        return dbus_uint(self._channel.number)

    @dbus_property(access=RO)
    def Name(self) -> "s":
        return dbus_string(self._channel.name)

    @dbus_property(access=RO)
    def Frequency(self) -> "u":
        return dbus_uint(self._channel.frequency)

    @dbus_property(access=RO)
    def AudioGain(self) -> "i":
        if isinstance(self._channel, ShureChannel):
            return dbus_int32(self._channel.audio_gain)
        return dbus_int32(self._channel.audio_in_level)

    @dbus_property(access=RO)
    def AudioMute(self) -> "b":
        if isinstance(self._channel, ShureChannel):
            return bool(self._channel.audio_mute)
        return bool(self._channel.rf_mute)

    @dbus_property(access=RO)
    def AudioLevelPeak(self) -> "i":
        channel = self._ad4d_channel()
        return dbus_int32(channel.audio_level_peak if channel is not None else None)

    @dbus_property(access=RO)
    def AudioLevelRms(self) -> "i":
        channel = self._ad4d_channel()
        return dbus_int32(channel.audio_level_rms if channel is not None else None)

    @dbus_property(access=RO)
    def SignalQuality(self) -> "u":
        channel = self._ad4d_channel()
        return dbus_uint(channel.signal_quality if channel is not None else None)

    @dbus_property(access=RO)
    def AntennaStatus(self) -> "s":
        channel = self._ad4d_channel()
        return dbus_string(channel.antenna_status if channel is not None else None)

    @dbus_property(access=RO)
    def Active(self) -> "b":
        channel = self._ad4d_channel()
        return bool(channel.active) if channel is not None else False

    @dbus_property(access=RO)
    def TransmitterModel(self) -> "s":
        channel = self._ad4d_channel()
        tx = channel.transmitter if channel is not None else None
        if tx is not None:
            return dbus_string(tx.model)
        return ""

    @dbus_property(access=RO)
    def TransmitterConnected(self) -> "b":
        channel = self._ad4d_channel()
        tx = channel.transmitter if channel is not None else None
        if tx is not None:
            return bool(tx.connected)
        return False

    @dbus_property(access=RO)
    def BatteryMinutes(self) -> "u":
        channel = self._ad4d_channel()
        tx = channel.transmitter if channel is not None else None
        if tx and tx.battery_minutes is not None:
            return dbus_uint(tx.battery_minutes)
        return 0

    @dbus_property(access=RO)
    def BatteryChargePercent(self) -> "u":
        channel = self._ad4d_channel()
        tx = channel.transmitter if channel is not None else None
        if tx and tx.battery_charge_percent is not None:
            return dbus_uint(tx.battery_charge_percent)
        return 0
