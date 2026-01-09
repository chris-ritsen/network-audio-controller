from ipaddress import IPv4Address

from .device import DanteDevice
from .service import DanteMulticastService
from .util import (
    decode_integer,
    decode_mac_address,
    LOGGER,
    PCMEncoding,
    SampleRate,
)


class DanteNotificationService(DanteMulticastService):
    """
    Dante Notifications

    These are unsolicited messages coming from devices on the Dante Network notifying about
    changes to, amongst other things, configuration.

    Please note that a lot of the comments are based on observations made interacting with a
    single device, and so may not hold true for all Dante-enabled devices.

    ----

    According to the GearSpace discussion, some of these might actually be in response to
    messages sent (to port 8700)
    """
    SERVICE_MCAST_GRP: str = '224.0.0.231'
    SERVICE_PORT: int = 8702

    def _receive(self, address: tuple[IPv4Address, int], message: bytes):
        ipv4_addr = address[0]

        #  0- 1: `\xFF\xFF`
        #  2- 3: Total Length of Message ## length = decode_integer(message, 2)
        #  4- 5: Sequence ID             ## seq_id = decode_integer(message, 4)
        #  6- 7: null hextet
        #  8-13: MAC Address            ## mac_addr = decode_mac_address(message[8:14])
        # 14-15: null hextet
        # 16-23: `Audinate`
        # 24-25: `\x07\x31` (as integers: either 7,49 or 1041)
        notification_id = decode_integer(message, 26)
        # 28-31: 2 x null hextets

        payload = message[32:] # Not all notifications have a payload

        dante_device = self._app.get_device_by_ipv4(ipv4_addr)
        if not dante_device:
            LOGGER.error("Unrecognised IPv4 address %s", ipv4_addr)
            return

        handler_func_name = f"handle_{notification_id}"
        LOGGER.debug(
            "Notification %i payload: %s",
            notification_id,
            ' '.join(f"{b:02x}" for b in payload)
        )
        if hasattr(self, handler_func_name):
            getattr(self, handler_func_name)(dante_device, payload)
        else:
            LOGGER.error("No Method found to handle change notification ID %i", notification_id)

    def get_channels_affected(self, payload: bytes) -> list[int]:
        channels_affected = []
        for bank_idx in range(0, decode_integer(payload, 0)):
            channels = decode_integer(payload, 2 + bank_idx, 1)
            for channel_number in range(1, 9):
                if channels & 1:
                    channels_affected.append(channel_number + bank_idx * 8)
                channels = channels >> 1
        return channels_affected

    def handle_17(self, device: DanteDevice, payload: bytes) -> None:
        """ Network Details
        * See also ID#32, which also contains MAC Address
        """
        # 0-7: \x00\x01 \x00\x01 \x00\x00 \x03\xe8
        # 8-9: \x00\x01 (set via dhcp)
        #      \x00\x03 (manually set)
        mac_address = decode_mac_address(payload[10:16])
        current = {
            'IPv4':    IPv4Address(payload[16:20]),
            'subnet':  IPv4Address(payload[20:24]),
            'dns':     IPv4Address(payload[24:28]),
            'gateway': IPv4Address(payload[28:32]),
        }
        # 32-39: \x00\x18 \x00\x30 \x00\x00 \x00\x00
        # 40-41: \x00\x00 (normal)
        #        \x00\x06 (changing from dhcp -> manual)
        #        \x00\x04 (changing from manual -> dhcp)
        # 42-43: \x00\x00
        pending = {
            'IPv4':    IPv4Address(payload[44:48]),
            'subnet':  IPv4Address(payload[48:52]),
            'dns':     IPv4Address(payload[52:56]),
            'gateway': IPv4Address(payload[56:60]),
        }
        # 60+: \x00\x00 \x00\x00           # Booting up as Manual
        #      \x00\x00 \x00\x48 \x00      # When setting to Manual
        #      \x00\x48 \x00\x00 \x00      # Booting up when just set to Manual
        #      \x00\x48\ x00\x49 \x00\x00  # When setting to Auto

    def handle_20(self, device: DanteDevice, payload: bytes) -> None:
        """Internal network switch configuration"""
        #   0- 13: \x00\x02 \x00\x18 \x00\x10 \x00\x04 \x00\x00 \x00\x7f \x00\x01
        #  14- 15: \x00\x01 (switched)
        #          \x00\x02 (redundant)
        #  16- 19: \x00\x01 \x00\x00
        #  20- 27: `Switched`
        #  28-149: 61 null hextets
        # 150-151: \x00\x2b
        # 152-163: 6 null hextets
        # 164-167: \x00\x02 \x00\x00
        # 168-176: `Redundant`
        # 177-298: 61 null hextets
        # 299-312: \x29\x00 \x00\x00 \x22\x00 \x00\x00 \x00\x00 \x00\x00 \x00

    def handle_32(self, device: DanteDevice, payload: bytes) -> None:
        """ ???
        * Payload observed to contain the device's MAC Address, repeatedly
          - See also ID#17
          - unbroken: @12, 20, 28, 
          - split around a `\xFF\xFE`: @168, 176, 184
        * AES67:
          -           @ 54-55   @112-113  @124-125
          - enabled : \x08\x02  \x00\x00  \x00\x04
          - disabled: \x00\x02  \x00\x01  \x00\x03
        * Payload also contains similar pattern found in ID#132 when Sample Rate Pull-up is set:
          - @ 56-61
        * Payload also contains value that changes when "Unicast Delay Requests" is enabled:
          - @ 96-98
          - \x00\x04 - enabled
          - \x00\x00 - disabled
        """

    def handle_96(self, device: DanteDevice, payload: bytes) -> None:
        """ ???
        * According to GearSpace discussion: dante model
        * Payload observed to contain the name of the Dante Chipset,
          once in abbreviated form (@ 12), then in full (@ 56)
        """

    def handle_98(self, device: DanteDevice, payload: bytes) -> None:
        """ ???
        * According to GearSpace discussion: identify device
        * Not personally observed at present
        """

    def handle_112(self, device: DanteDevice, payload: bytes) -> None:
        """ ???
        * According to GearSpace discussion: firmware version related
        """

    def handle_120(self, device: DanteDevice, payload: bytes) -> None:
        """ ??? """

    def handle_128(self, device: DanteDevice, payload: bytes) -> None:
        """Sample Rate
        * Payload contains new Sample Rate and list of possible Sample Rates
        """
        # 0-1: \x00\x18 (Coincidentally, the length of the payload)
        sample_rate_count = decode_integer(payload, 2)
        new_sample_rate = SampleRate.decode(payload, 4)
        # 8-15: \x00\x00 \x00\x00 \x00\x02 \x00\x00
        rate_options = []
        for idx in range(sample_rate_count):
            rate_options.append(SampleRate.decode(payload, 16 + idx * 4))

        LOGGER.debug(
            "%s is reporting a possible Sample Rate change (new: %s; options: %s)",
            device.name,
            new_sample_rate,
            ", ".join([str(rate) for rate in rate_options]),
        )
        if new_sample_rate and new_sample_rate != device.sample_rate:
            device._sample_rate = new_sample_rate

    def handle_130(self, device: DanteDevice, payload: bytes) -> None:
        """ PCM Encoding
        * Payload contains new PCM Encoding and list of possible encodings
        """
        # 0-3: \x00\x18
        pcm_encoding_count = decode_integer(payload, 2)
        new_pcm_encoding = PCMEncoding.decode(payload, 4)
        # 8-15: \x00\x00 \x00\x00 \x00\x02 \x00\x00
        encoding_options = []
        for idx in range(pcm_encoding_count):
            encoding_options.append(PCMEncoding.decode(payload, 16 + idx * 4))

        LOGGER.debug(
            "%s is reporting a possible PCM Encoding change (new: %s; options: %s)",
            device.name,
            new_pcm_encoding,
            ", ".join([str(enc) for enc in encoding_options]),
        )
        if new_pcm_encoding and new_pcm_encoding != device.pcm_encoding:
            device._pcm_encoding = new_pcm_encoding

    def handle_132(self, device: DanteDevice, payload: bytes) -> None:
        """ Sample Rate Pull-up """
        # Differences between messages when different options are chosen
        #         None              +4.667%   +1%       -1%       -4
        #-------  ----------------  --------  --------  --------  --------
        # @ 6- 7  \x00\x00          \x00\x01  \x00\x02  \x00\x03  \x00\x04
        # @10-11  \x00\x00          \x00\x01  \x00\x02  \x00\x03  \x00\x04
        # @24-27  \x00\x00\x00\x00  \x5f\x41\x4c\x54
        # @28-29  \x00\x00          \x31\x00  \x32\x00  \x33\x00  \x34\x00

        # When AES67 is...
        #                 Enabled   Disabled
        #                 --------  --------
        # @2-3            \x00\x01  \x00\x05
        # payload length  44 octet  60 octet
        #
        # Payload missing is:
        #     \x00\x00\x00\x01 \x00\x00\x00\x02 \x00\x00\x00\x03 \x00\x00\x00\x04
        # which appear to be the options used in the first two rows of the first table
        # (Also, Pull-up is marked as disabled in Dante Controller UI when AES67 is enabled.)

    def handle_134(self, device: DanteDevice, payload: bytes) -> None:
        """ ??? """

    def handle_146(self, device: DanteDevice, payload: bytes) -> None:
        """ Device is Restarting
        * payload appears to be two null hextets
        """

    def handle_176(self, device: DanteDevice, payload: bytes) -> None:
        """ ???
        * Observed to appear after a Sample Rate has been set, but not immediately
        """
        # Observed payload, regardless of sample rate set:
        # * \x00\x03\x00\x00

    def handle_192(self, device: DanteDevice, payload: bytes) -> None:
        """ ???
        * According to GearSpace discussion: make/model info
        * Payload observed to contain:
          - Manufacturer name, abbreviated @ 0
          - Some code? @ 8
          - Manufacturer name, full @ 44
          - Device Name @ 172
          - Device version @ 304
        """

    def handle_256(self, device: DanteDevice, payload: bytes) -> None:
        """ ??? """

    def handle_257(self, device: DanteDevice, payload: bytes) -> None:
        """TX Channel(s)
        * Emitted if one or more channel name has been changed
        * Payload contains information about which channels are affected
        """
        channels_affected = self.get_channels_affected(payload)
        LOGGER.debug(
            "TX channel(s) %s of %s have changed",
            ', '.join([str(c) for c in channels_affected]),
            device.name
        )
        # As it doesn't seem possible to request an update of a single channel,
        # request all channels.
        device.request_tx_channels()

    def handle_258(self, device: DanteDevice, payload: bytes) -> None:
        """RX Channel(s)
        * Emitted if one or more channels renamed or subscription changed
        * Payload contains information about which channels are affected
        """
        channels_affected = self.get_channels_affected(payload)
        LOGGER.debug(
            "RX channel(s) %s of %s have changed",
            ', '.join([str(c) for c in channels_affected]),
            device.name
        )
        # As it doesn't seem possible to request an update of a single channel,
        # request all channels.
        device.request_rx_channels()


    def handle_260(self, device: DanteDevice, payload: bytes) -> None:
        """ ???
        * Not personally observed, but mentioned in the GearSpace discussion
        """

    def handle_261(self, device: DanteDevice, payload: bytes) -> None:
        """ ???
        * According to the GearSpace: sub/unsub related
        * Not personally observed at present
        """

    def handle_262(self, device: DanteDevice, _: bytes) -> None:
        """ Latency
        * No payload expected
        """
        device.request_latency()

    def handle_288(self, device: DanteDevice, payload: bytes) -> None:
        """ ???
        * No payload
        """

    def handle_4097(self, device: DanteDevice, payload: bytes) -> None:
        """ ???
        * Not personally observed, but mentioned in the GearSpace discussion
        """

    def handle_4103(self, device: DanteDevice, payload: bytes) -> None:
        """ AES67 """
        # 0-1: \x00\x00 (AES67 disabled)
        #      \x00\x03 (AES67 enabled)
        # 2-3: null hextet
        LOGGER.debug("%s has changed its AES67 status", device.name)

    def handle_4105(self, device: DanteDevice, payload: bytes) -> None:
        """ ???
        * According to GearSpace discussion: device lock/unlock status
        """
        # 0- 5: 3 x null hextets
        # 6- 7: \x00\x08
        # 8-15: 4 x null hextets

    def handle_4107(self, device: DanteDevice, payload: bytes) -> None:
        """ ???
        * Not personally observed, but mentioned in the GearSpace discussion
        """
