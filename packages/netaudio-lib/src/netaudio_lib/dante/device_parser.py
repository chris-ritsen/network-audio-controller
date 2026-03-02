import logging
import struct
import traceback

from netaudio_lib.dante.channel import DanteChannel
from netaudio_lib.dante.subscription import DanteSubscription

logger = logging.getLogger("netaudio")

RESPONSE_HEADER_SIZE = 10
BODY_HEADER_SIZE = 2
RX_RECORD_SIZE = 20
TX_RECORD_SIZE = 8
TX_FRIENDLY_RECORD_SIZE = 6


class DanteDeviceParser:
    @staticmethod
    def _get_string_at_offset(data: bytes, offset: int) -> str:
        if offset == 0 or offset >= len(data):
            return None
        end = data.find(b'\x00', offset)
        if end == -1:
            end = len(data)
        try:
            return data[offset:end].decode('utf-8')
        except Exception:
            return None

    @staticmethod
    def parse_bluetooth_status(response):
        if not response or len(response) < 50:
            return None

        if response[36] != 0x12 or response[38] != 0x0a:
            return None

        field1_len = response[39]
        pos = 40 + field1_len

        if pos < len(response) and response[pos] == 0x18:
            pos += 1
            while pos < len(response) and response[pos] & 0x80:
                pos += 1
            pos += 1

        if pos >= len(response) or response[pos] != 0x22:
            return None

        pos += 1
        if pos >= len(response):
            return None

        field4_len = response[pos]
        pos += 1
        field4_end = pos + field4_len

        if field4_end > len(response):
            return None

        i = field4_end - 1
        while i >= pos and 0x20 <= response[i] < 0x7f:
            i -= 1

        if i >= pos and i < field4_end - 1 and i >= 1 and response[i - 1] == 0x12:
            name_start = i + 1
            try:
                return response[name_start:field4_end].decode('utf-8')
            except (UnicodeDecodeError, ValueError):
                return None

        return None

    def parse_volume(
        self, bytes_volume, rx_count_raw, tx_count_raw, tx_channels, rx_channels
    ):
        rx_channel_volumes = bytes_volume[-1 - rx_count_raw : -1]
        tx_channel_volumes = bytes_volume[
            -1 - rx_count_raw - tx_count_raw : -1 - rx_count_raw
        ]

        try:
            for _, channel in tx_channels.items():
                channel.volume = tx_channel_volumes[channel.number - 1]

            for _, channel in rx_channels.items():
                channel.volume = rx_channel_volumes[channel.number - 1]

        except Exception as e:
            print(e)
            traceback.print_exc()

    async def get_rx_channels(self, device, dante_command_func):
        rx_channels = {}
        subscriptions = []

        try:
            for page in range(0, max(int(device.rx_count / 16), 1)):
                receivers_args = device.commands.command_receivers(page)
                response = await dante_command_func(
                    *receivers_args, logical_command_name="get_receivers"
                )
                if response is None:
                    logger.debug(
                        f"No response received for get_receivers command on page {page}"
                    )
                    continue

                body = response[RESPONSE_HEADER_SIZE:]
                channels_this_page = 0

                for index in range(0, min(device.rx_count, 16)):
                    record_offset = BODY_HEADER_SIZE + (index * RX_RECORD_SIZE)
                    if record_offset + RX_RECORD_SIZE > len(body):
                        break

                    record = body[record_offset : record_offset + RX_RECORD_SIZE]

                    channel_number = struct.unpack(">H", record[0:2])[0]
                    expected = (page * 16) + index + 1
                    if channel_number == 0 or channel_number != expected:
                        break

                    tx_channel_offset = struct.unpack(">H", record[6:8])[0]
                    tx_device_offset = struct.unpack(">H", record[8:10])[0]
                    rx_channel_offset = struct.unpack(">H", record[10:12])[0]
                    rx_channel_status_code = struct.unpack(">H", record[12:14])[0]
                    subscription_status_code = struct.unpack(">H", record[14:16])[0]

                    rx_channel_name = self._get_string_at_offset(response, rx_channel_offset)
                    tx_device_name = self._get_string_at_offset(response, tx_device_offset)

                    if tx_channel_offset != 0:
                        tx_channel_name = self._get_string_at_offset(response, tx_channel_offset)
                    else:
                        tx_channel_name = rx_channel_name

                    if index == 0 and tx_device_offset != 0:
                        sample_rate_offset = struct.unpack(">H", record[4:6])[0]
                        if sample_rate_offset + 4 <= len(response):
                            sample_rate_bytes = response[sample_rate_offset:sample_rate_offset + 4]
                            sample_rate = struct.unpack(">I", sample_rate_bytes)[0]
                            if sample_rate:
                                device.sample_rate = sample_rate

                    subscription = DanteSubscription()
                    rx_channel = DanteChannel()

                    rx_channel.channel_type = "rx"
                    rx_channel.device = device
                    rx_channel.name = rx_channel_name
                    rx_channel.number = channel_number
                    rx_channel.status_code = rx_channel_status_code

                    rx_channels[channel_number] = rx_channel

                    subscription.rx_channel_name = rx_channel_name
                    subscription.rx_device_name = device.name
                    subscription.tx_channel_name = tx_channel_name
                    subscription.status_code = subscription_status_code
                    subscription.rx_channel_status_code = rx_channel_status_code

                    if tx_device_name == ".":
                        subscription.tx_device_name = device.name
                    else:
                        subscription.tx_device_name = tx_device_name

                    subscriptions.append(subscription)
                    channels_this_page += 1

                if channels_this_page < 16:
                    break
        except Exception as e:
            device.error = e
            print(e)
            traceback.print_exc()

        return rx_channels, subscriptions

    async def get_tx_channels(self, device, dante_command_func):
        tx_channels = {}
        tx_friendly_channel_names = {}

        try:
            num_pages = max(1, (device.tx_count + 31) // 32)
            for page in range(0, num_pages):
                transmitters_friendly_args = device.commands.command_transmitters(
                    page, friendly_names=True
                )
                response_friendly = await dante_command_func(
                    *transmitters_friendly_args,
                    logical_command_name="get_transmitters_friendly",
                )
                if response_friendly is None:
                    logger.debug(
                        f"No response received for get_transmitters_friendly command on page {page}"
                    )
                    continue

                body = response_friendly[RESPONSE_HEADER_SIZE:]
                channels_this_page = 0

                for index in range(0, min(device.tx_count, 32)):
                    record_offset = BODY_HEADER_SIZE + (index * TX_FRIENDLY_RECORD_SIZE)
                    if record_offset + TX_FRIENDLY_RECORD_SIZE > len(body):
                        break

                    record = body[record_offset : record_offset + TX_FRIENDLY_RECORD_SIZE]

                    channel_number = struct.unpack(">H", record[2:4])[0]
                    if channel_number == 0:
                        break

                    name_offset = struct.unpack(">H", record[4:6])[0]
                    friendly_name = self._get_string_at_offset(response_friendly, name_offset)

                    if friendly_name:
                        tx_friendly_channel_names[channel_number] = friendly_name

                    channels_this_page += 1

                if channels_this_page < 32:
                    break

            for page in range(0, num_pages):
                transmitters_raw_args = device.commands.command_transmitters(
                    page, friendly_names=False
                )
                response_raw = await dante_command_func(
                    *transmitters_raw_args, logical_command_name="get_transmitters_raw"
                )
                if response_raw is None:
                    logger.debug(
                        f"No response received for get_transmitters_raw command on page {page}"
                    )
                    continue

                body = response_raw[RESPONSE_HEADER_SIZE:]

                first_channel_group = None
                channels_this_page = 0

                for index in range(0, min(device.tx_count, 32)):
                    record_offset = BODY_HEADER_SIZE + (index * TX_RECORD_SIZE)
                    if record_offset + TX_RECORD_SIZE > len(body):
                        break

                    record = body[record_offset : record_offset + TX_RECORD_SIZE]

                    channel_number = struct.unpack(">H", record[0:2])[0]
                    expected = (page * 32) + index + 1
                    if channel_number == 0 or channel_number != expected:
                        break

                    channel_group = struct.unpack(">H", record[4:6])[0]
                    name_offset = struct.unpack(">H", record[6:8])[0]

                    if index == 0:
                        first_channel_group = channel_group
                        sample_rate_offset = channel_group
                        if sample_rate_offset + 4 <= len(response_raw):
                            sample_rate_bytes = response_raw[sample_rate_offset:sample_rate_offset + 4]
                            sample_rate = struct.unpack(">I", sample_rate_bytes)[0]
                            if sample_rate:
                                device.sample_rate = sample_rate

                    if channel_group != first_channel_group:
                        break

                    tx_channel_name = self._get_string_at_offset(response_raw, name_offset)

                    tx_channel = DanteChannel()
                    tx_channel.channel_type = "tx"
                    tx_channel.number = channel_number
                    tx_channel.device = device
                    tx_channel.name = tx_channel_name

                    if channel_number in tx_friendly_channel_names:
                        tx_channel.friendly_name = tx_friendly_channel_names[
                            channel_number
                        ]

                    tx_channels[channel_number] = tx_channel
                    channels_this_page += 1

                if channels_this_page < 32:
                    break

        except Exception as e:
            device.error = e
            print(e)
            traceback.print_exc()

        return tx_channels
