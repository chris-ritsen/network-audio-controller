import pprint
import traceback

from netaudio.dante.channel import DanteChannel
from netaudio.dante.subscription import DanteSubscription


class DanteChannelHelper:
    def __init__(self, device):
        self.device = device

    async def get_rx_channels(self):
        rx_channels = {}
        subscriptions = []

        try:
            for page in range(0, max(int(self.device.rx_count / 16), 1)):
                receivers = await self.device.command.send(
                    *self.device.command_builder.get_receivers(page)
                )
                hex_rx_response = receivers.hex()

                for index in range(0, min(self.device.rx_count, 16)):
                    n = 4
                    str1 = hex_rx_response[(24 + (index * 40)) : (56 + (index * 40))]
                    channel = [str1[i : i + n] for i in range(0, len(str1), n)]

                    if channel:
                        channel_number = int(channel[0], 16)
                        channel_offset = channel[3]
                        device_offset = channel[4]
                        rx_channel_offset = channel[5]
                        rx_channel_status_code = int(channel[6], 16)
                        subscription_status_code = int(channel[7], 16)

                        rx_channel_name = self._get_label(
                            hex_rx_response, rx_channel_offset
                        )

                        tx_device_name = self._get_label(hex_rx_response, device_offset)

                        if not channel_offset == "0000":
                            tx_channel_name = self._get_label(
                                hex_rx_response, channel_offset
                            )
                        else:
                            tx_channel_name = rx_channel_name

                        if index == 0 and not device_offset == "0000":
                            o1 = (int(channel[2], 16) * 2) + 2
                            o2 = o1 + 6
                            sample_rate = int(hex_rx_response[o1:o2], 16)

                            if sample_rate:
                                self.device.sample_rate = sample_rate

                        subscription = DanteSubscription()
                        rx_channel = DanteChannel()

                        rx_channel.channel_type = "rx"
                        rx_channel.device = self
                        rx_channel.name = rx_channel_name
                        rx_channel.number = channel_number
                        rx_channel.status_code = rx_channel_status_code

                        rx_channels[channel_number] = rx_channel

                        subscription.rx_channel_name = rx_channel_name
                        subscription.rx_device_name = self.device.name
                        subscription.tx_channel_name = tx_channel_name
                        subscription.status_code = subscription_status_code
                        subscription.rx_channel_status_code = rx_channel_status_code

                        if tx_device_name == ".":
                            subscription.tx_device_name = self.device.name
                        else:
                            subscription.tx_device_name = tx_device_name

                        subscriptions.append(subscription)
        except Exception as e:
            self.device.error = e
            print(e)
            traceback.print_exc()

        self.device.rx_channels = rx_channels
        self.device.subscriptions = subscriptions

    async def get_tx_channels(self):
        tx_channels = {}
        tx_friendly_channel_names = {}

        try:
            for page in range(0, max(1, int(self.device.tx_count / 16)), 2):
                response = await self.device.command.send(
                    *self.device.command_builder.get_transmitters(
                        page, friendly_names=True
                    )
                )
                tx_friendly_names = response.hex()

                if self.device.name == "avio-usb":
                    pprint.pprint(
                        self.device.command_builder.get_transmitters(
                            page, friendly_names=True
                        )
                    )
                    pprint.pprint(tx_friendly_names)

                for index in range(0, min(self.device.tx_count, 32)):
                    str1 = tx_friendly_names[(24 + (index * 12)) : (36 + (index * 12))]
                    n = 4
                    channel = [str1[i : i + 4] for i in range(0, len(str1), n)]
                    #  channel_index = int(channel[0], 16)
                    channel_number = int(channel[1], 16)
                    channel_offset = channel[2]
                    tx_channel_friendly_name = self._get_label(
                        tx_friendly_names, channel_offset
                    )

                    if tx_channel_friendly_name:
                        tx_friendly_channel_names[channel_number] = (
                            tx_channel_friendly_name
                        )

            for page in range(0, max(1, int(self.device.tx_count / 16)), 2):
                response = await self.device.command.send(
                    *self.device.command_builder.get_transmitters(
                        page, friendly_names=False
                    )
                )

                transmitters = response.hex()
                has_disabled_channels = False

                # TODO: Find the sample rate in the response instead of relying on it being already set from elsewhere
                if self.device.sample_rate:
                    has_disabled_channels = (
                        transmitters.count(f"{self.device.sample_rate:06x}") == 2
                    )

                first_channel = []

                for index in range(0, min(self.device.tx_count, 32)):
                    str1 = transmitters[(24 + (index * 16)) : (40 + (index * 16))]
                    n = 4
                    channel = [str1[i : i + 4] for i in range(0, len(str1), n)]

                    if index == 0:
                        first_channel = channel

                    if channel:
                        o1 = (int(channel[2], 16) * 2) + 2
                        o2 = o1 + 6
                        sample_rate_hex = transmitters[o1:o2]

                        if sample_rate_hex != "000000":
                            self.device.sample_rate = int(sample_rate_hex, 16)

                        channel_number = int(channel[0], 16)
                        # channel_status = channel[1][2:]
                        channel_group = channel[2]
                        channel_offset = channel[3]

                        #  channel_enabled = channel_group == first_channel[2]
                        channel_disabled = channel_group != first_channel[2]

                        if channel_disabled:
                            break

                        tx_channel_name = self._get_label(transmitters, channel_offset)

                        tx_channel = DanteChannel()
                        tx_channel.channel_type = "tx"
                        tx_channel.number = channel_number
                        tx_channel.device = self
                        tx_channel.name = tx_channel_name

                        if channel_number in tx_friendly_channel_names:
                            tx_channel.friendly_name = tx_friendly_channel_names[
                                channel_number
                            ]

                        tx_channels[channel_number] = tx_channel

                if has_disabled_channels:
                    break

        except Exception as e:
            self.device.error = e
            print(e)
            traceback.print_exc()

        self.device.tx_channels = tx_channels

    def _get_label(self, hex_str, offset):
        parsed_get_label = None

        try:
            hex_substring = hex_str[int(offset, 16) * 2 :]
            partitioned_bytes = bytes.fromhex(hex_substring).partition(b"\x00")[0]
            parsed_get_label = partitioned_bytes.decode("utf-8")
        except Exception:
            pass
            #  traceback.print_exc()

        return parsed_get_label
