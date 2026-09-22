from __future__ import annotations

from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from netaudio.dante.device import DanteDevice


def _valid_channel_number(number):
    return isinstance(number, int) and not isinstance(number, bool) and number > 0


def channel_by_number(channels, number):
    if not _valid_channel_number(number):
        raise RuntimeError("channel number must be a positive integer")

    return channels_by_number(channels).get(number)


def channels_by_number(channels):
    indexed = {}

    for channel in channels:
        identity = getattr(channel, "number", None)

        if not _valid_channel_number(identity):
            raise RuntimeError("channel inventory contains an invalid inventory identity")

        if identity in indexed:
            raise RuntimeError(f"channel {identity} has conflicting identities")

        indexed[identity] = channel

    return indexed


class DanteChannel:
    def __init__(self):
        self.channel_type: Literal["rx", "tx"] | None = None
        self.device: DanteDevice | None = None
        self.friendly_name = None
        self.factory_name = None
        self.name = None
        self.number = None
        self.status_code: int | None = None
        self.status_text = None
        self.volume = None
        self.muted = None
        self.bit_depth = None
        self.samples_per_frame = None
        self.flags = None
        self.media_type_code = None
        self.media_type = None
        self.media_local_id = None
        self.format_descriptor_hexadecimal = None
        self.sample_rate = None
        self.encoding = None
        self.media_service = None
        self.can_subscribe_self: bool | None = None
        self.direct_can_subscribe_self: bool | None = None
        self.can_rename: bool | None = None
        self.receiver_flags: int | None = None
        self.receiver_capability_flags: int | None = None
        self.receiver_status_flags: int | None = None
        self.managed_can_subscribe_self: bool | None = None
        self.managed_can_subscribe_self_fresh: bool | None = None
        self.can_subscribe_self_conflict: bool | None = None
        self.ddm_channel_id = None
        self.ddm_enabled = None
        self.ddm_encryption_policy = None
        self.ddm_encryption_scheme = None
        self.ddm_media_type = None
        self.ddm_signal_presence = None
        self.ddm_status = None
        self.ddm_status_message = None
        self.ddm_summary = None

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

    def __getstate__(self):
        state = self.__dict__.copy()
        state["device"] = None
        return state

    def to_json(self):
        from netaudio.dante.device_serializer import DanteDeviceSerializer

        return DanteDeviceSerializer.channel_to_json(self)
