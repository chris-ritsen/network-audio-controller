import pytest

from netaudio.dante.channel import DanteChannel
from netaudio.dante.const import (
    MODERN_ARC_MEDIA_TYPE_ANCILLARY,
    MODERN_ARC_MEDIA_TYPE_AUDIO,
    MODERN_ARC_MEDIA_TYPE_VIDEO,
)
from netaudio.dante.device import DanteDevice


@pytest.mark.parametrize(
    ("media_type_code", "expected_label"),
    [
        (MODERN_ARC_MEDIA_TYPE_AUDIO, "audio"),
        (MODERN_ARC_MEDIA_TYPE_VIDEO, "video"),
        (MODERN_ARC_MEDIA_TYPE_ANCILLARY, "ancillary"),
    ],
)
def test_modern_arc_channel_media_type_labels(media_type_code, expected_label):
    channel = DanteChannel()

    DanteDevice._apply_modern_arc_channel_metadata(
        channel,
        {"media_type_code": media_type_code, "media_local_channel_id": 1},
    )

    assert channel.media_type_code == media_type_code
    assert channel.media_type == expected_label
