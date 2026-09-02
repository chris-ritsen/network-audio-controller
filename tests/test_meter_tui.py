from __future__ import annotations

import time
from dataclasses import replace
from types import SimpleNamespace

import pytest
from netaudio.commands.meter.models import (
    MeterFilterDialog,
    MeterRow,
    MeterRowKey,
    MeterViewModel,
    MeterViewport,
    automatic_detailed_metering_targets,
    format_meter_sample,
)
from netaudio.commands.meter.rendering import (
    _dbfs_label,
    _meter_bar,
    _state_label,
    render_meter_filter_prompt,
    render_meter_frame,
)
from netaudio.commands.meter.terminal import KeyDecoder
from rich.cells import cell_len


def _channel(number: int, name: str, friendly_name: str | None = None):
    return SimpleNamespace(number=number, name=name, friendly_name=friendly_name)


def _device(
    server_name: str,
    name: str,
    *,
    tx: dict[int, object] | None = None,
    rx: dict[int, object] | None = None,
    tx_count: int | None = None,
    rx_count: int | None = None,
    ipv4: str = "192.0.2.10",
    model_id: str | None = None,
    dante_model: str | None = None,
    online: bool = True,
):
    return SimpleNamespace(
        server_name=server_name,
        name=name,
        online=online,
        ipv4=ipv4,
        model_id=model_id,
        dante_model=dante_model,
        tx_channels=dict(tx or {}),
        rx_channels=dict(rx or {}),
        tx_count=tx_count,
        rx_count=rx_count,
    )


def _sample(*, tx=None, rx=None, tx_indications=None, rx_indications=None, source="signal_presence"):
    return {
        "tx": tx or {},
        "rx": rx or {},
        "tx_signal_presence": tx_indications or {},
        "rx_signal_presence": rx_indications or {},
        "metering_source": source,
        "wall_time": 100.0,
        "source_ip": "192.0.2.10",
        "source_port": 8700,
    }


def _serialized_device(server_name: str, channel_count: int) -> dict:
    channels = {str(number): {"name": f"Channel {number}"} for number in range(1, channel_count + 1)}
    return {
        "server_name": server_name,
        "name": "A32",
        "online": True,
        "tx_count": channel_count,
        "rx_count": channel_count,
        "channels": {
            "transmitters": channels,
            "receivers": channels,
        },
        "subscriptions": [],
    }


def _row(
    number: int,
    *,
    server_name: str = "device.local.",
    direction: str = "TX",
    level: int | None = 0x7B,
    indication: str = "signal_present",
    device_name: str = "Device",
    channel_name: str | None = None,
    source: str = "signal_presence",
) -> MeterRow:
    return MeterRow(
        key=MeterRowKey(server_name, direction, number),
        device_name=device_name,
        channel_name=channel_name or f"Channel {number}",
        level=level,
        indication=indication,
        metering_source=source,
        age=0.25,
    )


def test_view_model_rejects_malformed_sample_atomically_and_owns_accepted_maps():
    device = _device(
        "input.local.",
        "Input",
        tx={1: _channel(1, "mic")},
        rx={1: _channel(1, "return")},
    )
    model = MeterViewModel({"input.local.": device})
    accepted = _sample(
        tx={"1": 0x7B},
        rx={"1": 0xFE},
        tx_indications={"1": "signal_present"},
        rx_indications={"1": "muted"},
    )

    assert model.apply_sample("input.local.", accepted) is True
    accepted["tx"]["1"] = 0x00
    before = model.samples["input.local."].copy()
    before["tx"] = before["tx"].copy()
    before["rx"] = before["rx"].copy()

    malformed = _sample(tx={"1": 0x00, "2": 256}, rx={"1": 0xFD})
    assert model.apply_sample("input.local.", malformed) is False
    assert model.apply_sample("missing.local.", _sample(tx={1: 1})) is False

    assert model.samples["input.local."] == before
    assert model.samples["input.local."]["tx"] == {1: 0x7B}

    older = _sample(tx={1: 0x01})
    older["wall_time"] = 99.0
    assert model.apply_sample("input.local.", older) is False
    assert model.samples["input.local."]["tx"] == {1: 0x7B}

    for invalid_time in (float("nan"), float("inf"), time.time() + 60):
        malformed_time = _sample(tx={1: 0x01})
        malformed_time["wall_time"] = invalid_time
        assert model.apply_sample("input.local.", malformed_time) is False
        assert model.samples["input.local."]["tx"] == {1: 0x7B}


def test_only_online_lx_dante_is_automatically_started_for_detailed_metering():
    devices = {
        "lx.local.": _device("lx.local.", "renamed", model_id="LX-DANTE"),
        "a32.local.": _device(
            "a32.local.",
            "renamed-too",
            model_id="_0000000000000001",
            dante_model="A32 Dante AD/DA Converter",
        ),
        "avio.local.": _device("avio.local.", "avio-input", model_id="DAI2"),
        "name-only.local.": _device("name-only.local.", "lx-dante", model_id="OTHER"),
        "a32-name-only.local.": _device("a32-name-only.local.", "a32", model_id="OTHER"),
        "offline-lx.local.": _device("offline-lx.local.", "lx-dante", model_id="LX-DANTE", online=False),
        "addressless-lx.local.": _device(
            "addressless-lx.local.",
            "lx-dante",
            ipv4="",
            model_id="LX-DANTE",
        ),
    }

    assert automatic_detailed_metering_targets(devices) == ["a32.local.", "lx.local."]


def test_format_meter_sample_preserves_raw_values_and_channel_names():
    device = _device(
        "input.local.",
        "Input",
        tx={17: _channel(17, "tx-17", "Shelford")},
    )

    result = format_meter_sample(device, _sample(tx={17: 0x00}))

    assert result is not None
    assert result["tx"][17] == {
        "name": "Shelford",
        "level": 0x00,
        "signal_presence": "clipping",
    }


def test_view_model_orders_devices_directions_and_numeric_channels():
    zulu = _device(
        "zulu.local.",
        "Zulu",
        tx={10: _channel(10, "ten"), 2: _channel(2, "two")},
        rx={3: _channel(3, "three")},
    )
    alpha = _device(
        "alpha.local.",
        "Alpha",
        tx={4: _channel(4, "four")},
        rx={1: _channel(1, "one")},
    )
    model = MeterViewModel({"zulu.local.": zulu, "alpha.local.": alpha})
    assert model.apply_sample("zulu.local.", _sample(tx={10: 0xFE, 2: 0xFD}, rx={3: 0x7B}))
    assert model.apply_sample("alpha.local.", _sample(tx={4: 0x01, 6: 0x00}, rx={1: 0xFF}))

    assert [row.key for row in model.rows(now=101.0)] == [
        MeterRowKey("alpha.local.", "TX", 4),
        MeterRowKey("alpha.local.", "TX", 6),
        MeterRowKey("alpha.local.", "RX", 1),
        MeterRowKey("zulu.local.", "TX", 2),
        MeterRowKey("zulu.local.", "TX", 10),
        MeterRowKey("zulu.local.", "RX", 3),
    ]


def test_filter_dialog_accepts_typed_device_substring_and_direction():
    zulu = _device(
        "zulu.local.",
        "Zulu",
        tx={1: _channel(1, "tx")},
        rx={1: _channel(1, "rx")},
    )
    alpha = _device(
        "alpha.local.",
        "Alpha",
        tx={1: _channel(1, "tx")},
        rx={1: _channel(1, "rx")},
    )
    model = MeterViewModel({"zulu.local.": zulu, "alpha.local.": alpha})

    assert model.filter_status == "all devices · TX+RX"
    dialog = MeterFilterDialog(model)
    assert dialog.device_label == "all"
    assert dialog.direction_label == "Both (TX + RX)"
    dialog.append_text("ALPHA")
    assert dialog.device_label == "ALPHA"
    dialog.move_field(1)
    dialog.change_value(1)
    assert dialog.direction_label == "TX only"
    dialog.apply(model)

    assert {row.key.server_name for row in model.rows()} == {"alpha.local."}
    assert model.filter_status == "devices~ALPHA · TX"
    assert {row.key.direction for row in model.rows()} == {"TX"}


def test_device_filter_matches_display_or_server_name_but_not_channel_name():
    devices = {
        "avio-input-123.local.": _device(
            "avio-input-123.local.",
            "Rack Input",
            tx={1: _channel(1, "Shelford")},
        ),
        "other.local.": _device(
            "other.local.",
            "AVIO Output",
            tx={1: _channel(1, "Other")},
        ),
    }
    model = MeterViewModel(devices)

    model.set_device_query("AvIo")
    assert {row.key.server_name for row in model.rows()} == {
        "avio-input-123.local.",
        "other.local.",
    }

    model.set_device_query("shelford")
    assert model.rows() == []


def test_vim_search_matches_device_server_and_channel_and_wraps():
    devices = {
        "first.local.": _device("first.local.", "First", tx={1: _channel(1, "Shelford")}),
        "avio-input.local.": _device("avio-input.local.", "Second", tx={1: _channel(1, "Other")}),
        "third.local.": _device("third.local.", "Third", tx={1: _channel(1, "Shelford Return")}),
    }
    model = MeterViewModel(devices)
    rows = model.rows()
    viewport = MeterViewport()
    viewport.replace_rows(rows, page_size=2)

    assert any(model.row_matches_search(row, "avio") for row in rows)
    assert any(model.row_matches_search(row, "shelford") for row in rows)
    assert viewport.move_to_match(
        lambda row: model.row_matches_search(row, "shelford"),
        direction=1,
        page_size=2,
    )
    first_shelford_key = viewport.selected_key
    assert viewport.move_to_match(
        lambda row: model.row_matches_search(row, "shelford"),
        direction=1,
        page_size=2,
    )
    second_shelford_key = viewport.selected_key
    assert second_shelford_key != first_shelford_key
    assert viewport.move_to_match(
        lambda row: model.row_matches_search(row, "shelford"),
        direction=1,
        page_size=2,
    )
    assert viewport.selected_key == first_shelford_key
    assert viewport.move_to_match(
        lambda row: model.row_matches_search(row, "shelford"),
        direction=-1,
        page_size=2,
    )
    assert viewport.selected_key == second_shelford_key


def test_interactive_device_filter_composes_with_cli_direction_and_channel_constraints():
    model = MeterViewModel(
        {
            "avio.local.": _device(
                "avio.local.",
                "AVIO Input",
                tx={1: _channel(1, "Shelford"), 2: _channel(2, "Other")},
                rx={1: _channel(1, "Shelford Return")},
            )
        },
        show_tx=True,
        show_rx=False,
        channel_patterns=["shel*"],
    )
    model.set_device_query("avio")

    rows = model.rows()

    assert [(row.key.direction, row.channel_name) for row in rows] == [("TX", "Shelford")]


def test_filter_dialog_renders_as_a_single_status_prompt():
    model = MeterViewModel({"alpha.local.": _device("alpha.local.", "Alpha")})
    dialog = MeterFilterDialog(model)

    prompt = render_meter_filter_prompt(dialog)

    assert prompt == "filter  device contains: █   direction: Both (TX + RX)"
    dialog.move_field(1)
    assert render_meter_filter_prompt(dialog) == "filter  device contains: all   direction: Both (TX + RX)█"


def test_passive_topology_metadata_immediately_bounds_stale_inventory():
    inventory = {number: _channel(number, f"channel-{number}") for number in range(1, 65)}
    device = _device(
        "a32.local.",
        "A32",
        tx=inventory,
        rx=inventory,
        tx_count=64,
        rx_count=64,
    )
    model = MeterViewModel({"a32.local.": device})
    sample = _sample(tx={1: 0x7B}, rx={1: 0xFD})
    sample.update(
        {
            "tx_count": 16,
            "rx_count": 16,
            "tx_first_channel_index": 0,
            "rx_first_channel_index": 0,
        }
    )

    assert model.apply_sample("a32.local.", sample)

    rows = model.rows(now=100.5)
    assert len(rows) == 32
    assert max(row.key.channel_number for row in rows) == 16


def test_device_capacity_change_discards_old_samples_and_rebuilds_rows():
    inventory = {number: _channel(number, f"channel-{number}") for number in range(1, 65)}
    device = _device(
        "a32.local.",
        "A32",
        tx=inventory,
        rx=inventory,
        tx_count=64,
        rx_count=64,
    )
    model = MeterViewModel({"a32.local.": device})
    assert model.apply_sample(
        "a32.local.",
        _sample(tx={64: 0x01}, rx={64: 0xFD}, source="detailed"),
    )

    assert model.apply_event(
        {
            "event": "device_updated",
            "server_name": "a32.local.",
            "device": _serialized_device("a32.local.", 16),
        }
    )

    reduced_rows = model.rows(now=100.5)
    assert "a32.local." not in model.samples
    assert len(reduced_rows) == 32
    assert max(row.key.channel_number for row in reduced_rows) == 16
    assert all(row.level is None and row.indication == "waiting" for row in reduced_rows)

    assert model.apply_event(
        {
            "event": "device_updated",
            "server_name": "a32.local.",
            "device": _serialized_device("a32.local.", 64),
        }
    )
    restored_rows = model.rows(now=100.5)
    assert len(restored_rows) == 128
    assert max(row.key.channel_number for row in restored_rows) == 64
    assert all(row.level is None for row in restored_rows)


@pytest.mark.parametrize(
    ("raw", "indication", "state", "dbfs"),
    [
        (0x00, "clipping", "CLIP", "clip"),
        (0xFD, "below_threshold", "quiet", "-126.0"),
        (0xFE, "muted", "MUTED", "mute"),
        (0xFF, "unknown", "UNKNOWN", "invalid"),
    ],
)
def test_raw_sentinels_are_preserved_and_render_with_distinct_semantics(raw, indication, state, dbfs):
    device = _device("sentinel.local.", "Sentinel", tx={1: _channel(1, "input")})
    model = MeterViewModel({"sentinel.local.": device})
    assert model.apply_sample("sentinel.local.", _sample(tx={1: raw}))

    row = model.rows(now=100.5)[0]
    assert row.level == raw
    assert row.indication == indication
    assert _state_label(row) == state
    assert _dbfs_label(row.level) == dbfs


@pytest.mark.parametrize(
    ("level", "expected_fill", "expected_color"),
    [
        (0x00, 10, "91;1"),
        (0x01, 10, "92;1"),
        (0x02, 10, "92;1"),
        (0x3E, 5, "92;1"),
        (0x7B, 0, ""),
        (0xFD, 0, ""),
        (0xFE, 0, ""),
        (0xFF, 0, ""),
        (None, 0, ""),
    ],
)
def test_meter_bar_maps_available_level_and_leaves_non_levels_blank(level, expected_fill, expected_color):
    row = _row(1, level=level)

    bar, color = _meter_bar(row, 10)

    assert len(bar) == 10
    assert bar.count("█") == expected_fill
    assert color == expected_color


def test_meter_bar_suppresses_stale_values():
    stale = replace(_row(1, level=0x01), age=3.0)

    assert _meter_bar(stale, 12) == (" " * 12, "")


def test_level_bar_is_responsive_and_caps_at_twenty_cells():
    viewport = MeterViewport()
    viewport.replace_rows([_row(1, level=0x00, indication="clipping")], page_size=4)

    narrow = render_meter_frame(
        viewport,
        width=80,
        height=5,
        mode="detailed",
        connection_status="connected",
        no_color=True,
    )
    ordinary = render_meter_frame(
        viewport,
        width=100,
        height=5,
        mode="detailed",
        connection_status="connected",
        no_color=True,
    )
    wide = render_meter_frame(
        viewport,
        width=200,
        height=5,
        mode="detailed",
        connection_status="connected",
        no_color=True,
    )

    assert "Level" not in narrow
    assert "█" not in narrow
    assert "Level" in ordinary
    assert "█" in ordinary
    assert wide.count("█") == 20
    assert all(cell_len(line) <= 200 for line in wide.splitlines())


def test_user_labels_cannot_collide_with_state_or_level_render_markers():
    viewport = MeterViewport()
    viewport.replace_rows(
        [
            _row(
                1,
                device_name="¤rack",
                channel_name="§" * 17,
                level=0x00,
                indication="clipping",
            )
        ],
        page_size=4,
    )

    frame = render_meter_frame(
        viewport,
        width=100,
        height=5,
        mode="detailed",
        connection_status="connected",
        no_color=True,
    )

    assert "¤" not in frame
    assert "§" not in frame
    assert "?rack" in frame
    assert "?" * 17 in frame
    assert frame.count("█") == 17


def test_viewport_navigation_is_bounded_and_preserves_selected_identity():
    rows = [_row(number) for number in range(1, 11)]
    viewport = MeterViewport()
    viewport.replace_rows(rows, page_size=3)

    viewport.move(4, page_size=3)
    assert viewport.selected == 4
    assert viewport.top == 2
    assert [index for index, _ in viewport.visible_rows(3)] == [2, 3, 4]

    selected_key = viewport.selected_key
    reordered = rows[:2] + rows[5:] + rows[2:5]
    viewport.replace_rows(reordered, page_size=3)
    assert viewport.selected_key == selected_key

    viewport.move(100, page_size=3)
    assert viewport.selected == len(rows) - 1
    assert viewport.top == len(rows) - 3
    viewport.move(-100, page_size=3)
    assert viewport.selected == 0
    assert viewport.top == 0

    viewport.end(page_size=3)
    assert viewport.selected == 9
    viewport.home(page_size=3)
    assert viewport.selected == 0
    viewport.replace_rows([], page_size=0)
    assert viewport.selected_key is None
    assert viewport.visible_rows(0) == []


def test_viewport_can_place_selected_row_at_top_center_or_bottom():
    rows = [_row(number) for number in range(1, 11)]
    viewport = MeterViewport()
    viewport.replace_rows(rows, page_size=3)
    viewport.move(5, page_size=3)

    viewport.align_selected("top", page_size=3)
    assert viewport.top == 5
    viewport.replace_rows(rows, page_size=3)
    assert viewport.top == 5

    viewport.align_selected("center", page_size=3)
    assert viewport.top == 4
    viewport.align_selected("bottom", page_size=3)
    assert viewport.top == 3

    with pytest.raises(ValueError, match="unknown viewport placement"):
        viewport.align_selected("sideways", page_size=3)


def test_key_decoder_handles_fragmented_and_coalesced_escape_sequences():
    decoder = KeyDecoder()

    assert decoder.feed("\x1b") == []
    assert decoder.feed("[") == []
    assert decoder.feed("A") == ["up"]
    assert decoder.feed("j\x1b[6~k") == ["down", "page_down", "up"]
    assert decoder.feed("\x1b[5") == []
    assert decoder.feed("~\x1b[H\x1b[Fq") == ["page_up", "home", "end", "quit"]
    assert decoder.feed("\x1b[999~G\x03") == ["end", "quit"]
    assert decoder.feed("f\r\x1b[D\x1b[C") == ["open_filter", "accept", "left", "right"]
    assert decoder.feed("/") == ["open_search"]
    assert decoder.feed("g") == []
    assert decoder.feed("gG") == ["home", "end"]
    assert decoder.feed("zzztzbzmz.z\rz-") == [
        "align_center",
        "align_top",
        "align_bottom",
        "align_center",
        "align_center",
        "align_top",
        "align_bottom",
    ]
    assert decoder.feed("\x02\x04\x06\x15") == ["page_up", "half_page_down", "page_down", "half_page_up"]
    assert decoder.feed("\x0e\x10\x16\x1bv\x1b<\x1b>") == [
        "down",
        "up",
        "page_down",
        "page_up",
        "home",
        "end",
    ]
    assert decoder.feed("\x7f\b") == ["clear_filter", "clear_filter"]
    assert decoder.feed("\x1b") == []
    assert decoder.flush() == ["cancel"]


@pytest.mark.parametrize(
    "sequence",
    [b"\x1bOM", b"\x1b[13u", b"\x1b[13;1u", b"\x1b[27;1;13~"],
)
def test_key_decoder_accepts_terminal_enter_variants_when_fragmented(sequence):
    decoder = KeyDecoder()

    assert decoder.feed_bytes(sequence[:2]) == []
    assert decoder.feed_bytes(sequence[2:]) == ["accept"]


def test_terminal_enter_variants_end_prompt_mode_and_preserve_vim_z_enter():
    decoder = KeyDecoder()

    assert decoder.feed_bytes(b"favio\x1bOMj") == [
        "open_filter",
        *(f"text:{character}" for character in "avio"),
        "accept",
        "down",
    ]
    assert KeyDecoder().feed_bytes(b"/avio\x1b[13uq") == [
        "open_search",
        *(f"text:{character}" for character in "avio"),
        "accept",
        "quit",
    ]
    assert KeyDecoder().feed_bytes(b"z\x1bOM") == ["align_top"]


def test_key_decoder_handles_sgr_mouse_wheel_and_ignores_other_mouse_events():
    decoder = KeyDecoder()

    assert decoder.feed("\x1b[<64;12") == []
    assert decoder.feed(";7M\x1b[<65;12;7M") == ["wheel_up", "wheel_down"]
    assert decoder.feed("\x1b[<68;12;7M\x1b[<73;12;7M") == ["wheel_up", "wheel_down"]
    assert decoder.feed("\x1b[<0;12;7M\x1b[<0;12;7m") == []
    assert decoder.feed("\x1b[<64;12;7mq") == ["quit"]


def test_key_decoder_ignores_malformed_and_oversized_sgr_mouse_atomically():
    decoder = KeyDecoder()

    assert decoder.feed("\x1b[<64;q;7M") == []
    assert decoder.feed("\x1b[<64;12;jM") == []
    assert decoder.feed(f"\x1b[<64;{'1' * 100};qM") == []
    assert decoder.feed("\x1b[<64;q") == []
    assert decoder.flush() == []


def test_key_decoder_bounds_unterminated_sgr_mouse_without_leaking_following_keys():
    decoder = KeyDecoder()

    assert decoder.feed("\x1b[<" + "1" * 100) == []
    assert decoder.feed("qj") == []
    assert decoder.flush() == []
    assert decoder.feed("q") == ["quit"]


def test_key_decoder_handles_fragmented_x10_mouse_without_leaking_coordinates_as_keys():
    decoder = KeyDecoder()

    assert decoder.feed_bytes(b"\x1b[") == []
    assert decoder.feed_bytes(b"M`qj") == ["wheel_up"]
    assert decoder.feed_bytes(b"k\x1b[Mapf") == ["up", "wheel_down"]
    assert decoder.feed_bytes(b"\x1b[M qk") == []


def test_key_decoder_discards_incomplete_x10_mouse_packet_on_timeout():
    decoder = KeyDecoder()

    assert decoder.feed_bytes(b"\x1b[M`q") == []
    assert decoder.flush_bytes() == []
    assert decoder.flush() == []


def test_key_decoder_preserves_normal_command_characters_in_text_mode():
    decoder = KeyDecoder()

    assert decoder.feed("avio-input-qjkfg/z", text_mode=True) == [
        *(f"text:{character}" for character in "avio-input-qjkfg/z"),
    ]
    assert decoder.feed("\x7f\b\r", text_mode=True) == ["backspace", "backspace", "accept"]


def test_key_decoder_preserves_prompt_text_coalesced_with_the_open_key():
    decoder = KeyDecoder()

    assert decoder.feed("/qjkfg\rj") == [
        "open_search",
        *(f"text:{character}" for character in "qjkfg"),
        "accept",
        "down",
    ]
    assert decoder.feed("favio-input-\r") == [
        "open_filter",
        *(f"text:{character}" for character in "avio-input-"),
        "accept",
    ]


def test_key_decoder_does_not_swallow_escape_coalesced_with_following_input():
    decoder = KeyDecoder()

    assert decoder.feed("\x1bq", text_mode=True) == ["cancel", "quit"]
    assert decoder.feed("\x1bx") == ["cancel"]
    assert decoder._decode_character("\x1b", text_mode=True) == "cancel"


@pytest.mark.parametrize("width", [40, 100])
def test_render_is_bounded_sanitized_and_contains_no_ansi_when_color_is_disabled(width):
    viewport = MeterViewport()
    viewport.replace_rows(
        [
            _row(
                1,
                device_name="Rack\x1b[31m\nDevice 🎛️ with a very long name",
                channel_name="Shelford\tchannel with a very long microphone label",
                level=0x00,
                indication="clipping",
            ),
            _row(2, level=0xFD, indication="below_threshold"),
            _row(3, level=0xFE, indication="muted"),
            _row(4, level=0xFF, indication="unknown"),
        ],
        page_size=8,
    )

    frame = render_meter_frame(
        viewport,
        width=width,
        height=9,
        mode="passive",
        connection_status="connected\x1b[2J\nforged",
        no_color=True,
    )
    lines = frame.splitlines()

    assert len(lines) == 9
    assert all(cell_len(line) <= width for line in lines)
    assert "\x1b" not in frame
    assert "Rack?" in frame
    assert "connected?" in frame
    if width == 100:
        assert "?forged" in frame
    assert "00" in frame
    assert "FD" in frame
    assert "FE" in frame
    assert "FF" in frame
    assert "PASSIVE" in frame
    assert ">" in frame
    assert "Age" not in frame
    if width == 100:
        assert "● CLIP" in frame
        assert "  quiet" in frame
        assert "○ MUTED" in frame
        assert "? UNKNOWN" in frame


def test_state_lamps_use_controller_style_colors_and_keep_text_labels():
    viewport = MeterViewport()
    viewport.replace_rows(
        [
            _row(1, level=0x00, indication="clipping"),
            _row(2, level=0x01, indication="signal_present"),
            _row(3, level=0xFD, indication="below_threshold"),
            _row(4, level=0xFE, indication="muted"),
            _row(5, level=0xFF, indication="unknown"),
            _row(6, level=None, indication="waiting"),
        ],
        page_size=8,
    )

    frame = render_meter_frame(
        viewport,
        width=100,
        height=10,
        mode="passive",
        connection_status="connected",
        no_color=False,
    )

    assert "\x1b[91;1m● CLIP" in frame
    assert "\x1b[92;1m● signal" in frame
    assert "\x1b[90m  quiet" in frame
    assert "\x1b[90m○ MUTED" in frame
    assert "\x1b[93;1m? UNKNOWN" in frame
    assert "\x1b[90m  waiting" in frame
    assert "Age" not in frame
    assert "<1s" not in frame


def test_tiny_terminal_keeps_selection_state_and_raw_value_visible():
    viewport = MeterViewport()
    stale = _row(17, level=0x7B, indication="signal_present")
    stale = replace(stale, age=3.0)
    viewport.replace_rows([stale], page_size=3)

    frame = render_meter_frame(
        viewport,
        width=20,
        height=5,
        mode="passive",
        connection_status="connected",
        no_color=True,
    )

    assert len(frame.splitlines()) == 5
    assert all(cell_len(line) < 20 for line in frame.splitlines())
    assert ">T" in frame
    assert "! STALE" in frame
    assert "STALE" in frame
    assert "0x7B" in frame


@pytest.mark.parametrize("height", [1, 2])
def test_very_short_terminal_has_exact_height(height):
    viewport = MeterViewport()
    viewport.replace_rows([_row(1)], page_size=0)

    frame = render_meter_frame(
        viewport,
        width=30,
        height=height,
        mode="passive",
        connection_status="connecting",
        no_color=True,
    )

    assert len(frame.splitlines()) == height
