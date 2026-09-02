import gzip
import io
import os
import stat
import tarfile
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from netaudio import DanteDevice, core
from netaudio.dante.application import CapabilityProbeTimeout
from netaudio.dante.application import DanteApplication
from netaudio.dante.capability_partition import (
    CapabilityPartitionExportError,
    parse_capability_partition_export,
    write_capability_partition,
)
from netaudio.dante.conmon_export import (
    ConmonExportCollector,
    ConmonExportError,
    ConmonExportUnavailableError,
)
from netaudio.dante.device_commands import DanteDeviceCommands
from netaudio.dante.diagnostic_logs import (
    DeviceLogExportError,
    parse_device_audio_capabilities,
    parse_device_log_export,
    write_device_log_archive,
)
from netaudio.dante.services.notification import DanteNotificationService


DEVICE_IP_ADDRESS = "10.0.2.15"
LOG_REQUEST_PAYLOAD = bytes.fromhex("ffff00280001000052550a0002020000417564696e6174650724ff04000000004c4f475300010000")
CAPABILITY_REQUEST_PAYLOAD = bytes.fromhex(
    "ffff002800010000c20f456899f50000417564696e6174650724ff04000000004341503100020000"
)


def _archive_payload(
    member_name: str = "tmp/dante_data/configd.log",
    member_payload: bytes = b"authentic device log\n",
) -> bytes:
    archive_buffer = io.BytesIO()
    with tarfile.open(fileobj=archive_buffer, mode="w", format=tarfile.USTAR_FORMAT) as archive:
        member_information = tarfile.TarInfo(member_name)
        member_information.size = len(member_payload)
        member_information.mode = 0o644
        member_information.mtime = 0
        archive.addfile(member_information, io.BytesIO(member_payload))
    return archive_buffer.getvalue()


def _encoded_payload(member_name: str, member_payload: bytes) -> bytes:
    return gzip.compress(_archive_payload(member_name, member_payload), mtime=0)


def _fragment_packet(
    echoed_tag: bytes,
    selector_value: int,
    total_encoded_size: int,
    fragment_identifier: int,
    fragment_payload: bytes,
    has_more_fragments: bool,
) -> bytes:
    packet = bytearray.fromhex("ffff0000000000000200000000010000417564696e6174650724ff0500000000")
    packet.extend(echoed_tag)
    packet.extend(total_encoded_size.to_bytes(4, "big"))
    packet.extend(selector_value.to_bytes(2, "big"))
    packet.extend(fragment_identifier.to_bytes(2, "big"))
    packet.extend(int(has_more_fragments).to_bytes(2, "big"))
    packet.extend(len(fragment_payload).to_bytes(2, "big"))
    packet.extend((28).to_bytes(2, "big"))
    packet.extend((0).to_bytes(2, "big"))
    packet.extend(fragment_payload)
    packet[2:4] = len(packet).to_bytes(2, "big")
    packet[4:6] = fragment_identifier.to_bytes(2, "big")
    return bytes(packet)


def _fragment_packets(
    echoed_tag: bytes = b"LOGS",
    selector_value: int = 1,
    member_name: str = "tmp/dante_data/configd.log",
    member_payload: bytes = b"authentic device log\n",
) -> tuple[bytes, bytes, bytes]:
    encoded_payload = _encoded_payload(member_name, member_payload)
    boundaries = (0, len(encoded_payload) // 3, 2 * len(encoded_payload) // 3, len(encoded_payload))
    return tuple(
        _fragment_packet(
            echoed_tag,
            selector_value,
            len(encoded_payload),
            index + 1,
            encoded_payload[boundaries[index] : boundaries[index + 1]],
            index < 2,
        )
        for index in range(3)
    )


def _parsed_fragments(**arguments) -> tuple[dict, dict, dict]:
    return tuple(core.parse_response("conmon_export_fragment", packet) for packet in _fragment_packets(**arguments))


def _collect_export(
    echoed_tag: bytes,
    selector_value: int,
    fragments: tuple[dict, ...],
):
    collector = ConmonExportCollector(echoed_tag, selector_value)
    result = None
    for fragment in fragments:
        result = collector.observe(fragment)
    assert result is not None
    return result


def test_export_builders_match_observed_requests():
    assert (
        core.build_command(
            {
                "command": "device_log_export",
                "host_mac": "52550a000202",
                "sequence": 1,
            }
        )
        == LOG_REQUEST_PAYLOAD
    )
    assert (
        core.build_command(
            {
                "command": "capability_partition_export",
                "host_mac": "c20f456899f5",
                "sequence": 1,
            }
        )
        == CAPABILITY_REQUEST_PAYLOAD
    )

    commands = DanteDeviceCommands()
    log_packet, _, log_port = commands.command_device_log_export(host_mac=bytes.fromhex("52550a000202"), sequence=1)
    capability_packet, _, capability_port = commands.command_capability_partition_export(
        host_mac=bytes.fromhex("c20f456899f5"), sequence=1
    )
    assert (log_packet, log_port) == (LOG_REQUEST_PAYLOAD, 8700)
    assert (capability_packet, capability_port) == (CAPABILITY_REQUEST_PAYLOAD, 8700)


def test_collector_reassembles_out_of_order_fragments_for_typed_decoders(tmp_path):
    log_fragments = _parsed_fragments()
    log_export = _collect_export(b"LOGS", 1, tuple(reversed(log_fragments)))
    log_result = parse_device_log_export(log_export)
    assert log_result.archive_payload == _archive_payload()
    assert log_result.fragment_count == 3
    assert [(member.name, member.size, member.kind) for member in log_result.members] == [
        ("tmp/dante_data/configd.log", 21, "file")
    ]

    capability_payload = bytes(range(256))
    capability_fragments = _parsed_fragments(
        echoed_tag=b"CAP1",
        selector_value=2,
        member_name="tmp/dante_data/capability.bin",
        member_payload=capability_payload,
    )
    capability_export = _collect_export(b"CAP1", 2, capability_fragments)
    capability_result = parse_capability_partition_export(capability_export)
    assert capability_result.capability_partition == capability_payload

    log_path = tmp_path / "device-logs.tar"
    capability_path = tmp_path / "capability.bin"
    write_device_log_archive(log_path, log_result.archive_payload)
    write_capability_partition(capability_path, capability_result.capability_partition)
    assert log_path.read_bytes() == log_result.archive_payload
    assert capability_path.read_bytes() == capability_payload
    if os.name != "nt":
        assert stat.S_IMODE(log_path.stat().st_mode) == 0o600
        assert stat.S_IMODE(capability_path.stat().st_mode) == 0o600
    with pytest.raises(FileExistsError):
        write_capability_partition(capability_path, b"replacement")
    assert capability_path.read_bytes() == capability_payload


def test_collector_rejects_conflicts_invalid_sequences_and_size_overflow():
    first_fragment, second_fragment, third_fragment = _parsed_fragments()
    collector = ConmonExportCollector(b"LOGS", 1)
    assert collector.observe(first_fragment) is None
    with pytest.raises(ConmonExportError, match="identifier conflicts"):
        collector.observe(first_fragment | {"data_hexadecimal": (b"x" * first_fragment["fragment_size"]).hex()})

    collector = ConmonExportCollector(b"LOGS", 1)
    assert collector.observe(third_fragment | {"has_more_fragments": True}) is None
    with pytest.raises(ConmonExportError, match="follows the terminal"):
        collector.observe(second_fragment | {"has_more_fragments": False})

    with pytest.raises(ConmonExportError, match="fields are invalid"):
        ConmonExportCollector(b"LOGS", 1, maximum_encoded_size=7).observe(
            first_fragment | {"total_encoded_size": 8, "fragment_size": 8}
        )


def test_typed_decoders_reject_the_other_export_type():
    log_export = _collect_export(b"LOGS", 1, _parsed_fragments())
    capability_export = _collect_export(
        b"CAP1",
        2,
        _parsed_fragments(
            echoed_tag=b"CAP1",
            selector_value=2,
            member_name="tmp/dante_data/capability.bin",
            member_payload=b"capability",
        ),
    )
    with pytest.raises(CapabilityPartitionExportError, match="not a CAP1"):
        parse_capability_partition_export(log_export)
    with pytest.raises(DeviceLogExportError, match="not a diagnostic-log"):
        parse_device_log_export(capability_export)


def test_audio_capabilities_are_parsed_from_apec_log():
    audio_log = b"""/cap/license.cfg
rsa.len = 128
is non-redundant
audio config dynamic 1 audio_frame 0 audio_align 0 audio start 1 default srate 48000 default enc 1 num srates 3 num enc 3
srate 44100 rxchan 64 txchan 64 mclk 4 tdm_chan 16
srate 48000 rxchan 64 txchan 64 mclk 4 tdm_chan 16
srate 96000 rxchan 32 txchan 32 mclk 4 tdm_chan 8
license rx chans 64 tx chans 64
MCLK = 4 sample_rate = 48000 TDM = 16
"""
    capabilities = parse_device_audio_capabilities(_archive_payload("tmp/dante_data/apec.log", audio_log))
    assert capabilities is not None
    assert capabilities.license_signature_length_bytes == 128
    assert capabilities.licensed_receive_channel_count == 64
    assert capabilities.licensed_transmit_channel_count == 64
    assert capabilities.licensed_redundancy_enabled is False
    assert capabilities.default_sample_rate_hertz == 48000
    assert capabilities.current_sample_rate_hertz == 48000
    assert [
        (
            capacity.sample_rate_hertz,
            capacity.receive_channel_count,
            capacity.transmit_channel_count,
        )
        for capacity in capabilities.channel_capacities
    ] == [(44100, 64, 64), (48000, 64, 64), (96000, 32, 32)]


def test_notification_service_matches_source_tag_and_selector():
    notifications = DanteNotificationService(dispatcher=MagicMock())
    waiter = notifications.register_conmon_export_waiter(DEVICE_IP_ADDRESS, b"CAP1", 2)
    packets = _fragment_packets(
        echoed_tag=b"CAP1",
        selector_value=2,
        member_name="tmp/dante_data/capability.bin",
        member_payload=b"capability",
    )
    wrong_packets = _fragment_packets()

    notifications._on_packet(packets[0], ("10.0.2.16", 8702))
    notifications._on_packet(wrong_packets[0], (DEVICE_IP_ADDRESS, 8702))
    for packet in reversed(packets):
        notifications._on_packet(packet, (DEVICE_IP_ADDRESS, 8702))

    assert waiter.event.is_set()
    assert waiter.error is None
    assert waiter.result is not None
    assert waiter.result.echoed_tag == b"CAP1"
    assert waiter.result.selector_value == 2
    notifications.unregister_waiter(waiter)
    assert not notifications.is_waiting("conmon_export", DEVICE_IP_ADDRESS)


@pytest.mark.asyncio
async def test_export_operations_use_the_shared_transport():
    application = DanteApplication()
    notifications = application.notifications

    log_packets = _fragment_packets()
    capability_packets = _fragment_packets(
        echoed_tag=b"CAP1",
        selector_value=2,
        member_name="tmp/dante_data/capability.bin",
        member_payload=b"capability",
    )

    def publish(packets):
        async def publish_fragments(device_ip_address):
            for packet in packets:
                notifications._on_packet(packet, (device_ip_address, 8702))

        return publish_fragments

    application.send_device_log_export_request = AsyncMock(side_effect=publish(log_packets))
    application.send_capability_partition_export_request = AsyncMock(side_effect=publish(capability_packets))

    log_result = await application.export_device_logs(DEVICE_IP_ADDRESS)
    capability_result = await application.export_capability_partition(DEVICE_IP_ADDRESS)

    assert log_result.archive_payload == _archive_payload()
    assert capability_result.capability_partition == b"capability"
    assert not notifications.is_waiting("conmon_export", DEVICE_IP_ADDRESS)


@pytest.mark.asyncio
async def test_export_times_out_and_recognizes_empty_response():
    application = DanteApplication()
    notifications = application.notifications
    application.send_device_log_export_request = AsyncMock()

    with pytest.raises(CapabilityProbeTimeout, match="device log export timed out"):
        await application.export_device_logs(DEVICE_IP_ADDRESS, timeout=0.001)

    device = DanteDevice()
    device.ipv4 = DEVICE_IP_ADDRESS
    application.attach_devices({"device.local.": device})

    async def publish_empty_response(device_ip_address):
        notifications._on_packet(b"", (device_ip_address, 8700))

    application.send_device_log_export_request = AsyncMock(side_effect=publish_empty_response)
    with pytest.raises(ConmonExportUnavailableError, match="empty response"):
        await application.export_device_logs(DEVICE_IP_ADDRESS)

    assert device.diagnostic_log_export_supported is False
    assert not notifications.is_waiting("conmon_export", DEVICE_IP_ADDRESS)


@pytest.mark.asyncio
async def test_application_executes_both_typed_export_requests():
    transport = SimpleNamespace(execute=AsyncMock(return_value=None))
    application = DanteApplication()
    application.transport = transport
    host_mac = bytes.fromhex("52550a000202")

    await application.send_device_log_export_request(DEVICE_IP_ADDRESS, host_mac=host_mac)
    await application.send_capability_partition_export_request(DEVICE_IP_ADDRESS, host_mac=host_mac)

    requests = [request.args for request in transport.execute.await_args_list]
    assert [address for address, _ in requests] == [DEVICE_IP_ADDRESS, DEVICE_IP_ADDRESS]
    assert [specification["command"] for _, specification in requests] == [
        "device_log_export",
        "capability_partition_export",
    ]
    assert all(specification["host_mac"] == "52550a000202" for _, specification in requests)
