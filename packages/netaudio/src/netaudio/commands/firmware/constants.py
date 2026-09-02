from __future__ import annotations

PARTITION_NAMES = {
    0: "all",
    1: "image",
    2: "fpga",
    3: "cap",
    4: "config",
    5: "_temp",
    6: "boot",
    7: "fpga96",
    8: "_psk",
    9: "cap1",
    10: "env",
    11: "user",
    12: "fpgar3",
    13: "safe",
    14: "capu",
    15: "flashlayout",
    16: "board",
    17: "_reserved",
    18: "cap2_manf",
    19: "switchphy",
    20: "data",
    21: "imxrt",
    22: "vcodec",
    23: "sii9777s",
    24: "cert",
    25: "vconfig",
    26: "tps65987d",
    27: "fpgar4",
}

CRAMFS_MAGIC_LE = b"\x45\x3d\xcd\x28"
CRAMFS_MAGIC_BE = b"\x28\xcd\x3d\x45"
GZIP_MAGIC = b"\x1f\x8b\x08"
DNT_PARSER_VERSION = 2
FIRMWARE_DATABASE_SCHEMA_VERSION = 2
UIMAGE_HEADER_SIZE = 64
CAPABILITY_9_DEVICE_DESCRIPTOR_SIZE = 0x1E
CAPABILITY_9_OEM_DESCRIPTOR_SIZE = 0x114
CAPABILITY_9_CHANNEL_NAME_SIZE = 32
BROOKLYN2_FLASH_SIZE = 0x800000
BROOKLYN2_FLASH_PARTITIONS = (
    ("safe", 0x000000, 0x150000),
    ("brdinfo", 0x150000, 0x010000),
    ("bootenv", 0x160000, 0x010000),
    ("boot", 0x170000, 0x030000),
    ("fpga", 0x1A0000, 0x100000),
    ("image", 0x2A0000, 0x320000),
    ("userarea", 0x5C0000, 0x200000),
    ("config", 0x7C0000, 0x020000),
    ("cap1", 0x7E0000, 0x010000),
    ("cap", 0x7F0000, 0x010000),
)
BROOKLYN2_PAYLOAD_PARTITION_NAMES = ("boot", "fpga", "image", "userarea", "cap1")
BROOKLYN2_HARDWARE_PROFILE_FORMAT_VERSION = 2
BROOKLYN2_BOARD_INFORMATION_DESCRIPTOR_FORMAT_VERSION = 2
BROOKLYN2_IMAGE_MANIFEST_FORMAT_VERSION = 4
BROOKLYN2_EVIDENCE_MANIFEST_MAXIMUM_SIZE = 4 * 1024 * 1024
BROOKLYN2_EVIDENCE_REQUEST_MAXIMUM_SIZE = 1024 * 1024
BROOKLYN2_EVIDENCE_FIRMWARE_MAXIMUM_SIZE = 64 * 1024 * 1024
BROOKLYN2_EVIDENCE_ARTIFACT_MAXIMUM_SIZE = 128 * 1024 * 1024
BROOKLYN2_EVIDENCE_ALL_ARTIFACTS_MAXIMUM_SIZE = 256 * 1024 * 1024
LINUX_CURRENT_WORKING_DIRECTORY_DESCRIPTOR = -100
LINUX_RENAME_WITHOUT_REPLACEMENT = 1
MACOS_RENAME_EXCLUSIVE = 4
(
    BROOKLYN2_BOARD_INFORMATION_PARTITION_OFFSET,
    BROOKLYN2_BOARD_INFORMATION_PARTITION_SIZE,
) = next(
    (partition_offset, partition_size)
    for partition_name, partition_offset, partition_size in BROOKLYN2_FLASH_PARTITIONS
    if partition_name == "brdinfo"
)
(
    BROOKLYN2_PROTECTED_CAPABILITY_PARTITION_OFFSET,
    BROOKLYN2_PROTECTED_CAPABILITY_PARTITION_SIZE,
) = next(
    (partition_offset, partition_size)
    for partition_name, partition_offset, partition_size in BROOKLYN2_FLASH_PARTITIONS
    if partition_name == "cap"
)
