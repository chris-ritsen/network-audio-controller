import zlib

SESSION_MEMBERSHIP_SQL = """id IN (
    SELECT packet_id FROM packet_sessions WHERE session_id = ?
    UNION
    SELECT id FROM packets WHERE session_id = ?
)"""


def extract_evidence_packet_ids(markers: list[dict]) -> set[int]:
    packet_ids = set()
    for marker in markers:
        if marker.get("marker_type") != "evidence":
            continue
        marker_data = marker.get("data")
        if not marker_data:
            continue
        marker_packet_ids = marker_data.get("packet_ids")
        if marker_packet_ids is None and marker_data.get("packet_id") is not None:
            marker_packet_ids = [marker_data["packet_id"]]
        for packet_id in marker_packet_ids or []:
            packet_ids.add(int(packet_id))
    return packet_ids


def decompress_payload(data):
    if not data:
        return b""
    if isinstance(data, str):
        return bytes.fromhex(data)
    try:
        return zlib.decompress(data)
    except zlib.error:
        return data


def safe_name(name):
    return "".join(character if character.isalnum() or character in ("_", "-") else "_" for character in name)
