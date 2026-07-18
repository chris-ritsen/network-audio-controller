from pathlib import Path


def response_input_bytes(fixtures_directory: Path, name, entry):
    if "hex" in entry:
        response = bytearray.fromhex(entry["hex"])
        if name in {"synthetic_make_model", "synthetic_dante_model"}:
            opcode = 0x00C0 if name == "synthetic_make_model" else 0x0060
            response[0:2] = (0xFFFF).to_bytes(2, "big")
            response[2:4] = len(response).to_bytes(2, "big")
            response[16:24] = b"Audinate"
            response[24] = 0x07
            response[26:28] = opcode.to_bytes(2, "big")
        return bytes(response)
    return (fixtures_directory / name).read_bytes()
