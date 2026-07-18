import ast
from pathlib import Path

import pytest

from netaudio import core
from netaudio.dante.device_operations import (
    LOCK_OPERATION_LOCK,
    LOCK_OPERATION_UNLOCK,
    _device_lock_operation,
)

SOURCE_ROOT = Path(__file__).parents[1] / "packages" / "netaudio" / "src" / "netaudio"

RAW_PACKET_MODULES = {
    "commands/capture.py",
    "commands/fact.py",
    "commands/provenance.py",
    "common/key_extract.py",
    "dante/packet_store.py",
    "dante/tshark_capture.py",
    "dante/virtual_device.py",
}

CMC_EXCEPTIONS = {
    ("_get_host_mac", "bytes.fromhex"),
    ("_get_host_mac", "uuid.getnode().to_bytes"),
}

NON_PACKET_BINARY_CONCATENATION = {
    ("commands/firmware.py", "_find_oem_strings"),
    ("dante/protocol_verifier.py", "export_session_bundle"),
}


def _call_name(node: ast.Call) -> str:
    return ast.unparse(node.func)


def _enclosing_function(node: ast.AST, parents: dict[ast.AST, ast.AST]) -> str:
    while node in parents:
        node = parents[node]
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            return node.name
    return "<module>"


def _is_os_structure_pack(node: ast.Call) -> bool:
    if not node.args or not isinstance(node.args[0], ast.Constant) or not isinstance(node.args[0].value, str):
        return False
    return node.args[0].value in {"4s4s", "4sL", "256s"}


def _contains_bytes_literal(node: ast.AST) -> bool:
    return any(isinstance(child, ast.Constant) and isinstance(child.value, bytes) for child in ast.walk(node))


def test_production_wire_builders_are_owned_by_netaudio_core():
    violations = []

    for path in sorted(SOURCE_ROOT.rglob("*.py")):
        relative = path.relative_to(SOURCE_ROOT).as_posix()
        tree = ast.parse(path.read_text())
        parents = {child: parent for parent in ast.walk(tree) for child in ast.iter_child_nodes(parent)}

        for node in ast.walk(tree):
            function = _enclosing_function(node, parents)

            if isinstance(node, ast.Call):
                call = _call_name(node)
                if call == "bytearray":
                    if relative in {"commands/firmware.py", "dante/virtual_device.py"}:
                        continue
                    violations.append(f"{relative}:{node.lineno}: {function} constructs a bytearray")
                    continue
                if call not in {"struct.pack", "struct.pack_into", "bytes.fromhex"} and not call.endswith(".to_bytes"):
                    continue

                if relative in RAW_PACKET_MODULES:
                    continue
                if call == "struct.pack" and _is_os_structure_pack(node):
                    continue
                if relative == "dante/services/cmc.py" and (function, call) in CMC_EXCEPTIONS:
                    continue

                violations.append(f"{relative}:{node.lineno}: {function} uses {call}")

            if not isinstance(node, ast.BinOp) or not isinstance(node.op, ast.Add) or not _contains_bytes_literal(node):
                continue
            parent = parents.get(node)
            if isinstance(parent, ast.BinOp) and isinstance(parent.op, ast.Add):
                continue
            if relative in RAW_PACKET_MODULES or (relative, function) in NON_PACKET_BINARY_CONCATENATION:
                continue
            violations.append(f"{relative}:{node.lineno}: {function} manually concatenates binary data")

    assert not violations, "Move production packet construction to netaudio-core:\n" + "\n".join(violations)


class _FakeCoreClient:
    instances = []

    def __init__(self, device_ip):
        self.device_ip = device_ip
        self.calls = []
        self.closed = False
        self.instances.append(self)

    def __enter__(self):
        return self

    def __exit__(self, *_exc_info):
        self.closed = True

    def lock(self, pin, key):
        self.calls.append(("lock", pin, key))
        return {"success": True, "status": 0, "lock_state": 1, "already": False}

    def unlock(self, pin, key):
        self.calls.append(("unlock", pin, key))
        return {"success": True, "status": 0, "lock_state": 0, "already": False}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("operation", "expected_call", "expected_state"),
    [
        (LOCK_OPERATION_LOCK, "lock", 1),
        (LOCK_OPERATION_UNLOCK, "unlock", 0),
    ],
)
async def test_lock_exchange_delegates_to_native_core(monkeypatch, operation, expected_call, expected_state):
    _FakeCoreClient.instances.clear()
    monkeypatch.setattr(core, "CoreClient", _FakeCoreClient)
    key = b"k" * 32

    result = await _device_lock_operation("192.0.2.10", "1234", key, operation)

    assert result["lock_state"] == expected_state
    assert len(_FakeCoreClient.instances) == 1
    client = _FakeCoreClient.instances[0]
    assert client.device_ip == "192.0.2.10"
    assert client.calls == [(expected_call, "1234", key)]
    assert client.closed is True


@pytest.mark.asyncio
async def test_lock_exchange_rejects_unknown_operation_before_opening_client(monkeypatch):
    _FakeCoreClient.instances.clear()
    monkeypatch.setattr(core, "CoreClient", _FakeCoreClient)

    with pytest.raises(ValueError, match="unknown lock operation"):
        await _device_lock_operation("192.0.2.10", "1234", b"k" * 32, 99)

    assert _FakeCoreClient.instances == []
