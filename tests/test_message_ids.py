from concurrent.futures import ThreadPoolExecutor

from netaudio import core
from netaudio.dante.commands import DanteCommands
from netaudio.dante.panel_transport import PanelTransport


def test_panel_transports_share_the_native_sequence_and_skip_pending_identifiers():
    first = PanelTransport(None)
    second = PanelTransport(None)
    previous = first.next_sequence()
    second.pending.add(previous + 1)
    following = second.next_sequence()

    assert following == previous + 2
    assert core.next_panel_sequence() == following + 1


def test_native_and_python_callers_share_one_nonzero_message_id_cycle():
    native = core.require().netaudio_next_message_id
    identifiers = [(native if index % 2 else core.next_message_id)() for index in range(65535)]

    assert set(identifiers) == set(range(1, 65536))
    assert core.next_message_id() == identifiers[0]


def test_command_instances_share_the_binding_sequence_without_overwriting_panel_sequence():
    previous = core.next_message_id()
    first = DanteCommands().identify()["message_id"]
    panel = DanteCommands().panel_control({}, requester=1, sequence=17)
    last = core.next_message_id()

    assert first == previous % 65535 + 1
    assert panel["message_id"] == first % 65535 + 1
    assert last == panel["message_id"] % 65535 + 1
    assert panel["sequence"] == 17


def test_concurrent_command_construction_does_not_reuse_message_ids():
    with ThreadPoolExecutor(max_workers=4) as executor:
        identifiers = list(executor.map(lambda _: DanteCommands().identify()["message_id"], range(1000)))

    assert len(set(identifiers)) == len(identifiers)
    assert 0 not in identifiers
