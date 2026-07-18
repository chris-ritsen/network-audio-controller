from subprocess import CompletedProcess

from netaudio.shure import discovery


def _completed(command, stdout="", returncode=0):
    return CompletedProcess(command, returncode, stdout=stdout, stderr="")


def test_linux_neighbor_discovery_filters_for_shure_oui(monkeypatch):
    output = """\
192.168.1.20 dev eth0 lladdr 00:0E:DD:AA:BB:CC REACHABLE
192.168.1.21 dev eth0 lladdr 00:11:22:33:44:55 STALE
"""
    calls = []

    def run(command, **kwargs):
        calls.append((command, kwargs))
        return _completed(command, output)

    monkeypatch.setattr(discovery.sys, "platform", "linux")
    monkeypatch.setattr(discovery.subprocess, "run", run)

    assert discovery.get_shure_neighbor_entries() == [("192.168.1.20", "00:0e:dd:aa:bb:cc")]
    assert calls[0][0] == ("ip", "neigh", "show")


def test_macos_neighbor_discovery_parses_bsd_arp_output(monkeypatch):
    output = """\
? (192.168.1.30) at 0:e:dd:1:2:3 on en0 ifscope [ethernet]
? (192.168.1.31) at (incomplete) on en0 ifscope [ethernet]
"""
    calls = []

    def run(command, **kwargs):
        calls.append(command)
        return _completed(command, output)

    monkeypatch.setattr(discovery.sys, "platform", "darwin")
    monkeypatch.setattr(discovery.subprocess, "run", run)

    assert discovery.get_shure_neighbor_entries() == [("192.168.1.30", "00:0e:dd:01:02:03")]
    assert calls == [("arp", "-an")]


def test_windows_neighbor_discovery_parses_hyphenated_mac(monkeypatch):
    output = """\
Interface: 192.168.1.5 --- 0x6
  Internet Address      Physical Address      Type
  192.168.1.40          00-0e-dd-a1-b2-c3     dynamic
"""
    calls = []

    def run(command, **kwargs):
        calls.append(command)
        return _completed(command, output)

    monkeypatch.setattr(discovery.sys, "platform", "win32")
    monkeypatch.setattr(discovery.subprocess, "run", run)

    assert discovery.get_shure_neighbor_entries() == [("192.168.1.40", "00:0e:dd:a1:b2:c3")]
    assert calls == [("arp", "-a")]


def test_linux_falls_back_to_arp_when_ip_command_is_missing(monkeypatch):
    def run(command, **kwargs):
        if command[0] == "ip":
            raise FileNotFoundError("ip is not installed")
        return _completed(command, "? (10.0.0.50) at 00:0e:dd:00:00:01 [ether] on eth0\n")

    monkeypatch.setattr(discovery.sys, "platform", "linux")
    monkeypatch.setattr(discovery.subprocess, "run", run)

    assert discovery.get_shure_neighbor_entries() == [("10.0.0.50", "00:0e:dd:00:00:01")]


def test_missing_neighbor_commands_are_a_quiet_empty_result(monkeypatch):
    monkeypatch.setattr(discovery.sys, "platform", "darwin")
    monkeypatch.setattr(
        discovery.subprocess,
        "run",
        lambda command, **kwargs: (_ for _ in ()).throw(FileNotFoundError(command[0])),
    )

    assert discovery.get_shure_neighbor_entries() == []


def test_failed_or_malformed_neighbor_commands_are_ignored(monkeypatch):
    results = iter(
        (
            _completed(("ip", "neigh", "show"), returncode=1),
            _completed(("arp", "-an"), "999.999.999.999 at 00:0e:dd:aa:bb:cc\n"),
        )
    )

    monkeypatch.setattr(discovery.sys, "platform", "linux")
    monkeypatch.setattr(discovery.subprocess, "run", lambda command, **kwargs: next(results))

    assert discovery.get_shure_neighbor_entries() == []
