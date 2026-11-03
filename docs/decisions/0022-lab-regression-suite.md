# Check regressions against lab devices

Status: proposed November 3, 2026. The requirement for a suite that runs on
emulated or real lab devices is the user's request and is accepted; the
mechanism below waits for the user's trial.

Unit tests replay captured packets, so they miss changes in what devices
actually answer and in how the native daemon and the CLI behave together on
a real network. GitHub #70 was this kind of failure: a receiver read with
the legacy query had no media types, and planning a subscription failed only
against a real DM3.

`make test-lab LAB=lab.toml` runs `tests/lab` against the devices a local lab
file names. The file is local configuration and is never committed. A run
starts as many netaudio virtual devices on the lab interface as the file
asks for, and a private native daemon in its own user and UTS
namespaces, with its own hostname, HTTP port and metering port and without
TLS, so it never collides with a daemon already serving the host. Each check
runs once through that daemon and once directly.

Every named device is read: discovery, channel counts and names, a
subscription plan for every receiver, the subscriptions it reports, and
agreement between the daemon and direct reads. Only devices marked writable
are changed, and the suite's virtual devices are the only ones marked by
default. Each change is a round trip: baseline, change, fresh readback,
restore, fresh readback. Physical devices stay read-only unless the user
marks one writable.

Consequences: the daemon, CLI and core are exercised against current
firmware on every run, and a device that stops answering fails loudly
instead of disappearing from a fixture. The run needs a host on the Dante
LAN, Linux namespaces for the daemon checks, and the named devices online.
netaudio virtual devices publish no change notifications and do not answer
modern ARC channel status queries, so channel renames on them are expected
failures and changes are read back directly from the device rather than
through the daemon.

Alternatives considered: the QEMU A32 images boot onto the LAN, but the image
without a license plugin answers ARC queries with result `0x0601` and no
body, and the licensed image depends on research treatments the suite must
not rely on. Reusing a host's running daemon would test the installed
version rather than the code under test.
