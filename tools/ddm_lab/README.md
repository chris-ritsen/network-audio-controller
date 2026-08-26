# DDM lab harness

This is a repository-only clean-room test harness. It starts one or more virtual
A32s from a hash-bound opaque base image on the ordinary Dante LAN. Every guest
gets a deterministic synthetic identity, private tmpfs flash, and a unique
TAP/QMP namespace. The harness makes bounded guest-TAP captures and talks to DDM
only through the public `/graphql` Managed API. Its only mutations enroll or
unenroll active leased synthetic guests with `clearConfig: false` and fresh
readback. It contains no Dante packet parser and performs no protocol
interpretation.

Read `AGENTS.md`, `docs/agent/DDM_CLEAN_ROOM.md`, and
`docs/agent/EXPERIMENTS.md` first.

## Quick use

```console
uv run python -m tools.ddm_lab validate
uv run python -m tools.ddm_lab graphql health
uv run python -m tools.ddm_lab smoke
```

`smoke` verifies the pinned QEMU and image, checks the public GraphQL endpoint,
starts identity `001`, captures only that guest TAP with both time and size caps,
waits until DDM inventory reports the guest (using its live MAC-to-IPv4 neighbor
binding when DDM omits the interface MAC), then stops the guest, removes its TAP
and private flash, and discards the PCAP. It overwrites one small
`/dev/shm/netaudio-ddm-lab/latest-smoke.json`; it does not create an ever-growing
research archive.

For an interactive bounded run:

```console
uv run python -m tools.ddm_lab start --capture
uv run python -m tools.ddm_lab status
uv run python -m tools.ddm_lab pause
uv run python -m tools.ddm_lab snapshot --name before
uv run python -m tools.ddm_lab resume
uv run python -m tools.ddm_lab capture-stop
uv run python -m tools.ddm_lab stop
```

For a transmitter/receiver-sized topology:

```console
uv run python -m tools.ddm_lab start-many --name ios-pair --count 2 --capture
uv run python -m tools.ddm_lab status-all --topology <returned-topology-id>
uv run python -m tools.ddm_lab enroll-all-virtual --domain test
uv run python -m tools.ddm_lab capture-stop --identity 001
uv run python -m tools.ddm_lab capture-stop --identity 002
uv run python -m tools.ddm_lab capture-discard --identity 001
uv run python -m tools.ddm_lab capture-discard --identity 002
# Use Controller, NetAudio, or the iOS app for as long as needed.
uv run python -m tools.ddm_lab stop-all --topology <returned-topology-id>
```

Use repeated `--identity` arguments instead of `--count` for a nonconsecutive
set. Numeric identities are not drawn from a fixed image pool: the launcher
copies the pinned base flash, substitutes only the declared synthetic MAC field
in the generated board-information partition, and records the base, descriptor,
board-information, changed-offset, and resulting private-flash hashes. The
default concurrent-guest limit is 64 and can be changed explicitly with
`--max-active-guests`; actual capacity remains bounded by host memory, tmpfs,
capture reservations, the LAN, and DDM licensing.

`start` and `start-many` are persistent lifecycle commands, not experiments with
an automatic teardown timer. QEMU continues after the CLI exits and remains
owned by its lease until `stop` or the exact-topology `stop-all`. Captures have
independent duration/size limits and are optional; stop them after API evidence
is collected when leaving a rig available for manual, Controller, NetAudio, or
iOS testing. Runtime flash is tmpfs-backed, so this is persistent across agent
and terminal sessions, not across a host reboot.

`capture-discard` deletes only stopped PCAPs from the exact leased guest session
and retains their size/SHA-256 metadata in compact state. Use it after a harness
check that produced no protocol finding; promoted minimal evidence is copied
outside the transient session and is unaffected.

To exercise documented enrollment while retaining a coupled capture only for
the duration of the run:

```console
uv run python -m tools.ddm_lab start --capture
uv run python -m tools.ddm_lab enroll-virtual --domain test
uv run python -m tools.ddm_lab unenroll-virtual
uv run python -m tools.ddm_lab stop
```

Both mutation commands discover the active virtual device and domain IDs at
runtime, require its bounded capture to still be running, send
`clearConfig: false`, and wait for inventory readback. `ok: true` alone is never
reported as final success. Omit the unenroll command when enrollment is the
declared final state for the run.

`stop` discards the raw session by default and retains only one compact history
record; history is capped. Use `promote` before `stop` only when an explicit list
of minimal artifacts supports a real claim. `prune` is a dry run unless
`--apply` is supplied, acts only under the harness state marker, refuses leased
or live sessions, and never touches the legacy `emulation/runs` tree.

## Important limits

- The canonical image remains the hash-bound synthetic-`001` base. Other
  identities are per-run synthetic derivatives, not claims that separately
  published firmware images exist. Their exact materialization manifest lives
  with the private run and is discarded or promoted under the normal evidence
  rules.
- The identity namespace is `001` through `65535`; concurrency is explicitly
  resource-bounded rather than tied to the eight existing descriptor files. A
  single batch accepts at most 64 guests and at most 256 MiB of aggregate capture
  reservation.
- The custom QEMU executable is hash-pinned, but its source worktree is detached
  and dirty. The harness records and verifies the executable; rebuilding it is a
  separate reproducibility task.
- Captures attach to the guest TAP, never `br0` or `ddm-tap`. The DDM GraphQL API
  is a separate control path. Broadcast and multicast ingress is still visible
  on that TAP, so promote only an endpoint/time-filtered derivative rather than
  the raw capture.
- General Managed API mutations and the shipping NetAudio client remain future
  product work. The lab surface is deliberately limited to fixed inventory,
  schema, health, and active-virtual-device enrollment operations.
- No command starts audio, changes routing, starts/stops MPD, or generates a test
  signal.
