# NetAudio agent policy

This is the canonical agent policy for this repository. `CLAUDE.md` and task
handoffs may add context, but they may not weaken these rules. Read the
task-specific documents linked below before doing DDM or lab work.

## Product and research boundary

- Shipping NetAudio code is a library, CLI, and daemon. Research tooling,
  captures, emulators, firmware images, VM state, and runtime observations are
  lab inputs, not production dependencies.
- The shipping DDM client may use only the documented Dante Managed API at the
  configurable `/graphql` endpoint. It must not use packet capture, process
  memory, internal ports, VM state, or emulator state at runtime.
- Protocol behavior must be established through the clean-room evidence allowed
  by the active task. Do not import meanings, constants, names, or structures
  from prohibited sources.
- Classify research claims as `documented`, `observed`, `causal`, `inferred`, or
  `unknown`. Green tests do not upgrade the evidence class.

## Clean-room rules

- Do not decompile, disassemble, inspect strings in, extract, mount, or otherwise
  statically inspect Dante firmware, DDM, or Dante Controller executables.
- Do not use recovered source, source maps, firmware-derived symbol catalogs,
  copied/deprecated status catalogs, or prior conclusions whose provenance is
  outside the active clean-room boundary.
- Never put decompiled/internal symbol names or firmware-derived identifiers in
  product code, tests, comments, or documentation.
- Preserve exact capture and artifact provenance: source, timestamp or frame,
  scope, SHA-256, and allowed/excluded evidence.
- Read `docs/agent/DDM_CLEAN_ROOM.md` before DDM or authentic-firmware lab work.

## Live systems and mutations

- Read-only is the default. Do not start, stop, restart, pause, resume, connect,
  route, capture from, or mutate a service, VM, emulator, interface, device, or
  audio path unless the current task explicitly authorizes it.
- Never generate audio, play a tone, change audio routing, or start/stop an audio
  service as an incidental test.
- DDM experiments may mutate only virtual devices explicitly designated as
  disposable for that run. Physical AVIOs and all other physical Dante devices
  are read-only unless Chris separately authorizes the exact mutation.
- A live mutation requires an exclusive resource lease, a bounded run manifest,
  a saved baseline, fresh readback, and restoration unless the authorized run
  declares the new state as its intended final state. Follow
  `docs/agent/EXPERIMENTS.md`.
- `tools/ddm_lab enroll-virtual`, `unenroll-virtual`, `enroll-all-virtual`, and
  `unenroll-all-virtual` are the only standing DDM mutations in the lab harness.
  They require every selected leased synthetic guest to be running under its own
  bounded guest-TAP capture, resolve each live ID through the public API, force
  `clearConfig: false`, and wait for readback. Batch commands select only active
  harness leases; they never make a physical device an implicit target.
- Runtime-memory inspection is observational by default. Runtime writes are
  prohibited unless a later explicit authorization names the exact virtual
  target and data treatment. Never patch instructions, branches, or return
  values.
- Do not weaken the DDM VM's outbound-Internet isolation.

## Secrets and captures

- Never print, log, commit, embed, or copy API keys, credentials, authorization
  headers, or secret-bearing command environments into artifacts or manifests.
- Minimal binary packet/capture fixtures are allowed when their clean-room
  provenance and hashes are recorded. Do not commit proprietary firmware, DDM
  or Controller binaries, VM images, private writable flash, or credentials.
- Lab endpoints and credential-file locations are local defaults. Shipping code
  must accept configuration and must not hard-code them.
- Bound every capture by interface, endpoints, duration, and output path. Record
  its digest. Do not capture unrelated traffic when a narrower filter suffices.

## Shared worktree and Git

- Inspect `git status`, branch, remotes, and relevant diffs before editing.
  Existing dirty and untracked paths belong to the user or another agent unless
  explicitly assigned.
- Do not use `git checkout`, `git restore`, `git reset`, or equivalent commands
  to overwrite files without explicit user confirmation. Show the exact command
  and target first.
- Do not commit, amend, rebase, merge, push, force-push, or change remotes unless
  requested. GPG signing may be disabled for this repository when needed; that
  does not authorize bypassing other hooks.
- Multi-agent work must use non-overlapping path ownership. Only the run owner
  writes shared manifests; collaborators write within their assigned namespace.

## Validation

- Keep hand-written source and test files at 900 lines or fewer. Split by
  responsibility instead of compressing code to stay under the limit.
- Default tests must be offline and deterministic. Use focused tests first, then
  `uv run pytest -q` and the relevant Rust checks when the change warrants them.
- Live tests must be opt-in. Mutating live tests require a second explicit opt-in
  and an authorized disposable target; they do not run in ordinary CI.
- Promote only minimal, digest-bound evidence into repository fixtures. Draft
  captures and research runs belong outside the source tree.
