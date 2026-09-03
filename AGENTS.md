# NetAudio agent policy

This is the canonical agent policy for this repository. `CLAUDE.md` and task
handoffs may add context or narrow a task, but they may not weaken these rules.

## Project purpose and scope

- NetAudio is primarily an interoperability reverse-engineering and protocol
  research project for Dante Controller and related software. It also produces
  useful end-user software: a library, CLI, daemon/server components, and
  potentially TUI, GUI, and web interfaces.
- Protocol research is first-class project work. Bounded lab harnesses,
  emulators, experiment tooling, findings, provenance records, and minimal
  evidence fixtures may belong in the repository when they make the work
  reproducible or reviewable.
- Most end users should be able to ignore the research surface and use the
  product interfaces normally. Shipping features must not require a lab,
  proprietary applications, packet captures, firmware, VM state, or research
  artifacts at runtime.
- The project independently establishes interoperability facts and keeps a
  defensible evidence trail. This policy reduces avoidable legal and provenance
  risk; it does not declare any activity legally risk-free.

## Independent research and evidence

- Do not join the words `clean` and `room`—with a space, hyphen, underscore, or
  no separator—in committed filenames or contents. Use neutral terms such as
  `protocol research`, `evidence boundary`, `capture-derived`, or `independent
  implementation`.
- Do not invent a named parallel process for excluded evidence. Identify
  excluded sources directly and explain why they are excluded.
- Permitted evidence includes public documentation and APIs; packet captures
  from real or synthetic devices; official tools exercised through their normal
  user-visible interfaces; controlled black-box experiments; authorized runtime
  observation; and causal treatments performed on authorized targets.
- Do not decompile, disassemble, inspect strings in, extract, mount, or otherwise
  statically inspect Dante firmware, DDM, or Dante Controller executables.
- Do not use recovered source, source maps, firmware-derived symbol catalogs,
  leaked material, copied internal status catalogs, or conclusions whose
  provenance falls outside the active task's evidence boundary.
- Never put decompiled or internal symbol names, firmware-derived identifiers,
  or unsupported proprietary terminology in product code, tests, comments, or
  documentation.
- Classify research claims as `documented`, `observed`, `causal`, `inferred`, or
  `unknown`. Passing tests do not upgrade the evidence class.
- Preserve exact capture and artifact provenance: source device or tool, run,
  timestamp or frame, scope, SHA-256, and allowed/excluded evidence. Record
  transformations from a source capture to a promoted fixture.
- Packet captures and ordinary experimentation sessions with official tools may
  support provenance records. Promote only the minimum sanitized, digest-bound
  packets or derived fixtures needed to substantiate a claim or deterministic
  test. Large captures and complete research runs normally remain outside the
  source tree.

## Product and research boundary

- Shipping protocol behavior must be implemented from permitted evidence and
  stand on its own as reviewed source, focused tests, and necessary user or
  operational documentation.
- Research tools and observations are development inputs, not hidden production
  dependencies. Keep experimental paths explicit and opt-in.
- Shipping DDM functionality may use both the documented Dante Managed API and
  independently derived Controller-compatible network services, including
  mDNS-discovered managed-control transports. Undocumented transports must be
  implemented from permitted evidence, described as version-scoped where the
  evidence is version-scoped, validate framing and state transitions strictly,
  and fail closed on unsupported variants.
- Shipping DDM functionality must not depend on packet capture, process memory,
  proprietary applications, VM state, emulator state, or research artifacts at
  runtime. Endpoints and credentials must come from explicit configuration or
  ordinary network discovery; do not hard-code lab infrastructure.
- Keep Dante wire-format encoding and parsing in the Rust core unless an
  explicit architecture decision establishes another boundary. Do not create
  silent parallel protocol implementations.
- Do not commit proprietary firmware, DDM or Controller binaries, VM images,
  private writable flash, credentials, or unrelated personal infrastructure.

## Interface evolution

- NetAudio is greenfield experimental work and has no compatibility or API
  stability contract. Do not characterize replacement or removal of an
  experimental interface as a breaking change.
- Do not preserve obsolete NetAudio behavior by default. When a CLI command,
  option, configuration shape, output field, identifier format, or internal API
  is replaced, remove the superseded path and its tests in the same change.
- Do not add deprecated aliases, transitional fallbacks, silent migrations, or
  version-selection defaults unless the current task explicitly requires one.
- Deliberate support for an older Dante device or wire-protocol revision is
  interoperability, not a NetAudio compatibility shim. Keep such support only
  when the revision is identified explicitly; unknown revisions must fail
  closed instead of selecting a presumed protocol.

## Live systems and mutations

- Read-only is the default. Do not start, stop, restart, pause, resume, connect,
  route, capture from, or mutate a service, VM, emulator, interface, device, or
  audio path unless the current task explicitly authorizes it.
- Never generate audio, play a tone, change audio routing, or start or stop an
  audio service as an incidental test.
- Experiments may mutate only virtual devices explicitly designated as
  disposable for that run. Physical AVIOs and all other physical Dante devices
  are read-only unless Chris separately authorizes the exact mutation.
- A live mutation requires an exclusive resource lease, a bounded run manifest,
  a saved baseline, fresh readback, and restoration unless the authorized run
  declares the new state as its intended final state.
- Runtime-memory inspection is observational by default. Runtime writes require
  explicit authorization naming the virtual target and data treatment. Never
  patch instructions, branches, or return values.
- Do not weaken the DDM VM's outbound-Internet isolation.

## Secrets and captures

- Never print, log, commit, embed, or copy API keys, credentials, authorization
  headers, or secret-bearing command environments into artifacts or manifests.
- Lab endpoints and credential-file locations are local configuration. Shipping
  code must accept configuration and must not hard-code them.
- Bound every capture by interface, endpoints, duration, and output path. Record
  its digest and avoid unrelated traffic when a narrower filter is possible.
- Existing dirty and untracked research artifacts belong to the user or another
  agent. Preserve them. Do not stage them unless the task explicitly promotes
  them under the evidence rules above.

## Shared worktree and Git

- Inspect Git status, branch, remotes, and relevant diffs before editing.
  Existing dirty and untracked paths belong to the user or another agent unless
  explicitly assigned.
- Stage exact paths only in a mixed worktree. Never use broad staging as a
  shortcut.
- Do not use `git checkout`, `git restore`, `git reset`, or equivalent commands
  to overwrite files without explicit user confirmation. Show the exact command
  and target first.
- Do not commit, amend, rebase, merge, push, force-push, rewrite history, or
  change remotes unless requested. GPG signing may be disabled when needed; that
  does not authorize bypassing other hooks.
- Multi-agent work must use non-overlapping path ownership. Only the run owner
  writes shared manifests; collaborators write within their assigned namespace.
- Put repository-maintenance and build automation under `scripts/`. Research
  harnesses that need lab hardware, VMs, or personal paths live outside this
  repository.

## Validation

- Split modules by responsibility, never by line count. A file named after its
  parent plus a suffix is not a module.
- `uv run ruff check .` must be clean, including undefined names, unused
  imports, and redefinitions.
- Default tests must be offline and deterministic. Use focused tests first, then
  `uv run pytest -q` and relevant Rust checks when the change warrants them.
- Live tests must be opt-in. Mutating live tests require a second explicit opt-in
  and an authorized disposable target; they do not run in ordinary CI.
- CI must remain portable across its declared operating systems. Isolate and
  condition platform-specific behavior honestly.
- Do not weaken checks, add exception inventories, or hide failures to make CI
  pass. Fix the implementation or reduce the enforced scope explicitly.
