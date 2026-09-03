# Code audit

Audit opened and completed 2026-09-03 against commit `6327ffc`.

The audit is complete only when every item below is resolved, focused regression
tests pass, the full Python and Rust suites pass, and a final tracked-tree scan
finds no remaining instance of the reported defect or placeholder behavior.

## Correctness and safety

- [x] Preserve the selected managed-device identity throughout command routing.
      Devices in different DDM contexts may have the same IP address, and a
      managed device may have no usable IP address. Ambiguous or incomplete
      managed identities now fail closed.
- [x] Make generated GraphQL Boolean options use paired `--enabled` and
      `--no-enabled` flags.
- [x] Keep passwords, credentials, and API-key material out of command arguments,
      dry-run output, logs, and ordinary rendered output.
- [x] Allow configuration recovery commands to run when configuration parsing
      fails, and report configuration errors as configuration errors.
- [x] Treat malformed or rejected generated mutation responses as failures.
- [x] Generate minimal, operation-specific GraphQL selection sets instead of
      recursively requesting the entire reachable schema.

## Interface quality

- [x] Replace the placeholder `device config clock-source` behavior with verified
      raw-code get/set support.
- [x] Remove duplicate generated serial-port commands and avoid blindly exposing
      unsuitable schema operations as ordinary commands.
- [x] Require explicit confirmation for destructive low-level DDM mutations.
- [x] Organize generated DDM operations as `api read` and resource-oriented
      `api write` commands instead of one flat command list.
- [x] Make bare commands with required parameters display full help and exit with
      status 2 consistently across supported Python versions.
- [x] Remove stale DDM configuration terminology and stale latency constants.

## Test and evidence quality

- [x] Replace test-local packet parsers with assertions against production
      parsers, and remove duplicate fixture tests that add no coverage.
- [x] Fail when required committed fixtures are missing instead of skipping.
- [x] Make schema-command coverage tests accurately enumerate the generated
      command surface.
- [x] Add provenance and SHA-256 records for promoted capture-derived fixtures
      that currently lack them, or remove fixtures that are not needed.
- [x] Remove unused fixture files and test-only dead code; retain only regression
      inputs exercised by production parsers or the Rust parity suite.

## Validation

- [x] `uv run ruff check .`
- [x] `uv run ruff format --check .`
- [x] `uv run pyright`
- [x] `uv run pytest -q` on Python 3.9 and 3.13: 2,249 passed, 3 platform skips.
- [x] `cargo fmt --manifest-path packages/netaudio-core/Cargo.toml --check`
- [x] `cargo clippy --manifest-path packages/netaudio-core/Cargo.toml --all-targets -- -D warnings`
- [x] `cargo test --manifest-path packages/netaudio-core/Cargo.toml`: 311 passed.
- [x] Exhaustive `-h` and bare-required-parameter checks for all 299 CLI paths.
- [x] Wheel build, artifact verification, isolated install, CLI smoke test, and uninstall.
- [x] C header verification and website validation.
- [x] Shell syntax, browser JavaScript syntax, and workflow YAML structure validation.
- [x] Vulture dead-code scan at 90% confidence.
- [x] Final placeholder, dead-code, skipped-test, duplicate-interface, and policy
      scan reviewed.
