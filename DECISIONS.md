# Decisions

Architecture and distribution decisions for netaudio. These were argued out once; don't re-litigate them without new facts.

## Product commitments

- The iOS app is definite: one-time purchase, no subscription, $49.99 floor.
- The virtual sound card (inferno-style JACK–Dante bridge) is definite. The Python `virtual_device.py` is a control-plane prototype; the audio data plane (RTP transport, sample handling) belongs in netaudio-core, where performance actually matters.
- The CLI/daemon stays the free public reference implementation.

## Architecture

- **netaudio-core (Rust) is the only protocol implementation.** Python contains no wire-format code. Every consumer — Python CLI/daemon, the iOS app, the future JACK bridge and CoreAudio driver — binds the same library.
- **Hand-rolled C ABI + cbindgen, not uniffi.** uniffi is right when you own all consumers; the C ABI lets any language bind without a binding generator in the loop. This was deliberate. Keep it.
- **ABI discipline:** any FFI signature change bumps `NETAUDIO_ABI_VERSION` in `ffi/mod.rs` and `binding.py` together, plus `make header`. The binding refuses to load a mismatched library rather than segfault.
- **Crate artifacts:** one build produces cdylib (Python loads), staticlib (iOS links), rlib (Rust-internal).
- Python remains the orchestration layer: CLI, daemon, relay HTTP API. Event-driven, Redis-backed, dbus-fast for systemd (launchctl subprocess is the sole exception — launchd has no library API).

## Distribution

- **Hatch build hook (`hatch_build_core.py`), not maturin.** maturin would reshape the project around the crate to get wheel tagging for free; the hook does the same job in a few lines.
- **Wheels are tagged `py3-none-<platform>`.** The library is ctypes-loaded, not a CPython extension, so one wheel per platform — never per Python version. Never publish a platform binary under `py3-none-any`.
- **The sdist compiles on install.** If no built library exists, the hook runs `cargo build --release`; it errors only when cargo is absent. First-class platforms get prebuilt wheels; everything else (FreeBSD, musl, Pi) gets a working compile-from-source path.
- **Releases run on GitHub Actions on the public repo** (free, including macOS runners). Native runner per architecture — no cross-compilation, ever. That matrix is where multi-platform Rust projects rot.
  - ubuntu-22.04 → linux x86_64 wheel (old glibc floor for the manylinux tag)
  - ubuntu-22.04-arm → linux aarch64 wheel
  - macos-14 → macOS arm64 wheel (`MACOSX_DEPLOYMENT_TARGET=11.0`)
  - windows-latest → win_amd64 wheel (real Windows users exist: issues #9/#17/#42)
  - macos-14 → `netaudio_core.xcframework.zip` (device + simulator staticlibs) as a GitHub Release asset
  - every wheel job smoke-tests its own wheel (`netaudio --help` — exercises the FFI load via core.require)
  - Intel macOS deliberately skipped (sdist covers it); glibc floor drops via manylinux container only if someone complains
- **Two distribution channels per tag.** GitHub Releases gets everything (wheels, sdist, XCFramework — the Swift binaryTarget URL lives here); PyPI gets wheels + sdist via trusted publishing (OIDC, configured 2026-06-12, no tokens). Versions are sequence counters: patch increment every release (0.2.4 → 0.2.5), no semver signaling — breaking changes are expected per CONTRIBUTING.md. PyPI versions are immutable, so a published version number is burned forever.
- **Version of record is `pyproject.toml`.** A release is: bump version, push matching `v*` tag. CI fails the release if they disagree.
- **Local dev is unchanged:** `make core` + `make install` per machine. CI is only the release path.
- **macOS codesigning comes with the macOS daemon, not before.** Sign with Developer ID so the binary keeps a stable TCC identity (Local Network grant for mDNS/multicast) across reinstalls. Two repo secrets and one signing step when the time comes.
- Intel macOS gets no prebuilt wheel unless someone asks; the sdist covers it.

## Rejected — and why

- **cargo-dist:** ships standalone Rust binaries; our deliverable is a wheel with a library inside, which it can't produce. Stolen from litter instead: the XCFramework recipe, trusted publishing, the codesigning-for-TCC note.
- **uniffi:** see C ABI decision above.
- **maturin:** see hatch hook decision above.
- **`daemon install --system`:** a root unit pointing into `~/.local/share/uv/tools/` is fragile and wrong. Revisit only when a system-wide install path exists (distro package, `/usr/local/bin`).
- **`daemon install --linger`:** lingering is account-wide, not per-service; a tool quietly reconfiguring login-manager lifecycle semantics erodes trust. The user runs `loginctl enable-linger` themselves.
- **Educational hints in command output:** don't assume the user is stupid. Output states what was done, not what the user might want to learn.

## Taste

- Explicit commands over magic. Each install flavor is its own command doing exactly one thing; no flag silently changes state outside netaudio's own scope.
- `install` never overwrites a user-managed unit without `--force`; `uninstall` never deletes anything lacking the `X-NetaudioManaged` marker.
- Generated units reference the stable `~/.local/bin/netaudio` shim, never resolved symlinks into uv internals.
- Protocol research: protocol knowledge comes from packet captures and runtime observation. No decompiled names anywhere.
