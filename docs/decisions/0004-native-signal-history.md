# Native signal history and persisted coverage

Date: 2026-10-20

Status: Proposed implementation of the accepted Rust daemon requirement.

## Context

The Python daemon keeps channel peaks and signal coverage for fifteen minutes
at second resolution and twenty-four hours at minute resolution. It saves
compressed machine-order arrays only on shutdown. Some saved coverage combines
missing readings with silence, and minute counts lose the positions of
recorded seconds.

Chris requested the Rust rewrite with unchanged consumer interfaces and
authorized fixing defects during the port.

## Decision

Keep the existing history periods and MCP arguments. Record measured silence
separately from unavailable readings. Retain per-minute bit sets for coverage
and signal so counts and signal boundaries remain accurate when combining
channels or selecting part of a minute. Mark an unavailable boundary peak
explicitly when the detailed samples have expired.

Persist changed buckets transactionally in SQLite. A single owned worker
coalesces observation-driven writes for one second and flushes after producers
stop during graceful shutdown. It does not periodically sample devices.
Timed listening retains its meter subscription until its deadline or
cancellation.

Read existing Python history in the same database without modifying its
original tables. Qualify its inherited coverage uncertainty rather than
present it as newly observed silence. New buckets use typed SQLite columns,
explicit layout versioning and exact coverage masks.

## Consequences

Existing clients keep their request interfaces and historical observations.
A crash can lose the pending coalescing window; completed transactions survive
process failure. The new tables add storage until saved history is retired.
Performance and restart behavior require qualification before cutover.

Whole-track snapshot writes were considered but would repeatedly serialize
unchanged history. A separate Python history service would retain a Python
daemon dependency and conflicts with the accepted rewrite.
