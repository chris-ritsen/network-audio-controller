# Durable native event journal

Date: 2026-10-20

Status: Proposed implementation of the accepted Rust daemon requirement.

## Context

The Python daemon retains a bounded journal in memory and saves it on shutdown.
This loses recent operations after process failure and makes the display limit
a storage limit. Chris requested a Rust daemon with unchanged public interfaces
and authorized correcting defects during the rewrite.

## Decision

One owned worker commits journal events to SQLite before publishing them.
Native controls record requests before sending commands, then distinguish
acknowledgements, rejected requests, fresh readback and unavailable outcomes.
A storage acknowledgement never proves persistence.

Keep the existing event schema, REST routes, MCP resources and historical
records. Add transactional indexes for device, kind, severity and observation
time. Retain all records until explicitly cleared; apply the existing limit
to presentation and expose a sequence cursor for older records. Clearing does
not reuse sequence identities.

Acquire device observations from a coherent snapshot and ordered subscription.
If the source queue overflows, retain an explicit observation gap and acquire
a new snapshot. Graceful stop and uncertain crash intervals have distinct
gap reasons. Device disappearance remains an informational observation.

Persist issue state and its journal transition in the same transaction.
Reconstruct the initial issue index from the existing Python journal without
rewriting its records. Retain resolved episodes independently of the default
history window, with a cursor for older episodes. Restored open issues start
as unobservable until fresh source evidence establishes their current state.

Evaluate detectors when their source fields change. Unrelated updates do not
increase occurrence counts. Repeated clock observations update retained
evidence without emitting identical journal transitions. Resolve issues only
from positive evidence for the affected fields; missing data and offline
dependencies establish uncertainty. Compare pull-up values within observed
clock groups.

Retain the latest device snapshot separately from live inventory. Complete
inventory updates and meaningful observed changes save that snapshot; frequent
telemetry updates replace the in-memory value and flush on graceful shutdown.
After a crash, the persisted observation time bounds the retained evidence.
Read views mark retained devices offline, preserve their source times, and
prefer the latest known name when a hardware address has been renamed.

## Consequences

Committed history survives process failure, and queries decode only their
selected events. Indexing existing saved history adds bounded-by-history
startup work once. Durable writes add disk activity and must be measured.

The journal database has one native owner. During development the candidate
uses a separate database; cutover must stop the Python writer before opening
the retained journal. No Python process remains behind the Rust API.
Rollback must restore the pre-cutover database backup because the Python
writer replaces the database and does not preserve native indexes.

Saving only at shutdown was rejected because it loses accepted operation
records on crashes. Unbounded in-memory history was rejected because storage
retention should not dictate the daemon's memory use. Runtime qualification
and user acceptance remain separate from this implementation decision.

The issue index adds writes while an issue's evidence changes and requires its
own migration validation. Existing HTTP, MCP and Python CLI issue interfaces
remain available. Host and wireless conditions still depend on their native
service ports before the complete daemon can replace Python.
