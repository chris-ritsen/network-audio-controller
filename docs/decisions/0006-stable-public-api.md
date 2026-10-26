# Stable public web-server API after the Rust rewrite

Date: 2026-10-21

Status: Accepted requirement; API 1 implemented for the native release.
Implementation acceptance by Chris remains separate from this contract.

## Context

Chris requested that external developers be able to build against the web
server without unexpected interface changes. He specified that the completed
Rust rewrite establishes the first supported versioned API, and that ordinary
minor changes must not cause repeated API version bumps.

The experimental server currently publishes `/api` with `version: 2` and a
feature list. Event snapshots carry the same experimental revision. These
markers do not establish a supported major-version compatibility contract.

## Decision

Publish the completed rewrite as the first stable public API contract. Define
and document its compatibility boundary before the cutover described in
[the Rust daemon decision](0003-rust-daemon.md). Preserve the Python CLI and
coordinate any client changes needed for the final versioning mechanism.

The contract includes public routes and methods, request and response schemas,
required fields, identifiers, units, nullability, errors and status codes,
authentication, command outcomes, and event ordering and recovery semantics.
Publish a machine-readable contract and consumer documentation with the
release. Distinguish the supported API major version from application builds,
protocol versions and the existing experimental feature revision.

Keep compatible additions and fixes within the same API major version. An
addition is compatible only if existing valid clients keep working; new
required inputs or changed meanings of existing fields are not minor changes.
Document extension rules so clients know how to handle optional fields,
capabilities and explicitly extensible values.

Incompatible changes require a deliberate new major contract, documented
migration and an explicit support or retirement decision for the earlier
contract. Do not silently repurpose an existing version or change its meaning.
Choose the exact version identifier and routing mechanism during final
contract review; this decision does not rename today's endpoints or declare
the incomplete candidate stable.

## Consequences

The earlier experimental no-stability policy no longer governs the public
contract after this release. Internal implementation can continue to change
without forcing consumers to migrate. API evolution requires compatibility
review and release documentation rather than an automatic version increment.

The rewrite remains incomplete until its supported contract is documented and
the ordinary consumers have been verified against it. After Chris accepts the
working behavior, retain contract and end-to-end validation to protect that
behavior. Source presence and an experimental revision marker are insufficient.

Chris additionally requested OpenAPI documentation on 2026-10-21. Publish the
HTTP contract as OpenAPI with a browsable reference while the rewrite proceeds.
Document implemented behavior and remaining gaps accurately; documentation
availability does not prematurely establish the stable-release boundary.

## Release mechanism, 2026-10-22

The existing routes are public API major 1. Omitted version selection continues
to select major 1; clients may pin it with `NetAudio-API-Version: 1`. Unsupported
values return 406 before dispatch, including for commands. Duplicate selection
headers return 400. Responses expose the selected major and vary caches by the
selection header. A future major cannot silently change these defaults.

`/api`, `/server-info`, and initial event snapshots publish `public_api` metadata.
The OpenAPI document's version is the contract revision, initially `1.0.0`.
Existing feature revision 2 remains separate, as do application and MCP versions.
The browsable reference and compatibility, security, command, history and event
guides are served from `/api/docs` and its canonical child paths.

The release preserves the existing query filters under the explicitly requested
consumer compatibility boundary. New interfaces use path resources and typed
bodies. A wholesale route rename was rejected because it would require the
consumer migration Chris expressly excluded from this daemon rewrite.

Event snapshots identify their producer lifetime with `source_generation`.
Every event carries a decimal-string producer revision. Filtered events leave
gaps; multiple projections can share a revision. Meter coalescing preserves the
latest value per identity and can reorder revisions across different identities.
These revisions are observations, not replay cursors. Disconnect and overflow
recovery always acquire a new coherent snapshot; durable history uses the journal.

The stable contract does not certify physical interoperability on untested
hardware or accept a new UI on Chris's behalf. Real appliance and platform
qualification remain explicit evidence boundaries.
