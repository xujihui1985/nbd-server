# Multi-Export API Design

## Status

Proposed.

This document describes how to evolve `nbd-server` from a single-export process into a long-running server that starts empty, manages multiple exports through an HTTP API, and stores authoritative volume metadata in object storage rather than in local server state.

## Summary

Today, one process owns exactly one `Export`:

- startup decides whether that export is `create`, `open`, or `clone`
- the admin API acts directly on that one export
- the NBD listener negotiates exactly one export name

We want to invert that model:

- the server starts by discovering exports from object storage
- exports are created, opened, or cloned through the admin API
- multiple exports can coexist in one process
- NBD negotiation resolves the requested export name from an in-memory registry

The important design change is that the process-local registry is no longer authoritative. The source of truth for a volume lives in object storage:

- one mutable `volume.json` per export
- immutable snapshot objects under that export prefix

That lets a restarted server reconstruct exports from remote metadata, and it avoids introducing a separate database or lock service.

## Design Principles

- Object storage is the shared control plane.
- Snapshot objects are immutable once written.
- A volume has exactly one mutable metadata object: `volume.json`.
- Snapshot identity is a globally unique lowercase id, not a sequential integer.
- Publishing a new snapshot is a CAS update of `volume.json`.
- Process-local registry state is rebuildable cache, not authoritative state.

## Goals

- Start the server by discovering active exports from object storage metadata.
- Load multiple exports into one process through API calls.
- Support the same three export creation modes:
  - create new empty export
  - open existing export
  - clone from published remote snapshot
- Allow NBD clients to connect to different export names through one listener.
- Preserve the existing per-export behavior for read, write, flush, snapshot, compact, and cache reset.
- Let servers recover the desired export state after restart from remote metadata.
- Avoid introducing a separate metadata service or database in the first multi-export step.

## Non-Goals

- Linear sequential snapshot numbering.
- Cross-export remote object sharing.
- Supporting multiple object-storage backends inside one server process.
- Sharing one local cache directory across different exports.
- Automatic restart of failed export loads.
- Cross-export scheduling, QoS, or bandwidth isolation.

## Current Constraints

The current code has three single-export assumptions:

1. `main` creates one `Export` before the listeners start.
2. `admin` holds one `Arc<Export>`.
3. `nbd` negotiates against one fixed export name and then serves requests through that one `Arc<Export>`.

Those assumptions appear in:

- [src/main.rs](/Users/xujihui/code/github.com/xujihui1985/nbd-server/src/main.rs)
- [src/admin.rs](/Users/xujihui/code/github.com/xujihui1985/nbd-server/src/admin.rs)
- [src/nbd.rs](/Users/xujihui/code/github.com/xujihui1985/nbd-server/src/nbd.rs)

## Chosen Approach

Introduce a process-wide export manager, but keep the authoritative volume state in object storage.

### High-level model

The server process becomes:

- one admin HTTP server
- one NBD TCP listener
- one shared object-storage backend
- one in-memory registry of active exports discovered from object storage

Each active export remains an `Arc<Export>` with the same internal behavior as today.

The authoritative metadata for each export becomes:

- `exports/<export_id>/volume.json`

That file defines:

- volume identity
- current published snapshot id
- current manifest key
- optional current base manifest/source information

### Registry responsibilities

The in-memory registry is responsible for:

- add export
- remove export
- list exports
- fetch export by name
- reject duplicate export names inside one process

NBD negotiation uses the registry only to find an export by name. Once negotiation succeeds, the client session holds its own `Arc<Export>` and proceeds exactly like today.

That means registry lookup happens only during negotiation, not on every request.

## Server Startup Model

Add a new startup mode:

- `serve`

`serve` starts:

- the admin Unix socket
- the NBD TCP listener
- an export registry populated by scanning object storage

The storage backend remains process-global and is configured at startup just like today:

- backend kind
- bucket
- region
- endpoint
- credentials
- a global root prefix for all exports

On startup, the server scans the global export root and loads all discovered `volume.json` objects into the registry.

### CLI surface

The runtime CLI should expose only `serve`.

Per-volume lifecycle operations such as create, open, clone, snapshot, compact, and cache reset are handled through the admin API rather than through separate process modes.

## Remote Volume Metadata

Each export gets one authoritative mutable metadata object:

- `exports/<export_id>/volume.json`

Recommended shape:

```json
{
  "version": 1,
  "export_id": "vm01",
  "image_size": 1073741824,
  "chunk_size": 4194304,
  "current_snapshot_id": "0195938f4f7a7c8ab1d2e3f4a5b6c7d8"
}
```

There is no `next_snapshot_id`.
There is no `current_manifest_key`.

Snapshot ids are unique lowercase ids, for example lowercase UUIDv7 without hyphens.

Example:

- `0195938f4f7a7c8ab1d2e3f4a5b6c7d8`

## Snapshot Layout

Each committed snapshot lives under its own immutable id:

- `exports/<export_id>/snapshots/<snapshot_id>/manifest.json`
- `exports/<export_id>/snapshots/<snapshot_id>/delta.blob`
- or:
  - `exports/<export_id>/snapshots/<snapshot_id>/base.blob`

There is no global sequential numbering requirement.

This removes allocation races entirely:

- each writer can generate a unique snapshot id locally
- writers never collide on immutable object keys
- only the `volume.json` CAS publish step determines which snapshot becomes current

## Why Unique Snapshot Ids Replace Sequential Ids

If snapshot identity were sequential, concurrent writers would still need a safe allocator for "next snapshot id".

Using a unique lowercase snapshot id instead means:

- no allocation race
- no separate reservation protocol
- object keys are naturally collision-free
- publish ordering is handled only through CAS on `volume.json`

This is simpler and better aligned with immutable object storage.

## Publish Protocol

Publishing a snapshot must keep the invariant:

- `volume.json` must never point to incomplete snapshot data

So the order is:

1. read `volume.json` and record its version/etag
2. generate a new unique lowercase `snapshot_id`
3. upload immutable snapshot objects under `snapshots/<snapshot_id>/...`
4. CAS-update `volume.json` so it points to the new snapshot

If step 4 fails:

- the new snapshot objects remain unreferenced
- they are safe but not current
- later GC can remove them

This is preferable to CAS-updating `volume.json` before upload, because that would allow head to point at missing or partial data after crash.

## CAS Requirements

The design assumes the storage backend can provide compare-and-swap semantics for `volume.json`, for example by conditional overwrite using object version/etag matching.

The exact backend support must be validated for:

- S3
- R2

If a backend cannot safely support CAS on `volume.json`, then multi-writer publish on that backend is not safe and the system must fall back to one-writer-per-export operational policy.

## Server Restart

Process-local registry state is not authoritative.

After restart:

- the server comes up with no exports loaded in memory
- callers can re-open exports through the API by reading remote `volume.json`
- nothing is lost at the volume metadata level, because `volume.json` and snapshot objects live in object storage

This is the key reason to put volume metadata remotely instead of in a local `server.state.json`.

## Export Identity

For the first implementation, use one name for both:

- registry key
- NBD export name

That name remains `export_id`.

This keeps the first step small. If we later want separate runtime aliases, we can add:

- `export_name`
- `remote_export_id`

That separation is not required for the first multi-export step.

## Admin API

### Top-level endpoints

Recommended endpoints:

- `GET /v1/exports`
- `POST /v1/exports/create`
- `POST /v1/exports/open`
- `POST /v1/exports/clone`
- `DELETE /v1/exports/{export_id}`

Per-export operations:

- `GET /v1/exports/{export_id}/status`
- `POST /v1/exports/{export_id}/snapshot`
- `POST /v1/exports/{export_id}/compact`
- `POST /v1/exports/{export_id}/cache/reset`

### Request model

Request bodies should carry logical identifiers only. Local paths and object-storage prefixes are derived from global server configuration plus `export_id`.

Derived values:

- cache dir: `<global_cache_root>/<export_id>`
- export prefix: `<global_export_root>/<export_id>`
- source prefix for clone: `<global_export_root>/<source_export_id>`

Example create request:

```json
{
  "export_id": "vm01",
  "size": 1073741824
}
```

Example open request:

```json
{
  "export_id": "vm01"
}
```

Open resolves from `volume.json` current head.

Example clone request:

```json
{
  "export_id": "vm02",
  "source_export_id": "vm01",
  "source_snapshot_id": "0195938f4f7a7c8ab1d2e3f4a5b6c7d8"
}
```

The process-global storage configuration is not repeated in every request, and neither are derived cache or prefix paths.

## NBD Changes

### Negotiation

The NBD listener should no longer receive one `Arc<Export>`.

Instead, it receives the registry and:

1. reads the requested export name from `NBD_OPT_EXPORT_NAME`, `NBD_OPT_INFO`, or `NBD_OPT_GO`
2. looks it up in the registry
3. if present:
   - sends export info for that export
   - clones the `Arc<Export>`
   - enters transmission phase
4. if absent:
   - reply with unknown export

### Transmission phase

No meaningful change.

Once a session has an `Arc<Export>`, the existing read/write/flush logic is still correct.

## Export Removal Semantics

Removing an export from the registry should mean:

- no new NBD sessions can negotiate that export
- no new admin operations can target that export

Existing NBD sessions may still continue if they already hold `Arc<Export>`.

That is acceptable for the first implementation and avoids having to forcibly tear down clients.

A later enhancement can add:

- session tracking
- refusal to remove exports with active sessions
- or forced disconnect support

Removing an export from the registry does not delete remote snapshot data automatically.

## Failure Model

### Adding an export

An export-add request either:

- succeeds fully and inserts the `Arc<Export>` into the registry
- or fails before insertion and leaves the registry unchanged

There should be no partial registry state inside the process.

### Runtime export errors

Per-export operations such as snapshot or compact can fail independently.

Those failures should:

- be returned only to the caller of that export operation
- not evict the export from the registry automatically
- not affect other exports

### Concurrent snapshots on different servers

Two servers may both prepare new snapshots for the same export concurrently.

That is safe as long as:

- each snapshot id is unique
- immutable objects are uploaded under that unique id
- `volume.json` is updated with CAS

Only one CAS update can advance current head from a given prior `volume.json` state.

The losing writer leaves an unreferenced snapshot object set that can be garbage-collected later.

## Concurrency Model

### In-process registry

Use a registry guarded by an async lock, for example:

- `RwLock<BTreeMap<String, Arc<Export>>>`

Expected access pattern:

- many reads during NBD negotiation and admin status queries
- infrequent writes for add/remove

### Per-export operations

No semantic change inside one process.

Each export still owns:

- its own write gate
- its own chunk locks
- its own cache files
- its own snapshot journal

That isolation is one of the main reasons this refactor is tractable.

### Cross-process safety

Cross-process safety does not come from the in-memory registry.

It comes from:

- immutable snapshot object keys
- CAS update of `volume.json`

That is the core distributed concurrency mechanism in this design.

## Persistence

There is no process-global local state file in the chosen design.

Persistent server-relevant state lives in object storage:

- one `volume.json` per export
- immutable snapshot objects per export

Local files remain per-export cache and recovery data only:

- `cache.raw`
- `cache.meta`
- `snapshot.journal.json`
- `clone.seed.json` where applicable

## Recommended Internal Structure

Add a new module, for example:

- `src/manager.rs`

Responsibilities:

- hold the in-memory export registry
- construct exports from API requests
- expose lookup/add/remove helpers
- scan remote `volume.json` objects at startup
- read remote `volume.json` when creating/opening/cloning exports

Suggested API shape:

- `ExportManager::new(storage_backend: Arc<dyn StorageBackend>)`
- `ExportManager::create_export(...)`
- `ExportManager::open_export(...)`
- `ExportManager::clone_export(...)`
- `ExportManager::get(name)`
- `ExportManager::remove(name)`
- `ExportManager::list()`

`admin` and `nbd` should both depend on the manager instead of on a single export.

## Incremental Implementation Plan

### Step 1

Introduce:

- `serve` startup mode
- `ExportManager`
- startup discovery of `volume.json` under the global export root
- `GET /v1/exports`
- `POST /v1/exports/create`
- `POST /v1/exports/open`
- `POST /v1/exports/clone`

And refactor NBD negotiation to look up exports by name from the registry.

### Step 2

Add remote `volume.json` support:

- create writes the initial volume metadata
- open resolves current head from `volume.json`
- snapshot publish CAS-updates `volume.json`
- manifest key is inferred from `current_snapshot_id`

### Step 3

Add per-export admin routes:

- status
- snapshot
- compact
- cache reset

### Step 4

Add export removal:

- `DELETE /v1/exports/{export_id}`

Initially, removal only unregisters the export for new sessions.

## Test Plan

### Manager tests

- add create/open/clone exports into the registry
- reject duplicate export ids inside one process
- remove export from registry
- list returns current registry state

### Metadata tests

- `volume.json` round-trip encode/decode
- CAS publish success updates current head
- CAS publish failure leaves old head intact
- open without explicit snapshot id resolves through `volume.json`
- manifest path is inferred from `current_snapshot_id`

### NBD tests

- unknown export name is rejected
- two different export names can be negotiated on one server
- each session gets the correct export geometry and description

### Admin tests

- create/open/clone endpoints insert exports
- per-export status routes reach the right export
- snapshot on one export does not affect another export

### Integration tests

- start server and discover existing exports from the bucket
- add `vm01`
- add `vm02`
- connect two NBD clients by different export names
- verify reads go to the intended exports
- simulate two concurrent snapshot publishes and verify only one CAS publish wins

## Risks

### Backend CAS support

The design depends on reliable CAS semantics for `volume.json`.

This must be validated explicitly for each supported backend.

### Cache directory collisions

The API makes it easier to accidentally load two exports that point to the same `cache_dir`.

Because `cache_dir` is derived from `export_id`, this should only happen if the local filesystem already contains conflicting data. That should be rejected if the cache metadata does not match the target export identity.

### Manager becoming too smart

The manager should remain a thin lifecycle layer. Export-specific correctness must stay inside `Export`.

That boundary is important for maintainability.

### Unreferenced snapshot garbage

Failed CAS publishes can leave behind unreferenced immutable snapshot objects.

That requires later garbage collection. It is acceptable for the first implementation.

## Open Questions

### Exact CAS primitive

What exact conditional write API should be used for:

- S3
- R2

This needs backend validation and possibly backend-specific adaptation.

### Removal policy with active sessions

Do we want:

- soft unregister only
- reject removal while sessions are active
- or forced disconnect

The first step should choose soft unregister only.

### Metrics

Once multiple exports exist in one process, per-export metrics become more important. That is deferred from the first step.
