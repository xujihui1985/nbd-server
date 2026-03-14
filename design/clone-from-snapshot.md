# Clone From Snapshot Design

## Status

Proposed.

This document describes a new clone workflow for creating a new export lineage from an existing export snapshot without introducing long-term remote object sharing between the two exports.

## Design Principles

- A snapshot is immutable once published.
- Clone always starts from a published remote snapshot, never from local dirty cache state.
- A cloned export is a new lineage whose own generation starts at `0`.
- The source export and the cloned export must not share mutable remote state.

## Summary

We want to support a workflow like:

- source export: `vm01`
- source snapshot: `2`
- target export: `vm02`

The target export should behave like a new volume:

- reads initially see the contents of `vm01@2`
- local writes are tracked only in `vm02`'s cache
- `vm02` starts at generation `0`
- the first snapshot taken on `vm02` becomes `vm02` generation `1`
- that first `vm02` snapshot uploads a full base blob for `vm02`
- after that point, `vm02` is fully independent from `vm01`

This is intentionally simpler than a Git-like shared-object clone. It avoids cross-export object sharing, so existing per-export garbage collection remains valid.

## Problem

The current server can:

- create a new empty export
- open an existing export at `current` or a specific snapshot generation
- lazily materialize data into a local cache
- publish snapshots and compactions back to object storage

It cannot create a new export lineage from an existing snapshot.

The more advanced design would let cloned exports share immutable remote objects until they diverge. That is attractive for storage efficiency, but it creates a real ownership problem:

- `vm02` would reference objects under `vm01`
- later snapshots or compactions on `vm01` could garbage-collect those objects
- `vm02` would break unless we add global reachability tracking or cross-export reference counting

That complexity is out of scope for the next step.

## Goals

- Support creating a new export lineage from an existing export snapshot.
- Restrict clone sources to immutable remote snapshots only.
- Keep the clone lazy on the read path before the first target snapshot.
- Make the first target snapshot generation `1`.
- Make the cloned export start at generation `0`.
- Avoid long-term remote object sharing between source and target exports.
- Preserve the existing mental model that one export lineage owns its own remote objects.
- Keep current snapshot and compaction behavior unchanged for ordinary exports.

## Non-Goals

- Cloning from unsnapshotted local dirty state.
- Cross-export remote object deduplication.
- Shared immutable object graphs between exports.
- Global garbage collection across multiple exports.
- Branch merge or rebase semantics.
- Multiple exports in one server process.

## Chosen Approach

### High-level model

Add an explicit clone operation that creates a new local export backed by:

- a target export identity and target storage prefix
- a local cache directory for the target export
- a seed read source taken from a source export snapshot

The seed read source is used only for:

- cache misses before the first target snapshot
- partial write materialization before the first target snapshot

The target export's own lineage generation starts at `0`, regardless of the source snapshot generation.

### Key rule

The first snapshot on the target export always uploads a full base blob for the target export.

That means:

- `vm02` generation `1` is self-contained
- `vm02` generation `1` does not reference `vm01` objects
- after `vm02` generation `1`, normal incremental snapshot behavior resumes

This avoids the need for cross-export reference tracking.

## Example Lifecycle

### Clone setup

Assume:

- source export: `vm01`
- source snapshot: `2`
- target export: `vm02`

Clone initialization:

1. Load the published remote manifest for `vm01@2`.
2. Create a fresh local cache for `vm02`.
3. Record that `vm02` has:
   - target lineage generation `0`
   - seed read source `vm01@2`
4. Do not create any remote objects for `vm02` yet.

If a running `vm01` process has local dirty data layered on top of `vm01@2`, that local dirty state is ignored by clone. `clone vm01@2 -> vm02` always means "clone from the published remote snapshot 2", not "clone the current in-memory or local-cache state of a running vm01 server".

### Reads before first `vm02` snapshot

On a cache miss:

1. resolve the chunk from the seed manifest `vm01@2`
2. fetch from object storage
3. write into `vm02` local cache
4. mark resident locally

### Writes before first `vm02` snapshot

On a write:

1. if the chunk is fully overwritten, write locally and mark dirty
2. if the chunk is partially overwritten and not resident, materialize it from the seed manifest first
3. write locally and mark dirty

This is the same local behavior as ordinary exports. The only difference is the remote read source.

### First `vm02` snapshot

When `vm02` takes its first snapshot:

1. block writes
2. materialize all missing chunks from the seed manifest into local cache
3. sync local data
4. upload a full base blob to `exports/vm02/snapshots/1/base.blob`
5. publish `exports/vm02/snapshots/1/manifest.json`
6. publish `exports/vm02/refs/current.json`
7. clear local dirty bits
8. drop the seed read source from runtime state

After step 8, `vm02` is a normal independent export.

## Remote Layout

The target export continues to use the existing remote layout:

- `exports/vm02/refs/current.json`
- `exports/vm02/snapshots/1/base.blob`
- `exports/vm02/snapshots/1/manifest.json`
- later:
  - `exports/vm02/snapshots/N/delta.blob`
  - `exports/vm02/snapshots/N/manifest.json`

No remote object under `exports/vm02/...` should reference `exports/vm01/...`.

## Runtime State Changes

The current runtime state assumes one remote head whose generation also determines the next snapshot generation.

Clone support needs to split these concepts:

- `read_source`
  - where cache misses are fetched from
  - either zero, a manifest from the target export, or a seed manifest from another export

- `lineage_generation`
  - the target export's own snapshot generation counter
  - for clones, starts at `0`

Today, these two concepts are conflated in `RemoteHead`.

For clone support, they should become separate fields in the export runtime state.

## Local Metadata

The cache metadata should continue to belong to the target export only.

For a clone, local metadata should store at least:

- target export id
- image size
- chunk size
- target lineage generation
- normal dirty and resident bitmaps

The seed source does not need to be persisted in `cache.meta` if clone is represented as a dedicated startup mode that reconstructs the seed state from explicit CLI or API input.

If we later want clone state to survive restart before the first snapshot without external arguments, we will need to persist seed source metadata separately.

That persistence is not required for the first implementation.

## Snapshot Semantics

### Source export

No behavior change.

### Target export before first snapshot

- local writes are allowed
- `FLUSH` still means local durability only
- remote lineage generation remains `0`

### Target export first snapshot

- always full base upload
- always generation `1`
- allowed even when there were no writes

The last point matters. A clone may be created and then snapshotted without any local modifications. We still need a self-contained `vm02` generation `1`.

## Clone Source Validation

Clone input must resolve to one immutable remote source:

- `source_export_id`
- either:
  - `source_snapshot_id`, or
  - remote `current`

Both resolve to a published remote manifest.

The clone operation must reject:

- any request to clone local unsnapshotted dirty state
- any request that depends on a running process's in-memory cache as the source of truth

If `vm01` is currently opened and has local dirty writes, that does not block cloning `vm01@<published-snapshot>`. The clone still resolves only against the published remote manifest for that snapshot.

This keeps clone semantics stable and reproducible.

### Target export after first snapshot

Normal incremental behavior resumes:

- dirty-only delta upload
- sparse manifest rewrite
- normal compaction behavior

## Garbage Collection

This approach deliberately preserves the current ownership model:

- `vm01` owns `exports/vm01/...`
- `vm02` owns `exports/vm02/...`

Because the first `vm02` snapshot creates a full independent base blob, the current per-export garbage collection model can remain in place.

No global GC redesign is required for this step.

## API and CLI Shape

The most natural surface is a new explicit clone command.

Example:

```bash
nbd-server clone \
  --source-export-id vm01 \
  --source-snapshot-id 2 \
  --target-export-id vm02 \
  --cache-dir /var/lib/nbd-server/vm02 \
  --bucket my-bucket \
  --prefix exports/vm02 \
  --listen 127.0.0.1:10810 \
  --admin-sock /tmp/nbd-server-vm02.sock
```

Alternative:

- a local admin API that prepares the clone metadata and then starts a new server instance

For the first implementation, the CLI command is simpler and easier to reason about.

## Failure and Recovery

### Before first target snapshot

All authoritative state is local:

- local dirty cache is authoritative
- the seed source is only a read source

If the process crashes:

- restart can continue from local cache if clone startup parameters are provided again
- or the local cache can be reset and the clone recreated

### During first target snapshot

Behavior should follow the existing full snapshot journal model:

- stage snapshot intent locally
- upload full base blob
- upload manifest
- publish current ref
- clear dirty bits

If the first target snapshot fails midway:

- local dirty state remains authoritative
- target generation stays `0`
- retry is safe

## Trade-offs

### Advantages

- Simple ownership model.
- No cross-export sharing.
- Current GC model remains usable.
- No manifest schema changes required.
- The clone behaves like a real new volume after generation `1`.

### Disadvantages

- The first target snapshot is expensive because it must upload a full base blob.
- Clone storage efficiency is worse than a shared-object approach.
- Pre-first-snapshot clone restart behavior may need explicit startup parameters.

This is acceptable for the next step because it minimizes correctness risk.

## Implementation Plan

1. Introduce a clone startup path.
2. Split runtime state into:
   - target lineage generation
   - remote read source
3. Represent the seed read source as a manifest loaded from another export snapshot.
4. Reuse existing lazy read and write materialization logic against that seed source.
5. Change first snapshot behavior for cloned exports:
   - materialize all chunks
   - upload full base blob
   - publish generation `1`
6. After generation `1`, switch the export into ordinary mode.
7. Add tests for:
   - clone read from source snapshot
   - partial write on clone before first snapshot
   - first clone snapshot creates generation `1`
   - first clone snapshot creates only target-owned remote objects
   - source export GC does not affect the cloned export

## Open Questions

### Clone state persistence before first snapshot

Do we want a clone to survive restart without having to provide:

- source export id
- source snapshot id
- target export id

If yes, we need a small persisted clone-state record in the target cache directory.

### Read-only clone mode

For debugging and inspection, a read-only clone mode may be useful. It is not required for the first implementation.

### Naming

We may eventually want to separate:

- remote export identity
- local NBD export name

That is orthogonal to the clone design described here.
