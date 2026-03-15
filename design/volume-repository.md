# Volume Repository Design

## Status

Proposed.

This document describes a new `VolumeRepository` layer to sit between:

- the block engine in `Export`
- the low-level object storage interface in `ObjectStore`

The goal is to move remote metadata and object-layout policy out of `Export` without changing the fundamental read/write behavior of the server.

## Summary

Today, `Export` is responsible for two different concerns:

1. block-device behavior
   - read
   - write
   - flush
   - chunk materialization
   - dirty/resident tracking

2. remote metadata and object-layout policy
   - `volume.json` load/store
   - CAS update of volume head
   - snapshot object-key construction
   - manifest path construction
   - current head resolution
   - clone source manifest resolution

That is too much responsibility for one type.

The proposed change is:

```text
Export
  -> VolumeRepository
      -> ObjectStore
```

`Export` should become a pure block engine plus snapshot orchestration.

`VolumeRepository` should own all remote persistence policy for:

- `volume.json`
- manifests
- snapshot object naming
- head publish
- clone source resolution

`ObjectStore` should remain a raw backend abstraction over S3/R2/OSS-like storage.

## Problem

The current `Export` implementation directly constructs remote object keys and performs head updates itself.

Examples of logic that currently lives in `Export`:

- build `manifest.json` key from export prefix and snapshot id
- build `delta.blob` and `base.blob` object keys
- read `volume.json`
- perform CAS update of `volume.json`
- update `refs/current.json`
- resolve snapshot id to manifest object path

This creates three problems:

### 1. Wrong abstraction boundary

`Export` is supposed to be the block engine. It should not know the storage naming scheme.

### 2. Storage policy leaks into the data path

Changes to:

- object naming
- snapshot publish policy
- volume metadata shape
- clone source layout

all force changes to the block engine.

### 3. Harder future evolution

Features like:

- snapshot delete
- list snapshots
- retention policy
- metadata version migration
- alternate object layouts

should not require touching read/write chunk logic.

## Goals

- Move volume- and snapshot-level remote persistence logic out of `Export`.
- Keep `ObjectStore` as a minimal low-level abstraction.
- Make remote metadata flows easier to test independently.
- Reduce the number of places that know remote path conventions.
- Keep current runtime behavior unchanged while improving layering.

## Non-Goals

- Redesigning the manifest format.
- Changing read/write block semantics.
- Introducing a database or external metadata service.
- Supporting transactional multi-object commits in object storage.
- Changing the local cache layout.

## Current Layering

Current structure:

```text
Export
  |- local cache operations
  |- manifest interpretation
  |- snapshot packing
  |- object key construction
  |- volume.json read/CAS update
  |- refs/current.json update
  `- direct ObjectStore calls
```

This means `Export` depends on:

- local persistence details
- remote metadata details
- object-store semantics

That coupling is broader than necessary.

## Proposed Layering

### Layer 1: `ObjectStore`

Purpose:

- raw object IO
- no domain concepts

Allowed responsibilities:

- `get_range`
- `get_object`
- `get_object_with_etag`
- `put_bytes`
- `put_bytes_if_match`
- `put_bytes_if_absent`
- `put_file`
- `delete_object`
- `list_prefix`

Not allowed:

- knowledge of `volume.json`
- knowledge of manifests
- knowledge of export ids
- knowledge of snapshot naming rules

### Layer 2: `VolumeRepository`

Purpose:

- remote volume metadata and snapshot persistence policy

Allowed responsibilities:

- derive object keys from export id and snapshot id
- load and validate `volume.json`
- create `volume.json`
- resolve current snapshot manifest
- resolve explicit snapshot manifest
- resolve clone seed manifest
- stage and publish manifest objects
- CAS-update the current volume head
- optionally maintain `refs/current.json` compatibility

Not allowed:

- block read/write logic
- chunk dirty tracking
- chunk residency policy
- local cache mutation

### Layer 3: `Export`

Purpose:

- block device engine

Allowed responsibilities:

- map read/write requests to chunks
- materialize missing chunks
- track dirty/resident state
- flush local state
- ask repository to publish a prepared snapshot

Not allowed:

- constructing object keys directly
- parsing and writing `volume.json` directly
- performing raw CAS writes itself

## Repository Responsibilities

The repository should own these workflows.

### Volume lifecycle

- `create_empty_volume(export_id, image_size, chunk_size)`
- `load_volume(export_id)`
- `load_all_volumes(export_root)`
- `update_volume_head(export_id, expected_etag, next_volume)`

### Manifest resolution

- `load_current_manifest(export_id)`
- `load_manifest_by_snapshot_id(export_id, snapshot_id)`
- `resolve_clone_source(source_export_id, snapshot_id_or_current)`

### Snapshot persistence

- build object paths for a given snapshot id
- write `manifest.json`
- optionally write/update `refs/current.json`
- CAS-publish `volume.json` to point to the new snapshot id

### Optional later responsibilities

- list snapshots
- delete snapshots
- retention and garbage collection

## Proposed Interface Shape

This is intentionally illustrative, not final Rust code.

```rust
pub struct SnapshotLayout {
    pub manifest_key: String,
    pub delta_key: String,
    pub base_key: String,
}

pub struct LoadedVolume {
    pub metadata: VolumeMetadata,
    pub etag: Option<String>,
}

pub struct ResolvedManifest {
    pub snapshot_id: Option<String>,
    pub manifest_key: String,
    pub manifest: Manifest,
}

pub struct PublishRequest {
    pub export_id: String,
    pub snapshot_id: String,
    pub manifest: Manifest,
}

#[async_trait]
pub trait VolumeRepository: Send + Sync {
    async fn create_volume(&self, volume: &VolumeMetadata) -> Result<LoadedVolume>;
    async fn load_volume(&self, export_id: &str) -> Result<LoadedVolume>;
    async fn list_volumes(&self) -> Result<Vec<LoadedVolume>>;

    async fn load_current_manifest(&self, export_id: &str) -> Result<ResolvedManifest>;
    async fn load_manifest_by_snapshot_id(
        &self,
        export_id: &str,
        snapshot_id: &str,
    ) -> Result<ResolvedManifest>;
    async fn resolve_clone_source(
        &self,
        export_id: &str,
        snapshot_id: Option<&str>,
    ) -> Result<ResolvedManifest>;

    fn snapshot_layout(&self, export_id: &str, snapshot_id: &str) -> SnapshotLayout;

    async fn put_manifest(&self, manifest_key: &str, manifest: &Manifest) -> Result<()>;
    async fn publish_volume_head(
        &self,
        export_id: &str,
        expected_etag: &str,
        snapshot_id: &str,
    ) -> Result<bool>;
}
```

The actual final API may merge some of these calls, but the important thing is the responsibility split.

## What `Export` Would Keep

`Export` should still do all chunk-oriented logic:

- `read()`
- `write()`
- `flush()`
- `materialize_chunk()`
- `fetch_chunk_bytes()`
- `publish_initial_sparse_snapshot()`
- `publish_delta_snapshot()`
- clone sparse snapshot packing
- compact packing

But the last step of publish should move from:

- raw object-store calls
- inline key construction

to:

- repository calls

### Example before

Current shape:

```text
Export
  -> build manifest key
  -> put manifest object
  -> get volume.json + etag
  -> update volume.json with CAS
  -> update refs/current.json
```

### Example after

Target shape:

```text
Export
  -> prepare manifest + snapshot id
  -> repository.publish_snapshot(...)
```

That is the main simplification.

## Object Layout Policy

Today the path layout is implicitly spread through helper functions and inline string formatting.

Examples:

- `<export-root>/<export-id>/volume.json`
- `<export-root>/<export-id>/snapshots/<snapshot-id>/manifest.json`
- `<export-root>/<export-id>/snapshots/<snapshot-id>/delta.blob`
- `<export-root>/<export-id>/snapshots/<snapshot-id>/base.blob`

This policy should move fully into the repository implementation.

That gives one place to change later if:

- `refs/current.json` is removed
- manifest path conventions change
- compatibility shims are retired

## Interaction With Read Path

The read path still needs a manifest in memory.

That does not mean the repository belongs in the read hot path for every request.

The intended model is:

1. repository resolves and loads the active manifest when an export is opened
2. `Export` stores that manifest as its current `read_source`
3. normal read misses use the in-memory manifest directly
4. repository is only involved again when:
   - a new export is opened
   - a snapshot is published
   - a clone source is resolved

So the repository is control-plane, not per-request data-plane.

## Interaction With Manager

The manager should talk to the repository for:

- volume discovery at server startup
- create/open/clone metadata operations
- volume existence checks

The manager should not need to know how `volume.json` or manifest paths are encoded either.

Target relationship:

```text
ExportManager
  -> VolumeRepository
  -> Export
```

instead of:

```text
ExportManager
  -> ObjectStore
  -> path helpers
  -> Export
```

## Testing Benefits

This split allows cleaner tests.

### `ObjectStore` tests

- backend-specific semantics
- CAS behavior
- list/get/put/delete behavior

### `VolumeRepository` tests

- path derivation
- volume metadata round trip
- manifest resolution by current head
- clone source resolution
- CAS publish behavior

### `Export` tests

- chunk materialization
- write semantics
- sparse snapshot packing
- clone sparse snapshot packing

That is a better separation than the current state, where `Export` tests also have to exercise path construction and remote metadata behavior directly.

## Migration Plan

This refactor should be done incrementally.

### Step 1

Introduce a concrete repository implementation over the current object store:

- `ObjectStoreVolumeRepository`

Do not change behavior yet.

### Step 2

Move helper functions and direct metadata load/store from `Export` into the repository:

- manifest resolution helpers
- volume head publish
- key construction

### Step 3

Update `Export` to depend on `Arc<dyn VolumeRepository>` plus any still-needed read-only object access path.

One practical option:

- keep `ObjectStore` available for chunk range GETs
- use `VolumeRepository` for metadata operations

### Step 4

Update `ExportManager` to use the repository for startup discovery and create/open/clone flows.

### Step 5

Delete remaining duplicate path helpers and direct metadata object manipulation from engine/manager code.

## Practical Note About Chunk Reads

The repository should not necessarily replace `ObjectStore` for chunk body reads.

Chunk fetches are simple:

- resolve chunk location from in-memory manifest
- range GET object bytes

That path is already efficient and does not need repository indirection.

So the practical dependency shape may be:

```text
Export
  -> ObjectStore         # data objects
  -> VolumeRepository    # metadata and publish
```

That is still a substantial improvement.

## Recommended First Scope

For the first refactor, `VolumeRepository` should own only:

- `volume.json`
- manifest loading by snapshot id or current head
- snapshot object key derivation
- head publish

Do not move chunk data reads into it.

That keeps the repository clearly about metadata and remote layout policy, which is the actual problem we want to solve.
