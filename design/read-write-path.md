# Read/Write Path Design

## Status

Current.

This document describes how the current server implements block reads and writes on top of:

- a local fixed-size cache file
- local cache metadata
- an object-storage-backed manifest
- chunk-level lazy materialization

It is an implementation design note for the current code, not a future proposal.

## Summary

The data path has four core ideas:

1. The exposed block device is backed by a local `cache.raw` file.
2. The local file is authoritative for all resident and dirty chunks.
3. Missing chunks are resolved through the active manifest and fetched lazily from object storage.
4. Chunk state is tracked separately in `cache.meta` through resident and dirty bitmaps.

The implementation lives primarily in:

- [src/core/engine/export.rs](/Users/xujihui/code/github.com/xujihui1985/nbd-server/src/core/engine/export.rs)
- [src/core/cache/local_cache.rs](/Users/xujihui/code/github.com/xujihui1985/nbd-server/src/core/cache/local_cache.rs)
- [src/core/model/manifest.rs](/Users/xujihui/code/github.com/xujihui1985/nbd-server/src/core/model/manifest.rs)
- [src/core/model/cache_meta.rs](/Users/xujihui/code/github.com/xujihui1985/nbd-server/src/core/model/cache_meta.rs)

## Goals

- Expose a raw block device over NBD.
- Avoid downloading the whole remote image before use.
- Make repeated reads hit local disk once chunks are materialized.
- Allow local writes before snapshot publish.
- Keep remote snapshots immutable.
- Keep object storage traffic chunk-oriented rather than request-oriented.

## Non-Goals

- Memory caching beyond what the operating system page cache already does.
- Cross-export cache sharing.
- In-place remote mutation.
- Full CoW filesystem semantics inside the server.

## Terms

- `chunk`: fixed-size block of the exported image, default `4 MiB`
- `resident`: chunk bytes are present in `cache.raw`
- `dirty`: resident chunk has local changes not yet published in a snapshot
- `read_source`: the currently active remote source for non-resident chunks
- `manifest`: sparse mapping from logical chunk index to remote object ranges

## Data Structures

### Local Disk Layout

Each export has its own local cache directory:

```text
<cache-root>/<export-id>/
├── cache.raw
├── cache.meta
├── clone.seed.json          # only before first clone snapshot
└── snapshot.journal.json    # only during snapshot/compact publish
```

`cache.raw` is a fixed-size raw file with length equal to the logical image size.

`cache.meta` stores:

- export id
- image size
- chunk size
- chunk count
- current manifest generation
- clean shutdown bit
- snapshot in progress bit
- dirty bitmap
- resident bitmap

### Remote Layout

Each export has one mutable volume head and immutable snapshots:

```text
<export-root>/<export-id>/
├── volume.json
├── refs/
│   └── current.json
└── snapshots/
    └── <snapshot-id>/
        ├── manifest.json
        ├── delta.blob
        └── base.blob
```

Only one of `delta.blob` or `base.blob` is expected for a given snapshot.

### Manifest Shape

The manifest is sparse. It does not enumerate every chunk unless needed.

```text
Manifest
├── image_size
├── chunk_size
├── chunk_count
├── base_ref?         -> optional full-image reference
├── refs[]            -> remote object references
└── entries[]         -> sparse per-chunk overrides
```

Resolution rule for logical chunk `i`:

```text
if entries contains i:
    read from entries[i].ref at entries[i].offset
else if base_ref exists:
    read from base_ref at i * chunk_size
else:
    treat chunk as all-zero
```

ASCII view:

```text
logical chunk index
        |
        v
+----------------------+
| manifest.entries[i]? |
+----------------------+
   | yes                  | no
   v                      v
+----------------+   +-------------------+
| ref + offset   |   | manifest.base_ref |
+----------------+   +-------------------+
                           | yes            | no
                           v                v
                   +----------------+   +--------+
                   | full base blob |   | zeroes |
                   +----------------+   +--------+
```

## Local Cache Semantics

The cache file and metadata file are intentionally separate:

- `cache.raw` holds bytes
- `cache.meta` holds chunk-state bookkeeping

Resident and dirty have different meanings:

- `resident = true`
  - chunk bytes can be read directly from `cache.raw`
- `dirty = true`
  - chunk bytes in `cache.raw` differ from the last published snapshot for this export

Invariant:

- `dirty => resident`

## Read Path

Read implementation starts in [Export::read()](/Users/xujihui/code/github.com/xujihui1985/nbd-server/src/core/engine/export.rs#L340).

### High-level flow

```text
NBD READ(offset, len)
        |
        v
validate range
        |
        v
map request to chunk range
        |
        v
for each chunk:
  lock chunk
  resident?
    yes -> read local bytes
    no  -> materialize chunk
        |
        v
copy requested subrange into reply buffer
        |
        v
return reply
```

### Materialization flow

```text
chunk miss
   |
   v
+----------------------+
| read_source lookup   |
+----------------------+
   |
   v
+------------------------------+
| manifest.chunk_location(i)   |
+------------------------------+
   |
   +--> zero chunk -> synthesize zero bytes
   |
   +--> ref chunk  -> range GET from object storage
   |
   v
pwrite bytes into cache.raw
   |
   v
mark resident
```

### Important details

- Reads are chunk-oriented even if the NBD request is small.
- A chunk is materialized at most once per miss path because the chunk lock serializes concurrent fetches.
- After materialization, the actual request bytes are read from local disk, not directly from the remote object.

## Write Path

Write implementation starts in [Export::write()](/Users/xujihui/code/github.com/xujihui1985/nbd-server/src/core/engine/export.rs#L368).

### High-level flow

```text
NBD WRITE(offset, data, fua?)
        |
        v
validate range
        |
        v
take shared write_gate lock
        |
        v
map request to chunk range
        |
        v
for each chunk:
  lock chunk
  determine affected subrange
  full-chunk overwrite?
    yes -> no remote read needed
    no  -> if not resident, materialize first
  pwrite new bytes into cache.raw
  mark dirty
        |
        v
if fua: flush
```

### Partial write behavior

Partial writes on a non-resident chunk must preserve untouched bytes.

That means:

```text
partial write on non-resident chunk
    |
    v
fetch current chunk bytes from read_source
    |
    v
write full chunk image into cache.raw
    |
    v
overwrite requested subrange
    |
    v
mark dirty
```

Without that fetch-first behavior, the unwritten portion of the chunk would be lost.

### Full-chunk overwrite optimization

If the write covers the entire chunk:

- no remote fetch is needed
- bytes are written directly into `cache.raw`
- chunk becomes resident and dirty

This is the cheapest write path.

## Flush Semantics

`FLUSH` does not publish to object storage.

Current behavior:

1. `cache.raw.sync_data()`
2. persist `cache.meta`

This means:

- local durability is provided
- remote durability is deferred to `snapshot`

`FUA` writes call the same flush path after the write completes.

## Concurrency Model

Two lock layers control the data path.

### Export-level gate

`write_gate` is a `tokio::RwLock<()>`.

- reads do not take it
- normal writes take a shared read guard
- snapshot and compact take the exclusive write guard

This means:

- normal writes can proceed concurrently with other writes
- snapshot/compact block all writes
- snapshot sees a stable local image

### Chunk-level locks

Each chunk has its own mutex.

This prevents races such as:

- two read misses fetching the same chunk simultaneously
- read materialization racing a write to the same chunk
- snapshot chunk packing observing half-updated chunk data

## On-Disk Layout and Logical Mapping

The local raw file is positional. Chunk `i` always maps to:

```text
local file offset = i * chunk_size
```

ASCII example with `chunk_size = 4 MiB`:

```text
cache.raw

0 MiB      4 MiB      8 MiB      12 MiB
|----------|----------|----------|----------|
| chunk 0  | chunk 1  | chunk 2  | chunk 3  |
|----------|----------|----------|----------|
```

The manifest may map those same logical chunks differently:

```text
logical image

chunk 0  -> refs[1] @ 0
chunk 1  -> refs[1] @ 4 MiB
chunk 2  -> entries[2] -> refs[2] @ 0
chunk 3  -> zero
```

So local layout is always dense and positional, while remote layout may be:

- full base
- sparse delta
- implicit zero

## Clone Read/Write Behavior

Before the first snapshot of a clone, the active `read_source` comes from the source export snapshot.

That means:

- read misses resolve through the source manifest
- partial writes on non-resident chunks fetch from the source snapshot first
- local dirty state still belongs only to the target export

The read/write code does not need a special clone branch. The difference is encoded entirely in `read_source`.

## Crash and Recovery

Crash handling is local-first.

On startup:

- `cache.meta` is loaded
- if shutdown was not clean:
  - resident is rebuilt conservatively from dirty bits
  - `snapshot_in_progress` is cleared

Recovery intentionally preserves:

- dirty local chunks

because local dirty data is authoritative until a snapshot is published.

## Why Reads and Writes Always Go Through the Local File

Even on a remote miss, the server writes fetched chunk bytes into `cache.raw` first, then serves later accesses from the local file.

This keeps one uniform writeable image for:

- reads
- writes
- checksumming during snapshot
- NBD semantics

Without that local image, the server would need a more complex in-memory overlay or multi-source read path for every request.

## Trade-offs

### Benefits

- simple block semantics
- cheap repeated reads after first materialization
- straightforward snapshot packing from one local image
- no remote mutation complexity

### Costs

- first read of a missing chunk pays remote latency
- partial writes to missing chunks require fetch-first
- local `cache.raw` is always dense in logical address space
- snapshot packing needs to walk dirty or effective chunk sets

## Future Improvements

- add read-ahead for sequential misses
- add sparse-hole awareness to local file allocation
- avoid persisting resident metadata on every miss path
- introduce a dedicated volume repository layer between `Export` and `ObjectStore`
- add metrics for:
  - resident hit ratio
  - remote range GET count
  - partial-write materialization count
  - bytes fetched vs bytes served
