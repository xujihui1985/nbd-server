# nbd-server

`nbd-server` is a Rust NBD server with a lazy local cache and a pluggable object-storage backend. It supports both direct single-export modes and a manager-backed `serve` mode that discovers and serves multiple exports.

## Model

- The live writable image is a fixed-size local raw file: `cache.raw`.
- Local chunk state lives in `cache.meta`.
- Reads first consult the local cache. Missing chunks are fetched from object storage, written into `cache.raw`, then served.
- Partial writes on missing chunks first materialize the full chunk from remote, then merge the write locally.
- `FLUSH` only guarantees local durability.
- `SNAPSHOT` publishes remote state:
  - for a brand-new export, the first publish writes a sparse manifest with no `base_ref` and uploads only dirty chunks into one data blob
  - later publishes upload one packed delta blob containing only dirty chunks
  - each publish writes a complete human-readable `manifest.json`
- `COMPACT` rewrites the entire current image into a fresh base object and garbage-collects unreferenced remote data objects.

## Build

```bash
cargo build
```

## Serve Mode

Start the multi-export server with a global cache root and export root. On startup it discovers existing `volume.json` files from object storage and loads those exports automatically.

```bash
cargo run -- serve \
  --cache-root /var/lib/nbd-server \
  --export-root exports \
  --bucket my-bucket \
  --listen 127.0.0.1:10809 \
  --admin-sock /tmp/nbd-server.sock
```

In `serve` mode:

- local cache directories are derived as `<cache-root>/<export_id>`
- remote export prefixes are derived as `<export-root>/<export_id>`
- the NBD listener can negotiate any currently loaded export name
- `volume.json` is the authoritative remote head for each export

## Create A New Export

### AWS S3

```bash
cargo run -- create \
  --export-id vm-001 \
  --size 1073741824 \
  --cache-dir /var/lib/nbd-server/vm-001 \
  --bucket my-bucket \
  --prefix exports/vm-001 \
  --listen 127.0.0.1:10809 \
  --admin-sock /tmp/nbd-server-vm-001.sock
```

### Cloudflare R2

Use your R2 access key pair through the normal AWS credential environment variables:

```bash
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
```

Then run:

```bash
cargo run -- create \
  --export-id vm-001 \
  --size 1073741824 \
  --cache-dir /var/lib/nbd-server/vm-001 \
  --storage-backend r2 \
  --bucket my-r2-bucket \
  --prefix exports/vm-001 \
  --region auto \
  --r2-account-id <account-id> \
  --listen 127.0.0.1:10809 \
  --admin-sock /tmp/nbd-server-vm-001.sock
```

## Open An Existing Export

```bash
cargo run -- open \
  --export-id vm-001 \
  --cache-dir /var/lib/nbd-server/vm-001 \
  --bucket my-bucket \
  --prefix exports/vm-001 \
  --listen 127.0.0.1:10809 \
  --admin-sock /tmp/nbd-server-vm-001.sock
```

By default, direct `open` resolves `exports/<id>/refs/current.json`. In `serve` mode, export loading resolves from `exports/<id>/volume.json`. To open a specific snapshot id in direct mode instead:

```bash
cargo run -- open \
  --export-id vm-001 \
  --cache-dir /var/lib/nbd-server/vm-001 \
  --bucket my-bucket \
  --prefix exports/vm-001 \
  --snapshot-id 0195938f4f7a7c8ab1d2e3f4a5b6c7d8 \
  --listen 127.0.0.1:10809 \
  --admin-sock /tmp/nbd-server-vm-001.sock
```

Reusing the same `--cache-dir` across snapshots is supported only when the local cache has no dirty chunks. If the cache still contains unsnapshotted local writes for another snapshot, `open` fails instead of silently mixing snapshot lineages.

For R2, add:

```bash
  --storage-backend r2 \
  --region auto \
  --r2-account-id <account-id>
```

## Clone From Snapshot

Create a new export lineage from a published remote snapshot. Before the first snapshot, reads are seeded from the source snapshot lazily. The first snapshot of the cloned export uploads a full base blob for the target export and becomes generation `1`.

```bash
cargo run -- clone \
  --export-id vm02 \
  --cache-dir /var/lib/nbd-server/vm02 \
  --bucket my-bucket \
  --prefix exports/vm02 \
  --source-prefix exports/vm01 \
  --source-snapshot-id 0195938f4f7a7c8ab1d2e3f4a5b6c7d8 \
  --listen 127.0.0.1:10810 \
  --admin-sock /tmp/nbd-server-vm02.sock
```

Clone always resolves against a published remote snapshot. If a running `vm01` process has local dirty writes, those writes are ignored by clone.

## Attach With `nbd-client`

The server speaks fixed-newstyle NBD and supports `OPT_GO`.

- in direct mode, there is one export named exactly like `--export-id`
- in `serve` mode, the export name is the managed `export_id`

```bash
sudo modprobe nbd max_part=8
sudo nbd-client 127.0.0.1 10809 /dev/nbd0 -N vm-001
```

Detach:

```bash
sudo nbd-client -d /dev/nbd0
```

## Admin API

The admin API is HTTP over a Unix domain socket.

### List Exports

```bash
curl --unix-socket /tmp/nbd-server.sock http://localhost/v1/exports
```

### Create Export

```bash
curl -X POST --unix-socket /tmp/nbd-server.sock \
  -H 'content-type: application/json' \
  -d '{"export_id":"vm01","size":1073741824}' \
  http://localhost/v1/exports/create
```

### Open Export

```bash
curl -X POST --unix-socket /tmp/nbd-server.sock \
  -H 'content-type: application/json' \
  -d '{"export_id":"vm01"}' \
  http://localhost/v1/exports/open
```

This loads an export that already exists remotely under `exports/<export_id>/volume.json`.

### Clone Export

```bash
curl -X POST --unix-socket /tmp/nbd-server.sock \
  -H 'content-type: application/json' \
  -d '{"export_id":"vm02","source_export_id":"vm01","source_snapshot_id":"0195938f4f7a7c8ab1d2e3f4a5b6c7d8"}' \
  http://localhost/v1/exports/clone
```

The request body carries only logical identifiers. The server derives cache and remote paths from global `serve` configuration.

### Status

```bash
curl --unix-socket /tmp/nbd-server.sock http://localhost/v1/exports/vm01/status
```

Example response:

```json
{
  "export_id": "vm-001",
  "image_size": 1073741824,
  "chunk_size": 4194304,
  "chunk_count": 256,
  "resident_chunks": 12,
  "dirty_chunks": 2,
  "snapshot_generation": 3,
  "remote_head_generation": 3,
  "operation_state": "idle"
}
```

### Snapshot

```bash
curl -X POST --unix-socket /tmp/nbd-server.sock \
  http://localhost/v1/exports/vm01/snapshot
```

Example response:

```json
{
  "snapshot_created": true,
  "generation": 4,
  "snapshot_id": "0195d4f7a7c84b8d9a4cf59d1c4c7c9e",
  "garbage_collected_objects": 1
}
```

### Compact

```bash
curl -X POST --unix-socket /tmp/nbd-server.sock \
  http://localhost/v1/exports/vm01/compact
```

Example response:

```json
{
  "generation": 5,
  "snapshot_id": "0195d501f1b84e7f8d9327a45aa89d10",
  "garbage_collected_objects": 3
}
```

### Reset Cache

Discard all local dirty and resident cache state for the current export. This is destructive for unsnapshotted local writes and is intended for cases where you want to abandon local changes before reopening another snapshot with the same `--cache-dir`.

```bash
curl -X POST --unix-socket /tmp/nbd-server.sock \
  http://localhost/v1/exports/vm01/cache/reset
```

## Remote Layout

- `exports/<id>/volume.json`
- `exports/<id>/refs/current.json` for direct-mode compatibility
- `exports/<id>/snapshots/<snapshot_id>/manifest.json`
- `exports/<id>/snapshots/<snapshot_id>/delta.blob`
- `exports/<id>/snapshots/<snapshot_id>/base.blob` for compaction output

`volume.json` stores the current published snapshot id for the export. The manifest path is inferred from that snapshot id.

## Recovery

- Dirty local chunks are authoritative across restart.
- Clean resident chunks are treated as cache and may be discarded after an unclean shutdown.
- `snapshot.journal.json` records the in-flight publish target so startup can clean up stale local staging files after a crash.
- Incomplete remote uploads are tolerated conservatively: the journal is cleared locally, dirty bits remain set, and a later snapshot can republish the data.
- In `serve` mode, server restart reconstructs the in-memory export registry by scanning remote `volume.json` objects under the configured export root.

## Tests

```bash
cargo test
```

## Storage Backends

### S3

- `--storage-backend s3`
- optional `--endpoint-url` for S3-compatible systems
- optional `--region`, default `us-east-1`

### Cloudflare R2

- `--storage-backend r2`
- `--r2-account-id <account-id>` or explicit `--endpoint-url`
- `--region auto`
- path-style requests are enabled automatically for R2
