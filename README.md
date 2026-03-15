# nbd-server

`nbd-server` is a Rust NBD server with a lazy local cache and a pluggable object-storage backend. The primary workflow is a long-running `serve` process that discovers and manages multiple exports through an HTTP API.

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
- `COMPACT` rewrites the entire current image into a fresh base object.
- Published snapshots are retained until explicitly deleted by the user.

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

## Manage Exports Through The API

The server-level storage configuration is provided once at `serve` startup. Export-management requests only carry logical identifiers such as `export_id` and `source_export_id`.

For `export_id = vm-001`:

- local cache dir is `/var/lib/nbd-server/vm-001`
- remote prefix is `exports/vm-001`

The client never supplies those derived paths in the request body.

### Credentials

For S3-compatible backends such as Cloudflare R2, provide credentials through the normal AWS environment variables before starting the server:

```bash
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
```

For R2, add `--storage-backend r2 --region auto --r2-account-id <account-id>` to the `serve` command.

## Attach With `nbd-client`

The server speaks fixed-newstyle NBD and supports `OPT_GO`. The NBD export name is the managed `export_id`.

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

If the export is already loaded in the current process, `open` returns a conflict instead of creating a second in-memory instance.

### Clone Export

```bash
curl -X POST --unix-socket /tmp/nbd-server.sock \
  -H 'content-type: application/json' \
  -d '{"export_id":"vm02","source_export_id":"vm01","source_snapshot_id":"0195938f4f7a7c8ab1d2e3f4a5b6c7d8"}' \
  http://localhost/v1/exports/clone
```

The request body carries only logical identifiers. The server derives cache and remote paths from global `serve` configuration.

Clone always resolves against a published remote snapshot. If `source_snapshot_id` is omitted, the server uses the source export's current published snapshot from `volume.json`.

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
  "snapshot_id": "0195d4f7a7c84b8d9a4cf59d1c4c7c9e"
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
  "snapshot_id": "0195d501f1b84e7f8d9327a45aa89d10"
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
Older snapshot objects are not deleted automatically when a new snapshot or compaction is published.


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
