# nbd-server

`nbd-server` is a single-export Rust NBD server with a lazy local cache and a pluggable object-storage backend.

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

By default, `open` resolves `exports/<id>/refs/current.json`. To open a specific snapshot generation instead:

```bash
cargo run -- open \
  --export-id vm-001 \
  --cache-dir /var/lib/nbd-server/vm-001 \
  --bucket my-bucket \
  --prefix exports/vm-001 \
  --snapshot-id 7 \
  --listen 127.0.0.1:10809 \
  --admin-sock /tmp/nbd-server-vm-001.sock
```

Reusing the same `--cache-dir` across generations is supported only when the local cache has no dirty chunks. If the cache still contains unsnapshotted local writes for another generation, `open` fails instead of silently mixing snapshot lineages.

For R2, add:

```bash
  --storage-backend r2 \
  --region auto \
  --r2-account-id <account-id>
```

## Attach With `nbd-client`

The server speaks fixed-newstyle NBD and supports `OPT_GO` for a single export named exactly like `--export-id`.

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

### Status

```bash
curl --unix-socket /tmp/nbd-server-vm-001.sock http://localhost/v1/status
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
curl -X POST --unix-socket /tmp/nbd-server-vm-001.sock \
  http://localhost/v1/snapshot
```

### Compact

```bash
curl -X POST --unix-socket /tmp/nbd-server-vm-001.sock \
  http://localhost/v1/compact
```

### Reset Cache

Discard all local dirty and resident cache state for the current export. This is destructive for unsnapshotted local writes and is intended for cases where you want to abandon local changes before reopening another snapshot with the same `--cache-dir`.

```bash
curl -X POST --unix-socket /tmp/nbd-server-vm-001.sock \
  http://localhost/v1/cache/reset
```

## Remote Layout

- `exports/<id>/refs/current.json`
- `exports/<id>/snapshots/<generation>/manifest.json`
- `exports/<id>/snapshots/<generation>/delta.blob`
- `exports/<id>/snapshots/<generation>/base.blob` for compaction output

## Recovery

- Dirty local chunks are authoritative across restart.
- Clean resident chunks are treated as cache and may be discarded after an unclean shutdown.
- `snapshot.journal.json` records the in-flight publish target so startup can clean up stale local staging files after a crash.
- Incomplete remote uploads are tolerated conservatively: the journal is cleared locally, dirty bits remain set, and a later snapshot can republish the data.

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
