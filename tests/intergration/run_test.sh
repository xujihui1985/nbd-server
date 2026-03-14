#!/usr/bin/env bash

set -euo pipefail

if [[ "$(uname -s)" != "Linux" ]]; then
  echo "This integration script only supports Linux." >&2
  exit 1
fi

if [[ "${EUID:-$(id -u)}" -ne 0 ]]; then
  echo "This integration script must run as root because it uses nbd-client and mount." >&2
  exit 1
fi

required_tools=(
  cargo
  curl
  jq
  fio
  mkfs.ext4
  mount
  umount
  nbd-client
  modprobe
)

for tool in "${required_tools[@]}"; do
  if ! command -v "${tool}" >/dev/null 2>&1; then
    echo "Missing required tool: ${tool}" >&2
    exit 1
  fi
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

: "${NBD_SERVER_LISTEN:=127.0.0.1:10809}"  # NBD_SERVER_LISTEN="${NBD_SERVER_LISTEN:-127.0.0.1:10809}"
: "${NBD_SERVER_ADMIN_SOCK:=/tmp/nbd-server-it.sock}"
: "${NBD_SERVER_CACHE_ROOT:=/var/tmp/nbd-server-it-cache}"
: "${NBD_SERVER_EXPORT_ROOT:=exports-it}"
: "${NBD_SERVER_BUCKET:?set NBD_SERVER_BUCKET to an existing S3/R2 bucket}"
: "${NBD_SERVER_STORAGE_BACKEND:=s3}"
: "${NBD_SERVER_REGION:=us-east-1}"
: "${NBD_SERVER_ENDPOINT_URL:=}"
: "${NBD_SERVER_R2_ACCOUNT_ID:=}"
: "${NBD_SERVER_VM01_SIZE:=1073741824}"
: "${NBD_SERVER_VM01:=vm01}"
: "${NBD_SERVER_VM02:=vm02}"
: "${NBD_SERVER_NBD0:=/dev/nbd0}"
: "${NBD_SERVER_NBD1:=/dev/nbd1}"
: "${NBD_SERVER_MOUNT_ROOT:=/var/tmp/nbd-server-it-mnt}"

SERVER_LOG="${NBD_SERVER_MOUNT_ROOT}/server.log"
VM01_MOUNT="${NBD_SERVER_MOUNT_ROOT}/${NBD_SERVER_VM01}"
VM02_MOUNT="${NBD_SERVER_MOUNT_ROOT}/${NBD_SERVER_VM02}"
SERVER_PID=""
VM01_SNAPSHOT_ID=""

mkdir -p "${NBD_SERVER_MOUNT_ROOT}"
mkdir -p "${VM01_MOUNT}" "${VM02_MOUNT}"
mkdir -p "${NBD_SERVER_CACHE_ROOT}"

cleanup() {
  set +e

  if mountpoint -q "${VM02_MOUNT}"; then
    umount "${VM02_MOUNT}"
  fi
  if mountpoint -q "${VM01_MOUNT}"; then
    umount "${VM01_MOUNT}"
  fi

  nbd-client -d "${NBD_SERVER_NBD1}" >/dev/null 2>&1 || true
  nbd-client -d "${NBD_SERVER_NBD0}" >/dev/null 2>&1 || true

  if [[ -n "${SERVER_PID}" ]] && kill -0 "${SERVER_PID}" >/dev/null 2>&1; then
    kill "${SERVER_PID}" >/dev/null 2>&1 || true
    wait "${SERVER_PID}" >/dev/null 2>&1 || true
  fi

  rm -f "${NBD_SERVER_ADMIN_SOCK}"
}

trap cleanup EXIT

admin_curl() {
  local method="$1"
  local path="$2"
  local body="${3:-}"

  if [[ -n "${body}" ]]; then
    curl --fail --silent --show-error \
      --unix-socket "${NBD_SERVER_ADMIN_SOCK}" \
      -X "${method}" \
      -H 'content-type: application/json' \
      -d "${body}" \
      "http://localhost${path}"
  else
    curl --fail --silent --show-error \
      --unix-socket "${NBD_SERVER_ADMIN_SOCK}" \
      -X "${method}" \
      "http://localhost${path}"
  fi
}

wait_for_admin() {
  local attempt
  for attempt in $(seq 1 60); do
    if [[ -S "${NBD_SERVER_ADMIN_SOCK}" ]] && admin_curl GET /v1/exports >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "Timed out waiting for admin socket ${NBD_SERVER_ADMIN_SOCK}" >&2
  return 1
}

start_server() {
  local cmd=(
    cargo run -- serve
    --cache-root "${NBD_SERVER_CACHE_ROOT}"
    --export-root "${NBD_SERVER_EXPORT_ROOT}"
    --bucket "${NBD_SERVER_BUCKET}"
    --listen "${NBD_SERVER_LISTEN}"
    --admin-sock "${NBD_SERVER_ADMIN_SOCK}"
    --storage-backend "${NBD_SERVER_STORAGE_BACKEND}"
    --region "${NBD_SERVER_REGION}"
  )

  if [[ -n "${NBD_SERVER_ENDPOINT_URL}" ]]; then
    cmd+=(--endpoint-url "${NBD_SERVER_ENDPOINT_URL}")
  fi
  if [[ -n "${NBD_SERVER_R2_ACCOUNT_ID}" ]]; then
    cmd+=(--r2-account-id "${NBD_SERVER_R2_ACCOUNT_ID}")
  fi

  (
    cd "${REPO_ROOT}"
    "${cmd[@]}"
  ) >"${SERVER_LOG}" 2>&1 &
  SERVER_PID="$!"
  wait_for_admin
}

attach_export() {
  local export_name="$1"
  local device="$2"
  local port="${NBD_SERVER_LISTEN##*:}"
  nbd-client 127.0.0.1 "${port}" "${device}" -N "${export_name}"
}

run_fio_job() {
  local mount_dir="$1"
  local job_name="$2"

  fio \
    --name="${job_name}" \
    --directory="${mount_dir}" \
    --filename="${job_name}.dat" \
    --rw=randrw \
    --bs=4k \
    --size=64M \
    --numjobs=1 \
    --iodepth=16 \
    --direct=1 \
    --time_based=0
}

modprobe nbd max_part=16
# cleanup any leftover state from previous runs to ensure a clean slate for the integration test
cleanup
start_server

echo "Creating ${NBD_SERVER_VM01}"
admin_curl POST /v1/exports/create \
  "{\"export_id\":\"${NBD_SERVER_VM01}\",\"size\":${NBD_SERVER_VM01_SIZE}}" | jq .

echo "Attaching ${NBD_SERVER_VM01} to ${NBD_SERVER_NBD0}"
attach_export "${NBD_SERVER_VM01}" "${NBD_SERVER_NBD0}"
mkfs.ext4 -F "${NBD_SERVER_NBD0}"
mount "${NBD_SERVER_NBD0}" "${VM01_MOUNT}"
run_fio_job "${VM01_MOUNT}" "${NBD_SERVER_VM01}-initial"
sync
umount "${VM01_MOUNT}"
nbd-client -d "${NBD_SERVER_NBD0}"

echo "Snapshotting ${NBD_SERVER_VM01}"
VM01_SNAPSHOT_ID="$(
  admin_curl POST "/v1/exports/${NBD_SERVER_VM01}/snapshot" \
    | tee /dev/stderr \
    | jq -r '.snapshot_id'
)"
if [[ -z "${VM01_SNAPSHOT_ID}" || "${VM01_SNAPSHOT_ID}" == "null" ]]; then
  echo "Snapshot did not return a snapshot_id" >&2
  exit 1
fi

echo "Removing ${NBD_SERVER_VM01} from the live registry"
admin_curl DELETE "/v1/exports/${NBD_SERVER_VM01}" >/dev/null

echo "Reopening ${NBD_SERVER_VM01}"
admin_curl POST /v1/exports/open \
  "{\"export_id\":\"${NBD_SERVER_VM01}\"}" | jq .

echo "Reattaching ${NBD_SERVER_VM01} to verify reopen"
attach_export "${NBD_SERVER_VM01}" "${NBD_SERVER_NBD0}"
mount "${NBD_SERVER_NBD0}" "${VM01_MOUNT}"
run_fio_job "${VM01_MOUNT}" "${NBD_SERVER_VM01}-reopen"
sync
umount "${VM01_MOUNT}"
nbd-client -d "${NBD_SERVER_NBD0}"

echo "Cloning ${NBD_SERVER_VM02} from ${NBD_SERVER_VM01}@${VM01_SNAPSHOT_ID}"
admin_curl POST /v1/exports/clone \
  "{\"export_id\":\"${NBD_SERVER_VM02}\",\"source_export_id\":\"${NBD_SERVER_VM01}\",\"source_snapshot_id\":\"${VM01_SNAPSHOT_ID}\"}" \
  | jq .

echo "Attaching ${NBD_SERVER_VM02} to ${NBD_SERVER_NBD1}"
attach_export "${NBD_SERVER_VM02}" "${NBD_SERVER_NBD1}"
mount "${NBD_SERVER_NBD1}" "${VM02_MOUNT}"
run_fio_job "${VM02_MOUNT}" "${NBD_SERVER_VM02}-clone"
sync
umount "${VM02_MOUNT}"
nbd-client -d "${NBD_SERVER_NBD1}"

echo "Snapshotting ${NBD_SERVER_VM02}"
admin_curl POST "/v1/exports/${NBD_SERVER_VM02}/snapshot" | jq .

echo "Integration flow completed successfully."
