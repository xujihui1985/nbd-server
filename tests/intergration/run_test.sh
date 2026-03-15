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
  md5sum
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
: "${NBD_SERVER_VM03:=vm03}"
: "${NBD_SERVER_VM04:=vm04}"
: "${NBD_SERVER_NBD0:=/dev/nbd0}"
: "${NBD_SERVER_NBD1:=/dev/nbd1}"
: "${NBD_SERVER_NBD2:=/dev/nbd2}"
: "${NBD_SERVER_NBD3:=/dev/nbd3}"
: "${NBD_SERVER_MOUNT_ROOT:=/var/tmp/nbd-server-it-mnt}"

SERVER_LOG="${NBD_SERVER_MOUNT_ROOT}/server.log"
VM01_MOUNT="${NBD_SERVER_MOUNT_ROOT}/${NBD_SERVER_VM01}"
VM02_MOUNT="${NBD_SERVER_MOUNT_ROOT}/${NBD_SERVER_VM02}"
VM03_MOUNT="${NBD_SERVER_MOUNT_ROOT}/${NBD_SERVER_VM03}"
VM04_MOUNT="${NBD_SERVER_MOUNT_ROOT}/${NBD_SERVER_VM04}"
SERVER_PID=""
VM01_SNAPSHOT_ID=""
VM02_SNAPSHOT_ID=""
VM03_SNAPSHOT_ID=""
VM02_FIXTURE_DIR="agent-fixture-tree"
VM02_FIXTURE_MD5_MANIFEST="${NBD_SERVER_MOUNT_ROOT}/vm02-fixture.md5"
VM03_HISTORY_DIR="history-rounds"
VM03_FULL_MD5_MANIFEST="${NBD_SERVER_MOUNT_ROOT}/vm03-full-tree.md5"
VM04_FULL_MD5_MANIFEST="${NBD_SERVER_MOUNT_ROOT}/vm04-full-tree.md5"

mkdir -p "${NBD_SERVER_MOUNT_ROOT}"
mkdir -p "${VM01_MOUNT}" "${VM02_MOUNT}" "${VM03_MOUNT}" "${VM04_MOUNT}"
mkdir -p "${NBD_SERVER_CACHE_ROOT}"

cleanup() {
  set +e

  if mountpoint -q "${VM02_MOUNT}"; then
    umount "${VM02_MOUNT}"
  fi
  if mountpoint -q "${VM01_MOUNT}"; then
    umount "${VM01_MOUNT}"
  fi
  if mountpoint -q "${VM03_MOUNT}"; then
    umount "${VM03_MOUNT}"
  fi
  if mountpoint -q "${VM04_MOUNT}"; then
    umount "${VM04_MOUNT}"
  fi

  nbd-client -d "${NBD_SERVER_NBD3}" >/dev/null 2>&1 || true
  nbd-client -d "${NBD_SERVER_NBD2}" >/dev/null 2>&1 || true
  nbd-client -d "${NBD_SERVER_NBD1}" >/dev/null 2>&1 || true
  nbd-client -d "${NBD_SERVER_NBD0}" >/dev/null 2>&1 || true

  if [[ -n "${SERVER_PID}" ]] && kill -0 "${SERVER_PID}" >/dev/null 2>&1; then
    kill "${SERVER_PID}" >/dev/null 2>&1 || true
    wait "${SERVER_PID}" >/dev/null 2>&1 || true
  fi

  rm -f "${NBD_SERVER_ADMIN_SOCK}"
  rm -f \
    "${VM02_FIXTURE_MD5_MANIFEST}" \
    "${NBD_SERVER_MOUNT_ROOT}/vm03-fixture.md5" \
    "${VM03_FULL_MD5_MANIFEST}" \
    "${VM04_FULL_MD5_MANIFEST}"
  rm -rf \
    "${NBD_SERVER_CACHE_ROOT}/${NBD_SERVER_VM01}" \
    "${NBD_SERVER_CACHE_ROOT}/${NBD_SERVER_VM02}" \
    "${NBD_SERVER_CACHE_ROOT}/${NBD_SERVER_VM03}" \
    "${NBD_SERVER_CACHE_ROOT}/${NBD_SERVER_VM04}"
}

trap cleanup EXIT

admin_curl() {
  local method="$1"
  local path="$2"
  local body="${3:-}"
  local response_body
  local http_code

  response_body="$(mktemp)"

  if [[ -n "${body}" ]]; then
    http_code="$(
      curl --silent --show-error \
        --output "${response_body}" \
        --write-out '%{http_code}' \
        --unix-socket "${NBD_SERVER_ADMIN_SOCK}" \
        -X "${method}" \
        -H 'content-type: application/json' \
        -d "${body}" \
        "http://localhost${path}"
    )"
  else
    http_code="$(
      curl --silent --show-error \
        --output "${response_body}" \
        --write-out '%{http_code}' \
        --unix-socket "${NBD_SERVER_ADMIN_SOCK}" \
        -X "${method}" \
        "http://localhost${path}"
    )"
  fi

  if [[ "${http_code}" -ge 400 ]]; then
    cat "${response_body}" >&2
    rm -f "${response_body}"
    return 22
  fi

  cat "${response_body}"
  rm -f "${response_body}"
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
    "${REPO_ROOT}"/target/debug/nbd-server serve
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

write_fixture_tree() {
  local mount_dir="$1"
  local root="${mount_dir}/${VM02_FIXTURE_DIR}"

  rm -rf "${root}"
  mkdir -p \
    "${root}/alpha" \
    "${root}/alpha/nested" \
    "${root}/beta" \
    "${root}/gamma/deep"

  for index in $(seq 1 24); do
    {
      printf 'fixture export=%s clone=%s file=%02d\n' \
        "${NBD_SERVER_VM02}" \
        "${NBD_SERVER_VM03}" \
        "${index}"
      for line in $(seq 1 128); do
        printf 'line=%03d value=%08d\n' "${line}" "$((index * 1000 + line))"
      done
    } >"${root}/alpha/file-${index}.txt"
  done

  for index in $(seq 1 12); do
    {
      printf 'nested export=%s index=%02d\n' "${NBD_SERVER_VM02}" "${index}"
      for line in $(seq 1 96); do
        printf 'payload=%02d-%03d-%s\n' "${index}" "${line}" "${NBD_SERVER_VM01}"
      done
    } >"${root}/alpha/nested/blob-${index}.log"
  done

  for index in $(seq 1 16); do
    {
      printf 'beta export=%s source=%s idx=%02d\n' \
        "${NBD_SERVER_VM02}" \
        "${NBD_SERVER_VM01}" \
        "${index}"
      for line in $(seq 1 80); do
        printf 'record=%02d/%03d checksum-seed=%08d\n' "${index}" "${line}" "$((index * line))"
      done
    } >"${root}/beta/data-${index}.cfg"
  done

  for index in $(seq 1 8); do
    {
      printf 'gamma export=%s clone=%s idx=%02d\n' \
        "${NBD_SERVER_VM02}" \
        "${NBD_SERVER_VM03}" \
        "${index}"
      for line in $(seq 1 160); do
        printf 'entry=%02d.%03d token=%08x\n' "${index}" "${line}" "$((index * 4096 + line))"
      done
    } >"${root}/gamma/deep/object-${index}.dat"
  done

  (
    cd "${root}"
    find . -type f -print0 \
      | sort -z \
      | xargs -0 md5sum
  ) >"${VM02_FIXTURE_MD5_MANIFEST}"
  sync
}

verify_fixture_tree() {
  local mount_dir="$1"
  local root="${mount_dir}/${VM02_FIXTURE_DIR}"
  local actual_manifest="${NBD_SERVER_MOUNT_ROOT}/vm03-fixture.md5"

  [[ -d "${root}" ]]
  find "${root}" -type f -exec cat {} + >/dev/null

  (
    cd "${root}"
    find . -type f -print0 \
      | sort -z \
      | xargs -0 md5sum
  ) >"${actual_manifest}"

  if ! diff -u "${VM02_FIXTURE_MD5_MANIFEST}" "${actual_manifest}"; then
    echo "Fixture directory checksum manifest mismatch" >&2
    return 1
  fi
  echo "Directory checksum verification succeeded"
}

record_tree_manifest() {
  local mount_dir="$1"
  local output_path="$2"

  (
    cd "${mount_dir}"
    find . -type f -print0 \
      | sort -z \
      | xargs -0 md5sum
  ) >"${output_path}"
}

verify_tree_manifest() {
  local mount_dir="$1"
  local expected_manifest="$2"
  local actual_manifest="$3"
  local label="$4"

  find "${mount_dir}" -type f -exec cat {} + >/dev/null
  record_tree_manifest "${mount_dir}" "${actual_manifest}"

  if ! diff -u "${expected_manifest}" "${actual_manifest}"; then
    echo "${label} checksum manifest mismatch" >&2
    return 1
  fi

  echo "${label} checksum verification succeeded"
}

write_vm03_round_files() {
  local mount_dir="$1"
  local round="$2"
  local root="${mount_dir}/${VM03_HISTORY_DIR}/round-${round}"

  mkdir -p "${root}/alpha" "${root}/beta"

  for index in $(seq 1 6); do
    {
      printf 'vm=%s round=%s series=alpha file=%02d\n' "${NBD_SERVER_VM03}" "${round}" "${index}"
      for line in $(seq 1 48); do
        printf 'alpha.%s.%02d.%03d seed=%08d\n' "${round}" "${index}" "${line}" "$((round * index * 100 + line))"
      done
    } >"${root}/alpha/file-${index}.txt"
  done

  for index in $(seq 1 4); do
    {
      printf 'vm=%s round=%s series=beta file=%02d\n' "${NBD_SERVER_VM03}" "${round}" "${index}"
      for line in $(seq 1 64); do
        printf 'beta.%s.%02d.%03d token=%08x\n' "${round}" "${index}" "${line}" "$((round * 4096 + index * 256 + line))"
      done
    } >"${root}/beta/blob-${index}.log"
  done
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
write_fixture_tree "${VM02_MOUNT}"
sync
umount "${VM02_MOUNT}"
nbd-client -d "${NBD_SERVER_NBD1}"

echo "Snapshotting ${NBD_SERVER_VM02}"
VM02_SNAPSHOT_ID="$(
  admin_curl POST "/v1/exports/${NBD_SERVER_VM02}/snapshot" \
    | tee /dev/stderr \
    | jq -r '.snapshot_id'
)"
if [[ -z "${VM02_SNAPSHOT_ID}" || "${VM02_SNAPSHOT_ID}" == "null" ]]; then
  echo "Snapshot did not return a snapshot_id for ${NBD_SERVER_VM02}" >&2
  exit 1
fi

echo "Cloning ${NBD_SERVER_VM03} from ${NBD_SERVER_VM02}@${VM02_SNAPSHOT_ID}"
admin_curl POST /v1/exports/clone \
  "{\"export_id\":\"${NBD_SERVER_VM03}\",\"source_export_id\":\"${NBD_SERVER_VM02}\",\"source_snapshot_id\":\"${VM02_SNAPSHOT_ID}\"}" \
  | jq .

echo "Attaching ${NBD_SERVER_VM03} to ${NBD_SERVER_NBD2}"
attach_export "${NBD_SERVER_VM03}" "${NBD_SERVER_NBD2}"
mount "${NBD_SERVER_NBD2}" "${VM03_MOUNT}"
verify_fixture_tree "${VM03_MOUNT}"
sync
umount "${VM03_MOUNT}"
nbd-client -d "${NBD_SERVER_NBD2}"

for round in 1 2 3; do
  echo "Attaching ${NBD_SERVER_VM03} for round ${round}"
  attach_export "${NBD_SERVER_VM03}" "${NBD_SERVER_NBD2}"
  mount "${NBD_SERVER_NBD2}" "${VM03_MOUNT}"
  write_vm03_round_files "${VM03_MOUNT}" "${round}"
  sync
  umount "${VM03_MOUNT}"
  nbd-client -d "${NBD_SERVER_NBD2}"

  echo "Snapshotting ${NBD_SERVER_VM03} after round ${round}"
  VM03_SNAPSHOT_ID="$(
    admin_curl POST "/v1/exports/${NBD_SERVER_VM03}/snapshot" \
      | tee /dev/stderr \
      | jq -r '.snapshot_id'
  )"
  if [[ -z "${VM03_SNAPSHOT_ID}" || "${VM03_SNAPSHOT_ID}" == "null" ]]; then
    echo "Snapshot did not return a snapshot_id for ${NBD_SERVER_VM03} round ${round}" >&2
    exit 1
  fi
done

echo "Recording full tree manifest for ${NBD_SERVER_VM03}"
attach_export "${NBD_SERVER_VM03}" "${NBD_SERVER_NBD2}"
mount "${NBD_SERVER_NBD2}" "${VM03_MOUNT}"
record_tree_manifest "${VM03_MOUNT}" "${VM03_FULL_MD5_MANIFEST}"
sync
umount "${VM03_MOUNT}"
nbd-client -d "${NBD_SERVER_NBD2}"

echo "Cloning ${NBD_SERVER_VM04} from ${NBD_SERVER_VM03}@${VM03_SNAPSHOT_ID}"
admin_curl POST /v1/exports/clone \
  "{\"export_id\":\"${NBD_SERVER_VM04}\",\"source_export_id\":\"${NBD_SERVER_VM03}\",\"source_snapshot_id\":\"${VM03_SNAPSHOT_ID}\"}" \
  | jq .

echo "Attaching ${NBD_SERVER_VM04} to ${NBD_SERVER_NBD3}"
attach_export "${NBD_SERVER_VM04}" "${NBD_SERVER_NBD3}"
mount "${NBD_SERVER_NBD3}" "${VM04_MOUNT}"
verify_tree_manifest "${VM04_MOUNT}" "${VM03_FULL_MD5_MANIFEST}" "${VM04_FULL_MD5_MANIFEST}" "${NBD_SERVER_VM04}"
sync
umount "${VM04_MOUNT}"
nbd-client -d "${NBD_SERVER_NBD3}"

echo "Integration flow completed successfully."
