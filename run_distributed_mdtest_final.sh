#!/bin/bash
set -u
umask 022

CONFIG_FILE=""
RUN_TAG="$(date +%Y%m%d_%H%M%S)"
OUT_DIR="./mdtest_run_${RUN_TAG}"

ALLOW_RUN_AS_ROOT=1
MPI_BIN="/usr/lib64/openmpi/bin/mpirun"
MPI_PREFIX="/usr/lib64/openmpi"
NP_PER_TENANT=64
FILES_PER_PROC=10000
ITERATIONS=3
MDTEST_ARGS="-F -C -T -r -R -u -w 4K -e 4K"
HOSTFILE_A="mpi_hosts_a"
HOSTFILE_B="mpi_hosts_b"
HOSTFILE_C="mpi_hosts_c"
BASE_MNT="/mnt/tenant_a"
TENANT_A_DIR="${BASE_MNT}/perf_tenant_a"
TENANT_B_DIR="${BASE_MNT}/perf_tenant_b"
TENANT_C_DIR="${BASE_MNT}/perf_tenant_c"
COLLECT_CEPH_STATUS=1
COLLECT_MDS_METRICS=1
CEPH_HOST_FILE="ceph_host"
MDS_COLLECTOR_SCRIPT="collect_mds_metrics.sh"
MDS_REMOTE_BASE="/tmp/mds_metrics"
MDS_INTERVAL=20
METRICS_REPORT_ENDPOINT=""
METRICS_REPORT_TIMEOUT=5
GENERATE_SUMMARY=1
SUMMARY_MODE="both"
CONFIG_NAME=""

PID_A=""
PID_B=""
PID_C=""
RC_A=0
RC_B=0
RC_C=0
REMOTE_MDS_COLLECTORS_STARTED=0
REMOTE_MDS_COLLECTORS_STOPPED=0

declare -a MDS_HOSTS=()

usage() {
  cat <<USAGE
Usage: $0 [--config FILE] [--run-tag TAG] [--output-dir DIR]
USAGE
}

ts() {
  date "+%F %T"
}

log() {
  echo "[$(ts)] $*"
}

warn() {
  echo "[$(ts)] WARN: $*" >&2
}

fail() {
  echo "[$(ts)] ERROR: $*" >&2
  exit 1
}

has_cmd() {
  command -v "$1" >/dev/null 2>&1
}

check_file() {
  local file_path="$1"
  [ -f "$file_path" ] || fail "file not found: $file_path"
}

check_dir() {
  local dir_path="$1"
  [ -d "$dir_path" ] || fail "directory not found: $dir_path"
}

check_exec() {
  local file_path="$1"
  [ -x "$file_path" ] || fail "executable not found: $file_path"
}

parse_bool() {
  if [ "$1" = "true" ]; then
    echo 1
  else
    echo 0
  fi
}

parse_args() {
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --config)
        CONFIG_FILE="$2"
        shift 2
        ;;
      --run-tag)
        RUN_TAG="$2"
        shift 2
        ;;
      --output-dir)
        OUT_DIR="$2"
        shift 2
        ;;
      -h|--help)
        usage
        exit 0
        ;;
      *)
        echo "unknown argument: $1" >&2
        usage >&2
        exit 1
        ;;
    esac
  done
}

load_config() {
  [ -n "$CONFIG_FILE" ] || return 0
  check_file "$CONFIG_FILE"
  has_cmd jq || fail "jq command not found"

  eval "$(
    jq -r '
      def b(v): if v then 1 else 0 end;
      [
        "CONFIG_NAME=\((.config_name // \"\") | @sh)",
        "ALLOW_RUN_AS_ROOT=\(b(.allow_run_as_root // true))",
        "MPI_BIN=\((.mpi_bin // \"/usr/lib64/openmpi/bin/mpirun\") | @sh)",
        "MPI_PREFIX=\((.mpi_prefix // \"/usr/lib64/openmpi\") | @sh)",
        "NP_PER_TENANT=\(.np_per_tenant // 64)",
        "FILES_PER_PROC=\(.files_per_proc // 10000)",
        "ITERATIONS=\(.iterations // 3)",
        "MDTEST_ARGS=\((.mdtest_args // \"\") | @sh)",
        "HOSTFILE_A=\((.hostfile_a // \"mpi_hosts_a\") | @sh)",
        "HOSTFILE_B=\((.hostfile_b // \"mpi_hosts_b\") | @sh)",
        "HOSTFILE_C=\((.hostfile_c // \"mpi_hosts_c\") | @sh)",
        "BASE_MNT=\((.base_mnt // \"/mnt/tenant_a\") | @sh)",
        "TENANT_A_DIR=\((.tenant_a_dir // ((.base_mnt // \"/mnt/tenant_a\") + \"/perf_tenant_a\")) | @sh)",
        "TENANT_B_DIR=\((.tenant_b_dir // ((.base_mnt // \"/mnt/tenant_a\") + \"/perf_tenant_b\")) | @sh)",
        "TENANT_C_DIR=\((.tenant_c_dir // ((.base_mnt // \"/mnt/tenant_a\") + \"/perf_tenant_c\")) | @sh)",
        "COLLECT_CEPH_STATUS=\(b(.collect_ceph_status // true))",
        "COLLECT_MDS_METRICS=\(b(.collect_mds_metrics // true))",
        "CEPH_HOST_FILE=\((.ceph_host_file // \"ceph_host\") | @sh)",
        "MDS_COLLECTOR_SCRIPT=\((.mds_collector_script // \"collect_mds_metrics.sh\") | @sh)",
        "MDS_REMOTE_BASE=\((.mds_remote_base // \"/tmp/mds_metrics\") | @sh)",
        "MDS_INTERVAL=\(.metrics_interval_sec // 20)",
        "METRICS_REPORT_ENDPOINT=\((.metrics_report_endpoint // \"\") | @sh)",
        "METRICS_REPORT_TIMEOUT=\(.metrics_report_timeout // 5)",
        "GENERATE_SUMMARY=\(b(.generate_summary // true))",
        "SUMMARY_MODE=\((.summary_mode // \"both\") | @sh)"
      ] | .[]
    ' "$CONFIG_FILE"
  )"
}

get_hosts_from_hostfile() {
  local hostfile="$1"
  awk '
    /^[[:space:]]*#/ { next }
    /^[[:space:]]*$/ { next }
    {
      host=$1
      sub(/,.*/, "", host)
      if (host != "") print host
    }
  ' "$hostfile" | sort -u
}

is_local_host() {
  local host="$1"
  local short_host fqdn_host
  short_host="$(hostname -s 2>/dev/null || true)"
  fqdn_host="$(hostname -f 2>/dev/null || true)"

  case "$host" in
    localhost|127.0.0.1|::1) return 0 ;;
  esac

  if [ -n "$short_host" ] && [ "$host" = "$short_host" ]; then
    return 0
  fi
  if [ -n "$fqdn_host" ] && [ "$host" = "$fqdn_host" ]; then
    return 0
  fi
  if hostname -I 2>/dev/null | tr ' ' '\n' | grep -Fxq "$host"; then
    return 0
  fi
  return 1
}

remote_client_cmd() {
  local host="$1"
  shift
  ssh -o BatchMode=yes -o ConnectTimeout=10 \
    -o StrictHostKeyChecking=no \
    -o UserKnownHostsFile=/dev/null \
    "root@${host}" "$@"
}

remote_mds_cmd() {
  local host="$1"
  shift
  ssh -o BatchMode=yes -o ConnectTimeout=10 \
    -o StrictHostKeyChecking=no \
    -o UserKnownHostsFile=/dev/null \
    "root@${host}" "$@"
}

cleanup_on_signal() {
  warn "received termination signal, stopping mdtest jobs"
  for pid in "$PID_A" "$PID_B" "$PID_C"; do
    if [ -n "${pid:-}" ] && kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
    fi
  done
  sleep 2
  for pid in "$PID_A" "$PID_B" "$PID_C"; do
    if [ -n "${pid:-}" ] && kill -0 "$pid" 2>/dev/null; then
      kill -9 "$pid" 2>/dev/null || true
    fi
  done
  stop_remote_mds_collectors
  exit 130
}

cleanup_on_exit() {
  local rc=$?
  if [ "$COLLECT_MDS_METRICS" -eq 1 ]; then
    stop_remote_mds_collectors
  fi
  return "$rc"
}

trap cleanup_on_signal INT TERM
trap cleanup_on_exit EXIT

load_ceph_hosts() {
  local host_file="$1"
  MDS_HOSTS=()
  while IFS= read -r line || [ -n "$line" ]; do
    line="${line%%#*}"
    line="$(echo "$line" | xargs)"
    [ -z "$line" ] && continue
    MDS_HOSTS+=("$line")
  done < "$host_file"
}

push_mds_collector() {
  local remote_dir="${MDS_REMOTE_BASE}/${RUN_TAG}"
  local host
  for host in "${MDS_HOSTS[@]}"; do
    remote_mds_cmd "$host" "mkdir -p '${remote_dir}'" || {
      warn "failed to create remote dir: ${host}:${remote_dir}"
      continue
    }
    scp -q -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
      "$MDS_COLLECTOR_SCRIPT" "root@${host}:${remote_dir}/" || {
      warn "failed to copy collector script: ${host}"
      continue
    }
  done
}

start_remote_mds_collectors() {
  local remote_dir="${MDS_REMOTE_BASE}/${RUN_TAG}"
  local host
  local started=0
  local report_endpoint_escaped
  report_endpoint_escaped="$(printf "%s" "$METRICS_REPORT_ENDPOINT" | sed "s/'/'\\\\''/g")"
  local config_name_escaped
  config_name_escaped="$(printf "%s" "$CONFIG_NAME" | sed "s/'/'\\\\''/g")"

  for host in "${MDS_HOSTS[@]}"; do
    if ! remote_mds_cmd "$host" "remote_dir='${remote_dir}'; collector='${remote_dir}/$(basename "$MDS_COLLECTOR_SCRIPT")'; stdout_log='${remote_dir}/collector.stdout.log'; stderr_log='${remote_dir}/collector.stderr.log'; pid_file='${remote_dir}/collector.pid'; pgid_file='${remote_dir}/collector.pgid'; rm -f \"\$pid_file\" \"\$pgid_file\"; nohup setsid bash \"\$collector\" --output-dir '${remote_dir}/data' --interval '${MDS_INTERVAL}' --report-endpoint '${report_endpoint_escaped}' --report-timeout '${METRICS_REPORT_TIMEOUT}' --run-id '${RUN_TAG}' --config-name '${config_name_escaped}' >\"\$stdout_log\" 2>\"\$stderr_log\" < /dev/null & pid=\$!; printf '%s\n' \"\$pid\" > \"\$pid_file\"; ps -o pgid= -p \"\$pid\" | tr -d ' ' > \"\$pgid_file\""; then
      warn "failed to start remote collector: ${host}"
    else
      started=1
      log "remote collector started: ${host}"
    fi
  done

  REMOTE_MDS_COLLECTORS_STARTED=$started
  REMOTE_MDS_COLLECTORS_STOPPED=0
}

stop_remote_mds_collectors() {
  local remote_dir="${MDS_REMOTE_BASE}/${RUN_TAG}"
  local host

  if [ "$REMOTE_MDS_COLLECTORS_STARTED" -ne 1 ] || [ "$REMOTE_MDS_COLLECTORS_STOPPED" -eq 1 ]; then
    return 0
  fi

  for host in "${MDS_HOSTS[@]}"; do
    remote_mds_cmd "$host" "remote_dir='${remote_dir}'; pid_file='${remote_dir}/collector.pid'; pgid_file='${remote_dir}/collector.pgid'; pid=''; pgid=''; if [ -f \"\$pid_file\" ]; then pid=\$(cat \"\$pid_file\" 2>/dev/null); fi; if [ -f \"\$pgid_file\" ]; then pgid=\$(cat \"\$pgid_file\" 2>/dev/null); fi; if [ -n \"\$pid\" ]; then kill -TERM \"\$pid\" 2>/dev/null || true; fi; if [ -n \"\$pgid\" ]; then kill -TERM -\"\$pgid\" 2>/dev/null || true; fi; sleep 1; if [ -n \"\$pid\" ] && kill -0 \"\$pid\" 2>/dev/null; then kill -KILL \"\$pid\" 2>/dev/null || true; fi; if [ -n \"\$pgid\" ]; then kill -KILL -\"\$pgid\" 2>/dev/null || true; fi" || warn "failed to stop remote collector: ${host}"
  done

  REMOTE_MDS_COLLECTORS_STOPPED=1
}

fetch_remote_mds_metrics() {
  local remote_dir="${MDS_REMOTE_BASE}/${RUN_TAG}"
  local local_base="${OUT_DIR}/mds_metrics"
  local host

  mkdir -p "$local_base"

  for host in "${MDS_HOSTS[@]}"; do
    local remote_tar="${remote_dir}/${host}_mds_metrics.tar.gz"
    local local_host_dir="${local_base}/${host}"

    mkdir -p "$local_host_dir"

    remote_mds_cmd "$host" "tar -C '${remote_dir}' -czf '${remote_tar}' data collector.stdout.log collector.stderr.log collector.pid collector.pgid 2>/dev/null || tar -C '${remote_dir}' -czf '${remote_tar}' data" || {
      warn "failed to package remote metrics: ${host}"
      continue
    }

    scp -q -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
      "root@${host}:${remote_tar}" "${local_host_dir}/" || {
      warn "failed to fetch remote metrics: ${host}"
      continue
    }

    tar -C "$local_host_dir" -xzf "${local_host_dir}/${host}_mds_metrics.tar.gz" || {
      warn "failed to extract remote metrics archive: ${host}"
      continue
    }

    log "fetched metrics archive from ${host}"
  done
}

count_slots_from_hostfile() {
  local hostfile="$1"
  awk '
    BEGIN { sum=0; hosts=0 }
    /^[[:space:]]*#/ { next }
    /^[[:space:]]*$/ { next }
    {
      hosts++
      if (match($0, /slots=([0-9]+)/, a)) {
        sum += a[1]
      }
    }
    END {
      if (sum > 0) print sum;
      else print hosts;
    }
  ' "$hostfile"
}

check_hostfile_capacity() {
  local hostfile="$1"
  local tenant="$2"
  local cap
  cap=$(count_slots_from_hostfile "$hostfile")
  if [ "$cap" -lt "$NP_PER_TENANT" ]; then
    warn "${tenant} host capacity estimate ${cap} < NP_PER_TENANT(${NP_PER_TENANT})"
  else
    log "${tenant} host capacity estimate: ${cap}"
  fi
}

check_env() {
  log "checking environment"

  check_exec "$MPI_BIN"
  has_cmd mdtest || fail "mdtest command not found in PATH"

  check_file "$HOSTFILE_A"
  check_file "$HOSTFILE_B"
  check_file "$HOSTFILE_C"

  if [ "$COLLECT_MDS_METRICS" -eq 1 ]; then
    check_file "$CEPH_HOST_FILE"
    check_file "$MDS_COLLECTOR_SCRIPT"
    load_ceph_hosts "$CEPH_HOST_FILE"
    if [ "${#MDS_HOSTS[@]}" -eq 0 ]; then
      warn "no valid hosts found in ${CEPH_HOST_FILE}, disable MDS metrics collection"
      COLLECT_MDS_METRICS=0
    fi
  fi

  check_dir "$BASE_MNT"
  mkdir -p "$OUT_DIR" || fail "failed to create output directory: $OUT_DIR"

  if [ -n "$CONFIG_FILE" ]; then
    cp "$CONFIG_FILE" "${OUT_DIR}/test.conf.json"
  fi
}

collect_ceph_status() {
  local phase="$1"
  local out="${OUT_DIR}/ceph_status_${phase}.log"

  [ "$COLLECT_CEPH_STATUS" -eq 1 ] || return 0
  has_cmd ceph || {
    warn "ceph command not found, skip ceph status collection"
    return 0
  }

  {
    echo "==================== $(ts) / ${phase} ===================="
    echo "---- ceph -s ----"
    ceph -s || true
    echo
    echo "---- ceph fs status ----"
    ceph fs status || true
    echo
    echo "---- ceph health detail ----"
    ceph health detail || true
    echo
    echo "---- ceph mds stat ----"
    ceph mds stat || true
    echo
    echo "---- ceph fs perf stats ----"
    ceph fs perf stats || true
    echo
    echo "---- ceph osd perf ----"
    ceph osd perf || true
  } > "$out" 2>&1
}

prepare_test_dir() {
  local tenant="$1"
  local hostfile="$2"
  local target_dir="$3"
  local host
  local ok_count=0

  while IFS= read -r host; do
    [ -z "$host" ] && continue
    if is_local_host "$host"; then
      if mkdir -p "$target_dir"; then
        ok_count=$((ok_count + 1))
      fi
      continue
    fi
    if remote_client_cmd "$host" "mkdir -p '$target_dir'"; then
      ok_count=$((ok_count + 1))
    else
      warn "${tenant}: failed to create test dir on ${host}:${target_dir}"
    fi
  done < <(get_hosts_from_hostfile "$hostfile")

  [ "$ok_count" -gt 0 ] || fail "${tenant}: failed to prepare test dir ${target_dir}"
}

start_mdtest_job() {
  local __pidvar="$1"
  local tenant="$2"
  local hostfile="$3"
  local target_dir="$4"
  local logfile="$5"

  local root_opt=""
  if [ "$ALLOW_RUN_AS_ROOT" -eq 1 ]; then
    root_opt="--allow-run-as-root"
  fi

  # shellcheck disable=SC2206
  local extra_args=( $MDTEST_ARGS )
  local cmd=(
    "$MPI_BIN"
    ${root_opt}
    --prefix "$MPI_PREFIX"
    --hostfile "$hostfile"
    -np "$NP_PER_TENANT"
    mdtest
    -n "$FILES_PER_PROC"
    -d "$target_dir"
    -i "$ITERATIONS"
  )
  cmd+=( "${extra_args[@]}" )

  {
    echo "==== $(ts) ${tenant} ===="
    echo "CONFIG_NAME=${CONFIG_NAME}"
    echo "RUN_TAG=${RUN_TAG}"
    echo "HOSTFILE=${hostfile}"
    echo "TARGET_DIR=${target_dir}"
    echo "NP_PER_TENANT=${NP_PER_TENANT}"
    echo "FILES_PER_PROC=${FILES_PER_PROC}"
    echo "ITERATIONS=${ITERATIONS}"
    echo "MDTEST_ARGS=${MDTEST_ARGS}"
    echo "CMD=${cmd[*]}"
    echo
  } > "$logfile"

  "${cmd[@]}" >> "$logfile" 2>&1 &
  local pid=$!
  printf -v "$__pidvar" '%s' "$pid"
}

wait_job() {
  local tenant="$1"
  local pid="$2"
  [ -n "$pid" ] || return 1
  log "waiting ${tenant}, pid=${pid}"
  wait "$pid"
  return $?
}

extract_mdtest_summary() {
  local logfile="$1"
  local out="$2"
  {
    echo "===== Summary from ${logfile} ====="
    grep -E 'File creation|Directory creation|File stat|File read|File removal|Directory removal|SUMMARY rate|max|min|mean|std dev|finished' "$logfile" || true
    echo
  } >> "$out"
}

extract_aggregated_totals() {
  local log_a="$1"
  local log_b="$2"
  local log_c="$3"
  local out="$4"

  awk '
    /File creation/ { create += $3; create_cnt++ }
    /File stat/     { statv  += $3; stat_cnt++ }
    /File read/     { readv  += $3; read_cnt++ }
    /File removal/  { remove += $3; remove_cnt++ }

    END {
      printf "===== Aggregated total throughput =====\n" >> out
      printf "Total File creation OPS : %.3f\n", create >> out
      printf "Total File stat OPS     : %.3f\n", statv  >> out
      printf "Total File read OPS     : %.3f\n", readv  >> out
      printf "Total File removal OPS  : %.3f\n\n", remove >> out
      printf "===== Per-tenant average throughput =====\n" >> out
      if (create_cnt > 0) printf "Avg File creation OPS   : %.3f\n", create / create_cnt >> out
      if (stat_cnt   > 0) printf "Avg File stat OPS       : %.3f\n", statv  / stat_cnt   >> out
      if (read_cnt   > 0) printf "Avg File read OPS       : %.3f\n", readv  / read_cnt   >> out
      if (remove_cnt > 0) printf "Avg File removal OPS    : %.3f\n", remove / remove_cnt >> out
      printf "\n" >> out
    }
  ' out="$out" "$log_a" "$log_b" "$log_c"
}

generate_final_report() {
  [ "$GENERATE_SUMMARY" -eq 1 ] || return 0

  local report="${OUT_DIR}/final_summary.txt"
  local json_report="${OUT_DIR}/summary.json"
  : > "$report"

  {
    echo "=================================================="
    echo "Run tag:        ${RUN_TAG}"
    echo "Config name:    ${CONFIG_NAME}"
    echo "Output dir:     ${OUT_DIR}"
    echo "MPI_BIN:        ${MPI_BIN}"
    echo "MPI_PREFIX:     ${MPI_PREFIX}"
    echo "NP per tenant:  ${NP_PER_TENANT}"
    echo "Files per proc: ${FILES_PER_PROC}"
    echo "Iterations:     ${ITERATIONS}"
    echo "Summary mode:   ${SUMMARY_MODE}"
    echo "Base mount:     ${BASE_MNT}"
    echo "Tenant A dir:   ${TENANT_A_DIR}"
    echo "Tenant B dir:   ${TENANT_B_DIR}"
    echo "Tenant C dir:   ${TENANT_C_DIR}"
    echo "=================================================="
    echo
    echo "Exit codes:"
    echo "  Tenant_A: ${RC_A}"
    echo "  Tenant_B: ${RC_B}"
    echo "  Tenant_C: ${RC_C}"
    echo
  } >> "$report"

  extract_mdtest_summary "${OUT_DIR}/result_tenant_a.log" "$report"
  extract_mdtest_summary "${OUT_DIR}/result_tenant_b.log" "$report"
  extract_mdtest_summary "${OUT_DIR}/result_tenant_c.log" "$report"
  extract_aggregated_totals "${OUT_DIR}/result_tenant_a.log" "${OUT_DIR}/result_tenant_b.log" "${OUT_DIR}/result_tenant_c.log" "$report"

  if has_cmd python3; then
    python3 - "$OUT_DIR" "$json_report" <<'PY'
import json
import re
import sys
from pathlib import Path

out_dir = Path(sys.argv[1])
json_report = Path(sys.argv[2])

pattern = re.compile(r"^(File creation|Directory creation|File stat|File read|File removal|Directory removal)\s*:\s*([0-9.]+)")

def parse_log(path: Path):
    metrics = {}
    if not path.exists():
        return metrics
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        match = pattern.search(line.strip())
        if match:
            metrics[match.group(1)] = float(match.group(2))
    return metrics

tenants = {
    "tenant_a": parse_log(out_dir / "result_tenant_a.log"),
    "tenant_b": parse_log(out_dir / "result_tenant_b.log"),
    "tenant_c": parse_log(out_dir / "result_tenant_c.log"),
}

totals = {}
for tenant_metrics in tenants.values():
    for key, value in tenant_metrics.items():
        totals[key] = totals.get(key, 0.0) + value

payload = {
    "run_tag": out_dir.name,
    "summary_mode": "generated",
    "tenants": tenants,
    "totals": totals,
}
json_report.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
PY
  fi
}

main() {
  parse_args "$@"
  load_config

  log "starting distributed mdtest run"
  check_env
  check_hostfile_capacity "$HOSTFILE_A" "Tenant_A"
  check_hostfile_capacity "$HOSTFILE_B" "Tenant_B"
  check_hostfile_capacity "$HOSTFILE_C" "Tenant_C"

  prepare_test_dir "Tenant_A" "$HOSTFILE_A" "$TENANT_A_DIR"
  prepare_test_dir "Tenant_B" "$HOSTFILE_B" "$TENANT_B_DIR"
  prepare_test_dir "Tenant_C" "$HOSTFILE_C" "$TENANT_C_DIR"

  collect_ceph_status "before"

  if [ "$COLLECT_MDS_METRICS" -eq 1 ]; then
    log "starting remote mds collectors"
    push_mds_collector
    start_remote_mds_collectors
    sleep 5
  fi

  start_mdtest_job PID_A "Tenant_A" "$HOSTFILE_A" "$TENANT_A_DIR" "${OUT_DIR}/result_tenant_a.log"
  start_mdtest_job PID_B "Tenant_B" "$HOSTFILE_B" "$TENANT_B_DIR" "${OUT_DIR}/result_tenant_b.log"
  start_mdtest_job PID_C "Tenant_C" "$HOSTFILE_C" "$TENANT_C_DIR" "${OUT_DIR}/result_tenant_c.log"

  wait_job "Tenant_A" "$PID_A"; RC_A=$?
  wait_job "Tenant_B" "$PID_B"; RC_B=$?
  wait_job "Tenant_C" "$PID_C"; RC_C=$?

  if [ "$COLLECT_MDS_METRICS" -eq 1 ]; then
    sleep 5
    stop_remote_mds_collectors
    sleep 1
    fetch_remote_mds_metrics
  fi

  collect_ceph_status "after"
  generate_final_report

  if [ "$RC_A" -ne 0 ] || [ "$RC_B" -ne 0 ] || [ "$RC_C" -ne 0 ]; then
    warn "at least one tenant run failed, check ${OUT_DIR}/result_tenant_*.log"
    exit 1
  fi

  log "all tenants finished successfully"
}

main "$@"
