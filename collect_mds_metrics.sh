#!/bin/bash
set -u

OUTDIR="./mds_metrics"
INTERVAL=10
REPORT_ENDPOINT=""
REPORT_TIMEOUT=5
RUN_ID=""
CONFIG_NAME=""
STOP=0

usage() {
  cat <<USAGE
Usage: $0 [--outdir DIR] [--interval SEC] [--report-endpoint URL] [--report-timeout SEC] [--run-id ID] [--config-name NAME]

Continuously collect ceph-mds perf dump and process stats for all local
/var/run/ceph/ceph-mds.*.asok instances until SIGINT/SIGTERM.
USAGE
}

ts_iso() {
  date -Iseconds
}

log_err() {
  local err_file="$1"
  shift
  printf '[%s] %s\n' "$(ts_iso)" "$*" >> "$err_file"
}

safe_append_jsonl() {
  local line="$1"
  local dst="$2"
  printf '%s\n' "$line" >> "$dst"
}

safe_append_tsv() {
  local line="$1"
  local dst="$2"
  printf '%s\n' "$line" >> "$dst"
}

report_payload() {
  local payload="$1"
  local err_file="$2"

  [ -n "$REPORT_ENDPOINT" ] || return 0

  if ! curl -fsS -m "$REPORT_TIMEOUT" \
    -H 'Content-Type: application/json' \
    -X POST \
    --data "$payload" \
    "$REPORT_ENDPOINT" >/dev/null 2>>"$err_file"; then
    log_err "$err_file" "http report failed: endpoint=${REPORT_ENDPOINT}"
    return 1
  fi
}

get_pid_from_asok() {
  local asok="$1"
  local base mds_name short_id pid

  pid="$(ceph daemon "$asok" status 2>/dev/null | jq -r '.pid // empty' 2>/dev/null || true)"
  if [[ "$pid" =~ ^[0-9]+$ ]] && kill -0 "$pid" 2>/dev/null; then
    echo "$pid"
    return 0
  fi

  base="$(basename "$asok")"
  mds_name="${base#ceph-mds.}"
  mds_name="${mds_name%.asok}"
  short_id="${mds_name%%.*}"

  pid="$(ps -eo pid=,args= | awk -v full="$mds_name" -v sid="$short_id" '
    /ceph-mds/ {
      if ($0 ~ full || $0 ~ ("-i[[:space:]]+" sid) || $0 ~ ("--id[=[:space:]]" sid)) {
        print $1
        exit
      }
    }
  ')"

  if [[ "$pid" =~ ^[0-9]+$ ]] && kill -0 "$pid" 2>/dev/null; then
    echo "$pid"
  fi
}

collect_one_mds() {
  local asok="$1"
  local host_outdir="$2"

  local base mds_name mds_daemon mds_dir perf_file proc_file err_file
  base="$(basename "$asok")"
  mds_name="${base#ceph-mds.}"
  mds_name="${mds_name%.asok}"
  mds_daemon="mds.${mds_name}"

  mds_dir="${host_outdir}/${mds_name}"
  perf_file="${mds_dir}/perf_dump_series.jsonl"
  proc_file="${mds_dir}/proc_stat_series.tsv"
  err_file="${mds_dir}/errors.log"

  mkdir -p "$mds_dir"
  touch "$perf_file" "$proc_file" "$err_file"

  if [ ! -s "$proc_file" ]; then
    safe_append_tsv $'ts\tmds\tpid\tcpu_pct\trss_kb\tread_bytes\twrite_bytes' "$proc_file"
  fi

  local now pid cpu rss read_b write_b io_file perf_raw perf_line payload
  now="$(ts_iso)"

  if ! perf_raw="$(ceph daemon "$asok" perf dump 2>>"$err_file")"; then
    log_err "$err_file" "perf dump failed for ${asok}"
    return 0
  fi

  if ! perf_line="$(printf '%s' "$perf_raw" | jq -c --arg ts "$now" --arg mds "$mds_daemon" '{ts:$ts,mds:$mds,perf_dump:{mds:.mds}}' 2>>"$err_file")"; then
    log_err "$err_file" "failed to transform perf dump for ${asok}"
    return 0
  fi
  safe_append_jsonl "$perf_line" "$perf_file"

  pid="$(get_pid_from_asok "$asok")"
  cpu=""
  rss=""
  read_b=""
  write_b=""

  if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
    cpu="$(ps -p "$pid" -o %cpu= 2>/dev/null | awk '{print $1}')"
    rss="$(ps -p "$pid" -o rss= 2>/dev/null | awk '{print $1}')"
    io_file="/proc/${pid}/io"
    if [ -r "$io_file" ]; then
      read_b="$(awk '/^read_bytes:/ {print $2}' "$io_file" 2>/dev/null)"
      write_b="$(awk '/^write_bytes:/ {print $2}' "$io_file" 2>/dev/null)"
    fi
  else
    log_err "$err_file" "pid not found or not alive for ${asok}"
  fi

  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$now" "$mds_daemon" "${pid:-}" "${cpu:-}" "${rss:-}" "${read_b:-}" "${write_b:-}" >> "$proc_file"

  if payload="$(
    jq -cn \
      --arg ts "$now" \
      --arg run_id "$RUN_ID" \
      --arg config_name "$CONFIG_NAME" \
      --arg host "$HOST_NAME" \
      --arg mds_name "$mds_name" \
      --arg mds "$mds_daemon" \
      --arg pid "${pid:-}" \
      --arg cpu_pct "${cpu:-}" \
      --arg rss_kb "${rss:-}" \
      --arg read_bytes "${read_b:-}" \
      --arg write_bytes "${write_b:-}" \
      --argjson perf_dump "$perf_raw" \
      '{
        ts:$ts,
        run_id:$run_id,
        config_name:$config_name,
        host:$host,
        mds_name:$mds_name,
        mds:$mds,
        proc_stat:{
          pid:$pid,
          cpu_pct:$cpu_pct,
          rss_kb:$rss_kb,
          read_bytes:$read_bytes,
          write_bytes:$write_bytes
        },
        perf_dump:{mds:$perf_dump.mds}
      }' 2>>"$err_file"
  )"; then
    report_payload "$payload" "$err_file" || true
  else
    log_err "$err_file" "failed to build report payload for ${asok}"
  fi
}

on_term() {
  STOP=1
}

trap on_term INT TERM

while [ "$#" -gt 0 ]; do
  case "$1" in
    --outdir|--output-dir)
      OUTDIR="$2"
      shift 2
      ;;
    --interval)
      INTERVAL="$2"
      shift 2
      ;;
    --report-endpoint)
      REPORT_ENDPOINT="$2"
      shift 2
      ;;
    --report-timeout)
      REPORT_TIMEOUT="$2"
      shift 2
      ;;
    --run-id)
      RUN_ID="$2"
      shift 2
      ;;
    --config-name)
      CONFIG_NAME="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if ! command -v ceph >/dev/null 2>&1; then
  echo "ceph command not found" >&2
  exit 1
fi
if ! command -v jq >/dev/null 2>&1; then
  echo "jq command not found" >&2
  exit 1
fi
if [ -n "$REPORT_ENDPOINT" ] && ! command -v curl >/dev/null 2>&1; then
  echo "curl command not found" >&2
  exit 1
fi
if ! [[ "$INTERVAL" =~ ^[0-9]+$ ]] || [ "$INTERVAL" -le 0 ]; then
  echo "--interval must be a positive integer" >&2
  exit 1
fi
if ! [[ "$REPORT_TIMEOUT" =~ ^[0-9]+$ ]] || [ "$REPORT_TIMEOUT" -le 0 ]; then
  echo "--report-timeout must be a positive integer" >&2
  exit 1
fi

HOST_NAME="$(hostname -s 2>/dev/null || hostname)"
HOST_OUTDIR="${OUTDIR}/${HOST_NAME}"
mkdir -p "$HOST_OUTDIR"

MANIFEST="${HOST_OUTDIR}/manifest.json"
START_TS="$(ts_iso)"
printf '{"host":"%s","start_ts":"%s","interval":%s,"run_id":"%s","config_name":"%s","report_endpoint":"%s"}\n' \
  "$HOST_NAME" "$START_TS" "$INTERVAL" "$RUN_ID" "$CONFIG_NAME" "$REPORT_ENDPOINT" > "$MANIFEST"

while [ "$STOP" -eq 0 ]; do
  mapfile -t ASOKS < <(find /var/run/ceph -maxdepth 1 -type s -name 'ceph-mds.*.asok' 2>/dev/null | sort)

  if [ "${#ASOKS[@]}" -eq 0 ]; then
    mkdir -p "${HOST_OUTDIR}/_collector"
    log_err "${HOST_OUTDIR}/_collector/errors.log" "no ceph-mds asok found under /var/run/ceph"
  else
    for asok in "${ASOKS[@]}"; do
      collect_one_mds "$asok" "$HOST_OUTDIR"
    done
  fi

  sleep "$INTERVAL" &
  wait $! || true
done

END_TS="$(ts_iso)"
printf '{"host":"%s","start_ts":"%s","end_ts":"%s","interval":%s,"run_id":"%s","config_name":"%s","stopped_by_signal":true}\n' \
  "$HOST_NAME" "$START_TS" "$END_TS" "$INTERVAL" "$RUN_ID" "$CONFIG_NAME" > "$MANIFEST"
