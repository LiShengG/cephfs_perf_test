#!/bin/bash
set -u

SCRIPT_NAME="collect_mds_perf_raw.sh"
SCRIPT_VERSION="1.0.0"

INTERVAL=60
SAMPLES=""
DURATION=""
ONCE=0
OUTPUT_DIR=""
CLUSTER_NAME="ceph"
TAG=""

STOP_REQUESTED=0
HOST_NAME="$(hostname -s 2>/dev/null || hostname)"
RUN_START_ISO=""
RUN_START_UNIX=""

META_DIR=""
RAW_DIR=""
SUMMARY_DIR=""
LOG_DIR=""
RUN_INFO_FILE=""
MDS_LIST_FILE=""
RAW_INDEX_FILE=""
COLLECT_LOG=""
ERROR_LOG=""

declare -a MDS_LIST=()
declare -A MDS_SEQ=()
TOTAL_SUCCESS=0
ROUNDS_DONE=0

usage() {
  cat <<USAGE
Usage: ${SCRIPT_NAME} [options]
  --interval SEC       Sampling interval in seconds (default: 60)
  --samples N          Number of rounds to run
  --duration SEC       Maximum runtime in seconds
  --once               Run one round only
  --output-dir DIR     Output directory (default: ./mds_perf_raw_<timestamp>)
  --cluster-name NAME  Cluster name in metadata (default: ceph)
  --tag TAG            Tag in metadata (default: empty)
  -h, --help           Show this help
USAGE
}

ts_iso() {
  date -Iseconds
}

ts_file() {
  date +%Y%m%dT%H%M%S%z
}

log_info() {
  local msg="$*"
  local line="[$(ts_iso)] INFO: ${msg}"
  printf '%s\n' "$line" | tee -a "$COLLECT_LOG" >/dev/null
}

log_error() {
  local msg="$*"
  local line="[$(ts_iso)] ERROR: ${msg}"
  printf '%s\n' "$line" | tee -a "$ERROR_LOG" >&2 >/dev/null
}

normalize_mds_name() {
  local raw="$1"
  local name="$raw"

  name="${name##*/}"
  name="${name%.asok}"
  name="${name#ceph-mds.}"
  name="${name#mds.}"

  printf 'mds.%s\n' "$name"
}

parse_args() {
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --interval)
        [ "$#" -ge 2 ] || { echo "missing value for --interval" >&2; exit 1; }
        INTERVAL="$2"
        shift 2
        ;;
      --samples)
        [ "$#" -ge 2 ] || { echo "missing value for --samples" >&2; exit 1; }
        SAMPLES="$2"
        shift 2
        ;;
      --duration)
        [ "$#" -ge 2 ] || { echo "missing value for --duration" >&2; exit 1; }
        DURATION="$2"
        shift 2
        ;;
      --once)
        ONCE=1
        shift
        ;;
      --output-dir)
        [ "$#" -ge 2 ] || { echo "missing value for --output-dir" >&2; exit 1; }
        OUTPUT_DIR="$2"
        shift 2
        ;;
      --cluster-name)
        [ "$#" -ge 2 ] || { echo "missing value for --cluster-name" >&2; exit 1; }
        CLUSTER_NAME="$2"
        shift 2
        ;;
      --tag)
        [ "$#" -ge 2 ] || { echo "missing value for --tag" >&2; exit 1; }
        TAG="$2"
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

  if ! [[ "$INTERVAL" =~ ^[0-9]+$ ]] || [ "$INTERVAL" -le 0 ]; then
    echo "--interval must be a positive integer" >&2
    exit 1
  fi

  if [ -n "$SAMPLES" ] && { ! [[ "$SAMPLES" =~ ^[0-9]+$ ]] || [ "$SAMPLES" -le 0 ]; }; then
    echo "--samples must be a positive integer" >&2
    exit 1
  fi

  if [ -n "$DURATION" ] && { ! [[ "$DURATION" =~ ^[0-9]+$ ]] || [ "$DURATION" -le 0 ]; }; then
    echo "--duration must be a positive integer" >&2
    exit 1
  fi

  if [ -z "$OUTPUT_DIR" ]; then
    OUTPUT_DIR="./mds_perf_raw_$(date +%Y%m%dT%H%M%S%z)"
  fi
}

check_dependencies() {
  local missing=0
  for cmd in jq ceph date hostname mkdir tee; do
    if ! command -v "$cmd" >/dev/null 2>&1; then
      echo "missing dependency: $cmd" >&2
      missing=1
    fi
  done
  [ "$missing" -eq 0 ] || exit 1
}

init_output_dirs() {
  META_DIR="${OUTPUT_DIR}/meta"
  RAW_DIR="${OUTPUT_DIR}/raw"
  SUMMARY_DIR="${OUTPUT_DIR}/summary"
  LOG_DIR="${OUTPUT_DIR}/logs"

  mkdir -p "$META_DIR" "$RAW_DIR" "$SUMMARY_DIR" "$LOG_DIR" || {
    echo "failed to create output directories under ${OUTPUT_DIR}" >&2
    exit 1
  }

  RUN_INFO_FILE="${META_DIR}/run_info.json"
  MDS_LIST_FILE="${META_DIR}/mds_list.txt"
  RAW_INDEX_FILE="${SUMMARY_DIR}/raw_index.jsonl"
  COLLECT_LOG="${LOG_DIR}/collect.log"
  ERROR_LOG="${LOG_DIR}/errors.log"

  : > "$COLLECT_LOG" || { echo "failed to write ${COLLECT_LOG}" >&2; exit 1; }
  : > "$ERROR_LOG" || { echo "failed to write ${ERROR_LOG}" >&2; exit 1; }
  : > "$RAW_INDEX_FILE" || { log_error "failed to initialize ${RAW_INDEX_FILE}"; exit 1; }
}

discover_mds_list() {
  local sock
  local daemon
  MDS_LIST=()

  while IFS= read -r sock; do
    daemon="$(normalize_mds_name "$sock")"
    MDS_LIST+=("$daemon")
  done < <(find /var/run/ceph -maxdepth 1 -type s -name 'ceph-mds.*.asok' 2>/dev/null | sort)

  if [ "${#MDS_LIST[@]}" -eq 0 ]; then
    log_error "no mds asok sockets found in /var/run/ceph"
    return 1
  fi

  return 0
}

write_mds_list() {
  : > "$MDS_LIST_FILE" || { log_error "failed to write ${MDS_LIST_FILE}"; exit 1; }
  local daemon
  for daemon in "${MDS_LIST[@]}"; do
    printf '%s\n' "$daemon" >> "$MDS_LIST_FILE" || { log_error "failed to append ${MDS_LIST_FILE}"; exit 1; }
  done
}

write_run_info() {
  local duration_json="null"
  local samples_json="null"
  [ -n "$DURATION" ] && duration_json="$DURATION"
  [ -n "$SAMPLES" ] && samples_json="$SAMPLES"

  local mds_json
  mds_json="$(printf '%s\n' "${MDS_LIST[@]}" | jq -R . | jq -s .)"

  jq -n \
    --arg tag "$TAG" \
    --arg cluster_name "$CLUSTER_NAME" \
    --arg start_time "$RUN_START_ISO" \
    --argjson start_time_unix "$RUN_START_UNIX" \
    --arg host "$HOST_NAME" \
    --argjson interval_sec "$INTERVAL" \
    --argjson duration_sec "$duration_json" \
    --argjson samples "$samples_json" \
    --argjson once "$ONCE" \
    --arg script_name "$SCRIPT_NAME" \
    --arg script_version "$SCRIPT_VERSION" \
    --arg output_dir "$OUTPUT_DIR" \
    --argjson mds_list "$mds_json" \
    --argjson rounds_completed "$ROUNDS_DONE" \
    --argjson total_success_samples "$TOTAL_SUCCESS" \
    '{
      tag:$tag,
      cluster_name:$cluster_name,
      start_time:$start_time,
      start_time_unix:$start_time_unix,
      host:$host,
      interval_sec:$interval_sec,
      duration_sec:$duration_sec,
      samples:$samples,
      once:($once==1),
      mds_list:$mds_list,
      script_name:$script_name,
      script_version:$script_version,
      output_dir:$output_dir,
      rounds_completed:$rounds_completed,
      total_success_samples:$total_success_samples
    }' > "$RUN_INFO_FILE.tmp" || {
      log_error "failed to build run_info json"
      exit 1
    }

  mv -f "$RUN_INFO_FILE.tmp" "$RUN_INFO_FILE" || {
    log_error "failed to write ${RUN_INFO_FILE}"
    exit 1
  }
}

append_raw_index() {
  local seq="$1"
  local timestamp_iso="$2"
  local timestamp_unix="$3"
  local mds_daemon="$4"
  local rel_file="$5"
  local mds_name="${mds_daemon#mds.}"

  jq -nc \
    --argjson seq "$seq" \
    --arg timestamp "$timestamp_iso" \
    --argjson timestamp_unix "$timestamp_unix" \
    --arg host "$HOST_NAME" \
    --arg mds_name "$mds_name" \
    --arg mds_daemon "$mds_daemon" \
    --arg cluster "$CLUSTER_NAME" \
    --arg tag "$TAG" \
    --arg file "$rel_file" \
    '{
      seq:$seq,
      timestamp:$timestamp,
      timestamp_unix:$timestamp_unix,
      host:$host,
      mds_name:$mds_name,
      mds_daemon:$mds_daemon,
      cluster:$cluster,
      tag:$tag,
      file:$file
    }' >> "$RAW_INDEX_FILE" || {
      log_error "failed to append index for ${mds_daemon} seq=${seq}"
      exit 1
    }
}

collect_one_mds() {
  local mds_daemon="$1"
  local seq="$2"

  local ts_iso_now ts_unix_now ts_file_now mds_name
  ts_iso_now="$(ts_iso)"
  ts_unix_now="$(date +%s)"
  ts_file_now="$(ts_file)"
  mds_name="${mds_daemon#mds.}"

  local daemon_raw_dir="${RAW_DIR}/${mds_daemon}"
  mkdir -p "$daemon_raw_dir" || {
    log_error "failed to create raw dir ${daemon_raw_dir}"
    exit 1
  }

  local seq6
  printf -v seq6 '%06d' "$seq"
  local filename="${seq6}_${ts_file_now}.json"
  local rel_file="raw/${mds_daemon}/${filename}"
  local out_file="${OUTPUT_DIR}/${rel_file}"

  local tmp_perf tmp_wrap
  tmp_perf="$(mktemp)"
  tmp_wrap="$(mktemp)"

  if ! ceph daemon "$mds_daemon" perf dump > "$tmp_perf" 2>>"$ERROR_LOG"; then
    log_error "ceph daemon ${mds_daemon} perf dump failed"
    rm -f "$tmp_perf" "$tmp_wrap"
    return 1
  fi

  if ! jq -e . "$tmp_perf" >/dev/null 2>&1; then
    log_error "invalid json from ${mds_daemon} perf dump"
    rm -f "$tmp_perf" "$tmp_wrap"
    return 1
  fi

  if ! jq -n \
      --arg timestamp "$ts_iso_now" \
      --argjson timestamp_unix "$ts_unix_now" \
      --arg host "$HOST_NAME" \
      --arg mds_name "$mds_name" \
      --arg mds_daemon "$mds_daemon" \
      --arg cluster "$CLUSTER_NAME" \
      --arg tag "$TAG" \
      --argjson seq "$seq" \
      --slurpfile perf "$tmp_perf" \
      '{
        sample_meta: {
          timestamp:$timestamp,
          timestamp_unix:$timestamp_unix,
          host:$host,
          mds_name:$mds_name,
          mds_daemon:$mds_daemon,
          cluster:$cluster,
          tag:$tag,
          seq:$seq
        },
        perf_dump: $perf[0]
      }' > "$tmp_wrap"; then
    log_error "failed to wrap json for ${mds_daemon} seq=${seq}"
    rm -f "$tmp_perf" "$tmp_wrap"
    return 1
  fi

  if ! mv -f "$tmp_wrap" "$out_file"; then
    log_error "failed to write raw file ${out_file}"
    rm -f "$tmp_perf" "$tmp_wrap"
    exit 1
  fi

  rm -f "$tmp_perf"

  append_raw_index "$seq" "$ts_iso_now" "$ts_unix_now" "$mds_daemon" "$rel_file"
  log_info "sample success mds=${mds_daemon} seq=${seq} file=${rel_file}"
  TOTAL_SUCCESS=$((TOTAL_SUCCESS + 1))
  return 0
}

handle_signal() {
  STOP_REQUESTED=1
  log_info "signal received, will stop before starting next round"
}

main_loop() {
  local run_start_unix
  run_start_unix="$RUN_START_UNIX"

  while :; do
    if [ "$STOP_REQUESTED" -eq 1 ]; then
      break
    fi
    if [ -n "$SAMPLES" ] && [ "$ROUNDS_DONE" -ge "$SAMPLES" ]; then
      break
    fi

    local now_unix elapsed_total
    now_unix="$(date +%s)"
    elapsed_total=$((now_unix - run_start_unix))
    if [ -n "$DURATION" ] && [ "$elapsed_total" -ge "$DURATION" ]; then
      log_info "duration limit reached (${DURATION}s), stopping"
      break
    fi

    local round_start round_end round_elapsed sleep_sec
    round_start="$(date +%s)"
    local round_id=$((ROUNDS_DONE + 1))
    log_info "round ${round_id} started"

    local daemon
    for daemon in "${MDS_LIST[@]}"; do
      local next_seq=$(( ${MDS_SEQ[$daemon]:-0} + 1 ))
      if collect_one_mds "$daemon" "$next_seq"; then
        MDS_SEQ[$daemon]="$next_seq"
      fi
    done

    ROUNDS_DONE=$((ROUNDS_DONE + 1))
    round_end="$(date +%s)"
    round_elapsed=$((round_end - round_start))
    log_info "round ${ROUNDS_DONE} finished elapsed=${round_elapsed}s"

    if [ "$ONCE" -eq 1 ]; then
      break
    fi

    sleep_sec=$((INTERVAL - round_elapsed))
    if [ "$sleep_sec" -le 0 ]; then
      log_error "round ${ROUNDS_DONE} elapsed ${round_elapsed}s exceeds interval ${INTERVAL}s"
      continue
    fi

    sleep "$sleep_sec" &
    wait $! || true
  done
}

main() {
  parse_args "$@"
  check_dependencies
  init_output_dirs

  trap handle_signal INT TERM

  RUN_START_ISO="$(ts_iso)"
  RUN_START_UNIX="$(date +%s)"

  log_info "script start: interval=${INTERVAL} samples=${SAMPLES:-null} duration=${DURATION:-null} once=${ONCE} output_dir=${OUTPUT_DIR} cluster=${CLUSTER_NAME} tag=${TAG}"

  if ! discover_mds_list; then
    exit 1
  fi

  write_mds_list
  write_run_info
  log_info "output initialized at ${OUTPUT_DIR}"

  main_loop

  write_run_info
  log_info "script exit: rounds=${ROUNDS_DONE} success_samples=${TOTAL_SUCCESS}"
}

main "$@"
