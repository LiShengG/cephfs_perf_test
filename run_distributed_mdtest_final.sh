#!/bin/bash
# ==============================================================================
# 脚本: run_distributed_mdtest_final.sh
# 目的: 三租户并发 mdtest 元数据压测（适配统一挂载点 /mnt/tenant_a）
#
# 说明:
#   1. 三组 MPI 作业分别使用不同 hostfile，避免 client 侧资源互抢
#   2. 三组作业共享同一个 CephFS 挂载点 /mnt/tenant_a
#   3. 通过不同测试子目录区分 tenant
#   4. 若要验证多 Active MDS 隔离，请确保这些子目录已提前完成 pin / 子树归属规划
# ==============================================================================

set -u
umask 022

# --------------------------------------
# 可配置参数
# --------------------------------------
ALLOW_RUN_AS_ROOT=1

MPI_BIN="/usr/lib64/openmpi/bin/mpirun"
MPI_PREFIX="/usr/lib64/openmpi"

# NP_PER_TENANT=64
# FILES_PER_PROC=50000
# ITERATIONS=3
NP_PER_TENANT=32
FILES_PER_PROC=50000
ITERATIONS=1
MDTEST_ARGS="-F -C -T -r -R -u -w 4K -e 4K"
# -F -C -T -r -R -u -w 4K   -e 4K
# -F -C -T -r -R -u -w 16K  -e 16K
# -F -C -T -r -R -u -w 64K  -e 64K
# -F -C -T -r -R -u -w 256K -e 256K
# -F -C -T -r -R -u -w 1M   -e 1M
# -F -C -T -r -R -u -w 4M   -e 4M
# -F -C -T -r -R -u -w 16M  -e 16M
# -F -C -T -r -R -u -U
# Dir Pin
# MDS Num: 3

# 三个 hostfile（已拆分）
HOSTFILE_A="mpi_hosts_a"
HOSTFILE_B="mpi_hosts_b"
HOSTFILE_C="mpi_hosts_c"

# 统一挂载点
BASE_MNT="/mnt/tenant_a"

# 在同一挂载点下划分三个独立测试目录
TENANT_A_DIR="${BASE_MNT}/perf_tenant_a"
TENANT_B_DIR="${BASE_MNT}/perf_tenant_b"
TENANT_C_DIR="${BASE_MNT}/perf_tenant_c"

# 输出目录
RUN_TAG="$(date +%F_%H%M%S)"
OUT_DIR="./mdtest_run_${RUN_TAG}"

# 是否采集 Ceph 状态
COLLECT_CEPH_STATUS=1

# MDS 指标采集控制
COLLECT_MDS_METRICS=1
CEPH_HOST_FILE="ceph_host"
MDS_COLLECTOR_SCRIPT="collect_mds_perf_raw.sh"
MDS_REMOTE_BASE="/tmp/mds_metrics"
MDS_INTERVAL=30

# --------------------------------------
# 内部变量
# --------------------------------------
PID_A=""
PID_B=""
PID_C=""
RC_A=0
RC_B=0
RC_C=0

declare -a MDS_HOSTS=()

# --------------------------------------
# 基础函数
# --------------------------------------
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
  local f="$1"
  [ -f "$f" ] || fail "文件不存在: $f"
}

check_dir() {
  local d="$1"
  [ -d "$d" ] || fail "目录不存在: $d"
}

check_exec() {
  local f="$1"
  [ -x "$f" ] || fail "不可执行或不存在: $f"
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
    localhost|127.0.0.1|::1)
      return 0
      ;;
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

cleanup_on_signal() {
  warn "捕获到中断信号，终止后台压测任务..."
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
  exit 130
}

trap cleanup_on_signal INT TERM

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

remote_mds_cmd() {
  local host="$1"
  shift
  # 新增 StrictHostKeyChecking=no 和 UserKnownHostsFile=/dev/null 跳过密钥校验
  ssh -o BatchMode=yes -o ConnectTimeout=10 \
      -o StrictHostKeyChecking=no \
      -o UserKnownHostsFile=/dev/null \
      "root@${host}" "$@"
}

push_mds_collector() {
  local remote_dir="${MDS_REMOTE_BASE}/${RUN_TAG}"
  local host
  for host in "${MDS_HOSTS[@]}"; do
    if ! remote_mds_cmd "$host" "mkdir -p '${remote_dir}'"; then
      warn "创建远程目录失败: ${host}:${remote_dir}"
      continue
    fi
    if ! scp -q -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "$MDS_COLLECTOR_SCRIPT" "root@${host}:${remote_dir}/"; then
      warn "分发采集脚本失败: ${host}"
      continue
    fi
  done
}

start_remote_mds_collectors() {
  local remote_dir="${MDS_REMOTE_BASE}/${RUN_TAG}"
  local host
  for host in "${MDS_HOSTS[@]}"; do
    if ! remote_mds_cmd "$host" "nohup bash '${remote_dir}/${MDS_COLLECTOR_SCRIPT}' --output-dir '${remote_dir}/data' --interval '${MDS_INTERVAL}' --tag '${RUN_TAG}' >'${remote_dir}/collector.stdout.log' 2>'${remote_dir}/collector.stderr.log' < /dev/null & echo \$! > '${remote_dir}/collector.pid'"; then
      warn "远程启动采集器失败: ${host}"
    else
      log "远程采集器已启动: ${host}"
    fi
  done
}

stop_remote_mds_collectors() {
  local remote_dir="${MDS_REMOTE_BASE}/${RUN_TAG}"
  local host
  for host in "${MDS_HOSTS[@]}"; do
    remote_mds_cmd "$host" "if [ -f '${remote_dir}/collector.pid' ]; then pid=\$(cat '${remote_dir}/collector.pid'); kill -TERM \"\$pid\" 2>/dev/null || true; fi" || warn "停止远程采集器失败: ${host}"
  done
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

    if ! remote_mds_cmd "$host" "tar -C '${remote_dir}' -czf '${remote_tar}' data collector.stdout.log collector.stderr.log collector.pid 2>/dev/null || tar -C '${remote_dir}' -czf '${remote_tar}' data"; then
      warn "远程打包失败: ${host}"
      continue
    fi

    if ! scp -q -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "root@${host}:${remote_tar}" "${local_host_dir}/"; then
      warn "拉取采集包失败: ${host}"
      continue
    fi

    if ! tar -C "$local_host_dir" -xzf "${local_host_dir}/${host}_mds_metrics.tar.gz"; then
      warn "本地解包失败: ${host}"
      continue
    fi

    log "已回传并解包 MDS 采集数据: ${host}"
  done
}

# --------------------------------------
# hostfile 容量检查
# --------------------------------------
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
    warn "$tenant 的 hostfile 容量估算值($cap) < NP_PER_TENANT($NP_PER_TENANT)"
  else
    log "$tenant hostfile 容量估算值: $cap"
  fi
}

# --------------------------------------
# 环境校验
# --------------------------------------
check_env() {
  log "开始环境校验"

  check_exec "$MPI_BIN"
  has_cmd mdtest || fail "找不到 mdtest，请确认 /usr/local/bin/mdtest 在 PATH 中"

  check_file "$HOSTFILE_A"
  check_file "$HOSTFILE_B"
  check_file "$HOSTFILE_C"

  if [ "$COLLECT_MDS_METRICS" -eq 1 ]; then
    check_file "$CEPH_HOST_FILE"
    check_file "$MDS_COLLECTOR_SCRIPT"
    load_ceph_hosts "$CEPH_HOST_FILE"
    if [ "${#MDS_HOSTS[@]}" -eq 0 ]; then
      warn "${CEPH_HOST_FILE} 中没有可用主机，MDS 指标采集将被跳过"
      COLLECT_MDS_METRICS=0
    fi
  fi

  check_dir "$BASE_MNT"

  mkdir -p "$OUT_DIR" || fail "无法创建输出目录: $OUT_DIR"

  log "MPI_BIN      = $MPI_BIN"
  log "MPI_PREFIX   = $MPI_PREFIX"
  log "BASE_MNT     = $BASE_MNT"
  log "TENANT_A_DIR = $TENANT_A_DIR"
  log "TENANT_B_DIR = $TENANT_B_DIR"
  log "TENANT_C_DIR = $TENANT_C_DIR"

  log "环境校验完成"
}

# --------------------------------------
# Ceph 状态采集
# --------------------------------------
collect_ceph_status() {
  local phase="$1"
  local out="${OUT_DIR}/ceph_status_${phase}.log"

  if [ "$COLLECT_CEPH_STATUS" -ne 1 ]; then
    return 0
  fi

  if ! has_cmd ceph; then
    warn "未找到 ceph 命令，跳过 Ceph 状态采集"
    return 0
  fi

  log "采集 Ceph 状态: ${phase}"

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
    echo
  } > "$out" 2>&1

  log "Ceph 状态已写入: $out"
}

# --------------------------------------
# 测试目录准备
# --------------------------------------
prepare_test_dir() {
  local tenant="$1"
  local hostfile="$2"
  local d="$3"
  local host
  local ok_count=0

  while IFS= read -r host; do
    [ -z "$host" ] && continue

    if is_local_host "$host"; then
      if mkdir -p "$d"; then
        log "${tenant}: 本地创建测试目录成功: ${d}"
        ok_count=$((ok_count + 1))
      else
        warn "${tenant}: 本地创建测试目录失败: ${d}"
      fi
      continue
    fi

    if remote_client_cmd "$host" "mkdir -p '$d'"; then
      log "${tenant}: 远程创建测试目录成功: ${host}:${d}"
      ok_count=$((ok_count + 1))
    else
      warn "${tenant}: 远程创建测试目录失败: ${host}:${d}"
    fi
  done < <(get_hosts_from_hostfile "$hostfile")

  if [ "$ok_count" -eq 0 ]; then
    fail "${tenant}: 未能在任何客户端节点创建测试目录: ${d}"
  fi
}

# --------------------------------------
# 启动作业
# --------------------------------------
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

  log "启动 ${tenant}: hostfile=${hostfile}, dir=${target_dir}"

  {
    echo "==== $(ts) ${tenant} ===="
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
# --------------------------------------
# 等待作业结束
# --------------------------------------
wait_job() {
  local tenant="$1"
  local pid="$2"

  if [ -z "$pid" ]; then
    warn "${tenant} PID 为空"
    return 1
  fi

  log "等待 ${tenant} 完成, PID=${pid}"
  wait "$pid"
  return $?
}

# --------------------------------------
# 结果汇总
# --------------------------------------
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
      printf "Total File removal OPS  : %.3f\n", remove >> out
      printf "\n" >> out

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
  local report="${OUT_DIR}/final_summary.txt"
  : > "$report"

  {
    echo "=================================================="
    echo "Run tag:        ${RUN_TAG}"
    echo "Output dir:     ${OUT_DIR}"
    echo "MPI_BIN:        ${MPI_BIN}"
    echo "MPI_PREFIX:     ${MPI_PREFIX}"
    echo "NP per tenant:  ${NP_PER_TENANT}"
    echo "Files per proc: ${FILES_PER_PROC}"
    echo "Iterations:     ${ITERATIONS}"
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

  extract_aggregated_totals \
  "${OUT_DIR}/result_tenant_a.log" \
  "${OUT_DIR}/result_tenant_b.log" \
  "${OUT_DIR}/result_tenant_c.log" \
  "$report"

  log "汇总报告生成完成: ${report}"
}

# --------------------------------------
# 主流程
# --------------------------------------
main() {
  log "=================================================="
  log "开始三租户并发 mdtest 压测"
  log "统一挂载点: ${BASE_MNT}"
  log "=================================================="

  check_env

  check_hostfile_capacity "$HOSTFILE_A" "Tenant_A"
  check_hostfile_capacity "$HOSTFILE_B" "Tenant_B"
  check_hostfile_capacity "$HOSTFILE_C" "Tenant_C"

  prepare_test_dir "Tenant_A" "$HOSTFILE_A" "$TENANT_A_DIR"
  prepare_test_dir "Tenant_B" "$HOSTFILE_B" "$TENANT_B_DIR"
  prepare_test_dir "Tenant_C" "$HOSTFILE_C" "$TENANT_C_DIR"

  collect_ceph_status "before"

  if [ "$COLLECT_MDS_METRICS" -eq 1 ]; then
    log "开始分发并启动远程 MDS 指标采集器"
    push_mds_collector
    start_remote_mds_collectors
    log "等待 5 秒后启动 mdtest"
    sleep 5
  fi

  start_mdtest_job PID_A "Tenant_A" "$HOSTFILE_A" "$TENANT_A_DIR" "${OUT_DIR}/result_tenant_a.log"
  start_mdtest_job PID_B "Tenant_B" "$HOSTFILE_B" "$TENANT_B_DIR" "${OUT_DIR}/result_tenant_b.log"
  start_mdtest_job PID_C "Tenant_C" "$HOSTFILE_C" "$TENANT_C_DIR" "${OUT_DIR}/result_tenant_c.log"

  log "所有压测任务已启动"
  log "PID_A=${PID_A}, PID_B=${PID_B}, PID_C=${PID_C}"

  wait_job "Tenant_A" "$PID_A"; RC_A=$?
  wait_job "Tenant_B" "$PID_B"; RC_B=$?
  wait_job "Tenant_C" "$PID_C"; RC_C=$?

  if [ "$COLLECT_MDS_METRICS" -eq 1 ]; then
    log "mdtest 已结束，等待 5 秒后停止并回传 MDS 采集数据"
    sleep 5
    stop_remote_mds_collectors
    sleep 1
    fetch_remote_mds_metrics
  fi

  collect_ceph_status "after"

  generate_final_report

  log "=================================================="
  log "压测完成"
  log "Tenant_A exit code: ${RC_A}"
  log "Tenant_B exit code: ${RC_B}"
  log "Tenant_C exit code: ${RC_C}"
  log "结果目录: ${OUT_DIR}"
  log "=================================================="

  if [ "$RC_A" -ne 0 ] || [ "$RC_B" -ne 0 ] || [ "$RC_C" -ne 0 ]; then
    warn "至少有一个租户压测失败，请检查 ${OUT_DIR}/result_tenant_*.log"
    exit 1
  fi

  log "所有租户压测成功"
  log "建议查看: grep -E 'File creation|File stat|File removal' ${OUT_DIR}/result_tenant_*.log"
}

main "$@"
