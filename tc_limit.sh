#!/bin/bash
# ============================================================================
# tc_limit.sh — RPi5 出口限速模拟 + 降档观测工具
#
# 原理: htb 限 eth0 上 8088 端口的出站流量 (中继→外网播放器这一段),
#       SSH(22) 及其他流量不受影响, 模拟 ISP 限流触发中继动态降档。
#
# 之前踩的坑 (本脚本已规避):
#   - netem 限整个 eth0 → SSH 也被限, 长会话卡死, 不可用
#   - htb default 0 → 未分类包走 direct 放行, 8088 流量根本没进限速 class
#   正解: htb default 10 (不限速放行) + class 1:1 限速 + filter sport 8088 → 1:1
#
# 用法:
#   bash tc_limit.sh limit <速率kbps>   # 限速 (如 limit 1500 = 1.5Mbps)
#   bash tc_limit.sh clear              # 清除限速
#   bash tc_limit.sh status             # 看当前限速状态 + 降档日志
#   bash tc_limit.sh watch              # 持续看 ABR/切档日志 (Ctrl+C 退出)
#   bash tc_limit.sh expect-down [秒]   # 等待真实播放触发降档并完成切档
#   bash tc_limit.sh expect-recover [kbps] [降档秒] [升档秒]
#                                        # 限速触发降档, 清限速后等待自动升档
# ============================================================================

set -u
DEV="${DEV:-eth0}"
PORT="${PORT:-8088}"
RELAY_LOG="${RELAY_LOG:-/root/relay.log}"
ARTIFACT_DIR="${ARTIFACT_DIR:-tc-artifacts}"
PLAYER_URL="${PLAYER_URL:-http://<relay-ip>:${PORT}/play/tv-1}"
VISUAL_OK="${VISUAL_OK:-pending}"
VISUAL_NOTE="${VISUAL_NOTE:-}"
LOG_RE="启动|首降|降档|升档|回退|无缝切档完成|观察期"
DOWN_RE="首降.*0→[1-9]|降档 [0-9]+→[1-9]"
DOWN_DONE_RE="无缝切档完成 → 档[1-9]"
UP_RE="升档 [0-9]+→0"
UP_DONE_RE="无缝切档完成 → 档0"

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

require_tc() {
  command -v tc >/dev/null 2>&1 || fail "找不到 tc 命令, 请安装 iproute2"
}

require_positive_int() {
  local name="$1"
  local value="$2"
  case "$value" in
    ''|*[!0-9]*)
      fail "${name} 必须是正整数: ${value}"
      ;;
  esac
  if [ "$value" -le 0 ]; then
    fail "${name} 必须大于0: ${value}"
  fi
}

run_tc() {
  "$@" || fail "tc 命令失败: $*"
}

tc_limit_class_exists() {
  tc class show dev "$DEV" 2>/dev/null | grep -q 'class htb 1:1'
}

tc_sent_bytes() {
  tc -s class show dev "$DEV" classid 1:1 2>/dev/null \
    | awk '/Sent/ {print $2; found=1; exit} END {if (!found) print ""}'
}

require_sent_increased() {
  local before="$1"
  local after
  after="$(tc_sent_bytes)"
  if [ -z "$before" ] || [ -z "$after" ]; then
    fail "无法读取 tc class 1:1 的 Sent 统计, 限速可能没有正确安装"
  fi
  if [ "$after" -le "$before" ]; then
    fail "tc class 1:1 Sent 未增长 (${before}→${after}), 8088 出站流量没有命中限速规则"
  fi
  echo "OK: tc class 1:1 已命中播放流量 Sent ${before}→${after} bytes"
}

write_tc_status() {
  local output="$1"
  {
    echo "=== qdisc ==="
    tc qdisc show dev "$DEV" 2>&1 || true
    echo
    echo "=== class ==="
    tc -s class show dev "$DEV" 2>&1 || true
    echo
    echo "=== filter ==="
    tc filter show dev "$DEV" parent 1:0 2>&1 || true
  } > "$output"
}

write_recover_summary() {
  local output="$1"
  local status="$2"
  local sent_before="$3"
  local sent_after="$4"
  {
    echo "status=${status}"
    echo "timestamp=$(date -Is)"
    echo "dev=${DEV}"
    echo "port=${PORT}"
    echo "rate_kbps=${RATE}"
    echo "down_wait_s=${DOWN_WAIT}"
    echo "up_wait_s=${UP_WAIT}"
    echo "relay_log=${RELAY_LOG}"
    echo "player_url=${PLAYER_URL}"
    echo "visual_ok=${VISUAL_OK}"
    echo "visual_note=${VISUAL_NOTE}"
    echo "sent_before=${sent_before}"
    echo "sent_after=${sent_after}"
    if [ -n "$sent_before" ] && [ -n "$sent_after" ]; then
      echo "sent_delta=$((sent_after - sent_before))"
    else
      echo "sent_delta="
    fi
    echo "down_log=down.log"
    echo "up_log=up.log"
    echo "tc_limited=tc-limited.txt"
    echo "tc_final=tc-final.txt"
  } > "$output"
}

write_down_summary() {
  local output="$1"
  local status="$2"
  local check_tc="$3"
  local sent_before="$4"
  local sent_after="$5"
  {
    echo "status=${status}"
    echo "timestamp=$(date -Is)"
    echo "dev=${DEV}"
    echo "port=${PORT}"
    echo "wait_s=${WAIT}"
    echo "relay_log=${RELAY_LOG}"
    echo "player_url=${PLAYER_URL}"
    echo "visual_ok=${VISUAL_OK}"
    echo "visual_note=${VISUAL_NOTE}"
    echo "checked_tc=${check_tc}"
    echo "sent_before=${sent_before}"
    echo "sent_after=${sent_after}"
    if [ -n "$sent_before" ] && [ -n "$sent_after" ]; then
      echo "sent_delta=$((sent_after - sent_before))"
    else
      echo "sent_delta="
    fi
    echo "down_log=down.log"
    echo "tc_final=tc-final.txt"
  } > "$output"
}

cmd="${1:-status}"

case "$cmd" in
  limit)
    require_tc
    RATE="${2:-1500}"   # kbps
    require_positive_int "速率kbps" "$RATE"
    echo "=== 限速 ${DEV} 端口 ${PORT} 出站 → ${RATE}kbps ($((RATE/8))KB/s) ==="
    # 清旧
    tc qdisc del dev "$DEV" root 2>/dev/null || true
    # htb: default 10 = 未分类流量走 1:10 不限速; 8088 走 1:1 限速
    run_tc tc qdisc add dev "$DEV" root handle 1: htb default 10
    run_tc tc class add dev "$DEV" parent 1: classid 1:10 htb rate 1000mbit ceil 1000mbit
    run_tc tc class add dev "$DEV" parent 1: classid 1:1 htb rate "${RATE}kbit" ceil "${RATE}kbit"
    # sport 8088 = RPi5 发出的 8088 流量 (中继响应给播放器)
    run_tc tc filter add dev "$DEV" protocol ip parent 1:0 prio 1 u32 \
        match ip sport "$PORT" 0xffff flowid 1:1
    echo "已限速 ${RATE}kbps (8088出站). SSH不受影响."
    echo "从另一台机器拉 ${PLAYER_URL} 应被限到此速率"
    echo ""
    echo "当前 qdisc:"; tc qdisc show dev "$DEV"
    ;;

  clear)
    require_tc
    echo "=== 清除限速 ==="
    tc qdisc del dev "$DEV" root 2>/dev/null && echo "已清除" || echo "本来无限速"
    tc qdisc show dev "$DEV"
    ;;

  status)
    require_tc
    echo "=== qdisc ==="; tc qdisc show dev "$DEV"
    echo "=== class (1:1=限速, 1:10=放行) ==="; tc class show dev "$DEV" 2>/dev/null
    echo "=== class 1:1 流量统计 (Sent>0 说明限速生效) ==="
    tc -s class show dev "$DEV" classid 1:1 2>/dev/null | grep -E "Sent|rate|backlog" | head -3
    echo "=== filter (应有 sport ${PORT} → 1:1) ==="
    tc filter show dev "$DEV" parent 1:0 2>/dev/null | sed -n '1,8p'
    echo ""
    echo "=== 最近 ABR/切档日志 ==="
    grep -E "$LOG_RE|↑|↓" "$RELAY_LOG" 2>/dev/null | tail -12 || echo "(无日志或中继未运行)"
    ;;

  watch)
    echo "=== 持续监控 ABR/切档日志 (Ctrl+C 退出) ==="
    echo "log=${RELAY_LOG}"
    tail -f "$RELAY_LOG" 2>/dev/null | grep --line-buffered -E "$LOG_RE|↑|↓" || echo "无日志"
    ;;

  expect-down)
    require_tc
    WAIT="${2:-90}"
    require_positive_int "等待秒数" "$WAIT"
    TMP="$(mktemp)"
    mkdir -p "$ARTIFACT_DIR"
    ARTIFACT_PATH="${ARTIFACT_DIR}/expect-down-$(date +%Y%m%d-%H%M%S)"
    mkdir -p "$ARTIFACT_PATH"
    trap 'cp "$TMP" "$ARTIFACT_PATH/down.log" 2>/dev/null || true; write_tc_status "$ARTIFACT_PATH/tc-final.txt" 2>/dev/null || true; rm -f "$TMP"' EXIT
    CHECK_TC=0
    SENT_BEFORE=""
    SENT_AFTER=""
    if tc_limit_class_exists; then
      CHECK_TC=1
      SENT_BEFORE="$(tc_sent_bytes)"
      echo "tc class 1:1 Sent 起点: ${SENT_BEFORE:-unknown} bytes"
    else
      echo "WARN: 未发现 tc class 1:1, 本次只验证日志; 如需真实限速证据请先运行 limit 或使用 expect-recover"
    fi
    echo "=== 等待 ${WAIT}s 内出现降档触发 + 切档完成 ==="
    echo "现在请从另一台机器播放 ${PLAYER_URL}"
    echo "如需把观感写入 summary: VISUAL_OK=1 VISUAL_NOTE='无明显卡顿' bash $0 expect-down ..."
    echo "log=${RELAY_LOG}"
    echo "artifact=${ARTIFACT_PATH}"
    timeout "$WAIT" bash -c "tail -n0 -F \"\$1\" | grep --line-buffered -E \"\$2|↑|↓\"" _ "$RELAY_LOG" "$LOG_RE" | tee "$TMP"
    if [ "$CHECK_TC" -eq 1 ]; then
      SENT_AFTER="$(tc_sent_bytes)"
      if [ -z "$SENT_BEFORE" ] || [ -z "$SENT_AFTER" ]; then
        echo "FAIL: 无法读取 tc class 1:1 的 Sent 统计, 限速可能没有正确安装"
        write_down_summary "$ARTIFACT_PATH/summary.txt" "failed-tc-sent" "$CHECK_TC" "$SENT_BEFORE" "$SENT_AFTER"
        exit 1
      fi
      if [ "$SENT_AFTER" -le "$SENT_BEFORE" ]; then
        echo "FAIL: tc class 1:1 Sent 未增长 (${SENT_BEFORE}→${SENT_AFTER}), 8088 出站流量没有命中限速规则"
        write_down_summary "$ARTIFACT_PATH/summary.txt" "failed-tc-sent" "$CHECK_TC" "$SENT_BEFORE" "$SENT_AFTER"
        exit 1
      fi
      echo "OK: tc class 1:1 已命中播放流量 Sent ${SENT_BEFORE}→${SENT_AFTER} bytes"
    fi
    if grep -Eq "$DOWN_RE" "$TMP" && grep -Eq "$DOWN_DONE_RE" "$TMP"; then
      echo "OK: 已观察到降到非0档并完成切档"
      write_down_summary "$ARTIFACT_PATH/summary.txt" "ok" "$CHECK_TC" "$SENT_BEFORE" "$SENT_AFTER"
      echo "验收记录已保存: ${ARTIFACT_PATH}"
      exit 0
    fi
    echo "FAIL: 未在 ${WAIT}s 内同时观察到降到非0档和切档完成"
    write_down_summary "$ARTIFACT_PATH/summary.txt" "failed-down" "$CHECK_TC" "$SENT_BEFORE" "$SENT_AFTER"
    exit 1
    ;;

  expect-recover)
    require_tc
    RATE="${2:-2000}"
    DOWN_WAIT="${3:-90}"
    UP_WAIT="${4:-120}"
    require_positive_int "速率kbps" "$RATE"
    require_positive_int "降档等待秒数" "$DOWN_WAIT"
    require_positive_int "升档等待秒数" "$UP_WAIT"
    TMP_DOWN="$(mktemp)"
    TMP_UP="$(mktemp)"
    ARTIFACT_PATH=""
    mkdir -p "$ARTIFACT_DIR"
    ARTIFACT_PATH="${ARTIFACT_DIR}/expect-recover-$(date +%Y%m%d-%H%M%S)"
    mkdir -p "$ARTIFACT_PATH"
    trap 'cp "$TMP_DOWN" "$ARTIFACT_PATH/down.log" 2>/dev/null || true; cp "$TMP_UP" "$ARTIFACT_PATH/up.log" 2>/dev/null || true; write_tc_status "$ARTIFACT_PATH/tc-final.txt" 2>/dev/null || true; rm -f "$TMP_DOWN" "$TMP_UP"; tc qdisc del dev "$DEV" root 2>/dev/null' EXIT
    echo "=== 真实恢复测试: 限速 ${RATE}kbps → 等降档 → 清限速 → 等升档 ==="
    echo "现在请从另一台机器持续播放 ${PLAYER_URL}"
    echo "如需把观感写入 summary: VISUAL_OK=1 VISUAL_NOTE='无明显卡顿' bash $0 expect-recover ..."
    echo "log=${RELAY_LOG}"
    echo "artifact=${ARTIFACT_PATH}"
    bash "${BASH_SOURCE[0]}" limit "$RATE"
    write_tc_status "$ARTIFACT_PATH/tc-limited.txt"
    SENT_BEFORE="$(tc_sent_bytes)"
    echo "tc class 1:1 Sent 起点: ${SENT_BEFORE:-unknown} bytes"
    echo "=== 等待 ${DOWN_WAIT}s 内出现降档触发 + 切档完成 ==="
    timeout "$DOWN_WAIT" bash -c "tail -n0 -F \"\$1\" | grep --line-buffered -E \"\$2|↑|↓\"" _ "$RELAY_LOG" "$LOG_RE" | tee "$TMP_DOWN"
    SENT_AFTER="$(tc_sent_bytes)"
    if [ -z "$SENT_BEFORE" ] || [ -z "$SENT_AFTER" ]; then
      echo "FAIL: 无法读取 tc class 1:1 的 Sent 统计, 限速可能没有正确安装"
      write_recover_summary "$ARTIFACT_PATH/summary.txt" "failed-tc-sent" "$SENT_BEFORE" "$SENT_AFTER"
      exit 1
    fi
    if [ "$SENT_AFTER" -le "$SENT_BEFORE" ]; then
      echo "FAIL: tc class 1:1 Sent 未增长 (${SENT_BEFORE}→${SENT_AFTER}), 8088 出站流量没有命中限速规则"
      write_recover_summary "$ARTIFACT_PATH/summary.txt" "failed-tc-sent" "$SENT_BEFORE" "$SENT_AFTER"
      exit 1
    fi
    echo "OK: tc class 1:1 已命中播放流量 Sent ${SENT_BEFORE}→${SENT_AFTER} bytes"
    if ! grep -Eq "$DOWN_RE" "$TMP_DOWN" || ! grep -Eq "$DOWN_DONE_RE" "$TMP_DOWN"; then
      echo "FAIL: 未在 ${DOWN_WAIT}s 内观察到降到非0档和切档完成"
      write_recover_summary "$ARTIFACT_PATH/summary.txt" "failed-down" "$SENT_BEFORE" "$SENT_AFTER"
      exit 1
    fi
    cp "$TMP_DOWN" "$ARTIFACT_PATH/down.log"
    echo "OK: 已观察到降档, 现在清除限速等待自动升档"
    bash "${BASH_SOURCE[0]}" clear
    echo "=== 等待 ${UP_WAIT}s 内出现升档触发 + 切档完成 ==="
    timeout "$UP_WAIT" bash -c "tail -n0 -F \"\$1\" | grep --line-buffered -E \"\$2|↑|↓\"" _ "$RELAY_LOG" "$LOG_RE" | tee "$TMP_UP"
    if grep -Eq "$UP_RE" "$TMP_UP" && grep -Eq "$UP_DONE_RE" "$TMP_UP"; then
      cp "$TMP_UP" "$ARTIFACT_PATH/up.log"
      write_tc_status "$ARTIFACT_PATH/tc-final.txt"
      write_recover_summary "$ARTIFACT_PATH/summary.txt" "ok" "$SENT_BEFORE" "$SENT_AFTER"
      echo "OK: 已观察到清限速后的自动升档回档0和切档完成"
      echo "验收记录已保存: ${ARTIFACT_PATH}"
      exit 0
    fi
    echo "FAIL: 未在 ${UP_WAIT}s 内同时观察到升档回档0和切档完成"
    write_recover_summary "$ARTIFACT_PATH/summary.txt" "failed-up" "$SENT_BEFORE" "$SENT_AFTER"
    exit 1
    ;;

  *)
    cat <<EOF
用法: bash $0 <命令>

  limit <kbps>   限速 8088 出站到指定速率 (默认1500=1.5Mbps)
                 例: limit 800   限到 800kbps (应触发更深降档)
                     limit 400   限到 400kbps
  clear          清除限速 (恢复全速)
  status         看当前限速状态 + 限速class流量 + 最近降档日志
  watch          持续看降档日志 (Ctrl+C 退出)
  expect-down [秒] 等待真实播放触发降档并完成切档 (默认90秒)
  expect-recover [kbps] [降档秒] [升档秒]
                 限速触发降档, 清限速后等待自动升档
                 例: expect-recover 2000 90 120

档位参考:
  限速阶段日志应出现「首降/降档 X→非0档」和「无缝切档完成 → 档非0」。
  恢复阶段日志应出现「升档 X→0」和「无缝切档完成 → 档0」。

环境变量:
  DEV=eth0 PORT=8088 RELAY_LOG=/root/relay.log bash $0 status
EOF
    ;;
esac
