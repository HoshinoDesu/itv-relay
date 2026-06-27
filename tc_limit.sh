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
#   bash tc_limit.sh watch              # 持续看降档日志 (Ctrl+C 退出)
# ============================================================================

set -u
DEV=eth0
PORT=8088
RELAY_LOG=/root/relay.log

cmd="${1:-status}"

case "$cmd" in
  limit)
    RATE="${2:-1500}"   # kbps
    echo "=== 限速 ${DEV} 端口 ${PORT} 出站 → ${RATE}kbps ($((RATE/8))KB/s) ==="
    # 清旧
    tc qdisc del dev $DEV root 2>/dev/null
    # htb: default 10 = 未分类流量走 1:10 不限速; 8088 走 1:1 限速
    tc qdisc add dev $DEV root handle 1: htb default 10
    tc class add dev $DEV parent 1: classid 1:10 htb rate 1000mbit ceil 1000mbit
    tc class add dev $DEV parent 1: classid 1:1  htb rate ${RATE}kbit ceil ${RATE}kbit
    # sport 8088 = RPi5 发出的 8088 流量 (中继响应给播放器)
    tc filter add dev $DEV protocol ip parent 1:0 prio 1 u32 \
        match ip sport $PORT 0xffff flowid 1:1
    echo "已限速 ${RATE}kbps (8088出站). SSH不受影响."
    echo "从另一台机器拉 http://<rpi-ip>:8088/play/tv-1 应被限到此速率"
    echo ""
    echo "当前 qdisc:"; tc qdisc show dev $DEV
    ;;

  clear)
    echo "=== 清除限速 ==="
    tc qdisc del dev $DEV root 2>/dev/null && echo "已清除" || echo "本来无限速"
    tc qdisc show dev $DEV
    ;;

  status)
    echo "=== qdisc ==="; tc qdisc show dev $DEV
    echo "=== class (1:1=限速, 1:10=放行) ==="; tc class show dev $DEV 2>/dev/null
    echo "=== class 1:1 流量统计 (Sent>0 说明限速生效) ==="
    tc -s class show dev $DEV classid 1:1 2>/dev/null | grep -E "Sent|rate|backlog" | head -3
    echo ""
    echo "=== 最近降档日志 ==="
    grep -E "切档|↑|↓" "$RELAY_LOG" 2>/dev/null | tail -6 || echo "(无日志或中继未运行)"
    ;;

  watch)
    echo "=== 持续监控降档日志 (Ctrl+C 退出) ==="
    tail -f "$RELAY_LOG" 2>/dev/null | grep --line-buffered -E "切档|↑|↓|启动" || echo "无日志"
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

档位参考 (限速触发后中继应逐级降):
  档0 直通 ~8000kbps(源) | 档1 3500k | 档2 2000k | 档3 1200k(720p)
EOF
    ;;
esac
