#!/bin/bash
# Hermes 服务健康检查 + 日志异常扫描
set -euo pipefail

LOGDIR="$HOME/.hermes/logs"
NOW=$(date +%s)
HALF_HOUR_AGO=$((NOW - 1800))
THRESHOLD_GROWTH_MB=50

# ── 1. 服务状态 ──
SERVICES=("hermes-gateway" "hermes-webui" "cloudflared-hermes" "hermes-hindsight" "xiaoq-api")
FAILED=()
for svc in "${SERVICES[@]}"; do
    if ! systemctl --user is-active --quiet "$svc" 2>/dev/null; then
        FAILED+=("$svc")
    fi
done
if [ ${#FAILED[@]} -gt 0 ]; then
    echo "⚠️ Hermes 服务异常: ${FAILED[*]}"
    for svc in "${FAILED[@]}"; do
        echo "[$svc]"
        systemctl --user status "$svc" --no-pager -l 2>&1 | head -10
    done
fi

# ── 2. 最近 ERROR 日志 ──
if [ -f "$LOGDIR/errors.log" ]; then
    ERRORS=$(awk -v ts="$HALF_HOUR_AGO" '
        /^[0-9]{4}-[0-9]{2}-[0-9]{2}/ {
            gsub(/[,T]/, " ", $1, $2); gsub(/-/, " ", $1); gsub(/:/, " ", $2);
            split($1 " " $2, a, " ");
            if (length(a) >= 6) {
                t = mktime(a[1] " " a[2] " " a[3] " " a[4] " " a[5] " " a[6]);
                if (t >= ts && /ERROR/) print;
            }
        }
    ' "$LOGDIR/errors.log" 2>/dev/null | head -20 || true)
    if [ -n "$ERRORS" ]; then
        echo "⚠️ 过去 30 分钟发现 ERROR 日志:"
        echo "$ERRORS"
    fi
fi

# ── 3. 日志文件大小 ──
for logfile in "$LOGDIR"/*.log "$LOGDIR"/*.log.*; do
    [ -f "$logfile" ] || continue
    SIZE_MB=$(du -m "$logfile" 2>/dev/null | cut -f1)
    if [ "${SIZE_MB:-0}" -gt "$THRESHOLD_GROWTH_MB" ]; then
        echo "⚠️ 日志文件过大: $(basename "$logfile") = ${SIZE_MB}MB"
    fi
done

# ── 4. 日志目录总大小 ──
TOTAL_SIZE=$(du -sm "$LOGDIR" 2>/dev/null | cut -f1)
if [ "${TOTAL_SIZE:-0}" -gt 200 ]; then
    echo "⚠️ 日志目录总大小 ${TOTAL_SIZE}MB，超过阈值 200MB"
fi

# ── 5. Hindsight ──
if ! curl -sf --max-time 5 http://localhost:8888/health >/dev/null 2>&1; then
    echo "⚠️ Hindsight 服务无法连接 (localhost:8888)"
fi

# ── 6. xiaoq-api ──
if ! curl -sf --max-time 5 http://localhost:8866/health >/dev/null 2>&1; then
    echo "⚠️ xiaoq-api 服务无法连接 (localhost:8866)"
fi

# ── 7. WebUI 响应性（用写文件规避 set -e 问题）──
# 注意：用 /health 而非 /api/health/agent，因为后者不在 PUBLIC_PATHS 白名单中，
# curl 不带 cookie 会返回 401。详见 api/auth.py:49
set +e
curl -s -o /dev/null -w "%{http_code}" --max-time 5 \
    http://127.0.0.1:8787/health >/tmp/hermes_webui_http_code 2>/dev/null
WEBUI_CODE=$(cat /tmp/hermes_webui_http_code 2>/dev/null || echo "000")
rm -f /tmp/hermes_webui_http_code
if [ "$WEBUI_CODE" != "200" ]; then
    echo "⚠️ WebUI /health 不可达 (http_code=$WEBUI_CODE)"
fi

# ── 8. Hermes 版本追踪 ──
# 注意: gateway-approval-channel.patch 已随 v0.18.0 原生审批通道退役（2026-07-04）。
# 不再需要检查或应用该补丁。

# ── 9. xiaoq-api LAN 可达性 ──
# 已在第6节通过 localhost:8866 验证服务运行。
# LAN 外部可达性（192.168.3.87:8866）无法从本机/ WSL 内部测试：
#   本机连自己的 IP 走 loopback 而非真实网口，测不出手机 WiFi 接入路径。
#   仅能由外部设备（手机 APP）实测验证。当前已验证手机可连。
