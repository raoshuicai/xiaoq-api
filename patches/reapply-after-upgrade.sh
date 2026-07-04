#!/bin/bash
# ============================================================================
# 小Q 审批通道补丁 — v0.18.0+ 一键恢复
# ============================================================================
# 用途：Hermes Agent v0.18.0+ 升级后，重新应用 Gateway 审批通道补丁。
#       v0.18.0 已原生支持审批通道（/v1/runs + /v1/runs/{run_id}/events），
#       但 /v1/chat/completions SSE 流不转发 approval.request 事件。
#       本补丁桥接审批事件到 chat-completions SSE 流，供给 xiaoq-api 拦截。
# 用法：bash patches/reapply-after-upgrade.sh
# 原理：git apply patches/gateway-approval-channel.patch → 重启 Gateway
# ============================================================================

set -e

PATCH_FILE="$(dirname "$0")/gateway-approval-channel.patch"
HERMES_SRC="$HOME/.hermes/hermes-agent"

echo "=== 小Q 审批通道补丁恢复 (v0.18.0+) ==="
echo ""

# 检查补丁文件
if [ ! -f "$PATCH_FILE" ]; then
    echo "❌ 补丁文件不存在: $PATCH_FILE"
    echo "   从 git clone 即可恢复："
    echo "   git clone https://github.com/xiaoqagent/Harmony_xiaoq.git -b xiaoq-api-main /tmp/harmony"
    echo "   cp /tmp/harmony/patches/gateway-approval-channel.patch $PATCH_FILE"
    exit 1
fi

echo "📄 补丁文件: $PATCH_FILE ($(wc -c < "$PATCH_FILE") bytes)"

# 备份原始文件
BACKUP_DIR="/tmp/hermes-patch-backup-$(date +%Y%m%d%H%M%S)"
mkdir -p "$BACKUP_DIR"
cp "$HERMES_SRC/gateway/platforms/api_server.py" "$BACKUP_DIR/" 2>/dev/null || true
echo "💾 备份: $BACKUP_DIR/api_server.py"

# 应用补丁
echo "🔧 应用补丁到 Gateway..."
cd "$HERMES_SRC"
if git apply --check "$PATCH_FILE" 2>/dev/null; then
    git apply "$PATCH_FILE"
    echo "✅ Gateway 补丁已应用"
else
    echo "⚠️  补丁无法直接应用（代码版本可能已变），尝试手动合并..."
    echo "   请检查 $HERMES_SRC/gateway/platforms/api_server.py"
    echo "   参考补丁内容手动修改："
    echo "   cat $PATCH_FILE"
    exit 1
fi

# 重启 Gateway
echo ""
echo "🔄 重启 Hermes Gateway..."
systemctl --user restart hermes-gateway
sleep 2
if systemctl --user is-active hermes-gateway > /dev/null; then
    echo "✅ Gateway 已重启"
else
    echo "❌ Gateway 重启失败"
    exit 1
fi

echo ""
echo "=== 完成 ==="
echo "从 APP 发送消息测试审批卡片是否正常。"
