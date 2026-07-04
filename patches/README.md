# Hermes 审批通道补丁 — 升级后恢复指南

升级 Hermes 后，审批通道补丁会被覆盖。以下所有操作已自动化。

## 自动恢复机制

每 30 分钟的健康检查会做三件事：
1. 读取 `~/.hermes/hermes-agent` 的当前 git commit hash
2. 跟上次保存的 hash 对比（`~/.hermes/scripts/.gateway-patch-version`）
3. 如果有变化 → 自动重打 `patches/gateway-approval-channel.patch` → 重启 Gateway

## 涉及的修改

### Gateway（~/.hermes/hermes-agent，升级后会被重置）
- `gateway/platforms/api_server.py` — 3 处改动：
  1. `_write_sse_chat_completion` 的 `_emit`：识别 `approval.request` 事件以 SSE 格式发出
  2. `_run_agent` 方法：新增 `stream_queue` 参数 + 注册审批 notify 回调
  3. `_handle_run_approval`：加中文 choice 映射（同意本次/同意会话/总是同意/拒绝）

### xiaoq-api（~/workspace/xiaoq-api，独立仓库不受影响）
- `server.py`：SSE 解析器识别 `approval.request` 并转发 APP WebSocket
- `server.py`：WebSocket 收到 `approve_response` 后 POST 到 Gateway 解阻塞

### 健康检查脚本（~/.hermes/scripts/health-check.sh，升级后需重放）
- 加了一个版本监控段，自动重打补丁并重启 Gateway

## 手动恢复（如果自动恢复失败）

```bash
bash ~/workspace/xiaoq-api/patches/reapply-after-upgrade.sh
```

如果新版 Hermes 改了 `api_server.py` 导致补丁冲突，脚本会提示手动合并，这时参考补丁内容手动修改。
