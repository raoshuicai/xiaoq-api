# Gateway 侧本地补丁（不作用于本仓库的代码）

这里的 `.patch` 是打给 **hermes-agent**（`~/.hermes/hermes-agent`）的，不是打给 xiaoq-api。
xiaoq-api 只依赖这些补丁提供的 Gateway 契约，代码本身不需要它们。

## yolo-session-endpoint.patch

给 Gateway 补上 `GET/POST /v1/session/yolo`，也就是小Q APP 输入框左下角那个「Y」气泡
（会话级跳过危险命令审批）。上游没有这个端点。

它由三处改动组成，全部在 `gateway/platforms/api_server.py`：

1. `_handle_session_yolo` 端点本体（`@_require_auth`）
2. `_http_route_table()` 里注册两条路由（v0.21.5+ 是表驱动，不再是 `router.add_get`）
3. `_run_agent` 里把「会话级 YOLO」继承到当次 run 的 approval key

第 3 条是重点：v0.21.5+ 上游把 chat 路径的审批 key 从 `session_id` 改成了每次请求的
`completion_id`（`approval_session_key=completion_id`）。只补端点不补继承的话，
Y 按钮会亮、但下一轮对话照样弹审批。

### 打到 Windows/WSL 的 Hermes 上

```bash
# 判断目标版本：本补丁针对 v0.21.5+（_http_route_table 表驱动 + approval_session_key）
cd ~/.hermes/hermes-agent
git apply --check ~/workspace/xiaoq-api/patches/yolo-session-endpoint.patch
git apply          ~/workspace/xiaoq-api/patches/yolo-session-endpoint.patch
./venv/Scripts/python.exe -m py_compile gateway/platforms/api_server.py   # Linux 用 venv/bin/python
```

跨大版本升级后 `--check` 若失败，说明上游行号/上下文漂移，按上面的三处改动手工插入更可控
（`git apply --3way` 会「带冲突应用成功」，留下 `<<<<<<<` 标记且 exit 0，别被它骗）。

打完必须**重启 Gateway** 才生效。

### 验证

```bash
KEY=$(grep -m1 '^API_SERVER_KEY=' ~/.hermes/.env | cut -d= -f2- | tr -d '\r "')
curl -s -H "Authorization: Bearer $KEY" -H "X-Hermes-Session-Id: probe" \
  http://127.0.0.1:8642/v1/session/yolo
# → {"session_key": "probe", "yolo": false}
```

YOLO 是进程内存态，Gateway 重启即清空；且只跳过普通「危险命令」审批，
硬线规则（SSH 私钥、系统保护路径等）不受影响。

排查与实现细节见 skill `xiaoq-api-server` 的 `references/yolo-session-endpoint.md`。
