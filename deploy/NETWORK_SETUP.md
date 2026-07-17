# 小Q API 网络配置

## 网络架构

```
手机 (WiFi HUAWEI-404)
    │  http://192.168.3.87:8866
    ▼
Windows (192.168.3.87) ─── WSL2 (networkingMode=mirrored)
    │                         │
    │ 防火墙放行 8866         │ xiaoq-api (0.0.0.0:8866)
    │                         │ → 文件浏览
    │                         │ → WebSocket 聊天
    │                         │ → 反向代理 /v1/* → Gateway (8642)
    │                         │ → Push Kit / Huawei 推送
    ▼                         │
Cloudflare Tunnel            │
(hermes.xiao-q.com)          │
(xiaoq.xiao-q.com)           │
```

## 服务配置

**系统服务**: `~/.config/systemd/user/xiaoq-api.service`

```ini
ExecStart=/home/xiaoq/.hermes/hermes-agent/venv/bin/python3 -m uvicorn server:app --host 0.0.0.0 --port 8866
```

- `--host 0.0.0.0` — 监听所有接口，必须（见下方网络说明）
- `--port 8866` — 服务端口

## WSL 网络模式

`.wslconfig` 配置：

```
[wsl2]
networkingMode=mirrored
```

### mirrored 模式的特点

| 特性 | 说明 |
|------|------|
| IP 共享 | WSL 和 Windows 共用 IP（192.168.3.87） |
| 127.0.0.1 共享 | WSL 和 Windows 共用 loopback，同一端口不能同时绑定 |
| portproxy 冲突 | 不可用！因为 WSL 和 Windows 争 `127.0.0.1:8866` |

### 为什么不用 portproxy

早期使用 NAT 模式 + `netsh interface portproxy`，切换到 mirrored 后：
- portproxy `0.0.0.0:8866 → 127.0.0.1:8866` → WSL 的 127.0.0.1 和 Windows 共享，端口冲突
- 服务绑 `127.0.0.1` + portproxy → `address already in use`
- **解决方案**: 服务直接绑 `0.0.0.0` + Windows 防火墙放行入站端口

## Windows 防火墙配置

```powershell
# 新增入站规则（仅需执行一次，重启后保留）
netsh advfirewall firewall add rule name="小Q API 8866" dir=in protocol=tcp localport=8866 action=allow
```

## 端口映射

| 服务 | 端口 | 绑定 | 说明 |
|------|------|------|------|
| xiaoq-api | 8866 | `0.0.0.0` | 主服务（外网可访问） |
| Hermes Gateway | 8642 | `0.0.0.0` | 仅供内部反向代理 |
| Hermes WebUI | 8787 | `127.0.0.1` | 仅供本地访问 |
| Hindsight | 8888 | `127.0.0.1` | 仅供本地访问 |
| Clash (代理) | 7897 | `0.0.0.0` | 出站代理 |

## APP 连接策略

**文件**: `entry/src/main/ets/pages/FileViewPage.ets`
**聊天**: `entry/src/main/ets/pages/ChatPage.ets`

```typescript
const HOME_WIFI_SSID: string = 'HUAWEI-404';
// WiFi 下走本地
const LOCAL_API: string = 'http://192.168.3.87:8866';
// 外网走 Cloudflare 隧道
const PUBLIC_API: string = 'https://xiaoq.xiao-q.com';
```

## 健康检查

脚本位置: `~/.hermes/scripts/health-check.sh`

检查项：
1. 系统服务状态（5个服务）
2. 近30分钟 ERROR 日志
3. 日志文件大小（超过 50MB 告警）
4. 日志目录总大小（超过 200MB 告警）
5. Hindsight 连通性 (`localhost:8888`)
6. xiaoq-api 连通性 (`localhost:8866`)
7. WebUI 响应性 (`127.0.0.1:8787`)

**注意**: LAN 外部可达性（`192.168.3.87:8866`）无法从本机/WSL 内部测试——本机连自己 IP 走 loopback，不经过真实网口。仅能由外部设备（手机）实测验证。

Cron: 每30分钟 `health-check.sh`（no_agent 模式），异常时推送通知。

## 排错指南

### 手机连不上 WiFi 本地服务

1. 确认手机连的是 `HUAWEI-404` WiFi
2. 确认 Windows 防火墙放行 8866：
   ```powershell
   netsh advfirewall firewall show rule name="小Q API 8866"
   ```
3. 确认 xiaoq-api 监听 `0.0.0.0`：
   ```bash
   ss -tlnp | grep 8866  # 应显示 0.0.0.0:8866
   ```
4. 从手机浏览器访问 `http://192.168.3.87:8866/health` 应返回 JSON

### 服务起不来

```bash
journalctl --user -u xiaoq-api -n 20 --no-pager
# 常见错误: address already in use → 检查 portproxy 是否冲突
# netsh interface portproxy delete v4tov4 listenport=8866 listenaddress=0.0.0.0
```

### 端口被占用

```bash
# WSL 侧
ss -tlnp | grep 8866
# Windows 侧（从 WSL 执行）
cd /mnt/c/Windows/System32 && ./cmd.exe /c 'netstat -ano | findstr ":8866"'
```
