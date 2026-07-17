# 小Q API - 部署文件

- `xiaoq-api.service` — systemd user service 配置
- `health-check.sh` — 健康检查脚本（每30分钟 cron 执行）
- `NETWORK_SETUP.md` — 网络架构、防火墙配置、排错指南

## 安装

```bash
cp xiaoq-api.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user start xiaoq-api
```
