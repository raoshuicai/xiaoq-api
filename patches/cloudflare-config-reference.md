# Cloudflare Tunnel 配置参考

文件位置: `~/.cloudflared/config.yml`

```yaml
ingress:
  - hostname: hermes.xiao-q.com
    service: http://localhost:8787
    originRequest:
      noTLSVerify: true
  - hostname: xiaoq.xiao-q.com
    service: http://localhost:8866
  - hostname: price.xiao-q.com
    service: http://localhost:8088
  - service: http_status:404
```

变更说明 (2026-07-04):
- 之前: xiaoq.xiao-q.com 的 /v1/* 分流到 Gateway (8642)，其余到 xiaoq-api (8866)
- 现在: 全部流量走 xiaoq-api (8866)，由 proxy_hermes_v1 转发 /v1/* 给 Gateway
- 原因: 审批POST在5G下通过Cloudflare时直接走了Gateway，绕过xiaoq-api的proxy拦截
