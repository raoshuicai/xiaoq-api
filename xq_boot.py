#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""xiaoq-api Windows 启动器。

为什么要这层：server.py 读 `~/.hermes/.env`，而 Windows 侧的配置在
`%LOCALAPPDATA%\\hermes\\.env`（HERMES_HOME），两者不是同一路径。这里先把
HERMES_HOME 的 .env 灌进环境变量，再显式覆盖几个路径，最后交给 uvicorn。

用法：python xq_boot.py [port]
"""
import os
import sys
from pathlib import Path

HH = Path(os.environ.get("LOCALAPPDATA", r"C:\Users\HW\AppData\Local")) / "hermes"
PROJ = Path(r"C:\Users\HW\workspace\xiaoq-api")

# ── 1. 清掉 PM 注入的 PYTHONPATH（与 gateway 同一个坑：它指向 Python 3.14 的环境）──
os.environ.pop("PYTHONPATH", None)
os.environ.pop("PYTHONHOME", None)

# ── 2. 从 HERMES_HOME/.env 灌入凭据（不覆盖已有环境变量）──
env_file = HH / ".env"
loaded = 0
if env_file.exists():
    for line in env_file.read_text(encoding="utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        k = k.strip()
        v = v.strip().strip('"').strip("'")
        if k and k not in os.environ:
            os.environ[k] = v
            loaded += 1

# ── 3. 路径覆盖（Windows 本机语义）──
os.environ["STATE_DB"] = str(HH / "state.db")
os.environ["WIN_STATE_DB"] = str(HH / "state.db")
os.environ["OBSIDIAN_VAULT"] = r"C:\Users\xiaoq\Documents\Obsidian\hermes-vault"
os.environ.setdefault(
    "PUSH_SERVICE_ACCOUNT_KEY",
    r"D:\05_HarmonyNext\XiaoQ\101653523864334050118035699private.json",
)
os.environ.setdefault("PUSH_PROJECT_ID", "101653523864334050")
# 华为 Push 走直连（server.py 里 use_proxy=False，这里只保证不发生意外走代理）
os.environ["NO_PROXY"] = "127.0.0.1,localhost,::1"
os.environ["no_proxy"] = "127.0.0.1,localhost,::1"

port = int(sys.argv[1]) if len(sys.argv) > 1 else int(os.getenv("PORT", "8866"))
os.environ["PORT"] = str(port)

# ── 4. 自检 ──
print(f"[xq_boot] python   = {sys.version.split()[0]}", flush=True)
print(f"[xq_boot] HERMES_HOME = {HH}", flush=True)
print(f"[xq_boot] .env keys   = {loaded} loaded", flush=True)
print(f"[xq_boot] STATE_DB    = {os.environ['STATE_DB']}  exists={Path(os.environ['STATE_DB']).exists()}", flush=True)
print(f"[xq_boot] PUSH_DB     = {Path.home() / '.hermes' / 'push_messages.db'}  exists={(Path.home()/'.hermes'/'push_messages.db').exists()}", flush=True)
print(f"[xq_boot] OBSIDIAN    = {os.environ['OBSIDIAN_VAULT']}  exists={Path(os.environ['OBSIDIAN_VAULT']).exists()}", flush=True)
print(f"[xq_boot] PUSH_KEY    = {os.environ['PUSH_SERVICE_ACCOUNT_KEY']}  exists={Path(os.environ['PUSH_SERVICE_ACCOUNT_KEY']).exists()}", flush=True)
print(f"[xq_boot] API_SERVER_KEY = {'set' if os.getenv('API_SERVER_KEY') else 'MISSING'}", flush=True)
print(f"[xq_boot] PORT        = {port}", flush=True)

os.chdir(PROJ)
sys.path.insert(0, str(PROJ))

# 关键依赖自检（早失败早暴露）
try:
    import fastapi, uvicorn, httpx, websockets, cryptography  # noqa: F401
    print("[xq_boot] deps fastapi/uvicorn/httpx/websockets/cryptography OK", flush=True)
except Exception as e:
    print(f"[xq_boot] ❌ 依赖缺失: {type(e).__name__}: {e}", flush=True)
    raise

import uvicorn  # noqa: E402

uvicorn.run("server:app", host="0.0.0.0", port=port, log_level="info")
