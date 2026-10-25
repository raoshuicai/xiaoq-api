#!/usr/bin/env python3
"""
小Q API - Hermes 伴侣服务
为 小Q Android APP 提供文件浏览 + Token 统计 API

部署: systemd user service on port 8089
暴露: cloudflared tunnel xiaoq.xiao-q.com → localhost:8089
"""

import asyncio
import base64
import json
import os
import re
import uuid
import shutil
import io
import sqlite3
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException, Query, Request, BackgroundTasks
from fastapi import UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, PlainTextResponse, StreamingResponse

# ── Config ──────────────────────────────────────────────────────────
SERVER_VERSION = "v1.0.1"   # V4 四bug修复：幂等模块/粘性计数/帧session_id/status拆档（S1-S15）

# 手机端可见的会话来源：APP 自己 + 桌面/WebUI + CLI + 各聊天平台。
# cron/subagent/oneshot 是内部调度噪音，不列（否则一天几十条把列表冲掉）。
SESSION_SOURCES = ("api_server", "webui", "desktop", "tui", "cli",
                   "feishu", "telegram", "weixin", "ntfy", "unknown")
_SESSION_SRC_SQL = ", ".join(f"'{s}'" for s in SESSION_SOURCES)

# 判断「另一端正在这个会话上跑 turn」：靠 state.db 的 last_activity_description
# 命中特征 + last_activity_at 在窗口内。描述会永久残留（实测 9 天前的会话还写着
# "terminal command running (81s elapsed)"），所以时间窗口是必要条件，不能只看字符串。
_BUSY_DESC = ("receiving stream response", "tool running",
              "terminal command running", "starting new turn")
_BUSY_WINDOW_S = 90


def _is_busy(desc: Optional[str], at: Optional[float]) -> bool:
    if not desc or not at:
        return False
    return (time.time() - float(at)) <= _BUSY_WINDOW_S and any(p in desc for p in _BUSY_DESC)


def _session_busy(session_id: str) -> tuple[bool, str]:
    """这个会话是不是正在被另一端（桌面/WebUI/CLI）跑。返回 (busy, 描述)。"""
    try:
        db = _state_db()
        row = db.execute(
            "SELECT last_activity_at, last_activity_description FROM sessions WHERE id = ?",
            (session_id,),
        ).fetchone()
        db.close()
    except Exception as e:
        print(f"[Busy] 查询失败 session={session_id}: {e}", flush=True)
        return False, ""
    if row is None:
        return False, ""
    desc = row[1] or ""
    return (_is_busy(desc, row[0]), desc)

# ── Proxy Config ──────────────────────────────────────────────────────
# 出站代理：用于访问外部 API（OpenRouter、华为 Push Kit 等）
# 入站请求不受影响（FastAPI 本身不使用这些变量）
PROXIES = {
    "http://": "http://127.0.0.1:7897",
    "https://": "http://127.0.0.1:7897",
}

# 注意：不要加 `-> httpx.AsyncClient` 返回注解 —— Python 会在**定义时**求值注解，
# 而本模块的 PROXIES/helper 出现在 `import httpx` 之前，加了就会 NameError。
def _httpx_async_client(timeout: float, use_proxy: bool = False):
    """统一构造 AsyncClient，屏蔽 httpx 0.28 的 proxies 参数变更。

    use_proxy=True  -> 走 PROXIES 配置（国外服务，如 OpenRouter）
    use_proxy=False -> 直连且 trust_env=False（国内服务，如华为 Push；
                       显式忽略环境里的代理变量，避免被兜到 Clash 上反而更慢/失败）

    NOTE: 版本判断放在函数体内做 —— 本模块的 PROXIES 定义在 import httpx
    之前，模块级引用 httpx 会在加载期直接 NameError。
    """
    if not use_proxy:
        return httpx.AsyncClient(timeout=timeout, trust_env=False)
    ver = tuple(int(p) for p in httpx.__version__.split(".")[:2] if p.isdigit())
    if ver < (0, 28):
        return httpx.AsyncClient(timeout=timeout, proxies=PROXIES)
    mounts = {}
    for scheme, url in PROXIES.items():
        mounts[scheme] = httpx.AsyncHTTPTransport(proxy=url)
    return httpx.AsyncClient(timeout=timeout, mounts=mounts)

# Load .env for OpenRouter credentials
_ENV_FILE = Path(os.path.expanduser("~/.hermes/.env"))
if _ENV_FILE.exists():
    for _line in _ENV_FILE.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _key, _, _val = _line.partition("=")
            if _key.strip() not in os.environ:
                os.environ[_key.strip()] = _val.strip()

OBSIDIAN_VAULT = Path(os.getenv("OBSIDIAN_VAULT",
    r"C:\Users\xiaoq\Documents\Obsidian\hermes-vault" if os.name == "nt"
    else "/mnt/c/Users/xiaoq/Documents/Obsidian/hermes-vault"))
STATE_DB = Path(os.getenv("STATE_DB", os.path.expanduser("~/.hermes/state.db")))
API_KEY = os.getenv("XIAOQ_API_KEY", os.getenv("API_SERVER_KEY", ""))
PORT = int(os.getenv("PORT", "8089"))
TZ = timezone(timedelta(hours=8))
OPENROUTER_KEY = os.getenv("OPENROUTER_API_KEY", "")
OPENROUTER_URL = os.getenv("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1")
GATEWAY_KEY = os.getenv("API_SERVER_KEY", "")

ALLOWED_EXTENSIONS = {".md", ".txt", ".yaml", ".yml", ".json", ".csv", ".log", ".py", ".js", ".ts", ".html", ".css", ".pdf", ".png", ".jpg", ".jpeg", ".gif", ".svg"}
READABLE_EXTENSIONS = {".md", ".txt", ".yaml", ".yml", ".json", ".csv", ".log", ".py", ".js", ".ts", ".html", ".css"}
MAX_FILE_SIZE = 5 * 1024 * 1024  # 5MB

# ── Resumable streaming state ───────────────────────────────────────
# {session_id: {"seq": int, "tokens": [...], "full_response": str, "status": "processing"|"complete"}}
active_streams: dict[str, dict] = {}

# ── Approval coordination ────────────────────────────────────────────
# {run_id: {"event": asyncio.Event, "result": str|None, "session_id": str,
#           "command": str, "description": str, "pattern_keys": list,
#           "created_at": float, "expired": bool}}
#
# S42：超时后**不再删除**，改为标记 expired 保留 —— 这样 APP 端误触/切页/断连
# 丢失的审批，可以在「设置-审批历史」里找回并继续提交（走直连 Gateway 补交）。
_pending_approvals: dict[str, dict] = {}
_approval_event_loop: asyncio.AbstractEventLoop | None = None
# 审批记录保留时长（秒）。超过则视为彻底过期，查询时顺手清掉。
# 取 30 分钟：足够用户在手机上滑出去再回来处理，又不至于无限堆积。
_APPROVAL_KEEP_SECONDS: float = 1800.0

app = FastAPI(title="小Q API", version=SERVER_VERSION.lstrip("v"))

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Auth ─────────────────────────────────────────────────────────────
def check_auth(request: Request):
    """Bearer Token 鉴权"""
    if not API_KEY:
        return  # 无 key 时跳过鉴权
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer ") or auth[7:] != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")


# ── Health ───────────────────────────────────────────────────────────
@app.get("/health")
def health():
    return {"status": "ok", "service": "xiaoq-api", "version": SERVER_VERSION.lstrip("v")}


# ── Scorer Names (射手中文名映射) ──────────────────────────────────
SCORER_NAMES_FILE = Path(__file__).parent / "scorer_names.json"

@app.get("/api/scorer_names")
def get_scorer_names():
    """返回射手英文名→中文名映射（APP启动时自动拉取）"""
    if SCORER_NAMES_FILE.exists():
        return JSONResponse(json.loads(SCORER_NAMES_FILE.read_text(encoding="utf-8")))
    return JSONResponse({})


# ── APK Download ─────────────────────────────────────────────────────
from fastapi.responses import FileResponse

@app.get("/xiaoq.apk")
@app.get("/dl/apk")
def download_apk():
    """直接下载最新 APK"""
    apk = Path(os.getenv("APK_PATH", r"D:\05_HarmonyNext\XiaoQ\dist\小Q.apk"))
    if not apk.exists():
        raise HTTPException(status_code=404, detail="APK not found")
    return FileResponse(
        apk,
        media_type="application/vnd.android.package-archive",
        filename="小Q-v2.0.10.apk",
        headers={
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )


# ══════════════════════════════════════════════════════════════════════
#  文件浏览 API
# ══════════════════════════════════════════════════════════════════════

# ─────────────────── S22：上传（图片 / 文件）───────────────────

UPLOAD_DIR = Path(os.path.expanduser("~/.hermes/uploads"))
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
MAX_UPLOAD_SIZE = 20 * 1024 * 1024      # 20MB


def _safe_upload_name(raw: str) -> str:
    """
    清洗上传文件名，只保留 [A-Za-z0-9._-]，其余替换为下划线。

    ⚠️ 必须做：filename 完全来自客户端，直接拼路径会被 `../../` 穿越到任意位置。
    这里连路径分隔符一起过滤，从源头消灭穿越可能；GET 侧还会再复核一次。
    """
    base = os.path.basename(raw or "upload.bin")
    safe = "".join(c if (c.isalnum() or c in "._-") else "_" for c in base)
    safe = safe.lstrip(".") or "upload.bin"
    return safe[:120]


@app.post("/api/upload")
async def upload_file(request: Request, file: UploadFile = File(...)):
    """上传图片/文件，返回可访问 URL（S22）。"""
    check_auth(request)
    try:
        data = await file.read()
        if len(data) > MAX_UPLOAD_SIZE:
            raise HTTPException(status_code=413, detail="FILE_TOO_LARGE")

        # ⚠️ 保留原名、不加时间戳前缀：客户端用的是系统级 uploadFile，
        #    **它不返回响应体** —— 客户端拿不到这里 JSON 里的 url，
        #    只能靠"服务端用原名存储"自己拼出可预测的 URL。
        #    代价是同名覆盖，对"发一张图/文件给助手看"这个场景可以接受。
        name = _safe_upload_name(file.filename or "upload.bin")
        target = UPLOAD_DIR / name
        target.write_bytes(data)

        ctype = file.content_type or "application/octet-stream"
        print(f"[Upload] {name} ({len(data)} bytes, {ctype})", flush=True)
        return {
            "status": "ok",
            "name": name,
            "size": len(data),
            "content_type": ctype,
            "url": f"/api/upload/{name}"
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Upload error: {e}")


# ─────────────────── 图片标记 → 原生视觉内容块 ───────────────────
#
# 背景：APP 的「图片」菜单走系统 uploadFile，服务端确认过它**拿不到响应体**，
# 所以客户端只能把 `[图片] /api/upload/<name>` 当纯文本塞进输入框发过来。
# 模型收到的是一行字符串，看不到图。这里在转发给 Gateway 之前把它还原成
# 真正的图片块，用上 DeepSeek V4.1 Flash 的原生视觉。
#
# ⚠️ 必须 base64 内联，不能直接把 /api/upload/xxx 的 URL 传给模型 ——
#    DeepSeek 是**它自己的服务器**去下载你给的外部链接，而那个地址是内网的
#    （192.168.3.87:8866），它够不着。
# ⚠️ 只处理**最后一条** user 消息。历史里的图不回头重编码：那会让每轮请求
#    都背上几十 MB 的历史图片，而且会改动缓存前缀。
# ⚠️ 单图有大小上限，见 MAX_INLINE_IMAGE_BYTES。

_IMG_MARKER_RE = re.compile(r"\[图片\]\s*/?api/upload/([A-Za-z0-9._-]+)")
_MIME_BY_EXT = {
    ".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".png": "image/png",
    ".gif": "image/gif", ".webp": "image/webp", ".bmp": "image/bmp",
}
# base64 后体积约 ×1.37。8MB 原图 → 约 11MB 字符串，给 WS 消息和 DeepSeek
# 48MiB 请求体都留了余量。调大前先想清楚内存压力。
MAX_INLINE_IMAGE_BYTES = 8 * 1024 * 1024


def _inline_image_markers(messages):
    """把最后一条 user 消息里的 `[图片] /api/upload/<name>` 换成图片块。

    返回 (新 messages, 内联成功的张数)。没有标记时原样返回，零开销。

    内容块用 OpenAI 原生 vision 格式（content 从字符串变成
    [{"type":"text"},{"type":"image_url"}] 数组），Gateway 的
    _normalize_multimodal_content 直接认，agent 内核也原样透传给
    OpenAI-wire provider。
    """
    if not isinstance(messages, list) or not messages:
        return messages, 0

    idx = -1
    for i in range(len(messages) - 1, -1, -1):
        m = messages[i]
        if isinstance(m, dict) and m.get("role") == "user":
            idx = i
            break
    if idx < 0:
        return messages, 0

    msg = messages[idx]
    content = msg.get("content")
    if not isinstance(content, str) or "[图片]" not in content:
        return messages, 0

    uploads_root = str(UPLOAD_DIR.resolve())
    blocks = []
    inlined = 0
    pos = 0

    for mt in _IMG_MARKER_RE.finditer(content):
        head = content[pos:mt.start()].strip()
        if head:
            blocks.append({"type": "text", "text": head})
        pos = mt.end()

        name = _safe_upload_name(mt.group(1))
        path = (UPLOAD_DIR / name).resolve()
        # 复核没逃出 uploads 目录（name 已被清洗，这是第二道防线）
        if not str(path).startswith(uploads_root):
            blocks.append({"type": "text", "text": f"[图片路径非法: {name}]"})
            continue

        mime = _MIME_BY_EXT.get(path.suffix.lower())
        if mime is None:
            blocks.append({"type": "text",
                           "text": f"[非图片格式，未内联: {name}]"})
            continue

        try:
            data = path.read_bytes()
        except OSError as e:
            print(f"[Vision] 读图失败 {name}: {e}", flush=True)
            blocks.append({"type": "text", "text": f"[图片读取失败: {name}]"})
            continue

        if len(data) > MAX_INLINE_IMAGE_BYTES:
            mb = len(data) / 1024 / 1024
            print(f"[Vision] {name} 过大({mb:.1f}MB)，未内联", flush=True)
            blocks.append({"type": "text",
                           "text": f"[图片过大未内联: {name}，{mb:.1f}MB]"})
            continue

        b64 = base64.b64encode(data).decode("ascii")
        blocks.append({"type": "image_url",
                       "image_url": {"url": f"data:{mime};base64,{b64}"}})
        inlined += 1
        print(f"[Vision] 内联 {name} ({len(data)}B, {mime})", flush=True)

    tail = content[pos:].strip()
    if tail:
        blocks.append({"type": "text", "text": tail})

    if inlined == 0:
        # 一张都没成 → 保持纯文本原样。用户至少能在回复里看到上面的报错文字。
        return messages, 0

    out = list(messages)
    nm = dict(msg)
    nm["content"] = blocks
    out[idx] = nm
    return out, inlined


@app.get("/api/upload/{name}")
def get_upload(name: str):
    """按名取回上传的文件（客户端预览 / 多模态消息引用）。"""
    safe = _safe_upload_name(name)
    target = (UPLOAD_DIR / safe).resolve()
    # 复核：解析后必须仍在 uploads 目录内
    if not str(target).startswith(str(UPLOAD_DIR.resolve())):
        raise HTTPException(status_code=403, detail="Access denied")
    if not target.exists() or not target.is_file():
        raise HTTPException(status_code=404, detail="UPLOAD_NOT_FOUND")
    return FileResponse(str(target))


@app.get("/api/files")
def list_files(request: Request, path: str = Query("", description="Obsidian 内相对路径")):
    """列出目录内容（Obsidian 只读浏览）"""
    check_auth(request)

    full_path = (OBSIDIAN_VAULT / path).resolve()

    # 安全检查：禁止逃逸出 vault
    if not str(full_path).startswith(str(OBSIDIAN_VAULT.resolve())):
        raise HTTPException(status_code=403, detail="Access denied: path traversal")

    if not full_path.exists():
        raise HTTPException(status_code=404, detail=f"Path not found: {path}")

    if full_path.is_file():
        stat = full_path.stat()
        return {
            "type": "file",
            "name": full_path.name,
            "path": path,
            "size": stat.st_size,
            "modified": datetime.fromtimestamp(stat.st_mtime, tz=TZ).isoformat(),
        }

    # 目录 → 列出内容
    items = []
    try:
        for entry in sorted(full_path.iterdir(), key=lambda x: (not x.is_dir(), x.name.lower())):
            if entry.name.startswith("."):
                continue
            if entry.is_dir():
                items.append({
                    "type": "directory",
                    "name": entry.name,
                    "path": str(entry.relative_to(OBSIDIAN_VAULT)),
                })
            elif entry.suffix.lower() in ALLOWED_EXTENSIONS:
                stat = entry.stat()
                items.append({
                    "type": "file",
                    "name": entry.name,
                    "path": str(entry.relative_to(OBSIDIAN_VAULT)),
                    "size": stat.st_size,
                    "modified": datetime.fromtimestamp(stat.st_mtime, tz=TZ).isoformat(),
                })
    except PermissionError:
        raise HTTPException(status_code=403, detail="Permission denied")

    rel = str(full_path.relative_to(OBSIDIAN_VAULT)) if full_path != OBSIDIAN_VAULT else ""
    return {
        "path": rel,
        "items": items,
    }


# ─────────────────── S17：md → HTML 渲染（带 mtime 缓存）───────────────────

# 缓存：{相对路径: (mtime, size, html)}。
# key 里带 mtime + size —— Vault 是活的（每天新增/编辑笔记），
# 用「路径」单键会让改过的文件一直吐旧 HTML；带 mtime 则自动失效，无需任何同步机制。
_MD_HTML_CACHE: dict = {}
_MD_CACHE_MAX = 300          # 上限，防长跑进程内存无限增长（565 个文件，够用）

# 阅读优化 CSS。
# ⚠️ 设计要点：
#   - 字号 16px / 行高 1.8：手机长文阅读的舒适区间（Vault 文档动辄几千字）
#   - 表格 display:block + overflow-x:auto：**这是选 WebView 方案的主要原因**
#     —— Vault 里 76% 的文档有表格，宽表必须能横向滑动而不是被压扁
#   - user-select: text + -webkit-user-select: text：**显式开启文本选择**
#     （部分 WebView 默认行为不一致，必须写死，否则用户长按选不中文字）
#   - 代码块横向滚动、引用块左边条、图片 max-width:100% 防溢出
_MD_CSS = """
*{box-sizing:border-box;-webkit-user-select:text;user-select:text;-webkit-touch-callout:default}
html,body{margin:0;padding:0}
body{font-family:-apple-system,'HarmonyOS Sans',sans-serif;font-size:16px;line-height:1.8;
     color:#1a1a2e;background:#ffffff;padding:14px 16px 40px;word-wrap:break-word;overflow-wrap:break-word}
p{margin:0 0 14px}
h1,h2,h3,h4,h5,h6{line-height:1.4;margin:24px 0 12px;font-weight:600}
h1{font-size:23px;border-bottom:2px solid #eee;padding-bottom:8px}
h2{font-size:20px;border-bottom:1px solid #f0f0f0;padding-bottom:6px}
h3{font-size:18px}
h4{font-size:16.5px}
h5,h6{font-size:16px;color:#555}
ul,ol{margin:0 0 14px;padding-left:24px}
li{margin:5px 0}
li>ul,li>ol{margin:5px 0}
a{color:#5B5BD6;text-decoration:none}
strong{font-weight:600}
hr{border:none;border-top:1px solid #eee;margin:20px 0}
blockquote{border-left:3px solid #d0d0dd;margin:14px 0;padding:6px 0 6px 14px;color:#5a5a6e;background:#fafafd}
code{background:#f0f0f5;padding:2px 5px;border-radius:4px;font-size:14px;
     font-family:'SF Mono',Consolas,monospace}
pre{background:#f6f8fa;padding:12px 14px;border-radius:8px;overflow-x:auto;margin:0 0 14px;
    border:1px solid #ececf2}
pre code{background:none;padding:0;font-size:13.5px;line-height:1.6}
table{border-collapse:collapse;display:block;overflow-x:auto;max-width:100%;margin:14px 0;
      font-size:14px;-webkit-overflow-scrolling:touch}
th,td{border:1px solid #e2e2ea;padding:7px 11px;text-align:left;white-space:nowrap}
th{background:#f5f5fa;font-weight:600}
tbody tr:nth-child(even){background:#fafafd}
img{max-width:100%;height:auto;border-radius:6px}
/* S18 frontmatter 信息卡 */
.fm-card{background:#f7f8fc;border:1px solid #e8e8f0;border-radius:10px;
         padding:12px 14px;margin:0 0 18px}
.fm-title{font-size:17px;font-weight:600;line-height:1.45;color:#1a1a2e}
.fm-meta{font-size:12.5px;color:#8a8a9a;margin-top:6px}
.fm-tags{margin-top:9px}
.fm-tag{display:inline-block;font-size:11.5px;color:#5B5BD6;background:#ececfb;
        border-radius:10px;padding:2px 9px;margin:0 6px 6px 0}
"""


# frontmatter 里要展示的字段（顺序即卡片上的顺序）
_FM_SHOW = ("title", "date", "issue", "tags", "source", "status")
# 明确隐藏的内部字段（长 ID / 外链，对阅读无价值）
_FM_HIDE = ("wechat_media_id", "zhihu_url", "media_id", "url", "id")


def _split_frontmatter(text: str):
    """拆出 (frontmatter_dict, 正文)。没有 frontmatter 或解析失败 -> ({}, 原文)。"""
    if not text.startswith("---"):
        return {}, text
    end = text.find("\n---", 3)
    if end < 0:
        return {}, text
    raw = text[3:end].strip()
    body = text[end + 4:].lstrip("\n")
    fm = {}
    for line in raw.split("\n"):
        if ":" not in line:
            continue
        k, v = line.split(":", 1)
        k = k.strip().lower()
        v = v.strip().strip('"').strip("'")
        if k and v:
            fm[k] = v
    return fm, body


def _frontmatter_card(fm: dict) -> str:
    """把 frontmatter 渲染成信息卡。无可用字段时返回空串（不产生空卡片）。"""
    if not fm:
        return ""

    import html as _h
    rows = []

    title = fm.get("title", "")
    if title:
        rows.append('<div class="fm-title">' + _h.escape(title) + '</div>')

    meta = []
    if fm.get("date"):
        meta.append(_h.escape(fm["date"]))
    if fm.get("issue"):
        meta.append("第 " + _h.escape(str(fm["issue"])) + " 期")
    if fm.get("source"):
        meta.append(_h.escape(fm["source"]))
    if fm.get("status"):
        meta.append(_h.escape(fm["status"]))
    if meta:
        rows.append('<div class="fm-meta">' + ' · '.join(meta) + '</div>')

    tags = fm.get("tags", "")
    if tags:
        # [a, b, c] -> 小胶囊
        t = tags.strip().strip("[]")
        chips = []
        for x in t.split(","):
            x = x.strip().strip('"').strip("'")
            if x:
                chips.append('<span class="fm-tag">' + _h.escape(x) + '</span>')
        if chips:
            rows.append('<div class="fm-tags">' + ''.join(chips) + '</div>')

    if not rows:
        return ""
    return '<div class="fm-card">' + ''.join(rows) + '</div>'


def _render_md_html(rel_path: str, full_path) -> str:
    """md → 完整 HTML 文档（含内联 CSS）。带 mtime+size 缓存。"""
    st = full_path.stat()
    key = rel_path
    hit = _MD_HTML_CACHE.get(key)
    if hit is not None and hit[0] == st.st_mtime and hit[1] == st.st_size:
        return hit[2]

    text = full_path.read_text(encoding="utf-8", errors="replace")
    # S18：frontmatter 单独摘出来做成信息卡，不混进正文（否则一堆 key: value 很吵）
    fm, md_body = _split_frontmatter(text)
    card = _frontmatter_card(fm)
    try:
        import markdown_it
        md = markdown_it.MarkdownIt("commonmark", {"html": True})
        md.enable("table")          # Vault 大量使用 GFM 表格，默认 commonmark 不认
        body = card + md.render(md_body)
    except Exception as e:          # 渲染失败不能让整个阅读功能挂掉 → 退回纯文本
        print(f"[MD] render failed, fallback to plain text: {e}", flush=True)
        import html as _h
        body = "<pre>" + _h.escape(text) + "</pre>"

    html = ("<!DOCTYPE html><html><head><meta charset='utf-8'>"
            "<meta name='viewport' content='width=device-width,initial-scale=1,maximum-scale=3'>"
            "<style>" + _MD_CSS + "</style></head><body>" + body + "</body></html>")

    if len(_MD_HTML_CACHE) >= _MD_CACHE_MAX:
        _MD_HTML_CACHE.clear()      # 简单粗暴但够用：Vault 才 565 个文件
    _MD_HTML_CACHE[key] = (st.st_mtime, st.st_size, html)
    return html


@app.get("/api/files/read")
def read_file(
    request: Request,
    path: str = Query(..., description="文件相对路径"),
    render: str = Query("text", description="text=原文 / html=md 渲染后的 HTML"),
):
    """读取文件内容（仅文本类文件）。render=html 时把 .md 渲染成适合手机阅读的 HTML。"""
    check_auth(request)

    full_path = (OBSIDIAN_VAULT / path).resolve()

    if not str(full_path).startswith(str(OBSIDIAN_VAULT.resolve())):
        raise HTTPException(status_code=403, detail="Access denied: path traversal")

    if not full_path.exists():
        raise HTTPException(status_code=404, detail=f"File not found: {path}")

    if not full_path.is_file():
        raise HTTPException(status_code=400, detail="Not a file")

    if full_path.stat().st_size > MAX_FILE_SIZE:
        raise HTTPException(status_code=413, detail="File too large (>5MB)")

    ext = full_path.suffix.lower()
    if ext not in READABLE_EXTENSIONS:
        raise HTTPException(status_code=415, detail=f"Unsupported file type: {ext}")

    # render=html：把 md 渲染成带阅读优化 CSS 的 HTML（客户端 WebView 直接加载）
    if render == "html" and ext == ".md":
        # 同时带上原文 content：客户端「复制全文」需要纯文本，
        # 让它为此再发一次请求不划算（而且那一次还得重新读盘）。
        return {
            "path": path,
            "name": full_path.name,
            "size": full_path.stat().st_size,
            "html": _render_md_html(path, full_path),
            "content": full_path.read_text(encoding="utf-8", errors="replace"),
            "rendered": True,
        }

    content = full_path.read_text(encoding="utf-8", errors="replace")
    return {
        "path": path,
        "name": full_path.name,
        "size": full_path.stat().st_size,
        "content": content,
        "rendered": False,
    }


@app.get("/api/files/search")
def search_files(request: Request, q: str = Query(..., min_length=1, description="搜索关键词")):
    """简单搜索 vault 中的 .md 文件（按文件名匹配）"""
    check_auth(request)

    q_lower = q.lower()
    results = []
    for md_file in OBSIDIAN_VAULT.rglob("*.md"):
        if md_file.name.startswith("."):
            continue
        if q_lower in md_file.name.lower():
            stat = md_file.stat()
            results.append({
                "type": "file",
                "name": md_file.name,
                "path": str(md_file.relative_to(OBSIDIAN_VAULT)),
                "size": stat.st_size,
                "modified": datetime.fromtimestamp(stat.st_mtime, tz=TZ).isoformat(),
            })

    return {"query": q, "results": results[:50]}


# ══════════════════════════════════════════════════════════════════════
#  Token 统计 API
# ══════════════════════════════════════════════════════════════════════

def _get_db() -> sqlite3.Connection:
    """连接 state.db（只读）"""
    db = sqlite3.connect(f"file:{STATE_DB}?mode=ro", uri=True)
    db.row_factory = sqlite3.Row
    return db





_token_snapshot_tmp: list = []


PUSH_DB = Path(os.path.expanduser("~/.hermes/push_messages.db"))


def _token_dbs() -> list:
    """HOTFIX 20261007: all state.db files for token stats. Desktop added."""
    paths = [STATE_DB]
    win_db = Path(os.getenv("WIN_STATE_DB", r"C:\Users\HW\AppData\Local\hermes\state.db"))
    if win_db.exists():
        try:
            # Windows Desktop 库被其进程锁定, 直连会 disk I/O error -> 拷贝只读快照
            # 1.35GB 经 9p 拷贝约 17s, 每次请求都拷会拖垮接口 -> 快照复用(10 分钟内)
            import shutil, tempfile, time as _time
            tmp = Path(tempfile.gettempdir()) / "win_state_token_snapshot.db"
            stale = _time.time() - tmp.stat().st_mtime > 600 if tmp.exists() else True
            if stale:
                tmp_unlink = tmp.with_suffix(".copying")
                shutil.copyfile(win_db, tmp_unlink)
                tmp_unlink.replace(tmp)
            paths.append(tmp)
            _token_snapshot_tmp.append(tmp)
        except Exception:
            pass
    out = []
    for pp in paths:
        try:
            if pp.exists():
                conn = sqlite3.connect("file:%s?mode=ro" % pp, uri=True)
                conn.row_factory = sqlite3.Row
                out.append(conn)
        except Exception:
            continue
    return out


def _token_close(dbs) -> None:
    for db in dbs:
        try:
            db.close()
        except Exception:
            pass

@app.get("/api/tokens/daily")
def tokens_daily(request: Request, days: int = Query(7, ge=1, le=90, description="最近 N 天")):
    """每日 token 消耗（最近 N 天）。

    HOTFIX 20261007 口径修正:
      - 数据源 sessions 表 -> session_model_usage（含 cache_read/cache_write/reasoning）
      - 双库汇总: WSL Gateway state.db + Windows Desktop state.db
      - total_tokens = input + output + cache_read + cache_write + reasoning（模型真实消耗）
    """
    check_auth(request)
    try:
        daily = {}
        totals = {"input": 0, "output": 0, "extra": 0}
        dbs = _token_dbs()
        for db in dbs:
            rows = db.execute(
                "SELECT date(s.started_at, 'unixepoch', '+8 hours') as day,"
                " COALESCE(SUM(u.input_tokens), 0) as inp,"
                " COALESCE(SUM(u.output_tokens), 0) as outp,"
                " COALESCE(SUM(u.cache_read_tokens), 0) as cr,"
                " COALESCE(SUM(u.cache_write_tokens), 0) as cw,"
                " COALESCE(SUM(u.reasoning_tokens), 0) as rt,"
                " COUNT(DISTINCT u.session_id) as session_count"
                " FROM session_model_usage u JOIN sessions s ON s.id = u.session_id"
                " WHERE s.started_at > unixepoch('now', ?)"
                " GROUP BY day ORDER BY day DESC",
                ["-%d days" % days],
            ).fetchall()
            for row in rows:
                d = daily.setdefault(row["day"], {"in": 0, "out": 0, "extra": 0, "sessions": 0})
                d["in"] += row["inp"]; d["out"] += row["outp"]
                d["extra"] += row["cr"] + row["cw"] + row["rt"]
                d["sessions"] += row["session_count"]
                totals["input"] += row["inp"]; totals["output"] += row["outp"]
                totals["extra"] += row["cr"] + row["cw"] + row["rt"]
        _token_close(dbs)
        daily_list = []
        for day in sorted(daily.keys(), reverse=True):
            d = daily[day]
            daily_list.append({
                "date": day,
                "input_tokens": d["in"],
                "output_tokens": d["out"],
                "cache_tokens": d["extra"],
                "total_tokens": d["in"] + d["out"] + d["extra"],
                "sessions": d["sessions"],
            })
        return {
            "period": "last_%d_days" % days,
            "total_input_tokens": totals["input"],
            "total_output_tokens": totals["output"],
            "total_tokens": totals["input"] + totals["output"] + totals["extra"],
            "daily": daily_list,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")


@app.get("/api/tokens/monthly")
def tokens_monthly(request: Request, months: int = Query(6, ge=1, le=24, description="最近 N 月")):
    """每月 token 消耗（最近 N 月）——口径同 daily（HOTFIX 20261007）。"""
    check_auth(request)
    try:
        monthly = {}
        dbs = _token_dbs()
        for db in dbs:
            rows = db.execute(
                "SELECT strftime('%Y-%m', s.started_at, 'unixepoch', '+8 hours') as month,"
                " COALESCE(SUM(u.input_tokens), 0) as inp,"
                " COALESCE(SUM(u.output_tokens), 0) as outp,"
                " COALESCE(SUM(u.cache_read_tokens), 0) as cr,"
                " COALESCE(SUM(u.cache_write_tokens), 0) as cw,"
                " COALESCE(SUM(u.reasoning_tokens), 0) as rt,"
                " COUNT(DISTINCT u.session_id) as session_count"
                " FROM session_model_usage u JOIN sessions s ON s.id = u.session_id"
                " WHERE s.started_at > unixepoch('now', ?)"
                " GROUP BY month ORDER BY month DESC",
                ["-%d months" % months],
            ).fetchall()
            for row in rows:
                mth = monthly.setdefault(row["month"], {"in": 0, "out": 0, "extra": 0, "sessions": 0})
                mth["in"] += row["inp"]; mth["out"] += row["outp"]
                mth["extra"] += row["cr"] + row["cw"] + row["rt"]
                mth["sessions"] += row["session_count"]
        _token_close(dbs)
        monthly_list = []
        for month in sorted(monthly.keys(), reverse=True):
            m = monthly[month]
            monthly_list.append({
                "month": month,
                "input_tokens": m["in"],
                "output_tokens": m["out"],
                "total_tokens": m["in"] + m["out"] + m["extra"],
                "sessions": m["sessions"],
                "active_days": 0,
            })
        return {"period": "last_%d_months" % months, "monthly": monthly_list}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")


@app.get("/api/tokens/summary")
def tokens_summary(request: Request):
    """Token 总览：今日/本月/总计 + 模型分布（HOTFIX 20261007 双库全口径）。"""
    check_auth(request)
    try:
        today_str = datetime.now(TZ).strftime("%Y-%m-%d")
        this_month_str = datetime.now(TZ).strftime("%Y-%m")

        today = {"in": 0, "out": 0, "extra": 0, "sessions": 0}
        month = {"in": 0, "out": 0, "extra": 0, "sessions": 0, "days": 0}
        total = {"in": 0, "out": 0, "extra": 0, "sessions": 0}
        models = {}

        dbs = _token_dbs()
        for db in dbs:
            r = db.execute(
                "SELECT COALESCE(SUM(u.input_tokens),0) inp, COALESCE(SUM(u.output_tokens),0) outp,"
                " COALESCE(SUM(u.cache_read_tokens),0) cr, COALESCE(SUM(u.cache_write_tokens),0) cw,"
                " COALESCE(SUM(u.reasoning_tokens),0) rt, COUNT(DISTINCT u.session_id) c"
                " FROM session_model_usage u JOIN sessions s ON s.id = u.session_id"
                " WHERE date(s.started_at,'unixepoch','+8 hours') = ?",
                [today_str],
            ).fetchone()
            today["in"] += r["inp"]; today["out"] += r["outp"]
            today["extra"] += r["cr"] + r["cw"] + r["rt"]; today["sessions"] += r["c"]

            r = db.execute(
                "SELECT COALESCE(SUM(u.input_tokens),0) inp, COALESCE(SUM(u.output_tokens),0) outp,"
                " COALESCE(SUM(u.cache_read_tokens),0) cr, COALESCE(SUM(u.cache_write_tokens),0) cw,"
                " COALESCE(SUM(u.reasoning_tokens),0) rt, COUNT(DISTINCT u.session_id) c,"
                " COUNT(DISTINCT date(s.started_at,'unixepoch','+8 hours')) d"
                " FROM session_model_usage u JOIN sessions s ON s.id = u.session_id"
                " WHERE strftime('%Y-%m', s.started_at,'unixepoch','+8 hours') = ?",
                [this_month_str],
            ).fetchone()
            month["in"] += r["inp"]; month["out"] += r["outp"]
            month["extra"] += r["cr"] + r["cw"] + r["rt"]
            month["sessions"] += r["c"]; month["days"] += r["d"]

            r = db.execute(
                "SELECT COALESCE(SUM(u.input_tokens),0) inp, COALESCE(SUM(u.output_tokens),0) outp,"
                " COALESCE(SUM(u.cache_read_tokens),0) cr, COALESCE(SUM(u.cache_write_tokens),0) cw,"
                " COALESCE(SUM(u.reasoning_tokens),0) rt, COUNT(DISTINCT u.session_id) c"
                " FROM session_model_usage u"
            ).fetchone()
            total["in"] += r["inp"]; total["out"] += r["outp"]
            total["extra"] += r["cr"] + r["cw"] + r["rt"]; total["sessions"] += r["c"]

            for row in db.execute(
                "SELECT u.model,"
                " COALESCE(SUM(u.input_tokens),0) inp, COALESCE(SUM(u.output_tokens),0) outp,"
                " COALESCE(SUM(u.cache_read_tokens),0) cr, COALESCE(SUM(u.cache_write_tokens),0) cw,"
                " COALESCE(SUM(u.reasoning_tokens),0) rt,"
                " COUNT(DISTINCT u.session_id) c"
                " FROM session_model_usage u WHERE u.model IS NOT NULL"
                " GROUP BY u.model"
            ).fetchall():
                mm = models.setdefault(row["model"], {"in": 0, "out": 0, "extra": 0, "c": 0})
                mm["in"] += row["inp"]; mm["out"] += row["outp"]
                mm["extra"] += row["cr"] + row["cw"] + row["rt"]; mm["c"] += row["c"]
        _token_close(dbs)

        ad = max(1, month["days"])
        daily_avg = (month["in"] + month["out"] + month["extra"]) / ad

        by_model = sorted(
            [{"model": k, "input_tokens": v["in"], "output_tokens": v["out"],
              "total_tokens": v["in"] + v["out"] + v["extra"], "sessions": v["c"]}
             for k, v in models.items()],
            key=lambda x: x["total_tokens"], reverse=True)[:10]

        return {
            "today": {
                "input_tokens": today["in"], "output_tokens": today["out"],
                "cache_tokens": today["extra"],
                "total_tokens": today["in"] + today["out"] + today["extra"],
            },
            "this_month": {
                "input_tokens": month["in"], "output_tokens": month["out"],
                "total_tokens": month["in"] + month["out"] + month["extra"],
                "sessions": month["sessions"],
                "active_days": ad,
                "daily_average": int(daily_avg),
                "projected_total": int(daily_avg * 30),
            },
            "all_time": {
                "input_tokens": total["in"], "output_tokens": total["out"],
                "total_tokens": total["in"] + total["out"] + total["extra"],
                "sessions": total["sessions"],
            },
            "by_model": by_model,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")



def _init_push_db():
    """Ensure push DB exists with correct schema."""
    db = sqlite3.connect(str(PUSH_DB))
    db.execute("""
        CREATE TABLE IF NOT EXISTS push_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL DEFAULT '',
            body TEXT NOT NULL,
            source TEXT DEFAULT 'intel',
            created_at TEXT NOT NULL DEFAULT (datetime('now', '+8 hours')),
            read_at TEXT
        )
    """)
    db.execute("""
        CREATE TABLE IF NOT EXISTS offline_chats (
            session_id TEXT PRIMARY KEY,
            content TEXT NOT NULL,
            updated_at TEXT NOT NULL DEFAULT (datetime('now', '+8 hours'))
        )
    """)
    db.commit()
    db.close()

_init_push_db()


class PushEventManager:
    """Simple SSE broadcaster for live push delivery."""
    def __init__(self):
        self._subscribers: list[asyncio.Queue] = []

    def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue()
        self._subscribers.append(q)
        return q

    def unsubscribe(self, q: asyncio.Queue):
        if q in self._subscribers:
            self._subscribers.remove(q)

    async def broadcast(self, message: dict):
        payload = json.dumps(message, ensure_ascii=False)
        for q in self._subscribers[:]:
            try:
                q.put_nowait(payload)
            except asyncio.QueueFull:
                pass

push_events = PushEventManager()


@app.post("/api/push")
async def push_message(request: Request, background_tasks: BackgroundTasks):
    """Receive a push message (from cron jobs, Hermes, etc.)"""
    body = await request.json()
    title = body.get("title", "")
    text = body.get("body", body.get("text", ""))
    source = body.get("source", "intel")

    if not text:
        raise HTTPException(status_code=400, detail="body or text is required")

    # 追加发送时间到消息体末尾
    now_str = datetime.now(TZ).strftime("%m-%d %H:%M")
    text_with_time = f"{text}\n—— {now_str}"

    db = sqlite3.connect(str(PUSH_DB))
    cur = db.execute(
        "INSERT INTO push_messages (title, body, source) VALUES (?, ?, ?)",
        (title, text_with_time, source),
    )
    msg_id = cur.lastrowid
    db.commit()

    row = db.execute("SELECT * FROM push_messages WHERE id = ?", (msg_id,)).fetchone()
    db.close()

    message = {
        "id": row[0],
        "title": row[1],
        "body": row[2],
        "source": row[3],
        "created_at": row[4],
        "read_at": row[5],
    }

    # 同步执行推送和广播
    try:
        await _push_to_huawei(title, text_with_time, source)
    except Exception as e:
        print(f"[PushKit] Error: {e}")
    # 写入离线对话缓存（APP sync_pending 可拉取，显示在对话列表中）
    try:
        db2 = sqlite3.connect(str(PUSH_DB))
        existing = db2.execute(
            "SELECT content FROM offline_chats WHERE session_id = '__push_inbox__'"
        ).fetchone()
        new_entry = f"[{title}]\n{text_with_time}\n---\n"
        if existing:
            new_entry = existing[0] + "\n" + new_entry
        db2.execute(
            "INSERT OR REPLACE INTO offline_chats (session_id, content) VALUES ('__push_inbox__', ?)",
            (new_entry,)
        )
        db2.commit()
        db2.close()
    except Exception as e:
        print(f"[PushKit] offline_chats write error: {e}")
    await push_events.broadcast(message)
    await manager.broadcast({
        "type": "notification", "title": title, "body": text_with_time, "source": source
    })

    return {"status": "ok", "message": message}


# ══════════════════════════════════════════════════════════════════════
#  Cron 任务面板 API（S10a）
#
#  直接复用 Hermes 自己的 cron.jobs / cron.scheduler：
#    - 锁与原子写由 cron.jobs 内部处理，不需要在这里重造并发保护
#    - 不走 `hermes cron edit`（那条路会把 next_run_at 往后推一整个周期）
#    - 手动执行与 scheduler tick 共用 run_one_job 路径，行为一致
# ══════════════════════════════════════════════════════════════════════

def _cron_jobs_module():
    """延迟导入 Hermes 的 cron.jobs（进程启动时就 import 会拖慢并可能受 PATH 影响）"""
    import sys as _sys
    hh = str(Path(os.path.expanduser("~/.hermes/hermes-agent")))
    if hh not in _sys.path:
        _sys.path.insert(0, hh)
    import cron.jobs as _jobs
    return _jobs


def _cron_scheduler_module():
    import sys as _sys
    hh = str(Path(os.path.expanduser("~/.hermes/hermes-agent")))
    if hh not in _sys.path:
        _sys.path.insert(0, hh)
    import cron.scheduler as _sched
    return _sched


def _cron_job_view(job: dict) -> dict:
    """把内部 job 记录裁剪成 APP 需要的字段（不暴露 prompt/密钥等）"""
    sched = job.get("schedule") or {}
    display = job.get("schedule_display") or sched.get("display") or sched.get("expr") or "?"
    return {
        "id": job.get("id"),
        "name": job.get("name") or "",
        "schedule": display,
        "enabled": bool(job.get("enabled", True)),
        "state": job.get("state") or ("scheduled" if job.get("enabled", True) else "paused"),
        "next_run_at": job.get("next_run_at"),
        "last_run_at": job.get("last_run_at"),
        "last_status": job.get("last_status"),
        "last_error": job.get("last_error"),
        "deliver": job.get("deliver") or "local",
        "no_agent": bool(job.get("no_agent")),
        "script": job.get("script") or "",
    }


@app.get("/api/cron/jobs")
def cron_list_jobs(request: Request):
    """列出全部定时任务（含已暂停的）"""
    check_auth(request)
    try:
        jobs_mod = _cron_jobs_module()
        raw = jobs_mod.list_jobs(include_disabled=True)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Cron read error: {e}")

    items = [_cron_job_view(j) for j in raw]
    # 按下次执行时间排序（None 排最后）
    items.sort(key=lambda x: x.get("next_run_at") or "9999")
    return {"items": items, "total": len(items), "has_more": False}


@app.post("/api/cron/jobs/{job_id}/toggle")
async def cron_toggle_job(job_id: str, request: Request):
    """启用/暂停任务。幂等：重复设同值返回当前状态，不报错。"""
    check_auth(request)
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    if "enabled" not in body:
        raise HTTPException(status_code=400, detail="Field 'enabled' is required")
    target = bool(body.get("enabled"))

    try:
        jobs_mod = _cron_jobs_module()
        job = jobs_mod.get_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="JOB_NOT_FOUND")

        # 幂等：已是目标状态就直接返回，不做写操作
        if bool(job.get("enabled", True)) == target:
            return {"id": job_id, "enabled": target, "changed": False}

        updated = jobs_mod.resume_job(job_id) if target else jobs_mod.pause_job(job_id)
        if updated is None:
            raise HTTPException(status_code=404, detail="JOB_NOT_FOUND")
        return {
            "id": job_id,
            "enabled": bool(updated.get("enabled", target)),
            "changed": True,
            "next_run_at": updated.get("next_run_at"),
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Cron toggle error: {e}")


@app.post("/api/cron/jobs/{job_id}/run")
def cron_run_job(job_id: str, request: Request):
    """立即执行一次。任务已在跑 -> 409 JOB_ALREADY_RUNNING。"""
    check_auth(request)
    try:
        jobs_mod = _cron_jobs_module()
        job = jobs_mod.get_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="JOB_NOT_FOUND")

        # ⚠️ 先单独判「暂停」：claim_job_for_fire 对 disabled/paused 的 job
        #    同样返回 False（cron/jobs.py 内部会先拒），若不区分就会把
        #    「已暂停」误报成「正在运行」。两者是完全不同的用户处境。
        if not job.get("enabled", True) or job.get("state") == "paused":
            raise HTTPException(status_code=409, detail="JOB_PAUSED")

        # at-most-once：抢不到 claim 说明 scheduler（或另一次手动触发）正在跑它
        if not jobs_mod.claim_job_for_fire(job_id):
            raise HTTPException(status_code=409, detail="JOB_ALREADY_RUNNING")

        sched = _cron_scheduler_module()
        # 执行可能很久（跑脚本/起 agent），丢后台线程，接口立刻返回
        def _fire():
            try:
                sched.run_one_job(job)
            except Exception as _e:
                print(f"[Cron] manual run failed for {job_id}: {_e}", flush=True)

        threading.Thread(target=_fire, daemon=True).start()
        return {"status": "triggered", "run_id": job_id}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Cron run error: {e}")


# ── Cron 详情 / 历史 / 编辑 ──────────────────────────────────────────

# 输出文件名白名单（防路径穿越）
_OUTPUT_NAME_RE = re.compile(r"^[A-Za-z0-9_.\-]{1,120}$")


def _cron_output_dir(job_id: str) -> Path:
    return Path(os.path.expanduser("~/.hermes/cron/output")) / job_id


@app.get("/api/cron/jobs/{job_id}")
def cron_job_detail(job_id: str, request: Request):
    """单个任务详情。含 prompt 全文（用户自己的数据）与完整 last_error。"""
    check_auth(request)
    try:
        jobs_mod = _cron_jobs_module()
        job = jobs_mod.get_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="JOB_NOT_FOUND")

        view = _cron_job_view(job)
        sched = job.get("schedule") or {}
        repeat = job.get("repeat") or {}

        # 详情页才给的字段
        view["prompt"] = job.get("prompt") or ""
        view["schedule_kind"] = sched.get("kind") or ""
        view["schedule_expr"] = sched.get("expr") or ""
        view["repeat_times"] = repeat.get("times")
        view["repeat_completed"] = repeat.get("completed")
        view["created_at"] = job.get("created_at")
        view["paused_at"] = job.get("paused_at")
        view["paused_reason"] = job.get("paused_reason")
        view["model"] = job.get("model")
        view["provider"] = job.get("provider")
        view["workdir"] = job.get("workdir")
        view["skills"] = job.get("skills") or []
        # 输出目录里有多少次历史
        out_dir = _cron_output_dir(job_id)
        runs = 0
        if out_dir.is_dir():
            try:
                runs = len([f for f in out_dir.iterdir() if f.is_file()])
            except Exception:
                runs = 0
        view["run_history_count"] = runs
        return view
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Cron detail error: {e}")


@app.get("/api/cron/jobs/{job_id}/outputs")
def cron_job_outputs(job_id: str, request: Request):
    """该任务的运行输出列表（新→旧）"""
    check_auth(request)
    try:
        jobs_mod = _cron_jobs_module()
        if jobs_mod.get_job(job_id) is None:
            raise HTTPException(status_code=404, detail="JOB_NOT_FOUND")

        out_dir = _cron_output_dir(job_id)
        items = []
        if out_dir.is_dir():
            for f in out_dir.iterdir():
                if not f.is_file() or not _OUTPUT_NAME_RE.match(f.name):
                    continue
                try:
                    st = f.stat()
                except Exception:
                    continue
                items.append({
                    "name": f.name,
                    "size": st.st_size,
                    "mtime": int(st.st_mtime),
                })
        items.sort(key=lambda x: x["mtime"], reverse=True)
        return {"items": items[:50], "total": len(items)}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Cron outputs error: {e}")


@app.get("/api/cron/jobs/{job_id}/outputs/{name}")
def cron_job_output_content(job_id: str, name: str, request: Request):
    """单次运行的输出内容（纯文本，最多返回 200KB）"""
    check_auth(request)
    # 双重防护：名字白名单 + 解析后仍在目标目录内
    if not _OUTPUT_NAME_RE.match(name):
        raise HTTPException(status_code=400, detail="INVALID_OUTPUT_NAME")
    try:
        jobs_mod = _cron_jobs_module()
        if jobs_mod.get_job(job_id) is None:
            raise HTTPException(status_code=404, detail="JOB_NOT_FOUND")

        out_dir = _cron_output_dir(job_id).resolve()
        target = (out_dir / name).resolve()
        if out_dir not in target.parents:
            raise HTTPException(status_code=400, detail="INVALID_OUTPUT_NAME")
        if not target.is_file():
            raise HTTPException(status_code=404, detail="OUTPUT_NOT_FOUND")

        data = target.read_bytes()[:200000]
        text = data.decode("utf-8", errors="replace")
        return {"name": name, "content": text, "truncated": len(data) >= 200000}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Cron output read error: {e}")


@app.post("/api/cron/jobs/{job_id}/update")
async def cron_job_update(job_id: str, request: Request):
    """编辑任务的 name / schedule / prompt。

    放开范围：
      - name      ✅
      - schedule  ✅（变更后 next_run_at 由 update_job 自动重算）
      - prompt    ✅（用户要求 APP 内可改）
    仍然挡住：
      - id        由 update_job 的 IMMUTABLE 保护兜底（它是 output 目录名）
      - script / no_agent  改了会让任务性质完全变样（Agent 任务变脚本任务），
                  继续在 Hermes 侧用 CLI 改
    """
    check_auth(request)
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    updates = {}
    if "name" in body:
        nm = str(body.get("name") or "").strip()
        if not nm:
            raise HTTPException(status_code=400, detail="NAME_EMPTY")
        if len(nm) > 120:
            raise HTTPException(status_code=400, detail="NAME_TOO_LONG")
        updates["name"] = nm

    if "schedule" in body:
        expr = str(body.get("schedule") or "").strip()
        if not expr:
            raise HTTPException(status_code=400, detail="SCHEDULE_EMPTY")
        try:
            jobs_mod_probe = _cron_jobs_module()
            jobs_mod_probe.parse_schedule(expr)   # 非法表达式会抛
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"INVALID_SCHEDULE: {e}")
        updates["schedule"] = expr

    # prompt：用户明确要求 APP 里也能改。
    # 长度上限给足（prompt 常有几千字），同时挡住异常巨大的输入。
    if "prompt" in body:
        p = str(body.get("prompt") or "")
        if len(p) > 50000:
            raise HTTPException(status_code=400, detail="PROMPT_TOO_LONG")
        updates["prompt"] = p

    if not updates:
        raise HTTPException(status_code=400, detail="NOTHING_TO_UPDATE")

    try:
        jobs_mod = _cron_jobs_module()
        if jobs_mod.get_job(job_id) is None:
            raise HTTPException(status_code=404, detail="JOB_NOT_FOUND")
        updated = jobs_mod.update_job(job_id, updates)
        if updated is None:
            raise HTTPException(status_code=404, detail="JOB_NOT_FOUND")
        view = _cron_job_view(updated)
        view["prompt"] = updated.get("prompt") or ""
        view["schedule_expr"] = (updated.get("schedule") or {}).get("expr") or ""
        return view
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Cron update error: {e}")


# ══════════════════════════════════════════════════════════════════════
#  会话列表 / 会话消息 API（S12a）
#
#  数据源: Hermes 的 ~/.hermes/state.db（sessions / messages 表）
#  作用域: 只列 source='api_server' 的会话 —— APP 自己产生的那些，
#          cron / CLI / 其它平台产生的几百个会话不该出现在 APP 列表里
# ══════════════════════════════════════════════════════════════════════

STATE_DB = Path(os.getenv("STATE_DB") or os.path.expanduser("~/.hermes/state.db"))


def _state_db():
    if not STATE_DB.exists():
        raise HTTPException(status_code=500, detail="STATE_DB_MISSING")
    return sqlite3.connect(str(STATE_DB))


@app.get("/api/sessions")
def list_sessions(
    request: Request,
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    """会话题列表（按 updated_at 倒序）"""
    check_auth(request)
    try:
        db = _state_db()
        db.row_factory = sqlite3.Row

        # updated_at 用最后一条消息的时间，没有消息时回退 started_at
        # archived=1 的会话是用户在 APP 里删掉的（软删除，Hermes 侧数据保留）
        total = db.execute(
            f"SELECT COUNT(*) FROM sessions WHERE source IN ({_SESSION_SRC_SQL})"
            " AND COALESCE(archived, 0) = 0"
        ).fetchone()[0]

        rows = db.execute(f"""
            SELECT s.id, s.title, s.model, s.started_at, s.message_count,
                   s.input_tokens, s.output_tokens, s.source, s.tool_call_count,
                   s.last_activity_at, s.last_activity_description,
                   COALESCE((SELECT MAX(m.timestamp) FROM messages m
                             WHERE m.session_id = s.id), s.started_at) AS updated_at,
                   (SELECT m.content FROM messages m
                     WHERE m.session_id = s.id AND m.role = 'user'
                       AND m.content IS NOT NULL AND TRIM(m.content) <> ''
                     ORDER BY m.timestamp ASC LIMIT 1) AS first_user_msg
            FROM sessions s
            WHERE s.source IN ({_SESSION_SRC_SQL})
              AND COALESCE(s.archived, 0) = 0
            ORDER BY updated_at DESC
            LIMIT ? OFFSET ?
        """, (limit, offset)).fetchall()

        items = []
        for r in rows:
            # 默认标题：优先用 Hermes 写好的 title；没有就用首条用户消息
            # （截断到 20 字）——避免客户端只能显示 app-xxxx 这种看不出内容的 ID
            _t = (r["title"] or "").strip()
            if not _t:
                _f = (r["first_user_msg"] or "").strip().replace("\n", " ")
                _t = _f[:20] + ("…" if len(_f) > 20 else "")
            items.append({
                "id": r["id"],
                "title": _t,
                "model": r["model"] or "",
                "started_at": int(r["started_at"]) if r["started_at"] else 0,
                "updated_at": int(r["updated_at"]) if r["updated_at"] else 0,
                "message_count": r["message_count"] or 0,
                "tokens": (r["input_tokens"] or 0) + (r["output_tokens"] or 0),
                # 跨端可见：来源标记 + 当前在干什么（桌面/WebUI 会话也走这里）
                "source": r["source"] or "",
                "tool_calls": r["tool_call_count"] or 0,
                "activity": r["last_activity_description"] or "",
                "active": _is_busy(r["last_activity_description"], r["last_activity_at"]),
            })
        db.close()
        return {
            "items": items,
            "total": total,
            "has_more": (offset + len(items)) < total,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Sessions error: {e}")


@app.patch("/api/sessions/{session_id}")
async def rename_session(session_id: str, request: Request):
    """重命名会话（写 Hermes sessions.title）"""
    check_auth(request)
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="INVALID_BODY")
    title = (body.get("title") or "").strip()
    if len(title) == 0:
        raise HTTPException(status_code=400, detail="EMPTY_TITLE")
    if len(title) > 60:
        title = title[:60]
    try:
        db = _state_db()
        cur = db.execute(
            "UPDATE sessions SET title = ? WHERE id = ?", (title, session_id)
        )
        db.commit()
        changed = cur.rowcount
        db.close()
        if changed == 0:
            raise HTTPException(status_code=404, detail="SESSION_NOT_FOUND")
        return {"status": "ok", "id": session_id, "title": title}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Rename error: {e}")


@app.delete("/api/sessions/{session_id}")
def delete_session(session_id: str, request: Request):
    """
    删除会话。

    ⚠️ 只做**软删除**（archived=1）：Hermes 的 sessions / messages 数据一律不动。
    理由：APP 端删错了要能恢复，而且这些表同时被 cron、WebUI、桌面端共用，
    真删会连带影响别的入口（历史的教训：把别人在用的数据删掉，排查起来极难）。
    """
    check_auth(request)
    try:
        db = _state_db()
        cur = db.execute(
            "UPDATE sessions SET archived = 1 WHERE id = ?", (session_id,)
        )
        db.commit()
        changed = cur.rowcount
        db.close()
        if changed == 0:
            raise HTTPException(status_code=404, detail="SESSION_NOT_FOUND")
        return {"status": "ok", "id": session_id, "archived": True}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Delete error: {e}")


def _clear_offline_cache(session_id: str) -> int:
    """
    删除某会话的离线缓存（S19）。

    ⚠️ 为什么在"历史已送达"之后清：offline_chats 是给**断线期间**没收到回复的客户端
    补投用的。一旦 APP 通过 /messages 拉到完整历史（其中已含该回复），缓存就完成使命了
    —— 留着的话，之后任何一次 sync_pending 都会把同一份内容再投一遍，
    用户会看到两条一模一样的回复（实测会话 app-42015772fca4497d）。
    """
    try:
        db = sqlite3.connect(str(PUSH_DB))
        cur = db.execute("DELETE FROM offline_chats WHERE session_id = ?", (session_id,))
        db.commit()
        n = cur.rowcount
        db.close()
        if n:
            print(f"[Offline] cleared {n} cache row(s) after history sync for {session_id}",
                  flush=True)
        return n
    except Exception as e:
        print(f"[Offline] clear cache failed for {session_id}: {e}", flush=True)
        return 0


def _normalize_content(s: str) -> str:
    """V4-S9：内容归一（去首尾空白）。用于缓存/历史比对，两边同样处理。"""
    return (s or "").strip()


def _clear_offline_cache_if_delivered(session_id: str, items: list) -> int:
    """
    V4-S9：内容级确认后才清缓存（防「拉过历史就清」误删未送达的新回复）。
    判据要点（修 V2「模板前缀假送达」）：
      ① 只比 role == 'assistant' 的项
      ② 只比【最后一条】assistant（防历史里旧回复的同前缀误命中）
      ③ 长度接近（abs(len(a) - len(cached)) < 20）—— 同一回复的不同阶段
      ④ >= 80 字符 → 前缀匹配；< 80 → strip() 后全等
      ⑤ 两边做同样 normalize
    ⚠️ 本函数在 FastAPI 线程池执行（session_messages 是同步 def），
      与 WS handler 并发访问 PUSH_DB → 必须显式 timeout，
      异常一律收敛为「保留缓存」（fail-safe：宁可重复投递也不丢回复）。
    """
    try:
        db = sqlite3.connect(str(PUSH_DB), timeout=5)
        try:
            row = db.execute(
                "SELECT content FROM offline_chats WHERE session_id = ?",
                (session_id,)).fetchone()
            if not row:
                return 0
            cached = _normalize_content(row[0] or "")
            if not cached:
                return 0
            # ② 只取最后一条 assistant
            last_a = ""
            for it in items:
                if (it.get("role") or "") == "assistant":
                    last_a = _normalize_content(str(it.get("content") or ""))
            delivered = False
            if last_a:
                # ③ 长度接近
                if abs(len(last_a) - len(cached)) < 20:
                    if len(cached) >= 80:
                        delivered = cached[:80] in last_a  # ④ 前缀
                    else:
                        delivered = (cached == last_a)     # ④ 全等
            if not delivered:
                # 未确认送达 → 保留缓存
                return 0
            cur = db.execute("DELETE FROM offline_chats WHERE session_id = ?",
                             (session_id,))
            db.commit()
            n = cur.rowcount
            if n:
                print(f"[Offline] cleared {n} cache row(s) after content-level confirm for {session_id}",
                      flush=True)
            return n
        finally:
            db.close()
    except Exception as e:
        # fail-safe：比对/DB 出错一律保留缓存
        print(f"[Offline] content-level clear skipped for {session_id}: {e}", flush=True)
        return 0


# ══════════════════════════════════════════════════════════════════════
#  V4-S12：chat 幂等模块（借鉴 Gateway api_server_run_idempotency.py）
#  保留：scope+key 主键 / fingerprint 冲突分类 / status_json / 双保留期
#        / BEGIN IMMEDIATE 原子 reserve / 只 prune 终态行
#  省略：owner_pid / owner_started（单进程，用启动清扫 idem_recover_orphans 替代）
# ══════════════════════════════════════════════════════════════════════
import hmac
import hashlib

_IDEM_TERMINAL = {"complete", "error", "superseded"}
IDEM_RETENTION = 1800          # 终态保留 30 分钟
IDEM_STALE_PROCESSING = 600    # processing 超 10 分钟视为孤儿（N2 超时放行）


def _idem_conn():
    return sqlite3.connect(str(PUSH_DB), timeout=5)


def _chat_fingerprint(session_id: str, messages: list) -> str:
    """fingerprint：同 key 不同内容 = conflict（不复用不误杀）。"""
    last_user = {}
    for m in reversed(messages):
        if (m.get("role") or "") == "user":
            last_user = m
            break
    payload = (session_id or "") + "|" + str(len(messages)) + "|" + str(last_user.get("content", ""))[:200]
    return hmac.new(b"chat-idem", payload.encode("utf-8", "replace"), hashlib.sha256).hexdigest()


def _idem_prune_locked(db, now: float):
    """只删终态且过保留期的行（processing 行永不被 prune —— 照 Gateway）。"""
    rows = db.execute(
        "SELECT idempotency_key, status_json, retention_until, updated_at"
        " FROM chat_idempotency WHERE scope='chat'").fetchall()
    for key, sj, rt, up in rows:
        try:
            terminal = json.loads(sj).get("status") in _IDEM_TERMINAL
        except Exception:
            terminal = False
        if not terminal:
            continue
        if (rt and rt <= now) or (not rt and up < now - IDEM_RETENTION):
            db.execute("DELETE FROM chat_idempotency WHERE scope='chat' AND idempotency_key=?", (key,))


def idem_reserve(key: str, fingerprint: str, run_id: str, status: str = "processing"):
    """原子预留（BEGIN IMMEDIATE）。返回 (created|reused|conflict, record)。"""
    now = time.time()
    db = _idem_conn()
    try:
        db.execute("BEGIN IMMEDIATE")
        _idem_prune_locked(db, now)
        row = db.execute(
            "SELECT fingerprint, status_json FROM chat_idempotency"
            " WHERE scope='chat' AND idempotency_key=?", (key,)).fetchone()
        if row is not None:
            db.commit()
            outcome = "reused" if hmac.compare_digest(row[0], fingerprint) else "conflict"
            return outcome, json.loads(row[1])
        encoded = json.dumps({"status": status}, ensure_ascii=False)
        db.execute(
            "INSERT INTO chat_idempotency(scope,idempotency_key,fingerprint,run_id,status_json,"
            "retention_until,created_at,updated_at) VALUES('chat',?,?,?,?,0,?,?)",
            (key, fingerprint, run_id, encoded, now, now))
        db.commit()
        return "created", {"status": status}
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def idem_lookup(key: str, fingerprint: str):
    """返回 (missing|reused|conflict|stale, record)。stale = 孤儿 processing（N2 超时放行）。"""
    db = _idem_conn()
    try:
        row = db.execute(
            "SELECT fingerprint, status_json, updated_at FROM chat_idempotency"
            " WHERE scope='chat' AND idempotency_key=?", (key,)).fetchone()
    finally:
        db.close()
    if row is None:
        return "missing", None
    outcome = "reused" if hmac.compare_digest(row[0], fingerprint) else "conflict"
    try:
        rec = json.loads(row[1])
    except Exception:
        rec = {"status": "error"}
    if rec.get("status") == "processing" and time.time() - (row[2] or 0) > IDEM_STALE_PROCESSING:
        return "stale", rec
    return outcome, rec


def idem_delete_stale(key: str):
    """stale 行放行前删掉（否则 reserve 主键冲突）。"""
    db = _idem_conn()
    try:
        db.execute("DELETE FROM chat_idempotency WHERE scope='chat' AND idempotency_key=?", (key,))
        db.commit()
    finally:
        db.close()


def idem_update_status(key: str, status: dict):
    """终态更新：写 status_json + 启用保留期（照 Gateway update_status）。"""
    db = _idem_conn()
    try:
        now = time.time()
        db.execute(
            "UPDATE chat_idempotency SET status_json=?, updated_at=? WHERE"
            " scope='chat' AND idempotency_key=?",
            (json.dumps(status, ensure_ascii=False), now, key))
        if (status.get("status") or "") in _IDEM_TERMINAL:
            db.execute(
                "UPDATE chat_idempotency SET retention_until=? WHERE"
                " scope='chat' AND idempotency_key=?",
                (now + IDEM_RETENTION, key))
        db.commit()
    finally:
        db.close()


def idem_recover_orphans():
    """★ N2 启动清扫：进程死亡后 processing 行必死 → 全部刷成 error（可重发）。
    在 startup 事件里调用。"""
    db = _idem_conn()
    try:
        rows = db.execute(
            "SELECT idempotency_key, status_json FROM chat_idempotency"
            " WHERE scope='chat'").fetchall()
        n = 0
        now = time.time()
        for key, sj in rows:
            try:
                if json.loads(sj).get("status") == "processing":
                    db.execute(
                        "UPDATE chat_idempotency SET status_json=?, updated_at=?,"
                        " retention_until=? WHERE scope='chat' AND idempotency_key=?",
                        (json.dumps({"status": "error"}), now, now + IDEM_RETENTION, key))
                    n += 1
            except Exception:
                continue
        db.commit()
        if n:
            print(f"[Idem] 启动清扫：恢复孤儿 processing 行 {n} 条 → error", flush=True)
    finally:
        db.close()


def _idem_ensure_table():
    """幂等表建表（幂等，startup 调）。"""
    db = _idem_conn()
    try:
        db.execute(
            """CREATE TABLE IF NOT EXISTS chat_idempotency (
                scope           TEXT NOT NULL,
                idempotency_key TEXT NOT NULL,
                fingerprint     TEXT NOT NULL,
                run_id          TEXT NOT NULL,
                status_json     TEXT NOT NULL,
                retention_until REAL NOT NULL DEFAULT 0,
                acknowledged_at REAL,
                created_at      REAL NOT NULL,
                updated_at      REAL NOT NULL,
                PRIMARY KEY (scope, idempotency_key)
            )""")
        db.commit()
    finally:
        db.close()


@app.get("/api/sessions/{session_id}/messages")
def session_messages(
    session_id: str,
    request: Request,
    limit: int = Query(100, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    """某个会话的消息。

    offset 语义：**从最新往历史翻的窗口偏移**。
      offset=0   -> 最新的一页（limit 条）
      offset=100 -> 再往前一页
    返回时始终按时间**正序**排（老的在前），便于界面直接往下追加渲染。
    """
    check_auth(request)
    try:
        db = _state_db()
        db.row_factory = sqlite3.Row

        exists = db.execute(
            "SELECT 1 FROM sessions WHERE id = ?", (session_id,)
        ).fetchone()
        if exists is None:
            db.close()
            raise HTTPException(status_code=404, detail="SESSION_NOT_FOUND")

        total = db.execute(
            "SELECT COUNT(*) FROM messages WHERE session_id = ? "
            "AND (active = 1 OR compacted = 1)", (session_id,)
        ).fetchone()[0]

        # 先按时间倒序取窗口，再反转成正序返回
        rows = db.execute("""
            SELECT id, role, content, timestamp
            FROM messages
            WHERE session_id = ?
              AND (active = 1 OR compacted = 1)
            ORDER BY timestamp DESC, id DESC
            LIMIT ? OFFSET ?
        """, (session_id, limit, offset)).fetchall()
        # 跨端可见性（2026-10-25）：把会话级状态随包返回。
        #   APP 轮询「桌面会话」时只看这一个接口就能判断对面还在不在跑、跑到哪一步，
        #   不必再额外打一次 /api/sessions 去列表里找自己。
        meta = db.execute(
            "SELECT source, last_activity_at, last_activity_description, message_count"
            " FROM sessions WHERE id = ?", (session_id,)
        ).fetchone()
        db.close()

        items = []
        for r in reversed(rows):
            items.append({
                "id": r["id"],
                "role": r["role"] or "",
                "content": r["content"] or "",
                "created_at": int(r["timestamp"]) if r["timestamp"] else 0,
            })
        # S19：历史已送达 → 清掉该会话的离线缓存，避免后续 sync_pending 重复投递同一份内容
        # （只在 offset=0 时清：分页翻更早的历史不该动缓存）
        # V4-S8：改传 items 做内容级确认（S9），不再「拉过历史就清」
        if offset == 0:
            _clear_offline_cache_if_delivered(session_id, items)
        return {
            "items": items,
            "total": total,
            "has_more": (offset + len(items)) < total,
            # 会话级状态：source = 哪一端建的；active = 对面是否正在这一轮里跑
            "session": {
                "id": session_id,
                "source": (meta[0] or "") if meta else "",
                "active": _is_busy((meta[2] or "") if meta else None,
                                  meta[1] if meta else None),
                "activity": (meta[2] or "") if meta else "",
                "message_count": (meta[3] or 0) if meta else 0,
            },
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Session messages error: {e}")


# ══════════════════════════════════════════════════════════════════════
#  模型 / 技能 / 记忆（S13a）
# ══════════════════════════════════════════════════════════════════════

HERMES_CONFIG = Path(os.path.expanduser("~/.hermes/config.yaml"))
# APP 专属模型覆盖（**不动全局 config.yaml**）。
# APP 的「模型」页切模型时写在这里，只影响 APP 自己发起的对话；
# WebUI / Desktop / CLI 继续走 config.yaml 的全局默认，互不干扰。
APP_MODEL_STATE = Path(os.path.expanduser("~/.hermes/xiaoq_app_model.json"))


def _load_app_model() -> dict:
    """读 APP 的模型覆盖；未设置/文件损坏 → 返回 {}（回落全局默认）。"""
    try:
        d = json.loads(APP_MODEL_STATE.read_text(encoding="utf-8"))
        mid = str(d.get("id") or "").strip()
        if not mid:
            return {}
        return {"id": mid, "provider": str(d.get("provider") or "").strip()}
    except Exception:
        return {}


def _save_app_model(mid: str, provider: str) -> None:
    """原子写 APP 模型覆盖。mid 为空 = 清除覆盖，回全局默认。"""
    if not mid:
        try:
            APP_MODEL_STATE.unlink()
        except FileNotFoundError:
            pass
        return
    tmp = str(APP_MODEL_STATE) + ".tmp"
    with io.open(tmp, "w", encoding="utf-8") as f:
        json.dump({"id": mid, "provider": provider,
                   "updated_at": datetime.now(TZ).isoformat()},
                  f, ensure_ascii=False, indent=2)
    os.replace(tmp, str(APP_MODEL_STATE))

HERMES_SKILLS_DIR = Path(os.path.expanduser("~/.hermes/skills"))
HERMES_MEMORY = Path(os.path.expanduser("~/.hermes/memories/MEMORY.md"))


def _load_hermes_config():
    try:
        import yaml
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"YAML_UNAVAILABLE: {e}")
    if not HERMES_CONFIG.exists():
        raise HTTPException(status_code=500, detail="CONFIG_MISSING")
    try:
        return yaml.safe_load(io.open(HERMES_CONFIG, encoding="utf-8")) or {}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"CONFIG_PARSE_ERROR: {e}")


def _collect_models(cfg: dict) -> list:
    """可用模型清单。

    数据源优先级（B 方案，2026-09-19）：
      1. gateway.platforms.api_server.extra.model_routes —— APP 侧可切的路由，
         每条都带**真实 provider**（上游 Gateway 原生机制，per-request 生效）
      2. 没有 model_routes 时回退旧逻辑：从 model/moa/auxiliary 里扫

    `current` 标记只认全局 model.default + model.provider 全匹配的那条。
    历史 bug：只比 model 名不比 provider，导致
    default=gemini-2.5-flash/provider=deepseek 这种错配下，APP 显示"已切到
    gemini"但实际仍走 DeepSeek。这里两个字段必须同时匹配。
    """
    cur_model = ((cfg.get("model") or {}).get("default")) or ""
    cur_provider = ((cfg.get("model") or {}).get("provider")) or ""
    # APP 有覆盖时，current 标记跟着 APP 走（这个接口只有 APP 用）
    _app = _load_app_model()
    eff_model = _app.get("id") or cur_model
    eff_provider = _app.get("provider") or cur_provider
    seen = {}
    order = []

    def add(mid, prov, is_cur=False):
        if not mid or not isinstance(mid, str):
            return
        key = "%s|%s" % (prov or "", mid)
        if key in seen:
            if is_cur:
                seen[key]["current"] = True
            return
        seen[key] = {"id": mid, "name": mid, "provider": prov or "", "current": is_cur}
        order.append(key)

    # ① 首选 model_routes
    routes = (((cfg.get("gateway") or {}).get("platforms") or {})
              .get("api_server") or {}).get("extra") or {}
    routes = routes.get("model_routes") or {}
    if isinstance(routes, dict):
        for alias, rc in routes.items():
            if not isinstance(rc, dict):
                continue
            m = str(rc.get("model") or alias).strip()
            p = str(rc.get("provider") or "").strip()
            if not m:
                continue
            # 当前生效模型（APP 覆盖优先）+ provider 必须同时匹配
            is_cur = (m == eff_model and (not p or not eff_provider or p == eff_provider))
            add(alias if alias != m else m, p, is_cur)

    if not order:
        # ② 回退：老逻辑
        add(eff_model, eff_provider, True)

        moa = cfg.get("moa") or {}
        for m in (moa.get("reference_models") or []):
            if isinstance(m, dict):
                add(m.get("model"), m.get("provider"))
        agg = moa.get("aggregator") or {}
        if isinstance(agg, dict):
            add(agg.get("model"), agg.get("provider"))
        presets = moa.get("presets") or {}
        if isinstance(presets, dict):
            for _, pv in presets.items():
                if not isinstance(pv, dict):
                    continue
                for m in (pv.get("reference_models") or []):
                    if isinstance(m, dict):
                        add(m.get("model"), m.get("provider"))
                a = pv.get("aggregator") or {}
                if isinstance(a, dict):
                    add(a.get("model"), a.get("provider"))

        aux = cfg.get("auxiliary") or {}
        if isinstance(aux, dict):
            for _, av in aux.items():
                if isinstance(av, dict) and av.get("provider") not in (None, "auto"):
                    add(av.get("model"), av.get("provider"))

    return [seen[k] for k in order]


@app.get("/api/models")
def list_models(request: Request):
    """可用模型（当前配置里出现过的那批）+ current 标记"""
    check_auth(request)
    cfg = _load_hermes_config()
    items = _collect_models(cfg)
    return {"items": items, "total": len(items)}


@app.post("/api/models/switch")
async def switch_model(request: Request):
    """切换 **APP 自己** 用的模型（写 ~/.hermes/xiaoq_app_model.json）。

    为什么不再改 config.yaml 的 model.default（2026-09-19 重构）：
      config.yaml 的 model 段是**全局单点** —— WebUI / Desktop / CLI / APP
      全读它。APP 切一下，所有人的模型都跟着变，这不是我们要的隔离。
      而且旧实现只写 default 不写 provider，造成
      `default=gemini-2.5-flash` + `provider=deepseek` 的错配，上游收到
      不认识的模型名后静默回落到 provider 自己的默认模型 —— 表现就是
      "APP 显示已切到 gemini，回话还是 DeepSeek"（本次定位的 bug）。

    现在：覆盖只存在本地小状态里，只影响 APP 发起的对话；
    WebUI / Desktop / CLI 继续走全局默认。
    `id` 传空串 / "current" / "default" = 清除覆盖，回全局默认。

    ⚠️ 生效时机 = **下一次对话**（不动正在进行的流）。
    """
    check_auth(request)
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    mid = str(body.get("id") or "").strip()

    # 清除覆盖 → 回全局默认
    if mid in ("", "current", "default", "reset"):
        _save_app_model("", "")
        print("[Models] APP 覆盖已清除，回落全局默认", flush=True)
        return {
            "id": "",
            "provider": "",
            "scope": "global",
            "applied_at": datetime.now(TZ).isoformat(),
            "previous": (_load_hermes_config().get("model") or {}).get("default") or "",
            "note": "已恢复全局默认模型，下一次对话生效",
        }

    cfg = _load_hermes_config()
    items = _collect_models(cfg)
    allowed = {m["id"] for m in items}
    if mid not in allowed:
        # 不在配置已知集合里 —— 不直接拒绝（用户可能想用新模型），但要显式确认
        if body.get("force") is not True:
            raise HTTPException(status_code=404, detail="MODEL_NOT_FOUND")

    # provider 从路由表反查（必须带上，否则又变成"只换名字不换后端"）
    provider = str(body.get("provider") or "").strip()
    if not provider:
        for m in items:
            if m["id"] == mid and m.get("provider"):
                provider = m["provider"]
                break
    if not provider:
        raise HTTPException(
            status_code=409,
            detail="PROVIDER_UNRESOLVED: 该模型不在 model_routes 里，"
                   "无法确定 provider（请在 config.yaml 的 "
                   "gateway.platforms.api_server.extra.model_routes 里补一条）",
        )

    prev = _load_app_model()
    try:
        _save_app_model(mid, provider)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"MODEL_SWITCH_WRITE_FAILED: {e}")

    print(f"[Models] APP 模型: {prev.get('id') or '(全局)'} -> {mid} ({provider})", flush=True)
    return {
        "id": mid,
        "provider": provider,
        "scope": "app",
        "applied_at": datetime.now(TZ).isoformat(),
        "previous": prev.get("id") or "",
        "note": "仅影响 APP，下一次对话生效；WebUI/Desktop/CLI 不受影响",
    }


@app.get("/api/skills")
def list_skills(request: Request, q: str = Query("")):
    """技能列表（只读）。从各分类目录下的 SKILL.md 抽取 frontmatter。"""
    check_auth(request)
    items = []
    if HERMES_SKILLS_DIR.is_dir():
        for cat in sorted(HERMES_SKILLS_DIR.iterdir()):
            # 跳过隐藏/归档目录（如 .archive / .cache）—— 它们不是可用的技能分类
            if not cat.is_dir() or cat.name.startswith("."):
                continue
            for sk in sorted(cat.iterdir()):
                if sk.name.startswith("."):
                    continue
                f = sk / "SKILL.md"
                if not f.is_file():
                    continue
                name, desc = sk.name, ""
                try:
                    head = f.read_text(encoding="utf-8", errors="replace")[:2000]
                    if head.startswith("---"):
                        end = head.find("\n---", 3)
                        fm = head[3:end] if end > 0 else head[3:]
                        for line in fm.splitlines():
                            if line.startswith("name:"):
                                name = line.split(":", 1)[1].strip().strip('"\'')
                            elif line.startswith("description:"):
                                desc = line.split(":", 1)[1].strip().strip('"\'')
                except Exception:
                    pass
                if q and q.lower() not in name.lower() and q.lower() not in desc.lower():
                    continue
                items.append({"name": name, "description": desc, "category": cat.name})
    return {"items": items, "total": len(items)}


@app.get("/api/memory")
def get_memory(request: Request):
    """Agent 记忆文件内容（只读）"""
    check_auth(request)
    if not HERMES_MEMORY.exists():
        return {"content": "", "exists": False}
    try:
        txt = HERMES_MEMORY.read_text(encoding="utf-8", errors="replace")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"MEMORY_READ_ERROR: {e}")
    info = HERMES_MEMORY.stat()
    return {
        "content": txt,
        "exists": True,
        "size": info.st_size,
        "updated_at": int(info.st_mtime),
    }


@app.get("/api/approvals/pending")
async def list_pending_approvals():
    """S42：列出未处理的审批（含已超时但保留的）。

    APP 在**启动**与**WS 重连成功**后各拉一次，用于把本地因误触/切页/断连
    丢失的审批卡片补回队列 —— 这是"误触不再等于永久失联"的关键一环。
    顺手清理超过 _APPROVAL_KEEP_SECONDS 的僵尸记录。
    """
    now = time.time()
    items = []
    for run_id, p in list(_pending_approvals.items()):
        age = now - float(p.get("created_at") or now)
        if age > _APPROVAL_KEEP_SECONDS:
            _pending_approvals.pop(run_id, None)
            continue
        if p.get("result") is not None:
            # 已选好、等待后台提交 —— 不该再让用户重复审批
            continue
        items.append({
            "run_id": run_id,
            "command": p.get("command", ""),
            "description": p.get("description", ""),
            "pattern_keys": p.get("pattern_keys", []),
            "session_id": p.get("session_id", ""),
            "created_at": p.get("created_at", 0),
            "age_s": round(age, 1),
            "expired": bool(p.get("expired", False)),
        })
    items.sort(key=lambda x: x.get("created_at", 0))
    return {"items": items, "count": len(items)}


@app.post("/api/test-approval")
async def test_approval(request: Request):
    """测试授权卡片：向所有WebSocket客户端发送approve_request"""
    # S11：测试消息也带上触发上下文 + 一条超长命令，便于验证
    # 「上下文展示」「长命令滚动」「始终允许二次确认」三项增强
    _long_cmd = "rm -rf /data/production/db && " + " && ".join(
        ["cp -r /etc/nginx/conf.d/%d /backup/nginx_%d" % (i, i) for i in range(1, 21)]
    )
    # 允许调用方指定 command 与 run_id，便于验证"同一 run 多次审批"的场景
    _cmd_override = request.query_params.get("command", "")
    _run_override = request.query_params.get("run_id", "test-run-001")
    await manager.broadcast({
        "type": "approve_request",
        "run_id": _run_override,
        "command": (_cmd_override if _cmd_override else _long_cmd),
        "description": "执行数据库清理脚本时触发安全扫描（测试消息）",
        "session_id": "device-test-session-s11",
        "pattern_keys": ["tirith:destructive_command"],
        "choices": ["once", "session", "always", "deny"]
    })
    return {"status": "ok", "message": "approve_request broadcast sent"}


@app.post("/api/test-disconnect")
async def test_disconnect(data: dict):
    """测试用：强制断开指定客户端的 WebSocket（服务端进程不受影响）

    body: {"client_id": "device-xxx"}  留空则断开全部
    """
    target = data.get("client_id", "")
    if target:
        async with manager.lock:
            ws = manager.active_connections.pop(target, None)
        if ws is None:
            return {"ok": False, "error": "client not connected",
                    "connected": list(manager.active_connections.keys())}
        try:
            await ws.close(code=1011, reason="test-disconnect")
        except Exception:
            pass
        return {"ok": True, "disconnected": target}

    async with manager.lock:
        items = list(manager.active_connections.items())
        manager.active_connections.clear()
    for cid, ws in items:
        try:
            await ws.close(code=1011, reason="test-disconnect")
        except Exception:
            pass
    return {"ok": True, "disconnected": [cid for cid, _ in items]}



@app.get("/api/push/messages")
def list_messages(
    request: Request,
    limit: int = Query(50, ge=1, le=200),
    unread_only: bool = Query(False),
):
    """Get push messages. Optionally filter to unread only."""
    check_auth(request)
    db = sqlite3.connect(str(PUSH_DB))
    if unread_only:
        rows = db.execute(
            "SELECT * FROM push_messages WHERE read_at IS NULL ORDER BY id DESC LIMIT ?",
            (limit,),
        ).fetchall()
    else:
        rows = db.execute(
            "SELECT * FROM push_messages ORDER BY id DESC LIMIT ?",
            (limit,),
        ).fetchall()
    db.close()

    messages = []
    for row in rows:
        messages.append({
            "id": row[0],
            "title": row[1],
            "body": row[2],
            "source": row[3],
            "created_at": row[4],
            "read_at": row[5],
        })
    return {"messages": messages, "total": len(messages)}


@app.get("/api/push/unread")
def unread_count(request: Request):
    """Get unread message count and latest preview."""
    check_auth(request)
    db = sqlite3.connect(str(PUSH_DB))
    count = db.execute(
        "SELECT COUNT(*) FROM push_messages WHERE read_at IS NULL"
    ).fetchone()[0]
    latest = db.execute(
        "SELECT id, title, body, created_at FROM push_messages WHERE read_at IS NULL ORDER BY id DESC LIMIT 1"
    ).fetchone()
    db.close()
    return {
        "unread_count": count,
        "latest": {
            "id": latest[0],
            "title": latest[1],
            "body": latest[2][:200],
            "created_at": latest[3],
        } if latest else None,
    }


@app.put("/api/push/messages/{msg_id}/read")
def mark_read(msg_id: int, request: Request):
    """Mark a message as read."""
    check_auth(request)
    db = sqlite3.connect(str(PUSH_DB))
    cur = db.execute(
        "UPDATE push_messages SET read_at = datetime('now', '+8 hours') WHERE id = ? AND read_at IS NULL",
        (msg_id,),
    )
    db.commit()
    affected = cur.rowcount
    db.close()
    return {"status": "ok", "updated": affected > 0}


@app.post("/api/push/read")
async def mark_read_batch(request: Request):
    """Batch mark-as-read.

    body: {"ids": [1,2,3]}  or  {"all": true}
    -> {"status": "ok", "updated": N}

    Idempotent: already-read rows are excluded by "read_at IS NULL",
    so a repeated call returns updated=0.
    """
    check_auth(request)
    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    mark_all = payload.get("all") is True
    ids = payload.get("ids")

    if not mark_all and not isinstance(ids, list):
        raise HTTPException(
            status_code=400,
            detail="Provide 'ids' (list of ints) or 'all': true",
        )

    db = sqlite3.connect(str(PUSH_DB))
    if mark_all:
        cur = db.execute(
            "UPDATE push_messages SET read_at = datetime('now', '+8 hours') "
            "WHERE read_at IS NULL"
        )
    else:
        clean = [x for x in ids if isinstance(x, int) and not isinstance(x, bool)]
        if not clean:
            db.close()
            return {"status": "ok", "updated": 0}
        placeholders = ",".join("?" * len(clean))
        cur = db.execute(
            "UPDATE push_messages SET read_at = datetime('now', '+8 hours') "
            "WHERE read_at IS NULL AND id IN (" + placeholders + ")",
            clean,
        )
    db.commit()
    updated = cur.rowcount
    db.close()
    return {"status": "ok", "updated": updated}


@app.get("/api/push/stream")
async def push_stream(request: Request):
    """SSE endpoint: real-time push delivery to APP."""
    q = push_events.subscribe()

    async def event_generator():
        try:
            while True:
                payload = await q.get()
                yield f"data: {payload}\n\n"
        except asyncio.CancelledError:
            pass
        finally:
            push_events.unsubscribe(q)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


# ── Main ─────────────────────────────────────────────────────────────


# ══════════════════════════════════════════════════════════════════════
#  Direct Model Proxy (bypasses Hermes agent — sub-3s TTFT)
# ══════════════════════════════════════════════════════════════════════

@app.post("/v1/chat/direct")
async def chat_direct(request: Request):
    """直连 OpenRouter，跳过 Hermes Agent 预处理，简单对话 <3s。
    同时异步写回 Hermes 存档。"""
    if not OPENROUTER_KEY:
        raise HTTPException(status_code=503, detail="OpenRouter not configured")
    
    body = await request.body()
    session_id = request.headers.get("X-Hermes-Session-Id", "")
    
    # ── Thread-based archive (non-blocking, runs after response) ──
    def archive_to_hermes():
        try:
            with httpx.Client(timeout=60.0) as c:
                c.request(
                    method="POST",
                    url=f"{HERMES_API}/v1/chat/completions",
                    headers={
                        "Content-Type": "application/json",
                        "Authorization": f"Bearer {GATEWAY_KEY}",
                        "X-Hermes-Session-Id": session_id,
                    },
                    content=body,
                )
        except Exception:
            pass
    
    threading.Thread(target=archive_to_hermes, daemon=True).start()
    
    # ── Fast path: stream from OpenRouter directly ──
    async with _httpx_async_client(120.0, use_proxy=True) as client:
        resp = await client.request(
            method="POST",
            url=f"{OPENROUTER_URL}/chat/completions",
            headers={
                "Authorization": f"Bearer {OPENROUTER_KEY}",
                "Content-Type": "application/json",
                "Accept": "text/event-stream",
            },
            content=body,
        )
        return StreamingResponse(
            resp.aiter_bytes(),
            status_code=resp.status_code,
            headers={k: v for k, v in resp.headers.items()
                     if k.lower() not in ("transfer-encoding", "content-encoding")},
            media_type=resp.headers.get("content-type", "application/json"),
        )


# ══════════════════════════════════════════════════════════════════════
#  Hermes API 反向代理 (绕过 Cloudflare Access)
# ══════════════════════════════════════════════════════════════════════

import httpx

HERMES_API = "http://127.0.0.1:8642"

@app.api_route("/v1/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"])
async def proxy_hermes_v1(request: Request, path: str):
    """代理 /v1/* 到 Hermes API Server"""
    body_bytes = await request.body()
    # 拦截 APP 发来的审批回复 POST，同步通知等待中的 _chat_stream_handler
    if request.method == "POST" and path.startswith("runs/") and path.endswith("/approval"):
        try:
            body_data = json.loads(body_bytes) if body_bytes else {}
            choice = body_data.get("choice", body_data.get("approval", "deny"))
            run_id = path.split("/")[1]
            pending = _pending_approvals.get(run_id)
            if pending is not None and not pending.get("expired"):
                pending["result"] = choice
                pending["event"].set()
                print(f"[Proxy] 审批拦截同步: run={run_id[:12]}, choice={choice}")
            elif pending is not None:
                # S42：已超时的记录 —— 交给下面的代理请求直接补交 Gateway
                _pending_approvals.pop(run_id, None)
                print(f"[Proxy] 审批为超时记录，交由代理补交: run={run_id[:12]}", flush=True)

            # ── 字段规范化（P4 修复，2026-10-25）──────────────────────────
            # 背景：APP 发的是 {"approval": "once"}，而 Gateway v0.21.3+ 的
            #   _handle_run_approval 只读 body["choice"]：
            #       raw_choice = str(body.get("choice", "")).strip().lower()
            #   读不到就落空 -> code=invalid_approval_choice -> HTTP 400
            #   （用户侧表现：「授权提交失败：HTTP 400」）
            # 以前能用是因为走的是旧的自写补丁，那个补丁认 approval 字段；
            #   v0.21.7 原生实现只认 choice。
            # 修法：在转发前统一成 {"choice": ...}，APP 端无需改动、无需重编译。
            #
            # 同时做中文 choice 映射 —— APP 某些入口会传中文按钮文案，
            #   Gateway 的 _APPROVAL_CHOICE_ALIASES 不含中文，不映射同样 400。
            _CN_CHOICE = {
                "同意本次": "once", "允许一次": "once", "同意": "once", "允许": "once",
                "同意会话": "session", "本次会话": "session", "本次允许": "session",
                "总是同意": "always", "始终允许": "always", "总是允许": "always",
                "拒绝": "deny", "禁止": "deny", "不同意": "deny", "拒绝本次": "deny",
            }
            choice = _CN_CHOICE.get(str(choice).strip(), choice)
            _norm_body = {"choice": choice}
            # 透传这几个 Gateway 认识的附加字段（room-scoped / 批量放行场景用）
            for _k in ("request_id", "all", "resolve_all"):
                if body_data.get(_k) is not None:
                    _norm_body[_k] = body_data[_k]
            body_bytes = json.dumps(_norm_body).encode("utf-8")
        except Exception as _norm_e:
            print(f"[Proxy] 审批 body 规范化失败（按原样转发）: {_norm_e}", flush=True)

    async with httpx.AsyncClient(timeout=1800.0) as client:
        url = f"{HERMES_API}/v1/{path}"
        if request.query_params:
            url += f"?{request.query_params}"
        headers = dict(request.headers)
        headers.pop("host", None)
        # ⚠️ 必须剔掉 content-length：上面的审批分支会把 body 规范化成
        #   {"choice": ...}，长度与原 body 不同。若不删，httpx 会用这个旧值
        #   声明长度却发送新 body → h11.LocalProtocolError:
        #   "Too little data for declared Content-Length" → HTTP 500。
        # 同理 content-encoding：body 已被替换成明文 JSON，不能再声称是压缩的。
        headers.pop("content-length", None)
        headers.pop("content-encoding", None)
        resp = await client.request(
            method=request.method, url=url, headers=headers,
            content=body_bytes, follow_redirects=False,
        )
        return StreamingResponse(
            resp.aiter_bytes(),
            status_code=resp.status_code,
            headers={k: v for k, v in resp.headers.items() if k.lower() not in ("transfer-encoding", "content-encoding")},
            media_type=resp.headers.get("content-type", "application/json"),
        )

@app.api_route("/api/jobs", methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"])
async def proxy_hermes_jobs_root(request: Request):
    """代理 /api/jobs 到 Hermes API Server"""
    async with httpx.AsyncClient(timeout=30.0) as client:
        url = f"{HERMES_API}/api/jobs"
        if request.query_params:
            url += f"?{request.query_params}"
        headers = dict(request.headers)
        headers.pop("host", None)
        body = await request.body()
        resp = await client.request(
            method=request.method, url=url, headers=headers,
            content=body, follow_redirects=False,
        )
        return StreamingResponse(
            resp.aiter_bytes(),
            status_code=resp.status_code,
            headers={k: v for k, v in resp.headers.items() if k.lower() not in ("transfer-encoding", "content-encoding")},
            media_type=resp.headers.get("content-type", "application/json"),
        )

@app.api_route("/api/jobs/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"])
async def proxy_hermes_jobs(request: Request, path: str = ""):
    """代理 /api/jobs/* 到 Hermes API Server"""
    async with httpx.AsyncClient(timeout=30.0) as client:
        url = f"{HERMES_API}/api/jobs/{path}" if path else f"{HERMES_API}/api/jobs"
        if request.query_params:
            url += f"?{request.query_params}"
        headers = dict(request.headers)
        headers.pop("host", None)
        body = await request.body()
        resp = await client.request(
            method=request.method, url=url, headers=headers,
            content=body, follow_redirects=False,
        )
        return StreamingResponse(
            resp.aiter_bytes(),
            status_code=resp.status_code,
            headers={k: v for k, v in resp.headers.items() if k.lower() not in ("transfer-encoding", "content-encoding")},
            media_type=resp.headers.get("content-type", "application/json"),
        )


# ══════════════════════════════════════════════════════════════════════
#  WebSocket 推送 (长连接，实时通知)
# ══════════════════════════════════════════════════════════════════════

import asyncio
from fastapi import WebSocket, WebSocketDisconnect

class ConnectionManager:
    """WebSocket 连接管理器"""
    def __init__(self):
        self.active_connections: dict[str, WebSocket] = {}
        self.lock = asyncio.Lock()

    async def connect(self, client_id: str, websocket: WebSocket):
        await websocket.accept()
        async with self.lock:
            old = self.active_connections.get(client_id)
            self.active_connections[client_id] = websocket
        # 主动关闭被顶替的旧连接：否则旧连接的 except 分支稍后会跑 disconnect，
        # 把刚注册的新连接从字典里误删（注册表与实际活跃连接脱钩 -> 收不到广播）
        if old is not None and old is not websocket:
            try:
                await old.close(code=1000, reason="replaced by newer connection")
            except Exception:
                pass

    async def disconnect(self, client_id: str, websocket: WebSocket = None):
        async with self.lock:
            current = self.active_connections.get(client_id)
            # 只有当前注册的就是自己时才摘除；否则说明已被新连接顶替，不能动
            if websocket is None or current is websocket:
                self.active_connections.pop(client_id, None)

    async def push(self, client_id: str, message: dict) -> bool:
        # V4-S3: 返回 True=送达当前连接；False=无连接或发送失败（调用方计数 push_failures）
        async with self.lock:
            ws = self.active_connections.get(client_id)
        if ws:
            try:
                await ws.send_json(message)
                return True
            except Exception:
                # 传捕获到的 ws（不能重读字典，否则会误摘重连后的新连接）
                await self.disconnect(client_id, ws)
                return False
        return False

    async def broadcast(self, message: dict):
        async with self.lock:
            connections = list(self.active_connections.items())
        for cid, ws in connections:
            try:
                await ws.send_json(message)
            except Exception:
                await self.disconnect(cid)

manager = ConnectionManager()

@app.websocket("/ws/push")
async def websocket_push(websocket: WebSocket):
    """WebSocket 长连接：接收实时推送 + 流式聊天"""
    client_id = websocket.query_params.get("client_id", f"anon-{id(websocket)}")
    await manager.connect(client_id, websocket)
    try:
        # 发送连接成功确认
        await websocket.send_json({"type": "connected", "client_id": client_id})
        # 自动补推未读推送消息（增量：只推上次重连之后的新消息）
        try:
            last_id_file = Path(os.path.expanduser("~/.hermes/last_auto_push_id.txt"))
            last_id = 0
            if last_id_file.exists():
                try:
                    last_id = int(last_id_file.read_text().strip())
                except ValueError:
                    pass

            db = sqlite3.connect(str(PUSH_DB))
            unread = db.execute(
                "SELECT id, title, body, source, created_at FROM push_messages "
                "WHERE id > ? ORDER BY id ASC LIMIT 50",
                (last_id,)
            ).fetchall()
            db.close()

            for row in unread:
                await websocket.send_json({
                    "type": "notification",
                    "id": row[0],
                    "title": row[1],
                    "body": row[2],
                    "source": row[3],
                    "created_at": row[4],
                })

            if unread:
                new_last_id = unread[-1][0]
                last_id_file.parent.mkdir(parents=True, exist_ok=True)
                last_id_file.write_text(str(new_last_id))
                print(f"[WS] Auto-pushed {len(unread)} new notifications (id {unread[0][0]}→{new_last_id}) to {client_id}")
        except Exception as e:
            print(f"[WS] Auto-push error: {e}")
        # 保持连接，等待消息
        while True:
            _t_recv = time.time()
            data = await websocket.receive_json()
            print(f"[WS-DIAG] recv from {client_id}: type={data.get('type')} "
                  f"keys={list(data.keys())} "
                  f"session_id={data.get('session_id')!r} "
                  f"active_keys={list(active_streams.keys())} "
                  f"t={_t_recv:.3f}", flush=True)
            # 客户端发来的ping保活
            if data.get("type") == "ping":
                await websocket.send_json({"type": "pong"})
            # 授权确认回复
            elif data.get("type") == "approve_response":
                run_id = data.get("run_id", "")
                choice = data.get("choice", "deny")
                print(f"[WS] 授权回复 from {client_id}: run={run_id[:12] if run_id else '?'}, choice={choice}")
                pending = _pending_approvals.get(run_id)
                if pending is not None and not pending.get("expired"):
                    # 正常路径：等待循环还活着，后台唤醒即可
                    pending["result"] = choice
                    pending["event"].set()
                else:
                    # 没有 pending，或 pending 已超时（等待循环早已退出）→
                    # S42：直连 Gateway 补交。只要 Gateway 侧的 run 仍在等审批就能成功，
                    #      这正是"事后在审批历史里继续执行"的落地路径。
                    if pending is not None:
                        _pending_approvals.pop(run_id, None)
                    try:
                        async with httpx.AsyncClient(timeout=10.0) as ac:
                            _r = await ac.post(
                                f"{HERMES_API}/v1/runs/{run_id}/approval",
                                json={"choice": choice},
                                headers={"Authorization": f"Bearer {GATEWAY_KEY}" if GATEWAY_KEY else ""},
                            )
                            print(f"[WS] 事后审批补交: choice={choice}, HTTP {_r.status_code} (run={run_id[:12]})", flush=True)
                    except Exception as e:
                        print(f"[WS] 直接审批回复失败: {e}")
            # 流式聊天
            elif data.get("type") == "chat":
                messages = data.get("messages", [])
                # 图片标记 → 原生视觉内容块（DeepSeek V4.1 Flash 识图）。
                # APP 的「图片」菜单只能发 `[图片] /api/upload/x.jpg` 纯文本，
                # 这里还原成真的图片块；没有标记时零开销直接返回。
                messages, n_img = _inline_image_markers(messages)
                if n_img:
                    print(f"[WS] 已内联 {n_img} 张图片", flush=True)
                # 模型：APP 消息里显式带 → 用它；否则用 APP 本地覆盖；都没有 → 全局默认。
                # 两者都靠 Gateway 的 model_routes 解析成真实 provider+model，
                # per-request 生效，不动全局 config.yaml。
                req_model = str(data.get("model") or "").strip()
                if not req_model:
                    req_model = _load_app_model().get("id") or ""
                # S12: session_id 现在是「会话 ID」（不再等于 device_id）。
                #   有值 -> 续接该会话；空 -> 新建，并用 chat_started 回执告知 APP。
                session_id = data.get("session_id") or ""
                if not session_id:
                    session_id = "app-" + uuid.uuid4().hex[:16]
                    print(f"[WS] 新建会话 {session_id} (client={client_id})", flush=True)
                print(f"[WS] 流式聊天 from {client_id}, session={session_id}, messages={len(messages)}", flush=True)

                # 跨端占用保护：APP 往桌面/WebUI/CLI 的会话里发消息时，如果那一端正在跑 turn，
                # 两条回复会写进同一个会话（Gateway 对同一 session 串行，后一条会读到前一条的流）。
                # APP 自己创建的会话（app-*）由下面的 active_streams / 幂等表接管，不在这里拦。
                if not session_id.startswith("app-"):
                    _busy, _desc = _session_busy(session_id)
                    if _busy:
                        print(f"[WS] 会话占用中，拒绝发送 session={session_id} desc={_desc}", flush=True)
                        await websocket.send_json({
                            "type": "chat_error",
                            "session_id": session_id,
                            "message": f"这个会话正在另一端执行（{_desc}），等它跑完再发",
                        })
                        continue

                # V4-S13: 双轨防护（N4）——有 client_msg_id 走幂等表（跨连接/重启都拦得住）；
                #   老 APP 无该字段 → 保留原「同一连接防重复」防护（灰度期不裸奔）。
                client_msg_id = str(data.get("client_msg_id") or "").strip()
                if client_msg_id:
                    _fp = _chat_fingerprint(session_id, messages)
                    _outcome, _prev = idem_lookup(client_msg_id, _fp)
                    if _outcome == "conflict":
                        print(f"[WS] 幂等 conflict（同 id 不同内容）key={client_msg_id}", flush=True)
                        await websocket.send_json({
                            "type": "chat_error",
                            "session_id": session_id,
                            "message": "消息内容与ID不匹配，请重新发送",
                        })
                        continue
                    if _outcome == "reused":
                        _st = (_prev or {}).get("status")
                        if _st == "processing":
                            print(f"[WS] 幂等拒绝（processing 中）key={client_msg_id}", flush=True)
                            await websocket.send_json({
                                "type": "chat_error",
                                "session_id": session_id,
                                "message": "上一条还在回复中，请等它说完再发",
                            })
                            continue
                        # reused + 终态：告知已处理，不重复执行（内容补推本轮不做）
                        print(f"[WS] 幂等命中终态（不重跑）key={client_msg_id} status={_st}", flush=True)
                        await websocket.send_json({
                            "type": "chat_started",
                            "session_id": session_id,
                            "duplicate": True,
                        })
                        continue
                    # missing 或 stale（N2 超时放行）→ 抢锁
                    if _outcome == "stale":
                        idem_delete_stale(client_msg_id)
                    _res, _ = idem_reserve(client_msg_id, _fp,
                                           run_id=session_id or client_msg_id)
                    if _res == "reused":
                        await websocket.send_json({
                            "type": "chat_error",
                            "session_id": session_id,
                            "message": "该消息正在处理中",
                        })
                        continue
                else:
                    # S12 修（保留）：同一连接上同一会话已有活跃流 → 拒绝，不要双双送去 Gateway。
                    # 背景：Gateway 对同一 session 是串行的，后一条请求会读到前一条的响应，
                    # 造成回复内容交叉。若旧流属于**别的连接**（断线重连场景）则放行 ——
                    # 那种情况由 handler 里的 superseded 机制安全接管。
                    _cur = active_streams.get(session_id)
                    if (_cur is not None and _cur.get("status") == "processing"
                            and _cur.get("ws") is websocket):
                        print(f"[WS] 拒绝并发 chat（同一连接已有活跃流）session={session_id}", flush=True)
                        await websocket.send_json({
                            "type": "chat_error",
                            "session_id": session_id,
                            "message": "上一条还在回复中，请等它说完再发",
                        })
                        continue

                # 先回执，保证 APP 在第一个 token 之前就知道归属会话
                await websocket.send_json({
                    "type": "chat_started",
                    "session_id": session_id,
                })
                await _chat_stream_handler(client_id, websocket, messages, session_id, req_model,
                                           client_msg_id)
            # 同步挂起缓存的消息回复（支持断点续流）
            elif data.get("type") == "sync_pending":
                session_id = data.get("session_id")
                last_seq = data.get("last_seq", -1)
                if session_id:
                    try:
                        # 优先检查活跃流（仍在处理中或刚完成的断点续传）
                        stream = active_streams.get(session_id)
                        _st = stream.get("status") if stream is not None else None
                        # V4-S10: 按 my_stream["status"] 拆档。
                        #   processing       → 续等（无 token）/ 续传（有 token）
                        #   complete         → 回放 + chat_done + 写缓存（内容完整）
                        #   superseded/error/timeout/无流 → 只走 offline_chats 兜底或 stream_missing，
                        #     绝不回放半截 token / 不写缓存（M4：半截回复污染）
                        if stream is not None and _st == "processing" and not stream["tokens"]:
                            # 流已注册但还没有 token（刚起步/上游还在思考）。
                            # 此处绝不能 fallback 到 offline_chats —— 那张表以
                            # session_id(= device_id) 为主键，同一设备共用一条记录，
                            # 返回的会是上一轮对话的回复（张冠李戴）。
                            await websocket.send_json({
                                "type": "sync_pending_response",
                                "session_id": session_id,
                                "content": "",
                            })
                            print(f"[Resume] Stream registered but no tokens yet for {session_id}")
                        elif stream and stream["tokens"] and _st in ("processing", "complete"):
                            # 使用服务端记录的已同步序列号防止重复推送（sync retry 会带相同的 last_seq）
                            stream.setdefault("last_synced_seq", -1)
                            effective_seq = max(last_seq, stream["last_synced_seq"])
                            # 发送所有错过的 token（含离线期间生成的）
                            missed = [t for t in stream["tokens"] if t["seq"] > effective_seq]
                            for t in missed:
                                await websocket.send_json({
                                    "type": "chat_token",
                                    "session_id": session_id,
                                    "seq": t["seq"],
                                    "content": t["content"],
                                })
                            # 更新已同步序列号，防止后续 sync retry 重复推送
                            if missed:
                                stream["last_synced_seq"] = max(
                                    stream["last_synced_seq"],
                                    max(t["seq"] for t in missed)
                                )
                            
                            if stream["status"] == "processing":
                                # 仍在处理中 → 通知客户端继续等待实时流
                                await websocket.send_json({
                                    "type": "sync_resume",
                                    "session_id": session_id,
                                    "status": "streaming",
                                    "caught_up": len(missed),
                                })
                                print(f"[Resume] Stream still active for {session_id}, replayed {len(missed)} tokens")
                            else:
                                # 已完成 → 先补 stream_end 再发结束标记
                                # [V2-3.3] 断线时该帧必丢，补投路径必须带上它，
                                #   否则 APP 的工具进度条永远清不掉（#IKM3HX）。
                                await websocket.send_json({
                                    "type": "chat_progress",
                                    "status": "stream_end",
                                    "session_id": session_id,
                                })
                                # 补投的结束帧必须带上本轮指标：否则它与实时帧竞争时，
                                #   先到的这一条（无 metrics）会把消息建好，指标永久丢失。
                                await websocket.send_json({
                                    "type": "chat_done",
                                    "session_id": session_id,
                                    "metrics": stream.get("metrics"),
                                })
                                # 同时写入离线缓存（完整回复）
                                if stream["full_response"]:
                                    db = sqlite3.connect(str(PUSH_DB))
                                    db.execute(
                                        "INSERT OR REPLACE INTO offline_chats (session_id, content, retention_until) VALUES (?, ?, ?)",
                                        (session_id, stream["full_response"], time.time() + 600)
                                    )
                                    db.commit()
                                    db.close()
                                # 清理活跃流（保留一段时间供重复查询，30分钟后由定期清理任务处理）
                                print(f"[Resume] Stream completed for {session_id}, replayed {len(missed)} tokens")
                        else:
                            # 回退：检查离线缓存
                            db = sqlite3.connect(str(PUSH_DB))
                            row = db.execute("SELECT content FROM offline_chats WHERE session_id = ?", (session_id,)).fetchone()
                            if row:
                                content = row[0]
                                # [V2-3.3] 同上：这条路径（流已清理、只剩离线缓存）
                                #   原先完全不带 stream_end，进度条照样残留。
                                await websocket.send_json({
                                    "type": "chat_progress",
                                    "status": "stream_end",
                                    "session_id": session_id,
                                })
                                await websocket.send_json({
                                    "type": "sync_pending_response",
                                    "session_id": session_id,
                                    "content": content,
                                })
                                db.execute("DELETE FROM offline_chats WHERE session_id = ?", (session_id,))
                                db.commit()
                                print(f"[Offline] Synced and deleted cached response for {session_id}")
                            else:
                                # [V2-3.6] 跨会话回退已移除：原「回退2（LIKE 'app-%' 前缀匹配）」与
                                #   「回退3（全局最近一条缓存）」会取到**其他会话**的回复，取走后还会 DELETE
                                #   掉那个会话的缓存 —— 既张冠李戴（把 B 的回复推给 A），又造成 B 的数据丢失。
                                #   本会话缓存已在上面按严格全等查过；拿不到就如实回 stream_missing。
                                # 没有任何活跃流、也没有离线缓存 → 明确告知客户端"别再等了"。
                                # 此前这里与「流刚起步还没 token」一样只回 content:""，
                                # 客户端无法区分二者：重试 60 秒计时器耗尽后 isStreaming 仍是 true，
                                # 占位符「恢复中」永远不消失（用户实测：会话一直卡住不恢复）。
                                await websocket.send_json({
                                    "type": "sync_pending_response",
                                    "session_id": session_id,
                                    "content": "",
                                    "stream_missing": True,
                                })
                                print(f"[Resume] No active stream & no cache for {session_id} -> stream_missing")
                            db.close()
                    except Exception as sync_err:
                        print(f"[Offline] Sync pending error: {sync_err}")
    except WebSocketDisconnect:
        await manager.disconnect(client_id, websocket)
    except RuntimeError:
        # 客户端连接已关闭但未走标准断开流程
        await manager.disconnect(client_id, websocket)


# ═══════════════════════════════════════════════════════════════════════════
# S41：每轮对话的性能指标（TTFT / TPS / KV 命中）
#
# 数据来源两类，各取其长：
#   1) **TTFT 自己算** —— WSL 的 Hermes v0.18.0 没有 on_stream_delta 钩子，
#      perf_metrics.jsonl 的 ttft_* 字段全是 null（实测 1306 行 0% 有值）。
#      xiaoq-api 自己从"请求发出 → 首个 token 到达"计时，口径与用户感知一致。
#   2) **TPS / KV 命中读 jsonl** —— tps_wall、cache_hit_rate、*_tokens 有值率 100%。
#      按 session_id + 时间窗(lookback)圈出本轮的行再聚合。
#
# ⚠️ 不用 turn_id 划轮：同一轮的每次工具调用都会生成新的 turn_id 后缀，
#    拿它当键会把一轮拆成很多"轮"。
# ═══════════════════════════════════════════════════════════════════════════

PERF_JSONL = Path.home() / ".hermes" / "logs" / "perf_metrics.jsonl"


def _read_jsonl_rows(path: Path, since: float, until: float, session_id: str) -> list:
    """流式读取 jsonl，取 (since <= ts <= until) 且 session_id 匹配的行。

    文件可能上 MB，用逐行 + 提前跳过的方式读，不做全量 json.load。
    """
    rows = []
    if not path.exists():
        return rows
    try:
        with path.open("r", encoding="utf-8", errors="replace") as f:
            for line in f:
                # 快速预筛：先看时间戳字段，避免对每行都 json.loads
                if '"ts"' not in line:
                    continue
                try:
                    r = json.loads(line)
                except Exception:
                    continue
                ts = r.get("ts")
                if not isinstance(ts, (int, float)):
                    continue
                if ts < since or ts > until:
                    continue
                if session_id and str(r.get("session_id", "")) != session_id:
                    continue
                rows.append(r)
    except Exception as e:
        print(f"[Metrics] read jsonl failed: {e}", flush=True)
    return rows


def _collect_turn_metrics(session_id: str, ttft_ms, started_wall: float) -> dict:
    """聚合本轮指标。返回给 APP 的 dict（字段缺失时省略，APP 会自动不显示那一项）。"""
    m = {}
    if isinstance(ttft_ms, (int, float)) and ttft_ms > 0:
        m["ttft_ms"] = round(float(ttft_ms), 1)

    # 时间窗：本轮开始往前留 120s 余量（覆盖 Hermes 侧写 jsonl 的少量延迟），
    # 往后留 10s 容错。窗口内可能混入同一会话上一轮的尾巴 —— 用 session_id 已过滤一次，
    # 剩下的靠"只看 api_call_count 递增序列"无法完全消除，所以窗口取紧一点更稳。
    rows = _read_jsonl_rows(PERF_JSONL, since=started_wall - 5.0,
                            until=time.time() + 10.0, session_id=session_id)
    if not rows:
        m["calls"] = 0
        return m

    # ⚠️ 划轮方式（踩过一次坑）：
    #   最初用"逐个回溯找 api_call_count==1"——结果两轮数据被混在一起
    #   （实测同一 session 两次不同 started_wall 得到完全相同的 calls=8）。
    #   正解：用本轮的 request_started_at 做下限。本轮每次调用（含工具轮）
    #   都在本轮开始之后才发出，所以 >= started_wall 的行才是本轮的。
    turn_rows = [r for r in rows
                 if isinstance(r.get("request_started_at"), (int, float))
                 and r["request_started_at"] >= started_wall - 3.0]
    turn_rows.sort(key=lambda r: r.get("request_started_at") or 0)

    out_tokens = sum(int(r.get("output_tokens") or 0) for r in turn_rows)
    cache_read = sum(int(r.get("cache_read_tokens") or 0) for r in turn_rows)
    prompt_tok = sum(int(r.get("prompt_tokens") or 0) for r in turn_rows)
    input_tok = sum(int(r.get("input_tokens") or 0) for r in turn_rows)

    # 本轮墙钟时长：从第一行 request_started_at 到最后一行 request_ended_at
    starts = [r.get("request_started_at") for r in turn_rows if isinstance(r.get("request_started_at"), (int, float))]
    ends = [r.get("request_ended_at") for r in turn_rows if isinstance(r.get("request_ended_at"), (int, float))]
    if starts and ends:
        span = max(ends) - min(starts)
        # 纯解码 TPS 优先用 decode_s；WSL 无该字段 → 用 span 近似（含工具轮空窗，偏保守）
        m["tps"] = round(out_tokens / span, 1) if (span > 0 and out_tokens > 0) else None
        m["span_s"] = round(span, 2)
    elif out_tokens > 0:
        # 只有单行时用 api_duration_s
        d = sum(float(r.get("api_duration_s") or 0) for r in turn_rows)
        m["tps"] = round(out_tokens / d, 1) if d > 0 else None

    if prompt_tok > 0:
        m["kv_hit"] = round(cache_read / prompt_tok, 4)

    m["calls"] = len(turn_rows)
    m["tokens_in"] = input_tok
    m["tokens_out"] = out_tokens
    m["cache_read"] = cache_read
    return m


async def _chat_stream_handler(client_id: str, websocket: WebSocket, messages: list, session_id: str,
                               model: str = "", client_msg_id: str = ""):
    """通过 Hermes API 流式处理聊天，结果通过 WebSocket 推送

    `model` = APP 侧选中的模型别名（来自 /api/models 的 id）。
    Gateway 的 model_routes 会按这个字段解析出真实的 provider + model，
    做到 per-request 切模型，而**不动全局 model.default** —— 所以 WebUI /
    Desktop / CLI 仍走各自的默认模型，互不干扰。
    空串 = 用全局默认（"current" 是 Gateway 的兜底值）。
    """
    # S12 修：同一 session 只允许一个活跃流。
    # 断线重连时旧连接的 handler 可能仍在跑（await 未结束），新连接又发起一个 ——
    # 若不处理，下面那行赋值会把旧流的对象直接丢掉，而旧流后续仍按 session_id
    # 动态查找 active_streams，于是写进【新流的对象】→ 内容交叉、seq 错乱。
    prev_stream = active_streams.get(session_id)
    # ⚠️ 只对「正在推 token」的旧流做 superseded。
    #   「等审批」的流同样处于 processing 且可能挂 300 秒，但那是**合法状态** ——
    #   把它杀掉会导致用户稍后批准的结果被丢弃。真正需要防的是两条流同时推 token
    #   互相覆盖对方的内容。
    if (prev_stream is not None
            and prev_stream.get("status") == "processing"
            and not prev_stream.get("awaiting_approval", False)):
        prev_stream["status"] = "superseded"
        # 方案C：主动取消旧流的 task。只标记 superseded 是**不够的** ——
        #   旧 handler 仍会继续消费 Gateway 的 SSE，占着同 session 的串行槽，
        #   导致新请求的响应被抢走/混流。实测：APP 重启后旧 handler 发 17 次必失败。
        _old_task = prev_stream.get("task")
        if _old_task is not None and not _old_task.done():
            _old_task.cancel()
            print(f"[WS] session {session_id} 旧流 task 已取消（释放 Gateway 串行槽）", flush=True)
        print(f"[WS] session {session_id} 已有活跃流（推流中），旧的标记为 superseded", flush=True)
    elif prev_stream is not None and prev_stream.get("awaiting_approval", False):
        print(f"[WS] session {session_id} 旧流正在等审批，不做 superseded（保留其结果）", flush=True)

    # 本次流的对象用**局部引用**贯穿始终（不再按 session_id 动态查找）
    # 方案C：记录本流所属的 asyncio task —— 新流据此可主动取消旧流，
    #   释放 Gateway「同 session 串行」的槽位（否则旧流会一直占着，
    #   新请求的响应可能被旧 handler 抢走/混流）。
    my_stream = {"seq": 0, "tokens": [], "full_response": "", "status": "processing",
                 "ws": websocket, "task": asyncio.current_task()}
    active_streams[session_id] = my_stream
    
    # 先发一个思考状态，告诉用户正在处理
    try:
        await websocket.send_json({"type": "chat_thinking", "session_id": session_id})
    except Exception:
        pass

    # S41：本轮计时起点（TTFT 的分母）。要在 payload 发出前记，才是用户感知的口径。
    _turn_t0 = time.monotonic()
    _turn_started_wall = time.time()
    _ttft_ms = None

    payload = {
        "model": model or "current",
        "stream": True,
        "messages": messages,
    }
    client_connected = True
    full_response = ""
    # V4-S1: 粘性失败计数 + 成功帧计数（S2 末尾判据用）
    push_failures = 0
    push_frames = 0

    async def _push(frame: dict) -> bool:
        """V4-S4: 统一走 manager.push（按 client_id 寻址 = S11 动态推送目标），并计数。
        粘性语义：push_failures 只增不减；任一帧失败即客户端未在线到流结束 → 末尾写离线缓存。"""
        nonlocal push_failures, push_frames, client_connected
        try:
            ok = await manager.push(client_id, frame)
        except Exception:
            ok = False
        if ok:
            push_frames += 1
        else:
            push_failures += 1
            client_connected = False
        return ok
    try:
        has_tokens = False
        async with asyncio.timeout(10800):  # 硬上限3小时（20261015 由 1h 提到 3h：1h 闸门曾掐断 app-3029 的整轮长任务）：只防死循环，不掐正常长任务
            async with httpx.AsyncClient(timeout=300.0) as client:
                async with client.stream(
                    "POST",
                    f"{HERMES_API}/v1/chat/completions",
                    json=payload,
                    headers={
                        "Content-Type": "application/json",
                        "Authorization": f"Bearer {GATEWAY_KEY}" if GATEWAY_KEY else "",
                        "X-Hermes-Session-Id": session_id,
                    },
                ) as resp:
                    print(f"[WS-PUSH] gateway responded status={resp.status_code}", flush=True)
                    if resp.status_code != 200:
                        error_body = await resp.aread()
                        print(f"[WS] Hermes API error {resp.status_code}: {error_body[:200]}")
                        try:
                            await _push({"type": "chat_error", "session_id": session_id, "message": f"API error {resp.status_code}"})
                        except Exception:
                            pass
                        active_streams.pop(session_id, None)
                        return

                    buffer = ""
                    last_token_time = time.monotonic()
                    SSE_IDLE_TIMEOUT = 120  # 120秒无SSE数据判定断流（原45s对长思考/复杂工具偏紧）
                    ait = resp.aiter_bytes().__aiter__()
                    _turn_tool_order: dict = {}   # S45-A2: toolCallId -> 本轮序号（整条流只初始化一次）
                    while True:
                        # 方案1（2026-09-24 修）：客户端断开**不再**终止上游消费。
                        #   旧版（方案B）在这里 break = 主动关掉 Gateway 的上游请求，
                        #   正在跑的工具会被 SIGINT（实测 exit_code=130）——
                        #   退后台断一次连接就杀掉整轮任务，代价太大。
                        #   现在断线后继续消费 SSE，token 照常进 my_stream 缓存
                        #   （推不出去就不推），函数末尾统一写 offline_chats +
                        #   Push 通知，回前台由 sync_pending 回放。
                        #   只有本流已被取代/结束（superseded / timeout / error）才退出。
                        if my_stream.get("status") != "processing":
                            print("[WS] 流状态=" + str(my_stream.get("status")) + "，停止消费 SSE session=" + session_id, flush=True)
                            break
                        try:
                            chunk = await asyncio.wait_for(ait.__anext__(), timeout=SSE_IDLE_TIMEOUT)
                        except StopAsyncIteration:
                            break
                        except asyncio.TimeoutError:
                            print(f"[WS] SSE idle timeout ({SSE_IDLE_TIMEOUT}s) for {session_id}, caching current response")
                            # 评审 REV-03：不能对客户端静默，否则用户要硬等 20s 停滞检测才发现流已断。
                            #   ⚠️ 必须在置 client_connected=False **之前**发送，否则自己把自己挡住。
                            #   ⚠️ 用字符串拼接（禁 f-string 嵌套引号 —— 曾导致服务每 5 秒崩溃重启）。
                            # [V2-3.1] 不再看 client_connected：_push 内部走 manager.push 动态寻址，
                            #   重连后的新连接照样收到；无连接则记账，末尾写离线缓存。
                            await _push({
                                "type": "chat_error",
                                "session_id": session_id,
                                "message": "上游响应超时（超过 " + str(SSE_IDLE_TIMEOUT) + "s 无数据），已保存当前进度"
                            })
                            client_connected = False
                            break
                        last_token_time = time.monotonic()
                        text = chunk.decode() if isinstance(chunk, bytes) else str(chunk)
                        buffer += text
                        # SSE events are separated by \n\n
                        while "\n\n" in buffer:
                            event_block, buffer = buffer.split("\n\n", 1)
                            event_type = ""  # track event: line if present
                            data_line = ""
                            for line in event_block.strip().split("\n"):
                                line = line.strip()
                                if line.startswith("event: "):
                                    event_type = line[7:].strip()
                                elif line.startswith("data: "):
                                    data_line = line[6:].strip()
                            # ── S45-A：工具进度（Milo: 让 APP 在长工具轮期间有反馈）──
                            # Gateway v0.21.3 会把它作为**独立 SSE 帧**发出（不在 delta 里）：
                            #   event: hermes.tool.progress
                            #   data: {"tool":"terminal","label":"ls /tmp + 2 commands",
                            #          "status":"running","toolCallId":"call_...","emoji":"💻"}
                            # 字段名以实测为准（见 skill/V2 文档），不要用 name/input_summary。
                            if event_type == "hermes.tool.progress" and data_line:
                                try:
                                    _tp = json.loads(data_line)
                                    _cid = str(_tp.get("toolCallId", ""))
                                    if _cid and _cid not in _turn_tool_order:
                                        _turn_tool_order[_cid] = len(_turn_tool_order) + 1
                                    _tp["index"] = _turn_tool_order.get(_cid, 0)
                                    _tp["total"] = len(_turn_tool_order)
                                    # 方案1：进度日志无条件打（断线期间也要留痕，用来确认流仍在消费）
                                    _st = str(_tp.get("status"))
                                    _tl = str(_tp.get("tool"))
                                    _lb = str(_tp.get("label"))[:40]
                                    # [V2-3.1] 文案改用 push_failures：client_connected 拆闸门后只剩
                                    #   "曾经失败过"的粘性语义，重连成功后仍会一直显示"已断开"，误导排查。
                                    print("[WS] 工具进度" + ("转发" if push_failures == 0 else "（本轮已有推送失败，仅记录）") + " status=" + _st + " tool=" + _tl + " label=" + _lb + " [" + str(_tp.get("index")) + "/" + str(_tp.get("total")) + "]", flush=True)
                                    # [V2-3.1] 去闸门：断线重连后进度照推
                                    await _push({
                                        "type": "chat_progress",
                                        "tool": _tp.get("tool", ""),
                                        "label": _tp.get("label", ""),
                                        # running / completed
                                        "status": _tp.get("status", ""),
                                        # 并发场景必需：同名工具的多个调用靠它区分
                                        "toolCallId": _tp.get("toolCallId", ""),
                                        "emoji": _tp.get("emoji", ""),
                                        "index": _tp.get("index", 0),
                                        # 评审 REV-04：跨会话隔离必需 —— 客户端据此丢弃非当前会话的进度
                                        "session_id": session_id,
                                    })
                                except Exception as _e:
                                    # 方案1：发送失败只代表**这个 socket** 死了（客户端断开），
                                    #   不再终止本轮 —— 循环顶部已不再因 client_connected 退出。
                                    print(f"[WS] tool.progress 转发失败: {_e} —— 标记断开，本轮继续消费上游（方案1）", flush=True)
                                    client_connected = False

                            # Handle approval.request events
                            if event_type == "approval.request" and data_line:
                                try:
                                    approval_evt = json.loads(data_line)
                                    run_id = approval_evt.get("run_id", "")
                                    # 方案1：注册与推送解耦 —— 断线也必须注册 pending，
                                    #   否则 APP 回前台在「审批历史」里也找不到这张卡，
                                    #   用户批了也没人接（proxy 拦截找不到 entry）。
                                    if run_id:
                                        # Set up event so proxy/WebSocket handler can signal us
                                        approval_event = asyncio.Event()
                                        _pending_approvals[run_id] = {
                                            "event": approval_event,
                                            "result": None,
                                            "session_id": session_id,
                                            # S42：带上完整上下文，供事后恢复时重建卡片
                                            "command": approval_evt.get("command", ""),
                                            "description": approval_evt.get("description", ""),
                                            "pattern_keys": approval_evt.get("pattern_keys", []),
                                            "created_at": time.time(),
                                            "expired": False,
                                        }
                                        # Forward to APP（方案1：断线时推送会失败 —— 不能让它上抛，
                                        #   否则会落到外层 except Exception 把整轮判成 error。
                                        #   pending 已注册，用户回前台从「审批历史」提交、
                                        #   proxy 拦截照常唤醒下面的轮询循环 → 上游继续跑）
                                        try:
                                            await _push({
                                                "type": "approve_request",
                                                "run_id": run_id,
                                                "command": approval_evt.get("command", ""),
                                                # description：Gateway 给的触发上下文说明（S11 起 APP 会显示）
                                                "description": approval_evt.get("description", ""),
                                                "pattern_keys": approval_evt.get("pattern_keys", []),
                                                "choices": approval_evt.get("choices", ["once", "session", "always", "deny"]),
                                                # session_id：哪个会话触发的（S11 新增，便于 APP 显示上下文）
                                                "session_id": session_id,
                                            })
                                        except Exception as _pe:
                                            print(f"[WS] 审批卡片推送失败（客户端已断开）: {_pe} (run={run_id[:12]})", flush=True)
                                        print(f"[WS] 审批请求已注册 (run={run_id[:12]}, session={session_id}, pushed={push_failures == 0})")
                                        # Wait for approval resolution (proxy intercept or WebSocket).
                                        # Poll result flag instead of blocking on asyncio.Event to
                                        # avoid inter-task wakeup races on busy event loops.
                                        # S12 修：标记本流进入"等审批"状态。
                                        # 等审批期间它不应被同一 session 上的新请求 superseded。
                                        my_stream["awaiting_approval"] = True
                                        _wait_start = time.monotonic()
                                        _approval_done = False
                                        while time.monotonic() - _wait_start < 300:
                                            pending = _pending_approvals.get(run_id)
                                            if pending and pending["result"] is not None:
                                                _approval_done = True
                                                choice = pending["result"]
                                                break
                                            await asyncio.sleep(2.0)
                                        my_stream["awaiting_approval"] = False
                                        if _approval_done:
                                            # POST choice back to Gateway (idempotent — Gateway handles duplicate)
                                            try:
                                                async with httpx.AsyncClient(timeout=10.0) as ac:
                                                    await ac.post(
                                                        f"{HERMES_API}/v1/runs/{run_id}/approval",
                                                        json={"choice": choice},
                                                        headers={
                                                            "Authorization": f"Bearer {GATEWAY_KEY}" if GATEWAY_KEY else "",
                                                        },
                                                    )
                                                print(f"[WS] 审批回复已提交: {choice} (run={run_id[:12]})")
                                            except Exception as e:
                                                print(f"[WS] 审批回复提交失败: {e} (run={run_id[:12]})")
                                        else:
                                            # S42：不再"静默丢弃"。标记 expired 但保留记录，
                                            #      让 APP 的「设置-审批历史」能找回并继续审批。
                                            _pexp = _pending_approvals.get(run_id)
                                            if _pexp is not None:
                                                _pexp["expired"] = True
                                                _pexp["expired_at"] = time.time()
                                            print(f"[WS] 审批等待超时 (run={run_id[:12]}) —— 记录已保留，供事后审批", flush=True)
                                except json.JSONDecodeError:
                                    pass
                                finally:
                                    # S42：已正常处理 → 移除；超时（expired）→ **保留**，
                                    #      供用户在审批历史里找回后继续提交。
                                    _pfin = _pending_approvals.get(run_id)
                                    if _pfin is None or not _pfin.get("expired"):
                                        _pending_approvals.pop(run_id, None)
                                continue
                            # Standard OpenAI chat completion data
                            if data_line:
                                raw = data_line
                                if raw == "[DONE]":
                                    # [V2-3.1] 去闸门：结束帧必须发得出去（断线重连后亦然）
                                    try:
                                        # S45-A：先告诉 APP 清理工具进度状态
                                        # （否则某条进度若没等到 completed，进度条会一直转）
                                        await _push({
                                            "type": "chat_progress",
                                            "status": "stream_end",
                                            "session_id": session_id,
                                        })
                                    except Exception:
                                        pass
                                    try:
                                        # S41：结束帧带上本轮指标（不新增帧类型，老客户端自动忽略）
                                        _metrics = _collect_turn_metrics(
                                            session_id, _ttft_ms, _turn_started_wall)
                                        print(f"[Metrics] chat_done metrics={_metrics}", flush=True)
                                        # 指标落进流状态：补投路径的 chat_done 也要带它 ——
                                        #   断线重连时 sync_pending 的补投帧可能先于实时帧到达，而客户端
                                        #   handleDone 会清空 streamContent：先到者建好消息、后到者内容为
                                        #   空被忽略 → 无 metrics 的那条赢了，指标永久丢失（实测 23:03）。
                                        my_stream["metrics"] = _metrics
                                        await _push({
                                            "type": "chat_done",
                                            "session_id": session_id,
                                            "metrics": _metrics,
                                        })
                                    except Exception:
                                        pass
                                elif raw:
                                        try:
                                            evt = json.loads(raw)
                                            choices = evt.get("choices", [{}])
                                            if choices:
                                                delta = choices[0].get("delta", {})
                                                content = delta.get("content", "")
                                                if content:
                                                    # S12 修：被新流取代后立刻停手，避免污染对方的缓存
                                                    if my_stream.get("status") != "processing":
                                                        print(f"[WS] 流已被取代，停止处理 (session_id={session_id})", flush=True)
                                                        break
                                                    # S41：首个 token 到达 → 记 TTFT
                                                    if not has_tokens:
                                                        _ttft_ms = (time.monotonic() - _turn_t0) * 1000.0
                                                        print(f"[Metrics] first token after {_ttft_ms:.0f}ms (session={session_id})", flush=True)
                                                    has_tokens = True
                                                    full_response += content
                                                    # 分配序列号（全部走本次流的局部对象）
                                                    seq = my_stream["seq"]
                                                    my_stream["seq"] = seq + 1
                                                    my_stream["full_response"] = full_response
                                                
                                                    # [V2-3.1] 去闸门（推给"当前"连接）
                                                    _tok_ok = await _push({
                                                        "type": "chat_token",
                                                        "session_id": session_id,
                                                        "seq": seq,
                                                        "content": content,
                                                    })
                                                    if _tok_ok:
                                                        # [V2-3.2] 实时推送处同步水位：否则重连后 sync_pending
                                                        #   会把这批 token 当"漏掉的"再补一遍，而客户端
                                                        #   handleToken 是按序无条件拼接 → 正文重复。
                                                        my_stream["last_synced_seq"] = max(
                                                            my_stream.get("last_synced_seq", -1), seq)
                                                        if seq < 3:
                                                            print(f"[WS-PUSH] token #{seq} pushed ok (len={len(content)})", flush=True)
                                                    elif seq < 3:
                                                        print(f"[WS-PUSH] token #{seq} NOT pushed (no active connection)", flush=True)
                                                    # 无论客户端是否连接，都缓存 token（写入本次流的对象）
                                                    my_stream["tokens"].append({
                                                        "seq": seq,
                                                        "content": content
                                                    })
                                        except json.JSONDecodeError:
                                            pass
        
        # 整个流处理完成
        # V4-S2: 判据从 client_connected 布尔改为粘性失败+帧计数
        #   push_failures>0 = 有帧没送到（客户端未在线到流结束）→ 必写缓存
        #   push_frames==0 = 零帧流（首帧就断）→ 布尔判据同样漏，补上
        if push_failures > 0 or push_frames == 0:
            # 客户端已经断开，将拼好的完整数据存入本地 SQLite
            if full_response:
                try:
                    db = sqlite3.connect(str(PUSH_DB))
                    db.execute(
                        "INSERT OR REPLACE INTO offline_chats (session_id, content, retention_until) VALUES (?, ?, ?)",
                        (session_id, full_response, time.time() + 600)
                    )
                    db.commit()
                    db.close()
                    print(f"[Offline] Successfully cached response for {session_id} ({len(full_response)} chars)")
                    # Batch 2: WS 断线时通过 Push Kit 通知 APP 有新回复
                    try:
                        title = f"💬 小Q回复"
                        preview = full_response[:80].replace('\n', ' ').strip()
                        await _push_to_huawei(title, preview + '...', "chat")
                        print(f"[PushKit] Notified via push for {session_id}")
                    except Exception as push_e:
                        print(f"[PushKit] WS-disconnect push notify failed: {push_e}")
                except Exception as cache_err:
                    print(f"[Offline] Failed to cache response: {cache_err}")
        else:
            if not has_tokens:
                try:
                    _metrics = _collect_turn_metrics(session_id, _ttft_ms, _turn_started_wall)
                    await _push({"type": "chat_done", "session_id": session_id, "metrics": _metrics})
                except Exception:
                    pass
        
        # 标记完成，保留 token 缓冲供后续 sync_pending 查询
        my_stream["status"] = "complete"
        my_stream["completed_at"] = time.time()
    except asyncio.CancelledError:
        # 方案C 配套：本流被新流取消（见 C-2）。做清理后必须 re-raise，
        #   否则 asyncio 会报 "task was destroyed but it is pending"。
        print(f"[WS] 本轮流已被新流取代（task cancelled）session={session_id}", flush=True)
        my_stream["status"] = "superseded"
        my_stream["completed_at"] = time.time()
        # V4-S5: CAS —— 只在全局表仍指向自己时才回写。
        #   被取代时新流 preamble 已写入新对象，此处无条件回写会把新流挤掉（L2895 竞态）。
        if active_streams.get(session_id) is my_stream:
            active_streams[session_id] = my_stream
        raise
    except asyncio.TimeoutError:
        print(f"[WS] Chat stream timeout for {session_id}")
        my_stream["status"] = "timeout"
        my_stream["completed_at"] = time.time()
        my_stream.pop("reconnect_ws", None)
    except Exception as e:
        print(f"[WS] Chat stream error: {e}")
        try:
            await _push({"type": "chat_error", "session_id": session_id, "message": str(e)})
        except Exception:
            pass
        my_stream["status"] = "error"
        my_stream["completed_at"] = time.time()
        my_stream.pop("reconnect_ws", None)
    finally:
        # V4-S14: 幂等终态更新（三出口统一走 finally：complete/superseded/error/timeout）
        if client_msg_id:
            try:
                idem_update_status(client_msg_id, {"status": my_stream.get("status", "error")})
            except Exception as _idem_e:
                print(f"[Idem] 终态更新失败 key={client_msg_id}: {_idem_e}", flush=True)

# 推送API (HTTP 触发 WebSocket 推送)
@app.post("/api/push/ws")
async def push_via_websocket(data: dict):
    """通过WebSocket推送给指定客户端"""
    client_id = data.get("client_id", "")
    message = data.get("message", {})
    if client_id:
        await manager.push(client_id, message)
    else:
        await manager.broadcast(message)
    return {"ok": True, "pushed": bool(client_id)}


# ══════════════════════════════════════════════════════════════════════
#  Huawei Push Kit 推送通道 (系统级推送，APP被杀也能收到)
# ══════════════════════════════════════════════════════════════════════

import json as json_module
from urllib.parse import urlencode

# Push Kit 配置（HarmonyOS 5+ 使用 JWT 鉴权，不再支持 OAuth 2.0）
PUSH_PROJECT_ID = os.getenv("PUSH_PROJECT_ID", "101653523864334050")
PUSH_SERVICE_ACCOUNT_KEY = os.getenv("PUSH_SERVICE_ACCOUNT_KEY", "")  # 服务账号密钥JSON路径
PUSH_TOKEN_FILE = Path(os.path.expanduser("~/.hermes/push_tokens.json"))

# JWT 缓存（有效期1小时，避免每次推送都重新签名）
_jwt_cache: dict = {"token": "", "expires_at": 0}

def _generate_jwt() -> str:
    """使用服务账号密钥生成 JWT，用于 Push Kit V3 鉴权"""
    import base64
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import padding
    from cryptography.hazmat.backends import default_backend

    now = int(time.time())
    # 缓存未过期直接返回
    if _jwt_cache["token"] and _jwt_cache["expires_at"] > now + 60:
        return _jwt_cache["token"]

    key_path = Path(PUSH_SERVICE_ACCOUNT_KEY)
    if not key_path.exists():
        raise FileNotFoundError(f"Service account key not found: {key_path}")

    key_data = json_module.loads(key_path.read_text())
    private_key = key_data["private_key"]
    key_id = key_data["key_id"]
    sub_account = key_data["sub_account"]

    pkey = serialization.load_pem_private_key(
        private_key.encode(), password=None, backend=default_backend()
    )

    def _b64url(data: bytes) -> str:
        return base64.urlsafe_b64encode(data).rstrip(b"=").decode()

    header = {"alg": "RS256", "kid": key_id, "typ": "JWT"}
    payload = {
        "iss": sub_account,
        "aud": "https://oauth-login.cloud.huawei.com/oauth2/v3/token",
        "iat": now,
        "exp": now + 3600,
    }
    header_b64 = _b64url(json_module.dumps(header).encode())
    payload_b64 = _b64url(json_module.dumps(payload).encode())
    signing_input = f"{header_b64}.{payload_b64}"

    signature = pkey.sign(
        signing_input.encode(), padding.PKCS1v15(), hashes.SHA256()
    )
    jwt = f"{signing_input}.{_b64url(signature)}"

    _jwt_cache["token"] = jwt
    _jwt_cache["expires_at"] = now + 3600
    return jwt

# 设备推送令牌存储
def _load_push_tokens() -> list[dict]:
    if PUSH_TOKEN_FILE.exists():
        try:
            data = json_module.loads(PUSH_TOKEN_FILE.read_text())
            return data if isinstance(data, list) else []
        except Exception:
            return []
    return []

def _save_push_tokens(tokens: list[dict]):
    PUSH_TOKEN_FILE.parent.mkdir(parents=True, exist_ok=True)
    PUSH_TOKEN_FILE.write_text(json_module.dumps(tokens, ensure_ascii=False, indent=2))

@app.post("/api/push/register")
async def register_push_token(data: dict):
    """APP注册Push Kit推送令牌"""
    token = data.get("token", "")
    device_id = data.get("device_id", "unknown")
    if not token:
        return {"ok": False, "error": "missing token"}
    tokens = _load_push_tokens()
    # 去重：同device_id更新，否则新增
    for t in tokens:
        if t.get("device_id") == device_id:
            t["token"] = token
            t["updated_at"] = datetime.now(TZ).isoformat()
            break
    else:
        tokens.append({
            "device_id": device_id,
            "token": token,
            "created_at": datetime.now(TZ).isoformat()
        })
    _save_push_tokens(tokens)
    print(f"[PushKit] Registered token for {device_id}, total={len(tokens)}")
    return {"ok": True, "total": len(tokens)}

async def _push_to_huawei(title: str, body: str, source: str = "intel"):
    """通过华为Push Kit REST API (JWT鉴权) 推送消息到所有注册设备"""
    print(f"[PushKit] Called: title={title}, body={body}")
    if not PUSH_SERVICE_ACCOUNT_KEY:
        print("[PushKit] Skipped: PUSH_SERVICE_ACCOUNT_KEY not configured")
        return

    tokens = _load_push_tokens()
    if not tokens:
        print("[PushKit] No tokens")
        return

    print(f"[PushKit] Starting push with {len(tokens)} tokens")

    try:
        # 生成 JWT（带缓存，1小时有效）
        jwt = _generate_jwt()

        # 逐设备推送
        device_tokens = [t["token"] for t in tokens if t.get("token")]
        if not device_tokens:
            return

        batch_size = 500
        for i in range(0, len(device_tokens), batch_size):
            batch = device_tokens[i:i + batch_size]
            message = {
                "payload": {
                    "notification": {
                        "category": "IM",
                        "title": title,
                        "body": body,
                        "clickAction": {
                            "actionType": 0
                        },
                        "foregroundShow": True
                    }
                },
                "target": {
                    "token": batch
                },
                "pushOptions": {
                    # [V2-3.5] 原为 True：华为测试消息模式有 token 数量上限（约 10 个），
                    #   14 个 token 整批被拒（80300030）→ PushKit 兜底从未真正生效过。
                    "testMessage": False,
                    "ttl": 86400
                }
            }
            async with _httpx_async_client(10.0, use_proxy=False) as client:
                push_resp = await client.post(
                    f"https://push-api.cloud.huawei.com/v3/{PUSH_PROJECT_ID}/messages:send",
                    headers={
                        "Authorization": f"Bearer {jwt}",
                        "Content-Type": "application/json",
                        "push-type": "0",
                    },
                    content=json_module.dumps(message)
                )
                result = push_resp.json()
                code = result.get("code", "")
                if code != "80000000":
                    print(f"[PushKit] Send failed batch {i//batch_size}: {result}")
                else:
                    print(f"[PushKit] Sent to {len(batch)} devices")
    except Exception as e:
        print(f"[PushKit] Error: {e}")



# ── 定期清理活跃流（防止内存泄漏）──────────────────────────────
async def cleanup_stale_streams():
    """清理超时的活跃流（completed/error/timeout 超过10分钟）"""
    while True:
        await asyncio.sleep(300)
        now = time.time()
        stale = [
            sid for sid, s in list(active_streams.items())
            if s.get("status") in ("complete", "error", "timeout", "superseded")
            and now - s.get("completed_at", 0) > 600
        ]
        for sid in stale:
            active_streams.pop(sid, None)
        if stale:
            print(f"[Cleanup] Removed {len(stale)} stale streams")


@app.on_event("startup")
async def start_cleanup_task():
    asyncio.create_task(cleanup_stale_streams())
    # V4-S12/N2: 幂等表建表 + 启动清扫孤儿 processing 行
    try:
        _idem_ensure_table()
        idem_recover_orphans()
    except Exception as _ie:
        print(f"[Idem] 启动初始化失败: {_ie}", flush=True)
    # V4-S15: offline_chats.retention_until 迁移（幂等）
    try:
        _db = _idem_conn()
        try:
            _cols = {r[1] for r in _db.execute("PRAGMA table_info(offline_chats)")}
            if "retention_until" not in _cols:
                _db.execute("ALTER TABLE offline_chats ADD COLUMN retention_until REAL")
                _db.commit()
                print("[Offline] 已加列 offline_chats.retention_until", flush=True)
        finally:
            _db.close()
    except Exception as _ae:
        print(f"[Offline] retention_until 迁移失败: {_ae}", flush=True)
    print("[Startup] Stale stream cleanup task started")

if __name__ == "__main__":
    import uvicorn
    print(f"📁 Obsidian Vault: {OBSIDIAN_VAULT}")
    print(f"🗄️  State DB: {STATE_DB}")
    print(f"🔑 Auth: {'enabled' if API_KEY else 'disabled (no key set)'}")
    print(f"🔌 WebSocket: /ws/push (实时推送)")
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="info")
