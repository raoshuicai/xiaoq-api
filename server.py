#!/usr/bin/env python3
"""
小Q API - Hermes 伴侣服务
为 小Q Android APP 提供文件浏览 + Token 统计 API

部署: systemd user service on port 8089
暴露: cloudflared tunnel xiaoq.xiao-q.com → localhost:8089
"""

import asyncio
import json
import os
import sqlite3
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException, Query, Request, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, PlainTextResponse, StreamingResponse

# ── Config ──────────────────────────────────────────────────────────
# Load .env for OpenRouter credentials
_ENV_FILE = Path(os.path.expanduser("~/.hermes/.env"))
if _ENV_FILE.exists():
    for _line in _ENV_FILE.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _key, _, _val = _line.partition("=")
            if _key.strip() not in os.environ:
                os.environ[_key.strip()] = _val.strip()

OBSIDIAN_VAULT = Path(os.getenv("OBSIDIAN_VAULT", "/mnt/c/Users/xiaoq/Documents/Obsidian/hermes-vault"))
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

app = FastAPI(title="小Q API", version="1.0.0")

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
    return {"status": "ok", "service": "xiaoq-api", "version": "1.0.0"}


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
    apk = Path("/home/xiaoq/workspace/hermes-xiaoq-app/小Q-v2.0.10.apk")
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


@app.get("/api/files/read")
def read_file(request: Request, path: str = Query(..., description="文件相对路径")):
    """读取文件内容（仅文本类文件）"""
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

    content = full_path.read_text(encoding="utf-8", errors="replace")
    return {
        "path": path,
        "name": full_path.name,
        "size": full_path.stat().st_size,
        "content": content,
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


@app.get("/api/tokens/daily")
def tokens_daily(request: Request, days: int = Query(7, ge=1, le=90, description="最近 N 天")):
    """每日 token 消耗（最近 N 天）"""
    check_auth(request)

    try:
        db = _get_db()
        rows = db.execute("""
            SELECT
                date(started_at, 'unixepoch', '+8 hours') as day,
                COALESCE(SUM(input_tokens), 0) as input_tokens,
                COALESCE(SUM(output_tokens), 0) as output_tokens,
                COUNT(*) as session_count
            FROM sessions
            WHERE started_at > unixepoch('now', '-%d days')
            GROUP BY day
            ORDER BY day DESC
        """ % days).fetchall()
        db.close()

        daily = []
        for row in rows:
            daily.append({
                "date": row["day"],
                "input_tokens": row["input_tokens"],
                "output_tokens": row["output_tokens"],
                "total_tokens": row["input_tokens"] + row["output_tokens"],
                "sessions": row["session_count"],
            })

        total_input = sum(d["input_tokens"] for d in daily)
        total_output = sum(d["output_tokens"] for d in daily)

        return {
            "period": f"last_{days}_days",
            "total_input_tokens": total_input,
            "total_output_tokens": total_output,
            "total_tokens": total_input + total_output,
            "daily": daily,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")


@app.get("/api/tokens/monthly")
def tokens_monthly(request: Request, months: int = Query(6, ge=1, le=24, description="最近 N 月")):
    """每月 token 消耗（最近 N 月）"""
    check_auth(request)

    try:
        db = _get_db()
        rows = db.execute("""
            SELECT
                strftime('%%Y-%%m', started_at, 'unixepoch', '+8 hours') as month,
                COALESCE(SUM(input_tokens), 0) as input_tokens,
                COALESCE(SUM(output_tokens), 0) as output_tokens,
                COUNT(*) as session_count,
                COUNT(DISTINCT date(started_at, 'unixepoch', '+8 hours')) as active_days
            FROM sessions
            WHERE started_at > unixepoch('now', '-%d months')
            GROUP BY month
            ORDER BY month DESC
        """ % months).fetchall()
        db.close()

        monthly = []
        for row in rows:
            monthly.append({
                "month": row["month"],
                "input_tokens": row["input_tokens"],
                "output_tokens": row["output_tokens"],
                "total_tokens": row["input_tokens"] + row["output_tokens"],
                "sessions": row["session_count"],
                "active_days": row["active_days"],
            })

        return {
            "period": f"last_{months}_months",
            "monthly": monthly,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")


@app.get("/api/tokens/summary")
def tokens_summary(request: Request):
    """Token 总览：今日/本月/总计 + 模型分布"""
    check_auth(request)

    try:
        db = _get_db()
        today_str = datetime.now(TZ).strftime("%Y-%m-%d")
        this_month_str = datetime.now(TZ).strftime("%Y-%m")

        today = db.execute("""
            SELECT COALESCE(SUM(input_tokens), 0) as inp, COALESCE(SUM(output_tokens), 0) as outp
            FROM sessions WHERE date(started_at, 'unixepoch', '+8 hours') = ?
        """, [today_str]).fetchone()

        this_month = db.execute("""
            SELECT COALESCE(SUM(input_tokens), 0) as inp, COALESCE(SUM(output_tokens), 0) as outp, COUNT(*) as cnt
            FROM sessions WHERE strftime('%%Y-%%m', started_at, 'unixepoch', '+8 hours') = ?
        """, [this_month_str]).fetchone()

        total = db.execute("""
            SELECT COALESCE(SUM(input_tokens), 0) as inp, COALESCE(SUM(output_tokens), 0) as outp, COUNT(*) as cnt
            FROM sessions
        """).fetchone()

        active_days = db.execute("""
            SELECT COUNT(DISTINCT date(started_at, 'unixepoch', '+8 hours')) as d
            FROM sessions WHERE strftime('%%Y-%%m', started_at, 'unixepoch', '+8 hours') = ?
        """, [this_month_str]).fetchone()["d"] or 1

        models = db.execute("""
            SELECT model, COALESCE(SUM(input_tokens), 0) as inp, COALESCE(SUM(output_tokens), 0) as outp, COUNT(*) as cnt
            FROM sessions WHERE model IS NOT NULL
            GROUP BY model ORDER BY inp + outp DESC LIMIT 10
        """).fetchall()
        db.close()

        today_input, today_output = today["inp"], today["outp"]
        month_input, month_output = this_month["inp"], this_month["outp"]
        total_input, total_output = total["inp"], total["outp"]
        daily_avg = (month_input + month_output) / active_days if active_days > 0 else 0

        return {
            "today": {
                "input_tokens": today_input,
                "output_tokens": today_output,
                "total_tokens": today_input + today_output,
            },
            "this_month": {
                "input_tokens": month_input,
                "output_tokens": month_output,
                "total_tokens": month_input + month_output,
                "sessions": this_month["cnt"],
                "active_days": active_days,
                "daily_average": int(daily_avg),
                "projected_total": int(daily_avg * 30),
            },
            "all_time": {
                "input_tokens": total_input,
                "output_tokens": total_output,
                "total_tokens": total_input + total_output,
                "sessions": total["cnt"],
            },
            "by_model": [{
                "model": m["model"] or "unknown",
                "input_tokens": m["inp"],
                "output_tokens": m["outp"],
                "total_tokens": m["inp"] + m["outp"],
                "sessions": m["cnt"],
            } for m in models],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")


# ══════════════════════════════════════════════════════════════════════
#  Push Notification API (for cron job → APP delivery)
# ══════════════════════════════════════════════════════════════════════

PUSH_DB = Path(os.path.expanduser("~/.hermes/push_messages.db"))

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

    db = sqlite3.connect(str(PUSH_DB))
    cur = db.execute(
        "INSERT INTO push_messages (title, body, source) VALUES (?, ?, ?)",
        (title, text, source),
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
        await _push_to_huawei(title, text, source)
    except Exception as e:
        print(f"[PushKit] Error: {e}")
    await push_events.broadcast(message)
    await manager.broadcast({
        "type": "notification", "title": title, "body": text, "source": source
    })

    return {"status": "ok", "message": message}


@app.post("/api/test-approval")
async def test_approval(request: Request):
    """测试授权卡片：向所有WebSocket客户端发送approve_request"""
    await manager.broadcast({
        "type": "approve_request",
        "run_id": "test-run-001",
        "command": "rm -rf /data/production/db",
        "pattern_keys": ["tirith:destructive_command"],
        "choices": ["once", "session", "always", "deny"]
    })
    return {"status": "ok", "message": "approve_request broadcast sent"}


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
    async with httpx.AsyncClient(timeout=120.0) as client:
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
    async with httpx.AsyncClient(timeout=1800.0) as client:
        url = f"{HERMES_API}/v1/{path}"
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
            self.active_connections[client_id] = websocket

    async def disconnect(self, client_id: str):
        async with self.lock:
            self.active_connections.pop(client_id, None)

    async def push(self, client_id: str, message: dict):
        async with self.lock:
            ws = self.active_connections.get(client_id)
        if ws:
            try:
                await ws.send_json(message)
            except Exception:
                await self.disconnect(client_id)

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
        # 保持连接，等待消息
        while True:
            data = await websocket.receive_json()
            # 客户端发来的ping保活
            if data.get("type") == "ping":
                await websocket.send_json({"type": "pong"})
            # 授权确认回复
            elif data.get("type") == "approve_response":
                print(f"[WS] 授权回复 from {client_id}: {data}")
            # 流式聊天
            elif data.get("type") == "chat":
                messages = data.get("messages", [])
                session_id = data.get("session_id", f"harmony-ws-{int(datetime.now().timestamp())}")
                print(f"[WS] 流式聊天 from {client_id}, messages={len(messages)}")
                await _chat_stream_handler(websocket, messages, session_id)
            # 同步挂起缓存的消息回复（支持断点续流）
            elif data.get("type") == "sync_pending":
                session_id = data.get("session_id")
                last_seq = data.get("last_seq", -1)
                if session_id:
                    try:
                        # 优先检查活跃流（仍在处理中或刚完成的断点续传）
                        stream = active_streams.get(session_id)
                        if stream and stream["tokens"]:
                            # 发送所有错过的 token（含离线期间生成的）
                            missed = [t for t in stream["tokens"] if t["seq"] > last_seq]
                            for t in missed:
                                await websocket.send_json({
                                    "type": "chat_token",
                                    "seq": t["seq"],
                                    "content": t["content"],
                                })
                            
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
                                # 已完成 → 发送结束标记
                                await websocket.send_json({"type": "chat_done"})
                                # 同时写入离线缓存（完整回复）
                                if stream["full_response"]:
                                    db = sqlite3.connect(str(PUSH_DB))
                                    db.execute(
                                        "INSERT OR REPLACE INTO offline_chats (session_id, content) VALUES (?, ?)",
                                        (session_id, stream["full_response"])
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
                                await websocket.send_json({
                                    "type": "sync_pending_response",
                                    "session_id": session_id,
                                    "content": content,
                                })
                                db.execute("DELETE FROM offline_chats WHERE session_id = ?", (session_id,))
                                db.commit()
                                print(f"[Offline] Synced and deleted cached response for {session_id}")
                            else:
                                await websocket.send_json({
                                    "type": "sync_pending_response",
                                    "session_id": session_id,
                                    "content": "",
                                })
                            db.close()
                    except Exception as sync_err:
                        print(f"[Offline] Sync pending error: {sync_err}")
    except WebSocketDisconnect:
        await manager.disconnect(client_id)
    except RuntimeError:
        # 客户端连接已关闭但未走标准断开流程
        await manager.disconnect(client_id)


async def _chat_stream_handler(websocket: WebSocket, messages: list, session_id: str):
    """通过 Hermes API 流式处理聊天，结果通过 WebSocket 推送"""
    # 注册活跃流（支持断点续传）
    active_streams[session_id] = {"seq": 0, "tokens": [], "full_response": "", "status": "processing"}
    
    # 先发一个思考状态，告诉用户正在处理
    try:
        await websocket.send_json({"type": "chat_thinking"})
    except Exception:
        pass

    payload = {
        "model": "current",
        "stream": True,
        "messages": messages,
    }
    client_connected = True
    full_response = ""
    try:
        has_tokens = False
        async with asyncio.timeout(300):  # 方案: 总超时5分钟
            async with httpx.AsyncClient(timeout=300.0) as client:
                async with client.stream(
                    "POST",
                    f"{HERMES_API}/v1/chat/completions",
                    json=payload,
                    headers={
                        "Content-Type": "application/json",
                        "Authorization": f"Bearer {GATEWAY_KEY}" if GATEWAY_KEY else "",
                        "X-Hermes-Session-Id": session_id,
                        "X-Hermes-Skip-Approval": "true",
                    },
                ) as resp:
                    if resp.status_code != 200:
                        error_body = await resp.aread()
                        print(f"[WS] Hermes API error {resp.status_code}: {error_body[:200]}")
                        if client_connected:
                            try:
                                await websocket.send_json({"type": "chat_error", "message": f"API error {resp.status_code}"})
                            except Exception:
                                pass
                        active_streams.pop(session_id, None)
                        return

                    buffer = ""
                    async for chunk in resp.aiter_bytes():
                        text = chunk.decode() if isinstance(chunk, bytes) else str(chunk)
                        buffer += text
                        # SSE events are separated by \n\n
                        while "\n\n" in buffer:
                            event, buffer = buffer.split("\n\n", 1)
                            for line in event.strip().split("\n"):
                                line = line.strip()
                                if line.startswith("data: "):
                                    raw = line[6:].strip()
                                    if raw == "[DONE]":
                                        if client_connected:
                                            try:
                                                await websocket.send_json({"type": "chat_done"})
                                            except Exception:
                                                client_connected = False
                                    elif raw:
                                        try:
                                            evt = json.loads(raw)
                                            choices = evt.get("choices", [{}])
                                            if choices:
                                                delta = choices[0].get("delta", {})
                                                content = delta.get("content", "")
                                                if content:
                                                    has_tokens = True
                                                    full_response += content
                                                    # 分配序列号
                                                    seq = active_streams[session_id]["seq"]
                                                    active_streams[session_id]["seq"] = seq + 1
                                                    active_streams[session_id]["full_response"] = full_response
                                                
                                                    if client_connected:
                                                        try:
                                                            await websocket.send_json({
                                                                "type": "chat_token",
                                                                "seq": seq,
                                                                "content": content,
                                                            })
                                                        except Exception:
                                                            print(f"[WS] 客户端意外断开，切换到后台流缓存模式 (session_id={session_id})")
                                                            client_connected = False
                                                    # 无论客户端是否连接，都缓存 token
                                                    active_streams[session_id]["tokens"].append({
                                                        "seq": seq,
                                                        "content": content
                                                    })
                                        except json.JSONDecodeError:
                                            pass
        
        # 整个流处理完成
        if not client_connected:
            # 客户端已经断开，将拼好的完整数据存入本地 SQLite
            if full_response:
                try:
                    db = sqlite3.connect(str(PUSH_DB))
                    db.execute(
                        "INSERT OR REPLACE INTO offline_chats (session_id, content) VALUES (?, ?)",
                        (session_id, full_response)
                    )
                    db.commit()
                    db.close()
                    print(f"[Offline] Successfully cached response for {session_id} ({len(full_response)} chars)")
                except Exception as cache_err:
                    print(f"[Offline] Failed to cache response: {cache_err}")
        else:
            if not has_tokens:
                try:
                    await websocket.send_json({"type": "chat_done"})
                except Exception:
                    pass
        
        # 标记完成，保留 token 缓冲供后续 sync_pending 查询
        active_streams[session_id]["status"] = "complete"
        active_streams[session_id]["completed_at"] = time.time()
    except asyncio.TimeoutError:
        print(f"[WS] Chat stream timeout for {session_id}")
        active_streams[session_id]["status"] = "timeout"
        active_streams[session_id]["completed_at"] = time.time()
        active_streams[session_id].pop("reconnect_ws", None)
    except Exception as e:
        print(f"[WS] Chat stream error: {e}")
        if client_connected:
            try:
                await websocket.send_json({"type": "chat_error", "message": str(e)})
            except Exception:
                pass
        active_streams[session_id]["status"] = "error"
        active_streams[session_id]["completed_at"] = time.time()
        active_streams[session_id].pop("reconnect_ws", None)

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

# Push Kit 配置（从 .env 读取，需在AGC开启Push Kit后设置）
PUSH_APP_ID = os.getenv("PUSH_APP_ID", "")
PUSH_APP_SECRET = os.getenv("PUSH_APP_SECRET", "")
PUSH_PROJECT_ID = os.getenv("PUSH_PROJECT_ID", "101653523864334050")  # AGC Sender ID
PUSH_TOKEN_FILE = Path(os.path.expanduser("~/.hermes/push_tokens.json"))

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
    """通过华为Push Kit REST API推送消息到所有注册设备"""
    print(f"[PushKit] Called: title={title}, body={body}")
    if not PUSH_APP_ID or not PUSH_APP_SECRET:
        print(f"[PushKit] Skipped: ID={'yes' if PUSH_APP_ID else 'no'}, Secret={'yes' if PUSH_APP_SECRET else 'no'}")
        return  # 未配置 Push Kit，跳过

    tokens = _load_push_tokens()
    if not tokens:
        print("[PushKit] No tokens")
        return

    print(f"[PushKit] Starting push with {len(tokens)} tokens")

    try:
        # 1. 获取 OAuth 2.0 Access Token
        async with httpx.AsyncClient(timeout=10.0) as client:
            auth_resp = await client.post(
                "https://oauth-login.cloud.huawei.com/oauth2/v3/token",
                data={
                    "grant_type": "client_credentials",
                    "client_id": PUSH_APP_ID,
                    "client_secret": PUSH_APP_SECRET,
                }
            )
            auth_data = auth_resp.json()
            access_token = auth_data.get("access_token", "")
            if not access_token:
                print(f"[PushKit] Auth failed: {auth_data.get('error_description', 'unknown')}")
                return

        # 2. 逐设备推送
        device_tokens = [t["token"] for t in tokens if t.get("token")]
        if not device_tokens:
            return

        # 每条消息最多500个token，分批发送
        batch_size = 500
        for i in range(0, len(device_tokens), batch_size):
            batch = device_tokens[i:i + batch_size]
            # HarmonyOS 5 Push Kit REST API 格式 (push-type: 0 通知消息)
            message = {
                "payload": {
                    "notification": {
                        "category": "IM",
                        "title": title,
                        "body": body,
                        "clickAction": {
                            "actionType": 0  # 打开应用首页
                        },
                        "foregroundShow": True
                    }
                },
                "target": {
                    "token": batch
                },
                "pushOptions": {
                    "testMessage": True,  # 测试消息，绕过频控
                    "ttl": 86400  # 24小时有效期
                }
            }
            async with httpx.AsyncClient(timeout=10.0) as client:
                push_resp = await client.post(
                    f"https://push-api.cloud.huawei.com/v3/{PUSH_APP_ID}/messages:send",
                    headers={
                        "Authorization": f"Bearer {access_token}",
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
            if s.get("status") in ("complete", "error", "timeout")
            and now - s.get("completed_at", 0) > 600
        ]
        for sid in stale:
            active_streams.pop(sid, None)
        if stale:
            print(f"[Cleanup] Removed {len(stale)} stale streams")


@app.on_event("startup")
async def start_cleanup_task():
    asyncio.create_task(cleanup_stale_streams())
    print("[Startup] Stale stream cleanup task started")

if __name__ == "__main__":
    import uvicorn
    print(f"📁 Obsidian Vault: {OBSIDIAN_VAULT}")
    print(f"🗄️  State DB: {STATE_DB}")
    print(f"🔑 Auth: {'enabled' if API_KEY else 'disabled (no key set)'}")
    print(f"🔌 WebSocket: /ws/push (实时推送)")
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="info")
