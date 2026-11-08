#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""语音 API —— ASR（语音转文字）+ 分段 TTS（文字转语音）

小Q APP 语音消息功能 P0。设计文档：
    C:/Users/xiaoq/Documents/Obsidian/hermes-vault/04-Projects/03-XiaoQ/20261108_小QAPP语音消息功能_V3_施工版.md

设计要点（与 V3 一致）：
  * ASR **前置到客户端**：APP 上传完立刻调 /api/asr 拿文字，消息 content 直接是转写文本。
    服务端因此不需要解析任何 [语音] 标记，也不需要 _inline_audio_markers。
  * TTS 按句分段合成：安卓端边收边播，首句 ~0.95s 出声。
  * turn_key 复用客户端 client_msg_id（客户端天然知道，补投路径也能带）。
  * 全部走国内直连（use_proxy=False），**绝不用同步 requests**（会卡死 WS 事件循环）。

独立模块：server.py 只 import 并调用 register_voice_routes(app, ctx)，把对现有
3789 行文件的改动压到最小，出问题直接删掉那一行即可回滚。
"""

import asyncio
import hashlib
import json
import os
import re
import time
from pathlib import Path
from typing import Optional

from fastapi import HTTPException, Request
from fastapi.responses import FileResponse

# ── 配置 ─────────────────────────────────────────────────────────────
SILICONFLOW_BASE = "https://api.siliconflow.cn/v1"
ASR_MODEL = "FunAudioLLM/SenseVoiceSmall"          # 免费
TTS_MODEL = "FunAudioLLM/CosyVoice2-0.5B"          # ¥50/M tokens
DEFAULT_VOICE = "claire"
ALLOWED_VOICES = {"alex", "anna", "bella", "benjamin", "charles",
                  "claire", "david", "diana"}

MAX_AUDIO_BYTES = 20 * 1024 * 1024    # 单条语音上限（与 /api/upload 一致）
MAX_TTS_CHARS = 2000                  # 单次 prepare 的文本上限
MAX_SEGS_PER_TURN = 60                # 一轮最多几段（防 agent 吐超长）
SEG_TTL_SECONDS = 48 * 3600           # 段文件生命周期（独立于 uploads 的 30 天）

TURN_KEY_RE = re.compile(r"[^A-Za-z0-9_-]")

# 句末标点（分句用）
_SENT_END = "。！？；\n"
# 分句阈值
MIN_SEG_CHARS = 10
MAX_SEG_CHARS = 80
HARD_SPLIT_CHARS = 120


def voice_enabled() -> bool:
    """服务端总开关：回滚时置 XIAOQ_VOICE_ENABLED=0 重启即可，客户端零改动。"""
    return os.environ.get("XIAOQ_VOICE_ENABLED", "1").strip() != "0"


def _api_key() -> str:
    return (os.environ.get("SILICONFLOW_API_KEY") or "").strip()


# ── MP3 时长解析（纯 Python，避免为每段起一次 ffprobe）────────────────
# 实测 SiliconFlow 输出在 128-130kbps 浮动，用「字节数 ÷ 128kbps」估算会有
# 1.3% 误差，90 秒音频累计漂移 >1 秒 → 句子高亮会越来越歪。逐帧累加才准。
_BR_V1_L3 = [0, 32, 40, 48, 56, 64, 80, 96, 112, 128, 160, 192, 224, 256, 320, 0]
_BR_V2_L3 = [0, 8, 16, 24, 32, 40, 48, 56, 64, 80, 96, 112, 128, 144, 160, 0]
_SR_V1 = [44100, 48000, 32000, 0]
_SR_V2 = [22050, 24000, 16000, 0]


def mp3_duration_ms(data: bytes) -> int:
    """逐帧累加 MPEG 音频时长（毫秒）。解析不出返回 0。"""
    n = len(data)
    if n < 4:
        return 0
    i = 0
    # 跳过 ID3v2 头
    if data[:3] == b"ID3" and n > 10:
        size = ((data[6] & 0x7F) << 21) | ((data[7] & 0x7F) << 14) \
             | ((data[8] & 0x7F) << 7) | (data[9] & 0x7F)
        i = 10 + size
    total = 0.0
    frames = 0
    stale = 0
    while i + 4 <= n:
        if data[i] != 0xFF or (data[i + 1] & 0xE0) != 0xE0:
            i += 1
            stale += 1
            if stale > 4096 and frames == 0:
                return 0
            continue
        b1 = data[i + 1]
        b2 = data[i + 2]
        ver = (b1 >> 3) & 0x03      # 3=MPEG1, 2=MPEG2, 0=MPEG2.5
        layer = (b1 >> 1) & 0x03    # 1=Layer III
        br_idx = (b2 >> 4) & 0x0F
        sr_idx = (b2 >> 2) & 0x03
        pad = (b2 >> 1) & 0x01
        if layer != 1 or br_idx in (0, 15) or sr_idx == 3 or ver == 1:
            i += 1
            stale += 1
            if stale > 4096 and frames == 0:
                return 0
            continue
        if ver == 3:
            bitrate = _BR_V1_L3[br_idx] * 1000
            samplerate = _SR_V1[sr_idx]
            spf = 1152
        else:
            bitrate = _BR_V2_L3[br_idx] * 1000
            samplerate = _SR_V2[sr_idx]
            spf = 576
        if bitrate == 0 or samplerate == 0:
            i += 1
            continue
        frame_len = int(spf / 8 * bitrate / samplerate) + pad
        if frame_len <= 4:
            i += 1
            continue
        total += spf / samplerate
        frames += 1
        stale = 0
        i += frame_len
    return int(total * 1000)


# ── 文本清洗 / 分句 ──────────────────────────────────────────────────
_MD_PATTERNS = [
    # 代码块 / 表格整块删掉，不念。念「（表格略）」给听者没有任何信息量，
    # 实测还会和句子粘在一起变成「…的交期。（表格略）（表格略）（代码略）」。
    (re.compile(r"```.*?```", re.S), " "),
    (re.compile(r"(?:^\s*\|.*\|\s*$\n?)+", re.M), " "),
    (re.compile(r"`([^`]*)`"), r"\1"),
    (re.compile(r"!\[[^\]]*\]\([^)]*\)"), ""),
    (re.compile(r"\[([^\]]*)\]\([^)]*\)"), r"\1"),
    (re.compile(r"\*\*([^*]*)\*\*"), r"\1"),
    (re.compile(r"\*([^*]*)\*"), r"\1"),
    (re.compile(r"^#{1,6}\s*", re.M), ""),
    (re.compile(r"^\s*[-*+]\s+", re.M), ""),
    (re.compile(r"^>\s?", re.M), ""),
    (re.compile(r"<[^>]{1,40}>"), ""),
]
_THINKING = re.compile(r"<thinking>.*?</thinking>", re.S | re.I)
# ASR 文本里伪造的附件标记要中和，避免被当成真标记解析
_FAKE_MARKER = re.compile(r"\[(图片|语音)\]\s*/?api/upload/[A-Za-z0-9._-]*")
# 段里必须有实质内容（汉字/字母/数字），否则纯符号段（"___"、"—"）会被送去合成
_HAS_CONTENT = re.compile(r"[\u4e00-\u9fffA-Za-z0-9]")


def clean_for_tts(text: str) -> str:
    """去掉不该念出来的东西：思考块、代码块、表格、Markdown 符号。"""
    t = _THINKING.sub("", text or "")
    for pat, repl in _MD_PATTERNS:
        t = pat.sub(repl, t)
    t = re.sub(r"[ \t]+", " ", t)
    t = re.sub(r"\n{2,}", "\n", t)
    return t.strip()


def clean_transcript(text: str) -> str:
    """ASR 结果清洗：SenseVoice 会带表情符号，还可能有伪标记。"""
    t = (text or "").strip()
    t = _FAKE_MARKER.sub(lambda m: "［" + m.group(1) + "］", t)
    # 去掉表情符号 / 杂符号，保留中文、英文、数字、常用标点
    t = re.sub(r"[\U0001F000-\U0001FAFF\u2600-\u27BF\uFE0F\u200D]", "", t)
    t = re.sub(r"[\x00-\x1F\x7F]", "", t)
    t = re.sub(r"\s+", " ", t)
    return t.strip()


def split_sentences(text: str) -> list:
    """把一段回复切成适合逐段合成的句子列表。

    规则（V3 §4.3）：句末标点断开；不足 10 字向下合并；超过 80 字先按逗号切；
    仍超 120 字硬切。返回 [{"idx":0,"text":"..."}]。
    """
    t = clean_for_tts(text)
    if not t:
        return []
    raw = []
    buf = ""
    for ch in t:
        buf += ch
        if ch in _SENT_END:
            if buf.strip():
                raw.append(buf.strip())
            buf = ""
    if buf.strip():
        raw.append(buf.strip())

    # 超长先按逗号/顿号切
    expanded = []
    for s in raw:
        if len(s) <= MAX_SEG_CHARS:
            expanded.append(s)
            continue
        piece = ""
        for ch in s:
            piece += ch
            if ch in "，、：,;" and len(piece) >= MIN_SEG_CHARS:
                expanded.append(piece.strip())
                piece = ""
            elif len(piece) >= HARD_SPLIT_CHARS:
                expanded.append(piece.strip())
                piece = ""
        if piece.strip():
            expanded.append(piece.strip())

    # 过短的向下合并
    merged = []
    for s in expanded:
        if merged and len(merged[-1]) < MIN_SEG_CHARS:
            merged[-1] = (merged[-1] + s)
            continue
        if len(s) < MIN_SEG_CHARS and merged:
            merged[-1] = merged[-1] + s
        else:
            merged.append(s)
    if len(merged) >= 2 and len(merged[-1]) < MIN_SEG_CHARS:
        merged[-2] = merged[-2] + merged[-1]
        merged.pop()

    out = []
    for s in merged[:MAX_SEGS_PER_TURN]:
        s = s.strip()
        if s and _HAS_CONTENT.search(s):
            out.append({"idx": len(out), "text": s})
    return out


def safe_turn_key(raw: Optional[str], text: str, voice: str) -> str:
    """turn_key 字符集收敛；没给就用内容哈希（手动播放路径）。"""
    t = TURN_KEY_RE.sub("", (raw or ""))[:64]
    if t:
        return t
    return "h" + hashlib.sha256((text + "|" + voice).encode("utf-8")).hexdigest()[:16]


# ── 路由注册 ─────────────────────────────────────────────────────────
def register_voice_routes(app, ctx: dict) -> None:
    """把语音路由挂到 FastAPI app 上。

    ctx 需要提供（避免和 server.py 循环 import）：
        check_auth(client_request)  -> None（不通过时抛 HTTPException）
        upload_dir                  -> Path
        make_client(timeout, use_proxy) -> httpx.AsyncClient 上下文管理器
        safe_name(raw)              -> str
    """
    check_auth = ctx["check_auth"]
    upload_dir: Path = Path(ctx["upload_dir"])
    make_client = ctx["make_client"]
    safe_name = ctx["safe_name"]

    tts_root = upload_dir / "tts"
    try:
        tts_root.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        print(f"[Voice] TTS 目录创建失败 {tts_root}: {e}", flush=True)

    _cleanup_state = {"at": 0.0}

    # ── 内部工具 ──
    def _require_enabled():
        if not voice_enabled():
            raise HTTPException(status_code=404, detail="VOICE_DISABLED")

    def _turn_dir(turn: str) -> Path:
        d = (tts_root / turn).resolve()
        if not str(d).startswith(str(tts_root.resolve())):
            raise HTTPException(status_code=400, detail="BAD_TURN_KEY")
        return d

    def _cleanup_tts(force: bool = False) -> int:
        """段目录 48 小时过期；每 10 分钟最多跑一次。"""
        now = time.time()
        if not force and (now - _cleanup_state["at"]) < 600:
            return 0
        _cleanup_state["at"] = now
        removed = 0
        try:
            for d in tts_root.iterdir():
                if not d.is_dir():
                    continue
                try:
                    if (now - d.stat().st_mtime) > SEG_TTL_SECONDS:
                        for f in d.iterdir():
                            f.unlink(missing_ok=True)
                        d.rmdir()
                        removed += 1
                except OSError:
                    pass
            for f in tts_root.glob("*.json"):
                try:
                    if (now - f.stat().st_mtime) > SEG_TTL_SECONDS:
                        f.unlink(missing_ok=True)
                        removed += 1
                except OSError:
                    pass
        except OSError as e:
            print(f"[Voice] 清理失败: {e}", flush=True)
        if removed:
            print(f"[Voice] 清理过期段目录 {removed} 个", flush=True)
        return removed

    async def _synth_one(client, turn: str, idx: int, text: str,
                         voice: str, key: str) -> dict:
        """合成一段（带缓存）。返回段描述。"""
        d = _turn_dir(turn)
        d.mkdir(parents=True, exist_ok=True)
        mp3 = d / f"{idx}.mp3"
        meta_path = d / f"{idx}.json"
        cache_key = hashlib.sha256(
            f"{turn}:{idx}:{text}:{voice}".encode("utf-8")).hexdigest()[:20]

        if mp3.is_file() and mp3.stat().st_size > 0 and meta_path.is_file():
            try:
                meta = json.loads(meta_path.read_text(encoding="utf-8"))
                if meta.get("cache_key") == cache_key:
                    return {"idx": idx, "text": text,
                            "duration_ms": int(meta.get("duration_ms", 0)),
                            "status": "ready"}
            except Exception:
                pass

        payload = {"model": TTS_MODEL, "voice": f"{TTS_MODEL}:{voice}",
                   "input": text, "response_format": "mp3"}
        try:
            r = await client.post(f"{SILICONFLOW_BASE}/audio/speech",
                                  headers={"Authorization": f"Bearer {key}"},
                                  json=payload)
        except Exception as e:
            print(f"[Voice] TTS 网络异常 turn={turn} idx={idx}: {e}", flush=True)
            return {"idx": idx, "text": text, "duration_ms": 0, "status": "failed"}

        if r.status_code != 200:
            print(f"[Voice] TTS 失败 turn={turn} idx={idx} "
                  f"status={r.status_code} body={r.text[:200]}", flush=True)
            return {"idx": idx, "text": text, "duration_ms": 0, "status": "failed"}

        data = r.content or b""
        if len(data) < 1024:
            print(f"[Voice] TTS 返回过小 turn={turn} idx={idx} bytes={len(data)}", flush=True)
            return {"idx": idx, "text": text, "duration_ms": 0, "status": "failed"}

        mp3.write_bytes(data)
        dur = mp3_duration_ms(data)
        try:
            meta_path.write_text(json.dumps(
                {"cache_key": cache_key, "text": text, "duration_ms": dur,
                 "voice": voice, "status": "ready", "at": int(time.time())},
                ensure_ascii=False),
                encoding="utf-8")
        except OSError:
            pass
        return {"idx": idx, "text": text, "duration_ms": dur, "status": "ready"}

    # ── 后台合成任务管理（P2 异步 prepare）────────────────────────────
    # turn -> asyncio.Task。同一 turn 只允许一个任务；全局并发上限防刷。
    _tts_jobs: dict = {}
    MAX_TTS_JOBS: int = 3

    def _seg_state(turn: str, seg: dict, voice: str):
        """查段缓存：返回 ('ready'|'pending', duration_ms)。"""
        d = _turn_dir(turn)
        mp3 = d / f"{seg['idx']}.mp3"
        meta_path = d / f"{seg['idx']}.json"
        cache_key = hashlib.sha256(
            f"{turn}:{seg['idx']}:{seg['text']}:{voice}".encode("utf-8")).hexdigest()[:20]
        if mp3.is_file() and mp3.stat().st_size > 0 and meta_path.is_file():
            try:
                meta = json.loads(meta_path.read_text(encoding="utf-8"))
                if meta.get("cache_key") == cache_key and meta.get("status") == "ready":
                    return "ready", int(meta.get("duration_ms", 0))
            except Exception:
                pass
        return "pending", 0

    def _ensure_pending_meta(turn: str, seg: dict, voice: str) -> None:
        """给「还没合成」的段先写一份 pending 元数据。

        ⚠️ 为什么必须写：GET /api/tts/segments 是按目录里的 *.json 枚举段的，
           合成中的段没有 json 就**完全不出现在列表里** → 客户端不知道总共几段，
           会把「只返回 1 段」当成「只有 1 段」，播完第一句就以为结束了（实测踩到）。
        """
        d = _turn_dir(turn)
        try:
            d.mkdir(parents=True, exist_ok=True)
        except OSError:
            return
        meta_path = d / f"{seg['idx']}.json"
        cache_key = hashlib.sha256(
            f"{turn}:{seg['idx']}:{seg['text']}:{voice}".encode("utf-8")).hexdigest()[:20]
        if meta_path.is_file():
            try:
                meta = json.loads(meta_path.read_text(encoding="utf-8"))
                if (meta.get("cache_key") == cache_key
                        and meta.get("status") == "ready"
                        and (d / f"{seg['idx']}.mp3").is_file()):
                    return
            except Exception:
                pass
        try:
            meta_path.write_text(json.dumps(
                {"cache_key": cache_key, "text": seg["text"], "duration_ms": 0,
                 "voice": voice, "status": "pending", "at": int(time.time())},
                ensure_ascii=False), encoding="utf-8")
        except OSError:
            pass

    async def _synth_all(turn: str, segs: list, voice: str, key: str) -> None:
        """后台逐段合成。串行（实测并行无收益），任何异常只记日志不外抛。

        ⚠️ finally 里必须把自己从 _tts_jobs 摘掉 —— 否则同一 turn 永远起不来新任务，
           客户端会一直轮询到超时。
        """
        try:
            async with make_client(60.0, False) as client:
                for seg in segs:
                    await _synth_one(client, turn, seg["idx"], seg["text"], voice, key)
            print(f"[Voice] 后台合成完成 turn={turn} segs={len(segs)}", flush=True)
        except asyncio.CancelledError:
            print(f"[Voice] 后台合成被取消 turn={turn}", flush=True)
            raise
        except Exception as e:      # noqa: BLE001
            print(f"[Voice] 后台合成异常 turn={turn}: {e}", flush=True)
        finally:
            _tts_jobs.pop(turn, None)

    # ── 1) POST /api/asr ──────────────────────────────────────────────
    @app.post("/api/asr")
    async def api_asr(request: Request, payload: dict):
        _require_enabled()
        check_auth(request)

        name = safe_name(payload.get("name") or "")
        path = (upload_dir / name).resolve()
        if not str(path).startswith(str(upload_dir.resolve())) or not path.is_file():
            raise HTTPException(status_code=404, detail="AUDIO_NOT_FOUND")

        data = path.read_bytes()
        if len(data) > MAX_AUDIO_BYTES:
            raise HTTPException(status_code=413, detail="AUDIO_TOO_LARGE")
        if len(data) < 512:
            raise HTTPException(status_code=422, detail="AUDIO_EMPTY")

        key = _api_key()
        if not key:
            raise HTTPException(status_code=503, detail="ASR_NOT_CONFIGURED")

        suffix = path.suffix.lower()
        mime = {".m4a": "audio/mp4", ".mp4": "audio/mp4", ".mp3": "audio/mpeg",
                ".wav": "audio/wav", ".aac": "audio/aac",
                ".ogg": "audio/ogg", ".opus": "audio/opus"}.get(
                    suffix, "application/octet-stream")

        try:
            async with make_client(25.0, False) as client:
                r = await client.post(
                    f"{SILICONFLOW_BASE}/audio/transcriptions",
                    headers={"Authorization": f"Bearer {key}"},
                    files={"file": (name, data, mime)},
                    data={"model": ASR_MODEL})
        except Exception as e:
            print(f"[Voice] ASR 网络异常 {name}: {e}", flush=True)
            raise HTTPException(status_code=502, detail="ASR_UPSTREAM_ERROR")

        if r.status_code != 200:
            print(f"[Voice] ASR 失败 {name} status={r.status_code} "
                  f"body={r.text[:200]}", flush=True)
            raise HTTPException(status_code=502, detail="ASR_UPSTREAM_ERROR")

        try:
            body = r.json() or {}
        except Exception:
            body = {}
        transcript = clean_transcript(body.get("text") or "")
        if not transcript:
            raise HTTPException(status_code=422, detail="ASR_EMPTY")

        seconds = 0.0
        usage = body.get("usage") or {}
        try:
            seconds = float(usage.get("seconds") or 0)
        except Exception:
            seconds = 0.0

        print(f"[Voice] ASR {name} -> {len(transcript)}字 "
              f"({seconds:.0f}s audio, {len(data)}B)", flush=True)
        return {"ok": True, "transcript": transcript,
                "duration_ms": int(seconds * 1000)}

    # ── 2) POST /api/tts/prepare ──────────────────────────────────────
    @app.post("/api/tts/prepare")
    async def api_tts_prepare(request: Request, payload: dict):
        _require_enabled()
        check_auth(request)

        raw_text = (payload.get("text") or "").strip()
        if not raw_text:
            raise HTTPException(status_code=400, detail="TEXT_EMPTY")
        truncated = False
        if len(raw_text) > MAX_TTS_CHARS:
            raw_text = raw_text[:MAX_TTS_CHARS]
            truncated = True

        voice = (payload.get("voice") or DEFAULT_VOICE).strip()
        if voice not in ALLOWED_VOICES:
            voice = DEFAULT_VOICE

        segs = split_sentences(raw_text)
        if not segs:
            raise HTTPException(status_code=400, detail="NO_SPEAKABLE_TEXT")

        turn = safe_turn_key(payload.get("turn_key"), raw_text, voice)
        key = _api_key()
        if not key:
            raise HTTPException(status_code=503, detail="TTS_NOT_CONFIGURED")

        _cleanup_tts()

        # ⚠️ 异步化（P2）：旧实现是把所有段合成完才返回（4 段 8.34s），
        #    客户端点播放要干等 8 秒才能出声，达不到「1.5s 内出声」的验收线。
        #    现在：已缓存的段立即标 ready，缺的丢后台任务，**立刻返回**；
        #    客户端拉 GET /api/tts/segments 轮询，第一段 ready 就开播。
        out = []
        pending = []
        for seg in segs:
            st, dur = _seg_state(turn, seg, voice)
            if st == "pending":
                _ensure_pending_meta(turn, seg, voice)   # 让 segments 接口能列出全部段
            out.append({"idx": seg["idx"], "text": seg["text"],
                        "duration_ms": dur, "status": st,
                        "url": f"/api/tts/segments/{turn}/{seg['idx']}.mp3"})
            if st == "pending":
                pending.append(seg)

        started = False
        if pending and turn not in _tts_jobs and len(_tts_jobs) < MAX_TTS_JOBS:
            task = asyncio.create_task(_synth_all(turn, pending, voice, key))
            _tts_jobs[turn] = task
            started = True
        elif pending and (turn in _tts_jobs or len(_tts_jobs) >= MAX_TTS_JOBS):
            # 已在合成 / 并发已满：照常返回 pending，客户端轮询等待
            started = False

        ready_n = sum(1 for s in out if s["status"] == "ready")
        print(f"[Voice] prepare turn={turn} segs={len(out)} ready={ready_n} "
              f"pending={len(pending)} started={started} voice={voice}", flush=True)

        return {"turn_key": turn, "truncated": truncated,
                "async": started, "segments": out}

    # ── 3) GET /api/tts/segments ──────────────────────────────────────
    @app.get("/api/tts/segments")
    def api_tts_segments(turn: str = ""):
        """列出某轮的段（预合成轮询用）。不鉴权，与 GET /api/upload/{name} 对齐。"""
        _require_enabled()
        turn = TURN_KEY_RE.sub("", turn or "")[:64]
        if not turn:
            raise HTTPException(status_code=400, detail="TURN_REQUIRED")
        d = _turn_dir(turn)
        if not d.is_dir():
            raise HTTPException(status_code=404, detail="TURN_NOT_FOUND")

        segs = []
        for meta_path in sorted(d.glob("*.json")):
            try:
                meta = json.loads(meta_path.read_text(encoding="utf-8"))
            except Exception:
                continue
            idx = int(meta_path.stem)
            mp3 = d / f"{idx}.mp3"
            ok = (mp3.is_file() and mp3.stat().st_size > 0
                  and meta.get("status") == "ready")
            segs.append({"idx": idx, "text": meta.get("text", ""),
                         "duration_ms": int(meta.get("duration_ms", 0)),
                         "status": "ready" if ok else "pending",
                         "url": f"/api/tts/segments/{turn}/{idx}.mp3"})
        segs.sort(key=lambda s: s["idx"])
        return {"turn_key": turn,
                "all_ready": bool(segs) and all(s["status"] == "ready" for s in segs),
                "segments": segs}

    # ── 4) GET /api/tts/segments/{turn}/{idx}.mp3 ─────────────────────
    # ⚠️ 必须是独立的音频路由：现有 GET /api/upload/{name} 的 {name} 不跨 "/"，
    #    且 _safe_upload_name 会把 "/" 洗成 "_"，拿不到 tts/<turn>/<idx>.mp3。
    @app.get("/api/tts/segments/{turn}/{idx}.mp3")
    def api_tts_segment_file(turn: str, idx: int):
        _require_enabled()
        turn = TURN_KEY_RE.sub("", turn or "")[:64]
        if not turn or idx < 0 or idx >= MAX_SEGS_PER_TURN:
            raise HTTPException(status_code=400, detail="BAD_SEGMENT_PATH")
        d = _turn_dir(turn)
        mp3 = (d / f"{idx}.mp3").resolve()
        if not str(mp3).startswith(str(d)):
            raise HTTPException(status_code=403, detail="Access denied")
        if not mp3.is_file() or mp3.stat().st_size == 0:
            raise HTTPException(status_code=404, detail="SEGMENT_NOT_FOUND")
        return FileResponse(str(mp3), media_type="audio/mpeg")

    print(f"[Voice] 语音路由已注册 (enabled={voice_enabled()}, "
          f"tts_root={tts_root})", flush=True)
