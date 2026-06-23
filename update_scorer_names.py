#!/usr/bin/env python3
"""
自动采集世界杯射手 → 翻译中文名 → 更新 JSON
每天跑一次，新增射手自动翻译，不用手动维护
"""
import json
from pathlib import Path

import httpx
from deep_translator import GoogleTranslator

# ── 配置 ──
API_URL = "https://worldcup26.ir/get/games"
SCORER_NAMES_FILE = Path(__file__).parent / "scorer_names.json"

# 🛡️ 自动校准音译别名映射（专门解决伊朗官方 API 拼写不标准的世纪大坑，在此自动强匹配并持久化同步）
SPELLING_ALIASES = {
    "Abdallh Alamri": "阿尔阿姆里",       # API拼写 vs 标准: Abdulelah Al-Amri
    "Ali Jast": "贾斯特",               # API拼写 vs 标准: Elijah Just
    "Maksimilianv Araivkhv": "阿劳霍",  # API拼写 vs 标准: Maximiliano Araújo
    "Mohamed Mhbi": "穆赫比",           # API拼写 vs 标准: Mohammad Mohebi
    "Ramin Rzaiian": "雷扎伊安"          # API拼写 vs 标准: Ramin Rezaiian
}

def fetch_scorers() -> set:
    """从 API 采集所有射手英文名（HTTP/2 直连，绕过代理，自动重试）"""
    for attempt in range(3):
        try:
            with httpx.Client(http2=True, trust_env=False) as client:
                resp = client.get(API_URL, timeout=15)
                resp.raise_for_status()
                data = resp.json()
            return _parse_scorers(data)
        except Exception as e:
            if attempt < 2:
                import time
                wait = (attempt + 1) * 3
                print(f"[WARN] fetch attempt {attempt + 1} failed: {e}, retry in {wait}s...")
                time.sleep(wait)
            else:
                print(f"[ERROR] fetch API failed after 3 attempts: {e}")
                return set()


def _parse_scorers(data: dict) -> set:
    """从 API 响应解析射手名"""
    scorers = set()
    import re
    for g in data.get("games", []):
        if str(g.get("finished", "")).upper() != "TRUE":
            continue
        for field in ["home_scorers", "away_scorers"]:
            raw = g.get(field, "")
            if not raw or raw == "null":
                continue
            try:
                cleaned = raw.replace("\u2018", '"').replace("\u2019", '"')
                cleaned = cleaned.replace("\u201c", '"').replace("\u201d", '"')
                if cleaned.startswith("{"):
                    cleaned = "[" + cleaned[1:]
                if cleaned.endswith("}"):
                    cleaned = cleaned[:-1] + "]"
                names = json.loads(cleaned)
                for n in names:
                    name = re.sub(r"\s*\d+.*", "", n).replace("(OG)", "").replace("(p)", "").strip()
                    if name:
                        scorers.add(name)
            except Exception:
                pass
    return scorers


def load_mapping() -> dict:
    """加载现有映射"""
    if SCORER_NAMES_FILE.exists():
        return json.loads(SCORER_NAMES_FILE.read_text(encoding="utf-8"))
    return {}


def save_mapping(mapping: dict):
    """保存映射（按键排序）"""
    sorted_map = dict(sorted(mapping.items()))
    SCORER_NAMES_FILE.write_text(
        json.dumps(sorted_map, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(f"[OK] saved {len(sorted_map)} entries to {SCORER_NAMES_FILE}")


def translate_names(names: list) -> dict:
    """用 Google Translate 免费网页翻译英文球员名为中文"""
    if not names:
        return {}
    
    translator = GoogleTranslator(source='auto', target='zh-CN')
    result = {}
    for name in names:
        try:
            cn = translator.translate(name)
            result[name] = cn if cn and cn != name else name
        except Exception as e:
            print(f"[WARN] translate '{name}' failed: {e}, keep original")
            result[name] = name
    
    print(f"[OK] translated {len(result)} names via Google Translate")
    return result


def main():
    print("=== Scorer Names Auto-Update ===")

    # 1. 采集射手
    scorers = fetch_scorers()
    print(f"[INFO] found {len(scorers)} unique scorers")

    # 2. 加载现有映射，并自动强制注入别名/修正对照
    mapping = load_mapping()
    for alias_en, cn_name in SPELLING_ALIASES.items():
        mapping[alias_en] = cn_name
    print(f"[INFO] current mapping has {len(mapping)} entries (including auto-injected aliases)")

    # 3. 找出新增射手
    new_scorers = [s for s in scorers if s not in mapping]
    print(f"[INFO] {len(new_scorers)} new scorers to translate")

    if new_scorers:
        # 4. 翻译新增射手
        translations = translate_names(new_scorers)

        # 5. 合并
        for en, cn in translations.items():
            if en in SPELLING_ALIASES:
                mapping[en] = SPELLING_ALIASES[en]
            else:
                mapping[en] = cn if cn != en else en  # 翻译失败保留英文

        # 6. 保存
        save_mapping(mapping)
    else:
        # 即使没有新增射手，只要别名发生改动也会自动重写存盘，保持数据高度一致性
        save_mapping(mapping)

    print("=== Done ===")


if __name__ == "__main__":
    main()
