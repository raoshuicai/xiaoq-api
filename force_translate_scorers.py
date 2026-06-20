#!/usr/bin/env python3
"""强制翻译 scorer_names.json 中未翻译的球员名为中文"""
import json
import os
from pathlib import Path

import httpx

BASE = Path(__file__).parent
FILE = BASE / "scorer_names.json"

KEY = os.getenv("DEEPSEEK_API_KEY", "")
if not KEY:
    KEY = os.getenv("OPENROUTER_API_KEY", "")
URL = os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com/v1")
MODEL = "deepseek-chat"

# 加载
mapping = json.loads(FILE.read_text(encoding="utf-8"))

# 找出未翻译的（值等于键）
untranslated = [k for k, v in mapping.items() if k == v]
print(f"未翻译数量: {len(untranslated)}")

if not untranslated:
    print("全部已翻译，无需操作")
    exit(0)

# 按球员名发音、习惯中文名的翻译规则给出提示
prompt = (
    "你是专业的足球运动员中文译名翻译专家。"
    "将以下英文足球运动员名字翻译为通用的中文译名（新华社标准译名优先）。\n"
    "注意：\n"
    "1. 亚洲球员（日本、韩国、伊朗、沙特等）使用其汉字原名\n"
    "2. K. Mbappé 翻译为 姆巴佩\n"
    "3. Lionel Messi 翻译为 梅西\n"
    "4. Erling Haaland 翻译为 哈兰德\n"
    "5. 只返回 JSON 格式 {\"英文名\": \"中文名\", ...}，不要其他文字\n\n"
    + json.dumps(untranslated, ensure_ascii=False)
)

try:
    with httpx.Client(http2=True) as client:
        resp = client.post(
            f"{URL}/chat/completions",
            headers={
                "Authorization": f"Bearer {KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": MODEL,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0,
            },
            timeout=30,
        )
        content = resp.json()["choices"][0]["message"]["content"]
        if "```" in content:
            content = content.split("```")[1]
            if content.startswith("json"):
                content = content[4:]
        translations = json.loads(content.strip())
        print(f"翻译成功: {len(translations)} 个名字")
except Exception as e:
    print(f"[ERROR] 翻译失败: {e}")
    exit(1)

# 合并
for en, cn in translations.items():
    if cn and cn != en:
        mapping[en] = cn
        print(f"  {en} → {cn}")

# 保存
sorted_map = dict(sorted(mapping.items()))
FILE.write_text(json.dumps(sorted_map, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
print(f"\n已保存到 {FILE}，共 {len(sorted_map)} 条映射")
