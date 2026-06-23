#!/usr/bin/env python3
"""强制翻译 scorer_names.json 中未翻译的球员名为中文（Google Translate 免费版）"""
import json
from pathlib import Path

from deep_translator import GoogleTranslator

BASE = Path(__file__).parent
FILE = BASE / "scorer_names.json"

# 加载
mapping = json.loads(FILE.read_text(encoding="utf-8"))

# 找出未翻译的（值等于键）
untranslated = [k for k, v in mapping.items() if k == v]
print(f"未翻译数量: {len(untranslated)}")

if not untranslated:
    print("全部已翻译，无需操作")
    exit(0)

# 用 Google Translate 逐个翻译
translator = GoogleTranslator(source='auto', target='zh-CN')
translations = {}
for name in untranslated:
    try:
        cn = translator.translate(name)
        if cn and cn != name:
            translations[name] = cn
            print(f"  {name} → {cn}")
        else:
            print(f"  [SKIP] {name} (翻译结果与原文相同)")
    except Exception as e:
        print(f"  [ERROR] {name}: {e}")

print(f"\n翻译成功: {len(translations)} 个名字")

# 合并
for en, cn in translations.items():
    mapping[en] = cn

# 保存
sorted_map = dict(sorted(mapping.items()))
FILE.write_text(json.dumps(sorted_map, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
print(f"已保存到 {FILE}，共 {len(sorted_map)} 条映射")
