#!/usr/bin/env python3
"""手动更新 scorer_names.json 中未翻译的球员名"""
import json
from pathlib import Path

FILE = Path(__file__).parent / "scorer_names.json"
mapping = json.loads(FILE.read_text(encoding="utf-8"))

# 手动翻译映射表
translations = {
    "Abdulelah Al-Amri": "阿尔阿姆里",
    "Ali Avlvan": "阿尔布莱希",
    "Aymen Hussein": "侯赛因",
    "B. Barcola": "巴尔科拉",
    "Elijah Just": "贾斯特",
    "Emam Ashour": "阿舒尔",
    "Erling Haaland": "哈兰德",
    "I. Mbaye": "姆巴耶",
    "Izn Alarb": "阿尔阿拉比",
    "K. Mbappé": "姆巴佩",
    "Leo Østigård": "厄斯蒂高",
    "Lionel Messi": "梅西",
    "Maximiliano Araújo": "阿劳霍",
    "Mohamed Hany": "穆罕默德·哈尼",
    "Mohammad Mohebi": "穆赫比",
    "Ramin Rezaiian": "雷扎伊安",
    "Rvmanv Ashmid": "阿什米德",
}

updated = 0
for en, cn in translations.items():
    if en in mapping and mapping[en] == en:
        mapping[en] = cn
        print(f"  {en} → {cn}")
        updated += 1

sorted_map = dict(sorted(mapping.items()))
FILE.write_text(json.dumps(sorted_map, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
print(f"\n更新完成：{updated} 个名字翻译，共 {len(sorted_map)} 条映射")
