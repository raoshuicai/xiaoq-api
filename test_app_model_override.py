#!/usr/bin/env python3
"""自检：APP 模型覆盖的取舍逻辑 + current 标记不串位。

跑法: cd /home/xiaoq/workspace/xiaoq-api && python3 test_app_model_override.py
不依赖网络、不依赖真实 config.yaml —— 纯逻辑，坏了一跑就红。
"""
import io
import os
import sys
import tempfile

os.environ.setdefault("API_SERVER_KEY", "test-key")

import yaml  # noqa: E402

# 把状态文件指到临时目录，别碰真实 ~/.hermes/xiaoq_app_model.json
import server  # noqa: E402

_tmpdir = tempfile.mkdtemp(prefix="xiaoq-model-test-")
server.APP_MODEL_STATE = server.Path(os.path.join(_tmpdir, "app_model.json"))

CFG = {
    "model": {"default": "deepseek-v4-flash", "provider": "deepseek",
              "base_url": "https://api.deepseek.com/v1"},
    "gateway": {"platforms": {"api_server": {"extra": {"model_routes": {
        "deepseek-v4-flash": {"model": "deepseek-v4-flash", "provider": "deepseek"},
        "gemini-2.5-flash": {"model": "gemini-2.5-flash", "provider": "google"},
    }}}}},
}


def cur_of(items):
    return [m["id"] for m in items if m["current"]]


# ① 无覆盖 → current = 全局默认
server._save_app_model("", "")
items = server._collect_models(CFG)
assert cur_of(items) == ["deepseek-v4-flash"], cur_of(items)
assert {m["id"]: m["provider"] for m in items} == {
    "deepseek-v4-flash": "deepseek", "gemini-2.5-flash": "google"}, items
print("① 无覆盖 current = 全局默认 ✓")

# ② 有覆盖 → current 跟 APP 走，全局默认不再标 current
server._save_app_model("gemini-2.5-flash", "google")
items = server._collect_models(CFG)
assert cur_of(items) == ["gemini-2.5-flash"], cur_of(items)
print("② APP 覆盖 current = APP 选的 ✓")

# ③ 覆盖清除 → 回到全局默认
server._save_app_model("", "")
items = server._collect_models(CFG)
assert cur_of(items) == ["deepseek-v4-flash"], cur_of(items)
print("③ 清除覆盖回落全局 ✓")

# ④ 状态文件损坏 → 当没有覆盖，不炸
with io.open(server.APP_MODEL_STATE, "w", encoding="utf-8") as f:
    f.write("{ this is not json")
assert server._load_app_model() == {}
assert cur_of(server._collect_models(CFG)) == ["deepseek-v4-flash"]
print("④ 状态文件损坏 → 安全回落 ✓")

# ⑤ 回归：老 bug 场景必须不再复现
#    若 provider 错配（default=gemini-2.5-flash / provider=deepseek），
#    gemini 路由的 provider 是 google，不该被判成 current。
bad = {"model": {"default": "gemini-2.5-flash", "provider": "deepseek"},
       "gateway": CFG["gateway"]}
items = server._collect_models(bad)
assert cur_of(items) == [], "provider 错配时不应标 current：%s" % cur_of(items)
print("⑤ provider 错配不再误标 current ✓")

# ⑥ 没有 model_routes 时回退老逻辑
legacy = {"model": {"default": "deepseek-v4-flash", "provider": "deepseek"}}
items = server._collect_models(legacy)
assert cur_of(items) == ["deepseek-v4-flash"], cur_of(items)
print("⑥ 无 model_routes 回退老逻辑 ✓")

os.remove(server.APP_MODEL_STATE) if os.path.exists(server.APP_MODEL_STATE) else None
print("\n全部通过")
