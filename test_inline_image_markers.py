#!/usr/bin/env python3
"""自检：_inline_image_markers —— APP 的 `[图片] /api/upload/<name>` 标记
还原成 OpenAI 原生 vision 内容块。

跑法: cd /home/xiaoq/workspace/xiaoq-api && python3 test_inline_image_markers.py
纯逻辑，不联网、不碰真实 ~/.hermes/uploads。
"""
import base64
import os
import sys
import tempfile

os.environ.setdefault("API_SERVER_KEY", "test-key")

import server  # noqa: E402

# 把上传目录指到临时目录，别污染真实的
_tmp = tempfile.mkdtemp(prefix="xiaoq-vision-test-")
server.UPLOAD_DIR = server.Path(_tmp)

# 造两张小图（内容随意，这里只验证编码/解码往返）
PNG_BYTES = bytes.fromhex("89504e470d0a1a0a0000000d49484452") + b"fake-png-body"
JPG_BYTES = bytes.fromhex("ffd8ffe000104a464946") + b"fake-jpg-body"
(_ := server.Path(_tmp, "a.png")).write_bytes(PNG_BYTES)
server.Path(_tmp, "b.jpg").write_bytes(JPG_BYTES)
server.Path(_tmp, "notes.pdf").write_bytes(b"%PDF-1.4 fake")
server.Path(_tmp, "huge.png").write_bytes(b"x" * (server.MAX_INLINE_IMAGE_BYTES + 1))

fails = []


def check(name, cond, extra=None):
    if cond:
        print(f"  ✓ {name}")
    else:
        print(f"  ✗ {name} {extra}")
        fails.append(name)


def user(content):
    return [{"role": "user", "content": content}]


def img_block(b):
    return b.get("type") == "image_url"


def decode(b):
    url = b["image_url"]["url"]
    assert url.startswith("data:image/"), url[:40]
    head, _, data = url.partition(",")
    return head, base64.b64decode(data)


print("① 没标记 → 原样返回，零改动")
msgs = [{"role": "user", "content": "普通一句话"}]
out, n = server._inline_image_markers(msgs)
check("no-op", out is msgs and n == 0)

print("② 单个标记 → 一个图片块")
out, n = server._inline_image_markers(user("[图片] /api/upload/a.png"))
check("张数=1", n == 1, n)
check("块类型", len(out[0]["content"]) == 1 and img_block(out[0]["content"][0]))
head, raw = decode(out[0]["content"][0])
check("mime=png", head.startswith("data:image/png"), head)
check("字节往返一致", raw == PNG_BYTES)

print("③ 标记前后带文字 → 顺序保留")
out, n = server._inline_image_markers(
    user("帮我看看这张图\n[图片] /api/upload/b.jpg\n重点看左上角"))
c = out[0]["content"]
check("三段", len(c) == 3, [x["type"] for x in c])
check("text→image→text",
      c[0]["type"] == "text" and c[1]["type"] == "image_url" and c[2]["type"] == "text")
check("前文保留", c[0]["text"] == "帮我看看这张图", repr(c[0]["text"]))
check("后文保留", c[2]["text"] == "重点看左上角", repr(c[2]["text"]))
head, raw = decode(c[1])
check("mime=jpeg", head.startswith("data:image/jpeg"), head)

print("④ 多图 → 两块")
out, n = server._inline_image_markers(
    user("[图片] /api/upload/a.png\n[图片] /api/upload/b.jpg"))
check("张数=2", n == 2, n)
check("两个图片块", sum(1 for b in out[0]["content"] if img_block(b)) == 2)

print("⑤ 非图片扩展名 → 不内联，保持纯文本")
msgs = user("[图片] /api/upload/notes.pdf")
out, n = server._inline_image_markers(msgs)
check("张数=0", n == 0, n)
check("messages 未被替换", out is msgs)

print("⑥ 图片过大 → 跳过，不内联")
msgs = user("[图片] /api/upload/huge.png")
out, n = server._inline_image_markers(msgs)
check("张数=0", n == 0, n)

print("⑦ 文件不存在 → 不崩，保持纯文本")
msgs = user("[图片] /api/upload/nope.png")
out, n = server._inline_image_markers(msgs)
check("张数=0 且不抛", n == 0, n)

print("⑧ 路径穿越 → 被 _safe_upload_name 洗净后找不到文件，不越界")
msgs = user("[图片] /api/upload/../../../etc/passwd")
out, n = server._inline_image_markers(msgs)
check("没读到 /etc/passwd", n == 0, n)

print("⑨ 历史消息里的标记不动，只有最后一条 user 被处理")
msgs = [
    {"role": "user", "content": "[图片] /api/upload/a.png"},
    {"role": "assistant", "content": "看到了"},
    {"role": "user", "content": "[图片] /api/upload/b.jpg"},
]
out, n = server._inline_image_markers(msgs)
check("只处理一条", n == 1, n)
check("历史仍是字符串", isinstance(out[0]["content"], str))
check("末条变成块", isinstance(out[2]["content"], list))
check("原 list 未被就地改动", isinstance(msgs[2]["content"], str))
check("其他消息对象原样复用", out[0] is msgs[0] and out[1] is msgs[1])

print("⑩ 异常输入 → 不崩")
for bad in ([], None, "notalist", [{"role": "user"}], [{"role": "user", "content": 123}]):
    try:
        o, k = server._inline_image_markers(bad)
        check(f"输入 {bad!r}", k == 0, k)
    except Exception as e:
        check(f"输入 {bad!r}", False, f"抛了 {type(e).__name__}: {e}")

print()
if fails:
    print(f"{len(fails)} 项失败: {fails}")
    sys.exit(1)
print("全部通过")
