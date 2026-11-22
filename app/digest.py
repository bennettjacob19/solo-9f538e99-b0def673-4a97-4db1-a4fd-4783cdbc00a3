"""SHA-256 摘要相关的常量与工具。

一次校验上传里会出现三份摘要，只有三份相符才算成功：

1. **终端声明摘要**：终端在 ``X-Content-SHA256`` 请求头里填的 64 位十六进制
   字符串，表示“我发的正文应当是这个摘要”；
2. **网关实际摘要**：网关在转发正文时独立、增量地计算，收完整包后放在
   HTTP trailer（``X-Content-SHA256``）里送出；
3. **源站实收摘要**：源站对真正收到的正文字节独立计算。

网关不信任终端声明，源站也不信任前两份——三份都相等才增加成功计数。
"""

from __future__ import annotations

import hashlib
import re

# 请求头 / trailer 使用的小写字节名（h11 的 header 名一律按小写比较）
HEADER_NAME = b"x-content-sha256"
HEADER_TEXT = "X-Content-SHA256"

# RFC 定义：SHA-256 十六进制摘要固定 64 个字符
HEX_DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")


def normalize(value: bytes | bytearray | str | None) -> str | None:
    """把摘要值规范化成小写十六进制字符串；缺失或格式非法时返回 ``None``。"""
    if value is None:
        return None
    if isinstance(value, (bytes, bytearray)):
        try:
            value = bytes(value).decode("ascii")
        except UnicodeDecodeError:
            return None
    value = value.strip().lower()
    return value if HEX_DIGEST_RE.match(value) else None


def is_valid(value: bytes | bytearray | str | None) -> bool:
    """判断是否是合法的 64 位十六进制 SHA-256 摘要。"""
    return normalize(value) is not None


def new_hasher():
    """创建增量 SHA-256 计算器；调用方逐块 ``update`` 即可。"""
    return hashlib.sha256()
