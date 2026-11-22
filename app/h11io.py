"""h11 与 asyncio 流之间的粘合层。

h11 本身只做协议状态机，不负责 socket I/O；这里提供：

* ``send_event``：把 h11 事件序列化后写入 asyncio StreamWriter（带背压）
* ``read_event``：从 StreamReader 取字节喂给 h11，直到吐出下一个协议事件
* 一组 ``LocalProtocolError`` / ``RemoteProtocolError`` 到 HTTP 响应码的映射
"""

from __future__ import annotations

import asyncio
from typing import Any

import h11

# RFC 7230 §6.1 定义的逐跳头，代理转发时必须剔除
HOP_BY_HOP_HEADERS = frozenset(
    [
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
    ]
)


class PeerClosed(Exception):
    """对端在完整响应/请求之前关闭了连接。"""


class PausedError(Exception):
    """h11 进入 PAUSED（超出本示例支持范围的连接复用等情况）。"""


async def send_event(
    conn: h11.Connection, writer: asyncio.StreamWriter, event: Any
) -> None:
    """序列化一个 h11 事件并完整写出。"""
    data = conn.send(event)
    # h11 在没有数据可发时会返回 b""（例如 body 已全部发完后的 EndOfMessage 缓冲）
    if data:
        writer.write(data)
        await writer.drain()


async def read_event(
    conn: h11.Connection, reader: asyncio.StreamReader, timeout: float
) -> Any:
    """从 ``reader`` 读字节驱动 ``conn``，返回下一个 h11 事件。

    对端正常结束时返回 ``h11.EndOfMessage``（由 h11 生成）或
    ``h11.ConnectionClosed``（连接级关闭）。
    """
    while True:
        event = conn.next_event()
        if event is h11.NEED_DATA:
            try:
                chunk = await asyncio.wait_for(reader.read(65536), timeout=timeout)
            except asyncio.TimeoutError:
                raise
            if not chunk:
                conn.receive_data(b"")
                continue
            conn.receive_data(chunk)
            continue
        if event is h11.PAUSED:
            raise PausedError("h11 连接进入 PAUSED 状态，本网关不支持该场景")
        return event


def filter_forward_headers(headers, host_value: str | None = None):
    """剔除逐跳头；可顺便重写 Host。返回 h11 需要的 ``[(bytes, bytes), ...]``。"""
    result = []
    for name, value in headers:
        key = name.decode("latin-1").lower() if isinstance(name, bytes) else name.lower()
        if key in HOP_BY_HOP_HEADERS:
            continue
        if host_value is not None and key == "host":
            value = host_value.encode("latin-1")
        result.append((name if isinstance(name, bytes) else name.encode("latin-1"),
                       value if isinstance(value, bytes) else value.encode("latin-1")))
    if host_value is not None and not any(
        n.decode("latin-1").lower() == "host" for n, _ in result
    ):
        result.insert(0, (b"host", host_value.encode("latin-1")))
    return result


def status_phrase(status_code: int) -> bytes:
    """给 h11.Response 提供一个 reason phrase。"""
    phrases = {
        200: b"OK",
        201: b"Created",
        400: b"Bad Request",
        403: b"Forbidden",
        405: b"Method Not Allowed",
        408: b"Request Timeout",
        411: b"Length Required",
        413: b"Payload Too Large",
        417: b"Expectation Failed",
        422: b"Unprocessable Entity",
        502: b"Bad Gateway",
        504: b"Gateway Timeout",
    }
    return phrases.get(status_code, b"Error")


async def send_simple_response(
    conn: h11.Connection,
    writer: asyncio.StreamWriter,
    status_code: int,
    body: bytes,
    content_type: bytes = b"text/plain; charset=utf-8",
) -> None:
    """向终端侧发一个自包含的最终响应并结束消息。"""
    try:
        await send_event(
            conn,
            writer,
            h11.Response(
                status_code=status_code,
                headers=[
                    (b"content-type", content_type),
                    (b"content-length", str(len(body)).encode("ascii")),
                    (b"connection", b"close"),
                ],
            ),
        )
        await send_event(conn, writer, h11.Data(data=body))
        await send_event(conn, writer, h11.EndOfMessage())
    except (ConnectionError, BrokenPipeError, h11.LocalProtocolError):
        # 终端可能已经断开，交给上层关连接即可
        pass
