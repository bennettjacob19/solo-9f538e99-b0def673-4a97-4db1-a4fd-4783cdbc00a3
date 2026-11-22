"""试传客户端：用 h11 手工发起带 ``Expect: 100-continue`` 的 POST。

与普通 HTTP 库不同，这里显式区分三个时刻，便于在 /docs 里观察响应先后：

1. 请求头发完、正文暂缓；
2. 收到 100 Continue（许可）后才开始推正文；
3. 收到最终响应；若源站中途拒收，未发完的正文立即放弃。

返回值里带 ``timeline``（相对开始时间的毫秒级事件序列）和最终响应。
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from dataclasses import dataclass, field

import h11

from . import digest
from .h11io import read_event, send_event


@dataclass
class UploadResult:
    host: str
    port: int
    path: str
    body_size: int
    timeline: list[dict] = field(default_factory=list)
    status_code: int | None = None
    reason: str | None = None
    response_headers: list[tuple[str, str]] = field(default_factory=list)
    response_body: bytes = b""
    body_bytes_sent: int = 0
    got_100: bool = False
    error: str | None = None

    def as_dict(self) -> dict:
        return {
            "target": f"http://{self.host}:{self.port}{self.path}",
            "body_size": self.body_size,
            "got_100_continue": self.got_100,
            "status_code": self.status_code,
            "reason": self.reason,
            "response_headers": self.response_headers,
            "response_body": self.response_body.decode("utf-8", "replace"),
            "body_bytes_sent": self.body_bytes_sent,
            "error": self.error,
            "timeline": self.timeline,
        }


# 每次 write 的块大小；故意取小一点，让中途拒收的时序更明显
CHUNK_SIZE = 2048


async def upload_once(
    host: str,
    port: int,
    path: str,
    body_size: int,
    *,
    permission_timeout: float = 30.0,
    overall_timeout: float = 60.0,
    body: bytes | None = None,
    digest_value: str | None = None,
) -> UploadResult:
    """发起一次 100-continue 上传，返回带时间线的结果。

    * ``body``：显式正文；为 None 时发送 ``body_size`` 个 ``b"x"``。
      ``body_size`` 仍作为 Content-Length 声明值。
    * ``digest_value``：非 None 时在请求头里加 ``X-Content-SHA256``，
      值为调用方给出的字符串（用于演示“正确/错误摘要”两种情形）。
    """
    if body is not None:
        body_size = len(body)
    result = UploadResult(host=host, port=port, path=path, body_size=body_size)
    start = time.monotonic()

    def mark(event: str, **extra) -> None:
        item = {"t_ms": round((time.monotonic() - start) * 1000, 2), "event": event}
        item.update(extra)
        result.timeline.append(item)

    reader, writer = await asyncio.wait_for(
        asyncio.open_connection(host, port), timeout=permission_timeout
    )
    conn = h11.Connection(our_role=h11.CLIENT)
    permission = asyncio.Event()
    final_arrived = asyncio.Event()
    final_response: dict = {}

    async def receiver() -> None:
        """读循环：先等 100，再收最终响应。"""
        while True:
            event = await read_event(conn, reader, overall_timeout)
            if isinstance(event, h11.InformationalResponse) and \
                    event.status_code == 100:
                result.got_100 = True
                mark("recv_100_continue")
                permission.set()
                continue
            if isinstance(event, h11.Response):
                result.status_code = event.status_code
                result.reason = event.reason.decode("latin-1") if event.reason else ""
                result.response_headers = [
                    (n.decode("latin-1"), v.decode("latin-1"))
                    for n, v in event.headers
                ]
                mark("recv_final_response", status=event.status_code)
                final_response["done"] = False
                final_arrived.set()
                continue
            if isinstance(event, h11.Data):
                result.response_body += event.data
                continue
            if isinstance(event, h11.EndOfMessage):
                final_response["done"] = True
                mark("response_end_of_message",
                     body_len=len(result.response_body))
                return
            if isinstance(event, h11.ConnectionClosed):
                mark("server_closed_connection")
                return

    async def sender() -> None:
        """发循环：请求头 → 等许可 → 分块推正文（可被中途拒收打断）。"""
        request_headers = [
            (b"host", f"{host}:{port}".encode("ascii")),
            (b"expect", b"100-continue"),
            (b"content-length", str(body_size).encode("ascii")),
            (b"connection", b"close"),
        ]
        if digest_value is not None:
            request_headers.append(
                (digest.HEADER_NAME, digest_value.encode("ascii"))
            )
        await send_event(
            conn, writer,
            h11.Request(
                method=b"POST",
                target=path.encode("ascii"),
                headers=request_headers,
            ),
        )
        mark("request_headers_sent", body_size=body_size,
             declared_sha256=digest_value)

        # 关键：在源站许可到达前不发送任何正文；
        # 若最终响应（拒收/504）先到，也立即结束发送协程
        wait_tasks = {
            asyncio.create_task(permission.wait()),
            asyncio.create_task(final_arrived.wait()),
        }
        try:
            done, _ = await asyncio.wait(
                wait_tasks, return_when=asyncio.FIRST_COMPLETED,
                timeout=permission_timeout,
            )
        except asyncio.TimeoutError:
            mark("permission_wait_timeout")
            raise
        finally:
            for task in wait_tasks:
                if not task.done():
                    task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await task
        if not permission.is_set():
            mark("final_response_before_permission",
                 bytes_sent=result.body_bytes_sent)
            return

        remaining = body_size
        offset = 0
        mark("body_send_start")
        while remaining > 0:
            # 源站已经在中途回最终响应时，立刻停止发送（终端无需发完正文）
            if "done" in final_response:
                mark("body_send_aborted",
                     bytes_sent=result.body_bytes_sent,
                     unsent=remaining)
                return
            n = min(CHUNK_SIZE, remaining)
            if body is not None:
                chunk = body[offset:offset + n]
            else:
                chunk = b"x" * n
            try:
                await send_event(conn, writer, h11.Data(data=chunk))
            except (ConnectionError, BrokenPipeError,
                    h11.LocalProtocolError) as exc:
                # 网关/源站中途拒收后可能直接关连接，未发完的正文就此放弃
                mark("body_send_aborted",
                     bytes_sent=result.body_bytes_sent,
                     unsent=remaining,
                     reason=f"{type(exc).__name__}: {exc}")
                return
            result.body_bytes_sent += len(chunk)
            remaining -= len(chunk)
            offset += len(chunk)
            # 每块之间让出事件循环，使接收协程能及时处理中途拒收
            await asyncio.sleep(0)
        await send_event(conn, writer, h11.EndOfMessage())
        mark("body_send_complete", bytes_sent=result.body_bytes_sent)

    recv_task = asyncio.create_task(receiver())
    send_task = asyncio.create_task(sender())
    pending = {recv_task, send_task}
    try:
        while pending:
            done, pending = await asyncio.wait(
                pending,
                return_when=asyncio.FIRST_COMPLETED,
                timeout=overall_timeout,
            )
            for task in done:
                if task.exception() is not None and result.error is None:
                    exc = task.exception()
                    result.error = f"{type(exc).__name__}: {exc}"
                    mark("task_error", error=str(exc))
    except asyncio.TimeoutError:
        result.error = result.error or "overall timeout"
        mark("overall_timeout")
    except (ConnectionError, BrokenPipeError, h11.RemoteProtocolError,
            h11.LocalProtocolError) as exc:
        result.error = f"{type(exc).__name__}: {exc}"
        mark("connection_error", error=str(exc))
    finally:
        for task in (recv_task, send_task):
            if not task.done():
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task
        with contextlib.suppress(Exception):
            writer.close()
            await writer.wait_closed()

    return result
