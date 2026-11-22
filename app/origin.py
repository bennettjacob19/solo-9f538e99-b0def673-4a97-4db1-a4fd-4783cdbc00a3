"""本地样例源站（h11 + asyncio）。

提供的路径：

* ``POST /accept``      —— 正常放行：先回 100，读完整个 body 再回 200
* ``POST /verify``      —— **SHA-256 校验收件口**：先回 100，边收边算实收正文
                           摘要，收完整包后核对三份摘要，全部相符才增加成功
                           计数并回 201；缺摘要或不符回 422
* ``POST /reject``      —— 看了请求头直接拒收（回 403），**不发 100、不读 body**
* ``POST /reject-mid``  —— 先回 100，读够 ``REJECT_AFTER_BYTES`` 字节后中途回 413，
                           不再读取剩余正文
* ``POST /hang``        —— 收到请求头后保持沉默，用来制造许可超时（网关回 504）
* ``GET  /stats``       —— 返回各路径累计实收的正文字节数与 /verify 成功计数
* ``POST /stats/reset`` —— 清零计数器

每条连接同样只处理一个请求。
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from dataclasses import dataclass, field

import h11

from . import digest
from .config import Settings
from .h11io import (
    PausedError,
    read_event,
    send_event,
)

log = logging.getLogger("origin")


@dataclass
class OriginStats:
    """线程/任务安全的源站计数器。"""

    requests: dict[str, int] = field(default_factory=dict)
    body_bytes: dict[str, int] = field(default_factory=dict)
    # /verify 收到完整正文、且三份摘要相符的次数
    verified_success: int = 0
    # /verify 被拒绝（摘要缺失/不符/正文不完整）的次数
    verified_failure: int = 0
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def record(self, path: str, nbytes: int) -> None:
        async with self._lock:
            self.requests[path] = self.requests.get(path, 0) + 1
            self.body_bytes[path] = self.body_bytes.get(path, 0) + nbytes

    async def record_verification(self, path: str, nbytes: int, ok: bool) -> None:
        """一次 /verify 收件的结果：同时记账 requests/body_bytes 与成功计数。"""
        async with self._lock:
            self.requests[path] = self.requests.get(path, 0) + 1
            self.body_bytes[path] = self.body_bytes.get(path, 0) + nbytes
            if ok:
                self.verified_success += 1
            else:
                self.verified_failure += 1

    async def snapshot(self) -> dict:
        async with self._lock:
            return {
                "requests": dict(sorted(self.requests.items())),
                "body_bytes": dict(sorted(self.body_bytes.items())),
                "total_body_bytes": sum(self.body_bytes.values()),
                "verified_success": self.verified_success,
                "verified_failure": self.verified_failure,
            }

    async def reset(self) -> None:
        async with self._lock:
            self.requests.clear()
            self.body_bytes.clear()
            self.verified_success = 0
            self.verified_failure = 0


def _json_response(status: int, payload: dict) -> tuple[h11.Response, h11.Data,
                                                        h11.EndOfMessage]:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    response = h11.Response(
        status_code=status,
        headers=[
            (b"content-type", b"application/json; charset=utf-8"),
            (b"content-length", str(len(body)).encode("ascii")),
            (b"connection", b"close"),
        ],
    )
    return response, h11.Data(data=body), h11.EndOfMessage()


async def _send_json(conn, writer, status: int, payload: dict) -> None:
    for event in _json_response(status, payload):
        await send_event(conn, writer, event)


async def _drain_body_until(
    conn: h11.Connection,
    reader: asyncio.StreamReader,
    limit: int | None,
    idle_timeout: float,
) -> int:
    """读取请求正文；``limit`` 为 None 时读完整条 body，否则读到该字节数即停。

    返回实际读取的正文字节数。
    """
    received = 0
    while True:
        event = await read_event(conn, reader, idle_timeout)
        if isinstance(event, h11.Data):
            received += len(event.data)
            if limit is not None and received >= limit:
                return received
        elif isinstance(event, h11.EndOfMessage):
            return received
        elif isinstance(event, h11.ConnectionClosed):
            return received


async def _receive_verified_body(
    conn: h11.Connection,
    reader: asyncio.StreamReader,
    idle_timeout: float,
) -> tuple[int, str, bytes | None, bool]:
    """收完整条正文并计算实收摘要。

    返回 ``(实收字节数, 实收摘要十六进制, 网关 trailer 里的摘要原值,
    是否完整收到 EndOfMessage)``。中途连接断开时仍返回已读部分，
    交由调用方判定为失败。
    """
    hasher = digest.new_hasher()
    received = 0
    complete = False
    trailer: bytes | None = None
    while True:
        try:
            event = await read_event(conn, reader, idle_timeout)
        except (ConnectionError, BrokenPipeError, h11.RemoteProtocolError,
                PausedError, asyncio.TimeoutError):
            # 超时或连接异常：已读部分照实返回，由调用方判定为不完整
            break
        if isinstance(event, h11.Data):
            hasher.update(event.data)
            received += len(event.data)
        elif isinstance(event, h11.EndOfMessage):
            complete = True
            for name, value in event.headers:
                if name.lower() == digest.HEADER_NAME:
                    trailer = value
            break
        elif isinstance(event, h11.ConnectionClosed):
            break
    return received, hasher.hexdigest(), trailer, complete


async def _handle_verify(
    conn: h11.Connection,
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    settings: Settings,
    stats: OriginStats,
    declared_headers,
) -> None:
    """SHA-256 校验收件：三份摘要相符才 201，否则 422。

    三份摘要是：终端在 ``X-Content-SHA256`` 请求头里声明的、网关在 trailer
    里送出的实际摘要、源站对实收正文计算出的摘要。源站不信任前两份。
    """
    declared = digest.normalize(
        next((v for n, v in declared_headers
              if n.lower() == digest.HEADER_NAME), None)
    )

    received, actual, trailer_value, complete = await _receive_verified_body(
        conn, reader, settings.idle_timeout
    )
    relayed = digest.normalize(trailer_value)

    if not complete:
        # 正文没收完（终端断开、超时或网关提前中止）：此次上传不算成功
        await stats.record_verification("/verify", received, False)
        log.info("源站 /verify 正文不完整（%d 字节），未计成功", received)
        with contextlib.suppress(Exception):
            await _send_json(conn, writer, 422, {
                "ok": False,
                "reason": "incomplete body",
                "received_bytes": received,
            })
        return

    reasons = []
    if declared is None:
        reasons.append("terminal X-Content-SHA256 missing or malformed")
    if relayed is None:
        reasons.append("gateway trailer X-Content-SHA256 missing or malformed")
    if not reasons and declared != relayed:
        reasons.append("declared digest != gateway digest")
    if not reasons and relayed != actual:
        reasons.append("gateway digest != origin actual digest")
    if not reasons and declared != actual:
        reasons.append("declared digest != origin actual digest")

    ok = not reasons
    await stats.record_verification("/verify", received, ok)
    if ok:
        await _send_json(conn, writer, 201, {
            "ok": True,
            "path": "/verify",
            "received_bytes": received,
            "declared_sha256": declared,
            "gateway_sha256": relayed,
            "origin_sha256": actual,
            "verified_success": stats.verified_success,
        })
        log.info("源站 /verify 校验通过，正文 %d 字节，回 201", received)
    else:
        await _send_json(conn, writer, 422, {
            "ok": False,
            "path": "/verify",
            "received_bytes": received,
            "reasons": reasons,
            "declared_sha256": declared,
            "gateway_sha256": relayed,
            "origin_sha256": actual,
            "verified_success": stats.verified_success,
        })
        log.info("源站 /verify 校验失败（%s），回 422: %s",
                 "; ".join(reasons), reasons)


async def _handle(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    settings: Settings,
    stats: OriginStats,
) -> None:
    conn = h11.Connection(our_role=h11.SERVER)
    peer = writer.get_extra_info("peername")
    try:
        event = await read_event(conn, reader, settings.idle_timeout)
        if not isinstance(event, h11.Request):
            await _send_json(conn, writer, 400, {"error": "expected request"})
            return

        target = event.target.decode("latin-1").split("?", 1)[0]
        log.info("源站收到 %s %s 来自 %s", event.method.decode(), target, peer)

        if event.method == b"GET" and target == "/stats":
            await _send_json(conn, writer, 200, await stats.snapshot())
            return
        if event.method == b"POST" and target == "/stats/reset":
            await stats.reset()
            await _send_json(conn, writer, 200, {"reset": True})
            return
        if event.method != b"POST":
            await _send_json(conn, writer, 405, {"error": "POST only"})
            return

        if target == "/reject":
            # 许可阶段直接拒收：不发 100、不读任何正文
            await stats.record(target, 0)
            await _send_json(
                conn, writer, 403,
                {"error": "rejected by origin before body", "path": target},
            )
            log.info("源站拒收 %s（正文未读）", target)
            return

        if target == "/hang":
            # 保持沉默直到对端断开，用来触发网关的许可超时
            await stats.record(target, 0)
            log.info("源站对 %s 保持沉默，等待对端关闭……", target)
            try:
                await read_event(conn, reader, settings.idle_timeout * 6)
            except (asyncio.TimeoutError, ConnectionError, PausedError,
                    h11.RemoteProtocolError):
                pass
            return

        if target not in ("/accept", "/reject-mid", "/verify",
                          "/verify-reject-mid"):
            await _send_json(conn, writer, 404, {"error": f"unknown path {target}"})
            return

        # 上述路径都先放行（100 Continue）
        await send_event(
            conn, writer,
            h11.InformationalResponse(status_code=100, headers=[]),
        )

        if target == "/verify":
            await _handle_verify(
                conn, reader, writer, settings, stats, event.headers
            )
            return

        if target == "/verify-reject-mid":
            # 演示校验入口的“源站提前拒收”：读够阈值即回 413，
            # 不收完整包、也不增加成功计数
            received = await _drain_body_until(
                conn, reader, settings.reject_after_bytes, settings.idle_timeout
            )
            await stats.record_verification(target, received, False)
            await _send_json(
                conn, writer, 413,
                {
                    "ok": False,
                    "reason": "rejected mid-stream before full body",
                    "path": target,
                    "received_before_reject": received,
                },
            )
            log.info("源站 /verify-reject-mid 拒收，已读 %d 字节，未计成功",
                     received)
            return

        if target == "/accept":
            received = await _drain_body_until(
                conn, reader, None, settings.idle_timeout
            )
            await stats.record(target, received)
            await _send_json(
                conn, writer, 200,
                {"ok": True, "path": target, "received_bytes": received},
            )
            log.info("源站收完 %s，正文 %d 字节，回 200", target, received)
            return

        # /reject-mid：读够阈值后立刻拒收，剩余正文不再读
        received = await _drain_body_until(
            conn, reader, settings.reject_after_bytes, settings.idle_timeout
        )
        await stats.record(target, received)
        await _send_json(
            conn, writer, 413,
            {
                "error": "payload too large: rejected mid-stream",
                "path": target,
                "received_before_reject": received,
            },
        )
        log.info("源站中途拒收 %s，拒收前已读 %d 字节", target, received)


    except (ConnectionError, BrokenPipeError, h11.RemoteProtocolError,
            asyncio.TimeoutError, PausedError) as exc:
        log.info("源站连接异常: %s", exc)
    except Exception:  # pragma: no cover
        log.exception("源站处理失败")
    finally:
        with contextlib.suppress(Exception):
            writer.close()
            await writer.wait_closed()


async def serve(settings: Settings, stats: OriginStats | None = None) -> None:
    stats = stats or OriginStats()
    server = await asyncio.start_server(
        lambda r, w: _handle(r, w, settings, stats),
        host="0.0.0.0", port=settings.origin_port,
    )
    log.info("样例源站监听 0.0.0.0:%s（中途拒收阈值 %d 字节）",
             settings.origin_port, settings.reject_after_bytes)
    async with server:
        await server.serve_forever()


def main() -> None:  # pragma: no cover
    from .config import env_settings

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    asyncio.run(serve(env_settings()))


if __name__ == "__main__":  # pragma: no cover
    main()
