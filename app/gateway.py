"""HTTP/1.1 上传网关（h11 + asyncio）。

约定（对应需求）：

* 每条 TCP 连接只处理一次 POST，请求必须带 ``Content-Length`` 与
  ``Expect: 100-continue``，否则直接回 4xx；
* 网关先把请求头转发给源站，**在收到源站 ``100 Continue`` 之前绝不转发正文**，
  也不向终端回 100；
* 源站回 100 后，网关向终端回 100，随后边收边转请求正文；
* 源站在许可阶段直接回最终响应（拒收）：原样转回终端，不转发正文，回 504 同理；
* 等许可超时：网关自己回 ``504 Gateway Timeout``，不转发正文；
* 转发正文途中源站提前给最终响应（中途拒收）：立即停止读取/转发正文，
  终端不必发完 body 就能拿到最终响应；
* 一条连接处理完即关，不做 keep-alive 复用。
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time

import h11

from .config import Settings
from .h11io import (
    PausedError,
    PeerClosed,
    filter_forward_headers,
    read_event,
    send_event,
    send_simple_response,
    status_phrase,
)

log = logging.getLogger("gateway")


class GatewayError(Exception):
    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status = status
        self.message = message


def _header(headers, name: bytes) -> bytes | None:
    for key, value in headers:
        if key.lower() == name:
            return value
    return None


def _validate_request(request: h11.Request) -> int:
    """返回需要上传的正文长度；不合法时抛 GatewayError。"""
    if request.method != b"POST":
        raise GatewayError(405, "网关只接受 POST 请求")
    expect = _header(request.headers, b"expect")
    if expect is None or b"100-continue" not in expect.lower():
        raise GatewayError(417, "请求必须带 Expect: 100-continue")
    if _header(request.headers, b"transfer-encoding") is not None:
        raise GatewayError(400, "网关仅支持 Content-Length，不支持 Transfer-Encoding")
    length_value = _header(request.headers, b"content-length")
    if length_value is None:
        raise GatewayError(411, "请求必须带 Content-Length")
    try:
        length = int(length_value)
    except ValueError:
        raise GatewayError(400, "Content-Length 不是合法整数")
    if length < 0:
        raise GatewayError(400, "Content-Length 不能为负数")
    return length


async def _open_origin(settings: Settings) -> tuple[h11.Connection, asyncio.StreamReader,
                                                    asyncio.StreamWriter]:
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(settings.origin_host, settings.origin_port),
            timeout=settings.idle_timeout,
        )
    except (OSError, asyncio.TimeoutError) as exc:
        raise GatewayError(502, f"无法连接源站: {exc}")
    conn = h11.Connection(our_role=h11.CLIENT)
    return conn, reader, writer


async def _await_permission(
    origin: h11.Connection,
    origin_reader: asyncio.StreamReader,
    settings: Settings,
) -> h11.Response:
    """等待源站的许可响应。

    * 收到 100（informational response）→ 返回该事件，调用方继续等最终响应；
    * 收到最终响应 → 抛 :class:`GatewayError` 之外的信号：返回 ``(None, response)``；
    * 超时 → 抛 TimeoutError，由上层回 504。
    """
    event = await read_event(origin, origin_reader, settings.permission_timeout)
    if isinstance(event, h11.InformationalResponse) and event.status_code == 100:
        return event
    if not isinstance(event, h11.Response):
        raise GatewayError(502, "源站返回了无法识别的响应")
    # 源站在看了请求头之后直接拒收（也可能是其他 1xx，本示例统一按最终响应处理）
    if 100 <= event.status_code < 200:
        raise GatewayError(502, f"源站返回了不支持的 1xx 响应: {event.status_code}")
    return event  # 最终响应：调用方据此判断“已拒收”


async def _forward_origin_final_response(
    terminal: h11.Connection,
    terminal_writer: asyncio.StreamWriter,
    origin: h11.Connection,
    origin_reader: asyncio.StreamReader,
    settings: Settings,
    first_event: h11.Response | None = None,
) -> None:
    """把源站的最终响应（含 body）完整转发给终端。"""
    event = first_event
    while True:
        if event is None:
            event = await read_event(origin, origin_reader, settings.idle_timeout)
        if isinstance(event, h11.ConnectionClosed):
            raise PeerClosed("源站在响应结束前关闭连接")
        if isinstance(event, h11.Response):
            headers = filter_forward_headers(event.headers)
            forwarded = h11.Response(
                status_code=event.status_code,
                headers=headers,
                reason=event.reason or status_phrase(event.status_code),
            )
            await send_event(terminal, terminal_writer, forwarded)
        elif isinstance(event, h11.Data):
            await send_event(terminal, terminal_writer, event)
        elif isinstance(event, h11.EndOfMessage):
            # 终端侧连接一律不复用
            await send_event(
                terminal, terminal_writer, h11.EndOfMessage(headers=event.headers)
            )
            return
        event = None


async def _pump_body_to_origin(
    terminal: h11.Connection,
    terminal_reader: asyncio.StreamReader,
    origin: h11.Connection,
    origin_writer: asyncio.StreamWriter,
    total_length: int,
    settings: Settings,
) -> None:
    """许可通过后：边收终端正文边转发源站，直到整条 body 发完。"""
    remaining = total_length
    while remaining > 0:
        event = await read_event(terminal, terminal_reader, settings.idle_timeout)
        if isinstance(event, h11.Data):
            chunk = event.data
            if len(chunk) > remaining:
                raise GatewayError(400, "请求正文超过 Content-Length 声明的长度")
            await send_event(origin, origin_writer, h11.Data(data=chunk))
            remaining -= len(chunk)
        elif isinstance(event, h11.EndOfMessage):
            if remaining != 0:
                raise GatewayError(400, "请求正文短于 Content-Length 声明的长度")
            await send_event(origin, origin_writer, h11.EndOfMessage())
            return
        else:
            raise GatewayError(400, "读取请求正文时收到了意外事件")
    # body 恰好读完，再等终端的 EndOfMessage
    event = await read_event(terminal, terminal_reader, settings.idle_timeout)
    if not isinstance(event, h11.EndOfMessage):
        raise GatewayError(400, "Content-Length 已读完但未收到消息结束标记")
    await send_event(origin, origin_writer, h11.EndOfMessage())


async def handle_terminal(
    terminal_reader: asyncio.StreamReader,
    terminal_writer: asyncio.StreamWriter,
    settings: Settings,
) -> None:
    peer = terminal_writer.get_extra_info("peername")
    started = time.monotonic()
    terminal = h11.Connection(our_role=h11.SERVER)
    origin: h11.Connection | None = None
    origin_writer: asyncio.StreamWriter | None = None
    try:
        # ---- 阶段 1：读取并校验终端请求头 ---------------------------------
        try:
            event = await read_event(terminal, terminal_reader, settings.idle_timeout)
        except (asyncio.TimeoutError, PeerClosed):
            log.info("终端 %s 在发送请求头前断开/超时", peer)
            return
        if not isinstance(event, h11.Request):
            await send_simple_response(terminal, terminal_writer, 400,
                                       b"expected an HTTP request\n")
            return
        request: h11.Request = event
        try:
            total_length = _validate_request(request)
        except GatewayError as exc:
            log.info("终端 %s 请求被拒: %s %s", peer, exc.status, exc.message)
            await send_simple_response(
                terminal, terminal_writer, exc.status,
                f"{exc.message}\n".encode("utf-8"),
            )
            return

        # ---- 阶段 2：连接源站并转发请求头（不含正文） ----------------------
        origin, origin_reader, origin_writer = await _open_origin(settings)
        forwarded_headers = filter_forward_headers(
            request.headers,
            host_value=f"{settings.origin_host}:{settings.origin_port}",
        )
        await send_event(
            origin,
            origin_writer,
            h11.Request(method=request.method, target=request.target,
                        headers=forwarded_headers),
        )

        # ---- 阶段 3：等源站许可 -------------------------------------------
        try:
            permission = await _await_permission(origin, origin_reader, settings)
        except asyncio.TimeoutError:
            log.warning("终端 %s 等待源站许可超时，回 504", peer)
            await send_simple_response(
                terminal, terminal_writer, 504,
                b"origin did not grant 100-continue permission in time\n",
            )
            return
        except GatewayError as exc:
            await send_simple_response(
                terminal, terminal_writer, exc.status,
                f"{exc.message}\n".encode("utf-8"),
            )
            return

        if permission.status_code != 100:
            # 源站在许可阶段直接拒收：转回最终响应，正文一字不转
            log.info("终端 %s 源站许可阶段拒收 %s（正文不转发）",
                     peer, permission.status_code)
            await _forward_origin_final_response(
                terminal, terminal_writer, origin, origin_reader,
                settings, first_event=permission,
            )
            return

        # 源站许可到达 → 先把 100 转给终端，终端随后才会发正文
        await send_event(
            terminal, terminal_writer,
            h11.InformationalResponse(status_code=100, headers=[]),
        )

        # ---- 阶段 4：边收边转正文，同时监听源站的中途拒收 ------------------
        pump_task = asyncio.create_task(
            _pump_body_to_origin(
                terminal, terminal_reader, origin, origin_writer,
                total_length, settings,
            )
        )
        relay_task = asyncio.create_task(
            _forward_origin_final_response(
                terminal, terminal_writer, origin, origin_reader, settings,
            )
        )

        try:
            done, pending = await asyncio.wait(
                {pump_task, relay_task}, return_when=asyncio.FIRST_COMPLETED,
            )
        except asyncio.CancelledError:
            pump_task.cancel()
            relay_task.cancel()
            raise

        if relay_task in done:
            # 源站中途拒收（或在 body 转发完前后很快回了响应）：
            # 立即停止转发，终端无需发完正文
            pump_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await pump_task
            exc = relay_task.exception()
            if exc is not None:
                raise exc
            log.info("终端 %s 源站最终响应已转回，停止正文转发", peer)
        else:
            # 正文已全部转发且 pump 正常结束：等 relay 把源站最终响应转完，
            # 不取消它，避免重复转发响应头
            exc = pump_task.exception()
            if exc is not None:
                relay_task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await relay_task
                raise exc
            await relay_task

    except GatewayError as exc:
        log.info("终端 %s 网关错误: %s %s", peer, exc.status, exc.message)
        with contextlib.suppress(Exception):
            await send_simple_response(
                terminal, terminal_writer, exc.status,
                f"{exc.message}\n".encode("utf-8"),
            )
    except PausedError as exc:
        log.warning("终端 %s 协议暂停: %s", peer, exc)
        with contextlib.suppress(Exception):
            await send_simple_response(terminal, terminal_writer, 400,
                                       b"unsupported connection state\n")
    except (PeerClosed, ConnectionError, BrokenPipeError, h11.RemoteProtocolError) as exc:
        log.info("终端 %s 连接异常: %s", peer, exc)
    except asyncio.TimeoutError:
        log.warning("终端 %s 读写超时", peer)
        with contextlib.suppress(Exception):
            await send_simple_response(terminal, terminal_writer, 504,
                                       b"gateway I/O timeout\n")
    except Exception:  # pragma: no cover - 兜底，避免单连接异常拖垮服务
        log.exception("终端 %s 处理失败", peer)
        with contextlib.suppress(Exception):
            await send_simple_response(terminal, terminal_writer, 502,
                                       b"gateway internal error\n")
    finally:
        if origin_writer is not None:
            with contextlib.suppress(Exception):
                origin_writer.close()
                await origin_writer.wait_closed()
        with contextlib.suppress(Exception):
            terminal_writer.close()
            await terminal_writer.wait_closed()
        log.info("终端 %s 连接结束，耗时 %.1f ms",
                 peer, (time.monotonic() - started) * 1000)


async def serve(settings: Settings) -> None:
    server = await asyncio.start_server(
        lambda r, w: handle_terminal(r, w, settings),
        host=settings.gateway_host, port=settings.gateway_port,
    )
    log.info("上传网关监听 %s:%s（源站 %s:%s，许可超时 %.1fs）",
             settings.gateway_host, settings.gateway_port,
             settings.origin_host, settings.origin_port,
             settings.permission_timeout)
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
