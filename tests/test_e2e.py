"""端到端测试：正常上传、许可前拒收、中途拒收、许可超时、连接只处理一次。"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import sys
from pathlib import Path

import h11
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.h11io import read_event, send_event  # noqa: E402
from app.rawclient import upload_once  # noqa: E402
from tests.harness import Harness  # noqa: E402


@pytest.fixture
def harness():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    h = loop.run_until_complete(
        Harness(permission_timeout=1.5, reject_after_bytes=4096).start()
    )
    yield h
    loop.run_until_complete(h.stop())
    loop.close()
    asyncio.set_event_loop(None)


def _timeline_events(result) -> list[str]:
    return [item["event"] for item in result.timeline]


def _event_time(result, name: str) -> float:
    for item in result.timeline:
        if item["event"] == name:
            return item["t_ms"]
    raise AssertionError(f"事件 {name} 未出现: {result.timeline}")


# ---------------------------------------------------------------- 正常上传
def test_accept_full_upload(harness):
    host, port = harness.gateway_addr

    async def scenario():
        await harness.stats.reset()
        result = await upload_once(host, port, "/accept", 20000)
        stats = await harness.stats.snapshot()
        return result, stats

    result, stats = asyncio.get_event_loop().run_until_complete(scenario())

    assert result.error is None
    assert result.got_100 is True
    assert result.status_code == 200
    assert result.body_bytes_sent == 20000
    # 时序：请求头 → 100 许可 → 正文开始 → 正文发完 → 最终响应
    events = _timeline_events(result)
    assert events.index("request_headers_sent") < events.index("recv_100_continue")
    assert events.index("recv_100_continue") < events.index("body_send_start")
    assert events.index("body_send_complete") < events.index("recv_final_response")
    assert stats["body_bytes"].get("/accept") == 20000


# ------------------------------------------------------- 源站许可前直接拒收
def test_reject_before_permission_forwards_no_body(harness):
    host, port = harness.gateway_addr

    async def scenario():
        await harness.stats.reset()
        result = await upload_once(host, port, "/reject", 50000,
                                   permission_timeout=5)
        stats = await harness.stats.snapshot()
        return result, stats

    result, stats = asyncio.get_event_loop().run_until_complete(scenario())

    assert result.got_100 is False
    assert result.status_code == 403
    # 没有收到许可，正文一个字节都不能发
    assert result.body_bytes_sent == 0
    assert "body_send_start" not in _timeline_events(result)
    # 源站同样实收 0 字节正文
    assert stats["body_bytes"].get("/reject", 0) == 0


# ----------------------------------------------------------- 源站中途拒收
def test_reject_mid_stream_terminal_need_not_finish(harness):
    host, port = harness.gateway_addr
    body_size = 100_000  # 远大于 4096 的拒收阈值

    async def scenario():
        await harness.stats.reset()
        result = await upload_once(host, port, "/reject-mid", body_size)
        # 给网关一点时间把剩余字节情况落定后再读统计
        await asyncio.sleep(0.2)
        stats = await harness.stats.snapshot()
        return result, stats

    result, stats = asyncio.get_event_loop().run_until_complete(scenario())

    assert result.got_100 is True
    assert result.status_code == 413
    events = _timeline_events(result)
    assert "body_send_aborted" in events
    # 终端在发完之前就拿到了最终响应
    assert result.body_bytes_sent < body_size
    assert _event_time(result, "recv_final_response") <= \
        _event_time(result, "body_send_aborted") + 5
    # 源站只读到阈值附近的字节，远小于整个 body
    origin_received = stats["body_bytes"].get("/reject-mid", 0)
    assert 4096 <= origin_received < body_size
    assert origin_received <= result.body_bytes_sent + 4096


# ------------------------------------------------------------- 许可超时 504
def test_permission_timeout_returns_504(harness):
    host, port = harness.gateway_addr

    async def scenario():
        await harness.stats.reset()
        result = await upload_once(host, port, "/hang", 30000,
                                   permission_timeout=5)
        stats = await harness.stats.snapshot()
        return result, stats

    result, stats = asyncio.get_event_loop().run_until_complete(scenario())

    assert result.got_100 is False
    assert result.status_code == 504
    assert result.body_bytes_sent == 0
    assert stats["body_bytes"].get("/hang", 0) == 0


# ------------------------------------------------- 每条连接只处理一次请求
def test_connection_handles_single_request_only(harness):
    host, port = harness.gateway_addr

    async def scenario():
        reader, writer = await asyncio.open_connection(host, port)
        conn = h11.Connection(our_role=h11.CLIENT)
        await send_event(conn, writer, h11.Request(
            method=b"POST", target=b"/accept",
            headers=[
                (b"host", b"test"),
                (b"expect", b"100-continue"),
                (b"content-length", b"5"),
                (b"connection", b"keep-alive"),
            ],
        ))
        event = await read_event(conn, reader, 5)
        assert isinstance(event, h11.InformationalResponse)
        assert event.status_code == 100
        await send_event(conn, writer, h11.Data(data=b"hello"))
        await send_event(conn, writer, h11.EndOfMessage())
        # 收完最终响应
        while True:
            event = await read_event(conn, reader, 5)
            if isinstance(event, h11.EndOfMessage):
                break
        # 网关应当关闭连接，而不是接受第二次请求
        try:
            chunk = await asyncio.wait_for(reader.read(1), timeout=3)
        except asyncio.TimeoutError:
            chunk = b"timeout"
        writer.close()
        with contextlib.suppress(Exception):
            await writer.wait_closed()
        return chunk

    chunk = asyncio.get_event_loop().run_until_complete(scenario())
    assert chunk == b""


# ------------------------------------------------ 非法请求的直接 4xx 拒绝
@pytest.mark.parametrize(
    "headers,expected_status",
    [
        ([(b"host", b"t"), (b"content-length", b"10")], 417),          # 缺 Expect
        ([(b"host", b"t"), (b"expect", b"100-continue")], 411),        # 缺 Content-Length
    ],
)
def test_invalid_requests_rejected(harness, headers, expected_status):
    host, port = harness.gateway_addr

    async def scenario():
        reader, writer = await asyncio.open_connection(host, port)
        conn = h11.Connection(our_role=h11.CLIENT)
        await send_event(conn, writer, h11.Request(
            method=b"POST", target=b"/accept", headers=headers,
        ))
        event = await read_event(conn, reader, 5)
        writer.close()
        return event.status_code

    status = asyncio.get_event_loop().run_until_complete(scenario())
    assert status == expected_status


# =====================================================================
# SHA-256 校验入口
# =====================================================================
def _synthetic_body(size: int) -> bytes:
    """与 demo 相同的可复现正文，便于在测试里独立算摘要。"""
    pattern = b"diagnostic-package/0123456789\n"
    full, rest = divmod(size, len(pattern))
    return pattern * full + pattern[:rest]


def _raw_upload(host, port, path, chunks, declared, *, abandon_after=None):
    """用 h11 直接上传：先等 100，再按 ``chunks`` 分块发送。

    ``declared`` 为 None 时不发 ``X-Content-SHA256``；``abandon_after`` 非 None
    时发够这么多字节后直接中止连接（模拟终端未发完就断开）。
    返回 ``(最终状态码, 响应正文, 已发送字节数)``。若网关在许可前就回了
    最终响应（例如摘要头缺失/非法），不再发送正文，直接返回该响应。
    """
    async def scenario():
        reader, writer = await asyncio.open_connection(host, port)
        conn = h11.Connection(our_role=h11.CLIENT)
        total = sum(len(c) for c in chunks)
        headers = [
            (b"host", b"t"),
            (b"expect", b"100-continue"),
            (b"content-length", str(total).encode("ascii")),
        ]
        if declared is not None:
            headers.append((b"x-content-sha256", declared.encode("ascii")))
        await send_event(conn, writer, h11.Request(
            method=b"POST", target=path.encode("ascii"), headers=headers,
        ))
        event = await read_event(conn, reader, 5)
        pre_100 = []
        if not isinstance(event, h11.InformationalResponse):
            # 网关在许可前就回了最终响应：读完整条响应
            while True:
                if isinstance(event, h11.Response):
                    pre_100.append(event.status_code)
                elif isinstance(event, h11.EndOfMessage):
                    break
                elif isinstance(event, h11.ConnectionClosed):
                    break
                event = await read_event(conn, reader, 5)
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()
            return pre_100[-1] if pre_100 else None, b"", 0
        assert event.status_code == 100

        sent = 0
        for chunk in chunks:
            if abandon_after is not None and sent + len(chunk) > abandon_after:
                writer.transport.abort()
                await asyncio.sleep(0.3)
                return None, b"", abandon_after
            try:
                await send_event(conn, writer, h11.Data(data=chunk))
            except (ConnectionError, BrokenPipeError, h11.LocalProtocolError):
                # 源站中途拒收后连接可能已被重置，转去读最终响应
                break
            sent += len(chunk)
            await asyncio.sleep(0)
        else:
            with contextlib.suppress(ConnectionError, BrokenPipeError,
                                     h11.LocalProtocolError):
                await send_event(conn, writer, h11.EndOfMessage())

        status, body = None, b""
        with contextlib.suppress(ConnectionError, BrokenPipeError,
                                 h11.RemoteProtocolError):
            while True:
                event = await read_event(conn, reader, 5)
                if isinstance(event, h11.Response):
                    status = event.status_code
                elif isinstance(event, h11.Data):
                    body += event.data
                elif isinstance(event, h11.EndOfMessage):
                    break
                elif isinstance(event, h11.ConnectionClosed):
                    break
        writer.close()
        with contextlib.suppress(Exception):
            await writer.wait_closed()
        return status, body, sent

    return asyncio.get_event_loop().run_until_complete(scenario())


def test_verify_correct_digest_accepted_and_counted(harness):
    body = _synthetic_body(50_000)
    # 刻意用非 2 的幂的分块，覆盖跨块增量计算
    chunks = [body[i:i + 333] for i in range(0, len(body), 333)]
    good = hashlib.sha256(body).hexdigest()

    async def reset():
        await harness.stats.reset()

    asyncio.get_event_loop().run_until_complete(reset())
    status, resp_body, total = _raw_upload(
        *harness.gateway_addr, "/verify", chunks, good,
    )
    stats = asyncio.get_event_loop().run_until_complete(harness.stats.snapshot())

    assert status == 201, resp_body
    payload = json.loads(resp_body)
    assert payload["declared_sha256"] == good
    assert payload["gateway_sha256"] == good      # 网关独立算出的实际摘要
    assert payload["origin_sha256"] == good       # 源站对实收正文算出的摘要
    assert stats["verified_success"] == 1
    assert stats["verified_failure"] == 0
    assert stats["body_bytes"].get("/verify") == len(body)


def test_verify_rejects_wrong_digest_and_does_not_count(harness):
    body = _synthetic_body(30_000)
    wrong = hashlib.sha256(b"not-the-body").hexdigest()

    asyncio.get_event_loop().run_until_complete(harness.stats.reset())
    status, resp_body, _ = _raw_upload(
        *harness.gateway_addr, "/verify", [body], wrong,
    )
    stats = asyncio.get_event_loop().run_until_complete(harness.stats.snapshot())

    assert status == 422
    payload = json.loads(resp_body)
    assert payload["gateway_sha256"] != wrong
    assert payload["gateway_sha256"] == payload["origin_sha256"]
    assert stats["verified_success"] == 0
    assert stats["verified_failure"] == 1


@pytest.mark.parametrize("digest_mode", ["missing", "malformed"])
def test_verify_requires_valid_digest_header(harness, digest_mode):
    body = _synthetic_body(10_000)
    declared = None if digest_mode == "missing" else "abc123"

    asyncio.get_event_loop().run_until_complete(harness.stats.reset())
    status, _, _ = _raw_upload(
        *harness.gateway_addr, "/verify", [body], declared,
    )
    stats = asyncio.get_event_loop().run_until_complete(harness.stats.snapshot())

    # 请求完整但摘要缺失/非法：422，且不计成功
    assert status == 422
    assert stats["verified_success"] == 0


def test_verify_terminal_abort_before_complete_not_counted(harness):
    body = _synthetic_body(200_000)
    good = hashlib.sha256(body).hexdigest()

    asyncio.get_event_loop().run_until_complete(harness.stats.reset())
    status, _, _ = _raw_upload(
        *harness.gateway_addr, "/verify",
        [body[i:i + 1000] for i in range(0, len(body), 1000)], good,
        abandon_after=50_000,
    )
    asyncio.get_event_loop().run_until_complete(asyncio.sleep(0.2))
    stats = asyncio.get_event_loop().run_until_complete(harness.stats.snapshot())

    assert status is None                      # 终端没等到最终响应
    assert stats["verified_success"] == 0      # 未发完 → 不计成功
    assert stats["body_bytes"].get("/verify", 0) < len(body)


def test_verify_origin_rejects_midway_terminal_need_not_finish(harness):
    body = _synthetic_body(200_000)
    good = hashlib.sha256(body).hexdigest()

    async def scenario():
        await harness.stats.reset()
        result = await upload_once(
            *harness.gateway_addr, "/verify-reject-mid", len(body),
            body=body, digest_value=good,
        )
        await asyncio.sleep(0.2)
        stats = await harness.stats.snapshot()
        return result, stats

    result, stats = asyncio.get_event_loop().run_until_complete(scenario())

    assert result.got_100 is True
    assert result.status_code == 413
    # 终端无需发完正文就能拿到最终响应
    assert "body_send_aborted" in _timeline_events(result)
    assert result.body_bytes_sent < len(body)
    assert stats["verified_success"] == 0
    # 源站提前拒收：只收到阈值附近字节，远小于整个 body
    received = stats["body_bytes"].get("/verify-reject-mid", 0)
    assert 4096 <= received < len(body)

