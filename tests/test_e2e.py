"""端到端测试：正常上传、许可前拒收、中途拒收、许可超时、连接只处理一次。"""

from __future__ import annotations

import asyncio
import contextlib
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
