#!/usr/bin/env python3
"""交付前自测脚本。

覆盖：
  1. 正常上传（/accept）：100 → 正文转发 → 200，源站字节数与上传一致
  2. 许可前拒收（/reject）：终端收 403、正文 0 字节，源站实收 0 字节
  3. 中途拒收（/reject-mid）：终端收 413 且无需发完，源站只收到阈值附近字节
  4. 许可超时（/hang）：网关回 504，正文 0 字节，源站实收 0 字节
  5. 非法请求（缺 Expect / Content-Length）：网关直接 4xx

直接运行：``python scripts/selftest.py``（需在仓库根目录、依赖已安装）。
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.rawclient import upload_once  # noqa: E402
from tests.harness import Harness  # noqa: E402

PASS = "PASS"
FAIL = "FAIL"


def _tl(result, name: str) -> float | None:
    for item in result.timeline:
        if item["event"] == name:
            return item["t_ms"]
    return None


def _check(label: str, condition: bool, detail: str = "") -> bool:
    mark = PASS if condition else FAIL
    print(f"  [{mark}] {label}" + (f" —— {detail}" if detail else ""))
    return condition


async def run() -> int:
    harness = await Harness(permission_timeout=1.5,
                            reject_after_bytes=4096).start()
    host, port = harness.gateway_addr
    failures = 0
    try:
        # ------------------------------------------------------ 1. 正常上传
        print("场景 1：正常上传 /accept（20KB）")
        await harness.stats.reset()
        r = await upload_once(host, port, "/accept", 20_000,
                              permission_timeout=5)
        stats = await harness.stats.snapshot()
        ok = [
            _check("收到 100 Continue", r.got_100),
            _check("最终状态码 200", r.status_code == 200,
                   f"实际 {r.status_code}"),
            _check("正文 20000 字节全部发出", r.body_bytes_sent == 20_000,
                   f"实际 {r.body_bytes_sent}"),
            _check("100 早于正文发送",
                   _tl(r, "recv_100_continue") is not None
                   and _tl(r, "body_send_start") is not None
                   and _tl(r, "recv_100_continue") < _tl(r, "body_send_start")),
            _check("源站实收正文 20000 字节",
                   stats["body_bytes"].get("/accept") == 20_000,
                   f"统计 {stats['body_bytes']}"),
        ]
        failures += not all(ok)
        print("    时间线:", [(i["event"], i["t_ms"]) for i in r.timeline])

        # ------------------------------------------------- 2. 许可前拒收
        print("场景 2：源站许可前拒收 /reject（50KB，应一字不转）")
        await harness.stats.reset()
        r = await upload_once(host, port, "/reject", 50_000,
                              permission_timeout=5)
        stats = await harness.stats.snapshot()
        ok = [
            _check("未收到 100", not r.got_100),
            _check("最终状态码 403", r.status_code == 403,
                   f"实际 {r.status_code}"),
            _check("终端正文发出 0 字节", r.body_bytes_sent == 0),
            _check("源站实收正文 0 字节",
                   stats["body_bytes"].get("/reject", 0) == 0,
                   f"统计 {stats['body_bytes']}"),
        ]
        failures += not all(ok)
        print("    时间线:", [(i["event"], i["t_ms"]) for i in r.timeline])

        # ------------------------------------------------- 3. 中途拒收
        print("场景 3：源站中途拒收 /reject-mid（100KB，阈值 4096）")
        await harness.stats.reset()
        r = await upload_once(host, port, "/reject-mid", 100_000)
        await asyncio.sleep(0.2)
        stats = await harness.stats.snapshot()
        origin_received = stats["body_bytes"].get("/reject-mid", 0)
        ok = [
            _check("先收到 100", r.got_100),
            _check("最终状态码 413", r.status_code == 413,
                   f"实际 {r.status_code}"),
            _check("终端未发完即中止", r.body_bytes_sent < 100_000,
                   f"已发 {r.body_bytes_sent} / 100000"),
            _check("最终响应在发送完成前到达",
                   _tl(r, "recv_final_response") is not None),
            _check("源站只读到阈值附近字节",
                   4096 <= origin_received < 100_000,
                   f"源站实收 {origin_received}"),
        ]
        failures += not all(ok)
        print("    时间线:", [(i["event"], i["t_ms"]) for i in r.timeline])

        # ------------------------------------------------- 4. 许可超时
        print("场景 4：源站沉默 /hang（网关许可超时 1.5s，期望 504）")
        await harness.stats.reset()
        t0 = asyncio.get_event_loop().time()
        r = await upload_once(host, port, "/hang", 30_000,
                              permission_timeout=5)
        elapsed = asyncio.get_event_loop().time() - t0
        stats = await harness.stats.snapshot()
        ok = [
            _check("未收到 100", not r.got_100),
            _check("网关回 504", r.status_code == 504,
                   f"实际 {r.status_code}"),
            _check("终端正文发出 0 字节", r.body_bytes_sent == 0),
            _check("源站实收正文 0 字节",
                   stats["body_bytes"].get("/hang", 0) == 0),
            _check("约在许可超时后返回（<4s）", elapsed < 4,
                   f"耗时 {elapsed:.2f}s"),
        ]
        failures += not all(ok)
        print("    时间线:", [(i["event"], i["t_ms"]) for i in r.timeline])

        # ------------------------------------------------- 5. 非法请求
        print("场景 5：非法请求直接 4xx")
        import h11

        from app.h11io import read_event, send_event

        async def bad_request(headers):
            reader, writer = await asyncio.open_connection(host, port)
            conn = h11.Connection(our_role=h11.CLIENT)
            await send_event(conn, writer, h11.Request(
                method=b"POST", target=b"/accept", headers=headers))
            event = await read_event(conn, reader, 5)
            writer.close()
            return event.status_code

        s1 = await bad_request([(b"host", b"t"), (b"content-length", b"10")])
        s2 = await bad_request([(b"host", b"t"),
                                (b"expect", b"100-continue")])
        ok = [
            _check("缺 Expect → 417", s1 == 417, f"实际 {s1}"),
            _check("缺 Content-Length → 411", s2 == 411, f"实际 {s2}"),
        ]
        failures += not all(ok)
    finally:
        await harness.stop()

    print()
    if failures:
        print(f"自测失败：{failures} 组断言未通过")
        return 1
    print("全部自测通过 ✅")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(run()))
