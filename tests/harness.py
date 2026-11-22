"""自测/测试共用的进程内服务脚手架：在随机端口上启动源站与网关。"""

from __future__ import annotations

import asyncio

from app import gateway, origin
from app.config import Settings
from app.origin import OriginStats


class Harness:
    def __init__(self, *, permission_timeout: float = 2.0,
                 reject_after_bytes: int = 4096):
        self.settings = Settings(
            gateway_host="127.0.0.1",
            gateway_port=0,
            origin_host="127.0.0.1",
            origin_port=0,
            permission_timeout=permission_timeout,
            idle_timeout=10.0,
            reject_after_bytes=reject_after_bytes,
        )
        self.stats = OriginStats()
        self._origin_server: asyncio.AbstractServer | None = None
        self._gateway_server: asyncio.AbstractServer | None = None

    async def start(self) -> "Harness":
        s = self.settings
        self._origin_server = await asyncio.start_server(
            lambda r, w: origin._handle(r, w, s, self.stats),
            host="127.0.0.1", port=0,
        )
        s.origin_port = self._origin_server.sockets[0].getsockname()[1]
        self._gateway_server = await asyncio.start_server(
            lambda r, w: gateway.handle_terminal(r, w, s),
            host="127.0.0.1", port=0,
        )
        s.gateway_port = self._gateway_server.sockets[0].getsockname()[1]
        return self

    async def stop(self) -> None:
        for server in (self._gateway_server, self._origin_server):
            if server is not None:
                server.close()
                await server.wait_closed()

    @property
    def gateway_addr(self) -> tuple[str, int]:
        return "127.0.0.1", self.settings.gateway_port
