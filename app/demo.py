"""FastAPI 演示面板。

启动后访问 ``/docs`` 可直接试传：

* ``POST /demo/upload`` —— 经由网关向样例源站发起一次 100-continue 上传，
  返回事件时间线、最终响应与上传前后的源站字节计数；
* ``GET  /demo/stats``  —— 查看源站实收正文字节数；
* ``POST /demo/stats/reset`` —— 清零源站计数。
"""

from __future__ import annotations

import logging
from enum import Enum

import httpx
from fastapi import FastAPI
from pydantic import BaseModel, Field

from .config import Settings, env_settings
from .rawclient import upload_once

log = logging.getLogger("demo")


class OriginMode(str, Enum):
    accept = "accept"
    reject = "reject"
    reject_mid = "reject-mid"
    hang = "hang"


PATH_BY_MODE = {
    OriginMode.accept: "/accept",
    OriginMode.reject: "/reject",
    OriginMode.reject_mid: "/reject-mid",
    OriginMode.hang: "/hang",
}


class UploadRequest(BaseModel):
    mode: OriginMode = Field(
        default=OriginMode.accept,
        description="源站行为：accept=正常放行；reject=许可前拒收；"
        "reject-mid=收到部分正文后中途拒收；hang=源站沉默触发 504",
    )
    body_size: int = Field(
        default=524288, ge=0, le=16 * 1024 * 1024,
        description="上传正文字节数（默认 512KB；观察 reject-mid 时建议保持较大，"
        "以便在发完前收到 413）",
    )
    reset_stats: bool = Field(
        default=True, description="试传前是否先清零源站计数器"
    )


class UploadResponse(BaseModel):
    mode: OriginMode
    path: str
    result: dict
    stats_before: dict | None
    stats_after: dict


class StatsResponse(BaseModel):
    stats: dict


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or env_settings()
    app = FastAPI(
        title="100-continue 上传网关演示",
        description=(
            "通过网关向样例源站发起带 `Expect: 100-continue` 的 POST，"
            "观察 100 许可、最终响应的先后顺序，以及源站实收的正文字节数。"
        ),
        version="1.0.0",
    )
    origin_base = (
        f"http://{settings.origin_host}:{settings.origin_port}"
    )

    async def _get_stats() -> dict:
        async with httpx.AsyncClient(timeout=5) as client:
            resp = await client.get(f"{origin_base}/stats")
            resp.raise_for_status()
            return resp.json()

    async def _reset_stats() -> None:
        async with httpx.AsyncClient(timeout=5) as client:
            await client.post(f"{origin_base}/stats/reset")

    @app.get("/demo/modes", tags=["demo"])
    async def modes() -> dict:
        return {
            "modes": [m.value for m in OriginMode],
            "paths": PATH_BY_MODE,
            "gateway": f"{settings.gateway_host}:{settings.gateway_port}",
            "origin": f"{settings.origin_host}:{settings.origin_port}",
            "permission_timeout_s": settings.permission_timeout,
        }

    @app.post("/demo/upload", response_model=UploadResponse, tags=["demo"])
    async def demo_upload(req: UploadRequest) -> UploadResponse:
        path = PATH_BY_MODE[req.mode]
        stats_before = None
        if req.reset_stats:
            await _reset_stats()
            stats_before = await _get_stats()

        result = await upload_once(
            settings.gateway_host if settings.gateway_host != "0.0.0.0"
            else "127.0.0.1",
            settings.gateway_port,
            path,
            req.body_size,
            permission_timeout=settings.permission_timeout + 5,
            overall_timeout=max(60.0, settings.permission_timeout * 4),
        )
        stats_after = await _get_stats()
        return UploadResponse(
            mode=req.mode,
            path=path,
            result=result.as_dict(),
            stats_before=stats_before,
            stats_after=stats_after,
        )

    @app.get("/demo/stats", response_model=StatsResponse, tags=["demo"])
    async def demo_stats() -> StatsResponse:
        return StatsResponse(stats=await _get_stats())

    @app.post("/demo/stats/reset", tags=["demo"])
    async def demo_stats_reset() -> dict:
        await _reset_stats()
        return {"reset": True}

    return app


def main() -> None:  # pragma: no cover
    import uvicorn

    settings = env_settings()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    uvicorn.run(
        create_app(settings),
        host=settings.demo_host,
        port=settings.demo_port,
        log_level="info",
    )


if __name__ == "__main__":  # pragma: no cover
    main()
