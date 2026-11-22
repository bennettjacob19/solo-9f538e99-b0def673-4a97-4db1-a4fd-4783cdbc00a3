"""FastAPI 演示面板。

启动后访问 ``/docs`` 可直接试传：

* ``POST /demo/upload`` —— 经由网关向样例源站发起一次 100-continue 上传，
  返回事件时间线、最终响应、三份 SHA-256 摘要与上传前后的源站计数；
  ``mode=verify`` 时可选 ``digest=correct|wrong|missing|malformed``，
  用正确或错误摘要试传，并观察源站成功计数是否增加；
* ``GET  /demo/stats``  —— 查看源站实收正文字节数与 ``verified_success`` 计数；
* ``POST /demo/stats/reset`` —— 清零源站计数。
"""

from __future__ import annotations

import hashlib
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
    verify = "verify"
    verify_reject_mid = "verify-reject-mid"
    reject = "reject"
    reject_mid = "reject-mid"
    hang = "hang"


class DigestMode(str, Enum):
    correct = "correct"
    wrong = "wrong"
    missing = "missing"
    malformed = "malformed"


PATH_BY_MODE = {
    OriginMode.accept: "/accept",
    OriginMode.verify: "/verify",
    OriginMode.verify_reject_mid: "/verify-reject-mid",
    OriginMode.reject: "/reject",
    OriginMode.reject_mid: "/reject-mid",
    OriginMode.hang: "/hang",
}


class UploadRequest(BaseModel):
    mode: OriginMode = Field(
        default=OriginMode.verify,
        description="源站行为：verify=校验入口（默认，三份摘要相符才 201）；"
        "verify-reject-mid=校验入口但源站读够阈值后提前拒收；"
        "accept=普通放行；reject=许可前拒收；reject-mid=中途拒收；"
        "hang=源站沉默触发 504",
    )
    body_size: int = Field(
        default=524288, ge=0, le=16 * 1024 * 1024,
        description="上传正文字节数（默认 512KB；观察中途拒收时建议保持较大，"
        "以便在发完前收到 413）",
    )
    digest: DigestMode = Field(
        default=DigestMode.correct,
        description="X-Content-SHA256 的取值：correct=真实摘要（应 201）；"
        "wrong=错误但格式合法的摘要（应 422）；missing=不发该头（应 422）；"
        "malformed=非 64 位十六进制（网关立即 422）",
    )
    reset_stats: bool = Field(
        default=True, description="试传前是否先清零源站计数器"
    )


class UploadResponse(BaseModel):
    mode: OriginMode
    path: str
    digest: DigestMode
    body_sha256: str
    sent_sha256: str | None
    result: dict
    stats_before: dict | None
    stats_after: dict


class StatsResponse(BaseModel):
    stats: dict


def _synthetic_body(size: int) -> bytes:
    """可复现的演示正文：周期性文本，便于跨块计算与人工比对摘要。"""
    pattern = b"diagnostic-package/0123456789\n"
    full, rest = divmod(size, len(pattern))
    return pattern * full + pattern[:rest]


def _digest_header_value(mode: DigestMode, body_sha256: str) -> str | None:
    if mode is DigestMode.correct:
        return body_sha256
    if mode is DigestMode.wrong:
        # 格式合法、但和正文对不上
        return hashlib.sha256(b"definitely-not-the-body").hexdigest()
    if mode is DigestMode.malformed:
        return "not-a-valid-sha256"
    return None  # missing


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or env_settings()
    app = FastAPI(
        title="100-continue 上传网关演示",
        description=(
            "通过网关向样例源站发起带 `Expect: 100-continue` 的 POST，"
            "观察 100 许可、最终响应的先后顺序、三份 SHA-256 摘要，以及"
            "源站 `verified_success` 成功计数。\n\n"
            "**SHA-256 校验流程**：终端在 `X-Content-SHA256` 声明摘要 → "
            "网关边转发边独立计算，收完整包后用 HTTP trailer 送出实际摘要 → "
            "源站对实收正文再算一次；三份相符才 201，否则 422。"
        ),
        version="1.1.0",
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
            "digest_modes": [m.value for m in DigestMode],
            "paths": PATH_BY_MODE,
            "gateway": f"{settings.gateway_host}:{settings.gateway_port}",
            "origin": f"{settings.origin_host}:{settings.origin_port}",
            "permission_timeout_s": settings.permission_timeout,
        }

    @app.post("/demo/upload", response_model=UploadResponse, tags=["demo"])
    async def demo_upload(req: UploadRequest) -> UploadResponse:
        path = PATH_BY_MODE[req.mode]
        body = _synthetic_body(req.body_size)
        body_sha256 = hashlib.sha256(body).hexdigest()
        sent_sha256 = _digest_header_value(req.digest, body_sha256)

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
            body=body,
            digest_value=sent_sha256,
        )
        stats_after = await _get_stats()
        return UploadResponse(
            mode=req.mode,
            path=path,
            digest=req.digest,
            body_sha256=body_sha256,
            sent_sha256=sent_sha256,
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
