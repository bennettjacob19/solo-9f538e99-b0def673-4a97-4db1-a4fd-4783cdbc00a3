"""全局配置：全部参数都可以通过环境变量覆盖，方便容器与自测使用。"""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass
class Settings:
    # 网关监听地址
    gateway_host: str = "0.0.0.0"
    gateway_port: int = 8080
    # 源站地址
    origin_host: str = "127.0.0.1"
    origin_port: int = 8081
    # FastAPI 演示面板
    demo_host: str = "0.0.0.0"
    demo_port: int = 8000
    # 等待源站许可（100-continue）的最长时间，超时网关回 504
    permission_timeout: float = 10.0
    # 连接上任意一次读事件的空闲超时
    idle_timeout: float = 30.0
    # 样例源站 /reject-mid 读取多少字节后拒收
    reject_after_bytes: int = 4096


def env_settings() -> Settings:
    def _get(name: str, default):
        value = os.environ.get(name)
        return default if value is None or value == "" else type(default)(value)

    return Settings(
        gateway_host=_get("GATEWAY_HOST", "0.0.0.0"),
        gateway_port=_get("GATEWAY_PORT", 8080),
        origin_host=_get("ORIGIN_HOST", "127.0.0.1"),
        origin_port=_get("ORIGIN_PORT", 8081),
        demo_host=_get("DEMO_HOST", "0.0.0.0"),
        demo_port=_get("DEMO_PORT", 8000),
        permission_timeout=_get("PERMISSION_TIMEOUT", 10.0),
        idle_timeout=_get("IDLE_TIMEOUT", 30.0),
        reject_after_bytes=_get("REJECT_AFTER_BYTES", 4096),
    )
