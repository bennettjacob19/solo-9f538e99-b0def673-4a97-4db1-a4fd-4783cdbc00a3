# 100-continue HTTP/1.1 上传网关（Python + h11）

站点终端上传诊断包时，先等源站许可再走正文，避免慢链路上白传。本项目用
[h11](https://h11.readthedocs.io/) 状态机 + asyncio 实现一个 HTTP/1.1 上传网关，
并附带本地样例源站与 FastAPI 演示面板。

除了原有的“先许可、后正文”流程，网关还提供一个**独立的 SHA-256 校验入口**
`POST /verify`：终端声明摘要，网关边转发边独立计算，收完整包后用 HTTP trailer
把实际摘要送给源站，源站在三份摘要相符时才增加成功计数并回 201。

## 行为约定

每条 TCP 连接只处理一次带 `Content-Length` 与 `Expect: 100-continue` 的 POST：

```
终端                     网关                      源站
 │  POST + Expect 头      │                        │
 │───────────────────────>│  转发请求头（无正文）   │
 │                        │───────────────────────>│
 │                        │   ① 100 Continue        │
 │                        │<───────────────────────│
 │  100 Continue（许可）   │                        │
 │<───────────────────────│                        │
 │  正文分块               │  边收边转正文           │
 │───────────────────────>│───────────────────────>│
 │                        │   最终响应 200/...      │
 │  最终响应               │<───────────────────────│
 │<───────────────────────│                        │
```

* **源站放行（100）**：网关把 100 转给终端，随后边收边转正文，再转回最终响应。
* **源站许可前直接拒收（最终响应）**：网关原样转回最终响应，**正文一字不转**。
* **等许可超时**：网关自己回 `504 Gateway Timeout`，**正文一字不转**。
* **源站中途拒收**：网关立即停止读/转正文并把最终响应转给终端，
  终端无需发完 body 就能收到最终响应。
* 连接处理完即关，不做 keep-alive 复用；非法请求（非 POST、缺 Expect、
  缺/错 Content-Length、用 Transfer-Encoding）直接回 4xx。

### SHA-256 校验入口 `POST /verify`

```
终端                       网关                        源站
 │ X-Content-SHA256: H(正文) │                          │
 │ Expect: 100-continue      │                          │
 │──────────────────────────>│  转发请求头 + Trailer 声明 │
 │                           │─────────────────────────>│
 │                           │      100 Continue         │
 │   100 Continue            │<─────────────────────────│
 │<──────────────────────────│                          │
 │  正文分块（Content-Length）│  边转发边算 SHA-256        │
 │──────────────────────────>│─────────────────────────>│ 边收边算 SHA-256
 │                           │                          │
 │                           │  0\r\nX-Content-SHA256: 实际摘要\r\n\r\n（trailer）
 │                           │─────────────────────────>│
 │                           │   201 / 422 + JSON        │ 三份摘要比对
 │   201 / 422               │<─────────────────────────│
 │<──────────────────────────│                          │
```

三份摘要：

1. **终端声明**：`X-Content-SHA256` 请求头，必须是 64 个十六进制字符；
2. **网关实际**：网关对真正转发的每个字节增量计算，收完整包后放在 HTTP
   trailer 的 `X-Content-SHA256` 里送出（h11 不允许 Content-Length 与 trailer
   并存，因此网关→源站这一段自动改用 `Transfer-Encoding: chunked`；
   终端→网关仍保持 Content-Length）；
3. **源站实收**：源站对真正收到的正文字节独立计算，**不信任**前两份。

判定与计数：

| 情形 | 结果 |
| --- | --- |
| 请求完整、三份摘要相符 | **201**，源站 `verified_success` +1 |
| 请求完整、摘要缺失/格式非法/不符 | **422**，不增加成功计数 |
| 终端未发完就断开 | 该次上传**不计入成功数** |
| 源站提前/中途拒收（如 `413`） | 网关立即停止转发，终端无需发完，**不计成功** |
| 源站许可前拒收 / 网关许可超时 | 原样转回最终响应 / 回 504，正文不转发，**不计成功** |

## 目录结构

| 路径 | 说明 |
| --- | --- |
| `app/h11io.py` | h11 与 asyncio 流的粘合层：事件收发、逐跳头过滤、简单响应 |
| `app/digest.py` | SHA-256 常量与工具：头名、64 位十六进制校验、增量计算器 |
| `app/gateway.py` | 上传网关核心：请求校验、许可等待、正文边收边转、摘要与 trailer、超时/拒收处理 |
| `app/origin.py` | 样例源站：`/accept` `/verify` `/verify-reject-mid` `/reject` `/reject-mid` `/hang` + 统计 `/stats` |
| `app/rawclient.py` | h11 试传客户端：严格“先许可后正文”，可带自定义摘要，记录毫秒级事件时间线 |
| `app/demo.py` | FastAPI 面板（`/docs`），调用试传客户端并返回时间线、三份摘要与源站计数 |
| `app/config.py` | 环境变量配置 |
| `tests/` | pytest 端到端测试（进程内起服务，13 个用例） |
| `scripts/selftest.py` | 交付前自测脚本（正常上传 + 两种拒收 + 超时 + 非法请求 + SHA-256 五种情形） |
| `Dockerfile` / `docker-compose.yml` | 容器化，compose 一键起三个服务 |

## 快速开始

### docker compose（推荐）

```bash
docker compose up --build
```

| 服务 | 地址 | 用途 |
| --- | --- | --- |
| origin（样例源站） | `:8081` | 接收/拒收上传，校验摘要，统计成功计数 |
| gateway（上传网关） | `:8080` | 终端实际连接的网关 |
| demo（FastAPI） | `:8000` | 浏览器打开 <http://localhost:8000/docs> |

在 `/docs` 里调 `POST /demo/upload`：

* `mode=verify`（默认）—— 走 SHA-256 校验入口；
  `digest` 可选 `correct`（应 201）、`wrong`（应 422）、`missing`（应 422）、
  `malformed`（应 422）；
* `mode=verify-reject-mid` —— 校验入口但源站读够阈值后提前拒收（413）；
* `mode=accept` —— 普通放行；
* `mode=reject` —— 源站看头后直接 403（许可前拒收）；
* `mode=reject-mid` —— 源站收 4096 字节后回 413（中途拒收，默认 body 512KB，
  可观察到终端未发完即中止）；
* `mode=hang` —— 源站沉默，网关许可超时（compose 中为 3s）后回 504。

响应中 `result.timeline` 是事件时间线（请求头发送 → 100 → 正文开始/中止 →
最终响应），`body_sha256` / `sent_sha256` 是正文真实摘要与终端实际发送的摘要，
`stats_before` / `stats_after` 里的 `verified_success` 是源站成功计数。

用“正确摘要”试传一次，`stats_after.verified_success` 会从 0 变 1；
换成 `digest=wrong` 再试，状态码为 422 且 `verified_success` 保持 0。

### 本地直接运行

```bash
python -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/python -m app.origin      # :8081
.venv/bin/python -m app.gateway     # :8080
.venv/bin/python -m app.demo        # :8000，然后开 /docs
```

也可以用原生 curl 验证 SHA-256 校验入口（curl 支持 Expect 与自定义头）：

```bash
head -c 100000 /dev/zero | tr '\0' a > /tmp/pkg.bin
SHA=$(sha256sum /tmp/pkg.bin | awk '{print $1}')
curl -i -X POST --data-binary @/tmp/pkg.bin \
  -H "Expect: 100-continue" -H "X-Content-SHA256: $SHA" \
  http://localhost:8080/verify        # → 201

curl -i -X POST --data-binary @/tmp/pkg.bin \
  -H "Expect: 100-continue" \
  -H "X-Content-SHA256: 0000000000000000000000000000000000000000000000000000000000000000" \
  http://localhost:8080/verify        # → 422
```

## 自测

```bash
.venv/bin/pip install -r requirements-dev.txt
.venv/bin/python -m pytest tests/ -q      # 13 个端到端用例
.venv/bin/python scripts/selftest.py      # 场景化自测，输出每条断言
```

自测覆盖 SHA-256 相关的：跨块（奇数分块）正确摘要 201、错误摘要 422、
缺失摘要 422、终端未发完断开不计成功、源站中途拒收 413 且不计成功。

## 关键配置（环境变量）

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `GATEWAY_HOST/PORT` | `0.0.0.0:8080` | 网关监听 |
| `ORIGIN_HOST/PORT` | `127.0.0.1:8081` | 源站地址 |
| `DEMO_HOST/PORT` | `0.0.0.0:8000` | FastAPI 面板 |
| `PERMISSION_TIMEOUT` | `10` | 等源站 100 许可的秒数，超时回 504 |
| `IDLE_TIMEOUT` | `30` | 连接读空闲超时 |
| `REJECT_AFTER_BYTES` | `4096` | 样例源站 `/reject-mid`、`/verify-reject-mid` 读多少字节后拒收 |

## 边界与限制

* 终端→网关仅支持 `Content-Length` 请求，不支持 `Transfer-Encoding: chunked`；
  网关→源站仅在 SHA-256 校验入口上使用 chunked（为了携带 trailer）。
* 每条连接一个请求，不做连接复用（`Connection: close`）。
* SHA-256 校验入口要求请求头里就带上 `X-Content-SHA256`。摘要缺失或格式非法时，
  网关仍会完整转发正文，由源站判定后回 422（不计成功并给出原因）；网关不替
  源站下结论。摘要比较不区分大小写（统一按小写十六进制比对）。
* 样例源站为演示用途，生产使用时把 `ORIGIN_HOST/PORT` 指向真实源站即可，
  网关协议逻辑无需改动；真实源站需支持 chunked 请求体与 HTTP trailer
  才能接收摘要。
