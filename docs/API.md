# API 参考

提供本地消息查询与实时订阅。HTTP 基础地址为 `http://127.0.0.1:18889`，端口以启动配置为准；响应使用 JSON。

## 鉴权

HTTP 请求及 WebSocket 握手使用请求头：

```http
x-miliastra-token: <token>
```

WebSocket 也支持 `?token=<token>`，适用于不能设置自定义请求头的客户端。

```sh
curl -H "x-miliastra-token: <token>" http://127.0.0.1:18889/health
```

## 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/` | 内置实时观察页（无需鉴权，不含消息数据） |
| GET | `/favicon.ico` | 空响应（`204`） |
| GET | `/health` | 服务与数据源状态 |
| GET | `/messages` | 查询缓冲消息 |
| GET | `/names` | 批量查询昵称 |
| WebSocket | `/stream` | 实时事件订阅 |
| GET | `/debug/metrics` | 完整运行指标（诊断） |
| GET | `/debug/events` | 最近关键事件（诊断） |
| GET | `/debug/packets` | 调试包模式快照（诊断/开关） |
| POST | `/debug/packets` | 切换调试包模式 |

### GET /

返回内置的实时观察页（单文件 HTML，不引用外部资源），无需鉴权：页面本身不含消息数据，打开后填写令牌即可查看实时消息与连接状态。令牌只保存在浏览器 `localStorage`；WS 使用 `?token=`，其余请求使用 `x-miliastra-token` 请求头。

桥接重启后消息 `id` 会归零，页面清空旧列表并从新基线开始。`GET /favicon.ico` 固定返回 `204`。

### GET /health

鉴权通过后返回 `200`，数据源是否可用由 `gameConnected` 表示。

```json
{
  "ok": true,
  "version": "0.1.0",
  "uptimeSec": 3600,
  "gameConnected": true,
  "currentHall": { "id": 1000000000000000001, "type": 16 },
  "lastMessageId": 42,
  "subscribers": 1,
  "diag": {
    "dllConnected": true,
    "framesPerSec": 21.4,
    "packetsPerSec": 20.9,
    "lastFrameAgeMs": 120,
    "ackP99Us": 51000,
    "spinHitRate": 0.02,
    "bufferedBytes": 0,
    "lastMessageAgeMs": 850,
    "subscribers": 1,
    "laggedTotal": 0,
    "build": "f418449"
  }
}
```

`currentHall` 未知时为 `null`，其中 `type` 未知时也可为 `null`；`subscribers` 为实时订阅数。

`diag.ackP99Us` 持续接近 50000 微秒，说明收包循环在等 50ms 空闲轮询；`lastMessageAgeMs` 是"距最近一条消息多久"，可用来区分"大厅没人说话"与"链路卡住"。

### GET /debug/metrics

返回完整诊断快照；字段较多且可能随版本增加，调用方应按需取用。

```json
{
  "uptimeMs": 3600000,
  "build": { "version": "0.1.2", "sha": "f418449", "builtAt": "2026-10-26T08:00:00Z" },
  "pipe": {
    "dllConnected": true,
    "framesTotal": 120000,
    "framesPerSec": 21.4,
    "packetsTotal": 118000,
    "packetsPerSec": 20.9,
    "bytesTotal": 128000000,
    "lastFrameAgeMs": 120,
    "bufferedBytes": 0,
    "ackCount": 118000,
    "ackAvgUs": 62,
    "ackP50Us": 48,
    "ackP99Us": 2400,
    "ackMaxUs": 51000,
    "dispatchAvgUs": 180,
    "dispatchP50Us": 150,
    "dispatchP99Us": 900,
    "dispatchMaxUs": 12000,
    "spinHit": 116000,
    "spinMiss": 2000,
    "spinHitRate": 0.983,
    "idlePolls": 8600
  },
  "chat": {
    "messagesTotal": 421,
    "lastMessageId": 421,
    "lastMessageTs": 1792772291,
    "lastMessageAgeMs": 850,
    "subscribers": 1,
    "laggedTotal": 0
  },
  "topCmds": [ { "cmd": 2246, "count": 117000 }, { "cmd": 486, "count": 1000 } ],
  "service": { "port": 18889, "selfUid": 100000009, "uptimeSec": 3600, "lastMessageId": 421, "currentHall": { "id": 1000000000000000001, "type": 16 } }
}
```

### GET /debug/events

最近 64 条关键事件，`offsetMs` 相对进程启动。

```json
{ "events": [
  { "offsetMs": 1200, "kind": "dll_connected", "detail": "" },
  { "offsetMs": 540000, "kind": "ws_lagged", "detail": "skipped=12" }
] }
```

### GET /debug/packets

返回调试包模式、DLL 控制通道状态、全包计数与 raw 抓包状态。默认 `chat`（只转发聊天包）；`all` 时 DLL 额外转发全部包。

```json
{
  "ok": true,
  "mode": "all",
  "dllControl": true,
  "counts": {
    "packets": 1200,
    "bytes": 345600,
    "cmds": {
      "2246": { "count": 1180, "bytes": 330400 },
      "4410": { "count": 20, "bytes": 15200 }
    }
  },
  "capture": {
    "enabled": true,
    "dir": "captures",
    "maxBytes": 1073741824,
    "files": 1180,
    "bytes": 330400,
    "dropped": 0,
    "stopped": false
  }
}
```

| 字段 | 类型 | 说明 |
|---|---|---|
| `ok` | bool | 恒为 `true` |
| `mode` | string | 当前模式：`chat` 或 `all` |
| `dllControl` | bool | DLL 控制管道是否已连接；旧 DLL 恒为 `false` |
| `counts.packets` | uint64 | 本次运行收到并统计的 debug 包数 |
| `counts.bytes` | uint64 | 上述包的 payload 总字节数 |
| `counts.cmds` | object | 按 cmd 统计：cmd（十进制字符串）→ `{count,bytes}` |
| `capture` | object 或 null | 未配置 `--capture-dir`，或配置了但目录初始化失败（宿主日志有 warn）时为 `null`，否则见下 |

`capture` 对象字段：`enabled`（是否已启用）、`dir`（落盘目录）、`maxBytes`（总字节上限）、`files`（已写文件数）、`bytes`（已写字节数）、`dropped`（因队列满或超限丢弃的抓包数）、`stopped`（是否已达上限停止抓包）。抓包只丢抓包，不影响转发。

`counts` 统计的是 debug 管道收到的包；切到 `chat` 后 DLL 不再转发全包，计数停在切换前的值（重启后清零）。

### POST /debug/packets

切换调试包模式，请求体为 JSON：

```json
{ "mode": "all" }
```

| 字段 | 类型 | 说明 |
|---|---|---|
| `mode` | string | 必填，只接受 `chat` 或 `all`（区分大小写） |

成功返回 `200`：

```json
{"ok":true,"mode":"all","applied":true,"dllControl":true}
```

| 字段 | 类型 | 说明 |
|---|---|---|
| `ok` | bool | 恒为 `true` |
| `mode` | string | 切换后的模式 |
| `applied` | bool | `true` 表示模式已写入 DLL 控制管道并在 DLL 生效；`false` 表示只在宿主侧记录 |
| `dllControl` | bool | 响应时 DLL 控制管道是否已连接 |

`applied` / `dllControl` 语义：

- `dllControl: true` + `applied: true`：控制管道已连接，DLL 已切换。
- `dllControl: false` + `applied: false`：DLL 未连接或为旧 DLL（没有控制管道）。宿主仍会记录新模式（后续 `GET /debug/packets` 的 `mode` 已是新值），但 DLL 行为不会改变。
- 宿主只接受一次控制/数据管道连接：DLL 断开后宿主记录的模式保留，但不会自动重连或重新下发；需要重启宿主与 DLL 才能恢复 debug 能力。

模式是运行期状态，**不持久化**：进程重启后回到 `chat`，需要重新调用本接口。

错误：令牌缺失或错误返回 `401`（`unauthorized`）；请求体不是合法 JSON、缺 `mode` 字段或类型不对返回 `400`（`bad_request`）；`mode` 不在 `chat` / `all` 内返回 `400`（`bad_request`，`message` 为 `mode must be chat or all`）。

```sh
curl -X POST -H "x-miliastra-token: <token>" -H "Content-Type: application/json" \
     -d '{"mode":"all"}' http://127.0.0.1:18889/debug/packets
```

### GET /messages

```http
GET /messages?since=40&limit=200
```

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `since` | uint64 | `0` | 返回 `id > since` 的消息 |
| `limit` | 非负整数 | `200` | 返回条数，最大 `1000` |

```json
{
  "messages": [
    {
      "id": 41,
      "ts": 1792772291,
      "channel": "current_hall",
      "hallId": 1000000000000000001,
      "hallType": 16,
      "from": { "uid": 100000001, "name": "Alice" },
      "to": null,
      "text": "hello"
    }
  ],
  "lastMessageId": 42,
  "truncated": false
}
```

消息按 `id` 升序返回，缓冲保留最近 2000 条。`truncated=true` 表示请求范围内有历史消息已被淘汰，不表示分页是否结束。

`lastMessageId` 是服务端当前最大 ID，不一定是本页最后一条。分页时使用本页最后一条消息的 `id` 作为下一次 `since`。

### GET /names

必填参数 `uids` 为逗号分隔的 uint32 UID，一次最多 100 个。

```http
GET /names?uids=100000001,100000002
```

```json
{
  "names": {
    "100000001": "Alice",
    "100000002": null
  }
}
```

昵称未知时为 `null`；消息身份与权限判断应使用 UID。昵称后续更新不会修改已缓存消息。

### WebSocket /stream

连接 `ws://127.0.0.1:18889/stream`，鉴权方式同上。事件使用 JSON 文本帧：

| `type` | 字段 | 说明 |
|---|---|---|
| `hello` | `version`、`lastMessageId` | 连接后的首个事件 |
| `message` | `message` | 一条 ChatMessage |
| `hall_changed` | `hallId`、`hallType` | 大厅状态变化 |

```json
{"type":"hello","version":"0.1.0","lastMessageId":42}
```

```json
{"type":"message","message":{"id":43,"ts":1792772292,"channel":"private","hallId":null,"hallType":null,"from":{"uid":100000001,"name":null},"to":100000002,"text":"hello"}}
```

```json
{"type":"hall_changed","hallId":1000000000000000001,"hallType":16}
```

服务端每 30 秒发送 Ping；客户端应在 10 秒内回复相同 payload 的 Pong，超时以关闭码 `1008` 断连。订阅积压导致事件丢失时，以 `1013` 断连；客户端应重连并通过 `/messages?since=<最后处理的ID>` 补拉，合并实时事件时按 `id` 去重。客户端发送的业务文本会被忽略。

## ChatMessage

| 字段 | 类型 | 说明 |
|---|---|---|
| `id` | uint64 | 进程内递增序号，从 1 开始；服务重启后重新计数 |
| `ts` | uint64 | Unix 秒级时间戳 |
| `channel` | string | `current_hall` 或 `private` |
| `hallId` | uint64 或 null | 大厅 ID；私聊为 null |
| `hallType` | uint32 或 null | 大厅类型；私聊为 null |
| `from.uid` | uint32 | 发送者 UID |
| `from.name` | string 或 null | 发送者昵称，可能尚未获取 |
| `to` | uint32 或 null | 接收者 UID |
| `text` | string | 消息正文 |

仅提供当前大厅与私聊文本，不推送公共频道、自己的发言、纯表情及系统消息。`id` 用于同次运行内排序和去重；重启后客户端需重置游标。大厅 ID 可能超过 JavaScript 安全整数范围，需使用支持无损整数的 JSON 解析方式。

## 错误响应

```json
{"error":{"code":"unauthorized","message":"invalid or missing x-miliastra-token"}}
```

| HTTP 状态码 | `error.code` | 说明 |
|---|---|---|
| 400 | `bad_request` | 参数格式错误 |
| 401 | `unauthorized` | 令牌缺失或错误 |
| 404 | `not_found` | 路径不存在 |
| 405 | `method_not_allowed` | 请求方法不支持 |
| 503 | `game_not_connected` | 数据源未连接，适用于 `/messages` 与 `/names` |
