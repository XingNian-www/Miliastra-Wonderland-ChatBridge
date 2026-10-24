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
| GET | `/health` | 服务与数据源状态 |
| GET | `/messages` | 查询缓冲消息 |
| GET | `/names` | 批量查询昵称 |
| WebSocket | `/stream` | 实时事件订阅 |

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
  "subscribers": 1
}
```

`currentHall` 未知时为 `null`，其中 `type` 未知时也可为 `null`；`subscribers` 为实时订阅数。

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
