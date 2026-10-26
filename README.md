# Miliastra-Wonderland-ChatBridge

用于本地消息接入与应用集成的辅助工具，提供 HTTP / WebSocket 接口。

## 构建

宿主使用 Rust，原生组件使用 .NET 9。完整构建需要 Windows x64 及相应的 MSVC / Windows SDK 工具链。

```sh
cargo build --release
dotnet publish native/MiliastraWonderlandChatBridge.Native.csproj -c Release
```

## 使用

从 GitHub Releases 下载 Windows x64 压缩包并解压，使用独立端口和自定义令牌启动。包内包含宿主、原生组件和版本相关的 `metadata.bin`；游戏更新后可能需要更新该资源。启动前退出已有游戏进程。

```sh
miliastra-wonderland-chatbridge --port 18889 --token <自定义令牌> --self-uid <UID>
```

服务监听本机地址。请求使用 `x-miliastra-token` 鉴权，提供 `/health`、`/messages`、`/names` 和 `/stream` 接口。使用 `--help` 查看启动参数。

### 实时观察页

浏览器打开 `http://127.0.0.1:<监听端口>/`，填写启动参数或配置里的访问令牌，即可实时观察注入链路检测到的消息、当前大厅和连接状态。页面用 `/stream` 订阅、`/messages` 补拉；桥接重启（消息 `id` 归零）时会清空旧列表并重新建立基线。

页面是内嵌的单文件 HTML，不引用外部资源；令牌只保存在浏览器 `localStorage`，不写入页面，也不随页面响应返回。

## 诊断

被 Music 以隐藏窗口拉起时，stdout 进 NUL，控制台什么也看不到。为此提供四类观测手段：

- **文件日志**：默认写到程序同目录的 `chatbridge.log`（超过 8 MiB 滚动为 `.log.1`）。可用 `--log-file <path>` 改路径，`--no-log-file` 关闭。
- **`GET /health` 的 `diag` 字段**：一眼判断死活——`dllConnected`、`framesPerSec`、`lastFrameAgeMs`、`ackP99Us`、`spinHitRate`、`lastMessageAgeMs` 等。
- **`GET /debug/metrics`**：完整快照（管道吞吐、回执/解析分位、缓冲字节、订阅数、丢帧滞后、高频 cmd、构建版本）。
- **`GET /debug/events`**：最近 64 条关键事件（连接/断开、协议错误、订阅滞后），带相对启动的毫秒偏移。

启动前可先跑自检，不启动游戏、不注入：

```sh
miliastra-wonderland-chatbridge --diagnose --port 18889 --token <令牌> --self-uid <UID>
```

它会检查端口占用、DLL、`metadata.bin`，Windows 上还会校验游戏 exe 的 CRC32 与 RVA 表完整性。

### 排查"消息延迟"的顺序

1. `/health` 的 `diag.framesPerSec`：约 20/秒说明收包循环仍被 50ms 轮询限制；数百以上说明管道正常。
2. `diag.ackP99Us`：接近 50ms 代表每包都在等空闲轮询；个位数毫秒代表自旋命中。
3. 延迟发生时立刻 `GET /messages?since=<最后一条 id>`：立刻有 → 延迟在 WS/观察台；没有 → 还在管道或注入。
4. `/debug/events` 里出现 `ws_lagged` 表示订阅端落后被断开，需要看页面是否卡顿。

## 许可与致谢

本项目采用 **GNU GPL v3.0（GPL-3.0-only）**，完整条款见 [LICENSE](LICENSE)。第三方组件保留各自的版权与许可。

原生组件基于 [YaeAchievement](https://github.com/HolographicHat/Yae) 修改。原作者版权声明：Copyright (C) 2022–2024 HoloHat。修改包括消息接入、生命周期、资源管理与健壮性处理；上游原有的 GPL v3 或更高版本授权声明予以保留。

包含 [MinHook](https://github.com/TsudaKageyu/minhook)（作者 Tsuda Kageyu），采用 BSD-2-Clause，条款见 [THIRD_PARTY_LICENSES](THIRD_PARTY_LICENSES)。
