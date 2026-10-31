# Miliastra-Wonderland-ChatBridge

用于本地消息接入与应用集成的辅助工具，提供 HTTP / WebSocket 接口。

## 构建

宿主使用 Rust，原生组件使用 .NET 9。完整构建需要 Windows x64 及相应的 MSVC / Windows SDK 工具链。

```sh
cargo build --release
dotnet publish native/MiliastraWonderlandChatBridge.Native.csproj -c Release
```

## 使用

从 GitHub Releases 下载 Windows x64 压缩包并解压，使用独立端口和自定义令牌启动。包内包含宿主、原生组件和版本相关的 `metadata.bin`；游戏更新后可能需要更新该资源。游戏已在运行时默认进入 reattach 等待（见「生命周期：reattach 与退出码」），不需要先手动退出；只有用 `--reattach-seconds 0` 关闭 reattach 时才要求先退出游戏。

```sh
miliastra-wonderland-chatbridge --port 18889 --token <自定义令牌> --self-uid <UID>
```

服务监听本机地址。请求使用 `x-miliastra-token` 鉴权，提供 `/health`、`/messages`、`/names` 和 `/stream` 接口。使用 `--help` 查看启动参数。

### 实时观察页

浏览器打开 `http://127.0.0.1:<监听端口>/`，填写启动参数或配置里的访问令牌，即可实时观察注入链路检测到的消息、当前大厅和连接状态。页面用 `/stream` 订阅、`/messages` 补拉；桥接重启（消息 `id` 归零）时会清空旧列表并重新建立基线。

页面是内嵌的单文件 HTML，不引用外部资源；令牌只保存在浏览器 `localStorage`，不写入页面，也不随页面响应返回。

## 生命周期：reattach 与退出码

上游 Miliastra-Wonderland-Music 依赖本进程的退出码决定下一步。这套契约一旦发布就不要再改：如果所有失败都塌缩成 0/1，启动器就无法区分「正常结束」「等玩家退出」和「链路已失效必须重启游戏」，只能静默降级或误重试。

### 游戏已在运行：reattach

旧行为是游戏已在运行时直接以退出码 3 结束。这样 DLL 侧的重连逻辑永远等不到宿主，因此现在默认不再退出，而是创建同名管道等待游戏内的 DLL 主动重连。DLL 的重连退避是 1s→30s 封顶，默认 60s 窗口足够覆盖至少两次重连尝试。

- `--reattach-seconds <n>`：等待窗口秒数，默认 `60`；`0` 表示禁用 reattach，回到旧行为（游戏已在运行就直接以退出码 3 结束）。
- 游戏已在运行且 reattach 启用：不启动新游戏、不注入、不重新加载 DLL，只创建 `MiliastraWonderlandChatBridgePipe` 等服务端管道，等旧 DLL 自己连上来。
- reattach 启用且还有轮数时，收包会话读到 EOF 而游戏仍存活：不退出，重新进入等待。一个进程最多 3 轮窗口（`MAX_REATTACH_ROUNDS`），总等待上限约 `3 × reattach-seconds`（默认约 180s）。
- 窗口/轮数耗尽且游戏仍在运行：以退出码 4 结束，表示聊天链路无法恢复。
- 等待期间游戏退出：以退出码 0 正常结束。
- 该逻辑只作用于默认自动拉起路径；`--no-launch` 只提供 HTTP，不启动注入，也不会进入 reattach 等待。

reattach 的安全边界是**只建管道、只等待**：绝不 `CreateProcess`、`TerminateProcess` 或重复注入。这也意味着如果游戏启动时没有加载过本 DLL（或 DLL 已彻底退出），reattach 等不到连接，最终仍会以退出码 4 结束；这种情况只能重启游戏重新注入。

等待与失败轮数可以从 `/health` 的 `attached`、`reattachWaiting`、`reattachAttempts` 观察（见 [docs/API.md](docs/API.md)）。

### 退出码（供 Miliastra-Wonderland-Music）

| 退出码 | 含义 | 何时出现 | 上游建议动作 |
|---|---|---|---|
| `0` | 正常结束 | 游戏退出、Ctrl-C，或 DLL 主动结束会话 | 释放资源，下一轮可正常重新拉起；不需要报警或降级 |
| `1` | 启动错误 | 端口占用、DLL/metadata 缺失、参数错误或监听失败等启动阶段错误 | 记录日志并提示修复；不要无限循环重试，也不要静默改走 OCR |
| `3` | 游戏已在运行 | 仅在 reattach 被禁用（`--reattach-seconds 0`）且游戏已在运行时 | 等玩家退出游戏后重试；不需要人工修复，不要降级 |
| `4` | 链路丢失且游戏仍在运行 | reattach 窗口/轮数耗尽，或 reattach 禁用后主通道无法恢复，而游戏仍存活 | 默认 reattach 已经尝试过（约 `3 × reattach-seconds`）；优先提示玩家重启游戏。若确认游戏内旧 DLL 仍在重连，可在有限次数内重拉宿主依赖 reattach 恢复；禁止无限重启宿主或静默降级 |

补充：

- `0` / `1` / `3` / `4` 是稳定契约，不要改变含义；上游据此决定「直接重拉」「等玩家退出」「提示重启游戏」还是「报错退出」。
- 默认 reattach 开启时，游戏已在运行不会再返回 `3`；`3` 只用于关闭 reattach 的兼容路径。
- 退出码 `4` 不是「可以立刻自动重启宿主」的信号：默认策略已经等过最多 3 轮窗口。优先让玩家重启游戏；只有确认旧 DLL 仍在重连时，重拉宿主才有恢复意义。

## 诊断

被 Music 以隐藏窗口拉起时，stdout 进 NUL，控制台什么也看不到。为此提供四类观测手段：

- **文件日志**：默认写到程序同目录的 `chatbridge.log`（超过 8 MiB 滚动为 `.log.1`）。可用 `--log-file <path>` 改路径，`--no-log-file` 关闭。
- **`GET /health`**：顶层的 `attached` / `reattachWaiting` / `reattachAttempts` 直接反映会话是否接入、是否在等 DLL 重连；`diag` 里的 `dllConnected`、`framesPerSec`、`lastFrameAgeMs`、`ackP99Us`、`spinHitRate`、`lastMessageAgeMs` 等用于判断死活。
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

### 链路自愈与超时（DLL 侧）

DLL 在宿主重启/闪退后会自行重连聊天主通道；默认 reattach 配置下不需要重启游戏。重连与超时行为如下：

- 聊天主通道断开后由独立后台线程重连，退避 1s→2s→4s…封顶 30s；宿主重建同名管道等待即可（reattach 做的就是这件事）。重连期间钩子只把包复制进有界队列，不阻塞游戏网络线程。
- 聊天包发送超时默认 1500ms（钳制 100–10000ms），不再「一次即永久停用」：写阻塞/IO 异常会立即废弃当前连接并重连。发送结果当前只分「成功 / 连接不可恢复」两类；旧版对「可继续的失败」按连续次数降级的路径已删除（宿主回执位只用于流对齐，不再参与降级判断）。只有重连握手连续失败达到上限、进入终态时才停止转发。
- debug 数据通道的写帧超时默认 1000ms（钳制 100–30000ms）。写超时或写异常只停用 debug 通道并关闭其管道，聊天通道完全不受影响；本次游戏进程内不会自动恢复。
- 控制管道和 debug 通道不参与聊天主通道的重连，见下一节的边界说明。

### 调试全包模式（API 开关）

默认 `chat`：DLL 只转发聊天包，聊天处理与未启用时完全一致。排查未知包时可在运行时切到 `all`：DLL 通过**独立的 debug 管道**额外转发所有能抓到的包，聊天管道的队列、ACK 与线程都不受影响。模式只存在进程内存里，**不持久化**，重启后回到 `chat`。

```sh
# 查看当前模式与全包计数（默认 chat）
curl -H "x-miliastra-token: <令牌>" http://127.0.0.1:18889/debug/packets

# 切到全包模式
curl -X POST -H "x-miliastra-token: <令牌>" -H "Content-Type: application/json" \
     -d '{"mode":"all"}' http://127.0.0.1:18889/debug/packets

# 切回聊天模式
curl -X POST -H "x-miliastra-token: <令牌>" -H "Content-Type: application/json" \
     -d '{"mode":"chat"}' http://127.0.0.1:18889/debug/packets
```

切换响应里的 `applied` 表示模式是否真的下发给 DLL：`applied: true` 说明控制管道已连接（`dllControl: true`）且 DLL 已切换；`applied: false` 表示 DLL 未连接或仍是**旧 DLL**（没有控制管道），此时只在宿主侧记录模式，`dllControl` 如实反映控制通道是否可用。要真正拿到全包，需要配套的新 DLL。

**已知边界：控制管道与 debug 数据通道不参与重连。** DLL 只在初始化时连接这两条可选管道一次。宿主重启后，即使聊天主通道通过 reattach 恢复，DLL 也不会重新连接它们：`GET /debug/packets` 的 `dllControl` 会是 `false`，`applied` 只能是 `false`；全包模式要恢复必须重启游戏重新注入 DLL（聊天主通道不受影响）。宿主进程重启后 `mode` 也会回到默认 `chat`，接口仍可记录模式但无法下发。

raw 抓包（可选，**只有全包模式才有数据**）：

```sh
miliastra-wonderland-chatbridge --port 18889 --token <令牌> --self-uid <UID> \
  --debug-packets --capture-dir captures --capture-max-mib 1024
```

- `--debug-packets`：启动即为 `all`；不写则从 `chat` 开始，之后用 API 切换。
- `--capture-dir <目录>`：raw 包落盘目录；不指定就不落盘。
- `--capture-max-mib <n>`：落盘 payload 总字节上限，默认 `1024`（1 GiB），不含 index/文件系统开销；超过后停止抓包，只丢抓包，不影响聊天与转发。

落盘文件名形如 `p_00042_02246_137.bin`（序号、cmd、payload 长度），`index.csv` 逐行 `seq,cmd,len,unixMillis`；文件内容是与游戏嗅探一致的原始 protobuf payload，不含管道帧头。每次启动只重建 `index.csv`，不会清理目录里上一次留下的 `p_*.bin`；复用目录前请先清空，避免新旧文件混在一起。

注意：全包模式会把**所有**能抓到的包（包含非聊天、可能含账号/环境信息）写入磁盘与 debug 管道，只建议在内网、可信环境短期开启；`captures/` 目录不要外传，用完及时删除。debug 走独立管道，即使它断开或写盘失败，聊天通道也照常工作；但全包模式会让游戏的网络解包线程对每个包多一次解压/复制，属于重诊断模式，长期开启有性能代价，用完请切回 `chat`。

## 许可与致谢

本项目采用 **GNU GPL v3.0（GPL-3.0-only）**，完整条款见 [LICENSE](LICENSE)。第三方组件保留各自的版权与许可。

原生组件基于 [YaeAchievement](https://github.com/HolographicHat/Yae) 修改。原作者版权声明：Copyright (C) 2022–2024 HoloHat。修改包括消息接入、生命周期、资源管理与健壮性处理；上游原有的 GPL v3 或更高版本授权声明予以保留。

包含 [MinHook](https://github.com/TsudaKageyu/minhook)（作者 Tsuda Kageyu），采用 BSD-2-Clause，条款见 [THIRD_PARTY_LICENSES](THIRD_PARTY_LICENSES)。
