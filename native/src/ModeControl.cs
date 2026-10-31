using System.IO.Pipes;
using MiliastraWonderlandChatBridge.Utilities;

namespace MiliastraWonderlandChatBridge;

/// <summary>
/// debug 全包模式的**控制通道**（可选增强）。
///
/// 宿主是命名管道 server，DLL 是 client：管道名 <see cref="PipeName"/>，方向
/// 宿主→DLL。每条消息只有 1 字节模式：0=chat（默认，只转发 RequiredPackets），
/// 1=all（除聊天包外，所有合法包再额外进 debug 数据管道）。宿主在 DLL 连上后
/// 立即下发当前模式，之后每次 API 切换再写 1 字节。
///
/// 连接与读取全部放在后台线程：连接失败只回落 chat，绝不阻塞、也绝不影响 Awake。
///
/// 重连（复核 M3）：宿主重启后控制管道会断开；旧实现直接 return，导致控制通道
/// 永久失联，而宿主 /debug/packets 仍报 dllControl=true、mode=all（静默不一致）。
/// 现在断开/连接失败都按 1s/2s/4s... 封顶 30s 退避重连（独立后台线程、不阻塞
/// 游戏线程）；宿主重建同名管道并下发当前模式后自动恢复一致。断线期间强制回落
/// chat，避免停在 all 模式却没人再控制。
/// </summary>
internal static class ModeControl {

    /// <summary>控制管道名，必须与宿主 src/packet_control.rs 的 CONTROL_PIPE_NAME 一致。</summary>
    internal const string PipeName = "MiliastraWonderlandChatBridgeCtl";

    /// <summary>连接超时。管道由宿主在注入前创建，正常在毫秒级就能连上。</summary>
    private const int ConnectTimeoutMs = 5000;

    internal const int DefaultReconnectInitialDelayMs = 1000;
    internal const int DefaultReconnectMaxDelayMs = 30000;
    internal const int MinReconnectDelayMs = 10;
    internal const int MaxReconnectDelayMs = 300000;

    private static int _started;
    private static int _allPackets;
    private static int _reconnectInitialDelayMs = DefaultReconnectInitialDelayMs;
    private static int _reconnectMaxDelayMs = DefaultReconnectMaxDelayMs;
    private static long _connectCount;
    private static long _reconnectAttempts;

    /// <summary>当前是否转发全部合法包。钩子每条包都读它，用 volatile 避免锁竞争。</summary>
    internal static bool AllPackets => Volatile.Read(ref _allPackets) != 0;

    /// <summary>累计成功建立控制管道的次数（观测/回归用）。</summary>
    internal static long ConnectCount => Interlocked.Read(ref _connectCount);

    /// <summary>累计控制管道（重）连接尝试次数（观测/回归用）。</summary>
    internal static long ReconnectAttempts => Interlocked.Read(ref _reconnectAttempts);

    /// <summary>重连首次退避（毫秒），默认 1000；测试可调小。</summary>
    internal static int ReconnectInitialDelayMs {
        get => Volatile.Read(ref _reconnectInitialDelayMs);
        set => Volatile.Write(ref _reconnectInitialDelayMs, Math.Clamp(value, MinReconnectDelayMs, MaxReconnectDelayMs));
    }

    /// <summary>重连退避上限（毫秒），默认 30000；宿主长时间不在时按该间隔稳定重试。</summary>
    internal static int ReconnectMaxDelayMs {
        get => Volatile.Read(ref _reconnectMaxDelayMs);
        set => Volatile.Write(ref _reconnectMaxDelayMs, Math.Clamp(value, MinReconnectDelayMs, MaxReconnectDelayMs));
    }

    /// <summary>
    /// 回归测试注入点：返回一条已"连接"的流，避免测试触碰真实命名管道。
    /// 生产环境保持 null，走真实的 <see cref="NamedPipeClientStream"/>。
    /// </summary>
    internal static Func<Stream>? ConnectStreamForTest { get; set; }

    /// <summary>启动后台控制线程。幂等，且立即返回，绝不阻塞初始化。</summary>
    internal static void Start() {
        if (Interlocked.Exchange(ref _started, 1) != 0) {
            return;
        }
        var thread = new Thread(ControlLoop) {
            IsBackground = true,
            Name = "mwc-mode-control",
        };
        thread.Start();
    }

    /// <summary>回归测试用：不触碰真实管道，直接切换模式标志。</summary>
    internal static void SetAllPacketsForTest(bool allPackets) =>
        Volatile.Write(ref _allPackets, allPackets ? 1 : 0);

    private static void ControlLoop() {
        var delay = ReconnectInitialDelayMs;
        while (true) {
            Interlocked.Increment(ref _reconnectAttempts);
            Stream? stream = null;
            try {
                stream = ConnectStream();
                Interlocked.Increment(ref _connectCount);
                // 连接成功后重置退避：下次断线从最小间隔重新开始。
                delay = ReconnectInitialDelayMs;
                Log.Trace("Mode control pipe connected.");
                while (true) {
                    // 阻塞读：宿主每次切换模式写 1 字节，流结束（-1）表示宿主已关闭。
                    var mode = stream.ReadByte();
                    if (mode < 0) {
                        Log.Warn("控制管道已断开，回落 chat 模式并等待重连");
                        break;
                    }
                    Volatile.Write(ref _allPackets, mode == 1 ? 1 : 0);
                }
            } catch (Exception ex) {
                // 旧宿主没有控制管道，或宿主中途退出：只影响增强功能，绝不影响聊天通道。
                Log.Warn($"控制管道不可用，回落 chat 模式并等待重连: {ex.Message}");
            } finally {
                // 断开/异常后一律回落 chat，避免停在 all 模式却没人再控制。
                Volatile.Write(ref _allPackets, 0);
                try {
                    stream?.Dispose();
                } catch {
                    // 关闭过程中的异常无需处理。
                }
            }
            // 宿主重启后会重建同名控制管道；后台线程按退避重试，不阻塞游戏。
            Thread.Sleep(delay);
            delay = Math.Min(delay * 2, ReconnectMaxDelayMs);
        }
    }

    private static Stream ConnectStream() {
        var factory = ConnectStreamForTest;
        if (factory != null) {
            return factory();
        }
        var pipe = new NamedPipeClientStream(".", PipeName, PipeDirection.In);
        try {
            pipe.Connect(ConnectTimeoutMs);
            return pipe;
        } catch {
            // Connect 抛异常时句柄仍在 pipe 对象里，必须显式 dispose。
            try {
                pipe.Dispose();
            } catch {
                // 关闭过程中的异常无需处理。
            }
            throw;
        }
    }
}
