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
/// 连接与读取全部放在后台线程：旧宿主不会创建这条管道，Connect 5s 后超时，
/// 这里只记一行日志并回落 chat——绝不阻塞、也绝不影响 Awake 初始化。
/// </summary>
internal static class ModeControl {

    /// <summary>控制管道名，必须与宿主 src/packet_control.rs 的 CONTROL_PIPE_NAME 一致。</summary>
    internal const string PipeName = "MiliastraWonderlandChatBridgeCtl";

    /// <summary>连接超时。管道由宿主在注入前创建，正常在毫秒级就能连上。</summary>
    private const int ConnectTimeoutMs = 5000;

    private static int _started;
    private static int _allPackets;

    /// <summary>当前是否转发全部合法包。钩子每条包都读它，用 volatile 避免锁竞争。</summary>
    internal static bool AllPackets => Volatile.Read(ref _allPackets) != 0;

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
        NamedPipeClientStream? pipe = null;
        try {
            pipe = new NamedPipeClientStream(".", PipeName, PipeDirection.In);
            pipe.Connect(ConnectTimeoutMs);
            Log.Trace("Mode control pipe connected.");
            while (true) {
                // 阻塞读：宿主每次切换模式写 1 字节，流结束（-1）表示宿主已关闭。
                var mode = pipe.ReadByte();
                if (mode < 0) {
                    Log.Warn("控制管道已断开，回落 chat 模式");
                    return;
                }
                Volatile.Write(ref _allPackets, mode == 1 ? 1 : 0);
            }
        } catch (Exception ex) {
            // 旧宿主没有控制管道，或宿主中途退出：只影响增强功能，绝不影响聊天通道。
            Log.Warn($"控制管道不可用，保持 chat 模式: {ex.Message}");
        } finally {
            // 读结束/异常后一律回落 chat，避免停在 all 模式却没人再控制。
            Volatile.Write(ref _allPackets, 0);
            try {
                pipe?.Dispose();
            } catch {
                // 关闭过程中的异常无需处理。
            }
        }
    }
}
