using System.IO.Pipes;
using MiliastraWonderlandChatBridge.Utilities;

namespace MiliastraWonderlandChatBridge;

/// <summary>
/// debug 全包模式的**数据通道**（可选增强）。
///
/// 宿主是命名管道 server，DLL 是 client：管道名 <see cref="PipeName"/>，单向
/// DLL→宿主、无 ACK。每帧格式与聊天管道的 0x04 完全一致：
///   0x04 + cmdId(u16 LE) + 长度(u32 LE) + 数据
/// 只在 <see cref="ModeControl.AllPackets"/> 时由钩子入队；发送线程是唯一 reader。
///
/// 与聊天通道彻底隔离：独立管道、独立队列、独立后台线程。连接或写入失败只会
/// Disable 本通道（<see cref="PacketOutbox"/> 完全不受影响），钩子线程也绝不等待 IO。
/// </summary>
internal static class DebugOutbox {

    /// <summary>debug 数据管道名，必须与宿主 src/packet_control.rs 的 DEBUG_PIPE_NAME 一致。</summary>
    internal const string PipeName = "MiliastraWonderlandChatBridgeDebug";

    /// <summary>队列容量。全包模式吞吐远高于聊天，1024 用来吸收突发。</summary>
    private const int Capacity = 1024;

    /// <summary>排队数据的总字节上限（32 MiB）。超过就丢弃新包，避免把游戏内存吃满。</summary>
    private const long MaxQueuedBytes = 32L * 1024 * 1024;

    /// <summary>连接超时。管道由宿主在注入前创建，正常在毫秒级就能连上。</summary>
    private const int ConnectTimeoutMs = 5000;

    /// <summary>每丢弃这么多次才记一行 Warn，避免刷屏。</summary>
    private const int DropLogInterval = 1024;

    private static readonly System.Threading.Channels.Channel<PendingPacket> Queue =
        System.Threading.Channels.Channel.CreateBounded<PendingPacket>(
            new System.Threading.Channels.BoundedChannelOptions(Capacity) {
                FullMode = System.Threading.Channels.BoundedChannelFullMode.DropWrite,
                SingleReader = true,
                SingleWriter = false,
            });

    private static int _started;
    private static int _disabled;
    private static long _dropped;
    private static long _queuedBytes;
    private static long _queuedItems;
    private static NamedPipeClientStream? _pipeStream;
    private static BinaryWriter? _writer;

    /// <summary>debug 通道是否已停用（连接或写入失败后置位）。</summary>
    internal static bool Disabled => Volatile.Read(ref _disabled) != 0;

    /// <summary>累计丢弃的包数（队列满或超过字节上限）。</summary>
    internal static long Dropped => Interlocked.Read(ref _dropped);

    /// <summary>启动后台发送线程（连接也在该线程里做）。幂等，且立即返回。</summary>
    internal static void Start() {
        if (Interlocked.Exchange(ref _started, 1) != 0) {
            return;
        }
        var thread = new Thread(SenderLoop) {
            IsBackground = true,
            Name = "mwc-debug-sender",
        };
        thread.Start();
    }

    /// <summary>钩子调用：只做"计数 + 入队"，立即返回，绝不做 IO。</summary>
    internal static void Enqueue(ushort cmd, byte[] data) {
        if (Disabled) {
            return;
        }
        var size = data.Length;
        // DropWrite 模式下 channel 满时 TryWrite 仍返回 true（拿不到"是否真的入队"），
        // 所以容量先用我们自己的计数挡：超限直接计数丢弃，也不会把字节额度算漏。
        if (Interlocked.Increment(ref _queuedItems) > Capacity) {
            Interlocked.Decrement(ref _queuedItems);
            Drop();
            return;
        }
        if (Interlocked.Add(ref _queuedBytes, size) > MaxQueuedBytes) {
            Interlocked.Add(ref _queuedBytes, -size);
            Interlocked.Decrement(ref _queuedItems);
            Drop();
            return;
        }
        if (!Queue.Writer.TryWrite(new PendingPacket { Cmd = cmd, Data = data })) {
            // 只会发生在通道被完成之后（Disable）；回滚额度再计数。
            Interlocked.Add(ref _queuedBytes, -size);
            Interlocked.Decrement(ref _queuedItems);
            Drop();
        }
    }

    /// <summary>只停 debug 通道，绝不触碰聊天 PacketOutbox。</summary>
    internal static void Disable(string reason) {
        if (Interlocked.Exchange(ref _disabled, 1) != 0) {
            return;
        }
        Log.Warn($"停止转发 debug 包: {reason}（已丢弃 {Dropped} 份副本）");
        Queue.Writer.TryComplete();
        try {
            _writer?.Dispose();
        } catch {
            // 关闭过程中的异常无需处理。
        }
        try {
            _pipeStream?.Dispose();
        } catch {
            // 关闭过程中的异常无需处理。
        }
    }

    /// <summary>帧编码。拆成纯函数，回归测试可以只验证字节序列。</summary>
    internal static void WriteFrame(BinaryWriter writer, ushort cmd, byte[] data) {
        writer.Write((byte) 4);
        writer.Write(cmd);
        writer.Write(data.Length);
        writer.Write(data);
        writer.Flush();
    }

    private static void Drop() {
        var dropped = Interlocked.Increment(ref _dropped);
        if (dropped % DropLogInterval == 0) {
            Log.Warn($"debug 队列已满，累计丢弃 {dropped} 个包");
        }
    }

    private static void SenderLoop() {
        try {
            // 懒连接只发生在后台线程：旧宿主没有这条管道时，5s 后超时并永久停用 debug。
            // 先保存到字段，连接失败时交给 Disable 统一关闭，避免句柄泄漏。
            var pipe = new NamedPipeClientStream(".", PipeName, PipeDirection.Out);
            _pipeStream = pipe;
            pipe.Connect(ConnectTimeoutMs);
            _writer = new BinaryWriter(pipe);
        } catch (Exception ex) {
            // 后台线程的未捕获异常会触发进程级异常处理（游戏退出），必须在这里兜住。
            Disable($"debug 管道连接失败: {ex.Message}");
            return;
        }
        Log.Trace("Debug pipe connected.");
        while (true) {
            PendingPacket packet;
            try {
                packet = Queue.Reader.ReadAsync().AsTask().GetAwaiter().GetResult();
            } catch {
                return;
            }
            var size = packet.Data.Length;
            try {
                if (Disabled) {
                    continue;
                }
                try {
                    WriteFrame(_writer!, packet.Cmd, packet.Data);
                } catch (Exception ex) {
                    // 写失败只停 debug，聊天通道的 PacketOutbox 由未受影响的钩子继续工作。
                    Disable($"debug 管道写入失败: {ex.Message}");
                    return;
                }
            } finally {
                Interlocked.Add(ref _queuedBytes, -size);
                Interlocked.Decrement(ref _queuedItems);
            }
        }
    }
}
