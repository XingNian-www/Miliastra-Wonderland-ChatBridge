using System.IO.Pipes;
using MiliastraWonderlandChatBridge.Utilities;

namespace MiliastraWonderlandChatBridge;

/// <summary>
/// 游戏内函数的地址表。由宿主通过 0xFD 帧下发 RVA，基址是游戏主模块。
///
/// 只有 <see cref="ToInt32"/> 是聊天嗅探必须的；其余字段保留是为了
/// 与宿主协议完全兼容（宿主固定下发 10 个值，顺序不能变）。
/// </summary>
internal static unsafe class GameMethod {

    public static delegate*unmanaged<nint, int, double, double, int, void> UpdateNormalProp { get; set; }

    public static delegate*unmanaged<nint, nint> NewString { get; set; }

    public static delegate*unmanaged<nint, nint> FindGameObject { get; set; }

    public static delegate*unmanaged<nint, void> EventSystemUpdate { get; set; }

    public static delegate*unmanaged<nint, nint, bool> SimulatePointerClick { get; set; }

    /// <summary>网络解包入口，钩住它就能拿到所有服务器包。</summary>
    public static delegate*unmanaged<byte*, int, int> ToInt32 { get; set; }

    public static void** TcpStatePtr { get; set; }

    public static void** SharedInfoPtr { get; set; }

    public static delegate*unmanaged<void*, void*, void*, uint, void*, uint, bool> Decompress { get; set; }

}

/// <summary>
/// 待发送的聊天包。钩子只负责把数据复制到这里，绝不等待管道。
/// </summary>
internal struct PendingPacket {
    public ushort Cmd;
    public byte[] Data;
}

/// <summary>
/// 发送队列与后台发送线程。
///
/// 设计要点：钩子运行在游戏的网络解包线程里，任何阻塞都会直接拖慢游戏
/// （好友列表加载、世界切换都会受影响）。所以钩子只做"复制 + 入队"，
/// 队列满了就丢弃这一份副本——丢几条聊天远比卡住游戏可以接受。
/// </summary>
internal static class PacketOutbox {

    /// 队列容量。按聊天包频率估算，256 足够吸收突发。
    private const int Capacity = 256;

    /// 单次发送（写管道 + 读回执）的总超时。
    private const int SendTimeoutMs = 500;

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

    /// 是否已停止转发（管道故障或超时后置位，游戏保持直通）。
    public static bool Disabled => Volatile.Read(ref _disabled) != 0;

    public static long Dropped => Interlocked.Read(ref _dropped);

    /// 启动后台发送线程。重复调用无副作用。
    public static void Start() {
        if (Interlocked.Exchange(ref _started, 1) != 0) {
            return;
        }
        var thread = new Thread(SenderLoop) {
            IsBackground = true,
            Name = "mwc-packet-sender",
        };
        thread.Start();
    }

    /// 钩子调用：复制数据并入队，立即返回。
    public static void Enqueue(ushort cmd, byte[] data) {
        if (Disabled) {
            return;
        }
        if (!Queue.Writer.TryWrite(new PendingPacket { Cmd = cmd, Data = data })) {
            // 队列已满：丢弃这份副本，绝不让游戏线程等待。
            Interlocked.Increment(ref _dropped);
        }
    }

    private static void SenderLoop() {
        while (true) {
            PendingPacket packet;
            try {
                packet = Queue.Reader.ReadAsync().AsTask().GetAwaiter().GetResult();
            } catch {
                return;
            }
            if (Disabled) {
                continue;
            }
            if (!TrySend(packet)) {
                Disable("单次发送超过 500ms 或被对端拒绝");
                return;
            }
        }
    }

    private static bool TrySend(PendingPacket packet) {
        // 写与读回执必须在同一把锁里，且整体受超时约束；
        // 超时后由 Disable 关闭管道，迟到的回执不会污染后续协议。
        var task = Task.Run(() => {
            return Goshujin.WritePacketAndAwaitAck(packet.Cmd, packet.Data);
        });
        try {
            if (!task.Wait(SendTimeoutMs)) {
                return false;
            }
            return !task.IsFaulted && task.Result;
        } catch {
            return false;
        }
    }

    /// 停止转发：关闭管道，之后钩子只透传原始返回值。
    public static void Disable(string reason) {
        if (Interlocked.Exchange(ref _disabled, 1) != 0) {
            return;
        }
        Log.Warn($"停止转发聊天包: {reason}（已丢弃 {Dropped} 份副本）");
        Queue.Writer.TryComplete();
        // 关闭管道让宿主感知；阻塞中的读/写会因此抛异常退出。
        Goshujin.Shutdown();
    }
}
/// <summary>
/// 与宿主进程的命名管道通信。
///
/// 协议（单字节类型 + 各类型载荷，全部小端）：
///   0x04 PushPacketData : cmdId(u16) + 长度(u32) + 数据，宿主回一个 bool
///   0xFA LoadTasks      : 宿主回 cmdId 列表 + u32::MAX 结束
///   0xFB LoadProps      : 宿主回 prop 列表 + u32::MAX 结束（本项目不使用）
///   0xFD LoadMethodTable: 宿主回 10 个 u32 RVA
///   0xFE ResumeMainThread: 恢复游戏主线程（无回执）
///
/// 与上游相比删掉了玩家属性推送，并**禁用了自动退出**——
/// 上游在订阅清空时会 Environment.Exit(0) 直接结束游戏进程，
/// 对常驻嗅探来说太危险。
/// </summary>
internal static class Goshujin {

    private static NamedPipeClientStream _pipeStream = null!;
    private static BinaryReader _pipeReader = null!;
    private static BinaryWriter _pipeWriter = null!;
    private static Lock _lock = null!;

    public static void Init(string pipeName = "MiliastraWonderlandChatBridgePipe") {
        _lock = new Lock();
        _pipeStream = new NamedPipeClientStream(pipeName);
        _pipeReader = new BinaryReader(_pipeStream);
        _pipeWriter = new BinaryWriter(_pipeStream);
        _pipeStream.Connect();
        Log.Trace("Pipe server connected.");
    }

    /// <summary>
    /// 后台发送线程调用：写一个包并读掉回执。
    ///
    /// 这个函数**只**允许在发送线程里调用。钩子线程绝不能进来——
    /// 它会在管道上阻塞，直到宿主读到数据并回一个字节。
    /// </summary>
    public static bool WritePacketAndAwaitAck(ushort cmdId, byte[] data) {
        using (_lock.EnterScope()) {
            _pipeWriter.Write((byte) 4);
            _pipeWriter.Write(cmdId);
            _pipeWriter.Write(data.Length);
            _pipeWriter.Write(data);
            _pipeWriter.Flush();
            // 回执只用于"是否继续订阅"；本项目宿主恒回 false，读掉即可保持流对齐。
            _ = _pipeReader.ReadBoolean();
            return true;
        }
    }

    /// <summary>关闭管道。失败转发的收尾动作，让宿主和阻塞中的 IO 都能退出。</summary>
    public static void Shutdown() {
        try {
            _pipeStream?.Dispose();
        } catch {
            // 关闭过程中的异常无需处理。
        }
    }
    public static void LoadTasks() {
        _pipeWriter.Write((byte) 0xFA);
        uint cmdId;
        while ((cmdId = _pipeReader.ReadUInt32()) != uint.MaxValue) {
            Application.RequiredPackets.Add(cmdId);
        }
    }

    /// <summary>
    /// 读取宿主订阅的玩家属性。本项目不使用，但要按协议把列表消费干净，
    /// 否则后续帧会错位。
    /// </summary>
    public static void LoadProps() {
        _pipeWriter.Write((byte) 0xFB);
        uint propType;
        while ((propType = _pipeReader.ReadUInt32()) != uint.MaxValue) {
            // 忽略：聊天嗅探不需要玩家属性。
        }
    }

    public static unsafe void LoadMethodTable() {
        _pipeWriter.Write((byte) 0xFD);
        _ = _pipeReader.ReadUInt32(); // DoCmd，本项目不使用
        GameMethod.UpdateNormalProp = (delegate*unmanaged<nint, int, double, double, int, void>) Native.RVAToVA(_pipeReader.ReadUInt32());
        GameMethod.NewString = (delegate*unmanaged<nint, nint>) Native.RVAToVA(_pipeReader.ReadUInt32());
        GameMethod.FindGameObject = (delegate*unmanaged<nint, nint>) Native.RVAToVA(_pipeReader.ReadUInt32());
        GameMethod.EventSystemUpdate = (delegate*unmanaged<nint, void>) Native.RVAToVA(_pipeReader.ReadUInt32());
        GameMethod.SimulatePointerClick = (delegate*unmanaged<nint, nint, bool>) Native.RVAToVA(_pipeReader.ReadUInt32());
        GameMethod.ToInt32 = (delegate*unmanaged<byte*, int, int>) Native.RVAToVA(_pipeReader.ReadUInt32());
        GameMethod.TcpStatePtr = (void**) Native.RVAToVA(_pipeReader.ReadUInt32());
        GameMethod.SharedInfoPtr = (void**) Native.RVAToVA(_pipeReader.ReadUInt32());
        GameMethod.Decompress = (delegate*unmanaged<void*, void*, void*, uint, void*, uint, bool>) Native.RVAToVA(_pipeReader.ReadUInt32());
    }

    public static void ResumeMainThread() {
        _pipeWriter.Write((byte) 0xFE);
    }
}
