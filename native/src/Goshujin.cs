using System.IO.Pipes;
using System.Text;
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
///
/// 发送失败策略（长期运行稳定性，task-17）：只有两类结果——成功，或"连接不可恢复"。
///   - TimedOutStuck：Wait 超时后发送任务仍卡在写/读里（持有旧连接锁）；
///   - Faulted：写/读直接抛 I/O 异常（管道大概率已断）。
/// 两者都会**立即**废弃当前连接、dispose 旧管道并触发重连；绝不能"丢一包继续"，
/// 否则后续发送都会排在那把锁上、线程池里累积卡死任务，半写的帧也无法撤回。
/// 只有重连进入终态（Goshujin.Disabled）才永久停止转发。
///
/// 注：历史版本里还有一条"宿主回执明确表示不再继续订阅、连续 N 次才降级"的路径；
/// 但本项目的宿主对每个包恒回 1 字节 false，且写入侧一直忽略该值（协议里它只表示
/// "是否继续订阅"，订阅变更走控制管道），该分支从未被走到，已删除以免误导维护者。
/// </summary>
internal static class PacketOutbox {

    /// <summary>队列容量。按聊天包频率估算，256 足够吸收突发。</summary>
    private const int Capacity = 256;

    /// <summary>单次发送（写管道 + 读回执）的默认总超时（毫秒）。</summary>
    internal const int DefaultSendTimeoutMs = 1500;

    /// <summary>SendTimeoutMs 的下限；0/负数/过小值都会被钳到这里。</summary>
    internal const int MinSendTimeoutMs = 100;

    /// <summary>SendTimeoutMs 的上限，避免误配置成接近永久等待。</summary>
    internal const int MaxSendTimeoutMs = 10000;

    private static readonly System.Threading.Channels.Channel<PendingPacket> Queue =
        System.Threading.Channels.Channel.CreateBounded<PendingPacket>(
            new System.Threading.Channels.BoundedChannelOptions(Capacity) {
                FullMode = System.Threading.Channels.BoundedChannelFullMode.DropWrite,
                SingleReader = true,
                SingleWriter = false,
            },
            // DropWrite 在队列满时"丢弃但返回 true"，靠 TryWrite 返回值统计会漏数
            // （审计 F4 已确认）。.NET 7+ 的 itemDropped 回调是官方给出的准确计数入口，
            // 回调在写入方线程同步执行，这里只做一个 Interlocked.Increment。
            _ => Interlocked.Increment(ref _dropped));

    private static int _started;
    private static int _disabled;
    private static long _dropped;
    private static int _sendTimeoutMs = DefaultSendTimeoutMs;
    private static long _timeoutCount;
    private static int _abandonedTasks;

    /// <summary>是否已停止转发（终态：重连彻底失败或显式 Disable 后置位，游戏保持直通）。</summary>
    public static bool Disabled => Volatile.Read(ref _disabled) != 0;

    /// <summary>累计丢弃的包数（队列满，或 Disable 后仍有入队竞态）。</summary>
    public static long Dropped => Interlocked.Read(ref _dropped);

    /// <summary>累计超时次数（观测用；成功不清零，便于判断宿主整体健康度）。</summary>
    internal static long TimeoutCount => Interlocked.Read(ref _timeoutCount);

    /// <summary>
    /// 已被放弃、但仍卡在旧连接同步 IO 里的发送任务数（观测/回归用）。
    /// 判定连接卡死时会把旧流 dispose 让它们尽快退出；正常情况下应回落为 0，
    /// 长期 &gt; 0 且持续增长说明 .NET 同步管道 IO 未能被取消（见 Goshujin 类型注释）。
    /// </summary>
    internal static int AbandonedSendTasks => Volatile.Read(ref _abandonedTasks);

    /// <summary>
    /// 单次发送（写管道 + 读回执）的总超时，毫秒。0/负数会被钳到
    /// <see cref="MinSendTimeoutMs"/>，过大值钳到 <see cref="MaxSendTimeoutMs"/>；
    /// 默认 1500ms（旧的固定 500ms 太容易把一次宿主卡顿放大成整局失效）。
    /// </summary>
    internal static int SendTimeoutMs {
        get => Volatile.Read(ref _sendTimeoutMs);
        set => Volatile.Write(ref _sendTimeoutMs, Math.Clamp(value, MinSendTimeoutMs, MaxSendTimeoutMs));
    }

    /// <summary>启动后台发送线程。重复调用无副作用。</summary>
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

    /// <summary>
    /// 钩子调用：在容量检查通过后复制载荷并入队，立即返回。
    ///
    /// 拷贝放在容量预检之后：队列满/终态时游戏网络线程不做无意义的 ToArray。
    /// Dropped 计数也在这里补上——历史实现由调用方用 CanEnqueue 预检查后直接跳过
    /// Enqueue，导致满队列时的丢弃从未被计数（复核意见）。
    /// </summary>
    public static void Enqueue(ushort cmd, ReadOnlySpan<byte> data) {
        if (Disabled) {
            return;
        }
        // DropWrite 在满队列时"丢弃但返回 true"，所以用 Reader.Count 先挡容量；
        // 并发竞态下 TryWrite 仍可能静默丢弃，那种情况由 itemDropped 回调计数。
        if (Queue.Reader.Count >= Capacity) {
            Interlocked.Increment(ref _dropped);
            return;
        }
        var copy = data.ToArray();
        if (!Queue.Writer.TryWrite(new PendingPacket { Cmd = cmd, Data = copy })) {
            // 只会在通道被完成后发生（Disable 与入队之间的竞态）。
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
            var result = TrySend(packet);
            if (result == SendResult.Success) {
                continue;
            }
            // 关键：不能"丢包继续"。TimedOutStuck 表示 Task 仍卡在写/读里并持有旧连接的
            // 锁（后续发送必然排队超时、线程池里累积卡死任务）；Faulted 表示写/读已经
            // 抛异常（管道大概率已断）。两者都必须立即废弃当前连接并重连。
            if (result == SendResult.TimedOutStuck) {
                Interlocked.Increment(ref _timeoutCount);
                Log.Warn($"发送超时后 IO 仍未退出（写阻塞/回执阻塞），判定连接不可恢复，" +
                         $"关闭旧管道并重连（累计超时 {TimeoutCount} 次，" +
                         $"被放弃的卡死任务 {AbandonedSendTasks}）");
            } else {
                Log.Warn($"聊天包发送 I/O 异常，判定连接不可恢复，关闭旧管道并重连" +
                         $"（累计超时 {TimeoutCount} 次）");
            }
            if (!ReconnectOrDisable($"连接不可恢复（累计超时 {TimeoutCount} 次）")) {
                return;
            }
        }
    }

    /// <summary>请求重连并等待结果；返回 false 表示已进入终态，发送线程应退出。</summary>
    private static bool ReconnectOrDisable(string reason) {
        if (!Goshujin.RequestReconnect(reason)) {
            Disable(reason);
            return false;
        }
        var state = Goshujin.WaitForConnectedOrDisabled();
        if (state != Goshujin.ConnectionState.Connected) {
            Disable(reason);
            return false;
        }
        Log.Info($"聊天管道已重连，恢复转发（累计重连尝试 {Goshujin.ReconnectAttempts} 次）");
        return true;
    }

    private enum SendResult {
        /// <summary>写完并读掉回执。</summary>
        Success,

        /// <summary>Wait 超时后 Task 仍未完成：还卡在写/读里，连接不可恢复。</summary>
        TimedOutStuck,

        /// <summary>Task 已完成但抛了 I/O 异常：管道已断，连接不可恢复。</summary>
        Faulted,
    }

    /// <summary>
    /// 发送一包并等待回执。写与回执使用**捕获到的同一代连接**（锁/reader/writer 一起
    /// 快照），且整体受 <see cref="SendTimeoutMs"/> 约束。
    ///
    /// 结果只有三类（历史版本里"宿主明确不再订阅、连续 N 次才降级"的分支因为宿主对
    /// 每个包恒回 false、DLL 侧从未使用该值而删除；保留它只会成为无法被测试覆盖的死代码）：
    /// - Success：Task 已完成且成功（包括恰好在超时窗口末完成的成功）。
    /// - TimedOutStuck：Wait 超时后 Task **仍未完成**。它卡在 Write 或 ReadBoolean 里
    ///   并一直持有该连接的锁；继续用这条连接只会让后续发送排队超时、线程池里累积
    ///   卡死任务，而且写了一半的帧不可能从字节流里撤回。必须废弃连接并重连。
    /// - Faulted：Task 已完成但抛 I/O 异常（写失败/读回执失败），管道已断，同样必须重连。
    /// 注意：废弃连接时被放弃的 Task 是最多 1 个/连接（发送线程在重连期间不再发送），
    /// 用 <see cref="AbandonedSendTasks"/> 观测它是否最终退出。
    /// </summary>
    private static SendResult TrySend(PendingPacket packet) {
        var task = Task.Run(() => Goshujin.WritePacketAndAwaitAck(packet.Cmd, packet.Data));
        try {
            if (task.Wait(SendTimeoutMs)) {
                return SendResult.Success;
            }
            // Wait 返回 false：若 Task 恰好在超时窗口末完成且没有异常，仍算成功；
            // 否则说明它还卡在 IO 里，连接已经不可用。
            if (!task.IsCompleted) {
                TrackAbandoned(task);
                return SendResult.TimedOutStuck;
            }
            if (task.IsFaulted) {
                TrackAbandoned(task);
                return SendResult.Faulted;
            }
            return SendResult.Success;
        } catch (AggregateException) {
            // Wait 在超时前就观察到任务失败（写/读异常）。
            TrackAbandoned(task);
            return SendResult.Faulted;
        } catch {
            TrackAbandoned(task);
            return SendResult.Faulted;
        }
    }

    /// <summary>记录一个被放弃的发送任务；它退出后计数回落（观测用）。</summary>
    private static void TrackAbandoned(Task task) {
        Interlocked.Increment(ref _abandonedTasks);
        _ = task.ContinueWith(_ => Interlocked.Decrement(ref _abandonedTasks), TaskScheduler.Default);
    }

    /// <summary>永久停止转发：关闭管道，之后钩子只透传原始返回值。</summary>
    public static void Disable(string reason) {
        if (Interlocked.Exchange(ref _disabled, 1) != 0) {
            return;
        }
        Log.Warn($"停止转发聊天包: {reason}（已丢弃 {Dropped} 份副本，累计超时 {TimeoutCount} 次）");
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
/// 主管道之外还有两条**可选增强**管道（宿主是 server、DLL 是 client）：
///   MiliastraWonderlandChatBridgeCtl  : 控制管道，宿主→DLL。每条消息 1 字节模式，
///                                       0=chat（默认，只转发 RequiredPackets），
///                                       1=all（转发所有合法包）。
///   MiliastraWonderlandChatBridgeDebug: debug 数据管道，DLL→宿主，单向、无 ACK。
///
/// 重连（长期运行自愈，task-17）：宿主重启/闪退后，DLL 不再"管道一断就永久失效"。
/// 状态机（单一写者：发送线程或重连线程）：
///
///   Connected --(连接不可恢复)--> Reconnecting --(握手成功)--> Connected
///                                               |
///                                               +--(握手连续失败达到上限)--> Disabled（终态）
///
///   - 重连全部在独立后台线程 "mwc-pipe-reconnect" 上，绝不阻塞游戏网络线程/主线程。
///     退避 1s/2s/4s... 封顶 30s（可配置），不会形成重连风暴；宿主长时间不在时保持
///     该节奏重试，期间钩子始终直通、聊天包只进有界队列（满即丢）。
///   - 握手幂等：LoadTasks 列表先用临时数组读全，再在 RequiredPackets 锁内整体替换，
///     钩子不会看到半更新的 HashSet；LoadProps 消费干净；LoadMethodTable 在重连时
///     **只消费不覆盖**已解析的函数指针（宿主 metadata 版本不一致时覆盖会把游戏打崩）；
///     ResumeMainThread 只在首次初始化发送，重连不发（游戏早已在运行）。
///   - 连接代际隔离：每次连接有独立的 reader/writer/Lock，发送线程在进入 IO 前把三者
///     一起快照；重连只是替换静态字段，卡在旧连接上的发送任务永远碰不到新连接。
///   - 宿主侧要求：宿主重启后重新创建同名管道 server 并接受 DLL 连接即可（DLL 会主动
///     Connect）。如果宿主不重建管道，DLL 停在 Reconnecting 有界重试，不崩溃、不阻塞游戏。
///
/// 与上游相比删掉了玩家属性推送，并**禁用了自动退出**——
/// 上游在订阅清空时会 Environment.Exit(0) 直接结束游戏进程，
/// 对常驻嗅探来说太危险。
/// </summary>
internal static class Goshujin {

    private const string DefaultPipeName = "MiliastraWonderlandChatBridgePipe";

    /// <summary>重连时单次 Connect 的超时（毫秒）。</summary>
    private const int ConnectTimeoutMs = 5000;

    /// <summary>一次重连握手的总超时（毫秒）。</summary>
    private const int HandshakeTimeoutMs = 5000;

    /// <summary>握手连续失败多少次后判定"重连彻底失败"，进入终态 Disabled。</summary>
    internal const int MaxHandshakeFailures = 3;

    internal const int DefaultReconnectInitialDelayMs = 1000;
    internal const int DefaultReconnectMaxDelayMs = 30000;
    internal const int MinReconnectDelayMs = 10;
    internal const int MaxReconnectDelayMs = 300000;

    /// <summary>连接状态。所有迁移都通过 Interlocked/Volatile 写入 int。</summary>
    internal enum ConnectionState {
        Disconnected = 0,
        Connected = 1,
        Reconnecting = 2,
        Disabled = 3,
    }

    /// <summary>只保护"当前连接字段"的快照/替换，持有时间极短，绝不在里面做 IO。</summary>
    private static readonly Lock _stateLock = new();

    /// <summary>连接状态变化信号：Connected/Disabled 时置位，开始重连时复位。</summary>
    private static readonly ManualResetEventSlim _connectionChanged = new(false);

    // ---- 当前连接（一次连接一代；重连时整体替换） ----
    private static Lock? _lock;
    private static Stream? _pipeStream;
    private static BinaryReader? _pipeReader;
    private static BinaryWriter? _pipeWriter;

    private static string _pipeName = DefaultPipeName;
    private static int _state; // ConnectionState
    private static int _reconnectThreadRunning;
    private static int _reconnectInitialDelayMs = DefaultReconnectInitialDelayMs;
    private static int _reconnectMaxDelayMs = DefaultReconnectMaxDelayMs;
    private static long _reconnectAttempts;
    private static long _handshakeFailures;
    private static long _lastConnectedTicks;

    /// <summary>
    /// 回归测试注入点：返回一条已"连接"的流，避免测试触碰真实命名管道。
    /// 生产环境保持 null，走真实的 <see cref="NamedPipeClientStream"/>。
    /// </summary>
    internal static Func<Stream>? ConnectStreamForTest { get; set; }

    internal static ConnectionState State => (ConnectionState) Volatile.Read(ref _state);

    /// <summary>累计重连尝试次数（观测用）。</summary>
    internal static long ReconnectAttempts => Interlocked.Read(ref _reconnectAttempts);

    /// <summary>当前连续握手失败次数（成功一次归零；达到 <see cref="MaxHandshakeFailures"/> 进入终态）。</summary>
    internal static long HandshakeFailures => Interlocked.Read(ref _handshakeFailures);

    /// <summary>上次成功连接的时刻（Environment.TickCount64，观测用；0 表示从未连上）。</summary>
    internal static long LastConnectedTicks => Volatile.Read(ref _lastConnectedTicks);

    /// <summary>重连首次退避（毫秒），默认 1000；测试可调小。</summary>
    internal static int ReconnectInitialDelayMs {
        get => Volatile.Read(ref _reconnectInitialDelayMs);
        set => Volatile.Write(ref _reconnectInitialDelayMs, Math.Clamp(value, MinReconnectDelayMs, MaxReconnectDelayMs));
    }

    /// <summary>重连退避上限（毫秒），默认 30000；宿主机长时间不在时按该间隔稳定重试。</summary>
    internal static int ReconnectMaxDelayMs {
        get => Volatile.Read(ref _reconnectMaxDelayMs);
        set => Volatile.Write(ref _reconnectMaxDelayMs, Math.Clamp(value, MinReconnectDelayMs, MaxReconnectDelayMs));
    }

    /// <summary>
    /// 首次连接（Awake 调用）。连不上直接抛出，由 Awake 决定注入失败——
    /// 与旧行为一致；重连场景走后台线程，绝不在这里阻塞游戏线程。
    /// </summary>
    public static void Init(string pipeName = DefaultPipeName) {
        _pipeName = pipeName;
        var pipe = new NamedPipeClientStream(pipeName);
        pipe.Connect();
        InstallConnection(pipe);
        Log.Trace("Pipe server connected.");
    }

    /// <summary>
    /// 后台发送线程调用：写一个包并读掉回执（无返回值）。
    ///
    /// 这个函数**只**允许在发送线程里调用。钩子线程绝不能进来——
    /// 它会在管道上阻塞，直到宿主读到数据并回一个字节。
    ///
    /// 为什么不返回 ACK 的值：协议里这 1 字节只表示"是否继续订阅"，本项目的宿主对
    /// 每个包恒回 false，订阅变更走控制管道；DLL 侧从未使用过该值。历史版本恒
    /// return true 曾让上层"可继续失败"分支成为死代码（复核发现），故改为 void。
    ///
    /// 连接代际隔离：先在 _stateLock 里把当前代的 Lock/Writer/Reader 一起快照，
    /// 再在快照到的 Lock 里做 IO。重连替换静态字段后，旧连接上卡住的任务只会操作
    /// 旧（已丢弃）的 writer，绝不会和新连接交错写。
    /// </summary>
    public static void WritePacketAndAwaitAck(ushort cmdId, byte[] data) {
        Lock sync;
        BinaryWriter writer;
        BinaryReader reader;
        using (_stateLock.EnterScope()) {
            if (State != ConnectionState.Connected || _lock is null || _pipeWriter is null || _pipeReader is null) {
                throw new IOException("聊天管道未连接");
            }
            sync = _lock;
            writer = _pipeWriter;
            reader = _pipeReader;
        }
        using (sync.EnterScope()) {
            writer.Write((byte) 4);
            writer.Write(cmdId);
            writer.Write(data.Length);
            writer.Write(data);
            writer.Flush();
            // 回执只用于"是否继续订阅"；本项目宿主恒回 false，读掉即可保持流对齐。
            // 值刻意不返回：见方法注释（避免死分支）。
            _ = reader.ReadBoolean();
        }
    }

    /// <summary>
    /// 永久关闭管道（终态）：置 Disabled、唤醒等待中的发送/重连线程、dispose 当前流，
    /// 让阻塞中的读/写以异常退出。可重复调用。
    /// </summary>
    public static void Shutdown() {
        Stream? stream;
        using (_stateLock.EnterScope()) {
            Volatile.Write(ref _state, (int) ConnectionState.Disabled);
            stream = _pipeStream;
            _pipeStream = null;
            _pipeReader = null;
            _pipeWriter = null;
            _lock = null;
        }
        try {
            stream?.Dispose();
        } catch {
            // 关闭过程中的异常无需处理。
        }
        _connectionChanged.Set();
    }

    /// <summary>
    /// 发送线程检测到连续失败后调用：把当前连接置为 Reconnecting，并启动重连线程。
    /// 返回 false 表示已经处于终态 Disabled（调用方应永久停用）。
    /// 检测到已断开时是幂等的：不会重复启动第二条重连线程，也不会形成重连风暴。
    /// </summary>
    internal static bool RequestReconnect(string reason) {
        Stream? oldStream;
        using (_stateLock.EnterScope()) {
            switch (State) {
                case ConnectionState.Disabled:
                    return false;
                case ConnectionState.Reconnecting:
                    return true; // 已有重连线程负责，本次请求合并
                case ConnectionState.Connected:
                case ConnectionState.Disconnected:
                default:
                    break;
            }
            oldStream = _pipeStream;
            _pipeStream = null;
            _pipeReader = null;
            _pipeWriter = null;
            _lock = null;
            Volatile.Write(ref _state, (int) ConnectionState.Reconnecting);
            _connectionChanged.Reset();
        }
        // 先 dispose 旧流：让卡在旧连接读/写上的发送任务终止，而不是一直占着旧 Lock。
        try {
            oldStream?.Dispose();
        } catch {
            // 关闭过程中的异常无需处理。
        }
        Log.Warn($"聊天管道失效，开始重连: {reason}");
        EnsureReconnectThread();
        return true;
    }

    /// <summary>
    /// 发送线程等待重连结果：成功返回 Connected；终态返回 Disabled。
    /// 阻塞在事件上（无自旋），Disable/重连成功都会立即唤醒。
    /// </summary>
    internal static ConnectionState WaitForConnectedOrDisabled() {
        while (true) {
            var state = State;
            if (state is ConnectionState.Connected or ConnectionState.Disabled) {
                return state;
            }
            _connectionChanged.Wait();
        }
    }

    // ------------------------------------------------------------------
    // 首次握手（Awake 路径）。协议帧格式与重连握手共用同一批私有方法。
    // ------------------------------------------------------------------

    public static void LoadTasks() {
        ReadTaskList(PipeReader, PipeWriter);
    }

    /// <summary>
    /// 读取宿主订阅的玩家属性。本项目不使用，但要按协议把列表消费干净，
    /// 否则后续帧会错位。
    /// </summary>
    public static void LoadProps() {
        ReadProps(PipeReader, PipeWriter);
    }

    public static unsafe void LoadMethodTable() {
        ApplyMethodTable(ReadMethodTable(PipeReader, PipeWriter));
    }

    public static void ResumeMainThread() {
        PipeWriter.Write((byte) 0xFE);
    }

    private static BinaryReader PipeReader =>
        _pipeReader ?? throw new IOException("聊天管道未连接");

    private static BinaryWriter PipeWriter =>
        _pipeWriter ?? throw new IOException("聊天管道未连接");

    // ------------------------------------------------------------------
    // 重连后台线程
    // ------------------------------------------------------------------

    private static void EnsureReconnectThread() {
        if (Interlocked.CompareExchange(ref _reconnectThreadRunning, 1, 0) != 0) {
            return;
        }
        try {
            var thread = new Thread(ReconnectLoop) {
                IsBackground = true,
                Name = "mwc-pipe-reconnect",
            };
            thread.Start();
        } catch (Exception ex) {
            Interlocked.Exchange(ref _reconnectThreadRunning, 0);
            MarkDisabled($"重连线程启动失败: {ex.Message}");
        }
    }

    /// <summary>
    /// 重连循环：退避重试 + 每次成功 Connect 后做一次幂等握手。
    /// 只有"连上了但握手连续失败达到 <see cref="MaxHandshakeFailures"/>"才进入终态；
    /// 宿主一直不在时保持退避上限间隔持续重试（有界 CPU，无自旋）。
    /// </summary>
    private static void ReconnectLoop() {
        try {
            var delay = ReconnectInitialDelayMs;
            while (true) {
                if (State == ConnectionState.Disabled) {
                    return;
                }
                Interlocked.Increment(ref _reconnectAttempts);
                Stream? stream = null;
                try {
                    stream = ConnectForReconnect();
                    if (TryHandshake(stream)) {
                        if (InstallConnection(stream)) {
                            Interlocked.Exchange(ref _handshakeFailures, 0);
                            Log.Info($"聊天管道重连成功（累计尝试 {ReconnectAttempts} 次）");
                            return;
                        }
                        // 握手期间被 Shutdown（终态）：丢弃新流，线程退出。
                        try {
                            stream.Dispose();
                        } catch {
                            // 关闭过程中的异常无需处理。
                        }
                        return;
                    }
                    try {
                        stream.Dispose();
                    } catch {
                        // 关闭过程中的异常无需处理。
                    }
                    stream = null;
                    var failures = Interlocked.Increment(ref _handshakeFailures);
                    Log.Warn($"重连握手失败（连续 {failures}/{MaxHandshakeFailures}）");
                    if (failures >= MaxHandshakeFailures) {
                        MarkDisabled($"重连握手连续失败 {failures} 次");
                        return;
                    }
                } catch (Exception ex) {
                    try {
                        stream?.Dispose();
                    } catch {
                        // 关闭过程中的异常无需处理。
                    }
                    Log.Trace($"重连尝试失败（累计尝试 {ReconnectAttempts} 次）: {ex.Message}");
                }
                if (State == ConnectionState.Disabled) {
                    return;
                }
                // 退避等待；Disable/重连成功会置位事件，立即醒来。
                _ = _connectionChanged.Wait(delay);
                delay = Math.Min(delay * 2, ReconnectMaxDelayMs);
            }
        } catch (Exception ex) {
            // 后台线程绝不能让异常逃逸：Native 的 AppDomain 处理器会弹框 + 退出游戏。
            MarkDisabled($"重连线程异常: {ex.Message}");
        } finally {
            Interlocked.Exchange(ref _reconnectThreadRunning, 0);
            // 极小竞态：重连成功瞬间又被请求重连，本线程已退出但状态仍是 Reconnecting，
            // 这里补启动下一条重连线程，避免停在 Reconnecting 无人推进。
            if (State == ConnectionState.Reconnecting) {
                EnsureReconnectThread();
            }
        }
    }

    private static Stream ConnectForReconnect() {
        var factory = ConnectStreamForTest;
        if (factory != null) {
            return factory();
        }
        var pipe = new NamedPipeClientStream(".", _pipeName);
        try {
            pipe.Connect(ConnectTimeoutMs);
            return pipe;
        } catch {
            // Connect 抛异常时句柄仍在 pipe 对象里；必须显式 dispose，
            // 否则每次重连失败都会泄漏一个命名管道句柄（复核意见）。
            try {
                pipe.Dispose();
            } catch {
                // 关闭过程中的异常无需处理。
            }
            throw;
        }
    }

    /// <summary>
    /// 带总超时执行一次握手。超时/异常 -> dispose 流让阻塞读退出，返回 false。
    /// </summary>
    private static bool TryHandshake(Stream stream) {
        var task = Task.Run(() => {
            try {
                return HandshakeCore(stream);
            } catch {
                return false;
            }
        });
        try {
            if (!task.Wait(HandshakeTimeoutMs)) {
                try {
                    stream.Dispose();
                } catch {
                    // 关闭过程中的异常无需处理。
                }
                return false;
            }
            return !task.IsFaulted && task.Result;
        } catch {
            try {
                stream.Dispose();
            } catch {
                // 关闭过程中的异常无需处理。
            }
            return false;
        }
    }

    /// <summary>
    /// 重连握手。幂等保证见 <see cref="Goshujin"/> 类型注释；这里刻意**不**发送
    /// 0xFE ResumeMainThread：游戏已经在运行，重连时再 resume 既无必要也有副作用。
    /// </summary>
    private static bool HandshakeCore(Stream stream) {
        using var reader = new BinaryReader(stream, Encoding.UTF8, leaveOpen: true);
        using var writer = new BinaryWriter(stream, Encoding.UTF8, leaveOpen: true);
        // 0xFA：先把列表读进临时数组，再在锁内整体替换 RequiredPackets，
        // 避免钩子线程看到空集合或半更新的 HashSet。
        ReadTaskList(reader, writer);
        // 0xFB：消费干净。
        ReadProps(reader, writer);
        // 0xFD：只消费，不覆盖已解析的指针（幂等）。
        ApplyMethodTableIfMissing(ReadMethodTable(reader, writer));
        return true;
    }

    // ------------------------------------------------------------------
    // 协议帧（首次握手与重连握手共用）
    // ------------------------------------------------------------------

    private static void ReadTaskList(BinaryReader reader, BinaryWriter writer) {
        writer.Write((byte) 0xFA);
        writer.Flush();
        var cmds = new List<uint>();
        uint cmdId;
        while ((cmdId = reader.ReadUInt32()) != uint.MaxValue) {
            cmds.Add(cmdId);
        }
        lock (Application.RequiredPackets) {
            Application.RequiredPackets.Clear();
            foreach (var cmd in cmds) {
                Application.RequiredPackets.Add(cmd);
            }
        }
    }

    private static void ReadProps(BinaryReader reader, BinaryWriter writer) {
        writer.Write((byte) 0xFB);
        writer.Flush();
        while (reader.ReadUInt32() != uint.MaxValue) {
            // 忽略：聊天嗅探不需要玩家属性。
        }
    }

    /// <summary>发送 0xFD 并读回 10 个 u32 RVA（下标 0 是 DoCmd，本项目不使用）。</summary>
    private static uint[] ReadMethodTable(BinaryReader reader, BinaryWriter writer) {
        writer.Write((byte) 0xFD);
        writer.Flush();
        var rvas = new uint[10];
        for (var i = 0; i < rvas.Length; i++) {
            rvas[i] = reader.ReadUInt32();
        }
        return rvas;
    }

    /// <summary>
    /// 回归测试注入点：RVA→VA 映射（生产为 null，走 <see cref="Native.RVAToVA"/>）。
    /// 用于在非 Windows 环境下验证"逐字段补齐"的重连幂等逻辑，避免测试触碰 P/Invoke。
    /// </summary>
    internal static Func<uint, nint>? RvaResolverForTest { get; set; }

    private static nint RvaToVa(uint rva) =>
        RvaResolverForTest is { } resolver ? resolver(rva) : Native.RVAToVA(rva);

    private static unsafe void ApplyMethodTable(uint[] rvas) {
        GameMethod.UpdateNormalProp = (delegate*unmanaged<nint, int, double, double, int, void>) RvaToVa(rvas[1]);
        GameMethod.NewString = (delegate*unmanaged<nint, nint>) RvaToVa(rvas[2]);
        GameMethod.FindGameObject = (delegate*unmanaged<nint, nint>) RvaToVa(rvas[3]);
        GameMethod.EventSystemUpdate = (delegate*unmanaged<nint, void>) RvaToVa(rvas[4]);
        GameMethod.SimulatePointerClick = (delegate*unmanaged<nint, nint, bool>) RvaToVa(rvas[5]);
        GameMethod.ToInt32 = (delegate*unmanaged<byte*, int, int>) RvaToVa(rvas[6]);
        GameMethod.TcpStatePtr = (void**) RvaToVa(rvas[7]);
        GameMethod.SharedInfoPtr = (void**) RvaToVa(rvas[8]);
        GameMethod.Decompress = (delegate*unmanaged<void*, void*, void*, uint, void*, uint, bool>) RvaToVa(rvas[9]);
    }

    /// <summary>
    /// 重连握手时的**幂等**写回：**逐字段**判断，只补齐仍为 null 的项，绝不覆盖任何
    /// 已解析成功的函数指针。
    ///
    /// 为什么：宿主重启后可能是另一个版本/另一份 metadata，RVA 与当前游戏进程
    /// 不一定匹配；覆盖已工作的函数指针会立刻在下一次解包时调用野指针，直接崩溃。
    /// 旧实现是"全有或全无"（任一关键指针为 null 就整表覆盖，包括原本正常的 ToInt32），
    /// 与"只补绝不覆盖"的注释矛盾（复核 M2），这里按字段逐个补齐。
    ///
    /// 注：只补 null 的分支会调用 <see cref="Native.RVAToVA"/>（Windows P/Invoke），
    /// 正常生产路径下首次 Awake 已把全部指针填好，重连不会走到这里。
    /// </summary>
    private static unsafe void ApplyMethodTableIfMissing(uint[] rvas) {
        if (GameMethod.UpdateNormalProp == null) {
            GameMethod.UpdateNormalProp = (delegate*unmanaged<nint, int, double, double, int, void>) RvaToVa(rvas[1]);
        }
        if (GameMethod.NewString == null) {
            GameMethod.NewString = (delegate*unmanaged<nint, nint>) RvaToVa(rvas[2]);
        }
        if (GameMethod.FindGameObject == null) {
            GameMethod.FindGameObject = (delegate*unmanaged<nint, nint>) RvaToVa(rvas[3]);
        }
        if (GameMethod.EventSystemUpdate == null) {
            GameMethod.EventSystemUpdate = (delegate*unmanaged<nint, void>) RvaToVa(rvas[4]);
        }
        if (GameMethod.SimulatePointerClick == null) {
            GameMethod.SimulatePointerClick = (delegate*unmanaged<nint, nint, bool>) RvaToVa(rvas[5]);
        }
        if (GameMethod.ToInt32 == null) {
            GameMethod.ToInt32 = (delegate*unmanaged<byte*, int, int>) RvaToVa(rvas[6]);
        }
        if (GameMethod.TcpStatePtr == null) {
            GameMethod.TcpStatePtr = (void**) RvaToVa(rvas[7]);
        }
        if (GameMethod.SharedInfoPtr == null) {
            GameMethod.SharedInfoPtr = (void**) RvaToVa(rvas[8]);
        }
        if (GameMethod.Decompress == null) {
            GameMethod.Decompress = (delegate*unmanaged<void*, void*, void*, uint, void*, uint, bool>) RvaToVa(rvas[9]);
        }
    }

    // ------------------------------------------------------------------
    // 连接安装/终态
    // ------------------------------------------------------------------

    /// <summary>
    /// 安装一条已握手成功的新连接。返回 false 表示期间已进入终态（调用方负责 dispose 流）。
    /// 只有在这里才替换静态连接字段，因此状态一定是 Connected 与字段一致。
    /// </summary>
    private static bool InstallConnection(Stream stream) {
        var reader = new BinaryReader(stream);
        var writer = new BinaryWriter(stream);
        using (_stateLock.EnterScope()) {
            if (State == ConnectionState.Disabled) {
                return false;
            }
            _pipeStream = stream;
            _pipeReader = reader;
            _pipeWriter = writer;
            _lock = new Lock();
            Volatile.Write(ref _lastConnectedTicks, Environment.TickCount64);
            Volatile.Write(ref _state, (int) ConnectionState.Connected);
        }
        _connectionChanged.Set();
        return true;
    }

    private static void MarkDisabled(string reason) {
        Log.Error($"聊天管道重连彻底失败，进入终态: {reason}");
        Shutdown();
    }

    /// <summary>回归测试用：把注入好的当前连接标记为 Connected（不触碰真实管道）。</summary>
    internal static void MarkConnectedForTest() {
        Volatile.Write(ref _lastConnectedTicks, Environment.TickCount64);
        Volatile.Write(ref _state, (int) ConnectionState.Connected);
        _connectionChanged.Set();
    }
}
