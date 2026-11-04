using System.Buffers.Binary;
using System.IO.Pipes;
using MiliastraWonderlandChatBridge.Utilities;

namespace MiliastraWonderlandChatBridge;

/// <summary>
/// 健康上报通道（第四条命名管道，可选增强）。
///
/// 宿主是 server，DLL 是 client：管道名 <see cref="PipeName"/>，方向 DLL→宿主。
/// 每 <see cref="HeartbeatIntervalMs"/> 写一帧固定 324 字节小端心跳，让宿主能区分
/// 「DLL 静默」和「DLL 活着但已停止转发」（尤其 <see cref="PacketOutbox.Disable"/> 之后）。
///
/// 三条铁律：
/// 1. 完全独立于 PacketOutbox：聊天发送线程/队列停用绝不能停掉本线程；本线程也不等待
///    任何聊天 IO，只读取静态状态。
/// 2. 绝不阻塞游戏线程：所有连接/写入/退避都在独立后台线程。
/// 3. 异常绝不逃逸：NativeAOT 下未处理异常会直接终止游戏进程，ReporterLoop 全程兜底。
///
/// 帧格式（与宿主 src/health.rs 对齐，固定 324 字节小端）：
///   magic u16 = 0x4D57, version u8 = 1, kind u8 = 0x01,
///   seq u32, flags u32,
///   timeout_count u64, abandoned_tasks u64。
/// flags: bit0=PacketOutbox.Disabled, bit1=State==Reconnecting,
///        bit2=DebugOutbox.Disabled, bit3=AbandonedIoTaskLimitReached。
/// </summary>
internal static class HealthReporter {

    /// <summary>健康管道名，必须与宿主 src/health.rs 的 HEALTH_PIPE_NAME 一致。</summary>
    internal const string PipeName = "MiliastraWonderlandChatBridgeHealth";

    internal const ushort FrameMagic = 0x4D57;
    internal const byte FrameVersion = 1;
    internal const byte FrameKind = 0x01;
    internal const int FrameSize = 324;

    internal const uint FlagChatDisabled = 1u << 0;
    internal const uint FlagReconnecting = 1u << 1;
    internal const uint FlagDebugDisabled = 1u << 2;
    internal const uint FlagAbandonedLimit = 1u << 3;

    /// <summary>连接超时。管道由宿主在注入前创建，正常在毫秒级就能连上。</summary>
    private const int ConnectTimeoutMs = 5000;

    /// <summary>单帧写的超时（毫秒）。宿主假死/停读时不能把上报线程永久挂住。</summary>
    internal const int WriteTimeoutMs = 1000;

    internal const int DefaultHeartbeatIntervalMs = 1000;
    internal const int MinHeartbeatIntervalMs = 10;
    internal const int MaxHeartbeatIntervalMs = 600_000;

    internal const int DefaultReconnectInitialDelayMs = 1000;
    internal const int DefaultReconnectMaxDelayMs = 30000;
    internal const int MinReconnectDelayMs = 10;
    internal const int MaxReconnectDelayMs = 300000;

    private static int _started;
    private static long _seq;
    private static long _connectCount;
    private static long _reconnectAttempts;
    private static long _framesSent;
    private static int _heartbeatIntervalMs = DefaultHeartbeatIntervalMs;
    private static int _reconnectInitialDelayMs = DefaultReconnectInitialDelayMs;
    private static int _reconnectMaxDelayMs = DefaultReconnectMaxDelayMs;

    /// <summary>累计成功连接的次数（观测/回归用）。</summary>
    internal static long ConnectCount => Interlocked.Read(ref _connectCount);

    /// <summary>累计连接尝试次数（观测/回归用）。</summary>
    internal static long ReconnectAttempts => Interlocked.Read(ref _reconnectAttempts);

    /// <summary>累计成功写出的心跳帧数（观测/回归用）。</summary>
    internal static long FramesSent => Interlocked.Read(ref _framesSent);

    /// <summary>最近一次分配的序号（观测/回归用）。</summary>
    internal static uint LastSeq => unchecked((uint) Interlocked.Read(ref _seq));

    /// <summary>心跳间隔（毫秒），默认 1000；测试可调小。</summary>
    internal static int HeartbeatIntervalMs {
        get => Volatile.Read(ref _heartbeatIntervalMs);
        set => Volatile.Write(ref _heartbeatIntervalMs,
            Math.Clamp(value, MinHeartbeatIntervalMs, MaxHeartbeatIntervalMs));
    }

    /// <summary>重连首次退避（毫秒），默认 1000；测试可调小。</summary>
    internal static int ReconnectInitialDelayMs {
        get => Volatile.Read(ref _reconnectInitialDelayMs);
        set => Volatile.Write(ref _reconnectInitialDelayMs, Math.Clamp(value, MinReconnectDelayMs, MaxReconnectDelayMs));
    }

    /// <summary>重连退避上限（毫秒），默认 30000；宿主长期不在时按该间隔稳定重试。</summary>
    internal static int ReconnectMaxDelayMs {
        get => Volatile.Read(ref _reconnectMaxDelayMs);
        set => Volatile.Write(ref _reconnectMaxDelayMs, Math.Clamp(value, MinReconnectDelayMs, MaxReconnectDelayMs));
    }

    /// <summary>
    /// 回归测试注入点：返回一条已"连接"的流，避免测试触碰真实命名管道。
    /// 生产环境保持 null，走真实的 <see cref="NamedPipeClientStream"/>。
    /// </summary>
    internal static Func<Stream>? ConnectStreamForTest { get; set; }

    /// <summary>启动后台心跳线程。幂等，且立即返回，绝不阻塞初始化。</summary>
    internal static void Start() {
        if (Interlocked.Exchange(ref _started, 1) != 0) {
            return;
        }
        var thread = new Thread(ReporterLoop) {
            IsBackground = true,
            Name = "mwc-health-reporter",
        };
        try {
            thread.Start();
        } catch (Exception ex) {
            // 线程都起不来时绝不能影响注入/聊天；允许后续极端情况下重试。
            Volatile.Write(ref _started, 0);
            Log.Warn($"健康上报线程启动失败: {ex.Message}");
        }
    }

    /// <summary>
    /// 当前心跳 flags。只读取静态状态，不做 IO、不加聊天锁，因此可在任何线程调用。
    /// </summary>
    internal static uint CurrentFlags() {
        uint flags = 0;
        if (PacketOutbox.Disabled) {
            flags |= FlagChatDisabled;
        }
        if (Goshujin.State == Goshujin.ConnectionState.Reconnecting) {
            flags |= FlagReconnecting;
        }
        if (DebugOutbox.Disabled) {
            flags |= FlagDebugDisabled;
        }
        if (Goshujin.AbandonedIoTaskLimitReached) {
            flags |= FlagAbandonedLimit;
        }
        return flags;
    }

    /// <summary>按当前静态状态编码一帧心跳（测试可直接验证帧格式/字段）。</summary>
    internal static byte[] EncodeFrame(uint seq) {
        Application.SnapshotRequired();
        return EncodeFrameFields(seq, CurrentFlags(), unchecked((ulong) PacketOutbox.TimeoutCount),
            unchecked((ulong) Goshujin.AbandonedIoTaskCount),
            unchecked((ulong) Application.InvalidPacketLengthCount),
            unchecked((ulong) Application.ForwardErrorCount),
            unchecked((ulong) Application.HookEnteredCount),
            unchecked((ulong) Application.BadMagicCount),
            unchecked((ulong) Application.TryGetDataFailCount),
            unchecked((ulong) Application.AcceptedCount),
            unchecked((ulong) Application.RequiredCount),
            unchecked((ulong) Application.FilteredCount),
            unchecked((ulong) Application.LastFilteredCmd),
            unchecked((ulong) Application.RequiredSample(0)), unchecked((ulong) Application.RequiredSample(1)),
            unchecked((ulong) Application.RequiredSample(2)), unchecked((ulong) Application.RequiredSample(3)),
            unchecked((ulong) Application.RequiredSample(4)), unchecked((ulong) Application.RequiredSample(5)),
            unchecked((ulong) Application.RequiredSample(6)), unchecked((ulong) Application.RequiredSample(7)),
            Application.CmdSeen(0), Application.CmdSeen(1), Application.CmdSeen(2), Application.CmdSeen(3),
            Application.CmdSeen(4), Application.CmdSeen(5), Application.CmdSeen(6), Application.CmdSeen(7),
            Application.CmdSeenTotal,
            Application.RawHeader(0), Application.RawHeader(1), Application.RawHeader(2),
            Application.RawHeader(3), Application.RawHeader(4), Application.RawHeader(5),
            Application.ProbeLen(0), Application.ProbeLen(1), Application.ProbeLen(2),
            Application.ProbeLen(3), Application.ProbeLen(4));
    }

    /// <summary>
    /// 纯函数版编码：字段全部由调用方给出，不读任何静态状态。
    ///
    /// 存在的理由是可测性——只有这一层才能与 Rust src/health_vector.rs 里钉死的字节
    /// 做逐字节比对。若只测 ParseHealthFrame（解码器），「发射端改了字节序或字段偏移」
    /// 不会被黄金向量抓到，只能靠间接的行为断言兜。
    /// </summary>
    internal static byte[] EncodeFrameFields(uint seq, uint flags, ulong timeoutCount, ulong abandonedTasks) =>
        EncodeFrameFields(seq, flags, timeoutCount, abandonedTasks, 0, 0);

    /// <summary>带诊断计数的完整版编码（协议 v2，帧长 44 字节）。</summary>
    internal static byte[] EncodeFrameFields(uint seq, uint flags, ulong timeoutCount,
        ulong abandonedTasks, ulong invalidLength, ulong forwardErrors) =>
        EncodeFrameFields(seq, flags, timeoutCount, abandonedTasks, invalidLength, forwardErrors,
            0, 0, 0, 0, 0, 0, 0);

    /// <summary>协议 v3：额外携带转发流水线各阶段计数，用于定位「包在哪一步被丢」。</summary>
    internal static byte[] EncodeFrameFields(uint seq, uint flags, ulong timeoutCount,
        ulong abandonedTasks, ulong invalidLength, ulong forwardErrors,
        ulong hookEntered, ulong badMagic, ulong tryGetDataFail, ulong accepted) =>
        EncodeFrameFields(seq, flags, timeoutCount, abandonedTasks, invalidLength, forwardErrors,
            hookEntered, badMagic, tryGetDataFail, accepted, 0, 0, 0);

    /// <summary>协议 v4：再带上过滤阶段计数。</summary>
    internal static byte[] EncodeFrameFields(uint seq, uint flags, ulong timeoutCount,
        ulong abandonedTasks, ulong invalidLength, ulong forwardErrors,
        ulong hookEntered, ulong badMagic, ulong tryGetDataFail, ulong accepted,
        ulong requiredCount, ulong filtered, ulong lastFilteredCmd) =>
        EncodeFrameFields(seq, flags, timeoutCount, abandonedTasks, invalidLength, forwardErrors,
            hookEntered, badMagic, tryGetDataFail, accepted, requiredCount, filtered, lastFilteredCmd,
            0, 0, 0, 0, 0, 0, 0, 0, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L);

    /// <summary>协议 v5：额外携带订阅列表前 8 项，用于确认 LoadTasks 内容。</summary>
    internal static byte[] EncodeFrameFields(uint seq, uint flags, ulong timeoutCount,
        ulong abandonedTasks, ulong invalidLength, ulong forwardErrors,
        ulong hookEntered, ulong badMagic, ulong tryGetDataFail, ulong accepted,
        ulong requiredCount, ulong filtered, ulong lastFilteredCmd,
        ulong s0, ulong s1, ulong s2, ulong s3, ulong s4, ulong s5, ulong s6, ulong s7) =>
        EncodeFrameFields(seq, flags, timeoutCount, abandonedTasks, invalidLength, forwardErrors,
            hookEntered, badMagic, tryGetDataFail, accepted, requiredCount, filtered, lastFilteredCmd,
            s0, s1, s2, s3, s4, s5, s6, s7, 0, 0, 0, 0, 0, 0, 0, 0, 0);

    /// <summary>协议 v6：再带上观测到的 cmdId 环形缓冲。</summary>
    internal static byte[] EncodeFrameFields(uint seq, uint flags, ulong timeoutCount,
        ulong abandonedTasks, ulong invalidLength, ulong forwardErrors,
        ulong hookEntered, ulong badMagic, ulong tryGetDataFail, ulong accepted,
        ulong requiredCount, ulong filtered, ulong lastFilteredCmd,
        ulong s0, ulong s1, ulong s2, ulong s3, ulong s4, ulong s5, ulong s6, ulong s7,
        long c0, long c1, long c2, long c3, long c4, long c5, long c6, long c7, long cTotal) =>
        EncodeFrameFields(seq, flags, timeoutCount, abandonedTasks, invalidLength, forwardErrors,
            hookEntered, badMagic, tryGetDataFail, accepted, requiredCount, filtered, lastFilteredCmd,
            s0, s1, s2, s3, s4, s5, s6, s7, c0, c1, c2, c3, c4, c5, c6, c7, cTotal, 0, 0, 0, 0, 0, 0);

    /// <summary>协议 v7：带上不同包的原始 8 字节头。</summary>
    internal static byte[] EncodeFrameFields(uint seq, uint flags, ulong timeoutCount,
        ulong abandonedTasks, ulong invalidLength, ulong forwardErrors,
        ulong hookEntered, ulong badMagic, ulong tryGetDataFail, ulong accepted,
        ulong requiredCount, ulong filtered, ulong lastFilteredCmd,
        ulong s0, ulong s1, ulong s2, ulong s3, ulong s4, ulong s5, ulong s6, ulong s7,
        long c0, long c1, long c2, long c3, long c4, long c5, long c6, long c7, long cTotal,
        ulong r0, ulong r1, ulong r2, ulong r3, ulong r4, ulong r5) =>
        EncodeFrameFields(seq, flags, timeoutCount, abandonedTasks, invalidLength, forwardErrors,
            hookEntered, badMagic, tryGetDataFail, accepted, requiredCount, filtered, lastFilteredCmd,
            s0, s1, s2, s3, s4, s5, s6, s7, c0, c1, c2, c3, c4, c5, c6, c7, cTotal,
            r0, r1, r2, r3, r4, r5, 0, 0, 0, 0, 0);

    /// <summary>协议 v8：带上第一个包的完整 40 字节，判断 val 语义。</summary>
    internal static byte[] EncodeFrameFields(uint seq, uint flags, ulong timeoutCount,
        ulong abandonedTasks, ulong invalidLength, ulong forwardErrors,
        ulong hookEntered, ulong badMagic, ulong tryGetDataFail, ulong accepted,
        ulong requiredCount, ulong filtered, ulong lastFilteredCmd,
        ulong s0, ulong s1, ulong s2, ulong s3, ulong s4, ulong s5, ulong s6, ulong s7,
        long c0, long c1, long c2, long c3, long c4, long c5, long c6, long c7, long cTotal,
        ulong r0, ulong r1, ulong r2, ulong r3, ulong r4, ulong r5,
        ulong p0, ulong p1, ulong p2, ulong p3, ulong p4) {
        var frame = new byte[FrameSize];
        BinaryPrimitives.WriteUInt16LittleEndian(frame.AsSpan(0), FrameMagic);
        frame[2] = FrameVersion;
        frame[3] = FrameKind;
        BinaryPrimitives.WriteUInt32LittleEndian(frame.AsSpan(4), seq);
        BinaryPrimitives.WriteUInt32LittleEndian(frame.AsSpan(8), flags);
        BinaryPrimitives.WriteUInt64LittleEndian(frame.AsSpan(12), timeoutCount);
        BinaryPrimitives.WriteUInt64LittleEndian(frame.AsSpan(20), abandonedTasks);
        BinaryPrimitives.WriteUInt64LittleEndian(frame.AsSpan(28), invalidLength);
        BinaryPrimitives.WriteUInt64LittleEndian(frame.AsSpan(36), forwardErrors);
        BinaryPrimitives.WriteUInt64LittleEndian(frame.AsSpan(44), hookEntered);
        BinaryPrimitives.WriteUInt64LittleEndian(frame.AsSpan(52), badMagic);
        BinaryPrimitives.WriteUInt64LittleEndian(frame.AsSpan(60), tryGetDataFail);
        BinaryPrimitives.WriteUInt64LittleEndian(frame.AsSpan(68), accepted);
        BinaryPrimitives.WriteUInt64LittleEndian(frame.AsSpan(76), requiredCount);
        BinaryPrimitives.WriteUInt64LittleEndian(frame.AsSpan(84), filtered);
        BinaryPrimitives.WriteUInt64LittleEndian(frame.AsSpan(92), lastFilteredCmd);
        var samples = new[] { s0, s1, s2, s3, s4, s5, s6, s7 };
        for (var i = 0; i < samples.Length; i++) {
            BinaryPrimitives.WriteUInt64LittleEndian(frame.AsSpan(100 + i * 8), samples[i]);
        }
        var seen = new[] { c0, c1, c2, c3, c4, c5, c6, c7 };
        for (var i = 0; i < seen.Length; i++) {
            BinaryPrimitives.WriteUInt64LittleEndian(frame.AsSpan(164 + i * 8), unchecked((ulong) seen[i]));
        }
        BinaryPrimitives.WriteUInt64LittleEndian(frame.AsSpan(228), unchecked((ulong) cTotal));
        var raw = new[] { r0, r1, r2, r3, r4, r5 };
        for (var i = 0; i < raw.Length; i++) {
            BinaryPrimitives.WriteUInt64LittleEndian(frame.AsSpan(236 + i * 8), raw[i]);
        }
        var probe = new[] { p0, p1, p2, p3, p4 };
        for (var i = 0; i < probe.Length; i++) {
            BinaryPrimitives.WriteUInt64LittleEndian(frame.AsSpan(284 + i * 8), probe[i]);
        }
        return frame;
    }

    private static void ReporterLoop() {
        var delay = ReconnectInitialDelayMs;
        Stream? stream = null;
        try {
            while (true) {
                if (stream is null) {
                    Interlocked.Increment(ref _reconnectAttempts);
                    try {
                        stream = ConnectStream();
                        Interlocked.Increment(ref _connectCount);
                        // 成功连上后重置退避，下次断开从最小间隔开始。
                        delay = ReconnectInitialDelayMs;
                        Log.Trace("Health pipe connected.");
                    } catch (Exception ex) {
                        Log.Trace($"健康上报连接失败（累计尝试 {ReconnectAttempts} 次）: {ex.Message}");
                        Thread.Sleep(delay);
                        delay = Math.Min(delay * 2, ReconnectMaxDelayMs);
                        continue;
                    }
                }

                try {
                    var seq = unchecked((uint) Interlocked.Increment(ref _seq));
                    var frame = EncodeFrame(seq);
                    stream.Write(frame, 0, frame.Length);
                    stream.Flush();
                    Interlocked.Increment(ref _framesSent);
                } catch (Exception ex) {
                    // 写失败只影响上报：废弃当前连接，退避后重建；绝不触碰聊天通道。
                    Log.Warn($"健康上报写失败，重建连接: {ex.Message}");
                    try {
                        stream.Dispose();
                    } catch {
                        // 关闭过程中的异常无需处理。
                    }
                    stream = null;
                    Thread.Sleep(delay);
                    delay = Math.Min(delay * 2, ReconnectMaxDelayMs);
                    continue;
                }

                Thread.Sleep(HeartbeatIntervalMs);
            }
        } catch (Exception ex) {
            // 后台线程未捕获异常会触发 Native 的 AppDomain 处理器弹框 + 退出游戏，必须兜住。
            Log.Error($"健康上报线程异常退出: {ex.Message}");
        } finally {
            try {
                stream?.Dispose();
            } catch {
                // 关闭过程中的异常无需处理。
            }
        }
    }

    private static Stream ConnectStream() {
        var factory = ConnectStreamForTest;
        if (factory != null) {
            return factory();
        }
        var pipe = new NamedPipeClientStream(".", PipeName, PipeDirection.Out);
        try {
            pipe.Connect(ConnectTimeoutMs);
            // WriteTimeout 在个别平台/运行时上可能不支持；设置失败不应让健康通道永远连不上
            // （最坏情况是写阻塞在后台线程，游戏线程仍不受影响）。
            try {
                pipe.WriteTimeout = WriteTimeoutMs;
            } catch {
                // 保留系统默认超时；不因设置失败而废弃连接。
            }
            return pipe;
        } catch {
            // Connect/设置超时抛异常时句柄仍在 pipe 对象里，必须显式 dispose。
            try {
                pipe.Dispose();
            } catch {
                // 关闭过程中的异常无需处理。
            }
            throw;
        }
    }
}
