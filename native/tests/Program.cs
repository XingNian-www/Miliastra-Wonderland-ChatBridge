using System.Buffers.Binary;
using System.Diagnostics;
using System.IO.Pipes;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Channels;
using MiliastraWonderlandChatBridge;
using MiliastraWonderlandChatBridge.Utilities;

internal static class Program {
    private const BindingFlags Flags = BindingFlags.Static | BindingFlags.NonPublic;
    private const int WaitMs = 10000;

    /// <summary>方法表"已解析"哨兵值（测试用；绝不等于 Native.RVAToVA 的结果）。</summary>
    private const nint MethodTableSentinel = 0xCAFE;

    /// <summary>握手时宿主下发的假 RVA（重连测试用；断言它们不会被应用到已解析的函数指针）。</summary>
    private static readonly uint[] DefaultRvas = new uint[] {
        0x1001, 0x1002, 0x1003, 0x1004, 0x1005, 0x1006, 0x1007, 0x1008, 0x1009, 0x100A,
    };
    private static int _checks;
    private static bool _decompressResult;
    private static unsafe delegate* unmanaged<byte*, int, int> Hook =>
        (delegate* unmanaged<byte*, int, int>) typeof(Application)
            .GetMethod("OnToInt32", Flags)!.MethodHandle.GetFunctionPointer();
    private unsafe delegate bool ParseData(byte* value, int length, out Span<byte> data, out void* allocation);

    private static void Main(string[] args) {
        if (args.Length == 0) {
            ParserChecks();
            ModeChecks();
            DebugChecks();
            LogSafetyChecks();
            LogOutputSinkChecks();
            foreach (var scenario in new[]
                     {
                         "queue", "ack", "disconnect", "timeout", "sendstuck", "sendwriteblock",
                         "reconnectbackoff", "reconnecthandshake", "reconnectpassthrough", "reconnectexhaust",
                         "modereconnect", "controlstale", "abandonedlimit", "handshakewindow", "handshakegap",
                         "debugrecovery", "abortinit",
                         "debugcapacity", "debugbytes", "debugwritetimeout",
                     })
                RunIsolated(scenario);
        } else {
            switch (args[0]) {
                case "queue": QueueChecks(); break;
                case "ack": SenderChecks(false); break;
                case "disconnect": SenderChecks(true); break;
                case "timeout": TimeoutChecks(); break;
                case "sendstuck": SendStuckReconnectChecks(); break;
                case "sendwriteblock": SendWriteBlockReconnectChecks(); break;
                case "reconnectbackoff": ReconnectBackoffChecks(); break;
                case "reconnecthandshake": ReconnectHandshakeChecks(); break;
                case "reconnectpassthrough": ReconnectPassthroughChecks(); break;
                case "reconnectexhaust": ReconnectExhaustChecks(); break;
                case "modereconnect": ModeReconnectChecks(); break;
                case "controlstale": ControlStaleChecks(); break;
                case "abandonedlimit": AbandonedLimitChecks(); break;
                case "handshakegap": HandshakeGapChecks(); break;
                case "handshakewindow": HandshakeWindowChecks(); break;
                case "debugrecovery": DebugRecoveryChecks(); break;
                case "abortinit": AbortInitializationChecks(); break;
                case "debugcapacity": DebugCapacityChecks(); break;
                case "debugbytes": DebugByteCapChecks(); break;
                case "debugwritetimeout": DebugWriteTimeoutChecks(); break;
                default: throw new ArgumentException("Unknown regression scenario");
            }
        }
        Console.WriteLine($"PASS: {_checks} checks ({(args.Length == 0 ? "all scenarios" : args[0])}); no game or Windows APIs invoked.");
    }

    // A fresh process isolates the production static, single-use channel and sender lifecycle.
    private static void RunIsolated(string scenario) {
        var start = new ProcessStartInfo(Environment.ProcessPath!) {
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        };
        if (Path.GetFileNameWithoutExtension(Environment.ProcessPath!).Equals("dotnet", StringComparison.OrdinalIgnoreCase))
            start.ArgumentList.Add(typeof(Program).Assembly.Location);
        start.ArgumentList.Add(scenario);
        using var child = Process.Start(start)!;
        var output = child.StandardOutput.ReadToEndAsync();
        var error = child.StandardError.ReadToEndAsync();
        if (!child.WaitForExit(30000)) {
            child.Kill(entireProcessTree: true);
            child.WaitForExit();
            throw new Exception($"Scenario {scenario} exceeded 30 seconds");
        }
        Console.Write(output.GetAwaiter().GetResult());
        var stderr = error.GetAwaiter().GetResult();
        Check(child.ExitCode == 0, $"Scenario {scenario} failed: {stderr}");
    }

    private static unsafe void ParserChecks() {
        Check(Application.GetDecompressedSize([]) == 0, "empty header");
        Check(Application.GetDecompressedSize([0x40, 0x80, 1]) == 128, "size varint");
        Check(Application.GetDecompressedSize([0x0A, 2, 0xFF, 0xFF, 0x40, 7]) == 7,
            "length-delimited field advances past its length varint");
        Check(Application.GetDecompressedSize([0x0A, 0x80, 1, .. new byte[128], 0x40, 9]) == 9,
            "multibyte length offset regression");
        Check(Application.GetDecompressedSize([8, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
            0xFF, 0xFF, 0xFF, 0xFF, 1, 0x40, 3]) == 3, "ten-byte unknown varint");
        Check(Application.GetDecompressedSize([9, .. new byte[8], 21, .. new byte[4], 0x40, 4]) == 4,
            "fixed width fields");
        Check(Application.GetDecompressedSize([0x40, 1, 0x40, 2]) == 2, "last scalar wins");
        foreach (var invalid in new byte[][] {
            [0], [0x80], [0x0A, 5, 1], [9, 1], [13, 1], [0x0B], [0x0E], [0x40],
            [8, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 2],
            [0x40, 0x81, 0x80, 0x40], [0x40, 1, 0x80]
        }) {
            try {
                Application.GetDecompressedSize(invalid);
                throw new Exception("Expected InvalidDataException");
            } catch (InvalidDataException) {
                _checks++;
            }
        }
        var parse = typeof(Application).GetMethod("TryGetData", Flags)!.CreateDelegate<ParseData>();
        byte[] packet = [0x45, 0x67, 0, 1, 0, 0, 0, 0, 0, 2, 10, 20, 0x89, 0xAB];
        fixed (byte* p = packet) {
            Check(parse(p, packet.Length, out var data, out var allocation) && allocation == null &&
                  data.SequenceEqual(new byte[] {10, 20}), "uncompressed payload");
            BinaryPrimitives.WriteUInt32BigEndian(packet.AsSpan(6), uint.MaxValue);
            Check(!parse(p, packet.Length, out _, out _), "oversized packet rejected before pointer arithmetic");
            BinaryPrimitives.WriteUInt32BigEndian(packet.AsSpan(6), 3);
            Check(!parse(p, packet.Length, out _, out _), "truncated packet rejected");
        }
    }

    /// <summary>
    /// 生产 Release 只定义 EnableLogging、不定义 EnableConsoleOutput，因此 _consoleWriter
    /// 恒为 null。WriteLog 必须安全丢弃而不是抛 NRE（否则日志反而会杀掉游戏线程/后台线程）。
    /// 测试程序集同样没有启用控制台，这里用反射真正执行 WriteLog 的 null-writer 分支
    /// （[Conditional] 只剔除调用点，不影响方法体；不能写普通调用，否则测试工程里会被剔除）。
    /// </summary>
    private static void LogSafetyChecks() {
        var writeLog = typeof(Log).GetMethod("WriteLog", BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic);
        Check(writeLog is not null, "WriteLog is compiled (EnableLogging path present)");
        foreach (var level in new[] { LogLevel.Trace, LogLevel.Debug, LogLevel.Info, LogLevel.Warn, LogLevel.Error }) {
            try {
                writeLog!.Invoke(null, new object[] { "console-less regression log", "LogSafetyChecks", level });
            } catch (TargetInvocationException ex) {
                throw new Exception($"WriteLog threw with null console writer at {level}: {ex.InnerException}");
            }
            _checks++;
        }
    }

    /// <summary>
    /// P1-1：生产 Release 不定义 EnableConsoleOutput，_consoleWriter 为 null；此时 WriteLog
    /// 必须把日志投递到调试器输出（Kernel32!OutputDebugStringW，DebugView 可抓），而不是格式化后丢弃。
    /// 测试用 OutputSinkForTest 替换实际 P/Invoke，在非 Windows 环境验证格式化/级别/截断/投递；
    /// Windows 真机端到端步骤见交付说明（DebugView 过滤 [MWC]）。
    /// </summary>
    private static void LogOutputSinkChecks() {
        var captured = new List<string>();
        Log.OutputSinkForTest = line => captured.Add(line);
        try {
            var writeLog = typeof(Log).GetMethod("WriteLog", BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic)!;
            writeLog.Invoke(null, new object[] { "debugger sink message", "SinkCheck", LogLevel.Warn });
            Check(captured.Count == 1, "WriteLog emits one line to the debugger sink when console writer is null");
            Check(captured[0].StartsWith("[MWC]") && captured[0].Contains("[WARN]") &&
                  captured[0].Contains("SinkCheck") && captured[0].Contains("debugger sink message"),
                "debugger line carries MWC prefix, level, tag and message");
            var huge = new string('x', 5000);
            writeLog.Invoke(null, new object[] { huge, "SinkCheck", LogLevel.Error });
            Check(captured.Count == 2 && captured[1].Length < huge.Length,
                "debugger line body is bounded/truncated for oversized messages");
            writeLog.Invoke(null, new object[] { "trace line", "SinkCheck", LogLevel.Trace });
            Check(captured.Count == 3 && captured[2].Contains("[TRACE]"), "trace level is delivered to the debugger sink");
        } catch (TargetInvocationException ex) {
            throw new Exception($"WriteLog threw while writing to debugger sink: {ex.InnerException}");
        } finally {
            Log.OutputSinkForTest = null;
        }
    }

    private static void ModeChecks() {
        Check(!ModeControl.AllPackets, "mode control defaults to chat");
        const ushort unknown = 0x7777;
        Check(!Application.ShouldForward(unknown), "chat mode does not forward unknown commands");
        ModeControl.SetAllPacketsForTest(true);
        try {
            Check(ModeControl.AllPackets, "test switch turns mode control to all");
            Check(Application.ShouldForward(unknown), "all mode forwards unknown commands");
        } finally {
            ModeControl.SetAllPacketsForTest(false);
        }
        const ushort chat = 0x7778;
        Application.RequiredPackets.Add(chat);
        try {
            Check(Application.ShouldForward(chat), "required chat command forwards in chat mode");
            ModeControl.SetAllPacketsForTest(true);
            Check(Application.ShouldForward(chat), "required chat command still forwards in all mode");
        } finally {
            ModeControl.SetAllPacketsForTest(false);
            Application.RequiredPackets.Remove(chat);
        }
        Check(!ModeControl.AllPackets, "mode control returns to chat after the test switch");
    }

    private static void DebugChecks() {
        var capacity = (int) typeof(DebugOutbox).GetField("Capacity", Flags)!.GetRawConstantValue()!;
        Check(capacity == 1024, "debug queue is bounded at 1024 packets");
        var maxBytes = (long) typeof(DebugOutbox).GetField("MaxQueuedBytes", Flags)!.GetRawConstantValue()!;
        Check(maxBytes == 32L * 1024 * 1024, "debug queue caps queued bytes at 32 MiB");
        // 帧编码：0x04 + cmdId(u16 LE) + 长度(u32 LE) + 数据，与聊天管道 0x04 完全一致。
        using var sink = new MemoryStream();
        using (var writer = new BinaryWriter(sink, Encoding.UTF8, leaveOpen: true)) {
            DebugOutbox.WriteFrame(writer, 0x1234, [10, 20]);
        }
        Check(sink.ToArray().SequenceEqual(new byte[] { 4, 0x34, 0x12, 2, 0, 0, 0, 10, 20 }),
            "debug frame encodes 0x04 + cmd + length + payload");
        // P2-2：主窗口等待必须有界（60-120s），通过反射读常量避免触发 Native 静态初始化。
        var waitTimeout = (int) typeof(Native).GetField("DefaultWaitMainWindowMs", Flags)!.GetRawConstantValue()!;
        Check(waitTimeout >= 60_000 && waitTimeout <= 120_000,
            "main-window wait timeout is bounded to 60-120 seconds");
    }

    // 每个场景单独进程：debug 队列与聊天队列都是单次使用的静态对象。
    private static void DebugCapacityChecks() {
        var capacity = (int) typeof(DebugOutbox).GetField("Capacity", Flags)!.GetRawConstantValue()!;
        for (var i = 0; i < capacity; i++) DebugOutbox.Enqueue((ushort) i, [(byte) i]);
        Check(DebugOutbox.Dropped == 0, "debug queue holds exactly capacity packets");
        DebugOutbox.Enqueue(0, [1]);
        Check(DebugOutbox.Dropped == 1, "debug queue drops the capacity+1 packet");
        Check(!PacketOutbox.Disabled, "debug queue overflow leaves the chat outbox enabled");

        DebugOutbox.Disable("regression");
        Check(DebugOutbox.Disabled, "debug disable latches");
        DebugOutbox.Enqueue(0, [2]);
        Check(DebugOutbox.Dropped == 1, "disabled debug outbox ignores new packets");
        Check(!PacketOutbox.Disabled, "debug disable leaves the chat outbox enabled");
    }

    private static void DebugByteCapChecks() {
        var maxBytes = (long) typeof(DebugOutbox).GetField("MaxQueuedBytes", Flags)!.GetRawConstantValue()!;
        // 32 份 1 MiB 正好填满 32 MiB 预算，但远未触及 1024 条容量上限。
        var chunk = new byte[maxBytes / 32];
        for (var i = 0; i < 32; i++) DebugOutbox.Enqueue((ushort) i, chunk);
        Check(DebugOutbox.Dropped == 0, "debug byte budget accepts 32 MiB");
        DebugOutbox.Enqueue(0, chunk);
        Check(DebugOutbox.Dropped == 1, "debug byte budget rejects the packet that would exceed it");
        Check(!PacketOutbox.Disabled, "debug byte budget overflow leaves the chat outbox enabled");
    }

    /// <summary>
    /// debug 写帧超时：阻塞的写必须被超时判定并 Disable，且 Disable 要 dispose 管道
    /// 让阻塞写退出；停用后钩子与 Enqueue 都不再做载荷拷贝（Copied 不增长）。
    /// </summary>
    private static void DebugWriteTimeoutChecks() {
        Check(DebugOutbox.DefaultWriteTimeoutMs == 1000, "default debug write deadline is 1000ms");
        Check(DebugOutbox.MinWriteTimeoutMs > 0, "debug write deadline has a positive lower bound");
        DebugOutbox.WriteTimeoutMs = 0;
        Check(DebugOutbox.WriteTimeoutMs == DebugOutbox.MinWriteTimeoutMs,
            "debug write timeout 0 clamps to minimum");
        DebugOutbox.WriteTimeoutMs = -1;
        Check(DebugOutbox.WriteTimeoutMs == DebugOutbox.MinWriteTimeoutMs,
            "negative debug write timeout clamps to minimum");
        DebugOutbox.WriteTimeoutMs = int.MaxValue;
        Check(DebugOutbox.WriteTimeoutMs == DebugOutbox.MaxWriteTimeoutMs,
            "debug write timeout clamps to maximum");
        DebugOutbox.WriteTimeoutMs = 150;

        using var sink = new BlockingWriteStream();
        DebugOutbox.ConnectStreamForTest = () => sink;
        try {
            // 先在队列里放一包；Start 之后发送线程写帧会阻塞在 fake 管道上。
            DebugOutbox.Enqueue(1, [10, 20]);
            Check(DebugOutbox.Copied == 1, "accepted debug packet is copied once");
            DebugOutbox.Start();
            Check(SpinWait.SpinUntil(() => DebugOutbox.Disabled, WaitMs),
                "blocked debug frame write times out and disables the debug channel");
            Check(!PacketOutbox.Disabled, "debug write timeout leaves the chat outbox enabled");
            Check(sink.Disposed.Wait(WaitMs), "timed-out debug pipe is disposed to unblock the write");
            Check(sink.WriteExited.Wait(WaitMs), "blocked debug frame write exits after disable");
            // M1：debug 写超时只进入 debug 自己的放弃任务计数，绝不占用聊天阈值。
            // Windows 上 Disable 可能立刻解除阻塞写，Task 随即完成并把当前计数减回 0；
            // 用历史峰值断言"这次超时确实被记为放弃任务"，对完成时序免疫（CI 抓到的竞态）。
            Check(Goshujin.PeakAbandonedDebugIoTaskCount >= 1,
                "timed-out debug write was recorded as a debug abandoned task (peak)");
            Check(Goshujin.AbandonedIoTaskCount == 0,
                "debug abandoned task does not consume the chat abandoned counter");
            Check(!Goshujin.AbandonedIoTaskLimitReached,
                "debug abandoned task cannot trip the chat terminal threshold");

            // Disable 之后：直接 Enqueue 与 all 模式钩子都不应该再复制载荷。
            var copiedBefore = DebugOutbox.Copied;
            var droppedBefore = DebugOutbox.Dropped;
            DebugOutbox.Enqueue(2, [30, 40]);
            Check(DebugOutbox.Copied == copiedBefore, "disabled debug outbox does not copy payloads");
            Check(DebugOutbox.Dropped == droppedBefore, "disabled debug outbox ignores new packets");
            ModeControl.SetAllPacketsForTest(true);
            try {
                InitializeHook();
                byte[] packet = [0x45, 0x67, 0, 1, 0, 0, 0, 0, 0, 2, 10, 20, 0x89, 0xAB];
                Check(InvokeHook(packet) == 1234, "all-mode hook stays pass-through after debug disable");
                Check(DebugOutbox.Copied == copiedBefore,
                    "all-mode hook does not copy payloads after debug disable");
            } finally {
                ModeControl.SetAllPacketsForTest(false);
            }
        } finally {
            DebugOutbox.ConnectStreamForTest = null;
            sink.Release.Set();
        }
    }

    private static Channel<PendingPacket> Queue =>
        (Channel<PendingPacket>) typeof(PacketOutbox).GetField("Queue", Flags)!.GetValue(null)!;

    private static unsafe void QueueChecks() {
        InitializeHook();
        byte[] packet = [0x45, 0x67, 0, 1, 0, 0, 0, 0, 0, 2, 10, 20, 0x89, 0xAB];
        Check(InvokeHook(packet, overwriteSource: true) == 1234, "hook preserves original return before sender starts");
        Check(Queue.Reader.TryRead(out var copied) && copied.Cmd == 1 &&
              copied.Data.SequenceEqual(new byte[] {10, 20}), "hook queues a copy independent of source memory");
        Check(!Queue.Reader.TryRead(out _), "hook enqueues exactly once without a pipe");

        // Exercise the real decompressor path; its native allocation is freed before we inspect the copy.
        void* tcp = null;
        void* shared = null;
        GameMethod.TcpStatePtr = &tcp;
        GameMethod.SharedInfoPtr = &shared;
        GameMethod.Decompress = (delegate* unmanaged<void*, void*, void*, uint, void*, uint, bool>)
            (delegate* unmanaged<void*, void*, void*, uint, void*, uint, byte>) &Decompress;
        byte[] compressed = [0x45, 0x67, 0, 1, 0, 2, 0, 0, 0, 2, 0x40, 3, 7, 8, 0x89, 0xAB];
        _decompressResult = true;
        Check(InvokeHook(compressed) == 1234 && Queue.Reader.TryRead(out var decompressed) &&
              decompressed.Data.SequenceEqual(new byte[] {41, 42, 43}), "decompressed payload survives native buffer free");
        _decompressResult = false;
        Check(InvokeHook(compressed) == 1234 && !Queue.Reader.TryRead(out _), "decompression failure queues nothing");
        _decompressResult = true;
        compressed[11] = 0x80;
        Check(InvokeHook(compressed) == 1234 && !Queue.Reader.TryRead(out _), "malformed header stays within callback");
        // M3：异常包可能每包都走通用 catch；OutputDebugStringW 在 DebugView 下是同步 IPC，
        // 必须限流（前 5 次 + 每 1024 次），不能在游戏网络线程无限刷。
        var forwardLogs = new List<string>();
        Log.OutputSinkForTest = forwardLogs.Add;
        try {
            for (var i = 0; i < 10; i++) {
                Check(InvokeHook(compressed) == 1234 && !Queue.Reader.TryRead(out _),
                    $"malformed packet {i} stays within callback");
            }
        } finally {
            Log.OutputSinkForTest = null;
        }
        Check(forwardLogs.Count(line => line.Contains("Failed to forward packet")) <= 5,
            "hook-path exception logging is throttled (M3)");
        compressed[11] = 3;
        Check(InvokeHook(compressed) == 1234 && Queue.Reader.TryRead(out _), "valid packet after malformed packet still queues");

        // P1-3：长度不可信时必须丢包，绝不进入无边界解析（否则越界读 AccessViolation 杀游戏）。
        // m6：测试工程定义 EnableLogging 后，这里验证真实 Log.Warn 调用点（不只是反射 WriteLog 本体）。
        var invalidLengthLogs = new List<string>();
        Log.OutputSinkForTest = invalidLengthLogs.Add;
        try {
            var invalidLengthBefore = Application.InvalidPacketLengthCount;
            Check(InvokeHook(packet, packetLengthOverride: 0) == 1234,
                "invalid native length returns the original value");
            Check(InvokeHook(packet, packetLengthOverride: 11) == 1234,
                "too-short native length returns the original value");
            Check(Application.InvalidPacketLengthCount == invalidLengthBefore + 2,
                "invalid native lengths are counted");
            Check(!Queue.Reader.TryRead(out _),
                "invalid-length packets are dropped without parsing or queueing");
        } finally {
            Log.OutputSinkForTest = null;
        }
        Check(invalidLengthLogs.Any(line => line.Contains("packetLength 异常")),
            "invalid length reaches the real EnableLogging callsite");

        var capacity = (int) typeof(PacketOutbox).GetField("Capacity", Flags)!.GetRawConstantValue()!;
        Check(capacity == 256, "production queue is bounded at 256 packets");
        for (var i = 0; i < capacity + 32; i++) PacketOutbox.Enqueue((ushort) i, [(byte) i]);
        for (var i = 0; i < capacity; i++)
            Check(Queue.Reader.TryRead(out var queued) && queued.Cmd == i && queued.Data[0] == (byte) i,
                $"full queue preserves earlier packet {i}");
        Check(!Queue.Reader.TryRead(out _), "full queue drops new packets rather than growing");
        // DropWrite 在满队列时"丢弃但返回 true"，所以必须靠计数补上（审计 F4）。
        Check(PacketOutbox.Dropped == 32, "queue overflow increments the dropped counter");

        // 钩子路径在队列满时也必须计数 Dropped：历史实现用 CanEnqueue 预检查后
        // 直接跳过 Enqueue，导致满队列丢弃漏计（复核意见）。这里用钩子重新填满再打一包。
        for (var i = 0; i < capacity; i++) InvokeHook(packet);
        var droppedBeforeHook = PacketOutbox.Dropped;
        Check(InvokeHook(packet) == 1234, "hook stays pass-through when the queue is full");
        Check(PacketOutbox.Dropped == droppedBeforeHook + 1,
            "hook packet dropped on a full queue is counted");
    }

    private static void SenderChecks(bool disconnect) {
        InitializeHook();
        using var sink = new MemoryStream();
        using var ack = new AckStream(block: false);
        SetPipe(sink, ack);
        if (disconnect) {
            sink.Dispose();
            // 断线后不允许测试触碰真实管道：重连一律走注入工厂（这里表示"宿主不在"）。
            Goshujin.ConnectStreamForTest = () => throw new IOException("regression: host absent");
            Goshujin.ReconnectInitialDelayMs = 10;
            Goshujin.ReconnectMaxDelayMs = 20;
        }
        PacketOutbox.Start();
        PacketOutbox.Start(); // Idempotence must not create a competing channel reader.
        byte[] packet = [0x45, 0x67, 0, 1, 0, 0, 0, 0, 0, 2, 10, 20, 0x89, 0xAB];
        Check(InvokeHook(packet) == 1234, "hook returns normally with asynchronous sender");
        if (disconnect) {
            // task-17：写 I/O 失败（已完成但抛异常）判定连接不可恢复 -> 立即重连，
            // 不再"连续 N 次后永久停用"。
            Check(SpinWait.SpinUntil(() => Goshujin.State == Goshujin.ConnectionState.Reconnecting, WaitMs),
                "write failure immediately puts the pipe into reconnecting state");
            Check(!PacketOutbox.Disabled, "single write failure no longer permanently disables the outbox");
            Check(InvokeHook(packet) == 1234, "disconnected hook retains original return");
            Check(ack.ReadCount == 0, "failed write never attempts ACK read");
            PacketOutbox.Disable("regression cleanup");
            Check(PacketOutbox.Disabled, "explicit disable still latches");
            Check(!Goshujin.RequestReconnect("after disable"), "terminal state rejects further reconnect");
            return;
        }
        Check(SpinWait.SpinUntil(() => ack.ReadCount == 1, WaitMs), "background writer consumes false ACK");
        byte[] frame = [4, 1, 0, 2, 0, 0, 0, 10, 20];
        Check(sink.ToArray().SequenceEqual(frame), "background sender writes correct pipe framing");
        Check(!PacketOutbox.Disabled, "false ACK preserves subscription transport");
        Check(InvokeHook(packet) == 1234, "second callback returns normally");
        Check(SpinWait.SpinUntil(() => ack.ReadCount == 2, WaitMs), "second frame consumes exactly one ACK");
        Check(sink.ToArray().SequenceEqual(frame.Concat(frame)), "successive frames remain aligned");
        PacketOutbox.Disable("regression cleanup");
    }

    private static void TimeoutChecks() {
        // 超时可配置且有上下界；默认值从旧的固定 500ms 提到 1500ms。
        Check(PacketOutbox.DefaultSendTimeoutMs == 1500, "default chat send deadline is 1500ms");
        Check(PacketOutbox.MinSendTimeoutMs > 0, "send deadline has a positive lower bound");
        Check(PacketOutbox.MaxSendTimeoutMs >= PacketOutbox.MinSendTimeoutMs,
            "send deadline upper bound is sane");
        PacketOutbox.SendTimeoutMs = 0;
        Check(PacketOutbox.SendTimeoutMs == PacketOutbox.MinSendTimeoutMs, "timeout 0 clamps to minimum");
        PacketOutbox.SendTimeoutMs = -1;
        Check(PacketOutbox.SendTimeoutMs == PacketOutbox.MinSendTimeoutMs, "negative timeout clamps to minimum");
        PacketOutbox.SendTimeoutMs = int.MaxValue;
        Check(PacketOutbox.SendTimeoutMs == PacketOutbox.MaxSendTimeoutMs, "huge timeout clamps to maximum");

        using var ack = new AckStream(block: true);
        SetPipe(Stream.Null, ack);
        Goshujin.ConnectStreamForTest = () => throw new IOException("regression: host absent");
        Goshujin.ReconnectInitialDelayMs = 10;
        Goshujin.ReconnectMaxDelayMs = 20;
        PacketOutbox.SendTimeoutMs = 150;
        try {
            PacketOutbox.Start();
            var elapsed = Stopwatch.StartNew();
            PacketOutbox.Enqueue(1, [10, 20]);
            Check(ack.Entered.Wait(WaitMs), "fake host receives frame and withholds ACK");
            Check(SpinWait.SpinUntil(() => Goshujin.State == Goshujin.ConnectionState.Reconnecting, WaitMs),
                "hung send (write/read still running) abandons the connection and starts reconnecting");
            Check(elapsed.ElapsedMilliseconds >= PacketOutbox.SendTimeoutMs,
                "hung send uses the configured deadline rather than failing immediately");
            Check(!PacketOutbox.Disabled, "hung send no longer permanently disables chat forwarding");
            Check(PacketOutbox.TimeoutCount == 1, "hung send is counted as a timeout");
            Check(ack.ReadCount == 0, "ACK was never consumed before the connection was abandoned");
            Check(Goshujin.AbandonedIoTaskCount == 1, "at most one send task is abandoned per connection");
        } finally {
            ack.Release.Set();
            Check(SpinWait.SpinUntil(() => ack.ReadCount > 0, WaitMs), "blocked fake read exits during cleanup");
            Check(SpinWait.SpinUntil(() => Goshujin.AbandonedIoTaskCount == 0, WaitMs),
                "abandoned send task exits after its blocked IO is released");
            PacketOutbox.Disable("regression cleanup");
        }
    }

    /// <summary>
    /// 写/读卡死（Wait 超时后 Task 仍未完成）的核心回归：
    /// - 必须立即废弃旧连接（dispose 旧流）并进入 Reconnecting，而不是"丢包继续"；
    /// - 旧连接上最多遗留 1 个被放弃的任务，重连期间不再产生新的发送任务（不会累积持锁任务）；
    /// - 宿主回来后重连成功，排队中的包在新连接上继续转发。
    /// </summary>
    private static void SendStuckReconnectChecks() {
        using var oldReader = new ScriptedAckStream(); // 第一次 ACK 读永久阻塞
        using var oldPipe = new DisposeTrackingStream(); // 模拟旧主管道句柄，dispose 可观测
        SetPipe(Stream.Null, oldReader);
        SetPipeField("_pipeStream", oldPipe);
        SetPipeField("_pipeWriter", new BinaryWriter(Stream.Null));
        Goshujin.ReconnectInitialDelayMs = 10;
        Goshujin.ReconnectMaxDelayMs = 20;
        Goshujin.ConnectStreamForTest = () => throw new IOException("regression: host absent");
        PacketOutbox.SendTimeoutMs = 150;
        MarkMethodTableResolvedForTest();
        try {
            PacketOutbox.Start();
            PacketOutbox.Enqueue(1, [10, 20]);
            PacketOutbox.Enqueue(2, [30, 40]);
            Check(oldReader.Entered.Wait(WaitMs), "sender is blocked in the ACK read");
            Check(SpinWait.SpinUntil(() => Goshujin.State == Goshujin.ConnectionState.Reconnecting, WaitMs),
                "timed-out send with live IO abandons the connection");
            Check(oldPipe.Disposed, "the stuck pipe is disposed so the blocked IO can abort");
            Check(Goshujin.AbandonedIoTaskCount == 1, "exactly one send task is abandoned per connection");
            Check(Queue.Reader.Count == 1, "no new send task starts while reconnecting (second packet stays queued)");
            Check(!PacketOutbox.Disabled, "reconnecting does not permanently disable forwarding");

            var host = new FakeHostStream(HandshakeBytes(new uint[] { 2246 }, DefaultRvas));
            Goshujin.ConnectStreamForTest = () => host;
            Check(SpinWait.SpinUntil(() => Goshujin.State == Goshujin.ConnectionState.Connected, WaitMs),
                "reconnect succeeds once the host returns");
            Check(Goshujin.LastConnectedTicks > 0, "last connected time is recorded");
            var expected = new byte[] { 4, 2, 0, 2, 0, 0, 0, 30, 40 };
            Check(SpinWait.SpinUntil(() => EndsWith(host.WrittenSnapshot(), expected), WaitMs),
                "queued packet is forwarded on the new connection");
            Check(PacketOutbox.TimeoutCount >= 1, "hung send was counted as a timeout");
        } finally {
            oldReader.Release.Set();
            Check(SpinWait.SpinUntil(() => Goshujin.AbandonedIoTaskCount == 0, WaitMs),
                "abandoned send task exits once its blocked IO is released");
        }
    }

    /// <summary>
    /// 写阻塞（管道缓冲满、宿主不读）——线上"宿主被 Hyper-V 暂停"的直接复现：
    /// 超时后 Task 仍卡在 Write 里并持有旧连接锁，必须 dispose 旧管道让写退出、进入重连，
    /// 且重连期间不再启动新的发送任务（被放弃任务数回落，不累积）。
    /// </summary>
    private static void SendWriteBlockReconnectChecks() {
        using var blocked = new BlockingWriteStream();
        using var ack = new AckStream(block: false);
        SetPipe(Stream.Null, ack);
        // 让写和"管道句柄"是同一个对象：RequestReconnect dispose 时会解除写阻塞（生产同理）。
        SetPipeField("_pipeStream", blocked);
        SetPipeField("_pipeWriter", new BinaryWriter(blocked));
        MarkMethodTableResolvedForTest();
        Goshujin.ConnectStreamForTest = () => throw new IOException("regression: host absent");
        Goshujin.ReconnectInitialDelayMs = 10;
        Goshujin.ReconnectMaxDelayMs = 20;
        PacketOutbox.SendTimeoutMs = 150;
        try {
            PacketOutbox.Start();
            PacketOutbox.Enqueue(1, [10, 20]);
            PacketOutbox.Enqueue(2, [30, 40]);
            Check(blocked.Entered.Wait(WaitMs), "sender is blocked inside the pipe write");
            Check(SpinWait.SpinUntil(() => Goshujin.State == Goshujin.ConnectionState.Reconnecting, WaitMs),
                "blocked write times out, abandons the connection and starts reconnecting");
            Check(blocked.Disposed.Wait(WaitMs), "the blocked pipe is disposed so the write can exit");
            Check(blocked.WriteExited.Wait(WaitMs), "the blocked write actually exits after dispose");
            Check(SpinWait.SpinUntil(() => Goshujin.AbandonedIoTaskCount == 0, WaitMs),
                "the abandoned send task exits and releases the old lock");
            Check(Queue.Reader.Count == 1, "no new send task starts while reconnecting");
            Check(!PacketOutbox.Disabled, "blocked write does not permanently disable forwarding");
        } finally {
            PacketOutbox.Disable("regression cleanup");
        }
    }

    /// <summary>宿主一直不在时：退避重试但不风暴；宿主回来后自动恢复。</summary>
    private static void ReconnectBackoffChecks() {
        SetPipe(Stream.Null, new AckStream(block: false));
        Goshujin.ReconnectInitialDelayMs = 10;
        Goshujin.ReconnectMaxDelayMs = 40;
        Goshujin.ConnectStreamForTest = () => throw new IOException("regression: host absent");
        MarkMethodTableResolvedForTest();
        Check(Goshujin.RequestReconnect("regression"), "reconnect request is accepted while connected");
        Check(SpinWait.SpinUntil(() => Goshujin.State == Goshujin.ConnectionState.Reconnecting, WaitMs),
            "state transitions to reconnecting");
        Check(SpinWait.SpinUntil(() => Goshujin.ReconnectAttempts >= 3, WaitMs),
            "retry loop keeps trying while the host is gone");
        var before = Goshujin.ReconnectAttempts;
        Thread.Sleep(250);
        var delta = Goshujin.ReconnectAttempts - before;
        Check(delta >= 1, "retry loop is still running after the initial attempts");
        Check(delta <= 20, "capped backoff prevents a reconnect storm");

        var host = new FakeHostStream(HandshakeBytes(new uint[] { 2246 }, DefaultRvas));
        Goshujin.ConnectStreamForTest = () => host;
        Check(SpinWait.SpinUntil(() => Goshujin.State == Goshujin.ConnectionState.Connected, WaitMs),
            "reconnect succeeds once the host returns");
        Check(Goshujin.LastConnectedTicks > 0, "last connected time is recorded");
        Check(Goshujin.HandshakeFailures == 0, "handshake failure counter resets on success");
    }

    /// <summary>
    /// 握手幂等：重复 LoadMethodTable 绝不覆盖已解析的函数指针；RequiredPackets 整体替换。
    /// </summary>
    private static unsafe void ReconnectHandshakeChecks() {
        SetPipe(Stream.Null, new AckStream(block: false));
        Goshujin.ReconnectInitialDelayMs = 10;
        Goshujin.ReconnectMaxDelayMs = 20;
        MarkMethodTableResolvedForTest();
        Application.RequiredPackets.Clear();
        Application.RequiredPackets.Add(999);

        // 宿主返回的 RVA 是明显不同的噪声：若被应用会覆盖上面的哨兵。
        var noisyRvas = new uint[] { 0xDEAD, 0xBEEF, 0xDEAD, 0xBEEF, 0xDEAD, 0xBEEF, 0xDEAD, 0xBEEF, 0xDEAD, 0xBEEF };
        var firstHost = new FakeHostStream(HandshakeBytes(new uint[] { 2246, 486 }, noisyRvas));
        var secondHost = new FakeHostStream(HandshakeBytes(new uint[] { 2246 }, noisyRvas));
        var thirdHost = new FakeHostStream(HandshakeBytes(new uint[] { 2246 }, noisyRvas));
        var calls = 0;
        Goshujin.ConnectStreamForTest = () => {
            var n = Interlocked.Increment(ref calls);
            return n == 1 ? firstHost : n == 2 ? secondHost : thirdHost;
        };

        Check(Goshujin.RequestReconnect("regression"), "reconnect request is accepted");
        Check(SpinWait.SpinUntil(() => Goshujin.State == Goshujin.ConnectionState.Connected, WaitMs),
            "first reconnect handshake succeeds");
        Check(Application.RequiredPackets.SetEquals(new uint[] { 2246, 486 }),
            "RequiredPackets is rebuilt from the handshake list");
        Check(!Application.RequiredPackets.Contains(999), "stale required cmd is removed");
        // M2 复核：逐字段幂等，而不是"任一为 null 就整表覆盖"。
        Check((nint) GameMethod.ToInt32 == MethodTableSentinel,
            "reconnect never overwrites resolved ToInt32 pointer");
        Check((nint) GameMethod.Decompress == MethodTableSentinel,
            "reconnect never overwrites the decompressor pointer");
        Check(GameMethod.TcpStatePtr == (void**) MethodTableSentinel &&
              GameMethod.SharedInfoPtr == (void**) MethodTableSentinel,
            "reconnect never overwrites global state pointers");
        Check((nint) GameMethod.UpdateNormalProp == MethodTableSentinel &&
              (nint) GameMethod.NewString == MethodTableSentinel &&
              (nint) GameMethod.FindGameObject == MethodTableSentinel &&
              (nint) GameMethod.EventSystemUpdate == MethodTableSentinel &&
              (nint) GameMethod.SimulatePointerClick == MethodTableSentinel,
            "per-field idempotent apply leaves every resolved pointer untouched");

        Check(Goshujin.RequestReconnect("regression 2"), "second reconnect request is accepted");
        Check(SpinWait.SpinUntil(() => Volatile.Read(ref calls) >= 2 &&
                                      Goshujin.State == Goshujin.ConnectionState.Connected, WaitMs),
            "second reconnect handshake succeeds");
        Check(Application.RequiredPackets.SetEquals(new uint[] { 2246 }),
            "RequiredPackets is replaced on each handshake, never appended");
        Check((nint) GameMethod.ToInt32 == MethodTableSentinel,
            "repeated handshakes stay idempotent (pointers untouched)");

        // M2 复核：逐字段补齐。只把仍为 null 的指针补上，绝不整表覆盖。
        GameMethod.NewString = null;
        Goshujin.RvaResolverForTest = rva => (nint) (0xA000u + rva);
        try {
            Check(Goshujin.RequestReconnect("regression 3"), "third reconnect request is accepted");
            Check(SpinWait.SpinUntil(() => Volatile.Read(ref calls) >= 3 &&
                                          Goshujin.State == Goshujin.ConnectionState.Connected, WaitMs),
                "third reconnect handshake succeeds");
            Check((nint) GameMethod.NewString == (nint) (0xA000u + noisyRvas[2]),
                "only the missing method pointer is filled on reconnect");
            Check((nint) GameMethod.ToInt32 == MethodTableSentinel &&
                  (nint) GameMethod.FindGameObject == MethodTableSentinel,
                "other resolved pointers stay untouched (per-field apply, not all-or-nothing)");
        } finally {
            Goshujin.RvaResolverForTest = null;
        }
    }

    /// <summary>
    /// B1 回归：控制管道重连必须对主通道做陈旧判定。
    /// 新鲜主通道（<10s）连续控制重连不触发主通道回收；陈旧主通道最多触发一次，
    /// 避免 reattach 竞速下每轮把刚恢复的主通道再关掉、数秒耗尽 3 轮窗口。
    /// </summary>
    private static void ControlStaleChecks() {
        using var ack = new AckStream(block: false);
        SetPipe(Stream.Null, ack);
        Goshujin.ConnectStreamForTest = () => throw new IOException("regression: host absent");
        var triggersBefore = Goshujin.ControlReconnectTriggers;

        // 1) 新鲜主通道：连续 3 次控制重连都不触发（修复前每轮都会触发）。
        for (var i = 0; i < 3; i++) {
            Goshujin.SetLastConnectedTicksForTest(Environment.TickCount64);
            Goshujin.OnControlPipeReconnected();
            Check(Goshujin.State == Goshujin.ConnectionState.Connected,
                $"fresh main connection stays connected after control reconnect {i + 1}");
        }
        Check(Goshujin.ControlReconnectTriggers == triggersBefore,
            "fresh main connection never triggers a control-driven main reconnect (B1)");

        // 2) 陈旧主通道：只触发一次；后续控制重连在 Reconnecting 状态不再重复触发。
        Goshujin.SetLastConnectedTicksForTest(
            Environment.TickCount64 - Goshujin.ControlReconnectStalenessMs - 1_000);
        Goshujin.OnControlPipeReconnected();
        Check(Goshujin.ControlReconnectTriggers == triggersBefore + 1,
            "stale main connection triggers exactly one main reconnect");
        Check(Goshujin.State == Goshujin.ConnectionState.Reconnecting,
            "stale-control trigger puts the main pipe into reconnecting");
        for (var i = 0; i < 3; i++) {
            Goshujin.OnControlPipeReconnected();
        }
        Check(Goshujin.ControlReconnectTriggers == triggersBefore + 1,
            "further control reconnects while reconnecting do not retrigger (at most one)");

        // 3) 模拟宿主下一轮：主通道已快速重连成功（新鲜）。后续控制重连不得再触发，
        //    这正是 B1 自噬链条（关掉刚恢复的主通道 -> 宿主下一轮）的断点。
        Goshujin.MarkConnectedForTest();
        for (var i = 0; i < 3; i++) {
            Goshujin.OnControlPipeReconnected();
        }
        Check(Goshujin.ControlReconnectTriggers == triggersBefore + 1,
            "after a fresh main reconnect, later control rounds do not trigger again (B1)");
        PacketOutbox.Disable("regression cleanup");
    }

    /// <summary>
    /// P1-2/M1 回归：聊天 send+handshake 共用一个放弃任务计数并触发终态；debug 任务只
    /// 观测、绝不参与聊天阈值（debug 失败不能停掉聊天）。
    /// </summary>
    private static void AbandonedLimitChecks() {
        Check(Goshujin.MaxAbandonedIoTaskCount >= 4 && Goshujin.MaxAbandonedIoTaskCount <= 8,
            "abandoned IO cap is within the recommended 4-8 range");
        Check(Goshujin.AbandonedIoTaskCount == 0, "chat abandoned IO counter starts at zero");

        // M1：debug 任务再多也只计入自己的计数，不能触发聊天终态。
        // 峰值语义对时序免疫：即使 Task 立刻完成、当前计数马上回落，峰值仍能证明它被记过。
        var debugPeakBefore = Goshujin.PeakAbandonedDebugIoTaskCount;
        Goshujin.TrackDebugAbandonedIoTask(Task.CompletedTask);
        Check(Goshujin.PeakAbandonedDebugIoTaskCount > debugPeakBefore,
            "peak captures an already-completed abandoned debug task");
        Check(SpinWait.SpinUntil(() => Goshujin.AbandonedDebugIoTaskCount == 0, WaitMs),
            "completed debug task releases the current abandoned slot");

        var debugBlockers = new List<TaskCompletionSource<object?>>();
        for (var i = 0; i < Goshujin.MaxAbandonedIoTaskCount + 2; i++) {
            var blocker = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);
            debugBlockers.Add(blocker);
            Goshujin.TrackDebugAbandonedIoTask(blocker.Task);
        }
        Check(Goshujin.PeakAbandonedDebugIoTaskCount >= Goshujin.MaxAbandonedIoTaskCount + 2,
            "debug abandoned tasks are recorded separately (peak)");
        Check(Goshujin.AbandonedDebugIoTaskCount == Goshujin.MaxAbandonedIoTaskCount + 2,
            "debug blockers stay incomplete so the current count is stable");
        Check(Goshujin.AbandonedIoTaskCount == 0,
            "debug abandoned tasks do not consume the chat threshold");
        Check(!Goshujin.AbandonedIoTaskLimitReached,
            "debug failures cannot trip the chat terminal threshold");
        foreach (var blocker in debugBlockers) {
            blocker.SetResult(null);
        }

        var tcs = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);
        Goshujin.TrackAbandonedIoTask(tcs.Task);
        Check(Goshujin.AbandonedIoTaskCount == 1, "send/handshake tasks share the chat abandoned counter");
        tcs.SetResult(null);
        Check(SpinWait.SpinUntil(() => Goshujin.AbandonedIoTaskCount == 0, WaitMs),
            "completed abandoned task releases its slot");

        var blockers = new List<TaskCompletionSource<object?>>();
        for (var i = 0; i < Goshujin.MaxAbandonedIoTaskCount; i++) {
            var blocker = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);
            blockers.Add(blocker);
            Goshujin.TrackAbandonedIoTask(blocker.Task);
        }
        Check(Goshujin.AbandonedIoTaskCount == Goshujin.MaxAbandonedIoTaskCount,
            "abandoned IO count reaches the cap");
        Check(Goshujin.AbandonedIoTaskLimitReached, "cap flag flips at the threshold");

        using var ack = new AckStream(block: false);
        SetPipe(Stream.Null, ack);
        Check(!Goshujin.RequestReconnect("regression threshold"),
            "reconnect is refused once the abandoned IO cap is reached");
        Check(Goshujin.State == Goshujin.ConnectionState.Disabled,
            "abandoned IO cap enters a clear terminal state");
        Check(!Goshujin.RequestReconnect("after terminal"),
            "terminal state keeps rejecting reconnect");
        foreach (var blocker in blockers) {
            blocker.SetResult(null);
        }
    }

    /// <summary>
    /// M3 回归：控制管道断开后必须退避重连（而不是永久失联），
    /// 宿主重建管道后重新应用模式；断开期间回落 chat，避免与宿主
    /// /debug/packets 的 dllControl/mode 静默不一致。
    /// </summary>
    private static void ModeReconnectChecks() {
        Check(ModeControl.DefaultReconnectInitialDelayMs == 1000, "default control reconnect delay is 1000ms");
        Check(ModeControl.DefaultReconnectMaxDelayMs == 30000, "default control reconnect cap is 30000ms");
        ModeControl.ReconnectInitialDelayMs = 10;
        ModeControl.ReconnectMaxDelayMs = 40;
        // 主通道以"已连接"状态注入，用于验证控制管道重连会主动触发主通道回收（P1-4）。
        using var ack = new AckStream(block: false);
        SetPipe(Stream.Null, ack);
        Goshujin.ReconnectInitialDelayMs = 10;
        Goshujin.ReconnectMaxDelayMs = 20;
        Goshujin.ConnectStreamForTest = () => throw new IOException("regression: host absent");
        // B1：主通道视为"陈旧"（>10s）才允许控制重连触发主通道回收。
        Goshujin.SetLastConnectedTicksForTest(
            Environment.TickCount64 - Goshujin.ControlReconnectStalenessMs - 1_000);
        var triggersBefore = Goshujin.ControlReconnectTriggers;
        var first = new ModePipeStream([0], blockAfterBytes: true);       // chat，随后 Dispose 模拟宿主断开
        var second = new ModePipeStream([1], blockAfterBytes: true);      // all，然后阻塞（便于观测）
        var calls = 0;
        ModeControl.ConnectStreamForTest = () =>
            Interlocked.Increment(ref calls) == 1 ? first : second;
        try {
            Check(!ModeControl.AllPackets, "mode control starts in chat mode");
            ModeControl.Start();
            Check(SpinWait.SpinUntil(() => ModeControl.ConnectCount >= 1, WaitMs), "control pipe connects");
            // m5：ConnectCount 会先于状态更新；等一个稳定的观察窗，避免"先读 State"的竞态假绿。
            Thread.Sleep(100);
            Check(!ModeControl.AllPackets, "first control pipe applies chat mode");
            Check(Goshujin.State == Goshujin.ConnectionState.Connected,
                "initial control connect does not trigger a main pipe reconnect");
            Check(Goshujin.ControlReconnectTriggers == triggersBefore,
                "initial control connect leaves the stale-control trigger counter at zero");

            first.Dispose(); // EOF：控制管道断开并重连
            Check(SpinWait.SpinUntil(() => ModeControl.ConnectCount >= 2, WaitMs),
                "control pipe is re-established after peer close");
            Check(SpinWait.SpinUntil(() => ModeControl.AllPackets, WaitMs),
                "mode from the reconnected control pipe is applied");
            Check(ModeControl.ReconnectAttempts >= 2, "control reconnect attempts are counted");
            Check(SpinWait.SpinUntil(() => Goshujin.State == Goshujin.ConnectionState.Reconnecting, WaitMs),
                "control reconnect triggers a main pipe re-probe");
            Check(Goshujin.ControlReconnectTriggers == triggersBefore + 1,
                "stale main connection triggers exactly one control-driven re-probe");
            Check(SpinWait.SpinUntil(() => Goshujin.ReconnectAttempts >= 1, WaitMs),
                "main pipe reconnect attempts are counted after the control trigger");
        } finally {
            ModeControl.ConnectStreamForTest = null;
            ModeControl.SetAllPacketsForTest(false);
            Goshujin.ConnectStreamForTest = null;
            PacketOutbox.Disable("regression cleanup");
        }
    }

    /// <summary>重连期间钩子必须保持直通；显式 Disable 才进入终态。</summary>
    private static unsafe void ReconnectPassthroughChecks() {
        SetPipe(Stream.Null, new AckStream(block: false));
        Goshujin.ReconnectInitialDelayMs = 10;
        Goshujin.ReconnectMaxDelayMs = 20;
        Goshujin.ConnectStreamForTest = () => throw new IOException("regression: host absent");
        MarkMethodTableResolvedForTest();
        InitializeHook();
        Check(Goshujin.RequestReconnect("regression"), "reconnect request is accepted");
        Check(SpinWait.SpinUntil(() => Goshujin.State == Goshujin.ConnectionState.Reconnecting, WaitMs),
            "state transitions to reconnecting");
        byte[] packet = [0x45, 0x67, 0, 1, 0, 0, 0, 0, 0, 2, 10, 20, 0x89, 0xAB];
        Check(InvokeHook(packet) == 1234, "hook stays pass-through while reconnecting");
        ModeControl.SetAllPacketsForTest(true);
        try {
            Check(InvokeHook(packet) == 1234, "all-mode hook stays pass-through while reconnecting");
        } finally {
            ModeControl.SetAllPacketsForTest(false);
        }
        Check(!PacketOutbox.Disabled, "reconnecting does not permanently disable forwarding");
        PacketOutbox.Disable("regression cleanup");
        Check(Goshujin.State == Goshujin.ConnectionState.Disabled, "explicit disable enters terminal state");
        Check(!Goshujin.RequestReconnect("after disable"), "terminal state rejects further reconnect");
    }

    /// <summary>
    /// 握手失败在时间窗内+次数达标后进入终态；发送线程据此永久停用（不再反复重连）。
    /// 时间窗用测试值 50ms 缩短（生产默认 5 分钟，覆盖宿主 reattach 3×60s，见 P2-1）。
    /// </summary>
    private static unsafe void ReconnectExhaustChecks() {
        SetPipe(Stream.Null, new AckStream(block: false));
        Goshujin.ReconnectInitialDelayMs = 10;
        Goshujin.ReconnectMaxDelayMs = 20;
        Goshujin.HandshakeFailureWindowMs = 50;
        // 空流：握手读立即 EndOfStream，模拟"连上了但协议永远谈不拢"。
        Goshujin.ConnectStreamForTest = () => new FakeHostStream();
        InitializeHook();
        PacketOutbox.Start();
        Check(Goshujin.RequestReconnect("regression"), "reconnect request is accepted");
        Check(SpinWait.SpinUntil(() => Goshujin.State == Goshujin.ConnectionState.Disabled, WaitMs),
            "handshake failures within the time window enter the terminal disabled state");
        Check(Goshujin.HandshakeFailures >= 3, "terminal decision sees at least three handshake failures");
        Check(!Goshujin.RequestReconnect("after terminal"), "terminal state rejects reconnect");
        byte[] packet = [0x45, 0x67, 0, 1, 0, 0, 0, 0, 0, 2, 10, 20, 0x89, 0xAB];
        Check(InvokeHook(packet) == 1234, "hook stays pass-through after terminal reconnect failure");
        Check(SpinWait.SpinUntil(() => PacketOutbox.Disabled, WaitMs),
            "sender observes the terminal state and disables chat forwarding");
    }

    /// <summary>
    /// P2-1 回归：3 次快速握手失败不再立刻终态；只有累计时间超过窗口才终态。
    /// </summary>
    private static void HandshakeWindowChecks() {
        Check(Goshujin.DefaultHandshakeFailureWindowMs >= 180_000,
            "default handshake failure window covers the host 60s x 3 reattach window");
        Check(Goshujin.MaxHandshakeFailures >= 10, "absolute handshake failure cap is generous");
        SetPipe(Stream.Null, new AckStream(block: false));
        Goshujin.ReconnectInitialDelayMs = 10;
        Goshujin.ReconnectMaxDelayMs = 20;
        Goshujin.HandshakeFailureWindowMs = 5_000; // 先给足够长的窗口
        Goshujin.ConnectStreamForTest = () => new FakeHostStream(); // 空流：快速协议失败
        Check(Goshujin.RequestReconnect("regression"), "reconnect request is accepted");
        Check(SpinWait.SpinUntil(() => Goshujin.HandshakeFailures >= 3, WaitMs),
            "handshake failures keep accumulating inside the window");
        Check(Goshujin.State != Goshujin.ConnectionState.Disabled,
            "three fast failures alone do not terminate inside the time window");
        Goshujin.HandshakeFailureWindowMs = Goshujin.MinHandshakeFailureWindowMs;
        Check(SpinWait.SpinUntil(() => Goshujin.State == Goshujin.ConnectionState.Disabled, WaitMs),
            "terminal decision happens once the time window elapsed");
    }

    /// <summary>
    /// M2 回归：握手时间窗必须是"连续失败"窗口。宿主长时间不在（Connect 失败）后，
    /// 旧时间戳不能用于第一次恢复就立即终态。
    /// </summary>
    private static void HandshakeGapChecks() {
        SetPipe(Stream.Null, new AckStream(block: false));
        Goshujin.ReconnectInitialDelayMs = 10;
        Goshujin.ReconnectMaxDelayMs = 20;
        Goshujin.HandshakeFailureWindowMs = 5_000;
        var hostAbsent = 0;
        var connectCalls = 0;
        Goshujin.ConnectStreamForTest = () => {
            Interlocked.Increment(ref connectCalls);
            if (Volatile.Read(ref hostAbsent) != 0) {
                throw new IOException("regression: host absent");
            }
            return new FakeHostStream(); // 空流：握手立即失败
        };
        Check(Goshujin.RequestReconnect("regression"), "reconnect request is accepted");
        Check(SpinWait.SpinUntil(() => Goshujin.HandshakeFailures >= 2, WaitMs),
            "two handshake failures recorded before the host disappears");
        Volatile.Write(ref hostAbsent, 1);
        var callsAtAbsent = Volatile.Read(ref connectCalls);
        Check(SpinWait.SpinUntil(() => Volatile.Read(ref connectCalls) >= callsAtAbsent + 2, WaitMs),
            "connect failures observed while the host is absent");
        // 模拟旧窗口时间戳：这些握手失败已经过去很久。
        Goshujin.SetHandshakeFailureSinceTicksForTest(Environment.TickCount64 - 600_000);
        // 再让一次 Connect 失败：真实实现应在 Connect 失败时重置窗口（旧实现不会）。
        Check(SpinWait.SpinUntil(() => Volatile.Read(ref connectCalls) >= callsAtAbsent + 3, WaitMs),
            "another connect failure resets the handshake window");
        Volatile.Write(ref hostAbsent, 0);
        Check(SpinWait.SpinUntil(() => Goshujin.HandshakeFailures >= 3, WaitMs),
            "fresh handshake failures accumulate after the host returns");
        Check(Goshujin.State != Goshujin.ConnectionState.Disabled,
            "long connect-failure gap does not terminal on a stale timestamp (M2)");
    }

    /// <summary>
    /// P2-4 回归：DebugOutbox 终态 Disable 后可在宿主 reattach（控制管道重连）或
    /// 控制模式 chat→all 时恢复；恢复后使用新队列/线程，帧编码不变。
    /// </summary>
    private static void DebugRecoveryChecks() {
        DebugOutbox.ConnectStreamForTest = () => throw new IOException("regression: no debug host");
        DebugOutbox.Start();
        Check(SpinWait.SpinUntil(() => DebugOutbox.Disabled, WaitMs),
            "debug disables when its pipe is missing");
        Check(SpinWait.SpinUntil(() => !DebugOutbox.SenderRunning, WaitMs),
            "dead debug sender exits before recovery");

        // 触发 1：控制管道重连（模拟宿主 reattach；Goshujin 默认 Disconnected，只走 debug 恢复）。
        var sink = new MemoryStream();
        DebugOutbox.ConnectStreamForTest = () => sink;
        Goshujin.OnControlPipeReconnected();
        Check(SpinWait.SpinUntil(() => !DebugOutbox.Disabled, WaitMs),
            "control pipe reattach restarts the debug channel");
        DebugOutbox.Enqueue(1, [10, 20]);
        Check(SpinWait.SpinUntil(() => sink.Length >= 9, WaitMs),
            "recovered debug sender writes frames");
        Check(sink.ToArray().Take(9).SequenceEqual(new byte[] { 4, 1, 0, 2, 0, 0, 0, 10, 20 }),
            "recovered debug frame keeps the normal 0x04 encoding");
        Check(DebugOutbox.RecoveryAttempts == 0,
            "successful debug recovery resets the consecutive attempt counter");

        // 触发 2：控制模式 chat→all。
        DebugOutbox.Disable("regression 2");
        Check(SpinWait.SpinUntil(() => !DebugOutbox.SenderRunning, WaitMs),
            "disabled debug sender exits before the mode-triggered recovery");
        var sink2 = new MemoryStream();
        DebugOutbox.ConnectStreamForTest = () => sink2;
        ModeControl.SetAllPacketsForTest(false);
        ModeControl.ApplyMode(1);
        Check(SpinWait.SpinUntil(() => !DebugOutbox.Disabled, WaitMs),
            "mode chat->all restarts the debug channel");
        DebugOutbox.Enqueue(2, [30, 40]);
        Check(SpinWait.SpinUntil(() => sink2.Length >= 9, WaitMs),
            "recovered debug sender writes after the mode trigger");
        Check(DebugOutbox.RecoveryAttempts == 0,
            "second successful recovery resets the attempt counter");
        ModeControl.SetAllPacketsForTest(false);
        DebugOutbox.ConnectStreamForTest = null;
    }

    /// <summary>
    /// P2-3 回归：Awake 失败收尾必须关闭主通道（宿主看到 EOF，而不是半活 DLL）并停用转发。
    /// </summary>
    private static void AbortInitializationChecks() {
        using var tracking = new DisposeTrackingStream();
        using var ack = new AckStream(block: false);
        SetPipe(Stream.Null, ack);
        SetPipeField("_pipeStream", tracking);
        var logs = new List<string>();
        Log.OutputSinkForTest = logs.Add;
        try {
            Application.AbortInitialization("regression abort");
        } finally {
            Log.OutputSinkForTest = null;
        }
        Check(logs.Any(line => line.Contains("Native initialization failed: regression abort")),
            "abort initialization reaches the real EnableLogging callsite");
        Check(PacketOutbox.Disabled, "abort initialization disables forwarding");
        Check(Goshujin.State == Goshujin.ConnectionState.Disabled,
            "abort initialization enters the terminal pipe state");
        Check(tracking.Disposed, "abort initialization disposes the main pipe so the host sees EOF");
        Check(!Goshujin.RequestReconnect("after abort"), "aborted initialization refuses reconnect");
    }

    /// <summary>
    /// 把方法表标记为"已解析"（哨兵值）。重连握手据此走幂等分支、不触碰 Native.RVAToVA，
    /// 从而让这组回归在非 Windows 开发机上也能跑（测试不调用任何 Windows API）。
    /// </summary>
    private static unsafe void MarkMethodTableResolvedForTest() {
        GameMethod.UpdateNormalProp = (delegate* unmanaged<nint, int, double, double, int, void>) MethodTableSentinel;
        GameMethod.NewString = (delegate* unmanaged<nint, nint>) MethodTableSentinel;
        GameMethod.FindGameObject = (delegate* unmanaged<nint, nint>) MethodTableSentinel;
        GameMethod.EventSystemUpdate = (delegate* unmanaged<nint, void>) MethodTableSentinel;
        GameMethod.SimulatePointerClick = (delegate* unmanaged<nint, nint, bool>) MethodTableSentinel;
        GameMethod.ToInt32 = (delegate* unmanaged<byte*, int, int>) MethodTableSentinel;
        GameMethod.TcpStatePtr = (void**) MethodTableSentinel;
        GameMethod.SharedInfoPtr = (void**) MethodTableSentinel;
        GameMethod.Decompress = (delegate* unmanaged<void*, void*, void*, uint, void*, uint, bool>) MethodTableSentinel;
    }

    private static unsafe void InitializeHook() {
        typeof(Application).GetField("_toInt32", Flags)!.SetValue(null,
            (nint) (delegate* unmanaged<byte*, int, int>) &Original);
        Application.RequiredPackets.Add(1);
    }

    private static unsafe int InvokeHook(byte[] packet, bool overwriteSource = false, nuint? packetLengthOverride = null) {
        var array = new byte[0x20 + packet.Length];
        packet.CopyTo(array, 0x20);
        fixed (byte* p = array) {
            *(nuint*) (p + 0x18) = packetLengthOverride ?? (nuint) packet.Length;
            var result = Hook(p, 6);
            if (overwriteSource) array.AsSpan(0x20).Fill(0xCC);
            return result;
        }
    }

    private static void SetPipe(Stream writer, Stream reader) {
        SetPipeField("_lock", new System.Threading.Lock());
        SetPipeField("_pipeStream", new NamedPipeClientStream("unused-regression-pipe"));
        SetPipeField("_pipeWriter", new BinaryWriter(writer));
        SetPipeField("_pipeReader", new BinaryReader(reader));
        // 生产环境由 Init() 建立首个连接；测试里直接注入字段后手动标记 Connected。
        Goshujin.MarkConnectedForTest();
    }

    private static void SetPipeField(string name, object value) =>
        typeof(Goshujin).GetField(name, Flags)!.SetValue(null, value);

    private static void Check(bool condition, string name) {
        if (!condition) throw new Exception(name);
        _checks++;
    }

    [UnmanagedCallersOnly]
    private static unsafe int Original(byte* value, int startIndex) => 1234;

    [UnmanagedCallersOnly]
    private static unsafe byte Decompress(void* tcp, void* shared, void* source, uint sourceSize, void* target, uint size) {
        new byte[] {41, 42, 43}.CopyTo(new Span<byte>(target, (int) size));
        return _decompressResult ? (byte) 1 : (byte) 0;
    }

    private sealed class AckStream(bool block) : Stream {
        private int _readCount;
        public int ReadCount => Volatile.Read(ref _readCount);
        public ManualResetEventSlim Entered { get; } = new(false);
        public ManualResetEventSlim Release { get; } = new(false);
        public ManualResetEventSlim Exited { get; } = new(false);
        public override int ReadByte() {
            Entered.Set();
            try {
                if (block && !Release.Wait(WaitMs)) throw new IOException("Fake ACK wait exceeded bound");
                Interlocked.Increment(ref _readCount);
                return 0; // The real host ACK is false and must still count as a successful send.
            } finally {
                Exited.Set();
            }
        }
        public override int Read(byte[] buffer, int offset, int count) {
            if (count == 0) return 0;
            buffer[offset] = (byte) ReadByte();
            return 1;
        }
        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override void Flush() { }
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }

    /// <summary>
    /// 写会阻塞到显式 Release 或被 Dispose（模拟命名管道读端假死 + 关闭句柄解除阻塞）。
    /// </summary>
    private sealed class BlockingWriteStream : Stream {
        public ManualResetEventSlim Entered { get; } = new(false);
        public ManualResetEventSlim Release { get; } = new(false);
        public ManualResetEventSlim Disposed { get; } = new(false);
        public ManualResetEventSlim WriteExited { get; } = new(false);
        public override void Write(byte[] buffer, int offset, int count) {
            if (count == 0) return;
            Entered.Set();
            try {
                _ = Release.Wait(WaitMs);
            } finally {
                WriteExited.Set();
            }
        }
        protected override void Dispose(bool disposing) {
            Disposed.Set();
            Release.Set(); // Disable 会 dispose 管道；这里模拟关闭句柄后阻塞写退出。
            base.Dispose(disposing);
        }
        public override bool CanRead => false;
        public override bool CanSeek => false;
        public override bool CanWrite => true;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override void Flush() { }
        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
    }

    /// <summary>
    /// 控制管道测试流：读侧返回预置模式字节；EOF 后返回 -1 触发重连，
    /// 或（blockAfterBytes=true）阻塞模拟"宿主仍在但不再发模式"。
    /// </summary>
    private sealed class ModePipeStream : Stream {
        private readonly Queue<byte> _bytes;
        private readonly ManualResetEventSlim? _blockAfterBytes;
        private readonly object _gate = new();
        private int _disposed;
        public ModePipeStream(byte[] bytes, bool blockAfterBytes = false) {
            _bytes = new Queue<byte>(bytes);
            if (blockAfterBytes) {
                _blockAfterBytes = new ManualResetEventSlim(false);
            }
        }
        public bool Disposed => Volatile.Read(ref _disposed) != 0;
        public override int ReadByte() {
            lock (_gate) {
                if (_bytes.Count > 0) return _bytes.Dequeue();
            }
            if (_blockAfterBytes != null) _blockAfterBytes.Wait();
            return -1;
        }
        public override int Read(byte[] buffer, int offset, int count) {
            if (count == 0) return 0;
            buffer[offset] = (byte) ReadByte();
            return 1;
        }
        protected override void Dispose(bool disposing) {
            Interlocked.Exchange(ref _disposed, 1);
            _blockAfterBytes?.Set();
            base.Dispose(disposing);
        }
        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override void Flush() { }
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }

    /// <summary>第一次 ACK 读会阻塞到测试显式 Release；之后自动返回 0（false ACK）。</summary>
    private sealed class ScriptedAckStream : Stream {
        private int _readCount;
        private int _blockedReads;
        public int ReadCount => Volatile.Read(ref _readCount);
        public ManualResetEventSlim Entered { get; } = new(false);
        public ManualResetEventSlim Release { get; } = new(false);
        public override int ReadByte() {
            if (Interlocked.Increment(ref _blockedReads) == 1) {
                Entered.Set();
                _ = Release.Wait(WaitMs);
            }
            Interlocked.Increment(ref _readCount);
            return 0;
        }
        public override int Read(byte[] buffer, int offset, int count) {
            if (count == 0) return 0;
            buffer[offset] = (byte) ReadByte();
            return 1;
        }
        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override void Flush() { }
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }

    /// <summary>
    /// 模拟一条宿主（server）连接：读侧预置握手响应 + ACK 字节，写侧记录 DLL 发出的帧。
    /// 读空时返回 0/-1，让握手立即失败（用于"握手谈不拢"场景）。
    /// </summary>
    private sealed class FakeHostStream : Stream {
        private readonly Queue<byte> _read;
        private readonly List<byte> _written = [];
        private readonly object _gate = new();
        private int _readCount;
        private int _disposed;
        public FakeHostStream(params byte[] initialRead) {
            _read = new Queue<byte>(initialRead);
        }
        public bool Disposed => Volatile.Read(ref _disposed) != 0;
        public int ReadCount {
            get {
                lock (_gate) return _readCount;
            }
        }
        public byte[] WrittenSnapshot() {
            lock (_gate) return _written.ToArray();
        }
        public override void Write(byte[] buffer, int offset, int count) {
            lock (_gate) {
                for (var i = 0; i < count; i++) {
                    _written.Add(buffer[offset + i]);
                }
            }
        }
        public override void WriteByte(byte value) {
            lock (_gate) _written.Add(value);
        }
        public override int ReadByte() {
            lock (_gate) {
                if (_read.Count == 0) return -1;
                _readCount++;
                return _read.Dequeue();
            }
        }
        public override int Read(byte[] buffer, int offset, int count) {
            lock (_gate) {
                var read = 0;
                while (read < count && _read.Count > 0) {
                    buffer[offset + read++] = _read.Dequeue();
                }
                _readCount += read;
                return read;
            }
        }
        protected override void Dispose(bool disposing) {
            Interlocked.Exchange(ref _disposed, 1);
            base.Dispose(disposing);
        }
        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => true;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override void Flush() { }
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
    }

    /// <summary>只用于观测"旧连接是否被 Dispose"的假管道句柄（跨线程读，用 Volatile）。</summary>
    private sealed class DisposeTrackingStream : Stream {
        private int _disposed;
        public bool Disposed => Volatile.Read(ref _disposed) != 0;
        protected override void Dispose(bool disposing) {
            Interlocked.Exchange(ref _disposed, 1);
            base.Dispose(disposing);
        }
        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => true;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override void Flush() { }
        public override int Read(byte[] buffer, int offset, int count) => 0;
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) { }
    }

    // ---- 重连测试辅助 ----

    /// <summary>
    /// 构造一次握手的宿主响应字节流：
    /// 0xFA 的 cmdId 列表 + u32::MAX；0xFB 的 u32::MAX；0xFD 的 10 个 u32；末尾若干 ACK 字节。
    /// </summary>
    private static byte[] HandshakeBytes(uint[] cmds, uint[] rvas, int ackBytes = 8) {
        var bytes = new List<byte>();
        foreach (var cmd in cmds) {
            AddU32(bytes, cmd);
        }
        AddU32(bytes, uint.MaxValue);
        AddU32(bytes, uint.MaxValue);
        foreach (var rva in rvas) {
            AddU32(bytes, rva);
        }
        for (var i = 0; i < ackBytes; i++) {
            bytes.Add(0);
        }
        return bytes.ToArray();
    }

    private static void AddU32(List<byte> bytes, uint value) {
        var buffer = new byte[4];
        BinaryPrimitives.WriteUInt32LittleEndian(buffer, value);
        bytes.AddRange(buffer);
    }

    private static bool EndsWith(byte[] haystack, byte[] needle) {
        if (haystack.Length < needle.Length) return false;
        for (var i = 0; i < needle.Length; i++) {
            if (haystack[haystack.Length - needle.Length + i] != needle[i]) return false;
        }
        return true;
    }
}
