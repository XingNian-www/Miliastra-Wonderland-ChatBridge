using System.Buffers.Binary;
using System.Runtime.InteropServices;
using MiliastraWonderlandChatBridge.Utilities;
using static MiliastraWonderlandChatBridge.GameMethod;

namespace MiliastraWonderlandChatBridge;

/// <summary>
/// DLL 入口。只做一件事：钩住网络解包函数，把聊天相关的包转发给宿主。
///
/// 与上游 YaeAchievement 相比，这里删掉了成就、玩家属性、自动点击开始按钮等功能——
/// 本项目只需要聊天数据。协议层（管道握手、方法表）保持完全兼容。
///
/// 可选增强：宿主可通过控制管道打开 "debug 全包模式"，此时除聊天包外，所有合法包
/// 还会额外复制一份到独立的 debug 管道（见 <see cref="ModeControl"/> /
/// <see cref="DebugOutbox"/>）。聊天通道的队列容量、ACK 协议与发送线程完全不变。
/// </summary>
internal static unsafe class Application {

    private static int _initialized;

    [UnmanagedCallersOnly(EntryPoint = "YaeMain")]
    private static uint Awake(nint hModule) {
        if (Interlocked.Exchange(ref _initialized, 1) != 0) {
            return 1;
        }
        try {
            Native.RegisterUnhandledExceptionHandler();
            // P3-2：钉住注入 DLL，防止游戏/加载器 FreeLibrary 后钩子与后台线程代码失效。
            if (!Native.PinCurrentModule((nint) (delegate* unmanaged<nint, uint>) &Awake)) {
                Log.Warn("Pin 当前 DLL 模块失败（游戏若卸载 DLL 可能崩溃）");
            }
            Log.UseConsoleOutput();
            Log.Trace("~");
            Goshujin.Init();
            // 可选增强通道：连接在后台线程进行，旧宿主没有管道时 5s 超时后仅停用增强功能。
            // 再包一层 try：即使线程都起不来（极端资源耗尽），也绝不能影响注入与聊天。
            try {
                ModeControl.Start();
                DebugOutbox.Start();
            } catch (Exception ex) {
                Log.Warn($"debug/control 通道启动失败，保持 chat 模式: {ex.Message}");
            }
            // 健康上报单独 try：它独立于聊天/debug，即使上面两条增强通道启动失败也必须启动，
            // 这样宿主在 DLL 停止转发（PacketOutbox.Disable）后仍能收到「我还活着但停了」。
            try {
                HealthReporter.Start();
            } catch (Exception ex) {
                Log.Warn($"健康上报通道启动失败: {ex.Message}");
            }
            Goshujin.LoadTasks();
            Goshujin.LoadProps();
            Goshujin.LoadMethodTable();
            Goshujin.ResumeMainThread();
            //
            // P2-2：窗口等待有上限；超时也继续安装钩子（ToInt32 不依赖窗口），
            // 避免窗口探测失败时 Awake 永久卡住、宿主看到"已连接但零包"。
            if (!Native.WaitMainWindow()) {
                Log.Warn("等待游戏主窗口超时，继续安装钩子（聊天转发不依赖窗口）");
            }
            Log.ResetConsole();
            //
            // 只需要 ToInt32：它是网络包的入口，聊天数据全部从这里经过。
            //
            // 上游用 SourceGenerator 自动把这里的调用改写成正确类型；本项目不引入那个
            // 生成器，改用手写转换。函数指针 → void* → nint 都是隐式/显式允许的。
            PacketOutbox.Start();
            MinHook.Attach(
                (nint) ToInt32,
                (nint) (delegate* unmanaged<byte*, int, int>) &OnToInt32,
                ref _toInt32);
            return 0;
        } catch (Exception ex) {
            AbortInitialization(ex.Message);
            return 1;
        }
    }

    /// <summary>
    /// 初始化失败收尾：置停转发、关闭主通道（宿主立即看到 EOF，而不是半活 DLL），
    /// 并记录失败原因。独立出来便于回归测试（P2-3）。
    /// </summary>
    internal static void AbortInitialization(string reason) {
        Interlocked.Exchange(ref _forwardingDisabled, 1);
        Log.Error($"Native initialization failed: {reason}");
        try {
            // PacketOutbox.Disable -> Goshujin.Shutdown：关闭主通道并进入终态，宿主不再空等。
            PacketOutbox.Disable($"native initialization failed: {reason}");
        } catch {
            // 收尾路径绝不能再抛异常。
        }
    }

    [UnmanagedCallersOnly(EntryPoint = "YaeWndHook")]
    private static nint WndHook(int nCode, nint wParam, nint lParam) {
        ((delegate*unmanaged<nint, uint>) &Awake)(0);
        return User32.CallNextHookEx(0, nCode, wParam, lParam);
    }

    #region RecvPacket

    /// <summary>宿主订阅的 cmd 列表。</summary>
    internal static readonly HashSet<uint> RequiredPackets = [];

    private static nint _toInt32;
    private static int _forwardingDisabled;
    private static long _invalidPacketLengthCount;
    private static long _forwardErrorLogCount;
    private static long _hookEnteredCount;
    private static long _badMagicCount;
    private static long _tryGetDataFailCount;
    private static long _acceptedCount;
    /// <summary>探针：各候选地址被调用次数（诊断 7.2 地址用）。</summary>
    private static readonly long[] _probeHits = new long[8];
    /// <summary>因 cmdId 未订阅而被丢弃的包数。</summary>
    private static long _filteredCount;
    /// <summary>最近一个未订阅的 cmdId（诊断用）。</summary>
    private static int _lastFilteredCmd;
    /// <summary>订阅列表快照（诊断用，最多 8 项）。</summary>
    private static readonly uint[] _requiredSample = new uint[8];
    /// <summary>第一个包的 val+0..+0x28 原始字节（诊断用，5 个 u64）。</summary>
    private static readonly ulong[] _probeLen = new ulong[5];
    private static int _probeLenDone;

    /// <summary>前 6 个不同包的原始头部（每包 8 字节，诊断用）。</summary>
    private static readonly ulong[] _rawHeader = new ulong[6];
    private static int _rawCount;
    private static uint _lastRawCmd;

    /// <summary>最近见到的 cmdId（环形，诊断用）。</summary>
    private static readonly uint[] _cmdSeen = new uint[8];
    private static int _cmdSeenIdx;
    private static long _cmdSeenTotal;
    // Must match the host pipe protocol MAX_PAYLOAD (1 MiB).
    internal const uint MaxPacketDataLength = 1024 * 1024;

    /// <summary>钩子被调用且 startIndex==6 的次数（= 真正进入网络包路径）。</summary>
    internal static long HookEnteredCount => Interlocked.Read(ref _hookEnteredCount);

    /// <summary>包起始魔数不是 0x6745 的次数（7.2 若改了包格式，这个会飙升）。</summary>
    internal static long BadMagicCount => Interlocked.Read(ref _badMagicCount);

    /// <summary>通过魔数校验但 TryGetData 返回 false 的次数（多为解压失败）。</summary>
    internal static long TryGetDataFailCount => Interlocked.Read(ref _tryGetDataFailCount);

    /// <summary>成功提取出包体并转发的次数。</summary>
    internal static long AcceptedCount => Interlocked.Read(ref _acceptedCount);

    /// <summary>当前 RequiredPackets 数量。直接读集合，供 1 Hz 心跳采样。</summary>
    internal static long RequiredCount {
        get {
            lock (RequiredPackets) {
                return RequiredPackets.Count;
            }
        }
    }

    /// <summary>因未订阅被过滤的包数。</summary>
    internal static long FilteredCount => Interlocked.Read(ref _filteredCount);

    /// <summary>最近被过滤的 cmdId。</summary>
    internal static long LastFilteredCmd => Volatile.Read(ref _lastFilteredCmd);

    /// <summary>订阅列表第 i 项（诊断用）。</summary>
    internal static long RequiredSample(int i) => Volatile.Read(ref _requiredSample[i]);

    /// <summary>最近见到的 cmdId 第 i 项（环形缓冲）。</summary>
    internal static long CmdSeen(int i) => Volatile.Read(ref _cmdSeen[i]);

    /// <summary>已见到的包总数。</summary>
    internal static long CmdSeenTotal => Interlocked.Read(ref _cmdSeenTotal);

    /// <summary>第 i 个不同包的原始 8 字节头。</summary>
    internal static ulong RawHeader(int i) => Volatile.Read(ref _rawHeader[i]);

    /// <summary>第一个包 val+0..+0x28 的字节（每 8 字节一组）。</summary>
    internal static ulong ProbeLen(int i) => Volatile.Read(ref _probeLen[i]);

    /// <summary>记录一个见到的 cmdId。</summary>
    private static void RecordCmd(ushort cmd) {
        Interlocked.Increment(ref _cmdSeenTotal);
        var idx = (int) (Interlocked.Increment(ref _cmdSeenIdx) & 7);
        Volatile.Write(ref _cmdSeen[idx], cmd);
    }

    /// <summary>把当前订阅列表的前 8 项拍一份快照，供心跳上报。</summary>
    internal static void SnapshotRequired() {
        lock (RequiredPackets) {
            var i = 0;
            foreach (var cmd in RequiredPackets) {
                if (i >= _requiredSample.Length) {
                    break;
                }
                Volatile.Write(ref _requiredSample[i], cmd);
                i++;
            }
            for (; i < _requiredSample.Length; i++) {
                Volatile.Write(ref _requiredSample[i], 0);
            }
        }
    }

    /// <summary>探针计数：第 i 个候选地址被调用的次数。</summary>
    internal static long ProbeHits(int i) => Interlocked.Read(ref _probeHits[i]);

    /// <summary>探针总数（用于健康帧打包）。</summary>
    internal static long ProbeTotal() {
        long sum = 0;
        for (var i = 0; i < _probeHits.Length; i++) {
            sum += Interlocked.Read(ref _probeHits[i]);
        }
        return sum;
    }

    [UnmanagedCallersOnly]
    private static int OnProbe(byte* val, int startIndex) {
        Interlocked.Increment(ref _probeHits[0]);
        return 0;
    }

    /// <summary>
    /// 因 native 长度字段不可信而丢弃的包数（观测用）。见 <see cref="OnToInt32"/>：
    /// 这类包绝不做无边界指针运算（越界读的 AccessViolation 无法被托管 catch 捕获）。
    /// </summary>
    internal static long InvalidPacketLengthCount => Interlocked.Read(ref _invalidPacketLengthCount);

    /// <summary>
    /// 转发过程中的托管异常次数（IO/解压/格式）。
    ///
    /// 与 <see cref="InvalidPacketLengthCount"/> 一起通过健康心跳上报，用来区分
    /// 「钩子根本没被调用」和「包进了钩子但被 TryGetData 丢弃」——后者非 0。
    /// </summary>
    internal static long ForwardErrorCount => Interlocked.Read(ref _forwardErrorLogCount);

    /// <summary>宿主是否订阅了这个 cmd。保持与原有实现相同的锁语义。</summary>
    internal static bool IsRequired(ushort cmdId) {
        lock (RequiredPackets) {
            return RequiredPackets.Contains(cmdId);
        }
    }

    /// <summary>
    /// 过滤判定：聊天包在 chat/all 两种模式下都转发；其它包只在 debug 全包模式
    /// （<see cref="ModeControl.AllPackets"/>）下转发。钩子为了区分两个通道会分别
    /// 读取这两项，这里供回归测试验证过滤语义。
    /// </summary>
    internal static bool ShouldForward(ushort cmdId) =>
        IsRequired(cmdId) || ModeControl.AllPackets;

    [UnmanagedCallersOnly]
    private static int OnToInt32(byte* val, int startIndex) {
        var original = (delegate* unmanaged<byte*, int, int>) Volatile.Read(ref _toInt32);
        var ret = original(val, startIndex);
        if (startIndex != 6 || val == null || Volatile.Read(ref _forwardingDisabled) != 0) {
            return ret;
        }
        void* unzipBuf = null;
        if (Interlocked.Exchange(ref _probeLenDone, 1) == 0) {
            for (var i = 0; i < _probeLen.Length; i++) {
                Volatile.Write(ref _probeLen[i], *(ulong*) (val + i * 8));
            }
        }
        try {
            // IL2CPP byte[] 的 native 长度紧挨在数据之前（偏移 0x18）。
            // 这个布局是推断值；长度不可信时**直接丢弃该包**：绝不退化成"无边界检查"，
            // 因为随后按包内长度做指针运算时，越界读产生的 AccessViolation 无法被
            // 本方法的托管 catch 捕获，会直接终止游戏进程（复核 P1-3）。
            var packetLength = *(nuint*) (val + 0x18);
            if (packetLength < 12 || packetLength > int.MaxValue) {
                var invalidCount = Interlocked.Increment(ref _invalidPacketLengthCount);
                // 限流：异常长度若持续出现（例如游戏布局变化），不要让日志本身变成负担。
                if (invalidCount <= 5 || invalidCount % 1024 == 0) {
                    Log.Warn($"packetLength 异常({packetLength})，丢弃该包（累计 {invalidCount} 次）");
                }
                return ret;
            }
            Interlocked.Increment(ref _hookEnteredCount);
            val += 0x20;
            if (*(ushort*) val != 0x6745) {
                Interlocked.Increment(ref _badMagicCount);
                return ret;
            }
            var cmdId = BinaryPrimitives.ReverseEndianness(*(ushort*) (val + 2));
            RecordCmd(cmdId);
            if (cmdId != _lastRawCmd) {
                Volatile.Write(ref _lastRawCmd, cmdId);
                var slot = Interlocked.Increment(ref _rawCount) - 1;
                if (slot >= 0 && slot < _rawHeader.Length) {
                    Volatile.Write(ref _rawHeader[slot], *(ulong*) val);
                }
            }
            // RequiredPackets 查询保持原锁；AllPackets 是 volatile 读，放锁外避免每条包争锁。
            var required = IsRequired(cmdId);
            var allPackets = ModeControl.AllPackets;
            if (!required && !allPackets) {
                Interlocked.Increment(ref _filteredCount);
                Volatile.Write(ref _lastFilteredCmd, cmdId);
                return ret;
            }
            if (TryGetData(val, (int) packetLength, out var data, out unzipBuf)) {
                Interlocked.Increment(ref _acceptedCount);
                // 只复制并入队，立即返回：这一步绝不能碰管道。
                if (required) {
                    // 容量检查、满队列 Dropped 计数与载荷拷贝都在 Enqueue 内部；
                    // 队列满/终态时游戏网络线程不会做无意义的 ToArray。
                    PacketOutbox.Enqueue(cmdId, data);
                }
                if (allPackets && !DebugOutbox.Disabled) {
                    // debug 通道只入队/计数，不做 IO；写失败由后台线程单独停用，不影响聊天。
                    // 载荷拷贝移进 Enqueue：Disable/容量/字节检查都在拷贝之前，
                    // 停用或满队列时游戏网络线程不再做无意义的 ToArray。
                    // TryGetData 已按 MaxPacketDataLength(1 MiB) 过滤，debug 包与聊天包同一上限。
                    DebugOutbox.Enqueue(cmdId, data);
                }
            } else {
                Interlocked.Increment(ref _tryGetDataFailCount);
            }
        } catch (Exception ex) when (
            // InvalidDataException 与 IOException 是兄弟（都继承 SystemException），
            // 所以这里只需判断 IOException；写 `not InvalidDataException` 会编译失败。
            ex is IOException || ex is ObjectDisposedException) {
            Interlocked.Exchange(ref _forwardingDisabled, 1);
            Log.Warn($"Packet forwarding disabled: {ex.Message}");
        } catch (Exception ex) {
            // This observer must not propagate managed errors across the unmanaged hook.
            // M3：异常包可能每包都走这里；OutputDebugStringW 在 DebugView 监听时是同步 IPC，
            // 不能在游戏网络线程上无节制地刷。与 packetLength 日志一样限流（前 5 次 + 每 1024 次）。
            var errorCount = Interlocked.Increment(ref _forwardErrorLogCount);
            if (errorCount <= 5 || errorCount % 1024 == 0) {
                Log.Error($"Failed to forward packet: {ex.Message}（累计 {errorCount} 次）");
            }
        } finally {
            // Covers success, decompression failure, malformed headers and pipe errors.
            NativeMemory.Free(unzipBuf);
        }
        return ret;
    }

    private static bool TryGetData(byte* val, int packetLength, out Span<byte> data, out void* unzipBuf) {
        data = default;
        unzipBuf = null;
        var headLen = BinaryPrimitives.ReverseEndianness(*(ushort*) (val + 4));
        var dataLen = BinaryPrimitives.ReverseEndianness(*(uint*) (val + 6));
        if (dataLen > MaxPacketDataLength) {
            return false;
        }
        // 调用方（OnToInt32）已保证 packetLength >= 12；保留 >0 防御是为了让
        // 测试/未来调用方在传入未知长度时也不会做无边界读取。
        if (packetLength > 0
            && (headLen > packetLength - 12 || dataLen > (uint) (packetLength - 12 - headLen))) {
            return false;
        }
        var headPtr = val + 10;
        var dataPtr = headPtr + headLen;
        if (*(ushort*) (dataPtr + dataLen) != 0xAB89) {
            return false;
        }
        var unzipLen = GetDecompressedSize(new ReadOnlySpan<byte>(headPtr, headLen));
        if (unzipLen == 0) {
            data = new Span<byte>(dataPtr, (int) dataLen);
            return true;
        }
        if (unzipLen > MaxPacketDataLength || Decompress == null || TcpStatePtr == null || SharedInfoPtr == null) {
            return false;
        }
        unzipBuf = NativeMemory.Alloc(unzipLen);
        if (unzipBuf == null) {
            throw new OutOfMemoryException();
        }
        if (!Decompress(*TcpStatePtr, *SharedInfoPtr, dataPtr, dataLen, unzipBuf, unzipLen)) {
            return false;
        }
        data = new Span<byte>(unzipBuf, (int) unzipLen);
        return true;
    }

    internal static uint GetDecompressedSize(ReadOnlySpan<byte> header) {
        var offset = 0;
        uint decompressedSize = 0;
        while (offset < header.Length) {
            var tag = ReadRawVarInt64(header, ref offset);
            if ((tag >> 3) == 0 || (tag >> 3) > 0x1FFFFFFF) {
                throw new InvalidDataException("Invalid protobuf field number.");
            }
            if (tag == 64) {
                var size = ReadRawVarInt64(header, ref offset);
                if (size > MaxPacketDataLength) {
                    throw new InvalidDataException("Decompressed packet exceeds supported length.");
                }
                decompressedSize = (uint) size;
                continue;
            }
            switch (tag & 7) {
                case 0:
                    ReadRawVarInt64(header, ref offset);
                    break;
                case 1:
                    SkipHeaderBytes(header, ref offset, 8);
                    break;
                case 2:
                    // Read first: compound assignment captures offset BEFORE ref updates it.
                    var length = ReadRawVarInt64(header, ref offset);
                    SkipHeaderBytes(header, ref offset, length);
                    break;
                case 5:
                    SkipHeaderBytes(header, ref offset, 4);
                    break;
                default:
                    throw new InvalidDataException("Unsupported protobuf wire type.");
            }
        }
        return decompressedSize;
    }

    private static void SkipHeaderBytes(ReadOnlySpan<byte> header, ref int offset, ulong count) {
        if (count > (ulong) (header.Length - offset)) {
            throw new InvalidDataException("Truncated protobuf field.");
        }
        offset += (int) count;
    }

    private static ulong ReadRawVarInt64(ReadOnlySpan<byte> span, ref int offset) {
        ulong result = 0;
        for (var i = 0; i < 10; i++) {
            if (offset >= span.Length) {
                throw new InvalidDataException("Truncated protobuf varint.");
            }
            var b = span[offset++];
            if (i == 9 && b > 1) {
                throw new InvalidDataException("Overflowing protobuf varint.");
            }
            result |= (ulong) (b & 0x7F) << (i * 7);
            if (b < 0x80) {
                return result;
            }
        }
        throw new InvalidDataException("CodedInputStream encountered a malformed varint.");
    }

    #endregion
}
