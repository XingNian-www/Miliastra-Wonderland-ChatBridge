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
            Goshujin.LoadTasks();
            Goshujin.LoadProps();
            Goshujin.LoadMethodTable();
            Goshujin.ResumeMainThread();
            //
            Native.WaitMainWindow();
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
            Interlocked.Exchange(ref _forwardingDisabled, 1);
            Log.Error($"Native initialization failed: {ex.Message}");
            return 1;
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
    // Must match the host pipe protocol MAX_PAYLOAD (1 MiB).
    internal const uint MaxPacketDataLength = 1024 * 1024;

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
        try {
            // IL2CPP byte[] 的 native 长度紧挨在数据之前（偏移 0x18）。
            // 但这个布局是推断值：若读出来不合理，就退化为 0（未知长度），
            // 跳过边界检查继续解析，而不是把包全丢掉。
            var packetLength = *(nuint*) (val + 0x18);
            if (packetLength < 12 || packetLength > int.MaxValue) {
                Log.Warn($"packetLength 异常({packetLength})，退化为无边界检查");
                packetLength = 0;
            }
            val += 0x20;
            if (*(ushort*) val != 0x6745) {
                return ret;
            }
            var cmdId = BinaryPrimitives.ReverseEndianness(*(ushort*) (val + 2));
            // RequiredPackets 查询保持原锁；AllPackets 是 volatile 读，放锁外避免每条包争锁。
            var required = IsRequired(cmdId);
            var allPackets = ModeControl.AllPackets;
            if (!required && !allPackets) {
                return ret;
            }
            if (TryGetData(val, (int) packetLength, out var data, out unzipBuf)) {
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
            }
        } catch (Exception ex) when (
            // InvalidDataException 与 IOException 是兄弟（都继承 SystemException），
            // 所以这里只需判断 IOException；写 `not InvalidDataException` 会编译失败。
            ex is IOException || ex is ObjectDisposedException) {
            Interlocked.Exchange(ref _forwardingDisabled, 1);
            Log.Warn($"Packet forwarding disabled: {ex.Message}");
        } catch (Exception ex) {
            // This observer must not propagate managed errors across the unmanaged hook.
            Log.Error($"Failed to forward packet: {ex.Message}");
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
        // packetLength 未知（0）时跳过边界检查：保持与上游一致的行为。
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
