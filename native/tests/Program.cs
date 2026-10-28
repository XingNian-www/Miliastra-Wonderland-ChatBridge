using System.Buffers.Binary;
using System.Diagnostics;
using System.IO.Pipes;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Channels;
using MiliastraWonderlandChatBridge;

internal static class Program {
    private const BindingFlags Flags = BindingFlags.Static | BindingFlags.NonPublic;
    private const int WaitMs = 10000;
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
            foreach (var scenario in new[] { "queue", "ack", "disconnect", "timeout", "debugcapacity", "debugbytes" })
                RunIsolated(scenario);
        } else {
            switch (args[0]) {
                case "queue": QueueChecks(); break;
                case "ack": SenderChecks(false); break;
                case "disconnect": SenderChecks(true); break;
                case "timeout": TimeoutChecks(); break;
                case "debugcapacity": DebugCapacityChecks(); break;
                case "debugbytes": DebugByteCapChecks(); break;
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
        compressed[11] = 3;
        Check(InvokeHook(compressed) == 1234 && Queue.Reader.TryRead(out _), "valid packet after malformed packet still queues");

        var capacity = (int) typeof(PacketOutbox).GetField("Capacity", Flags)!.GetRawConstantValue()!;
        Check(capacity == 256, "production queue is bounded at 256 packets");
        for (var i = 0; i < capacity + 32; i++) PacketOutbox.Enqueue((ushort) i, [(byte) i]);
        for (var i = 0; i < capacity; i++)
            Check(Queue.Reader.TryRead(out var queued) && queued.Cmd == i && queued.Data[0] == (byte) i,
                $"full queue preserves earlier packet {i}");
        Check(!Queue.Reader.TryRead(out _), "full queue drops new packets rather than growing");
    }

    private static void SenderChecks(bool disconnect) {
        InitializeHook();
        using var sink = new MemoryStream();
        using var ack = new AckStream(block: false);
        SetPipe(sink, ack);
        if (disconnect) sink.Dispose();
        PacketOutbox.Start();
        PacketOutbox.Start(); // Idempotence must not create a competing channel reader.
        byte[] packet = [0x45, 0x67, 0, 1, 0, 0, 0, 0, 0, 2, 10, 20, 0x89, 0xAB];
        Check(InvokeHook(packet) == 1234, "hook returns normally with asynchronous sender");
        if (disconnect) {
            Check(SpinWait.SpinUntil(() => PacketOutbox.Disabled, WaitMs), "write failure disables background forwarding");
            Check(InvokeHook(packet) == 1234, "disconnected hook retains original return");
            Check(!Queue.Reader.TryRead(out _), "disabled outbox rejects subsequent hook packets");
            Check(ack.ReadCount == 0, "failed write never attempts ACK read");
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
        using var ack = new AckStream(block: true);
        SetPipe(Stream.Null, ack);
        Check((int) typeof(PacketOutbox).GetField("SendTimeoutMs", Flags)!.GetRawConstantValue()! == 500,
            "production send deadline is 500ms");
        try {
            PacketOutbox.Start();
            var elapsed = Stopwatch.StartNew();
            PacketOutbox.Enqueue(1, [10, 20]);
            Check(ack.Entered.Wait(WaitMs), "fake host receives frame and withholds ACK");
            Check(SpinWait.SpinUntil(() => PacketOutbox.Disabled, WaitMs), "missing ACK disables sender within bounded wait");
            Check(elapsed.ElapsedMilliseconds >= 450, "missing ACK uses deadline rather than immediate failure");
            PacketOutbox.Enqueue(2, [30]);
            Check(!Queue.Reader.TryRead(out _), "timed out outbox rejects later packets");
        } finally {
            // Shutdown disposes the real pipe field, not this injected reader; release its pending Task explicitly.
            ack.Release.Set();
            Check(ack.Exited.Wait(WaitMs), "blocked fake read exits during cleanup");
        }
    }

    private static unsafe void InitializeHook() {
        typeof(Application).GetField("_toInt32", Flags)!.SetValue(null,
            (nint) (delegate* unmanaged<byte*, int, int>) &Original);
        Application.RequiredPackets.Add(1);
    }

    private static unsafe int InvokeHook(byte[] packet, bool overwriteSource = false) {
        var array = new byte[0x20 + packet.Length];
        packet.CopyTo(array, 0x20);
        fixed (byte* p = array) {
            *(nuint*) (p + 0x18) = (nuint) packet.Length;
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
}
