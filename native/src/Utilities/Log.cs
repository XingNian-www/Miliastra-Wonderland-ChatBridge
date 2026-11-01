using System.Runtime.CompilerServices;

// ReSharper disable MemberCanBePrivate.Global

namespace MiliastraWonderlandChatBridge.Utilities;

[Flags]
internal enum LogLevel : byte {
    Trace     = 0x00,
    Debug     = 0x01,
    Info      = 0x02,
    Warn      = 0x03,
    Error     = 0x04,
    Fatal     = 0x05,
    Time      = 0x06,
    LevelMask = 0x0F,
    FileOnly  = 0x10,
}

internal static class Log {

    #region ConsoleWriter

    private static TextWriter? _consoleWriter;

    /// <summary>
    /// 单条调试器日志的最大正文字符数（有界输出，避免异常消息/超大命令刷爆调试器）。
    /// </summary>
    private const int MaxDebuggerBodyChars = 2048;

    /// <summary>
    /// 回归测试注入点：替换真实的 Kernel32!OutputDebugStringW，便于在非 Windows
    /// 环境验证格式化与投递（生产保持 null，直接走 OutputDebugStringW）。
    /// </summary>
    internal static Action<string>? OutputSinkForTest { get; set; }

    // 两个符号刻意分开：
    //   EnableLogging        —— 控制日志代码是否编译进产物（生产必须开启）。
    //   EnableConsoleOutput  —— 只控制是否给游戏进程分配控制台（生产不定义，
    //                           避免注入后弹出一个对玩家可见的黑窗口）。
    [Conditional("EnableConsoleOutput")]
    public static void UseConsoleOutput() {
        try {
            InitializeConsole();
            _consoleWriter = Console.Out;
        } catch {
            // 日志是诊断增强：控制台初始化失败时退回"无输出"，
            // 绝不能让异常影响注入（Awake 里这次调用一旦抛出会导致注入失败）。
            _consoleWriter = null;
        }
    }

    [Conditional("EnableConsoleOutput")]
    public static void ResetConsole() {
        try {
            Kernel32.FreeConsole();
            InitializeConsole();
            var sw = new StreamWriter(Console.OpenStandardOutput(), _consoleWriter!.Encoding, 256, true) {
                AutoFlush = true
            };
            _consoleWriter = TextWriter.Synchronized(sw);
            Console.SetOut(_consoleWriter);
        } catch {
            // 控制台重置失败时保留现有 writer（可能为 null）。
            // Release 构建启用日志后，这里失败也绝不能冒泡到 Awake 让注入失败。
        }
    }

    private static unsafe void InitializeConsole() {
        Kernel32.AllocConsole();
        uint mode;
        var cHandle = Kernel32.GetStdHandle(Kernel32.STD_OUTPUT_HANDLE);
        if (!Kernel32.GetConsoleMode(cHandle, &mode)) {
            return;
        }
        Kernel32.SetConsoleMode(cHandle, mode | Kernel32.ENABLE_VIRTUAL_TERMINAL_PROCESSING);
        Console.OutputEncoding = Console.InputEncoding = System.Text.Encoding.UTF8;
    }

    #endregion

    [DoesNotReturn]
    public static void ErrorAndExit(string value, [CallerMemberName] string callerName = "") {
        WriteLog(value, callerName, LogLevel.Fatal);
        Environment.Exit(0);
    }

    public static void Error(string value, [CallerMemberName] string callerName = "") {
        WriteLog(value, callerName, LogLevel.Error);
    }

    public static void Warn(string value, [CallerMemberName] string callerName = "") {
        WriteLog(value, callerName, LogLevel.Warn);
    }

    public static void Info(string value, [CallerMemberName] string callerName = "") {
        WriteLog(value, callerName, LogLevel.Info);
    }

    public static void Debug(string value, [CallerMemberName] string callerName = "") {
        WriteLog(value, callerName, LogLevel.Debug);
    }

    public static void Trace(string value, [CallerMemberName] string callerName = "") {
        WriteLog(value, callerName, LogLevel.Trace);
    }

    public static void Time(string value, [CallerMemberName] string callerName = "") {
        WriteLog(value, callerName, LogLevel.Time);
    }

    [Conditional("EnableLogging")]
    public static void WriteLog(string message, string tag, LogLevel level) {
        var time = DateTimeOffset.Now.ToString("HH:mm:ss.fff");
        var writer = _consoleWriter;
        if (writer != null) {
            try {
                var color = level switch {
                    LogLevel.Error or LogLevel.Fatal => "244;67;54",
                    LogLevel.Warn  => "255;235;59",
                    LogLevel.Info  => "153;255;153",
                    LogLevel.Debug => "91;206;250",
                    LogLevel.Trace => "246;168;184",
                    LogLevel.Time  => "19;161;14",
                    _ => throw new ArgumentException($"Invalid log level: {level}")
                };
                writer.Write($"[{time}][\e[38;2;{color}m{level,5}\e[0m] {tag} : ");
                writer.WriteLine(message);
            } catch {
                // 控制台句柄被关闭/重定向失败时吞掉异常：日志绝不能把异常带回游戏线程
                // 或后台发送/重连线程（NativeAOT 下会触发进程级未处理异常，直接杀游戏）。
            }
        } else {
            // 生产 Release 不定义 EnableConsoleOutput，_consoleWriter 恒为 null。
            // 输出到 OutputDebugStringW（零窗口、零文件），DebugView / ProcMon / 调试器可抓；
            // 测试用 OutputSinkForTest 替换 P/Invoke 以在非 Windows 环境验证。
            try {
                var line = FormatDebuggerLine(time, message, tag, level);
                if (OutputSinkForTest is { } sink) {
                    sink(line);
                } else {
                    Kernel32.OutputDebugStringW(line);
                }
            } catch {
                // 日志绝不能把异常带回游戏线程/后台线程：OutputDebugString 不可用、
                // 测试 sink 抛异常等一律静默丢弃。
            }
        }
        if (level == LogLevel.Fatal) {
            if (_consoleWriter != null) {
                WriteLog("Error occurred, press enter key to exit", tag, LogLevel.Error);
                Console.ReadLine();
            } else {
                User32.MessageBoxW(0, "An critical error occurred.", "Error", 0x10);
            }
        }
    }

    /// <summary>
    /// 调试器输出行格式（有界）：[MWC][HH:mm:ss.fff][LEVEL] Tag : message。
    /// 前缀 MWC 方便在 DebugView 里过滤其它进程；正文超过上限截断。
    /// </summary>
    private static string FormatDebuggerLine(string time, string message, string tag, LogLevel level) {
        var levelText = level switch {
            LogLevel.Error or LogLevel.Fatal => "ERROR",
            LogLevel.Warn => "WARN",
            LogLevel.Info => "INFO",
            LogLevel.Debug => "DEBUG",
            LogLevel.Trace => "TRACE",
            LogLevel.Time => "TIME",
            _ => "LOG",
        };
        var body = message.Length > MaxDebuggerBodyChars
            ? message[..MaxDebuggerBodyChars] + "...(truncated)"
            : message;
        return $"[MWC][{time}][{levelText}] {tag} : {body}";
    }
}
