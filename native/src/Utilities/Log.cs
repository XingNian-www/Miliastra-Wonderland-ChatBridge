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

    [Conditional("EnableLogging")]
    public static void UseConsoleOutput() {
        InitializeConsole();
        _consoleWriter = Console.Out;
    }

    [Conditional("EnableLogging")]
    public static void ResetConsole() {
        Kernel32.FreeConsole();
        InitializeConsole();
        var sw = new StreamWriter(Console.OpenStandardOutput(), _consoleWriter!.Encoding, 256, true) {
            AutoFlush = true
        };
        _consoleWriter = TextWriter.Synchronized(sw);
        Console.SetOut(_consoleWriter);
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
        if (_consoleWriter != null) {
            var color = level switch {
                LogLevel.Error or LogLevel.Fatal => "244;67;54",
                LogLevel.Warn  => "255;235;59",
                LogLevel.Info  => "153;255;153",
                LogLevel.Debug => "91;206;250",
                LogLevel.Trace => "246;168;184",
                LogLevel.Time  => "19;161;14",
                _ => throw new ArgumentException($"Invalid log level: {level}")
            };
            _consoleWriter.Write($"[{time}][\e[38;2;{color}m{level,5}\e[0m] {tag} : ");
            _consoleWriter.WriteLine(message);
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
}
