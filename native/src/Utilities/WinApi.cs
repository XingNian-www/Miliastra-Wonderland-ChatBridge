using System.Runtime.InteropServices;

namespace MiliastraWonderlandChatBridge.Utilities;

#pragma warning disable CS0649, CA1069 // ReSharper disable IdentifierTypo, InconsistentNaming, UnassignedField.Global

internal static unsafe partial class Kernel32 {

    [LibraryImport("KERNEL32.dll")]
    internal static partial uint GetCurrentProcessId();

    [LibraryImport("KERNEL32.dll", SetLastError = true)]
    internal static partial nint GetModuleHandleW(char* lpModuleName);

    [LibraryImport("KERNEL32.dll", SetLastError = true)]
    internal static partial uint GetModuleFileNameW(nint hModule, char* lpFilename, uint nSize);

    internal const uint PAGE_EXECUTE_READWRITE = 0x00000040;

    [return:MarshalAs(UnmanagedType.I4)]
    [LibraryImport("KERNEL32.dll", SetLastError = true)]
    internal static partial bool VirtualProtect(nint lpAddress, nuint dwSize, uint flNewProtect, uint* lpflOldProtect);

    internal const uint STD_OUTPUT_HANDLE = 0xFFFFFFF5;

    internal const uint ENABLE_VIRTUAL_TERMINAL_PROCESSING = 0x00000004;

    [return:MarshalAs(UnmanagedType.I4)]
    [LibraryImport("KERNEL32.dll", SetLastError = true)]
    internal static partial bool AllocConsole();

    [return:MarshalAs(UnmanagedType.I4)]
    [LibraryImport("KERNEL32.dll", SetLastError = true)]
    internal static partial bool FreeConsole();

    [LibraryImport("KERNEL32.dll", SetLastError = true)]
    internal static partial nint GetStdHandle(uint nStdHandle);

    [return:MarshalAs(UnmanagedType.I4)]
    [LibraryImport("KERNEL32.dll", SetLastError = true)]
    internal static partial bool GetConsoleMode(nint hConsoleHandle, uint* lpMode);

    [return:MarshalAs(UnmanagedType.I4)]
    [LibraryImport("KERNEL32.dll", SetLastError = true)]
    internal static partial bool SetConsoleMode(nint hConsoleHandle, uint dwMode);
    
    [LibraryImport("KERNEL32.dll", SetLastError = true)]
    internal static partial nint CreateThread(nint lpThreadAttributes, nint dwStackSize, delegate*unmanaged<nint, uint> lpStartAddress, nint lpParameter, uint dwCreationFlags, uint* lpThreadId);

}

internal static unsafe partial class User32 {

    [LibraryImport("USER32.dll", SetLastError = true)]
    internal static partial uint GetWindowThreadProcessId(nint hWnd, uint* lpdwProcessId);

    [LibraryImport("USER32.dll", SetLastError = true)]
    internal static partial int GetClassNameW(nint hWnd, char* lpClassName, int nMaxCount);

    [return: MarshalAs(UnmanagedType.I4)]
    [LibraryImport("USER32.dll")]
    internal static partial bool IsWindowVisible(nint hWnd);

    [return: MarshalAs(UnmanagedType.I4)]
    [LibraryImport("USER32.dll", SetLastError = true)]
    internal static partial bool EnumWindows(delegate *unmanaged[Stdcall]<nint, nint, int> lpEnumFunc, nint lParam);

    [LibraryImport("USER32.dll", StringMarshalling = StringMarshalling.Utf16, SetLastError = true)]
    internal static partial int MessageBoxW(nint hWnd, string text, string caption, uint uType);

    [LibraryImport("USER32.dll")]
    internal static partial nint CallNextHookEx(nint hhk, int nCode, nint wParam, nint lParam);

}
