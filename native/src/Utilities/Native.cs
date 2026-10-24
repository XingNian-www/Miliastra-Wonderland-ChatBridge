using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

// ReSharper disable MemberCanBePrivate.Global

namespace MiliastraWonderlandChatBridge.Utilities;

internal static unsafe class Native {

    #region WaitMainWindow

    private static nint _hwnd;
    private static readonly uint ProcessId = Kernel32.GetCurrentProcessId();

    public static void WaitMainWindow() {
        _hwnd = 0;
        do {
            Thread.Sleep(100);
            _ = User32.EnumWindows(&EnumWindowsCallback, 0);
        } while (_hwnd == 0);
        return;
        [UnmanagedCallersOnly(CallConvs = [ typeof(CallConvStdcall) ])]
        static int EnumWindowsCallback(nint handle, nint extraParameter) {
            uint wProcessId = 0; // Avoid uninitialized variable if the window got closed in the meantime
            _ = User32.GetWindowThreadProcessId(handle, &wProcessId);
            const int classNameCapacity = 256;
            var cName = stackalloc char[classNameCapacity];
            var length = User32.GetClassNameW(handle, cName, classNameCapacity);
            if (length > 0) {
                if (wProcessId == ProcessId && User32.IsWindowVisible(handle) && new string(cName, 0, length) == "UnityWndClass") {
                    _hwnd = handle;
                }
            }
            return _hwnd == 0 ? 1 : 0;
        }
    }

    #endregion

    #region RestoreVirtualProtect

    public static bool RestoreVirtualProtect() {
        // NtProtectVirtualMemoryImpl
        // _ = stackalloc byte[] { 0x4C, 0x8B, 0xD1, 0xB8, 0xFF, 0xFF, 0xFF, 0xFF, 0x0F, 0x05, 0xC3 };
        if (!NativeLibrary.TryLoad("ntdll.dll", out var hPtr)) {
            return false;
        }
        if (!NativeLibrary.TryGetExport(hPtr, "NtProtectVirtualMemory", out var mPtr)) {
            return false;
        }
        // 4C 8B D1         mov     r10, rcx
        // B8               mov     eax, $imm32
        if (*(uint*) (mPtr - 0x20) != 0xB8D18B4C) { // previous
            return false;
        }
        var syscallNumber = (ulong) *(uint*) (mPtr - 0x1C) + 1;
        var old = 0u;
        if (!Kernel32.VirtualProtect(mPtr, 1, Kernel32.PAGE_EXECUTE_READWRITE, &old)) {
            return false;
        }
        *(ulong*) mPtr = 0xB8D18B4C | syscallNumber << 32;
        return Kernel32.VirtualProtect(mPtr, 1, old, &old);
    }

    #endregion

    #region GetModuleHandle

    public static string GetModulePath(nint hModule) {
        var buffer = stackalloc char[256];
        _ = Kernel32.GetModuleFileNameW(hModule, buffer, 256);
        return new string(buffer);
    }

    public static nint GetModuleHandle(string? moduleName = null) {
        fixed (char* pName = moduleName ?? Path.GetFileName(GetModulePath(0))) {
            return Kernel32.GetModuleHandleW(pName);
        }
    }

    #endregion

    public static readonly nint ModuleBase = GetModuleHandle();

    public static nint RVAToVA(uint addr) => ModuleBase + (nint) addr;

    public static void RegisterUnhandledExceptionHandler() {
        AppDomain.CurrentDomain.UnhandledException += OnUnhandledException;
        return;
        static void OnUnhandledException(object? sender, UnhandledExceptionEventArgs e) {
            var ex = e.ExceptionObject as Exception;
            User32.MessageBoxW(0, ex?.ToString() ?? "null", "Unhandled Exception", 0x10);
            Environment.Exit(-1);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static nint AsPointer(this ReadOnlySpan<byte> span) => *(nint*) Unsafe.AsPointer(ref span);

}

internal static partial class MinHook {

    /// <summary>
    /// Initialize the MinHook library. You must call this function EXACTLY ONCE at the beginning of your program.
    /// </summary>
    [LibraryImport("libMinHook.x64", EntryPoint = "MH_Initialize")]
    private static partial uint MinHookInitialize();

    /// <summary>
    /// Creates a hook for the specified target function, in disabled state.
    /// </summary>
    /// <param name="pTarget">A pointer to the target function, which will be overridden by the detour function.</param>
    /// <param name="pDetour">A pointer to the detour function, which will override the target function.</param>
    /// <param name="ppOriginal">
    /// A pointer to the trampoline function, which will be used to call the original target function.
    /// This parameter can be NULL.
    /// </param>
    [LibraryImport("libMinHook.x64", EntryPoint = "MH_CreateHook")]
    private static partial uint MinHookCreate(nint pTarget, nint pDetour, out nint ppOriginal);

    /// <summary>
    /// Enables an already created hook.
    /// </summary>
    /// <param name="pTarget">
    /// A pointer to the target function.
    /// If this parameter is MH_ALL_HOOKS, all created hooks are enabled in one go.
    /// </param>
    [LibraryImport("libMinHook.x64", EntryPoint = "MH_EnableHook")]
    private static partial uint MinHookEnable(nint pTarget);

    /// <summary>
    /// Disables an already created hook.
    /// </summary>
    /// <param name="pTarget">
    /// A pointer to the target function.
    /// If this parameter is MH_ALL_HOOKS, all created hooks are enabled in one go.
    /// </param>
    [LibraryImport("libMinHook.x64", EntryPoint = "MH_DisableHook")]
    private static partial uint MinHookDisable(nint pTarget);

    /// <summary>
    /// Removes an already created hook.
    /// </summary>
    /// <param name="pTarget">A pointer to the target function.</param>
    [LibraryImport("libMinHook.x64", EntryPoint = "MH_RemoveHook")]
    private static partial uint MinHookRemove(nint pTarget);

    /// <summary>
    /// Uninitialize the MinHook library. You must call this function EXACTLY ONCE at the end of your program.
    /// </summary>
    [LibraryImport("libMinHook.x64", EntryPoint = "MH_Uninitialize")]
    // ReSharper disable once UnusedMember.Local
    private static partial uint MinHookUninitialize();

    static MinHook() {
        var result = MinHookInitialize();
        if (result != 0) {
            throw new InvalidOperationException($"Failed to initialize MinHook: {result}");
        }
    }

    // Publish the trampoline before enabling the detour: another thread may call it
    // as soon as MH_EnableHook patches the target.
    public static void Attach(nint origin, nint handler, ref nint trampoline) {
        if (origin == 0 || handler == 0) {
            throw new ArgumentException("Hook target and handler must be non-null.");
        }
        var result = MinHookCreate(origin, handler, out var original);
        if (result != 0) {
            throw new InvalidOperationException($"Failed to create hook: {result}");
        }
        if (original == 0) {
            MinHookRemove(origin);
            throw new InvalidOperationException("MinHook returned a null trampoline.");
        }
        Volatile.Write(ref trampoline, original);
        if ((result = MinHookEnable(origin)) != 0) {
            // Enabling failed; remove the disabled hook to release its trampoline.
            var cleanupResult = MinHookRemove(origin);
            if (cleanupResult == 0) {
                Volatile.Write(ref trampoline, 0);
            }
            throw new InvalidOperationException($"Failed to enable hook: {result}; remove hook: {cleanupResult}");
        }
    }

    public static void Detach(nint origin) {
        uint result;
        if ((result = MinHookDisable(origin)) != 0) {
            throw new InvalidOperationException($"Failed to create hook: {result}");
        }
        if ((result = MinHookRemove(origin)) != 0) {
            throw new InvalidOperationException($"Failed to enable hook: {result}");
        }
    }
}
