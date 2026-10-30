//! Windows 侧：注入游戏进程并接收管道数据。
//!
//! 链路（复用 YaeAchievement 的 DLL 契约）：
//!   1. 创建命名管道 "MiliastraWonderlandChatBridgePipe"
//!   2. 以挂起状态启动游戏进程
//!   3. LoadLibraryW 注入 DLL
//!   4. 恢复主线程；DLL 主动连管道
//!   5. 按 Goshujin 协议应答握手，之后持续收包
//!
//! 协议（见 YaeAchievementLib/src/Goshujin.cs 与宿主 Utils.cs）：
//!   客户端→宿主 `type` 字节，宿主按 type 应答：
//!     0xFA LoadTasks：宿主写 cmdId 列表 + u32::MAX(EOF)
//!     0xFB LoadProps：宿主写 prop 列表 + EOF（本项目不关心属性）
//!     0xFD LoadMethodTable：宿主写 10 个 u32 RVA
//!     0xFE ResumeMainThread：宿主恢复游戏主线程
//!     0x04 PushPacketData：{ cmdId:u16, len:u32, data }，宿主回 bool
//!     0xFF Exit：宿主退出

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::{Context, Result};

use crate::extract;
use crate::packet_control::{CONTROL_PIPE_NAME, DEBUG_PIPE_NAME, PacketControl};
use crate::state::ChatState;

/// 与 DLL 约定的管道名。
/// 与 DLL 约定的管道短名（DLL 侧用 NamedPipeClientStream 连接）。
pub const PIPE_NAME: &str = "MiliastraWonderlandChatBridgePipe";

/// Windows 命名管道的完整路径。
///
/// CreateNamedPipeW 要求 `\\.\pipe\<name>` 前缀；只给短名会得到
/// ERROR_INVALID_NAME (123)。DLL 侧用 NamedPipeClientStream 只需短名。
fn pipe_path_named(name: &str) -> Vec<u16> {
    format!(r"\\.\pipe\{name}")
        .encode_utf16()
        .chain(Some(0))
        .collect()
}

/// 启动注入并持续接收数据。
///
/// 单次会话；阻塞工作通过取消标志和短周期轮询退出。
#[allow(clippy::too_many_arguments)]
pub async fn run(
    chat: Arc<ChatState>,
    connected: Arc<AtomicBool>,
    packets: Arc<PacketControl>,
    auto_launch: bool,
    spawn_only: bool,
    lib_only: bool,
    game_path: Option<std::path::PathBuf>,
    cancelled: Arc<AtomicBool>,
) -> Result<()> {
    tokio::task::spawn_blocking(move || {
        // 只有一轮：本程序是"启动器"，游戏退出后自己也退出。
        // 反复重试会在游戏已运行时试图再拉一个实例，那是灾难性的。
        let result = serve_once(
            &chat,
            &connected,
            &packets,
            auto_launch,
            spawn_only,
            lib_only,
            game_path.as_deref(),
            &cancelled,
        );
        connected.store(false, Ordering::Relaxed);
        crate::diag::dll_connected(false);
        result
    })
    .await
    .context("注入线程异常退出")?
}

/// 收包循环空转时的自旋窗口。
///
/// 为什么需要它：DLL 的发送线程一次只写一个包并等宿主回执，收到回执后才写下一包。
/// 宿主旧实现回执后立刻 Peek，此时下一包通常还没写进来，于是**每个包**都要吃一次
/// `poll_pause()`（50ms）。千星大厅里 2246 信封的速率约 20 包/秒，正好卡在这个
/// 吞吐上限附近；一旦超过，DLL 侧 256 容量的发送队列就会积满：
///
///   256 包 × 50ms ≈ 12.8s 固定积压
///
/// 实测聊天消息从游戏画面出现到 ChatBridge 落库稳定晚 10–13s，正好等于该上限；
/// 队列满时新包被 DropWrite 丢弃，表现为「偶尔漏命令」。自旋窗口让宿主在回执后
/// 继续短等下一包，把吞吐从 ~20/s 提到数千/s，积压随即排空。
const PIPE_SPIN_WINDOW: std::time::Duration = std::time::Duration::from_millis(2);

/// 最近一次收包后多久内仍按「高压」处理（自旋等下一包）；超过则回到 50ms 空闲轮询。
const ACTIVE_PIPE_WINDOW: std::time::Duration = std::time::Duration::from_millis(250);

/// 在自旋窗口内短等下一包；返回是否等到数据。
///
/// 只在「刚收过包」时调用：静默期仍走 50ms 空闲轮询，CPU 占用与旧实现一致。
fn wait_for_pipe_data(stream: &std::fs::File) -> Result<bool> {
    let deadline = std::time::Instant::now() + PIPE_SPIN_WINDOW;
    loop {
        match poll_pipe(stream)? {
            PipeState::Data => return Ok(true),
            PipeState::Closed => return Ok(false),
            PipeState::Empty => {}
        }
        if std::time::Instant::now() >= deadline {
            return Ok(false);
        }
        std::hint::spin_loop();
        std::thread::yield_now();
    }
}

/// 一次完整的注入会话。
#[allow(clippy::too_many_arguments)]
fn serve_once(
    chat: &ChatState,
    connected: &AtomicBool,
    packets: &Arc<PacketControl>,
    auto_launch: bool,
    spawn_only: bool,
    lib_only: bool,
    game_path: Option<&std::path::Path>,
    cancelled: &AtomicBool,
) -> Result<()> {
    use std::io::{Read, Write};
    use std::os::windows::io::FromRawHandle;

    check_cancelled(cancelled)?;
    if !auto_launch {
        return Ok(());
    }
    let exe = locate_game(game_path)?;

    // 必须先确认游戏没在运行。
    //
    // 附加模式在技术上不可行：原神的反作弊会拦截外部进程对游戏的内存写入。
    // 实测 OpenProcess 各种权限组合都成功，但 VirtualAllocEx 一律拒绝访问(err=5)；
    // 而 CreateProcess(CREATE_SUSPENDED) 拿到的是"创建者句柄"，不受该拦截。
    // 因此只能自己拉起游戏，绝不能在游戏已运行时再启动第二个实例。
    if let Some(existing) = open_running_game() {
        let code = crate::exit_code::code_of(crate::exit_code::Kind::GameAlreadyRunning);
        tracing::error!(
            "游戏已在运行 (pid={existing})，但反作弊会拦截外部注入。\
             请先退出游戏，本服务会自己拉起它。退出码 {code}（等玩家退出即可，无需人工修复）"
        );
        return Err(crate::exit_code::Marked(crate::exit_code::Kind::GameAlreadyRunning).into());
    }
    if spawn_only || lib_only {
        let proc = spawn_suspended(&exe)?;
        if lib_only {
            load_library_remote(&proc, &locate_dll()?, cancelled)?;
        }
        check_cancelled(cancelled)?;
        resume_thread(&proc)?;
        return wait_for_game(&proc, cancelled);
    }
    let dll = locate_dll()?;
    let rva = resolve_rva(&exe)?;
    let pipe = create_pipe()?;
    // debug 数据管道与模式控制管道必须也在注入前创建：DLL 初始化时会主动连接。
    // 这两条是可选增强：创建失败只停用 debug 能力，不能因此中止聊天注入。
    match create_named_pipe_server(
        DEBUG_PIPE_NAME,
        windows_sys::Win32::Storage::FileSystem::PIPE_ACCESS_INBOUND,
    ) {
        Ok(debug_pipe) => start_debug_pipe(debug_pipe, Arc::clone(packets)),
        Err(error) => {
            tracing::warn!("debug 数据管道创建失败，全包模式不可用（聊天不受影响）：{error:#}")
        }
    }
    match create_named_pipe_server(
        CONTROL_PIPE_NAME,
        windows_sys::Win32::Storage::FileSystem::PIPE_ACCESS_OUTBOUND,
    ) {
        Ok(control_pipe) => start_control_pipe(control_pipe, Arc::clone(packets)),
        Err(error) => {
            tracing::warn!("控制管道创建失败，debug 开关不可用（聊天不受影响）：{error:#}")
        }
    }
    check_cancelled(cancelled)?;
    tracing::info!("启动并注入 {} -> {}", exe.display(), dll.display());
    let proc = spawn_suspended(&exe)?;
    inject_dll(&proc, &dll, cancelled)?;

    // DLL 连接管道后才会继续初始化。
    connect_pipe(&pipe, &proc, cancelled)?;
    // SAFETY: pipe 是刚创建并已连接的合法句柄，所有权转交后不再用裸句柄。
    // SAFETY: 句柄由 CreateNamedPipeW 返回，所有权转交 File。
    let mut stream = unsafe { std::fs::File::from_raw_handle(pipe.0 as *mut _) };
    // 阻止 PipeHandle 的 Drop 再次关闭这个句柄。
    std::mem::forget(pipe);

    connected.store(true, Ordering::Relaxed);
    crate::diag::dll_connected(true);
    tracing::info!("DLL 已连接，开始收包");
    // 管道保持阻塞模式：PIPE_NOWAIT 已废弃，实测会让 ReadFile 返回假的 EOF。
    // 非阻塞检查改用 PeekNamedPipe：高压时先自旋接下一包（见 PIPE_SPIN_WINDOW），
    // 静默时才回到每 50ms 检查取消和进程。
    tracing::info!("等待 DLL 发送首帧...");

    let mut buffered: Vec<u8> = Vec::new();
    let mut frames_seen: u64 = 0;
    let mut last_report = std::time::Instant::now();
    // 最近一次读到数据的时刻：只有刚收过包才自旋等下一包，静默期不额外占 CPU。
    let mut last_data_at = std::time::Instant::now() - std::time::Duration::from_secs(1);
    let init_deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    loop {
        check_session(&proc, cancelled, init_deadline)?;
        if !process_alive(proc.process) {
            return Ok(());
        }
        // 先探测：无数据就短暂等待，有数据立即读（阻塞读此时不会卡）。
        match poll_pipe(&stream)? {
            PipeState::Data => {}
            PipeState::Empty => {
                // 有积压时下一包在回执后几微秒内就到；先自旋接住它，
                // 不能每包都退回去吃一次 50ms 空闲轮询（见 PIPE_SPIN_WINDOW）。
                if last_data_at.elapsed() < ACTIVE_PIPE_WINDOW {
                    let hit = wait_for_pipe_data(&stream)?;
                    crate::diag::spin(hit);
                    if hit {
                        last_data_at = std::time::Instant::now();
                        continue;
                    }
                }
                crate::diag::idle_poll();
                poll_pause();
                continue;
            }
            PipeState::Closed => {
                return Err(session_end_error(&proc, "对端已关闭").into());
            }
        }
        let mut chunk = [0u8; 4096];
        let read = match stream.read(&mut chunk) {
            Ok(n) => n,
            Err(error) if matches!(error.raw_os_error(), Some(109 | 232 | 233)) => {
                poll_pause();
                continue;
            }
            Err(error) => return Err(error).context("读管道失败"),
        };
        if read == 0 {
            return Err(session_end_error(&proc, "管道返回 0 字节").into());
        }
        last_data_at = std::time::Instant::now();
        crate::diag::frame(read);
        buffered.extend_from_slice(&chunk[..read]);
        crate::diag::buffered(buffered.len());
        while let Some(&kind) = buffered.first() {
            let (frame, used) = match crate::pipe::parse(kind, &buffered[1..]) {
                Ok(Some(parsed)) => parsed,
                Ok(None) => break,
                Err(error) => {
                    tracing::warn!("管道协议错误，终止会话: {error:#}");
                    crate::diag::note("pipe_protocol_error", format!("{error:#}"));
                    return Ok(());
                }
            };
            buffered.drain(..1 + used);
            frames_seen += 1;
            // 包帧的 ACK 只表示「保持订阅」，与解析结果无关：先回执让 DLL
            // 立刻写下一包，把 dispatch 的耗时移出往返路径。
            let reply = crate::pipe::reply(&frame, &rva);
            let is_packet = matches!(&frame, crate::pipe::Frame::Packet { .. });
            if is_packet && !reply.is_empty() {
                let ack_started = std::time::Instant::now();
                stream.write_all(&reply).context("写管道失败")?;
                crate::diag::ack(ack_started.elapsed().as_micros() as u64);
            }
            if let crate::pipe::Frame::Packet { cmd, payload } = &frame {
                crate::diag::packet(*cmd, payload.len());
                let dispatch_started = std::time::Instant::now();
                dispatch(chat, *cmd, payload);
                crate::diag::dispatch(dispatch_started.elapsed().as_micros() as u64);
            }
            if last_report.elapsed() >= std::time::Duration::from_secs(5) {
                tracing::info!("已收帧 {frames_seen} 个，缓冲 {} 字节", buffered.len());
                last_report = std::time::Instant::now();
            }
            if frame == crate::pipe::Frame::ResumeMainThread {
                for _ in 0..30 {
                    check_session(&proc, cancelled, init_deadline)?;
                    poll_pause();
                }
                resume_thread(&proc)?;
                tracing::info!("已恢复游戏主线程");
            }
            // 其余帧（握手/退出）的回执要等对应动作完成后再写。
            if !is_packet && !reply.is_empty() {
                stream.write_all(&reply).context("写管道失败")?;
            }
            if frame == crate::pipe::Frame::Exit {
                return Ok(());
            }
        }
    }
}

fn dispatch(chat: &ChatState, cmd: u16, payload: &[u8]) {
    // 独立 486 只承载私聊；信封 2246 才有多路复用内容。
    // 好友列表（含备注）也在 2246 信封内部（内层 #13=4410），跟着信封一起解析。
    let (halls, names, population, chats, profiles) = if cmd == extract::CMD_PRIVATE_DIRECT {
        (
            None,
            Vec::new(),
            None,
            extract::extract_direct_private(payload),
            Vec::new(),
        )
    } else {
        (
            extract::extract_hall_context(payload),
            extract::extract_names(payload),
            extract::extract_population_hall(payload),
            extract::extract_chats(payload),
            extract::extract_friend_profiles(payload),
        )
    };
    tracing::debug!(
        "2246 解析: hall={:?} names={} pop={:?} chats={} profiles={}",
        halls,
        names.len(),
        population,
        chats.len(),
        profiles.len()
    );
    if halls.is_none()
        && names.is_empty()
        && population.is_none()
        && chats.is_empty()
        && profiles.is_empty()
    {
        return;
    }
    // 阻塞线程里没有 tokio 上下文，用 Handle::block_on 进入运行时。
    let handle = tokio::runtime::Handle::current();
    handle.block_on(async {
        if !names.is_empty() {
            chat.record_names(names).await;
        }
        if !profiles.is_empty() {
            // 进游戏全量一次、之后懒加载增量；只在全量那次打 info，避免刷日志。
            let remarked = profiles.iter().filter(|p| p.remark.is_some()).count();
            let total = profiles.len();
            let baseline = chat.record_friend_profiles(profiles).await;
            if baseline {
                tracing::info!(
                    "好友列表（全量基线）: {total} 条记录，其中 {remarked} 条带备注，累计备注 {} 条",
                    chat.remark_count().await
                );
            } else {
                tracing::debug!("好友列表（增量）: {total} 条记录，其中 {remarked} 条带备注");
            }
        }
        if let Some(hall_id) = population {
            chat.note_population_hall(hall_id).await;
        }
        chat.ingest(chats).await;
    });
}

struct PipeHandle(windows_sys::Win32::Foundation::HANDLE);

impl Drop for PipeHandle {
    fn drop(&mut self) {
        // 不实现 Drop 会泄漏管道实例：名字一直被占用，下一次 create_pipe 永远
        // 拿不到实例（ERROR_PIPE_BUSY），最后把自己锁死。
        if !self.0.is_null() && self.0 as isize != -1 {
            // SAFETY: 句柄由 CreateNamedPipeW 返回且未被转交。
            unsafe { windows_sys::Win32::Foundation::CloseHandle(self.0) };
        }
    }
}

fn create_pipe() -> Result<PipeHandle> {
    create_named_pipe_server(
        PIPE_NAME,
        windows_sys::Win32::Storage::FileSystem::PIPE_ACCESS_DUPLEX,
    )
}

/// 创建一条命名管道 server；同名实例被占用时重试。
///
/// 上一次会话被强杀后，管道名会被内核短时间占用（ERROR_PIPE_BUSY）。
/// 等待重试比失败退出好：用户重启服务时不需要手动等待。
fn create_named_pipe_server(name: &str, access: u32) -> Result<PipeHandle> {
    let mut last_error = None;
    for attempt in 0..10 {
        match create_named_pipe(&pipe_path_named(name), access) {
            Ok(pipe) => return Ok(pipe),
            Err(error) => {
                let busy = error
                    .downcast_ref::<std::io::Error>()
                    .and_then(|e| e.raw_os_error())
                    == Some(231); // ERROR_PIPE_BUSY
                if !busy {
                    return Err(error);
                }
                last_error = Some(error);
                if attempt < 9 {
                    tracing::debug!("管道 {name} 被占用，200ms 后重试 ({}/10)", attempt + 1);
                    std::thread::sleep(std::time::Duration::from_millis(200));
                }
            }
        }
    }
    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("创建管道 {name} 失败")))
}

/// 尝试打开一条已有服务实例的管道。
///
/// 如果有另一个 Miliastra-Wonderland-Chat 已经在提供服务，我们应该复用它而不是抢名字——
/// 抢名字失败后强杀旧进程会让游戏里的 DLL 管道断裂并弹窗崩溃。
fn probe_existing_server() -> bool {
    use windows_sys::Win32::Foundation::{GENERIC_READ, GENERIC_WRITE};
    use windows_sys::Win32::Storage::FileSystem::{CreateFileW, OPEN_EXISTING};
    let name: Vec<u16> = format!(r"\\.\pipe\{PIPE_NAME}")
        .encode_utf16()
        .chain(Some(0))
        .collect();
    // SAFETY: name 以 NUL 结尾。
    let handle = unsafe {
        CreateFileW(
            name.as_ptr(),
            GENERIC_READ | GENERIC_WRITE,
            0,
            std::ptr::null_mut(),
            OPEN_EXISTING,
            0,
            std::ptr::null_mut(),
        )
    };
    if handle.is_null() || handle as isize == -1 {
        return false;
    }
    // SAFETY: handle 由 CreateFileW 返回。
    unsafe { windows_sys::Win32::Foundation::CloseHandle(handle) };
    true
}

fn create_named_pipe(name: &[u16], access: u32) -> Result<PipeHandle> {
    use windows_sys::Win32::Storage::FileSystem::FILE_FLAG_FIRST_PIPE_INSTANCE;
    use windows_sys::Win32::System::Pipes::{
        CreateNamedPipeW, PIPE_READMODE_BYTE, PIPE_TYPE_BYTE, PIPE_WAIT,
    };
    // SAFETY: 调用方提供以 NUL 结尾的管道名，其余参数是常量。
    let handle = unsafe {
        CreateNamedPipeW(
            name.as_ptr(),
            access | FILE_FLAG_FIRST_PIPE_INSTANCE,
            PIPE_TYPE_BYTE | PIPE_READMODE_BYTE | PIPE_WAIT,
            1,
            65536,
            65536,
            0,
            std::ptr::null_mut(),
        )
    };
    if handle.is_null() || handle as isize == -1 {
        return Err(std::io::Error::last_os_error()).context("创建命名管道失败");
    }
    Ok(PipeHandle(handle))
}

/// 管道当前状态。
enum PipeState {
    Data,
    Empty,
    Closed,
}

/// 探测管道状态。
///
/// 管道是阻塞模式，直接 read 会一直等；Peek 让我们保留轮询语义，
/// 同时不引入 PIPE_NOWAIT 的语义陷阱。
fn poll_pipe(stream: &std::fs::File) -> Result<PipeState> {
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::System::Pipes::PeekNamedPipe;
    let mut available = 0u32;
    // SAFETY: 句柄来自 File，buffer 传 null + 长度 0 时只查询可用字节数。
    let ok = unsafe {
        PeekNamedPipe(
            stream.as_raw_handle() as *mut _,
            std::ptr::null_mut(),
            0,
            std::ptr::null_mut(),
            &mut available,
            std::ptr::null_mut(),
        )
    };
    if ok == 0 {
        let error = std::io::Error::last_os_error();
        if matches!(error.raw_os_error(), Some(109 | 233)) {
            return Ok(PipeState::Closed);
        }
        return Err(error).context("检查管道状态失败");
    }
    Ok(if available > 0 {
        PipeState::Data
    } else {
        PipeState::Empty
    })
}
fn poll_pause() {
    std::thread::sleep(std::time::Duration::from_millis(50));
}

fn check_cancelled(cancelled: &AtomicBool) -> Result<()> {
    anyhow::ensure!(!cancelled.load(Ordering::Acquire), "注入会话已取消");
    Ok(())
}

fn check_session(
    proc: &SuspendedProcess,
    cancelled: &AtomicBool,
    deadline: std::time::Instant,
) -> Result<()> {
    check_cancelled(cancelled)?;
    anyhow::ensure!(
        proc.started.get() || std::time::Instant::now() < deadline,
        "DLL 初始化超时"
    );
    Ok(())
}

fn wait_for_game(proc: &SuspendedProcess, cancelled: &AtomicBool) -> Result<()> {
    while process_alive(proc.process) {
        check_cancelled(cancelled)?;
        poll_pause();
    }
    Ok(())
}

fn connect_pipe(pipe: &PipeHandle, proc: &SuspendedProcess, cancelled: &AtomicBool) -> Result<()> {
    use windows_sys::Win32::System::Pipes::ConnectNamedPipe;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    check_session(proc, cancelled, deadline)?;
    anyhow::ensure!(process_alive(proc.process), "等待管道时游戏已退出");
    // 阻塞等待：注入成功后 DLL 会立刻连接（实测 <100ms），这里不会久等。
    if unsafe { ConnectNamedPipe(pipe.0, std::ptr::null_mut()) } != 0 {
        return Ok(());
    }
    let error = std::io::Error::last_os_error();
    match error.raw_os_error() {
        // ERROR_PIPE_CONNECTED：客户端已经连上，视为成功。
        Some(535) => Ok(()),
        _ => Err(error).context("等待管道连接失败"),
    }
}
/// 等待 DLL 连上控制管道；成功后把只写句柄交给 PacketControl。
fn start_control_pipe(pipe: PipeHandle, packets: Arc<PacketControl>) {
    // PipeHandle 是裸句柄，不实现 Send；把所有权编码成整数交给后台线程。
    let raw = pipe.0 as usize;
    std::mem::forget(pipe);
    let spawned = std::thread::Builder::new()
        .name("mwc-control-pipe".to_owned())
        .spawn(move || {
            let Some(file) = take_connected_pipe(raw, "control") else {
                return;
            };
            packets.attach_control(Box::new(file));
        });
    if let Err(error) = spawned {
        tracing::warn!("启动控制管道线程失败：{error}");
    }
}

/// 等待 DLL 连上 debug 数据管道，然后持续解析包并交给 PacketControl。
///
/// debug 管道是单向的：DLL 写、宿主读，没有 ACK；协议帧复用聊天管道的
/// 0x04 PushPacketData 编码。解析/统计/落盘都在这个独立线程里，不碰聊天状态。
fn start_debug_pipe(pipe: PipeHandle, packets: Arc<PacketControl>) {
    let raw = pipe.0 as usize;
    std::mem::forget(pipe);
    let spawned = std::thread::Builder::new()
        .name("mwc-debug-pipe".to_owned())
        .spawn(move || {
            use std::io::Read;
            let Some(mut file) = take_connected_pipe(raw, "debug") else {
                return;
            };
            tracing::info!("debug 数据管道已连接");
            let mut buffered: Vec<u8> = Vec::new();
            let mut chunk = [0u8; 65536];
            loop {
                match file.read(&mut chunk) {
                    Ok(0) => break,
                    Ok(read) => buffered.extend_from_slice(&chunk[..read]),
                    Err(error) => {
                        tracing::warn!("读 debug 管道失败：{error}");
                        break;
                    }
                }
                while let Some(&kind) = buffered.first() {
                    match crate::pipe::parse(kind, &buffered[1..]) {
                        Ok(Some((frame, used))) => {
                            buffered.drain(..1 + used);
                            if let crate::pipe::Frame::Packet { cmd, payload } = frame {
                                packets.observe_debug_packet(cmd, &payload);
                            }
                        }
                        Ok(None) => break,
                        Err(error) => {
                            tracing::warn!("debug 管道协议错误，停止抓包：{error:#}");
                            return;
                        }
                    }
                }
                if buffered.len() > crate::pipe::MAX_PAYLOAD + 6 {
                    tracing::warn!("debug 管道缓冲异常，停止抓包");
                    return;
                }
            }
            tracing::info!("debug 数据管道已关闭");
        });
    if let Err(error) = spawned {
        tracing::warn!("启动 debug 管道线程失败：{error}");
    }
}

/// 阻塞等待一条已创建的命名管道被客户端连接，成功后转成 File。
fn take_connected_pipe(raw: usize, label: &str) -> Option<std::fs::File> {
    use std::os::windows::io::FromRawHandle;
    let handle = raw as *mut std::ffi::c_void;
    if let Err(error) = accept_pipe(handle) {
        tracing::warn!("等待 {label} 管道连接失败：{error:#}");
        // SAFETY: 句柄来自 CreateNamedPipeW，尚未转交。
        unsafe { windows_sys::Win32::Foundation::CloseHandle(handle) };
        return None;
    }
    // SAFETY: 句柄已连接，所有权转交 File，之后由 File 关闭。
    Some(unsafe { std::fs::File::from_raw_handle(handle) })
}

fn accept_pipe(handle: windows_sys::Win32::Foundation::HANDLE) -> Result<()> {
    use windows_sys::Win32::System::Pipes::ConnectNamedPipe;
    // SAFETY: handle 是 CreateNamedPipeW 返回的合法实例句柄。
    if unsafe { ConnectNamedPipe(handle, std::ptr::null_mut()) } != 0 {
        return Ok(());
    }
    let error = std::io::Error::last_os_error();
    match error.raw_os_error() {
        // ERROR_PIPE_CONNECTED：客户端已经连上，视为成功。
        Some(535) => Ok(()),
        _ => Err(error).context("等待 debug/control 管道连接失败"),
    }
}

/// 解析 metadata，按游戏 exe 的 CRC32 取 RVA 表。
fn resolve_rva(exe: &std::path::Path) -> Result<crate::metadata::MethodRva> {
    use std::io::Read;
    use std::sync::OnceLock;

    // metadata 是 165KB 的 protobuf，每轮重试都解析纯属浪费。
    // 游戏更新会换 metadata 文件，但那种情况需要重启本程序，缓存是安全的。
    static META: OnceLock<Result<crate::metadata::Metadata, String>> = OnceLock::new();
    let meta = META.get_or_init(|| {
        let meta_path = locate_metadata().map_err(|e| format!("{e:#}"))?;
        let bytes = std::fs::read(&meta_path)
            .map_err(|e| format!("读取 metadata 失败 {}: {e}", meta_path.display()))?;
        let meta = crate::metadata::Metadata::parse(&bytes).map_err(|e| format!("{e:#}"))?;
        tracing::info!(
            "metadata 版本 {}，包含 {} 个游戏版本的 RVA 表",
            meta.version,
            meta.rva_by_game.len()
        );
        Ok(meta)
    });
    let meta = match meta {
        Ok(meta) => meta,
        Err(message) => anyhow::bail!("{message}"),
    };

    let mut head = vec![0u8; 65536];
    let mut file = std::fs::File::open(exe)
        .with_context(|| format!("打开游戏 exe 失败: {}", exe.display()))?;
    let read = file.read(&mut head).context("读取游戏 exe 头部失败")?;
    head.truncate(read);
    let crc = crate::metadata::game_crc32(&head);
    tracing::debug!("游戏 exe CRC32 = 0x{crc:08x}");

    let table = meta.for_game(crc).with_context(|| {
        format!(
            "metadata 里没有 CRC32=0x{crc:08x} 的 RVA 表（游戏版本可能已更新，需重新获取 metadata）"
        )
    })?;
    if !table.is_valid() {
        anyhow::bail!("RVA 表不完整（to_int32/tcp_state_ptr/decompress 不能为 0）");
    }
    Ok(table.clone())
}

/// 从启动器注册表读安装目录。
///
/// 与 Miliastra-Wonderland-Music 完全一致的两条键：
///   HKCU\Software\miHoYo\HYP\1_1\hk4e_cn        （国服）
///   HKCU\Software\Cognosphere\HYP\1_0\hk4e_global（国际服）
/// 值名都是 GameInstallPath，类型 REG_SZ。
#[cfg(windows)]
fn registry_install_dirs() -> Vec<std::path::PathBuf> {
    use windows_sys::Win32::System::Registry::{HKEY_CURRENT_USER, RRF_RT_REG_SZ, RegGetValueW};

    /// 读一个 REG_SZ 值；不存在或类型不符都返回 None。
    fn read_string(subkey: &str, value: &str) -> Option<String> {
        let subkey: Vec<u16> = subkey.encode_utf16().chain(Some(0)).collect();
        let value: Vec<u16> = value.encode_utf16().chain(Some(0)).collect();
        let mut bytes = 0u32;
        // 先问长度。失败说明键/值不存在，直接放弃。
        // SAFETY: 空数据指针是查询长度模式的约定用法。
        let status = unsafe {
            RegGetValueW(
                HKEY_CURRENT_USER,
                subkey.as_ptr(),
                value.as_ptr(),
                RRF_RT_REG_SZ,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                &mut bytes,
            )
        };
        if status != 0 || bytes == 0 {
            return None;
        }
        let mut buffer = vec![0u16; (bytes as usize).div_ceil(2)];
        // SAFETY: 缓冲区按刚查到的字节数分配。
        let status = unsafe {
            RegGetValueW(
                HKEY_CURRENT_USER,
                subkey.as_ptr(),
                value.as_ptr(),
                RRF_RT_REG_SZ,
                std::ptr::null_mut(),
                buffer.as_mut_ptr().cast(),
                &mut bytes,
            )
        };
        if status != 0 {
            return None;
        }
        let len = buffer
            .iter()
            .position(|ch| *ch == 0)
            .unwrap_or(buffer.len());
        let text = String::from_utf16_lossy(&buffer[..len]);
        let trimmed = text.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_owned())
        }
    }

    [
        r"Software\miHoYo\HYP\1_1\hk4e_cn",
        r"Software\Cognosphere\HYP\1_0\hk4e_global",
    ]
    .into_iter()
    .filter_map(|key| read_string(key, "GameInstallPath"))
    .map(std::path::PathBuf::from)
    .collect()
}

#[cfg(not(windows))]
fn registry_install_dirs() -> Vec<std::path::PathBuf> {
    Vec::new()
}

/// 找到游戏可执行文件。
///
/// 优先级与 Miliastra-Wonderland-Music 保持一致：
///   1. 显式指定的 --game-path（文件或目录）
///   2. 启动器注册表里的安装目录（国服 / 国际服）
///   3. 常见默认安装位置
///
/// 之前这里是三处硬编码路径，换个盘或非默认安装就找不到游戏。
pub(crate) fn locate_game(explicit: Option<&std::path::Path>) -> Result<std::path::PathBuf> {
    crate::game_path::resolve(explicit, registry_install_dirs())
}

/// metadata.bin 的位置（与程序同目录）。
fn locate_metadata() -> Result<std::path::PathBuf> {
    let exe_dir = std::env::current_exe()?;
    let dir = exe_dir.parent().unwrap_or(std::path::Path::new("."));
    let path = dir.join("metadata.bin");
    if path.is_file() {
        return Ok(path);
    }
    anyhow::bail!("未找到 metadata.bin（应与本程序同目录，来自 YaeAchievement 的 CDN）")
}

/// 注入用的 DLL。
fn locate_dll() -> Result<std::path::PathBuf> {
    let exe_dir = std::env::current_exe()?;
    let dir = exe_dir.parent().unwrap_or(std::path::Path::new("."));
    // 只认本项目自己的嗅探 DLL。上游实现已不再作为回退：
    // 它有多处已知缺陷（解压缓冲泄漏、类名缓冲越界、偏移计算错误）。
    let path = dir.join("MiliastraWonderlandChatBridge.Native.dll");
    if path.is_file() {
        return Ok(path);
    }
    anyhow::bail!("未找到嗅探 DLL MiliastraWonderlandChatBridge.Native.dll（应与本程序同目录）")
}
/// 找到并打开正在运行的游戏进程。
///
/// 附加模式不需要挂起游戏，也不会改动它的启动参数——比自行拉起更"安静"。
fn open_running_game() -> Option<u32> {
    use windows_sys::Win32::Foundation::CloseHandle;
    use windows_sys::Win32::System::Diagnostics::ToolHelp::{
        CreateToolhelp32Snapshot, PROCESSENTRY32W, Process32FirstW, Process32NextW,
        TH32CS_SNAPPROCESS,
    };

    let wanted = ["yuanshen.exe", "genshinimpact.exe"];
    // SAFETY: 标准进程快照调用。
    let snapshot = unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPPROCESS, 0) };
    if snapshot.is_null() || snapshot as isize == -1 {
        tracing::warn!("创建进程快照失败: {}", std::io::Error::last_os_error());
        return None;
    }
    // SAFETY: 零初始化 + 设置 dwSize 是快照 API 的约定。
    let mut entry: PROCESSENTRY32W = unsafe { std::mem::zeroed() };
    entry.dwSize = std::mem::size_of::<PROCESSENTRY32W>() as u32;
    // SAFETY: snapshot 有效，entry 尺寸正确。
    if unsafe { Process32FirstW(snapshot, &mut entry) } == 0 {
        // SAFETY: snapshot 来自快照 API。
        unsafe { CloseHandle(snapshot) };
        return None;
    }
    loop {
        let end = entry
            .szExeFile
            .iter()
            .position(|&c| c == 0)
            .unwrap_or(entry.szExeFile.len());
        let name = String::from_utf16_lossy(&entry.szExeFile[..end]).to_lowercase();
        if wanted.contains(&name.as_str()) {
            let pid = entry.th32ProcessID;
            // SAFETY: snapshot 来自快照 API。
            unsafe { CloseHandle(snapshot) };
            return Some(pid);
        }
        // SAFETY: snapshot 有效，entry 已初始化。
        if unsafe { Process32NextW(snapshot, &mut entry) } == 0 {
            break;
        }
    }
    // SAFETY: snapshot 来自快照 API。
    unsafe { CloseHandle(snapshot) };
    None
}

struct SuspendedProcess {
    process: windows_sys::Win32::Foundation::HANDLE,
    thread: windows_sys::Win32::Foundation::HANDLE,
    /// 进程 ID。CreateToolhelp32Snapshot 要 PID 而不是句柄。
    pid: u32,
    /// 是否由本程序启动。附加模式下为 false，不需要 ResumeThread。
    spawned_by_us: bool,
    started: std::cell::Cell<bool>,
}

// 清理边界：只有本轮创建且尚未成功恢复主线程的进程才会被终止。
// 成功恢复后，Ctrl-C/管道断开只关闭本地句柄，游戏由用户管理。
impl Drop for SuspendedProcess {
    fn drop(&mut self) {
        unsafe {
            if self.spawned_by_us
                && !self.started.get()
                && process_alive(self.process)
                && windows_sys::Win32::System::Threading::TerminateProcess(self.process, 1) == 0
            {
                tracing::error!(
                    "清理未完成启动的进程失败: {}",
                    std::io::Error::last_os_error()
                );
            }
            if !self.thread.is_null() {
                windows_sys::Win32::Foundation::CloseHandle(self.thread);
            }
            if !self.process.is_null() {
                windows_sys::Win32::Foundation::CloseHandle(self.process);
            }
        }
    }
}

fn spawn_suspended(exe: &std::path::Path) -> Result<SuspendedProcess> {
    use windows_sys::Win32::System::Threading::{
        CREATE_SUSPENDED, CreateProcessW, PROCESS_INFORMATION, STARTF_USESHOWWINDOW, STARTUPINFOW,
    };
    /// SW_SHOWNORMAL：要求窗口正常显示。
    const SW_SHOWNORMAL: u16 = 1;
    let mut cmdline: Vec<u16> = format!("\"{}\"", exe.display())
        .encode_utf16()
        .chain(Some(0))
        .collect();
    // SAFETY: 结构体已零初始化，句柄由 CreateProcessW 填写。
    unsafe {
        let mut si: STARTUPINFOW = std::mem::zeroed();
        si.cb = std::mem::size_of::<STARTUPINFOW>() as u32;
        // 必须显式要求正常显示窗口：本程序自身是以隐藏窗口启动的，
        // 而 dwFlags 不含 STARTF_USESHOWWINDOW 时子进程会用 SW_SHOWDEFAULT，
        // 于是继承父进程的隐藏状态——游戏在跑却完全没有窗口。
        si.dwFlags = STARTF_USESHOWWINDOW;
        si.wShowWindow = SW_SHOWNORMAL;
        // 显式指定交互式桌面。父进程是从计划任务/后台启动的，
        // 不指定时子进程可能落在非交互窗口站，导致窗口创建但不可见。
        let mut desktop: Vec<u16> = "WinSta0\\Default".encode_utf16().chain(Some(0)).collect();
        si.lpDesktop = desktop.as_mut_ptr();
        let mut pi: PROCESS_INFORMATION = std::mem::zeroed();
        let ok = CreateProcessW(
            std::ptr::null(),
            cmdline.as_mut_ptr(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            0,
            CREATE_SUSPENDED,
            std::ptr::null_mut(),
            std::ptr::null(),
            &si,
            &mut pi,
        );
        if ok == 0 {
            return Err(std::io::Error::last_os_error()).context("启动游戏失败");
        }
        Ok(SuspendedProcess {
            process: pi.hProcess,
            thread: pi.hThread,
            pid: pi.dwProcessId,
            spawned_by_us: true,
            started: std::cell::Cell::new(false),
        })
    }
}

fn resume_thread(proc: &SuspendedProcess) -> Result<()> {
    use windows_sys::Win32::System::Threading::ResumeThread;
    // 附加模式没有主线程句柄，也不需要恢复。
    if proc.thread.is_null() {
        return Ok(());
    }
    // SAFETY: 线程句柄来自 CreateProcessW，调用时仍有效。
    if proc.started.get() {
        return Ok(());
    }
    let previous = unsafe { ResumeThread(proc.thread) };
    if previous == u32::MAX {
        return Err(std::io::Error::last_os_error()).context("恢复游戏线程失败");
    }
    proc.started.set(true);
    Ok(())
}

/// 把 DLL 注入挂起的游戏进程，并调用它的 YaeMain 入口。
///
/// 步骤对齐 YaeAchievement 的 `GameProcess.LoadLibrary`：
///   1. VirtualAllocEx + WriteProcessMemory 写入 DLL 路径
///   2. CreateRemoteThread(LoadLibraryW) 让目标进程加载 DLL
///   3. 枚举模块拿到 DLL 在目标进程里的基址
///   4. 本地 LoadLibraryEx(DONT_RESOLVE_DLL_REFERENCES) 拿到 YaeMain 的 RVA
///   5. CreateRemoteThread(基址 + RVA) 执行入口
///
/// 注意：LoadLibraryEx 用 DONT_RESOLVE_DLL_REFERENCES 只是为了算 RVA，
/// 不会真正初始化 DLL，因此不影响目标进程。
struct OwnedHandle(windows_sys::Win32::Foundation::HANDLE);
impl Drop for OwnedHandle {
    fn drop(&mut self) {
        unsafe {
            windows_sys::Win32::Foundation::CloseHandle(self.0);
        }
    }
}
struct LocalModule(windows_sys::Win32::Foundation::HMODULE);
impl Drop for LocalModule {
    fn drop(&mut self) {
        unsafe {
            windows_sys::Win32::Foundation::FreeLibrary(self.0);
        }
    }
}

fn wait_remote_thread(thread: &OwnedHandle, cancelled: &AtomicBool) -> Result<()> {
    use windows_sys::Win32::Foundation::{WAIT_OBJECT_0, WAIT_TIMEOUT};
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        check_cancelled(cancelled)?;
        match unsafe { windows_sys::Win32::System::Threading::WaitForSingleObject(thread.0, 50) } {
            WAIT_OBJECT_0 => return Ok(()),
            WAIT_TIMEOUT if std::time::Instant::now() < deadline => {}
            WAIT_TIMEOUT => anyhow::bail!("LoadLibraryW 30 秒超时，本轮终止"),
            _ => return Err(std::io::Error::last_os_error()).context("等待远程线程失败"),
        }
    }
}

struct RemoteAllocation<'a> {
    proc: &'a SuspendedProcess,
    address: *mut std::ffi::c_void,
    in_use: bool,
}
impl Drop for RemoteAllocation<'_> {
    fn drop(&mut self) {
        // 等待失败/取消时远程线程可能仍读取参数。保留内存，随后由未启动进程的
        // Drop 终止进程并让内核回收；绝不能提前 VirtualFreeEx 或继续注入。
        if !self.in_use {
            unsafe {
                windows_sys::Win32::System::Memory::VirtualFreeEx(
                    self.proc.process,
                    self.address,
                    0,
                    windows_sys::Win32::System::Memory::MEM_RELEASE,
                );
            }
        }
    }
}

fn load_library_remote(
    proc: &SuspendedProcess,
    dll: &std::path::Path,
    cancelled: &AtomicBool,
) -> Result<()> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::System::LibraryLoader::{GetModuleHandleW, GetProcAddress};
    use windows_sys::Win32::System::Memory::{
        MEM_COMMIT, MEM_RESERVE, PAGE_READWRITE, VirtualAllocEx,
    };
    use windows_sys::Win32::System::Threading::{CreateRemoteThread, GetExitCodeThread};
    check_cancelled(cancelled)?;
    let path_wide: Vec<u16> = dll.as_os_str().encode_wide().chain(Some(0)).collect();
    let bytes = path_wide.len() * 2;
    let address = unsafe {
        VirtualAllocEx(
            proc.process,
            std::ptr::null(),
            bytes,
            MEM_RESERVE | MEM_COMMIT,
            PAGE_READWRITE,
        )
    };
    anyhow::ensure!(
        !address.is_null(),
        "VirtualAllocEx 失败: {}",
        std::io::Error::last_os_error()
    );
    let mut remote = RemoteAllocation {
        proc,
        address,
        in_use: false,
    };
    let mut written = 0;
    let ok = unsafe {
        windows_sys::Win32::System::Diagnostics::Debug::WriteProcessMemory(
            proc.process,
            address,
            path_wide.as_ptr().cast(),
            bytes,
            &mut written,
        )
    };
    anyhow::ensure!(
        ok != 0 && written == bytes,
        "WriteProcessMemory 失败: {}",
        std::io::Error::last_os_error()
    );
    let kernel32: Vec<u16> = "kernel32.dll".encode_utf16().chain(Some(0)).collect();
    let kernel = unsafe { GetModuleHandleW(kernel32.as_ptr()) };
    anyhow::ensure!(!kernel.is_null(), "找不到 kernel32.dll");
    let load_library = unsafe { GetProcAddress(kernel, c"LoadLibraryW".as_ptr().cast()) }
        .context("找不到 LoadLibraryW")?;
    let thread = unsafe {
        CreateRemoteThread(
            proc.process,
            std::ptr::null(),
            0,
            Some(std::mem::transmute::<
                unsafe extern "system" fn() -> isize,
                unsafe extern "system" fn(*mut std::ffi::c_void) -> u32,
            >(load_library)),
            address,
            0,
            std::ptr::null_mut(),
        )
    };
    anyhow::ensure!(
        !thread.is_null(),
        "CreateRemoteThread(LoadLibraryW) 失败: {}",
        std::io::Error::last_os_error()
    );
    let thread = OwnedHandle(thread);
    remote.in_use = true;
    wait_remote_thread(&thread, cancelled)?;
    remote.in_use = false;
    let mut exit_code = 0;
    anyhow::ensure!(
        unsafe { GetExitCodeThread(thread.0, &mut exit_code) } != 0 && exit_code != 0,
        "LoadLibraryW 失败"
    );
    find_remote_module(proc.pid, dll)?;
    Ok(())
}

fn inject_dll(
    proc: &SuspendedProcess,
    dll: &std::path::Path,
    cancelled: &AtomicBool,
) -> Result<()> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::System::LibraryLoader::{
        DONT_RESOLVE_DLL_REFERENCES, GetProcAddress, LoadLibraryExW,
    };
    use windows_sys::Win32::System::Threading::CreateRemoteThread;
    load_library_remote(proc, dll, cancelled)?;
    check_cancelled(cancelled)?;
    let path_wide: Vec<u16> = dll.as_os_str().encode_wide().chain(Some(0)).collect();
    // 3) 找到 DLL 在目标进程的基址。
    let base = find_remote_module(proc.pid, dll)?;

    // 4) 本地加载同一 DLL 只为算 YaeMain 的 RVA。
    let local = unsafe {
        LoadLibraryExW(
            path_wide.as_ptr(),
            std::ptr::null_mut(),
            DONT_RESOLVE_DLL_REFERENCES,
        )
    };
    if local.is_null() {
        return Err(std::io::Error::last_os_error()).context("本地 LoadLibraryEx 失败");
    }
    let local = LocalModule(local);
    let entry: Vec<u8> = b"YaeMain\0".to_vec();
    // SAFETY: local 有效，entry 是 NUL 结尾。
    let main = unsafe { GetProcAddress(local.0, entry.as_ptr()) }
        .ok_or_else(|| anyhow::anyhow!("DLL 缺少 YaeMain 导出"))?;
    let rva = main as usize - local.0 as usize;
    let remote_main = (base as usize)
        .checked_add(rva)
        .ok_or_else(|| anyhow::anyhow!("计算远程入口地址溢出"))? as usize;

    // 5) 远程调用 YaeMain(0)。
    // SAFETY: remote_main 指向目标进程里的 YaeMain。
    let main_thread = unsafe {
        CreateRemoteThread(
            proc.process,
            std::ptr::null(),
            0,
            Some(std::mem::transmute::<
                usize,
                unsafe extern "system" fn(*mut std::ffi::c_void) -> u32,
            >(remote_main)),
            std::ptr::null_mut(),
            0,
            std::ptr::null_mut(),
        )
    };
    if main_thread.is_null() {
        return Err(std::io::Error::last_os_error()).context("CreateRemoteThread(YaeMain) 失败");
    }
    let _main_thread = OwnedHandle(main_thread);
    Ok(())
}

/// 进程是否还活着。
/// 注入会话断开时判定该报哪种错误。
///
/// 游戏还活着 = 链路彻底失效且无法重新注入，必须让上游知道要人工重启游戏；
/// 游戏已退出 = 正常结束，上游下一轮可以直接重新拉起。
fn session_end_error(proc: &SuspendedProcess, reason: &str) -> anyhow::Error {
    crate::diag::note("pipe_closed", "host observed peer close");
    if process_alive(proc.process) {
        let code = crate::exit_code::code_of(crate::exit_code::Kind::SessionLostGameAlive);
        tracing::error!(
            "{reason}，但游戏进程仍在运行 —— 外部聊天源已失效，必须重启游戏才能恢复。退出码 {code}"
        );
        crate::exit_code::Marked(crate::exit_code::Kind::SessionLostGameAlive).into()
    } else {
        tracing::info!("{reason}，游戏已退出，结束会话");
        crate::exit_code::Marked(crate::exit_code::Kind::GameExited).into()
    }
}

fn process_alive(process: windows_sys::Win32::Foundation::HANDLE) -> bool {
    use windows_sys::Win32::Foundation::WAIT_TIMEOUT;
    use windows_sys::Win32::System::Threading::WaitForSingleObject;
    // SAFETY: process 句柄有效。
    unsafe { WaitForSingleObject(process, 0) == WAIT_TIMEOUT }
}

/// 枚举目标进程模块，找到指定 DLL 的加载基址。
fn find_remote_module(pid: u32, dll: &std::path::Path) -> Result<*mut std::ffi::c_void> {
    use windows_sys::Win32::System::Diagnostics::ToolHelp::{
        CreateToolhelp32Snapshot, MODULEENTRY32W, Module32FirstW, Module32NextW, TH32CS_SNAPMODULE,
    };

    let target = dll.to_string_lossy().to_lowercase();
    // SAFETY: 标准模块快照调用。
    let snapshot = unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPMODULE, pid) };
    if snapshot.is_null() || snapshot as isize == -1 {
        return Err(std::io::Error::last_os_error()).context("CreateToolhelp32Snapshot 失败");
    }
    // SAFETY: 结构体零初始化是 CreateToolhelp32Snapshot 的约定用法。
    let mut entry: MODULEENTRY32W = unsafe { std::mem::zeroed() };
    entry.dwSize = std::mem::size_of::<MODULEENTRY32W>() as u32;
    // SAFETY: snapshot 有效，entry 尺寸正确。
    let ok = unsafe { Module32FirstW(snapshot, &mut entry) };
    if ok == 0 {
        // SAFETY: snapshot 来自快照 API。
        unsafe { windows_sys::Win32::Foundation::CloseHandle(snapshot) };
        return Err(std::io::Error::last_os_error()).context("Module32FirstW 失败");
    }
    loop {
        let end = entry
            .szExePath
            .iter()
            .position(|&c| c == 0)
            .unwrap_or(entry.szExePath.len());
        let path = String::from_utf16_lossy(&entry.szExePath[..end]).to_lowercase();
        if path == target {
            let base = entry.modBaseAddr.cast();
            // SAFETY: snapshot 来自快照 API。
            unsafe { windows_sys::Win32::Foundation::CloseHandle(snapshot) };
            return Ok(base);
        }
        // SAFETY: snapshot 有效，entry 已初始化。
        if unsafe { Module32NextW(snapshot, &mut entry) } == 0 {
            break;
        }
    }
    // SAFETY: snapshot 来自快照 API。
    unsafe { windows_sys::Win32::Foundation::CloseHandle(snapshot) };
    anyhow::bail!("目标进程里找不到模块: {}", dll.display())
}

#[cfg(test)]
mod tests {
    use super::*;
    use windows_sys::Win32::Foundation::WAIT_OBJECT_0;
    use windows_sys::Win32::System::Threading::{OpenProcess, WaitForSingleObject};

    fn suspended_cmd() -> SuspendedProcess {
        let root = std::env::var_os("SystemRoot").expect("SystemRoot");
        spawn_suspended(
            &std::path::PathBuf::from(root)
                .join("System32")
                .join("cmd.exe"),
        )
        .unwrap()
    }

    #[test]
    fn unstarted_child_is_terminated_by_guard() {
        let proc = suspended_cmd();
        // 独立观察句柄在 guard 关闭创建者句柄之后仍可检查退出。
        let observer = OwnedHandle(unsafe { OpenProcess(0x0010_0000, 0, proc.pid) });
        assert!(!observer.0.is_null());
        drop(proc);
        assert_eq!(
            unsafe { WaitForSingleObject(observer.0, 5000) },
            WAIT_OBJECT_0
        );
    }

    #[test]
    fn pre_cancelled_pipe_connection_does_not_consume_connection() {
        let name: Vec<u16> = format!(
            r"\\.\pipe\miliastra-test-{}-{}",
            std::process::id(),
            rand::random::<u64>()
        )
        .encode_utf16()
        .chain(Some(0))
        .collect();
        let access = windows_sys::Win32::Storage::FileSystem::PIPE_ACCESS_DUPLEX;
        let pipe = create_named_pipe(&name, access).unwrap();
        // FIRST_PIPE_INSTANCE 检测冲突不需要客户端 CreateFile，因此连接仍留给DLL。
        assert!(create_named_pipe(&name, access).is_err());
        let proc = suspended_cmd();
        // 已取消时不进入阻塞的 ConnectNamedPipe。
        let cancelled = AtomicBool::new(true);
        let start = std::time::Instant::now();
        assert!(connect_pipe(&pipe, &proc, &cancelled).is_err());
        assert!(start.elapsed() < std::time::Duration::from_secs(2));
    }
    #[test]
    fn initialization_wait_and_remote_thread_wait_observe_cancellation() {
        use windows_sys::Win32::System::Threading::CreateEventW;
        let event = OwnedHandle(unsafe { CreateEventW(std::ptr::null(), 1, 0, std::ptr::null()) });
        assert!(!event.0.is_null());
        let cancelled = AtomicBool::new(true);
        let start = std::time::Instant::now();
        assert!(wait_remote_thread(&event, &cancelled).is_err());
        assert!(start.elapsed() < std::time::Duration::from_secs(1));
        let proc = suspended_cmd();
        assert!(
            check_session(
                &proc,
                &cancelled,
                std::time::Instant::now() + std::time::Duration::from_secs(60)
            )
            .is_err()
        );
        assert!(check_session(&proc, &AtomicBool::new(false), std::time::Instant::now()).is_err());
    }
}
