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
//!
//! reattach（重连）模式：DLL 侧已实现断线重连（退避 1s→30s、握手幂等、
//! 连接代际隔离）。宿主必须配合，否则重连是空转：
//!   - 游戏已在运行时不再直接退出，而是建管道等 DLL 主动连上来；
//!   - 收包循环读到 EOF 后不退出，回到同样的等待；
//!   - 等待窗口耗尽且游戏还活着 → 退出码 4；游戏已退出 → 退出码 0。
//!
//! 安全边界：reattach 只创建命名管道并等待连接。
//! 绝不 CreateProcess、绝不注入 DLL、绝不 TerminateProcess。

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};

use crate::extract;
use crate::packet_control::{CONTROL_PIPE_NAME, DEBUG_PIPE_NAME, PacketControl};
use crate::state::ChatState;
use crate::{InitialAction, MAX_REATTACH_ROUNDS, PeerCloseAction, SessionStatus};

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

/// 启动注入/reattach，并持续接收数据。
///
/// 一次调用可能横跨多个会话：首次注入成功后若对端关闭而游戏仍在运行，
/// 会回到“等待 DLL 重连”而不是退出（见 `supervise`）。
/// 阻塞工作通过取消标志和短周期轮询退出。
#[allow(clippy::too_many_arguments)]
pub async fn run(
    chat: Arc<ChatState>,
    connected: Arc<AtomicBool>,
    packets: Arc<PacketControl>,
    session: Arc<SessionStatus>,
    auto_launch: bool,
    spawn_only: bool,
    lib_only: bool,
    game_path: Option<std::path::PathBuf>,
    reattach_window: Duration,
    cancelled: Arc<AtomicBool>,
) -> Result<()> {
    tokio::task::spawn_blocking(move || {
        let result = supervise(
            &chat,
            &connected,
            &packets,
            &session,
            auto_launch,
            spawn_only,
            lib_only,
            game_path.as_deref(),
            reattach_window,
            &cancelled,
        );
        connected.store(false, Ordering::Relaxed);
        crate::diag::dll_connected(false);
        session.mark_detached();
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

/// 会话总控：首次注入 / reattach 等待 → 收包 → EOF 后按策略重连或退出。
#[allow(clippy::too_many_arguments)]
fn supervise(
    chat: &ChatState,
    connected: &AtomicBool,
    packets: &Arc<PacketControl>,
    session: &SessionStatus,
    auto_launch: bool,
    spawn_only: bool,
    lib_only: bool,
    game_path: Option<&std::path::Path>,
    reattach_window: Duration,
    cancelled: &AtomicBool,
) -> Result<()> {
    check_cancelled(cancelled)?;
    if !auto_launch {
        return Ok(());
    }
    let exe = locate_game(game_path)?;

    // 起点判断：游戏已在运行时不再直接退出（旧行为退出码 3），而是进入
    // reattach 等 DLL 主动重连。reattach 只建管道：绝不 CreateProcess、
    // 绝不注入 DLL、绝不 TerminateProcess。
    let running = open_running_game();
    let action = crate::initial_action(running, reattach_window);
    if action == InitialAction::BailAlreadyRunning {
        let pid = running.unwrap_or(0);
        let code = crate::exit_code::code_of(crate::exit_code::Kind::GameAlreadyRunning);
        tracing::error!(
            "游戏已在运行 (pid={pid})，且 reattach 已禁用（--reattach-seconds 0）。\
             请先退出游戏，本服务会自己拉起它。退出码 {code}（等玩家退出即可，无需人工修复）"
        );
        return Err(crate::exit_code::Marked(crate::exit_code::Kind::GameAlreadyRunning).into());
    }

    // --spawn-only / --lib-only 是首次注入的调试开关；reattach 下没有东西可 spawn。
    if action == InitialAction::Inject && (spawn_only || lib_only) {
        return debug_spawn_path(&exe, lib_only, cancelled);
    }

    let rva = resolve_rva(&exe)?;
    let mut rounds: u32 = 0;
    let peer: SuspendedProcess;
    let mut stream: std::fs::File;
    let mut aux: AuxPipes;

    match action {
        InitialAction::Inject => {
            let dll = locate_dll()?;
            tracing::info!("启动并注入 {} -> {}", exe.display(), dll.display());
            let (proc, pipe_stream, pipes) = inject_and_connect(&exe, &dll, packets, cancelled)?;
            peer = proc;
            stream = pipe_stream;
            aux = pipes;
        }
        InitialAction::Reattach => {
            let pid = running.expect("reattach 必须有已运行的游戏 pid");
            tracing::warn!(
                "游戏已在运行 (pid={pid})：进入 reattach 等待（窗口 {}s）。\
                 不重新启动、不注入、不终止游戏；等 DLL 主动重连主通道",
                reattach_window.as_secs()
            );
            peer = attach_running_process(pid);
            rounds += 1;
            session.mark_reattach_waiting();
            match wait_for_reattach(&peer, packets, cancelled, reattach_window) {
                Ok((pipe_stream, pipes)) => {
                    stream = pipe_stream;
                    aux = pipes;
                }
                Err(failure) => return reattach_failure_to_result(failure, &peer),
            }
        }
        InitialAction::BailAlreadyRunning => unreachable!("已在上面处理"),
    }

    loop {
        // 连接就绪：标记 attached，进入收包循环。
        connected.store(true, Ordering::Relaxed);
        crate::diag::dll_connected(true);
        session.mark_attached();
        tracing::info!("DLL 已连接，开始收包");
        let end = serve_connected(&peer, &mut stream, &rva, chat, cancelled)?;

        // 会话结束：先撤销 attached，再关闭全部管道实例。必须关闭旧实例，
        // 否则同名管道仍被占用，下一次 CreateNamedPipeW(FIRST_PIPE_INSTANCE) 失败。
        connected.store(false, Ordering::Relaxed);
        crate::diag::dll_connected(false);
        drop(stream);
        aux.retire(packets);

        match end {
            SessionEnd::Finished(reason) => {
                // DLL 主动退出 / 协议错误 / 游戏退出：与旧实现一致，正常结束（退出码 0）。
                tracing::info!("会话结束：{reason}");
                return Ok(());
            }
            SessionEnd::PeerClosed(reason) => {
                if !peer.alive() {
                    crate::diag::note("pipe_closed", reason);
                    tracing::info!("{reason}，游戏已退出，结束会话");
                    return Err(crate::exit_code::Marked(crate::exit_code::Kind::GameExited).into());
                }
                let attempts_left = MAX_REATTACH_ROUNDS.saturating_sub(rounds);
                match crate::peer_close_action(true, reattach_window, attempts_left) {
                    PeerCloseAction::WaitReconnect => {
                        crate::diag::note("pipe_closed", reason);
                        rounds += 1;
                        session.mark_reattach_waiting();
                        tracing::warn!(
                            "{reason}，游戏仍在运行：回到 reattach 等待（第 {rounds}/{MAX_REATTACH_ROUNDS} 轮，窗口 {}s）。\
                             不会重启游戏，也不会重新注入",
                            reattach_window.as_secs()
                        );
                        match wait_for_reattach(&peer, packets, cancelled, reattach_window) {
                            Ok((pipe_stream, pipes)) => {
                                stream = pipe_stream;
                                aux = pipes;
                            }
                            Err(failure) => return reattach_failure_to_result(failure, &peer),
                        }
                    }
                    PeerCloseAction::SessionLost => {
                        // 游戏还活着但已无重连机会：退出码 4，上游须人工重启游戏。
                        return Err(session_end_error(&peer, reason));
                    }
                    PeerCloseAction::GameExited => {
                        return Err(
                            crate::exit_code::Marked(crate::exit_code::Kind::GameExited).into()
                        );
                    }
                }
            }
        }
    }
}

/// 首次注入的调试路径（--spawn-only / --lib-only），行为与旧实现一致。
fn debug_spawn_path(exe: &std::path::Path, lib_only: bool, cancelled: &AtomicBool) -> Result<()> {
    let proc = spawn_suspended(exe)?;
    if lib_only {
        load_library_remote(&proc, &locate_dll()?, cancelled)?;
    }
    check_cancelled(cancelled)?;
    resume_thread(&proc)?;
    wait_for_game(&proc, cancelled)
}

/// 首次注入路径：建管道 → spawn_suspended → 注入 DLL → 等 DLL 连接。
///
/// 顺序与旧实现相同；连接等待换成带截止时间的版本，让取消/游戏退出及时收尾。
fn inject_and_connect(
    exe: &std::path::Path,
    dll: &std::path::Path,
    packets: &Arc<PacketControl>,
    cancelled: &AtomicBool,
) -> Result<(SuspendedProcess, std::fs::File, AuxPipes)> {
    use std::os::windows::io::FromRawHandle;

    let mut pipe = create_pipe()?;
    let mut aux = AuxPipes::open(packets);
    if let Err(error) = check_cancelled(cancelled) {
        aux.retire(packets);
        return Err(error);
    }
    let proc = match spawn_suspended(exe) {
        Ok(proc) => proc,
        Err(error) => {
            aux.retire(packets);
            return Err(error);
        }
    };
    if let Err(error) = inject_dll(&proc, dll, cancelled) {
        aux.retire(packets);
        return Err(error);
    }
    // DLL 连接管道后才会继续初始化；旧实现的等待上限是 30s。
    let deadline = Instant::now() + Duration::from_secs(30);
    if let Err(failure) = connect_pipe_until(
        &mut pipe,
        PIPE_NAME,
        CLIENT_ACCESS_FULL,
        &proc,
        cancelled,
        deadline,
    ) {
        aux.retire(packets);
        return Err(connect_failure_to_error(failure));
    }
    // SAFETY: pipe 是刚创建并已连接的句柄，所有权转交 File 后不再用裸句柄。
    let stream = unsafe { std::fs::File::from_raw_handle(pipe.0 as *mut _) };
    std::mem::forget(pipe);
    Ok((proc, stream, aux))
}

/// 一次已建立会话的结束原因。
enum SessionEnd {
    /// 对端断开：读到 EOF/ERROR_BROKEN_PIPE，或回执（ACK/握手）写失败。
    /// 游戏还活着时交给 peer_close_action 决定回到 reattach 还是收尾。
    PeerClosed(&'static str),
    /// 会话正常结束：DLL 主动退出 / 协议错误 / 检测到游戏退出。
    Finished(&'static str),
}

/// 收包循环；返回会话结束原因而不是直接退出，交给 supervise 决定是否重连。
fn serve_connected(
    proc: &SuspendedProcess,
    stream: &mut std::fs::File,
    rva: &crate::metadata::MethodRva,
    chat: &ChatState,
    cancelled: &AtomicBool,
) -> Result<SessionEnd> {
    use std::io::{Read, Write};

    // 管道保持阻塞模式：PIPE_NOWAIT 已废弃，实测会让 ReadFile 返回假的 EOF。
    // 非阻塞检查改用 PeekNamedPipe：高压时先自旋接下一包（见 PIPE_SPIN_WINDOW），
    // 静默时才回到每 50ms 检查取消和进程。
    tracing::info!("等待 DLL 发送首帧...");

    let mut buffered: Vec<u8> = Vec::new();
    let mut frames_seen: u64 = 0;
    let mut last_report = Instant::now();
    // 最近一次读到数据的时刻：只有刚收过包才自旋等下一包，静默期不额外占 CPU。
    let mut last_data_at = Instant::now() - Duration::from_secs(1);
    let init_deadline = Instant::now() + Duration::from_secs(60);
    loop {
        check_session(proc, cancelled, init_deadline)?;
        if !proc.alive() {
            return Ok(SessionEnd::Finished("游戏已退出"));
        }
        // 先探测：无数据就短暂等待，有数据立即读（阻塞读此时不会卡）。
        match poll_pipe(stream)? {
            PipeState::Data => {}
            PipeState::Empty => {
                // 有积压时下一包在回执后几微秒内就到；先自旋接住它，
                // 不能每包都退回去吃一次 50ms 空闲轮询（见 PIPE_SPIN_WINDOW）。
                if last_data_at.elapsed() < ACTIVE_PIPE_WINDOW {
                    let hit = wait_for_pipe_data(stream)?;
                    crate::diag::spin(hit);
                    if hit {
                        last_data_at = Instant::now();
                        continue;
                    }
                }
                crate::diag::idle_poll();
                poll_pause();
                continue;
            }
            PipeState::Closed => return Ok(SessionEnd::PeerClosed("对端已关闭")),
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
            return Ok(SessionEnd::PeerClosed("管道返回 0 字节"));
        }
        last_data_at = Instant::now();
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
                    return Ok(SessionEnd::Finished("管道协议错误"));
                }
            };
            buffered.drain(..1 + used);
            frames_seen += 1;
            // 包帧的 ACK 只表示「保持订阅」，与解析结果无关：先回执让 DLL
            // 立刻写下一包，把 dispatch 的耗时移出往返路径。
            let reply = crate::pipe::reply(&frame, rva);
            let is_packet = matches!(&frame, crate::pipe::Frame::Packet { .. });
            if is_packet && !reply.is_empty() {
                let ack_started = Instant::now();
                if let Err(error) = stream.write_all(&reply) {
                    // 写 ACK 失败意味着对端可能已断开或正在卡顿（宿主卡顿恢复时
                    // 最常见的断开路径）。必须与读到 EOF 同等对待，交给
                    // supervise 的 peer_close_action 决定重连还是退出；绝不能
                    // 用 ? 冒泡成进程级错误，否则 reattach 在这条路径上失效。
                    tracing::warn!("写 ACK 失败（对端可能已断开）：{error}");
                    return Ok(SessionEnd::PeerClosed("写 ACK 失败"));
                }
                crate::diag::ack(ack_started.elapsed().as_micros() as u64);
            }
            if let crate::pipe::Frame::Packet { cmd, payload } = &frame {
                crate::diag::packet(*cmd, payload.len());
                let dispatch_started = Instant::now();
                dispatch(chat, *cmd, payload);
                crate::diag::dispatch(dispatch_started.elapsed().as_micros() as u64);
            }
            if last_report.elapsed() >= Duration::from_secs(5) {
                tracing::info!("已收帧 {frames_seen} 个，缓冲 {} 字节", buffered.len());
                last_report = Instant::now();
            }
            if frame == crate::pipe::Frame::ResumeMainThread {
                if proc.spawned_by_us {
                    for _ in 0..30 {
                        check_session(proc, cancelled, init_deadline)?;
                        poll_pause();
                    }
                    resume_thread(proc)?;
                    tracing::info!("已恢复游戏主线程");
                } else {
                    // reattach：游戏主线程从未被挂起，恢复请求只是 DLL 初始化
                    // 流程的幂等重放；直接忽略，不等待、不碰游戏。
                    tracing::debug!("reattach 会话收到 ResumeMainThread，忽略（主线程未被挂起）");
                }
            }
            // 其余帧（握手/退出）的回执要等对应动作完成后再写。
            if !is_packet
                && !reply.is_empty()
                && let Err(error) = stream.write_all(&reply)
            {
                // 同 ACK：写失败按对端断开处理，回到 reattach 等待而不是退出。
                tracing::warn!("写握手回执失败（对端可能已断开）：{error}");
                return Ok(SessionEnd::PeerClosed("写握手回执失败"));
            }
            if frame == crate::pipe::Frame::Exit {
                return Ok(SessionEnd::Finished("DLL 请求退出"));
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

impl PipeHandle {
    /// 提前关闭实例句柄（幂等）；Drop 随后不会再关闭。
    ///
    /// 用途：连接等待在“dummy 客户端也唤不醒阻塞的 ConnectNamedPipe”时
    /// 用关闭句柄做最后兜底（见 wait_connect_thread）。
    fn close_now(&mut self) {
        if !self.0.is_null() && self.0 as isize != -1 {
            // SAFETY: 句柄由 CreateNamedPipeW 返回且只关闭一次（关闭后置空）。
            unsafe { windows_sys::Win32::Foundation::CloseHandle(self.0) };
            self.0 = std::ptr::null_mut();
        }
    }
}

impl Drop for PipeHandle {
    fn drop(&mut self) {
        // 不实现 Drop 会泄漏管道实例：名字一直被占用，下一次 create_pipe 永远
        // 拿不到实例（ERROR_PIPE_BUSY），最后把自己锁死。
        self.close_now();
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

/// 主通道客户端需要的访问权限：双工管道，读写都要。
const CLIENT_ACCESS_FULL: u32 =
    windows_sys::Win32::Foundation::GENERIC_READ | windows_sys::Win32::Foundation::GENERIC_WRITE;

/// 带截止时间的连接等待失败原因。
enum ConnectFailure {
    /// 窗口耗尽，客户端仍未连接。
    Timeout,
    /// 等待期间游戏进程退出。
    GameExited,
    /// Ctrl-C / 取消。
    Cancelled,
    /// 其它错误（创建/连接失败）。
    Failed(anyhow::Error),
}

/// 以客户端身份连接一条正在监听的命名管道；失败返回 None。
///
/// 用途只有两个，都是本机短连接，不会向对端发业务数据：
///   1. 解除阻塞的 ConnectNamedPipe（取消/超时路径）；
///   2. 唤醒 debug/control 的等待线程，让它们释放同名管道实例。
fn open_pipe_client(name: &str, access: u32) -> Option<windows_sys::Win32::Foundation::HANDLE> {
    use windows_sys::Win32::Storage::FileSystem::{CreateFileW, OPEN_EXISTING};
    let path = pipe_path_named(name);
    // SAFETY: path 以 NUL 结尾。
    let handle = unsafe {
        CreateFileW(
            path.as_ptr(),
            access,
            0,
            std::ptr::null_mut(),
            OPEN_EXISTING,
            0,
            std::ptr::null_mut(),
        )
    };
    if handle.is_null() || handle as isize == -1 {
        None
    } else {
        Some(handle)
    }
}

/// 连接并立刻断开一条本机命名管道，用于唤醒等待中的服务端线程。
fn wake_pipe_client(name: &str, access: u32) {
    if let Some(handle) = open_pipe_client(name, access) {
        // SAFETY: handle 由 CreateFileW 返回。
        unsafe { windows_sys::Win32::Foundation::CloseHandle(handle) };
    }
}

/// 等连接线程退出；dummy 客户端唤不醒时关闭实例句柄兜底。
///
/// 两次等待都是有界的（各 2s）：关闭句柄之后也不做无超时 join。
/// 万一线程仍卡在未文档化的阻塞语义里，宁可放弃 join（分离线程）并 warn，
/// 也不能让会话收尾/进程退出挂死；句柄已关闭，实例名不会被它继续占着。
fn wait_connect_thread(waiter: std::thread::JoinHandle<()>, pipe: &mut PipeHandle) {
    for _ in 0..200 {
        if waiter.is_finished() {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    if !waiter.is_finished() {
        tracing::warn!("dummy 客户端未能解除 ConnectNamedPipe 阻塞，改用关闭句柄兜底");
        pipe.close_now();
        // 关句柄后仍要有界：给内核一点时间让阻塞调用返回。
        for _ in 0..200 {
            if waiter.is_finished() {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }
    if waiter.is_finished() {
        let _ = waiter.join();
    } else {
        tracing::warn!("连接等待线程未在兜底窗口内退出，放弃 join（线程分离，句柄已关闭）");
        drop(waiter);
    }
}

/// 在截止时间内等待客户端连接主通道。
///
/// ConnectNamedPipe 没有超时参数，所以放到辅助线程里阻塞，主线程每 50ms
/// 检查一次取消条件（Ctrl-C / 游戏退出 / 截止时间）。取消时用一个本地
/// dummy 客户端连接正在监听的实例来解除阻塞——比从别的线程关闭阻塞中的
/// 句柄更符合文档语义（后者只是兜底）。
///
/// 边界：截止瞬间真实客户端也可能抢先连上。此时 dummy 会失败，连接线程
/// 报告成功，这里按成功处理，避免把已经就绪的 DLL 连接丢掉。
fn connect_pipe_until(
    pipe: &mut PipeHandle,
    name: &str,
    client_access: u32,
    proc: &SuspendedProcess,
    cancelled: &AtomicBool,
    deadline: Instant,
) -> std::result::Result<(), ConnectFailure> {
    use std::sync::mpsc::{RecvTimeoutError, channel};
    use windows_sys::Win32::System::Pipes::ConnectNamedPipe;

    // 先同步检查一次：已取消/已超时就不进入 ConnectNamedPipe，
    // 避免 dummy 客户端把本该留给 DLL 的连接消费掉。
    if cancelled.load(Ordering::Acquire) {
        return Err(ConnectFailure::Cancelled);
    }
    if !proc.alive() {
        return Err(ConnectFailure::GameExited);
    }
    if Instant::now() >= deadline {
        return Err(ConnectFailure::Timeout);
    }

    // 裸句柄不是 Send：编码成整数交给连接线程（与 AuxPipes 的做法一致）。
    let raw = pipe.0 as usize;
    let (tx, rx) = channel();
    let waiter = std::thread::Builder::new()
        .name("mwc-pipe-connect".to_owned())
        .spawn(move || {
            // SAFETY: raw 在连接线程结束前不会被关闭（调用方先 join 再 drop）。
            let ok = unsafe { ConnectNamedPipe(raw as *mut _, std::ptr::null_mut()) };
            let result = if ok != 0 {
                Ok(())
            } else {
                let error = std::io::Error::last_os_error();
                match error.raw_os_error() {
                    // ERROR_PIPE_CONNECTED：客户端已经连上。
                    Some(535) => Ok(()),
                    _ => Err(error.to_string()),
                }
            };
            let _ = tx.send(result);
        })
        .map_err(|error| {
            ConnectFailure::Failed(anyhow::Error::from(error).context("启动管道连接线程失败"))
        })?;

    loop {
        match rx.recv_timeout(Duration::from_millis(50)) {
            Ok(Ok(())) => {
                let _ = waiter.join();
                return Ok(());
            }
            Ok(Err(message)) => {
                let _ = waiter.join();
                return Err(ConnectFailure::Failed(anyhow::anyhow!(
                    "等待管道连接失败：{message}"
                )));
            }
            Err(RecvTimeoutError::Disconnected) => {
                let _ = waiter.join();
                return Err(ConnectFailure::Failed(anyhow::anyhow!(
                    "等待管道连接的线程异常退出"
                )));
            }
            Err(RecvTimeoutError::Timeout) => {
                let failure = if cancelled.load(Ordering::Acquire) {
                    Some(ConnectFailure::Cancelled)
                } else if !proc.alive() {
                    Some(ConnectFailure::GameExited)
                } else if Instant::now() >= deadline {
                    Some(ConnectFailure::Timeout)
                } else {
                    None
                };
                let Some(failure) = failure else {
                    continue;
                };
                let dummy = open_pipe_client(name, client_access);
                let dummy_connected = dummy.is_some();
                wait_connect_thread(waiter, pipe);
                if let Some(dummy) = dummy {
                    // SAFETY: dummy 由 CreateFileW 返回。
                    unsafe { windows_sys::Win32::Foundation::CloseHandle(dummy) };
                }
                // dummy 没连上说明真实客户端占用了实例；再看连接线程的结果。
                if matches!(failure, ConnectFailure::Timeout)
                    && !dummy_connected
                    && matches!(rx.try_recv(), Ok(Ok(())))
                {
                    return Ok(());
                }
                return Err(failure);
            }
        }
    }
}

fn connect_failure_to_error(failure: ConnectFailure) -> anyhow::Error {
    match failure {
        ConnectFailure::Timeout => anyhow::anyhow!("等待管道连接超时"),
        ConnectFailure::GameExited => anyhow::anyhow!("等待管道时游戏已退出"),
        ConnectFailure::Cancelled => anyhow::anyhow!("注入会话已取消"),
        ConnectFailure::Failed(error) => error,
    }
}

/// 把 reattach 等待失败映射成退出码语义：
/// 窗口耗尽且游戏还活着 → 4（须人工重启游戏）；游戏已退出 → 0；取消 → 错误。
fn reattach_failure_to_result(failure: ConnectFailure, proc: &SuspendedProcess) -> Result<()> {
    match failure {
        ConnectFailure::Cancelled => Err(anyhow::anyhow!("注入会话已取消")),
        ConnectFailure::GameExited => {
            tracing::info!("reattach 等待期间游戏已退出，正常结束");
            Err(crate::exit_code::Marked(crate::exit_code::Kind::GameExited).into())
        }
        ConnectFailure::Timeout => {
            if proc.alive() {
                let code = crate::exit_code::code_of(crate::exit_code::Kind::SessionLostGameAlive);
                tracing::error!(
                    "reattach 等待窗口已耗尽，游戏仍在运行 —— DLL 未重连，聊天链路无法恢复，必须重启游戏。退出码 {code}"
                );
                Err(crate::exit_code::Marked(crate::exit_code::Kind::SessionLostGameAlive).into())
            } else {
                tracing::info!("reattach 等待期间游戏已退出，正常结束");
                Err(crate::exit_code::Marked(crate::exit_code::Kind::GameExited).into())
            }
        }
        ConnectFailure::Failed(error) => Err(error),
    }
}

/// 在窗口内创建主通道并等待 DLL 主动连接。
///
/// 只创建管道并等待：不启动游戏、不注入、不终止任何进程。
fn wait_for_reattach(
    proc: &SuspendedProcess,
    packets: &Arc<PacketControl>,
    cancelled: &AtomicBool,
    window: Duration,
) -> std::result::Result<(std::fs::File, AuxPipes), ConnectFailure> {
    use std::os::windows::io::FromRawHandle;

    let deadline = Instant::now() + window;
    let mut pipe = create_pipe_within(proc, cancelled, deadline)?;
    let mut aux = AuxPipes::open(packets);
    if let Err(failure) = connect_pipe_until(
        &mut pipe,
        PIPE_NAME,
        CLIENT_ACCESS_FULL,
        proc,
        cancelled,
        deadline,
    ) {
        aux.retire(packets);
        return Err(failure);
    }
    // SAFETY: pipe 是刚创建并已连接的句柄，所有权转交 File。
    let stream = unsafe { std::fs::File::from_raw_handle(pipe.0 as *mut _) };
    std::mem::forget(pipe);
    Ok((stream, aux))
}

/// 在截止时间内重试创建主通道。
///
/// 上一轮的实例句柄可能还没被内核完全回收（FIRST_PIPE_INSTANCE 冲突），
/// 这里用短间隔重试，直到窗口耗尽或游戏退出。
fn create_pipe_within(
    proc: &SuspendedProcess,
    cancelled: &AtomicBool,
    deadline: Instant,
) -> std::result::Result<PipeHandle, ConnectFailure> {
    loop {
        match create_pipe() {
            Ok(pipe) => return Ok(pipe),
            Err(error) => {
                if cancelled.load(Ordering::Acquire) {
                    return Err(ConnectFailure::Cancelled);
                }
                if !proc.alive() {
                    return Err(ConnectFailure::GameExited);
                }
                if Instant::now() >= deadline {
                    tracing::warn!("reattach 创建主通道失败且窗口已耗尽：{error:#}");
                    return Err(ConnectFailure::Timeout);
                }
                tracing::debug!("reattach 创建主通道暂时失败，100ms 后重试：{error:#}");
                std::thread::sleep(Duration::from_millis(100));
            }
        }
    }
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

/// 检查取消与“DLL 初始化超时”。
///
/// started 的含义是“主线程已恢复 / 不存在初始化等待”：
///   - 首次注入：ResumeMainThread 之前为 false，超过 deadline 判超时，
///     保护的是“DLL 没起来、游戏主线程还挂着”的情况；
///   - reattach：attach_running_process 直接置 true。游戏早已完成初始化，
///     重连会话没有需要等待的初始化；等待窗口由 connect_pipe_until 的
///     截止时间单独保证。若照搬 spawn 句柄判据（started=false），这个
///     60s 超时会错误地作用在重连会话上（审计 P1 假活），因此明确不参与。
fn check_session(proc: &SuspendedProcess, cancelled: &AtomicBool, deadline: Instant) -> Result<()> {
    check_cancelled(cancelled)?;
    anyhow::ensure!(
        proc.started.get() || Instant::now() < deadline,
        "DLL 初始化超时"
    );
    Ok(())
}

fn wait_for_game(proc: &SuspendedProcess, cancelled: &AtomicBool) -> Result<()> {
    while proc.alive() {
        check_cancelled(cancelled)?;
        poll_pause();
    }
    Ok(())
}

/// debug（全包数据）与 control（模式开关）两条辅助管道。
///
/// 它们都是可选增强：创建/重连失败只记警告，绝不影响聊天主通道。
///
/// 为什么需要显式 retire（而不是靠线程自然退出）：命名管道实例在服务端
/// 句柄关闭前一直占着名字；不释放的话，下一轮 CreateNamedPipeW
/// （带 FILE_FLAG_FIRST_PIPE_INSTANCE）会因同名实例存在而失败。
struct AuxPipes {
    debug: Option<std::thread::JoinHandle<()>>,
    control: Option<std::thread::JoinHandle<()>>,
    /// 通知 debug 读线程退出（它在空转时每 20ms 检查一次）。
    debug_stop: Arc<AtomicBool>,
}

impl AuxPipes {
    /// 创建两条辅助管道并启动各自的线程；失败只记警告。
    fn open(packets: &Arc<PacketControl>) -> Self {
        use windows_sys::Win32::Storage::FileSystem::{PIPE_ACCESS_INBOUND, PIPE_ACCESS_OUTBOUND};

        let mut aux = Self {
            debug: None,
            control: None,
            debug_stop: Arc::new(AtomicBool::new(false)),
        };
        match create_named_pipe_server(DEBUG_PIPE_NAME, PIPE_ACCESS_INBOUND) {
            Ok(pipe) => {
                aux.debug =
                    start_debug_pipe(pipe, Arc::clone(packets), Arc::clone(&aux.debug_stop));
            }
            Err(error) => {
                tracing::warn!("debug 数据管道创建失败，全包模式不可用（聊天不受影响）：{error:#}")
            }
        }
        match create_named_pipe_server(CONTROL_PIPE_NAME, PIPE_ACCESS_OUTBOUND) {
            Ok(pipe) => aux.control = start_control_pipe(pipe, Arc::clone(packets)),
            Err(error) => {
                tracing::warn!("控制管道创建失败，debug 开关不可用（聊天不受影响）：{error:#}")
            }
        }
        aux
    }

    /// 会话结束/重连前调用：释放这两条同名管道实例。
    ///
    /// 先用本地 dummy 客户端唤醒可能仍阻塞在 ConnectNamedPipe 的线程，
    /// 再 join；随后 detach_control 关闭控制管道的写端。顺序不能反——
    /// detach 之后实例已关，dummy 就没有可连的对象了。
    fn retire(&mut self, packets: &PacketControl) {
        use windows_sys::Win32::Foundation::{GENERIC_READ, GENERIC_WRITE};

        // control：客户端读（server 是 PIPE_ACCESS_OUTBOUND）。
        wake_pipe_client(CONTROL_PIPE_NAME, GENERIC_READ);
        if let Some(handle) = self.control.take() {
            join_bounded(handle, Duration::from_secs(2), "control");
        }
        packets.detach_control();

        // debug：客户端写（server 是 PIPE_ACCESS_INBOUND）。
        self.debug_stop.store(true, Ordering::Release);
        wake_pipe_client(DEBUG_PIPE_NAME, GENERIC_WRITE);
        if let Some(handle) = self.debug.take() {
            join_bounded(handle, Duration::from_secs(2), "debug");
        }
    }
}

/// 有界等待线程退出。
///
/// 超时只记日志、不 join（join 会无限卡住）：辅助管道是可选能力，不能让
/// 一次线程卡死拖垮聊天主通道或进程退出。代价是那个线程仍占着实例句柄，
/// 下一轮创建同名管道会失败并记警告——届时 chat 仍正常工作。
fn join_bounded(handle: std::thread::JoinHandle<()>, timeout: Duration, label: &str) {
    let deadline = Instant::now() + timeout;
    while !handle.is_finished() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    if handle.is_finished() {
        let _ = handle.join();
    } else {
        tracing::warn!("{label} 管道线程未在 {timeout:?} 内退出，放弃等待（同名实例可能仍被占用）");
        drop(handle);
    }
}

/// 等待 DLL 连上控制管道；成功后把只写句柄交给 PacketControl。
fn start_control_pipe(
    pipe: PipeHandle,
    packets: Arc<PacketControl>,
) -> Option<std::thread::JoinHandle<()>> {
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
    match spawned {
        Ok(handle) => Some(handle),
        Err(error) => {
            tracing::warn!("启动控制管道线程失败：{error}");
            // SAFETY: 句柄来自 CreateNamedPipeW 且尚未转交。
            unsafe {
                windows_sys::Win32::Foundation::CloseHandle(raw as *mut std::ffi::c_void);
            }
            None
        }
    }
}

/// 等待 DLL 连上 debug 数据管道，然后持续解析包并交给 PacketControl。
///
/// debug 管道是单向的：DLL 写、宿主读，没有 ACK；协议帧复用聊天管道的
/// 0x04 PushPacketData 编码。解析/统计/落盘都在这个独立线程里，不碰聊天状态。
///
/// 与旧实现的区别：读到空缓冲时不再阻塞 read，而是 PeekNamedPipe + 短睡眠，
/// 这样 retire 的停止标志能在 20ms 内生效，及时释放同名管道实例。
fn start_debug_pipe(
    pipe: PipeHandle,
    packets: Arc<PacketControl>,
    stop: Arc<AtomicBool>,
) -> Option<std::thread::JoinHandle<()>> {
    let raw = pipe.0 as usize;
    std::mem::forget(pipe);
    let spawned = std::thread::Builder::new()
        .name("mwc-debug-pipe".to_owned())
        .spawn(move || {
            use std::io::Read;
            let Some(mut file) = take_connected_pipe(raw, "debug") else {
                return;
            };
            // 线程启动前会话可能已经结束/进入重连：放弃这条实例。
            if stop.load(Ordering::Acquire) {
                return;
            }
            tracing::info!("debug 数据管道已连接");
            let mut buffered: Vec<u8> = Vec::new();
            let mut chunk = [0u8; 65536];
            loop {
                if stop.load(Ordering::Acquire) {
                    break;
                }
                match poll_pipe(&file) {
                    Ok(PipeState::Data) => match file.read(&mut chunk) {
                        Ok(0) => break,
                        Ok(read) => buffered.extend_from_slice(&chunk[..read]),
                        Err(error) => {
                            tracing::warn!("读 debug 管道失败：{error}");
                            break;
                        }
                    },
                    Ok(PipeState::Empty) => {
                        std::thread::sleep(Duration::from_millis(20));
                        continue;
                    }
                    Ok(PipeState::Closed) => break,
                    Err(error) => {
                        tracing::warn!("检查 debug 管道状态失败：{error:#}");
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
    match spawned {
        Ok(handle) => Some(handle),
        Err(error) => {
            tracing::warn!("启动 debug 管道线程失败：{error}");
            // SAFETY: 句柄来自 CreateNamedPipeW 且尚未转交。
            unsafe {
                windows_sys::Win32::Foundation::CloseHandle(raw as *mut std::ffi::c_void);
            }
            None
        }
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
/// 游戏进程名（小写）。Toolhelp 快照里比较用。
const GAME_EXE_NAMES: [&str; 2] = ["yuanshen.exe", "genshinimpact.exe"];

/// 按 PID 在进程快照里确认目标仍是游戏进程（只读快照，不打开进程）。
///
/// reattach 时 OpenProcess 可能因反作弊/权限失败，这时靠它判断存活。
/// 同时匹配 exe 名，避免 PID 被系统复用后误判游戏还活着。
fn process_exists(pid: u32) -> bool {
    use windows_sys::Win32::System::Diagnostics::ToolHelp::{
        CreateToolhelp32Snapshot, PROCESSENTRY32W, Process32FirstW, Process32NextW,
        TH32CS_SNAPPROCESS,
    };

    // SAFETY: 标准进程快照调用。
    let snapshot = unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPPROCESS, 0) };
    if snapshot.is_null() || snapshot as isize == -1 {
        return false;
    }
    // SAFETY: 零初始化 + 设置 dwSize 是快照 API 的约定。
    let mut entry: PROCESSENTRY32W = unsafe { std::mem::zeroed() };
    entry.dwSize = std::mem::size_of::<PROCESSENTRY32W>() as u32;
    let mut found = false;
    // SAFETY: snapshot 有效，entry 尺寸正确。
    if unsafe { Process32FirstW(snapshot, &mut entry) } != 0 {
        loop {
            if entry.th32ProcessID == pid {
                let end = entry
                    .szExeFile
                    .iter()
                    .position(|&c| c == 0)
                    .unwrap_or(entry.szExeFile.len());
                let name = String::from_utf16_lossy(&entry.szExeFile[..end]).to_lowercase();
                if GAME_EXE_NAMES.contains(&name.as_str()) {
                    found = true;
                    break;
                }
            }
            // SAFETY: snapshot 有效，entry 已初始化。
            if unsafe { Process32NextW(snapshot, &mut entry) } == 0 {
                break;
            }
        }
    }
    // SAFETY: snapshot 来自快照 API。
    unsafe { windows_sys::Win32::Foundation::CloseHandle(snapshot) };
    found
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

    let wanted = GAME_EXE_NAMES;
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
    /// 是否由本程序启动。reattach 模式下为 false，不需要 ResumeThread，
    /// Drop 也绝不会终止进程。
    spawned_by_us: bool,
    started: std::cell::Cell<bool>,
}

impl SuspendedProcess {
    /// 游戏是否还活着。
    ///
    /// reattach 可能拿不到进程句柄（反作弊/权限），此时退回进程快照判断；
    /// 绝不为“判断存活”去请求读写内存所需的权限。
    fn alive(&self) -> bool {
        if self.process.is_null() {
            process_exists(self.pid)
        } else {
            process_alive(self.process)
        }
    }
}

/// 打开已在运行的游戏进程，只用于观察存活（reattach 用）。
///
/// 只请求 PROCESS_SYNCHRONIZE：不读内存、不写内存、不注入、不终止进程。
///
/// started 直接置 true —— reattach 不存在“DLL 初始化”等待：游戏早已完成
/// 启动，等待窗口由 connect_pipe_until 的截止时间单独保证。若照搬 spawn
/// 句柄语义（started=false），check_session 的 60s 初始化超时会错误地作用
/// 在重连会话上（审计 P1 假活）；首次注入路径的 started 语义完全不变。
fn attach_running_process(pid: u32) -> SuspendedProcess {
    use windows_sys::Win32::System::Threading::OpenProcess;
    /// PROCESS_SYNCHRONIZE：只等进程退出，不碰内存。
    const PROCESS_SYNCHRONIZE: u32 = 0x0010_0000;
    // SAFETY: OpenProcess 是标准调用，且只请求同步权限。
    let handle = unsafe { OpenProcess(PROCESS_SYNCHRONIZE, 0, pid) };
    if handle.is_null() {
        tracing::warn!(
            "OpenProcess(PROCESS_SYNCHRONIZE) 失败（{}），改用进程快照判断游戏存活",
            std::io::Error::last_os_error()
        );
    }
    SuspendedProcess {
        process: handle,
        thread: std::ptr::null_mut(),
        pid,
        spawned_by_us: false,
        started: std::cell::Cell::new(true),
    }
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
    if proc.alive() {
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
        let short = format!(
            "miliastra-test-{}-{}",
            std::process::id(),
            rand::random::<u64>()
        );
        let name = pipe_path_named(&short);
        let access = windows_sys::Win32::Storage::FileSystem::PIPE_ACCESS_DUPLEX;
        let mut pipe = create_named_pipe(&name, access).unwrap();
        // FIRST_PIPE_INSTANCE 检测冲突不需要客户端 CreateFile，因此连接仍留给 DLL。
        assert!(create_named_pipe(&name, access).is_err());
        let proc = suspended_cmd();
        // 已取消时必须在进入 ConnectNamedPipe 之前就返回，且不能用 dummy
        // 客户端把本应留给 DLL 的连接消费掉（见 connect_pipe_until 的前置检查）。
        let cancelled = AtomicBool::new(true);
        let start = Instant::now();
        let deadline = start + Duration::from_secs(30);
        let result = connect_pipe_until(
            &mut pipe,
            &short,
            CLIENT_ACCESS_FULL,
            &proc,
            &cancelled,
            deadline,
        );
        assert!(result.is_err());
        assert!(start.elapsed() < Duration::from_secs(2));
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

    fn sample_rva() -> crate::metadata::MethodRva {
        crate::metadata::MethodRva {
            do_cmd: 1,
            update_normal_prop: 2,
            new_string: 3,
            find_game_object: 4,
            event_system_update: 5,
            simulate_pointer_click: 6,
            to_int32: 425_587_504,
            tcp_state_ptr: 91_775_080,
            shared_info_ptr: 91_775_072,
            decompress: 115_234_864,
        }
    }

    /// 建立一个“服务端只读、客户端只写”的命名管道对，并让客户端预先写入一帧。
    ///
    /// 服务端句柄没有写权限，所以 serve_connected 的回执写入必然失败，
    /// 正好复现“对端已断开/正在卡顿”的写失败路径。
    fn read_only_pipe_with_frame(frame: &[u8]) -> (std::fs::File, std::fs::File, SuspendedProcess) {
        use std::io::Write;
        use std::os::windows::io::FromRawHandle;

        let short = format!(
            "miliastra-write-fail-{}-{}",
            std::process::id(),
            rand::random::<u64>()
        );
        let name = pipe_path_named(&short);
        let pipe = create_named_pipe(
            &name,
            windows_sys::Win32::Storage::FileSystem::PIPE_ACCESS_INBOUND,
        )
        .unwrap();
        let client = open_pipe_client(&short, windows_sys::Win32::Foundation::GENERIC_WRITE)
            .expect("客户端连接失败");
        // 客户端可能已先连上：ConnectNamedPipe 返回 ERROR_PIPE_CONNECTED，视为成功。
        accept_pipe(pipe.0).unwrap();
        let stream = unsafe { std::fs::File::from_raw_handle(pipe.0 as *mut _) };
        std::mem::forget(pipe);
        let mut client = unsafe { std::fs::File::from_raw_handle(client as *mut _) };
        client.write_all(frame).unwrap();
        client.flush().unwrap();
        (stream, client, suspended_cmd())
    }

    /// 收包中途 ACK 写失败必须返回 PeerClosed（→ supervise 走 peer_close_action
    /// → 游戏还活着时回到 reattach 等待），不能冒泡成进程级错误直接退出。
    #[test]
    fn ack_write_failure_is_peer_closed_not_process_error() {
        let mut frame = vec![0x04u8];
        frame.extend(2246u16.to_le_bytes());
        frame.extend(4u32.to_le_bytes());
        frame.extend([1, 2, 3, 4]);
        let (mut stream, _client, proc) = read_only_pipe_with_frame(&frame);

        let cancelled = AtomicBool::new(false);
        let chat = ChatState::new(999);
        let rva = sample_rva();
        match serve_connected(&proc, &mut stream, &rva, &chat, &cancelled) {
            Ok(SessionEnd::PeerClosed(reason)) => {
                assert!(reason.contains("ACK"), "原因应说明 ACK 写失败: {reason}");
            }
            Ok(SessionEnd::Finished(reason)) => {
                panic!("ACK 写失败不能当成正常结束: {reason}");
            }
            Err(error) => {
                panic!("ACK 写失败必须回到 reattach 决策，不能冒泡成进程级错误: {error:#}");
            }
        }
        // 与 supervise 同一决策函数：游戏存活 + 窗口未用尽 → 回到 reattach 等待。
        assert!(proc.alive(), "测试进程应仍存活");
        assert_eq!(
            crate::peer_close_action(
                proc.alive(),
                Duration::from_secs(60),
                crate::MAX_REATTACH_ROUNDS
            ),
            crate::PeerCloseAction::WaitReconnect,
            "ACK 写失败后必须回到 reattach 等待，而不是退出"
        );
    }

    /// 握手回执写失败走同一条语义：PeerClosed，而不是进程级错误。
    #[test]
    fn handshake_write_failure_is_peer_closed_not_process_error() {
        let (mut stream, _client, proc) = read_only_pipe_with_frame(&[0xFA]);
        let cancelled = AtomicBool::new(false);
        let chat = ChatState::new(999);
        let rva = sample_rva();
        match serve_connected(&proc, &mut stream, &rva, &chat, &cancelled) {
            Ok(SessionEnd::PeerClosed(reason)) => {
                assert!(reason.contains("握手"), "原因应说明握手写失败: {reason}");
            }
            Ok(SessionEnd::Finished(reason)) => {
                panic!("握手回执写失败不能当成正常结束: {reason}");
            }
            Err(error) => {
                panic!("握手回执写失败必须回到 reattach 决策，不能冒泡: {error:#}");
            }
        }
        assert_eq!(
            crate::peer_close_action(
                proc.alive(),
                Duration::from_secs(60),
                crate::MAX_REATTACH_ROUNDS
            ),
            crate::PeerCloseAction::WaitReconnect,
        );
    }
}
