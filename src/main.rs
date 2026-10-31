//! 千星奇域聊天 API。
//!
//! 通过注入游戏的 DLL 嗅探聊天包，解析后经 HTTP/WS 提供给 BOT。
//! 基础使用与许可见 README.md。

// 非 Windows 平台没有注入入口，大部分模块只被测试引用。
// 这是有意为之：注入链路只能在 Windows + 真实游戏下验证。
#![allow(dead_code)]

mod config;
mod diag;
mod extract;
mod metadata;
mod model;
mod packet_control;
mod pipe;
mod proto;
mod server;
mod state;

mod exit_code;
mod game_path;
#[cfg(windows)]
mod inject;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use clap_free_args::Args;

fn main() -> std::process::ExitCode {
    match run() {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            // 退出原因既进日志也进 stderr：隐藏窗口下 stderr 是 NUL，
            // 但上游可以靠退出码判断该不该人工介入。
            tracing::error!("{error:#}");
            eprintln!("{error:#}");
            std::process::ExitCode::from(exit_code_of(&error))
        }
    }
}

/// 把错误映射成进程退出码，供上游区分「等玩家退出」与「必须人工处理」。
fn exit_code_of(error: &anyhow::Error) -> u8 {
    let code = exit_code::classify(error);
    tracing::info!("退出码 {code}：{}", exit_code::describe(code));
    code as u8
}

// ---------------------------------------------------------------------------
// 会话附着状态与 reattach（重连）策略
// ---------------------------------------------------------------------------
//
// 为什么放在 main.rs 而不是 inject.rs：inject.rs 只在 Windows 编译，
// 而这里的相位/状态机是纯策略、不依赖 Win32。放在平台无关的 crate 根上，
// Linux CI 也能单测这些分支（真实管道语义仍需 Windows 真机验证）。

/// reattach 等待窗口默认值：DLL 退避上限 30s，留一倍余量。
pub const DEFAULT_REATTACH_WINDOW_SECS: u64 = 60;

/// 一个进程内最多尝试几轮 reattach，避免“连上又立刻断”时无限空转。
pub const MAX_REATTACH_ROUNDS: u32 = 3;

/// 宿主与 DLL 的会话可观测状态，供 `/health` 输出。
///
/// - `attached`：当前是否有活跃会话（DLL 已连上管道并完成握手前的连接）；
/// - `reattach_waiting`：是否正处在“等 DLL 重连”的窗口里；
/// - `reattach_attempts`：本进程启动以来进入等待的轮数（可看出重连在反复失败）。
#[derive(Debug, Default)]
pub struct SessionStatus {
    attached: AtomicBool,
    reattach_waiting: AtomicBool,
    reattach_attempts: AtomicU32,
}

impl SessionStatus {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// 管道已连上（首次注入成功或 reattach 成功）。
    pub fn mark_attached(&self) {
        self.attached
            .store(true, std::sync::atomic::Ordering::Relaxed);
        self.reattach_waiting
            .store(false, std::sync::atomic::Ordering::Relaxed);
    }

    /// 进入等待 DLL 重连的窗口（轮数 +1）。
    pub fn mark_reattach_waiting(&self) {
        self.attached
            .store(false, std::sync::atomic::Ordering::Relaxed);
        self.reattach_waiting
            .store(true, std::sync::atomic::Ordering::Relaxed);
        self.reattach_attempts
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// 会话结束且不再等待（进程退出/游戏退出）。
    pub fn mark_detached(&self) {
        self.attached
            .store(false, std::sync::atomic::Ordering::Relaxed);
        self.reattach_waiting
            .store(false, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn attached(&self) -> bool {
        self.attached.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn reattach_waiting(&self) -> bool {
        self.reattach_waiting
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn reattach_attempts(&self) -> u32 {
        self.reattach_attempts
            .load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// 启动时的初始动作。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InitialAction {
    /// 游戏未运行：照旧 spawn_suspended + 注入，行为不变。
    Inject,
    /// 游戏已在运行：只建管道等 DLL 主动重连，不注入、不杀进程。
    Reattach,
    /// reattach 被禁用（窗口 0）：保持旧行为，退出码 3。
    BailAlreadyRunning,
}

/// 决定启动时是正常注入还是进入 reattach 等待。
///
/// 旧实现在“游戏已在运行”时直接退出（退出码 3），于是 DLL 侧的重连逻辑
/// 永远等不到宿主；这里改为进入 reattach。窗口为 0 时保留旧行为作显式退路。
pub fn initial_action(running: Option<u32>, reattach_window: Duration) -> InitialAction {
    match running {
        None => InitialAction::Inject,
        Some(_) if reattach_window.is_zero() => InitialAction::BailAlreadyRunning,
        Some(_) => InitialAction::Reattach,
    }
}

/// 对端关闭（EOF）后的动作。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerCloseAction {
    /// 游戏还活着、窗口未耗尽：回到等待 DLL 重连。
    WaitReconnect,
    /// 游戏还活着但已无重连机会：退出码 4（须人工重启游戏）。
    SessionLost,
    /// 游戏已退出：正常结束，退出码 0。
    GameExited,
}

/// 决定会话断开（读到 EOF 或回执写失败）之后是继续等重连还是收尾。
///
/// `reattach_window` 是**每轮**窗口（0 表示 reattach 被禁用）——每轮重新计时，
/// 总等待上限约为 轮数 × 窗口；`attempts_left` 是本进程还能进入等待的轮数，
/// 防止“连上又立刻断”时无限重试。
pub fn peer_close_action(
    game_alive: bool,
    reattach_window: Duration,
    attempts_left: u32,
) -> PeerCloseAction {
    if !game_alive {
        PeerCloseAction::GameExited
    } else if reattach_window.is_zero() || attempts_left == 0 {
        PeerCloseAction::SessionLost
    } else {
        PeerCloseAction::WaitReconnect
    }
}

/// 实际的启动与主循环；错误在这里向上抛，由 main 决定退出码。
fn run() -> Result<()> {
    let args = Args::parse(std::env::args().skip(1))?;
    let log_file = if args.no_log_file {
        None
    } else {
        Some(args.log_file.clone().unwrap_or_else(default_log_path))
    };
    init_tracing(log_file.clone());
    diag::init();
    if let Some(path) = log_file.as_deref() {
        tracing::info!("日志文件: {}", path.display());
    }
    let path = args.config.clone();
    let cfg = config::load(
        &path,
        args.self_uid,
        &config::Overrides {
            port: args.port,
            token: args.token.clone(),
        },
    )?;
    tracing::info!(
        "配置已加载: port={} token=<hidden> selfUid={}",
        cfg.port,
        cfg.self_uid
    );
    if args.diagnose {
        return diagnose(&cfg, args.game_path.as_deref());
    }

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("创建 tokio 运行时失败")?;

    let chat = state::ChatState::new(cfg.self_uid);
    let connected = Arc::new(AtomicBool::new(false));
    let packets = packet_control::PacketControl::new(
        if args.debug_packets {
            packet_control::PacketMode::All
        } else {
            packet_control::PacketMode::Chat
        },
        args.capture_dir
            .clone()
            .map(|dir| packet_control::CaptureConfig {
                dir,
                max_bytes: args.capture_max_mib.saturating_mul(1024 * 1024),
            }),
    );
    if let Some(dir) = args.capture_dir.as_deref() {
        tracing::info!(
            "raw 抓包目录: {}（debug 模式收到全包后写入；每次启动重建 index.csv）",
            dir.display()
        );
    }
    // 会话状态由 inject（Windows）更新、/health 观测；reattach 期间它让
    // 上游能区分“正在重连”与“彻底失效”。
    let session = SessionStatus::new();
    let app = Arc::new(server::AppState {
        chat: Arc::clone(&chat),
        token: cfg.token.clone(),
        started: Instant::now(),
        connected: Arc::clone(&connected),
        port: cfg.port,
        self_uid: cfg.self_uid,
        packets: Arc::clone(&packets),
        session: Arc::clone(&session),
    });

    let result = runtime.block_on(async move {
        // 先成功绑定 HTTP，端口冲突时绝不创建游戏进程。
        let port = cfg.port;
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", port)).await
            .with_context(|| format!("监听 127.0.0.1:{port} 失败"))?;
        tracing::info!("HTTP/WS 服务已启动: http://127.0.0.1:{port}");
        let serve = async { axum::serve(listener, server::router(app)).await.context("HTTP 服务异常退出") };
        let cancelled = Arc::new(AtomicBool::new(false));
        #[cfg(windows)]
        let ingest = {
            let cancelled = Arc::clone(&cancelled);
            // reattach 窗口：0 表示禁用（回到旧行为：游戏已在运行直接退出码 3）。
            let reattach_window = Duration::from_secs(args.reattach_seconds);
            async move {
                if !args.auto_launch { return std::future::pending::<Result<()>>().await; }
                inject::run(chat, connected, Arc::clone(&packets), Arc::clone(&session), args.auto_launch, args.spawn_only, args.lib_only, args.game_path.clone(), reattach_window, cancelled).await
            }
        };
        #[cfg(not(windows))]
        let ingest = async { std::future::pending::<Result<()>>().await };
        tokio::pin!(ingest);
        #[cfg(windows)]
        let mut ingest_finished = false;
        let result = tokio::select! {
            result = serve => result,
            result = &mut ingest => {
                #[cfg(windows)]
                { ingest_finished = true; }
                // --no-launch 持续提供 HTTP；启动器则随注入会话/游戏结束退出。
                if args.auto_launch { result } else { unreachable!("no-launch ingest should remain pending") }
            },
            signal = tokio::signal::ctrl_c() => {
                tracing::info!("收到 Ctrl-C，退出");
                signal.context("监听 Ctrl-C 失败")
            }
        };
        cancelled.store(true, std::sync::atomic::Ordering::Release);
        // spawn_blocking 不能 abort；显式等待短周期轮询退出，运行时不再卡在同步管道 I/O。
        #[cfg(windows)]
        if args.auto_launch && !ingest_finished { let _ = ingest.await; }
        result
    });
    tracing::info!("主循环退出");
    result
}

/// 日志文件默认路径：与可执行文件同目录，便于跟着程序走。
fn default_log_path() -> std::path::PathBuf {
    std::env::current_exe()
        .ok()
        .and_then(|exe| exe.parent().map(|dir| dir.join("chatbridge.log")))
        .unwrap_or_else(|| std::path::PathBuf::from("chatbridge.log"))
}

/// 日志文件超过这个大小就滚动一次（只保留一份历史）。
const LOG_ROTATE_BYTES: u64 = 8 * 1024 * 1024;

fn init_tracing(log_file: Option<std::path::PathBuf>) {
    use tracing_subscriber::{EnvFilter, fmt};
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("miliastra_wonderland_chatbridge=debug,warn"));
    let writer = TeeWriter::new(log_file.as_deref());
    fmt()
        .with_env_filter(filter)
        .with_target(false)
        .with_ansi(false)
        .with_writer(writer)
        .init();
}

/// 同时写 stderr 与文件。
///
/// 为什么需要文件：程序通常被 Music 以隐藏窗口拉起，stdout/stderr 指向 NUL，
/// 只靠控制台观察等于没有日志。文件层让"事后回看"成为可能。
/// 日志文件句柄 + 运行期滚动所需的路径与已写字节数。
///
/// 不实现 Clone：内部持有 File，整个结构被 Mutex 独占。
struct LogFile {
    file: Option<std::fs::File>,
    path: std::path::PathBuf,
    written: u64,
}

/// 滚动检查的间隔写入量：每写这么多字节就核对一次文件大小。
///
/// 不必每次 write 都 stat（日志热路径），也不能只在启动时检查一次——
/// 之前就是后者，导致 8 MiB 之后无界追加直到磁盘满。
const LOG_ROTATE_CHECK_BYTES: u64 = 256 * 1024;

#[derive(Clone, Default)]
struct TeeWriter {
    file: Option<std::sync::Arc<std::sync::Mutex<Option<LogFile>>>>,
}

impl TeeWriter {
    fn new(path: Option<&std::path::Path>) -> Self {
        let Some(path) = path else {
            return Self::default();
        };
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
            && let Err(error) = std::fs::create_dir_all(parent)
        {
            eprintln!(
                "日志目录创建失败（改为仅 stderr）: {}: {error}",
                parent.display()
            );
            return Self::default();
        }
        rotate_log(path);
        match std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
        {
            Ok(file) => Self {
                file: Some(std::sync::Arc::new(std::sync::Mutex::new(Some(LogFile {
                    file: Some(file),
                    path: path.to_path_buf(),
                    written: 0,
                })))),
            },
            Err(error) => {
                eprintln!(
                    "日志文件打开失败（改为仅 stderr）: {}: {error}",
                    path.display()
                );
                Self::default()
            }
        }
    }
}

fn rotate_log(path: &std::path::Path) {
    let Ok(metadata) = std::fs::metadata(path) else {
        return;
    };
    if metadata.len() < LOG_ROTATE_BYTES {
        return;
    }
    let mut rotated = path.as_os_str().to_owned();
    rotated.push(".1");
    let rotated = std::path::PathBuf::from(rotated);
    let _ = std::fs::remove_file(&rotated);
    let _ = std::fs::rename(path, rotated);
}

/// 把内容写进日志文件，必要时先滚动。
///
/// 滚动失败不影响写日志：宁可继续往旧文件追加，也不能把日志整条丢掉。
/// 写失败要报到 stderr —— 之前用 `let _` 吞掉，磁盘满之后完全静默。
fn write_rolled(log: &mut LogFile, buf: &[u8]) {
    use std::io::Write as _;
    if log.written >= LOG_ROTATE_CHECK_BYTES {
        log.written = 0;
        let too_big = std::fs::metadata(&log.path)
            .map(|meta| meta.len() >= LOG_ROTATE_BYTES)
            .unwrap_or(false);
        if too_big {
            // 先关句柄再改名：Windows 上打开中的文件无法 rename。
            log.file = None;
            rotate_log(&log.path);
            match std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&log.path)
            {
                Ok(file) => log.file = Some(file),
                Err(error) => {
                    eprintln!("日志重新打开失败（后续只写 stderr）: {error}");
                }
            }
        }
    }
    let Some(file) = log.file.as_mut() else {
        return;
    };
    if let Err(error) = file.write_all(buf) {
        eprintln!("日志写入失败（后续只写 stderr）: {error}");
        log.file = None;
        return;
    }
    log.written = log.written.saturating_add(buf.len() as u64);
}

impl std::io::Write for TeeWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if let Some(file) = &self.file
            && let Ok(mut guard) = file.lock()
            && let Some(log) = guard.as_mut()
        {
            write_rolled(log, buf);
            if let Err(error) = log.file.as_mut().map(|file| file.flush()).unwrap_or(Ok(())) {
                eprintln!("日志刷新失败: {error}");
            }
        }
        std::io::stderr().write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        if let Some(file) = &self.file
            && let Ok(mut guard) = file.lock()
            && let Some(log) = guard.as_mut()
            && let Some(file) = log.file.as_mut()
        {
            let _ = file.flush();
        }
        std::io::stderr().flush()
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for TeeWriter {
    type Writer = TeeWriter;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// 启动前自检：在拉起游戏之前把"环境不对"暴露出来。
///
/// 只读文件 + 占用一次端口做冲突检测；不启动游戏、不注入、不改任何状态。
fn diagnose(cfg: &config::Config, game_path: Option<&std::path::Path>) -> Result<()> {
    use std::path::PathBuf;

    // 只有 Windows 的自检会用游戏路径做校验；其它平台避免 unused 警告。
    #[cfg(not(windows))]
    let _ = game_path;

    println!(
        "ChatBridge 自检  version={}  port={}  selfUid={}",
        env!("CARGO_PKG_VERSION"),
        cfg.port,
        cfg.self_uid
    );

    let exe_dir = std::env::current_exe()
        .ok()
        .and_then(|exe| exe.parent().map(PathBuf::from))
        .unwrap_or_else(|| PathBuf::from("."));
    let mut checks: Vec<(bool, String)> = Vec::new();

    checks.push((
        !cfg.token.is_empty(),
        format!(
            "配置: token 长度 {}，不打印内容；端口 {}",
            cfg.token.len(),
            cfg.port
        ),
    ));

    checks.push(match std::net::TcpListener::bind(("127.0.0.1", cfg.port)) {
        Ok(listener) => {
            drop(listener);
            (true, format!("端口 127.0.0.1:{} 可用", cfg.port))
        }
        Err(error) => (
            false,
            format!(
                "端口 127.0.0.1:{} 被占用或不可绑定: {error}（是否已有实例在跑？）",
                cfg.port
            ),
        ),
    });

    let dll = exe_dir.join("MiliastraWonderlandChatBridge.Native.dll");
    checks.push(match std::fs::metadata(&dll) {
        Ok(metadata) => (
            metadata.len() > 0,
            format!("原生 DLL: {}（{} 字节）", dll.display(), metadata.len()),
        ),
        Err(error) => (false, format!("原生 DLL 缺失: {}: {error}", dll.display())),
    });

    let metadata_path = exe_dir.join("metadata.bin");
    checks.push(match std::fs::read(&metadata_path) {
        Ok(bytes) => match crate::metadata::Metadata::parse(&bytes) {
            Ok(parsed) => (
                parsed.games() > 0,
                format!(
                    "metadata.bin: {} 字节，{} 个游戏版本",
                    bytes.len(),
                    parsed.games()
                ),
            ),
            Err(error) => (false, format!("metadata.bin 解析失败: {error:#}")),
        },
        Err(error) => (
            false,
            format!("metadata.bin 缺失: {}: {error}", metadata_path.display()),
        ),
    });

    #[cfg(windows)]
    checks.push(game_check(&metadata_path, game_path));

    let mut failures = 0usize;
    for (ok, detail) in &checks {
        println!("  [{}] {detail}", if *ok { "OK" } else { "FAIL" });
        if !ok {
            failures += 1;
        }
    }
    if failures > 0 {
        anyhow::bail!("自检失败 {failures} 项");
    }
    println!("自检通过。");
    Ok(())
}

/// 校验游戏 exe：找得到、能算出 CRC、metadata 里有完整 RVA 表。
#[cfg(windows)]
fn game_check(
    metadata_path: &std::path::Path,
    game_path: Option<&std::path::Path>,
) -> (bool, String) {
    use anyhow::Context as _;
    // 与注入路径共用同一套查找（--game-path → 注册表 → 默认位置），
    // 否则自检和真正启动可能指向不同的 exe。
    let game = match crate::inject::locate_game(game_path) {
        Ok(game) => game,
        Err(error) => return (false, format!("{error:#}")),
    };
    let game = game.as_path();
    let check = (|| -> anyhow::Result<(u32, bool)> {
        use std::io::Read as _;
        let mut head = vec![0u8; 65536];
        let mut file = std::fs::File::open(game).context("打开游戏 exe 失败")?;
        let read = file.read(&mut head)?;
        head.truncate(read);
        let crc = crate::metadata::game_crc32(&head);
        let bytes = std::fs::read(metadata_path).context("读取 metadata.bin 失败")?;
        let metadata = crate::metadata::Metadata::parse(&bytes)?;
        let rva = metadata
            .for_game(crc)
            .with_context(|| format!("metadata 里没有 CRC32=0x{crc:08x} 的 RVA 表"))?;
        Ok((crc, rva.is_valid()))
    })();
    match check {
        Ok((crc, true)) => (
            true,
            format!(
                "游戏 exe: {}（CRC32=0x{crc:08x}，RVA 表完整）",
                game.display()
            ),
        ),
        Ok((crc, false)) => (
            false,
            format!(
                "游戏 exe: {}（CRC32=0x{crc:08x}）的 RVA 表不完整",
                game.display()
            ),
        ),
        Err(error) => (false, format!("游戏 exe 检查失败: {error:#}")),
    }
}

/// 极简参数解析：只支持 --config 与 --self-uid。
mod clap_free_args {
    use std::path::PathBuf;

    use anyhow::{Context, Result};

    pub struct Args {
        pub config: PathBuf,
        pub self_uid: u32,
        /// 是否自动拉起游戏。默认 true：本程序就是启动器。
        ///
        /// 原神反作弊会拦截外部进程注入，所以只能在游戏启动前用
        /// CREATE_SUSPENDED 自己拉起再注入，无法附加到已运行的游戏。
        pub auto_launch: bool,
        /// 调试用：只启动游戏并恢复主线程，不注入 DLL。
        pub spawn_only: bool,
        /// 调试用：注入 DLL 但不调 YaeMain。
        pub lib_only: bool,
        /// 直接指定监听端口（覆盖 config.yaml 的 port+1 规则）。
        pub port: Option<u16>,
        /// 直接指定鉴权令牌（覆盖 config.yaml 的 access_token）。
        pub token: Option<String>,
        /// 日志文件路径；默认是程序同目录的 chatbridge.log。
        pub log_file: Option<PathBuf>,
        /// 关闭文件日志，只写 stderr。
        pub no_log_file: bool,
        /// 只做启动前自检并退出，不启动游戏。
        pub diagnose: bool,
        /// debug 模式初始为 all；不持久化，推荐运行时通过 API 切换。
        pub debug_packets: bool,
        /// raw 抓包目录；只在显式配置时落盘。
        pub capture_dir: Option<PathBuf>,
        /// raw 抓包总字节上限（MiB），默认 1024。
        pub capture_max_mib: u64,
        /// 显式指定游戏可执行文件或其所在目录。
        ///
        /// 不填时按启动器注册表自动查找（与 Miliastra-Wonderland-Music 一致），
        /// 再退回常见默认安装位置。
        pub game_path: Option<PathBuf>,
        /// 游戏已在运行（或会话断开）时，每轮等 DLL 重连的窗口秒数。
        ///
        /// 默认 60s（DLL 退避上限 30s 的一倍余量）。每轮重新计时，进程内最多
        /// MAX_REATTACH_ROUNDS 轮，总等待上限约为 轮数 × 该值。
        /// 0 表示禁用 reattach：游戏已在运行时沿用旧行为，直接以退出码 3 结束。
        pub reattach_seconds: u64,
    }

    impl Args {
        pub fn parse(mut args: impl Iterator<Item = String>) -> Result<Self> {
            let mut config = super::config::default_config_path();
            let mut self_uid = 0u32;
            let mut auto_launch = true;
            let mut spawn_only = false;
            let mut lib_only = false;
            let mut port: Option<u16> = None;
            let mut token: Option<String> = None;
            let mut log_file: Option<PathBuf> = None;
            let mut no_log_file = false;
            let mut diagnose = false;
            let mut debug_packets = false;
            let mut capture_dir: Option<PathBuf> = None;
            let mut capture_max_mib = 1024u64;
            let mut game_path: Option<PathBuf> = None;
            let mut reattach_seconds = super::DEFAULT_REATTACH_WINDOW_SECS;
            while let Some(arg) = args.next() {
                match arg.as_str() {
                    "--config" => {
                        let value = args.next().context("--config 需要路径参数")?;
                        config = PathBuf::from(value);
                    }
                    "--self-uid" => {
                        let value = args.next().context("--self-uid 需要数字参数")?;
                        self_uid = value.parse().context("--self-uid 必须是数字")?;
                    }
                    "--auto-launch" => auto_launch = true,
                    "--no-launch" => auto_launch = false,
                    "--spawn-only" => spawn_only = true,
                    "--lib-only" => lib_only = true,
                    "--port" => {
                        let value = args.next().context("--port 需要数字参数")?;
                        port = Some(value.parse().context("--port 必须是数字")?);
                    }
                    "--token" => {
                        token = Some(args.next().context("--token 需要参数")?);
                    }
                    "--log-file" => {
                        log_file = Some(PathBuf::from(
                            args.next().context("--log-file 需要路径参数")?,
                        ));
                    }
                    "--no-log-file" => no_log_file = true,
                    "--diagnose" => diagnose = true,
                    "--debug-packets" => debug_packets = true,
                    "--capture-dir" => {
                        capture_dir = Some(PathBuf::from(
                            args.next().context("--capture-dir 需要路径参数")?,
                        ));
                    }
                    "--capture-max-mib" => {
                        let value = args.next().context("--capture-max-mib 需要数字参数")?;
                        capture_max_mib =
                            value.parse().context("--capture-max-mib 必须是正整数")?;
                        anyhow::ensure!(capture_max_mib > 0, "--capture-max-mib 必须大于 0");
                    }
                    "--game-path" => {
                        game_path = Some(PathBuf::from(
                            args.next().context("--game-path 需要路径参数")?,
                        ));
                    }
                    "--reattach-seconds" => {
                        let value = args.next().context("--reattach-seconds 需要数字参数")?;
                        reattach_seconds =
                            value.parse().context("--reattach-seconds 必须是整数")?;
                    }
                    "--version" | "-V" => {
                        println!(
                            "Miliastra-Wonderland-ChatBridge {}",
                            env!("CARGO_PKG_VERSION")
                        );
                        std::process::exit(0);
                    }
                    "--help" | "-h" => {
                        println!(
                            "用法: Miliastra-Wonderland-ChatBridge [--config <path>] [--self-uid <uid>] [--port <n>] [--token <s>] [--log-file <path> | --no-log-file] [--game-path <exe|dir>] [--reattach-seconds <n>] [--no-launch] [--spawn-only] [--lib-only] [--diagnose] [--debug-packets] [--capture-dir <path>] [--capture-max-mib <n>] [--version]"
                        );
                        println!(
                            "  --reattach-seconds: 每轮等待 DLL 重连的窗口，默认 {}s；0=禁用 reattach。最多 {} 轮，总等待上限约 {}s",
                            super::DEFAULT_REATTACH_WINDOW_SECS,
                            super::MAX_REATTACH_ROUNDS,
                            super::DEFAULT_REATTACH_WINDOW_SECS
                                * u64::from(super::MAX_REATTACH_ROUNDS),
                        );
                        std::process::exit(0);
                    }
                    other => anyhow::bail!("未知参数: {other}"),
                }
            }
            Ok(Self {
                config,
                self_uid,
                auto_launch,
                spawn_only,
                lib_only,
                port,
                token,
                log_file,
                no_log_file,
                diagnose,
                debug_packets,
                capture_dir,
                capture_max_mib,
                game_path,
                reattach_seconds,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn game_already_running_enters_reattach_waiting() {
        // 旧实现这里直接以退出码 3 退出，导致 DLL 侧重连永远等不到宿主。
        assert_eq!(
            initial_action(Some(4321), Duration::from_secs(60)),
            InitialAction::Reattach
        );
        // 窗口可配置：非零即启用。
        assert_eq!(
            initial_action(Some(4321), Duration::from_secs(1)),
            InitialAction::Reattach
        );
    }

    #[test]
    fn first_launch_still_injects() {
        // 游戏未运行：保持 spawn_suspended + 注入的旧路径。
        assert_eq!(
            initial_action(None, Duration::from_secs(60)),
            InitialAction::Inject
        );
    }

    #[test]
    fn zero_window_keeps_legacy_already_running_bail() {
        // --reattach-seconds 0 是显式退路：回到旧行为（退出码 3）。
        assert_eq!(
            initial_action(Some(4321), Duration::ZERO),
            InitialAction::BailAlreadyRunning
        );
        assert_eq!(exit_code::GAME_ALREADY_RUNNING, 3);
    }

    #[test]
    fn eof_with_game_alive_returns_to_reattach_waiting() {
        // 读管道 EOF 后游戏还在：不再 return 退出，而是回到等待。
        // 回执（ACK/握手）写失败在 inject 侧同样映射成 PeerClosed，也走这条分支
        // （Windows-only 测试 ack_write_failure_is_peer_closed_not_process_error 覆盖产生端）。
        assert_eq!(
            peer_close_action(true, Duration::from_secs(60), MAX_REATTACH_ROUNDS),
            PeerCloseAction::WaitReconnect
        );
    }

    #[test]
    fn eof_after_game_exit_ends_cleanly() {
        assert_eq!(
            peer_close_action(false, Duration::from_secs(60), MAX_REATTACH_ROUNDS),
            PeerCloseAction::GameExited
        );
        assert_eq!(exit_code::code_of(exit_code::Kind::GameExited), 0);
    }

    #[test]
    fn reattach_window_timeout_maps_to_session_lost_game_alive() {
        // 窗口耗尽（或轮数用完）且游戏还活着：退出码 4，让上游知道须人工重启游戏。
        assert_eq!(
            peer_close_action(true, Duration::ZERO, MAX_REATTACH_ROUNDS),
            PeerCloseAction::SessionLost
        );
        assert_eq!(
            peer_close_action(true, Duration::from_secs(60), 0),
            PeerCloseAction::SessionLost
        );
        assert_eq!(exit_code::SESSION_LOST_GAME_ALIVE, 4);
        assert_eq!(exit_code::code_of(exit_code::Kind::SessionLostGameAlive), 4);
    }

    #[test]
    fn session_status_tracks_attach_and_reattach_rounds() {
        let status = SessionStatus::new();
        assert!(!status.attached());
        assert!(!status.reattach_waiting());
        assert_eq!(status.reattach_attempts(), 0);

        status.mark_reattach_waiting();
        assert!(!status.attached());
        assert!(status.reattach_waiting());
        assert_eq!(status.reattach_attempts(), 1);

        status.mark_attached();
        assert!(status.attached());
        assert!(!status.reattach_waiting());
        assert_eq!(status.reattach_attempts(), 1);

        // 再次断开进入下一轮等待：轮数累加，便于 /health 看出反复重连。
        status.mark_reattach_waiting();
        assert_eq!(status.reattach_attempts(), 2);

        status.mark_detached();
        assert!(!status.attached());
        assert!(!status.reattach_waiting());
    }
}
