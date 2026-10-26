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
mod pipe;
mod proto;
mod server;
mod state;

#[cfg(windows)]
mod inject;

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Instant;

use anyhow::{Context, Result};
use clap_free_args::Args;

fn main() -> Result<()> {
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
        return diagnose(&cfg);
    }

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("创建 tokio 运行时失败")?;

    let chat = state::ChatState::new(cfg.self_uid);
    let connected = Arc::new(AtomicBool::new(false));
    let app = Arc::new(server::AppState {
        chat: Arc::clone(&chat),
        token: cfg.token.clone(),
        started: Instant::now(),
        connected: Arc::clone(&connected),
        port: cfg.port,
        self_uid: cfg.self_uid,
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
            async move {
                if !args.auto_launch { return std::future::pending::<Result<()>>().await; }
                inject::run(chat, connected, args.auto_launch, args.spawn_only, args.lib_only, cancelled).await
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
#[derive(Clone, Default)]
struct TeeWriter {
    file: Option<std::sync::Arc<std::sync::Mutex<Option<std::fs::File>>>>,
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
                file: Some(std::sync::Arc::new(std::sync::Mutex::new(Some(file)))),
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

impl std::io::Write for TeeWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if let Some(file) = &self.file
            && let Ok(mut guard) = file.lock()
            && let Some(file) = guard.as_mut()
        {
            let _ = file.write_all(buf);
        }
        std::io::stderr().write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        if let Some(file) = &self.file
            && let Ok(mut guard) = file.lock()
            && let Some(file) = guard.as_mut()
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
fn diagnose(cfg: &config::Config) -> Result<()> {
    use std::path::PathBuf;

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
    checks.push(game_check(&metadata_path));

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
fn game_check(metadata_path: &std::path::Path) -> (bool, String) {
    use anyhow::Context as _;
    let candidates = [
        r"D:\Genshin Impact Game\YuanShen.exe",
        r"C:\Program Files\Genshin Impact\Genshin Impact Game\GenshinImpact.exe",
        r"C:\Program Files\Genshin Impact\Genshin Impact Game\YuanShen.exe",
    ];
    let Some(game) = candidates
        .iter()
        .map(std::path::Path::new)
        .find(|path| path.is_file())
    else {
        return (
            false,
            "未找到游戏可执行文件（YuanShen.exe / GenshinImpact.exe）".into(),
        );
    };
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
                    "--version" | "-V" => {
                        println!(
                            "Miliastra-Wonderland-ChatBridge {}",
                            env!("CARGO_PKG_VERSION")
                        );
                        std::process::exit(0);
                    }
                    "--help" | "-h" => {
                        println!(
                            "用法: Miliastra-Wonderland-ChatBridge [--config <path>] [--self-uid <uid>] [--port <n>] [--token <s>] [--log-file <path> | --no-log-file] [--no-launch] [--spawn-only] [--lib-only] [--diagnose] [--version]"
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
            })
        }
    }
}
