//! 千星奇域聊天 API。
//!
//! 通过注入游戏的 DLL 嗅探聊天包，解析后经 HTTP/WS 提供给 BOT。
//! 基础使用与许可见 README.md。

// 非 Windows 平台没有注入入口，大部分模块只被测试引用。
// 这是有意为之：注入链路只能在 Windows + 真实游戏下验证。
#![allow(dead_code)]

mod config;
mod extract;
mod inject_stats;
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
    init_tracing();
    let args = Args::parse(std::env::args().skip(1))?;
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

fn init_tracing() {
    use tracing_subscriber::{EnvFilter, fmt};
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("miliastra_wonderland_chatbridge=debug,warn"));
    fmt().with_env_filter(filter).with_target(false).init();
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
                    "--help" | "-h" => {
                        println!(
                            "用法: Miliastra-Wonderland-ChatBridge [--config <path>] [--self-uid <uid>] [--port <n>] [--token <s>] [--no-launch] [--spawn-only] [--lib-only]"
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
            })
        }
    }
}
