//! 进程退出码契约。
//!
//! 上游（Miliastra-Wonderland-Music）靠退出码决定下一步：是"等玩家退出后重试"、
//! "下一轮直接重拉"，还是"链路已失效必须人工重启游戏"。
//!
//! 2026-10-30 的线上事故就是所有失败都塌缩成退出码 0/1，上游无法区分，
//! 于是静默降级到 OCR 整晚。所以这些值一旦发布就不要改。

/// 正常结束（游戏退出、Ctrl-C）。
pub const OK: i32 = 0;
/// 启动前的通用错误（端口占用、DLL/metadata 缺失等）。
pub const STARTUP_ERROR: i32 = 1;
/// 游戏已在运行：反作弊会拦截注入。等玩家退出即可，**不需要人工修**。
pub const GAME_ALREADY_RUNNING: i32 = 3;
/// 注入模块主动断开但游戏还活着：链路已失效，**只能重启游戏恢复**。
pub const SESSION_LOST_GAME_ALIVE: i32 = 4;

/// 错误类别。inject 侧的具名错误通过它跨平台传递，避免 main 依赖 Windows 模块。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// 游戏已在运行。
    GameAlreadyRunning,
    /// 注入会话丢失但游戏仍活着。
    SessionLostGameAlive,
    /// 游戏正常退出。
    GameExited,
    /// 其它启动错误。
    Other,
}

/// 把类别映射成退出码。
pub fn code_of(kind: Kind) -> i32 {
    match kind {
        Kind::GameAlreadyRunning => GAME_ALREADY_RUNNING,
        Kind::SessionLostGameAlive => SESSION_LOST_GAME_ALIVE,
        Kind::GameExited => OK,
        Kind::Other => STARTUP_ERROR,
    }
}

/// 从错误链里找注入了类别标记的那一层。
pub fn classify(error: &anyhow::Error) -> i32 {
    for cause in error.chain() {
        if let Some(marked) = cause.downcast_ref::<Marked>() {
            return code_of(marked.0);
        }
    }
    STARTUP_ERROR
}

/// 携带类别的错误包装：inject 侧用它把语义带到 main，不必共享类型定义。
#[derive(Debug)]
pub struct Marked(pub Kind);

impl std::fmt::Display for Marked {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", describe(code_of(self.0)))
    }
}

impl std::error::Error for Marked {}

/// 人类可读的退出码说明，写进日志便于对账。
pub fn describe(code: i32) -> &'static str {
    match code {
        OK => "正常结束",
        STARTUP_ERROR => "启动错误",
        GAME_ALREADY_RUNNING => "游戏已在运行（等玩家退出即可）",
        SESSION_LOST_GAME_ALIVE => "注入链路丢失且游戏仍在运行（须重启游戏）",
        _ => "未知退出码",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exit_codes_are_distinct() {
        // 上游靠区分度做决策，任何两个码相同都是契约破坏。
        let codes = [
            OK,
            STARTUP_ERROR,
            GAME_ALREADY_RUNNING,
            SESSION_LOST_GAME_ALIVE,
        ];
        for (i, left) in codes.iter().enumerate() {
            for right in &codes[i + 1..] {
                assert_ne!(left, right, "退出码不能重复");
            }
        }
    }

    #[test]
    fn classify_reads_the_marker_through_anyhow_context() {
        use anyhow::Context as _;
        let error = Err::<(), _>(Marked(Kind::SessionLostGameAlive))
            .context("外层上下文")
            .unwrap_err();
        assert_eq!(classify(&error), SESSION_LOST_GAME_ALIVE);

        let plain = anyhow::anyhow!("随便什么错误");
        assert_eq!(classify(&plain), STARTUP_ERROR);
    }

    #[test]
    fn every_documented_code_has_a_description() {
        for code in [
            OK,
            STARTUP_ERROR,
            GAME_ALREADY_RUNNING,
            SESSION_LOST_GAME_ALIVE,
        ] {
            assert_ne!(describe(code), "未知退出码");
        }
    }
}
