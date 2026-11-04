//! DLL 健康心跳的宿主侧运行时状态：最近心跳、阈值判定、/health 快照与看门狗日志。
//!
//! 协议本身（管道名、帧编解码、flags 位）在 crate::health；本模块只消费
//! crate::health::HealthFrame，负责回答"DLL 现在到底怎么样"：
//!
//! - unknown：本次会话/本进程从未收到心跳（旧 DLL 没有健康管道时永远是它）；
//! - forwarding / disabled / reconnecting：收到过心跳，且最近一次心跳仍在
//!   alive 阈值内（默认 3s），状态取自帧里的 flags；
//! - silent：超过 silent 阈值（默认 5s）没有新心跳。3s~5s 之间刻意留一个
//!   宽限带：alive=false 但 state 仍按最后一次 flags 展示，容忍 DLL 侧 GC 停顿
//!   或宿主调度抖动造成的丢拍，避免抖动一两次就误报"静默"。
//!
//! 看门狗线程只负责"silent 只记一次 ERROR、恢复记一次 INFO"的日志；状态本身
//! 永远由 snapshot(now) 按时间现算，所以 /health 不依赖看门狗是否被调度到。

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::{Duration, Instant};

use serde_json::{Value, json};

use crate::health::HealthFrame;

/// 默认 alive 阈值：3 秒内收到过心跳即视为活着（DLL 每 1s 发一帧，允许丢 1-2 拍）。
pub const DEFAULT_ALIVE_MS: u64 = 3_000;
/// 默认 silent 阈值：连续 5 秒没有心跳判定静默（给足 3 个发送周期的余量）。
pub const DEFAULT_SILENT_MS: u64 = 5_000;

/// 视为"真实丢帧"的最大 seq 缺口；更大的跳变按对端重连处理，不计入丢帧。
///
/// DLL 每 1 秒一帧，600 秒（约 10 分钟）的缺口已经远超任何合理的网络/调度抖动，
/// 更可能是宿主长时间未运行或对端重启。用阈值区分两者，避免"重连即误报几万丢帧"。
const MAX_PLAUSIBLE_GAP: u32 = 600;

/// 一次心跳产生的边沿事件，调用方据此决定记什么日志。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct HeartbeatEffect {
    /// 从静默恢复（之前记过 silent ERROR）。
    pub recovered: bool,
    /// "聊天转发已停用"的上升沿（只在 false→true 时为真，避免每秒刷屏）。
    pub disabled_edge: bool,
}
/// 看门狗轮询间隔。检测静默的额外延迟上限就是它。
const WATCHDOG_TICK: Duration = Duration::from_millis(250);

/// 判定阈值。测试可以构造很小的阈值，不必真的等 3/5 秒。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HealthThresholds {
    pub alive: Duration,
    pub silent: Duration,
}

impl Default for HealthThresholds {
    fn default() -> Self {
        Self {
            alive: Duration::from_millis(DEFAULT_ALIVE_MS),
            silent: Duration::from_millis(DEFAULT_SILENT_MS),
        }
    }
}

impl HealthThresholds {
    /// 构造一组阈值；silent 小于 alive 时按 alive 处理，避免状态机出现空洞。
    pub fn new(alive: Duration, silent: Duration) -> Self {
        Self {
            alive,
            silent: if silent < alive { alive } else { silent },
        }
    }
}

/// /health 里 dll.state 的取值。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DllState {
    /// 从未收到心跳（旧 DLL / 尚未连上）。
    Unknown,
    /// DLL 报告聊天转发正常。
    Forwarding,
    /// DLL 报告 PacketOutbox 已停用（活着但不再转发聊天）。
    Disabled,
    /// DLL 报告正在重连宿主。
    Reconnecting,
    /// 超过 silent 阈值没有心跳。
    Silent,
}

impl DllState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::Forwarding => "forwarding",
            Self::Disabled => "disabled",
            Self::Reconnecting => "reconnecting",
            Self::Silent => "silent",
        }
    }
}

/// 一次 /health 快照（字段与 JSON 一一对应，便于测试断言）。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Snapshot {
    pub state: DllState,
    pub alive: bool,
    /// 距最近一次心跳的毫秒数；从未收到为 None（JSON 里是 null）。
    pub heartbeat_age_ms: Option<u64>,
    pub forwarding: bool,
    pub reconnecting: bool,
    pub debug_disabled: bool,
    pub abandoned_limit_reached: bool,
    pub timeout_count: u64,
    pub abandoned_tasks: u64,
    pub seq: u32,
    /// 按 seq 缺口推算的累计丢帧数（wrapping 语义）。
    pub lost_frames: u64,
    pub parse_errors: u64,
    /// DLL 因 packetLength 异常丢弃的包数（协议 v2 诊断字段）。
    pub invalid_length: u64,
    /// DLL 转发过程中的异常次数（协议 v2 诊断字段）。
    pub forward_errors: u64,
    /// 钩子进入网络包路径的次数（协议 v3）。
    pub hook_entered: u64,
    /// 包起始魔数不匹配次数（协议 v3）。
    pub bad_magic: u64,
    /// 魔数通过但 TryGetData 失败次数（协议 v3）。
    pub try_get_data_fail: u64,
    /// 成功提取并转发的包数（协议 v3）。
    pub accepted: u64,
    /// 当前订阅的 cmdId 数量（协议 v4）。
    pub required_count: u64,
    /// 因 cmdId 未订阅被过滤的包数（协议 v4）。
    pub filtered: u64,
    /// 最近被过滤的 cmdId（协议 v4）。
    pub last_filtered_cmd: u64,
    /// 订阅列表前 8 项（协议 v5）。
    pub required_sample: [u64; 8],
    /// 最近见到的 cmdId（协议 v6）。
    pub cmd_seen: [u64; 8],
    /// 已见到的包总数（协议 v6）。
    pub cmd_seen_total: u64,
    /// 不同包的原始 8 字节头（协议 v7）。
    pub raw_header: [u64; 6],
    /// 第一个包的 40 字节探针（协议 v8）。
    pub probe_len: [u64; 5],
}

impl Snapshot {
    /// 转成 /health 的 dll 对象。Snapshot 是 Copy，按值接收更符合 clippy 约定。
    pub fn to_json(self) -> Value {
        json!({
            "alive": self.alive,
            "heartbeatAgeMs": self.heartbeat_age_ms,
            "state": self.state.as_str(),
            "forwarding": self.forwarding,
            "reconnecting": self.reconnecting,
            "debugDisabled": self.debug_disabled,
            "abandonedLimitReached": self.abandoned_limit_reached,
            "timeoutCount": self.timeout_count,
            "abandonedTasks": self.abandoned_tasks,
            "seq": self.seq,
            "lostFrames": self.lost_frames,
            "parseErrors": self.parse_errors,
            "invalidLength": self.invalid_length,
            "forwardErrors": self.forward_errors,
            "hookEntered": self.hook_entered,
            "badMagic": self.bad_magic,
            "tryGetDataFail": self.try_get_data_fail,
            "accepted": self.accepted,
            "requiredCount": self.required_count,
            "filtered": self.filtered,
            "lastFilteredCmd": self.last_filtered_cmd,
            "requiredSample": self.required_sample,
            "cmdSeen": self.cmd_seen,
            "cmdSeenTotal": self.cmd_seen_total,
            "rawHeader": self.raw_header,
            "probeLen": self.probe_len,
        })
    }
}

#[derive(Default)]
struct Inner {
    /// 最近一次心跳的时间点；None = 本次会话从未收到。
    last_heartbeat: Option<Instant>,
    /// 最近一帧（保留 flags/计数，silent 时仍可展示最后一次已知状态）。
    frame: Option<HealthFrame>,
    /// 看门狗是否已经为本轮"静默"记过 ERROR。只影响日志，不影响 snapshot。
    silent_logged: bool,
    /// 健康管道解析失败次数（magic/version/kind 不符，通常意味着旧 DLL）。
    parse_errors: u64,
    /// 按 seq 缺口累计的丢帧数。会话内累计，reset_session 不清零（进程级观测）。
    lost_frames: u64,
    /// 上一帧的 seq，用于推算缺口；None = 本会话还没有基线。
    last_seq: Option<u32>,
    /// 是否已经为"聊天转发已停用"记过日志，用于只报上升沿。
    disabled_logged: bool,
}

/// 线程安全的健康状态。由 main 构造后同时交给 inject（更新）与 server（读取）。
pub struct HealthState {
    thresholds: HealthThresholds,
    inner: Mutex<Inner>,
    watchdog_started: AtomicBool,
}

impl HealthState {
    pub fn new(thresholds: HealthThresholds) -> Arc<Self> {
        Arc::new(Self {
            thresholds,
            inner: Mutex::new(Inner::default()),
            watchdog_started: AtomicBool::new(false),
        })
    }

    pub fn with_defaults() -> Arc<Self> {
        Self::new(HealthThresholds::default())
    }

    pub fn thresholds(&self) -> HealthThresholds {
        self.thresholds
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// 收到一帧心跳，返回本次需要写日志的**边沿事件**。
    ///
    /// 幂等：同一时刻重复调用只更新时间点。
    pub fn note_heartbeat(&self, frame: HealthFrame, at: Instant) -> HeartbeatEffect {
        let mut inner = self.lock();
        let previous_age = inner
            .last_heartbeat
            .map(|last| at.saturating_duration_since(last));
        let recovered = inner.silent_logged
            || previous_age
                .map(|age| age >= self.thresholds.silent)
                .unwrap_or(false);
        inner.silent_logged = false;
        inner.last_heartbeat = Some(at);

        // 丢帧统计：seq 每帧 +1，缺口即丢帧。必须用 wrapping 语义（u32 会回绕）。
        // 大跳变（> MAX_PLAUSIBLE_GAP）按「对端重连/长时间断开」处理，只重设基线
        // 不累加——否则一次重连就会误报几万丢帧。
        if let Some(previous) = inner.last_seq {
            let gap = frame.seq.wrapping_sub(previous.wrapping_add(1));
            if gap != 0 && gap <= MAX_PLAUSIBLE_GAP {
                inner.lost_frames = inner.lost_frames.saturating_add(u64::from(gap));
            }
        }
        inner.last_seq = Some(frame.seq);

        // disabled 上升沿：本功能存在的理由就是让「DLL 活着但不转发」可见。
        // 只在上跳沿报告一次，避免每秒刷屏。
        let disabled_now = frame.packet_outbox_disabled();
        let disabled_edge = disabled_now && !inner.disabled_logged;
        inner.disabled_logged = disabled_now;

        inner.frame = Some(frame);
        HeartbeatEffect {
            recovered,
            disabled_edge,
        }
    }

    /// 健康管道上出现无法解析的前缀（magic/version/kind 不符）。
    ///
    /// 返回累计失败次数；调用方据此决定日志频率（第一次 + 每 N 次）。
    pub fn note_parse_error(&self) -> u64 {
        let mut inner = self.lock();
        inner.parse_errors = inner.parse_errors.saturating_add(1);
        inner.parse_errors
    }

    pub fn parse_errors(&self) -> u64 {
        self.lock().parse_errors
    }

    /// 新建/重建健康管道时调用：回到 unknown，直到首个心跳。
    ///
    /// 解析失败计数不清零（进程级观测，用于看"客户端是不是旧 DLL"）。
    pub fn reset_session(&self) {
        let mut inner = self.lock();
        inner.last_heartbeat = None;
        inner.frame = None;
        inner.silent_logged = false;
        // 新会话的 seq 基线必须重建，否则重连后的第一帧会被当成巨大缺口。
        inner.last_seq = None;
        // 边沿标记随会话重置：新会话若仍是 disabled，应该再报一次。
        inner.disabled_logged = false;
    }

    /// 看门狗周期调用：若本轮首次进入 silent，返回 true（只返回一次）。
    ///
    /// 没有收到过心跳（unknown）不会报 silent，也不会刷日志。
    pub fn poll_silent(&self, now: Instant) -> bool {
        let mut inner = self.lock();
        let Some(last) = inner.last_heartbeat else {
            return false;
        };
        if inner.silent_logged {
            return false;
        }
        if now.saturating_duration_since(last) >= self.thresholds.silent {
            inner.silent_logged = true;
            return true;
        }
        false
    }

    /// 当前快照。now 由调用方传入，测试可用合成时间。
    pub fn snapshot(&self, now: Instant) -> Snapshot {
        let inner = self.lock();
        let parse_errors = inner.parse_errors;
        let lost_frames = inner.lost_frames;
        let Some(last) = inner.last_heartbeat else {
            return Snapshot {
                state: DllState::Unknown,
                alive: false,
                heartbeat_age_ms: None,
                forwarding: false,
                reconnecting: false,
                debug_disabled: false,
                abandoned_limit_reached: false,
                timeout_count: 0,
                abandoned_tasks: 0,
                seq: 0,
                lost_frames,
                parse_errors,
                invalid_length: 0,
                forward_errors: 0,
                hook_entered: 0,
                bad_magic: 0,
                try_get_data_fail: 0,
                accepted: 0,
                required_count: 0,
                filtered: 0,
                last_filtered_cmd: 0,
                required_sample: [0; 8],
                cmd_seen: [0; 8],
                cmd_seen_total: 0,
                raw_header: [0; 6],
                probe_len: [0; 5],
            };
        };
        let age = now.saturating_duration_since(last);
        let age_ms = age.as_millis().min(u128::from(u64::MAX)) as u64;
        let alive = age <= self.thresholds.alive;
        let silent = age >= self.thresholds.silent;
        let frame = inner.frame.unwrap_or(HealthFrame::new(0, 0, 0, 0));
        let state = if silent {
            DllState::Silent
        } else if frame.packet_outbox_disabled() {
            DllState::Disabled
        } else if frame.reconnecting() {
            DllState::Reconnecting
        } else {
            DllState::Forwarding
        };
        Snapshot {
            state,
            alive,
            heartbeat_age_ms: Some(age_ms),
            forwarding: !frame.packet_outbox_disabled() && !frame.reconnecting(),
            reconnecting: frame.reconnecting(),
            debug_disabled: frame.debug_outbox_disabled(),
            abandoned_limit_reached: frame.abandoned_limit_reached(),
            timeout_count: frame.timeout_count,
            abandoned_tasks: frame.abandoned_tasks,
            seq: frame.seq,
            lost_frames,
            parse_errors,
            invalid_length: frame.invalid_length,
            forward_errors: frame.forward_errors,
            hook_entered: frame.hook_entered,
            bad_magic: frame.bad_magic,
            try_get_data_fail: frame.try_get_data_fail,
            accepted: frame.accepted,
            required_count: frame.required_count,
            filtered: frame.filtered,
            last_filtered_cmd: frame.last_filtered_cmd,
            required_sample: frame.required_sample,
            cmd_seen: frame.cmd_seen,
            cmd_seen_total: frame.cmd_seen_total,
            raw_header: frame.raw_header,
            probe_len: frame.probe_len,
        }
    }

    /// 当前 /health 的 dll 对象（用真实时间）。
    pub fn snapshot_json(&self) -> Value {
        self.snapshot(Instant::now()).to_json()
    }

    /// 启动看门狗线程（幂等）。线程只做 silent 日志，不改变状态。
    pub fn start_watchdog(self: &Arc<Self>) {
        if self.watchdog_started.swap(true, Ordering::AcqRel) {
            return;
        }
        let weak: Weak<Self> = Arc::downgrade(self);
        let spawned = std::thread::Builder::new()
            .name("mwc-dll-health-watchdog".to_owned())
            .spawn(move || loop {
                std::thread::sleep(WATCHDOG_TICK);
                let Some(state) = weak.upgrade() else {
                    return;
                };
                if state.poll_silent(Instant::now()) {
                    let snapshot = state.snapshot(Instant::now());
                    tracing::error!(
                        "DLL 健康心跳已静默：{}ms 未收到心跳（阈值 {}ms），/health dll.state=silent。\
                         聊天链路可能已停转，请结合 dll.timeoutCount/abandonedTasks 判断",
                        snapshot.heartbeat_age_ms.unwrap_or(0),
                        state.thresholds.silent.as_millis()
                    );
                }
            });
        if let Err(error) = spawned {
            self.watchdog_started.store(false, Ordering::Release);
            tracing::warn!("启动 DLL 健康看门狗线程失败（不影响 /health 现算状态）：{error}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::health::{
        FLAG_DEBUG_OUTBOX_DISABLED, FLAG_PACKET_OUTBOX_DISABLED, FLAG_RECONNECTING,
    };

    fn thresholds() -> HealthThresholds {
        HealthThresholds::new(Duration::from_millis(300), Duration::from_millis(500))
    }

    fn forwarding(seq: u32) -> HealthFrame {
        HealthFrame::new(seq, 0, 0, 0)
    }

    #[test]
    fn unknown_before_first_heartbeat() {
        let state = HealthState::new(thresholds());
        let snapshot = state.snapshot(Instant::now());
        assert_eq!(snapshot.state, DllState::Unknown);
        assert!(!snapshot.alive);
        assert_eq!(snapshot.heartbeat_age_ms, None);
        assert!(!snapshot.forwarding);
        assert_eq!(snapshot.parse_errors, 0);
        let value = snapshot.to_json();
        assert_eq!(value["state"], "unknown");
        assert_eq!(value["alive"], false);
        assert_eq!(value["heartbeatAgeMs"], Value::Null);
    }

    #[test]
    fn heartbeat_reports_alive_and_age() {
        let state = HealthState::new(thresholds());
        let t0 = Instant::now();
        let recovered = state
            .note_heartbeat(HealthFrame::new(7, 0, 42, 3), t0)
            .recovered;
        assert!(!recovered);
        let snapshot = state.snapshot(t0 + Duration::from_millis(100));
        assert_eq!(snapshot.state, DllState::Forwarding);
        assert!(snapshot.alive);
        assert_eq!(snapshot.heartbeat_age_ms, Some(100));
        assert!(snapshot.forwarding);
        assert_eq!(snapshot.seq, 7);
        assert_eq!(snapshot.timeout_count, 42);
        assert_eq!(snapshot.abandoned_tasks, 3);
    }

    #[test]
    fn flags_map_to_disabled_and_reconnecting_states() {
        let state = HealthState::new(thresholds());
        let t0 = Instant::now();
        state.note_heartbeat(
            HealthFrame::new(
                1,
                FLAG_PACKET_OUTBOX_DISABLED | FLAG_DEBUG_OUTBOX_DISABLED,
                9,
                0,
            ),
            t0,
        );
        let snapshot = state.snapshot(t0 + Duration::from_millis(10));
        assert_eq!(snapshot.state, DllState::Disabled);
        assert!(!snapshot.forwarding);
        assert!(snapshot.debug_disabled);
        assert_eq!(snapshot.timeout_count, 9);

        let t1 = t0 + Duration::from_millis(20);
        state.note_heartbeat(HealthFrame::new(2, FLAG_RECONNECTING, 9, 1), t1);
        let snapshot = state.snapshot(t1 + Duration::from_millis(10));
        assert_eq!(snapshot.state, DllState::Reconnecting);
        assert!(snapshot.reconnecting);
        assert!(!snapshot.forwarding);
    }

    #[test]
    fn grace_band_is_not_alive_but_not_silent() {
        let state = HealthState::new(thresholds());
        let t0 = Instant::now();
        state.note_heartbeat(forwarding(1), t0);
        // alive=300ms, silent=500ms：400ms 处于宽限带。
        let snapshot = state.snapshot(t0 + Duration::from_millis(400));
        assert!(!snapshot.alive);
        assert_eq!(
            snapshot.state,
            DllState::Forwarding,
            "宽限带内仍展示最后一次已知状态"
        );
        // 超过 silent 才算静默。
        let snapshot = state.snapshot(t0 + Duration::from_millis(500));
        assert_eq!(snapshot.state, DllState::Silent);
        assert!(!snapshot.alive);
    }

    #[test]
    fn silent_logged_once_and_recovered_on_heartbeat() {
        let state = HealthState::new(thresholds());
        let t0 = Instant::now();
        state.note_heartbeat(forwarding(1), t0);
        // 从未收到心跳之前不会误报。
        let fresh = HealthState::new(thresholds());
        assert!(!fresh.poll_silent(t0 + Duration::from_secs(10)));
        // 第一次到达 silent 阈值：只返回一次 true。
        assert!(state.poll_silent(t0 + Duration::from_millis(600)));
        assert!(!state.poll_silent(t0 + Duration::from_millis(700)));
        // 恢复：note_heartbeat 返回 true，之后 snapshot 回到 forwarding。
        let recovered = state
            .note_heartbeat(forwarding(2), t0 + Duration::from_millis(800))
            .recovered;
        assert!(recovered);
        let snapshot = state.snapshot(t0 + Duration::from_millis(810));
        assert_eq!(snapshot.state, DllState::Forwarding);
        assert!(snapshot.alive);
        // 恢复后再次静默可以再次报 ERROR。
        assert!(state.poll_silent(t0 + Duration::from_millis(1400)));
    }

    #[test]
    fn reset_session_returns_to_unknown() {
        let state = HealthState::new(thresholds());
        let t0 = Instant::now();
        state.note_heartbeat(forwarding(1), t0);
        state.note_parse_error();
        state.reset_session();
        let snapshot = state.snapshot(t0 + Duration::from_millis(10));
        assert_eq!(snapshot.state, DllState::Unknown);
        assert_eq!(snapshot.heartbeat_age_ms, None);
        assert!(!snapshot.alive);
        // 解析失败计数是进程级观测，不在 reattach 时清零。
        assert_eq!(snapshot.parse_errors, 1);
        // reset 之后收到新心跳立即恢复 alive（reset 本身不是 silent 恢复，故 recovered=false）。
        let recovered = state
            .note_heartbeat(forwarding(2), t0 + Duration::from_millis(20))
            .recovered;
        assert!(!recovered);
        let snapshot = state.snapshot(t0 + Duration::from_millis(30));
        assert_eq!(snapshot.state, DllState::Forwarding);
        assert!(snapshot.alive);
    }

    #[test]
    fn thresholds_silent_is_clamped_to_alive() {
        let thresholds = HealthThresholds::new(Duration::from_secs(1), Duration::from_millis(1));
        assert_eq!(thresholds.silent, Duration::from_secs(1));
    }

    #[test]
    fn json_shape_matches_health_contract() {
        let state = HealthState::new(thresholds());
        let t0 = Instant::now();
        state.note_heartbeat(HealthFrame::new(1234, FLAG_DEBUG_OUTBOX_DISABLED, 5, 2), t0);
        let value = state.snapshot(t0 + Duration::from_millis(12)).to_json();
        for key in [
            "alive",
            "heartbeatAgeMs",
            "state",
            "forwarding",
            "reconnecting",
            "debugDisabled",
            "abandonedLimitReached",
            "timeoutCount",
            "abandonedTasks",
            "seq",
            "parseErrors",
        ] {
            assert!(value.get(key).is_some(), "缺少 dll.{key}");
        }
        assert_eq!(value["state"], "forwarding");
        assert_eq!(value["alive"], true);
        assert_eq!(value["heartbeatAgeMs"], 12);
        assert_eq!(value["timeoutCount"], 5);
        assert_eq!(value["abandonedTasks"], 2);
        assert_eq!(value["seq"], 1234);
        assert_eq!(value["debugDisabled"], true);
        assert_eq!(value["reconnecting"], false);
    }

    /// 丢帧统计：seq 缺口应累加到 lost_frames（wrapping 语义）。
    #[test]
    fn lost_frames_counts_sequence_gaps() {
        let state = HealthState::with_defaults();
        let t0 = Instant::now();
        state.note_heartbeat(HealthFrame::new(1, 0, 0, 0), t0);
        assert_eq!(state.snapshot(t0).lost_frames, 0, "首帧不计数");

        state.note_heartbeat(HealthFrame::new(2, 0, 0, 0), t0);
        assert_eq!(state.snapshot(t0).lost_frames, 0, "连续帧不计数");

        // 跳过 3、4：应记 2 帧丢失。
        state.note_heartbeat(HealthFrame::new(5, 0, 0, 0), t0);
        assert_eq!(state.snapshot(t0).lost_frames, 2, "缺口 2 应累加 2");
    }

    /// seq 回绕必须按 wrapping 语义处理，不能因 u32 回绕误报巨量丢帧。
    #[test]
    fn lost_frames_handles_sequence_wraparound() {
        let state = HealthState::with_defaults();
        let t0 = Instant::now();
        state.note_heartbeat(HealthFrame::new(u32::MAX, 0, 0, 0), t0);
        state.note_heartbeat(HealthFrame::new(0, 0, 0, 0), t0);
        assert_eq!(
            state.snapshot(t0).lost_frames,
            0,
            "回绕后连续，不应计入丢帧"
        );
        state.note_heartbeat(HealthFrame::new(2, 0, 0, 0), t0);
        assert_eq!(state.snapshot(t0).lost_frames, 1, "回绕后跳 1 帧应计 1");
    }

    /// 超大跳变（对端重连）不应被当成丢帧，否则一次重连会误报几万帧。
    #[test]
    fn implausible_gap_is_treated_as_reconnect_not_loss() {
        let state = HealthState::with_defaults();
        let t0 = Instant::now();
        state.note_heartbeat(HealthFrame::new(1, 0, 0, 0), t0);
        state.note_heartbeat(HealthFrame::new(1_000_000, 0, 0, 0), t0);
        assert_eq!(
            state.snapshot(t0).lost_frames,
            0,
            "大跳变按重连处理，不计丢帧"
        );
    }

    /// 会话重建后必须重建 seq 基线，否则首帧会被算成巨大缺口。
    #[test]
    fn reset_session_rebuilds_sequence_baseline() {
        let state = HealthState::with_defaults();
        let t0 = Instant::now();
        state.note_heartbeat(HealthFrame::new(500, 0, 0, 0), t0);
        let before = state.snapshot(t0).lost_frames;
        state.reset_session();
        state.note_heartbeat(HealthFrame::new(1, 0, 0, 0), t0);
        assert_eq!(
            state.snapshot(t0).lost_frames,
            before,
            "新会话首帧不应因 seq 从 1 重开而计入丢帧"
        );
    }

    /// disabled 只在上升沿报告一次，避免每帧刷日志。
    #[test]
    fn disabled_edge_is_reported_once() {
        let state = HealthState::with_defaults();
        let t0 = Instant::now();
        let down = |seq: u32| HealthFrame::new(seq, FLAG_PACKET_OUTBOX_DISABLED, 1, 0);

        // 首帧就是 disabled：也要报一次，否则「一启动就坏了」这种情况永远没日志。
        assert!(
            state.note_heartbeat(down(1), t0).disabled_edge,
            "首次 disabled 应报告"
        );
        assert!(
            !state.note_heartbeat(down(2), t0).disabled_edge,
            "持续 disabled 不应重复报告"
        );
        // 恢复（forwarding）后再进入 disabled，应再报一次。
        state.note_heartbeat(HealthFrame::new(3, 0, 0, 0), t0);
        assert!(
            state.note_heartbeat(down(4), t0).disabled_edge,
            "再次进入 disabled 应重新报告"
        );
    }
}
