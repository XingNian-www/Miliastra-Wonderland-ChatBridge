//! 运行期诊断：让"慢"和"停"在没有控制台、没有调试器的情况下也能看见。
//!
//! 背景：ChatBridge 通常由 Music 以隐藏窗口拉起，stdout 直接进 NUL，
//! 一旦出现"消息晚十几秒"或"彻底不动"，现场没有任何证据可查。本模块把
//! 关键状态同时保存在进程内，通过 GET /debug/metrics 与 GET /debug/events
//! 暴露，并配合文件日志留证。
//!
//! 设计目标：
//! 1. 能回答三个问题：还活着吗（fps/年龄/连接）、慢在哪一段
//!    （ack/dispatch 分位、自旋命中率、缓冲、订阅滞后）、刚才发生了什么（事件环）。
//! 2. 热路径便宜：每帧/每包只有若干原子加与一次定长环形写；互斥锁只保护
//!    事件环、直方图和 1 秒速率窗口，且都是短临界区。
//! 3. 不记录内容：只存长度、cmd、耗时，不存聊天正文，避免敏感数据外泄。

use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{LazyLock, Mutex, MutexGuard, OnceLock};
use std::time::Instant;

use serde::Serialize;
use serde_json::{Value, json};

/// 延迟样本环长度：够算 p50/p99，又不占内存。
const LATENCY_SAMPLES: usize = 256;
/// 事件环长度：足够覆盖一次重连/一次排障窗口。
const EVENT_RING: usize = 64;
/// 速率窗口：1 秒。
const RATE_WINDOW_MS: u64 = 1_000;
/// 打印收包汇总的间隔（按包计数）。
const SUMMARY_EVERY: u64 = 200;
/// 汇总日志里列出的高频 cmd 数量。
const TOP_CMDS: usize = 8;

static START: OnceLock<Instant> = OnceLock::new();

static FRAMES: AtomicU64 = AtomicU64::new(0);
static BYTES: AtomicU64 = AtomicU64::new(0);
static PACKETS: AtomicU64 = AtomicU64::new(0);
static ACK_COUNT: AtomicU64 = AtomicU64::new(0);
static ACK_TOTAL_US: AtomicU64 = AtomicU64::new(0);
static ACK_MAX_US: AtomicU64 = AtomicU64::new(0);
static DISPATCH_COUNT: AtomicU64 = AtomicU64::new(0);
static DISPATCH_TOTAL_US: AtomicU64 = AtomicU64::new(0);
static DISPATCH_MAX_US: AtomicU64 = AtomicU64::new(0);
static SPIN_HIT: AtomicU64 = AtomicU64::new(0);
static SPIN_MISS: AtomicU64 = AtomicU64::new(0);
static IDLE_POLLS: AtomicU64 = AtomicU64::new(0);
static MESSAGES: AtomicU64 = AtomicU64::new(0);
static LAST_MESSAGE_ID: AtomicU64 = AtomicU64::new(0);
static LAST_MESSAGE_TS: AtomicU64 = AtomicU64::new(0);
static LAST_MESSAGE_MS: AtomicU64 = AtomicU64::new(0);
static LAST_FRAME_MS: AtomicU64 = AtomicU64::new(0);
static BUFFERED_BYTES: AtomicU64 = AtomicU64::new(0);
static SUBSCRIBERS: AtomicI64 = AtomicI64::new(0);
static LAGGED_TOTAL: AtomicU64 = AtomicU64::new(0);
static DLL_CONNECTED: AtomicBool = AtomicBool::new(false);

static ACK_SAMPLES: LazyLock<Mutex<Histogram>> = LazyLock::new(|| Mutex::new(Histogram::new()));
static DISPATCH_SAMPLES: LazyLock<Mutex<Histogram>> =
    LazyLock::new(|| Mutex::new(Histogram::new()));
static RATES: LazyLock<Mutex<RateMeter>> = LazyLock::new(|| Mutex::new(RateMeter::default()));
static EVENTS: LazyLock<Mutex<EventLog>> = LazyLock::new(|| Mutex::new(EventLog::default()));
static CMDS: LazyLock<Mutex<BTreeMap<u16, u64>>> = LazyLock::new(|| Mutex::new(BTreeMap::new()));

/// 记录进程启动时刻。必须在其它诊断调用之前执行一次；重复调用无副作用。
pub fn init() {
    let _ = START.set(Instant::now());
    note(
        "service_start",
        format!("version={}", env!("CARGO_PKG_VERSION")),
    );
}

fn now_ms() -> u64 {
    START
        .get()
        .map(|started| started.elapsed().as_millis() as u64)
        .unwrap_or(0)
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    // 诊断数据没有一致性要求，锁中毒时继续用，不要让排查本身变成故障。
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// 定长延迟样本环，用于分位数。
struct Histogram {
    samples: [u32; LATENCY_SAMPLES],
    next: usize,
    filled: usize,
}

impl Histogram {
    fn new() -> Self {
        Self {
            samples: [0; LATENCY_SAMPLES],
            next: 0,
            filled: 0,
        }
    }

    fn push(&mut self, micros: u64) {
        self.samples[self.next] = micros.min(u32::MAX as u64) as u32;
        self.next = (self.next + 1) % LATENCY_SAMPLES;
        self.filled = (self.filled + 1).min(LATENCY_SAMPLES);
    }

    /// p 取 0.0-1.0；无样本返回 0。
    fn percentile(&self, p: f64) -> u32 {
        if self.filled == 0 {
            return 0;
        }
        let mut sorted = [0u32; LATENCY_SAMPLES];
        sorted[..self.filled].copy_from_slice(&self.samples[..self.filled]);
        let slice = &mut sorted[..self.filled];
        slice.sort_unstable();
        let index = ((slice.len() - 1) as f64 * p).round() as usize;
        slice[index]
    }
}

/// 1 秒滑窗速率。读的时候按经过时间折算，避免"窗口边界"造成 0/突刺。
#[derive(Default)]
struct RateMeter {
    last_ms: u64,
    frames: u64,
    packets: u64,
}

impl RateMeter {
    fn tick(&mut self, now: u64, frames: u64, packets: u64) {
        // 先判断是否跨窗口再累加，否则窗口第一秒的样本会被刚清掉。
        if now.saturating_sub(self.last_ms) >= RATE_WINDOW_MS {
            self.last_ms = now;
            self.frames = 0;
            self.packets = 0;
        }
        self.frames += frames;
        self.packets += packets;
    }

    fn per_second(&mut self, now: u64) -> (f64, f64) {
        let elapsed = now.saturating_sub(self.last_ms).max(1);
        if elapsed > 2 * RATE_WINDOW_MS {
            // 长时间没有流量：速率就是 0，不要用陈旧窗口算。
            self.last_ms = now;
            self.frames = 0;
            self.packets = 0;
            return (0.0, 0.0);
        }
        (
            self.frames as f64 * 1_000.0 / elapsed as f64,
            self.packets as f64 * 1_000.0 / elapsed as f64,
        )
    }
}

/// 事件环：只保留摘要，不保留正文。
#[derive(Default)]
struct EventLog {
    events: VecDeque<EventView>,
}

/// 一条诊断事件。
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EventView {
    /// 相对进程启动的毫秒数。
    pub offset_ms: u64,
    /// 事件类型，稳定字符串，便于脚本过滤。
    pub kind: String,
    /// 人类可读细节；不包含消息正文。
    pub detail: String,
}

/// 记一条事件。事件环满时丢最旧的。
pub fn note(kind: &str, detail: impl Into<String>) {
    let view = EventView {
        offset_ms: now_ms(),
        kind: kind.to_owned(),
        detail: detail.into(),
    };
    let mut log = lock(&EVENTS);
    if log.events.len() >= EVENT_RING {
        log.events.pop_front();
    }
    log.events.push_back(view);
}

/// 收到底层一帧（一次成功 read）。bytes 是本次读到的字节数。
pub fn frame(bytes: usize) {
    let now = now_ms();
    FRAMES.fetch_add(1, Ordering::Relaxed);
    BYTES.fetch_add(bytes as u64, Ordering::Relaxed);
    LAST_FRAME_MS.store(now, Ordering::Relaxed);
    lock(&RATES).tick(now, 1, 0);
}

/// 收到一个已订阅的包。会更新 cmd 直方图，并按计数打印汇总日志。
pub fn packet(cmd: u16, len: usize) {
    let total = PACKETS.fetch_add(1, Ordering::Relaxed) + 1;
    lock(&RATES).tick(now_ms(), 0, 1);
    let mut cmds = lock(&CMDS);
    *cmds.entry(cmd).or_insert(0) += 1;
    if total.is_multiple_of(SUMMARY_EVERY) {
        let top: Vec<String> = cmds
            .iter()
            .rev()
            .take(TOP_CMDS)
            .map(|(cmd, count)| format!("{cmd}×{count}"))
            .collect();
        tracing::info!(
            "收包统计: 共 {total}，字节 {}，最高频 {}",
            BYTES.load(Ordering::Relaxed),
            top.join(" ")
        );
    }
    let _ = len;
}

/// 回执写入耗时（微秒）。
pub fn ack(micros: u64) {
    ACK_COUNT.fetch_add(1, Ordering::Relaxed);
    ACK_TOTAL_US.fetch_add(micros, Ordering::Relaxed);
    update_max(&ACK_MAX_US, micros);
    lock(&ACK_SAMPLES).push(micros);
}

/// dispatch（解析 + 入库 + 广播）耗时（微秒）。
pub fn dispatch(micros: u64) {
    DISPATCH_COUNT.fetch_add(1, Ordering::Relaxed);
    DISPATCH_TOTAL_US.fetch_add(micros, Ordering::Relaxed);
    update_max(&DISPATCH_MAX_US, micros);
    lock(&DISPATCH_SAMPLES).push(micros);
}

/// 自旋窗口结果：true 表示窗口内等到了下一包。
pub fn spin(hit: bool) {
    if hit {
        SPIN_HIT.fetch_add(1, Ordering::Relaxed);
    } else {
        SPIN_MISS.fetch_add(1, Ordering::Relaxed);
    }
}

/// 一次空闲轮询（50ms 睡眠）。
pub fn idle_poll() {
    IDLE_POLLS.fetch_add(1, Ordering::Relaxed);
}

/// 当前管道缓冲里尚未解析的字节数。
pub fn buffered(bytes: usize) {
    BUFFERED_BYTES.store(bytes as u64, Ordering::Relaxed);
}

/// 推送了一条消息到订阅者。
pub fn message(id: u64, ts: u64) {
    MESSAGES.fetch_add(1, Ordering::Relaxed);
    LAST_MESSAGE_ID.store(id, Ordering::Relaxed);
    LAST_MESSAGE_TS.store(ts, Ordering::Relaxed);
    LAST_MESSAGE_MS.store(now_ms(), Ordering::Relaxed);
}

/// DLL 管道是否已连通。
pub fn dll_connected(connected: bool) {
    if DLL_CONNECTED.swap(connected, Ordering::AcqRel) != connected {
        note(
            if connected {
                "dll_connected"
            } else {
                "dll_disconnected"
            },
            "",
        );
    }
}

/// 订阅者计数的 RAII 守卫，异常退出也能回收。
pub struct SubscriberGuard;

impl Drop for SubscriberGuard {
    fn drop(&mut self) {
        SUBSCRIBERS.fetch_sub(1, Ordering::Relaxed);
    }
}

/// 登记一个 WS 订阅者，返回的守卫析构时自动递减。
pub fn subscriber() -> SubscriberGuard {
    SUBSCRIBERS.fetch_add(1, Ordering::Relaxed);
    SubscriberGuard
}

/// WS 订阅者因落后被断开。
pub fn lagged(skipped: u64) {
    LAGGED_TOTAL.fetch_add(1, Ordering::Relaxed);
    note("ws_lagged", format!("skipped={skipped}"));
}

fn update_max(target: &AtomicU64, value: u64) {
    let mut current = target.load(Ordering::Relaxed);
    while value > current {
        match target.compare_exchange_weak(current, value, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return,
            Err(actual) => current = actual,
        }
    }
}

fn average_us(total: &AtomicU64, count: &AtomicU64) -> u64 {
    let count = count.load(Ordering::Relaxed);
    if count == 0 {
        return 0;
    }
    total.load(Ordering::Relaxed) / count
}

/// 构建信息：版本固定，sha/时间由 CI 注入（缺失时为 unknown）。
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BuildInfo {
    pub version: &'static str,
    pub sha: &'static str,
    pub built_at: &'static str,
}

impl BuildInfo {
    fn current() -> Self {
        Self {
            version: env!("CARGO_PKG_VERSION"),
            sha: option_env!("CHATBRIDGE_BUILD_SHA").unwrap_or("unknown"),
            built_at: option_env!("CHATBRIDGE_BUILD_TIME").unwrap_or("unknown"),
        }
    }
}

/// 管道 / 收包侧指标。
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PipeStats {
    pub dll_connected: bool,
    pub frames_total: u64,
    pub frames_per_sec: f64,
    pub packets_total: u64,
    pub packets_per_sec: f64,
    pub bytes_total: u64,
    /// 距最近一帧的毫秒数；还没收到过任何帧时为 null（比 0 明确）。
    pub last_frame_age_ms: Option<u64>,
    pub buffered_bytes: u64,
    pub ack_count: u64,
    pub ack_avg_us: u64,
    pub ack_p50_us: u32,
    pub ack_p99_us: u32,
    pub ack_max_us: u64,
    pub dispatch_avg_us: u64,
    pub dispatch_p50_us: u32,
    pub dispatch_p99_us: u32,
    pub dispatch_max_us: u64,
    pub spin_hit: u64,
    pub spin_miss: u64,
    pub spin_hit_rate: f64,
    pub idle_polls: u64,
}

/// 消息侧指标。
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChatStats {
    pub messages_total: u64,
    pub last_message_id: u64,
    pub last_message_ts: u64,
    /// 距最近一条消息的毫秒数；还没有消息时为 null。
    pub last_message_age_ms: Option<u64>,
    pub subscribers: i64,
    pub lagged_total: u64,
}

/// 完整快照。
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Snapshot {
    pub uptime_ms: u64,
    pub build: BuildInfo,
    pub pipe: PipeStats,
    pub chat: ChatStats,
    pub top_cmds: Vec<CmdCount>,
}

/// 高频 cmd 计数。
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CmdCount {
    pub cmd: u16,
    pub count: u64,
}

/// 当前指标快照。耗时字段单位都是微秒。
pub fn snapshot() -> Snapshot {
    let now = now_ms();
    let (frames_per_sec, packets_per_sec) = lock(&RATES).per_second(now);
    let mut top_cmds: Vec<CmdCount> = lock(&CMDS)
        .iter()
        .map(|(cmd, count)| CmdCount {
            cmd: *cmd,
            count: *count,
        })
        .collect();
    top_cmds.sort_by(|left, right| right.count.cmp(&left.count).then(left.cmd.cmp(&right.cmd)));
    top_cmds.truncate(TOP_CMDS);

    // 同一把锁的分位要先各算一次：写在结构体字面量里会同时活到语句结束，
    // 两次 lock 同一把非重入锁就是死锁（这个坑已经踩过一次）。
    let ack_p50_us = lock(&ACK_SAMPLES).percentile(0.50);
    let ack_p99_us = lock(&ACK_SAMPLES).percentile(0.99);
    let dispatch_p50_us = lock(&DISPATCH_SAMPLES).percentile(0.50);
    let dispatch_p99_us = lock(&DISPATCH_SAMPLES).percentile(0.99);
    let spin_hit = SPIN_HIT.load(Ordering::Relaxed);
    let spin_miss = SPIN_MISS.load(Ordering::Relaxed);
    let spin_total = spin_hit + spin_miss;
    let last_frame = LAST_FRAME_MS.load(Ordering::Relaxed);
    let last_message = LAST_MESSAGE_MS.load(Ordering::Relaxed);

    Snapshot {
        uptime_ms: now,
        build: BuildInfo::current(),
        pipe: PipeStats {
            dll_connected: DLL_CONNECTED.load(Ordering::Acquire),
            frames_total: FRAMES.load(Ordering::Relaxed),
            frames_per_sec,
            packets_total: PACKETS.load(Ordering::Relaxed),
            packets_per_sec,
            bytes_total: BYTES.load(Ordering::Relaxed),
            last_frame_age_ms: (last_frame != 0).then(|| now.saturating_sub(last_frame)),
            buffered_bytes: BUFFERED_BYTES.load(Ordering::Relaxed),
            ack_count: ACK_COUNT.load(Ordering::Relaxed),
            ack_avg_us: average_us(&ACK_TOTAL_US, &ACK_COUNT),
            ack_p50_us,
            ack_p99_us,
            ack_max_us: ACK_MAX_US.load(Ordering::Relaxed),
            dispatch_avg_us: average_us(&DISPATCH_TOTAL_US, &DISPATCH_COUNT),
            dispatch_p50_us,
            dispatch_p99_us,
            dispatch_max_us: DISPATCH_MAX_US.load(Ordering::Relaxed),
            spin_hit,
            spin_miss,
            spin_hit_rate: if spin_total == 0 {
                0.0
            } else {
                spin_hit as f64 / spin_total as f64
            },
            idle_polls: IDLE_POLLS.load(Ordering::Relaxed),
        },
        chat: ChatStats {
            messages_total: MESSAGES.load(Ordering::Relaxed),
            last_message_id: LAST_MESSAGE_ID.load(Ordering::Relaxed),
            last_message_ts: LAST_MESSAGE_TS.load(Ordering::Relaxed),
            last_message_age_ms: (last_message != 0).then(|| now.saturating_sub(last_message)),
            subscribers: SUBSCRIBERS.load(Ordering::Relaxed),
            lagged_total: LAGGED_TOTAL.load(Ordering::Relaxed),
        },
        top_cmds,
    }
}

/// 最近的事件（旧的在前）。
pub fn events() -> Vec<EventView> {
    lock(&EVENTS).events.iter().cloned().collect()
}

/// /health 里内嵌的紧凑摘要：只放"一眼判断死活"的字段。
pub fn health_summary() -> Value {
    let snapshot = snapshot();
    json!({
        "dllConnected": snapshot.pipe.dll_connected,
        "framesPerSec": snapshot.pipe.frames_per_sec,
        "packetsPerSec": snapshot.pipe.packets_per_sec,
        "lastFrameAgeMs": snapshot.pipe.last_frame_age_ms,
        "ackP99Us": snapshot.pipe.ack_p99_us,
        "spinHitRate": snapshot.pipe.spin_hit_rate,
        "bufferedBytes": snapshot.pipe.buffered_bytes,
        "lastMessageAgeMs": snapshot.chat.last_message_age_ms,
        "subscribers": snapshot.chat.subscribers,
        "laggedTotal": snapshot.chat.lagged_total,
        "build": snapshot.build.sha,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn histogram_percentiles_track_pushed_samples() {
        let mut histogram = Histogram::new();
        assert_eq!(histogram.percentile(0.5), 0);
        for value in 1..=100u32 {
            histogram.push(u64::from(value));
        }
        assert_eq!(histogram.percentile(0.50), 51);
        // 100 个样本时，p99 落在倒数第二个样本上；最大值的语义用 p=1.0。
        assert_eq!(histogram.percentile(0.99), 99);
        assert_eq!(histogram.percentile(1.0), 100);
    }

    #[test]
    fn histogram_keeps_only_latest_samples() {
        let mut histogram = Histogram::new();
        for value in 1..=10u32 {
            histogram.push(u64::from(value));
        }
        for value in 1..=LATENCY_SAMPLES as u32 {
            histogram.push(u64::from(value));
        }
        assert_eq!(histogram.percentile(0.0), 1);
        assert_eq!(histogram.percentile(1.0), LATENCY_SAMPLES as u32);
    }

    #[test]
    fn rate_meter_drops_stale_window_to_zero() {
        let mut meter = RateMeter::default();
        meter.tick(1_000, 10, 10);
        let (frames, packets) = meter.per_second(1_500);
        assert!(frames > 0.0 && packets > 0.0);
        let (frames, packets) = meter.per_second(5_000);
        assert_eq!((frames, packets), (0.0, 0.0));
    }

    #[test]
    fn event_log_keeps_only_latest_entries() {
        let mut log = EventLog::default();
        for index in 0..EVENT_RING + 5 {
            log.events.push_back(EventView {
                offset_ms: index as u64,
                kind: "test".into(),
                detail: String::new(),
            });
            if log.events.len() > EVENT_RING {
                log.events.pop_front();
            }
        }
        assert_eq!(log.events.len(), EVENT_RING);
        assert_eq!(log.events.front().map(|event| event.offset_ms), Some(5));
    }

    #[test]
    fn snapshot_serializes_with_expected_fields() {
        init();
        let value = serde_json::to_value(snapshot()).unwrap();
        for key in ["uptimeMs", "build", "pipe", "chat", "topCmds"] {
            assert!(value.get(key).is_some(), "缺少字段 {key}");
        }
        for key in [
            "framesPerSec",
            "lastFrameAgeMs",
            "ackP99Us",
            "spinHitRate",
            "bufferedBytes",
        ] {
            assert!(value["pipe"].get(key).is_some(), "缺少 pipe.{key}");
        }
        assert!(health_summary().get("dllConnected").is_some());
    }
}
