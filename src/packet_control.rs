//! Debug 全包模式：运行时模式开关、DLL 控制通道、全包统计与 raw 落盘。
//!
//! 默认 `chat`：DLL 只转发聊天包，行为与未启用本模块时完全一致。
//! 切到 `all` 后，DLL 通过**独立的 debug 数据管道**转发所有合法包；聊天管道
//! 的队列/ACK/线程都不受影响。本模块负责：
//! - 保存模式，并把模式写到控制管道（DLL 据此切换）；
//! - 统计 debug 流里的 cmd 分布（只记 cmd/长度，不解析 payload）；
//! - 可选把 debug 流按现有嗅探格式落盘，供离线筛选。
//!
//! 模式不持久化：重启后回到 `chat`，需要重新通过 API 打开。

use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::sync::mpsc::{SyncSender, TrySendError, sync_channel};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::JoinHandle;
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};

/// 宿主 → DLL 的控制管道（只写模式字节）。
pub const CONTROL_PIPE_NAME: &str = "MiliastraWonderlandChatBridgeCtl";
/// DLL → 宿主的 debug 数据管道（单向、无 ACK）。
pub const DEBUG_PIPE_NAME: &str = "MiliastraWonderlandChatBridgeDebug";

/// 默认抓包目录：程序同目录下的 capture。
///
/// 有显式目录就别用这个；这里是「切到 ALL 但没指定目录」时的落点，
/// 目的是让抓包变成纯运行时操作 —— 不用为了带目录而重启。
pub fn default_capture_dir() -> PathBuf {
    std::env::current_exe()
        .ok()
        .and_then(|exe| exe.parent().map(|parent| parent.join("capture")))
        .unwrap_or_else(|| PathBuf::from("capture"))
}

/// 默认抓包上限（总字节）。超过后只停止抓包，不影响聊天与转发。
pub const DEFAULT_CAPTURE_MAX_BYTES: u64 = 512 * 1024 * 1024;

/// raw 落盘队列长度（包数）。满了只丢抓包，不阻塞 debug 读线程。
const CAPTURE_QUEUE: usize = 4096;
/// index.csv 每多少条 flush 一次。
const INDEX_FLUSH_EVERY: u64 = 64;

/// 调试模式。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PacketMode {
    /// 默认：DLL 只转发聊天包。
    Chat,
    /// 调试：DLL 转发所有能抓到的包。
    All,
}

impl PacketMode {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "chat" => Some(Self::Chat),
            "all" => Some(Self::All),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Chat => "chat",
            Self::All => "all",
        }
    }

    /// 控制管道上的单字节编码：0 = chat，1 = all。
    pub fn as_byte(self) -> u8 {
        match self {
            Self::Chat => 0,
            Self::All => 1,
        }
    }
}

/// raw 抓包配置。只有显式给 `--capture-dir` 才创建。
#[derive(Clone, Debug)]
pub struct CaptureConfig {
    pub dir: PathBuf,
    /// 落盘总字节上限；超过后停止抓包（只丢抓包，不影响聊天/转发）。
    pub max_bytes: u64,
}

struct CmdStat {
    count: u64,
    bytes: u64,
}

/// 运行期跨线程共享的 debug 模式状态。
pub struct PacketControl {
    mode: AtomicU8,
    control: Mutex<Option<Box<dyn Write + Send>>>,
    control_connected: AtomicBool,
    stats: Mutex<BTreeMap<u16, CmdStat>>,
    packets: AtomicU64,
    bytes: AtomicU64,
    /// 运行期可开关：切 ALL 或调 /debug/capture 时在这里装拆。
    capture: Mutex<Option<CaptureSink>>,
    /// 切到 ALL 时是否自动开抓包。默认开；--no-auto-capture 或测试里关掉。
    auto_capture: AtomicBool,
    /// 当前抓包是不是「切到 ALL 时自动开的」。
    ///
    /// 用来对称收尾：自动开的，切回 chat 就自动停；手工开的（POST /debug/capture）
    /// 不动，否则用户以为自己开的抓包被悄悄关掉了。
    auto_started: AtomicBool,
}

impl PacketControl {
    pub fn new(initial: PacketMode, capture: Option<CaptureConfig>) -> Arc<Self> {
        let capture = capture.and_then(|config| match CaptureSink::start(config) {
            Ok(sink) => Some(sink),
            Err(error) => {
                tracing::warn!("raw 抓包不可用：{error:#}");
                None
            }
        });
        Arc::new(Self {
            mode: AtomicU8::new(initial.as_byte()),
            control: Mutex::new(None),
            control_connected: AtomicBool::new(false),
            stats: Mutex::new(BTreeMap::new()),
            packets: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            capture: Mutex::new(capture),
            auto_capture: AtomicBool::new(true),
            auto_started: AtomicBool::new(false),
        })
    }

    pub fn mode(&self) -> PacketMode {
        if self.mode.load(Ordering::Relaxed) == 1 {
            PacketMode::All
        } else {
            PacketMode::Chat
        }
    }

    pub fn control_connected(&self) -> bool {
        self.control_connected.load(Ordering::Relaxed)
    }

    /// 更新本地模式；DLL 控制管道已连接时立即写入。返回是否已作用于 DLL。
    pub fn set_mode(&self, mode: PacketMode) -> bool {
        self.mode.store(mode.as_byte(), Ordering::Relaxed);
        let mut guard = lock(&self.control);
        let Some(writer) = guard.as_mut() else {
            return false;
        };
        match write_mode(writer, mode) {
            Ok(()) => {
                tracing::info!("已切换 debug 模式：{}", mode.as_str());
                true
            }
            Err(error) => {
                tracing::warn!("写 DLL 控制管道失败，模式未生效：{error}");
                *guard = None;
                self.control_connected.store(false, Ordering::Relaxed);
                false
            }
        }
    }

    /// DLL 连上控制管道时调用：保存 writer，并立即下发当前模式。
    pub fn attach_control(&self, mut writer: Box<dyn Write + Send>) {
        let mode = self.mode();
        let mut guard = lock(&self.control);
        if let Err(error) = write_mode(&mut writer, mode) {
            tracing::warn!("初始化 DLL 控制管道失败：{error}");
            self.control_connected.store(false, Ordering::Relaxed);
            return;
        }
        *guard = Some(writer);
        self.control_connected.store(true, Ordering::Relaxed);
        tracing::info!("DLL 控制管道已连接，当前模式 {}", mode.as_str());
    }

    /// 当前是否在抓包。
    pub fn capture_active(&self) -> bool {
        lock(&self.capture).is_some()
    }

    /// 运行期开启抓包。dir 为 None 时落到 default_capture_dir()。
    ///
    /// 好处是不用为了带 --capture-dir 重启进程：切到 ALL 就自动抓，
    /// 也可以在这里改目录重新开始。
    pub fn start_capture(&self, dir: Option<PathBuf>, max_bytes: u64) -> Result<PathBuf, String> {
        self.start_capture_with(dir, max_bytes, false)
    }

    /// auto 表示这次是切到 ALL 时顺手开的，切回 chat 会自动停。
    fn start_capture_with(
        &self,
        dir: Option<PathBuf>,
        max_bytes: u64,
        auto: bool,
    ) -> Result<PathBuf, String> {
        let max_bytes = if max_bytes == 0 {
            DEFAULT_CAPTURE_MAX_BYTES
        } else {
            max_bytes
        };
        let dir = dir.unwrap_or_else(default_capture_dir);
        let mut guard = lock(&self.capture);
        if guard.is_some() {
            return Err("抓包已在进行中，先停再改".to_owned());
        }
        match CaptureSink::start(CaptureConfig {
            dir: dir.clone(),
            max_bytes,
        }) {
            Ok(sink) => {
                *guard = Some(sink);
                self.auto_started.store(auto, Ordering::Relaxed);
                Ok(dir)
            }
            Err(error) => Err(format!("{error:#}")),
        }
    }

    /// 停止抓包并等待落盘线程退出。返回之前是否在抓。
    pub fn stop_capture(&self) -> bool {
        let mut guard = lock(&self.capture);
        let stopped = guard.take().is_some();
        self.auto_started.store(false, Ordering::Relaxed);
        stopped
    }

    /// 当前抓包目录（没在抓则 None）。
    ///
    /// 切模式时若抓包已在进行，响应里也要给出目录，
    /// 否则 captureDir=null 会让调用方误以为没在抓。
    pub fn capture_dir(&self) -> Option<PathBuf> {
        lock(&self.capture)
            .as_ref()
            .map(|sink| sink.shared.dir.clone())
    }

    /// 切回 chat 时调用：只停自动开的抓包，手工开的保留。
    pub fn stop_auto_capture(&self) -> bool {
        if !self.auto_started.load(Ordering::Relaxed) {
            return false;
        }
        self.stop_capture()
    }

    /// 切到 ALL 时确保抓包已开（用默认目录）。已开则不动。
    /// 控制「切到 ALL 时是否自动开抓包」。
    pub fn set_auto_capture(&self, enabled: bool) {
        self.auto_capture.store(enabled, Ordering::Relaxed);
    }

    /// 切到 ALL 时由 HTTP 层调用：没在抓就按默认目录开一个。
    ///
    /// 放在 HTTP 层而不是 set_mode 里，是为了让 PacketControl 在单测里保持
    /// 无副作用（否则一测切模式就会在临时目录里建 capture）。
    pub fn ensure_capture_for_all(&self) -> Option<PathBuf> {
        if !self.auto_capture.load(Ordering::Relaxed) {
            return None;
        }
        if lock(&self.capture).is_some() {
            return None;
        }
        match self.start_capture_with(None, DEFAULT_CAPTURE_MAX_BYTES, true) {
            Ok(dir) => {
                tracing::info!("已自动开启 raw 抓包：{}", dir.display());
                Some(dir)
            }
            Err(error) => {
                tracing::warn!("自动开启 raw 抓包失败（只影响抓包）：{error}");
                None
            }
        }
    }

    pub fn detach_control(&self) {
        *lock(&self.control) = None;
        self.control_connected.store(false, Ordering::Relaxed);
    }

    /// debug 读线程每收到一个包调用一次：cmd 统计 + 可选 raw 落盘。
    ///
    /// 这里不做 protobuf 解析，也不进入聊天状态；聊天业务只走原有聊天管道。
    pub fn observe_debug_packet(&self, cmd: u16, payload: &[u8]) {
        self.packets.fetch_add(1, Ordering::Relaxed);
        self.bytes
            .fetch_add(payload.len() as u64, Ordering::Relaxed);
        {
            let mut stats = lock(&self.stats);
            let entry = stats.entry(cmd).or_insert(CmdStat { count: 0, bytes: 0 });
            entry.count += 1;
            entry.bytes += payload.len() as u64;
        }
        if let Some(capture) = lock(&self.capture).as_ref() {
            capture.submit(cmd, payload);
        }
    }

    /// 给 GET /debug/packets 的快照。
    pub fn snapshot_json(&self) -> Value {
        let cmds = {
            let stats = lock(&self.stats);
            let mut map = serde_json::Map::new();
            for (cmd, stat) in stats.iter() {
                map.insert(
                    cmd.to_string(),
                    json!({ "count": stat.count, "bytes": stat.bytes }),
                );
            }
            Value::Object(map)
        };
        json!({
            "ok": true,
            "mode": self.mode().as_str(),
            "dllControl": self.control_connected(),
            "counts": {
                "packets": self.packets.load(Ordering::Relaxed),
                "bytes": self.bytes.load(Ordering::Relaxed),
                "cmds": cmds,
            },
            "capture": lock(&self.capture).as_ref().map(CaptureSink::snapshot_json),
        })
    }
}

fn write_mode(writer: &mut Box<dyn Write + Send>, mode: PacketMode) -> std::io::Result<()> {
    writer.write_all(&[mode.as_byte()])?;
    writer.flush()
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

// ---------------------------------------------------------------------------
// raw 抓包
// ---------------------------------------------------------------------------

struct CapturedPacket {
    seq: u64,
    cmd: u16,
    payload: Vec<u8>,
}

struct CaptureShared {
    dir: PathBuf,
    max_bytes: u64,
    seq: AtomicU64,
    accepted_bytes: AtomicU64,
    files: AtomicU64,
    written_bytes: AtomicU64,
    dropped: AtomicU64,
    stopped: AtomicBool,
}

struct CaptureSink {
    shared: Arc<CaptureShared>,
    tx: Option<SyncSender<CapturedPacket>>,
    thread: Option<JoinHandle<()>>,
}

impl CaptureSink {
    fn start(config: CaptureConfig) -> std::io::Result<Self> {
        std::fs::create_dir_all(&config.dir)?;
        // 每次启动重建索引，避免和上一次运行的 seq 混在一起。
        // 但旧索引先改名保留：raw 文件还在目录里，直接 truncate 会让它们失去索引，
        // 变成一堆无法对应 cmd 的孤儿 bin。
        let index_path = config.dir.join("index.csv");
        if index_path.metadata().map(|m| m.len() > 0).unwrap_or(false) {
            let stamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|elapsed| elapsed.as_secs())
                .unwrap_or(0);
            let archived = config.dir.join(format!("index-{stamp}.csv"));
            match std::fs::rename(&index_path, &archived) {
                Ok(()) => tracing::info!("旧抓包索引已归档：{}", archived.display()),
                Err(error) => tracing::warn!("归档旧抓包索引失败（将覆盖）：{error}"),
            }
        }
        let index = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&index_path)?;
        let shared = Arc::new(CaptureShared {
            dir: config.dir,
            max_bytes: config.max_bytes,
            seq: AtomicU64::new(0),
            accepted_bytes: AtomicU64::new(0),
            files: AtomicU64::new(0),
            written_bytes: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
            stopped: AtomicBool::new(false),
        });
        let (tx, rx) = sync_channel::<CapturedPacket>(CAPTURE_QUEUE);
        let writer_shared = Arc::clone(&shared);
        let thread = std::thread::Builder::new()
            .name("mwc-capture".to_owned())
            .spawn(move || {
                let mut index = BufWriter::new(index);
                let mut since_flush = 0u64;
                while let Ok(packet) = rx.recv() {
                    let name = format!(
                        "p_{:05}_{:05}_{}.bin",
                        packet.seq,
                        packet.cmd,
                        packet.payload.len()
                    );
                    let path = writer_shared.dir.join(&name);
                    match std::fs::write(&path, &packet.payload) {
                        Ok(()) => {
                            let millis = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .map(|elapsed| elapsed.as_millis())
                                .unwrap_or(0);
                            if let Err(error) = writeln!(
                                index,
                                "{},{},{},{}",
                                packet.seq,
                                packet.cmd,
                                packet.payload.len(),
                                millis
                            ) {
                                tracing::warn!("写 index.csv 失败：{error}");
                            }
                            since_flush += 1;
                            if since_flush >= INDEX_FLUSH_EVERY {
                                let _ = index.flush();
                                since_flush = 0;
                            }
                            writer_shared.files.fetch_add(1, Ordering::Relaxed);
                            writer_shared
                                .written_bytes
                                .fetch_add(packet.payload.len() as u64, Ordering::Relaxed);
                        }
                        Err(error) => {
                            writer_shared.dropped.fetch_add(1, Ordering::Relaxed);
                            tracing::warn!("写 raw 抓包失败 {}：{error}", path.display());
                        }
                    }
                }
                let _ = index.flush();
                tracing::info!("raw 抓包线程退出");
            })?;
        tracing::info!("raw 抓包已启用：{}", shared.dir.display());
        Ok(Self {
            shared,
            tx: Some(tx),
            thread: Some(thread),
        })
    }

    fn submit(&self, cmd: u16, payload: &[u8]) {
        if self.shared.stopped.load(Ordering::Relaxed) {
            self.shared.dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let len = payload.len() as u64;
        let accepted = self.shared.accepted_bytes.fetch_add(len, Ordering::Relaxed) + len;
        if accepted > self.shared.max_bytes {
            if !self.shared.stopped.swap(true, Ordering::Relaxed) {
                tracing::warn!(
                    "raw 抓包达到 {} 字节上限，已停止落盘（后续包只丢弃抓包，不影响转发）",
                    self.shared.max_bytes
                );
            }
            self.shared.dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let seq = self.shared.seq.fetch_add(1, Ordering::Relaxed) + 1;
        let packet = CapturedPacket {
            seq,
            cmd,
            payload: payload.to_vec(),
        };
        let Some(tx) = self.tx.as_ref() else {
            return;
        };
        match tx.try_send(packet) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) => {
                let dropped = self
                    .shared
                    .dropped
                    .fetch_add(1, Ordering::Relaxed)
                    .saturating_add(1);
                if dropped.is_multiple_of(1024) {
                    tracing::warn!("raw 抓包队列已满，正在丢抓包（已丢 {dropped} 包）");
                }
            }
        }
    }

    fn snapshot_json(&self) -> Value {
        json!({
            "enabled": true,
            "dir": self.shared.dir.display().to_string(),
            "maxBytes": self.shared.max_bytes,
            "files": self.shared.files.load(Ordering::Relaxed),
            "bytes": self.shared.written_bytes.load(Ordering::Relaxed),
            "dropped": self.shared.dropped.load(Ordering::Relaxed),
            "stopped": self.shared.stopped.load(Ordering::Relaxed),
        })
    }
}

impl Drop for CaptureSink {
    fn drop(&mut self) {
        // 关掉发送端，writer 线程把队列里剩下的包写完再退出，保证测试/退出前落盘完整。
        self.tx.take();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[derive(Clone, Default)]
    struct SharedBuf(Arc<Mutex<Vec<u8>>>);

    impl Write for SharedBuf {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            let mut guard = self.0.lock().unwrap();
            guard.extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    fn temp_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "mwc-packet-control-{tag}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn mode_parse_and_bytes() {
        assert_eq!(PacketMode::parse("chat"), Some(PacketMode::Chat));
        assert_eq!(PacketMode::parse("all"), Some(PacketMode::All));
        assert_eq!(PacketMode::parse("other"), None);
        assert_eq!(PacketMode::Chat.as_byte(), 0);
        assert_eq!(PacketMode::All.as_byte(), 1);
        assert_eq!(PacketMode::Chat.as_str(), "chat");
        assert_eq!(PacketMode::All.as_str(), "all");
    }

    #[test]
    fn set_mode_reports_false_without_dll_control() {
        let control = PacketControl::new(PacketMode::Chat, None);
        assert_eq!(control.mode(), PacketMode::Chat);
        assert!(!control.set_mode(PacketMode::All));
        assert_eq!(control.mode(), PacketMode::All);
        assert!(!control.control_connected());
        let snapshot = control.snapshot_json();
        assert_eq!(snapshot["mode"], "all");
        assert_eq!(snapshot["dllControl"], false);
        assert!(snapshot["capture"].is_null());
    }

    #[test]
    fn attach_control_writes_initial_and_followup_modes() {
        let control = PacketControl::new(PacketMode::Chat, None);
        let buffer = SharedBuf::default();
        let writer = buffer.clone();
        control.attach_control(Box::new(writer));
        assert!(control.control_connected());
        assert!(control.set_mode(PacketMode::All));
        assert_eq!(*buffer.0.lock().unwrap(), vec![0u8, 1u8]);
        control.detach_control();
        assert!(!control.control_connected());
        assert!(!control.set_mode(PacketMode::Chat));
    }

    #[test]
    fn capture_writes_files_and_index() {
        let dir = temp_dir("capture");
        let capture = CaptureConfig {
            dir: dir.clone(),
            max_bytes: 1024 * 1024,
        };
        let control = PacketControl::new(PacketMode::All, Some(capture));
        control.observe_debug_packet(3347, &[10, 20, 30]);
        control.observe_debug_packet(2246, &[1, 2]);
        // 丢掉最后一个 Arc 触发 CaptureSink::drop，等待 writer 写完。
        drop(control);

        let file = dir.join("p_00001_03347_3.bin");
        assert_eq!(std::fs::read(&file).unwrap(), vec![10, 20, 30]);
        let index = std::fs::read_to_string(dir.join("index.csv")).unwrap();
        let first = index.lines().next().unwrap();
        let fields: Vec<&str> = first.split(',').collect();
        assert_eq!(fields[0], "1");
        assert_eq!(fields[1], "3347");
        assert_eq!(fields[2], "3");
        assert!(fields[3].parse::<u128>().unwrap() > 0);
        assert!(index.contains(",2246,2,"));
        assert!(std::fs::read(dir.join("p_00002_02246_2.bin")).is_ok());
    }

    #[test]
    fn capture_stops_at_size_limit() {
        let dir = temp_dir("limit");
        let capture = CaptureConfig {
            dir: dir.clone(),
            max_bytes: 4,
        };
        let control = PacketControl::new(PacketMode::All, Some(capture));
        control.observe_debug_packet(2246, &[1, 2, 3]);
        control.observe_debug_packet(2246, &[4, 5]); // 超上限
        let snapshot = control.snapshot_json();
        assert_eq!(snapshot["capture"]["stopped"], true);
        assert_eq!(snapshot["capture"]["dropped"], 1);
        drop(control);
        assert!(std::fs::read(dir.join("p_00001_02246_3.bin")).is_ok());
        assert!(!dir.join("p_00002_02246_2.bin").exists());
    }

    #[test]
    fn counts_cover_all_cmds() {
        let control = PacketControl::new(PacketMode::All, None);
        control.observe_debug_packet(3347, &[0; 10]);
        control.observe_debug_packet(3347, &[0; 5]);
        control.observe_debug_packet(4410, &[0; 7]);
        let snapshot = control.snapshot_json();
        assert_eq!(snapshot["counts"]["packets"], 3);
        assert_eq!(snapshot["counts"]["bytes"], 22);
        assert_eq!(snapshot["counts"]["cmds"]["3347"]["count"], 2);
        assert_eq!(snapshot["counts"]["cmds"]["3347"]["bytes"], 15);
        assert_eq!(snapshot["counts"]["cmds"]["4410"]["count"], 1);
    }
    #[test]
    fn auto_capture_stops_when_leaving_all_mode() {
        let dir = temp_dir("autostop");
        let control = PacketControl::new(PacketMode::Chat, None);
        assert!(!control.capture_active());
        // 切 ALL：自动开抓包，并记成「自动开的」。
        let started = control.ensure_capture_for_all();
        assert!(started.is_some());
        assert!(control.capture_active());
        assert_eq!(control.capture_dir(), started);
        // 切回 chat：自动收掉。
        assert!(control.stop_auto_capture());
        assert!(!control.capture_active());
        assert_eq!(control.capture_dir(), None);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn manual_capture_survives_leaving_all_mode() {
        let dir = temp_dir("manualkeep");
        let control = PacketControl::new(PacketMode::All, None);
        // 手工开（POST /debug/capture）：不能被 stop_auto_capture 关掉。
        let started = control.start_capture(Some(dir.clone()), 1024).unwrap();
        assert_eq!(started, dir);
        assert!(!control.stop_auto_capture(), "手工抓包不该被自动收尾关掉");
        assert!(control.capture_active());
        assert_eq!(control.capture_dir(), Some(dir.clone()));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn restarting_capture_archives_previous_index() {
        let dir = temp_dir("archive");
        {
            let control = PacketControl::new(PacketMode::All, None);
            control
                .start_capture(Some(dir.clone()), 1024 * 1024)
                .unwrap();
            control.observe_debug_packet(3347, &[1, 2, 3]);
            // 停抓包让 writer 落盘。
            assert!(control.stop_capture());
        }
        let first_index = std::fs::read_to_string(dir.join("index.csv")).unwrap();
        assert!(first_index.contains(",3347,3,"));

        // 再开一次：旧索引应被改名保留，而不是被清掉。
        {
            let control = PacketControl::new(PacketMode::All, None);
            control
                .start_capture(Some(dir.clone()), 1024 * 1024)
                .unwrap();
            assert!(control.stop_capture());
        }
        let archived: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .filter(|name| name.starts_with("index-") && name.ends_with(".csv"))
            .collect();
        assert_eq!(archived.len(), 1, "旧索引应归档一份，实际 {archived:?}");
        let archived_body = std::fs::read_to_string(dir.join(&archived[0])).unwrap();
        assert!(archived_body.contains(",3347,3,"), "归档内容应保留旧记录");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
