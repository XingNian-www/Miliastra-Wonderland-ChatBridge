//! DLL 健康心跳协议：独立管道、单向、固定 28 字节帧。
//!
//! 为什么需要它：宿主过去只能用「有没有聊天数据」判断 DLL 状态，但「空闲」与
//! 「故障」不可区分——`PacketOutbox.Disable()`（DLL 活着但已停止转发）、
//! 游戏主线程被占用、正常单人挂机，从宿主视角都是「没有数据」。心跳把
//! 「DLL 活着且愿意上报」变成一个独立、可观测的信号。
//!
//! 传输约定（本文件只定义协议，不实现管道 IO）：
//!
//! - 独立管道 `MiliastraWonderlandChatBridgeHealth`：DLL 是 client，宿主是 server；
//! - **单向、无 ACK**：DLL 每 1 秒写一帧，宿主只读。写失败只影响上报，DLL
//!   必须忽略错误，绝不能因此阻塞游戏线程，也绝不能回退到主管道；
//! - **独立于主管道**：主管道是严格 FIFO + 每包 ACK（上一轮刚修过「超时后
//!   迟到的 ACK 被下一包读走」），心跳混进去会打松这个假设；debug/control
//!   管道各自负责自己的数据，不承担健康上报；
//! - **不受 `PacketOutbox.Disable()` 影响**：聊天转发停用后心跳必须继续发。
//!   否则最常见的一种「静默」反而上报不出来。
//!
//! 帧格式（固定 28 字节，全部小端，没有长度字段也没有分隔符）：
//!
//! | 偏移 | 长度 | 字段 | 说明 |
//! |---|---|---|---|
//! | 0 | 2 | `magic` | u16 = `0x4D57`。小端线上字节是 `57 4D`（'W' 'M'），不是 `4D 57` |
//! | 2 | 1 | `version` | u8 = `1`；协议升级时递增，宿主按版本回落 |
//! | 3 | 1 | `kind` | u8 = `0x01`（Health） |
//! | 4 | 4 | `seq` | u32 每帧递增，回绕到 0；宿主按 wrapping 语义累计丢帧到 `/health` 的 `dll.lostFrames` |
//! | 8 | 4 | `flags` | u32 状态位，见下表 |
//! | 12 | 8 | `timeout_count` | u64 累计发送超时次数 |
//! | 20 | 8 | `abandoned_tasks` | u64 当前被放弃的 IO 任务数 |
//!
//! flags 位定义：
//!
//! | 位 | 常量 | 含义 |
//! |---|---|---|
//! | 0 | `FLAG_PACKET_OUTBOX_DISABLED` | 聊天发送队列已停用：DLL 活着但不再转发聊天 |
//! | 1 | `FLAG_RECONNECTING` | DLL 正在重连宿主（连接代际切换中） |
//! | 2 | `FLAG_DEBUG_OUTBOX_DISABLED` | debug 全包队列已停用 |
//! | 3 | `FLAG_ABANDONED_LIMIT_REACHED` | 放弃的 IO 任务数达到上限 |
//!
//! 未定义的 flags 位保留给未来版本：**解析器不因未知位报错**（向前兼容），
//! 原值保留在 `HealthFrame::flags`，现有 getter 只看已定义位。新增状态位
//! 不需要升 `version`，升版本只用于帧布局变化。
//!
//! 解析语义（见 `parse_health`）：
//!
//! - 返回 `Ok(None)`：字节还不够凑一帧（半帧）→ 调用方保留缓冲继续读；
//! - 返回 `Err`：已收到的前缀不可能构成合法帧（magic/version/kind 不符）→
//!   致命协议错误：应关闭并重建健康管道（或停用心跳），**不要**继续吞字节，
//!   避免把旧 DLL / 串道数据当成半帧无限缓存；
//! - 前缀校验「收到多少校验多少」：例如只收到 1 字节且已经不对，立即报
//!   `BadMagic`，不必等满 28 字节；
//! - 第 28 字节之后不属于本帧：解析只消费固定 `FRAME_SIZE` 字节，调用方
//!   自行 drain；缓冲区里可以连续放多帧（见测试）；
//! - `seq` 是 u32 会回绕：判断丢帧用 `wrapping_sub`/`wrapping_add`，不能
//!   直接比较大小。宿主在 `dll_health` 里据此累计 `lostFrames`，并过滤掉
//!   超过 `MAX_PLAUSIBLE_GAP` 的跳变（那是对端重连，不是真丢帧）。

/// 与 DLL 约定的健康管道名（DLL 是 client，宿主是 server）。
pub const PIPE_NAME: &str = "MiliastraWonderlandChatBridgeHealth";

/// DLL 发送间隔（秒）。宿主判断「心跳停了」时至少留 3 个周期的余量。
pub const HEARTBEAT_INTERVAL_SECS: u64 = 1;

/// 魔数 u16 = 0x4D57（'MW' 的小端值）。
pub const MAGIC: u16 = 0x4D57;

/// 线上字节序的 magic：`0x57, 0x4D`（'W' 'M'）。
///
/// 单独暴露是为了让 DLL 实现直接对照两个字节，避免把 `0x4D57` 按大端写反。
pub const MAGIC_BYTES: [u8; 2] = [0x57, 0x4D];

/// 协议版本。帧布局变化时递增；新增 flags 位不需要升版本。
pub const VERSION: u8 = 1;

/// 帧类型：Health。
pub const KIND: u8 = 0x01;

/// 固定帧长度（字节）。
pub const FRAME_SIZE: usize = 324;

/// flags bit0：聊天发送队列（PacketOutbox）已停用。
pub const FLAG_PACKET_OUTBOX_DISABLED: u32 = 1 << 0;
/// flags bit1：DLL 正在重连宿主。
pub const FLAG_RECONNECTING: u32 = 1 << 1;
/// flags bit2：debug 全包队列（DebugOutbox）已停用。
pub const FLAG_DEBUG_OUTBOX_DISABLED: u32 = 1 << 2;
/// flags bit3：被放弃的 IO 任务数已达上限。
pub const FLAG_ABANDONED_LIMIT_REACHED: u32 = 1 << 3;

/// 当前已定义的 flags 位掩码。未知位保留，解析时不报错。
pub const KNOWN_FLAGS: u32 = FLAG_PACKET_OUTBOX_DISABLED
    | FLAG_RECONNECTING
    | FLAG_DEBUG_OUTBOX_DISABLED
    | FLAG_ABANDONED_LIMIT_REACHED;

/// 一帧健康上报（不含 magic/version/kind，这三个由编解码负责）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HealthFrame {
    /// 每帧递增；u32 回绕（见模块文档）。
    pub seq: u32,
    /// flags 位集合（含未来未知位）。
    pub flags: u32,
    /// 累计发送超时次数。
    pub timeout_count: u64,
    /// 当前被放弃的 IO 任务数。
    pub abandoned_tasks: u64,
    /// packetLength 异常被丢弃的包数（协议 v2 新增，诊断用）。
    ///
    /// 排查「钩子挂对了但收不到包」时最关键的一个数：非 0 说明读到的长度
    /// 字段不可信，包在边界检查之前就被丢弃了。
    pub invalid_length: u64,
    /// 转发过程中的异常次数（IO/解压/格式），诊断用。
    pub forward_errors: u64,
    /// 钩子进入网络包路径的次数（startIndex==6 且长度合法）。
    pub hook_entered: u64,
    /// 包起始魔数不匹配的次数（7.2 若改包格式，这个会飙升）。
    pub bad_magic: u64,
    /// 通过魔数校验但 TryGetData 失败的次数（多为解压失败）。
    pub try_get_data_fail: u64,
    /// 成功提取包体并转发的次数。
    pub accepted: u64,
    /// 当前订阅的 cmdId 数量（0 说明任务列表没加载）。
    pub required_count: u64,
    /// 因 cmdId 未订阅被过滤的包数。
    pub filtered: u64,
    /// 最近被过滤的 cmdId。
    pub last_filtered_cmd: u64,
    /// 订阅列表前 8 项快照（协议 v5，确认 LoadTasks 实际内容）。
    pub required_sample: [u64; 8],
    /// 最近见到的 cmdId 环形缓冲（协议 v6）。
    pub cmd_seen: [u64; 8],
    /// 已见到的包总数（协议 v6）。
    pub cmd_seen_total: u64,
    /// 不同包的原始 8 字节头（协议 v7）。
    pub raw_header: [u64; 6],
    /// 第一个包 val+0..+0x28 的 40 字节（协议 v8）。
    pub probe_len: [u64; 5],
}

impl HealthFrame {
    pub const fn new(seq: u32, flags: u32, timeout_count: u64, abandoned_tasks: u64) -> Self {
        Self {
            seq,
            flags,
            timeout_count,
            abandoned_tasks,
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
        }
    }

    /// 供 DLL 侧填充诊断计数器。
    pub const fn with_diagnostics(mut self, invalid_length: u64, forward_errors: u64) -> Self {
        self.invalid_length = invalid_length;
        self.forward_errors = forward_errors;
        self
    }

    /// 供 DLL 侧填充订阅列表快照（协议 v5）。
    pub const fn with_required_sample(mut self, sample: [u64; 8]) -> Self {
        self.required_sample = sample;
        self
    }

    /// 供 DLL 侧填充观测到的 cmdId（协议 v6）。
    pub const fn with_cmd_seen(mut self, seen: [u64; 8], total: u64) -> Self {
        self.cmd_seen = seen;
        self.cmd_seen_total = total;
        self
    }

    /// 供 DLL 侧填充原始包头（协议 v7）。
    pub const fn with_raw_header(mut self, raw: [u64; 6]) -> Self {
        self.raw_header = raw;
        self
    }

    /// 供 DLL 侧填充首包探针（协议 v8）。
    pub const fn with_probe_len(mut self, probe: [u64; 5]) -> Self {
        self.probe_len = probe;
        self
    }

    /// 供 DLL 侧填充过滤阶段的计数（协议 v4）。
    pub const fn with_filter(
        mut self,
        required_count: u64,
        filtered: u64,
        last_filtered_cmd: u64,
    ) -> Self {
        self.required_count = required_count;
        self.filtered = filtered;
        self.last_filtered_cmd = last_filtered_cmd;
        self
    }

    /// 供 DLL 侧填充转发流水线各阶段的计数（协议 v3）。
    pub const fn with_pipeline(
        mut self,
        hook_entered: u64,
        bad_magic: u64,
        try_get_data_fail: u64,
        accepted: u64,
    ) -> Self {
        self.hook_entered = hook_entered;
        self.bad_magic = bad_magic;
        self.try_get_data_fail = try_get_data_fail;
        self.accepted = accepted;
        self
    }

    /// 聊天发送队列是否已停用（「静默」的核心信号）。
    pub fn packet_outbox_disabled(&self) -> bool {
        self.flags & FLAG_PACKET_OUTBOX_DISABLED != 0
    }

    /// DLL 是否正在重连宿主。
    pub fn reconnecting(&self) -> bool {
        self.flags & FLAG_RECONNECTING != 0
    }

    /// debug 全包队列是否已停用。
    pub fn debug_outbox_disabled(&self) -> bool {
        self.flags & FLAG_DEBUG_OUTBOX_DISABLED != 0
    }

    /// 放弃的 IO 任务数是否已达上限。
    pub fn abandoned_limit_reached(&self) -> bool {
        self.flags & FLAG_ABANDONED_LIMIT_REACHED != 0
    }
}

/// 解析失败的致命原因。返回 `Err` 表示输入前缀已确定不是本协议的帧，
/// 调用方应重建管道而不是继续缓存。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
// BadMagic/BadVersion/BadKind 是最直白的叫法；不为了躲 lint 改成含糊的名字。
#[allow(clippy::enum_variant_names)]
pub enum HealthError {
    /// magic 不符。携带已收到的前缀（不足 2 字节时高位补 0）。
    BadMagic(u16),
    /// version 不符（当前只支持 `VERSION = 1`）。
    BadVersion(u8),
    /// kind 不符（当前只支持 `KIND = 0x01`）。
    BadKind(u8),
}

impl std::fmt::Display for HealthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BadMagic(actual) => write!(
                f,
                "健康心跳 magic 不符：期望 0x{MAGIC:04X}，实际 0x{actual:04X}（前缀可能来自旧 DLL/其他数据）"
            ),
            Self::BadVersion(actual) => write!(
                f,
                "健康心跳 version 不符：期望 {VERSION}，实际 {actual}（协议版本不兼容）"
            ),
            Self::BadKind(actual) => write!(
                f,
                "健康心跳 kind 不符：期望 0x{KIND:02X}，实际 0x{actual:02X}"
            ),
        }
    }
}

impl std::error::Error for HealthError {}

/// 把四个已知状态位打包成 flags 字段（DLL 侧发送时可直接用）。
pub fn pack_flags(
    packet_outbox_disabled: bool,
    reconnecting: bool,
    debug_outbox_disabled: bool,
    abandoned_limit_reached: bool,
) -> u32 {
    let mut flags = 0;
    if packet_outbox_disabled {
        flags |= FLAG_PACKET_OUTBOX_DISABLED;
    }
    if reconnecting {
        flags |= FLAG_RECONNECTING;
    }
    if debug_outbox_disabled {
        flags |= FLAG_DEBUG_OUTBOX_DISABLED;
    }
    if abandoned_limit_reached {
        flags |= FLAG_ABANDONED_LIMIT_REACHED;
    }
    flags
}

/// 编码一帧固定 28 字节（小端）。
///
/// 无长度字段：写端必须一次写满 `FRAME_SIZE` 字节，或按字节流顺序写；
/// 读端按 `FRAME_SIZE` 切片解析。
pub fn encode_health(frame: &HealthFrame) -> [u8; FRAME_SIZE] {
    let mut out = [0u8; FRAME_SIZE];
    out[0..2].copy_from_slice(&MAGIC_BYTES);
    out[2] = VERSION;
    out[3] = KIND;
    out[4..8].copy_from_slice(&frame.seq.to_le_bytes());
    out[8..12].copy_from_slice(&frame.flags.to_le_bytes());
    out[12..20].copy_from_slice(&frame.timeout_count.to_le_bytes());
    out[20..28].copy_from_slice(&frame.abandoned_tasks.to_le_bytes());
    out[28..36].copy_from_slice(&frame.invalid_length.to_le_bytes());
    out[36..44].copy_from_slice(&frame.forward_errors.to_le_bytes());
    out[44..52].copy_from_slice(&frame.hook_entered.to_le_bytes());
    out[52..60].copy_from_slice(&frame.bad_magic.to_le_bytes());
    out[60..68].copy_from_slice(&frame.try_get_data_fail.to_le_bytes());
    out[68..76].copy_from_slice(&frame.accepted.to_le_bytes());
    out[76..84].copy_from_slice(&frame.required_count.to_le_bytes());
    out[84..92].copy_from_slice(&frame.filtered.to_le_bytes());
    out[92..100].copy_from_slice(&frame.last_filtered_cmd.to_le_bytes());
    for (i, value) in frame.required_sample.iter().enumerate() {
        let at = 100 + i * 8;
        out[at..at + 8].copy_from_slice(&value.to_le_bytes());
    }
    for (i, value) in frame.cmd_seen.iter().enumerate() {
        let at = 164 + i * 8;
        out[at..at + 8].copy_from_slice(&value.to_le_bytes());
    }
    out[228..236].copy_from_slice(&frame.cmd_seen_total.to_le_bytes());
    for (i, value) in frame.raw_header.iter().enumerate() {
        let at = 236 + i * 8;
        out[at..at + 8].copy_from_slice(&value.to_le_bytes());
    }
    for (i, value) in frame.probe_len.iter().enumerate() {
        let at = 284 + i * 8;
        out[at..at + 8].copy_from_slice(&value.to_le_bytes());
    }
    out
}

/// 解析缓冲区开头的一帧。
///
/// - `Ok(Some(frame))`：开头是完整合法帧；只消费前 `FRAME_SIZE` 字节，
///   剩余字节由调用方处理（可能还有下一帧或半帧）；
/// - `Ok(None)`：开头是合法前缀但不足 `FRAME_SIZE` 字节，需要更多字节；
/// - `Err(HealthError)`：前缀已确定不合法（magic/version/kind），致命错误。
///
/// magic/version/kind 的校验是增量的：只要收到的字节已经能判定错误就立即
/// 返回 `Err`，不会为了凑满 28 字节把垃圾数据长期留在缓冲区。
pub fn parse_health(bytes: &[u8]) -> Result<Option<HealthFrame>, HealthError> {
    // 1 字节就足以否定 magic 的低字节。
    if let Some(&actual) = bytes.first()
        && actual != MAGIC_BYTES[0]
    {
        return Err(HealthError::BadMagic(u16::from(actual)));
    }
    // 2 字节可以完整校验 magic。
    if bytes.len() >= 2 {
        let actual = u16::from_le_bytes([bytes[0], bytes[1]]);
        if actual != MAGIC {
            return Err(HealthError::BadMagic(actual));
        }
    }
    // 3 字节可以校验 version。
    if let Some(&actual) = bytes.get(2)
        && actual != VERSION
    {
        return Err(HealthError::BadVersion(actual));
    }
    // 4 字节可以校验 kind。
    if let Some(&actual) = bytes.get(3)
        && actual != KIND
    {
        return Err(HealthError::BadKind(actual));
    }
    if bytes.len() < FRAME_SIZE {
        return Ok(None);
    }
    Ok(Some(
        HealthFrame::new(
            u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]),
            u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]),
            u64::from_le_bytes(bytes[12..20].try_into().expect("FRAME_SIZE 已保证长度")),
            u64::from_le_bytes(bytes[20..28].try_into().expect("FRAME_SIZE 已保证长度")),
        )
        .with_diagnostics(
            u64::from_le_bytes(bytes[28..36].try_into().expect("FRAME_SIZE 已保证长度")),
            u64::from_le_bytes(bytes[36..44].try_into().expect("FRAME_SIZE 已保证长度")),
        )
        .with_pipeline(
            u64::from_le_bytes(bytes[44..52].try_into().expect("FRAME_SIZE 已保证长度")),
            u64::from_le_bytes(bytes[52..60].try_into().expect("FRAME_SIZE 已保证长度")),
            u64::from_le_bytes(bytes[60..68].try_into().expect("FRAME_SIZE 已保证长度")),
            u64::from_le_bytes(bytes[68..76].try_into().expect("FRAME_SIZE 已保证长度")),
        )
        .with_filter(
            u64::from_le_bytes(bytes[76..84].try_into().expect("FRAME_SIZE 已保证长度")),
            u64::from_le_bytes(bytes[84..92].try_into().expect("FRAME_SIZE 已保证长度")),
            u64::from_le_bytes(bytes[92..100].try_into().expect("FRAME_SIZE 已保证长度")),
        )
        .with_required_sample(std::array::from_fn(|i| {
            let at = 100 + i * 8;
            u64::from_le_bytes(bytes[at..at + 8].try_into().expect("FRAME_SIZE 已保证长度"))
        }))
        .with_cmd_seen(
            std::array::from_fn(|i| {
                let at = 164 + i * 8;
                u64::from_le_bytes(bytes[at..at + 8].try_into().expect("FRAME_SIZE 已保证长度"))
            }),
            u64::from_le_bytes(bytes[228..236].try_into().expect("FRAME_SIZE 已保证长度")),
        )
        .with_raw_header(std::array::from_fn(|i| {
            let at = 236 + i * 8;
            u64::from_le_bytes(bytes[at..at + 8].try_into().expect("FRAME_SIZE 已保证长度"))
        }))
        .with_probe_len(std::array::from_fn(|i| {
            let at = 284 + i * 8;
            u64::from_le_bytes(bytes[at..at + 8].try_into().expect("FRAME_SIZE 已保证长度"))
        })),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> HealthFrame {
        HealthFrame::new(7, FLAG_RECONNECTING | FLAG_DEBUG_OUTBOX_DISABLED, 42, 3)
    }

    #[test]
    fn frame_size_matches_field_widths() {
        // 2(magic)+1(version)+1(kind)+4(seq)+4(flags)+8(timeout)+8(abandoned)
        // +8(invalid_length)+8(forward_errors) = 44。
        assert_eq!(FRAME_SIZE, 2 + 1 + 1 + 4 + 4 + 8 * 39);
        assert_eq!(FRAME_SIZE, 324);
    }

    #[test]
    fn wire_magic_bytes_match_declared_u16() {
        assert_eq!(MAGIC, 0x4D57);
        assert_eq!(MAGIC.to_le_bytes(), MAGIC_BYTES);
        // 线上字节是 'W' 'M'（0x57 0x4D），不是 0x4D 0x57。
        assert_eq!(MAGIC_BYTES, [0x57, 0x4D]);
    }

    #[test]
    fn wire_layout_is_pinned() {
        let encoded = encode_health(&sample());
        assert_eq!(encoded.len(), FRAME_SIZE);
        // 逐字段按偏移解码：偏移/字节序一改就是协议破坏，DLL 与宿主都会错位。
        assert_eq!(u16::from_le_bytes([encoded[0], encoded[1]]), MAGIC);
        assert_eq!(encoded[2], VERSION);
        assert_eq!(encoded[3], KIND);
        assert_eq!(
            u32::from_le_bytes(encoded[4..8].try_into().unwrap()),
            sample().seq
        );
        assert_eq!(
            u32::from_le_bytes(encoded[8..12].try_into().unwrap()),
            sample().flags
        );
        assert_eq!(
            u64::from_le_bytes(encoded[12..20].try_into().unwrap()),
            sample().timeout_count
        );
        assert_eq!(
            u64::from_le_bytes(encoded[20..28].try_into().unwrap()),
            sample().abandoned_tasks
        );
    }

    #[test]
    fn roundtrip_preserves_extreme_values() {
        let cases = [
            HealthFrame::new(0, 0, 0, 0),
            HealthFrame::new(1, FLAG_PACKET_OUTBOX_DISABLED, 1, 1),
            HealthFrame::new(123_456, KNOWN_FLAGS, 987_654_321, 5),
            HealthFrame::new(u32::MAX, u32::MAX, u64::MAX, u64::MAX),
            HealthFrame::new(u32::MAX - 1, 1 << 31, u64::MAX - 1, 1 << 63),
        ];
        for frame in cases {
            let parsed = parse_health(&encode_health(&frame)).unwrap().unwrap();
            assert_eq!(parsed, frame);
        }
    }

    #[test]
    fn empty_and_partial_frames_ask_for_more_bytes() {
        let encoded = encode_health(&sample());
        // 0..FRAME_SIZE 的全部长度都是合法前缀 → 需要更多字节。
        for len in 0..FRAME_SIZE {
            assert_eq!(parse_health(&encoded[..len]), Ok(None), "len={len}");
        }
        // 恰好 28 字节才解析成功。
        assert_eq!(parse_health(&encoded).unwrap().unwrap(), sample());
    }

    #[test]
    fn prefix_errors_are_rejected_without_waiting_for_full_frame() {
        // 1 字节：低字节就不是 'W'，立即拒绝（不等 28 字节）。
        assert_eq!(parse_health(&[0x4D]), Err(HealthError::BadMagic(0x004D)));
        // 2 字节：magic 高低写反。低字节先被判错，只报已确认的部分。
        assert_eq!(
            parse_health(&[0x4D, 0x57]),
            Err(HealthError::BadMagic(0x004D))
        );
        // 2 字节：magic 第二位错。
        assert_eq!(
            parse_health(&[0x57, 0x57]),
            Err(HealthError::BadMagic(0x5757))
        );
        // 3 字节：magic 对、version 错。
        assert_eq!(
            parse_health(&[0x57, 0x4D, 2]),
            Err(HealthError::BadVersion(2))
        );
        // 4 字节：magic/version 对、kind 错。
        assert_eq!(
            parse_health(&[0x57, 0x4D, 1, 2]),
            Err(HealthError::BadKind(2))
        );
        // 合法前缀但不足 28：仍然是"需要更多字节"，不是错。
        assert_eq!(parse_health(&[0x57, 0x4D, 1, 0x01]), Ok(None));
    }

    #[test]
    fn bad_magic_is_rejected_on_a_full_frame() {
        // 完整 28 字节里 magic 写错，也必须拒绝（不能因为"看起来够长"就解析）。
        let mut encoded = encode_health(&sample());
        encoded[0] = 0x4D;
        assert_eq!(parse_health(&encoded), Err(HealthError::BadMagic(0x004D)));

        let mut encoded = encode_health(&sample());
        encoded[1] = 0x57;
        assert_eq!(parse_health(&encoded), Err(HealthError::BadMagic(0x5757)));
    }

    #[test]
    fn bad_version_and_kind_are_rejected_on_a_full_frame() {
        for version in [0u8, 2, 0x7F, 0xFF] {
            let mut encoded = encode_health(&sample());
            encoded[2] = version;
            assert_eq!(
                parse_health(&encoded),
                Err(HealthError::BadVersion(version)),
                "version={version}"
            );
        }
        for kind in [0u8, 2, 0x04, 0xFF] {
            let mut encoded = encode_health(&sample());
            encoded[3] = kind;
            assert_eq!(
                parse_health(&encoded),
                Err(HealthError::BadKind(kind)),
                "kind={kind}"
            );
        }
    }

    #[test]
    fn all_known_flag_combinations_roundtrip() {
        for bits in 0u32..16 {
            let packet = bits & FLAG_PACKET_OUTBOX_DISABLED != 0;
            let reconnecting = bits & FLAG_RECONNECTING != 0;
            let debug = bits & FLAG_DEBUG_OUTBOX_DISABLED != 0;
            let abandoned = bits & FLAG_ABANDONED_LIMIT_REACHED != 0;
            let flags = pack_flags(packet, reconnecting, debug, abandoned);
            // pack_flags 只使用已知位。
            assert_eq!(flags, bits);

            let frame = HealthFrame::new(1, flags, 0, 0);
            let parsed = parse_health(&encode_health(&frame)).unwrap().unwrap();
            assert_eq!(parsed.flags, flags, "bits={bits}");
            assert_eq!(parsed.packet_outbox_disabled(), packet);
            assert_eq!(parsed.reconnecting(), reconnecting);
            assert_eq!(parsed.debug_outbox_disabled(), debug);
            assert_eq!(parsed.abandoned_limit_reached(), abandoned);
        }
        // 四个全开 = KNOWN_FLAGS，全关 = 0。
        assert_eq!(pack_flags(true, true, true, true), KNOWN_FLAGS);
        assert_eq!(pack_flags(false, false, false, false), 0);
    }

    #[test]
    fn unknown_flag_bits_are_preserved_and_not_rejected() {
        // 未来版本新增的状态位不能让旧宿主报错；原值必须保留供诊断。
        for flags in [1 << 31, 0xFFFF_FFFF, KNOWN_FLAGS | (1 << 16)] {
            let frame = HealthFrame::new(9, flags, 0, 0);
            let parsed = parse_health(&encode_health(&frame)).unwrap().unwrap();
            assert_eq!(parsed.flags, flags);
            // 已知位仍然可读。
            assert_eq!(parsed.packet_outbox_disabled(), flags & 1 != 0);
            assert_eq!(parsed.reconnecting(), flags & FLAG_RECONNECTING != 0);
        }
    }

    #[test]
    fn seq_wraparound_is_representable() {
        let last = HealthFrame::new(u32::MAX, 0, 0, 0);
        let wrapped = HealthFrame::new(last.seq.wrapping_add(1), 0, 0, 0);
        assert_eq!(wrapped.seq, 0);
        assert_eq!(
            parse_health(&encode_health(&last)).unwrap().unwrap().seq,
            u32::MAX
        );
        assert_eq!(
            parse_health(&encode_health(&wrapped)).unwrap().unwrap().seq,
            0
        );

        // 连续两帧放在同一缓冲区：按固定 28 字节切分后 seq 回绕仍然连续。
        let mut buffer = Vec::new();
        buffer.extend_from_slice(&encode_health(&last));
        buffer.extend_from_slice(&encode_health(&wrapped));
        let first = parse_health(&buffer).unwrap().unwrap();
        let second = parse_health(&buffer[FRAME_SIZE..]).unwrap().unwrap();
        assert_eq!(first.seq, u32::MAX);
        assert_eq!(second.seq, 0);
        // 宿主判断丢帧必须用 wrapping 语义：回绕后差值仍是 1。
        assert_eq!(second.seq.wrapping_sub(first.seq), 1);
    }

    #[test]
    fn stream_buffer_can_hold_multiple_frames_and_a_partial_one() {
        let first = HealthFrame::new(1, 0, 2, 3);
        let second = HealthFrame::new(2, FLAG_RECONNECTING, 4, 5);
        let mut buffer = Vec::new();
        buffer.extend_from_slice(&encode_health(&first));
        buffer.extend_from_slice(&encode_health(&second));
        buffer.extend_from_slice(&encode_health(&first)[..10]); // 尾部的半帧

        assert_eq!(parse_health(&buffer).unwrap().unwrap(), first);
        let rest = &buffer[FRAME_SIZE..];
        assert_eq!(parse_health(rest).unwrap().unwrap(), second);
        let tail = &rest[FRAME_SIZE..];
        assert_eq!(tail.len(), 10);
        assert_eq!(parse_health(tail), Ok(None));
    }

    #[test]
    fn truncated_frame_that_is_already_invalid_is_an_error() {
        // 半帧里 magic 已经错：立即报错，而不是"等更多字节再判断"。
        let encoded = encode_health(&sample());
        for len in 1..FRAME_SIZE {
            let mut broken = encoded[..len].to_vec();
            if len >= 1 {
                broken[0] = 0x00;
            }
            assert_eq!(
                parse_health(&broken),
                Err(HealthError::BadMagic(0x0000)),
                "len={len}"
            );
        }
    }

    #[test]
    fn parse_health_never_panics_for_any_short_length() {
        // 穷举 0..FRAME_SIZE 的前缀 + 多种 garbage，只允许 Ok/Err，不允许 panic。
        let mut garbage = [0u8; FRAME_SIZE];
        for (index, byte) in garbage.iter_mut().enumerate() {
            *byte = (index as u8).wrapping_mul(37);
        }
        for len in 0..FRAME_SIZE {
            let _ = parse_health(&garbage[..len]);
        }
        let _ = parse_health(&[]);
        let _ = parse_health(&[0xFF; FRAME_SIZE]);
    }

    /// 跨语言契约：解析 `health_vector.rs` 里钉死的字节，必须得到预期字段。
    ///
    /// 这条测试的价值在于断言的是**固定字节**，而不是「自己编码再自己解码」
    /// （后者两侧都改也能自洽）。C# 侧使用同一组字节做同名断言。
    #[test]
    fn parses_cross_language_golden_vector() {
        use crate::health_vector::*;
        let frame = parse_health(&GOLDEN_HEALTH_FRAME)
            .expect("golden vector must parse")
            .expect("golden vector is a complete frame");
        assert_eq!(frame.seq, GOLDEN_SEQ);
        assert_eq!(frame.flags, GOLDEN_FLAGS);
        assert_eq!(frame.timeout_count, GOLDEN_TIMEOUT_COUNT);
        assert_eq!(frame.abandoned_tasks, GOLDEN_ABANDONED_TASKS);
        assert!(frame.packet_outbox_disabled());
        assert!(frame.reconnecting());
        assert!(!frame.debug_outbox_disabled());
        assert!(!frame.abandoned_limit_reached());
    }

    /// 编码器必须能原样复现同一组字节。
    ///
    /// 与上一条合起来形成双向约束：解析认固定字节，编码产出固定字节。
    #[test]
    fn encoder_reproduces_golden_vector() {
        use crate::health_vector::*;
        let frame = HealthFrame::new(
            GOLDEN_SEQ,
            GOLDEN_FLAGS,
            GOLDEN_TIMEOUT_COUNT,
            GOLDEN_ABANDONED_TASKS,
        )
        .with_diagnostics(GOLDEN_INVALID_LENGTH, GOLDEN_FORWARD_ERRORS)
        .with_pipeline(
            GOLDEN_HOOK_ENTERED,
            GOLDEN_BAD_MAGIC,
            GOLDEN_TRY_GET_DATA_FAIL,
            GOLDEN_ACCEPTED,
        )
        .with_filter(
            GOLDEN_REQUIRED_COUNT,
            GOLDEN_FILTERED,
            GOLDEN_LAST_FILTERED_CMD,
        )
        .with_required_sample(GOLDEN_REQUIRED_SAMPLE)
        .with_cmd_seen(GOLDEN_CMD_SEEN, GOLDEN_CMD_SEEN_TOTAL)
        .with_raw_header(GOLDEN_RAW_HEADER)
        .with_probe_len(GOLDEN_PROBE_LEN);
        assert_eq!(encode_health(&frame), GOLDEN_HEALTH_FRAME);
    }
}
