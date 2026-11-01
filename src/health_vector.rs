//! 跨语言协议契约测试向量。
//!
//! 为什么需要这个文件：Rust 宿主与 C# DLL 各自测自己的编码/解析，**两边都通过**
//! 并不等于**两边一致**——单侧悄悄改了魔数、字段偏移或字节序，双侧单测仍然全绿，
//! 只有真机联调才会暴露。这里把一端真实产出的字节钉死成向量，另一个语言直接
//! 用它断言解析结果，任何单侧改动都会立刻失败。
//!
//! 向量由 `src/health.rs` 的 `encode_health` 实际产出：
//!   HealthFrame::new(seq=7, flags=FLAG_PACKET_OUTBOX_DISABLED|FLAG_RECONNECTING, timeout_count=42, abandoned_tasks=3)
//!
//! 修改协议时必须同步更新本文件，并在两侧测试中各跑一次。

/// 固定测试向量：28 字节，小端。
pub const GOLDEN_HEALTH_FRAME: [u8; 28] = [
    0x57, 0x4D, // magic（u16 0x4D57 的小端写法，'W' 'M'）
    0x01, // version
    0x01, // kind = Health
    0x07, 0x00, 0x00, 0x00, // seq = 7
    0x03, 0x00, 0x00, 0x00, // flags = bit0|bit1
    0x2A, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // timeout_count = 42
    0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // abandoned_tasks = 3
];

/// 向量期望解析出的 seq。
pub const GOLDEN_SEQ: u32 = 7;
/// 向量期望解析出的 flags。
pub const GOLDEN_FLAGS: u32 = 0b11;
/// 向量期望解析出的 timeout_count。
pub const GOLDEN_TIMEOUT_COUNT: u64 = 42;
/// 向量期望解析出的 abandoned_tasks。
pub const GOLDEN_ABANDONED_TASKS: u64 = 3;
