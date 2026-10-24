//! 收包统计：用于确认注入链路是否真的在收数据。

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

static TOTAL: AtomicU64 = AtomicU64::new(0);
static COUNTS: Mutex<BTreeMap<u16, u64>> = Mutex::new(BTreeMap::new());

/// 记录一个包。每 200 个包打印一次汇总，便于现场诊断。
pub fn record(cmd: u16, len: usize) {
    let total = TOTAL.fetch_add(1, Ordering::Relaxed) + 1;
    if let Ok(mut counts) = COUNTS.lock() {
        *counts.entry(cmd).or_insert(0) += 1;
        if total.is_multiple_of(200) {
            let snapshot: Vec<String> = counts
                .iter()
                .rev()
                .take(8)
                .map(|(c, n)| format!("{c}×{n}"))
                .collect();
            tracing::info!("收包统计: 共 {total}，最高频 {}", snapshot.join(" "));
        }
    }
    let _ = len;
}

pub fn total() -> u64 {
    TOTAL.load(Ordering::Relaxed)
}