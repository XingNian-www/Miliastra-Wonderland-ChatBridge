//! 解析 YaeAchievement 的 metadata：RVA 表与 cmdId。
//!
//! metadata 是作者 CDN 下发的 protobuf（无公开 .proto，结构见
//! Genshin/Yae/YaeAchievement/res/proto/AchievementInfo.proto）。
//!
//! 关键结构：
//!   AchievementInfo {
//!     version = 1
//!     native_config = 5 {
//!       store_cmd_id = 1
//!       achievement_cmd_id = 2
//!       method_rva = 10 (map<uint32 crc, MethodRvaConfig>)
//!     }
//!   }
//!   MethodRvaConfig { do_cmd=1, update_normal_prop=3, new_string=4, find_game_object=5,
//!                     event_system_update=6, simulate_pointer_click=7, to_int32=8,
//!                     tcp_state_ptr=9, shared_info_ptr=10, decompress=11 }
//!
//! map 的 key 是游戏 exe 前 64KiB 的 CRC32；游戏更新后这张表会换新。

use std::collections::BTreeMap;

use anyhow::{Context, Result};

use crate::proto::{self, Fields};

/// 一次注入需要的全部函数 RVA。
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MethodRva {
    pub do_cmd: u32,
    pub update_normal_prop: u32,
    pub new_string: u32,
    pub find_game_object: u32,
    pub event_system_update: u32,
    pub simulate_pointer_click: u32,
    pub to_int32: u32,
    pub tcp_state_ptr: u32,
    pub shared_info_ptr: u32,
    pub decompress: u32,
}

impl MethodRva {
    /// 按 DLL 期望的顺序展开（见 Goshujin.LoadMethodTable）。
    pub fn to_wire(&self) -> [u32; 10] {
        [
            self.do_cmd,
            self.update_normal_prop,
            self.new_string,
            self.find_game_object,
            self.event_system_update,
            self.simulate_pointer_click,
            self.to_int32,
            self.tcp_state_ptr,
            self.shared_info_ptr,
            self.decompress,
        ]
    }

    /// 钩子和解压使用的地址必须全部存在；缺失指针槽不能当作模块基址使用。
    pub fn is_valid(&self) -> bool {
        self.to_int32 != 0
            && self.tcp_state_ptr != 0
            && self.shared_info_ptr != 0
            && self.decompress != 0
    }
}

/// 解析后的 metadata。
#[derive(Clone, Debug)]
pub struct Metadata {
    pub version: String,
    /// 游戏 exe CRC32 → RVA 表。
    pub rva_by_game: BTreeMap<u32, MethodRva>,
}

impl Metadata {
    /// 解析 metadata protobuf。
    pub fn parse(bytes: &[u8]) -> Result<Self> {
        let root = proto::parse(bytes).context("解析 metadata 顶层失败")?;
        let version = root.string(1).unwrap_or("unknown").to_owned();
        let native = root.message(5).context("metadata 缺少 native_config")?;
        let mut rva_by_game = BTreeMap::new();
        for entry in native.repeated_messages(10) {
            let Some(crc) = entry.varint(1).and_then(|v| u32::try_from(v).ok()) else {
                continue;
            };
            let Some(config) = entry.message(2) else {
                continue;
            };
            rva_by_game.insert(crc, parse_rva(&config));
        }
        if rva_by_game.is_empty() {
            anyhow::bail!("metadata 里没有任何 method_rva 条目");
        }
        Ok(Self {
            version,
            rva_by_game,
        })
    }

    /// 按游戏 exe CRC32 查 RVA 表。
    pub fn for_game(&self, crc32: u32) -> Option<&MethodRva> {
        self.rva_by_game.get(&crc32)
    }
}

fn parse_rva(config: &Fields) -> MethodRva {
    let get = |field: u32| {
        config
            .varint(field)
            .and_then(|v| u32::try_from(v).ok())
            .unwrap_or(0)
    };
    MethodRva {
        do_cmd: get(1),
        update_normal_prop: get(3),
        new_string: get(4),
        find_game_object: get(5),
        event_system_update: get(6),
        simulate_pointer_click: get(7),
        to_int32: get(8),
        tcp_state_ptr: get(9),
        shared_info_ptr: get(10),
        decompress: get(11),
    }
}

/// 复刻 Yae 的 GetGameHash：前 64KiB 的标准 CRC32。
pub fn game_crc32(head: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFFu32;
    for byte in head {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0xEDB8_8320
            } else {
                crc >> 1
            };
        }
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::*;

    fn varint(mut v: u64) -> Vec<u8> {
        let mut out = Vec::new();
        loop {
            let mut b = (v & 0x7f) as u8;
            v >>= 7;
            if v != 0 {
                b |= 0x80;
            }
            out.push(b);
            if v == 0 {
                return out;
            }
        }
    }

    fn tag(f: u32, w: u8) -> Vec<u8> {
        varint(((f as u64) << 3) | u64::from(w))
    }

    fn fv(f: u32, v: u64) -> Vec<u8> {
        let mut o = tag(f, 0);
        o.extend(varint(v));
        o
    }

    fn fb(f: u32, v: &[u8]) -> Vec<u8> {
        let mut o = tag(f, 2);
        o.extend(varint(v.len() as u64));
        o.extend(v);
        o
    }

    #[test]
    fn crc32_matches_known_value() {
        // "123456789" 的标准 CRC32 是 0xCBF43926。
        assert_eq!(game_crc32(b"123456789"), 0xCBF4_3926);
    }

    #[test]
    fn parses_metadata_with_rva_table() {
        let rva = [
            fv(1, 193063152),
            fv(8, 425587504),
            fv(9, 91775080),
            fv(10, 91775072),
            fv(11, 115234864),
        ]
        .concat();
        let entry = [fv(1, 0xe597_a764), fb(2, &rva)].concat();
        let native = [fv(1, 22160), fv(2, 24317), fb(10, &entry)].concat();
        let root = [fb(1, b"7.1.0"), fb(5, &native)].concat();

        let meta = Metadata::parse(&root).expect("解析");
        assert_eq!(meta.version, "7.1.0");
        let table = meta.for_game(0xe597_a764).expect("查到 RVA 表");
        assert_eq!(table.to_int32, 425587504);
        assert_eq!(table.tcp_state_ptr, 91775080);
        assert_eq!(table.decompress, 115234864);
        assert!(table.is_valid());
    }

    #[test]
    fn unknown_game_crc_returns_none() {
        let rva = fv(8, 12345);
        let entry = [fv(1, 1), fb(2, &rva)].concat();
        let native = fb(10, &entry);
        let root = [fb(1, b"x"), fb(5, &native)].concat();
        let meta = Metadata::parse(&root).expect("解析");
        assert!(meta.for_game(999).is_none());
    }

    #[test]
    fn zero_rva_is_invalid() {
        assert!(!MethodRva::default().is_valid());
        let ok = MethodRva {
            to_int32: 1,
            tcp_state_ptr: 1,
            shared_info_ptr: 1,
            decompress: 1,
            ..Default::default()
        };
        assert!(ok.is_valid());
        for missing in 0..4 {
            let mut incomplete = ok.clone();
            match missing {
                0 => incomplete.to_int32 = 0,
                1 => incomplete.tcp_state_ptr = 0,
                2 => incomplete.shared_info_ptr = 0,
                _ => incomplete.decompress = 0,
            }
            assert!(
                !incomplete.is_valid(),
                "缺少必要 RVA {missing} 时必须拒绝注入"
            );
        }
    }
}
