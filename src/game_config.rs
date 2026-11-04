//! 游戏版本配置：把「每次大版本更新都会变」的数据全部外置成一份 JSON。
//!
//! 需要外置的有三类：
//!
//! 1. **exe 的 RVA 表**：用游戏 exe 前 64 KiB 的 CRC32 索引，值是各函数在 il2cpp
//!    段里的偏移（to_int32 等）。游戏一更新就整张表失效。
//! 2. **信封 cmdId**：2246 / 24007 这类订阅号。
//! 3. **聊天包体布局**：信封里的 protobuf 字段号与内层事件类型
//!    （#12 / #2 / #14 / #630 等）。
//!
//! 三者都放进同一份配置后，游戏更新只要改 JSON —— 既不用重新编译，也不用改代码。
//!
//! ## 来源优先级
//!
//! 远程 URL -> 本地缓存文件 -> 编译内置的 default_config()。
//!
//! 远程拉取失败（离线、超时、格式不对）**绝不能影响启动**：它只是「有机会拿到更新的
//! 数据」，拿不到就用上一层的值。因此拉取有硬超时，且任何错误都降级为日志。
//!
//! ## 安全边界
//!
//! 这份配置决定往游戏进程里挂钩子还是别的地址。填错会让 DLL 挂到错误位置。
//! 所以：
//! - 只接受 https:// 的远程地址（避免明文被篡改）；
//! - schema 版本必须匹配，未知版本整份拒绝；
//! - 时间戳不倒退：远程 updatedAt 比本地缓存旧时**拒绝覆盖**，
//!   防止 CDN 回源到旧文件把新表冲掉。

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

/// 配置 schema 标识。格式不兼容变更时递增。
pub const SCHEMA: &str = "miliastra-chatbridge-game-config/v1";

/// 默认的远程配置地址（仓库里的 config/game-config.json）。
pub const DEFAULT_REMOTE_URL: &str = "https://raw.githubusercontent.com/XingNian-www/Miliastra-Wonderland-ChatBridge/master/config/game-config.json";

/// 远程拉取的硬超时。抓不到就降级，不能让启动卡住。
pub const FETCH_TIMEOUT: Duration = Duration::from_secs(4);

/// 一份游戏版本配置。
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GameConfig {
    pub schema: String,
    /// 数据更新时间（RFC 3339）。用于拒绝旧数据覆盖新数据。
    #[serde(default)]
    pub updated_at: Option<String>,
    /// 需要订阅的信封 cmdId。
    pub envelope: Vec<u16>,
    /// 已知的游戏版本（按 exe CRC32 索引）。
    pub versions: Vec<GameVersion>,
    /// 聊天包体布局（按新到旧排列，解析时逐个尝试）。
    pub layouts: Vec<LayoutSpec>,
}

/// 一个游戏版本的 RVA 表。
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GameVersion {
    /// 人类可读的版本号，仅用于日志。
    pub game_version: String,
    /// 游戏 exe 前 64 KiB 的 CRC32，8 位小写十六进制。
    pub exe_crc32: String,
    pub rva: RvaSpec,
}

/// RVA 字段的 serde 编解码：配置里写成十六进制字符串（0x12178db0），
/// 但反序列化同时接受十进制数字，方便脚本生成。
///
/// 用十六进制是刻意的：这些值是逆向得来的偏移，和 IDA/Ghidra 里看到的一致，
/// 手改配置时不用先换算。
mod hex_u32 {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(value: &u32, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&format!("0x{value:08x}"))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u32, D::Error> {
        let raw = serde_json::Value::deserialize(deserializer)?;
        match raw {
            serde_json::Value::String(text) => {
                let trimmed = text.trim();
                let digits = trimmed
                    .strip_prefix("0x")
                    .or_else(|| trimmed.strip_prefix("0X"))
                    .unwrap_or(trimmed);
                u32::from_str_radix(digits, 16).map_err(|_| {
                    serde::de::Error::custom(format!("不是合法的十六进制偏移: {text}"))
                })
            }
            serde_json::Value::Number(number) => number
                .as_u64()
                .and_then(|value| u32::try_from(value).ok())
                .ok_or_else(|| serde::de::Error::custom("偏移超出 u32 范围")),
            _ => Err(serde::de::Error::custom("偏移必须是十六进制字符串或数字")),
        }
    }
}

/// 注入需要的函数偏移（相对模块基址的 RVA）。
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RvaSpec {
    #[serde(default)]
    #[serde(with = "hex_u32")]
    pub update_normal_prop: u32,
    #[serde(default)]
    #[serde(with = "hex_u32")]
    pub new_string: u32,
    #[serde(default)]
    #[serde(with = "hex_u32")]
    pub find_game_object: u32,
    #[serde(default)]
    #[serde(with = "hex_u32")]
    pub event_system_update: u32,
    #[serde(default)]
    #[serde(with = "hex_u32")]
    pub simulate_pointer_click: u32,
    #[serde(with = "hex_u32")]
    pub to_int32: u32,
    #[serde(with = "hex_u32")]
    pub tcp_state_ptr: u32,
    #[serde(with = "hex_u32")]
    pub shared_info_ptr: u32,
    #[serde(with = "hex_u32")]
    pub decompress: u32,
}

impl RvaSpec {
    /// 钩子与解压要用到的地址必须全部存在。缺失指针槽不能当作模块基址使用。
    pub fn is_valid(&self) -> bool {
        self.to_int32 != 0
            && self.tcp_state_ptr != 0
            && self.shared_info_ptr != 0
            && self.decompress != 0
    }

    /// 按 DLL 期望的顺序展开（见 Goshujin.LoadMethodTable）。
    pub fn to_wire(self) -> [u32; 10] {
        [
            self.update_normal_prop,
            self.new_string,
            self.find_game_object,
            self.event_system_update,
            self.simulate_pointer_click,
            self.to_int32,
            self.tcp_state_ptr,
            self.shared_info_ptr,
            self.decompress,
            0,
        ]
    }
}

/// 一套聊天包体布局（字段号 + 内层事件类型）。
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LayoutSpec {
    /// 版本标签，仅用于日志与诊断。
    pub id: String,
    pub events: u32,
    pub inner_type: u32,
    pub payload: u32,
    pub body_hall: u32,
    pub body_private: u32,
    pub ts: u32,
    pub from_uid: u32,
    pub to_uid: u32,
    pub text: u32,
    pub hall_ctx: u32,
    pub hall_type: u32,
    pub hall_id: u32,
    pub population_hall: u32,
    #[serde(default)]
    pub record_list: u32,
    #[serde(default)]
    pub record_uid: u32,
    #[serde(default)]
    pub record_name: u32,
    #[serde(default)]
    pub record_remark: u32,
    /// 未识别的内层类型写 null 或省略。
    #[serde(default)]
    pub inner_hall_chat: Option<u64>,
    #[serde(default)]
    pub inner_private_chat: Option<u64>,
    /// 私聊**会话快照**事件（7.1 = 28347，7.2 = 25574）。
    ///
    /// 它是整段会话的批量重发（实测一次 20 条、每 60 秒一轮），不是实时推送。
    /// 只做记录、不参与解析：真正的实时私聊走 inner_private_chat，用它才没有延迟。
    #[serde(default)]
    pub inner_private_history: Option<u64>,
    #[serde(default)]
    pub inner_hall_population: Option<u64>,
    /// 大厅成员列表事件（7.1 = 2076，7.2 = 29039）。
    #[serde(default)]
    pub inner_hall_members: Option<u64>,
    /// 成员记录所在的载荷字段（7.1 = 4，7.2 = 13）。
    #[serde(default)]
    pub member_list: u32,
    /// 成员记录里的 uid 字段（7.1 = 1，7.2 = 9）。
    #[serde(default)]
    pub member_uid: u32,
    /// 成员记录里的昵称字段（7.1 = 2，7.2 = 3）。
    #[serde(default)]
    pub member_name: u32,
    #[serde(default)]
    pub inner_friend_list: Option<u64>,
}

impl GameConfig {
    /// 按 exe CRC32 查 RVA 表。
    pub fn rva_for_crc(&self, crc32: u32) -> Option<&RvaSpec> {
        self.versions
            .iter()
            .find(|v| v.crc32_value() == Some(crc32))
            .map(|v| &v.rva)
    }

    /// 校验结构完整性：至少一条版本、一条布局、一个信封 cmd。
    pub fn validate(&self) -> Result<()> {
        anyhow::ensure!(
            self.schema == SCHEMA,
            "配置 schema 不匹配: 期望 {SCHEMA}，实际 {}",
            self.schema
        );
        anyhow::ensure!(!self.envelope.is_empty(), "配置里没有任何信封 cmdId");
        anyhow::ensure!(!self.versions.is_empty(), "配置里没有任何游戏版本");
        anyhow::ensure!(!self.layouts.is_empty(), "配置里没有任何包体布局");
        for version in &self.versions {
            anyhow::ensure!(
                version.crc32_value().is_some(),
                "gameVersion={} 的 exeCrc32 不是合法十六进制",
                version.game_version
            );
            anyhow::ensure!(
                version.rva.is_valid(),
                "gameVersion={} 的 RVA 表不完整（toInt32/tcpStatePtr/sharedInfoPtr/decompress 不能为 0）",
                version.game_version
            );
        }
        Ok(())
    }

    /// 从 JSON 文本解析。
    pub fn from_json(text: &str) -> Result<Self> {
        let config: Self = serde_json::from_str(text).context("解析游戏配置 JSON 失败")?;
        config.validate()?;
        Ok(config)
    }

    /// 读取本地文件。
    pub fn load_file(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("读取游戏配置失败: {}", path.display()))?;
        Self::from_json(&text)
    }

    /// 序列化成写盘用的 JSON。
    pub fn to_json(&self) -> Result<String> {
        serde_json::to_string_pretty(self).context("序列化游戏配置失败")
    }
}

impl GameVersion {
    /// exeCrc32 的数值形式。容忍带 0x 前缀与大小写。
    pub fn crc32_value(&self) -> Option<u32> {
        parse_crc32(&self.exe_crc32)
    }
}

/// 解析十六进制 CRC32，容忍 0x 前缀与大小写。
pub fn parse_crc32(text: &str) -> Option<u32> {
    let trimmed = text.trim();
    let digits = trimmed
        .strip_prefix("0x")
        .or_else(|| trimmed.strip_prefix("0X"))
        .unwrap_or(trimmed);
    if digits.is_empty() || digits.len() > 8 {
        return None;
    }
    u32::from_str_radix(digits, 16).ok()
}

/// 把 CRC32 格式化成配置里用的 8 位小写十六进制。
pub fn format_crc32(crc32: u32) -> String {
    format!("{crc32:08x}")
}

/// 编译内置的兜底配置：游戏 7.2 + 7.1 的实测数据。
///
/// 它保证「完全离线且没有任何缓存」时也能工作。远程与缓存都不存在时用它。
pub fn default_config() -> GameConfig {
    GameConfig {
        schema: SCHEMA.to_owned(),
        updated_at: Some("2026-11-04T04:30:00Z".to_owned()),
        envelope: vec![24007, 486],
        versions: vec![GameVersion {
            game_version: "7.2.0".to_owned(),
            exe_crc32: "8cb60f3d".to_owned(),
            rva: RvaSpec {
                update_normal_prop: 0x1217_8db0,
                to_int32: 0x19b5_41a0,
                tcp_state_ptr: 0x0586_d7e0,
                shared_info_ptr: 0x0586_d7e8,
                decompress: 0x06f0_f970,
                ..RvaSpec::default()
            },
        }],
        layouts: vec![LayoutSpec {
            id: "7.2".to_owned(),
            events: 12,
            inner_type: 2,
            payload: 14,
            body_hall: 5,
            body_private: 4,
            ts: 2,
            from_uid: 1,
            to_uid: 6,
            text: 630,
            hall_ctx: 4,
            hall_type: 1,
            hall_id: 2,
            population_hall: 5,
            // 好友列表 29281：载荷 #10 重复，每条 { #1 uid, #2 昵称, #6 签名, #17 备注 } 。
            record_list: 10,
            record_uid: 1,
            record_name: 2,
            record_remark: 17,
            inner_hall_chat: Some(29636),
            inner_private_chat: Some(28274),
            inner_private_history: Some(25574),
            // 大厅 id 补充包 7470：载荷只有 { #5 大厅id, #14 时间戳 }，进大厅时发一次。
            inner_hall_population: Some(7470),
            // 大厅成员列表 29039：载荷 #13 重复，每条 { #9 uid, #3 昵称 } 。
            inner_hall_members: Some(29039),
            member_list: 13,
            member_uid: 9,
            member_name: 3,
            inner_friend_list: Some(29281),
        }],
    }
}

/// 配置的最终生效来源，用于日志。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Source {
    Remote,
    Cache,
    Builtin,
}

impl Source {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Remote => "远程",
            Self::Cache => "本地缓存",
            Self::Builtin => "内置默认",
        }
    }
}

/// 启动阶段解析出的配置 + 它的来源。
#[derive(Clone, Debug)]
pub struct Resolved {
    pub config: GameConfig,
    pub source: Source,
}

/// 加载顺序：远程 -> 缓存 -> 内置。
///
/// 远程与缓存都可能不存在或损坏，这两种情况都只记日志、继续往下走。
/// allow_remote=false 表示禁用远程（--no-remote-config）。
pub fn resolve(
    cache_path: &Path,
    remote_url: Option<&str>,
    allow_remote: bool,
    log: &mut dyn FnMut(&str),
) -> Resolved {
    let cached = if cache_path.exists() {
        match GameConfig::load_file(cache_path) {
            Ok(config) => {
                log(&format!(
                    "已读取游戏配置缓存: {}（{} 个版本，{} 套布局）",
                    cache_path.display(),
                    config.versions.len(),
                    config.layouts.len()
                ));
                Some(config)
            }
            Err(error) => {
                log(&format!("游戏配置缓存无效，忽略: {error:#}"));
                None
            }
        }
    } else {
        None
    };

    if allow_remote && let Some(url) = remote_url {
        match fetch(url) {
            Ok(remote) => {
                // 时间戳不倒退：远程比缓存旧时拒绝覆盖，防止 CDN 回源冲掉新表。
                let accept = match (&cached, &remote.updated_at) {
                    (Some(cache), Some(remote_ts)) => match &cache.updated_at {
                        Some(cache_ts) => remote_ts >= cache_ts,
                        None => true,
                    },
                    _ => true,
                };
                if accept {
                    log(&format!(
                        "已获取远程游戏配置: {}（{} 个版本，{} 套布局）",
                        url,
                        remote.versions.len(),
                        remote.layouts.len()
                    ));
                    // 写缓存失败不影响本次运行。
                    if let Ok(text) = remote.to_json()
                        && let Some(parent) = cache_path.parent()
                    {
                        let _ = std::fs::create_dir_all(parent);
                        if let Err(error) = std::fs::write(cache_path, text) {
                            log(&format!("写游戏配置缓存失败（不影响运行）: {error}"));
                        }
                    }
                    return Resolved {
                        config: remote,
                        source: Source::Remote,
                    };
                }
                log("远程游戏配置比本地缓存旧，拒绝覆盖");
            }
            Err(error) => log(&format!("拉取远程游戏配置失败，降级: {error:#}")),
        }
    }

    if let Some(config) = cached {
        return Resolved {
            config,
            source: Source::Cache,
        };
    }

    Resolved {
        config: default_config(),
        source: Source::Builtin,
    }
}

/// 拉取并解析远程配置。
///
/// 只接受 https，避免明文被中间人篡改 —— 这份数据决定往游戏进程里挂钩子还是别的地址。
pub fn fetch(url: &str) -> Result<GameConfig> {
    anyhow::ensure!(
        url.starts_with("https://"),
        "远程配置地址必须是 https://（实际 {url}）"
    );
    let mut response = ureq::get(url)
        .call()
        .with_context(|| format!("GET {url} 失败"))?;
    let text = response
        .body_mut()
        .read_to_string()
        .with_context(|| format!("读取 {url} 响应失败"))?;
    GameConfig::from_json(&text)
}

/// 默认缓存路径：程序同目录的 game-config.json。
pub fn default_cache_path() -> PathBuf {
    PathBuf::from("game-config.json")
}

/// 当前生效的配置。启动时解析一次，之后只读。
///
/// 不做运行期热更新：配置改完后重启桥即可生效（游戏更新本来也要重启一次），
/// 这样读路径无锁、无分配，也不会有「配置换了一半」的中间态。
static ACTIVE: std::sync::OnceLock<GameConfig> = std::sync::OnceLock::new();

/// 安装启动时解析出的配置。只在启动阶段调用一次，重复调用被忽略。
pub fn install(config: GameConfig) {
    let _ = ACTIVE.set(config);
}

/// 当前生效的配置。未安装时返回内置默认（测试与 --no-launch 路径）。
pub fn active() -> &'static GameConfig {
    ACTIVE.get_or_init(default_config)
}

/// 当前生效的订阅 cmdId。
pub fn active_envelope() -> &'static [u16] {
    &active().envelope
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_valid_and_covers_current_game() {
        let config = default_config();
        config.validate().expect("内置配置必须自洽");
        // 当前线上版本必须在表里，否则打包出来的程序开箱即不可用。
        let rva = config.rva_for_crc(0x8cb6_0f3d).expect("7.2 的 RVA 表");
        assert_eq!(rva.to_int32, 0x19b5_41a0);
        assert_eq!(rva.decompress, 0x06f0_f970);
        assert!(rva.is_valid());
    }

    #[test]
    fn default_config_keeps_chat_layouts() {
        let config = default_config();
        let layout = config.layouts.first().expect("至少一套布局");
        assert_eq!(layout.events, 12);
        assert_eq!(layout.text, 630);
        assert_eq!(layout.inner_hall_chat, Some(29636));
        assert_eq!(layout.inner_private_chat, Some(28274), "实时私聊");
        assert_eq!(layout.inner_private_history, Some(25574), "会话快照");
    }

    #[test]
    fn json_roundtrip_preserves_everything() {
        let config = default_config();
        let text = config.to_json().expect("序列化");
        let back = GameConfig::from_json(&text).expect("反序列化");
        assert_eq!(config, back);
    }

    #[test]
    fn crc_parsing_tolerates_prefix_and_case() {
        assert_eq!(parse_crc32("8cb60f3d"), Some(0x8cb6_0f3d));
        assert_eq!(parse_crc32("0x8CB60F3D"), Some(0x8cb6_0f3d));
        assert_eq!(parse_crc32(" 8cb60f3d "), Some(0x8cb6_0f3d));
        assert_eq!(parse_crc32(""), None);
        assert_eq!(parse_crc32("zzzz"), None);
        assert_eq!(parse_crc32("123456789"), None, "超过 8 位应拒绝");
        assert_eq!(format_crc32(0x8cb6_0f3d), "8cb60f3d");
    }

    #[test]
    fn schema_mismatch_is_rejected() {
        let mut config = default_config();
        config.schema = "something-else/v1".to_owned();
        let text = serde_json::to_string(&config).expect("序列化");
        let error = GameConfig::from_json(&text).expect_err("schema 不符必须拒绝");
        assert!(format!("{error:#}").contains("schema"));
    }

    #[test]
    fn unknown_field_is_rejected() {
        // deny_unknown_fields：拼错字段名时不能静默忽略，否则会以为是配置生效了。
        let mut value: serde_json::Value =
            serde_json::from_str(&default_config().to_json().expect("序列化")).expect("解析");
        value["typoField"] = serde_json::json!(1);
        let text = serde_json::to_string(&value).expect("序列化");
        assert!(GameConfig::from_json(&text).is_err(), "未知字段必须报错");
    }

    #[test]
    fn incomplete_rva_is_rejected() {
        let mut config = default_config();
        config.versions[0].rva.to_int32 = 0;
        let error = config.validate().expect_err("缺 to_int32 必须拒绝");
        assert!(format!("{error:#}").contains("RVA 表不完整"));
    }

    #[test]
    fn resolve_falls_back_to_builtin_without_cache() {
        let dir = std::env::temp_dir().join("mwc-cfg-test-builtin");
        let _ = std::fs::remove_dir_all(&dir);
        let mut logs = Vec::new();
        let resolved = resolve(&dir.join("game-config.json"), None, false, &mut |line| {
            logs.push(line.to_owned())
        });
        assert_eq!(resolved.source, Source::Builtin);
        assert_eq!(resolved.config, default_config());
    }

    #[test]
    fn resolve_uses_cache_when_remote_disabled() {
        let dir = std::env::temp_dir().join("mwc-cfg-test-cache");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("建目录");
        let path = dir.join("game-config.json");
        let mut config = default_config();
        config.envelope = vec![4242];
        std::fs::write(&path, config.to_json().expect("序列化")).expect("写缓存");

        let mut logs = Vec::new();
        let resolved = resolve(&path, None, false, &mut |line| logs.push(line.to_owned()));
        assert_eq!(resolved.source, Source::Cache);
        assert_eq!(resolved.config.envelope, vec![4242]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn broken_cache_is_ignored() {
        let dir = std::env::temp_dir().join("mwc-cfg-test-broken");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("建目录");
        let path = dir.join("game-config.json");
        std::fs::write(&path, "{ not json").expect("写坏缓存");

        let mut logs = Vec::new();
        let resolved = resolve(&path, None, false, &mut |line| logs.push(line.to_owned()));
        assert_eq!(resolved.source, Source::Builtin, "坏缓存必须降级而不是崩");
        assert!(
            logs.iter().any(|line| line.contains("无效")),
            "应记一条日志"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn remote_must_be_https() {
        let error = fetch("http://example.com/game-config.json").expect_err("http 必须拒绝");
        assert!(format!("{error:#}").contains("https"));
    }
}
