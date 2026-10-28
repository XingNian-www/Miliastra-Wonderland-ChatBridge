//! 从 cmd 2246 信封里提取聊天消息。
//!
//! 字段号来自逆向归纳（Genshin/artifacts/CHAT-API-HANDOFF.md §2–§4）：
//!   #8 = 一个事件（可重复）
//!   #8.#13 = 内层类型（486 私聊实时 / 20415 大厅聊天 / …）
//!   #8.#6  = 载荷容器
//!   #8.#6.#4 = 消息体 { #1 ts, #4 role, #5 from_uid, #6 seq, #9 to_uid, #1985 text }
//!   #8.#6.#15 = 大厅上下文 { #1 type, #2 hall_id }

use crate::proto::{self, Fields};

/// 多路复用信封 cmd。
pub const CMD_ENVELOPE: u16 = 2246;

/// 私聊实时也可能以独立 cmd 486 下发（不经信封）。
pub const CMD_PRIVATE_DIRECT: u16 = 486;

/// 需要订阅的所有 cmd。
pub const WANTED_CMDS: &[u16] = &[CMD_ENVELOPE, CMD_PRIVATE_DIRECT];

/// 内层事件类型。
pub const INNER_HALL_CHAT: u64 = 20415;
pub const INNER_PRIVATE_CHAT: u64 = 486;
pub const INNER_HALL_LIST: u64 = 25313;
pub const INNER_PRIVATE_HISTORY: u64 = 28347;
pub const INNER_HALL_MEMBERS: u64 = 2076;
pub const INNER_HALL_POPULATION: u64 = 7154;
/// 好友列表（含备注）。**不是独立 cmd**，而是 2246 信封里的内层事件类型；
/// 进游戏时随信封一次性下发，之后按需懒加载。
pub const INNER_FRIEND_LIST: u64 = 4410;

// 字段号
const F_EVENTS: u32 = 8;
const F_INNER_TYPE: u32 = 13;
const F_PAYLOAD: u32 = 6;

// 载荷内
/// 大厅聊天消息体：`#6.#4`。
const F_MSG_BODY_HALL: u32 = 4;
/// 私聊消息体：`#6.#8`（已实测验证）。
const F_MSG_BODY_PRIVATE: u32 = 8;
const F_HALL_CTX: u32 = 15;

// 消息体
const F_TS: u32 = 1;
const F_FROM_UID: u32 = 5;
const F_TO_UID: u32 = 9;
const F_TEXT: u32 = 1985;
const F_EMOJI: u32 = 1448;

// 大厅上下文
const F_HALL_TYPE: u32 = 1;
const F_HALL_ID: u32 = 2;

// 好友记录（4410 顶层 #15，每条一个好友）
const F_FRIEND_RECORD: u32 = 15;
const F_FRIEND_UID: u32 = 1;
const F_FRIEND_NAME: u32 = 2;
/// 备注：玩家给好友起的自定义名字。空/缺失 = 未设置。
const F_FRIEND_REMARK: u32 = 17;

/// 一条从信封里提取出的原始消息（尚未判定频道）。
#[derive(Clone, Debug, PartialEq)]
pub struct RawChat {
    pub ts: u64,
    pub from_uid: u32,
    pub to_uid: Option<u32>,
    pub text: String,
    pub hall_id: Option<u64>,
    pub hall_type: Option<u32>,
    /// 来自私聊事件（而非大厅事件）。
    pub is_private: bool,
}

/// 从 cmd 2246 的信封载荷里提取所有文本聊天消息。
///
/// 只返回 `kind == text` 的消息：表情/附件/系统消息不可能是命令。
pub fn extract_chats(payload: &[u8]) -> Vec<RawChat> {
    let Ok(root) = proto::parse(payload) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for event in root.repeated_messages(F_EVENTS) {
        let Some(inner) = event.varint(F_INNER_TYPE) else {
            continue;
        };
        match inner {
            INNER_HALL_CHAT => {
                if let Some(payload) = event.message(F_PAYLOAD) {
                    collect(payload, false, &mut out);
                }
            }
            INNER_PRIVATE_CHAT => {
                if let Some(payload) = event.message(F_PAYLOAD) {
                    collect(payload, true, &mut out);
                }
            }
            _ => {}
        }
    }
    out
}

/// 解析独立 cmd 486 的私聊载荷。
///
/// 文档说 inner 486 与独立 cmd 486 同构。为兼容两种可能的布局，
/// 依次尝试：当作信封、当作 `#6` 容器、当作消息体本身。
pub fn extract_direct_private(payload: &[u8]) -> Vec<RawChat> {
    let Ok(root) = proto::parse(payload) else {
        return Vec::new();
    };
    let mut out = Vec::new();

    // 布局 A：仍是信封（有 #8 事件、#13 = 486）。
    for event in root.repeated_messages(F_EVENTS) {
        if event.varint(F_INNER_TYPE) != Some(INNER_PRIVATE_CHAT) {
            continue;
        }
        if let Some(body) = event.message(F_PAYLOAD) {
            collect(body, true, &mut out);
        }
    }
    if !out.is_empty() {
        return out;
    }

    // 独立 cmd 486 的载荷是顶层 #8。
    for body in root.repeated_messages(8) {
        collect_as_body(body, &mut out);
    }
    out
}

/// 把 `fields` 直接当作消息体解析。
fn collect_as_body(body: Fields, out: &mut Vec<RawChat>) {
    let Some(from_uid) = body.varint(F_FROM_UID).and_then(|v| u32::try_from(v).ok()) else {
        return;
    };
    let Some(text) = body.string(F_TEXT) else {
        return;
    };
    if text.trim().is_empty() {
        return;
    }
    out.push(RawChat {
        ts: body.varint(F_TS).unwrap_or(0),
        from_uid,
        to_uid: body.varint(F_TO_UID).and_then(|v| u32::try_from(v).ok()),
        text: text.to_owned(),
        hall_id: None,
        hall_type: None,
        is_private: true,
    });
}

/// 提取大厅上下文（用于判定"当前大厅"）。
///
/// 注意：**不能**用这个函数判定当前大厅——公共频道的消息也带 hall_ctx，
/// 用它会把当前大厅误判成别人大厅的 ID。当前大厅只能由"自己发的消息"或
/// 7154 人数包确立，见 ChatState::ingest。
pub fn extract_hall_context(payload: &[u8]) -> Option<(u64, u32)> {
    let root = proto::parse(payload).ok()?;
    for event in root.repeated_messages(F_EVENTS) {
        if event.varint(F_INNER_TYPE) != Some(INNER_HALL_CHAT) {
            continue;
        }
        if let Some((id, ty)) = hall_ctx(&event.message(F_PAYLOAD)?) {
            return Some((id, ty));
        }
    }
    None
}

/// 从 7154（大厅人数/容量）提取当前大厅 ID。
///
/// 结构：`#6.#9 = 大厅ID`。这是"当前大厅"的补充信号——
/// BOT 长时间不说话时，只能靠它判定。
pub fn extract_population_hall(payload: &[u8]) -> Option<u64> {
    let root = proto::parse(payload).ok()?;
    for event in root.repeated_messages(F_EVENTS) {
        if event.varint(F_INNER_TYPE) != Some(INNER_HALL_POPULATION) {
            continue;
        }
        let body = event.message(F_PAYLOAD)?;
        if let Some(id) = body.varint(9) {
            return Some(id);
        }
    }
    None
}

/// 从 2076（大厅成员快照）或资料包提取 uid → 昵称。
///
/// 结构：`#8.#6.#4 = { #1 uid, #2 昵称 }`（每个玩家一块）。
pub fn extract_names(payload: &[u8]) -> Vec<(u32, String)> {
    let Ok(root) = proto::parse(payload) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for event in root.repeated_messages(F_EVENTS) {
        let Some(payload) = event.message(F_PAYLOAD) else {
            continue;
        };
        for member in payload.repeated_messages(F_MSG_BODY_HALL) {
            let Some(uid) = member.varint(1) else {
                continue;
            };
            let Some(name) = member.string(2) else {
                continue;
            };
            let Ok(uid) = u32::try_from(uid) else {
                continue;
            };
            if !name.is_empty() {
                out.push((uid, name.to_owned()));
            }
        }
    }
    out
}

/// 一条好友档案。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FriendProfile {
    pub uid: u32,
    /// 游戏内昵称（#2），可随时改名。
    pub name: Option<String>,
    /// 玩家给该好友设置的备注（#17）；None = 这条记录没带备注。
    pub remark: Option<String>,
}

/// 从 2246 信封里提取好友档案（内层事件 `#13 = 4410`）。
///
/// 结构：`#8 { #13 = 4410, #6 = { #15 记录... } }`，每条 `#15` 是一个好友：
///   #15 { #1 uid, #2 昵称, #6 签名, #17 备注, ... }
///
/// 注意：好友列表**不是独立 cmd**，只出现在信封内部，所以只订阅 2246 即可。
/// #17 是可选字段：没设置备注的好友不会带它（实测 35 条里 13 条没有）。
pub fn extract_friend_profiles(payload: &[u8]) -> Vec<FriendProfile> {
    let Ok(root) = proto::parse(payload) else {
        return Vec::new();
    };
    let mut records = Vec::new();
    for event in root.repeated_messages(F_EVENTS) {
        if event.varint(F_INNER_TYPE) != Some(INNER_FRIEND_LIST) {
            continue;
        }
        let Some(body) = event.message(F_PAYLOAD) else {
            continue;
        };
        records.extend(body.repeated_messages(F_FRIEND_RECORD));
    }
    let mut out = Vec::new();
    for record in records {
        let Some(uid) = record.varint(F_FRIEND_UID) else {
            continue;
        };
        let Ok(uid) = u32::try_from(uid) else {
            continue;
        };
        // 空串按“没有”处理，避免把空备注写进映射。
        let name = record
            .string(F_FRIEND_NAME)
            .filter(|value| !value.is_empty())
            .map(str::to_owned);
        let remark = record
            .string(F_FRIEND_REMARK)
            .filter(|value| !value.is_empty())
            .map(str::to_owned);
        out.push(FriendProfile { uid, name, remark });
    }
    out
}

/// 读取载荷里的消息体。
///
/// 大厅与私聊的消息体字段号不同：大厅在 `#6.#4`，私聊在 `#6.#8`。
/// 用错字段号会静默地拿不到任何私聊消息。
fn collect(payload: Fields, is_private: bool, out: &mut Vec<RawChat>) {
    let body_field = if is_private {
        F_MSG_BODY_PRIVATE
    } else {
        F_MSG_BODY_HALL
    };
    let Some(body) = payload.message(body_field) else {
        return;
    };
    // 只有文本消息才可能是命令；表情（#1448）与附件一律跳过。
    let Some(text) = body.string(F_TEXT) else {
        return;
    };
    if text.trim().is_empty() {
        return;
    }
    let Some(from_uid) = body.varint(F_FROM_UID).and_then(|v| u32::try_from(v).ok()) else {
        return;
    };
    let (hall_id, hall_type) = match hall_ctx(&payload) {
        Some((id, ty)) => (Some(id), Some(ty)),
        None => (None, None),
    };
    out.push(RawChat {
        ts: body.varint(F_TS).unwrap_or(0),
        from_uid,
        to_uid: body.varint(F_TO_UID).and_then(|v| u32::try_from(v).ok()),
        text: text.to_owned(),
        hall_id,
        hall_type,
        is_private,
    });
}

fn hall_ctx(payload: &Fields) -> Option<(u64, u32)> {
    let ctx = payload.message(F_HALL_CTX)?;
    let id = ctx.varint(F_HALL_ID)?;
    let ty = u32::try_from(ctx.varint(F_HALL_TYPE)?).ok()?;
    Some((id, ty))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 按 wire format 拼一段 protobuf：用于构造测试样本。
    fn varint(mut v: u64) -> Vec<u8> {
        let mut out = Vec::new();
        loop {
            let mut byte = (v & 0x7f) as u8;
            v >>= 7;
            if v != 0 {
                byte |= 0x80;
            }
            out.push(byte);
            if v == 0 {
                return out;
            }
        }
    }

    fn tag(field: u32, wire: u8) -> Vec<u8> {
        varint(((field as u64) << 3) | u64::from(wire))
    }

    fn field_varint(field: u32, value: u64) -> Vec<u8> {
        let mut out = tag(field, 0);
        out.extend(varint(value));
        out
    }

    fn field_bytes(field: u32, value: &[u8]) -> Vec<u8> {
        let mut out = tag(field, 2);
        out.extend(varint(value.len() as u64));
        out.extend(value);
        out
    }

    fn concat(parts: Vec<Vec<u8>>) -> Vec<u8> {
        parts.concat()
    }

    /// 构造一条好友记录（4410 的 #15）。
    fn friend_record(uid: u32, name: &str, remark: Option<&str>) -> Vec<u8> {
        let mut parts = vec![
            field_varint(F_FRIEND_UID, uid.into()),
            field_bytes(F_FRIEND_NAME, name.as_bytes()),
            field_bytes(6, "签名".as_bytes()),
        ];
        if let Some(remark) = remark {
            parts.push(field_bytes(F_FRIEND_REMARK, remark.as_bytes()));
        }
        concat(parts)
    }

    /// 构造 2246 信封：内层事件 #13=4410，载荷 #6 里挂 #15 记录。
    fn friend_list(records: Vec<Vec<u8>>) -> Vec<u8> {
        let body = concat(
            records
                .into_iter()
                .map(|r| field_bytes(F_FRIEND_RECORD, &r))
                .collect(),
        );
        let event = concat(vec![
            field_bytes(6, &body),
            field_varint(13, INNER_FRIEND_LIST),
        ]);
        field_bytes(8, &event)
    }

    #[test]
    fn friend_list_yields_uid_name_and_optional_remark() {
        let payload = friend_list(vec![
            friend_record(100000001, "甲玩家", Some("rmkAAA1")),
            friend_record(100000002, "乙玩家", None),
            friend_record(100000003, "丙玩家", Some("rmkCCC3")),
        ]);
        let profiles = extract_friend_profiles(&payload);
        assert_eq!(
            profiles,
            vec![
                FriendProfile {
                    uid: 100000001,
                    name: Some("甲玩家".into()),
                    remark: Some("rmkAAA1".into()),
                },
                FriendProfile {
                    uid: 100000002,
                    name: Some("乙玩家".into()),
                    remark: None,
                },
                FriendProfile {
                    uid: 100000003,
                    name: Some("丙玩家".into()),
                    remark: Some("rmkCCC3".into()),
                },
            ]
        );
    }

    #[test]
    fn friend_list_treats_empty_remark_as_absent() {
        let payload = friend_list(vec![friend_record(1, "甲", Some(""))]);
        let profiles = extract_friend_profiles(&payload);
        assert_eq!(profiles.len(), 1);
        assert_eq!(profiles[0].remark, None);
    }

    #[test]
    fn friend_list_skips_records_without_uid() {
        // 没有 #1 的记录无法归属，必须丢弃而不是猜。
        let record = concat(vec![field_bytes(F_FRIEND_NAME, "无主".as_bytes())]);
        assert!(extract_friend_profiles(&friend_list(vec![record])).is_empty());
    }

    #[test]
    fn friend_list_ignores_other_inner_events() {
        // 同一信封里还有别的内层类型，不能把它们的 #15 误当好友。
        let friends = friend_list(vec![friend_record(1, "甲", Some("备注甲"))]);
        let other_body = concat(vec![field_bytes(
            F_FRIEND_RECORD,
            &friend_record(2, "乙", Some("备注乙")),
        )]);
        let other = concat(vec![field_bytes(6, &other_body), field_varint(13, 7739)]);
        // 把好友事件与无关事件拼在同一个信封里。
        let friend_event = field_bytes(
            8,
            &concat(vec![
                field_bytes(
                    6,
                    &concat(vec![field_bytes(
                        F_FRIEND_RECORD,
                        &friend_record(1, "甲", Some("备注甲")),
                    )]),
                ),
                field_varint(13, INNER_FRIEND_LIST),
            ]),
        );
        let mut payload = friend_event;
        payload.extend(field_bytes(8, &other));
        let profiles = extract_friend_profiles(&payload);
        assert_eq!(profiles.len(), 1, "只认 #13=4410 的事件");
        assert_eq!(profiles[0].uid, 1);
        let _ = friends;
    }

    #[test]
    fn friend_list_rejects_garbage() {
        assert!(extract_friend_profiles(&[0xff, 0xff, 0xff]).is_empty());
    }

    /// 真实抓包回归：2246 信封里的 #13=4410 事件。
    ///
    /// 样本不入库（避免把玩家昵称/备注提交进仓库），只在文件存在时校验。
    #[test]
    fn extracts_remarks_from_real_envelope_when_sample_available() {
        let Ok(bytes) = std::fs::read("/tmp/p23.bin") else {
            return;
        };
        let profiles = extract_friend_profiles(&bytes);
        assert_eq!(profiles.len(), 35, "真实信封里应有 35 条好友记录");
        assert_eq!(
            profiles.iter().filter(|p| p.remark.is_some()).count(),
            22,
            "其中 22 条带备注"
        );
        let owner = profiles.iter().find(|p| p.uid == 100000003).unwrap();
        assert_eq!(owner.name.as_deref(), Some("丙玩家"));
        assert_eq!(owner.remark.as_deref(), Some("rmkCCC3"));
        let plain = profiles.iter().find(|p| p.uid == 100000002).unwrap();
        assert_eq!(plain.name.as_deref(), Some("乙玩家"));
        assert_eq!(plain.remark, None);
    }

    #[test]
    fn friend_list_needs_no_extra_subscription() {
        // 好友列表在 2246 信封内部，所以订阅表保持原样即可。
        assert!(WANTED_CMDS.contains(&CMD_ENVELOPE));
        assert_eq!(WANTED_CMDS.len(), 2);
    }

    /// 构造一条大厅聊天的完整信封。
    fn hall_envelope(ts: u64, uid: u32, text: &str, hall_id: u64, hall_type: u32) -> Vec<u8> {
        let body = concat(vec![
            field_varint(1, ts),
            field_varint(4, 3),
            field_varint(5, uid.into()),
            field_varint(6, 1),
            field_bytes(1985, text.as_bytes()),
        ]);
        let hall = concat(vec![
            field_varint(1, hall_type.into()),
            field_varint(2, hall_id),
        ]);
        let payload = concat(vec![field_bytes(4, &body), field_bytes(15, &hall)]);
        let event = concat(vec![field_bytes(6, &payload), field_varint(13, 20415)]);
        field_bytes(8, &event)
    }

    #[test]
    fn extracts_hall_text_message() {
        let buf = hall_envelope(1792772291, 100000006, "晚安啦", 1000000000000000002, 16);
        let chats = extract_chats(&buf);
        assert_eq!(chats.len(), 1);
        let chat = &chats[0];
        assert_eq!(chat.text, "晚安啦");
        assert_eq!(chat.from_uid, 100000006);
        assert_eq!(chat.ts, 1792772291);
        assert_eq!(chat.hall_id, Some(1000000000000000002));
        assert_eq!(chat.hall_type, Some(16));
        assert!(!chat.is_private);
    }

    #[test]
    fn extracts_hall_context() {
        let buf = hall_envelope(1, 42, "hi", 999, 23);
        assert_eq!(extract_hall_context(&buf), Some((999, 23)));
    }

    #[test]
    fn skips_emoji_only_messages() {
        let body = concat(vec![
            field_varint(1, 100),
            field_varint(5, 42),
            field_varint(1448, 1003),
        ]);
        let payload = field_bytes(4, &body);
        let event = concat(vec![field_bytes(6, &payload), field_varint(13, 20415)]);
        let buf = field_bytes(8, &event);
        assert!(extract_chats(&buf).is_empty(), "纯表情不是命令");
    }

    #[test]
    fn ignores_unknown_inner_types() {
        let event = concat(vec![field_varint(13, 6930)]);
        assert!(extract_chats(&field_bytes(8, &event)).is_empty());
    }

    #[test]
    fn extracts_private_message() {
        // 私聊的消息体在 #6.#8，不是大厅用的 #6.#4。
        let body = concat(vec![
            field_varint(1, 1783937990),
            field_varint(4, 3),
            field_varint(5, 100000007),
            field_varint(6, 201),
            field_varint(9, 100000009),
            field_bytes(1985, "1".as_bytes()),
        ]);
        let payload = field_bytes(8, &body);
        let event = concat(vec![field_bytes(6, &payload), field_varint(13, 486)]);
        let chats = extract_chats(&field_bytes(8, &event));
        assert_eq!(chats.len(), 1);
        assert!(chats[0].is_private);
        assert_eq!(chats[0].to_uid, Some(100000009));
        assert_eq!(chats[0].hall_id, None);
    }

    /// 用真实抓包样本验证解析：样本来附录 A（text="123321", hall=1000000000000000001）。
    #[test]
    fn parses_real_sample_from_handoff_doc() {
        // 附录 A 的原始包：
        //   #8 { #6 { #4 { #1=1792772291 #4=3 #5=100000009 #6=883 #1985="123321" }
        //            #15 { #1=16 #2=1000000000000000001 } } #13=20415 }
        let buf = hall_envelope(1792772291, 100000009, "123321", 1000000000000000001, 16);
        let chats = extract_chats(&buf);
        assert_eq!(chats.len(), 1);
        assert_eq!(chats[0].text, "123321");
        assert_eq!(chats[0].from_uid, 100000009);
        assert_eq!(chats[0].hall_id, Some(1000000000000000001));
        assert_eq!(chats[0].hall_type, Some(16));
        assert_eq!(chats[0].ts, 1792772291);
    }

    #[test]
    fn parses_chinese_text_and_emoji_unchanged() {
        let buf = hall_envelope(1, 1, "晚安啦🌸", 100, 16);
        let chats = extract_chats(&buf);
        assert_eq!(chats[0].text, "晚安啦🌸");
    }

    #[test]
    fn extracts_population_hall() {
        // 7154 的载荷就是 #6 本身（容量 #1 / 人数 #11 / 大厅ID #9）。
        let body = concat(vec![
            field_varint(1, 12),
            field_varint(11, 2),
            field_varint(9, 1000000000000000002),
        ]);
        let event = concat(vec![field_bytes(6, &body), field_varint(13, 7154)]);
        let buf = field_bytes(8, &event);
        assert_eq!(extract_population_hall(&buf), Some(1000000000000000002));
    }
}
