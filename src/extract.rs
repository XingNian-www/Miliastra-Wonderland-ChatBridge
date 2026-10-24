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
            let Some(uid) = member.varint(1) else { continue };
            let Some(name) = member.string(2) else { continue };
            let Ok(uid) = u32::try_from(uid) else { continue };
            if !name.is_empty() {
                out.push((uid, name.to_owned()));
            }
        }
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
        let event = concat(vec![
            field_bytes(6, &payload),
            field_varint(13, 20415),
        ]);
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