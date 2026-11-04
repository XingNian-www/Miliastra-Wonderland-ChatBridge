//! 从信封里提取聊天消息。
//!
//! 信封的 cmdId 与内部字段号**每次大版本更新都可能变**，所以两者都不写死：
//!
//! - cmdId：`CMD_ENVELOPE` 只是默认值，运行时用 `--cmd` 覆盖（见 `wanted_cmds`）。
//! - 字段号：集中成 [`Layout`] 数据，`LAYOUTS` 里按新到旧排列，逐套尝试、命中即用。
//!   下次更新**只需要在 `LAYOUTS` 前面加一条**，其余代码不用动。
//!
//! 已知布局：
//!
//! ```text
//! 7.1（cmd 2246）                 7.2（cmd 24007）
//!   #8   事件（可重复）             #12  事件（可重复）
//!   #8.#13 内层类型                #12.#2  内层类型
//!   #8.#6  载荷                    #12.#14 载荷
//!   #8.#6.#4  大厅消息体            #14.#5  消息体（大厅与私聊同号）
//!   #8.#6.#8  私聊消息体
//!   #8.#6.#15 大厅上下文            #14.#4  大厅上下文
//! 消息体字段：ts=#1 from=#5 to=#9   消息体字段：ts=#2 from=#1 to=#6
//!             text=#1985                       text=#630
//! 内层类型：大厅 29636 私聊 28274（会话快照 25574）
//! ```
//!
//! 回归锚点：`tests` 里的 `extract_72_*` 用真机抓到的包做字节级样本，
//! 版本再变时照此新增一组 `extract_7x_*`，就能立刻定位是哪一层坏了。

use crate::game_config::LayoutSpec;
use crate::proto::{self, Fields};

/// 多路复用信封 cmd。
///
/// 只维护当前线上版本：旧的 2246 已不再订阅。
/// 运行时可用 --cmd 覆盖，见 wanted_cmds。
pub const CMD_ENVELOPE: u16 = 24007;

/// 需要订阅的所有 cmd（编译期默认值）。
///
/// 7.2 的私聊已经并进信封（内层 28274），不再有独立的 486 直连通道
/// ——实测 167 个全包抓取里 486 出现 0 次。游戏更新后这些编号会变，
/// 运行时可用 --cmd 覆盖或用 game-config.json 的 envelope 覆盖。
pub const WANTED_CMDS: &[u16] = &[CMD_ENVELOPE];

/// 运行时订阅表：覆盖值为空时回落到编译期默认。
///
/// 覆盖值来自 --cmd 24007 或 game-config.json 的 envelope。
/// 做成运行时可变的原因：游戏每次大版本更新都可能改 cmdId，
/// 而重新编译宿主 + 换 metadata 的成本远高于改一行参数。
pub fn wanted_cmds(override_list: &[u16]) -> Vec<u16> {
    if override_list.is_empty() {
        WANTED_CMDS.to_vec()
    } else {
        override_list.to_vec()
    }
}

/// 运行期布局表：启动时由 game_config 安装，未安装时用内置默认。
///
/// 做成运行期而不是编译期常量，是为了让「游戏更新只改 JSON」真正成立：
/// 字段号来自配置文件，改完重启即可生效，不需要重新编译。
static ACTIVE_LAYOUTS: std::sync::OnceLock<Vec<LayoutSpec>> = std::sync::OnceLock::new();

/// 安装启动时解析出的布局表。重复调用被忽略（进程内只装一次）。
pub fn install_layouts(layouts: Vec<LayoutSpec>) {
    if layouts.is_empty() {
        return;
    }
    let _ = ACTIVE_LAYOUTS.set(layouts);
}

/// 当前生效的布局表（按新到旧排列）。
fn layouts() -> &'static [LayoutSpec] {
    ACTIVE_LAYOUTS.get_or_init(|| crate::game_config::default_config().layouts)
}

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

/// 从信封载荷里提取所有文本聊天消息。
///
/// 逐套布局尝试：7.2 与 7.1 的字段号完全不同，命中哪套就用哪套。
/// 只返回文本消息：表情/附件/系统消息不可能是命令。
pub fn extract_chats(payload: &[u8]) -> Vec<RawChat> {
    let Ok(root) = proto::parse(payload) else {
        return Vec::new();
    };
    for layout in layouts() {
        let mut out = Vec::new();
        for event in root.repeated_messages(layout.events) {
            let Some(inner) = event.varint(layout.inner_type) else {
                continue;
            };
            let is_private = if Some(inner) == layout.inner_hall_chat {
                false
            } else if Some(inner) == layout.inner_private_chat {
                true
            } else {
                continue;
            };
            if let Some(payload_fields) = event.message(layout.payload) {
                collect(&payload_fields, layout, is_private, &mut out);
            }
        }
        if !out.is_empty() {
            return out;
        }
    }
    Vec::new()
}

/// 提取大厅上下文（用于判定"当前大厅"）。
///
/// 注意：**不能**用这个函数判定当前大厅——公共频道的消息也带 hall_ctx，
/// 用它会把当前大厅误判成别人大厅的 ID。当前大厅只能由"自己发的消息"
/// 或人数包确立，见 ChatState::ingest。
pub fn extract_hall_context(payload: &[u8]) -> Option<(u64, u32)> {
    let root = proto::parse(payload).ok()?;
    for layout in layouts() {
        for event in root.repeated_messages(layout.events) {
            if event.varint(layout.inner_type) != layout.inner_hall_chat {
                continue;
            }
            if let Some(body) = event.message(layout.payload)
                && let Some(ctx) = hall_ctx(&body, layout)
            {
                return Some(ctx);
            }
        }
    }
    None
}

/// 从大厅人数/容量包提取当前大厅 ID（结构：载荷里的大厅上下文）。
///
/// BOT 长时间不说话时，只能靠它判定当前大厅。
pub fn extract_population_hall(payload: &[u8]) -> Option<u64> {
    let root = proto::parse(payload).ok()?;
    for layout in layouts() {
        let Some(wanted) = layout.inner_hall_population else {
            continue;
        };
        for event in root.repeated_messages(layout.events) {
            if event.varint(layout.inner_type) != Some(wanted) {
                continue;
            }
            let Some(body) = event.message(layout.payload) else {
                continue;
            };
            if let Some(id) = body.varint(layout.population_hall) {
                return Some(id);
            }
        }
    }
    None
}

/// 从成员快照或资料包提取 uid -> 昵称。
pub fn extract_names(payload: &[u8]) -> Vec<(u32, String)> {
    let Ok(root) = proto::parse(payload) else {
        return Vec::new();
    };
    for layout in layouts() {
        // 7.2 起 uid -> 昵称的唯一可靠来源是好友列表（事件 24893，#8 重复，#1 uid，#4 昵称）。
        if let Some(wanted) = layout.inner_friend_list
            && let Some(names) = names_from_friend_list(&root, layout, wanted)
            && !names.is_empty()
        {
            return names;
        }

        // 大厅成员列表（7.1 = 2076）：载荷里的重复记录是 { #1 uid, #2 昵称 }。
        let Some(wanted) = layout.inner_hall_members else {
            continue;
        };
        let mut out = Vec::new();
        for event in root.repeated_messages(layout.events) {
            if event.varint(layout.inner_type) != Some(wanted) {
                continue;
            }
            let Some(body) = event.message(layout.payload) else {
                continue;
            };
            for member in body.repeated_messages(layout.member_list) {
                let Some(uid) = member.varint(layout.member_uid) else {
                    continue;
                };
                let Some(name) = member.string(layout.member_name) else {
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
        if !out.is_empty() {
            return out;
        }
    }
    Vec::new()
}

/// 从好友列表事件里抽 uid -> 昵称。事件不存在时返回 None，让调用方回退。
fn names_from_friend_list(
    root: &Fields,
    layout: &LayoutSpec,
    wanted: u64,
) -> Option<Vec<(u32, String)>> {
    let mut out = Vec::new();
    for event in root.repeated_messages(layout.events) {
        if event.varint(layout.inner_type) != Some(wanted) {
            continue;
        }
        let Some(body) = event.message(layout.payload) else {
            continue;
        };
        for record in body.repeated_messages(layout.record_list) {
            let (Some(uid), Some(name)) = (
                record.varint(layout.record_uid),
                record.string(layout.record_name),
            ) else {
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
    Some(out)
}

/// 一条好友档案。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FriendProfile {
    pub uid: u32,
    /// 游戏内昵称（随时可改）。
    pub name: Option<String>,
    /// 玩家给该好友设置的备注；None = 这条记录没带备注。
    pub remark: Option<String>,
}

/// 从信封里提取好友档案（含备注）。
pub fn extract_friend_profiles(payload: &[u8]) -> Vec<FriendProfile> {
    let Ok(root) = proto::parse(payload) else {
        return Vec::new();
    };
    for layout in layouts() {
        let Some(wanted) = layout.inner_friend_list else {
            continue;
        };
        let mut records = Vec::new();
        for event in root.repeated_messages(layout.events) {
            if event.varint(layout.inner_type) != Some(wanted) {
                continue;
            }
            let Some(body) = event.message(layout.payload) else {
                continue;
            };
            records.extend(body.repeated_messages(layout.record_list));
        }
        if records.is_empty() {
            continue;
        }
        let mut out = Vec::new();
        for record in records {
            let Some(uid) = record.varint(layout.record_uid) else {
                continue;
            };
            let Ok(uid) = u32::try_from(uid) else {
                continue;
            };
            let name = record
                .string(layout.record_name)
                .filter(|value| !value.is_empty())
                .map(str::to_owned);
            let remark = record
                .string(layout.record_remark)
                .filter(|value| !value.is_empty())
                .map(str::to_owned);
            out.push(FriendProfile { uid, name, remark });
        }
        if !out.is_empty() {
            return out;
        }
    }
    Vec::new()
}

/// 读取载荷里的消息体。
///
/// 7.1 大厅在 `载荷#4`、私聊在 `载荷#8`；7.2 两者都是 `载荷#5`。
/// 7.2 的私聊载荷里 `#5` 是**重复**的（一包多条），所以必须遍历。
fn collect(payload: &Fields, layout: &LayoutSpec, is_private: bool, out: &mut Vec<RawChat>) {
    let body_field = if is_private {
        layout.body_private
    } else {
        layout.body_hall
    };
    // 只有大厅消息才有大厅上下文。私聊事件的载荷字段号可能和大厅上下文重合
    // （7.2 的实时私聊 #4 就是消息体本身），不加这道判断会把「发送者/时间戳」
    // 误读成 hall_type/hall_id。
    let (hall_id, hall_type) = if is_private {
        (None, None)
    } else {
        match hall_ctx(payload, layout) {
            Some((id, ty)) => (Some(id), Some(ty)),
            None => (None, None),
        }
    };
    for body in payload.repeated_messages(body_field) {
        let Some(text) = body.string(layout.text) else {
            continue;
        };
        if text.trim().is_empty() {
            continue;
        }
        let Some(from_uid) = body
            .varint(layout.from_uid)
            .and_then(|v| u32::try_from(v).ok())
        else {
            continue;
        };
        out.push(RawChat {
            ts: body.varint(layout.ts).unwrap_or(0),
            from_uid,
            to_uid: body
                .varint(layout.to_uid)
                .and_then(|v| u32::try_from(v).ok()),
            text: text.to_owned(),
            hall_id,
            hall_type,
            is_private,
        });
    }
}

fn hall_ctx(payload: &Fields, layout: &LayoutSpec) -> Option<(u64, u32)> {
    let ctx = payload.message(layout.hall_ctx)?;
    let id = ctx.varint(layout.hall_id)?;
    let ty = u32::try_from(ctx.varint(layout.hall_type)?).ok()?;
    Some((id, ty))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 7.2.0 真机抓到的**大厅聊天**包（24007 信封、内层 29636）。
    ///
    /// 这组常量是跨版本回归的锚点：7.2 把信封字段号整体换掉了（#8->#12、
    /// #13->#2、#6->#14、#1985->#630），只跑 7.1 样本的测试抓不到这种破坏。
    const HALL_72: &str = "622f10c4e7017229220c0810108180c89d9deb96f8062a190881c2d72f10a1e0aad7065803b2270948414c4c4d41524b31";

    /// 7.2 **私聊**包夹具（内层 25574，一包三条消息，第三条无文本）。
    ///
    /// 真机包是 876 字节，日志里只能拿到前 256 字节（被截断，严格解析会拒绝），
    /// 所以这里按**实测确认的字段映射**构造一份完整夹具：结构同上，尺寸可控。
    /// 7.2 **实时私聊**包（按真机结构构造，数值已脱敏）：事件 #2=28274，消息体在载荷 #4。
    ///
    /// 这条锚点很关键：它是「单条消息」的形态，与 25574 那种一次 20 条的会话快照完全不同。
    const PRIV_72: &str =
        "622910f2dc01722322210881c2d72f10ec84abd7063082c2d72f580378c901b22709505249564d41524b31";

    fn varint_for_test(mut value: u64) -> Vec<u8> {
        let mut out = Vec::new();
        loop {
            let byte = (value & 0x7f) as u8;
            value >>= 7;
            if value == 0 {
                out.push(byte);
                return out;
            }
            out.push(byte | 0x80);
        }
    }

    fn fv_for_test(field: u32, value: u64) -> Vec<u8> {
        let mut out = varint_for_test(u64::from(field) << 3);
        out.extend(varint_for_test(value));
        out
    }

    fn fb_for_test(field: u32, body: &[u8]) -> Vec<u8> {
        let mut out = varint_for_test((u64::from(field) << 3) | 2);
        out.extend(varint_for_test(body.len() as u64));
        out.extend_from_slice(body);
        out
    }

    fn concat_for_test(parts: &[Vec<u8>]) -> Vec<u8> {
        parts.iter().flatten().copied().collect()
    }

    fn hex_bytes(hex: &str) -> Vec<u8> {
        (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).expect("合法 hex"))
            .collect()
    }

    /// 7.2 的好友列表（事件 29281）：记录在 #10，uid 在 #1、昵称在 #2、签名在 #6、备注在 #17。
    ///
    /// 昵称和备注都不进聊天包，只在这里下发；备注只有设置过的好友才有。
    const FRIENDS_72: &str = "625010e1e401724a52290882c2d72f183c1209e78ea9e5aeb6e794b23209e7adbee5908de794b28a0109e5a487e6b3a8e794b2521d0883c2d72f183c1209e78ea9e5aeb6e4b9993209e7adbee5908de4b999";

    #[test]
    fn extract_72_friend_names() {
        let names = extract_names(&hex_bytes(FRIENDS_72));
        assert_eq!(names.len(), 2, "两条好友记录都应取到昵称");
        assert_eq!(names[0], (100_000_002, "玩家甲".to_owned()));
        assert_eq!(names[1], (100_000_003, "玩家乙".to_owned()));
    }

    #[test]
    fn extract_72_friend_remarks() {
        // 备注在 #17，只有设置过的好友才有；没设置就应当是 None 而不是空串。
        let profiles = extract_friend_profiles(&hex_bytes(FRIENDS_72));
        assert_eq!(profiles.len(), 2);
        assert_eq!(profiles[0].uid, 100_000_002);
        assert_eq!(profiles[0].name.as_deref(), Some("玩家甲"));
        assert_eq!(profiles[0].remark.as_deref(), Some("备注甲"));
        assert_eq!(profiles[1].uid, 100_000_003);
        assert_eq!(profiles[1].remark, None, "没设备注的好友不应产出空备注");
    }

    /// 7.2 的大厅成员列表（事件 29039）：记录在 #13，uid 在 #9、昵称在 #3。
    ///
    /// 这条通路补的是「不是好友、但同在大厅」的玩家昵称。
    const MEMBERS_72: &str = "623010efe201722a6a134884c2d72f1a09e68890e59198e794b26884166a134885c2d72f1a09e68890e59198e4b999688416";

    #[test]
    fn extract_72_member_names() {
        let names = extract_names(&hex_bytes(MEMBERS_72));
        assert_eq!(names.len(), 2, "成员列表应给出 uid -> 昵称");
        assert_eq!(names[0], (100_000_004, "成员甲".to_owned()));
        assert_eq!(names[1], (100_000_005, "成员乙".to_owned()));
    }

    #[test]
    fn extract_72_friend_names_ignores_other_events() {
        // 好友列表事件缺失时不能把聊天包误当成员表（昵称会变成时间戳）。
        let names = extract_names(&hex_bytes(HALL_72));
        assert!(names.is_empty(), "聊天包不应产出昵称: {names:?}");
    }

    #[test]
    fn extract_72_hall_chat() {
        let chats = extract_chats(&hex_bytes(HALL_72));
        assert_eq!(chats.len(), 1, "大厅包只应产出一条带文本的消息");
        let chat = &chats[0];
        assert_eq!(chat.text, "HALLMARK1");
        assert_eq!(chat.from_uid, 100_000_001);
        assert_eq!(chat.ts, 1_793_765_409);
        assert!(!chat.is_private);
        assert_eq!(chat.hall_id, Some(500_000_000_000_000_001));
        assert_eq!(chat.hall_type, Some(16));
    }

    #[test]
    fn extract_72_private_chat() {
        let chats = extract_chats(&hex_bytes(PRIV_72));
        assert_eq!(chats.len(), 1, "实时私聊是单条消息");
        assert_eq!(chats[0].from_uid, 100_000_001);
        assert_eq!(chats[0].to_uid, Some(100_000_002));
        assert_eq!(chats[0].text, "PRIVMARK1");
        assert_eq!(chats[0].ts, 1_793_770_092);
        assert!(chats[0].is_private);
        assert!(chats[0].hall_id.is_none(), "私聊包没有大厅上下文");
    }

    #[test]
    fn extract_72_conversation_snapshot_is_not_live_chat() {
        // 25574 是「整段会话快照」（一次 20 条、每 60 秒一轮）。
        // 它必须**不**被当成实时私聊，否则会既延迟又有重复。
        let snapshot = fb_for_test(
            12,
            &concat_for_test(&[
                fv_for_test(2, 25574),
                fb_for_test(
                    14,
                    &concat_for_test(&[fb_for_test(
                        5,
                        &concat_for_test(&[
                            fv_for_test(1, 100_000_002),
                            fv_for_test(2, 1_793_770_100),
                            fv_for_test(6, 100_000_001),
                            fb_for_test(630, "旧消息".as_bytes()),
                        ]),
                    )]),
                ),
            ]),
        );
        assert!(
            extract_chats(&snapshot).is_empty(),
            "会话快照不应产生实时消息"
        );
    }

    #[test]
    fn extract_72_hall_context() {
        assert_eq!(
            extract_hall_context(&hex_bytes(HALL_72)),
            Some((500_000_000_000_000_001, 16))
        );
    }

    /// 7.2 的心跳包不应被误判成聊天。
    /// 7.2 的大厅 id 补充包（事件 7470）：载荷 { #5 大厅id, #14 时间戳 }。
    const HALL_ID_72: &str = "621510ae3a7210288180c89d9deb96f80670dacbabd706";

    #[test]
    fn extract_72_population_hall_id() {
        // 没有这条信号时，进大厅但还没人说话的话就是不知道当前在哪个大厅。
        assert_eq!(
            extract_population_hall(&hex_bytes(HALL_ID_72)),
            Some(500_000_000_000_000_001)
        );
    }

    #[test]
    fn extract_72_heartbeat_yields_nothing() {
        let hb = hex_bytes(
            "6240109f2d723b20912972363007423008a9808040121e0a0f0dfc0ba04315b5b966431dca6bd543120515b9fbb4421a002002320018abcd970320acea0728017802",
        );
        assert!(extract_chats(&hb).is_empty());
    }

    /// 按 wire format 拼一段 protobuf：用于构造测试样本。
    // ---- 7.2 负向用例（只维护当前版本）--------------------------------

    #[test]
    fn extract_72_ignores_garbage() {
        // 长度字段错乱/半包不该 panic，也不该吐出假消息。
        let junk = [0xffu8, 0xff, 0xff, 0xff];
        assert!(extract_chats(&junk).is_empty());
        assert!(extract_names(&junk).is_empty());
        assert!(extract_friend_profiles(&junk).is_empty());
        assert!(extract_hall_context(&junk).is_none());
        assert!(extract_population_hall(&junk).is_none());
    }

    #[test]
    fn extract_72_skips_message_without_text() {
        // 只有表情/系统事件没有 #630，不该产出空文本消息。
        let packet = fb_for_test(
            12,
            &concat_for_test(&[
                fv_for_test(2, 28274),
                fb_for_test(
                    14,
                    &concat_for_test(&[fb_for_test(
                        4,
                        &concat_for_test(&[
                            fv_for_test(1, 100_000_001),
                            fv_for_test(2, 1_793_770_092),
                            fv_for_test(6, 100_000_002),
                        ]),
                    )]),
                ),
            ]),
        );
        assert!(extract_chats(&packet).is_empty());
    }

    #[test]
    fn extract_72_empty_remark_is_absent() {
        // 备注字段存在但为空 = 没设备注，应当报 None 而不是空串。
        let packet = fb_for_test(
            12,
            &concat_for_test(&[
                fv_for_test(2, 29281),
                fb_for_test(
                    14,
                    &fb_for_test(
                        10,
                        &concat_for_test(&[
                            fv_for_test(1, 100_000_002),
                            fb_for_test(2, "玩家甲".as_bytes()),
                            fb_for_test(17, b""),
                        ]),
                    ),
                ),
            ]),
        );
        let profiles = extract_friend_profiles(&packet);
        assert_eq!(profiles.len(), 1);
        assert_eq!(profiles[0].remark, None);
    }

    #[test]
    fn extract_72_friend_record_without_uid_is_skipped() {
        let packet = fb_for_test(
            12,
            &concat_for_test(&[
                fv_for_test(2, 29281),
                fb_for_test(14, &fb_for_test(10, &fb_for_test(2, "玩家甲".as_bytes()))),
            ]),
        );
        assert!(
            extract_names(&packet).is_empty(),
            "没有 uid 的记录不能当昵称"
        );
        assert!(extract_friend_profiles(&packet).is_empty());
    }
}
