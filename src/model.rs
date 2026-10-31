//! 对外 API 的数据模型。
//!
//! 只保留"必要且稳定"的字段：能被命令消费的文本消息。
//! 明确排除：表情、附件、系统消息、跨厅公共频道、大厅容量/人数。

use serde::Serialize;

/// 频道类型。
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Channel {
    /// 当前所在大厅。
    CurrentHall,
    /// 好友私聊。
    Private,
}

impl Channel {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::CurrentHall => "current_hall",
            Self::Private => "private",
        }
    }
}

/// 发送者。
#[derive(Clone, Debug, Serialize)]
pub struct Sender {
    /// 原神账号 UID。
    pub uid: u32,
    /// 昵称。资料包可能尚未到达，此时为 None。
    pub name: Option<String>,
}

/// 一条聊天消息。
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChatMessage {
    /// 进程内自增序号，从 1 开始。重启归零，不跨进程唯一。
    pub id: u64,
    /// Unix 秒。同秒多条会重复，不能当排序键。
    pub ts: u64,
    pub channel: Channel,
    /// 大厅 ID；私聊为 None。
    pub hall_id: Option<u64>,
    /// 大厅类型（实测 16/23）；私聊为 None。
    pub hall_type: Option<u32>,
    pub from: Sender,
    /// 接收者 UID；大厅广播为 None。
    pub to: Option<u32>,
    pub text: String,
}

/// 服务端推送的 WS 帧。
#[derive(Clone, Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerEvent {
    Hello {
        version: &'static str,
        #[serde(rename = "lastMessageId")]
        last_message_id: u64,
    },
    Message {
        message: ChatMessage,
    },
    #[serde(rename_all = "camelCase")]
    HallChanged {
        hall_id: u64,
        hall_type: u32,
    },
}
