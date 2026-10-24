//! 聊天状态：当前大厅判定、昵称缓存、消息缓冲与广播。
//!
//! 公共频道（hall_type=23）先过滤，包含 BOT 自己的发言。
//! 其余大厅消息按消息中的 hall_id / hall_type 跟随当前大厅；
//! 自己的发言只更新大厅状态，不作为命令推送。
//! 7154 人数包提供补充大厅信号，不用于过滤实时大厅消息。

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

use tokio::sync::{RwLock, broadcast};

use crate::extract::RawChat;
use crate::model::{Channel, ChatMessage, Sender, ServerEvent};

/// 消息缓冲容量。
pub const MESSAGE_BUFFER: usize = 2000;
/// WS 广播通道容量。
pub const BROADCAST_CAPACITY: usize = 256;

/// 公共频道的 hall_type。
///
/// 实测：跨厅公共频道固定为 23，私人大厅为 16。用它可以在还不知道
/// "我在哪个大厅"时就先排掉公共频道，减少噪音与误判。
pub const PUBLIC_CHANNEL_HALL_TYPE: u32 = 23;

/// 全局聊天状态。
pub struct ChatState {
    inner: RwLock<Inner>,
    tx: broadcast::Sender<ServerEvent>,
    /// BOT 自己的 UID（配置注入）。用于识别"自己发的消息"。
    self_uid: u32,
}

struct Inner {
    next_id: u64,
    current_hall: Option<(u64, u32)>,
    messages: VecDeque<ChatMessage>,
    names: BTreeMap<u32, String>,
    /// 已推送的最大 id。
    last_id: u64,
}

impl ChatState {
    pub fn new(self_uid: u32) -> Arc<Self> {
        let (tx, _) = broadcast::channel(BROADCAST_CAPACITY);
        Arc::new(Self {
            inner: RwLock::new(Inner {
                next_id: 1,
                current_hall: None,
                messages: VecDeque::new(),
                names: BTreeMap::new(),
                last_id: 0,
            }),
            tx,
            self_uid,
        })
    }

    pub fn subscribe(&self) -> broadcast::Receiver<ServerEvent> {
        self.tx.subscribe()
    }

    /// 在同一锁边界订阅并读取 hello 水位，之后的事件不会漏收。
    pub async fn subscribe_with_last_id(&self) -> (broadcast::Receiver<ServerEvent>, u64) {
        let inner = self.inner.read().await;
        (self.tx.subscribe(), inner.last_id)
    }

    pub async fn last_id(&self) -> u64 {
        self.inner.read().await.last_id
    }

    pub async fn current_hall(&self) -> Option<(u64, u32)> {
        self.inner.read().await.current_hall
    }

    pub async fn subscriber_count(&self) -> usize {
        self.tx.receiver_count()
    }

    /// 记录昵称（资料包 / 成员快照）。
    pub async fn record_names(&self, pairs: Vec<(u32, String)>) {
        if pairs.is_empty() {
            return;
        }
        let mut inner = self.inner.write().await;
        for (uid, name) in pairs {
            inner.names.insert(uid, name);
        }
    }

    /// 批量查昵称。
    pub async fn names(&self, uids: &[u32]) -> BTreeMap<u32, Option<String>> {
        let inner = self.inner.read().await;
        uids.iter()
            .map(|uid| (*uid, inner.names.get(uid).cloned()))
            .collect()
    }

    /// 按 id 之后的消息（补拉）。
    pub async fn messages_since(&self, since: u64, limit: usize) -> (Vec<ChatMessage>, u64, bool) {
        let inner = self.inner.read().await;
        let oldest = inner.messages.front().map(|m| m.id).unwrap_or(0);
        // 请求区间早于缓冲起点，说明中间有遗漏。
        let truncated = since.saturating_add(1) < oldest;
        let messages: Vec<_> = inner
            .messages
            .iter()
            .filter(|m| m.id > since)
            .take(limit)
            .cloned()
            .collect();
        (messages, inner.last_id, truncated)
    }

    /// 处理一批原始消息。
    pub async fn ingest(&self, raws: Vec<RawChat>) {
        if raws.is_empty() {
            return;
        }
        let mut inner = self.inner.write().await;
        let mut events = Vec::new();
        for raw in raws {
            // 包括自己发言在内，公共频道都不能影响当前大厅。
            if !raw.is_private && raw.hall_type == Some(PUBLIC_CHANNEL_HALL_TYPE) {
                tracing::info!(
                    "[丢弃-公共频道] hall={:?} from={} ts={} text={:?}",
                    raw.hall_id,
                    raw.from_uid,
                    raw.ts,
                    raw.text
                );
                continue;
            }
            // 自己发的消息：只用于更新"当前大厅"，不作为命令推送给 BOT。
            let from_self = raw.from_uid == self.self_uid;
            if !raw.is_private
                && from_self
                && let (Some(hall_id), Some(hall_type)) = (raw.hall_id, raw.hall_type)
                && inner.current_hall != Some((hall_id, hall_type))
            {
                inner.current_hall = Some((hall_id, hall_type));
                events.push(ServerEvent::HallChanged { hall_id, hall_type });
            }
            if from_self {
                tracing::debug!("跳过自己发的消息: {:?}", raw.text);
                continue;
            }
            let channel = if raw.is_private {
                Channel::Private
            } else {
                // 剩下的大厅消息就是当前所在大厅的。
                //
                // 不能拿 7154 的 hall_id 做过滤条件：切换大厅后 7154 不会立刻重发，
                // 会把新大厅的消息全部误丢。消息流本身才是实时的权威来源，
                // 收到哪个大厅的消息就跟随到哪个大厅。
                if let (Some(hall_id), Some(hall_type)) = (raw.hall_id, raw.hall_type)
                    && inner.current_hall != Some((hall_id, hall_type))
                {
                    if inner.current_hall.is_some() {
                        tracing::info!("跟随消息切换当前大厅: {hall_id} type={hall_type}");
                    }
                    inner.current_hall = Some((hall_id, hall_type));
                    events.push(ServerEvent::HallChanged { hall_id, hall_type });
                }
                Channel::CurrentHall
            };
            tracing::info!(
                "推送消息: channel={:?} hall={:?} from={} text={:?}",
                channel,
                raw.hall_id,
                raw.from_uid,
                raw.text
            );
            let id = inner.next_id;
            inner.next_id += 1;
            let name = inner.names.get(&raw.from_uid).cloned();
            let message = ChatMessage {
                id,
                ts: raw.ts,
                channel,
                hall_id: raw.hall_id,
                hall_type: raw.hall_type,
                from: Sender {
                    uid: raw.from_uid,
                    name,
                },
                to: raw.to_uid,
                text: raw.text,
            };
            inner.last_id = id;
            // 缓冲写满后丢弃最旧的。
            if inner.messages.len() >= MESSAGE_BUFFER {
                inner.messages.pop_front();
            }
            inner.messages.push_back(message.clone());
            events.push(ServerEvent::Message { message });
        }
        // 持锁发布，保证并发 ingest 与订阅水位的事件顺序。
        for event in events {
            // 没有订阅者时 send 会失败，这是正常情况。
            let _ = self.tx.send(event);
        }
    }

    /// 记录 7154 包给出的当前大厅（补充信号）。
    pub async fn note_population_hall(&self, hall_id: u64) {
        let mut inner = self.inner.write().await;
        tracing::info!("7154 人数包给出当前大厅: {hall_id}");
        let hall_type = inner.current_hall.map(|(_, ty)| ty).unwrap_or(0);
        if inner.current_hall.map(|(id, _)| id) != Some(hall_id) {
            inner.current_hall = Some((hall_id, hall_type));
            let _ = self
                .tx
                .send(ServerEvent::HallChanged { hall_id, hall_type });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn raw(text: &str, from: u32, hall: Option<u64>, private: bool) -> RawChat {
        RawChat {
            ts: 100,
            from_uid: from,
            to_uid: None,
            text: text.into(),
            hall_id: hall,
            hall_type: hall.map(|_| 16),
            is_private: private,
        }
    }

    #[tokio::test]
    async fn follows_hall_from_first_message() {
        // 切换大厅后 7154 不会立刻重发，消息流是实时权威来源。
        // 因此未知当前大厅时，第一条私人大厅消息应确立并跟随该大厅。
        let state = ChatState::new(999);
        let mut msg = raw("hi", 1, Some(100), false);
        msg.hall_type = Some(16);
        state.ingest(vec![msg]).await;
        assert_eq!(state.current_hall().await, Some((100, 16)));
        assert_eq!(state.last_id().await, 1);
    }

    #[tokio::test]
    async fn accepts_messages_from_current_hall() {
        let state = ChatState::new(999);
        // 自己发言确立当前大厅。
        state.ingest(vec![raw("self", 999, Some(100), false)]).await;
        assert_eq!(state.current_hall().await, Some((100, 16)));
        // 他人同厅消息应被推送。
        state.ingest(vec![raw("hello", 1, Some(100), false)]).await;
        assert_eq!(state.last_id().await, 1);
    }

    #[tokio::test]
    async fn drops_public_channel_even_without_current_hall() {
        // type 23 是公共频道。即使还不知道自己在哪个大厅，也应直接丢弃，
        // 不能靠"hall_id 不匹配"——那样在 current_hall 未知时会漏判。
        let state = ChatState::new(999);
        let mut public = raw("跨厅喊话", 1, Some(100000008), false);
        public.hall_type = Some(PUBLIC_CHANNEL_HALL_TYPE);
        state.ingest(vec![public]).await;
        assert_eq!(state.last_id().await, 0, "公共频道消息必须丢弃");
        assert_eq!(state.current_hall().await, None, "公共频道不能确立当前大厅");
    }

    #[tokio::test]
    async fn own_public_channel_never_sets_or_changes_hall() {
        let state = ChatState::new(999);
        let mut public = raw("own public", 999, Some(23), false);
        public.hall_type = Some(PUBLIC_CHANNEL_HALL_TYPE);
        let mut rx = state.subscribe();
        state.ingest(vec![public.clone()]).await;
        assert_eq!(state.current_hall().await, None);
        assert!(matches!(
            rx.try_recv(),
            Err(broadcast::error::TryRecvError::Empty)
        ));
        state
            .ingest(vec![raw("own hall", 999, Some(100), false)])
            .await;
        rx.try_recv().unwrap();
        state.ingest(vec![public]).await;
        assert_eq!(state.current_hall().await, Some((100, 16)));
        assert_eq!(state.last_id().await, 0);
        assert!(matches!(
            rx.try_recv(),
            Err(broadcast::error::TryRecvError::Empty)
        ));
    }

    #[tokio::test]
    async fn subscription_watermark_separates_history_from_live_events() {
        let state = ChatState::new(999);
        state.ingest(vec![raw("before", 1, None, true)]).await;
        let (mut rx, watermark) = state.subscribe_with_last_id().await;
        assert_eq!(watermark, 1);
        state.ingest(vec![raw("after", 1, None, true)]).await;
        let ServerEvent::Message { message } = rx.try_recv().unwrap() else {
            panic!("expected message")
        };
        assert_eq!(message.id, watermark + 1);
        assert!(matches!(
            rx.try_recv(),
            Err(broadcast::error::TryRecvError::Empty)
        ));
    }

    #[tokio::test]
    async fn private_hall_type_is_accepted_once_current_known() {
        // type 16 是私人大厅。确立当前大厅后，同 hall 的他人消息应推送。
        let state = ChatState::new(999);
        let mut mine = raw("我的发言", 999, Some(500), false);
        mine.hall_type = Some(16);
        state.ingest(vec![mine]).await;
        assert_eq!(state.current_hall().await, Some((500, 16)));

        let mut other = raw("别人说话", 1, Some(500), false);
        other.hall_type = Some(16);
        state.ingest(vec![other]).await;
        assert_eq!(state.last_id().await, 1);
    }

    #[tokio::test]
    async fn follows_hall_switch_and_keeps_delivering() {
        // 玩家切换大厅：新大厅的消息不应被旧 hall_id 挡掉。
        let state = ChatState::new(999);
        let mut first = raw("old hall", 999, Some(100), false);
        first.hall_type = Some(16);
        state.ingest(vec![first]).await;
        assert_eq!(state.current_hall().await, Some((100, 16)));

        let mut moved = raw("new hall", 1, Some(200), false);
        moved.hall_type = Some(16);
        state.ingest(vec![moved]).await;
        assert_eq!(
            state.current_hall().await,
            Some((200, 16)),
            "应跟随到新大厅"
        );
        assert_eq!(state.last_id().await, 1, "新大厅他人消息必须推送");
    }

    #[tokio::test]
    async fn private_messages_always_accepted() {
        let state = ChatState::new(999);
        state.ingest(vec![raw("dm", 1, None, true)]).await;
        assert_eq!(state.last_id().await, 1);
    }

    #[tokio::test]
    async fn self_messages_are_not_dispatched() {
        let state = ChatState::new(999);
        state.ingest(vec![raw("mine", 999, Some(100), false)]).await;
        assert_eq!(state.last_id().await, 0, "自己发的消息不应作为命令推送");
        assert_eq!(state.current_hall().await, Some((100, 16)));
    }

    #[tokio::test]
    async fn buffered_messages_are_capped() {
        let state = ChatState::new(999);
        state.ingest(vec![raw("self", 999, Some(100), false)]).await;
        for i in 0..(MESSAGE_BUFFER + 10) {
            state
                .ingest(vec![raw(&format!("m{i}"), 1, Some(100), false)])
                .await;
        }
        let (msgs, last, truncated) = state.messages_since(0, usize::MAX).await;
        assert!(truncated, "since=0 也必须报告已淘汰的历史");
        assert_eq!(msgs.len(), MESSAGE_BUFFER);
        assert_eq!(last, (MESSAGE_BUFFER + 10) as u64);
    }

    #[tokio::test]
    async fn reports_truncation_when_since_is_too_old() {
        let state = ChatState::new(999);
        state.ingest(vec![raw("self", 999, Some(100), false)]).await;
        for i in 0..(MESSAGE_BUFFER + 10) {
            state
                .ingest(vec![raw(&format!("m{i}"), 1, Some(100), false)])
                .await;
        }
        let (_, _, truncated) = state.messages_since(1, 10).await;
        assert!(truncated, "请求区间早于缓冲起点应标记截断");
        assert!(
            !state.messages_since(10, 10).await.2,
            "恰好接续缓存不算截断"
        );
        assert!(!state.messages_since(u64::MAX, 10).await.2);
        let empty = ChatState::new(999);
        assert!(!empty.messages_since(0, 10).await.2);
        empty.ingest(vec![raw("first", 1, None, true)]).await;
        assert!(!empty.messages_since(0, 1).await.2);
    }

    #[tokio::test]
    async fn names_are_filled_from_cache() {
        let state = ChatState::new(999);
        state.record_names(vec![(1, "洛".into())]).await;
        state.ingest(vec![raw("self", 999, Some(100), false)]).await;
        state.ingest(vec![raw("hi", 1, Some(100), false)]).await;
        let (msgs, _, _) = state.messages_since(0, 10).await;
        assert_eq!(msgs[0].from.name.as_deref(), Some("洛"));
    }
}
