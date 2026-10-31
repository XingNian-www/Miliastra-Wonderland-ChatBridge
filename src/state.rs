//! 聊天状态：当前大厅判定、昵称缓存、消息缓冲与广播。
//!
//! 公共频道（hall_type=23）先过滤，包含 BOT 自己的发言。
//! 其余大厅消息按消息中的 hall_id / hall_type 跟随当前大厅；
//! 自己的发言只更新大厅状态，不作为命令推送。
//! 7154 人数包提供补充大厅信号，不用于过滤实时大厅消息。

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

use tokio::sync::{RwLock, broadcast};

use crate::extract::{FriendProfile, RawChat};
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

/// ingest 期间收集的待打印日志。
///
/// 为什么不在写锁内直接 `tracing::info!`：`TeeWriter` 是同步文件 IO，
/// 写盘会把 `ChatState` 写锁的持有时间放大，`/messages`、`/health` 与
/// DLL 侧 ACK 都被拖住。因此锁内只记录"哪条消息要打哪类日志"，
/// 锁释放后再用下方 `emit` 展开（format + 同步写盘）。
///
/// 文本通过下标借用 `raws`，所以锁内既不需要格式化，也不需要为打印
/// clone 消息正文；日志内容与旧实现逐字相同。
enum IngestLog {
    /// 公共频道（hall_type=23）被丢弃。
    PublicDiscard { index: usize },
    /// 自己发言：只更新大厅，不作为命令推送。
    OwnMessage { index: usize },
    /// 跟随消息切换了当前大厅（只在已知旧大厅时打印，与旧实现一致）。
    HallFollow { hall_id: u64, hall_type: u32 },
    /// 已推送的消息。
    Dispatch { index: usize, channel: Channel },
}

impl IngestLog {
    /// 展开日志：**必须**在 `ChatState` 写锁释放后调用。
    ///
    /// 这里的 `tracing` 调用会同步写文件；放在锁内会重新引入审计指出的
    /// "持锁做日志 IO"问题。
    fn emit(&self, raws: &[RawChat]) {
        match self {
            Self::PublicDiscard { index } => {
                let raw = &raws[*index];
                tracing::info!(
                    "[丢弃-公共频道] hall={:?} from={} ts={} text={:?}",
                    raw.hall_id,
                    raw.from_uid,
                    raw.ts,
                    raw.text
                );
            }
            Self::OwnMessage { index } => {
                tracing::debug!("跳过自己发的消息: {:?}", raws[*index].text);
            }
            Self::HallFollow { hall_id, hall_type } => {
                tracing::info!("跟随消息切换当前大厅: {hall_id} type={hall_type}");
            }
            Self::Dispatch { index, channel } => {
                let raw = &raws[*index];
                tracing::info!(
                    "推送消息: channel={:?} hall={:?} from={} text={:?}",
                    channel,
                    raw.hall_id,
                    raw.from_uid,
                    raw.text
                );
            }
        }
    }
}

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
    /// 好友备注（4410 的 #17）。与昵称分开存：备注是玩家自己起的名字，
    /// 可随时清空或改写，绝不能拿来当身份判断。
    remarks: BTreeMap<u32, String>,
    /// 已推送的最大 id。
    last_id: u64,
    /// 是否已收到过 4410 好友列表。
    ///
    /// 4410 在进游戏时全量下发一次，之后只有懒加载增量；协议里没有快照/增量
    /// 标记字段，所以只能按时序区分：第一次收到的当全量基线，之后都当增量合并。
    friend_list_seen: bool,
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
                remarks: BTreeMap::new(),
                last_id: 0,
                friend_list_seen: false,
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

    /// 合并好友档案（4410）：昵称与备注各写各的。
    ///
    /// 这张表只在进游戏时全量来一次，之后都是懒加载的增量；所以合并必须
    /// 只增不删——某次增量里没提到的 UID，不能把它已有的备注抹掉。
    /// 备注为空、或本条记录根本没带 #17 时，保留原值。
    /// 返回本次是否建立了全量基线（用于日志区分“首次全量”与“懒加载增量”）。
    pub async fn record_friend_profiles(&self, profiles: Vec<FriendProfile>) -> bool {
        if profiles.is_empty() {
            return false;
        }
        let mut inner = self.inner.write().await;
        let baseline = !inner.friend_list_seen;
        inner.friend_list_seen = true;
        for profile in profiles {
            if let Some(name) = profile.name {
                inner.names.insert(profile.uid, name);
            }
            if let Some(remark) = profile.remark {
                inner.remarks.insert(profile.uid, remark);
            }
        }
        baseline
    }

    /// 备注表规模，供诊断接口观察是否已经收到过全量基线。
    pub async fn remark_count(&self) -> usize {
        self.inner.read().await.remarks.len()
    }

    /// 批量查备注。没有备注的 UID 不返回，调用方据此区分“没设备注”与“空备注”。
    pub async fn remarks(&self, uids: &[u32]) -> BTreeMap<u32, String> {
        let inner = self.inner.read().await;
        uids.iter()
            .filter_map(|uid| inner.remarks.get(uid).map(|remark| (*uid, remark.clone())))
            .collect()
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
    ///
    /// 锁边界：批量处理与事件发布仍在写锁内（保持并发 ingest 与订阅水位
    /// 的事件顺序）；但所有日志（format + tracing 的同步写盘）都推迟到
    /// `drop(inner)` 之后，避免日志 IO 占住写锁、拖慢 /messages、/health
    /// 以及 DLL 侧等待的 ACK。
    pub async fn ingest(&self, raws: Vec<RawChat>) {
        if raws.is_empty() {
            return;
        }
        let mut inner = self.inner.write().await;
        let mut events = Vec::new();
        // 锁内只记下标与小字段：不含 String，不做 format，不做任何 IO。
        let mut logs = Vec::new();
        for (index, raw) in raws.iter().enumerate() {
            // 包括自己发言在内，公共频道都不能影响当前大厅。
            if !raw.is_private && raw.hall_type == Some(PUBLIC_CHANNEL_HALL_TYPE) {
                logs.push(IngestLog::PublicDiscard { index });
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
                logs.push(IngestLog::OwnMessage { index });
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
                        logs.push(IngestLog::HallFollow { hall_id, hall_type });
                    }
                    inner.current_hall = Some((hall_id, hall_type));
                    events.push(ServerEvent::HallChanged { hall_id, hall_type });
                }
                Channel::CurrentHall
            };
            logs.push(IngestLog::Dispatch { index, channel });
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
                // raws 要活到锁外打日志，这里只复制正文（String）；
                // 这是消息本体的必需字段，不是为打印而 clone 整条消息。
                text: raw.text.clone(),
            };
            inner.last_id = id;
            crate::diag::message(id, raw.ts);
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
        // 关键：先释放写锁再打印。tracing 的 TeeWriter 是同步文件 IO，
        // 留在锁内会放大锁持有时间，并可能让 DLL 侧等不到 ACK 而超时。
        drop(inner);
        for log in logs {
            log.emit(&raws);
        }
    }

    /// 记录 7154 包给出的当前大厅（补充信号）。
    ///
    /// 日志同样放在写锁之外：7154 可能高频到达，不能让同步写盘占住写锁。
    pub async fn note_population_hall(&self, hall_id: u64) {
        let mut inner = self.inner.write().await;
        let hall_type = inner.current_hall.map(|(_, ty)| ty).unwrap_or(0);
        if inner.current_hall.map(|(id, _)| id) != Some(hall_id) {
            inner.current_hall = Some((hall_id, hall_type));
            let _ = self
                .tx
                .send(ServerEvent::HallChanged { hall_id, hall_type });
        }
        drop(inner);
        tracing::info!("7154 人数包给出当前大厅: {hall_id}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::atomic::{AtomicUsize, Ordering};

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

    fn profile(uid: u32, name: Option<&str>, remark: Option<&str>) -> FriendProfile {
        FriendProfile {
            uid,
            name: name.map(str::to_owned),
            remark: remark.map(str::to_owned),
        }
    }

    #[tokio::test]
    async fn friend_profiles_fill_names_and_remarks_separately() {
        let state = ChatState::new(999);
        state
            .record_friend_profiles(vec![
                profile(100000001, Some("甲玩家"), Some("rmkAAA1")),
                profile(100000002, Some("乙玩家"), None),
            ])
            .await;
        let names = state.names(&[100000001, 100000002]).await;
        assert_eq!(names[&100000001].as_deref(), Some("甲玩家"));
        assert_eq!(names[&100000002].as_deref(), Some("乙玩家"));
        // 没设备注的 UID 不出现在备注表里，不能被当成空串。
        let remarks = state.remarks(&[100000001, 100000002]).await;
        assert_eq!(remarks.get(&100000001).map(String::as_str), Some("rmkAAA1"));
        assert!(!remarks.contains_key(&100000002));
    }

    /// 进游戏全量一次、之后懒加载增量：增量里没提到的条目不能被抹掉。
    #[tokio::test]
    async fn lazy_refresh_keeps_previous_remarks() {
        let state = ChatState::new(999);
        state
            .record_friend_profiles(vec![
                profile(1, Some("甲"), Some("备注甲")),
                profile(2, Some("乙"), Some("备注乙")),
            ])
            .await;
        // 懒加载只带回第二个人的昵称，且没带 #17。
        state
            .record_friend_profiles(vec![profile(2, Some("乙改名"), None)])
            .await;
        let remarks = state.remarks(&[1, 2]).await;
        assert_eq!(remarks.get(&1).map(String::as_str), Some("备注甲"));
        assert_eq!(remarks.get(&2).map(String::as_str), Some("备注乙"));
        let names = state.names(&[2]).await;
        assert_eq!(names[&2].as_deref(), Some("乙改名"));
    }

    #[tokio::test]
    async fn first_friend_list_is_baseline_and_later_ones_are_incremental() {
        let state = ChatState::new(999);
        assert_eq!(state.remark_count().await, 0);
        let baseline = state
            .record_friend_profiles(vec![
                profile(1, Some("甲"), Some("备注甲")),
                profile(2, Some("乙"), None),
            ])
            .await;
        assert!(baseline, "首次 4410 应视为全量基线");
        assert_eq!(state.remark_count().await, 1);
        let baseline = state
            .record_friend_profiles(vec![profile(3, Some("丙"), Some("备注丙"))])
            .await;
        assert!(!baseline, "之后的 4410 是懒加载增量");
        assert_eq!(state.remark_count().await, 2);
        // 空批次不改状态，也不该把首次基线“用掉”。
        let empty = ChatState::new(999);
        assert!(!empty.record_friend_profiles(Vec::new()).await);
        assert!(
            empty
                .record_friend_profiles(vec![profile(1, Some("甲"), None)])
                .await
        );
    }

    #[tokio::test]
    async fn remarks_win_over_older_value_but_never_become_empty() {
        let state = ChatState::new(999);
        state
            .record_friend_profiles(vec![profile(1, None, Some("旧备注"))])
            .await;
        // 在游戏里改了备注：新值覆盖旧值。
        state
            .record_friend_profiles(vec![profile(1, None, Some("新备注"))])
            .await;
        assert_eq!(
            state.remarks(&[1]).await.get(&1).map(String::as_str),
            Some("新备注")
        );
        // 只有昵称的增量不会清掉备注。
        state
            .record_friend_profiles(vec![profile(1, Some("甲"), None)])
            .await;
        assert_eq!(
            state.remarks(&[1]).await.get(&1).map(String::as_str),
            Some("新备注")
        );
    }

    #[tokio::test]
    async fn message_sender_name_ignores_remarks() {
        let state = ChatState::new(999);
        state
            .record_friend_profiles(vec![profile(1, Some("昵称"), Some("备注"))])
            .await;
        state.ingest(vec![raw("hi", 1, Some(100), false)]).await;
        let (msgs, _, _) = state.messages_since(0, 10).await;
        // 消息只带昵称：备注是不能作为身份判断的展示名。
        assert_eq!(msgs[0].from.name.as_deref(), Some("昵称"));
    }

    /// 日志探针：统计事件数量，并检查事件到达时 ChatState 写锁是否已释放。
    ///
    /// `try_write` 成功 = 写锁空闲；失败 = 事件仍在写锁临界区内（旧行为，
    /// 正是审计要消除的"持锁做日志 IO"）。
    ///
    /// 为什么装**全局** subscriber：tracing 的 callsite interest 是进程级
    /// 缓存，scoped subscriber 无法可靠接管已被并行测试线程注册成 never 的
    /// callsite（会让日志事件偶发丢失）。全局安装 + 重建 interest 缓存后，
    /// 所有 callsite 都注册到本探针；再用线程 id 过滤，只统计当前测试线程。
    #[derive(Clone)]
    struct LogProbe {
        state: std::sync::Weak<ChatState>,
        owner_thread: std::thread::ThreadId,
        events: Arc<AtomicUsize>,
        events_under_lock: Arc<AtomicUsize>,
        messages: Arc<std::sync::Mutex<Vec<String>>>,
    }

    struct ProbeLayer(LogProbe);

    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for ProbeLayer {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            let probe = &self.0;
            // 全局 subscriber 会收到所有测试线程的事件；只处理本测试线程的。
            if std::thread::current().id() != probe.owner_thread {
                return;
            }
            let Some(state) = probe.state.upgrade() else {
                return;
            };
            probe.events.fetch_add(1, Ordering::Relaxed);
            if state.inner.try_write().is_err() {
                probe.events_under_lock.fetch_add(1, Ordering::Relaxed);
            }
            let mut visitor = MessageVisitor::default();
            event.record(&mut visitor);
            if let Some(message) = visitor.message {
                probe.messages.lock().unwrap().push(message);
            }
        }
    }

    /// 安装进程级日志探针（整个测试进程只装一次）。
    fn install_log_probe(probe: LogProbe) {
        use tracing_subscriber::layer::SubscriberExt as _;

        static INSTALL: std::sync::Once = std::sync::Once::new();
        INSTALL.call_once(|| {
            let subscriber = tracing_subscriber::registry().with(ProbeLayer(probe));
            tracing::subscriber::set_global_default(subscriber)
                .expect("安装测试用全局 tracing subscriber 失败");
            // 之前可能已有 callsite 在无 subscriber 时被缓存为 never，重建。
            tracing::callsite::rebuild_interest_cache();
        });
    }

    /// 只取 tracing 事件的 `message` 字段（即日志正文）。
    #[derive(Default)]
    struct MessageVisitor {
        message: Option<String>,
    }

    impl tracing::field::Visit for MessageVisitor {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            if field.name() == "message" {
                self.message = Some(format!("{value:?}"));
            }
        }
    }

    /// 回归测试：ingest 的日志必须在写锁释放后发出，且内容与旧实现等价。
    #[tokio::test]
    async fn ingest_logs_after_releasing_write_lock() {
        let state = ChatState::new(999);
        let probe = LogProbe {
            state: Arc::downgrade(&state),
            owner_thread: std::thread::current().id(),
            events: Arc::new(AtomicUsize::new(0)),
            events_under_lock: Arc::new(AtomicUsize::new(0)),
            messages: Arc::new(std::sync::Mutex::new(Vec::new())),
        };
        install_log_probe(probe.clone());
        let events = Arc::clone(&probe.events);
        let events_under_lock = Arc::clone(&probe.events_under_lock);
        let messages = Arc::clone(&probe.messages);

        let mut public = raw("跨厅喊话", 1, Some(100000008), false);
        public.hall_type = Some(PUBLIC_CHANNEL_HALL_TYPE);
        state
            .ingest(vec![
                public,                             // 丢弃公共频道 → info
                raw("mine", 999, Some(100), false), // 跳过自己 → debug
                raw("hello", 1, None, true),        // 推送私聊 → info
                raw("moved", 1, Some(200), false),  // 跟随大厅 → info + 推送 info
            ])
            .await;
        // 先确认业务行为没变：公共频道与自己发言不推送，其余按 id 入队。
        assert_eq!(state.last_id().await, 2, "入队逻辑与顺序必须保持不变");
        assert_eq!(state.current_hall().await, Some((200, 16)));
        // 7154 只更新大厅，不产生消息（既有语义），日志同样应在锁外。
        state.note_population_hall(300).await;
        assert_eq!(state.current_hall().await, Some((300, 16)));

        // 5 条来自 ingest（丢弃/跳过/推送/跟随/推送）+ 1 条 7154。
        assert_eq!(events.load(Ordering::Relaxed), 6, "日志: {messages:?}");
        assert_eq!(
            events_under_lock.load(Ordering::Relaxed),
            0,
            "日志不得在 ChatState 写锁内发出"
        );

        let messages = messages.lock().unwrap();
        assert!(
            messages
                .iter()
                .any(|m| m.contains("[丢弃-公共频道]") && m.contains("跨厅喊话")),
            "{messages:?}"
        );
        assert!(
            messages
                .iter()
                .any(|m| m.contains("跳过自己发的消息") && m.contains("mine")),
            "{messages:?}"
        );
        assert!(
            messages.iter().any(|m| m.contains("推送消息")
                && m.contains("channel=Private")
                && m.contains("from=1")
                && m.contains("hello")),
            "{messages:?}"
        );
        assert!(
            messages
                .iter()
                .any(|m| m.contains("跟随消息切换当前大厅: 200 type=16")),
            "{messages:?}"
        );
        assert!(
            messages.iter().any(|m| m.contains("推送消息")
                && m.contains("channel=CurrentHall")
                && m.contains("moved")),
            "{messages:?}"
        );
        assert!(
            messages
                .iter()
                .any(|m| m.contains("7154 人数包给出当前大厅: 300")),
            "{messages:?}"
        );
    }
}
