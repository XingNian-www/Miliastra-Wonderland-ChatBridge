//! 聊天状态：当前大厅判定、昵称缓存、消息缓冲与广播。
//!
//! 公共频道（hall_type=23）先过滤，包含 BOT 自己的发言。
//! 其余大厅消息按消息中的 hall_id / hall_type 跟随当前大厅；
//! 自己的发言只更新大厅状态，不作为命令推送。
//! 7154 人数包提供补充大厅信号，不用于过滤实时大厅消息。

use std::collections::{BTreeMap, BTreeSet, VecDeque};
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

/// 判定全量快照所需的最少记录数。
///
/// 低于这个数量时无法区分「全量」与「懒加载增量」，一律按增量合并，
/// 避免小好友表里错把增量当快照而误删备注。
const FULL_SNAPSHOT_MIN_RECORDS: usize = 4;

/// 全量快照允许缺席的好友数（好友被删时会出现）。
const FULL_SNAPSHOT_MAX_MISSING: usize = 2;

/// 历史消息的容忍窗口（秒）。
///
/// 登录时游戏会把私聊历史用**实时事件**（7.2 的 25574）一次性重放一遍，
/// 协议里没有「这是历史」的标记，只能靠时间戳区分。实测重放消息的 ts 比当前时间
/// 早几分钟到几小时，而真正的实时消息 ts 就是当前时间，所以按这个窗口切开。
///
/// 代价：断线期间积压、且延迟超过该窗口的消息会被丢弃。对聊天机器人来说这是对的取舍
/// —— 重放会把历史命令再执行一遍，比漏一条旧消息危险得多。
pub const STALE_GRACE_SECS: i64 = 120;

/// 去重窗口长度（条数）。
///
/// 游戏会**周期性重放**整段私聊（实测每 60 秒一次、一次 18 条），
/// 其中「刚发出不久」的消息仍在 STALE_GRACE_SECS 之内，会重复通过时间戳过滤。
/// 所以再按 (发送者, 时间戳, 文本) 做一层去重。
const RECENT_LIMIT: usize = 1024;

/// 当前 Unix 时间（秒）。
/// 当前 Unix 时间（秒），测试里给消息打时间戳用。
#[cfg(test)]
pub(crate) fn now_unix_secs() -> u64 {
    unix_now().max(1) as u64
}

/// 消息指纹：发送者 + 时间戳 + 文本的哈希。
///
/// 只用来压重放，不参与业务判断；哈希冲突最多误丢一条，代价可接受。
fn message_fingerprint(raw: &RawChat) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    raw.from_uid.hash(&mut hasher);
    raw.ts.hash(&mut hasher);
    raw.text.hash(&mut hasher);
    hasher.finish()
}

/// 当前 Unix 时间（秒）。
fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| i64::try_from(elapsed.as_secs()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

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
    /// 历史重放：时间戳早于容忍窗口，直接丢弃。
    Stale { index: usize, ts: i64 },
    /// 周期性重放导致的重复消息，已发过同样的。
    Duplicate { index: usize },
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
            Self::Duplicate { index } => {
                tracing::info!(
                    "[丢弃-重复] from={} text={:?}",
                    raws[*index].from_uid,
                    raws[*index].text
                );
            }
            Self::Stale { index, ts } => {
                let raw = &raws[*index];
                tracing::info!(
                    "[丢弃-历史重放] ts={ts} from={} text={:?}",
                    raw.from_uid,
                    raw.text
                );
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
    /// 已知好友的 UID 集合。用来判断一次好友列表是全量快照还是懒加载增量，
    /// 并支撑「全量里没带备注 = 已被删除」的覆盖语义。
    friend_uids: BTreeSet<u32>,
    /// 当前好友表是否来自「全量快照」。
    ///
    /// 只有 true 时「列表里没有某个好友 / 某个好友没有备注」才是权威结论；
    /// 一旦混入懒加载增量就置 false，下游应只做增补，等下一次全量（≤60 秒）再清理。
    friends_complete: bool,
    /// 最近一次全量快照的 Unix 秒时间戳；从没收到过为 None。
    friends_updated_at: Option<i64>,
    /// 已推送的最大 id。
    last_id: u64,
    /// 是否已收到过 4410 好友列表。
    ///
    /// 4410 在进游戏时全量下发一次，之后只有懒加载增量；协议里没有快照/增量
    /// 标记字段，所以只能按时序区分：第一次收到的当全量基线，之后都当增量合并。
    friend_list_seen: bool,
    /// 早于这个时间戳的消息一律当历史重放丢弃。进程启动时定一次。
    accepted_after: i64,
    /// 最近发过的消息指纹，用于压掉周期性重放。
    recent: VecDeque<u64>,
    recent_set: std::collections::HashSet<u64>,
}

/// 单个好友的公开视图。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FriendEntry {
    pub uid: u32,
    /// 游戏内昵称；未知时为 None。
    pub name: Option<String>,
    /// 玩家设置的备注；None = 当前没有备注。
    pub remark: Option<String>,
}

/// 一份好友快照 + 它的完整性。
#[derive(Clone, Debug)]
pub struct FriendsSnapshot {
    /// 列表是否来自全量快照（决定调用方能否据此删除）。
    pub complete: bool,
    /// 最近一次全量快照的时间（Unix 秒）。
    pub updated_at: Option<i64>,
    pub entries: Vec<FriendEntry>,
}

/// 判定一次好友列表是不是「全量快照」。
///
/// 真机上每 60 秒的全量包包含所有好友；懒加载增量只带一两条。
/// 判错的代价不对称：把增量当全量会**误删**备注，把全量当增量只是少清一次，
/// 所以这里一律从严：
///
/// - 记录数少于 FULL_SNAPSHOT_MIN_RECORDS：当增量（极小好友表无法区分）；
/// - 覆盖不到已知好友的 3/4：当增量；
/// - 缺失好友数超过容忍值：当增量（避免一次残缺包清掉一堆备注）。
fn is_full_snapshot(incoming: &BTreeSet<u32>, known: &BTreeSet<u32>) -> bool {
    if known.is_empty() {
        return true;
    }
    if incoming.len() < FULL_SNAPSHOT_MIN_RECORDS {
        return false;
    }
    if incoming.len() * 4 < known.len() * 3 {
        return false;
    }
    let tolerance = FULL_SNAPSHOT_MAX_MISSING + known.len() / 10;
    known.difference(incoming).count() <= tolerance
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
                friend_uids: BTreeSet::new(),
                friends_complete: false,
                friends_updated_at: None,
                last_id: 0,
                friend_list_seen: false,
                accepted_after: unix_now() - STALE_GRACE_SECS,
                recent: VecDeque::new(),
                recent_set: std::collections::HashSet::new(),
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

    /// 合并好友档案（29281）：昵称与备注各写各的。
    ///
    /// 现实约束（真机实测）：好友列表进游戏时全量下发一次，之后**每 60 秒
    /// 全量重发一次**；此外还有只带一两条的懒加载增量。两类包没有显式标记，
    /// 只能按内容区分，见 is_full_snapshot。
    ///
    /// 语义分两种：
    /// - **全量快照**：记录里没带 #17 就是「该好友没有备注」→ 清除旧值；
    ///   快照里消失的好友，备注也一并清掉（昵称保留，历史消息还要用）。
    ///   这是实时同步的关键：玩家在游戏里删掉备注，最多 60 秒后这里就清掉。
    /// - **懒加载增量**：只增不删。增量没提到某人时保留原值，
    ///   否则一次只带一条的增量会把其他人的备注全抹掉。
    ///
    /// 返回本次是否建立了全量基线（首次全量）。
    pub async fn record_friend_profiles(&self, profiles: Vec<FriendProfile>) -> bool {
        if profiles.is_empty() {
            return false;
        }
        let mut inner = self.inner.write().await;
        let baseline = !inner.friend_list_seen;
        inner.friend_list_seen = true;

        let incoming: BTreeSet<u32> = profiles.iter().map(|profile| profile.uid).collect();
        let full = is_full_snapshot(&incoming, &inner.friend_uids);

        let mut updated = 0usize;
        let mut cleared = 0usize;
        for profile in &profiles {
            if let Some(name) = profile.name.as_deref() {
                inner.names.insert(profile.uid, name.to_owned());
            }
            match profile.remark.as_deref() {
                Some(remark) if !remark.is_empty() => {
                    let previous = inner.remarks.insert(profile.uid, remark.to_owned());
                    if previous.as_deref() != Some(remark) {
                        updated += 1;
                    }
                }
                _ => {
                    // 全量快照里没带备注 = 玩家删掉了；增量包保留原值。
                    if full && inner.remarks.remove(&profile.uid).is_some() {
                        cleared += 1;
                    }
                }
            }
        }

        if full {
            inner.friends_complete = true;
            inner.friends_updated_at = Some(unix_now());
            // 先算出差异再改：同时借用 friend_uids 与 remarks 会被借用检查拒绝。
            let gone: Vec<u32> = inner.friend_uids.difference(&incoming).copied().collect();
            for uid in gone {
                if inner.remarks.remove(&uid).is_some() {
                    cleared += 1;
                }
            }
            inner.friend_uids = incoming;
        } else {
            // 增量进了表：「缺谁」不再权威，等下一次全量快照再恢复。
            inner.friends_complete = false;
            inner.friend_uids.extend(incoming);
        }

        if updated > 0 || cleared > 0 {
            tracing::info!("好友备注同步：更新 {updated} 条、清除 {cleared} 条（全量快照={full}）");
        }
        baseline
    }

    /// 全量好友快照，供 GET /friends 使用。
    ///
    /// complete=true 表示这份列表来自全量快照：不在列表里的好友、以及 remark
    /// 为 None 的条目都是权威结论，调用方可以据此**删除**本地数据。
    /// complete=false 时只能增补，不能删（增量包没提到的人不代表不存在）。
    pub async fn friends_snapshot(&self) -> FriendsSnapshot {
        let inner = self.inner.read().await;
        let entries: Vec<FriendEntry> = inner
            .friend_uids
            .iter()
            .map(|uid| FriendEntry {
                uid: *uid,
                name: inner.names.get(uid).cloned(),
                remark: inner.remarks.get(uid).cloned(),
            })
            .collect();
        FriendsSnapshot {
            complete: inner.friends_complete,
            updated_at: inner.friends_updated_at,
            entries,
        }
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
        let mut messages: Vec<_> = inner
            .messages
            .iter()
            .filter(|m| m.id > since)
            .take(limit)
            .cloned()
            .collect();
        // 名字在读取时补：好友列表/成员列表可能比消息晚到（桥重启后尤其明显），
        // 入库时查不到名字的消息，不该永远显示 uid。
        for message in &mut messages {
            if message.from.name.is_none()
                && let Some(name) = inner.names.get(&message.from.uid)
            {
                message.from.name = Some(name.clone());
            }
        }
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
        // 历史重放过滤：见 STALE_GRACE_SECS 的说明。
        let now_secs = unix_now();
        for (index, raw) in raws.iter().enumerate() {
            let ts = i64::try_from(raw.ts).unwrap_or(i64::MAX);
            if ts > 0 && ts < now_secs - STALE_GRACE_SECS {
                logs.push(IngestLog::Stale { index, ts });
                continue;
            }
            // 周期性重放压重：同一 (发送者, 时间戳, 文本) 只放行一次。
            let fingerprint = message_fingerprint(raw);
            if !inner.recent_set.insert(fingerprint) {
                logs.push(IngestLog::Duplicate { index });
                continue;
            }
            inner.recent.push_back(fingerprint);
            if inner.recent.len() > RECENT_LIMIT
                && let Some(expired) = inner.recent.pop_front()
            {
                inner.recent_set.remove(&expired);
            }
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
            ts: unix_now().max(1) as u64,
            from_uid: from,
            to_uid: None,
            text: text.into(),
            hall_id: hall,
            hall_type: hall.map(|_| 16),
            is_private: private,
        }
    }

    #[tokio::test]
    async fn stale_history_burst_is_dropped() {
        // 7.2 登录时会把私聊历史用实时事件一次性重放（实测 20 条，ts 早几十分钟）。
        // 这些消息必须丢弃：否则历史里的 @命令 会被重新执行一遍。
        let chat = ChatState::new(1);
        let now = unix_now();
        let stale = RawChat {
            ts: (now - STALE_GRACE_SECS - 60) as u64,
            ..raw("旧命令", 2, None, true)
        };
        let fresh = RawChat {
            ts: now as u64,
            ..raw("新消息", 2, None, true)
        };
        chat.ingest(vec![stale, fresh]).await;
        let messages = chat.messages_since(0, 10).await.0;
        assert_eq!(messages.len(), 1, "历史重放应被丢弃，只留实时消息");
        assert_eq!(messages[0].text, "新消息");
    }

    #[tokio::test]
    async fn late_name_backfills_existing_messages() {
        // 桥重启后好友列表可能比消息晚到二十来秒；名字应在读取时补上，
        // 而不是让早期消息永远只有 uid。
        let chat = ChatState::new(1);
        chat.ingest(vec![raw("先到的消息", 100000002, None, true)])
            .await;
        let before = chat.messages_since(0, 10).await.0;
        assert_eq!(before[0].from.name, None, "此时还没有名字");

        chat.record_names(vec![(100000002, "玩家甲".to_owned())])
            .await;

        let after = chat.messages_since(0, 10).await.0;
        assert_eq!(after[0].from.name.as_deref(), Some("玩家甲"), "名字应回填");
        assert_eq!(after[0].text, "先到的消息", "消息本体不受影响");
    }

    #[tokio::test]
    async fn periodic_replay_does_not_duplicate() {
        // 游戏每 60 秒重放一次整段私聊，最近的消息仍在时间窗口内，必须靠指纹压掉。
        let chat = ChatState::new(1);
        let msg = raw("111", 100000001, None, true);
        chat.ingest(vec![msg.clone(), msg.clone(), msg]).await;
        let messages = chat.messages_since(0, 10).await.0;
        assert_eq!(messages.len(), 1, "同一条重放三次只能推一次");
        assert_eq!(messages[0].text, "111");
    }

    #[tokio::test]
    async fn same_text_new_timestamp_is_not_a_duplicate() {
        // 同一句话隔一会儿又说一遍，是两条独立消息，不能当成重放。
        let chat = ChatState::new(1);
        let first = raw("111", 100000001, None, true);
        let mut second = raw("111", 100000001, None, true);
        second.ts = first.ts + 5;
        chat.ingest(vec![first, second]).await;
        assert_eq!(chat.messages_since(0, 10).await.0.len(), 2);
    }

    #[tokio::test]
    async fn stale_window_boundary_is_inclusive() {
        // 正好落在窗口边界上的消息仍然接收，避免「刚好 120 秒」被误杀。
        let chat = ChatState::new(1);
        let now = unix_now();
        let on_edge = RawChat {
            ts: (now - STALE_GRACE_SECS) as u64,
            ..raw("边界", 2, None, true)
        };
        chat.ingest(vec![on_edge]).await;
        assert_eq!(chat.messages_since(0, 10).await.0.len(), 1);
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
    /// 6 个好友的完整快照：模拟真机每 60 秒一次的全量包。
    fn full_snapshot(remarks: &[(u32, Option<&str>)]) -> Vec<FriendProfile> {
        remarks
            .iter()
            .map(|(uid, remark)| profile(*uid, Some("昵称"), *remark))
            .collect()
    }

    #[tokio::test]
    async fn full_snapshot_clears_remark_when_field_is_gone() {
        let state = ChatState::new(999);
        let uids = [1, 2, 3, 4, 5, 6];
        state
            .record_friend_profiles(
                uids.iter()
                    .map(|uid| profile(*uid, Some("昵称"), Some("旧备注")))
                    .collect(),
            )
            .await;
        assert_eq!(state.remark_count().await, 6);

        // 下一次全量快照里 #1 不再带 #17 —— 玩家把备注删了。
        state
            .record_friend_profiles(full_snapshot(&[
                (1, None),
                (2, Some("旧备注")),
                (3, Some("旧备注")),
                (4, Some("旧备注")),
                (5, Some("旧备注")),
                (6, Some("旧备注")),
            ]))
            .await;
        assert_eq!(state.remark_count().await, 5, "全量快照缺 #17 应清除备注");
        assert!(!state.remarks(&[1, 2]).await.contains_key(&1));
    }

    #[tokio::test]
    async fn full_snapshot_updates_changed_remark() {
        let state = ChatState::new(999);
        let uids = [1, 2, 3, 4, 5, 6];
        state
            .record_friend_profiles(
                uids.iter()
                    .map(|uid| profile(*uid, Some("昵称"), Some("旧备注")))
                    .collect(),
            )
            .await;
        let mut next = full_snapshot(&[
            (1, Some("新备注")),
            (2, Some("旧备注")),
            (3, Some("旧备注")),
            (4, Some("旧备注")),
            (5, Some("旧备注")),
            (6, Some("旧备注")),
        ]);
        next[0].remark = Some("新备注".to_owned());
        state.record_friend_profiles(next).await;
        let remarks = state.remarks(&[1]).await;
        assert_eq!(remarks.get(&1).map(String::as_str), Some("新备注"));
    }

    #[tokio::test]
    async fn incremental_packet_still_keeps_other_remarks() {
        // 与 lazy_refresh_keeps_previous_remarks 互补：好友数较多时，
        // 只带 2 条的增量也必须按合并处理，不能清掉其他人的备注。
        let state = ChatState::new(999);
        let uids = [1, 2, 3, 4, 5, 6];
        state
            .record_friend_profiles(
                uids.iter()
                    .map(|uid| profile(*uid, Some("昵称"), Some("备注")))
                    .collect(),
            )
            .await;
        state
            .record_friend_profiles(vec![
                profile(2, Some("乙改名"), None),
                profile(3, None, None),
            ])
            .await;
        assert_eq!(state.remark_count().await, 6, "增量包不能清除备注");
    }

    #[tokio::test]
    async fn full_snapshot_drops_remark_of_removed_friend() {
        let state = ChatState::new(999);
        let uids = [1, 2, 3, 4, 5, 6];
        state
            .record_friend_profiles(
                uids.iter()
                    .map(|uid| profile(*uid, Some("昵称"), Some("备注")))
                    .collect(),
            )
            .await;
        // 6 号好友被删：快照里只剩 5 个。
        state
            .record_friend_profiles(full_snapshot(&[
                (1, Some("备注")),
                (2, Some("备注")),
                (3, Some("备注")),
                (4, Some("备注")),
                (5, Some("备注")),
            ]))
            .await;
        assert!(
            !state.remarks(&[6]).await.contains_key(&6),
            "好友被删应清掉备注"
        );
        assert_eq!(state.remark_count().await, 5);
    }

    #[test]
    fn snapshot_detection_is_conservative() {
        let known: BTreeSet<u32> = (1..=10).collect();
        // 全量：10/10。
        assert!(is_full_snapshot(&(1..=10).collect(), &known));
        // 少 1~2 个（好友被删）仍算全量。
        assert!(is_full_snapshot(&(1..=9).collect(), &known));
        assert!(is_full_snapshot(&(1..=8).collect(), &known));
        // 只带几条：增量。
        assert!(!is_full_snapshot(&(1..=3).collect(), &known));
        assert!(!is_full_snapshot(&(1..=6).collect(), &known));
        // 首包无基线：当全量建立基线。
        assert!(is_full_snapshot(&(1..=6).collect(), &BTreeSet::new()));
        // 极小好友表：记录不足下限，绝不按全量处理。
        let tiny: BTreeSet<u32> = (1..=3).collect();
        assert!(!is_full_snapshot(&(1..=2).collect(), &tiny));
    }
    #[tokio::test]
    async fn friends_snapshot_reports_completeness() {
        let state = ChatState::new(999);
        let empty = state.friends_snapshot().await;
        assert!(!empty.complete, "没收到过快照时不能声称完整");
        assert!(empty.entries.is_empty());
        assert!(empty.updated_at.is_none());

        let uids = [1, 2, 3, 4, 5, 6];
        state
            .record_friend_profiles(
                uids.iter()
                    .map(|uid| {
                        let remark = if *uid == 1 { Some("备注甲") } else { None };
                        profile(*uid, Some("昵称"), remark)
                    })
                    .collect(),
            )
            .await;
        let snap = state.friends_snapshot().await;
        assert!(snap.complete, "全量快照应标记完整");
        assert!(snap.updated_at.is_some());
        assert_eq!(snap.entries.len(), 6);
        let first = snap.entries.iter().find(|entry| entry.uid == 1).unwrap();
        assert_eq!(first.name.as_deref(), Some("昵称"));
        assert_eq!(first.remark.as_deref(), Some("备注甲"));
        let second = snap.entries.iter().find(|entry| entry.uid == 2).unwrap();
        assert!(second.remark.is_none(), "没备注的好友 remark 应为 None");

        // 混入增量包后，「缺席者」不再权威。
        state
            .record_friend_profiles(vec![profile(2, Some("乙"), None)])
            .await;
        let snap = state.friends_snapshot().await;
        assert!(!snap.complete, "增量之后必须降级为不完整");
        assert_eq!(snap.entries.len(), 6, "增量不能删除已有好友");
    }
}
