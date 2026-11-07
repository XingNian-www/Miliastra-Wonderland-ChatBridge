//! HTTP + WebSocket 服务。

use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::{Sink, SinkExt, Stream, StreamExt};

use axum::extract::rejection::{JsonRejection, QueryRejection};
use axum::extract::ws::rejection::WebSocketUpgradeRejection;
use axum::extract::ws::{CloseFrame, Message, WebSocketUpgrade};
use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::json;
use tokio::sync::broadcast::error::RecvError;

use crate::model::ServerEvent;
use crate::packet_control::{PacketControl, PacketMode};
use crate::state::ChatState;

/// 服务运行期状态。
pub struct AppState {
    pub chat: Arc<ChatState>,
    pub token: String,
    pub started: Instant,
    pub connected: Arc<std::sync::atomic::AtomicBool>,
    /// 监听端口与自身 UID：诊断接口要展示"这份实例是谁"。
    pub port: u16,
    pub self_uid: u32,
    /// 调试全包模式开关与抓包状态，由 main 构造；HTTP 侧只做读写。
    pub packets: Arc<PacketControl>,
    /// 会话附着状态（attached / reattach 等待与轮数），由 main 构造、
    /// inject 更新；/health 只读。
    pub session: Arc<crate::SessionStatus>,
    /// DLL 健康心跳状态：inject 的健康管道读线程更新，/health 只读。
    /// 旧 DLL 不连健康管道时 state 会一直是 unknown（这不是错误）。
    pub dll_health: Arc<crate::dll_health::HealthState>,
    /// 进程启动标识：main 构造时生成一次，生命周期内不变。
    ///
    /// 消息 id 每次进程重启都从 1 开始；上游用 bootId 区分「同一进程内
    /// 序号异常回退」与「宿主重启（应重置游标）」。同时暴露在 /health 与
    /// WS hello，便于上游确认自己连的是哪个实例。
    pub boot_id: u64,
}

/// 生成进程级 bootId：启动时刻的 Unix 毫秒 + 10 位随机低位。
///
/// - 不用 pid：系统会复用；
/// - 不用纯时间戳：同一毫秒内的快速重启/双实例区分不开；
/// - 左移 10 位后仍小于 2^53，浏览器按无损整数解析也不会丢精度。
///
/// 只在 main 构造 AppState 时调用一次；之后只读，所以 /health 与 hello
/// 永远给出同一个值。
pub fn generate_boot_id() -> u64 {
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .unwrap_or(0);
    (millis << 10) | (rand::random::<u64>() & 0x3ff)
}

/// 组装路由。
pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        // 观察页由浏览器直接打开，页面本身不含数据，令牌由页面本地保存。
        .route("/", get(observer_page))
        .route("/favicon.ico", get(favicon))
        .route("/health", get(health))
        .route("/messages", get(messages))
        .route("/names", get(names))
        // 全量好友快照：/names 必须带 UID 才能查，应用侧拿不到「当前有哪些好友」。
        .route("/friends", get(friends))
        .route("/stream", get(stream))
        .route("/debug/metrics", get(debug_metrics))
        .route("/debug/events", get(debug_events))
        // GET 读快照、POST 切模式：必须挂在同一路由上，否则 POST 会被
        // 全局 method_not_allowed 兜底吞掉（405 而不是业务响应）。
        .route(
            "/debug/packets",
            get(debug_packets_get).post(debug_packets_set),
        )
        // 抓包开关：运行期开/关与换目录，不用为 --capture-dir 重启进程。
        .route(
            "/debug/capture",
            get(debug_capture_get).post(debug_capture_set),
        )
        .fallback(not_found)
        .method_not_allowed_fallback(method_not_allowed)
        .with_state(state)
}

/// 内嵌的实时观察页：单文件 HTML，不引用外部资源。
///
/// 页面只有静态外壳，消息通过同源 API/WS 获取；令牌由浏览器填好后存在
/// localStorage，不写进 HTML，也不经过本函数的响应。
async fn observer_page() -> Response {
    (
        [
            (header::CONTENT_TYPE, "text/html; charset=utf-8"),
            (header::CACHE_CONTROL, "no-store"),
            (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
            (header::REFERRER_POLICY, "no-referrer"),
        ],
        include_str!("../web/index.html"),
    )
        .into_response()
}

/// 浏览器会固定请求 /favicon.ico；返回 204，避免落到 404 JSON 噪音。
async fn favicon() -> StatusCode {
    StatusCode::NO_CONTENT
}

/// 统一错误响应。
struct ApiError(StatusCode, &'static str, String);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, code, message) = (self.0, self.1, self.2);
        (
            status,
            Json(json!({ "error": { "code": code, "message": message } })),
        )
            .into_response()
    }
}

async fn method_not_allowed() -> ApiError {
    ApiError(
        StatusCode::METHOD_NOT_ALLOWED,
        "method_not_allowed",
        "method not allowed".into(),
    )
}

async fn not_found() -> ApiError {
    ApiError(StatusCode::NOT_FOUND, "not_found", "path not found".into())
}

fn parse_query<T>(query: Result<Query<T>, QueryRejection>) -> Result<T, ApiError> {
    query
        .map(|Query(value)| value)
        .map_err(|err| ApiError(StatusCode::BAD_REQUEST, "bad_request", err.body_text()))
}

fn unauthorized() -> ApiError {
    ApiError(
        StatusCode::UNAUTHORIZED,
        "unauthorized",
        "invalid or missing x-miliastra-token".into(),
    )
}

/// 常数时间比较，避免时序侧信道。
fn token_matches(expected: &str, actual: &str) -> bool {
    let a = expected.as_bytes();
    let b = actual.as_bytes();
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b) {
        diff |= x ^ y;
    }
    diff == 0
}

fn check_token(state: &AppState, headers: &HeaderMap) -> Result<(), ApiError> {
    let token = headers
        .get("x-miliastra-token")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(unauthorized)?;
    if token_matches(&state.token, token) {
        Ok(())
    } else {
        Err(unauthorized())
    }
}

async fn health(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    // 鉴权通过后始终返回 200，连接状态由 gameConnected 表示。
    if let Err(err) = check_token(&state, &headers) {
        return err.into_response();
    }
    let hall = state.chat.current_hall().await;
    Json(json!({
        "ok": true,
        "version": env!("CARGO_PKG_VERSION"),
        // 进程启动标识：同一进程内恒定，重启后变化。上游据此检测
        // 「消息序号归零」，重置自己的游标/水位（见 docs/API.md）。
        "bootId": state.boot_id,
        "uptimeSec": state.started.elapsed().as_secs(),
        "gameConnected": state.connected.load(std::sync::atomic::Ordering::Relaxed),
        // reattach 观测：attached=当前有活跃会话；reattachWaiting=正在等 DLL
        // 重连；reattachAttempts=本进程进入等待的轮数（反复失败会持续增大）。
        "attached": state.session.attached(),
        "reattachWaiting": state.session.reattach_waiting(),
        "reattachAttempts": state.session.reattach_attempts(),
        // DLL 健康心跳（独立管道，每 1s 一帧）：unknown=旧 DLL/尚未收到；
        // alive/heartbeatAgeMs/state 按阈值现算；silent=5s 无心跳。
        "dll": state.dll_health.snapshot_json(),
        // type 为 0 表示还没观测到（7154 人数包只给 hall_id）。
        // 输出 null 而不是 0，避免调用方把 0 当成真实大厅类型。
        "currentHall": hall.map(|(id, ty)| {
            json!({ "id": id, "type": if ty == 0 { serde_json::Value::Null } else { json!(ty) } })
        }),
        "lastMessageId": state.chat.last_id().await,
        "subscribers": state.chat.subscriber_count().await,
        // 一眼判断"活着/卡住/断流"的摘要；完整指标在 /debug/metrics。
        "diag": crate::diag::health_summary(),
    }))
    .into_response()
}

#[derive(Deserialize)]
struct MessagesQuery {
    since: Option<u64>,
    limit: Option<usize>,
}

async fn messages(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    query: Result<Query<MessagesQuery>, QueryRejection>,
) -> Response {
    if let Err(err) = check_token(&state, &headers) {
        return err.into_response();
    }
    if !state.connected.load(std::sync::atomic::Ordering::Relaxed) {
        return ApiError(
            StatusCode::SERVICE_UNAVAILABLE,
            "game_not_connected",
            "game is not running or injection failed".into(),
        )
        .into_response();
    }
    let query = match parse_query(query) {
        Ok(query) => query,
        Err(err) => return err.into_response(),
    };
    let since = query.since.unwrap_or(0);
    let limit = query.limit.unwrap_or(200).min(1000);
    let (messages, last, truncated) = state.chat.messages_since(since, limit).await;
    Json(json!({
        "messages": messages,
        "lastMessageId": last,
        "truncated": truncated,
    }))
    .into_response()
}

#[derive(Deserialize)]
struct NamesQuery {
    uids: String,
}

/// GET /friends：返回全量好友快照。
///
/// 为什么需要它：/names 必须显式传 UID 才能查，应用侧无法知道「当前有哪些好友」，
/// 也就没法做强制覆盖（拿着旧名单去比对）。好友列表本来每 60 秒全量重发一次，
/// 这里直接把它暴露出来，附 complete 标志说明缺席者是否可信。
async fn friends(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    if let Err(err) = check_token(&state, &headers) {
        return err.into_response();
    }
    if !state.connected.load(std::sync::atomic::Ordering::Relaxed) {
        return ApiError(
            StatusCode::SERVICE_UNAVAILABLE,
            "game_not_connected",
            "game is not running or injection failed".into(),
        )
        .into_response();
    }
    let snapshot = state.chat.friends_snapshot().await;
    let friends: Vec<serde_json::Value> = snapshot
        .entries
        .iter()
        .map(|entry| {
            json!({
                "uid": entry.uid,
                "name": entry.name,
                "remark": entry.remark,
            })
        })
        .collect();
    Json(json!({
        "ok": true,
        // 只有 true 才能据此删除本地数据：缺席者与 remark=null 都是权威结论。
        // false 表示中间混入过懒加载增量，只可增补，等下一次全量（≤60 秒）。
        "complete": snapshot.complete,
        "updatedAt": snapshot.updated_at,
        "count": friends.len(),
        "friends": friends,
    }))
    .into_response()
}

async fn names(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    query: Result<Query<NamesQuery>, QueryRejection>,
) -> Response {
    if let Err(err) = check_token(&state, &headers) {
        return err.into_response();
    }
    let query = match parse_query(query) {
        Ok(query) => query,
        Err(err) => return err.into_response(),
    };
    if !state.connected.load(std::sync::atomic::Ordering::Relaxed) {
        return ApiError(
            StatusCode::SERVICE_UNAVAILABLE,
            "game_not_connected",
            "game is not running or injection failed".into(),
        )
        .into_response();
    }
    let mut uids = Vec::new();
    for part in query.uids.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        match part.parse::<u32>() {
            Ok(uid) => uids.push(uid),
            Err(_) => {
                return ApiError(
                    StatusCode::BAD_REQUEST,
                    "bad_request",
                    format!("invalid uid: {part}"),
                )
                .into_response();
            }
        }
    }
    if uids.len() > 100 {
        return ApiError(
            StatusCode::BAD_REQUEST,
            "bad_request",
            "too many uids (max 100)".into(),
        )
        .into_response();
    }
    let found = state.chat.names(&uids).await;
    let mut out = serde_json::Map::new();
    for (uid, name) in found {
        out.insert(uid.to_string(), json!(name));
    }
    // 备注是新增字段：老客户端忽略即可；没设备注的 UID 不出现，
    // 调用方不会把空串误当成“备注为空”。
    let remarked = state.chat.remarks(&uids).await;
    let mut remarks = serde_json::Map::new();
    for (uid, remark) in remarked {
        remarks.insert(uid.to_string(), json!(remark));
    }
    Json(json!({ "names": out, "remarks": remarks })).into_response()
}

#[derive(Deserialize)]
struct StreamQuery {
    token: Option<String>,
}

async fn stream(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    query: Result<Query<StreamQuery>, QueryRejection>,
    ws: Result<WebSocketUpgrade, WebSocketUpgradeRejection>,
) -> Response {
    let query = match parse_query(query) {
        Ok(query) => query,
        Err(err) => return err.into_response(),
    };
    // 浏览器 WS API 不能设自定义头，额外支持 ?token=。
    let authorized = match query.token.as_deref() {
        Some(token) => token_matches(&state.token, token),
        None => check_token(&state, &headers).is_ok(),
    };
    if !authorized {
        return unauthorized().into_response();
    }
    let ws = match ws {
        Ok(ws) => ws,
        Err(err) => return ApiError(err.status(), "bad_request", err.body_text()).into_response(),
    };
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

const PING_INTERVAL: Duration = Duration::from_secs(30);
const PONG_TIMEOUT: Duration = Duration::from_secs(10);

/// 诊断指标：管道、收包、消息、订阅的完整快照。
///
/// 与 /health 分开：这里字段多、会变动，调用方按需取，不污染健康检查的稳定契约。
async fn debug_metrics(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    if let Err(err) = check_token(&state, &headers) {
        return err.into_response();
    }
    let hall = state.chat.current_hall().await;
    let snapshot = crate::diag::snapshot();
    let mut value = serde_json::to_value(&snapshot).unwrap_or_else(|_| json!({}));
    value["service"] = json!({
        "port": state.port,
        "selfUid": state.self_uid,
        "uptimeSec": state.started.elapsed().as_secs(),
        "lastMessageId": state.chat.last_id().await,
        "currentHall": hall.map(|(id, ty)| json!({
            "id": id,
            "type": if ty == 0 { serde_json::Value::Null } else { json!(ty) },
        })),
    });
    Json(value).into_response()
}

/// 最近的关键事件（连接/断开/协议错误/订阅滞后），旧的在前。
async fn debug_events(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    if let Err(err) = check_token(&state, &headers) {
        return err.into_response();
    }
    Json(json!({ "events": crate::diag::events() })).into_response()
}

/// 调试包模式快照。
///
/// 默认 chat（只转发聊天包）；all 时新 DLL 会额外转发全部包。
/// 快照已包含 ok/mode/dllControl/计数/抓包状态，这里原样透传。
async fn debug_packets_get(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    if let Err(err) = check_token(&state, &headers) {
        return err.into_response();
    }
    Json(state.packets.snapshot_json()).into_response()
}

#[derive(Deserialize)]
struct PacketModeBody {
    mode: String,
}

/// /debug/capture 的 POST 体。enabled=false 表示停止；dir/maxMib 只在启动时用。
#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct DebugCaptureBody {
    enabled: bool,
    #[serde(default)]
    dir: Option<String>,
    #[serde(default)]
    max_mib: Option<u64>,
}

/// 切换调试包模式。
///
/// 响应里的 applied 表示是否真的写到了 DLL 控制管道：DLL 未连接或
/// 旧 DLL 不支持控制管道时为 false（此时只在宿主侧记录模式，
/// dllControl 明确告诉调用方控制通道是否可用）。
async fn debug_packets_set(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Result<Json<PacketModeBody>, JsonRejection>,
) -> Response {
    if let Err(err) = check_token(&state, &headers) {
        return err.into_response();
    }
    let Json(body) = match body {
        Ok(body) => body,
        Err(err) => {
            return ApiError(StatusCode::BAD_REQUEST, "bad_request", err.body_text())
                .into_response();
        }
    };
    let Some(mode) = PacketMode::parse(&body.mode) else {
        return ApiError(
            StatusCode::BAD_REQUEST,
            "bad_request",
            "mode must be chat or all".into(),
        )
        .into_response();
    };
    // 写 DLL 控制管道是同步 write+flush，可能等待管道对端；放到阻塞线程池，
    // 避免占住 tokio worker（聊天链路虽然独立，但 HTTP 仍要响应）。
    let packets = Arc::clone(&state.packets);
    let applied = match tokio::task::spawn_blocking(move || packets.set_mode(mode)).await {
        Ok(applied) => applied,
        Err(error) => {
            tracing::warn!("切换 debug 模式任务失败：{error}");
            false
        }
    };
    // 切到 ALL 就顺手把抓包打开：省掉「为了抓包重启一次」这个步骤。
    // 切回 chat 则把「自动开的」抓包收掉——不对称收尾会让抓包在 chat 模式下
    // 一直挂着，用户以为已经停了。手工开的（POST /debug/capture）不受影响。
    if mode == PacketMode::All {
        state.packets.ensure_capture_for_all();
    } else {
        state.packets.stop_auto_capture();
    }
    // 抓包目录永远按「当前是否在抓」返回：之前只在本次新开抓包时给值，
    // 已在进行时返回 null，调用方会误以为没在抓。
    let capture_dir = state.packets.capture_dir();
    Json(json!({
        "ok": true,
        "mode": mode.as_str(),
        "applied": applied,
        "dllControl": state.packets.control_connected(),
        "captureDir": capture_dir.map(|dir| dir.display().to_string()),
        "captureActive": state.packets.capture_active(),
    }))
    .into_response()
}

async fn debug_capture_get(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    if let Err(err) = check_token(&state, &headers) {
        return err.into_response();
    }
    Json(json!({
        "ok": true,
        "active": state.packets.capture_active(),
        "defaultDir": crate::packet_control::default_capture_dir().display().to_string(),
        "capture": state.packets.snapshot_json()["capture"].clone(),
    }))
    .into_response()
}

async fn debug_capture_set(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Result<Json<DebugCaptureBody>, JsonRejection>,
) -> Response {
    if let Err(err) = check_token(&state, &headers) {
        return err.into_response();
    }
    let Json(body) = match body {
        Ok(body) => body,
        Err(err) => {
            return ApiError(StatusCode::BAD_REQUEST, "bad_request", err.body_text())
                .into_response();
        }
    };
    let packets = Arc::clone(&state.packets);
    let dir = body.dir.map(std::path::PathBuf::from);
    let max_bytes = body.max_mib.unwrap_or(0).saturating_mul(1024 * 1024);
    let result = tokio::task::spawn_blocking(move || {
        if body.enabled {
            packets
                .start_capture(dir, max_bytes)
                .map(|path| json!({ "active": true, "dir": path.display().to_string() }))
        } else {
            Ok(json!({ "active": false, "stopped": packets.stop_capture() }))
        }
    })
    .await;
    match result {
        Ok(Ok(value)) => Json(json!({ "ok": true, "capture": value })).into_response(),
        Ok(Err(message)) => {
            ApiError(StatusCode::BAD_REQUEST, "bad_request", message).into_response()
        }
        Err(error) => {
            tracing::warn!("切换抓包任务失败：{error}");
            ApiError(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                error.to_string(),
            )
            .into_response()
        }
    }
}

async fn handle_socket<S, E>(mut socket: S, state: Arc<AppState>)
where
    S: Stream<Item = Result<Message, E>> + Sink<Message> + Unpin,
{
    let _subscriber = crate::diag::subscriber();
    let (mut rx, last_message_id) = state.chat.subscribe_with_last_id().await;
    // 接入状态是"连接时刻"的快照：之后会变化（attach/detach/reattach），
    // 客户端需要持续观测 /health，不能把 hello 里的 true 当成永久在线。
    let hello = ServerEvent::Hello {
        version: env!("CARGO_PKG_VERSION"),
        last_message_id,
        boot_id: state.boot_id,
        game_connected: state.connected.load(std::sync::atomic::Ordering::Relaxed),
        attached: state.session.attached(),
        reattach_waiting: state.session.reattach_waiting(),
    };
    if send_event(&mut socket, &hello, None).await.is_err() {
        return;
    }
    let mut next_ping = tokio::time::Instant::now() + PING_INTERVAL;
    let mut pending_pong: Option<(bytes::Bytes, tokio::time::Instant)> = None;
    let mut ping_id = 0u64;
    loop {
        let deadline = pending_pong.as_ref().map(|(_, deadline)| *deadline);
        tokio::select! {
            // 默认公平选择，持续的输入或聊天都不能独占循环。
            _ = tokio::time::sleep_until(deadline.unwrap_or(next_ping)) => {
                if pending_pong.is_some() {
                    close_socket(&mut socket, 1008, "pong timeout").await;
                    break;
                }
                ping_id = ping_id.wrapping_add(1);
                let payload = bytes::Bytes::copy_from_slice(&ping_id.to_be_bytes());
                let deadline = tokio::time::Instant::now() + PONG_TIMEOUT;
                if send_frame(&mut socket, Message::Ping(payload.clone()), Some(deadline)).await.is_err() {
                    break;
                }
                pending_pong = Some((payload, deadline));
                next_ping += PING_INTERVAL;
            },
            incoming = socket.next() => match incoming {
                Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                Some(Ok(Message::Pong(payload))) => {
                    if pending_pong.as_ref().is_some_and(|(expected, _)| *expected == payload) {
                        pending_pong = None;
                    }
                }
                Some(Ok(Message::Ping(payload))) => {
                    if send_frame(&mut socket, Message::Pong(payload), deadline).await.is_err() {
                        break;
                    }
                }
                Some(Ok(_)) => {}
            },
            event = rx.recv() => match event {
                Ok(event) => {
                    if send_event(&mut socket, &event, deadline).await.is_err() {
                        break;
                    }
                }
                Err(RecvError::Lagged(n)) => {
                    tracing::warn!("WS 订阅者落后 {n} 条事件，断开以便重连补拉");
                    crate::diag::lagged(n);
                    close_socket(&mut socket, 1013, "stream lagged; reconnect and fetch /messages").await;
                    break;
                }
                Err(RecvError::Closed) => break,
            },
        }
    }
}

async fn close_socket<S>(socket: &mut S, code: u16, reason: &'static str)
where
    S: Sink<Message> + Unpin,
{
    let _ = send_frame(
        socket,
        Message::Close(Some(CloseFrame {
            code,
            reason: reason.into(),
        })),
        None,
    )
    .await;
}

async fn send_frame<S>(
    socket: &mut S,
    message: Message,
    deadline: Option<tokio::time::Instant>,
) -> Result<(), ()>
where
    S: Sink<Message> + Unpin,
{
    // 慢客户端也不能让发送永久卡住或越过未收到 Pong 的截止时间。
    let deadline = deadline.unwrap_or_else(|| tokio::time::Instant::now() + PONG_TIMEOUT);
    tokio::time::timeout_at(deadline, socket.send(message))
        .await
        .map_err(|_| ())?
        .map_err(|_| ())
}

async fn send_event<S>(
    socket: &mut S,
    event: &ServerEvent,
    deadline: Option<tokio::time::Instant>,
) -> Result<(), ()>
where
    S: Sink<Message> + Unpin,
{
    let text = serde_json::to_string(event).map_err(|_| ())?;
    send_frame(socket, Message::Text(text.into()), deadline).await
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::pin::Pin;
    use std::task::{Context, Poll};
    use tokio::sync::{mpsc, oneshot};
    use tower::ServiceExt;

    struct MockSocket {
        incoming: mpsc::UnboundedReceiver<Result<Message, ()>>,
        outgoing: mpsc::UnboundedSender<Message>,
        flush_gate: Option<oneshot::Receiver<()>>,
    }

    impl Stream for MockSocket {
        type Item = Result<Message, ()>;
        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            self.incoming.poll_recv(cx)
        }
    }

    impl Sink<Message> for MockSocket {
        type Error = ();
        fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), ()>> {
            Poll::Ready(Ok(()))
        }
        fn start_send(self: Pin<&mut Self>, item: Message) -> Result<(), ()> {
            self.outgoing.send(item).map_err(|_| ())
        }
        fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), ()>> {
            if let Some(gate) = &mut self.flush_gate {
                if std::future::Future::poll(Pin::new(gate), cx).is_pending() {
                    return Poll::Pending;
                }
                self.flush_gate = None;
            }
            Poll::Ready(Ok(()))
        }
        fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), ()>> {
            Poll::Ready(Ok(()))
        }
    }

    fn app_state(connected: bool) -> Arc<AppState> {
        Arc::new(AppState {
            chat: ChatState::new(999),
            token: "secret".into(),
            started: Instant::now(),
            connected: Arc::new(std::sync::atomic::AtomicBool::new(connected)),
            port: 18889,
            self_uid: 999,
            // 单测里没有 DLL：默认 chat，控制管道未连接。
            packets: PacketControl::new(PacketMode::Chat, None),
            session: crate::SessionStatus::new(),
            dll_health: crate::dll_health::HealthState::with_defaults(),
            boot_id: 12345,
        })
    }

    #[tokio::test]
    async fn debug_endpoints_require_token_and_expose_metrics() {
        let request = || {
            axum::http::Request::builder()
                .uri("/debug/metrics")
                .body(axum::body::Body::empty())
                .unwrap()
        };
        // 未带令牌：拒绝，且不泄露任何计数。
        let response = router(app_state(true)).oneshot(request()).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        // 带令牌：返回完整快照，至少包含"活着/慢在哪"的关键字段。
        let authorized = || {
            axum::http::Request::builder()
                .uri("/debug/metrics")
                .header("x-miliastra-token", "secret")
                .body(axum::body::Body::empty())
                .unwrap()
        };
        let response = router(app_state(true)).oneshot(authorized()).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(value["pipe"]["framesPerSec"].is_number());
        assert!(value["pipe"]["ackP99Us"].is_number());
        assert!(value["pipe"]["bufferedBytes"].is_number());
        assert!(value["chat"]["messagesTotal"].is_number());
        assert_eq!(value["service"]["port"], 18889);
        assert_eq!(value["service"]["selfUid"], 999);
        assert!(value["build"]["version"].is_string());

        // 事件接口同样要鉴权，并给出数组。
        let events = axum::http::Request::builder()
            .uri("/debug/events")
            .header("x-miliastra-token", "secret")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = router(app_state(true)).oneshot(events).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(value["events"].is_array());
    }

    async fn get_health(state: Arc<AppState>) -> serde_json::Value {
        let request = axum::http::Request::builder()
            .uri("/health")
            .header("x-miliastra-token", "secret")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = router(state).oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        json_body(response).await
    }

    #[tokio::test]
    async fn health_reports_attach_and_reattach_state() {
        let state = app_state(false);

        // 初始：未接入、未等待、0 轮。
        let value = get_health(Arc::clone(&state)).await;
        assert_eq!(value["attached"], false);
        assert_eq!(value["reattachWaiting"], false);
        assert_eq!(value["reattachAttempts"], 0);

        // 进入 reattach 等待：等待中、轮数 +1。
        state.session.mark_reattach_waiting();
        let value = get_health(Arc::clone(&state)).await;
        assert_eq!(value["attached"], false);
        assert_eq!(value["reattachWaiting"], true);
        assert_eq!(value["reattachAttempts"], 1);

        // DLL 重连成功：已接入、不再等待，轮数保留。
        state.session.mark_attached();
        let value = get_health(Arc::clone(&state)).await;
        assert_eq!(value["attached"], true);
        assert_eq!(value["reattachWaiting"], false);
        assert_eq!(value["reattachAttempts"], 1);

        // 再次断开：第二轮等待，轮数累加，上游可据此看出反复重连。
        state.session.mark_reattach_waiting();
        let value = get_health(Arc::clone(&state)).await;
        assert_eq!(value["reattachWaiting"], true);
        assert_eq!(value["reattachAttempts"], 2);
    }

    #[tokio::test]
    async fn health_reports_dll_unknown_without_heartbeat() {
        let state = app_state(false);
        let value = get_health(Arc::clone(&state)).await;
        assert_eq!(value["dll"]["state"], "unknown");
        assert_eq!(value["dll"]["alive"], false);
        assert!(value["dll"]["heartbeatAgeMs"].is_null());
        assert_eq!(value["dll"]["forwarding"], false);
        assert_eq!(value["dll"]["reconnecting"], false);
        assert_eq!(value["dll"]["debugDisabled"], false);
        assert_eq!(value["dll"]["abandonedLimitReached"], false);
        assert_eq!(value["dll"]["timeoutCount"], 0);
        assert_eq!(value["dll"]["abandonedTasks"], 0);
        assert_eq!(value["dll"]["seq"], 0);
        assert_eq!(value["dll"]["parseErrors"], 0);
    }

    #[tokio::test]
    async fn health_reports_dll_heartbeat_forwarding() {
        let state = app_state(false);
        state
            .dll_health
            .note_heartbeat(crate::health::HealthFrame::new(77, 0, 4, 1), Instant::now());
        let value = get_health(Arc::clone(&state)).await;
        assert_eq!(value["dll"]["state"], "forwarding");
        assert_eq!(value["dll"]["alive"], true);
        assert!(value["dll"]["heartbeatAgeMs"].is_number());
        assert_eq!(value["dll"]["forwarding"], true);
        assert_eq!(value["dll"]["reconnecting"], false);
        assert_eq!(value["dll"]["seq"], 77);
        assert_eq!(value["dll"]["timeoutCount"], 4);
        assert_eq!(value["dll"]["abandonedTasks"], 1);
    }

    #[tokio::test]
    async fn health_reports_dll_silent_and_parse_errors() {
        let state = app_state(false);
        // 最近一次心跳是 6s 前：默认 silent=5s -> silent；flags 仍可读。
        let frame =
            crate::health::HealthFrame::new(9, crate::health::FLAG_PACKET_OUTBOX_DISABLED, 2, 0);
        state
            .dll_health
            .note_heartbeat(frame, Instant::now() - Duration::from_secs(6));
        state.dll_health.note_parse_error();
        let value = get_health(Arc::clone(&state)).await;
        assert_eq!(value["dll"]["state"], "silent");
        assert_eq!(value["dll"]["alive"], false);
        assert_eq!(value["dll"]["forwarding"], false);
        assert_eq!(value["dll"]["parseErrors"], 1);
        assert!(value["dll"]["heartbeatAgeMs"].as_u64().unwrap() >= 6000);
    }

    #[tokio::test]
    async fn health_dll_resets_to_unknown_after_reattach() {
        let state = app_state(false);
        state
            .dll_health
            .note_heartbeat(crate::health::HealthFrame::new(1, 0, 0, 0), Instant::now());
        let value = get_health(Arc::clone(&state)).await;
        assert_eq!(value["dll"]["state"], "forwarding");
        // AuxPipes 在 reattach 重建时调用 reset_session：回到 unknown 直到首个心跳。
        state.dll_health.reset_session();
        let value = get_health(Arc::clone(&state)).await;
        assert_eq!(value["dll"]["state"], "unknown");
        assert!(value["dll"]["heartbeatAgeMs"].is_null());
        assert_eq!(value["dll"]["alive"], false);
    }

    #[tokio::test]
    async fn health_reports_stable_boot_id() {
        // bootId 由 main 在进程启动时生成一次；同一 AppState 的多次查询必须一致，
        // 否则上游会误判宿主反复重启、不断重置游标。
        let state = app_state(false);
        let first = get_health(Arc::clone(&state)).await;
        let second = get_health(Arc::clone(&state)).await;
        assert_eq!(first["bootId"], 12345);
        assert_eq!(second["bootId"], 12345);
        assert_eq!(first["bootId"], second["bootId"]);
    }

    /// 构造 /debug/packets 请求：GET 无 body，POST 可带 JSON。
    fn packets_request(
        method: &str,
        body: Option<&str>,
        token: Option<&str>,
    ) -> axum::http::Request<axum::body::Body> {
        let mut builder = axum::http::Request::builder()
            .method(method)
            .uri("/debug/packets");
        if let Some(token) = token {
            builder = builder.header("x-miliastra-token", token);
        }
        if body.is_some() {
            builder = builder.header("content-type", "application/json");
        }
        builder
            .body(match body {
                Some(body) => axum::body::Body::from(body.to_string()),
                None => axum::body::Body::empty(),
            })
            .unwrap()
    }

    async fn json_body(response: Response) -> serde_json::Value {
        let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        serde_json::from_slice(&body).unwrap()
    }

    /// 构造任意路径的 JSON 请求（/debug/capture 用）。
    fn json_request(
        method: &str,
        uri: &str,
        body: Option<&str>,
        token: Option<&str>,
    ) -> axum::http::Request<axum::body::Body> {
        let mut builder = axum::http::Request::builder().method(method).uri(uri);
        if let Some(token) = token {
            builder = builder.header("x-miliastra-token", token);
        }
        if body.is_some() {
            builder = builder.header("content-type", "application/json");
        }
        builder
            .body(match body {
                Some(body) => axum::body::Body::from(body.to_string()),
                None => axum::body::Body::empty(),
            })
            .unwrap()
    }

    #[tokio::test]
    async fn debug_capture_requires_token() {
        let state = app_state(true);
        let app = router(state);
        for method in ["GET", "POST"] {
            let body = (method == "POST").then_some(r#"{"enabled":true}"#);
            let response = app
                .clone()
                .oneshot(json_request(method, "/debug/capture", body, None))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED, "{method}");
        }
    }

    #[tokio::test]
    async fn debug_capture_starts_and_stops_without_restart() {
        // 这条用例守护的核心诉求：抓包必须能在运行期打开，不用为了带目录重启进程。
        let state = app_state(true);
        state.packets.set_auto_capture(false);
        let app = router(state.clone());
        let dir = std::env::temp_dir().join("mwc-capture-api-test");
        let _ = std::fs::remove_dir_all(&dir);
        let body = format!(
            r#"{{"enabled":true,"dir":{},"maxMib":1}}"#,
            serde_json::to_string(&dir.display().to_string()).expect("路径转字符串")
        );
        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/debug/capture",
                Some(&body),
                Some("secret"),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let value = json_body(response).await;
        assert_eq!(value["ok"], true);
        assert_eq!(value["capture"]["active"], true);
        assert!(dir.exists(), "抓包目录应被创建");

        // GET 反映正在抓包。
        let response = app
            .clone()
            .oneshot(json_request("GET", "/debug/capture", None, Some("secret")))
            .await
            .unwrap();
        assert_eq!(json_body(response).await["active"], true);

        // 停止后线程退出、目录仍在但不再写。
        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/debug/capture",
                Some(r#"{"enabled":false}"#),
                Some("secret"),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(json_body(response).await["capture"]["stopped"], true);
        let response = app
            .oneshot(json_request("GET", "/debug/capture", None, Some("secret")))
            .await
            .unwrap();
        assert_eq!(json_body(response).await["active"], false);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn debug_packets_require_token() {
        for method in ["GET", "POST"] {
            let body = (method == "POST").then_some(r#"{"mode":"chat"}"#);
            let response = router(app_state(true))
                .oneshot(packets_request(method, body, None))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED, "{method}");
            assert_eq!(response.headers()["content-type"], "application/json");
        }
    }

    #[tokio::test]
    async fn debug_packets_get_reports_chat_mode_without_dll() {
        let response = router(app_state(true))
            .oneshot(packets_request("GET", None, Some("secret")))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let value = json_body(response).await;
        assert_eq!(value["mode"], "chat");
        assert_eq!(value["dllControl"], false);
    }

    #[tokio::test]
    async fn debug_packets_post_switches_mode_and_reports_applied() {
        let state = app_state(true);
        // 测试里别真的往磁盘写抓包。
        state.packets.set_auto_capture(false);
        let app = router(state.clone());
        // 同路由注册 POST，不能被全局 method_not_allowed 兜底抢成 405。
        let response = app
            .clone()
            .oneshot(packets_request(
                "POST",
                Some(r#"{"mode":"all"}"#),
                Some("secret"),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let value = json_body(response).await;
        assert_eq!(value["ok"], true);
        assert_eq!(value["mode"], "all");
        assert_eq!(value["applied"], false);
        assert_eq!(value["dllControl"], false);
        // 自动抓包被关掉时不建目录、不报错。
        assert_eq!(value["captureActive"], false);
        assert!(value["captureDir"].is_null());
        // DLL 未连接时只在宿主侧记录模式：GET 立即反映 all，且不报错。
        let response = app
            .clone()
            .oneshot(packets_request("GET", None, Some("secret")))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(json_body(response).await["mode"], "all");
        // 切回 chat：模式是运行期状态，不持久化。
        let response = app
            .oneshot(packets_request(
                "POST",
                Some(r#"{"mode":"chat"}"#),
                Some("secret"),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let value = json_body(response).await;
        assert_eq!(value["mode"], "chat");
        assert_eq!(value["applied"], false);
    }

    #[tokio::test]
    async fn debug_packets_post_rejects_bad_mode_and_bad_json() {
        let app = router(app_state(true));
        // 合法 JSON 但模式不认识：固定错误消息，方便调用方排查。
        for body in [
            r#"{"mode":"everything"}"#,
            r#"{"mode":""}"#,
            r#"{"mode":"CHAT"}"#,
        ] {
            let response = app
                .clone()
                .oneshot(packets_request("POST", Some(body), Some("secret")))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{body}");
            assert_eq!(response.headers()["content-type"], "application/json");
            let value = json_body(response).await;
            assert_eq!(value["error"]["code"], "bad_request");
            assert_eq!(value["error"]["message"], "mode must be chat or all");
        }
        // 解析失败/缺字段/类型不对：也是 400 ApiError，不泄露内部细节。
        for body in ["not json", r#"{}"#, r#"{"mode":42}"#] {
            let response = app
                .clone()
                .oneshot(packets_request("POST", Some(body), Some("secret")))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{body}");
            assert_eq!(response.headers()["content-type"], "application/json");
            let value = json_body(response).await;
            assert_eq!(value["error"]["code"], "bad_request");
            assert!(value["error"]["message"].is_string());
        }
    }

    #[tokio::test]
    async fn names_endpoint_adds_remarks_without_changing_names() {
        let state = app_state(true);
        state
            .chat
            .record_friend_profiles(vec![
                crate::extract::FriendProfile {
                    uid: 100000001,
                    name: Some("甲玩家".into()),
                    remark: Some("rmkAAA1".into()),
                },
                crate::extract::FriendProfile {
                    uid: 100000002,
                    name: Some("乙玩家".into()),
                    remark: None,
                },
            ])
            .await;
        let request = axum::http::Request::builder()
            .uri("/names?uids=100000001,100000002")
            .header("x-miliastra-token", "secret")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = router(state).oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let value = json_body(response).await;
        // names 字段保持原语义，老客户端照旧可用。
        assert_eq!(value["names"]["100000001"], "甲玩家");
        assert_eq!(value["names"]["100000002"], "乙玩家");
        // remarks 是新增字段，且只列有备注的 UID。
        assert_eq!(value["remarks"]["100000001"], "rmkAAA1");
        assert!(value["remarks"].get("100000002").is_none());
    }

    fn private_message() -> crate::extract::RawChat {
        private_message_text("hello")
    }

    /// 内容不同的私聊消息。压广播队列的用例要靠文本区分，
    /// 否则会被 state 的重放去重（同一 发送者+时间戳+文本）当成重复丢掉。
    fn private_message_text(text: &str) -> crate::extract::RawChat {
        crate::extract::RawChat {
            // 必须是「现在」，否则会被历史重放过滤掉（见 state::STALE_GRACE_SECS）。
            ts: crate::state::now_unix_secs(),
            from_uid: 1,
            to_uid: Some(999),
            text: text.into(),
            hall_id: None,
            hall_type: None,
            is_private: true,
        }
    }

    type MockConnection = (
        MockSocket,
        mpsc::UnboundedSender<Result<Message, ()>>,
        mpsc::UnboundedReceiver<Message>,
        oneshot::Sender<()>,
    );

    fn mock_socket() -> MockConnection {
        let (input, incoming) = mpsc::unbounded_channel();
        let (outgoing, output) = mpsc::unbounded_channel();
        let (release, gate) = oneshot::channel();
        (
            MockSocket {
                incoming,
                outgoing,
                flush_gate: Some(gate),
            },
            input,
            output,
            release,
        )
    }

    #[tokio::test]
    async fn events_during_hello_send_are_delivered() {
        let state = app_state(true);
        let (socket, _input, mut output, release) = mock_socket();
        let task = tokio::spawn(handle_socket(socket, state.clone()));
        let Message::Text(hello) = output.recv().await.unwrap() else {
            panic!("expected hello")
        };
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&hello).unwrap()["lastMessageId"],
            0
        );
        state.chat.ingest(vec![private_message()]).await;
        release.send(()).unwrap();
        let Message::Text(event) = output.recv().await.unwrap() else {
            panic!("expected message")
        };
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&event).unwrap()["message"]["id"],
            1
        );
        task.abort();
        let _ = task.await;
    }

    /// 连接 mock socket，只取第一条 hello 帧（其余事件不消费）。
    async fn read_hello(state: Arc<AppState>) -> serde_json::Value {
        let (socket, _input, mut output, _release) = mock_socket();
        let task = tokio::spawn(handle_socket(socket, state));
        let Message::Text(text) = output.recv().await.unwrap() else {
            panic!("expected hello")
        };
        task.abort();
        let _ = task.await;
        serde_json::from_str(&text).unwrap()
    }

    #[tokio::test]
    async fn hello_includes_boot_id_and_session_state() {
        let state = app_state(false);

        // 等待 DLL 重连：未接入、gameConnected=false、reattachWaiting=true。
        state.session.mark_reattach_waiting();
        let hello = read_hello(Arc::clone(&state)).await;
        assert_eq!(hello["type"], "hello");
        assert_eq!(hello["bootId"], 12345);
        assert_eq!(hello["bootId"], state.boot_id);
        assert_eq!(hello["gameConnected"], false);
        assert_eq!(hello["attached"], false);
        assert_eq!(hello["reattachWaiting"], true);
        assert_eq!(hello["lastMessageId"], 0);
        assert!(hello["version"].is_string());

        // 与 /health 的 bootId 必须一致：两处都读同一个 AppState 字段。
        let health = get_health(Arc::clone(&state)).await;
        assert_eq!(hello["bootId"], health["bootId"]);

        // 重连成功：已接入、不再等待、gameConnected=true。
        state
            .connected
            .store(true, std::sync::atomic::Ordering::Relaxed);
        state.session.mark_attached();
        let hello = read_hello(Arc::clone(&state)).await;
        assert_eq!(hello["gameConnected"], true);
        assert_eq!(hello["attached"], true);
        assert_eq!(hello["reattachWaiting"], false);
    }

    #[tokio::test]
    async fn lagged_stream_closes_instead_of_silently_skipping() {
        let state = app_state(true);
        let (socket, _input, mut output, release) = mock_socket();
        let task = tokio::spawn(handle_socket(socket, state.clone()));
        output.recv().await.unwrap();
        state
            .chat
            .ingest(
                (0..crate::state::BROADCAST_CAPACITY * 2)
                    .map(|index| private_message_text(&format!("m{index}")))
                    .collect::<Vec<_>>(),
            )
            .await;
        release.send(()).unwrap();
        let Message::Close(Some(frame)) = output.recv().await.unwrap() else {
            panic!("expected close")
        };
        assert_eq!(frame.code, 1013);
        assert!(frame.reason.contains("reconnect"));
        task.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn heartbeat_pings_at_30_seconds_and_closes_after_10_without_pong() {
        let (socket, input, mut output, release) = mock_socket();
        release.send(()).unwrap();
        let task = tokio::spawn(handle_socket(socket, app_state(true)));
        output.recv().await.unwrap();
        tokio::time::advance(Duration::from_secs(29)).await;
        assert!(output.try_recv().is_err());
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(matches!(output.recv().await.unwrap(), Message::Ping(_)));
        // 文本和不匹配的 Pong 都不能延长当前探测的截止时间。
        input.send(Ok(Message::Text("active".into()))).unwrap();
        input.send(Ok(Message::Pong(bytes::Bytes::new()))).unwrap();
        tokio::time::advance(Duration::from_secs(9)).await;
        assert!(output.try_recv().is_err());
        tokio::time::advance(Duration::from_secs(1)).await;
        let Message::Close(Some(frame)) = output.recv().await.unwrap() else {
            panic!("expected close")
        };
        assert_eq!(frame.reason, "pong timeout");
        task.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn matching_pong_keeps_stream_alive_for_next_ping() {
        let (socket, input, mut output, release) = mock_socket();
        release.send(()).unwrap();
        let task = tokio::spawn(handle_socket(socket, app_state(true)));
        output.recv().await.unwrap();
        tokio::time::advance(PING_INTERVAL).await;
        let Message::Ping(payload) = output.recv().await.unwrap() else {
            panic!("expected ping")
        };
        input.send(Ok(Message::Pong(payload))).unwrap();
        tokio::task::yield_now().await;
        tokio::time::advance(PING_INTERVAL).await;
        assert!(matches!(output.recv().await.unwrap(), Message::Ping(_)));
        input.send(Ok(Message::Close(None))).unwrap();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn query_and_missing_route_errors_are_json() {
        let app = router(app_state(true));
        for (uri, status, code) in [
            (
                "/messages?since=nope",
                StatusCode::BAD_REQUEST,
                "bad_request",
            ),
            ("/messages?limit=-1", StatusCode::BAD_REQUEST, "bad_request"),
            ("/names", StatusCode::BAD_REQUEST, "bad_request"),
            (
                "/stream?token=a&token=b",
                StatusCode::BAD_REQUEST,
                "bad_request",
            ),
            ("/names?uids=abc", StatusCode::BAD_REQUEST, "bad_request"),
            ("/absent", StatusCode::NOT_FOUND, "not_found"),
        ] {
            let request = axum::http::Request::builder()
                .uri(uri)
                .header("x-miliastra-token", "secret")
                .body(axum::body::Body::empty())
                .unwrap();
            let response = app.clone().oneshot(request).await.unwrap();
            assert_eq!(response.status(), status, "{uri}");
            assert_eq!(response.headers()["content-type"], "application/json");
            let body = axum::body::to_bytes(response.into_body(), 4096)
                .await
                .unwrap();
            let error: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(error["error"]["code"], code);
            assert!(error["error"]["message"].is_string());
        }
    }

    #[tokio::test]
    async fn unsupported_method_returns_json_and_preserves_allow_header() {
        let request = axum::http::Request::builder()
            .method("POST")
            .uri("/messages")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = router(app_state(true)).oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
        assert!(
            response.headers()["allow"]
                .to_str()
                .unwrap()
                .contains("GET")
        );
        assert_eq!(response.headers()["content-type"], "application/json");
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let error: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(error["error"]["code"], "method_not_allowed");
    }

    #[tokio::test]
    async fn disconnected_names_and_messages_return_503_but_health_stays_available() {
        let app = router(app_state(false));
        for (uri, status) in [
            ("/names?uids=1", StatusCode::SERVICE_UNAVAILABLE),
            ("/messages", StatusCode::SERVICE_UNAVAILABLE),
            ("/health", StatusCode::OK),
        ] {
            let request = axum::http::Request::builder()
                .uri(uri)
                .header("x-miliastra-token", "secret")
                .body(axum::body::Body::empty())
                .unwrap();
            assert_eq!(app.clone().oneshot(request).await.unwrap().status(), status);
        }
        let request = axum::http::Request::builder()
            .uri("/health")
            .body(axum::body::Body::empty())
            .unwrap();
        assert_eq!(
            app.oneshot(request).await.unwrap().status(),
            StatusCode::UNAUTHORIZED
        );
    }

    #[tokio::test]
    async fn observer_page_is_served_without_token() {
        let response = router(app_state(false))
            .oneshot(
                axum::http::Request::builder()
                    .uri("/")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()["content-type"],
            "text/html; charset=utf-8"
        );
        assert_eq!(response.headers()["cache-control"], "no-store");
        assert_eq!(response.headers()["x-content-type-options"], "nosniff");

        let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let html = std::str::from_utf8(&body).unwrap();
        // 页面契约：观察页必须带这些稳定标记，供使用与端到端验证依赖。
        for marker in [
            "id=\"status-ws\"",
            "id=\"token-input\"",
            "id=\"filter-channel\"",
            "id=\"messages\"",
        ] {
            assert!(html.contains(marker), "观察页缺少标记: {marker}");
        }
        // 静态外壳不得携带服务端令牌。
        assert!(!html.contains("secret"), "观察页不应包含令牌");
    }

    #[tokio::test]
    async fn favicon_is_no_content() {
        let response = router(app_state(false))
            .oneshot(
                axum::http::Request::builder()
                    .uri("/favicon.ico")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert!(response.headers().get("content-type").is_none());
    }

    #[test]
    fn token_comparison_is_exact() {
        assert!(token_matches("abc", "abc"));
        assert!(!token_matches("abc", "abd"));
        assert!(!token_matches("abc", "ab"));
        assert!(!token_matches("abc", "abcd"));
        assert!(!token_matches("abc", ""));
    }
    #[tokio::test]
    async fn friends_endpoint_returns_full_snapshot() {
        use crate::extract::FriendProfile;
        let state = app_state(true);
        state
            .chat
            .record_friend_profiles(vec![
                FriendProfile {
                    uid: 100000001,
                    name: Some("甲".into()),
                    remark: Some("备注甲".into()),
                },
                FriendProfile {
                    uid: 100000002,
                    name: Some("乙".into()),
                    remark: None,
                },
                FriendProfile {
                    uid: 100000003,
                    name: Some("丙".into()),
                    remark: None,
                },
                FriendProfile {
                    uid: 100000004,
                    name: Some("丁".into()),
                    remark: None,
                },
            ])
            .await;
        let app = router(state);
        let response = app
            .clone()
            .oneshot(json_request("GET", "/friends", None, Some("secret")))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let value = json_body(response).await;
        assert_eq!(value["ok"], true);
        assert_eq!(value["complete"], true);
        assert_eq!(value["count"], 4);
        let friends = value["friends"].as_array().unwrap();
        assert_eq!(friends.len(), 4);
        let first = friends.iter().find(|f| f["uid"] == 100000001).unwrap();
        assert_eq!(first["name"], "甲");
        assert_eq!(first["remark"], "备注甲");
        let second = friends.iter().find(|f| f["uid"] == 100000002).unwrap();
        assert!(second["remark"].is_null());

        // 令牌是硬要求：缺失即 401。
        let response = app
            .oneshot(json_request("GET", "/friends", None, None))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }
}
