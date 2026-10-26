//! HTTP + WebSocket 服务。

use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::{Sink, SinkExt, Stream, StreamExt};

use axum::extract::rejection::QueryRejection;
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
        .route("/stream", get(stream))
        .route("/debug/metrics", get(debug_metrics))
        .route("/debug/events", get(debug_events))
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
        "uptimeSec": state.started.elapsed().as_secs(),
        "gameConnected": state.connected.load(std::sync::atomic::Ordering::Relaxed),
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
    Json(json!({ "names": out })).into_response()
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

async fn handle_socket<S, E>(mut socket: S, state: Arc<AppState>)
where
    S: Stream<Item = Result<Message, E>> + Sink<Message> + Unpin,
{
    let _subscriber = crate::diag::subscriber();
    let (mut rx, last_message_id) = state.chat.subscribe_with_last_id().await;
    let hello = ServerEvent::Hello {
        version: env!("CARGO_PKG_VERSION"),
        last_message_id,
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

    fn private_message() -> crate::extract::RawChat {
        crate::extract::RawChat {
            ts: 1,
            from_uid: 1,
            to_uid: Some(999),
            text: "hello".into(),
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

    #[tokio::test]
    async fn lagged_stream_closes_instead_of_silently_skipping() {
        let state = app_state(true);
        let (socket, _input, mut output, release) = mock_socket();
        let task = tokio::spawn(handle_socket(socket, state.clone()));
        output.recv().await.unwrap();
        state
            .chat
            .ingest(vec![
                private_message();
                crate::state::BROADCAST_CAPACITY * 2
            ])
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
}
