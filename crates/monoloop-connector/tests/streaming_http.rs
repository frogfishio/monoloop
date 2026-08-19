//! WP-08: generic StreamingHttpConnector against a local scripted server.

use axum::body::Body;
use axum::extract::Request;
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use axum::routing::{get, post};
use axum::Router;
use bytes::Bytes;
use monoloop_connector::{
    validate_endpoint_url, CancellationReason, ConnectionEndKind, Connector, ConnectorErrorKind,
    ConnectorLimits, MapCredentialResolver, OpenConnection, StreamingHttpConfig,
    StreamingHttpConnector, TerminationReason, TransportBufferLimits,
};
use monoloop_contracts::ConnectionId;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

fn drive_owner(opened: &mut monoloop_connector::OpenedRawConnection) {
    if let Some(work) = opened.take_owner_work() {
        tokio::spawn(work.into_future());
    }
}

async fn bind_router(app: Router) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let join = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    // Brief settle for accept path.
    tokio::time::sleep(Duration::from_millis(10)).await;
    (addr, join)
}

fn connector(
    credentials: Arc<dyn monoloop_connector::CredentialResolver>,
    mut config: StreamingHttpConfig,
) -> StreamingHttpConnector {
    config.require_https = false;
    StreamingHttpConnector::try_new(config, credentials).unwrap()
}

async fn open_and_send(
    c: &StreamingHttpConnector,
    url: String,
    body: &[u8],
    credential_ref: Option<&str>,
    limits: ConnectorLimits,
) -> monoloop_connector::OpenedRawConnection {
    let mut open = OpenConnection::new(ConnectionId::generate(), url);
    open.credential_ref = credential_ref.map(|s| s.to_string());
    open.limits = limits;
    let pending = c.begin_open(open);
    let mut opened = pending.opened.await.unwrap();
    drive_owner(&mut opened);
    if !body.is_empty() {
        opened.input.send(Bytes::from(body.to_vec())).await.unwrap();
    }
    opened.input.finish().await.unwrap();
    opened
}

async fn read_all(opened: monoloop_connector::OpenedRawConnection) -> (Vec<u8>, ConnectionEndKind) {
    let mut collected = Vec::new();
    loop {
        match opened.output.receive().await {
            Ok(Some(chunk)) => collected.extend_from_slice(&chunk),
            Ok(None) => break,
            Err(_) => break,
        }
    }
    let end = opened.completion.wait().await;
    (collected, end.kind)
}

#[test]
fn endpoint_validation() {
    assert!(validate_endpoint_url("http://127.0.0.1:9/v1", false).is_ok());
    assert!(validate_endpoint_url("https://example.com/v1", true).is_ok());
    assert!(validate_endpoint_url("http://example.com/v1", true).is_err());
    assert!(validate_endpoint_url("http://user:pass@example.com/", false).is_err());
    assert!(validate_endpoint_url("ftp://example.com/", false).is_err());
    assert!(validate_endpoint_url("", false).is_err());
    assert!(validate_endpoint_url("not a url", false).is_err());
}

#[tokio::test]
async fn fragmented_sse_body_round_trip() {
    let app = Router::new().route(
        "/v1/stream",
        post(|req: Request| async move {
            let body = axum::body::to_bytes(req.into_body(), 1024 * 1024)
                .await
                .unwrap();
            assert_eq!(&body[..], b"{\"hello\":1}");
            // Fragmented SSE-looking body (connector must not parse it).
            let chunks = [
                Bytes::from_static(b"data: {\"a\":"),
                Bytes::from_static(b"1}\n\n"),
                Bytes::from_static(b"data: [DONE]\n\n"),
            ];
            let stream =
                futures_util::stream::iter(chunks.into_iter().map(Ok::<_, std::io::Error>));
            Response::builder()
                .status(StatusCode::OK)
                .body(Body::from_stream(stream))
                .unwrap()
        }),
    );
    let (addr, _join) = bind_router(app).await;
    let url = format!("http://{addr}/v1/stream");
    let c = connector(
        Arc::new(MapCredentialResolver::empty()),
        StreamingHttpConfig::default(),
    );
    let opened = open_and_send(&c, url, b"{\"hello\":1}", None, ConnectorLimits::default()).await;
    let (body, kind) = read_all(opened).await;
    assert_eq!(kind, ConnectionEndKind::RemoteEof);
    assert_eq!(
        String::from_utf8_lossy(&body),
        "data: {\"a\":1}\n\ndata: [DONE]\n\n"
    );
}

#[tokio::test]
async fn non_success_status_bounded_error() {
    let app = Router::new().route(
        "/fail",
        post(|| async {
            (
                StatusCode::UNAUTHORIZED,
                "secret-token-must-not-leak-into-connector-error",
            )
        }),
    );
    let (addr, _join) = bind_router(app).await;
    let c = connector(
        Arc::new(MapCredentialResolver::empty()),
        StreamingHttpConfig::default(),
    );
    let opened = open_and_send(
        &c,
        format!("http://{addr}/fail"),
        b"{}",
        None,
        ConnectorLimits::default(),
    )
    .await;
    let end = opened.completion.wait().await;
    assert_eq!(end.kind, ConnectionEndKind::TransportFailure);
    let msg = end.safe_transport_error.unwrap_or_default();
    assert!(msg.contains("401") || msg.contains("http status"));
    assert!(!msg.contains("secret-token"));
}

#[tokio::test]
async fn credential_resolution_and_header() {
    let saw_auth = Arc::new(AtomicUsize::new(0));
    let saw = Arc::clone(&saw_auth);
    let app = Router::new().route(
        "/auth",
        post(move |headers: HeaderMap| {
            let saw = Arc::clone(&saw);
            async move {
                if headers
                    .get(axum::http::header::AUTHORIZATION)
                    .and_then(|v| v.to_str().ok())
                    == Some("Bearer test-secret")
                {
                    saw.store(1, Ordering::SeqCst);
                }
                StatusCode::OK
            }
        }),
    );
    let (addr, _join) = bind_router(app).await;
    let resolver = Arc::new(MapCredentialResolver::new([(
        "cred-a",
        "Bearer test-secret",
    )]));
    let c = connector(resolver, StreamingHttpConfig::default());
    let opened = open_and_send(
        &c,
        format!("http://{addr}/auth"),
        b"{}",
        Some("cred-a"),
        ConnectorLimits::default(),
    )
    .await;
    let end = opened.completion.wait().await;
    assert_eq!(end.kind, ConnectionEndKind::RemoteEof);
    assert_eq!(saw_auth.load(Ordering::SeqCst), 1);

    // Missing credential ref fails at open.
    let mut open = OpenConnection::new(ConnectionId::generate(), format!("http://{addr}/auth"));
    open.credential_ref = Some("missing".into());
    let pending = c.begin_open(open);
    let err = match pending.opened.await {
        Err(e) => e,
        Ok(_) => panic!("expected credential failure"),
    };
    assert_eq!(err.kind, ConnectorErrorKind::CredentialUnavailable);
    assert!(!format!("{err:?}").contains("test-secret"));
}

#[tokio::test]
async fn secrets_absent_from_debug() {
    let resolver = MapCredentialResolver::new([("k", "super-secret-value")]);
    let c = connector(Arc::new(resolver), StreamingHttpConfig::default());
    let dbg = format!("{c:?}");
    assert!(!dbg.contains("super-secret-value"));
    assert!(dbg.contains("<injected>") || dbg.contains("StreamingHttpConnector"));
}

#[tokio::test]
async fn max_request_bytes_plus_one() {
    let app = Router::new().route("/ok", post(|| async { StatusCode::OK }));
    let (addr, _join) = bind_router(app).await;
    let config = StreamingHttpConfig {
        max_request_bytes: 8,
        ..Default::default()
    };
    let c = connector(Arc::new(MapCredentialResolver::empty()), config);
    let mut open = OpenConnection::new(ConnectionId::generate(), format!("http://{addr}/ok"));
    open.limits.buffers = TransportBufferLimits {
        max_queued_input_bytes: 1024,
        max_queued_output_bytes: 1024,
        max_chunk_bytes: 64,
    };
    let pending = c.begin_open(open);
    let mut opened = pending.opened.await.unwrap();
    drive_owner(&mut opened);
    opened.input.send(Bytes::from(vec![b'x'; 9])).await.unwrap();
    opened.input.finish().await.unwrap();
    let end = opened.completion.wait().await;
    assert_eq!(end.kind, ConnectionEndKind::TransportFailure);
    assert!(end
        .safe_transport_error
        .unwrap_or_default()
        .contains("max_request_bytes"));
}

#[tokio::test]
async fn max_response_bytes_plus_one() {
    let app = Router::new().route(
        "/big",
        post(|| async { "0123456789abcdef" }), // 16 bytes
    );
    let (addr, _join) = bind_router(app).await;
    let config = StreamingHttpConfig {
        max_response_bytes: 8,
        ..Default::default()
    };
    let c = connector(Arc::new(MapCredentialResolver::empty()), config);
    let opened = open_and_send(
        &c,
        format!("http://{addr}/big"),
        b"{}",
        None,
        ConnectorLimits::default(),
    )
    .await;
    let end = opened.completion.wait().await;
    assert_eq!(end.kind, ConnectionEndKind::TransportFailure);
    assert!(end
        .safe_transport_error
        .unwrap_or_default()
        .contains("max_response_bytes"));
}

#[tokio::test]
async fn cancel_while_request_in_flight() {
    let app = Router::new().route(
        "/ok",
        post(|| async {
            tokio::time::sleep(Duration::from_secs(5)).await;
            StatusCode::OK
        }),
    );
    let (addr, _join) = bind_router(app).await;
    let c = connector(
        Arc::new(MapCredentialResolver::empty()),
        StreamingHttpConfig::default(),
    );
    let mut open = OpenConnection::new(ConnectionId::generate(), format!("http://{addr}/ok"));
    open.limits.connect_deadline = Duration::from_secs(10);
    let pending = c.begin_open(open);
    let control = pending.control.clone();
    let mut opened = pending.opened.await.unwrap();
    drive_owner(&mut opened);
    opened.input.send(Bytes::from_static(b"{}")).await.unwrap();
    opened.input.finish().await.unwrap();
    // Cancel while HTTP request is in flight.
    tokio::time::sleep(Duration::from_millis(20)).await;
    control.cancel(CancellationReason::CallerRequested);
    let end = opened.completion.wait().await;
    assert!(
        matches!(
            end.kind,
            ConnectionEndKind::Cancelled | ConnectionEndKind::Terminated
        ),
        "got {:?}",
        end.kind
    );
}

#[tokio::test]
async fn cancel_before_request_send() {
    let app = Router::new().route("/ok", post(|| async { StatusCode::OK }));
    let (addr, _join) = bind_router(app).await;
    let c = connector(
        Arc::new(MapCredentialResolver::empty()),
        StreamingHttpConfig::default(),
    );
    let pending = c.begin_open(OpenConnection::new(
        ConnectionId::generate(),
        format!("http://{addr}/ok"),
    ));
    let control = pending.control.clone();
    let mut opened = pending.opened.await.unwrap();
    drive_owner(&mut opened);
    // Cancel while waiting for body finish.
    control.cancel(CancellationReason::CallerRequested);
    let end = opened.completion.wait().await;
    assert_eq!(end.kind, ConnectionEndKind::Cancelled);
}

#[tokio::test]
async fn terminate_during_open_collect() {
    let app = Router::new().route("/ok", post(|| async { StatusCode::OK }));
    let (addr, _join) = bind_router(app).await;
    let c = connector(
        Arc::new(MapCredentialResolver::empty()),
        StreamingHttpConfig::default(),
    );
    let pending = c.begin_open(OpenConnection::new(
        ConnectionId::generate(),
        format!("http://{addr}/ok"),
    ));
    let control = pending.control.clone();
    let mut opened = pending.opened.await.unwrap();
    drive_owner(&mut opened);
    control.terminate(TerminationReason::CallerForced);
    let end = opened.completion.wait().await;
    assert_eq!(end.kind, ConnectionEndKind::Terminated);
}

#[tokio::test]
async fn idle_timeout_on_stalled_stream() {
    let app = Router::new().route(
        "/stall",
        post(|| async {
            let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(2);
            tokio::spawn(async move {
                let _ = tx.send(Ok(Bytes::from_static(b"part"))).await;
                // Never send more — client idle timeout should fire.
                tokio::time::sleep(Duration::from_secs(30)).await;
            });
            // Use Stream without tokio-stream: unfold
            let stream = futures_util::stream::unfold(rx, |mut rx| async move {
                rx.recv().await.map(|item| (item, rx))
            });
            Response::builder()
                .status(StatusCode::OK)
                .body(Body::from_stream(stream))
                .unwrap()
        }),
    );
    let (addr, _join) = bind_router(app).await;
    let config = StreamingHttpConfig {
        idle_timeout: Duration::from_millis(100),
        request_timeout: Duration::from_secs(5),
        ..Default::default()
    };
    let c = connector(Arc::new(MapCredentialResolver::empty()), config);
    let opened = open_and_send(
        &c,
        format!("http://{addr}/stall"),
        b"{}",
        None,
        ConnectorLimits::default(),
    )
    .await;
    let end = opened.completion.wait().await;
    assert_eq!(end.kind, ConnectionEndKind::TransportFailure);
    assert!(end
        .safe_transport_error
        .unwrap_or_default()
        .contains("idle"));
}

#[tokio::test]
async fn connection_pool_reuse_no_semantic_session() {
    let hits = Arc::new(AtomicUsize::new(0));
    let hits2 = Arc::clone(&hits);
    let app = Router::new().route(
        "/n",
        post(move || {
            let hits2 = Arc::clone(&hits2);
            async move {
                hits2.fetch_add(1, Ordering::SeqCst);
                StatusCode::OK
            }
        }),
    );
    let (addr, _join) = bind_router(app).await;
    let c = connector(
        Arc::new(MapCredentialResolver::empty()),
        StreamingHttpConfig::default(),
    );
    for _ in 0..3 {
        let opened = open_and_send(
            &c,
            format!("http://{addr}/n"),
            b"{}",
            None,
            ConnectorLimits::default(),
        )
        .await;
        let end = opened.completion.wait().await;
        assert_eq!(end.kind, ConnectionEndKind::RemoteEof);
    }
    assert_eq!(hits.load(Ordering::SeqCst), 3);
    // Each open is an independent connection identity (no session reuse semantics).
}

#[tokio::test]
async fn malformed_endpoint_fails_open() {
    let c = connector(
        Arc::new(MapCredentialResolver::empty()),
        StreamingHttpConfig::default(),
    );
    let pending = c.begin_open(OpenConnection::new(ConnectionId::generate(), "not-a-url"));
    let err = match pending.opened.await {
        Err(e) => e,
        Ok(_) => panic!("expected config failure"),
    };
    assert_eq!(err.kind, ConnectorErrorKind::ConfigurationInvalid);
}

#[tokio::test]
async fn health_get_unused() {
    // Ensure bind helper works with GET too (sanity).
    let app = Router::new().route("/h", get(|| async { "ok" }));
    let (addr, join) = bind_router(app).await;
    let client = reqwest::Client::new();
    let r = client.get(format!("http://{addr}/h")).send().await.unwrap();
    assert_eq!(r.status(), StatusCode::OK);
    join.abort();
}
