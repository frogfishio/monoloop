//! Grok Build connector tests against the mock ACP server.

mod common;

use monoloop_connector::{
    CancellationReason, ConnectionId, Connector, ConnectorProxy, OpenConnection, ProxyRoute,
};
use monoloop_connector_grok::{
    EncodedAcpSessionMessage, GrokConnector, GrokServerConfig, GrokSessionConfig,
    GrokSessionLoadConfig, InMemorySecretResolver, RawDumpCollector, SecretRef,
};
use monoloop_contracts::GrokSessionId;
use std::sync::Arc;
use std::time::Duration;

fn drive_owner(opened: &mut monoloop_connector::OpenedRawConnection) {
    if let Some(work) = opened.take_owner_work() {
        tokio::spawn(work.into_future());
    }
}

#[tokio::test]
async fn initialize_session_new_and_prompt_roundtrip() {
    let secret = "test-secret-abc";
    let addr = common::mock_acp_server::start_mock_acp_server(secret).await;
    let secrets = Arc::new(InMemorySecretResolver::new());
    secrets.insert("GROK_WS_SECRET", secret);

    let connector = GrokConnector::new(secrets);
    let config =
        GrokServerConfig::loopback(addr.port(), SecretRef::new("GROK_WS_SECRET")).expect("config");
    let pending = connector.connect(config).expect("connect pending");
    let server = tokio::time::timeout(Duration::from_secs(5), pending.wait())
        .await
        .expect("timeout")
        .expect("server");

    let session = tokio::time::timeout(
        Duration::from_secs(5),
        server
            .sessions
            .begin_new(GrokSessionConfig {
                cwd: Some("/tmp".into()),
                ..Default::default()
            })
            .expect("begin_new")
            .opened,
    )
    .await
    .expect("timeout")
    .expect("channel")
    .expect("session");

    assert!(session.session_id.as_str().starts_with("sess-"));

    let exchange = session
        .input
        .begin_send(EncodedAcpSessionMessage {
            method: "session/prompt".into(),
            params: serde_json::json!({
                "prompt": [{ "type": "text", "text": "hi" }]
            }),
        })
        .expect("begin_send");

    let result = tokio::time::timeout(Duration::from_secs(5), exchange.response)
        .await
        .expect("timeout")
        .expect("channel")
        .expect("rpc");
    assert_eq!(result["stopReason"], "end_turn");

    let update = tokio::time::timeout(Duration::from_secs(2), session.output.receive())
        .await
        .expect("timeout")
        .expect("recv")
        .expect("bytes");
    let msg: serde_json::Value = serde_json::from_slice(&update).expect("json");
    assert_eq!(msg["method"], "session/update");
    assert_eq!(msg["params"]["sessionId"], session.session_id.as_str());
}

#[tokio::test]
async fn raw_dump_captures_exact_inbound_from_grok() {
    let secret = "raw-dump-secret";
    let addr = common::mock_acp_server::start_mock_acp_server(secret).await;
    let secrets = Arc::new(InMemorySecretResolver::new());
    secrets.insert("S", secret);

    let dump = Arc::new(RawDumpCollector::enabled());
    let connector = GrokConnector::new(secrets);
    let config = GrokServerConfig::loopback(addr.port(), SecretRef::new("S"))
        .unwrap()
        .with_raw_dump(Arc::clone(&dump));

    let server = connector
        .connect(config)
        .unwrap()
        .wait()
        .await
        .unwrap();

    let session = server
        .sessions
        .begin_new(GrokSessionConfig::default())
        .unwrap()
        .opened
        .await
        .unwrap()
        .unwrap();

    let exchange = session
        .input
        .begin_send(EncodedAcpSessionMessage {
            method: "session/prompt".into(),
            params: serde_json::json!({
                "prompt": [{ "type": "text", "text": "hi" }]
            }),
        })
        .unwrap();
    let _ = exchange.response.await.unwrap().unwrap();
    let _ = session.output.receive().await.unwrap().unwrap();

    // Allow demux path to finish recording.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let snap = dump.snapshot();
    assert!(
        snap.frames.len() >= 3,
        "expected initialize + session/new + session/update (+ prompt result); got {}\n{}",
        snap.frames.len(),
        snap.format_text()
    );

    // Exact expected markers from the mock ACP server (what Grok "sends us").
    assert!(
        snap.contains_str("protocolVersion") || snap.contains_str("agentCapabilities"),
        "missing initialize result in raw dump:\n{}",
        snap.format_text()
    );
    assert!(
        snap.contains_str("sessionId"),
        "missing sessionId in raw dump:\n{}",
        snap.format_text()
    );
    assert!(
        snap.contains_str("session/update"),
        "missing session/update in raw dump:\n{}",
        snap.format_text()
    );
    assert!(
        snap.contains_str("agent_message_chunk") || snap.contains_str("hello"),
        "missing agent message content in raw dump:\n{}",
        snap.format_text()
    );
    assert!(
        snap.contains_str("stopReason") || snap.contains_str("end_turn"),
        "missing prompt terminal result in raw dump:\n{}",
        snap.format_text()
    );

    // Frames are valid JSON-RPC objects (exact wire payloads, not re-encoded).
    let values = snap.json_values();
    assert!(
        values.len() >= 3,
        "could not parse frames as JSON: {}",
        snap.format_text()
    );
    assert!(
        values
            .iter()
            .any(|v| v.get("result").is_some() || v.get("method").is_some()),
        "unexpected JSON shapes: {values:?}"
    );

    // Handle exposes the same collector.
    assert!(server.raw_dump.is_some());
    assert_eq!(
        server.raw_dump.as_ref().unwrap().snapshot().frames.len(),
        snap.frames.len()
    );
}

#[tokio::test]
async fn multiple_sessions_isolated() {
    let secret = "multi-secret";
    let addr = common::mock_acp_server::start_mock_acp_server(secret).await;
    let secrets = Arc::new(InMemorySecretResolver::new());
    secrets.insert("S", secret);
    let connector = GrokConnector::new(secrets);
    let config = GrokServerConfig::loopback(addr.port(), SecretRef::new("S")).unwrap();
    let server = connector
        .connect(config)
        .unwrap()
        .wait()
        .await
        .unwrap();

    let s1 = server
        .sessions
        .begin_new(GrokSessionConfig::default())
        .unwrap()
        .opened
        .await
        .unwrap()
        .unwrap();
    let s2 = server
        .sessions
        .begin_new(GrokSessionConfig::default())
        .unwrap()
        .opened
        .await
        .unwrap()
        .unwrap();
    assert_ne!(s1.session_id.as_str(), s2.session_id.as_str());

    let e1 = s1
        .input
        .begin_send(EncodedAcpSessionMessage {
            method: "session/prompt".into(),
            params: serde_json::json!({ "prompt": [] }),
        })
        .unwrap();
    let e2 = s2
        .input
        .begin_send(EncodedAcpSessionMessage {
            method: "session/prompt".into(),
            params: serde_json::json!({ "prompt": [] }),
        })
        .unwrap();
    let _ = e1.response.await.unwrap().unwrap();
    let _ = e2.response.await.unwrap().unwrap();

    let u1 = s1.output.receive().await.unwrap().unwrap();
    let u2 = s2.output.receive().await.unwrap().unwrap();
    let m1: serde_json::Value = serde_json::from_slice(&u1).unwrap();
    let m2: serde_json::Value = serde_json::from_slice(&u2).unwrap();
    assert_eq!(m1["params"]["sessionId"], s1.session_id.as_str());
    assert_eq!(m2["params"]["sessionId"], s2.session_id.as_str());
}

#[tokio::test]
async fn session_load_uses_explicit_id() {
    let secret = "load-secret";
    let addr = common::mock_acp_server::start_mock_acp_server(secret).await;
    let secrets = Arc::new(InMemorySecretResolver::new());
    secrets.insert("S", secret);
    let connector = GrokConnector::new(secrets);
    let config = GrokServerConfig::loopback(addr.port(), SecretRef::new("S")).unwrap();
    let server = connector
        .connect(config)
        .unwrap()
        .wait()
        .await
        .unwrap();

    let known = GrokSessionId::new("explicit-resume-id");
    let session = server
        .sessions
        .begin_load(known.clone(), GrokSessionLoadConfig::default())
        .unwrap()
        .opened
        .await
        .unwrap()
        .unwrap();
    assert_eq!(session.session_id.as_str(), "explicit-resume-id");
}

#[tokio::test]
async fn non_loopback_without_opt_in_fails_closed() {
    let secrets = Arc::new(InMemorySecretResolver::new());
    secrets.insert("S", "x");
    let connector = GrokConnector::new(secrets);
    let mut config = GrokServerConfig::loopback(9, SecretRef::new("S")).unwrap();
    config.websocket_endpoint = url::Url::parse("ws://example.com:2419").unwrap();
    config.allow_non_loopback = false;
    let err = match connector.connect(config) {
        Err(e) => e,
        Ok(_) => panic!("must reject"),
    };
    assert!(
        err.to_string().contains("non-loopback") || err.to_string().contains("loopback"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn non_loopback_opt_in_still_requires_wss() {
    let secrets = Arc::new(InMemorySecretResolver::new());
    secrets.insert("S", "x");
    let connector = GrokConnector::new(secrets);
    let mut config = GrokServerConfig::loopback(9, SecretRef::new("S")).unwrap();
    config.websocket_endpoint = url::Url::parse("ws://example.com:2419").unwrap();
    config.allow_non_loopback = true;
    let err = match connector.connect(config) {
        Err(e) => e,
        Ok(_) => panic!("ws:// non-loopback must reject even when allowed"),
    };
    assert!(
        err.to_string().contains("wss") || err.to_string().contains("authenticated"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn bad_secret_rejected() {
    let secret = "real-secret";
    let addr = common::mock_acp_server::start_mock_acp_server(secret).await;
    let secrets = Arc::new(InMemorySecretResolver::new());
    secrets.insert("S", "wrong-secret");
    let connector = GrokConnector::new(secrets);
    let config = GrokServerConfig::loopback(addr.port(), SecretRef::new("S")).unwrap();
    let result = connector.connect(config).unwrap().wait().await;
    assert!(result.is_err(), "auth must fail");
}

#[tokio::test]
async fn proxy_routes_to_grok_backend() {
    let secret = "proxy-secret";
    let addr = common::mock_acp_server::start_mock_acp_server(secret).await;
    let secrets = Arc::new(InMemorySecretResolver::new());
    secrets.insert("GROK_SECRET", secret);
    let grok = Arc::new(GrokConnector::new(secrets));
    let proxy = ConnectorProxy::builder()
        .register("grok", grok)
        .route(ProxyRoute::EndpointPrefix)
        .build()
        .unwrap();

    let mut open = OpenConnection::new(
        ConnectionId::new("via-proxy"),
        format!("grok:ws://127.0.0.1:{}", addr.port()),
    );
    open.credential_ref = Some("GROK_SECRET".into());
    open.limits.connect_deadline = Duration::from_secs(5);

    let mut opened = tokio::time::timeout(Duration::from_secs(10), proxy.begin_open(open).opened)
        .await
        .expect("timeout")
        .expect("open");
    drive_owner(&mut opened);
    assert!(opened.external_session_id.is_some());
    assert_eq!(opened.dialect.input.framing, "json_rpc");

    let body = serde_json::json!({
        "prompt": [{ "type": "text", "text": "ping" }]
    });
    opened
        .input
        .send(bytes::Bytes::from(serde_json::to_vec(&body).unwrap()))
        .await
        .unwrap();

    let update = tokio::time::timeout(Duration::from_secs(5), opened.output.receive())
        .await
        .expect("timeout")
        .expect("recv")
        .expect("bytes");
    let msg: serde_json::Value = serde_json::from_slice(&update).unwrap();
    assert_eq!(msg["method"], "session/update");

    opened.control.cancel(CancellationReason::CallerRequested);
}
