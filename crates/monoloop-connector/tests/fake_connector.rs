//! FakeConnector and ConnectorProxy qualification tests.

use bytes::Bytes;
use monoloop_connector::{
    CancellationReason, ConnectionEndKind, ConnectionId, Connector, ConnectorProxy,
    ControlDisposition, FakeConnector, FakeConnectorConfig, FakeEndpoint, OpenConnection,
    ProxyRoute, TerminationReason,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

fn drive_owner(opened: &mut monoloop_connector::OpenedRawConnection) {
    if let Some(work) = opened.take_owner_work() {
        tokio::spawn(work.into_future());
    }
}

#[tokio::test]
async fn echo_preserves_order_and_bytes() {
    let connector = FakeConnector::echo();
    let pending = connector.begin_open(OpenConnection::new(ConnectionId::new("c1"), "default"));
    let mut opened = pending.opened.await.expect("open");
    drive_owner(&mut opened);
    opened
        .input
        .send(Bytes::from_static(b"hello"))
        .await
        .unwrap();
    opened
        .input
        .send(Bytes::from_static(b" world"))
        .await
        .unwrap();
    opened.input.finish().await.unwrap();

    let a = opened.output.receive().await.unwrap().unwrap();
    let b = opened.output.receive().await.unwrap().unwrap();
    assert_eq!(&a[..], b"hello");
    assert_eq!(&b[..], b" world");

    let end = opened.completion.wait().await;
    assert_eq!(end.kind, ConnectionEndKind::RemoteEof);
    assert_eq!(end.bytes_accepted, 11);
}

#[tokio::test]
async fn scripted_output_independent_of_fragmentation() {
    let mut endpoints = HashMap::new();
    endpoints.insert(
        "script".into(),
        FakeEndpoint::Scripted {
            chunks: vec![Bytes::from_static(b"ab"), Bytes::from_static(b"cd")],
        },
    );
    let connector = FakeConnector::new(FakeConnectorConfig {
        endpoints,
        ..Default::default()
    });
    let pending = connector.begin_open(OpenConnection::new(ConnectionId::new("c2"), "script"));
    let mut opened = pending.opened.await.unwrap();
    drive_owner(&mut opened);
    let a = opened.output.receive().await.unwrap().unwrap();
    let b = opened.output.receive().await.unwrap().unwrap();
    assert_eq!(&a[..], b"ab");
    assert_eq!(&b[..], b"cd");
}

#[tokio::test]
async fn cancel_during_open_returns_cancelled() {
    let connector = FakeConnector::new(FakeConnectorConfig {
        open_delay: Duration::from_secs(5),
        ..Default::default()
    });
    let pending = connector.begin_open(OpenConnection::new(ConnectionId::new("c3"), "default"));
    assert_eq!(
        pending.control.cancel(CancellationReason::CallerRequested),
        ControlDisposition::Accepted
    );
    let err = match pending.opened.await {
        Err(e) => e,
        Ok(_) => panic!("should cancel"),
    };
    assert_eq!(err.kind, monoloop_connector::ConnectorErrorKind::Cancelled);
}

#[tokio::test]
async fn cancel_after_open_yields_cancelled_terminal() {
    let connector = FakeConnector::echo();
    let pending = connector.begin_open(OpenConnection::new(ConnectionId::new("c4"), "default"));
    let mut opened = pending.opened.await.unwrap();
    drive_owner(&mut opened);
    assert_eq!(
        opened.control.cancel(CancellationReason::CallerRequested),
        ControlDisposition::Accepted
    );
    // Send may fail once cancel is recorded.
    let _ = opened.input.send(Bytes::from_static(b"x")).await;
    let end = opened.completion.wait().await;
    assert_eq!(end.kind, ConnectionEndKind::Cancelled);
}

#[tokio::test]
async fn terminate_wins_over_cancel() {
    let connector = FakeConnector::echo();
    let pending = connector.begin_open(OpenConnection::new(ConnectionId::new("c5"), "default"));
    let mut opened = pending.opened.await.unwrap();
    drive_owner(&mut opened);
    opened.control.cancel(CancellationReason::CallerRequested);
    opened
        .control
        .terminate(TerminationReason::CancelEscalation);
    let end = opened.completion.wait().await;
    assert_eq!(end.kind, ConnectionEndKind::Terminated);
}

#[tokio::test]
async fn repeated_cancel_is_idempotent() {
    let connector = FakeConnector::echo();
    let pending = connector.begin_open(OpenConnection::new(ConnectionId::new("c6"), "default"));
    let mut opened = pending.opened.await.unwrap();
    drive_owner(&mut opened);
    assert_eq!(
        opened.control.cancel(CancellationReason::CallerRequested),
        ControlDisposition::Accepted
    );
    assert_eq!(
        opened.control.cancel(CancellationReason::CallerRequested),
        ControlDisposition::AlreadyRequested
    );
    let _ = opened.completion.wait().await;
    assert_eq!(
        opened.control.cancel(CancellationReason::CallerRequested),
        ControlDisposition::AlreadyTerminal
    );
}

#[tokio::test]
async fn sibling_connections_are_isolated() {
    let connector = FakeConnector::echo();
    let mut a = connector
        .begin_open(OpenConnection::new(ConnectionId::new("a"), "default"))
        .opened
        .await
        .unwrap();
    drive_owner(&mut a);
    let mut b = connector
        .begin_open(OpenConnection::new(ConnectionId::new("b"), "default"))
        .opened
        .await
        .unwrap();
    drive_owner(&mut b);
    a.control.cancel(CancellationReason::CallerRequested);
    b.input.send(Bytes::from_static(b"ok")).await.unwrap();
    let got = b.output.receive().await.unwrap().unwrap();
    assert_eq!(&got[..], b"ok");
    let end_a = a.completion.wait().await;
    assert_eq!(end_a.kind, ConnectionEndKind::Cancelled);
    b.input.finish().await.unwrap();
    let end_b = b.completion.wait().await;
    assert_eq!(end_b.kind, ConnectionEndKind::RemoteEof);
}

#[tokio::test]
async fn proxy_routes_by_prefix() {
    let fake = Arc::new(FakeConnector::echo());
    let proxy = ConnectorProxy::builder()
        .register("fake", fake)
        .default_backend("fake")
        .route(ProxyRoute::EndpointPrefix)
        .build()
        .unwrap();

    let pending = proxy.begin_open(OpenConnection::new(ConnectionId::new("p1"), "fake:default"));
    let mut opened = pending.opened.await.unwrap();
    drive_owner(&mut opened);
    opened.input.send(Bytes::from_static(b"z")).await.unwrap();
    let got = opened.output.receive().await.unwrap().unwrap();
    assert_eq!(&got[..], b"z");
}

#[tokio::test]
async fn proxy_unknown_backend_fails_closed() {
    let fake = Arc::new(FakeConnector::echo());
    let proxy = ConnectorProxy::builder()
        .register("fake", fake)
        .route(ProxyRoute::EndpointPrefix)
        .build()
        .unwrap();
    // No default and no matching registered prefix for bare "default"
    let pending = proxy.begin_open(OpenConnection::new(ConnectionId::new("p2"), "default"));
    let err = match pending.opened.await {
        Err(e) => e,
        Ok(_) => panic!("must fail"),
    };
    assert_eq!(
        err.kind,
        monoloop_connector::ConnectorErrorKind::ConfigurationInvalid
    );
}

#[tokio::test]
async fn external_session_id_propagated_unchanged() {
    let connector = FakeConnector::echo();
    let mut req = OpenConnection::new(ConnectionId::new("c7"), "default");
    req.external_session_id = Some(monoloop_connector::ExternalSessionId::new(
        "grok-session-xyz",
    ));
    let mut opened = connector.begin_open(req).opened.await.unwrap();
    drive_owner(&mut opened);
    assert_eq!(
        opened.external_session_id.as_ref().map(|s| s.as_str()),
        Some("grok-session-xyz")
    );
}

#[tokio::test]
async fn send_after_finish_fails() {
    let connector = FakeConnector::echo();
    let mut opened = connector
        .begin_open(OpenConnection::new(ConnectionId::new("c8"), "default"))
        .opened
        .await
        .unwrap();
    drive_owner(&mut opened);
    opened.input.finish().await.unwrap();
    let err = opened
        .input
        .send(Bytes::from_static(b"nope"))
        .await
        .unwrap_err();
    assert_eq!(
        err.kind,
        monoloop_connector::ConnectorErrorKind::WriteFailed
    );
}
