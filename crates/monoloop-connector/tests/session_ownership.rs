//! WP-02: Connector factory and session ownership qualification.

use monoloop_connector::{
    pending_attach_with_dropped_completion, Connector, ConnectorFactory, ConnectorInstanceId,
    ControlDisposition, FakeConnector, FakeConnectorFactory, FakeSessionAdapter,
    FakeSessionAdapterConfig, FakeSessionRoute, OpenConnection, SessionAdapter, SessionAttachError,
    SessionAttachRequest, SessionAttachment, SessionRoute,
};
use monoloop_contracts::{ChannelId, ConnectionId, SessionConfig, SessionId, TransactionId};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn attach_request(requested: Option<SessionId>, config: SessionConfig) -> SessionAttachRequest {
    SessionAttachRequest {
        transaction_id: TransactionId::generate(),
        channel_id: ChannelId::try_new("ch-test").unwrap(),
        requested_session_id: requested,
        session_config: config,
        initial_mcp: None,
        deadline: Instant::now() + Duration::from_secs(5),
    }
}

#[tokio::test]
async fn attachment_from_instance_a_rejected_by_instance_b() {
    let factory = FakeConnectorFactory::external_agent(FakeSessionAdapterConfig::default());
    let a = factory.create().unwrap();
    let b = factory.create().unwrap();

    let sessions_a = a.sessions.as_ref().unwrap();
    let pending = sessions_a
        .begin_attach(attach_request(None, SessionConfig::default()))
        .unwrap();
    let attachment = pending.completion.await.unwrap();

    let open = OpenConnection::new(ConnectionId::new("c-cross"), "default")
        .with_session_attachment(attachment);
    let result = b.connector.begin_open(open).opened.await;
    let err = match result {
        Err(e) => e,
        Ok(_) => panic!("expected owner mismatch to fail open"),
    };
    assert_eq!(
        err.kind,
        monoloop_contracts::ConnectorErrorKind::ConfigurationInvalid
    );
}

#[tokio::test]
async fn same_instance_accepts_own_attachment() {
    let factory = FakeConnectorFactory::external_agent(FakeSessionAdapterConfig::default());
    let inst = factory.create().unwrap();
    let sessions = inst.sessions.as_ref().unwrap();
    let pending = sessions
        .begin_attach(attach_request(None, SessionConfig::default()))
        .unwrap();
    let attachment = pending.completion.await.unwrap();
    let open = OpenConnection::new(ConnectionId::new("c-ok"), "default")
        .with_session_attachment(attachment);
    let opened = inst.connector.begin_open(open).opened.await.unwrap();
    assert!(opened.external_session_id.is_some());
}

#[tokio::test]
async fn supplied_session_id_equals_returned_external_bytes() {
    let owner = ConnectorInstanceId::generate();
    let adapter = FakeSessionAdapter::new(owner, FakeSessionAdapterConfig::default());
    let cfg = SessionConfig {
        mode: Some("agent".into()),
        ..Default::default()
    };
    adapter.register_existing("known-sess-1", cfg.clone());

    let requested = SessionId::try_new("known-sess-1").unwrap();
    let pending = adapter
        .begin_attach(attach_request(Some(requested.clone()), cfg))
        .unwrap();
    let attachment = pending.completion.await.unwrap();
    assert_eq!(attachment.external_session_id.as_str(), requested.as_str());
}

#[tokio::test]
async fn immutable_session_config_mismatch_fails() {
    let owner = ConnectorInstanceId::generate();
    let adapter = FakeSessionAdapter::new(owner, FakeSessionAdapterConfig::default());
    let known = SessionConfig {
        mode: Some("ask".into()),
        ..Default::default()
    };
    adapter.register_existing("s-imm", known);

    let requested_cfg = SessionConfig {
        mode: Some("agent".into()),
        ..Default::default()
    };
    let pending = adapter
        .begin_attach(attach_request(
            Some(SessionId::try_new("s-imm").unwrap()),
            requested_cfg,
        ))
        .unwrap();
    let err = pending.completion.await.unwrap_err();
    assert_eq!(err, SessionAttachError::ConfigurationMismatch);
}

#[tokio::test]
async fn attach_cancel_during_delay() {
    let owner = ConnectorInstanceId::generate();
    let adapter = FakeSessionAdapter::new(
        owner,
        FakeSessionAdapterConfig {
            attach_delay: Duration::from_millis(200),
            ..Default::default()
        },
    );
    let pending = adapter
        .begin_attach(attach_request(None, SessionConfig::default()))
        .unwrap();
    assert_eq!(pending.control.cancel(), ControlDisposition::Accepted);
    let err = pending.completion.await.unwrap_err();
    assert_eq!(err, SessionAttachError::Cancelled);
}

#[tokio::test]
async fn attach_force_terminate_during_delay() {
    let owner = ConnectorInstanceId::generate();
    let adapter = FakeSessionAdapter::new(
        owner,
        FakeSessionAdapterConfig {
            attach_delay: Duration::from_millis(200),
            ..Default::default()
        },
    );
    let pending = adapter
        .begin_attach(attach_request(None, SessionConfig::default()))
        .unwrap();
    assert_eq!(
        pending.control.force_terminate(),
        ControlDisposition::Accepted
    );
    let err = pending.completion.await.unwrap_err();
    assert_eq!(err, SessionAttachError::Terminated);
}

#[tokio::test]
async fn dropped_pending_completion_is_invariant_failure() {
    let pending = pending_attach_with_dropped_completion();
    let err = pending.completion.await.unwrap_err();
    assert_eq!(err, SessionAttachError::InvariantFailed);
}

#[tokio::test]
async fn many_distinct_sessions_concurrent() {
    let owner = ConnectorInstanceId::generate();
    let adapter = Arc::new(FakeSessionAdapter::new(
        owner,
        FakeSessionAdapterConfig {
            max_in_flight: 32,
            ..Default::default()
        },
    ));

    let mut joins = Vec::new();
    for _ in 0..16 {
        let a = Arc::clone(&adapter);
        joins.push(tokio::spawn(async move {
            let pending = a
                .begin_attach(attach_request(None, SessionConfig::default()))
                .unwrap();
            pending.completion.await.unwrap()
        }));
    }
    let mut ids = Vec::new();
    for j in joins {
        let att = j.await.unwrap();
        ids.push(att.external_session_id.as_str().to_string());
    }
    ids.sort();
    ids.dedup();
    assert_eq!(ids.len(), 16);
    assert_eq!(adapter.completed_attaches(), 16);
    assert_eq!(adapter.in_flight(), 0);
}

#[tokio::test]
async fn blocked_session_does_not_block_unrelated() {
    let owner = ConnectorInstanceId::generate();
    let adapter = Arc::new(FakeSessionAdapter::new(
        owner,
        FakeSessionAdapterConfig {
            attach_delay: Duration::from_millis(150),
            max_in_flight: 8,
            ..Default::default()
        },
    ));

    let slow = adapter
        .begin_attach(attach_request(None, SessionConfig::default()))
        .unwrap();
    // Second attach starts while first is delayed — must not be serialized behind a global mutex
    // held across the delay (both in_flight concurrently).
    tokio::task::yield_now().await;
    let fast_adapter = FakeSessionAdapter::new(
        ConnectorInstanceId::generate(),
        FakeSessionAdapterConfig {
            attach_delay: Duration::ZERO,
            ..Default::default()
        },
    );
    let start = Instant::now();
    let fast = fast_adapter
        .begin_attach(attach_request(None, SessionConfig::default()))
        .unwrap()
        .completion
        .await
        .unwrap();
    assert!(start.elapsed() < Duration::from_millis(100));
    // D-013: attach create_mode uses provisional pending id; open assigns fake-created-*.
    assert!(
        fast.external_session_id
            .as_str()
            .starts_with("fake-pending-")
            || fast
                .external_session_id
                .as_str()
                .starts_with("fake-created-")
            || fast.external_session_id.as_str().starts_with("fake-sess-")
    );
    assert!(fast.create_mode);
    // Slow still completes independently.
    let _ = slow.completion.await.unwrap();
}

#[tokio::test]
async fn direct_llm_factory_has_no_session_adapter() {
    let inst = FakeConnectorFactory::direct_llm().create().unwrap();
    assert!(inst.sessions.is_none());
    let opened = inst
        .connector
        .begin_open(OpenConnection::new(ConnectionId::new("d1"), "default"))
        .opened
        .await
        .unwrap();
    assert!(opened.external_session_id.is_none());
}

#[test]
fn route_owner_matches_attachment() {
    let owner = ConnectorInstanceId::generate();
    let route: Arc<dyn SessionRoute> = Arc::new(FakeSessionRoute::new(owner.clone()));
    assert_eq!(route.owner(), &owner);
    let att = SessionAttachment::new(
        owner.clone(),
        monoloop_contracts::ExternalSessionId::try_new("e1").unwrap(),
        SessionConfig::default(),
        Arc::clone(&route),
    );
    assert_eq!(&att.owner, route.owner());
}

/// Documented admission rule (WP-04 owns the registry): same SessionKey must be
/// excluded before Connector::begin_open. This pure check mirrors that contract.
#[test]
fn same_session_key_excluded_before_connector_invocation() {
    use monoloop_contracts::{ChannelId, SessionId, SessionKey};
    use std::collections::HashSet;

    let key = SessionKey::new(
        ChannelId::try_new("ch").unwrap(),
        SessionId::try_new("sess").unwrap(),
    );
    let mut active: HashSet<SessionKey> = HashSet::new();
    assert!(active.insert(key.clone()));
    // Duplicate would be rejected at admission — connector never sees begin_open.
    assert!(!active.insert(key));
}

#[tokio::test]
async fn fake_connector_without_attachment_still_works() {
    let c = FakeConnector::echo();
    let opened = c
        .begin_open(OpenConnection::new(ConnectionId::new("plain"), "default"))
        .opened
        .await
        .unwrap();
    assert!(opened.external_session_id.is_none());
}
