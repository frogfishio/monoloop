//! Non-testkit Grok Build assembly for product hosts (e.g. Tauri / desktop).
//!
//! ```bash
//! cargo run -p monoloop --example host_grok_wiring --features grok
//! ```
//!
//! This example **only wires** the Channel + runtime by default. It does not open
//! a WebSocket unless you set `MONOLOOP_GROK_SUBMIT=1` and provide endpoint +
//! secret env vars. Live qualification drivers remain in `monoloop-testkit`.
//!
//! Runtime v2: `StartedRuntime::start` owns the executor (no external `Handle`).

#[cfg(not(feature = "grok"))]
compile_error!("enable `--features grok` for this example");

use monoloop::contracts::{
    transaction_delivery, user_text_input, CanonicalInput, CanonicalMessage, DeliveryLimits,
    InputLimits, InvocationConfig, SessionId, ShutdownWaitOutcome, TextPart,
    TransactionEventPayload, TransactionSubmitRequest,
};
use monoloop::interpreter::DefaultInterpreterFactory;
use monoloop::loop_runtime::{
    AcpPromptEncoder, ChannelRegistry, HostToolRegistry, RuntimeBootstrap, RuntimeConfig,
    StartedRuntime,
};
use std::sync::Arc;
use std::time::Duration;

/// Host keychain adapter: map Monoloop `SecretRef` names → secret material.
///
/// Production hosts should resolve from their OS keychain / secure store.
/// Never log the returned string.
struct HostKeychainResolver {
    /// Example: ref name `"grok-server-secret"` → secret bytes as String.
    grok_server_secret: String,
}

impl monoloop::connector_grok::SecretResolver for HostKeychainResolver {
    fn resolve(
        &self,
        secret_ref: &monoloop::connector_grok::SecretRef,
    ) -> Result<String, monoloop::connector_grok::GrokConnectorError> {
        match secret_ref.as_str() {
            "grok-server-secret" => Ok(self.grok_server_secret.clone()),
            _ => Err(monoloop::connector_grok::GrokConnectorError::credential_unavailable()),
        }
    }
}

/// Host journal → canonical multi-turn input (User / Assistant text turns).
fn journal_to_input(
    turns: &[(&str, &str)],
) -> Result<CanonicalInput, monoloop::contracts::InputValidationError> {
    let limits = InputLimits::default();
    let mut messages = Vec::with_capacity(turns.len());
    for &(role, text) in turns {
        let part = TextPart::try_new(text, limits.max_text_part_bytes)?;
        messages.push(match role {
            "system" => CanonicalMessage::System {
                content: vec![part],
                name: None,
            },
            "user" => CanonicalMessage::User {
                content: vec![part],
                name: None,
            },
            "assistant" => CanonicalMessage::Assistant {
                content: vec![part],
                tool_calls: vec![],
            },
            other => panic!("unsupported journal role in example: {other}"),
        });
    }
    CanonicalInput::try_new(messages, &limits)
}

fn main() {
    // --- 1. Secrets (keychain / secure store) ---------------------------------
    let secrets = Arc::new(HostKeychainResolver {
        grok_server_secret: std::env::var("MONOLOOP_GROK_SECRET")
            .unwrap_or_else(|_| "replace-me".into()),
    });

    // --- 2. Channel binding (public signature) --------------------------------
    let endpoint =
        std::env::var("MONOLOOP_GROK_ENDPOINT").unwrap_or_else(|_| "ws://127.0.0.1:2419".into());

    let binding = monoloop::connector_grok::grok_channel_binding(
        "grok",
        endpoint.clone(),
        "grok-server-secret",
        secrets,
        Arc::new(AcpPromptEncoder::grok()),
        Arc::new(DefaultInterpreterFactory::new()),
    );

    println!(
        "wired ChannelBinding id={} kind={:?} endpoint_ref={} credential_ref={:?}",
        binding.id.as_str(),
        binding.kind,
        binding.endpoint_ref,
        binding.credential_ref
    );

    // --- 3. Runtime owns its executor (v2) ------------------------------------
    // No bare tokio::Handle on RuntimeBootstrap. Desktop hosts call
    // StartedRuntime::start from process setup; the owner thread joins on shutdown.
    let started = StartedRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            enable_mcp_listener: false,
            ..Default::default()
        },
        channels: ChannelRegistry::build(vec![binding]).expect("registry"),
        tools: HostToolRegistry::empty(),
    })
    .expect("runtime start");

    // --- 4. Multi-turn input (host journal) -----------------------------------
    let _history = journal_to_input(&[
        ("user", "What is Monoloop?"),
        ("assistant", "Monoloop is Connector + Interpreter + Loop."),
        ("user", "Say hello in one short sentence."),
    ])
    .expect("history");
    let one_shot = user_text_input("hello").expect("one-shot");

    // Live text path: TransactionEventPayload::CanonicalUnit only (complete units).
    println!("events: match TransactionEventPayload::CanonicalUnit(_); no token stream");

    if std::env::var("MONOLOOP_GROK_SUBMIT").ok().as_deref() != Some("1") {
        println!(
            "assembly ok (no submit). Set MONOLOOP_GROK_SUBMIT=1 plus \
             MONOLOOP_GROK_ENDPOINT / MONOLOOP_GROK_SECRET to exercise a live turn."
        );
        let mut owner = started.owner;
        let wait_rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("wait runtime");
        let outcome = wait_rt.block_on(async {
            owner.begin_shutdown();
            owner.wait_stopped(Duration::from_secs(3)).await
        });
        assert!(
            matches!(outcome, ShutdownWaitOutcome::Stopped(_)),
            "expected Stopped, got {outcome:?}"
        );
        return;
    }

    // --- 5. Optional live submit (push delivery) ------------------------------
    // New session: session_id = None.
    // Resume: session_id = Some(SessionId::try_new("<grok-sessionId>").unwrap())
    let handle = started.handle.clone();
    let (delivery, mut receiver) =
        transaction_delivery(DeliveryLimits::try_new(64, 256 * 1024).expect("limits"))
            .expect("delivery");

    let _receipt = handle
        .submit(TransactionSubmitRequest {
            channel_id: monoloop::contracts::ChannelId::try_new("grok").expect("id"),
            session_id: None::<SessionId>,
            input: one_shot,
            session_config: None,
            invocation_config: InvocationConfig {
                deadline: Some(Duration::from_secs(120)),
                ..Default::default()
            },
            tools: vec![],
            delivery,
        })
        .expect("admit");

    let wait_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("wait runtime");

    let completion = wait_rt.block_on(async {
        tokio::time::timeout(Duration::from_secs(180), receiver.completion.recv()).await
    });
    match completion {
        Ok(Ok(end)) => {
            while let Ok(ev) = receiver.events.try_recv() {
                if let TransactionEventPayload::CanonicalUnit(unit) = &ev.payload {
                    println!("canonical unit: {:?}", unit.snapshot().unit.kind_label());
                }
            }
            println!("transaction end: {:?}", end.end.kind);
        }
        Ok(Err(_)) => println!("completion channel closed"),
        Err(_) => println!("live submit timed out"),
    }

    let mut owner = started.owner;
    let outcome = wait_rt.block_on(async {
        owner.begin_shutdown();
        owner.wait_stopped(Duration::from_secs(5)).await
    });
    assert!(
        matches!(outcome, ShutdownWaitOutcome::Stopped(_)),
        "expected Stopped, got {outcome:?}"
    );
}
