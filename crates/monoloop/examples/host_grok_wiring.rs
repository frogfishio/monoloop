//! Non-testkit Grok Build assembly for product hosts (e.g. Tauri / desktop).
//!
//! ```bash
//! cargo run -p monoloop --example host_grok_wiring --features grok
//! ```
//!
//! This example **only wires** the Channel + runtime. It does not open a WebSocket
//! unless you set `MONOLOOP_GROK_SUBMIT=1` and provide endpoint + secret env vars
//! (see comments below). Live qualification drivers remain in `monoloop-testkit`.

#[cfg(not(feature = "grok"))]
compile_error!("enable `--features grok` for this example");

use monoloop::contracts::{
    user_text_input, CanonicalInput, CanonicalMessage, FnCompletionCallback, FnEventSink,
    InputLimits, InvocationConfig, SessionId, TextPart, TransactionEnd, TransactionEvent,
    TransactionEventPayload, TransactionRequest, TransactionRuntime,
};
use monoloop::interpreter::DefaultInterpreterFactory;
use monoloop::loop_runtime::{
    AcpPromptEncoder, ChannelRegistry, DefaultTransactionRuntime, HostToolRegistry,
    RuntimeBootstrap, RuntimeConfig,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;

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

#[tokio::main]
async fn main() {
    // --- 1. Secrets (keychain / secure store) ---------------------------------
    let secrets = Arc::new(HostKeychainResolver {
        grok_server_secret: std::env::var("MONOLOOP_GROK_SECRET")
            .unwrap_or_else(|_| "replace-me".into()),
    });

    // --- 2. Channel binding (public signature) --------------------------------
    //
    // pub fn grok_channel_binding(
    //     id: impl AsRef<str>,
    //     endpoint_ref: impl Into<String>,      // e.g. "ws://127.0.0.1:2419"
    //     credential_ref: impl Into<String>,    // SecretResolver key name
    //     secrets: Arc<dyn SecretResolver>,
    //     encoder: Arc<dyn OutboundDialectEncoder>,
    //     interpreter: Arc<dyn InterpreterFactory>,
    // ) -> ChannelBinding
    //
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

    // --- 3. Tokio Handle (Tauri / desktop hosts) ------------------------------
    //
    // RuntimeBootstrap.executor requires a tokio::runtime::Handle.
    // Supported host pattern: start a dedicated multi-thread Tokio runtime at
    // app setup and pass Handle::current() (or runtime.handle().clone()) here.
    // #[tokio::main] is fine for CLI samples; Tauri should own the runtime.
    //
    let rt = DefaultTransactionRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            enable_mcp_listener: false,
            ..Default::default()
        },
        channels: ChannelRegistry::build(vec![binding]).expect("registry"),
        tools: HostToolRegistry::empty(),
        executor: tokio::runtime::Handle::current(),
    })
    .await
    .expect("runtime start");

    // --- 4. Multi-turn input (host journal) -----------------------------------
    let _history = journal_to_input(&[
        ("user", "What is Monoloop?"),
        ("assistant", "Monoloop is Connector + Interpreter + Loop."),
        ("user", "Say hello in one short sentence."),
    ])
    .expect("history");
    let _one_shot = user_text_input("hello").expect("one-shot");

    // Live text path: TransactionEventPayload::CanonicalUnit only (complete units).
    // There is no token / delta stream API.
    println!("events: match TransactionEventPayload::CanonicalUnit(_); no token stream");

    if std::env::var("MONOLOOP_GROK_SUBMIT").ok().as_deref() != Some("1") {
        println!(
            "assembly ok (no submit). Set MONOLOOP_GROK_SUBMIT=1 plus \
             MONOLOOP_GROK_ENDPOINT / MONOLOOP_GROK_SECRET to exercise a live turn."
        );
        return;
    }

    // --- 5. Optional live submit ----------------------------------------------
    // New session: session_id = None.
    // Resume: session_id = Some(SessionId::from_external(
    //     &ExternalSessionId::try_new("<grok-sessionId>").unwrap()))
    let (done_tx, done_rx) = oneshot::channel::<()>();
    let done_tx = Arc::new(std::sync::Mutex::new(Some(done_tx)));

    let events = Arc::new(FnEventSink(move |ev: TransactionEvent| {
        Box::pin(async move {
            if let TransactionEventPayload::CanonicalUnit(unit) = &ev.payload {
                println!("canonical unit: {:?}", unit.snapshot().unit.kind_label());
            }
            Ok(())
        }) as monoloop::contracts::EventDelivery
    }));

    let done_cb = Arc::clone(&done_tx);
    let completion = Box::new(FnCompletionCallback(move |end: TransactionEnd| {
        let done_cb = Arc::clone(&done_cb);
        Box::pin(async move {
            println!("transaction end: {:?}", end.kind);
            if let Some(tx) = done_cb.lock().expect("lock").take() {
                let _ = tx.send(());
            }
            Ok(())
        }) as monoloop::contracts::CompletionDelivery
    }));

    let _receipt = TransactionRuntime::submit(
        rt.as_ref(),
        TransactionRequest {
            channel_id: monoloop::contracts::ChannelId::try_new("grok").expect("id"),
            session_id: None::<SessionId>,
            input: _one_shot,
            session_config: None,
            invocation_config: InvocationConfig {
                deadline: Some(Duration::from_secs(120)),
                ..Default::default()
            },
            tools: vec![],
            events,
            completion,
        },
    )
    .expect("admit");

    let _ = tokio::time::timeout(Duration::from_secs(180), done_rx).await;
}
