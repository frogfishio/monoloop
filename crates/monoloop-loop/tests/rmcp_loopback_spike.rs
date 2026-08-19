//! WP-00 compile/bind spike for `rmcp` Streamable HTTP on loopback.
//!
//! Proves the selected SDK can construct a Streamable HTTP service, bind a
//! loopback listener, and shut down cleanly. This is **not** the production
//! MCP gateway (WP-07); the handler is intentionally empty and no capability
//! token routing is implemented here.

use std::sync::Arc;

use axum::Router;
use rmcp::{
    model::{ServerCapabilities, ServerInfo},
    transport::streamable_http_server::{
        session::local::LocalSessionManager, StreamableHttpServerConfig, StreamableHttpService,
    },
    ServerHandler,
};
use tokio_util::sync::CancellationToken;

/// Minimal `ServerHandler` with no tools — compile and lifecycle only.
#[derive(Clone, Default)]
struct EmptySpikeHandler;

impl ServerHandler for EmptySpikeHandler {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().build())
    }
}

/// Bind Streamable HTTP on `127.0.0.1:0`, then cancel. No product behavior.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rmcp_streamable_http_binds_loopback_and_shuts_down() {
    let cancellation = CancellationToken::new();
    let mut config = StreamableHttpServerConfig::default();
    config.cancellation_token = cancellation.clone();
    // Keep spike light; no long-lived SSE.
    config.sse_keep_alive = None;
    config.sse_retry = None;

    let service: StreamableHttpService<EmptySpikeHandler, LocalSessionManager> =
        StreamableHttpService::new(
            || Ok(EmptySpikeHandler),
            Arc::new(LocalSessionManager::default()),
            config,
        );

    let router = Router::new().nest_service("/mcp", service);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("loopback bind");
    let addr = listener.local_addr().expect("local addr");
    assert!(
        addr.ip().is_loopback(),
        "MCP spike must bind loopback only; got {addr}"
    );

    let serve = tokio::spawn({
        let cancellation = cancellation.clone();
        async move {
            let _ = axum::serve(listener, router)
                .with_graceful_shutdown(async move {
                    cancellation.cancelled().await;
                })
                .await;
        }
    });

    // Brief ready check: TCP accept path is up.
    let _ = tokio::net::TcpStream::connect(addr).await;

    cancellation.cancel();
    serve.await.expect("serve task join");
}

/// Workspace deps used by later WPs must type-check with pinned features.
#[test]
fn wp00_selected_deps_typecheck() {
    // reqwest (Rustls, no default-tls)
    let _builder = reqwest::Client::builder();

    // secrecy
    let secret = secrecy::SecretString::from("wp00-not-a-real-secret");
    let _len = secrecy::ExposeSecret::expose_secret(&secret).len();

    // jsonschema (no network resolvers — default-features off)
    let schema = serde_json::json!({
        "type": "object",
        "properties": { "n": { "type": "integer" } },
        "required": ["n"]
    });
    let validator = jsonschema::validator_for(&schema).expect("compile schema");
    assert!(validator.is_valid(&serde_json::json!({ "n": 1 })));
    assert!(!validator.is_valid(&serde_json::json!({ "n": "x" })));

    // OS CSPRNG for future MCP capability tokens (OsRng via rand 0.9)
    let mut token = [0u8; 32];
    rand::TryRngCore::try_fill_bytes(&mut rand::rngs::OsRng, &mut token)
        .expect("OS CSPRNG must provide entropy");
    assert_ne!(token, [0u8; 32]);
}
