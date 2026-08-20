//! Loopback MCP Streamable HTTP gateway with per-capability routing.

use super::binding::{CapabilityToken, McpInstallError, McpRouteTable, PendingMcpBinding};
use super::handler::TransactionMcpHandler;
use crate::transaction::dispatcher::TransactionToolDispatcher;
use crate::transaction::resolved_tools::ResolvedToolSet;
use axum::body::Body;
use axum::extract::{Path, Request, State};
use axum::http::{Response, StatusCode};
use axum::routing::any;
use axum::Router;
use monoloop_contracts::{ExchangeId, TransactionId};
use rmcp::transport::streamable_http_server::{
    session::local::LocalSessionManager, StreamableHttpServerConfig, StreamableHttpService,
};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Axum state: routes + gateway-owned capability services (not process-global — §17).
#[derive(Clone)]
struct GatewayState {
    routes: Arc<McpRouteTable>,
    services: Arc<std::sync::Mutex<std::collections::HashMap<String, Arc<CapabilityHttpService>>>>,
    request_permits: Arc<tokio::sync::Semaphore>,
}

/// Cloneable handle for install/activate/revoke without owning the listener.
#[derive(Clone)]
pub struct McpGatewayHandle {
    routes: Arc<McpRouteTable>,
    services: Arc<std::sync::Mutex<std::collections::HashMap<String, Arc<CapabilityHttpService>>>>,
    /// Retained so Clone keeps the same gateway-scoped concurrency budget.
    #[allow(dead_code)]
    request_permits: Arc<tokio::sync::Semaphore>,
    base_url: String,
    local_addr: SocketAddr,
}

impl McpGatewayHandle {
    /// Bound loopback address.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Base URL `http://127.0.0.1:port` (no path).
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Shared route table.
    pub fn routes(&self) -> &Arc<McpRouteTable> {
        &self.routes
    }

    /// Install a pending capability for a transaction.
    pub fn install_pending(
        &self,
        transaction_id: TransactionId,
        tools: ResolvedToolSet,
        dispatcher: Arc<TransactionToolDispatcher>,
        exchange_id: ExchangeId,
    ) -> Result<PendingMcpBinding, McpInstallError> {
        self.routes.install_pending(
            transaction_id,
            tools,
            dispatcher,
            exchange_id,
            &self.base_url,
        )
    }

    /// Activate a pending capability.
    pub fn activate(&self, token: &CapabilityToken) -> Result<(), McpInstallError> {
        self.routes.activate(token)
    }

    /// Revoke one capability (idempotent).
    pub fn revoke(&self, token: &CapabilityToken) -> bool {
        let removed = self.routes.revoke(token);
        if removed {
            drop_capability_service(&self.services, &token.to_hex());
        }
        removed
    }

}

/// Production MCP gateway: one loopback listener, many capability routes.
pub struct McpGateway {
    handle: McpGatewayHandle,
    cancel: CancellationToken,
    join: JoinHandle<()>,
}

impl McpGateway {
    /// Bind `127.0.0.1:0`, serve Streamable HTTP, fail closed if not loopback.
    pub async fn bind_loopback(max_routes: usize) -> Result<Self, McpInstallError> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|_| McpInstallError::InvalidDescriptor)?;
        let local_addr = listener
            .local_addr()
            .map_err(|_| McpInstallError::InvalidDescriptor)?;
        if !local_addr.ip().is_loopback() {
            return Err(McpInstallError::InvalidDescriptor);
        }

        let routes = McpRouteTable::new(max_routes);
        let services = Arc::new(std::sync::Mutex::new(std::collections::HashMap::new()));
        let request_permits = Arc::new(tokio::sync::Semaphore::new(MAX_GLOBAL_MCP_REQUESTS));
        let base_url = format!("http://{}", local_addr);
        let cancel = CancellationToken::new();
        let cancel_serve = cancel.clone();
        let state = GatewayState {
            routes: Arc::clone(&routes),
            services: Arc::clone(&services),
            request_permits: Arc::clone(&request_permits),
        };

        let app = Router::new()
            .route("/mcp/{token}", any(mcp_dispatch))
            .route("/mcp/{token}/{*rest}", any(mcp_dispatch_rest))
            .with_state(state);

        // Listener task is owned by this gateway JoinHandle until `shutdown`.
        // RuntimeOwner integration (TaskClass::RuntimeService) is a follow-on.
        let join = tokio::spawn(async move {
            let _ = axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    cancel_serve.cancelled().await;
                })
                .await;
        });

        Ok(Self {
            handle: McpGatewayHandle {
                routes,
                services,
                request_permits,
                base_url,
                local_addr,
            },
            cancel,
            join,
        })
    }

    /// Cloneable handle for actors and admission.
    pub fn handle(&self) -> McpGatewayHandle {
        self.handle.clone()
    }

    /// Bound loopback address.
    pub fn local_addr(&self) -> SocketAddr {
        self.handle.local_addr()
    }

    /// Base URL.
    pub fn base_url(&self) -> &str {
        self.handle.base_url()
    }

    /// Shared route table.
    pub fn routes(&self) -> &Arc<McpRouteTable> {
        self.handle.routes()
    }

    /// Install a pending capability for a transaction.
    pub fn install_pending(
        &self,
        transaction_id: TransactionId,
        tools: ResolvedToolSet,
        dispatcher: Arc<TransactionToolDispatcher>,
        exchange_id: ExchangeId,
    ) -> Result<PendingMcpBinding, McpInstallError> {
        self.handle
            .install_pending(transaction_id, tools, dispatcher, exchange_id)
    }

    /// Activate a pending capability.
    pub fn activate(&self, token: &CapabilityToken) -> Result<(), McpInstallError> {
        self.handle.activate(token)
    }

    /// Revoke one capability (idempotent).
    pub fn revoke(&self, token: &CapabilityToken) -> bool {
        self.handle.revoke(token)
    }

    /// Shutdown: revoke this gateway's routes, cancel their MCP services, stop listener.
    pub async fn shutdown(self) {
        // Only drop services owned by this gateway (tokens in its route table).
        let tokens = self.handle.routes.revoke_all();
        for hex in tokens {
            drop_capability_service(&self.handle.services, &hex);
        }
        self.cancel.cancel();
        let _ = self.join.await;
    }
}

async fn mcp_dispatch(
    State(state): State<GatewayState>,
    Path(token): Path<String>,
    req: Request,
) -> Response<Body> {
    forward_mcp(state, &token, req).await
}

async fn mcp_dispatch_rest(
    State(state): State<GatewayState>,
    Path((token, _rest)): Path<(String, String)>,
    req: Request,
) -> Response<Body> {
    forward_mcp(state, &token, req).await
}

/// Per-capability Streamable HTTP service (shared across requests for one token).
struct CapabilityHttpService {
    service: StreamableHttpService<TransactionMcpHandler, LocalSessionManager>,
    cancel: CancellationToken,
    /// Per-capability concurrent request bound (D-034).
    permits: Arc<tokio::sync::Semaphore>,
}

const MAX_GLOBAL_MCP_REQUESTS: usize = 64;
const MAX_PER_CAPABILITY_MCP_REQUESTS: usize = 8;
const MCP_REQUEST_DURATION: std::time::Duration = std::time::Duration::from_secs(30);

/// Drop and cancel the per-token Streamable HTTP service for this gateway (D-018).
fn drop_capability_service(
    services: &std::sync::Mutex<std::collections::HashMap<String, Arc<CapabilityHttpService>>>,
    token_hex: &str,
) {
    let key = CapabilityToken::from_hex(token_hex)
        .map(|t| t.to_hex())
        .unwrap_or_else(|| token_hex.to_ascii_lowercase());
    if let Ok(mut map) = services.lock() {
        if let Some(svc) = map.remove(&key) {
            svc.cancel.cancel();
        }
    }
}

async fn forward_mcp(state: GatewayState, token_hex: &str, req: Request) -> Response<Body> {
    // D-034: canonicalize hex spelling before route/service-map access so
    // uppercase/lowercase equivalents share one service and revoke key.
    let Some(canonical) = CapabilityToken::from_hex(token_hex).map(|t| t.to_hex()) else {
        return Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::from("unknown capability"))
            .unwrap_or_else(|_| Response::new(Body::empty()));
    };
    let Some(binding) = state.routes.get_by_hex(&canonical) else {
        drop_capability_service(&state.services, &canonical);
        return Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::from("unknown capability"))
            .unwrap_or_else(|_| Response::new(Body::empty()));
    };

    // Acquire gateway + per-capability permits before body buffering (D-034).
    let Ok(_global) = state.request_permits.clone().try_acquire_owned() else {
        return Response::builder()
            .status(StatusCode::TOO_MANY_REQUESTS)
            .body(Body::from("mcp gateway concurrency exceeded"))
            .unwrap_or_else(|_| Response::new(Body::empty()));
    };

    let service = {
        let mut map = state.services.lock().unwrap_or_else(|e| e.into_inner());
        map.entry(canonical.clone())
            .or_insert_with(|| {
                let handler = binding.handler.clone();
                let cancel = CancellationToken::new();
                let mut config = StreamableHttpServerConfig::default();
                config.cancellation_token = cancel.clone();
                // No long-lived SSE keep-alive; request streams complete with the response.
                config.sse_keep_alive = None;
                config.sse_retry = None;
                // Prefer JSON when possible for simpler clients; SSE still used when needed.
                config.json_response = true;
                Arc::new(CapabilityHttpService {
                    service: StreamableHttpService::new(
                        move || Ok(handler.clone()),
                        Arc::new(LocalSessionManager::default()),
                        config,
                    ),
                    cancel,
                    permits: Arc::new(tokio::sync::Semaphore::new(MAX_PER_CAPABILITY_MCP_REQUESTS)),
                })
            })
            .clone()
    };

    let Ok(_local) = service.permits.clone().try_acquire_owned() else {
        return Response::builder()
            .status(StatusCode::TOO_MANY_REQUESTS)
            .body(Body::from("mcp capability concurrency exceeded"))
            .unwrap_or_else(|_| Response::new(Body::empty()));
    };

    let deadline_at = tokio::time::Instant::now() + MCP_REQUEST_DURATION;
    let (parts, body) = req.into_parts();
    let body_budget = deadline_at.saturating_duration_since(tokio::time::Instant::now());
    let collected =
        match tokio::time::timeout(body_budget, axum::body::to_bytes(body, 1024 * 1024)).await {
            Ok(Ok(b)) => b,
            Ok(Err(_)) => {
                return Response::builder()
                    .status(StatusCode::PAYLOAD_TOO_LARGE)
                    .body(Body::from("request body exceeds bound"))
                    .unwrap_or_else(|_| Response::new(Body::empty()));
            }
            Err(_) => {
                return Response::builder()
                    .status(StatusCode::GATEWAY_TIMEOUT)
                    .body(Body::from("mcp request deadline exceeded"))
                    .unwrap_or_else(|_| Response::new(Body::empty()));
            }
        };
    let req = rewrite_path(
        Request::from_parts(parts, Body::from(collected)),
        &canonical,
    );
    let handle_budget = deadline_at.saturating_duration_since(tokio::time::Instant::now());
    if handle_budget.is_zero() {
        return Response::builder()
            .status(StatusCode::GATEWAY_TIMEOUT)
            .body(Body::from("mcp request deadline exceeded"))
            .unwrap_or_else(|_| Response::new(Body::empty()));
    }
    match tokio::time::timeout(handle_budget, service.service.handle(req)).await {
        Ok(response) => response.map(Body::new),
        Err(_) => Response::builder()
            .status(StatusCode::GATEWAY_TIMEOUT)
            .body(Body::from("mcp request deadline exceeded"))
            .unwrap_or_else(|_| Response::new(Body::empty())),
    }
}

fn rewrite_path(req: Request, token_hex: &str) -> Request {
    let (mut parts, body) = req.into_parts();
    let path = parts.uri.path().to_string();
    let query = parts.uri.query().map(|q| q.to_string());
    let prefix = format!("/mcp/{token_hex}");
    let new_path = if let Some(rest) = path.strip_prefix(&prefix) {
        if rest.is_empty() {
            "/".to_string()
        } else {
            rest.to_string()
        }
    } else {
        path
    };
    let pq = match query {
        Some(q) => format!("{new_path}?{q}"),
        None => new_path,
    };
    if let Ok(uri) = pq.parse::<axum::http::Uri>() {
        parts.uri = uri;
    }
    Request::from_parts(parts, body)
}
