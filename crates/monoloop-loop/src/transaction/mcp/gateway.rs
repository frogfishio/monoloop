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
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

/// Runs one MCP HTTP request under TaskSupervisor as `TaskClass::McpRequest` (§17).
///
/// Injected by RuntimeOwner; standalone prepare+serve tests leave this unset
/// and execute request work inline.
pub trait McpRequestOwner: Send + Sync {
    /// Own `work` for `transaction_id` and return its response.
    fn run_owned(
        &self,
        transaction_id: TransactionId,
        work: Pin<Box<dyn Future<Output = Response<Body>> + Send>>,
    ) -> Pin<Box<dyn Future<Output = Response<Body>> + Send>>;
}

/// Bounded MCP request concurrency and duration (D-034 / Law 22).
///
/// Production defaults match the historical constants. Tests inject smaller
/// budgets for exact-limit and plus-one proofs without multi-second waits.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct McpGatewayLimits {
    /// Gateway-wide concurrent in-flight MCP HTTP requests.
    pub max_global_requests: usize,
    /// Per-capability concurrent in-flight MCP HTTP requests.
    pub max_per_capability_requests: usize,
    /// Absolute wall budget for body read + Streamable HTTP handle.
    pub request_duration: std::time::Duration,
}

impl Default for McpGatewayLimits {
    fn default() -> Self {
        Self {
            max_global_requests: DEFAULT_MAX_GLOBAL_MCP_REQUESTS,
            max_per_capability_requests: DEFAULT_MAX_PER_CAPABILITY_MCP_REQUESTS,
            request_duration: DEFAULT_MCP_REQUEST_DURATION,
        }
    }
}

impl McpGatewayLimits {
    fn validated(self) -> Result<Self, McpInstallError> {
        if self.max_global_requests == 0
            || self.max_per_capability_requests == 0
            || self.request_duration.is_zero()
        {
            return Err(McpInstallError::InvalidDescriptor);
        }
        Ok(self)
    }
}

/// Axum state: routes + gateway-owned capability services (not process-global — §17).
#[derive(Clone)]
struct GatewayState {
    routes: Arc<McpRouteTable>,
    services: Arc<std::sync::Mutex<std::collections::HashMap<String, Arc<CapabilityHttpService>>>>,
    request_permits: Arc<tokio::sync::Semaphore>,
    request_owner: Option<Arc<dyn McpRequestOwner>>,
    max_per_capability_requests: usize,
    request_duration: std::time::Duration,
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

    /// Revoke every route and cancel per-capability services (shutdown / quiesce).
    pub fn revoke_all_services(&self) {
        let tokens = self.routes.revoke_all();
        for hex in tokens {
            drop_capability_service(&self.services, &hex);
        }
    }
}

/// Listener + router prepared without spawning (TaskSupervisor / RuntimeService).
pub struct PreparedMcpGateway {
    handle: McpGatewayHandle,
    cancel: CancellationToken,
    listener: tokio::net::TcpListener,
    app: Router,
}

impl PreparedMcpGateway {
    /// Cloneable install/activate handle.
    pub fn handle(&self) -> McpGatewayHandle {
        self.handle.clone()
    }

    /// Cancellation token that stops [`Self::serve`].
    pub fn cancel_token(&self) -> CancellationToken {
        self.cancel.clone()
    }

    /// Bound loopback address.
    pub fn local_addr(&self) -> SocketAddr {
        self.handle.local_addr()
    }

    /// Serve until [`Self::cancel_token`] is cancelled. Revokes routes on exit.
    pub async fn serve(self) {
        let cancel_serve = self.cancel.clone();
        let handle = self.handle;
        let _ = axum::serve(self.listener, self.app)
            .with_graceful_shutdown(async move {
                cancel_serve.cancelled().await;
            })
            .await;
        handle.revoke_all_services();
    }
}

/// MCP gateway constructors (no ambient spawn — Law 23).
///
/// Production RuntimeOwner uses [`PreparedMcpGateway`] under
/// `TaskClass::RuntimeService`. Standalone tests prepare + spawn explicitly.
pub struct McpGateway;

impl McpGateway {
    /// Build from a pre-bound non-blocking std listener (fail-closed startup bind).
    pub fn prepare_from_std_listener(
        std_listener: std::net::TcpListener,
        max_routes: usize,
        request_owner: Option<Arc<dyn McpRequestOwner>>,
    ) -> Result<PreparedMcpGateway, McpInstallError> {
        Self::prepare_from_std_listener_with_limits(
            std_listener,
            max_routes,
            request_owner,
            McpGatewayLimits::default(),
        )
    }

    /// [`Self::prepare_from_std_listener`] with explicit concurrency/duration limits.
    pub fn prepare_from_std_listener_with_limits(
        std_listener: std::net::TcpListener,
        max_routes: usize,
        request_owner: Option<Arc<dyn McpRequestOwner>>,
        limits: McpGatewayLimits,
    ) -> Result<PreparedMcpGateway, McpInstallError> {
        let listener = tokio::net::TcpListener::from_std(std_listener)
            .map_err(|_| McpInstallError::InvalidDescriptor)?;
        Self::prepare_from_tokio_listener_with_limits(listener, max_routes, request_owner, limits)
    }

    /// Build from an already-bound Tokio loopback listener (no spawn).
    pub fn prepare_from_tokio_listener(
        listener: tokio::net::TcpListener,
        max_routes: usize,
        request_owner: Option<Arc<dyn McpRequestOwner>>,
    ) -> Result<PreparedMcpGateway, McpInstallError> {
        Self::prepare_from_tokio_listener_with_limits(
            listener,
            max_routes,
            request_owner,
            McpGatewayLimits::default(),
        )
    }

    /// [`Self::prepare_from_tokio_listener`] with explicit concurrency/duration limits.
    pub fn prepare_from_tokio_listener_with_limits(
        listener: tokio::net::TcpListener,
        max_routes: usize,
        request_owner: Option<Arc<dyn McpRequestOwner>>,
        limits: McpGatewayLimits,
    ) -> Result<PreparedMcpGateway, McpInstallError> {
        let limits = limits.validated()?;
        let local_addr = listener
            .local_addr()
            .map_err(|_| McpInstallError::InvalidDescriptor)?;
        if !local_addr.ip().is_loopback() {
            return Err(McpInstallError::InvalidDescriptor);
        }

        let routes = McpRouteTable::new(max_routes);
        let services = Arc::new(std::sync::Mutex::new(std::collections::HashMap::new()));
        let request_permits =
            Arc::new(tokio::sync::Semaphore::new(limits.max_global_requests));
        let base_url = format!("http://{}", local_addr);
        let cancel = CancellationToken::new();
        let state = GatewayState {
            routes: Arc::clone(&routes),
            services: Arc::clone(&services),
            request_permits: Arc::clone(&request_permits),
            request_owner,
            max_per_capability_requests: limits.max_per_capability_requests,
            request_duration: limits.request_duration,
        };

        let app = Router::new()
            .route("/mcp/{token}", any(mcp_dispatch))
            .route("/mcp/{token}/{*rest}", any(mcp_dispatch_rest))
            .with_state(state);

        Ok(PreparedMcpGateway {
            handle: McpGatewayHandle {
                routes,
                services,
                request_permits,
                base_url,
                local_addr,
            },
            cancel,
            listener,
            app,
        })
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

const DEFAULT_MAX_GLOBAL_MCP_REQUESTS: usize = 64;
const DEFAULT_MAX_PER_CAPABILITY_MCP_REQUESTS: usize = 8;
const DEFAULT_MCP_REQUEST_DURATION: std::time::Duration = std::time::Duration::from_secs(30);

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
    // Cheap fail-closed route lookup only — concurrency budget + body buffering
    // run inside the owned McpRequest task so permits cannot outlive the handler
    // if the axum task is dropped (Law 22 / §17).
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

    let transaction_id = binding.transaction_id;
    let state_work = state.clone();
    let work = async move { execute_mcp_request(state_work, canonical, binding, req).await };

    // RuntimeOwner path: each active request is TaskClass::McpRequest (§17).
    if let Some(owner) = state.request_owner.as_ref() {
        owner.run_owned(transaction_id, Box::pin(work)).await
    } else {
        work.await
    }
}

/// Permit acquire + body buffer + Streamable HTTP handle (owned-task body).
async fn execute_mcp_request(
    state: GatewayState,
    canonical: String,
    binding: Arc<super::binding::McpBinding>,
    req: Request,
) -> Response<Body> {
    // Re-check route after spawn (may have been revoked).
    if state.routes.get_by_hex(&canonical).is_none() {
        drop_capability_service(&state.services, &canonical);
        return Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::from("unknown capability"))
            .unwrap_or_else(|_| Response::new(Body::empty()));
    }

    let Ok(_global) = state.request_permits.clone().try_acquire_owned() else {
        return Response::builder()
            .status(StatusCode::TOO_MANY_REQUESTS)
            .body(Body::from("mcp gateway concurrency exceeded"))
            .unwrap_or_else(|_| Response::new(Body::empty()));
    };

    let max_per_capability = state.max_per_capability_requests;
    let service = {
        let mut map = state.services.lock().unwrap_or_else(|e| e.into_inner());
        map.entry(canonical.clone())
            .or_insert_with(|| {
                let handler = binding.handler.clone();
                let cancel = CancellationToken::new();
                let mut config = StreamableHttpServerConfig::default();
                config.cancellation_token = cancel.clone();
                config.sse_keep_alive = None;
                config.sse_retry = None;
                config.json_response = true;
                Arc::new(CapabilityHttpService {
                    service: StreamableHttpService::new(
                        move || Ok(handler.clone()),
                        Arc::new(LocalSessionManager::default()),
                        config,
                    ),
                    cancel,
                    permits: Arc::new(tokio::sync::Semaphore::new(max_per_capability)),
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

    let deadline_at = tokio::time::Instant::now() + state.request_duration;
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
