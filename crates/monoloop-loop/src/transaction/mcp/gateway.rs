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

/// Cloneable handle for install/activate/revoke without owning the listener.
#[derive(Clone)]
pub struct McpGatewayHandle {
    routes: Arc<McpRouteTable>,
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
        self.routes.revoke(token)
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
        let base_url = format!("http://{}", local_addr);
        let cancel = CancellationToken::new();
        let cancel_serve = cancel.clone();
        let routes_state = Arc::clone(&routes);

        let app = Router::new()
            .route("/mcp/{token}", any(mcp_dispatch))
            .route("/mcp/{token}/{*rest}", any(mcp_dispatch_rest))
            .with_state(routes_state);

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

    /// Shutdown: revoke all routes, stop listener, join serve task.
    pub async fn shutdown(self) {
        self.handle.routes.revoke_all();
        self.cancel.cancel();
        let _ = self.join.await;
    }
}

async fn mcp_dispatch(
    State(routes): State<Arc<McpRouteTable>>,
    Path(token): Path<String>,
    req: Request,
) -> Response<Body> {
    forward_mcp(routes, &token, req).await
}

async fn mcp_dispatch_rest(
    State(routes): State<Arc<McpRouteTable>>,
    Path((token, _rest)): Path<(String, String)>,
    req: Request,
) -> Response<Body> {
    forward_mcp(routes, &token, req).await
}

async fn forward_mcp(
    routes: Arc<McpRouteTable>,
    token_hex: &str,
    req: Request,
) -> Response<Body> {
    let Some(binding) = routes.get_by_hex(token_hex) else {
        return Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::from("unknown capability"))
            .unwrap_or_else(|_| Response::new(Body::empty()));
    };

    let handler = binding.handler.clone();
    let cancel = CancellationToken::new();
    let mut config = StreamableHttpServerConfig::default();
    config.cancellation_token = cancel;
    config.sse_keep_alive = None;
    config.sse_retry = None;

    let service: StreamableHttpService<TransactionMcpHandler, LocalSessionManager> =
        StreamableHttpService::new(
            move || Ok(handler.clone()),
            Arc::new(LocalSessionManager::default()),
            config,
        );

    let req = rewrite_path(req, token_hex);
    let response = service.handle(req).await;
    response.map(Body::new)
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
