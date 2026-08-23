//! MCP gateway: loopback Streamable HTTP + transaction capability routing (WP-07).

mod binding;
mod gateway;
mod handler;

pub use binding::{
    CapabilityToken, McpBindingState, McpInstallError, McpRouteTable, PendingMcpBinding,
};
pub use gateway::{
    McpGateway, McpGatewayHandle, McpGatewayLimits, McpRequestOwner, PreparedMcpGateway,
};
pub use handler::{tool_definitions_from_resolved, TransactionMcpHandler};
