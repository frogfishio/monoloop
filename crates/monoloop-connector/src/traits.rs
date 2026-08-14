//! Abstract Connector factory contract.

use crate::descriptor::ConnectorDescriptor;
use crate::open::{OpenConnection, PendingRawConnection};

/// Reusable connector factory. Contains no ambient current connection.
pub trait Connector: Send + Sync {
    /// Immutable implementation descriptor.
    fn descriptor(&self) -> &ConnectorDescriptor;

    /// Begin opening a connection. Returns immediately with control available.
    fn begin_open(&self, request: OpenConnection) -> PendingRawConnection;
}
