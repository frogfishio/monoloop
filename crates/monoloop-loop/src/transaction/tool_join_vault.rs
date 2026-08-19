//! Retains tool worker joins (+ permits) when dispatch is dropped before join
//! completes, so workers are not detached while concurrency capacity is freed.
//!
//! Each [`DefaultTransactionRuntime`] owns its own vault — never process-global.

use super::tool_capacity::ToolPermit;
use std::sync::{Arc, Mutex};
use tokio::task::JoinHandle;

struct VaultItem {
    join: JoinHandle<()>,
    permit: Option<ToolPermit>,
}

/// Runtime-scoped vault for unfinished tool joins (drained at that runtime's shutdown).
#[derive(Clone, Default)]
pub struct ToolJoinVault {
    items: Arc<Mutex<Vec<VaultItem>>>,
}

impl ToolJoinVault {
    pub fn new() -> Self {
        Self::default()
    }

    /// Park an unfinished worker join, holding its concurrency permit until joined.
    pub fn park(&self, join: JoinHandle<()>, permit: Option<ToolPermit>) {
        if join.is_finished() {
            drop(join);
            drop(permit);
            return;
        }
        self.items
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(VaultItem { join, permit });
    }

    /// Abort and join parked workers within `budget`. Unfinished items stay
    /// parked with their permits (fail closed on concurrency).
    pub async fn drain(&self, budget: std::time::Duration) {
        let mut items = {
            let mut g = self.items.lock().unwrap_or_else(|e| e.into_inner());
            std::mem::take(&mut *g)
        };
        let start = tokio::time::Instant::now();
        let mut still = Vec::new();
        for mut item in items.drain(..) {
            item.join.abort();
            let left = budget.saturating_sub(start.elapsed());
            if left.is_zero() {
                still.push(item);
                continue;
            }
            match tokio::time::timeout(left, &mut item.join).await {
                Ok(_) => {
                    drop(item.permit);
                }
                Err(_) => still.push(item),
            }
        }
        if !still.is_empty() {
            *self.items.lock().unwrap_or_else(|e| e.into_inner()) = still;
        }
    }
}
