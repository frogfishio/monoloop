//! Retains tool worker joins (+ permits) when dispatch is dropped before join
//! completes, so workers are not detached while concurrency capacity is freed.
//!
//! Each [`crate::DefaultTransactionRuntime`] owns its own vault — never process-global.

use super::tool_capacity::ToolPermit;
use std::sync::{Arc, Mutex};
use tokio::task::JoinHandle;

enum VaultItem {
    /// Real worker join — may be aborted on drain.
    Worker {
        join: JoinHandle<()>,
        permit: Option<ToolPermit>,
    },
    /// Capacity hold without a killable worker join (handler contract violation).
    /// Drain waits without aborting a fabricated waiter.
    OrphanPermit { permit: ToolPermit },
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

    /// Drop permits for workers that have already finished (normal-operation reap).
    pub fn reap_finished(&self) {
        let mut g = self.items.lock().unwrap_or_else(|e| e.into_inner());
        let mut keep = Vec::with_capacity(g.len());
        for item in g.drain(..) {
            match item {
                VaultItem::Worker { join, permit } if join.is_finished() => {
                    drop(join);
                    drop(permit);
                }
                other => keep.push(other),
            }
        }
        *g = keep;
    }

    /// Park an unfinished worker join, holding its concurrency permit until joined.
    pub fn park(&self, join: JoinHandle<()>, permit: Option<ToolPermit>) {
        self.reap_finished();
        if join.is_finished() {
            drop(join);
            drop(permit);
            return;
        }
        self.items
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(VaultItem::Worker { join, permit });
    }

    /// Hold a permit with no worker join (missing-kill contract violation).
    ///
    /// Capacity stays occupied until shutdown drain — we must not release it by
    /// aborting a fabricated completion waiter while real work may still run.
    pub fn park_orphan_permit(&self, permit: ToolPermit) {
        self.reap_finished();
        self.items
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(VaultItem::OrphanPermit { permit });
    }

    /// Abort and join parked workers within `budget`. Unfinished items stay
    /// parked with their permits (fail closed on concurrency). Orphan permits
    /// are released only when `release_orphans` is true (runtime shutdown end).
    pub async fn drain(&self, budget: std::time::Duration, release_orphans: bool) {
        self.reap_finished();
        let mut items = {
            let mut g = self.items.lock().unwrap_or_else(|e| e.into_inner());
            std::mem::take(&mut *g)
        };
        let start = tokio::time::Instant::now();
        let mut still = Vec::new();
        for item in items.drain(..) {
            match item {
                VaultItem::Worker { mut join, permit } => {
                    join.abort();
                    let left = budget.saturating_sub(start.elapsed());
                    if left.is_zero() {
                        if join.is_finished() {
                            drop(join);
                            drop(permit);
                        } else {
                            still.push(VaultItem::Worker { join, permit });
                        }
                        continue;
                    }
                    match tokio::time::timeout(left, &mut join).await {
                        Ok(_) => {
                            drop(permit);
                        }
                        Err(_) => still.push(VaultItem::Worker { join, permit }),
                    }
                }
                VaultItem::OrphanPermit { permit } => {
                    if release_orphans {
                        drop(permit);
                    } else {
                        still.push(VaultItem::OrphanPermit { permit });
                    }
                }
            }
        }
        if !still.is_empty() {
            let mut g = self.items.lock().unwrap_or_else(|e| e.into_inner());
            g.extend(still);
        }
    }
}
