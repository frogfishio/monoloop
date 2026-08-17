//! Bounded global, per-transaction, and per-tool concurrency for linked tools.

use monoloop_contracts::ToolId;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

/// Shared process-wide concurrent tool execution limit.
#[derive(Debug)]
pub struct SharedToolCapacity {
    max: usize,
    active: AtomicUsize,
}

impl SharedToolCapacity {
    /// Create with a maximum concurrent executions across all transactions.
    pub fn new(max: usize) -> Arc<Self> {
        Arc::new(Self {
            max: max.max(1),
            active: AtomicUsize::new(0),
        })
    }

    /// Unlimited (practical) capacity for isolated tests.
    pub fn unlimited() -> Arc<Self> {
        Self::new(usize::MAX / 4)
    }

    fn try_acquire(&self) -> bool {
        loop {
            let cur = self.active.load(Ordering::SeqCst);
            if cur >= self.max {
                return false;
            }
            if self
                .active
                .compare_exchange(cur, cur + 1, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                return true;
            }
        }
    }

    fn release(&self) {
        self.active.fetch_sub(1, Ordering::SeqCst);
    }

    /// Current active count.
    pub fn active(&self) -> usize {
        self.active.load(Ordering::SeqCst)
    }
}

/// Per-transaction tool capacity tracker (item queue + concurrency + per-tool).
#[derive(Debug)]
pub struct TransactionToolCapacity {
    shared: Arc<SharedToolCapacity>,
    max_concurrent: usize,
    max_queued: usize,
    txn_active: AtomicUsize,
    txn_queued: AtomicUsize,
    per_tool: Mutex<HashMap<ToolId, ToolSlot>>,
}

#[derive(Debug, Default)]
struct ToolSlot {
    max_concurrent: usize,
    active: usize,
}

/// RAII permit for one running tool execution.
pub struct ToolPermit {
    shared: Arc<SharedToolCapacity>,
    txn: Arc<TransactionToolCapacity>,
    tool_id: ToolId,
}

impl Drop for ToolPermit {
    fn drop(&mut self) {
        self.shared.release();
        self.txn.txn_active.fetch_sub(1, Ordering::SeqCst);
        if let Ok(mut map) = self.txn.per_tool.lock() {
            if let Some(slot) = map.get_mut(&self.tool_id) {
                slot.active = slot.active.saturating_sub(1);
            }
        }
    }
}

impl TransactionToolCapacity {
    /// Build for one transaction.
    pub fn new(
        shared: Arc<SharedToolCapacity>,
        max_concurrent: usize,
        max_queued: usize,
    ) -> Arc<Self> {
        Arc::new(Self {
            shared,
            max_concurrent: max_concurrent.max(1),
            max_queued: max_queued.max(1),
            txn_active: AtomicUsize::new(0),
            txn_queued: AtomicUsize::new(0),
            per_tool: Mutex::new(HashMap::new()),
        })
    }

    /// Register per-tool max concurrency from a resolved set.
    pub fn configure_tool(self: &Arc<Self>, tool_id: ToolId, max_concurrent: usize) {
        let mut map = self.per_tool.lock().unwrap_or_else(|e| e.into_inner());
        map.insert(
            tool_id,
            ToolSlot {
                max_concurrent: max_concurrent.max(1),
                active: 0,
            },
        );
    }

    /// Reserve a queue slot before validation work (released on failure or after acquire).
    pub fn try_enqueue(self: &Arc<Self>) -> bool {
        loop {
            let q = self.txn_queued.load(Ordering::SeqCst);
            if q >= self.max_queued {
                return false;
            }
            if self
                .txn_queued
                .compare_exchange(q, q + 1, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                return true;
            }
        }
    }

    /// Drop a prior queue reservation without starting.
    pub fn dequeue(self: &Arc<Self>) {
        self.txn_queued.fetch_sub(1, Ordering::SeqCst);
    }

    /// Move from queued to running if capacity allows.
    pub fn try_acquire(self: &Arc<Self>, tool_id: &ToolId) -> Option<ToolPermit> {
        // Per-tool + txn concurrency.
        {
            let mut map = self.per_tool.lock().unwrap_or_else(|e| e.into_inner());
            let slot = map.entry(tool_id.clone()).or_insert(ToolSlot {
                max_concurrent: 1,
                active: 0,
            });
            if slot.active >= slot.max_concurrent {
                return None;
            }
            let txn = self.txn_active.load(Ordering::SeqCst);
            if txn >= self.max_concurrent {
                return None;
            }
            if !self.shared.try_acquire() {
                return None;
            }
            slot.active += 1;
            self.txn_active.fetch_add(1, Ordering::SeqCst);
            self.txn_queued.fetch_sub(1, Ordering::SeqCst);
        }
        Some(ToolPermit {
            shared: Arc::clone(&self.shared),
            txn: Arc::clone(self),
            tool_id: tool_id.clone(),
        })
    }

    /// Active executions in this transaction.
    pub fn active(&self) -> usize {
        self.txn_active.load(Ordering::SeqCst)
    }

    /// Queued starts waiting for concurrency.
    pub fn queued(&self) -> usize {
        self.txn_queued.load(Ordering::SeqCst)
    }
}
