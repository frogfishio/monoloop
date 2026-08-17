//! Global and per-Channel active-transaction capacity (reserved at admission in WP-04).

use monoloop_contracts::ChannelId;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Capacity counters installed at startup; admission acquires permits later.
#[derive(Debug)]
pub struct CapacityManagers {
    max_global: usize,
    global_active: AtomicUsize,
    per_channel: HashMap<ChannelId, ChannelCapacity>,
}

#[derive(Debug)]
struct ChannelCapacity {
    max: usize,
    active: AtomicUsize,
}

impl CapacityManagers {
    /// Build managers from runtime and Channel limits.
    pub fn new(
        max_global: usize,
        channels: impl IntoIterator<Item = (ChannelId, usize)>,
    ) -> Self {
        let mut per_channel = HashMap::new();
        for (id, max) in channels {
            per_channel.insert(
                id,
                ChannelCapacity {
                    max,
                    active: AtomicUsize::new(0),
                },
            );
        }
        Self {
            max_global,
            global_active: AtomicUsize::new(0),
            per_channel,
        }
    }

    /// Global active count (observability / tests).
    pub fn global_active(&self) -> usize {
        self.global_active.load(Ordering::SeqCst)
    }

    /// Per-Channel active count.
    pub fn channel_active(&self, id: &ChannelId) -> Option<usize> {
        self.per_channel
            .get(id)
            .map(|c| c.active.load(Ordering::SeqCst))
    }

    /// Configured global max.
    pub fn max_global(&self) -> usize {
        self.max_global
    }

    /// Try reserve one global + channel slot (WP-04 will use; available for tests).
    pub fn try_reserve(self: &Arc<Self>, channel: &ChannelId) -> bool {
        let Some(ch) = self.per_channel.get(channel) else {
            return false;
        };
        // Optimistic CAS loops.
        loop {
            let g = self.global_active.load(Ordering::SeqCst);
            if g >= self.max_global {
                return false;
            }
            if self
                .global_active
                .compare_exchange(g, g + 1, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                break;
            }
        }
        loop {
            let c = ch.active.load(Ordering::SeqCst);
            if c >= ch.max {
                self.global_active.fetch_sub(1, Ordering::SeqCst);
                return false;
            }
            if ch
                .active
                .compare_exchange(c, c + 1, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                return true;
            }
        }
    }

    /// Release a prior reservation.
    pub fn release(&self, channel: &ChannelId) {
        if let Some(ch) = self.per_channel.get(channel) {
            ch.active.fetch_sub(1, Ordering::SeqCst);
        }
        self.global_active.fetch_sub(1, Ordering::SeqCst);
    }
}
