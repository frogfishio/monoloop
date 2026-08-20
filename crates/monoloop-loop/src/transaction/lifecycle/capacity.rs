//! RAII transaction reservations (v2 §9.3).
//!
//! Counter-only acquire/release APIs are forbidden. Zero capacities are rejected
//! at construction — never silently substituted with `.max(1)`.

use monoloop_contracts::ChannelId;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Runtime-wide reservation pool installed at start.
#[derive(Debug)]
pub struct ReservationPool {
    max_global: usize,
    max_ledger: usize,
    global_active: AtomicUsize,
    ledger_active: AtomicUsize,
    per_channel: HashMap<ChannelId, ChannelSlot>,
}

#[derive(Debug)]
struct ChannelSlot {
    max: usize,
    active: AtomicUsize,
}

/// Invalid reservation-pool construction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReservationPoolError {
    /// Global capacity was zero.
    ZeroGlobal,
    /// A per-channel capacity was zero.
    ZeroChannel,
}

/// RAII permits held for one admitted transaction until cleanup releases them.
#[derive(Debug)]
pub struct TransactionReservations {
    pool: Arc<ReservationPool>,
    channel_id: ChannelId,
    global: Option<GlobalPermit>,
    channel: Option<ChannelPermit>,
    ledger: Option<LedgerPermit>,
}

#[derive(Debug)]
struct GlobalPermit {
    pool: Arc<ReservationPool>,
}

#[derive(Debug)]
struct ChannelPermit {
    pool: Arc<ReservationPool>,
    channel_id: ChannelId,
}

#[derive(Debug)]
struct LedgerPermit {
    pool: Arc<ReservationPool>,
}

impl Drop for GlobalPermit {
    fn drop(&mut self) {
        self.pool.global_active.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Drop for ChannelPermit {
    fn drop(&mut self) {
        if let Some(ch) = self.pool.per_channel.get(&self.channel_id) {
            ch.active.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

impl Drop for LedgerPermit {
    fn drop(&mut self) {
        self.pool.ledger_active.fetch_sub(1, Ordering::SeqCst);
    }
}

impl ReservationPool {
    /// Build from global and per-channel maxima. Ledger capacity equals global.
    ///
    /// Returns an error when any capacity is zero (fail closed; no silent bump).
    pub fn try_new(
        max_global: usize,
        channels: impl IntoIterator<Item = (ChannelId, usize)>,
    ) -> Result<Arc<Self>, ReservationPoolError> {
        if max_global == 0 {
            return Err(ReservationPoolError::ZeroGlobal);
        }
        let mut per_channel = HashMap::new();
        for (id, max) in channels {
            if max == 0 {
                return Err(ReservationPoolError::ZeroChannel);
            }
            per_channel.insert(
                id,
                ChannelSlot {
                    max,
                    active: AtomicUsize::new(0),
                },
            );
        }
        Ok(Arc::new(Self {
            max_global,
            max_ledger: max_global,
            global_active: AtomicUsize::new(0),
            ledger_active: AtomicUsize::new(0),
            per_channel,
        }))
    }

    /// Observability: global active reservations.
    pub fn global_active(&self) -> usize {
        self.global_active.load(Ordering::SeqCst)
    }

    /// Observability: ledger entries reserved.
    pub fn ledger_active(&self) -> usize {
        self.ledger_active.load(Ordering::SeqCst)
    }

    /// Observability: active reservations for one Channel.
    pub fn channel_active(&self, id: &ChannelId) -> usize {
        self.per_channel
            .get(id)
            .map(|ch| ch.active.load(Ordering::SeqCst))
            .unwrap_or(0)
    }

    /// Configured global maximum.
    pub fn max_global(&self) -> usize {
        self.max_global
    }

    /// Try to acquire the full reservation bundle without waiting.
    pub fn try_reserve(self: &Arc<Self>, channel: &ChannelId) -> Option<TransactionReservations> {
        let ch = self.per_channel.get(channel)?;

        // Global
        loop {
            let g = self.global_active.load(Ordering::SeqCst);
            if g >= self.max_global {
                return None;
            }
            if self
                .global_active
                .compare_exchange(g, g + 1, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                break;
            }
        }
        let global = GlobalPermit {
            pool: Arc::clone(self),
        };

        // Channel
        loop {
            let c = ch.active.load(Ordering::SeqCst);
            if c >= ch.max {
                drop(global);
                return None;
            }
            if ch
                .active
                .compare_exchange(c, c + 1, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                break;
            }
        }
        let channel_permit = ChannelPermit {
            pool: Arc::clone(self),
            channel_id: channel.clone(),
        };

        // Ledger slot
        loop {
            let l = self.ledger_active.load(Ordering::SeqCst);
            if l >= self.max_ledger {
                drop(channel_permit);
                drop(global);
                return None;
            }
            if self
                .ledger_active
                .compare_exchange(l, l + 1, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                break;
            }
        }
        let ledger = LedgerPermit {
            pool: Arc::clone(self),
        };

        Some(TransactionReservations {
            pool: Arc::clone(self),
            channel_id: channel.clone(),
            global: Some(global),
            channel: Some(channel_permit),
            ledger: Some(ledger),
        })
    }
}

impl TransactionReservations {
    /// Channel this reservation was taken for.
    pub fn channel_id(&self) -> &ChannelId {
        &self.channel_id
    }

    /// Explicitly release (also runs on Drop).
    pub fn release(self) {
        drop(self);
    }
}

impl Drop for TransactionReservations {
    fn drop(&mut self) {
        self.ledger.take();
        self.channel.take();
        self.global.take();
        let _ = &self.pool;
    }
}
