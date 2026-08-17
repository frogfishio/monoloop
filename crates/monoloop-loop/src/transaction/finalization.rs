//! Exactly-once finalization guard and event sequence allocation.

use monoloop_contracts::{
    ChannelId, CompletionCallback, EventDeliveryOutcome, SessionId, TransactionEnd,
    TransactionEndKind, TransactionId, TransactionUsage,
};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// Sole allocator of transaction event sequence numbers (starts at 1).
#[derive(Debug)]
pub struct EventSequencer {
    next: AtomicU64,
}

impl EventSequencer {
    /// Create a sequencer; first allocated sequence is 1.
    pub fn new() -> Self {
        Self {
            next: AtomicU64::new(1),
        }
    }

    /// Allocate the next contiguous sequence number.
    pub fn allocate(&self) -> u64 {
        self.next.fetch_add(1, Ordering::SeqCst)
    }

    /// Last allocated sequence (0 if none).
    pub fn last_allocated(&self) -> u64 {
        self.next.load(Ordering::SeqCst).saturating_sub(1)
    }
}

impl Default for EventSequencer {
    fn default() -> Self {
        Self::new()
    }
}

/// Material taken exactly once by the winning finalization path.
pub struct FinalizationPayload {
    /// Completion callback.
    pub callback: Box<dyn CompletionCallback>,
    /// Channel id for terminal.
    pub channel_id: ChannelId,
    /// Session when known.
    pub session_id: Option<SessionId>,
    /// Transaction id.
    pub transaction_id: TransactionId,
}

/// Atomic exactly-once finalization claim shared by actor and shutdown supervisor.
pub struct FinalizationGuard {
    claimed: AtomicBool,
    payload: Mutex<Option<FinalizationPayload>>,
    sequencer: Arc<EventSequencer>,
    /// Whether the callback was scheduled (tests / shutdown accounting).
    callback_scheduled: AtomicBool,
}

impl FinalizationGuard {
    /// Create a guard holding the one-shot callback.
    pub fn new(
        transaction_id: TransactionId,
        channel_id: ChannelId,
        session_id: Option<SessionId>,
        callback: Box<dyn CompletionCallback>,
        sequencer: Arc<EventSequencer>,
    ) -> Arc<Self> {
        Arc::new(Self {
            claimed: AtomicBool::new(false),
            payload: Mutex::new(Some(FinalizationPayload {
                callback,
                channel_id,
                session_id,
                transaction_id,
            })),
            sequencer,
            callback_scheduled: AtomicBool::new(false),
        })
    }

    /// Event sequencer for this transaction.
    pub fn sequencer(&self) -> &Arc<EventSequencer> {
        &self.sequencer
    }

    /// Update session id on the payload before claim (session establishment).
    pub fn set_session_id(&self, session_id: SessionId) {
        if let Ok(mut g) = self.payload.lock() {
            if let Some(p) = g.as_mut() {
                p.session_id = Some(session_id);
            }
        }
    }

    /// Claim exactly once. Winner takes payload; losers get `None`.
    pub fn try_claim(&self) -> Option<FinalizationPayload> {
        if self
            .claimed
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return None;
        }
        self.payload
            .lock()
            .ok()
            .and_then(|mut g| g.take())
    }

    /// Whether already claimed.
    pub fn is_claimed(&self) -> bool {
        self.claimed.load(Ordering::SeqCst)
    }

    /// Mark that a callback was scheduled (accounting).
    pub fn mark_callback_scheduled(&self) {
        self.callback_scheduled.store(true, Ordering::SeqCst);
    }

    /// Whether callback was scheduled.
    pub fn callback_was_scheduled(&self) -> bool {
        self.callback_scheduled.load(Ordering::SeqCst)
    }
}

/// Build a terminal end event payload fields helper.
pub fn build_transaction_end(
    payload: &FinalizationPayload,
    kind: TransactionEndKind,
    prior: Option<TransactionEndKind>,
    event_delivery: EventDeliveryOutcome,
    emitted_events: u64,
) -> TransactionEnd {
    TransactionEnd {
        transaction_id: payload.transaction_id,
        session_id: payload.session_id.clone(),
        channel_id: payload.channel_id.clone(),
        kind,
        prior_terminal_cause: prior,
        event_delivery,
        emitted_events,
        usage: TransactionUsage::default(),
        diagnostics: vec![],
    }
}
