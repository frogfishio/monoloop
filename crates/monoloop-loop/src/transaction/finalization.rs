//! Exactly-once finalization guard and event sequence allocation.

use monoloop_contracts::{
    ChannelId, CompletionCallback, EventDeliveryOutcome, SessionId, TransactionEnd,
    TransactionEndKind, TransactionId, TransactionUsage,
};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::Notify;

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

    /// Next sequence that [`Self::allocate`] will return (D-036 peek-before-enqueue).
    pub fn peek_next(&self) -> u64 {
        self.next.load(Ordering::SeqCst)
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
    /// Wakes supervisor waiting for a mid-finalize restore after actor abort.
    restore_notify: Notify,
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
            restore_notify: Notify::new(),
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
        self.payload.lock().ok().and_then(|mut g| g.take())
    }

    /// Restore a claimed-but-not-scheduled payload so the supervisor can reclaim
    /// after an aborted mid-finalize actor (D-029).
    pub fn restore_unscheduled(&self, payload: FinalizationPayload) {
        if self.callback_scheduled.load(Ordering::SeqCst) {
            return;
        }
        if let Ok(mut g) = self.payload.lock() {
            *g = Some(payload);
        }
        self.claimed.store(false, Ordering::SeqCst);
        self.restore_notify.notify_waiters();
    }

    /// Whether already claimed.
    pub fn is_claimed(&self) -> bool {
        self.claimed.load(Ordering::SeqCst)
    }

    /// Mark that a callback was scheduled (accounting).
    pub fn mark_callback_scheduled(&self) {
        self.callback_scheduled.store(true, Ordering::SeqCst);
        self.restore_notify.notify_waiters();
    }

    /// Whether callback was scheduled.
    pub fn callback_was_scheduled(&self) -> bool {
        self.callback_scheduled.load(Ordering::SeqCst)
    }

    /// Claim for shutdown after actor abort: wait (within `budget`) for a possible
    /// restore from [`ClaimedFinalization`] Drop, without racing a bare try_claim.
    pub(crate) async fn claim_for_shutdown(&self, budget: Duration) -> Option<FinalizationPayload> {
        if let Some(p) = self.try_claim() {
            return Some(p);
        }
        if self.callback_was_scheduled() {
            return None;
        }
        if budget.is_zero() {
            return self.try_claim();
        }
        let deadline = Instant::now() + budget;
        loop {
            if let Some(p) = self.try_claim() {
                return Some(p);
            }
            if self.callback_was_scheduled() {
                return None;
            }
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return self.try_claim();
            }
            tokio::select! {
                biased;
                _ = self.restore_notify.notified() => {}
                _ = tokio::time::sleep(left) => {
                    return self.try_claim();
                }
            }
        }
    }
}

/// Holds a claimed payload; restores it on drop unless consumed for schedule.
pub(crate) struct ClaimedFinalization {
    guard: Arc<FinalizationGuard>,
    payload: Option<FinalizationPayload>,
}

impl ClaimedFinalization {
    pub(crate) fn new(guard: Arc<FinalizationGuard>, payload: FinalizationPayload) -> Self {
        Self {
            guard,
            payload: Some(payload),
        }
    }

    pub(crate) fn payload(&self) -> &FinalizationPayload {
        self.payload.as_ref().expect("claimed payload present")
    }

    /// Take the payload for callback scheduling (disarms restore-on-drop).
    pub(crate) fn take(mut self) -> FinalizationPayload {
        self.payload.take().expect("claimed payload present")
    }
}

impl Drop for ClaimedFinalization {
    fn drop(&mut self) {
        if let Some(p) = self.payload.take() {
            self.guard.restore_unscheduled(p);
        }
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

/// Bound a diagnostic list by count and per-message bytes (D-015).
pub fn bound_diagnostics(
    mut diagnostics: Vec<monoloop_contracts::TransactionDiagnostic>,
    max_count: usize,
    max_message_bytes: usize,
) -> Vec<monoloop_contracts::TransactionDiagnostic> {
    if diagnostics.len() > max_count {
        diagnostics.truncate(max_count.max(1));
    }
    for d in &mut diagnostics {
        if let Some(ref mut msg) = d.diagnostic.message {
            if msg.len() > max_message_bytes {
                msg.truncate(max_message_bytes);
                while !msg.is_char_boundary(msg.len()) {
                    msg.pop();
                }
            }
        }
    }
    diagnostics
}
