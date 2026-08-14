//! Loop runtime: state machine, empty-registry dispatch, terminal accounting.

use crate::registry::{EmptyToolRegistry, ResolveToolRequest, ToolRegistry, ToolResolution};
use crate::subscription::{CanonicalEventSubscription, SubscriptionStatus};
use crate::tools::{NoToolRuntime, ToolRuntime};
use monoloop_contracts::{
    CanonicalUnit, CanonicalUnitEvent, InterpretationEnd, InterpreterOutputEvent, LoopEnd,
    LoopEndKind, LoopError, LoopId, LoopLimits, LoopOutputEvent, LoopScope, MonoloopRunId,
    OutboundToolOutcome, OutboundToolResult, ToolActionId, ToolRequestState, UnitId,
};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, Mutex};

/// Start request for one Loop instance.
pub struct StartLoop {
    /// Owning run.
    pub monoloop_run_id: MonoloopRunId,
    /// Loop id.
    pub loop_id: LoopId,
    /// Admission scope.
    pub scope: LoopScope,
    /// Lossless subscription (exclusive to this Loop).
    pub subscription: CanonicalEventSubscription,
    /// Tool registry.
    pub tool_registry: Arc<dyn ToolRegistry>,
    /// Tool runtime.
    pub tool_runtime: Arc<dyn ToolRuntime>,
    /// Output sink capacity (loop publishes here).
    pub output_capacity: usize,
    /// Limits.
    pub limits: LoopLimits,
}

/// Live loop handle.
pub struct LoopHandle {
    /// Loop identity.
    pub loop_id: LoopId,
    /// Control.
    pub control: LoopControl,
    /// Health counters.
    pub health: LoopHealth,
    /// Completion.
    pub completion: LoopCompletion,
    /// Output events (independent of Interpreter stream).
    pub output: Mutex<mpsc::Receiver<LoopOutputEvent>>,
}

/// Cancellation control.
#[derive(Clone)]
pub struct LoopControl {
    cancelled: Arc<AtomicBool>,
    notify: Arc<tokio::sync::Notify>,
}

impl LoopControl {
    fn new() -> Self {
        Self {
            cancelled: Arc::new(AtomicBool::new(false)),
            notify: Arc::new(tokio::sync::Notify::new()),
        }
    }

    /// Request cancel (idempotent).
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
        self.notify.notify_waiters();
    }

    fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }
}

/// Content-free health.
#[derive(Clone, Debug, Default)]
pub struct LoopHealth {
    /// Events received.
    pub events_received: Arc<AtomicU64>,
    /// Tools resolved unavailable.
    pub tools_unavailable: Arc<AtomicU64>,
}

/// Completion handle.
pub struct LoopCompletion {
    rx: Mutex<Option<oneshot::Receiver<LoopEnd>>>,
}

impl LoopCompletion {
    /// Wait for exactly one LoopEnd.
    pub async fn wait(self) -> LoopEnd {
        let mut guard = self.rx.lock().await;
        let rx = guard.take().expect("LoopCompletion polled twice");
        rx.await.unwrap_or(LoopEnd {
            monoloop_run_id: MonoloopRunId::new("unknown"),
            loop_id: LoopId::new("unknown"),
            kind: LoopEndKind::InvariantFailed,
            delivery_events_received: 0,
            duplicate_events: 0,
            tools_unavailable: 0,
            outbound_results_emitted: 0,
            safe_diagnostics: vec!["completion dropped".into()],
        })
    }
}

/// Default runtime factory.
#[derive(Clone, Debug, Default)]
pub struct DefaultLoopRuntime;

impl DefaultLoopRuntime {
    /// Create.
    pub fn new() -> Self {
        Self
    }

    /// Start a loop with EmptyToolRegistry + NoToolRuntime defaults available via helpers.
    pub fn start(&self, request: StartLoop) -> Result<LoopHandle, LoopError> {
        spawn_loop(request)
    }

    /// Convenience: empty-tool composition.
    pub fn start_empty(
        &self,
        monoloop_run_id: MonoloopRunId,
        loop_id: LoopId,
        scope: LoopScope,
        subscription: CanonicalEventSubscription,
        limits: LoopLimits,
    ) -> Result<LoopHandle, LoopError> {
        self.start(StartLoop {
            monoloop_run_id,
            loop_id,
            scope,
            subscription,
            tool_registry: Arc::new(EmptyToolRegistry::new()),
            tool_runtime: Arc::new(NoToolRuntime::new()),
            output_capacity: limits.max_output_queue,
            limits,
        })
    }
}

fn spawn_loop(request: StartLoop) -> Result<LoopHandle, LoopError> {
    let control = LoopControl::new();
    let health = LoopHealth::default();
    let (out_tx, out_rx) = mpsc::channel(request.output_capacity.max(1));
    let (end_tx, end_rx) = oneshot::channel();

    let loop_id = request.loop_id.clone();
    let control_task = control.clone();
    let health_task = health.clone();

    tokio::spawn(async move {
        let mut owner = LoopOwner {
            monoloop_run_id: request.monoloop_run_id,
            loop_id: request.loop_id,
            scope: request.scope,
            registry: request.tool_registry,
            _runtime: request.tool_runtime,
            limits: request.limits,
            out_tx,
            control: control_task,
            health: health_task,
            last_seq: 0,
            actions: HashMap::new(),
            dedup: HashMap::new(),
            delivery_events: 0,
            duplicates: 0,
            tools_unavailable: 0,
            outbound_results: 0,
            diagnostics: Vec::new(),
            ended: false,
        };
        owner
            .run(request.subscription, end_tx)
            .await;
    });

    Ok(LoopHandle {
        loop_id,
        control,
        health,
        completion: LoopCompletion {
            rx: Mutex::new(Some(end_rx)),
        },
        output: Mutex::new(out_rx),
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ActionState {
    ObservedWaiting,
    RequestReady,
    Unavailable,
    Incomplete,
}

struct ActionRecord {
    tool_action_id: ToolActionId,
    unit_id: UnitId,
    last_generation: u64,
    state: ActionState,
    dispatched: bool,
}

struct LoopOwner {
    monoloop_run_id: MonoloopRunId,
    loop_id: LoopId,
    scope: LoopScope,
    registry: Arc<dyn ToolRegistry>,
    _runtime: Arc<dyn ToolRuntime>,
    limits: LoopLimits,
    out_tx: mpsc::Sender<LoopOutputEvent>,
    control: LoopControl,
    health: LoopHealth,
    last_seq: u64,
    actions: HashMap<String, ActionRecord>,
    dedup: HashMap<String, u64>,
    delivery_events: u64,
    duplicates: u64,
    tools_unavailable: u64,
    outbound_results: u64,
    diagnostics: Vec<String>,
    ended: bool,
}

impl LoopOwner {
    async fn run(
        &mut self,
        mut subscription: CanonicalEventSubscription,
        end_tx: oneshot::Sender<LoopEnd>,
    ) {
        loop {
            if self.control.is_cancelled() {
                self.finish(LoopEndKind::Cancelled, end_tx).await;
                return;
            }

            tokio::select! {
                biased;
                _ = self.control.notify.notified() => {
                    if self.control.is_cancelled() {
                        self.finish(LoopEndKind::Cancelled, end_tx).await;
                        return;
                    }
                }
                msg = subscription.recv() => {
                    match msg {
                        None => {
                            self.finish(LoopEndKind::Drained, end_tx).await;
                            return;
                        }
                        Some(Err(SubscriptionStatus::Gap(_)))
                        | Some(Err(SubscriptionStatus::Lost)) => {
                            self.finish(LoopEndKind::SubscriptionLost, end_tx).await;
                            return;
                        }
                        Some(Err(SubscriptionStatus::Opened | SubscriptionStatus::Closing)) => {}
                        Some(Ok(delivered)) => {
                            if let Err(kind) = self.on_delivered(delivered).await {
                                self.finish(kind, end_tx).await;
                                return;
                            }
                        }
                    }
                }
            }
        }
    }

    async fn on_delivered(
        &mut self,
        delivered: crate::subscription::DeliveredEvent,
    ) -> Result<(), LoopEndKind> {
        // Gap detection: sequences must be contiguous.
        if self.last_seq > 0 && delivered.delivery_sequence != self.last_seq + 1 {
            self.diag(format!(
                "delivery gap: expected {}, got {}",
                self.last_seq + 1,
                delivered.delivery_sequence
            ));
            return Err(LoopEndKind::SubscriptionLost);
        }
        self.last_seq = delivered.delivery_sequence;
        self.delivery_events += 1;
        self.health
            .events_received
            .fetch_add(1, Ordering::Relaxed);

        match delivered.event {
            InterpreterOutputEvent::Unit(ev) => self.on_unit(ev).await,
            InterpreterOutputEvent::Ended(end) => {
                self.on_interpretation_end(end).await?;
                // Source drained for this interpretation — loop may still end when subscription closes.
                Ok(())
            }
        }
    }

    async fn on_unit(&mut self, event: CanonicalUnitEvent) -> Result<(), LoopEndKind> {
        let snap = event.snapshot();
        if !self.in_scope(snap) {
            // Observe sequence only; do not mutate tool state.
            return Ok(());
        }

        match &snap.unit {
            CanonicalUnit::Tool(tool) => {
                let key = format!(
                    "{}:{}",
                    snap.interpretation_id.as_str(),
                    tool.tool_action_id.as_str()
                );
                let dig_key = format!("{}:{}", key, snap.unit_generation);
                if let Some(prev) = self.dedup.get(&dig_key) {
                    if *prev == snap.unit_generation {
                        self.duplicates += 1;
                        return Ok(());
                    }
                }
                if self.dedup.len() >= self.limits.max_dedup_entries {
                    return Err(LoopEndKind::InvariantFailed);
                }
                self.dedup.insert(dig_key, snap.unit_generation);

                match tool.request_state {
                    ToolRequestState::Assembling => {
                        self.track_waiting(&key, tool.tool_action_id.clone(), snap.unit_id.clone(), snap.unit_generation);
                    }
                    ToolRequestState::Ready => {
                        self.on_request_ready(
                            &key,
                            tool.tool_action_id.clone(),
                            snap.unit_id.clone(),
                            snap.unit_generation,
                            tool.tool_name.clone(),
                            tool.request_payload.clone(),
                            snap,
                        )
                        .await?;
                    }
                    ToolRequestState::Incomplete | ToolRequestState::Malformed => {
                        let rec = self.actions.entry(key).or_insert_with(|| ActionRecord {
                            tool_action_id: tool.tool_action_id.clone(),
                            unit_id: snap.unit_id.clone(),
                            last_generation: 0,
                            state: ActionState::Incomplete,
                            dispatched: false,
                        });
                        if snap.unit_generation >= rec.last_generation {
                            rec.last_generation = snap.unit_generation;
                            rec.state = ActionState::Incomplete;
                        }
                    }
                }
            }
            // Text, structure, etc. — observe only.
            _ => {}
        }
        Ok(())
    }

    fn track_waiting(
        &mut self,
        key: &str,
        tool_action_id: ToolActionId,
        unit_id: UnitId,
        generation: u64,
    ) {
        let rec = self.actions.entry(key.to_string()).or_insert_with(|| ActionRecord {
            tool_action_id,
            unit_id,
            last_generation: 0,
            state: ActionState::ObservedWaiting,
            dispatched: false,
        });
        if generation < rec.last_generation {
            return; // stale
        }
        rec.last_generation = generation;
        if rec.state != ActionState::Unavailable && !rec.dispatched {
            rec.state = ActionState::ObservedWaiting;
        }
    }

    async fn on_request_ready(
        &mut self,
        key: &str,
        tool_action_id: ToolActionId,
        unit_id: UnitId,
        generation: u64,
        tool_name: Option<String>,
        request_payload: Option<String>,
        snap: &monoloop_contracts::CanonicalUnitSnapshot,
    ) -> Result<(), LoopEndKind> {
        if self.actions.len() >= self.limits.max_tool_actions && !self.actions.contains_key(key) {
            return Err(LoopEndKind::InvariantFailed);
        }

        let rec = self.actions.entry(key.to_string()).or_insert_with(|| ActionRecord {
            tool_action_id: tool_action_id.clone(),
            unit_id: unit_id.clone(),
            last_generation: 0,
            state: ActionState::RequestReady,
            dispatched: false,
        });

        if generation < rec.last_generation {
            return Ok(()); // stale
        }
        rec.last_generation = generation;

        // At-most-once dispatch per action in this Loop incarnation.
        if rec.dispatched {
            self.duplicates += 1;
            return Ok(());
        }

        let Some(name) = tool_name else {
            self.diag("ToolRequestReady missing tool name".into());
            return Ok(());
        };
        let Some(payload) = request_payload else {
            self.diag("ToolRequestReady missing payload".into());
            return Ok(());
        };

        rec.state = ActionState::RequestReady;
        rec.dispatched = true;

        if self
            .out_tx
            .send(LoopOutputEvent::ToolDispatchRequested {
                tool_action_id: tool_action_id.clone(),
                request_generation: generation,
            })
            .await
            .is_err()
        {
            return Err(LoopEndKind::OutputFailed);
        }

        let resolution = self
            .registry
            .resolve(ResolveToolRequest {
                tool_action_id: tool_action_id.clone(),
                tool_name: name.clone(),
                request_payload: payload.clone(),
            })
            .await
            .map_err(|_| LoopEndKind::InvariantFailed)?;

        match resolution {
            ToolResolution::Unavailable(reason) => {
                rec.state = ActionState::Unavailable;
                self.tools_unavailable += 1;
                self.health
                    .tools_unavailable
                    .fetch_add(1, Ordering::Relaxed);

                if self
                    .out_tx
                    .send(LoopOutputEvent::ToolUnavailable {
                        tool_action_id: tool_action_id.clone(),
                        reason,
                    })
                    .await
                    .is_err()
                {
                    return Err(LoopEndKind::OutputFailed);
                }

                let result = OutboundToolResult {
                    outbound_result_id: uuid::Uuid::new_v4().to_string(),
                    monoloop_run_id: self.monoloop_run_id.clone(),
                    loop_id: self.loop_id.clone(),
                    source_interpretation_id: snap.interpretation_id.clone(),
                    source_connection_id: snap.connection_id.clone(),
                    external_session_id: snap.external_session_id.clone(),
                    tool_action_id,
                    request_generation: generation,
                    tool_execution_id: None,
                    outcome: OutboundToolOutcome::ToolUnavailable,
                    payload: format!("{reason:?}"),
                    source_unit_id: unit_id,
                };
                self.outbound_results += 1;
                if self
                    .out_tx
                    .send(LoopOutputEvent::OutboundToolResult(result))
                    .await
                    .is_err()
                {
                    return Err(LoopEndKind::OutputFailed);
                }
                // Empty registry: never call ToolRuntime.start.
            }
            ToolResolution::Available(_) => {
                // Future path: allocate execution id and call runtime.
                // Initial product forbids starting without later policy; treat as rejected.
                self.diag("Available tool without runtime policy — dispatch_rejected".into());
                if self
                    .out_tx
                    .send(LoopOutputEvent::OutboundToolResult(OutboundToolResult {
                        outbound_result_id: uuid::Uuid::new_v4().to_string(),
                        monoloop_run_id: self.monoloop_run_id.clone(),
                        loop_id: self.loop_id.clone(),
                        source_interpretation_id: snap.interpretation_id.clone(),
                        source_connection_id: snap.connection_id.clone(),
                        external_session_id: snap.external_session_id.clone(),
                        tool_action_id,
                        request_generation: generation,
                        tool_execution_id: None,
                        outcome: OutboundToolOutcome::DispatchRejected,
                        payload: "available tool path deferred".into(),
                        source_unit_id: unit_id,
                    }))
                    .await
                    .is_err()
                {
                    return Err(LoopEndKind::OutputFailed);
                }
            }
        }
        Ok(())
    }

    async fn on_interpretation_end(
        &mut self,
        _end: InterpretationEnd,
    ) -> Result<(), LoopEndKind> {
        // Not turn completion. Do not expand scope.
        Ok(())
    }

    fn in_scope(&self, snap: &monoloop_contracts::CanonicalUnitSnapshot) -> bool {
        if self.scope.accept_all_in_run {
            return true;
        }
        if !self.scope.accepted_interpretation_ids.is_empty()
            && !self
                .scope
                .accepted_interpretation_ids
                .iter()
                .any(|id| id.as_str() == snap.interpretation_id.as_str())
        {
            return false;
        }
        if !self.scope.accepted_connection_ids.is_empty()
            && !self
                .scope
                .accepted_connection_ids
                .iter()
                .any(|id| id.as_str() == snap.connection_id.as_str())
        {
            return false;
        }
        if !self.scope.accepted_external_session_ids.is_empty() {
            match &snap.external_session_id {
                Some(ext) => {
                    if !self
                        .scope
                        .accepted_external_session_ids
                        .iter()
                        .any(|id| id.as_str() == ext.as_str())
                    {
                        return false;
                    }
                }
                None => return false,
            }
        }
        true
    }

    fn diag(&mut self, msg: String) {
        if self.diagnostics.len() < 32 {
            self.diagnostics.push(msg);
        }
    }

    async fn finish(&mut self, kind: LoopEndKind, end_tx: oneshot::Sender<LoopEnd>) {
        if self.ended {
            return;
        }
        self.ended = true;
        let end = LoopEnd {
            monoloop_run_id: self.monoloop_run_id.clone(),
            loop_id: self.loop_id.clone(),
            kind,
            delivery_events_received: self.delivery_events,
            duplicate_events: self.duplicates,
            tools_unavailable: self.tools_unavailable,
            outbound_results_emitted: self.outbound_results,
            safe_diagnostics: self.diagnostics.clone(),
        };
        let _ = self
            .out_tx
            .send(LoopOutputEvent::LoopEnded(end.clone()))
            .await;
        let _ = end_tx.send(end);
    }
}
