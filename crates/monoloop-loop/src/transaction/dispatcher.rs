//! Single validated execution path for linked tools (MCP and model share this).

use super::resolved_tools::ResolvedToolSet;
use super::tool_capacity::{SharedToolCapacity, ToolPermit, TransactionToolCapacity};
use super::tool_handler::{ToolExecutionControl, ToolKillHandle};

/// Holds unfinished tool worker joins and/or orphaned permits (§22.4 / D-028).
///
/// Permits stay acquired until the corresponding join is observed finished (or
/// until an orphaned cooperative permit is explicitly reaped at shutdown).
struct ToolJoinVault {
    parked: std::sync::Mutex<Vec<ParkedToolWork>>,
}

struct ParkedToolWork {
    join: Option<tokio::task::JoinHandle<()>>,
    permit: Option<ToolPermit>,
    /// When true, a future supervisor drain may abort (AbortableAtYield).
    /// Cooperative `JoinOnly` stays false — cancel is best-effort only (§14.1).
    /// Retained on parked work for shutdown integration (not read on Drop transfer).
    #[allow(dead_code)]
    may_abort: bool,
}

impl ToolJoinVault {
    fn new() -> Self {
        Self {
            parked: std::sync::Mutex::new(Vec::new()),
        }
    }

    /// Park a live worker join with its permit (released only after join finishes).
    fn park(
        &self,
        join: tokio::task::JoinHandle<()>,
        permit: Option<ToolPermit>,
        may_abort: bool,
    ) {
        if join.is_finished() {
            // Finished: drop handle (safe) and release permit immediately.
            drop(join);
            drop(permit);
            return;
        }
        let mut parked = self.parked.lock().unwrap_or_else(|e| e.into_inner());
        parked.push(ParkedToolWork {
            join: Some(join),
            permit,
            may_abort,
        });
    }

    /// Hold a permit with no killable join (missing kill fallback).
    fn park_orphan_permit(&self, permit: ToolPermit) {
        let mut parked = self.parked.lock().unwrap_or_else(|e| e.into_inner());
        parked.push(ParkedToolWork {
            join: None,
            permit: Some(permit),
            may_abort: false,
        });
    }

    /// Drop finished joins and release their permits. Orphan permits are retained.
    fn reap_finished(&self) {
        let mut parked = self.parked.lock().unwrap_or_else(|e| e.into_inner());
        parked.retain(|entry| match &entry.join {
            Some(join) => !join.is_finished(),
            None => true, // orphan permit — keep capacity held
        });
    }

    /// Number of permits still held by the vault (joins + orphans).
    fn pending_permits(&self) -> usize {
        let parked = self.parked.lock().unwrap_or_else(|e| e.into_inner());
        parked.iter().filter(|e| e.permit.is_some()).count()
    }
}

/// Process-scoped pending tool work transferred from a dropped vault (§22.4).
///
/// Keeps joins + permits owned without aborting cooperative work, without
/// `mem::forget`, and without unbounded Drop waits. Slots stay unavailable
/// until process exit or an explicit reap of this set (cleanup-pending).
fn process_pending_tool_work() -> &'static std::sync::Mutex<Vec<ParkedToolWork>> {
    static PENDING: std::sync::OnceLock<std::sync::Mutex<Vec<ParkedToolWork>>> =
        std::sync::OnceLock::new();
    PENDING.get_or_init(|| std::sync::Mutex::new(Vec::new()))
}

impl Drop for ToolJoinVault {
    fn drop(&mut self) {
        let mut parked = std::mem::take(self.parked.get_mut().unwrap_or_else(|e| e.into_inner()));
        if parked.is_empty() {
            return;
        }
        // Transfer ownership out of this Drop — do not abort JoinOnly, detach,
        // or release permits here. Fail-closed: capacity remains held.
        if let Ok(mut global) = process_pending_tool_work().lock() {
            global.append(&mut parked);
        } else {
            // Poisoned mutex: still do not free capacity / detach joins.
            let mut guard = process_pending_tool_work()
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            guard.append(&mut parked);
        }
    }
}

use super::validation::{
    validate_tool_completion, validate_tool_input, InputValidationFailure, DEFAULT_MAX_JSON_DEPTH,
};
use monoloop_contracts::{
    CanonicalToolError, CanonicalToolOutput, CanonicalToolResult, CanonicalToolResultOutcome,
    ExchangeId, SessionKey, ToolActionId, ToolCall, ToolCallContext, ToolExecutionClass,
    ToolCompletion, ToolId, ToolLifecycleEvent, ToolName, ToolRuntimeError, ToolStartError,
    TransactionId,
};
use std::future::Future;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Request to dispatch one complete tool call through the linked handler path.
#[derive(Clone, Debug)]
pub struct DispatchRequest {
    /// Exchange owning this call (model path); MCP may synthesize one.
    pub exchange_id: ExchangeId,
    /// Internal action id.
    pub tool_action_id: ToolActionId,
    /// Public tool name as requested.
    pub tool_name: ToolName,
    /// Provider correlation id preserved exactly.
    pub provider_tool_call_id: String,
    /// Model-declared order.
    pub request_ordinal: u32,
    /// Complete JSON argument payload.
    pub arguments_json: String,
}

/// Outcome of a dispatch attempt.
#[derive(Clone, Debug)]
pub enum DispatchOutcome {
    /// Canonical success or declared domain failure (continuation product).
    Canonical {
        /// Validated result.
        result: CanonicalToolResult,
        /// Lifecycle events produced (Started + Completed).
        lifecycle: Vec<ToolLifecycleEvent>,
    },
    /// Invalid/disallowed arguments — rejected tool result, not transaction failure.
    Rejected {
        /// Action id.
        tool_action_id: ToolActionId,
        /// Safe reason code.
        code: &'static str,
        /// Safe message.
        message: String,
        /// Lifecycle (Started may be omitted when never accepted).
        lifecycle: Vec<ToolLifecycleEvent>,
    },
    /// Handler/runtime failure — selects `ToolExchangeFailed` when policy requires.
    RuntimeFailed {
        /// Action id.
        tool_action_id: ToolActionId,
        /// Tool id when known.
        tool_id: Option<ToolId>,
        /// Safe failure code.
        code: String,
        /// Lifecycle events (may include Started + RuntimeFailed).
        lifecycle: Vec<ToolLifecycleEvent>,
    },
}

/// Capacity and payload bounds for one transaction dispatcher (D-015).
#[derive(Clone, Copy, Debug)]
pub struct DispatcherLimits {
    /// Max concurrent tool executions for this transaction.
    pub max_concurrent_tools: usize,
    /// Max queued tool starts for this transaction.
    pub max_queued_tools: usize,
    /// Transaction-wide payload cap; applied as min with per-tool limit.
    pub max_tool_payload_bytes: usize,
    /// Transaction-wide output cap; applied as min with per-tool limit.
    pub max_tool_output_bytes: usize,
}

impl Default for DispatcherLimits {
    fn default() -> Self {
        Self {
            max_concurrent_tools: 16,
            max_queued_tools: 64,
            max_tool_payload_bytes: usize::MAX,
            max_tool_output_bytes: usize::MAX,
        }
    }
}

/// Transaction-owned dispatcher: allowlist, validation, capacity, handler, output check.
pub struct TransactionToolDispatcher {
    transaction_id: TransactionId,
    /// Authoritative after claim; may start provisional on create (D-026).
    session_key: std::sync::Mutex<SessionKey>,
    tools: ResolvedToolSet,
    capacity: Arc<TransactionToolCapacity>,
    /// Runtime-scoped vault for unfinished joins (D-028).
    tool_join_vault: Arc<ToolJoinVault>,
    /// Transaction-wide payload cap (D-015); applied as min with per-tool limit.
    max_tool_payload_bytes: usize,
    /// Transaction-wide output cap (D-015); applied as min with per-tool limit.
    max_tool_output_bytes: usize,
    max_error_message_bytes: usize,
    max_json_depth: u32,
}

impl TransactionToolDispatcher {
    /// Build a dispatcher for one admitted transaction.
    pub fn new(
        transaction_id: TransactionId,
        session_key: SessionKey,
        tools: ResolvedToolSet,
        shared_capacity: Arc<SharedToolCapacity>,
        max_concurrent_tools: usize,
        max_queued_tools: usize,
    ) -> Arc<Self> {
        Self::with_limits(
            transaction_id,
            session_key,
            tools,
            shared_capacity,
            DispatcherLimits {
                max_concurrent_tools,
                max_queued_tools,
                max_tool_payload_bytes: usize::MAX,
                max_tool_output_bytes: usize::MAX,
            },
        )
    }

    /// Build with explicit concurrency and payload/output caps (D-015).
    pub fn with_limits(
        transaction_id: TransactionId,
        session_key: SessionKey,
        tools: ResolvedToolSet,
        shared_capacity: Arc<SharedToolCapacity>,
        limits: DispatcherLimits,
    ) -> Arc<Self> {
        Self::with_limits_and_vault(
            transaction_id,
            session_key,
            tools,
            shared_capacity,
            limits,
            Arc::new(ToolJoinVault::new()),
        )
    }

    /// Build with an explicit runtime-scoped tool join vault (D-028).
    fn with_limits_and_vault(
        transaction_id: TransactionId,
        session_key: SessionKey,
        tools: ResolvedToolSet,
        shared_capacity: Arc<SharedToolCapacity>,
        limits: DispatcherLimits,
        tool_join_vault: Arc<ToolJoinVault>,
    ) -> Arc<Self> {
        let capacity = TransactionToolCapacity::new(
            shared_capacity,
            limits.max_concurrent_tools,
            limits.max_queued_tools,
        );
        for spec in tools.specs() {
            capacity.configure_tool(spec.id.clone(), spec.limits.max_concurrent);
        }
        Arc::new(Self {
            transaction_id,
            session_key: std::sync::Mutex::new(session_key),
            tools,
            capacity,
            tool_join_vault,
            max_tool_payload_bytes: limits.max_tool_payload_bytes.max(1),
            max_tool_output_bytes: limits.max_tool_output_bytes.max(1),
            max_error_message_bytes: 1024,
            max_json_depth: DEFAULT_MAX_JSON_DEPTH,
        })
    }

    /// Resolved tool set (encoder / MCP projection).
    pub fn tools(&self) -> &ResolvedToolSet {
        &self.tools
    }

    /// Transaction identity.
    pub fn transaction_id(&self) -> TransactionId {
        self.transaction_id
    }

    /// Session key (clone under lock).
    pub fn session_key(&self) -> SessionKey {
        self.session_key
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// Replace provisional create key with the claimed authoritative key (D-026).
    pub fn rebind_session(&self, session_key: SessionKey) {
        *self.session_key.lock().unwrap_or_else(|e| e.into_inner()) = session_key;
    }

    /// Permits held by unfinished/orphaned tool workers (§22.4 observability).
    pub fn vault_pending_permits(&self) -> usize {
        self.tool_join_vault.pending_permits()
    }

    /// Reap finished parked joins so capacity can recover.
    pub fn reap_vault(&self) {
        self.tool_join_vault.reap_finished();
    }

    /// Dispatch one call end-to-end.
    pub async fn dispatch(self: &Arc<Self>, request: DispatchRequest) -> DispatchOutcome {
        self.dispatch_with_cancel(request, None).await
    }

    /// Dispatch with an optional external cancel signal (D-028).
    ///
    /// When `cancel` is notified, the dispatcher runs the same termination path as
    /// an execution deadline so the worker is cancel/kill/joined instead of detached.
    pub async fn dispatch_with_cancel(
        self: &Arc<Self>,
        request: DispatchRequest,
        cancel: Option<Arc<super::sticky_cancel::StickyCancel>>,
    ) -> DispatchOutcome {
        let action = request.tool_action_id.clone();

        // Allowlist by public name.
        let Some(resolved) = self.tools.get_by_name(&request.tool_name) else {
            return DispatchOutcome::Rejected {
                tool_action_id: action,
                code: "tool_not_allowed",
                message: "tool not in resolved set".into(),
                lifecycle: vec![],
            };
        };
        let tool_id = resolved.spec.id.clone();
        let spec = resolved.spec.clone();
        let handler = Arc::clone(&resolved.handler);

        if !self.capacity.try_enqueue() {
            return DispatchOutcome::Rejected {
                tool_action_id: action,
                code: "tool_queue_full",
                message: "per-transaction tool queue full".into(),
                lifecycle: vec![],
            };
        }

        // Input validation before capacity acquire for execution.
        // Effective payload limit is min(per-tool, transaction-wide) (D-015).
        let max_payload = spec.limits.max_input_bytes.min(self.max_tool_payload_bytes);
        let arguments = match validate_tool_input(
            &request.arguments_json,
            &spec.input_schema,
            max_payload,
            self.max_json_depth,
        ) {
            Ok(v) => v,
            Err(f) => {
                self.capacity.dequeue();
                return reject_input(action, f);
            }
        };

        // Wait briefly for concurrency (bounded spin/yield); fail closed if not acquired.
        let permit = {
            let mut acquired = None;
            let deadline = Instant::now() + Duration::from_millis(50);
            while Instant::now() < deadline {
                if let Some(p) = self.capacity.try_acquire(&tool_id) {
                    acquired = Some(p);
                    break;
                }
                tokio::task::yield_now().await;
            }
            match acquired {
                Some(p) => p,
                None => {
                    // try_acquire dequeues only on success; still queued.
                    self.capacity.dequeue();
                    return DispatchOutcome::Rejected {
                        tool_action_id: action,
                        code: "tool_capacity_exceeded",
                        message: "tool concurrency capacity exceeded".into(),
                        lifecycle: vec![],
                    };
                }
            }
        };
        // Reap completed parked workers so prior cancels cannot permanently starve capacity.
        self.tool_join_vault.reap_finished();

        // RAII: if this future is dropped mid-join, park worker+permit in the vault.
        let mut dispatch_guard = DispatchGuard {
            permit: Some(permit),
            kill: None,
            vault: Arc::clone(&self.tool_join_vault),
        };

        let mut lifecycle = vec![ToolLifecycleEvent::Started {
            tool_action_id: action.clone(),
            tool_id: tool_id.clone(),
            tool_name: request.tool_name.clone(),
            provider_tool_call_id: request.provider_tool_call_id.clone(),
            request_ordinal: request.request_ordinal,
        }];

        let call = ToolCall {
            tool_name: request.tool_name.clone(),
            tool_id: tool_id.clone(),
            provider_tool_call_id: request.provider_tool_call_id.clone(),
            arguments,
            request_ordinal: request.request_ordinal,
        };
        let context = ToolCallContext {
            transaction_id: self.transaction_id,
            session_key: self.session_key(),
            exchange_id: Some(request.exchange_id),
            tool_action_id: action.clone(),
            tool_id: tool_id.clone(),
            deadline: Instant::now() + spec.limits.execution_deadline,
        };

        // Bounded execution: deadline / external cancel → grace → kill → join (D-024 / D-028).
        let deadline = spec.limits.execution_deadline;
        let policy = spec.execution_class.clone();

        // Structural termination support must be confirmed *before* start so a
        // missing kill capability cannot leave an ignoring worker running (D-028).
        let supports_required_termination = match &policy {
            ToolExecutionClass::AbortableAtYield { .. } => handler.supports_abort(),
            // V2 §14.3: structural OS process claim, not boolean-only kill support.
            ToolExecutionClass::ProcessIsolated { .. } => {
                handler.os_process_isolated() && handler.supports_isolated_kill()
            }
            ToolExecutionClass::CooperativeInProcess { .. } => true,
        };
        if !supports_required_termination {
            drop(dispatch_guard);
            lifecycle.push(ToolLifecycleEvent::RuntimeFailed {
                tool_action_id: action.clone(),
                tool_id: tool_id.clone(),
                code: "missing_kill_handle".into(),
            });
            return DispatchOutcome::RuntimeFailed {
                tool_action_id: action,
                tool_id: Some(tool_id),
                code: "missing_kill_handle".into(),
                lifecycle,
            };
        }

        let start_result = catch_unwind(AssertUnwindSafe(|| handler.start(call, context)));
        let handle = match start_result {
            Ok(Ok(h)) => h,
            Ok(Err(ToolStartError::CapacityExceeded)) => {
                drop(dispatch_guard);
                return DispatchOutcome::Rejected {
                    tool_action_id: action,
                    code: "tool_capacity_exceeded",
                    message: "handler capacity exceeded".into(),
                    lifecycle,
                };
            }
            Ok(Err(ToolStartError::Rejected(reason))) => {
                drop(dispatch_guard);
                lifecycle.push(ToolLifecycleEvent::RuntimeFailed {
                    tool_action_id: action.clone(),
                    tool_id: tool_id.clone(),
                    code: "tool_start_rejected".into(),
                });
                return DispatchOutcome::RuntimeFailed {
                    tool_action_id: action,
                    tool_id: Some(tool_id),
                    code: format!("start_rejected:{reason}"),
                    lifecycle,
                };
            }
            Err(_) => {
                drop(dispatch_guard);
                lifecycle.push(ToolLifecycleEvent::RuntimeFailed {
                    tool_action_id: action.clone(),
                    tool_id: tool_id.clone(),
                    code: "tool_panicked".into(),
                });
                return DispatchOutcome::RuntimeFailed {
                    tool_action_id: action,
                    tool_id: Some(tool_id),
                    code: "panicked".into(),
                    lifecycle,
                };
            }
        };

        let control = handle.control.clone();
        dispatch_guard.kill = handle.kill.clone();
        let kill = dispatch_guard.kill.clone();
        // Post-start invariant: Abortable needs a kill handle; ProcessIsolated
        // needs an OS-process kill handle (not Tokio abort).
        let kill_ok = match &policy {
            ToolExecutionClass::AbortableAtYield { .. } => kill.is_some(),
            ToolExecutionClass::ProcessIsolated { .. } => {
                kill.as_ref().is_some_and(|k| k.is_process_isolated())
            }
            ToolExecutionClass::CooperativeInProcess { .. } => true,
        };
        if !kill_ok {
            // Handler claimed abort/kill support but returned no ToolKillHandle —
            // we do not own the real worker. Never fabricate a completion waiter
            // whose abort would release the permit while work may still run.
            control.cancel();
            let completion = handle.completion;
            let wait = completion.wait();
            tokio::pin!(wait);
            match tokio::time::timeout(Duration::from_millis(200), &mut wait).await {
                Ok(_) => {
                    // Completion already observed — safe to release capacity.
                    dispatch_guard.release_if_idle();
                }
                Err(_) => {
                    // Pending work without a killable join: orphan the permit
                    // (held until runtime shutdown). Do not vault an abortable waiter.
                    if let Some(permit) = dispatch_guard.permit.take() {
                        self.tool_join_vault.park_orphan_permit(permit);
                    }
                }
            }
            lifecycle.push(ToolLifecycleEvent::RuntimeFailed {
                tool_action_id: action.clone(),
                tool_id: tool_id.clone(),
                code: "missing_kill_handle".into(),
            });
            return DispatchOutcome::RuntimeFailed {
                tool_action_id: action,
                tool_id: Some(tool_id),
                code: "missing_kill_handle".into(),
                lifecycle,
            };
        }
        let wait = handle.completion.wait();
        tokio::pin!(wait);
        let cancel_fut = async {
            if let Some(n) = cancel.as_ref() {
                n.cancelled().await;
            } else {
                std::future::pending::<()>().await;
            }
        };
        let completion = tokio::select! {
            biased;
            c = &mut wait => c,
            _ = cancel_fut => {
                await_tool_termination(&mut wait, &control, kill.as_ref(), &policy).await
            }
            _ = tokio::time::sleep(deadline) => {
                await_tool_termination(&mut wait, &control, kill.as_ref(), &policy).await
            }
        };
        // Join worker within a short bound; if still pending, Drop parks join+permit.
        if let Some(ref k) = dispatch_guard.kill {
            let _ = k.join_timeout(Duration::from_millis(50)).await;
        } else if matches!(policy, ToolExecutionClass::CooperativeInProcess { .. })
            && matches!(
                &completion,
                ToolCompletion::RuntimeFailed(ToolRuntimeError::DeadlineExceeded)
            )
        {
            // §22.4: cooperative non-ack (grace elapsed, no completion) — keep
            // capacity held. Acknowledged cancel that joins must not orphan.
            if let Some(permit) = dispatch_guard.permit.take() {
                self.tool_join_vault.park_orphan_permit(permit);
            }
        }
        dispatch_guard.release_if_idle();

        let max_output = spec.limits.max_output_bytes.min(self.max_tool_output_bytes);
        let validated = match validate_tool_completion(
            completion,
            &spec.output_contract,
            max_output,
            self.max_error_message_bytes,
            self.max_json_depth,
        ) {
            Ok(c) => c,
            Err(_) => {
                lifecycle.push(ToolLifecycleEvent::RuntimeFailed {
                    tool_action_id: action.clone(),
                    tool_id: tool_id.clone(),
                    code: "output_contract_violated".into(),
                });
                return DispatchOutcome::RuntimeFailed {
                    tool_action_id: action,
                    tool_id: Some(tool_id),
                    code: "output_contract_violated".into(),
                    lifecycle,
                };
            }
        };

        match validated {
            ToolCompletion::Succeeded(output) => {
                let result = CanonicalToolResult {
                    transaction_id: self.transaction_id,
                    session_key: self.session_key(),
                    exchange_id: request.exchange_id,
                    tool_action_id: action.clone(),
                    tool_id: tool_id.clone(),
                    provider_tool_call_id: request.provider_tool_call_id,
                    request_ordinal: request.request_ordinal,
                    outcome: CanonicalToolResultOutcome::Succeeded(output),
                };
                lifecycle.push(ToolLifecycleEvent::Completed {
                    result: result.clone(),
                });
                DispatchOutcome::Canonical { result, lifecycle }
            }
            ToolCompletion::DomainFailed(err) => {
                let result = CanonicalToolResult {
                    transaction_id: self.transaction_id,
                    session_key: self.session_key(),
                    exchange_id: request.exchange_id,
                    tool_action_id: action.clone(),
                    tool_id: tool_id.clone(),
                    provider_tool_call_id: request.provider_tool_call_id,
                    request_ordinal: request.request_ordinal,
                    outcome: CanonicalToolResultOutcome::DomainFailed(err),
                };
                lifecycle.push(ToolLifecycleEvent::Completed {
                    result: result.clone(),
                });
                DispatchOutcome::Canonical { result, lifecycle }
            }
            ToolCompletion::RuntimeFailed(e) => {
                let code = match e {
                    ToolRuntimeError::Panicked => "panicked",
                    ToolRuntimeError::CompletionLost => "completion_lost",
                    ToolRuntimeError::OutputContractViolated => "output_contract_violated",
                    ToolRuntimeError::TerminationFailed => "termination_failed",
                    ToolRuntimeError::DeadlineExceeded => "deadline_exceeded",
                };
                lifecycle.push(ToolLifecycleEvent::RuntimeFailed {
                    tool_action_id: action.clone(),
                    tool_id: tool_id.clone(),
                    code: code.into(),
                });
                DispatchOutcome::RuntimeFailed {
                    tool_action_id: action,
                    tool_id: Some(tool_id),
                    code: code.into(),
                    lifecycle,
                }
            }
        }
    }
}

/// After execution deadline: cooperative cancel, optional grace, then kill+join (D-024).
async fn await_tool_termination(
    wait: &mut Pin<&mut impl Future<Output = ToolCompletion>>,
    control: &ToolExecutionControl,
    kill: Option<&ToolKillHandle>,
    policy: &ToolExecutionClass,
) -> ToolCompletion {
    control.cancel();
    let join_grace = Duration::from_millis(200);
    match policy {
        ToolExecutionClass::AbortableAtYield { .. } => {
            if let Some(k) = kill {
                k.kill();
                // Bounded join only — never await unboundedly here (actor cleanup
                // may drop this future). Unfinished joins stay owned for
                // DispatchGuard to vault with the permit.
                if k.join_timeout(join_grace).await.is_err() {
                    return ToolCompletion::RuntimeFailed(ToolRuntimeError::TerminationFailed);
                }
            }
            match tokio::time::timeout(join_grace, wait).await {
                Ok(c) => c,
                Err(_) => ToolCompletion::RuntimeFailed(ToolRuntimeError::DeadlineExceeded),
            }
        }
        ToolExecutionClass::CooperativeInProcess { grace } => {
            match tokio::time::timeout(*grace, &mut *wait).await {
                Ok(c) => c,
                Err(_) => ToolCompletion::RuntimeFailed(ToolRuntimeError::DeadlineExceeded),
            }
        }
        ToolExecutionClass::ProcessIsolated {
            grace,
            kill_deadline,
        } => {
            match tokio::time::timeout(*grace, &mut *wait).await {
                Ok(c) => c,
                Err(_) => {
                    if let Some(k) = kill {
                        k.kill();
                        if k.join_timeout(*kill_deadline).await.is_err() {
                            return ToolCompletion::RuntimeFailed(
                                ToolRuntimeError::TerminationFailed,
                            );
                        }
                    }
                    match tokio::time::timeout(*kill_deadline, wait).await {
                        Ok(c) => c,
                        Err(_) => {
                            ToolCompletion::RuntimeFailed(ToolRuntimeError::TerminationFailed)
                        }
                    }
                }
            }
        }
    }
}

/// Holds the tool permit and optional kill handle for the duration of dispatch.
/// On drop (including actor cleanup cancelling this future), unfinished workers
/// are parked in the runtime tool-join vault together with the permit.
struct DispatchGuard {
    permit: Option<ToolPermit>,
    kill: Option<ToolKillHandle>,
    vault: Arc<ToolJoinVault>,
}

impl DispatchGuard {
    /// Release permit only when no unfinished worker join remains.
    fn release_if_idle(&mut self) {
        if self.kill.as_ref().is_some_and(ToolKillHandle::has_join) {
            return;
        }
        self.kill.take();
        drop(self.permit.take());
    }
}

impl Drop for DispatchGuard {
    fn drop(&mut self) {
        if let Some(k) = self.kill.take() {
            let may_abort = !k.is_join_only();
            k.kill(); // no-op for JoinOnly cooperative
            if let Some(join) = k.take_join() {
                if !join.is_finished() {
                    // Park the *real* worker join — never a completion waiter.
                    self.vault.park(join, self.permit.take(), may_abort);
                    return;
                }
            }
        }
        drop(self.permit.take());
    }
}

fn reject_input(action: ToolActionId, f: InputValidationFailure) -> DispatchOutcome {
    let (code, message) = match f {
        InputValidationFailure::OversizedInput => ("oversized_input", "tool input exceeds limit"),
        InputValidationFailure::InvalidJson => {
            ("invalid_json", "tool arguments are not valid JSON")
        }
        InputValidationFailure::DepthExceeded => ("json_depth_exceeded", "tool arguments too deep"),
        InputValidationFailure::SchemaInvalid => {
            ("schema_invalid", "tool arguments fail input schema")
        }
    };
    // Rejected path still yields a domain-style rejection result shape for tests.
    let _ = CanonicalToolError::try_new(code, message, None, 256);
    let _ = CanonicalToolOutput::Text(String::new());
    DispatchOutcome::Rejected {
        tool_action_id: action,
        code,
        message: message.into(),
        lifecycle: vec![],
    }
}
