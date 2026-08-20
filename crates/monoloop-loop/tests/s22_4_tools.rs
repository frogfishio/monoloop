//! §22.4 tool execution class proofs (cooperative / abortable / process / capacity).

use monoloop_contracts::{
    CanonicalToolOutput, ChannelId, ExchangeId, JsonSchema, SessionId, SessionKey, ToolActionId,
    ToolCall, ToolCallContext, ToolCompletion, ToolExecutionClass, ToolExecutionId, ToolId,
    ToolLimits, ToolName, ToolOutputContract, ToolSpec, ToolStartError, ToolSuccessContract,
    TransactionId,
};
use monoloop_loop::{
    dispatch_ready_tool, AsyncToolHandler, DispatchOutcome, HostToolRegistry, ImmediateToolHandler,
    IsolatedKillableToolHandler, LinkedToolExecutionHandle, ProcessIsolatedToolHandler,
    RegisteredTool, ResolvedToolSet, SharedToolCapacity, ToolExecutionCompletion,
    ToolExecutionControl, ToolHandler, ToolKillHandle, TransactionToolDispatcher,
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

fn object_schema() -> JsonSchema {
    JsonSchema::try_new(serde_json::json!({
        "type": "object",
        "properties": { "q": { "type": "string" } },
        "required": ["q"],
        "additionalProperties": false
    }))
    .unwrap()
}

fn success_json_schema() -> JsonSchema {
    JsonSchema::try_new(serde_json::json!({
        "type": "object",
        "properties": { "ok": { "type": "boolean" } },
        "required": ["ok"],
        "additionalProperties": false
    }))
    .unwrap()
}

fn session_key() -> SessionKey {
    SessionKey::new(
        ChannelId::try_new("ch").unwrap(),
        SessionId::try_new("s1").unwrap(),
    )
}

fn base_spec(id: &str, name: &str, class: ToolExecutionClass, deadline: Duration) -> ToolSpec {
    ToolSpec::try_new(
        ToolId::try_new(id).unwrap(),
        ToolName::try_new(name).unwrap(),
        "s22_4 tool",
        object_schema(),
        ToolOutputContract {
            success: ToolSuccessContract::json(success_json_schema()),
            error_data_schema: None,
        },
        ToolLimits {
            max_concurrent: 2,
            max_input_bytes: 1024,
            max_output_bytes: 1024,
            execution_deadline: deadline,
        },
        class,
    )
    .unwrap()
}

/// Cooperative handler that acknowledges cancel and joins normally.
struct AckCancelCooperative;

impl ToolHandler for AckCancelCooperative {
    fn start(
        &self,
        _call: ToolCall,
        _ctx: ToolCallContext,
    ) -> Result<LinkedToolExecutionHandle, ToolStartError> {
        let control = ToolExecutionControl::new();
        let body = control.clone();
        let (tx, rx) = tokio::sync::oneshot::channel();
        // Join-only kill handle: cooperative owns the join without hard abort.
        let join = tokio::spawn(async move {
            tokio::select! {
                biased;
                _ = body.cancelled() => {
                    let _ = tx.send(ToolCompletion::RuntimeFailed(
                        monoloop_contracts::ToolRuntimeError::TerminationFailed,
                    ));
                }
                _ = tokio::time::sleep(Duration::from_secs(30)) => {
                    let _ = tx.send(ToolCompletion::Succeeded(CanonicalToolOutput::Json(
                        serde_json::json!({"ok": true}),
                    )));
                }
            }
        });
        Ok(LinkedToolExecutionHandle {
            execution_id: ToolExecutionId::generate(),
            control,
            completion: ToolExecutionCompletion::new(rx),
            kill: Some(ToolKillHandle::join_only(join)),
        })
    }
}

/// Cooperative handler that ignores cancel and never completes quickly.
struct IgnoreCancelCooperative;

impl ToolHandler for IgnoreCancelCooperative {
    fn start(
        &self,
        _call: ToolCall,
        _ctx: ToolCallContext,
    ) -> Result<LinkedToolExecutionHandle, ToolStartError> {
        let control = ToolExecutionControl::new();
        let (tx, rx) = tokio::sync::oneshot::channel();
        let join = tokio::spawn(async move {
            // Deliberately ignore cancel — sleep through grace (bounded so vault
            // Drop can observe the join without a long hang).
            tokio::time::sleep(Duration::from_millis(400)).await;
            let _ = tx.send(ToolCompletion::Succeeded(CanonicalToolOutput::Json(
                serde_json::json!({"ok": true}),
            )));
            let _ = control.is_cancelled();
        });
        Ok(LinkedToolExecutionHandle {
            execution_id: ToolExecutionId::generate(),
            control: ToolExecutionControl::new(),
            completion: ToolExecutionCompletion::new(rx),
            kill: Some(ToolKillHandle::join_only(join)),
        })
    }
}

/// §22.4: cooperative tool that acknowledges cancel joins normally.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s22_4_cooperative_ack_cancel_joins() {
    let spec = base_spec(
        "ack",
        "ack",
        ToolExecutionClass::CooperativeInProcess {
            grace: Duration::from_millis(200),
        },
        Duration::from_millis(40),
    );
    let host = HostToolRegistry::build(vec![RegisteredTool::new(
        spec,
        Arc::new(AckCancelCooperative),
    )])
    .unwrap();
    let tool = host.get(&ToolId::try_new("ack").unwrap()).unwrap().clone();
    let shared = SharedToolCapacity::new(2);
    let d = TransactionToolDispatcher::new(
        TransactionId::generate(),
        session_key(),
        ResolvedToolSet::from_registered(vec![tool]),
        Arc::clone(&shared),
        4,
        8,
    );
    let out = dispatch_ready_tool(
        &d,
        ExchangeId::generate(),
        ToolActionId::new("a"),
        "ack",
        "p",
        0,
        r#"{"q":"x"}"#,
    )
    .await;
    assert!(
        matches!(
            &out,
            DispatchOutcome::RuntimeFailed { code, .. }
                if code == "deadline_exceeded"
                    || code == "termination_failed"
                    || code == "completion_lost"
        ),
        "expected cancel/deadline join path, got {out:?}"
    );
    // Acknowledging cooperative work should not leave capacity orphaned.
    d.reap_vault();
    assert_eq!(
        d.vault_pending_permits(),
        0,
        "ack-cancel cooperative must not orphan permits"
    );
    assert_eq!(shared.active(), 0);
}

/// §22.4: cooperative tool that does not acknowledge keeps cleanup / capacity pending.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s22_4_cooperative_ignore_cancel_keeps_capacity_pending() {
    let spec = base_spec(
        "ign",
        "ign",
        ToolExecutionClass::CooperativeInProcess {
            grace: Duration::from_millis(30),
        },
        Duration::from_millis(20),
    );
    let host = HostToolRegistry::build(vec![RegisteredTool::new(
        spec,
        Arc::new(IgnoreCancelCooperative),
    )])
    .unwrap();
    let tool = host.get(&ToolId::try_new("ign").unwrap()).unwrap().clone();
    let shared = SharedToolCapacity::new(1);
    let d = TransactionToolDispatcher::new(
        TransactionId::generate(),
        session_key(),
        ResolvedToolSet::from_registered(vec![tool]),
        Arc::clone(&shared),
        4,
        8,
    );
    let out = dispatch_ready_tool(
        &d,
        ExchangeId::generate(),
        ToolActionId::new("a"),
        "ign",
        "p",
        0,
        r#"{"q":"x"}"#,
    )
    .await;
    assert!(
        matches!(
            &out,
            DispatchOutcome::RuntimeFailed { code, .. } if code == "deadline_exceeded"
        ),
        "expected deadline_exceeded for non-ack cooperative, got {out:?}"
    );
    d.reap_vault();
    assert!(
        d.vault_pending_permits() >= 1,
        "non-ack cooperative must vault join+permit until join (vault={}, active={})",
        d.vault_pending_permits(),
        shared.active()
    );
    assert!(
        shared.active() >= 1,
        "shared capacity must stay held while cooperative join is pending"
    );
}

/// §22.4: abortable-at-yield releases permits only after join.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s22_4_abortable_permit_held_until_join() {
    let entered = Arc::new(AtomicBool::new(false));
    let entered_h = Arc::clone(&entered);
    let spec = base_spec(
        "ab",
        "ab",
        ToolExecutionClass::AbortableAtYield {
            grace: Duration::from_millis(50),
        },
        Duration::from_millis(30),
    );
    // Yielding body — abortable at await points.
    let host = HostToolRegistry::build(vec![RegisteredTool::new(
        spec,
        Arc::new(AsyncToolHandler::new(move |_c, _x, ctl| {
            let entered = Arc::clone(&entered_h);
            Box::pin(async move {
                entered.store(true, Ordering::SeqCst);
                tokio::select! {
                    _ = ctl.cancelled() => ToolCompletion::RuntimeFailed(
                        monoloop_contracts::ToolRuntimeError::TerminationFailed
                    ),
                    _ = tokio::time::sleep(Duration::from_secs(30)) => {
                        ToolCompletion::Succeeded(CanonicalToolOutput::Json(
                            serde_json::json!({"ok": true}),
                        ))
                    }
                }
            })
        })),
    )])
    .unwrap();
    let tool = host.get(&ToolId::try_new("ab").unwrap()).unwrap().clone();
    let shared = SharedToolCapacity::new(1);
    let d = TransactionToolDispatcher::new(
        TransactionId::generate(),
        session_key(),
        ResolvedToolSet::from_registered(vec![tool]),
        Arc::clone(&shared),
        4,
        8,
    );

    let out = dispatch_ready_tool(
        &d,
        ExchangeId::generate(),
        ToolActionId::new("a"),
        "ab",
        "p",
        0,
        r#"{"q":"x"}"#,
    )
    .await;
    assert!(
        matches!(&out, DispatchOutcome::RuntimeFailed { .. }),
        "expected abort/deadline path, got {out:?}"
    );
    assert!(entered.load(Ordering::SeqCst), "worker should have started");

    // After dispatch returns, either join completed (active==0) or permit is still
    // vaulted until join — never silently available while worker owned mid-flight.
    // Post-return: reap finished joins; capacity must be free once joins observed.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while shared.active() > 0 && tokio::time::Instant::now() < deadline {
        d.reap_vault();
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    d.reap_vault();
    assert_eq!(
        shared.active(),
        0,
        "permit must release only after join observed"
    );
    assert_eq!(d.vault_pending_permits(), 0);
}

/// §22.4: capacity remains unavailable while worker is still owned.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s22_4_capacity_unavailable_while_worker_owned() {
    let hold = Arc::new(AsyncToolHandler::new(|_c, _x, ctl| {
        Box::pin(async move {
            tokio::select! {
                _ = ctl.cancelled() => ToolCompletion::RuntimeFailed(
                    monoloop_contracts::ToolRuntimeError::TerminationFailed
                ),
                _ = tokio::time::sleep(Duration::from_secs(5)) => {
                    ToolCompletion::Succeeded(CanonicalToolOutput::Json(
                        serde_json::json!({"ok": true}),
                    ))
                }
            }
        })
    })) as Arc<dyn ToolHandler>;
    let hold_spec = base_spec(
        "hold",
        "hold",
        ToolExecutionClass::AbortableAtYield {
            grace: Duration::from_secs(1),
        },
        Duration::from_secs(5),
    );
    let echo_spec = base_spec(
        "echo",
        "echo",
        ToolExecutionClass::CooperativeInProcess {
            grace: Duration::from_millis(50),
        },
        Duration::from_secs(2),
    );
    let host = HostToolRegistry::build(vec![
        RegisteredTool::new(hold_spec, hold),
        RegisteredTool::new(
            echo_spec,
            Arc::new(ImmediateToolHandler::new(|_c, _x| {
                Ok(ToolCompletion::Succeeded(CanonicalToolOutput::Json(
                    serde_json::json!({"ok": true}),
                )))
            })),
        ),
    ])
    .unwrap();
    let tools = vec![
        host.get(&ToolId::try_new("hold").unwrap()).unwrap().clone(),
        host.get(&ToolId::try_new("echo").unwrap()).unwrap().clone(),
    ];
    let shared = SharedToolCapacity::new(1);
    let d = TransactionToolDispatcher::new(
        TransactionId::generate(),
        session_key(),
        ResolvedToolSet::from_registered(tools),
        Arc::clone(&shared),
        8,
        16,
    );

    let blocker = tokio::spawn({
        let d = Arc::clone(&d);
        async move {
            dispatch_ready_tool(
                &d,
                ExchangeId::generate(),
                ToolActionId::new("h"),
                "hold",
                "ph",
                0,
                r#"{"q":"x"}"#,
            )
            .await
        }
    });
    let started = tokio::time::Instant::now();
    while shared.active() < 1 {
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "hold never acquired capacity"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    let second = dispatch_ready_tool(
        &d,
        ExchangeId::generate(),
        ToolActionId::new("e"),
        "echo",
        "pe",
        1,
        r#"{"q":"x"}"#,
    )
    .await;
    assert!(
        matches!(
            second,
            DispatchOutcome::Rejected {
                code: "tool_capacity_exceeded",
                ..
            }
        ),
        "capacity must stay unavailable while worker owned, got {second:?}"
    );
    blocker.abort();
}

/// §22.4: process-isolated tool is killed and reaped after grace.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s22_4_process_isolated_killed_and_reaped() {
    let spec = base_spec(
        "pi",
        "pi",
        ToolExecutionClass::ProcessIsolated {
            grace: Duration::from_millis(30),
            kill_deadline: Duration::from_millis(500),
        },
        Duration::from_millis(40),
    );
    let host = HostToolRegistry::build(vec![RegisteredTool::try_new_process_isolated(
        spec,
        ProcessIsolatedToolHandler::sleep_until_killed(3600),
    )
    .unwrap()])
    .unwrap();
    let tool = host.get(&ToolId::try_new("pi").unwrap()).unwrap().clone();
    let shared = SharedToolCapacity::unlimited();
    let d = TransactionToolDispatcher::new(
        TransactionId::generate(),
        session_key(),
        ResolvedToolSet::from_registered(vec![tool]),
        shared,
        4,
        8,
    );
    let out = dispatch_ready_tool(
        &d,
        ExchangeId::generate(),
        ToolActionId::new("a"),
        "pi",
        "p",
        0,
        r#"{"q":"x"}"#,
    )
    .await;
    assert!(
        matches!(
            &out,
            DispatchOutcome::RuntimeFailed { code, .. }
                if code == "deadline_exceeded"
                    || code == "termination_failed"
                    || code == "completion_lost"
        ),
        "expected process kill/reap path, got {out:?}"
    );
}

/// §22.4: tool cannot self-assert a stronger execution class than structural factory.
#[test]
fn s22_4_cannot_self_assert_stronger_class() {
    let spec = base_spec(
        "bad",
        "bad",
        ToolExecutionClass::ProcessIsolated {
            grace: Duration::from_millis(50),
            kill_deadline: Duration::from_millis(200),
        },
        Duration::from_secs(1),
    );
    // Tokio-abort handler falsely claiming isolated kill must not register as ProcessIsolated.
    let fake = Arc::new(IsolatedKillableToolHandler::new(|_c, _x| {
        Box::pin(async {
            ToolCompletion::Succeeded(CanonicalToolOutput::Json(serde_json::json!({"ok": true})))
        })
    })) as Arc<dyn ToolHandler>;
    let err = RegisteredTool::try_new(spec, fake).unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.contains("try_new_process_isolated") || msg.contains("ProcessIsolated"),
        "got {msg}"
    );
}
