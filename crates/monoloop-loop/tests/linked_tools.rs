//! WP-06: linked tools — registry, dispatcher validation, Loop adapters.

use monoloop_contracts::OutboundToolOutcome;
use monoloop_contracts::{
    CanonicalToolError, CanonicalToolOutput, ChannelId, ExchangeId, JsonSchema, SessionId,
    SessionKey, ToolActionId, ToolCompletion, ToolExecutionClass, ToolId, ToolLimits, ToolName,
    ToolOutputContract, ToolSpec, ToolSuccessContract, TransactionId,
};
use monoloop_loop::{
    dispatch_ready_tool, AsyncToolHandler, DispatchOutcome, EmptyToolRegistry, HostToolRegistry,
    HostToolRuntime, ImmediateToolHandler, LostCompletionHandler, PanicOnStartHandler,
    RegisteredTool, ResolveToolRequest, ResolvedToolRegistry, ResolvedToolSet, SharedToolCapacity,
    StartFailHandler, StartToolExecution, ToolHandler, ToolRegistry, ToolResolution, ToolRuntime,
    ToolUnavailableReason, TransactionToolDispatcher,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

fn object_schema() -> JsonSchema {
    JsonSchema::try_new(serde_json::json!({
        "type": "object",
        "properties": {
            "q": { "type": "string" }
        },
        "required": ["q"],
        "additionalProperties": false
    }))
    .unwrap()
}

fn success_json_schema() -> JsonSchema {
    JsonSchema::try_new(serde_json::json!({
        "type": "object",
        "properties": {
            "ok": { "type": "boolean" }
        },
        "required": ["ok"],
        "additionalProperties": false
    }))
    .unwrap()
}

fn make_spec(id: &str, name: &str) -> ToolSpec {
    ToolSpec::try_new(
        ToolId::try_new(id).unwrap(),
        ToolName::try_new(name).unwrap(),
        "test tool",
        object_schema(),
        ToolOutputContract {
            success: ToolSuccessContract::json(success_json_schema()),
            error_data_schema: None,
        },
        ToolLimits {
            max_concurrent: 2,
            max_input_bytes: 1024,
            max_output_bytes: 1024,
            execution_deadline: Duration::from_secs(5),
        },
        ToolExecutionClass::CooperativeInProcess {
            grace: std::time::Duration::from_millis(50),
        },
    )
    .unwrap()
}

fn session_key() -> SessionKey {
    SessionKey::new(
        ChannelId::try_new("ch").unwrap(),
        SessionId::try_new("s1").unwrap(),
    )
}

fn ok_handler() -> Arc<dyn ToolHandler> {
    Arc::new(ImmediateToolHandler::new(|_call, _ctx| {
        Ok(ToolCompletion::Succeeded(CanonicalToolOutput::Json(
            serde_json::json!({"ok": true}),
        )))
    }))
}

fn domain_fail_handler() -> Arc<dyn ToolHandler> {
    Arc::new(ImmediateToolHandler::new(|_call, _ctx| {
        Ok(ToolCompletion::DomainFailed(
            CanonicalToolError::try_new("not_found", "missing", None, 256).unwrap(),
        ))
    }))
}

fn build_registry(tools: Vec<(&str, &str, Arc<dyn ToolHandler>)>) -> HostToolRegistry {
    let registered = tools
        .into_iter()
        .map(|(id, name, h)| RegisteredTool::new(make_spec(id, name), h))
        .collect();
    HostToolRegistry::build(registered).unwrap()
}

fn dispatcher_from(host: &HostToolRegistry, ids: &[&str]) -> Arc<TransactionToolDispatcher> {
    let tools: Vec<_> = ids
        .iter()
        .map(|id| host.get(&ToolId::try_new(*id).unwrap()).unwrap().clone())
        .collect();
    let resolved = ResolvedToolSet::from_registered(tools);
    TransactionToolDispatcher::new(
        TransactionId::generate(),
        session_key(),
        resolved,
        SharedToolCapacity::unlimited(),
        8,
        16,
    )
}

#[test]
fn empty_registry_and_unknown_duplicate() {
    assert!(HostToolRegistry::empty().is_empty());
    let host = build_registry(vec![("echo", "echo", ok_handler())]);
    assert!(host.get(&ToolId::try_new("missing").unwrap()).is_none());

    let dup_id = HostToolRegistry::build(vec![
        RegisteredTool::new(make_spec("a", "n1"), ok_handler()),
        RegisteredTool::new(make_spec("a", "n2"), ok_handler()),
    ]);
    assert!(dup_id.is_err());

    let dup_name = HostToolRegistry::build(vec![
        RegisteredTool::new(make_spec("a", "same"), ok_handler()),
        RegisteredTool::new(make_spec("b", "same"), ok_handler()),
    ]);
    assert!(dup_name.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_resolved_set_rejects_dispatch() {
    let d = TransactionToolDispatcher::new(
        TransactionId::generate(),
        session_key(),
        ResolvedToolSet::empty(),
        SharedToolCapacity::unlimited(),
        8,
        16,
    );
    let out = dispatch_ready_tool(
        &d,
        ExchangeId::generate(),
        ToolActionId::new("a1"),
        "echo",
        "p1",
        0,
        r#"{"q":"hi"}"#,
    )
    .await;
    assert!(matches!(
        out,
        DispatchOutcome::Rejected {
            code: "tool_not_allowed",
            ..
        }
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn success_and_domain_failure() {
    let host = build_registry(vec![
        ("echo", "echo", ok_handler()),
        ("fail", "fail", domain_fail_handler()),
    ]);
    let d = dispatcher_from(&host, &["echo", "fail"]);

    let ok = dispatch_ready_tool(
        &d,
        ExchangeId::generate(),
        ToolActionId::new("a1"),
        "echo",
        "p1",
        0,
        r#"{"q":"hi"}"#,
    )
    .await;
    match ok {
        DispatchOutcome::Canonical { result, lifecycle } => {
            assert!(matches!(
                result.outcome,
                monoloop_contracts::CanonicalToolResultOutcome::Succeeded(_)
            ));
            assert!(lifecycle.len() >= 2);
        }
        other => panic!("expected canonical success: {other:?}"),
    }

    let fail = dispatch_ready_tool(
        &d,
        ExchangeId::generate(),
        ToolActionId::new("a2"),
        "fail",
        "p2",
        1,
        r#"{"q":"x"}"#,
    )
    .await;
    match fail {
        DispatchOutcome::Canonical { result, .. } => {
            assert!(matches!(
                result.outcome,
                monoloop_contracts::CanonicalToolResultOutcome::DomainFailed(_)
            ));
        }
        other => panic!("expected domain failure: {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn schema_invalid_and_oversized_arguments() {
    let host = build_registry(vec![("echo", "echo", ok_handler())]);
    let d = dispatcher_from(&host, &["echo"]);

    let bad = dispatch_ready_tool(
        &d,
        ExchangeId::generate(),
        ToolActionId::new("a1"),
        "echo",
        "p1",
        0,
        r#"{"q":1}"#,
    )
    .await;
    assert!(matches!(
        bad,
        DispatchOutcome::Rejected {
            code: "schema_invalid",
            ..
        }
    ));

    let big = format!(r#"{{"q":"{}"}}"#, "x".repeat(2000));
    let over = dispatch_ready_tool(
        &d,
        ExchangeId::generate(),
        ToolActionId::new("a2"),
        "echo",
        "p2",
        1,
        &big,
    )
    .await;
    assert!(matches!(
        over,
        DispatchOutcome::Rejected {
            code: "oversized_input",
            ..
        }
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn start_failure_panic_lost_completion() {
    let host = build_registry(vec![
        (
            "sf",
            "sf",
            Arc::new(StartFailHandler { reason: "nope" }) as Arc<dyn ToolHandler>,
        ),
        (
            "panic",
            "panic",
            Arc::new(PanicOnStartHandler) as Arc<dyn ToolHandler>,
        ),
        (
            "lost",
            "lost",
            Arc::new(LostCompletionHandler) as Arc<dyn ToolHandler>,
        ),
    ]);
    let d = dispatcher_from(&host, &["sf", "panic", "lost"]);

    let sf = dispatch_ready_tool(
        &d,
        ExchangeId::generate(),
        ToolActionId::new("a1"),
        "sf",
        "p1",
        0,
        r#"{"q":"x"}"#,
    )
    .await;
    assert!(matches!(sf, DispatchOutcome::RuntimeFailed { .. }));

    let pan = dispatch_ready_tool(
        &d,
        ExchangeId::generate(),
        ToolActionId::new("a2"),
        "panic",
        "p2",
        1,
        r#"{"q":"x"}"#,
    )
    .await;
    assert!(matches!(pan, DispatchOutcome::RuntimeFailed { code, .. } if code == "panicked"));

    let lost = dispatch_ready_tool(
        &d,
        ExchangeId::generate(),
        ToolActionId::new("a3"),
        "lost",
        "p3",
        2,
        r#"{"q":"x"}"#,
    )
    .await;
    assert!(matches!(
        lost,
        DispatchOutcome::RuntimeFailed { code, .. } if code == "completion_lost"
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn output_contract_violation() {
    let bad_out = Arc::new(ImmediateToolHandler::new(|_call, _ctx| {
        Ok(ToolCompletion::Succeeded(CanonicalToolOutput::Text(
            "not-json".into(),
        )))
    })) as Arc<dyn ToolHandler>;
    let host = build_registry(vec![("echo", "echo", bad_out)]);
    let d = dispatcher_from(&host, &["echo"]);
    let out = dispatch_ready_tool(
        &d,
        ExchangeId::generate(),
        ToolActionId::new("a1"),
        "echo",
        "p1",
        0,
        r#"{"q":"x"}"#,
    )
    .await;
    assert!(matches!(
        out,
        DispatchOutcome::RuntimeFailed { code, .. } if code == "output_contract_violated"
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_dispatches_different_order() {
    let counter = Arc::new(AtomicUsize::new(0));
    let c2 = Arc::clone(&counter);
    let slow = Arc::new(AsyncToolHandler::new(move |_call, _ctx, _ctl| {
        let c2 = Arc::clone(&c2);
        Box::pin(async move {
            tokio::time::sleep(Duration::from_millis(30)).await;
            let _n = c2.fetch_add(1, Ordering::SeqCst);
            ToolCompletion::Succeeded(CanonicalToolOutput::Json(serde_json::json!({"ok": true})))
        })
    })) as Arc<dyn ToolHandler>;
    let fast = Arc::new(ImmediateToolHandler::new({
        let c = Arc::clone(&counter);
        move |_call, _ctx| {
            let _n = c.fetch_add(1, Ordering::SeqCst);
            Ok(ToolCompletion::Succeeded(CanonicalToolOutput::Json(
                serde_json::json!({"ok": true}),
            )))
        }
    })) as Arc<dyn ToolHandler>;

    // Two tool names; same schema.
    let host = HostToolRegistry::build(vec![
        RegisteredTool::new(make_spec("slow", "slow"), slow),
        RegisteredTool::new(make_spec("fast", "fast"), fast),
    ])
    .unwrap();
    let d = dispatcher_from(&host, &["slow", "fast"]);

    let (r_slow, r_fast) = tokio::join!(
        dispatch_ready_tool(
            &d,
            ExchangeId::generate(),
            ToolActionId::new("s"),
            "slow",
            "ps",
            0,
            r#"{"q":"s"}"#,
        ),
        dispatch_ready_tool(
            &d,
            ExchangeId::generate(),
            ToolActionId::new("f"),
            "fast",
            "pf",
            1,
            r#"{"q":"f"}"#,
        ),
    );
    assert!(
        matches!(r_slow, DispatchOutcome::Canonical { .. }),
        "slow: {r_slow:?}"
    );
    assert!(
        matches!(r_fast, DispatchOutcome::Canonical { .. }),
        "fast: {r_fast:?}"
    );
    // Fast may complete first; ordinals remain request order independent of completion.
    if let DispatchOutcome::Canonical { result, .. } = r_fast {
        assert_eq!(result.request_ordinal, 1);
    }
    if let DispatchOutcome::Canonical { result, .. } = r_slow {
        assert_eq!(result.request_ordinal, 0);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn capacity_limit_plus_one_rejects() {
    // Shared global capacity of 1: second concurrent start must reject.
    let hold = Arc::new(AsyncToolHandler::new(|_call, _ctx, ctl| {
        Box::pin(async move {
            tokio::select! {
                _ = ctl.cancelled() => ToolCompletion::RuntimeFailed(
                    monoloop_contracts::ToolRuntimeError::TerminationFailed
                ),
                _ = tokio::time::sleep(Duration::from_secs(2)) => {
                    ToolCompletion::Succeeded(CanonicalToolOutput::Json(
                        serde_json::json!({"ok": true}),
                    ))
                }
            }
        })
    })) as Arc<dyn ToolHandler>;
    let host = HostToolRegistry::build(vec![
        RegisteredTool::new(make_spec("hold", "hold"), hold),
        RegisteredTool::new(make_spec("echo", "echo"), ok_handler()),
    ])
    .unwrap();
    let tools = vec![
        host.get(&ToolId::try_new("hold").unwrap()).unwrap().clone(),
        host.get(&ToolId::try_new("echo").unwrap()).unwrap().clone(),
    ];
    let resolved = ResolvedToolSet::from_registered(tools);
    let shared = SharedToolCapacity::new(1);
    let d = TransactionToolDispatcher::new(
        TransactionId::generate(),
        session_key(),
        resolved,
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
    // Wait until the hold tool has acquired the sole shared slot (not a fixed sleep).
    let started = tokio::time::Instant::now();
    while shared.active() < 1 {
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "hold tool never acquired capacity"
        );
        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    let second = dispatch_ready_tool(
        &d,
        ExchangeId::generate(),
        ToolActionId::new("a1"),
        "echo",
        "p1",
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
        "expected capacity reject, got {second:?}"
    );
    blocker.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn simultaneous_transactions_different_tools() {
    let host_a = build_registry(vec![("a", "tool_a", ok_handler())]);
    let host_b = build_registry(vec![("b", "tool_b", ok_handler())]);
    let da = dispatcher_from(&host_a, &["a"]);
    let db = dispatcher_from(&host_b, &["b"]);

    let a_on_b = dispatch_ready_tool(
        &db,
        ExchangeId::generate(),
        ToolActionId::new("x"),
        "tool_a",
        "p",
        0,
        r#"{"q":"x"}"#,
    )
    .await;
    assert!(matches!(
        a_on_b,
        DispatchOutcome::Rejected {
            code: "tool_not_allowed",
            ..
        }
    ));

    let b_ok = dispatch_ready_tool(
        &db,
        ExchangeId::generate(),
        ToolActionId::new("y"),
        "tool_b",
        "p",
        0,
        r#"{"q":"x"}"#,
    )
    .await;
    assert!(matches!(b_ok, DispatchOutcome::Canonical { .. }));

    let a_ok = dispatch_ready_tool(
        &da,
        ExchangeId::generate(),
        ToolActionId::new("z"),
        "tool_a",
        "p",
        0,
        r#"{"q":"x"}"#,
    )
    .await;
    assert!(matches!(a_ok, DispatchOutcome::Canonical { .. }));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn loop_adapters_available_not_dispatch_rejected_placeholder() {
    let host = build_registry(vec![("echo", "echo", ok_handler())]);
    let tools = vec![host.get(&ToolId::try_new("echo").unwrap()).unwrap().clone()];
    let resolved = ResolvedToolSet::from_registered(tools);
    let reg = ResolvedToolRegistry::new(resolved.clone());
    let d = TransactionToolDispatcher::new(
        TransactionId::generate(),
        session_key(),
        resolved,
        SharedToolCapacity::unlimited(),
        8,
        16,
    );
    let runtime = HostToolRuntime::new(d, ExchangeId::generate());

    let resolution = reg
        .resolve(ResolveToolRequest {
            tool_action_id: ToolActionId::new("a1"),
            tool_name: "echo".into(),
            request_payload: r#"{"q":"hi"}"#.into(),
        })
        .await
        .unwrap();
    assert!(matches!(resolution, ToolResolution::Available(_)));

    let handle = runtime
        .start(StartToolExecution {
            execution_id: monoloop_contracts::ToolExecutionId::generate(),
            tool_action_id: "a1".into(),
            tool_name: "echo".into(),
            request_payload: r#"{"q":"hi"}"#.into(),
            request_generation: 1,
        })
        .unwrap();
    let terminal = handle.completion.unwrap().await.unwrap();
    assert_eq!(terminal.outcome, OutboundToolOutcome::Success);
    assert!(!terminal.payload.contains("deferred"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_tool_registry_still_unavailable() {
    let empty = EmptyToolRegistry::new();
    let r = empty
        .resolve(ResolveToolRequest {
            tool_action_id: ToolActionId::new("a"),
            tool_name: "x".into(),
            request_payload: "{}".into(),
        })
        .await
        .unwrap();
    assert!(matches!(
        r,
        ToolResolution::Unavailable(ToolUnavailableReason::NoRegisteredTool)
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancel_running_async_tool() {
    // Short deadline forces timeout cancel path while the handler still runs.
    // Abortable + AsyncToolHandler (supports_abort + kill handle) is the structural pair.
    let mut spec = make_spec("c", "cancel_me");
    spec.execution_class = ToolExecutionClass::AbortableAtYield {
        grace: std::time::Duration::from_secs(1),
    };
    spec.limits.execution_deadline = Duration::from_millis(50);
    let host = HostToolRegistry::build(vec![RegisteredTool::new(
        spec,
        Arc::new(AsyncToolHandler::new(move |_call, _ctx, ctl| {
            Box::pin(async move {
                tokio::time::sleep(Duration::from_secs(5)).await;
                let _ = ctl.is_cancelled();
                ToolCompletion::Succeeded(CanonicalToolOutput::Json(
                    serde_json::json!({"ok": true}),
                ))
            })
        })),
    )])
    .unwrap();
    let d = dispatcher_from(&host, &["c"]);
    let out = dispatch_ready_tool(
        &d,
        ExchangeId::generate(),
        ToolActionId::new("a"),
        "cancel_me",
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
                    || code == "completion_lost"
                    || code == "termination_failed"
        ),
        "expected deadline abort path, got {out:?}"
    );
}

/// D-043: ProcessIsolated uses OS child; escalate kill stops work within deadline.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn process_isolated_escalates_after_grace_and_stops_work() {
    let spec = ToolSpec::try_new(
        ToolId::try_new("ik").unwrap(),
        ToolName::try_new("ik").unwrap(),
        "isolated",
        object_schema(),
        ToolOutputContract {
            success: ToolSuccessContract::json(success_json_schema()),
            error_data_schema: None,
        },
        ToolLimits {
            max_concurrent: 2,
            max_input_bytes: 1024,
            max_output_bytes: 1024,
            execution_deadline: Duration::from_millis(40),
        },
        ToolExecutionClass::ProcessIsolated {
            grace: Duration::from_millis(30),
            kill_deadline: Duration::from_millis(200),
        },
    )
    .unwrap();
    let host = HostToolRegistry::build(vec![RegisteredTool::try_new_process_isolated(
        spec,
        monoloop_loop::ProcessIsolatedToolHandler::sleep_until_killed(3600),
    )
    .unwrap()])
    .unwrap();
    let tool = host.get(&ToolId::try_new("ik").unwrap()).unwrap().clone();
    let d = TransactionToolDispatcher::new(
        TransactionId::generate(),
        session_key(),
        ResolvedToolSet::from_registered(vec![tool]),
        SharedToolCapacity::unlimited(),
        8,
        16,
    );
    let out = dispatch_ready_tool(
        &d,
        ExchangeId::generate(),
        ToolActionId::new("a"),
        "ik",
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
        "expected terminate/deadline path, got {out:?}"
    );
}

/// D-043: ProcessIsolated cannot register via dyn try_new (structural factory required).
#[test]
fn process_isolated_requires_typed_factory() {
    let spec = ToolSpec::try_new(
        ToolId::try_new("bad").unwrap(),
        ToolName::try_new("bad").unwrap(),
        "bad",
        object_schema(),
        ToolOutputContract {
            success: ToolSuccessContract::json(success_json_schema()),
            error_data_schema: None,
        },
        ToolLimits {
            max_concurrent: 1,
            max_input_bytes: 1024,
            max_output_bytes: 1024,
            execution_deadline: Duration::from_secs(1),
        },
        ToolExecutionClass::ProcessIsolated {
            grace: Duration::from_millis(50),
            kill_deadline: Duration::from_millis(200),
        },
    )
    .unwrap();
    let err = RegisteredTool::try_new(spec, ok_handler()).unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.contains("try_new_process_isolated") || msg.contains("ProcessIsolated"),
        "got {msg}"
    );
}
