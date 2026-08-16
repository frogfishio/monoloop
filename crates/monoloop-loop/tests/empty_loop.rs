//! Empty-tool Loop qualification.

use monoloop_contracts::{
    CanonicalUnit, CanonicalUnitEvent, CanonicalUnitSnapshot, ConnectionId, ExternalSessionId,
    FlowId, InterpretationId, InterpreterOutputEvent, LaneId, LoopEndKind, LoopId, LoopLimits,
    LoopOutputEvent, LoopScope, MonoloopRunId, OutboundToolOutcome, TextChannel, TextSentence,
    ToolActionEvent, ToolActionId, ToolExecutionState, ToolRequestState, ToolResultState, UnitId,
    UnitState,
};
use monoloop_loop::{DefaultLoopRuntime, SubscriptionPublisher};

fn text_event(interp: &str, content: &str) -> InterpreterOutputEvent {
    InterpreterOutputEvent::Unit(CanonicalUnitEvent::Created(CanonicalUnitSnapshot {
        unit_id: UnitId::new("s1"),
        unit_generation: 1,
        unit_state: UnitState::Complete,
        interpretation_id: InterpretationId::new(interp),
        connection_id: ConnectionId::new("c1"),
        external_session_id: None,
        flow_id: FlowId::main(),
        lane_id: LaneId::response(),
        lane_ordinal: 1,
        causal_parent_id: None,
        source_time: None,
        unit: CanonicalUnit::Text(TextSentence {
            sentence_id: UnitId::new("s1"),
            channel: TextChannel::PublicResponse,
            paragraph_id: None,
            sentence_ordinal: 1,
            content: content.into(),
        }),
    }))
}

fn tool_ready(
    interp: &str,
    action: &str,
    name: &str,
    payload: &str,
    gen: u64,
) -> InterpreterOutputEvent {
    InterpreterOutputEvent::Unit(CanonicalUnitEvent::Created(CanonicalUnitSnapshot {
        unit_id: UnitId::new(format!("tool-{action}")),
        unit_generation: gen,
        unit_state: UnitState::Waiting,
        interpretation_id: InterpretationId::new(interp),
        connection_id: ConnectionId::new("c1"),
        external_session_id: Some(ExternalSessionId::new("sess-1")),
        flow_id: FlowId::main(),
        lane_id: LaneId::tool(),
        lane_ordinal: 1,
        causal_parent_id: None,
        source_time: None,
        unit: CanonicalUnit::Tool(ToolActionEvent {
            tool_action_id: ToolActionId::new(action),
            tool_name: Some(name.into()),
            request_state: ToolRequestState::Ready,
            execution_state: ToolExecutionState::Waiting,
            result_state: ToolResultState::Absent,
            request_payload: Some(payload.into()),
            result_payload: None,
            terminal_outcome: None,
            waiting_for: Some("external execution".into()),
        }),
    }))
}

fn tool_waiting(interp: &str, action: &str, gen: u64) -> InterpreterOutputEvent {
    InterpreterOutputEvent::Unit(CanonicalUnitEvent::Created(CanonicalUnitSnapshot {
        unit_id: UnitId::new(format!("tool-{action}")),
        unit_generation: gen,
        unit_state: UnitState::Waiting,
        interpretation_id: InterpretationId::new(interp),
        connection_id: ConnectionId::new("c1"),
        external_session_id: None,
        flow_id: FlowId::main(),
        lane_id: LaneId::tool(),
        lane_ordinal: 1,
        causal_parent_id: None,
        source_time: None,
        unit: CanonicalUnit::Tool(ToolActionEvent {
            tool_action_id: ToolActionId::new(action),
            tool_name: Some("bash".into()),
            request_state: ToolRequestState::Assembling,
            execution_state: ToolExecutionState::Waiting,
            result_state: ToolResultState::Absent,
            request_payload: None,
            result_payload: None,
            terminal_outcome: None,
            waiting_for: Some("args".into()),
        }),
    }))
}

fn open_scope(run: MonoloopRunId, loop_id: LoopId) -> LoopScope {
    LoopScope {
        monoloop_run_id: run,
        loop_id,
        accepted_interpretation_ids: vec![],
        accepted_connection_ids: vec![],
        accepted_external_session_ids: vec![],
        accept_all_in_run: true,
    }
}

#[tokio::test]
async fn text_does_not_dispatch() {
    let (pub_, sub) = SubscriptionPublisher::channel("loop", 16);
    let run = MonoloopRunId::new("r1");
    let loop_id = LoopId::new("l1");
    let handle = DefaultLoopRuntime::new()
        .start_empty(
            run.clone(),
            loop_id.clone(),
            open_scope(run, loop_id),
            sub,
            LoopLimits::default(),
        )
        .unwrap();

    pub_.publish(text_event("i1", "Hello world.")).await.unwrap();
    drop(pub_);

    let end = handle.completion.wait().await;
    assert_eq!(end.kind, LoopEndKind::Drained);
    assert_eq!(end.tools_unavailable, 0);
    assert_eq!(end.outbound_results_emitted, 0);
}

#[tokio::test]
async fn empty_registry_unavailable_zero_effects() {
    let (pub_, sub) = SubscriptionPublisher::channel("loop", 16);
    let run = MonoloopRunId::new("r2");
    let loop_id = LoopId::new("l2");
    let handle = DefaultLoopRuntime::new()
        .start_empty(
            run.clone(),
            loop_id.clone(),
            open_scope(run, loop_id),
            sub,
            LoopLimits::default(),
        )
        .unwrap();

    pub_.publish(tool_waiting("i1", "t1", 1)).await.unwrap();
    pub_
        .publish(tool_ready("i1", "t1", "bash", r#"{"cmd":"ls"}"#, 2))
        .await
        .unwrap();
    // Duplicate ready must not double-dispatch
    pub_
        .publish(tool_ready("i1", "t1", "bash", r#"{"cmd":"ls"}"#, 2))
        .await
        .unwrap();
    drop(pub_);

    let mut unavailable = 0;
    let mut outbound = 0;
    {
        let mut rx = handle.output.lock().await;
        while let Some(ev) = rx.recv().await {
            match &ev {
                LoopOutputEvent::ToolUnavailable { .. } => unavailable += 1,
                LoopOutputEvent::OutboundToolResult(r) => {
                    assert_eq!(r.outcome, OutboundToolOutcome::ToolUnavailable);
                    assert!(r.tool_execution_id.is_none());
                    outbound += 1;
                }
                LoopOutputEvent::LoopEnded(_) => break,
                _ => {}
            }
        }
    }
    assert_eq!(unavailable, 1);
    assert_eq!(outbound, 1);
    let end = handle.completion.wait().await;
    assert_eq!(end.tools_unavailable, 1);
    assert_eq!(end.outbound_results_emitted, 1);
    assert!(end.duplicate_events >= 1);
}

#[tokio::test]
async fn waiting_never_dispatches() {
    let (pub_, sub) = SubscriptionPublisher::channel("loop", 8);
    let run = MonoloopRunId::new("r3");
    let loop_id = LoopId::new("l3");
    let handle = DefaultLoopRuntime::new()
        .start_empty(
            run.clone(),
            loop_id.clone(),
            open_scope(run, loop_id),
            sub,
            LoopLimits::default(),
        )
        .unwrap();

    pub_.publish(tool_waiting("i1", "t9", 1)).await.unwrap();
    drop(pub_);
    let end = handle.completion.wait().await;
    assert_eq!(end.tools_unavailable, 0);
    assert_eq!(end.outbound_results_emitted, 0);
}

#[tokio::test]
async fn cancel_stops_loop() {
    let (_pub_, sub) = SubscriptionPublisher::channel("loop", 8);
    let run = MonoloopRunId::new("r4");
    let loop_id = LoopId::new("l4");
    let handle = DefaultLoopRuntime::new()
        .start_empty(
            run.clone(),
            loop_id.clone(),
            open_scope(run, loop_id),
            sub,
            LoopLimits::default(),
        )
        .unwrap();
    handle.control.cancel();
    let end = handle.completion.wait().await;
    assert_eq!(end.kind, LoopEndKind::Cancelled);
}
