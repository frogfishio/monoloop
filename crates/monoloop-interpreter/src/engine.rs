//! Interpretation instance: feed raw bytes, assemble, publish canonical events.

use crate::acp::{drain_json_values, AcpDialect, AcpFragment, ToolSignal};
use crate::sentence::SentenceSegmenter;
use crate::stream::{CanonicalEventStream, EventPublisher};
use monoloop_contracts::{
    BoundaryKind, CanonicalUnit, CanonicalUnitEvent, CanonicalUnitSnapshot, ConnectionId,
    DiagnosticKind, DialectBinding, DialectFamily, ExternalSessionId, FlowId, InterpretationEnd,
    InterpretationEndKind, InterpretationId, InterpretationLimits, InterpreterError,
    InterpreterErrorKind, InterpreterOutputEvent, LaneId, ModelDiagnostic, SemanticBoundary,
    TextChannel, TextSentence, ToolActionEvent, ToolActionId, ToolExecutionState, ToolRequestState,
    ToolResultState, ToolTerminalOutcome, UnitId, UnitState, UsageObservation,
};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, Mutex};

/// Request to start an interpretation on one connection output.
#[derive(Clone, Debug)]
pub struct StartInterpretation {
    /// Interpretation identity.
    pub interpretation_id: InterpretationId,
    /// Connection identity.
    pub connection_id: ConnectionId,
    /// External session when present (propagated unchanged).
    pub external_session_id: Option<ExternalSessionId>,
    /// Frozen dialect binding from Connector open.
    pub dialect: DialectBinding,
    /// Assembly limits.
    pub limits: InterpretationLimits,
}

/// Handle returned by the factory.
pub struct Interpretation {
    /// Feed raw bytes here.
    pub input: InterpretationInput,
    /// Canonical event stream.
    pub events: Arc<CanonicalEventStream>,
    /// Status snapshot.
    pub status: InterpretationStatus,
    /// Completes with InterpretationEnd.
    pub completion: InterpretationCompletion,
}

/// Cloneable input handle for raw Connector output chunks.
#[derive(Clone)]
pub struct InterpretationInput {
    tx: mpsc::Sender<InputCmd>,
}

enum InputCmd {
    Bytes(bytes::Bytes),
    /// Clean dialect/source end (remote EOF after clean response).
    FinishClean,
    /// Abrupt cancel.
    Cancel,
    /// Transport failure.
    TransportFailed,
}

impl InterpretationInput {
    /// Push an ordered raw chunk (fragment boundaries carry no meaning).
    pub async fn push_bytes(&self, bytes: bytes::Bytes) -> Result<(), InterpreterError> {
        self.tx
            .send(InputCmd::Bytes(bytes))
            .await
            .map_err(|_| InterpreterError::cancelled())
    }

    /// Signal clean source completion (may seal final sentences).
    pub async fn finish_clean(&self) -> Result<(), InterpreterError> {
        self.tx
            .send(InputCmd::FinishClean)
            .await
            .map_err(|_| InterpreterError::cancelled())
    }

    /// Cancel interpretation.
    pub async fn cancel(&self) -> Result<(), InterpreterError> {
        self.tx
            .send(InputCmd::Cancel)
            .await
            .map_err(|_| InterpreterError::cancelled())
    }

    /// Abrupt transport failure.
    pub async fn transport_failed(&self) -> Result<(), InterpreterError> {
        self.tx
            .send(InputCmd::TransportFailed)
            .await
            .map_err(|_| InterpreterError::cancelled())
    }
}

/// Lightweight status.
#[derive(Clone, Debug, Default)]
pub struct InterpretationStatus {
    /// Whether terminal end was published.
    pub terminal: Arc<AtomicBool>,
    /// Source bytes consumed.
    pub bytes_consumed: Arc<AtomicU64>,
}

/// Completion handle.
pub struct InterpretationCompletion {
    rx: Mutex<Option<oneshot::Receiver<InterpretationEnd>>>,
}

impl InterpretationCompletion {
    /// Wait for exactly one terminal InterpretationEnd.
    pub async fn wait(self) -> InterpretationEnd {
        let mut guard = self.rx.lock().await;
        let rx = guard
            .take()
            .expect("InterpretationCompletion polled twice");
        rx.await.unwrap_or_else(|_| InterpretationEnd {
            interpretation_id: InterpretationId::new("unknown"),
            connection_id: ConnectionId::new("unknown"),
            external_session_id: None,
            kind: InterpretationEndKind::InvariantFailed,
            canonical_event_count: 0,
            completed_sentence_count: 0,
            completed_structure_count: 0,
            unresolved_text_bytes: 0,
            source_bytes_consumed: 0,
            safe_diagnostics: vec!["completion channel dropped".into()],
        })
    }
}

pub(crate) fn spawn_interpretation(
    request: StartInterpretation,
) -> Result<Interpretation, InterpreterError> {
    validate_dialect(&request.dialect)?;

    let (pub_, events) = EventPublisher::new(request.limits.max_output_queue_items);
    let (cmd_tx, cmd_rx) = mpsc::channel::<InputCmd>(64);
    let (end_tx, end_rx) = oneshot::channel();
    let status = InterpretationStatus::default();

    let input = InterpretationInput { tx: cmd_tx };
    let events = Arc::new(events);

    let status_terminal = Arc::clone(&status.terminal);
    let status_bytes = Arc::clone(&status.bytes_consumed);

    tokio::spawn(async move {
        let mut owner = Owner {
            request,
            pub_,
            segmenters: HashMap::new(),
            tools: HashMap::new(),
            lane_ordinals: HashMap::new(),
            next_unit: 1,
            sentence_count: 0,
            structure_count: 0,
            source_bytes: 0,
            frame_buf: Vec::new(),
            ended: false,
            diagnostics: Vec::new(),
            response_started: false,
            unresolved_bytes_at_end: 0,
        };
        owner.run(cmd_rx, end_tx, status_terminal, status_bytes).await;
    });

    Ok(Interpretation {
        input,
        events,
        status,
        completion: InterpretationCompletion {
            rx: Mutex::new(Some(end_rx)),
        },
    })
}

fn validate_dialect(binding: &DialectBinding) -> Result<(), InterpreterError> {
    match &binding.output.family {
        DialectFamily::Acp | DialectFamily::GrokBuild | DialectFamily::Test => Ok(()),
        other => Err(InterpreterError::unsupported_dialect(format!(
            "unsupported dialect family: {other:?}"
        ))),
    }
}

struct Owner {
    request: StartInterpretation,
    pub_: EventPublisher,
    segmenters: HashMap<(TextChannel,), SentenceSegmenter>,
    tools: HashMap<String, ToolAssembler>,
    lane_ordinals: HashMap<String, u64>,
    next_unit: u64,
    sentence_count: u64,
    structure_count: u64,
    source_bytes: u64,
    frame_buf: Vec<u8>,
    ended: bool,
    diagnostics: Vec<String>,
    response_started: bool,
    unresolved_bytes_at_end: u64,
}

struct ToolAssembler {
    action_id: ToolActionId,
    unit_id: UnitId,
    generation: u64,
    tool_name: Option<String>,
    request_state: ToolRequestState,
    execution_state: ToolExecutionState,
    result_state: ToolResultState,
    request_payload: Option<String>,
    result_payload: Option<String>,
    terminal: Option<ToolTerminalOutcome>,
    waiting_for: Option<String>,
}

impl Owner {
    async fn run(
        &mut self,
        mut cmd_rx: mpsc::Receiver<InputCmd>,
        end_tx: oneshot::Sender<InterpretationEnd>,
        status_terminal: Arc<AtomicBool>,
        status_bytes: Arc<AtomicU64>,
    ) {
        while let Some(cmd) = cmd_rx.recv().await {
            match cmd {
                InputCmd::Bytes(b) => {
                    if self.ended {
                        continue;
                    }
                    self.source_bytes += b.len() as u64;
                    status_bytes.store(self.source_bytes, Ordering::Relaxed);
                    if let Err(e) = self.ingest_bytes(&b).await {
                        let kind = match e.kind {
                            InterpreterErrorKind::Cancelled => InterpretationEndKind::Cancelled,
                            InterpreterErrorKind::FrameLimitExceeded
                            | InterpreterErrorKind::SentenceLimitExceeded
                            | InterpreterErrorKind::StructureLimitExceeded
                            | InterpreterErrorKind::ToolLimitExceeded => {
                                InterpretationEndKind::LimitExceeded
                            }
                            InterpreterErrorKind::MalformedFrame
                            | InterpreterErrorKind::MalformedSemanticPayload => {
                                InterpretationEndKind::DialectFailed
                            }
                            _ => InterpretationEndKind::DialectFailed,
                        };
                        self.finish(kind, end_tx, status_terminal).await;
                        return;
                    }
                }
                InputCmd::FinishClean => {
                    let _ = self.seal_clean().await;
                    self.finish(InterpretationEndKind::Complete, end_tx, status_terminal)
                        .await;
                    return;
                }
                InputCmd::Cancel => {
                    let _ = self.quarantine_partials().await;
                    self.finish(InterpretationEndKind::Cancelled, end_tx, status_terminal)
                        .await;
                    return;
                }
                InputCmd::TransportFailed => {
                    let _ = self.quarantine_partials().await;
                    self.finish(
                        InterpretationEndKind::TransportFailed,
                        end_tx,
                        status_terminal,
                    )
                    .await;
                    return;
                }
            }
        }
        // Input dropped without finish
        let _ = self.quarantine_partials().await;
        self.finish(
            InterpretationEndKind::TransportFailed,
            end_tx,
            status_terminal,
        )
        .await;
    }

    async fn ingest_bytes(&mut self, chunk: &[u8]) -> Result<(), InterpreterError> {
        if self.frame_buf.len() + chunk.len() > self.request.limits.max_undecoded_bytes {
            return Err(InterpreterError::limit("undecoded buffer limit exceeded"));
        }
        self.frame_buf.extend_from_slice(chunk);

        match &self.request.dialect.output.family {
            DialectFamily::Test => self.ingest_test_text().await,
            DialectFamily::Acp | DialectFamily::GrokBuild => self.ingest_acp().await,
            _ => Err(InterpreterError::unsupported_dialect("dialect")),
        }
    }

    /// Test dialect: raw UTF-8 text assembly (no JSON framing).
    async fn ingest_test_text(&mut self) -> Result<(), InterpreterError> {
        // Only process complete UTF-8; keep incomplete trailing bytes.
        let (valid, rest) = split_valid_utf8(&self.frame_buf);
        if valid.is_empty() && !rest.is_empty() {
            return Ok(());
        }
        let text = String::from_utf8_lossy(valid).into_owned();
        self.frame_buf = rest.to_vec();
        self.on_text(TextChannel::PublicResponse, &text).await
    }

    async fn ingest_acp(&mut self) -> Result<(), InterpreterError> {
        if self.frame_buf.len() > self.request.limits.max_frame_bytes {
            return Err(InterpreterError::limit("frame buffer limit exceeded"));
        }
        let values = drain_json_values(&mut self.frame_buf).map_err(|e| {
            // Incomplete JSON is not an error — only true parse failures after complete brace match
            if e.starts_with("json parse") {
                InterpreterError::malformed_frame(e)
            } else {
                InterpreterError::malformed_frame(e)
            }
        })?;
        for value in values {
            if !self.response_started {
                self.response_started = true;
                self.emit_boundary(BoundaryKind::ResponseStarted).await?;
            }
            for frag in AcpDialect::map_message(&value) {
                self.on_fragment(frag).await?;
            }
        }
        Ok(())
    }

    async fn on_fragment(&mut self, frag: AcpFragment) -> Result<(), InterpreterError> {
        match frag {
            AcpFragment::TextDelta { channel, text } => self.on_text(channel, &text).await,
            AcpFragment::Tool { action_id, signal } => self.on_tool(action_id, signal).await,
            AcpFragment::ResponseFinished => {
                self.seal_text_channels().await?;
                self.emit_boundary(BoundaryKind::ResponseFinished).await
            }
            AcpFragment::Diagnostic { message } => {
                self.push_diagnostic(message.clone());
                self.emit_diagnostic(DiagnosticKind::UnsupportedEvent, message)
                    .await
            }
        }
    }

    async fn on_text(&mut self, channel: TextChannel, text: &str) -> Result<(), InterpreterError> {
        let key = (channel,);
        let seg = self.segmenters.entry(key).or_default();
        if seg.buffered_bytes() + text.len() > self.request.limits.max_sentence_assembly_bytes {
            return Err(InterpreterError::new(
                InterpreterErrorKind::SentenceLimitExceeded,
                "sentence assembly limit exceeded",
            ));
        }
        let completed = seg.push(text);
        for sentence in completed {
            self.emit_sentence(channel, sentence).await?;
        }
        Ok(())
    }

    async fn seal_text_channels(&mut self) -> Result<(), InterpreterError> {
        let channels: Vec<TextChannel> = self.segmenters.keys().map(|(c,)| *c).collect();
        for channel in channels {
            let sealed = self
                .segmenters
                .get_mut(&(channel,))
                .map(|s| s.seal_at_clean_end())
                .unwrap_or_default();
            for sentence in sealed {
                self.emit_sentence(channel, sentence).await?;
            }
        }
        Ok(())
    }

    async fn seal_clean(&mut self) -> Result<(), InterpreterError> {
        self.seal_text_channels().await
    }

    async fn quarantine_partials(&mut self) -> Result<(), InterpreterError> {
        // Do not promote incomplete sentences.
        for ((channel,), seg) in self.segmenters.iter_mut() {
            let unresolved = seg.take_unresolved();
            if !unresolved.is_empty() {
                self.unresolved_bytes_at_end += unresolved.len() as u64;
                self.diagnostics.push(format!(
                    "unresolved text on {:?}: {} bytes",
                    channel,
                    unresolved.len()
                ));
            }
        }
        self.unresolved_bytes_at_end += self.frame_buf.len() as u64;
        self.frame_buf.clear();
        // Mark incomplete tools
        let ids: Vec<String> = self.tools.keys().cloned().collect();
        for id in ids {
            if let Some(tool) = self.tools.get_mut(&id) {
                if tool.terminal.is_none() {
                    tool.request_state = match tool.request_state {
                        ToolRequestState::Ready => ToolRequestState::Ready,
                        _ => ToolRequestState::Incomplete,
                    };
                    tool.result_state = match tool.result_state {
                        ToolResultState::Complete => ToolResultState::Complete,
                        _ => ToolResultState::Incomplete,
                    };
                    tool.generation += 1;
                    let snap = self.tool_snapshot_from(&id);
                    if let Some(s) = snap {
                        self.pub_
                            .publish(InterpreterOutputEvent::Unit(CanonicalUnitEvent::Incomplete(
                                s,
                            )))
                            .await?;
                    }
                }
            }
        }
        Ok(())
    }

    async fn on_tool(
        &mut self,
        action_id: ToolActionId,
        signal: ToolSignal,
    ) -> Result<(), InterpreterError> {
        if self.tools.len() >= self.request.limits.max_pending_tool_actions
            && !self.tools.contains_key(action_id.as_str())
        {
            return Err(InterpreterError::new(
                InterpreterErrorKind::ToolLimitExceeded,
                "max pending tool actions",
            ));
        }

        let is_new = !self.tools.contains_key(action_id.as_str());
        if is_new {
            let unit_id = UnitId::new(format!("tool-{}", action_id.as_str()));
            self.tools.insert(
                action_id.as_str().to_string(),
                ToolAssembler {
                    action_id: action_id.clone(),
                    unit_id,
                    generation: 0,
                    tool_name: None,
                    request_state: ToolRequestState::Assembling,
                    execution_state: ToolExecutionState::NotObserved,
                    result_state: ToolResultState::Absent,
                    request_payload: None,
                    result_payload: None,
                    terminal: None,
                    waiting_for: None,
                },
            );
        }

        let event_kind = {
            let tool = self.tools.get_mut(action_id.as_str()).unwrap();
            tool.generation += 1;
            match signal {
                ToolSignal::Waiting {
                    tool_name,
                    waiting_for,
                } => {
                    if tool_name.is_some() {
                        tool.tool_name = tool_name;
                    }
                    tool.waiting_for = Some(waiting_for);
                    tool.request_state = ToolRequestState::Assembling;
                    tool.execution_state = ToolExecutionState::Waiting;
                    if is_new {
                        "created"
                    } else {
                        "advanced"
                    }
                }
                ToolSignal::RequestReady {
                    tool_name,
                    arguments_json,
                } => {
                    if arguments_json.len() > self.request.limits.max_bytes_per_tool_action {
                        return Err(InterpreterError::new(
                            InterpreterErrorKind::ToolLimitExceeded,
                            "tool payload limit",
                        ));
                    }
                    tool.tool_name = Some(tool_name);
                    tool.request_payload = Some(arguments_json);
                    tool.request_state = ToolRequestState::Ready;
                    tool.execution_state = ToolExecutionState::Waiting;
                    tool.waiting_for = Some("external execution".into());
                    tool.result_state = ToolResultState::Absent;
                    if is_new {
                        "created"
                    } else {
                        "advanced"
                    }
                }
                ToolSignal::Resolved {
                    success,
                    result_json,
                } => {
                    tool.execution_state = ToolExecutionState::Terminal;
                    tool.result_state = ToolResultState::Complete;
                    tool.result_payload = result_json;
                    tool.terminal = Some(if success {
                        ToolTerminalOutcome::Success
                    } else {
                        ToolTerminalOutcome::Failure
                    });
                    tool.waiting_for = None;
                    "completed"
                }
            }
        };

        let snap = self.tool_snapshot_from(action_id.as_str()).unwrap();
        let unit_event = match event_kind {
            "created" => CanonicalUnitEvent::Created(snap),
            "completed" => CanonicalUnitEvent::Completed(snap),
            _ => CanonicalUnitEvent::Advanced(snap),
        };
        self.pub_
            .publish(InterpreterOutputEvent::Unit(unit_event))
            .await
    }

    fn tool_snapshot_from(&self, id: &str) -> Option<CanonicalUnitSnapshot> {
        let tool = self.tools.get(id)?;
        let unit = CanonicalUnit::Tool(ToolActionEvent {
            tool_action_id: tool.action_id.clone(),
            tool_name: tool.tool_name.clone(),
            request_state: tool.request_state,
            execution_state: tool.execution_state,
            result_state: tool.result_state,
            // Only expose complete payloads
            request_payload: if tool.request_state == ToolRequestState::Ready {
                tool.request_payload.clone()
            } else {
                None
            },
            result_payload: if tool.result_state == ToolResultState::Complete {
                tool.result_payload.clone()
            } else {
                None
            },
            terminal_outcome: tool.terminal,
            waiting_for: tool.waiting_for.clone(),
        });
        let state = if tool.terminal.is_some() {
            UnitState::Complete
        } else if tool.request_state == ToolRequestState::Incomplete {
            UnitState::Incomplete
        } else {
            UnitState::Waiting
        };
        Some(self.snapshot(
            tool.unit_id.clone(),
            tool.generation,
            state,
            LaneId::tool(),
            unit,
        ))
    }

    async fn emit_sentence(
        &mut self,
        channel: TextChannel,
        content: String,
    ) -> Result<(), InterpreterError> {
        let unit_id = UnitId::new(format!("s-{}", self.next_unit));
        self.next_unit += 1;
        self.sentence_count += 1;
        let ordinal = self.next_lane_ordinal(LaneId::response().as_str());
        let unit = CanonicalUnit::Text(TextSentence {
            sentence_id: unit_id.clone(),
            channel,
            paragraph_id: None,
            sentence_ordinal: ordinal,
            content,
        });
        let lane = match channel {
            TextChannel::PublicReasoningSummary => LaneId::reasoning(),
            _ => LaneId::response(),
        };
        let snap = self.snapshot(unit_id, 1, UnitState::Complete, lane, unit);
        // Created-and-complete: emit Created (complete state)
        self.pub_
            .publish(InterpreterOutputEvent::Unit(CanonicalUnitEvent::Created(
                snap,
            )))
            .await
    }

    async fn emit_boundary(&mut self, kind: BoundaryKind) -> Result<(), InterpreterError> {
        let unit_id = UnitId::new(format!("b-{}", self.next_unit));
        self.next_unit += 1;
        let unit = CanonicalUnit::Boundary(SemanticBoundary { kind });
        let snap = self.snapshot(
            unit_id,
            1,
            UnitState::Complete,
            LaneId::response(),
            unit,
        );
        self.pub_
            .publish(InterpreterOutputEvent::Unit(CanonicalUnitEvent::Created(
                snap,
            )))
            .await
    }

    async fn emit_diagnostic(
        &mut self,
        kind: DiagnosticKind,
        message: String,
    ) -> Result<(), InterpreterError> {
        let unit_id = UnitId::new(format!("d-{}", self.next_unit));
        self.next_unit += 1;
        let unit = CanonicalUnit::Diagnostic(ModelDiagnostic { kind, message });
        let snap = self.snapshot(
            unit_id,
            1,
            UnitState::Complete,
            LaneId::response(),
            unit,
        );
        self.pub_
            .publish(InterpreterOutputEvent::Unit(CanonicalUnitEvent::Created(
                snap,
            )))
            .await
    }

    fn snapshot(
        &self,
        unit_id: UnitId,
        generation: u64,
        state: UnitState,
        lane_id: LaneId,
        unit: CanonicalUnit,
    ) -> CanonicalUnitSnapshot {
        let lane_ordinal = self
            .lane_ordinals
            .get(lane_id.as_str())
            .copied()
            .unwrap_or(0);
        CanonicalUnitSnapshot {
            unit_id,
            unit_generation: generation,
            unit_state: state,
            interpretation_id: self.request.interpretation_id.clone(),
            connection_id: self.request.connection_id.clone(),
            external_session_id: self.request.external_session_id.clone(),
            flow_id: FlowId::main(),
            lane_id,
            lane_ordinal,
            causal_parent_id: None,
            unit,
        }
    }

    fn next_lane_ordinal(&mut self, lane: &str) -> u64 {
        let e = self.lane_ordinals.entry(lane.to_string()).or_insert(0);
        *e += 1;
        *e
    }

    fn push_diagnostic(&mut self, msg: String) {
        if self.diagnostics.len() < self.request.limits.max_safe_diagnostics {
            self.diagnostics.push(msg);
        }
    }

    async fn finish(
        &mut self,
        kind: InterpretationEndKind,
        end_tx: oneshot::Sender<InterpretationEnd>,
        status_terminal: Arc<AtomicBool>,
    ) {
        if self.ended {
            return;
        }
        self.ended = true;
        let mut unresolved = self.unresolved_bytes_at_end;
        for seg in self.segmenters.values() {
            unresolved += seg.buffered_bytes() as u64;
        }
        unresolved += self.frame_buf.len() as u64;

        let end = InterpretationEnd {
            interpretation_id: self.request.interpretation_id.clone(),
            connection_id: self.request.connection_id.clone(),
            external_session_id: self.request.external_session_id.clone(),
            kind,
            canonical_event_count: self.pub_.count(),
            completed_sentence_count: self.sentence_count,
            completed_structure_count: self.structure_count,
            unresolved_text_bytes: unresolved,
            source_bytes_consumed: self.source_bytes,
            safe_diagnostics: self.diagnostics.clone(),
        };
        let _ = self
            .pub_
            .publish(InterpreterOutputEvent::Ended(end.clone()))
            .await;
        status_terminal.store(true, Ordering::SeqCst);
        let _ = end_tx.send(end);
    }
}

fn split_valid_utf8(buf: &[u8]) -> (&[u8], &[u8]) {
    match std::str::from_utf8(buf) {
        Ok(_) => (buf, &[]),
        Err(e) => {
            let valid_up_to = e.valid_up_to();
            (&buf[..valid_up_to], &buf[valid_up_to..])
        }
    }
}

// silence unused import warning for UsageObservation if not used yet
#[allow(dead_code)]
fn _u() -> Option<UsageObservation> {
    None
}
