//! Provider-neutral canonical semantic units and lifecycle events.
//!
//! See `doc/INTERPRETER.md`. No provider-native DTOs.

use crate::id::{ConnectionId, ExternalSessionId};
use serde::{Deserialize, Serialize};

/// Interpretation instance identity.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct InterpretationId(String);

impl InterpretationId {
    /// Create from an explicit value.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Allocate a random id.
    pub fn generate() -> Self {
        Self(uuid::Uuid::new_v4().to_string())
    }

    /// Borrow the underlying string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Stable unit identity within one interpretation.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct UnitId(String);

impl UnitId {
    /// Create from an explicit value.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Borrow the underlying string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Flow identity (one logical dialect exchange/response).
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FlowId(String);

impl FlowId {
    /// Create from an explicit value.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Default main flow.
    pub fn main() -> Self {
        Self("main".into())
    }

    /// Borrow the underlying string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Lane identity within a flow.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct LaneId(String);

impl LaneId {
    /// Create from an explicit value.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Response text lane.
    pub fn response() -> Self {
        Self("response".into())
    }

    /// Tool lane.
    pub fn tool() -> Self {
        Self("tool".into())
    }

    /// Reasoning summary lane.
    pub fn reasoning() -> Self {
        Self("reasoning".into())
    }

    /// Borrow the underlying string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Tool action identity (dialect-provided or interpretation-scoped).
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ToolActionId(String);

impl ToolActionId {
    /// Create from an explicit value.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Borrow the underlying string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Canonical text channel.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TextChannel {
    /// Public assistant response.
    PublicResponse,
    /// Publishable reasoning summary only (never private CoT).
    PublicReasoningSummary,
    /// Status / narration.
    StatusNarration,
    /// Quoted external content (untrusted).
    QuotedExternalContent,
}

impl TextChannel {
    /// Short label for console rendering.
    pub fn label(self) -> &'static str {
        match self {
            Self::PublicResponse => "assistant",
            Self::PublicReasoningSummary => "reasoning",
            Self::StatusNarration => "status",
            Self::QuotedExternalContent => "quoted",
        }
    }
}

/// Lifecycle state of a canonical unit.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum UnitState {
    /// Fully assembled and immutable (typical for sentences).
    Complete,
    /// Lifecycle-bearing unit awaiting more correlated material.
    Waiting,
    /// Explicitly incomplete (malformed or terminated mid-assembly).
    Incomplete,
    /// Malformed input sealed as such.
    Malformed,
}

/// Closed top-level canonical unit vocabulary.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CanonicalUnit {
    /// Complete sentence atom.
    Text(TextSentence),
    /// Complete structural atom (heading, code block, …).
    Structure(StructuralAtom),
    /// Paragraph open/close.
    Paragraph(ParagraphBoundary),
    /// Tool action lifecycle.
    Tool(ToolActionEvent),
    /// Usage observation.
    Usage(UsageObservation),
    /// Model/dialect diagnostic.
    Diagnostic(ModelDiagnostic),
    /// Semantic boundary observation (not turn completion).
    Boundary(SemanticBoundary),
}

impl CanonicalUnit {
    /// Short kind label.
    pub fn kind_label(&self) -> &'static str {
        match self {
            Self::Text(_) => "text",
            Self::Structure(_) => "structure",
            Self::Paragraph(_) => "paragraph",
            Self::Tool(_) => "tool",
            Self::Usage(_) => "usage",
            Self::Diagnostic(_) => "diagnostic",
            Self::Boundary(_) => "boundary",
        }
    }
}

/// Complete sentence atom (immutable after emission).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TextSentence {
    /// Sentence identity.
    pub sentence_id: UnitId,
    /// Canonical channel.
    pub channel: TextChannel,
    /// Optional paragraph membership.
    pub paragraph_id: Option<UnitId>,
    /// Ordinal within the lane/paragraph.
    pub sentence_ordinal: u64,
    /// Complete sentence content.
    pub content: String,
}

/// Non-sentence structural atom.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StructuralAtom {
    /// Structure identity.
    pub structure_id: UnitId,
    /// Kind of structure.
    pub kind: StructureKind,
    /// Complete textual payload where applicable.
    pub content: String,
}

/// Structural kinds recognized for canonical assembly.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum StructureKind {
    /// Heading.
    Heading,
    /// List item boundary.
    ListItem,
    /// Fenced code block.
    CodeBlock,
    /// Table row.
    TableRow,
    /// Block quote boundary.
    BlockQuote,
    /// Thematic break.
    ThematicBreak,
    /// Declared raw block.
    RawBlock,
}

/// Paragraph open/close.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParagraphBoundary {
    /// Paragraph identity.
    pub paragraph_id: UnitId,
    /// Opened or closed.
    pub kind: ParagraphKind,
    /// Channel.
    pub channel: TextChannel,
}

/// Paragraph boundary kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ParagraphKind {
    /// Paragraph opened.
    Opened,
    /// Paragraph closed.
    Closed,
}

/// Tool-action event payload.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolActionEvent {
    /// Stable tool action id.
    pub tool_action_id: ToolActionId,
    /// Tool name when known (complete only for ready/resolved).
    pub tool_name: Option<String>,
    /// Request state.
    pub request_state: ToolRequestState,
    /// Execution state.
    pub execution_state: ToolExecutionState,
    /// Result state.
    pub result_state: ToolResultState,
    /// Complete request payload JSON when ready (not partial fragments).
    pub request_payload: Option<String>,
    /// Complete result payload when resolved.
    pub result_payload: Option<String>,
    /// Terminal outcome when known.
    pub terminal_outcome: Option<ToolTerminalOutcome>,
    /// What the action is waiting for (when waiting).
    pub waiting_for: Option<String>,
}

/// Tool request assembly state.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ToolRequestState {
    /// Still assembling.
    Assembling,
    /// Complete and syntactically valid.
    Ready,
    /// Malformed.
    Malformed,
    /// Incomplete at termination.
    Incomplete,
}

/// Observed execution state from dialect (not host Loop execution).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ToolExecutionState {
    /// Not observed in dialect stream.
    NotObserved,
    /// Waiting for execution/result.
    Waiting,
    /// Running (if dialect reports it).
    Running,
    /// Terminal observed.
    Terminal,
}

/// Result assembly state.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ToolResultState {
    /// No result yet.
    Absent,
    /// Assembling.
    Assembling,
    /// Complete.
    Complete,
    /// Malformed.
    Malformed,
    /// Incomplete.
    Incomplete,
}

/// Terminal tool outcome as observed in the dialect (not host tool runtime).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ToolTerminalOutcome {
    /// Success.
    Success,
    /// Failure.
    Failure,
    /// Cancelled.
    Cancelled,
    /// Lost / unknown.
    Lost,
}

/// Usage observation (unavailable is not zero).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct UsageObservation {
    /// Input tokens.
    pub input_tokens: TokenCount,
    /// Output tokens.
    pub output_tokens: TokenCount,
}

/// Measured or unavailable count.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TokenCount {
    /// Measured value.
    Measured(u64),
    /// Not supplied by dialect.
    Unavailable,
}

/// Safe model/dialect diagnostic.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelDiagnostic {
    /// Classification.
    pub kind: DiagnosticKind,
    /// Bounded safe message (no secrets/raw bodies).
    pub message: String,
}

/// Diagnostic kinds.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DiagnosticKind {
    /// Dialect warning.
    DialectWarning,
    /// Model-reported error (normalized).
    ModelReportedError,
    /// Unsupported event.
    UnsupportedEvent,
    /// Malformed frame.
    MalformedFrame,
    /// Malformed semantic payload.
    MalformedSemanticPayload,
    /// Incomplete text at termination.
    IncompleteText,
    /// Incomplete structure.
    IncompleteStructure,
    /// Incomplete tool.
    IncompleteToolAction,
    /// Limit exceeded.
    LimitExceeded,
}

/// Semantic boundary (not turn/task completion authority).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SemanticBoundary {
    /// Boundary kind.
    pub kind: BoundaryKind,
}

/// Boundary kinds.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BoundaryKind {
    /// Response started.
    ResponseStarted,
    /// Channel started.
    ChannelStarted,
    /// Channel finished.
    ChannelFinished,
    /// Response finished (dialect-level).
    ResponseFinished,
    /// Usage finalized.
    UsageFinalized,
}

/// Dialect-observed source time for a complete (or lifecycle) unit.
///
/// Observational only: does **not** establish causality, turn success, or
/// authority. Lane ordinal / explicit causal parent remain primary. Values are
/// provider clock milliseconds when the dialect supplies them (e.g. Grok ACP
/// `params._meta.agentTimestampMs`); absent when the dialect does not.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceTimeObservation {
    /// Earliest dialect-reported source timestamp (ms) among contributing fragments.
    pub first_ms: u64,
    /// Latest dialect-reported source timestamp (ms) among contributing fragments.
    pub last_ms: u64,
}

impl SourceTimeObservation {
    /// Build from a single observed timestamp.
    pub fn point(ms: u64) -> Self {
        Self {
            first_ms: ms,
            last_ms: ms,
        }
    }

    /// Merge two observations (min first, max last).
    pub fn merge(self, other: Self) -> Self {
        Self {
            first_ms: self.first_ms.min(other.first_ms),
            last_ms: self.last_ms.max(other.last_ms),
        }
    }

    /// Extend with an optional single timestamp.
    pub fn include(self, ms: Option<u64>) -> Self {
        match ms {
            Some(t) => self.merge(Self::point(t)),
            None => self,
        }
    }

    /// From optional first/last (None if neither known).
    pub fn from_bounds(first: Option<u64>, last: Option<u64>) -> Option<Self> {
        match (first, last) {
            (Some(f), Some(l)) => Some(Self {
                first_ms: f.min(l),
                last_ms: f.max(l),
            }),
            (Some(t), None) | (None, Some(t)) => Some(Self::point(t)),
            (None, None) => None,
        }
    }
}

/// Correlation + lifecycle envelope for one unit generation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanonicalUnitSnapshot {
    /// Unit identity (stable across generations).
    pub unit_id: UnitId,
    /// Monotonic generation (starts at 1).
    pub unit_generation: u64,
    /// Lifecycle state.
    pub unit_state: UnitState,
    /// Interpretation identity.
    pub interpretation_id: InterpretationId,
    /// Connection identity.
    pub connection_id: ConnectionId,
    /// External session when present (e.g. Grok sessionId).
    pub external_session_id: Option<ExternalSessionId>,
    /// Flow.
    pub flow_id: FlowId,
    /// Lane.
    pub lane_id: LaneId,
    /// Strict ordinal within the lane.
    pub lane_ordinal: u64,
    /// Optional causal parent unit.
    pub causal_parent_id: Option<UnitId>,
    /// Optional dialect source time (observational; not causality).
    pub source_time: Option<SourceTimeObservation>,
    /// Optional dialect stream step / sequence id (observational; not causality).
    ///
    /// Examples: Antigravity ACP `update._meta.stepIdx`, numeric `messageId`.
    /// Used by human projection when wall-clock source times are absent.
    pub source_step: Option<u64>,
    /// Canonical unit content allowed for this state.
    pub unit: CanonicalUnit,
}

/// Unit lifecycle event (closed vocabulary).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CanonicalUnitEvent {
    /// Unit created (often already complete for sentences).
    Created(CanonicalUnitSnapshot),
    /// Lifecycle-bearing unit advanced.
    Advanced(CanonicalUnitSnapshot),
    /// Unit completed.
    Completed(CanonicalUnitSnapshot),
    /// Unit incomplete at termination or failure.
    Incomplete(CanonicalUnitSnapshot),
}

impl CanonicalUnitEvent {
    /// Borrow the snapshot.
    pub fn snapshot(&self) -> &CanonicalUnitSnapshot {
        match self {
            Self::Created(s) | Self::Advanced(s) | Self::Completed(s) | Self::Incomplete(s) => s,
        }
    }

    /// Lifecycle label for console.
    pub fn lifecycle_label(&self) -> &'static str {
        match self {
            Self::Created(_) => "created",
            Self::Advanced(_) => "advanced",
            Self::Completed(_) => "completed",
            Self::Incomplete(_) => "incomplete",
        }
    }
}

/// Exactly one terminal report per interpretation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InterpretationEnd {
    /// Interpretation identity.
    pub interpretation_id: InterpretationId,
    /// Connection identity.
    pub connection_id: ConnectionId,
    /// External session when present.
    pub external_session_id: Option<ExternalSessionId>,
    /// Terminal kind.
    pub kind: InterpretationEndKind,
    /// Canonical events published.
    pub canonical_event_count: u64,
    /// Completed sentences.
    pub completed_sentence_count: u64,
    /// Completed structures.
    pub completed_structure_count: u64,
    /// Unresolved text bytes at end.
    pub unresolved_text_bytes: u64,
    /// Source bytes consumed.
    pub source_bytes_consumed: u64,
    /// Bounded safe diagnostics.
    pub safe_diagnostics: Vec<String>,
}

/// Interpretation terminal kinds.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum InterpretationEndKind {
    /// Clean complete.
    Complete,
    /// Cancelled.
    Cancelled,
    /// Terminated.
    Terminated,
    /// Transport failed.
    TransportFailed,
    /// Dialect failed.
    DialectFailed,
    /// Limit exceeded.
    LimitExceeded,
    /// Invariant failed.
    InvariantFailed,
}

/// Stream events delivered to subscribers (Interpreter output + end).
///
/// `Unit` is boxed so the enum stays small (strict Clippy large-variant rule).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum InterpreterOutputEvent {
    /// Canonical unit lifecycle.
    Unit(Box<CanonicalUnitEvent>),
    /// Interpretation ended.
    Ended(InterpretationEnd),
}

impl InterpreterOutputEvent {
    /// Wrap a unit lifecycle event.
    pub fn unit(event: CanonicalUnitEvent) -> Self {
        Self::Unit(Box::new(event))
    }
}
