//! Shared contracts for Monoloop product components.
//!
//! Product components share identities, dialect descriptors, canonical events,
//! and closed error families. This crate intentionally contains no transport,
//! dialect decoder, tool execution, UI, or host-agent logic.

#![deny(missing_docs)]

mod canonical;
mod dialect;
mod error;
mod id;
mod limits;
mod loop_types;

pub use canonical::{
    BoundaryKind, CanonicalUnit, CanonicalUnitEvent, CanonicalUnitSnapshot, DiagnosticKind,
    FlowId, InterpretationEnd, InterpretationEndKind, InterpretationId, InterpreterOutputEvent,
    LaneId, ModelDiagnostic, ParagraphBoundary, ParagraphKind, SemanticBoundary, StructuralAtom,
    StructureKind, TextChannel, TextSentence, TokenCount, ToolActionEvent, ToolActionId,
    ToolExecutionState, ToolRequestState, ToolResultState, ToolTerminalOutcome, UnitId, UnitState,
    UsageObservation,
};
pub use dialect::{DialectBinding, DialectDescriptor, DialectFamily, DialectNegotiation};
pub use error::{
    ConnectorError, ConnectorErrorKind, InterpreterError, InterpreterErrorKind,
};
pub use id::{
    ConnectionId, ExternalSessionId, GrokSessionId, MonoloopRunId, RequestId,
};
pub use limits::{ConnectorLimits, InterpretationLimits, TransportBufferLimits};
pub use loop_types::{
    LoopEnd, LoopEndKind, LoopError, LoopErrorKind, LoopId, LoopLimits, LoopOutputEvent, LoopScope,
    OutboundToolOutcome, OutboundToolResult, ToolExecutionId, ToolUnavailableReason,
};

pub use bytes::Bytes;
