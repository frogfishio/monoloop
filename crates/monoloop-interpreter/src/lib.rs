//! SPDX-License-Identifier: AGPL-3.0-or-later
//! Copyright (C) Alexander R. Croft
//!
//! Component 02 — Interpreter.
//!
//! Consumes ordered raw Connector bytes + dialect binding, assembles complete
//! provider-neutral canonical units, and publishes them immediately. Never emits
//! tokens, text deltas, or partial tool JSON as canonical content.
//!
//! See `doc/INTERPRETER.md`.

#![deny(missing_docs)]

mod acp;
mod claude_stream;
mod engine;
mod factory;
mod openai_chat;
mod sentence;
mod stream;
mod zai_chat;

pub use acp::AcpDialect;
pub use engine::{Interpretation, InterpretationInput, StartInterpretation};
pub use factory::{DefaultInterpreterFactory, InterpreterFactory, SupportLevel};
pub use openai_chat::{OpenAiSseState, DEFAULT_CHOICE_INDEX};
pub use sentence::{CompletedSentence, SentenceSegmenter, SENTENCE_SEGMENTER_VERSION};
pub use stream::CanonicalEventStream;

pub use monoloop_contracts::{
    BoundaryKind, Bytes, CanonicalUnit, CanonicalUnitEvent, CanonicalUnitSnapshot, ConnectionId,
    DialectBinding, DialectDescriptor, DialectFamily, ExternalSessionId, FlowId, InterpretationEnd,
    InterpretationEndKind, InterpretationId, InterpretationLimits, InterpreterError,
    InterpreterErrorKind, InterpreterOutputEvent, LaneId, SourceTimeObservation, TextChannel,
    TextSentence, ToolActionId, UnitId, UnitState,
};
