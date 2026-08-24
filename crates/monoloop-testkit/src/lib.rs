//! SPDX-License-Identifier: AGPL-3.0-or-later
//! Copyright (C) Alexander R. Croft
//!
//! Monoloop test kit — **not a product component**.
//!
//! Console renderer, event distributor, and Driver composition helpers.
//! Product crates must not depend on this package.

#![deny(missing_docs)]
// Test-kit helpers prioritize clarity over every Clippy style preference.
#![allow(
    clippy::while_let_loop,
    clippy::too_many_arguments,
    clippy::field_reassign_with_default,
    clippy::single_char_add_str,
    dead_code
)]

mod chat_projector;
mod console;
mod distribute;
mod driver;
mod grok_serve;
mod html_report;
mod live_agy;
mod live_claude;
mod live_codex;
mod live_cursor;
mod live_grok;
mod live_zai;
mod pipeline;

pub use chat_projector::{
    project_chat, project_chat_with, ChatLine, ChatProjection, ChatRole, ProjectChatOptions,
    ProjectedTool, ProjectionConfidence, ProjectionStrategy,
};
pub use console::{
    ConsoleRenderRecord, ConsoleRenderer, ConsoleRendererConfig, ConsoleSink, StdoutSink,
    SyncMemorySink,
};
pub use distribute::{pump_interpreter_to_distributor, EventDistributor, SubscriberPolicy};
pub use driver::{
    run_bytes_pipeline, run_bytes_pipeline_with_params, DriverRunReport, PipelineParams,
    PipelineRawDump, RawInputFrame,
};
pub use grok_serve::{GrokServeOptions, ManagedGrokServe};
pub use html_report::{
    build_html_report, join_sentences_as_markdown, markdown_to_html, write_html_report, HtmlReport,
    HtmlReportParams,
};
pub use live_agy::{
    run_live_agy_prompt, LiveAgyArtifactPaths, LiveAgyRunOptions, LiveAgyRunReport,
};
pub use live_claude::{
    run_live_claude_prompt, LiveClaudeArtifactPaths, LiveClaudeRunOptions, LiveClaudeRunReport,
};
pub use live_codex::{
    run_live_codex_prompt, LiveCodexArtifactPaths, LiveCodexRunOptions, LiveCodexRunReport,
};
pub use live_cursor::{
    run_live_cursor_prompt, LiveCursorArtifactPaths, LiveCursorRunOptions, LiveCursorRunReport,
};
pub use live_grok::{
    run_live_grok_multi_session, run_live_grok_prompt, LiveGrokArtifactPaths,
    LiveGrokMultiSessionOptions, LiveGrokMultiSessionReport, LiveGrokRunOptions, LiveGrokRunReport,
    LiveGrokSessionOutcome,
};
pub use live_zai::{
    run_live_zai_prompt, LiveZaiArtifactPaths, LiveZaiRunOptions, LiveZaiRunReport,
};
pub use pipeline::{
    acp_binding, agy_acp_binding, claude_code_binding, codex_acp_binding, collect_interpretation,
    cursor_acp_binding, feed_chunks, interpret_and_render, interpret_bytes, render_all,
    test_text_binding, zai_cli_binding,
};
