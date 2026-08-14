//! Monoloop test kit — **not a product component**.
//!
//! Console renderer, event distributor, and Driver composition helpers.
//! Product crates must not depend on this package.

#![deny(missing_docs)]

mod console;
mod distribute;
mod driver;
mod html_report;
mod pipeline;

pub use console::{
    ConsoleRenderRecord, ConsoleRenderer, ConsoleRendererConfig, ConsoleSink, StdoutSink,
    SyncMemorySink,
};
pub use distribute::{
    pump_interpreter_to_distributor, EventDistributor, SubscriberPolicy,
};
pub use driver::{
    run_bytes_pipeline, run_bytes_pipeline_with_params, DriverRunReport, PipelineParams,
    PipelineRawDump, RawInputFrame,
};
pub use html_report::{
    build_html_report, join_sentences_as_markdown, markdown_to_html, write_html_report, HtmlReport,
    HtmlReportParams,
};
pub use pipeline::{
    acp_binding, collect_interpretation, feed_chunks, interpret_and_render, interpret_bytes,
    render_all, test_text_binding,
};
