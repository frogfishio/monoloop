//! Minimal test Driver composition (not a product component).
//!
//! Wires Interpreter → event distributor → independent Console + Loop
//! subscriptions with EmptyToolRegistry.

use crate::console::{ConsoleRenderer, ConsoleRendererConfig, ConsoleSink, SyncMemorySink};
use crate::distribute::{pump_interpreter_to_distributor, EventDistributor, SubscriberPolicy};
use crate::html_report::{build_html_report, write_html_report, HtmlReport, HtmlReportParams};
use monoloop_contracts::{
    DialectBinding, InterpretationId, InterpretationLimits, InterpreterOutputEvent, LoopEnd,
    LoopId, LoopLimits, LoopOutputEvent, LoopScope, MonoloopRunId, OutboundToolOutcome,
};
use monoloop_interpreter::{
    ConnectionId, DefaultInterpreterFactory, InterpreterFactory, StartInterpretation,
};
use monoloop_loop::{DefaultLoopRuntime, LoopHandle};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Optional parameters for a pipeline run (mirrors a `--params` style switch).
#[derive(Clone, Debug)]
pub struct PipelineParams {
    /// Render append-only console output.
    pub render_console: bool,
    /// Capture exact inbound raw chunks fed to the Interpreter (as a wire dump).
    pub dump_raw: bool,
    /// When set, write a self-contained HTML review of canonical events to this path.
    pub html_dump_path: Option<PathBuf>,
    /// HTML report options (used when `html_dump_path` is set or HTML is requested).
    pub html_params: HtmlReportParams,
    /// When true, always build an in-memory HTML report (even without a path).
    pub build_html: bool,
}

impl Default for PipelineParams {
    fn default() -> Self {
        Self {
            render_console: true,
            dump_raw: false,
            html_dump_path: None,
            html_params: HtmlReportParams::default(),
            build_html: false,
        }
    }
}

impl PipelineParams {
    /// Console only (legacy default behaviour).
    pub fn console_only() -> Self {
        Self {
            render_console: true,
            dump_raw: false,
            html_dump_path: None,
            html_params: HtmlReportParams::default(),
            build_html: false,
        }
    }

    /// Console + raw dump of exact dialect bytes.
    pub fn with_raw_dump() -> Self {
        Self {
            render_console: true,
            dump_raw: true,
            html_dump_path: None,
            html_params: HtmlReportParams::default(),
            build_html: false,
        }
    }

    /// Console + HTML file dump for visual interpretation review.
    pub fn with_html_dump(path: impl Into<PathBuf>) -> Self {
        Self {
            render_console: true,
            dump_raw: false,
            html_dump_path: Some(path.into()),
            html_params: HtmlReportParams::default(),
            build_html: true,
        }
    }

    /// Console + raw dump + HTML file.
    pub fn with_raw_and_html(path: impl Into<PathBuf>) -> Self {
        Self {
            render_console: true,
            dump_raw: true,
            html_dump_path: Some(path.into()),
            html_params: HtmlReportParams::default(),
            build_html: true,
        }
    }

    /// Quiet: no console, no dump.
    pub fn quiet() -> Self {
        Self {
            render_console: false,
            dump_raw: false,
            html_dump_path: None,
            html_params: HtmlReportParams::default(),
            build_html: false,
        }
    }
}

/// One raw frame as presented to the Interpreter (exact chunk bytes).
#[derive(Clone, Debug)]
pub struct RawInputFrame {
    /// Index in feed order.
    pub index: u64,
    /// Exact bytes of this chunk.
    pub bytes: bytes::Bytes,
}

/// Snapshot of raw input for a pipeline run.
#[derive(Clone, Debug, Default)]
pub struct PipelineRawDump {
    /// Chunks in the order they were pushed.
    pub frames: Vec<RawInputFrame>,
}

impl PipelineRawDump {
    /// Human-readable dump of exact chunks.
    pub fn format_text(&self) -> String {
        let mut s = String::new();
        s.push_str(&format!(
            "=== PIPELINE RAW DUMP (frames={}) ===\n",
            self.frames.len()
        ));
        for f in &self.frames {
            s.push_str(&format!(
                "--- chunk #{} len={} ---\n",
                f.index,
                f.bytes.len()
            ));
            if let Ok(v) = serde_json::from_slice::<serde_json::Value>(&f.bytes) {
                if let Ok(pretty) = serde_json::to_string_pretty(&v) {
                    s.push_str(&pretty);
                    s.push('\n');
                    continue;
                }
            }
            // May be a partial JSON chunk — print lossy exact bytes.
            s.push_str(&String::from_utf8_lossy(&f.bytes));
            if !s.ends_with('\n') {
                s.push('\n');
            }
        }
        s.push_str("=== END PIPELINE RAW DUMP ===\n");
        s
    }

    /// Concatenate all chunk bytes in order (exact stream).
    pub fn concat(&self) -> bytes::Bytes {
        let mut out = Vec::new();
        for f in &self.frames {
            out.extend_from_slice(&f.bytes);
        }
        bytes::Bytes::from(out)
    }

    /// True if any chunk contains the needle.
    pub fn contains_str(&self, needle: &str) -> bool {
        self.frames
            .iter()
            .any(|f| String::from_utf8_lossy(&f.bytes).contains(needle))
    }
}

/// Result of a test driver run over raw dialect bytes.
#[derive(Debug)]
pub struct DriverRunReport {
    /// Owning run id.
    pub run_id: MonoloopRunId,
    /// Interpreter events collected (from a third lossless tap).
    pub interpreter_events: Vec<InterpreterOutputEvent>,
    /// Loop output events.
    pub loop_events: Vec<LoopOutputEvent>,
    /// Loop terminal.
    pub loop_end: LoopEnd,
    /// Console text (if rendered).
    pub console_text: String,
    /// Count of tool_unavailable outcomes.
    pub tools_unavailable: u64,
    /// Exact input chunks when `PipelineParams.dump_raw` is set.
    pub raw_dump: Option<PipelineRawDump>,
    /// HTML review built from canonical events (when requested).
    pub html_report: Option<HtmlReport>,
    /// Path written when `html_dump_path` was set.
    pub html_dump_path: Option<PathBuf>,
}

/// Run Interpreter + Console + Empty Loop over pre-chunked raw bytes.
pub async fn run_bytes_pipeline(
    dialect: DialectBinding,
    chunks: &[bytes::Bytes],
    render_console: bool,
) -> DriverRunReport {
    run_bytes_pipeline_with_params(
        dialect,
        chunks,
        PipelineParams {
            render_console,
            dump_raw: false,
            html_dump_path: None,
            html_params: HtmlReportParams::default(),
            build_html: false,
        },
    )
    .await
}

/// Same as [`run_bytes_pipeline`] with full params (console + optional raw dump).
pub async fn run_bytes_pipeline_with_params(
    dialect: DialectBinding,
    chunks: &[bytes::Bytes],
    params: PipelineParams,
) -> DriverRunReport {
    let run_id = MonoloopRunId::generate();
    let interpretation_id = InterpretationId::generate();
    let connection_id = ConnectionId::new("driver-conn");
    let loop_id = LoopId::generate();

    let raw_dump = if params.dump_raw {
        Some(PipelineRawDump {
            frames: chunks
                .iter()
                .enumerate()
                .map(|(i, b)| RawInputFrame {
                    index: i as u64,
                    bytes: b.clone(),
                })
                .collect(),
        })
    } else {
        None
    };

    let factory = DefaultInterpreterFactory::new();
    let interp = factory
        .start(StartInterpretation {
            interpretation_id: interpretation_id.clone(),
            connection_id: connection_id.clone(),
            external_session_id: None,
            dialect,
            limits: InterpretationLimits::default(),
        })
        .expect("start interpretation");

    let mut dist = EventDistributor::new();
    // Independent subscriptions — never one shared receiver.
    let loop_sub = dist.subscribe("loop", SubscriberPolicy::Lossless, 1024);
    let console_sub = dist.subscribe("console", SubscriberPolicy::BestEffort, 1024);
    let tap_sub = dist.subscribe("tap", SubscriberPolicy::Lossless, 4096);

    let loop_rt = DefaultLoopRuntime::new();
    let scope = LoopScope::single(
        run_id.clone(),
        loop_id.clone(),
        interpretation_id,
        connection_id,
        None,
    );
    let loop_handle = loop_rt
        .start_empty(
            run_id.clone(),
            loop_id,
            scope,
            loop_sub,
            LoopLimits::default(),
        )
        .expect("start loop");

    let sink = Arc::new(SyncMemorySink::new());
    let console_task = if params.render_console {
        let renderer = Arc::new(ConsoleRenderer::new(
            ConsoleRendererConfig::default(),
            sink.clone() as Arc<dyn ConsoleSink>,
        ));
        Some(tokio::spawn(async move {
            let mut sub = console_sub;
            while let Some(msg) = sub.recv().await {
                if let Ok(delivered) = msg {
                    renderer.render(&delivered.event);
                    if matches!(delivered.event, InterpreterOutputEvent::Ended(_)) {
                        break;
                    }
                }
            }
        }))
    } else {
        drop(console_sub);
        None
    };

    let tap_events = Arc::new(Mutex::new(Vec::new()));
    let tap_events2 = Arc::clone(&tap_events);
    let tap_task = tokio::spawn(async move {
        let mut sub = tap_sub;
        let mut out = Vec::new();
        while let Some(msg) = sub.recv().await {
            if let Ok(delivered) = msg {
                let done = matches!(delivered.event, InterpreterOutputEvent::Ended(_));
                out.push(delivered.event);
                if done {
                    break;
                }
            }
        }
        *tap_events2.lock().await = out;
    });

    let loop_collect = tokio::spawn(collect_loop_output(loop_handle));

    let pump = {
        let events = Arc::clone(&interp.events);
        tokio::spawn(async move {
            pump_interpreter_to_distributor(events, dist).await;
        })
    };

    for chunk in chunks {
        interp
            .input
            .push_bytes(chunk.clone())
            .await
            .expect("push");
    }
    interp.input.finish_clean().await.expect("finish");

    let _ = pump.await;
    let (loop_events, loop_end) = loop_collect.await.expect("loop join");
    let _ = tap_task.await;
    if let Some(t) = console_task {
        let _ = t.await;
    }

    let interpreter_events = tap_events.lock().await.clone();
    let tools_unavailable = loop_events
        .iter()
        .filter(|e| {
            matches!(
                e,
                LoopOutputEvent::OutboundToolResult(r)
                    if r.outcome == OutboundToolOutcome::ToolUnavailable
            )
        })
        .count() as u64;

    let want_html = params.build_html || params.html_dump_path.is_some();
    let (html_report, html_dump_path) = if want_html {
        let report = build_html_report(&interpreter_events, &params.html_params);
        let written = if let Some(ref path) = params.html_dump_path {
            write_html_report(path, &report).expect("write html dump");
            Some(path.clone())
        } else {
            None
        };
        (Some(report), written)
    } else {
        (None, None)
    };

    DriverRunReport {
        run_id,
        interpreter_events,
        loop_events,
        loop_end,
        console_text: sink.join(),
        tools_unavailable,
        raw_dump,
        html_report,
        html_dump_path,
    }
}

async fn collect_loop_output(handle: LoopHandle) -> (Vec<LoopOutputEvent>, LoopEnd) {
    let mut out = Vec::new();
    {
        let mut rx = handle.output.lock().await;
        while let Some(ev) = rx.recv().await {
            let done = matches!(ev, LoopOutputEvent::LoopEnded(_));
            out.push(ev);
            if done {
                break;
            }
        }
    }
    let from_stream = out.iter().rev().find_map(|e| match e {
        LoopOutputEvent::LoopEnded(le) => Some(le.clone()),
        _ => None,
    });
    let loop_end = match from_stream {
        Some(e) => e,
        None => handle.completion.wait().await,
    };
    (out, loop_end)
}
