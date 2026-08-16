//! Helpers to feed interpreters and collect/render events.

use monoloop_contracts::{
    ConnectionId, DialectBinding, DialectDescriptor, ExternalSessionId, InterpretationId,
    InterpretationLimits, InterpreterOutputEvent,
};
use monoloop_interpreter::{
    DefaultInterpreterFactory, InterpreterFactory, StartInterpretation,
};
use std::sync::Arc;

use crate::console::{ConsoleRenderer, ConsoleRendererConfig, ConsoleSink, SyncMemorySink};

/// Cursor ACP dialect binding (stdio NDJSON profile).
pub fn cursor_acp_binding() -> DialectBinding {
    DialectBinding::negotiated(DialectDescriptor::cursor_acp("1"))
}

/// Interpret complete (or pre-concatenated) raw bytes under a dialect binding.
pub async fn interpret_bytes(
    dialect: DialectBinding,
    bytes: &[u8],
    external_session_id: Option<ExternalSessionId>,
) -> Vec<InterpreterOutputEvent> {
    feed_chunks(dialect, &[bytes::Bytes::copy_from_slice(bytes)], external_session_id).await
}

/// Feed arbitrary fragmentation of the same logical stream.
pub async fn feed_chunks(
    dialect: DialectBinding,
    chunks: &[bytes::Bytes],
    external_session_id: Option<ExternalSessionId>,
) -> Vec<InterpreterOutputEvent> {
    let factory = DefaultInterpreterFactory::new();
    let interp = factory
        .start(StartInterpretation {
            interpretation_id: InterpretationId::generate(),
            connection_id: ConnectionId::new("test-conn"),
            external_session_id,
            dialect,
            limits: InterpretationLimits::default(),
        })
        .expect("start");

    for chunk in chunks {
        interp
            .input
            .push_bytes(chunk.clone())
            .await
            .expect("push");
    }
    interp.input.finish_clean().await.expect("finish");
    collect_interpretation(&interp).await
}

/// Drain events until Ended.
pub async fn collect_interpretation(
    interp: &monoloop_interpreter::Interpretation,
) -> Vec<InterpreterOutputEvent> {
    let mut out = Vec::new();
    loop {
        match interp.events.recv().await {
            Some(ev) => {
                let done = matches!(ev, InterpreterOutputEvent::Ended(_));
                out.push(ev);
                if done {
                    break;
                }
            }
            None => break,
        }
    }
    out
}

/// Run interpretation and render to a memory sink; return (events, console text).
pub async fn interpret_and_render(
    dialect: DialectBinding,
    chunks: &[bytes::Bytes],
) -> (Vec<InterpreterOutputEvent>, String) {
    let events = feed_chunks(dialect, chunks, None).await;
    let sink = Arc::new(SyncMemorySink::new());
    let renderer = ConsoleRenderer::new(ConsoleRendererConfig::default(), sink.clone());
    for ev in &events {
        renderer.render(ev);
    }
    (events, sink.join())
}

/// ACP dialect binding helper.
pub fn acp_binding() -> DialectBinding {
    DialectBinding::negotiated(DialectDescriptor::acp_json_rpc("1"))
}

/// Test raw text dialect binding.
pub fn test_text_binding() -> DialectBinding {
    DialectBinding::fixed(DialectDescriptor::test_raw())
}

/// Render events with a custom sink.
pub fn render_all(events: &[InterpreterOutputEvent], sink: Arc<dyn ConsoleSink>) {
    let renderer = ConsoleRenderer::new(ConsoleRendererConfig::default(), sink);
    for ev in events {
        renderer.render(ev);
    }
}
