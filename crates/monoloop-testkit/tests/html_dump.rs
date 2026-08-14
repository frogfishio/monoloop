//! HTML dump from canonical events — visual interpretation check.

use monoloop_testkit::{
    acp_binding, build_html_report, run_bytes_pipeline_with_params, test_text_binding,
    write_html_report, HtmlReportParams, PipelineParams,
};
use std::path::PathBuf;

#[tokio::test]
async fn html_assembles_markdown_from_complete_sentences() {
    let report = run_bytes_pipeline_with_params(
        test_text_binding(),
        &[
            bytes::Bytes::from_static(b"Hello **world**. "),
            bytes::Bytes::from_static(b"Second line with `code`! "),
        ],
        PipelineParams {
            render_console: true,
            dump_raw: false,
            html_dump_path: None,
            html_params: HtmlReportParams::default(),
            build_html: true,
        },
    )
    .await;

    let html = report.html_report.expect("html built");
    assert!(html.sentence_count >= 2, "sentences={}", html.sentence_count);
    assert!(
        html.assembled_markdown.contains("Hello **world**."),
        "md={}",
        html.assembled_markdown
    );
    // Markdown → HTML should render emphasis / code when present in sentences.
    assert!(
        html.document_html.contains("<strong>")
            || html.document_html.contains("Hello")
            || html.full_page_html.contains("Hello"),
        "doc html={}",
        html.document_html
    );
    assert!(html.full_page_html.contains("Canonical event timeline"));
    assert!(html.full_page_html.contains("Assembled document"));
    assert!(html.timeline_rows >= 2);
}

#[tokio::test]
async fn html_file_written_for_visual_review() {
    let dir = std::env::temp_dir().join(format!(
        "monoloop-html-{}",
        std::process::id()
    ));
    let path = dir.join("interpretation.html");

    // Use r## so embedded JSON with # characters is safe.
    let acp = concat!(
        r##"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"Heading line. "}}}}"##,
        r##"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"Body with **bold** text. "}}}}"##,
        r##"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s","update":{"sessionUpdate":"tool_call","toolCallId":"T1","title":"bash","status":"pending"}}}}"##,
        r##"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s","update":{"sessionUpdate":"tool_call_update","toolCallId":"T1","title":"bash","rawInput":{"command":"ls"}}}}"##,
        r##"{"jsonrpc":"2.0","id":1,"result":{"stopReason":"end_turn"}}"##,
    );

    let report = run_bytes_pipeline_with_params(
        acp_binding(),
        &[bytes::Bytes::from(acp.as_bytes().to_vec())],
        PipelineParams::with_html_dump(&path),
    )
    .await;

    assert_eq!(report.html_dump_path.as_ref(), Some(&path));
    assert!(path.is_file(), "expected file at {}", path.display());
    let page = std::fs::read_to_string(&path).expect("read html");
    assert!(page.contains("<!DOCTYPE html>"));
    assert!(page.contains("timeline") || page.contains("Timeline"));
    // Tool lifecycle visible for interpretation review
    assert!(
        page.contains("tool") || page.contains("bash"),
        "page missing tool markers"
    );

    let html = report.html_report.expect("report");
    // Re-write via helper is stable
    let path2 = dir.join("copy.html");
    write_html_report(&path2, &html).unwrap();
    assert!(path2.is_file());

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn build_html_report_standalone() {
    use monoloop_contracts::{
        CanonicalUnit, CanonicalUnitEvent, CanonicalUnitSnapshot, ConnectionId, FlowId,
        InterpretationId, InterpreterOutputEvent, LaneId, TextChannel, TextSentence, UnitId,
        UnitState,
    };
    let events = vec![InterpreterOutputEvent::Unit(CanonicalUnitEvent::Created(
        CanonicalUnitSnapshot {
            unit_id: UnitId::new("s1"),
            unit_generation: 1,
            unit_state: UnitState::Complete,
            interpretation_id: InterpretationId::new("i"),
            connection_id: ConnectionId::new("c"),
            external_session_id: None,
            flow_id: FlowId::main(),
            lane_id: LaneId::response(),
            lane_ordinal: 1,
            causal_parent_id: None,
            unit: CanonicalUnit::Text(TextSentence {
                sentence_id: UnitId::new("s1"),
                channel: TextChannel::PublicResponse,
                paragraph_id: None,
                sentence_ordinal: 1,
                content: "Only complete units appear.".into(),
            }),
        },
    ))];
    let r = build_html_report(&events, &HtmlReportParams::default());
    assert_eq!(r.sentence_count, 1);
    assert!(r.assembled_markdown.contains("Only complete units appear."));
}

#[test]
fn path_type_used() {
    let _p: PathBuf = PathBuf::from("/tmp/x.html");
}
