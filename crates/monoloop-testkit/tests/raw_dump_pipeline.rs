//! Pipeline raw dump: exact chunks as presented to the Interpreter.

use monoloop_testkit::{
    acp_binding, run_bytes_pipeline_with_params, PipelineParams,
};

#[tokio::test]
async fn pipeline_raw_dump_matches_fed_chunks_exactly() {
    let chunk_a = bytes::Bytes::from_static(
        br#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"Hi. "}}}}"#,
    );
    let chunk_b = bytes::Bytes::from_static(
        br#"{"jsonrpc":"2.0","id":1,"result":{"stopReason":"end_turn"}}"#,
    );

    let report = run_bytes_pipeline_with_params(
        acp_binding(),
        &[chunk_a.clone(), chunk_b.clone()],
        PipelineParams::with_raw_dump(),
    )
    .await;

    let dump = report.raw_dump.expect("raw dump enabled");
    assert_eq!(dump.frames.len(), 2);
    assert_eq!(&dump.frames[0].bytes[..], &chunk_a[..]);
    assert_eq!(&dump.frames[1].bytes[..], &chunk_b[..]);
    assert_eq!(dump.concat(), {
        let mut v = chunk_a.to_vec();
        v.extend_from_slice(&chunk_b);
        bytes::Bytes::from(v)
    });

    assert!(dump.contains_str("session/update"));
    assert!(dump.contains_str("end_turn"));

    let text = dump.format_text();
    assert!(text.contains("PIPELINE RAW DUMP"));
    assert!(text.contains("session/update"), "{text}");

    // Interpreter still assembled a sentence from the dump stream.
    assert!(
        report.console_text.contains("Hi."),
        "console: {}",
        report.console_text
    );
}

#[tokio::test]
async fn pipeline_without_dump_param_has_no_raw() {
    let report = run_bytes_pipeline_with_params(
        acp_binding(),
        &[bytes::Bytes::from_static(
            br#"{"jsonrpc":"2.0","id":1,"result":{"stopReason":"end_turn"}}"#,
        )],
        PipelineParams::console_only(),
    )
    .await;
    assert!(report.raw_dump.is_none());
}
