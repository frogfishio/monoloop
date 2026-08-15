//! Replay a saved raw dump through Interpreter → HTML review (no live Grok).
//!
//! ```bash
//! cargo run -p monoloop-testkit --example replay_raw_html -- target/live_grok_crud.raw.txt
//! open target/live_grok_crud.replay.html
//! ```

use monoloop_testkit::{
    acp_binding, build_html_report, run_bytes_pipeline_with_params, write_html_report,
    HtmlReportParams, PipelineParams,
};
use std::path::{Path, PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let raw_path = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("target/live_grok_crud.raw.txt"));
    if !raw_path.is_file() {
        return Err(format!("raw dump not found: {}", raw_path.display()).into());
    }

    let raw = std::fs::read_to_string(&raw_path)?;
    let frames = extract_json_frames(&raw);
    println!("extracted {} JSON frames from {}", frames.len(), raw_path.display());

    // Feed each complete JSON object as its own byte chunk (plus newline) —
    // fragmentation-invariant Interpreter path.
    let chunks: Vec<bytes::Bytes> = frames
        .into_iter()
        .map(|j| bytes::Bytes::from(format!("{j}\n")))
        .collect();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let report = rt.block_on(run_bytes_pipeline_with_params(
        acp_binding(),
        &chunks,
        PipelineParams {
            render_console: false,
            dump_raw: false,
            html_dump_path: None,
            html_params: HtmlReportParams {
                title: "Live Grok CRUD — replayed assembly".into(),
                ..HtmlReportParams::default()
            },
            build_html: true,
        },
    ));

    let html = report.html_report.expect("html built");
    let out = out_path_for(&raw_path);
    write_html_report(&out, &html)?;

    // Also refresh sequence summary for quick inspection.
    let seq_path = out.with_extension("sequence.txt");
    let mut seq = String::new();
    seq.push_str("=== REPLAY — CANONICAL TEXT SENTENCES ===\n");
    for (i, line) in html.assembled_markdown.lines().enumerate() {
        seq.push_str(&format!("{i:04} | {line}\n"));
    }
    seq.push_str(&format!(
        "\nsentences={} timeline_rows={}\nassembled markdown:\n{}\n",
        html.sentence_count, html.timeline_rows, html.assembled_markdown
    ));
    std::fs::write(&seq_path, seq)?;

    println!("wrote {}", out.display());
    println!("wrote {}", seq_path.display());
    println!(
        "sentences={} timeline_rows={}",
        html.sentence_count, html.timeline_rows
    );
    println!("--- assembled markdown ---");
    println!("{}", html.assembled_markdown);

    // Standalone build also keeps the API warm for tools without pipeline.
    let _ = build_html_report(&[], &HtmlReportParams::default());
    Ok(())
}

fn out_path_for(raw: &Path) -> PathBuf {
    // target/live_grok_crud.raw.txt → target/live_grok_crud.replay.html
    let name = raw
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("replay.raw.txt");
    let base = name
        .strip_suffix(".raw.txt")
        .or_else(|| name.strip_suffix(".txt"))
        .unwrap_or(name);
    raw.with_file_name(format!("{base}.replay.html"))
}

/// Pull complete JSON objects from a raw dump text file (frame-annotated or not).
fn extract_json_frames(raw: &str) -> Vec<String> {
    let mut out = Vec::new();
    let bytes = raw.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        // skip to next '{'
        while i < bytes.len() && bytes[i] != b'{' {
            i += 1;
        }
        if i >= bytes.len() {
            break;
        }
        let start = i;
        let mut depth = 0i32;
        let mut in_str = false;
        let mut escape = false;
        while i < bytes.len() {
            let c = bytes[i];
            if in_str {
                if escape {
                    escape = false;
                } else if c == b'\\' {
                    escape = true;
                } else if c == b'"' {
                    in_str = false;
                }
            } else {
                match c {
                    b'"' => in_str = true,
                    b'{' => depth += 1,
                    b'}' => {
                        depth -= 1;
                        if depth == 0 {
                            i += 1;
                            if let Ok(s) = std::str::from_utf8(&bytes[start..i]) {
                                // Keep session traffic + results; skip pure dump headers.
                                if s.contains("jsonrpc") || s.contains("sessionUpdate") {
                                    out.push(s.to_string());
                                }
                            }
                            break;
                        }
                    }
                    _ => {}
                }
            }
            i += 1;
        }
        if depth != 0 {
            break;
        }
    }
    out
}
