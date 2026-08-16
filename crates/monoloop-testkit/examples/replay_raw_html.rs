//! Replay a saved raw dump through Interpreter → HTML review (no live agent).
//!
//! Works for Grok WebSocket dumps and Cursor NDJSON dumps (`>>` / `<<` lines).
//!
//! ```bash
//! cargo run -p monoloop-testkit --example replay_raw_html -- target/live_grok_crud.raw.txt
//! cargo run -p monoloop-testkit --example replay_raw_html -- target/live_cursor_ask.raw.txt
//! open target/live_cursor_ask.replay.html
//! ```

use monoloop_testkit::{
    acp_binding, agy_acp_binding, build_html_report, codex_acp_binding, cursor_acp_binding,
    run_bytes_pipeline_with_params, write_html_report, HtmlReportParams, PipelineParams,
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
    let dialect_kind = detect_dialect(&raw_path, &raw);
    let frames = extract_json_frames(&raw, dialect_kind);
    println!(
        "extracted {} JSON frames from {} (dialect={dialect_kind:?})",
        frames.len(),
        raw_path.display()
    );

    let chunks: Vec<bytes::Bytes> = frames
        .into_iter()
        .map(|j| bytes::Bytes::from(format!("{j}\n")))
        .collect();

    let dialect = match dialect_kind {
        DialectKind::CursorAcp => cursor_acp_binding(),
        DialectKind::AgyAcp => agy_acp_binding(),
        DialectKind::CodexAcp => codex_acp_binding(),
        DialectKind::AcpGrok => acp_binding(),
    };
    let title = match dialect_kind {
        DialectKind::CursorAcp => "Cursor ACP — replayed assembly".to_string(),
        DialectKind::AgyAcp => "Antigravity ACP — replayed assembly".to_string(),
        DialectKind::CodexAcp => "Codex ACP — replayed assembly".to_string(),
        DialectKind::AcpGrok => "Grok/ACP — replayed assembly".to_string(),
    };

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let report = rt.block_on(run_bytes_pipeline_with_params(
        dialect,
        &chunks,
        PipelineParams {
            render_console: false,
            dump_raw: false,
            html_dump_path: None,
            html_params: HtmlReportParams {
                title,
                ..HtmlReportParams::default()
            },
            build_html: true,
        },
    ));

    let html = report.html_report.expect("html built");
    let out = out_path_for(&raw_path);
    write_html_report(&out, &html)?;

    let seq_path = out.with_extension("sequence.txt");
    let mut seq = String::new();
    seq.push_str("=== REPLAY — CANONICAL TEXT SENTENCES ===\n");
    for (i, line) in html.assembled_markdown.lines().enumerate() {
        seq.push_str(&format!("{i:04} | {line}\n"));
    }
    seq.push_str(&format!(
        "\nsentences={} timeline_rows={} strategy={:?} confidence={:?}\nassembled markdown:\n{}\n",
        html.sentence_count,
        html.timeline_rows,
        html.chat_projection.strategy,
        html.chat_projection.confidence,
        html.assembled_markdown
    ));
    seq.push_str("\n=== CHAT PROJECTION ===\n");
    seq.push_str(&html.chat_projection.plain_text);
    std::fs::write(&seq_path, seq)?;

    let chat_path = out.with_extension("chat.txt");
    std::fs::write(&chat_path, &html.chat_projection.plain_text)?;

    println!("wrote {}", out.display());
    println!("wrote {}", seq_path.display());
    println!("wrote {}", chat_path.display());
    println!(
        "sentences={} timeline_rows={} confidence={:?}",
        html.sentence_count, html.timeline_rows, html.chat_projection.confidence
    );
    println!("--- chat projection ---");
    println!("{}", html.chat_projection.plain_text);

    let _ = build_html_report(&[], &HtmlReportParams::default());
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DialectKind {
    AcpGrok,
    CursorAcp,
    AgyAcp,
    CodexAcp,
}

fn detect_dialect(path: &Path, raw: &str) -> DialectKind {
    let name = path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    if name.contains("codex") {
        DialectKind::CodexAcp
    } else if name.contains("agy") || name.contains("antigravity") {
        DialectKind::AgyAcp
    } else if name.contains("cursor")
        || raw.contains("<< {")
        || raw.lines().any(|l| l.starts_with("<< "))
    {
        DialectKind::CursorAcp
    } else {
        DialectKind::AcpGrok
    }
}

fn out_path_for(raw: &Path) -> PathBuf {
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

/// Pull complete JSON objects useful for interpretation.
///
/// For Cursor dumps with `>>` / `<<` prefixes, only **inbound** (`<<`) lines that
/// carry `session/update` (or prompt `stopReason` results) are kept.
fn extract_json_frames(raw: &str, kind: DialectKind) -> Vec<String> {
    let mut out = Vec::new();
    let bytes = raw.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        while i < bytes.len() && bytes[i] != b'{' {
            i += 1;
        }
        if i >= bytes.len() {
            break;
        }
        // Cursor dump: only take JSON that follows an inbound marker on this line.
        let line_start = raw[..i].rfind('\n').map(|p| p + 1).unwrap_or(0);
        let prefix = &raw[line_start..i];
        let inbound = match kind {
            DialectKind::CursorAcp | DialectKind::AgyAcp | DialectKind::CodexAcp => {
                prefix.contains("<<") || (!prefix.contains(">>") && prefix.trim().is_empty())
            }
            DialectKind::AcpGrok => true,
        };

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
                                if inbound && keep_frame(s, kind) {
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

fn keep_frame(s: &str, kind: DialectKind) -> bool {
    if !(s.contains("jsonrpc") || s.contains("sessionUpdate") || s.contains("session/update")) {
        return false;
    }
    match kind {
        // Interpretation cares about streamed updates + terminal stopReason.
        DialectKind::CursorAcp | DialectKind::AgyAcp | DialectKind::CodexAcp => {
            s.contains("session/update")
                || s.contains("stopReason")
                || s.contains("\"method\":\"session/update\"")
        }
        DialectKind::AcpGrok => true,
    }
}
