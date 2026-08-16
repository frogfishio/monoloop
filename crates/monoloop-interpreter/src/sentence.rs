//! Deterministic sentence segmentation (versioned rules).
//!
//! Prefers waiting over premature emission. Special-cases ordered list markers
//! (`1.` `2.`) so they stay attached to the following item text.

/// Version label for the segmenter (recorded in interpretation diagnostics).
pub const SENTENCE_SEGMENTER_VERSION: &str = "v2";

/// One completed sentence from the segmenter, with buffer consumption.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompletedSentence {
    /// Sentence text (trimmed trailing whitespace).
    pub content: String,
    /// Bytes of the content region (through terminator, excluding following
    /// whitespace). Used for dialect source-time attribution.
    pub content_bytes: usize,
    /// Bytes removed from the assembly buffer for this sentence
    /// (content region + trailing whitespace after the terminator).
    pub bytes_consumed: usize,
}

/// Deterministic sentence boundary finder.
///
/// Prefers waiting over premature emission. Does not emit incomplete fragments.
#[derive(Clone, Debug, Default)]
pub struct SentenceSegmenter {
    buf: String,
}

impl SentenceSegmenter {
    /// Create an empty segmenter.
    pub fn new() -> Self {
        Self::default()
    }

    /// Current assembly buffer length in bytes.
    pub fn buffered_bytes(&self) -> usize {
        self.buf.len()
    }

    /// Push text and return newly completed sentences (in order).
    ///
    /// Incomplete remainder stays buffered.
    pub fn push(&mut self, text: &str) -> Vec<CompletedSentence> {
        self.buf.push_str(text);
        self.drain_complete(false)
    }

    /// Seal remaining buffer at clean semantic completion (may lack terminal punctuation).
    pub fn seal_at_clean_end(&mut self) -> Vec<CompletedSentence> {
        self.drain_complete(true)
    }

    /// Discard buffer at abrupt end; return unresolved content without promoting it.
    pub fn take_unresolved(&mut self) -> String {
        std::mem::take(&mut self.buf)
    }

    fn drain_complete(&mut self, seal_remainder: bool) -> Vec<CompletedSentence> {
        let mut out = Vec::new();
        loop {
            if let Some(end) = find_sentence_end(&self.buf) {
                let raw = self.buf[..end].to_string();
                let content = raw.trim_end().to_string();
                // Content region ends at last non-whitespace of `raw` (usually `end`
                // when the terminator is non-whitespace).
                let content_bytes = content.len();
                // advance past end and following whitespace (but keep newlines as
                // paragraph hints only by dropping them from the next sentence start)
                let mut consume = end;
                while consume < self.buf.len()
                    && self.buf.as_bytes()[consume].is_ascii_whitespace()
                {
                    consume += 1;
                }
                self.buf = self.buf[consume..].to_string();
                if !content.is_empty() {
                    out.push(CompletedSentence {
                        content,
                        content_bytes,
                        bytes_consumed: consume,
                    });
                }
            } else {
                break;
            }
        }
        if seal_remainder && !self.buf.trim().is_empty() {
            let bytes_consumed = self.buf.len();
            let content = self.buf.trim().to_string();
            let content_bytes = content.len();
            self.buf.clear();
            if !content.is_empty() {
                out.push(CompletedSentence {
                    content,
                    content_bytes,
                    bytes_consumed,
                });
            }
        }
        out
    }
}

/// Find exclusive end index of the first complete sentence, if any.
fn find_sentence_end(s: &str) -> Option<usize> {
    let bytes = s.as_bytes();
    let mut i = 0;
    let mut in_code_span = false;
    let mut paren = 0i32;
    let mut bracket = 0i32;
    let mut brace = 0i32;

    while i < bytes.len() {
        let c = bytes[i] as char;

        if c == '`' {
            in_code_span = !in_code_span;
            i += 1;
            continue;
        }
        if in_code_span {
            i += 1;
            continue;
        }

        match c {
            '(' => paren += 1,
            ')' => paren = (paren - 1).max(0),
            '[' => bracket += 1,
            ']' => bracket = (bracket - 1).max(0),
            '{' => brace += 1,
            '}' => brace = (brace - 1).max(0),
            '.' | '!' | '?' => {
                if paren > 0 || bracket > 0 || brace > 0 {
                    i += 1;
                    continue;
                }
                // Ordered list markers: "1." "12." at line/token start — not ends.
                if c == '.' && looks_like_ordered_list_marker(s, i) {
                    i += 1;
                    continue;
                }
                // abbreviations / decimals / versions / URLs
                if c == '.' && looks_like_abbreviation_or_decimal(s, i) {
                    i += 1;
                    continue;
                }

                let next = bytes.get(i + 1).copied();
                match next {
                    // Terminator at buffer end: wait for more (or seal_at_clean_end).
                    None => {}
                    Some(b) if b.is_ascii_whitespace() => {
                        return Some(i + 1);
                    }
                    Some(b) if b == b'"' || b == b'\'' || b == b')' || b == b']' => {
                        return Some(i + 1);
                    }
                    // Missing space between sentences: "create.CRUD" → split after '.'
                    Some(b)
                        if c == '.'
                            && b.is_ascii_uppercase()
                            && prev_is_sentence_letter(bytes, i) =>
                    {
                        return Some(i + 1);
                    }
                    // file.ext / version-like: stay open
                    Some(b) if c == '.' && b.is_ascii_alphanumeric() => {}
                    _ => {}
                }
            }
            // Hard break: double newline closes a paragraph-like unit when stable.
            // Do not seal if the only content so far is an ordered-list marker
            // ("1." / "2.") — keep it for the following item text.
            '\n' => {
                if i + 1 < bytes.len() && bytes[i + 1] == b'\n' {
                    let before = s[..i].trim();
                    if !before.is_empty() && !is_only_list_marker(before) {
                        return Some(i);
                    }
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

fn prev_is_sentence_letter(bytes: &[u8], dot_idx: usize) -> bool {
    if dot_idx == 0 {
        return false;
    }
    let p = bytes[dot_idx - 1];
    p.is_ascii_lowercase() || p.is_ascii_uppercase()
}

fn is_only_list_marker(s: &str) -> bool {
    let t = s.trim();
    let b = t.as_bytes();
    if b.is_empty() || b[b.len() - 1] != b'.' {
        return false;
    }
    looks_like_ordered_list_marker(t, t.len() - 1)
}

/// `1.` / `12.` at the start of a line or after whitespace — keep with following item.
fn looks_like_ordered_list_marker(s: &str, dot_idx: usize) -> bool {
    let bytes = s.as_bytes();
    let mut start = dot_idx;
    while start > 0 && bytes[start - 1].is_ascii_digit() {
        start -= 1;
    }
    if start == dot_idx {
        return false;
    }
    let digit_len = dot_idx - start;
    if !(1..=3).contains(&digit_len) {
        return false;
    }
    // Must be at buffer start or after whitespace/newline.
    if start > 0 && !bytes[start - 1].is_ascii_whitespace() {
        return false;
    }
    // After the marker: whitespace, end of buffer, or markdown emphasis/start of item.
    match bytes.get(dot_idx + 1) {
        None => true,
        Some(b) if b.is_ascii_whitespace() => true,
        Some(b) if *b == b'*' || *b == b'_' || *b == b'`' || *b == b'[' => true,
        _ => false,
    }
}

fn looks_like_abbreviation_or_decimal(s: &str, dot_idx: usize) -> bool {
    let bytes = s.as_bytes();
    let prev = bytes.get(dot_idx.wrapping_sub(1)).copied();
    let next = bytes.get(dot_idx + 1).copied();
    if prev.is_some_and(|b| b.is_ascii_digit()) && next.is_some_and(|b| b.is_ascii_digit()) {
        return true;
    }
    // Do not treat pure digit runs as abbreviations here — list markers handled above.
    if prev.is_some_and(|b| b.is_ascii_alphabetic()) {
        let mut start = dot_idx;
        while start > 0 && bytes[start - 1].is_ascii_alphabetic() {
            start -= 1;
        }
        let len = dot_idx - start;
        if (1..=3).contains(&len) {
            if len == 1 {
                return true;
            }
            let word = &s[start..dot_idx];
            const ABBREVS: &[&str] = &[
                "Mr", "Mrs", "Ms", "Dr", "Prof", "Sr", "Jr", "vs", "etc", "Inc", "Ltd", "St",
            ];
            if ABBREVS.iter().any(|a| a.eq_ignore_ascii_case(word)) {
                return true;
            }
        }
    }
    let mut t = dot_idx;
    while t > 0 && !bytes[t - 1].is_ascii_whitespace() {
        t -= 1;
    }
    let end = (dot_idx + 1).min(s.len());
    let token = &s[t..end];
    if token.contains("://") || token.contains("www.") {
        return true;
    }
    if token.contains('/') {
        return true;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    fn contents(done: &[CompletedSentence]) -> Vec<String> {
        done.iter().map(|c| c.content.clone()).collect()
    }

    #[test]
    fn splits_on_period_space() {
        let mut s = SentenceSegmenter::new();
        let a = s.push("Hello world. ");
        assert_eq!(contents(&a), vec!["Hello world.".to_string()]);
        let b = s.push("Next one! ");
        assert_eq!(contents(&b), vec!["Next one!".to_string()]);
    }

    #[test]
    fn does_not_emit_partial() {
        let mut s = SentenceSegmenter::new();
        assert!(s.push("The build uses std::").is_empty());
        let done = s.push("sync::Arc to share the handle. ");
        assert_eq!(
            contents(&done),
            vec!["The build uses std::sync::Arc to share the handle.".to_string()]
        );
    }

    #[test]
    fn decimal_not_boundary() {
        let mut s = SentenceSegmenter::new();
        assert_eq!(s.push("Version 1.2.3 is ready. ").len(), 1);
    }

    #[test]
    fn list_markers_stay_with_item() {
        let mut s = SentenceSegmenter::new();
        // classic broken case: "1.\n\n**CREATE** — foo."
        assert!(s.push("1.\n\n").is_empty());
        let done = s.push("**CREATE** — Wrote the file with `hello monoloop crud`.\n\n");
        assert_eq!(done.len(), 1, "{done:?}");
        assert!(
            done[0].content.starts_with("1."),
            "list marker must stay attached: {}",
            done[0].content
        );
        assert!(done[0].content.contains("**CREATE**"));
    }

    #[test]
    fn list_sequence_does_not_emit_bare_numbers() {
        let mut s = SentenceSegmenter::new();
        let text = "1. **CREATE** — Wrote the file.\n\n2. **READ** — File contained x.\n\n3. **UPDATE** — Done.\n\n";
        let done = s.push(text);
        assert_eq!(done.len(), 3, "{done:?}");
        assert!(done
            .iter()
            .all(|x| !x.content.trim().chars().all(|c| c.is_ascii_digit() || c == '.')));
        assert!(done[0].content.contains("CREATE"));
        assert!(done[1].content.contains("READ"));
        assert!(done[2].content.contains("UPDATE"));
    }

    #[test]
    fn missing_space_after_period_splits() {
        let mut s = SentenceSegmenter::new();
        // Grok sometimes concatenates chunks without a space after '.'
        assert!(s.push("starting with create.").is_empty());
        let done = s.push("CRUD exercise on the file only:\n\n");
        assert_eq!(done.len(), 2, "{done:?}");
        assert_eq!(done[0].content, "starting with create.");
        assert!(done[1].content.starts_with("CRUD exercise"));
    }

    /// Exact token stream observed from live Grok CRUD (`target/live_grok_crud.raw.txt`).
    #[test]
    fn live_grok_crud_token_stream_assembles_cleanly() {
        let chunks = [
            "I'll",
            " run",
            " the",
            " five",
            " CRUD",
            " steps",
            " on",
            " that",
            " one",
            " file",
            " only",
            ",",
            " starting",
            " with",
            " create",
            ".",
            "CRUD",
            " exercise",
            " on",
            " `",
            "mon",
            "olo",
            "op",
            "_",
            "live",
            "_",
            "crud",
            "_",
            "test",
            ".txt",
            "`",
            " only",
            ":\n\n",
            "1",
            ".",
            " **",
            "CREATE",
            "**",
            " —",
            " Wrote",
            " the",
            " file",
            " with",
            " `",
            "hello",
            " mon",
            "olo",
            "op",
            " crud",
            "`.",
            "\n",
            "2",
            ".",
            " **",
            "READ",
            "**",
            " —",
            " File",
            " contained",
            " `",
            "hello",
            " mon",
            "olo",
            "op",
            " crud",
            "`.",
            "\n",
            "3",
            ".",
            " **",
            "UPDATE",
            "**",
            " —",
            " Over",
            "wrote",
            " it",
            " with",
            " `",
            "hello",
            " mon",
            "olo",
            "op",
            " crud",
            " UPD",
            "ATED",
            "`.",
            "\n",
            "4",
            ".",
            " **",
            "READ",
            "**",
            " —",
            " File",
            " contained",
            " `",
            "hello",
            " mon",
            "olo",
            "op",
            " crud",
            " UPD",
            "ATED",
            "`.",
            "\n",
            "5",
            ".",
            " **",
            "DELETE",
            "**",
            " —",
            " Removed",
            " the",
            " file",
            " (`",
            "rm",
            "`",
            " exited",
            " ",
            "0",
            ").",
            "\n\n",
            "No",
            " other",
            " files",
            " were",
            " touched",
            ".",
        ];
        let mut s = SentenceSegmenter::new();
        let mut done = Vec::new();
        for c in chunks {
            done.extend(s.push(c));
        }
        done.extend(s.seal_at_clean_end());
        let texts = contents(&done);

        // No glued create.CRUD; no bare list markers.
        assert!(
            texts.iter().all(|x| !x.contains("create.CRUD")),
            "must split missing-space: {texts:?}"
        );
        assert!(
            texts
                .iter()
                .all(|x| !is_only_list_marker(x) && x.trim() != "1." && x.trim() != "2."),
            "bare list markers leaked: {texts:?}"
        );
        assert!(
            texts.iter().any(|x| x.ends_with("create.")),
            "expected sentence ending create.: {texts:?}"
        );
        assert!(
            texts.iter().any(|x| x.starts_with("CRUD exercise")),
            "expected CRUD exercise sentence: {texts:?}"
        );
        assert_eq!(
            texts
                .iter()
                .filter(|x| x.contains("**CREATE**")
                    || x.contains("**READ**")
                    || x.contains("**UPDATE**")
                    || x.contains("**DELETE**"))
                .count(),
            5,
            "five step sentences: {texts:?}"
        );
        for step in texts.iter().filter(|x| {
            x.contains("**CREATE**")
                || x.contains("**READ**")
                || x.contains("**UPDATE**")
                || x.contains("**DELETE**")
        }) {
            assert!(
                step.chars().next().is_some_and(|c| c.is_ascii_digit()),
                "list marker must attach: {step}"
            );
        }
        assert!(
            texts.iter().any(|x| x.contains("No other files were touched")),
            "{texts:?}"
        );
    }

    #[test]
    fn seal_done_without_punct() {
        let mut s = SentenceSegmenter::new();
        s.push("Done");
        let sealed = s.seal_at_clean_end();
        assert_eq!(contents(&sealed), vec!["Done".to_string()]);
    }

    #[test]
    fn abrupt_does_not_promote() {
        let mut s = SentenceSegmenter::new();
        s.push("The implementation will");
        let unresolved = s.take_unresolved();
        assert_eq!(unresolved, "The implementation will");
    }
}
