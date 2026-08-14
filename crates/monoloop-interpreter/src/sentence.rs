//! Deterministic sentence segmentation (versioned rules).

/// Version label for the segmenter (recorded in interpretation diagnostics).
#[allow(dead_code)]
pub const SENTENCE_SEGMENTER_VERSION: &str = "v1";

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
    pub fn push(&mut self, text: &str) -> Vec<String> {
        self.buf.push_str(text);
        self.drain_complete(false)
    }

    /// Seal remaining buffer at clean semantic completion (may lack terminal punctuation).
    pub fn seal_at_clean_end(&mut self) -> Vec<String> {
        self.drain_complete(true)
    }

    /// Discard buffer at abrupt end; return unresolved content without promoting it.
    pub fn take_unresolved(&mut self) -> String {
        std::mem::take(&mut self.buf)
    }

    fn drain_complete(&mut self, seal_remainder: bool) -> Vec<String> {
        let mut out = Vec::new();
        loop {
            if let Some((end, include_ws)) = find_sentence_end(&self.buf) {
                let _ = include_ws;
                let raw = self.buf[..end].to_string();
                let content = raw.trim_end().to_string();
                // advance past end and following whitespace
                let mut consume = end;
                while consume < self.buf.len()
                    && self.buf.as_bytes()[consume].is_ascii_whitespace()
                {
                    consume += 1;
                }
                self.buf = self.buf[consume..].to_string();
                if !content.is_empty() {
                    out.push(content);
                }
            } else {
                break;
            }
        }
        if seal_remainder && !self.buf.trim().is_empty() {
            let content = self.buf.trim().to_string();
            self.buf.clear();
            if !content.is_empty() {
                out.push(content);
            }
        }
        out
    }
}

/// Find exclusive end index of the first complete sentence, if any.
/// Returns (end_index, _).
fn find_sentence_end(s: &str) -> Option<(usize, bool)> {
    let bytes = s.as_bytes();
    let mut i = 0;
    let mut in_code_span = false;
    let mut paren = 0i32;
    let mut bracket = 0i32;
    let mut brace = 0i32;

    while i < bytes.len() {
        let c = bytes[i] as char;

        // backtick code spans (simple toggle)
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
                // abbreviations / decimals / versions / URLs
                if c == '.' && looks_like_abbreviation_or_decimal(s, i) {
                    i += 1;
                    continue;
                }
                // require end or whitespace after terminator for stability
                let next = bytes.get(i + 1).copied();
                match next {
                    // Prefer waiting over premature emission: a terminator at the
                    // end of the current buffer is not complete until we see the
                    // next character (or seal_at_clean_end is called).
                    None => {}
                    Some(b) if b.is_ascii_whitespace() => {
                        return Some((i + 1, true));
                    }
                    Some(b) if b == b'"' || b == b'\'' || b == b')' || b == b']' => {
                        // closing quote/paren after punctuation still ends sentence
                        return Some((i + 1, true));
                    }
                    _ => {
                        // e.g. file.ext or version-like — treat carefully
                        if c == '.' && next.is_some_and(|b| b.is_ascii_alphanumeric()) {
                            i += 1;
                            continue;
                        }
                    }
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

fn looks_like_abbreviation_or_decimal(s: &str, dot_idx: usize) -> bool {
    let bytes = s.as_bytes();
    // decimal: digit before and after
    let prev = bytes.get(dot_idx.wrapping_sub(1)).copied();
    let next = bytes.get(dot_idx + 1).copied();
    if prev.is_some_and(|b| b.is_ascii_digit()) && next.is_some_and(|b| b.is_ascii_digit()) {
        return true;
    }
    // single-letter initials: "A." or "e.g."
    if prev.is_some_and(|b| b.is_ascii_alphabetic()) {
        // if previous token is short (1-3 letters) treat as abbreviation candidate
        let mut start = dot_idx;
        while start > 0 && bytes[start - 1].is_ascii_alphabetic() {
            start -= 1;
        }
        let len = dot_idx - start;
        if (1..=3).contains(&len) {
            // "Mr." "Dr." "e.g" handled via multi-dot; single capital letter initials
            if len == 1 {
                return true;
            }
            let word = &s[start..dot_idx];
            const ABBREVS: &[&str] = &[
                "Mr", "Mrs", "Ms", "Dr", "Prof", "Sr", "Jr", "vs", "etc", "e.g", "i.e", "Inc",
                "Ltd", "St",
            ];
            if ABBREVS.iter().any(|a| a.eq_ignore_ascii_case(word)) {
                return true;
            }
        }
    }
    // URL-ish: "://" earlier in token
    let mut t = dot_idx;
    while t > 0 && !bytes[t - 1].is_ascii_whitespace() {
        t -= 1;
    }
    let token = &s[t..=dot_idx.min(s.len().saturating_sub(1))];
    if token.contains("://") || token.contains("www.") {
        return true;
    }
    // path-ish: slash in token
    if token.contains('/') {
        return true;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_on_period_space() {
        let mut s = SentenceSegmenter::new();
        let a = s.push("Hello world. ");
        assert_eq!(a, vec!["Hello world.".to_string()]);
        // Without trailing whitespace, '!' may seal when it is the last char.
        let b = s.push("Next one! ");
        assert_eq!(b, vec!["Next one!".to_string()]);
    }

    #[test]
    fn does_not_emit_partial() {
        let mut s = SentenceSegmenter::new();
        assert!(s.push("The build uses std::").is_empty());
        let done = s.push("sync::Arc to share the handle. ");
        assert_eq!(
            done,
            vec!["The build uses std::sync::Arc to share the handle.".to_string()]
        );
    }

    #[test]
    fn decimal_not_boundary() {
        let mut s = SentenceSegmenter::new();
        assert!(s.push("Version 1.2.3 is ready. ").len() == 1);
    }

    #[test]
    fn seal_done_without_punct() {
        let mut s = SentenceSegmenter::new();
        s.push("Done");
        let sealed = s.seal_at_clean_end();
        assert_eq!(sealed, vec!["Done".to_string()]);
    }

    #[test]
    fn abrupt_does_not_promote() {
        let mut s = SentenceSegmenter::new();
        s.push("The implementation will");
        let unresolved = s.take_unresolved();
        assert_eq!(unresolved, "The implementation will");
    }
}
