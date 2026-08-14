# History / Generated Artifacts

This directory holds transient or generated files:
- prompt-*.md (conversation summaries, polish experiments, engineering notes)
- session-*.md (persist debugging notes)
- *-summary-*.md and similar

**These are NOT auto-injected into prompts.**

- Use `always: true` or `scope: ...` frontmatter if you want one pulled in.
- Otherwise they are ignored by `loadContext` / `getAugmentedPrompt` (high-signal .koderra RAG).
- Safe to prune periodically.
- For durable notes, prefer root docs (with frontmatter) or memory/.

See ARCHITECTURE and load logic for exclusion rules.
