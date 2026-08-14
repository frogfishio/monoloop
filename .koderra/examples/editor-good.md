---
scope: editor, file-editor, prism
priority: 8
---

## Good pattern when working on editor / syntax

Always:
- Preserve the overlay textarea + Prism sync
- Use OnPush where possible
- Debounce expensive highlight
- Expose via service for testability

Bad: monolithic updates in app.ts that touch DOM directly.

Good response includes a small focused diff + test note.