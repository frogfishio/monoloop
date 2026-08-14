---
always: true
priority: 5
---

## Good general response pattern

User asked about X.

Response structure:
- Briefly acknowledge request
- Reference relevant rules/decisions from context (cite by name)
- Provide minimal correct change or answer
- Note any files touched
- End with verification note if code

Example:
"Understood — this touches the editor layer.

Per ARCHITECTURE and STYLE: keep thin, use composition.

Here's the diff...

(files touched: src/app/...)
Verified against rules: no bloat introduced."
