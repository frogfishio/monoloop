# Agents

Custom agent definitions and monoloop-specific gate guidance.

## Built-in Delegation Tools (Agent mode)

When running as **Agent**, delegate with exact syntax:

- `ask_expert("...")` — deep technical implementation review
- `ask_advisor("...")` — governance, compliance, and product quality review

### When to use each

**ask_expert**: Is the *implementation* sound?
- Fit for purpose against `doc/` contracts?
- Edge cases: cancel/EOF races, fragmentation, multi-session isolation, backpressure?
- Holey async (unowned tasks, guards across await, unbounded queues)?
- Real reliability/load risks?

**ask_advisor**: Does it meet the *project bar*?
- ARCHITECTURE / LAWS / STYLE / SECURITY compliance?
- Three-component boundary intact (no testkit/product bleed)?
- Quality tier (Bronze / Silver / Golden) for a kernel this strict?
- Spec drift, persistence creep, ambient identity, or scope expansion?

Emit the calls literally. Use as final gates after functional delivery; re-invoke until passing.

## Monoloop gate checklist (both specialists)

- Correct component ownership (Connector / Interpreter / Loop / testkit)
- Explicit identities; no ambient current session/run/tool
- Bounded fail-closed resources; lossless Loop subscription
- Empty-tool path: zero effects, truthful unavailable
- No provider-native DTO across canonical boundary
- Product crates do not depend on testkit/console/host UI
