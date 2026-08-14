---
always: true
priority: 15
---

# Coding Style & Conventions — Monoloop

## General

- Follow existing patterns once crates exist; until then, follow `doc/` contracts.
- Prefer **composition over inheritance** and **componentisation over monolith**.
- Use meaningful, domain-aligned names from the specs (`ConnectionId`,
  `ToolRequestReady`, `DialectBinding`, `MonoloopRunId`, etc.).
- Prefer small public surfaces: traits for ports, explicit request/handle/completion
  types, closed error enums.

## Spec and documentation style

- Normative language in `doc/`: **MUST / MUST NOT / SHOULD / MAY** as used today.
- Keep non-responsibilities lists honest when adding features — do not silently
  delete a MUST NOT to make an implementation easier.
- Trait spellings in docs may vary; preserve **required semantics** (immediate
  return of pending handles, exactly one terminal, etc.).

## Rust / async (when implementing)

- Tokio multi-thread async runtime for the initial implementation.
- `Send + Sync` on shared factories and registries as required by the contracts.
- No blocking I/O or blocking waits on async worker threads.
- Do not hold a `Mutex`/`RwLock` guard across `.await`.
- Unavoidable blocking work goes to a bounded blocking facility, not the async pool.
- Prefer bounded channels (`mpsc` with capacity) over unbounded.
- Cancellation: cooperative, idempotent, wakes blocked I/O; escalate with bounded
  grace then force where the contract allows.

## Identity and types

- Thread correlation IDs on every event and result; do not rely on “the only”
  connection/tool in scope.
- External session IDs are opaque wrappers (e.g. Grok `sessionId`) — compare and
  route, do not parse for authority.
- Prefer newtypes for IDs over bare `String`/`Uuid` at public boundaries.
- Provider-native DTOs stop at dialect plugins; canonical types stay provider-neutral.

## Errors and diagnostics

- Closed error families per component (see each `doc/*` error vocabulary).
- Bounded, redacted diagnostics only — no raw bodies, prompts, secrets, or
  unrestricted endpoints in errors or metrics labels.
- Distinguish transport failure, dialect failure, cancellation, limit exceeded,
  and invariant violation; do not flatten everything to a string.

## Testing style

- Deterministic fixtures over live flaky paths for unit/conformance.
- Architecture/import gates for forbidden dependencies.
- Test fragmentation invariance for Interpreter; race matrices for cancel/EOF.
- Empty-tool path is required qualification, not a temporary stub to skip.
- Console adapters optional: product suites must pass without them.

## Module placement

- Put types shared by all three components in contracts.
- Dialect-specific code lives in Interpreter dialect plugins or Connector profiles —
  not in The Loop.
- Test Driver composition stays in testkit; never import testkit from product crates.

## What not to “clean up”

- Do not merge Console and Loop onto one event receiver.
- Do not add “temporary” persistence, global current session, or prompt helpers
  inside product components for convenience.
- Do not invent successful tool results for unavailable tools.
