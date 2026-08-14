# Test Kit — Console Renderer

**Status:** Foundational test-adapter specification

**Product:** [Monoloop](MONOLOOP.md)

**System:** Ground-zero cognitive runtime

**Component kind:** Passive diagnostic projection

**Consumes:** [Component 02 — Interpreter](INTERPRETER.md) canonical events

**Produces:** Human-readable console output and optional canonical JSONL debug
output

**Parent index:** [README.md](README.md)

The words **MUST**, **MUST NOT**, **REQUIRED**, **SHOULD**, **SHOULD NOT**, and
**MAY** are normative requirements.

---

## 1. Purpose

The Console Renderer listens to canonical in-memory events and writes a safe,
legible diagnostic representation to a console sink.

It exists primarily to:

- observe Connector/Interpreter behavior before a graphical client exists;
- inspect sentence assembly and tool-action lifecycles in real time;
- distinguish simultaneous connections, flows, lanes, and units;
- diagnose ordering, cancellation, completion, and malformed-input behavior;
- provide a deterministic append-only trace for development and tests; and
- prove that presentation can remain separate from execution.

Its governing relationship is:

```text
Connector
    -> raw bytes
Interpreter
    -> canonical in-memory events
Console Renderer
    -> escaped console records
```

The Console Renderer is a view. It is never a source of runtime truth.

## 2. Architectural decision

The default Console Renderer is append-only and passive.

For example:

```text
[c:42 i:7 f:main l:response u:11 g:1] assistant  Hello world.
[c:42 i:7 f:main l:tool u:T1 g:1]      tool       waiting: request fragments
[c:42 i:7 f:main l:response u:12 g:1] assistant  I will inspect the file.
[c:42 i:7 f:main l:tool u:T1 g:2]      tool       ready: read_file
[c:42 i:7 f:main l:tool u:T1 g:3]      tool       completed: success
[c:42 i:7]                              end        complete
```

Each canonical generation receives its own console record. The renderer does
not erase earlier waiting states or pretend that the final state was always
known.

## 3. Responsibilities

The Console Renderer MUST:

- consume a bounded asynchronous canonical-event subscription;
- render events as they are received;
- preserve the received event order;
- include sufficient correlation on every record;
- display unit identity and generation for lifecycle-bearing units;
- distinguish complete, waiting, incomplete, failed, and terminal states;
- escape untrusted terminal content;
- serialize writes to one sink without interleaving bytes from records;
- expose renderer health and a terminal result;
- apply an explicit configured backpressure/loss policy;
- report any dropped render records explicitly; and
- release its subscription and writer resources on shutdown.

## 4. Explicit non-responsibilities

The Console Renderer MUST NOT:

- consume raw Connector bytes;
- select or interpret a dialect;
- reassemble text, Markdown, JSON, or tool fragments;
- change or enrich canonical meaning;
- mutate a canonical unit or generation;
- execute or control tools;
- cancel or terminate a Connector;
- decide whether an invocation, turn, task, or activity is complete;
- route models or build prompts;
- persist canonical or console output;
- update cognitive or product state;
- emit events back into the runtime;
- own a process-global event bus;
- infer a current connection/session/activity; or
- become a hidden dependency of runtime progress.

The renderer may display `InterpretationEnd::Complete`. It may not translate
that into `Turn complete` or any other state not present in the input event.

## 5. Input contract

The Renderer consumes an async stream of already canonical events:

```rust
pub enum ConsoleInputEvent {
    Unit(CanonicalUnitEvent),
    InterpretationEnded(InterpretationEnd),
    SourceStatus(CanonicalSourceStatus),
}
```

`CanonicalSourceStatus` is limited to safe source lifecycle observations needed
for diagnostics, such as subscription opened, closing, or lost. It does not
carry raw payloads or create semantic state.

The Console Renderer never receives a `RawOutput`, dialect-native event, token,
text delta, partial argument fragment, provider SDK object, or product UI block.

## 6. Renderer instance

```rust
pub trait ConsoleRendererFactory: Send + Sync {
    fn start(
        &self,
        request: StartConsoleRenderer,
    ) -> Result<ConsoleRendererHandle, ConsoleRendererError>;
}

pub struct StartConsoleRenderer {
    pub renderer_id: ConsoleRendererId,
    pub subscription: CanonicalEventSubscription,
    pub writer: ConsoleWriter,
    pub config: ConsoleRendererConfig,
}

pub struct ConsoleRendererHandle {
    pub renderer_id: ConsoleRendererId,
    pub control: ConsoleRendererControl,
    pub health: ConsoleRendererHealth,
    pub completion: ConsoleRendererCompletion,
}
```

The exact Rust interface may vary. Required semantics:

- `start` does not block on the lifetime of the event stream;
- one renderer instance owns one subscription and one serialized writer;
- the instance has explicit cancellation and completion;
- all internal queues are bounded;
- no instance is stored in global mutable current state; and
- a renderer may observe many connections because every event is explicitly
  correlated.

## 7. Correlation envelope

Every rendered canonical record includes the identities present in its event:

```text
renderer_id?          optional in human output, required in JSONL
connection_id
interpretation_id
external_session_id?
flow_id?
lane_id?
lane_ordinal?
unit_id?
unit_generation?
causal_parent_id?
```

For the Grok Build profile, `external_session_id` is Grok's `sessionId` and is
the session correlation identity shown by the test renderer.

If a later event distributor supplies an explicit host envelope, the renderer
may additionally display:

```text
host_id?
project_id?
turn_id?
activity_id?
```

These fields are displayed only when explicitly supplied. The Renderer never
derives them from connection order, text, task-local variables, or the most
recent event.

Equal local IDs in separate scopes must remain visually distinguishable.

## 8. Output modes

Initial modes:

```text
append_only_human
canonical_jsonl
```

### 8.1 Append-only human

The required default. It prints one complete escaped record per canonical event
generation. It favors diagnosis over visual minimalism.

### 8.2 Canonical JSONL

Optional deterministic machine-readable debugging output. Each line contains a
versioned render envelope and one canonical event projection.

JSONL is a debugging/export representation, not a persistence API or an
alternative canonical-event contract.

### 8.3 Interactive mode

An interactive TTY mode may later update waiting actions in place. It is
non-authoritative and optional. Append-only mode remains the required diagnostic
truth because it preserves every observed generation.

## 9. Append-only format

The human format has four conceptual columns:

```text
[correlation]  kind/state  label  content
```

Examples:

```text
[c:42 i:7 f:main l:response u:11 g:1] text/complete assistant Hello world.
[c:42 i:7 f:main l:tool u:T1 g:1] tool/waiting read_file waiting for arguments
[c:42 i:7 f:main l:tool u:T1 g:2] tool/ready read_file request complete
[c:42 i:7 f:main l:tool u:T1 g:3] tool/complete read_file success
[c:42 i:7] interpretation/complete events=18 sentences=9 tools=1
```

The precise spacing and optional color may be configurable. Field meaning,
escaping, identity, generation, and append-only behavior are fixed.

Human output is not parsed back into canonical events.

## 10. Text rendering

A `TextSentence` is printed only after the Interpreter has emitted the complete
canonical sentence.

The Console Renderer:

- prints the complete sentence once;
- identifies its channel;
- preserves textual content after terminal-safe escaping;
- may wrap visually without inserting semantic boundaries;
- does not join separate sentences into a new canonical paragraph;
- does not split a sentence into events; and
- does not revise the line when later events arrive.

Line wrapping is presentation only. Tests compare logical rendered records
before terminal-width wrapping.

## 11. Paragraph and structure rendering

Paragraph boundaries and structural atoms are rendered from their canonical
types.

The renderer may display:

- paragraph open/close markers in verbose mode;
- headings with a safe textual prefix;
- list boundaries and indentation;
- code blocks with explicit begin/end markers;
- table rows in a stable plain-text form;
- blockquote markers; and
- incomplete/oversized structural diagnostics.

It does not parse Markdown again. It trusts only the canonical structure
produced by the Interpreter.

## 12. Tool lifecycle rendering

Every meaningful `ToolActionId` generation is visible in append-only mode:

```text
g:1  waiting     action exists; waiting for complete request
g:2  ready       request is complete
g:3  waiting     external execution/result pending
g:4  completed   correlated terminal result observed
```

The Renderer MUST distinguish:

- request ready;
- execution waiting/running;
- result assembling;
- resolved success;
- resolved failure;
- malformed; and
- incomplete at interpretation termination.

It must not print hidden partial arguments/results from a waiting snapshot.
Full complete tool payload display is disabled by default and, if enabled for a
secured diagnostic session, remains escaped and bounded.

## 13. Concurrent rendering

Events from many connections, sessions, interpretations, flows, and lanes may
arrive concurrently.

The Renderer:

- serializes complete console records onto the writer;
- preserves its subscription receive order;
- prints correlation on every record;
- never groups by “currently active connection”;
- never delays one lane while waiting for another lane to resolve;
- never reorders by wall-clock timestamp; and
- never claims cross-lane causality absent from the event.

Console order is an observation order. Canonical lane ordinals and causal-parent
identity remain the semantic ordering authority.

## 14. Async execution

The Console Renderer is async from the first implementation.

Requirements:

- event consumption does not block an async worker on synchronous terminal I/O;
- writes use an async writer or a specifically owned bounded blocking writer;
- one slow console does not block unrelated renderer instances;
- cancellation wakes blocked reads and writes;
- all spawned work belongs to the renderer handle;
- completion resolves exactly once;
- shutdown is bounded; and
- no polling of UI or global state is used.

Async does not mean detached fire-and-forget tasks. Every task has an owner,
bounded queue, cancellation path, terminal outcome, and cleanup obligation.

## 15. Backpressure and loss policy

`ConsoleRendererConfig` selects one explicit policy:

```text
lossless_backpressure
best_effort_debug
```

### 15.1 Lossless backpressure

Every accepted input event is rendered before the subscription advances.
Console slowness may backpressure this renderer's subscription. It must not
silently drop records.

### 15.2 Best-effort debug

The renderer prioritizes runtime isolation. If its bounded write queue is full,
it may drop console records. It MUST:

- count dropped records;
- preserve counts by broad event kind where bounded;
- emit an explicit `renderer dropped N records` notice when output resumes;
- include the total in its terminal report; and
- never claim its output is complete.

The selected policy applies to presentation only. It does not delete canonical
events from another consumer or alter runtime authority.

## 16. Subscription boundary

The Renderer receives an owned bounded subscription. It does not create or own
the process-wide fan-out mechanism.

A later event-dispatch component may provide subscriptions to console, product UI,
execution, recording, and inspection consumers. That component—not the Console
Renderer—owns fan-out, replay, subscriber isolation, and delivery guarantees.

The Renderer cannot subscribe directly to random internal actors or bypass the
canonical event contract.

## 17. Console writer

`ConsoleWriter` is an injected output abstraction supporting:

```text
stdout
stderr
test memory writer
file descriptor/pipe supplied by caller
```

The writer contract accepts complete render records. It never receives
canonical state by mutable reference.

The Renderer does not open arbitrary files. Durable logging and file rotation
are separate components. A caller may explicitly supply a writer attached to a
file or pipe, but that does not make persistence a Renderer responsibility.

## 18. Output-write atomicity

One rendered record is encoded fully before writing. Concurrent events cannot
interleave their bytes within a record.

For sinks without atomic multi-byte writes, the renderer's single writer owner
serializes the complete write and handles partial-write retries at the I/O
level. A failed partial write produces renderer failure; it is not reported as a
complete record.

## 19. Terminal safety

Canonical text and tool content are untrusted terminal input.

Before human-mode output, the Renderer MUST neutralize or visibly escape:

- ANSI/ECMA-48 control sequences;
- cursor movement and screen clearing;
- OSC hyperlinks and clipboard commands;
- carriage returns capable of overwriting prefixes;
- bidirectional text controls according to configured safety policy;
- non-printing control characters other than permitted newline/tab handling;
- excessively long unbroken content; and
- terminal title or notification sequences.

ANSI color/style may be generated only by the Renderer from trusted event kinds
and only when enabled for a compatible TTY.

Redirected output defaults to no color.

## 20. Content bounds

The Renderer applies display bounds independently of canonical validity:

```text
maximum rendered record bytes
maximum displayed tool payload bytes
maximum diagnostic detail bytes
maximum line width before visual wrapping
maximum queued render records/bytes
maximum accumulated drop counters
```

If complete canonical content exceeds a display bound, the Renderer prints an
explicit truncation marker with original byte count and safe digest/reference
when supplied. It does not modify the canonical event.

Display truncation is always visible.

## 21. Filtering

Optional immutable filter configuration may select:

- connection or interpretation identity;
- explicit host/session/activity identity when present;
- event/unit kind;
- channel;
- tool state;
- minimum diagnostic severity; and
- inclusion of paragraph/structural markers.

Filtering affects display only. Filtered events are counted and declared in the
renderer terminal report. Filters cannot alter upstream subscriptions, state,
execution, or persistence.

## 22. Renderer lifecycle

```text
configured
    -> starting
        -> running
            -> draining
            -> cancelling
            -> writer_failed
            -> source_ended
        -> start_failed

draining, cancelling, writer_failed, source_ended, start_failed
    -> terminal
```

The lifecycle is renderer-local. It contains no model/turn/task state.

Stopping the Renderer detaches its subscription and closes its owned writer
handle. It does not cancel the Connector, Interpreter, invocation, or activity.

## 23. Renderer completion

```text
ConsoleRendererEnd
    renderer_id
    kind
    events_received
    records_rendered
    records_filtered
    records_dropped
    bytes_written
    safe_writer_error?
```

Kinds:

```text
source_ended
cancelled
writer_failed
configuration_failed
invariant_failed
```

Exactly one terminal result is produced. Renderer failure is an observer
failure, not an execution failure.

## 24. Failure isolation

If the console closes, blocks, or fails:

- the Renderer reports unhealthy/terminal;
- its owned tasks and buffers are released;
- no canonical unit is changed;
- no upstream state transition is created;
- no Connector is cancelled by the Renderer;
- no other subscriber is detached; and
- no activity is declared failed or complete.

The composition layer may choose to stop a debugging run when its only renderer
fails. That is composition policy, not Renderer behavior.

## 25. Human and JSONL stability

Human output is intended for people and may evolve with an explicit format
revision. Tests should validate semantic fields rather than incidental spacing
or ANSI bytes.

JSONL output has a versioned schema:

```text
console_render_event/v1
    render_sequence
    correlation
    event_kind
    canonical_schema_version
    canonical_projection
    display_truncation?
```

JSONL contains a safe projection, not arbitrary serialization of internal Rust
types. Unknown schema versions fail explicitly in consumers.

## 26. Configuration

```text
ConsoleRendererConfig
    mode
    color: auto | always | never
    verbosity
    correlation_style
    backpressure_policy
    queue_limits
    content_limits
    filter
    terminal_safety_policy_version
    format_version
```

Configuration is immutable for one renderer instance. Reconfiguration creates a
new renderer or an explicit later control contract; it is not inferred from
terminal size or environment changes mid-record.

## 27. Observability

The Renderer exposes content-free metrics:

```text
console_renderer_events_received{kind}
console_renderer_records_rendered{kind,state}
console_renderer_records_filtered{kind}
console_renderer_records_dropped{kind}
console_renderer_bytes_written
console_renderer_queue_depth
console_renderer_write_latency
console_renderer_terminal{kind}
```

Metrics do not contain raw text, tool payloads, project/session IDs, paths,
credentials, or unbounded writer errors.

## 28. Required tests

### 28.1 Basic rendering

- Complete sentence produces one complete console record.
- Paragraph/structure events render from canonical structure without reparsing.
- Interpretation completion renders without claiming turn completion.
- Waiting, ready, running, resolved, malformed, and incomplete tool states are
  visually distinct.
- Every tool generation remains visible in append-only mode.

### 28.2 Correlation and concurrency

- Events from many connections remain distinguishable.
- Equal unit IDs in different interpretations do not collide.
- No current-session/current-connection heuristic exists.
- Concurrent writer input cannot interleave record bytes.
- Subscription order is preserved.
- Cross-lane arrival order is not described as causality.

### 28.3 Async isolation

- Slow console affects only its renderer/subscription according to policy.
- Cancellation wakes blocked subscription and writer work.
- Writer failure terminates only the Renderer.
- Dropping the Renderer does not cancel upstream execution.
- Renderer completion cannot panic if writer/subscription already completed.
- All tasks and buffers are reclaimed after terminal state.

### 28.4 Backpressure and loss

- Lossless mode drops zero accepted records.
- Best-effort mode stays within bounds.
- Every best-effort drop is counted.
- Output recovery emits an explicit drop notice.
- Terminal report includes all drops and filters.
- No unbounded queue or forwarding task appears under load.

### 28.5 Terminal security

- ANSI escape injection is neutralized.
- OSC hyperlink/clipboard/title sequences are neutralized.
- Carriage-return overwrite attacks cannot erase correlation prefixes.
- Bidirectional controls follow policy and are visible/neutralized.
- Invalid UTF-8 cannot reach the writer in human mode.
- Renderer-generated color is absent when redirected or disabled.

### 28.6 Bounds and truncation

- Oversized content produces an explicit truncation marker.
- Original size/digest metadata remains truthful.
- Tool payloads are hidden by default.
- Long unbroken text cannot cause unbounded allocation.
- Logical records remain stable across terminal widths.

### 28.7 JSONL

- Every line is one valid versioned object.
- Concurrent events cannot interleave JSON lines.
- Safe projections contain no forbidden raw/internal fields.
- Output is deterministic for an identical canonical event sequence.
- Display truncation is explicit.

### 28.8 Architecture

- Renderer crates do not import Connector implementations, dialect decoders,
  host agent, product UI, Kanban, DAL, Residiuum, tool execution, router, prompt
  builder, or state-machine modules.
- No raw byte/dialect input method exists.
- No persistence or execution-control path is reachable.
- No event is emitted back into the runtime.
- No global subscriber/current-session state exists.
- No unbounded queue exists.

## 29. Acceptance criteria

The Console Renderer test adapter is accepted only when:

1. it renders the full Component 02 canonical vocabulary;
2. complete sentence events appear in real time without waiting for response
   completion;
3. tool/task generations are append-only and truthfully labelled;
4. simultaneous connections/sessions remain distinguishable;
5. received order is preserved without invented causality;
6. untrusted terminal content is neutralized;
7. output writes cannot interleave record bytes;
8. queues, records, payload display, and shutdown are bounded;
9. strict and best-effort policies have explicit tested behavior;
10. dropped/filtered/truncated output is always declared;
11. writer or renderer failure cannot alter or stop runtime execution;
12. no raw parsing, state, persistence, control, routing, prompting, or tool
    responsibility enters the component;
13. append-only human output and versioned JSONL pass golden tests; and
14. all concurrency, security, failure, and architecture suites pass without a
    partial or “shaped” qualification.

## 30. First vertical qualification

Components 01–02 plus this test adapter form the first observable transport and
interpretation slice:

```text
one or more real/fake Connectors
    -> one Interpreter per connection
    -> canonical async event subscriptions
    -> one Console Renderer
```

Qualification runs multiple simultaneous connections and demonstrates:

- byte fragmentation disappears at the Interpreter boundary;
- complete sentences appear immediately;
- concurrent tools progress through explicit generations;
- cancellation yields truthful incomplete/terminal observations;
- console correlation remains clear under interleaving;
- console failure does not alter the connections; and
- no persistence, product UI, host agent, task system, or prompt engine is
  required.

This slice is a diagnostic harness, not yet an LLM execution loop.

## 31. Deferred presentation

Later components may provide:

- event fan-out and replay;
- product UI rendering;
- collapsible activity groups;
- interactive terminal line replacement;
- durable diagnostic logging;
- user controls; and
- activity/turn-level presentation.

They consume canonical events or later runtime projections. They do not expand
the Console Renderer's authority.

## 32. Governing rule

> The Console Renderer observes canonical events and writes an escaped,
> correlated, append-only debugging projection. It may fail, lag, filter, or be
> absent without changing the execution it observes.
