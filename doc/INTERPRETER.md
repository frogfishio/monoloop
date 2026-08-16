# Component 02 — Interpreter

**Status:** Foundational component specification

**Product:** [Monoloop](MONOLOOP.md)

**System:** Ground-zero cognitive runtime

**Component kind:** Incremental dialect interpreter and semantic-unit assembler

**Consumes:** [Component 01 — Connector](CONNECTOR.md)

**Produces:** Provider-neutral canonical semantic stream

**Parent index:** [README.md](README.md)

The words **MUST**, **MUST NOT**, **REQUIRED**, **SHOULD**, **SHOULD NOT**, and
**MAY** are normative requirements.

---

## 1. Purpose

The Interpreter consumes the raw output bytes and immutable dialect binding of
one Connector connection. It incrementally decodes the dialect, correlates
interleaved material, assembles fragments, and emits provider-neutral canonical
semantic units.

The Interpreter's defining rule is:

> No provider token, byte fragment, text delta, partial JSON argument, or
> arbitrary transport chunk escapes as canonical content.

Text is emitted as complete sentences or complete non-sentence structural
atoms. Tool material is correlated by identity and represented as complete,
explicitly waiting, or explicitly incomplete. Nothing is silently guessed.

## 2. Position in the system

```text
Connector
    RawOutput<Bytes> + DialectBinding + ConnectionEnd
                         |
                         v
                    Interpreter
          framing -> dialect mapping -> assembly
                         |
                         v
              CanonicalSemanticStream
```

The Connector owns transport truth. The Interpreter owns dialect meaning and
fragment assembly. A later execution component owns invocation/turn control,
tool execution, product effects, persistence, and continuation.

## 3. Architectural decision

The Interpreter is stateful per connection but product-neutral.

For each opened raw connection it:

1. selects an exact dialect implementation from the negotiated binding;
2. incrementally frames raw bytes;
3. decodes dialect messages;
4. maps dialect messages into semantic fragments;
5. reassembles text, structure, tool activity, usage, diagnostics, and semantic
   boundaries;
6. correlates concurrent lanes by stable dialect identity;
7. publishes an in-memory event immediately whenever a canonical unit or a new
   canonical unit generation is created;
8. emits complete text/structure units and truthfully incomplete waiting units
   only for lifecycle-bearing constructs such as tools; and
9. closes with a complete interpretation report that accounts for unresolved
   assembly state.

It does not drive the external system and does not decide what happens next.

## 4. Responsibilities

The Interpreter MUST:

- consume ordered raw Connector output;
- honor the exact immutable output dialect descriptor;
- perform incremental byte/text/frame decoding;
- map dialect-native events into a closed canonical vocabulary;
- assemble text fragments into complete sentence atoms;
- assemble non-sentence structures into complete structural atoms;
- correlate tool fragments and lifecycle observations by stable identity;
- represent concurrent tool activity without inventing a total order;
- preserve causal and lane-local ordering;
- distinguish complete, waiting, incomplete, malformed, and unsupported input;
- enforce bounded assembly buffers and pending-item counts;
- expose exactly one interpretation terminal report;
- publish canonical-unit lifecycle events in real time through a bounded async
  output;
- flush only semantically complete buffered material on clean completion; and
- quarantine rather than promote unresolved material on abrupt termination.

## 5. Explicit non-responsibilities

The Interpreter MUST NOT:

- open, close, retry, cancel, or terminate a Connector on its own;
- choose a Connector, provider, model, route, or dialect;
- build or augment prompts;
- tokenize text for model accounting;
- emit provider token fragments or text deltas;
- execute, approve, reject, or schedule tools;
- decide that a user turn or activity has completed;
- maintain authoritative conversation or working state;
- persist canonical output;
- render Markdown, HTML, cards, or UI components;
- infer task, package, session, or project identity;
- launch Tasker, specialists, subagents, or another invocation;
- expose private provider chain-of-thought;
- repair missing semantic content with an LLM; or
- merge unrelated lanes based on arrival proximity.

## 6. Interpreter factory and instance

```rust
pub trait InterpreterFactory: Send + Sync {
    fn supports(&self, dialect: &DialectBinding) -> SupportLevel;

    fn start(
        &self,
        request: StartInterpretation,
    ) -> Result<Interpretation, InterpreterError>;
}

pub struct StartInterpretation {
    pub interpretation_id: InterpretationId,
    pub connection_id: ConnectionId,
    pub external_session_id: Option<ExternalSessionId>,
    pub dialect: DialectBinding,
    pub limits: InterpretationLimits,
}

pub struct Interpretation {
    pub input: InterpretationInput,
    pub events: CanonicalEventStream,
    pub status: InterpretationStatus,
    pub completion: InterpretationCompletion,
}
```

The exact Rust interface may vary. Required semantics:

- one Interpretation consumes one connection output;
- dialect selection is complete before bytes are accepted;
- one instance owns its assembly state;
- instances share no mutable semantic buffers;
- all channels are bounded; and
- the interpretation ID is correlation identity only, not activity authority.

`external_session_id`, when present, is supplied by the Connector envelope. For
the Grok Build profile it is the Grok-returned `sessionId`. The Interpreter
propagates it unchanged and never invents, parses, or selects a session.

An implementation may consume `RawOutput` directly rather than expose an input
handle, provided the same ownership and testing boundaries remain explicit.

## 7. Input contract

Interpreter input consists of:

```text
raw output byte chunks
selected output dialect descriptor
Connector terminal outcome
```

Byte chunks are transport fragments. Their boundaries carry no meaning.

The Interpreter MUST produce identical canonical output for every possible
fragmentation of the same byte sequence.

For example, these are equivalent:

```text
["hello world."]
["hello "]["world."]
["h"]["e"]["l"]["l"]["o world."]
```

## 8. Interpretation pipeline

The internal conceptual pipeline is:

```text
Raw bytes
    -> transport-payload byte accumulator
    -> dialect framer
    -> dialect message decoder
    -> semantic fragment mapper
    -> lane/correlation dispatcher
    -> sentence and structure assemblers
    -> tool-action assemblers
    -> canonical validator
    -> canonical semantic output
```

Implementations may fuse stages for efficiency, but tests and diagnostics must
retain these distinct failure classes.

No stage after framing may depend on Connector chunk boundaries.

## 9. Canonical semantic event stream

The Interpreter publishes canonical units as an asynchronous in-memory event
stream. It does not wait for the complete response or complete Interpretation
before making a newly created canonical unit available.

The closed top-level canonical-unit vocabulary is initially:

```rust
pub enum CanonicalUnit {
    Text(TextSentence),
    Structure(StructuralAtom),
    Paragraph(ParagraphBoundary),
    Tool(ToolActionEvent),
    Usage(UsageObservation),
    Diagnostic(ModelDiagnostic),
    Boundary(SemanticBoundary),
}
```

Every event carries:

```text
canonical_event_id
interpretation_id
connection_id
external_session_id?
flow_id
lane_id
lane_ordinal
causal_parent_id?
source_position_range
```

Canonical units contain no provider-native DTO. Dialect-specific evidence may
be referenced through bounded safe diagnostics but cannot become control state.

### 9.1 Unit lifecycle events

Canonical units are delivered through a closed lifecycle-event vocabulary:

```rust
pub enum CanonicalUnitEvent {
    Created(CanonicalUnitSnapshot),
    Advanced(CanonicalUnitSnapshot),
    Completed(CanonicalUnitSnapshot),
    Incomplete(CanonicalUnitSnapshot),
}
```

`CanonicalUnitSnapshot` contains one `CanonicalUnit` plus its lifecycle and
correlation envelope.

Each snapshot carries:

```text
unit_id
unit_kind
unit_generation
unit_state
interpretation_id
connection_id
flow_id
lane_id
lane_ordinal
causal_parent_id?
source_time?          # observational dialect time (first_ms/last_ms); not causality
canonical content allowed for that state
```

`source_time` is **optional** and **observational only**. When the dialect
supplies a source clock (Grok ACP: `params._meta.agentTimestampMs`), the
Interpreter records the earliest and latest contributing fragment timestamps
on the complete unit. It MUST NOT:

- establish cross-lane causality;
- replace lane ordinal or explicit causal parent;
- invent wall-clock times when the dialect omits them; or
- treat source time as turn success, authorization, or run completion.

`Created` is published as soon as a canonical unit exists. In the common case,
a sentence or structural atom is already complete when created, so a single
created-and-complete snapshot is sufficient. Lifecycle-bearing units such as
concurrent tool actions may be created in an explicit waiting/incomplete state
and advanced as correlated information arrives.

`Advanced` and `Completed` retain the same unit ID and increment generation.
Generation cannot go backwards, skip an accepted prior generation, or mutate a
previously emitted immutable sentence.

### 9.2 What may be published incomplete

Incomplete canonical publication is permitted only when the incomplete state is
itself meaningful and safe:

```text
tool action declared but awaiting complete request
tool request complete but awaiting external execution
tool execution observed but awaiting terminal result
explicit dialect task/activity awaiting resolution
open lifecycle-bearing structure whose existence is already certain
```

It is not permitted for raw textual fragments, partial JSON arguments, partial
tool results, ambiguous Markdown, or arbitrary byte chunks.

Therefore:

```text
["hel"] ["lo wor"]
    -> no canonical unit event

["ld."]
    -> Created(TextSentence("hello world."), complete)
```

### 9.3 Publication point

One serialized Interpretation owner performs an atomic logical step:

```text
validate assembled unit/generation
    -> commit it to in-memory interpretation state
    -> enqueue its canonical event to the bounded output
    -> continue consuming input
```

The event is published before later source material is processed. The
Interpreter must not accumulate complete units for batch delivery.

If the bounded output cannot accept the event, interpretation applies
backpressure. It does not drop the event, create an unbounded forwarding task,
or continue indefinitely while canonical state becomes invisible downstream.

### 9.4 Delivery semantics

Within one Interpretation:

- events are emitted in the serialized order in which canonical generations are
  committed;
- lane ordinals and causal relationships remain authoritative over incidental
  cross-lane arrival order;
- each accepted generation is enqueued once by the Interpreter;
- consumers use interpretation/unit/generation identity for deduplication; and
- no global ordering is claimed across Interpretations or connections.

Fan-out to UI, execution, inspection, recording, or other consumers belongs to
a later runtime event-dispatch component. The Interpreter has one bounded
output contract and does not manage a process-global subscriber registry.

### 9.5 In-memory scope

Canonical event production, assembly state, pending tools, paragraph state, and
lane correlation are in memory and scoped to the Interpretation instance.

The Interpreter performs no durable write before, during, or after publication.
A later component may consume canonical events and decide what, if anything,
becomes a durable product.

## 10. No-token contract

The Interpreter never emits:

```text
Token("hel")
TextDelta("lo")
ContentFragment(" world")
ArgumentDelta("{\"pa")
```

Model billing tokens are unrelated to canonical text units. Tokenization may be
measured elsewhere but does not influence the Interpreter's output granularity.

Temporary fragments exist only inside bounded assembly state. They disappear
when incorporated into a complete atom or are accounted for as unresolved at
interpretation termination.

## 11. Text sentence atom

```text
TextSentence
    sentence_id
    channel
    paragraph_id?
    sentence_ordinal
    content
    completeness = complete
    source_position_range
```

Initial canonical channels:

```text
public_response
public_reasoning_summary
status_narration
quoted_external_content
```

Unknown channels do not default to public response. They are unsupported or
mapped to a safe diagnostic according to the dialect contract.

The Interpreter never labels hidden/private chain-of-thought as public
reasoning. Only a dialect field explicitly qualified as publishable reasoning
summary may use that channel.

## 12. Sentence atomicity

A sentence becomes canonical only when its boundary is stable.

```text
raw fragments:
    ["The build uses std::"]
    ["sync::Arc to share the handle."]

canonical:
    TextSentence("The build uses std::sync::Arc to share the handle.")
```

The first fragment is never emitted independently.

A sentence is immutable after emission. Later bytes cannot append to, replace,
or reinterpret it. Therefore the segmenter must prefer waiting over premature
emission.

As soon as the stable sentence is committed, its `Created` event is enqueued.
The Interpreter does not wait for paragraph closure, response completion, or
Connector EOF.

## 13. Sentence boundary rules

Sentence segmentation is deterministic and versioned.

The segmenter considers:

- dialect-supplied text-block boundaries;
- terminal punctuation;
- abbreviations and initials;
- decimal numbers and version identifiers;
- URLs, paths, identifiers, and code spans;
- Markdown emphasis/link delimiters;
- quotation and bracket balance;
- paragraph/structure boundaries;
- channel changes;
- tool-action boundaries; and
- clean semantic completion.

Punctuation alone is not sufficient when the surrounding structure remains
open or ambiguous.

At a clean dialect semantic completion, a final stable utterance such as
`Done` MAY be sealed as a sentence without terminal punctuation. At abrupt
transport failure, cancellation, termination, malformed framing, or truncation,
an unterminated text buffer MUST NOT be promoted to a complete sentence.

## 14. Paragraphs

A paragraph is an ordered grouping of sentence atoms, not a large mutable text
blob.

```text
ParagraphBoundary
    paragraph_id
    kind: opened | closed
    channel
    lane_id
```

Sentences may be emitted while their paragraph remains open. Each sentence
carries its immutable paragraph identity.

A tool action may be causally positioned between two sentences associated with
the same broader response. Whether it visually breaks the paragraph is a later
presentation decision. The Interpreter preserves both sentence membership and
tool causality; it does not rewrite either to suit a renderer.

## 15. Non-sentence structural atoms

Not all meaningful model output is grammatical prose. The Interpreter emits
complete structural atoms for:

```text
heading
list_item_boundary
code_block
table_row
blockquote_boundary
thematic_break
declared_raw_block
```

Text inside headings, list items, and block quotes may still contain sentence
atoms. Code blocks and table rows are sealed only when their dialect/Markdown
structure is complete.

The Interpreter does not render these atoms into HTML. Structural output
preserves content and relationships required by a later renderer.

If an individual structural atom exceeds its configured bound, interpretation
fails or produces an explicit oversized/incomplete diagnostic. It is not split
into arbitrary display fragments and called complete.

## 16. Markdown boundary

Markdown is interpreted only to the extent required to establish canonical
semantic and structural units.

The Interpreter may recognize:

- paragraph boundaries;
- headings;
- lists and list-item boundaries;
- fenced code blocks;
- block quotes;
- tables;
- links/code spans needed for safe sentence segmentation; and
- thematic breaks.

It does not produce DOM, HTML, CSS, layout, typography, collapsible groups, or
product UI-specific activity cards.

Malformed Markdown remains literal content or an explicit structural diagnostic
according to deterministic rules. It is never repaired by guessing intent.

## 17. Flows, lanes, and causality

The canonical model is a partially ordered stream, not one invented global
sequence.

```text
Flow
    one logical dialect exchange or response

Lane
    one ordered source of semantic material within that flow

Causal parent
    the item that caused or owns another item
```

Ordering guarantees:

- events within one lane have a strict ordinal;
- source position is monotonic within a decoded dialect stream;
- cross-lane relationships require an explicit dialect correlation or causal
  link;
- arrival time alone does not establish causality;
- dialect source timestamps (when present) are observational metadata and do
  not establish causality; and
- a later renderer may choose a visual linearization without changing canonical
  order.

## 18. Tool-action identity

Every tool action is keyed by a stable `ToolActionId` obtained from the dialect
or deterministically scoped to the interpretation when the dialect lacks one.

Identity derivation MUST NOT rely only on:

- tool name;
- array position;
- arrival adjacency;
- currently visible action; or
- “the last unresolved tool.”

If a dialect cannot provide or safely derive distinct identities for concurrent
tool actions, that concurrency is unsupported and must fail explicitly.

## 19. Tool-action model

```text
CanonicalToolAction
    tool_action_id
    flow_id
    lane_id
    causal_parent_id?
    tool_name?
    request_payload?
    request_state
    execution_state
    result_payload?
    result_state
    terminal_outcome?
    generation
```

States are explicit:

```text
request_state:
    assembling | ready | malformed | incomplete

execution_state:
    not_observed | waiting | running | terminal

result_state:
    absent | assembling | complete | malformed | incomplete
```

`request_payload` appears in canonical output only when the complete dialect
payload is assembled and syntactically valid. Partial JSON or argument text
never escapes.

## 20. Tool-action events

The Interpreter may emit:

```text
ToolActionWaiting
    identity and safe known metadata
    waiting_for
    generation

ToolRequestReady
    complete tool name and request payload
    generation

ToolActionResolved
    complete observed request/result lifecycle
    terminal outcome
    generation

ToolActionIncomplete
    unresolved portions and termination cause
    generation
```

All events for one action retain the same identity and monotonically increasing
generation.

Waiting events never expose partial arguments or claim executability. They make
concurrent pending work visible without manufacturing completion.

The initial waiting snapshot is published immediately when the action's stable
identity and existence are known. Each mechanically meaningful state advance is
published immediately; raw fragment arrival alone is not a meaningful advance.

## 21. Meaning of tool completeness

Two different boundaries must remain explicit:

### 21.1 Request completeness

The dialect has supplied a complete, syntactically valid tool name and argument
payload. `ToolRequestReady` may be consumed by a later validation/execution
component.

This does not mean the tool executed.

### 21.2 Action resolution

The observed tool lifecycle has a correlated terminal result or terminal
failure. Only then may the Interpreter emit `ToolActionResolved` when the
dialect itself carries that lifecycle.

Some dialects, such as an agent protocol, may report request, execution, and
result through the same raw stream. Other dialects, such as a direct model API,
may provide only the tool request. In the latter case the Interpreter emits a
complete request marked `execution_state = waiting`; a later execution
component owns execution and eventual cross-component resolution.

The Interpreter must never fabricate an execution result merely because the
request was complete.

## 22. Concurrent tool assembly

The Interpreter maintains a bounded correlation table keyed by ToolActionId:

```text
T1: assembling request arguments
T2: request ready, execution waiting
T3: assembling result
T4: resolved
```

Fragments for T1, T2, and T3 may arrive interleaved. Each fragment updates only
its identified assembler.

If downstream visibility is useful before resolution, the Interpreter emits a
waiting state with `waiting_for`, not raw fragments. When additional material
arrives, it emits a new generation for the same action.

Limits apply to pending action count, per-action bytes, aggregate tool bytes,
and maximum unresolved lifetime/source distance.

## 23. Usage and diagnostics

Usage is emitted only when a dialect supplies a complete normalized observation:

```text
UsageObservation
    input_tokens: measured | unavailable
    output_tokens: measured | unavailable
    cached_tokens: measured | unavailable
    reasoning_tokens: measured | unavailable
    provider_units[]
```

Unavailable is not zero.

Diagnostics distinguish:

```text
dialect_warning
model_reported_error
unsupported_event
malformed_frame
malformed_semantic_payload
incomplete_text
incomplete_structure
incomplete_tool_action
limit_exceeded
```

Provider error bodies are normalized/redacted according to dialect policy. A
model-reported error is not a Connector transport failure.

## 24. Semantic boundaries

The Interpreter may recognize dialect-level boundaries such as:

```text
response_started
channel_started
channel_finished
response_finished
usage_finalized
```

These are observations about the decoded dialect exchange. They are not
authoritative states such as:

```text
turn_finished
task_complete
work_accepted
review_passed
activity_sealed
```

The future execution machine decides whether a semantic boundary satisfies its
invocation or turn transition requirements.

## 25. Clean completion

On a clean dialect `response_finished` followed by compatible Connector closure,
the Interpreter:

1. completes all dialect frames;
2. seals any final structurally valid sentence/atom;
3. finalizes complete usage and diagnostics;
4. marks unresolved tools explicitly incomplete or waiting according to dialect
   semantics;
5. closes open paragraph/structure boundaries where mechanically valid; and
6. publishes one `InterpretationEnd::Complete` report.

Clean completion does not mean the user turn or work item is complete.

## 26. Abrupt termination

On Connector cancellation, forced termination, transport failure, malformed
terminal framing, or unexpected EOF, the Interpreter:

- stops accepting new bytes;
- drains only bytes already accepted according to bounded policy;
- emits already completed canonical atoms unchanged;
- does not flush an unterminated text fragment as a sentence;
- marks incomplete structures and tool actions explicitly;
- records bounded counts/digests of discarded unresolved fragments;
- publishes one non-success interpretation report; and
- releases all assembly buffers.

Cancellation never transforms partial content into a successful response.

## 27. Interpretation completion

```text
InterpretationEnd
    interpretation_id
    connection_id
    kind
    dialect
    canonical_event_count
    completed_sentence_count
    completed_structure_count
    tool_action_counts_by_state
    unresolved_text_bytes
    unresolved_structure_count
    source_bytes_consumed
    safe_diagnostics[]
```

Kinds:

```text
complete
cancelled
terminated
transport_failed
dialect_failed
limit_exceeded
invariant_failed
```

Exactly one terminal report is published. It references, but does not replace,
the Connector's terminal outcome.

## 28. Bounds and backpressure

`InterpretationLimits` includes:

```text
maximum undecoded bytes
maximum dialect frame bytes
maximum sentence assembly bytes
maximum structural atom bytes
maximum simultaneous paragraphs/structures
maximum pending tool actions
maximum bytes per pending tool action
maximum aggregate pending tool bytes
maximum canonical output queue items/bytes
maximum safe diagnostics
```

Every buffer is bounded. When a required unit exceeds its bound, the Interpreter
fails explicitly or emits a typed oversized diagnostic according to dialect
policy. It never emits arbitrary fragments to relieve memory pressure.

If downstream stops consuming canonical events, backpressure reaches raw-output
consumption. The execution machine may then cancel the Connector. The
Interpreter does not create an unbounded drain task or silently discard events.

## 29. Determinism

Given identical:

- raw payload byte sequence;
- selected dialect binding;
- Interpreter implementation/version;
- sentence segmenter version;
- limits; and
- Connector terminal outcome,

the Interpreter produces identical canonical semantic events and terminal
report regardless of raw byte fragmentation or asynchronous polling schedule.

Wall-clock arrival timing must not alter sentence segmentation, tool identity,
causal relationships, or completion classification.

## 30. Dialect implementations

Each dialect implementation supplies:

```text
frame decoder
semantic event mapper
channel classification
tool identity and fragment rules
usage mapping
semantic completion rules
safe diagnostic normalization
declared unsupported cases
```

Expected implementations may include:

```text
OpenAI Responses SSE
Anthropic Messages SSE
ACP / JSON-RPC
Cursor ACP profile
Grok Build ACP or JSONL profile
local model protocol
deterministic test dialect
```

Dialect plugins depend on Interpreter contracts and a minimal codec layer. They
do not depend on host agents, product UI, Kanban, persistence, tools, or the
future execution machine.

## 31. Versioning

The following are versioned independently:

- dialect family/version/profile;
- frame decoder;
- semantic mapper;
- sentence segmenter;
- Markdown structural assembler;
- canonical schema; and
- safe diagnostic policy.

An interpretation report records these versions. Changing one may change
canonical output and therefore requires golden-fixture review.

An opened Interpretation cannot switch implementation versions mid-stream.

## 32. Error vocabulary

Closed error families:

```text
unsupported_dialect
dialect_binding_mismatch
invalid_utf8_where_required
malformed_frame
frame_limit_exceeded
unsupported_semantic_event
malformed_semantic_payload
sentence_limit_exceeded
structure_limit_exceeded
tool_identity_missing
tool_identity_conflict
tool_limit_exceeded
causal_reference_invalid
output_backpressure_exceeded
connector_ended_unexpectedly
invariant_violation
```

Errors carry safe source positions and correlation IDs. They do not contain raw
prompts, secrets, complete provider bodies, or unbounded malformed payloads.

## 33. Security and trust

All raw output is untrusted.

The Interpreter MUST:

- enforce frame and aggregate limits before allocation growth;
- treat tool names/arguments as data, not executable instructions;
- avoid terminal escape/control-sequence interpretation;
- preserve quoted external content as untrusted content;
- reject cross-action fragment injection;
- prevent one lane from closing or resolving another without correlation;
- redact provider diagnostics before canonical emission; and
- avoid logging raw bytes by default.

Canonical does not mean trusted. It means structurally normalized and fully
assembled. Authority and effect validation occur downstream.

## 34. Concurrency

The Interpreter supports concurrent lanes inside one dialect stream and many
Interpretation instances concurrently.

Requirements:

- one serialized assembly owner per Interpretation;
- lane-local strict ordering;
- bounded correlation maps;
- identity-based tool correlation;
- no shared mutable “current tool” or “current paragraph” across instances;
- cancellation of one connection affects only its Interpretation;
- completion of one lane does not close siblings; and
- terminal cleanup accounts for every pending assembler.

The implementation may parallelize expensive decoding only if canonical output
remains deterministic and lane ordering is preserved.

The complete runtime is async from the first implementation:

- Connector reads, Interpreter assembly, and canonical event consumption run as
  independently scheduled bounded tasks;
- no blocking I/O or blocking wait occurs on an async worker thread;
- one slow connection cannot block another Interpretation;
- connection, interpretation, flow, lane, and unit identity accompany every
  event rather than relying on task-local or global current state;
- an explicitly supplied external session identity accompanies every event for
  a session-based Connector profile;
- shared registries, if later introduced, store handles only and do not merge
  assembly state; and
- cancellation and shutdown are propagated asynchronously without polling UI
  state.

“Async” does not permit uncontrolled task spawning. Every task has an owner,
bounded input, cancellation path, terminal result, and cleanup responsibility.

The Rust implementation is safe on a multi-threaded async runtime. It performs
no blocking waits and holds no synchronization guard across an await. Separate
Interpretations can execute on different runtime workers without sharing
mutable assembly state.

## 35. Observability

Content-free measurements include:

```text
raw bytes consumed
frames decoded by kind
canonical events by kind
fragment-to-sentence assembly ratio
sentence assembly latency
pending tool actions by state
tool resolution latency when observable
unresolved bytes/items at termination
malformed and unsupported events
output backpressure duration
interpretation terminal kind
```

Metrics labels are bounded. Raw text, tool arguments/results, prompts, project
IDs, and provider bodies are not metric labels or default logs.

## 36. Required tests

### 36.1 Fragment independence

- Every possible split of representative byte sequences yields identical
  canonical output.
- Random chunk fragmentation/coalescing property tests pass.
- Multi-byte UTF-8 split at every byte boundary reconstructs exactly.
- Empty chunks and delayed chunks change nothing.
- No token/text-fragment event appears in canonical output.
- A complete sentence event is observable before response completion.

### 36.2 Sentence assembly

- Ordinary punctuation produces complete sentences.
- Abbreviations, decimals, versions, URLs, paths, code spans, and quotations do
  not cause premature boundaries.
- A clean final `Done` seals as one sentence.
- Abrupt EOF with `The implementation will` emits no complete sentence for that
  fragment and reports it unresolved.
- Emitted sentences are immutable.
- Paragraph membership remains stable around interspersed tool actions.

### 36.3 Structures

- Headings, nested lists, block quotes, fenced code, and tables assemble into
  complete canonical structure.
- Fragmentation inside delimiters changes nothing.
- Malformed/unclosed structures are explicit on abrupt end.
- Oversized structures fail according to policy without arbitrary output
  fragmentation.
- Renderer/DOM types are absent.

### 36.4 Tool assembly

- Fragmented name and JSON arguments emit no executable request until complete.
- Complete request emits exactly one `ToolRequestReady` generation.
- Interleaved fragments for many tool IDs never cross-contaminate.
- Result before/after other lane text correlates by identity.
- Waiting states expose no partial arguments.
- Request-ready is never mistaken for execution-complete.
- Agent dialect with terminal result emits one resolved action.
- Direct-model dialect without result remains waiting for downstream execution.
- Missing/conflicting tool identity fails explicitly.
- Abrupt end produces incomplete actions, never success.
- A known concurrent action publishes an immediate waiting event.
- Each meaningful action-state generation publishes immediately and in order.
- Raw argument/result fragments do not create generation events.

### 36.5 Partial ordering

- Lane ordinals are strict and stable.
- Arrival timing cannot manufacture cross-lane causality.
- Explicit causal parents survive interleaving.
- A presentation linearization does not mutate canonical relationships.
- Equal input under different polling schedules produces equal output.

### 36.6 Completion and failure

- Clean semantic completion seals valid final atoms.
- Connector remote EOF without semantic completion is classified correctly.
- Cancel/terminate/failure preserve completed atoms and quarantine partial ones.
- Exactly one InterpretationEnd is emitted.
- Connector end and dialect completion races follow deterministic rules.
- Awaiting completion after internal reader completion cannot panic.

### 36.7 Bounds and load

- Every declared buffer limit is enforced.
- Long punctuation-free output cannot grow memory without bound.
- Thousands of pending fake tool calls hit the configured limit safely.
- Slow canonical consumer applies bounded backpressure.
- Many simultaneous interpretations remain isolated.
- Cleanup releases every pending assembler after terminal state.
- Hundreds of connections/sessions stream canonical events concurrently without
  shared current-state leakage.
- Interleaved Grok sessions preserve their supplied Grok `sessionId` on every
  canonical event.
- A blocked consumer backpressures only its owned path, not unrelated
  Interpretations.

### 36.8 Dialect conformance

For every dialect:

- golden raw payloads produce canonical fixtures;
- malformed and unknown events have explicit behavior;
- usage unavailable is not zero;
- dialect completion is not turn completion;
- provider-native DTOs do not escape; and
- fixture replay is independent of chunking.

### 36.9 Architecture

- Interpreter crates do not import host agent, product UI, Kanban, DAL,
  Residiuum, tool execution, router, prompt builder, or renderer modules.
- Connector implementations do not contain Interpreter logic.
- Interpreter does not call Connector cancellation/termination itself.
- No persistence path is reachable.
- No model tokenizer controls semantic granularity.
- No generic JSON canonical-event escape hatch exists.
- No unbounded channel or accumulator exists.
- No process-global event bus or subscriber registry is owned by Interpreter.
- No completed canonical unit is retained for batch-only publication.

## 37. Acceptance criteria

Component 02 is accepted only when:

1. at least one streaming HTTP dialect, one process/agent dialect, and the
   deterministic test dialect satisfy the same canonical contract;
2. arbitrary byte fragmentation produces identical output;
3. no text token/delta escapes downstream;
4. sentences are emitted only when stable and remain immutable;
5. non-sentence structures are fully assembled and typed;
6. paragraphs retain stable sentence membership around tool activity;
7. concurrent tool actions correlate only by stable identity;
8. no partial arguments become executable canonical requests;
9. request completeness and execution resolution remain distinct;
10. waiting and incomplete actions are visible without counterfeit completion;
11. abrupt termination never promotes unresolved text/tool content;
12. canonical output is deterministic under concurrency and polling variation;
13. all buffers, correlation tables, and queues are bounded;
14. each canonical creation or meaningful generation is published immediately
    through the in-memory async event stream;
15. incomplete publication is restricted to safe lifecycle-bearing units and
    never exposes text or argument fragments;
16. many connections/sessions operate concurrently without global state or
    cross-stream ordering assumptions;
17. semantic completion is not treated as activity/turn completion;
18. no persistence, execution, routing, prompting, state, or UI responsibility
    enters the component;
19. architecture gates enforce those boundaries; and
20. all fragmentation, dialect, sentence, tool, failure, concurrency, and load
    suites pass without partial or “shaped” qualification.

## 38. Deferred components

The Interpreter deliberately leaves the following to later components:

- canonical request encoder;
- invocation/activity execution machine;
- tool policy, execution, supervision, and result injection;
- model routing;
- prompt/context compilation;
- product and cognitive state;
- canonical record persistence;
- rendering and product UI activity grouping; and
- retry, continuation, and completion policy.

Those components consume canonical semantic units. They do not obtain raw
provider fragments as an alternative path.

## 39. Governing rule

> The Interpreter turns dialect-labelled raw bytes into fully assembled,
> provider-neutral semantic units. Sentences and structures are atomic. Tool
> actions are identity-correlated and truthfully complete, waiting, or
> incomplete. Every canonical creation or meaningful state advance is published
> immediately through a bounded in-memory async stream. No fragment is promoted
> into meaning before the Interpreter can prove that meaning is whole.
