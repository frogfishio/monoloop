# Test Kit — Console Input

**Status:** Foundational test-adapter specification

**Product:** [Monoloop](MONOLOOP.md)

**Component kind:** Test-only canonical request source

**Produces:** One complete request submission for one explicitly selected
Channel

**Production status:** Prohibited as a product dependency

**Parent index:** [README.md](README.md)

The words **MUST**, **MUST NOT**, **REQUIRED**, **SHOULD**, **SHOULD NOT**, and
**MAY** are normative requirements.

---

## 1. Purpose

Console Input is the smallest possible human-operated test entrance to
Monoloop.

It lets a developer:

1. select a preconfigured Channel;
2. type one complete prompt;
3. submit that prompt as one canonical request; and
4. retain the returned run handle for observation or cancellation.

It exists to exercise Monoloop without host runtime, product UI, a GUI, a prompt engine,
or a persistence system.

## 2. Governing rule

> Console Input captures one complete prompt and hands it, unchanged in
> meaning, to one explicitly selected Channel as one new Monoloop run.

Console Input is not a terminal chat client. Every submission is an independent
test operation.

## 3. Explicit non-responsibilities

Console Input MUST NOT:

- select, rank, recommend, or fall back between Channels;
- choose a model or provider;
- add system instructions, memory, project context, files, or tool schemas;
- compile, improve, score, summarize, or otherwise rewrite the prompt;
- encode a provider-native request;
- parse a provider dialect;
- retain conversation history or reconstruct a previous conversation;
- create a durable session, chat, task, turn, or message;
- persist input, output, events, or run state;
- interpret slash commands as product commands;
- call tools;
- render canonical response events;
- infer that two submissions belong to the same conversation; or
- expose itself as a required production dependency.

Channel selection is test configuration. Outbound dialect encoding belongs to
the selected Channel binding. Output belongs to an independent subscriber such
as Console Renderer.

## 4. Input modes

The initial adapter supports two explicit modes.

### 4.1 One-shot mode

One-shot mode reads a complete prompt from standard input until EOF and creates
exactly one submission.

```text
bytes until EOF
    -> validate and decode
    -> one ConsolePromptSubmission
    -> end input adapter
```

This is the normative automation and conformance-test mode.

### 4.2 Interactive line mode

Interactive line mode treats each submitted line as one complete prompt.

```text
line + Enter
    -> one ConsolePromptSubmission
next line + Enter
    -> another independent ConsolePromptSubmission
```

It MUST display clearly that each line starts a fresh run. It MUST NOT present
the interface as a continuing chat.

Multiline interactive editing is deferred. When added, it requires an explicit
submission boundary; the adapter must never guess prompt completion from
punctuation, Markdown, silence, or elapsed time.

## 5. Configuration

Console Input receives configuration before accepting submissions:

```text
ConsoleInputConfig
    selected_channel_id
    mode
    maximum_prompt_bytes
    maximum_pending_submissions
    read_shutdown_grace
    display_policy
```

`selected_channel_id` is REQUIRED. It identifies an existing Channel binding by
stable identity, not display label.

If the Channel does not exist or is unavailable, the host rejects the
submission. Console Input does not substitute another Channel.

The implementation MAY permit a developer to change the configured Channel
between submissions. Such a change is explicit and affects only later
submissions; it can never retarget an active run.

## 6. Canonical output

Console Input emits a test-harness product:

```rust
pub struct ConsolePromptSubmission {
    pub submission_id: SubmissionId,
    pub request_id: RequestId,
    pub selected_channel_id: ChannelId,
    pub input: CanonicalInput,
    pub limits_override: Option<MonoloopLimitsOverride>,
}

pub enum CanonicalInput {
    Text(CanonicalTextPrompt),
}

pub struct CanonicalTextPrompt {
    pub text: String,
}
```

The exact Rust spelling may vary. Required semantics:

- every accepted submission has fresh stable identities;
- the Channel identity is explicit;
- the prompt is complete;
- the payload is provider-neutral;
- the payload contains no terminal presentation markup;
- the submission contains no previous request or response; and
- conversion into `MonoloopRequest` is mechanical.

The host supplies or validates `MonoloopRunId`. Console Input MUST NOT reuse a
run identity.

## 7. Text fidelity

The submitted canonical text MUST preserve the user's decoded text exactly,
except for the explicitly documented removal of the input submission delimiter.

Console Input MUST NOT silently:

- trim leading or trailing semantic whitespace;
- normalize Markdown;
- correct spelling;
- replace quotes or punctuation;
- interpolate environment variables;
- expand shell syntax;
- interpret escape sequences not defined by the selected input mode;
- prepend labels such as `user:`; or
- append a newline merely because the terminal supplied one as a delimiter.

UTF-8 is the initial canonical text encoding. Invalid input returns a typed
input error; lossy replacement is prohibited.

## 8. Submission lifecycle

```text
idle
    -> reading
        -> complete
            -> validating
                -> submitted
                    -> idle

reading | validating
    -> rejected
    -> cancelled

any nonterminal state
    -> shutting_down
        -> terminal
```

`submitted` means only that the host accepted the immutable request for a new
Monoloop run. It does not mean:

- Connector opened;
- request bytes were written;
- a model responded;
- tools completed; or
- the run succeeded.

Those facts belong to Monoloop events and completion.

## 9. Relationship to a Monoloop run

Every accepted submission produces exactly one call to:

```text
Monoloop.process(new request)
```

The returned `MonoloopRun` is registered only in the test harness so the
operator can observe or cancel that exact run.

Console Input owns no state inside the run. Once the submission is accepted it
may discard the prompt buffer and submission object. It does not wait for one
run to finish before another may be submitted, subject to configured test-harness
bounds.

There is no `current session`. An interactive implementation MAY expose the
most recently launched run as a display convenience, but control operations
must resolve to an explicit `MonoloopRunId` before execution.

## 10. Cancellation and terminal control

Terminal interrupt is an out-of-band test control, not prompt content.

The initial behavior is:

- while reading an unsubmitted prompt, interrupt cancels that input operation;
- when exactly one run is active and the harness explicitly enables
  single-run shorthand, interrupt may call that run's control handle;
- with multiple active runs, cancellation requires an explicit run identity;
- shutdown cancels pending reads and applies the harness's explicit active-run
  shutdown policy; and
- no textual marker such as `^C`, `/stop`, or `cancel` is sent to the Channel.

Console Input does not directly close Connector handles or kill tools. It calls
the selected `MonoloopRunControl` only.

## 11. Async architecture

Terminal reading MUST NOT block Monoloop's async execution workers.

An implementation shall use either:

- a runtime-supported asynchronous terminal reader; or
- one explicitly owned blocking-reader task/thread that forwards bounded
  complete input records to the async host.

Required properties:

- input buffering is bounded by bytes and pending submissions;
- blocked reads wake or terminate during shutdown;
- each reader task has one owner and one terminal outcome;
- no detached input task survives the adapter;
- a slow terminal cannot block Connector, Interpreter, Loop, or event delivery;
- active runs do not share mutable prompt buffers; and
- simultaneous submissions remain identity-isolated.

## 12. Backpressure

When the bounded submission queue is full, Console Input MUST stop admitting
additional complete prompts or reject them explicitly.

It MUST NOT:

- create unbounded tasks;
- overwrite an older pending submission;
- merge two prompts;
- partially submit a prompt;
- block the runtime globally; or
- report acceptance before the host has accepted ownership.

Interactive mode should make backpressure visible with a concise diagnostic.

## 13. Error vocabulary

The initial closed error vocabulary is:

```text
input_cancelled
input_closed
invalid_utf8
empty_prompt
prompt_too_large
submission_queue_full
channel_not_configured
channel_not_found
request_identity_conflict
host_rejected
reader_failed
shutdown_timeout
internal_invariant_failed
```

Whether an empty prompt is allowed is explicit configuration. The default is
rejection.

Errors identify the input/submission operation safely. They MUST NOT include
the complete prompt, credentials, environment contents, or provider secrets.

## 14. Security and terminal safety

Terminal input is untrusted data.

Console Input:

- treats prompt text as data, never a shell command;
- performs no command substitution or environment expansion;
- does not echo prompt contents into structured logs by default;
- limits prompt bytes before allocation grows without bound;
- does not persist terminal history itself;
- does not claim the surrounding shell or terminal emulator has no history;
- zeroizes buffers only where a later explicit sensitive-input mode requires
  it; and
- emits safe length/digest/identity diagnostics instead of raw text.

## 15. Observability

Safe diagnostics may include:

```text
submission_id
request_id
selected_channel_id
input mode
prompt byte count
read duration
queue wait duration
accepted/rejected/cancelled outcome
safe error kind
```

Prompt content is not included by default.

Console Input does not report model, Connector, Interpreter, Loop, or tool
outcomes as its own outcome.

## 16. Test seam

Terminal access is abstracted:

```rust
pub trait ConsoleReader: Send {
    fn next_input(
        &mut self,
        control: InputReadControl,
    ) -> ConsoleReadFuture;
}
```

Conformance tests use a deterministic fake reader. Tests MUST NOT require a real
TTY, a shell, or a configured LLM.

The fake must model:

- chunked byte arrival;
- delayed newline/EOF;
- invalid UTF-8;
- cancellation while blocked;
- reader failure;
- oversized input; and
- several independently submitted prompts.

## 17. Required tests

### 17.1 One-shot input

- bytes until EOF produce exactly one submission;
- the EOF delimiter is not added to canonical text;
- an empty stream follows configured empty-input policy;
- second submission cannot appear after one-shot completion.

### 17.2 Interactive input

- each submitted line creates exactly one fresh request;
- two lines never become one prompt;
- one line never becomes two prompts;
- line delimiter handling is deterministic;
- each request uses the Channel selected at its submission boundary.

### 17.3 Fidelity and identity

- Unicode and Markdown survive unchanged;
- shell-like text remains inert text;
- every submission has unique request/submission identity;
- no previous prompt or response is present;
- no provider-native field appears.

### 17.4 Async and isolation

- a blocked reader does not block active runs;
- several accepted submissions create several independent runs;
- cancellation targets the exact identified run;
- cancelling input does not cancel an unrelated run;
- queue and byte bounds are enforced under load;
- shutdown owns and joins the reader.

### 17.5 Boundaries

- no outbound dialect encoder is imported or implemented here;
- no Connector, Interpreter, Loop, renderer, database, host runtime, or product UI
  implementation dependency is reachable;
- no prompt augmentation or Channel-selection policy is reachable;
- no filesystem or persistence path is reachable; and
- no global current run/session variable exists.

## 18. Acceptance criteria

The Console Input test adapter is accepted only when:

1. one complete terminal submission creates one complete canonical request;
2. every request explicitly names one preconfigured Channel;
3. prompt meaning and text are preserved without hidden augmentation;
4. each submission creates a new independent Monoloop run;
5. no conversation/session/history state exists;
6. input and active-run cancellation are explicit and identity-safe;
7. terminal reading cannot block the async runtime;
8. all buffers and queues are bounded;
9. Console Input contains no provider encoding, interpretation, tools,
   rendering, persistence, routing, or prompting logic;
10. deterministic non-TTY tests prove input framing, fidelity, isolation,
    cancellation, and shutdown; and
11. none of the three Monoloop product components depends on Console Input.

## 19. Initial deliverable

The initial implementation contains only:

- the canonical submission types;
- one-shot stdin framing;
- interactive single-line framing;
- explicit Channel selection supplied by test configuration;
- bounded asynchronous handoff;
- exact-run control registration in the test harness;
- deterministic fake-reader qualification; and
- a thin executable that composes Console Input, the test Driver, the selected
  Monoloop components, and Console Renderer.

It contains no terminal UI framework, history, completion, rich editing,
configuration editor, slash commands, or application integration.

## 20. Final rule

> Console Input is disposable test scaffolding. It submits complete canonical
> prompts; it never becomes the place where Monoloop product behavior hides.
