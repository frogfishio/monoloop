# Transaction Runtime v2 Specification

**Status:** Normative replacement specification; M0–M5 landed (D-003; D-042;
D-043; D-044 Fixed) — Ready units feed canonical `DefaultLoopRuntime` under
`TaskClass::LoopRuntime`; `StickyCancel`; oneshot take for completion/output;
typed `try_new_process_isolated`; Busy supervisor retry; ambient `start`
cfg-gated; MCP loopback listener as `RuntimeService`; sessionless DirectLlm
tool envelopes use transaction-scoped `SessionKey` (DECISIONS D-004). **M6 partial:** short `wait_stopped` TimedOut→Quiescing then Stopped; Seal
prefers authoritative session over synthetic. Remaining: full §22 adversarial
matrix (subprocess barriers), full MCP gateway + non-empty tools (deferred),
**M7** façade cutover.

**Scope:** Component 3 transaction lifecycle and its Connector/tool ownership seams

**Supersedes:** Lifecycle, admission, callback, task-ownership, finalization, and
shutdown portions of `TRANSACTION_RUNTIME_DESIGN.md` and
`TRANSACTION_RUNTIME_IMPLEMENTATION.md`

**Preserves:** Connector → Interpreter → Loop, canonical input/events, Channel and
transaction identity, configuration merging, bounded resources, MCP capability
routing, and provider-neutral tool semantics

## 1. Purpose

This specification replaces the rejected transaction lifecycle implementation.
The old implementation attempted to guarantee all of the following at once:

- synchronous non-blocking submission;
- externally owned executor lifetime;
- arbitrary in-process event, callback, and tool futures;
- hard shutdown deadlines;
- no detached work; and
- exactly-once callback invocation.

Those guarantees cannot all be satisfied by Rust futures. Dropping or aborting a
Tokio task does not preempt a future that does not yield. A caller-provided trait
method can block before it returns a future. An externally owned executor can stop
after admission. A finite shutdown deadline cannot prove teardown of an arbitrary
in-process operation.

Runtime v2 therefore defines guarantees that can be implemented and tested. It
separates:

1. terminal decision;
2. notification publication;
3. resource teardown;
4. host-side callback execution; and
5. the time a shutdown caller is willing to wait.

## 2. Normative language

`MUST`, `MUST NOT`, `SHOULD`, and `MAY` are normative.

An **owned task** is a task whose join handle is retained by the runtime task
supervisor until the task completes. An aborted task is still owned until its join
result is observed.

An **admitted transaction** is a transaction whose ledger entry and delivery
ports have been installed and whose start command has been accepted by the
supervisor queue. Admission does not mean that provider I/O has started.

**Publication** means successful insertion into a library-created bounded channel.
It does not mean that arbitrary host code has processed the value.

## 3. Guarantee precedence

When guarantees compete, the runtime MUST apply this precedence:

1. memory safety and correlation integrity;
2. no new transaction work after admission closes;
3. one immutable terminal decision per admitted transaction;
4. continued ownership of every unfinished runtime task and isolated process;
5. exactly one completion publication attempt;
6. bounded memory and concurrency;
7. bounded waiting by API callers; and
8. best-effort cooperative cleanup.

Consequences:

- A shutdown wait deadline bounds the wait operation, not physical teardown.
- Deadline expiry MUST NOT cause the runtime to drop a live join handle.
- Deadline expiry MUST NOT cause the runtime to report `Stopped`.
- The runtime MAY remain `Quiescing` after a timed-out wait while its owner
  continues teardown.
- A non-cooperative in-process tool can prevent `Stopped`. It cannot be described
  as hard-killable.
- Exactly-once applies to completion publication into a one-shot mailbox. The core
  runtime does not guarantee execution of arbitrary callback code.

## 4. Preserved architecture

The product remains three components:

```text
CanonicalInput
    -> selected Channel / outbound encoder
    -> Connector
    -> raw dialect bytes
    -> Interpreter
    -> complete CanonicalUnitEvent values
    -> Loop transaction coordinator and tool state machine
    -> bounded event and completion mailboxes
```

Responsibilities remain:

- **Connector:** transport, authentication, external-session routing, ordered raw
  bytes, transport control, and transport terminal result.
- **Interpreter:** incremental dialect decoding and complete canonical units.
- **Loop:** admission, correlation, transaction state, event sequencing, tool
  execution, continuation, terminal selection, and resource ownership.

The runtime MUST use one canonical tool state machine. Production transaction
execution MUST either invoke `DefaultLoopRuntime` or replace it with the
transaction coordinator's state machine. Two independent production tool state
machines are forbidden.

## 5. Explicit non-goals

Runtime v2 does not guarantee:

- callback recovery after process loss;
- durable event delivery;
- hard termination of arbitrary Rust futures or OS threads;
- completion processing by a receiver that has been dropped;
- cleanup after the host leaks or forcibly forgets the runtime owner;
- provider-side deletion of externally owned sessions; or
- successful shutdown when a registered cooperative operation violates its
  contract and never yields.

## 6. Public delivery contract

### 6.1 Remove arbitrary sinks and callbacks from the core

The following core request fields and traits MUST be retired:

```rust
Arc<dyn TransactionEventSink>
Box<dyn CompletionCallback>
```

They MAY remain temporarily in a compatibility crate or façade adapter, but the
core runtime MUST NOT invoke those traits.

The core uses concrete library-created Tokio mailboxes:

```rust
pub struct TransactionDelivery {
    event_tx: TransactionEventSender,
    completion_tx: TransactionCompletionSender,
}

pub struct TransactionReceiver {
    pub events: TransactionEventReceiver,
    pub completion: TransactionCompletionReceiver,
}

pub fn transaction_delivery(
    limits: DeliveryLimits,
) -> Result<(TransactionDelivery, TransactionReceiver), DeliveryConfigError>;
```

Requirements:

- `TransactionEventSender` wraps a bounded `tokio::sync::mpsc::Sender` plus byte
  accounting.
- `TransactionCompletionSender` wraps a one-shot sender.
- Constructors MUST validate nonzero item and byte capacities.
- Sender internals MUST not be publicly replaceable with user implementations.
- Completion send MUST be non-blocking and consume the sender exactly once.
- Dropping a receiver is an observable delivery failure, not a lifecycle leak.
- Host callback adapters drain these receivers outside the runtime ownership
  boundary.

### 6.2 Request and receipt

```rust
pub struct TransactionRequest {
    pub channel_id: ChannelId,
    pub session_id: Option<SessionId>,
    pub input: CanonicalInput,
    pub session_config: Option<SessionConfig>,
    pub invocation_config: InvocationConfig,
    pub tools: Vec<ToolId>,
    pub delivery: TransactionDelivery,
}

pub struct AdmissionReceipt {
    pub transaction_id: TransactionId,
    pub session_id: Option<SessionId>,
}
```

The caller constructs delivery ports before submission and therefore cannot miss
an event published immediately after admission.

### 6.3 Completion semantics

For every admitted transaction, the runtime MUST make exactly one call to the
internal one-shot completion sender.

Possible send results are:

- `Published`: receiver existed and accepted the result;
- `ReceiverDropped`: the host dropped its receiver; or
- `InvariantFailed`: the sender was unexpectedly absent or previously consumed.

The transaction is finalized after the send attempt, regardless of whether the
receiver still exists. A host-side callback may run zero, one, or more times if a
host adapter is incorrectly implemented; that is outside the core guarantee.

### 6.4 Event delivery semantics

Ordinary events are lossless while the event receiver remains open and the
transaction deadline permits waiting for capacity.

- Event queue item and byte limits MUST both be enforced.
- Ordinary event enqueue is bounded by the remaining transaction deadline.
- `Ended` enqueue is bounded by an independent terminal-event budget.
- No event may be enqueued after the terminal enqueue attempt.
- A failed `Ended` enqueue MUST be recorded in the completion result.
- `TransactionEnd` inside `Ended` MUST NOT contain the outcome of its own
  delivery. That outcome is unknowable until after the event has been sent.

Replace the self-referential event field with:

```rust
pub struct TransactionEndEvent {
    pub transaction_id: TransactionId,
    pub session_id: Option<SessionId>,
    pub channel_id: ChannelId,
    pub kind: TransactionEndKind,
    pub emitted_events: u64,
    pub usage: TransactionUsage,
    pub diagnostics: Vec<TransactionDiagnostic>,
}

pub struct TransactionCompletion {
    pub end: TransactionEndEvent,
    pub terminal_event_delivery: TerminalEventDelivery,
    pub cleanup: CleanupStatus,
}
```

## 7. Runtime ownership

### 7.1 Runtime owner and handle

Runtime v2 separates the unique owner from cloneable control handles:

```rust
pub struct RuntimeOwner { /* executor and supervisor ownership */ }
#[derive(Clone)]
pub struct TransactionRuntimeHandle { /* command/admission handles only */ }

pub struct StartedRuntime {
    pub owner: RuntimeOwner,
    pub handle: TransactionRuntimeHandle,
}
```

`DefaultTransactionRuntime::start` MUST return `StartedRuntime` only after the
supervisor, registries, Connector instances, and optional MCP gateway are ready.

`RuntimeOwner` owns:

- the dedicated Tokio executor;
- the supervisor task;
- the supervisor OS thread join handle, when a dedicated thread is used;
- the lifecycle ledger;
- the task supervisor;
- all runtime-wide permits;
- the MCP gateway;
- realized Connector instances; and
- shutdown completion state.

`TransactionRuntimeHandle` MUST NOT own executor shutdown authority.

### 7.2 Executor ownership

`RuntimeBootstrap.executor: tokio::runtime::Handle` MUST be removed from the
production constructor.

The production runtime MUST construct and own its executor. Recommended shape:

- one dedicated supervisor OS thread;
- one owned multi-thread Tokio runtime created on that thread;
- an initialization handshake back to `start`; and
- a retained OS thread `JoinHandle` owned by `RuntimeOwner`.

Tests MAY inject a deterministic executor through a test-only constructor. The
public production API MUST not accept a bare external Tokio handle.

The runtime MUST never run caller event or completion code on this executor.

### 7.3 Task ownership

All runtime tasks MUST be spawned through one `TaskSupervisor`.

```rust
pub struct TaskId(u64);

pub enum TaskClass {
    TransactionCoordinator(TransactionId),
    EventPublisher(TransactionId),
    ConnectorOwner(TransactionId, ExchangeId),
    InterpreterOwner(TransactionId, ExchangeId),
    ToolWorker(TransactionId, ToolExecutionId),
    McpRequest(TransactionId),
    RuntimeService,
}

pub struct TaskSupervisor {
    joins: tokio::task::JoinSet<TaskExit>,
    by_transaction: HashMap<TransactionId, BoundedSet<TaskId>>,
}
```

Rules:

- No transaction registry entry stores a `JoinHandle`.
- No transaction task owns the join handle for itself.
- No reaper task is permitted.
- A task is registered before its start gate is released.
- Dropping a live `JoinHandle` is forbidden.
- Aborting a task changes its state to `AbortRequested`; it remains registered
  until its join result is observed.
- Every spawn site MUST identify a `TaskClass` and owner transaction.
- Ambient `tokio::spawn` is forbidden in lifecycle, exchange, tool, MCP, and
  Connector owner paths.

## 8. Lifecycle ledger

### 8.1 Purpose

The ledger is the runtime's source of truth from admission through completion
publication. It replaces `ActiveTransactionRegistry` and `FinalizationGuard`.

```rust
pub struct LifecycleLedger {
    by_transaction: HashMap<TransactionId, LedgerEntry>,
    by_session: HashMap<SessionKey, TransactionId>,
}

pub struct LedgerEntry {
    pub transaction_id: TransactionId,
    pub channel_id: ChannelId,
    pub session_key: Option<SessionKey>,
    pub phase: TransactionPhase,
    pub terminal: Option<TerminalDecision>,
    pub event_sequence: u64,
    pub delivery: Option<TransactionDelivery>,
    pub reservations: TransactionReservations,
    pub resources: ResourceControls,
    pub usage: TransactionUsage,
    pub diagnostics: BoundedDiagnostics,
}
```

### 8.2 Phases

```rust
pub enum TransactionPhase {
    Queued,
    EstablishingSession,
    Running,
    Cancelling,
    Finalizing,
    CompletionPublished,
    CleanupPending,
}
```

`CompletionPublished` is a short-lived ledger tombstone when cleanup is already
complete. `CleanupPending` is a potentially longer-lived tombstone after the
completion result has been published but owned work remains. The entry is
removed only after:

1. all transaction tasks have joined and all isolated processes have been
   reaped;
2. routes and capabilities are revoked;
3. the terminal event enqueue has been attempted;
4. the completion send has been attempted; and
5. reservations have been released according to policy.

There MUST be no interval in which an admitted but not completion-published
transaction has no ledger entry.

### 8.3 Mutation authority

The supervisor is the sole asynchronous mutator of ledger entries after
admission. Synchronous admission may create a `Queued` entry while holding the
short admission lock.

Workers submit bounded `SupervisorCommand` values. They MUST NOT:

- remove ledger entries;
- select or publish final completion;
- allocate terminal event sequence numbers;
- release transaction reservations; or
- directly mutate session routing.

## 9. Synchronous admission

### 9.1 Admission properties

`submit` MUST:

- perform no network, filesystem, process, Connector, Interpreter, tool, event,
  completion, or executor operation;
- never wait for a spawned task to be polled;
- use only bounded validation, lookup, allocation, short mutex sections, permit
  acquisition, and bounded `try_send`;
- return a typed error without event or completion publication when rejected; and
- return only after a complete `Queued` ledger entry and start command exist.

### 9.2 Admission algorithm

The normative order is:

1. Check the atomic runtime state is `Accepting`.
2. Validate input, configuration, deadline, tools, and delivery-port ownership.
3. Resolve Channel and immutable tool definitions.
4. Compute effective configuration.
5. Allocate `TransactionId` and any immediately known `SessionId`/`SessionKey`.
6. Construct all bounded internal mailboxes and RAII reservation objects.
7. Acquire global, Channel, event, tool, and ledger capacity without waiting.
8. Lock the admission/ledger critical section.
9. Recheck runtime state under that lock.
10. Reject duplicate or excess `SessionKey` use.
11. Insert a complete `Queued` `LedgerEntry`.
12. `try_send(SupervisorCommand::Start(transaction_id))` while rollback remains
    possible.
13. On queue failure, remove the entry and drop all RAII reservations.
14. Unlock and return `AdmissionReceipt`.

The supervisor start queue capacity MUST be at least
`max_active_transactions`. Queue capacity is validated at startup.

No task is spawned during this algorithm.

### 9.3 Capacity

All capacity acquisitions MUST return RAII permits. Counter-only acquire/release
APIs are forbidden because they permit double release, forgotten release, and
underflow.

```rust
pub struct TransactionReservations {
    global: OwnedPermit,
    channel: OwnedPermit,
    ledger: OwnedPermit,
    event_bytes: ByteBudget,
}
```

`mem::forget` is forbidden in production lifecycle code.

## 10. Supervisor commands

The command vocabulary is closed and bounded:

```rust
pub enum SupervisorCommand {
    Start(TransactionId),
    ClaimSession {
        transaction_id: TransactionId,
        session_key: SessionKey,
        reply: oneshot::Sender<Result<(), SessionClaimError>>,
    },
    WorkerExited {
        transaction_id: TransactionId,
        proposal: TerminalProposal,
    },
    PublisherFailed {
        transaction_id: TransactionId,
        failure: EventPublicationFailure,
    },
    Cancel {
        selector: TransactionSelector,
        reason: CancellationReason,
    },
    ForceTerminate {
        selector: TransactionSelector,
        reason: TerminationReason,
    },
    DeadlineExpired(TransactionId),
    DeliveryFailed(TransactionId),
    TaskExited(TaskExit),
    BeginShutdown,
}
```

Variable-sized commands MUST reserve an item slot and byte budget before send.
Canonical units larger than configured command/event limits fail the transaction
as `LimitExceeded`; they are not truncated.

## 11. Transaction coordinator

One coordinator worker executes transaction business logic. It owns mutable
provider/tool continuation state, but not lifecycle finalization.

It MAY:

- attach/create a session through the selected adapter;
- request atomic session claim from the supervisor;
- run provider exchanges;
- receive complete Interpreter units;
- advance the single canonical tool state machine;
- dispatch permitted tools;
- create inline continuations; and
- return a `TerminalProposal`.

It MUST NOT publish completion or remove routing.

Every blocking phase is raced against the transaction cancellation token and
deadline. Losing a race requests child termination but does not imply the child
has joined.

## 12. Event sequencing

Each admitted transaction has one `EventPublisher` task registered with
`TaskSupervisor`. Connector, Interpreter, MCP, and tool tasks report internal
messages to the coordinator. The coordinator is the sole sender of ordinary
`EventPublisherCommand::Publish` commands. The supervisor is the sole sender of
`EventPublisherCommand::Seal`.

```rust
pub enum EventPublisherCommand {
    Publish(TransactionEventPayload),
    Seal {
        terminal: TransactionEndEvent,
        reply: oneshot::Sender<TerminalPublicationResult>,
    },
}
```

The event publisher—not the global supervisor—may wait for external event queue
capacity. A slow event consumer therefore backpressures only its transaction.
The event publisher reports queue closure, deadline, limit failure, and its last
committed sequence to the supervisor through bounded internal commands.

Sequence rules:

- first ordinary event sequence is 1;
- allocation occurs only after queue capacity and byte capacity are reserved;
- allocation and enqueue are one serialized operation;
- failed enqueue does not consume a sequence;
- new external sessions publish `SessionEstablished` before every other ordinary
  event;
- after the coordinator joins, the supervisor sends `Seal`; the event publisher
  alone allocates and attempts the terminal event; and
- no sequence can be allocated after the terminal attempt.

The last committed sequence is reported to and stored in the ledger, not only in
publisher memory. Once `Seal` is accepted, later `Publish` commands are rejected
without sequence allocation. The supervisor command loop MUST NOT await a caller
event queue directly.

## 13. Terminal selection

### 13.1 One decision

The supervisor is the only terminal authority. The first accepted terminal
trigger selects the primary cause, with one permitted upgrade:

- `Cancel` may be upgraded to `Terminated` if force termination is accepted
  before terminal commit.

Otherwise, later failures become bounded diagnostics or cleanup status. Runtime
shutdown MUST NOT rewrite a terminal cause already selected.

Examples:

- provider failure selected before shutdown remains `ConnectorFailed`;
- shutdown selected before normal completion remains `RuntimeShutdown`;
- a cleanup join timeout does not replace `Cancelled`; it sets cleanup status;
- terminal event delivery failure does not replace the primary cause; it is
  reported separately in completion; and
- an invariant discovered before any terminal selection selects
  `InvariantFailed`.

`prior_terminal_cause` SHOULD be removed after migration because the rules no
longer rewrite ordinary causes.

### 13.2 Finalization algorithm

For one ledger entry, the supervisor MUST:

1. Set phase to `Finalizing` and freeze the terminal decision.
2. Reject further ordinary `Emit` and tool-start commands.
3. Revoke MCP capability and session routes for this transaction.
4. Signal coordinator, Connector, Interpreter, and tools according to their
   execution class.
5. Await or observe joins according to cleanup policy without dropping handles.
6. Record `CleanupStatus`.
7. Send `Seal` to the event publisher and receive its bounded terminal result.
8. Join the sealed event publisher or retain its ownership as cleanup pending.
9. Release the `SessionKey` reservation only if cleanup is complete.
10. Build immutable `TransactionCompletion`.
11. Consume the one-shot completion sender exactly once.
12. Mark `CompletionPublished` when cleanup is complete, otherwise mark
    `CleanupPending`.
13. Release normal transaction capacity. When cleanup is pending, transfer its
    cleanup permit, session reservation, and owned resources into the tombstone.
14. After cleanup completes, release retained reservations and remove the ledger
    tombstone.

If cleanup is still pending, steps 7–13 MAY occur before physical cleanup
completes, but the ledger tombstone, task ownership, and runtime-wide capacity
needed to bound that cleanup MUST remain. The same `SessionKey` MUST remain
reserved so later work cannot overlap residual effects from the prior
transaction. `CleanupStatus::Pending` makes this observable.

## 14. Tool execution classes

The existing cancellation names MUST be revised to describe real guarantees:

```rust
pub enum ToolExecutionClass {
    CooperativeInProcess { grace: Duration },
    AbortableAtYield { grace: Duration },
    ProcessIsolated { grace: Duration, kill_deadline: Duration },
}
```

### 14.1 CooperativeInProcess

- The tool receives a cancellation token.
- The runtime cannot force it to stop.
- Failure to join leaves cleanup pending and can prevent `Stopped`.
- It MUST run in a bounded tool executor isolated from transaction coordination
  threads.

### 14.2 AbortableAtYield

- The runtime owns a Tokio join handle and may call `abort`.
- Abort is effective only when the future yields.
- It MUST NOT be described as hard-killable.
- The permit remains held until join completion is observed.

### 14.3 ProcessIsolated

- The runtime owns a child process or equivalent killable isolation boundary.
- Control includes cooperative cancel, kill, and join/wait.
- A hard cleanup deadline is supportable only for this class, subject to OS
  process APIs.
- The process handle remains owned until wait/join is observed.

`IsolatedKillableToolHandler` MUST either be removed or reimplemented with a real
process boundary. A Tokio task is not an isolation boundary.

Handler capability booleans are insufficient. Registration MUST require a
structural execution factory for the declared class.

## 15. Connector ownership seam

The current Connector completion contract does not let the Loop prove task
teardown. Runtime v2 requires a joinable operation owner:

```rust
pub struct PendingRawConnection {
    pub connection_id: ConnectionId,
    pub control: ConnectionControlHandle,
    pub opened: OpenCompletion,
    pub owner: ConnectionOwnerHandle,
}

pub trait ConnectionOwnerHandle: Send + Sync {
    fn request_cancel(&self) -> ControlDisposition;
    fn request_terminate(&self) -> ControlDisposition;
    fn teardown_state(&self) -> TeardownState;
    fn join(&self) -> ConnectionJoin;
}
```

The exact Rust representation may differ, but the following are mandatory:

- open-owner identity exists before I/O starts;
- the Loop can request control while open is pending;
- the owner has one join completion independent of transport semantic completion;
- transport completion does not imply owner join unless explicitly documented;
- profile connectors use a shared spawn/ownership abstraction; and
- every process stdout/stderr pump and pending RPC waiter is included in owner
  teardown.

ACP stdio connectors SHOULD share one common bounded process/NDJSON core instead
of copying lifecycle code across profiles.

## 16. Exchange ownership seam

`exchange.rs` must be adapted; it cannot continue accepting a Tokio `Handle` or
`SpawnGate`.

New exchange APIs receive:

```rust
pub struct ExchangeContext<'a> {
    pub transaction_id: TransactionId,
    pub exchange_id: ExchangeId,
    pub tasks: &'a TransactionTaskSpawner,
    pub cancellation: &'a CancellationToken,
    pub deadline: Instant,
    pub limits: &'a EffectiveExchangeLimits,
}
```

Exchange rules:

- the task supervisor owns all pump, Connector, Interpreter, and fan-out joins;
- an exchange result is not terminal until required child joins are observed or
  cleanup is recorded pending;
- raw output is streamed into Interpreter incrementally;
- complete units are forwarded incrementally and retained only when continuation
  policy requires bounded context;
- one absolute exchange deadline covers open, send, receive, and interpretation;
- dropping the exchange future is not a cleanup mechanism; and
- provider and session identity are checked before prompt transmission.

## 17. MCP ownership

MCP routes remain transaction-scoped capabilities.

- Pending route installation may occur before external session creation when a
  CreationOnly profile requires it.
- Activation occurs only after authoritative `SessionKey` claim.
- Revocation begins at terminal selection.
- Every active MCP request is registered as an owned task.
- Shutdown cannot report `Stopped` while MCP listener/request tasks remain.
- Process-global mutable service maps are forbidden; services belong to one
  `RuntimeOwner`.

## 18. Runtime lifecycle and shutdown

### 18.1 States

```rust
pub enum RuntimeState {
    Starting,
    Accepting,
    Quiescing,
    Stopped,
}
```

`Draining` is renamed `Quiescing` to make clear that shutdown may be incomplete.

### 18.2 API

```rust
impl RuntimeOwner {
    pub fn begin_shutdown(&self) -> ShutdownTicket;
    pub async fn wait_stopped(
        &mut self,
        deadline: Duration,
    ) -> ShutdownWaitOutcome;
}

pub enum ShutdownWaitOutcome {
    Stopped(ShutdownReport),
    TimedOut(ShutdownSnapshot),
}
```

`begin_shutdown` is idempotent and synchronously transitions admission to
`Quiescing` under the same lock used by admission install.

`wait_stopped` may time out. On timeout:

- state remains `Quiescing`;
- the owner retains all task/process joins;
- cleanup continues;
- callers may call `wait_stopped` again; and
- the snapshot reports remaining transactions, tasks, tools, processes, routes,
  and completion publications.

Only `Stopped` guarantees:

- zero ledger entries;
- zero owned Tokio tasks;
- zero owned child processes;
- zero MCP routes and requests;
- zero held transaction/tool/event permits;
- listener closed;
- supervisor task joined; and
- executor and its OS thread joined.

### 18.3 Shutdown algorithm

The supervisor MUST:

1. Atomically close admission.
2. Snapshot ledger IDs, not join handles.
3. Select `RuntimeShutdown` only for transactions without a prior terminal cause.
4. Signal all transactions as a group.
5. Revoke routes and prevent new tool/exchange starts.
6. Continue reaping the global task supervisor.
7. Kill `ProcessIsolated` work after its grace.
8. Abort `AbortableAtYield` work after its grace, retaining joins.
9. Continue waiting for `CooperativeInProcess` work without pretending it is
   stopped.
10. Publish every outstanding completion once its terminal record is available.
11. Close MCP and runtime services.
12. Enter `Stopped` only after the stopped invariants hold.

Concurrent shutdown callers share one shutdown generation and observe compatible
snapshots. They do not create competing shutdown leaders.

### 18.4 Owner drop

`RuntimeOwner` MUST be annotated `#[must_use]`. The supported lifecycle is an
explicit `begin_shutdown` followed by `wait_stopped` until `Stopped`.

Dropping `RuntimeOwner` before `Stopped` is a contract violation. Its `Drop`
implementation MUST still preserve ownership: it initiates shutdown and joins the
supervisor/executor thread. This fallback MAY block indefinitely when
non-cooperative in-process work remains. It MUST NOT detach the executor thread,
drop a live thread join handle, or report a successful stop.

Applications that require a bounded process-exit time MUST use only
`ProcessIsolated` untrusted work and complete explicit shutdown before dropping
the owner.

## 19. Failure and cleanup reporting

```rust
pub enum CleanupStatus {
    Complete,
    Pending {
        owned_tasks: u32,
        owned_processes: u32,
        cooperative_tools: u32,
    },
    Failed {
        code: CleanupFailureCode,
    },
}

pub enum TerminalEventDelivery {
    Published,
    QueueClosed,
    DeadlineExceeded,
    LimitExceeded,
}
```

Cleanup failure never becomes silent success. It also does not erase the primary
transaction cause.

Diagnostics remain closed, bounded, safe, and content-free. Join panics and
contract violations map to stable diagnostic codes rather than arbitrary panic
strings.

## 20. Proposed module layout

Create:

```text
src/transaction/lifecycle/
    mod.rs                 public assembly and exports
    owner.rs               RuntimeOwner / handle / executor ownership
    supervisor.rs          command loop and terminal authority
    task_supervisor.rs     JoinSet, TaskId, TaskClass, reaping
    ledger.rs              lifecycle ledger and session indices
    admission.rs           synchronous validation/install/rollback
    coordinator.rs         per-transaction business state machine
    delivery.rs            concrete event/completion mailboxes
    terminal.rs            terminal decision and completion construction
    shutdown.rs            quiescing, snapshots, stopped proof
    capacity.rs            RAII transaction reservations
```

Adapt:

- `bootstrap.rs`: return owner+handle and remove external executor.
- `events.rs`: replace arbitrary sink delivery with concrete mailbox delivery.
- `exchange.rs`: use supervised task spawning and joinable Connector ownership.
- `dispatcher.rs`: use one canonical Loop state machine and structural execution
  classes.
- `tool_handler.rs`: remove fake isolation and ambient spawning.
- `mcp/*`: register request tasks with the runtime task supervisor.
- `state.rs`: use `Quiescing` and stopped proof.
- `capacity.rs`: return RAII permits rather than boolean counters.
- `monoloop-contracts/src/transaction.rs`: delivery ports, completion structure,
  and shutdown outcomes.
- `monoloop-connector`: add operation-owner join semantics.

Delete or permanently retire:

- `active_registry.rs`;
- `spawn_gate.rs`;
- callback-based core traits after compatibility migration;
- `FinalizationGuard` and any equivalent actor/supervisor shared CAS;
- reaper tasks;
- callback services inside the runtime; and
- tool join vaults. The task supervisor itself retains real worker joins.

## 21. Forbidden implementation patterns

Production lifecycle code MUST fail review if it contains:

- ambient `tokio::spawn`;
- a dropped live `JoinHandle`;
- `mem::forget` of capacity, callbacks, tasks, or resource owners;
- a transaction actor removing its own registry/ledger entry;
- a callback or event trait invocation inside runtime tasks;
- timeout followed by dropping a join handle;
- a hard-kill claim for an in-process future;
- a bare externally owned Tokio `Handle` in production bootstrap;
- an event that reports the result of its own delivery;
- `Stopped` while any owned work remains;
- conditional tests that accept fewer completions than admitted transactions; or
- multiple independent production tool state machines.

## 22. Acceptance tests

The following deterministic tests are mandatory before v2 replaces the public
runtime.

### 22.1 Admission

- Submission from a plain OS thread returns without entering a Tokio context.
- A deliberately parked runtime worker cannot delay synchronous admission.
- Start queue full rolls back ledger, session, delivery, and every permit.
- Submit versus `begin_shutdown` has only two outcomes: fully rejected or fully
  admitted and present in the shutdown ledger.
- Duplicate session race admits exactly one transaction.
- Rejected admission publishes no event and no completion.

### 22.2 Finalization

- Every admitted transaction performs exactly one completion send attempt.
- Receiver dropped before completion is accounted without a task leak.
- Coordinator panic produces one `InvariantFailed` completion.
- Coordinator completion racing cancel produces one documented cause.
- Cancel upgraded to force terminate produces one `Terminated` completion.
- Shutdown between terminal-event attempt and completion send cannot lose the
  ledger entry or completion.
- No event appears after terminal attempt.
- Failed event enqueue consumes no sequence.

### 22.3 Task ownership

- Every spawn registers a task before first poll.
- Abort is followed by observed join before stopped proof.
- A yielding abortable future is aborted and joined.
- A non-yielding future, run in a sacrificial test process, causes shutdown wait
  timeout and `Quiescing`, never false `Stopped`.
- Dropping an exchange coordinator does not detach Connector/Interpreter pumps.
- Task counts return to zero after normal, failure, cancel, and process-kill paths.

### 22.4 Tools

- Cooperative tool that acknowledges cancel joins normally.
- Cooperative tool that does not acknowledge keeps cleanup pending.
- Abortable-at-yield tool releases permits only after join.
- Process-isolated tool is killed and reaped after grace.
- A tool cannot self-assert a stronger execution class than its structural
  factory provides.
- Tool capacity remains unavailable while its worker is still owned.

### 22.5 Shutdown

- `wait_stopped(short)` returns `TimedOut` within tolerance and leaves state
  `Quiescing`.
- A later `wait_stopped(long)` returns the same generation's final report.
- N admitted transactions produce exactly N completion attempts during shutdown.
- Concurrent waiters receive compatible snapshots and one final report.
- `Stopped` asserts zero ledger entries, tasks, processes, routes, and permits.
- Repeated start/stop does not leak executor threads or listeners.

### 22.6 Events and identity

- `SessionEstablished` is sequence 1 for new external sessions.
- Multiple concurrent event producers cannot bypass the coordinator.
- Events are delivered in contiguous sequence order.
- Same session string on different Channels remains isolated.
- Reused provider tool-call IDs across exchanges remain distinct.
- Event byte and item plus-one tests fail closed.

### 22.7 Adversarial host behavior

Host event and completion adapters are tested outside the core:

- callback blocks before returning a future;
- callback future never yields;
- event consumer stops draining;
- receivers are dropped immediately; and
- host callback executor is destroyed.

None of these may block the runtime supervisor because the core only publishes to
concrete mailboxes.

## 23. Verification gates

All of the following must pass on a clean checkout:

```text
cargo fmt --all -- --check
cargo test --workspace --all-targets --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps
```

Additional gates:

- no forbidden-pattern search hits without documented exception;
- adversarial lifecycle tests run in isolated subprocesses with an outer test
  timeout;
- model checking or deterministic barrier tests cover admission/shutdown and
  terminal/cancel races;
- every public limit has an exact-limit and plus-one test; and
- an independent review finds no unresolved P0/P1/P2 lifecycle defect.

## 24. Migration plan

### M0 — Contract decision

1. Accept this specification.
2. Mark old runtime lifecycle documents as superseded for these sections.
3. Remove claims that the deleted runtime is release-proven.

### M1 — Delivery and shutdown contracts

1. Add concrete delivery mailboxes in `monoloop-contracts`.
2. Split terminal event data from completion delivery outcome.
3. Add `Quiescing`, `ShutdownWaitOutcome`, and snapshots.
4. Build host callback compatibility adapters outside the core.

### M2 — Owner, task supervisor, and ledger

1. Implement the owned executor and unique `RuntimeOwner`.
2. Implement `TaskSupervisor` and stopped proof.
3. Implement ledger entries, session indices, and RAII reservations.
4. Implement synchronous admission with start-queue rollback.

### M3 — Coordinator and events

1. Implement one transaction coordinator.
2. Stream Interpreter units through bounded internal commands.
3. Implement sequence allocation and terminal publication.
4. Integrate the single canonical Loop tool state machine.

### M4 — Connector and exchange ownership

1. Add Connector owner/join contract.
2. Migrate Fake and HTTP connectors first.
3. Extract and migrate the shared ACP process core.
4. Migrate remaining provider profiles.

### M5 — Tools and MCP

1. Replace cancellation policy names with execution classes.
2. Implement process isolation where hard kill is required.
3. Register MCP and tool workers with `TaskSupervisor`.
4. Remove join vaults and ambient spawn paths.

### M6 — Shutdown and adversarial proof

1. Implement quiescing and repeatable shutdown waits.
2. Add all adversarial acceptance tests.
3. Prove stopped invariants.
4. Run the full verification gates.

### M7 — Cutover and deletion

1. Move the public façade to runtime v2.
2. Port examples and testkit adapters.
3. Remove callback-based core APIs and compatibility aliases on the planned
   breaking-version boundary.
4. Delete `active_registry.rs`, `spawn_gate.rs`, obsolete event delivery, and the
   unused duplicate Loop implementation only after behavior is consolidated.

## 25. Definition of done

Runtime v2 is complete only when:

- submission performs no spawn and no executor wait;
- every admitted transaction is continuously represented in the lifecycle
  ledger until completion publication;
- every runtime task/process is owned until join/reap;
- exactly one completion publication attempt occurs per admission;
- arbitrary host callbacks and sinks do not execute in the runtime;
- shutdown timeout returns `Quiescing`, not false `Stopped`;
- only process-isolated work is described as hard-killable;
- production uses one canonical Loop tool state machine;
- Connector teardown is joinable and observable;
- all stopped invariants are mechanically checked; and
- every acceptance and verification gate above passes without conditional or
  weakened assertions.
