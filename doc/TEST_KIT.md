# Monoloop Test Kit and Driver

**Status:** Foundational test-infrastructure specification

**Product dependency status:** Prohibited

**Runtime:** Tokio multi-thread test runtime

**Exercises:**

- [Component 01 — Connector](CONNECTOR.md)
- [Component 02 — Interpreter](INTERPRETER.md)
- [Component 03 — The Loop](THE_LOOP.md)

**Optional adapters:**

- [Console Input](CONSOLE_INPUT.md)
- [Console Renderer](CONSOLE_RENDERER.md)

**Parent index:** [README.md](README.md)

The words **MUST**, **MUST NOT**, **REQUIRED**, **SHOULD**, **SHOULD NOT**, and
**MAY** are normative requirements.

---

## 1. Purpose

The test kit proves that the three Monoloop components work independently and
together. It supplies deterministic fixtures and a minimal Driver that wires
the components into executable conformance scenarios.

The Driver is not a fourth Monoloop component. It is disposable composition
scaffolding. A future production host may use different orchestration without
depending on this package.

## 2. Contents

```text
monoloop-testkit
    Driver
    deterministic Connector and remote fixtures
    outbound dialect encoder fixtures
    bounded event distributor fixture
    empty and deterministic tool fixtures
    Console Input adapter
    Console Renderer adapter
    conformance scenarios and assertions
```

The test kit may depend on all three product components. No product component
may depend on the test kit.

## 3. Driver responsibility

The Driver performs only test composition:

1. accept a complete test request and explicit Channel fixture;
2. acquire or create the requested external-session attachment;
3. open the Connector path;
4. perform test-profile outbound dialect encoding;
5. start the Interpreter on Connector output;
6. distribute canonical events through independent bounded subscriptions;
7. start the Loop with its lossless subscription;
8. observe component health and completion;
9. coordinate test cancellation and bounded teardown; and
10. return one truthful test-run report.

It does not define cognition, production routing, durable sessions, prompting,
memory, product UI, or business completion.

## 4. Test Driver interface

Conceptually:

```rust
pub trait TestDriver: Send + Sync {
    fn start(&self, request: TestRunRequest) -> Result<TestRun, TestDriverError>;
}

pub struct TestRun {
    pub run_id: TestRunId,
    pub events: TestEventSubscription,
    pub control: TestRunControl,
    pub completion: TestRunCompletion,
}
```

`start` returns immediately. All work proceeds asynchronously. The exact Rust
spelling may vary, but every task, queue, handle, and deadline has one explicit
owner.

## 5. Console adapters

Console Input and Console Renderer are optional human-facing test adapters.

```text
stdin -> Console Input -> Driver
Driver events -> Console Renderer -> stdout/stderr
```

They are not the Connector transport used by Grok Build. In particular, Grok
Build uses authenticated ACP/JSON-RPC over WebSocket. The words `stdin` and
`stdout` in the console adapter specifications refer only to the local test
harness.

Deleting both console adapters must leave all Connector, Interpreter, Loop, and
non-console conformance tests unchanged.

## 6. Event distribution fixture

The Driver supplies a bounded run-scoped distributor with independent
subscriptions:

```text
Interpreter canonical output
    +-> Loop subscription          lossless, gap-detecting
    +-> assertion subscription     lossless within declared fixture bounds
    +-> Console Renderer           best effort or lossless by test policy
```

Subscribers never compete for a single queue entry. Console loss cannot remove
an event from the Loop. Every gap, drop, detach, and terminal condition is
explicit and assertable.

## 7. Grok Build fixture composition

The initial real integration fixture connects to one configured Grok Build
server using the [Grok Build Network Connector](GROK_BUILD_CONNECTOR.md).

The Driver:

- never passes prompts through command-line arguments;
- never launches one Grok process per session;
- connects to the configured WebSocket endpoint;
- performs ACP `initialize` as required by the connection profile;
- sends session configuration through `session/new`;
- uses the Grok-returned `sessionId` as the session correlation identity;
- uses the outbound ACP fixture encoder to create complete `session/prompt`
  messages while the Connector assigns wire request IDs and session routing;
- exercises explicit `session/load` after reconnect when configured; and
- interprets streamed ACP updates only through the Interpreter.

The Driver may start the single Grok server as fixture setup through an injected
process supervisor, or attach to an already running authenticated instance. The
server lifecycle remains distinct from every Connector connection and logical
session.

## 8. Async and concurrency requirements

The Rust test kit uses a multi-threaded async runtime and MUST demonstrate:

- many Grok sessions progressing concurrently through one Grok server;
- non-blocking WebSocket reads and writes;
- independent per-session request queues and cancellation;
- serialized writes per WebSocket so frames cannot interleave;
- bounded queues, tables, buffers, and task counts;
- no mutex or write guard held across remote I/O;
- blocking process or filesystem work isolated from async workers;
- one slow session not blocking unrelated sessions;
- connection loss waking all affected waiters; and
- bounded teardown joining every task exactly once.

Concurrency across sessions is required. Prompts within one Grok session are
serialized unless the negotiated protocol explicitly declares concurrent
prompt mutation safe.

## 9. Required fixtures

The initial test kit includes:

```text
fragmenting byte transport
coalescing byte transport
blocked read/write transport
cancel/EOF race transport
deterministic ACP server
deterministic sentence/tool event streams
empty tool registry/runtime
bounded slow subscriber
failing console writer
Grok Build WebSocket integration fixture
```

Real Grok tests are integration tests and must be separable from deterministic
offline conformance tests.

## 10. Security

- Grok server secrets and external session identities are injected, never
  embedded in fixtures, snapshots, or source control.
- Real authenticated tests are opt-in and clearly identified.
- The default Grok server bind is loopback.
- Session configuration, prompts, responses, and tool payloads are absent from
  default logs and metric labels.
- Tests never print or snapshot authentication material.
- Permission requests are handled explicitly unless a test deliberately selects
  a separately declared automatic-approval profile.

## 11. Architecture tests

Automated dependency checks MUST prove:

- `monoloop-connector`, `monoloop-interpreter`, and `monoloop-loop` do not
  import `monoloop-testkit`;
- Console types do not appear in product component APIs;
- the Driver owns orchestration rather than leaking it into Connector,
  Interpreter, or Loop;
- deterministic fixtures can replace Grok without conditional product logic;
- no console adapter participates in runtime progress; and
- no unbounded channel or detached task exists in the test kit.

## 12. Governing rule

> The Driver, stdin, stdout, fixtures, and conformance orchestration exist to
> test the Connector, Interpreter, and Loop. They are not additional Monoloop
> components and cannot become production dependencies.
