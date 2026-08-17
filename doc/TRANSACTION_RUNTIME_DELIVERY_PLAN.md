# Transaction Runtime Delivery Plan

**Status:** Ready for engineering execution
**Requirements:** `REQUIREMENTS.md` R-000 through R-004
**Architecture:** `TRANSACTION_RUNTIME_DESIGN.md`  
**Normative implementation contract:** `TRANSACTION_RUNTIME_IMPLEMENTATION.md`

This plan converts the accepted architecture into reviewable engineering
deliveries. It does not replace the normative specification. If this plan and
the implementation specification differ, the implementation specification
wins and this plan must be corrected.

## 1. Delivery objective

Deliver one production transaction runtime that:

- accepts bounded, non-blocking transaction submissions;
- correlates transactions, Channel-scoped sessions, and provider exchanges;
- streams canonical events through a required push sink;
- invokes exactly one completion callback per admitted transaction;
- supports request-scoped linked tools through direct-LLM and MCP paths;
- supports OpenAI Chat Completions v1 over streaming HTTP/SSE;
- adapts all six existing external-agent profiles honestly;
- rejects unsupported Channel/session/tool combinations explicitly;
- shuts down without losing callbacks or leaking owned work; and
- satisfies the R-000 A- engineering gate.

The complete feature is not one large merge. It is delivered through the
ordered work packages below. Every merged package keeps the workspace green and
truthfully labels unimplemented later capabilities as unavailable.

## 2. Authority and scope

Developers must read these documents before starting:

1. `REQUIREMENTS.md`
2. `TRANSACTION_RUNTIME_DESIGN.md`
3. `TRANSACTION_RUNTIME_IMPLEMENTATION.md`
4. `MONOLOOP.md`
5. the component specification for the crate being changed

In scope:

- contracts, Channel bindings, runtime startup, admission, actor, exchanges;
- event sequencing, finalization, callbacks, cancellation, shutdown;
- linked tools, MCP, HTTP, Chat Completions, profile adapters;
- deterministic test infrastructure and conformance suites;
- migration from connector-local prompt shortcuts.

Out of scope:

- durable persistence or callback recovery after process loss;
- prompt construction, context compilation, memory, UI, or model routing;
- dynamic loading of tool executable code;
- OpenAI Responses, non-streaming Chat Completions, or multimodal input;
- remote MCP transport until separately specified and secured; and
- claiming reusable-session MCP parity for `CreationOnly` profiles.

## 3. Non-negotiable delivery rules

Every pull request must:

- compile and test independently;
- preserve the three-component boundary;
- contain no production `todo!`, `unimplemented!`, placeholder success, or
  knowingly unreachable advertised path;
- add direct tests for every behavior it introduces;
- include typed failures and enforced bounds in the same PR as the behavior;
- own and terminate every task, process, connection, route, and callback;
- avoid provider-name branching in shared transaction code;
- avoid test-kit dependencies from product crates;
- update affected specifications when implementation proves a contract wrong;
- leave unchecked any requirement not delivered end to end; and
- pass the workspace gate in §11.

Large mechanical moves are separate from behavioral changes. Refactors must not
be mixed with new protocol behavior unless the behavior cannot be introduced
through the existing seam.

## 4. Suggested team topology

The plan supports four parallel engineering workstreams after the contract
foundation merges:

### Runtime/kernel

Owns:

- runtime startup and shutdown;
- admission and active registries;
- actor, exchange ownership, event delivery, finalization, callbacks;
- integration of Connector, Interpreter, Loop, tools, and MCP handles.

### Connector/Channel

Owns:

- `ConnectorFactory`, matched Connector/SessionAdapter instances;
- session attachment and MCP configuration controls;
- generic HTTP Connector and credential resolution;
- external profile Channel bindings.

### Interpreter/dialect

Owns:

- Chat Completions SSE framing and assembly;
- canonical tool-call correlation;
- dialect qualification fixtures;
- malformed, fragmented, and oversized stream behavior.

### Tools/MCP/conformance

Owns:

- host tool registry, resolved sets, dispatcher, execution isolation;
- MCP gateway and transaction capability lifecycle;
- deterministic fake tools/MCP clients;
- cross-component race, parity, and security suites.

One technical owner must approve changes to public contracts and cross-crate
dependency direction. Test ownership remains with the engineer delivering each
behavior; conformance engineers supplement tests rather than replacing them.

## 5. Dependency graph

```text
WP-00 -> WP-01
WP-01 -> WP-02, WP-03
WP-02 + WP-03 -> WP-04
WP-04 -> WP-05
WP-05 -> WP-06, WP-08, WP-09
WP-06 -> WP-07
WP-06 + WP-08 + WP-09 -> WP-10
WP-05 + WP-06 + WP-07 -> WP-11
WP-10 + WP-11 -> WP-12
```

Parallel work:

- WP-02 and the internal parts of WP-03 can proceed after WP-01.
- WP-06, WP-08, and WP-09 can proceed in parallel after their contract subsets
  merge.
- External profile capability investigation can begin during WP-00, but profile
  production migration waits for WP-05 and WP-07 interfaces.

## 6. Work packages

### WP-00 — Baseline and dependency qualification

Purpose:

Prove the selected dependencies and current workspace are a safe starting point
before public contracts are expanded.

Deliver:

- record the current full workspace test/Clippy/doc baseline;
- add workspace dependencies through Cargo for `reqwest`, `rmcp`, required
  `axum`/`tower` integration, `jsonschema`, `secrecy`, and OS CSPRNG support;
- verify MSRV, licenses, enabled features, duplicate dependency impact, Rustls,
  and Streamable HTTP support;
- create a compile-only `rmcp` loopback server spike and remove spike code or
  promote it into a tested production seam;
- create the six-profile capability worksheet:
  - session create;
  - explicit session load/reuse;
  - MCP `None`, `CreationOnly`, or `Refreshable`;
  - loopback namespace reachability;
  - exchange mode;
  - supported continuation policy;
- identify every connector-local prompt shortcut requiring migration; and
- capture current component acceptance gaps without relabelling them complete.

Exit gate:

- selected dependencies compile at workspace MSRV;
- no unresolved license/security/MSRV blocker;
- every profile has evidence-backed capability declarations;
- every profile declaring Bidirectional is assigned SendAndRetain qualification
  in WP-05 and its WP-11 profile PR;
- the baseline suite is green or every pre-existing failure is recorded and
  assigned before feature work.

Recommended PR:

- PR 01: dependency qualification, capability evidence, and no product behavior.

### WP-01 — Provider-neutral contracts

Purpose:

Land stable types before runtime implementations depend on them.

Deliver in `monoloop-contracts`:

- `TransactionId`, `ExchangeId`, `SessionId`, `SessionKey`, `ChannelId`;
- `ToolId`, `ToolName`, and correlation keys;
- typed canonical system/user/assistant/tool messages;
- historical assistant tool calls and correlated tool results;
- `SessionConfig`, `InvocationConfig`, `EffectiveConfig`, extension bounds;
- Channel capability data and validation errors;
- transaction request, receipt, selector, event, terminal, and usage contracts;
- event sink, completion callback, shutdown future, cancellation/termination
  reasons;
- tool specification, output contract, canonical output/error/result;
- outbound encoder request/result contracts;
- all corresponding limits and error kinds; and
- `OpenAiChatCompletions` dialect descriptor.

Required tests:

- every newtype validation boundary;
- SessionKey isolation across equal session strings on different Channels;
- canonical message role/tool-reference validation;
- extension nesting/key/value/serialized-byte bounds;
- configuration merge precedence and immutable-session failures;
- tool schema/output contract construction;
- safe diagnostic redaction and bounds;
- serialization round trips where contracts are serializable; and
- architecture guard rejecting product-crate dependencies on
  `monoloop-testkit`.

Exit gate:

- contracts contain no dependency on Connector, Interpreter, Loop, or testkit;
- invalid values cannot be constructed through public APIs;
- all downstream crates compile after mechanical adoption;
- no behavioral runtime is falsely advertised.

Recommended PRs:

- PR 02: identities, canonical input, configuration, and limits.
- PR 03: transaction, Channel, encoder, and tool contracts.

### WP-02 — Connector factory and session ownership

Purpose:

Create the exact ownership seam required for external sessions and concurrent
Channels.

Deliver in `monoloop-connector`:

- `ConnectorInstanceId`;
- `ConnectorFactory` and `ConnectorInstance`;
- matched Connector/SessionAdapter construction;
- `SessionAttachRequest`, pending attachment/configuration handles and controls;
- `SessionAttachment` with owner, effective configuration, external ID, and
  opaque route;
- owner validation in `OpenConnection`;
- bounded cancellation and forced termination for session operations;
- concurrent distinct-session behavior; and
- deterministic fake implementations for every lifecycle phase.

Required tests:

- attachment from Connector instance A is rejected by instance B;
- supplied SessionId must equal returned ExternalSessionId bytes;
- attach/create/load cancellation and termination;
- dropped pending completion becomes an invariant failure;
- many distinct sessions operate concurrently within limits;
- same SessionKey is excluded before the Connector is invoked;
- one blocked session operation does not block unrelated sessions;
- immutable session configuration mismatch fails explicitly.

Exit gate:

- all existing profile crates mechanically compile against the ownership seam;
- no profile prompt is migrated yet unless covered by its final qualification;
- no async mutex is held across unrelated provider I/O.

Recommended PR:

- PR 04: abstract factory/session ownership plus fakes and migration shims.

### WP-03 — Runtime startup and Channel registry

Purpose:

Create a fully validated runtime that is either ready or unavailable—never
partially started.

Deliver in `monoloop-loop`:

- `RuntimeBootstrap`;
- asynchronous `DefaultTransactionRuntime::start`;
- immutable `ChannelRegistry`;
- immutable `HostToolRegistry` shell supporting empty tools initially;
- Channel capability validation matrix;
- one matched ConnectorInstance per Channel;
- global/per-Channel capacity managers;
- runtime states `Starting`, `Accepting`, `Draining`, `Stopped`;
- typed startup cleanup and `StartupError`; and
- MCP listener lifecycle shell without advertising tool methods before WP-07.

Required tests:

- every invalid Channel capability combination;
- duplicate Channel IDs and tool IDs;
- partial startup failure cleans every created service;
- no submission before `Accepting`;
- no submission after `Draining`;
- startup/shutdown repeated across many fake runtimes without leaked listeners.

Exit gate:

- a runtime with fake Channels starts and stops deterministically;
- no transaction submission exists until WP-04;
- startup performs all required validation before exposure.

Recommended PR:

- PR 05: runtime bootstrap, registries, state, and startup cleanup.

### WP-04 — Admission, event delivery, finalization, and callbacks

Purpose:

Establish the lifecycle invariants before real transport work is composed.

Deliver:

- synchronous bounded `submit`;
- dual active indexes by TransactionId and established SessionKey;
- generated direct-LLM session identities;
- provisional external-session admission;
- capacity reservation and rollback;
- control channel and transaction lookup;
- `FinalizationGuard` with atomic exactly-once claim;
- runtime-owned EventSequencer, bounded item/byte queue, delivery task;
- callback reservation and bounded invocation;
- per-request Channel/config/tool-mode capability validation;
- deterministic EffectiveConfig merge without I/O;
- requested ToolId resolution through the currently installed registry,
  including an immutable empty ResolvedToolSet;
- direct-LLM rejection of SessionConfig;
- terminal-event delivery deadline;
- ordinary sink-failure terminalization;
- `terminate(TransactionId | SessionKey, mode)`; and
- shutdown-supervisor finalization for aborted actors;
- transaction testkit foundations: fake Channel/session, callback and event
  recorders, barriers, and paused-time helpers.

Required tests:

- submit performs no I/O and returns while fake work is blocked;
- duplicate SessionKey rejection and cross-Channel same-string acceptance;
- unsupported Channel/config/tool-mode combinations;
- configuration merge precedence and direct-LLM SessionConfig rejection;
- empty tool resolution and typed rejection of unavailable non-empty sets before
  WP-06 installs linked tools;
- generated and supplied direct-LLM sessions;
- terminate before external session establishment;
- sink backpressure while control remains responsive;
- ordinary and final event-delivery failures;
- contiguous sequence including `Ended`;
- callback success, failure, panic, timeout, and exactly-one invocation;
- actor completion/cancel/deadline/forced-termination races;
- forced shutdown finalizes callbacks after actor abort;
- zero registry, permit, event task, or callback leaks.

Exit gate:

- fake no-I/O transactions terminalize exactly once through every race;
- shutdown accounts for every admitted FinalizationGuard;
- admission, event sequencing, callback, termination, and shutdown-supervisor
  behavior satisfy their applicable R-002 through R-004 lifecycle criteria.

Recommended PRs:

- PR 06: admission, registry, control, and capacity rollback.
- PR 07: event sequencing, finalization, callbacks, and shutdown races.

### WP-05 — Exchange driver and transaction actor

Purpose:

Compose real Connector and Interpreter handles while preserving per-exchange
isolation.

Deliver:

- explicit actor state transition function;
- fixed external attach-before-open ordering;
- no-op versus MCP tool-activation transitions;
- one ExchangeDriver per provider cycle;
- SendAndFinish request/response and qualified SendAndRetain bidirectional
  exchange behavior;
- fresh ExchangeId, ConnectionId, and InterpretationId per exchange;
- direct bounded RawOutputHandle-to-InterpretationInput pump;
- authoritative Connector/Interpretation terminal reconciliation;
- exchange canonical distributor;
- actor subscription and conditional inner-Loop subscription;
- stale exchange/child command rejection;
- child JoinSet ownership and panic reporting; and
- fake Channel end-to-end transaction completion.

Required tests:

- every allowed and invalid `(phase, command)` pair;
- cancellation/termination during session attach, open, send, receive,
  interpretation, distribution, and cleanup;
- clean EOF, abrupt EOF, transport failure, Interpreter failure;
- direct raw-byte fragmentation invariance;
- SendAndRetain permits later encoded writes on the same qualified exchange,
  while SendAndFinish rejects writes after half-close;
- no raw bytes enter actor command queues;
- ModelToolCalls receives inner-Loop fan-out;
- MCP/None modes never double-execute provider-observed tool events;
- stale exchange completion after a continuation is rejected;
- many concurrent transactions cannot cross-route bytes or events.

Exit gate:

- one fake provider transaction works end to end through the public API;
- every exchange-owned task and handle is joined or aborted;
- no provider-specific branch exists in transaction actor code.

Recommended PRs:

- PR 08: ExchangeDriver, pump, distributor, and terminal reconciliation.
- PR 09: actor state machine and fake Channel vertical integration.

### WP-06 — Real linked tools

Purpose:

Replace the empty-tool production path with bounded linked execution while
retaining empty-tool conformance.

Deliver:

- validated immutable HostToolRegistry;
- immutable per-transaction ResolvedToolSet;
- ToolCallContext;
- TransactionToolDispatcher;
- input-schema and payload validation;
- output-contract and output-byte validation;
- canonical success and domain-failure results;
- distinct runtime failure termination;
- global, per-transaction, and per-tool item/concurrency/byte limits;
- cooperative, abortable, and isolated-killable execution;
- rejection of unstoppable in-process handlers;
- real ToolRegistry/ToolRuntime adapters for the inner Loop; and
- fake dialect continuation through completed linked tools.

Required tests:

- unknown, duplicate, disallowed, and empty tool sets;
- schema-invalid and oversized arguments;
- success, domain failure, start failure, panic, lost completion;
- output schema/media type/byte violations;
- cancellation during queued and running execution;
- concurrency and queue limits plus one;
- different simultaneous transactions expose different tools;
- execution completion order differs from model request order;
- zero `Available -> DispatchRejected` placeholder on production paths.

Exit gate:

- direct fake-model tool/result/continuation is complete;
- output parity data is ready for both MCP and OpenAI encoders;
- every handler has bounded teardown.

Recommended PRs:

- PR 10: host registry, resolved set, and validation.
- PR 11: dispatcher, execution controls, Loop adapters, and fake continuation.

### WP-07 — MCP gateway and external-agent parity

Purpose:

Expose the exact resolved tool set to external agents without stale-call or
cross-transaction leakage.

Deliver:

- qualified `rmcp` Streamable HTTP loopback server;
- CSPRNG transaction capability generation;
- redacted McpServerDescriptor;
- pending, active, revoked capability states;
- install-at-creation and refresh-existing flows;
- SessionKey claim before route activation;
- `initialize`, `notifications/initialized`, `ping`, `tools/list`, `tools/call`;
- bounded request bodies, duration, concurrency, and pagination;
- dispatcher integration;
- local revocation before external descriptor removal;
- `None`, `CreationOnly`, and `Refreshable` enforcement; and
- deterministic MCP client test fixture.

Required tests:

- initialize/list/call normal flow;
- empty resolved set lists no tools;
- unknown, revoked, pending, stale, disallowed, and cross-session calls;
- delayed capability from transaction A cannot enter transaction B;
- capability never appears in diagnostics or test log capture;
- MCP and local paths expose equivalent definitions;
- both paths invoke the identical RegisteredTool handler;
- CreationOnly accepts a new session and rejects later reuse;
- Refreshable rotates on every transaction;
- unreachable loopback namespace fails capability validation;
- shutdown revokes all routes and closes the listener.

Exit gate:

- one deterministic external-agent fake executes linked tools through MCP;
- stale-call isolation is proven, not inferred from timing;
- no profile is labelled reusable-session tool compatible without refresh proof.

Recommended PRs:

- PR 12: gateway protocol, capability lifecycle, and test client.
- PR 13: SessionAdapter MCP configuration and parity integration.

### WP-08 — Generic streaming HTTP Connector

Purpose:

Provide one transport implementation reusable by conforming direct-LLM
providers.

Deliver:

- reqwest/Rustls Connector implementation;
- complete encoded request-body intake and SendAndFinish behavior;
- endpoint URL validation and configured path resolution;
- host-injected CredentialResolver with secrecy types;
- status and bounded safe-error mapping;
- streaming response chunks through RawOutputHandle;
- DNS/connect/request/body/idle/overall timeouts;
- proxy and TLS policy from Channel configuration;
- cancellation and forced termination; and
- no prompt/model/tool interpretation in the Connector.

Required tests with a local scripted server:

- successful fragmented SSE body;
- non-success status with bounded/redacted diagnostics;
- malformed endpoint and credential resolution failure;
- connect, headers, body, idle, and overall timeout;
- cancel before request, during connect, and mid-body;
- maximum request/response/chunk bytes plus one;
- connection pool reuse without semantic session reuse;
- secrets absent from Debug, logs, and errors.

Exit gate:

- Connector passes its component contract independently;
- no OpenAI-specific JSON/SSE code exists in the Connector.

Recommended PR:

- PR 14: generic HTTP Connector and deterministic transport suite.

### WP-09 — OpenAI Chat Completions encoder and Interpreter

Purpose:

Implement one explicitly qualified OpenAI-compatible dialect.

Deliver:

- outbound initial request encoding;
- typed option/capability validation;
- canonical message and historical tool-call encoding;
- tool definitions from ordered ToolSpec slices;
- continuation encoding with preserved provider call IDs;
- incremental SSE framing;
- choice selection;
- text assembly into complete canonical units;
- tool-call assembly by exchange, choice, index, ID, name, arguments;
- qualified finish-reason and `[DONE]` handling;
- malformed/truncated/oversized stream failures; and
- usage observations where supplied by the qualified dialect.

Required fixture tests:

- every relevant byte fragmentation of frames and UTF-8;
- text-only completion;
- assistant text plus tool calls;
- multiple fragmented tool calls;
- repeated provider call ID in different exchanges;
- invalid JSON arguments never become ToolRequestReady;
- multiple choice indices and unsupported choice behavior;
- every supported and unsupported finish reason;
- missing `[DONE]`, abrupt EOF, oversized line/event/arguments;
- initial and continuation golden request fixtures;
- configuration option rejection and capability-dependent field names.

Exit gate:

- encoder and Interpreter pass without HTTP or runtime integration;
- OpenAI Responses and non-streaming JSON remain explicitly unsupported.

Recommended PRs:

- PR 15: Chat Completions outbound encoder and fixtures.
- PR 16: Chat Completions SSE Interpreter and fragmentation suite.

### WP-10 — Direct-LLM vertical integration

Purpose:

Complete R-001 and the direct local-tool loop through the public transaction
API.

Deliver:

- direct-LLM Channel binding;
- effective configuration and credential wiring;
- HTTP ExchangeDriver integration;
- text streaming to TransactionEventSink;
- grouped tool execution;
- continuation ordering independent of execution completion order;
- fresh exchange identities per continuation;
- continuation-context and aggregate provider byte limits;
- completed, continuation-required, cancelled, failed, and limited terminal
  outcomes; and
- two provider profiles using the same implementation without name branches.

Required tests:

- text-only transaction;
- model/tool/model transaction;
- CallerControlled tool exchange terminates as `ContinuationRequired` without
  opening another provider exchange;
- multiple tools in one model response;
- supplied and generated ephemeral SessionId;
- two differently configured provider profiles;
- transaction cancellation during every HTTP/tool/continuation phase;
- max continuations, exchanges, context bytes, input bytes, output bytes;
- callback and final event exactly once under completion races;
- concurrent direct-LLM transactions with no mixed output.

Exit gate:

- R-001 acceptance criteria are directly tested;
- direct-LLM sessions leave no state after terminal completion.

Recommended PR:

- PR 17: direct-LLM Channel and complete transaction integration.

### WP-11 — Six external-profile migrations

Purpose:

Move existing agents behind ChannelBinding without bypassing canonical
transaction behavior.

Migration order:

1. Grok Build as the reference stateful/multi-session profile.
2. Cursor.
3. Codex.
4. Antigravity.
5. Z.ai.
6. Claude Code.

For each profile deliver:

- ConnectorFactory and matched Connector/SessionAdapter;
- ChannelBinding with truthful capabilities and limits;
- create/load/reuse behavior;
- effective SessionConfig validation;
- MCP `None`, `CreationOnly`, or `Refreshable` evidence;
- loopback reachability declaration;
- request encoding through the outbound encoder seam;
- removal of connector-local prompt shortcuts;
- authoritative completion rule;
- cancellation/termination at every supported phase; and
- deterministic profile qualification tests.

Profile PR exit gate:

- no prompt enters through `begin_open`;
- explicit existing SessionId never creates a new session;
- same SessionKey is rejected while active;
- different sessions progress concurrently;
- a profile declaring Bidirectional passes SendAndRetain write/receive,
  cancellation, and terminal-ordering qualification;
- non-empty tools are rejected unless MCP behavior is proven;
- no unsupported behavior is hidden behind defaults or fallback.

Recommended PRs:

- PR 18: Grok reference migration.
- PR 19: Cursor migration.
- PR 20: Codex migration.
- PR 21: Antigravity migration.
- PR 22: Z.ai migration.
- PR 23: Claude Code migration.

Profiles may be developed concurrently after the Grok reference seam is
accepted. Each profile remains independently reviewable and revertible.

### WP-12 — Full-system hardening and release qualification

Purpose:

Prove the integrated system meets R-000 rather than merely passing vertical
happy paths.

Deliver:

- architecture import/dependency tests;
- full terminal-race matrix;
- paused-time deadline suites;
- multi-Channel/multi-session load suite;
- byte/item/concurrency capacity-plus-one suites;
- shutdown with active sessions, exchanges, tools, MCP calls, sinks, callbacks;
- task/process/listener/route/permit leak accounting;
- secret/redaction and capability security review;
- malformed/untrusted provider/MCP/tool input corpus;
- testkit proof that presentation can be reconstructed solely from canonical
  TransactionEvents without feeding presentation state back into the runtime;
- profile capability report generated from tested declarations;
- requirements acceptance checklist update;
- current limitations document; and
- release candidate review with no unresolved P0/P1/P2 defects.

Required system scenarios:

- thousands of deterministic fake concurrent transactions within configured
  test limits;
- identical session strings on different Channels;
- stale exchange, stale child, stale capability, and old callback races;
- completion versus cancel, terminate, timeout, shutdown, sink failure;
- tool completion versus cancellation and output-contract failure;
- subscriber backpressure isolated to its transaction;
- direct LLM and external MCP calls running concurrently;
- canonical event replay produces the same test presentation projection;
- forced actor abort with supervisor callback finalization;
- zero events after Ended and zero duplicate callbacks.

Exit gate:

- every delivered acceptance checkbox has a direct non-conditional test;
- full workspace gate passes;
- documentation reflects actual behavior and profile limitations;
- independent review reports no unresolved P0, P1, or P2 issue.

Recommended PRs:

- PR 24: cross-cutting race/load/security conformance.
- PR 25: release qualification, requirements evidence, and documentation only.

## 7. Pull request sequencing

Required merge order:

```text
01 -> 02 -> 03 -> 04 -> 05 -> 06 -> 07 -> 08 -> 09
09 -> 10 -> 11 -> 12 -> 13
09 -> 14
09 -> 15 -> 16
11 + 14 + 16 -> 17
09 + 13 -> 18
18 -> 19/20/21/22/23
17 + 18 + 19 + 20 + 21 + 22 + 23 -> 24 -> 25
```

The numbering is a review sequence, not a requirement to serialize all coding.
Branches may proceed in parallel after their prerequisite contract PR is stable,
but they rebase onto the merged prerequisite before review.

Avoid one PR that simultaneously changes contracts, all profiles, MCP, HTTP, and
the actor. Such a change cannot be reviewed or reverted safely.

## 8. Definition of ready for each work package

A work package may begin when:

- all prerequisite contract PRs are merged;
- referenced specification sections are accepted;
- test fixtures needed for deterministic proof are available or included;
- no unresolved ownership or crate-dependency decision remains;
- expected error and terminal kinds are known;
- bounds and cancellation controls are identified; and
- the PR can be completed without advertising a later package's feature.

If any condition is absent, the developer raises a specification issue before
writing a substitute design in code.

## 9. Definition of done for each PR

A PR is done when:

- implementation and tests cover normal, boundary, failure, cancellation, and
  concurrency behavior introduced by the PR;
- all queues, byte stores, registries, and concurrency are bounded;
- every spawned task and pending operation has terminal ownership;
- all error paths produce typed truthful outcomes;
- no valid external input can reach a panic;
- public APIs have complete Rust documentation;
- relevant architecture and requirement references are included in the PR
  description;
- the workspace gate passes; and
- review has no unresolved P0, P1, or P2 finding.

## 10. Feature completion map

R-001 is complete only after:

- WP-08, WP-09, and WP-10 pass;
- two provider configurations reuse one Chat Completions implementation; and
- configuration, credentials, cancellation, tools, and streaming are tested.

R-002 is complete only after:

- WP-03 through WP-05, WP-10, WP-11, and shutdown portions of WP-12 pass;
- external create/reuse, direct supplied/generated identities, concurrent
  stateless LLMs, SessionKey/exchange isolation, and all terminal races are
  proven.

R-003 is complete only after:

- WP-06, WP-07, and WP-10 pass;
- direct and MCP paths share one registered handler and equivalent contracts;
- callback completion and tool cancellation are proven.

R-004 is complete only after:

- WP-01 canonical input, WP-04 event delivery, WP-10 direct-LLM cleanup, and
  WP-12 cleanup/reconstruction tests pass;
- no persistence or recovery path is introduced.

R-000 is complete only after:

- every applicable work package and WP-12 pass;
- no known P0/P1/P2 issue remains.

## 11. Mandatory verification gate

Every PR:

```text
cargo fmt --all -- --check
cargo test --workspace --all-targets --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo doc --workspace --no-deps
```

Additional package-specific gates:

- deterministic tests use barriers and paused time, not timing sleeps;
- protocol fixtures run under fragmentation matrices;
- concurrency tests assert isolation and outcomes, not only task completion;
- capacity tests exercise the configured maximum and maximum plus one;
- shutdown tests assert zero owned resources after completion;
- secret tests inspect diagnostics and captured logs;
- live-provider tests are qualification evidence only and never replace
  deterministic acceptance tests.

## 12. Review checklist

Architecture:

- Is responsibility in Connector, Interpreter, Component 3 transaction layer,
  inner Loop, or testkit as specified?
- Does dependency direction remain valid?
- Is provider-specific behavior confined to profile/encoder/Interpreter code?

Identity:

- Are TransactionId, SessionKey, ExchangeId, connection, interpretation, and
  tool identities checked at every asynchronous boundary?
- Can any late result enter a newer transaction?

Lifecycle:

- Is terminal selection exactly once?
- Can cancellation or shutdown preempt every blocking phase?
- Is every callback invoked once after terminal event delivery attempt?

Bounds:

- Are both item and byte capacities enforced?
- Are continuation history, diagnostics, schemas, MCP bodies, and outputs
  bounded?

Security:

- Are credentials and capabilities redacted and absent from invocation config?
- Do stale, unknown, cross-session, and disallowed tool calls fail closed?
- Can an unstoppable in-process tool be registered?

Tests:

- Does the PR prove malformed, delayed, disconnected, oversized, duplicate, and
  concurrent behavior relevant to its scope?
- Can the test pass without exercising the claimed branch?

## 13. Risk controls

### MCP profile capability

Risk:

Existing external agents may accept MCP only at session creation.

Control:

Qualify as `CreationOnly`, reject reuse, and do not claim reusable-session
parity. Only `Refreshable` profiles may rotate tools across transactions on one
external session.

### Dependency/MSRV drift

Risk:

Current `rmcp`, reqwest, or schema-validation releases may exceed workspace
MSRV or enable unwanted native dependencies.

Control:

Resolve in WP-00, pin through Cargo.lock, record enabled features, and change
MSRV only through an explicit reviewed decision.

### Connector migration regressions

Risk:

Existing prompt-in-open behavior may hide profile-specific sequencing.

Control:

Migrate Grok first, require per-profile deterministic fixtures, and remove each
shortcut only in its profile migration PR.

### Callback and shutdown races

Risk:

Actor abort may lose the final event or callback.

Control:

FinalizationGuard is runtime-owned and tested under forced actor abort before
any real provider profile is accepted.

### Tool execution leakage

Risk:

A linked handler ignores cancellation.

Control:

Reject handlers without cooperative+abortable or isolated-killable termination.
Exercise grace expiry and process kill in deterministic tests.

### Scope inflation

Risk:

Provider quirks create branches in shared transaction code.

Control:

Represent protocol differences through Channel capabilities, encoders,
Interpreter dialects, and profile adapters. Reject unsupported behavior rather
than adding provider-name conditions.

## 14. Delivery reporting

For every merged PR, record:

- work package and requirement references;
- behavior delivered;
- tests added;
- limits enforced;
- known unsupported behavior;
- profile capability changes;
- terminal/error kinds introduced;
- resource cleanup evidence; and
- next unblocked work packages.

Weekly or milestone reporting should state:

- merged work packages;
- currently executable end-to-end paths;
- blocked dependencies or profile limitations;
- failing acceptance scenarios;
- unresolved P0/P1/P2 findings; and
- whether the critical path changed.

Do not report a requirement or profile as complete because its types, mocks, or
happy-path example exist.

## 15. Final handoff

The delivery is ready for product integration only when:

1. WP-00 through WP-12 are complete;
2. R-001 through R-004 acceptance evidence is linked;
3. R-000 verification passes;
4. all six profiles have truthful capability declarations;
5. direct Chat Completions and external MCP tool paths are qualified;
6. graceful shutdown accounts for every admitted transaction;
7. no persistent Monoloop state exists;
8. the full workspace gate passes; and
9. independent review finds no unresolved P0, P1, or P2 issue.
