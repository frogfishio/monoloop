# Monoloop — Ground-zero cognitive runtime

**Status:** Architecture under construction

**Product specification:** [MONOLOOP.md](MONOLOOP.md)

**Production cognitive integration:**
[Cognitive Runtime ↔ Monoloop](../CONTEXT_COMPILER/COGNITIVE_RUNTIME_MONOLOOP.md)

This directory defines Monoloop from first principles. It does
not describe a refactor of the existing previous implementation loop. Existing code may
later supply adapters or reusable implementations only after it satisfies these
contracts.

Monoloop is a stateless asynchronous request/response processor. It accepts one
canonical request and one explicitly selected channel, processes that channel
correctly, emits fully assembled canonical events in real time, returns one
terminal result, and retains nothing after the run.

Console input and output are test adapters only. They are not production product
surfaces.

Components are specified independently before the complete execution machine is
assembled.

| Component | Specification | Responsibility |
|---|---|---|
| 01 | [Connector](CONNECTOR.md) | Dialect-labelled raw input/output transport with immediate cancellation and termination |
| 02 | [Interpreter](INTERPRETER.md) | Async incremental dialect interpretation and immediate in-memory canonical-unit events |
| 03 | [Console Renderer](CONSOLE_RENDERER.md) | Passive async rendering of canonical events for terminal debugging |
| 04 | [The Loop](THE_LOOP.md) | Lossless canonical-event consumer that dispatches complete tool requests through an abstract tool runtime |
| 05 | [Console Input](CONSOLE_INPUT.md) | Test-only conversion of complete terminal input into one canonical request |

Required supporting seam, contractually defined in
[Monoloop §7](MONOLOOP.md#7-outbound-encoder-seam) and intentionally awaiting a
separate implementation specification: canonical outbound request/result
encoding into the selected channel dialect. This seam belongs to Channel
composition, never Connector, Console Input, or The Loop.

No component acquires responsibilities merely because the current application
already combines them. Cross-component behavior is added only through an
explicit contract in this directory.
