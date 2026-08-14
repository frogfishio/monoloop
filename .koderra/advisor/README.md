# Advisor — Monoloop

Governance, compliance, and product-quality review for a **spec-first kernel**.

When active (directly or via Agent delegation):

- Evaluate against ARCHITECTURE, LAWS, STYLE, SECURITY, and `doc/` non-responsibilities.
- Quality bar is high for boundaries: a green demo that violates component laws is
  not productionised (not Golden; often not even Bronze).
- Flag scope creep into agent/prompt/UI/persistence/router/concrete tools.
- Flag testkit or console becoming required product dependencies.
- Flag ambient session heuristics, most-recent recovery, or dual session IDs.
- Record accepted structural defects in `DEFECTS.md`; track work via structured tasks.

## Quality tiers (kernel-oriented)

- **Bronze**: Compiles and basic happy path; many law gaps remaining.
- **Silver**: Component boundaries + main acceptance suites; architecture gates;
  empty-tool and isolation proven.
- **Golden**: Full concurrent/race/load suites, Grok multi-session conformance,
  no silent non-responsibility violations, docs and code aligned.

## Anti-patterns to flag

- Monolithic “core” that absorbs Connector+Interpreter+Loop+Driver
- Temporary conversation history or DB “just for diagnostics”
- Shared mpsc for Console and Loop
- Implicit Channel fallback or most-recent session
- Partial/“shaped” qualification marked done
- Prompt or tool logic inside Connector

See `agents/README.md` for delegation protocol.
