# WP-00 — Six-profile capability worksheet

Status: evidence-backed **provisional** declarations for TransactionRuntime
Channel validation (WP-03+) and profile migration (WP-11). Values are taken from
connector profile docs and current connector code; they are **not** a claim that
production `TransactionRuntime` already exercises every path.

Legend for MCP support (external agent consuming Monoloop’s MCP gateway):

| Value | Meaning |
|---|---|
| `None` | Profile cannot attach Monoloop MCP tools for the transaction |
| `CreationOnly` | MCP may be offered only when creating a new external session |
| `Refreshable` | Same external session may refresh tool set across transactions |

Exchange modes (design vocabulary):

| Value | Meaning |
|---|---|
| `SendAndFinish` | One outbound prompt/request cycle per connection open |
| `SendAndRetain` / Bidirectional | Session retained; further exchanges on same external session |

Continuation policies (runtime): `inline` (tool results continue in-runtime) and
`caller_controlled` (host decides next submit). Profiles that cannot retain a
session cannot claim multi-exchange continuation on one external ID.

---

## Summary matrix

| Profile | Session create | Explicit session load/reuse | MCP | Loopback MCP reachability | Exchange mode | Continuation |
|---|---|---|---|---|---|---|
| Grok Build | Yes (`session/new`) | Yes (`session/load`, explicit id) | **CreationOnly** (provisional) | Yes (WS loopback default) | Bidirectional / **SendAndRetain** | `inline` + `caller_controlled` candidates |
| Cursor | Yes (`session/new`) | Yes (`session/load`) | **CreationOnly** (provisional) | Agent process local; MCP URL must be loopback-reachable from agent | Bidirectional / **SendAndRetain** | same |
| Antigravity (agy) | Yes (`session/new`) | Yes (`session/load`) | **CreationOnly** (provisional) | Same as Cursor (stdio agent) | Bidirectional / **SendAndRetain** | same |
| Codex | Yes (`session/new`) | Yes (`session/load`) | **CreationOnly** (provisional) | Same as Cursor (stdio adapter) | Bidirectional / **SendAndRetain** | same |
| Z.ai CLI | Synthetic per run only | **No** durable load | **None** | N/A (no Monoloop MCP attach) | **SendAndFinish** only | single-shot; no external-session continuation |
| Claude Code | Synthetic / stream `session_id` observational | **No** Monoloop `session/load` | **None** | N/A | **SendAndFinish** only | single-shot |

### Bidirectional → SendAndRetain qualification (exit gate)

Every profile declaring Bidirectional is assigned **SendAndRetain** qualification
work in **WP-05** (exchange driver) and its **WP-11** profile PR:

| Profile | WP-05 SendAndRetain | WP-11 profile PR |
|---|---|---|
| Grok Build | Required | Required (first migration target) |
| Cursor | Required | Required |
| Antigravity | Required | Required |
| Codex | Required | Required |
| Z.ai CLI | Not applicable (`SendAndFinish` only) | Fixture/migration only |
| Claude Code | Not applicable (`SendAndFinish` only) | Fixture/migration only |

---

## Per-profile evidence

### Grok Build (`monoloop-connector-grok`)

| Capability | Declaration | Evidence |
|---|---|---|
| Session create | Yes | `doc/GROK_BUILD_CONNECTOR.md` § session/new; `GrokSessionManager::begin_new` |
| Explicit load | Yes | `session/load` with explicit `sessionId`; no most-recent heuristic |
| MCP | CreationOnly provisional | No Monoloop MCP wiring yet; ACP session may accept tool/server config only at create — treat as CreationOnly until WP-11 proves Refreshable |
| Loopback | Fail-closed non-loopback by default | `allow_non_loopback` default false; tests `non_loopback_without_opt_in_fails_closed` |
| Exchange | Bidirectional | Long-lived server; many sessions; multi-prompt per session with prompt lock |
| Continuation | Both candidates | Session retained across prompts; runtime policies land in WP-04/05 |

### Cursor (`monoloop-connector-cursor`)

| Capability | Declaration | Evidence |
|---|---|---|
| Session create | Yes | `session/new` after `initialize` / `authenticate` |
| Explicit load | Yes | `session/load` when `OpenConnection.external_session_id` set |
| MCP | CreationOnly provisional | Not wired; agent-local MCP config typically at session create |
| Loopback MCP | Conditional | Stdio agent on host must reach `127.0.0.1` Monoloop MCP URL |
| Exchange | Bidirectional | `session/prompt` + retain process/session until shutdown |
| Continuation | Both candidates | Same external `sessionId` |

### Antigravity / agy (`monoloop-connector-agy`)

Same shape as Cursor (ACP NDJSON stdio). Source-step ordering via `_meta.stepIdx` /
`messageId` is observational only. MCP provisional **CreationOnly**.

### Codex (`monoloop-connector-codex`)

ACP via `@agentclientprotocol/codex-acp`. Session create/load same family.
MCP provisional **CreationOnly**. Exchange Bidirectional / SendAndRetain.

### Z.ai CLI (`monoloop-connector-zai`)

| Capability | Declaration | Evidence |
|---|---|---|
| Session create | Synthetic `zai-<uuid>` per run | No provider session store |
| Load/reuse | **No** | Headless one-shot process |
| MCP | **None** | Tools execute inside CLI; Monoloop does not attach MCP |
| Exchange | **SendAndFinish** | Process exits after one `-p` prompt |
| Continuation | None on external session | New process per transaction |

### Claude Code (`monoloop-connector-claude`)

| Capability | Declaration | Evidence |
|---|---|---|
| Session create | Observational `session_id` from stream | Not Monoloop-owned resume |
| Load/reuse | **No** Monoloop load path | `claude -p` one-shot |
| MCP | **None** | Tools inside Claude Code |
| Exchange | **SendAndFinish** | Process exits after print run |
| Continuation | None on external session | New process per transaction |

---

## Prompt-shortcut inventory (migration targets)

Connector-local patterns that fold prompt admission into `OpenConnection` /
process spawn instead of a pure transaction encoder path. Each must be removed
or replaced only in its WP-11 profile migration PR (Grok first).

| ID | Profile | Shortcut | Location | Migration note |
|---|---|---|---|---|
| PS-01 | Cursor | First `RawInputMessage::Bytes` → `session/prompt` text inside open pump | `connector-cursor` `lib.rs` open task | Encoder should emit dialect-complete `session/prompt`; connector only transports |
| PS-02 | Agy | Same as Cursor | `connector-agy` open task | Same |
| PS-03 | Codex | Same as Cursor | `connector-codex` open task | Same |
| PS-04 | Z.ai | Prompt on argv: `zai -p <prompt>` | `config::argv_for_prompt` / `run_headless_prompt` | Headless CLI **requires** argv prompt by product contract; document as profile exception or switch dialect if a non-argv API appears |
| PS-05 | Claude | Prompt as CLI positional after flags | `config::argv_for_prompt` / `run_claude_print` | Same class as Z.ai (print mode) |
| PS-06 | Grok | Open may `session/new` or `session/load` then map raw input to `session/prompt` RPC | `connector-grok` `server.rs` / session | Closest to target: session config without prompt; ensure transaction encoder owns prompt body |
| PS-07 | All ACP | `session/request_permission` auto allow-once under test config | `*_config` skip/allow flags | Must remain fail-closed by default; not a prompt shortcut but a permission shortcut — host policy later |

**Grok note:** Grok already avoids putting prompts on process argv (LAW). Shortcut
risk is sequencing (attach/open/prompt folded in connector open), not argv.

---

## Component acceptance gaps (current vs TransactionRuntime plan)

Honest gaps — **not** complete:

| Area | Current | Target (plan) |
|---|---|---|
| Contracts | Run/loop/tool ids; no full Transaction* surface | WP-01 |
| Connector factory / session ownership | Per-profile open; FakeConnector | WP-02 |
| Runtime bootstrap / Channel registry | Absent | WP-03 |
| Admission / finalization / callbacks | Absent | WP-04 |
| Exchange driver | Implicit in connectors | WP-05 |
| Linked tools | EmptyToolRegistry only | WP-06 |
| MCP gateway | Spike only (WP-00) | WP-07 |
| Outbound encoders | Testkit / connector-local | WP-08 |
| HTTP connector / credentials | Grok WS only | WP-09 |
| Direct LLM path | Absent | WP-10 |
| Profile production migration | Live testkit examples only | WP-11 |
| Hardening / conformance | Partial empty-loop + fixtures | WP-12 |

Empty-tool path is **already** qualified (required). Do not relabel it as
TransactionRuntime complete.
