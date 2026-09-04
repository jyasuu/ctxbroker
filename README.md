# ctxbroker

Broker → agent-context bridge. A small Rust CLI (+ MCP server) that pulls
messages published to a message broker (NATS JetStream or RabbitMQ) into a
local SQLite claim/lease ledger, and lets an agent hook claim and **ack** those
messages exactly-once.

It's designed as the "pull side" of a context-injection loop for CLI coding
agents (opencode, etc.): other processes publish context/instructions to a
broker, and on the next agent turn this tool drains them into the conversation
as if they were a normal user message — no agent ever sees a message twice.

## How it works

```
   publishers  ──►  broker (NATS/RabbitMQ)  ──►  ctxbroker drain  ──►  SQLite ledger
                                                       │
   agent hook  ──►  ctxbroker fetch (claim+lease) ◄────┘
                                                       │
   agent hook  ──►  ctxbroker ack  (exactly-once)  ◄───┘  (only lease holder may ack)
```

- **`drain`** pulls up to `max` messages from the broker, stamps each into the
  local SQLite inbox *before* acking the broker (store-then-ack / outbox
  ordering). Broker redelivery of the same `msg_id` is a no-op (`INSERT ...
  ON CONFLICT DO NOTHING`), so the broker's at-least-once semantics never leak
  into the ledger. If staging a message fails, the broker receives a NAK so the
  message is redelivered instead of lost.
- **`fetch`** atomically claims the oldest pending message (via `BEGIN
  IMMEDIATE`, so concurrent processes can never both win) and assigns a lease.
  A leased message whose lease has expired can be reclaimed by another session
  (crash/timeout recovery).
- **`ack`** permanently marks a claimed message delivered — *only* if the
  caller is still the lease holder. A late/crashed claimant's ack is a no-op,
  which is what makes reclaim safe and delivers exactly-once semantics.

The SQLite store uses WAL mode so multiple OS processes (main agent + each
subagent) can read/write concurrently.

## Build

Requires Rust 1.85+ (the `rmcp`/`schemars` dependency trees need edition 2024).
Verified with rustc/cargo 1.98.

```sh
cargo build --release
# binary: target/release/ctxbroker
```

## CLI

```
ctxbroker [--db <path>] <subcommand>
```

Global flags:

- `--db <path>` — SQLite store (ledger). Default `.ctxbroker/store.db`.
- One of `--amqp-url` or `--nats-url` is required for any broker-touching
  subcommand (`drain`, `mcp-serve`, and `send` when publishing for real).
  Passing both (or neither) is an error.

### Fetch — claim the next message

```sh
ctxbroker fetch --session my-agent --lease-secs 300 [--db ...]
# { "message": { "id": "...", "source": "...", "body": "...", "received_at": "...", "lease_expires_at": null } }
# ..., or { "message": null } when the inbox is empty / nothing to reclaim
```

### Ack — confirm delivery (lease-holder only)

```sh
ctxbroker ack <msg-id> --session my-agent [--db ...]
# { "acked": true, "id": "..." }   (false if the lease expired / was reclaimed)
```

### Send — publish a message

```sh
# Publish for real (NATS)
ctxbroker send --body "the context" --nats-url nats://localhost:4222 --subject agent.context

# Publish for real (RabbitMQ)
ctxbroker send --body "the context" --amqp-url amqp://localhost --queue agent.context

# No broker flags: stage directly into the local inbox (test the fetch/ack
# loop with no broker running)
ctxbroker send --body "the context"
# { "id": "local-...", "status": "staged (no broker configured)" }
```

### Drain — pull broker messages into the ledger

```sh
ctxbroker drain --nats-url nats://localhost:4222 --subject agent.context --durable ctxbroker --max 50 [--db ...]
# { "drained_from_broker": 2, "newly_enqueued": 2 }
```

Run this periodically (cron / systemd timer) or on-demand before `fetch`. It is
a short-lived process, deliberately **not** a long-running consumer.

### mcp-serve — run as an MCP server

Exposes a `send_message(topic, body)` tool over stdio (same publish path as
`send`, so the CLI and MCP tool can never drift):

```sh
ctxbroker mcp-serve --nats-url nats://localhost:4222 --subject agent.context
```

## Broker notes

- **NATS**: JetStream-only, on purpose. Plain (core) NATS is fire-and-forget —
  a message published while the CLI isn't running is lost with no redelivery,
  which breaks the hook-only-on-invoke model. The subject doubles as the
  JetStream stream name (dots → underscores).
- **RabbitMQ**: durable queue + persistent deliveries (`delivery_mode=2`),
  publishes wait for a broker confirm.

## Docker NATS (JetStream)

```sh
docker compose up -d
# client :4222, monitoring/JetStream :8222 (nats:2.10, -js)
```

## opencode plugin

`.opencode/plugins/nats-context.ts` bridges NATS → the agent loop without any
CLI wiring by hand:

- On `tool.execute.after` (scoped to the `*_send_message` MCP tool) it runs
  `ctxbroker drain` right after the agent publishes — the exact moment a reply
  could be inbound — moving broker messages into the ledger with minimal
  latency. (An `event` hook on `session.*` is deliberately avoided:
  `session.updated`/`session.status` fire dozens of times per run alongside
  streaming deltas, causing many redundant drains.)
- On `experimental.chat.messages.transform` it drains again, then claims every
  pending/expired message (`fetch` + `ack`) and injects them as a synthetic
  user "context" message into the conversation, so the agent observes and can
  act on them. This hook is the only reliable *injection* point (see note
  below) and fires exactly once per LLM round trip.

Env overrides: `CTXBROKER_BIN`, `CTXBROKER_DB`, `NATS_URL`,
`CTXBROKER_SUBJECT`, `CTXBROKER_DURABLE`, `CTXBROKER_SESSION`,
`CTXBROKER_MCP_NAME` (prefix of the MCP `send_message` tool the plugin drains
after; default `ctxbroker`).

> Note: this plugin targets opencode 1.18.26 (anomalyco fork), where
> `experimental.chat.system.transform` output is silently ignored; the
> `experimental.chat.messages.transform` hook is the working injection point.

## Project layout

- `src/main.rs` — CLI (`send`/`drain`/`fetch`/`ack`/`mcp-serve`).
- `src/store.rs` — SQLite inbox + claim/lease/ack ledger (WAL mode).
- `src/broker/{mod,nats,rabbitmq}.rs` — `MessageBroker` trait + JetStream /
  RabbitMQ implementations.
- `src/mcp.rs` — rmcp `send_message` tool / stdio server.
- `.opencode/plugins/nats-context.ts` — opencode injection plugin.
- `docker-compose.yml` — local NATS (JetStream) for development.
