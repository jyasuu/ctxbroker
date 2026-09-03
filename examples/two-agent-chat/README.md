# Example: two opencode agents chatting over a NATS bus

This example runs **two separate `opencode run` processes** (ALICE and BOB) that
talk to each other over a NATS message bus bridged by **ctxbroker**. Each agent
is a self-contained project directory with **its own opencode config and its own
plugin**, so the subjects are fully under each agent's control — no shared or
global configuration is required.

## How it works

Each agent directory is an independent opencode project. When `opencode run` is
launched from inside one, opencode auto-discovers that directory's
`.opencode/` config and local plugin:

| Agent | Working dir | Plugin drains (inbound) | MCP server publishes (outbound) |
|-------|-------------|--------------------------|----------------------------------|
| ALICE | `alice/`  | `examples.chat.to_alice` | `examples.chat.to_bob`  (`ctxbroker-alice-send`) |
| BOB   | `bob/`    | `examples.chat.to_bob`   | `examples.chat.to_alice` (`ctxbroker-bob-send`) |

Flow per turn:

1. Agent runs. Its plugin (`alice/.opencode/plugins/nats-context.ts` or
   `bob/...`) drains its **own inbound subject** from NATS into a per-agent
   claim ledger (exactly-once), then injects the message as a synthetic user
   "ctxbroker" context block.
2. The model reads that context, composes a reply, and calls its `send_message`
   MCP tool to publish the reply to the **other agent's** inbound subject.
3. The next run on the other side picks it up, and so on.

The two subjects are directional and distinct from the project's own
`agent.context` subject, so this example never collides with the main
ctxbroker plugin.

## Subjects used

- `examples.chat.to_alice` — messages to ALICE (drained by ALICE's plugin)
- `examples.chat.to_bob`   — messages to BOB (drained by BOB's plugin)

Both live in the `examples.chat.*` namespace, separate from `agent.context`.

## Prerequisites

- NATS server with JetStream running (`docker compose up -d` in the repo root).
- `ctxbroker` release binary built: `cargo build --release`.
- `opencode` binary (default `/root/.opencode/bin/opencode`; override with `OPENCODE`).
- Node available for opencode's plugin runtime (auto-installs `@opencode-ai/plugin` per agent dir).

## Run

```bash
examples/two-agent-chat/run-chat.sh [turns]     # default 4 turns
```

`run-chat.sh` alternates: ALICE run → BOB run → ALICE run → BOB run → …
Each turn produces one publish.

### Manual (without the runner)

```bash
# ALICE's turn (runs from her dir so her plugin + MCP server load)
cd examples/two-agent-chat/alice
opencode run "$(cat prompt.md)"

# BOB's turn
cd ../bob
opencode run "$(cat prompt.md)"
```

## Reconstructing the transcript

Messages remain in the JetStream streams after delivery. Drain each directional
subject with a throwaway durable consumer:

```bash
BIN=target/release/ctxbroker
$BIN drain --db /tmp/alice.db --nats-url nats://localhost:4222 \
     --subject examples.chat.to_alice --durable dt-$(date +%s) --max 50
$BIN drain --db /tmp/bob.db   --nats-url nats://localhost:4222 \
     --subject examples.chat.to_bob   --durable dt-$(date +%s) --max 50
# then fetch/ack each db in a loop to print the bodies
```

## Files

```
two-agent-chat/
  run-chat.sh                       # alternates the two opencode processes
  README.md                         # this file
  alice/
    .opencode/opencode.json         # registers ctxbroker-alice-send -> examples.chat.to_bob
    .opencode/package.json          # @opencode-ai/plugin dependency
    .opencode/plugins/nats-context.ts  # alice's plugin: drains examples.chat.to_alice
    prompt.md                       # ALICE persona + behavior
  bob/
    .opencode/opencode.json         # registers ctxbroker-bob-send -> examples.chat.to_alice
    .opencode/package.json          # @opencode-ai/plugin dependency
    .opencode/plugins/nats-context.ts  # bob's plugin: drains examples.chat.to_bob
    prompt.md                       # BOB persona + behavior
```

## Caveats

- Each agent's plugin resolves the `ctxbroker` binary by walking up from its
  working directory to the repo's `target/release/ctxbroker`, or via
  `CTXBROKER_BIN`.
- Plugin edits are auto-detected by opencode; if a plugin stops loading, ensure
  `@opencode-ai/plugin` is installed in that directory's `.opencode/`
  (`bun install` / `npm install`).
