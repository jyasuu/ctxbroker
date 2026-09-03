#!/usr/bin/env bash
# Two-agent chat over ctxbroker's NATS bus.
#
# Runs two self-contained opencode "agents" (ALICE and BOB) as PARALLEL
# background processes. Each agent directory owns its own opencode config +
# plugin, so running from that directory:
#   - auto-loads its plugin, which drains ONLY its own inbound subject
#       ALICE -> examples.chat.to_alice
#       BOB   -> examples.chat.to_bob
#   - auto-registers its MCP send server (see .opencode/opencode.json)
#       ALICE sends to examples.chat.to_bob
#       BOB   sends to examples.chat.to_alice
#
# Each agent loops N times: run opencode (compose a reply from the injected
# context and publish it), then SLEEP so the other side has time to drain and
# reply before the next iteration. Exactly-once delivery is handled by the
# shared JetStream + per-agent claim ledger.
#
# Per-agent stdout/stderr is appended to <agent>.log; both logs are printed
# after both processes finish.
#
# Usage:
#   examples/two-agent-chat/run-chat.sh [turns] [sleep]   # default 4 turns, 5s sleep
# Env:
#   OPENCODE_BIN   path to the opencode binary        (default /root/.opencode/bin/opencode)
#   CTXBROKER_BIN  path to the ctxbroker binary       (optional; auto-discovered otherwise)
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OPENCODE_BIN="${OPENCODE_BIN:-/root/.opencode/bin/opencode}"
TURNS="${1:-4}"
SLEEP="${2:-5}"

run_agent() {
  local agent="$1"
  local prompt="$SCRIPT_DIR/$agent/prompt.md"
  local log="$SCRIPT_DIR/$agent.log"
  : > "$log"
  for ((i = 0; i < TURNS; i++)); do
    {
      echo ""
      echo "===== $agent run $((i + 1))/$TURNS ====="
      (cd "$SCRIPT_DIR/$agent" && "$OPENCODE_BIN" run "$(cat "$prompt")")
      echo "===== $agent sleeping ${SLEEP}s (waiting for a reply) ====="
      sleep "$SLEEP"
    } >> "$log" 2>&1
  done
}

echo "Starting ALICE and BOB in parallel (${TURNS} turns each, ${SLEEP}s sleep)..."
run_agent alice &
PID_A=$!
run_agent bob &
PID_B=$!
wait "$PID_A" "$PID_B"

echo ""
echo "=============== ALICE LOG ==============="
cat "$SCRIPT_DIR/alice.log"
echo ""
echo "=============== BOB LOG ==============="
cat "$SCRIPT_DIR/bob.log"
echo ""
echo "Done. Logs saved to alice.log and bob.log under examples/two-agent-chat/."
