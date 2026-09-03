#!/usr/bin/env bash
# Two-agent chat over ctxbroker's NATS bus.
#
# Runs two self-contained opencode "agents" (ALICE and BOB) as alternating
# `opencode run` processes. Each agent directory owns its own opencode
# config + plugin, so running from that directory:
#   - auto-loads its plugin, which drains ONLY its own inbound subject
#       ALICE -> examples.chat.to_alice
#       BOB   -> examples.chat.to_bob
#   - auto-registers its MCP send server (see .opencode/opencode.json)
#       ALICE sends to examples.chat.to_bob
#       BOB   sends to examples.chat.to_alice
#
# Each opencode run composes a reply from the injected context and publishes
# it back; the next run on the other side picks it up. Exactly-once delivery
# is handled by the shared JetStream + per-agent claim ledger.
#
# Usage:
#   examples/two-agent-chat/run-chat.sh [turns]    # default 4 turns
# Env:
#   OPENCODE_BIN   path to the opencode binary        (default /root/.opencode/bin/opencode)
#   CTXBROKER_BIN  path to the ctxbroker binary       (optional; auto-discovered otherwise)
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OPENCODE_BIN="${OPENCODE_BIN:-/root/.opencode/bin/opencode}"
TURNS="${1:-4}"

run_side() {
  local agent="$1"
  local prompt="$SCRIPT_DIR/$agent/prompt.md"
  echo ""
  echo "################ $agent — opencode run ################"
  (cd "$SCRIPT_DIR/$agent" && "$OPENCODE_BIN" run "$(cat "$prompt")")
}

for ((i = 0; i < TURNS; i++)); do
  if (( i % 2 == 0 )); then
    run_side alice
  else
    run_side bob
  fi
done

echo ""
echo "Done. Reconstruct the transcript by draining the two example subjects:"
echo "  ctxbroker drain --subject examples.chat.to_alice ...   (what ALICE received)"
echo "  ctxbroker drain --subject examples.chat.to_bob   ...   (what BOB received)"
