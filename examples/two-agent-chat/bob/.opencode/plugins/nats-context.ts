import type { Plugin } from "@opencode-ai/plugin"
import { existsSync } from "node:fs"
import { join } from "node:path"

// ===== Agent-specific configuration =====
// This example agent (BOB) drains ONLY its own inbound subject:
//   examples.chat.to_bob
// It receives messages here and replies via its MCP `send_message` tool,
// which publishes to the ALICE-ward subject examples.chat.to_alice
// (see ./.opencode/opencode.json for the MCP server binding).
const SUBJECT = "examples.chat.to_bob"
const DURABLE = "example-bob"
const SESSION_ID = "example-bob"
const DB = ".ctxbroker/example-bob.db"

const NATS_URL = process.env.NATS_URL ?? "nats://localhost:4222"

// Resolve the ctxbroker binary: prefer $CTXBROKER_BIN, else walk up from the
// project directory looking for a release build (works from any examples dir).
function resolveBin(directory: string): string {
  if (process.env.CTXBROKER_BIN) return process.env.CTXBROKER_BIN
  let dir = directory
  for (let i = 0; i < 6; i++) {
    const candidate = join(dir, "target", "release", "ctxbroker")
    if (existsSync(candidate)) return candidate
    const parent = join(dir, "..")
    if (parent === dir) break
    dir = parent
  }
  return "ctxbroker"
}

export const NatsContextPlugin: Plugin = async ({ $, directory }) => {
  const bin = resolveBin(directory)

  const shell = async (cmd: string) => {
    const parts = cmd.split(/\s+/).filter(Boolean)
    return $`${bin} ${parts}`.nothrow().quiet()
  }

  async function drain() {
    await shell(
      `drain --db ${DB} --nats-url ${NATS_URL} --subject ${SUBJECT} --durable ${DURABLE} --max 50`,
    ).catch(() => {})
  }

  async function collectContext(): Promise<string[]> {
    const acc: string[] = []
    let guard = 0
    while (guard++ < 100) {
      const fetch = await shell(`fetch --db ${DB} --session ${SESSION_ID} --lease-secs 60`)
      let msg: any
      try {
        msg = JSON.parse(fetch.text().trim())?.message
      } catch {
        break
      }
      if (!msg || !msg.id) break
      acc.push(msg.body)
      await shell(`ack ${msg.id} --db ${DB} --session ${SESSION_ID}`).catch(() => {})
    }
    return acc
  }

  return {
    async event({ event }) {
      if (["session.created", "session.updated", "session.idle", "session.compacted"].includes(event.type)) {
        await drain()
      }
    },
    async "experimental.chat.messages.transform"(_input, output) {
      await drain()
      const bodies = await collectContext()
      if (bodies.length === 0) return

      const last = output.messages[output.messages.length - 1]
      const text =
        "[ctxbroker: NATS context messages]\n" +
        bodies.map((b, i) => `  ${i + 1}. ${b}`).join("\n")
      output.messages.push({
        info: last?.info ?? {},
        parts: [
          {
            id: `ctxbroker-${Date.now()}`,
            sessionID: last?.info?.sessionID ?? "",
            messageID: last?.info?.id ?? "",
            type: "text",
            text,
            synthetic: true,
          } as any,
        ],
      })
    },
  }
}

export default NatsContextPlugin
