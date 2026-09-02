import type { Plugin } from "@opencode-ai/plugin"

// Pull-based context bridge: messages published to a NATS subject (via
// `ctxbroker send` or the MCP tool) are drained into a local SQLite claim
// ledger, then claimed-and-acked into the conversation as a synthetic user
// "context" message on the next chat. Guarantees exactly-once delivery per
// NATS message because ack only succeeds for the lease holder (src/store.rs).
//
// NOTE: this build of opencode (1.18.26) silently ignores
// `experimental.chat.system.transform` output, so we inject via
// `experimental.chat.messages.transform` instead, which IS respected.

const DB = process.env.CTXBROKER_DB ?? ".ctxbroker/store.db"
const NATS_URL = process.env.NATS_URL ?? "nats://localhost:4222"
const SUBJECT = process.env.CTXBROKER_SUBJECT ?? "agent.context"
const DURABLE = process.env.CTXBROKER_DURABLE ?? "ctxbroker"
const SESSION_ID = process.env.CTXBROKER_SESSION ?? "opencode-plugin"

export const NatsContextPlugin: Plugin = async ({ $, directory }) => {
  const bin = process.env.CTXBROKER_BIN ?? `${directory}/target/release/ctxbroker`

  // `$` already runs in the project directory by default. Do NOT chain
  // `$.env(...)`/`$.cwd(...)` and call `.nothrow()` on the result (that chain
  // lacks the shell methods here), and DO split bin + args into separate
  // template slices (a single interpolated string is not word-split).
  const shell = async (cmd: string) => {
    const parts = cmd.split(/\s+/).filter(Boolean)
    return $`${bin} ${parts}`.nothrow().quiet()
  }

  async function drain() {
    // Pull pending messages from the broker into the local ledger. Fire-and-
    // forget: if the broker is down or the store isn't there, show nothing.
    await shell(
      `drain --db ${DB} --nats-url ${NATS_URL} --subject ${SUBJECT} --durable ${DURABLE} --max 50`,
    ).catch(() => {})
  }

  async function collectContext(): Promise<string[]> {
    const acc: string[] = []
    let guard = 0
    // Claim every pending/expired message, one at a time, and ack the ones we
    // own. Loop until empty or a safety cap (protects against a tight loop).
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
      // Pull messages into the store as early as possible so the messages
      // transform (which runs on the first LLM request) finds them.
      if (["session.created", "session.updated", "session.idle", "session.compacted"].includes(event.type)) {
        await drain()
      }
    },
    async "experimental.chat.messages.transform"(_input, output) {
      await drain()
      const bodies = await collectContext()
      if (bodies.length === 0) return

      // Append a synthetic user "context" message so the agent observes what
      // was received and can act on / report it.
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
