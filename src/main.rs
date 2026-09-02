mod broker;
mod mcp;
mod store;

use broker::{MessageBroker, NatsBroker, RabbitMqBroker};
use clap::{Args, Parser, Subcommand};
use serde_json::{json, Value};
use std::sync::Arc;
use store::Store;

#[derive(Parser)]
#[command(name = "ctxbroker", about = "Broker <-> agent-context bridge")]
struct Cli {
    /// Path to the local SQLite store (the claim/lease ledger)
    #[arg(long, global = true, default_value = ".ctxbroker/store.db")]
    db: String,

    #[command(subcommand)]
    command: Command,
}

/// Exactly one of these must be set for any broker-touching subcommand --
/// picking RabbitMQ or NATS is a deployment choice, not a per-call one, but
/// keeping it a flag (vs. a config file) keeps the CLI self-contained for now.
#[derive(Args, Clone)]
struct BrokerArgs {
    #[arg(long)]
    amqp_url: Option<String>,
    /// RabbitMQ queue name. Required if --amqp-url is set.
    #[arg(long)]
    queue: Option<String>,

    #[arg(long)]
    nats_url: Option<String>,
    /// NATS subject. Required if --nats-url is set.
    #[arg(long)]
    subject: Option<String>,
    /// JetStream durable consumer name. Only used with --nats-url.
    #[arg(long, default_value = "ctxbroker")]
    durable: String,
}

impl BrokerArgs {
    async fn connect(&self) -> anyhow::Result<Arc<dyn MessageBroker>> {
        let has_amqp = self.amqp_url.is_some();
        let has_nats = self.nats_url.is_some();
        if has_amqp && has_nats {
            anyhow::bail!("use either --amqp-url or --nats-url, not both");
        }
        if !has_amqp && !has_nats {
            anyhow::bail!("pass --amqp-url or --nats-url");
        }
        if let Some(url) = &self.amqp_url {
            let queue = self
                .queue
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("--queue is required with --amqp-url"))?;
            let b = RabbitMqBroker::connect(url, queue).await?;
            return Ok(Arc::new(b));
        }
        if let Some(url) = &self.nats_url {
            let subject = self
                .subject
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("--subject is required with --nats-url"))?;
            let b = NatsBroker::connect(url, subject, &self.durable).await?;
            return Ok(Arc::new(b));
        }
        anyhow::bail!("no broker configured -- pass --amqp-url or --nats-url")
    }

    /// Topic/subject name to publish to -- unifies RabbitMQ's `queue` and
    /// NATS's `subject` since a single `send` call needs one or the other.
    fn default_topic(&self) -> Option<&str> {
        self.queue.as_deref().or(self.subject.as_deref())
    }
}

#[derive(Subcommand)]
enum Command {
    /// Claim the next unclaimed (or lease-expired) message, if any.
    Fetch {
        #[arg(long)]
        session: String,
        #[arg(long, default_value_t = 300)]
        lease_secs: i64,
    },
    /// Permanently mark a claimed message as delivered. Fails (acked:false)
    /// if `session` is not the current lease holder -- e.g. the lease expired
    /// and someone else already reclaimed it.
    Ack {
        msg_id: String,
        #[arg(long)]
        session: String,
    },
    /// Publish a message. Without broker flags, stages directly into the
    /// local inbox -- useful for testing the fetch/ack loop with no broker
    /// running. With --amqp-url/--nats-url, publishes for real.
    Send {
        #[arg(long)]
        topic: Option<String>,
        #[arg(long)]
        body: String,
        #[command(flatten)]
        broker: Option<BrokerArgsOpt>,
    },
    /// Pull pending messages from the broker into the local claim ledger.
    /// Run periodically (cron, systemd timer) or on-demand before `fetch` --
    /// NOT a long-running consumer, see broker/mod.rs notes.
    Drain {
        #[command(flatten)]
        broker: BrokerArgs,
        #[arg(long, default_value_t = 50)]
        max: usize,
    },
    /// Run as an MCP server over stdio, exposing a `send_message` tool that
    /// publishes through the same path as `send --amqp-url`/`--nats-url`.
    McpServe {
        #[command(flatten)]
        broker: BrokerArgs,
    },
}

// `Send` needs the broker args to be *optional as a whole* (no-broker mode is
// valid), which clap's required-group doesn't directly support alongside the
// other subcommands' required group -- so it gets its own non-required copy.
#[derive(Args, Clone)]
struct BrokerArgsOpt {
    #[arg(long)]
    amqp_url: Option<String>,
    #[arg(long)]
    queue: Option<String>,
    #[arg(long)]
    nats_url: Option<String>,
    #[arg(long)]
    subject: Option<String>,
    #[arg(long, default_value = "ctxbroker")]
    durable: String,
}

impl BrokerArgsOpt {
    fn as_required(&self) -> Option<BrokerArgs> {
        if self.amqp_url.is_none() && self.nats_url.is_none() {
            return None;
        }
        Some(BrokerArgs {
            amqp_url: self.amqp_url.clone(),
            queue: self.queue.clone(),
            nats_url: self.nats_url.clone(),
            subject: self.subject.clone(),
            durable: self.durable.clone(),
        })
    }
}

fn main() {
    let cli = Cli::parse();
    let rt = tokio::runtime::Runtime::new().expect("failed to start tokio runtime");
    std::process::exit(match rt.block_on(run(&cli)) {
        Ok(value) => {
            println!("{value}");
            0
        }
        Err(e) => {
            println!("{}", json!({ "error": e.to_string() }));
            1
        }
    });
}

async fn run(cli: &Cli) -> anyhow::Result<Value> {
    if let Some(parent) = std::path::Path::new(&cli.db).parent() {
        std::fs::create_dir_all(parent)?;
    }

    match &cli.command {
        Command::Fetch { session, lease_secs } => {
            let mut store = Store::open(&cli.db)?;
            let message = store.claim_next(session, *lease_secs)?;
            Ok(json!({ "message": message }))
        }
        Command::Ack { msg_id, session } => {
            let store = Store::open(&cli.db)?;
            let acked = store.ack(msg_id, session)?;
            Ok(json!({ "acked": acked, "id": msg_id }))
        }
        Command::Send { topic, body, broker } => {
            match broker.as_ref().and_then(|b| b.as_required()) {
                Some(b) => {
                    let conn = b.connect().await?;
                    let t = topic
                        .clone()
                        .or_else(|| b.default_topic().map(str::to_string))
                        .ok_or_else(|| anyhow::anyhow!("--topic (or --queue/--subject) is required"))?;
                    conn.publish(&t, body.as_bytes()).await?;
                    Ok(json!({ "status": "published", "topic": t }))
                }
                None => {
                    let mut store = Store::open(&cli.db)?;
                    let id = format!("local-{}", nanos_stamp());
                    let t = topic.clone().unwrap_or_else(|| "local".to_string());
                    store.enqueue(&id, &t, body)?;
                    Ok(json!({ "id": id, "status": "staged (no broker configured)" }))
                }
            }
        }
        Command::Drain { broker, max } => {
            let mut store = Store::open(&cli.db)?;
            let conn = broker.connect().await?;
            let mut count = 0usize;
            let drained = conn
                .drain(*max, &mut |m| {
                    if store.enqueue(&m.id, &m.source, &m.body)? {
                        count += 1;
                    }
                    Ok(())
                })
                .await?;
            Ok(json!({ "drained_from_broker": drained, "newly_enqueued": count }))
        }
        Command::McpServe { broker } => {
            let conn = broker.connect().await?;
            mcp::serve_stdio(conn).await?;
            Ok(json!({ "status": "mcp server exited" }))
        }
    }
}

/// Minimal timestamp without pulling in a chrono/time dependency --
/// good enough for a locally-generated id; a real broker gives its own.
fn nanos_stamp() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}
