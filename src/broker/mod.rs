pub mod nats;
pub mod rabbitmq;

pub use nats::NatsBroker;
pub use rabbitmq::RabbitMqBroker;

use anyhow::Result;
use async_trait::async_trait;

/// A message pulled from the broker, not yet in our local ledger.
pub struct BrokerMessage {
    pub id: String,
    pub source: String,
    pub body: String,
}

/// Abstraction over the underlying broker so `drain`/`publish` callers (the
/// CLI, the MCP tool) don't care whether it's RabbitMQ or NATS underneath.
#[async_trait]
pub trait MessageBroker: Send + Sync {
    async fn publish(&self, topic: &str, body: &[u8]) -> Result<()>;

    /// Pulls up to `max` messages one at a time (this is a short-lived CLI
    /// process, not a long-running consumer -- see design notes on why a
    /// pull-based fetch loop is deliberate, not a simplification).
    ///
    /// For each message, `sink` is called before the broker ack. If `sink`
    /// returns Ok, we ack (message is now durably in our local ledger). If
    /// `sink` errors (e.g. the SQLite write failed), we nack+redeliver and
    /// stop draining immediately -- same store-then-ack ordering as the
    /// outbox pattern: never ack a broker message before it's durably
    /// recorded on our side, or a crash between the two loses it silently.
    async fn drain(
        &self,
        max: usize,
        sink: &mut (dyn FnMut(BrokerMessage) -> Result<()> + Send),
    ) -> Result<usize>;
}
