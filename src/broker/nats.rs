//! NATS JetStream backend, verified with async-nats 0.37 / rustc 1.98.
//!
//! Design note, not a bug: plain (core) NATS is fire-and-forget -- a message
//! published while this CLI isn't running is simply lost, no redelivery. That
//! silently breaks the "agent hook only runs when invoked" model this whole
//! project is built around, so this implementation is JetStream-only. There
//! is no core-NATS fallback here on purpose.

use super::{BrokerMessage, MessageBroker};
use anyhow::{Context, Result};
use async_trait::async_trait;
use async_nats::jetstream::{
    self,
    consumer::{pull::Config as PullConfig, PullConsumer},
    stream::Config as StreamConfig,
};
use futures::StreamExt;

pub struct NatsBroker {
    js: jetstream::Context,
    stream_name: String,
    consumer_name: String,
    subject: String,
}

impl NatsBroker {
    /// `subject` doubles as both the publish subject and (with dots replaced)
    /// the JetStream stream name -- fine for a single-topic setup; a
    /// multi-topic deployment would want the stream/subject mapping made
    /// explicit instead of derived.
    pub async fn connect(nats_url: &str, subject: &str, durable_name: &str) -> Result<Self> {
        let client = async_nats::connect(nats_url)
            .await
            .context("connecting to NATS")?;
        let js = jetstream::new(client);

        let stream_name = subject.replace('.', "_");
        let stream = js
            .get_or_create_stream(StreamConfig {
                name: stream_name.clone(),
                subjects: vec![subject.to_string()],
                ..Default::default()
            })
            .await
            .context("creating/getting JetStream stream")?;

        // Ensure the durable pull consumer exists up front -- drain() re-fetches
        // a handle each call rather than holding one, since this is a short-
        // lived process per invocation.
        stream
            .get_or_create_consumer::<PullConfig>(
                durable_name,
                PullConfig {
                    durable_name: Some(durable_name.to_string()),
                    ..Default::default()
                },
            )
            .await
            .context("creating/getting pull consumer")?;

        Ok(Self {
            js,
            stream_name,
            consumer_name: durable_name.to_string(),
            subject: subject.to_string(),
        })
    }
}

#[async_trait]
impl MessageBroker for NatsBroker {
    async fn publish(&self, topic: &str, body: &[u8]) -> Result<()> {
        let ack_future = self
            .js
            .publish(topic.to_string(), body.to_vec().into())
            .await
            .context("publishing to JetStream")?;
        ack_future
            .await
            .context("waiting for JetStream publish ack")?; // durably stored before we return
        Ok(())
    }

    async fn drain(
        &self,
        max: usize,
        sink: &mut (dyn FnMut(BrokerMessage) -> Result<()> + Send),
    ) -> Result<usize> {
        let stream = self
            .js
            .get_stream(&self.stream_name)
            .await
            .context("getting stream handle")?;
        let consumer: PullConsumer = stream
            .get_consumer(&self.consumer_name)
            .await
            .map_err(|e| anyhow::anyhow!("getting consumer handle: {e}"))?;

        // expires bounds how long we wait if the queue is empty -- this is a
        // one-shot CLI call, it must not hang indefinitely.
        let mut messages = consumer
            .fetch()
            .max_messages(max)
            .expires(std::time::Duration::from_secs(2))
            .messages()
            .await
            .context("pulling messages")?;

        let mut drained = 0;
        while let Some(msg) = messages.next().await {
            let msg = msg.map_err(|e| anyhow::anyhow!("reading pulled message: {e}"))?;

            let id = msg
                .headers
                .as_ref()
                .and_then(|h| h.get("Nats-Msg-Id"))
                .map(|v| v.to_string())
                .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
            let body = String::from_utf8_lossy(&msg.payload).into_owned();

            let bm = BrokerMessage {
                id,
                source: self.subject.clone(),
                body,
            };

            match sink(bm) {
                Ok(()) => {
                    msg.ack().await.map_err(|e| anyhow::anyhow!("ack failed: {e}"))?;
                    drained += 1;
                }
                Err(e) => {
                    // Nak (negative-ack) puts it back for redelivery rather
                    // than losing it -- mirrors the RabbitMQ nack+requeue path.
                    let _ = msg.ack_with(jetstream::AckKind::Nak(None)).await;
                    return Err(e).context("staging drained message into local store");
                }
            }
        }
        Ok(drained)
    }
}
