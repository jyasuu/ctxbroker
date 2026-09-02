//! UNVERIFIED: written against documented `lapin` 2.x APIs from memory/training,
//! not compiled. rustc in the build environment caps at 1.75; lapin's async
//! runtime dependencies need edition2024. Run `cargo check` on a real toolchain
//! (1.85+) before trusting this -- likely fixes needed: exact `lapin` option
//! struct field names, and whether `BasicGetOptions`/`BasicNackOptions` still
//! shape this way in whatever 2.x patch you land on.

use super::{BrokerMessage, MessageBroker};
use anyhow::{Context, Result};
use async_trait::async_trait;
use lapin::{
    options::{
        BasicAckOptions, BasicGetOptions, BasicNackOptions, BasicPublishOptions,
        QueueDeclareOptions,
    },
    types::FieldTable,
    BasicProperties, Connection, ConnectionProperties,
};

pub struct RabbitMqBroker {
    channel: lapin::Channel,
    queue: String,
}

impl RabbitMqBroker {
    pub async fn connect(uri: &str, queue: &str) -> Result<Self> {
        let conn = Connection::connect(uri, ConnectionProperties::default())
            .await
            .context("connecting to RabbitMQ")?;
        let channel = conn.create_channel().await?;
        channel
            .queue_declare(
                queue,
                QueueDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .context("declaring queue")?;
        Ok(Self {
            channel,
            queue: queue.to_string(),
        })
    }
}

#[async_trait]
impl MessageBroker for RabbitMqBroker {
    async fn publish(&self, topic: &str, body: &[u8]) -> Result<()> {
        self.channel
            .basic_publish(
                "", // default exchange: routing key == queue name
                topic,
                BasicPublishOptions::default(),
                body,
                BasicProperties::default().with_delivery_mode(2), // persistent
            )
            .await?
            .await?; // wait for broker confirm
        Ok(())
    }

    async fn drain(
        &self,
        max: usize,
        sink: &mut dyn FnMut(BrokerMessage) -> Result<()>,
    ) -> Result<usize> {
        let mut drained = 0;
        for _ in 0..max {
            let Some(delivery) = self
                .channel
                .basic_get(&self.queue, BasicGetOptions { no_ack: false })
                .await?
            else {
                break; // queue empty
            };

            let id = delivery
                .properties
                .message_id()
                .as_ref()
                .map(|m| m.to_string())
                .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
            let body = String::from_utf8_lossy(&delivery.data).into_owned();

            let msg = BrokerMessage {
                id,
                source: self.queue.clone(),
                body,
            };

            match sink(msg) {
                Ok(()) => {
                    delivery.ack(BasicAckOptions::default()).await?;
                    drained += 1;
                }
                Err(e) => {
                    delivery
                        .nack(BasicNackOptions {
                            requeue: true,
                            ..Default::default()
                        })
                        .await?;
                    return Err(e).context("staging drained message into local store");
                }
            }
        }
        Ok(drained)
    }
}
