use anyhow::{Context, Result};
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct Message {
    pub id: String,
    pub source: String,
    pub body: String,
    pub received_at: String,
    pub lease_expires_at: Option<String>,
}

pub struct Store {
    conn: Connection,
}

impl Store {
    /// Opens (creating if needed) the SQLite store at `path` and applies schema.
    /// WAL mode is required: it's what lets multiple OS processes (main agent +
    /// subagents, each shelling out to this binary) read/write concurrently
    /// without stepping on each other.
    pub fn open(path: &str) -> Result<Self> {
        let conn = Connection::open(path).with_context(|| format!("opening store at {path}"))?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "busy_timeout", 5000)?;
        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS inbox (
                msg_id           TEXT PRIMARY KEY,
                source           TEXT NOT NULL,
                body             TEXT NOT NULL,
                received_at      TEXT NOT NULL DEFAULT (datetime('now')),
                status           TEXT NOT NULL DEFAULT 'pending'
                                   CHECK (status IN ('pending','leased','delivered')),
                claimed_by       TEXT,
                lease_expires_at TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_inbox_status ON inbox(status, received_at);
            "#,
        )?;
        Ok(Self { conn })
    }

    /// Inserts a message pulled from the broker. Idempotent: broker redelivery
    /// of the same msg_id (its at-least-once guarantee) is a no-op here, so the
    /// broker's delivery semantics never leak into the CLI's exactly-once claim.
    pub fn enqueue(&mut self, msg_id: &str, source: &str, body: &str) -> Result<bool> {
        let changed = self.conn.execute(
            "INSERT INTO inbox (msg_id, source, body) VALUES (?1, ?2, ?3)
             ON CONFLICT(msg_id) DO NOTHING",
            params![msg_id, source, body],
        )?;
        Ok(changed == 1)
    }

    /// Atomically claims the oldest pending message, or reclaims a leased one
    /// whose lease has expired (crash/timeout recovery). BEGIN IMMEDIATE takes
    /// the write lock up front so two processes racing here can never both win.
    pub fn claim_next(&mut self, session_id: &str, lease_secs: i64) -> Result<Option<Message>> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;

        let row = tx
            .query_row(
                "SELECT msg_id, source, body, received_at FROM inbox
                 WHERE status = 'pending'
                    OR (status = 'leased' AND lease_expires_at < datetime('now'))
                 ORDER BY received_at ASC
                 LIMIT 1",
                [],
                |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, String>(2)?,
                        r.get::<_, String>(3)?,
                    ))
                },
            )
            .optional()?;

        let Some((id, source, body, received_at)) = row else {
            tx.commit()?;
            return Ok(None);
        };

        tx.execute(
            "UPDATE inbox
             SET status = 'leased', claimed_by = ?1,
                 lease_expires_at = datetime('now', ?2)
             WHERE msg_id = ?3",
            params![session_id, format!("+{lease_secs} seconds"), id],
        )?;
        tx.commit()?;

        Ok(Some(Message {
            id: id.clone(),
            source,
            body,
            received_at,
            lease_expires_at: Some(format!("+{lease_secs}s")),
        }))
    }

    /// Marks a leased message as finally, permanently delivered — but only if
    /// `session_id` is still the current lease holder. This is the piece that
    /// makes reclaim safe: if a lease expired and someone else reclaimed the
    /// message, the original (crashed/late) claimant's ack must be a no-op,
    /// not a success — otherwise both claimants could believe they delivered
    /// the same message, which defeats exactly-once.
    pub fn ack(&self, msg_id: &str, session_id: &str) -> Result<bool> {
        let changed = self.conn.execute(
            "UPDATE inbox SET status = 'delivered', lease_expires_at = NULL
             WHERE msg_id = ?1 AND status = 'leased' AND claimed_by = ?2",
            params![msg_id, session_id],
        )?;
        Ok(changed == 1)
    }
}
