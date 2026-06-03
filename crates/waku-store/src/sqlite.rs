//! SQLite-backed [`MessageStore`].

use std::str::FromStr;

use async_trait::async_trait;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions, SqliteRow};
use sqlx::{QueryBuilder, Row, Sqlite, SqlitePool};
use waku_core::{deterministic_hash, MessageHash, WakuMessage};

use crate::{MessageStore, QueryResult, StoreError, StoreQuery, StoredMessage};

const CREATE_TABLE: &str = "\
CREATE TABLE IF NOT EXISTS messages (
    message_hash     BLOB PRIMARY KEY,
    pubsub_topic     TEXT    NOT NULL,
    content_topic    TEXT    NOT NULL,
    payload          BLOB    NOT NULL,
    version          INTEGER,
    timestamp        INTEGER,
    meta             BLOB,
    rate_limit_proof BLOB,
    ephemeral        INTEGER,
    receiver_time    INTEGER NOT NULL,
    sort_time        INTEGER NOT NULL
)";

const CREATE_INDEX: &str = "\
CREATE INDEX IF NOT EXISTS idx_messages_query
    ON messages (content_topic, sort_time, message_hash)";

/// A SQLite message store.
pub struct SqliteStore {
    pool: SqlitePool,
}

impl SqliteStore {
    /// Open (creating if missing) a SQLite database at `url`, e.g.
    /// `sqlite:store.db` or `sqlite::memory:`.
    pub async fn connect(url: &str) -> Result<Self, StoreError> {
        let opts = SqliteConnectOptions::from_str(url)?.create_if_missing(true);
        let pool = SqlitePoolOptions::new().connect_with(opts).await?;
        let store = Self { pool };
        store.migrate().await?;
        Ok(store)
    }

    /// An ephemeral in-memory store (single connection so it persists for the
    /// lifetime of the store).
    pub async fn in_memory() -> Result<Self, StoreError> {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await?;
        let store = Self { pool };
        store.migrate().await?;
        Ok(store)
    }

    async fn migrate(&self) -> Result<(), StoreError> {
        sqlx::query(CREATE_TABLE).execute(&self.pool).await?;
        sqlx::query(CREATE_INDEX).execute(&self.pool).await?;
        Ok(())
    }

    /// Total number of stored messages.
    pub async fn count(&self) -> Result<u64, StoreError> {
        let row = sqlx::query("SELECT COUNT(*) AS n FROM messages")
            .fetch_one(&self.pool)
            .await?;
        Ok(row.get::<i64, _>("n") as u64)
    }

    /// Delete messages whose `sort_time` is older than `cutoff_ns` (time retention).
    pub async fn delete_older_than(&self, cutoff_ns: i64) -> Result<u64, StoreError> {
        let res = sqlx::query("DELETE FROM messages WHERE sort_time < ?")
            .bind(cutoff_ns)
            .execute(&self.pool)
            .await?;
        Ok(res.rows_affected())
    }

    /// Keep only the `max` most recent messages (capacity retention).
    pub async fn enforce_capacity(&self, max: u64) -> Result<u64, StoreError> {
        let res = sqlx::query(
            "DELETE FROM messages WHERE message_hash IN (
                 SELECT message_hash FROM messages
                 ORDER BY sort_time DESC, message_hash DESC
                 LIMIT -1 OFFSET ?
             )",
        )
        .bind(max as i64)
        .execute(&self.pool)
        .await?;
        Ok(res.rows_affected())
    }

    /// Resolve a cursor hash to its `sort_time` for keyset pagination.
    async fn cursor_sort_time(&self, hash: &MessageHash) -> Result<Option<i64>, StoreError> {
        let row = sqlx::query("SELECT sort_time FROM messages WHERE message_hash = ?")
            .bind(hash.to_vec())
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(|r| r.get::<i64, _>("sort_time")))
    }
}

const SELECT_COLS: &str = "message_hash, pubsub_topic, content_topic, payload, version, \
    timestamp, meta, rate_limit_proof, ephemeral, receiver_time, sort_time";

const SELECT_BY_HASH: &str = "SELECT message_hash, pubsub_topic, content_topic, payload, version, \
    timestamp, meta, rate_limit_proof, ephemeral, receiver_time, sort_time \
    FROM messages WHERE message_hash = ?";

fn row_to_message(row: &SqliteRow) -> Result<WakuMessage, StoreError> {
    Ok(WakuMessage {
        payload: row.try_get("payload")?,
        content_topic: row.try_get("content_topic")?,
        version: row.try_get::<Option<i64>, _>("version")?.map(|v| v as u32),
        timestamp: row.try_get("timestamp")?,
        meta: row.try_get("meta")?,
        rate_limit_proof: row.try_get("rate_limit_proof")?,
        ephemeral: row.try_get::<Option<i64>, _>("ephemeral")?.map(|v| v != 0),
    })
}

fn row_to_hash(row: &SqliteRow) -> Result<MessageHash, StoreError> {
    let bytes: Vec<u8> = row.try_get("message_hash")?;
    bytes
        .as_slice()
        .try_into()
        .map_err(|_| StoreError::Corrupt(format!("message_hash is {} bytes", bytes.len())))
}

#[async_trait]
impl MessageStore for SqliteStore {
    async fn put(
        &self,
        pubsub_topic: &str,
        msg: &WakuMessage,
        receiver_time_ns: i64,
    ) -> Result<(), StoreError> {
        let hash = deterministic_hash(pubsub_topic, msg);
        let sort_time = msg.timestamp.unwrap_or(receiver_time_ns);

        sqlx::query(
            "INSERT OR IGNORE INTO messages (message_hash, pubsub_topic, content_topic, payload, \
             version, timestamp, meta, rate_limit_proof, ephemeral, receiver_time, sort_time) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(hash.to_vec())
        .bind(pubsub_topic)
        .bind(msg.content_topic.as_str())
        .bind(msg.payload.clone())
        .bind(msg.version.map(|v| v as i64))
        .bind(msg.timestamp)
        .bind(msg.meta.clone())
        .bind(msg.rate_limit_proof.clone())
        .bind(msg.ephemeral.map(|b| b as i64))
        .bind(receiver_time_ns)
        .bind(sort_time)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn get(&self, hash: &MessageHash) -> Result<Option<WakuMessage>, StoreError> {
        let row = sqlx::query(SELECT_BY_HASH)
            .bind(hash.to_vec())
            .fetch_optional(&self.pool)
            .await?;
        row.as_ref().map(row_to_message).transpose()
    }

    async fn query(&self, query: &StoreQuery) -> Result<QueryResult, StoreError> {
        let cursor_key = match &query.cursor {
            Some(c) => self.cursor_sort_time(c).await?.map(|t| (t, c.to_vec())),
            None => None,
        };

        let mut qb =
            QueryBuilder::<Sqlite>::new(format!("SELECT {SELECT_COLS} FROM messages WHERE 1=1"));

        if let Some(pt) = &query.pubsub_topic {
            qb.push(" AND pubsub_topic = ").push_bind(pt.clone());
        }
        if !query.content_topics.is_empty() {
            qb.push(" AND content_topic IN (");
            let mut sep = qb.separated(", ");
            for ct in &query.content_topics {
                sep.push_bind(ct.clone());
            }
            qb.push(")");
        }
        if !query.message_hashes.is_empty() {
            qb.push(" AND message_hash IN (");
            let mut sep = qb.separated(", ");
            for h in &query.message_hashes {
                sep.push_bind(h.to_vec());
            }
            qb.push(")");
        }
        if let Some(start) = query.time_start_ns {
            qb.push(" AND sort_time >= ").push_bind(start);
        }
        if let Some(end) = query.time_end_ns {
            qb.push(" AND sort_time < ").push_bind(end);
        }

        // Keyset cursor: continue strictly after (sort_time, hash) in sort order.
        if let Some((ct, ch)) = &cursor_key {
            let cmp = if query.forward { ">" } else { "<" };
            qb.push(" AND (sort_time ")
                .push(cmp)
                .push(" ")
                .push_bind(*ct)
                .push(" OR (sort_time = ")
                .push_bind(*ct)
                .push(" AND message_hash ")
                .push(cmp)
                .push(" ")
                .push_bind(ch.clone())
                .push("))");
        }

        let dir = if query.forward { "ASC" } else { "DESC" };
        qb.push(" ORDER BY sort_time ")
            .push(dir)
            .push(", message_hash ")
            .push(dir);

        let limit = query.page_size.max(1) as i64;
        qb.push(" LIMIT ").push_bind(limit + 1);

        let rows = qb.build().fetch_all(&self.pool).await?;
        let has_more = rows.len() as i64 > limit;
        let take = rows.len().min(limit as usize);

        let mut messages = Vec::with_capacity(take);
        for row in &rows[..take] {
            messages.push(StoredMessage {
                hash: row_to_hash(row)?,
                pubsub_topic: row.try_get("pubsub_topic")?,
                message: if query.include_data {
                    Some(row_to_message(row)?)
                } else {
                    None
                },
                receiver_time_ns: row.try_get("receiver_time")?,
            });
        }

        let next_cursor = if has_more {
            messages.last().map(|m| m.hash)
        } else {
            None
        };
        Ok(QueryResult {
            messages,
            next_cursor,
        })
    }

    async fn message_count(&self) -> Result<u64, StoreError> {
        self.count().await
    }

    async fn exists(&self, hashes: &[MessageHash]) -> Result<Vec<MessageHash>, StoreError> {
        if hashes.is_empty() {
            return Ok(Vec::new());
        }
        let mut qb = QueryBuilder::<Sqlite>::new(
            "SELECT message_hash FROM messages WHERE message_hash IN (",
        );
        let mut sep = qb.separated(", ");
        for h in hashes {
            sep.push_bind(h.to_vec());
        }
        qb.push(")");

        let rows = qb.build().fetch_all(&self.pool).await?;
        rows.iter().map(row_to_hash).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn msg(content_topic: &str, payload: &[u8], ts: i64) -> WakuMessage {
        let mut m = WakuMessage::new(content_topic, payload.to_vec());
        m.timestamp = Some(ts);
        m
    }

    #[tokio::test]
    async fn put_get_roundtrip_and_dedup() {
        let store = SqliteStore::in_memory().await.unwrap();
        let topic = "/waku/2/rs/1/0";
        let mut m = msg("/app/1/x/proto", b"hello", 1000);
        m.meta = Some(b"m".to_vec());
        m.version = Some(1);

        store.put(topic, &m, 1234).await.unwrap();
        store.put(topic, &m, 1234).await.unwrap(); // dedup
        assert_eq!(store.count().await.unwrap(), 1);

        let hash = deterministic_hash(topic, &m);
        let got = store.get(&hash).await.unwrap().expect("present");
        assert_eq!(got.payload, m.payload);
        assert_eq!(got.content_topic, m.content_topic);
        assert_eq!(got.meta, m.meta);
        assert_eq!(got.version, m.version);
        assert_eq!(got.timestamp, m.timestamp);

        assert!(store.get(&[0u8; 32]).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn query_filters_orders_and_paginates() {
        let store = SqliteStore::in_memory().await.unwrap();
        let topic = "/waku/2/rs/1/0";
        for i in 0..5i64 {
            store
                .put(topic, &msg("/app/1/chat/proto", &[i as u8], 100 + i), 0)
                .await
                .unwrap();
        }
        // A message on a different content topic must be filtered out.
        store
            .put(topic, &msg("/app/1/other/proto", b"x", 102), 0)
            .await
            .unwrap();

        // Page 1: first 3 of the chat topic, ascending by time.
        let q = StoreQuery::new()
            .content_topic("/app/1/chat/proto")
            .page_size(3);
        let page1 = store.query(&q).await.unwrap();
        assert_eq!(page1.messages.len(), 3);
        assert_eq!(page1.messages[0].message.as_ref().unwrap().payload, vec![0]);
        assert_eq!(page1.messages[2].message.as_ref().unwrap().payload, vec![2]);
        let cursor = page1.next_cursor.expect("more pages");

        // Page 2: the remaining 2.
        let page2 = store.query(&q.clone().cursor(cursor)).await.unwrap();
        assert_eq!(page2.messages.len(), 2);
        assert_eq!(page2.messages[0].message.as_ref().unwrap().payload, vec![3]);
        assert!(page2.next_cursor.is_none());

        // Time range [101, 103) → payloads 1 and 2.
        let ranged = store
            .query(
                &StoreQuery::new()
                    .content_topic("/app/1/chat/proto")
                    .time_range(101, 103),
            )
            .await
            .unwrap();
        let payloads: Vec<u8> = ranged
            .messages
            .iter()
            .map(|m| m.message.as_ref().unwrap().payload[0])
            .collect();
        assert_eq!(payloads, vec![1, 2]);
    }

    #[tokio::test]
    async fn include_data_false_returns_hashes_only() {
        let store = SqliteStore::in_memory().await.unwrap();
        let topic = "/waku/2/rs/1/0";
        let m = msg("/app/1/x/proto", b"hi", 5);
        store.put(topic, &m, 0).await.unwrap();

        let mut q = StoreQuery::new();
        q.include_data = false;
        let res = store.query(&q).await.unwrap();
        assert_eq!(res.messages.len(), 1);
        assert!(res.messages[0].message.is_none());
        assert_eq!(res.messages[0].hash, deterministic_hash(topic, &m));
    }

    #[tokio::test]
    async fn exists_and_retention() {
        let store = SqliteStore::in_memory().await.unwrap();
        let topic = "/waku/2/rs/1/0";
        let mut hashes = Vec::new();
        for i in 0..4i64 {
            let m = msg("/app/1/x/proto", &[i as u8], 10 + i);
            store.put(topic, &m, 0).await.unwrap();
            hashes.push(deterministic_hash(topic, &m));
        }

        let mut probe = hashes.clone();
        probe.push([9u8; 32]); // not stored
        let found = store.exists(&probe).await.unwrap();
        assert_eq!(found.len(), 4);

        // Time retention: drop sort_time < 12 (payloads 0,1).
        assert_eq!(store.delete_older_than(12).await.unwrap(), 2);
        assert_eq!(store.count().await.unwrap(), 2);

        // Capacity retention: keep only the newest 1.
        assert_eq!(store.enforce_capacity(1).await.unwrap(), 1);
        assert_eq!(store.count().await.unwrap(), 1);
    }
}
