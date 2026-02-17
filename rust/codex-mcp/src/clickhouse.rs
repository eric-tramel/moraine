use crate::config::ClickHouseConfig;
use anyhow::Result;
use moraine_clickhouse::ClickHouseClient as SharedClickHouseClient;
use serde::de::DeserializeOwned;
use serde_json::Value;

#[derive(Clone)]
pub struct ClickHouseClient {
    inner: SharedClickHouseClient,
}

impl ClickHouseClient {
    pub fn new(cfg: ClickHouseConfig) -> Result<Self> {
        Ok(Self {
            inner: SharedClickHouseClient::new(cfg)?,
        })
    }

    pub async fn ping(&self) -> Result<()> {
        self.inner.ping().await
    }

    pub async fn query_json_rows<T: DeserializeOwned>(&self, query: &str) -> Result<Vec<T>> {
        match self.inner.query_json_each_row(query, None).await {
            Ok(rows) => Ok(rows),
            Err(_) => self.inner.query_json_data(query, None).await,
        }
    }

    pub async fn insert_json_rows(&self, table: &str, rows: &[Value]) -> Result<()> {
        self.inner.insert_json_rows(table, rows).await
    }
}
