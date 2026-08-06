use crate::config::ClickHouseConfig;
use anyhow::Result;
use moraine_clickhouse::{ClickHouseClient as SharedClickHouseClient, QueryOwner, QueryWorkload};
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
        let owner = QueryOwner::new(&self.inner.runtime(), QueryWorkload::Internal)?;
        owner.scope(self.inner.ping()).await
    }

    pub async fn query_json_rows<T: DeserializeOwned>(&self, query: &str) -> Result<Vec<T>> {
        let owner = QueryOwner::new(&self.inner.runtime(), QueryWorkload::Mcp)?;
        owner.scope(self.inner.query_rows(query, None)).await
    }

    pub async fn insert_json_rows(&self, table: &str, rows: &[Value]) -> Result<()> {
        let owner = QueryOwner::new(&self.inner.runtime(), QueryWorkload::Mcp)?;
        owner.scope(self.inner.insert_json_rows(table, rows)).await
    }
}
