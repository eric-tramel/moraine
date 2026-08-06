use crate::config::ClickHouseConfig;
use anyhow::Result;
use moraine_clickhouse::{ClickHouseClient as SharedClickHouseClient, QueryOwner, QueryWorkload};
use serde::de::DeserializeOwned;

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

    pub fn config(&self) -> &ClickHouseConfig {
        self.inner.config()
    }

    pub async fn query_rows<T: DeserializeOwned>(&self, query: &str) -> Result<Vec<T>> {
        let owner = QueryOwner::new(&self.inner.runtime(), QueryWorkload::Monitor)?;
        owner.scope(self.inner.query_rows(query, None)).await
    }

    pub async fn ping(&self) -> Result<()> {
        let owner = QueryOwner::new(&self.inner.runtime(), QueryWorkload::Internal)?;
        owner.scope(self.inner.ping()).await
    }

    pub async fn version(&self) -> Result<String> {
        let owner = QueryOwner::new(&self.inner.runtime(), QueryWorkload::Monitor)?;
        owner.scope(self.inner.version()).await
    }
}
