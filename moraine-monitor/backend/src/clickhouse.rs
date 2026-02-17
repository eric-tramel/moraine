use crate::config::ClickHouseConfig;
use anyhow::Result;
use moraine_clickhouse::ClickHouseClient as SharedClickHouseClient;
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
        match self.inner.query_json_data(query, None).await {
            Ok(rows) => Ok(rows),
            Err(_) => self.inner.query_json_each_row(query, None).await,
        }
    }

    pub async fn ping(&self) -> Result<()> {
        self.inner.ping().await
    }

    pub async fn version(&self) -> Result<String> {
        self.inner.version().await
    }
}
