use crate::config::ClickHouseConfig;
use crate::model::Checkpoint;
use anyhow::Result;
use moraine_clickhouse::ClickHouseClient as SharedClickHouseClient;
use serde_json::Value;
use std::collections::HashMap;

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

    pub async fn insert_json_rows(&self, table: &str, rows: &[Value]) -> Result<()> {
        self.inner.insert_json_rows(table, rows).await
    }

    pub async fn load_checkpoints(&self) -> Result<HashMap<String, Checkpoint>> {
        let query = format!(
            "SELECT source_name, source_file, argMax(source_inode, updated_at), argMax(source_generation, updated_at), argMax(last_offset, updated_at), argMax(last_line_no, updated_at), argMax(status, updated_at) FROM {}.ingest_checkpoints GROUP BY source_name, source_file FORMAT TabSeparated",
            self.inner.config().database
        );

        let raw = self
            .inner
            .request_text(&query, None, None, false, None)
            .await?;

        let mut map = HashMap::<String, Checkpoint>::new();

        for line in raw.lines() {
            if line.trim().is_empty() {
                continue;
            }
            let fields: Vec<&str> = line.split('\t').collect();
            if fields.len() < 7 {
                continue;
            }

            let source_name = fields[0].to_string();
            let source_file = fields[1].to_string();
            let source_inode = fields[2].parse::<u64>().unwrap_or(0);
            let source_generation = fields[3].parse::<u32>().unwrap_or(1);
            let last_offset = fields[4].parse::<u64>().unwrap_or(0);
            let last_line_no = fields[5].parse::<u64>().unwrap_or(0);
            let status = fields[6].to_string();

            map.insert(
                format!("{}\n{}", source_name, source_file),
                Checkpoint {
                    source_name,
                    source_file,
                    source_inode,
                    source_generation,
                    last_offset,
                    last_line_no,
                    status,
                },
            );
        }

        Ok(map)
    }
}
