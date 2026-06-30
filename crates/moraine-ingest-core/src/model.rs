use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Checkpoint {
    pub source_name: String,
    pub source_file: String,
    pub source_inode: u64,
    pub source_generation: u32,
    pub last_offset: u64,
    pub last_line_no: u64,
    pub status: String,
    /// Structured poll cursor for database-backed sources (issue #361).
    /// Authoritative for `cursor_sqlite`; always empty for file-backed
    /// formats, whose `last_offset`/`last_line_no` semantics are unchanged.
    #[serde(default)]
    pub cursor_json: String,
    /// Identity hash of the underlying database file for DB-backed sources.
    #[serde(default)]
    pub source_fingerprint: u64,
    /// Hash of the relevant database schema for DB-backed sources.
    #[serde(default)]
    pub schema_fingerprint: u64,
}

#[derive(Debug, Clone, Default)]
pub struct NormalizedRecord {
    pub raw_row: Value,
    pub event_rows: Vec<Value>,
    pub link_rows: Vec<Value>,
    pub tool_rows: Vec<Value>,
    pub error_rows: Vec<Value>,
    pub session_hint: String,
    pub model_hint: String,
    /// Resolved working directory for this record (record-level cwd where the
    /// harness carries one, otherwise the caller-supplied session-level hint).
    /// Callers chain it back in like `session_hint`/`model_hint` so records
    /// after a harness's session header inherit the session cwd.
    pub cwd_hint: String,
}

#[derive(Debug, Clone, Default)]
pub struct RowBatch {
    pub raw_rows: Vec<Value>,
    pub event_rows: Vec<Value>,
    pub link_rows: Vec<Value>,
    pub tool_rows: Vec<Value>,
    pub error_rows: Vec<Value>,
    pub checkpoint: Option<Checkpoint>,
    pub lines_processed: u64,
    approx_bytes: usize,
}

impl RowBatch {
    pub fn row_count(&self) -> usize {
        self.raw_rows.len()
            + self.event_rows.len()
            + self.link_rows.len()
            + self.tool_rows.len()
            + self.error_rows.len()
    }

    pub fn approx_bytes(&self) -> usize {
        if self.approx_bytes > 0 {
            return self.approx_bytes;
        }

        self.raw_rows
            .iter()
            .map(approx_json_row_bytes)
            .sum::<usize>()
            + self
                .event_rows
                .iter()
                .map(approx_json_row_bytes)
                .sum::<usize>()
            + self
                .link_rows
                .iter()
                .map(approx_json_row_bytes)
                .sum::<usize>()
            + self
                .tool_rows
                .iter()
                .map(approx_json_row_bytes)
                .sum::<usize>()
            + self
                .error_rows
                .iter()
                .map(approx_json_row_bytes)
                .sum::<usize>()
    }

    pub(crate) fn recompute_approx_bytes(&mut self) {
        self.approx_bytes = 0;
        self.approx_bytes = self.approx_bytes();
    }

    pub fn push_raw_row(&mut self, row: Value) {
        self.approx_bytes = self
            .approx_bytes
            .saturating_add(approx_json_row_bytes(&row));
        self.raw_rows.push(row);
    }

    pub fn push_error_row(&mut self, row: Value) {
        self.approx_bytes = self
            .approx_bytes
            .saturating_add(approx_json_row_bytes(&row));
        self.error_rows.push(row);
    }

    pub fn extend_normalized(&mut self, normalized: NormalizedRecord) {
        if !normalized.raw_row.is_null() {
            self.push_raw_row(normalized.raw_row);
        }
        self.extend_event_rows(normalized.event_rows);
        self.extend_link_rows(normalized.link_rows);
        self.extend_tool_rows(normalized.tool_rows);
        self.extend_error_rows(normalized.error_rows);
    }

    pub fn extend_event_rows<I>(&mut self, rows: I)
    where
        I: IntoIterator<Item = Value>,
    {
        for row in rows {
            self.approx_bytes = self
                .approx_bytes
                .saturating_add(approx_json_row_bytes(&row));
            self.event_rows.push(row);
        }
    }

    pub fn extend_link_rows<I>(&mut self, rows: I)
    where
        I: IntoIterator<Item = Value>,
    {
        for row in rows {
            self.approx_bytes = self
                .approx_bytes
                .saturating_add(approx_json_row_bytes(&row));
            self.link_rows.push(row);
        }
    }

    pub fn extend_tool_rows<I>(&mut self, rows: I)
    where
        I: IntoIterator<Item = Value>,
    {
        for row in rows {
            self.approx_bytes = self
                .approx_bytes
                .saturating_add(approx_json_row_bytes(&row));
            self.tool_rows.push(row);
        }
    }

    pub fn extend_error_rows<I>(&mut self, rows: I)
    where
        I: IntoIterator<Item = Value>,
    {
        for row in rows {
            self.push_error_row(row);
        }
    }

    pub fn drain_to_chunk(&mut self) -> Self {
        let chunk = Self {
            raw_rows: std::mem::take(&mut self.raw_rows),
            event_rows: std::mem::take(&mut self.event_rows),
            link_rows: std::mem::take(&mut self.link_rows),
            tool_rows: std::mem::take(&mut self.tool_rows),
            error_rows: std::mem::take(&mut self.error_rows),
            checkpoint: None,
            lines_processed: self.lines_processed,
            approx_bytes: self.approx_bytes,
        };
        self.lines_processed = 0;
        self.approx_bytes = 0;
        chunk
    }

    pub fn exceeds_limits(&self, max_rows: usize, max_bytes: usize) -> bool {
        let max_rows = max_rows.max(1);
        let max_bytes = max_bytes.max(1);
        self.row_count() >= max_rows || self.approx_bytes() >= max_bytes
    }
}

fn approx_json_row_bytes(row: &Value) -> usize {
    serde_json::to_vec(row)
        .map(|bytes| bytes.len().saturating_add(1))
        .unwrap_or(1)
}
