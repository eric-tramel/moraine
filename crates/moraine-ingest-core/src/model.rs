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
}

impl RowBatch {
    pub fn row_count(&self) -> usize {
        self.raw_rows.len()
            + self.event_rows.len()
            + self.link_rows.len()
            + self.tool_rows.len()
            + self.error_rows.len()
    }
}
