use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
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
    /// Identity hash of the underlying database file for DB-backed sources,
    /// or companion metadata for paired file-backed formats.
    #[serde(default)]
    pub source_fingerprint: u64,
    /// Hash of the relevant database schema for DB-backed sources.
    #[serde(default)]
    pub schema_fingerprint: u64,
    /// Fingerprint of the normalization and exclusion policy that produced
    /// this cursor. A policy change is a whole-source replacement even when
    /// the underlying file identity is unchanged.
    #[serde(default)]
    pub policy_fingerprint: String,
    /// Causal revision assigned by the serialized checkpoint writer. Zero is
    /// an unassigned in-memory transition and is never persisted to the
    /// causal transition relation.
    #[serde(default)]
    pub checkpoint_revision: u64,
    /// Retry-stable identity for one causal transition.
    #[serde(default)]
    pub operation_id: String,
    /// File identity and boundary captured for a replacement scan.
    #[serde(default)]
    pub scan_inode: u64,
    #[serde(default)]
    pub scan_boundary: u64,
    /// True only after the captured scan boundary has been consumed and the
    /// source identity has been revalidated.
    #[serde(default)]
    pub final_scan_complete: bool,
    /// Durable fail-closed reason. Lifecycle remains in `status`; error detail
    /// is intentionally separate so an error string cannot accidentally make
    /// a generation active.
    #[serde(default)]
    pub block_reason: String,
    #[serde(default)]
    pub compatibility_prepared: bool,
    #[serde(default)]
    pub backend_caught_up: bool,
    /// Same-generation append fence state associated with this cursor.
    #[serde(default)]
    pub append_batch_id: String,
    #[serde(default)]
    pub cache_epoch: u64,
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

    /// Stamp the configured author onto raw/event rows, the only event tables
    /// carrying migration-024 `author` columns.
    pub(crate) fn stamp_author(
        &mut self,
        author: &str,
        raw_events_column: bool,
        events_column: bool,
    ) {
        if author.is_empty() {
            return;
        }

        let mut stamped = 0usize;
        let mut recompute = false;

        if raw_events_column {
            let stats = stamp_author_rows(&mut self.raw_rows, author);
            stamped = stamped.saturating_add(stats.inserted);
            recompute |= stats.replaced;
        } else {
            recompute |= strip_author_rows(&mut self.raw_rows);
        }

        if events_column {
            let stats = stamp_author_rows(&mut self.event_rows, author);
            stamped = stamped.saturating_add(stats.inserted);
            recompute |= stats.replaced;
        } else {
            recompute |= strip_author_rows(&mut self.event_rows);
        }

        if recompute {
            self.recompute_approx_bytes();
        } else if self.approx_bytes > 0 {
            let per_row_bytes = r#""author":"","#.len().saturating_add(author.len());
            self.approx_bytes = self
                .approx_bytes
                .saturating_add(stamped.saturating_mul(per_row_bytes));
        }
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

struct AuthorStampStats {
    inserted: usize,
    replaced: bool,
}

fn stamp_author_rows(rows: &mut [Value], author: &str) -> AuthorStampStats {
    let mut stats = AuthorStampStats {
        inserted: 0,
        replaced: false,
    };

    for row in rows {
        let Some(obj) = row.as_object_mut() else {
            continue;
        };
        if obj
            .insert("author".to_string(), Value::String(author.to_string()))
            .is_some()
        {
            stats.replaced = true;
        } else {
            stats.inserted = stats.inserted.saturating_add(1);
        }
    }

    stats
}

fn strip_author_rows(rows: &mut [Value]) -> bool {
    let mut removed = false;
    for row in rows {
        let Some(obj) = row.as_object_mut() else {
            continue;
        };
        removed |= obj.remove("author").is_some();
    }
    removed
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn batch_with_all_row_kinds() -> RowBatch {
        let mut batch = RowBatch::default();
        batch.push_raw_row(json!({"event_uid": "raw-1"}));
        batch.extend_event_rows(vec![json!({"event_uid": "event-1"})]);
        batch.extend_link_rows(vec![json!({"event_uid": "link-1"})]);
        batch.extend_tool_rows(vec![json!({"event_uid": "tool-1"})]);
        batch.push_error_row(json!({"error_kind": "parse"}));
        batch
    }

    #[test]
    fn stamp_author_covers_raw_and_event_rows_only() {
        let mut batch = batch_with_all_row_kinds();

        batch.stamp_author("alice@example.com", true, true);

        assert_eq!(
            batch.raw_rows[0].get("author").and_then(Value::as_str),
            Some("alice@example.com")
        );
        assert_eq!(
            batch.event_rows[0].get("author").and_then(Value::as_str),
            Some("alice@example.com")
        );
        assert!(batch.link_rows[0].get("author").is_none());
        assert!(batch.tool_rows[0].get("author").is_none());
        assert!(batch.error_rows[0].get("author").is_none());
    }

    #[test]
    fn stamp_author_strips_per_table_when_default_schema_lacks_column() {
        let mut batch = RowBatch::default();
        batch.push_raw_row(json!({"event_uid": "raw-1", "author": "old"}));
        batch.extend_event_rows(vec![json!({"event_uid": "event-1", "author": "old"})]);

        batch.stamp_author("alice@example.com", false, true);

        assert!(batch.raw_rows[0].get("author").is_none());
        assert_eq!(
            batch.event_rows[0].get("author").and_then(Value::as_str),
            Some("alice@example.com")
        );
    }

    #[test]
    fn stamp_author_with_empty_identity_is_a_no_op() {
        let mut batch = batch_with_all_row_kinds();
        let before_bytes = batch.approx_bytes();

        batch.stamp_author("", true, true);

        assert!(batch.raw_rows[0].get("author").is_none());
        assert!(batch.event_rows[0].get("author").is_none());
        assert_eq!(batch.approx_bytes(), before_bytes);
    }

    #[test]
    fn stamp_author_advances_cached_batch_size_by_insert_delta() {
        let author = "alice@example.com";
        let mut batch = batch_with_all_row_kinds();
        let before = batch.approx_bytes();

        batch.stamp_author(author, true, true);

        let expected_delta = 2 * (r#""author":"","#.len() + author.len());
        assert_eq!(batch.approx_bytes(), before + expected_delta);
    }
}
