use serde_json::Value;
use std::sync::OnceLock;

pub(crate) mod claude_code;
pub(crate) mod codex;
pub(crate) mod cursor;
pub(crate) mod emitter;
pub(crate) mod hermes;
pub(crate) mod kimi_cli;
pub(crate) mod opencode;
pub(crate) mod pi;
pub(crate) mod qwen_code;
pub(crate) mod record_view;
pub(crate) mod shared;

use shared::{infer_session_id_from_file, RecordContext};

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SourceFormat {
    Jsonl,
    SessionJson,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum Preflight<'a> {
    Keep(&'a Value),
    Skip,
}

#[derive(Debug, Clone)]
pub(crate) struct SourceMetadata {
    pub(crate) inference_provider: String,
    pub(crate) model_hint_fallback: String,
}

impl SourceMetadata {
    pub(crate) fn new(inference_provider: impl Into<String>) -> Self {
        Self {
            inference_provider: inference_provider.into(),
            model_hint_fallback: String::new(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct SourceRecordContext<'a> {
    pub(crate) source_name: &'a str,
    pub(crate) source_file: &'a str,
    pub(crate) session_hint: &'a str,
    pub(crate) top_type: &'a str,
    pub(crate) base_uid: &'a str,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct NormalizedPartials {
    pub(crate) event_rows: Vec<Value>,
    pub(crate) link_rows: Vec<Value>,
    pub(crate) tool_rows: Vec<Value>,
}

#[allow(dead_code)]
impl NormalizedPartials {
    pub(crate) fn push_event(&mut self, row: Value) {
        self.event_rows.push(row);
    }

    pub(crate) fn push_link(&mut self, row: Value) {
        self.link_rows.push(row);
    }

    pub(crate) fn push_tool(&mut self, row: Value) {
        self.tool_rows.push(row);
    }

    pub(crate) fn append(&mut self, mut other: Self) {
        self.event_rows.append(&mut other.event_rows);
        self.link_rows.append(&mut other.link_rows);
        self.tool_rows.append(&mut other.tool_rows);
    }
}

impl From<(Vec<Value>, Vec<Value>, Vec<Value>)> for NormalizedPartials {
    fn from((event_rows, link_rows, tool_rows): (Vec<Value>, Vec<Value>, Vec<Value>)) -> Self {
        Self {
            event_rows,
            link_rows,
            tool_rows,
        }
    }
}

pub(crate) trait IngestSource: Send + Sync {
    fn harness(&self) -> &'static str;

    fn default_inference_provider(&self) -> Option<&'static str>;

    fn format(&self) -> SourceFormat {
        SourceFormat::Jsonl
    }

    fn preflight<'a>(&self, record: &'a Value) -> Preflight<'a> {
        Preflight::Keep(record)
    }

    fn source_metadata(&self, _record: &Value) -> SourceMetadata {
        SourceMetadata::new(self.default_inference_provider().unwrap_or_default())
    }

    fn record_ts(&self, record: &Value) -> String {
        shared::to_str(record.get("timestamp"))
    }

    fn top_type(&self, record: &Value) -> String {
        shared::to_str(record.get("type"))
    }

    /// Whether file-backed JSONL records can expose a session working
    /// directory. SQLite pollers may still synthesize cwd-bearing records for
    /// an adapter that returns false here.
    fn jsonl_carries_cwd(&self) -> bool {
        false
    }

    /// Working directory carried by the record content itself (never derived
    /// from file paths). Harnesses that only expose a session-level cwd return
    /// it on the records that carry it; the normalizer handles the
    /// session-level fallback for the rest. Defaults to empty for harnesses
    /// without a discoverable cwd.
    fn cwd(&self, _record: &Value) -> String {
        String::new()
    }

    fn session_id(&self, _record: &Value, ctx: &SourceRecordContext<'_>) -> String {
        if ctx.session_hint.is_empty() {
            infer_session_id_from_file(ctx.source_file)
        } else {
            ctx.session_hint.to_string()
        }
    }

    fn normalize(
        &self,
        record: &Value,
        ctx: &RecordContext<'_>,
        top_type: &str,
        base_uid: &str,
        model_hint: &str,
    ) -> NormalizedPartials;
}

pub(crate) struct SourceRegistry {
    sources: Vec<&'static dyn IngestSource>,
}

impl SourceRegistry {
    pub(crate) fn new() -> Self {
        Self {
            sources: Vec::new(),
        }
    }

    pub(crate) fn register<T>(mut self, source: &'static T) -> Self
    where
        T: IngestSource + 'static,
    {
        self.sources.push(source);
        self
    }

    pub(crate) fn get(&self, harness: &str) -> Option<&'static dyn IngestSource> {
        let normalized = harness.trim().to_ascii_lowercase();
        self.sources
            .iter()
            .copied()
            .find(|source| source.harness() == normalized)
    }

    pub(crate) fn is_known(&self, harness: &str) -> bool {
        self.get(harness).is_some()
    }

    pub(crate) fn known_harnesses(&self) -> Vec<&'static str> {
        self.sources.iter().map(|source| source.harness()).collect()
    }
}

pub(crate) fn registry() -> &'static SourceRegistry {
    static REGISTRY: OnceLock<SourceRegistry> = OnceLock::new();
    REGISTRY.get_or_init(|| {
        SourceRegistry::new()
            .register(&codex::CODEX)
            .register(&claude_code::CLAUDE_CODE)
            .register(&cursor::CURSOR)
            .register(&hermes::HERMES)
            .register(&kimi_cli::KIMI_CLI)
            .register(&opencode::OPENCODE)
            .register(&pi::PI_CODING_AGENT)
            .register(&qwen_code::QWEN_CODE)
    })
}

#[cfg(test)]
mod tests {
    use super::registry;

    #[test]
    fn registry_matches_config_known_harnesses() {
        let mut registry_harnesses = registry().known_harnesses();
        let mut config_harnesses = moraine_config::KNOWN_INGEST_HARNESSES.to_vec();
        registry_harnesses.sort_unstable();
        config_harnesses.sort_unstable();
        assert_eq!(registry_harnesses, config_harnesses);
    }
}
