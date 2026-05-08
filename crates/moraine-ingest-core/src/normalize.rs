use crate::model::NormalizedRecord;
use crate::sources::shared::{
    compact_json, event_uid, infer_rollout_record_ts_from_file, parse_event_ts, raw_hash,
    resolve_model_hint, truncate_chars, RecordContext, UNPARSEABLE_EVENT_TS,
};
use crate::sources::{registry, Preflight, SourceRecordContext};
use anyhow::{anyhow, Result};
use serde_json::{json, Value};

pub use crate::sources::shared::{infer_session_date_from_file, infer_session_id_from_file};

pub fn normalize_record(
    record: &Value,
    source_name: &str,
    harness: &str,
    source_file: &str,
    source_inode: u64,
    source_generation: u32,
    source_line_no: u64,
    source_offset: u64,
    session_hint: &str,
    model_hint: &str,
) -> Result<NormalizedRecord> {
    normalize_record_with_ts_hint(
        record,
        source_name,
        harness,
        source_file,
        source_inode,
        source_generation,
        source_line_no,
        source_offset,
        session_hint,
        model_hint,
        "",
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn normalize_record_with_ts_hint(
    record: &Value,
    source_name: &str,
    harness: &str,
    source_file: &str,
    source_inode: u64,
    source_generation: u32,
    source_line_no: u64,
    source_offset: u64,
    session_hint: &str,
    model_hint: &str,
    record_ts_hint: &str,
) -> Result<NormalizedRecord> {
    let sources = registry();
    let source = if sources.is_known(harness) {
        sources
            .get(harness)
            .expect("known source should resolve to a registered source")
    } else {
        return Err(anyhow!(
            "unsupported harness `{}`; expected one of: {}",
            harness.trim(),
            sources.known_harnesses().join(", ")
        ));
    };
    let _source_format = source.format();

    let record = match source.preflight(record) {
        Preflight::Keep(record) => record,
        Preflight::Skip => return Ok(NormalizedRecord::default()),
    };

    let harness_name = source.harness();
    let metadata = source.source_metadata(record);
    let source_record_ts = source.record_ts(record);
    let record_ts = resolve_record_ts(harness_name, source_file, &source_record_ts, record_ts_hint);
    let (event_ts, event_ts_parse_failed) = parse_event_ts(&record_ts);
    let top_type = source.top_type(record);
    let session_date = infer_session_date_from_file(source_file, &record_ts);

    let raw_json = compact_json(record);
    let base_uid = event_uid(
        source_file,
        source_generation,
        source_line_no,
        source_offset,
        &raw_json,
        "raw",
    );

    let source_ctx = SourceRecordContext {
        source_file,
        session_hint,
        top_type: &top_type,
        base_uid: &base_uid,
    };
    let session_id = source.session_id(record, &source_ctx);

    let raw_row = json!({
        "source_name": source_name,
        "harness": harness_name,
        "inference_provider": metadata.inference_provider,
        "source_file": source_file,
        "source_inode": source_inode,
        "source_generation": source_generation,
        "source_line_no": source_line_no,
        "source_offset": source_offset,
        "record_ts": record_ts,
        "top_type": top_type,
        "session_id": session_id,
        "raw_json": raw_json,
        "raw_json_hash": raw_hash(&raw_json),
        "event_uid": base_uid,
    });

    let mut error_rows = Vec::<Value>::new();
    if event_ts_parse_failed {
        error_rows.push(json!({
            "source_name": source_name,
            "harness": harness_name,
            "inference_provider": metadata.inference_provider,
            "source_file": source_file,
            "source_inode": source_inode,
            "source_generation": source_generation,
            "source_line_no": source_line_no,
            "source_offset": source_offset,
            "error_kind": "timestamp_parse_error",
            "error_text": format!(
                "timestamp is missing or not supported ISO8601/RFC3339; used {} UTC fallback",
                UNPARSEABLE_EVENT_TS
            ),
            "raw_fragment": truncate_chars(&record_ts, 20_000),
        }));
    }

    let ctx = RecordContext {
        source_name,
        harness: harness_name,
        inference_provider: &metadata.inference_provider,
        session_id: &session_id,
        session_date: &session_date,
        source_file,
        source_inode,
        source_generation,
        source_line_no,
        source_offset,
        record_ts: &record_ts,
        event_ts: &event_ts,
    };

    let partials = source.normalize(record, &ctx, &top_type, &base_uid, model_hint);
    let hint_fallback = if metadata.model_hint_fallback.is_empty() {
        model_hint
    } else {
        metadata.model_hint_fallback.as_str()
    };
    let model_hint = resolve_model_hint(&partials.event_rows, harness_name, hint_fallback);

    Ok(NormalizedRecord {
        raw_row,
        event_rows: partials.event_rows,
        link_rows: partials.link_rows,
        tool_rows: partials.tool_rows,
        error_rows,
        session_hint: session_id,
        model_hint,
    })
}

fn resolve_record_ts(
    harness: &str,
    source_file: &str,
    source_record_ts: &str,
    record_ts_hint: &str,
) -> String {
    let trimmed = source_record_ts.trim();
    if !trimmed.is_empty() {
        return trimmed.to_string();
    }

    let hint = record_ts_hint.trim();
    if !hint.is_empty() {
        return hint.to_string();
    }

    if harness == "codex" {
        if let Some(file_ts) = infer_rollout_record_ts_from_file(source_file) {
            return file_ts;
        }
    }

    String::new()
}
