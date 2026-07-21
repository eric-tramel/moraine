use crate::model::NormalizedRecord;
use crate::sources::shared::{
    compact_json, event_uid, infer_rollout_record_ts_from_file, parse_event_ts, raw_hash,
    resolve_model_hint, truncate_chars, RecordContext, UNPARSEABLE_EVENT_TS,
};
use crate::sources::{registry, NormalizedPartials, Preflight, SourceRecordContext};
use anyhow::{anyhow, Result};
use serde_json::{json, Value};
use std::collections::HashMap;

pub use crate::sources::shared::{infer_session_date_from_file, infer_session_id_from_file};

#[allow(clippy::too_many_arguments)]
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
    cwd_hint: &str,
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
        cwd_hint,
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
    cwd_hint: &str,
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
        source_name,
        source_file,
        session_hint,
        top_type: &top_type,
        base_uid: &base_uid,
    };
    let session_id = source.session_id(record, &source_ctx);
    let cwd = resolve_cwd(&source.cwd(record), cwd_hint);

    let raw_row = json!({
        "source_name": source_name,
        "harness": harness_name,
        "inference_provider": metadata.inference_provider,
        "cwd": cwd,
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
        session_hint,
        session_date: &session_date,
        cwd: &cwd,
        source_file,
        source_inode,
        source_generation,
        source_line_no,
        source_offset,
        record_ts: &record_ts,
        event_ts: &event_ts,
    };

    let mut partials = source.normalize(record, &ctx, &top_type, &base_uid, model_hint);
    bind_derived_event_versions(&mut partials)?;
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
        cwd_hint: cwd,
    })
}

/// Bind each derived row to the exact canonical event revision emitted by the
/// same normalized source record. `event_links.event_version` and
/// `tool_io.event_version` remain independent replacement-order tokens; the
/// causal owner revision is stored separately so live views can reject stale
/// derived keys after a stable event UID is re-emitted. Version zero is
/// reserved for legacy/unbound derived rows and cannot identify an owner.
fn bind_derived_event_versions(partials: &mut NormalizedPartials) -> Result<()> {
    let mut event_versions = HashMap::<String, u64>::with_capacity(partials.event_rows.len());

    for event in &partials.event_rows {
        let event_uid = event
            .get("event_uid")
            .and_then(Value::as_str)
            .filter(|event_uid| !event_uid.is_empty())
            .ok_or_else(|| anyhow!("normalized event is missing a nonempty event_uid"))?;
        let event_version = event
            .get("event_version")
            .and_then(Value::as_u64)
            .ok_or_else(|| anyhow!("normalized event `{event_uid}` is missing event_version"))?;
        if event_version == 0 {
            return Err(anyhow!(
                "normalized event `{event_uid}` has reserved event_version 0"
            ));
        }
        if event_versions
            .insert(event_uid.to_string(), event_version)
            .is_some()
        {
            return Err(anyhow!(
                "normalized record emitted ambiguous duplicate event_uid `{event_uid}`"
            ));
        }
    }

    for (relation, rows) in [
        ("event_links", &mut partials.link_rows),
        ("tool_io", &mut partials.tool_rows),
    ] {
        for row in rows {
            let object = row
                .as_object_mut()
                .ok_or_else(|| anyhow!("normalized {relation} row is not an object"))?;
            let event_uid = object
                .get("event_uid")
                .and_then(Value::as_str)
                .filter(|event_uid| !event_uid.is_empty())
                .ok_or_else(|| {
                    anyhow!("normalized {relation} row is missing a nonempty event_uid")
                })?
                .to_string();
            let event_version = event_versions.get(&event_uid).copied().ok_or_else(|| {
                anyhow!(
                    "normalized {relation} row references event_uid `{event_uid}` without an emitted owner event"
                )
            })?;
            object.insert("source_event_version".to_string(), json!(event_version));
        }
    }

    Ok(())
}

/// Record-level cwd wins; otherwise fall back to the session-level hint
/// chained in by the caller. Whitespace-only values count as absent.
fn resolve_cwd(record_cwd: &str, cwd_hint: &str) -> String {
    let trimmed = record_cwd.trim();
    if !trimmed.is_empty() {
        return trimmed.to_string();
    }

    cwd_hint.trim().to_string()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derived_rows_bind_to_owner_event_version_without_changing_row_version() {
        let mut partials = NormalizedPartials {
            event_rows: vec![json!({"event_uid": "event-1", "event_version": 7})],
            link_rows: vec![json!({"event_uid": "event-1", "event_version": 10})],
            tool_rows: vec![json!({"event_uid": "event-1", "event_version": 11})],
        };

        bind_derived_event_versions(&mut partials).expect("derived owners bind");

        assert_eq!(partials.link_rows[0]["event_version"], json!(10));
        assert_eq!(partials.link_rows[0]["source_event_version"], json!(7));
        assert_eq!(partials.tool_rows[0]["event_version"], json!(11));
        assert_eq!(partials.tool_rows[0]["source_event_version"], json!(7));
    }

    #[test]
    fn derived_rows_fail_closed_without_an_owner_event() {
        let mut partials = NormalizedPartials {
            event_rows: Vec::new(),
            link_rows: vec![json!({"event_uid": "missing", "event_version": 10})],
            tool_rows: Vec::new(),
        };

        let error = bind_derived_event_versions(&mut partials)
            .expect_err("orphan derived rows must fail normalization");
        assert!(error.to_string().contains("without an emitted owner event"));
        assert!(partials.link_rows[0].get("source_event_version").is_none());
    }

    #[test]
    fn duplicate_owner_event_uids_are_ambiguous() {
        let mut partials = NormalizedPartials {
            event_rows: vec![
                json!({"event_uid": "duplicate", "event_version": 7}),
                json!({"event_uid": "duplicate", "event_version": 8}),
            ],
            link_rows: Vec::new(),
            tool_rows: Vec::new(),
        };

        let error = bind_derived_event_versions(&mut partials)
            .expect_err("duplicate owner UIDs must fail normalization");
        assert!(error.to_string().contains("ambiguous duplicate event_uid"));
    }

    #[test]
    fn zero_owner_event_version_is_reserved_for_unbound_derived_rows() {
        let mut partials = NormalizedPartials {
            event_rows: vec![json!({"event_uid": "zero", "event_version": 0})],
            link_rows: vec![json!({"event_uid": "zero", "event_version": 10})],
            tool_rows: Vec::new(),
        };

        let error = bind_derived_event_versions(&mut partials)
            .expect_err("zero cannot be a causal owner revision");
        assert!(error.to_string().contains("reserved event_version 0"));
        assert!(partials.link_rows[0].get("source_event_version").is_none());
    }
}
