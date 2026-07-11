use super::*;

#[tokio::test(flavor = "multi_thread")]
async fn table_summaries_map_metadata_and_active_part_counts() {
    let responses = vec![
        ScriptedResponse::rows(
            &[
                "SELECT name, engine, toUInt8(is_temporary) AS is_temporary",
                "FROM system.tables",
                "WHERE database = 'moraine'",
                "ORDER BY name ASC",
                "FORMAT JSONEachRow",
            ],
            json!([
                {
                    "name": "events",
                    "engine": "ReplacingMergeTree",
                    "is_temporary": 0_u8
                },
                {
                    "name": "scratch",
                    "engine": "Memory",
                    "is_temporary": 1_u8
                },
                {
                    "name": "v_session_summary",
                    "engine": "View",
                    "is_temporary": 0_u8
                }
            ]),
        ),
        ScriptedResponse::rows(
            &[
                "SELECT table, toUInt64(sum(rows)) AS rows",
                "FROM system.parts",
                "WHERE database = 'moraine' AND active",
                "GROUP BY table",
                "ORDER BY table ASC",
                "FORMAT JSONEachRow",
            ],
            json!([
                { "table": "events", "rows": 42_u64 },
                { "table": "scratch", "rows": 7_u64 }
            ]),
        ),
    ];
    let (repo, state) = build_scripted_repo(responses).await;

    let summaries = repo
        .list_table_summaries()
        .await
        .expect("table summaries succeed");

    assert!(summaries.row_counts_error.is_none());
    assert_eq!(summaries.tables.len(), 3);
    assert_eq!(summaries.tables[0].name, "events");
    assert_eq!(summaries.tables[0].engine, "ReplacingMergeTree");
    assert!(!summaries.tables[0].is_temporary);
    assert_eq!(summaries.tables[0].rows, 42);
    assert!(summaries.tables[1].is_temporary);
    assert_eq!(summaries.tables[1].rows, 7);
    assert_eq!(summaries.tables[2].rows, 0);
    assert_script_consumed(&state, 2);
}
#[tokio::test(flavor = "multi_thread")]
async fn table_summaries_return_metadata_when_parts_probe_fails() {
    let responses = vec![
        ScriptedResponse::rows(
            &["FROM system.tables", "FORMAT JSONEachRow"],
            json!([{
                "name": "events",
                "engine": "ReplacingMergeTree",
                "is_temporary": 0_u8
            }]),
        ),
        ScriptedResponse::failure(
            &["FROM system.parts", "FORMAT JSONEachRow"],
            "active parts probe failed",
        ),
    ];
    let (repo, state) = build_scripted_repo(responses).await;

    let summaries = repo
        .list_table_summaries()
        .await
        .expect("parts failure is partial");
    assert_eq!(summaries.tables.len(), 1);
    assert_eq!(summaries.tables[0].rows, 0);
    assert!(summaries
        .row_counts_error
        .as_deref()
        .is_some_and(|message| message.contains("active parts probe failed")));
    assert_script_consumed(&state, 2);
}
#[tokio::test(flavor = "multi_thread")]
async fn preview_rejects_invalid_identifier_before_any_http_call() {
    let (repo, state) = build_repo().await;

    let error = repo
        .preview_table(TablePreviewQuery {
            table: "events; DROP TABLE events".to_string(),
            limit: 25,
        })
        .await
        .expect_err("invalid table identifier is rejected");

    assert!(error
        .to_string()
        .contains("table must match [A-Za-z_][A-Za-z0-9_]*"));
    assert!(state.queries.lock().expect("queries lock").is_empty());
}
#[tokio::test(flavor = "multi_thread")]
async fn preview_clamps_limit_and_maps_ordered_schema_and_json_values() {
    let responses = vec![
        ScriptedResponse::rows(
            &[
                "SELECT name, type AS type_name, default_expression",
                "FROM system.columns",
                "WHERE database = 'moraine' AND table = 'events'",
                "ORDER BY position ASC",
                "FORMAT JSONEachRow",
            ],
            json!([
                {
                    "name": "id",
                    "type_name": "UInt64",
                    "default_expression": ""
                },
                {
                    "name": "name",
                    "type_name": "String",
                    "default_expression": "'anonymous'"
                }
            ]),
        ),
        ScriptedResponse::rows(
            &[
                "SELECT * FROM `moraine`.`events`",
                "ORDER BY `id`, `name`",
                "LIMIT 500",
                "FORMAT JSONEachRow",
            ],
            json!([
                { "id": 1_u64, "name": "alpha", "nested": { "ok": true } },
                { "id": 2_u64, "name": "beta", "nullable": null }
            ]),
        ),
    ];
    let (repo, state) = build_scripted_repo(responses).await;

    let preview = repo
        .preview_table(TablePreviewQuery {
            table: "events".to_string(),
            limit: u16::MAX,
        })
        .await
        .expect("table preview succeeds");

    assert_eq!(preview.table, "events");
    assert_eq!(preview.limit, 500);
    assert_eq!(preview.schema.len(), 2);
    assert_eq!(preview.schema[0].name, "id");
    assert_eq!(preview.schema[0].type_name, "UInt64");
    assert_eq!(preview.schema[1].default_expression, "'anonymous'");
    assert_eq!(preview.rows[0]["nested"]["ok"], json!(true));
    assert_eq!(preview.rows[1]["nullable"], serde_json::Value::Null);
    assert_script_consumed(&state, 2);
}
#[tokio::test(flavor = "multi_thread")]
async fn preview_propagates_schema_and_row_stage_errors() {
    let (schema_repo, schema_state) = build_scripted_repo(vec![ScriptedResponse::failure(
        &["FROM system.columns", "FORMAT JSONEachRow"],
        "preview schema failed",
    )])
    .await;
    let schema_error = schema_repo
        .preview_table(TablePreviewQuery {
            table: "events".to_string(),
            limit: 10,
        })
        .await
        .expect_err("schema failure propagates");
    assert!(schema_error.to_string().contains("preview schema failed"));
    assert_script_consumed(&schema_state, 1);

    let (row_repo, row_state) = build_scripted_repo(vec![
        ScriptedResponse::rows(
            &["FROM system.columns", "FORMAT JSONEachRow"],
            json!([{
                "name": "id",
                "type_name": "UInt64",
                "default_expression": ""
            }]),
        ),
        ScriptedResponse::failure(
            &["SELECT * FROM `moraine`.`events` ORDER BY `id` LIMIT 10 FORMAT JSONEachRow"],
            "preview row read failed",
        ),
    ])
    .await;
    let row_error = row_repo
        .preview_table(TablePreviewQuery {
            table: "events".to_string(),
            limit: 10,
        })
        .await
        .expect_err("row failure propagates");
    assert!(row_error.to_string().contains("preview row read failed"));
    assert_script_consumed(&row_state, 2);
}
