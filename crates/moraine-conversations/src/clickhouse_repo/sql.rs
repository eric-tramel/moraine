pub(super) fn sql_quote(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "''"))
}

pub(super) fn sql_identifier(value: &str) -> String {
    format!("`{}`", value.replace('`', "``"))
}

pub(super) fn sql_array_strings(items: &[String]) -> String {
    let parts = items.iter().map(|item| sql_quote(item)).collect::<Vec<_>>();
    format!("[{}]", parts.join(","))
}

pub(super) fn sql_array_f64(items: &[f64]) -> String {
    let parts = items
        .iter()
        .map(|v| format!("{:.12}", v))
        .collect::<Vec<_>>();
    format!("[{}]", parts.join(","))
}

/// Shared canonical-read assertions for SQL-shape tests.
///
/// Extracted in issue #597 (risk R9): `assert_no_projection` previously existed
/// as two `#[cfg(test)]`-private copies, one in `canonical_open.rs` and one
/// inlined in `canonical_list.rs`. A third copy for the search builders is how
/// one of them quietly stops asserting — the whole point of the gate is that
/// every canonical reader answers to the SAME predicate.
#[cfg(test)]
pub(super) mod canonical_assertions {
    /// The issue-598 exit gate: a canonical reader statement names no
    /// `mcp_open_*` relation. `context` identifies the builder so a failure
    /// points at one statement rather than a wall of SQL.
    pub(in crate::clickhouse_repo) fn assert_no_projection(context: &str, sql: &str) {
        assert!(
            !sql.contains("mcp_open_"),
            "{context} touched mcp_open_*:\n{sql}"
        );
    }

    /// No legacy view chain either. `mcp_open_*` absence alone is satisfiable by
    /// a statement that reads `v_session_summary` / `v_conversation_trace`,
    /// which are the projector-era aggregates the canonical readers replace.
    pub(in crate::clickhouse_repo) fn assert_no_legacy_view_chain(context: &str, sql: &str) {
        assert!(
            !sql.contains("v_session_summary") && !sql.contains("v_conversation_trace"),
            "{context} touched a legacy view chain:\n{sql}"
        );
    }

    /// Content-freeness for the statements that must never decompress a wide
    /// column. Denominated on the column NAMES appearing anywhere in the
    /// statement, because a columnar engine reads exactly what is named.
    pub(in crate::clickhouse_repo) fn assert_content_free(context: &str, sql: &str) {
        for column in ["text_content", "payload_json"] {
            assert!(
                !names_identifier(sql, column),
                "{context} must be content-free but names `{column}`:\n{sql}"
            );
        }
    }

    /// Whole-identifier containment. Substring matching would flag
    /// `text_content_digest` — a 32-byte fixed-width column whose entire purpose
    /// is to avoid reading `text_content` — and the predictable response to a
    /// guard that fires on the fix is to weaken the guard.
    fn names_identifier(sql: &str, identifier: &str) -> bool {
        let is_ident = |byte: u8| byte == b'_' || byte.is_ascii_alphanumeric();
        let bytes = sql.as_bytes();
        sql.match_indices(identifier).any(|(start, _)| {
            let end = start + identifier.len();
            let before_ok = start == 0 || !is_ident(bytes[start - 1]);
            let after_ok = end == bytes.len() || !is_ident(bytes[end]);
            before_ok && after_ok
        })
    }
}
