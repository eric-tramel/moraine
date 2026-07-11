use super::mock_clickhouse::MockState;

pub(crate) fn assert_script_consumed(state: &MockState, expected_requests: usize) {
    let queries = state.queries.lock().expect("queries lock");
    assert_eq!(
        queries.len(),
        expected_requests,
        "captured queries: {queries:#?}"
    );
    drop(queries);
    let scripted = state
        .scripted_responses
        .lock()
        .expect("scripted response lock");
    assert!(
        scripted
            .as_ref()
            .is_some_and(std::collections::VecDeque::is_empty),
        "unconsumed scripted responses remain"
    );
}

/// Regression helper for issue #253: ClickHouse 25.12's new analyzer treats
/// `any(column) AS column` as a nested aggregate because the inner `column`
/// binds to the alias expression. Returns true if the SQL contains that
/// buggy self-alias pattern for the given column (with word-boundary checks
/// on either side, so `t.column` prefixes and `column_raw` suffixes don't
/// trigger false positives).
pub(crate) fn sql_self_aliases_aggregate(sql: &str, column: &str) -> bool {
    let needle = format!("any({column}) AS {column}");
    sql.match_indices(&needle).any(|(idx, _)| {
        let head = sql[..idx].chars().next_back();
        let tail = sql[idx + needle.len()..].chars().next();
        let head_word =
            matches!(head, Some(ch) if ch.is_ascii_alphanumeric() || ch == '_' || ch == '.');
        let tail_word = matches!(tail, Some(ch) if ch.is_ascii_alphanumeric() || ch == '_');
        !head_word && !tail_word
    })
}
