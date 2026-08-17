pub(super) fn sql_quote(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "''"))
}

pub(super) fn sql_identifier(value: &str) -> String {
    format!("`{}`", value.replace('`', "``"))
}

pub(super) fn canonical_events_source(events_ref: &str) -> String {
    // `v_live_events` already applies ReplacingMergeTree collapse and exact
    // source-head authorization. Keeping the wrapper makes it safe for outer
    // predicates and preserves the query-builder contract.
    format!("(SELECT * FROM {events_ref})")
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

#[cfg(test)]
mod tests {
    use super::canonical_events_source;

    #[test]
    fn canonical_source_wraps_final_for_outer_predicates() {
        assert_eq!(
            canonical_events_source("`moraine`.`v_live_events`"),
            "(SELECT * FROM `moraine`.`v_live_events`)"
        );
    }
}
