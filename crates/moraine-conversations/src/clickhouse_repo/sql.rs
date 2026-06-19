pub(super) fn sql_quote(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "''"))
}

pub(super) fn sql_identifier(value: &str) -> String {
    format!("`{}`", value.replace('`', "``"))
}

pub(super) fn truncated_utf8_sql(column: &str, max_chars: usize) -> String {
    let max_chars = max_chars.max(4);
    let prefix_chars = max_chars.saturating_sub(3);
    format!(
        "if(lengthUTF8({column}) > {max_chars}, concat(leftUTF8({column}, {prefix_chars}), '...'), {column})"
    )
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
