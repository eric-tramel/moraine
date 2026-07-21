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
