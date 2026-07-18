/// Moraine MCP tools whose own calls must not appear as ordinary user search
/// results or tool-calling sessions. `search` is the legacy tool name retained
/// for historical rows.
pub const INTERNAL_TOOL_NAMES: &[&str] = &[
    "search",
    "search_sessions",
    "open",
    "list_sessions",
    "file_attention",
];

/// Return the canonical Moraine MCP leaf name for bare historical names and
/// Qwen Code's `mcp__<server>__<tool>` representation.
pub fn canonical_internal_tool_name(name: &str) -> Option<&'static str> {
    let normalized = name.trim().to_ascii_lowercase();
    let leaf = if let Some(qualified) = normalized.strip_prefix("mcp__") {
        let mut parts = qualified.split("__");
        let server = parts.next()?;
        let leaf = parts.next()?;
        if server != "moraine" || parts.next().is_some() {
            return None;
        }
        leaf
    } else {
        normalized.as_str()
    };

    INTERNAL_TOOL_NAMES
        .iter()
        .copied()
        .find(|candidate| *candidate == leaf)
}

pub fn is_internal_tool_name(name: &str) -> bool {
    canonical_internal_tool_name(name).is_some()
}

/// Build a ClickHouse predicate for a trusted column expression. Callers own
/// the column expression; values are compared against this module's fixed
/// allowlist and the structured Qwen server alias is required to be `moraine`.
pub fn sql_predicate(column: &str) -> String {
    let normalized = format!("lowerUTF8(trimBoth({column}))");
    let parts = format!("splitByString('__', {normalized})");
    let names_sql = INTERNAL_TOOL_NAMES
        .iter()
        .map(|name| format!("'{name}'"))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "({normalized} IN ({names_sql}) OR (length({parts}) = 3 AND arrayElement({parts}, 1) = 'mcp' AND arrayElement({parts}, 2) = 'moraine' AND arrayElement({parts}, 3) IN ({names_sql})))"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonicalizes_every_bare_and_qwen_qualified_internal_tool() {
        for leaf in INTERNAL_TOOL_NAMES {
            assert_eq!(canonical_internal_tool_name(leaf), Some(*leaf));
            assert_eq!(
                canonical_internal_tool_name(&format!("mcp__moraine__{leaf}")),
                Some(*leaf)
            );
            assert_eq!(
                canonical_internal_tool_name(&format!("MCP__MORAINE__{}", leaf.to_uppercase())),
                Some(*leaf)
            );
        }
    }

    #[test]
    fn rejects_other_servers_malformed_names_and_unrelated_moraine_tools() {
        for name in [
            "mcp__other__search_sessions",
            "mcp__moraine__search_sessions__extra",
            "mcp__moraine",
            "mcp____open",
            "mcp__moraine__delete_everything",
            "delete_everything",
            "",
        ] {
            assert_eq!(canonical_internal_tool_name(name), None, "{name}");
        }
    }

    #[test]
    fn sql_predicate_is_column_parameterized_and_alias_scoped() {
        let predicate = sql_predicate("tool_name");
        assert!(predicate.contains("lowerUTF8(trimBoth(tool_name))"));
        assert!(predicate.contains("arrayElement"));
        assert!(predicate.contains("= 'moraine'"));
        for leaf in INTERNAL_TOOL_NAMES {
            assert!(predicate.contains(&format!("'{leaf}'")));
        }
        assert!(!predicate.contains("mcp__*"));
    }
}
