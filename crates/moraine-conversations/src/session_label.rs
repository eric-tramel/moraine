pub(crate) struct SessionDisplayLabelInput<'a> {
    pub(crate) explicit_title: Option<&'a str>,
    pub(crate) user_message_preview: Option<&'a str>,
    pub(crate) wire_title: Option<&'a str>,
    pub(crate) summary: Option<&'a str>,
    pub(crate) slug: Option<&'a str>,
    pub(crate) harness: Option<&'a str>,
    pub(crate) mode: &'a str,
    pub(crate) updated_at: &'a str,
    pub(crate) total_turns: u32,
}

pub(crate) fn is_genuine_codex_user_message(
    harness: &str,
    actor_kind: &str,
    event_kind: &str,
    payload_type: &str,
) -> bool {
    harness == "codex"
        && actor_kind == "user"
        && event_kind == "event_msg"
        && payload_type == "user_message"
}

/// Resolve the shared discovery label without exposing a raw prompt field.
///
/// Precedence is explicit title/name, a bounded genuine user-message preview,
/// the existing wire title, summary, slug, then a privacy-safe descriptor.
pub(crate) fn build_session_display_label(input: SessionDisplayLabelInput<'_>) -> String {
    if let Some(label) = input
        .explicit_title
        .filter(|label| !label.trim().is_empty())
    {
        return label.to_string();
    }
    if let Some(label) = input
        .user_message_preview
        .and_then(compact_user_message_preview)
    {
        return label;
    }
    if let Some(label) = input
        .wire_title
        .filter(|label| !label.trim().is_empty())
        .or(input.summary)
        .filter(|label| !label.trim().is_empty())
        .or(input.slug)
        .filter(|label| !label.trim().is_empty())
    {
        return label.to_string();
    }

    let harness = readable_harness(input.harness.unwrap_or("session"));
    let mode = readable_mode(input.mode);
    let updated_at = input.updated_at.trim();
    let turns = if input.total_turns == 1 {
        "1 turn".to_string()
    } else {
        format!("{} turns", input.total_turns)
    };
    format!("{harness}, {mode}, {updated_at}, {turns}")
}

fn compact_user_message_preview(preview: &str) -> Option<String> {
    let trimmed = preview.trim();
    let first_line = trimmed.lines().next()?.trim();
    if first_line.is_empty() {
        return None;
    }
    if first_line.chars().count() <= 120 {
        Some(first_line.to_string())
    } else {
        Some(format!(
            "{}…",
            first_line.chars().take(120).collect::<String>()
        ))
    }
}

fn readable_harness(harness: &str) -> &str {
    match harness {
        "codex" => "Codex",
        "claude-code" => "Claude Code",
        "cursor" => "Cursor",
        "hermes" => "Hermes",
        "kiro-cli" => "Kiro CLI",
        "kimi-cli" => "Kimi CLI",
        "nac" => "NAC",
        "opencode" => "OpenCode",
        "pi-coding-agent" => "Pi Coding Agent",
        "prime-agent" => "Prime Agent",
        "qwen-code" => "Qwen Code",
        _ => harness,
    }
}

fn readable_mode(mode: &str) -> &'static str {
    match mode {
        "web_search" => "web search",
        "tool_calling" => "tool session",
        "mcp_internal" => "MCP session",
        "chat" => "chat",
        _ => "session",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input<'a>() -> SessionDisplayLabelInput<'a> {
        SessionDisplayLabelInput {
            explicit_title: None,
            user_message_preview: None,
            wire_title: None,
            summary: None,
            slug: None,
            harness: Some("codex"),
            mode: "web_search",
            updated_at: "2026-04-30 13:10:00.000",
            total_turns: 29,
        }
    }

    #[test]
    fn genuine_codex_prompt_requires_full_event_provenance() {
        assert!(is_genuine_codex_user_message(
            "codex",
            "user",
            "event_msg",
            "user_message"
        ));
        assert!(!is_genuine_codex_user_message(
            "prime-agent",
            "user",
            "event_msg",
            "user_message"
        ));
        assert!(!is_genuine_codex_user_message(
            "codex", "user", "message", "message"
        ));
    }

    #[test]
    fn explicit_title_precedes_prompt_and_fallbacks() {
        let mut value = input();
        value.explicit_title = Some("Explicit title");
        value.user_message_preview = Some("Genuine prompt");
        value.wire_title = Some("Wire title");
        assert_eq!(build_session_display_label(value), "Explicit title");
    }

    #[test]
    fn prompt_is_first_line_and_unicode_scalar_safe() {
        let long = "é".repeat(121);
        let mut value = input();
        value.user_message_preview = Some(&long);
        assert_eq!(
            build_session_display_label(value),
            format!("{}…", "é".repeat(120))
        );

        let mut value = input();
        value.user_message_preview = Some(
            "  First line
second line  ",
        );
        assert_eq!(build_session_display_label(value), "First line");
    }

    #[test]
    fn readable_harness_covers_canonical_harness_ids() {
        let cases = [
            ("codex", "Codex"),
            ("claude-code", "Claude Code"),
            ("cursor", "Cursor"),
            ("hermes", "Hermes"),
            ("kiro-cli", "Kiro CLI"),
            ("kimi-cli", "Kimi CLI"),
            ("nac", "NAC"),
            ("opencode", "OpenCode"),
            ("pi-coding-agent", "Pi Coding Agent"),
            ("prime-agent", "Prime Agent"),
            ("qwen-code", "Qwen Code"),
            ("future-harness", "future-harness"),
        ];
        for (raw, expected) in cases {
            assert_eq!(readable_harness(raw), expected);
        }

        let covered = cases[..cases.len() - 1]
            .iter()
            .map(|(raw, _)| *raw)
            .collect::<std::collections::BTreeSet<_>>();
        let known = moraine_config::KNOWN_INGEST_HARNESSES
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(covered, known);
    }

    #[test]
    fn blank_values_fall_through_to_privacy_safe_descriptor() {
        let mut value = input();
        value.explicit_title = Some("  ");
        value.user_message_preview = Some(
            "
",
        );
        value.wire_title = Some("	");
        assert!(build_session_display_label(value).starts_with("Codex, web search,"));
    }
}
