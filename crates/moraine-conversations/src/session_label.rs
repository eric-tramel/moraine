//! The shared human label for a discovered session.
//!
//! MCP `list_sessions` and the monitor session feed both render a session
//! summary, and issue-599 §5.7 requires the two to agree field by field —
//! including the synthesized fallback. The ladder therefore lives beside
//! [`McpSessionListItem`] rather than in either boundary crate, so a change to
//! it cannot land on one surface only.

use crate::domain::McpSessionListItem;

/// `title` -> `session_summary` -> `session_slug` -> a synthesized descriptor.
///
/// Blank-after-trim values are skipped rather than rendered, so an
/// all-whitespace title cannot present as a label.
pub fn session_display_label(session: &McpSessionListItem) -> String {
    if let Some(label) = session
        .title
        .as_deref()
        .filter(|label| !label.trim().is_empty())
        .or(session.session_summary.as_deref())
        .filter(|label| !label.trim().is_empty())
        .or(session.session_slug.as_deref())
        .filter(|label| !label.trim().is_empty())
    {
        return label.to_string();
    }

    let harness = readable_harness(session.harness.as_deref().unwrap_or("session"));
    let mode = readable_mode(session.mode.as_str());
    let updated_at = compact_utc_datetime(session.last_event_unix_ms);
    let turns = pluralize(u64::from(session.total_turns), "turn", "turns");

    format!("{harness}, {mode}, {updated_at}, {turns}")
}

/// Display casing for a canonical harness id. Must cover
/// `moraine_config::KNOWN_INGEST_HARNESSES` exactly; an unknown id renders
/// verbatim so a newly supported harness degrades to its raw id rather than a
/// wrong name.
pub fn readable_harness(harness: &str) -> &str {
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

/// `Mon D HH:MM UTC`. Proleptic-Gregorian civil arithmetic rather than a
/// calendar dependency, matching how the MCP contract layer already formats
/// wire timestamps.
fn compact_utc_datetime(unix_ms: i64) -> String {
    const MONTHS: [&str; 12] = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];

    let seconds = unix_ms.div_euclid(1000);
    let days = seconds.div_euclid(86_400);
    let seconds_of_day = seconds.rem_euclid(86_400);
    let (_, month, day) = civil_from_days(days);
    let hour = seconds_of_day / 3600;
    let minute = (seconds_of_day % 3600) / 60;
    let month_name = MONTHS
        .get((month as usize).saturating_sub(1))
        .copied()
        .unwrap_or("Jan");

    format!("{month_name} {day} {hour:02}:{minute:02} UTC")
}

/// Howard Hinnant's `civil_from_days`: days since the Unix epoch to a
/// proleptic-Gregorian `(year, month, day)`.
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let day_of_era = z.rem_euclid(146_097);
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = (day_of_year - (153 * month_prime + 2) / 5 + 1) as u32;
    let month = if month_prime < 10 {
        month_prime + 3
    } else {
        month_prime - 9
    } as u32;
    let year = if month <= 2 { year + 1 } else { year };

    (year, month, day)
}

fn pluralize(count: u64, singular: &str, plural: &str) -> String {
    if count == 1 {
        format!("1 {singular}")
    } else {
        format!("{count} {plural}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::ConversationMode;
    use std::collections::BTreeSet;

    fn item() -> McpSessionListItem {
        McpSessionListItem {
            session_id: "sess-label".to_string(),
            first_event_time: "2026-04-30 09:00:00".to_string(),
            first_event_unix_ms: 1_777_554_000_000,
            last_event_time: "2026-04-30 09:10:00".to_string(),
            last_event_unix_ms: 1_777_554_600_000,
            total_turns: 29,
            total_events: 768,
            mode: ConversationMode::WebSearch,
            completed: false,
            title: None,
            source: Some("codex".to_string()),
            harness: Some("codex".to_string()),
            inference_provider: None,
            session_slug: None,
            session_summary: None,
            tool_calls: 0,
        }
    }

    #[test]
    fn synthesized_label_reads_harness_mode_time_and_turns() {
        assert_eq!(
            session_display_label(&item()),
            "Codex, web search, Apr 30 13:10 UTC, 29 turns"
        );
    }

    #[test]
    fn label_ladder_skips_blank_values() {
        let mut session = item();
        session.title = Some("   ".to_string());
        session.session_summary = Some("\t\n".to_string());
        session.session_slug = Some("useful-slug".to_string());
        assert_eq!(session_display_label(&session), "useful-slug");

        session.session_summary = Some("a summary".to_string());
        assert_eq!(session_display_label(&session), "a summary");

        session.title = Some("a title".to_string());
        assert_eq!(session_display_label(&session), "a title");
    }

    #[test]
    fn compact_datetime_matches_civil_calendar_boundaries() {
        // Epoch, a leap day, and a pre-epoch instant: the three cases a naive
        // days/month conversion gets wrong.
        assert_eq!(compact_utc_datetime(0), "Jan 1 00:00 UTC");
        assert_eq!(compact_utc_datetime(1_709_164_800_000), "Feb 29 00:00 UTC");
        assert_eq!(compact_utc_datetime(-1), "Dec 31 23:59 UTC");
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
            ("qwen-code", "Qwen Code"),
            ("future-harness", "future-harness"),
        ];

        for (raw, expected) in cases {
            assert_eq!(readable_harness(raw), expected);
        }

        let covered: BTreeSet<&str> = cases
            .iter()
            .map(|(raw, _)| *raw)
            .filter(|raw| *raw != "future-harness")
            .collect();
        let known: BTreeSet<&str> = moraine_config::KNOWN_INGEST_HARNESSES
            .iter()
            .copied()
            .collect();
        assert_eq!(covered, known);
    }

    #[test]
    fn one_turn_is_singular() {
        let mut session = item();
        session.total_turns = 1;
        assert!(session_display_label(&session).ends_with("1 turn"));
    }
}
