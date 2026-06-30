use crate::model::RowBatch;
use crate::sources::shared::{io_hash, raw_hash, truncate_chars, PREVIEW_LIMIT};
use aho_corasick::{AhoCorasick, AhoCorasickBuilder};
use anyhow::{Context, Result};
use moraine_config::RedactionConfig;
use regex::bytes::{Regex, RegexSet};
use serde_json::{json, Map, Value};
use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::sync::Mutex;

#[derive(Clone, Copy, Debug)]
enum EntropyGate {
    None,
    GenericCredential,
}

#[derive(Clone, Debug)]
struct Rule {
    id: String,
    pattern: String,
    regex: Regex,
    keywords: Vec<String>,
    secret_group: usize,
    entropy_gate: EntropyGate,
    min_len: usize,
    priority: usize,
}

#[derive(Debug)]
struct KeywordPrefilter {
    matcher: AhoCorasick,
    rules_by_pattern: Vec<Vec<usize>>,
    always_scan_rules: Vec<usize>,
}

#[derive(Clone, Debug)]
struct RedactionSpan {
    start: usize,
    end: usize,
    rule_id: String,
    priority: usize,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct ScrubbedText {
    pub(crate) text: String,
    pub(crate) counts: BTreeMap<String, u64>,
}

#[derive(Debug, Default)]
pub(crate) struct BatchRedactionReport {
    pub(crate) total: u64,
    pub(crate) per_rule: BTreeMap<String, u64>,
}

#[derive(Debug)]
pub(crate) struct SecretRedactor {
    rules: Vec<Rule>,
    prefilter: Option<KeywordPrefilter>,
    regex_set: RegexSet,
}

#[derive(Debug, Default)]
pub(crate) struct RedactionAudit {
    counts: Mutex<BTreeMap<String, u64>>,
}

impl SecretRedactor {
    pub(crate) fn new(config: &RedactionConfig) -> Result<Self> {
        if config.ruleset != "builtin" {
            return Err(anyhow::anyhow!(
                "invalid redaction.ruleset `{}`; only `builtin` is supported",
                config.ruleset
            ));
        }

        let specs = builtin_rules()?;
        let mut rules = specs
            .iter()
            .enumerate()
            .map(|(priority, spec)| spec.compile(priority))
            .collect::<Result<Vec<_>>>()?;
        for (idx, pattern) in config.extra_patterns.iter().enumerate() {
            let id = format!("extra-pattern-{}", idx + 1);
            rules.push(Rule {
                id: id.clone(),
                pattern: pattern.clone(),
                regex: Regex::new(pattern).with_context(|| {
                    format!("failed to compile redaction.extra_patterns[{idx}] for {id}")
                })?,
                keywords: Vec::new(),
                secret_group: 0,
                entropy_gate: EntropyGate::None,
                min_len: 1,
                priority: rules.len(),
            });
        }
        let regex_set = RegexSet::new(rules.iter().map(|rule| rule.pattern.as_str()))
            .context("failed to compile redaction regex set")?;
        let prefilter = build_prefilter(&rules)?;

        Ok(Self {
            rules,
            prefilter,
            regex_set,
        })
    }

    pub(crate) fn redact_batch(&self, batch: &mut RowBatch) -> BatchRedactionReport {
        let mut report = BatchRedactionReport::default();

        for row in &mut batch.raw_rows {
            let counts = self.redact_raw_row(row);
            report.record(counts);
        }
        for row in &mut batch.event_rows {
            let counts = self.redact_event_row(row);
            report.record(counts);
        }
        for row in &mut batch.tool_rows {
            let counts = self.redact_tool_row(row);
            report.record(counts);
        }
        for row in &mut batch.error_rows {
            let counts = self.redact_error_row(row);
            report.record(counts);
        }

        batch.recompute_approx_bytes();
        report
    }

    pub(crate) fn scrub_text(&self, text: &str) -> ScrubbedText {
        let spans = self.non_overlapping_spans(text);
        if spans.is_empty() {
            return ScrubbedText {
                text: text.to_string(),
                counts: BTreeMap::new(),
            };
        }

        let mut out = String::with_capacity(text.len());
        let mut last = 0usize;
        let mut counts = BTreeMap::<String, u64>::new();
        for span in spans {
            out.push_str(&text[last..span.start]);
            out.push_str("[REDACTED:");
            out.push_str(&span.rule_id);
            out.push(']');
            last = span.end;
            *counts.entry(span.rule_id).or_default() += 1;
        }
        out.push_str(&text[last..]);

        ScrubbedText { text: out, counts }
    }

    fn non_overlapping_spans(&self, text: &str) -> Vec<RedactionSpan> {
        let placeholder_ranges = placeholder_ranges(text);
        let candidate_rules = self.prefilter_candidates(text);
        if candidate_rules.iter().all(|candidate| !candidate) {
            return Vec::new();
        }

        let set_matches = self.regex_set.matches(text.as_bytes());
        let mut spans = Vec::<RedactionSpan>::new();

        for rule_idx in set_matches.iter() {
            if !candidate_rules[rule_idx] {
                continue;
            }
            let rule = &self.rules[rule_idx];

            for captures in rule.regex.captures_iter(text.as_bytes()) {
                let Some(secret) = captures.get(rule.secret_group) else {
                    continue;
                };
                if span_overlaps(&placeholder_ranges, secret.start(), secret.end()) {
                    continue;
                }
                let Ok(candidate) = std::str::from_utf8(secret.as_bytes()) else {
                    continue;
                };
                if !self.should_redact(rule, candidate) {
                    continue;
                }
                spans.push(RedactionSpan {
                    start: secret.start(),
                    end: secret.end(),
                    rule_id: rule.id.clone(),
                    priority: rule.priority,
                });
            }
        }

        spans.sort_by_key(|span| {
            (
                span.start,
                Reverse(span.end.saturating_sub(span.start)),
                span.priority,
            )
        });

        let mut kept = Vec::<RedactionSpan>::new();
        let mut occupied_until = 0usize;
        for span in spans {
            if span.start < occupied_until {
                continue;
            }
            occupied_until = span.end;
            kept.push(span);
        }
        kept
    }

    fn should_redact(&self, rule: &Rule, candidate: &str) -> bool {
        if candidate.len() < rule.min_len || candidate.contains("[REDACTED:") {
            return false;
        }

        match rule.entropy_gate {
            EntropyGate::None => true,
            EntropyGate::GenericCredential => generic_candidate_is_secret(candidate),
        }
    }

    fn prefilter_candidates(&self, text: &str) -> Vec<bool> {
        let mut candidates = vec![false; self.rules.len()];
        let Some(prefilter) = &self.prefilter else {
            candidates.fill(true);
            return candidates;
        };

        for rule_idx in &prefilter.always_scan_rules {
            candidates[*rule_idx] = true;
        }
        for mat in prefilter.matcher.find_overlapping_iter(text.as_bytes()) {
            for rule_idx in &prefilter.rules_by_pattern[mat.pattern().as_usize()] {
                candidates[*rule_idx] = true;
            }
        }

        candidates
    }

    fn redact_raw_row(&self, row: &mut Value) -> BTreeMap<String, u64> {
        let mut counts = BTreeMap::new();
        let Some(obj) = row.as_object_mut() else {
            return counts;
        };

        let raw_changed = redact_string_field(self, obj, "raw_json", &mut counts);
        if raw_changed {
            if let Some(raw_json) = obj.get("raw_json").and_then(Value::as_str) {
                obj.insert("raw_json_hash".to_string(), json!(raw_hash(raw_json)));
            }
        }
        counts
    }

    fn redact_event_row(&self, row: &mut Value) -> BTreeMap<String, u64> {
        let mut counts = BTreeMap::new();
        let Some(obj) = row.as_object_mut() else {
            return counts;
        };

        let text_changed = redact_string_field(self, obj, "text_content", &mut counts);
        redact_string_field(self, obj, "payload_json", &mut counts);

        if text_changed {
            if let Some(text_content) = obj.get("text_content").and_then(Value::as_str) {
                obj.insert(
                    "text_preview".to_string(),
                    Value::String(truncate_chars(text_content, PREVIEW_LIMIT)),
                );
            }
        } else {
            redact_string_field(self, obj, "text_preview", &mut counts);
        }

        counts
    }

    fn redact_tool_row(&self, row: &mut Value) -> BTreeMap<String, u64> {
        let mut counts = BTreeMap::new();
        let Some(obj) = row.as_object_mut() else {
            return counts;
        };

        let input_changed = redact_string_field(self, obj, "input_json", &mut counts);
        let output_json_changed = redact_string_field(self, obj, "output_json", &mut counts);
        let output_text_changed = redact_string_field(self, obj, "output_text", &mut counts);

        if input_changed {
            if let Some(input_json) = obj
                .get("input_json")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
            {
                obj.insert("input_bytes".to_string(), json!(input_json.len() as u32));
                obj.insert(
                    "input_preview".to_string(),
                    Value::String(truncate_chars(&input_json, PREVIEW_LIMIT)),
                );
            }
        } else {
            redact_string_field(self, obj, "input_preview", &mut counts);
        }

        if output_text_changed {
            if let Some(output_text) = obj
                .get("output_text")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
            {
                obj.insert(
                    "output_preview".to_string(),
                    Value::String(truncate_chars(&output_text, PREVIEW_LIMIT)),
                );
            }
        } else {
            redact_string_field(self, obj, "output_preview", &mut counts);
        }

        if output_json_changed {
            if let Some(output_json) = obj
                .get("output_json")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
            {
                obj.insert("output_bytes".to_string(), json!(output_json.len() as u32));
            }
        }

        if input_changed || output_json_changed {
            let input_json = obj.get("input_json").and_then(Value::as_str).unwrap_or("");
            let output_json = obj.get("output_json").and_then(Value::as_str).unwrap_or("");
            obj.insert(
                "io_hash".to_string(),
                json!(io_hash(input_json, output_json)),
            );
        }

        counts
    }

    fn redact_error_row(&self, row: &mut Value) -> BTreeMap<String, u64> {
        let mut counts = BTreeMap::new();
        let Some(obj) = row.as_object_mut() else {
            return counts;
        };

        redact_string_field(self, obj, "raw_fragment", &mut counts);
        counts
    }
}

impl RedactionAudit {
    pub(crate) fn record(&self, report: &BatchRedactionReport) {
        if report.total == 0 {
            return;
        }

        {
            let mut counts = self.counts.lock().expect("redaction audit mutex poisoned");
            for (rule, count) in &report.per_rule {
                *counts.entry(rule.clone()).or_default() += *count;
            }
        }
    }

    pub(crate) fn snapshot(&self) -> BTreeMap<String, u64> {
        self.counts
            .lock()
            .expect("redaction audit mutex poisoned")
            .clone()
    }
}

impl BatchRedactionReport {
    fn record(&mut self, counts: BTreeMap<String, u64>) {
        let row_total: u64 = counts.values().copied().sum();
        if row_total == 0 {
            return;
        }

        for (rule, count) in &counts {
            *self.per_rule.entry(rule.clone()).or_default() += *count;
        }
        self.total += row_total;
    }
}

fn redact_string_field(
    redactor: &SecretRedactor,
    obj: &mut Map<String, Value>,
    field: &str,
    counts: &mut BTreeMap<String, u64>,
) -> bool {
    let Some(Value::String(value)) = obj.get_mut(field) else {
        return false;
    };
    if value.is_empty() {
        return false;
    }

    let scrubbed = redactor.scrub_text(value);
    if scrubbed.counts.is_empty() {
        return false;
    }

    *value = scrubbed.text;
    merge_counts(counts, scrubbed.counts);
    true
}

fn merge_counts(into: &mut BTreeMap<String, u64>, counts: BTreeMap<String, u64>) {
    for (rule, count) in counts {
        *into.entry(rule).or_default() += count;
    }
}

fn build_prefilter(rules: &[Rule]) -> Result<Option<KeywordPrefilter>> {
    let mut keyword_ids = BTreeMap::<String, usize>::new();
    let mut keywords = Vec::<String>::new();
    let mut rules_by_pattern = Vec::<Vec<usize>>::new();
    let mut always_scan_rules = Vec::<usize>::new();

    for (rule_idx, rule) in rules.iter().enumerate() {
        if rule.keywords.is_empty() {
            always_scan_rules.push(rule_idx);
            continue;
        }
        for keyword in &rule.keywords {
            let keyword = keyword.trim().to_ascii_lowercase();
            if keyword.is_empty() {
                continue;
            }
            let keyword_idx = if let Some(keyword_idx) = keyword_ids.get(&keyword) {
                *keyword_idx
            } else {
                let keyword_idx = keywords.len();
                keyword_ids.insert(keyword.clone(), keyword_idx);
                keywords.push(keyword);
                rules_by_pattern.push(Vec::new());
                keyword_idx
            };
            rules_by_pattern[keyword_idx].push(rule_idx);
        }
    }

    if keywords.is_empty() {
        return Ok(None);
    }

    let matcher = AhoCorasickBuilder::new()
        .ascii_case_insensitive(true)
        .build(&keywords)
        .context("failed to compile redaction keyword prefilter")?;
    Ok(Some(KeywordPrefilter {
        matcher,
        rules_by_pattern,
        always_scan_rules,
    }))
}

fn placeholder_ranges(text: &str) -> Vec<(usize, usize)> {
    let mut ranges = Vec::new();
    let mut search_start = 0usize;
    while let Some(relative_start) = text[search_start..].find("[REDACTED:") {
        let start = search_start + relative_start;
        let Some(relative_end) = text[start..].find(']') else {
            break;
        };
        let end = start + relative_end + 1;
        ranges.push((start, end));
        search_start = end;
    }
    ranges
}

fn span_overlaps(ranges: &[(usize, usize)], start: usize, end: usize) -> bool {
    ranges
        .iter()
        .any(|(range_start, range_end)| start < *range_end && end > *range_start)
}

fn builtin_rules() -> Result<Vec<RuleSpec>> {
    let specs = [
        RuleSpec::literal(
            "aws-access-token",
            r"\b((?:A3T[A-Z0-9]|AKIA|ASIA|ABIA|ACCA)[A-Z2-7]{16})\b",
            &["a3t", "akia", "asia", "abia", "acca"],
        ),
        RuleSpec::literal(
            "github-token",
            r"\bgh[pousr]_[0-9A-Za-z_]{36,255}\b",
            &["ghp_", "gho_", "ghu_", "ghs_", "ghr_"],
        ),
        RuleSpec::literal(
            "github-fine-grained-token",
            r"\bgithub_pat_[0-9A-Za-z_]{82,255}\b",
            &["github_pat_"],
        ),
        RuleSpec::literal(
            "gitlab-token",
            r"\b(?:glpat|glft|glcbt|glsoat|gldt)-[0-9A-Za-z_-]{20,}\b",
            &["glpat-", "glft-", "glcbt-", "glsoat-", "gldt-"],
        ),
        RuleSpec::literal(
            "slack-token",
            r"\bxox[pbarose]-[0-9A-Za-z-]{10,}\b",
            &[
                "xoxp-", "xoxb-", "xoxa-", "xoxr-", "xoxo-", "xoxe-", "xoxs-",
            ],
        ),
        RuleSpec::literal(
            "slack-webhook",
            r"https://hooks\.slack\.com/services/[0-9A-Za-z/_+-]{20,}",
            &["hooks.slack.com/services"],
        ),
        RuleSpec::literal("google-api-key", r"\bAIza[0-9A-Za-z_-]{35}\b", &["aiza"]),
        RuleSpec::literal(
            "google-oauth-token",
            r"\bya29\.[0-9A-Za-z_-]{20,}\b",
            &["ya29."],
        ),
        RuleSpec::literal(
            "pem-private-key",
            r"(?s)-----BEGIN (?:[A-Z0-9]+ )?PRIVATE KEY-----.*?-----END (?:[A-Z0-9]+ )?PRIVATE KEY-----",
            &["private key"],
        ),
        RuleSpec::literal(
            "openssh-private-key",
            r"(?s)-----BEGIN OPENSSH PRIVATE KEY-----.*?-----END OPENSSH PRIVATE KEY-----",
            &["openssh private key"],
        ),
        RuleSpec::literal(
            "jwt",
            r"\beyJ[0-9A-Za-z_-]{5,}\.eyJ[0-9A-Za-z_-]{5,}\.[0-9A-Za-z_-]*\b",
            &["eyj"],
        ),
        RuleSpec::literal(
            "stripe-token",
            r"\b(?:sk|rk|pk)_(?:test|live)_[0-9A-Za-z]{10,}\b",
            &[
                "sk_test_", "sk_live_", "rk_test_", "rk_live_", "pk_test_", "pk_live_",
            ],
        ),
        RuleSpec::literal(
            "stripe-webhook-secret",
            r"\bwhsec_[0-9A-Za-z]{20,}\b",
            &["whsec_"],
        ),
        RuleSpec::literal(
            "anthropic-api-key",
            r"\bsk-ant-[0-9A-Za-z_-]{20,}\b",
            &["sk-ant-"],
        ),
        RuleSpec::literal(
            "openai-api-key",
            r"\bsk-(?:proj-)?[0-9A-Za-z_-]{20,}\b",
            &["sk-", "sk-proj-"],
        ),
        RuleSpec::literal("huggingface-token", r"\bhf_[0-9A-Za-z]{30,}\b", &["hf_"]),
        RuleSpec::literal(
            "sendgrid-api-key",
            r"\bSG\.[0-9A-Za-z_-]{20,}\.[0-9A-Za-z_-]{20,}\b",
            &["sg."],
        ),
        RuleSpec::literal(
            "discord-webhook",
            r"https://discord(?:app)?\.com/api/webhooks/[0-9]{6,}/[0-9A-Za-z_-]{20,}",
            &["discord.com/api/webhooks", "discordapp.com/api/webhooks"],
        ),
        RuleSpec::literal("npm-token", r"\bnpm_[0-9A-Za-z]{30,}\b", &["npm_"]),
        RuleSpec::literal("pypi-token", r"\bpypi-[0-9A-Za-z_-]{30,}\b", &["pypi-"]),
        RuleSpec::literal(
            "digitalocean-token",
            r"\bdop_v1_[0-9a-f]{64}\b",
            &["dop_v1_"],
        ),
        RuleSpec::literal("databricks-token", r"\bdapi[0-9a-f]{32}\b", &["dapi"]),
        RuleSpec::literal(
            "dockerhub-token",
            r"\bdckr_pat_[0-9A-Za-z_-]{20,}\b",
            &["dckr_pat_"],
        ),
        RuleSpec::literal(
            "linear-api-key",
            r"\blin_api_[0-9A-Za-z]{30,}\b",
            &["lin_api_"],
        ),
        RuleSpec::literal("notion-token", r"\bsecret_[0-9A-Za-z]{30,}\b", &["secret_"]),
        RuleSpec::generic(
            "generic-api-key",
            r#"(?i)\b(?:api[_-]?key|access[_-]?token|auth(?:orization)?|bearer|client[_-]?secret|credential|passwd|password|private[_-]?key|secret|token)\b["']?\s*[:=]\s*["']?([0-9A-Za-z_./+=-]{20,})["']?"#,
            &[
                "api",
                "access",
                "auth",
                "bearer",
                "client",
                "credential",
                "passwd",
                "password",
                "private",
                "secret",
                "token",
            ],
            1,
        ),
    ];

    Ok(Vec::from(specs))
}

#[derive(Clone, Copy, Debug)]
struct RuleSpec {
    id: &'static str,
    pattern: &'static str,
    keywords: &'static [&'static str],
    secret_group: usize,
    entropy_gate: EntropyGate,
    min_len: usize,
}

impl RuleSpec {
    const fn literal(
        id: &'static str,
        pattern: &'static str,
        keywords: &'static [&'static str],
    ) -> Self {
        Self {
            id,
            pattern,
            keywords,
            secret_group: 0,
            entropy_gate: EntropyGate::None,
            min_len: 1,
        }
    }

    const fn generic(
        id: &'static str,
        pattern: &'static str,
        keywords: &'static [&'static str],
        secret_group: usize,
    ) -> Self {
        Self {
            id,
            pattern,
            keywords,
            secret_group,
            entropy_gate: EntropyGate::GenericCredential,
            min_len: 20,
        }
    }

    fn compile(&self, priority: usize) -> Result<Rule> {
        Ok(Rule {
            id: self.id.to_string(),
            pattern: self.pattern.to_string(),
            regex: Regex::new(self.pattern)
                .with_context(|| format!("failed to compile builtin redaction rule {}", self.id))?,
            keywords: self
                .keywords
                .iter()
                .map(|keyword| keyword.to_ascii_lowercase())
                .collect(),
            secret_group: self.secret_group,
            entropy_gate: self.entropy_gate,
            min_len: self.min_len,
            priority,
        })
    }
}

fn generic_candidate_is_secret(candidate: &str) -> bool {
    let trimmed = candidate.trim_matches(|c: char| matches!(c, '"' | '\'' | '`' | ',' | ';'));
    if trimmed.len() < 20 || is_structural_false_positive(trimmed) {
        return false;
    }

    let entropy = shannon_entropy(trimmed);
    if is_hex(trimmed) {
        return entropy - all_digit_hex_penalty(trimmed) > 3.0;
    }
    if is_base64ish(trimmed) {
        return entropy > 4.5;
    }
    entropy > 3.5
}

fn shannon_entropy(value: &str) -> f64 {
    let mut counts = [0usize; 256];
    let bytes = value.as_bytes();
    if bytes.is_empty() {
        return 0.0;
    }
    for byte in bytes {
        counts[*byte as usize] += 1;
    }
    let len = bytes.len() as f64;
    counts
        .iter()
        .copied()
        .filter(|count| *count > 0)
        .map(|count| {
            let p = count as f64 / len;
            -p * p.log2()
        })
        .sum()
}

fn is_hex(value: &str) -> bool {
    value.len() >= 20 && value.as_bytes().iter().all(|byte| byte.is_ascii_hexdigit())
}

fn is_base64ish(value: &str) -> bool {
    value.len() >= 20
        && value.as_bytes().iter().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(*byte, b'+' | b'/' | b'_' | b'-' | b'=')
        })
}

fn all_digit_hex_penalty(value: &str) -> f64 {
    if value.as_bytes().iter().all(|byte| byte.is_ascii_digit()) {
        1.2 / (value.len() as f64).log2()
    } else {
        0.0
    }
}

fn is_structural_false_positive(value: &str) -> bool {
    let lower = value.to_ascii_lowercase();
    let stopwords = [
        "example",
        "placeholder",
        "changeme",
        "notasecret",
        "not-a-secret",
        "dummy",
        "sample",
        "redacted",
        "password",
        "secret",
        "token",
    ];
    if stopwords.iter().any(|word| lower.contains(word)) {
        return true;
    }

    let mut chars = lower.chars();
    let Some(first) = chars.next() else {
        return true;
    };
    if chars.all(|ch| ch == first) {
        return true;
    }

    let alnum: String = lower
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .collect();
    matches!(
        alnum.as_str(),
        "abcdefghijklmnopqrstuvwxyz"
            | "zyxwvutsrqponmlkjihgfedcba"
            | "01234567890123456789"
            | "12345678901234567890"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use moraine_config::RedactionConfig;

    fn redactor() -> SecretRedactor {
        SecretRedactor::new(&RedactionConfig::default()).expect("builtin redactor compiles")
    }

    fn fixture(parts: &[&str]) -> String {
        parts.concat()
    }

    #[test]
    fn redacts_specific_secret_families() {
        let redactor = redactor();
        let input = format!(
            "{} {} {} {} {} {}",
            fixture(&["aws=", "AKIAIOSFODNN7EXAMPLE"]),
            fixture(&["ghp_", "abcdefghijklmnopqrstuvwxyzABCDE12345"]),
            fixture(&["glpat-", "abcdefABCDEF12345678"]),
            fixture(&["xoxb-", "123456789012-ABCDEFGHIJKL"]),
            fixture(&["AIza", "0123456789abcdefghijklmnopqrstuvwxy"]),
            fixture(&["sk_live_", "abcdefghijklmnop"])
        );

        let scrubbed = redactor.scrub_text(&input);

        assert!(scrubbed.text.contains("[REDACTED:aws-access-token]"));
        assert!(scrubbed.text.contains("[REDACTED:github-token]"));
        assert!(scrubbed.text.contains("[REDACTED:gitlab-token]"));
        assert!(scrubbed.text.contains("[REDACTED:slack-token]"));
        assert!(scrubbed.text.contains("[REDACTED:google-api-key]"));
        assert!(scrubbed.text.contains("[REDACTED:stripe-token]"));
    }

    #[test]
    fn redacts_common_service_token_prefixes() {
        let redactor = redactor();
        let input = format!(
            "{} {} {} {} {} {} {} {} {} {} {} {} {}",
            fixture(&["sk-ant-", "AbCdEfGhIjKlMnOpQrStUvWxYz1234567890"]),
            fixture(&["sk-proj-", "AbCdEfGhIjKlMnOpQrStUvWxYz1234567890"]),
            fixture(&["hf_", "abcdefghijklmnopqrstuvwxyzABCDEFGH"]),
            fixture(&[
                "SG.",
                "ABCDEFGHIJKLMNOPQRSTUV",
                ".abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNO",
            ]),
            fixture(&[
                "https://hooks.slack.com/",
                "services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX",
            ]),
            fixture(&[
                "https://discord.com/api/",
                "webhooks/123456789012345678/AbCdEfGhIjKlMnOpQrStUvWxYz1234567890",
            ]),
            fixture(&["npm_", "abcdefghijklmnopqrstuvwxyzABCDEFGHIJ"]),
            fixture(&["pypi-", "abcdefghijklmnopqrstuvwxyzABCDEFGHIJ"]),
            fixture(&[
                "dop_v1_",
                "0123456789abcdef0123456789abcdef",
                "0123456789abcdef0123456789abcdef",
            ]),
            fixture(&["dapi", "0123456789abcdef0123456789abcdef"]),
            fixture(&["dckr_pat_", "AbCdEfGhIjKlMnOpQrStUvWxYz1234567890"]),
            fixture(&["lin_api_", "abcdefghijklmnopqrstuvwxyzABCDEFGHIJ"]),
            fixture(&["secret_", "abcdefghijklmnopqrstuvwxyzABCDEFGHIJ"])
        );

        let scrubbed = redactor.scrub_text(&input);

        for rule in [
            "anthropic-api-key",
            "openai-api-key",
            "huggingface-token",
            "sendgrid-api-key",
            "slack-webhook",
            "discord-webhook",
            "npm-token",
            "pypi-token",
            "digitalocean-token",
            "databricks-token",
            "dockerhub-token",
            "linear-api-key",
            "notion-token",
        ] {
            assert!(
                scrubbed.text.contains(&format!("[REDACTED:{rule}]")),
                "missing redaction for {rule}: {}",
                scrubbed.text
            );
        }
    }

    #[test]
    fn redacts_pem_and_jwt() {
        let redactor = redactor();
        let input = "-----BEGIN PRIVATE KEY-----\nabc123\n-----END PRIVATE KEY----- jwt=eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjMifQ.signature";

        let scrubbed = redactor.scrub_text(input);

        assert!(scrubbed.text.contains("[REDACTED:pem-private-key]"));
        assert!(scrubbed.text.contains("[REDACTED:jwt]"));
    }

    #[test]
    fn generic_assignment_uses_entropy_gate() {
        let redactor = redactor();
        let scrubbed = redactor.scrub_text(
            r#"password = "passwordpasswordpassword" token = "Vb9pQx7Lm2Nf8Rs4Ty6Ua0Kc3Wd5Ez""#,
        );

        assert!(scrubbed.text.contains("passwordpasswordpassword"));
        assert!(scrubbed
            .text
            .contains(r#"token = "[REDACTED:generic-api-key]""#));
    }

    #[test]
    fn scrub_is_idempotent() {
        let redactor = redactor();
        let once = redactor.scrub_text("token = Vb9pQx7Lm2Nf8Rs4Ty6Ua0Kc3Wd5Ez");
        let twice = redactor.scrub_text(&once.text);

        assert_eq!(once.text, twice.text);
        assert!(twice.counts.is_empty());
    }

    #[test]
    fn placeholders_are_protected_from_future_rules() {
        let redactor = redactor();
        let scrubbed = redactor.scrub_text("[REDACTED:github-token]");

        assert_eq!(scrubbed.text, "[REDACTED:github-token]");
        assert!(scrubbed.counts.is_empty());
    }

    #[test]
    fn extra_pattern_redacts_and_stays_idempotent() {
        let config = RedactionConfig {
            extra_patterns: vec![r"acme_internal_[0-9A-Za-z]{32}".to_string()],
            ..RedactionConfig::default()
        };
        let redactor = SecretRedactor::new(&config).expect("redactor compiles");
        let once = redactor.scrub_text("token=acme_internal_0123456789abcdef0123456789abcdef");
        let twice = redactor.scrub_text(&once.text);

        assert_eq!(once.text, "token=[REDACTED:extra-pattern-1]");
        assert_eq!(once.text, twice.text);
        assert!(twice.counts.is_empty());
    }

    #[test]
    fn bad_extra_pattern_is_startup_error() {
        let config = RedactionConfig {
            extra_patterns: vec!["(".to_string()],
            ..RedactionConfig::default()
        };
        let err = SecretRedactor::new(&config).expect_err("invalid regex should fail");

        assert!(format!("{err:#}").contains("extra_patterns[0]"));
    }

    #[test]
    fn large_secret_free_text_returns_without_matches() {
        let redactor = redactor();
        let input = "z".repeat(2 * 1024 * 1024);

        let scrubbed = redactor.scrub_text(&input);

        assert_eq!(scrubbed.text.len(), input.len());
        assert!(scrubbed.counts.is_empty());
    }

    #[test]
    fn redacts_batch_fields_and_recomputes_derived_values() {
        let redactor = redactor();
        let original_raw = r#"{"token":"ghp_abcdefghijklmnopqrstuvwxyzABCDE12345"}"#;
        let original_input =
            r#"{"api_key":"AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVv1234567890+/"}"#;
        let mut batch = RowBatch::default();
        batch.push_raw_row(json!({
            "session_id": "s1",
            "raw_json": original_raw,
            "raw_json_hash": raw_hash(original_raw)
        }));
        batch.extend_event_rows([json!({
            "session_id": "s1",
            "text_content": "ghp_abcdefghijklmnopqrstuvwxyzABCDE12345",
            "text_preview": "ghp_abcdefghijklmnopqrstuvwxyzABCDE12345",
            "payload_json": "{}"
        })]);
        batch.extend_tool_rows([json!({
            "session_id": "s1",
            "input_json": original_input,
            "output_json": "{}",
            "output_text": "ghp_abcdefghijklmnopqrstuvwxyzABCDE12345",
            "input_bytes": original_input.len() as u32,
            "output_bytes": 2u32,
            "input_preview": original_input,
            "output_preview": "ghp_abcdefghijklmnopqrstuvwxyzABCDE12345",
            "io_hash": io_hash(original_input, "{}")
        })]);
        batch.push_error_row(json!({
            "session_id": "s1",
            "raw_fragment": "malformed ghp_abcdefghijklmnopqrstuvwxyzABCDE12345"
        }));

        let report = redactor.redact_batch(&mut batch);

        assert!(report.total >= 5);
        let raw = &batch.raw_rows[0];
        let redacted_raw = raw.get("raw_json").and_then(Value::as_str).unwrap();
        assert!(redacted_raw.contains("[REDACTED:github-token]"));
        assert_eq!(
            raw.get("raw_json_hash").and_then(Value::as_u64),
            Some(raw_hash(redacted_raw))
        );

        let event = &batch.event_rows[0];
        assert_eq!(
            event.get("text_preview").and_then(Value::as_str),
            Some("[REDACTED:github-token]")
        );

        let tool = &batch.tool_rows[0];
        let input = tool.get("input_json").and_then(Value::as_str).unwrap();
        let output_json = tool.get("output_json").and_then(Value::as_str).unwrap();
        assert!(input.contains("[REDACTED:generic-api-key]"));
        assert_eq!(
            tool.get("io_hash").and_then(Value::as_u64),
            Some(io_hash(input, output_json))
        );
        assert_eq!(
            tool.get("input_bytes").and_then(Value::as_u64),
            Some(input.len() as u64)
        );
        assert_eq!(
            tool.get("output_preview").and_then(Value::as_str),
            Some("[REDACTED:github-token]")
        );

        let error = &batch.error_rows[0];
        assert_eq!(
            error.get("raw_fragment").and_then(Value::as_str),
            Some("malformed [REDACTED:github-token]")
        );
    }

    #[test]
    fn audit_accumulates_rule_counts() {
        let audit = RedactionAudit::default();
        let mut report = BatchRedactionReport::default();
        report.per_rule.insert("github-token".to_string(), 2);
        report.total = 2;

        audit.record(&report);

        assert_eq!(audit.snapshot().get("github-token"), Some(&2));
    }
}
