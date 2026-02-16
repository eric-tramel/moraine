use anyhow::{Context, Result};
use serde::Deserialize;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IngestSource {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub provider: String,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    #[serde(default)]
    pub glob: String,
    #[serde(default)]
    pub watch_root: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClickHouseConfig {
    #[serde(default = "default_ch_url")]
    pub url: String,
    #[serde(default = "default_ch_database")]
    pub database: String,
    #[serde(default = "default_ch_username")]
    pub username: String,
    #[serde(default)]
    pub password: String,
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: f64,
    #[serde(default = "default_true")]
    pub async_insert: bool,
    #[serde(default = "default_true")]
    pub wait_for_async_insert: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IngestConfig {
    #[serde(default = "default_sources")]
    pub sources: Vec<IngestSource>,
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    #[serde(default = "default_flush_interval_seconds")]
    pub flush_interval_seconds: f64,
    #[serde(default = "default_state_dir")]
    pub state_dir: String,
    #[serde(default = "default_true")]
    pub backfill_on_start: bool,
    #[serde(default = "default_max_file_workers")]
    pub max_file_workers: usize,
    #[serde(default = "default_max_inflight_batches")]
    pub max_inflight_batches: usize,
    #[serde(default = "default_debounce_ms")]
    pub debounce_ms: u64,
    #[serde(default = "default_reconcile_interval_seconds")]
    pub reconcile_interval_seconds: f64,
    #[serde(default = "default_heartbeat_interval_seconds")]
    pub heartbeat_interval_seconds: f64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpConfig {
    #[serde(default = "default_max_results")]
    pub max_results: u16,
    #[serde(default = "default_preview_chars")]
    pub preview_chars: u16,
    #[serde(default = "default_context_before")]
    pub default_context_before: u16,
    #[serde(default = "default_context_after")]
    pub default_context_after: u16,
    #[serde(default = "default_false")]
    pub default_include_tool_events: bool,
    #[serde(default = "default_true")]
    pub default_exclude_codex_mcp: bool,
    #[serde(default = "default_false")]
    pub async_log_writes: bool,
    #[serde(default = "default_protocol_version")]
    pub protocol_version: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Bm25Config {
    #[serde(default = "default_k1")]
    pub k1: f64,
    #[serde(default = "default_b")]
    pub b: f64,
    #[serde(default = "default_min_score")]
    pub default_min_score: f64,
    #[serde(default = "default_min_should_match")]
    pub default_min_should_match: u16,
    #[serde(default = "default_max_query_terms")]
    pub max_query_terms: usize,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MonitorConfig {
    #[serde(default = "default_monitor_host")]
    pub host: String,
    #[serde(default = "default_monitor_port")]
    pub port: u16,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeConfig {
    #[serde(default = "default_runtime_root")]
    pub root_dir: String,
    #[serde(default = "default_runtime_logs_dir")]
    pub logs_dir: String,
    #[serde(default = "default_runtime_pids_dir")]
    pub pids_dir: String,
    #[serde(default = "default_service_bin_dir")]
    pub service_bin_dir: String,
    #[serde(default = "default_managed_clickhouse_dir")]
    pub managed_clickhouse_dir: String,
    #[serde(default = "default_clickhouse_start_timeout_seconds")]
    pub clickhouse_start_timeout_seconds: f64,
    #[serde(default = "default_healthcheck_interval_ms")]
    pub healthcheck_interval_ms: u64,
    #[serde(default = "default_true")]
    pub clickhouse_auto_install: bool,
    #[serde(default = "default_clickhouse_version")]
    pub clickhouse_version: String,
    #[serde(default = "default_true")]
    pub start_monitor_on_up: bool,
    #[serde(default = "default_false")]
    pub start_mcp_on_up: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppConfig {
    #[serde(default)]
    pub clickhouse: ClickHouseConfig,
    #[serde(default)]
    pub ingest: IngestConfig,
    #[serde(default)]
    pub mcp: McpConfig,
    #[serde(default)]
    pub bm25: Bm25Config,
    #[serde(default)]
    pub monitor: MonitorConfig,
    #[serde(default)]
    pub runtime: RuntimeConfig,
}

impl Default for ClickHouseConfig {
    fn default() -> Self {
        Self {
            url: default_ch_url(),
            database: default_ch_database(),
            username: default_ch_username(),
            password: String::new(),
            timeout_seconds: default_timeout_seconds(),
            async_insert: true,
            wait_for_async_insert: true,
        }
    }
}

impl Default for IngestConfig {
    fn default() -> Self {
        Self {
            sources: default_sources(),
            batch_size: default_batch_size(),
            flush_interval_seconds: default_flush_interval_seconds(),
            state_dir: default_state_dir(),
            backfill_on_start: true,
            max_file_workers: default_max_file_workers(),
            max_inflight_batches: default_max_inflight_batches(),
            debounce_ms: default_debounce_ms(),
            reconcile_interval_seconds: default_reconcile_interval_seconds(),
            heartbeat_interval_seconds: default_heartbeat_interval_seconds(),
        }
    }
}

impl Default for McpConfig {
    fn default() -> Self {
        Self {
            max_results: default_max_results(),
            preview_chars: default_preview_chars(),
            default_context_before: default_context_before(),
            default_context_after: default_context_after(),
            default_include_tool_events: false,
            default_exclude_codex_mcp: true,
            async_log_writes: false,
            protocol_version: default_protocol_version(),
        }
    }
}

impl Default for Bm25Config {
    fn default() -> Self {
        Self {
            k1: default_k1(),
            b: default_b(),
            default_min_score: default_min_score(),
            default_min_should_match: default_min_should_match(),
            max_query_terms: default_max_query_terms(),
        }
    }
}

impl Default for MonitorConfig {
    fn default() -> Self {
        Self {
            host: default_monitor_host(),
            port: default_monitor_port(),
        }
    }
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            root_dir: default_runtime_root(),
            logs_dir: default_runtime_logs_dir(),
            pids_dir: default_runtime_pids_dir(),
            service_bin_dir: default_service_bin_dir(),
            managed_clickhouse_dir: default_managed_clickhouse_dir(),
            clickhouse_start_timeout_seconds: default_clickhouse_start_timeout_seconds(),
            healthcheck_interval_ms: default_healthcheck_interval_ms(),
            clickhouse_auto_install: true,
            clickhouse_version: default_clickhouse_version(),
            start_monitor_on_up: true,
            start_mcp_on_up: false,
        }
    }
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            clickhouse: ClickHouseConfig::default(),
            ingest: IngestConfig::default(),
            mcp: McpConfig::default(),
            bm25: Bm25Config::default(),
            monitor: MonitorConfig::default(),
            runtime: RuntimeConfig::default(),
        }
    }
}

fn default_ch_url() -> String {
    "http://127.0.0.1:8123".to_string()
}

fn default_ch_database() -> String {
    "cortex".to_string()
}

fn default_ch_username() -> String {
    "default".to_string()
}

fn default_timeout_seconds() -> f64 {
    30.0
}

fn default_enabled() -> bool {
    true
}

fn default_sources() -> Vec<IngestSource> {
    vec![
        IngestSource {
            name: "codex".to_string(),
            provider: "codex".to_string(),
            enabled: true,
            glob: "~/.codex/sessions/**/*.jsonl".to_string(),
            watch_root: "~/.codex/sessions".to_string(),
        },
        IngestSource {
            name: "claude".to_string(),
            provider: "claude".to_string(),
            enabled: true,
            glob: "~/.claude/projects/**/*.jsonl".to_string(),
            watch_root: "~/.claude/projects".to_string(),
        },
    ]
}

fn default_batch_size() -> usize {
    4000
}

fn default_flush_interval_seconds() -> f64 {
    0.5
}

fn default_state_dir() -> String {
    "~/.cortex/ingestor".to_string()
}

fn default_max_file_workers() -> usize {
    8
}

fn default_max_inflight_batches() -> usize {
    64
}

fn default_debounce_ms() -> u64 {
    50
}

fn default_reconcile_interval_seconds() -> f64 {
    30.0
}

fn default_heartbeat_interval_seconds() -> f64 {
    5.0
}

fn default_max_results() -> u16 {
    25
}

fn default_preview_chars() -> u16 {
    320
}

fn default_context_before() -> u16 {
    3
}

fn default_context_after() -> u16 {
    3
}

fn default_protocol_version() -> String {
    "2024-11-05".to_string()
}

fn default_k1() -> f64 {
    1.2
}

fn default_b() -> f64 {
    0.75
}

fn default_min_score() -> f64 {
    0.0
}

fn default_min_should_match() -> u16 {
    1
}

fn default_max_query_terms() -> usize {
    32
}

fn default_monitor_host() -> String {
    "127.0.0.1".to_string()
}

fn default_monitor_port() -> u16 {
    8080
}

fn default_runtime_root() -> String {
    "~/.cortex".to_string()
}

fn default_runtime_logs_dir() -> String {
    "logs".to_string()
}

fn default_runtime_pids_dir() -> String {
    "run".to_string()
}

fn default_service_bin_dir() -> String {
    "~/.local/lib/cortex/current/bin".to_string()
}

fn default_managed_clickhouse_dir() -> String {
    "~/.local/lib/cortex/clickhouse/current".to_string()
}

fn default_clickhouse_start_timeout_seconds() -> f64 {
    30.0
}

fn default_healthcheck_interval_ms() -> u64 {
    500
}

fn default_clickhouse_version() -> String {
    "v25.12.5.44-stable".to_string()
}

fn default_true() -> bool {
    true
}

fn default_false() -> bool {
    false
}

pub fn expand_path(path: &str) -> String {
    if let Some(stripped) = path.strip_prefix("~/") {
        if let Some(home) = std::env::var_os("HOME") {
            return format!("{}/{}", home.to_string_lossy(), stripped);
        }
    }
    path.to_string()
}

pub fn watch_root_from_glob(glob_pattern: &str) -> String {
    if let Some(idx) = glob_pattern.find("/**") {
        return glob_pattern[..idx].to_string();
    }

    if let Some(idx) = glob_pattern.find('*') {
        let prefix = glob_pattern[..idx].trim_end_matches('/');
        if !prefix.is_empty() {
            return prefix.to_string();
        }
        let path = Path::new(prefix);
        if let Some(parent) = path.parent() {
            return parent.to_string_lossy().to_string();
        }
    }

    Path::new(glob_pattern)
        .parent()
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_else(|| glob_pattern.to_string())
}

fn home_config_path() -> Option<PathBuf> {
    std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".cortex").join("config.toml"))
}

fn repo_default_config_path() -> PathBuf {
    PathBuf::from("config/cortex.toml")
}

fn resolve_config_path_with_overrides(
    raw_path: Option<PathBuf>,
    env_keys: &[&str],
    home_path: Option<PathBuf>,
    repo_default: PathBuf,
) -> PathBuf {
    if let Some(path) = raw_path {
        return path;
    }

    for key in env_keys {
        if let Ok(value) = std::env::var(key) {
            let trimmed = value.trim();
            if !trimmed.is_empty() {
                return PathBuf::from(trimmed);
            }
        }
    }

    if let Some(path) = home_path {
        if path.exists() {
            return path;
        }
    }

    if repo_default.exists() {
        return repo_default;
    }

    home_config_path().unwrap_or(repo_default)
}

pub fn resolve_config_path(raw_path: Option<PathBuf>) -> PathBuf {
    resolve_config_path_with_overrides(
        raw_path,
        &["CORTEX_CONFIG"],
        home_config_path(),
        repo_default_config_path(),
    )
}

pub fn resolve_mcp_config_path(raw_path: Option<PathBuf>) -> PathBuf {
    resolve_config_path_with_overrides(
        raw_path,
        &["CORTEX_MCP_CONFIG", "CORTEX_CONFIG"],
        home_config_path(),
        repo_default_config_path(),
    )
}

pub fn resolve_monitor_config_path(raw_path: Option<PathBuf>) -> PathBuf {
    resolve_config_path_with_overrides(
        raw_path,
        &["CORTEX_MONITOR_CONFIG", "CORTEX_CONFIG"],
        home_config_path(),
        repo_default_config_path(),
    )
}

pub fn resolve_ingest_config_path(raw_path: Option<PathBuf>) -> PathBuf {
    resolve_config_path_with_overrides(
        raw_path,
        &["CORTEX_INGEST_CONFIG", "CORTEX_CONFIG"],
        home_config_path(),
        repo_default_config_path(),
    )
}

fn resolve_runtime_subdir(root: &str, value: &str) -> String {
    let expanded = expand_path(value);
    let path = Path::new(&expanded);
    if path.is_absolute() {
        return expanded;
    }

    Path::new(root).join(path).to_string_lossy().to_string()
}

fn normalize_config(mut cfg: AppConfig) -> AppConfig {
    for source in &mut cfg.ingest.sources {
        source.glob = expand_path(&source.glob);
        source.watch_root = if source.watch_root.trim().is_empty() {
            watch_root_from_glob(&source.glob)
        } else {
            expand_path(&source.watch_root)
        };
    }

    cfg.ingest.state_dir = expand_path(&cfg.ingest.state_dir);
    cfg.runtime.root_dir = expand_path(&cfg.runtime.root_dir);
    cfg.runtime.logs_dir = resolve_runtime_subdir(&cfg.runtime.root_dir, &cfg.runtime.logs_dir);
    cfg.runtime.pids_dir = resolve_runtime_subdir(&cfg.runtime.root_dir, &cfg.runtime.pids_dir);
    cfg.runtime.service_bin_dir = expand_path(&cfg.runtime.service_bin_dir);
    cfg.runtime.managed_clickhouse_dir = expand_path(&cfg.runtime.managed_clickhouse_dir);

    cfg
}

pub fn load_config(path: impl AsRef<Path>) -> Result<AppConfig> {
    let content = std::fs::read_to_string(path.as_ref())
        .with_context(|| format!("failed to read config {}", path.as_ref().display()))?;
    let cfg: AppConfig = toml::from_str(&content).context("failed to parse TOML config")?;
    Ok(normalize_config(cfg))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_temp_config(contents: &str, label: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "cortex-config-{label}-{}-{}.toml",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time after unix epoch")
                .as_nanos()
        ));
        std::fs::write(&path, contents).expect("write temp config");
        path
    }

    #[test]
    fn resolve_order_prefers_cli_then_env_then_home_then_repo() {
        let raw = Some(PathBuf::from("/tmp/cli.toml"));
        let chosen = resolve_config_path_with_overrides(
            raw,
            &["CORTEX_CONFIG"],
            Some(PathBuf::from("/tmp/home.toml")),
            PathBuf::from("/tmp/repo.toml"),
        );
        assert_eq!(chosen, PathBuf::from("/tmp/cli.toml"));
    }

    #[test]
    fn watch_root_extracts_prefix() {
        assert_eq!(watch_root_from_glob("/tmp/a/**/*.jsonl"), "/tmp/a");
        assert_eq!(watch_root_from_glob("/tmp/a/*.jsonl"), "/tmp/a");
    }

    #[test]
    fn runtime_subdir_joins_relative_paths() {
        let root = "/tmp/cortex";
        assert_eq!(
            resolve_runtime_subdir(root, "logs"),
            "/tmp/cortex/logs".to_string()
        );
        assert_eq!(
            resolve_runtime_subdir(root, "/var/tmp/cortex"),
            "/var/tmp/cortex".to_string()
        );
    }

    #[test]
    fn resolve_order_prefers_env_over_home_and_repo() {
        let env_key = "CORTEX_CONFIG_TEST_KEY";
        std::env::set_var(env_key, "/tmp/from-env.toml");

        let chosen = resolve_config_path_with_overrides(
            None,
            &[env_key],
            Some(PathBuf::from("/tmp/from-home.toml")),
            PathBuf::from("/tmp/from-repo.toml"),
        );

        std::env::remove_var(env_key);
        assert_eq!(chosen, PathBuf::from("/tmp/from-env.toml"));
    }

    #[test]
    fn resolve_order_uses_repo_when_home_missing() {
        let repo_default = std::env::temp_dir().join("cortex-config-repo-default.toml");
        std::fs::write(&repo_default, "x=1").expect("write temp repo default");

        let chosen = resolve_config_path_with_overrides(
            None,
            &["CORTEX_CONFIG_TEST_DOES_NOT_EXIST"],
            Some(PathBuf::from("/tmp/definitely-missing-home.toml")),
            repo_default.clone(),
        );

        std::fs::remove_file(&repo_default).ok();
        assert_eq!(chosen, repo_default);
    }

    #[test]
    fn mcp_config_env_has_priority_over_generic_env() {
        std::env::set_var("CORTEX_MCP_CONFIG", "/tmp/mcp.toml");
        std::env::set_var("CORTEX_CONFIG", "/tmp/generic.toml");

        let chosen = resolve_mcp_config_path(None);

        std::env::remove_var("CORTEX_MCP_CONFIG");
        std::env::remove_var("CORTEX_CONFIG");
        assert_eq!(chosen, PathBuf::from("/tmp/mcp.toml"));
    }

    #[test]
    fn load_config_errors_when_path_missing() {
        let path = std::env::temp_dir().join("cortex-missing-config-does-not-exist.toml");
        let err = load_config(&path).expect_err("missing config path should fail");
        assert!(
            err.to_string().contains("failed to read config"),
            "unexpected error: {err:#}"
        );
    }

    #[test]
    fn load_config_errors_on_unknown_top_level_section() {
        let path = write_temp_config(
            r#"
[clickhouse]
url = "http://127.0.0.1:8123"

[unexpected]
enabled = true
"#,
            "unknown-top-level",
        );
        let err = load_config(&path).expect_err("unknown top-level section should fail");
        std::fs::remove_file(&path).ok();
        assert!(
            format!("{err:#}").contains("unknown field `unexpected`"),
            "unexpected error: {err:#}"
        );
    }

    #[test]
    fn load_config_errors_on_unknown_ingest_source_key() {
        let path = write_temp_config(
            r#"
[[ingest.sources]]
name = "codex"
provider = "codex"
enabled = true
glob = "~/.codex/sessions/**/*.jsonl"
watch_root = "~/.codex/sessions"
extra = "not-allowed"
"#,
            "unknown-source-key",
        );
        let err = load_config(&path).expect_err("unknown ingest source key should fail");
        std::fs::remove_file(&path).ok();
        assert!(
            format!("{err:#}").contains("unknown field `extra`"),
            "unexpected error: {err:#}"
        );
    }
}
