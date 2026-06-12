use anyhow::{Context, Result};
use serde::Deserialize;
use std::path::{Component, Path, PathBuf};

pub const KNOWN_INGEST_HARNESSES: &[&str] = &[
    "codex",
    "claude-code",
    "cursor",
    "hermes",
    "kimi-cli",
    "pi-coding-agent",
];

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IngestSource {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub harness: String,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    #[serde(default)]
    pub glob: String,
    #[serde(default)]
    pub watch_root: String,
    /// On-disk trace format: `"jsonl"` (append-only newline-delimited records,
    /// the default used by Codex, Claude Code, Kimi CLI, and Hermes ShareGPT
    /// dumps), `"session_json"` (single-file-per-session JSON rewritten in
    /// place via atomic rename — used by live Hermes agent sessions), or
    /// `"cursor_sqlite"` (polled Cursor `state.vscdb` SQLite databases). Empty
    /// means "infer": hermes + `*.json` glob → `session_json`, otherwise `jsonl`.
    #[serde(default)]
    pub format: String,
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
    #[serde(default = "default_max_batch_bytes")]
    pub max_batch_bytes: usize,
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
    pub prewarm_on_initialize: bool,
    #[serde(default = "default_false")]
    pub async_log_writes: bool,
    #[serde(default = "default_protocol_version")]
    pub protocol_version: String,
    /// When true, `moraine run mcp` first tries to reach the shared central
    /// MCP server over its Unix socket and proxies to it; if the socket is
    /// absent or unreachable it transparently falls back to an embedded
    /// stdio server. When false, `moraine run mcp` always runs embedded
    /// (pre-central behavior).
    #[serde(default = "default_true")]
    pub use_central_server: bool,
    /// Filesystem path of the central MCP server's Unix domain socket. A
    /// bare filename is resolved relative to the runtime pids dir
    /// (`~/.moraine/run`), so the default lands at `~/.moraine/run/mcp.sock`.
    /// An absolute path is used verbatim.
    #[serde(default = "default_mcp_socket")]
    pub central_socket_path: String,
    /// When true, `moraine up` launches the shared central MCP server as a
    /// background daemon (Service::Mcp in `--serve socket` mode).
    #[serde(default = "default_true")]
    pub start_central_on_up: bool,
    /// Upper bound, in milliseconds, on how long a proxy client waits to
    /// connect to the central socket before giving up and falling back to
    /// an embedded server. Keeps startup fast when the daemon is absent.
    #[serde(default = "default_central_connect_timeout_ms")]
    pub central_connect_timeout_ms: u64,
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
    /// Deprecated (v0.6.0): the up-managed MCP service is always the shared
    /// central server now, controlled by `mcp.start_central_on_up`. This key
    /// is still parsed (existing configs must keep loading under
    /// `deny_unknown_fields`) and acts as a force-on alias, like
    /// `moraine up --mcp`.
    #[serde(default = "default_false")]
    pub start_mcp_on_up: bool,
}

#[derive(Debug, Clone, Deserialize, Default)]
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
            max_batch_bytes: default_max_batch_bytes(),
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
            prewarm_on_initialize: false,
            async_log_writes: true,
            protocol_version: default_protocol_version(),
            use_central_server: true,
            central_socket_path: default_mcp_socket(),
            start_central_on_up: true,
            central_connect_timeout_ms: default_central_connect_timeout_ms(),
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

fn default_ch_url() -> String {
    "http://127.0.0.1:8123".to_string()
}

fn default_ch_database() -> String {
    "moraine".to_string()
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
            harness: "codex".to_string(),
            enabled: true,
            glob: "~/.codex/sessions/**/*.jsonl".to_string(),
            watch_root: "~/.codex/sessions".to_string(),
            format: String::new(),
        },
        IngestSource {
            name: "claude".to_string(),
            harness: "claude-code".to_string(),
            enabled: true,
            glob: "~/.claude/projects/**/*.jsonl".to_string(),
            watch_root: "~/.claude/projects".to_string(),
            format: String::new(),
        },
        IngestSource {
            name: "hermes".to_string(),
            harness: "hermes".to_string(),
            enabled: true,
            glob: "~/.hermes/sessions/session_*.json".to_string(),
            watch_root: "~/.hermes/sessions".to_string(),
            format: String::new(),
        },
        IngestSource {
            name: "kimi-cli".to_string(),
            harness: "kimi-cli".to_string(),
            enabled: true,
            glob: "~/.kimi/sessions/**/wire.jsonl".to_string(),
            watch_root: "~/.kimi/sessions".to_string(),
            format: String::new(),
        },
        IngestSource {
            name: "cursor".to_string(),
            harness: "cursor".to_string(),
            enabled: true,
            glob: "~/.cursor/projects/*/agent-transcripts/**/*.jsonl".to_string(),
            watch_root: "~/.cursor/projects".to_string(),
            format: String::new(),
        },
        IngestSource {
            name: "pi".to_string(),
            harness: "pi-coding-agent".to_string(),
            enabled: true,
            glob: "~/.pi/agent/sessions/**/*.jsonl".to_string(),
            watch_root: "~/.pi/agent/sessions".to_string(),
            format: String::new(),
        },
    ]
}

pub const SOURCE_FORMAT_JSONL: &str = "jsonl";
pub const SOURCE_FORMAT_SESSION_JSON: &str = "session_json";
pub const SOURCE_FORMAT_CURSOR_SQLITE: &str = "cursor_sqlite";

/// SQLite WAL sidecar suffixes that must map back to the canonical database
/// path for watching/debouncing. WAL-mode writes often touch only these files,
/// so dropping them would miss updates entirely (issue #361, decision 5).
const SQLITE_SIDECAR_SUFFIXES: &[&str] = &["-wal", "-shm"];

fn infer_source_format(harness: &str, glob: &str) -> &'static str {
    let glob_lower = glob.to_ascii_lowercase();
    if glob_lower.ends_with(".vscdb") {
        return SOURCE_FORMAT_CURSOR_SQLITE;
    }
    let looks_like_json = !glob_lower.ends_with(".jsonl")
        && (glob_lower.ends_with(".json") || glob_lower.contains(".json"));
    if harness == "hermes" && looks_like_json {
        SOURCE_FORMAT_SESSION_JSON
    } else {
        SOURCE_FORMAT_JSONL
    }
}

fn normalize_source_format(
    format: &str,
    harness: &str,
    glob: &str,
    source_idx: usize,
    source_name: &str,
) -> Result<String> {
    let trimmed = format.trim().to_ascii_lowercase();
    let resolved = if trimmed.is_empty() {
        infer_source_format(harness, glob).to_string()
    } else {
        trimmed
    };
    match resolved.as_str() {
        SOURCE_FORMAT_JSONL | SOURCE_FORMAT_SESSION_JSON | SOURCE_FORMAT_CURSOR_SQLITE => {
            Ok(resolved)
        }
        _ => Err(anyhow::anyhow!(
            "invalid ingest.sources[{source_idx}].format `{}` for source `{}`; expected one of: {SOURCE_FORMAT_JSONL}, {SOURCE_FORMAT_SESSION_JSON}, {SOURCE_FORMAT_CURSOR_SQLITE}",
            format.trim(),
            source_name
        )),
    }
}

impl IngestSource {
    /// Returns the file extension (without leading `.`) this source's format
    /// records are stored in: `jsonl`, `json`, or `vscdb`.
    pub fn tracked_extension(&self) -> &'static str {
        format_tracked_extension(&self.format)
    }
}

fn format_tracked_extension(format: &str) -> &'static str {
    match format {
        SOURCE_FORMAT_SESSION_JSON => "json",
        SOURCE_FORMAT_CURSOR_SQLITE => "vscdb",
        _ => "jsonl",
    }
}

/// Maps a filesystem path seen by enumeration or the watcher to the canonical
/// tracked path for `format`, or `None` when the path is not tracked.
///
/// For file-backed formats this is an extension filter that returns the path
/// unchanged. For SQLite-backed formats the canonical path is the base
/// database file: `state.vscdb` maps to itself, while the `state.vscdb-wal` /
/// `state.vscdb-shm` sidecars map back to `state.vscdb` so WAL-only writes
/// still enqueue (and debounce-coalesce on) the database they belong to.
/// Anything else — including `state.vscdb.backup` — is untracked.
pub fn map_tracked_path(format: &str, path: &str) -> Option<String> {
    let extension = format_tracked_extension(format);
    let has_extension = |candidate: &str| {
        Path::new(candidate)
            .extension()
            .and_then(|s| s.to_str())
            .map(|ext| ext == extension)
            .unwrap_or(false)
    };

    if has_extension(path) {
        return Some(path.to_string());
    }

    if format == SOURCE_FORMAT_CURSOR_SQLITE {
        for suffix in SQLITE_SIDECAR_SUFFIXES {
            if let Some(base) = path.strip_suffix(suffix) {
                if has_extension(base) {
                    return Some(base.to_string());
                }
            }
        }
    }

    None
}

fn default_batch_size() -> usize {
    4000
}

fn default_max_batch_bytes() -> usize {
    8 * 1024 * 1024
}

fn default_flush_interval_seconds() -> f64 {
    0.5
}

fn default_state_dir() -> String {
    "~/.moraine/ingestor".to_string()
}

fn default_max_file_workers() -> usize {
    8
}

fn default_max_inflight_batches() -> usize {
    16
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

fn default_mcp_socket() -> String {
    "mcp.sock".to_string()
}

fn default_central_connect_timeout_ms() -> u64 {
    250
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
    "~/.moraine".to_string()
}

fn default_runtime_logs_dir() -> String {
    "logs".to_string()
}

fn default_runtime_pids_dir() -> String {
    "run".to_string()
}

fn default_service_bin_dir() -> String {
    "~/.local/bin".to_string()
}

fn default_managed_clickhouse_dir() -> String {
    "~/.local/lib/moraine/clickhouse/current".to_string()
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
    fn component_contains_glob(component: Component<'_>) -> bool {
        match component {
            Component::Normal(part) => {
                let value = part.to_string_lossy();
                value.contains('*')
                    || value.contains('?')
                    || value.contains('[')
                    || value.contains(']')
                    || value.contains('{')
                    || value.contains('}')
            }
            _ => false,
        }
    }

    let path = Path::new(glob_pattern);
    let mut root = PathBuf::new();

    for component in path.components() {
        if component_contains_glob(component) {
            return if root.as_os_str().is_empty() {
                ".".to_string()
            } else {
                root.to_string_lossy().to_string()
            };
        }

        root.push(component.as_os_str());
    }

    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .map(|parent| parent.to_string_lossy().to_string())
        .unwrap_or_else(|| ".".to_string())
}

fn home_config_path() -> Option<PathBuf> {
    std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".moraine").join("config.toml"))
}

fn repo_default_config_path() -> PathBuf {
    PathBuf::from("config/moraine.toml")
}

const DEFAULT_CONFIG_ENV_KEYS: &[&str] = &["MORAINE_DEFAULT_CONFIG"];

fn resolve_config_path_with_overrides(
    raw_path: Option<PathBuf>,
    env_keys: &[&str],
    home_path: Option<PathBuf>,
    default_env_keys: &[&str],
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

    for key in default_env_keys {
        if let Ok(value) = std::env::var(key) {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                continue;
            }
            let candidate = PathBuf::from(trimmed);
            if candidate.exists() {
                return candidate;
            }
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
        &["MORAINE_CONFIG"],
        home_config_path(),
        DEFAULT_CONFIG_ENV_KEYS,
        repo_default_config_path(),
    )
}

pub fn resolve_mcp_config_path(raw_path: Option<PathBuf>) -> PathBuf {
    resolve_config_path_with_overrides(
        raw_path,
        &["MORAINE_MCP_CONFIG", "MORAINE_CONFIG"],
        home_config_path(),
        DEFAULT_CONFIG_ENV_KEYS,
        repo_default_config_path(),
    )
}

pub fn resolve_monitor_config_path(raw_path: Option<PathBuf>) -> PathBuf {
    resolve_config_path_with_overrides(
        raw_path,
        &["MORAINE_MONITOR_CONFIG", "MORAINE_CONFIG"],
        home_config_path(),
        DEFAULT_CONFIG_ENV_KEYS,
        repo_default_config_path(),
    )
}

pub fn resolve_ingest_config_path(raw_path: Option<PathBuf>) -> PathBuf {
    resolve_config_path_with_overrides(
        raw_path,
        &["MORAINE_INGEST_CONFIG", "MORAINE_CONFIG"],
        home_config_path(),
        DEFAULT_CONFIG_ENV_KEYS,
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

fn normalize_harness(harness: &str, source_idx: usize, source_name: &str) -> Result<String> {
    let normalized = harness.trim().to_ascii_lowercase();
    if KNOWN_INGEST_HARNESSES.contains(&normalized.as_str()) {
        return Ok(normalized);
    }

    Err(anyhow::anyhow!(
        "invalid ingest.sources[{source_idx}].harness `{}` for source `{}`; expected one of: {}",
        harness.trim(),
        source_name,
        KNOWN_INGEST_HARNESSES.join(", ")
    ))
}

fn normalize_config(mut cfg: AppConfig) -> Result<AppConfig> {
    for (source_idx, source) in cfg.ingest.sources.iter_mut().enumerate() {
        source.harness = normalize_harness(&source.harness, source_idx, &source.name)?;
        source.glob = expand_path(&source.glob);
        source.watch_root = if source.watch_root.trim().is_empty() {
            watch_root_from_glob(&source.glob)
        } else {
            expand_path(&source.watch_root)
        };
        source.format = normalize_source_format(
            &source.format,
            &source.harness,
            &source.glob,
            source_idx,
            &source.name,
        )?;
    }

    cfg.ingest.state_dir = expand_path(&cfg.ingest.state_dir);
    cfg.runtime.root_dir = expand_path(&cfg.runtime.root_dir);
    cfg.runtime.logs_dir = resolve_runtime_subdir(&cfg.runtime.root_dir, &cfg.runtime.logs_dir);
    cfg.runtime.pids_dir = resolve_runtime_subdir(&cfg.runtime.root_dir, &cfg.runtime.pids_dir);
    cfg.runtime.service_bin_dir = expand_path(&cfg.runtime.service_bin_dir);
    cfg.runtime.managed_clickhouse_dir = expand_path(&cfg.runtime.managed_clickhouse_dir);

    // Resolve the central MCP socket path against the (already-normalized)
    // pids dir so a bare filename lands at `~/.moraine/run/mcp.sock` while an
    // absolute path is preserved. The daemon (`moraine up`) and proxy clients
    // (`moraine run mcp`) both read this resolved value, so they agree on the
    // socket as long as they load the same config.
    cfg.mcp.central_socket_path =
        resolve_runtime_subdir(&cfg.runtime.pids_dir, &cfg.mcp.central_socket_path);

    Ok(cfg)
}

pub fn load_config(path: impl AsRef<Path>) -> Result<AppConfig> {
    let content = std::fs::read_to_string(path.as_ref())
        .with_context(|| format!("failed to read config {}", path.as_ref().display()))?;
    let cfg: AppConfig = toml::from_str(&content).context("failed to parse TOML config")?;
    normalize_config(cfg)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_temp_config(contents: &str, label: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "moraine-config-{label}-{}-{}.toml",
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
            &["MORAINE_CONFIG"],
            Some(PathBuf::from("/tmp/home.toml")),
            &[],
            PathBuf::from("/tmp/repo.toml"),
        );
        assert_eq!(chosen, PathBuf::from("/tmp/cli.toml"));
    }

    #[test]
    fn watch_root_extracts_prefix() {
        assert_eq!(watch_root_from_glob("/tmp/a/**/*.jsonl"), "/tmp/a");
        assert_eq!(watch_root_from_glob("/tmp/a/*.jsonl"), "/tmp/a");
        assert_eq!(watch_root_from_glob("logs/*.jsonl"), "logs");
        assert_eq!(watch_root_from_glob("logs/session-*.jsonl"), "logs");
        assert_eq!(watch_root_from_glob("*.jsonl"), ".");
        assert_eq!(watch_root_from_glob("*/*.jsonl"), ".");
        assert_eq!(watch_root_from_glob("/**/*.jsonl"), "/");
    }

    #[test]
    fn runtime_subdir_joins_relative_paths() {
        let root = "/tmp/moraine";
        assert_eq!(
            resolve_runtime_subdir(root, "logs"),
            "/tmp/moraine/logs".to_string()
        );
        assert_eq!(
            resolve_runtime_subdir(root, "/var/tmp/moraine"),
            "/var/tmp/moraine".to_string()
        );
    }

    #[test]
    fn resolve_order_prefers_env_over_home_and_repo() {
        let env_key = "MORAINE_CONFIG_TEST_KEY";
        std::env::set_var(env_key, "/tmp/from-env.toml");

        let chosen = resolve_config_path_with_overrides(
            None,
            &[env_key],
            Some(PathBuf::from("/tmp/from-home.toml")),
            &[],
            PathBuf::from("/tmp/from-repo.toml"),
        );

        std::env::remove_var(env_key);
        assert_eq!(chosen, PathBuf::from("/tmp/from-env.toml"));
    }

    #[test]
    fn resolve_order_uses_repo_when_home_missing() {
        let repo_default = std::env::temp_dir().join("moraine-config-repo-default.toml");
        std::fs::write(&repo_default, "x=1").expect("write temp repo default");

        let chosen = resolve_config_path_with_overrides(
            None,
            &["MORAINE_CONFIG_TEST_DOES_NOT_EXIST"],
            Some(PathBuf::from("/tmp/definitely-missing-home.toml")),
            &[],
            repo_default.clone(),
        );

        std::fs::remove_file(&repo_default).ok();
        assert_eq!(chosen, repo_default);
    }

    #[test]
    fn default_env_used_when_home_missing_and_path_exists() {
        let default_path = std::env::temp_dir().join(format!(
            "moraine-default-config-{}-{}.toml",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        std::fs::write(&default_path, "x=1").expect("write default");
        let env_key = "MORAINE_DEFAULT_CONFIG_TEST_EXISTS";
        std::env::set_var(env_key, default_path.to_string_lossy().to_string());

        let chosen = resolve_config_path_with_overrides(
            None,
            &["MORAINE_CONFIG_TEST_DOES_NOT_EXIST"],
            Some(PathBuf::from("/tmp/definitely-missing-home.toml")),
            &[env_key],
            PathBuf::from("/tmp/definitely-missing-repo-default.toml"),
        );

        std::env::remove_var(env_key);
        std::fs::remove_file(&default_path).ok();
        assert_eq!(chosen, default_path);
    }

    #[test]
    fn default_env_skipped_when_path_missing() {
        let repo_default = std::env::temp_dir().join(format!(
            "moraine-default-repo-{}-{}.toml",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        std::fs::write(&repo_default, "x=1").expect("write repo default");
        let env_key = "MORAINE_DEFAULT_CONFIG_TEST_MISSING";
        std::env::set_var(env_key, "/tmp/definitely-missing-default.toml");

        let chosen = resolve_config_path_with_overrides(
            None,
            &["MORAINE_CONFIG_TEST_DOES_NOT_EXIST"],
            Some(PathBuf::from("/tmp/definitely-missing-home.toml")),
            &[env_key],
            repo_default.clone(),
        );

        std::env::remove_var(env_key);
        std::fs::remove_file(&repo_default).ok();
        assert_eq!(chosen, repo_default);
    }

    #[test]
    fn default_env_does_not_override_home_when_home_exists() {
        let home_path = std::env::temp_dir().join(format!(
            "moraine-default-home-{}-{}.toml",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        let default_path = std::env::temp_dir().join(format!(
            "moraine-default-lower-{}-{}.toml",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        std::fs::write(&home_path, "x=1").expect("write home");
        std::fs::write(&default_path, "x=1").expect("write default");
        let env_key = "MORAINE_DEFAULT_CONFIG_TEST_NOT_HIGHER";
        std::env::set_var(env_key, default_path.to_string_lossy().to_string());

        let chosen = resolve_config_path_with_overrides(
            None,
            &["MORAINE_CONFIG_TEST_DOES_NOT_EXIST"],
            Some(home_path.clone()),
            &[env_key],
            PathBuf::from("/tmp/definitely-missing-repo-default.toml"),
        );

        std::env::remove_var(env_key);
        std::fs::remove_file(&home_path).ok();
        std::fs::remove_file(&default_path).ok();
        assert_eq!(chosen, home_path);
    }

    #[test]
    fn mcp_config_env_has_priority_over_generic_env() {
        std::env::set_var("MORAINE_MCP_CONFIG", "/tmp/mcp.toml");
        std::env::set_var("MORAINE_CONFIG", "/tmp/generic.toml");

        let chosen = resolve_mcp_config_path(None);

        std::env::remove_var("MORAINE_MCP_CONFIG");
        std::env::remove_var("MORAINE_CONFIG");
        assert_eq!(chosen, PathBuf::from("/tmp/mcp.toml"));
    }

    #[test]
    fn load_config_errors_when_path_missing() {
        let path = std::env::temp_dir().join("moraine-missing-config-does-not-exist.toml");
        let err = load_config(&path).expect_err("missing config path should fail");
        assert!(
            err.to_string().contains("failed to read config"),
            "unexpected error: {err:#}"
        );
    }

    #[test]
    fn load_config_accepts_minimal_comment_only_file() {
        let path = write_temp_config(
            r#"
# Moraine default config.
# Values omitted here are filled by built-in defaults.
"#,
            "minimal-comment-only",
        );
        let cfg = load_config(&path).expect("minimal config should load with defaults");
        std::fs::remove_file(&path).ok();
        assert_eq!(cfg.clickhouse.url, "http://127.0.0.1:8123");
        assert!(!cfg.mcp.prewarm_on_initialize);
        assert!(!cfg.ingest.sources.is_empty());
    }

    #[test]
    fn load_config_accepts_mcp_prewarm_toggle() {
        let path = write_temp_config(
            r#"
[mcp]
prewarm_on_initialize = true
"#,
            "mcp-prewarm-toggle",
        );
        let cfg = load_config(&path).expect("mcp prewarm toggle should load");
        std::fs::remove_file(&path).ok();
        assert!(cfg.mcp.prewarm_on_initialize);
    }

    #[test]
    fn central_mcp_defaults_are_on() {
        let cfg = McpConfig::default();
        assert!(cfg.use_central_server);
        assert!(cfg.start_central_on_up);
        assert_eq!(cfg.central_connect_timeout_ms, 250);
        assert_eq!(cfg.central_socket_path, "mcp.sock");
    }

    #[test]
    fn central_socket_path_resolves_relative_to_pids_dir() {
        let path = write_temp_config(
            r#"
[runtime]
root_dir = "/tmp/moraine-central-test"
"#,
            "central-socket-relative",
        );
        let cfg = load_config(&path).expect("config should load");
        std::fs::remove_file(&path).ok();
        // Bare "mcp.sock" default resolves under <root>/run.
        assert_eq!(
            cfg.mcp.central_socket_path,
            "/tmp/moraine-central-test/run/mcp.sock"
        );
    }

    #[test]
    fn central_socket_path_absolute_is_preserved() {
        let path = write_temp_config(
            r#"
[runtime]
root_dir = "/tmp/moraine-central-test"

[mcp]
central_socket_path = "/var/run/moraine/custom.sock"
"#,
            "central-socket-absolute",
        );
        let cfg = load_config(&path).expect("config should load");
        std::fs::remove_file(&path).ok();
        assert_eq!(cfg.mcp.central_socket_path, "/var/run/moraine/custom.sock");
    }

    #[test]
    fn central_mcp_toggles_are_optional_in_toml() {
        // A config that predates the central-server feature (no central keys)
        // must still parse, picking up the new defaults.
        let path = write_temp_config(
            r#"
[mcp]
max_results = 25
"#,
            "central-omitted-keys",
        );
        let cfg = load_config(&path).expect("legacy mcp config should load");
        std::fs::remove_file(&path).ok();
        assert_eq!(cfg.mcp.max_results, 25);
        assert!(cfg.mcp.use_central_server);
        assert!(cfg.mcp.start_central_on_up);
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
harness = "codex"
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

    #[test]
    fn load_config_errors_on_unknown_ingest_harness() {
        let path = write_temp_config(
            r#"
[[ingest.sources]]
name = "custom"
harness = "openai"
enabled = true
glob = "~/.custom/sessions/**/*.jsonl"
watch_root = "~/.custom/sessions"
"#,
            "unknown-harness",
        );
        let err = load_config(&path).expect_err("unknown ingest harness should fail");
        std::fs::remove_file(&path).ok();
        assert!(
            format!("{err:#}").contains(
                "expected one of: codex, claude-code, cursor, hermes, kimi-cli, pi-coding-agent"
            ),
            "unexpected error: {err:#}"
        );
    }

    #[test]
    fn load_config_rejects_legacy_claude_harness_value() {
        let path = write_temp_config(
            r#"
[[ingest.sources]]
name = "claude"
harness = "claude"
enabled = true
glob = "~/.claude/projects/**/*.jsonl"
watch_root = "~/.claude/projects"
"#,
            "legacy-claude-harness",
        );
        let err = load_config(&path).expect_err("legacy `claude` harness value should fail");
        std::fs::remove_file(&path).ok();
        assert!(
            format!("{err:#}").contains(
                "expected one of: codex, claude-code, cursor, hermes, kimi-cli, pi-coding-agent"
            ),
            "unexpected error: {err:#}"
        );
    }

    #[test]
    fn load_config_accepts_claude_code_harness_value() {
        let path = write_temp_config(
            r#"
[[ingest.sources]]
name = "claude"
harness = "claude-code"
enabled = true
glob = "~/.claude/projects/**/*.jsonl"
watch_root = "~/.claude/projects"
"#,
            "claude-code-harness",
        );
        let cfg = load_config(&path).expect("claude-code harness should be accepted");
        std::fs::remove_file(&path).ok();
        let source = cfg
            .ingest
            .sources
            .iter()
            .find(|s| s.name == "claude")
            .expect("claude source should be present");
        assert_eq!(source.harness, "claude-code");
    }

    #[test]
    fn load_config_accepts_hermes_harness_value() {
        let path = write_temp_config(
            r#"
[[ingest.sources]]
name = "hermes"
harness = "hermes"
enabled = true
glob = "~/trajectories/**/*.jsonl"
watch_root = "~/trajectories"
"#,
            "hermes-harness",
        );
        let cfg = load_config(&path).expect("hermes harness should be accepted");
        std::fs::remove_file(&path).ok();
        let source = cfg
            .ingest
            .sources
            .iter()
            .find(|source| source.harness == "hermes")
            .expect("hermes source");
        assert_eq!(source.name, "hermes");
        assert!(source.glob.ends_with("/trajectories/**/*.jsonl"));
        assert!(source.watch_root.ends_with("/trajectories"));
    }

    #[test]
    fn load_config_accepts_kimi_cli_harness_value() {
        let path = write_temp_config(
            r#"
[[ingest.sources]]
name = "kimi-cli"
harness = "kimi-cli"
enabled = true
glob = "~/.kimi/sessions/**/wire.jsonl"
watch_root = "~/.kimi/sessions"
"#,
            "kimi-cli-harness",
        );
        let cfg = load_config(&path).expect("kimi-cli harness should be accepted");
        std::fs::remove_file(&path).ok();
        let source = cfg
            .ingest
            .sources
            .iter()
            .find(|source| source.harness == "kimi-cli")
            .expect("kimi-cli source");
        assert_eq!(source.format, SOURCE_FORMAT_JSONL);
        assert_eq!(source.tracked_extension(), "jsonl");
    }

    #[test]
    fn load_config_accepts_cursor_harness_value() {
        let path = write_temp_config(
            r#"
[[ingest.sources]]
name = "cursor"
harness = "cursor"
enabled = true
glob = "~/.cursor/projects/*/agent-transcripts/**/*.jsonl"
watch_root = "~/.cursor/projects"
"#,
            "cursor-harness",
        );
        let cfg = load_config(&path).expect("cursor harness should be accepted");
        std::fs::remove_file(&path).ok();
        let source = cfg
            .ingest
            .sources
            .iter()
            .find(|source| source.harness == "cursor")
            .expect("cursor source");
        assert_eq!(source.format, SOURCE_FORMAT_JSONL);
        assert_eq!(source.tracked_extension(), "jsonl");
    }

    #[test]
    fn load_config_accepts_cursor_sqlite_format() {
        let path = write_temp_config(
            r#"
[[ingest.sources]]
name = "cursor-sqlite"
harness = "cursor"
enabled = false
glob = "~/Library/Application Support/Cursor/User/**/state.vscdb"
watch_root = "~/Library/Application Support/Cursor/User"
format = "cursor_sqlite"
"#,
            "cursor-sqlite-format",
        );
        let cfg = load_config(&path).expect("cursor_sqlite format should be accepted");
        std::fs::remove_file(&path).ok();
        let source = cfg
            .ingest
            .sources
            .iter()
            .find(|source| source.name == "cursor-sqlite")
            .expect("cursor-sqlite source");
        assert_eq!(source.format, SOURCE_FORMAT_CURSOR_SQLITE);
        assert_eq!(source.tracked_extension(), "vscdb");
        assert!(!source.enabled);
    }

    #[test]
    fn load_config_rejects_unknown_format_value() {
        let path = write_temp_config(
            r#"
[[ingest.sources]]
name = "cursor-sqlite"
harness = "cursor"
enabled = true
glob = "~/Library/Application Support/Cursor/User/**/state.vscdb"
watch_root = "~/Library/Application Support/Cursor/User"
format = "sqlite"
"#,
            "unknown-format",
        );
        let err = load_config(&path).expect_err("unknown format should fail");
        std::fs::remove_file(&path).ok();
        assert!(
            format!("{err:#}").contains("expected one of: jsonl, session_json, cursor_sqlite"),
            "unexpected error: {err:#}"
        );
    }

    #[test]
    fn map_tracked_path_filters_by_extension_for_file_formats() {
        assert_eq!(
            map_tracked_path(SOURCE_FORMAT_JSONL, "/tmp/a.jsonl"),
            Some("/tmp/a.jsonl".to_string())
        );
        assert_eq!(map_tracked_path(SOURCE_FORMAT_JSONL, "/tmp/a.json"), None);
        assert_eq!(
            map_tracked_path(SOURCE_FORMAT_SESSION_JSON, "/tmp/session_a.json"),
            Some("/tmp/session_a.json".to_string())
        );
        assert_eq!(
            map_tracked_path(SOURCE_FORMAT_SESSION_JSON, "/tmp/a.jsonl"),
            None
        );
    }

    #[test]
    fn map_tracked_path_maps_sqlite_sidecars_to_canonical_db() {
        let base = "/tmp/User/globalStorage/state.vscdb";
        assert_eq!(
            map_tracked_path(SOURCE_FORMAT_CURSOR_SQLITE, base),
            Some(base.to_string())
        );
        assert_eq!(
            map_tracked_path(SOURCE_FORMAT_CURSOR_SQLITE, "/tmp/User/state.vscdb-wal"),
            Some("/tmp/User/state.vscdb".to_string())
        );
        assert_eq!(
            map_tracked_path(SOURCE_FORMAT_CURSOR_SQLITE, "/tmp/User/state.vscdb-shm"),
            Some("/tmp/User/state.vscdb".to_string())
        );
        // Backups and unrelated files must stay untracked.
        assert_eq!(
            map_tracked_path(SOURCE_FORMAT_CURSOR_SQLITE, "/tmp/User/state.vscdb.backup"),
            None
        );
        assert_eq!(
            map_tracked_path(SOURCE_FORMAT_CURSOR_SQLITE, "/tmp/User/state.db-wal"),
            None
        );
        assert_eq!(
            map_tracked_path(SOURCE_FORMAT_CURSOR_SQLITE, "/tmp/User/notes.jsonl"),
            None
        );
    }

    #[test]
    fn load_config_accepts_pi_coding_agent_harness_value() {
        let path = write_temp_config(
            r#"
[[ingest.sources]]
name = "pi"
harness = "pi-coding-agent"
enabled = true
glob = "~/.pi/agent/sessions/**/*.jsonl"
watch_root = "~/.pi/agent/sessions"
"#,
            "pi-coding-agent-harness",
        );
        let cfg = load_config(&path).expect("pi-coding-agent harness should be accepted");
        std::fs::remove_file(&path).ok();
        let source = cfg
            .ingest
            .sources
            .iter()
            .find(|source| source.harness == "pi-coding-agent")
            .expect("pi source");
        assert_eq!(source.format, SOURCE_FORMAT_JSONL);
        assert_eq!(source.tracked_extension(), "jsonl");
    }
}
