use anyhow::{Context, Result};
use serde::{de::DeserializeOwned, Deserialize, Deserializer, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fmt;
use std::net::IpAddr;
#[cfg(unix)]
use std::os::unix::ffi::OsStrExt;
#[cfg(windows)]
use std::os::windows::ffi::OsStrExt;
use std::path::{Component, Path, PathBuf};

pub const KNOWN_INGEST_HARNESSES: &[&str] = &[
    "codex",
    "claude-code",
    "cursor",
    "hermes",
    "kiro-cli",
    "kimi-cli",
    "nac",
    "opencode",
    "pi-coding-agent",
    "qwen-code",
];

#[derive(Debug, Clone, Copy, Default, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SourceFormat {
    #[default]
    #[serde(rename = "")]
    Infer,
    Jsonl,
    SessionJson,
    KiroSession,
    CursorSqlite,
    NacSqlite,
    #[serde(rename = "opencode_sqlite")]
    OpenCodeSqlite,
}

impl SourceFormat {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Infer => "",
            Self::Jsonl => SOURCE_FORMAT_JSONL,
            Self::SessionJson => SOURCE_FORMAT_SESSION_JSON,
            Self::KiroSession => SOURCE_FORMAT_KIRO_SESSION,
            Self::CursorSqlite => SOURCE_FORMAT_CURSOR_SQLITE,
            Self::NacSqlite => SOURCE_FORMAT_NAC_SQLITE,
            Self::OpenCodeSqlite => SOURCE_FORMAT_OPENCODE_SQLITE,
        }
    }

    pub const fn uses_recursive_watch(self) -> bool {
        match self {
            Self::Infer => panic!("source format must be normalized before selecting watch scope"),
            Self::Jsonl
            | Self::SessionJson
            | Self::KiroSession
            | Self::CursorSqlite
            | Self::OpenCodeSqlite => true,
            Self::NacSqlite => false,
        }
    }

    fn parse(value: &str) -> Option<Self> {
        let value = value.trim();
        if value.is_empty() {
            Some(Self::Infer)
        } else if value.eq_ignore_ascii_case(SOURCE_FORMAT_JSONL) {
            Some(Self::Jsonl)
        } else if value.eq_ignore_ascii_case(SOURCE_FORMAT_SESSION_JSON) {
            Some(Self::SessionJson)
        } else if value.eq_ignore_ascii_case(SOURCE_FORMAT_KIRO_SESSION) {
            Some(Self::KiroSession)
        } else if value.eq_ignore_ascii_case(SOURCE_FORMAT_CURSOR_SQLITE) {
            Some(Self::CursorSqlite)
        } else if value.eq_ignore_ascii_case(SOURCE_FORMAT_NAC_SQLITE) {
            Some(Self::NacSqlite)
        } else if value.eq_ignore_ascii_case(SOURCE_FORMAT_OPENCODE_SQLITE) {
            Some(Self::OpenCodeSqlite)
        } else {
            None
        }
    }
}

impl<'de> Deserialize<'de> for SourceFormat {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse(&value).ok_or_else(|| {
            serde::de::Error::unknown_variant(
                value.trim(),
                &[
                    "",
                    SOURCE_FORMAT_JSONL,
                    SOURCE_FORMAT_SESSION_JSON,
                    SOURCE_FORMAT_KIRO_SESSION,
                    SOURCE_FORMAT_CURSOR_SQLITE,
                    SOURCE_FORMAT_NAC_SQLITE,
                    SOURCE_FORMAT_OPENCODE_SQLITE,
                ],
            )
        })
    }
}
impl AsRef<str> for SourceFormat {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for SourceFormat {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

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
    /// On-disk trace format. Empty means infer from harness and glob.
    #[serde(default)]
    pub format: SourceFormat,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ClickHouseRequestCompression {
    #[default]
    None,
    Gzip,
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
    /// Compression applied to non-empty ClickHouse HTTP request bodies.
    #[serde(default)]
    pub request_compression: ClickHouseRequestCompression,
    #[serde(default = "default_true")]
    pub async_insert: bool,
    #[serde(default = "default_true")]
    pub wait_for_async_insert: bool,
    /// Permit connecting to a backend whose schema ledger holds migration
    /// versions newer than this build's bundled set. Only consulted for
    /// non-default backends (the default backend is migrated by moraine
    /// itself); false means unknown server-side versions are a hard error.
    #[serde(default = "default_false")]
    pub allow_newer_server: bool,
}

/// Reserved name of the backend that the `[clickhouse]` block aliases. The
/// default backend is the always-on local sink and the only backend moraine
/// migrates itself.
pub const DEFAULT_BACKEND_NAME: &str = "default";

/// The only routing mode implemented in v1: routed sessions are mirrored
/// (tee'd) to the named backend in addition to the default backend.
pub const ROUTE_MODE_MIRROR: &str = "mirror";

/// Routes sessions whose working directory matches `dir` to a named backend.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RouteConfig {
    /// Directory glob matched against a session's absolute working
    /// directory; `~` is expanded during config normalization. A glob ending
    /// in `/**` matches the base directory itself as well as everything
    /// beneath it.
    pub dir: String,
    /// Name of a `[backends.<name>]` entry; unknown names are a load error
    /// (the home config is user-owned — typos should fail loudly).
    pub backend: String,
    /// Routing mode; `"mirror"` is the only accepted value in v1.
    #[serde(default = "default_route_mode")]
    pub mode: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RedactionConfig {
    #[serde(default)]
    pub dangerously_skip_secret_redaction: bool,
    #[serde(default = "default_redaction_ruleset")]
    pub ruleset: String,
    #[serde(default)]
    pub extra_patterns: Vec<String>,
    #[serde(skip)]
    pub dangerously_skip_secret_redaction_ignored: bool,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IdentityConfig {
    #[serde(default)]
    pub author: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IngestConfig {
    /// Directory globs matched against each session's first non-empty working
    /// directory. Matching sessions are omitted from ingestion.
    #[serde(default)]
    pub exclude_project_dirs: Vec<String>,
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
    /// Emit content-free ingest acknowledgement observations for benchmark
    /// correlation. Disabled by default; observations contain only a batch
    /// sequence, SHA-256 event-identity digests, and a monotonic timestamp.
    #[serde(default)]
    pub ack_observation: bool,
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
    #[serde(default = "default_true")]
    pub async_log_writes: bool,
    #[serde(default = "default_protocol_version")]
    pub protocol_version: String,
    /// Maximum retrieval requests executed concurrently by each MCP server
    /// process. When omitted, the server executes up to eight. At most sixteen
    /// additional requests wait in FIFO order until capacity becomes available
    /// or the request is cancelled.
    #[serde(default)]
    pub max_parallel_requests: Option<u16>,
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
    /// Deprecated compatibility key. Its value is validated but no longer
    /// gates the unified backend started by `moraine up`.
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

/// Ceiling on any configured query deadline. A deadline above one day is
/// indistinguishable from "unlimited" for an interactive local service.
pub const QUERY_DEADLINE_MAX_SECONDS: f64 = 86_400.0;

/// Server-side per-query memory backstop, matching the `max_memory_usage`
/// profile shipped to managed ClickHouse installs (`config/users.xml`). No
/// class budget may exceed it: the envelope's per-statement ceiling must stay
/// enforceable even when the server profile is the only remaining bound.
pub const QUERY_MEMORY_BACKSTOP_BYTES: u64 = 4 * BYTES_PER_GIB;

const BYTES_PER_MIB: u64 = 1024 * 1024;
const BYTES_PER_GIB: u64 = 1024 * BYTES_PER_MIB;

/// One `[query_budgets.<class>]` table: the finite execution budget every
/// ClickHouse statement in that class runs under. Zero is rejected at load
/// time for every field (ClickHouse treats zero limits as "unlimited"), so
/// an unbounded budget is unrepresentable in configuration.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct QueryBudgetClassConfig {
    /// Absolute server execution deadline, in seconds, for the whole logical
    /// operation (`max_execution_time` derives from its remaining portion).
    pub deadline_seconds: f64,
    /// Per-statement `max_memory_usage` ceiling in bytes.
    pub memory_bytes: u64,
    /// Sort/group-by spill threshold in bytes
    /// (`max_bytes_before_external_group_by` / `_sort`).
    pub spill_bytes: u64,
    /// Cumulative request-level row-read allowance (`max_rows_to_read`).
    pub read_rows: u64,
    /// Cumulative request-level byte-read allowance (`max_bytes_to_read`).
    pub read_bytes: u64,
    /// Fixed maximum number of statements per logical operation.
    pub statement_cap: u32,
}

/// Per-class query budgets for the mandatory query envelope (issue #600).
/// Absent classes and fields fall back to the documented defaults; partial
/// tables override only the fields they name.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct QueryBudgetsConfig {
    /// MCP tools, monitor reads, CLI status/doctor and other user-facing reads.
    pub interactive: QueryBudgetClassConfig,
    /// Ingest publication inspections, projection refreshes, janitor work.
    pub background: QueryBudgetClassConfig,
    /// Schema migrations and read-model backfill machinery.
    pub migration: QueryBudgetClassConfig,
    /// KILL statements and telemetry one-shots only.
    pub administrative: QueryBudgetClassConfig,
    /// `moraine export` streaming reads (explicit generous budget; amendment
    /// A9 forbids code-side unlimited escapes, so export is its own entry).
    pub export: QueryBudgetClassConfig,
}

const DEFAULT_INTERACTIVE_QUERY_BUDGET: QueryBudgetClassConfig = QueryBudgetClassConfig {
    deadline_seconds: 15.0,
    memory_bytes: BYTES_PER_GIB,
    spill_bytes: 256 * BYTES_PER_MIB,
    read_rows: 500_000_000,
    read_bytes: 10 * BYTES_PER_GIB,
    statement_cap: 256,
};

const DEFAULT_BACKGROUND_QUERY_BUDGET: QueryBudgetClassConfig = QueryBudgetClassConfig {
    // 600s matches the projection debounce ceiling so the worst-case
    // legitimate projection refresh fits inside one budget (amendment A7);
    // allowances are sized for publication-actor full-table inspections.
    deadline_seconds: 600.0,
    memory_bytes: 2 * BYTES_PER_GIB,
    spill_bytes: 512 * BYTES_PER_MIB,
    read_rows: 5_000_000_000,
    read_bytes: 100 * BYTES_PER_GIB,
    statement_cap: 512,
};

const DEFAULT_MIGRATION_QUERY_BUDGET: QueryBudgetClassConfig = QueryBudgetClassConfig {
    deadline_seconds: 600.0,
    memory_bytes: 4 * BYTES_PER_GIB,
    spill_bytes: BYTES_PER_GIB,
    read_rows: 10_000_000_000,
    read_bytes: 200 * BYTES_PER_GIB,
    statement_cap: 1024,
};

const DEFAULT_ADMINISTRATIVE_QUERY_BUDGET: QueryBudgetClassConfig = QueryBudgetClassConfig {
    deadline_seconds: 5.0,
    memory_bytes: 256 * BYTES_PER_MIB,
    spill_bytes: 64 * BYTES_PER_MIB,
    read_rows: 1_000_000,
    read_bytes: 256 * BYTES_PER_MIB,
    statement_cap: 4,
};

const DEFAULT_EXPORT_QUERY_BUDGET: QueryBudgetClassConfig = QueryBudgetClassConfig {
    // Matches the pre-envelope export ceiling (600s max_execution_time).
    deadline_seconds: 600.0,
    memory_bytes: 2 * BYTES_PER_GIB,
    spill_bytes: 512 * BYTES_PER_MIB,
    read_rows: 10_000_000_000,
    read_bytes: 200 * BYTES_PER_GIB,
    statement_cap: 64,
};

impl Default for QueryBudgetsConfig {
    fn default() -> Self {
        Self {
            interactive: DEFAULT_INTERACTIVE_QUERY_BUDGET,
            background: DEFAULT_BACKGROUND_QUERY_BUDGET,
            migration: DEFAULT_MIGRATION_QUERY_BUDGET,
            administrative: DEFAULT_ADMINISTRATIVE_QUERY_BUDGET,
            export: DEFAULT_EXPORT_QUERY_BUDGET,
        }
    }
}

/// Serde defaults are per-struct, but each class has its own default budget,
/// so deserialization goes through per-field optionals merged over the class
/// defaults. This keeps partial tables ergonomic (override one field, keep
/// the rest) without ever representing an absent budget.
#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct PartialQueryBudgetClass {
    deadline_seconds: Option<f64>,
    memory_bytes: Option<u64>,
    spill_bytes: Option<u64>,
    read_rows: Option<u64>,
    read_bytes: Option<u64>,
    statement_cap: Option<u32>,
}

impl PartialQueryBudgetClass {
    fn merged(self, defaults: QueryBudgetClassConfig) -> QueryBudgetClassConfig {
        QueryBudgetClassConfig {
            deadline_seconds: self.deadline_seconds.unwrap_or(defaults.deadline_seconds),
            memory_bytes: self.memory_bytes.unwrap_or(defaults.memory_bytes),
            spill_bytes: self.spill_bytes.unwrap_or(defaults.spill_bytes),
            read_rows: self.read_rows.unwrap_or(defaults.read_rows),
            read_bytes: self.read_bytes.unwrap_or(defaults.read_bytes),
            statement_cap: self.statement_cap.unwrap_or(defaults.statement_cap),
        }
    }
}

impl<'de> Deserialize<'de> for QueryBudgetsConfig {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Default, Deserialize)]
        #[serde(default, deny_unknown_fields)]
        struct PartialQueryBudgets {
            interactive: PartialQueryBudgetClass,
            background: PartialQueryBudgetClass,
            migration: PartialQueryBudgetClass,
            administrative: PartialQueryBudgetClass,
            export: PartialQueryBudgetClass,
        }

        let partial = PartialQueryBudgets::deserialize(deserializer)?;
        Ok(Self {
            interactive: partial.interactive.merged(DEFAULT_INTERACTIVE_QUERY_BUDGET),
            background: partial.background.merged(DEFAULT_BACKGROUND_QUERY_BUDGET),
            migration: partial.migration.merged(DEFAULT_MIGRATION_QUERY_BUDGET),
            administrative: partial
                .administrative
                .merged(DEFAULT_ADMINISTRATIVE_QUERY_BUDGET),
            export: partial.export.merged(DEFAULT_EXPORT_QUERY_BUDGET),
        })
    }
}

/// Typed rejection from [`ValidatedQueryBudgets`] construction. Every
/// variant names the offending `query_budgets.<class>.<field>`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum QueryBudgetsValidationError {
    DeadlineNotFinite {
        class: &'static str,
    },
    DeadlineNotPositive {
        class: &'static str,
        value: f64,
    },
    DeadlineAboveMax {
        class: &'static str,
        value: f64,
    },
    MemoryZero {
        class: &'static str,
    },
    MemoryAboveBackstop {
        class: &'static str,
        value: u64,
        backstop: u64,
    },
    SpillZero {
        class: &'static str,
    },
    ReadRowsZero {
        class: &'static str,
    },
    ReadBytesZero {
        class: &'static str,
    },
    StatementCapZero {
        class: &'static str,
    },
}

impl fmt::Display for QueryBudgetsValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DeadlineNotFinite { class } => write!(
                formatter,
                "query_budgets.{class}.deadline_seconds must be a finite number of seconds"
            ),
            Self::DeadlineNotPositive { class, value } => write!(
                formatter,
                "query_budgets.{class}.deadline_seconds must be greater than zero (got {value})"
            ),
            Self::DeadlineAboveMax { class, value } => write!(
                formatter,
                "query_budgets.{class}.deadline_seconds must be at most {QUERY_DEADLINE_MAX_SECONDS} (24h); got {value}"
            ),
            Self::MemoryZero { class } => write!(
                formatter,
                "query_budgets.{class}.memory_bytes must be at least 1; zero would disable the per-query memory ceiling"
            ),
            Self::MemoryAboveBackstop {
                class,
                value,
                backstop,
            } => write!(
                formatter,
                "query_budgets.{class}.memory_bytes ({value}) exceeds the per-query server memory backstop ({backstop} bytes)"
            ),
            Self::SpillZero { class } => write!(
                formatter,
                "query_budgets.{class}.spill_bytes must be at least 1; zero would disable sort/group-by spill"
            ),
            Self::ReadRowsZero { class } => write!(
                formatter,
                "query_budgets.{class}.read_rows must be at least 1"
            ),
            Self::ReadBytesZero { class } => write!(
                formatter,
                "query_budgets.{class}.read_bytes must be at least 1"
            ),
            Self::StatementCapZero { class } => write!(
                formatter,
                "query_budgets.{class}.statement_cap must be at least 1"
            ),
        }
    }
}

impl std::error::Error for QueryBudgetsValidationError {}

/// A query budget that passed fail-closed validation. Fields are private so
/// an invalid budget (zero, non-finite, above the backstop) is
/// unrepresentable once constructed.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ValidatedQueryBudget {
    deadline_seconds: f64,
    memory_bytes: u64,
    spill_bytes: u64,
    read_rows: u64,
    read_bytes: u64,
    statement_cap: u32,
}

impl ValidatedQueryBudget {
    fn from_class_config(
        class: &'static str,
        cfg: &QueryBudgetClassConfig,
        memory_backstop_bytes: u64,
    ) -> std::result::Result<Self, QueryBudgetsValidationError> {
        if !cfg.deadline_seconds.is_finite() {
            return Err(QueryBudgetsValidationError::DeadlineNotFinite { class });
        }
        if cfg.deadline_seconds <= 0.0 {
            return Err(QueryBudgetsValidationError::DeadlineNotPositive {
                class,
                value: cfg.deadline_seconds,
            });
        }
        if cfg.deadline_seconds > QUERY_DEADLINE_MAX_SECONDS {
            return Err(QueryBudgetsValidationError::DeadlineAboveMax {
                class,
                value: cfg.deadline_seconds,
            });
        }
        if cfg.memory_bytes == 0 {
            return Err(QueryBudgetsValidationError::MemoryZero { class });
        }
        if cfg.memory_bytes > memory_backstop_bytes {
            return Err(QueryBudgetsValidationError::MemoryAboveBackstop {
                class,
                value: cfg.memory_bytes,
                backstop: memory_backstop_bytes,
            });
        }
        if cfg.spill_bytes == 0 {
            return Err(QueryBudgetsValidationError::SpillZero { class });
        }
        if cfg.read_rows == 0 {
            return Err(QueryBudgetsValidationError::ReadRowsZero { class });
        }
        if cfg.read_bytes == 0 {
            return Err(QueryBudgetsValidationError::ReadBytesZero { class });
        }
        if cfg.statement_cap == 0 {
            return Err(QueryBudgetsValidationError::StatementCapZero { class });
        }
        Ok(Self {
            deadline_seconds: cfg.deadline_seconds,
            memory_bytes: cfg.memory_bytes,
            spill_bytes: cfg.spill_bytes,
            read_rows: cfg.read_rows,
            read_bytes: cfg.read_bytes,
            statement_cap: cfg.statement_cap,
        })
    }

    pub fn deadline_seconds(&self) -> f64 {
        self.deadline_seconds
    }

    /// Validated finite and positive, so the conversion cannot panic.
    pub fn deadline(&self) -> std::time::Duration {
        std::time::Duration::from_secs_f64(self.deadline_seconds)
    }

    pub fn memory_bytes(&self) -> u64 {
        self.memory_bytes
    }

    pub fn spill_bytes(&self) -> u64 {
        self.spill_bytes
    }

    pub fn read_rows(&self) -> u64 {
        self.read_rows
    }

    pub fn read_bytes(&self) -> u64 {
        self.read_bytes
    }

    pub fn statement_cap(&self) -> u32 {
        self.statement_cap
    }
}

/// The validated product of `[query_budgets]`: the only type query envelopes
/// accept (amendment A9), so ad-hoc unlimited budgets cannot be constructed
/// in code either. `load_config` performs this validation, making a config
/// with any unbounded budget a load error.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ValidatedQueryBudgets {
    pub interactive: ValidatedQueryBudget,
    pub background: ValidatedQueryBudget,
    pub migration: ValidatedQueryBudget,
    pub administrative: ValidatedQueryBudget,
    pub export: ValidatedQueryBudget,
}

impl ValidatedQueryBudgets {
    /// Validates against the shipped managed-install backstop
    /// [`QUERY_MEMORY_BACKSTOP_BYTES`].
    pub fn from_config(
        cfg: &QueryBudgetsConfig,
    ) -> std::result::Result<Self, QueryBudgetsValidationError> {
        Self::with_memory_backstop(cfg, QUERY_MEMORY_BACKSTOP_BYTES)
    }

    /// Validates against an explicit per-query server memory backstop, for
    /// deployments whose ClickHouse profile differs from the managed one.
    pub fn with_memory_backstop(
        cfg: &QueryBudgetsConfig,
        memory_backstop_bytes: u64,
    ) -> std::result::Result<Self, QueryBudgetsValidationError> {
        Ok(Self {
            interactive: ValidatedQueryBudget::from_class_config(
                "interactive",
                &cfg.interactive,
                memory_backstop_bytes,
            )?,
            background: ValidatedQueryBudget::from_class_config(
                "background",
                &cfg.background,
                memory_backstop_bytes,
            )?,
            migration: ValidatedQueryBudget::from_class_config(
                "migration",
                &cfg.migration,
                memory_backstop_bytes,
            )?,
            administrative: ValidatedQueryBudget::from_class_config(
                "administrative",
                &cfg.administrative,
                memory_backstop_bytes,
            )?,
            export: ValidatedQueryBudget::from_class_config(
                "export",
                &cfg.export,
                memory_backstop_bytes,
            )?,
        })
    }
}

const REDACTED_AUTH_TOKEN: &str = "[REDACTED]";

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackendConfig {
    /// Deprecated compatibility input. The unified backend always launches
    /// from `moraine up`; loaded configurations normalize this value to true.
    #[serde(default = "default_true")]
    pub start_on_up: bool,
    /// Host or interface for the monitor HTTP listener; `monitor.port`
    /// remains the port configuration.
    #[serde(default = "default_backend_bind")]
    pub bind: String,
    /// Startup prerequisite for non-loopback binds. Request authentication
    /// is intentionally outside this config-groundwork change.
    #[serde(default)]
    pub auth_token: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MonitorConfig {
    /// Legacy loader compatibility input for `backend.bind`.
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
    /// Deprecated compatibility key. Its value is validated but no longer
    /// gates the unified backend started by `moraine up`.
    #[serde(default = "default_true")]
    pub start_monitor_on_up: bool,
    /// Deprecated compatibility key retained for configs written by v0.6.0.
    /// Its value is validated but no longer gates backend startup.
    #[serde(default = "default_false")]
    pub start_mcp_on_up: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppConfig {
    #[serde(default)]
    pub clickhouse: ClickHouseConfig,
    /// Named ClickHouse backends. `"default"` always exists after config
    /// load: it is synthesized from `[clickhouse]` (or built-in defaults)
    /// unless declared explicitly as `[backends.default]`. Declaring both
    /// `[clickhouse]` and `[backends.default]` is a load error (ambiguous).
    #[serde(default)]
    pub backends: BTreeMap<String, ClickHouseConfig>,
    /// Ordered directory-glob routes to named backends; first match wins.
    #[serde(default)]
    pub routes: Vec<RouteConfig>,
    #[serde(default)]
    pub identity: IdentityConfig,
    #[serde(default)]
    pub redaction: RedactionConfig,
    #[serde(default)]
    pub ingest: IngestConfig,
    #[serde(default)]
    pub mcp: McpConfig,
    #[serde(default)]
    pub backend: BackendConfig,
    #[serde(default)]
    pub bm25: Bm25Config,
    #[serde(default)]
    pub query_budgets: QueryBudgetsConfig,
    #[serde(default)]
    pub monitor: MonitorConfig,
    #[serde(default)]
    pub runtime: RuntimeConfig,
}

impl AppConfig {
    /// Returns the first route whose `dir` glob matches the absolute
    /// directory `dir` (routes are ordered; first match wins), or `None`
    /// when no route matches. Trailing slashes on `dir` are ignored, and a
    /// glob ending in `/**` also matches its base directory itself
    /// (`~/p/**` matches `~/p`), since sessions usually start at the
    /// project root. `*` does not cross `/`, matching the per-component
    /// semantics of the filesystem glob enumeration used by ingest sources.
    pub fn route_for_dir(&self, dir: &str) -> Option<&RouteConfig> {
        self.routes
            .iter()
            .find(|route| directory_glob_matches(&route.dir, dir))
    }

    /// Returns whether the absolute working directory `dir` matches any
    /// ingest exclusion. Empty directories never match.
    pub fn is_project_dir_excluded(&self, dir: &str) -> bool {
        self.ingest
            .exclude_project_dirs
            .iter()
            .any(|pattern| directory_glob_matches(pattern, dir))
    }
}

fn directory_glob_matches(pattern: &str, dir: &str) -> bool {
    if dir.trim().is_empty() {
        return false;
    }
    let trimmed = dir.trim_end_matches('/');
    let candidate = if trimmed.is_empty() { "/" } else { trimmed };
    let options = glob::MatchOptions {
        case_sensitive: true,
        require_literal_separator: true,
        require_literal_leading_dot: false,
    };
    // Directory globs are validated at load time; a programmatically built
    // config with an invalid glob simply never matches.
    let Ok(pattern_glob) = glob::Pattern::new(pattern) else {
        return false;
    };
    pattern_glob.matches_with(candidate, options)
        || (pattern.ends_with("/**")
            && pattern_glob.matches_with(&format!("{candidate}/"), options))
}

impl Default for ClickHouseConfig {
    fn default() -> Self {
        Self {
            url: default_ch_url(),
            database: default_ch_database(),
            username: default_ch_username(),
            password: String::new(),
            timeout_seconds: default_timeout_seconds(),
            request_compression: ClickHouseRequestCompression::None,
            async_insert: true,
            wait_for_async_insert: true,
            allow_newer_server: false,
        }
    }
}

impl fmt::Debug for BackendConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BackendConfig")
            .field("start_on_up", &self.start_on_up)
            .field("bind", &self.bind)
            .field(
                "auth_token",
                &self.auth_token.as_ref().map(|_| REDACTED_AUTH_TOKEN),
            )
            .finish()
    }
}

impl Default for BackendConfig {
    fn default() -> Self {
        Self {
            start_on_up: true,
            bind: default_backend_bind(),
            auth_token: None,
        }
    }
}

impl Default for AppConfig {
    fn default() -> Self {
        // Uphold the post-load invariant for programmatic construction too:
        // `backends["default"]` always exists and mirrors `clickhouse`.
        let clickhouse = ClickHouseConfig::default();
        let mut backends = BTreeMap::new();
        backends.insert(DEFAULT_BACKEND_NAME.to_string(), clickhouse.clone());
        Self {
            clickhouse,
            backends,
            routes: Vec::new(),
            identity: IdentityConfig::default(),
            redaction: RedactionConfig::default(),
            ingest: IngestConfig::default(),
            mcp: McpConfig::default(),
            backend: BackendConfig::default(),
            bm25: Bm25Config::default(),
            query_budgets: QueryBudgetsConfig::default(),
            monitor: MonitorConfig::default(),
            runtime: RuntimeConfig::default(),
        }
    }
}

impl Default for IngestConfig {
    fn default() -> Self {
        Self {
            exclude_project_dirs: Vec::new(),
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
            ack_observation: false,
        }
    }
}

impl Default for RedactionConfig {
    fn default() -> Self {
        Self {
            dangerously_skip_secret_redaction: false,
            ruleset: default_redaction_ruleset(),
            extra_patterns: Vec::new(),
            dangerously_skip_secret_redaction_ignored: false,
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
            max_parallel_requests: None,
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
    // Cursor's IDE state directory differs by platform.
    let cursor_state_root = if cfg!(target_os = "macos") {
        "~/Library/Application Support/Cursor/User"
    } else {
        "~/.config/Cursor/User"
    };
    let mut sources = vec![
        IngestSource {
            name: "codex".to_string(),
            harness: "codex".to_string(),
            enabled: true,
            glob: "~/.codex/sessions/**/*.jsonl".to_string(),
            watch_root: "~/.codex/sessions".to_string(),
            format: SourceFormat::Infer,
        },
        IngestSource {
            name: "claude".to_string(),
            harness: "claude-code".to_string(),
            enabled: true,
            glob: "~/.claude/projects/**/*.jsonl".to_string(),
            watch_root: "~/.claude/projects".to_string(),
            format: SourceFormat::Infer,
        },
    ];
    #[cfg(target_os = "macos")]
    sources.push(IngestSource {
        name: "claude-cowork".to_string(),
        harness: "claude-code".to_string(),
        enabled: true,
        glob: "~/Library/Application Support/Claude/local-agent-mode-sessions/**/.claude/projects/**/*.jsonl".to_string(),
        watch_root:
            "~/Library/Application Support/Claude/local-agent-mode-sessions".to_string(),
        format: SourceFormat::Infer,
    });
    sources.extend([
        IngestSource {
            name: "hermes".to_string(),
            harness: "hermes".to_string(),
            enabled: true,
            glob: "~/.hermes/sessions/session_*.json".to_string(),
            watch_root: "~/.hermes/sessions".to_string(),
            format: SourceFormat::Infer,
        },
        IngestSource {
            name: "kiro".to_string(),
            harness: "kiro-cli".to_string(),
            enabled: true,
            glob: "~/.kiro/sessions/cli/*.jsonl".to_string(),
            watch_root: "~/.kiro/sessions/cli".to_string(),
            format: SourceFormat::KiroSession,
        },
        IngestSource {
            name: "kimi-cli".to_string(),
            harness: "kimi-cli".to_string(),
            enabled: true,
            glob: "~/.kimi/sessions/**/wire.jsonl".to_string(),
            watch_root: "~/.kimi/sessions".to_string(),
            format: SourceFormat::Infer,
        },
        IngestSource {
            name: "qwen-code".to_string(),
            harness: "qwen-code".to_string(),
            enabled: true,
            glob: "~/.qwen/projects/*/chats/*.jsonl".to_string(),
            watch_root: "~/.qwen/projects".to_string(),
            format: SourceFormat::Jsonl,
        },
        IngestSource {
            name: "opencode".to_string(),
            harness: "opencode".to_string(),
            enabled: true,
            glob: "~/.local/share/opencode/opencode*.db".to_string(),
            watch_root: "~/.local/share/opencode".to_string(),
            format: SourceFormat::OpenCodeSqlite,
        },
        IngestSource {
            name: "cursor".to_string(),
            harness: "cursor".to_string(),
            enabled: true,
            glob: "~/.cursor/projects/*/agent-transcripts/**/*.jsonl".to_string(),
            watch_root: "~/.cursor/projects".to_string(),
            format: SourceFormat::Infer,
        },
        IngestSource {
            name: "cursor-sqlite".to_string(),
            harness: "cursor".to_string(),
            enabled: true,
            glob: format!("{cursor_state_root}/**/state.vscdb"),
            watch_root: cursor_state_root.to_string(),
            format: SourceFormat::CursorSqlite,
        },
        IngestSource {
            name: "pi".to_string(),
            harness: "pi-coding-agent".to_string(),
            enabled: true,
            glob: "~/.pi/agent/sessions/**/*.jsonl".to_string(),
            watch_root: "~/.pi/agent/sessions".to_string(),
            format: SourceFormat::Infer,
        },
        IngestSource {
            name: "omp".to_string(),
            harness: "pi-coding-agent".to_string(),
            enabled: true,
            glob: "~/.omp/agent/sessions/**/*.jsonl".to_string(),
            watch_root: "~/.omp/agent/sessions".to_string(),
            format: SourceFormat::Infer,
        },
    ]);
    sources
}

pub const SOURCE_FORMAT_JSONL: &str = "jsonl";
pub const SOURCE_FORMAT_SESSION_JSON: &str = "session_json";
pub const SOURCE_FORMAT_KIRO_SESSION: &str = "kiro_session";
pub const SOURCE_FORMAT_CURSOR_SQLITE: &str = "cursor_sqlite";
pub const SOURCE_FORMAT_NAC_SQLITE: &str = "nac_sqlite";
pub const SOURCE_FORMAT_OPENCODE_SQLITE: &str = "opencode_sqlite";

/// SQLite WAL sidecar suffixes that must map back to the canonical database
/// path for watching/debouncing. WAL-mode writes often touch only these files,
/// so dropping them would miss updates entirely (issue #361, decision 5).
const SQLITE_SIDECAR_SUFFIXES: &[&str] = &["-wal", "-shm"];

fn infer_source_format(harness: &str, glob: &str) -> SourceFormat {
    let glob_lower = glob.to_ascii_lowercase();
    if harness == "cursor" && glob_lower.ends_with(".vscdb") {
        return SourceFormat::CursorSqlite;
    }
    if harness == "nac"
        && Path::new(&glob_lower)
            .file_name()
            .and_then(|name| name.to_str())
            == Some("store.db")
    {
        return SourceFormat::NacSqlite;
    }
    if harness == "opencode" && opencode_db_name_matches(Path::new(&glob_lower)) {
        return SourceFormat::OpenCodeSqlite;
    }
    if harness == "kiro-cli" {
        return SourceFormat::KiroSession;
    }
    let looks_like_json = !glob_lower.ends_with(".jsonl")
        && (glob_lower.ends_with(".json") || glob_lower.contains(".json"));
    if harness == "hermes" && looks_like_json {
        SourceFormat::SessionJson
    } else {
        SourceFormat::Jsonl
    }
}

fn normalize_source_format(
    format: SourceFormat,
    harness: &str,
    glob: &str,
    source_idx: usize,
    source_name: &str,
) -> Result<SourceFormat> {
    let resolved = if format == SourceFormat::Infer {
        infer_source_format(harness, glob)
    } else {
        format
    };
    match resolved {
        SourceFormat::Infer => {
            unreachable!("source format inference must resolve to a concrete format")
        }
        SourceFormat::Jsonl | SourceFormat::SessionJson => Ok(resolved),
        SourceFormat::KiroSession => {
            if harness == "kiro-cli" {
                Ok(resolved)
            } else {
                Err(anyhow::anyhow!(
                    "ingest.sources[{source_idx}].format `{SOURCE_FORMAT_KIRO_SESSION}` requires harness `kiro-cli` (source `{source_name}` has harness `{harness}`); its metadata sidecars only normalize through the Kiro CLI adapter"
                ))
            }
        }
        SourceFormat::CursorSqlite => {
            if harness == "cursor" {
                Ok(resolved)
            } else {
                Err(anyhow::anyhow!(
                    "ingest.sources[{source_idx}].format `{SOURCE_FORMAT_CURSOR_SQLITE}` requires harness `cursor` (source `{source_name}` has harness `{harness}`); its synthetic records only normalize through the cursor adapter"
                ))
            }
        }
        SourceFormat::NacSqlite => {
            if harness == "nac" {
                Ok(resolved)
            } else {
                Err(anyhow::anyhow!(
                    "ingest.sources[{source_idx}].format `{SOURCE_FORMAT_NAC_SQLITE}` requires harness `nac` (source `{source_name}` has harness `{harness}`); its synthetic records only normalize through the nac adapter"
                ))
            }
        }
        SourceFormat::OpenCodeSqlite => {
            if harness == "opencode" {
                Ok(resolved)
            } else {
                Err(anyhow::anyhow!(
                    "ingest.sources[{source_idx}].format `{SOURCE_FORMAT_OPENCODE_SQLITE}` requires harness `opencode` (source `{source_name}` has harness `{harness}`); its synthetic records only normalize through the opencode adapter"
                ))
            }
        }
    }
}

impl IngestSource {
    /// Returns the file extension (without leading `.`) this source's format
    /// records are stored in: `jsonl`, `json`, `vscdb`, or `db`.
    pub fn tracked_extension(&self) -> &'static str {
        format_tracked_extension(self.format)
            .expect("source format must be normalized before selecting a tracked extension")
    }
}

fn format_tracked_extension(format: SourceFormat) -> Option<&'static str> {
    match format {
        SourceFormat::Infer => None,
        SourceFormat::Jsonl => Some("jsonl"),
        SourceFormat::SessionJson => Some("json"),
        SourceFormat::KiroSession => Some("jsonl"),
        SourceFormat::CursorSqlite => Some("vscdb"),
        SourceFormat::NacSqlite | SourceFormat::OpenCodeSqlite => Some("db"),
    }
}

fn opencode_db_name_matches(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .map(|name| {
            let lower = name.to_ascii_lowercase();
            lower.starts_with("opencode")
                && lower.ends_with(".db")
                && !lower.ends_with(".db-wal")
                && !lower.ends_with(".db-shm")
        })
        .unwrap_or(false)
}

/// Maps a filesystem path seen by enumeration or the watcher to the canonical
/// tracked path for `format`, or `None` when the path is not tracked.
///
/// For file-backed formats this is an extension filter that returns the path
/// unchanged. SQLite sidecars map back to their base database. NAC is stricter:
/// its configured glob must equal `glob::Pattern::escape(candidate)`, so only
/// the literal configured database and its exact WAL/SHM sidecars are accepted.
/// Anything else — including `state.vscdb.backup` — is untracked.
pub fn map_tracked_path(format: impl AsRef<str>, source_glob: &str, path: &str) -> Option<String> {
    let format = SourceFormat::parse(format.as_ref())?;
    let has_extension = |candidate: &str, extension: &str| {
        Path::new(candidate)
            .extension()
            .and_then(|s| s.to_str())
            .map(|ext| ext == extension)
            .unwrap_or(false)
    };
    let map_extension_or_sidecar = |extension: &str| {
        if has_extension(path, extension) {
            return Some(path.to_string());
        }
        for suffix in SQLITE_SIDECAR_SUFFIXES {
            if let Some(base) = path.strip_suffix(suffix) {
                if has_extension(base, extension) {
                    return Some(base.to_string());
                }
            }
        }
        None
    };

    match format {
        SourceFormat::Infer => None,
        SourceFormat::Jsonl | SourceFormat::SessionJson => {
            let extension = format_tracked_extension(format)
                .expect("concrete file format must have a tracked extension");
            has_extension(path, extension).then(|| path.to_string())
        }
        SourceFormat::KiroSession => {
            match Path::new(path).extension().and_then(|ext| ext.to_str()) {
                Some("jsonl") => Some(path.to_string()),
                Some("json") => Some(
                    Path::new(path)
                        .with_extension("jsonl")
                        .to_string_lossy()
                        .into_owned(),
                ),
                _ => None,
            }
        }
        SourceFormat::CursorSqlite => map_extension_or_sidecar(
            format_tracked_extension(format)
                .expect("concrete SQLite format must have a tracked extension"),
        ),
        SourceFormat::NacSqlite => {
            let matches_configured =
                |candidate: &str| escape_literal_glob(candidate) == source_glob;
            if matches_configured(path) {
                return Some(path.to_string());
            }
            for suffix in SQLITE_SIDECAR_SUFFIXES {
                if let Some(base) = path.strip_suffix(suffix) {
                    if matches_configured(base) {
                        return Some(base.to_string());
                    }
                }
            }
            None
        }
        SourceFormat::OpenCodeSqlite => {
            if opencode_db_name_matches(Path::new(path)) {
                return Some(path.to_string());
            }
            for suffix in SQLITE_SIDECAR_SUFFIXES {
                if let Some(base) = path.strip_suffix(suffix) {
                    if opencode_db_name_matches(Path::new(base)) {
                        return Some(base.to_string());
                    }
                }
            }
            None
        }
    }
}

/// Escapes a literal filesystem path for storage in a glob field.
pub fn escape_literal_glob(path: &str) -> String {
    glob::Pattern::escape(path)
}

/// True for Claude Code `Workflow` orchestration journals, which share the
/// claude-code source glob/extension but are not user sessions and must never
/// be ingested.
///
/// A `Workflow` run writes `<project>/<session-uuid>/subagents/workflows/<wf>/journal.jsonl`,
/// a `started`/`result` event log with no `sessionId`. The recursive
/// `~/.claude/projects/**/*.jsonl` glob predates this Claude Code feature and
/// slurps these journals; lacking a `sessionId` they land as empty-`session_id`
/// `unknown` events that break `list_sessions` for any time range overlapping
/// them (issue #386).
///
/// The match is scoped deliberately tight — only a file literally named
/// `journal.jsonl` beneath a `subagents/workflows/` directory. The sibling
/// `agent-*.jsonl` transcripts in the same workflow directory (and the
/// `subagents/agent-*.jsonl` Task-subagent transcripts) DO carry valid
/// `sessionId`s and remain ingestible.
pub fn is_workflow_journal_path(path: &str) -> bool {
    let path = Path::new(path);
    if path.file_name().and_then(|name| name.to_str()) != Some("journal.jsonl") {
        return false;
    }

    // Single allocation-free pass over the path's named segments, looking for
    // an adjacent `subagents/workflows` pair. Non-`Normal` components (root,
    // prefix) and any non-UTF-8 segment are skipped without advancing the
    // window, so they never bridge the pair we are matching.
    let mut prev: Option<&str> = None;
    for component in path.components() {
        let Component::Normal(segment) = component else {
            continue;
        };
        let Some(segment) = segment.to_str() else {
            continue;
        };
        if prev == Some("subagents") && segment == "workflows" {
            return true;
        }
        prev = Some(segment);
    }
    false
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

fn default_route_mode() -> String {
    ROUTE_MODE_MIRROR.to_string()
}

fn default_redaction_ruleset() -> String {
    "builtin".to_string()
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

fn default_backend_bind() -> String {
    "127.0.0.1".to_string()
}

fn default_monitor_host() -> String {
    default_backend_bind()
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

/// Synthesizes the `"default"` backend from `[clickhouse]` — or copies an
/// explicit `[backends.default]` back onto `cfg.clickhouse` so existing call
/// sites keep working unchanged — then expands `~` in route dir globs and
/// validates that every route names a configured backend with a parseable
/// glob and a supported mode. The both-declared ambiguity (`[clickhouse]`
/// AND `[backends.default]`) is rejected earlier in `load_config`, where the
/// raw TOML document is still visible.
fn normalize_backends_and_routes(cfg: &mut AppConfig) -> Result<()> {
    match cfg.backends.get(DEFAULT_BACKEND_NAME) {
        Some(default_backend) => cfg.clickhouse = default_backend.clone(),
        None => {
            cfg.backends
                .insert(DEFAULT_BACKEND_NAME.to_string(), cfg.clickhouse.clone());
        }
    }

    for (route_idx, route) in cfg.routes.iter_mut().enumerate() {
        route.dir = expand_path(route.dir.trim());
        if route.dir.is_empty() {
            return Err(anyhow::anyhow!(
                "routes[{route_idx}].dir must be a non-empty directory glob"
            ));
        }
        glob::Pattern::new(&route.dir).map_err(|exc| {
            anyhow::anyhow!(
                "invalid routes[{route_idx}].dir glob `{}`: {exc}",
                route.dir
            )
        })?;

        route.backend = route.backend.trim().to_string();
        if !cfg.backends.contains_key(&route.backend) {
            return Err(anyhow::anyhow!(
                "routes[{route_idx}].backend `{}` is not a configured backend; configured backends: {}",
                route.backend,
                cfg.backends.keys().cloned().collect::<Vec<_>>().join(", ")
            ));
        }

        let mode = route.mode.trim().to_ascii_lowercase();
        if mode != ROUTE_MODE_MIRROR {
            return Err(anyhow::anyhow!(
                "invalid routes[{route_idx}].mode `{}` (backend `{}`); only `{ROUTE_MODE_MIRROR}` is supported (`exclusive` is not implemented yet)",
                route.mode.trim(),
                route.backend
            ));
        }
        route.mode = mode;
    }

    Ok(())
}

#[cfg(target_os = "macos")]
fn migrate_legacy_claude_source(sources: &mut Vec<IngestSource>) {
    if sources
        .iter()
        .any(|source| source.name.trim() == "claude-cowork")
    {
        return;
    }

    let Some(enabled) = sources
        .iter()
        .find(|source| {
            source.name.trim() == "claude"
                && source.harness.trim() == "claude-code"
                && source.glob.trim() == "~/.claude/projects/**/*.jsonl"
                && source.watch_root.trim() == "~/.claude/projects"
                && matches!(source.format, SourceFormat::Infer | SourceFormat::Jsonl)
        })
        .map(|source| source.enabled)
    else {
        return;
    };

    sources.push(IngestSource {
        name: "claude-cowork".to_string(),
        harness: "claude-code".to_string(),
        enabled,
        glob: "~/Library/Application Support/Claude/local-agent-mode-sessions/**/.claude/projects/**/*.jsonl".to_string(),
        watch_root:
            "~/Library/Application Support/Claude/local-agent-mode-sessions".to_string(),
        format: SourceFormat::Infer,
    });
}

fn migrate_legacy_pi_source(sources: &mut Vec<IngestSource>) {
    if sources.iter().any(|source| source.name.trim() == "omp") {
        return;
    }

    let Some(enabled) = sources
        .iter()
        .find(|source| {
            source.name.trim() == "pi"
                && source.harness.trim() == "pi-coding-agent"
                && source.glob.trim() == "~/.pi/agent/sessions/**/*.jsonl"
                && source.watch_root.trim() == "~/.pi/agent/sessions"
                && matches!(source.format, SourceFormat::Infer | SourceFormat::Jsonl)
        })
        .map(|source| source.enabled)
    else {
        return;
    };

    // Configs created before OMP moved its session tree explicitly list only
    // the setup-owned Pi source, so Serde defaults cannot add the new source.
    // Migrate that exact legacy source in memory without rewriting user config
    // or changing customized Pi sources. A distinct source name gives OMP a
    // fresh checkpoint namespace, causing the first post-upgrade startup to
    // backfill the full ~/.omp tree.
    sources.push(IngestSource {
        name: "omp".to_string(),
        harness: "pi-coding-agent".to_string(),
        enabled,
        glob: "~/.omp/agent/sessions/**/*.jsonl".to_string(),
        watch_root: "~/.omp/agent/sessions".to_string(),
        format: SourceFormat::Infer,
    });
}

fn expand_ingest_source_path(
    source: &IngestSource,
    path: &str,
    kiro_home: Option<&Path>,
) -> String {
    if source.name == "kiro" && source.harness == "kiro-cli" {
        if let (Some(kiro_home), Some(relative)) = (kiro_home, path.strip_prefix("~/.kiro")) {
            if relative.is_empty() || relative.starts_with('/') {
                return kiro_home
                    .join(relative.trim_start_matches('/'))
                    .to_string_lossy()
                    .into_owned();
            }
        }
    }
    expand_path(path)
}

fn normalize_config(mut cfg: AppConfig) -> Result<AppConfig> {
    if cfg.mcp.max_parallel_requests == Some(0) {
        return Err(anyhow::anyhow!(
            "mcp.max_parallel_requests must be greater than zero when configured"
        ));
    }

    // Fail closed at load time: a config carrying any unbounded query budget
    // never produces a usable AppConfig (issue #600 exit gate 7).
    ValidatedQueryBudgets::from_config(&cfg.query_budgets)
        .map_err(|error| anyhow::anyhow!("{error}"))?;

    for (exclude_idx, pattern) in cfg.ingest.exclude_project_dirs.iter_mut().enumerate() {
        *pattern = expand_path(pattern.trim());
        if pattern.is_empty() {
            return Err(anyhow::anyhow!(
                "ingest.exclude_project_dirs[{exclude_idx}] must be a non-empty directory glob"
            ));
        }
        glob::Pattern::new(pattern.as_str()).map_err(|exc| {
            anyhow::anyhow!(
                "invalid ingest.exclude_project_dirs[{exclude_idx}] glob `{pattern}`: {exc}"
            )
        })?;
    }

    #[cfg(target_os = "macos")]
    migrate_legacy_claude_source(&mut cfg.ingest.sources);
    migrate_legacy_pi_source(&mut cfg.ingest.sources);
    let kiro_home = std::env::var_os("KIRO_HOME").map(PathBuf::from);
    for (source_idx, source) in cfg.ingest.sources.iter_mut().enumerate() {
        source.harness = normalize_harness(&source.harness, source_idx, &source.name)?;
        source.glob = expand_ingest_source_path(source, &source.glob, kiro_home.as_deref());
        source.watch_root = if source.watch_root.trim().is_empty() {
            watch_root_from_glob(&source.glob)
        } else {
            expand_ingest_source_path(source, &source.watch_root, kiro_home.as_deref())
        };
        source.format = normalize_source_format(
            source.format,
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

    normalize_backends_and_routes(&mut cfg)?;
    normalize_redaction(&mut cfg)?;
    normalize_identity(&mut cfg);

    Ok(cfg)
}

fn normalize_identity(cfg: &mut AppConfig) {
    cfg.identity.author = cfg.identity.author.trim().to_string();
}

fn normalize_redaction(cfg: &mut AppConfig) -> Result<()> {
    cfg.redaction.ruleset = cfg.redaction.ruleset.trim().to_ascii_lowercase();
    if cfg.redaction.ruleset.is_empty() {
        cfg.redaction.ruleset = default_redaction_ruleset();
    }
    if cfg.redaction.ruleset != "builtin" {
        return Err(anyhow::anyhow!(
            "invalid redaction.ruleset `{}`; only `builtin` is supported",
            cfg.redaction.ruleset
        ));
    }
    cfg.redaction.extra_patterns = cfg
        .redaction
        .extra_patterns
        .iter()
        .map(|pattern| pattern.trim().to_string())
        .filter(|pattern| !pattern.is_empty())
        .collect();
    Ok(())
}

fn paths_refer_to_same_file(path: &Path, other: &Path) -> bool {
    match (std::fs::canonicalize(path), std::fs::canonicalize(other)) {
        (Ok(path), Ok(other)) => path == other,
        _ => path == other,
    }
}

fn path_is_home_config(path: &Path, home_path: Option<&Path>) -> bool {
    home_path
        .map(|home| paths_refer_to_same_file(path, home))
        .unwrap_or(false)
}

fn raw_config_bool(raw: &toml::Value, section: &str, field: &str) -> Option<bool> {
    raw.get(section)
        .and_then(|section| section.get(field))
        .and_then(toml::Value::as_bool)
}

fn is_explicit_loopback_bind(bind: &str) -> bool {
    let ip_literal = bind
        .strip_prefix('[')
        .and_then(|inner| inner.strip_suffix(']'))
        .unwrap_or(bind);
    ip_literal
        .parse::<IpAddr>()
        .map(|address| address.is_loopback())
        .unwrap_or(false)
}

fn backend_launch_was_explicitly_enabled(raw: &toml::Value) -> bool {
    match raw_config_bool(raw, "backend", "start_on_up") {
        Some(enabled) => enabled,
        None => {
            raw_config_bool(raw, "runtime", "start_monitor_on_up").unwrap_or(false)
                || raw_config_bool(raw, "runtime", "start_mcp_on_up").unwrap_or(false)
                || raw_config_bool(raw, "mcp", "start_central_on_up").unwrap_or(false)
        }
    }
}

fn validate_non_loopback_backend_upgrade(cfg: &AppConfig, raw: &toml::Value) -> Result<()> {
    if is_explicit_loopback_bind(&cfg.backend.bind) || backend_launch_was_explicitly_enabled(raw) {
        return Ok(());
    }

    Err(anyhow::anyhow!(
        "refusing to automatically enable the backend on non-loopback bind `{}`: set backend.start_on_up = true to acknowledge startup, or change backend.bind to an explicit loopback IP",
        cfg.backend.bind
    ))
}

fn normalize_backend_start_on_up(cfg: &mut AppConfig, _raw: &toml::Value) {
    // Deserialization above still validates the canonical and legacy launch
    // keys so existing configuration files remain load-compatible. Their
    // values no longer gate startup: every `moraine up` includes the backend.
    cfg.backend.start_on_up = true;
}

fn normalize_backend_bind(cfg: &mut AppConfig, raw: &toml::Value) {
    if raw
        .get("backend")
        .and_then(|backend| backend.get("bind"))
        .is_some()
    {
        return;
    }

    if raw
        .get("monitor")
        .and_then(|monitor| monitor.get("host"))
        .is_some()
    {
        cfg.backend.bind.clone_from(&cfg.monitor.host);
    }
}

fn parse_config_toml<T: DeserializeOwned>(content: &str) -> Result<T> {
    toml::from_str(content).map_err(|error| {
        let location = error
            .span()
            .map(|span| {
                let offset = span.start.min(content.len());
                let prefix = &content[..offset];
                let line = prefix.bytes().filter(|byte| *byte == b'\n').count() + 1;
                let column = prefix
                    .rsplit('\n')
                    .next()
                    .map(|line| line.chars().count() + 1)
                    .unwrap_or(1);
                format!(" at line {line}, column {column}")
            })
            .unwrap_or_default();
        anyhow::anyhow!("failed to parse TOML config{location}: {}", error.message())
    })
}

fn load_config_with_home_path(path: &Path, home_path: Option<PathBuf>) -> Result<AppConfig> {
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read config {}", path.display()))?;
    let mut cfg: AppConfig = parse_config_toml(&content)?;
    // The struct-level parse cannot tell an explicit `[clickhouse]` block
    // from its serde default, so the both-declared ambiguity is detected on
    // the raw TOML document instead.
    let raw: toml::Value = parse_config_toml(&content)?;
    if raw.get("clickhouse").is_some()
        && raw
            .get("backends")
            .and_then(|backends| backends.get(DEFAULT_BACKEND_NAME))
            .is_some()
    {
        return Err(anyhow::anyhow!(
            "config declares both [clickhouse] and [backends.default]; they are aliases for the same backend — keep exactly one"
        ));
    }
    normalize_backend_bind(&mut cfg, &raw);
    validate_non_loopback_backend_upgrade(&cfg, &raw)?;
    normalize_backend_start_on_up(&mut cfg, &raw);

    if cfg.redaction.dangerously_skip_secret_redaction
        && !path_is_home_config(path, home_path.as_deref())
    {
        cfg.redaction.dangerously_skip_secret_redaction = false;
        cfg.redaction.dangerously_skip_secret_redaction_ignored = true;
    }

    normalize_config(cfg)
}

pub fn load_config(path: impl AsRef<Path>) -> Result<AppConfig> {
    load_config_with_home_path(path.as_ref(), home_config_path())
}

/// Name of the optional repo-level backend reference file. It carries a
/// backend *name only* — never credentials or URLs — and resolves only if
/// that name exists in the user's home config. This is the trust boundary:
/// a hostile cloned repo cannot redirect traces.
pub const REPO_BACKEND_FILE: &str = ".moraine.toml";

/// Walks up from `start_dir` (inclusive) looking for a [`REPO_BACKEND_FILE`]
/// and returns the `backend = "<name>"` it declares. The walk stops after
/// checking `$HOME` (when `start_dir` is beneath it) or the filesystem root,
/// whichever comes first. The nearest file wins and ends the walk even when
/// it declares no usable name; unknown keys in the file are ignored, and a
/// malformed file is treated as absent. Callers are expected to pass an
/// absolute directory, warn on names that do not resolve against
/// `AppConfig::backends`, and treat that as no route.
pub fn find_repo_backend_ref(start_dir: impl AsRef<Path>) -> Option<String> {
    let home = std::env::var_os("HOME").map(PathBuf::from);
    find_repo_backend_ref_bounded(start_dir.as_ref(), home.as_deref())
}

fn find_repo_backend_ref_bounded(start_dir: &Path, stop_at: Option<&Path>) -> Option<String> {
    let mut dir = start_dir;
    loop {
        let candidate = dir.join(REPO_BACKEND_FILE);
        if candidate.is_file() {
            return parse_repo_backend_ref(&candidate);
        }
        if stop_at == Some(dir) {
            return None;
        }
        dir = dir.parent()?;
    }
}

fn parse_repo_backend_ref(path: &Path) -> Option<String> {
    // Deliberately NOT deny_unknown_fields: the repo file may grow keys in
    // future versions and older binaries must keep ignoring them.
    #[derive(Deserialize)]
    struct RepoBackendFile {
        backend: Option<String>,
    }

    let content = std::fs::read_to_string(path).ok()?;
    let parsed: RepoBackendFile = toml::from_str(&content).ok()?;
    let name = parsed.backend?.trim().to_string();
    if name.is_empty() {
        None
    } else {
        Some(name)
    }
}

/// Stable, opaque identity for a project root.
///
/// Git repositories use their common directory so linked worktrees share one
/// identity. A non-Git directory uses its canonical path and therefore stays
/// scoped to that exact launch directory. The optional repo backend marker
/// controls routing independently and is not part of either identity.
pub fn project_id_for_repo_root(root: impl AsRef<Path>) -> Option<String> {
    let root = root.as_ref();
    if let Some(common_dir) = git_common_dir(root) {
        return Some(project_id_for_common_dir(&common_dir));
    }
    let canonical_root = root.canonicalize().ok()?;
    canonical_root
        .is_dir()
        .then(|| project_id_for_directory_root(&canonical_root))
}

fn project_id_for_common_dir(common_dir: &Path) -> String {
    project_id_for_path(b"moraine-git-common-dir\0", "git", common_dir)
}

fn project_id_for_directory_root(root: &Path) -> String {
    project_id_for_path(b"moraine-directory-root\0", "dir", root)
}

fn project_id_for_path(domain: &[u8], prefix: &str, path: &Path) -> String {
    let mut hasher = Sha256::new();
    hasher.update(domain);
    #[cfg(unix)]
    hasher.update(path.as_os_str().as_bytes());
    #[cfg(windows)]
    for unit in path.as_os_str().encode_wide() {
        hasher.update(unit.to_le_bytes());
    }
    #[cfg(not(any(unix, windows)))]
    hasher.update(path.to_string_lossy().as_bytes());
    format!("{prefix}:{:x}", hasher.finalize())
}

/// UTF-8 roots currently associated with this project identity.
///
/// Git repositories include registered linked worktrees. Non-Git projects
/// include only the supplied directory and its canonical spelling.
pub fn worktree_roots_for_repo_root(root: impl AsRef<Path>) -> Option<Vec<String>> {
    let pwd = std::env::var_os("PWD").map(PathBuf::from);
    worktree_roots_for_repo_root_with_pwd(root.as_ref(), pwd.as_deref())
}

fn worktree_roots_for_repo_root_with_pwd(
    root: &Path,
    logical_cwd: Option<&Path>,
) -> Option<Vec<String>> {
    let common_dir = git_common_dir(root);
    let canonical_root = root.canonicalize().ok()?;
    if !canonical_root.is_dir() {
        return None;
    }
    let mut roots = vec![root.to_path_buf()];

    if let Some(common_dir) = common_dir.as_deref() {
        if common_dir.file_name().is_some_and(|name| name == ".git") {
            if let Some(main_root) = common_dir.parent() {
                roots.push(main_root.to_path_buf());
            }
        }

        if let Ok(entries) = std::fs::read_dir(common_dir.join("worktrees")) {
            for entry in entries.flatten() {
                let gitdir_file = entry.path().join("gitdir");
                let Ok(content) = std::fs::read_to_string(&gitdir_file) else {
                    continue;
                };
                let git_marker = Path::new(content.trim());
                let git_marker = if git_marker.is_absolute() {
                    git_marker.to_path_buf()
                } else {
                    entry.path().join(git_marker)
                };
                if let Some(worktree_root) = git_marker.parent() {
                    // A stale worktree registration may point at a path that
                    // was deleted and later reused by another repository. Only
                    // roots that still resolve to this common directory are
                    // safe to add to the current mapping; already-durable
                    // deleted roots are handled by the repository layer.
                    if git_common_dir(worktree_root).as_deref() == Some(common_dir) {
                        roots.push(worktree_root.to_path_buf());
                    }
                }
            }
        }
    }

    if let Some(logical_cwd) = logical_cwd {
        if let Ok(canonical_cwd) = logical_cwd.canonicalize() {
            if let Ok(relative_cwd) = canonical_cwd.strip_prefix(&canonical_root) {
                let depth = relative_cwd.components().count();
                if let Some(logical_root) = logical_cwd.ancestors().nth(depth) {
                    if logical_root.canonicalize().ok().as_ref() == Some(&canonical_root) {
                        roots.push(logical_root.to_path_buf());
                    }
                }
            }
        }
    }

    roots.extend(
        roots
            .iter()
            .filter_map(|root| root.canonicalize().ok())
            .collect::<Vec<_>>(),
    );
    let mut roots = roots
        .into_iter()
        .filter_map(|root| root.to_str().map(str::to_owned))
        .collect::<Vec<_>>();
    roots.sort();
    roots.dedup();
    Some(roots)
}

fn git_common_dir(root: &Path) -> Option<PathBuf> {
    let marker = root.join(".git");
    let git_dir = if marker.is_dir() {
        marker
    } else {
        let content = std::fs::read_to_string(marker).ok()?;
        let path = content.trim().strip_prefix("gitdir:")?.trim();
        let path = Path::new(path);
        if path.is_absolute() {
            path.to_path_buf()
        } else {
            root.join(path)
        }
    };
    let common_dir_file = git_dir.join("commondir");
    let common_dir = if common_dir_file.is_file() {
        let content = std::fs::read_to_string(common_dir_file).ok()?;
        let path = Path::new(content.trim());
        if path.is_absolute() {
            path.to_path_buf()
        } else {
            git_dir.join(path)
        }
    } else {
        git_dir
    };
    common_dir.canonicalize().ok()
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

    fn assert_backend_start_on_up(label: &str, contents: &str, expected: bool) {
        let path = write_temp_config(contents, label);
        let cfg = load_config(&path).expect("backend launch config should load");
        std::fs::remove_file(&path).ok();
        assert_eq!(cfg.backend.start_on_up, expected, "case `{label}`");
    }

    #[test]
    fn project_id_unifies_linked_worktrees_but_not_other_repositories() {
        let base = std::env::temp_dir().join(format!(
            "moraine-project-id-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time after unix epoch")
                .as_nanos()
        ));
        let main = base.join("main");
        let linked = base.join("linked");
        let other = base.join("other");
        let linked_git_dir = main.join(".git/worktrees/linked");
        let stale_git_dir = main.join(".git/worktrees/stale");
        std::fs::create_dir_all(&linked_git_dir).expect("create main git dirs");
        std::fs::create_dir_all(&stale_git_dir).expect("create stale git dirs");
        std::fs::create_dir_all(&linked).expect("create linked worktree");
        std::fs::create_dir_all(other.join(".git")).expect("create other git dir");
        std::fs::write(linked_git_dir.join("commondir"), "../..\n")
            .expect("write common-dir pointer");
        std::fs::write(
            linked_git_dir.join("gitdir"),
            format!("{}\n", linked.join(".git").to_string_lossy()),
        )
        .expect("write linked worktree pointer");
        std::fs::write(
            linked.join(".git"),
            format!("gitdir: {}\n", linked_git_dir.to_string_lossy()),
        )
        .expect("write linked git pointer");
        std::fs::write(
            stale_git_dir.join("gitdir"),
            format!("{}\n", other.join(".git").to_string_lossy()),
        )
        .expect("write stale worktree pointer");

        let main_id = project_id_for_repo_root(&main).expect("main project id");
        let linked_id = project_id_for_repo_root(&linked).expect("linked project id");
        let other_id = project_id_for_repo_root(&other).expect("other project id");
        assert!(main_id.starts_with("git:"));
        assert_eq!(main_id, linked_id);
        assert_ne!(main_id, other_id);
        assert_eq!(find_repo_backend_ref(&main), None);
        assert_eq!(find_repo_backend_ref(&linked), None);
        assert_eq!(find_repo_backend_ref(&other), None);
        let main_roots =
            worktree_roots_for_repo_root(&main).expect("main registered worktree roots");
        let linked_roots =
            worktree_roots_for_repo_root(&linked).expect("linked registered worktree roots");
        let canonical_main = main.canonicalize().expect("canonical main");
        let canonical_linked = linked.canonicalize().expect("canonical linked");
        let canonical_other = other.canonicalize().expect("canonical other");
        for roots in [&main_roots, &linked_roots] {
            assert!(roots.contains(&canonical_main.to_string_lossy().to_string()));
            assert!(roots.contains(&canonical_linked.to_string_lossy().to_string()));
            assert!(!roots.contains(&canonical_other.to_string_lossy().to_string()));
        }

        std::fs::remove_dir_all(base).ok();
    }

    #[test]
    fn project_id_scopes_non_git_projects_to_the_exact_directory() {
        let base = std::env::temp_dir().join(format!(
            "moraine-directory-project-id-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time after unix epoch")
                .as_nanos()
        ));
        let root = base.join("project");
        let nested = root.join("nested");
        std::fs::create_dir_all(&nested).expect("create non-Git project directories");

        let root_id = project_id_for_repo_root(&root).expect("directory project id");
        let nested_id = project_id_for_repo_root(&nested).expect("nested directory project id");
        assert!(root_id.starts_with("dir:"));
        assert!(nested_id.starts_with("dir:"));
        assert_ne!(root_id, nested_id);

        let roots = worktree_roots_for_repo_root(&root).expect("directory project roots");
        assert!(roots.contains(&root.to_string_lossy().to_string()));
        assert!(roots.contains(
            &root
                .canonicalize()
                .expect("canonical directory project root")
                .to_string_lossy()
                .to_string()
        ));
        assert_eq!(project_id_for_repo_root(root.join("missing")), None);

        std::fs::remove_dir_all(base).ok();
    }

    #[cfg(unix)]
    #[test]
    fn directory_project_identity_unifies_logical_aliases() {
        let base = std::env::temp_dir().join(format!(
            "moraine-directory-project-alias-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time after unix epoch")
                .as_nanos()
        ));
        let physical_root = base.join("physical");
        let logical_root = base.join("logical");
        std::fs::create_dir_all(&physical_root).expect("create physical directory project");
        std::os::unix::fs::symlink(&physical_root, &logical_root)
            .expect("create logical directory alias");

        assert_eq!(
            project_id_for_repo_root(&physical_root),
            project_id_for_repo_root(&logical_root)
        );
        let roots =
            worktree_roots_for_repo_root(&logical_root).expect("logical directory project roots");
        assert!(roots.contains(&logical_root.to_string_lossy().to_string()));
        assert!(roots.contains(
            &physical_root
                .canonicalize()
                .expect("canonical physical directory")
                .to_string_lossy()
                .to_string()
        ));

        std::fs::remove_dir_all(base).ok();
    }

    #[cfg(unix)]
    #[test]
    fn worktree_roots_include_logical_launch_alias() {
        let base = std::env::temp_dir().join(format!(
            "moraine-project-root-alias-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time after unix epoch")
                .as_nanos()
        ));
        let physical_root = base.join("physical/repo");
        let logical_root = base.join("logical-repo");
        std::fs::create_dir_all(physical_root.join(".git")).expect("create physical repository");
        std::fs::create_dir_all(physical_root.join("nested")).expect("create nested launch dir");
        std::os::unix::fs::symlink(&physical_root, &logical_root)
            .expect("create logical repository alias");

        let roots = worktree_roots_for_repo_root_with_pwd(
            &physical_root,
            Some(&logical_root.join("nested")),
        )
        .expect("registered worktree roots");

        assert!(roots.contains(&logical_root.to_string_lossy().to_string()));
        assert!(roots.contains(
            &physical_root
                .canonicalize()
                .expect("canonical physical root")
                .to_string_lossy()
                .to_string()
        ));
        std::fs::remove_dir_all(base).ok();
    }

    #[cfg(unix)]
    #[test]
    fn project_id_hashes_non_utf8_paths_losslessly() {
        use std::os::unix::ffi::OsStringExt;

        let first = PathBuf::from(std::ffi::OsString::from_vec(b"/repo-\x80/.git".to_vec()));
        let second = PathBuf::from(std::ffi::OsString::from_vec(b"/repo-\x81/.git".to_vec()));
        assert_ne!(
            project_id_for_common_dir(&first),
            project_id_for_common_dir(&second)
        );
    }

    #[test]
    fn redaction_defaults_to_enabled() {
        let path = write_temp_config("", "redaction-defaults");
        let cfg = load_config(&path).expect("empty config should load with defaults");

        assert!(!cfg.redaction.dangerously_skip_secret_redaction);
        assert_eq!(cfg.redaction.ruleset, "builtin");
        assert!(cfg.redaction.extra_patterns.is_empty());
        assert!(!cfg.redaction.dangerously_skip_secret_redaction_ignored);

        std::fs::remove_file(path).ok();
    }

    #[test]
    fn redaction_skip_is_honored_from_home_config_path() {
        let path = write_temp_config(
            r#"
[redaction]
dangerously_skip_secret_redaction = true
"#,
            "redaction-home-skip",
        );
        let cfg = load_config_with_home_path(&path, Some(path.clone()))
            .expect("home config skip should load");

        assert!(cfg.redaction.dangerously_skip_secret_redaction);
        assert!(!cfg.redaction.dangerously_skip_secret_redaction_ignored);

        std::fs::remove_file(path).ok();
    }

    #[test]
    fn redaction_skip_is_ignored_outside_home_config_path() {
        let path = write_temp_config(
            r#"
[redaction]
dangerously_skip_secret_redaction = true
"#,
            "redaction-repo-skip",
        );
        let home = std::env::temp_dir().join("moraine-config-home-does-not-match.toml");
        let cfg = load_config_with_home_path(&path, Some(home))
            .expect("non-home config skip should be ignored");

        assert!(!cfg.redaction.dangerously_skip_secret_redaction);
        assert!(cfg.redaction.dangerously_skip_secret_redaction_ignored);

        std::fs::remove_file(path).ok();
    }

    #[test]
    fn redaction_rejects_allowlist_policy_key() {
        let path = write_temp_config(
            r#"
[redaction]
allowlist = ["AKIAEXAMPLEEXAMPLE12"]
"#,
            "redaction-allowlist",
        );
        let err = load_config(&path).expect_err("unknown redaction keys should fail");

        assert!(
            format!("{err:#}").contains("unknown field `allowlist`"),
            "error names unknown redaction key: {err:#}"
        );

        std::fs::remove_file(path).ok();
    }

    #[test]
    fn redaction_rejects_unknown_ruleset() {
        let path = write_temp_config(
            r#"
[redaction]
ruleset = "custom"
"#,
            "redaction-ruleset",
        );
        let err = load_config(&path).expect_err("unknown ruleset should fail");

        assert!(
            format!("{err:#}").contains("invalid redaction.ruleset"),
            "error names redaction ruleset: {err:#}"
        );

        std::fs::remove_file(path).ok();
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
        assert_eq!(cfg.identity.author, "");
        assert!(!cfg.mcp.prewarm_on_initialize);
        assert!(!cfg.ingest.sources.is_empty());
    }

    #[test]
    fn clickhouse_request_compression_defaults_to_none() {
        let path = write_temp_config("", "clickhouse-request-compression-default");
        let cfg = load_config(&path).expect("empty config should load with defaults");
        std::fs::remove_file(&path).ok();

        assert_eq!(
            cfg.clickhouse.request_compression,
            ClickHouseRequestCompression::None
        );
        assert_eq!(
            cfg.backends[DEFAULT_BACKEND_NAME].request_compression,
            ClickHouseRequestCompression::None
        );
    }

    #[test]
    fn clickhouse_request_compression_parses_gzip() {
        let path = write_temp_config(
            r#"
[clickhouse]
request_compression = "gzip"
"#,
            "clickhouse-request-compression-gzip",
        );
        let cfg = load_config(&path).expect("gzip compression should parse");
        std::fs::remove_file(&path).ok();

        assert_eq!(
            cfg.clickhouse.request_compression,
            ClickHouseRequestCompression::Gzip
        );
        assert_eq!(
            cfg.backends[DEFAULT_BACKEND_NAME].request_compression,
            ClickHouseRequestCompression::Gzip
        );
    }

    #[test]
    fn clickhouse_request_compression_rejects_unknown_values() {
        let path = write_temp_config(
            r#"
[clickhouse]
request_compression = "brotli"
"#,
            "clickhouse-request-compression-invalid",
        );
        let err = load_config(&path).expect_err("unknown compression should fail");
        std::fs::remove_file(&path).ok();

        assert!(
            format!("{err:#}").contains("unknown variant `brotli`"),
            "unexpected error: {err:#}"
        );
    }

    #[test]
    fn identity_author_defaults_to_empty() {
        let path = write_temp_config("", "identity-defaults");
        let cfg = load_config(&path).expect("empty config should load with defaults");
        std::fs::remove_file(&path).ok();

        assert_eq!(cfg.identity.author, "");
    }

    #[test]
    fn identity_author_parses_and_trims() {
        let path = write_temp_config(
            r#"
[identity]
author = "  alice@example.com  "
"#,
            "identity-author",
        );
        let cfg = load_config(&path).expect("identity config should load");
        std::fs::remove_file(&path).ok();

        assert_eq!(cfg.identity.author, "alice@example.com");
    }

    #[test]
    fn identity_author_whitespace_only_normalizes_to_empty() {
        let path = write_temp_config(
            r#"
[identity]
author = "    "
"#,
            "identity-author-empty",
        );
        let cfg = load_config(&path).expect("whitespace identity should load");
        std::fs::remove_file(&path).ok();

        assert_eq!(cfg.identity.author, "");
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
    fn mcp_parallel_request_limit_is_optional_and_positive() {
        let default_cfg = McpConfig::default();
        assert_eq!(default_cfg.max_parallel_requests, None);

        let explicit_path = write_temp_config(
            r#"
[mcp]
max_parallel_requests = 12
"#,
            "mcp-parallel-requests",
        );
        let explicit = load_config(&explicit_path).expect("positive limit should load");
        std::fs::remove_file(&explicit_path).ok();
        assert_eq!(explicit.mcp.max_parallel_requests, Some(12));

        let zero_path = write_temp_config(
            r#"
[mcp]
max_parallel_requests = 0
"#,
            "mcp-parallel-requests-zero",
        );
        let error = load_config(&zero_path).expect_err("zero limit must fail");
        std::fs::remove_file(&zero_path).ok();
        assert!(error
            .to_string()
            .contains("mcp.max_parallel_requests must be greater than zero"));
    }

    #[test]
    fn central_mcp_defaults_are_on() {
        let cfg = McpConfig::default();
        assert!(cfg.use_central_server);
        assert!(cfg.start_central_on_up);
        assert_eq!(cfg.central_connect_timeout_ms, 250);
        assert_eq!(cfg.central_socket_path, "mcp.sock");
    }

    fn assert_backend_defaults(backend: &BackendConfig) {
        assert!(backend.start_on_up);
        assert_eq!(backend.bind, "127.0.0.1");
        assert_eq!(backend.auth_token, None);
    }

    #[test]
    fn ingest_ack_observation_is_explicit_and_disabled_by_default() {
        assert!(!AppConfig::default().ingest.ack_observation);
        let omitted: AppConfig =
            toml::from_str("").expect("empty config should deserialize with ack observation off");
        assert!(!omitted.ingest.ack_observation);
        let enabled: AppConfig = toml::from_str("[ingest]\nack_observation = true\n")
            .expect("ack observation key should deserialize");
        assert!(enabled.ingest.ack_observation);
    }

    #[test]
    fn backend_defaults_match_programmatic_deserialized_and_file_configs() {
        assert_backend_defaults(&BackendConfig::default());
        assert_backend_defaults(&AppConfig::default().backend);

        let deserialized_backend: BackendConfig =
            toml::from_str("").expect("empty backend config should deserialize");
        assert_backend_defaults(&deserialized_backend);

        let deserialized_app: AppConfig =
            toml::from_str("").expect("empty app config should deserialize");
        assert_backend_defaults(&deserialized_app.backend);

        let path = write_temp_config("", "backend-file-defaults");
        let loaded = load_config(&path).expect("empty config file should load");
        std::fs::remove_file(path).ok();
        assert_backend_defaults(&loaded.backend);
    }

    #[test]
    fn backend_bind_and_auth_token_parse_exact_values_including_empty_token() {
        let configured: BackendConfig = toml::from_str(
            r#"
start_on_up = true
bind = "0.0.0.0"
auth_token = "  exact token value  "
"#,
        )
        .expect("backend config should deserialize");
        assert!(configured.start_on_up);
        assert_eq!(configured.bind, "0.0.0.0");
        assert_eq!(
            configured.auth_token.as_deref(),
            Some("  exact token value  ")
        );

        let empty_token: BackendConfig =
            toml::from_str("auth_token = \"\"\n").expect("empty token should deserialize");
        assert_eq!(empty_token.auth_token.as_deref(), Some(""));
        assert_eq!(
            format!("{empty_token:?}"),
            r#"BackendConfig { start_on_up: true, bind: "127.0.0.1", auth_token: Some("[REDACTED]") }"#
        );
    }

    #[test]
    fn backend_debug_redacts_auth_token_and_shows_other_fields() {
        let backend = BackendConfig {
            start_on_up: true,
            bind: "0.0.0.0".to_string(),
            auth_token: Some("unique-secret-token".to_string()),
        };

        let debug = format!("{backend:?}");
        assert_eq!(
            debug,
            r#"BackendConfig { start_on_up: true, bind: "0.0.0.0", auth_token: Some("[REDACTED]") }"#
        );
        assert!(!debug.contains("unique-secret-token"));
        let pretty_debug = format!("{backend:#?}");
        assert!(pretty_debug.contains("[REDACTED]"));
        assert!(!pretty_debug.contains("unique-secret-token"));
    }

    #[test]
    fn app_config_debug_uses_redacted_backend_debug() {
        let mut cfg = AppConfig::default();
        cfg.backend.start_on_up = true;
        cfg.backend.bind = "192.0.2.10".to_string();
        cfg.backend.auth_token = Some("app-config-secret-token".to_string());

        let debug = format!("{cfg:?}");
        assert!(debug.contains(
            r#"backend: BackendConfig { start_on_up: true, bind: "192.0.2.10", auth_token: Some("[REDACTED]") }"#
        ));
        assert!(!debug.contains("app-config-secret-token"));
        let pretty_debug = format!("{cfg:#?}");
        assert!(pretty_debug.contains("[REDACTED]"));
        assert!(!pretty_debug.contains("app-config-secret-token"));
    }

    #[test]
    fn explicit_legacy_monitor_host_maps_to_backend_bind() {
        let path = write_temp_config(
            "[backend]\nstart_on_up = true\n\n[monitor]\nhost = \"192.0.2.20\"\n",
            "backend-legacy-monitor-host",
        );
        let cfg = load_config(&path).expect("legacy monitor host should load");
        std::fs::remove_file(path).ok();

        assert_eq!(cfg.monitor.host, "192.0.2.20");
        assert_eq!(cfg.backend.bind, "192.0.2.20");
    }

    #[test]
    fn explicit_backend_bind_wins_over_legacy_monitor_host() {
        let path = write_temp_config(
            "[backend]\nbind = \"0.0.0.0\"\nstart_on_up = true\n\n[monitor]\nhost = \"192.0.2.20\"\n",
            "backend-bind-precedence",
        );
        let cfg = load_config(&path).expect("canonical and legacy bind config should load");
        std::fs::remove_file(path).ok();

        assert_eq!(cfg.monitor.host, "192.0.2.20");
        assert_eq!(cfg.backend.bind, "0.0.0.0");
    }

    #[test]
    fn backend_start_on_up_is_effectively_on_for_missing_and_false_states() {
        assert!(BackendConfig::default().start_on_up);
        assert!(AppConfig::default().backend.start_on_up);

        for (label, contents) in [
            ("backend-empty-config", ""),
            ("backend-empty-table", "[backend]\n"),
            (
                "backend-canonical-false",
                "[backend]\nstart_on_up = false\n",
            ),
            (
                "backend-monitor-false",
                "[runtime]\nstart_monitor_on_up = false\n",
            ),
            (
                "backend-central-false",
                "[mcp]\nstart_central_on_up = false\n",
            ),
            (
                "backend-tertiary-false",
                "[runtime]\nstart_mcp_on_up = false\n",
            ),
            (
                "backend-all-legacy-false",
                "[runtime]\nstart_monitor_on_up = false\nstart_mcp_on_up = false\n\n[mcp]\nstart_central_on_up = false\n",
            ),
        ] {
            assert_backend_start_on_up(label, contents, true);
        }
    }

    #[test]
    fn non_loopback_backend_requires_prior_launch_opt_in() {
        for (label, contents) in [
            (
                "missing-launch-switch",
                "[backend]\nbind = \"0.0.0.0\"\nauth_token = \"guard\"\n",
            ),
            (
                "canonical-false",
                "[backend]\nbind = \"0.0.0.0\"\nauth_token = \"guard\"\nstart_on_up = false\n",
            ),
            (
                "canonical-false-overrides-legacy-true",
                "[backend]\nbind = \"0.0.0.0\"\nauth_token = \"guard\"\nstart_on_up = false\n\n[runtime]\nstart_monitor_on_up = true\n",
            ),
            (
                "legacy-false",
                "[backend]\nbind = \"0.0.0.0\"\nauth_token = \"guard\"\n\n[runtime]\nstart_monitor_on_up = false\n",
            ),
        ] {
            let path = write_temp_config(contents, label);
            let error = load_config(&path).expect_err("non-loopback auto-enable must fail closed");
            std::fs::remove_file(path).ok();
            assert!(
                error.to_string().contains("set backend.start_on_up = true"),
                "{label}: {error:#}"
            );
        }

        for (label, contents) in [
            (
                "canonical-true",
                "[backend]\nbind = \"0.0.0.0\"\nauth_token = \"guard\"\nstart_on_up = true\n",
            ),
            (
                "legacy-true",
                "[backend]\nbind = \"0.0.0.0\"\nauth_token = \"guard\"\n\n[mcp]\nstart_central_on_up = true\n",
            ),
        ] {
            assert_backend_start_on_up(label, contents, true);
        }
    }

    #[test]
    fn backend_start_on_up_ignores_all_primary_legacy_alias_states() {
        for monitor in [false, true] {
            for central in [false, true] {
                let content = format!(
                    "[runtime]\nstart_monitor_on_up = {monitor}\n\n[mcp]\nstart_central_on_up = {central}\n"
                );
                assert_backend_start_on_up(
                    &format!("backend-primary-{monitor}-{central}"),
                    &content,
                    true,
                );
            }
        }
    }

    #[test]
    fn backend_start_on_up_ignores_tertiary_runtime_mcp_alias() {
        for start_mcp in [false, true] {
            let content = format!("[runtime]\nstart_mcp_on_up = {start_mcp}\n");
            assert_backend_start_on_up(&format!("backend-tertiary-{start_mcp}"), &content, true);
        }
    }

    #[test]
    fn explicit_backend_start_on_up_is_accepted_but_effectively_on() {
        for explicit in [false, true] {
            for monitor in [false, true] {
                for central in [false, true] {
                    for start_mcp in [false, true] {
                        let content = format!(
                            "[backend]\nstart_on_up = {explicit}\n\n[runtime]\nstart_monitor_on_up = {monitor}\nstart_mcp_on_up = {start_mcp}\n\n[mcp]\nstart_central_on_up = {central}\n"
                        );
                        assert_backend_start_on_up(
                            &format!(
                                "backend-precedence-{explicit}-{monitor}-{central}-{start_mcp}"
                            ),
                            &content,
                            true,
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn malformed_backend_fields_are_load_errors() {
        for (label, contents) in [
            (
                "backend-string-value",
                "[backend]\nstart_on_up = \"false\"\n",
            ),
            ("backend-integer-value", "[backend]\nstart_on_up = 0\n"),
            ("backend-unknown-field", "[backend]\nstart_on_upp = false\n"),
            ("backend-bind-not-a-string", "[backend]\nbind = 8080\n"),
            (
                "backend-auth-token-not-a-string",
                "[backend]\nauth_token = false\n",
            ),
            ("backend-not-a-table", "backend = false\n"),
        ] {
            let path = write_temp_config(contents, label);
            let result = load_config(&path);
            std::fs::remove_file(&path).ok();
            let error = result.expect_err("malformed backend config must not load");
            assert!(
                format!("{error:#}").contains("failed to parse TOML config"),
                "case `{label}` returned an unexpected error: {error:#}"
            );
        }
    }

    #[test]
    fn malformed_config_errors_do_not_expose_auth_tokens() {
        const SECRET: &str = "issue-462-parse-error-secret";
        for (label, contents) in [
            (
                "backend-secret-neighbor-error",
                format!("[backend]\nauth_token = \"{SECRET}\"\nbind = 8080\n"),
            ),
            (
                "backend-inline-secret-neighbor-error",
                format!("backend = {{ auth_token = \"{SECRET}\", bind = 8080 }}\n"),
            ),
            (
                "backend-secret-syntax-error",
                format!("[backend]\nauth_token = \"{SECRET}\",\n"),
            ),
        ] {
            let path = write_temp_config(&contents, label);
            let error = load_config(&path).expect_err("malformed secret config must fail");
            std::fs::remove_file(path).ok();

            for rendered in [format!("{error:#}"), format!("{error:?}")] {
                assert!(
                    !rendered.contains(SECRET),
                    "config error leaked auth token: {rendered}"
                );
                assert!(rendered.contains("failed to parse TOML config"));
                assert!(rendered.contains("line"));
                assert!(rendered.contains("column"));
            }
        }
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
        assert!(cfg.mcp.async_log_writes);
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
                "expected one of: codex, claude-code, cursor, hermes, kiro-cli, kimi-cli, nac, opencode, pi-coding-agent, qwen-code"
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
                "expected one of: codex, claude-code, cursor, hermes, kiro-cli, kimi-cli, nac, opencode, pi-coding-agent, qwen-code"
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

    #[cfg(target_os = "macos")]
    #[test]
    fn load_config_migrates_only_the_default_claude_source_to_cowork() {
        let default_path = write_temp_config(
            r#"
[[ingest.sources]]
name = "claude"
harness = "claude-code"
enabled = false
glob = "~/.claude/projects/**/*.jsonl"
watch_root = "~/.claude/projects"
format = "jsonl"
"#,
            "legacy-default-claude-source",
        );
        let default_cfg = load_config(&default_path).expect("default Claude config should load");
        std::fs::remove_file(&default_path).ok();
        let cowork = default_cfg
            .ingest
            .sources
            .iter()
            .find(|source| source.name == "claude-cowork")
            .expect("default Claude source should gain Cowork");
        assert!(!cowork.enabled, "Cowork inherits the Claude enabled state");
        assert_eq!(cowork.harness, "claude-code");
        assert!(cowork.glob.ends_with(
            "/Library/Application Support/Claude/local-agent-mode-sessions/**/.claude/projects/**/*.jsonl"
        ));
        assert!(cowork
            .watch_root
            .ends_with("/Library/Application Support/Claude/local-agent-mode-sessions"));

        let custom_path = write_temp_config(
            r#"
[[ingest.sources]]
name = "claude"
harness = "claude-code"
enabled = true
glob = "~/custom/claude/**/*.jsonl"
watch_root = "~/custom/claude"
"#,
            "custom-claude-source",
        );
        let custom_cfg = load_config(&custom_path).expect("custom Claude config should load");
        std::fs::remove_file(&custom_path).ok();
        assert!(custom_cfg
            .ingest
            .sources
            .iter()
            .all(|source| source.name != "claude-cowork"));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn load_config_does_not_duplicate_preexisting_cowork_source() {
        let path = write_temp_config(
            r#"
[[ingest.sources]]
name = "claude"
harness = "claude-code"
glob = "~/.claude/projects/**/*.jsonl"
watch_root = "~/.claude/projects"

[[ingest.sources]]
name = "claude-cowork"
harness = "claude-code"
glob = "~/custom/cowork/**/*.jsonl"
watch_root = "~/custom/cowork"
"#,
            "existing-cowork-source",
        );
        let cfg = load_config(&path).expect("preexisting Cowork config should load");
        std::fs::remove_file(&path).ok();
        let cowork = cfg
            .ingest
            .sources
            .iter()
            .filter(|source| source.name == "claude-cowork")
            .collect::<Vec<_>>();
        assert_eq!(cowork.len(), 1);
        assert!(cowork[0].glob.ends_with("/custom/cowork/**/*.jsonl"));
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
        assert_eq!(source.format, SourceFormat::Jsonl);
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
        assert_eq!(source.format, SourceFormat::Jsonl);
        assert_eq!(source.tracked_extension(), "jsonl");
    }

    #[test]
    fn shipped_template_enables_cursor_sqlite_by_default() {
        let path = write_temp_config(
            include_str!("../../../config/moraine.toml"),
            "shipped-template",
        );
        let cfg = load_config(&path).expect("shipped template must parse");
        std::fs::remove_file(&path).ok();
        let source = cfg
            .ingest
            .sources
            .iter()
            .find(|source| source.name == "cursor-sqlite")
            .expect("template ships a cursor-sqlite source");
        assert!(source.enabled, "cursor_sqlite is default on");
        assert_eq!(source.format, SourceFormat::CursorSqlite);
        assert_eq!(source.harness, "cursor");
    }

    #[test]
    fn shipped_template_enables_kiro_sessions_by_default() {
        let path = write_temp_config(
            include_str!("../../../config/moraine.toml"),
            "shipped-template-kiro",
        );
        let cfg = load_config(&path).expect("shipped template must parse");
        std::fs::remove_file(&path).ok();
        let source = cfg
            .ingest
            .sources
            .iter()
            .find(|source| source.name == "kiro")
            .expect("template ships a Kiro CLI source");
        assert!(source.enabled);
        assert_eq!(source.format, SourceFormat::KiroSession);
        assert_eq!(source.harness, "kiro-cli");
    }

    #[test]
    fn kiro_default_source_paths_follow_kiro_home() {
        let source = IngestSource {
            name: "kiro".to_string(),
            harness: "kiro-cli".to_string(),
            enabled: true,
            glob: "~/.kiro/sessions/cli/*.jsonl".to_string(),
            watch_root: "~/.kiro/sessions/cli".to_string(),
            format: SourceFormat::KiroSession,
        };
        let kiro_home = Path::new("/tmp/custom-kiro-home");

        assert_eq!(
            expand_ingest_source_path(&source, &source.glob, Some(kiro_home)),
            "/tmp/custom-kiro-home/sessions/cli/*.jsonl"
        );
        assert_eq!(
            expand_ingest_source_path(&source, &source.watch_root, Some(kiro_home)),
            "/tmp/custom-kiro-home/sessions/cli"
        );
        assert_eq!(
            expand_ingest_source_path(&source, "/custom/kiro/*.jsonl", Some(kiro_home)),
            "/custom/kiro/*.jsonl",
            "an explicit custom source path must not be rewritten"
        );
    }

    #[test]
    fn shipped_template_uses_canonical_backend_config_without_legacy_keys() {
        let contents = include_str!("../../../config/moraine.toml");
        let raw: toml::Value = toml::from_str(contents).expect("shipped template must be TOML");

        assert_eq!(
            raw_config_bool(&raw, "backend", "start_on_up"),
            Some(true),
            "template must explicitly enable backend launch"
        );
        assert_eq!(
            raw.get("backend")
                .and_then(|backend| backend.get("bind"))
                .and_then(toml::Value::as_str),
            Some("127.0.0.1"),
            "template must explicitly declare the canonical loopback bind"
        );
        assert!(
            raw.get("backend")
                .and_then(|backend| backend.get("auth_token"))
                .is_none(),
            "template must not ship a concrete backend auth token"
        );
        assert!(
            raw.get("monitor")
                .and_then(|monitor| monitor.get("host"))
                .is_none(),
            "template must not ship the legacy monitor host key"
        );
        assert_eq!(
            raw_config_bool(&raw, "mcp", "use_central_server"),
            Some(true),
            "MCP clients should keep using the central socket when available"
        );
        assert!(
            raw.get("mcp")
                .and_then(|mcp| mcp.get("start_central_on_up"))
                .is_none(),
            "template must not ship the obsolete MCP launch key"
        );
        assert!(
            raw.get("runtime")
                .and_then(|runtime| runtime.get("start_monitor_on_up"))
                .is_none(),
            "template must not ship the obsolete monitor launch key"
        );
        assert!(
            raw.get("runtime")
                .and_then(|runtime| runtime.get("start_mcp_on_up"))
                .is_none(),
            "template must not ship the tertiary MCP launch key"
        );

        let path = write_temp_config(contents, "shipped-template-backend-on");
        let cfg = load_config(&path).expect("shipped template must load");
        std::fs::remove_file(&path).ok();
        assert!(cfg.backend.start_on_up);
        assert_eq!(cfg.backend.bind, "127.0.0.1");
        assert_eq!(cfg.backend.auth_token, None);
        assert!(cfg.mcp.use_central_server);
    }

    #[test]
    fn shipped_template_enables_pi_and_omp_sources_by_default() {
        let path = write_temp_config(
            include_str!("../../../config/moraine.toml"),
            "shipped-template-pi-omp",
        );
        let cfg = load_config(&path).expect("shipped template must parse");
        std::fs::remove_file(&path).ok();

        let pi = cfg
            .ingest
            .sources
            .iter()
            .find(|source| source.name == "pi")
            .expect("template retains the historical pi source");
        let omp = cfg
            .ingest
            .sources
            .iter()
            .find(|source| source.name == "omp")
            .expect("template ships an omp source");

        for source in [pi, omp] {
            assert!(source.enabled);
            assert_eq!(source.harness, "pi-coding-agent");
            assert_eq!(source.format, SourceFormat::Jsonl);
        }
        assert!(pi.glob.ends_with("/.pi/agent/sessions/**/*.jsonl"));
        assert!(pi.watch_root.ends_with("/.pi/agent/sessions"));
        assert!(omp.glob.ends_with("/.omp/agent/sessions/**/*.jsonl"));
        assert!(omp.watch_root.ends_with("/.omp/agent/sessions"));
    }

    #[test]
    fn default_sources_gate_cowork_to_macos() {
        let sources = default_sources();
        let cowork = sources.iter().find(|source| source.name == "claude-cowork");
        #[cfg(target_os = "macos")]
        {
            let cowork = cowork.expect("macOS defaults include Claude Cowork");
            assert!(cowork.enabled);
            assert_eq!(cowork.harness, "claude-code");
            assert_eq!(
                cowork.glob,
                "~/Library/Application Support/Claude/local-agent-mode-sessions/**/.claude/projects/**/*.jsonl"
            );
            assert_eq!(
                cowork.watch_root,
                "~/Library/Application Support/Claude/local-agent-mode-sessions"
            );
        }
        #[cfg(not(target_os = "macos"))]
        assert!(cowork.is_none(), "non-macOS defaults must omit Cowork");
    }

    #[test]
    fn default_sources_enable_cursor_sqlite() {
        let sources = default_sources();
        let source = sources
            .iter()
            .find(|source| source.name == "cursor-sqlite")
            .expect("defaults include a cursor-sqlite source");
        assert!(source.enabled, "cursor_sqlite is default on");
        assert_eq!(source.format, SourceFormat::CursorSqlite);
        assert!(source.glob.ends_with("/**/state.vscdb"));
    }

    #[test]
    fn default_sources_cover_pi_and_omp_session_trees() {
        let sources = default_sources();
        let pi = sources
            .iter()
            .find(|source| source.name == "pi")
            .expect("defaults retain the historical pi source");
        let omp = sources
            .iter()
            .find(|source| source.name == "omp")
            .expect("defaults include an omp source");

        for source in [pi, omp] {
            assert!(source.enabled);
            assert_eq!(source.harness, "pi-coding-agent");
            assert_eq!(source.format, SourceFormat::Infer);
        }
        assert_eq!(pi.glob, "~/.pi/agent/sessions/**/*.jsonl");
        assert_eq!(pi.watch_root, "~/.pi/agent/sessions");
        assert_eq!(omp.glob, "~/.omp/agent/sessions/**/*.jsonl");
        assert_eq!(omp.watch_root, "~/.omp/agent/sessions");
    }

    #[test]
    fn default_sources_enable_qwen_code_jsonl() {
        let sources = default_sources();
        let source = sources
            .iter()
            .find(|source| source.name == "qwen-code")
            .expect("defaults include a qwen-code source");
        assert!(source.enabled);
        assert_eq!(source.harness, "qwen-code");
        assert_eq!(source.glob, "~/.qwen/projects/*/chats/*.jsonl");
        assert_eq!(source.watch_root, "~/.qwen/projects");
        assert_eq!(source.format, SourceFormat::Jsonl);
    }

    #[test]
    fn load_config_migrates_only_the_legacy_default_pi_source() {
        let legacy_path = write_temp_config(
            r#"
[[ingest.sources]]
name = "pi"
harness = "pi-coding-agent"
enabled = true
glob = "~/.pi/agent/sessions/**/*.jsonl"
watch_root = "~/.pi/agent/sessions"
format = "jsonl"
"#,
            "legacy-pi-source",
        );
        let legacy = load_config(&legacy_path).expect("legacy Pi config should load");
        std::fs::remove_file(&legacy_path).ok();
        let omp = legacy
            .ingest
            .sources
            .iter()
            .find(|source| source.name == "omp")
            .expect("legacy default Pi config should gain OMP");
        assert!(omp.enabled);
        assert_eq!(omp.harness, "pi-coding-agent");
        assert_eq!(omp.format, SourceFormat::Jsonl);
        assert!(omp.glob.ends_with("/.omp/agent/sessions/**/*.jsonl"));

        let custom_path = write_temp_config(
            r#"
[[ingest.sources]]
name = "pi"
harness = "pi-coding-agent"
enabled = true
glob = "~/custom/pi/**/*.jsonl"
watch_root = "~/custom/pi"
format = "jsonl"
"#,
            "custom-pi-source",
        );
        let custom = load_config(&custom_path).expect("custom Pi config should load");
        std::fs::remove_file(&custom_path).ok();
        assert!(
            custom
                .ingest
                .sources
                .iter()
                .all(|source| source.name != "omp"),
            "custom Pi sources must not opt into OMP implicitly"
        );
    }

    #[test]
    fn default_sources_enable_opencode_sqlite() {
        let sources = default_sources();
        let source = sources
            .iter()
            .find(|source| source.name == "opencode")
            .expect("defaults include an opencode source");
        assert!(source.enabled, "opencode_sqlite is default on");
        assert_eq!(source.harness, "opencode");
        assert_eq!(source.format, SourceFormat::OpenCodeSqlite);
        assert_eq!(source.glob, "~/.local/share/opencode/opencode*.db");
    }

    #[test]
    fn default_sources_enable_kiro_sessions() {
        let sources = default_sources();
        let source = sources
            .iter()
            .find(|source| source.name == "kiro")
            .expect("defaults include a Kiro CLI source");
        assert!(source.enabled);
        assert_eq!(source.harness, "kiro-cli");
        assert_eq!(source.format, SourceFormat::KiroSession);
        assert_eq!(source.glob, "~/.kiro/sessions/cli/*.jsonl");
        assert_eq!(source.watch_root, "~/.kiro/sessions/cli");
    }

    #[test]
    fn load_config_infers_kiro_session_format() {
        let path = write_temp_config(
            r#"
[[ingest.sources]]
name = "kiro"
harness = "kiro-cli"
enabled = true
glob = "~/.kiro/sessions/cli/*.jsonl"
watch_root = "~/.kiro/sessions/cli"
"#,
            "kiro-session-format",
        );
        let cfg = load_config(&path).expect("Kiro CLI source should be accepted");
        std::fs::remove_file(&path).ok();
        let source = cfg
            .ingest
            .sources
            .iter()
            .find(|source| source.name == "kiro")
            .expect("Kiro source");
        assert_eq!(source.format, SourceFormat::KiroSession);
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
        assert_eq!(source.format, SourceFormat::CursorSqlite);
        assert_eq!(source.tracked_extension(), "vscdb");
        assert!(!source.enabled);
    }

    #[test]
    fn load_config_accepts_opencode_sqlite_format() {
        let path = write_temp_config(
            r#"
[[ingest.sources]]
name = "opencode"
harness = "opencode"
enabled = false
glob = "~/.local/share/opencode/opencode*.db"
watch_root = "~/.local/share/opencode"
format = "opencode_sqlite"
"#,
            "opencode-sqlite-format",
        );
        let cfg = load_config(&path).expect("opencode_sqlite format should be accepted");
        std::fs::remove_file(&path).ok();
        let source = cfg
            .ingest
            .sources
            .iter()
            .find(|source| source.name == "opencode")
            .expect("opencode source");
        assert_eq!(source.format, SourceFormat::OpenCodeSqlite);
        assert_eq!(source.tracked_extension(), "db");
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
        let message = format!("{err:#}");
        for expected in [
            "jsonl",
            "session_json",
            "kiro_session",
            "cursor_sqlite",
            "nac_sqlite",
            "opencode_sqlite",
        ] {
            assert!(message.contains(expected), "unexpected error: {message}");
        }
    }

    #[test]
    fn format_path_capabilities_reject_unresolved_and_unknown_formats() {
        assert_eq!(format_tracked_extension(SourceFormat::Infer), None);
        assert_eq!(
            map_tracked_path(SourceFormat::Infer, "", "/tmp/a.jsonl"),
            None
        );
        assert_eq!(map_tracked_path("future_format", "", "/tmp/a.jsonl"), None);

        for (format, extension) in [
            (SourceFormat::Jsonl, "jsonl"),
            (SourceFormat::SessionJson, "json"),
            (SourceFormat::KiroSession, "jsonl"),
            (SourceFormat::CursorSqlite, "vscdb"),
            (SourceFormat::NacSqlite, "db"),
            (SourceFormat::OpenCodeSqlite, "db"),
        ] {
            assert_eq!(format_tracked_extension(format), Some(extension));
        }
    }

    #[test]
    fn map_tracked_path_filters_by_extension_for_file_formats() {
        assert_eq!(
            map_tracked_path(SOURCE_FORMAT_JSONL, "", "/tmp/a.jsonl"),
            Some("/tmp/a.jsonl".to_string())
        );
        assert_eq!(
            map_tracked_path(SOURCE_FORMAT_JSONL, "", "/tmp/a.json"),
            None
        );
        assert_eq!(
            map_tracked_path(SOURCE_FORMAT_SESSION_JSON, "", "/tmp/session_a.json"),
            Some("/tmp/session_a.json".to_string())
        );
        assert_eq!(
            map_tracked_path(SOURCE_FORMAT_SESSION_JSON, "", "/tmp/a.jsonl"),
            None
        );
        assert_eq!(
            map_tracked_path(SOURCE_FORMAT_KIRO_SESSION, "", "/tmp/session.jsonl"),
            Some("/tmp/session.jsonl".to_string())
        );
        assert_eq!(
            map_tracked_path(SOURCE_FORMAT_KIRO_SESSION, "", "/tmp/session.json"),
            Some("/tmp/session.jsonl".to_string())
        );
        assert_eq!(
            map_tracked_path(SOURCE_FORMAT_KIRO_SESSION, "", "/tmp/session.txt"),
            None
        );
    }

    #[test]
    fn map_tracked_path_maps_sqlite_sidecars_to_canonical_db() {
        let cursor = "/tmp/User/globalStorage/state.vscdb";
        assert_eq!(
            map_tracked_path(SOURCE_FORMAT_CURSOR_SQLITE, "", cursor),
            Some(cursor.to_string())
        );
        assert_eq!(
            map_tracked_path(SOURCE_FORMAT_CURSOR_SQLITE, "", "/tmp/User/state.vscdb-wal"),
            Some("/tmp/User/state.vscdb".to_string())
        );
        assert_eq!(
            map_tracked_path(SOURCE_FORMAT_CURSOR_SQLITE, "", "/tmp/User/state.vscdb-shm"),
            Some("/tmp/User/state.vscdb".to_string())
        );
        assert_eq!(
            map_tracked_path(
                SOURCE_FORMAT_CURSOR_SQLITE,
                "",
                "/tmp/User/state.vscdb.backup"
            ),
            None
        );
        assert_eq!(
            map_tracked_path(SOURCE_FORMAT_CURSOR_SQLITE, "", "/tmp/User/state.db-wal"),
            None
        );
        assert_eq!(
            map_tracked_path(SOURCE_FORMAT_CURSOR_SQLITE, "", "/tmp/User/notes.jsonl"),
            None
        );

        let nac = "/tmp/nac/[workspace]*/store?.db";
        let nac_glob = glob::Pattern::escape(nac);
        assert_eq!(
            map_tracked_path(SOURCE_FORMAT_NAC_SQLITE, &nac_glob, nac),
            Some(nac.to_string())
        );
        assert_eq!(
            map_tracked_path(SOURCE_FORMAT_NAC_SQLITE, &nac_glob, &format!("{nac}-wal")),
            Some(nac.to_string())
        );
        assert_eq!(
            map_tracked_path(
                SOURCE_FORMAT_NAC_SQLITE,
                &nac_glob,
                "/tmp/nac/other/store.db"
            ),
            None
        );

        let opencode = "/tmp/opencode/opencode.db";
        assert_eq!(
            map_tracked_path(SOURCE_FORMAT_OPENCODE_SQLITE, "", opencode),
            Some(opencode.to_string())
        );
        assert_eq!(
            map_tracked_path(
                SOURCE_FORMAT_OPENCODE_SQLITE,
                "",
                "/tmp/opencode/opencode-local.db"
            ),
            Some("/tmp/opencode/opencode-local.db".to_string())
        );
        assert_eq!(
            map_tracked_path(
                SOURCE_FORMAT_OPENCODE_SQLITE,
                "",
                "/tmp/opencode/opencode.db-wal"
            ),
            Some("/tmp/opencode/opencode.db".to_string())
        );
        assert_eq!(
            map_tracked_path(
                SOURCE_FORMAT_OPENCODE_SQLITE,
                "",
                "/tmp/opencode/opencode.db-shm"
            ),
            Some("/tmp/opencode/opencode.db".to_string())
        );
        assert_eq!(
            map_tracked_path(
                SOURCE_FORMAT_OPENCODE_SQLITE,
                "",
                "/tmp/opencode/unrelated.db"
            ),
            None
        );
        assert_eq!(
            map_tracked_path(
                SOURCE_FORMAT_OPENCODE_SQLITE,
                "",
                "/tmp/opencode/unrelated.db-wal"
            ),
            None
        );
    }

    #[test]
    fn workflow_journals_are_excluded_but_real_sessions_and_subagents_are_not() {
        let project = "/Users/x/.claude/projects/-Users-x-src-moraine";
        let session = "7e74512d-612b-4406-ae5e-069e73d7f2dc";

        // The orphan journals: only these get excluded.
        assert!(is_workflow_journal_path(&format!(
            "{project}/{session}/subagents/workflows/wf_12dc2994-7e9/journal.jsonl"
        )));
        // Relative paths and trailing-component variants still match.
        assert!(is_workflow_journal_path(
            "subagents/workflows/wf_abc/journal.jsonl"
        ));

        // Workflow subagent transcripts carry a sessionId — keep them.
        assert!(!is_workflow_journal_path(&format!(
            "{project}/{session}/subagents/workflows/wf_8dc1b543-8da/agent-a38ca143465605620.jsonl"
        )));
        // Task-subagent transcripts (no `workflows` segment) — keep them.
        assert!(!is_workflow_journal_path(&format!(
            "{project}/{session}/subagents/agent-a5a524a7f876aa747.jsonl"
        )));
        // The real top-level session transcript — keep it.
        assert!(!is_workflow_journal_path(&format!(
            "{project}/{session}.jsonl"
        )));
        // A `journal.jsonl` that is NOT under `subagents/workflows/` must not
        // be swept up by the filename alone.
        assert!(!is_workflow_journal_path(&format!(
            "{project}/{session}/journal.jsonl"
        )));
        assert!(!is_workflow_journal_path("/tmp/workflows/journal.jsonl"));
        assert!(!is_workflow_journal_path("/tmp/subagents/journal.jsonl"));
    }

    fn make_temp_dir(label: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "moraine-config-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time after unix epoch")
                .as_nanos()
        ));
        std::fs::create_dir_all(&path).expect("create temp dir");
        path
    }

    #[test]
    fn backends_default_synthesized_from_clickhouse_block() {
        let path = write_temp_config(
            r#"
[clickhouse]
url = "http://127.0.0.1:9999"
database = "custom"
"#,
            "backends-synthesized",
        );
        let cfg = load_config(&path).expect("legacy [clickhouse] shape should load");
        std::fs::remove_file(&path).ok();
        let default_backend = cfg
            .backends
            .get(DEFAULT_BACKEND_NAME)
            .expect("default backend synthesized from [clickhouse]");
        assert_eq!(default_backend.url, "http://127.0.0.1:9999");
        assert_eq!(default_backend.database, "custom");
        assert_eq!(default_backend.url, cfg.clickhouse.url);
        assert!(!default_backend.allow_newer_server);
        assert_eq!(cfg.backends.len(), 1);
    }

    #[test]
    fn explicit_backends_default_back_fills_clickhouse() {
        let path = write_temp_config(
            r#"
[backends.default]
url = "http://10.0.0.1:8123"
database = "moraine"
"#,
            "backends-explicit-default",
        );
        let cfg = load_config(&path).expect("[backends.default] shape should load");
        std::fs::remove_file(&path).ok();
        // Existing call sites read cfg.clickhouse; it must mirror the
        // explicit default backend.
        assert_eq!(cfg.clickhouse.url, "http://10.0.0.1:8123");
        assert_eq!(
            cfg.backends[DEFAULT_BACKEND_NAME].url,
            "http://10.0.0.1:8123"
        );
    }

    #[test]
    fn load_config_errors_when_clickhouse_and_backends_default_both_declared() {
        let path = write_temp_config(
            r#"
[clickhouse]
url = "http://127.0.0.1:8123"

[backends.default]
url = "http://10.0.0.1:8123"
"#,
            "backends-ambiguous-default",
        );
        let err = load_config(&path).expect_err("declaring both aliases should fail");
        std::fs::remove_file(&path).ok();
        assert!(
            format!("{err:#}").contains("both [clickhouse] and [backends.default]"),
            "unexpected error: {err:#}"
        );
    }

    #[test]
    fn clickhouse_block_plus_named_backends_coexist_in_sorted_order() {
        let path = write_temp_config(
            r#"
[clickhouse]
url = "http://127.0.0.1:9999"

[backends.team-ch]
url = "https://ch.team.example:8443"
database = "moraine_team"
allow_newer_server = true

[backends.alpha]
url = "http://alpha.example:8123"
"#,
            "backends-named",
        );
        let cfg = load_config(&path).expect("[clickhouse] + named backends should load");
        std::fs::remove_file(&path).ok();
        // BTreeMap iteration order is deterministic for logs/tests.
        let names: Vec<&str> = cfg.backends.keys().map(String::as_str).collect();
        assert_eq!(names, vec!["alpha", "default", "team-ch"]);
        assert_eq!(cfg.backends["default"].url, "http://127.0.0.1:9999");
        assert!(cfg.backends["team-ch"].allow_newer_server);
        assert!(!cfg.backends["alpha"].allow_newer_server);
        assert!(!cfg.backends["default"].allow_newer_server);
    }

    #[test]
    fn app_config_default_upholds_default_backend_invariant() {
        let cfg = AppConfig::default();
        let default_backend = cfg
            .backends
            .get(DEFAULT_BACKEND_NAME)
            .expect("default backend present on AppConfig::default()");
        assert_eq!(default_backend.url, cfg.clickhouse.url);
        assert!(cfg.routes.is_empty());
    }

    #[test]
    fn routes_unknown_backend_is_load_error() {
        let path = write_temp_config(
            r#"
[backends.team]
url = "http://team.example:8123"

[[routes]]
dir = "~/src/teamproject/**"
backend = "tema"
"#,
            "routes-unknown-backend",
        );
        let err = load_config(&path).expect_err("unknown route backend should fail");
        std::fs::remove_file(&path).ok();
        let message = format!("{err:#}");
        assert!(
            message.contains("routes[0].backend `tema` is not a configured backend"),
            "unexpected error: {message}"
        );
        assert!(
            message.contains("default, team"),
            "error should list configured backends: {message}"
        );
    }

    #[test]
    fn routes_reject_non_mirror_mode() {
        let path = write_temp_config(
            r#"
[backends.team]
url = "http://team.example:8123"

[[routes]]
dir = "~/src/teamproject/**"
backend = "team"
mode = "exclusive"
"#,
            "routes-exclusive-mode",
        );
        let err = load_config(&path).expect_err("exclusive mode should fail");
        std::fs::remove_file(&path).ok();
        assert!(
            format!("{err:#}").contains("`exclusive` is not implemented yet"),
            "unexpected error: {err:#}"
        );
    }

    #[test]
    fn routes_mode_defaults_to_mirror_and_normalizes_case() {
        let path = write_temp_config(
            r#"
[backends.team]
url = "http://team.example:8123"

[[routes]]
dir = "~/src/a/**"
backend = "team"

[[routes]]
dir = "~/src/b/**"
backend = "team"
mode = "Mirror"
"#,
            "routes-default-mode",
        );
        let cfg = load_config(&path).expect("routes with default mode should load");
        std::fs::remove_file(&path).ok();
        assert_eq!(cfg.routes[0].mode, ROUTE_MODE_MIRROR);
        assert_eq!(cfg.routes[1].mode, ROUTE_MODE_MIRROR);
    }

    #[test]
    fn routes_reject_unknown_keys_and_missing_fields() {
        let unknown_key = write_temp_config(
            r#"
[backends.team]
url = "http://team.example:8123"

[[routes]]
dir = "~/src/a/**"
backend = "team"
extra = "nope"
"#,
            "routes-unknown-key",
        );
        let err = load_config(&unknown_key).expect_err("unknown route key should fail");
        std::fs::remove_file(&unknown_key).ok();
        assert!(
            format!("{err:#}").contains("unknown field `extra`"),
            "unexpected error: {err:#}"
        );

        let missing_dir = write_temp_config(
            r#"
[backends.team]
url = "http://team.example:8123"

[[routes]]
backend = "team"
"#,
            "routes-missing-dir",
        );
        let err = load_config(&missing_dir).expect_err("missing route dir should fail");
        std::fs::remove_file(&missing_dir).ok();
        assert!(
            format!("{err:#}").contains("missing field `dir`"),
            "unexpected error: {err:#}"
        );
    }

    #[test]
    fn routes_reject_invalid_dir_glob() {
        let path = write_temp_config(
            r#"
[backends.team]
url = "http://team.example:8123"

[[routes]]
dir = "/work/a**"
backend = "team"
"#,
            "routes-invalid-glob",
        );
        let err = load_config(&path).expect_err("invalid route glob should fail");
        std::fs::remove_file(&path).ok();
        assert!(
            format!("{err:#}").contains("invalid routes[0].dir glob"),
            "unexpected error: {err:#}"
        );
    }

    #[test]
    fn route_for_dir_expands_tilde_and_matches_project_root_and_descendants() {
        let home = std::env::var("HOME").expect("HOME set in test env");
        let path = write_temp_config(
            r#"
[backends.team]
url = "http://team.example:8123"

[[routes]]
dir = "~/src/teamproject/**"
backend = "team"
"#,
            "route-match-tilde",
        );
        let cfg = load_config(&path).expect("route config should load");
        std::fs::remove_file(&path).ok();
        assert_eq!(cfg.routes[0].dir, format!("{home}/src/teamproject/**"));
        // The project root itself, nested dirs, and trailing slashes match.
        for dir in [
            format!("{home}/src/teamproject"),
            format!("{home}/src/teamproject/"),
            format!("{home}/src/teamproject/sub/dir"),
        ] {
            let route = cfg.route_for_dir(&dir);
            assert_eq!(
                route.map(|r| r.backend.as_str()),
                Some("team"),
                "expected match for {dir}"
            );
        }
        assert!(cfg.route_for_dir(&format!("{home}/src/other")).is_none());
        assert!(cfg
            .route_for_dir(&format!("{home}/src/teamproject2"))
            .is_none());
        assert!(cfg.route_for_dir("").is_none());
    }

    #[test]
    fn ingest_exclude_project_dirs_expand_tilde_and_match_route_semantics() {
        let home = std::env::var("HOME").expect("HOME set in test env");
        let path = write_temp_config(
            r#"
[ingest]
exclude_project_dirs = ["~/src/large-project/**"]
"#,
            "ingest-exclude-project-dirs",
        );
        let cfg = load_config(&path).expect("ingest exclusions should load");
        std::fs::remove_file(&path).ok();

        assert_eq!(
            cfg.ingest.exclude_project_dirs,
            vec![format!("{home}/src/large-project/**")]
        );
        assert!(cfg.is_project_dir_excluded(&format!("{home}/src/large-project")));
        assert!(cfg.is_project_dir_excluded(&format!("{home}/src/large-project/sub/dir")));
        assert!(!cfg.is_project_dir_excluded(&format!("{home}/src/large-project2")));
        assert!(!cfg.is_project_dir_excluded(""));
    }

    #[test]
    fn ingest_exclude_project_dirs_reject_invalid_globs() {
        let path = write_temp_config(
            r#"
[ingest]
exclude_project_dirs = ["/work/a**"]
"#,
            "ingest-exclude-project-dirs-invalid",
        );
        let err = load_config(&path).expect_err("invalid ingest exclusion should fail");
        std::fs::remove_file(&path).ok();
        assert!(
            format!("{err:#}").contains("invalid ingest.exclude_project_dirs[0] glob"),
            "unexpected error: {err:#}"
        );
    }

    #[test]
    fn route_for_dir_first_match_wins() {
        let mut cfg = AppConfig::default();
        cfg.backends
            .insert("a".to_string(), ClickHouseConfig::default());
        cfg.backends
            .insert("b".to_string(), ClickHouseConfig::default());
        cfg.routes = vec![
            RouteConfig {
                dir: "/work/**".to_string(),
                backend: "a".to_string(),
                mode: ROUTE_MODE_MIRROR.to_string(),
            },
            RouteConfig {
                dir: "/work/proj/**".to_string(),
                backend: "b".to_string(),
                mode: ROUTE_MODE_MIRROR.to_string(),
            },
        ];
        let route = cfg.route_for_dir("/work/proj/x").expect("route matches");
        assert_eq!(route.backend, "a");
    }

    #[test]
    fn route_for_dir_single_star_stays_within_one_component() {
        let mut cfg = AppConfig::default();
        cfg.backends
            .insert("a".to_string(), ClickHouseConfig::default());
        cfg.routes = vec![RouteConfig {
            dir: "/work/*".to_string(),
            backend: "a".to_string(),
            mode: ROUTE_MODE_MIRROR.to_string(),
        }];
        assert!(cfg.route_for_dir("/work/proj").is_some());
        assert!(cfg.route_for_dir("/work/proj/sub").is_none());
        // Without a trailing `/**` the base dir itself is not matched.
        assert!(cfg.route_for_dir("/work").is_none());
    }

    #[test]
    fn repo_backend_ref_found_in_ancestor_and_nearest_wins() {
        let root = make_temp_dir("repo-ref-nearest");
        let nested = root.join("repo/sub/dir");
        std::fs::create_dir_all(&nested).expect("create nested dirs");
        std::fs::write(
            root.join(REPO_BACKEND_FILE),
            "backend = \"outer\"\nunknown_key = true\n",
        )
        .expect("write outer ref");

        // Ancestor file resolves through intermediate dirs without one.
        assert_eq!(
            find_repo_backend_ref_bounded(&nested, Some(&root)),
            Some("outer".to_string())
        );

        // A nearer file shadows the ancestor.
        std::fs::write(
            root.join("repo").join(REPO_BACKEND_FILE),
            "backend = \"inner\"\n",
        )
        .expect("write inner ref");
        assert_eq!(
            find_repo_backend_ref_bounded(&nested, Some(&root)),
            Some("inner".to_string())
        );

        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn repo_backend_ref_stops_at_boundary_inclusive() {
        let root = make_temp_dir("repo-ref-boundary");
        let home = root.join("home");
        let nested = home.join("src/project");
        std::fs::create_dir_all(&nested).expect("create nested dirs");
        // A file ABOVE the stop dir must never be consulted.
        std::fs::write(root.join(REPO_BACKEND_FILE), "backend = \"escaped\"\n")
            .expect("write outer ref");
        assert_eq!(find_repo_backend_ref_bounded(&nested, Some(&home)), None);

        // A file AT the stop dir is still consulted (stop is inclusive).
        std::fs::write(home.join(REPO_BACKEND_FILE), "backend = \"athome\"\n")
            .expect("write home ref");
        assert_eq!(
            find_repo_backend_ref_bounded(&nested, Some(&home)),
            Some("athome".to_string())
        );

        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn repo_backend_ref_nearest_file_ends_walk_even_without_name() {
        let root = make_temp_dir("repo-ref-ends-walk");
        let nested = root.join("repo");
        std::fs::create_dir_all(&nested).expect("create nested dir");
        std::fs::write(root.join(REPO_BACKEND_FILE), "backend = \"outer\"\n")
            .expect("write outer ref");

        // Nearest file has no backend key: walk ends there with no name.
        std::fs::write(nested.join(REPO_BACKEND_FILE), "other = 1\n").expect("write keyless ref");
        assert_eq!(find_repo_backend_ref_bounded(&nested, Some(&root)), None);

        // Malformed TOML and blank names are treated the same way.
        std::fs::write(nested.join(REPO_BACKEND_FILE), "backend = [broken\n")
            .expect("write malformed ref");
        assert_eq!(find_repo_backend_ref_bounded(&nested, Some(&root)), None);
        std::fs::write(nested.join(REPO_BACKEND_FILE), "backend = \"  \"\n")
            .expect("write blank ref");
        assert_eq!(find_repo_backend_ref_bounded(&nested, Some(&root)), None);

        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn repo_backend_ref_public_helper_walks_up_from_start_dir() {
        // temp_dir is outside $HOME on macOS and Linux CI, so the public
        // helper's walk terminates via the filesystem-root bound; the file
        // is planted close enough that the search never escapes the tempdir.
        let root = make_temp_dir("repo-ref-public");
        let nested = root.join("a/b");
        std::fs::create_dir_all(&nested).expect("create nested dirs");
        std::fs::write(root.join(REPO_BACKEND_FILE), "backend = \"team-ch\"\n").expect("write ref");
        assert_eq!(find_repo_backend_ref(&nested), Some("team-ch".to_string()));
        std::fs::remove_dir_all(&root).ok();
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
        assert_eq!(source.format, SourceFormat::Jsonl);
        assert_eq!(source.tracked_extension(), "jsonl");
    }

    #[test]
    fn query_budgets_absent_section_yields_documented_defaults() {
        let path = write_temp_config("[identity]\nauthor = \"\"\n", "query-budgets-absent");
        let cfg = load_config(&path).expect("config without [query_budgets] should load");
        std::fs::remove_file(&path).ok();
        assert_eq!(cfg.query_budgets, QueryBudgetsConfig::default());

        let validated = ValidatedQueryBudgets::from_config(&cfg.query_budgets)
            .expect("default budgets must validate");
        assert_eq!(validated.interactive.deadline_seconds(), 15.0);
        assert_eq!(
            validated.interactive.deadline(),
            std::time::Duration::from_secs(15)
        );
        assert_eq!(validated.interactive.statement_cap(), 256);
        assert_eq!(validated.background.deadline_seconds(), 600.0);
        assert_eq!(validated.migration.memory_bytes(), 4 * BYTES_PER_GIB);
        assert_eq!(validated.administrative.deadline_seconds(), 5.0);
        assert_eq!(validated.administrative.statement_cap(), 4);
        assert_eq!(validated.export.deadline_seconds(), 600.0);
    }

    #[test]
    fn query_budgets_partial_table_overrides_only_named_fields() {
        let path = write_temp_config(
            r#"
[query_budgets.interactive]
deadline_seconds = 30.0

[query_budgets.background]
statement_cap = 64
"#,
            "query-budgets-partial",
        );
        let cfg = load_config(&path).expect("partial [query_budgets] should load");
        std::fs::remove_file(&path).ok();
        assert_eq!(cfg.query_budgets.interactive.deadline_seconds, 30.0);
        assert_eq!(cfg.query_budgets.interactive.statement_cap, 256);
        assert_eq!(cfg.query_budgets.interactive.memory_bytes, BYTES_PER_GIB);
        assert_eq!(cfg.query_budgets.background.statement_cap, 64);
        assert_eq!(cfg.query_budgets.background.deadline_seconds, 600.0);
        assert_eq!(
            cfg.query_budgets.migration,
            QueryBudgetsConfig::default().migration
        );
        assert_eq!(
            cfg.query_budgets.export,
            QueryBudgetsConfig::default().export
        );
    }

    #[test]
    fn query_budgets_zero_values_fail_config_load() {
        for (label, contents, expected) in [
            (
                "cap",
                "[query_budgets.interactive]\nstatement_cap = 0\n",
                "query_budgets.interactive.statement_cap",
            ),
            (
                "memory",
                "[query_budgets.background]\nmemory_bytes = 0\n",
                "query_budgets.background.memory_bytes",
            ),
            (
                "spill",
                "[query_budgets.migration]\nspill_bytes = 0\n",
                "query_budgets.migration.spill_bytes",
            ),
            (
                "rows",
                "[query_budgets.export]\nread_rows = 0\n",
                "query_budgets.export.read_rows",
            ),
            (
                "bytes",
                "[query_budgets.administrative]\nread_bytes = 0\n",
                "query_budgets.administrative.read_bytes",
            ),
            (
                "deadline",
                "[query_budgets.interactive]\ndeadline_seconds = 0.0\n",
                "query_budgets.interactive.deadline_seconds",
            ),
        ] {
            let path = write_temp_config(contents, &format!("query-budgets-zero-{label}"));
            let error = load_config(&path).expect_err("zero budget must fail closed");
            std::fs::remove_file(&path).ok();
            assert!(
                error.to_string().contains(expected),
                "case `{label}`: {error}"
            );
        }
    }

    #[test]
    fn query_budgets_negative_and_unknown_inputs_fail_config_load() {
        for (label, contents) in [
            (
                "negative-rows",
                "[query_budgets.interactive]\nread_rows = -1\n",
            ),
            (
                "unknown-field",
                "[query_budgets.interactive]\ndeadline = 5.0\n",
            ),
            ("unknown-class", "[query_budgets.turbo]\nread_rows = 1\n"),
        ] {
            let path = write_temp_config(contents, &format!("query-budgets-reject-{label}"));
            let error = load_config(&path).expect_err("malformed budget must fail closed");
            std::fs::remove_file(&path).ok();
            assert!(!error.to_string().is_empty(), "case `{label}`");
        }
    }

    #[test]
    fn query_budget_validation_deadline_edges() {
        let mut cfg = QueryBudgetsConfig::default();

        cfg.interactive.deadline_seconds = -1.0;
        assert_eq!(
            ValidatedQueryBudgets::from_config(&cfg),
            Err(QueryBudgetsValidationError::DeadlineNotPositive {
                class: "interactive",
                value: -1.0,
            })
        );

        cfg.interactive.deadline_seconds = f64::NAN;
        assert!(matches!(
            ValidatedQueryBudgets::from_config(&cfg),
            Err(QueryBudgetsValidationError::DeadlineNotFinite {
                class: "interactive"
            })
        ));

        cfg.interactive.deadline_seconds = f64::INFINITY;
        assert_eq!(
            ValidatedQueryBudgets::from_config(&cfg),
            Err(QueryBudgetsValidationError::DeadlineNotFinite {
                class: "interactive"
            })
        );

        cfg.interactive.deadline_seconds = QUERY_DEADLINE_MAX_SECONDS + 0.1;
        assert_eq!(
            ValidatedQueryBudgets::from_config(&cfg),
            Err(QueryBudgetsValidationError::DeadlineAboveMax {
                class: "interactive",
                value: QUERY_DEADLINE_MAX_SECONDS + 0.1,
            })
        );

        // Boundary acceptance: exactly 24h and exactly one statement.
        cfg.interactive.deadline_seconds = QUERY_DEADLINE_MAX_SECONDS;
        cfg.interactive.statement_cap = 1;
        ValidatedQueryBudgets::from_config(&cfg).expect("24h deadline and cap 1 are valid");
    }

    #[test]
    fn query_budget_validation_enforces_memory_backstop() {
        let mut cfg = QueryBudgetsConfig::default();

        // The migration default sits exactly at the shipped backstop.
        cfg.migration.memory_bytes = QUERY_MEMORY_BACKSTOP_BYTES;
        ValidatedQueryBudgets::from_config(&cfg).expect("backstop-equal memory is valid");

        cfg.migration.memory_bytes = QUERY_MEMORY_BACKSTOP_BYTES + 1;
        assert_eq!(
            ValidatedQueryBudgets::from_config(&cfg),
            Err(QueryBudgetsValidationError::MemoryAboveBackstop {
                class: "migration",
                value: QUERY_MEMORY_BACKSTOP_BYTES + 1,
                backstop: QUERY_MEMORY_BACKSTOP_BYTES,
            })
        );

        // A stricter deployment backstop rejects the shipped defaults too.
        assert_eq!(
            ValidatedQueryBudgets::with_memory_backstop(
                &QueryBudgetsConfig::default(),
                512 * BYTES_PER_MIB,
            ),
            Err(QueryBudgetsValidationError::MemoryAboveBackstop {
                class: "interactive",
                value: BYTES_PER_GIB,
                backstop: 512 * BYTES_PER_MIB,
            })
        );
    }

    #[test]
    fn shipped_template_query_budgets_match_code_defaults() {
        let path = write_temp_config(
            include_str!("../../../config/moraine.toml"),
            "shipped-template-query-budgets",
        );
        let cfg = load_config(&path).expect("shipped template must parse");
        std::fs::remove_file(&path).ok();
        assert_eq!(
            cfg.query_budgets,
            QueryBudgetsConfig::default(),
            "template [query_budgets] values must not drift from code defaults"
        );
    }
}
