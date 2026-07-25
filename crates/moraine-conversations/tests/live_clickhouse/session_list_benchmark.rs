//! Issue #599 WI-07 boundedness gate (plan §5.3): prove on a real ClickHouse
//! that session DISCOVERY costs scale with scalar directory state and the
//! requested page size — never with transcript content.
//!
//! Registered as the `list-bench` `run-live-test` mode; the root
//! `live_clickhouse.rs` owns the `#[tokio::test]` wrapper.
//!
//! Two operational lessons are inherited verbatim from the #598 benchmark
//! (`canonical_open_benchmark.rs`) and are load-bearing here:
//!
//! **Lesson 1 — consolidate before measuring.** Each batched seed INSERT
//! flushes a new part, and a pruned read pays a boundary-granule floor
//! (~8192 rows) in EVERY part that could contain its key until background
//! merges consolidate. [`consolidate`] runs `OPTIMIZE … FINAL` over `events`
//! and the three migration-036 tables before every measurement BLOCK; no
//! INSERT runs between the phases inside a block, so one consolidation per
//! block is the whole of the discipline.
//!
//! **Lesson 2 — denominate each budget so it can actually fail for the
//! regression it names.** A page's read cost at `index_granularity = 8192` is
//! dominated by granule floors, so a budget expressed as a fraction of the
//! corpus is unsatisfiable physics and gets weakened until it means nothing,
//! while a bytes-only budget cannot catch a full scan of narrow scalars. Every
//! assertion below therefore states which regression it fails for, and the
//! statement-class split ([`StatementClass`]) is what keeps a per-statement
//! claim from being diluted by the statements it is not about.
//!
//! Each phase runs under its own `moraine-issue599-{phase}-…` Interactive
//! envelope, aggregated from `system.query_log` per phase AND per statement
//! class. Everything measured — asserted or not — is emitted as an evidence
//! JSON document so numbers can be diffed release over release.

use super::canonical_open_parity::with_owned_live_db;
use super::*;
use moraine_conversations::{ConversationMode, SessionOriginScope};

// --- corpus shape ----------------------------------------------------------

/// Sessions in the reference corpus (`S`). The measured axis is DIRECTORY
/// rows, which grow with `sessions x source-files`, so `S` is chosen to make
/// the directory large relative to a page while keeping the seeded event count
/// inside a sandbox's disk and the wrapper's 1800 s timeout.
const BASE_SESSIONS: u64 = 4_000;
const EVENTS_PER_SESSION: u64 = 20;
/// The permitted growth axis: `S x 10` additional sessions. Their per-session
/// event count is deliberately SMALLER than the reference corpus — this phase
/// exists to grow `mcp_session_directory`, and inflating the event corpus
/// tenfold as well would only buy seed time.
const MANY_SESSIONS: u64 = 40_000;
const EVENTS_PER_MANY_SESSION: u64 = 6;
/// The selective-filter fixture: fewer sessions than one hydration chunk, so a
/// dropped candidate filter is visible as extra hydration rather than as an
/// identical page served two ways.
const RARE_SESSIONS: u64 = 20;

/// `text_content` repeat counts. The fat corpus carries 50x the reference
/// corpus's transcript text at an identical row count — roughly 10x total
/// event bytes — which is exactly the axis the issue says must not move.
const BASE_TEXT_REPEAT: u32 = 2;
const FAT_TEXT_REPEAT: u32 = 100;
const TEXT_FRAGMENT: &str = "discovery benchmark transcript line ";

/// Session-id and harness prefixes are equal-length across corpora on purpose:
/// the response-payload flatness budget compares serialized page LENGTHS, and
/// a one-character difference in an id would show up as payload drift.
const BASE_PREFIX: &str = "bench-a";
const FAT_PREFIX: &str = "bench-b";
const MANY_PREFIX: &str = "bench-c";
const RARE_PREFIX: &str = "bench-r";
const BASE_HARNESS: &str = "bench-harness-a";
const FAT_HARNESS: &str = "bench-harness-b";
const MANY_HARNESS: &str = "bench-harness-c";
const RARE_HARNESS: &str = "bench-harness-r";
const BASE_CWD: &str = "/repo";
const RARE_CWD: &str = "/repo/rare";

/// Days after the anchor at which each corpus starts. Windows are 31 days
/// apart and each corpus spans well under a day, so a repository filter picks
/// exactly one corpus.
const BASE_WINDOW_DAYS: i64 = 0;
const FAT_WINDOW_DAYS: i64 = 31;
const MANY_WINDOW_DAYS: i64 = 62;
/// Width of each measurement window.
const WINDOW_SPAN_DAYS: i64 = 7;
/// The reference corpus starts one day into its window so the selective-filter
/// fixture (seeded at the window start) is strictly OLDER, and therefore never
/// occupies page 1 of an unfiltered descending read.
const BASE_SECOND_OFFSET: i64 = 86_400;

// --- budgets ---------------------------------------------------------------

/// The default interactive page size, and the size every per-page budget is
/// denominated against.
const PAGE_LIMIT: u16 = 25;
const LARGE_PAGE_LIMIT: u16 = 100;
const GRANULE_ROWS: u64 = 8_192;

/// One page issues at most 13 data statements (1 directory + 4 hydration
/// chunks x 3), each of which can pay up to two boundary granules on a read
/// that is otherwise perfectly pruned. Constant in corpus size — that is the
/// property that lets it bound a content-growth comparison, where a real
/// pruning regression overshoots by an order of magnitude. Same construction
/// as `canonical_open_benchmark::INDEPENDENCE_GRANULE_ALLOWANCE`.
const STATEMENT_GRANULE_ALLOWANCE: u64 = 13 * 2 * GRANULE_ROWS;
/// Per-session hydration allowance: three batched statements, one granule each.
///
/// One granule PER SESSION rather than per statement, because
/// `mcp_event_navigation` is `PARTITION BY cityHash64(session_id) % 64`
/// (`sql/036:116`): hydrating K sessions touches up to K distinct partitions,
/// and every partition pays its own boundary granule no matter how well the
/// primary key prunes inside it.
const PER_SESSION_ALLOWANCE: u64 = 3 * GRANULE_ROWS;
/// Sessions one hydration chunk covers at `limit = 1`:
/// `hydration_chunk_size(1) = clamp(2 × (1 + 1), 2, 256)`. Budget 4b is
/// denominated against this, not against the page size — a K = 1 page still
/// hydrates a whole chunk.
const PRUNE_CHUNK_SESSIONS: u64 = 4;
/// The §2.8 migration trigger, and the coefficient budget 4 asserts: Phase A
/// may read at most this multiple of the directory's own row count. A ratio
/// above it means the candidate pass is reading something that is not the
/// directory (an accidental join to `events`, say).
const DIRECTORY_READ_MULTIPLE: u64 = 3;
/// Interactive memory ceiling (issue-598 BINDING D10, unchanged here).
const MEMORY_CEILING_BYTES: u64 = 512 * 1024 * 1024;
/// Warm page-1 wall budget for every phase.
const WALL_BUDGET_MS: u64 = 1_000;
/// A relative wall budget is meaningless when the reference measurement is
/// tens of milliseconds — scheduler noise alone exceeds 1.5x. The comparison
/// therefore takes the larger of `1.5 x base` and this floor. It does not
/// weaken the gate: the regression it names (decompressing transcripts to
/// render a card) costs hundreds of milliseconds at 50x text, not tens.
const WALL_NOISE_FLOOR_MS: u64 = 250;
/// One page's statement budget (13 data statements plus publication snapshot
/// capture and revalidation). Fails if the batched builders were replaced by a
/// per-session loop.
const STATEMENT_BUDGET: u64 = 16;
/// Response-payload flatness tolerance (plan §5.3 budget 3).
const PAYLOAD_DRIFT_TOLERANCE: f64 = 0.10;

/// Rows per seeding INSERT. The fat corpus uses a smaller block because each
/// of its rows carries ~3.7 KB of text.
const SEED_BATCH: u64 = 40_000;
const FAT_SEED_BATCH: u64 = 8_000;

// --- query-log aggregation -------------------------------------------------

/// Which relation a measured statement opened. The split is what lets a
/// per-statement claim ("Phase A reads only the directory") be asserted
/// without the other statements' granule floors diluting it.
///
/// The classes are disjoint by construction: the candidate statement names
/// `mcp_session_directory` and never the navigation index, while all three
/// hydration statements name `mcp_event_navigation` and never the directory.
/// Everything else — publication snapshot capture and revalidation — is
/// `control`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StatementClass {
    Directory,
    Hydration,
}

impl StatementClass {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Directory => "directory",
            Self::Hydration => "hydration",
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct PhaseCostRow {
    phase: String,
    statement_class: String,
    query_count: u64,
    read_rows: u64,
    read_bytes: u64,
    /// Rows the statement RETURNED. On the directory class this is the
    /// candidate count, which is the only direct measurement of "filters
    /// narrowed before hydration" — `read_rows` cannot express it, because
    /// Phase A scans the whole directory either way (plan §2.8).
    result_rows: u64,
    peak_memory: u64,
    duration_ms: u64,
    retired_refs: u64,
}

#[derive(Debug, Clone, Default)]
struct Cost {
    query_count: u64,
    read_rows: u64,
    read_bytes: u64,
    result_rows: u64,
    peak_memory: u64,
    duration_ms: u64,
    retired_refs: u64,
}

impl Cost {
    fn add(&mut self, row: &PhaseCostRow) {
        self.query_count += row.query_count;
        self.read_rows += row.read_rows;
        self.read_bytes += row.read_bytes;
        self.result_rows += row.result_rows;
        self.peak_memory = self.peak_memory.max(row.peak_memory);
        self.duration_ms += row.duration_ms;
        self.retired_refs += row.retired_refs;
    }

    fn json(&self) -> Value {
        json!({
            "query_count": self.query_count,
            "read_rows": self.read_rows,
            "read_bytes": self.read_bytes,
            "result_rows": self.result_rows,
            "peak_memory_bytes": self.peak_memory,
            "duration_ms": self.duration_ms,
        })
    }
}

/// Per-phase totals plus the per-statement-class breakdown.
#[derive(Debug, Default)]
struct Measurements {
    totals: HashMap<String, Cost>,
    classes: HashMap<(String, String), Cost>,
}

impl Measurements {
    fn total(&self, phase: &str) -> Result<&Cost> {
        self.totals
            .get(phase)
            .with_context(|| format!("phase '{phase}' logged no statements"))
    }

    fn class(&self, phase: &str, class: StatementClass) -> Result<&Cost> {
        self.classes
            .get(&(phase.to_string(), class.as_str().to_string()))
            .with_context(|| format!("phase '{phase}' logged no {} statements", class.as_str()))
    }
}

/// Aggregate `moraine-issue599-{phase}-…` cost from `system.query_log`, split
/// by statement class, plus a per-group count of statements that referenced a
/// retired projected relation.
async fn read_measurements(clickhouse: &ClickHouseClient, database: &str) -> Result<Measurements> {
    clickhouse
        .request_text("SYSTEM FLUSH LOGS", None, None, false, None)
        .await
        .context("failed to flush the query log")?;
    let rows: Vec<PhaseCostRow> = clickhouse
        .query_rows(
            "SELECT
  extract(query_id, '^moraine-issue599-([a-z0-9]+)-') AS phase,
  multiIf(
    positionCaseInsensitive(query, 'mcp_session_directory') > 0, 'directory',
    positionCaseInsensitive(query, 'mcp_event_navigation') > 0, 'hydration',
    'control') AS statement_class,
  toUInt64(count()) AS query_count,
  toUInt64(sum(read_rows)) AS read_rows,
  toUInt64(sum(read_bytes)) AS read_bytes,
  toUInt64(sum(result_rows)) AS result_rows,
  toUInt64(max(memory_usage)) AS peak_memory,
  toUInt64(sum(query_duration_ms)) AS duration_ms,
  toUInt64(countIf(
    positionCaseInsensitive(query, 'mcp_open_') > 0
    OR positionCaseInsensitive(query, 'v_session_summary') > 0
    OR positionCaseInsensitive(query, 'v_conversation_trace') > 0
    OR positionCaseInsensitive(query, 'v_turn_summary') > 0)) AS retired_refs
FROM system.query_log
WHERE type = 'QueryFinish'
  AND startsWith(query_id, 'moraine-issue599-')
  AND current_database = currentDatabase()
GROUP BY phase, statement_class
FORMAT JSONEachRow",
            Some(database),
        )
        .await
        .context("failed to aggregate session-list discovery cost")?;

    let mut measurements = Measurements::default();
    for row in &rows {
        measurements
            .totals
            .entry(row.phase.clone())
            .or_default()
            .add(row);
        measurements
            .classes
            .entry((row.phase.clone(), row.statement_class.clone()))
            .or_default()
            .add(row);
    }
    Ok(measurements)
}

// --- fixture seeding -------------------------------------------------------

/// One generated corpus: `sessions x events_per_session` rows under ONE source
/// file, so `mcp_session_directory` holds exactly one row per session and the
/// pinned-heads join stays a single tuple.
#[allow(clippy::too_many_arguments)]
async fn seed_corpus(
    clickhouse: &ClickHouseClient,
    database: &str,
    prefix: &str,
    harness: &str,
    cwd: &str,
    sessions: u64,
    events_per_session: u64,
    base_expr: &str,
    text_repeat: u32,
    tool_calls: bool,
    batch_rows: u64,
) -> Result<()> {
    let total = sessions * events_per_session;
    // Index 2 of every session is a `session_meta` row. Without one, NO row in
    // the corpus is `is_metadata_bearing`, the batched metadata statement's
    // inner uid set is empty, and the only statement that decompresses
    // `payload_json` resolves zero rows in every phase — a budget denominated
    // on its cost would then be measuring an empty statement rather than the
    // hydration pass. The title payload is deliberately narrow and
    // equal-length across corpora (all prefixes are 7 characters), because the
    // payload-flatness budget compares serialized page LENGTHS.
    let event_kind_expr = if tool_calls {
        format!(
            "multiIf(n % {events_per_session} = 1, 'tool_call', \
             n % {events_per_session} = 2, 'session_meta', 'message')"
        )
    } else {
        format!("if(n % {events_per_session} = 2, 'session_meta', 'message')")
    };
    let tool_name_expr = if tool_calls {
        format!("if(n % {events_per_session} = 1, 'Bash', '')")
    } else {
        "''".to_string()
    };
    let payload_expr =
        format!("if(n % {events_per_session} = 2, '{{\"title\":\"{prefix} session\"}}', '{{}}')");
    let mut offset = 0_u64;
    while offset < total {
        let batch = batch_rows.min(total - offset);
        let insert = format!(
            "INSERT INTO `{database}`.`events`
             (ingested_at, event_uid, session_id, session_date, source_host, source_name, harness,
              inference_provider, source_file, source_ref, source_generation, source_offset,
              source_line_no, record_ts, event_ts, event_kind, actor_kind, payload_type, tool_name,
              tool_call_id, item_id, op_status, cwd, turn_index, text_content, payload_json,
              event_version)
             SELECT
               now64(3),
               concat('{prefix}-', leftPad(toString(n), 9, '0')),
               concat('{prefix}-', leftPad(toString(intDiv(n, {events_per_session})), 7, '0')),
               today(), 'issue599-host', 'fixture', '{harness}', 'openai',
               '/fixtures/{prefix}.jsonl', concat('{prefix}:', toString(n)), toUInt32(1),
               toUInt64(n * 4096), toUInt64(n + 1),
               toString(addSeconds({base_expr}, toInt32(n))),
               addSeconds({base_expr}, toInt32(n)),
               {event_kind_expr},
               if(n % {events_per_session} = 0, 'user', 'assistant'),
               'message',
               {tool_name_expr},
               '', '', '', '{cwd}', toUInt32(0),
               repeat('{TEXT_FRAGMENT}', {text_repeat}),
               {payload_expr},
               toUInt64(n + 1)
             FROM (SELECT number + {offset} AS n FROM numbers({batch}))"
        );
        clickhouse
            .request_text(&insert, None, Some(database), false, None)
            .await
            .with_context(|| format!("failed to seed benchmark corpus {prefix}"))?;
        offset += batch;
    }
    Ok(())
}

/// Lesson 1: make the measurement deterministic instead of racing the merge
/// scheduler. Called before every measurement block; no INSERT runs between
/// the phases inside a block.
async fn consolidate(clickhouse: &ClickHouseClient, database: &str) -> Result<()> {
    for table in [
        "events",
        "mcp_event_navigation",
        "mcp_event_locator",
        "mcp_session_directory",
    ] {
        clickhouse
            .request_text(
                &format!("OPTIMIZE TABLE `{database}`.`{table}` FINAL"),
                None,
                Some(database),
                false,
                None,
            )
            .await
            .with_context(|| format!("failed to consolidate {table} before measuring"))?;
    }
    Ok(())
}

#[derive(Debug, Deserialize)]
struct ScalarRow {
    value: String,
}

#[derive(Debug, Deserialize)]
struct CountRow {
    value: u64,
}

#[derive(Debug, Clone, Copy)]
struct Window {
    start_unix_ms: i64,
    end_unix_ms: i64,
}

/// One fixed anchor for the whole run, far enough in the past that the
/// monitor's `since=all` vocabulary covers every corpus while the fixtures
/// never sit in the future relative to the server clock.
async fn read_anchor(clickhouse: &ClickHouseClient, database: &str) -> Result<String> {
    Ok(clickhouse
        .query_rows::<ScalarRow>(
            "SELECT toString(toStartOfDay(subtractDays(now64(3), 120))) AS value \
             FORMAT JSONEachRow",
            Some(database),
        )
        .await
        .context("failed to read the benchmark time anchor")?
        .into_iter()
        .next()
        .context("time-anchor query returned no row")?
        .value)
}

fn window_base_expr(anchor: &str, window_days: i64) -> String {
    format!("addDays(toDateTime64('{anchor}', 3), {window_days})")
}

async fn read_window(
    clickhouse: &ClickHouseClient,
    database: &str,
    anchor: &str,
    window_days: i64,
) -> Result<Window> {
    #[derive(Deserialize)]
    struct WindowRow {
        start_unix_ms: i64,
        end_unix_ms: i64,
    }
    let row = clickhouse
        .query_rows::<WindowRow>(
            &format!(
                "SELECT
  toInt64(toUnixTimestamp64Milli({start})) AS start_unix_ms,
  toInt64(toUnixTimestamp64Milli({end})) AS end_unix_ms
FORMAT JSONEachRow",
                start = window_base_expr(anchor, window_days),
                end = window_base_expr(anchor, window_days + WINDOW_SPAN_DAYS),
            ),
            Some(database),
        )
        .await
        .context("failed to resolve a benchmark measurement window")?
        .into_iter()
        .next()
        .context("window query returned no row")?;
    Ok(Window {
        start_unix_ms: row.start_unix_ms,
        end_unix_ms: row.end_unix_ms,
    })
}

async fn table_row_count(
    clickhouse: &ClickHouseClient,
    database: &str,
    table: &str,
) -> Result<u64> {
    Ok(clickhouse
        .query_rows::<CountRow>(
            &format!(
                "SELECT toUInt64(count()) AS value \
                 FROM `{database}`.`{table}` FINAL FORMAT JSONEachRow"
            ),
            Some(database),
        )
        .await
        .with_context(|| format!("failed to count {table} rows"))?
        .into_iter()
        .next()
        .with_context(|| format!("{table} count returned no row"))?
        .value)
}

// --- phase driver ----------------------------------------------------------

fn admin_budget() -> moraine_config::ValidatedQueryBudget {
    ValidatedQueryBudgets::from_config(&QueryBudgetsConfig::default())
        .expect("bundled default query budgets are valid")
        .administrative
}

fn bench_repository(
    clickhouse: &ClickHouseClient,
    max_results: u16,
    scope_roots: Option<&[&str]>,
) -> ClickHouseConversationRepository {
    ClickHouseConversationRepository::new(
        clickhouse.clone(),
        RepoConfig {
            max_results,
            session_scope: scope_roots
                .and_then(|roots| SessionOriginScope::from_roots(roots.iter().copied())),
            ..RepoConfig::default()
        },
    )
}

fn bench_filter(
    window: Window,
    mode: Option<ConversationMode>,
    harness: Option<&str>,
) -> McpSessionListFilter {
    McpSessionListFilter {
        start_unix_ms: window.start_unix_ms,
        end_unix_ms: window.end_unix_ms,
        mode,
        sort: ConversationListSort::Desc,
        harness: harness.map(str::to_string),
        source_name: None,
    }
}

/// One measured page under its own phase-tagged Interactive envelope, exactly
/// the shape production gives a single `tools/call` or HTTP request. Returns
/// the serialized page (for the payload-flatness budget) and the wall time.
async fn measure_page(
    phase: &str,
    repository: &ClickHouseConversationRepository,
    filter: McpSessionListFilter,
    limit: u16,
) -> Result<(Value, u64, usize)> {
    QueryEnvelope::new(
        &format!("issue599-{phase}"),
        QueryClass::Interactive,
        &default_interactive_budget(),
    )
    .scope(async move {
        let started = Instant::now();
        let page = repository
            .list_mcp_sessions(
                filter,
                PageRequest {
                    limit,
                    cursor: None,
                },
            )
            .await
            .map_err(|error| anyhow!("phase '{phase}' page read failed: {error:#}"))?;
        let elapsed = started.elapsed().as_millis() as u64;
        let count = page.items.len();
        let payload =
            serde_json::to_value(&page.items).context("failed to serialize a measured page")?;
        Ok((payload, elapsed, count))
    })
    .await
}

/// Fetch one monitor session feed body, for the response-payload flatness
/// budget. The monitor mints its own envelope per request, so these bytes are
/// deliberately outside the phase aggregation.
async fn monitor_feed(client: &Client, base: &Url, harness: &str) -> Result<(Value, usize)> {
    let url = base.join(&format!("sessions?since=all&limit=25&harness={harness}"))?;
    let response = client
        .get(url.clone())
        .send()
        .await
        .with_context(|| format!("monitor request failed: {url}"))?;
    let status = response.status();
    let body = response.bytes().await.context("monitor body read failed")?;
    if !status.is_success() {
        bail!(
            "monitor returned HTTP {status}: {}",
            String::from_utf8_lossy(&body)
        );
    }
    let payload: Value = serde_json::from_slice(&body).context("monitor returned invalid JSON")?;
    if payload["ok"] != json!(true) {
        bail!("monitor session feed returned ok=false: {payload}");
    }
    Ok((payload, body.len()))
}

/// The feed is summaries only: no transcript key may appear at any depth.
fn assert_no_transcript_keys(payload: &Value, label: &str) -> Result<()> {
    fn walk(node: &Value, label: &str) -> Result<()> {
        match node {
            Value::Object(map) => {
                for (key, value) in map {
                    if key == "turns" || key == "text" {
                        bail!("{label} carries the transcript key {key:?}");
                    }
                    walk(value, label)?;
                }
                Ok(())
            }
            Value::Array(values) => values.iter().try_for_each(|value| walk(value, label)),
            _ => Ok(()),
        }
    }
    walk(payload, label)
}

fn relative_drift(base: usize, other: usize) -> f64 {
    if base == 0 {
        return f64::INFINITY;
    }
    (other as f64 - base as f64).abs() / base as f64
}

// ===========================================================================
// The gate
// ===========================================================================

/// **Fails for:** transcript content reaching the discovery page (budgets 1
/// and 3), a lost primary-key prune on either the candidate pass or the
/// batched hydration (budgets 2, 4 and 4b), candidate filters demoted to
/// post-hydration filters (budget 6), hydration that scales super-linearly in
/// the page size (budget 5), a per-session loop replacing the batched builders
/// (budget 7), and any reintroduction of a retired projected relation
/// (budget 10).
pub(super) async fn boundedness() -> Result<()> {
    with_owned_live_db(
        "session-list boundedness benchmark",
        |clickhouse, database| async move {
            let database_name = database.as_str();
            clickhouse
                .run_migrations()
                .await
                .context("failed to migrate the session-list benchmark database")?;

            let anchor = read_anchor(&clickhouse, database_name).await?;

            // --- reference corpus + the selective-filter fixture -----------
            seed_corpus(
                &clickhouse,
                database_name,
                RARE_PREFIX,
                RARE_HARNESS,
                RARE_CWD,
                RARE_SESSIONS,
                EVENTS_PER_SESSION,
                &window_base_expr(&anchor, BASE_WINDOW_DAYS),
                BASE_TEXT_REPEAT,
                true,
                SEED_BATCH,
            )
            .await?;
            seed_corpus(
                &clickhouse,
                database_name,
                BASE_PREFIX,
                BASE_HARNESS,
                BASE_CWD,
                BASE_SESSIONS,
                EVENTS_PER_SESSION,
                &format!(
                    "addSeconds({}, {BASE_SECOND_OFFSET})",
                    window_base_expr(&anchor, BASE_WINDOW_DAYS)
                ),
                BASE_TEXT_REPEAT,
                false,
                SEED_BATCH,
            )
            .await?;
            // Identical session and event counts, 50x the transcript text.
            seed_corpus(
                &clickhouse,
                database_name,
                FAT_PREFIX,
                FAT_HARNESS,
                BASE_CWD,
                BASE_SESSIONS,
                EVENTS_PER_SESSION,
                &window_base_expr(&anchor, FAT_WINDOW_DAYS),
                FAT_TEXT_REPEAT,
                false,
                FAT_SEED_BATCH,
            )
            .await?;

            publish_missing_schema_fixture_sources(&clickhouse, &database).await?;
            clickhouse
                .backfill_canonical_read_indexes(
                    true,
                    &live_fixture_budget(),
                    &admin_budget(),
                    |_| {},
                )
                .await
                .context("failed to backfill the canonical read indexes")?;

            let base_window =
                read_window(&clickhouse, database_name, &anchor, BASE_WINDOW_DAYS).await?;
            let fat_window =
                read_window(&clickhouse, database_name, &anchor, FAT_WINDOW_DAYS).await?;
            let many_window =
                read_window(&clickhouse, database_name, &anchor, MANY_WINDOW_DAYS).await?;

            let repository = bench_repository(&clickhouse, PAGE_LIMIT, None);
            if !repository.canonical_reader_ready().await {
                bail!(
                    "the directory path must be selected before this benchmark measures \
                     anything; the projected-header fallback is a different design"
                );
            }
            let large_repository = bench_repository(&clickhouse, 200, None);
            let scoped_repository = bench_repository(&clickhouse, PAGE_LIMIT, Some(&[RARE_CWD]));

            // --- measurement block 1 --------------------------------------
            consolidate(&clickhouse, database_name).await?;
            // Warm the connection and the readiness latch outside every
            // measured phase, so their one-time cost is not attributed to one.
            let _ = measure_page(
                "warm",
                &repository,
                bench_filter(base_window, None, None),
                PAGE_LIMIT,
            )
            .await?;

            let (base_payload, base_ms, base_count) = measure_page(
                "base",
                &repository,
                bench_filter(base_window, None, None),
                PAGE_LIMIT,
            )
            .await?;
            let (fat_payload, fat_ms, fat_count) = measure_page(
                "fatcontent",
                &repository,
                bench_filter(fat_window, None, None),
                PAGE_LIMIT,
            )
            .await?;
            let (_, k1_ms, k1_count) = measure_page(
                "pagek1",
                &repository,
                bench_filter(base_window, None, None),
                1,
            )
            .await?;
            let (_, k100_ms, k100_count) = measure_page(
                "pagek100",
                &large_repository,
                bench_filter(base_window, None, None),
                LARGE_PAGE_LIMIT,
            )
            .await?;
            let (_, filtered_ms, filtered_count) = measure_page(
                "filtered",
                &scoped_repository,
                bench_filter(
                    base_window,
                    Some(ConversationMode::ToolCalling),
                    Some(RARE_HARNESS),
                ),
                PAGE_LIMIT,
            )
            .await?;

            if base_count != usize::from(PAGE_LIMIT) || fat_count != usize::from(PAGE_LIMIT) {
                bail!(
                    "the reference and fat phases must both serve a full page; got {base_count} \
                     and {fat_count}"
                );
            }
            if k1_count != 1 {
                bail!("the K=1 phase served {k1_count} sessions");
            }
            if k100_count != usize::from(LARGE_PAGE_LIMIT) {
                bail!("the K=100 phase served {k100_count} sessions");
            }
            // The filtered page must serve the whole selective fixture. Zero is
            // the signature of budget 6's regression arriving early: candidate
            // filters that never reached the directory hand the chunk loop
            // reference-corpus sessions, which Phase C then eliminates, and the
            // page returns empty with a continuation. More than the fixture
            // means the filter is not being applied at all.
            if filtered_count != RARE_SESSIONS as usize {
                bail!(
                    "the selective-filter phase served {filtered_count} sessions against a \
                     fixture of {RARE_SESSIONS}; 0 means the candidate filters are being \
                     applied only after hydration"
                );
            }

            // --- monitor payload flatness ---------------------------------
            let client = Client::builder().timeout(Duration::from_secs(60)).build()?;
            let (monitor_base, shutdown, server, static_dir) =
                start_owned_monitor(bench_repository(&clickhouse, PAGE_LIMIT, None)).await?;
            let monitor_outcome: Result<(usize, usize)> = async {
                let (base_body, base_len) =
                    monitor_feed(&client, &monitor_base, BASE_HARNESS).await?;
                let (fat_body, fat_len) = monitor_feed(&client, &monitor_base, FAT_HARNESS).await?;
                assert_no_transcript_keys(&base_body, "monitor reference feed")?;
                assert_no_transcript_keys(&fat_body, "monitor fat-content feed")?;
                Ok((base_len, fat_len))
            }
            .await;
            let _ = shutdown.send(());
            let server_result = match server.await {
                Ok(result) => result,
                Err(error) => Err(anyhow!("owned monitor task failed to join: {error}")),
            };
            let static_cleanup = fs::remove_dir_all(&static_dir)
                .context("failed to remove owned monitor static directory");
            let monitor_cleanup = finish_with_cleanup(server_result, static_cleanup);
            let (monitor_base_len, monitor_fat_len) = match (monitor_outcome, monitor_cleanup) {
                (Ok(lengths), Ok(())) => lengths,
                (Err(error), Ok(())) => return Err(error),
                (Ok(_), Err(cleanup)) => {
                    return Err(cleanup.context("live monitor teardown failed"))
                }
                (Err(error), Err(cleanup)) => {
                    return Err(anyhow!(
                        "{error:#}; live monitor teardown also failed: {cleanup:#}"
                    ))
                }
            };

            // --- measurement block 2: the permitted growth axis ------------
            seed_corpus(
                &clickhouse,
                database_name,
                MANY_PREFIX,
                MANY_HARNESS,
                BASE_CWD,
                MANY_SESSIONS,
                EVENTS_PER_MANY_SESSION,
                &window_base_expr(&anchor, MANY_WINDOW_DAYS),
                BASE_TEXT_REPEAT,
                false,
                SEED_BATCH,
            )
            .await?;
            publish_missing_schema_fixture_sources(&clickhouse, &database).await?;
            consolidate(&clickhouse, database_name).await?;
            let directory_rows =
                table_row_count(&clickhouse, database_name, "mcp_session_directory").await?;
            // The denominator budget 4b's separation is stated against: one
            // unpruned pass over the navigation index.
            let navigation_rows =
                table_row_count(&clickhouse, database_name, "mcp_event_navigation").await?;
            // `consolidate` rewrote every part, so block 2's first read would
            // otherwise pay a cold page cache that block 1's phases did not.
            // Warm outside the measured phases, exactly as block 1 does.
            let _ = measure_page(
                "warmtwo",
                &repository,
                bench_filter(many_window, None, None),
                PAGE_LIMIT,
            )
            .await?;

            let (_, many_ms, many_count) = measure_page(
                "manysessions",
                &repository,
                bench_filter(many_window, None, None),
                PAGE_LIMIT,
            )
            .await?;
            let (_, prune_ms, prune_count) = measure_page(
                "prune",
                &repository,
                bench_filter(many_window, None, None),
                1,
            )
            .await?;
            if many_count != usize::from(PAGE_LIMIT) || prune_count != 1 {
                bail!(
                    "the growth phases must serve full pages; got {many_count} and {prune_count}"
                );
            }

            // --- budgets ---------------------------------------------------
            let measurements = read_measurements(&clickhouse, database_name).await?;
            let base_cost = measurements.total("base")?;
            let fat_cost = measurements.total("fatcontent")?;
            let k1_cost = measurements.total("pagek1")?;
            let k100_cost = measurements.total("pagek100")?;
            let filtered_cost = measurements.total("filtered")?;
            let many_cost = measurements.total("manysessions")?;
            let prune_cost = measurements.total("prune")?;

            // 1. Content-independence, BYTES — the primary gate. `read_bytes`
            //    scales directly with wide-column decompression, so a
            //    transcript leak into the page fails this by an order of
            //    magnitude. A row-count budget cannot fail here at all: 50x
            //    fatter rows are the same NUMBER of rows.
            if fat_cost.read_bytes > 2 * base_cost.read_bytes {
                bail!(
                    "50x transcript text moved the page's read_bytes: base={} fat={} \
                     (budget: <= 2x). Discovery is reading a wide column.",
                    base_cost.read_bytes,
                    fat_cost.read_bytes
                );
            }
            // 1b. The same claim denominated against the statements it is
            //     about. The totals above include the directory pass, which is
            //     identical in both phases and therefore dilutes the ratio;
            //     the hydration class is where a wide column would be read.
            let base_hydration = measurements.class("base", StatementClass::Hydration)?;
            let fat_hydration = measurements.class("fatcontent", StatementClass::Hydration)?;
            if fat_hydration.read_bytes > 2 * base_hydration.read_bytes {
                bail!(
                    "50x transcript text moved HYDRATION read_bytes: base={} fat={} \
                     (budget: <= 2x). The metadata pass is decompressing text_content.",
                    base_hydration.read_bytes,
                    fat_hydration.read_bytes
                );
            }
            // 2. Content-independence, ROWS. Both phases hydrate the same
            //    number of sessions from tables of the same size, so the only
            //    legitimate difference is boundary granules — constant in
            //    corpus size, while a pruning regression overshoots by orders
            //    of magnitude.
            if fat_cost.read_rows > base_cost.read_rows + STATEMENT_GRANULE_ALLOWANCE {
                bail!(
                    "the fat-content page read {} rows against the reference page's {} \
                     (allowance {STATEMENT_GRANULE_ALLOWANCE}); a prune was lost",
                    fat_cost.read_rows,
                    base_cost.read_rows
                );
            }
            // 3. Response payload flatness — the direct "no transcripts"
            //    assertion, which needs no granule reasoning at all.
            let mcp_base_len = serde_json::to_string(&base_payload)?.len();
            let mcp_fat_len = serde_json::to_string(&fat_payload)?.len();
            let mcp_drift = relative_drift(mcp_base_len, mcp_fat_len);
            if mcp_drift > PAYLOAD_DRIFT_TOLERANCE {
                bail!(
                    "the MCP page payload grew with transcript size: base={mcp_base_len} B \
                     fat={mcp_fat_len} B (drift {mcp_drift:.3} > {PAYLOAD_DRIFT_TOLERANCE})"
                );
            }
            let monitor_drift = relative_drift(monitor_base_len, monitor_fat_len);
            if monitor_drift > PAYLOAD_DRIFT_TOLERANCE {
                bail!(
                    "the monitor feed payload grew with transcript size: base={monitor_base_len} \
                     B fat={monitor_fat_len} B (drift {monitor_drift:.3} > \
                     {PAYLOAD_DRIFT_TOLERANCE})"
                );
            }
            // 4. Session-count growth is linear in scalar directory state,
            //    with a stated coefficient. NOT flatness: the directory is
            //    legitimately O(sessions) (plan §2.8). Denominated against the
            //    candidate statement alone, because that is the only statement
            //    the claim is about — and a ratio above the multiple is also
            //    the §2.8 follow-up-migration trigger.
            let many_directory = measurements.class("manysessions", StatementClass::Directory)?;
            let directory_budget = DIRECTORY_READ_MULTIPLE * directory_rows;
            if many_directory.read_rows > directory_budget {
                bail!(
                    "the candidate pass read {} rows against {directory_rows} directory rows \
                     (budget {DIRECTORY_READ_MULTIPLE}x = {directory_budget}); Phase A is \
                     reading something other than mcp_session_directory",
                    many_directory.read_rows
                );
            }
            // 4b. Hydration is PK-pruned to the candidate sessions. Measured
            //     at K = 1 on the largest corpus, where the hydrated chunk's
            //     floors sit an order of magnitude below one unpruned pass over
            //     the navigation index — the separation a K = 25 page cannot
            //     provide at this corpus size, which is why the pruning claim is
            //     asserted here and not on the `manysessions` page.
            //
            //     Denominated PER SESSION, not per statement.
            //     `mcp_event_navigation` is `PARTITION BY cityHash64(session_id)
            //     % 64` (sql/036:116), so K hydrated sessions land in up to K
            //     distinct partitions and each one contributes its own granule
            //     floor; a per-statement budget would be the denomination trap
            //     in miniature — unsatisfiable physics that gets weakened until
            //     it means nothing.
            let prune_hydration = measurements.class("prune", StatementClass::Hydration)?;
            let prune_budget = PRUNE_CHUNK_SESSIONS * PER_SESSION_ALLOWANCE * 2;
            if prune_hydration.read_rows > prune_budget {
                bail!(
                    "hydration read {} rows for a one-session page over a {directory_rows}-session \
                     corpus (budget {prune_budget}); the `session_id IN (…)` prune was lost — one \
                     unpruned pass over the navigation index is {navigation_rows} rows per \
                     statement",
                    prune_hydration.read_rows
                );
            }
            // 5. Page-size scaling: the INCREMENT from K=1 to K=25 is bounded
            //    by the page size, not by the corpus. This budget bounds a
            //    growth rate only — at this corpus size a hydration pass that
            //    ignored the candidate list entirely would read the same rows
            //    at both page sizes and produce a delta of zero, so the
            //    absolute pruning claim is budget 4b's, not this one's.
            let page_scaling_budget =
                u64::from(PAGE_LIMIT - 1) * PER_SESSION_ALLOWANCE + STATEMENT_GRANULE_ALLOWANCE;
            let page_scaling_delta = base_cost.read_rows.saturating_sub(k1_cost.read_rows);
            if page_scaling_delta > page_scaling_budget {
                bail!(
                    "a K={PAGE_LIMIT} page read {} rows against a K=1 page's {} \
                     (budget {page_scaling_budget}); hydration is not output-sized",
                    base_cost.read_rows,
                    k1_cost.read_rows
                );
            }
            // 6. Filter push-down actually happens.
            //
            //    The regression this names: candidate filters demoted to
            //    post-hydration filters. Phase A would hand back its whole
            //    over-fetch of reference-corpus candidates, the chunk loop
            //    would spend all four hydration chunks eliminating them, and
            //    the page would come back empty.
            //
            //    (a) Candidate NARROWING, read off the directory statement's
            //        `result_rows`. This is the one direct measurement of
            //        "filters narrow before hydration": `read_rows` cannot
            //        express it, because Phase A scans the whole directory
            //        under either behaviour (plan §2.8, session_id-leading
            //        sort key). A dropped filter returns
            //        `candidate_fetch_size(25) = 208` rows here instead of the
            //        fixture's 20.
            let base_directory = measurements.class("base", StatementClass::Directory)?;
            let filtered_directory = measurements.class("filtered", StatementClass::Directory)?;
            let filtered_hydration = measurements.class("filtered", StatementClass::Hydration)?;
            if filtered_directory.result_rows > RARE_SESSIONS {
                bail!(
                    "the filtered candidate pass returned {} candidates for a fixture of \
                     {RARE_SESSIONS} matching sessions (the unfiltered pass returns {}); \
                     harness/mode/scope are not reaching the directory's HAVING",
                    filtered_directory.result_rows,
                    base_directory.result_rows
                );
            }
            //    (b) ONE hydration chunk, not four. A page whose candidates
            //        are all eliminated after hydration exhausts
            //        MAX_HYDRATION_CHUNKS and issues 4 x 3 = 12 statements.
            if filtered_hydration.query_count > 3 {
                bail!(
                    "the filtered page issued {} hydration statements (budget 3 = one chunk); \
                     the chunk loop is discarding whole chunks the directory should never have \
                     selected",
                    filtered_hydration.query_count
                );
            }
            //    (c) The plan's read_rows axis, denominated `<=` rather than
            //        `<`. At `index_granularity = 8192` a 20-session and a
            //        52-session hydration are both ONE granule per statement,
            //        so a HEALTHY filtered page ties the reference page; a
            //        strict `<` would be a coin flip on granule alignment
            //        rather than a contract — Lesson 2 in the other direction.
            //        It still fails the regression, which reads four chunks'
            //        worth of scattered sessions.
            if filtered_cost.read_rows > base_cost.read_rows {
                bail!(
                    "a page filtered to {filtered_count} sessions read {} rows against the \
                     unfiltered page's {}; candidate filters are not reaching the directory",
                    filtered_cost.read_rows,
                    base_cost.read_rows
                );
            }
            // 7. Statement count: fails if the batched builders became a
            //    per-session loop (3 x 25 = 75 round trips).
            for (phase, cost) in [("base", base_cost), ("manysessions", many_cost)] {
                if cost.query_count > STATEMENT_BUDGET {
                    bail!(
                        "phase '{phase}' issued {} statements (budget {STATEMENT_BUDGET}); the \
                         batched hydration builders were replaced by a per-session loop",
                        cost.query_count
                    );
                }
            }
            // 8. Peak memory over EVERY measured phase (BINDING D10 parity).
            let peak_memory = measurements
                .totals
                .values()
                .map(|cost| cost.peak_memory)
                .max()
                .unwrap_or(0);
            if peak_memory > MEMORY_CEILING_BYTES {
                bail!(
                    "interactive query memory exceeded 512 MiB during discovery: {peak_memory} \
                     bytes"
                );
            }
            // 9. Wall clock. The relative arm takes the larger of 1.5x the
            //    reference and a noise floor: a ratio over a tens-of-
            //    milliseconds measurement is scheduler jitter, while the
            //    regression it names costs hundreds of milliseconds.
            for (phase, elapsed) in [
                ("base", base_ms),
                ("fatcontent", fat_ms),
                ("pagek1", k1_ms),
                ("pagek100", k100_ms),
                ("filtered", filtered_ms),
                ("manysessions", many_ms),
                ("prune", prune_ms),
            ] {
                if elapsed > WALL_BUDGET_MS {
                    bail!(
                        "phase '{phase}' warm page-1 took {elapsed} ms (budget {WALL_BUDGET_MS})"
                    );
                }
            }
            let relative_wall_budget = (base_ms * 3 / 2).max(WALL_NOISE_FLOOR_MS);
            for (phase, elapsed) in [("fatcontent", fat_ms), ("filtered", filtered_ms)] {
                if elapsed > relative_wall_budget {
                    bail!(
                        "phase '{phase}' took {elapsed} ms against a reference of {base_ms} ms \
                         (budget {relative_wall_budget})"
                    );
                }
            }
            // 10. No measured statement may reopen a retired projected
            //     relation. Cheap here, and this is the one gate that observes
            //     every phase.
            for (phase, cost) in &measurements.totals {
                if cost.retired_refs != 0 {
                    bail!(
                        "phase '{phase}' ran {} statement(s) against a retired projected \
                         relation (mcp_open_* / v_session_summary / v_conversation_trace / \
                         v_turn_summary)",
                        cost.retired_refs
                    );
                }
            }

            println!(
                "{}",
                json!({
                    "benchmark_id": "session-list-boundedness",
                    "corpus": {
                        "base_sessions": BASE_SESSIONS,
                        "fat_sessions": BASE_SESSIONS,
                        "many_sessions": MANY_SESSIONS,
                        "rare_sessions": RARE_SESSIONS,
                        "events_per_session": EVENTS_PER_SESSION,
                        "events_per_many_session": EVENTS_PER_MANY_SESSION,
                        "directory_rows": directory_rows,
                        "navigation_rows": navigation_rows,
                        "fat_text_multiple": FAT_TEXT_REPEAT / BASE_TEXT_REPEAT,
                    },
                    "latency_ms": {
                        "base": base_ms,
                        "fatcontent": fat_ms,
                        "pagek1": k1_ms,
                        "pagek100": k100_ms,
                        "filtered": filtered_ms,
                        "manysessions": many_ms,
                        "prune": prune_ms,
                    },
                    "phase_totals": {
                        "base": base_cost.json(),
                        "fatcontent": fat_cost.json(),
                        "pagek1": k1_cost.json(),
                        "pagek100": k100_cost.json(),
                        "filtered": filtered_cost.json(),
                        "manysessions": many_cost.json(),
                        "prune": prune_cost.json(),
                    },
                    "statement_classes": {
                        "base_directory": base_directory.json(),
                        "base_hydration": base_hydration.json(),
                        "fatcontent_hydration": fat_hydration.json(),
                        "filtered_directory": filtered_directory.json(),
                        "filtered_hydration": filtered_hydration.json(),
                        "manysessions_directory": many_directory.json(),
                        "prune_hydration": prune_hydration.json(),
                    },
                    "budgets": {
                        // Reported, not asserted: a K=100 page's hydration is
                        // capped by the navigation index's total granule count
                        // at this corpus size, so a fixed multiple of the K=25
                        // page is unsatisfiable physics rather than a contract
                        // (same reasoning as the #598 `reread_multiple`).
                        "pagek100_read_rows": k100_cost.read_rows,
                        "directory_read_multiple_measured":
                            many_directory.read_rows as f64 / directory_rows.max(1) as f64,
                        "directory_read_multiple_budget": DIRECTORY_READ_MULTIPLE,
                        "statement_granule_allowance": STATEMENT_GRANULE_ALLOWANCE,
                        "per_session_allowance": PER_SESSION_ALLOWANCE,
                        "prune_hydration_read_rows": prune_hydration.read_rows,
                        "prune_hydration_budget": prune_budget,
                        "mcp_payload_bytes": {"base": mcp_base_len, "fatcontent": mcp_fat_len},
                        "monitor_payload_bytes": {
                            "base": monitor_base_len,
                            "fatcontent": monitor_fat_len,
                        },
                        "peak_query_memory_bytes": peak_memory,
                        "memory_ceiling_bytes": MEMORY_CEILING_BYTES,
                    },
                })
            );
            Ok(())
        },
    )
    .await
}
