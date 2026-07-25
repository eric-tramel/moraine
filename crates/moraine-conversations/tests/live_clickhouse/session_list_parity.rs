//! Issue #599 WI-07 exit gates for session DISCOVERY (plan §5.1 and §5.2).
//!
//! Two gates live here, each registered as its own `run-live-test` mode:
//!   * `list-parity`   -> [`directory_parity`]  (plan §5.1)
//!   * `list-query-log`-> [`query_log_clean`]   (plan §5.2)
//!
//! The root `live_clickhouse.rs` owns the `#[tokio::test]` wrappers so the
//! `run-live-test` `--exact` paths stay flat, and scopes each body under the
//! shared live-fixture Migration envelope; the operation under test builds its
//! own Interactive envelope per page, exactly the way the MCP and monitor
//! boundaries do.
//!
//! Both gates drive `ConversationRepository::list_mcp_sessions` — the single
//! shared discovery operation. `moraine-mcp-core` is not a dependency of this
//! crate and adds no ClickHouse statement of its own to the page, so the
//! repository call IS the MCP read path for query-log purposes.

use super::canonical_open_parity::{seed_events, with_owned_live_db, Ev};
use super::*;
use moraine_conversations::{ConversationMode, McpSessionListItem, SessionOriginScope};

/// A window wide enough to contain every fixture in this module, so a filter
/// combination narrows on `mode`/`harness`/`source`/scope alone.
const WINDOW_START_MS: i64 = 0;
const WINDOW_END_MS: i64 = 4_102_444_800_000;

/// Small enough that every filter combination pages several times: the parity
/// contract covers multi-page traversal, not just page 1.
const PAGE_LIMIT: u16 = 3;

/// Guard against a cursor that fails to advance. The fixture corpus is ~20
/// sessions, so a correct traversal at [`PAGE_LIMIT`] never approaches this.
const MAX_PAGES: usize = 64;

/// The project roots the scope arm reads under.
const SCOPE_ROOT: &str = "/scope/root";
const SCOPE_ROOTS: &[&str] = &[SCOPE_ROOT];

// ---------------------------------------------------------------------------
// Fixture corpus
// ---------------------------------------------------------------------------

/// Every fixture is seeded through `events` + publication control only, so the
/// projected-header path and the directory path derive from one corpus.
///
/// The cases are the plan §5.1 enumeration: omp / non-omp title precedence,
/// terminal-in-a-middle-turn, blank and whitespace-only session ids, a session
/// whose harness differs between two source files, one session per mode, the
/// four `--project-only` scope shapes, and the tombstone source whose
/// generation is replaced later in the gate.
fn fixture_corpus() -> Vec<Ev> {
    vec![
        // --- omp LIST chain: dispatch-title fallback ----------------------
        // No metadata title/name/summary at all, so the omp branch falls
        // through to `omp_dispatch_title` (the source file stem, because the
        // file is not named after the session).
        Ev::new("list-omp-dispatch", "list-omp-dispatch-u1", 10)
            .user()
            .source("omp", "/tmp/omp/DispatchTitle.jsonl"),
        Ev::new("list-omp-dispatch", "list-omp-dispatch-a1", 11)
            .source("omp", "/tmp/omp/DispatchTitle.jsonl"),
        // --- omp LIST chain: an explicit `title_change` outranks a later
        // name. `is_metadata_bearing` admits the omp title payload
        // (sql/036:213), so the omp branch's `latest_metadata_title` sees it
        // while the non-omp branch (session_meta only) would not.
        Ev::new("list-omp-explicit", "list-omp-explicit-title", 20)
            .source("omp", "/tmp/omp/ExplicitTask.jsonl")
            .payload(
                "unknown",
                r#"{"type":"title_change","title":"Explicit OMP Title"}"#,
            )
            .payload_type("title_change"),
        Ev::new("list-omp-explicit", "list-omp-explicit-name", 21)
            .source("omp", "/tmp/omp/ExplicitTask.jsonl")
            .payload("session_meta", r#"{"name":"Later Lower Priority Name"}"#)
            .payload_type("session_meta"),
        // --- non-omp LIST chain: the divergence migration 035 exists to
        // keep. OPEN resolves `coalesce(latest_metadata_title,
        // latest_metadata_name)` and answers "Pi Title"; LIST resolves
        // `latest_session_meta_title`, the LATEST session_meta row's
        // `coalesce(title, name, summary)`, and answers "Later Pi Name". Only
        // this fixture separates the two folds.
        Ev::new("list-pi-precedence", "list-pi-title", 30)
            .source("pi", "/tmp/pi/precedence.jsonl")
            .payload("session_meta", r#"{"title":"Pi Title","slug":"pi-slug"}"#)
            .payload_type("session_meta"),
        Ev::new("list-pi-precedence", "list-pi-name", 31)
            .source("pi", "/tmp/pi/precedence.jsonl")
            .payload("session_meta", r#"{"name":"Later Pi Name"}"#)
            .payload_type("session_meta"),
        // Summary-only non-omp session: LIST title falls through the coalesce
        // to the summary, where OPEN reports no title at all.
        Ev::new("list-pi-summary", "list-pi-summary-meta", 40)
            .source("pi", "/tmp/pi/summary.jsonl")
            .payload("session_meta", r#"{"summary":"Pi Summary"}"#)
            .payload_type("session_meta"),
        Ev::new("list-pi-summary", "list-pi-summary-a1", 41).source("pi", "/tmp/pi/summary.jsonl"),
        // --- terminal event in a MIDDLE turn ------------------------------
        // `completed` is the LAST turn's flag. A session-wide latest-terminal
        // argMax would answer `true` here; the two-level rule answers `false`.
        Ev::new("list-terminal-mid", "list-terminal-mid-t1u", 50)
            .turn(1)
            .user(),
        Ev::new("list-terminal-mid", "list-terminal-mid-t1a", 51).turn(1),
        Ev::new("list-terminal-mid", "list-terminal-mid-t2u", 52)
            .turn(2)
            .user(),
        Ev::new("list-terminal-mid", "list-terminal-mid-t2a", 53).turn(2),
        Ev::new("list-terminal-mid", "list-terminal-mid-t2done", 54)
            .turn(2)
            .payload("queue_operation", "{}")
            .payload_type("task_complete"),
        Ev::new("list-terminal-mid", "list-terminal-mid-t3u", 55)
            .turn(3)
            .user(),
        Ev::new("list-terminal-mid", "list-terminal-mid-t3a", 56).turn(3),
        // The same shape with the terminal in the LAST turn, so the fixture
        // pair pins both answers rather than only the negative one.
        Ev::new("list-terminal-last", "list-terminal-last-t1u", 60)
            .turn(1)
            .user(),
        Ev::new("list-terminal-last", "list-terminal-last-t1a", 61).turn(1),
        Ev::new("list-terminal-last", "list-terminal-last-t2u", 62)
            .turn(2)
            .user(),
        Ev::new("list-terminal-last", "list-terminal-last-t2done", 63)
            .turn(2)
            .payload("queue_operation", "{}")
            .payload_type("task_complete"),
        // --- blank and whitespace-only session ids ------------------------
        // The 036 MVs guard only `notEmpty`, so the whitespace id reaches the
        // directory. Neither may appear, consume a LIMIT slot, or anchor a
        // cursor.
        Ev::new("   ", "list-blank-whitespace", 70)
            .user()
            .source("fixture", "/fixtures/list-blank.jsonl"),
        Ev::new("", "list-blank-empty", 71)
            .user()
            .source("fixture", "/fixtures/list-blank.jsonl"),
        // A real session sharing the blank rows' source file, so publishing
        // that source is not an artifact of the blank rows alone.
        Ev::new("list-blank-neighbour", "list-blank-neighbour-a1", 72)
            .source("fixture", "/fixtures/list-blank.jsonl"),
        // --- one session, two source files, two harnesses -----------------
        // The exact filter value is the LATEST by `(event_ts, event_uid)`, not
        // any and not the first; the directory's
        // `has(groupUniqArray(harness), …)` is recall only and admits both.
        Ev::new("list-multi-harness", "list-multi-harness-a1", 80)
            .source("fixture", "/fixtures/list-multi-harness-a.jsonl")
            .harness("codex")
            .provider("openai"),
        Ev::new("list-multi-harness", "list-multi-harness-b1", 81)
            .source("fixture", "/fixtures/list-multi-harness-b.jsonl")
            .harness("claude-code")
            .provider("anthropic"),
        // --- one session per mode -----------------------------------------
        Ev::new("list-mode-chat", "list-mode-chat-u1", 90).user(),
        Ev::new("list-mode-chat", "list-mode-chat-a1", 91),
        Ev::new("list-mode-tool", "list-mode-tool-u1", 100).user(),
        Ev::new("list-mode-tool", "list-mode-tool-c1", 101).tool_call("Bash", "call-tool-1"),
        Ev::new("list-mode-web", "list-mode-web-u1", 110).user(),
        Ev::new("list-mode-web", "list-mode-web-c1", 111)
            .tool_call("WebSearch", "call-web-1")
            .payload_type("web_search_call"),
        // The §2.3 landmine session: an internal Moraine MCP tool call, so the
        // LIVE `cd::mode_aggregate_expr` answers `mcp_internal`. The gate then
        // rewrites this session's stored `mode_hint` DOWN to 1 — see
        // [`force_stale_mode_hint`].
        Ev::new("list-mode-mcp", "list-mode-mcp-u1", 120).user(),
        Ev::new("list-mode-mcp", "list-mode-mcp-c1", 121)
            .tool_call("search_sessions", "call-mcp-1"),
        // --- `--project-only` scope shapes --------------------------------
        Ev::new("list-scope-exact", "list-scope-exact-a1", 130).cwd(SCOPE_ROOT),
        Ev::new("list-scope-prefix", "list-scope-prefix-a1", 131)
            .cwd(&format!("{SCOPE_ROOT}/nested/worktree")),
        Ev::new("list-scope-outside", "list-scope-outside-a1", 132).cwd("/scope/other"),
        // A harness that records no cwd matches no root, under either path.
        Ev::new("list-scope-nocwd", "list-scope-nocwd-a1", 133).cwd(""),
        // --- tombstone source: generation 1 carries both sessions ---------
        Ev::new("list-tombstone", "list-tombstone-g1-a1", 140)
            .source("fixture", "/fixtures/list-tombstone.jsonl")
            .generation(1),
        Ev::new(
            "list-tombstone-survivor",
            "list-tombstone-survivor-g1-a1",
            141,
        )
        .source("fixture", "/fixtures/list-tombstone.jsonl")
        .generation(1),
    ]
}

/// Generation 2 of the tombstone source: the replacement no longer contains
/// `list-tombstone`. Publishing it must remove that session from the feed.
fn tombstone_replacement_generation() -> Vec<Ev> {
    vec![Ev::new(
        "list-tombstone-survivor",
        "list-tombstone-survivor-g2-a1",
        142,
    )
    .source("fixture", "/fixtures/list-tombstone.jsonl")
    .generation(2)]
}

/// Every session id [`fixture_corpus`] seeds, and therefore exactly what an
/// unfiltered traversal must return under BOTH paths before the tombstone
/// replacement runs.
///
/// This is the gate's non-vacuity guard. A parity diff of two empty vectors
/// passes, so a fixture that failed to publish its source heads, a projector
/// backfill that left every session dirty, or a canonical backfill that
/// covered nothing would all read as "the paths agree". Pinning the set makes
/// each of those name itself, and it is also what stops a session silently
/// disappearing from both implementations at once.
const FIXTURE_SESSIONS: &[&str] = &[
    "list-blank-neighbour",
    "list-mode-chat",
    "list-mode-mcp",
    "list-mode-tool",
    "list-mode-web",
    "list-multi-harness",
    "list-omp-dispatch",
    "list-omp-explicit",
    "list-pi-precedence",
    "list-pi-summary",
    "list-scope-exact",
    "list-scope-nocwd",
    "list-scope-outside",
    "list-scope-prefix",
    "list-terminal-last",
    "list-terminal-mid",
    "list-tombstone",
    "list-tombstone-survivor",
];

/// The LIST title/summary the §2.6 fold must produce, asserted directly as
/// well as through parity. Parity alone would pass if BOTH paths regressed
/// together; these expectations come from the projector golden
/// (`projected_publication_header.sql:123-124`).
const EXPECTED_LIST_METADATA: &[(&str, Option<&str>, Option<&str>)] = &[
    (
        "list-omp-dispatch",
        Some("DispatchTitle"),
        Some("DispatchTitle"),
    ),
    (
        "list-omp-explicit",
        Some("Explicit OMP Title"),
        Some("Explicit OMP Title"),
    ),
    (
        "list-pi-precedence",
        Some("Later Pi Name"),
        Some("Later Pi Name"),
    ),
    ("list-pi-summary", Some("Pi Summary"), Some("Pi Summary")),
    ("list-mode-chat", None, None),
];

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

fn admin_budget() -> moraine_config::ValidatedQueryBudget {
    ValidatedQueryBudgets::from_config(&QueryBudgetsConfig::default())
        .expect("bundled default query budgets are valid")
        .administrative
}

fn scoped_repository(
    clickhouse: &ClickHouseClient,
    scope_roots: Option<&[&str]>,
) -> ClickHouseConversationRepository {
    let cfg = RepoConfig {
        session_scope: scope_roots
            .and_then(|roots| SessionOriginScope::from_roots(roots.iter().copied())),
        ..RepoConfig::default()
    };
    ClickHouseConversationRepository::new(clickhouse.clone(), cfg)
}

fn filter(
    mode: Option<ConversationMode>,
    harness: Option<&str>,
    source_name: Option<&str>,
    sort: ConversationListSort,
) -> McpSessionListFilter {
    McpSessionListFilter {
        start_unix_ms: WINDOW_START_MS,
        end_unix_ms: WINDOW_END_MS,
        mode,
        sort,
        harness: harness.map(str::to_string),
        source_name: source_name.map(str::to_string),
    }
}

/// One filter combination the parity matrix collects under both sort
/// directions. `scope` is a repository configuration rather than a filter
/// field, which is why it travels beside the filter instead of inside it.
struct MatrixArm {
    label: &'static str,
    mode: Option<ConversationMode>,
    harness: Option<&'static str>,
    source: Option<&'static str>,
    scope: Option<&'static [&'static str]>,
}

impl MatrixArm {
    fn filter(&self, sort: ConversationListSort) -> McpSessionListFilter {
        filter(self.mode, self.harness, self.source, sort)
    }

    /// The arm whose result set is the whole seeded corpus, and therefore the
    /// one [`assert_is_whole_corpus`] can check.
    fn is_unfiltered(&self) -> bool {
        self.mode.is_none()
            && self.harness.is_none()
            && self.source.is_none()
            && self.scope.is_none()
    }
}

/// Every filter dimension the operation supports. Each arm is collected under
/// both sort directions and both read paths.
fn parity_matrix() -> Vec<MatrixArm> {
    let arm = |label, mode, harness, source, scope| MatrixArm {
        label,
        mode,
        harness,
        source,
        scope,
    };
    vec![
        arm("unfiltered", None, None, None, None),
        arm("harness=codex", None, Some("codex"), None, None),
        arm("harness=claude-code", None, Some("claude-code"), None, None),
        arm("source=omp", None, None, Some("omp"), None),
        arm("source=pi", None, None, Some("pi"), None),
        arm("mode=chat", Some(ConversationMode::Chat), None, None, None),
        arm(
            "mode=tool_calling",
            Some(ConversationMode::ToolCalling),
            None,
            None,
            None,
        ),
        arm(
            "mode=web_search",
            Some(ConversationMode::WebSearch),
            None,
            None,
            None,
        ),
        arm(
            "mode=mcp_internal",
            Some(ConversationMode::McpInternal),
            None,
            None,
            None,
        ),
        arm(
            "mode+harness",
            Some(ConversationMode::ToolCalling),
            Some("codex"),
            None,
            None,
        ),
        arm("scope", None, None, None, Some(SCOPE_ROOTS)),
        arm(
            "scope+harness",
            None,
            Some("codex"),
            None,
            Some(SCOPE_ROOTS),
        ),
    ]
}

/// Page one filter to exhaustion, one fresh Interactive envelope per page.
///
/// Production scopes one envelope per request, and the Interactive
/// `statement_cap` is per request; running a whole traversal under a single
/// envelope would trip that cap, which is the #600 contract rather than a
/// discovery defect.
///
/// An EMPTY page carrying a cursor is a legal "keep going" signal (plan §1.3),
/// so the loop terminates on `next_cursor == None` and never on an empty page.
async fn traverse(
    repository: &ClickHouseConversationRepository,
    filter: &McpSessionListFilter,
    limit: u16,
    label: &str,
) -> Result<Vec<McpSessionListItem>> {
    let mut cursor: Option<String> = None;
    let mut items: Vec<McpSessionListItem> = Vec::new();
    let mut seen: BTreeSet<String> = BTreeSet::new();
    for page_number in 1..=MAX_PAGES {
        let page = QueryEnvelope::new(
            "issue599-listparity",
            QueryClass::Interactive,
            &default_interactive_budget(),
        )
        .scope(repository.list_mcp_sessions(
            filter.clone(),
            PageRequest {
                limit,
                cursor: cursor.clone(),
            },
        ))
        .await
        .map_err(|error| anyhow!("{label} page {page_number} failed: {error:#}"))?;
        if page.items.len() > usize::from(limit) {
            bail!(
                "{label} page {page_number} returned {} items above its limit {limit}",
                page.items.len()
            );
        }
        for item in &page.items {
            if !seen.insert(item.session_id.clone()) {
                bail!(
                    "{label} returned session {} twice across a quiescent traversal",
                    item.session_id
                );
            }
        }
        items.extend(page.items);
        match page.next_cursor {
            Some(next) => cursor = Some(next),
            None => return Ok(items),
        }
    }
    bail!("{label} did not exhaust its cursor within {MAX_PAGES} pages")
}

/// Every field of [`McpSessionListItem`], so a divergence in any one of them
/// fails rather than only the ones a hand-written projection happened to list.
fn item_json(item: &McpSessionListItem) -> Result<Value> {
    serde_json::to_value(item).context("failed to serialize a session list item")
}

fn items_json(items: &[McpSessionListItem]) -> Result<Vec<Value>> {
    items.iter().map(item_json).collect()
}

/// Assert one unfiltered traversal is the whole seeded corpus.
///
/// Run against BOTH paths before they are diffed, because the diff itself
/// cannot distinguish "the two paths agree" from "neither path returned
/// anything".
fn assert_is_whole_corpus(path: &str, items: &[McpSessionListItem]) -> Result<()> {
    let returned: BTreeSet<&str> = items
        .iter()
        .map(|item| item.session_id.as_str())
        .collect::<BTreeSet<_>>();
    let expected: BTreeSet<&str> = FIXTURE_SESSIONS.iter().copied().collect();
    if returned != expected {
        let missing: Vec<&str> = expected.difference(&returned).copied().collect();
        let unexpected: Vec<&str> = returned.difference(&expected).copied().collect();
        bail!(
            "the {path} path's unfiltered traversal is not the seeded corpus: \
             missing {missing:?}, unexpected {unexpected:?}"
        );
    }
    Ok(())
}

/// Publish every fixture source head, then build ONLY the v1 projector, so the
/// first collection pass provably runs the projected-header path (the
/// canonical `open_v2` readiness key is still 0).
async fn publish_and_build_headers(
    clickhouse: &ClickHouseClient,
    database: &OwnedDatabaseName,
) -> Result<()> {
    publish_missing_schema_fixture_sources(clickhouse, database).await?;
    clickhouse
        .backfill_mcp_open_read_model()
        .await
        .context("failed to backfill the v1 projector read model")?;
    Ok(())
}

/// Backfill the migration-036 read indexes. On a Local-publication backend
/// this also publishes the one-way `open_v2` readiness key, which is the gate
/// `list_mcp_sessions` consults to select the directory path.
async fn backfill_and_promote_canonical(clickhouse: &ClickHouseClient) -> Result<()> {
    clickhouse
        .backfill_canonical_read_indexes(true, &live_fixture_budget(), &admin_budget(), |_| {})
        .await
        .context("failed to backfill the canonical read indexes")?;
    if !clickhouse
        .open_v2_reader_ready()
        .await
        .context("failed to read the open_v2 readiness key")?
    {
        bail!("canonical backfill completed without publishing open_v2 readiness");
    }
    Ok(())
}

/// Rewrite one session's stored `mode_hint` DOWN to `tool_calling` (1) while
/// its live rows still aggregate to `mcp_internal` (2).
///
/// This is the §2.3 landmine, and it cannot be produced by seeding alone: the
/// internal-tool-name allowlist is frozen inside the migration-036 MV body
/// (`sql/036:156`) and the compiled `cd::mode_mcp_internal_predicate` currently
/// carries the identical list, so a fresh database has no drift. A store that
/// was migrated BEFORE a tool name was added to `mcp_tool_names` does, and this
/// is exactly its directory state: a hint strictly below the live rank.
///
/// The mutation touches `mcp_session_directory` only, so the projected-header
/// path — collected before this runs — is unaffected and parity still holds.
///
/// A regression that precision-filters (`mode_hint = 2`) or pushes a lower
/// bound at all (`mode_hint >= 2`) for `mcp_internal` drops this session from
/// the candidate page, and both the direct assertion and the parity diff fail.
async fn force_stale_mode_hint(
    clickhouse: &ClickHouseClient,
    database: &OwnedDatabaseName,
    session_id: &str,
) -> Result<()> {
    let database = database.as_str();
    clickhouse
        .request_text(
            &format!(
                "ALTER TABLE `{database}`.`mcp_session_directory` \
                 DELETE WHERE session_id = '{session_id}' SETTINGS mutations_sync = 2"
            ),
            None,
            Some(database),
            false,
            None,
        )
        .await
        .context("failed to clear the landmine session's directory rows")?;
    // Re-materialize the MV body with `mode_hint` pinned to the stale rank.
    // The column list is the migration-036 table order.
    clickhouse
        .request_text(
            &format!(
                "INSERT INTO `{database}`.`mcp_session_directory`
SELECT
  session_id,
  source_host,
  source_name,
  source_file,
  source_generation,
  harness,
  toUInt8(1) AS mode_hint,
  min(ifNull(parseDateTime64BestEffortOrNull(record_ts), ingested_at)) AS min_observed_event_time,
  max(ifNull(parseDateTime64BestEffortOrNull(record_ts), ingested_at)) AS max_observed_event_time,
  count() AS observed_events,
  argMinIfState(cwd, tuple(event_ts, event_uid), cwd != '') AS origin_cwd_state
FROM `{database}`.`events`
WHERE session_id = '{session_id}'
GROUP BY session_id, source_host, source_name, source_file, source_generation, harness"
            ),
            None,
            Some(database),
            false,
            None,
        )
        .await
        .context("failed to re-seed the landmine session's stale directory rows")?;

    #[derive(Deserialize)]
    struct HintRow {
        value: u8,
    }
    let hint = clickhouse
        .query_rows::<HintRow>(
            &format!(
                "SELECT toUInt8(max(mode_hint)) AS value \
                 FROM `{database}`.`mcp_session_directory` \
                 WHERE session_id = '{session_id}' FORMAT JSONEachRow"
            ),
            Some(database),
        )
        .await
        .context("failed to verify the stale mode hint")?
        .into_iter()
        .next()
        .context("stale mode-hint verification returned no row")?
        .value;
    if hint != 1 {
        bail!("landmine directory rows report mode_hint {hint}, expected the stale rank 1");
    }
    Ok(())
}

// ===========================================================================
// Gate 1: list-parity
// ===========================================================================

/// **Fails for:** any semantic divergence between the directory path and the
/// projected-header path it replaces — a wrong LIST title/summary fold, a
/// dropped or mis-ordered session, a `completed` derived from the wrong turn,
/// a recall filter left as the exact filter, a blank id consuming a LIMIT slot,
/// a lost `--project-only` boundary, or a precision `mode_hint` prefilter.
///
/// Both paths are driven off ONE seeded corpus. The projected-header page is
/// collected first, while `open_v2` readiness is still 0; the canonical
/// backfill then publishes readiness and the same matrix is collected from the
/// directory path. Comparing full multi-page traversals (not page 1) is what
/// makes the cursor and the `limit + 1` probe part of the contract.
pub(super) async fn directory_parity() -> Result<()> {
    with_owned_live_db("session-list parity gate", |clickhouse, database| async move {
        clickhouse
            .run_migrations()
            .await
            .context("failed to migrate the session-list parity database")?;
        seed_events(&clickhouse, &fixture_corpus()).await?;
        publish_and_build_headers(&clickhouse, &database).await?;

        let arms = parity_matrix();
        let sorts = [
            ("desc", ConversationListSort::Desc),
            ("asc", ConversationListSort::Asc),
        ];

        // --- collection pass 1: the projected-header path -----------------
        if clickhouse
            .open_v2_reader_ready()
            .await
            .context("failed to read the open_v2 readiness key")?
        {
            bail!("open_v2 readiness must still be 0 before the canonical backfill");
        }
        let mut header_results: Vec<(String, Vec<Value>)> = Vec::new();
        for matrix_arm in &arms {
            let repository = scoped_repository(&clickhouse, matrix_arm.scope);
            for (sort_label, sort) in sorts {
                let arm = format!("{}/{sort_label}", matrix_arm.label);
                let items = traverse(
                    &repository,
                    &matrix_arm.filter(sort),
                    PAGE_LIMIT,
                    &format!("header {arm}"),
                )
                .await?;
                if matrix_arm.is_unfiltered() && matches!(sort, ConversationListSort::Desc) {
                    assert_is_whole_corpus("projected-header", &items)?;
                }
                header_results.push((arm, items_json(&items)?));
            }
        }

        // --- publish the canonical read path ------------------------------
        backfill_and_promote_canonical(&clickhouse).await?;
        force_stale_mode_hint(&clickhouse, &database, "list-mode-mcp").await?;

        // --- collection pass 2: the directory path ------------------------
        let mut directory_results: Vec<(String, Vec<Value>)> = Vec::new();
        let mut unfiltered_desc: Vec<McpSessionListItem> = Vec::new();
        for matrix_arm in &arms {
            let repository = scoped_repository(&clickhouse, matrix_arm.scope);
            if !repository.canonical_reader_ready().await {
                bail!("the directory path must be selected once open_v2 readiness is published");
            }
            for (sort_label, sort) in sorts {
                let arm = format!("{}/{sort_label}", matrix_arm.label);
                let items = traverse(
                    &repository,
                    &matrix_arm.filter(sort),
                    PAGE_LIMIT,
                    &format!("directory {arm}"),
                )
                .await?;
                if matrix_arm.is_unfiltered() && matches!(sort, ConversationListSort::Desc) {
                    assert_is_whole_corpus("directory", &items)?;
                    unfiltered_desc.clone_from(&items);
                }
                directory_results.push((arm, items_json(&items)?));
            }
        }

        // --- the parity diff ----------------------------------------------
        if header_results.len() != directory_results.len() {
            bail!(
                "collected {} header arms and {} directory arms",
                header_results.len(),
                directory_results.len()
            );
        }
        for ((header_arm, header_items), (directory_arm, directory_items)) in
            header_results.iter().zip(directory_results.iter())
        {
            if header_arm != directory_arm {
                bail!("arm order diverged: {header_arm} vs {directory_arm}");
            }
            if header_items != directory_items {
                bail!(
                    "session-list parity mismatch on arm {header_arm}\n  headers  ={}\n  directory={}",
                    Value::Array(header_items.clone()),
                    Value::Array(directory_items.clone())
                );
            }
        }

        // --- the assertions parity alone cannot make ----------------------
        // Parity proves the two paths agree; these prove they agree on the
        // RIGHT answer, so a fold that regressed in both cannot pass.
        let by_id: HashMap<&str, &McpSessionListItem> = unfiltered_desc
            .iter()
            .map(|item| (item.session_id.as_str(), item))
            .collect();
        for (session_id, expected_title, expected_summary) in EXPECTED_LIST_METADATA {
            let item = by_id
                .get(session_id)
                .with_context(|| format!("fixture session {session_id} missing from the feed"))?;
            if item.title.as_deref() != *expected_title
                || item.session_summary.as_deref() != *expected_summary
            {
                bail!(
                    "LIST metadata fold wrong for {session_id}: title={:?} summary={:?}, expected {:?} / {:?}",
                    item.title,
                    item.session_summary,
                    expected_title,
                    expected_summary
                );
            }
        }
        let completed = |session_id: &str| -> Result<bool> {
            Ok(by_id
                .get(session_id)
                .with_context(|| format!("fixture session {session_id} missing from the feed"))?
                .completed)
        };
        if completed("list-terminal-mid")? {
            bail!("`completed` must be the LAST turn's flag; the mid-turn terminal leaked");
        }
        if !completed("list-terminal-last")? {
            bail!("a session whose last turn completed must report completed = true");
        }
        let multi_harness = by_id
            .get("list-multi-harness")
            .context("multi-harness fixture missing from the feed")?;
        if multi_harness.harness.as_deref() != Some("claude-code") {
            bail!(
                "exact harness must be the LATEST value, got {:?}",
                multi_harness.harness
            );
        }
        for blank in ["", "   "] {
            if by_id.contains_key(blank) {
                bail!("a blank session id reached the feed: {blank:?}");
            }
        }

        // The §2.3 landmine, asserted directly so the failure names the
        // regression instead of surfacing only as an arm diff.
        let mcp_internal = scoped_repository(&clickhouse, None);
        let internal_items = traverse(
            &mcp_internal,
            &filter(
                Some(ConversationMode::McpInternal),
                None,
                None,
                ConversationListSort::Desc,
            ),
            PAGE_LIMIT,
            "landmine mode=mcp_internal",
        )
        .await?;
        if !internal_items
            .iter()
            .any(|item| item.session_id == "list-mode-mcp")
        {
            bail!(
                "a session whose stored mode_hint predates its tool name was dropped from \
                 mode = mcp_internal: the directory hint must never be precision-filtered \
                 (issue-599 §2.3, sql/036:156)"
            );
        }

        // --- §2.7 tombstone: a superseded generation removes the session ---
        let feed = scoped_repository(&clickhouse, None);
        let before = traverse(
            &feed,
            &filter(None, None, Some("fixture"), ConversationListSort::Desc),
            PAGE_LIMIT,
            "tombstone before",
        )
        .await?;
        if !before.iter().any(|item| item.session_id == "list-tombstone") {
            bail!("the tombstone fixture must be visible before its generation is replaced");
        }
        seed_events(&clickhouse, &tombstone_replacement_generation()).await?;
        publish_replaced_schema_fixture_sources(&clickhouse, &database).await?;
        let after = traverse(
            &feed,
            &filter(None, None, Some("fixture"), ConversationListSort::Desc),
            PAGE_LIMIT,
            "tombstone after",
        )
        .await?;
        if after.iter().any(|item| item.session_id == "list-tombstone") {
            bail!(
                "a session dropped by a replacement generation must leave the feed: the \
                 published-generation tuple-IN is the canonical replacement for the retired \
                 `tombstone` column (issue-599 §2.7)"
            );
        }
        if !after
            .iter()
            .any(|item| item.session_id == "list-tombstone-survivor")
        {
            bail!("the surviving session of the replacement generation must stay in the feed");
        }
        Ok(())
    })
    .await
}

// ===========================================================================
// Gate 2: list-query-log
// ===========================================================================

/// The relations the interactive discovery path must never open again.
/// `mcp_open_` is the catch-all; the named entries exist so a failure says
/// which one came back.
const FORBIDDEN_RELATIONS: &[&str] = &[
    "v_session_summary",
    "v_conversation_trace",
    "v_turn_summary",
    "mcp_open_sessions",
    "mcp_open_publication_headers",
    "mcp_open_dirty_sessions",
    "mcp_open_",
];

/// The relations that MUST appear, so an empty or mis-filtered query log
/// cannot pass this gate vacuously.
const REQUIRED_RELATIONS: &[&str] = &["mcp_session_directory", "mcp_event_navigation"];

/// The corpus-wide session-origin subquery `scope.rs` emits
/// (`argMin(cwd, tuple(event_ts, event_uid))`). The directory path resolves
/// scope from `argMinIfMerge(origin_cwd_state)` and re-checks it against
/// `argMinIf(nav.cwd, …)`, so this exact token must be gone. Neither surviving
/// expression contains it: `argMinIf(` and `argMinIfMerge(` both fail the
/// `argMin(cwd` match.
const CORPUS_SCOPE_SUBQUERY_TOKEN: &str = "argMin(cwd";

#[derive(Debug, Deserialize)]
struct RelationHitRow {
    matches: u64,
    total: u64,
}

/// Count statements matching `arm_predicate` that mention `needle`, plus the
/// arm's total statement count (so "zero matches" can be distinguished from
/// "zero statements").
async fn relation_hits(
    clickhouse: &ClickHouseClient,
    database: &str,
    arm_predicate: &str,
    needle: &str,
) -> Result<RelationHitRow> {
    clickhouse
        .query_rows::<RelationHitRow>(
            &format!(
                "SELECT
  toUInt64(countIf(positionCaseInsensitive(query, '{needle}') > 0)) AS matches,
  toUInt64(count()) AS total
FROM system.query_log
WHERE type != 'QueryStart'
  AND current_database = currentDatabase()
  AND {arm_predicate}
FORMAT JSONEachRow"
            ),
            Some(database),
        )
        .await
        .with_context(|| format!("failed to count '{needle}' statements where {arm_predicate}"))?
        .into_iter()
        .next()
        .context("relation-hit query returned no row")
}

async fn server_now(clickhouse: &ClickHouseClient, database: &str) -> Result<String> {
    #[derive(Deserialize)]
    struct NowRow {
        value: String,
    }
    Ok(clickhouse
        .query_rows::<NowRow>(
            "SELECT toString(now64(6)) AS value FORMAT JSONEachRow",
            Some(database),
        )
        .await
        .context("failed to read the server clock")?
        .into_iter()
        .next()
        .context("server clock query returned no row")?
        .value)
}

async fn assert_arm_is_clean(
    clickhouse: &ClickHouseClient,
    database: &str,
    arm: &str,
    arm_predicate: &str,
) -> Result<()> {
    for needle in REQUIRED_RELATIONS {
        let hits = relation_hits(clickhouse, database, arm_predicate, needle).await?;
        if hits.total == 0 {
            bail!(
                "the {arm} arm logged no statements at all ({arm_predicate}); the gate would \
                 have passed vacuously"
            );
        }
        if hits.matches == 0 {
            bail!(
                "the {arm} arm ran {} statements but none referenced {needle}; discovery did \
                 not read the canonical indexes",
                hits.total
            );
        }
    }
    for needle in FORBIDDEN_RELATIONS {
        let hits = relation_hits(clickhouse, database, arm_predicate, needle).await?;
        if hits.matches != 0 {
            bail!(
                "the {arm} arm ran {} statement(s) referencing {needle}; the interactive \
                 discovery path must not open it (issue-599 §5.2)",
                hits.matches
            );
        }
    }
    let scope = relation_hits(
        clickhouse,
        database,
        arm_predicate,
        CORPUS_SCOPE_SUBQUERY_TOKEN,
    )
    .await?;
    if scope.matches != 0 {
        bail!(
            "the {arm} arm ran {} statement(s) carrying the corpus-wide session-origin \
             subquery ({CORPUS_SCOPE_SUBQUERY_TOKEN}); scope is resolved from the directory's \
             own aggregate state (issue-599 §2.4)",
            scope.matches
        );
    }
    Ok(())
}

/// **Fails for:** any reintroduction of a projected or corpus-shaped relation
/// into the interactive session feed — a fallback to
/// `mcp_open_publication_headers`, a `v_session_summary` join, a
/// `v_conversation_trace` / `v_turn_summary` read, or the corpus-wide
/// `argMin(cwd, …)` scope subquery — on either the MCP or the monitor caller.
///
/// The positive half (`mcp_session_directory` AND `mcp_event_navigation` must
/// both appear) is what stops an empty or mis-filtered `system.query_log`
/// selection from passing the negative half for free.
///
/// Both repositories are project-scoped, so a scope regression has something
/// to reintroduce; the fixture publishes BOTH read models, so a header-path
/// fallback would return data rather than fail loudly on an empty table.
pub(super) async fn query_log_clean() -> Result<()> {
    with_owned_live_db(
        "session-list query-log gate",
        |clickhouse, database| async move {
            clickhouse
                .run_migrations()
                .await
                .context("failed to migrate the session-list query-log database")?;
            seed_events(&clickhouse, &fixture_corpus()).await?;
            publish_and_build_headers(&clickhouse, &database).await?;
            backfill_and_promote_canonical(&clickhouse).await?;

            let database_name = database.as_str();
            let scoped = scoped_repository(&clickhouse, Some(SCOPE_ROOTS));
            if !scoped.canonical_reader_ready().await {
                bail!("the directory path must be selected before this gate can mean anything");
            }

            // --- MCP arm: its own phase-tagged Interactive envelope --------
            let page = QueryEnvelope::new(
                "issue599-mcplist",
                QueryClass::Interactive,
                &default_interactive_budget(),
            )
            .scope(scoped.list_mcp_sessions(
                filter(None, None, None, ConversationListSort::Desc),
                PageRequest {
                    limit: 25,
                    cursor: None,
                },
            ))
            .await
            .map_err(|error| anyhow!("MCP-arm list_sessions failed: {error:#}"))?;
            if page.items.is_empty() {
                bail!("the MCP arm returned no sessions; the scoped fixture must match");
            }

            // --- monitor arm ----------------------------------------------
            // The monitor middleware mints its own `moraine-monitor-…`
            // envelope per HTTP request, so the arm is isolated by that prefix
            // plus a server-clock lower bound taken after the readiness probe
            // and before the only request this gate issues.
            let monitor_repository = scoped_repository(&clickhouse, Some(SCOPE_ROOTS));
            let client = Client::builder().timeout(Duration::from_secs(30)).build()?;
            let (base, shutdown, server, static_dir) =
                start_owned_monitor(monitor_repository).await?;
            let monitor_since = server_now(&clickhouse, database_name).await?;
            let monitor_outcome = async {
                let url = base.join("sessions?since=all&limit=25")?;
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
                let payload: Value =
                    serde_json::from_slice(&body).context("monitor returned invalid JSON")?;
                if payload["ok"] != json!(true) {
                    bail!("monitor session feed returned ok=false: {payload}");
                }
                // The arm must have HYDRATED something. A feed that returned
                // no sessions never issues a `mcp_event_navigation` statement,
                // so the positive half of `assert_arm_is_clean` would fail on
                // a fixture symptom while naming a discovery defect.
                if payload["sessions"].as_array().is_none_or(Vec::is_empty) {
                    bail!("the monitor arm returned no sessions; the scoped fixture must match");
                }
                Ok(())
            }
            .await;
            let _ = shutdown.send(());
            let server_result = match server.await {
                Ok(result) => result,
                Err(error) => Err(anyhow!("owned monitor task failed to join: {error}")),
            };
            let static_cleanup = fs::remove_dir_all(&static_dir)
                .context("failed to remove owned monitor static directory");
            finish_with_cleanup(
                monitor_outcome,
                finish_with_cleanup(server_result, static_cleanup),
            )?;

            // --- assertions -------------------------------------------------
            clickhouse
                .request_text("SYSTEM FLUSH LOGS", None, None, false, None)
                .await
                .context("failed to flush the query log")?;
            assert_arm_is_clean(
                &clickhouse,
                database_name,
                "MCP list_sessions",
                "startsWith(query_id, 'moraine-issue599-mcplist-')",
            )
            .await?;
            assert_arm_is_clean(
                &clickhouse,
                database_name,
                "monitor /api/v1/sessions",
                &format!(
                    "startsWith(query_id, 'moraine-monitor-') \
                     AND event_time_microseconds >= toDateTime64('{monitor_since}', 6)"
                ),
            )
            .await?;
            Ok(())
        },
    )
    .await
}
