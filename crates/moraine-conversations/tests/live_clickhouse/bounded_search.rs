//! Issue #597 live exit gates for BOUNDED SEARCH (plan §6.3, correction C6).
//!
//! Two gates live here, each registered as its own `run-live-test` mode:
//!   * `bounded-search-exec`        -> [`statement_execution`]
//!   * `bounded-search-attribution` -> [`double_attribution`]
//!
//! The root `live_clickhouse.rs` owns the `#[tokio::test]` wrappers so the
//! `run-live-test` `--exact` paths stay flat, and scopes each body under the
//! shared live-fixture Migration envelope; the operations under test build
//! their own Interactive envelopes, exactly the way the MCP boundary does.
//!
//! # Why these exist
//!
//! `plans/597-bounded-search.md` §6.3 makes live execution a hard rule for
//! this work, and states the reason precisely: an outer query that references
//! `alias.column` when the inner derived table fails to project it is a
//! perfectly well-formed STRING. ClickHouse rejects it only when the statement
//! runs. Every SQL-shape test in `search_canonical.rs` passes against such a
//! statement. This repository has already shipped that defect twice in this
//! epic — `ev.event_ts` (#598) and `nav.cwd` (#599) — so a text-matching test
//! is not evidence that a rewritten statement works.
//!
//! [`statement_execution`] therefore drives EVERY v2 search statement through
//! a live server, across the query shapes that select the optional clauses
//! (project scope, session scope, turn scope, harness/source filters, the
//! `extra_posting_columns` variant conversation search uses). It asserts
//! results, not just absence of error, so a builder that executes and returns
//! nothing cannot pass.
//!
//! [`double_attribution`] is the document-grain gate (C1, corrected by ledger
//! D1). It proves on the SERVER that the search read model cannot hold two
//! attributions of one uid — the locator version join already reduces them to
//! one before any merge, and `OPTIMIZE … FINAL` leaves one physical row in each
//! of `search_documents`, `search_postings` and `mcp_event_locator` — and then
//! that ranking is document-grained end to end: one hit, under the attribution
//! the locator authorizes, and none under the losing one.

use super::canonical_open_parity::{seed_events, with_owned_live_db, Ev};
use super::*;
use moraine_conversations::{
    ConversationSearchQuery, SearchEventsQuery, SearchMcpEventsQuery, SessionOriginScope,
};

/// The distinctive query term. Fixture text is derived from the event uid
/// (`"assistant text for {uid}"` / `"user prompt for {uid}"`), and the postings
/// tokenizer is `extractAll(lowerUTF8(text), '[a-z0-9_]+')` with terms of
/// length 2..=64, so embedding the term in the uid is what puts it in the
/// index. Nothing else in any fixture corpus contains it.
const TERM: &str = "zephyr";

/// The project root the scoped arms read under. `Ev`'s default `cwd` is
/// `/repo`, so the in-scope sessions need no override and the out-of-scope one
/// sets `cwd` explicitly.
const SCOPE_ROOT: &str = "/repo";

fn repository(
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

fn admin_budget() -> moraine_config::ValidatedQueryBudget {
    ValidatedQueryBudgets::from_config(&QueryBudgetsConfig::default())
        .expect("bundled default query budgets are valid")
        .administrative
}

/// Publish every seeded source head and backfill the migration-036 read
/// indexes, which on a Local-publication backend also publishes the one-way
/// `open_v2` readiness key the v2 search path branches on.
async fn publish_and_promote(
    clickhouse: &ClickHouseClient,
    database: &OwnedDatabaseName,
) -> Result<()> {
    publish_missing_schema_fixture_sources(clickhouse, database).await?;
    clickhouse
        .backfill_canonical_read_indexes(true, &live_fixture_budget(), &admin_budget(), |_| {})
        .await
        .context("failed to backfill the canonical read indexes")?;
    if !clickhouse
        .open_v2_reader_ready()
        .await
        .context("failed to read the open_v2 readiness key")?
    {
        // Report the audit's own numbers. Guessing which check withheld
        // readiness cost two speculative fixes (#613, #614) before this gate
        // could say what it actually observed.
        let outcome = clickhouse
            .core_index_audit_outcome()
            .await
            .context("failed to read the core-index audit outcome")?;
        bail!(
            "canonical backfill completed without publishing open_v2 readiness; \
             audit outcome = {outcome:?}"
        );
    }
    Ok(())
}

#[derive(Debug, Deserialize)]
struct CountRow {
    value: u64,
}

/// The session a surviving `search_postings` row names, for the attribution
/// gate's "which one won" probe.
#[derive(Debug, Deserialize)]
struct AttributionRow {
    session_id: String,
}

async fn scalar_count(clickhouse: &ClickHouseClient, sql: &str) -> Result<u64> {
    Ok(clickhouse
        .query_rows::<CountRow>(sql, None)
        .await
        .with_context(|| format!("count query failed: {sql}"))?
        .into_iter()
        .next()
        .with_context(|| format!("count query returned no row: {sql}"))?
        .value)
}

/// Count statements matching `predicate` for one request-id prefix, plus that
/// arm's total statement count, so "zero matches" is distinguishable from
/// "zero statements" (a gate that passes because nothing ran is not a gate).
#[derive(Debug, Deserialize)]
struct LogHitRow {
    matches: u64,
    total: u64,
}

async fn log_hits(
    clickhouse: &ClickHouseClient,
    database: &str,
    request_prefix: &str,
    needle: &str,
) -> Result<LogHitRow> {
    clickhouse
        .query_rows::<LogHitRow>(
            &format!(
                "SELECT
  toUInt64(countIf(positionCaseInsensitive(query, '{needle}') > 0)) AS matches,
  toUInt64(count()) AS total
FROM system.query_log
WHERE type != 'QueryStart'
  AND current_database = currentDatabase()
  AND startsWith(query_id, '{request_prefix}')
FORMAT JSONEachRow"
            ),
            Some(database),
        )
        .await
        .with_context(|| format!("query-log probe failed for {request_prefix}/{needle}"))?
        .into_iter()
        .next()
        .context("query-log probe returned no row")
}

// ---------------------------------------------------------------------------
// Gate 1: bounded-search-exec
// ---------------------------------------------------------------------------

/// Two in-scope sessions and one out-of-scope session, all carrying [`TERM`].
///
/// `bs-alpha` spans two explicit turns so turn-scoped ranking has a turn to
/// select and a turn to exclude; `bs-beta` gives the ranking pass a second
/// session so a session predicate has something to remove; `bs-outside` sits
/// under a different `cwd` so the project-scope recall filter and the exact
/// re-check both have a session to drop.
fn execution_corpus() -> Vec<Ev> {
    vec![
        Ev::new("bs-alpha", "bs-alpha-zephyr-u1", 10).user().turn(1),
        Ev::new("bs-alpha", "bs-alpha-zephyr-a1", 11).turn(1),
        Ev::new("bs-alpha", "bs-alpha-zephyr-u2", 12).user().turn(2),
        Ev::new("bs-alpha", "bs-alpha-zephyr-a2", 13).turn(2),
        Ev::new("bs-beta", "bs-beta-zephyr-u1", 20).user().turn(1),
        Ev::new("bs-beta", "bs-beta-zephyr-a1", 21)
            .turn(1)
            .harness("claude")
            .source("claude", "/fixtures/bs-beta-claude.jsonl"),
        Ev::new("bs-outside", "bs-outside-zephyr-u1", 30)
            .user()
            .turn(1)
            .cwd("/elsewhere"),
        Ev::new("bs-outside", "bs-outside-zephyr-a1", 31)
            .turn(1)
            .cwd("/elsewhere"),
    ]
}

fn mcp_query(query: &str) -> SearchMcpEventsQuery {
    SearchMcpEventsQuery {
        query: query.to_string(),
        cancellation_token: None,
        n_hits: Some(10),
        session_id: None,
        turn_seq: None,
        event_types: None,
        harness: None,
        source_name: None,
        min_score: Some(0.0),
        min_should_match: Some(1),
    }
}

/// **Fails for:** any v2 search statement that ClickHouse refuses to execute —
/// an outer reference to a column the inner derived table does not project, an
/// aggregate alias filtered in the `HAVING` of the statement that defines it,
/// an `argMax`/`uniqExact` used where the server rejects it, a window function
/// in an unsupported position. None of those are visible to a shape test.
///
/// It also fails for a statement that executes but answers nothing: every arm
/// asserts the rows it must return, so a builder that quietly selects an empty
/// relation cannot pass.
///
/// **UNRUN.** This body has never been executed against a server — the agent
/// that wrote it cannot boot a sandbox. Treat every expectation below as a
/// claim to be checked by `scripts/dev/sandbox/run-live-test bounded-search-exec`,
/// not as evidence.
pub(super) async fn statement_execution() -> Result<()> {
    with_owned_live_db(
        "bounded-search execution gate",
        |clickhouse, database| async move {
            clickhouse
                .run_migrations()
                .await
                .context("failed to migrate the bounded-search execution database")?;
            seed_events(&clickhouse, &execution_corpus()).await?;
            publish_and_promote(&clickhouse, &database).await?;

            let repo = repository(&clickhouse, None);
            if !repo.canonical_reader_ready().await {
                bail!(
                    "the canonical path must be ready before this gate can \
                     mean anything: an unready store refuses every arm typed, \
                     and a gate that measures refusals measures nothing"
                );
            }
            let scoped = repository(&clickhouse, Some(&[SCOPE_ROOT]));

            // --- (a) unscoped MCP search: ranking + candidate derivation +
            //         dedup keys + turn aggregates + winner hydration.
            let unscoped = QueryEnvelope::new(
                "issue597-v2search-unscoped",
                QueryClass::Interactive,
                &default_interactive_budget(),
            )
            .scope(repo.search_mcp_events(mcp_query(TERM)))
            .await
            .map_err(|error| anyhow!("unscoped v2 search failed: {error:#}"))?;
            let unscoped_sessions: BTreeSet<&str> = unscoped
                .hits
                .iter()
                .map(|hit| hit.session_id.as_str())
                .collect();
            if unscoped_sessions
                != BTreeSet::from(["bs-alpha", "bs-beta", "bs-outside"])
            {
                bail!(
                    "unscoped search must rank every seeded session; got {:?}",
                    unscoped_sessions
                );
            }
            // Winner hydration really produced the wide fields, so the phase
            // ran rather than being skipped for an empty winner set.
            if unscoped.hits.iter().any(|hit| hit.snippet.trim().is_empty()) {
                bail!("winner hydration returned no text for at least one hit");
            }
            if unscoped.hits.iter().any(|hit| hit.turn_event_count == 0) {
                bail!("the per-turn aggregate returned no turn for at least one hit");
            }

            // --- (b) session scope: adds the directory point read.
            let session_scoped = QueryEnvelope::new(
                "issue597-v2search-session",
                QueryClass::Interactive,
                &default_interactive_budget(),
            )
            .scope(repo.search_mcp_events(SearchMcpEventsQuery {
                session_id: Some("bs-alpha".to_string()),
                ..mcp_query(TERM)
            }))
            .await
            .map_err(|error| anyhow!("session-scoped v2 search failed: {error:#}"))?;
            if !session_scoped.scope_exists {
                bail!("a seeded session must exist");
            }
            if session_scoped.hits.is_empty()
                || session_scoped
                    .hits
                    .iter()
                    .any(|hit| hit.session_id != "bs-alpha")
            {
                bail!(
                    "session scope must return that session's hits and no other: {:?}",
                    session_scoped
                        .hits
                        .iter()
                        .map(|hit| hit.session_id.as_str())
                        .collect::<Vec<_>>()
                );
            }

            // --- (c) turn scope: adds the navigation uid derivation.
            let turn_scoped = QueryEnvelope::new(
                "issue597-v2search-turn",
                QueryClass::Interactive,
                &default_interactive_budget(),
            )
            .scope(repo.search_mcp_events(SearchMcpEventsQuery {
                session_id: Some("bs-alpha".to_string()),
                turn_seq: Some(2),
                ..mcp_query(TERM)
            }))
            .await
            .map_err(|error| anyhow!("turn-scoped v2 search failed: {error:#}"))?;
            if !turn_scoped.scope_exists {
                bail!("turn 2 of bs-alpha must exist");
            }
            if turn_scoped.hits.is_empty() || turn_scoped.hits.iter().any(|hit| hit.turn_seq != 2) {
                bail!(
                    "turn scope must return turn 2 and nothing else: {:?}",
                    turn_scoped
                        .hits
                        .iter()
                        .map(|hit| hit.turn_seq)
                        .collect::<Vec<_>>()
                );
            }

            // --- (d) a turn that does not exist derives no uids, which is how
            //         the tool answers `not_found` rather than "zero hits".
            let missing_turn = QueryEnvelope::new(
                "issue597-v2search-noturn",
                QueryClass::Interactive,
                &default_interactive_budget(),
            )
            .scope(repo.search_mcp_events(SearchMcpEventsQuery {
                session_id: Some("bs-alpha".to_string()),
                turn_seq: Some(99),
                ..mcp_query(TERM)
            }))
            .await
            .map_err(|error| anyhow!("absent-turn v2 search failed: {error:#}"))?;
            if missing_turn.scope_exists || !missing_turn.hits.is_empty() {
                bail!("turn 99 does not exist and must answer scope_exists = false");
            }

            // --- (e) project scope, session door: the directory point read's
            //         `argMinIfMerge(origin_cwd_state)` branch, plus the
            //         ranking recall subquery and the exact re-check.
            let scoped_in = QueryEnvelope::new(
                "issue597-v2search-scopein",
                QueryClass::Interactive,
                &default_interactive_budget(),
            )
            .scope(scoped.search_mcp_events(SearchMcpEventsQuery {
                session_id: Some("bs-alpha".to_string()),
                ..mcp_query(TERM)
            }))
            .await
            .map_err(|error| anyhow!("in-scope v2 search failed: {error:#}"))?;
            if !scoped_in.scope_exists || scoped_in.hits.is_empty() {
                bail!("an in-scope session must exist and rank");
            }

            let scoped_out = QueryEnvelope::new(
                "issue597-v2search-scopeout",
                QueryClass::Interactive,
                &default_interactive_budget(),
            )
            .scope(scoped.search_mcp_events(SearchMcpEventsQuery {
                session_id: Some("bs-outside".to_string()),
                ..mcp_query(TERM)
            }))
            .await
            .map_err(|error| anyhow!("out-of-scope v2 search failed: {error:#}"))?;
            if scoped_out.scope_exists {
                bail!(
                    "an out-of-scope session must be indistinguishable from one \
                     that never existed"
                );
            }

            // --- (f) project scope, turn door: the `session_origin_cwd`
            //         WITH-scalar branch of the uid derivation.
            let scoped_turn = QueryEnvelope::new(
                "issue597-v2search-scopeturn",
                QueryClass::Interactive,
                &default_interactive_budget(),
            )
            .scope(scoped.search_mcp_events(SearchMcpEventsQuery {
                session_id: Some("bs-outside".to_string()),
                turn_seq: Some(1),
                ..mcp_query(TERM)
            }))
            .await
            .map_err(|error| anyhow!("out-of-scope turn v2 search failed: {error:#}"))?;
            if scoped_turn.scope_exists {
                bail!("an out-of-scope session's turn must derive no uids");
            }

            // --- (g) unscoped search: every ranking hit must be in scope.
            let scoped_global = QueryEnvelope::new(
                "issue597-v2search-scopeglobal",
                QueryClass::Interactive,
                &default_interactive_budget(),
            )
            .scope(scoped.search_mcp_events(mcp_query(TERM)))
            .await
            .map_err(|error| anyhow!("scoped global v2 search failed: {error:#}"))?;
            if scoped_global.hits.is_empty() {
                bail!("a scoped global search must still rank the in-scope sessions");
            }
            if scoped_global
                .hits
                .iter()
                .any(|hit| hit.session_id == "bs-outside")
            {
                bail!("the exact project-scope re-check must drop the out-of-scope session");
            }

            // --- (h) harness / source filters: the remaining ranking WHERE
            //         conjuncts.
            let filtered = QueryEnvelope::new(
                "issue597-v2search-filters",
                QueryClass::Interactive,
                &default_interactive_budget(),
            )
            .scope(repo.search_mcp_events(SearchMcpEventsQuery {
                harness: Some("claude".to_string()),
                source_name: Some("claude".to_string()),
                ..mcp_query(TERM)
            }))
            .await
            .map_err(|error| anyhow!("filtered v2 search failed: {error:#}"))?;
            if filtered.hits.iter().any(|hit| hit.session_id != "bs-beta") {
                bail!("the harness/source filters must narrow to the claude-sourced event");
            }
            if filtered.hits.is_empty() {
                bail!("the harness/source filters must not narrow to nothing");
            }

            // --- (i) search_events: the second projection over the shared
            //         ranking CTEs.
            let events = QueryEnvelope::new(
                "issue597-v2search-events",
                QueryClass::Interactive,
                &default_interactive_budget(),
            )
            .scope(repo.search_events(SearchEventsQuery {
                query: TERM.to_string(),
                limit: Some(10),
                min_score: Some(0.0),
                min_should_match: Some(1),
                include_tool_events: Some(true),
                bypass_cache: Some(true),
                ..SearchEventsQuery::default()
            }))
            .await
            .map_err(|error| anyhow!("search_events failed: {error:#}"))?;
            if events.hits.is_empty() {
                bail!("search_events must rank the seeded corpus");
            }
            if events.stats.docs == 0 {
                bail!("search_events must report a non-empty corpus");
            }

            // --- (j) search_conversations: the term-df read, the two
            //         candidate statements, the scoring statement and the
            //         snippet read — and the ONLY caller that passes
            //         `extra_posting_columns`, so this is the arm that
            //         executes that variant of the shared fragment.
            let conversations = QueryEnvelope::new(
                "issue597-v2search-conversations",
                QueryClass::Interactive,
                &default_interactive_budget(),
            )
            .scope(repo.search_conversations(ConversationSearchQuery {
                query: TERM.to_string(),
                limit: Some(10),
                min_score: Some(0.0),
                min_should_match: Some(1),
                from_unix_ms: None,
                to_unix_ms: None,
                mode: None,
                include_tool_events: Some(true),
                exclude_codex_mcp: Some(false),
            }))
            .await
            .map_err(|error| anyhow!("search_conversations failed: {error:#}"))?;
            if conversations.hits.is_empty() {
                bail!("search_conversations must rank the seeded corpus");
            }

            // --- query-log assertions ---------------------------------------
            clickhouse
                .request_text("SYSTEM FLUSH LOGS", None, None, false, None)
                .await
                .context("failed to flush the query log")?;
            let database_name = database.as_str();

            for arm in [
                "moraine-issue597-v2search-unscoped-",
                "moraine-issue597-v2search-session-",
                "moraine-issue597-v2search-turn-",
                "moraine-issue597-v2search-scopein-",
                "moraine-issue597-v2search-scopeglobal-",
                "moraine-issue597-v2search-events-",
                "moraine-issue597-v2search-conversations-",
            ] {
                // The #598 exit gate: no projected relation on the v2 path.
                let projection = log_hits(&clickhouse, database_name, arm, "mcp_open_").await?;
                if projection.total == 0 {
                    bail!("{arm} logged no statements at all; the gate would pass vacuously");
                }
                if projection.matches != 0 {
                    bail!(
                        "{arm} ran {} statement(s) referencing a projected \
                         relation; the v2 search path must not open one",
                        projection.matches
                    );
                }
                // Correction C2: corpus scalars come from ONE statement over
                // ONE relation. A statement that aggregates `total_doc_len`
                // AND opens the locator is the O(D)x O(E) semi-join the design
                // decided against, back on the interactive path.
                let corpus_join =
                    log_hits(&clickhouse, database_name, arm, "AS total_doc_len").await?;
                if corpus_join.matches > 0 {
                    let locator_stats = clickhouse
                        .query_rows::<CountRow>(
                            &format!(
                                "SELECT toUInt64(countIf(
    positionCaseInsensitive(query, 'AS total_doc_len') > 0
    AND positionCaseInsensitive(query, 'mcp_event_locator') > 0
  )) AS value
FROM system.query_log
WHERE type != 'QueryStart'
  AND current_database = currentDatabase()
  AND startsWith(query_id, '{arm}')
FORMAT JSONEachRow"
                            ),
                            Some(database_name),
                        )
                        .await
                        .context("corpus-statement probe failed")?
                        .into_iter()
                        .next()
                        .context("corpus-statement probe returned no row")?
                        .value;
                    if locator_stats != 0 {
                        bail!(
                            "{arm} computed corpus scalars over a second \
                             corpus-sized relation ({locator_stats} statement(s))"
                        );
                    }
                }
            }

            // Non-vacuity: the ranking arm really did open the two relations
            // the design says candidate selection is built from.
            for needle in ["search_postings", "mcp_event_locator"] {
                let hits = log_hits(
                    &clickhouse,
                    database_name,
                    "moraine-issue597-v2search-unscoped-",
                    needle,
                )
                .await?;
                if hits.matches == 0 {
                    bail!("the ranking arm never opened {needle}; the fixture is not exercising the bounded path");
                }
            }

            Ok(())
        },
    )
    .await
}

// ---------------------------------------------------------------------------
// Gate 2: bounded-search-attribution
// ---------------------------------------------------------------------------

/// The uid that ingest attributes to two sessions.
const SHARED_UID: &str = "bs-shared-zephyr-a1";
/// The one physical line both attributions describe. Both `Ev`s must agree on
/// EVERY source coordinate — `event_uid` is content-addressed over exactly
/// those fields, which is why one line can carry one uid under two sessions.
const SHARED_FILE: &str = "/fixtures/bs-shared.jsonl";

/// The LOSING attribution's `event_version`, and the WINNING one's.
///
/// Production shape, and it is load-bearing. `event_version` is wall-clock
/// millis at emit time (`moraine-ingest-core`, `sources/shared.rs`), a single
/// normalized batch cannot emit one uid twice, and the documented production
/// shape of a double attribution is a pair ingested ~26 s apart with different
/// session resolution (#608). Two attributions therefore carry DIFFERENT
/// versions, and every replacement in the read model —
/// `search_documents(doc_version)`, `search_postings(post_version)`,
/// `mcp_event_locator(event_version)` — resolves to the later one.
///
/// A fixture that left both at `Ev`'s default `event_version: 1` would model
/// nothing: equal-version `ReplacingMergeTree` rows are collapsed arbitrarily,
/// so such a corpus is not merely unrealistic, it is unresolvable.
const LOSING_VERSION: u64 = 1_700_000_000_000;
const WINNING_VERSION: u64 = 1_700_000_026_000;
/// The session the winning attribution names — the one every replacement in the
/// read model resolves the shared uid to.
const WINNING_SESSION: &str = "bs-right";
/// The session the losing attribution names. On a real server this session
/// cannot find the shared line through search; see [`double_attribution`].
const LOSING_SESSION: &str = "bs-left";

/// One physical line, two sessions (#608), plus a control session so the
/// corpus is not degenerate.
fn attribution_corpus() -> Vec<Ev> {
    vec![
        // Both attributions: identical uid, identical source coordinates
        // (`Ev` derives `source_line_no`/`source_offset` from the seconds
        // argument, so the shared `40` is what makes them the same line),
        // different session, and — production shape — different
        // `event_version`, 26 s apart.
        Ev::new(LOSING_SESSION, SHARED_UID, 40)
            .source("fixture", SHARED_FILE)
            .version(LOSING_VERSION),
        Ev::new(WINNING_SESSION, SHARED_UID, 40)
            .source("fixture", SHARED_FILE)
            .version(WINNING_VERSION),
        // Each session also carries a user message so its turn derivation is
        // well formed and the hit lands in turn 1 of its OWN session.
        Ev::new(LOSING_SESSION, "bs-left-open-u1", 39)
            .user()
            .source("fixture", SHARED_FILE)
            .version(LOSING_VERSION),
        Ev::new(WINNING_SESSION, "bs-right-open-u1", 39)
            .user()
            .source("fixture", SHARED_FILE)
            .version(WINNING_VERSION),
        // Control: a second document containing the term, so `df` is 2 and a
        // corpus of one document cannot make the assertions trivial.
        Ev::new("bs-control", "bs-control-zephyr-a1", 50),
        Ev::new("bs-control", "bs-control-open-u1", 49).user(),
    ]
}

/// **Fails for:** ranking that is not document-grained — a per-attribution
/// collapse over `search_postings`, a weakened locator version join, or a `df`
/// that stops being an exact document count.
///
/// This gate replaces the one written for correction C1, which asserted the
/// opposite contract on a premise that is false (ledger D1). It is in five
/// parts, and the first four are what make the fifth meaningful:
///
/// 1. **The corpus really is double-attributed.** Canonical `events` is
///    `ORDER BY (session_id, event_ts, …, event_uid, source_host)` with
///    `session_id` LEADING, so it is the one relation that durably holds both
///    attributions. Two rows, one per session, one uid. If ingest ever stops
///    producing that shape this fails and the rest of the gate is known to be
///    vacuous.
/// 2. **The version join alone reduces it to one document, pre-merge.** Reading
///    `search_postings` WITHOUT `FINAL` and joining `mcp_event_locator FINAL`
///    on `event_version = post_version` returns ONE row, whether or not a
///    background merge has run — because the locator holds exactly one row per
///    `(event_uid, source_host)` at `max(event_version)`, and the losing
///    attribution's `post_version` is not that value. This is the assertion
///    that retires the in-query per-attribution collapse: the collapse cannot
///    recover a row the join is about to drop.
/// 3. **And the index cannot hold two anyway, durably.** `OPTIMIZE … FINAL`
///    forces the background merge, and afterwards the PHYSICAL tables hold one
///    row each: `search_documents` and `mcp_event_locator` are
///    `ORDER BY (event_uid, source_host)`, `search_postings` is
///    `ORDER BY (term, doc_id, source_host)`, and `session_id` is in none of
///    those keys. `ReplacingMergeTree` deduplicates on the sort key at STORAGE
///    time; a second row is an artifact of unmerged parts, never a state a
///    query may be built on.
/// 4. **`df`'s `count()` is exact.** Over `search_postings FINAL` joined to the
///    locator, `count()` and `uniqExact(tuple(doc_id, source_host))` agree per
///    term. That equality is the invariant the reverted `df` rests on; the
///    server is the only place it can be checked.
/// 5. **Ranking is document-grained end to end.** One hit for the shared uid,
///    under the WINNING session, and a session-scoped search of that session
///    finds it. A scoped search of the LOSING session does NOT — stated as the
///    contract rather than discovered later as a defect. That loss is an INGEST
///    bug owned by #608 (one of the two session ids is simply wrong); the read
///    model must not mirror it.
///
/// **UNRUN.** Never executed against a server; run
/// `scripts/dev/sandbox/run-live-test bounded-search-attribution`.
pub(super) async fn double_attribution() -> Result<()> {
    with_owned_live_db(
        "bounded-search double-attribution gate",
        |clickhouse, database| async move {
            clickhouse
                .run_migrations()
                .await
                .context("failed to migrate the bounded-search attribution database")?;
            seed_events(&clickhouse, &attribution_corpus()).await?;
            publish_and_promote(&clickhouse, &database).await?;

            // --- (1) the fixture premise, in the one relation that keeps it --
            let canonical = scalar_count(
                &clickhouse,
                &format!(
                    "SELECT toUInt64(count()) AS value FROM (
  SELECT session_id
  FROM events
  WHERE event_uid = '{SHARED_UID}'
  GROUP BY session_id
) FORMAT JSONEachRow"
                ),
            )
            .await?;
            if canonical != 2 {
                bail!(
                    "the fixture must attribute ONE uid to TWO sessions in \
                     canonical `events`, got {canonical}; without that this \
                     gate proves nothing"
                );
            }

            // --- (2) the version join closes the pre-merge window ------------
            //
            // Deliberately WITHOUT `FINAL` on the postings scan: this is the
            // count an attribution-keyed collapse would have been reading, and
            // it is already 1.
            let joined = scalar_count(
                &clickhouse,
                &format!(
                    "SELECT toUInt64(count()) AS value
FROM search_postings AS p
ALL INNER JOIN (
  SELECT l.event_uid AS event_uid, l.source_host AS source_host,
         l.event_version AS event_version
  FROM mcp_event_locator AS l FINAL
) AS l
  ON l.event_uid = p.doc_id
 AND l.source_host = p.source_host
 AND l.event_version = p.post_version
WHERE p.term = '{TERM}' AND p.doc_id = '{SHARED_UID}'
FORMAT JSONEachRow"
                ),
            )
            .await?;
            if joined != 1 {
                bail!(
                    "the locator version join must reduce a double-attributed \
                     uid to ONE posting even before a merge runs, got {joined}; \
                     if that is no longer true, re-derive the ranking shape \
                     rather than trusting this gate"
                );
            }

            // …and it kept the LATER attribution, which is the one every other
            // replacement in the read model resolves to.
            let winning: Vec<AttributionRow> = clickhouse
                .query_rows(
                    &format!(
                        "SELECT p.session_id AS session_id
FROM search_postings AS p
ALL INNER JOIN (
  SELECT l.event_uid AS event_uid, l.source_host AS source_host,
         l.event_version AS event_version
  FROM mcp_event_locator AS l FINAL
) AS l
  ON l.event_uid = p.doc_id
 AND l.source_host = p.source_host
 AND l.event_version = p.post_version
WHERE p.term = '{TERM}' AND p.doc_id = '{SHARED_UID}'
FORMAT JSONEachRow"
                    ),
                    None,
                )
                .await
                .context("winning-attribution probe failed")?;
            match winning.first() {
                Some(row) if row.session_id == WINNING_SESSION => {}
                other => bail!(
                    "the surviving posting must name the later attribution \
                     ({WINNING_SESSION}); got {other:?}"
                ),
            }

            // --- (3) the durable state, after forcing the merge --------------
            for table in ["search_documents", "search_postings", "mcp_event_locator"] {
                clickhouse
                    .request_text(
                        &format!("OPTIMIZE TABLE `{}`.`{table}` FINAL", database.as_str()),
                        None,
                        Some(database.as_str()),
                        false,
                        None,
                    )
                    .await
                    .with_context(|| format!("failed to OPTIMIZE {table} FINAL"))?;
            }
            for (table, sql) in [
                (
                    "search_postings",
                    format!(
                        "SELECT toUInt64(count()) AS value FROM search_postings \
                         WHERE term = '{TERM}' AND doc_id = '{SHARED_UID}' \
                         FORMAT JSONEachRow"
                    ),
                ),
                (
                    "search_documents",
                    format!(
                        "SELECT toUInt64(count()) AS value FROM search_documents \
                         WHERE event_uid = '{SHARED_UID}' FORMAT JSONEachRow"
                    ),
                ),
                (
                    "mcp_event_locator",
                    format!(
                        "SELECT toUInt64(count()) AS value FROM mcp_event_locator \
                         WHERE event_uid = '{SHARED_UID}' FORMAT JSONEachRow"
                    ),
                ),
            ] {
                let physical = scalar_count(&clickhouse, &sql).await?;
                if physical != 1 {
                    bail!(
                        "`{table}` holds {physical} physical rows for a \
                         double-attributed uid after OPTIMIZE FINAL. The whole \
                         document-grain argument is that it holds 1, because \
                         `session_id` is not in its sort key — if that is no \
                         longer true, re-derive the design rather than \
                         trusting this gate"
                    );
                }
            }

            // --- (4) `df`'s count() is an exact document count ---------------
            let df_rows = scalar_count(
                &clickhouse,
                &format!(
                    "SELECT toUInt64(count()) AS value
FROM (SELECT * FROM search_postings FINAL) AS p
ALL INNER JOIN (
  SELECT l.event_uid AS event_uid, l.source_host AS source_host,
         l.event_version AS event_version
  FROM mcp_event_locator AS l FINAL
) AS l
  ON l.event_uid = p.doc_id
 AND l.source_host = p.source_host
 AND l.event_version = p.post_version
WHERE p.term = '{TERM}'
FORMAT JSONEachRow"
                ),
            )
            .await?;
            let df_documents = scalar_count(
                &clickhouse,
                &format!(
                    "SELECT toUInt64(uniqExact(tuple(p.doc_id, p.source_host))) AS value
FROM (SELECT * FROM search_postings FINAL) AS p
ALL INNER JOIN (
  SELECT l.event_uid AS event_uid, l.source_host AS source_host,
         l.event_version AS event_version
  FROM mcp_event_locator AS l FINAL
) AS l
  ON l.event_uid = p.doc_id
 AND l.source_host = p.source_host
 AND l.event_version = p.post_version
WHERE p.term = '{TERM}'
FORMAT JSONEachRow"
                ),
            )
            .await?;
            if df_rows != df_documents {
                bail!(
                    "`count()` ({df_rows}) and \
                     `uniqExact(tuple(doc_id, source_host))` ({df_documents}) \
                     must agree on the ranking relation — that equality is the \
                     only reason `df` may use the O(1) accumulator"
                );
            }
            if df_documents != 2 {
                bail!(
                    "the corpus must hold TWO live documents containing the \
                     term (the shared line and the control), got \
                     {df_documents}; a one-document corpus makes the equality \
                     above trivial"
                );
            }

            // --- (5) the ranking pass ---------------------------------------
            let repo = repository(&clickhouse, None);
            if !repo.canonical_reader_ready().await {
                bail!("the canonical path must be selected before this gate can mean anything");
            }

            let unscoped = QueryEnvelope::new(
                "issue597-attribution-unscoped",
                QueryClass::Interactive,
                &default_interactive_budget(),
            )
            .scope(repo.search_mcp_events(mcp_query(TERM)))
            .await
            .map_err(|error| anyhow!("unscoped attribution search failed: {error:#}"))?;
            let shared: Vec<&str> = unscoped
                .hits
                .iter()
                .filter(|hit| hit.event_uid == SHARED_UID)
                .map(|hit| hit.session_id.as_str())
                .collect();
            if shared != vec![WINNING_SESSION] {
                bail!(
                    "one physical line is ONE document and must rank ONCE, \
                     under the attribution the locator authorizes \
                     ({WINNING_SESSION}); got {shared:?}"
                );
            }

            // The winning session finds its own line…
            let scoped_hit = QueryEnvelope::new(
                "issue597-attribution-winning",
                QueryClass::Interactive,
                &default_interactive_budget(),
            )
            .scope(repo.search_mcp_events(SearchMcpEventsQuery {
                session_id: Some(WINNING_SESSION.to_string()),
                ..mcp_query(TERM)
            }))
            .await
            .map_err(|error| anyhow!("{WINNING_SESSION}-scoped search failed: {error:#}"))?;
            if !scoped_hit
                .hits
                .iter()
                .any(|hit| hit.event_uid == SHARED_UID && hit.session_id == WINNING_SESSION)
            {
                bail!("a session-scoped search of {WINNING_SESSION} must find the line it owns");
            }

            // …and the losing session does not. This is the CONTRACT, not a
            // regression: the read model is document-grained, one of the two
            // session ids is an ingest defect (#608), and mirroring it in the
            // index would fork `df`/`docs` into two corpora over one physical
            // line and let search resolve the uid to a different session than
            // `open` does. ~1% of the reference corpus is affected.
            let losing = QueryEnvelope::new(
                "issue597-attribution-losing",
                QueryClass::Interactive,
                &default_interactive_budget(),
            )
            .scope(repo.search_mcp_events(SearchMcpEventsQuery {
                session_id: Some(LOSING_SESSION.to_string()),
                ..mcp_query(TERM)
            }))
            .await
            .map_err(|error| anyhow!("{LOSING_SESSION}-scoped search failed: {error:#}"))?;
            if losing.hits.iter().any(|hit| hit.event_uid == SHARED_UID) {
                bail!(
                    "a scoped search of the LOSING attribution returned the \
                     shared uid. That would mean the index is holding two \
                     attributions of one document — re-derive the design (and \
                     `df`, which assumes it does not) rather than relaxing \
                     this assertion"
                );
            }

            Ok(())
        },
    )
    .await
}
