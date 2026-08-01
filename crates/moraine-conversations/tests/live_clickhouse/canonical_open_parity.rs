//! Issue #598 WI-10 exit-gate live tests over the canonical `open` reader
//! (design-598-final §5, `canonical_open.rs`). Until issue #603 WI-10 retired
//! the v1 projector, the parity gate byte-diffed the two readers; the v1
//! oracle is gone with the projection, so `parity` now proves the properties
//! the diff certified that still have an oracle: page-size independence (the
//! same corpus read under multi-page traversal equals the single-page read),
//! pinned derivation values (turn_id, metadata precedence, sentinel
//! ordering), and origin-scope visibility. The other gates are unchanged:
//! `open(event)` rejects non-live generations via `mcp_event_locator`, cursor
//! continuation is correct under concurrent append-only mutation and the
//! anchor boundary guard, and an append-fenced source (#602) serves the
//! pinned pre-fence state without a spurious reopen while a genuine revision
//! move returns the structured reopen (BINDING D9).
//!
//! The root `live_clickhouse.rs` owns the `#[tokio::test]` wrappers (so the
//! `run-live-test` `--exact` paths stay flat) and scopes each body under the
//! shared live-fixture Migration envelope; operations under test build their
//! own Interactive-class envelopes exactly the way the MCP boundary does.
//!
//! Each gate is registered as its own `run-live-test` mode:
//!   * `canonical-open-parity`       -> [`parity`]
//!   * `canonical-open-locator`      -> [`locator`]
//!   * `canonical-open-continuation` -> [`continuation`]
//!   * `canonical-open-fence`        -> [`fence`]

use super::*;
use moraine_config::{QueryBudgetsConfig, ValidatedQueryBudgets};
use moraine_conversations::{
    CanonicalContinuation, CanonicalReadOutcome, ConversationRepository, McpSessionOpen,
    McpTurnOpen, RepoError, SessionOriginScope,
};
use serde_json::Value;

// --- fixture-building primitives -------------------------------------------

const HOST_A: &str = "canonical-open-host-a";
const HOST_B: &str = "canonical-open-host-b";
const SESSION_DATE: &str = "2026-07-20";

/// An ISO-8601 `record_ts` string (the raw source timestamp) at
/// `12:mm:ss.000Z` on the fixture date; `secs` is total seconds past 12:00.
fn iso(secs: u32) -> String {
    format!("2026-07-20T12:{:02}:{:02}.000Z", secs / 60 % 60, secs % 60)
}

/// The ClickHouse `DateTime64(3)` literal for the same instant, used for
/// `event_ts` / `ingested_at`.
fn dt(secs: u32) -> String {
    format!("2026-07-20 12:{:02}:{:02}.000", secs / 60 % 60, secs % 60)
}

/// A single fixture `events` row. Defaults model a well-formed assistant text
/// message; fixtures override the fields that matter to the case under test.
/// Every field the navigation / directory / locator MVs read is representable
/// here, so the corpus is seeded purely through `events` + publication control
/// (never by writing the derived tables directly).
///
/// Shared with the issue-599 session-list gates
/// ([`session_list_parity`](super::session_list_parity)): both corpora are
/// seeded the same way, and one row DSL is what keeps a fixture written for
/// one gate meaningful to the other.
#[derive(Clone)]
pub(super) struct Ev {
    session_id: String,
    source_host: String,
    source_name: String,
    source_file: String,
    source_generation: u32,
    source_offset: u64,
    source_line_no: u64,
    event_uid: String,
    record_ts: String,
    event_ts: String,
    ingested_at: String,
    event_kind: String,
    actor_kind: String,
    payload_type: String,
    tool_name: String,
    tool_call_id: String,
    item_id: String,
    op_status: String,
    inference_provider: String,
    harness: String,
    cwd: String,
    turn_index: u32,
    text_content: String,
    payload_json: String,
    event_version: u64,
}

impl Ev {
    pub(super) fn new(session_id: &str, event_uid: &str, secs: u32) -> Self {
        Self {
            session_id: session_id.to_string(),
            source_host: HOST_A.to_string(),
            source_name: "fixture".to_string(),
            source_file: format!("/fixtures/{session_id}.jsonl"),
            source_generation: 1,
            source_offset: u64::from(secs) * 4096,
            source_line_no: u64::from(secs) + 1,
            event_uid: event_uid.to_string(),
            record_ts: iso(secs),
            event_ts: dt(secs),
            ingested_at: dt(secs),
            event_kind: "message".to_string(),
            actor_kind: "assistant".to_string(),
            payload_type: "message".to_string(),
            tool_name: String::new(),
            tool_call_id: String::new(),
            item_id: String::new(),
            op_status: String::new(),
            inference_provider: "openai".to_string(),
            harness: "codex".to_string(),
            cwd: "/repo".to_string(),
            turn_index: 0,
            text_content: format!("assistant text for {event_uid}"),
            payload_json: "{}".to_string(),
            event_version: 1,
        }
    }

    pub(super) fn user(mut self) -> Self {
        self.actor_kind = "user".to_string();
        self.payload_type = "message".to_string();
        self.text_content = format!("user prompt for {}", self.event_uid);
        self
    }

    pub(super) fn tool_call(mut self, tool: &str, call_id: &str) -> Self {
        self.event_kind = "tool_call".to_string();
        self.actor_kind = "assistant".to_string();
        self.payload_type = "function_call".to_string();
        self.tool_name = tool.to_string();
        self.tool_call_id = call_id.to_string();
        self.text_content = String::new();
        self
    }

    pub(super) fn turn(mut self, turn_index: u32) -> Self {
        self.turn_index = turn_index;
        self
    }

    pub(super) fn host(mut self, host: &str) -> Self {
        self.source_host = host.to_string();
        self
    }

    pub(super) fn generation(mut self, generation: u32) -> Self {
        self.source_generation = generation;
        self
    }

    pub(super) fn record_ts(mut self, raw: &str) -> Self {
        self.record_ts = raw.to_string();
        self
    }

    pub(super) fn event_ts(mut self, raw: &str) -> Self {
        self.event_ts = raw.to_string();
        self
    }

    pub(super) fn ingested_at_secs(mut self, secs: u32) -> Self {
        self.ingested_at = dt(secs);
        self
    }

    pub(super) fn version(mut self, version: u64) -> Self {
        self.event_version = version;
        self
    }

    pub(super) fn cwd(mut self, cwd: &str) -> Self {
        self.cwd = cwd.to_string();
        self
    }

    pub(super) fn source(mut self, name: &str, file: &str) -> Self {
        self.source_name = name.to_string();
        self.source_file = file.to_string();
        self
    }

    pub(super) fn payload(mut self, kind: &str, payload_json: &str) -> Self {
        self.event_kind = kind.to_string();
        self.payload_json = payload_json.to_string();
        self
    }

    pub(super) fn payload_type(mut self, payload_type: &str) -> Self {
        self.payload_type = payload_type.to_string();
        self
    }

    pub(super) fn harness(mut self, harness: &str) -> Self {
        self.harness = harness.to_string();
        self
    }

    pub(super) fn provider(mut self, inference_provider: &str) -> Self {
        self.inference_provider = inference_provider.to_string();
        self
    }

    pub(super) fn value(&self) -> Value {
        serde_json::json!({
            "ingested_at": self.ingested_at,
            "source_host": self.source_host,
            "event_uid": self.event_uid,
            "session_id": self.session_id,
            "session_date": SESSION_DATE,
            "source_name": self.source_name,
            "harness": self.harness,
            "inference_provider": self.inference_provider,
            "source_file": self.source_file,
            "source_generation": self.source_generation,
            "source_line_no": self.source_line_no,
            "source_offset": self.source_offset,
            "source_ref": self.event_uid,
            "record_ts": self.record_ts,
            "event_ts": self.event_ts,
            "event_kind": self.event_kind,
            "actor_kind": self.actor_kind,
            "payload_type": self.payload_type,
            "op_status": self.op_status,
            "turn_index": self.turn_index,
            "item_id": self.item_id,
            "tool_call_id": self.tool_call_id,
            "tool_name": self.tool_name,
            "cwd": self.cwd,
            "text_content": self.text_content,
            "payload_json": self.payload_json,
            "event_version": self.event_version,
        })
    }
}

/// Insert a batch of fixture events synchronously (each raw statement rides the
/// ambient live-fixture envelope).
pub(super) async fn seed_events(clickhouse: &ClickHouseClient, rows: &[Ev]) -> Result<()> {
    let values: Vec<Value> = rows.iter().map(Ev::value).collect();
    clickhouse
        .insert_json_rows_sync("events", &values)
        .await
        // `{error:#}` so the ClickHouse message survives: a bare context line
        // hides which column or value the insert actually rejected.
        .map_err(|error| anyhow::anyhow!("failed to seed canonical-open fixture events: {error:#}"))
}

fn admin_budget() -> moraine_config::ValidatedQueryBudget {
    ValidatedQueryBudgets::from_config(&QueryBudgetsConfig::default())
        .expect("bundled default query budgets are valid")
        .administrative
}

/// Publish every not-yet-published source head, then backfill the canonical
/// read indexes (migration-036 directory/locator/navigation, driving the
/// `canonical_*` readers — the only `open` read model since issue #603 WI-10).
async fn publish_and_backfill(
    clickhouse: &ClickHouseClient,
    database: &OwnedDatabaseName,
) -> Result<()> {
    publish_missing_schema_fixture_sources(clickhouse, database).await?;
    clickhouse
        .backfill_canonical_read_indexes(true, &live_fixture_budget(), &admin_budget(), |_| {})
        .await
        .context("failed to backfill the v2 canonical read indexes")?;
    Ok(())
}

// --- v2 page accumulation ---------------------------------------------------

/// Drive `canonical_open_session_page` across full pagination and merge the
/// per-page turns into one `McpSessionOpen`, exactly the way the tool layer
/// reconstructs a full session for a parity diff. A `Reopen` during a quiescent
/// traversal is a failure.
async fn open_session_v2(
    repository: &ClickHouseConversationRepository,
    session_id: &str,
    limit: u16,
) -> Result<Option<McpSessionOpen>> {
    let mut after: Option<CanonicalContinuation> = None;
    let mut merged: Option<McpSessionOpen> = None;
    loop {
        let outcome = repository
            .canonical_open_session_page(session_id, limit, after.clone())
            .await
            .with_context(|| format!("v2 session page read failed for {session_id}"))?;
        let page = match outcome {
            None => return Ok(None),
            Some(CanonicalReadOutcome::Reopen) => {
                bail!("unexpected reopen during quiescent v2 traversal of {session_id}")
            }
            Some(CanonicalReadOutcome::Page(page)) => page,
        };
        let continuation = page.continuation.clone();
        match merged.as_mut() {
            None => merged = Some(page.session),
            Some(accumulated) => accumulated.turns.extend(page.session.turns),
        }
        match continuation {
            Some(next) => after = Some(next),
            None => break,
        }
    }
    Ok(merged)
}

/// Drive `canonical_open_turn_page` across full pagination and merge the
/// per-page events into one `McpTurnOpen`.
async fn open_turn_v2(
    repository: &ClickHouseConversationRepository,
    session_id: &str,
    turn_seq: u32,
    limit: u16,
) -> Result<Option<McpTurnOpen>> {
    let mut after: Option<CanonicalContinuation> = None;
    let mut merged: Option<McpTurnOpen> = None;
    loop {
        let outcome = repository
            .canonical_open_turn_page(session_id, turn_seq, limit, true, after.clone())
            .await
            .with_context(|| {
                format!("v2 turn page read failed for {session_id} turn {turn_seq}")
            })?;
        let page = match outcome {
            None => return Ok(None),
            Some(CanonicalReadOutcome::Reopen) => {
                bail!("unexpected reopen during quiescent v2 turn traversal of {session_id}")
            }
            Some(CanonicalReadOutcome::Page(page)) => page,
        };
        let continuation = page.continuation.clone();
        match merged.as_mut() {
            None => merged = Some(page.turn),
            Some(accumulated) => accumulated.events.extend(page.turn.events),
        }
        match continuation {
            Some(next) => after = Some(next),
            None => break,
        }
    }
    Ok(merged)
}

// --- page-size self-parity (the post-retirement oracle) ---------------------
//
// The retired v1 reader was the original parity oracle (design-598-final LIVE
// TEST PLAN §2). What replaces it is the property the multi-page traversal
// was stressing all along: the reader's continuation must be a pure
// re-anchoring, so the SAME corpus read at a stressed page size (forcing
// multi-page traversal and anchor re-derivation on every boundary) must be
// byte-identical to the single-page read (limit 500, no continuation taken).
// A paging regression — a dropped boundary row, a re-read window, an anchor
// derived off the wrong tuple — diverges the two; concrete derivation VALUES
// (turn_id, metadata precedence, sentinel order) are pinned separately so a
// both-page-sizes-wrong drift cannot pass.
const SINGLE_PAGE_LIMIT: u16 = 500;

fn normalized_session(open: &McpSessionOpen) -> Result<Value> {
    serde_json::to_value(open).context("failed to serialize session open")
}

fn normalized_turn(open: &McpTurnOpen) -> Result<Value> {
    serde_json::to_value(open).context("failed to serialize turn open")
}

/// Assert `open(session)` at a stressed page size equals the single-page read.
async fn assert_session_parity(
    repository: &ClickHouseConversationRepository,
    session_id: &str,
    limit: u16,
) -> Result<McpSessionOpen> {
    let single = open_session_v2(repository, session_id, SINGLE_PAGE_LIMIT)
        .await?
        .with_context(|| format!("single-page session open returned None for {session_id}"))?;
    let paged = open_session_v2(repository, session_id, limit)
        .await?
        .with_context(|| format!("paged session open returned None for {session_id}"))?;
    let single_norm = normalized_session(&single)?;
    let paged_norm = normalized_session(&paged)?;
    if single_norm != paged_norm {
        bail!(
            "session-open page-size parity mismatch for {session_id}\n  single={single_norm}\n  paged={paged_norm}"
        );
    }
    Ok(paged)
}

/// Assert `open(turn)` at a stressed page size equals the single-page read.
async fn assert_turn_parity(
    repository: &ClickHouseConversationRepository,
    session_id: &str,
    turn_seq: u32,
    limit: u16,
) -> Result<()> {
    let single = open_turn_v2(repository, session_id, turn_seq, SINGLE_PAGE_LIMIT)
        .await?
        .with_context(|| {
            format!("single-page turn open returned None for {session_id} turn {turn_seq}")
        })?;
    let paged = open_turn_v2(repository, session_id, turn_seq, limit)
        .await?
        .with_context(|| {
            format!("paged turn open returned None for {session_id} turn {turn_seq}")
        })?;
    let single_norm = normalized_turn(&single)?;
    let paged_norm = normalized_turn(&paged)?;
    if single_norm != paged_norm {
        bail!(
            "turn-open page-size parity mismatch for {session_id} turn {turn_seq}\n  single={single_norm}\n  paged={paged_norm}"
        );
    }
    Ok(())
}

/// Assert `open(event)` resolves the referenced uid to its own row.
async fn assert_event_parity(
    repository: &ClickHouseConversationRepository,
    event_uid: &str,
) -> Result<()> {
    let event = repository
        .canonical_open_event(event_uid)
        .await
        .with_context(|| format!("v2 event open failed for {event_uid}"))?
        .with_context(|| format!("v2 event open returned None for {event_uid}"))?;
    if event.event.event_uid != event_uid {
        bail!(
            "event open resolved the wrong row: asked {event_uid}, got {}",
            event.event.event_uid
        );
    }
    Ok(())
}

// --- shared prep/cleanup scaffolding ---------------------------------------

pub(super) async fn with_owned_live_db<F, Fut>(phase: &str, body: F) -> Result<()>
where
    F: FnOnce(ClickHouseClient, OwnedDatabaseName) -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    let prerequisites = LivePrerequisites::load()?;
    let database = prepare_owned_database_identity(&prerequisites.sandbox_id)?;
    let clickhouse = live_client(&prerequisites, &database)?;
    assert_owned_database_census_empty(&clickhouse, &format!("before {phase}")).await?;
    let outcome = body(clickhouse.clone(), database.clone()).await;
    let cleanup = cleanup_database(&clickhouse, &database).await;
    let census = assert_owned_database_census_empty(&clickhouse, &format!("after {phase}")).await;
    finish_with_cleanup(outcome, finish_with_cleanup(cleanup, census))
}

// ===========================================================================
// Gate 1: canonical-open-parity
// ===========================================================================

/// The canonical reader over the rich fixture corpus enumerated in the LIVE
/// TEST PLAN §2.
///
/// This gate WAS a byte-parity diff of the v2 reader against the v1 projector,
/// both driven off one seeded `events` corpus. Issue #603 WI-10 deleted the
/// projector, so the oracle is gone and nothing here diffs two readers. What
/// runs is the subset of that contract with a surviving oracle, over the same
/// corpus and the same cases: page-size independence (a multi-page traversal
/// equals the single-page read), pinned derivation values, sentinel ordering,
/// and origin-scope visibility. Comments below that describe what "v1" did are
/// the record of where a pinned expectation came from.
pub(super) async fn parity() -> Result<()> {
    with_owned_live_db(
        "canonical-open parity gate",
        |clickhouse, database| async move {
            clickhouse
                .run_migrations()
                .await
                .context("failed to migrate canonical-open parity database")?;

            seed_parity_corpus(&clickhouse).await?;
            publish_and_backfill(&clickhouse, &database).await?;

            let repository =
                ClickHouseConversationRepository::new(clickhouse.clone(), RepoConfig::default());

            // (a) Empty / absent session: Ok(None) for an absent id, and a
            // blank-session row is excluded (the notEmpty(session_id) MV
            // filter).
            assert!(open_session_v2(&repository, "parity-does-not-exist", 25)
                .await?
                .is_none());
            // Blank ids are rejected at the shared validation layer
            // (`validate_session_id`) before any SQL runs — the typed
            // rejection, not Ok(None), is the contract. The blank ROW seeded
            // above is excluded from listings by the MV `notEmpty(session_id)`
            // filter, which the directory census in later cases exercises.
            assert!(matches!(
                repository.canonical_open_session_page("", 25, None).await,
                Err(RepoError::InvalidArgument(_))
            ));

            // (b) Single-turn, (c) tool-only (turn_seq floor 1), (d) multi-turn,
            // (g) multi-host ordering, (h) terminal-in-middle-turn. Full-session
            // parity plus per-turn and terminal-event parity, exercised at a page
            // size that forces multi-page traversal on the multi-turn session.
            for session_id in [
                "parity-single-turn",
                "parity-tool-only",
                "parity-multi-turn",
                "parity-multi-host",
                "parity-terminal-mid",
                "parity-metadata",
            ] {
                let session = assert_session_parity(&repository, session_id, 2).await?;
                for turn in &session.turns {
                    assert_turn_parity(&repository, session_id, turn.metadata.turn_seq, 3).await?;
                    if let Some(reference) = &turn.first_event {
                        assert_event_parity(&repository, &reference.event_uid).await?;
                    }
                    if let Some(reference) = &turn.last_event {
                        assert_event_parity(&repository, &reference.event_uid).await?;
                    }
                }
            }

            // (e) Non-contiguous `turn_index` override recurrence mixed with the
            // counter path (design §5.3 membership rule + VERIFIER ADDENDUM item 2
            // stray-override folding). The whole session diffs equal — including
            // `turn_id`, since every turn here carries a single `turn_index`
            // value — and the override turn's concrete turn_id value is pinned.
            let overridden = assert_session_parity(&repository, "parity-override", 2).await?;
            assert!(
                overridden.turns.len() >= 2,
                "override fixture must expand to at least two turns"
            );
            for turn in &overridden.turns {
                assert_turn_parity(&repository, "parity-override", turn.metadata.turn_seq, 4)
                    .await?;
            }
            assert_override_turn_id_within_value_set(&repository, "parity-override").await?;
            // Pin the turn_id DERIVATION where the two candidate rules diverge:
            // a counter-path turn with turn_seq >= 2 must report turn_id "0"
            // (first member row's turn_index), never turn_seq's "2"/"3".
            assert_counter_turn_id_pins_turn_index(&repository).await?;

            // Review-finding regression trio (issue-598 adversarial review):
            // (f1) epoch-sentinel event_ts rows survive hydration end-to-end;
            // (f2) an early high-override stray is served complete across
            // limit-1 pagination (the from-anchor window can never re-read
            // it); (f3) continuation anchors on float-lossy 2038 sort_times
            // resume exactly. All three sessions read identically under
            // single-page and paginated traversal.
            // parity-epoch was deliberately NOT byte-diffed against v1: a
            // malformed record_ts row's ORDER legitimately diverged (v2 sorts
            // it at the deterministic epoch sentinel, v1 at its ingested_at
            // fallback) — the same whitelisted class as parity-malformed. What
            // must hold
            // is that v2 still HYDRATES the row: the finding's failure mode
            // was the temporal bound silently excluding it, so open(event)
            // returned None and turn membership lost its content.
            assert_epoch_sentinel_rows_hydrate(&repository).await?;
            for (session_id, page_limit) in [("parity-override-early", 1u16), ("parity-2038", 1)] {
                let session = assert_session_parity(&repository, session_id, page_limit).await?;
                for turn in &session.turns {
                    assert_turn_parity(&repository, session_id, turn.metadata.turn_seq, 2).await?;
                    if let Some(reference) = &turn.first_event {
                        assert_event_parity(&repository, &reference.event_uid).await?;
                    }
                    if let Some(reference) = &turn.last_event {
                        assert_event_parity(&repository, &reference.event_uid).await?;
                    }
                }
            }
            let early_override =
                assert_session_parity(&repository, "parity-override-early", 1).await?;
            assert_eq!(
                early_override
                    .turns
                    .iter()
                    .map(|turn| turn.metadata.turn_seq)
                    .collect::<Vec<_>>(),
                vec![1, 2],
                "both turns survive pagination despite inverted tuple order"
            );
            let stray_turn = early_override
                .turns
                .iter()
                .find(|turn| turn.metadata.turn_seq == 2)
                .context("turn 2 missing from early-override session")?;
            assert_eq!(
                stray_turn.metadata.total_events, 2,
                "turn 2 keeps both pre-anchor rows across pagination"
            );

            // 035 metadata precedence surface: OPEN per-field-latest title/name/
            // summary/slug from session_meta + omp title/title_change, with the omp
            // dispatch-title fallback. The `parity-metadata` session above already
            // page-size-diffed equal end-to-end; assert the concrete resolved
            // values so a both-page-sizes-wrong regression cannot pass.
            let metadata_session = open_session_v2(&repository, "parity-metadata", 25)
                .await?
                .context("metadata session missing from the canonical reader")?;
            assert_eq!(
                metadata_session.title.as_deref(),
                Some("Latest Explicit Title")
            );

            // Malformed-timestamp / sentinel divergence (whitelisted): v2 sorts the
            // malformed row at the epoch sentinel deterministically, and that order
            // is stable across a same-uid re-insert (the §1a "no ghost rows"
            // property) even though the display timestamp legitimately moves.
            assert_malformed_timestamp_divergence(&clickhouse, &repository).await?;

            // Scope in/out including trailing-slash root/origin shapes (design R8):
            // a scoped repository sees only sessions whose origin_cwd is under the
            // configured root.
            assert_scope_parity(&clickhouse).await?;

            Ok(())
        },
    )
    .await
}

/// Seed every parity session in one place. Ordering fields (`record_ts` -> the
/// deterministic v2 `sort_time`; `source_offset`/`source_line_no`) are chosen
/// so the derived public order is unambiguous.
#[allow(clippy::vec_init_then_push)] // readable interleaved fixture with per-row comments
async fn seed_parity_corpus(clickhouse: &ClickHouseClient) -> Result<()> {
    let mut rows: Vec<Ev> = Vec::new();

    // Blank-session row: excluded by the reader.
    rows.push(Ev::new("", "parity-blank-1", 1).user());

    // Single-turn: one user + one assistant, counter path. Origin carries a
    // trailing slash (`/repo/`) so the scope check exercises R8 trailing-slash
    // root/origin matching against the no-trailing-slash root `/repo`.
    rows.push(
        Ev::new("parity-single-turn", "single-u1", 10)
            .user()
            .cwd("/repo/"),
    );
    rows.push(Ev::new("parity-single-turn", "single-a1", 11).cwd("/repo/"));

    // Out-of-scope: origin under a different root; hidden from a `/repo`-scoped
    // repository.
    rows.push(
        Ev::new("parity-out-of-scope", "oos-u1", 12)
            .user()
            .cwd("/elsewhere")
            .source("fixture", "/fixtures/parity-out-of-scope.jsonl"),
    );
    rows.push(
        Ev::new("parity-out-of-scope", "oos-a1", 13)
            .cwd("/elsewhere")
            .source("fixture", "/fixtures/parity-out-of-scope.jsonl"),
    );

    // Tool-only: assistant tool calls, no user message -> turn_seq floor 1.
    rows.push(Ev::new("parity-tool-only", "tool-c1", 20).tool_call("shell", "call-1"));
    rows.push(Ev::new("parity-tool-only", "tool-c2", 21).tool_call("shell", "call-2"));

    // Multi-turn: three user/assistant pairs via the counter path.
    for turn in 0..3u32 {
        let base = 30 + turn * 4;
        rows.push(Ev::new("parity-multi-turn", &format!("multi-u{turn}"), base).user());
        rows.push(Ev::new(
            "parity-multi-turn",
            &format!("multi-a{turn}"),
            base + 1,
        ));
    }

    // Multi-host: interleaved hosts; the projector order key is host-aware, so
    // v2 must match the PROJECTOR (not the host-less v_conversation_trace).
    rows.push(
        Ev::new("parity-multi-host", "mh-u1", 40)
            .user()
            .host(HOST_A),
    );
    rows.push(Ev::new("parity-multi-host", "mh-a1", 41).host(HOST_B));
    rows.push(
        Ev::new("parity-multi-host", "mh-u2", 42)
            .user()
            .host(HOST_B),
    );
    rows.push(Ev::new("parity-multi-host", "mh-a2", 43).host(HOST_A));

    // Terminal-in-middle-turn: a `task_complete` in turn 1 while turn 2
    // continues; session completed/terminal come from the max-turn_seq turn.
    rows.push(Ev::new("parity-terminal-mid", "term-u1", 50).user());
    rows.push(Ev::new("parity-terminal-mid", "term-a1", 51));
    // A task_complete terminates the (middle) turn 1 while turn 2 continues;
    // the session-level terminal must come from the max-turn_seq turn (turn 2),
    // not this completed middle turn (R1(b)).
    rows.push(Ev::new("parity-terminal-mid", "term-c1", 52).payload_type("task_complete"));
    rows.push(Ev::new("parity-terminal-mid", "term-u2", 53).user());
    rows.push(Ev::new("parity-terminal-mid", "term-a2", 54));

    // Override recurrence: explicit turn_index that revisits an earlier turn
    // out of contiguous order, mixed with counter-path rows.
    rows.push(Ev::new("parity-override", "ovr-u1", 60).user().turn(1));
    rows.push(Ev::new("parity-override", "ovr-a1", 61).turn(1));
    rows.push(Ev::new("parity-override", "ovr-u2", 62).user().turn(2));
    rows.push(Ev::new("parity-override", "ovr-a2", 63).turn(2));
    // A later row that folds back into turn 1 (non-contiguous recurrence).
    rows.push(Ev::new("parity-override", "ovr-a1-late", 64).turn(1));

    // Malformed record_ts (sentinel divergence, LIVE TEST PLAN §2 whitelist):
    // `mal-a1`'s record_ts fails BestEffort parse, so the deterministic
    // sort_time is the epoch sentinel (sorts at session start) — where v1
    // sorted it at its ingested_at position. This was the whitelisted
    // divergence when the two readers were diffed; it is asserted directly.
    rows.push(Ev::new("parity-malformed", "mal-u1", 80).user());
    rows.push(
        Ev::new("parity-malformed", "mal-a1", 81)
            .record_ts("not-a-timestamp")
            .ingested_at_secs(5)
            .version(1),
    );
    rows.push(Ev::new("parity-malformed", "mal-a2", 82));

    // Metadata precedence: session_meta title/name/summary/slug + omp
    // title_change, latest-wins per field.
    rows.push(
        Ev::new("parity-metadata", "meta-title-early", 70)
            .source("omp", "/tmp/omp/parity-metadata.jsonl")
            .payload("session_meta", r#"{"title":"Early Title"}"#),
    );
    rows.push(Ev::new("parity-metadata", "meta-user", 71).user());
    rows.push(
        Ev::new("parity-metadata", "meta-title-late", 72)
            .source("omp", "/tmp/omp/parity-metadata.jsonl")
            .payload("session_meta", r#"{"title":"Latest Explicit Title"}"#),
    );
    rows.push(Ev::new("parity-metadata", "meta-a", 73));

    // Real-ingest malformed shape (issue-598 review finding 1): when the Rust
    // normalizer cannot parse record_ts it stores the EPOCH SENTINEL in
    // events.event_ts (never a well-formed time), while the navigation MV's
    // display_time falls back to ingested_at. Hydration must still return the
    // row (the temporal bound carries an epoch-sentinel branch); an epoch
    // event_ts sorts first, which is where v1 put it too, so this session was
    // inside the diffed set rather than whitelisted out of it.
    rows.push(Ev::new("parity-epoch", "epoch-u1", 90).user());
    rows.push(
        Ev::new("parity-epoch", "epoch-a1", 91)
            .record_ts("not-a-timestamp")
            .event_ts("1970-01-01 00:00:00.000")
            .ingested_at_secs(92),
    );
    rows.push(Ev::new("parity-epoch", "epoch-a2", 93));

    // Override turn served AFTER rows that sort BEFORE it (review finding 2).
    // Turn 2's rows come FIRST in ordering-tuple order, turn 1's come last, so
    // page 1 (limit 1) serves turn 1 — the session's LAST rows — and its
    // anchor lands past every row of turn 2. A from-anchor continuation window
    // can never see turn 2 again, so the page reader must re-fold from the
    // session start to serve it. Overrides stay dense (max turn_index equals
    // the turn count) because the projector's total_turns identity made a
    // sparse high override unrepresentable in the v1 read model, and the
    // canonical reader inherits that identity.
    rows.push(
        Ev::new("parity-override-early", "oe-u2", 100)
            .user()
            .turn(2),
    );
    rows.push(Ev::new("parity-override-early", "oe-a2", 101).turn(2));
    rows.push(
        Ev::new("parity-override-early", "oe-u1", 102)
            .user()
            .turn(1),
    );
    rows.push(Ev::new("parity-override-early", "oe-a1", 103).turn(1));

    // Float-lossy keyset window (review finding 3). Every instant below is
    // VERIFIED lossy: `toDateTime64(ms/1000.0, 3)` reconstructs each one 1 ms
    // LOW (e.g. …27.595 -> …27.594), because ulp(ms/1000)·1000 exceeds ulp(ms)
    // once ms >= 1000·2^31 and the float->Decimal cast truncates. An anchor on
    // such an instant must still resume exactly — no re-served event, no
    // spurious reopen — which holds only with exact fromUnixTimestamp64Milli
    // reconstruction. Picking merely "a 2038 timestamp" is not enough: most
    // instants in the window round-trip fine, so a careless fixture passes
    // with or without the fix.
    for (index, (uid, record, event, user)) in [
        (
            "y38-u1",
            "2038-01-19T03:17:27.595Z",
            "2038-01-19 03:17:27.595",
            true,
        ),
        (
            "y38-a1",
            "2038-01-19T03:17:27.601Z",
            "2038-01-19 03:17:27.601",
            false,
        ),
        (
            "y38-u2",
            "2038-01-19T03:17:27.607Z",
            "2038-01-19 03:17:27.607",
            true,
        ),
        (
            "y38-a2",
            "2038-01-19T03:17:27.608Z",
            "2038-01-19 03:17:27.608",
            false,
        ),
        (
            "y38-u3",
            "2038-01-19T03:17:27.614Z",
            "2038-01-19 03:17:27.614",
            true,
        ),
        (
            "y38-a3",
            "2038-01-19T03:17:27.615Z",
            "2038-01-19 03:17:27.615",
            false,
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let mut row = Ev::new("parity-2038", uid, 110 + index as u32)
            .record_ts(record)
            .event_ts(event);
        if user {
            row = row.user();
        }
        rows.push(row);
    }

    seed_events(clickhouse, &rows).await
}

/// The epoch-sentinel hydration contract (issue-598 review finding 1). Real
/// ingest stores `event_ts = 1970-01-01` whenever the normalizer cannot parse
/// `record_ts`, while the navigation index's `display_time` falls back to
/// `ingested_at` — decades apart. A hydration bound derived from display time
/// (with any finite slack) drops the row, so `open(event)` reports it missing
/// and its turn loses the row's content. Both hydration branches are pinned:
/// the pure epoch-equality bound (`open(event)`, a single sentinel row) and
/// the mixed range-plus-sentinel bound (`open(turn)`, sentinel row alongside
/// well-formed ones).
async fn assert_epoch_sentinel_rows_hydrate(
    repository: &ClickHouseConversationRepository,
) -> Result<()> {
    let event = repository
        .canonical_open_event("epoch-a1")
        .await
        .context("v2 event open failed for the epoch-sentinel row")?
        .context("v2 open(event) dropped the epoch-sentinel row — hydration excluded it")?;
    assert_eq!(
        event.event.text_content.as_str(),
        "assistant text for epoch-a1",
        "the epoch-sentinel row must hydrate its wide content, not just its reference"
    );

    let turn = open_turn_v2(repository, "parity-epoch", 1, 4)
        .await?
        .context("v2 turn open returned None for the epoch-sentinel session")?;
    let uids: Vec<&str> = turn
        .events
        .iter()
        .map(|event| event.event_uid.as_str())
        .collect();
    assert_eq!(
        uids,
        vec!["epoch-a1", "epoch-u1", "epoch-a2"],
        "the sentinel row sorts first deterministically and stays a turn member"
    );
    let sentinel = turn
        .events
        .iter()
        .find(|event| event.event_uid == "epoch-a1")
        .context("sentinel row missing from its own turn")?;
    assert_eq!(
        sentinel.text_preview.as_deref(),
        Some("assistant text for epoch-a1"),
        "mixed range+sentinel hydration must return the sentinel row's content"
    );
    Ok(())
}

/// Assert the override turn's deterministic turn_id equals its single member
/// `turn_index` value (the LIVE TEST PLAN §2 turn_id membership rule). The
/// turn's only `turn_index` value is 1 — and there `turn_index == turn_seq`,
/// so this checkpoint alone cannot distinguish the correct derivation (first
/// row's turn_index) from the turn_seq fallback; that distinction is pinned
/// by [`assert_counter_turn_id_pins_turn_index`].
async fn assert_override_turn_id_within_value_set(
    repository: &ClickHouseConversationRepository,
    session_id: &str,
) -> Result<()> {
    let turn = open_turn_v2(repository, session_id, 1, 8)
        .await?
        .context("override turn 1 missing from the canonical reader")?;
    // turn_id derives from a member row's `toString(turn_index)`; the turn's
    // only turn_index value is "1".
    assert_eq!(turn.metadata.turn_id, "1");
    Ok(())
}

/// Pin the turn_id derivation on a case where the two candidate rules actually
/// diverge: `parity-multi-turn` turn 3 is counter-path (`turn_index = 0` on
/// every row) with `turn_seq = 3`, so the correct turn_id — the
/// first-member-row `turn_index` (design R1(d), the retired v1 reader's
/// `anyIf(toString(turn_index))` agreed) — is "0", while a regression to the
/// `turn_seq` fallback (canonical_open.rs `assemble_turn_compact`) would
/// report "3". The page-size diffs above compare turn_id across page sizes;
/// this assert pins the concrete value so a both-wrong drift cannot pass.
async fn assert_counter_turn_id_pins_turn_index(
    repository: &ClickHouseConversationRepository,
) -> Result<()> {
    let turn = open_turn_v2(repository, "parity-multi-turn", 3, 8)
        .await?
        .context("counter turn 3 missing from the canonical reader")?;
    assert_eq!(
        turn.metadata.turn_id, "0",
        "counter-path turn_id must derive from the first member row's turn_index, not the turn_seq fallback"
    );
    Ok(())
}

/// Sentinel-`record_ts` ordering divergence (LIVE TEST PLAN §2 whitelist). The
/// malformed row sorts first at the epoch sentinel, and that order is stable
/// across a same-uid re-insert with a fresh `ingested_at`/version because the
/// v2 `sort_time` is a pure function of the record bytes (design §1a). The
/// re-insert version-collapses to a single navigation row — no ghost. Display
/// timestamps may legitimately move with `ingested_at`, so we assert order and
/// cardinality, not byte-identical output.
async fn assert_malformed_timestamp_divergence(
    clickhouse: &ClickHouseClient,
    repository: &ClickHouseConversationRepository,
) -> Result<()> {
    let before = open_session_v2(repository, "parity-malformed", 25)
        .await?
        .context("malformed session missing from v2 reader")?;
    let first_turn = before
        .turns
        .first()
        .context("malformed session must expand to at least one turn")?;
    let first_event = first_turn
        .first_event
        .as_ref()
        .context("malformed session's first turn must carry a first event")?;
    assert_eq!(
        first_event.event_uid, "mal-a1",
        "the malformed-record_ts row must sort first at the epoch sentinel"
    );
    let total_events_before = before.metadata.total_events;

    // Re-insert the same uid with a new ingested_at and higher version. The
    // navigation MV fires on the insert; the deterministic sort_time keeps the
    // PK identical, so ReplacingMergeTree(event_version) collapses it.
    let reinsert = vec![Ev::new("parity-malformed", "mal-a1", 81)
        .record_ts("not-a-timestamp")
        .ingested_at_secs(3000)
        .version(2)];
    seed_events(clickhouse, &reinsert).await?;

    let after = open_session_v2(repository, "parity-malformed", 25)
        .await?
        .context("malformed session missing from v2 reader after re-insert")?;
    let after_first = after
        .turns
        .first()
        .and_then(|turn| turn.first_event.as_ref())
        .context("malformed session's first turn must survive the re-insert")?;
    assert_eq!(
        after_first.event_uid, "mal-a1",
        "the malformed row must still sort first after a same-uid re-insert (deterministic sort_time)"
    );
    assert_eq!(
        after.metadata.total_events, total_events_before,
        "a same-uid re-insert must not add a phantom event (no ghost row)"
    );

    #[derive(serde::Deserialize)]
    struct CountRow {
        value: u64,
    }
    let navigation_rows = clickhouse
        .query_rows::<CountRow>(
            "SELECT toUInt64(count()) AS value FROM mcp_event_navigation FINAL \
             WHERE session_id = 'parity-malformed' AND event_uid = 'mal-a1' \
             FORMAT JSONEachRow",
            None,
        )
        .await?
        .into_iter()
        .next()
        .map(|row| row.value)
        .unwrap_or_default();
    assert_eq!(
        navigation_rows, 1,
        "the re-inserted malformed row must version-collapse to one navigation row"
    );
    Ok(())
}

/// Trailing-slash root/origin scope visibility (design R8): a repository
/// scoped to `/repo` (no trailing slash) must expose a session whose
/// origin_cwd is `/repo/` and hide a session originating outside.
async fn assert_scope_parity(clickhouse: &ClickHouseClient) -> Result<()> {
    let scoped = ClickHouseConversationRepository::new(
        clickhouse.clone(),
        RepoConfig {
            session_scope: Some(SessionOriginScope {
                roots: vec!["/repo".to_string()],
            }),
            ..RepoConfig::default()
        },
    );

    // In-scope: origin_cwd `/repo/` (trailing slash) is under root `/repo`.
    let in_scope = open_session_v2(&scoped, "parity-single-turn", 25).await?;
    assert!(
        in_scope.is_some(),
        "in-scope session must be visible under the scoped repo"
    );

    // Out-of-scope: `parity-tool-only` originates under `/repo` too by default,
    // so seed comparison relies on an explicitly out-of-root origin. The
    // multi-host fixture keeps `/repo`; use a dedicated out-of-scope probe.
    let out = open_session_v2(&scoped, "parity-out-of-scope", 25).await?;
    assert!(
        out.is_none(),
        "out-of-scope session must be hidden under the scoped repo"
    );
    Ok(())
}

// ===========================================================================
// Gate 2: canonical-open-locator
// ===========================================================================

/// `open(event)` via `mcp_event_locator` rejects a non-live (superseded or
/// replaying-only) generation and resolves a live one; the locator seek reads
/// no unrelated session's rows (issue #598 LIVE TEST PLAN §3).
pub(super) async fn locator() -> Result<()> {
    with_owned_live_db(
        "canonical-open locator gate",
        |clickhouse, database| async move {
            clickhouse
                .run_migrations()
                .await
                .context("failed to migrate canonical-open locator database")?;

            // Session under replacement: generation 1 (originally live), then a
            // generation-2 replacement of the same source file, plus an unrelated
            // session that must never be touched by a locator seek.
            let gen1 = vec![
                Ev::new("locator-session", "loc-g1-u1", 10)
                    .user()
                    .source("fixture", "/fixtures/locator.jsonl")
                    .generation(1),
                Ev::new("locator-session", "loc-g1-a1", 11)
                    .source("fixture", "/fixtures/locator.jsonl")
                    .generation(1),
            ];
            let gen2 = vec![
                Ev::new("locator-session", "loc-g2-u1", 12)
                    .user()
                    .source("fixture", "/fixtures/locator.jsonl")
                    .generation(2),
                Ev::new("locator-session", "loc-g2-a1", 13)
                    .source("fixture", "/fixtures/locator.jsonl")
                    .generation(2),
            ];
            let unrelated = vec![
                Ev::new("locator-unrelated", "loc-other-u1", 20)
                    .user()
                    .source("fixture", "/fixtures/locator-other.jsonl"),
                Ev::new("locator-unrelated", "loc-other-a1", 21)
                    .source("fixture", "/fixtures/locator-other.jsonl"),
            ];

            // --- Phase 1: generation 1 is the only published head. ---
            seed_events(&clickhouse, &gen1).await?;
            seed_events(&clickhouse, &unrelated).await?;
            publish_and_backfill(&clickhouse, &database).await?;

            let repository =
                ClickHouseConversationRepository::new(clickhouse.clone(), RepoConfig::default());

            // A live generation-1 uid resolves.
            let live = repository
                .canonical_open_event("loc-g1-a1")
                .await?
                .context("live generation-1 event must resolve via the locator")?;
            assert_eq!(live.event.session_id, "locator-session");

            // --- Phase 2: seed generation 2 but leave it unpublished (replaying
            // only). The navigation/locator MVs store all generations, but the
            // reader filters to the pinned live head (generation 1), so a
            // replaying-only uid is rejected and reads still expose complete-old. ---
            seed_events(&clickhouse, &gen2).await?;
            assert!(
                repository
                    .canonical_open_event("loc-g2-a1")
                    .await?
                    .is_none(),
                "a replaying-only (unpublished) generation uid must be rejected as non-live"
            );
            let pre_activation = repository
                .canonical_open_event("loc-g1-a1")
                .await?
                .context("generation-1 event must still resolve while g2 is replaying")?;
            assert_eq!(pre_activation.event.event_uid, "loc-g1-a1");

            // --- Phase 3: activate generation 2 (publish its head). Generation 1 is
            // now superseded and rejected; generation 2 resolves. Never a mix.
            // Superseded generation 1 legitimately stays unpublished. ---
            publish_replaced_schema_fixture_sources(&clickhouse, &database).await?;
            assert!(
                repository
                    .canonical_open_event("loc-g1-a1")
                    .await?
                    .is_none(),
                "a superseded generation uid must be rejected once replaced"
            );
            let activated = repository
                .canonical_open_event("loc-g2-a1")
                .await?
                .context("activated generation-2 event must resolve")?;
            assert_eq!(activated.event.session_id, "locator-session");

            // The locator seek must not scan unrelated sessions' rows: assert
            // via query_log that every statement over a session-holding table
            // in the seek envelope carried its uid/session scoping predicate.
            assert_locator_seek_is_scoped(&clickhouse, &repository).await?;

            Ok(())
        },
    )
    .await
}

/// Run one `open(event)` inside a uniquely-labelled Interactive envelope and
/// assert from `system.query_log` that every statement over a session-holding
/// table carried its scoping predicate — the structural property whose loss IS
/// the unrelated-session/full-corpus scan regression.
///
/// Why structural: an UNSCOPED statement's SQL text never mentions the
/// unrelated session by name (grepping for 'locator-unrelated' is tautologically
/// zero), and at this fixture's size (six rows) `read_rows` cannot separate a
/// point-read from a full scan because both sit under one granule. What a
/// scoping regression in `canonical_open.rs` MUST change is the statement text
/// itself: the locator point-read loses its `event_uid = 'loc-g2-a1'`
/// predicate, or a navigation/directory/hydration statement loses its
/// `session_id = 'locator-session'` predicate. So the census requires every
/// statement referencing `mcp_event_locator`, `mcp_event_navigation`,
/// `mcp_session_directory`, or the wide `events` read to contain the target
/// uid or the resolved session id, and requires the census to be non-empty so
/// it cannot pass vacuously.
async fn assert_locator_seek_is_scoped(
    clickhouse: &ClickHouseClient,
    repository: &ClickHouseConversationRepository,
) -> Result<()> {
    QueryEnvelope::new(
        "issue598-locator-seek",
        QueryClass::Interactive,
        &default_interactive_budget(),
    )
    .scope(async {
        repository
            .canonical_open_event("loc-g2-a1")
            .await
            .context("scoped locator seek failed")?;
        Ok::<(), anyhow::Error>(())
    })
    .await?;

    clickhouse
        .request_text("SYSTEM FLUSH LOGS", None, None, false, None)
        .await?;

    #[derive(serde::Deserialize)]
    struct CensusRow {
        session_table_statements: u64,
        unscoped_statements: u64,
    }
    let census = clickhouse
        .query_rows::<CensusRow>(
            "SELECT \
               toUInt64(countIf( \
                 position(query, 'mcp_event_locator') > 0 \
                 OR position(query, 'mcp_event_navigation') > 0 \
                 OR position(query, 'mcp_session_directory') > 0 \
                 OR position(query, '`events` AS e FINAL') > 0)) AS session_table_statements, \
               toUInt64(countIf( \
                 (position(query, 'mcp_event_locator') > 0 \
                  OR position(query, 'mcp_event_navigation') > 0 \
                  OR position(query, 'mcp_session_directory') > 0 \
                  OR position(query, '`events` AS e FINAL') > 0) \
                 AND position(query, 'loc-g2-a1') = 0 \
                 AND position(query, 'locator-session') = 0)) AS unscoped_statements \
             FROM system.query_log \
             WHERE type = 'QueryFinish' \
               AND startsWith(query_id, 'moraine-issue598-locator-seek') \
               AND current_database = currentDatabase() \
             FORMAT JSONEachRow",
            None,
        )
        .await?
        .into_iter()
        .next()
        .context("locator-seek scoping census returned no row")?;
    assert!(
        census.session_table_statements > 0,
        "locator-seek census saw no statements over the session-holding tables — \
         the scoping gate would pass vacuously (query-id prefix or table names drifted?)"
    );
    assert_eq!(
        census.unscoped_statements, 0,
        "every locator-seek statement over a session-holding table must be scoped \
         by the target event uid or the resolved session id ({} of {} were not)",
        census.unscoped_statements, census.session_table_statements
    );
    Ok(())
}

// ===========================================================================
// Gate 3: canonical-open-continuation
// ===========================================================================

/// Cursor continuation under concurrency (design §5.4, D1's dedicated gate):
/// quiescent continuation touches only the directory point-read; in-order
/// appends continue via the boundary guard with no dropped/duplicated events;
/// an out-of-order append reopens; a replacement mid-traversal reopens; an
/// unrelated-source publication does NOT reopen (step-3 precision).
pub(super) async fn continuation() -> Result<()> {
    with_owned_live_db(
        "canonical-open continuation gate",
        |clickhouse, database| async move {
            clickhouse
                .run_migrations()
                .await
                .context("failed to migrate canonical-open continuation database")?;

            // A multi-turn session plus an unrelated session (for the step-3
            // precision case), both published and backfilled.
            let mut rows = Vec::new();
            for turn in 0..6u32 {
                let base = 10 + turn * 4;
                rows.push(Ev::new("cont-session", &format!("cont-u{turn}"), base).user());
                rows.push(Ev::new("cont-session", &format!("cont-a{turn}"), base + 1));
            }
            rows.push(
                Ev::new("cont-unrelated", "cont-other-u1", 90)
                    .user()
                    .source("fixture", "/fixtures/cont-other.jsonl"),
            );
            rows.push(
                Ev::new("cont-unrelated", "cont-other-a1", 91)
                    .source("fixture", "/fixtures/cont-other.jsonl"),
            );
            seed_events(&clickhouse, &rows).await?;
            publish_and_backfill(&clickhouse, &database).await?;

            let repository =
                ClickHouseConversationRepository::new(clickhouse.clone(), RepoConfig::default());

            // (a) Quiescent continuation touches only the directory point-read:
            // no statement in the second page's envelope reads
            // `mcp_event_navigation`.
            assert_quiescent_continuation_is_directory_only(&clickhouse, &repository).await?;

            // (b) In-order appends mid-traversal: page 1, then append new turns
            // that sort AFTER the anchor (higher record_ts). The boundary guard
            // passes and the merged traversal is gap-free / duplicate-free
            // against a final reference read.
            assert_in_order_append_continues(&clickhouse, &repository, &database).await?;

            // (c) Out-of-order append (older record_ts than the anchor) sorts
            // into the served prefix -> boundary guard fails -> structured
            // reopen.
            assert_out_of_order_append_reopens(&clickhouse, &repository, &database).await?;

            // (g) Unrelated-source publication moves the global revision but the
            // target session's heads are unchanged -> NO reopen.
            assert_unrelated_publication_does_not_reopen(&clickhouse, &repository, &database)
                .await?;

            Ok(())
        },
    )
    .await
}

/// Mint a page-1 cursor, then read page 2 inside a labelled envelope and assert
/// no statement read the navigation index (quiescent path = directory
/// point-read only).
async fn assert_quiescent_continuation_is_directory_only(
    clickhouse: &ClickHouseClient,
    repository: &ClickHouseConversationRepository,
) -> Result<()> {
    let first = repository
        .canonical_open_session_page("cont-session", 2, None)
        .await?
        .context("quiescent page 1 returned no outcome")?;
    let continuation = match first {
        CanonicalReadOutcome::Page(page) => page
            .continuation
            .context("quiescent page 1 must yield a continuation")?,
        CanonicalReadOutcome::Reopen => bail!("quiescent page 1 unexpectedly reopened"),
    };

    QueryEnvelope::new(
        "issue598-quiescent-page",
        QueryClass::Interactive,
        &default_interactive_budget(),
    )
    .scope(async {
        repository
            .canonical_open_session_page("cont-session", 2, Some(continuation))
            .await
            .context("quiescent page 2 read failed")?;
        Ok::<(), anyhow::Error>(())
    })
    .await?;

    clickhouse
        .request_text("SYSTEM FLUSH LOGS", None, None, false, None)
        .await?;
    // The quiescent contract (BINDING D1): the staleness DECISION is a
    // directory point-read; serving the page still reads its bounded
    // navigation window. What a quiescent page must NEVER do: run the prefix
    // boundary guard, or recompute the session-wide header scans (totals /
    // metadata / terminal — the carried header is reused verbatim).
    #[derive(serde::Deserialize)]
    struct CensusRow {
        guard_statements: u64,
        header_scans: u64,
        navigation_read_rows: u64,
    }
    let census = clickhouse
        .query_rows::<CensusRow>(
            "SELECT \
               toUInt64(countIf(position(query, 'count_le_anchor') > 0)) AS guard_statements, \
               toUInt64(countIf( \
                 position(query, 'counter_user_messages') > 0 \
                 OR position(query, 'turn_completed') > 0 \
                 OR position(query, 'is_metadata_bearing') > 0)) AS header_scans, \
               toUInt64(sumIf(read_rows, position(query, 'mcp_event_navigation') > 0)) AS navigation_read_rows \
             FROM system.query_log \
             WHERE type = 'QueryFinish' \
               AND startsWith(query_id, 'moraine-issue598-quiescent-page') \
               AND current_database = currentDatabase() \
             FORMAT JSONEachRow",
            None,
        )
        .await?
        .into_iter()
        .next()
        .context("quiescent census returned no row")?;
    assert_eq!(
        census.guard_statements, 0,
        "a quiescent continuation must not run the prefix boundary guard"
    );
    assert_eq!(
        census.header_scans, 0,
        "a quiescent continuation must reuse the carried header (no totals/metadata/terminal scan)"
    );
    // Bounded window reads only: the WI-09 chunk floor is 1024 rows; the
    // 14-event fixture must come in far below one chunk.
    assert!(
        census.navigation_read_rows <= 1024,
        "quiescent page-2 navigation reads must stay window-bounded, read {} rows",
        census.navigation_read_rows
    );
    Ok(())
}

/// Page 1, append new turns that sort after the anchor, then continue. The
/// boundary guard passes, the traversal proceeds, and the concatenation of all
/// served pages is gap-free and duplicate-free at EVENT identity — every
/// turn's `(turn_seq, first/last event uid, total_events)` is pinned against
/// the fixture, and the full served turn content byte-equals a fresh full
/// read. Comparing turn_seq sequences alone would let a continuation that
/// drops or duplicates a non-user event (the chunk-boundary-resume /
/// `dedup_adjacent_navigation_versions` bug class) pass, because only user
/// messages shift turn derivation.
async fn assert_in_order_append_continues(
    clickhouse: &ClickHouseClient,
    repository: &ClickHouseConversationRepository,
    database: &OwnedDatabaseName,
) -> Result<()> {
    let first = repository
        .canonical_open_session_page("cont-session", 2, None)
        .await?
        .context("append page 1 returned no outcome")?;
    let (mut served, continuation) = match first {
        CanonicalReadOutcome::Page(page) => (
            page.session.turns,
            page.continuation
                .context("append page 1 must yield a continuation")?,
        ),
        CanonicalReadOutcome::Reopen => bail!("append page 1 unexpectedly reopened"),
    };

    // Concurrent in-order appends: two new turns whose record_ts sort strictly
    // after every served row. The MV maintains navigation/directory on insert,
    // so no re-backfill is needed.
    let appended = vec![
        Ev::new("cont-session", "cont-u-append-0", 200).user(),
        Ev::new("cont-session", "cont-a-append-0", 201),
        Ev::new("cont-session", "cont-u-append-1", 202).user(),
        Ev::new("cont-session", "cont-a-append-1", 203),
    ];
    seed_events(clickhouse, &appended).await?;
    // The appends are within the same live generation; republish is a no-op for
    // the head, but the source head must remain published.
    publish_missing_schema_fixture_sources(clickhouse, database).await?;

    // Continue paging from the page-1 cursor to the end.
    let mut after = Some(continuation);
    loop {
        let outcome = repository
            .canonical_open_session_page("cont-session", 2, after.clone())
            .await?
            .context("append continuation returned no outcome")?;
        let page = match outcome {
            CanonicalReadOutcome::Page(page) => page,
            CanonicalReadOutcome::Reopen => {
                bail!("an in-order append must not reopen the cursor")
            }
        };
        served.extend(page.session.turns);
        match page.continuation {
            Some(next) => after = Some(next),
            None => break,
        }
    }

    // Turn-level ordering: the served turn_seq stream is strictly increasing.
    let served_seqs: Vec<u32> = served.iter().map(|turn| turn.metadata.turn_seq).collect();
    for window in served_seqs.windows(2) {
        assert!(
            window[1] > window[0],
            "served turns must be strictly increasing (no gaps/dupes): {served_seqs:?}"
        );
    }

    // Event identity, pinned to the fixture: six seeded turns plus the two
    // appended ones, each exactly (user, assistant). A dropped or duplicated
    // event anywhere changes a turn's first/last uid or its event count.
    let expected_identity: Vec<(u32, String, String, u64)> = (0..6u32)
        .map(|t| (t + 1, format!("cont-u{t}"), format!("cont-a{t}"), 2))
        .chain((0..2u32).map(|a| {
            (
                7 + a,
                format!("cont-u-append-{a}"),
                format!("cont-a-append-{a}"),
                2,
            )
        }))
        .collect();
    let served_identity: Vec<(u32, String, String, u64)> = served
        .iter()
        .map(|turn| {
            Ok((
                turn.metadata.turn_seq,
                turn.first_event
                    .as_ref()
                    .context("served turn missing its first event ref")?
                    .event_uid
                    .clone(),
                turn.last_event
                    .as_ref()
                    .context("served turn missing its last event ref")?
                    .event_uid
                    .clone(),
                turn.metadata.total_events,
            ))
        })
        .collect::<Result<_>>()?;
    assert_eq!(
        served_identity, expected_identity,
        "merged traversal must serve every fixture event exactly once across the append boundary"
    );

    // And the full served turn content must byte-equal a fresh full read, so
    // any divergence the identity triples cannot see (summaries, per-class
    // counts, event refs) fails loudly too.
    let reference = open_session_v2(repository, "cont-session", 25)
        .await?
        .context("reference full read returned None")?;
    let served_json = serde_json::to_value(&served).context("failed to serialize served turns")?;
    let reference_json =
        serde_json::to_value(&reference.turns).context("failed to serialize reference turns")?;
    if served_json != reference_json {
        bail!(
            "continued traversal diverged from a fresh full read after the appends\n  served={served_json}\n  reference={reference_json}"
        );
    }
    Ok(())
}

/// Page 1, then insert an event whose record_ts is OLDER than the served
/// anchor. It sorts into the prefix, so the boundary guard's counts no longer
/// match the anchor and the next page returns the structured reopen.
async fn assert_out_of_order_append_reopens(
    clickhouse: &ClickHouseClient,
    repository: &ClickHouseConversationRepository,
    database: &OwnedDatabaseName,
) -> Result<()> {
    // Use a fresh session so the earlier append test does not interfere.
    let mut rows = Vec::new();
    for turn in 0..4u32 {
        let base = 300 + turn * 4;
        rows.push(Ev::new("cont-ooo-session", &format!("ooo-u{turn}"), base).user());
        rows.push(Ev::new(
            "cont-ooo-session",
            &format!("ooo-a{turn}"),
            base + 1,
        ));
    }
    seed_events(clickhouse, &rows).await?;
    // The fresh source file is new to this helper; publish its head so the
    // pinned-heads reader sees the session as live.
    publish_missing_schema_fixture_sources(clickhouse, database).await?;

    let first = repository
        .canonical_open_session_page("cont-ooo-session", 2, None)
        .await?
        .context("out-of-order page 1 returned no outcome")?;
    let continuation = match first {
        CanonicalReadOutcome::Page(page) => page
            .continuation
            .context("out-of-order page 1 must yield a continuation")?,
        CanonicalReadOutcome::Reopen => bail!("out-of-order page 1 unexpectedly reopened"),
    };

    // Insert a row that sorts BEFORE the anchor (record_ts at 12:05:00, earlier
    // than the served rows at 12:05:00+): here we use an explicitly small secs
    // value so the deterministic sort_time lands in the served prefix.
    let out_of_order = vec![Ev::new("cont-ooo-session", "ooo-prefix-insert", 1).user()];
    seed_events(clickhouse, &out_of_order).await?;

    let second = repository
        .canonical_open_session_page("cont-ooo-session", 2, Some(continuation))
        .await?
        .context("out-of-order page 2 returned no outcome")?;
    match second {
        CanonicalReadOutcome::Reopen => Ok(()),
        CanonicalReadOutcome::Page(_) => {
            bail!("a prefix-inserting out-of-order append must return the structured reopen")
        }
    }
}

/// Page 1 of the target session, publish a NEW generation of an UNRELATED
/// source (moving the global publication revision), then continue the target.
/// The target's live heads are unchanged, so step-3's fingerprint check keeps
/// the cursor alive: no reopen.
async fn assert_unrelated_publication_does_not_reopen(
    clickhouse: &ClickHouseClient,
    repository: &ClickHouseConversationRepository,
    database: &OwnedDatabaseName,
) -> Result<()> {
    let first = repository
        .canonical_open_session_page("cont-session", 2, None)
        .await?
        .context("unrelated-publication page 1 returned no outcome")?;
    let continuation = match first {
        CanonicalReadOutcome::Page(page) => page
            .continuation
            .context("unrelated-publication page 1 must yield a continuation")?,
        CanonicalReadOutcome::Reopen => bail!("unrelated-publication page 1 unexpectedly reopened"),
    };

    // Publish a brand-new unrelated source generation, advancing the global
    // publication revision without touching cont-session's heads.
    let unrelated_gen2 = vec![Ev::new("cont-unrelated", "cont-other-g2", 95)
        .source("fixture", "/fixtures/cont-other.jsonl")
        .generation(2)];
    seed_events(clickhouse, &unrelated_gen2).await?;
    // The unrelated source already has a published gen-1 head; advancing it to
    // gen 2 is a replacement publication (superseded gen 1 stays unpublished).
    publish_replaced_schema_fixture_sources(clickhouse, database).await?;

    let second = repository
        .canonical_open_session_page("cont-session", 2, Some(continuation))
        .await?
        .context("unrelated-publication page 2 returned no outcome")?;
    match second {
        CanonicalReadOutcome::Page(_) => Ok(()),
        CanonicalReadOutcome::Reopen => {
            bail!("an unrelated-source publication must NOT reopen the target cursor (step-3)")
        }
    }
}

// ===========================================================================
// Gate 4: canonical-open-fence
// ===========================================================================

/// An append-fenced source (#602) serves the pinned pre-fence state
/// consistently: ordinary fenced appends produce no spurious `ReadModelChanged`
/// (BINDING D9 — the v2 `AnchoredSession` read proceeds), while a genuine
/// revision move (a replacement generation) returns the structured reopen.
pub(super) async fn fence() -> Result<()> {
    with_owned_live_db("canonical-open fence gate", |clickhouse, database| async move {
        clickhouse
            .run_migrations()
            .await
            .context("failed to migrate canonical-open fence database")?;

        let mut rows = Vec::new();
        for turn in 0..4u32 {
            let base = 10 + turn * 4;
            rows.push(
                Ev::new("fence-session", &format!("fence-u{turn}"), base)
                    .user()
                    .source("fixture", "/fixtures/fence.jsonl"),
            );
            rows.push(
                Ev::new("fence-session", &format!("fence-a{turn}"), base + 1)
                    .source("fixture", "/fixtures/fence.jsonl"),
            );
        }
        seed_events(&clickhouse, &rows).await?;
        publish_and_backfill(&clickhouse, &database).await?;

        let repository =
            ClickHouseConversationRepository::new(clickhouse.clone(), RepoConfig::default());

        // D9: an ordinary append to the live fenced source is visible and the
        // v2 open proceeds — no spurious ReadModelChanged. (An insert-only
        // append fence is exactly the steady-state case here: the append writes
        // no head rows, so the pinned as-of heads are untouched and the
        // AnchoredSession read is allowed.)
        let appended = vec![
            Ev::new("fence-session", "fence-u-append", 200)
                .user()
                .source("fixture", "/fixtures/fence.jsonl"),
            Ev::new("fence-session", "fence-a-append", 201)
                .source("fixture", "/fixtures/fence.jsonl"),
        ];
        seed_events(&clickhouse, &appended).await?;
        publish_missing_schema_fixture_sources(&clickhouse, &database).await?;

        match repository
            .canonical_open_session_page("fence-session", 25, None)
            .await
        {
            Ok(Some(CanonicalReadOutcome::Page(_))) => {}
            Ok(Some(CanonicalReadOutcome::Reopen)) => {
                bail!("an ordinary fenced append must not reopen the initial open")
            }
            Ok(None) => bail!("fence-session must be openable after an append"),
            Err(RepoError::ReadModelChanged) => {
                bail!("an ordinary insert-only fenced append must not surface ReadModelChanged (D9)")
            }
            Err(other) => return Err(other.into()),
        }

        // A genuine revision move: publish a replacement generation of the same
        // source mid-traversal. The session's live head flips, the cursor's
        // heads-fingerprint changes, and the next page returns the structured
        // reopen (complete-old -> complete-new, never mixed).
        assert_revision_move_reopens(&clickhouse, &repository, &database).await?;

        Ok(())
    })
    .await
}

/// Page 1, publish a replacement generation of the target's source, then
/// continue: the heads-fingerprint change forces a structured reopen.
async fn assert_revision_move_reopens(
    clickhouse: &ClickHouseClient,
    repository: &ClickHouseConversationRepository,
    database: &OwnedDatabaseName,
) -> Result<()> {
    let first = repository
        .canonical_open_session_page("fence-session", 2, None)
        .await?
        .context("revision-move page 1 returned no outcome")?;
    let continuation = match first {
        CanonicalReadOutcome::Page(page) => page
            .continuation
            .context("revision-move page 1 must yield a continuation")?,
        CanonicalReadOutcome::Reopen => bail!("revision-move page 1 unexpectedly reopened"),
    };

    // Replacement generation 2 of the SAME source file, published as the new
    // head: this is a genuine revision move over the target session.
    let replacement = vec![
        Ev::new("fence-session", "fence-g2-u", 12)
            .user()
            .source("fixture", "/fixtures/fence.jsonl")
            .generation(2),
        Ev::new("fence-session", "fence-g2-a", 13)
            .source("fixture", "/fixtures/fence.jsonl")
            .generation(2),
    ];
    seed_events(clickhouse, &replacement).await?;
    // Publishing generation 2 over the live generation-1 head is a
    // replacement; the superseded generation legitimately stays unpublished.
    publish_replaced_schema_fixture_sources(clickhouse, database).await?;

    let second = repository
        .canonical_open_session_page("fence-session", 2, Some(continuation))
        .await;
    match second {
        Ok(Some(CanonicalReadOutcome::Reopen)) => Ok(()),
        Ok(Some(CanonicalReadOutcome::Page(_))) => {
            bail!("a genuine revision move must return the structured reopen, not a page")
        }
        Ok(None) => bail!("revision-move page 2 unexpectedly returned no outcome"),
        // A ReadModelChanged from snapshot revalidation exhaustion is also an
        // acceptable "do not continue a stale snapshot" surface, but the
        // designed path is the structured reopen.
        Err(RepoError::ReadModelChanged) => Ok(()),
        Err(other) => Err(other.into()),
    }
}

// ===========================================================================
// Gate 5: append-to-visible (WI-11, BINDING D8 / R5)
// ===========================================================================
//
// The spec realtime contract (issue #598 §Realtime): a committed append to a
// live file-backed session must become visible through `open` within 2s p95 on
// the reference host. Per BINDING D8 the clock STARTS at durable canonical
// insert acknowledgment (`insert_json_rows_sync` returning) and STOPS at the
// first `open` that returns the appended event as visible/valid through the v2
// canonical reader. Two run-live-test modes exercise the gate with two distinct
// oracles, both against the same D8 clock:
//   * `append-to-visible`  -> [`append_to_visible`]  (open(session) turn presence)
//   * `fsync-to-open-valid` -> [`fsync_to_open_valid`] (open(event) via locator)

const APPEND_PROBE_SAMPLES: usize = 40;
const APPEND_PROBE_P95_BUDGET_MS: u128 = 2000;
const APPEND_PROBE_POLL_INTERVAL: Duration = Duration::from_millis(15);
const APPEND_PROBE_TIMEOUT: Duration = Duration::from_secs(8);

/// Seed a published + backfilled single-turn base session and return a
/// repository. Every later append reuses this already-published source head, so
/// the append is live the instant its `events` insert is durable.
async fn append_probe_setup(
    clickhouse: &ClickHouseClient,
    database: &OwnedDatabaseName,
) -> Result<ClickHouseConversationRepository> {
    let base = vec![
        Ev::new("probe-session", "probe-base-u0", 10).user(),
        Ev::new("probe-session", "probe-base-a0", 11),
    ];
    seed_events(clickhouse, &base).await?;
    publish_and_backfill(clickhouse, database).await?;
    Ok(ClickHouseConversationRepository::new(
        clickhouse.clone(),
        RepoConfig::default(),
    ))
}

/// The `i`-th append: a counter-path user+assistant turn whose `record_ts`
/// sorts strictly after every prior row (append-only, the live-ingest shape).
/// Returns the assistant event uid whose visibility ends the timed window.
fn append_probe_turn(i: usize) -> (Vec<Ev>, String) {
    let secs = 1000 + (i as u32) * 2;
    let user_uid = format!("probe-append-u{i}");
    let asst_uid = format!("probe-append-a{i}");
    let rows = vec![
        Ev::new("probe-session", &user_uid, secs).user(),
        Ev::new("probe-session", &asst_uid, secs + 1),
    ];
    (rows, asst_uid)
}

/// p95 in whole milliseconds by nearest-rank over the collected samples.
fn append_probe_p95_millis(mut samples: Vec<Duration>) -> u128 {
    assert!(
        !samples.is_empty(),
        "no append-to-visible samples collected"
    );
    samples.sort_unstable();
    let rank = ((samples.len() as f64) * 0.95).ceil() as usize;
    let idx = rank.saturating_sub(1).min(samples.len() - 1);
    samples[idx].as_millis()
}

/// Gate: open(session) turn-presence oracle. For each of `APPEND_PROBE_SAMPLES`
/// appends, start the clock at the durable insert ack and stop it when a full
/// v2 session traversal first reflects the new turn. Assert p95 <= 2000ms.
pub(super) async fn append_to_visible() -> Result<()> {
    with_owned_live_db(
        "canonical-open append-to-visible gate",
        |clickhouse, database| async move {
            clickhouse.run_migrations().await.context(
                "failed to migrate canonical-open append-to-visible database",
            )?;
            let repository = append_probe_setup(&clickhouse, &database).await?;

            // Baseline turn count (the single base turn), read outside the
            // timed window.
            let mut expected_turns = open_session_v2(&repository, "probe-session", 64)
                .await?
                .context("base session must open")?
                .turns
                .len();

            let mut samples = Vec::with_capacity(APPEND_PROBE_SAMPLES);
            for i in 0..APPEND_PROBE_SAMPLES {
                let (rows, _asst_uid) = append_probe_turn(i);
                // Durable insert; its acknowledgment is the D8 clock start.
                seed_events(&clickhouse, &rows).await?;
                let started = Instant::now();
                expected_turns += 1;
                loop {
                    let visible = open_session_v2(&repository, "probe-session", 64)
                        .await?
                        .map(|session| session.turns.len() >= expected_turns)
                        .unwrap_or(false);
                    if visible {
                        break;
                    }
                    if started.elapsed() > APPEND_PROBE_TIMEOUT {
                        bail!(
                            "append {i} not visible via open(session) within {:?}",
                            APPEND_PROBE_TIMEOUT
                        );
                    }
                    tokio::time::sleep(APPEND_PROBE_POLL_INTERVAL).await;
                }
                samples.push(started.elapsed());
            }

            let p95 = append_probe_p95_millis(samples);
            assert!(
                p95 <= APPEND_PROBE_P95_BUDGET_MS,
                "insert-ack -> open(session)-visible p95 {p95}ms exceeds the {APPEND_PROBE_P95_BUDGET_MS}ms budget"
            );
            Ok(())
        },
    )
    .await
}

/// Gate: open(event)-via-locator oracle. Same D8 clock, but the timed window
/// ends when `canonical_open_event` first resolves the appended event through
/// `mcp_event_locator`. Assert p95 <= 2000ms.
pub(super) async fn fsync_to_open_valid() -> Result<()> {
    with_owned_live_db(
        "canonical-open fsync-to-open-valid gate",
        |clickhouse, database| async move {
            clickhouse.run_migrations().await.context(
                "failed to migrate canonical-open fsync-to-open-valid database",
            )?;
            let repository = append_probe_setup(&clickhouse, &database).await?;

            let mut samples = Vec::with_capacity(APPEND_PROBE_SAMPLES);
            for i in 0..APPEND_PROBE_SAMPLES {
                let (rows, asst_uid) = append_probe_turn(i);
                seed_events(&clickhouse, &rows).await?;
                let started = Instant::now();
                loop {
                    if repository.canonical_open_event(&asst_uid).await?.is_some() {
                        break;
                    }
                    if started.elapsed() > APPEND_PROBE_TIMEOUT {
                        bail!(
                            "append {i} ({asst_uid}) not visible via open(event) within {:?}",
                            APPEND_PROBE_TIMEOUT
                        );
                    }
                    tokio::time::sleep(APPEND_PROBE_POLL_INTERVAL).await;
                }
                samples.push(started.elapsed());
            }

            let p95 = append_probe_p95_millis(samples);
            assert!(
                p95 <= APPEND_PROBE_P95_BUDGET_MS,
                "insert-ack -> open(event)-valid p95 {p95}ms exceeds the {APPEND_PROBE_P95_BUDGET_MS}ms budget"
            );
            Ok(())
        },
    )
    .await
}
