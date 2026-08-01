//! Issue #603 WI-02 — storage observability.
//!
//! Before this module, nothing anywhere in the repository queried
//! `system.disks`, `free_space`, or `total_bytes_on_disk`; `TableSummary`
//! carried `{name, engine, is_temporary, rows}` and the operations probe
//! selected `sum(rows)` only. The reference host filled its disk twice with no
//! Moraine-native signal available at any point.
//!
//! **Nothing here deletes anything.** It is measurement, and it ships before
//! the ledger deliberately: a reclaimer whose effect cannot be observed is a
//! reclaimer whose effect cannot be reviewed.
//!
//! ## Why `oldest_retained` comes from `system.parts` and not from the data
//!
//! The obvious implementation is `SELECT min(ingested_at) FROM <table>` per
//! table. That is a column scan per table on every status/health request, and
//! plan hazard H4 is specifically about unbounded probes for sizing. Measured
//! on the reference host (2026-07-27), `system.parts.min_time` is already
//! populated for every table whose partition key is a time expression
//! (`toYYYYMM(ingested_at)` / `toYYYYMM(ts)`) and holds the epoch sentinel for
//! every other table. So the entire storage report — bytes, rows, parts, and
//! the oldest retained timestamp — is **one** `system.parts` aggregate plus
//! **one** `system.disks` read, and reads no user data at all.
//!
//! The cost of that choice is honest and documented: `oldest_retained` is
//! `None` for tables with no time-typed partition key, rather than wrong.

use std::collections::BTreeMap;

use anyhow::{Context, Result};
use moraine_config::{ProtectedRetentionBucket, RetentionConfig, RetentionHorizon};
use serde::{Deserialize, Serialize};

use crate::storage_class::{classify, TableClass, CLASSIFIED_TABLES};
use crate::{escape_literal, ClickHouseClient};

/// `min_time` for a part in a table with no time-typed partition key. ClickHouse
/// reports the Unix epoch rather than NULL, and an epoch timestamp presented as
/// "oldest retained row" would be a wrong answer dressed as a precise one.
const EPOCH_SENTINEL_SECONDS: i64 = 0;

/// Per-table physical storage facts, exact at query time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageTableReport {
    pub name: String,
    /// The §1 bucket, surfaced. `None` only for a table present in the
    /// database that [`classify`] does not know — which is itself the signal.
    pub class: Option<TableClass>,
    pub rows: u64,
    pub compressed_bytes: u64,
    pub uncompressed_bytes: u64,
    pub active_parts: u64,
    /// Oldest retained row, ISO-8601, for tables with a time-typed partition
    /// key. `None` for untimed control tables and for hash-partitioned derived
    /// tables — see the module docs.
    pub oldest_retained: Option<String>,
}

/// Fold of [`StorageTableReport`] by ownership class.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageBucketReport {
    pub class: TableClass,
    /// Human-readable bucket name, so a JSON consumer need not re-derive it.
    pub label: String,
    pub tables: u64,
    pub rows: u64,
    pub compressed_bytes: u64,
}

/// Free/total space on the disk backing the ClickHouse data path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageDiskReport {
    pub free_bytes: u64,
    pub total_bytes: u64,
}

impl StorageDiskReport {
    pub fn used_bytes(&self) -> u64 {
        self.total_bytes.saturating_sub(self.free_bytes)
    }
}

/// Where a policy value came from. A `configured` protected-bucket policy is
/// the only thing in this report that can lead to user data being deleted, so
/// the provenance is a first-class field rather than a rendering detail.
pub const POLICY_SOURCE_DEFAULT: &str = "default";
pub const POLICY_SOURCE_CONFIGURED: &str = "configured";

/// One row of the effective retention policy.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RetentionPolicyEntry {
    pub class: TableClass,
    /// Horizon in seconds, or `None` when the class has no horizon at all
    /// (`never_delete`) or none is configured (buckets 1 and 2 by default).
    pub horizon_seconds: Option<f64>,
    /// [`POLICY_SOURCE_DEFAULT`] or [`POLICY_SOURCE_CONFIGURED`].
    pub source: String,
    /// The config key an operator would set to change it, when there is one.
    pub config_key: Option<String>,
    /// A qualification the number alone would misstate, rendered inline by
    /// every surface that prints the entry.
    ///
    /// Exists for exactly one entry today: bucket 4's horizon is baked into
    /// `sql/039_telemetry_retention.sql` as static text, so `config_key` names
    /// a key that cannot move the number. Reporting the key with no note would
    /// invite an operator to set it and believe the answer.
    pub note: Option<String>,
}

impl RetentionPolicyEntry {
    /// Whether this entry authorizes deleting user history. Exactly the
    /// condition `moraine status` promotes to a prominent line.
    pub fn is_destructive(&self) -> bool {
        self.class.is_protected()
            && self.class != TableClass::NeverDelete
            && self.horizon_seconds.is_some()
    }
}

/// The WI-02 storage surface: per-table and per-bucket bytes, disk headroom,
/// and the effective retention policy. Carries no estimates and no reclaim
/// state — those belong to [`crate::reclaim::ReclaimStatusReport`], which
/// composes this.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StorageReport {
    pub tables: Vec<StorageTableReport>,
    pub buckets: Vec<StorageBucketReport>,
    pub disk: Option<StorageDiskReport>,
    pub policy: Vec<RetentionPolicyEntry>,
    /// Non-fatal degradations (a `system.disks` read that failed, a table in
    /// the database that carries no class). Present so a partial report is
    /// distinguishable from a complete one without comparing field counts.
    pub notes: Vec<String>,
}

impl StorageReport {
    pub fn total_compressed_bytes(&self) -> u64 {
        self.buckets
            .iter()
            .map(|bucket| bucket.compressed_bytes)
            .sum()
    }

    pub fn bucket(&self, class: TableClass) -> Option<&StorageBucketReport> {
        self.buckets.iter().find(|bucket| bucket.class == class)
    }

    /// Policy entries that authorize deleting user history, if any.
    pub fn destructive_policies(&self) -> Vec<&RetentionPolicyEntry> {
        self.policy
            .iter()
            .filter(|entry| entry.is_destructive())
            .collect()
    }

    /// Tables present in the database that [`classify`] does not know. An
    /// operator-visible restatement of the §4 S1 invariant that the unit test
    /// enforces at build time.
    pub fn unclassified_tables(&self) -> Vec<&str> {
        self.tables
            .iter()
            .filter(|table| table.class.is_none())
            .map(|table| table.name.as_str())
            .collect()
    }
}

/// Provenance of a value that has a serde default, decided by comparing it to
/// that default.
///
/// The two protected buckets are `Option`s, so their provenance is exact:
/// absent means default. `derived_horizon_hours` and `telemetry_horizon_days`
/// are not — they carry `#[serde(default = …)]` and land in the struct as
/// plain values, so nothing downstream can distinguish "unset" from "set to
/// the default". Reporting both as `default` unconditionally was wrong in a
/// user-visible way: an operator who had configured either one read `default`
/// back from `/api/v1/health` and `moraine db doctor`.
///
/// The residual imprecision is one-directional and stated rather than hidden:
/// a config that sets a bucket to exactly the shipped default reads as
/// `default`. That is the safe direction — the only entries whose provenance
/// gates behaviour are the two protected ones, and those are exact.
fn source_of(value: f64, default: f64) -> String {
    if (value - default).abs() < f64::EPSILON {
        POLICY_SOURCE_DEFAULT.to_string()
    } else {
        POLICY_SOURCE_CONFIGURED.to_string()
    }
}

/// The qualification bucket 4's entry carries. See
/// [`RetentionPolicyEntry::note`].
pub const TELEMETRY_HORIZON_NOT_CONFIGURABLE_NOTE: &str =
    "fixed by sql/039_telemetry_retention.sql; this key is not yet operator-configurable and a \
     config load rejects any other value";

/// Build the config-derived half of the report. Pure, so the rendering and
/// "is any destructive policy active" logic is testable without a server.
pub fn retention_policy_entries(retention: &RetentionConfig) -> Vec<RetentionPolicyEntry> {
    let protected = |bucket: ProtectedRetentionBucket| {
        let horizon = RetentionHorizon::from_config(retention, bucket);
        RetentionPolicyEntry {
            class: match bucket {
                ProtectedRetentionBucket::RawAudit => TableClass::RawAudit,
                ProtectedRetentionBucket::CanonicalHistory => TableClass::CanonicalHistory,
            },
            horizon_seconds: horizon.map(|horizon| horizon.seconds()),
            source: if horizon.is_some() {
                POLICY_SOURCE_CONFIGURED.to_string()
            } else {
                POLICY_SOURCE_DEFAULT.to_string()
            },
            config_key: Some(bucket.config_key().to_string()),
            note: None,
        }
    };
    vec![
        protected(ProtectedRetentionBucket::CanonicalHistory),
        protected(ProtectedRetentionBucket::RawAudit),
        RetentionPolicyEntry {
            class: TableClass::Derived,
            horizon_seconds: Some(retention.derived_horizon_seconds()),
            source: source_of(
                retention.derived_horizon_hours,
                moraine_config::DEFAULT_RETENTION_DERIVED_HORIZON_HOURS,
            ),
            config_key: Some("retention.derived_horizon_hours".to_string()),
            note: None,
        },
        RetentionPolicyEntry {
            class: TableClass::Telemetry,
            // Reported from the constant, not from the field. The TTL that
            // enforces this horizon is `INTERVAL 30 DAY` written four times in
            // static migration SQL, so the field cannot move it; a
            // programmatically built `RetentionConfig` that bypasses
            // `validate_retention` (tests, and any future in-process caller)
            // must not be able to make this line disagree with what ClickHouse
            // will do.
            horizon_seconds: Some(
                f64::from(moraine_config::DEFAULT_RETENTION_TELEMETRY_HORIZON_DAYS) * 86_400.0,
            ),
            // Never `configured`: there is no value of the key that changes
            // the applied horizon, and `configured` is a claim about effect.
            source: POLICY_SOURCE_DEFAULT.to_string(),
            config_key: Some("retention.telemetry_horizon_days".to_string()),
            note: Some(TELEMETRY_HORIZON_NOT_CONFIGURABLE_NOTE.to_string()),
        },
        RetentionPolicyEntry {
            class: TableClass::NeverDelete,
            horizon_seconds: None,
            source: POLICY_SOURCE_DEFAULT.to_string(),
            config_key: None,
            note: None,
        },
    ]
}

/// Fold per-table rows into per-bucket totals, in [`TableClass::ALL`] order so
/// two reports of the same database are byte-comparable.
pub fn fold_buckets(tables: &[StorageTableReport]) -> Vec<StorageBucketReport> {
    TableClass::ALL
        .iter()
        .map(|class| {
            let matching = tables
                .iter()
                .filter(|table| table.class == Some(*class))
                .collect::<Vec<_>>();
            StorageBucketReport {
                class: *class,
                label: class.label().to_string(),
                tables: matching.len() as u64,
                rows: matching.iter().map(|table| table.rows).sum(),
                compressed_bytes: matching.iter().map(|table| table.compressed_bytes).sum(),
            }
        })
        .collect()
}

/// `system.parts` aggregate for one database. Restricted to `active` parts:
/// without that filter, inactive parts awaiting cleanup keep a table's name
/// and byte total present long after its rows are gone, which is exactly why
/// the pre-existing live storage assertion in `source_publication.rs` could
/// not fail on over-deletion.
pub(crate) fn storage_parts_sql(database: &str) -> String {
    format!(
        "SELECT\n  \
           table AS name,\n  \
           toUInt64(sum(rows)) AS rows,\n  \
           toUInt64(sum(data_compressed_bytes)) AS compressed_bytes,\n  \
           toUInt64(sum(data_uncompressed_bytes)) AS uncompressed_bytes,\n  \
           toUInt64(count()) AS active_parts,\n  \
           toInt64(toUnixTimestamp(min(min_time))) AS oldest_retained_unix,\n  \
           formatDateTime(min(min_time), '%Y-%m-%dT%H:%i:%SZ', 'UTC') AS oldest_retained_text\n \
         FROM system.parts\n \
         WHERE database = {}\n   \
           AND active\n \
         GROUP BY table\n \
         ORDER BY table ASC\n \
         FORMAT JSONEachRow",
        escape_literal(database)
    )
}

/// Free/total bytes of the disk backing the ClickHouse data path.
pub(crate) fn storage_disk_sql() -> &'static str {
    "SELECT\n  \
       toUInt64(sum(free_space)) AS free_bytes,\n  \
       toUInt64(sum(total_space)) AS total_bytes\n \
     FROM system.disks\n \
     WHERE name = 'default'\n \
     FORMAT JSONEachRow"
}

#[derive(Debug, Deserialize)]
struct StoragePartsRow {
    name: String,
    rows: u64,
    compressed_bytes: u64,
    uncompressed_bytes: u64,
    active_parts: u64,
    oldest_retained_unix: i64,
    oldest_retained_text: String,
}

#[derive(Debug, Deserialize)]
struct StorageDiskRow {
    free_bytes: u64,
    total_bytes: u64,
}

/// Drop the epoch sentinel from the server-formatted timestamp. Kept separate
/// from the query so the sentinel rule is unit testable without a server.
pub(crate) fn oldest_retained_label(unix_seconds: i64, formatted: &str) -> Option<String> {
    if unix_seconds <= EPOCH_SENTINEL_SECONDS || formatted.is_empty() {
        return None;
    }
    Some(formatted.to_string())
}

impl ClickHouseClient {
    /// One `system.parts` aggregate plus one `system.disks` read, folded with
    /// the [`crate::storage_class`] ownership model and the effective
    /// `[retention]` policy.
    ///
    /// Reads no user data. A `system.disks` failure degrades to
    /// `disk: None` plus a note rather than failing the whole report, matching
    /// the never-fails shape the core-index report established: storage state
    /// is transient and must not fail a doctor exit code.
    pub async fn storage_report(&self, retention: &RetentionConfig) -> Result<StorageReport> {
        let parts: Vec<StoragePartsRow> = self
            .query_json_each_row(&storage_parts_sql(&self.cfg.database), Some("system"))
            .await
            .context("failed to read per-table storage from system.parts")?;

        let mut notes = Vec::new();
        let tables: Vec<StorageTableReport> = parts
            .into_iter()
            .map(|row| StorageTableReport {
                class: classify(&row.name),
                name: row.name,
                rows: row.rows,
                compressed_bytes: row.compressed_bytes,
                uncompressed_bytes: row.uncompressed_bytes,
                active_parts: row.active_parts,
                oldest_retained: oldest_retained_label(
                    row.oldest_retained_unix,
                    &row.oldest_retained_text,
                ),
            })
            .collect();

        let unclassified: Vec<&str> = tables
            .iter()
            .filter(|table| table.class.is_none())
            .map(|table| table.name.as_str())
            .collect();
        if !unclassified.is_empty() {
            notes.push(format!(
                "unclassified tables present in the database (no reclaim scope may name them): {}",
                unclassified.join(", ")
            ));
        }

        let disk = match self
            .query_json_each_row::<StorageDiskRow>(storage_disk_sql(), Some("system"))
            .await
        {
            Ok(rows) => rows.into_iter().next().map(|row| StorageDiskReport {
                free_bytes: row.free_bytes,
                total_bytes: row.total_bytes,
            }),
            Err(error) => {
                notes.push(format!("disk headroom unavailable: {error:#}"));
                None
            }
        };

        Ok(StorageReport {
            buckets: fold_buckets(&tables),
            tables,
            disk,
            policy: retention_policy_entries(retention),
            notes,
        })
    }
}

/// Names in [`CLASSIFIED_TABLES`] that a live database is expected to carry,
/// for the doctor's benefit. Excludes the system logs, which live in another
/// database.
pub fn expected_moraine_tables() -> BTreeMap<&'static str, TableClass> {
    CLASSIFIED_TABLES
        .iter()
        .filter(|entry| !entry.name.starts_with("system."))
        .map(|entry| (entry.name, entry.class))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table(name: &str, class: Option<TableClass>, rows: u64, bytes: u64) -> StorageTableReport {
        StorageTableReport {
            name: name.to_string(),
            class,
            rows,
            compressed_bytes: bytes,
            uncompressed_bytes: bytes * 3,
            active_parts: 1,
            oldest_retained: None,
        }
    }

    #[test]
    fn parts_query_reads_only_active_parts_and_never_the_table_itself() {
        let sql = storage_parts_sql("moraine");
        assert!(sql.contains("FROM system.parts"));
        assert!(
            sql.contains("AND active"),
            "without the active filter, inactive parts keep a table's bytes present after its \
             rows are gone — the exact reason the pre-existing live storage assertion could not \
             fail on over-deletion"
        );
        assert!(sql.contains("database = 'moraine'"));
        // H4: no FINAL, no read of any user relation.
        assert!(!sql.contains("FINAL"));
        assert!(!sql.contains("moraine."));
        // ClickHouse's `formatDateTime` uses `%i` for minutes; `%M` is the
        // full month NAME. Verified against the live host 2026-07-27, where
        // `%M` rendered `2026-02-20T14:February:47Z`.
        assert!(
            sql.contains("'%Y-%m-%dT%H:%i:%SZ'"),
            "minutes are %i in ClickHouse; %M renders the month name"
        );
        assert!(!sql.contains("%M"));
    }

    #[test]
    fn disk_query_reads_only_system_disks() {
        let sql = storage_disk_sql();
        assert!(sql.contains("FROM system.disks"));
        assert!(sql.contains("free_space"));
        assert!(sql.contains("total_space"));
        assert!(!sql.contains("FINAL"));
    }

    #[test]
    fn the_epoch_sentinel_is_reported_as_no_oldest_row_rather_than_1970() {
        // Verified on the reference host 2026-07-27: every table without a
        // time-typed partition key reports min_time at the Unix epoch.
        assert_eq!(oldest_retained_label(0, "1970-01-01T00:00:00Z"), None);
        assert_eq!(oldest_retained_label(-1, "1969-12-31T23:59:59Z"), None);
        assert_eq!(oldest_retained_label(1_771_579_005, ""), None);
        assert_eq!(
            oldest_retained_label(1_771_579_005, "2026-02-20T09:16:45Z"),
            Some("2026-02-20T09:16:45Z".to_string())
        );
    }

    #[test]
    fn buckets_fold_every_class_even_when_empty() {
        let tables = vec![
            table("events", Some(TableClass::CanonicalHistory), 10, 100),
            table("raw_events", Some(TableClass::RawAudit), 20, 200),
            table("mcp_event_navigation", Some(TableClass::Derived), 30, 300),
            table("mcp_event_locator", Some(TableClass::Derived), 40, 400),
            table("mystery_table", None, 50, 500),
        ];
        let buckets = fold_buckets(&tables);
        assert_eq!(buckets.len(), TableClass::ALL.len());
        let derived = buckets
            .iter()
            .find(|bucket| bucket.class == TableClass::Derived)
            .expect("derived bucket");
        assert_eq!(derived.tables, 2);
        assert_eq!(derived.rows, 70);
        assert_eq!(derived.compressed_bytes, 700);
        let telemetry = buckets
            .iter()
            .find(|bucket| bucket.class == TableClass::Telemetry)
            .expect("telemetry bucket must be present even at zero");
        assert_eq!(telemetry.tables, 0);
        // The unclassified table contributes to no bucket: it must never be
        // silently folded into "derived".
        assert_eq!(
            buckets.iter().map(|bucket| bucket.rows).sum::<u64>(),
            10 + 20 + 30 + 40
        );
    }

    /// The policy half of **G-DEFAULT**: stock configuration surfaces no
    /// destructive policy, and a configured one is flagged as destructive.
    ///
    /// MUTATION (executed 2026-07-27): make `is_destructive` return
    /// `self.horizon_seconds.is_some()` (dropping the `is_protected` guard) =>
    /// the derived and telemetry entries become "destructive" and the
    /// `default policy is not destructive` assertion FAILS. Bounds the
    /// direction *a harmless policy is reported as destructive*; the
    /// `configured` half below bounds *a destructive policy is reported as
    /// harmless*.
    #[test]
    fn default_retention_surfaces_no_destructive_policy() {
        let policy = retention_policy_entries(&RetentionConfig::default());
        assert_eq!(policy.len(), TableClass::ALL.len());
        assert!(
            policy.iter().all(|entry| !entry.is_destructive()),
            "default policy is not destructive: {policy:#?}"
        );
        for class in [TableClass::CanonicalHistory, TableClass::RawAudit] {
            let entry = policy
                .iter()
                .find(|entry| entry.class == class)
                .expect("protected entry");
            assert_eq!(entry.horizon_seconds, None);
            assert_eq!(entry.source, POLICY_SOURCE_DEFAULT);
            assert!(entry.config_key.is_some(), "a refusal must name the key");
        }
        let derived = policy
            .iter()
            .find(|entry| entry.class == TableClass::Derived)
            .expect("derived entry");
        assert_eq!(derived.horizon_seconds, Some(86_400.0));
        let never = policy
            .iter()
            .find(|entry| entry.class == TableClass::NeverDelete)
            .expect("never-delete entry");
        assert_eq!(never.horizon_seconds, None);
        assert!(!never.is_destructive());
    }

    /// Provenance is reported for **all four** horizon buckets, not only the
    /// two whose provenance gates deletion.
    ///
    /// An operator who sets `retention.derived_horizon_hours` reads the value
    /// back from `moraine status`, `moraine db doctor`, and
    /// `/api/v1/health`; reporting `default` next to their own number is the
    /// kind of small lie that costs an hour of debugging at the worst moment.
    ///
    /// MUTATION (executed 2026-07-27): hard-code
    /// `source: POLICY_SOURCE_DEFAULT` for the derived entry (the state this
    /// PR shipped in) => FAILS on the configured case below. **Lower bound.**
    ///
    /// MUTATION (executed 2026-07-27): hard-code
    /// `source: POLICY_SOURCE_CONFIGURED` for it => FAILS on
    /// `default_retention_surfaces_no_destructive_policy`, which asserts
    /// `POLICY_SOURCE_DEFAULT` under a stock config. **Upper bound.**
    ///
    /// Telemetry is deliberately **not** in the configured loop: its horizon
    /// is not operator-configurable at all, and
    /// `the_telemetry_horizon_never_reports_itself_as_configured` is its
    /// counterpart guard.
    #[test]
    fn every_horizon_bucket_reports_its_own_provenance() {
        let stock = retention_policy_entries(&RetentionConfig::default());
        for class in TableClass::ALL {
            let entry = stock
                .iter()
                .find(|entry| entry.class == class)
                .expect("every class has a policy row");
            assert_eq!(
                entry.source, POLICY_SOURCE_DEFAULT,
                "{class:?} is unconfigured under a stock config"
            );
        }

        let configured = retention_policy_entries(&RetentionConfig {
            derived_horizon_hours: 6.0,
            ..RetentionConfig::default()
        });
        let derived = configured
            .iter()
            .find(|entry| entry.class == TableClass::Derived)
            .expect("derived policy row");
        assert_eq!(
            derived.source, POLICY_SOURCE_CONFIGURED,
            "a configured derived horizon must not read back as `default`"
        );
        assert_eq!(derived.horizon_seconds, Some(6.0 * 3_600.0));
        // Configuring a bucket-3 horizon is still not destructive: only the
        // two protected buckets can be.
        assert!(!derived.is_destructive());

        // The protected buckets keep their exact provenance, which is what the
        // destructive-policy warning is derived from.
        let protected_configured = retention_policy_entries(&RetentionConfig {
            raw_audit_horizon_days: Some(90.0),
            ..RetentionConfig::default()
        });
        let raw = protected_configured
            .iter()
            .find(|entry| entry.class == TableClass::RawAudit)
            .expect("raw entry");
        assert_eq!(raw.source, POLICY_SOURCE_CONFIGURED);
        assert!(raw.is_destructive());
    }

    /// **G-TELEMETRY-HONESTY.** Fails for: a reported telemetry horizon that
    /// differs from the one `sql/039_telemetry_retention.sql` actually applies,
    /// and for reporting it as `configured`.
    /// Denomination: exact seconds, exact source string, note present.
    ///
    /// The regression: `retention.telemetry_horizon_days` is reported by
    /// `moraine db reclaim status`, `moraine db doctor` and `/api/v1/health`,
    /// but migration 039 writes `INTERVAL 30 DAY` four times as static text.
    /// An operator who set 90 read back "configured, 90 days" while ClickHouse
    /// deleted at 30 — a silent shortening relative to stated intent, on the
    /// only surface that stated it. Setting 1 was equally ineffective the
    /// other way.
    ///
    /// Two mechanisms, because the config-load rejection cannot see a
    /// `RetentionConfig` built in code. `validate_retention` refuses the key
    /// at load (`telemetry_horizon_days_is_not_yet_configurable` in
    /// moraine-config); this asserts the report is honest even for the
    /// bypassed case.
    ///
    /// MUTATION (executed 2026-07-27): restore
    /// `horizon_seconds: Some(f64::from(retention.telemetry_horizon_days) *
    /// 86_400.0)` => FAILS on the 90-day case. **Lower bound.**
    ///
    /// MUTATION (executed 2026-07-27): restore `source: source_of(…)` for the
    /// telemetry entry => FAILS on the source assertion for the 90-day case.
    ///
    /// MUTATION (executed 2026-07-27): drop the `note` => FAILS here, and
    /// `render.rs`'s `status_lines_render_the_telemetry_horizon_caveat`
    /// FAILS too. **Width: the caveat has to reach the operator, not just
    /// exist in a struct.**
    #[test]
    fn the_telemetry_horizon_never_reports_itself_as_configured() {
        let applied =
            f64::from(moraine_config::DEFAULT_RETENTION_TELEMETRY_HORIZON_DAYS) * 86_400.0;
        for days in [
            moraine_config::DEFAULT_RETENTION_TELEMETRY_HORIZON_DAYS,
            1,
            90,
        ] {
            // Built in code, so it bypasses `validate_retention` exactly the
            // way a future in-process caller would.
            let policy = retention_policy_entries(&RetentionConfig {
                telemetry_horizon_days: days,
                ..RetentionConfig::default()
            });
            let entry = policy
                .iter()
                .find(|entry| entry.class == TableClass::Telemetry)
                .expect("telemetry policy row");
            assert_eq!(
                entry.horizon_seconds,
                Some(applied),
                "the reported horizon must be the one migration 039 applies, not the one the \
                 field holds (field was {days})"
            );
            assert_eq!(
                entry.source, POLICY_SOURCE_DEFAULT,
                "`configured` is a claim about effect, and this key has none (field was {days})"
            );
            assert_eq!(
                entry.note.as_deref(),
                Some(TELEMETRY_HORIZON_NOT_CONFIGURABLE_NOTE),
                "the entry names a config key, so it must also say the key does nothing"
            );
            assert_eq!(
                entry.config_key.as_deref(),
                Some("retention.telemetry_horizon_days")
            );
        }

        // Upper bound: the note is for this entry alone. A note on every row
        // would make the caveat unreadable and unfalsifiable.
        let policy = retention_policy_entries(&RetentionConfig::default());
        let noted: Vec<TableClass> = policy
            .iter()
            .filter(|entry| entry.note.is_some())
            .map(|entry| entry.class)
            .collect();
        assert_eq!(noted, vec![TableClass::Telemetry]);
    }

    #[test]
    fn configured_protected_retention_is_reported_as_destructive() {
        let retention = RetentionConfig {
            canonical_history_horizon_days: Some(365.0),
            ..RetentionConfig::default()
        };
        let policy = retention_policy_entries(&retention);
        let canonical = policy
            .iter()
            .find(|entry| entry.class == TableClass::CanonicalHistory)
            .expect("canonical entry");
        assert_eq!(canonical.horizon_seconds, Some(365.0 * 86_400.0));
        assert_eq!(canonical.source, POLICY_SOURCE_CONFIGURED);
        assert!(canonical.is_destructive());
        // Raw/audit was not configured and must stay non-destructive: one
        // configured bucket may not imply the other.
        let raw = policy
            .iter()
            .find(|entry| entry.class == TableClass::RawAudit)
            .expect("raw entry");
        assert!(!raw.is_destructive());
    }

    #[test]
    fn expected_tables_exclude_system_logs() {
        let expected = expected_moraine_tables();
        assert!(expected.contains_key("events"));
        assert!(expected.contains_key("storage_reclaim_ledger"));
        assert!(!expected.keys().any(|name| name.starts_with("system.")));
    }
}
