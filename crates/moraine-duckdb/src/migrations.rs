use duckdb::{params, Connection, OptionalExt, Transaction};
use sha2::{Digest, Sha256};

use crate::{DuckDbError, DuckDbResult};

const INITIAL_SQL: &str = include_str!("../../../sql/duckdb/001_initial.sql");

#[derive(Debug, Clone, Copy)]
pub struct Migration {
    pub version: u32,
    pub filename: &'static str,
    pub sql: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppliedMigration {
    pub version: u32,
    pub filename: String,
    pub checksum: String,
}

pub fn bundled_migrations() -> &'static [Migration] {
    &[Migration {
        version: 1,
        filename: "001_initial.sql",
        sql: INITIAL_SQL,
    }]
}

fn checksum(sql: &str) -> String {
    format!("{:x}", Sha256::digest(sql.as_bytes()))
}

fn applied_migrations(connection: &Connection) -> DuckDbResult<Vec<AppliedMigration>> {
    let ledger_exists: bool = connection.query_row(
        "SELECT count(*) > 0 FROM information_schema.tables WHERE table_schema = 'main' AND table_name = 'moraine_schema_migrations'",
        [],
        |row| row.get(0),
    )?;
    if !ledger_exists {
        return Ok(Vec::new());
    }
    let mut statement = connection.prepare(
        "SELECT version, filename, checksum FROM moraine_schema_migrations ORDER BY version",
    )?;
    let rows = statement.query_map([], |row| {
        Ok(AppliedMigration {
            version: row.get(0)?,
            filename: row.get(1)?,
            checksum: row.get(2)?,
        })
    })?;
    rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
}

fn ledger_exists(connection: &Connection) -> DuckDbResult<bool> {
    connection
        .query_row(
            "SELECT count(*) > 0 FROM information_schema.tables WHERE table_schema = 'main' AND table_name = 'moraine_schema_migrations'",
            [],
            |row| row.get(0),
        )
        .map_err(Into::into)
}

fn reject_foreign_or_partial_database(
    connection: &Connection,
    applied: &[AppliedMigration],
) -> DuckDbResult<()> {
    if ledger_exists(connection)? {
        if applied.is_empty() {
            return Err(DuckDbError::MigrationInvariant(
                "migration ledger exists but has no applied versions".to_string(),
            ));
        }
        return Ok(());
    }
    let user_table_count: u64 = connection.query_row(
        "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'main'",
        [],
        |row| row.get(0),
    )?;
    if user_table_count > 0 {
        return Err(DuckDbError::MigrationInvariant(
            "database contains tables but no Moraine migration ledger".to_string(),
        ));
    }
    Ok(())
}

fn validate_ledger(applied: &[AppliedMigration]) -> DuckDbResult<()> {
    let supported = bundled_migrations().last().map_or(0, |item| item.version);
    if let Some(newest) = applied.last().filter(|item| item.version > supported) {
        return Err(DuckDbError::NewerSchema {
            database: newest.version,
            supported,
        });
    }
    for migration in applied {
        let Some(bundled) = bundled_migrations()
            .iter()
            .find(|item| item.version == migration.version)
        else {
            return Err(DuckDbError::MigrationInvariant(format!(
                "unknown migration version {}",
                migration.version
            )));
        };
        if migration.filename != bundled.filename {
            return Err(DuckDbError::MigrationInvariant(format!(
                "migration {} filename is `{}` instead of `{}`",
                migration.version, migration.filename, bundled.filename
            )));
        }
        if migration.checksum != checksum(bundled.sql) {
            return Err(DuckDbError::MigrationChecksum {
                version: migration.version,
            });
        }
    }
    Ok(())
}

fn apply_one(transaction: &Transaction<'_>, migration: Migration) -> DuckDbResult<()> {
    transaction.execute_batch(migration.sql)?;
    transaction.execute(
        "INSERT INTO moraine_schema_migrations (version, filename, checksum, applied_at) VALUES (?, ?, ?, current_timestamp)",
        params![migration.version, migration.filename, checksum(migration.sql)],
    )?;
    Ok(())
}

pub(crate) fn migrate(connection: &mut Connection) -> DuckDbResult<Vec<AppliedMigration>> {
    let applied = applied_migrations(connection)?;
    reject_foreign_or_partial_database(connection, &applied)?;
    validate_ledger(&applied)?;
    let latest = applied.last().map_or(0, |item| item.version);
    for migration in bundled_migrations()
        .iter()
        .copied()
        .filter(|item| item.version > latest)
    {
        let transaction = connection.transaction()?;
        apply_one(&transaction, migration)?;
        transaction.commit()?;
    }
    applied_migrations(connection)
}

pub(crate) fn schema_version(connection: &Connection) -> DuckDbResult<u32> {
    connection
        .query_row(
            "SELECT max(version) FROM moraine_schema_migrations",
            [],
            |row| row.get::<_, Option<u32>>(0),
        )
        .optional()?
        .flatten()
        .ok_or_else(|| DuckDbError::MigrationInvariant("migration ledger is empty".to_string()))
}
