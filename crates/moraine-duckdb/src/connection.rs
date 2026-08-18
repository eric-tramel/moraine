use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use duckdb::{params, Connection};
use moraine_config::{AppConfig, StorageEngine};
use moraine_conversations::RepoConfig;

use crate::migrations::{migrate, schema_version};
use crate::security::{prepare_paths, secure_database_file, OwnerLock};
use crate::{AppliedMigration, DuckDbError, DuckDbResult};

#[derive(Debug, Clone)]
pub struct DuckDbStoreOptions {
    pub runtime_root: PathBuf,
    pub database_path: PathBuf,
    pub temp_directory: PathBuf,
    pub memory_limit_mb: u32,
    pub threads: u16,
    pub read_connections: u16,
    pub max_temp_directory_size_mb: u32,
}

impl DuckDbStoreOptions {
    pub fn for_runtime_root(root: impl Into<PathBuf>) -> Self {
        let runtime_root = root.into();
        Self {
            database_path: runtime_root.join("duckdb/moraine.duckdb"),
            temp_directory: runtime_root.join("duckdb/tmp"),
            runtime_root,
            memory_limit_mb: 512,
            threads: 2,
            read_connections: 2,
            max_temp_directory_size_mb: 2048,
        }
    }

    pub fn from_config(config: &AppConfig) -> DuckDbResult<Self> {
        if config.storage.engine != StorageEngine::Duckdb {
            return Err(DuckDbError::InvalidPath(
                "DuckDB store requested while storage.engine is not `duckdb`".to_string(),
            ));
        }
        Ok(Self {
            runtime_root: PathBuf::from(&config.runtime.root_dir),
            database_path: PathBuf::from(&config.duckdb.database_path),
            temp_directory: PathBuf::from(&config.duckdb.temp_directory),
            memory_limit_mb: config.duckdb.memory_limit_mb,
            threads: config.duckdb.threads,
            read_connections: config.duckdb.read_connections,
            max_temp_directory_size_mb: config.duckdb.max_temp_directory_size_mb,
        })
    }
}

pub struct DuckDbStore {
    pub(crate) writer: Mutex<Option<Connection>>,
    pub(crate) readers: Arc<Mutex<Vec<Connection>>>,
    pub(crate) reader_admission: Arc<tokio::sync::Semaphore>,
    read_connection_count: u32,
    migrations: Vec<AppliedMigration>,
    pub(crate) database_path: PathBuf,
    pub(crate) repository_config: RepoConfig,
    pub(crate) engine_version: String,
    pub(crate) shutting_down: Arc<AtomicBool>,
    owner_lock: Mutex<Option<OwnerLock>>,
}

impl DuckDbStore {
    pub async fn open(options: DuckDbStoreOptions) -> DuckDbResult<Arc<Self>> {
        Self::open_with_repository_config(options, RepoConfig::default()).await
    }

    pub async fn open_with_repository_config(
        options: DuckDbStoreOptions,
        repository_config: RepoConfig,
    ) -> DuckDbResult<Arc<Self>> {
        validate_resource_settings(&options)?;
        let open_options = options.clone();
        tokio::task::spawn_blocking(move || Self::open_blocking(open_options, repository_config))
            .await
            .map_err(|error| DuckDbError::BlockingJob(error.to_string()))?
    }

    fn open_blocking(
        options: DuckDbStoreOptions,
        repository_config: RepoConfig,
    ) -> DuckDbResult<Arc<Self>> {
        let (database_path, temp_directory, owner_lock) = prepare_paths(
            &options.runtime_root,
            &options.database_path,
            &options.temp_directory,
        )?;
        let mut writer = Connection::open(&database_path)?;
        configure_connection(&writer, &options, &temp_directory)?;
        let migrations = migrate(&mut writer)?;
        let engine_version = writer.version()?;
        secure_database_file(&database_path)?;

        let mut readers = Vec::with_capacity(usize::from(options.read_connections));
        for _ in 0..options.read_connections {
            readers.push(writer.try_clone()?);
        }

        Ok(Arc::new(Self {
            writer: Mutex::new(Some(writer)),
            readers: Arc::new(Mutex::new(readers)),
            reader_admission: Arc::new(tokio::sync::Semaphore::new(usize::from(
                options.read_connections,
            ))),
            read_connection_count: u32::from(options.read_connections),
            migrations,
            database_path,
            repository_config,
            engine_version,
            shutting_down: Arc::new(AtomicBool::new(false)),
            owner_lock: Mutex::new(Some(owner_lock)),
        }))
    }

    pub fn applied_migrations(&self) -> &[AppliedMigration] {
        &self.migrations
    }

    pub fn schema_version(&self) -> DuckDbResult<u32> {
        let guard = self
            .writer
            .lock()
            .map_err(|_| DuckDbError::BlockingJob("writer lock poisoned".to_string()))?;
        let connection = guard.as_ref().ok_or(DuckDbError::ShuttingDown)?;
        schema_version(connection)
    }

    pub fn reader_count(&self) -> usize {
        self.readers.lock().map_or(0, |readers| readers.len())
    }

    pub fn is_shutting_down(&self) -> bool {
        self.shutting_down.load(Ordering::Acquire)
    }

    pub async fn close(&self) -> DuckDbResult<()> {
        self.shutting_down.store(true, Ordering::Release);
        let _reader_drain = self
            .reader_admission
            .clone()
            .acquire_many_owned(self.read_connection_count)
            .await
            .map_err(|error| DuckDbError::BlockingJob(error.to_string()))?;
        let writer = self
            .writer
            .lock()
            .map_err(|_| DuckDbError::BlockingJob("writer lock poisoned".to_string()))?
            .take();
        self.readers
            .lock()
            .map_err(|_| DuckDbError::BlockingJob("reader lock poisoned".to_string()))?
            .clear();
        if let Some(writer) = writer {
            writer.execute_batch("CHECKPOINT")?;
            drop(writer);
        }
        secure_database_file(&self.database_path)?;
        self.owner_lock
            .lock()
            .map_err(|_| DuckDbError::BlockingJob("owner lock poisoned".to_string()))?
            .take();
        Ok(())
    }
}

fn validate_resource_settings(options: &DuckDbStoreOptions) -> DuckDbResult<()> {
    if options.memory_limit_mb == 0 {
        return Err(DuckDbError::InvalidPath(
            "memory_limit_mb must be greater than zero".to_string(),
        ));
    }
    if options.threads == 0 {
        return Err(DuckDbError::InvalidPath(
            "threads must be greater than zero".to_string(),
        ));
    }
    if options.read_connections == 0 {
        return Err(DuckDbError::InvalidPath(
            "read_connections must be greater than zero".to_string(),
        ));
    }
    if options.max_temp_directory_size_mb == 0 {
        return Err(DuckDbError::InvalidPath(
            "max_temp_directory_size_mb must be greater than zero".to_string(),
        ));
    }
    Ok(())
}

fn configure_connection(
    connection: &Connection,
    options: &DuckDbStoreOptions,
    temp_directory: &Path,
) -> DuckDbResult<()> {
    connection.execute(
        "SET memory_limit = ?",
        params![format!("{}MB", options.memory_limit_mb)],
    )?;
    connection.execute("SET threads = ?", params![options.threads])?;
    connection.execute(
        "SET temp_directory = ?",
        params![temp_directory.to_string_lossy().as_ref()],
    )?;
    connection.execute(
        "SET max_temp_directory_size = ?",
        params![format!("{}MB", options.max_temp_directory_size_mb)],
    )?;
    // Set the private spill directory before disabling all external access;
    // DuckDB intentionally freezes the setting once the lockdown is active.
    connection.execute_batch(
        "SET autoinstall_known_extensions = false;\n\
         SET autoload_known_extensions = false;\n\
         SET allow_unsigned_extensions = false;\n\
         SET enable_external_access = false;",
    )?;
    Ok(())
}
