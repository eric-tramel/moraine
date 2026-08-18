use std::fs::{self, File, OpenOptions};
use std::path::{Component, Path, PathBuf};

use fs2::FileExt;

use crate::{DuckDbError, DuckDbResult};

pub(crate) struct OwnerLock {
    _file: File,
}

fn io_error(path: &Path, source: std::io::Error) -> DuckDbError {
    DuckDbError::Io {
        path: path.to_path_buf(),
        source,
    }
}

fn validate_lexical_path(
    configured_root: &Path,
    canonical_root: &Path,
    path: &Path,
    label: &str,
) -> DuckDbResult<PathBuf> {
    if path.as_os_str().is_empty() || path == Path::new(":memory:") {
        return Err(DuckDbError::InvalidPath(format!(
            "{label} must be a non-empty file path"
        )));
    }
    if path
        .components()
        .any(|component| matches!(component, Component::ParentDir))
    {
        return Err(DuckDbError::InvalidPath(format!(
            "{label} must not contain `..`"
        )));
    }
    let resolved = if path.is_absolute() {
        let relative = path.strip_prefix(configured_root).map_err(|_| {
            DuckDbError::InvalidPath(format!(
                "{label} must remain beneath the configured runtime root"
            ))
        })?;
        canonical_root.join(relative)
    } else {
        canonical_root.join(path)
    };
    if !resolved.starts_with(canonical_root) {
        return Err(DuckDbError::InvalidPath(format!(
            "{label} must remain beneath the configured runtime root"
        )));
    }
    Ok(resolved)
}

fn reject_existing_symlinks(root: &Path, path: &Path) -> DuckDbResult<()> {
    let relative = path.strip_prefix(root).map_err(|_| {
        DuckDbError::InvalidPath("storage path escaped the runtime root".to_string())
    })?;
    let mut current = root.to_path_buf();
    for component in relative.components() {
        current.push(component.as_os_str());
        match fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(DuckDbError::InvalidPath(format!(
                    "storage path component `{}` is a symlink",
                    component.as_os_str().to_string_lossy()
                )));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
            Err(error) => return Err(io_error(&current, error)),
        }
    }
    Ok(())
}

#[cfg(unix)]
fn set_mode(path: &Path, mode: u32) -> DuckDbResult<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(mode)).map_err(|e| io_error(path, e))
}

#[cfg(not(unix))]
fn set_mode(_path: &Path, _mode: u32) -> DuckDbResult<()> {
    Ok(())
}

pub(crate) fn prepare_paths(
    root: &Path,
    database: &Path,
    temp: &Path,
) -> DuckDbResult<(PathBuf, PathBuf, OwnerLock)> {
    if !root.is_absolute() {
        return Err(DuckDbError::InvalidPath(
            "runtime root must be absolute".to_string(),
        ));
    }
    if fs::symlink_metadata(root)
        .map(|metadata| metadata.file_type().is_symlink())
        .unwrap_or(false)
    {
        return Err(DuckDbError::InvalidPath(
            "runtime root must not be a symlink".to_string(),
        ));
    }
    fs::create_dir_all(root).map_err(|e| io_error(root, e))?;
    let configured_root = root.to_path_buf();
    let root = root.canonicalize().map_err(|e| io_error(root, e))?;
    let database = validate_lexical_path(&configured_root, &root, database, "database path")?;
    let temp = validate_lexical_path(&configured_root, &root, temp, "temp directory")?;
    reject_existing_symlinks(&root, &database)?;
    reject_existing_symlinks(&root, &temp)?;

    let database_parent = database.parent().ok_or_else(|| {
        DuckDbError::InvalidPath("database path has no parent directory".to_string())
    })?;
    fs::create_dir_all(database_parent).map_err(|e| io_error(database_parent, e))?;
    fs::create_dir_all(&temp).map_err(|e| io_error(&temp, e))?;
    set_mode(database_parent, 0o700)?;
    set_mode(&temp, 0o700)?;

    let lock_path = database.with_extension("owner.lock");
    let lock_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock_path)
        .map_err(|e| io_error(&lock_path, e))?;
    set_mode(&lock_path, 0o600)?;
    lock_file
        .try_lock_exclusive()
        .map_err(|error| match error.kind() {
            std::io::ErrorKind::WouldBlock => DuckDbError::AlreadyOwned,
            _ => io_error(&lock_path, error),
        })?;
    Ok((database, temp, OwnerLock { _file: lock_file }))
}

pub(crate) fn secure_database_file(path: &Path) -> DuckDbResult<()> {
    if path.exists() {
        set_mode(path, 0o600)?;
    }
    let wal = PathBuf::from(format!("{}.wal", path.display()));
    if wal.exists() {
        set_mode(&wal, 0o600)?;
    }
    Ok(())
}
