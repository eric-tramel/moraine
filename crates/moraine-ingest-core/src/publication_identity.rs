use anyhow::{bail, Context, Result};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use uuid::{Uuid, Variant, Version};

pub(crate) const PUBLICATION_HOST_ID_FILE_NAME: &str = "publication-host-id";
const MAX_IDENTITY_FILE_BYTES: u64 = 128;

struct TemporaryIdentityFile {
    path: PathBuf,
    present: bool,
}

impl TemporaryIdentityFile {
    fn new(path: PathBuf) -> Self {
        Self {
            path,
            present: true,
        }
    }

    fn remove(&mut self) -> Result<()> {
        remove_temporary_identity(&self.path)?;
        self.present = false;
        Ok(())
    }
}

impl Drop for TemporaryIdentityFile {
    fn drop(&mut self) {
        if self.present {
            let _ = fs::remove_file(&self.path);
        }
    }
}

/// Process-wide publication identity derived from one durable installation ID.
///
/// The host ID is the shared-backend checkpoint and physical-row authority.
/// The publisher ID additionally distinguishes process instances so unresolved
/// append ownership can be diagnosed across restarts.
#[derive(Clone, Debug)]
pub(crate) struct PublicationIdentity {
    host_id: Arc<str>,
    publisher_id: Arc<str>,
    created: bool,
}

impl PublicationIdentity {
    pub(crate) fn load_or_create(state_dir: impl AsRef<Path>) -> Result<Self> {
        let state_dir = state_dir.as_ref();
        fs::create_dir_all(state_dir).with_context(|| {
            format!(
                "failed to create ingest state directory {} for publication identity",
                state_dir.display()
            )
        })?;

        let path = state_dir.join(PUBLICATION_HOST_ID_FILE_NAME);
        match open_existing_identity(&path) {
            Ok(mut file) => {
                let host_id = read_identity(&mut file, &path)?;
                Ok(Self::new(host_id, false))
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                create_identity(state_dir, &path)
            }
            Err(error) => Err(error).with_context(|| {
                format!(
                    "failed to open durable publication identity {}; refusing to derive shared-backend authority from the environment",
                    path.display()
                )
            }),
        }
    }

    fn new(host_id: String, created: bool) -> Self {
        let publisher_id = format!("{}:{}:{}", host_id, std::process::id(), Uuid::new_v4());
        Self {
            host_id: host_id.into(),
            publisher_id: publisher_id.into(),
            created,
        }
    }

    pub(crate) fn host_id(&self) -> &str {
        &self.host_id
    }

    pub(crate) fn publisher_id(&self) -> &str {
        &self.publisher_id
    }

    pub(crate) fn was_created(&self) -> bool {
        self.created
    }

    #[cfg(test)]
    pub(crate) fn for_test(host_id: &str) -> Self {
        Self::new(
            validate_host_id(host_id).expect("valid test publication host ID"),
            false,
        )
    }
}

fn create_identity(state_dir: &Path, path: &Path) -> Result<PublicationIdentity> {
    let generated = Uuid::new_v4().hyphenated().to_string();
    let temp_path = temporary_identity_path(state_dir);
    let mut options = OpenOptions::new();
    options.create_new(true).read(true).write(true);
    configure_secure_open(&mut options);
    let mut temp = options.open(&temp_path).with_context(|| {
        format!(
            "failed to create temporary publication identity {}",
            temp_path.display()
        )
    })?;
    let mut temp_guard = TemporaryIdentityFile::new(temp_path.clone());

    let payload = format!("{generated}\n");
    temp.write_all(payload.as_bytes())
        .context("failed to write temporary publication identity")?;
    temp.sync_all()
        .context("failed to durably sync temporary publication identity")?;
    validate_identity_file(&temp, &temp_path)?;
    drop(temp);

    match fs::hard_link(&temp_path, path) {
        Ok(()) => {
            sync_directory(state_dir)?;
            temp_guard.remove()?;
            sync_directory(state_dir)?;

            let mut installed = open_existing_identity(path).with_context(|| {
                format!(
                    "failed to reopen installed publication identity {}",
                    path.display()
                )
            })?;
            let host_id = read_identity(&mut installed, path)?;
            if host_id != generated {
                bail!(
                    "installed publication identity {} changed during atomic creation",
                    path.display()
                );
            }
            Ok(PublicationIdentity::new(host_id, true))
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            temp_guard.remove()?;
            sync_directory(state_dir)?;
            let mut installed = open_existing_identity(path).with_context(|| {
                format!(
                    "failed to open concurrently installed publication identity {}",
                    path.display()
                )
            })?;
            let host_id = read_identity(&mut installed, path)?;
            Ok(PublicationIdentity::new(host_id, false))
        }
        Err(error) => {
            Err(error).with_context(|| {
                format!(
                    "failed to atomically install publication identity {}; the state directory must support same-directory hard links",
                    path.display()
                )
            })
        }
    }
}

fn temporary_identity_path(state_dir: &Path) -> PathBuf {
    state_dir.join(format!(
        ".{PUBLICATION_HOST_ID_FILE_NAME}.{}.{}.tmp",
        std::process::id(),
        Uuid::new_v4()
    ))
}

fn open_existing_identity(path: &Path) -> std::io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW);
    }
    options.open(path)
}

fn configure_secure_open(options: &mut OpenOptions) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options
            .mode(0o600)
            .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW);
    }
}

fn validate_identity_file(file: &File, path: &Path) -> Result<()> {
    let metadata = file
        .metadata()
        .with_context(|| format!("failed to inspect publication identity {}", path.display()))?;
    if !metadata.is_file() {
        bail!(
            "publication identity {} is not a regular file",
            path.display()
        );
    }
    if metadata.len() > MAX_IDENTITY_FILE_BYTES {
        bail!(
            "publication identity {} is unexpectedly large ({} bytes)",
            path.display(),
            metadata.len()
        );
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let mode = metadata.permissions().mode() & 0o777;
        if mode & 0o077 != 0 {
            bail!(
                "publication identity {} has insecure permissions {:o}; expected 600",
                path.display(),
                mode
            );
        }
        // SAFETY: geteuid has no arguments and no failure mode.
        let effective_uid = unsafe { libc::geteuid() };
        if metadata.uid() != effective_uid {
            bail!(
                "publication identity {} is owned by uid {}, not current uid {}",
                path.display(),
                metadata.uid(),
                effective_uid
            );
        }
    }

    Ok(())
}

fn read_identity(file: &mut File, path: &Path) -> Result<String> {
    validate_identity_file(file, path)?;
    let mut payload = String::new();
    file.read_to_string(&mut payload)
        .with_context(|| format!("failed to read publication identity {}", path.display()))?;
    let Some(host_id) = payload.strip_suffix('\n') else {
        bail!(
            "publication identity {} has invalid framing; expected one canonical UUID line",
            path.display()
        );
    };
    if host_id.is_empty() || host_id.contains(char::is_whitespace) {
        bail!(
            "publication identity {} is empty or contains whitespace",
            path.display()
        );
    }
    validate_host_id(host_id).with_context(|| {
        format!(
            "publication identity {} is corrupt; refusing to adopt a legacy HOSTNAME/USER key",
            path.display()
        )
    })
}

fn validate_host_id(host_id: &str) -> Result<String> {
    let parsed = Uuid::parse_str(host_id).context("publication host ID is not a UUID")?;
    if parsed.get_variant() != Variant::RFC4122 || parsed.get_version() != Some(Version::Random) {
        bail!("publication host ID must be an RFC 4122 version-4 UUID");
    }
    let canonical = parsed.hyphenated().to_string();
    if canonical != host_id {
        bail!("publication host ID must use canonical lowercase hyphenated form");
    }
    Ok(canonical)
}

fn remove_temporary_identity(path: &Path) -> Result<()> {
    fs::remove_file(path).with_context(|| {
        format!(
            "failed to remove temporary publication identity {}",
            path.display()
        )
    })
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)
        .with_context(|| format!("failed to open state directory {} for sync", path.display()))?
        .sync_all()
        .with_context(|| format!("failed to durably sync state directory {}", path.display()))
}

#[cfg(not(unix))]
fn sync_directory(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;
    use std::thread;

    const SPOOFED_HOSTNAME: &str = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
    const SPOOFED_USER: &str = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb";
    const ENV_PROBE: &str = "MORAINE_PUBLICATION_IDENTITY_ENV_PROBE";

    struct TestStateDir(PathBuf);

    impl TestStateDir {
        fn new(label: &str) -> Self {
            Self(std::env::temp_dir().join(format!(
                "moraine-publication-identity-{label}-{}",
                Uuid::new_v4()
            )))
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TestStateDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn derives_canonical_random_identity_with_restrictive_permissions() {
        let state = TestStateDir::new("derive");
        let identity = PublicationIdentity::load_or_create(state.path()).expect("create identity");

        assert!(identity.was_created());
        assert_eq!(
            validate_host_id(identity.host_id()).unwrap(),
            identity.host_id()
        );
        assert!(identity.publisher_id().starts_with(identity.host_id()));
        let path = state.path().join(PUBLICATION_HOST_ID_FILE_NAME);
        assert_eq!(
            fs::read_to_string(&path).unwrap(),
            format!("{}\n", identity.host_id())
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
    }

    #[test]
    fn reload_reuses_the_same_host_id_with_a_new_publisher_instance() {
        let state = TestStateDir::new("restart");
        let first = PublicationIdentity::load_or_create(state.path()).expect("first identity");
        let second = PublicationIdentity::load_or_create(state.path()).expect("second identity");

        assert!(first.was_created());
        assert!(!second.was_created());
        assert_eq!(first.host_id(), second.host_id());
        assert_ne!(first.publisher_id(), second.publisher_id());
    }

    #[test]
    fn atomic_create_or_read_converges_across_threads() {
        let state = TestStateDir::new("concurrent");
        let path = Arc::new(state.path().to_path_buf());
        let barrier = Arc::new(std::sync::Barrier::new(8));
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let path = Arc::clone(&path);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    let identity = PublicationIdentity::load_or_create(path.as_path())
                        .expect("concurrent identity");
                    (identity.host_id().to_string(), identity.was_created())
                })
            })
            .collect();

        let identities: Vec<_> = handles
            .into_iter()
            .map(|handle| handle.join().expect("identity thread"))
            .collect();
        assert!(identities
            .iter()
            .all(|(host_id, _)| host_id == &identities[0].0));
        assert_eq!(
            identities.iter().filter(|(_, created)| *created).count(),
            1,
            "exactly one concurrent creator installs the durable identity"
        );
    }

    #[test]
    fn distinct_state_directories_receive_distinct_host_ids() {
        let first_state = TestStateDir::new("first");
        let second_state = TestStateDir::new("second");

        let first = PublicationIdentity::load_or_create(first_state.path()).unwrap();
        let second = PublicationIdentity::load_or_create(second_state.path()).unwrap();

        assert_ne!(first.host_id(), second.host_id());
    }

    #[test]
    fn corrupt_identity_file_fails_closed() {
        let state = TestStateDir::new("corrupt");
        fs::create_dir_all(state.path()).unwrap();
        let path = state.path().join(PUBLICATION_HOST_ID_FILE_NAME);
        fs::write(&path, b"not-a-publication-uuid\n").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        }

        let error = PublicationIdentity::load_or_create(state.path()).unwrap_err();
        assert!(error.to_string().contains("corrupt"));
    }

    #[cfg(unix)]
    #[test]
    fn writable_by_other_users_identity_file_fails_closed() {
        use std::os::unix::fs::PermissionsExt;

        let state = TestStateDir::new("permissions");
        let identity = PublicationIdentity::load_or_create(state.path()).unwrap();
        let path = state.path().join(PUBLICATION_HOST_ID_FILE_NAME);
        fs::set_permissions(&path, fs::Permissions::from_mode(0o666)).unwrap();

        let error = PublicationIdentity::load_or_create(state.path()).unwrap_err();
        assert!(error.to_string().contains("insecure permissions"));
        assert_eq!(
            fs::read_to_string(path).unwrap(),
            format!("{}\n", identity.host_id())
        );
    }

    #[test]
    fn publication_identity_ignores_environment_spoofing() {
        if std::env::var_os(ENV_PROBE).is_some() {
            let state = TestStateDir::new("env-child");
            let identity = PublicationIdentity::load_or_create(state.path()).unwrap();
            assert_ne!(identity.host_id(), SPOOFED_HOSTNAME);
            assert_ne!(identity.host_id(), SPOOFED_USER);
            return;
        }

        let output = Command::new(std::env::current_exe().expect("current test executable"))
            .args([
                "--exact",
                "publication_identity::tests::publication_identity_ignores_environment_spoofing",
                "--nocapture",
            ])
            .env(ENV_PROBE, "1")
            .env("HOSTNAME", SPOOFED_HOSTNAME)
            .env("USER", SPOOFED_USER)
            .output()
            .expect("run isolated environment-spoof probe");
        assert!(
            output.status.success(),
            "environment-spoof probe failed:\n{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
