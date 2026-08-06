use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsStr;
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use super::harnesses::{ManagedFileWrite, McpConfigWrite};
use super::{read_json_object_or_default, write_json_atomic, McpConfigFormat};

const SKILL_MD: &str =
    include_str!("../../../../../plugins/prime-agent-moraine/skills/moraine/SKILL.md");
const PYPROJECT: &str =
    include_str!("../../../../../plugins/prime-agent-moraine/skills/moraine/pyproject.toml");
const INIT_PY: &str = include_str!(
    "../../../../../plugins/prime-agent-moraine/skills/moraine/src/moraine/__init__.py"
);
const MARKER_NAME: &str = ".moraine-setup.json";
const MANAGED_FILES: [(&str, &str); 3] = [
    ("SKILL.md", SKILL_MD),
    ("pyproject.toml", PYPROJECT),
    ("src/moraine/__init__.py", INIT_PY),
];
const LEGACY_HASHES: [(&str, &str); 3] = [
    (
        "SKILL.md",
        "857f2959f256d567d669c4eb540384dbee9e9a941281ee81d25f271bb351fe6d",
    ),
    (
        "pyproject.toml",
        "83722c101e80c7d09dcaf50c2b295b5e696f23f3e07c054e8fc76e03150ccea1",
    ),
    (
        "src/moraine/__init__.py",
        "2c5005ad35e4b263fe31d3fb3f70ba4186ceb285b122b5efe869e525769f2b4f",
    ),
];

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SkillManifest {
    owner: String,
    schema: u32,
    version: String,
    files: BTreeMap<String, String>,
}

pub(super) fn resolve_agent_dir(
    configured: Option<&OsStr>,
    home: Option<&Path>,
    launch_cwd: &Path,
) -> Result<Option<PathBuf>> {
    let Some(configured) = configured else {
        return Ok(home.map(|home| home.join(".prime").join("agent")));
    };
    if configured.is_empty() {
        return Ok(home.map(|home| home.join(".prime").join("agent")));
    }
    let path = PathBuf::from(configured);
    let expanded = if path == Path::new("~") {
        home.map(Path::to_path_buf)
            .context("PRIME_AGENT_CODING_AGENT_DIR uses `~` but HOME is not set")?
    } else if let Ok(rest) = path.strip_prefix("~") {
        home.context("PRIME_AGENT_CODING_AGENT_DIR uses `~` but HOME is not set")?
            .join(rest)
    } else {
        path
    };
    if !expanded.is_absolute() {
        bail!(
            "PRIME_AGENT_CODING_AGENT_DIR must be absolute for global setup (got {} from {})",
            expanded.display(),
            launch_cwd.display()
        );
    }
    Ok(Some(expanded))
}

pub(super) fn resolve_mcp_executable(current_exe: &Path) -> Result<PathBuf> {
    let current = fs::canonicalize(current_exe).with_context(|| {
        format!(
            "failed to resolve running Moraine executable {}",
            current_exe.display()
        )
    })?;
    let parent = current
        .parent()
        .context("running Moraine executable has no parent directory")?;
    let candidate = parent.join(format!("moraine-mcp{}", std::env::consts::EXE_SUFFIX));
    let resolved = fs::canonicalize(&candidate).with_context(|| {
        format!(
            "Prime Agent setup requires the installed sibling binary {}",
            candidate.display()
        )
    })?;
    if resolved.parent() != Some(parent) {
        bail!(
            "refusing Moraine MCP sibling that resolves outside the installed binary directory: {}",
            resolved.display()
        );
    }
    let metadata = fs::metadata(&resolved)
        .with_context(|| format!("failed to inspect {}", resolved.display()))?;
    if !metadata.is_file() {
        bail!(
            "Moraine MCP sibling is not a regular file: {}",
            resolved.display()
        );
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o111 == 0 {
            bail!(
                "Moraine MCP sibling is not executable: {}",
                resolved.display()
            );
        }
    }
    Ok(resolved)
}

pub(super) fn skill_writes(agent_dir: &Path) -> Result<Vec<ManagedFileWrite>> {
    let skill_dir = agent_dir.join("skills").join("moraine");
    preflight_skill_dir(&skill_dir)?;
    let marker = render_manifest()?;
    let mut writes = MANAGED_FILES
        .iter()
        .map(|(relative, content)| {
            ManagedFileWrite::new(
                skill_dir.join(relative),
                "Prime Agent Moraine skill",
                (*content).to_string(),
            )
        })
        .collect::<Vec<_>>();
    writes.push(ManagedFileWrite::new(
        skill_dir.join(MARKER_NAME),
        "Prime Agent Moraine skill manifest",
        marker,
    ));
    Ok(writes)
}

fn render_manifest() -> Result<String> {
    let files = MANAGED_FILES
        .iter()
        .map(|(path, content)| ((*path).to_string(), sha256(content.as_bytes())))
        .collect();
    let manifest = SkillManifest {
        owner: "moraine-setup".to_string(),
        schema: 1,
        version: env!("CARGO_PKG_VERSION").to_string(),
        files,
    };
    let mut rendered = serde_json::to_string_pretty(&manifest)?;
    rendered.push('\n');
    Ok(rendered)
}

fn preflight_skill_dir(skill_dir: &Path) -> Result<()> {
    match fs::symlink_metadata(skill_dir) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            bail!(
                "refusing symlinked Prime Agent Moraine skill at {}",
                skill_dir.display()
            )
        }
        Ok(metadata) if !metadata.is_dir() => {
            bail!(
                "Prime Agent Moraine skill path is not a directory: {}",
                skill_dir.display()
            )
        }
        Err(exc) if exc.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(exc) => {
            return Err(exc).with_context(|| format!("failed to inspect {}", skill_dir.display()))
        }
        Ok(_) => {}
    }

    let paths = collect_skill_paths(skill_dir)?;
    let expected = MANAGED_FILES
        .iter()
        .map(|(path, _)| (*path).to_string())
        .collect::<BTreeSet<_>>();
    let marker_path = skill_dir.join(MARKER_NAME);
    if !marker_path.exists() {
        if paths != expected {
            bail!(
                "unowned Prime Agent Moraine skill already exists at {}; remove or move it before running setup",
                skill_dir.display()
            );
        }
        let matches_current = MANAGED_FILES.iter().all(|(relative, content)| {
            fs::read(skill_dir.join(relative)).is_ok_and(|bytes| bytes == content.as_bytes())
        });
        let matches_legacy = LEGACY_HASHES.iter().all(|(relative, expected)| {
            fs::read(skill_dir.join(relative)).is_ok_and(|bytes| sha256(&bytes) == *expected)
        });
        if !matches_current && !matches_legacy {
            bail!(
                "unowned Prime Agent Moraine skill already exists at {}; remove or move it before running setup",
                skill_dir.display()
            );
        }
        return Ok(());
    }

    let marker_bytes = fs::read(&marker_path)
        .with_context(|| format!("failed to read {}", marker_path.display()))?;
    let marker: SkillManifest = serde_json::from_slice(&marker_bytes).with_context(|| {
        format!(
            "invalid Moraine skill ownership manifest {}",
            marker_path.display()
        )
    })?;
    if marker.owner != "moraine-setup" || marker.schema != 1 {
        bail!(
            "unrecognized Moraine skill ownership manifest at {}",
            marker_path.display()
        );
    }
    let marker_paths = marker.files.keys().cloned().collect::<BTreeSet<_>>();
    if marker_paths != expected {
        bail!(
            "Moraine skill ownership manifest has an unexpected file set at {}",
            marker_path.display()
        );
    }
    let mut allowed = expected.clone();
    allowed.insert(MARKER_NAME.to_string());
    if paths != allowed {
        bail!(
            "managed Prime Agent Moraine skill contains unexpected files at {}",
            skill_dir.display()
        );
    }
    for (relative, new_content) in MANAGED_FILES {
        validate_relative(relative)?;
        let bytes = fs::read(skill_dir.join(relative))?;
        let actual = sha256(&bytes);
        let old = marker.files.get(relative).expect("file set was checked");
        let new = sha256(new_content.as_bytes());
        if actual != *old && actual != new {
            bail!(
                "managed Prime Agent Moraine skill was modified: {}",
                skill_dir.join(relative).display()
            );
        }
    }
    Ok(())
}

fn collect_skill_paths(root: &Path) -> Result<BTreeSet<String>> {
    fn visit(root: &Path, dir: &Path, out: &mut BTreeSet<String>) -> Result<()> {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            let metadata = fs::symlink_metadata(&path)?;
            if metadata.file_type().is_symlink() {
                bail!(
                    "refusing symlink inside managed Prime Agent skill: {}",
                    path.display()
                );
            }
            let relative = path.strip_prefix(root).expect("entry is below root");
            if metadata.is_dir() {
                if entry.file_name() == "__pycache__" {
                    validate_pycache(&path)?;
                    continue;
                }
                visit(root, &path, out)?;
            } else if metadata.is_file() {
                out.insert(relative.to_string_lossy().replace('\\', "/"));
            } else {
                bail!(
                    "refusing non-regular entry inside managed Prime Agent skill: {}",
                    path.display()
                );
            }
        }
        Ok(())
    }
    let mut paths = BTreeSet::new();
    visit(root, root, &mut paths)?;
    Ok(paths)
}

fn validate_pycache(dir: &Path) -> Result<()> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path)?;
        if !metadata.is_file() || path.extension() != Some(OsStr::new("pyc")) {
            bail!(
                "unexpected generated entry in Prime Agent skill cache: {}",
                path.display()
            );
        }
    }
    Ok(())
}

fn validate_relative(value: &str) -> Result<()> {
    let path = Path::new(value);
    if path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        bail!("invalid managed Prime Agent skill path: {value}");
    }
    Ok(())
}

fn sha256(content: &[u8]) -> String {
    format!("{:x}", Sha256::digest(content))
}

fn contains_pycache(root: &Path) -> Result<bool> {
    if !root.exists() {
        return Ok(false);
    }
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path)?;
        if metadata.is_dir() {
            if entry.file_name() == "__pycache__" || contains_pycache(&path)? {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

pub(super) fn publish_skill(writes: &[ManagedFileWrite]) -> Result<Vec<bool>> {
    let first = writes.first().context("Prime Agent skill plan is empty")?;
    let skill_dir = first
        .path()
        .parent()
        .context("Prime Agent skill plan has no destination directory")?
        .to_path_buf();
    preflight_skill_dir(&skill_dir)?;

    let generated_cache = contains_pycache(&skill_dir)?;
    let changed = writes
        .iter()
        .map(|write| {
            generated_cache
                || fs::read(write.path()).map_or(true, |bytes| bytes != write.content().as_bytes())
        })
        .collect::<Vec<_>>();
    if !changed.iter().any(|value| *value) {
        return Ok(changed);
    }

    let skills_dir = skill_dir
        .parent()
        .context("Prime Agent skill destination has no parent")?;
    fs::create_dir_all(skills_dir)
        .with_context(|| format!("failed to create {}", skills_dir.display()))?;
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let staging = skills_dir.join(format!(".moraine.stage.{}.{nonce}", std::process::id()));
    let backup = skills_dir.join(format!(".moraine.backup.{}.{nonce}", std::process::id()));
    fs::create_dir(&staging).with_context(|| format!("failed to create {}", staging.display()))?;

    let staged = (|| -> Result<()> {
        for write in writes {
            let relative = write.path().strip_prefix(&skill_dir).with_context(|| {
                format!("Prime Agent skill write escaped {}", skill_dir.display())
            })?;
            let relative_text = relative
                .to_str()
                .context("Prime Agent skill path is not valid UTF-8")?;
            validate_relative(relative_text)?;
            let destination = staging.join(relative);
            if let Some(parent) = destination.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::write(&destination, write.content())?;
        }
        Ok(())
    })();
    if let Err(error) = staged {
        let _ = fs::remove_dir_all(&staging);
        return Err(error).context("failed to stage Prime Agent Moraine skill");
    }

    let had_existing = skill_dir.exists();
    if had_existing {
        fs::rename(&skill_dir, &backup).with_context(|| {
            format!(
                "failed to preserve existing Prime Agent skill {}",
                skill_dir.display()
            )
        })?;
    }
    if let Err(error) = fs::rename(&staging, &skill_dir) {
        if had_existing {
            let _ = fs::rename(&backup, &skill_dir);
        }
        let _ = fs::remove_dir_all(&staging);
        return Err(error).with_context(|| {
            format!(
                "failed to publish Prime Agent skill {}",
                skill_dir.display()
            )
        });
    }
    if had_existing {
        fs::remove_dir_all(&backup).with_context(|| {
            format!(
                "failed to remove Prime Agent skill backup {}",
                backup.display()
            )
        })?;
    }
    Ok(changed)
}

const SETTINGS_LOCK_STALE: Duration = Duration::from_secs(10);

fn inspect_settings_path(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => bail!(
            "refusing symlinked Prime Agent settings file at {}",
            path.display()
        ),
        Ok(metadata) if !metadata.is_file() => bail!(
            "Prime Agent settings path is not a regular file: {}",
            path.display()
        ),
        Ok(_) => {
            let bytes =
                fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
            if bytes.iter().all(u8::is_ascii_whitespace) {
                bail!(
                    "existing Prime Agent settings file is empty: {}",
                    path.display()
                );
            }
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| format!("failed to inspect {}", path.display())),
    }
}

fn acquire_settings_lock(lock_path: &Path, stale_after: Duration) -> Result<()> {
    for _ in 0..10 {
        match fs::create_dir(lock_path) {
            Ok(()) => return Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                let stale = fs::metadata(lock_path)
                    .and_then(|metadata| metadata.modified())
                    .ok()
                    .and_then(|modified| SystemTime::now().duration_since(modified).ok())
                    .is_some_and(|age| age >= stale_after);
                if stale {
                    match fs::remove_dir(lock_path) {
                        Ok(()) => continue,
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                        Err(error) if error.kind() == std::io::ErrorKind::DirectoryNotEmpty => {}
                        Err(error) => {
                            return Err(error).with_context(|| {
                                format!("failed to reclaim stale lock {}", lock_path.display())
                            })
                        }
                    }
                }
                thread::sleep(Duration::from_millis(20));
            }
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("failed to create lock {}", lock_path.display()))
            }
        }
    }
    bail!("Prime Agent settings are locked by another process")
}

fn shell_quote(value: &OsStr) -> String {
    let value = value.to_string_lossy();
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

pub(super) fn activation_guidance(agent_dir: &Path, python: &OsStr) -> String {
    let python = shell_quote(python);
    let skill = shell_quote(agent_dir.join("skills").join("moraine").as_os_str());
    format!(
        "PRIME_AGENT_KERNEL_PYTHON disables automatic Python-skill installation. Activate Moraine with: uv pip install --python {python} -e {skill} && {python} -c 'import mcp, moraine'"
    )
}

pub(super) fn preflight_settings(write: &McpConfigWrite) -> Result<()> {
    let path = write.path();
    inspect_settings_path(path)?;
    let mut root = read_json_object_or_default(path, McpConfigFormat::Json)?;
    write.merge_into(&mut root)?;
    Ok(())
}

pub(super) fn apply_settings(write: &McpConfigWrite) -> Result<bool> {
    let path = write.path();
    inspect_settings_path(path)?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let mut lock_name = path.as_os_str().to_os_string();
    lock_name.push(".lock");
    let lock_path = PathBuf::from(lock_name);
    acquire_settings_lock(&lock_path, SETTINGS_LOCK_STALE)
        .with_context(|| format!("failed to lock {}", path.display()))?;
    let result = (|| {
        inspect_settings_path(path)?;
        let mut root = read_json_object_or_default(path, McpConfigFormat::Json)?;
        let before = Value::Object(root.clone());
        write.merge_into(&mut root)?;
        let after = Value::Object(root);
        if before == after {
            return Ok(false);
        }
        write_json_atomic(path, &after)?;
        Ok(true)
    })();
    let unlock =
        fs::remove_dir(&lock_path).with_context(|| format!("failed to unlock {}", path.display()));
    match (result, unlock) {
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error),
        (Ok(changed), Ok(())) => Ok(changed),
    }
}

pub(super) fn is_known_moraine_server(value: &Value) -> bool {
    let Some(object) = value.as_object() else {
        return false;
    };
    if !object
        .keys()
        .all(|key| matches!(key.as_str(), "type" | "command" | "args" | "enabled"))
        || object.get("type").and_then(Value::as_str) != Some("stdio")
        || object
            .get("enabled")
            .is_some_and(|enabled| enabled != &Value::Bool(true))
    {
        return false;
    }
    let Some(command) = object.get("command").and_then(Value::as_str) else {
        return false;
    };
    let Some(args) = object.get("args").and_then(Value::as_array) else {
        return false;
    };
    if !args.iter().all(Value::is_string) {
        return false;
    }
    let args = args
        .iter()
        .map(|arg| arg.as_str().expect("argument types were checked"))
        .collect::<Vec<_>>();
    let command_name = Path::new(command).file_name().and_then(OsStr::to_str);
    let executable_suffix = std::env::consts::EXE_SUFFIX;
    let is_moraine = matches!(command_name, Some("moraine"))
        || command_name.is_some_and(|name| name == format!("moraine{executable_suffix}"));
    let is_mcp = matches!(command_name, Some("moraine-mcp"))
        || command_name.is_some_and(|name| name == format!("moraine-mcp{executable_suffix}"));

    (is_moraine && args == ["run", "mcp"])
        || (is_mcp
            && args.len() == 4
            && args[0] == "--config"
            && Path::new(args[1]).is_absolute()
            && args[2..] == ["--serve", "stdio"])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::setup::harnesses::{mcp_plan, McpConfigWrite, SetupPathContext};
    use crate::commands::setup::{
        apply_managed_file_write, ConfigTarget, ConfigTargetSource, SetupMcpTarget,
    };
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir(label: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "moraine-prime-setup-{label}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        fs::create_dir_all(&path).expect("create temp dir");
        path
    }

    #[test]
    fn agent_dir_defaults_expands_tilde_and_rejects_relative_override() {
        let home = Path::new("/home/test");
        assert_eq!(
            resolve_agent_dir(None, Some(home), Path::new("/work")).expect("default"),
            Some(PathBuf::from("/home/test/.prime/agent"))
        );
        assert_eq!(
            resolve_agent_dir(Some(OsStr::new("~/custom")), Some(home), Path::new("/work"))
                .expect("tilde"),
            Some(PathBuf::from("/home/test/custom"))
        );
        assert!(resolve_agent_dir(
            Some(OsStr::new("relative/agent")),
            Some(home),
            Path::new("/work")
        )
        .expect_err("relative override")
        .to_string()
        .contains("must be absolute"));
    }

    #[test]
    fn skill_install_is_idempotent_and_refuses_modified_content() {
        let agent = temp_dir("skill");
        let writes = skill_writes(&agent).expect("fresh plan");
        for write in &writes {
            apply_managed_file_write(write).expect("write skill");
        }
        assert_eq!(skill_writes(&agent).expect("repeat").len(), 4);
        let cache = agent.join("skills/moraine/src/moraine/__pycache__");
        fs::create_dir(&cache).expect("cache directory");
        fs::write(
            cache.join("__init__.cpython-313.pyc"),
            b"untrusted bytecode",
        )
        .expect("cache file");
        let writes = skill_writes(&agent).expect("cache is recoverable");
        assert!(publish_skill(&writes)
            .expect("clean cached bytecode")
            .into_iter()
            .all(|changed| changed));
        assert!(!cache.exists());
        fs::write(agent.join("skills/moraine/SKILL.md"), "modified\n")
            .expect("modify managed skill");
        assert!(skill_writes(&agent)
            .expect_err("modified skill")
            .to_string()
            .contains("was modified"));
        let _ = fs::remove_dir_all(agent);
    }

    #[test]
    fn settings_merge_preserves_unrelated_values_and_honors_lock() {
        let root = temp_dir("settings");
        let agent = root.join("agent");
        fs::create_dir_all(&agent).expect("agent dir");
        fs::write(
            agent.join("settings.json"),
            r#"{"theme":"dark","mcpServers":{"other":{"command":"node"}}}"#,
        )
        .expect("settings");
        let config = ConfigTarget {
            path: root.join("moraine.toml"),
            source: ConfigTargetSource::Cli,
        };
        let write =
            McpConfigWrite::prime_agent(&agent, &config, Path::new("/opt/moraine/bin/moraine-mcp"));
        assert!(apply_settings(&write).expect("first merge"));
        assert!(!apply_settings(&write).expect("repeat merge"));
        let value: Value =
            serde_json::from_slice(&fs::read(agent.join("settings.json")).unwrap()).expect("JSON");
        assert_eq!(value["theme"], "dark");
        assert_eq!(value["mcpServers"]["other"]["command"], "node");
        assert_eq!(
            value["mcpServers"]["moraine"]["command"],
            "/opt/moraine/bin/moraine-mcp"
        );
        fs::create_dir(agent.join("settings.json.lock")).expect("lock settings");
        let error = apply_settings(&write).expect_err("locked");
        assert!(format!("{error:#}").contains("locked by another process"));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn existing_empty_settings_are_rejected() {
        let root = temp_dir("empty-settings");
        let agent = root.join("agent");
        fs::create_dir_all(&agent).expect("agent dir");
        fs::write(agent.join("settings.json"), "  \n").expect("empty settings");
        let config = ConfigTarget {
            path: root.join("moraine.toml"),
            source: ConfigTargetSource::Cli,
        };
        let write =
            McpConfigWrite::prime_agent(&agent, &config, Path::new("/opt/moraine/bin/moraine-mcp"));
        assert!(preflight_settings(&write)
            .expect_err("empty settings")
            .to_string()
            .contains("is empty"));
        assert!(!agent.join("skills/moraine").exists());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn stale_settings_lock_is_reclaimed_but_live_lock_is_not() {
        let root = temp_dir("stale-lock");
        let lock = root.join("settings.json.lock");
        fs::create_dir(&lock).expect("live lock");
        assert!(acquire_settings_lock(&lock, Duration::from_secs(60)).is_err());
        thread::sleep(Duration::from_millis(30));
        acquire_settings_lock(&lock, Duration::from_millis(20)).expect("reclaim stale lock");
        assert!(lock.is_dir());
        fs::remove_dir(&lock).expect("release test lock");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn activation_guidance_quotes_the_configured_interpreter() {
        let guidance = activation_guidance(
            Path::new("/tmp/agent root"),
            OsStr::new("/tmp/python's bin/python"),
        );
        assert!(guidance.contains("--python '/tmp/python'\"'\"'s bin/python'"));
        assert!(guidance.contains("-e '/tmp/agent root/skills/moraine'"));
        assert!(guidance.contains("&& '/tmp/python'\"'\"'s bin/python' -c"));
        assert!(!guidance.contains("<configured-python>"));
    }

    #[test]
    fn settings_merge_refuses_custom_moraine_registration() {
        let root = temp_dir("custom-settings");
        let agent = root.join("agent");
        fs::create_dir_all(&agent).expect("agent dir");
        fs::write(
            agent.join("settings.json"),
            r#"{"mcpServers":{"moraine":{"type":"http","url":"https://example.test"}}}"#,
        )
        .expect("settings");
        let config = ConfigTarget {
            path: root.join("moraine.toml"),
            source: ConfigTargetSource::Cli,
        };
        let write =
            McpConfigWrite::prime_agent(&agent, &config, Path::new("/opt/moraine/bin/moraine-mcp"));
        assert!(apply_settings(&write)
            .expect_err("custom entry")
            .to_string()
            .contains("customized"));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn only_exact_known_server_shapes_are_adoptable() {
        let canonical = serde_json::json!({
            "type": "stdio",
            "command": "/opt/moraine/bin/moraine-mcp",
            "args": ["--config", "/tmp/moraine.toml", "--serve", "stdio"],
            "enabled": true
        });
        assert!(is_known_moraine_server(&canonical));
        let malformed = serde_json::json!({
            "type": "stdio",
            "command": "/opt/moraine/bin/moraine-mcp",
            "args": [42, "--serve", "stdio"]
        });
        assert!(!is_known_moraine_server(&malformed));
        let customized_env = serde_json::json!({
            "type": "stdio",
            "command": "/opt/moraine/bin/moraine-mcp",
            "args": ["--config", "/tmp/moraine.toml", "--serve", "stdio"],
            "env": {"CLICKHOUSE_PASSWORD": "required"}
        });
        assert!(!is_known_moraine_server(&customized_env));
        let extra_arg = serde_json::json!({
            "type": "stdio",
            "command": "moraine",
            "args": ["--verbose", "run", "mcp"]
        });
        assert!(!is_known_moraine_server(&extra_arg));
    }

    #[cfg(unix)]
    #[test]
    fn plan_preflights_settings_conflict_before_skill_publication() {
        use std::cell::OnceCell;
        use std::os::unix::fs::PermissionsExt;

        let root = temp_dir("plan-preflight");
        let agent = root.join("agent");
        let bin = root.join("bin");
        fs::create_dir_all(&agent).expect("agent");
        fs::create_dir_all(&bin).expect("bin");
        fs::write(
            agent.join("settings.json"),
            r#"{"mcpServers":{"moraine":{"type":"http","url":"https://example.test"}}}"#,
        )
        .expect("settings");
        let moraine = bin.join("moraine");
        let mcp = bin.join("moraine-mcp");
        fs::write(&moraine, "binary").expect("moraine");
        fs::write(&mcp, "binary").expect("mcp");
        fs::set_permissions(&moraine, fs::Permissions::from_mode(0o755)).unwrap();
        fs::set_permissions(&mcp, fs::Permissions::from_mode(0o755)).unwrap();
        let paths = SetupPathContext {
            launch_cwd: root.clone(),
            home: Some(root.clone()),
            xdg_config_home: None,
            kiro_home: None,
            nac_home: None,
            prime_agent_dir: Some(agent.clone()),
            current_exe: Some(moraine),
            nac_snapshot: OnceCell::new(),
            nac_expected_content: OnceCell::new(),
        };
        let config = ConfigTarget {
            path: root.join("moraine.toml"),
            source: ConfigTargetSource::Cli,
        };

        let error = mcp_plan(SetupMcpTarget::PrimeAgent, &config, &paths)
            .expect_err("custom settings conflict");
        assert!(format!("{error:#}").contains("customized"));
        assert!(!agent.join("skills/moraine").exists());
        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[test]
    fn mcp_executable_must_be_an_executable_sibling() {
        use std::os::unix::fs::PermissionsExt;
        let root = temp_dir("binary");
        let moraine = root.join("moraine");
        let mcp = root.join("moraine-mcp");
        fs::write(&moraine, "binary").expect("moraine");
        fs::write(&mcp, "binary").expect("mcp");
        fs::set_permissions(&moraine, fs::Permissions::from_mode(0o755)).unwrap();
        fs::set_permissions(&mcp, fs::Permissions::from_mode(0o755)).unwrap();
        assert_eq!(
            resolve_mcp_executable(&moraine).expect("resolve"),
            fs::canonicalize(&mcp).expect("canonical mcp")
        );
        fs::set_permissions(&mcp, fs::Permissions::from_mode(0o644)).unwrap();
        assert!(resolve_mcp_executable(&moraine)
            .expect_err("not executable")
            .to_string()
            .contains("not executable"));
        let _ = fs::remove_dir_all(root);
    }
}
