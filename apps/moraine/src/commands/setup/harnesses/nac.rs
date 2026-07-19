use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use toml_edit::{value as toml_value, Array, DocumentMut, InlineTable, Item, Table, Value};

const CUSTOM_SOURCE_NAME: &str = "nac-workspace";

#[derive(Debug, Clone)]
pub(crate) struct ConfigSnapshot {
    path: PathBuf,
    original: Option<Vec<u8>>,
    document: DocumentMut,
}

impl ConfigSnapshot {
    pub(super) fn read(path: PathBuf) -> Result<Self> {
        let original = match fs::read(&path) {
            Ok(content) => Some(content),
            Err(exc) if exc.kind() == ErrorKind::NotFound => None,
            Err(exc) => {
                return Err(exc).with_context(|| format!("failed to read {}", path.display()))
            }
        };
        let document = match original.as_deref() {
            None | Some([]) => DocumentMut::new(),
            Some(content) if content.iter().all(u8::is_ascii_whitespace) => DocumentMut::new(),
            Some(content) => {
                let content = std::str::from_utf8(content)
                    .with_context(|| format!("{} is not valid UTF-8 TOML", path.display()))?;
                content
                    .parse::<DocumentMut>()
                    .with_context(|| format!("{} is not valid NAC TOML", path.display()))?
            }
        };
        Ok(Self {
            path,
            original,
            document,
        })
    }

    pub(super) fn path(&self) -> &Path {
        &self.path
    }

    pub(super) fn resolve_store(
        &self,
        launch_cwd: &Path,
        nac_home: PathBuf,
        stable_home: bool,
    ) -> Result<StoreResolution> {
        let configured = match self.document.get("storage") {
            None => None,
            Some(storage) => {
                let storage = storage.as_table_like().ok_or_else(|| {
                    anyhow::anyhow!("{} storage must be a TOML table", self.path.display())
                })?;
                storage
                    .get("store_path")
                    .map(|value| {
                        value.as_str().map(PathBuf::from).ok_or_else(|| {
                            anyhow::anyhow!(
                                "{} storage.store_path must be a TOML string",
                                self.path.display()
                            )
                        })
                    })
                    .transpose()?
            }
        };
        let (path, auto_ingest) = match configured {
            Some(path) if path.is_absolute() => (path, true),
            Some(path) => (launch_cwd.join(path), false),
            None => (nac_home.join("store.db"), stable_home),
        };
        Ok(StoreResolution { path, auto_ingest })
    }

    pub(super) fn prepare_mcp_write(&self, command: &[String]) -> Result<PreparedMcpWrite> {
        let mut document = self.document.clone();
        validate_mcp_tables(&document).with_context(|| {
            format!("{} has invalid NAC MCP configuration", self.path.display())
        })?;
        merge_moraine_server(&mut document, command)?;
        let rendered = document.to_string().into_bytes();
        std::str::from_utf8(&rendered)
            .expect("toml_edit renders UTF-8")
            .parse::<DocumentMut>()
            .with_context(|| format!("rendered {} is not valid TOML", self.path.display()))?;
        Ok(PreparedMcpWrite {
            snapshot: self.clone(),
            rendered,
        })
    }

    pub(super) fn verify_current(&self, expected_after_apply: Option<&[u8]>) -> Result<()> {
        let expected = expected_after_apply
            .map(|content| Some(content.to_vec()))
            .unwrap_or_else(|| self.original.clone());
        let current = read_optional(&self.path)?;
        if current != expected {
            bail!(
                "{} changed after setup read it; refusing to overwrite a concurrent update",
                self.path.display()
            );
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub(super) struct PreparedMcpWrite {
    snapshot: ConfigSnapshot,
    rendered: Vec<u8>,
}

impl PreparedMcpWrite {
    pub(super) fn path(&self) -> &Path {
        self.snapshot.path()
    }

    pub(super) fn rendered(&self) -> &[u8] {
        &self.rendered
    }

    pub(super) fn is_unchanged(&self) -> bool {
        self.snapshot.original.as_deref() == Some(self.rendered.as_slice())
    }

    pub(super) fn verify_snapshot_current(&self) -> Result<()> {
        self.snapshot.verify_current(None)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct StoreResolution {
    pub(super) path: PathBuf,
    pub(super) auto_ingest: bool,
}

impl StoreResolution {
    pub(super) fn manual_guidance(&self) -> Result<Option<String>> {
        if self.auto_ingest {
            return Ok(None);
        }
        let (path, watch_root) = self.path_strings()?;
        let escaped = moraine_config::escape_literal_glob(path);
        Ok(Some(format!(
            "NAC's store path is launch-directory-relative, so setup did not add an ingest source that could silently follow the wrong database. Add one source for each launch directory you use. Choose a unique source name for every workspace and do not use the setup-reserved name `nac`; the same unique-name rule applies to a manually configured NAC CLI `--store-path` source. For this setup invocation:\n[[ingest.sources]]\nname = \"{CUSTOM_SOURCE_NAME}\"\nharness = \"nac\"\nenabled = true\nglob = {escaped:?}\nwatch_root = {watch_root:?}\nformat = \"nac_sqlite\""
        )))
    }

    pub(super) fn validate_paths(&self) -> Result<()> {
        self.path_strings().map(|_| ())
    }

    pub(super) fn source_paths(&self) -> Result<(String, String)> {
        let (path, watch_root) = self.path_strings()?;
        Ok((
            moraine_config::escape_literal_glob(path),
            watch_root.to_string(),
        ))
    }

    fn path_strings(&self) -> Result<(&str, &str)> {
        let path = path_str(&self.path, "resolved NAC store path")?;
        let watch_root = self.path.parent().unwrap_or_else(|| Path::new("."));
        let watch_root = path_str(watch_root, "resolved NAC watch path")?;
        Ok((path, watch_root))
    }
}

fn path_str<'a>(path: &'a Path, label: &str) -> Result<&'a str> {
    path.to_str()
        .ok_or_else(|| anyhow::anyhow!("{label} is not valid UTF-8: {}", path.display()))
}

fn read_optional(path: &Path) -> Result<Option<Vec<u8>>> {
    match fs::read(path) {
        Ok(content) => Ok(Some(content)),
        Err(exc) if exc.kind() == ErrorKind::NotFound => Ok(None),
        Err(exc) => Err(exc).with_context(|| format!("failed to read {}", path.display())),
    }
}

fn validate_mcp_tables(document: &DocumentMut) -> Result<()> {
    let Some(servers) = document.as_table().get("mcp_servers") else {
        return Ok(());
    };
    if let Some(servers) = servers.as_table() {
        if let Some(server) = servers.get("moraine") {
            if !server.is_table() && !server.is_inline_table() {
                bail!("mcp_servers.moraine must be a TOML table");
            }
        }
        return Ok(());
    }
    if let Some(servers) = servers.as_inline_table() {
        if let Some(server) = servers.get("moraine") {
            if !server.is_inline_table() {
                bail!("mcp_servers.moraine must be a TOML table");
            }
        }
        return Ok(());
    }
    bail!("mcp_servers must be a TOML table")
}

fn merge_moraine_server(document: &mut DocumentMut, command: &[String]) -> Result<()> {
    if document.as_table().get("mcp_servers").is_none() {
        document["mcp_servers"] = Item::Table(Table::new());
    }
    let servers = document
        .as_table_mut()
        .get_mut("mcp_servers")
        .expect("mcp_servers was inserted above");
    if let Some(servers) = servers.as_table_mut() {
        match servers.get_mut("moraine") {
            Some(server) if server.is_table() => fill_table(
                server
                    .as_table_mut()
                    .expect("server was checked as a table"),
                command,
            ),
            Some(server) if server.is_inline_table() => fill_inline(
                server
                    .as_inline_table_mut()
                    .expect("server was checked as an inline table"),
                command,
            ),
            Some(_) => bail!("mcp_servers.moraine must be a TOML table"),
            None => {
                let mut server = Table::new();
                fill_table(&mut server, command);
                servers.insert("moraine", Item::Table(server));
            }
        }
    } else if let Some(servers) = servers.as_inline_table_mut() {
        match servers.get_mut("moraine") {
            Some(server) if server.is_inline_table() => fill_inline(
                server
                    .as_inline_table_mut()
                    .expect("server was checked as an inline table"),
                command,
            ),
            Some(_) => bail!("mcp_servers.moraine must be a TOML table"),
            None => {
                let mut server = InlineTable::new();
                fill_inline(&mut server, command);
                servers.insert("moraine", Value::InlineTable(server));
            }
        }
    } else {
        bail!("mcp_servers must be a TOML table");
    }
    Ok(())
}

fn fill_table(server: &mut Table, command: &[String]) {
    server.clear();
    server["enabled"] = toml_value(true);
    server["transport"] = toml_value("stdio");
    server["command"] = toml_value("moraine");
    server["args"] = Item::Value(Value::Array(args_array(command)));
}

fn fill_inline(server: &mut InlineTable, command: &[String]) {
    server.clear();
    server.insert("enabled", Value::from(true));
    server.insert("transport", Value::from("stdio"));
    server.insert("command", Value::from("moraine"));
    server.insert("args", Value::Array(args_array(command)));
}

fn args_array(command: &[String]) -> Array {
    let mut args = Array::new();
    for arg in command {
        args.push(arg.as_str());
    }
    args
}
