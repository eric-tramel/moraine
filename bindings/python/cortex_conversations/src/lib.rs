use cortex_clickhouse::ClickHouseClient;
use cortex_config::ClickHouseConfig;
use cortex_conversations::{
    ClickHouseConversationRepository, ConversationDetailOptions, ConversationListFilter,
    ConversationMode, ConversationRepository, ConversationSearchQuery, PageRequest, RepoConfig,
};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;

#[pyclass]
struct ConversationClient {
    repo: ClickHouseConversationRepository,
    rt: tokio::runtime::Runtime,
}

#[pymethods]
impl ConversationClient {
    #[new]
    #[pyo3(signature = (
        url,
        database = "cortex".to_string(),
        username = "default".to_string(),
        password = "".to_string(),
        timeout_seconds = 5.0,
        max_results = 100,
    ))]
    fn new(
        url: String,
        database: String,
        username: String,
        password: String,
        timeout_seconds: f64,
        max_results: u16,
    ) -> PyResult<Self> {
        let clickhouse = ClickHouseClient::new(ClickHouseConfig {
            url,
            database,
            username,
            password,
            timeout_seconds,
            async_insert: true,
            wait_for_async_insert: true,
        })
        .map_err(py_runtime_err)?;

        let repo = ClickHouseConversationRepository::new(
            clickhouse,
            RepoConfig {
                max_results,
                ..RepoConfig::default()
            },
        );

        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(py_runtime_err)?;

        Ok(Self { repo, rt })
    }

    #[pyo3(signature = (from_unix_ms=None, to_unix_ms=None, mode=None, limit=50, cursor=None))]
    fn list_conversations_json(
        &self,
        from_unix_ms: Option<i64>,
        to_unix_ms: Option<i64>,
        mode: Option<&str>,
        limit: u16,
        cursor: Option<String>,
    ) -> PyResult<String> {
        let parsed_mode = parse_mode(mode)?;
        let page = self
            .rt
            .block_on(self.repo.list_conversations(
                ConversationListFilter {
                    from_unix_ms,
                    to_unix_ms,
                    mode: parsed_mode,
                },
                PageRequest { limit, cursor },
            ))
            .map_err(py_runtime_err)?;

        serde_json::to_string(&page).map_err(py_runtime_err)
    }

    #[pyo3(signature = (session_id, include_turns=false))]
    fn get_conversation_json(&self, session_id: String, include_turns: bool) -> PyResult<String> {
        let conversation = self
            .rt
            .block_on(
                self.repo
                    .get_conversation(&session_id, ConversationDetailOptions { include_turns }),
            )
            .map_err(py_runtime_err)?;

        serde_json::to_string(&conversation).map_err(py_runtime_err)
    }

    #[pyo3(signature = (
        query,
        limit=None,
        min_score=None,
        min_should_match=None,
        from_unix_ms=None,
        to_unix_ms=None,
        mode=None,
        include_tool_events=None,
        exclude_codex_mcp=None,
    ))]
    fn search_conversations_json(
        &self,
        query: String,
        limit: Option<u16>,
        min_score: Option<f64>,
        min_should_match: Option<u16>,
        from_unix_ms: Option<i64>,
        to_unix_ms: Option<i64>,
        mode: Option<&str>,
        include_tool_events: Option<bool>,
        exclude_codex_mcp: Option<bool>,
    ) -> PyResult<String> {
        let parsed_mode = parse_mode(mode)?;
        let results = self
            .rt
            .block_on(self.repo.search_conversations(ConversationSearchQuery {
                query,
                limit,
                min_score,
                min_should_match,
                from_unix_ms,
                to_unix_ms,
                mode: parsed_mode,
                include_tool_events,
                exclude_codex_mcp,
            }))
            .map_err(py_runtime_err)?;

        serde_json::to_string(&results).map_err(py_runtime_err)
    }
}

fn parse_mode(raw: Option<&str>) -> PyResult<Option<ConversationMode>> {
    let Some(raw) = raw else {
        return Ok(None);
    };

    match raw {
        "web_search" => Ok(Some(ConversationMode::WebSearch)),
        "mcp_internal" => Ok(Some(ConversationMode::McpInternal)),
        "tool_calling" => Ok(Some(ConversationMode::ToolCalling)),
        "chat" => Ok(Some(ConversationMode::Chat)),
        _ => Err(PyValueError::new_err(
            "mode must be one of: web_search, mcp_internal, tool_calling, chat",
        )),
    }
}

fn py_runtime_err(err: impl ToString) -> PyErr {
    PyRuntimeError::new_err(err.to_string())
}

#[pymodule]
fn _core(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add_class::<ConversationClient>()?;
    Ok(())
}
