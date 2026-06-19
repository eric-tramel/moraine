use clap::ValueEnum;

#[derive(Clone, Copy, PartialEq, Eq, Debug, ValueEnum, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum Service {
    #[value(name = "clickhouse")]
    ClickHouse,
    #[value(name = "ingest")]
    Ingest,
    #[value(name = "monitor")]
    Monitor,
    #[value(name = "mcp")]
    Mcp,
}

impl Service {
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::ClickHouse => "clickhouse",
            Self::Ingest => "ingest",
            Self::Monitor => "monitor",
            Self::Mcp => "mcp",
        }
    }

    pub(crate) fn pid_file(self) -> &'static str {
        match self {
            Self::ClickHouse => "clickhouse.pid",
            Self::Ingest => "ingest.pid",
            Self::Monitor => "monitor.pid",
            Self::Mcp => "mcp.pid",
        }
    }

    pub(crate) fn log_file(self) -> &'static str {
        match self {
            Self::ClickHouse => "clickhouse.log",
            Self::Ingest => "ingest.log",
            Self::Monitor => "monitor.log",
            Self::Mcp => "mcp.log",
        }
    }

    pub(crate) fn binary_name(self) -> Option<&'static str> {
        match self {
            Self::ClickHouse => None,
            Self::Ingest => Some("moraine-ingest"),
            Self::Monitor => Some("moraine-monitor"),
            Self::Mcp => Some("moraine-mcp"),
        }
    }
}
