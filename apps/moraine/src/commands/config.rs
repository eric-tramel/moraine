use anyhow::Result;
use moraine_config::AppConfig;
use std::process::ExitCode;

use crate::cli::{ConfigArgs, ConfigCommand, ConfigKey};
use crate::render::CliOutput;

pub(super) fn handle(cfg: &AppConfig, args: ConfigArgs) -> Result<ExitCode> {
    match args.command {
        ConfigCommand::Get(get) => {
            let output = CliOutput::from_options(get.output.output, false);
            let key = get.key.as_str();
            let value = get_value(cfg, get.key);
            if output.is_json() {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "key": key,
                        "value": value,
                    }))?
                );
            } else {
                println!("{value}");
            }
            Ok(ExitCode::SUCCESS)
        }
    }
}

fn get_value(cfg: &AppConfig, key: ConfigKey) -> String {
    match key {
        ConfigKey::BackendStartOnUp => cfg.backend.start_on_up.to_string(),
        ConfigKey::ClickhouseUrl => cfg.clickhouse.url.clone(),
        ConfigKey::ClickhouseDatabase => cfg.clickhouse.database.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_value_returns_supported_keys() {
        let mut cfg = AppConfig::default();
        cfg.clickhouse.url = "http://127.0.0.1:18123".to_string();
        cfg.clickhouse.database = "analytics".to_string();

        assert_eq!(
            get_value(&cfg, ConfigKey::ClickhouseUrl),
            "http://127.0.0.1:18123"
        );
        assert_eq!(get_value(&cfg, ConfigKey::ClickhouseDatabase), "analytics");
        cfg.backend.start_on_up = true;
        assert_eq!(get_value(&cfg, ConfigKey::BackendStartOnUp), "true");
    }
}
