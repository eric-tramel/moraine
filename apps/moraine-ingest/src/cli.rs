use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct CliArgs {
    pub config_path: PathBuf,
}

enum ParseOutcome {
    Args(CliArgs),
    Help,
}

fn usage() {
    eprintln!(
        "usage:
  moraine-ingest [--config <path>]
"
    );
}

fn parse_args_impl(mut args: impl Iterator<Item = String>) -> Result<ParseOutcome, String> {
    let mut config_path: Option<PathBuf> = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                let value = args
                    .next()
                    .ok_or_else(|| "--config requires a value".to_string())?;
                config_path = Some(PathBuf::from(value));
            }
            "-h" | "--help" | "help" => {
                return Ok(ParseOutcome::Help);
            }
            _ => {}
        }
    }

    Ok(ParseOutcome::Args(CliArgs {
        config_path: moraine_config::resolve_ingest_config_path(config_path),
    }))
}

pub fn parse_args() -> CliArgs {
    match parse_args_impl(std::env::args().skip(1)) {
        Ok(ParseOutcome::Args(args)) => args,
        Ok(ParseOutcome::Help) => {
            usage();
            std::process::exit(0);
        }
        Err(error) => {
            eprintln!("error: {error}");
            usage();
            std::process::exit(2);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{parse_args_impl, ParseOutcome};
    use std::path::PathBuf;

    #[test]
    fn parse_args_rejects_config_without_value() {
        let result = parse_args_impl(vec!["--config".to_string()].into_iter());
        assert!(matches!(
            result,
            Err(error) if error == "--config requires a value"
        ));
    }

    #[test]
    fn parse_args_accepts_config_with_value() {
        let result =
            parse_args_impl(vec!["--config".to_string(), "custom.toml".to_string()].into_iter());

        let ParseOutcome::Args(args) = result.expect("parse success") else {
            panic!("expected parsed args");
        };

        assert_eq!(args.config_path, PathBuf::from("custom.toml"));
    }
}
