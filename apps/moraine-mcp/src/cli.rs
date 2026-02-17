use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct CliArgs {
    pub config_path: PathBuf,
}

fn usage() {
    eprintln!(
        "usage:
  moraine-mcp [--config <path>]
"
    );
}

pub fn parse_args() -> CliArgs {
    let mut args = std::env::args().skip(1);
    let mut config_path: Option<PathBuf> = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                if let Some(value) = args.next() {
                    config_path = Some(PathBuf::from(value));
                }
            }
            "-h" | "--help" | "help" => {
                usage();
                std::process::exit(0);
            }
            _ => {}
        }
    }

    CliArgs {
        config_path: moraine_config::resolve_mcp_config_path(config_path),
    }
}
