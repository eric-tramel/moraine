# Environment Variables

This page is the canonical list of environment variables currently read by Moraine binaries and first-party scripts in this repository.

## Runtime and Config Resolution

These variables affect runtime behavior of `moraine`, `moraine-ingest`, `moraine-monitor`, and `moraine-mcp`.

| Variable | Scope | Behavior |
|---|---|---|
| `MORAINE_CONFIG` | All services | Base config path override. Used directly and as fallback for service-specific config env vars. |
| `MORAINE_MCP_CONFIG` | MCP service | MCP config override. Falls back to `MORAINE_CONFIG` if unset. |
| `MORAINE_MONITOR_CONFIG` | Monitor service | Monitor config override. Falls back to `MORAINE_CONFIG` if unset. |
| `MORAINE_INGEST_CONFIG` | Ingest service | Ingest config override. Falls back to `MORAINE_CONFIG` if unset. |
| `MORAINE_SERVICE_BIN_DIR` | `moraine` process manager | Overrides the directory probed for service binaries (`moraine-ingest`, `moraine-monitor`, `moraine-mcp`). |
| `MORAINE_SOURCE_TREE_MODE` | `moraine` process manager | When truthy (`1`, `true`, `yes`, `on`), allows source-tree fallback probes (for example `target/debug/*`) when service binaries are not found in installed locations. |
| `MORAINE_MONITOR_STATIC_DIR` | Monitor static asset resolution | Overrides monitor static asset directory when the given path exists. |

## Installer (`scripts/install.sh`)

These variables configure installer behavior.

| Variable | Default | Behavior |
|---|---|---|
| `MORAINE_INSTALL_REPO` | `eric-tramel/moraine` | GitHub repo used for release asset lookup. |
| `MORAINE_INSTALL_VERSION` | `latest` | Release tag to install. `latest` resolves via GitHub Releases API. |
| `MORAINE_INSTALL_ASSET_BASE_URL` | unset | Custom base URL for bundle and checksum assets. Requires `MORAINE_INSTALL_VERSION` to be a non-`latest` tag. |
| `MORAINE_INSTALL_SKIP_CLICKHOUSE` | unset | Skip managed ClickHouse install when truthy (`1`, `true`, `yes`, `on`). |
| `MORAINE_INSTALL_DIR` | unset | Explicit install target directory for installed binaries. Highest precedence for install location. |

Installer install-directory precedence:

1. `MORAINE_INSTALL_DIR`
2. `XDG_BIN_HOME`
3. `$(dirname "$XDG_DATA_HOME")/bin`
4. `~/.local/bin`

Installer receipt location:

- `${XDG_CONFIG_HOME:-~/.config}/moraine/install-receipt.json`

## Source-Tree Wrapper Controls (`bin/moraine`)

These are developer-focused controls for the source-tree wrapper script (`bin/moraine`).

| Variable | Default | Behavior |
|---|---|---|
| `MORAINE_CTL_CARGO` | `cargo` | Cargo binary used by the wrapper for builds/runs. |
| `MORAINE_CTL_USE_CARGO_RUN` | `0` | When set to `1`, wrapper executes `cargo run` instead of invoking `target/debug/moraine` directly. |
| `MORAINE_CTL_SKIP_BUILD` | `0` | When set to `1`, wrapper skips build-before-run. If `target/debug/moraine` is missing, wrapper exits with an error. |

## CI and Test Variables

These variables are used by repository CI/e2e workflows and test helpers.

| Variable | Scope | Behavior |
|---|---|---|
| `MORAINECTL_BIN` | `scripts/ci/e2e-stack.sh` | Overrides the `moraine` binary path used by e2e stack tests. |
| `MORAINE_TEST_KEYWORD` | `scripts/ci/e2e-stack.sh` | Base keyword seed used to generate deterministic e2e search markers. |
| `MORAINE_E2E_CODEX_KEYWORD` | `web/monitor` live e2e | Optional expected Codex keyword for monitor live Playwright assertions. |
| `MORAINE_E2E_CLAUDE_KEYWORD` | `web/monitor` live e2e | Optional expected Claude keyword for monitor live Playwright assertions. |
| `MORAINE_E2E_CODEX_TRACE_MARKER` | `web/monitor` live e2e | Optional expected Codex trace marker for monitor live Playwright assertions. |
| `MORAINE_E2E_CLAUDE_TRACE_MARKER` | `web/monitor` live e2e | Optional expected Claude trace marker for monitor live Playwright assertions. |
| `MORAINE_CONFIG_TEST_KEY` | `crates/moraine-config` tests | Internal unit-test variable used by config resolution tests. Not a public runtime contract. |
| `MORAINE_CONFIG_TEST_DOES_NOT_EXIST` | `crates/moraine-config` tests | Internal unit-test sentinel variable. Not a public runtime contract. |

## Notes

- `__MORAINE_HOME__` appearing in templates/config files is a placeholder token, not an environment variable.
- This list intentionally includes CI/test variables to be exhaustive for the repository.
