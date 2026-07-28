# PR 560 config-provenance security verifier

Run this verifier cold from the Moraine repository root. Treat every Required item as falsifiable; report `PASS` or `FAIL` for each with command output or file/line evidence. Overall verdict is `PASS` only when every Required item passes.

## Required

1. **Isolated delivery.** `git branch --show-current` is a staging branch other than `feat/clickhouse-env-credentials`; `git status --short` contains no unintended files; the implementation is committed and pushed to a new branch in `candyflipline/moraine`. Live GitHub metadata for `eric-tramel/moraine#560` still reports head SHA `5fdaea9805badab6226477bd266ed02441f20fc9`. No PR body, comments, reviews, labels, or metadata were changed.

2. **Current base and complete feature.** The staging branch contains current `upstream/main`, the environment-backed ClickHouse feature, and the provenance fix. `git merge-base --is-ancestor upstream/main HEAD` exits zero. The existing `{ env = "NAME" }` behavior, validation, redaction, setup protection, and documentation remain present.

3. **Trust is based on selection provenance, not pathname.** Resolution preserves at least `Explicit`, `Home`, and `ImplicitRepoFallback`. CLI paths and supported config environment overrides are explicit; the user home config is home-owned; only automatic cwd `config/moraine.toml` discovery is the implicit repository fallback. An explicitly selected `./config/moraine.toml` remains trusted.

4. **No inherited-secret lookup from implicit repository config.** Before any `std::env::var` lookup for a ClickHouse value, loading knows the config origin. `{ env = "SENSITIVE_TOKEN" }` in an implicitly discovered repository config fails with a clear non-secret error. The sentinel value is absent from normal and pretty/debug error output, and no ClickHouse/network operation is used by the regression test.

5. **Compatibility.** Explicitly selected and home configs resolve valid environment references for `url`, `database`, `username`, and `password`, including named/default backends. Literal ClickHouse values still load from implicit repository fallback. Missing, non-Unicode, malformed, and invalid-name behavior remains covered. Passwords remain redacted.

6. **Every auto-resolving runtime preserves provenance.** The main CLI, MCP, ingest, monitor compatibility path, and legacy wrapper cannot resolve a path and then silently discard its origin before loading. Search all `resolve_*config_path` and `load_config` call sites and provide evidence.

7. **Deterministic verification passes.** Run `cargo test -p moraine-config --lib --locked`, the path-relevant binary tests for `moraine`, `moraine-mcp`, `moraine-ingest`, and `moraine-monitor`, `make ci-check`, and `make docs-build`. Every selected command exits zero with nonzero tests where applicable.

8. **Repository review and runtime QA pass.** Run the repository-required code-review workflow and resolve or explicitly reject every finding with evidence. Run the repository-required sandbox QA appropriate to this config/runtime boundary, capture its owned sandbox ID, exercise trusted and implicit-fallback behavior without real credentials, and tear down that exact sandbox. No owned sandbox remains.

## Nice to have

- The rejection message tells users to opt in with `--config ./config/moraine.toml` or `MORAINE_CONFIG=./config/moraine.toml`.

## Verdict

Return a table of conditions 1-8 with evidence and `PASS`/`FAIL`, followed by `OVERALL: PASS` only if all eight pass.
