# Testing and benchmarking

Moraine deliberately uses each ecosystem's native harness. Cargo/libtest, Bun/Vitest,
Playwright, pytest, Python `unittest`, shell stack checks, and the sandbox remain direct
entry points; there is no universal repository test runner.

`make test` means **the root Rust workspace only**. `make ci-check` adds the dependency
policy, Rust formatting, strict clippy, and the locked workspace build, but it is still
not proof of every repository surface. Repository-wide validation is the set of
path-relevant native suites in this document.

## Stable entry points

Run focused suites first, then expand according to the changed contract.

| Intent | Native entry point | Scope |
| --- | --- | --- |
| Discover common local commands | `make help` | Help only. |
| Root Rust workspace tests | `make test` | `cargo test --workspace --locked`; excludes frontend, browser, bindings, stack, paid-agent, packaging, and benchmark runs. |
| Local Rust/policy baseline | `make ci-check` | Dependency policy, fmt, strict clippy, locked workspace build, and root workspace tests. It is not repository-wide validation. |
| Source-tree functional stack | `bash scripts/ci/e2e-stack.sh` | One owned ClickHouse/ingest/backend/MCP lifecycle. |
| Installed-artifact functional stack | `bash scripts/ci/e2e-install-artifact.sh <target-triple>` | Package, install into an isolated home, then run the canonical stack lifecycle. |
| Frontend deterministic checks | `cd web/monitor && bun install --frozen-lockfile && bun run typecheck && bun run test` | Svelte/TypeScript plus Vitest; not Playwright. |
| Mocked browser | `cd web/monitor && bun install --frozen-lockfile && bunx playwright@1.58.2 install chromium && bun run test:e2e:mocked` | Exactly two Chromium cases, one worker, no retries. |
| Python binding smoke | See [Binding suite](#binding-suite) | Binding-local locked venv, maturin build, and pytest. |
| Live ClickHouse schema/parity/publication | `scripts/dev/sandbox/run-live-test analytics-schema`, `scripts/dev/sandbox/run-live-test analytics-parity`, `scripts/dev/sandbox/run-live-test source-publication`, or `scripts/dev/sandbox/run-live-test mcp-backfill` | Fresh owned sandbox and exact ignored test. |
| Fixed-resource performance suite | `python3 scripts/bench/performance_suite.py validate <artifact.json> [...]` | Validates scenario, comparison, repeatability, and root suite documents. |

Rare/manual benchmark, paid-agent, and raw ignored-test commands stay direct and are
listed in their owning sections rather than gaining Make wrappers.

## Taxonomy and placement

Classification describes the observable boundary, not the filename.

| Harness class | Placement | Contract |
| --- | --- | --- |
| `unit` | Inline `#[cfg(test)] mod tests` or `src/<module>/tests.rs` | Deterministic private behavior; no live database, subprocess stack, shared home, or uncontrolled network. |
| `integration` | `<crate>/tests/<suite>/main.rs` plus suite-local modules/support | Public crate behavior, optionally through an owned in-process loopback server. |
| `live_db` | Dedicated Cargo target and exact ignored function | ClickHouse analyzer, migration, schema, view, JSONEachRow, or shared-dataset behavior mocks cannot prove. |
| `stack` | `scripts/ci/e2e-stack.sh` and installed-artifact wrapper | Real binaries, persistence, networking, topology, fallback, and lifecycle. |
| `frontend` | `web/monitor/src/**/*.test.ts` | jsdom client/component behavior and mocked HTTP. |
| `browser` | `web/monitor/e2e/*.spec.ts` | Chromium interaction; mocked and live suites have different ownership. |
| `binding` | Binding-local pytest | Public PyO3 behavior in its separately provisioned package. |
| `packaging` | Existing packaging/install scripts | Artifact contents, installation, entry points, and platform compatibility. |
| `benchmark` | Package-local `benches/` or `scripts/bench/` | Reproducible scenario, semantic oracle, v1 artifact, and non-blocking timing. |
| `paid_agent` | Existing sandbox agent smoke | Real model/tool compatibility requiring a secret and paid calls. |

Contract tags are a second axis: `cli`, `http`, `mcp`, `migration`, `golden`,
`routing`, `serialization`, `schema`, `topology`, `cache`, and `lifecycle` are
used below. Prefer the cheapest harness that faithfully observes the contract.
Do not widen production visibility, add a Cargo feature, or create a production
dependency merely to relocate a test.

### Result semantics

A selected deterministic command passes only when its process exits zero and its
expected test count is nonzero. Missing tools, malformed or stale lockfiles, missing
fixtures, zero collection, launch failure, timeout, cleanup failure, and assertion
failure are failures, never skips. `#[ignore]` routes a test to an explicit owner; it
does not remove ownership.

`not_run` is available only to scheduled/manual orchestration when a declared live or
paid prerequisite is absent. Directly invoking the concrete live command fails closed.
No deterministic job may report `not_run`. No suite uses blanket retries, blanket
`--ignored`, or global single-threading to hide a contract failure.

## Rust workspace suite registry

The root workspace contains ten packages. The commands below are exact focused
entry points; `cargo test --workspace --locked` runs all deterministic rows and library
doctests. Libtest has no built-in wall-clock timeout; CI/workflow or suite-specific
timeouts below are the owning deadline. Temporary files, port-0 listeners, child
processes, and environment changes are owned by the invoking test and restored before
it returns.

### Source-local target harnesses

| Owner / target | Class and tags | Exact focused command | Prerequisites and mutable resources | Tier and result |
| --- | --- | --- | --- | --- |
| `moraine-config` library | `unit`; config, serialization, migration | `cargo test -p moraine-config --lib --locked` | Rust/Cargo; test-local temp files/env guards. | T0; zero/failure is fail. |
| `moraine-clickhouse` library | `unit`; http, serialization, error mapping | `cargo test -p moraine-clickhouse --lib --locked` | Rust/Cargo; owned loopback mocks only. | T0. |
| `moraine-conversations` library | `unit`; repository, SQL wire, cache, routing, analytics | `cargo test -p moraine-conversations --lib --locked` | Rust/Cargo; test-local memory, temp paths, and port-0 mocks. | T0. |
| `moraine-ingest-core` library | `unit`; normalization, ingestion, config | `cargo test -p moraine-ingest-core --lib --locked` | Rust/Cargo; committed fixtures plus test-local files. | T0. |
| `moraine-mcp-core` library | `unit`; MCP, routing, serialization, socket lifecycle | `cargo test -p moraine-mcp-core --lib --locked` | Rust/Cargo; Unix socket cases are selected only on supported hosts and own their paths/listeners. | T0 Linux/macOS. |
| `moraine-monitor-core` library | `unit`; HTTP, serialization, static assets | `cargo test -p moraine-monitor-core --lib --locked` | Rust/Cargo; in-process routers/mocks. | T0. |
| `moraine-ingest` binary | `unit`; CLI, ingestion lifecycle | `cargo test -p moraine-ingest --bin moraine-ingest --locked` | Rust/Cargo; test-local config/temp resources. | T0. |
| `moraine-mcp` binary | `unit`; CLI, daemon/MCP lifecycle, routing | `cargo test -p moraine-mcp --bin moraine-mcp --locked` | Rust/Cargo; owned loopback/socket/process resources. | T0 Linux/macOS where selected. |
| `moraine-monitor` compatibility binary | `unit`; CLI compatibility | `cargo test -p moraine-monitor --bin moraine-monitor --locked` | Rust/Cargo; no live service. | T0. The binary remains compatibility-only, but this workspace target is covered. |
| `moraine` binary | `unit`; CLI, setup, status, supervision | `cargo test -p moraine --bin moraine --locked` | Rust/Cargo; temp homes/configs and owned subprocesses. | T0 Linux/macOS where selected. |

### Cargo integration targets

| Owner / target | Class and tags | Exact command | Prerequisites, resources, cleanup | Tier and result |
| --- | --- | --- | --- | --- |
| `moraine-conversations/repository_integration` | `integration`; repository, SQL-wire, cache, search, sessions, analytics | `cargo test -p moraine-conversations --test repository_integration --locked` | Rust/Cargo. One executable uses owned Axum mock-ClickHouse listeners on `127.0.0.1:0`; Tokio owns task/socket teardown; no real ClickHouse or persistent files. | T0; the owning CI job supplies the wall-clock timeout. The 67-test move map is `crates/moraine-conversations/tests/repository_integration/test-name-map.json`. |
| `moraine-conversations/live_clickhouse` deterministic support | `integration`; destructive guards, ownership, cleanup composition, analytics schema/parity semantics, bounded-open corpus oracle | `cargo test -p moraine-conversations --test live_clickhouse --locked` | Rust/Cargo; deterministic tests use no live endpoint. The four ignored functions are separately owned below and remain unexecuted by this command. | T0; zero/failure is fail, exactly four live functions remain ignored. |
| `moraine-ingest-core/golden_fixtures` | `integration`; golden, normalization, schema | `cargo test -p moraine-ingest-core --test golden_fixtures --locked` | Rust/Cargo and committed raw/golden families; read-only unless the explicit update mode below is used. | T0; byte drift fails. |
| `moraine-ingest-core/hermes_fixture` | `integration`; ingest, serialization | `cargo test -p moraine-ingest-core --test hermes_fixture --locked` | Committed Hermes trajectory; no external service. | T0. |
| `moraine-ingest-core/hermes_session_fixture` | `integration`; ingest, serialization | `cargo test -p moraine-ingest-core --test hermes_session_fixture --locked` | Committed Hermes session JSON; no external service. | T0. |
| `moraine-ingest-core/kimi_cli_fixture` | `integration`; ingest, parent/subagent links | `cargo test -p moraine-ingest-core --test kimi_cli_fixture --locked` | Committed Kimi fixtures; no external service. | T0. |
| `moraine-ingest-core/qwen_code_fixture` | `integration`; ingest, parent/rewind links, tools, tokens, malformed timestamps | `cargo test -p moraine-ingest-core --test qwen_code_fixture --locked` | Committed sanitized Qwen Code 0.19.x fixture; no external service. | T0. |
| `moraine-ingest-core/normalize_unit` | `integration`; normalization | `cargo test -p moraine-ingest-core --test normalize_unit --locked` | Rust/Cargo; despite its historical name, Cargo classifies it as an integration target. | T0. |
| `moraine/clickhouse_supervision` | `integration`; CLI, process, lifecycle | `cargo test -p moraine --test clickhouse_supervision --locked` | Unix process semantics, temp home/runtime, owned children. | T0 on supported Linux/macOS hosts; cfg-unselected cases are not macOS proof from Linux. |
| `moraine/export_cli` | `integration`; CLI, stdout/stderr/exit | `cargo test -p moraine --test export_cli --locked` | Rust/Cargo; temp files and owned command processes. | T0. |
| `moraine/status_cli` | `integration`; CLI, HTTP/direct fallback, topology | `cargo test -p moraine --test status_cli --locked` | Rust/Cargo; owned mocks/temp homes; individual Unix cases are cfg-selected. | T0 on Linux/macOS. |

The module split of `repository_integration` intentionally remains one linked Cargo
executable and preserves its target command. Support stays local to its owning crate.
A shared support crate is not pre-approved: extraction requires at least two package
adopters with identical semantics and evidence that normal production dependencies,
public API/features, release bundles, dependency policy, and material compile cost are
unchanged. An in-process listener helper can hand an already-bound port-0 listener to
an in-process server; it cannot reserve a port for an unrelated child process.

### Package-isolation acceptance

Manifest, feature, dev-dependency, target-topology, or shared-support changes also run:

```bash
python3 docs/development/test-architecture/package_isolation.py
```

The script first resolves the current `workspace_members` through
`cargo metadata --format-version 1 --locked --no-deps`, rejects empty/unresolved or
ambiguous membership, then runs the metadata-derived command
`cargo test -p <exact-package> --locked --no-run` for every sorted member. It returns
nonzero on the first metadata/tool/compile failure and has no list-only success mode.
This is a package-isolation compile check, not another workspace test runner and not
coverage for the standalone binding or legacy trees.

### Architecture validation harnesses

| Owner / harness | Class and tags | Exact native command | Prerequisites, timeout, resources, cleanup | Tier and result |
| --- | --- | --- | --- | --- |
| `docs/development/test-architecture/package_isolation.py` | `integration`; Cargo metadata, package isolation, target topology | `python3 docs/development/test-architecture/package_isolation.py` | Python 3, locked Cargo toolchain and registry/cache access. The architecture workflow owns a 45m job deadline; Cargo owns its configured target directory. No live service. | Path-filtered T0. Zero workspace packages, missing tools, metadata errors, or any package compile failure returns nonzero; never `not_run` once selected. |
| `scripts/dev/sandbox/tests/test_run_live_test.py` | `unit`; sandbox ownership, concurrency, timeout, signal, cleanup | `python3 -m unittest scripts/dev/sandbox/tests/test_run_live_test.py` | Python 3 and POSIX process semantics; fake Docker/sandbox/Cargo binaries and test-owned private temp roots. Individual fake deadlines are seconds; the architecture workflow owns a 45m job deadline. Every spawned child is registered for TERM/KILL/reap cleanup. | Path-filtered T0. Zero tests or any lifecycle/resource assertion failure returns nonzero. |
| `scripts/bench/tests/test_*.py` | `unit`; fixed-resource protocol, fixtures, runtime, scenarios, schema, comparison, and CLI | `python3 -m unittest discover -v -s scripts/bench/tests -p 'test_*.py'` | Python 3; deterministic tests use test-owned temporary files and fakes, with no live service. The architecture workflow owns a 45m job deadline. | Path-filtered T0. CI counts the selected cases first and fails on zero discovery or any assertion failure. |

## Live ClickHouse suites

Only these five ignored tests are supported. Never use a blanket `--ignored` command.
The wrapper is the contributor entry point where listed; the raw commands document exact routing
and intentionally fail unless `MORAINE_ALLOW_DESTRUCTIVE_TESTS=1` and the endpoint is
the wrapper-owned sandbox.

| Owner / full function | Exact wrapper and raw Cargo command | Prerequisites, timeout, resources, cleanup | Tier and semantics |
| --- | --- | --- | --- |
| `moraine-conversations/live_clickhouse::live_schema_semantics_and_teardown` | `scripts/dev/sandbox/run-live-test analytics-schema`; raw: `cargo test -p moraine-conversations --test live_clickhouse --locked live_schema_semantics_and_teardown -- --exact --ignored --nocapture` | Bash, Docker/Compose, sandbox toolchain. Default wrapper timeout 1,800s (`MORAINE_LIVE_TEST_TIMEOUT_SECONDS` accepts a positive integer). Wrapper owns a fresh `sb-xxxxxx` sandbox and Rust generates an uncaller-controlled `moraine_test_<uuid>` database. Empty, `moraine`, or non-prefix names are refused before SQL. | T3 manual/scheduled. Direct missing/unsafe prerequisites fail. Success and every catchable failure with successful cleanup leave no owned sandbox/database. |
| `moraine-conversations/live_clickhouse::live_monitor_repository_semantic_parity` | `scripts/dev/sandbox/run-live-test analytics-parity`; raw: `cargo test -p moraine-conversations --test live_clickhouse --locked live_monitor_repository_semantic_parity -- --exact --ignored --nocapture` | Same owned sandbox; both arms use the same generated database/dataset. Cardinality, digest, or oracle mismatch fails independently of timing. | T3 unless the same semantics are already proven in T1. Timing is not the pass condition. |
| `moraine-conversations/live_clickhouse::live_source_publication_cutover_crash_recovery` | `scripts/dev/sandbox/run-live-test source-publication`; raw inside the wrapper-owned sandbox: `MORAINE_LIVE_TEST_INGEST_BIN=/opt/moraine/bin/moraine-ingest cargo test -p moraine-conversations --test live_clickhouse --locked live_source_publication_cutover_crash_recovery -- --exact --ignored --nocapture` | Same owned sandbox and generated database. A pre-cutover layer builds the actual schema through migration 030, seeds default-local, named shared, equal-timestamp checkpoint, hostless event, and legacy MCP-control rows, then applies 031–034 with the production runner. It measures the upgrade, forces an idempotent 031 replay, compares raw control-table counts and causal tuples, and proves default-local visibility while shared hostless rows fail closed. The database is then reset. The deterministic layer reconstructs fresh ClickHouse clients and repositories with empty caches across every durable publication stage, pins physical/candidate cardinality, and rejects mixed old/new models. The process layer launches the wrapper-built production `moraine-ingest` binary with one persistent state directory behind a loopback commit-then-drop ClickHouse proxy. It atomically replaces an owned JSONL source, drops acknowledged responses only after the upstream synchronous insert commits, sends real process `SIGKILL`, and restarts the same binary/state. It covers replaying checkpoint boundaries `last_line=0..4`, every pre-head physical/compatibility/final-control stage, and a committed source-head response loss, requiring the causal checkpoint/readiness tuple, source-head history, live model, and exact legacy candidate pointer to recover without an extra publication revision. No production fault hook is compiled or configured. | T3 manual/scheduled and required for source-publication, checkpoint, liveness-view, or publication-consistency changes. Direct failure is fail; wrapper cleanup rules are identical to the other live modes. |
| `moraine-conversations/live_clickhouse::live_mcp_open_batched_backfill_resources` | `scripts/dev/sandbox/run-live-test mcp-backfill`; raw: `cargo test -p moraine-conversations --test live_clickhouse --locked live_mcp_open_batched_backfill_resources -- --exact --ignored --nocapture` inside the wrapper-owned sandbox | Seeds 256 sessions with 32 payload-bearing events each, runs the historical MCP read-model backfill in four 64-session pages, and asserts exact session/event/turn cardinality, one event and turn insert per page, bounded active parts, and ready publication plans. It reconstructs committed-response-loss states at child phases 0 and 1 plus finalization phase 2, then resets only the global cursor; every recovery must preserve raw child cardinality. Prints elapsed time plus query-log rows, bytes, duration, and peak memory for PR resource reporting. | T3 manual regression required for historical MCP projection, migration 034, batching, or projection-resource changes. Timing is reported for comparison; exact batching, replay idempotence, and cardinality are pass conditions. |
| `moraine-conversations/live_clickhouse::live_mcp_open_boundedness_benchmark` | Raw: `cargo test -p moraine-conversations --test live_clickhouse --locked live_mcp_open_boundedness_benchmark -- --exact --ignored --nocapture` inside a caller-owned sandbox | Same owned database guard and cleanup. Opens separate realistic targets spanning 100 turns, 500 full-payload events, and a 1,000-event compact turn; then seeds 100,000 unrelated sessions and 1,000,000 substantial unrelated events into both canonical `events` and the bounded MCP read model. Compares exact session/turn/event semantics before and after growth, exercises sequential/concurrent/recovery opens, and records labeled `system.query_log` latency, throughput, errors, rows, bytes, and memory. Fails on SLA misses, errors, semantic drift, or corpus-linear row/byte/memory growth. Requires about 2 GB free. | T3 manual regression benchmark. Timing and bounded-cost assertions are pass conditions. |

Each wrapper run records its sandbox ID, exact Cargo command, generated database and a
redacted cleanup command before mutation in
`${TMPDIR:-/tmp}/moraine-live-test-diagnostics/run.*/run.log`. It tears down the exact
sandbox on success, boot/test/assertion failure, timeout, and caught INT/TERM/HUP.
Original failure status is preserved; cleanup failure is reported alongside it and is
the only catchable case allowed to retain an owned database. No cleanup is promised
after SIGKILL. Wrapper failure behavior is deterministic under:

```bash
python3 -m unittest scripts/dev/sandbox/tests/test_run_live_test.py
```

Two concurrent wrapper invocations must own distinct sandbox IDs and database names.
Before/after resource census for isolation changes records sandbox/container/volume,
listener, and `moraine_test_` database ownership; the wrapper's diagnostic log is the
retained failure artifact.

Isolation validation for the final implementation ran the 21-case wrapper unittest
suite once; its concurrency case launched exactly two wrapper processes simultaneously
and verified two sandbox/token/database identities. Each raw Cargo route selects one
exact ignored function (one selected libtest case; no global thread-count override).
One final real `analytics-schema` invocation and one final real `analytics-parity`
invocation each started from zero token-owned containers, volumes, networks, listeners,
and `moraine_test_` databases and returned every numeric census to zero after cleanup.
The TERM-resistant boot/test/down cases each ran once in the focused suite; a separate
three-run parallel stress check exercised the TERM/KILL/reap path. Owned resource prefix:
`sb-` for sandboxes and `moraine_test_` for databases.

## Frontend and browser suites

| Owner / suite | Class and tags | Exact command | Prerequisites, timeout, resources, cleanup | Tier and result |
| --- | --- | --- | --- | --- |
| `web/monitor` typecheck + Vitest | `frontend`; HTTP client, components, analytics, serialization | `cd web/monitor && bun install --frozen-lockfile && bun run typecheck && bun run test` | Bun 1.3.9, `bun.lock`, package registry for provisioning. CI timeout 10m; ephemeral `node_modules`. Vitest has `passWithNoTests: false`. | T0 path-filtered. Frozen-install/type/test failure or zero tests fails; never `not_run` once selected. |
| `web/monitor` mocked Playwright | `browser`; HTTP, interaction, responsive layout | `cd web/monitor && bun install --frozen-lockfile && bunx playwright@1.58.2 install chromium && bun run test:e2e:mocked` | Bun 1.3.9, Playwright/Chromium 1.58.2. Exactly two cases, one worker, zero retries; 30s/test, 5s expectations, 120s preview startup. Playwright owns `127.0.0.1:4173` preview/browser teardown; traces remain under its failure results. | Visible, path-filtered, initially non-required T0 candidate. Expected-case reporter makes zero/partial execution fail. Promotion belongs to a dedicated CI-policy change after duration/flake evidence. |
| `web/monitor` live Playwright | `browser`; live HTTP, ingest-to-UI | `cd web/monitor && MONITOR_BASE_URL=<caller-owned-stack-url> bun run test:e2e:live` | One case, one worker, zero retries; 30s/test and 5s expectations. Caller supplies and cleans a seeded owned stack; Playwright owns only Chromium. Missing URL fails during config load and no local server is provisioned. | T3 manual/scheduled; explicit invocation cannot skip. |

Frontend T0 path ownership is `.github/workflows/ci-monitor-frontend.yml`,
`web/monitor/**`, `crates/moraine-monitor-core/**`, root `Cargo.toml`, and `Cargo.lock`.
The mocked-browser workflow owns its own workflow and `web/monitor/**`. Browser
reliability is measured without CI retries; visibility precedes branch-protection
promotion.

## Binding suite

`bindings/python/moraine_conversations` is intentionally outside the root Cargo
workspace. Its empty `[workspace]`, binding-local `Cargo.lock`, and hash-locked Python
test requirements preserve independent PyO3 provisioning. From that directory run:

```bash
python -m venv .venv-test
.venv-test/bin/pip install --require-hashes -r requirements-test.lock
VIRTUAL_ENV="$PWD/.venv-test" .venv-test/bin/maturin develop --locked
.venv-test/bin/pytest -q tests/test_smoke.py
```

This is a `binding` suite tagged `python`, `pyo3`, `http`, and `serialization`. CI pins
Python 3.12 and stable Rust, has a 20-minute job timeout, asserts at least one collected
case, then verifies the JUnit executed count equals collection. The single smoke owns a
loopback fake ClickHouse child on `127.0.0.1:0` and terminates/joins it in `finally`;
the ephemeral runner owns `.venv-test`, Cargo cache/output, and `.pytest-smoke.xml`.
Missing tools, a stale binding lock, zero collection, build failure, or test failure is
fail. The binding/local dependency path filter is its workflow, the binding tree,
`crates/moraine-clickhouse/**`, `crates/moraine-config/**`,
`crates/moraine-conversations/**`, and `sql/**`.

`bindings/python/moraine-cli` is a separate prebuilt-binary packaging wrapper. Its
entry points are exercised by packaging jobs, not by this PyO3 pytest.

## Policy, packaging, and documentation-quality suites

These native checks are active repository surfaces even though they are not product
runtime tests.

| Owner / suite | Class and tags | Exact native command | Prerequisites, resources, cleanup | Tier and result |
| --- | --- | --- | --- | --- |
| `scripts/ci/test_dependency_policy.py` | `unit`; dependency, SQL policy | `python3 -m unittest -v scripts/ci/test_dependency_policy.py` | Python 3; temporary fixture trees are test-owned. No network or persistent output. | T0; any fixture failure or zero selection fails. `make dependency-policy` runs this before enforcement. |
| `scripts/ci/dependency_policy.py` | policy; dependency boundary, SQL ownership | `python3 scripts/ci/dependency_policy.py` (or `make dependency-policy USE_RUSTUP=0`) | Python 3 and locked Cargo metadata. Read-only manifest/source scan. | T0; only `moraine`, `moraine-conversations`, and `moraine-ingest-core` may directly depend on `moraine-clickhouse`; monitor-core production SQL remains forbidden. |
| Release-target consistency | policy; packaging, platform matrix | `python3 scripts/ci/assert-release-targets.py` | Python 3; reads workflow/build mappings only. | T0 on every packaging workflow invocation; exact four-target disagreement fails. |
| Release bundle and Python wrapper | `packaging`; artifact, entry points | `scripts/package-moraine-release.sh <target-triple> dist`, then `python3 scripts/build-python-wheels.py --target <target-triple> --bundle dist/moraine-bundle-<target-triple>.tar.gz --out-dir dist/wheels --version <version>` and `python3 scripts/build-python-sdist.py --out-dir dist/wheels --version <version>` | Stable Rust target, Bun, Python 3.12; Linux manylinux container or native macOS. Owns `dist/`; CI runner/container cleanup. | T1 path-filtered dry run and T2/T3 artifact lanes. Build/missing member/nonzero command fails. |
| Wheel layout/install/entry points | `packaging`; archive, modes, glibc, CLI | `bash scripts/ci/assert-wheel-layout.sh <wheel>`; install the wheel into an owned venv and run `moraine --help`, `moraine-ingest --help`, `moraine-monitor --help`, and `moraine-mcp --help` | Python/zip/readelf where applicable; Debian check owns a `--rm` container and read-only wheel mount. Temporary extraction/venv is check-owned. | T1 packaging paths and T2 artifacts; absent `readelf` skips only the optional glibc inspection, not archive/install/entry-point checks. |
| Stub-sdist refusal | `packaging`; installation failure contract | `bash scripts/ci/assert-sdist-refuses.sh <sdist>` | Python 3; owns/removes temporary venv/log. | T1/T2; pass requires the expected prebuilt-wheel-only refusal, not merely any failure. |
| Documentation build | quality; docs, landing assets | `make docs-build` | `uv`, Zensical, checked-in docs/assets; workflow timeout 10m and owns generated `site/`. | Existing T2 Pages build. Required files, CNAME, and headline checks fail closed; deployment is operational and not a runtime suite. |

## Canonical stack and shell phases

`scripts/ci/e2e-stack.sh` is the single non-interactive source-tree lifecycle owner.
Do not shard assertions against one shared stack. It allocates a temporary root,
dynamic ports, a managed ClickHouse, generated multi-harness inputs, ingest, the
unified backend, monitor assets, MCP clients, and named-backend state. Its ordered phase
inventory is generated at
`docs/development/test-architecture/baseline-77c90d6/e2e-phases.json`.

The T1 gate must retain these observable contracts in order:

1. managed ClickHouse install, monitor asset preparation, named-backend schema, and canonical process/executable topology;
2. health/static UI, all source families, normalized ClickHouse rows, live Cursor SQLite mutation, NAC live-update and same-path database-replacement lifecycles, exact migrations, and representative real read/write decoding;
3. canonical monitor v1 routes/capabilities and one-release aliases;
4. stable `turn_seq` and search-to-open handles in a live duplicate state, with no retry masking;
5. monitor/repository semantic parity, fresh-process cache miss then hit, daemon cache markers, named-backend MCP/HTTP routing, isolation, skew policy, and bounded query ownership;
6. MCP initialize, exact tools/list, representative search/open/list/file-attention contracts for every fixture family and project-scope exclusion;
7. daemon-preferred status, abrupt daemon loss, database/ingest survival, embedded and named fallback, byte-identical daemon/embedded envelopes where promised, and all-down teardown.

Required commands and ownership:

| Suite | Exact command | Prerequisites/resources/cleanup | Tier/result |
| --- | --- | --- | --- |
| Source-tree stack | `bash scripts/ci/e2e-stack.sh` | Linux CI; built debug binaries; Bash, curl, Python 3, `ps`; Bun if monitor assets are stale. EXIT stops only owned processes/sockets/databases. Success removes its temp root unless `KEEP_E2E_TMP=1`; failure identifies the phase, returns nonzero, retains and prints diagnostics/log tails. | T1 on relevant paths; timeout 30m in PR job. |
| Installed artifact | `bash scripts/ci/e2e-install-artifact.sh <target-triple>` | Bash, tar, Python/curl, packaging tools; isolated `HOME` and install root. Delegates to the canonical stack; success cleans wrapper temp unless retained, failure preserves diagnostics. | T2 four-platform matrix; 45m workflow. |
| Raw MCP protocol component | `bash scripts/ci/e2e-stack.sh` | `scripts/ci/mcp_smoke.py` is an internal parameterized component repeatedly provisioned by the stack with owned binary/config/query/working-directory/snapshot paths; it has no supported standalone entry point. Stack ownership supplies lifecycle and cleanup. | T1/T2 within the exact stack command; never substitute an ad hoc component invocation for stack coverage. |
| Paid real-agent smoke | `scripts/dev/sandbox/agent-smoke-e2e` | Docker sandbox, `ANTHROPIC_API_KEY`, Claude CLI/model access. Owns one sandbox and fresh MCP children; default tears down on all catchable exits unless `--keep`/adopted `--id`. | T3 manual/pre-release. Missing secret may be orchestration `not_run`; direct command fails early. |

T1 path globs cover its workflow, root Cargo manifests/lockfile, `apps/**`, `bin/**`,
`config/**`, `crates/**`, `scripts/**`, `sql/**`, and `web/monitor/**`. `web/landing/**`
is documentation/landing ownership, not a stack-test claim. Changes to migrations,
SQL/query/row decoding, stack fixtures, real schema, or support keep the T1 real
ClickHouse phase because an HTTP mock cannot prove analyzer/schema compatibility.

## Fixture and golden ownership

Fixtures are owned by family rather than by every inline JSON value.

| Family / owner | Consumers and contract | Check/update and policy |
| --- | --- | --- |
| `fixtures/{codex,claude-code,nac,opencode,cursor}` plus primary `kiro`, `kimi-cli`, `hermes`, and `pi` inputs / `moraine-ingest-core` | Immutable representative raw inputs consumed by `tests/golden_fixtures.rs`; Cursor transcript and SQLite schema fixtures remain distinct, and Kiro's paired JSON sidecar is also exercised by its source unit test. | `cargo test -p moraine-ingest-core --test golden_fixtures --locked` plus `cargo test -p moraine-ingest-core --lib --locked sources::kiro_cli::tests::paired_fixture_loads_sidecar_and_normalizes_with_session_hints -- --exact`; pre-existing fixture checksums are frozen in `baseline-77c90d6/fixture-checksums.tsv`; mechanical moves preserve paths and bytes. |
| `fixtures/qwen-code/session.jsonl` / Qwen Code adapter | Sanitized Qwen 0.19.x `ChatRecord` compatibility input consumed by the focused adapter test and normalization golden suite. | `cargo test -p moraine-ingest-core --test qwen_code_fixture --locked` plus the `golden_fixtures` command above; review intentional raw-input and `qwen-code.json` golden changes together. |
| `fixtures/kimi-cli/project-273/**` / Kimi discovery and dispatch | Parent/subagent discovery, attribution, deduplication, and dispatch contracts; the subagent input also participates in the normalization golden suite. | `cargo test -p moraine-ingest-core --test kimi_cli_fixture --locked` plus the `golden_fixtures` command above. |
| `fixtures/hermes/{trajectories,sessions}/**` / Hermes parsers | The trajectory and session-JSON shapes remain separate parser/discovery contracts and both participate in normalization goldens. | `cargo test -p moraine-ingest-core --test hermes_fixture --locked`; `cargo test -p moraine-ingest-core --test hermes_session_fixture --locked`; plus the `golden_fixtures` command above. |
| `fixtures/pi/malformed.jsonl` / Pi parser | Malformed-record recovery input, separate from the valid Pi normalization fixture. | `cargo test -p moraine-ingest-core --lib --locked dispatch::tests::process_file_reports_pi_malformed_jsonl_without_dropping_valid_rows -- --exact`; checksums are frozen with the other committed fixture inputs. |
| `crates/moraine-ingest-core/tests/goldens/source_normalization/*.json` / `moraine-ingest-core` | Thirteen full-shape normalized outputs complement focused assertions; only intentionally unstable fields are normalized by the test. | Check with `cargo test -p moraine-ingest-core --test golden_fixtures --locked`. |
| `apps/moraine/src/commands/analytics_schema_v1.golden.json` / `moraine` CLI | Exact JSON schema bytes for the analytics CLI contract. | `cargo test -p moraine --bin moraine --locked commands::schema::tests::analytics_schema_json_matches_golden -- --exact`; baseline checksum is in the generated fixture artifacts. Mechanical moves preserve its bytes. |
| Scenario-stamped payloads synthesized by `scripts/ci/e2e-stack.sh` / stack owner | Canonical wire shapes plus unique semantic markers and lifecycle mutations. They stay separate from raw/golden bodies so search/open ambiguity and mutation transitions remain observable. | Generated/owned in the stack temp root; cleanup follows the stack. Do not consolidate fixture bodies during a move. |
| Test-local mock responses and inline constructors / owning package or suite | Minimal private or public-contract cases tied to one harness. | Keep local until two packages prove identical primitive semantics; no fixture DSL is implied. |
| `scripts/bench/tests/fixtures/` / performance protocol | Valid/invalid `moraine-performance-v1` and `moraine-performance-suite-v1` documents, including mutation and policy boundaries. | `python3 -m unittest discover -v -s scripts/bench/tests -p 'test_*.py'`. Contains no credentials, user content, or host identities. |

The only intentional ingest-golden update is:

```bash
env -u CI MORAINE_UPDATE_INGEST_GOLDENS=1 \
  cargo test -p moraine-ingest-core --test golden_fixtures --locked \
  source_normalization_golden_fixtures_are_stable -- --exact --nocapture
```

Any other present value fails. Any present `CI` refuses update mode before writing.
Updates use same-directory temporary files plus atomic rename, print each changed path,
leave unchanged files untouched, and remove catchable temporary files. Review changed
bytes, then rerun the ordinary check without update mode. Fixture consolidation,
generation changes, or expectation changes are separate contract changes, never part
of a mechanical move. See [Ingest sources](ingest-sources.md#fixtures-and-contracts)
and [Harness author workflow](harness-author-workflow.md#6-add-fixtures).

## Fixed-resource search performance suite

`scripts/bench/performance_suite.py` is the only end-to-end benchmark interface.
The protocol, runtime, fixture, and scenario modules under `scripts/bench` are
import-only helpers. The canonical scenarios are `qps`, `ttr`, `etd_idle`,
`etd_loaded`, and `mixed`; there are no independent producer CLIs.

The scenario, comparison, and repeatability schema is
`scripts/bench/schema/moraine-performance-v1.schema.json`. Root suite manifests use
`scripts/bench/schema/moraine-performance-suite-v1.schema.json`. The CLI dispatches
documents explicitly and rejects unknown document types.

```bash
# Freeze deterministic fixture and policy artifacts without starting services.
python3 scripts/bench/performance_suite.py freeze \
  --profile full --output target/bench/performance/frozen

# One non-comparable owned smoke.
python3 scripts/bench/performance_suite.py smoke \
  --repo . --output target/bench/performance/smoke

# First prove baseline repeatability in seven fresh owned sandboxes.
python3 scripts/bench/performance_suite.py run \
  --baseline <baseline-worktree> \
  --output target/bench/performance/baseline-study

# Then run seven counterbalanced AB/BA pairs. The immutable baseline study
# selects the fixed 75%-capacity loaded schedules and must match this build.
python3 scripts/bench/performance_suite.py run \
  --baseline <baseline-worktree> \
  --candidate <candidate-worktree> \
  --baseline-manifests \
    target/bench/performance/baseline-study/baseline-{01,02,03,04,05,06,07}/manifest.json \
  --output target/bench/performance/full

# Run a non-authoritative paired diagnostic on Docker Desktop or a dev host.
python3 scripts/bench/performance_suite.py run \
  --mode local --profile smoke --pairs 1 \
  --baseline <baseline-worktree> --candidate <candidate-worktree> \
  --output target/bench/performance/local

# Emit OMP-compatible METRIC lines from a QPS artifact or local comparison.
python3 scripts/bench/performance_suite.py autoresearch-metrics \
  target/bench/performance/local/local-comparison.json

# Validate one or more checked-in or produced documents.
python3 scripts/bench/performance_suite.py validate <document.json> [...]

# Compare two complete validated root manifests.
python3 scripts/bench/performance_suite.py compare \
  <baseline-manifest.json> <candidate-manifest.json> \
  --output target/bench/performance/comparison.json

# Check exactly seven complete baseline manifests from distinct resets.
python3 scripts/bench/performance_suite.py repeatability \
  <baseline-1.json> <baseline-2.json> <baseline-3.json> \
  <baseline-4.json> <baseline-5.json> <baseline-6.json> <baseline-7.json> \
  --output target/bench/performance/repeatability.json

# On native arm64 macOS, gate the central MCP search path against a separately
# owned, migrated, full-profile fixture database. Never point this at ~/.moraine.
python3 scripts/bench/performance_suite.py native-central-burst \
  --mcp-binary ./target/release/moraine-mcp \
  --config /tmp/moraine-native-fixture/config.toml \
  --profile full --split research \
  --cold-repetitions 100 --minimum-cold-samples 100 \
  --bursts-per-case 25 \
  --warm-p95-limit-ms 750 --cold-p95-limit-ms 2000 \
  --max-latency-ms 5000 \
  --collect-query-log \
  --output target/bench/performance/native-central-burst.json

# Candidate-only real-ingest check for issue #602. The first event publishes
# generation 1; the next 100 records append to that same inode/generation.
python3 scripts/bench/performance_suite.py source-publication-append-probe \
  --mode local --repo <clean-candidate-worktree> --samples 100 \
  --p95-limit-ms 2000 \
  --output target/bench/performance/source-publication-append

# Candidate-only replacement replay/resource capture for issue #602. The
# default fixture replaces one one-event generation with 2,500 events in one
# session so production ingest must flush at least two replay batches.
python3 scripts/bench/performance_suite.py source-publication-replay-probe \
  --mode local --repo <clean-candidate-worktree> --events 2500 \
  --timeout-seconds 180 \
  --output target/bench/performance/source-publication-replay
```

The native central burst is a T3 manual native regression probe, not a substitute
for the fixed-resource Linux comparison and not merge/release evidence. Its
`cold_lifecycle` phase restarts the central daemon before every first query
(startup remains outside the timing boundary), while `steady_state` warms each
selected query before synchronized
C1/C4/C8 bursts. The defaults require at least 100 successful cold samples per
concurrency, require every warm-mode p95 to be at most 750 ms, require cold
high-hydration/common and session-scope p95 to be at most two seconds, and cap
every raw sample at five seconds. The command rejects live/default or routed
backends and verifies exact fixture/index/projection state before and after the
run. `--collect-query-log` additionally classifies publication-head capture,
append-fence capture, candidate, detail, publication revalidation, and append-fence
revalidation separately. Every cold request must execute exactly one of each. A
steady warmup executes all six; a measured cache hit executes the four control
statements and no candidate/detail statement. Counts scale exactly with C1/C4/C8
and the declared burst count; any unknown owned-family statement fails the inventory. See
`scripts/bench/README-native-central-burst.md` for fixture ownership, ClickHouse
permissions, artifact fields, and cleanup details.

The append probe is a T3 manual production-path diagnostic. It uses a clean,
pinned worktree, builds frozen binaries, owns a fresh performance sandbox, and
publishes one shared JSONL file with file fsync (plus directory fsync on create).
It measures publication capture first with one seeded head, runs the append
stream on the resulting two-source fixture, then expands the head table
insert-only to exactly 10,000 and 100,000 logical heads. At each reported scale
point it runs two fixed warmups and ten measured copies of the exact local-mode
capture statement (`max(publication_revision)` over raw head history). Separate
untimed logical-head and storage checks prove the reported scale identity. The
artifact retains client and server latency,
`read_rows`, `read_bytes`, result rows, peak query memory, and control-table rows,
active parts, and compressed bytes for every scale point. It then reports every
raw fsync-to-first-valid-live-query sample, p50/p95/max,
process CPU and peak memory evidence, and before/after control-table storage. It
also reports before/after compressed, uncompressed, and on-disk bytes for the
new `source_host` column across all seven physical event/index tables (including
zero-filled empty tables). It fails unless at least 100 measured appends complete,
p95 is at most two seconds, the publication revision is unchanged, and source-head
history receives zero append writes. Local mode is explicitly non-authoritative; use
`--mode authoritative` only on the dedicated cgroup-v2 benchmark host.
The run-level CPU and memory counters cover both bounded capture-scaling and
append phases; they are not presented as append-only process costs.

The replay probe is the dedicated replacement/resource capture for issue #602.
It builds the pinned candidate, owns a fresh performance sandbox, publishes a
one-event generation, then atomically renames a file-fsynced 2,500-event
single-session replacement over the same path. Its wall-time boundary starts
immediately before the durable rename and ends only after generation 2 and its
legacy MCP compatibility row are visible. Content-free ingest ACK observations
count actual replay flush batches. ClickHouse query-log `QueryFinish` rows for
`mcp_open_sessions` classify the one candidate activation reconciliation and
reject any additional per-chunk refresh. The artifact is
`source-publication-replay-probe.json` and records raw batch sequences, affected
and prepared session counts, exact generation/checkpoint/readiness states, and
before/after/delta rows, active parts, compressed bytes, and bytes on disk for
the physical data, publication-control, and MCP compatibility tables. Net
`bytes_on_disk` growth is reported per retained inactive generation; those rows
are persistent and reclamation remains #603.

Process CPU is the `moraine-ingest` `/proc/<pid>/stat` delta. Process disk I/O is
the `/proc/<pid>/io` delta when readable. Replay-window peak RSS is reported only
when the process lifetime `VmHWM` advances during the bounded replacement; if it
does not, the measured equal before/after high-water bound is retained and the
peak is explicitly unavailable. In authoritative mode the probe additionally
resets and captures the owned cgroup-v2 CPU, peak-memory, throttling, and
`io.stat` window. Local mode preserves the same topology but labels cgroup
throttling and block-I/O unavailable rather than treating sentinel zeroes as
measurements. The generation wait uses a fixed 50 ms publication-state poll, so
the artifact records that observer interval alongside the timeout.

### Atomic source-publication PR report

An issue #602 PR records the following in its description; omitted or unavailable
measurements are labeled as such rather than inferred:

- pinned baseline/candidate SHAs, fixture cardinality, host/container limits,
  warmup, repetitions, and exact commands;
- paired local or authoritative values/deltas for list/open/search latency and
  ClickHouse `read_rows`, `read_bytes`, and `memory_usage`;
- append-probe p50/p95 and head-write invariants from
  `source-publication-append-probe.json`;
- replacement replay wall time, process CPU, peak RSS, throttling/disk bytes,
  replay-batch count, and compatibility refresh count (one activation refresh,
  none per chunk) from `source-publication-replay-probe.json`; the acceptance
  fixture supplies migration/control-storage evidence, not process metrics;
- migration/backfill duration and control-table rows, active parts, and
  compressed bytes for source-head history, causal checkpoints, append control,
  generation readiness, and MCP compatibility headers;
- persistent bytes per retained inactive replacement generation. Those rows are
  persistent overhead; reclamation belongs to #603 and must not be called temporary.

Generated search fixtures seed an active causal checkpoint, generation readiness,
and the publication head after canonical/search rows. Older baseline binaries
without the publication schema keep the legacy one-insert seed path, which permits
a pinned pre-#602 baseline to run in the same paired harness.

Local mode uses the same scenarios, fresh physical sandboxes, semantic oracles, and
frozen binaries, but reports `authoritative: false` because it observes rather than
owns the Docker host resource envelope. It never converts scheduler failures or
right-censored ETD into passing evidence.

`./autoresearch.sh` is the optimization-loop adapter. Its default `inner` mode runs
the ignored in-process cached-posting ranker benchmark inside one dev sandbox and
emits low-noise `METRIC` lines while allowing a dirty candidate tree. This is a fast
proxy, not end-to-end evidence. Retained changes must be checked against the suite.
For a clean retained commit, `MORAINE_AUTORESEARCH_MODE=e2e` runs one local paired
suite and translates its validated candidate QPS, TTR, and uncensored loaded-ETD
evidence into the same metric protocol:
The adapter reloads each referenced candidate artifact; copied summary values,
censored capacity, failed gates, or fixture-oracle fingerprint mismatches fail
closed instead of producing optimization metrics.

```bash
MORAINE_AUTORESEARCH_MODE=e2e \
MORAINE_AUTORESEARCH_BASELINE=<clean-baseline-worktree> \
MORAINE_AUTORESEARCH_OUTPUT=target/bench/performance/autoresearch-local \
./autoresearch.sh
```

The output directory must not already exist. E2E autoresearch always uses the
`full` profile so QPS is bracketed before the metric bridge accepts it;
`MORAINE_AUTORESEARCH_PAIRS` defaults to `1`. Neither local mode nor the inner-loop
proxy can satisfy the authoritative merge-evidence contract.

Authoritative runs require a dedicated rootful Linux cgroup-v2 Docker host with
strictly more than 8 GiB of host memory. They refuse Docker Desktop, rootless
Docker, missing controller delegation, and non-cgroup-v2 drivers before collecting
results. ClickHouse, central Moraine, and ingest/indexing workers share one aggregate
1-CPU/8-GiB/no-swap server envelope; load generation and result collection remain
outside it. A synthetic busy-child proof verifies that aggregate CPU accounting
includes descendants. OOM, swap activity, misplaced processes, binary drift, or an
unremovable owned resource fails the run.

The suite prebuilds one shared runtime image before any physical reset and pins
that image identity for both arms; reset startup never rebuilds it inside a
measured lifecycle.

Each source worktree is clean and pinned to a full commit. The suite builds its four
release binaries once, copies them into a read-only immutable directory, and records
the compiler, target, linker, allowlisted build environment, manifest, and binary
digests. Every measured sandbox mounts only those frozen binaries. The benchmark
adapter and Compose overlay always come from the suite checkout—not from the
baseline worktree—so an older baseline needs no benchmark-specific files. Before
measurement, the arm's frozen `moraine` binary migrates its fresh database, the
fixture seeds normalized `events`, and the production materialized view derives
`search_documents`. Runtime evidence binds the exact image IDs and hashes each live
`/proc/<pid>/exe` for `moraine-ingest`, `moraine-mcp`, and load-generator processes.

Every physical arm receives a new sandbox project, delegated cgroup, ClickHouse
volume, source root, and cache generation. The sandbox wrapper remains the sole
Docker lifecycle and ownership authority; the suite never truncates or drops a
pre-existing database. Setup, fixture generation, build, migration, seed, and
readiness stay outside measured boundaries. Cleanup attempts every owned resource
and propagates any uncertainty as a failed run rather than touching resources whose
ownership is not proven.

QPS uses an open arrival schedule and counts only semantically correct completions
that satisfy the frozen latency, scheduling, error, and drain gates. TTR starts
immediately before a fresh central process is spawned and ends only at the first
independent-oracle-valid result through the proved central route. ETD publishes
durably staged source events and polls with one-use terms so negative and posting
caches cannot hide visibility. Loaded ETD runs at 75% of baseline sustainable QPS.
Mixed runs compare simultaneous query and ingest streams against fresh query-only
and ingest-only controls and require overlap, exact delivery, semantic validity, and
complete drain.

Full comparison uses seven physically reset pairs in the fixed order
`AB, BA, AB, BA, AB, BA, AB`. It retains every arm and applies the frozen correctness,
resource, constituent, variability, direction-agreement, and paired-bootstrap
policies. Baseline repeatability accepts exactly seven complete manifests from
distinct resets and performs no outlier deletion. Raw results stay under
`target/bench/performance/`; repository baseline snapshots are not generated or
updated by this suite.

The complete deterministic Python lane is:

```bash
python3 -m unittest discover -v -s scripts/bench/tests -p 'test_*.py'
```

CI counts the discovered tests first and fails if the count is zero. Production
analytics schema and monitor/repository parity remain live semantic checks, not
benchmarks:

```bash
scripts/dev/sandbox/run-live-test analytics-schema
scripts/dev/sandbox/run-live-test analytics-parity
scripts/dev/sandbox/run-live-test source-publication
scripts/dev/sandbox/run-live-test mcp-backfill
```

## CI ownership and promotion

| Workflow / job owner | Trigger, timeout, and responsibility |
| --- | --- |
| `.github/workflows/ci-pr-fast.yml` / `pr-fast-linux` | Every PR/manual, Ubuntu, 30m. Unconditional Rust/policy T0; path-filtered canonical source-stack T1 using the explicit stack globs above. |
| `.github/workflows/ci-monitor-frontend.yml` / `monitor-frontend-bun` | PR paths/manual, Ubuntu, 10m. Bun 1.3.9 frozen install, typecheck, and nonzero Vitest. |
| `.github/workflows/ci-python-binding.yml` / `python-binding-smoke` | Binding/dependency/SQL PR paths/manual, Ubuntu, 20m. Python 3.12 locked venv, `maturin develop --locked`, nonzero pytest/JUnit. |
| `.github/workflows/ci-monitor-browser-mocked.yml` / `monitor-browser-mocked` | Visible PR/manual job, Ubuntu 24.04, 15m. Path-filtered pinned Chromium, exactly two mocked cases, no retry; initially non-required. |
| `.github/workflows/ci-test-architecture.yml` / `architecture-t0` | Benchmark/autoresearch/live-wrapper/support/manifests PR paths/manual, Ubuntu, 45m. Python 3.12 nonzero protocol/adapter/wrapper unittest discovery, autoresearch shell syntax, deterministic live ClickHouse support, and path-filtered metadata-derived package-isolation compilation. |
| `.github/workflows/ci-packaging.yml` / `packaging-dryrun` | Every PR/manual, Ubuntu, 30m. Always asserts four-target consistency; matching paths own bundle/wheel/sdist, host/Debian install, layout/mode/glibc, entry points, and retained seven-day artifact. |
| `.github/workflows/ci-functional.yml` / `functional-<target>` | Main/manual, 45m, four Linux/macOS targets with fail-fast disabled. Workspace checks plus installed-artifact stack T2. |
| `.github/workflows/docs-deploy.yml` | Main/manual, Ubuntu, 10m. Build/required-asset validation, then Pages deployment; documentation-quality rather than runtime coverage. |
| `.github/workflows/release-moraine.yml` | Tag/manual four-target bundle and conditional TestPyPI/PyPI publication. Operational only; consumes exact-commit T0/T1/T2 evidence rather than replacing it. |

| Tier | Owned checks and policy |
| --- | --- |
| T0 — deterministic PR | Existing dependency policy/fmt/strict clippy/locked build/workspace tests; path-filtered frontend typecheck+Vitest; path-filtered locked Python binding smoke; benchmark schema/CLI/unit checks and Rust bench `--no-run`; metadata-derived package isolation when manifests/dev-dependencies/features/support change. Selected jobs fail on zero execution. |
| T1 — relevant-path Linux functional | One source-tree stack and raw MCP smoke for the explicit globs above. The lifecycle is not sharded. Failure identifies its phase, cleans only owned resources where catchable, retains diagnostics, and exits nonzero. |
| T2 — post-merge platform/artifact | Locked workspace checks plus installed-artifact stack and packaging on `x86_64-unknown-linux-gnu`, `aarch64-unknown-linux-gnu`, `aarch64-apple-darwin`, and `x86_64-apple-darwin`. Windows is unsupported. Platform-specific collection is performed on the selected host; Linux is not macOS proof. |
| T3 — scheduled/manual live | Exact live ClickHouse schema, monitor/repository parity when not covered by T1, live Playwright, and paid real-agent smoke. Orchestration may report missing optional secrets as `not_run`; direct commands fail closed. |
| T4 — benchmark | Hermetic validation/compilation on relevant PRs, reduced live smoke only where the boundary needs it, and reproducible full manual artifact runs. No latency merge/release gate or schedule exists without controlled infrastructure, baseline store, owner, and complete comparison policy. |

The frontend workflow is visible and deterministic. Mocked Playwright is separately
visible and initially non-required, with one worker and no CI retry. New jobs become
required only in a dedicated CI-policy change after recording prerequisites, positive
path filters, expected execution count, time-to-signal, and flake evidence. Branch
protection and live/benchmark scheduling never ride inside a move or new-workflow PR.

Packaging PR ownership always checks the four release targets and, on matching paths,
builds bundle/wheel/sdist, checks wheel layout/modes/glibc, installs and executes four
entry points on the host and Debian Bookworm, and proves the stub sdist refuses a local
build. Outputs under `dist/wheels/*` are retained by the dry-run workflow for seven
days. Documentation quality (`make docs-build` plus required site assets/content) is an
existing 10-minute Pages build tier, not a runtime suite. Release publication consumes
already-validated artifacts and is operationally outside this architecture; a release
job does not substitute for exact-commit T0/T1/T2 evidence.

### Release safety

A release containing test-architecture changes is eligible only when the exact release
commit (not an ancestor) has green T0 checks, every path-selected T1 real-stack
contract, and all four T2 installed-artifact/packaging targets. Each move retains its
generated executable mapping, fixed-output comparison, focused result, workspace
result, and unchanged dependency-policy evidence. The umbrella closes with a final
equivalence report covering Cargo targets/tests, fixture/golden checksums, normal
dependency graph, exported production API, named stack contracts, and links to exact
T0/T1/T2 runs. Benchmark timing cannot override failed correctness or artifact gates.
An intentional production API/dependency/config/schema/service/package change belongs
to a separate product change with its own release evidence.

## Excluded and compatibility surfaces

| Surface | Status, owner, and command |
| --- | --- |
| `rust/ingestor` | Legacy standalone `moraine-ingestor`; not a root member. Manual compatibility command: `cargo test --manifest-path rust/ingestor/Cargo.toml --locked`. Rust/Cargo only; local fixture/temp ownership; nonzero test/assertion or compile failure returns nonzero. Do not add it to the root workspace here. |
| `rust/codex-mcp` | Legacy standalone `codex-mcp`; not root coverage. Manual compatibility command: `cargo test --manifest-path rust/codex-mcp/Cargo.toml --locked`. Rust/Cargo and host-local test resources; command/compile/assertion failure is fail. If Cargo reports zero executable cases, record it as compile-only compatibility evidence, not a test pass. |
| `moraine-monitor/backend` | Legacy standalone backend whose package name collides with current `apps/moraine-monitor`. Manual compatibility command: `cargo test --manifest-path moraine-monitor/backend/Cargo.toml --locked`. Rust/Cargo; own manifest/lock and test-local resources. Zero cases are compile-only evidence. Root inventory selects exact `workspace_members`, never filesystem manifest scanning. |
| `bindings/python/moraine_conversations` | Active, but deliberately standalone and owned by the provisioned binding job above; never claim root Cargo covers it. |
| `web/landing` and documentation build | Owned by documentation/Pages quality, not monitor runtime tests or stack path selection. |
| Windows | Unsupported; no Linux/macOS result implies Windows support. |
| Release publication | Operational workflow consuming validated artifacts; not a test suite and never correctness evidence by itself. |

## Frozen migration baseline

The generated migration evidence under
`docs/development/test-architecture/baseline-77c90d6/` is from verified merge commit
`77c90d6c572ba71bcfaa0362f7ead2cfa5e6712a`, tree
`0ae5df87300c6748161f2035394b6e0b9a92e627` (merged PR #476). It records:

- normalized Cargo workspace packages, target kinds/paths, and harness flags;
- metadata-derived per-package isolation commands;
- the static Rust attribute inventory (717 attributes: 589 source-local, 128 in ten integration files), the two exact ignored functions, and PR #476 runtime evidence (720 passed, two ignored across 32 reported suites);
- metadata-derived Vitest, Playwright, and pytest file/case inventory, including expanded PR evidence where declaration counts differ from runtime cases;
- native Linux arm64 and macOS arm64 libtest list output with package, target, fully qualified name, and ignored state;
- raw fixture and golden per-file SHA-256/byte counts plus path-and-byte family digests;
- ordered shell E2E phase labels; and
- the five benchmark producers present in that historical tree and their distinct lifecycle scenarios.

Regenerate with the issue-477 generator outside an unmodified source archive/worktree
of that exact commit; output must also remain outside the authenticated source tree:

```bash
python3 <issue-477-checkout>/docs/development/test-architecture/generate_baseline.py \
  --repo-root <exact-77c90d6-source> \
  --source-commit 77c90d6c572ba71bcfaa0362f7ead2cfa5e6712a \
  --node-bin-dir <baseline-node_modules/.bin-containing-vitest-and-playwright> \
  --pytest-executable <python-environment>/pytest \
  --output <empty-output-directory>
```

The generator recomputes the Git tree object without requiring a `.git` directory and
refuses any tree other than `0ae5df87300c6748161f2035394b6e0b9a92e627`; the commit
argument alone cannot relabel another checkout. Default generation uses locked Cargo
metadata and source/fixture inspection. Append `--collect-libtest` separately on clean
Linux and macOS sources to compile harnesses and invoke only `--list`/`--ignored` list
modes; no test body executes. Artifact names include OS and architecture, and platform
lists are never inferred from another host.

These artifacts are migration evidence, not a permanent hand-maintained catalog of
ordinary test functions. A target move compares generated before/after collections and
provides a bijection for every affected executable test; changed fully qualified module
paths are explicit, leaf names/cfg/ignore state remain stable, and N:1 suite grouping
never substitutes for executable mapping. Pure moves preserve fixture hashes and fixed
CLI exit/stdout/stderr, HTTP status/body, promised MCP/schema bytes, normalization
goldens, exact migration filenames, and stable duplicate-state search/open handles.
When topology changes, report target count and reproducible same-host clean/warm compile
plus linked-test-executable observations; there is no invented global percentage gate.

A production defect found during migration is fixed in a separate behavior change. Test
moves do not weaken expectations, alter production API/features/dependencies/config,
or silently change release artifacts.
