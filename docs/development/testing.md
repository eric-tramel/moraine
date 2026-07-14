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
| Live ClickHouse schema/parity | `scripts/dev/sandbox/run-live-test analytics-schema` or `scripts/dev/sandbox/run-live-test analytics-parity` | Fresh owned sandbox and exact ignored test. |
| Benchmark protocol | `python3 scripts/bench/benchmark_protocol.py validate <artifact.json>` | Validates one `moraine-benchmark-v1` result. |

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
| `moraine-conversations/analytics_benchmark_support` | `integration`; benchmark serialization, digest, percentile, cardinality, profile, comparison | `cargo test -p moraine-conversations --test analytics_benchmark_support --locked` | Rust/Cargo; deterministic shared bench support only, no live endpoint. | T0. |
| `moraine-conversations/live_clickhouse` deterministic support | `integration`; destructive guards, ownership, cleanup composition, corpus oracle | `cargo test -p moraine-conversations --test live_clickhouse --locked` | Rust/Cargo; deterministic tests use no live endpoint. The three ignored functions are separately owned below and remain unexecuted by this command. | T0; zero/failure is fail. |
| `moraine-ingest-core/golden_fixtures` | `integration`; golden, normalization, schema | `cargo test -p moraine-ingest-core --test golden_fixtures --locked` | Rust/Cargo and committed raw/golden families; read-only unless the explicit update mode below is used. | T0; byte drift fails. |
| `moraine-ingest-core/hermes_fixture` | `integration`; ingest, serialization | `cargo test -p moraine-ingest-core --test hermes_fixture --locked` | Committed Hermes trajectory; no external service. | T0. |
| `moraine-ingest-core/hermes_session_fixture` | `integration`; ingest, serialization | `cargo test -p moraine-ingest-core --test hermes_session_fixture --locked` | Committed Hermes session JSON; no external service. | T0. |
| `moraine-ingest-core/kimi_cli_fixture` | `integration`; ingest, parent/subagent links | `cargo test -p moraine-ingest-core --test kimi_cli_fixture --locked` | Committed Kimi fixtures; no external service. | T0. |
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
| `scripts/bench/tests/test_benchmark_protocol.py` | `unit`; JSON Schema, invariants, redaction, comparison, atomic artifacts | `python3 -m unittest discover -s scripts/bench/tests -p 'test_benchmark_protocol.py' -v` | Python 3 standard library; test-owned temporary files only, no network or service. The architecture workflow owns a 45m job deadline. | Path-filtered T0. Zero tests, invalid fixture behavior, or any assertion failure returns nonzero. |

## Live ClickHouse suites

Only these three ignored tests are supported. Never use a blanket `--ignored` command.
The wrapper is the contributor entry point where listed; the raw commands document exact routing
and intentionally fail unless `MORAINE_ALLOW_DESTRUCTIVE_TESTS=1` and the endpoint is
the wrapper-owned sandbox.

| Owner / full function | Exact wrapper and raw Cargo command | Prerequisites, timeout, resources, cleanup | Tier and semantics |
| --- | --- | --- | --- |
| `moraine-conversations/live_clickhouse::live_schema_semantics_and_teardown` | `scripts/dev/sandbox/run-live-test analytics-schema`; raw: `cargo test -p moraine-conversations --test live_clickhouse --locked live_schema_semantics_and_teardown -- --exact --ignored --nocapture` | Bash, Docker/Compose, sandbox toolchain. Default wrapper timeout 1,800s (`MORAINE_LIVE_TEST_TIMEOUT_SECONDS` accepts a positive integer). Wrapper owns a fresh `sb-xxxxxx` sandbox and Rust generates an uncaller-controlled `moraine_test_<uuid>` database. Empty, `moraine`, or non-prefix names are refused before SQL. | T3 manual/scheduled. Direct missing/unsafe prerequisites fail. Success and every catchable failure with successful cleanup leave no owned sandbox/database. |
| `moraine-conversations/live_clickhouse::live_monitor_repository_semantic_parity` | `scripts/dev/sandbox/run-live-test analytics-parity`; raw: `cargo test -p moraine-conversations --test live_clickhouse --locked live_monitor_repository_semantic_parity -- --exact --ignored --nocapture` | Same owned sandbox; both arms use the same generated database/dataset. Cardinality, digest, or oracle mismatch fails independently of timing. | T3 unless the same semantics are already proven in T1. Timing is not the pass condition. |
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
2. health/static UI, all source families, normalized ClickHouse rows, live Cursor SQLite mutation, exact migrations, and representative real read/write decoding;
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
| `fixtures/{codex,claude-code,opencode,cursor}` plus primary `kimi-cli`, `hermes`, and `pi` inputs / `moraine-ingest-core` | Immutable representative raw inputs consumed by `tests/golden_fixtures.rs`; Cursor transcript and SQLite wire export remain distinct. | `cargo test -p moraine-ingest-core --test golden_fixtures --locked`; checksums are frozen in `baseline-77c90d6/fixture-checksums.tsv`; mechanical moves preserve paths and bytes. |
| `fixtures/kimi-cli/project-273/**` / Kimi discovery and dispatch | Parent/subagent discovery, attribution, deduplication, and dispatch contracts; the subagent input also participates in the normalization golden suite. | `cargo test -p moraine-ingest-core --test kimi_cli_fixture --locked` plus the `golden_fixtures` command above. |
| `fixtures/hermes/{trajectories,sessions}/**` / Hermes parsers | The trajectory and session-JSON shapes remain separate parser/discovery contracts and both participate in normalization goldens. | `cargo test -p moraine-ingest-core --test hermes_fixture --locked`; `cargo test -p moraine-ingest-core --test hermes_session_fixture --locked`; plus the `golden_fixtures` command above. |
| `fixtures/pi/malformed.jsonl` / Pi parser | Malformed-record recovery input, separate from the valid Pi normalization fixture. | `cargo test -p moraine-ingest-core --lib --locked dispatch::tests::process_file_reports_pi_malformed_jsonl_without_dropping_valid_rows -- --exact`; checksums are frozen with the other committed fixture inputs. |
| `crates/moraine-ingest-core/tests/goldens/source_normalization/*.json` / `moraine-ingest-core` | Ten full-shape normalized outputs complement focused assertions; only intentionally unstable fields are normalized by the test. | Check with `cargo test -p moraine-ingest-core --test golden_fixtures --locked`. |
| `apps/moraine/src/commands/analytics_schema_v1.golden.json` / `moraine` CLI | Exact JSON schema bytes for the analytics CLI contract. | `cargo test -p moraine --bin moraine --locked commands::schema::tests::analytics_schema_json_matches_golden -- --exact`; baseline checksum is in the generated fixture artifacts. Mechanical moves preserve its bytes. |
| Scenario-stamped payloads synthesized by `scripts/ci/e2e-stack.sh` / stack owner | Canonical wire shapes plus unique semantic markers and lifecycle mutations. They stay separate from raw/golden bodies so search/open ambiguity and mutation transitions remain observable. | Generated/owned in the stack temp root; cleanup follows the stack. Do not consolidate fixture bodies during a move. |
| Test-local mock responses and inline constructors / owning package or suite | Minimal private or public-contract cases tied to one harness. | Keep local until two packages prove identical primitive semantics; no fixture DSL is implied. |
| `scripts/bench/tests/fixtures/` / benchmark protocol | Valid/invalid v1 envelopes, invariant, redaction, and comparability cases. | `python3 -m unittest discover -s scripts/bench/tests -p 'test_benchmark_protocol.py' -v`. Contains no credentials or user content. |

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

## Benchmark protocol and registry

All six existing producers emit one `moraine-benchmark-v1` result object per output
file. Rust owns cross-process analytics latency; `scripts/bench` owns process/database/
system measurement. Setup, seeding, build, startup, and warmup stay outside timed
samples. Smoke records latency but never gates on it.

### Normative artifact

The schema is `scripts/bench/schema/moraine-benchmark-v1.schema.json`; the standard-
library validator/comparator is `scripts/bench/benchmark_protocol.py`.

```bash
python3 scripts/bench/benchmark_protocol.py validate <artifact.json>
python3 scripts/bench/benchmark_protocol.py compare \
  <baseline.json> <candidate.json> [--measurement latency_ms] [--output <comparison.json>]
```

The envelope records `schema_version`, benchmark/scenario IDs,
`source.{git_commit,dirty}`, build profile/target, non-identifying OS/CPU runner class,
workload/profile/measured boundary, applicable fingerprints, planned/attempted/
successful/error counts, unit-suffixed successful raw measurements, semantic status,
non-blocking timing status, safe diagnostics, and artifact references. Request-producing
load scenarios may also retain redaction-safe per-case records and arbitrary-cardinality
observation series. Dataset-backed, cache-sensitive, concurrent, and request-producing
scenarios conditionally require dataset digest/cardinality, cache state, concurrency,
and request source.

Validation enforces `attempted = successful + errors`, `attempted <= planned`, each raw
measurement series length equals `successful`, outcome totals equal `attempted`, and
burst records agree with both. Percentiles use successes while error and rejection
rates stay explicit. Producers omit meaningless null/sentinel fields. Artifacts must
not contain credentials, credentialized URLs, raw query/conversation/prompt/content,
absolute home paths, or user/host identity. Large raw samples may be externalized only
with count and checksum retained.

A comparison rejects different schema versions, workload/dataset, measured boundary,
cache state, request source, concurrency, build profile, or any applicable fingerprint.
Smoke/full, warm/cold, embedded/central, and server/end-to-end runs are not equivalents.
`timing.status` remains `not_evaluated` unless statistic, minimum independent run pairs,
threshold, error-rate policy, and variability bound are complete. Comparable timing
may be `pass`, `fail`, or `inconclusive`, but `non_blocking` is always true in this
program: semantic/oracle failure is correctness failure; latency never blocks merge or
release. Build baseline/candidate in the same profile, run on the same host and dataset,
alternate order, and retain raw artifacts. Comparator timing failure reports the result
without a correctness exit failure; invalid/incomparable artifacts return nonzero.

Deterministic protocol validation (no network, mutable resources, or cleanup) is:

```bash
python3 -m unittest discover -s scripts/bench/tests \
  -p 'test_benchmark_protocol.py' -v
```

### Producer registry

| Owner / scenario | Native smoke/full command and deterministic check | Prerequisites, boundary, timeout, cleanup | Tier/result |
| --- | --- | --- | --- |
| `moraine-conversations/analytics_latency` | Compile: `cargo bench -p moraine-conversations --bench analytics_latency --locked --no-run`. Smoke: `MORAINE_BENCH_MONITOR_URL=<owned-monitor-url> MORAINE_BENCH_CLICKHOUSE_URL=<owned-clickhouse-url> MORAINE_BENCH_DATASET_MANIFEST=<owned-dataset-manifest.json> cargo bench -p moraine-conversations --bench analytics_latency --locked -- --profile smoke --output target/bench/analytics-latency-smoke.json`; full substitutes `--profile full` and `...-full.json`. The seed owner, independently of either measured arm, writes `{"fingerprint":"sha256:<64-lowercase-hex>","cardinality":<seeded-row-count>,"expected_semantics":{"analytics-24h":{"cardinality":{"kind":"analytics_series","tokens":<n>,"turns":<n>,"concurrent_sessions":<n>},"digest":"fnv1a64:<16-lowercase-hex>"},"sessions-30d50":{"cardinality":{"kind":"sessions","sessions":<n>},"digest":"fnv1a64:<16-lowercase-hex>"}}}` from the canonical seed contract. Optional database/user/password variables identify that same owned dataset for all arms. Helpers: `cargo test -p moraine-conversations --test analytics_benchmark_support --locked`. | Caller provisions one owned live stack/database, derives the manifest from the complete canonical seeded corpus without consulting measured responses, and always tears it down (for a sandbox: `scripts/dev/sandbox/moraine-sandbox down <id>`). The bench verifies the dataset identity, then verifies monitor, cold-repository, and warm-repository outputs separately against the two seed-owned semantic expectations before supplemental arm-parity checks; it verifies the corpus stayed unchanged afterward. It is read-only, uses 30s HTTP requests, and owns only its atomic output; unlike the live-test wrapper, it does not provision or clean the stack. Custom Cargo bench, not Criterion. | T4 compile/deterministic on PR when wired; reduced live smoke/manual and full manual. Missing URLs/manifest/expected scenario, corpus drift, semantic/cardinality/digest/output failure is nonzero; timing non-blocking. |
| `replay_search_latency` / local PyO3 | `scripts/bench/replay_search_latency.py --config <moraine.toml> --profile smoke --output-json target/bench/replay-search-latency-smoke.json` (or `full`/`...-full.json`). Check: `python3 -m unittest scripts/bench/test_replay_search_latency.py`. | Readable config, populated ClickHouse `search_query_log`, uv/self-declared maturin+psutil, Rust/local binding. Smoke 2 rows/0 warmups/1 repeat; full 20/1/5. 20s request, 600s owned maturin child; process-group TERM/KILL cleanup. | T4. Read-only DB; atomic schema-valid output; missing data/tool, oracle/semantic/output failure is nonzero; timing `not_evaluated`. |
| `replay_mcp_latency` / persistent and cold-process | `python3 scripts/bench/replay_mcp_latency.py --config <moraine.toml> --oracle-manifest <seed-owned-oracle.json> --profile smoke --mode persistent --output-json target/bench/replay-mcp-persistent-smoke.json`; use `cold_process` for the distinct lifecycle boundary and `full` for full runs. Check: `python3 -m unittest scripts/bench/test_replay_mcp_latency.py`. The independent manifest is `{"schema_version":"moraine-replay-mcp-oracle-v1","provenance":"<seed/query-generator-reference>","cases":[{"query_id":"<telemetry-query-id>","variant_label":"<expanded-query-variant>","tool":"search","semantic_digest":"sha256:<64-lowercase-hex>","cardinality":<positive-result-count>,"marker":"<seed-owned-marker>"}]}` with one case for every selected query variant and tool. | Populated telemetry, production `bin/moraine`/`moraine-mcp`, exact git provenance, and complete seed/query expectations created without invoking the measured MCP path. Persistent owns one long-lived MCP child; cold owns one per sample. Per-RPC timeout and TERM/KILL cleanup are producer-owned. Manifest/case fingerprints and provenance are retained in workload identity. | T4; modes are fingerprint-distinct and never compared to one another. Missing/malformed/incomplete oracle fails before process spawn; repeatably wrong or one-sample smoke semantics, child/timeout/output failure is nonzero; timing non-blocking. |
| `mcp_two_tool_sla` / MCP tool matrix | `python3 scripts/bench/mcp_two_tool_sla.py --config <owned-config> --oracle-json <seed-owned-oracle.json> --profile smoke --output-json target/bench/mcp-two-tool-sla.json` (or `--profile full`; full defaults warmup=1/repeats=5/min-docs=100000). Check: `python3 scripts/bench/test_mcp_two_tool_sla.py`. The independent oracle is `{"schema_version":"moraine-mcp-two-tool-oracle-v1","queries":[{"query":"<selected-query>","expected":{"result_count":<n>,"open_ids":{"event":"<seed-id>","turn":"<seed-id>","session":"<seed-id>"},"result_marker":{"<seed-field>":"<seed-value>"}}}],"list_sessions":{"result_count":<n>,"session_ids":["<seed-session-id>"],"result_marker":{"<seed-field>":"<seed-value>"}}}`. Each query needs at least one expectation; only requested open kinds require IDs. The root `list_sessions` object is required whenever that default boundary is measured and needs at least one count/ID/object-marker expectation. | Python 3, read-only ClickHouse corpus, production binaries, exact git provenance, and a complete oracle generated by the corpus owner without measured MCP responses. Search cardinality/IDs/object marker are checked against seed truth; open targets the oracle ID rather than a measured search result; every list result checks the exact count, presence of all seed IDs, and matching seed object marker. One owned stdio MCP child; 20s RPC timeout; close stdin, TERM/wait 5s, KILL/wait 5s. | T4. Missing/incomplete oracle or corpus/prerequisite, repeatable wrong search/open/list result, insufficient sample, validation/write failure is nonzero. No first measured response becomes expected truth. Historical SLA values are diagnostics; v1 timing stays `not_evaluated`. |
| `central_mcp_resource` / embedded-versus-central | Smoke: `uv run --script scripts/bench/central_mcp_resource.py --moraine-mcp target/debug/moraine-mcp --arms embedded,central --ns 1 --reps 1 --profile smoke --settle-seconds 0 --batch 1 --startup-timeout-seconds 5 --request-timeout-seconds 20 --dataset-fingerprint sha256:<64-lowercase-hex> --dataset-cardinality <seeded-session-count> --json-out target/bench/central-mcp-resource-smoke.json`. Full substitutes the release binary, owned sandbox ClickHouse URL, `--ns 1,10,50,100 --reps 3 --profile full --settle-seconds 2 --batch 25 --startup-timeout-seconds 10`, and `...-full.json`. Check: `python3 scripts/bench/test_central_mcp_resource.py`. | Built MCP binary, deterministic seeded ClickHouse, and psutil or Linux `/proc`; Linux is authoritative. Each repetition owns temporary config/root/socket, central daemon, clients, loopback port, and static fixture; cleanup TERM/KILLs children and removes the root. macOS observations are directional. | T4. JSON output requires dataset fingerprint/cardinality. The configured arm×N matrix is one paired result. Startup/exit/timeout/semantic/sampling/cleanup/validation/atomic-output failure is nonzero; timing/resources remain `not_evaluated` and non-blocking. |
| `concurrent_mcp_retrieval` / central persistent concurrent uncached `search_sessions` | Smoke: `python3 scripts/bench/concurrent_mcp_retrieval.py --moraine-mcp target/debug/moraine-mcp --clickhouse-url http://clickhouse:8123 --database moraine --oracle-json <seed-owned-oracle.json> --concurrency 1 --concurrency 4 --reps 1 --profile smoke --startup-timeout-seconds 10 --request-timeout-seconds 20 --run-timeout-seconds 120 --max-processes 16 --output-dir target/bench/concurrent-smoke`. Full uses a release binary, `--profile full --concurrency 1,2,4,8,16,32 --reps 3 --run-timeout-seconds 1800 --max-processes 64`; choose the sweep and high-stress value as caller workload inputs, never from the backend admission limit. A range such as `--concurrency 1:32:2` is inclusive. Smoke recovery is fixed at 0 and 1 seconds; full recovery is fixed at 0 and 10 seconds. Check: `python3 -m unittest scripts/bench/test_concurrent_mcp_retrieval.py`; validate every result with `for artifact in target/bench/concurrent-smoke/*.json; do python3 scripts/bench/benchmark_protocol.py validate "$artifact"; done`. The independent seed owner writes `{"schema_version":"moraine-concurrent-mcp-oracle-v1","provenance":"<stable-lowercase-slug>","dataset":{"fingerprint":"sha256:<64-lowercase-hex>","cardinality":<searchable-document-count>},"warmup":{"id":"warmup","query":"<startup-only-query>","result_count":<1..10>,"result_digest":"sha256:<digest-of-sorted-event-and-session-ids>"},"measured":[<same-shape-distinct-cases>],"recovery":[<same-shape-distinct-cases>]}`. Warmup, measured, and recovery query strings and IDs must all be distinct; the full manifest declares at least 100,000 searchable documents. | Linux owned dev sandbox, built MCP binary, deterministic seeded ClickHouse, and one oracle case per maximum requested concurrency plus two recovery probes. The timed boundary is each stdio `tools/call` write through its central-server response; client initialization, tool discovery, and one disjoint startup query are outside the burst. Every repetition owns a fresh central service, socket, clients, config, root, and temporary directory; the producer terminates/removes all of them while the sandbox owner owns ClickHouse corpus cleanup. `--max-processes` and startup/request/run timeouts protect the runner and do not state backend capacity or admission policy; request timeout is comparison-fingerprinted. | T4. Read-only seeded DB. One comparison-distinct schema-valid artifact per exact concurrency. Raw successful wall/server/SLA series, per-case outcome records, admission/error/timeout/deadline counts and rates, p50/p95/p99/max, throughput, maximum in-flight, hard-deadline violations, and largest-burst sequential recovery are retained. Timing is diagnostic `not_evaluated`; semantic-oracle and protocol failures remain correctness failures. |
Prepare only an owned sandbox corpus. Both commands below truncate
`moraine.search_documents`, `search_postings`, `search_query_log`, and
`search_hit_log`; never point them at a host or shared database.

```bash
# Small live wiring/semantic smoke.
python3 scripts/bench/seed_concurrent_mcp_benchmark.py seed \
  --clickhouse-url http://clickhouse:8123 --database moraine \
  --documents 200 --measured-cases 8 --recovery-cases 2 \
  --oracle-json /tmp/concurrent-smoke-oracle.json

# Full SLA-tier corpus and broad caller-selected sweep.
python3 scripts/bench/seed_concurrent_mcp_benchmark.py seed \
  --clickhouse-url http://clickhouse:8123 --database moraine \
  --documents 100000 --measured-cases 64 --recovery-cases 2 \
  --oracle-json /tmp/concurrent-full-oracle.json
```

The seeder writes the expected public event/session ID digest directly from its
deterministic recipe; it never learns expectations from an MCP response. Cases
alternate selective one-posting terms and distinct common terms, and reserve
disjoint warmup/recovery terms. `seed ... --documents 100000` is the full-profile
minimum. `clean` with the same ClickHouse/database arguments truncates the four
tables explicitly; normally `moraine-sandbox down <id>` owns complete database,
socket, child, config, temporary-directory, and volume cleanup.

#### Current-main evidence (2026-07-12)

An owned Linux sandbox run at `2ba068794554710708be8ca48ed88b8c73825c31`
used the deterministic 100,000-document corpus, the sandbox workspace binary
(`build.profile=unknown`), three fresh-service repetitions, and the caller-selected
`1,2,4,8,16,32,64` sweep. Every artifact passed
`benchmark_protocol.py validate`; every arm reported overlap and exact requested
maximum in-flight. These are diagnostic observations, not a capacity contract:

| Requested | Success / attempted | Rejected | p50 ms | p95 ms | p99 ms | max ms | mean success/s |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 3 / 3 | 0 | 43.7 | 123.3 | 130.4 | 132.2 | 19.1 |
| 2 | 6 / 6 | 0 | 59.3 | 69.0 | 69.8 | 70.0 | 30.2 |
| 4 | 12 / 12 | 0 | 91.0 | 149.0 | 149.9 | 150.1 | 38.6 |
| 8 | 24 / 24 | 0 | 134.9 | 166.2 | 167.3 | 167.4 | 53.3 |
| 16 | 24 / 48 | 24 | 120.1 | 143.5 | 147.7 | 148.9 | 61.2 |
| 32 | 24 / 96 | 72 | 114.0 | 167.3 | 169.2 | 169.6 | 57.4 |
| 64 | 24 / 192 | 168 | 184.1 | 243.9 | 247.9 | 248.8 | 38.4 |

The observed curve rises through eight callers, then successful completions plateau
at eight per repetition while admission rejections account for every additional
attempt. That ordinal is an observation of this build, not an asserted product
limit. All successful samples met their advertised SLA and none crossed the
five-second hard deadline. At the largest arm, recovery wall latencies at fixed
0/10-second offsets were `31.6/126.3`, `43.2/108.2`, and `34.7/103.1` ms across
the three repetitions. Three repetitions show run-to-run spread but do not define
the controlled runner, baseline, threshold, error, or variability policy; all
artifacts therefore retain `timing.status=not_evaluated`.



Every producer's deterministic checks cover arguments/profile/default/threshold
propagation, schema serialization, contradictory inputs, output-write failures, and
applicable child crash/timeout/cleanup, insufficient sample, and oracle instability.
A live smoke is required only when it proves production wiring, telemetry discovery,
or cleanup; a database-reading `--dry-run` is not a T0 hermetic test.

## CI ownership and promotion

| Workflow / job owner | Trigger, timeout, and responsibility |
| --- | --- |
| `.github/workflows/ci-pr-fast.yml` / `pr-fast-linux` | Every PR/manual, Ubuntu, 30m. Unconditional Rust/policy T0; path-filtered canonical source-stack T1 using the explicit stack globs above. |
| `.github/workflows/ci-monitor-frontend.yml` / `monitor-frontend-bun` | PR paths/manual, Ubuntu, 10m. Bun 1.3.9 frozen install, typecheck, and nonzero Vitest. |
| `.github/workflows/ci-python-binding.yml` / `python-binding-smoke` | Binding/dependency/SQL PR paths/manual, Ubuntu, 20m. Python 3.12 locked venv, `maturin develop --locked`, nonzero pytest/JUnit. |
| `.github/workflows/ci-monitor-browser-mocked.yml` / `monitor-browser-mocked` | Visible PR/manual job, Ubuntu 24.04, 15m. Path-filtered pinned Chromium, exactly two mocked cases, no retry; initially non-required. |
| `.github/workflows/ci-test-architecture.yml` / `architecture-t0` | Benchmark/live-wrapper/support/manifests PR paths/manual, Ubuntu, 45m. Python 3.12 nonzero protocol/adapter/wrapper unittest discovery, Rust analytics bench `--no-run`, and path-filtered metadata-derived package-isolation compilation. |
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
- the five existing benchmark producers and distinct lifecycle scenarios.

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
