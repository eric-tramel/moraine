# Native central burst probe

`performance_suite.py native-central-burst` is the narrow native-macOS probe
for the central MCP search path. It reports two deliberately separate phases at
concurrency 1, 4, and 8:

- `cold_lifecycle` starts a fresh central daemon for every repetition, opens the
  required persistent stdio routes, and times the first synchronized search.
  Daemon startup and route initialization are outside the boundary, so this is
  a cold process-cache query measurement, not time-to-readiness.
- `steady_state` starts one fresh daemon per concurrency, warms every selected
  query once outside the boundary, and then times synchronized identical calls
  over persistent routes.

Every measured request is checked against the existing frozen
performance-fixture oracle and timed outside the process with
`time.monotonic_ns()`.

The command intentionally does not install ClickHouse, migrate a database, or
seed user state. Point `--config` at a caller-owned, migrated database already
seeded with the matching `scripts/bench/performance_fixtures.py` profile,
including its active causal checkpoint, readiness row, and generation-1 source
head. The
config must live outside `~/.moraine`, explicitly select a loopback ClickHouse
HTTP endpoint and a non-default database name, and resolve only the default
backend. Non-default `[[routes]]` and repository `.moraine.toml` backend markers
are rejected. The harness executes from an owned neutral directory, verifies
that ClickHouse itself is a Darwin build, and checks exact event/search-index
cardinality, publication/checkpoint/append-control state, plus MCP projection
readiness and dirty-session lag before and after measurement. These checks prevent an innocent `--route-cwd` from silently
selecting Docker, a remote server, or live state.

Use a native arm64 release binary; debug binaries and non-native Python
processes are rejected. The fixture config must leave
`mcp.prewarm_on_initialize = false` (the default), so background prewarming
cannot overlap or invalidate the cold phase.

```bash
python3 scripts/bench/performance_suite.py native-central-burst \
  --mcp-binary ./target/release/moraine-mcp \
  --config /tmp/moraine-native-fixture/config.toml \
  --profile full \
  --split research \
  --bursts-per-case 25 \
  --cold-repetitions 100 \
  --minimum-cold-samples 100 \
  --warm-p95-limit-ms 750 \
  --cold-p95-limit-ms 2000 \
  --max-latency-ms 5000 \
  --collect-query-log \
  --output target/bench/performance/native-central-burst.json
```

The default modes cover a singleton term, the high-hydration family, a scoped
search, and a deterministic ranking tie. Cold repetitions rotate through those
cases and alternate C1/C4/C8 order between repetitions. The defaults retain at
least 100 successful cold samples at each concurrency before a result can pass.
All workers in one burst use the same case. At each concurrency, every selected
steady-state mode must have p95 at or below 750 ms, the high-hydration (common)
and session-scope cold modes must have p95 at or below 2,000 ms, and no raw
request sample may exceed 5,000 ms. Each threshold has its own CLI override.
Request errors, timeouts, an insufficient cold sample count, or any latency
gate miss makes the artifact fail.

`--collect-query-log` reads the configured ClickHouse `system.query_log` after
the owned daemons stop. It filters on their exact generated query-id prefixes
and retains only numeric publication capture, append-fence capture, candidate,
detail, publication revalidation, and append-fence revalidation counts,
latency, rows, bytes, result rows, memory, and exception counts. Every cold
lifecycle must show exactly one of all six statements per accepted request. A
steady lifecycle shows one six-statement warmup per selected case; its measured
cache hits must show the four control statements and zero candidate/detail
statements. Missing, extra, exceptional,
unknown-shape, or unattributed statements fail the run. SQL, query text, credentials, and
responses are never retained. Evidence requests revalidate the loopback URL and
explicitly disable environment HTTP proxies. The ClickHouse account in
`--config` needs read access to `system.query_log` and permission to execute
`SYSTEM FLUSH LOGS`.

The artifact retains every raw monotonic start/completion sample, phase and
lifecycle repetition, barrier slip, burst wall time, timeout,
protocol/tool/semantic error classification, binary and config digests,
latency-gate decisions, optional aggregate query-log evidence, and cleanup
evidence. The binary and config are copied once into owned storage for the run,
and their source/frozen/post-run hashes must agree; the validator also
recomputes the selected fixture case and oracle digests. Request text and
response content are not retained. Any failed gate makes the command exit
nonzero after writing the artifact. Daemons, proxy routes, temporary logs, and
Unix sockets are closed in `finally`-equivalent cleanup on both success and
failure.

`search_mcp_events` is not included: it is a repository operation, not a tool
published by the current MCP `tools/list` surface. Tool order is ignored and
additional unrelated public tools are allowed, but every lifecycle must expose
the same membership, include `search_sessions`, and omit `search_mcp_events`.
