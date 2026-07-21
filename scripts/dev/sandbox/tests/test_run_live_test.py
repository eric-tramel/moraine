#!/usr/bin/env python3
"""Focused lifecycle tests for scripts/dev/sandbox/run-live-test."""

from __future__ import annotations

import os
from pathlib import Path
import signal
import subprocess
import re
import tempfile
import time
import unittest


SCRIPT = Path(__file__).resolve().parents[1] / "run-live-test"
SANDBOX_SCRIPT = SCRIPT.parent / "moraine-sandbox"


FAKE_SANDBOX = r"""#!/usr/bin/env bash
set -u
cmd="${1:-}"
shift || true
case "$cmd" in
  up)
    id=""
    quiet=0
    while [[ $# -gt 0 ]]; do
      case "$1" in
        --id) id="${2:-}"; shift 2 ;;
        --quiet) quiet=1; shift ;;
        *) printf 'unexpected up argument: %s\n' "$1" >&2; exit 90 ;;
      esac
    done
    token="${MORAINE_SANDBOX_OWNERSHIP_TOKEN:-}"
    [[ -n "$id" && "$quiet" == 1 && "$token" =~ ^[a-f0-9]{64}$ ]] || exit 91
    printf 'up %s\n' "$id" >>"$FAKE_SANDBOX_CALLS"
    state="$FAKE_COMPOSE_DIR/$id"
    if [[ "${FAKE_EXISTING_FOREIGN_PROJECT:-0}" != 0 ]]; then
      printf '%064d\n' 0 >"$state"
      printf 'foreign project collision: %s\n' "$id" >&2
      exit 73
    fi
    if [[ -e "$state" ]]; then
      printf 'foreign project collision: %s\n' "$id" >&2
      exit 73
    fi
    printf '%s\n' "$token" >"$state"
    printf '%s %s\n' "$id" "$token" >>"$FAKE_TOKEN_LOG"
    if [[ "${FAKE_UP_RC:-0}" != 0 ]]; then
      exit "$FAKE_UP_RC"
    fi
    if [[ "${FAKE_UP_HANG:-0}" != 0 ]]; then
      trap '' TERM
      while :; do sleep 1; done
    fi
    if [[ -n "${FAKE_UP_OUTPUT:-}" ]]; then
      printf '%s\n' "$FAKE_UP_OUTPUT"
    else
      printf '%s\n' "$id"
    fi
    ;;
  down)
    id="${1:-}"
    [[ -n "$id" && $# == 1 ]] || exit 92
    printf 'down %s\n' "$id" >>"$FAKE_SANDBOX_CALLS"
    state="$FAKE_COMPOSE_DIR/$id"
    [[ -f "$state" ]] || exit 75
    actual="$(cat "$state")"
    token="${MORAINE_SANDBOX_OWNERSHIP_TOKEN:-}"
    if [[ "${FAKE_DOWN_TOKEN_MISMATCH:-0}" != 0 || "$actual" != "$token" ]]; then
      printf 'ownership token mismatch\n' >&2
      exit 74
    fi
    if [[ "${FAKE_DOWN_HANG:-0}" != 0 ]]; then
      printf '%s\n' "$$" >"$FAKE_PID_DIR/down-${id}.pid"
      trap '' TERM
      while :; do sleep 1 || true; done
    fi
    if [[ "${FAKE_DOWN_RC:-0}" != 0 ]]; then
      exit "$FAKE_DOWN_RC"
    fi
    if [[ "${FAKE_DATABASE_CLEANUP_RC:-0}" == 0 ]]; then
      suffix="${id#sb-}"
      printf -v database 'moraine_test_%s%026d' "$suffix" 0
      rm -f "$FAKE_DATABASE_DIR/$database"
    fi
    rm -f "$state"
    ;;
  *)
    printf 'unexpected sandbox command: %s\n' "$cmd" >&2
    exit 93
    ;;
esac
"""


FAKE_DOCKER = r"""#!/usr/bin/env bash
set -u
if [[ "${FAKE_CARGO_IGNORE_TERM:-0}" != 0 ]]; then
  trap '' TERM
fi
unset MORAINE_LIVE_TEST_DATABASE MORAINE_BENCH_CLICKHOUSE_DATABASE
[[ "${1:-}" == exec ]] || exit 80
shift
container=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    -i) shift ;;
    -u|-w) shift 2 ;;
    -e)
      export "$2"
      shift 2
      ;;
    -*) printf 'unexpected docker option: %s\n' "$1" >&2; exit 81 ;;
    *) container="$1"; shift; break ;;
  esac
done
[[ -n "$container" && $# -gt 0 ]] || exit 82
export FAKE_DOCKER_CONTAINER="$container"
exec "$@"
"""


FAKE_CARGO = r"""#!/usr/bin/env bash
set -u
if [[ "${FAKE_CARGO_IGNORE_TERM:-0}" != 0 ]]; then
  trap '' TERM
fi
sandbox_id="${MORAINE_LIVE_TEST_SANDBOX_ID:?}"
suffix="${sandbox_id#sb-}"
printf -v database 'moraine_test_%s%026d' "$suffix" 0
record="$FAKE_RECORD_DIR/cargo-${sandbox_id}.log"
resource="$FAKE_DATABASE_DIR/$database"
{
  printf 'container=%s\n' "$FAKE_DOCKER_CONTAINER"
  printf 'allow=%s\n' "${MORAINE_ALLOW_DESTRUCTIVE_TESTS:-}"
  printf 'sandbox_id=%s\n' "$sandbox_id"
  printf 'database=%s\n' "$database"
  printf 'caller_database=%s\n' "${MORAINE_LIVE_TEST_DATABASE:-}"
  printf 'bench_database=%s\n' "${MORAINE_BENCH_CLICKHOUSE_DATABASE:-}"
  printf 'clickhouse_url=%s\n' "${MORAINE_BENCH_CLICKHOUSE_URL:-}"
  printf 'ingest_bin=%s\n' "${MORAINE_LIVE_TEST_INGEST_BIN:-}"
  printf 'argv'
  printf ' <%s>' "$@"
  printf '\n'
} >"$record"
printf '%s\n' "$$" >"$FAKE_PID_DIR/cargo-${sandbox_id}.pid"
printf 'live ClickHouse database: %s\n' "$database"
printf 'cleanup: sandbox=%s endpoint=<redacted-owned-clickhouse> query=DROP DATABASE IF EXISTS `%s` SYNC\n' \
  "$sandbox_id" "$database"
: >"$resource"
if [[ "${FAKE_CARGO_IGNORE_TERM:-0}" != 0 ]]; then
  while :; do sleep 1 || true; done
elif [[ "${FAKE_CARGO_SLEEP:-0}" != 0 ]]; then
  sleep "$FAKE_CARGO_SLEEP"
fi
primary_rc="${FAKE_CARGO_RC:-0}"
cleanup_rc="${FAKE_DATABASE_CLEANUP_RC:-0}"
if [[ "$cleanup_rc" == 0 ]]; then
  rm -f "$resource"
else
  printf 'live ClickHouse teardown failed: retained %s\n' "$database" >&2
fi
if [[ "$primary_rc" != 0 ]]; then
  exit "$primary_rc"
fi
exit "$cleanup_rc"
"""


FAKE_REAL_DOCKER = r"""#!/usr/bin/env bash
set -u
printf '%s\n' "$*" >>"$FAKE_DOCKER_CALLS"
state="$FAKE_DOCKER_STATE"
owner_label="io.moraine.sandbox.ownership-token"
cmd="${1:-}"
shift || true
case "$cmd" in
  compose)
    if [[ "${1:-}" == version ]]; then exit 0; fi
    if [[ "${1:-}" == ls ]]; then printf '[]\n'; exit 0; fi
    if [[ "$*" == *" down -v --remove-orphans"* ]]; then
      rc="${FAKE_COMPOSE_DOWN_RC:-0}"
      if [[ "$rc" == 0 ]]; then
        rm -f "$state/container-token" "$state/network-token"
      fi
      exit "$rc"
    fi
    exit 0
    ;;
  ps)
    if [[ -n "${FAKE_EXISTING_PROJECT:-}" ]]; then
      printf 'foreign-container\n'
    elif [[ -f "$state/container-token" ]]; then
      printf 'owned-container\n'
    fi
    ;;
  network)
    sub="${1:-}"; shift || true
    case "$sub" in
      ls)
        if [[ -f "$state/network-token" ]]; then
          printf 'owned-network\n'
        fi
        ;;
      inspect)
        [[ "$*" == *".Labels"* && "$*" != *".Config.Labels"* ]] || exit 89
        cat "$state/network-token" 2>/dev/null || exit 1
        ;;
      *) exit 88 ;;
    esac
    ;;
  container)
    [[ "${1:-}" == inspect ]] || exit 87
    shift
    [[ "$*" == *".Config.Labels"* ]] || exit 89
    cat "$state/container-token" 2>/dev/null || exit 1
    ;;
  volume)
    sub="${1:-}"; shift || true
    case "$sub" in
      ls)
        if [[ -n "${FAKE_EXISTING_PROJECT:-}" ]]; then
          printf 'foreign-volume\n'
        elif [[ -f "$state/marker-name" ]]; then
          cat "$state/marker-name"
        fi
        ;;
      create)
        project=""
        token=""
        name=""
        while [[ $# -gt 0 ]]; do
          case "$1" in
            --label)
              case "$2" in
                com.docker.compose.project=*) project="${2#*=}" ;;
                "$owner_label"=*) token="${2#*=}" ;;
              esac
              shift 2
              ;;
            *) name="$1"; shift ;;
          esac
        done
        if [[ ! -f "$state/marker-name" ]]; then
          printf '%s\n' "$name" >"$state/marker-name"
          printf '%s\n' "$project" >"$state/project"
          printf '%s\n' "$token" >"$state/marker-token"
        fi
        printf '%s\n' "$name"
        ;;
      inspect)
        cat "$state/marker-token" 2>/dev/null || exit 1
        ;;
      rm)
        if [[ "${FAKE_VOLUME_RM_RC:-0}" != 0 ]]; then exit "$FAKE_VOLUME_RM_RC"; fi
        rm -f "$state/marker-name" "$state/marker-token" "$state/project"
        ;;
      *) exit 86 ;;
    esac
    ;;
  inspect)
    cat "$state/container-token" 2>/dev/null || exit 1
    ;;
  *)
    exit 85
    ;;
esac
"""


class RunLiveTestTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tempdir.name)
        self.bin_dir = self.root / "bin"
        self.bin_dir.mkdir()
        self.calls = self.root / "sandbox-calls.log"
        self.calls.touch()
        self.records = self.root / "records"
        self.records.mkdir()
        self.databases = self.root / "databases"
        self.databases.mkdir()
        self.diagnostics = self.root / "diagnostics"
        self.locks = self.root / "locks"
        self.compose_projects = self.root / "compose-projects"
        self.compose_projects.mkdir()
        self.token_log = self.root / "ownership-tokens.log"
        self.token_log.touch()
        self.pid_dir = self.root / "pids"
        self.pid_dir.mkdir()

        self.sandbox = self.bin_dir / "fake-sandbox"
        self.docker = self.bin_dir / "docker"
        self.cargo = self.bin_dir / "cargo"
        for path, body in (
            (self.sandbox, FAKE_SANDBOX),
            (self.docker, FAKE_DOCKER),
            (self.cargo, FAKE_CARGO),
        ):
            path.write_text(body)
            path.chmod(0o755)

        self.env = os.environ.copy()
        self.env.update(
            {
                "PATH": f"{self.bin_dir}{os.pathsep}{self.env['PATH']}",
                "MORAINE_SANDBOX_CLI": str(self.sandbox),
                "MORAINE_LIVE_TEST_DIAGNOSTIC_ROOT": str(self.diagnostics),
                "MORAINE_LIVE_TEST_ID_LOCK_ROOT": str(self.locks),
                "MORAINE_LIVE_TEST_TIMEOUT_SECONDS": "5",
                "FAKE_SANDBOX_CALLS": str(self.calls),
                "FAKE_RECORD_DIR": str(self.records),
                "FAKE_DATABASE_DIR": str(self.databases),
                "FAKE_COMPOSE_DIR": str(self.compose_projects),
                "FAKE_TOKEN_LOG": str(self.token_log),
                "FAKE_PID_DIR": str(self.pid_dir),
            }
        )

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def run_wrapper(
        self, *args: str, env_overrides: dict[str, str] | None = None
    ) -> subprocess.CompletedProcess[str]:
        env = self.env.copy()
        if env_overrides:
            env.update(env_overrides)
        return subprocess.run(
            [str(SCRIPT), *args],
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=15,
            check=False,
        )

    def spawn_wrapper(
        self, mode: str, env_overrides: dict[str, str] | None = None
    ) -> subprocess.Popen[str]:
        env = self.env.copy()
        if env_overrides:
            env.update(env_overrides)
        process = subprocess.Popen(
            [str(SCRIPT), mode],
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,
        )
        self.addCleanup(self.cleanup_process, process)
        return process

    def cleanup_process(self, process: subprocess.Popen[str]) -> None:
        if process.poll() is None:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        try:
            process.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.communicate(timeout=5)

    def assert_recorded_processes_reaped(self) -> None:
        for pid_path in self.pid_dir.glob("*.pid"):
            pid = int(pid_path.read_text())
            with self.assertRaises(ProcessLookupError, msg=f"process {pid} from {pid_path}"):
                os.kill(pid, 0)

    def sandbox_calls(self) -> list[tuple[str, str]]:
        return [tuple(line.split()) for line in self.calls.read_text().splitlines()]

    def cargo_records(self) -> list[Path]:
        return sorted(self.records.glob("cargo-*.log"))

    def database_resources(self) -> list[str]:
        return sorted(path.name for path in self.databases.iterdir())

    def diagnostic_identities(self) -> dict[str, str]:
        identities = {}
        for log in self.diagnostics.glob("run.*/run.log"):
            contents = log.read_text()
            sandbox = re.search(r"^sandbox_id=(sb-[a-f0-9]{6})$", contents, re.MULTILINE)
            database = re.search(
                r"^live ClickHouse database: (moraine_test_[a-f0-9]{32})$",
                contents,
                re.MULTILINE,
            )
            self.assertIsNotNone(sandbox, contents)
            self.assertIsNotNone(database, contents)
            identities[sandbox.group(1)] = database.group(1)
        return identities

    def only_diagnostic(self) -> str:
        logs = list(self.diagnostics.glob("run.*/run.log"))
        self.assertEqual(len(logs), 1)
        return logs[0].read_text()

    def assert_exact_invocation(self, mode: str, function: str) -> None:
        self.assertEqual(self.database_resources(), [])
        result = self.run_wrapper(mode)
        self.assertEqual(result.returncode, 0, result.stderr)

        calls = self.sandbox_calls()
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[0][0], "up")
        self.assertEqual(calls[1], ("down", calls[0][1]))
        sandbox_id = calls[0][1]

        records = self.cargo_records()
        self.assertEqual(len(records), 1)
        record = records[0].read_text()
        self.assertIn(f"container=moraine-sandbox-{sandbox_id}\n", record)
        self.assertIn("allow=1\n", record)
        self.assertIn(f"sandbox_id={sandbox_id}\n", record)
        self.assertIn("clickhouse_url=http://clickhouse:8123\n", record)
        self.assertIn(
            "ingest_bin=/opt/moraine/bin/moraine-ingest\n", record
        )
        self.assertIn(
            "argv <test> <-p> <moraine-conversations> <--test> <live_clickhouse> "
            f"<--locked> <{function}> <--> <--exact> <--ignored> <--nocapture>\n",
            record,
        )

        diagnostic = self.only_diagnostic()
        self.assertIn(f"sandbox_id={sandbox_id}\n", diagnostic)
        self.assertIn(
            f"sandbox_cleanup_redacted=scripts/dev/sandbox/moraine-sandbox down {sandbox_id}",
            diagnostic,
        )
        self.assertIn("credentials redacted", diagnostic)
        self.assertNotIn("password=", diagnostic.lower())
        self.assertEqual(self.database_resources(), [])

    def test_schema_mode_runs_only_exact_ignored_function(self) -> None:
        self.assert_exact_invocation(
            "analytics-schema", "live_schema_semantics_and_teardown"
        )

    def test_parity_mode_runs_only_exact_ignored_function(self) -> None:
        self.assert_exact_invocation(
            "analytics-parity", "live_monitor_repository_semantic_parity"
        )

    def test_source_publication_mode_runs_only_exact_ignored_function(self) -> None:
        self.assert_exact_invocation(
            "source-publication",
            "live_source_publication_cutover_crash_recovery",
        )

    def test_mcp_backfill_mode_runs_only_exact_ignored_function(self) -> None:
        self.assert_exact_invocation(
            "mcp-backfill", "live_mcp_open_batched_backfill_resources"
        )

    def test_missing_and_unknown_modes_fail_before_boot(self) -> None:
        missing = self.run_wrapper()
        unknown = self.run_wrapper("all-ignored")
        extra = self.run_wrapper("analytics-schema", "unexpected")

        self.assertEqual(missing.returncode, 2)
        self.assertEqual(unknown.returncode, 2)
        self.assertEqual(extra.returncode, 2)
        self.assertIn("exactly one mode is required", missing.stderr)
        self.assertIn("unknown mode: all-ignored", unknown.stderr)
        self.assertEqual(self.sandbox_calls(), [])
        self.assertEqual(self.cargo_records(), [])

    def test_primary_failure_is_preserved_and_owned_sandbox_is_cleaned(self) -> None:
        self.assertEqual(self.database_resources(), [])
        result = self.run_wrapper(
            "analytics-schema", env_overrides={"FAKE_CARGO_RC": "37"}
        )

        self.assertEqual(result.returncode, 37, result.stderr)
        calls = self.sandbox_calls()
        self.assertEqual(calls, [("up", calls[0][1]), ("down", calls[0][1])])
        self.assertIn("cargo exit=37", self.only_diagnostic())
        self.assertEqual(self.database_resources(), [])

    def test_cleanup_failure_is_explicit_alongside_primary_failure(self) -> None:
        result = self.run_wrapper(
            "analytics-schema",
            env_overrides={"FAKE_CARGO_RC": "23", "FAKE_DOWN_RC": "17"},
        )

        self.assertEqual(result.returncode, 23, result.stderr)
        self.assertIn("primary failure exit 23; cleanup also failed with exit 17", result.stderr)
        diagnostic = self.only_diagnostic()
        self.assertIn("cleanup FAILED", diagnostic)
        self.assertIn("primary failure exit=23; cleanup failure exit=17", diagnostic)
        self.assertEqual(self.database_resources(), [])

    def test_cleanup_failure_becomes_failure_after_successful_test(self) -> None:
        result = self.run_wrapper(
            "analytics-parity", env_overrides={"FAKE_DOWN_RC": "19"}
        )

        self.assertEqual(result.returncode, 19, result.stderr)
        self.assertIn("cleanup failed", result.stderr.lower())
        self.assertIn("cleanup FAILED", self.only_diagnostic())
        self.assertEqual(self.database_resources(), [])

    def test_term_resistant_cleanup_is_killed_and_retains_ownership_evidence(self) -> None:
        result = self.run_wrapper(
            "analytics-schema",
            env_overrides={
                "FAKE_DOWN_HANG": "1",
                "MORAINE_LIVE_TEST_CLEANUP_TIMEOUT_SECONDS": "2",
            },
        )

        self.assertEqual(result.returncode, 124, result.stderr)
        diagnostic = self.only_diagnostic()
        self.assertIn("cleanup TIMEOUT after 2 seconds", diagnostic)
        self.assertIn("process group resisted TERM; sending KILL", diagnostic)
        self.assertIn("ownership evidence retained", diagnostic)
        calls = self.sandbox_calls()
        self.assertEqual(calls, [("up", calls[0][1]), ("down", calls[0][1])])
        self.assertEqual(len(list(self.compose_projects.iterdir())), 1)
        ownership_records = list(self.locks.glob("run.*/ownership"))
        self.assertEqual(len(ownership_records), 1)
        self.assert_recorded_processes_reaped()

    def test_boot_failure_still_attempts_cleanup_of_reserved_id(self) -> None:
        result = self.run_wrapper(
            "analytics-schema", env_overrides={"FAKE_UP_RC": "31"}
        )

        self.assertEqual(result.returncode, 31, result.stderr)
        calls = self.sandbox_calls()
        self.assertEqual(calls, [("up", calls[0][1]), ("down", calls[0][1])])
        self.assertEqual(self.cargo_records(), [])
        self.assertEqual(self.database_resources(), [])

    def test_two_concurrent_invocations_own_distinct_sandboxes_and_databases(self) -> None:
        overrides = {
            "FAKE_CARGO_SLEEP": "2",
            "MORAINE_LIVE_TEST_DATABASE": "caller_database_must_not_be_used",
            "MORAINE_BENCH_CLICKHOUSE_DATABASE": "caller_database_must_not_be_used",
        }
        self.assertEqual(self.database_resources(), [])
        processes = [
            self.spawn_wrapper("analytics-schema", overrides) for _ in range(2)
        ]
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline and len(self.database_resources()) != 2:
            time.sleep(0.02)
        active_databases = self.database_resources()
        self.assertEqual(len(active_databases), 2)
        self.assertEqual(len(set(active_databases)), 2)
        for database in active_databases:
            self.assertRegex(database, r"^moraine_test_[a-f0-9]{32}$")
            self.assertNotIn("caller_database", database)

        results = [process.communicate(timeout=15) for process in processes]
        for process, (_, stderr) in zip(processes, results):
            self.assertEqual(process.returncode, 0, stderr)

        calls = self.sandbox_calls()
        up_ids = [sandbox_id for action, sandbox_id in calls if action == "up"]
        down_ids = [sandbox_id for action, sandbox_id in calls if action == "down"]
        self.assertEqual(len(up_ids), 2)
        self.assertEqual(len(set(up_ids)), 2)
        self.assertCountEqual(down_ids, up_ids)
        tokens = [line.split() for line in self.token_log.read_text().splitlines()]
        self.assertEqual(len(tokens), 2)
        self.assertEqual({item[0] for item in tokens}, set(up_ids))
        self.assertEqual(len({item[1] for item in tokens}), 2)
        for _, token in tokens:
            self.assertRegex(token, r"^[a-f0-9]{64}$")

        identities = self.diagnostic_identities()
        self.assertEqual(set(identities), set(up_ids))
        self.assertEqual(set(identities.values()), set(active_databases))
        self.assertEqual(len(set(identities.values())), 2)
        records = self.cargo_records()
        self.assertEqual(len(records), 2)
        for record_path in records:
            record = record_path.read_text()
            sandbox_id = re.search(r"^sandbox_id=(sb-[a-f0-9]{6})$", record, re.MULTILINE)
            database = re.search(
                r"^database=(moraine_test_[a-f0-9]{32})$", record, re.MULTILINE
            )
            self.assertIsNotNone(sandbox_id, record)
            self.assertIsNotNone(database, record)
            self.assertEqual(database.group(1), identities[sandbox_id.group(1)])
            self.assertIn("caller_database=\n", record)
            self.assertIn("bench_database=\n", record)
        self.assertEqual(self.database_resources(), [])
        self.assert_recorded_processes_reaped()

    def test_database_cleanup_failure_is_only_catchable_database_residue(self) -> None:
        self.assertEqual(self.database_resources(), [])
        result = self.run_wrapper(
            "analytics-schema", env_overrides={"FAKE_DATABASE_CLEANUP_RC": "41"}
        )

        self.assertEqual(result.returncode, 41, result.stderr)
        resources = self.database_resources()
        self.assertEqual(len(resources), 1)
        self.assertRegex(resources[0], r"^moraine_test_[a-f0-9]{32}$")
        diagnostic = self.only_diagnostic()
        self.assertIn(f"live ClickHouse database: {resources[0]}", diagnostic)
        self.assertIn("live ClickHouse teardown failed", diagnostic)

    def test_timeout_returns_124_and_cleans_owned_sandbox(self) -> None:
        result = self.run_wrapper(
            "analytics-parity",
            env_overrides={
                "FAKE_CARGO_SLEEP": "10",
                "MORAINE_LIVE_TEST_TIMEOUT_SECONDS": "1",
            },
        )

        self.assertEqual(result.returncode, 124, result.stderr)
        calls = self.sandbox_calls()
        self.assertEqual(calls, [("up", calls[0][1]), ("down", calls[0][1])])
        self.assertIn("timeout after 1 seconds", self.only_diagnostic())
        self.assert_recorded_processes_reaped()
        self.assertEqual(self.database_resources(), [])
        self.assertEqual(list(self.compose_projects.iterdir()), [])
        self.assertEqual(list(self.locks.iterdir()), [])

    def test_foreign_project_collision_is_rejected_without_destructive_teardown(self) -> None:
        result = self.run_wrapper(
            "analytics-schema",
            env_overrides={"FAKE_EXISTING_FOREIGN_PROJECT": "1"},
        )

        self.assertEqual(result.returncode, 73, result.stderr)
        calls = self.sandbox_calls()
        self.assertEqual(calls, [("up", calls[0][1]), ("down", calls[0][1])])
        self.assertEqual(len(list(self.compose_projects.iterdir())), 1)
        self.assertIn("ownership token mismatch", self.only_diagnostic())
        self.assertEqual(self.cargo_records(), [])

    def test_unsafe_precreated_and_symlink_roots_are_rejected_before_boot(self) -> None:
        self.diagnostics.mkdir(mode=0o755)
        insecure = self.run_wrapper("analytics-schema")
        self.assertEqual(insecure.returncode, 2, insecure.stderr)
        self.assertIn("unsafe pre-created temporary root", insecure.stderr)
        self.assertEqual(self.sandbox_calls(), [])

        self.diagnostics.chmod(0o700)
        target = self.root / "symlink-target"
        target.mkdir(mode=0o700)
        self.locks.symlink_to(target, target_is_directory=True)
        symlink = self.run_wrapper("analytics-schema")
        self.assertEqual(symlink.returncode, 2, symlink.stderr)
        self.assertIn("unsafe pre-created temporary root", symlink.stderr)
        self.assertEqual(self.sandbox_calls(), [])

    def test_boot_hang_uses_whole_invocation_deadline_and_reaps_process_group(self) -> None:
        started = time.monotonic()
        result = self.run_wrapper(
            "analytics-schema",
            env_overrides={
                "FAKE_UP_HANG": "1",
                "MORAINE_LIVE_TEST_TIMEOUT_SECONDS": "1",
            },
        )

        self.assertEqual(result.returncode, 124, result.stderr)
        self.assertLess(time.monotonic() - started, 7)
        calls = self.sandbox_calls()
        self.assertEqual(calls, [("up", calls[0][1]), ("down", calls[0][1])])
        diagnostic = self.only_diagnostic()
        self.assertIn("during sandbox boot", diagnostic)
        self.assertIn("process group resisted TERM; sending KILL", diagnostic)
        self.assertEqual(list(self.compose_projects.iterdir()), [])
        self.assertEqual(self.database_resources(), [])
        self.assertEqual(list(self.locks.iterdir()), [])

    def test_term_resistant_test_is_killed_reaped_and_owned_sandbox_is_cleaned(self) -> None:
        result = self.run_wrapper(
            "analytics-parity",
            env_overrides={
                "FAKE_CARGO_IGNORE_TERM": "1",
                "FAKE_CARGO_SLEEP": "30",
                "MORAINE_LIVE_TEST_TIMEOUT_SECONDS": "2",
            },
        )

        self.assertEqual(result.returncode, 124, result.stderr)
        self.assertIn("process group resisted TERM; sending KILL", self.only_diagnostic())
        self.assert_recorded_processes_reaped()
        self.assertEqual(list(self.compose_projects.iterdir()), [])
        self.assertEqual(list(self.locks.iterdir()), [])
        self.assertEqual(self.database_resources(), [])

    def test_sigterm_returns_143_and_cleans_owned_sandbox(self) -> None:
        process = self.spawn_wrapper(
            "analytics-schema", {"FAKE_CARGO_SLEEP": "30"}
        )
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and not self.cargo_records():
            time.sleep(0.02)
        self.assertTrue(self.cargo_records(), "fake Cargo process did not start")

        process.send_signal(signal.SIGTERM)
        _, stderr = process.communicate(timeout=10)

        self.assertEqual(process.returncode, 143, stderr)
        calls = self.sandbox_calls()
        self.assertEqual(calls, [("up", calls[0][1]), ("down", calls[0][1])])
        self.assertIn("caught signal TERM", self.only_diagnostic())
        self.assert_recorded_processes_reaped()
        self.assertEqual(self.database_resources(), [])
        self.assertEqual(list(self.compose_projects.iterdir()), [])
        self.assertEqual(list(self.locks.iterdir()), [])



class MoraineSandboxOwnershipTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tempdir.name)
        self.bin_dir = self.root / "bin"
        self.bin_dir.mkdir()
        self.state_root = self.root / "sandbox-state"
        self.state_root.mkdir(mode=0o700)
        self.docker_state = self.root / "docker-state"
        self.docker_state.mkdir()
        self.docker_calls = self.root / "docker-calls.log"
        self.docker_calls.touch()
        docker = self.bin_dir / "docker"
        docker.write_text(FAKE_REAL_DOCKER)
        docker.chmod(0o755)
        self.env = os.environ.copy()
        self.env.update(
            {
                "PATH": f"{self.bin_dir}{os.pathsep}{self.env['PATH']}",
                "MORAINE_SANDBOX_STATE_ROOT": str(self.state_root),
                "FAKE_DOCKER_STATE": str(self.docker_state),
                "FAKE_DOCKER_CALLS": str(self.docker_calls),
            }
        )

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def run_sandbox(
        self, *args: str, env_overrides: dict[str, str] | None = None
    ) -> subprocess.CompletedProcess[str]:
        env = self.env.copy()
        if env_overrides:
            env.update(env_overrides)
        return subprocess.run(
            [str(SANDBOX_SCRIPT), *args],
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=10,
            check=False,
        )

    def prepare_owned_project(self, sandbox_id: str, token: str) -> Path:
        project = f"moraine-sandbox-{sandbox_id}"
        config_dir = self.state_root / project
        config_dir.mkdir(mode=0o700)
        token_path = config_dir / ".ownership-token"
        token_path.write_text(f"{token}\n")
        token_path.chmod(0o600)
        (config_dir / "moraine.toml").write_text("generated\n")
        (config_dir / "ownership.compose.yaml").write_text("generated\n")
        (config_dir / "fixtures" / "codex" / "sessions").mkdir(parents=True)
        (self.docker_state / "marker-name").write_text(f"{project}_ownership\n")
        (self.docker_state / "marker-token").write_text(f"{token}\n")
        (self.docker_state / "project").write_text(f"{project}\n")
        (self.docker_state / "container-token").write_text(f"{token}\n")
        (self.docker_state / "network-token").write_text(f"{token}\n")
        return config_dir

    def test_existing_foreign_compose_project_is_rejected_before_build_or_up(self) -> None:
        result = self.run_sandbox(
            "up",
            "--id",
            "sb-a1b2c3",
            "--quiet",
            env_overrides={
                "FAKE_EXISTING_PROJECT": "1",
                "MORAINE_SANDBOX_OWNERSHIP_TOKEN": "a" * 64,
            },
        )

        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("already exists or could not be reserved", result.stderr)
        calls = self.docker_calls.read_text()
        self.assertNotIn(" build", calls)
        self.assertNotIn(" up -d", calls)
        self.assertFalse((self.state_root / "moraine-sandbox-sb-a1b2c3").exists())
        self.assertFalse((self.docker_state / "marker-name").exists())

    def test_down_rejects_caller_token_mismatch_without_touching_resources(self) -> None:
        config_dir = self.prepare_owned_project("sb-b2c3d4", "b" * 64)
        result = self.run_sandbox(
            "down",
            "sb-b2c3d4",
            env_overrides={"MORAINE_SANDBOX_OWNERSHIP_TOKEN": "c" * 64},
        )

        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("ownership token mismatch", result.stderr)
        self.assertTrue(config_dir.exists())
        self.assertTrue((self.docker_state / "marker-name").exists())
        self.assertNotIn(" down -v ", self.docker_calls.read_text())

    def test_compose_down_failure_propagates_and_retains_ownership_evidence(self) -> None:
        token = "d" * 64
        config_dir = self.prepare_owned_project("sb-c3d4e5", token)
        result = self.run_sandbox(
            "down",
            "sb-c3d4e5",
            env_overrides={
                "MORAINE_SANDBOX_OWNERSHIP_TOKEN": token,
                "FAKE_COMPOSE_DOWN_RC": "29",
            },
        )

        self.assertEqual(result.returncode, 29, result.stderr)
        self.assertIn("ownership evidence retained", result.stderr)
        self.assertTrue((config_dir / ".ownership-token").exists())
        self.assertTrue((config_dir / "moraine.toml").exists())
        self.assertTrue((config_dir / "ownership.compose.yaml").exists())
        self.assertTrue((self.docker_state / "marker-name").exists())

    def test_marker_volume_failure_propagates_and_retains_ownership_evidence(self) -> None:
        token = "e" * 64
        config_dir = self.prepare_owned_project("sb-d4e5f6", token)
        result = self.run_sandbox(
            "down",
            "sb-d4e5f6",
            env_overrides={
                "MORAINE_SANDBOX_OWNERSHIP_TOKEN": token,
                "FAKE_VOLUME_RM_RC": "31",
            },
        )

        self.assertEqual(result.returncode, 31, result.stderr)
        self.assertIn("marker volume removal failed", result.stderr)
        self.assertTrue((config_dir / ".ownership-token").exists())
        self.assertTrue((self.docker_state / "marker-name").exists())

    def test_successful_down_removes_marker_and_private_config(self) -> None:
        token = "f" * 64
        config_dir = self.prepare_owned_project("sb-e5f6a7", token)
        result = self.run_sandbox(
            "down",
            "sb-e5f6a7",
            env_overrides={"MORAINE_SANDBOX_OWNERSHIP_TOKEN": token},
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse(config_dir.exists())
        self.assertFalse((self.docker_state / "marker-name").exists())

if __name__ == "__main__":
    unittest.main()
