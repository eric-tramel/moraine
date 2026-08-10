#!/usr/bin/env python3
import argparse
import json
import os
import signal
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

from mcp_smoke import (
    assert_rpc_ok,
    assert_structured_content,
    call_tool,
    collect_stderr,
    select_search_sessions_result,
    send_request,
)


def launch(moraine: str, config: str, working_dir: str) -> subprocess.Popen[str]:
    return subprocess.Popen(
        [moraine, "--config", config, "run", "mcp"],
        cwd=working_dir,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )


def send_notification(proc: subprocess.Popen[str], payload: dict[str, Any]) -> None:
    if proc.stdin is None:
        raise RuntimeError("MCP stdin pipe is unavailable")
    proc.stdin.write(json.dumps(payload) + "\n")
    proc.stdin.flush()


def initialize_and_search(
    proc: subprocess.Popen[str], query: str, expect_session_id: str
) -> set[str]:
    initialized = assert_rpc_ok(
        send_request(
            proc,
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {},
            },
        ),
        1,
    )
    if "protocolVersion" not in initialized:
        raise AssertionError("initialize response missing protocolVersion")
    send_notification(
        proc,
        {"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}},
    )
    listed = assert_rpc_ok(
        send_request(
            proc,
            {
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/list",
                "params": {},
            },
        ),
        2,
    )
    tools = listed.get("tools")
    if not isinstance(tools, list):
        raise TypeError("tools/list response missing tools array")
    names = {tool.get("name") for tool in tools if isinstance(tool, dict)}
    if "search_sessions" not in names or "open" not in names:
        raise AssertionError(f"canonical Moraine tools missing: {sorted(names)}")

    search = call_tool(
        proc,
        3,
        "search_sessions",
        {"query": query, "n_hits": 20},
    )
    payload = assert_structured_content(search, "search_sessions")
    results = payload["data"].get("results")
    if not isinstance(results, list):
        raise TypeError("search_sessions response missing results array")
    select_search_sessions_result(results, expect_session_id, None)
    return {name for name in names if isinstance(name, str)}


def stop(proc: subprocess.Popen[str]) -> None:
    if proc.stdin is not None:
        proc.stdin.close()
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)


def wait_for_child_pid(proc: subprocess.Popen[str]) -> int:
    children_path = Path(f"/proc/{proc.pid}/task/{proc.pid}/children")
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise AssertionError(
                f"canonical MCP wrapper exited before spawning its child: {proc.returncode}"
            )
        try:
            children = children_path.read_text(encoding="utf-8").split()
        except FileNotFoundError:
            children = []
        if children:
            return int(children[0])
        time.sleep(0.02)
    raise AssertionError("canonical MCP wrapper did not spawn a child within 5 seconds")


def process_is_live(pid: int) -> bool:
    try:
        state = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8").split()[2]
    except FileNotFoundError:
        return False
    return state != "Z"


def assert_wrapper_cleans_up_child(
    moraine: str, config: str, working_dir: str, *, force: bool
) -> None:
    proc = launch(moraine, config, working_dir)
    child_pid = wait_for_child_pid(proc)
    if force:
        os.kill(proc.pid, signal.SIGKILL)
    else:
        proc.terminate()
    proc.wait(timeout=10)

    deadline = time.monotonic() + 5
    while process_is_live(child_pid) and time.monotonic() < deadline:
        time.sleep(0.02)
    if process_is_live(child_pid):
        mode = "SIGKILL" if force else "SIGTERM"
        raise AssertionError(
            f"MCP child {child_pid} survived wrapper shutdown via {mode}"
        )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--moraine", required=True)
    parser.add_argument("--config", required=True)
    parser.add_argument("--working-dir", required=True)
    parser.add_argument("--legacy-pid-file", required=True)
    parser.add_argument("--query", required=True)
    parser.add_argument("--expect-session-id", required=True)
    args = parser.parse_args()

    legacy_pid = Path(args.legacy_pid_file)
    if legacy_pid.exists():
        raise AssertionError(
            f"legacy MCP PID exists before clients start: {legacy_pid}"
        )

    clients = [
        launch(args.moraine, args.config, args.working_dir),
        launch(args.moraine, args.config, args.working_dir),
    ]
    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [
                executor.submit(
                    initialize_and_search,
                    client,
                    args.query,
                    args.expect_session_id,
                )
                for client in clients
            ]
            tool_sets = []
            for client, future in zip(clients, futures):
                try:
                    tool_sets.append(future.result(timeout=30))
                except Exception as error:
                    stderr = collect_stderr(client, wait_seconds=0.5, max_bytes=65_536)
                    raise AssertionError(
                        f"concurrent canonical MCP client failed: {error}; "
                        f"stderr={stderr.strip()}"
                    ) from error
        if tool_sets[0] != tool_sets[1]:
            raise AssertionError("concurrent clients advertised different tool sets")
        if legacy_pid.exists():
            raise AssertionError(
                f"concurrent MCP clients created legacy PID: {legacy_pid}"
            )
    finally:
        for client in clients:
            stop(client)

    if sys.platform.startswith("linux"):
        assert_wrapper_cleans_up_child(
            args.moraine, args.config, args.working_dir, force=False
        )
        assert_wrapper_cleans_up_child(
            args.moraine, args.config, args.working_dir, force=True
        )

    print("concurrent canonical stdio MCP clients passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
