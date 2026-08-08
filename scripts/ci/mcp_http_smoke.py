#!/usr/bin/env python3
import argparse
import json
import threading
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

PROTOCOL_VERSION = "2025-06-18"


def post_json(
    url: str,
    payload: Dict[str, Any],
    *,
    initialized: bool = True,
) -> Tuple[int, bytes, Optional[str]]:
    headers = {
        "Accept": "application/json, text/event-stream",
        "Content-Type": "application/json",
        "Origin": "http://127.0.0.1",
    }
    if initialized:
        headers["MCP-Protocol-Version"] = PROTOCOL_VERSION
    request = urllib.request.Request(
        url,
        data=json.dumps(payload, separators=(",", ":")).encode("utf-8"),
        headers=headers,
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.status, response.read(), response.headers.get("Content-Type")
    except urllib.error.HTTPError as error:
        body = error.read()
        raise AssertionError(
            f"MCP HTTP request failed with {error.code}: {body.decode('utf-8', 'replace')}"
        ) from error


def rpc(url: str, request_id: int, method: str, params: Dict[str, Any]) -> Dict[str, Any]:
    status, body, content_type = post_json(
        url,
        {
            "jsonrpc": "2.0",
            "id": request_id,
            "method": method,
            "params": params,
        },
    )
    if status != 200:
        raise AssertionError(f"{method} returned HTTP {status}, expected 200")
    if not content_type or not content_type.startswith("application/json"):
        raise AssertionError(f"{method} returned unexpected content type {content_type!r}")
    payload = json.loads(body)
    if payload.get("id") != request_id:
        raise AssertionError(f"{method} response id mismatch: {payload!r}")
    if "error" in payload:
        raise AssertionError(f"{method} returned JSON-RPC error: {payload['error']!r}")
    return payload.get("result", {})


def initialize(url: str, request_id: int) -> None:
    status, body, _ = post_json(
        url,
        {
            "jsonrpc": "2.0",
            "id": request_id,
            "method": "initialize",
            "params": {
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": {},
                "clientInfo": {"name": "moraine-http-smoke", "version": "1"},
            },
        },
        initialized=False,
    )
    if status != 200:
        raise AssertionError(f"initialize returned HTTP {status}, expected 200")
    payload = json.loads(body)
    if payload.get("result", {}).get("protocolVersion") != PROTOCOL_VERSION:
        raise AssertionError(f"initialize protocol mismatch: {payload!r}")

    status, body, _ = post_json(
        url,
        {"jsonrpc": "2.0", "method": "notifications/initialized"},
    )
    if status != 202 or body:
        raise AssertionError(
            f"initialized notification returned status={status}, body={body!r}; expected empty 202"
        )


def run(url: str, query: str, pids_dir: Path) -> None:
    mcp_pid = pids_dir / "mcp.pid"
    if mcp_pid.exists():
        raise AssertionError("mcp.pid existed before direct HTTP clients started")

    start_barrier = threading.Barrier(3)
    overlap_barrier = threading.Barrier(2)
    stop_watcher = threading.Event()
    watcher_ready = threading.Event()
    pid_observed = threading.Event()

    def watch_pid() -> None:
        watcher_ready.set()
        while not stop_watcher.wait(0.001):
            if mcp_pid.exists():
                pid_observed.set()

    watcher = threading.Thread(target=watch_pid, name="mcp-pid-watcher", daemon=True)
    watcher.start()
    if not watcher_ready.wait(timeout=5):
        raise AssertionError("mcp.pid watcher did not start")

    def client(request_id: int) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        start_barrier.wait(timeout=10)
        initialize(url, request_id)
        tools = rpc(url, request_id + 1, "tools/list", {})
        overlap_barrier.wait(timeout=10)
        result = rpc(
            url,
            request_id + 2,
            "tools/call",
            {
                "name": "search_sessions",
                "arguments": {"query": query, "n_hits": 3},
            },
        )
        return tools, result

    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            first = executor.submit(client, 10)
            second = executor.submit(client, 20)
            start_barrier.wait(timeout=10)
            sequences = [first.result(timeout=30), second.result(timeout=30)]
    finally:
        stop_watcher.set()
        watcher.join(timeout=5)

    if watcher.is_alive():
        raise AssertionError("mcp.pid watcher did not stop")
    if pid_observed.is_set() or mcp_pid.exists():
        raise AssertionError("overlapping HTTP client sequences created mcp.pid")

    for tools, result in sequences:
        names = [tool.get("name") for tool in tools.get("tools", [])]
        if "search_sessions" not in names:
            raise AssertionError(f"tools/list omitted search_sessions: {names!r}")
        if result.get("isError") is not False:
            raise AssertionError(f"concurrent tools/call failed: {result!r}")
        structured = result.get("structuredContent", {})
        if structured.get("tool") != "search_sessions":
            raise AssertionError(f"unexpected tools/call payload: {structured!r}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True)
    parser.add_argument("--query", required=True)
    parser.add_argument("--pids-dir", required=True)
    args = parser.parse_args()
    run(args.url, args.query, Path(args.pids_dir))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
