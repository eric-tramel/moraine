#!/usr/bin/env python3
import argparse
import json
import select
import subprocess
import sys
from typing import Any, Dict


def read_json_line(proc: subprocess.Popen[str], timeout_seconds: int = 20) -> Dict[str, Any]:
    if proc.stdout is None:
        raise RuntimeError("MCP stdout pipe is unavailable")

    ready, _, _ = select.select([proc.stdout], [], [], timeout_seconds)
    if not ready:
        stderr = proc.stderr.read() if proc.stderr else ""
        raise TimeoutError(f"timed out waiting for MCP response; stderr={stderr.strip()}")

    line = proc.stdout.readline()
    if line == "":
        stderr = proc.stderr.read() if proc.stderr else ""
        raise RuntimeError(f"MCP process exited unexpectedly; stderr={stderr.strip()}")

    return json.loads(line)


def send_request(proc: subprocess.Popen[str], payload: Dict[str, Any]) -> Dict[str, Any]:
    if proc.stdin is None:
        raise RuntimeError("MCP stdin pipe is unavailable")

    proc.stdin.write(json.dumps(payload) + "\n")
    proc.stdin.flush()
    return read_json_line(proc)


def assert_rpc_ok(response: Dict[str, Any], expected_id: int) -> Dict[str, Any]:
    if response.get("id") != expected_id:
        raise AssertionError(f"unexpected rpc id: {response.get('id')} (wanted {expected_id})")
    if "error" in response:
        raise AssertionError(f"rpc error: {response['error']}")
    result = response.get("result")
    if not isinstance(result, dict):
        raise AssertionError("rpc response missing result object")
    return result


def assert_tool_success(result: Dict[str, Any]) -> Dict[str, Any]:
    if result.get("isError"):
        raise AssertionError(f"tool call returned isError=true: {result}")
    return result


def run_smoke(cortexctl: str, config: str, query: str) -> None:
    proc = subprocess.Popen(
        [cortexctl, "run", "mcp", "--config", config],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )

    try:
        init_resp = send_request(
            proc,
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {},
            },
        )
        init_result = assert_rpc_ok(init_resp, 1)
        if "protocolVersion" not in init_result:
            raise AssertionError("initialize response missing protocolVersion")

        tools_resp = send_request(
            proc,
            {
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/list",
                "params": {},
            },
        )
        tools_result = assert_rpc_ok(tools_resp, 2)
        tools = tools_result.get("tools")
        if not isinstance(tools, list):
            raise AssertionError("tools/list missing tools array")

        tool_names = {tool.get("name") for tool in tools if isinstance(tool, dict)}
        if "search" not in tool_names or "open" not in tool_names:
            raise AssertionError(f"tools/list did not include search/open: {tool_names}")

        search_resp = send_request(
            proc,
            {
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/call",
                "params": {
                    "name": "search",
                    "arguments": {
                        "query": query,
                        "verbosity": "full",
                        "limit": 5,
                        "exclude_codex_mcp": False,
                    },
                },
            },
        )
        search_result = assert_tool_success(assert_rpc_ok(search_resp, 3))
        search_payload = search_result.get("structuredContent")
        if not isinstance(search_payload, dict):
            raise AssertionError("search structuredContent missing")

        hits = search_payload.get("hits")
        if not isinstance(hits, list) or not hits:
            raise AssertionError(f"search returned no hits for query={query}")

        first_hit = hits[0]
        if not isinstance(first_hit, dict):
            raise AssertionError("search first hit is not an object")
        event_uid = first_hit.get("event_uid")
        if not isinstance(event_uid, str) or not event_uid:
            raise AssertionError("search first hit missing event_uid")

        open_resp = send_request(
            proc,
            {
                "jsonrpc": "2.0",
                "id": 4,
                "method": "tools/call",
                "params": {
                    "name": "open",
                    "arguments": {
                        "event_uid": event_uid,
                        "verbosity": "full",
                    },
                },
            },
        )
        open_result = assert_tool_success(assert_rpc_ok(open_resp, 4))
        open_payload = open_result.get("structuredContent")
        if not isinstance(open_payload, dict):
            raise AssertionError("open structuredContent missing")

        events = open_payload.get("events")
        if not isinstance(events, list) or not events:
            raise AssertionError("open returned no context events")

        if not any(
            isinstance(event, dict) and event.get("event_uid") == event_uid for event in events
        ):
            raise AssertionError("open response did not include requested event_uid")
    finally:
        if proc.stdin:
            proc.stdin.close()
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cortexctl", required=True)
    parser.add_argument("--config", required=True)
    parser.add_argument("--query", required=True)
    args = parser.parse_args()

    run_smoke(args.cortexctl, args.config, args.query)
    print("mcp smoke passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

