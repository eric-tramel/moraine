#!/usr/bin/env python3
import argparse
import base64
import json
import os
import select
import subprocess
from typing import Any, Dict, Optional


def collect_stderr(
    proc: subprocess.Popen[str], wait_seconds: float = 0.2, max_bytes: int = 8192
) -> str:
    if proc.stderr is None:
        return ""

    chunks: list[str] = []
    bytes_read = 0
    timeout = wait_seconds
    fd = proc.stderr.fileno()
    while bytes_read < max_bytes:
        ready, _, _ = select.select([proc.stderr], [], [], timeout)
        if not ready:
            break

        timeout = 0
        chunk = os.read(fd, min(4096, max_bytes - bytes_read))
        if not chunk:
            break
        chunks.append(chunk.decode("utf-8", errors="replace"))
        bytes_read += len(chunk)

    return "".join(chunks)


def read_json_line(proc: subprocess.Popen[str], timeout_seconds: int = 20) -> Dict[str, Any]:
    if proc.stdout is None:
        raise RuntimeError("MCP stdout pipe is unavailable")

    ready, _, _ = select.select([proc.stdout], [], [], timeout_seconds)
    if not ready:
        stderr = collect_stderr(proc)
        raise TimeoutError(f"timed out waiting for MCP response; stderr={stderr.strip()}")

    line = proc.stdout.readline()
    if line == "":
        stderr = collect_stderr(proc)
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


def call_tool(
    proc: subprocess.Popen[str],
    expected_id: int,
    name: str,
    arguments: Dict[str, Any],
) -> Dict[str, Any]:
    response = send_request(
        proc,
        {
            "jsonrpc": "2.0",
            "id": expected_id,
            "method": "tools/call",
            "params": {
                "name": name,
                "arguments": arguments,
            },
        },
    )
    return assert_tool_success(assert_rpc_ok(response, expected_id))


def assert_structured_content(result: Dict[str, Any], tool_name: str) -> Dict[str, Any]:
    payload = result.get("structuredContent")
    if not isinstance(payload, dict):
        raise AssertionError(f"{tool_name} structuredContent missing")
    if payload.get("tool") != tool_name:
        raise AssertionError(
            f"{tool_name} structuredContent has wrong tool: {payload.get('tool')}"
        )
    if "error" in payload:
        raise AssertionError(f"{tool_name} returned error envelope: {payload['error']}")
    if not isinstance(payload.get("data"), dict):
        raise AssertionError(f"{tool_name} structuredContent missing data object")
    return payload


def encode_mcp_component(raw: str) -> str:
    encoded = base64.urlsafe_b64encode(raw.encode("utf-8")).decode("ascii")
    return encoded.rstrip("=")


def expected_mcp_session_id(raw_session_id: Optional[str]) -> Optional[str]:
    if raw_session_id is None:
        return None
    return f"session:{encode_mcp_component(raw_session_id)}"


def nested_string(value: Dict[str, Any], *path: str) -> Optional[str]:
    current: Any = value
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current if isinstance(current, str) else None


def collect_strings(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, dict):
        strings: list[str] = []
        for child in value.values():
            strings.extend(collect_strings(child))
        return strings
    if isinstance(value, list):
        strings = []
        for child in value:
            strings.extend(collect_strings(child))
        return strings
    return []


def contains_text(value: Any, needle: str) -> bool:
    return any(needle in text for text in collect_strings(value))


def assert_tools_surface(tool_names_ordered: list[str]) -> None:
    if tool_names_ordered != ["search_sessions", "open"]:
        raise AssertionError(
            "tools/list must publish the two-tool search surface exactly: "
            f"{tool_names_ordered}"
        )


def select_search_sessions_result(
    results: list[Any],
    expect_session_id: Optional[str],
    expect_open_text: Optional[str],
) -> Dict[str, Any]:
    expected_session = expected_mcp_session_id(expect_session_id)
    candidates: list[Dict[str, Any]] = []

    for result in results:
        if not isinstance(result, dict):
            continue
        session_id = nested_string(result, "session", "id")
        open_session_id = nested_string(result, "open", "session_id")
        if expected_session is not None and expected_session not in {
            session_id,
            open_session_id,
        }:
            continue
        candidates.append(result)

    if not candidates:
        debug_hits = [
            {
                "id": hit.get("id"),
                "session_id": nested_string(hit, "session", "id"),
                "open_session_id": nested_string(hit, "open", "session_id"),
                "snippet": nested_string(hit, "snippet", "text"),
            }
            for hit in results
            if isinstance(hit, dict)
        ][:5]
        raise AssertionError(
            "search_sessions did not return a hit matching expected filters: "
            f"session_id={expect_session_id}, hits={debug_hits}"
        )

    if expect_open_text is not None:
        for result in candidates:
            if contains_text(result, expect_open_text):
                return result

    return candidates[0]


def open_ids_from_search_result(result: Dict[str, Any]) -> list[str]:
    handles = result.get("open")
    if not isinstance(handles, dict):
        raise AssertionError(f"search_sessions result missing open handles: {result}")

    open_ids: list[str] = []
    for key in ["event_id", "turn_id", "session_id"]:
        open_id = handles.get(key)
        if not isinstance(open_id, str) or not open_id:
            raise AssertionError(f"search_sessions result missing {key}: {result}")
        open_ids.append(open_id)
    return list(dict.fromkeys(open_ids))


def open_payload_session_id(payload: Dict[str, Any]) -> Optional[str]:
    kind = nested_string(payload, "data", "kind")
    if kind == "event":
        return nested_string(payload, "data", "event", "session_id")
    if kind == "turn":
        return nested_string(payload, "data", "turn", "session_id")
    if kind == "session":
        return nested_string(payload, "data", "session", "id")
    return None


def assert_open_search_ids(
    proc: subprocess.Popen[str],
    next_id: int,
    open_ids: list[str],
    expect_session_id: Optional[str],
    expect_open_text: Optional[str],
) -> int:
    expected_session = expected_mcp_session_id(expect_session_id)
    opened_payloads: list[Dict[str, Any]] = []

    for open_id in open_ids:
        open_result = call_tool(proc, next_id, "open", {"id": open_id})
        next_id += 1
        open_payload = assert_structured_content(open_result, "open")
        if nested_string(open_payload, "request", "id") != open_id:
            raise AssertionError(
                f"open request id mismatch: got={nested_string(open_payload, 'request', 'id')} "
                f"want={open_id}"
            )
        open_session_id = open_payload_session_id(open_payload)
        if expected_session is not None and open_session_id != expected_session:
            raise AssertionError(
                "open session mismatch: "
                f"got={open_session_id} want={expected_session}"
            )
        opened_payloads.append(open_payload)

    if expect_open_text is not None and not any(
        contains_text(payload, expect_open_text) for payload in opened_payloads
    ):
        opened_ids = [nested_string(payload, "request", "id") for payload in opened_payloads]
        raise AssertionError(
            f"open responses for search_sessions IDs did not include expected text marker: "
            f"{expect_open_text}; opened={opened_ids}"
        )

    return next_id


def run_smoke(
    moraine: str,
    config: str,
    query: str,
    expect_session_id: Optional[str],
    expect_source_file: Optional[str],
    expect_open_text: Optional[str],
) -> None:
    proc = subprocess.Popen(
        [moraine, "run", "mcp", "--config", config],
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

        tool_names_ordered = [
            tool.get("name")
            for tool in tools
            if isinstance(tool, dict) and isinstance(tool.get("name"), str)
        ]
        if len(tool_names_ordered) != len(tools) or len(tool_names_ordered) != len(
            set(tool_names_ordered)
        ):
            raise AssertionError(f"tools/list returned duplicate or invalid tool names: {tools}")
        assert_tools_surface(tool_names_ordered)

        next_id = 3
        search_result = call_tool(
            proc,
            next_id,
            "search_sessions",
            {
                "query": query,
                "n_hits": 20,
            },
        )
        next_id += 1
        search_payload = assert_structured_content(search_result, "search_sessions")

        results = search_payload["data"].get("results")
        if not isinstance(results, list) or not results:
            raise AssertionError(f"search_sessions returned no results for query={query}")

        selected_result = select_search_sessions_result(
            results,
            expect_session_id,
            expect_open_text,
        )
        next_id = assert_open_search_ids(
            proc,
            next_id,
            open_ids_from_search_result(selected_result),
            expect_session_id,
            expect_open_text,
        )

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
    parser.add_argument("--moraine", required=True)
    parser.add_argument("--config", required=True)
    parser.add_argument("--query", required=True)
    parser.add_argument("--expect-session-id")
    parser.add_argument("--expect-source-file")
    parser.add_argument("--expect-open-text")
    args = parser.parse_args()

    run_smoke(
        args.moraine,
        args.config,
        args.query,
        args.expect_session_id,
        args.expect_source_file,
        args.expect_open_text,
    )
    print("mcp smoke passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
