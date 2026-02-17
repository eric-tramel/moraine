#!/usr/bin/env python3
import argparse
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


def select_hit(
    hits: list[Any],
    expect_session_id: Optional[str],
    expect_source_file: Optional[str],
) -> Dict[str, Any]:
    for hit in hits:
        if not isinstance(hit, dict):
            continue
        if expect_session_id is not None and hit.get("session_id") != expect_session_id:
            continue
        if expect_source_file is not None:
            source_ref = hit.get("source_ref")
            if not isinstance(source_ref, str) or expect_source_file not in source_ref:
                continue
        return hit

    debug_hits = [
        {
            "event_uid": hit.get("event_uid"),
            "session_id": hit.get("session_id"),
            "source_ref": hit.get("source_ref"),
        }
        for hit in hits
        if isinstance(hit, dict)
    ][:5]
    raise AssertionError(
        "search did not return a hit matching expected filters: "
        f"session_id={expect_session_id}, source_file={expect_source_file}, "
        f"hits={debug_hits}"
    )


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
                        "limit": 20,
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

        selected_hit = select_hit(hits, expect_session_id, expect_source_file)
        event_uid = selected_hit.get("event_uid")
        if not isinstance(event_uid, str) or not event_uid:
            raise AssertionError("selected search hit missing event_uid")

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
        if open_payload.get("found") is not True:
            raise AssertionError(f"open did not find event_uid={event_uid}: {open_payload}")
        if expect_session_id is not None and open_payload.get("session_id") != expect_session_id:
            raise AssertionError(
                f"open session mismatch: got={open_payload.get('session_id')} want={expect_session_id}"
            )

        events = open_payload.get("events")
        if not isinstance(events, list) or not events:
            raise AssertionError("open returned no context events")

        if not any(
            isinstance(event, dict) and event.get("event_uid") == event_uid for event in events
        ):
            raise AssertionError("open response did not include requested event_uid")
        if expect_source_file is not None and not any(
            isinstance(event, dict)
            and isinstance(event.get("source_ref"), str)
            and expect_source_file in event.get("source_ref", "")
            for event in events
        ):
            raise AssertionError(
                f"open response did not include expected source file: {expect_source_file}"
            )
        if expect_open_text is not None and not any(
            isinstance(event, dict)
            and isinstance(event.get("text_content"), str)
            and expect_open_text in event.get("text_content", "")
            for event in events
        ):
            raise AssertionError(
                f"open response did not include expected text marker: {expect_open_text}"
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
