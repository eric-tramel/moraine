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
    if result.get("isError") is not False:
        raise AssertionError(f"tool call did not return isError=false: {result}")
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


def call_tool_expect_handled_error(
    proc: subprocess.Popen[str],
    expected_id: int,
    name: str,
    arguments: Dict[str, Any],
    expected_code: str,
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
    result = assert_rpc_ok(response, expected_id)
    if result.get("isError") is not True:
        raise AssertionError(f"{name} handled error must return isError=true: {result}")
    payload = result.get("structuredContent")
    if not isinstance(payload, dict):
        raise AssertionError(f"{name} handled error structuredContent missing: {result}")
    if payload.get("schema_version") != "moraine.mcp.error.v1":
        raise AssertionError(
            f"{name} handled error has wrong schema_version: "
            f"{payload.get('schema_version')}"
        )
    if payload.get("tool") != name:
        raise AssertionError(
            f"{name} handled error has wrong tool: {payload.get('tool')}"
        )
    error = payload.get("error")
    if not isinstance(error, dict) or error.get("code") != expected_code:
        raise AssertionError(
            f"{name} handled error must return {expected_code}, got: {error}"
        )
    return payload


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
    if tool_names_ordered != [
        "search_sessions",
        "open",
        "list_sessions",
        "file_attention",
    ]:
        raise AssertionError(
            "tools/list must publish the MCP search surface exactly: "
            f"{tool_names_ordered}"
        )

def write_tools_snapshot(path: str, tools_result: Dict[str, Any]) -> None:
    canonical = json.dumps(
        tools_result,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    with open(path, "w", encoding="utf-8", newline="\n") as snapshot:
        snapshot.write(canonical)
        snapshot.write("\n")


def assert_embedded_fallback(stderr: str) -> None:
    if "central MCP server" not in stderr or "using embedded server" not in stderr:
        raise AssertionError(
            "MCP stderr missing the central-server embedded fallback warning; "
            f"stderr={stderr.strip()!r}"
        )



def matching_search_sessions_results(
    results: list[Any],
    expect_session_id: Optional[str],
    expect_open_text: Optional[str],
) -> list[Dict[str, Any]]:
    expected_session = expected_mcp_session_id(expect_session_id)
    matches: list[Dict[str, Any]] = []
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
        if expect_open_text is not None and not contains_text(result, expect_open_text):
            continue
        matches.append(result)
    return matches


def select_search_sessions_result(
    results: list[Any],
    expect_session_id: Optional[str],
    expect_open_text: Optional[str],
) -> Dict[str, Any]:
    candidates = matching_search_sessions_results(
        results,
        expect_session_id,
        None,
    )

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


def select_list_sessions_result(
    sessions: list[Any],
    expect_session_id: Optional[str],
) -> Dict[str, Any]:
    expected_session = expected_mcp_session_id(expect_session_id)

    for result in sessions:
        if not isinstance(result, dict):
            continue
        session_id = nested_string(result, "session", "id")
        open_session_id = nested_string(result, "open", "session_id")
        if expected_session is None or expected_session in {session_id, open_session_id}:
            return result

    debug_sessions = [
        {
            "id": session.get("id"),
            "session_id": nested_string(session, "session", "id"),
            "open_session_id": nested_string(session, "open", "session_id"),
            "updated_at": nested_string(session, "session", "updated_at"),
        }
        for session in sessions
        if isinstance(session, dict)
    ][:5]
    raise AssertionError(
        "list_sessions did not return a session matching expected filters: "
        f"session_id={expect_session_id}, sessions={debug_sessions}"
    )


def open_id_from_list_sessions_result(result: Dict[str, Any]) -> str:
    open_id = nested_string(result, "open", "session_id")
    session_id = nested_string(result, "session", "id")
    if not open_id or open_id != session_id:
        raise AssertionError(f"list_sessions result missing matching open.session_id: {result}")
    for forbidden in ["snippet", "payload_json", "events", "text_content"]:
        if forbidden in result:
            raise AssertionError(f"list_sessions returned non-metadata field {forbidden}: {result}")
    return open_id


def open_ids_from_file_attention_item(result: Dict[str, Any]) -> list[str]:
    handles = result.get("open")
    if not isinstance(handles, dict):
        raise AssertionError(f"file_attention result missing open handles: {result}")

    open_ids: list[str] = []
    for key in ["event_id", "turn_id", "session_id"]:
        open_id = handles.get(key)
        if isinstance(open_id, str) and open_id:
            open_ids.append(open_id)
    if not open_ids:
        raise AssertionError(f"file_attention result had no openable handles: {result}")
    return list(dict.fromkeys(open_ids))


def select_file_attention_result(
    payload: Dict[str, Any],
    expect_session_id: Optional[str],
    expect_root: Optional[str] = None,
    absent_session_ids: Optional[list[str]] = None,
) -> Dict[str, Any]:
    data = payload.get("data")
    if not isinstance(data, dict):
        raise AssertionError(f"file_attention payload missing data: {payload}")
    events = data.get("events")
    if not isinstance(events, list):
        raise AssertionError(f"file_attention events response missing events array: {payload}")
    if not events:
        raise AssertionError(f"file_attention returned no events: {payload}")

    expected_session = expected_mcp_session_id(expect_session_id)
    forbidden_sessions = {
        session_id
        for raw_session_id in absent_session_ids or []
        if (session_id := expected_mcp_session_id(raw_session_id)) is not None
    }
    selected: Optional[Dict[str, Any]] = None
    for event in events:
        if not isinstance(event, dict):
            continue
        session_id = nested_string(event, "event", "session_id")
        open_session_id = nested_string(event, "open", "session_id")
        if forbidden_sessions.intersection({session_id, open_session_id}):
            raise AssertionError(
                "project-scoped file_attention leaked an excluded session: "
                f"event={event}, excluded={sorted(forbidden_sessions)}"
            )
        if expected_session is None or expected_session in {session_id, open_session_id}:
            selected = event

    if selected is not None:
        if expect_root is not None:
            actual_root = nested_string(selected, "event", "worktree_root")
            if actual_root != expect_root:
                raise AssertionError(
                    "file_attention returned the expected session with the wrong root: "
                    f"got={actual_root!r}, wanted={expect_root!r}, event={selected}"
                )
        return selected

    debug_events = [
        {
            "id": event.get("id"),
            "session_id": nested_string(event, "event", "session_id"),
            "open_session_id": nested_string(event, "open", "session_id"),
            "tool_name": nested_string(event, "event", "tool_name"),
        }
        for event in events
        if isinstance(event, dict)
    ][:5]
    raise AssertionError(
        "file_attention did not return a touch matching expected session: "
        f"session_id={expect_session_id}, events={debug_events}"
    )


def open_payload_session_id(payload: Dict[str, Any]) -> Optional[str]:
    kind = nested_string(payload, "data", "kind")
    if kind == "event":
        return nested_string(payload, "data", "event", "session_id")
    if kind == "turn":
        return nested_string(payload, "data", "turn", "session_id")
    if kind == "session":
        return nested_string(payload, "data", "session", "id")
    return None


def open_payload_source(payload: Dict[str, Any]) -> Optional[str]:
    return nested_string(payload, "data", "session", "source")


def assert_open_search_ids(
    proc: subprocess.Popen[str],
    next_id: int,
    open_ids: list[str],
    expect_session_id: Optional[str],
    expect_open_text: Optional[str],
    expect_source: Optional[str] = None,
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
        open_source = open_payload_source(open_payload)
        if expect_source is not None and open_source != expect_source:
            raise AssertionError(
                "open source mismatch: "
                f"id={open_id} got={open_source!r} want={expect_source!r}"
            )
        opened_payloads.append(open_payload)

        kind = nested_string(open_payload, "data", "kind")
        child_field = "turns" if kind == "session" else "events" if kind == "turn" else None
        if child_field is not None:
            summary_children = open_payload["data"].get(child_field)
            if summary_children != [] or open_payload["data"].get("next_cursor") is not None:
                raise AssertionError(
                    f"open({kind}) id-only response must be summary-only: {open_payload}"
                )

            page_result = call_tool(
                proc,
                next_id,
                "open",
                {"id": open_id, "limit": 1},
            )
            next_id += 1
            page_payload = assert_structured_content(page_result, "open")
            opened_payloads.append(page_payload)
            page_children = page_payload["data"].get(child_field)
            if not isinstance(page_children, list) or len(page_children) > 1:
                raise AssertionError(
                    f"open({kind}) bounded page exceeded limit: {page_payload}"
                )
            cursor = page_payload["data"].get("next_cursor")
            seen_cursors: set[str] = set()
            while cursor is not None:
                if not isinstance(cursor, str) or not cursor:
                    raise AssertionError(f"open({kind}) returned invalid cursor: {page_payload}")
                if cursor in seen_cursors:
                    raise AssertionError(f"open({kind}) repeated a cursor: {cursor}")
                seen_cursors.add(cursor)
                continuation_result = call_tool(
                    proc,
                    next_id,
                    "open",
                    {"cursor": cursor},
                )
                next_id += 1
                continuation = assert_structured_content(continuation_result, "open")
                opened_payloads.append(continuation)
                if nested_string(continuation, "request", "cursor") != cursor:
                    raise AssertionError(
                        f"open({kind}) continuation did not echo its cursor: {continuation}"
                    )
                continued_children = continuation["data"].get(child_field)
                if not isinstance(continued_children, list) or len(continued_children) > 1:
                    raise AssertionError(
                        f"open({kind}) continuation exceeded embedded limit: {continuation}"
                    )
                cursor = continuation["data"].get("next_cursor")

    if expect_open_text is not None and not any(
        contains_text(payload, expect_open_text) for payload in opened_payloads
    ):
        opened_ids = [nested_string(payload, "request", "id") for payload in opened_payloads]
        raise AssertionError(
            f"open responses for search_sessions IDs did not include expected text marker: "
            f"{expect_open_text}; opened={opened_ids}"
        )

    return next_id


def assert_sessions_absent(
    results: list[Any],
    absent_session_ids: list[str],
    tool_name: str,
) -> None:
    for raw_session_id in absent_session_ids:
        expected_session = expected_mcp_session_id(raw_session_id)
        for result in results:
            if not isinstance(result, dict):
                continue
            found = {
                nested_string(result, "session", "id"),
                nested_string(result, "open", "session_id"),
            }
            if expected_session in found:
                raise AssertionError(
                    f"{tool_name} leaked out-of-scope session {raw_session_id}: {result}"
                )


def assert_open_not_found(
    proc: subprocess.Popen[str],
    next_id: int,
    raw_session_id: str,
) -> int:
    open_id = expected_mcp_session_id(raw_session_id)
    assert open_id is not None
    call_tool_expect_handled_error(
        proc, next_id, "open", {"id": open_id}, "not_found"
    )
    return next_id + 1


def run_smoke(
    moraine: str,
    config: str,
    query: str,
    expect_session_id: Optional[str],
    expect_open_text: Optional[str],
    file_attention_path: Optional[str] = None,
    project_dir: Optional[str] = None,
    working_dir: Optional[str] = None,
    absent_session_ids: Optional[list[str]] = None,
    file_attention_absent_session_ids: Optional[list[str]] = None,
    expect_no_results: bool = False,
    expect_event_count: Optional[int] = None,
    expect_updated_at: Optional[str] = None,
    expect_matching_search_hits: Optional[int] = None,
    require_embedded_fallback: bool = False,
    tools_snapshot: Optional[str] = None,
) -> None:
    absent_session_ids = absent_session_ids or []
    argv = [moraine, "run", "mcp", "--config", config]
    popen_kwargs: Dict[str, Any] = {}
    launch_dir = project_dir or working_dir
    if project_dir is not None:
        argv.append("--project-only")
    if launch_dir is not None:
        # Mimic a shell launching from the project directory: cwd is the
        # physical path and PWD carries the logical spelling, which may
        # differ through symlinks (e.g. /var vs /private/var on macOS).
        popen_kwargs["cwd"] = launch_dir
        popen_kwargs["env"] = {**os.environ, "PWD": launch_dir}

    proc = subprocess.Popen(
        argv,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
        **popen_kwargs,
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
        if project_dir is not None:
            instructions = init_result.get("instructions")
            if not isinstance(instructions, str) or "scoped" not in instructions:
                raise AssertionError(
                    f"project-only initialize must advertise the scope, got: {instructions!r}"
                )

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
        if tools_snapshot is not None:
            write_tools_snapshot(tools_snapshot, tools_result)

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
        if not isinstance(results, list):
            raise AssertionError(f"search_sessions returned no results array for query={query}")

        assert_sessions_absent(results, absent_session_ids, "search_sessions")
        if expect_matching_search_hits is not None:
            matching_results = matching_search_sessions_results(
                results,
                expect_session_id,
                expect_open_text,
            )
            if len(matching_results) != expect_matching_search_hits:
                raise AssertionError(
                    "search_sessions returned an unexpected number of matching hits: "
                    f"expected={expect_matching_search_hits}, actual={len(matching_results)}, "
                    f"session_id={expect_session_id}, text={expect_open_text!r}"
                )

        selected_result: Optional[Dict[str, Any]] = None
        if expect_no_results:
            if results:
                raise AssertionError(
                    f"search_sessions must return no results for query={query}, got: {results[:3]}"
                )
        else:
            if not results:
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

        list_result = call_tool(
            proc,
            next_id,
            "list_sessions",
            {
                "start_datetime": "2026-02-16T11:59:00Z",
                "end_datetime": "2026-02-16T12:01:00Z",
                "limit": 20,
            },
        )
        next_id += 1
        list_payload = assert_structured_content(list_result, "list_sessions")
        sessions = list_payload["data"].get("sessions")
        if not isinstance(sessions, list):
            raise AssertionError("list_sessions returned no sessions array")

        assert_sessions_absent(sessions, absent_session_ids, "list_sessions")

        # Verifying the in-scope session is reachable runs even in the
        # expect_no_results case (where the search query is for an out-of-scope
        # keyword): it proves the scoped server is genuinely live and serving,
        # so an empty search result reflects scoping rather than a dead backend.
        if expect_session_id is not None:
            if not sessions:
                raise AssertionError(
                    "list_sessions returned no sessions for the in-scope fixture window"
                )
            selected_session = select_list_sessions_result(sessions, expect_session_id)
            session_metadata = selected_session.get("session")
            if not isinstance(session_metadata, dict):
                raise AssertionError(
                    f"list_sessions result missing session metadata: {selected_session}"
                )
            if (
                expect_event_count is not None
                and session_metadata.get("event_count") != expect_event_count
            ):
                raise AssertionError(
                    "list_sessions event_count parity failed: "
                    f"got {session_metadata.get('event_count')}, "
                    f"wanted {expect_event_count}"
                )
            if (
                expect_updated_at is not None
                and session_metadata.get("updated_at") != expect_updated_at
            ):
                raise AssertionError(
                    "list_sessions updated_at parity failed: "
                    f"got {session_metadata.get('updated_at')!r}, "
                    f"wanted {expect_updated_at!r}"
                )
            listed_source = nested_string(selected_session, "session", "source")
            if listed_source is None:
                raise AssertionError(
                    f"list_sessions result missing configured ingest source: {selected_session}"
                )
            listed_open_id = open_id_from_list_sessions_result(selected_session)
            open_ids = [listed_open_id]
            if selected_result is not None:
                open_ids.extend(open_ids_from_search_result(selected_result))
                open_ids = list(dict.fromkeys(open_ids))
            next_id = assert_open_search_ids(
                proc,
                next_id,
                open_ids,
                expect_session_id,
                expect_open_text,
                listed_source,
            )
        elif not sessions and not expect_no_results:
            raise AssertionError("list_sessions returned no sessions for e2e fixture window")

        for raw_session_id in absent_session_ids:
            next_id = assert_open_not_found(proc, next_id, raw_session_id)

        if file_attention_path is not None:
            file_attention_result = call_tool(
                proc,
                next_id,
                "file_attention",
                {
                    "path": file_attention_path,
                    "scope": "project" if launch_dir is not None else "all",
                    "granularity": "events",
                    "limit": 10,
                },
            )
            next_id += 1
            file_attention_payload = assert_structured_content(
                file_attention_result, "file_attention"
            )
            selected_touch = select_file_attention_result(
                file_attention_payload,
                expect_session_id,
                expect_root=launch_dir if launch_dir is not None else None,
                absent_session_ids=file_attention_absent_session_ids,
            )
            next_id = assert_open_search_ids(
                proc,
                next_id,
                open_ids_from_file_attention_item(selected_touch),
                expect_session_id,
                None,
            )

        if require_embedded_fallback:
            assert_embedded_fallback(
                collect_stderr(proc, wait_seconds=0.5, max_bytes=65_536)
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
    parser.add_argument("--expect-open-text")
    parser.add_argument(
        "--file-attention-path",
        help="also call file_attention for this path and open one returned touch",
    )
    parser.add_argument(
        "--project-dir",
        help=(
            "run `moraine run mcp --project-only` from this directory and "
            "assert the initialize response advertises the scope"
        ),
    )
    parser.add_argument(
        "--working-dir",
        help=(
            "launch `moraine run mcp` from this directory without enabling "
            "--project-only (used to exercise daemon-owned backend routing)"
        ),
    )
    parser.add_argument(
        "--expect-absent-session-id",
        action="append",
        default=[],
        help=(
            "raw session id that must NOT appear in search_sessions or "
            "list_sessions results, and whose open must return not_found "
            "(repeatable)"
        ),
    )
    parser.add_argument(
        "--file-attention-expect-absent-session-id",
        action="append",
        default=[],
        help=(
            "raw session id that must not appear in the file_attention event "
            "timeline (repeatable)"
        ),
    )
    parser.add_argument(
        "--expect-no-results",
        action="store_true",
        help="assert search_sessions returns zero results for the query",
    )
    parser.add_argument(
        "--expect-event-count",
        type=int,
        help="expected canonical event_count for the selected list_sessions fixture",
    )
    parser.add_argument(
        "--expect-matching-search-hits",
        type=int,
        help=(
            "expected search_sessions hit count after filtering by "
            "--expect-session-id and --expect-open-text"
        ),
    )
    parser.add_argument(
        "--expect-updated-at",
        help="expected RFC3339 updated_at for the selected list_sessions fixture",
    )
    parser.add_argument(
        "--require-embedded-fallback",
        action="store_true",
        help=(
            "require stderr to report that the unreachable central MCP server "
            "fell back to an embedded server"
        ),
    )
    parser.add_argument(
        "--write-tools-snapshot",
        help=(
            "write the tools/list result as canonical deterministic JSON "
            "(sorted object keys, compact separators, one trailing newline)"
        ),
    )
    args = parser.parse_args()
    if args.project_dir and args.working_dir:
        parser.error("--project-dir and --working-dir are mutually exclusive")

    run_smoke(
        args.moraine,
        args.config,
        args.query,
        args.expect_session_id,
        args.expect_open_text,
        file_attention_path=args.file_attention_path,
        project_dir=args.project_dir,
        working_dir=args.working_dir,
        absent_session_ids=args.expect_absent_session_id,
        file_attention_absent_session_ids=(
            args.file_attention_expect_absent_session_id
        ),
        expect_no_results=args.expect_no_results,
        expect_event_count=args.expect_event_count,
        expect_updated_at=args.expect_updated_at,
        expect_matching_search_hits=args.expect_matching_search_hits,
        require_embedded_fallback=args.require_embedded_fallback,
        tools_snapshot=args.write_tools_snapshot,
    )
    print("mcp smoke passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
