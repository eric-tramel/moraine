import json
import multiprocessing as mp
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import pytest

from moraine_conversations import ConversationClient


def _session_rows() -> list[dict]:
    return [
        {
            "session_id": "sess_c",
            "first_event_time": "2026-01-03 10:00:00",
            "first_event_unix_ms": 1767434400000,
            "last_event_time": "2026-01-03 10:10:00",
            "last_event_unix_ms": 1767435000000,
            "total_turns": 3,
            "total_events": 30,
            "user_messages": 6,
            "assistant_messages": 6,
            "tool_calls": 3,
            "tool_results": 3,
            "mode": "web_search",
        },
        {
            "session_id": "sess_b",
            "first_event_time": "2026-01-02 10:00:00",
            "first_event_unix_ms": 1767348000000,
            "last_event_time": "2026-01-02 10:10:00",
            "last_event_unix_ms": 1767348600000,
            "total_turns": 2,
            "total_events": 22,
            "user_messages": 4,
            "assistant_messages": 4,
            "tool_calls": 2,
            "tool_results": 2,
            "mode": "web_search",
        },
        {
            "session_id": "sess_a",
            "first_event_time": "2026-01-01 10:00:00",
            "first_event_unix_ms": 1767261600000,
            "last_event_time": "2026-01-01 10:10:00",
            "last_event_unix_ms": 1767262200000,
            "total_turns": 2,
            "total_events": 20,
            "user_messages": 4,
            "assistant_messages": 4,
            "tool_calls": 2,
            "tool_results": 2,
            "mode": "web_search",
        },
    ]


def _rows_for_query(query: str) -> list[dict]:
    if "FROM `moraine`.`v_session_summary` AS s" in query:
        if "WHERE s.session_id = 'sess_a'" in query:
            return [_session_rows()[2]]
        return _session_rows()

    if "FROM `moraine`.`search_corpus_stats`" in query:
        return [{"docs": 100, "total_doc_len": 5000}]

    if "FROM `moraine`.`search_term_stats`" in query:
        return [{"term": "hello", "df": 20}, {"term": "world", "df": 10}]

    if "GROUP BY e.session_id" in query:
        return [
            {
                "session_id": "sess_c",
                "score": 12.5,
                "matched_terms": 2,
                "event_count_considered": 3,
                "best_event_uid": "evt-c-42",
                "snippet": "best match from session c",
            },
            {
                "session_id": "sess_a",
                "score": 7.0,
                "matched_terms": 1,
                "event_count_considered": 2,
                "best_event_uid": "evt-a-11",
                "snippet": "weaker match from session a",
            },
        ]

    if "GROUP BY p.doc_id" in query and "ORDER BY score DESC, event_uid ASC" in query:
        return [
            {
                "event_uid": "evt-c-42",
                "session_id": "sess_c",
                "source_name": "codex",
                "provider": "codex",
                "event_class": "message",
                "payload_type": "text",
                "actor_role": "assistant",
                "name": "",
                "phase": "",
                "source_ref": "/tmp/sess_c.jsonl:1:42",
                "doc_len": 19,
                "text_preview": "best event in session c",
                "score": 12.5,
                "matched_terms": 2,
            },
            {
                "event_uid": "evt-a-11",
                "session_id": "sess_a",
                "source_name": "codex",
                "provider": "codex",
                "event_class": "message",
                "payload_type": "text",
                "actor_role": "assistant",
                "name": "",
                "phase": "",
                "source_ref": "/tmp/sess_a.jsonl:1:11",
                "doc_len": 13,
                "text_preview": "weaker event in session a",
                "score": 7.0,
                "matched_terms": 1,
            },
        ]

    return []


def _run_mock_clickhouse(port_queue: mp.Queue) -> None:
    class Handler(BaseHTTPRequestHandler):
        def _handle(self) -> None:
            parsed = urlparse(self.path)
            query = parse_qs(parsed.query).get("query", [""])[0]
            rows = _rows_for_query(query)
            body = json.dumps({"data": rows}).encode("utf-8")

            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:  # noqa: N802
            self._handle()

        def do_POST(self) -> None:  # noqa: N802
            self._handle()

        def log_message(self, format: str, *args) -> None:  # noqa: A003
            del format
            del args

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    port_queue.put(server.server_address[1])
    server.serve_forever()


def test_conversation_client_smoke() -> None:
    port_queue: mp.Queue[int] = mp.Queue()
    proc = mp.Process(target=_run_mock_clickhouse, args=(port_queue,), daemon=True)
    proc.start()

    try:
        port = port_queue.get(timeout=5)
        client = ConversationClient(
            url=f"http://127.0.0.1:{port}",
            database="moraine",
            timeout_seconds=2.0,
        )

        conversations = json.loads(
            client.list_conversations_json(
                from_unix_ms=1767261600000,
                to_unix_ms=1767500000000,
                mode="web_search",
                limit=2,
            )
        )
        assert len(conversations["items"]) == 2
        assert conversations["items"][0]["session_id"] == "sess_c"
        assert conversations["next_cursor"] is not None

        conversation = json.loads(client.get_conversation_json("sess_a"))
        assert conversation["summary"]["session_id"] == "sess_a"
        assert conversation["turns"] == []

        search = json.loads(
            client.search_conversations_json(
                query="hello world",
                limit=10,
                min_score=0.0,
                min_should_match=1,
                from_unix_ms=1767261600000,
                to_unix_ms=1767500000000,
                mode="chat",
                include_tool_events=True,
                exclude_codex_mcp=False,
            )
        )
        assert len(search["hits"]) == 2
        assert search["hits"][0]["session_id"] == "sess_c"

        event_search = json.loads(
            client.search_events_json(
                query="hello world",
                limit=10,
                session_id="sess_c",
                min_score=0.0,
                min_should_match=1,
                include_tool_events=True,
                exclude_codex_mcp=False,
                search_strategy="optimized",
            )
        )
        assert len(event_search["hits"]) == 2
        assert event_search["hits"][0]["event_uid"] == "evt-c-42"

        with pytest.raises(ValueError):
            client.search_events_json(query="hello world", search_strategy="not_a_strategy")
    finally:
        proc.terminate()
        proc.join(timeout=5)
