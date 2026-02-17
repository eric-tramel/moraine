# Python Conversations Client Tutorial

This tutorial shows how to use the `moraine-conversations` Python binding as a high-level read-only client for Moraine conversation data. The binding exports `ConversationClient` and provides three primary methods: `list_conversations_json`, `get_conversation_json`, and `search_conversations_json`. [src: bindings/python/moraine_conversations/python/moraine_conversations/__init__.py:L1-L3, bindings/python/moraine_conversations/src/lib.rs:L62-L140]

## Prerequisites

You need a running Moraine stack (or any reachable ClickHouse endpoint with the Moraine schema) and Python 3.9+. A standard local setup is to start the stack with `bin/moraine up`. The binding package itself is configured for Python `>=3.9` and uses `maturin` as the build backend. [src: docs/operations/build-and-operations.md:L89-L104, bindings/python/moraine_conversations/pyproject.toml:L1-L10]

## Install The Binding Locally

From the repo root:

```bash
cd bindings/python/moraine_conversations
python3 -m venv .venv
source .venv/bin/activate
pip install maturin
maturin develop
```

At that point, `import moraine_conversations` loads the compiled extension module in your virtualenv. [src: bindings/python/moraine_conversations/README.md:L5-L17, bindings/python/moraine_conversations/pyproject.toml:L20-L23]

## Create A Client

```python
from moraine_conversations import ConversationClient

client = ConversationClient(
    url="http://127.0.0.1:8123",
    database="moraine",
    timeout_seconds=5.0,
)
```

The constructor accepts `url`, `database`, `username`, `password`, `timeout_seconds`, and `max_results`. [src: bindings/python/moraine_conversations/src/lib.rs:L18-L60]

## List Conversations By Date And Mode

```python
import json

page = json.loads(
    client.list_conversations_json(
        from_unix_ms=1767261600000,
        to_unix_ms=1767500000000,
        mode="web_search",
        limit=50,
        cursor=None,
    )
)

for convo in page["items"]:
    print(convo["session_id"], convo["last_event_time"], convo["mode"])

next_cursor = page["next_cursor"]
```

`mode` must be one of `web_search`, `mcp_internal`, `tool_calling`, or `chat`. Results are paginated with `next_cursor`. [src: bindings/python/moraine_conversations/src/lib.rs:L62-L85, bindings/python/moraine_conversations/src/lib.rs:L148-L155]

## Retrieve One Conversation By ID

```python
import json

conversation = json.loads(client.get_conversation_json("sess_a", include_turns=True))
if conversation is None:
    print("not found")
else:
    print(conversation["summary"]["session_id"])
    print(len(conversation["turns"]))
```

The method returns JSON for `Option<Conversation>`, so a missing session is `null` in Python after `json.loads`. [src: bindings/python/moraine_conversations/src/lib.rs:L87-L98]

## Search Conversations (Whole-Conversation Ranking)

```python
import json

results = json.loads(
    client.search_conversations_json(
        query="vector store migration",
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

for hit in results["hits"]:
    print(hit["session_id"], hit["score"], hit.get("best_event_uid"))
```

This search is ranked at the conversation/session level, not only single events. Internally, event scores are aggregated by `session_id`, and the best event per session is carried through as `best_event_uid` plus a snippet. [src: bindings/python/moraine_conversations/src/lib.rs:L100-L140, crates/moraine-conversations/src/clickhouse_repo.rs:L592-L620]

## Run The Python Smoke Test

```bash
cd bindings/python/moraine_conversations
source .venv/bin/activate
pip install pytest
CARGO_HOME=/tmp/cargo-home maturin develop
pytest -q tests/test_smoke.py
```

The smoke test exercises list/get/search end-to-end against a mock ClickHouse HTTP endpoint. [src: bindings/python/moraine_conversations/tests/test_smoke.py:L120-L166, bindings/python/moraine_conversations/pyproject.toml:L17-L18]

## Error Semantics

Invalid mode values raise a Python `ValueError`. Backend or query failures are surfaced as Python `RuntimeError` values with the underlying error text. [src: bindings/python/moraine_conversations/src/lib.rs:L7-L8, bindings/python/moraine_conversations/src/lib.rs:L153-L160]
