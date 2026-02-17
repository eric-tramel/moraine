# moraine-conversations (Python)

PyO3 + maturin bindings for `moraine-conversations`.

## Quick start

```bash
cd bindings/python/moraine_conversations
maturin develop
```

```python
from moraine_conversations import ConversationClient

client = ConversationClient(url="http://127.0.0.1:8123", database="moraine")
print(client.search_conversations_json(query="agent tool use", limit=5))
```

## Smoke test (pytest)

```bash
cd bindings/python/moraine_conversations
python3 -m venv .venv
source .venv/bin/activate
pip install maturin pytest
maturin develop
pytest -q tests/test_smoke.py
```
