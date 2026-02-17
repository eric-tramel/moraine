# cortex-conversations (Python)

PyO3 + maturin bindings for `cortex-conversations`.

## Quick start

```bash
cd bindings/python/cortex_conversations
maturin develop
```

```python
from cortex_conversations import ConversationClient

client = ConversationClient(url="http://127.0.0.1:8123", database="cortex")
print(client.search_conversations_json(query="agent tool use", limit=5))
```
