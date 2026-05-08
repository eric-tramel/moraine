# Agent MCP Search

Moraine gives agent harnesses a shared, local memory over prior agent sessions.
The MCP server exposes that memory as read-only search and navigation tools, so
Codex, Claude Code, Hermes, Cursor, Kimi CLI, Pi, and other MCP clients can find
past decisions, commands, errors, and active agent work without copying session
logs by hand.

Start with these pages:

- [Install by Harness](install.md) shows how to connect common clients to
  `moraine run mcp`.
- [Patterns](patterns.md) explains the system instructions and search habits
  that make lexical session search work well.
- [MCP Interface](interface.md) explains what the tools are, what they return,
  and how agents should move from a search hit to a session record.
- [MCP Search Interface Specification](../mcp-search-interface-spec.md) is the
  full contract reference for implementers and tests.

Moraine does not replace the harness's own context window. It gives the harness
a retrieval path into the local trace store. The useful workflow is usually:
search for a clue, open the exact event, expand to the surrounding turn, then
open the whole session only when the broader context matters.
