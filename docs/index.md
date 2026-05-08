# Moraine

Moraine is a local trace stack for agent work. It watches agent session files,
normalizes them into a ClickHouse database, serves a monitor UI, and exposes an
MCP server so agents can search prior sessions.

Use Moraine when you want a private record of what your agents did across
harnesses such as Codex, Claude Code, Cursor, Kimi CLI, Hermes, and Pi Coding
Agent. The default setup runs on your machine and writes runtime state under
`~/.moraine`.

## What You Get

- **Unified trace database.** Conversation turns, tool calls, token counts, and
  timestamps land in ClickHouse under a consistent schema.
- **Realtime local ingest.** Moraine watches Codex, Claude Code, Cursor, Kimi
  CLI, Hermes, and Pi Coding Agent session files and backfills existing history
  on startup.
- **Monitor UI.** Browse sessions, inspect indexing health, and check what has
  been captured at `http://127.0.0.1:8080`.
- **MCP retrieval.** Agents can search prior decisions, fixes, errors, and
  session context through `moraine run mcp`.
- **Direct database access.** Query `moraine.events` and related views with your
  own SQL, dashboards, or experiments.
- **Fully local by default.** Runtime state lives under `~/.moraine`; nothing
  leaves your machine unless you point Moraine at remote infrastructure.

Moraine is under active development. Config keys, schemas, and MCP tool names
can change across minor releases.

## Supported Agent Harnesses

Moraine ships session trace ingestion adapters for these agent harnesses:

| Harness | Config value | Session traces ingested |
| --- | --- | --- |
| [Codex](https://developers.openai.com/codex) | `codex` | JSONL session files under `~/.codex/sessions` |
| [Claude Code](https://code.claude.com/docs/en/overview) | `claude-code` | JSONL project session files under `~/.claude/projects` |
| [Kimi CLI](https://moonshotai.github.io/kimi-cli/en/) | `kimi-cli` | `wire.jsonl` session traces under `~/.kimi/sessions` |
| [Hermes](https://hermes-agent.nousresearch.com/docs/) | `hermes` | Live session JSON and trajectory JSONL traces |
| [Pi Coding Agent](https://pi.dev/docs/latest) | `pi-coding-agent` | JSONL session trees under `~/.pi/agent/sessions` |

## Where To Start

Read [Quickstart and Installation](quickstart.md) to install Moraine and start a
local stack.

Read [Configuration](configuration.md) when you need to change watched sources,
ports, ClickHouse settings, MCP defaults, or runtime paths.

Read [MCP Search Interface Specification](mcp-search-interface-spec.md) for the
concrete `search_sessions` and `open` tool contracts.
