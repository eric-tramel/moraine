# Moraine

![Moraine Banner](https://github.com/user-attachments/assets/efa723e6-bcc9-4402-bbdf-083297f7e1e2)

[![Docs](https://github.com/eric-tramel/moraine/actions/workflows/docs-deploy.yml/badge.svg)](https://eric-tramel.github.io/moraine/)

Moraine is a local trace stack for agent work. It indexes sessions from agent
harnesses such as Codex, Claude Code, Kimi CLI, Hermes, and Pi Coding Agent into ClickHouse,
serves a monitor UI, and exposes MCP retrieval over the indexed history.

Agents get searchable long-term memory through MCP. You get a unified local
record of what happened across providers, including tools, tokens, and
conversation history.

Moraine is under active development. Config keys, schemas, and MCP tools can
change across minor releases.

## Screenshots

<img width="1635" height="1170" alt="Moraine monitor session overview" src="https://github.com/user-attachments/assets/b1e67007-aa73-4c31-80b9-79c0d4fb91da" />

<img width="1635" height="1170" alt="Moraine monitor trace details" src="https://github.com/user-attachments/assets/f7ec8582-8d3c-4745-bdbe-a2fe71df6004" />

## Documentation

- [Introduction](https://eric-tramel.github.io/moraine/)
- [Quickstart and Installation](https://eric-tramel.github.io/moraine/quickstart.html)
- [Configuration](https://eric-tramel.github.io/moraine/configuration.html)
- [Agent MCP Search](https://eric-tramel.github.io/moraine/agent-mcp-search/index.html)

## Supported Agent Harnesses

Moraine ships session trace ingestion adapters for these agent harnesses:

| Harness | Config value | Session traces ingested |
| --- | --- | --- |
| [Codex](https://developers.openai.com/codex) | `codex` | JSONL session files under `~/.codex/sessions` |
| [Claude Code](https://code.claude.com/docs/en/overview) | `claude-code` | JSONL project session files under `~/.claude/projects` |
| [Kimi CLI](https://moonshotai.github.io/kimi-cli/en/) | `kimi-cli` | `wire.jsonl` session traces under `~/.kimi/sessions` |
| [Cursor](https://cursor.com/docs) | `cursor` | Agent transcript JSONL under `~/.cursor/projects` (default on); Cursor IDE chat history from `state.vscdb` SQLite databases (default on; `cursor_sqlite` format) |
| [Hermes](https://hermes-agent.nousresearch.com/docs/) | `hermes` | Live session JSON and trajectory JSONL traces |
| [Pi Coding Agent](https://pi.dev/docs/latest) | `pi-coding-agent` | JSONL session trees under `~/.pi/agent/sessions` |

## Quickstart

Install from PyPI with `uv`:

```bash
uv tool install moraine-cli
```

Or install the latest release bundle:

```bash
curl -fsSL https://raw.githubusercontent.com/eric-tramel/moraine/main/scripts/install.sh | sh
```

Start the local stack:

```bash
moraine up
moraine status
```

The monitor UI runs at `http://127.0.0.1:8080` by default.

## Agent Harness Guidance

Moraine is most useful when your agent harness knows that it can search prior
sessions. Add the following guidance to your global harness instructions, such
as `~/.codex/AGENTS.md` for Codex or `~/.claude/CLAUDE.md` for Claude Code:

```markdown
- You have access to all past agent sessions (whether codex, claude, hermes, etc., anything) via moraine search tools.
- Any time the user asks about information not located within your context, that you can't see or don't know, but implies that you *should* know, it is because it was a past conversation. You can reference and search for this conversation with moraine search tools or session listing.
- Moraine search tooling is built around BM25 keyword search, so target your queries to keywords rather than questions. There is no semantic search.
- Successful keyword searches often go from broad to narrow. Sometimes this narrowing isn't successful, in which case, you can always back up and try a different path.
- Moraine has a *real-time* view of agent sessions. You can use it to peek on active AI agent sessions, including your own, sessions running in different harnesses, sessions that are sub-agents of your current conversation, or even their subagents. All agent work, in realtime, is visible to you.
```

With this you can do operations like the following:

```
claude -p "What are my agents doing right now?"
codex exec "What are my agents doing right now?"
```

## Development

```bash
cargo build --workspace --locked
cargo test --workspace --locked
cargo fmt --all -- --check
```

The repository-managed pre-commit hook can be installed with:

```bash
make hooks-install
```
