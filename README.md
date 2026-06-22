# Moraine

![Moraine Banner](https://github.com/user-attachments/assets/efa723e6-bcc9-4402-bbdf-083297f7e1e2)

[![Docs](https://github.com/eric-tramel/moraine/actions/workflows/docs-deploy.yml/badge.svg)](https://eric-tramel.github.io/moraine/)

Moraine is a local trace stack for agent work. It indexes sessions from agent
harnesses such as Codex, Claude Code, Kimi CLI, OpenCode, Hermes, and Pi Coding Agent into ClickHouse,
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

- [Home](https://eric-tramel.github.io/moraine/)
- [Introduction](https://eric-tramel.github.io/moraine/introduction.html)
- [Quickstart and Installation](https://eric-tramel.github.io/moraine/quickstart.html)
- [Configuration](https://eric-tramel.github.io/moraine/configuration.html)
- [Remote ClickHouse](https://eric-tramel.github.io/moraine/remote-clickhouse.html)
- [Agent MCP Search](https://eric-tramel.github.io/moraine/agent-mcp-search/index.html)

## Supported Agent Harnesses

Moraine ships session trace ingestion adapters for these agent harnesses:

| Harness | Config value | Session traces ingested |
| --- | --- | --- |
| [Codex](https://developers.openai.com/codex) | `codex` | JSONL session files under `~/.codex/sessions` |
| [Claude Code](https://code.claude.com/docs/en/overview) | `claude-code` | JSONL project session files under `~/.claude/projects` |
| [Kimi CLI](https://moonshotai.github.io/kimi-cli/en/) | `kimi-cli` | `wire.jsonl` session traces under `~/.kimi/sessions` |
| [OpenCode](https://opencode.ai/) | `opencode` | SQLite session history from `~/.local/share/opencode/opencode*.db` (default on; `opencode_sqlite` format) |
| [Cursor](https://cursor.com/docs) | `cursor` | Agent transcript JSONL under `~/.cursor/projects` (default on); Cursor IDE chat history from `state.vscdb` SQLite databases (default on; `cursor_sqlite` format) |
| [Hermes](https://hermes-agent.nousresearch.com/docs/) | `hermes` | Live session JSON and trajectory JSONL traces |
| [Pi Coding Agent](https://pi.dev/docs/latest) | `pi-coding-agent` | JSONL session trees under `~/.pi/agent/sessions` |

## Quickstart

```bash
uv tool install moraine-cli
moraine setup
moraine up
```

`moraine setup` creates or repairs `~/.moraine/config.toml` and guides plugin or
MCP registration for detected agent harnesses.

The monitor UI runs at `http://127.0.0.1:8080` by default.

For release bundles, upgrades, project-scoped setup, and other harnesses, see the
[Quickstart and Installation](https://eric-tramel.github.io/moraine/quickstart.html).

## Connect Agent Harnesses

Use `moraine setup` to install or update the Moraine plugins for Claude Code,
Codex, and Hermes, or to register Moraine MCP for supported harnesses such as
OpenCode, Cursor, Kimi CLI, and Pi Coding Agent. The integrations use the
`moraine` CLI on your `PATH` and the running local stack.

Start a new agent session after installing an integration. Claude Code, Codex, and
Hermes sessions get the `moraine:session-search` and `moraine:realtime-peek`
guidance, and Moraine MCP tools are exposed with each harness's MCP naming
scheme. Then ask:

```text
What are my agents doing right now?
```

The user-scoped plugins can search the host-wide Moraine history visible to your
user. For project-scoped setup, duplicate MCP cleanup, and other clients, see
[Agent MCP Search](https://eric-tramel.github.io/moraine/agent-mcp-search/index.html).

## Agent Harness Guidance

The Claude Code, Codex, and Hermes plugins already bundle Moraine search
guidance. If you use manual MCP registration or another harness, add the
following guidance to your global harness instructions, such as
`~/.codex/AGENTS.md` for Codex or `~/.claude/CLAUDE.md` for Claude Code:

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
hermes -z "What are my agents doing right now?"
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

To put the current checkout (tip-of-branch, no tagged release needed) onto your
host for testing, build and install it over the active `moraine` on your PATH:

```bash
make install
```

This release-builds the host target and installs through the same
`scripts/install.sh` path end users get. ClickHouse is skipped by default
(`make install INSTALL_ARGS="--with-clickhouse"` to include it). See
`scripts/dev/install-host.sh --help` for the full set of options.
