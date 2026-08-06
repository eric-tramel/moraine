"""Moraine MCP tools exposed as async module methods in Prime Agent."""

from __future__ import annotations

import json
import os
from contextlib import AsyncExitStack
from pathlib import Path
from typing import Any

from rlm import McpIntegration

__all__ = ["Moraine", "moraine"]


class Moraine(McpIntegration):
    """Local stdio integration for the Moraine session-search MCP server."""

    server = "moraine"

    @staticmethod
    def _agent_dir() -> Path:
        configured = os.environ.get("PRIME_AGENT_CODING_AGENT_DIR")
        path = Path(configured or Path.home() / ".prime" / "agent").expanduser()
        if not path.is_absolute():
            raise RuntimeError("PRIME_AGENT_CODING_AGENT_DIR must be an absolute path")
        return path

    @classmethod
    def _settings(cls) -> dict[str, Any]:
        path = cls._agent_dir() / "settings.json"
        try:
            data = json.loads(path.read_text())
        except FileNotFoundError as exc:
            raise RuntimeError(
                "Moraine is not configured for Prime Agent; run `moraine setup --mcp-target prime-agent`."
            ) from exc
        except (OSError, ValueError, TypeError) as exc:
            raise RuntimeError(f"Unable to read Prime Agent Moraine settings from {path}") from exc
        if not isinstance(data, dict):
            raise RuntimeError("Prime Agent settings must contain a JSON object")
        servers = data.get("mcpServers")
        if not isinstance(servers, dict):
            raise RuntimeError("Prime Agent settings are missing the mcpServers object")
        config = servers.get("moraine")
        if not isinstance(config, dict):
            raise RuntimeError("Prime Agent settings are missing mcpServers.moraine")
        if config.get("enabled") is False:
            raise RuntimeError("The Moraine MCP integration is disabled in Prime Agent settings")
        if config.get("type") != "stdio":
            raise RuntimeError("The Moraine Prime Agent integration requires stdio transport")
        command = config.get("command")
        args = config.get("args")
        if not isinstance(command, str) or not command:
            raise RuntimeError("mcpServers.moraine.command must be a non-empty string")
        if not isinstance(args, list) or not all(isinstance(arg, str) for arg in args):
            raise RuntimeError("mcpServers.moraine.args must be an array of strings")
        env = config.get("env")
        if env is not None and (
            not isinstance(env, dict)
            or not all(isinstance(key, str) and isinstance(value, str) for key, value in env.items())
        ):
            raise RuntimeError("mcpServers.moraine.env must be an object of string values")
        return config

    async def _open_session(self, stack: AsyncExitStack):
        from mcp import ClientSession, StdioServerParameters
        from mcp.client.stdio import stdio_client

        config = self._settings()
        configured_env = config.get("env")
        env = None
        if configured_env:
            env = {**os.environ, **configured_env}
        read, write = await stack.enter_async_context(
            stdio_client(
                StdioServerParameters(
                    command=config["command"],
                    args=config["args"],
                    env=env,
                )
            )
        )
        session = await stack.enter_async_context(ClientSession(read, write))
        await session.initialize()
        return session


moraine = Moraine()

_RESERVED = {"run", "__wrapped__", "__call__"}


def __getattr__(name: str):
    if name.startswith("_") or name in _RESERVED:
        raise AttributeError(name)
    return getattr(moraine, name)
