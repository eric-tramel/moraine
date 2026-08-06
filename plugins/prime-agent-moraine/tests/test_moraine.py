from __future__ import annotations

import asyncio
import importlib.util
import json
import os
import sys
import tempfile
import types
import unittest
from contextlib import asynccontextmanager
from pathlib import Path
from unittest.mock import patch


class FakeMcpIntegration:
    def __getattr__(self, name):
        async def tool(**kwargs):
            return name, kwargs

        return tool


def load_skill():
    rlm = types.ModuleType("rlm")
    rlm.McpIntegration = FakeMcpIntegration
    sys.modules["rlm"] = rlm
    path = Path(__file__).parents[1] / "skills/moraine/src/moraine/__init__.py"
    spec = importlib.util.spec_from_file_location("prime_moraine_test", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class MoraineSkillTests(unittest.TestCase):
    def setUp(self):
        self.modules = patch.dict(sys.modules, {})
        self.modules.start()
        self.module = load_skill()
        self.temp = tempfile.TemporaryDirectory()
        self.agent = Path(self.temp.name) / "agent"
        self.agent.mkdir()
        self.env = patch.dict(
            os.environ, {"PRIME_AGENT_CODING_AGENT_DIR": str(self.agent)}, clear=False
        )
        self.env.start()

    def tearDown(self):
        self.env.stop()
        self.temp.cleanup()
        self.modules.stop()

    def write_settings(self, value):
        (self.agent / "settings.json").write_text(json.dumps(value))

    def test_settings_validation_is_actionable(self):
        with self.assertRaisesRegex(RuntimeError, "moraine setup --mcp-target prime-agent"):
            self.module.Moraine._settings()
        self.write_settings({"mcpServers": {"moraine": {"enabled": False}}})
        with self.assertRaisesRegex(RuntimeError, "disabled"):
            self.module.Moraine._settings()
        self.write_settings(
            {"mcpServers": {"moraine": {"type": "http", "command": "x", "args": []}}}
        )
        with self.assertRaisesRegex(RuntimeError, "stdio"):
            self.module.Moraine._settings()

    def test_settings_accepts_only_complete_stdio_configuration(self):
        expected = {
            "type": "stdio",
            "command": "/opt/moraine/bin/moraine-mcp",
            "args": ["--config", "/tmp/moraine.toml", "--serve", "stdio"],
            "enabled": True,
            "env": {"SAFE": "value"},
        }
        self.write_settings({"theme": "dark", "mcpServers": {"moraine": expected}})
        self.assertEqual(self.module.Moraine._settings(), expected)

    def test_reserved_attributes_are_not_forwarded(self):
        with self.assertRaises(AttributeError):
            self.module.__getattr__("run")
        with self.assertRaises(AttributeError):
            self.module.__getattr__("_private")
        forwarded = self.module.__getattr__("search_sessions")
        self.assertEqual(
            asyncio.run(forwarded(query="prime")),
            ("search_sessions", {"query": "prime"}),
        )

    def test_stdio_session_uses_configured_argv_and_environment(self):
        captured = {}
        mcp = types.ModuleType("mcp")
        client = types.ModuleType("mcp.client")
        stdio = types.ModuleType("mcp.client.stdio")

        class Parameters:
            def __init__(self, *, command, args, env):
                captured.update(command=command, args=args, env=env)

        class Session:
            def __init__(self, read, write):
                captured.update(read=read, write=write)

            async def __aenter__(self):
                return self

            async def __aexit__(self, *_):
                captured["session_closed"] = True

            async def initialize(self):
                captured["initialized"] = True

        @asynccontextmanager
        async def stdio_client(parameters):
            captured["parameters"] = parameters
            yield "read", "write"
            captured["stdio_closed"] = True

        mcp.ClientSession = Session
        mcp.StdioServerParameters = Parameters
        stdio.stdio_client = stdio_client
        sys.modules.update(
            {"mcp": mcp, "mcp.client": client, "mcp.client.stdio": stdio}
        )
        self.write_settings(
            {
                "mcpServers": {
                    "moraine": {
                        "type": "stdio",
                        "command": "/trusted/moraine-mcp",
                        "args": ["--serve", "stdio"],
                        "env": {"MORAINE_TEST": "yes"},
                    }
                }
            }
        )

        async def exercise():
            from contextlib import AsyncExitStack

            async with AsyncExitStack() as stack:
                session = await self.module.Moraine()._open_session(stack)
                self.assertIsInstance(session, Session)

        asyncio.run(exercise())
        self.assertEqual(captured["command"], "/trusted/moraine-mcp")
        self.assertEqual(captured["args"], ["--serve", "stdio"])
        self.assertEqual(captured["env"]["MORAINE_TEST"], "yes")
        self.assertTrue(captured["initialized"])
        self.assertTrue(captured["session_closed"])
        self.assertTrue(captured["stdio_closed"])


if __name__ == "__main__":
    unittest.main()
