"""Hermes plugin for Moraine MCP guidance and setup diagnostics."""

from __future__ import annotations

import json
import os
import shlex
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

try:
    from hermes_constants import get_hermes_home
except Exception:  # pragma: no cover - Hermes always provides this at runtime.
    get_hermes_home = None  # type: ignore[assignment]


PLUGIN_DIR = Path(__file__).resolve().parent
REQUIRED_MCP_TOOLS = {"search_sessions", "open", "list_sessions", "file_attention"}
GUIDANCE_TRIGGERS = (
    "another agent",
    "other agent",
    "agents doing",
    "agent history",
    "agent session",
    "active agent",
    "current agent",
    "file_attention",
    "last time",
    "past conversation",
    "past session",
    "previous",
    "prior",
    "realtime",
    "real-time",
    "search sessions",
    "session history",
    "who touched",
)

DOCTOR_SCHEMA = {
    "name": "moraine_doctor",
    "description": (
        "Check whether Hermes is configured to use Moraine MCP session search. "
        "Use when Moraine tools are missing, setup may be broken, or the user "
        "asks to diagnose the Hermes/Moraine integration."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "run_mcp_test": {
                "type": "boolean",
                "description": (
                    "Run `hermes mcp test moraine` as part of the diagnosis. "
                    "Defaults to false for quick, read-only checks."
                ),
            }
        },
    },
}


def register(ctx) -> None:
    """Register Hermes-native Moraine guidance and diagnostics."""
    ctx.register_skill(
        "session-search",
        PLUGIN_DIR / "skills" / "session-search" / "SKILL.md",
        "Recover prior agent-session context with Moraine MCP search.",
    )
    ctx.register_skill(
        "realtime-peek",
        PLUGIN_DIR / "skills" / "realtime-peek" / "SKILL.md",
        "Inspect active or very recent agent sessions through Moraine.",
    )
    ctx.register_tool(
        name="moraine_doctor",
        toolset="moraine",
        schema=DOCTOR_SCHEMA,
        handler=_doctor_tool,
        description="Check Moraine MCP setup for the active Hermes profile.",
    )
    ctx.register_hook("pre_llm_call", _pre_llm_call)
    ctx.register_cli_command(
        name="moraine",
        help="Manage Moraine MCP integration",
        setup_fn=_setup_cli,
        handler_fn=_handle_cli,
        description="Diagnose or configure Hermes' Moraine MCP integration.",
    )
    ctx.register_command(
        "moraine",
        handler=_handle_slash,
        description="Check or set up Moraine MCP integration.",
        args_hint="[status|doctor|setup|help]",
    )


def _doctor_tool(args: dict[str, Any], **_kwargs: Any) -> str:
    """Return a JSON doctor report for model-initiated diagnosis."""
    try:
        run_mcp_test = bool(args.get("run_mcp_test", False))
        return json.dumps(_doctor_report(run_mcp_test=run_mcp_test))
    except Exception as exc:  # pragma: no cover - fail closed for tool calls.
        return json.dumps({"ok": False, "error": str(exc)})


def _pre_llm_call(user_message: str = "", **_kwargs: Any) -> dict[str, str] | None:
    """Inject compact Moraine guidance only on turns that imply session recall."""
    text = (user_message or "").lower()
    if "moraine guidance for hermes" in text:
        return None
    if "moraine" not in text and not any(trigger in text for trigger in GUIDANCE_TRIGGERS):
        return None

    return {
        "context": (
            "Moraine guidance for Hermes: when the user asks about prior work, "
            "agent history, active agents, or files touched by other agents, use "
            "the Moraine MCP tools registered as `mcp_moraine_search_sessions`, "
            "`mcp_moraine_list_sessions`, `mcp_moraine_open`, and "
            "`mcp_moraine_file_attention` if available. Moraine search is BM25, "
            "so use compact keyword queries, then open returned event/turn/session "
            "IDs for evidence. For full procedures, load "
            "`skill_view(\"moraine:session-search\")` or "
            "`skill_view(\"moraine:realtime-peek\")`. If the MCP tools are "
            "missing, ask the user to run `hermes moraine doctor` or "
            "`hermes moraine setup`."
        )
    }


def _setup_cli(parser) -> None:
    subparsers = parser.add_subparsers(dest="moraine_action")

    status = subparsers.add_parser("status", help="Show quick Moraine integration status")
    status.add_argument("--json", action="store_true", help="Print machine-readable JSON")

    doctor = subparsers.add_parser("doctor", help="Diagnose Moraine integration")
    doctor.add_argument("--json", action="store_true", help="Print machine-readable JSON")
    doctor.add_argument(
        "--no-mcp-test",
        action="store_true",
        help="Skip `hermes mcp test moraine`",
    )

    setup = subparsers.add_parser("setup", help="Register Moraine MCP in this Hermes profile")
    setup.add_argument(
        "--force",
        action="store_true",
        help="Remove any existing Moraine MCP registration before adding it",
    )
    setup.add_argument(
        "--no-test",
        action="store_true",
        help="Skip `hermes mcp test moraine` after setup",
    )
    setup.add_argument("--json", action="store_true", help="Print machine-readable JSON")


def _handle_cli(args) -> None:
    action = getattr(args, "moraine_action", None) or "doctor"
    if action == "setup":
        result = _setup_mcp(force=bool(args.force), run_test=not bool(args.no_test))
        _print_result(result, as_json=bool(args.json))
        raise SystemExit(0 if result["ok"] else 1)

    run_mcp_test = action == "doctor" and not bool(getattr(args, "no_mcp_test", False))
    report = _doctor_report(run_mcp_test=run_mcp_test)
    _print_result(report, as_json=bool(getattr(args, "json", False)))
    raise SystemExit(0 if report["ok"] else 1)


def _handle_slash(raw_args: str) -> str:
    try:
        parts = shlex.split(raw_args or "")
    except ValueError as exc:
        return f"Could not parse /moraine arguments: {exc}"

    action = parts[0] if parts else "status"
    flags = set(parts[1:])

    if action in {"help", "-h", "--help"}:
        return (
            "/moraine status - quick setup check\n"
            "/moraine doctor [--no-mcp-test] - diagnose setup\n"
            "/moraine setup [--force] [--no-test] - register Moraine MCP"
        )
    if action == "setup":
        return _format_result(
            _setup_mcp(force="--force" in flags, run_test="--no-test" not in flags)
        )
    if action == "doctor":
        return _format_result(_doctor_report(run_mcp_test="--no-mcp-test" not in flags))
    if action == "status":
        return _format_result(_doctor_report(run_mcp_test=False))
    return f"Unknown /moraine action: {action}. Try /moraine help."


def _setup_mcp(*, force: bool, run_test: bool) -> dict[str, Any]:
    steps: list[dict[str, Any]] = []
    hermes = _resolve_executable("hermes")
    if not hermes:
        return {
            "ok": False,
            "summary": "Hermes executable was not found on PATH.",
            "steps": [{"status": "error", "message": "Cannot run `hermes mcp add`."}],
        }

    moraine = _resolve_executable("moraine")
    if not moraine:
        return {
            "ok": False,
            "summary": "Moraine CLI was not found on PATH.",
            "steps": [{"status": "error", "message": "Install Moraine first, then rerun setup."}],
        }

    existing = _mcp_config_state()
    if existing["configured"] and existing["expected"] and not force:
        steps.append({"status": "ok", "message": "Moraine MCP is already configured."})
    else:
        if force or existing["configured"]:
            remove = _run_command([hermes, "mcp", "remove", "moraine"], timeout=20)
            steps.append(
                {
                    "status": "ok" if remove["returncode"] == 0 else "warn",
                    "message": "Removed existing Moraine MCP registration."
                    if remove["returncode"] == 0
                    else "Existing Moraine MCP registration was absent or could not be removed.",
                    "detail": remove["summary"],
                }
            )

        add = _run_command(
            [hermes, "mcp", "add", "moraine", "--command", "moraine", "--args", "run", "mcp"],
            timeout=30,
            input_text="\n",
        )
        steps.append(
            {
                "status": "ok" if add["returncode"] == 0 else "error",
                "message": "Registered Moraine MCP server."
                if add["returncode"] == 0
                else "Failed to register Moraine MCP server.",
                "detail": add["summary"],
            }
        )

    if run_test:
        test = _run_command([hermes, "mcp", "test", "moraine"], timeout=30)
        steps.append(
            {
                "status": "ok" if test["returncode"] == 0 else "error",
                "message": "Hermes MCP test passed."
                if test["returncode"] == 0
                else "Hermes MCP test failed.",
                "detail": test["summary"],
            }
        )

    ok = all(step["status"] != "error" for step in steps)
    return {
        "ok": ok,
        "summary": "Moraine MCP setup complete." if ok else "Moraine MCP setup needs attention.",
        "steps": steps,
        "next_steps": [
            "Start or restart Hermes so the MCP tool list is refreshed.",
            "Run `hermes moraine doctor` if the tools are still missing.",
        ],
    }


def _doctor_report(*, run_mcp_test: bool) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []

    moraine = _resolve_executable("moraine")
    if moraine:
        checks.append(_check("Moraine CLI", "ok", f"Found {moraine}."))
        version = _run_command([moraine, "--version"], timeout=8)
        checks.append(
            _check(
                "Moraine version",
                "ok" if version["returncode"] == 0 else "warn",
                version["summary"] or "Unable to read Moraine version.",
            )
        )
        trust = _moraine_path_trust(moraine)
        if trust:
            checks.append(_check("Moraine CLI trust", "warn", trust))
    else:
        checks.append(
            _check(
                "Moraine CLI",
                "error",
                "Moraine CLI was not found on PATH. Install it with `uv tool install moraine-cli`.",
            )
        )

    state = _mcp_config_state()
    if not state["config_exists"]:
        checks.append(_check("Hermes config", "error", f"Missing {state['config_path']}."))
    elif state["config_error"]:
        checks.append(_check("Hermes config", "error", state["config_error"]))
    elif not state["configured"]:
        checks.append(
            _check(
                "Hermes MCP registration",
                "error",
                "No `mcp_servers.moraine` entry found. Run `hermes moraine setup`.",
            )
        )
    elif not state["enabled"]:
        checks.append(
            _check(
                "Hermes MCP registration",
                "error",
                "`mcp_servers.moraine` is disabled in Hermes config.",
            )
        )
    elif not state["expected"]:
        checks.append(
            _check(
                "Hermes MCP registration",
                "error",
                "`mcp_servers.moraine` exists but does not launch `moraine run mcp`.",
                detail=state["detail"],
            )
        )
    else:
        checks.append(_check("Hermes MCP registration", "ok", "Configured as `moraine run mcp`."))

    if state["configured"] and state["tool_filter_issue"]:
        checks.append(
            _check(
                "Hermes MCP tool filter",
                "error",
                state["tool_filter_issue"],
            )
        )

    if moraine:
        status = _run_command([moraine, "status"], timeout=12)
        checks.append(
            _check(
                "Moraine stack",
                "ok" if status["returncode"] == 0 else "warn",
                "Moraine stack reports healthy status."
                if status["returncode"] == 0
                else "Moraine stack is not reporting healthy status. Run `moraine up` before searching.",
                detail=status["summary"],
            )
        )

    if run_mcp_test and state["configured"]:
        hermes = _resolve_executable("hermes")
        if hermes:
            test = _run_command([hermes, "mcp", "test", "moraine"], timeout=30)
            checks.append(
                _check(
                    "Hermes MCP live test",
                    "ok" if test["returncode"] == 0 else "error",
                    "Hermes can connect to Moraine MCP."
                    if test["returncode"] == 0
                    else "Hermes could not connect to Moraine MCP.",
                    detail=test["summary"],
                )
            )
        else:
            checks.append(_check("Hermes executable", "error", "Hermes was not found on PATH."))

    ok = all(check["status"] != "error" for check in checks)
    return {
        "ok": ok,
        "summary": "Moraine is ready for Hermes." if ok else "Moraine needs attention before Hermes can search it.",
        "checks": checks,
        "skills": ["moraine:session-search", "moraine:realtime-peek"],
        "expected_tools": sorted(f"mcp_moraine_{tool}" for tool in REQUIRED_MCP_TOOLS),
    }


def _mcp_config_state() -> dict[str, Any]:
    config_path = _hermes_config_path()
    state = {
        "config_path": str(config_path),
        "config_exists": config_path.exists(),
        "config_error": "",
        "configured": False,
        "enabled": False,
        "expected": False,
        "detail": "",
        "tool_filter_issue": "",
    }
    if not config_path.exists():
        return state

    try:
        import yaml

        data = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    except Exception as exc:
        state["config_error"] = f"Could not parse Hermes config: {exc}"
        return state

    servers = data.get("mcp_servers") if isinstance(data, dict) else None
    server = servers.get("moraine") if isinstance(servers, dict) else None
    if not isinstance(server, dict):
        return state

    state["configured"] = True
    state["enabled"] = server.get("enabled", True) is not False
    command = server.get("command")
    args = server.get("args") or []
    args_list = args if isinstance(args, list) else [args]
    normalized_args = [str(arg) for arg in args_list]
    command_name = Path(str(command)).name if command is not None else ""
    state["expected"] = command_name == "moraine" and normalized_args[:2] == ["run", "mcp"]
    state["detail"] = f"command={command!r} args={normalized_args!r}"

    tools = server.get("tools")
    if isinstance(tools, dict):
        include = tools.get("include")
        exclude = tools.get("exclude")
        if isinstance(include, list):
            missing = REQUIRED_MCP_TOOLS.difference(str(item) for item in include)
            if missing:
                state["tool_filter_issue"] = (
                    "Moraine MCP include filter hides required tools: "
                    + ", ".join(sorted(missing))
                )
        if isinstance(exclude, list):
            blocked = REQUIRED_MCP_TOOLS.intersection(str(item) for item in exclude)
            if blocked:
                state["tool_filter_issue"] = (
                    "Moraine MCP exclude filter blocks required tools: "
                    + ", ".join(sorted(blocked))
                )
    return state


def _hermes_config_path() -> Path:
    if get_hermes_home is not None:
        return get_hermes_home() / "config.yaml"
    return Path(os.path.expanduser("~/.hermes/config.yaml"))


def _resolve_executable(name: str) -> str | None:
    found = shutil.which(name)
    return found if found else None


def _moraine_path_trust(path: str) -> str:
    candidate = Path(path)
    if not candidate.is_absolute():
        return "Moraine resolved to a non-absolute PATH entry; start Hermes from a trusted shell."

    cwd = Path.cwd().resolve()
    try:
        if candidate.resolve().is_relative_to(cwd):
            return "Moraine resolves inside the current project directory; prefer an installed CLI earlier on PATH."
    except OSError:
        return "Could not resolve Moraine CLI path for trust checks."

    scan = candidate.resolve().parent
    for parent in [scan, *scan.parents]:
        if (parent / ".git").exists():
            return "Moraine resolves inside a Git worktree; prefer an installed CLI earlier on PATH."
    return ""


def _run_command(
    command: list[str],
    *,
    timeout: int,
    input_text: str | None = None,
) -> dict[str, Any]:
    try:
        result = subprocess.run(
            command,
            input=input_text,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
        output = "\n".join(part for part in [result.stdout.strip(), result.stderr.strip()] if part)
        return {
            "returncode": result.returncode,
            "summary": _shorten(output),
        }
    except FileNotFoundError:
        return {"returncode": 127, "summary": f"{command[0]} not found"}
    except subprocess.TimeoutExpired:
        return {"returncode": 124, "summary": f"{shlex.join(command)} timed out after {timeout}s"}


def _check(name: str, status: str, message: str, *, detail: str = "") -> dict[str, str]:
    item = {"name": name, "status": status, "message": message}
    if detail:
        item["detail"] = _shorten(detail)
    return item


def _print_result(result: dict[str, Any], *, as_json: bool) -> None:
    if as_json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        print(_format_result(result))


def _format_result(result: dict[str, Any]) -> str:
    lines = [result.get("summary", "Moraine status")]
    entries = result.get("checks") or result.get("steps") or []
    for item in entries:
        status = str(item.get("status", "")).upper()
        lines.append(f"[{status}] {item.get('name') or item.get('message')}")
        if item.get("name"):
            lines.append(f"  {item.get('message', '')}")
        if item.get("detail"):
            lines.append(f"  {item['detail']}")
    if result.get("next_steps"):
        lines.append("Next steps:")
        lines.extend(f"- {step}" for step in result["next_steps"])
    return "\n".join(lines)


def _shorten(text: str, limit: int = 700) -> str:
    clean = " ".join((text or "").split())
    if len(clean) <= limit:
        return clean
    return clean[: limit - 3] + "..."
