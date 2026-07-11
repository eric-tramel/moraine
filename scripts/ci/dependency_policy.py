#!/usr/bin/env python3
"""Enforce the epic #451 Phase 1 storage dependency boundaries."""

from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

CLICKHOUSE_PACKAGE = "moraine-clickhouse"
ALLOWED_DIRECT_DEPENDENTS = frozenset(
    {
        "moraine",
        "moraine-conversations",
        "moraine-ingest-core",
    }
)
EPIC_RULE = "epic #451 Phase 1 dependency rule"
SQL_STATEMENT = re.compile(
    r"(?i)(?P<select>\bSELECT\b)|(?P<insert>\bINSERT\s+INTO\b)"
)
ATTRIBUTE_START = re.compile(r"#\s*\[")
INCLUDE_STR_CALL = re.compile(r"\binclude_str\s*!\s*\(")
IGNORED_SOURCE_DIRS = frozenset({"fixture", "fixtures", "test", "tests"})
IGNORED_SOURCE_STEMS = frozenset({"test", "tests"})


class PolicyViolation(RuntimeError):
    """A checked workspace violates the Phase 1 dependency policy."""


@dataclass(frozen=True)
class SqlFinding:
    path: Path
    line: int
    statement: str


@dataclass(frozen=True)
class _RustString:
    start: int
    line: int
    value: str
    raw: bool


def direct_clickhouse_dependents(metadata: dict[str, Any]) -> tuple[str, ...]:
    """Return workspace packages with a direct dependency on moraine-clickhouse."""
    workspace_members = set(metadata.get("workspace_members", ()))
    dependents: set[str] = set()

    for package in metadata.get("packages", ()):
        if workspace_members and package.get("id") not in workspace_members:
            continue
        if any(
            dependency.get("name") == CLICKHOUSE_PACKAGE
            for dependency in package.get("dependencies", ())
        ):
            dependents.add(package["name"])

    return tuple(sorted(dependents))


def assert_clickhouse_dependency_policy(metadata: dict[str, Any]) -> tuple[str, ...]:
    dependents = direct_clickhouse_dependents(metadata)
    offenders = tuple(
        package for package in dependents if package not in ALLOWED_DIRECT_DEPENDENTS
    )
    if offenders:
        offender_lines = "\n".join(f"  - {package}" for package in offenders)
        allowed = ", ".join(sorted(ALLOWED_DIRECT_DEPENDENTS))
        raise PolicyViolation(
            f"{EPIC_RULE} violated: direct workspace dependents of "
            f"{CLICKHOUSE_PACKAGE} outside the allowlist:\n"
            f"{offender_lines}\n"
            f"Allowed direct dependents: {allowed}"
        )
    return dependents


def _blank(mask: list[str], start: int, end: int) -> None:
    for index in range(start, end):
        if mask[index] != "\n":
            mask[index] = " "


def _raw_string_at(source: str, start: int) -> tuple[int, str] | None:
    prefix_length = 0
    if source.startswith("br", start):
        prefix_length = 2
    elif source.startswith("r", start):
        prefix_length = 1
    else:
        return None

    cursor = start + prefix_length
    while cursor < len(source) and source[cursor] == "#":
        cursor += 1
    if cursor >= len(source) or source[cursor] != '"':
        return None

    hashes = source[start + prefix_length : cursor]
    terminator = '"' + hashes
    body_start = cursor + 1
    body_end = source.find(terminator, body_start)
    if body_end < 0:
        body_end = len(source)
        token_end = len(source)
    else:
        token_end = body_end + len(terminator)
    return token_end, source[body_start:body_end]


def _regular_string_at(source: str, quote: int) -> tuple[int, str]:
    cursor = quote + 1
    while cursor < len(source):
        if source[cursor] == "\\":
            cursor += 2
            continue
        if source[cursor] == '"':
            return cursor + 1, source[quote + 1 : cursor]
        cursor += 1
    return len(source), source[quote + 1 :]


def _char_literal_end(source: str, quote: int) -> int | None:
    if quote + 1 >= len(source):
        return None
    cursor = quote + 1
    if source[cursor] == "\\":
        cursor += 2
        if cursor < len(source) and source[cursor - 1] == "u" and source[cursor] == "{":
            closing = source.find("}", cursor + 1)
            if closing < 0:
                return None
            cursor = closing + 1
    else:
        cursor += 1
    if cursor < len(source) and source[cursor] == "'":
        return cursor + 1
    return None


def _lex_rust(source: str) -> tuple[str, tuple[_RustString, ...]]:
    """Mask comments/literals while retaining strings for SQL inspection."""
    mask = list(source)
    strings: list[_RustString] = []
    index = 0
    line = 1

    while index < len(source):
        if source.startswith("//", index):
            end = source.find("\n", index + 2)
            if end < 0:
                end = len(source)
            _blank(mask, index, end)
            index = end
            continue

        if source.startswith("/*", index):
            cursor = index + 2
            depth = 1
            while cursor < len(source) and depth:
                if source.startswith("/*", cursor):
                    depth += 1
                    cursor += 2
                elif source.startswith("*/", cursor):
                    depth -= 1
                    cursor += 2
                else:
                    cursor += 1
            line += source.count("\n", index, cursor)
            _blank(mask, index, cursor)
            index = cursor
            continue

        raw = _raw_string_at(source, index)
        if raw is not None and (index == 0 or not source[index - 1].isalnum()):
            end, value = raw
            strings.append(_RustString(index, line, value, raw=True))
            line += source.count("\n", index, end)
            _blank(mask, index, end)
            index = end
            continue

        if source[index] == '"':
            end, value = _regular_string_at(source, index)
            strings.append(_RustString(index, line, value, raw=False))
            line += source.count("\n", index, end)
            _blank(mask, index, end)
            index = end
            continue

        if source[index] == "'":
            end = _char_literal_end(source, index)
            if end is not None:
                _blank(mask, index, end)
                index = end
                continue

        if source[index] == "\n":
            line += 1
        index += 1

    return "".join(mask), tuple(strings)


def _matching_brace(masked_source: str, opening: int) -> int:
    depth = 0
    for index in range(opening, len(masked_source)):
        if masked_source[index] == "{":
            depth += 1
        elif masked_source[index] == "}":
            depth -= 1
            if depth == 0:
                return index + 1
    return len(masked_source)


def _call_parts(expression: str) -> tuple[str, str | None] | None:
    expression = expression.strip()
    name = re.match(r"[A-Za-z_]\w*(?:::[A-Za-z_]\w*)*", expression)
    if name is None:
        return None
    cursor = name.end()
    while cursor < len(expression) and expression[cursor].isspace():
        cursor += 1
    if cursor == len(expression):
        return name.group(), None
    if expression[cursor] != "(":
        return None

    depth = 0
    closing = -1
    for index in range(cursor, len(expression)):
        if expression[index] == "(":
            depth += 1
        elif expression[index] == ")":
            depth -= 1
            if depth == 0:
                closing = index
                break
    if closing < 0 or expression[closing + 1 :].strip():
        return None
    return name.group(), expression[cursor + 1 : closing]


def _split_cfg_arguments(arguments: str) -> tuple[str, ...]:
    parts: list[str] = []
    start = 0
    depth = 0
    for index, character in enumerate(arguments):
        if character == "(":
            depth += 1
        elif character == ")":
            depth -= 1
        elif character == "," and depth == 0:
            parts.append(arguments[start:index].strip())
            start = index + 1
    tail = arguments[start:].strip()
    if tail:
        parts.append(tail)
    return tuple(parts)


def _cfg_possibilities_without_test(expression: str) -> tuple[bool, bool]:
    """Return whether a cfg can be true/false when the `test` cfg is false."""
    call = _call_parts(expression)
    if call is None:
        return True, True
    name, arguments = call
    if arguments is None:
        return (False, True) if name == "test" else (True, True)

    children = [
        _cfg_possibilities_without_test(argument)
        for argument in _split_cfg_arguments(arguments)
    ]
    if name == "all":
        return all(child[0] for child in children), any(child[1] for child in children)
    if name == "any":
        return any(child[0] for child in children), all(child[1] for child in children)
    if name == "not" and len(children) == 1:
        can_be_true, can_be_false = children[0]
        return can_be_false, can_be_true
    return True, True


def _is_test_only_attribute(content: str) -> bool:
    call = _call_parts(content)
    if call is None:
        return False
    name, arguments = call
    if name == "test" or name.endswith("::test"):
        return True
    if name != "cfg" or arguments is None:
        return False
    can_be_true_without_test, _ = _cfg_possibilities_without_test(arguments)
    return not can_be_true_without_test


def _test_only_attributes(masked_source: str) -> tuple[tuple[int, int], ...]:
    attributes: list[tuple[int, int]] = []
    cursor = 0
    while found := ATTRIBUTE_START.search(masked_source, cursor):
        opening = masked_source.find("[", found.start(), found.end())
        depth = 1
        end = opening + 1
        while end < len(masked_source) and depth:
            if masked_source[end] == "[":
                depth += 1
            elif masked_source[end] == "]":
                depth -= 1
            end += 1
        if depth:
            break
        if _is_test_only_attribute(masked_source[opening + 1 : end - 1]):
            attributes.append((found.start(), end))
        cursor = end
    return tuple(attributes)


def _test_only_ranges(masked_source: str) -> tuple[tuple[int, int], ...]:
    ranges: list[tuple[int, int]] = []
    for attribute_start, attribute_end in _test_only_attributes(masked_source):
        opening = masked_source.find("{", attribute_end)
        semicolon = masked_source.find(";", attribute_end)
        if opening >= 0 and (semicolon < 0 or opening < semicolon):
            ranges.append((attribute_start, _matching_brace(masked_source, opening)))
        elif semicolon >= 0:
            ranges.append((attribute_start, semicolon + 1))
        else:
            ranges.append((attribute_start, len(masked_source)))
    return tuple(ranges)


def _inside_ranges(offset: int, ranges: Iterable[tuple[int, int]]) -> bool:
    return any(start <= offset < end for start, end in ranges)


def _normalized_string(literal: _RustString) -> str:
    if literal.raw:
        return literal.value

    value = literal.value
    normalized: list[str] = []
    index = 0
    escapes = {"n": "\n", "r": "\r", "t": "\t", "0": "\0", "\\": "\\", '"': '"'}
    while index < len(value):
        if value[index] != "\\":
            normalized.append(value[index])
            index += 1
            continue

        if index + 1 >= len(value):
            normalized.append("\\")
            break
        escaped = value[index + 1]
        if escaped == "\n":
            index += 2
            while index < len(value) and value[index] in " \t":
                index += 1
            continue
        if escaped == "\r" and value.startswith("\r\n", index + 1):
            index += 3
            while index < len(value) and value[index] in " \t":
                index += 1
            continue
        if escaped == "x" and re.fullmatch(r"[0-9A-Fa-f]{2}", value[index + 2 : index + 4]):
            normalized.append(chr(int(value[index + 2 : index + 4], 16)))
            index += 4
            continue
        if escaped == "u" and index + 2 < len(value) and value[index + 2] == "{":
            closing = value.find("}", index + 3)
            digits = value[index + 3 : closing].replace("_", "") if closing >= 0 else ""
            if digits and re.fullmatch(r"[0-9A-Fa-f]+", digits):
                normalized.append(chr(int(digits, 16)))
                index = closing + 1
                continue
        normalized.append(escapes.get(escaped, escaped))
        index += 2
    return "".join(normalized)


def _included_string_path(
    source_path: Path, masked_source: str, literal: _RustString
) -> tuple[int, Path] | None:
    prefix = masked_source[: literal.start]
    calls = tuple(INCLUDE_STR_CALL.finditer(prefix))
    if not calls or prefix[calls[-1].end() :].strip():
        return None
    return calls[-1].start(), (
        source_path.parent / _normalized_string(literal)
    ).resolve()


def _without_sql_comments(value: str) -> str:
    masked = list(value)
    index = 0
    quote: str | None = None
    while index < len(value):
        if quote is not None:
            if value[index] == "\\":
                index += 2
                continue
            if value[index] == quote:
                if index + 1 < len(value) and value[index + 1] == quote:
                    index += 2
                    continue
                quote = None
            index += 1
            continue
        if value[index] in {"'", '"'}:
            quote = value[index]
            index += 1
            continue

        hash_comment = value[index] == "#" and (
            index + 1 == len(value)
            or value[index + 1] == "!"
            or value[index + 1].isspace()
        )
        if value.startswith(("--", "//"), index) or hash_comment:
            end = value.find("\n", index + 1)
            if end < 0:
                end = len(value)
            _blank(masked, index, end)
            index = end
            continue
        if value.startswith("/*", index):
            cursor = index + 2
            depth = 1
            while cursor < len(value) and depth:
                if value.startswith("/*", cursor):
                    depth += 1
                    cursor += 2
                elif value.startswith("*/", cursor):
                    depth -= 1
                    cursor += 2
                else:
                    cursor += 1
            _blank(masked, index, cursor)
            index = cursor
            continue
        index += 1
    return "".join(masked)


def _append_sql_findings(
    findings: list[SqlFinding], path: Path, start_line: int, value: str
) -> None:
    for match in SQL_STATEMENT.finditer(value):
        statement = "SELECT" if match.group("select") else "INSERT INTO"
        line = start_line + value.count("\n", 0, match.start())
        findings.append(SqlFinding(path, line, statement))


def sql_findings(source_root: Path) -> tuple[SqlFinding, ...]:
    if not source_root.is_dir():
        raise RuntimeError(f"monitor source root does not exist: {source_root}")
    findings: list[SqlFinding] = []
    source_root_resolved = source_root.resolve()
    for path in sorted(source_root.rglob("*.rs")):
        relative = path.relative_to(source_root)
        if relative.stem.lower() in IGNORED_SOURCE_STEMS or any(
            part.lower() in IGNORED_SOURCE_DIRS for part in relative.parts[:-1]
        ):
            continue

        source = path.read_text(encoding="utf-8")
        masked, strings = _lex_rust(source)
        test_ranges = _test_only_ranges(masked)
        resolved_include_calls: set[int] = set()
        for literal in strings:
            if _inside_ranges(literal.start, test_ranges):
                continue
            included = _included_string_path(path, masked, literal)
            if included is not None:
                include_call, included_path = included
                resolved_include_calls.add(include_call)
                try:
                    included_display = included_path.relative_to(source_root_resolved)
                except ValueError:
                    included_display = included_path
                included_source = included_path.read_text(encoding="utf-8")
                _append_sql_findings(
                    findings,
                    included_display,
                    1,
                    _without_sql_comments(included_source),
                )
                continue

            _append_sql_findings(
                findings,
                relative,
                literal.line,
                _without_sql_comments(_normalized_string(literal)),
            )
        for include_call in INCLUDE_STR_CALL.finditer(masked):
            if (
                _inside_ranges(include_call.start(), test_ranges)
                or include_call.start() in resolved_include_calls
            ):
                continue
            line = source.count("\n", 0, include_call.start()) + 1
            raise PolicyViolation(
                f"{EPIC_RULE} violated: cannot verify production include_str! "
                f"at {relative}:{line}; use a direct string-literal path so the "
                "included source can be checked for SQL"
            )
    return tuple(findings)


def assert_monitor_sql_policy(source_root: Path) -> tuple[SqlFinding, ...]:
    findings = sql_findings(source_root)
    if findings:
        finding_lines = "\n".join(
            f"  - {finding.path}:{finding.line}: {finding.statement} statement literal"
            for finding in findings
        )
        raise PolicyViolation(
            f"{EPIC_RULE} violated: moraine-monitor-core contains SQL statement "
            f"literals in non-test source:\n{finding_lines}"
        )
    return findings


def load_cargo_metadata(workspace_root: Path, cargo_command: str) -> dict[str, Any]:
    command = shlex.split(cargo_command) + [
        "metadata",
        "--format-version",
        "1",
        "--no-deps",
        "--locked",
    ]
    completed = subprocess.run(
        command,
        cwd=workspace_root,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip()
        raise RuntimeError(f"cargo metadata failed ({completed.returncode}): {detail}")
    return json.loads(completed.stdout)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    default_root = Path(__file__).resolve().parents[2]
    parser.add_argument("--workspace-root", type=Path, default=default_root)
    parser.add_argument(
        "--metadata-json",
        type=Path,
        help="read Cargo metadata JSON from a fixture instead of invoking cargo",
    )
    parser.add_argument(
        "--monitor-source",
        type=Path,
        help="scan this source root instead of crates/moraine-monitor-core/src",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    workspace_root = args.workspace_root.resolve()
    monitor_source = (
        args.monitor_source.resolve()
        if args.monitor_source
        else workspace_root / "crates" / "moraine-monitor-core" / "src"
    )

    try:
        if args.metadata_json:
            metadata = json.loads(args.metadata_json.read_text(encoding="utf-8"))
        else:
            cargo_command = os.environ.get("MORAINE_CARGO", "cargo")
            metadata = load_cargo_metadata(workspace_root, cargo_command)
        dependents = assert_clickhouse_dependency_policy(metadata)
        assert_monitor_sql_policy(monitor_source)
    except (OSError, RuntimeError, ValueError, json.JSONDecodeError) as error:
        print(f"[dependency-policy] FAIL: {error}", file=sys.stderr)
        return 1

    print(
        "[dependency-policy] direct moraine-clickhouse dependents: "
        + ", ".join(dependents)
    )
    print(
        "[dependency-policy] moraine-monitor-core non-test Rust sources contain "
        "no SELECT or INSERT INTO statement literals"
    )
    print(f"[dependency-policy] PASS: {EPIC_RULE}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
