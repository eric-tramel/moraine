#!/usr/bin/env python3
"""Generate browsable source pages for documentation citations."""

from __future__ import annotations

import argparse
import html
import re
import shutil
from pathlib import Path

CITATION_RE = re.compile(r"\[src:\s*([^\]]+)\]")
REF_RE = re.compile(r"^([^:\s]+):(?:L)?(\d+)(?:-(?:L)?(\d+))?$", re.IGNORECASE)


def parse_ref(chunk: str) -> tuple[str, int, int] | None:
    parsed = REF_RE.match(chunk)
    if not parsed:
        return None
    rel_path = parsed.group(1)
    start = int(parsed.group(2))
    end = int(parsed.group(3) or start)
    if end < start:
        start, end = end, start
    return rel_path, start, end


def detect_language(rel_path: str) -> str:
    lowered = rel_path.lower()

    if lowered.endswith(".rs"):
        return "rust"
    if lowered.endswith(".py"):
        return "python"
    if lowered.endswith(".sql"):
        return "sql"
    if lowered.endswith(".toml"):
        return "toml"
    if lowered.endswith(".yml") or lowered.endswith(".yaml"):
        return "yaml"
    if lowered.endswith(".xml"):
        return "xml"
    if lowered.endswith(".json"):
        return "json"
    if lowered.endswith(".sh") or lowered.endswith("makefile"):
        return "bash"
    if lowered.endswith(".md"):
        return "markdown"

    return "plaintext"


def collect_references(docs_root: Path) -> dict[str, set[int]]:
    refs: dict[str, set[int]] = {}
    for md_path in sorted(docs_root.rglob("*.md")):
        if "_source" in md_path.parts or ".quality" in md_path.parts:
            continue
        text = md_path.read_text(encoding="utf-8")
        for match in CITATION_RE.finditer(text):
            raw = match.group(1)
            for chunk in [item.strip() for item in raw.split(",") if item.strip()]:
                parsed = parse_ref(chunk)
                if not parsed:
                    continue
                rel_path, start, end = parsed
                refs.setdefault(rel_path, set()).update(range(start, end + 1))
    return refs


def resolve_repo_source_path(repo_root: Path, rel_path: str) -> tuple[str, Path] | None:
    candidate = Path(rel_path)
    if candidate.is_absolute():
        return None
    if ".." in candidate.parts:
        return None

    abs_path = (repo_root / candidate).resolve()
    try:
        normalized_rel_path = abs_path.relative_to(repo_root).as_posix()
    except ValueError:
        return None

    return normalized_rel_path, abs_path


def resolve_output_path(out_dir: Path, rel_path: str) -> Path | None:
    dst = (out_dir / f"{rel_path}.md").resolve()
    try:
        dst.relative_to(out_dir)
    except ValueError:
        return None
    return dst


def render_source_page(abs_path: Path, rel_path: str, ref_lines: set[int]) -> str:
    source_text = abs_path.read_text(encoding="utf-8", errors="replace")
    src_lines = source_text.splitlines()

    language = detect_language(rel_path)

    out: list[str] = []
    out.append(f"# `{rel_path}`")
    out.append("")
    out.append(f'<pre class="source-code" data-language="{language}" data-path="{html.escape(rel_path)}"><code>')
    for idx, line in enumerate(src_lines, start=1):
        escaped = html.escape(line) if line else "&nbsp;"
        out.append(
            f'<span class="code-line" id="L{idx}" data-line="{idx}"><span class="lineno">{idx:>6}</span> '
            f'<span class="line-content">{escaped}</span></span>'
        )
    out.append("</code></pre>")
    out.append("")
    return "\n".join(out)


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate docs/_source pages from citation references")
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--docs-root", default="docs")
    parser.add_argument("--out-dir", default="docs/_source")
    args = parser.parse_args()

    repo_root = Path(args.repo_root).resolve()
    docs_root = (repo_root / args.docs_root).resolve()
    out_dir = (repo_root / args.out_dir).resolve()

    if not docs_root.exists():
        raise SystemExit(f"docs root does not exist: {docs_root}")

    refs = collect_references(docs_root)

    if out_dir.exists():
        shutil.rmtree(out_dir, ignore_errors=True)
    out_dir.mkdir(parents=True, exist_ok=True)

    generated = 0
    skipped = 0
    for raw_rel_path, lines in sorted(refs.items()):
        resolved = resolve_repo_source_path(repo_root, raw_rel_path)
        if not resolved:
            skipped += 1
            continue

        rel_path, abs_path = resolved
        if not abs_path.exists() or not abs_path.is_file():
            skipped += 1
            continue

        dst = resolve_output_path(out_dir, rel_path)
        if dst is None:
            skipped += 1
            continue
        dst.parent.mkdir(parents=True, exist_ok=True)
        dst.write_text(render_source_page(abs_path, rel_path, lines), encoding="utf-8")
        generated += 1

    print(f"generated source pages: {generated}")
    print(f"skipped missing references: {skipped}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
