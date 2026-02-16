# Cortex Documentation

Cortex is a local indexing and retrieval system built to ingest Codex JSONL session streams and Claude code-trace JSONL streams with low latency, keep full reconstruction fidelity, and serve lexical retrieval without rebuilding global indexes for every query. The system is deliberately opinionated: everything runs on one machine, all state is under `~/.cortex`, and all critical surfaces are SQL tables, Rust services, and deterministic shell entrypoints.

The practical consequence is that this documentation is not a user tutorial; it is a maintainer corpus. Each document is written to answer a specific class of engineering question: what the system guarantees, where it can fail, how failure is recovered, how freshness is preserved, and where performance is spent.

## Reader Model

This corpus targets two audiences. The first audience is the engineer operating the stack locally and needing to debug ingestion lag, missed events, index drift, or retrieval quality regressions. The second audience is the engineer extending the stack with new event types, ranking signals, or agent interfaces while preserving trace fidelity.

The corpus is now split by subsystem boundary: `Cortex Core` (data plane), `Cortex Search` (index and ranking substrate), and `Cortex MCP` (agent-facing retrieval interface). This partition matches code ownership and incident response paths, so readers can go deep in one layer without losing causal links to adjacent layers.

## Reading Path

Read in this order for full context: `docs/core/system-architecture.md`, `docs/core/data-model.md`, `docs/core/unified-trace-schema.md`, `docs/core/ingestion-service.md`, `docs/search/indexing-and-retrieval.md`, `docs/mcp/agent-interface.md`, `docs/architecture/design-tradeoffs.md`, and `docs/operations/build-and-operations.md`. The sequence follows dependency direction from invariants to schema, field-level normalization mapping, runtime behavior, search structures, interface contract, then architectural and operational consequences.

## External References

Core upstream references for this stack are [ClickHouse (GitHub)](https://github.com/ClickHouse/ClickHouse), [ClickHouse Documentation](https://clickhouse.com/docs), [Rust](https://www.rust-lang.org/), [Tokio](https://github.com/tokio-rs/tokio), [JSON Lines](https://jsonlines.org/), [Okapi BM25](https://en.wikipedia.org/wiki/Okapi_BM25), [BM25S docs](https://bm25s.github.io/), and [BM25S implementation](https://github.com/xhluca/bm25s).

## Evidence Policy

Core technical claims in these docs are annotated with inline `src` references that point to concrete code or SQL line numbers. This is intentional: maintainers should be able to jump directly from prose to implementation truth without re-deriving behavior from scratch.

## Documentation Quality Gates

This repository includes hard-fail quality checks that enforce minimum density and factual grounding for the core architecture documents. The checks are wired through `make docs-qc` and fail on unsupported claims, missing mandatory sections, weak source coverage, and list-heavy writing. The checks are strict by design; passing output should read like a technical design dossier, not release notes. [src: Makefile:L29-30, scripts/docs_qc/run.py:L117-126, scripts/docs_qc/run.py:L128-145, scripts/docs_qc/run.py:L169-178, scripts/docs_qc/run.py:L183-194, scripts/docs_qc/run.py:L298-347]

To run the documentation pipeline:

```bash
cd ~/src/cortex
make docs-qc
make docs-build
make docs-serve
```

If `make docs-qc` fails, treat failures as specification defects in the docs, not stylistic nits.
