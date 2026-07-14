#!/usr/bin/env python3
"""Emit autoresearch metrics from fixed concurrent MCP retrieval artifacts."""

from __future__ import annotations

import json
import sys
from pathlib import Path


def load(path: Path) -> dict:
    artifact = json.loads(path.read_text(encoding="utf-8"))
    if artifact["semantic"]["status"] != "pass":
        raise SystemExit(f"semantic oracle failed: {path}")
    quality = artifact["metrics"]["quality"]
    if quality["success_rate"] != 1.0 or quality["error_rate"] != 0.0:
        raise SystemExit(f"retrieval requests failed: {path}")
    return artifact


def resource_metric(artifact: dict, name: str) -> float:
    return float(artifact["metrics"]["resources"][name])


def main() -> int:
    if len(sys.argv) != 3:
        raise SystemExit("usage: autoresearch_retrieval_metrics.py N8_ARTIFACT N16_ARTIFACT")
    n8 = load(Path(sys.argv[1]))
    n16 = load(Path(sys.argv[2]))
    n8_qps = resource_metric(n8, "mean_throughput_per_second")
    n16_qps = resource_metric(n16, "mean_throughput_per_second")
    if n16_qps <= 0.0:
        raise SystemExit("n16 throughput must be positive")

    print(f"METRIC retrieval_operational_ns_per_query={round(1_000_000_000 / n16_qps)}")
    print(f"METRIC retrieval_n16_qps={n16_qps:.6f}")
    print(f"METRIC retrieval_n16_p50_ms={resource_metric(n16, 'p50_latency_ms'):.6f}")
    print(f"METRIC retrieval_n16_p95_ms={resource_metric(n16, 'p95_latency_ms'):.6f}")
    print(f"METRIC retrieval_n8_qps={n8_qps:.6f}")
    print(f"METRIC retrieval_n8_p50_ms={resource_metric(n8, 'p50_latency_ms'):.6f}")
    print(f"METRIC retrieval_n8_p95_ms={resource_metric(n8, 'p95_latency_ms'):.6f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
