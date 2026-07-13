#!/usr/bin/env python3
"""Deterministic tests for seed_concurrent_mcp_benchmark.py."""
from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock
from urllib.parse import parse_qs, urlparse

sys.path.insert(0, str(Path(__file__).resolve().parent))
import concurrent_mcp_retrieval as benchmark
import seed_concurrent_mcp_benchmark as seed


class SeedOracleTests(unittest.TestCase):
    def test_oracle_is_seed_owned_distinct_and_loadable(self):
        value = seed.build_oracle(100_000, 8, 2)
        self.assertEqual(value["measured"][0]["result_count"], 1)
        self.assertEqual(value["measured"][1]["result_count"], 10)
        queries = [value["warmup"]["query"]] + [item["query"] for item in value["measured"] + value["recovery"]]
        self.assertEqual(len(queries), len(set(queries)))
        with tempfile.TemporaryDirectory() as raw:
            path = Path(raw) / "oracle.json"
            path.write_text(json.dumps(value), encoding="utf-8")
            loaded = benchmark.load_oracle(path, profile="full", needed_queries=8, recovery_count=2)
        self.assertEqual(loaded.dataset_cardinality, 100_000)

    def test_public_id_and_digest_match_benchmark_contract(self):
        event = seed.public_id("event", seed.event_uid(1))
        session = seed.public_id("session", seed.session_id(1))
        expected = benchmark.sha256_json([{"event_id": event, "session_id": session}])
        self.assertEqual(seed.result_digest([1]), expected)

    def test_seed_sql_has_exact_cardinality_and_mixed_term_shapes(self):
        sql = seed.seed_sql("moraine", 100_000, 64, 2)
        self.assertIn("FROM numbers(100000)", sql)
        self.assertIn("selective_", sql)
        self.assertIn("common_", sql)
        self.assertIn("recovery_", sql)
        self.assertIn("warmupterm", sql)


class SeedLifecycleTests(unittest.TestCase):
    def test_seed_truncates_owned_tables_in_dependency_order_and_writes_oracle(self):
        responses = ["", "", "", "", "", "200\n"]
        with tempfile.TemporaryDirectory() as raw:
            output = Path(raw) / "oracle.json"
            with mock.patch.object(seed, "clickhouse_query", side_effect=responses) as query:
                code = seed.main([
                    "seed", "--documents", "200", "--measured-cases", "8",
                    "--recovery-cases", "2", "--oracle-json", str(output),
                ])
            self.assertEqual(code, 0)
            self.assertTrue(output.is_file())
            statements = [call.args[1] for call in query.call_args_list]
            self.assertEqual(
                statements[:4],
                [
                    "TRUNCATE TABLE moraine.search_hit_log",
                    "TRUNCATE TABLE moraine.search_query_log",
                    "TRUNCATE TABLE moraine.search_postings",
                    "TRUNCATE TABLE moraine.search_documents",
                ],
            )
            self.assertIn("INSERT INTO moraine.search_documents", statements[4])

    def test_clickhouse_query_preserves_existing_url_parameters(self):
        response = mock.MagicMock()
        response.__enter__.return_value.read.return_value = b"ok"
        with mock.patch.object(seed, "urlopen", return_value=response) as open_url:
            self.assertEqual(
                seed.clickhouse_query("http://clickhouse:8123/?database=moraine", "SELECT 1"),
                "ok",
            )
        parsed = urlparse(open_url.call_args.args[0].full_url)
        self.assertEqual(parse_qs(parsed.query), {"database": ["moraine"], "query": ["SELECT 1"]})

    def test_clean_only_truncates_owned_tables(self):
        with mock.patch.object(seed, "clickhouse_query", return_value="") as query:
            code = seed.main(["clean", "--documents", "200", "--measured-cases", "8", "--recovery-cases", "2"])
        self.assertEqual(code, 0)
        self.assertEqual(query.call_count, 4)


if __name__ == "__main__":
    unittest.main()
