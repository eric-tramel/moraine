#!/usr/bin/env python3
from __future__ import annotations

import copy
import hashlib
import json
import sys
import unittest
from pathlib import Path

BENCH = Path(__file__).resolve().parents[1]
if str(BENCH) not in sys.path:
    sys.path.insert(0, str(BENCH))

from performance_fixtures import (  # noqa: E402
    APPEND_INDEXED_PAYLOAD_TERMS,
    FINGERPRINT_FIELDS,
    FreshSeedTarget,
    FixtureError,
    OneUseTermBank,
    SPLITS,
    build_append_probe_events,
    build_recipe,
    codex_event_lines,
    compute_fingerprints,
    compute_fixture_sha256,
    mixed_control_schedules,
    open_event_schedule,
    open_query_schedule,
    required_split_usage,
    seed_manifest,
    seed_publication_control_sql,
    seed_search_sql,
    validate_event_result,
    validate_query_result,
    validate_recipe,
    validate_split_usage,
)


def structured(oracle: dict[str, object]) -> dict[str, object]:
    identities = oracle["ordered_identities"]
    assert isinstance(identities, list)
    return {
        "schema_version": "moraine.mcp.search_sessions.v1",
        "tool": "search_sessions",
        "request": {"query": "not-used-by-the-independent-oracle"},
        "data": {
            "result_count": len(identities),
            "limit": 10,
            "truncated": oracle["truncated"],
            "results": [
                {
                    "rank": index + 1,
                    "event": {"id": identity["event_id"]},
                    "session": {"id": identity["session_id"]},
                }
                for index, identity in enumerate(identities)
            ],
        },
    }


class RecipeTests(unittest.TestCase):
    def test_smoke_and_full_regenerate_byte_for_byte(self) -> None:
        for profile in ("smoke", "full"):
            with self.subTest(profile=profile):
                first = build_recipe(profile)
                second = build_recipe(profile)
                self.assertEqual(first, second)
                self.assertEqual(
                    json.dumps(first, separators=(",", ":"), sort_keys=True),
                    json.dumps(second, separators=(",", ":"), sort_keys=True),
                )
                validate_recipe(first)
        self.assertNotEqual(build_recipe("smoke")["fixture_sha256"], build_recipe("full")["fixture_sha256"])

    def test_corpus_and_workload_splits_are_disjoint_and_cover_all_dimensions(self) -> None:
        recipe = build_recipe("smoke")
        corpus = recipe["corpus"]
        cursor = 0
        for split in SPLITS:
            spec = corpus["splits"][split]
            self.assertEqual(spec["start"], cursor)
            cursor = spec["end"]
            self.assertEqual(
                {case["mode"] for case in recipe["query_splits"][split]},
                {
                    "rare",
                    "selective",
                    "session_scope",
                    "ranking_tie",
                    "hydration",
                    "event_type_filter",
                    "age_order",
                    "term_frequency",
                },
            )
        self.assertEqual(cursor, corpus["document_count"])

        queries = [
            case["arguments"]["query"]
            for split in SPLITS
            for case in recipe["query_splits"][split]
        ]
        self.assertEqual(len(queries), len(set(queries)))
        answers = {
            split: {
                identity["event_id"]
                for case in recipe["query_splits"][split]
                for identity in case["oracle"]["ordered_identities"]
            }
            for split in SPLITS
        }
        for index, split in enumerate(SPLITS):
            for other in SPLITS[index + 1 :]:
                self.assertTrue(answers[split].isdisjoint(answers[other]))

        event_ids = [
            event["raw_event_uid"]
            for split in SPLITS
            for event in recipe["event_splits"][split]
        ]
        probes = [
            term
            for split in SPLITS
            for event in recipe["event_splits"][split]
            for term in event["probe_terms"]
        ]
        self.assertEqual(len(event_ids), len(set(event_ids)))
        self.assertEqual(len(probes), len(set(probes)))

        dimensions = corpus["dimensions"]
        self.assertEqual(dimensions["session_lengths"], [1, 4, 16, 64])
        self.assertEqual(dimensions["hydration_payload_bytes"], [0, 256, 4096, 16384])
        self.assertEqual(dimensions["age_days"], [0, 1, 7, 30, 90, 180, 365, 730])
        self.assertGreaterEqual(len(dimensions["actor_roles"]), 4)
        self.assertGreaterEqual(len(dimensions["event_types"]), 6)

    def test_each_component_fingerprint_is_sensitive_only_to_its_source(self) -> None:
        original = build_recipe("smoke")
        mutations = {
            "corpus_sha256": lambda value: value["corpus"].__setitem__("source_name", "changed"),
            "query_research_sha256": lambda value: value["query_splits"]["research"][0].__setitem__("case_id", "changed"),
            "query_holdout_sha256": lambda value: value["query_splits"]["holdout"][0].__setitem__("case_id", "changed"),
            "query_stress_sha256": lambda value: value["query_splits"]["stress"][0].__setitem__("case_id", "changed"),
            "event_research_sha256": lambda value: value["event_splits"]["research"][0].__setitem__("marker", "changed"),
            "event_holdout_sha256": lambda value: value["event_splits"]["holdout"][0].__setitem__("marker", "changed"),
            "event_stress_sha256": lambda value: value["event_splits"]["stress"][0].__setitem__("marker", "changed"),
            "split_matrix_sha256": lambda value: value["split_matrix"]["qps"].append("stress"),
            "schedule_templates_sha256": lambda value: value["schedule_templates"]["qps"].__setitem__("duration_ns", 1),
        }
        self.assertEqual(tuple(original["fingerprints"]), FINGERPRINT_FIELDS)
        baseline = compute_fingerprints(original)
        for field, mutate in mutations.items():
            with self.subTest(field=field):
                changed = copy.deepcopy(original)
                mutate(changed)
                observed = compute_fingerprints(changed)
                self.assertNotEqual(observed[field], baseline[field])
                self.assertEqual(
                    {key for key in FINGERPRINT_FIELDS if observed[key] != baseline[key]},
                    {field},
                )
                self.assertNotEqual(compute_fixture_sha256(changed), original["fixture_sha256"])

    def test_fixture_fingerprint_covers_identity_seed_and_all_component_digests(self) -> None:
        recipe = build_recipe("smoke")
        for field in ("recipe_version", "profile", "seed", "fingerprints"):
            with self.subTest(field=field):
                changed = copy.deepcopy(recipe)
                if field in {"recipe_version", "profile"}:
                    changed[field] += "-changed"
                elif field == "seed":
                    changed[field]["value"] += 1
                else:
                    changed[field][FINGERPRINT_FIELDS[0]] = "sha256:" + "0" * 64
                self.assertNotEqual(compute_fixture_sha256(changed), recipe["fixture_sha256"])

    def test_validate_recipe_rejects_mutation_and_unknown_profile(self) -> None:
        recipe = build_recipe("smoke")
        recipe["corpus"]["document_count"] += 1
        with self.assertRaisesRegex(FixtureError, "fingerprint"):
            validate_recipe(recipe)
        with self.assertRaisesRegex(FixtureError, "unsupported profile"):
            build_recipe("tiny")

    def test_required_split_usage_is_exact_for_comparison_and_baseline(self) -> None:
        recipe = build_recipe("smoke")
        comparison = required_split_usage(recipe, "comparison")
        self.assertEqual(
            comparison,
            (
                ("qps", "research"),
                ("qps", "holdout"),
                ("ttr", "research"),
                ("ttr", "holdout"),
                ("etd_idle", "research"),
                ("etd_idle", "holdout"),
                ("etd_loaded", "research"),
                ("etd_loaded", "holdout"),
                ("mixed", "stress"),
            ),
        )
        baseline = required_split_usage(recipe, "baseline")
        validate_split_usage(recipe, comparison, "comparison")
        validate_split_usage(recipe, baseline, "baseline")
        with self.assertRaisesRegex(FixtureError, "missing"):
            validate_split_usage(recipe, comparison[:-1], "comparison")
        with self.assertRaisesRegex(FixtureError, "duplicate"):
            validate_split_usage(recipe, [*comparison, comparison[0]], "comparison")
        with self.assertRaisesRegex(FixtureError, "extra"):
            validate_split_usage(recipe, [*comparison, ("mixed", "research")], "comparison")


class ScheduleAndTermBankTests(unittest.TestCase):
    def setUp(self) -> None:
        self.recipe = build_recipe("smoke")

    def test_append_probe_events_share_one_generation_and_track_exact_offsets(self) -> None:
        events = build_append_probe_events(3, term_count=4)

        self.assertEqual(
            {event["destination_filename"] for event in events},
            {"source-publication-append.jsonl"},
        )
        self.assertEqual(
            [event["expected_source_generation"] for event in events],
            [1, 1, 1],
        )
        self.assertEqual(
            [event["expected_source_line_no"] for event in events],
            [2, 4, 6],
        )
        self.assertEqual(
            [event["expected_source_offset"] for event in events],
            sorted(event["expected_source_offset"] for event in events),
        )
        self.assertEqual(
            len({event["normalized_event_uid"] for event in events}),
            len(events),
        )
        self.assertEqual(
            len({event["indexed_target_term"] for event in events}),
            len(events),
        )
        self.assertEqual(
            {tuple(event["indexed_payload_terms"]) for event in events},
            {tuple(APPEND_INDEXED_PAYLOAD_TERMS)},
        )
        queries: list[str] = []
        for event in events:
            source_text = codex_event_lines(event).decode("utf-8")
            self.assertEqual(source_text.count(event["indexed_target_term"]), 1)
            self.assertTrue(
                all(term in source_text for term in event["indexed_payload_terms"])
            )
            self.assertTrue(
                all(term not in source_text for term in event["probe_terms"])
            )
            bank = OneUseTermBank(event)
            event_queries = [
                bank.claim_query()["query"] for _ in event["probe_terms"]
            ]
            self.assertEqual(
                event_queries,
                [
                    f"{event['indexed_target_term']} {cache_buster}"
                    for cache_buster in event["probe_terms"]
                ],
            )
            queries.extend(event_queries)
        self.assertEqual(len(queries), len(set(queries)))
        self.assertEqual(events, build_append_probe_events(3, term_count=4))

        expanded_warmup = build_append_probe_events(
            3, term_count=4, first_term_count=7
        )
        self.assertEqual(
            [len(event["probe_terms"]) for event in expanded_warmup],
            [7, 4, 4],
        )
        self.assertEqual(
            [event["expected_source_offset"] for event in expanded_warmup],
            sorted(event["expected_source_offset"] for event in expanded_warmup),
        )
        self.assertEqual(
            expanded_warmup,
            build_append_probe_events(3, term_count=4, first_term_count=7),
        )

        with self.assertRaisesRegex(FixtureError, "bounded positive"):
            build_append_probe_events(0)
        with self.assertRaisesRegex(FixtureError, "bounded positive"):
            build_append_probe_events(1, first_term_count=0)
        with self.assertRaisesRegex(FixtureError, "bounded positive"):
            build_append_probe_events(1, first_term_count=1.5)  # type: ignore[arg-type]

    def test_query_schedule_is_open_deterministic_and_response_independent(self) -> None:
        schedule = open_query_schedule(self.recipe, "research", 4)
        self.assertEqual(schedule, open_query_schedule(self.recipe, "research", 4))
        self.assertEqual(len(schedule), 12)
        self.assertEqual(
            [operation["scheduled_offset_ns"] for operation in schedule[:4]],
            [0, 250_000_000, 500_000_000, 750_000_000],
        )
        self.assertEqual([operation["sequence"] for operation in schedule], list(range(12)))
        with self.assertRaisesRegex(FixtureError, "positive"):
            open_query_schedule(self.recipe, "research", 0)

    def test_idle_loaded_and_mixed_controls_reuse_exact_stream_bytes(self) -> None:
        idle = open_event_schedule(self.recipe, "research")
        loaded = open_event_schedule(self.recipe, "research")
        self.assertEqual(idle, loaded)
        controls = mixed_control_schedules(self.recipe, 4)
        self.assertEqual(controls["query_only"]["queries"], controls["combined"]["queries"])
        self.assertEqual(controls["ingest_only"]["events"], controls["combined"]["events"])
        self.assertEqual(controls["query_only"]["events"], [])
        self.assertEqual(controls["ingest_only"]["queries"], [])
        self.assertLess(
            controls["combined"]["events"][0]["scheduled_offset_ns"],
            controls["combined"]["queries"][-1]["scheduled_offset_ns"],
        )

    def test_every_etd_poll_claim_is_unique_and_exhaustion_fails(self) -> None:
        event = self.recipe["event_splits"]["research"][0]
        bank = OneUseTermBank(event)
        claimed = [bank.claim() for _ in range(len(event["probe_terms"]))]
        self.assertEqual(claimed, event["probe_terms"])
        self.assertEqual(len(claimed), len(set(claimed)))
        self.assertEqual(bank.remaining, 0)
        with self.assertRaisesRegex(FixtureError, "exhausted"):
            bank.claim()
        required = (
            self.recipe["schedule_templates"]["etd"]["visibility_timeout_ns"]
            // self.recipe["schedule_templates"]["etd"]["poll_interval_ns"]
            + 1
        )
        self.assertGreaterEqual(len(claimed), required)

        query_bank = OneUseTermBank(event)
        self.assertEqual(
            query_bank.claim_query(),
            {"query": event["probe_terms"][0], "n_hits": 10},
        )


class SeedTests(unittest.TestCase):
    def setUp(self) -> None:
        self.recipe = build_recipe("smoke")
        self.target = FreshSeedTarget(
            database="moraine_perf",
            reset_id="reset-smoke-1",
            sandbox_owned=True,
            fresh_volume=True,
            empty_database=True,
        )

    def test_seed_requires_fresh_owned_empty_target_and_never_destroys_data(self) -> None:
        sql = seed_search_sql(self.target, self.recipe)
        self.assertTrue(sql.startswith("INSERT INTO moraine_perf.events"))
        upper = sql.upper()
        self.assertNotIn("TRUNCATE", upper)
        self.assertNotIn("DROP ", upper)
        self.assertIn("FROM numbers(1000)", sql)
        for field in ("sandbox_owned", "fresh_volume", "empty_database"):
            target = copy.copy(self.target)
            object.__setattr__(target, field, False)
            with self.subTest(field=field), self.assertRaises(FixtureError):
                seed_search_sql(target, self.recipe)
        with self.assertRaisesRegex(FixtureError, "FreshSeedTarget"):
            seed_search_sql("moraine_perf", self.recipe)  # type: ignore[arg-type]

    def test_server_seed_manifest_excludes_queries_events_schedules_and_answers(self) -> None:
        manifest = seed_manifest(self.recipe)
        self.assertEqual(
            set(manifest),
            {"recipe_version", "profile", "seed", "corpus", "corpus_sha256"},
        )
        serialized_manifest = json.dumps(manifest, sort_keys=True)
        sql = seed_search_sql(self.target, self.recipe)
        holdout = self.recipe["query_splits"]["holdout"][0]
        expected_id = holdout["oracle"]["ordered_identities"][0]["event_id"]
        self.assertNotIn(holdout["arguments"]["query"], serialized_manifest)
        self.assertNotIn(expected_id, serialized_manifest)
        self.assertNotIn(expected_id, sql)
        self.assertNotIn("ordered_identities", serialized_manifest)

    def test_publication_control_seed_is_causal_and_dirties_after_the_head(self) -> None:
        statements = seed_publication_control_sql(self.target, self.recipe)

        self.assertEqual(len(statements), 4)
        self.assertIn("ingest_checkpoint_transitions", statements[0])
        self.assertIn("source_generation_publication_readiness", statements[1])
        self.assertIn("published_source_generations", statements[2])
        self.assertIn("mcp_open_dirty_sessions", statements[3])
        self.assertIn("v_live_events", statements[3])
        self.assertIn("'active'", statements[0])
        self.assertIn("final_scan_complete", statements[0])
        self.assertIn("compatibility_prepared", statements[1])
        self.assertIn("publication_revision", statements[2])
        self.assertTrue(all("DROP " not in sql.upper() for sql in statements))
        self.assertTrue(all("TRUNCATE" not in sql.upper() for sql in statements))

    def test_event_serializer_binds_publish_filename_ack_and_every_probe(self) -> None:
        event = self.recipe["event_splits"]["holdout"][0]
        self.assertTrue(event["destination_filename"].endswith(".jsonl"))
        self.assertEqual(
            event["expected_ack_digest"],
            hashlib.sha256(event["normalized_event_uid"].encode("utf-8")).hexdigest(),
        )
        lines = [json.loads(line) for line in codex_event_lines(event).splitlines()]
        self.assertEqual(lines[0]["payload"]["id"], event["raw_session_id"])
        self.assertEqual(lines[1]["payload"]["id"], event["raw_event_uid"])
        text = lines[1]["payload"]["content"][0]["text"]
        self.assertIn(event["marker"], text)
        for term in event["probe_terms"]:
            self.assertIn(term, text)


class ExactOracleTests(unittest.TestCase):
    def setUp(self) -> None:
        recipe = build_recipe("smoke")
        self.case = next(
            case
            for case in recipe["query_splits"]["research"]
            if case["oracle"]["count"] >= 3
        )
        self.valid = structured(self.case["oracle"])

    def test_exact_query_oracle_accepts_the_seed_owned_order(self) -> None:
        validate_query_result(self.valid, self.case)

    def test_exact_query_oracle_rejects_stale_identity(self) -> None:
        stale = copy.deepcopy(self.valid)
        stale["data"]["results"][0]["event"]["id"] = "event:c3RhbGU"
        with self.assertRaisesRegex(FixtureError, "order/identity"):
            validate_query_result(stale, self.case)

    def test_exact_query_oracle_rejects_missing_result(self) -> None:
        missing = copy.deepcopy(self.valid)
        missing["data"]["results"].pop()
        missing["data"]["result_count"] -= 1
        with self.assertRaisesRegex(FixtureError, "count"):
            validate_query_result(missing, self.case)

    def test_exact_query_oracle_rejects_duplicate_result(self) -> None:
        duplicate = copy.deepcopy(self.valid)
        duplicate["data"]["results"][-1] = copy.deepcopy(duplicate["data"]["results"][0])
        with self.assertRaisesRegex(FixtureError, "duplicate"):
            validate_query_result(duplicate, self.case)

    def test_exact_query_oracle_rejects_truncated_result_array(self) -> None:
        truncated = copy.deepcopy(self.valid)
        truncated["data"]["results"] = truncated["data"]["results"][:1]
        truncated["data"]["result_count"] = 1
        with self.assertRaisesRegex(FixtureError, "count"):
            validate_query_result(truncated, self.case)

    def test_exact_query_oracle_rejects_changed_truncation_flag(self) -> None:
        changed = copy.deepcopy(self.valid)
        changed["data"]["truncated"] = not changed["data"]["truncated"]
        with self.assertRaisesRegex(FixtureError, "truncation"):
            validate_query_result(changed, self.case)

    def test_exact_query_oracle_rejects_reordered_results(self) -> None:
        reordered = copy.deepcopy(self.valid)
        reordered["data"]["results"][0], reordered["data"]["results"][1] = (
            reordered["data"]["results"][1],
            reordered["data"]["results"][0],
        )
        with self.assertRaisesRegex(FixtureError, "order/identity"):
            validate_query_result(reordered, self.case)

    def test_exact_event_oracle_rejects_any_stale_or_extra_identity(self) -> None:
        recipe = build_recipe("smoke")
        event = recipe["event_splits"]["stress"][0]
        valid = structured(event["oracle"])
        validate_event_result(valid, event)
        stale = copy.deepcopy(valid)
        stale["data"]["results"].append(
            {
                "event": {"id": "event:c3RhbGU"},
                "session": {"id": "session:c3RhbGU"},
            }
        )
        stale["data"]["result_count"] = 2
        with self.assertRaisesRegex(FixtureError, "count"):
            validate_event_result(stale, event)


if __name__ == "__main__":
    unittest.main()
