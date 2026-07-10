#!/usr/bin/env python3

from __future__ import annotations

import tempfile
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import dependency_policy as policy


def metadata_fixture(*dependents: str) -> dict[str, object]:
    package_ids = {name: f"path+file:///{name}#0.0.0" for name in dependents}
    packages = [
        {
            "id": package_id,
            "name": name,
            "dependencies": [{"name": policy.CLICKHOUSE_PACKAGE}],
        }
        for name, package_id in package_ids.items()
    ]
    packages.append(
        {
            "id": "path+file:///moraine-clickhouse#0.0.0",
            "name": policy.CLICKHOUSE_PACKAGE,
            "dependencies": [],
        }
    )
    return {
        "workspace_members": [
            *package_ids.values(),
            "path+file:///moraine-clickhouse#0.0.0",
        ],
        "packages": packages,
    }


class ClickHouseDependencyPolicyTests(unittest.TestCase):
    def test_exact_allowlist_is_accepted(self) -> None:
        dependents = policy.assert_clickhouse_dependency_policy(
            metadata_fixture(*sorted(policy.ALLOWED_DIRECT_DEPENDENTS))
        )
        self.assertEqual(dependents, tuple(sorted(policy.ALLOWED_DIRECT_DEPENDENTS)))

    def test_forbidden_direct_edge_is_rejected_with_epic_rule_and_offender(self) -> None:
        fixture = metadata_fixture(
            *sorted(policy.ALLOWED_DIRECT_DEPENDENTS), "moraine-mcp-core"
        )
        with self.assertRaises(policy.PolicyViolation) as raised:
            policy.assert_clickhouse_dependency_policy(fixture)

        message = str(raised.exception)
        self.assertIn("epic #451 Phase 1 dependency rule", message)
        self.assertIn("moraine-mcp-core", message)
        self.assertIn("outside the allowlist", message)


class MonitorSqlPolicyTests(unittest.TestCase):
    def test_comments_and_standard_test_sources_are_ignored(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            source_root = Path(temporary)
            (source_root / "lib.rs").write_text(
                """
// \"SELECT * FROM comments\"
/* \"INSERT INTO comments VALUES (1)\" */
const DESCRIPTION: &str = "selectivity is not a SQL statement";
const NOTES: &str = include_str!("notes.sql");
#[cfg(test)]
mod tests {
    const QUERY: &str = "SELECT * FROM test_rows";
    const WRITE: &str = r#"INSERT INTO test_rows VALUES (1)"#;
    const INCLUDED: &str = include_str!("test_query.sql");
}
#[test]
fn top_level_test() {
    let _ = "SELECT * FROM top_level_test_rows";
}
#[tokio::test(flavor = "current_thread")]
async fn tokio_test() {
    let _ = "INSERT INTO tokio_test_rows VALUES (1)";
}
#[cfg(all(unix, test))]
mod unix_tests {
    const QUERY: &str = "SELECT * FROM unix_test_rows";
}
""",
                encoding="utf-8",
            )
            fixture_dir = source_root / "fixtures"
            fixture_dir.mkdir()
            (fixture_dir / "sql.rs").write_text(
                'const QUERY: &str = "SELECT * FROM fixture_rows";\n',
                encoding="utf-8",
            )
            (source_root / "tests.rs").write_text(
                'const QUERY: &str = "SELECT * FROM external_test_rows";\n',
                encoding="utf-8",
            )
            (source_root / "notes.sql").write_text(
                "-- SELECT * FROM comments\n/* INSERT INTO comments VALUES (1) */\n",
                encoding="utf-8",
            )
            (source_root / "test_query.sql").write_text(
                "SELECT * FROM external_test_rows\n",
                encoding="utf-8",
            )


            self.assertEqual(policy.assert_monitor_sql_policy(source_root), ())

    def test_production_select_literal_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            source_root = Path(temporary)
            (source_root / "lib.rs").write_text(
                'const QUERY: &str = "SELECT * FROM sessions";\n',
                encoding="utf-8",
            )

            with self.assertRaises(policy.PolicyViolation) as raised:
                policy.assert_monitor_sql_policy(source_root)
            message = str(raised.exception)
            self.assertIn("lib.rs:1: SELECT statement literal", message)
            self.assertIn("epic #451 Phase 1 dependency rule", message)

    def test_production_select_token_forms_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            source_root = Path(temporary)
            (source_root / "lib.rs").write_text(
                'const STAR: &str = "SELECT* FROM sessions";\n'
                'const PAREN: &str = "SELECT(1)";\n',
                encoding="utf-8",
            )

            with self.assertRaises(policy.PolicyViolation):
                policy.assert_monitor_sql_policy(source_root)

    def test_escaped_production_select_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            source_root = Path(temporary)
            (source_root / "lib.rs").write_text(
                r'const QUERY: &str = "\x53ELECT * FROM sessions";' + "\n",
                encoding="utf-8",
            )

            with self.assertRaises(policy.PolicyViolation):
                policy.assert_monitor_sql_policy(source_root)

    def test_cfg_any_test_or_feature_remains_production(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            source_root = Path(temporary)
            (source_root / "lib.rs").write_text(
                """
#[cfg(any(test, feature = "debug-sql"))]
fn query() {
    let _ = "SELECT * FROM sessions";
}
""",
                encoding="utf-8",
            )

            with self.assertRaises(policy.PolicyViolation):
                policy.assert_monitor_sql_policy(source_root)

    def test_nested_cfg_that_can_run_without_test_is_scanned(self) -> None:
        expressions = (
            "all(unix, not(test))",
            'all(unix, any(test, feature = "debug-sql"))',
        )
        for expression in expressions:
            with self.subTest(expression=expression):
                with tempfile.TemporaryDirectory() as temporary:
                    source_root = Path(temporary)
                    (source_root / "lib.rs").write_text(
                        f"""
#[cfg({expression})]
fn query() {{
    let _ = "SELECT * FROM sessions";
}}
""",
                        encoding="utf-8",
                    )

                    with self.assertRaises(policy.PolicyViolation):
                        policy.assert_monitor_sql_policy(source_root)

    def test_production_include_str_sql_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            source_root = Path(temporary)
            (source_root / "lib.rs").write_text(
                'const QUERY: &str = include_str!("query.sql");\n',
                encoding="utf-8",
            )
            (source_root / "query.sql").write_text(
                "-- ignored comment\nSELECT * FROM sessions\n",
                encoding="utf-8",
            )

            with self.assertRaises(policy.PolicyViolation) as raised:
                policy.assert_monitor_sql_policy(source_root)
            self.assertIn("query.sql:2: SELECT statement literal", str(raised.exception))

    def test_unresolved_production_include_str_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            source_root = Path(temporary)
            (source_root / "lib.rs").write_text(
                'const QUERY: &str = include_str!(concat!('
                'env!("CARGO_MANIFEST_DIR"), "/query.sql"));\n',
                encoding="utf-8",
            )

            with self.assertRaises(policy.PolicyViolation) as raised:
                policy.assert_monitor_sql_policy(source_root)
            self.assertIn("cannot verify production include_str!", str(raised.exception))

    def test_comment_separated_insert_into_is_rejected(self) -> None:
        queries = (
            "INSERT/**/INTO sessions VALUES (1)",
            "INSERT// note\nINTO sessions VALUES (1)",
            "INSERT#! note\nINTO sessions VALUES (1)",
            "INSERT# note\nINTO sessions VALUES (1)",
            "INSERT/* outer /* nested */ outer */INTO sessions VALUES (1)",
        )
        for query in queries:
            with self.subTest(query=query):
                with tempfile.TemporaryDirectory() as temporary:
                    source_root = Path(temporary)
                    (source_root / "lib.rs").write_text(
                        f'const QUERY: &str = r#"{query}"#;\n',
                        encoding="utf-8",
                    )

                    with self.assertRaises(policy.PolicyViolation):
                        policy.assert_monitor_sql_policy(source_root)

    def test_production_insert_into_literal_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            source_root = Path(temporary)
            (source_root / "writer.rs").write_text(
                'const QUERY: &str = r#"INSERT INTO sessions VALUES (1)"#;\n',
                encoding="utf-8",
            )

            with self.assertRaises(policy.PolicyViolation) as raised:
                policy.assert_monitor_sql_policy(source_root)
            self.assertIn(
                "writer.rs:1: INSERT INTO statement literal", str(raised.exception)
            )


if __name__ == "__main__":
    unittest.main()
