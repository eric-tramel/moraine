from __future__ import annotations

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).parents[1] / "bump-version.py"
spec = importlib.util.spec_from_file_location("bump_version", SCRIPT)
bump = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(bump)


class RuntimeManifestVersionTests(unittest.TestCase):
    def fixture(self):
        temp = tempfile.TemporaryDirectory()
        root = Path(temp.name)
        manifests = [
            ("plugins/moraine/.claude-plugin/plugin.json", "moraine"),
            ("plugins/moraine/.codex-plugin/plugin.json", "moraine"),
            ("plugins/prime-agent-moraine/package.json", "moraine-prime-agent"),
        ]
        for rel, name in manifests:
            path = root / rel
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps({"name": name, "version": "0.7.2"}))
        python_skill = root / "plugins/prime-agent-moraine/skills/moraine/pyproject.toml"
        python_skill.parent.mkdir(parents=True, exist_ok=True)
        python_skill.write_text(
            '[project]\nname = "prime-agent-skill-moraine"\nversion = "0.7.2"\n'
        )
        hermes = root / "plugins/hermes-moraine/plugin.yaml"
        hermes.parent.mkdir(parents=True, exist_ok=True)
        hermes.write_text('name: "moraine"\nversion: "0.7.2"\n')
        return temp, root, manifests

    def test_updates_prime_manifest_with_other_runtime_manifests(self):
        temp, root, manifests = self.fixture()
        with temp:
            bump.bump_runtime_plugin_manifests(root, "0.7.2", "0.8.0", dry_run=False)
            for rel, _ in manifests:
                self.assertEqual(json.loads((root / rel).read_text())["version"], "0.8.0")
            self.assertIn('version: "0.8.0"', (root / "plugins/hermes-moraine/plugin.yaml").read_text())
            self.assertIn(
                'version = "0.8.0"',
                (root / "plugins/prime-agent-moraine/skills/moraine/pyproject.toml").read_text(),
            )

    def test_rejects_missing_wrong_or_non_string_prime_version(self):
        for value in (None, "0.7.1", 702):
            with self.subTest(value=value):
                temp, root, _ = self.fixture()
                with temp:
                    prime = root / "plugins/prime-agent-moraine/package.json"
                    data = json.loads(prime.read_text())
                    if value is None:
                        data.pop("version")
                    else:
                        data["version"] = value
                    prime.write_text(json.dumps(data))
                    with self.assertRaises(SystemExit):
                        bump.bump_runtime_plugin_manifests(
                            root, "0.7.2", "0.8.0", dry_run=True
                        )


if __name__ == "__main__":
    unittest.main()
