#!/usr/bin/env bash
set -euo pipefail

CODEX_CMD="${CODEX_CMD:-codex}"

die() {
    printf '[agent-plugins] error: %s\n' "$*" >&2
    exit 1
}

require_command() {
    local name="$1"
    command -v "$name" >/dev/null 2>&1 || die "$name is required but was not found on PATH"
}

require_command "$CODEX_CMD"
require_command python3

"$CODEX_CMD" plugin --help >/dev/null 2>&1 || die "$CODEX_CMD does not support 'plugin' commands"

repo_root="$(git rev-parse --show-toplevel 2>/dev/null)" || die "run this from inside the Moraine repository"
repo_root="$(cd "$repo_root" && pwd -P)"
marketplace_file="$repo_root/.agents/plugins/marketplace.json"
[[ -f "$marketplace_file" ]] || die "missing marketplace file: $marketplace_file"

marketplace_name="$(python3 - "$marketplace_file" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as fh:
    data = json.load(fh)

print(data.get("name", ""))
PY
)"
[[ -n "$marketplace_name" ]] || die "marketplace file has no name: $marketplace_file"

plugin_names="$(python3 - "$marketplace_file" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as fh:
    data = json.load(fh)

for plugin in data.get("plugins", []):
    name = plugin.get("name")
    if name:
        print(name)
PY
)"
[[ -n "$plugin_names" ]] || die "marketplace has no plugins: $marketplace_file"

add_error_file="$(mktemp "${TMPDIR:-/tmp}/moraine-agent-plugins.XXXXXX")"
trap 'rm -f "$add_error_file"' EXIT

printf '[agent-plugins] registering Codex marketplace: %s\n' "$repo_root"
if ! "$CODEX_CMD" plugin marketplace add "$repo_root" >/dev/null 2>"$add_error_file"; then
    add_error="$(<"$add_error_file")"
    if [[ "$add_error" == *"already added from a different source"* ]]; then
        printf '[agent-plugins] replacing %s marketplace source\n' "$marketplace_name"
        "$CODEX_CMD" plugin marketplace remove "$marketplace_name" >/dev/null
        "$CODEX_CMD" plugin marketplace add "$repo_root" >/dev/null
    else
        printf '%s\n' "$add_error" >&2
        die "failed to register Codex marketplace: $marketplace_name"
    fi
fi

while IFS= read -r plugin_name; do
    [[ -n "$plugin_name" ]] || continue
    install_json="$("$CODEX_CMD" plugin add "${plugin_name}@${marketplace_name}" --json)"
    version="$(PLUGIN_INSTALL_JSON="$install_json" python3 - <<'PY'
import json
import os

data = json.loads(os.environ["PLUGIN_INSTALL_JSON"])
print(data.get("version", "unknown"))
PY
)"
    printf '[agent-plugins] installed %s@%s (%s)\n' "$plugin_name" "$marketplace_name" "$version"
done <<< "$plugin_names"

printf '[agent-plugins] done\n'
