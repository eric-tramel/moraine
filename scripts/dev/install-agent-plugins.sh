#!/usr/bin/env bash
set -euo pipefail

CODEX_CMD="${CODEX_CMD:-codex}"
AGENT_PLUGINS_SOURCE="${AGENT_PLUGINS_SOURCE:-main}"
AGENT_PLUGINS_REMOTE="${AGENT_PLUGINS_REMOTE:-origin}"
INSTALL_PLUGIN_NAMES=("moraine-dev")
LEGACY_MARKETPLACE_NAMES=("moraine-developer-agents" "moraine-dev")

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

case "$AGENT_PLUGINS_SOURCE" in
    main)
        git_source="$(git remote get-url "$AGENT_PLUGINS_REMOTE" 2>/dev/null)" \
            || die "git remote not found: $AGENT_PLUGINS_REMOTE"
        marketplace_source=("$git_source" --ref main)
        source_label="${AGENT_PLUGINS_REMOTE}/main"
        should_upgrade=1
        ;;
    current)
        marketplace_source=("$repo_root")
        source_label="$repo_root"
        should_upgrade=0
        ;;
    *)
        die "AGENT_PLUGINS_SOURCE must be main or current"
        ;;
esac

add_error_file="$(mktemp "${TMPDIR:-/tmp}/moraine-agent-plugins.XXXXXX")"
trap 'rm -f "$add_error_file"' EXIT

printf '[agent-plugins] registering Codex marketplace: %s\n' "$source_label"
if ! add_json="$("$CODEX_CMD" plugin marketplace add "${marketplace_source[@]}" --json 2>"$add_error_file")"; then
    add_error="$(<"$add_error_file")"
    conflict_name="$(ADD_ERROR="$add_error" python3 - <<'PY'
import os
import re

match = re.search(
    r"marketplace '([^']+)' (?:is )?already added from a different source",
    os.environ["ADD_ERROR"],
)
print(match.group(1) if match else "")
PY
)"
    if [[ -n "$conflict_name" ]]; then
        printf '[agent-plugins] replacing %s marketplace source\n' "$conflict_name"
        "$CODEX_CMD" plugin marketplace remove "$conflict_name" >/dev/null
        add_json="$("$CODEX_CMD" plugin marketplace add "${marketplace_source[@]}" --json)"
    else
        printf '%s\n' "$add_error" >&2
        die "failed to register Codex marketplace"
    fi
fi

marketplace_name="$(MARKETPLACE_ADD_JSON="$add_json" python3 - <<'PY'
import json
import os

data = json.loads(os.environ["MARKETPLACE_ADD_JSON"])
print(data.get("marketplaceName", ""))
PY
)"
[[ -n "$marketplace_name" ]] || die "Codex did not report a marketplace name"

for legacy_name in "${LEGACY_MARKETPLACE_NAMES[@]}"; do
    if [[ "$marketplace_name" != "$legacy_name" ]]; then
        "$CODEX_CMD" plugin marketplace remove "$legacy_name" >/dev/null 2>&1 || true
    fi
done

if [[ "$should_upgrade" == 1 ]]; then
    printf '[agent-plugins] syncing Codex marketplace: %s\n' "$marketplace_name"
    "$CODEX_CMD" plugin marketplace upgrade "$marketplace_name" >/dev/null
fi

plugin_list_json="$("$CODEX_CMD" plugin list --marketplace "$marketplace_name" --available --json)"
if ! plugin_names="$(PLUGIN_LIST_JSON="$plugin_list_json" INSTALL_PLUGIN_NAMES="${INSTALL_PLUGIN_NAMES[*]}" python3 - <<'PY'
import json
import os
import sys

data = json.loads(os.environ["PLUGIN_LIST_JSON"])
wanted = os.environ["INSTALL_PLUGIN_NAMES"].split()
seen = set()

for section in ("installed", "available"):
    for plugin in data.get(section, []):
        name = plugin.get("name")
        if name:
            seen.add(name)

missing = [name for name in wanted if name not in seen]
if missing:
    print(
        "marketplace is missing developer workflow plugin(s): " + ", ".join(missing),
        file=sys.stderr,
    )
    sys.exit(1)

for name in wanted:
    print(name)
PY
)"; then
    die "marketplace is missing developer workflow plugin(s): ${INSTALL_PLUGIN_NAMES[*]}"
fi
[[ -n "$plugin_names" ]] || die "no developer workflow plugins selected for marketplace: $marketplace_name"

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
