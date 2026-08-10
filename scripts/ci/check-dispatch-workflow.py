#!/usr/bin/env python3
"""Structural security regression checks for issue dispatch."""

import re
from pathlib import Path


workflow = Path(__file__).resolve().parents[2] / ".github/workflows/dispatch-issue.yml"
text = workflow.read_text(encoding="utf-8")

required = (
    "issue_comment:",
    "types: [created]",
    "contents: read",
    "id-token: write",
    "github.event.repository.id == 1158239756",
    "github.event.comment.user.id == 1223539",
    "github.event.comment.body == '/dispatch'",
    "if jq -e '.comment.body == \"/dispatch\"' \"$GITHUB_EVENT_PATH\" >/dev/null; then",
    "github.event.issue.state == 'open'",
    "!github.event.issue.pull_request",
    "needs: gate",
    "if: needs.gate.outputs.dispatch == 'true'",
    "dispatch: ${{ steps.exact_command.outputs.dispatch }}",
    "cancel-in-progress: false",
    "group: dispatch-${{ github.event.repository.id }}-${{ github.event.issue.number }}",
    "tailscale/github-action@306e68a486fd2350f2bfc3b19fcd143891a4a2d8",
    "tags: tag:github-dispatch",
    "use-cache: false",
    'StrictHostKeyChecking=yes',
    'UserKnownHostsFile=$known_hosts_file',
    '"$GITHUB_EVENT_PATH"',
    '"moraine-dispatch@$DISPATCH_HOST"',
)
for fragment in required:
    if fragment not in text:
        raise SystemExit(f"dispatch workflow is missing invariant: {fragment}")

for forbidden in (
    "pull_request_target:",
    "actions/checkout@",
    "ssh-keyscan",
    "StrictHostKeyChecking=no",
    "issues: write",
    "contents: write",
    "github.event.issue.title",
    "github.event.issue.body",
):
    if forbidden in text:
        raise SystemExit(f"dispatch workflow contains forbidden fragment: {forbidden}")

permission_match = re.search(r"(?m)^permissions:\n((?:  [^\n]+\n)+)", text)
if permission_match is None or permission_match.group(0) != "permissions:\n  contents: read\n":
    raise SystemExit("dispatch workflow permissions are not exact")

gate_if_match = re.search(r"(?m)^    if: >-\n(?:      [^\n]+\n)+(?=    runs-on:)", text)
if gate_if_match is None or gate_if_match.group(0) != (
    "    if: >-\n"
    "      github.event.repository.id == 1158239756 &&\n"
    "      github.event.comment.user.id == 1223539 &&\n"
    "      github.event.comment.body == '/dispatch' &&\n"
    "      github.event.issue.state == 'open' &&\n"
    "      !github.event.issue.pull_request\n"
):
    raise SystemExit("dispatch workflow coarse job gate is not exact")

dispatch_permissions = re.search(
    r"(?m)^  dispatch:\n(?:    [^\n]+\n)*?    permissions:\n((?:      [^\n]+\n)+)", text
)
if dispatch_permissions is None or dispatch_permissions.group(1) != (
    "      contents: read\n      id-token: write\n"
):
    raise SystemExit("dispatch job permissions are not exact")

dispatch_if_match = re.search(
    r"(?m)^  dispatch:\n    needs: gate\n(    if: [^\n]+\n)", text
)
if dispatch_if_match is None or dispatch_if_match.group(1) != (
    "    if: needs.gate.outputs.dispatch == 'true'\n"
):
    raise SystemExit("dispatch job authorization gate is not exact")

print("dispatch workflow structural checks passed")
