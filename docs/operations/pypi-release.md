# PyPI release procedure

Canonical runbook for shipping moraine to TestPyPI and real PyPI as
`uv tool install moraine`. See [RFC #219](https://github.com/eric-tramel/moraine/issues/219)
for the rationale.

Moraine publishes via **OIDC trusted publishing** — there is no
long-lived PyPI token in the repo secrets. Trust is mediated by
per-repo+per-workflow+per-environment claims configured on the PyPI
web UI.

## One-time setup

Repeat once per index (TestPyPI for rehearsal, PyPI for production). You
only need to do this when bootstrapping a new package or re-registering
after a transfer.

### TestPyPI trusted publisher

1. Log in to <https://test.pypi.org/> with the account that will own the
   package.
2. Create the project `moraine` by publishing a manual `0.0.0.dev0`
   placeholder wheel (`twine upload --repository testpypi …`), or rely
   on the first publish run claiming the name. Trusted publishing works
   even for a not-yet-created project via the "pending publisher" flow.
3. Go to **Account → Publishing → Pending publishers → Add a new pending
   publisher** and set:
   - **PyPI project name:** `moraine`
   - **Owner:** `eric-tramel`
   - **Repository name:** `moraine`
   - **Workflow name:** `release-moraine.yml`
   - **Environment name:** leave blank (we don't gate publishes on a
     GitHub Environment today)
4. Save. Once the first real publish happens, TestPyPI promotes this
   from pending to active.

### Production PyPI trusted publisher

Same as above, on <https://pypi.org/>. Do **not** promote the production
publisher until the TestPyPI rehearsal has cleared the acceptance
criteria in [issue #225](https://github.com/eric-tramel/moraine/issues/225).

## Tag + version conventions

The release workflow key `workflow_dispatch.inputs.tag` accepts:

| Tag                     | Normalized version | Notes                                 |
| ----------------------- | ------------------ | ------------------------------------- |
| `v0.4.1`                | `0.4.1`            | Standard release                      |
| `v0.4.1-rc.1`           | `0.4.1rc1`         | Rehearsal / release candidate         |
| `v0.4.1-beta.2`         | `0.4.1b2`          | Public beta                           |
| `v0.4.1-alpha.1`        | `0.4.1a1`          | Internal alpha                        |
| `v0.4.1.post1`          | `0.4.1.post1`      | Wheel-only packaging fix (no Rust rebuild) |
| `v0.4.1.dev0`           | `0.4.1.dev0`       | Unattached dev build, manual dispatch only |

Normalization is performed by `scripts/build-python-wheels.py` and
mirrored inline in the `Resolve tag and PEP 440 version` step of the
workflow — keep both in sync.

## Rehearsal (TestPyPI)

Run this end-to-end before every production release of material changes
to the packaging scripts, the exec shim, or the bundle layout.

### 1. Cut the rehearsal tag

```bash
git checkout main
git pull --ff-only
git tag v0.4.1-rc.1
git push origin v0.4.1-rc.1
```

Pushing the tag kicks off the `release` matrix (3 platforms) which
builds bundles and uploads them to the GH Release. No PyPI publish
happens on the tag-triggered run — `workflow_dispatch.inputs.target`
defaults to `skip` and tag triggers ignore the `target` input entirely.

### 2. Trigger the publish

Once the 3 bundles are up on GH Releases, dispatch the workflow with
`target=test-pypi`:

```bash
gh workflow run release-moraine.yml \
  -f tag=v0.4.1-rc.1 \
  -f target=test-pypi
```

The `publish-pypi` job:

1. Rebuilds the bundles (same `release` matrix, `overwrite_files: true`).
2. Downloads all three bundle tarballs from the GH Release.
3. Runs `scripts/build-python-wheels.py` for each target + `scripts/build-python-sdist.py`.
4. Uploads to <https://test.pypi.org/legacy/> via
   `pypa/gh-action-pypi-publish@release/v1` using the OIDC trusted
   publisher.

### 3. Validate the install

**macOS arm64** (host):

```bash
uv tool install --reinstall \
  --index https://test.pypi.org/simple/ \
  --index-strategy unsafe-best-match \
  moraine==0.4.1rc1

moraine --version
moraine up
moraine status
moraine down
uv tool uninstall moraine
```

**Linux x86_64** (fresh `ubuntu:22.04` Docker container):

```bash
docker run --rm -it ubuntu:22.04 bash -lc '
  apt-get update -y && apt-get install -y curl ca-certificates
  curl -LsSf https://astral.sh/uv/install.sh | sh
  export PATH="$HOME/.local/bin:$PATH"
  uv tool install \
    --index https://test.pypi.org/simple/ \
    --index-strategy unsafe-best-match \
    moraine==0.4.1rc1
  moraine --version
  moraine up
  moraine status
  moraine down
  uv tool uninstall moraine
'
```

### Acceptance checklist

- [ ] `uv tool install` succeeds on both platforms.
- [ ] `moraine up` starts the stack (ClickHouse + monitor).
- [ ] `moraine status` reports healthy.
- [ ] Monitor UI serves at <http://127.0.0.1:8080/> (from the bundled
      `web/monitor/dist` inside the wheel — pointed to via
      `MORAINE_MONITOR_DIST`).
- [ ] `moraine down` exits cleanly.
- [ ] `uv tool uninstall moraine` removes the shim without residue
      under `~/.local/share/uv/tools/moraine`.

If any step fails, fix the underlying issue, bump the RC suffix
(`v0.4.1-rc.2`), and repeat.

## Production release

Only after a clean TestPyPI rehearsal:

1. Cut the real tag (`git tag v0.4.1 && git push origin v0.4.1`) — the
   `release` matrix builds and uploads bundles to the GH Release on
   push.
2. Manually dispatch:
   ```bash
   gh workflow run release-moraine.yml \
     -f tag=v0.4.1 \
     -f target=pypi
   ```
3. Confirm the wheels and sdist appear on <https://pypi.org/project/moraine/>.
4. **Canary week:** do *not* update the project README to recommend
   `uv tool install moraine` yet. Post the install command in the team
   channel only. Watch for issues. After a clean week, land the README
   update (tracked separately in
   [issue #228](https://github.com/eric-tramel/moraine/issues/228)).

## Rollback

PyPI does not allow version deletion — use `twine yank` to mark a
version as unsafe-to-install while keeping it downloadable for existing
resolvers. The forward fix is to cut `vX.Y.Z.post1` and republish via a
manual dispatch with `target=pypi`.

```bash
# example: yank a bad wheel set
twine yank moraine==0.4.1 --reason "packaging regression; use 0.4.1.post1"
```
