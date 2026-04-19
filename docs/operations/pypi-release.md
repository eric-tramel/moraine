# PyPI release procedure

Canonical runbook for shipping moraine to PyPI as
`uv tool install moraine-cli`. See [RFC #219](https://github.com/eric-tramel/moraine/issues/219)
for the rationale.

The PyPI distribution name is `moraine-cli` because the shorter
`moraine` is already taken on PyPI by an unrelated InSAR tool. The
binary + console-script names on the user's `PATH` are still the
original short forms — `moraine`, `moraine-ingest`, `moraine-monitor`,
`moraine-mcp` — since those are entry-point names, independent of the
distribution name.

Moraine publishes via **OIDC trusted publishing** — there is no
long-lived PyPI token in the repo secrets. Trust is mediated by
per-repo+per-workflow+per-environment claims configured on the PyPI
web UI.

## One-time setup

### Production PyPI trusted publisher

1. Log in to <https://pypi.org/> with the account that will own the
   package.
2. The project `moraine-cli` does not need to exist yet — PyPI's
   "pending publisher" flow creates it on first publish.
3. Go to **Account settings → Publishing → Pending publishers → Add a
   new pending publisher** and set:
   - **PyPI Project Name:** `moraine-cli`
   - **Owner:** `eric-tramel`
   - **Repository name:** `moraine`
   - **Workflow name:** `release-moraine.yml`
   - **Environment name:** *leave blank* (we do not gate publishes on a
     GitHub Environment today; see the follow-up note below)
4. Click **Add**. On the first successful publish the pending publisher
   is promoted to an active one.

**GitHub Environment as a future safety gate** (optional, not wired up
today): creating a `pypi` Environment in the repo with required
reviewers and adding `environment: pypi` to the `publish-pypi` job
would require manual approval on every PyPI publish. Worth doing once
the team grows; tracked separately from this runbook.

### TestPyPI trusted publisher (optional — for pre-production rehearsal)

Same flow as above, on <https://test.pypi.org/>, using **PyPI Project
Name:** `moraine-cli`. We do not use TestPyPI in the standard release
path today — the production flow publishes release candidates (`rcN`)
directly to PyPI, where `pip` and `uv tool install` treat them as
installable only when the caller opts in with `--prerelease=allow`.
Keep this section for teams that prefer a staging index.

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

## Release flow (production PyPI)

The standard path is: cut a prerelease tag, publish it to PyPI, verify
it on a clean macOS arm64 box and a clean `ubuntu:22.04` container,
then cut the real tag. Prereleases on PyPI are invisible to normal
`uv tool install moraine-cli` and `pip install moraine-cli` calls —
they only resolve when the caller explicitly opts in (see the
verification commands below).

### 1. Cut the prerelease tag

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

### 2. Trigger the PyPI publish

Once the 3 bundles are up on GH Releases, dispatch the workflow with
`target=pypi`:

```bash
gh workflow run release-moraine.yml \
  -f tag=v0.4.1-rc.1 \
  -f target=pypi
```

The `publish-pypi` job:

1. Rebuilds the bundles (same `release` matrix, `overwrite_files: true`).
2. Downloads all three bundle tarballs from the GH Release.
3. Runs `scripts/build-python-wheels.py` for each target + `scripts/build-python-sdist.py`.
4. Uploads to <https://pypi.org/> via `pypa/gh-action-pypi-publish@release/v1`
   using the OIDC trusted publisher.

### 3. Verify the prerelease

**macOS arm64** (host):

```bash
uv tool install --reinstall --prerelease=allow moraine-cli==0.4.1rc1

moraine --version
moraine up
moraine status
moraine down
uv tool uninstall moraine-cli
```

**Linux x86_64** (fresh `ubuntu:22.04` Docker container):

```bash
docker run --rm -it ubuntu:22.04 bash -lc '
  apt-get update -y && apt-get install -y curl ca-certificates
  curl -LsSf https://astral.sh/uv/install.sh | sh
  export PATH="$HOME/.local/bin:$PATH"
  uv tool install --prerelease=allow moraine-cli==0.4.1rc1
  moraine --version
  moraine up
  moraine status
  moraine down
  uv tool uninstall moraine-cli
'
```

### Verification checklist

- [ ] `uv tool install --prerelease=allow` succeeds on both platforms.
- [ ] `moraine up` starts the stack (ClickHouse + monitor).
- [ ] `moraine status` reports healthy.
- [ ] Monitor UI serves at <http://127.0.0.1:8080/> (from the bundled
      `web/monitor/dist` inside the wheel — pointed to via
      `MORAINE_MONITOR_DIST`).
- [ ] `moraine down` exits cleanly.
- [ ] `uv tool uninstall moraine-cli` removes the shim without residue
      under `~/.local/share/uv/tools/moraine-cli`.

If any step fails, fix the underlying issue, bump the RC suffix
(`v0.4.1-rc.2`), and repeat. Yank the bad prerelease with
`twine yank moraine-cli==0.4.1rc1 --reason "superseded by rc.2"`.

### 4. Cut the real release

Only after a clean RC verification:

1. `git tag v0.4.1 && git push origin v0.4.1` — the `release` matrix
   builds and uploads bundles to the GH Release on push.
2. Dispatch the publish:
   ```bash
   gh workflow run release-moraine.yml \
     -f tag=v0.4.1 \
     -f target=pypi
   ```
3. Confirm the wheels and sdist appear on <https://pypi.org/project/moraine-cli/>.
4. **Canary week:** do *not* update the project README to recommend
   `uv tool install moraine-cli` yet. Post the install command in the
   team channel only. Watch for issues. After a clean week, land the
   README update (tracked separately in
   [issue #228](https://github.com/eric-tramel/moraine/issues/228)).

## Rollback

PyPI does not allow version deletion — use `twine yank` to mark a
version as unsafe-to-install while keeping it downloadable for existing
resolvers. The forward fix is to cut `vX.Y.Z.post1` and republish via a
manual dispatch with `target=pypi`.

```bash
# example: yank a bad wheel set
twine yank moraine-cli==0.4.1 --reason "packaging regression; use 0.4.1.post1"
```
