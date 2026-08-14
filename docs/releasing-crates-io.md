# Releasing the workspace

This workspace has one version, one root `CHANGELOG.md`, and one canonical GitHub
tag and release. A production release publishes the same version to crates.io and
PyPI.

The canonical tag format is `timeseries-table-format-v<VERSION>`.

## One-time trusted publisher setup

Create these approval-gated GitHub environments:

- `crates-io-release`
- `pypi`
- `testpypi`
- `testpypi-snapshot`

Configure a crates.io trusted publisher for `timeseries-table-format` with:

- Repository: `mag1cfrog/timeseries-table-format`
- Workflow: `publish-crates.yml`
- Environment: `crates-io-release`

Configure PyPI and TestPyPI trusted publishers with their matching workflow and
environment names.

## Production release

For the first unified `0.3.0` release only, merge the implementation PR and
manually run the `Release-plz` workflow. This creates the first canonical
baseline without asking release-plz to compare the retired split-package tags.
The workflow refuses to create releases on ordinary `main` pushes.

After that bootstrap release:

1. Merge conventional commits to `main`.
2. Review and merge the release-plz PR. It updates the workspace version and the
   root `CHANGELOG.md`.
3. Release-plz creates the canonical tag and GitHub release.
4. Approve the `crates-io-release` deployment. The crates workflow verifies the
   tag and package with the CLI enabled, then publishes the canonical
   `timeseries-table-format` crate.
5. Approve the `pypi` deployment. The GitHub release event builds, tests, and
   publishes the Python distributions with the same version.
6. Verify that the canonical GitHub release, crates.io package, and PyPI package
   all show that exact version.

Do not create package-specific tags or releases. The retired
`timeseries-table-core`, `timeseries-table-datafusion`, and
`timeseries-table-cli` packages remain available for existing users, but
receive no new releases. See the
[source migration guide](../crates/timeseries-table-format/README.md#source-migration).

## TestPyPI

Use the `TestPyPI release rehearsal` workflow before a production release when a
full wheel and sdist rehearsal is useful. Keep its `snapshot` input enabled so
the build uses a unique development version and cannot collide with a production
version.

The `TestPyPI snapshot` workflow automatically publishes an Ubuntu wheel after
relevant changes land on `main`. TestPyPI uploads are disposable and may be
rerun; production PyPI uploads are immutable.

## Recovering a partial release

Rerun a failed crates.io workflow against the existing canonical tag. It skips
the canonical package when that exact version is already published.

Rerun PyPI only if no production file was accepted. If PyPI accepted only part
of the distribution set, do not overwrite or silently skip those files; fix
forward with a new unified patch version.

Never move or recreate a release tag. If a published artifact is defective,
prepare a fix and release a new patch version. Do not yank or delete a normal
release as a rollback mechanism.
