# Releasing Rust crates to crates.io (OIDC / Trusted Publishing)

This repo publishes these crates to crates.io:

- `timeseries-table-core`
- `timeseries-table-datafusion`
- `timeseries-table-format`
- `timeseries-table-cli`

Publishing is done from GitHub Actions using crates.io Trusted Publishing (OIDC) and is approval-gated via a GitHub Actions environment.

## One-time setup (repo + crates.io)

1) **GitHub Actions environment**
   - Create an environment named `crates-io-release`.
   - Configure required reviewers (manual approval gate).

2) **crates.io Trusted Publisher (per crate)**
   - For each crate above, configure a Trusted Publisher on crates.io:
     - Repository: `mag1cfrog/timeseries-table-format`
     - Workflow filename: `publish-crates.yml` (must live in `.github/workflows/`)
     - Environment: `crates-io-release`

## Release flow

1) Merge the `release-plz` PR(s) that bump versions.
2) `release-plz` creates tags like `timeseries-table-core-v0.2.1`.
3) The tag triggers the GitHub Actions workflow `Publish (crates.io)` (`.github/workflows/publish-crates.yml`).
4) Approve the `crates-io-release` environment deployment when prompted.
5) The workflow publishes any missing crate versions in dependency order and skips versions that already exist on crates.io.
