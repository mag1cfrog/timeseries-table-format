# Develop the Python package

Use this workflow to build the Python extension locally and run its checks.
Release operations are documented separately because the Rust crate and Python
package share one workspace version and release.

## Prerequisites

Install:

- a stable Rust toolchain
- Python 3.10 or later
- [uv](https://docs.astral.sh/uv/)

## Build the extension and run tests

From the repository root:

```bash
cd crates/timeseries-table-python
uv sync --group dev
uv run maturin develop --features test-utils
uv run pytest -q
```

Run `maturin develop` again after changing Rust code so the virtual environment
uses the rebuilt extension.

## Run Python quality checks

From `crates/timeseries-table-python`:

```bash
uv run ruff check python tests
uv run ruff format --check python tests
uv run ty check python tests
```

Run the workspace Rust checks from the repository root:

```bash
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo test --locked --workspace --all-targets --all-features --no-fail-fast
```

## Build the documentation

From the repository root:

```bash
uv run --no-project \
  --with-requirements docs-site/requirements.txt \
  python -m zensical build --strict -f docs-site/mkdocs.yml
```

The generated site is written to `docs-site/site/` and is not committed.

## Run Python benchmarks

Follow [Streaming query performance](performance.md#reproduce-the-benchmark) to
build the extension and run `bench/sql_conversion.py`. Use
`python bench/sql_conversion.py --help` from the Python crate directory for
the complete option reference.

## Prepare a release

Follow the
[workspace release guide](https://github.com/mag1cfrog/timeseries-table-format/blob/main/docs/releasing-crates-io.md).
Do not create a Python-only version, tag, or GitHub release.
