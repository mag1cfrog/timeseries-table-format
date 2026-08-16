# Streaming query performance

These benchmarks compare `Session.sql_reader(...)` with the fully materialized
`Session.sql(...)` result path.

## Test setup

The generated dataset contains about 10.5 million rows. Each result is the
median of three measured Linux runs after one warmup, using a local SSD and one
process.

## Time to first batch

| Query | `sql_reader(...)` | `Session.sql(...)` | Improvement |
|---|---:|---:|---:|
| `SELECT * FROM prices` | **370.7 ms** | 2,312 ms | 84% earlier |
| `SELECT * FROM prices ORDER BY ts` | **2,489 ms** | 13,182 ms | 81% earlier |

`sql_reader(...)` yields batches as the engine produces them. `Session.sql(...)`
must collect the complete result before returning.

## Peak process memory

These measurements process each batch immediately rather than retaining the
full result.

| Query | `sql_reader(...)` | `Session.sql(...)` and iterate | Reduction |
|---|---:|---:|---:|
| `SELECT * FROM prices` | **2.30 GiB** | 3.60 GiB | 36% lower |
| `SELECT * FROM prices ORDER BY ts` | **3.66 GiB** | 4.84 GiB | 24% lower |

The streaming path can discard each processed batch instead of retaining a
materialized table.

Calling `sql_reader(...).read_all()` removes this memory advantage. Its
performance is in the same range as `Session.sql(...)` when both paths collect
the complete result.

## Reproduce the benchmark

From the repository root:

```bash
cd crates/timeseries-table-python
uv pip install -p .venv/bin/python numpy
uv run -p .venv/bin/python maturin develop --features test-utils
.venv/bin/python bench/sql_conversion.py \
    --target-ipc-gb 2 \
    --warmups 1 \
    --runs 3 \
    --include-streaming \
    --summary
```

Increase `--target-ipc-gb` on machines with more memory. Use
`--json path/to/out.json` to save the raw results.

For application code, see [Stream query results](guides/stream_query_results.md).
