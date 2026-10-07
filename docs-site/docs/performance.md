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

## Wide binary rows

Streaming does not impose a byte limit on decoded batches or parallel
execution. The wide-row benchmark scans 32,768 rows in four segments, each
with one row group and eight 2,048-byte binary columns. The payload alone is
512 MiB when decoded, while the Parquet files total 11,271,319 bytes. The
values are intentionally compressible.

The following results were measured on October 6, 2026, using the published
`timeseries-table-format==0.8.0` wheel, CPython 3.12.12, PyArrow 25.0.0, Linux
x64, and 16 logical CPUs. Each value is the median of three fresh-process
samples after one discarded warmup for that configuration. Preparation runs
in a separate process so its allocations do not inflate reader peak memory.
The filesystem cache is not cleared. DataFusion's effective defaults were
`batch_size=8192` and `target_partitions=16`.

| Settings | Batches | Largest batch | Peak RSS | First batch | Total scan | Rows/s |
|---|---:|---:|---:|---:|---:|---:|
| Defaults | 4 | 128.9 MiB | 697.8 MiB | 79.9 ms | 109.1 ms | 300,438 |
| `batch_size=1024` | 32 | 16.1 MiB | 528.2 MiB | 67.4 ms | 126.8 ms | 258,474 |
| `target_partitions=1` | 4 | 128.9 MiB | 275.3 MiB | 64.0 ms | 283.0 ms | 115,802 |
| Both settings | 32 | 16.1 MiB | 162.8 MiB | 18.4 ms | 235.1 ms | 139,391 |

All full scans returned 32,768 rows and 540,672,000 Arrow bytes, consuming and
discarding each batch without `read_all()`. Smaller batches and fewer
partitions reduced peak memory, with a throughput cost on this workload.
Batch size counts rows, so even the smaller batches held 16.1 MiB each.

Peak RSS includes the interpreter, runtime, and other process allocations.
It does not isolate a particular queue or prove a memory ceiling. These are
local measurements, not portable performance thresholds. The
[raw results](https://github.com/mag1cfrog/timeseries-table-format/blob/main/docs/benchmarks/sql-reader-wide-2026-10-06.json)
include pre-query process peaks, effective DataFusion batch size and target
partition count, and every measured sample. The default target partition
count can be lower than the machine's logical CPU count, for example under
CPU affinity limits.

### Early close

For every configuration, the benchmark also consumes and discards one batch,
closes the reader, verifies that another close succeeds and another read
fails, then runs `SELECT 1` in the same session. Each worker has a 60-second
timeout to detect hangs.

All early-close checks passed. Median `close()` time was 9.9 ms with defaults
and 0.013 ms with both settings. Their process peaks were 692.0 MiB and
151.2 MiB, respectively. Parallel scans can decode substantial data before
the first batch reaches Python, so closing early is not a guarantee of low
peak memory or immediate memory return to the operating system.

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

To run only the wide-row scan and early-close matrix from the repository
root, using the same package versions as the measurements above:

```bash
uv run --no-project --python 3.12.12 \
    --with timeseries-table-format==0.8.0 --with pyarrow==25.0.0 \
    python crates/timeseries-table-python/bench/sql_conversion.py \
    --wide-streaming --warmups 1 --runs 3 --summary \
    --json sql-reader-wide.json
```

This mode uses the public API and does not require NumPy or `test-utils`.
After building a local extension, run the same script with its Python
environment to measure that build. For a small correctness check, use
`--wide-rows-per-segment 1025 --warmups 0 --runs 1`. The JSON records time to
first batch, total scan time, rows per second, largest batch `nbytes`, close
time, process peaks, and effective DataFusion settings. The worker reads the
settings after recording scan timing and peak memory. On Windows,
`peak_rss_bytes` reports peak working set. The additional sampled
`isolated_peak_rss_bytes` is available on Linux and includes the worker's
post-scan diagnostic queries.

For application code, see [Stream query results](guides/stream_query_results.md).
