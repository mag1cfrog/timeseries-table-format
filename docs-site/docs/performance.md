# Performance: streaming vs batch SQL

`Session` provides two ways to get query results:

| API | Returns | When data is ready |
|---|---|---|
| `Session.sql(query)` | `pyarrow.Table` (fully materialized) | After the full result is collected |
| `Session.sql_reader(query)` | `pyarrow.RecordBatchReader` (streaming) | First batch arrives as soon as the engine produces it |

For small to medium queries the difference is negligible. For large results — especially when you
want to process rows as they arrive or when memory is a concern — `sql_reader(...)` is
significantly better.

---

## Benchmark results

Measured on a generated dataset of **~10.5 million rows** (Linux, local SSD, single process).
Each number is a median over 3 runs after 1 warmup.

### Time to first batch

"First batch available" measures how long until your code can start working with data.

| Query | `sql_reader(...)` | `Session.sql(...)` | Improvement |
|---|---:|---:|---:|
| `SELECT * FROM prices` | **370.7 ms** | 2,312 ms | 84% earlier |
| `SELECT * FROM prices ORDER BY ts` | **2,489 ms** | 13,182 ms | 81% earlier |

With `sql_reader(...)`, the engine hands you batches incrementally as they are produced.
`Session.sql(...)` must collect the entire result before returning — for a sort query this means
waiting for the full sort to complete.

### Peak RSS (process memory)

"Process-as-you-go" pattern: processing each batch immediately as it arrives rather than holding
the full table in memory.

| Query | `sql_reader(...)` | `Session.sql(...)` + iterate | Reduction |
|---|---:|---:|---:|
| `SELECT * FROM prices` | **2.30 GiB** | 3.60 GiB | 36% lower |
| `SELECT * FROM prices ORDER BY ts` | **3.66 GiB** | 4.84 GiB | 24% lower |

Memory savings come from the fact that `sql_reader(...)` never holds a materialized copy of the
full result — batches can be processed and discarded one at a time.

### `sql_reader(...).read_all()` vs `Session.sql(...)`

If you call `reader.read_all()` (materializing everything eagerly), performance is in the same
range as `Session.sql(...)`. The streaming API does not add overhead when you ultimately collect
all rows.

---

## When to use each

**Use `Session.sql(...)`** when:

- Your result fits comfortably in memory.
- You want a `pyarrow.Table` directly (e.g. to pass to Polars, pandas, etc.).
- Simplicity matters more than latency-to-first-row.

**Use `Session.sql_reader(...)`** when:

- Your result is large and you want to process batches as they arrive (e.g. writing to disk,
  aggregating incrementally, piping to another system).
- You want lower peak memory usage.
- You care about time-to-first-result (e.g. streaming to a UI or a downstream pipeline).

---

## Usage

```python
import timeseries_table_format as ttf

sess = ttf.Session()
sess.register_tstable("prices", "./prices_table")

# Batch (eager) — returns a fully materialized pyarrow.Table
table = sess.sql("SELECT * FROM prices WHERE ts > '2024-05-01'")

# Streaming — process each batch as it arrives
reader = sess.sql_reader("SELECT * FROM prices WHERE ts > '2024-05-01'")
try:
    for batch in reader:
        # process batch (pyarrow.RecordBatch) here
        print(batch.num_rows)
finally:
    reader.close()

# Or collect everything at once via the reader (same performance as Session.sql)
reader2 = sess.sql_reader("SELECT * FROM prices WHERE ts > '2024-05-01'")
try:
    table2 = reader2.read_all()
finally:
    reader2.close()
```

---

## Reproducing the benchmark

```bash
cd python
uv pip install -p .venv/bin/python numpy
uv run -p .venv/bin/python maturin develop --features test-utils
.venv/bin/python bench/sql_conversion.py \
    --target-ipc-gb 2 \
    --warmups 1 \
    --runs 3 \
    --include-streaming \
    --summary
```

Increase `--target-ipc-gb` to `3` or `6` on machines with more memory for larger datasets.
Results are printed to stderr; use `--json path/to/out.json` to save the raw data.
