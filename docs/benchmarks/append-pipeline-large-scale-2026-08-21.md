# Large-scale append pipeline comparison, 2026-08-21

This report compares the complete path-first and streaming append pipelines on
one machine and one repository commit. It is historical benchmark evidence,
not a universal performance guarantee.

The machine-readable report is
[`append-pipeline-large-scale-2026-08-21.json`](append-pipeline-large-scale-2026-08-21.json).

## Result

For this run, streaming reduced median end-to-end pipeline time by 58.00% and
total retained ingestion bytes by 50.00%. Median peak RSS was 1.71% lower. Both
modes produced the same 3,622,108,947-byte table segment and passed every
logical equivalence check.

| Metric | Path-first median | Streaming median | Streaming relative to path-first |
| --- | ---: | ---: | ---: |
| End-to-end pipeline time | 7,164,048,887 ns (7.164049 s) | 3,008,608,601 ns (3.008609 s) | 58.00% lower, 2.38x as fast |
| Peak RSS | 2,078,629,888 bytes (1.936 GiB) | 2,043,043,840 bytes (1.903 GiB) | 1.71% lower |
| Table-owned segment | 3,622,108,947 bytes (3.373 GiB) | 3,622,108,947 bytes (3.373 GiB) | no difference |
| Total retained ingestion | 7,244,217,894 bytes (6.747 GiB) | 3,622,108,947 bytes (3.373 GiB) | 50.00% lower |

The path-first median phases were 3,003,721,994 ns for external Parquet
generation and 4,124,716,145 ns for append/copy/commit. Its median external
source size was 3,622,108,947 bytes. The streaming append median was
3,008,608,450 ns from lazy source consumption through the returned committed
version.

The end-to-end values are the primary pipeline comparison. The individual
append phase values do not represent identical work: path-first append starts
from an already encoded file, while streaming append consumes and encodes the
lazy Arrow source.

Artifact byte values are file sizes. They are not kernel block-I/O counters or
measurements of physical device reads and writes.

## Workload and method

The benchmark ran from clean commit
`6efd03e859065db1e26766283a01840150618e82` with this command:

```bash
python3 scripts/append_pipeline_benchmark.py \
  --workload large-scale \
  --samples 3 \
  --json-out /tmp/tstable-append-pipeline-large-6efd03e.json
```

The runner built the release benchmark with:

```bash
cargo build --locked --release \
  -p timeseries-table-format \
  --features cli \
  --example append_pipeline_bench
```

The workload used:

- 3,466,797 rows
- 8,192 rows per generated Arrow batch
- 1,024 payload bytes per row
- 3,550,000,128 generated payload bytes
- seed 20,260,821
- an Int64 `ts` index with bucket width 1 and no entity columns
- uncompressed Parquet, dictionary encoding, 1,048,576 maximum rows per row
  group, a 1,048,576-byte data-page limit, 1,024 write-batch rows, page-level
  statistics, and Parquet writer version 1.0

Each mode ran once as an untimed warm-up. The three measured repetitions used
path-first/streaming, streaming/path-first, and path-first/streaming order. All
invocations were sequential, used fresh directories and processes, and ran
with `LC_ALL=C`. GNU `/usr/bin/time -v` supplied peak RSS. The Rust driver used
a monotonic clock for pipeline durations.

## Validation

All warm-ups and measured samples completed successfully. Each mode committed
version 2 with:

- 3,466,797 rows and four Parquet row groups
- the generated logical Arrow schema
- complete coverage over index range 0 through 3,466,796
- an ordered full scan matching every generated row
- identical BLAKE3 checksums for `ts`, `sequence`, and `payload`

The runner compared every path-first/streaming pair and reported
`all_mode_pairs_equivalent: true`. The raw JSON contains every invocation,
exact command, timing, RSS value, artifact size, checksum, and validation
result.

## Environment and limitations

- CPU: AMD Ryzen 7 8845HS, 8 cores and 16 threads
- Architecture: x86_64
- Memory: 27.954 GiB total; 22.879 GiB available when the report was assembled
- OS: Fedora Linux with glibc 2.42
- Kernel: 6.19.14-200.fc43.x86_64
- Rust: rustc 1.97.1 (8bab26f4f 2026-07-14)
- Temporary storage: 14 GiB memory-backed `/tmp`

This was a developer machine rather than an isolated benchmark host. CPU boost
and frequency scaling were enabled, and unrelated system activity was not
controlled. The 14 GiB temporary-storage limit required completed invocation
data to be removed before the next invocation. `--keep-data` was not used, so
the raw report contains historical paths but no retained table or source
artifacts. These conditions limit comparisons with runs from other machines;
they do not affect the within-run logical equivalence validation.
