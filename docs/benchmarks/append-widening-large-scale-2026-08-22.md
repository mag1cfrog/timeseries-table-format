# Lossless widening append comparison, 2026-08-22

This report compares external Arrow normalization before append with direct
lossless widening during append. It records one run on one machine and is not
a universal performance guarantee.

The complete machine-readable report is
[`append-widening-large-scale-2026-08-22.json`](append-widening-large-scale-2026-08-22.json).

## Result

Direct widening reduced median end-to-end time by 42.80% and retained Parquet
bytes by 50.00%. Median peak RSS was effectively unchanged at 0.11% lower.
Both modes produced the same 3,622,108,931-byte table segment and passed every
logical equivalence check.

| Metric | External-normalization median | Direct-widening median | Direct widening relative to external normalization |
| --- | ---: | ---: | ---: |
| End-to-end pipeline time | 5,560,596,631 ns (5.560597 s) | 3,180,439,133 ns (3.180439 s) | 42.80% lower, 1.75x as fast |
| Peak RSS | 2,099,367,936 bytes (1.955 GiB) | 2,097,098,752 bytes (1.953 GiB) | 0.11% lower |
| Table-managed committed Parquet | 3,622,108,931 bytes (3.373 GiB) | 3,622,108,931 bytes (3.373 GiB) | no difference |
| Total retained ingestion Parquet | 7,244,217,862 bytes (6.747 GiB) | 3,622,108,931 bytes (3.373 GiB) | 50.00% lower |

The external-normalization median phases were 3,098,464,272 ns for
normalization and 2,490,578,743 ns for append and commit. Direct widening took
3,180,438,953 ns for append and commit, 27.70% longer than appending the
already-normalized file. The end-to-end values are the primary comparison
because only the external pipeline performs a separate full-file
normalization pass.

Artifact byte values are file sizes. They do not measure physical device I/O.

## Workload and method

The benchmark ran from clean commit
`d038eee9cca20ea32c621c0b50df7bc0da32ab09` with:

- 3,466,797 rows in 8,192-row generated Arrow batches
- 1,024 payload bytes per row and 3,550,000,128 generated payload bytes
- seed 20,260,821
- a registered `UInt64` `ordered_index` and incoming `UInt32` values
- index boundaries 0 and 4,294,967,295
- uncompressed Parquet with 1,048,576 maximum rows per row group

Each mode ran once as an untimed warm-up. Three measured repetitions
alternated mode order. Every invocation used a fresh process and table
directory with `LC_ALL=C`; GNU `/usr/bin/time -v` supplied peak RSS.

## Validation

All warm-ups and measured samples completed successfully. Every invocation
committed version 2 with:

- 3,466,797 rows in one table-managed Parquet segment and four row groups
- the exact registered logical schema
- complete coverage over the ordered-index range
- boundary values preserved as `UInt64`
- matching checksums for every generated column
- a full table scan matching every generated row

The direct pipeline created no external normalized Parquet artifact. The raw
report records every command, sample, timing, RSS value, artifact size,
checksum, and validation result.

## Environment and limitations

- CPU: AMD Ryzen 7 8845HS, 8 cores and 16 threads
- Architecture: x86_64
- Memory: 27.954 GiB total; 22.482 GiB available when recorded
- OS: Linux, kernel 6.19.14-200.fc43.x86_64
- Rust: rustc 1.97.1 (8bab26f4f 2026-07-14)
- Temporary storage: 14 GiB memory-backed `/tmp`

This was a developer machine, not an isolated benchmark host. CPU frequency
scaling and unrelated system activity were not controlled. Completed
invocation data was removed before the next invocation, and `--keep-data` was
not used. Compare the two modes within this report; do not compare these
timings directly with runs from other environments.
