# Keyed-update replacement staging

This measures preparation followed by verified Parquet replacement staging for issue #434. It does not publish a transaction. Each affected source file is rewritten once, preserving physical row order and entity layout. Unaffected files remain untouched.

## Workloads and method

Windows build 26200, Rust 1.97.1, Arrow/Parquet 59.2.0, AMD Family 25 Model 97. Fixtures and scratch use the system temporary directory on the same local Samsung NVMe SSD as the preparation benchmark. Every case contains 262,144 target rows in four files. Each process constructs fixtures and supplies updates in streaming 256-row batches. Wide values contain 4,096 distinct pseudorandom bytes per row; they cannot collapse into one repeated dictionary value. Source files use uncompressed, dictionary-disabled 1,024-row groups. Replacements use the existing default Zstd compression, with an 8 MiB encoded row-group target and a 65,536-row limit.

Six cases cover narrow values, sparse/scattered and concentrated label changes, 25% and 100% wide-value updates, and mixed versus single-entity layouts. The dense case supplies 1,076,363,264 logical input bytes, including keys, and rewrites about 1.08 GB of source Parquet. Each wide source file is about 270 MB, substantially larger than the working buffers. Three fresh-process repetitions run sequentially after compilation finishes; times below are medians and RSS is the largest process peak. All samples and the release executable SHA-256 are in [the JSON report](update-rewrite-2026-09-07.json).

These fixtures differ from the earlier preparation-only benchmark: fewer target rows, populated source payloads, entity keys, different source encoding, and a new output-writing phase. Their timings must not be treated as a before/after comparison with that benchmark.

## Results

| Workload | Updates | Files replaced | Prepare s | Rewrite s | Total s | Source read GB | Output GB | Scratch peak GB | Peak RSS MB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| narrow-quarter | 65,536 | 4 | 0.95 | 0.47 | 1.42 | 0.012 | 0.002 | 0.081 | 23.1 |
| wide-sparse | 4,096 | 4 | 1.26 | 2.41 | 3.66 | 1.095 | 1.077 | 0.096 | 33.7 |
| wide-concentrated | 4,096 | 1 | 1.27 | 0.56 | 1.83 | 0.274 | 0.269 | 0.096 | 31.3 |
| wide-quarter | 65,536 | 4 | 2.19 | 3.06 | 5.28 | 1.095 | 1.076 | 0.387 | 34.3 |
| wide-dense | 262,144 | 4 | 5.52 | 4.65 | 10.12 | 1.095 | 1.076 | 1.264 | 33.9 |
| wide-single | 65,536 | 4 | 2.16 | 3.04 | 5.18 | 1.095 | 1.076 | 0.387 | 33.0 |

GB and MB are decimal. Preparation timing ends when the matched cursor is returned; its value reads and cleanup occur during rewrite timing. Total includes both phases through successful replacement verification, and excludes fixture construction and abandonment cleanup.

Sparse label updates demonstrate the cost of immutable whole-file replacement: 4,096 updates spread across all four files still rewrite roughly 1.08 GB. Concentrating the same count in one file reduces the replaced source and output bytes by approximately four. Increasing entity count does not add full source passes: the mixed and single-entity cases read the same source byte count.

## IO, memory, and temporary space

`source_file_bytes` is the sum of unique affected source Parquet sizes. `replacement_file_bytes` is the sum of completed replacement sizes. They exclude coverage, staging scratch, and discovery-only files. The wide scattered cases read 1,095,393,932 source bytes from 1,080,953,580 bytes of source files, including metadata and buffered read-ahead. This is one sequential source scan during rewriting; preparation separately discovers keys across all target files. Its measured reads appear as `preparation_key_bytes_read` in the JSON.

Verification reads the completed output's key columns, checks their ordered BLAKE3 digest against the source's length-framed complete typed keys, recomputes coverage, compares committed coverage and layout, and validates canonical schema, row count, and index bounds. Matched row keys are also compared directly before applying each update. The digest is an integrity check, not a key-matching index. Payload arrays are written through Arrow/Parquet and tested with logical-value comparisons; verification does not reread every output payload column. `output_key_bytes_read` includes the projected verification reader's footer and read-ahead, but excludes the separate output-metadata inspection and sidecar reads. It must not be presented as all output IO.

Read amplification in the JSON summaries is `(source_bytes_read + output_key_bytes_read) / logical_source_bytes`; write amplification is `replacement_file_bytes / logical_source_bytes`. Logical input includes complete keys and selected values, without Arrow offsets/validity buffers. These ratios exclude preparation and sidecars. Sparse narrow labels have large amplification because the denominator is only 73,728 bytes while their affected files contain about 1 GB. This first version deliberately retains whole-file replacements; batch labels together when they address the same files.

The 8 MiB writer setting is a row-group target, not a process memory cap. Wide decoded batches measure about 1.06 MB and sampled writer allocations about 8.61 MB. Memory also includes source/output batches, up to one batch's replacement slices, Arrow/Parquet pages and dictionaries, the preparation cursor, and per-segment coverage metadata. A very large individual value or coverage map can increase memory. Footer input over 64 MiB and row groups declaring over 1 GiB uncompressed data fail with typed resource errors before source decoding. No complete segment payload, global update array, or writer per entity is retained.

The scratch column measures preparation scratch only, not all new disk usage. Scratch and replacement files coexist during staging. For the dense case, adding peak scratch to completed replacement Parquet sizes gives a conservative data-file provision of about 2.34 GB beyond the retained originals, plus sidecars. It is not a sampled combined peak. Original files remain intact and will remain retained by existing history rules after a future successful publication. Explicit abandonment and errors clean owned replacements; cancellation uses cleanup guards. Vacuum handles interrupted artifacts under the existing retention cutoff.

The Windows sampler records process peak working set and handles every 20 ms, including fixture generation and allocator retention. OS file cache is outside process RSS. These are newly written files on local NVMe; cold-cache, HDD, remote storage, and concurrent production writers are not measured. The results do not establish end-to-end transaction latency.

## Reproduction

```powershell
$env:RUSTFLAGS = '-C debuginfo=0'
$env:CARGO_INCREMENTAL = '0'
cargo test -p timeseries-table-format --no-default-features --release --lib --no-run
./scripts/bench/bench_update_prepare.ps1 -TestBinary <emitted-test-executable> -Stage rewrite -Suite bulk -Repetitions 3 -Label issue-434 -Output rewrite.json
```

Use `-WorkloadName wide-dense` for the roughly 1 GiB input case or `-Suite smoke` for smaller fixtures. The benchmark validates matched counts, affected-file counts, successful verification, and owned-artifact cleanup. Behavioral tests additionally exercise historical nullable nested columns, exact unsigned/timestamp keys, unchanged values, changed source bindings, output ordering, write/finish failures, late coverage failures, malformed payload metadata, cancellation, cleanup-error preservation, explicit ownership transfer, and vacuum retention.
