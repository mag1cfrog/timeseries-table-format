# Keyed update preparation: bounded staging

Historical measurement of the per-row IPC implementation at `c27f620`. The bulk-backfill implementation and current performance comparison are documented in [Bulk keyed-update preparation](update-prepare-bulk-2026-09-07.md); the numbers below are retained as baseline evidence.

This native Rust measurement covers validation, exact-key matching, external sorting, and consumption of the prepared cursor. It does not rewrite Parquet payloads or publish a transaction. The raw measurements are in [update-prepare-2026-09-07.json](update-prepare-2026-09-07.json).

## Environment and workload

Windows 11 (build 26200), x64, Rust 1.97.1, Arrow/Parquet 59.2.0; CPU identification is recorded in the JSON. These are **unoptimized test-profile** executions with `RUSTFLAGS=-C debuginfo=0` and incremental compilation disabled. They establish resource behavior, not production throughput.

Each run creates four Parquet segments with 1,024-row groups through a streaming writer. The table has a signed integer index and one nullable integer payload. A streaming source produces 256-row batches (4,096 logical value bytes, plus Arrow allocation/schema overhead). Multiplication by an odd number permutes a power-of-two key domain without allocating a permutation vector. Shuffled input spans all four segments; concentrated input touches one segment. Both exercise multiple external merge passes.

The sort budget is fixed at 65,536 bytes. The larger source contains 262,144 logical value bytes, four times that budget. No all-source or all-target Arrow batch is materialized. Four resident segment descriptors serialize to approximately 1 KiB; this is a metadata-size reference, not a heap measurement.

## Results

| Target rows | Update rows | Input | Peak process bytes | Peak sort bytes | Peak scratch bytes | Initial runs | Seconds |
| ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 16,384 | 4,096 | Shuffled | 14,270,464 | 65,520 | 8,191,436 | 171 | 1.04 |
| 16,384 | 4,096 | Concentrated | 14,295,040 | 65,520 | 8,184,218 | 171 | 1.03 |
| 65,536 | 16,384 | Shuffled | 14,286,848 | 65,520 | 33,048,857 | 679 | 4.56 |
| 65,536 | 16,384 | Concentrated | 14,831,616 | 65,520 | 33,023,864 | 679 | 4.65 |

Peak process handle count was 85 in every run, versus 82–83 in the sampled preparation baseline. Key discovery read 397,644 and 1,664,236 filesystem bytes respectively, including metadata and read-ahead. Projected compressed key-column sizes were 152,352 and 609,408 bytes. All target keys are discovered in this version; coverage pruning is not used. These measurements include per-record BLAKE3 checksums and run-completion footers added during correctness review.

The PowerShell driver samples Windows `PeakWorkingSet64` and handle count every 20 ms. The reported baseline is sampled after the native readiness marker and may include early preparation work. Process peaks include fixture creation and allocator retention, so they must not be presented as staging-only allocations. Very short terminal spikes can escape the final sample. Sort accounting includes record-vector capacity and owned byte-vector capacities; scratch accounting includes simultaneous merge inputs and completed output before input deletion.

## Bounds and costs

The implementation holds one byte-budgeted sort buffer, a constant number of merge records and IO buffers, and at most three open scratch files. A largest serialized source row may exceed the sort budget and spills alone. Caller-owned batches, one-row Arrow alignment/IPC work, snapshot/segment descriptors, and the current Parquet footer are separate contributions. Footer input exceeding 64 MiB is rejected before metadata parsing. Parquet pages and dictionaries are decoder allocations: projected key columns exceeding 256 MiB of declared uncompressed data in one row group produce a typed resource error before key-page decoding. These are physical-layout limits, not process RSS guarantees.

Per-row Arrow IPC deliberately reuses the existing codec for exact nested values. Repeated schema/framing has substantial scratch overhead for tiny rows: the largest run uses about 33 MB of temporary space for 256 KiB of logical update values. Batching value blocks would trade additional indexing/lifecycle code for lower CPU and disk cost; these measurements make that tradeoff visible.

Scratch is exclusively owned under `data/_staged/update-prepare/<uuid>/<20-digit-id>.run`. Each record has a checksum, and each completed run ends with a marker and record count: truncation between records is an error. Consumers must drain the prepared cursor through successful EOF before publication; explicit close abandons unread updates. Completion and explicit errors clean scratch, successful cleanup disarms directory ownership, Drop retries failed cleanup best effort, and vacuum recognizes process-interrupted files under its existing retention rules. The cutoff must predate active operations. Vacuum removes regular files; empty attempt directories can remain after process interruption, consistent with the existing staged-directory policy.

## Reproduction

Build the native test executable, then pass the emitted executable path to the driver:

```powershell
$env:RUSTFLAGS = '-C debuginfo=0'
$env:CARGO_INCREMENTAL = '0'
cargo test -p timeseries-table-format --no-default-features --lib --no-run
./scripts/bench/bench_update_prepare.ps1 -TestBinary <emitted-test-executable> -Output update-prepare-benchmark.json
```

On other platforms, invoke that executable with `--exact table::operations::update_prepare::tests::preparation_memory_benchmark --ignored --nocapture` under the platform's process-memory measurement tool. `TST_UPDATE_TARGET_ROWS`, `TST_UPDATE_ROWS`, `TST_UPDATE_SORT_BYTES`, and `TST_UPDATE_MODE` select the workload. Target counts must be powers of two; updates must fit in one quarter of the target population so the concentrated case remains valid.
