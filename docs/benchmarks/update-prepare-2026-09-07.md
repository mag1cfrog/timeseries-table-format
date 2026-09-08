# Keyed update preparation: bounded staging

This native Rust measurement covers validation, exact-key matching, external sorting, and consumption of the prepared cursor. It does not rewrite Parquet payloads or publish a transaction. The raw measurements are in [update-prepare-2026-09-07.json](update-prepare-2026-09-07.json).

## Environment and workload

Windows 11 (build 26200), x64, Rust 1.97.1, Arrow/Parquet 59.2.0; CPU identification is recorded in the JSON. These are **unoptimized test-profile** executions with `RUSTFLAGS=-C debuginfo=0` and incremental compilation disabled. They establish resource behavior, not production throughput.

Each run creates four Parquet segments with 1,024-row groups through a streaming writer. The table has a signed integer index and one nullable integer payload. A streaming source produces 256-row batches (4,096 logical value bytes, plus Arrow allocation/schema overhead). Multiplication by an odd number permutes a power-of-two key domain without allocating a permutation vector. Shuffled input spans all four segments; concentrated input touches one segment. Both exercise multiple external merge passes.

The sort budget is fixed at 65,536 bytes. The larger source contains 262,144 logical value bytes, four times that budget. No all-source or all-target Arrow batch is materialized. Four resident segment descriptors serialize to approximately 1 KiB; this is a metadata-size reference, not a heap measurement.

## Results

| Target rows | Update rows | Input | Peak process bytes | Peak sort bytes | Peak scratch bytes | Initial runs | Seconds |
| ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 16,384 | 4,096 | Shuffled | 14,340,096 | 65,520 | 7,406,252 | 171 | 0.68 |
| 16,384 | 4,096 | Concentrated | 14,368,768 | 65,520 | 7,399,034 | 171 | 0.70 |
| 65,536 | 16,384 | Shuffled | 14,286,848 | 65,520 | 29,900,105 | 679 | 3.00 |
| 65,536 | 16,384 | Concentrated | 14,417,920 | 65,520 | 29,875,112 | 679 | 2.97 |

Peak process handle count was 85 in every run, versus 82 in the sampled preparation baseline. Key discovery read 397,612 and 1,664,204 filesystem bytes respectively, including metadata and read-ahead. Projected compressed key-column sizes were 152,352 and 609,408 bytes. All target keys are discovered in this version; coverage pruning is not used.

The PowerShell driver samples Windows `PeakWorkingSet64` and handle count every 20 ms. The reported baseline is sampled after the native readiness marker and may include early preparation work. Process peaks include fixture creation and allocator retention, so they must not be presented as staging-only allocations. Very short terminal spikes can escape the final sample. Sort accounting includes record-vector capacity and owned byte-vector capacities; scratch accounting includes simultaneous merge inputs and completed output before input deletion.

## Bounds and costs

The implementation holds one byte-budgeted sort buffer, a constant number of merge records and IO buffers, and at most three open scratch files. A largest serialized source row may exceed the sort budget and spills alone. Caller-owned batches, one-row Arrow alignment/IPC work, snapshot/segment descriptors, and the current Parquet footer are separate contributions. Parquet pages and dictionaries are decoder allocations: projected key columns exceeding 256 MiB of declared uncompressed data in one row group produce a typed resource error before key-page decoding. This is a physical-layout limit, not a process RSS guarantee.

Per-row Arrow IPC deliberately reuses the existing codec for exact nested values. Repeated schema/framing has substantial scratch overhead for tiny rows: the largest run uses about 30 MB of temporary space for 256 KiB of logical update values. Batching value blocks would trade additional indexing/lifecycle code for lower CPU and disk cost; these measurements make that tradeoff visible.

Scratch is exclusively owned under `data/_staged/update-prepare/<uuid>/<20-digit-id>.run`. Completion and explicit errors clean it, Drop retries best effort, and vacuum recognizes process-interrupted files under its existing retention rules. The cutoff must predate active operations. Vacuum removes regular files; empty attempt directories can remain after process interruption, consistent with the existing staged-directory policy.

## Reproduction

Build the native test executable, then pass the emitted executable path to the driver:

```powershell
$env:RUSTFLAGS = '-C debuginfo=0'
$env:CARGO_INCREMENTAL = '0'
cargo test -p timeseries-table-format --no-default-features --lib --no-run
./scripts/bench/bench_update_prepare.ps1 -TestBinary <emitted-test-executable> -Output update-prepare-benchmark.json
```

On other platforms, invoke that executable with `--exact table::operations::update_prepare::tests::preparation_memory_benchmark --ignored --nocapture` under the platform's process-memory measurement tool. `TST_UPDATE_TARGET_ROWS`, `TST_UPDATE_ROWS`, `TST_UPDATE_SORT_BYTES`, and `TST_UPDATE_MODE` select the workload. Target counts must be powers of two; updates must fit in one quarter of the target population so the concentrated case remains valid.
