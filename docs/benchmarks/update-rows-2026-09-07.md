# Transactional keyed updates: public Rust API

The completed `TimeSeriesTable::update_rows` API updated 262,144 rows with about
1.08 GB of selected values in a median **10.13 seconds**, with a maximum observed
process working set of **35.30 MB** across three runs. The matching preparation
and rewrite control took **10.03 seconds**. The observed 0.10-second difference
(about 1%) is small compared with run-to-run variation; these sequential runs do
not isolate the exact cost of transaction publication.

The public boundary adds version checks, file-ownership validation, one log commit,
and replacement of the handle's segment metadata. It does not collect row values
in memory or repeat key discovery or file verification.

## Workloads and results

Each workload starts with 262,144 stored rows in four files. Wide rows contain a
distinct deterministic 4,096-byte binary value. Shuffled updates spread across
all four files; concentrated updates address only the first file. Except for
the no-entity narrow case and the single-entity case, there are four entities.
Times are medians of three fresh-process runs. Memory is the maximum observed
process working set across those runs. MB/GB use decimal units.

| Workload | Assigned rows | Selected column | Replaced files | Median seconds | Range, seconds | Peak MB |
| --- | ---: | --- | ---: | ---: | --- | ---: |
| Narrow quarter | 65,536 | Int64 | 4 | 1.44 | 1.43–1.45 | 23.99 |
| Wide sparse | 4,096 | Int64 | 4 | 3.86 | 3.85–3.87 | 33.98 |
| Wide concentrated | 4,096 | Int64 | 1 | 1.88 | 1.87–1.96 | 29.96 |
| Wide quarter | 65,536 | Binary | 4 | 5.25 | 5.22–5.30 | 34.79 |
| Wide dense | 262,144 | Binary | 4 | 10.13 | 10.13–10.34 | 35.30 |
| Wide single entity | 65,536 | Binary | 4 | 5.25 | 5.21–5.34 | 33.86 |

The wide dense run reports 1,080,953,580 source-file bytes and 1,076,455,380
replacement-file bytes. These are file sizes, not IO counters. Even the sparse
case rewrites about 1.08 GB because its 4,096 assignments touch all four files.
Concentrating the same number of assignments in one file reduces replacement
bytes to 269,114,473 and time to 1.88 seconds. Physical file distribution remains
an important cost factor for backfill workloads.

## Preparation/rewrite control

The same release binary and existing streaming fixture ran `-Stage rewrite` for
the wide dense workload three times. It uses the same preparation and rewrite
functions but does not publish a transaction. Its total median was 10.03 seconds
(9.79–10.10), and its maximum working set was 34.70 MB. Median phase times were
5.39 seconds for preparation and 4.64 seconds for rewriting.

Existing instrumentation reported identical counters in all three controls:

| Measurement | Bytes |
| --- | ---: |
| Peak live preparation scratch | 1,264,178,220 |
| Scratch written | 1,750,291,136 |
| Scratch read | 1,797,779,156 |
| Projected target-key discovery reads | 8,011,660 |
| Source reads during rewriting | 1,095,393,932 |
| Replacement key-verification reads | 5,054,592 |

These are logical library/file reads, including the documented decoder read-ahead;
they do not measure physical disk traffic or OS cache misses. The public report
does not expose these internal counters. The public and control runs produced
identical source/replacement size totals and affected-file counts.

Scratch capacity and file rewriting remain the main costs of this delivery.
Plan disk space for preparation scratch and new replacements while old source
files remain retained. Individual maxima are not a measurement of simultaneous
total disk use. Successful updates retain source files and sidecars for old
snapshots; vacuum does not expire referenced history.

## Method and limits

- Windows 10.0.26200, x86-64, AMD family 25 model 97 stepping 2; local files.
- Rust 1.97.1, release profile, no default features, `-C debuginfo=0`.
- The public timer spans the complete API call, including source generation,
  preparation, rewriting, checks, and transaction publication. Fixture construction
  and reopening to verify the committed state are outside this timer.
- The existing Windows sampler polls every 20 ms and records `PeakWorkingSet64`.
  This is a process-wide peak including fixture setup and verification, not the
  memory allocated by the update alone. Ready-time samples also reflect allocator
  reuse; subtracting them does not produce a reliable operation memory bound.
- Compilation and other benchmark runs were not run concurrently. Public cases
  ran before controls; cache and system variation remain possible. Three runs on
  one host cannot establish a general latency or memory guarantee.
- The fixture streams 256-row batches, uses four files and small identities.
  Large caller batches, huge individual values, wide keys, many segments, and
  other Parquet layouts may have different costs. Existing decoder limits apply.

Reproduction from the repository root:

```powershell
$env:RUSTFLAGS = '-C debuginfo=0'
$env:CARGO_INCREMENTAL = '0'
cargo test --release -p timeseries-table-format --no-default-features --lib --no-run
# Use the test executable printed by Cargo.
./scripts/bench/bench_update_prepare.ps1 -TestBinary '<test executable>' `
  -Stage update -Suite bulk -Repetitions 3 -Output update-rows.json
./scripts/bench/bench_update_prepare.ps1 -TestBinary '<test executable>' `
  -Stage rewrite -Suite bulk -WorkloadName wide-dense -Repetitions 3 `
  -Output update-rows-control.json
```

The measured binary SHA-256 was
`cab355a0008aa15eb28163e9c9f82259e665a8f10573fa67a6fd24788c79792b`.
Raw measurements: [public API](update-rows-2026-09-07.json) and
[preparation/rewrite control](update-rows-stage-control-2026-09-07.json).
