# Bulk keyed-update preparation

This comparison evaluates the internal preparation stage for large backfills. It includes source validation, exact-key matching, sorting, and complete cursor consumption. Parquet replacement writing and transaction publication are separate stages and are not measured here.

## Workload and acceptance criteria

Environment: Windows build 26200, x64, Rust 1.97.1, Arrow/Parquet 59.2.0, AMD Family 25 Model 97. Fixtures and scratch use the system temporary directory on a Samsung MZVL21T0HDLU-00BLL NVMe SSD.

Both implementations use optimized Rust release builds, an 8 MiB sort budget, four target segments, 1,024-row Parquet row groups, and streaming 256-row input batches. Each workload runs three times in a fresh process. The original five baseline workloads also receive one additional recheck after compilation ends: some initial baseline trials overlapped development builds. To avoid inflating speedup with slow baseline trials, the comparison uses the fastest available baseline run against the median of three optimized runs. Narrow values are nullable Int64; wide values are 1,024- or 4,096-byte Binary. The source includes an Int64 key. Shuffled input permutes keys without materializing an index vector. Concentrated input addresses one segment. Dense input updates every target row.

| Workload | Target rows | Update rows | Logical source bytes |
| --- | ---: | ---: | ---: |
| Narrow small | 1,048,576 | 262,144 | 4,194,304 |
| Narrow large | 4,194,304 | 1,048,576 | 16,777,216 |
| Narrow concentrated | 4,194,304 | 1,048,576 | 16,777,216 |
| Narrow dense | 1,048,576 | 1,048,576 | 16,777,216 |
| Wide | 1,048,576 | 262,144 | 270,532,608 |
| Wide 4 KiB | 1,048,576 | 262,144 | 1,075,838,976 |

The 4 KiB workload was added after the intended workload was clarified as hundreds of thousands of rows totaling a little over 1 GB. It conservatively treats that size as the update input, rather than the full table. It uses three fresh-process baseline and optimized repetitions after all compilation has finished.

Acceptance criteria chosen before measuring the optimized implementation: at least 50% less cumulative logical scratch IO and at least 1.5x throughput on each workload; a fourfold increase from narrow-small to narrow-large must keep the sort buffer within its fixed budget and peak process RSS within 1.5x. All existing correctness/cleanup checks must pass. These are comparison gates for this implementation, not production latency promises.

## Results

Times compare the fastest baseline against the optimized median. IO is cumulative logical scratch reads plus writes. GB and MB below are decimal units; RSS is the largest sampled process peak across optimized repetitions. Full samples and binary hashes are in [the JSON report](update-prepare-bulk-2026-09-07.json).

| Workload | Before seconds | After seconds | Speedup | Before IO GB | After IO GB | IO reduction | Before peak scratch GB | After peak scratch GB | After peak RSS MB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| narrow-small | 16.02 | 6.42 | 2.50x | 6.59 | 2.19 | 66.8% | 0.529 | 0.324 | 25.8 |
| narrow-large | 77.15 | 31.70 | 2.43x | 35.00 | 12.17 | 65.2% | 2.134 | 1.312 | 26.1 |
| narrow-concentrated | 77.54 | 31.63 | 2.45x | 34.96 | 12.14 | 65.3% | 2.133 | 1.310 | 26.2 |
| narrow-dense | 50.47 | 15.43 | 3.27x | 28.10 | 4.84 | 82.8% | 1.736 | 0.467 | 25.2 |
| wide | 30.07 | 7.30 | 4.12x | 16.64 | 2.74 | 83.5% | 1.100 | 0.600 | 25.6 |
| wide-4k | 76.98 | 9.19 | 8.37x | 51.93 | 4.40 | 91.5% | 2.710 | 1.430 | 26.0 |

All six workloads pass the comparison gates. Fourfold input/target growth keeps peak sort allocation within 8 MiB and changes peak process RSS from 25.8 to 26.1 MB. Peak process handles are 87 in all six workloads. The representative roughly 1 GiB input now uses about 1.43 GB peak scratch rather than 2.71 GB. Narrow large workloads still need roughly 1.31 GB scratch because discovering/sorting four million target keys remains substantial work; the optimized implementation does not make temporary space proportional only to replacement payload size.

## Implementation

The baseline is commit `64b65530a695702cd28f9780ba246573abdc4550` with measurement-only changes: scratch byte counters and larger/wider benchmark inputs. The reproducible changes are in [update-prepare-baseline-instrumentation.patch](update-prepare-baseline-instrumentation.patch).

The optimized implementation uses `arrow::row::RowConverter`, already included in the Arrow dependency, to encode values using a single snapshot schema. This removes per-row IPC schemas and message framing without creating a custom scalar/nested value codec. Tests verify Float32/Float64 bit patterns including NaN payloads and signed zero, canonical scalar types, sliced nested values, nulls, and schema rules.

Encoded values are appended once to one private file. Both sort phases carry complete keys and fixed-size offset/length references; merge passes do not copy the values. Cursor consumption performs one checked read per referenced value, decodes it, and checks that its key equals the matched key. Every value and sort record has a checksum. File completion/count and reference bounds are checked, including value-file completion at successful cursor EOF. The row decoder receives only verified bytes from the operation's own schema-specific encoder. The format is private to one operation and is never a persisted table format.

The sort buffer grows geometrically within its byte budget instead of reserving exactly one record slot per insertion. Unused reserved slots count against the budget. In the initial compact-value experiment, the narrow-small workload took about 11.9 seconds; fixing this allocation pattern reduced a subsequent release probe to about 6.2 seconds. The final comparison includes both changes.

There are at most four simultaneously open scratch files (three merge handles plus the value writer during source sorting). Consumption opens two files. Sort buffers remain byte-limited; one largest encoded value, Arrow conversion work, caller batches, snapshot/segment descriptors, Parquet metadata/pages, and fixed IO buffers are separate contributions. There is no cache or index growing with value-file size. The 64 MiB footer-input and 256 MiB projected-uncompressed-key row-group guards, plus retention-based vacuum recognition, remain in effect.

## Measurement boundaries

Scratch IO counts logical bytes of completed writes and successful record/value reads, including checksums and footer checks. It excludes OS cache behavior, device write amplification, and buffered read-ahead; it is not a physical-disk bandwidth measurement. Peak scratch includes overlapping merge inputs/output and the value file, including bytes buffered for writing. The driver measures peak working set and handles using the existing 20 ms Windows sampler. Process RSS includes fixture construction and allocator retention. The OS file cache is outside that RSS figure.

The value-reference design trades repeated sequential copies for random value reads at consumption. These local SSD runs use newly written files, which can be cached by the OS. They do not establish cold-cache or HDD performance. All target keys are still scanned and sorted; sparse updates against much larger tables may be dominated by target discovery. Neither increasing the sort budget nor release optimization changes that work.

## Reproduction

```powershell
$env:RUSTFLAGS = '-C debuginfo=0'
$env:CARGO_INCREMENTAL = '0'
cargo test -p timeseries-table-format --no-default-features --release --lib --no-run
./scripts/bench/bench_update_prepare.ps1 -TestBinary <emitted-test-executable> -Suite bulk -Repetitions 3 -Label compact-values -Output optimized.json
```

Use `-WorkloadName wide-4k` to run only the roughly 1 GiB input case.

To reproduce the baseline, use a separate checkout at the baseline commit, apply the instrumentation patch with `git apply --unidiff-zero <patch-path>`, and build with the same flags. Run that binary with this branch's PowerShell driver and `-Label baseline-ipc`. Run baseline and optimized measurement processes sequentially on the same host. Keep source shape, sort budget, profile, and machine conditions comparable. The JSON records all workload parameters and process measurements.
