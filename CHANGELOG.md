# Changelog

All notable changes to timeseries-table-format are documented here beginning
with the unified 0.3.0 release. This is the only changelog updated for current
Rust library, CLI, and Python releases.

## 0.5.0


### Bug Fixes

- Make streaming append cleanup safer and APIs clearer ([#371](https://github.com/mag1cfrog/timeseries-table-format/pull/371)) ([48da5b5](https://github.com/mag1cfrog/timeseries-table-format/commit/48da5b59cf2c645445fd0e0c6a0ebde4f87e2b70))

- Reject duplicate index intervals within one append ([0f07ebf](https://github.com/mag1cfrog/timeseries-table-format/commit/0f07ebf4a6cbefa6bf38c3b929684b46f00748be))


### Code Refactoring

- Extract notebook display stylesheet ([9586974](https://github.com/mag1cfrog/timeseries-table-format/commit/95869745acfd41742f241dd2780cd180673f7e4e))

- Remove legacy path-first append APIs ([b967711](https://github.com/mag1cfrog/timeseries-table-format/commit/b967711cbc8741f6f4c27b3da5457c5d8f81ea4f)) - **Breaking:** remove legacy path-first append APIs

- Rename ordered index granularity and intervals ([0f8a6fd](https://github.com/mag1cfrog/timeseries-table-format/commit/0f8a6fd1ae3f858ecfa41d414b163413be0216d2)) - **Breaking:** rename ordered index granularity and intervals

- Unify index granularity across Python and CLI ([828204c](https://github.com/mag1cfrog/timeseries-table-format/commit/828204ccf51d13d4045383a344999b089252ef87)) - **Breaking:** unify index granularity across Python and CLI

- Clarify index granularity interval APIs ([b3b5b4c](https://github.com/mag1cfrog/timeseries-table-format/commit/b3b5b4c632516c200191c50812aac54b73de8c50)) - **Breaking:** clarify index granularity interval APIs

- Rebuild subsystem error foundations ([7e71170](https://github.com/mag1cfrog/timeseries-table-format/commit/7e711707b82d979461b744250ab29a9906c4208c))

- Preserve typed append error sources ([a07f7f0](https://github.com/mag1cfrog/timeseries-table-format/commit/a07f7f0256c664b4cc01fe171090cc84c7afb95b)) - **Breaking:** preserve typed append error sources

- Organize table errors by operation ([7365c59](https://github.com/mag1cfrog/timeseries-table-format/commit/7365c59c8eb8f660ac8ea21b8c5187cfa71187a9)) - **Breaking:** organize table errors by operation

- Harden public error APIs and preserve typed causes ([f9183ed](https://github.com/mag1cfrog/timeseries-table-format/commit/f9183ed9b067aefe8429a1b1919f977be8c66281)) - **Breaking:** harden public error APIs and preserve typed causes

- Organize metadata modules ([#405](https://github.com/mag1cfrog/timeseries-table-format/pull/405)) ([ed63cbf](https://github.com/mag1cfrog/timeseries-table-format/commit/ed63cbfde38b05dc9245ffabfa757a522ac0e050))

- Preserve typed error sources across table operations ([2633392](https://github.com/mag1cfrog/timeseries-table-format/commit/263339230a71580498678003a988a56972938337)) - **Breaking:** preserve typed error sources across table operations


### Documentation

- Make the documentation site canonical for Python package guidance ([01f2747](https://github.com/mag1cfrog/timeseries-table-format/commit/01f27470b711d7464312714d7b3998d580ac4e71))


### Features

- Expose native Rust diagnostics through standard Python logging ([9e8a147](https://github.com/mag1cfrog/timeseries-table-format/commit/9e8a14733903c0514f5c9057ceab0e6cc7629683))

- Add Python Arrow stream append bindings ([#366](https://github.com/mag1cfrog/timeseries-table-format/pull/366)) ([791f7ef](https://github.com/mag1cfrog/timeseries-table-format/commit/791f7efab0a27dddd1de71d13256843b6a0e3aa7))

- Upgrade DataFusion to 55 and Arrow/Parquet to 59.2, align public APIs, and document migration changes ([8c36850](https://github.com/mag1cfrog/timeseries-table-format/commit/8c36850cf32365362487ae19dc9f16c518815ffb)) - **Breaking:** Public Arrow and DataFusion API types now use the upgraded dependency versions. Empty Parquet directories are rejected at registration, and the Rust MSRV is now 1.94.0.

- Add structured diagnostics for table operations, recovery, transaction commits, and scan planning ([ec264d1](https://github.com/mag1cfrog/timeseries-table-format/commit/ec264d10b6e25470280b787a6f0e290f0abde104))

- Surface configurable structured diagnostics from the tstable CLI ([17c8d7f](https://github.com/mag1cfrog/timeseries-table-format/commit/17c8d7f24aa3326f386374715ea41afaae6da218))

- Add core Rust streaming append transaction ([#364](https://github.com/mag1cfrog/timeseries-table-format/pull/364)) ([0ab1812](https://github.com/mag1cfrog/timeseries-table-format/commit/0ab18121b2dd796f77a0fb75a4f68b8a5b46f141))

- Route CLI Parquet imports through streaming append ([#365](https://github.com/mag1cfrog/timeseries-table-format/pull/365)) ([4902723](https://github.com/mag1cfrog/timeseries-table-format/commit/4902723c02c7fd611b78ddd3d73be4d6ede58851))

- Add per-append Parquet row-group sizing ([5d47600](https://github.com/mag1cfrog/timeseries-table-format/commit/5d47600d85c3a5441af757f1b1f9c0d8efca702a))

- Support lossless scalar widening during append ([de01516](https://github.com/mag1cfrog/timeseries-table-format/commit/de015166a1239b0e7e5e430d4aa08da0faf9a4c1))

- Preserve typed read operation errors ([#396](https://github.com/mag1cfrog/timeseries-table-format/pull/396)) ([be27912](https://github.com/mag1cfrog/timeseries-table-format/commit/be2791240b4800793665459428fbf4d3eec53e0d))

- Establish protocol v7 metadata foundation ([#402](https://github.com/mag1cfrog/timeseries-table-format/pull/402)) ([edd8fbd](https://github.com/mag1cfrog/timeseries-table-format/commit/edd8fbddef4b51e3399da773746cea24f0489ce1))

- Enforce protocol compatibility for table reads and writes ([1199c7a](https://github.com/mag1cfrog/timeseries-table-format/commit/1199c7a9b99d1a0e542585686c782551c9441d36))

- Add strict protocol v7 migration tool ([#407](https://github.com/mag1cfrog/timeseries-table-format/pull/407)) ([94d66e4](https://github.com/mag1cfrog/timeseries-table-format/commit/94d66e49ed458dee09e360d30ab77cdc211e94d3))


### Testing

- Cover segment recovery error chains ([#397](https://github.com/mag1cfrog/timeseries-table-format/pull/397)) ([f853722](https://github.com/mag1cfrog/timeseries-table-format/commit/f853722929845503cece3ea79d49db66210bf044))


### Perf

- Benchmark streaming append against path-first pipeline ([#367](https://github.com/mag1cfrog/timeseries-table-format/pull/367)) ([61cbe2e](https://github.com/mag1cfrog/timeseries-table-format/commit/61cbe2e320485ca68c688bd736621b622fafc0c5))

- Benchmark direct lossless widening during append ([752ed44](https://github.com/mag1cfrog/timeseries-table-format/commit/752ed444b40d77cc92be4b5cb6c2e3be405f905d))


## 0.4.0


### Bug Fixes

- Upgrade PyO3 to 0.29 ([#301](https://github.com/mag1cfrog/timeseries-table-format/pull/301)) ([888fd9a](https://github.com/mag1cfrog/timeseries-table-format/commit/888fd9a1ee7d881a892f75b6ea36b52b203bc772))

- Prevent failed concurrent appends from leaking or deleting table artifacts ([#291](https://github.com/mag1cfrog/timeseries-table-format/pull/291)) ([e36915e](https://github.com/mag1cfrog/timeseries-table-format/commit/e36915ec0e85f1a0c9dcaafe26e977ae9e23e7b2))

- Remove orphaned Parquet copies after failed appends ([#293](https://github.com/mag1cfrog/timeseries-table-format/pull/293)) ([fdd6d73](https://github.com/mag1cfrog/timeseries-table-format/commit/fdd6d731e9304db32edc2629e8b91ef28ba05b37))

- Upgrade CLI dependencies ([#304](https://github.com/mag1cfrog/timeseries-table-format/pull/304)) ([740dca6](https://github.com/mag1cfrog/timeseries-table-format/commit/740dca6e31c5caad274afd440a458e5904b7b2ab))

- Upgrade SNAFU to 0.9 ([#305](https://github.com/mag1cfrog/timeseries-table-format/pull/305)) ([d5f129d](https://github.com/mag1cfrog/timeseries-table-format/commit/d5f129d1249e30afa2e61c4b26b7087d9a1a5fe3))

- Prevent ordered index columns from being configured as entity identities ([6a0ef72](https://github.com/mag1cfrog/timeseries-table-format/commit/6a0ef722baaf90aa648bdf0d42bf38175d5649dd))

- Render logical overlap bucket ranges ([#342](https://github.com/mag1cfrog/timeseries-table-format/pull/342)) ([0ad9e58](https://github.com/mag1cfrog/timeseries-table-format/commit/0ad9e58ffb05d7a2359adc9c5a729bb3280859e4))


### Code Refactoring

- Remove the obsolete timestamp pruning engine after the DataFusion migration ([#308](https://github.com/mag1cfrog/timeseries-table-format/pull/308)) ([aacea8e](https://github.com/mag1cfrog/timeseries-table-format/commit/aacea8eb6581afa597e8fb89a0ae06c60e13975e))


### Documentation

- Overhaul project documentation ([#317](https://github.com/mag1cfrog/timeseries-table-format/pull/317)) ([bd971a4](https://github.com/mag1cfrog/timeseries-table-format/commit/bd971a4b4149a68d9ad88b8683ce4433ab2c1f7c))

- Clarify unordered row behavior and entity-aware coverage ([4eb3dac](https://github.com/mag1cfrog/timeseries-table-format/commit/4eb3dac815958897aa7e72bc8ac41c1d939b931e))


### Features

- Expose multi-entity tables and structured overlap diagnostics in Python ([902507c](https://github.com/mag1cfrog/timeseries-table-format/commit/902507cd9fdc85fb1cad7b0fee8cc1916e9f5beb))

- Inspect Parquet files without full payload buffering ([#282](https://github.com/mag1cfrog/timeseries-table-format/pull/282)) ([7056997](https://github.com/mag1cfrog/timeseries-table-format/commit/7056997f00d6d81b8b2e13bc12263a6396a7a58a))

- Finalize bounded Parquet append profiling and RSS proof ([#284](https://github.com/mag1cfrog/timeseries-table-format/pull/284)) ([823edc3](https://github.com/mag1cfrog/timeseries-table-format/commit/823edc3efefbaf24ee0f5325795335f79cd3e36b))

- Stream time-series range scans incrementally with bounded memory ([#290](https://github.com/mag1cfrog/timeseries-table-format/pull/290)) ([df76bbb](https://github.com/mag1cfrog/timeseries-table-format/commit/df76bbbf9ce15bd07b8c45090c42c67d854d2c73))

- Introduce ordered-index metadata and 64-bit coverage ([#295](https://github.com/mag1cfrog/timeseries-table-format/pull/295)) ([1d9bb52](https://github.com/mag1cfrog/timeseries-table-format/commit/1d9bb52f8873d4c58b64b1d2e1d456ca1674ae4a))

- Support signed and unsigned integer indexes for Parquet appends ([#296](https://github.com/mag1cfrog/timeseries-table-format/pull/296)) ([8002ec0](https://github.com/mag1cfrog/timeseries-table-format/commit/8002ec0e0023eebd8389d4a435e25f6df00f8065))

- Support signed and unsigned integer range scans and coverage queries ([#297](https://github.com/mag1cfrog/timeseries-table-format/pull/297)) ([467b2ed](https://github.com/mag1cfrog/timeseries-table-format/commit/467b2ede1ea56ddcba37421da7ed4df1111ec569))

- Speed up signed and unsigned integer index queries by skipping irrelevant segment files ([5eecce1](https://github.com/mag1cfrog/timeseries-table-format/commit/5eecce1a51a0bf21880e8c15cb5fe513d40aa8ca))

- Improve timestamp query pruning with shared metadata predicates ([#307](https://github.com/mag1cfrog/timeseries-table-format/pull/307)) ([4cbffec](https://github.com/mag1cfrog/timeseries-table-format/commit/4cbffec82e45c8e2bd1c7fa5b791e27c48aa1878))

- Create and query Timestamp, Int64, and UInt64 ordered-index tables from CLI and Python ([57a6f20](https://github.com/mag1cfrog/timeseries-table-format/commit/57a6f2050989e28d58fc374a926e5c105a44b28d))

- Add per-entity coverage model with deterministic sidecar encoding ([8d17c54](https://github.com/mag1cfrog/timeseries-table-format/commit/8d17c54521813e29635f3f5506dcd52c621537ea))

- Compute exact entity-scoped coverage for multi-entity Parquet segments ([#320](https://github.com/mag1cfrog/timeseries-table-format/pull/320)) ([e4a1e8a](https://github.com/mag1cfrog/timeseries-table-format/commit/e4a1e8ae670c299dfedbf3d30fe22f38ffde3081))

- Support entity-scoped append, overlap detection, and recovery ([3ee339c](https://github.com/mag1cfrog/timeseries-table-format/commit/3ee339c08d1a47929c1b261deaafd399a543f1f7))

- Query coverage and gaps safely for individual entities ([4227763](https://github.com/mag1cfrog/timeseries-table-format/commit/4227763922e7d4a08136674269eff0e0b8e006d9))

- Persist exact segment entity layouts for reliable optimization planning ([d6625c5](https://github.com/mag1cfrog/timeseries-table-format/commit/d6625c5327e5701ae378f8f098795d749a772b6c))

- Prune conflicting single-entity segments from DataFusion scans using log metadata ([da04bb9](https://github.com/mag1cfrog/timeseries-table-format/commit/da04bb9cc6260a13fdd303a94ecf46803690f5b1))

- Add explicit entity layout optimization to the CLI and Python API ([5a12258](https://github.com/mag1cfrog/timeseries-table-format/commit/5a122581bd1809b56c62ff49a47bc638770b4f0b))

- Support string and integer entity columns across the full table lifecycle ([6ff0e80](https://github.com/mag1cfrog/timeseries-table-format/commit/6ff0e809e1c7b12477953202db981bbb3047d140))


### Testing

- Characterize timestamp segment pruning across SQL transforms and DST ([#306](https://github.com/mag1cfrog/timeseries-table-format/pull/306)) ([cc8e9aa](https://github.com/mag1cfrog/timeseries-table-format/commit/cc8e9aa6cf1c9c8f3cdd29a6dd701973fd0150f9))


### Perf

- Add bounded-memory and first-batch regression coverage for range scans ([#292](https://github.com/mag1cfrog/timeseries-table-format/pull/292)) ([63e7676](https://github.com/mag1cfrog/timeseries-table-format/commit/63e76764629f1af956a638e0b432e375967872b7))


## Legacy package history

Before 0.3.0, the repository released several packages independently. Their
historical changelogs remain available at the last tag in each release stream:

- [timeseries-table-format through 0.1.4](https://github.com/mag1cfrog/timeseries-table-format/blob/timeseries-table-format-v0.1.4/crates/timeseries-table-format/CHANGELOG.md)
- [timeseries-table-core through 0.2.2](https://github.com/mag1cfrog/timeseries-table-format/blob/timeseries-table-core-v0.2.2/crates/timeseries-table-core/CHANGELOG.md)
- [timeseries-table-datafusion through 0.1.2](https://github.com/mag1cfrog/timeseries-table-format/blob/timeseries-table-datafusion-v0.1.2/crates/timeseries-table-datafusion/CHANGELOG.md)
- [timeseries-table-cli through 0.1.2](https://github.com/mag1cfrog/timeseries-table-format/blob/timeseries-table-cli-v0.1.2/crates/timeseries-table-cli/CHANGELOG.md)
- [timeseries-table-python through 0.1.4](https://github.com/mag1cfrog/timeseries-table-format/blob/timeseries-table-python-v0.1.4/crates/timeseries-table-python/CHANGELOG.md)
