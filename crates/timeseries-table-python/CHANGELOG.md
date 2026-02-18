# Changelog
All notable changes to timeseries-table-python will be documented in this file.
## 0.1.3


### Bug Fixes

- Correct formatting and improve readability in benchmark helper function ([9ba3331](https://github.com/mag1cfrog/timeseries-table-format/commit/9ba33318c8b68d1120213fa73b7c7143831b631f))


### Features

- Add ipc-zstd feature to enable zstd compression for arrow-ipc ([b47f9cb](https://github.com/mag1cfrog/timeseries-table-format/commit/b47f9cbb87f1101070e74340da58335ede517f3f))

- Add benchmark helper for SQL queries with IPC stream metrics ([e971812](https://github.com/mag1cfrog/timeseries-table-format/commit/e97181254bf30be8f65b0f3d5c5c29f75b706fdc))


## 0.1.2


### Bug Fixes

- Set publish flag to false in Cargo.toml ([fae263e](https://github.com/mag1cfrog/timeseries-table-format/commit/fae263e58c4b79d18cb64f4730c89970206154b4))


### Features

- Enhance documentation for Session and TimeSeriesTable structs ([314cc55](https://github.com/mag1cfrog/timeseries-table-format/commit/314cc558d9646ee7f629a7545c7257c1381cc907))

- Auto-register a Jupyter/IPython HTML formatter for `pyarrow.Table` results (bounded preview, safe escaping) ([5f4ee65](https://github.com/mag1cfrog/timeseries-table-format/commit/5f4ee6538a55ed1126c148db756574f8eb203ab3))

- Improve notebook table rendering with head/tail previews for large results ([694d5ca](https://github.com/mag1cfrog/timeseries-table-format/commit/694d5ca8d3711bb6ed2933ce6557fd33a96dcbd8))

- Add notebook display configuration via env vars and TOML config files ([3566607](https://github.com/mag1cfrog/timeseries-table-format/commit/3566607be8ae1cf48a5c089a77a31ab8373aeb36))


## 0.1.1


### Bug Fixes

- Update error messages in AppendParquetError for clarity ([7861f34](https://github.com/mag1cfrog/timeseries-table-format/commit/7861f3410b5250f5b0c0a31b3b10453439c6d92f))


## 0.1.0


### Bug Fixes

- Update library name from "_dev" to "_native" in Cargo.toml ([0a23135](https://github.com/mag1cfrog/timeseries-table-format/commit/0a23135621100f982f59eb94c8a158c001f01a99))

- Update exception creation from "_dev" to "_native" in exceptions.rs ([0060761](https://github.com/mag1cfrog/timeseries-table-format/commit/006076129797fb33cc80cc8fa11ca860868b1177))

- Rename module from "_dev" to "_native" for consistency in Python bindings ([070b6a0](https://github.com/mag1cfrog/timeseries-table-format/commit/070b6a0dee5f36e62f98b4e438b97efaf9941109))

- Improve error handling in parquet registration and session catalog semaphore acquisition ([21abdd2](https://github.com/mag1cfrog/timeseries-table-format/commit/21abdd23b74d46bb66af0e6e5c419fb35d17251d))

- Improve error handling in table registration by restructuring conditional logic ([4b5a7a9](https://github.com/mag1cfrog/timeseries-table-format/commit/4b5a7a96d2820a16a892ce7269abf2f0f7184f67))

- Update error message for query parameter validation in Session class ([4537908](https://github.com/mag1cfrog/timeseries-table-format/commit/4537908d08122c2632c40907553c5cdc4c2b93f1))

- Correct typo in tables documentation and enhance deregister error handling ([7d66a07](https://github.com/mag1cfrog/timeseries-table-format/commit/7d66a07d2a19f9d8551d13f15f954d372af6cd9b))

- Improve error handling for session tables lock in deregistration ([f91ae3b](https://github.com/mag1cfrog/timeseries-table-format/commit/f91ae3b42ef6f3fbca142e4de556f911f8d10c21))

- Specify versions for timeseries-table-core and timeseries-table-datafusion dependencies in Cargo.toml ([2dc1acf](https://github.com/mag1cfrog/timeseries-table-format/commit/2dc1acf63ab333750da79b30017b6a05f7103a6d))


### Code Refactoring

- Simplify error handling in table registration by using pattern matching ([35b0d98](https://github.com/mag1cfrog/timeseries-table-format/commit/35b0d98767a0165fe343290b5c4f2a1bc13eacd6))


### Documentation

- Update SQL method documentation to include supported Python value types ([763a019](https://github.com/mag1cfrog/timeseries-table-format/commit/763a019ab4167d299088f35a30db30407d7a465f))


### Features

- Add timeseries-table-python crate and update workspace members ([22804d0](https://github.com/mag1cfrog/timeseries-table-format/commit/22804d01f88dc2fafc365eabce6b458b57121be6))

- Update Cargo.toml for timeseries-table-python crate with pyo3 dependency and lib configuration ([e63ef4d](https://github.com/mag1cfrog/timeseries-table-format/commit/e63ef4d7e5210956a5162bc43976b8dde4929e66))

- Implement initial Python bindings for timeseries-table-format ([573a11c](https://github.com/mag1cfrog/timeseries-table-format/commit/573a11c753b9033bbb587c9f027075c482c9408b))

- Add tokio dependency with multi-threaded runtime support ([8d601f7](https://github.com/mag1cfrog/timeseries-table-format/commit/8d601f75d919e8a0dfbd5596abc9a5eb9b2fbfc2))

- Remove unnecessary features from pyo3 dependency in Cargo.toml ([dbf283d](https://github.com/mag1cfrog/timeseries-table-format/commit/dbf283d74cda3d149c0295525502c7180815cd99))

- Add tokio_runner module for asynchronous support ([36a26ab](https://github.com/mag1cfrog/timeseries-table-format/commit/36a26abeeeb6b6a391283a29b933f97766396787))

- Add tokio_runner module for async Rust integration with Python ([c608f2a](https://github.com/mag1cfrog/timeseries-table-format/commit/c608f2a6c7f1aa784f9413884e1f3c860e5f44d0))

- Add dependencies for timeseries-table-core and datafusion ([af83c38](https://github.com/mag1cfrog/timeseries-table-format/commit/af83c387621dfdceaf4e68e9937e1e0c85787ab2))

- Add custom exceptions for timeseries-table-python ([20c6b83](https://github.com/mag1cfrog/timeseries-table-format/commit/20c6b83a54b8413a3e9f613456a8c2240a6ef5c3))

- Implement error mapping for DataFusion and Storage errors in Python bindings ([b04ab68](https://github.com/mag1cfrog/timeseries-table-format/commit/b04ab688bfe5d93089a7c97491793a8306b947ca))

- Add error mapping in run_blocking_map_err for better error handling ([369bfa3](https://github.com/mag1cfrog/timeseries-table-format/commit/369bfa3890559006ab997ee488e14678a40288b4))

- Enhance Python bindings with new error handling and test function for overlap ([6bfc8f0](https://github.com/mag1cfrog/timeseries-table-format/commit/6bfc8f0e78c6c467f3c787a65be3de032c3eae08))

- Add dead code allowance for datafusion error conversion function ([da06eb1](https://github.com/mag1cfrog/timeseries-table-format/commit/da06eb18f6cc18a6eccff64189a74f921e061e8e))

- Update documentation for Python exception types in exceptions.rs ([cbcb954](https://github.com/mag1cfrog/timeseries-table-format/commit/cbcb95453e22faaa69f7e97ede8b9d6abd8a518d))

- Add missing #[allow(dead_code)] annotations for error conversion functions ([2726a36](https://github.com/mag1cfrog/timeseries-table-format/commit/2726a36c6e23a1f2de7cbd1824ac00581c3df284))

- Add features section to Cargo.toml for default and test-utils ([4d5ce0d](https://github.com/mag1cfrog/timeseries-table-format/commit/4d5ce0db72f03c441ee115dcf67e20a8f6aa659d))

- Refactor internal structure and add private testing module for overlap trigger ([3198980](https://github.com/mag1cfrog/timeseries-table-format/commit/3198980c540dd7496857efdbd4106d9810030add))

- Simplify error message handling in commit_error_to_py function ([7f3ecc2](https://github.com/mag1cfrog/timeseries-table-format/commit/7f3ecc2b9c169e353dbcadb15ad5b69201bbbb07))

- Enhance TimeSeriesTable creation with error handling and additional parameters ([ababce1](https://github.com/mag1cfrog/timeseries-table-format/commit/ababce10a24afcaf11c4db5db1db066b1ec9bf29))

- Export Session and TimeSeriesTable classes in Python module initialization ([30a5089](https://github.com/mag1cfrog/timeseries-table-format/commit/30a50892feaf737a728070250df0eec26ad797d1))

- Implement open method for TimeSeriesTable with error handling ([33142fa](https://github.com/mag1cfrog/timeseries-table-format/commit/33142faecd8dcb13d3eb151062fe76b89f43163a))

- Mark TimeSeriesTable struct as unused to suppress warnings ([b4d8552](https://github.com/mag1cfrog/timeseries-table-format/commit/b4d85523b0359ee2c87493637a1e2b6ea3fd0db1))

- Improve error handling for invalid bucket specification in TimeSeriesTable ([68b74d6](https://github.com/mag1cfrog/timeseries-table-format/commit/68b74d6465e70dd62aa329976249de40b88d1501))

- Conditionally expose _test_trigger_overlap function in testing module ([e66a075](https://github.com/mag1cfrog/timeseries-table-format/commit/e66a075e23ede94dc507e6d751f393f42a601982))

- Rename module from timeseries_table_format to _dev for internal consistency ([40c3429](https://github.com/mag1cfrog/timeseries-table-format/commit/40c3429f1292d73dadd27d5f78700f9b4a7c47ae))

- Add append_parquet method for appending parquet files with error handling ([1036295](https://github.com/mag1cfrog/timeseries-table-format/commit/1036295a49437389197a742e77e80d3d918517ae))

- Add test utility functions and improve error handling in _dev module ([468e266](https://github.com/mag1cfrog/timeseries-table-format/commit/468e266972343062c129f5ef06c922801121f514))

- Refactor error handling and improve formatting in test utilities ([38b0648](https://github.com/mag1cfrog/timeseries-table-format/commit/38b06483b642c1dd7153d84394f1d861d52473bc))

- Add methods to retrieve table root, version, and index specification in TimeSeriesTable ([017114a](https://github.com/mag1cfrog/timeseries-table-format/commit/017114adde85f8968d5dfed70d80742966524cb0))

- Enhance Session struct with runtime and context initialization ([453d4b7](https://github.com/mag1cfrog/timeseries-table-format/commit/453d4b76cd140e7f6b531cb2ac669c5dc3023abd))

- Add lazy once initialized global runner and tests ([2b4ca66](https://github.com/mag1cfrog/timeseries-table-format/commit/2b4ca66c5a4c0eac72c6fc92923842ba8ce51966))

- Add global_runtime() helper function to initialize or fetch an existing global runner ([a12d18b](https://github.com/mag1cfrog/timeseries-table-format/commit/a12d18b9cab63610c1707ca880081d0984250c24))

- Add timeseries-table-datafusion dependency to Cargo.toml ([8ea89ed](https://github.com/mag1cfrog/timeseries-table-format/commit/8ea89ed6f8ed7ac6bac27bae55fa77a7a7278b7d))

- Implement register_tstable method for session management ([b4c93c3](https://github.com/mag1cfrog/timeseries-table-format/commit/b4c93c37aac009c837e6f3fc9afd0d5a067761bb))

- Update Session struct to use Mutex for thread-safe table management ([8528ac3](https://github.com/mag1cfrog/timeseries-table-format/commit/8528ac37606a5b95b8b266919c1c13784a5ffae2))

- Enhance Session class with catalog lock for thread-safe table registration ([114e260](https://github.com/mag1cfrog/timeseries-table-format/commit/114e2605b820f7513878acaf14835755a8633b48))

- Implement register_parquet method for session management with error handling ([124aa1b](https://github.com/mag1cfrog/timeseries-table-format/commit/124aa1b42b92137e3d2df7891c17e1c121162690))

- Enhance error handling in register_parquet and register_ts_table methods with rollback support ([e34e0a8](https://github.com/mag1cfrog/timeseries-table-format/commit/e34e0a8b5dc8f9b6d1e44043f9b77956ca7a075f))

- Add 'sync' feature to tokio dependency in Cargo.toml ([b8e618e](https://github.com/mag1cfrog/timeseries-table-format/commit/b8e618ed49ea14a95309facc23c90f24264a68bd))

- Add test-only helper to check if a table name exists in the DataFusion catalog ([47276b4](https://github.com/mag1cfrog/timeseries-table-format/commit/47276b41268258a17b03a4f42ede96ccbeac9496))

- Add arrow-ipc and datafusion dependencies to Cargo.toml ([dace4ae](https://github.com/mag1cfrog/timeseries-table-format/commit/dace4ae62410993a483f5cc00699717332bc2b3a))

- Enhance query parameter handling with new scalar value conversions ([3a4d9b2](https://github.com/mag1cfrog/timeseries-table-format/commit/3a4d9b2f61159a5abf0052265420118e635da319))

- Implement SQL execution method in Session class with parameter handling ([881d45a](https://github.com/mag1cfrog/timeseries-table-format/commit/881d45a05840f7070224a63623fcd55c87720730))

- Add SQL execution method with parameter handling in Session class ([4c21f80](https://github.com/mag1cfrog/timeseries-table-format/commit/4c21f808dbf107e1ffb66710a78219495967c487))

- Enhance Session and TimeSeriesTable methods with detailed docstrings ([80f1d06](https://github.com/mag1cfrog/timeseries-table-format/commit/80f1d06ecd0fd0678ae5be22ad805fac43a61cec))

- Add deregister functionality and tables retrieval for session management ([746c05b](https://github.com/mag1cfrog/timeseries-table-format/commit/746c05b3cd036cdfab27b3e319d43ac11097d182))

- Add CHANGELOG.md files for timeseries-table-datafusion and timeseries-table-python ([97cc112](https://github.com/mag1cfrog/timeseries-table-format/commit/97cc112a5caeef3c9e514f292c9b953387bdbb96))

# Changelog

All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog, and this project adheres to Semantic Versioning.
