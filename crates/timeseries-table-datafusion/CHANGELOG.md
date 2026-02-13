# Changelog
All notable changes to timeseries-table-datafusion will be documented in this file.
## 0.1.1


### Bug Fixes

- Update path handling for Windows compatibility in TsTableProvider ([9106eec](https://github.com/mag1cfrog/timeseries-table-format/commit/9106eecbbced1590127a3d54964f0cb5aadb43d5))

- Refine path handling for Windows compatibility in TsTableProvider ([fd96d40](https://github.com/mag1cfrog/timeseries-table-format/commit/fd96d400b955a3a73f3cbc5b2f219036b3335b23))


### Features

- Add object_store dependency to Cargo.toml ([fa2c9cb](https://github.com/mag1cfrog/timeseries-table-format/commit/fa2c9cb5ee4af6461f25bed49b5bb7c0d317620f))


## 0.1.0


### Bug Fixes

- Simplify code by removing unnecessary line breaks in test functions ([ce67aa0](https://github.com/mag1cfrog/timeseries-table-format/commit/ce67aa0092b5051b89ca0a9fba3238011716ac76))

- Update TsTableProvider to use async file operations for segment handling ([3238cbf](https://github.com/mag1cfrog/timeseries-table-format/commit/3238cbfc65083f414c4494734161bf000928c8d4))

- Add workspace configuration for chrono dependency ([297547f](https://github.com/mag1cfrog/timeseries-table-format/commit/297547f11467a55c38a672b01cdd0f05874c7786))

- Update import for DataSourceExec in TsTableProvider ([38a701d](https://github.com/mag1cfrog/timeseries-table-format/commit/38a701d2247589ea000dc3639eb62936f565d6f5))

- Improve error message for EXPLAIN output in explain_plan_text function ([1235be2](https://github.com/mag1cfrog/timeseries-table-format/commit/1235be22b722f090614ff4f4e6e65e1d9503def4))

- Simplify error handling in explain_plan_text function for clarity ([11939a8](https://github.com/mag1cfrog/timeseries-table-format/commit/11939a87c43448c4a0b908193f428cc130246c5f))

- Optimize timestamp conversion using div_euclid and rem_euclid for better clarity in scalar_to_datetime ([4f754cf](https://github.com/mag1cfrog/timeseries-table-format/commit/4f754cfd5531cae4a26d971dd85711995d470d9d))

- Handle potential overflow in time range calculations using checked_add_signed ([346d99b](https://github.com/mag1cfrog/timeseries-table-format/commit/346d99be72851a5804a425cd46a09cc6eeb5b8a8))

- Restrict time comparison operators in match_time_comparison function ([e419d89](https://github.com/mag1cfrog/timeseries-table-format/commit/e419d897beac27b10957e854dc01965e0013aa2f))

- Correct typo in TsTransform variant from DateTruc to DateTrunc ([bc8bb72](https://github.com/mag1cfrog/timeseries-table-format/commit/bc8bb7279043f8832da4e483d2c87606f5f6eeab))

- Correct typos in comments for clarity in time predicate functions ([a66004f](https://github.com/mag1cfrog/timeseries-table-format/commit/a66004feab90adf66a2b87ef20fa4ad2f4fc258f))

- Initialize cache with table state and version in TsTableProvider ([09994bd](https://github.com/mag1cfrog/timeseries-table-format/commit/09994bdb1edce6c0381e319eff21f5328ad7c872))

- Specify version for timeseries-table-core dependency in Cargo.toml ([8a02c13](https://github.com/mag1cfrog/timeseries-table-format/commit/8a02c1369221e5e5ec8adaf7d27353cb31e9774c))

- Correct syntax error in timeseries-table-core dependency declaration in Cargo.toml ([300a95c](https://github.com/mag1cfrog/timeseries-table-format/commit/300a95ceded493846f63854758c8dbb1c6f7c0b4))


### Code Refactoring

- Rename overlap_inclusive to overlaps_segment for clarity in TimeRange implementation ([5bc24cc](https://github.com/mag1cfrog/timeseries-table-format/commit/5bc24cc83018f1bdc951ca87ccc4fbc695eeefd0))

- Improve code formatting and simplify function signatures in ts_table_provider_tests ([7ba752a](https://github.com/mag1cfrog/timeseries-table-format/commit/7ba752ae9aba166805d1949844a9825fa3b3cc85))

- Update comment to clarify filter parameter usage in TsTableProvider ([7668f99](https://github.com/mag1cfrog/timeseries-table-format/commit/7668f9948a4be3a7d50b5d898bbc93ee34bab64b))

- Enhance documentation for error handling and time comparison functions in TsTableProvider ([07aa847](https://github.com/mag1cfrog/timeseries-table-format/commit/07aa8475234f14b70a6e83a804b043b26ecad395))

- Split the unit tests into a separate sub-module ([a819b4c](https://github.com/mag1cfrog/timeseries-table-format/commit/a819b4c8c4252618f70e396d7a08923a352fdf62))

- Split into several sub-modules for readbility ([27d07ef](https://github.com/mag1cfrog/timeseries-table-format/commit/27d07eff11612cef69e8f9698d483e1a73c29880))

- Rewrite integration tests to use test_util for efficiency and accuracy ([d4b0fd9](https://github.com/mag1cfrog/timeseries-table-format/commit/d4b0fd99ab0d99fe5795b4cf5917f41b6833f619))

- Simplify SQL predicate test expressions for clarity ([2530182](https://github.com/mag1cfrog/timeseries-table-format/commit/2530182e1ece25c452e72815df4d7f6a60a8e7f6))

- Reorganize imports and improve test function formatting for clarity ([12989ff](https://github.com/mag1cfrog/timeseries-table-format/commit/12989ffb5f0cedc4ef720231f80fa699f9cba2a8))

- Simplify SQL predicate formatting for to_unixtime tests ([6fc76e6](https://github.com/mag1cfrog/timeseries-table-format/commit/6fc76e6056e14b6accf19c130d7bcf8b884c7922))

- Update TsTableProvider to use StorageLocation instead of TableLocation ([bfe8429](https://github.com/mag1cfrog/timeseries-table-format/commit/bfe8429efdef022c741bcd3a6b321c0c417f054d))

- Remove unnecessary blank line in binary function ([84f7bd9](https://github.com/mag1cfrog/timeseries-table-format/commit/84f7bd9ff726a0312ac23b1e5efcd069a5a27565))

- Update module imports to use new table metadata structure ([260c43d](https://github.com/mag1cfrog/timeseries-table-format/commit/260c43da0ce3c83a66c0f359967359f0f3727382))

- Bump version of timeseries-table-core to 0.2.0 in Cargo.toml files ([d240346](https://github.com/mag1cfrog/timeseries-table-format/commit/d2403463a40569dc09ae8022cde5410cde429f2f))


### Documentation

- Update README with detailed installation and usage examples ([a55504f](https://github.com/mag1cfrog/timeseries-table-format/commit/a55504fc2cbbdbc8ea51487f77801c2c706eaca5))


### Features

- Add timeseries-table-datafusion crate to workspace ([99a93d8](https://github.com/mag1cfrog/timeseries-table-format/commit/99a93d8df7a970df7c815f699f15a058f64cb8c2))

- Add initial lib.rs file and define datafusion dependency ([686b3c3](https://github.com/mag1cfrog/timeseries-table-format/commit/686b3c32a853d96a85c73958115a6346dd718ebf))

- Update datafusion dependency to use workspace reference ([c3416ac](https://github.com/mag1cfrog/timeseries-table-format/commit/c3416ac36d60996f1b3fb010a4c28660e5827fa2))

- Add timeseries-table-core dependency to timeseries-table-datafusion ([babb30d](https://github.com/mag1cfrog/timeseries-table-format/commit/babb30d1f64f5465838996f2701a849ad16cb8da))

- Add arrow dependency to timeseries-table-datafusion ([2bdbf16](https://github.com/mag1cfrog/timeseries-table-format/commit/2bdbf16d9d9d323e0b907769f376f0766bcca6f1))

- Update documentation for DataFusion integration in lib.rs ([568d185](https://github.com/mag1cfrog/timeseries-table-format/commit/568d18542a34527b894ef465974f79f9121b0b52))

- Implement TsTableProvider for DataFusion integration ([0dbeff0](https://github.com/mag1cfrog/timeseries-table-format/commit/0dbeff0c4ff134b4bdbfc1e82fabcdf3c6299646))

- Add smoke tests for TsTableProvider construction ([c8b40a2](https://github.com/mag1cfrog/timeseries-table-format/commit/c8b40a2e856d874b7421ec31b8bbb144b7b74e50))

- Enhance documentation for TsTableProvider struct ([940bfc8](https://github.com/mag1cfrog/timeseries-table-format/commit/940bfc8faa86c77a87f7e7d4c1cf5fe67964425e))

- Expose TsTableProvider in the DataFusion library ([8841627](https://github.com/mag1cfrog/timeseries-table-format/commit/8841627682ebacd97631fee22086f8df1be65f40))

- Remove smoke tests for TsTableProvider construction ([93a84ca](https://github.com/mag1cfrog/timeseries-table-format/commit/93a84cafc8e3f9951102a323728f049208eb4247))

- Refactor TsTableProvider to include TimeSeriesTable and update constructor ([ee37856](https://github.com/mag1cfrog/timeseries-table-format/commit/ee37856998a152d6fd3f2042f4851aef7a72de89))

- Update tokio dependency to use workspace configuration across all crates ([d207bd1](https://github.com/mag1cfrog/timeseries-table-format/commit/d207bd1d6ba2231783ad33b6125492cf123ecbcd))

- Implement TsTableProvider with caching and schema retrieval ([b1e127d](https://github.com/mag1cfrog/timeseries-table-format/commit/b1e127dea84bb9244fbae7a37b96ff66c42659aa))

- Enhance TsTableProvider with segment file handling and caching improvements ([442f5b3](https://github.com/mag1cfrog/timeseries-table-format/commit/442f5b31b0a95a2e4feb8f9bfe793808a1196e7f))

- Reorganize dev-dependencies in Cargo.toml for clarity ([cdfa7a5](https://github.com/mag1cfrog/timeseries-table-format/commit/cdfa7a512c77300150f00afda32c39dfbd7291fe))

- Add integration tests for TsTableProvider functionality ([4eb2252](https://github.com/mag1cfrog/timeseries-table-format/commit/4eb2252303a78159322c52da24aa8054466ed8bf))

- Refactor time conversion in tests to use minutes_to_millis function ([dc63fb0](https://github.com/mag1cfrog/timeseries-table-format/commit/dc63fb03619556a44907e48ebf913672e5e1f5e1))

- Add nested table meta creation and schema support tests for TsTableProvider ([a6945f1](https://github.com/mag1cfrog/timeseries-table-format/commit/a6945f1af00926c8b66114421a514572567513d2))

- Update documentation for TsTableProvider schema handling ([ed56152](https://github.com/mag1cfrog/timeseries-table-format/commit/ed561528057e406d4a2be802dd74bc216eb0be8d))

- Implement time range pruning for segment filtering in TsTableProvider ([1e019fa](https://github.com/mag1cfrog/timeseries-table-format/commit/1e019fa4c6c4e2fb5f7f9aa83b4c9b6970ba5ecf))

- Add tests for time range pruning and explain plan functionality in TsTableProvider ([ee6a360](https://github.com/mag1cfrog/timeseries-table-format/commit/ee6a360907e15ee3f48bcb04314ff679d357a0f2))

- Implement filter pushdown for time range pruning in TsTableProvider ([0e14e6d](https://github.com/mag1cfrog/timeseries-table-format/commit/0e14e6d26f093bb081a8b7d7bc4f1a0e185433e3))

- Add filter pushdown tests and enhance parquet writing with properties in TsTableProvider ([fbe8b2f](https://github.com/mag1cfrog/timeseries-table-format/commit/fbe8b2f8e2e978026fcef889241ffdcec511893e))

- Implement time range pruning with enhanced time predicate evaluation ([2bb3660](https://github.com/mag1cfrog/timeseries-table-format/commit/2bb3660ff7de618287fb7c732d51b8be1f1c29d9))

- Add compile-time predicate tests for time series constraints ([3124f07](https://github.com/mag1cfrog/timeseries-table-format/commit/3124f07c85aeab667067276b2c950d7f262b5eb1))

- Extend TimePred enum with NonTime variant for time-independent predicates ([a7fb488](https://github.com/mag1cfrog/timeseries-table-format/commit/a7fb488c8eb6d662d4d850931ae09ebc858eb5db))

- Add contains_ts function to check for timestamp column in expression tree ([db61487](https://github.com/mag1cfrog/timeseries-table-format/commit/db6148766b23c057f18c479944cf30f59d74d405))

- Enhance contains_ts function to support BETWEEN expressions for timestamp column checks ([52e7304](https://github.com/mag1cfrog/timeseries-table-format/commit/52e7304330451b7f7697b463e25c6f2ae8beefcf))

- Implement BETWEEN expression support for time series filtering ([4c66a13](https://github.com/mag1cfrog/timeseries-table-format/commit/4c66a1305c1d792b03819bf06e0c89b54aae2183))

- Add support for IN and NOT IN expressions in time series filtering ([d1f5043](https://github.com/mag1cfrog/timeseries-table-format/commit/d1f50437c847fe9138f859ad2e71ceb2dd50f56f))

- Add filter pruning tests for IN and NOT IN expressions in time series queries ([89d297e](https://github.com/mag1cfrog/timeseries-table-format/commit/89d297e626cc6b7b9b3af2647a22e4d2293f0e01))

- Improve AND expression handling to preserve constraints for Unknown values ([ed423eb](https://github.com/mag1cfrog/timeseries-table-format/commit/ed423ebe773df1d310f4db0bcf0c70e9252e1fbf))

- Clarify comment on Unknown time predicate for better pruning understanding ([3a3c56f](https://github.com/mag1cfrog/timeseries-table-format/commit/3a3c56fcd7fd3fd95f32728b3cc3371bdbd52b78))

- Enhance timestamp column expression handling with new utility functions ([9231abf](https://github.com/mag1cfrog/timeseries-table-format/commit/9231abfa37f17886afc62221d509f72234a020c1))

- Enhance timestamp parsing by adding support for date-only strings and refactoring related functions ([1ea0b72](https://github.com/mag1cfrog/timeseries-table-format/commit/1ea0b72e0fc3f00870bb6ba6fc2608895341c43b))

- Add functions for numeric expression handling and timestamp conversion ([3a200b3](https://github.com/mag1cfrog/timeseries-table-format/commit/3a200b3d5ade18dbdd2a507c773787301e488dd3))

- Add support for flipping operators in timestamp comparisons ([0112a02](https://github.com/mag1cfrog/timeseries-table-format/commit/0112a02c582f39147e12ac3b3b05578e696995cb))

- Refactor timestamp parsing and comparison logic for improved clarity and functionality ([d4b23a8](https://github.com/mag1cfrog/timeseries-table-format/commit/d4b23a8e9c49f025e8cd1111d926362c4a9aae1b))

- Remove redundant match_time_comparison function to streamline timestamp comparison logic ([0ab369a](https://github.com/mag1cfrog/timeseries-table-format/commit/0ab369abadf1b554670cfaf4426e7412c59e16af))

- Restrict supported comparison operators for compile-time reasoning in timestamp comparisons ([f1986a1](https://github.com/mag1cfrog/timeseries-table-format/commit/f1986a176dbb5023df50555d07bd6d036802aea2))

- Add test for unsupported operator rejection with timestamp literal ([75c3a6d](https://github.com/mag1cfrog/timeseries-table-format/commit/75c3a6da9b09b575209e8a2f1ade30760ae2b3f3))

- Enhance timestamp expression handling in filter pushdown ([3d4d674](https://github.com/mag1cfrog/timeseries-table-format/commit/3d4d67431054cc70146c3487cc236b836c746a95))

- Implement unified interval handling for timestamp expressions ([f504452](https://github.com/mag1cfrog/timeseries-table-format/commit/f5044529d5c655113b7cd1f5c40d1de79d20f41f))

- Refine timestamp interval handling in expression evaluation ([1cdd581](https://github.com/mag1cfrog/timeseries-table-format/commit/1cdd5815bf1ce864fc205f205283b0965206424d))

- Enhance timestamp expression handling with additional test cases and utility functions ([e94df59](https://github.com/mag1cfrog/timeseries-table-format/commit/e94df59ca0688eaaa70ec1ee22dc28d85de2930a))

- Add test utilities for time predicate compilation and interval handling ([6ef2edb](https://github.com/mag1cfrog/timeseries-table-format/commit/6ef2edbd31aab98a4c4582f2b4cf99cc543616dd))

- Add SQL predicate tests for timestamp expressions and interval handling ([ce25066](https://github.com/mag1cfrog/timeseries-table-format/commit/ce25066aa48c1f4491de38f0313bf0568c6466b2))

- Add evaluation logic for time predicates against segment intervals ([acc6592](https://github.com/mag1cfrog/timeseries-table-format/commit/acc6592d838e9f1212b6845f348c1435ca356d91))

- Add README documentation for timeseries-table-datafusion with pruning logic details ([f84fb1c](https://github.com/mag1cfrog/timeseries-table-format/commit/f84fb1c6475a9931678fef3a151caeaeccac6564))

- Add support for ToUnixtime transformation in time predicate compilation ([c3fad4d](https://github.com/mag1cfrog/timeseries-table-format/commit/c3fad4d1c45c7f509801c603c2c2696612600875))

- Add SQL tests for to_unixtime function with numeric and string comparisons ([fae098d](https://github.com/mag1cfrog/timeseries-table-format/commit/fae098d1403cbf6b26cae364ffa40a2ea014e34e))

- Add support for to_unixtime function with numeric literal comparisons in README ([5ed4d78](https://github.com/mag1cfrog/timeseries-table-format/commit/5ed4d7807e9d4085f9c47caddb0fc787e23153f1))

- Add ToDate transformation and parsing for date literals in time predicates ([da08da2](https://github.com/mag1cfrog/timeseries-table-format/commit/da08da2c62f2ece0641af11fc80ef5848f350b18))

- Add chrono-tz dependency for timezone support in datafusion ([7a92327](https://github.com/mag1cfrog/timeseries-table-format/commit/7a92327981358dfa18a9e0a0fe67b5ecebc93dea))

- Implement timezone parsing and integration in time predicates ([43f918e](https://github.com/mag1cfrog/timeseries-table-format/commit/43f918e3f442fb8357e2a9d85a22e25ec4237824))

- Update compile_time_pred function to accept an optional parameter for tests ([7bf01ca](https://github.com/mag1cfrog/timeseries-table-format/commit/7bf01ca86e0ea7a2b07592f225156347354de94b))

- Enhance time predicate compilation with optional timezone support ([d82c3c6](https://github.com/mag1cfrog/timeseries-table-format/commit/d82c3c66c4ba19d75dd69904199cd229efcd5e72))

- Add tests for to_date function and update eval_time_pred for UTC ([3cf4960](https://github.com/mag1cfrog/timeseries-table-format/commit/3cf496082244432994c8f7675c108ec2ae017633))

- Add support for to_date function with date literal comparisons ([efe5799](https://github.com/mag1cfrog/timeseries-table-format/commit/efe57996ca5a73a240e979c6a154449fcfab6cfd))

- Implement date truncation functionality for time predicates ([3c32a1f](https://github.com/mag1cfrog/timeseries-table-format/commit/3c32a1fd7c3f27144f18a1240ec5a73b23f1fc88))

- Add tests for date truncation with timezone handling and edge cases ([58e44aa](https://github.com/mag1cfrog/timeseries-table-format/commit/58e44aa85874d9aa4674f1ad21fae3ec14cbc22c))

- Add tests for date truncation functionality with timezone handling ([e8610ac](https://github.com/mag1cfrog/timeseries-table-format/commit/e8610aca9b90b733e2342d3cb646f9d7b01782c6))

- Adjust timestamp comparison logic for alignment in compile_transform_cmp function ([d050fc7](https://github.com/mag1cfrog/timeseries-table-format/commit/d050fc7791f05a1fa10b03a33a2640acf93161e9))

- Add test for date truncation comparison with aligned hour ([6d70f63](https://github.com/mag1cfrog/timeseries-table-format/commit/6d70f63de7852f94df1950a333d33a4a701b4090))

- Add support for date_trunc function in timestamp comparisons ([da37b6b](https://github.com/mag1cfrog/timeseries-table-format/commit/da37b6b979b4450e238f847a70a05ada68a8a867))

- Add support for date_bin function with interval and timestamp literals ([baf5cb6](https://github.com/mag1cfrog/timeseries-table-format/commit/baf5cb6fde22273fb75c512cb6953edf8bfa5f21))

- Implement date_bin function with stride and origin support ([444e993](https://github.com/mag1cfrog/timeseries-table-format/commit/444e9938246c81d713cf905167f3f849536be24c))

- Add SQL tests for date_bin function with minute intervals ([7245411](https://github.com/mag1cfrog/timeseries-table-format/commit/7245411a4246c263760bf312bd32372e8fdcea0f))

- Add explain plan tests for time filter pruning in DataFusion ([2fac5c4](https://github.com/mag1cfrog/timeseries-table-format/commit/2fac5c40ec0b7ee90e06d166d9c64c3177560e8f))

- Replace file size retrieval with file_size function for improved handling ([fed45be](https://github.com/mag1cfrog/timeseries-table-format/commit/fed45bed37147320a86001e905af1ea6d3cb992a))

- Add async-trait dependency to multiple crates for async support ([c29e229](https://github.com/mag1cfrog/timeseries-table-format/commit/c29e229bf9aa473fc800e064921fd1d784975d13))

- Add test-counters feature to timeseries-table-datafusion dependency ([8476ffe](https://github.com/mag1cfrog/timeseries-table-format/commit/8476ffe0f02332e1d7b1e46259d7af87c4c03195))

- Implement table metadata creation and test for provider cache initialization ([75d2aa0](https://github.com/mag1cfrog/timeseries-table-format/commit/75d2aa0d5255ac46a320b95a5fb2a6f48fa85d61))

- Add example query for NVDA sample table with 5-period moving average ([dd309ab](https://github.com/mag1cfrog/timeseries-table-format/commit/dd309ab0fcbf346a95bfda85c7e5c0adf39eb6c9))

- Add CHANGELOG.md files for timeseries-table-datafusion and timeseries-table-python ([97cc112](https://github.com/mag1cfrog/timeseries-table-format/commit/97cc112a5caeef3c9e514f292c9b953387bdbb96))


### Testing

- Add debug output for parquet predicate display in scan_attaches_parquet_predicate_for_non_time_filters ([1c21599](https://github.com/mag1cfrog/timeseries-table-format/commit/1c215993d1c2f58c18fdeaab0f8d929843aed9f3))

- Add filter pruning tests for time equality and comparison operators ([c0eefef](https://github.com/mag1cfrog/timeseries-table-format/commit/c0eefef0c0f2b40165e3b0796032e9cc7be6ba79))

- Add filter pruning tests for OR and NOT time predicates ([ccad37d](https://github.com/mag1cfrog/timeseries-table-format/commit/ccad37dd88b0d8e20002e01438e592a96c9fe91e))

- Add filter pruning tests for BETWEEN and NOT BETWEEN time predicates ([7700cac](https://github.com/mag1cfrog/timeseries-table-format/commit/7700cac71ea9d965d0125f1694806d1b3853e6f4))


### Style

- Format date_bin function call for better readability ([4222dd1](https://github.com/mag1cfrog/timeseries-table-format/commit/4222dd1005a417595f2d06d736d5fd4d00288df0))

# Changelog

All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog, and this project adheres to Semantic Versioning.

