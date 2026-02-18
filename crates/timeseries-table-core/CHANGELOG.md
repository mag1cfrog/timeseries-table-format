# Changelog
All notable changes to timeseries-table-core will be documented in this file.
## 0.2.2


### Bug Fixes

- Correct workspace root path resolution in ingest_nvda example ([cf3900d](https://github.com/mag1cfrog/timeseries-table-format/commit/cf3900dcb659934825b4b4d28ffda3fe3b06645d))


## 0.2.1


### Bug Fixes

- Use const initialization for thread-local Cell in rebuild counter ([31da389](https://github.com/mag1cfrog/timeseries-table-format/commit/31da389fbe1e693ecc80e792fc2e48333565d700))


### Code Refactoring

- Replace AtomicUsize with thread-local Cell for rebuild counter ([89d763e](https://github.com/mag1cfrog/timeseries-table-format/commit/89d763e14fdf52fe51cc350dc204a1256fb961ad))


## 0.2.0


### Bug Fixes

- Remove unnecessary comment about trailing space in CURRENT update ([d142f7a](https://github.com/mag1cfrog/timeseries-table-format/commit/d142f7af89351af2845e9a32cbc7c2264da9f4e0))

- Remove redundant comment in StorageError enum for clarity ([638c664](https://github.com/mag1cfrog/timeseries-table-format/commit/638c664ad6756619b260db86ee75ec2e20722cef))

- Correct typo in comment for segment insertion logic ([e8fd9d5](https://github.com/mag1cfrog/timeseries-table-format/commit/e8fd9d521402859d1afa2ae9754e1c22f4359ef2))

- Correct typos in comments for clarity in read_head_tail_4 function ([bbce721](https://github.com/mag1cfrog/timeseries-table-format/commit/bbce7211ee0db855e1b1f141660034086ce5eb8f))

- Expose SegmentResult type for external use ([d7e9507](https://github.com/mag1cfrog/timeseries-table-format/commit/d7e9507c84480a9461fe288d7bae7921755d5a99))

- Update clippy configuration path for timeseries-table-core ([bd399e4](https://github.com/mag1cfrog/timeseries-table-format/commit/bd399e40367019f5528be3a3d84464a862094616))

- Correct typo in comment to improve clarity in create method ([f3a5cec](https://github.com/mag1cfrog/timeseries-table-format/commit/f3a5cecb3b0eb9b13a8056df003afdb02fbe6cfb))

- Correct typo in schema compatibility error enum for time index type mismatch ([609ecdd](https://github.com/mag1cfrog/timeseries-table-format/commit/609ecdd7068607f6dfea1da7d717ab3498bc6a86))

- Correct spelling of 'UnsupportedTimeType' in error handling ([755a5d5](https://github.com/mag1cfrog/timeseries-table-format/commit/755a5d539031e309e902e5a6bff9762b6a740d62))

- Improve error message for invalid scan range and clarify sorting logic in time-series table ([4b273f7](https://github.com/mag1cfrog/timeseries-table-format/commit/4b273f7ff258e58acb37be4ed25a844eaa5710d8))

- Correct typo in comment for timestamp array downcasting ([2921829](https://github.com/mag1cfrog/timeseries-table-format/commit/29218296ef9051a1df259d5eade4eacc33c93d9c))

- Fix typo and make doc clearer ([4c42f5c](https://github.com/mag1cfrog/timeseries-table-format/commit/4c42f5c7c69b88cfb40f6c52c1ac3e7ca98ec288))

- Add missing dependency for roaring crate in Cargo.toml ([871ee6d](https://github.com/mag1cfrog/timeseries-table-format/commit/871ee6dd8fa70d9fed47b63a1bc16ab3efa622d2))

- Correct typo in coverage_ratio method name and add comprehensive tests for coverage module ([6a221da](https://github.com/mag1cfrog/timeseries-table-format/commit/6a221da9fcd6d3fbc41addcf37ec9174b6233cca))

- Correct assertion in last_run_with_min_len test for coverage module ([77df769](https://github.com/mag1cfrog/timeseries-table-format/commit/77df769272482d26a5aa693e4a7a654473fd8bf9))

- Update comment for min_len == 0 case in Coverage implementation ([7be7db4](https://github.com/mag1cfrog/timeseries-table-format/commit/7be7db4b59331582ce62e73647e7516a7d2495e2))

- Update usage examples to include re-exported RoaringBitmap ([f84a94e](https://github.com/mag1cfrog/timeseries-table-format/commit/f84a94e2c661b37276739f9cacc85021d3dc92e6))

- Update documentation to reflect correct return type in runs_from_bitmap function ([cc13b4e](https://github.com/mag1cfrog/timeseries-table-format/commit/cc13b4eebf374aaf0935dbc00f5b38cde1404f9c))

- Prevent overflow in runs_from_bitmap and split_runs_by_len functions ([1e83e7d](https://github.com/mag1cfrog/timeseries-table-format/commit/1e83e7d00f211fa54ea9ec6b167319f36b5741f8))

- Correct typo in comment for coverage ID validation function ([d272f8d](https://github.com/mag1cfrog/timeseries-table-format/commit/d272f8d722f62076b4ff87ee2e68269c81c84ac6))

- Correct error message documentation for Arrow read errors in SegmentCoverageError ([c49f332](https://github.com/mag1cfrog/timeseries-table-format/commit/c49f332a000aff7009e7ac3c12d3f0d38814267b))

- Correct function name in documentation for schema requirement ([e8b06d7](https://github.com/mag1cfrog/timeseries-table-format/commit/e8b06d7d01c9275ec26327df7ab4d19474aa6474))

- Update documentation for require_table_schema to clarify schema adoption behavior ([250bbc1](https://github.com/mag1cfrog/timeseries-table-format/commit/250bbc13c0ed2e395f0b0cff575c5843852e9a18))

- Clarify error message for missing coverage_path in existing segments ([b300291](https://github.com/mag1cfrog/timeseries-table-format/commit/b300291bc068d99f41b16fff592dc1ec7b924d17))

- Enforce OCC invariant by asserting expected transaction log version ([8116aa6](https://github.com/mag1cfrog/timeseries-table-format/commit/8116aa6adc4ac6db26e9829d24e2ee6e6078f105))

- Ensure table state is updated after modifying coverage ([52486fc](https://github.com/mag1cfrog/timeseries-table-format/commit/52486fcea461f2079090515d7847da515d166d0f))

- Reorder module declarations for consistency in helpers.rs ([dcd98c6](https://github.com/mag1cfrog/timeseries-table-format/commit/dcd98c64557113bc8fe760beac7785062f4a96be))

- Correct formatting in EntityMismatch error message for clarity ([527e9d7](https://github.com/mag1cfrog/timeseries-table-format/commit/527e9d7695c3034a4c82eb4cd21664f45d946904))

- Correct comment typo in try_entity_identity_from_stats function ([e2b0e6d](https://github.com/mag1cfrog/timeseries-table-format/commit/e2b0e6d5260e87b6bfe964c61a0e5633875e87c8))

- Remove unnecessary whitespace in SegmentMeta struct ([b3304d9](https://github.com/mag1cfrog/timeseries-table-format/commit/b3304d9045c4ffdfd44a9b415d32afd47f6d7053))

- Update chrono dependency to use workspace configuration ([be56d18](https://github.com/mag1cfrog/timeseries-table-format/commit/be56d181cf12a1780ca421f853c13c78badab580))

- Correct typo in comment for TempFileGuard drop implementation ([b581902](https://github.com/mag1cfrog/timeseries-table-format/commit/b581902c5e7c0473904593437a9354300668fd90))

- Use const initialization for REBUILD_TABLE_STATE_COUNT ([68f8470](https://github.com/mag1cfrog/timeseries-table-format/commit/68f8470a7146646f0c276efc47e3ae2fc8265092))

- Remove unnecessary data cloning in segment_meta_from_parquet_bytes_with_report ([980af15](https://github.com/mag1cfrog/timeseries-table-format/commit/980af1540d03e577fe4083538faa7ad8851df564))


### Code Refactoring

- Simplify imports and remove unused read_to_string_rel function ([4c69510](https://github.com/mag1cfrog/timeseries-table-format/commit/4c69510b2cc63f707f632b0911728338e6f9e3e5))

- Rename the log module to transaction_log for clarity ([09c1c92](https://github.com/mag1cfrog/timeseries-table-format/commit/09c1c9288f6afa8b51a1a1a494eaa6521ba3b473))

- Remove unused test module from lib.rs ([8f7e934](https://github.com/mag1cfrog/timeseries-table-format/commit/8f7e934b9a385a500e3ea6db42ab75fb62c01f4f))

- Remove obsolete log actions and log store modules ([f1c9690](https://github.com/mag1cfrog/timeseries-table-format/commit/f1c969022762e832b77edbadc3e23845c950f667))

- Improve error handling in SegmentMetaError by using StorageError ([a252f04](https://github.com/mag1cfrog/timeseries-table-format/commit/a252f04ed443f1e9562907137db9a4095c98d5c1))

- Use unstable sort for candidates in time series scan ([d5a4766](https://github.com/mag1cfrog/timeseries-table-format/commit/d5a4766bb92452668768d6831457f9583b9e4f76))

- Enhance coverage module with additional methods and improved documentation ([0b76aa1](https://github.com/mag1cfrog/timeseries-table-format/commit/0b76aa1e1319ca5be4866d4a20ea871b31d6ba14))

- Change return types from u64 to Bucket for coverage methods ([55012c3](https://github.com/mag1cfrog/timeseries-table-format/commit/55012c37c4b448c04107d820410dbf31336c7e4a))

- Clean up import statements and improve readability in tests ([f38da05](https://github.com/mag1cfrog/timeseries-table-format/commit/f38da051117426ff5443b75c14dc7ed7b4ed978e))

- Enhance comment for coverage ID hash collision resistance ([fc15f93](https://github.com/mag1cfrog/timeseries-table-format/commit/fc15f932cabfc01962b0c004c28939f28dd72925))

- Rename test for clarity on duplicate ID handling with non-overlapping coverage ([c0325b0](https://github.com/mag1cfrog/timeseries-table-format/commit/c0325b0263d8ca73ddc9b5cf3fef3267f8780925))

- Create a separate test_util for shared testing utils ([ce80ec6](https://github.com/mag1cfrog/timeseries-table-format/commit/ce80ec609433363dbb0c6e9c4c56f3fa9c34a5f2))

- Move scan related API to a separate submodule ([38725f7](https://github.com/mag1cfrog/timeseries-table-format/commit/38725f70f811859241b3c0d2ebded9e24d04ca2b))

- Clean up formatting in append module for improved readability ([0af37d7](https://github.com/mag1cfrog/timeseries-table-format/commit/0af37d77f4504cd51457865572894f623b7ef14e))

- Simplify assertions in last_window_contiguous_runs test ([b6e1632](https://github.com/mag1cfrog/timeseries-table-format/commit/b6e16324e415d580546828cb7793e45bbbb52a78))

- Split out the stats parquet code as a separate module ([24351ae](https://github.com/mag1cfrog/timeseries-table-format/commit/24351aea5f3bdec848fceb302af7399793831ca5))

- Split out schema casting code into a separate module ([50423d2](https://github.com/mag1cfrog/timeseries-table-format/commit/50423d2dcd1dbfb66c0c4ae79f5045aa46576ab8))

- Improve README clarity and structure for core engine responsibilities ([bbf771e](https://github.com/mag1cfrog/timeseries-table-format/commit/bbf771eaf83deb60c0713d38779db5438ea455c9))

- Remove unused import in tests module ([bee003b](https://github.com/mag1cfrog/timeseries-table-format/commit/bee003b474d80c59ac2533eb152d536b632b615b))

- Remove redundant documentation for StorageLocation enum ([8e7e9ec](https://github.com/mag1cfrog/timeseries-table-format/commit/8e7e9ec53064be0a3207f8f9e089bc70da048c1a))

- Update storage location references to use as_ref() for consistency ([73d4aab](https://github.com/mag1cfrog/timeseries-table-format/commit/73d4aab7bf84189af223eac343aa3ad1f5b12387))

- Update StorageLocation documentation for clarity and future extensibility ([ae4559f](https://github.com/mag1cfrog/timeseries-table-format/commit/ae4559f7c40ce3071484ecfbd858fb4c863e7ced))

- Remove deprecated metadata log module and update table metadata import ([2470b50](https://github.com/mag1cfrog/timeseries-table-format/commit/2470b503f9b7fb3f352ab94eb71af56e86dba874))

- Streamline segment error handling and metadata structure ([7db393d](https://github.com/mag1cfrog/timeseries-table-format/commit/7db393d47a9e58c7c0834cf22056ecbbfd462b53))

- Replace logical schema definitions with compatibility shim for older paths ([7514c82](https://github.com/mag1cfrog/timeseries-table-format/commit/7514c828a4c12670ea4286255b3413403a3e67f1))

- Update metadata module documentation and structure for clarity ([4524370](https://github.com/mag1cfrog/timeseries-table-format/commit/4524370d7c8cd600970b849665211429464ee894))

- Remove timestamp column error handling and update compatibility shim ([8de5fe4](https://github.com/mag1cfrog/timeseries-table-format/commit/8de5fe45d4708f8d95e0a75af19d13c74517afd6))

- Unify segment error handling by replacing SegmentMetaError with SegmentError ([a90c018](https://github.com/mag1cfrog/timeseries-table-format/commit/a90c0185aa49afed33eb8496649833e1b43ac597))

- Update metadata module description for clarity and accuracy ([eac0810](https://github.com/mag1cfrog/timeseries-table-format/commit/eac08107a4c1778c42629f15c85cbfe4b1462f20))

- Add on-disk layout helpers for table root and coverage data ([74da25b](https://github.com/mag1cfrog/timeseries-table-format/commit/74da25bcf1e99c20dfba42a74a2214f2ea737beb))

- Simplify layout module documentation and remove unused coverage data definitions ([bb93f96](https://github.com/mag1cfrog/timeseries-table-format/commit/bb93f968bbc2910db0b3524e12d384771062bd8a))

- Implement asynchronous file operations with atomic writes and enhanced error handling ([fec82ee](https://github.com/mag1cfrog/timeseries-table-format/commit/fec82ee70e468be86813f491e60977f18a70df23))

- Remove unused file operations and retain compatibility shim ([237ac97](https://github.com/mag1cfrog/timeseries-table-format/commit/237ac977f0c61b96e46da345f826ff580644bff5))

- Reorder module declarations for improved organization ([a9f36f5](https://github.com/mag1cfrog/timeseries-table-format/commit/a9f36f5615ebb76f7394c8728ca2b9ad8aad3b05))

- Replace hardcoded values with layout constants in TransactionLogStore ([e94db91](https://github.com/mag1cfrog/timeseries-table-format/commit/e94db91bec0aa0337da80969ab065a1ffa5dd14c))

- Update import path for CoverageLayoutError in coverage_sidecar module ([4d5b663](https://github.com/mag1cfrog/timeseries-table-format/commit/4d5b663bb0f545cbb4432a6b13f09a2f1dc29cd8))

- Update commit path construction to use layout helper ([843fe40](https://github.com/mag1cfrog/timeseries-table-format/commit/843fe40f2456df9105792caa6421b637247dc0c7))

- Use layout constant for data directory in TableLocation ([84c8f83](https://github.com/mag1cfrog/timeseries-table-format/commit/84c8f8334ba1ee20f670be1d7a37f0c2e7d49dcd))

- Update path construction to use layout helper in tests and append module ([50da2db](https://github.com/mag1cfrog/timeseries-table-format/commit/50da2db27b939c03cb24783c2b5bc1d3a9ec8d05))

- Clarify documentation on relative path usage in layout module ([b786c1d](https://github.com/mag1cfrog/timeseries-table-format/commit/b786c1df2204bfe9c400c33c4dcf139f92d82305))

- Replace time_bucket helpers with compatibility shim for coverage bucket ([45dabaa](https://github.com/mag1cfrog/timeseries-table-format/commit/45dabaa961bec0685338e0f0af86f8566542b8c7))

- Unify coverage module by replacing segment coverage helpers with parquet coverage implementation ([9d673a5](https://github.com/mag1cfrog/timeseries-table-format/commit/9d673a56818eae880418fdb9e71316e8fe4710c3))

- Remove legacy coverage query APIs and unify with new coverage implementation ([6d29993](https://github.com/mag1cfrog/timeseries-table-format/commit/6d29993857af9437dde15cadc735cf426dc99325))

- Remove coverage state helpers and unify with canonical implementation ([b50636e](https://github.com/mag1cfrog/timeseries-table-format/commit/b50636ecf357a0052373370e67b11e55dee2372a))

- Consolidate coverage sidecar functionality into io module and update compatibility shim ([8b2b73a](https://github.com/mag1cfrog/timeseries-table-format/commit/8b2b73ae6d321f83977115e399b3b84c070dfb4f))

- Add coverage layout helpers for ID validation and path construction ([3fce077](https://github.com/mag1cfrog/timeseries-table-format/commit/3fce077e859a9130b9d233eb1eceaa797ffa8b2e))

- Unify coverage layout by re-exporting from coverage module and removing legacy definitions ([46fe9da](https://github.com/mag1cfrog/timeseries-table-format/commit/46fe9daeb5495aebe0d053cad64bfd2996f7831d))

- Reorganize coverage module by adding submodules and updating imports ([95209f1](https://github.com/mag1cfrog/timeseries-table-format/commit/95209f1028e0b0d738db30063d3766f2a8f6e0d0))

- Add shared row-group parallelism helpers for Parquet scans ([267e3a1](https://github.com/mag1cfrog/timeseries-table-format/commit/267e3a14c9083cce3a1109b740f1c8d446bb5761))

- Add Parquet segment metadata extraction module ([1a128b1](https://github.com/mag1cfrog/timeseries-table-format/commit/1a128b1628cac5a87938e7b3885843c8531c8695))

- Streamline Parquet helper module by removing unused imports ([4ace42f](https://github.com/mag1cfrog/timeseries-table-format/commit/4ace42fdb11a8c494f1027358154993f4344565e))

- Enhance Parquet module documentation and streamline imports ([df7a085](https://github.com/mag1cfrog/timeseries-table-format/commit/df7a085717df816bf5d3ca8b136ef4740df2847a))

- Add append profiling report types and builder ([358b07b](https://github.com/mag1cfrog/timeseries-table-format/commit/358b07b4a4c6ab66349e89b697fb0f79a573c25a))

- Replace old test utility with new implementation from table module ([e49aaea](https://github.com/mag1cfrog/timeseries-table-format/commit/e49aaeafb6e4b0a9aa5a19ff79809ae91732c8b3))

- Replace table error module with compatibility shim for legacy support ([7573b74](https://github.com/mag1cfrog/timeseries-table-format/commit/7573b745b2ecacc180dd698e511ab2154bce0e88))

- Update module documentation and remove unused code ([db5b1c3](https://github.com/mag1cfrog/timeseries-table-format/commit/db5b1c3d8250dcabab3583664e678856e71cc4de))

- Enhance documentation and structure of TimeSeriesTable module ([dd70195](https://github.com/mag1cfrog/timeseries-table-format/commit/dd70195c67060ce073a9efa324733f08edbce448))

- Reorganize imports and add compatibility shim for append_report module ([f60792b](https://github.com/mag1cfrog/timeseries-table-format/commit/f60792b090324ba58ec0a3ed059c9f5cfd17fcaf))

- Remove compatibility shims and unused modules from timeseries-table-core ([82e3a77](https://github.com/mag1cfrog/timeseries-table-format/commit/82e3a77ea6ed7344e7a4611673a8866beaae3edd))

- Update module imports to use new table metadata structure ([260c43d](https://github.com/mag1cfrog/timeseries-table-format/commit/260c43da0ce3c83a66c0f359967359f0f3727382))

- Add deterministic ordering function for segment metadata and corresponding tests ([1ec14d9](https://github.com/mag1cfrog/timeseries-table-format/commit/1ec14d913e43efc17a98c357e75f0c8829ab6ac6))

- Implement schema compatibility checks and error handling ([e466b30](https://github.com/mag1cfrog/timeseries-table-format/commit/e466b30dc005ecd2c1cf00d300b581a81b500ec7))

- Clean up documentation and remove compatibility shims in core module ([4483cf0](https://github.com/mag1cfrog/timeseries-table-format/commit/4483cf017155ff6ac477551426b8d261d3f58a41))

- Add schema compatibility module to metadata layer ([467727e](https://github.com/mag1cfrog/timeseries-table-format/commit/467727e20f989bde04f4f269bc7b6aacae96a7c9))

- Remove operations module and clean up imports in storage ([862aada](https://github.com/mag1cfrog/timeseries-table-format/commit/862aada5052ac5cfb934a0137f4314c031aae54c))

- Bump version of timeseries-table-core to 0.2.0 in Cargo.toml files ([d240346](https://github.com/mag1cfrog/timeseries-table-format/commit/d2403463a40569dc09ae8022cde5410cde429f2f))


### Documentation

- Update module documentation for clarity and detail ([20cd8eb](https://github.com/mag1cfrog/timeseries-table-format/commit/20cd8eb35254b6ad79f3a1e34e667a4f8a022458))

- Clarify semantics and behavior of read_head_tail_4 function in documentation ([1652005](https://github.com/mag1cfrog/timeseries-table-format/commit/165200532b79a58b6f0e3058cdcca0d9ca56d4de))

- Update README to enhance clarity on module responsibilities and append pipeline details ([2f79198](https://github.com/mag1cfrog/timeseries-table-format/commit/2f79198f60d17dd60b7857494b5ab93cf965fb6d))

- Enhance documentation for OutputSink and open_output_sink function ([58da23c](https://github.com/mag1cfrog/timeseries-table-format/commit/58da23c39c24383060c1127e2d79b894f1e60278))

- Update TableLocation documentation for clarity and context ([b318679](https://github.com/mag1cfrog/timeseries-table-format/commit/b318679194389f50c96d38493b8cc85051a00971))

- Update README with module layout and responsibilities ([aee90b6](https://github.com/mag1cfrog/timeseries-table-format/commit/aee90b6fec66b315676209efafeaf834e4550208))

- Enhance module documentation with clearer responsibilities and compatibility shims ([056c7b9](https://github.com/mag1cfrog/timeseries-table-format/commit/056c7b906bca017a694e32234fc029888ea7c6e4))

- Add module skeletons for formats, metadata, and table layers ([b75eb39](https://github.com/mag1cfrog/timeseries-table-format/commit/b75eb39bd77ce6a2e784ed32a1884da734fede7e))

- Remove trailing whitespace in module files ([f945405](https://github.com/mag1cfrog/timeseries-table-format/commit/f945405ba18f119a1ff2b33f9c0456bf53f1b13c))


### Features

- Update serde dependency to include derive feature ([c9df884](https://github.com/mag1cfrog/timeseries-table-format/commit/c9df884ee9a3c7095a0e440b08c66988809b5de2))

- Implement metadata types for JSON serialization in the log module ([d770d80](https://github.com/mag1cfrog/timeseries-table-format/commit/d770d8053797449df2e981d1947ecf24da5bbb44))

- Add JSON serialization tests for Commit and related structures ([86903c0](https://github.com/mag1cfrog/timeseries-table-format/commit/86903c0ee28119ded3d2cc27ab18b239691600ba))

- Enhance segment metadata with file format information ([5535c33](https://github.com/mag1cfrog/timeseries-table-format/commit/5535c33ad116dd24e5cb156fbf70253cb5cf7b0a))

- Add error handling for commit log operations using snafu ([397928a](https://github.com/mag1cfrog/timeseries-table-format/commit/397928a4767e6b0e2e8012ba108786493c90e1db))

- Implement LogStore for managing commit log file paths and structure ([52c4cdf](https://github.com/mag1cfrog/timeseries-table-format/commit/52c4cdf68df0370b1e613b8c637241612b82d5ae))

- Add atomic write functionality for commit log files ([4cdd668](https://github.com/mag1cfrog/timeseries-table-format/commit/4cdd6683667aa25140fe9e36dd477608ec9c929b))

- Refactor filesystem utilities into a dedicated storage module ([4401ee6](https://github.com/mag1cfrog/timeseries-table-format/commit/4401ee6d7ba8c529044b9a92c3ef93d9ccc3e044))

- Update Tokio dependency to version 1.48.0 ([aab0839](https://github.com/mag1cfrog/timeseries-table-format/commit/aab0839d36dcdd30158e5b0b7cc8e72375093266))

- Update Tokio dependency to include fs and io-util features ([611dcf3](https://github.com/mag1cfrog/timeseries-table-format/commit/611dcf325372948ab07b51f25cc32f58a76c0551))

- Implement atomic write functionality for local filesystem paths ([37a3608](https://github.com/mag1cfrog/timeseries-table-format/commit/37a3608ed587bb56612bd96a79438729d48003f4))

- Add asynchronous read_to_string function for local file retrieval ([76cf07e](https://github.com/mag1cfrog/timeseries-table-format/commit/76cf07e5b7e69ea148862413414e300946015dea))

- Add Tokio macros and runtime features to dev-dependencies ([05b3af2](https://github.com/mag1cfrog/timeseries-table-format/commit/05b3af2a77509eec8f09501b1d1cf691f75f17d7))

- Add unit tests for atomic write and read_to_string functions ([51e2854](https://github.com/mag1cfrog/timeseries-table-format/commit/51e28541992a98fb8bec04e3f76e5ec828e217ac))

- Refactor log handling to use TableLocation and improve error management ([e95745d](https://github.com/mag1cfrog/timeseries-table-format/commit/e95745d8c96529d06076c89c6a29f0d6ec24bc59))

- Add AlreadyExists variant to StorageError for path creation conflicts ([bb61c93](https://github.com/mag1cfrog/timeseries-table-format/commit/bb61c93c0ccaacb7794336d4e984e8eaf842c031))

- Add write_new function for atomic file creation with uniqueness constraint ([daed5da](https://github.com/mag1cfrog/timeseries-table-format/commit/daed5da42ffa07fd744baeacd8a563b788f3e9ce))

- Enhance AlreadyExists variant in StorageError with detailed backtrace information ([7fda153](https://github.com/mag1cfrog/timeseries-table-format/commit/7fda1530f4ec7f89b5cf882a72ff93d98cb93133))

- Implement commit_with_expected_version for optimistic concurrency control ([190078b](https://github.com/mag1cfrog/timeseries-table-format/commit/190078b7b80336697f0f6460477bba181219d28f))

- Add tests for write_new function to validate file creation and error handling ([ac3abd0](https://github.com/mag1cfrog/timeseries-table-format/commit/ac3abd0c746b37529d57206a9224dc1bbf765ed2))

- Update commit handling to propagate AlreadyExists error for automatic conflict resolution ([dc125fc](https://github.com/mag1cfrog/timeseries-table-format/commit/dc125fcd0d29dd94390e0d043f05599cf6a73001))

- Enhance commit_with_expected_version documentation for concurrency and crash recovery ([fc5924e](https://github.com/mag1cfrog/timeseries-table-format/commit/fc5924ed185ed4d640db99abaecb2af065b247ab))

- Implement TempFileGuard for automatic cleanup of temporary files during atomic writes ([41a6e31](https://github.com/mag1cfrog/timeseries-table-format/commit/41a6e31735b2659927fd1ea0f651d89955d4ecbd))

- Implement LogStore module for managing commit logs and versioning ([fe56a9c](https://github.com/mag1cfrog/timeseries-table-format/commit/fe56a9c59ff0752eb0cbdb4fbf28ff465755c8f4))

- Add segments module for segment identifiers and metadata management ([78fb13f](https://github.com/mag1cfrog/timeseries-table-format/commit/78fb13f410b93700508901ca57858e1e0d362531))

- Add table metadata structures for log management ([cd8d7fe](https://github.com/mag1cfrog/timeseries-table-format/commit/cd8d7fe0d534c82a206cb30e00461dd949105cde))

- Define LogAction and Commit structures for commit log management ([2058bc2](https://github.com/mag1cfrog/timeseries-table-format/commit/2058bc2cd3cd8b9887ff9b7a228ab41457786056))

- Import SegmentId for enhanced log segment management in tests ([5624c5e](https://github.com/mag1cfrog/timeseries-table-format/commit/5624c5ecfcef527a50d5a124b40d6435ac579fa6))

- Refactor log module to expose actions and segment metadata for improved usability ([36e9395](https://github.com/mag1cfrog/timeseries-table-format/commit/36e93954b4abdde4633ac95ba07b65a2a9921851))

- Add TableState module for in-memory reconstruction of table metadata and live segments ([9fc0a68](https://github.com/mag1cfrog/timeseries-table-format/commit/9fc0a6811ce4044297255d717af690dc13babdcf))

- Add methods for reading log-relative files and loading commits by version ([a50bd04](https://github.com/mag1cfrog/timeseries-table-format/commit/a50bd04372f937f1434cac91fcb0b1a5dc310feb))

- Implement TableState reconstruction by replaying log commits ([5564f52](https://github.com/mag1cfrog/timeseries-table-format/commit/5564f52540b587e50791186702ab7ac25d5d92dc))

- Expose additional table metadata types and include TableState in log module ([30af57c](https://github.com/mag1cfrog/timeseries-table-format/commit/30af57c4ac075215270fa613af910d2d1b54a28b))

- Add unit tests for rebuilding table state with various scenarios ([611784f](https://github.com/mag1cfrog/timeseries-table-format/commit/611784fd73eadcd9d87eb4eed9bfa6abe37a32ab))

- Add integration tests for log-based metadata core functionality ([1c69898](https://github.com/mag1cfrog/timeseries-table-format/commit/1c69898929b70bd78662817d4848afac2a4e96f7))

- Refactor timestamp handling in tests to use a dedicated UTC datetime function ([b5247a2](https://github.com/mag1cfrog/timeseries-table-format/commit/b5247a26563894817b20c364268126f164bca1b7))

- Update timestamp creation in tests to use single() for better error handling ([bedd1cf](https://github.com/mag1cfrog/timeseries-table-format/commit/bedd1cfaaf147f3082e10a968a4abcbf32ee65d8))

- Enhance timestamp handling in integration tests with a dedicated utc_datetime function ([04ea8fe](https://github.com/mag1cfrog/timeseries-table-format/commit/04ea8fe973c557df5be4add0704abf06a6945670))

- Add error handling for segment metadata validation ([c7248ae](https://github.com/mag1cfrog/timeseries-table-format/commit/c7248aeaeaf849ff55ad2cad7772f4859c597cbe))

- Remove invalid extension error from segment metadata validation ([06424a3](https://github.com/mag1cfrog/timeseries-table-format/commit/06424a34032e59695401055955995c3c322f2666))

- Add file head/tail inspection utility for segment validation ([45f3e07](https://github.com/mag1cfrog/timeseries-table-format/commit/45f3e07a1f41ab44fc6c967f684472783a7834ea))

- Implement validated construction for Parquet SegmentMeta with error handling ([5f73c12](https://github.com/mag1cfrog/timeseries-table-format/commit/5f73c121ec836b0e57ad9427ec5773348b78a290))

- Add tests for SegmentMeta validation and error handling ([238d149](https://github.com/mag1cfrog/timeseries-table-format/commit/238d1490e1b580cb47902ee194949d4336a958e7))

- Add read_all_bytes function to read file contents from TableLocation ([5071001](https://github.com/mag1cfrog/timeseries-table-format/commit/5071001520757ac016fbc9d79c44d53586691f6c))

- Introduce BackendError type for improved error handling in storage operations ([9fd920c](https://github.com/mag1cfrog/timeseries-table-format/commit/9fd920c56eac673e0735a2b7cf20e5f7870ae76f))

- Enhance SegmentMetaError with Parquet-specific error handling ([1a7ff74](https://github.com/mag1cfrog/timeseries-table-format/commit/1a7ff74ffaebb484b4e7c9734f81048bc4127a7c))

- Add parquet dependency for enhanced data handling ([e1e9fa3](https://github.com/mag1cfrog/timeseries-table-format/commit/e1e9fa335476342d58f11379b8f6fc3eaca74bdc))

- Add bytes dependency for improved data handling ([52c6215](https://github.com/mag1cfrog/timeseries-table-format/commit/52c6215327dd835c2c11174444887b31b2189638))

- Add ParquetStatsShape error variant for decoding issues in Parquet row group statistics ([455e3cc](https://github.com/mag1cfrog/timeseries-table-format/commit/455e3cccccff575a4756c610f10016f1b9d75adb))

- Enhance SegmentMetaError with additional Parquet statistics handling ([311a761](https://github.com/mag1cfrog/timeseries-table-format/commit/311a761679506a396e1a1ca4c65fa9e660b8a229))

- Add logical field to SegmentMetaError for enhanced error context ([c252a92](https://github.com/mag1cfrog/timeseries-table-format/commit/c252a92c97e7f2c4b055b36480ee68401a56f5f7))

- Enhance SegmentMetaError with additional context and backtrace for improved diagnostics ([2895195](https://github.com/mag1cfrog/timeseries-table-format/commit/289519549c04b71282d3ce9d179e305271c14c5d))

- Introduce BackendError enum for improved error handling in storage operations ([0d259dc](https://github.com/mag1cfrog/timeseries-table-format/commit/0d259dcde052a130948ca5065cd111454776a3ec))

- Remove backtrace field from SegmentMetaError variants for simplified error handling ([43ee058](https://github.com/mag1cfrog/timeseries-table-format/commit/43ee058483a6fc2e42d54678334f4995a7e5f008))

- Implement segment_meta_from_parquet_location for reading Parquet files and extracting SegmentMeta ([f92a1e1](https://github.com/mag1cfrog/timeseries-table-format/commit/f92a1e1a4b38564dcfd1129697e2c174b8846e09))

- Update documentation for Parquet metadata extraction helpers ([2f7939c](https://github.com/mag1cfrog/timeseries-table-format/commit/2f7939cd812a7b45902a827903215891123d300d))

- Add high-level helpers for metadata construction and derivation ([980de1d](https://github.com/mag1cfrog/timeseries-table-format/commit/980de1de0aacb95d016dbae5fba96bd34a285101))

- Expose helpers module for enhanced metadata processing ([1c87882](https://github.com/mag1cfrog/timeseries-table-format/commit/1c87882383cc638e75e14824b1d5a2c152766d29))

- Enhance timestamp unit selection and error handling in Parquet helpers ([1e5ab47](https://github.com/mag1cfrog/timeseries-table-format/commit/1e5ab4717940d094042853c24edcedb701645cee))

- Optimize timestamp conversion for nanos using div_euclid and rem_euclid ([b0fa0ee](https://github.com/mag1cfrog/timeseries-table-format/commit/b0fa0eeafc85c817c6460d475ebacda6e444efb0))

- Update error handling in read_all_bytes to use StorageError::OtherIo ([096fdce](https://github.com/mag1cfrog/timeseries-table-format/commit/096fdce2dfbc8137c674547a4fdf7d5987c50834))

- Enforce missing documentation lints in workspace and core crate ([0eef092](https://github.com/mag1cfrog/timeseries-table-format/commit/0eef0927db3a28e66d92837af62dd83924f3af1b))

- Enhance linting rules and add clippy configuration for improved code quality ([fcdc5b6](https://github.com/mag1cfrog/timeseries-table-format/commit/fcdc5b69a49b52f26a3172a53ee07ec4a9c0878a))

- Add clippy configuration file for timeseries-table-core ([d5b0637](https://github.com/mag1cfrog/timeseries-table-format/commit/d5b0637d3a69e7ae444229812b00e17cdad0d33a))

- Rename table module to time_series_table and implement high-level abstraction ([f33b437](https://github.com/mag1cfrog/timeseries-table-format/commit/f33b43759a77bd1bd2d1cfe054469d7a6cc13b4f))

- Rename LogStore to TransactionLogStore for clarity and consistency ([3aa9cd2](https://github.com/mag1cfrog/timeseries-table-format/commit/3aa9cd26aaf04d1190d17e454e7c5d26dcceacf9))

- Implement TimesSeriesTable struct and open method for managing time-series tables ([fde4fb2](https://github.com/mag1cfrog/timeseries-table-format/commit/fde4fb2586c31b89fa098f2cda6812a0840e5a7b))

- Implement create method for TimesSeriesTable to initialize new time-series tables ([b76d187](https://github.com/mag1cfrog/timeseries-table-format/commit/b76d187c353125d9b952812407f4e9ce8a72e2c5))

- Rename TimesSeriesTable to TimeSeriesTable and add tests for create and open methods ([e44a2ae](https://github.com/mag1cfrog/timeseries-table-format/commit/e44a2aeba29fb295d46729f4c042444def2b0d30))

- Update log integration tests to use TransactionLogStore instead of LogStore ([bdad11d](https://github.com/mag1cfrog/timeseries-table-format/commit/bdad11d1fcaf3e91902c5deb9aebc871eb422024))

- Enhance open method to handle empty tables and improve error reporting ([29c40c1](https://github.com/mag1cfrog/timeseries-table-format/commit/29c40c10b2ea81a062136e29afbab7cd43375fd6))

- Add log_store method to retrieve transaction log store handle ([ff07bcd](https://github.com/mag1cfrog/timeseries-table-format/commit/ff07bcdf8f29ce431eb4c172b3ac025cf7319de1))

- Add schema compatibility helpers to enforce strict schema rules for segments ([d35c87f](https://github.com/mag1cfrog/timeseries-table-format/commit/d35c87f6294b5d086d9b60398d0a359ffa3a3823))

- Add unit tests for schema compatibility checks and update documentation ([8bac697](https://github.com/mag1cfrog/timeseries-table-format/commit/8bac6972d388bc4c009db97aed6e7a5803061108))

- Refactor LogicalSchema to enforce duplicate column validation and update related tests ([e464dc2](https://github.com/mag1cfrog/timeseries-table-format/commit/e464dc2b4b9a5a4ee522690dd86332d5974683c6))

- Add unit tests for schema compatibility edge cases and require_table_schema function ([91b1a7d](https://github.com/mag1cfrog/timeseries-table-format/commit/91b1a7d711fb87b348531ada42633250a8eabd94))

- Integrate LogicalDataType and LogicalTimestampUnit for schema validation and append API ([3c6cd45](https://github.com/mag1cfrog/timeseries-table-format/commit/3c6cd45c2ced0ffb7a63259bb38ac1aeb471b7ee))

- Add Int96 logical data type to LogicalDataType enum ([63bda45](https://github.com/mag1cfrog/timeseries-table-format/commit/63bda452e03fee27974e476adec2c9f48fc4959b))

- Implement logical schema extraction from Parquet files and add mapping for logical data types ([eed396e](https://github.com/mag1cfrog/timeseries-table-format/commit/eed396e9f4d27f44f1c3c0e6ca259c6239d0488e))

- Add LogicalSchema error handling to schema compatibility checks ([101b347](https://github.com/mag1cfrog/timeseries-table-format/commit/101b34762a710a113efd728d1a1826157f6849de))

- Add LogicalSchemaInvalid error variant to SegmentMetaError for Parquet schema validation ([7740b09](https://github.com/mag1cfrog/timeseries-table-format/commit/7740b09ae52a0e4d24e25329e7da3eae0a385d11))

- Enhance logical type mapping to support additional Parquet logical types ([c09a3df](https://github.com/mag1cfrog/timeseries-table-format/commit/c09a3df21a54a60a183f6f1de4227fd35748c176))

- Integrate logical schema handling and segment metadata validation in append API ([65e0cee](https://github.com/mag1cfrog/timeseries-table-format/commit/65e0ceed198d196c4340a041e6ddce0e1380ef4b))

- Extend logical data types and timestamp units in table metadata ([4fd1c2c](https://github.com/mag1cfrog/timeseries-table-format/commit/4fd1c2c48ecb4de80c44cb1a785f25443195cc6f))

- Add detailed error information for invalid logical schema in SegmentMetaError ([c920324](https://github.com/mag1cfrog/timeseries-table-format/commit/c92032431544fb67a4e20c49b5b0f8ab87a4b279))

- Add LogicalSchema error variant for schema validation failures ([2d724e4](https://github.com/mag1cfrog/timeseries-table-format/commit/2d724e49a5ab54fa76bf68e77ca9f4be2ba32c77))

- Add async function to read logical schema from Parquet file location ([3741b20](https://github.com/mag1cfrog/timeseries-table-format/commit/3741b20e6c97a04d03740a8ee9ab59166665f5e4))

- Update logical data types in tests to use strongly-typed variants ([fe0595f](https://github.com/mag1cfrog/timeseries-table-format/commit/fe0595f5b9498dbf0752c4d70eedb14816cf105e))

- Update sample_table_meta to use strongly-typed logical data types ([896e7e3](https://github.com/mag1cfrog/timeseries-table-format/commit/896e7e3b5e373ab3c5ec8dd2edb902bb71070338))

- Update timestamp unit in tests from Nanos to Micros and Millis ([0517b01](https://github.com/mag1cfrog/timeseries-table-format/commit/0517b01e95f3bf507e2dce30d29ea4eb0ec49af4))

- Update data type in logical column test from lowercase to capitalized variant ([0ede9e6](https://github.com/mag1cfrog/timeseries-table-format/commit/0ede9e656fd2af7e67e23b8e6ee1d7a4f51d6c82))

- Add tests for mapping Parquet column types to logical data types ([10444dc](https://github.com/mag1cfrog/timeseries-table-format/commit/10444dcd2a1c21dcf8cd3d3fed1d9b5f9098de0e))

- Implement append API for Parquet segment integration with state updates and schema adoption ([bbbb2ae](https://github.com/mag1cfrog/timeseries-table-format/commit/bbbb2ae7f36e37b32b79f9eb7e6009422d86f82c))

- Update LogicalColumn data_type to use strongly-typed enum for better type safety ([15a5217](https://github.com/mag1cfrog/timeseries-table-format/commit/15a5217028e9ee874a6dfb253b9d78e71872a4cf))

- Update timestamp unit in sample_table_meta from Nanos to Micros for consistency ([056526a](https://github.com/mag1cfrog/timeseries-table-format/commit/056526ad683bc6c63ce1769136fac88ed565bc82))

- Update timestamp unit in tests from Nanos to Micros for consistency ([a902901](https://github.com/mag1cfrog/timeseries-table-format/commit/a902901f4fd40a4be16494b8e0ad49c87ae00489))

- Add arrow dependency to Cargo.toml ([157b219](https://github.com/mag1cfrog/timeseries-table-format/commit/157b219b51d2f17f249792b463da9463de1a21bb))

- Enhance TableError enum with additional error variants for improved error handling ([246e5f2](https://github.com/mag1cfrog/timeseries-table-format/commit/246e5f2bec4acfaab5ab12a51d86c78d6f196632))

- Add futures dependency to Cargo.toml for asynchronous support ([94fe09c](https://github.com/mag1cfrog/timeseries-table-format/commit/94fe09c2254b88cfbe9a705876e43869d0857931))

- Add TimeSeriesScan type for streaming Arrow RecordBatch values ([7227304](https://github.com/mag1cfrog/timeseries-table-format/commit/7227304e84e6523499eabf3e98e6659d88c81c82))

- Implement range scanning for time-series data with Arrow RecordBatch support ([ad1b8e2](https://github.com/mag1cfrog/timeseries-table-format/commit/ad1b8e2b1ee2d4b55b43afbe367513954f660693))

- Add arrow-select dependency for enhanced Arrow functionality ([be29889](https://github.com/mag1cfrog/timeseries-table-format/commit/be29889324fbcfc50a27f14f52a170a674167716))

- Implement filtering for time-series data in Arrow RecordBatch ([524cbd1](https://github.com/mag1cfrog/timeseries-table-format/commit/524cbd1ec36f18acace59d3aa6c7d6362f593947))

- Refactor time-series table code for improved readability and structure ([ce3fe24](https://github.com/mag1cfrog/timeseries-table-format/commit/ce3fe24badb279e65c6863847397076a1f67a24a))

- Enhance scan range functionality with support for various timestamp units and error handling ([ff13d7e](https://github.com/mag1cfrog/timeseries-table-format/commit/ff13d7edd9d60e271a83696558f4ab4e66e7727d))

- Enhance timestamp filtering logic with vectorized comparisons and improved null handling ([b95b56a](https://github.com/mag1cfrog/timeseries-table-format/commit/b95b56ac1e164b6f610afbf6d8f922c73db48f35))

- Enhance timestamp scalar creation with timezone compatibility for range filtering ([a290066](https://github.com/mag1cfrog/timeseries-table-format/commit/a29006647135eb9615ea0847f88df8b78de3b14e))

- Add missing dependency for arrow-array in Cargo.toml ([b4e5617](https://github.com/mag1cfrog/timeseries-table-format/commit/b4e561712ae1854d1fe497ecd9f827cafd65d2cd))

- Enhance timestamp filtering with timezone-aware scalar comparisons ([3d178d4](https://github.com/mag1cfrog/timeseries-table-format/commit/3d178d451409f9967d829c259eb725258f9f925c))

- Add missing_runs method to group missing buckets into contiguous runs ([39b17fc](https://github.com/mag1cfrog/timeseries-table-format/commit/39b17fcab54c2a592aa97c075ad85598ec43c9e0))

- Add methods for last contiguous run, coverage ratio, and maximum gap length in coverage module ([15c725a](https://github.com/mag1cfrog/timeseries-table-format/commit/15c725aa56aae0719c86ed9ea9df31355d40bae2))

- Add time_bucket module for timestamp mapping to discrete bucket ids ([5192e0f](https://github.com/mag1cfrog/timeseries-table-format/commit/5192e0f8687aa09c70181b1c6821e27b2561104c))

- Add bucket_range function for inclusive bucket id range retrieval ([9db00a8](https://github.com/mag1cfrog/timeseries-table-format/commit/9db00a8e7cb0605d3c19b3c83a613f62bc71ad92))

- Add expected_buckets_for_range function for bitmap generation from bucket range ([c454e38](https://github.com/mag1cfrog/timeseries-table-format/commit/c454e38468081b99416664a2c02274020e80afda))

- Add convenience constructors for TableMeta to support time-series table creation ([719b0d2](https://github.com/mag1cfrog/timeseries-table-format/commit/719b0d2986f9e389b08d67cceec57040926d9cfa))

- Make TableMeta fields private and add accessor methods ([435468a](https://github.com/mag1cfrog/timeseries-table-format/commit/435468a5f1a15f6d1bb138d538f49dcfb1717ee5))

- Refactor sample_table_meta to use new constructor and update logical schema access ([8073092](https://github.com/mag1cfrog/timeseries-table-format/commit/8073092b74a9f025a4e742c4c0aa2a464f6f2347))

- Add debug assertion for bucket ID range in expected_buckets_for_range function ([1df9812](https://github.com/mag1cfrog/timeseries-table-format/commit/1df981277dd51d899b20cf9e51fe1e814deac1e3))

- Add clarification comment for format version in update_table_meta_last_one_wins test ([0459c6f](https://github.com/mag1cfrog/timeseries-table-format/commit/0459c6faca41240e1f56767bcd0e0b9315848796))

- Add debug assertion for pre-epoch timestamps in bucket_id function ([2157f5f](https://github.com/mag1cfrog/timeseries-table-format/commit/2157f5f18eb2f936055aa57a33688e8704c49ab2))

- Add layout module to core crate ([f88c5fe](https://github.com/mag1cfrog/timeseries-table-format/commit/f88c5fee3872b846afa405b8ce6534eda5355c20))

- Add initial coverage module ([537189e](https://github.com/mag1cfrog/timeseries-table-format/commit/537189e6c19ce2fadfe43ceba828c3206724c18f))

- Enhance coverage module documentation and add layout constants ([e644d4c](https://github.com/mag1cfrog/timeseries-table-format/commit/e644d4c234a8592edc48cc657f4bb40ecb088fcb))

- Add coverage ID validation and path generation functions ([f113aa3](https://github.com/mag1cfrog/timeseries-table-format/commit/f113aa32737cc4098d8b5fa51eeea5f693c4d69f))

- Add unit tests for coverage ID validation and path generation functions ([3c57435](https://github.com/mag1cfrog/timeseries-table-format/commit/3c57435832ca411ab73bdd648cd83ea79f9fb65a))

- Add serialization and deserialization for coverage bitmaps ([53d3f3d](https://github.com/mag1cfrog/timeseries-table-format/commit/53d3f3da94166341bf986d2f7b1406c245dbc153))

- Add unit tests for coverage serialization and deserialization ([c58af4f](https://github.com/mag1cfrog/timeseries-table-format/commit/c58af4f2d83f1a610ef60525270aa717bb0ada81))

- Implement coverage sidecar file management with atomic and exclusive write functions ([cabc3f1](https://github.com/mag1cfrog/timeseries-table-format/commit/cabc3f1e044f4f87d112da69b548f667fae59382))

- Update segment and table snapshot path functions to use PathBuf and validate coverage IDs ([949e786](https://github.com/mag1cfrog/timeseries-table-format/commit/949e786befd7ac8c90041dcca77f68425bf42bae))

- Update table snapshot path function to include snapshot ID in the file name ([6cdbe84](https://github.com/mag1cfrog/timeseries-table-format/commit/6cdbe84cbf49e2c6d85d302f265c3d99c92d591f))

- Add unit tests for coverage sidecar write functions ([8f7ad25](https://github.com/mag1cfrog/timeseries-table-format/commit/8f7ad251089df14d219dd6287d271e5450cbf9dc))

- Add read function for coverage sidecar with error handling ([19c2b80](https://github.com/mag1cfrog/timeseries-table-format/commit/19c2b808e9ffcecc5a3204fe5184931bf2333adc))

- Enhance coverage ID validation to require at least one alphanumeric character and reject leading dots ([1607eee](https://github.com/mag1cfrog/timeseries-table-format/commit/1607eee7e2023692223ea20c128b34799eafa29b))

- Add coverage_path field to segment metadata and related test cases ([037213f](https://github.com/mag1cfrog/timeseries-table-format/commit/037213f1c27c0417a08e816407a9b1fde1a4c6da))

- Add coverage_path parameter to segment_meta_from_parquet_location function ([128fba1](https://github.com/mag1cfrog/timeseries-table-format/commit/128fba138caa19cfe5820f814050edc8ca8da3dd))

- Add method to set coverage sidecar path in segment metadata ([e16157a](https://github.com/mag1cfrog/timeseries-table-format/commit/e16157a9ab371c88ddbe7968bfa65f0063204ee4))

- Initialize coverage_path field in sample_segment and add_segment_with_same_id_replaces tests ([06d9ec8](https://github.com/mag1cfrog/timeseries-table-format/commit/06d9ec8f6594bcba3254575a89c880014a648027))

- Add UpdateTableCoverage action and TableCoveragePointer struct for coverage metadata ([96d4adf](https://github.com/mag1cfrog/timeseries-table-format/commit/96d4adf9c90d8531e3d5c682648bd761418f625e))

- Add tests for SegmentMeta JSON serialization with coverage_path ([804a307](https://github.com/mag1cfrog/timeseries-table-format/commit/804a3071a708f01a9311036bd7d0db3ea475b74e))

- Add tests for UpdateTableCoverage actions and their impact on TableState ([6b23b65](https://github.com/mag1cfrog/timeseries-table-format/commit/6b23b65fc0c4e10c8765db34974ef88ba9f786c5))

- Update documentation for UpdateTableCoverage to clarify bucket_spec usage ([8d2c78b](https://github.com/mag1cfrog/timeseries-table-format/commit/8d2c78b12bd20546edd6305374a7e3b5dea5126e))

- Add test for multiple UpdateTableCoverage commits to verify last one wins ([83c91ca](https://github.com/mag1cfrog/timeseries-table-format/commit/83c91ca3299da3b317c828cd412984b17111f5e0))

- Improve documentation for coverage sidecar pointer and refactor test setup ([3a941fa](https://github.com/mag1cfrog/timeseries-table-format/commit/3a941fa457d2b0e5c9f6671bc4e85a582fb3d295))

- Add segment coverage error handling for Parquet file processing ([208e6e7](https://github.com/mag1cfrog/timeseries-table-format/commit/208e6e7ea73ada9e5050d85df10c95952bda48c6))

- Add common error types and utilities for timestamp column validation ([199ebc7](https://github.com/mag1cfrog/timeseries-table-format/commit/199ebc7545b2cf8febefc2d796cdaaa93d5ee7e6))

- Enhance segment coverage error handling with time column validation ([62d7ed4](https://github.com/mag1cfrog/timeseries-table-format/commit/62d7ed4e701f6f492c92b149f12aab99d5ba5df8))

- Refactor time column error handling to use unified TimeColumnError type ([2cb10f0](https://github.com/mag1cfrog/timeseries-table-format/commit/2cb10f02cc59f6607ec7c225acd6ab62b496634d))

- Update segment coverage error handling for invalid timestamps ([7401bec](https://github.com/mag1cfrog/timeseries-table-format/commit/7401bec1a64a15b40fc32edf0ce578c175e09d21))

- Implement segment coverage computation with timestamp validation and error handling ([3f4093c](https://github.com/mag1cfrog/timeseries-table-format/commit/3f4093c961af4d444e98e038a10c4587d531b312))

- Add bucket_id_from_epoch_secs function for mapping epoch seconds to bucket ids ([68aac67](https://github.com/mag1cfrog/timeseries-table-format/commit/68aac67f51b74a6e5a737d907819328da218d4c5))

- Refactor timestamp handling in segment coverage computation to use bucket_id_from_epoch_secs ([1325d14](https://github.com/mag1cfrog/timeseries-table-format/commit/1325d1442b3894f150d10763b05eb4ea0c098cb3))

- Add unit tests for compute_segment_coverage function with timestamp handling ([cefc8c9](https://github.com/mag1cfrog/timeseries-table-format/commit/cefc8c9d171ad3cb66000b64e8e4871e4c76d0d1))

- Enhance segment coverage tests with error handling for missing and unsupported time columns ([b1db422](https://github.com/mag1cfrog/timeseries-table-format/commit/b1db4222acbfc0e3225e1636fc8ebf8778564ad3))

- Add tests for bucket_id_from_epoch_secs to validate clamping and boundary behavior ([08702c5](https://github.com/mag1cfrog/timeseries-table-format/commit/08702c546c7d7e47f572f9c0d9e5c18a8e18c6f8))

- Streamline error handling in segment coverage tests for missing and unsupported columns ([69de0e9](https://github.com/mag1cfrog/timeseries-table-format/commit/69de0e963143008424826373109a1c417f6564f5))

- Simplify timestamp processing in compute_segment_coverage function ([33600f6](https://github.com/mag1cfrog/timeseries-table-format/commit/33600f683b045dfdccc027147ec2bfcb8fa5a319))

- Enhance CoverageError with backtrace for storage errors ([cecc253](https://github.com/mag1cfrog/timeseries-table-format/commit/cecc2537643089338a6d97a8941f9e6e6612bd81))

- Enhance SegmentCoverageError with backtrace for improved error context ([795b9fc](https://github.com/mag1cfrog/timeseries-table-format/commit/795b9fcb5325a5f2e5f3ade1d547e24dcbc3b2ad))

- Implement Display trait for SegmentId for improved formatting ([880c954](https://github.com/mag1cfrog/timeseries-table-format/commit/880c9545ff082e2ecc6e547bf60193c8ba1bbf8a))

- Add coverage-related error variants to TableError for improved error handling ([a455e5f](https://github.com/mag1cfrog/timeseries-table-format/commit/a455e5f1b38a096d950de775f1632fc166013229))

- Enhance TableError with new coverage-related variants and implement coverage validation functions ([8294aad](https://github.com/mag1cfrog/timeseries-table-format/commit/8294aad8452d764c5ab95dbca42d634ce3cdd2aa))

- Add deterministic coverage ID generation for segment sidecars ([1fab0ea](https://github.com/mag1cfrog/timeseries-table-format/commit/1fab0ea9fb6cb750667fcb175c698c7f99d644e4))

- Implement fast path for appending segments with coverage handling ([e7bd6d8](https://github.com/mag1cfrog/timeseries-table-format/commit/e7bd6d8bbae0dd8e17af681f240fc1351317b479))

- Add exclusive creation function for writing coverage bitmap bytes to sidecar ([39d0713](https://github.com/mag1cfrog/timeseries-table-format/commit/39d0713f6c651376af0585834decc5477d144ca9))

- Implement segment coverage ID generation with domain separation for TimeBucket ([0b11106](https://github.com/mag1cfrog/timeseries-table-format/commit/0b111063ba3851cfc99800286156696cdc4fda8d))

- Update coverage handling to use new serialization and segment coverage ID generation ([f7e45df](https://github.com/mag1cfrog/timeseries-table-format/commit/f7e45df6be0c7e945206eb2e1ca8e4736243605e))

- Add tests for deterministic and variable segment coverage ID generation ([af2f09d](https://github.com/mag1cfrog/timeseries-table-format/commit/af2f09d18d0bafe79b931806b8c3caa6ddd33ed2))

- Update coverage handling to use table coverage ID generation ([fe802a5](https://github.com/mag1cfrog/timeseries-table-format/commit/fe802a52b394ca0838368b2b2ff79e3b232caa7e))

- Add deterministic table coverage ID generation with domain separation ([756708e](https://github.com/mag1cfrog/timeseries-table-format/commit/756708e78d7e3bfa83238da8bba56e9c5f306f23))

- Update coverage sidecar writing to handle existing storage gracefully ([7d60923](https://github.com/mag1cfrog/timeseries-table-format/commit/7d609234aba659d43a73dcc9160abad82bc74c1d))

- Rename append_parquet_segment to append_parquet_segment_with_id and update documentation for coverage handling ([8387c68](https://github.com/mag1cfrog/timeseries-table-format/commit/8387c6847a11c7d27c42a9c5d351014342fff99d))

- Enhance append_parquet_segment documentation to include segment_id and coverage handling ([abfa53e](https://github.com/mag1cfrog/timeseries-table-format/commit/abfa53e498ffa8a42d132053fa3330af4439d263))

- Add segment_meta_from_parquet_bytes function for in-memory Parquet processing ([2f3678b](https://github.com/mag1cfrog/timeseries-table-format/commit/2f3678b7e3b43f3f7cea88f475016b515bcd2d08))

- Add logical_schema_from_parquet_bytes function for in-memory Parquet processing ([7ccd7a6](https://github.com/mag1cfrog/timeseries-table-format/commit/7ccd7a69f2be113abc64761aaa7218e2a39a0761))

- Add compute_segment_coverage_from_parquet_bytes function for in-memory Parquet processing ([a80bdd1](https://github.com/mag1cfrog/timeseries-table-format/commit/a80bdd15aea493b03698a5d8163614ccd53e09de))

- Implement fast path for appending Parquet segments with byte data ([823f8b2](https://github.com/mag1cfrog/timeseries-table-format/commit/823f8b25e53e8408c98dc46782e8dbe8583a6981))

- Add deterministic segment ID generation and append method for Parquet segments ([5a4fe67](https://github.com/mag1cfrog/timeseries-table-format/commit/5a4fe671bbb28dd7248b60b11b52a4a8df8c37eb))

- Enhance documentation and add tests for appending Parquet segments ([cbd3673](https://github.com/mag1cfrog/timeseries-table-format/commit/cbd3673441dffea96fb7eb10054d73e3dd6fc0bd))

- Add test for rebuilding table coverage with segment coverage paths ([559fe66](https://github.com/mag1cfrog/timeseries-table-format/commit/559fe66426c7fcefcb1855420c119b44a5eacaac))

- Add tests for handling append failures due to missing coverage paths and snapshot pointers ([ef9f185](https://github.com/mag1cfrog/timeseries-table-format/commit/ef9f1858c14f6c0213013ca2028723b70264b425))

- Update documentation for append entry points and coverage handling ([a324d0e](https://github.com/mag1cfrog/timeseries-table-format/commit/a324d0e5ed52725211f6673455f0594d92db9f20))

- Update test timestamps for segment scanning and range queries ([807f7ca](https://github.com/mag1cfrog/timeseries-table-format/commit/807f7ca613e7581b8d941ab204cab0a87243287d))

- Refactor coverage ID generation for segments and tables to improve code reuse ([9b726e6](https://github.com/mag1cfrog/timeseries-table-format/commit/9b726e614143dfb2df2316119455055782b14336))

- Implement deterministic segment ID generation for append entries ([40396f3](https://github.com/mag1cfrog/timeseries-table-format/commit/40396f3eae88a4eb125694bad1cf7a24254b0ba8))

- Add error handling documentation for write_coverage_sidecar_new_bytes function ([1069242](https://github.com/mag1cfrog/timeseries-table-format/commit/1069242cada17369ee9b92babb3907fb22fc5082))

- Add centralized error handling for time-series table operations ([48d68e4](https://github.com/mag1cfrog/timeseries-table-format/commit/48d68e48372995860ced6389ca0d86f21490b1ab))

- Implement append pipeline for TimeSeriesTable with coverage checks and segment ID derivation ([ab72936](https://github.com/mag1cfrog/timeseries-table-format/commit/ab72936b90eb989bb0d689bc68b0b540b40efafc))

- Add error variant for segment coverage sidecar read failures during recovery ([b6add4c](https://github.com/mag1cfrog/timeseries-table-format/commit/b6add4cdb97583987c7299f9c28955f9296002df))

- Add in-place union method for merging coverage bitmaps ([ff0d8e2](https://github.com/mag1cfrog/timeseries-table-format/commit/ff0d8e2b9c4987af1a60823e3871e4e798743f32))

- Implement recovery mechanism for table coverage from segments ([d683ed9](https://github.com/mag1cfrog/timeseries-table-format/commit/d683ed9bd8736782cd24895d69faee8c451cc42e))

- Add recovery tests for loading table snapshot coverage from segments ([9f0e8e1](https://github.com/mag1cfrog/timeseries-table-format/commit/9f0e8e11b082a742c6efcc53d6007ee6fd4777cc))

- Remove unused MissingTableCoveragePointer error variant from TableError ([4dffab2](https://github.com/mag1cfrog/timeseries-table-format/commit/4dffab2a52b6a1b63452bf5f0d0eaa5bdba62575))

- Add logging for recovery attempts when loading table snapshot coverage ([f2c2bc4](https://github.com/mag1cfrog/timeseries-table-format/commit/f2c2bc426ab42ccfe73613b27dc80fe1bf1f5dd7))

- Add coverage pipeline tests for create, open, and append operations ([9c8a637](https://github.com/mag1cfrog/timeseries-table-format/commit/9c8a637cbbf019131d6066495d76f4e6a57afd08))

- Add README documentation for timeseries-table-core architecture and usage ([bba49d7](https://github.com/mag1cfrog/timeseries-table-format/commit/bba49d746370d7f542e46c5d0b4f9b963b6516b4))

- Add expected_buckets_for_range_checked function and BucketDomainOverflow error variant ([f4adbbe](https://github.com/mag1cfrog/timeseries-table-format/commit/f4adbbe7f28c05701b0840f7481b7ca231429ab2))

- Add coverage state helpers and enhance error handling for missing snapshot pointer ([d862403](https://github.com/mag1cfrog/timeseries-table-format/commit/d862403c631e20bc5adadb80d42392b8e53ca006))

- Remove unused expected_buckets_for_range_checked function and related error handling ([f63b7cf](https://github.com/mag1cfrog/timeseries-table-format/commit/f63b7cffb103299094768eab010e8dc8671a7093))

- Add last_window_at_or_before method to Coverage for contiguous bucket analysis ([5919310](https://github.com/mag1cfrog/timeseries-table-format/commit/59193105e1111834999465df9883581e3dad72d0))

- Update table state to reflect modified snapshot without coverage_path ([9a936c7](https://github.com/mag1cfrog/timeseries-table-format/commit/9a936c7e4530239b46577e081cd9e4be6d2a76f1))

- Add coverage query APIs for TimeSeriesTable ([3adf01e](https://github.com/mag1cfrog/timeseries-table-format/commit/3adf01e76865b6e92b328446c8a3e669947d5d0f))

- Update expected bitmap range to include last bucket and add comprehensive tests ([b1fc31c](https://github.com/mag1cfrog/timeseries-table-format/commit/b1fc31c19daeb06147582ab4f15ad470343c2f12))

- Enhance error handling by adding BucketDomainOverflowSnafu and improve code formatting ([a63ede8](https://github.com/mag1cfrog/timeseries-table-format/commit/a63ede886505d1e2a22372c4dd2f8f441c0207f7))

- Add test for bucket domain overflow in last_fully_covered_window ([32301eb](https://github.com/mag1cfrog/timeseries-table-format/commit/32301eb4d04388c53bf9f97002380eb908e0a882))

- Add end-to-end tests for coverage queries in TimeSeriesTable ([e6d6f9f](https://github.com/mag1cfrog/timeseries-table-format/commit/e6d6f9f1d4a46546856c2a449940f6012b9ae7a1))

- Improve timestamp conversion error handling and enhance code readability ([264c67b](https://github.com/mag1cfrog/timeseries-table-format/commit/264c67bb2372f87d8bb4000b79cb696f5e3d8cb3))

- Enhance documentation for max_gap_len_for_range and last_fully_covered_window with error handling and examples ([0e8121c](https://github.com/mag1cfrog/timeseries-table-format/commit/0e8121c6c0e8168c02e262dc0cb8bd805a5e6aad))

- Clarify comment in coverage_queries_work_end_to_end test for better understanding of bucket coverage ([e9b10dd](https://github.com/mag1cfrog/timeseries-table-format/commit/e9b10dd552ad6a3ba89e74d378023d90dc61fefc))

- Add error handling for invalid range in end_bucket_for_half_open_end function ([ce8939f](https://github.com/mag1cfrog/timeseries-table-format/commit/ce8939fd4214946cdeb9790c08a9ba09930da51f))

- Add bucket domain overflow check and enhance coverage_ratio_for_range documentation ([e2cef54](https://github.com/mag1cfrog/timeseries-table-format/commit/e2cef543224f7474e2e89a706915c1db837333f5))

- Add entity identity field to TableMeta for single entity tracking ([49a3382](https://github.com/mag1cfrog/timeseries-table-format/commit/49a3382888dd7d7ca0a83f4f38c97a3d4ac8b7dc))

- Add entity identity field to multiple metadata structures for consistency ([5ba23ab](https://github.com/mag1cfrog/timeseries-table-format/commit/5ba23ab906bc60b596f3f019c4298d5006d947c3))

- Add segment_entity_identity module for entity identity metadata handling ([b762071](https://github.com/mag1cfrog/timeseries-table-format/commit/b762071b056838970a071072b0fcbb9c96faaee2))

- Implement segment entity identity error handling for metadata extraction ([1afc37e](https://github.com/mag1cfrog/timeseries-table-format/commit/1afc37ec7f761ceec62ad3c755eb26a9696d810a))

- Enhance entity identity handling with additional error checks and extraction logic ([768b16e](https://github.com/mag1cfrog/timeseries-table-format/commit/768b16eae77ee29306fa2098f8608e6b91b342cc))

- Add fast-path entity identity extraction from Parquet metadata ([a9617d7](https://github.com/mag1cfrog/timeseries-table-format/commit/a9617d7b97553112bfd48a346dbf6996a6c2cd47))

- Add unit tests for segment entity identity extraction from Parquet bytes ([fc0151a](https://github.com/mag1cfrog/timeseries-table-format/commit/fc0151a438a7362c3f13816ae66145b33364a16b))

- Add segment entity identity error handling to TableError enum ([daa1b14](https://github.com/mag1cfrog/timeseries-table-format/commit/daa1b144c20991f73c0165f5892845c52eebaed8))

- Implement entity identity enforcement for single-entity-per-table in append flow ([6315589](https://github.com/mag1cfrog/timeseries-table-format/commit/6315589f57d0717bca1b0c9e322b05391bf4b3a8))

- Add unit tests for JSON serialization of TableMeta with entity identity ([69cfb9c](https://github.com/mag1cfrog/timeseries-table-format/commit/69cfb9c123406722040ff4aff2710364440bc298))

- Update tests to use consistent entity identity for symbol "A" ([e08ff31](https://github.com/mag1cfrog/timeseries-table-format/commit/e08ff31deec61f9321167b08074475c5d831c4a1))

- Update coverage pipeline tests to use consistent entity identity for all rows ([023f15b](https://github.com/mag1cfrog/timeseries-table-format/commit/023f15b76521223abc246c15d239db451be471fe))

- Add segment_entity_identity module for entity identity extraction and validation ([3e111db](https://github.com/mag1cfrog/timeseries-table-format/commit/3e111db11ad5b14c48454a3bc8ab23f8da4870ed))

- Update dependencies in Cargo.toml to use workspace references ([59535e9](https://github.com/mag1cfrog/timeseries-table-format/commit/59535e916bbc7d2c2f3fe98d5b854c4d1854b003))

- Add segment ordering helpers for deterministic comparison of SegmentMeta ([cdffb0d](https://github.com/mag1cfrog/timeseries-table-format/commit/cdffb0d4c62364360b7d4bdf8267b7d98ddf059f))

- Add method to return live segments sorted deterministically by time ([42052d7](https://github.com/mag1cfrog/timeseries-table-format/commit/42052d7a8585055687d5cb482524976980b89598))

- Use segment ordering helper for chronological sorting of candidates ([1bde6c8](https://github.com/mag1cfrog/timeseries-table-format/commit/1bde6c883ff5448239a36b0d03c541e29e8c3de3))

- Add methods to load current log version and refresh in-memory state ([048acfc](https://github.com/mag1cfrog/timeseries-table-format/commit/048acfc831307ab949bb098e5467ff2963d7b535))

- Add integration test for loading latest state in TimeSeriesTable ([71b97c0](https://github.com/mag1cfrog/timeseries-table-format/commit/71b97c06371ba1420c4e136094cf8a23e66634ce))

- Refactor TimeSeriesTable to use log location instead of storing it directly ([1f8a9d1](https://github.com/mag1cfrog/timeseries-table-format/commit/1f8a9d1e40d8e6f2756733cc8a1b196492387131))

- Add method to retrieve TableLocation from TransactionLogStore ([576f607](https://github.com/mag1cfrog/timeseries-table-format/commit/576f6076de152356dfcdf13872992e737c715f56))

- Update location retrieval to use method call instead of direct access ([1c0caff](https://github.com/mag1cfrog/timeseries-table-format/commit/1c0caffb335ffbb68399f2083b2518cfc0ede37d))

- Add refresh tests for TimeSeriesTable to verify state updates and version changes ([f129bcf](https://github.com/mag1cfrog/timeseries-table-format/commit/f129bcf9eb6f5f9ea0300e05a35f65baf149ae86))

- Add deterministic sorting test for segments by time in TableState ([fdca27c](https://github.com/mag1cfrog/timeseries-table-format/commit/fdca27cc2001b362d11d5aa8f9a7acb5d0c4892c))

- Enhance latest snapshot tests for TimeSeriesTable to verify segment retrieval and current state ([7aecd96](https://github.com/mag1cfrog/timeseries-table-format/commit/7aecd96ed5740912af6471f2544affacf32252c5))

- Enhance sorting test for segments by time in TableState to ensure deterministic order ([97869ab](https://github.com/mag1cfrog/timeseries-table-format/commit/97869ab9ff2013fc2d80f55815cf046b247efdd5))

- Extend FixedBinary variant to include byte width in LogicalDataType ([bf59be5](https://github.com/mag1cfrog/timeseries-table-format/commit/bf59be55b1040e09d33c92f54b5aa15c51120fbd))

- Enhance logical type mapping to include byte width for FixedBinary ([17a62ac](https://github.com/mag1cfrog/timeseries-table-format/commit/17a62aca6b160a37da1cfeaa98ed7d08cd71e60f))

- Add tests for logical schema mapping of various data types in Parquet ([f9fdf72](https://github.com/mag1cfrog/timeseries-table-format/commit/f9fdf72ac5478ecef36a446a94f9809cc9e06ea2))

- Add Decimal logical data type to schema metadata with precision and scale ([80ecfd5](https://github.com/mag1cfrog/timeseries-table-format/commit/80ecfd599d206c7cf08fb4b3aca036598129d2ce))

- Improve mapping of Decimal logical type to return precise scale and precision ([7275cc1](https://github.com/mag1cfrog/timeseries-table-format/commit/7275cc1f03255cf657e70a9fbd0c04fd4ba39de2))

- Add error handling for logical schema conversion with detailed diagnostics ([f8b0eda](https://github.com/mag1cfrog/timeseries-table-format/commit/f8b0eda29f98309ef24048620015b730b366e649))

- Add conversion method for LogicalTimestampUnit to Arrow TimeUnit ([7639957](https://github.com/mag1cfrog/timeseries-table-format/commit/76399571987d645c89d69d2a8c18f126be669b2e))

- Implement LogicalDataType to Arrow DataType conversion with validation for Decimal types ([a7d5be0](https://github.com/mag1cfrog/timeseries-table-format/commit/a7d5be0b5f59d392db44c47864aaf995dee2d500))

- Add conversion methods for LogicalSchema to Arrow Schema and SchemaRef ([9cb2a52](https://github.com/mag1cfrog/timeseries-table-format/commit/9cb2a52a8cb48eeb60c6269e326b8d5fa1d37cfc))

- Add error handling for logical schema conversion with new TableMetaSchemaError type ([c91fe27](https://github.com/mag1cfrog/timeseries-table-format/commit/c91fe27448a7d392b64c4ee5ee573b526d01519d))

- Add comprehensive tests for LogicalSchema to Arrow Schema conversion ([bd1e91f](https://github.com/mag1cfrog/timeseries-table-format/commit/bd1e91f464bd7988564ab73f2715711a0edd8c08))

- Enhance logical type mapping with error handling for fixed-length byte arrays ([f7a530a](https://github.com/mag1cfrog/timeseries-table-format/commit/f7a530adbee5ee679997444cceeeb1c034169797))

- Add validation errors for FixedBinary columns in LogicalSchema ([5c0be3c](https://github.com/mag1cfrog/timeseries-table-format/commit/5c0be3cc64772869105e9fc1f06327be45a85f2b))

- Add validation tests for decimal fields in LogicalSchema to Arrow Schema conversion ([c5b522f](https://github.com/mag1cfrog/timeseries-table-format/commit/c5b522fc46362ac2eb18c500f3538ca47d1cfe18))

- Add LogicalField and support for Struct, List, and Map types in LogicalDataType ([d79d7e0](https://github.com/mag1cfrog/timeseries-table-format/commit/d79d7e0ac5d48900d02d67604d250079676ce9e0))

- Replace LogicalColumn with LogicalField across the codebase ([14be730](https://github.com/mag1cfrog/timeseries-table-format/commit/14be73049b7965b8ada49fd6ded682e01e4eda14))

- Enforce non-nullable map keys during Arrow schema conversion ([1af354f](https://github.com/mag1cfrog/timeseries-table-format/commit/1af354fcd3c590f77513128091d7f351781cb685))

- Rename key_sorted to keys_sorted in LogicalDataType for clarity ([5660a80](https://github.com/mag1cfrog/timeseries-table-format/commit/5660a80750c2211ee60603647364ccfee206576b))

- Add validation for LogicalField types and enforce non-nullable map keys ([258691d](https://github.com/mag1cfrog/timeseries-table-format/commit/258691deb9991f8dd47145869135a3e139c2b44d))

- Make value field in LogicalDataType optional and update related logic ([a27e6c8](https://github.com/mag1cfrog/timeseries-table-format/commit/a27e6c8f708f4add66558a3dd8c2ca7f95906b60))

- Clarify Map variant semantics in LogicalDataType to support Parquet keys-only representation ([19a9c6f](https://github.com/mag1cfrog/timeseries-table-format/commit/19a9c6f32922cd0c42fbf792277e8454e67773db))

- Add validation for non-empty struct and list element field names in LogicalSchema ([8d6fd1c](https://github.com/mag1cfrog/timeseries-table-format/commit/8d6fd1c70ac69c6fbca5f21a0269adce25ad6fcf))

- Add LogicalSchema, LogicalField, and LogicalDataType definitions with validation for table metadata ([e9f7319](https://github.com/mag1cfrog/timeseries-table-format/commit/e9f7319b2c048d0e5b90acf6fa40b53030d3568d))

- Reorganize imports to use logical_schema module for consistency ([5824bf1](https://github.com/mag1cfrog/timeseries-table-format/commit/5824bf19d4fd49a7d69ba94de1018b0590c3b142))

- Add comprehensive tests for LogicalSchema and TableMeta validation ([99bc2dc](https://github.com/mag1cfrog/timeseries-table-format/commit/99bc2dcd0ac85bb1ef6b21075c9a9ee31e3625fc))

- Add error variants for unsupported Parquet LIST and MAP encodings ([b36d1b9](https://github.com/mag1cfrog/timeseries-table-format/commit/b36d1b9d34d97e1331c32f83771dcfd8c05d27c1))

- Enhance logical schema parsing for Parquet LIST and MAP types ([8a0edaf](https://github.com/mag1cfrog/timeseries-table-format/commit/8a0edaf39b82869e76e20616e8b381771ca8ff10))

- Require explicit value for nullable field in logical column ([8733755](https://github.com/mag1cfrog/timeseries-table-format/commit/87337556070a79d8cf261083d27ca90bc0d432c8))

- Enhance logical schema validation for Parquet LIST and MAP types ([c6401fe](https://github.com/mag1cfrog/timeseries-table-format/commit/c6401fe89d888406ad0dc50c6641b0c57fec2a5b))

- Remove module map section from README for clarity ([a754e67](https://github.com/mag1cfrog/timeseries-table-format/commit/a754e67f03100d9a072a103a29d96b6f38761823))

- Enhance error messages for unsupported Parquet LIST and MAP encodings with type information ([48b5270](https://github.com/mag1cfrog/timeseries-table-format/commit/48b527029bbe6b3ae2c3abcd9e61278ded6459fa))

- Preserve semantics for LIST/MAP types in logical schema conversion and add tests for nested lists ([1e0e802](https://github.com/mag1cfrog/timeseries-table-format/commit/1e0e802ec85fc5bfbdf0f41c334a220fa3266cd4))

- Add test to ensure map entries field is non-nullable in logical schema ([811ff9e](https://github.com/mag1cfrog/timeseries-table-format/commit/811ff9eaaa9cfd20ca967f77242439e94f71b64f))

- Add tests to ensure logical schema preserves LIST and MAP types ([6b50e75](https://github.com/mag1cfrog/timeseries-table-format/commit/6b50e75ddce4e5c933188e08ffb9504d444420e6))

- Add tests for logical schema handling of struct with LIST and MAP fields ([99a2950](https://github.com/mag1cfrog/timeseries-table-format/commit/99a295043d28c64c0c9f2e7ecd715b5bcd3e0b9b))

- Add tests for nested LIST and MAP structures and schema validation errors ([feb74ca](https://github.com/mag1cfrog/timeseries-table-format/commit/feb74ca77c0ee2c0c190f8168a8bd7679f9b6a70))

- Update tokio dependency to use workspace configuration across all crates ([d207bd1](https://github.com/mag1cfrog/timeseries-table-format/commit/d207bd1d6ba2231783ad33b6125492cf123ecbcd))

- Add optional file size field to SegmentMeta and update related tests ([c646ea4](https://github.com/mag1cfrog/timeseries-table-format/commit/c646ea4f1f406a8f1f6e923bf2a8d464ad35560f))

- Add file size assertions in SegmentMeta tests ([35d281a](https://github.com/mag1cfrog/timeseries-table-format/commit/35d281a52e2128a47abdcbb63fc35596852a02e5))

- Add snafu dependency with workspace configuration ([53b3f41](https://github.com/mag1cfrog/timeseries-table-format/commit/53b3f41612e8a8e7f3f1a5c9eb3cea0d5716f648))

- Re-export TableError from time_series_table module ([f19b5aa](https://github.com/mag1cfrog/timeseries-table-format/commit/f19b5aa469060e5bac4daeb169ea0004ca7d2f57))

- Add ParseTimeBucketError for human-friendly time bucket spec parsing ([cd2e6d3](https://github.com/mag1cfrog/timeseries-table-format/commit/cd2e6d3e81c58b76f293a6a5910b13fad0ed3641))

- Enhance TimeBucket parsing with improved error handling and comprehensive tests ([db95fe1](https://github.com/mag1cfrog/timeseries-table-format/commit/db95fe112305b4d7aff392384b7bfe8772d76ef3))

- Re-export ParseTimeBucketError for improved accessibility in the core library ([38b9f7b](https://github.com/mag1cfrog/timeseries-table-format/commit/38b9f7b20512969ac9ed3d3ebf7b2311dccd8697))

- Add ensure_parquet_under_root method to TableLocation for managing parquet file paths ([ede0a76](https://github.com/mag1cfrog/timeseries-table-format/commit/ede0a76d61a7c2d0b8416ce05db1631724e0c4fd))

- Extend error handling in map_storage_error to include AlreadyExistsNoSource variant ([3862d25](https://github.com/mag1cfrog/timeseries-table-format/commit/3862d251725c197506989f30a69e1e8c542eb2d7))

- Enhance TableLocation with parsing functionality and improve error handling for parquet file operations ([5b68d64](https://github.com/mag1cfrog/timeseries-table-format/commit/5b68d6461dd0dcc5a327118b4ea915243606e54f))

- Enhance error handling in map_storage_error to include AlreadyExists and AlreadyExistsNoSource variants ([355fa65](https://github.com/mag1cfrog/timeseries-table-format/commit/355fa655e588d83b5f0cb48b9b828136556daec6))

- Improve error handling in TableLocation for path resolution and metadata checks ([094f3b3](https://github.com/mag1cfrog/timeseries-table-format/commit/094f3b3ddc6a19917b1dcf3f506bb7c77c09110e))

- Add tests for TableLocation parsing and ensure_parquet_under_root functionality ([595ffeb](https://github.com/mag1cfrog/timeseries-table-format/commit/595ffeb3cef16808d9687f1e81ec46bee66b3610))

- Update futures dependency to use workspace configuration ([cd28d2a](https://github.com/mag1cfrog/timeseries-table-format/commit/cd28d2a38d826ea5bc30a56ea9e8e615ab987aea))

- Introduce error handling module for storage backend with detailed error variants ([f2a3ed4](https://github.com/mag1cfrog/timeseries-table-format/commit/f2a3ed4d1cda0af3160f4e4961539dc3ab1d1d46))

- Implement storage operations module with atomic file handling and directory management ([b015eeb](https://github.com/mag1cfrog/timeseries-table-format/commit/b015eebba5a0a8f6c4ddfeb2cd7cc796d22ceb58))

- Refactor storage module to introduce StorageLocation and TableLocation abstractions ([4635f40](https://github.com/mag1cfrog/timeseries-table-format/commit/4635f40792962a97837a4b5b5a3ab159b08e2899))

- Add documentation for TableLocation struct and its methods ([ca4d110](https://github.com/mag1cfrog/timeseries-table-format/commit/ca4d1102047cd3d591249dd30defcf9d4cf15f5b))

- Add output module with LocalSink and OutputSink for file writing ([86d125b](https://github.com/mag1cfrog/timeseries-table-format/commit/86d125bddddd14bd40af2ece5acb571882647cf8))

- Add OutputLocation struct and parse method for output target specification ([2b0477c](https://github.com/mag1cfrog/timeseries-table-format/commit/2b0477c522905125ad3ad0280116239708c801b1))

- Add file_size function for retrieving file size in local storage ([1a693e8](https://github.com/mag1cfrog/timeseries-table-format/commit/1a693e8f8a92410b42d0ea7be65b90603e993e1e))

- Add from_state method to construct TimeSeriesTable from existing snapshot ([7f99266](https://github.com/mag1cfrog/timeseries-table-format/commit/7f99266206d578502bb7daa4fc77833dd196b712))

- Add test-counters feature to timeseries-table-core ([86806de](https://github.com/mag1cfrog/timeseries-table-format/commit/86806de3bc719aa4b01b8ee8c31e09ceaa878464))

- Add test counters for rebuild_table_state in TableState ([b35f3b3](https://github.com/mag1cfrog/timeseries-table-format/commit/b35f3b3e4fd65db0813edc23c92ba1629e7034da))

- Replace thread-local counter with atomic counter for rebuild tracking ([7e824d0](https://github.com/mag1cfrog/timeseries-table-format/commit/7e824d01f290d84ce6dd23fe06849fb041ea64e0))

- Add coverage benchmark tool for parquet file performance analysis ([31f90ce](https://github.com/mag1cfrog/timeseries-table-format/commit/31f90cec8085ce370d730393c3a97e7555b17c21))

- Improve parquet scan performance through optimized bitmap computation ([19def8f](https://github.com/mag1cfrog/timeseries-table-format/commit/19def8f24096674ae047f15f019fc999a60fecba))

- Enhance coverage benchmark with batch size and threading options ([f41e57e](https://github.com/mag1cfrog/timeseries-table-format/commit/f41e57e349d09254f6d137f519e6c7fc831b25d8))

- Enhance CSV output format with additional parameters for benchmarking ([2f81fe6](https://github.com/mag1cfrog/timeseries-table-format/commit/2f81fe63697cf7769a06093765ddab262c392b00))

- Add option to print parquet row group metadata ([ee264b1](https://github.com/mag1cfrog/timeseries-table-format/commit/ee264b1ad3c26521e197befbe370bbc96e26a3a2))

- Enhance coverage benchmark with configurable thread settings and automatic chunking ([1c78e42](https://github.com/mag1cfrog/timeseries-table-format/commit/1c78e42447dd1a348ffa540f7a659da18d151517))

- Improve Parquet scan performance with parallel processing and optimized metadata handling ([856afc5](https://github.com/mag1cfrog/timeseries-table-format/commit/856afc5e367b3b4d78ca64dcc13695bf7b9aba12))

- Optimize row group chunk calculation for improved performance ([8262d76](https://github.com/mag1cfrog/timeseries-table-format/commit/8262d767cd7ccb539431b3ab0c792113ecab09d2))

- Improve Parquet segment coverage computation with optimized row group handling ([fdd9e7f](https://github.com/mag1cfrog/timeseries-table-format/commit/fdd9e7fda8ee777dabafe00a33c9e3c6f1cd02a1))

- Fix row group chunk calculation logic in resolve_rg_settings function ([2473762](https://github.com/mag1cfrog/timeseries-table-format/commit/2473762cb7f6c395c1b230e2cdfd6b8955f80b1c))

- Add profiling support for append operations with detailed reports ([fb1adec](https://github.com/mag1cfrog/timeseries-table-format/commit/fb1adeca0d7535b00d77865210d5af80010e356d))

- Enhance segment meta benchmarking with new strategies and CSV output ([2305566](https://github.com/mag1cfrog/timeseries-table-format/commit/2305566de26c1e58bcc38d6bb3b71b1150ffeb43))

- Implement parallel row-group scanning for min/max timestamp extraction ([95559d9](https://github.com/mag1cfrog/timeseries-table-format/commit/95559d98ef579fc3cad4dd365f5dfcb61317d48c))

- Enhance error handling for Arrow read failures in segment metadata ([4239687](https://github.com/mag1cfrog/timeseries-table-format/commit/4239687c46d693c32d6035fb0fd3caa30876f5ba))

- Add shared row-group parallelism helpers for Parquet scans ([0e86acc](https://github.com/mag1cfrog/timeseries-table-format/commit/0e86acc2ce49a0563bd66b66314dc23dd3bc6004))

- Reorder imports for clarity and maintainability in segment coverage helpers ([b3959a6](https://github.com/mag1cfrog/timeseries-table-format/commit/b3959a6ec856de9a11dd9192ac7dd6309fe14073))

- Add example for ingesting NVDA 1h sample data into timeseries-table-format ([fe1a5d6](https://github.com/mag1cfrog/timeseries-table-format/commit/fe1a5d640bde787873f3d825334610d3454a8996))

- Add quickstart example for NVDA 1h data ingestion and moving average query ([3e8e38f](https://github.com/mag1cfrog/timeseries-table-format/commit/3e8e38fb66c2dab0e087bc5081d59d7d829a2ec6))

- Add logical schema definitions and validation for table metadata ([ef00a15](https://github.com/mag1cfrog/timeseries-table-format/commit/ef00a15aabde0e09ad55b4e2be9131601ee79148))

- Implement segment metadata structure and validation errors ([ce5dd09](https://github.com/mag1cfrog/timeseries-table-format/commit/ce5dd093382f26371196cde510c7be807f8d27e0))

- Add table metadata structures and time index specification ([0869e0b](https://github.com/mag1cfrog/timeseries-table-format/commit/0869e0b7e6e71a738dfcd16fc10d609b5e7bf2eb))

- Add error handling for timestamp column validation in schemas ([ff5c6f9](https://github.com/mag1cfrog/timeseries-table-format/commit/ff5c6f9d715018255be08c6d96b70418da28b431))


### Testing

- Improve test name for overflow handling in read_segment_range ([7249ea0](https://github.com/mag1cfrog/timeseries-table-format/commit/7249ea068045dd1d8aa664ff353a79939035dfcd))

- Add unit tests for Coverage struct methods and bitmap conversions ([9628ebb](https://github.com/mag1cfrog/timeseries-table-format/commit/9628ebb4aaa8dbc05ff3918982da0d483122d042))

- Add unit tests for bucket_id and bucket_range functions ([f1293dd](https://github.com/mag1cfrog/timeseries-table-format/commit/f1293dddf9d666a99d67ecce846818d6e1ca1616))

- Add unit tests for expected_buckets_for_range function ([89b4a41](https://github.com/mag1cfrog/timeseries-table-format/commit/89b4a415a725287da980bef43f54a2a86a6f6d6b))

- Update logical schema test to expect error for invalid FixedBinary width ([1b07381](https://github.com/mag1cfrog/timeseries-table-format/commit/1b07381f2a7963aeb3d5c3127946a577f09d54f6))


### Doc

- Add doc comment for macro for filtering RecordBatch by timestamp with timezone support ([c158a9a](https://github.com/mag1cfrog/timeseries-table-format/commit/c158a9a839fc9847106c9864a86c0156423d1cf6))


### Style

- Format imports for improved readability ([c13c4a0](https://github.com/mag1cfrog/timeseries-table-format/commit/c13c4a0088f1a8f80a00822f3fcbeb3f6c1e6fec))

- Improve formatting and readability in segment entity identity tests ([2a43313](https://github.com/mag1cfrog/timeseries-table-format/commit/2a43313457b50281ee3b19b56511608979782b1b))

