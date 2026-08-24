use super::*;

#[test]
fn external_error_mapping_preserves_scan_hierarchy() {
    use crate::{
        storage::StorageLocation,
        table::{ScanError, TableError},
    };

    let storage = StorageLocation::parse("").expect_err("empty location must fail");
    let error = df_external(TableError::Scan {
        source: ScanError::Storage {
            path: "data/missing.parquet".to_string(),
            source: Box::new(storage),
        },
    });

    let DataFusionError::External(source) = error else {
        panic!("table error must remain external");
    };
    assert!(matches!(
        source.downcast_ref::<TableError>(),
        Some(TableError::Scan {
            source: ScanError::Storage { path, .. }
        }) if path == "data/missing.parquet"
    ));
}

#[test]
fn metadata_pruning_accepts_only_exact_supported_entity_literals() -> DFResult<()> {
    use std::num::NonZeroU64;

    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::prelude::{col, lit};

    let schema = Arc::new(Schema::new(vec![
        Field::new("idx", DataType::Int64, false),
        Field::new("text", DataType::Utf8, false),
        Field::new("signed_32", DataType::Int32, false),
        Field::new("signed_64", DataType::Int64, false),
        Field::new("unsigned_64", DataType::UInt64, false),
    ]));
    let index = crate::metadata::index::IndexSpec {
        column: "idx".to_string(),
        entity_columns: vec![
            "text".to_string(),
            "signed_32".to_string(),
            "signed_64".to_string(),
            "unsigned_64".to_string(),
        ],
        kind: crate::metadata::index::IndexKind::Int64 {
            index_granularity: NonZeroU64::MIN,
        },
    };

    let exact = [
        col("text").eq(lit("device")),
        lit("device").eq(col("text")),
        col("signed_32").eq(lit(-1_i32)),
        lit(-1_i32).eq(col("signed_32")),
        col("signed_64").eq(lit(i64::MIN)),
        lit(i64::MAX).eq(col("signed_64")),
        col("unsigned_64").eq(lit(u64::MAX)),
        lit(u64::MAX).eq(col("unsigned_64")),
    ];
    for predicate in exact {
        assert_eq!(
            metadata_pruning_expr(&predicate, &index, &schema)?,
            Some(predicate)
        );
    }

    let mismatched = col("signed_32").eq(lit(-1_i64));
    assert!(metadata_pruning_expr(&mismatched, &index, &schema)?.is_none());
    let null = col("signed_32").eq(Expr::Literal(ScalarValue::Int32(None), None));
    assert!(metadata_pruning_expr(&null, &index, &schema)?.is_none());
    Ok(())
}

fn make_table_meta() -> crate::metadata::table::TableMeta {
    use crate::metadata::index::{IndexKind, IndexSpec, TimeIndexGranularity};
    use crate::metadata::logical_schema::{
        LogicalDataType, LogicalField, LogicalSchema, LogicalTimestampUnit,
    };

    let index = IndexSpec {
        column: "ts".to_string(),
        entity_columns: vec!["symbol".to_string()],
        kind: IndexKind::Timestamp {
            index_granularity: TimeIndexGranularity::Minutes(1),
            timezone: None,
        },
    };

    let logical_schema = LogicalSchema::new(vec![
        LogicalField {
            name: "ts".to_string(),
            data_type: LogicalDataType::Timestamp {
                unit: LogicalTimestampUnit::Millis,
                timezone: None,
            },
            nullable: false,
        },
        LogicalField {
            name: "symbol".to_string(),
            data_type: LogicalDataType::Utf8,
            nullable: false,
        },
        LogicalField {
            name: "price".to_string(),
            data_type: LogicalDataType::Float64,
            nullable: false,
        },
    ])
    .expect("valid logical schema");

    crate::metadata::table::TableMeta::new_time_series_with_schema(index, logical_schema)
}

#[tokio::test]
async fn scan_records_safe_segment_planning_counts() -> crate::table::test_util::TestResult {
    use datafusion::{
        catalog::TableProvider,
        prelude::{SessionContext, col, lit},
    };

    use crate::{
        storage::TableLocation,
        table::{
            TimeSeriesTable,
            test_util::{TestRow, TraceCapture, append_parquet_fixture, write_test_parquet},
        },
    };

    let tmp = tempfile::TempDir::new()?;
    let mut table =
        TimeSeriesTable::create(TableLocation::local(tmp.path()), make_table_meta()).await?;
    for (path, symbol) in [("data/a.parquet", "A"), ("data/b.parquet", "B")] {
        write_test_parquet(
            &tmp.path().join(path),
            true,
            false,
            &[TestRow {
                ts_millis: 0,
                symbol,
                price: 1.0,
            }],
        )?;
        append_parquet_fixture(&mut table, path).await?;
    }

    let snapshot_version = table.state().version;
    let provider = TsTableProvider::try_new(Arc::new(table))?;
    let state = SessionContext::new().state();
    let projection = vec![0, 2];
    let filters = vec![col("symbol").eq(lit("A"))];
    let capture = TraceCapture::default();

    capture
        .run(provider.scan(&state, Some(&projection), &filters, Some(1)))
        .await?;

    let spans: Vec<_> = capture
        .spans()
        .into_iter()
        .filter(|span| span.name == "table.scan.plan")
        .collect();
    assert_eq!(spans.len(), 1, "expected one table.scan.plan span");
    assert_eq!(spans[0].level, tracing::Level::DEBUG);
    for (field, expected) in [
        ("snapshot_version", snapshot_version.to_string()),
        ("total_candidate_segments", "2".to_string()),
        ("selected_segments", "1".to_string()),
        ("pruned_segments", "1".to_string()),
        ("filter_count", "1".to_string()),
        ("projection_column_count", "2".to_string()),
        ("limit", "1".to_string()),
    ] {
        assert_eq!(spans[0].fields.get(field), Some(&expected));
    }
    assert!(
        capture
            .events()
            .iter()
            .all(|event| event.name != "table.scan.plan")
    );
    for value in spans[0].fields.values() {
        for forbidden in ["symbol", "A", "BinaryExpr", "RecordBatch", "LogicalSchema"] {
            assert!(!value.contains(forbidden));
        }
        assert!(!value.contains(&tmp.path().display().to_string()));
    }
    Ok(())
}

#[cfg(feature = "test-counters")]
#[tokio::test(flavor = "current_thread")]
async fn provider_cache_is_primed_from_snapshot() -> DFResult<()> {
    use crate::{
        storage::TableLocation,
        table::TimeSeriesTable,
        transaction_log::table_state::{
            rebuild_table_state_count, reset_rebuild_table_state_count,
        },
    };

    let tmp = tempfile::TempDir::new().expect("tempdir");
    let location = TableLocation::local(tmp.path());
    TimeSeriesTable::create(location.clone(), make_table_meta())
        .await
        .expect("create");

    let table = TimeSeriesTable::open(location).await.expect("open");

    reset_rebuild_table_state_count();
    let provider = TsTableProvider::try_new(Arc::new(table))?;

    let _state = provider.latest_state().await?;
    assert_eq!(rebuild_table_state_count(), 0);

    Ok(())
}

#[tokio::test]
async fn provider_scan_refresh_allows_unknown_writer_features() -> DFResult<()> {
    use datafusion::{catalog::TableProvider, prelude::SessionContext};

    use crate::{
        storage::TableLocation,
        table::TimeSeriesTable,
        transaction_log::{LogAction, TransactionLogStore},
    };

    let tmp = tempfile::TempDir::new().expect("tempdir");
    let location = TableLocation::local(tmp.path());
    let table = TimeSeriesTable::create(location.clone(), make_table_meta())
        .await
        .expect("create");
    let mut updated_meta = table.state().table_meta.clone();
    updated_meta
        .required_writer_features
        .insert("future_writer".to_string());
    let provider = TsTableProvider::try_new(Arc::new(table))?;
    TransactionLogStore::new(location)
        .commit_with_expected_version(1, vec![LogAction::UpdateTableMeta(updated_meta)])
        .await
        .map_err(df_external)?;

    let session = SessionContext::new().state();
    provider.scan(&session, None, &[], None).await?;

    let cache = provider.cache.read().await;
    assert_eq!(cache.version, Some(2));
    assert_eq!(
        cache
            .state
            .as_ref()
            .expect("refreshed state")
            .table_meta
            .required_writer_features(),
        &["future_writer".to_string()].into_iter().collect()
    );
    Ok(())
}
