use super::*;

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
    let index = crate::metadata::table_metadata::IndexSpec {
        column: "idx".to_string(),
        entity_columns: vec![
            "text".to_string(),
            "signed_32".to_string(),
            "signed_64".to_string(),
            "unsigned_64".to_string(),
        ],
        kind: crate::metadata::table_metadata::IndexKind::Int64 {
            bucket_width: NonZeroU64::MIN,
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

#[cfg(feature = "test-counters")]
fn make_table_meta() -> crate::metadata::table_metadata::TableMeta {
    use crate::metadata::logical_schema::{
        LogicalDataType, LogicalField, LogicalSchema, LogicalTimestampUnit,
    };
    use crate::metadata::table_metadata::{IndexKind, IndexSpec, TimeBucket};

    let index = IndexSpec {
        column: "ts".to_string(),
        entity_columns: vec!["symbol".to_string()],
        kind: IndexKind::Timestamp {
            bucket: TimeBucket::Minutes(1),
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

    crate::metadata::table_metadata::TableMeta::new_time_series_with_schema(index, logical_schema)
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
