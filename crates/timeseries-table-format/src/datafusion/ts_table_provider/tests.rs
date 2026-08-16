#[cfg(feature = "test-counters")]
use super::*;

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
