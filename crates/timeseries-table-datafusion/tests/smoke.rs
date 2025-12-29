//! Smoke tests for the timeseries table DataFusion provider.

#[test]
fn can_construct_provider() {
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;
    use timeseries_table_datafusion::TsTableProvider;

    let schema = Arc::new(Schema::new(vec![Field::new("ts", DataType::Int64, false)]));

    let _provider = TsTableProvider::new(schema);
}
