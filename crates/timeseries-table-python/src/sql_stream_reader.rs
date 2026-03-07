use arrow_array::{RecordBatch, RecordBatchReader};
use datafusion::{
    arrow::{datatypes::SchemaRef, error::ArrowError},
    execution::SendableRecordBatchStream,
};
use futures_util::StreamExt;
use tokio::{runtime::Runtime, sync::mpsc, task::JoinHandle};

pub(crate) struct SqlStreamRecordBatchReader {
    schema: SchemaRef,
    rx: mpsc::Receiver<Result<RecordBatch, ArrowError>>,
    producer_task: Option<JoinHandle<()>>,
    finished: bool,
}

impl SqlStreamRecordBatchReader {
    /// Spawn a producer task that drains the async stream into a bounded channel.
    pub(crate) fn spawn(
        rt: &Runtime,
        schema: SchemaRef,
        mut stream: SendableRecordBatchStream,
    ) -> Self {
        let (tx, rx) = mpsc::channel(1);

        let producer_task = rt.spawn(async move {
            while let Some(item) = stream.next().await {
                let next_batch = item.map_err(ArrowError::from);
                if tx.send(next_batch).await.is_err() {
                    return;
                }
            }
        });

        Self {
            schema,
            rx,
            producer_task: Some(producer_task),
            finished: false,
        }
    }
}

impl Iterator for SqlStreamRecordBatchReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        match self.rx.blocking_recv() {
            Some(Ok(batch)) => Some(Ok(batch)),
            Some(Err(err)) => {
                self.finished = true;
                Some(Err(err))
            }
            None => {
                self.finished = true;
                None
            }
        }
    }
}

impl RecordBatchReader for SqlStreamRecordBatchReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Drop for SqlStreamRecordBatchReader {
    fn drop(&mut self) {
        if let Some(handle) = self.producer_task.take() {
            handle.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::Arc,
        time::{Duration, Instant},
    };

    use arrow_array::{ArrayRef, Int32Array, RecordBatch};
    use datafusion::{
        arrow::datatypes::{DataType, Field, Schema, SchemaRef},
        error::DataFusionError,
        execution::SendableRecordBatchStream,
        physical_plan::stream::RecordBatchStreamAdapter,
    };
    use futures_util::stream;

    use crate::sql_stream_reader::SqlStreamRecordBatchReader;

    fn make_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]))
    }

    fn make_batch(
        schema: &SchemaRef,
        values: &[i32],
    ) -> Result<RecordBatch, Box<dyn std::error::Error>> {
        let array: ArrayRef = Arc::new(Int32Array::from(values.to_vec()));
        Ok(RecordBatch::try_new(schema.clone(), vec![array])?)
    }

    fn batch_values(batch: &RecordBatch) -> Result<Vec<i32>, Box<dyn std::error::Error>> {
        let Some(array) = batch.column(0).as_any().downcast_ref::<Int32Array>() else {
            return Err(std::io::Error::other("expected Int32Array").into());
        };
        Ok(array.values().to_vec())
    }

    fn wait_until(timeout: Duration, predicate: impl Fn() -> bool) -> bool {
        let start = Instant::now();
        while start.elapsed() < timeout {
            if predicate() {
                return true;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        predicate()
    }

    #[test]
    fn yields_all_batches_in_order() -> Result<(), Box<dyn std::error::Error>> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;

        let schema = make_schema();
        let batch1 = make_batch(&schema, &[1, 2])?;
        let batch2 = make_batch(&schema, &[3, 4])?;

        let source = stream::iter(vec![Ok(batch1.clone()), Ok(batch2.clone())]);
        let stream: SendableRecordBatchStream =
            Box::pin(RecordBatchStreamAdapter::new(schema.clone(), source));

        let mut reader = SqlStreamRecordBatchReader::spawn(&rt, schema.clone(), stream);

        let first = match reader.next() {
            Some(Ok(batch)) => batch,
            Some(Err(err)) => return Err(Box::new(err)),
            None => return Err(std::io::Error::other("expected first batch").into()),
        };
        let second = match reader.next() {
            Some(Ok(batch)) => batch,
            Some(Err(err)) => return Err(Box::new(err)),
            None => return Err(std::io::Error::other("expected second batch").into()),
        };

        assert_eq!(batch_values(&first)?, vec![1, 2]);
        assert_eq!(batch_values(&second)?, vec![3, 4]);
        assert!(reader.next().is_none());
        assert!(reader.next().is_none());

        Ok(())
    }
}
