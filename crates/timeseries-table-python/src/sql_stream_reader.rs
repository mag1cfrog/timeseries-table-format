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
