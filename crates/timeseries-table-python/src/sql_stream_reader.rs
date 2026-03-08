use arrow_array::{RecordBatch, RecordBatchReader};
use datafusion::{
    arrow::{datatypes::SchemaRef, error::ArrowError},
    execution::SendableRecordBatchStream,
};
use futures_util::StreamExt;
use tokio::{
    runtime::{Handle, Runtime, RuntimeFlavor},
    sync::mpsc,
    task::JoinHandle,
};

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
        mut stream: SendableRecordBatchStream,
    ) -> Result<Self, ArrowError> {
        match rt.handle().runtime_flavor() {
            RuntimeFlavor::MultiThread => {}
            RuntimeFlavor::CurrentThread => {
                return Err(ArrowError::ExternalError(Box::new(std::io::Error::other(
                    "SqlStreamRecordBatchReader requires a Tokio multi-thread runtime",
                ))));
            }
            _ => {
                return Err(ArrowError::ExternalError(Box::new(std::io::Error::other(
                    "unsupported Tokio runtime flavor for SqlStreamRecordBatchReader",
                ))));
            }
        }

        let schema = stream.schema();
        let (tx, rx) = mpsc::channel(1);

        let producer_task = rt.spawn(async move {
            while let Some(item) = stream.next().await {
                match item {
                    Ok(batch) => {
                        if tx.send(Ok(batch)).await.is_err() {
                            return;
                        }
                    }

                    Err(err) => {
                        let _ = tx.send(Err(ArrowError::from(err))).await;
                        return;
                    }
                }
            }
        });

        Ok(Self {
            schema,
            rx,
            producer_task: Some(producer_task),
            finished: false,
        })
    }

    fn recv_next(&mut self) -> Option<Result<RecordBatch, ArrowError>> {
        match Handle::try_current() {
            Ok(handle) => match handle.runtime_flavor() {
                RuntimeFlavor::MultiThread => {
                    let rx = &mut self.rx;
                    tokio::task::block_in_place(|| handle.block_on(rx.recv()))
                }
                RuntimeFlavor::CurrentThread => Some(Err(ArrowError::ExternalError(Box::new(
                    std::io::Error::other(
                        "SqlStreamRecordBatchReader cannot block inside a Tokio current-thread runtime",
                    ),
                )))),
                _ => Some(Err(ArrowError::ExternalError(Box::new(
                    std::io::Error::other(
                        "unsupported Tokio runtime flavor for SqlStreamRecordBatchReader",
                    ),
                )))),
            },
            Err(_) => self.rx.blocking_recv(),
        }
    }

    fn abort_producer(&mut self) {
        if let Some(handle) = self.producer_task.take() {
            handle.abort();
        }
    }
}

impl Iterator for SqlStreamRecordBatchReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        match self.recv_next() {
            Some(Ok(batch)) => Some(Ok(batch)),
            Some(Err(err)) => {
                self.finished = true;
                self.abort_producer();
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
        self.abort_producer();
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        pin::Pin,
        sync::atomic::{AtomicBool, AtomicUsize, Ordering},
        sync::{Arc, mpsc as std_mpsc},
        task::{Context, Poll},
        time::{Duration, Instant},
    };

    use arrow_array::{ArrayRef, Int32Array, RecordBatch};
    use datafusion::{
        arrow::datatypes::{DataType, Field, Schema, SchemaRef},
        common::Result as DFResult,
        error::DataFusionError,
        physical_plan::{
            RecordBatchStream, SendableRecordBatchStream, stream::RecordBatchStreamAdapter,
        },
    };
    use futures_util::{Stream, stream};

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

    struct ScriptedStream {
        schema: SchemaRef,
        items: VecDeque<DFResult<RecordBatch>>,
        poll_count: Arc<AtomicUsize>,
        poll_signal: Option<std_mpsc::Sender<usize>>,
        dropped: Option<Arc<AtomicBool>>,
    }

    impl ScriptedStream {
        fn new(
            schema: SchemaRef,
            items: VecDeque<DFResult<RecordBatch>>,
            poll_count: Arc<AtomicUsize>,
        ) -> Self {
            Self {
                schema,
                items,
                poll_count,
                poll_signal: None,
                dropped: None,
            }
        }

        fn with_poll_signal(mut self, poll_signal: std_mpsc::Sender<usize>) -> Self {
            self.poll_signal = Some(poll_signal);
            self
        }

        fn with_dropped_flag(mut self, dropped: Arc<AtomicBool>) -> Self {
            self.dropped = Some(dropped);
            self
        }
    }

    impl Stream for ScriptedStream {
        type Item = DFResult<RecordBatch>;

        fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            let poll_index = self.poll_count.fetch_add(1, Ordering::SeqCst) + 1;
            if let Some(signal) = &self.poll_signal {
                let _ = signal.send(poll_index);
            }
            Poll::Ready(self.items.pop_front())
        }
    }

    impl RecordBatchStream for ScriptedStream {
        fn schema(&self) -> SchemaRef {
            self.schema.clone()
        }
    }

    impl Drop for ScriptedStream {
        fn drop(&mut self) {
            if let Some(dropped) = &self.dropped {
                dropped.store(true, Ordering::SeqCst);
            }
        }
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

        let mut reader = SqlStreamRecordBatchReader::spawn(&rt, stream)?;

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

    #[test]
    fn propagates_midstream_error() -> Result<(), Box<dyn std::error::Error>> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;

        let schema = make_schema();
        let batch = make_batch(&schema, &[1, 2])?;

        let source = stream::iter(vec![
            Ok(batch),
            Err(DataFusionError::Execution("boom".to_string())),
        ]);

        let stream: SendableRecordBatchStream =
            Box::pin(RecordBatchStreamAdapter::new(schema.clone(), source));

        let mut reader = SqlStreamRecordBatchReader::spawn(&rt, stream)?;

        match reader.next() {
            Some(Ok(_)) => {}
            Some(Err(err)) => return Err(Box::new(err)),
            None => return Err(std::io::Error::other("expected first batch").into()),
        }

        let err = match reader.next() {
            Some(Err(err)) => err,
            Some(Ok(_)) => {
                return Err(std::io::Error::other("expected stream error, got batch").into());
            }
            None => return Err(std::io::Error::other("expected stream error").into()),
        };

        assert!(err.to_string().contains("boom"));
        assert!(reader.next().is_none());

        Ok(())
    }

    #[test]
    fn producer_stops_polling_after_first_error() -> Result<(), Box<dyn std::error::Error>> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;

        let schema = make_schema();
        let batch1 = make_batch(&schema, &[1])?;
        let batch2 = make_batch(&schema, &[2])?;
        let poll_count = Arc::new(AtomicUsize::new(0));

        let stream = Box::pin(ScriptedStream::new(
            schema.clone(),
            VecDeque::from([
                Ok(batch1),
                Err(DataFusionError::Execution("boom".to_string())),
                Ok(batch2),
            ]),
            poll_count.clone(),
        )) as SendableRecordBatchStream;

        let mut reader = SqlStreamRecordBatchReader::spawn(&rt, stream)?;

        match reader.next() {
            Some(Ok(_)) => {}
            Some(Err(err)) => return Err(Box::new(err)),
            None => return Err(std::io::Error::other("expected first batch").into()),
        }

        match reader.next() {
            Some(Err(err)) => assert!(err.to_string().contains("boom")),
            Some(Ok(_)) => {
                return Err(std::io::Error::other("expected terminal stream error").into());
            }
            None => return Err(std::io::Error::other("expected stream error").into()),
        }

        assert!(wait_until(Duration::from_millis(100), || {
            poll_count.load(Ordering::SeqCst) >= 2
        }));
        std::thread::sleep(Duration::from_millis(20));
        assert_eq!(poll_count.load(Ordering::SeqCst), 2);
        assert!(reader.next().is_none());

        Ok(())
    }

    #[test]
    fn drop_aborts_producer_task() -> Result<(), Box<dyn std::error::Error>> {
        struct PendingStream {
            schema: SchemaRef,
            dropped: Arc<AtomicBool>,
        }

        impl Stream for PendingStream {
            type Item = DFResult<RecordBatch>;

            fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
                Poll::Pending
            }
        }

        impl RecordBatchStream for PendingStream {
            fn schema(&self) -> SchemaRef {
                self.schema.clone()
            }
        }

        impl Drop for PendingStream {
            fn drop(&mut self) {
                self.dropped.store(true, Ordering::SeqCst);
            }
        }

        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;

        let schema = make_schema();
        let dropped = Arc::new(AtomicBool::new(false));

        let stream = Box::pin(PendingStream {
            schema: schema.clone(),
            dropped: dropped.clone(),
        }) as SendableRecordBatchStream;

        let reader = SqlStreamRecordBatchReader::spawn(&rt, stream)?;
        drop(reader);

        assert!(wait_until(Duration::from_secs(1), || dropped.load(Ordering::SeqCst)));
        Ok(())
    }

    #[test]
    fn bounded_channel_applies_backpressure() -> Result<(), Box<dyn std::error::Error>> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;

        let schema = make_schema();
        let batch = make_batch(&schema, &[1])?;
        let produced = Arc::new(AtomicUsize::new(0));
        let (poll_tx, poll_rx) = std_mpsc::channel();

        let stream = Box::pin(
            ScriptedStream::new(
                schema.clone(),
                VecDeque::from([Ok(batch.clone()), Ok(batch.clone()), Ok(batch)]),
                produced.clone(),
            )
            .with_poll_signal(poll_tx),
        ) as SendableRecordBatchStream;

        let mut reader = SqlStreamRecordBatchReader::spawn(&rt, stream)?;

        assert_eq!(poll_rx.recv_timeout(Duration::from_secs(1))?, 1);
        assert_eq!(poll_rx.recv_timeout(Duration::from_secs(1))?, 2);
        assert!(poll_rx.recv_timeout(Duration::from_millis(100)).is_err());
        assert_eq!(produced.load(Ordering::SeqCst), 2);

        match reader.next() {
            Some(Ok(_)) => {}
            Some(Err(err)) => return Err(Box::new(err)),
            None => return Err(std::io::Error::other("expected first batch").into()),
        }

        assert_eq!(poll_rx.recv_timeout(Duration::from_secs(1))?, 3);
        assert_eq!(produced.load(Ordering::SeqCst), 3);

        Ok(())
    }

    #[test]
    fn drop_aborts_producer_blocked_on_full_channel() -> Result<(), Box<dyn std::error::Error>> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;

        let schema = make_schema();
        let batch = make_batch(&schema, &[1])?;
        let poll_count = Arc::new(AtomicUsize::new(0));
        let dropped = Arc::new(AtomicBool::new(false));
        let (poll_tx, poll_rx) = std_mpsc::channel();

        let stream = Box::pin(
            ScriptedStream::new(
                schema.clone(),
                VecDeque::from([Ok(batch.clone()), Ok(batch.clone()), Ok(batch)]),
                poll_count,
            )
            .with_poll_signal(poll_tx)
            .with_dropped_flag(dropped.clone()),
        ) as SendableRecordBatchStream;

        let reader = SqlStreamRecordBatchReader::spawn(&rt, stream)?;

        assert_eq!(poll_rx.recv_timeout(Duration::from_secs(1))?, 1);
        assert_eq!(poll_rx.recv_timeout(Duration::from_secs(1))?, 2);
        assert!(poll_rx.recv_timeout(Duration::from_millis(100)).is_err());

        drop(reader);

        assert!(wait_until(Duration::from_secs(1), || dropped.load(Ordering::SeqCst)));

        Ok(())
    }

    #[test]
    fn empty_stream_returns_none_immediately() -> Result<(), Box<dyn std::error::Error>> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;

        let schema = make_schema();
        let stream = Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            stream::iter(Vec::<DFResult<RecordBatch>>::new()),
        )) as SendableRecordBatchStream;

        let mut reader = SqlStreamRecordBatchReader::spawn(&rt, stream)?;

        assert!(reader.next().is_none());
        assert!(reader.next().is_none());

        Ok(())
    }

    #[test]
    fn drop_after_partial_consumption_aborts_producer() -> Result<(), Box<dyn std::error::Error>> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;

        let schema = make_schema();
        let batch = make_batch(&schema, &[1])?;
        let poll_count = Arc::new(AtomicUsize::new(0));
        let dropped = Arc::new(AtomicBool::new(false));
        let (poll_tx, poll_rx) = std_mpsc::channel();

        let stream = Box::pin(
            ScriptedStream::new(
                schema.clone(),
                VecDeque::from([
                    Ok(batch.clone()),
                    Ok(batch.clone()),
                    Ok(batch.clone()),
                    Ok(batch),
                ]),
                poll_count,
            )
            .with_poll_signal(poll_tx)
            .with_dropped_flag(dropped.clone()),
        ) as SendableRecordBatchStream;

        let mut reader = SqlStreamRecordBatchReader::spawn(&rt, stream)?;

        assert_eq!(poll_rx.recv_timeout(Duration::from_secs(1))?, 1);
        assert_eq!(poll_rx.recv_timeout(Duration::from_secs(1))?, 2);
        assert!(matches!(reader.next(), Some(Ok(_))));
        assert_eq!(poll_rx.recv_timeout(Duration::from_secs(1))?, 3);

        drop(reader);

        assert!(wait_until(Duration::from_secs(1), || dropped.load(Ordering::SeqCst)));

        Ok(())
    }

    #[test]
    fn next_works_inside_tokio_multithread_runtime() -> Result<(), Box<dyn std::error::Error>> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;

        let schema = make_schema();
        let batch = make_batch(&schema, &[1, 2])?;

        let source = stream::iter(vec![Ok(batch)]);
        let stream: SendableRecordBatchStream =
            Box::pin(RecordBatchStreamAdapter::new(schema.clone(), source));

        let reader = SqlStreamRecordBatchReader::spawn(&rt, stream)?;

        let out = rt.block_on(async move {
            tokio::spawn(async move {
                let mut reader = reader;
                reader.next()
            })
            .await
        });

        let item = match out {
            Ok(item) => item,
            Err(err) => {
                return Err(std::io::Error::other(format!("task should not panic: {err}")).into());
            }
        };
        match item {
            Some(Ok(batch)) => assert_eq!(batch_values(&batch)?, vec![1, 2]),
            Some(Err(err)) => return Err(Box::new(err)),
            None => return Err(std::io::Error::other("expected one batch").into()),
        }

        Ok(())
    }

    #[test]
    fn next_returns_error_inside_tokio_current_thread_runtime()
    -> Result<(), Box<dyn std::error::Error>> {
        let producer_rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        let consumer_rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;

        let schema = make_schema();
        let batch = make_batch(&schema, &[1, 2])?;

        let source = stream::iter(vec![Ok(batch)]);
        let stream: SendableRecordBatchStream =
            Box::pin(RecordBatchStreamAdapter::new(schema.clone(), source));

        let mut reader = SqlStreamRecordBatchReader::spawn(&producer_rt, stream)?;

        let item = consumer_rt.block_on(async move { reader.next() });

        match item {
            Some(Err(err)) => {
                assert!(
                    err.to_string()
                        .contains("cannot block inside a Tokio current-thread runtime")
                );
            }
            Some(Ok(_)) => {
                return Err(std::io::Error::other("expected runtime-context error").into());
            }
            None => {
                return Err(std::io::Error::other("expected an error item").into());
            }
        }

        Ok(())
    }

    #[test]
    fn spawn_rejects_current_thread_runtime() -> Result<(), Box<dyn std::error::Error>> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;

        let schema = make_schema();
        let batch = make_batch(&schema, &[1, 2])?;
        let stream: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            schema,
            stream::iter(vec![Ok(batch)]),
        ));

        let err = match SqlStreamRecordBatchReader::spawn(&rt, stream) {
            Ok(_) => {
                return Err(std::io::Error::other(
                    "expected current-thread runtime construction error",
                )
                .into());
            }
            Err(err) => err,
        };
        assert!(
            err.to_string()
                .contains("requires a Tokio multi-thread runtime")
        );

        Ok(())
    }

    #[test]
    fn current_thread_runtime_error_aborts_producer_immediately()
    -> Result<(), Box<dyn std::error::Error>> {
        let producer_rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        let consumer_rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;

        let schema = make_schema();
        let batch = make_batch(&schema, &[1, 2])?;
        let dropped = Arc::new(AtomicBool::new(false));

        let stream = Box::pin(
            ScriptedStream::new(
                schema.clone(),
                VecDeque::from([Ok(batch)]),
                Arc::new(AtomicUsize::new(0)),
            )
            .with_dropped_flag(dropped.clone()),
        ) as SendableRecordBatchStream;

        let mut reader = SqlStreamRecordBatchReader::spawn(&producer_rt, stream)?;

        let item = consumer_rt.block_on(async move { reader.next() });

        match item {
            Some(Err(err)) => {
                assert!(
                    err.to_string()
                        .contains("cannot block inside a Tokio current-thread runtime")
                );
            }
            Some(Ok(_)) => {
                return Err(std::io::Error::other("expected runtime-context error").into());
            }
            None => {
                return Err(std::io::Error::other("expected an error item").into());
            }
        }

        assert!(wait_until(Duration::from_secs(1), || dropped.load(Ordering::SeqCst)));

        Ok(())
    }
}
