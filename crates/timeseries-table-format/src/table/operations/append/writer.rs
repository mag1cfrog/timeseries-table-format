use super::{
    AppendError, AppendReport, AppendTelemetry, AppendWriterOptions, ArrowInputSnafu,
    ArrowRecordBatch, BatchSchemaAlignment, PendingAppendSegment, SchemaRef, TableError,
    TimeSeriesTable, checked_next_version,
};
use snafu::ResultExt;
use std::sync::Arc;

/// A caller-driven append transaction that can publish multiple segments.
///
/// The writer mutably borrows its table handle. Use a second handle to read
/// while it is open. Batches are encoded as they arrive; only Parquet's own
/// row-group buffering retains their data. Each successful [`Self::commit`]
/// publishes one segment and leaves the writer ready for the next one.
///
/// Pending rows are neither visible to other handles nor durable until commit
/// succeeds. A process that stops before commit loses its pending rows. To
/// recover, reopen the table and query coverage to find missing intervals.
/// Crash recovery requires every concurrent writer to use the commit-lock
/// implementation; upgrade all writers together. Keep `_timeseries_log/WRITE_LOCK`
/// in place. The lock covers publication, not the lifetime of this writer.
///
/// Dropping the writer closes its output and attempts synchronous cleanup.
/// Files that cannot be removed remain unreferenced and are reported by
/// retention-aware vacuum, including files without a valid Parquet footer.
#[must_use = "write batches and commit them, or abort the writer"]
pub struct AppendWriter<'a> {
    table: &'a mut TimeSeriesTable,
    schema: SchemaRef,
    alignment: BatchSchemaAlignment,
    options: AppendWriterOptions,
    pending: Option<PendingAppendSegment>,
    closed: bool,
    needs_schema_alignment: bool,
    pub(super) telemetry: AppendTelemetry,
}

impl<'a> AppendWriter<'a> {
    pub(super) fn new(
        table: &'a mut TimeSeriesTable,
        schema: SchemaRef,
        options: AppendWriterOptions,
        mut telemetry: AppendTelemetry,
    ) -> Result<Self, AppendError> {
        let preparation = (|| {
            table.ensure_write_compatible().map_err(AppendError::from)?;
            options.validate()?;
            checked_next_version(table.state.version).map_err(AppendError::from)?;
            table.build_append_schema_alignment(Arc::clone(&schema))
        })();
        let alignment = match preparation {
            Ok(alignment) => alignment,
            Err(error) => {
                telemetry.record_failure(&error);
                return Err(error);
            }
        };
        let span = tracing::Span::current();
        span.record("compression", options.compression.as_str());
        span.record("max_rows_per_row_group", options.max_rows_per_row_group);
        span.record("max_bytes_per_row_group", options.max_bytes_per_row_group);
        telemetry.complete_phase();
        telemetry.start_phase("source_consumption_and_parquet_write");
        Ok(Self {
            table,
            schema,
            alignment,
            options,
            pending: None,
            closed: false,
            needs_schema_alignment: false,
            telemetry,
        })
    }

    fn ensure_open(&self) -> Result<(), AppendError> {
        if self.closed {
            Err(AppendError::WriterClosed)
        } else {
            Ok(())
        }
    }

    /// Encode one batch into the pending segment without publishing it.
    ///
    /// The batch must match the declared source schema, including for empty
    /// batches. Empty batches create no output. A non-empty batch is encoded
    /// before this method returns; overlap and duplicate validation wait until
    /// commit. Rows remain invisible and are not durable until commit succeeds.
    ///
    /// On failure all rows since the last commit are discarded. Supply those
    /// rows again to retry. A writer closed by an ambiguous or cancelled commit
    /// rejects further writes with [`AppendError::WriterClosed`].
    pub async fn write(&mut self, batch: &ArrowRecordBatch) -> Result<(), TableError> {
        let result = self.write_inner(batch).await;
        self.telemetry.record_result(&result);
        if result.is_err() {
            self.reset_telemetry();
        }
        result.context(crate::table::error::AppendSnafu)
    }

    pub(super) async fn write_inner(
        &mut self,
        batch: &ArrowRecordBatch,
    ) -> Result<(), AppendError> {
        self.ensure_open()?;
        self.telemetry.record_batch(batch);
        let result = async {
            if self.needs_schema_alignment {
                self.alignment = self
                    .table
                    .build_append_schema_alignment(Arc::clone(&self.schema))?;
                self.needs_schema_alignment = false;
            }
            let batch = self.alignment.align_batch(batch).context(ArrowInputSnafu)?;
            if batch.num_rows() == 0 {
                return Ok(());
            }
            if self.pending.is_none() {
                self.pending = Some(
                    PendingAppendSegment::new(
                        self.table,
                        Arc::clone(self.alignment.output_schema()),
                        self.options,
                        &mut self.telemetry,
                    )
                    .await?,
                );
            }
            if let Some(segment) = &mut self.pending {
                segment.write(&batch)?;
            }
            Ok(())
        }
        .await;
        match result {
            Ok(()) => Ok(()),
            Err(error) => Err(self.discard(error).await),
        }
    }

    /// Publish the pending rows as one segment and one new table version.
    ///
    /// Returns `None` without creating artifacts when no rows are pending.
    /// Otherwise finalizes and syncs Parquet, validates coverage and duplicates,
    /// writes sidecars, and commits against the handle's current version.
    /// Rows become visible and durable only when this method succeeds. Recovery
    /// after a process stops is by reopening the table and querying coverage.
    ///
    /// A definite failure discards the pending rows and leaves the writer idle.
    /// After a conflict, end this writer, refresh the handle, and start a new
    /// one. Call [`Self::commit_with_retry`] instead to enable retries for a commit.
    /// An ambiguous outcome
    /// preserves the finished file and reports its path, then closes the writer.
    /// Cancelling this method while a segment is pending also closes the writer;
    /// end it and refresh or reopen before writing again.
    pub async fn commit(&mut self) -> Result<Option<AppendReport>, TableError> {
        self.commit_with_retry(0).await
    }

    /// Publish pending rows, retrying confirmed conflicts across concurrent appends.
    ///
    /// `max_retries` limits additional publication attempts after the first.
    /// Zero has the same behavior as [`Self::commit`]. Each retry waits between
    /// 10 and 250 milliseconds, with jitter, using the current Tokio runtime's
    /// timer. Empty writers still return `None`.
    ///
    /// Retries require a canonical schema in this handle's initial snapshot.
    /// Every intervening commit must only add segments and update coverage.
    /// Metadata or protocol changes, rewrites, overlapping index intervals,
    /// lock contention and uncertain publication stop retrying. The finished
    /// Parquet file and its derived metadata and coverage are reused.
    ///
    /// Success updates this handle to include concurrent appends and this
    /// writer's segment. [`AppendReport::starting_version`] is the base of the
    /// successful attempt. Failure leaves the handle's original snapshot intact;
    /// end a stale writer and refresh before starting another. Definite failure
    /// discards pending rows. Uncertain publication preserves files and closes
    /// the writer. Cancelling an in-flight commit closes the writer and attempts
    /// cleanup of unpublished owned files. Possibly committed files are preserved.
    pub async fn commit_with_retry(
        &mut self,
        max_retries: u32,
    ) -> Result<Option<AppendReport>, TableError> {
        let result = self.commit_inner(max_retries).await;
        self.telemetry.record_result(&result);
        self.reset_telemetry();
        result.context(crate::table::error::AppendSnafu)
    }

    pub(super) async fn commit_inner(
        &mut self,
        max_retries: u32,
    ) -> Result<Option<AppendReport>, AppendError> {
        self.ensure_open()?;
        let Some(segment) = self.pending.take() else {
            return Ok(None);
        };
        // Stay closed if publication is cancelled before its outcome is known.
        self.closed = true;
        self.telemetry.complete_phase();
        let adopts_schema = self.table.state.table_meta.logical_schema.is_none();
        let result = match checked_next_version(self.table.state.version) {
            Ok(next_version) => {
                segment
                    .commit_with_retry(self.table, next_version, max_retries, &mut self.telemetry)
                    .await
            }
            Err(error) => Err(segment
                .rollback(self.table, AppendError::from(error), &mut self.telemetry)
                .await),
        };
        match result {
            Ok(report) => {
                self.closed = false;
                self.needs_schema_alignment = adopts_schema;
                Ok(Some(report))
            }
            Err(error) => {
                self.closed = matches!(error, AppendError::CommitAmbiguous { .. });
                Err(error)
            }
        }
    }

    /// Discard pending output and end the writer.
    ///
    /// Reports every path that could not be removed. An ambiguous commit's
    /// preserved artifacts are kept so that refresh or reopen can resolve it.
    pub async fn abort(self) -> Result<(), TableError> {
        match self.pending {
            Some(segment) => segment.abort(self.table).await,
            None => Ok(()),
        }
        .context(crate::table::error::AppendSnafu)
    }

    /// Return the number of rows written since the last successful commit.
    pub fn pending_rows(&self) -> u64 {
        self.pending
            .as_ref()
            .map_or(0, |segment| segment.rows_written)
    }

    pub(super) async fn discard(&mut self, error: AppendError) -> AppendError {
        let Some(segment) = self.pending.take() else {
            return error;
        };
        self.closed = true;
        let error = segment
            .rollback(self.table, error, &mut self.telemetry)
            .await;
        self.closed = false;
        error
    }

    fn reset_telemetry(&mut self) {
        self.telemetry = AppendTelemetry::for_current_subscriber();
        self.telemetry
            .start_phase("source_consumption_and_parquet_write");
    }
}
