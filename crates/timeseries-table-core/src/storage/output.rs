use std::{
    io::{self, Write},
    path::{Path, PathBuf},
};

use snafu::{IntoError, ResultExt};
use tokio::fs;

use crate::storage::{
    BackendError, OtherIoSnafu, StorageLocation, StorageResult, TempFileGuard, create_parent_dir,
    join_local,
};

/// Local filesystem sink that writes to a temp file and renames on finish.
struct LocalSink {
    tmp_path: PathBuf,
    final_path: PathBuf,
    writer: io::BufWriter<std::fs::File>,
    guard: TempFileGuard,
}

impl LocalSink {
    async fn open(location: &StorageLocation, rel_path: &Path) -> StorageResult<Self> {
        let final_path = join_local(location, rel_path);
        create_parent_dir(&final_path).await?;

        let tmp_path = final_path.with_extension("tmp");

        // Use std::fs::File because Arrow writers require std::io::Write.
        let file = std::fs::File::create(&tmp_path)
            .map_err(BackendError::Local)
            .context(OtherIoSnafu {
                path: tmp_path.display().to_string(),
            })?;

        let writer = io::BufWriter::new(file);
        let guard = TempFileGuard::new(tmp_path.clone());

        Ok(Self {
            tmp_path,
            final_path,
            writer,
            guard,
        })
    }

    fn writer(&mut self) -> &mut dyn Write {
        &mut self.writer
    }

    async fn finish(&mut self) -> StorageResult<()> {
        self.writer
            .flush()
            .map_err(BackendError::Local)
            .context(OtherIoSnafu {
                path: self.tmp_path.display().to_string(),
            })?;

        self.writer
            .get_ref()
            .sync_all()
            .map_err(BackendError::Local)
            .context(OtherIoSnafu {
                path: self.tmp_path.display().to_string(),
            })?;

        fs::rename(&self.tmp_path, &self.final_path)
            .await
            .map_err(BackendError::Local)
            .context(OtherIoSnafu {
                path: self.final_path.display().to_string(),
            })?;

        self.guard.disarm();
        Ok(())
    }
}

enum OutputSinkInner {
    Local(LocalSink),
    // S3(S3Sink),
}

/// A streaming output sink for writing bytes to a storage backend.
///
/// This type abstracts over backend-specific sink implementations. Callers
/// obtain a sink via `open_output_sink` and then stream bytes through the
/// `writer()` handle. Finalization is explicit via `finish()` to allow
/// backend-specific commit semantics (e.g., atomic rename or multipart upload).
pub struct OutputSink {
    inner: OutputSinkInner,
}

impl OutputSink {
    /// Return a mutable Write handle for streaming bytes.
    pub fn writer(&mut self) -> &mut dyn Write {
        match &mut self.inner {
            OutputSinkInner::Local(s) => s.writer(),
        }
    }

    /// Flush, fsync, and commit to final location.
    pub async fn finish(self) -> StorageResult<()> {
        match self.inner {
            OutputSinkInner::Local(mut s) => s.finish().await,
        }
    }
}

/// Open a streaming output sink at `location` + `rel_path`.
///
/// The `location` identifies the backend root, while `rel_path` identifies
/// the object/key within that backend. For local filesystems this performs a
/// temp-file write and atomic rename on `finish()`.
///
/// v0.1: only StorageLocation::Local is supported.
pub async fn open_output_sink(
    location: &StorageLocation,
    rel_path: &Path,
) -> StorageResult<OutputSink> {
    match location {
        StorageLocation::Local(_) => {
            let sink = LocalSink::open(location, rel_path).await?;
            Ok(OutputSink {
                inner: OutputSinkInner::Local(sink),
            })
        }
    }
}

/// Fully-qualified output target: backend + relative path/key.
#[derive(Debug, Clone)]
pub struct OutputLocation {
    /// Backend where the output will be written.
    pub storage: StorageLocation,
    /// Path within the backend for the output object.
    pub rel_path: PathBuf,
}

impl OutputLocation {
    /// Parse a string specification into an `OutputLocation`, validating it is non-empty and supported.
    pub fn parse(spec: &str) -> StorageResult<OutputLocation> {
        let trimmed = spec.trim();
        if trimmed.is_empty() {
            return Err(OtherIoSnafu {
                path: "<empty output location>".to_string(),
            }
            .into_error(BackendError::Local(std::io::Error::new(
                io::ErrorKind::InvalidInput,
                "output location is empty",
            ))));
        }

        let storage = StorageLocation::parse(trimmed)?;

        match &storage {
            StorageLocation::Local(_) => {
                let path = PathBuf::from(trimmed);

                let base = PathBuf::from(".");
                let rel_path = path;

                Ok(OutputLocation {
                    storage: StorageLocation::Local(base),
                    rel_path,
                })
            }
        }
    }
}
