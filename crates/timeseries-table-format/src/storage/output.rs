use std::{
    io::{self, Write},
    path::{Path, PathBuf},
};

use snafu::{IntoError, ResultExt};
use tokio::fs;

use crate::storage::{
    BackendError, OtherIoSnafu, StorageLocation, StorageResult, TempFileGuard, create_new_file,
    create_parent_dir, join_local,
};

enum LocalFinish {
    Rename(PathBuf),
    Keep,
}

/// Local filesystem sink that either renames or keeps its path on finish.
struct LocalSink {
    path: PathBuf,
    finish: LocalFinish,
    writer: io::BufWriter<std::fs::File>,
    guard: TempFileGuard,
}

impl LocalSink {
    async fn open(location: &StorageLocation, rel_path: &Path) -> StorageResult<Self> {
        let final_path = join_local(location, rel_path)?;
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
            path: tmp_path,
            finish: LocalFinish::Rename(final_path),
            writer,
            guard,
        })
    }

    async fn open_new(location: &StorageLocation, rel_path: &Path) -> StorageResult<Self> {
        let path = join_local(location, rel_path)?;
        let file = create_new_file(&path).await?.into_std().await;
        let writer = io::BufWriter::new(file);
        let guard = TempFileGuard::new(path.clone());
        Ok(Self {
            path,
            finish: LocalFinish::Keep,
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
                path: self.path.display().to_string(),
            })?;

        self.writer
            .get_ref()
            .sync_all()
            .map_err(BackendError::Local)
            .context(OtherIoSnafu {
                path: self.path.display().to_string(),
            })?;

        if let LocalFinish::Rename(final_path) = &self.finish {
            fs::rename(&self.path, final_path)
                .await
                .map_err(BackendError::Local)
                .context(OtherIoSnafu {
                    path: final_path.display().to_string(),
                })?;
        }

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

/// Open a streaming output sink with exclusive creation.
///
/// The final path is created immediately and removed if the sink is dropped
/// before [`OutputSink::finish`] succeeds.
///
/// # Errors
///
/// Returns [`crate::storage::StorageError::AlreadyExists`] when `rel_path`
/// already exists, or another storage error when creation fails.
pub async fn open_new_output_sink(
    location: &StorageLocation,
    rel_path: &Path,
) -> StorageResult<OutputSink> {
    match location {
        StorageLocation::Local(_) => Ok(OutputSink {
            inner: OutputSinkInner::Local(LocalSink::open_new(location, rel_path).await?),
        }),
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
                let rel_path = path.file_name().ok_or_else(|| {
                    OtherIoSnafu {
                        path: trimmed.to_string(),
                    }
                    .into_error(BackendError::Local(std::io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "output location has no file name",
                    )))
                })?;
                let base = path
                    .parent()
                    .filter(|parent| !parent.as_os_str().is_empty())
                    .unwrap_or_else(|| Path::new("."));

                Ok(OutputLocation {
                    storage: StorageLocation::Local(base.to_path_buf()),
                    rel_path: PathBuf::from(rel_path),
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::StorageError;
    use tempfile::TempDir;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    #[tokio::test]
    async fn new_output_sink_creates_exclusively_and_finishes() -> TestResult {
        let temp = TempDir::new()?;
        let location = StorageLocation::local(temp.path());
        let path = Path::new("staged/output.parquet");
        let mut sink = open_new_output_sink(&location, path).await?;
        sink.writer().write_all(b"parquet")?;
        sink.finish().await?;

        assert_eq!(tokio::fs::read(temp.path().join(path)).await?, b"parquet");
        Ok(())
    }

    #[tokio::test]
    async fn new_output_sink_preserves_an_existing_object() -> TestResult {
        let temp = TempDir::new()?;
        let location = StorageLocation::local(temp.path());
        let path = Path::new("staged/existing.parquet");
        crate::storage::write_new(&location, path, b"existing").await?;

        let error = match open_new_output_sink(&location, path).await {
            Ok(_) => panic!("existing output must not be replaced"),
            Err(error) => error,
        };
        assert!(matches!(error, StorageError::AlreadyExists { .. }));
        assert_eq!(tokio::fs::read(temp.path().join(path)).await?, b"existing");
        Ok(())
    }

    #[tokio::test]
    async fn dropping_unfinished_new_output_removes_it() -> TestResult {
        let temp = TempDir::new()?;
        let location = StorageLocation::local(temp.path());
        let path = Path::new("staged/unfinished.parquet");
        let mut sink = open_new_output_sink(&location, path).await?;
        sink.writer().write_all(b"incomplete")?;
        drop(sink);

        assert!(!temp.path().join(path).exists());
        Ok(())
    }
}
