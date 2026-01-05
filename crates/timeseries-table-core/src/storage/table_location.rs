use std::path::{Path, PathBuf};

use snafu::{IntoError, ResultExt};
use tokio::fs;

use crate::storage::{
    AlreadyExistsNoSourceSnafu, BackendError, NotFoundSnafu, OtherIoSnafu, StorageLocation,
    StorageResult,
};

/// Table root location with table-scoped semantics.
///
/// This wraps `StorageLocation` and is used when callers need to treat the
/// location as a table root (e.g. log layout, segment paths, and helpers like
/// `ensure_parquet_under_root`).
#[derive(Debug, Clone)]
pub struct TableLocation(StorageLocation);

impl From<TableLocation> for StorageLocation {
    fn from(t: TableLocation) -> Self {
        t.0
    }
}

impl AsRef<StorageLocation> for TableLocation {
    fn as_ref(&self) -> &StorageLocation {
        &self.0
    }
}

impl TableLocation {
    /// Creates a new `TableLocation` for a local filesystem path.
    pub fn local(root: impl Into<PathBuf>) -> Self {
        TableLocation(StorageLocation::Local(root.into()))
    }

    /// Parse a user-facing table location string into a TableLocation.
    /// v0.1: only local filesystem paths are supported.
    pub fn parse(spec: &str) -> StorageResult<Self> {
        StorageLocation::parse(spec).map(TableLocation)
    }

    /// Return the underlying StorageLocation
    pub fn storage(&self) -> &StorageLocation {
        &self.0
    }

    /// Ensure `parquet_path` is under this table root.
    /// If not, copy it into `data/<filename>` and return the relative path.
    pub async fn ensure_parquet_under_root(&self, parquet_path: &Path) -> StorageResult<PathBuf> {
        match self.as_ref() {
            StorageLocation::Local(table_root) => {
                let root = fs::canonicalize(table_root)
                    .await
                    .map_err(BackendError::Local)
                    .context(NotFoundSnafu {
                        path: table_root.display().to_string(),
                    })?;

                let src = fs::canonicalize(parquet_path)
                    .await
                    .map_err(BackendError::Local)
                    .context(NotFoundSnafu {
                        path: parquet_path.display().to_string(),
                    })?;

                if let Ok(rel) = src.strip_prefix(&root) {
                    return Ok(rel.to_path_buf());
                }

                let file_name = src
                    .file_name()
                    .ok_or_else(|| {
                        OtherIoSnafu {
                            path: src.display().to_string(),
                        }
                        .into_error(BackendError::Local(
                            std::io::Error::other("parquet path has no filename"),
                        ))
                    })?
                    .to_owned();

                let data_dir = root.join("data");
                fs::create_dir_all(&data_dir)
                    .await
                    .map_err(BackendError::Local)
                    .context(OtherIoSnafu {
                        path: data_dir.display().to_string(),
                    })?;

                let dst = data_dir.join(file_name);

                match fs::metadata(&dst).await {
                    Ok(_) => {
                        return AlreadyExistsNoSourceSnafu {
                            path: dst.display().to_string(),
                        }
                        .fail();
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                        // ok to proceed
                    }

                    Err(e) => {
                        return Err(BackendError::Local(e)).context(OtherIoSnafu {
                            path: dst.display().to_string(),
                        });
                    }
                }

                fs::copy(&src, &dst)
                    .await
                    .map_err(BackendError::Local)
                    .context(OtherIoSnafu {
                        path: dst.display().to_string(),
                    })?;

                let dst = fs::canonicalize(&dst)
                    .await
                    .map_err(BackendError::Local)
                    .context(OtherIoSnafu {
                        path: dst.display().to_string(),
                    })?;

                let rel = dst.strip_prefix(&root).map_err(|_| {
                    OtherIoSnafu {
                        path: dst.display().to_string(),
                    }
                    .into_error(BackendError::Local(std::io::Error::other(
                        "copied parquet is not under table root",
                    )))
                })?;

                Ok(rel.to_path_buf())
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::storage::StorageError;

    use super::*;
    use tempfile::TempDir;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    #[tokio::test]
    async fn ensure_parquet_under_root_returns_relative_path() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());

        let rel_path = Path::new("data/seg.parquet");
        let abs_path = tmp.path().join(rel_path);
        tokio::fs::create_dir_all(abs_path.parent().unwrap()).await?;
        tokio::fs::write(&abs_path, b"parquet").await?;

        let rel = location.ensure_parquet_under_root(&abs_path).await?;
        assert_eq!(rel, rel_path);
        Ok(())
    }

    #[tokio::test]
    async fn ensure_parquet_under_root_copies_outside_file() -> TestResult {
        let tmp = TempDir::new()?;
        let table_root = tmp.path().join("table");
        tokio::fs::create_dir_all(&table_root).await?;
        let location = TableLocation::local(&table_root);

        let src_path = tmp.path().join("outside.parquet");
        tokio::fs::write(&src_path, b"parquet").await?;

        let rel = location.ensure_parquet_under_root(&src_path).await?;
        let expected_rel = PathBuf::from("data/outside.parquet");
        assert_eq!(rel, expected_rel);

        let dst = table_root.join(&expected_rel);
        assert!(dst.exists());
        let contents = tokio::fs::read(&dst).await?;
        assert_eq!(contents, b"parquet");
        Ok(())
    }

    #[tokio::test]
    async fn ensure_parquet_under_root_refuses_overwrite() -> TestResult {
        let tmp = TempDir::new()?;
        let table_root = tmp.path().join("table");
        tokio::fs::create_dir_all(&table_root).await?;
        let location = TableLocation::local(&table_root);

        let data_dir = table_root.join("data");
        tokio::fs::create_dir_all(&data_dir).await?;
        let existing_dst = data_dir.join("seg.parquet");
        tokio::fs::write(&existing_dst, b"existing").await?;

        let src_path = tmp.path().join("seg.parquet");
        tokio::fs::write(&src_path, b"new").await?;

        let err = location
            .ensure_parquet_under_root(&src_path)
            .await
            .expect_err("expected AlreadyExistsNoSource");

        assert!(matches!(err, StorageError::AlreadyExistsNoSource { .. }));
        Ok(())
    }
}
