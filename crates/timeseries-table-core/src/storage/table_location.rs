use std::path::{Path, PathBuf};

use snafu::{IntoError, ResultExt};
use tokio::fs;

use crate::storage::layout;
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

    /// Validate and normalize a segment path for storage in table metadata.
    ///
    /// The returned path is table-relative and always uses `/` separators.
    /// The path must resolve to a file under the table root.
    pub async fn normalize_segment_path(&self, segment_path: &Path) -> StorageResult<String> {
        let (normalized, native_path) = normalize_relative_segment_path(segment_path)?;
        self.validate_segment_file(segment_path, &native_path)
            .await?;
        Ok(normalized)
    }

    async fn validate_segment_file(
        &self,
        supplied_path: &Path,
        native_path: &Path,
    ) -> StorageResult<()> {
        match self.as_ref() {
            StorageLocation::Local(table_root) => {
                let root = fs::canonicalize(table_root)
                    .await
                    .map_err(BackendError::Local)
                    .context(NotFoundSnafu {
                        path: table_root.display().to_string(),
                    })?;
                let resolved = fs::canonicalize(root.join(native_path))
                    .await
                    .map_err(BackendError::Local)
                    .context(NotFoundSnafu {
                        path: supplied_path.display().to_string(),
                    })?;

                if resolved.strip_prefix(&root).is_err() {
                    return Err(invalid_segment_path(
                        supplied_path,
                        "path resolves outside the table root",
                    ));
                }
                if !fs::metadata(&resolved)
                    .await
                    .map_err(BackendError::Local)
                    .context(OtherIoSnafu {
                        path: resolved.display().to_string(),
                    })?
                    .is_file()
                {
                    return Err(invalid_segment_path(
                        supplied_path,
                        "path does not resolve to a file",
                    ));
                }

                Ok(())
            }
        }
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

                let data_dir = root.join(layout::DATA_DIR_NAME);
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

pub(crate) fn normalize_relative_segment_path(
    segment_path: &Path,
) -> StorageResult<(String, PathBuf)> {
    let supplied = segment_path
        .to_str()
        .ok_or_else(|| invalid_segment_path(segment_path, "path is not valid UTF-8"))?;
    let portable = supplied.replace('\\', "/");

    if portable.is_empty() {
        return Err(invalid_segment_path(segment_path, "path is empty"));
    }
    if portable.starts_with('/') {
        return Err(invalid_segment_path(segment_path, "path must be relative"));
    }

    let mut components = Vec::new();
    for component in portable
        .split('/')
        .filter(|component| !component.is_empty())
    {
        if component == "." || component == ".." {
            return Err(invalid_segment_path(
                segment_path,
                "path contains a current- or parent-directory component",
            ));
        }
        if components.is_empty() {
            let bytes = component.as_bytes();
            if bytes.len() >= 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':' {
                return Err(invalid_segment_path(
                    segment_path,
                    "path contains a platform prefix",
                ));
            }
        }
        components.push(component);
    }

    if components.is_empty() {
        return Err(invalid_segment_path(segment_path, "path is empty"));
    }
    let normalized = components.join("/");
    let mut native_path = PathBuf::new();
    for component in components {
        native_path.push(component);
    }
    Ok((normalized, native_path))
}

fn invalid_segment_path(path: &Path, reason: &str) -> crate::storage::StorageError {
    let path = if path.as_os_str().is_empty() {
        "<empty>".to_owned()
    } else {
        path.display().to_string()
    };
    OtherIoSnafu { path: path.clone() }.into_error(BackendError::Local(std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        format!("invalid segment path '{path}': {reason}"),
    )))
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

    #[tokio::test]
    async fn normalize_segment_path_enforces_canonical_table_relative_file_path() -> TestResult {
        let tmp = TempDir::new()?;
        let table_root = tmp.path().join("table");
        let segment = table_root.join("data/seg.parquet");
        tokio::fs::create_dir_all(segment.parent().unwrap()).await?;
        tokio::fs::write(&segment, b"parquet").await?;
        let location = TableLocation::local(&table_root);

        assert_eq!(
            location
                .normalize_segment_path(Path::new("data/seg.parquet"))
                .await?,
            "data/seg.parquet"
        );
        assert_eq!(
            location
                .normalize_segment_path(Path::new(r"data\seg.parquet"))
                .await?,
            "data/seg.parquet"
        );

        for invalid in [
            "",
            "/data/seg.parquet",
            r"C:\data\seg.parquet",
            "data/./seg.parquet",
            "data/../seg.parquet",
        ] {
            let err = location
                .normalize_segment_path(Path::new(invalid))
                .await
                .expect_err("path must be rejected");
            assert!(matches!(err, StorageError::OtherIo { .. }), "{invalid}");
        }

        let err = location
            .normalize_segment_path(Path::new("data"))
            .await
            .expect_err("directory must be rejected");
        assert!(err.to_string().contains("does not resolve to a file"));

        #[cfg(unix)]
        {
            let outside = tmp.path().join("outside.parquet");
            tokio::fs::write(&outside, b"parquet").await?;
            std::os::unix::fs::symlink(&outside, table_root.join("data/link.parquet"))?;

            let err = location
                .normalize_segment_path(Path::new("data/link.parquet"))
                .await
                .expect_err("symlink escape must be rejected");
            assert!(err.to_string().contains("outside the table root"));
        }

        Ok(())
    }
}
