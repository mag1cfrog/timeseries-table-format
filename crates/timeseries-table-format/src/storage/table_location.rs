use std::path::{Path, PathBuf};

#[cfg(feature = "datafusion")]
use object_store::path::Path as ObjectStorePath;
use snafu::{IntoError, ResultExt};

use crate::storage::{BackendError, OtherIoSnafu, StorageLocation, StorageResult};

/// Table root location with table-scoped semantics.
///
/// This wraps `StorageLocation` and is used when callers need to treat the
/// location as a table root (e.g. log layout and segment paths).
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

    #[cfg(feature = "datafusion")]
    pub(crate) fn object_store_url(&self) -> String {
        match self.as_ref() {
            StorageLocation::Local(_) => "file://".to_owned(),
        }
    }

    #[cfg(feature = "datafusion")]
    pub(crate) fn object_store_path(&self, relative_path: &Path) -> StorageResult<ObjectStorePath> {
        let (normalized, native_path) = normalize_relative_storage_path(relative_path)?;

        match self.as_ref() {
            StorageLocation::Local(root) => {
                let absolute = std::path::absolute(root.join(native_path))
                    .map_err(BackendError::Local)
                    .context(OtherIoSnafu {
                        path: normalized.clone(),
                    })?;

                ObjectStorePath::from_absolute_path(absolute).map_err(|source| {
                    OtherIoSnafu { path: normalized }.into_error(BackendError::Local(
                        std::io::Error::new(std::io::ErrorKind::InvalidInput, source),
                    ))
                })
            }
        }
    }
}

pub(crate) fn normalize_relative_storage_path(path: &Path) -> StorageResult<(String, PathBuf)> {
    let supplied = path
        .to_str()
        .ok_or_else(|| invalid_relative_storage_path(path, "path is not valid UTF-8"))?;
    let portable = supplied.replace('\\', "/");

    if portable.is_empty() {
        return Err(invalid_relative_storage_path(path, "path is empty"));
    }
    if portable.starts_with('/') {
        return Err(invalid_relative_storage_path(path, "path must be relative"));
    }

    let mut components = Vec::new();
    for component in portable
        .split('/')
        .filter(|component| !component.is_empty())
    {
        if component == "." || component == ".." {
            return Err(invalid_relative_storage_path(
                path,
                "path contains a current- or parent-directory component",
            ));
        }
        let bytes = component.as_bytes();
        if bytes.len() >= 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':' {
            return Err(invalid_relative_storage_path(
                path,
                "path contains a platform prefix",
            ));
        }
        components.push(component);
    }

    if components.is_empty() {
        return Err(invalid_relative_storage_path(path, "path is empty"));
    }
    let normalized = components.join("/");
    let mut native_path = PathBuf::new();
    for component in components {
        native_path.push(component);
    }
    Ok((normalized, native_path))
}

fn invalid_relative_storage_path(path: &Path, reason: &str) -> crate::storage::StorageError {
    let path = if path.as_os_str().is_empty() {
        "<empty>".to_owned()
    } else {
        path.display().to_string()
    };
    OtherIoSnafu { path: path.clone() }.into_error(BackendError::Local(std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        format!("invalid table-relative storage path '{path}': {reason}"),
    )))
}

#[cfg(test)]
mod tests {
    use crate::storage::StorageError;

    use super::*;

    #[test]
    fn normalize_relative_storage_path_enforces_portable_relative_paths() {
        assert_eq!(
            normalize_relative_storage_path(Path::new("data/seg.parquet")).unwrap(),
            (
                "data/seg.parquet".to_string(),
                PathBuf::from("data").join("seg.parquet")
            )
        );
        assert_eq!(
            normalize_relative_storage_path(Path::new(r"data\seg.parquet")).unwrap(),
            (
                "data/seg.parquet".to_string(),
                PathBuf::from("data").join("seg.parquet")
            )
        );

        for invalid in [
            "",
            "/data/seg.parquet",
            r"C:\data\seg.parquet",
            "data/C:/seg.parquet",
            "data/C:seg.parquet",
            "data/./seg.parquet",
            "data/../seg.parquet",
        ] {
            let err = normalize_relative_storage_path(Path::new(invalid))
                .expect_err("path must be rejected");
            assert!(matches!(err, StorageError::OtherIo { .. }), "{invalid}");
        }
    }
}
