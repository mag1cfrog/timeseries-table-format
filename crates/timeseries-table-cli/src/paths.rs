use std::path::{Path, PathBuf};

use snafu::{IntoError, ResultExt};
use tokio::fs;

use crate::error::{
    CliResult, CopyParquetSnafu, CreateDirAllSnafu, DestAlreadyExistsSnafu, ParquetMissingSnafu,
    ParquetNoFilenameSnafu, PathInvariantNoSourceSnafu, PathInvariantSnafu, TableRootMissingSnafu,
};

async fn exists(path: &Path) -> std::io::Result<bool> {
    match fs::metadata(path).await {
        Ok(_) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e),
    }
}

/// Ensure `parquet_path` is under `table_root`.
/// If not, copy to `table_root/data/<filename>` and return the *relative* path.
pub async fn ensure_parquet_under_table_root(
    table_root: &Path,
    parquet_path: &Path,
) -> CliResult<PathBuf> {
    let root = fs::canonicalize(table_root)
        .await
        .context(TableRootMissingSnafu {
            path: table_root.display().to_string(),
        })?;

    let src = fs::canonicalize(parquet_path)
        .await
        .context(ParquetMissingSnafu {
            path: parquet_path.display().to_string(),
        })?;

    if let Ok(rel) = src.strip_prefix(&root) {
        return Ok(rel.to_path_buf());
    }

    let file_name = src
        .file_name()
        .ok_or_else(|| {
            ParquetNoFilenameSnafu {
                path: src.display().to_string(),
            }
            .build()
        })?
        .to_owned();

    let data_dir = root.join("data");
    fs::create_dir_all(&data_dir)
        .await
        .context(CreateDirAllSnafu {
            path: data_dir.display().to_string(),
        })?;

    let dst = data_dir.join(file_name);

    if exists(&dst).await.map_err(|e| {
        TableRootMissingSnafu {
            path: dst.display().to_string(),
        }
        .into_error(e)
    })? {
        return Err(DestAlreadyExistsSnafu {
            path: dst.display().to_string(),
        }
        .build());
    }

    fs::copy(&src, &dst).await.context(CopyParquetSnafu {
        src: src.display().to_string(),
        dst: dst.display().to_string(),
    })?;

    let dst = fs::canonicalize(&dst).await.context(PathInvariantSnafu {
        message: "failed to canonicalize copied destination".to_string(),
        path: Some(dst.clone()),
    })?;

    let rel = dst.strip_prefix(&root).map_err(|_| {
        PathInvariantNoSourceSnafu {
            message: "copied parquet is not under table root".to_string(),
            path: Some(dst.clone()),
        }
        .build()
    })?;

    Ok(rel.to_path_buf())
}
