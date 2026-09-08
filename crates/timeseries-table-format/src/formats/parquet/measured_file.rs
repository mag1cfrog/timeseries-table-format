//! Synchronous local Parquet input with logical filesystem-read accounting.

use parquet::{
    errors::ParquetError,
    file::reader::{ChunkReader, Length},
};
use std::{
    fs::File,
    io::{self, BufReader, Read, Seek, SeekFrom},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

/// Counts successful reads, including metadata and buffered read-ahead.
/// OS cache hits are still reads; this does not measure physical device IO.
pub(crate) struct MeasuredParquetFile {
    pub(crate) file: File,
    pub(crate) bytes: Arc<AtomicU64>,
}
pub(crate) struct MeasuredRead {
    file: File,
    bytes: Arc<AtomicU64>,
}
impl Read for MeasuredRead {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        let bytes = self.file.read(buffer)?;
        self.bytes.fetch_add(bytes as u64, Ordering::Relaxed);
        Ok(bytes)
    }
}
impl Length for MeasuredParquetFile {
    fn len(&self) -> u64 {
        self.file.len()
    }
}
impl ChunkReader for MeasuredParquetFile {
    type T = BufReader<MeasuredRead>;
    fn get_read(&self, start: u64) -> Result<Self::T, ParquetError> {
        let mut file = self.file.try_clone()?;
        file.seek(SeekFrom::Start(start))?;
        Ok(BufReader::new(MeasuredRead {
            file,
            bytes: self.bytes.clone(),
        }))
    }
    fn get_bytes(&self, start: u64, length: usize) -> Result<bytes::Bytes, ParquetError> {
        let bytes = self.file.get_bytes(start, length)?;
        self.bytes.fetch_add(bytes.len() as u64, Ordering::Relaxed);
        Ok(bytes)
    }
}
