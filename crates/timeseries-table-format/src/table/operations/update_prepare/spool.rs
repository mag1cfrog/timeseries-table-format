//! Private length-framed runs; two-way merging never opens one file per run.

use std::{
    fs::{self, File, OpenOptions},
    io::{self, BufReader, BufWriter, Read, Write},
    mem::size_of,
    path::{Path, PathBuf},
};

use super::{PrepareError, Result};

/// Counters describe owned sort buffers, not total process RSS or Arrow/Parquet allocations.
#[derive(Debug, Default, Clone)]
pub(crate) struct PreparationMetrics {
    pub(crate) peak_sort_bytes: usize,
    pub(crate) largest_record_bytes: usize,
    pub(crate) peak_scratch_bytes: u64,
    pub(crate) initial_runs: u64,
    pub(crate) target_rows_read: u64,
    pub(crate) projected_column_bytes: u64,
    pub(crate) key_discovery_bytes_read: u64,
}

pub(super) fn io_error(path: &Path, source: io::Error) -> PrepareError {
    PrepareError::Io {
        path: path.to_owned(),
        source,
    }
}

/// Exclusive directory ownership avoids retaining an ever-growing path registry.
/// Files are flat, monotonically numbered, and never enter transaction actions.
pub(super) struct Scratch {
    pub(super) directory: PathBuf,
    next_id: u64,
    live_bytes: u64,
    pub(super) metrics: PreparationMetrics,
}

impl Scratch {
    pub(super) fn create(root: &Path) -> Result<Self> {
        let parent = root.join(crate::storage::layout::UPDATE_PREPARE_DIR);
        fs::create_dir_all(&parent).map_err(|e| io_error(&parent, e))?;
        let directory = parent.join(uuid::Uuid::new_v4().to_string());
        // Never adopt or clean a directory whose exclusive creation failed.
        fs::create_dir(&directory).map_err(|e| io_error(&directory, e))?;
        Ok(Self {
            directory,
            next_id: 0,
            live_bytes: 0,
            metrics: PreparationMetrics::default(),
        })
    }

    pub(super) fn path(&self, id: u64) -> PathBuf {
        self.directory.join(format!("{id:020}.run"))
    }

    fn writer(&mut self) -> Result<(u64, BufWriter<File>)> {
        let id = self.next_id;
        self.next_id = id.checked_add(1).ok_or(PrepareError::Resource {
            reason: "scratch file counter overflow",
        })?;
        let path = self.path(id);
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|e| io_error(&path, e))?;
        Ok((id, BufWriter::new(file)))
    }

    fn completed(&mut self, id: u64, mut writer: BufWriter<File>) -> Result<()> {
        let path = self.path(id);
        writer.flush().map_err(|e| io_error(&path, e))?;
        let bytes = writer
            .get_ref()
            .metadata()
            .map_err(|e| io_error(&path, e))?
            .len();
        self.live_bytes = self
            .live_bytes
            .checked_add(bytes)
            .ok_or(PrepareError::Resource {
                reason: "scratch byte counter overflow",
            })?;
        self.metrics.peak_scratch_bytes = self.metrics.peak_scratch_bytes.max(self.live_bytes);
        Ok(())
    }

    pub(super) fn remove(&mut self, id: u64) -> Result<()> {
        let path = self.path(id);
        let bytes = fs::metadata(&path).map_err(|e| io_error(&path, e))?.len();
        fs::remove_file(&path).map_err(|e| io_error(&path, e))?;
        self.live_bytes -= bytes;
        Ok(())
    }

    /// Keep at most one typed cleanup example, even if a whole disk is inaccessible.
    /// Drop retries remaining files; vacuum can reclaim process-interrupted leftovers.
    pub(super) fn cleanup(&mut self) -> Result<()> {
        let mut first = None;
        let mut failures = 0_u64;
        let entries = match fs::read_dir(&self.directory) {
            Ok(entries) => entries,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(io_error(&self.directory, e)),
        };
        for entry in entries {
            let result = entry.and_then(|entry| fs::remove_file(entry.path()));
            if let Err(e) = result {
                failures += 1;
                first.get_or_insert(e);
            }
        }
        if let Err(e) = fs::remove_dir(&self.directory) {
            failures += 1;
            first.get_or_insert(e);
        }
        if let Some(source) = first {
            Err(PrepareError::Cleanup {
                path: self.directory.clone(),
                failures,
                source,
            })
        } else {
            Ok(())
        }
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = self.cleanup();
    }
}

pub(super) struct Record {
    pub(super) order: Vec<u8>,
    pub(super) key: Vec<u8>,
    pub(super) values: Vec<u8>,
    pub(super) segment: u64,
    pub(super) row: u64,
}

impl Record {
    fn allocated_bytes(&self) -> usize {
        self.order.capacity() + self.key.capacity() + self.values.capacity()
    }

    fn write(&self, writer: &mut impl Write) -> io::Result<()> {
        for value in [&self.order, &self.key, &self.values] {
            writer.write_all(&(value.len() as u64).to_le_bytes())?;
            writer.write_all(value)?;
        }
        writer.write_all(&self.segment.to_le_bytes())?;
        writer.write_all(&self.row.to_le_bytes())
    }
}

pub(super) struct RunReader {
    path: PathBuf,
    reader: BufReader<File>,
    max_record: usize,
}

impl RunReader {
    pub(super) fn open(scratch: &Scratch, id: u64) -> Result<Self> {
        let path = scratch.path(id);
        let reader = BufReader::new(File::open(&path).map_err(|e| io_error(&path, e))?);
        Ok(Self {
            path,
            reader,
            max_record: scratch.metrics.largest_record_bytes,
        })
    }

    pub(super) fn next(&mut self) -> Result<Option<Record>> {
        self.read_record().map_err(|e| io_error(&self.path, e))
    }

    fn read_record(&mut self) -> io::Result<Option<Record>> {
        let mut length = [0; 8];
        if self.reader.read(&mut length[..1])? == 0 {
            return Ok(None);
        }
        self.reader.read_exact(&mut length[1..])?;
        let mut remaining = self.max_record;
        let mut read_value =
            |length: [u8; 8], reader: &mut BufReader<File>| -> io::Result<Vec<u8>> {
                let len = usize::try_from(u64::from_le_bytes(length))
                    .map_err(|_| io::Error::other("scratch record length overflow"))?;
                remaining = remaining.checked_sub(len).ok_or_else(|| {
                    io::Error::other("scratch record exceeds written record bound")
                })?;
                let mut bytes = Vec::new();
                bytes.try_reserve_exact(len).map_err(io::Error::other)?;
                bytes.resize(len, 0);
                reader.read_exact(&mut bytes)?;
                Ok(bytes)
            };
        let order = read_value(length, &mut self.reader)?;
        self.reader.read_exact(&mut length)?;
        let key = read_value(length, &mut self.reader)?;
        self.reader.read_exact(&mut length)?;
        let values = read_value(length, &mut self.reader)?;
        self.reader.read_exact(&mut length)?;
        let segment = u64::from_le_bytes(length);
        self.reader.read_exact(&mut length)?;
        Ok(Some(Record {
            order,
            key,
            values,
            segment,
            row: u64::from_le_bytes(length),
        }))
    }
}

/// One byte-budgeted buffer and contiguous run IDs, never a vector of all runs.
/// Only one sorter may allocate runs in this scratch directory until finish;
/// readers of already completed runs may coexist with it.
pub(super) struct Sorter {
    budget: usize,
    records: Vec<Record>,
    bytes: usize,
    first_run: u64,
    runs: u64,
}

impl Sorter {
    pub(super) fn new(scratch: &Scratch, budget: usize) -> Self {
        Self {
            budget,
            records: Vec::new(),
            bytes: 0,
            first_run: scratch.next_id,
            runs: 0,
        }
    }

    pub(super) fn push(&mut self, scratch: &mut Scratch, record: Record) -> Result<()> {
        let bytes = record.allocated_bytes();
        scratch.metrics.largest_record_bytes = scratch.metrics.largest_record_bytes.max(bytes);
        // Account for Vec capacity as well as heap payloads. One oversized record
        // may exceed the budget; it is flushed alone, never accumulated with others.
        if !self.records.is_empty()
            && self.bytes + bytes + (self.records.len() + 1) * size_of::<Record>() > self.budget
        {
            self.flush(scratch)?;
        }
        self.records
            .try_reserve_exact(1)
            .map_err(|_| PrepareError::Resource {
                reason: "sort allocation failed",
            })?;
        self.bytes += bytes;
        self.records.push(record);
        let allocated = self.bytes + self.records.capacity() * size_of::<Record>();
        scratch.metrics.peak_sort_bytes = scratch.metrics.peak_sort_bytes.max(allocated);
        if allocated >= self.budget {
            self.flush(scratch)?;
        }
        Ok(())
    }

    fn flush(&mut self, scratch: &mut Scratch) -> Result<()> {
        self.records.sort_unstable_by(|a, b| a.order.cmp(&b.order));
        let (id, mut writer) = scratch.writer()?;
        for record in &self.records {
            record
                .write(&mut writer)
                .map_err(|e| io_error(&scratch.path(id), e))?;
        }
        scratch.completed(id, writer)?;
        self.records = Vec::new();
        self.bytes = 0;
        self.runs += 1;
        scratch.metrics.initial_runs += 1;
        Ok(())
    }

    pub(super) async fn finish(mut self, scratch: &mut Scratch) -> Result<u64> {
        if !self.records.is_empty() || self.runs == 0 {
            self.flush(scratch)?;
        }
        while self.runs > 1 {
            let next_first = scratch.next_id;
            for pair in (0..self.runs).step_by(2) {
                let left_id = self.first_run + pair;
                let right_id = (pair + 1 < self.runs).then_some(left_id + 1);
                let mut left = RunReader::open(scratch, left_id)?;
                let mut right = right_id
                    .map(|id| RunReader::open(scratch, id))
                    .transpose()?;
                let (output, mut writer) = scratch.writer()?;
                let mut a = left.next()?;
                let mut b = match &mut right {
                    Some(reader) => reader.next()?,
                    None => None,
                };
                let mut written = 0_u64;
                while a.is_some() || b.is_some() {
                    let take_left = match (&a, &b) {
                        (Some(a), Some(b)) => a.order <= b.order,
                        (Some(_), None) => true,
                        _ => false,
                    };
                    if take_left {
                        if let Some(record) = a.take() {
                            record
                                .write(&mut writer)
                                .map_err(|e| io_error(&scratch.path(output), e))?;
                        }
                        a = left.next()?;
                    } else {
                        if let Some(record) = b.take() {
                            record
                                .write(&mut writer)
                                .map_err(|e| io_error(&scratch.path(output), e))?;
                        }
                        b = match &mut right {
                            Some(reader) => reader.next()?,
                            None => None,
                        };
                    }
                    written += 1;
                    if written.is_multiple_of(1024) {
                        tokio::task::yield_now().await;
                    }
                }
                drop((left, right));
                scratch.completed(output, writer)?;
                scratch.remove(left_id)?;
                if let Some(id) = right_id {
                    scratch.remove(id)?;
                }
                // Short runs must yield too: an early merge pass may contain
                // thousands of pairs without any pair reaching 1024 records.
                tokio::task::yield_now().await;
            }
            self.first_run = next_first;
            self.runs = self.runs.div_ceil(2);
        }
        Ok(self.first_run)
    }
}
