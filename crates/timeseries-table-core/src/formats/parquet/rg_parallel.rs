//! Shared row-group parallelism helpers for Parquet scans.

/// Resolve thread count and row-group chunk size for parallel scans.
pub fn resolve_rg_settings(num_row_groups: usize) -> (usize, usize) {
    let logical_threads = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    let max_threads = logical_threads.saturating_mul(2).max(1);
    let threads_used = if num_row_groups == 0 {
        logical_threads.max(1)
    } else if num_row_groups <= max_threads {
        num_row_groups
    } else {
        logical_threads.max(1)
    };
    let rg_chunk = if num_row_groups == 0 {
        1
    } else {
        num_row_groups.div_ceil(threads_used)
    };
    (threads_used, rg_chunk)
}
