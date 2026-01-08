//! Append profiling report types used by the CLI benchmark binary.

use std::time::{Duration, Instant};

/// Single append step timing entry.
#[derive(Debug, Clone)]
pub struct AppendStep {
    /// Step name (stable identifier).
    pub name: String,
    /// Elapsed wall time in milliseconds.
    pub elapsed_ms: u128,

    /// Arbitrary key/value annotations captured during the step.
    pub fields: Vec<(String, String)>,
}

/// Full append profiling report.
#[derive(Debug, Clone)]
pub struct AppendReport {
    /// Context-level metadata (table, segment, bytes, etc.).
    pub context: Vec<(String, String)>,
    /// Per-step timing breakdown.
    pub steps: Vec<AppendStep>,
    /// Total elapsed wall time in milliseconds.
    pub total_ms: u128,
}

/// Builder for append profiling reports.
#[derive(Debug)]
pub struct AppendReportBuilder {
    start: Instant,
    context: Vec<(String, String)>,
    steps: Vec<AppendStep>,
}

impl Default for AppendReportBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl AppendReportBuilder {
    /// Create a new report builder and start the total timer.
    pub fn new() -> Self {
        Self {
            start: Instant::now(),
            context: Vec::new(),
            steps: Vec::new(),
        }
    }

    /// Set or update a context key/value pair.
    pub fn set_context(&mut self, key: &str, value: impl Into<String>) {
        let value = value.into();
        if let Some((_, v)) = self.context.iter_mut().find(|(k, _)| k == key) {
            *v = value;
        } else {
            self.context.push((key.to_string(), value));
        }
    }

    /// Append a step timing entry.
    pub fn push_step<I>(&mut self, name: &str, elapsed: Duration, fields: I)
    where
        I: IntoIterator<Item = (String, String)>,
    {
        self.steps.push(AppendStep {
            name: name.to_string(),
            elapsed_ms: elapsed.as_millis(),
            fields: fields.into_iter().collect(),
        });
    }

    /// Finalize the report.
    pub fn finish(self) -> AppendReport {
        AppendReport {
            context: self.context,
            steps: self.steps,
            total_ms: self.start.elapsed().as_millis(),
        }
    }
}
