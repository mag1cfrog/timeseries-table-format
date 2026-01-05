use std::{
    path::{Path, PathBuf},
    time::Duration,
};

use tabled::{
    builder::Builder,
    settings::{Style, object::Rows, style::LineText},
};

use crate::error::CliResult;

#[derive(Debug, Clone, Copy)]
pub enum OutputFormat {
    Csv,
    Jsonl,
}

#[derive(Debug, Clone)]
pub struct QueryOpts {
    pub explain: bool,
    pub timing: bool,
    pub max_rows: usize,
    pub output: Option<PathBuf>,
    pub format: OutputFormat,
}

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub preview_rows: Vec<Vec<String>>,
    pub total_rows: u64,
    pub elapsed: Option<Duration>,
}

fn sanitize_identifier(raw: &str) -> String {
    let mut out = String::new();
    for ch in raw.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            out.push(ch);
        } else {
            out.push('_');
        }
    }

    if out.is_empty() {
        return "t".to_string();
    }

    if out
        .chars()
        .next()
        .map(|ch| !ch.is_ascii_alphabetic())
        .unwrap_or(false)
    {
        out = format!("t_{out}");
    }

    out.make_ascii_lowercase();
    out
}

pub fn quote_identifier(name: &str) -> String {
    let escaped = name.replace('"', "\"\"");
    format!("\"{escaped}\"")
}

/// Pick a stable, user-friendly SQL table name from the table root path.
/// Fallback is "t".
pub fn default_table_name(table_root: &Path) -> String {
    table_root
        .file_name()
        .map(|s| s.to_string_lossy().to_string())
        .filter(|s| !s.is_empty())
        .map(|s| sanitize_identifier(&s))
        .unwrap_or_else(|| "t".to_string())
}

fn render_table(columns: &[String], rows: &[Vec<String>]) -> String {
    if columns.is_empty() {
        return String::new();
    }

    let mut builder = Builder::default();
    builder.push_record(columns);
    for row in rows {
        builder.push_record(row);
    }

    let mut table = builder.build();
    table.with(Style::rounded());
    table.with(LineText::new("Preview output", Rows::first()).offset(6));
    table.to_string()
}

pub fn print_query_result(res: &QueryResult, opts: &QueryOpts) -> CliResult<()> {
    if !res.preview_rows.is_empty() {
        let rendered = render_table(&res.columns, &res.preview_rows);
        println!("{rendered}");
    } else if opts.max_rows == 0 && !res.columns.is_empty() {
        let rendered = render_table(&res.columns, &[]);
        println!("{rendered}");
        if res.total_rows > 0 {
            println!("(preview suppressed; use --max-rows > 0)");
        }
    } else if opts.max_rows == 0 && res.total_rows > 0 {
        println!("(preview suppressed; use --max-rows > 0)");
    } else {
        println!("(no rows)");
    }

    println!("total_rows: {}", res.total_rows);

    if let Some(d) = res.elapsed {
        println!("elapsed_ms: {}", d.as_millis());
    }

    if let Some(path) = &opts.output {
        println!("wrote: {} ({:?})", path.display(), opts.format);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{default_table_name, render_table};
    use std::path::Path;

    #[test]
    fn render_table_aligns_columns() {
        let columns = vec!["col1".to_string(), "longer".to_string()];
        let rows = vec![
            vec!["a".to_string(), "value".to_string()],
            vec!["bb".to_string(), "x".to_string()],
        ];

        let rendered = render_table(&columns, &rows);
        let lines: Vec<&str> = rendered.lines().collect();

        assert!(!lines.is_empty());
        assert!(rendered.contains("col1"));
        assert!(rendered.contains("longer"));
    }

    #[test]
    fn default_table_name_sanitizes() {
        let name = default_table_name(Path::new("/tmp/my-table 1"));
        assert_eq!(name, "my_table_1");

        let name = default_table_name(Path::new("/tmp/123-data"));
        assert_eq!(name, "t_123_data");

        let name = default_table_name(Path::new("/tmp/.tmpabc"));
        assert_eq!(name, "t__tmpabc");

        let name = default_table_name(Path::new(""));
        assert_eq!(name, "t");
    }
}
