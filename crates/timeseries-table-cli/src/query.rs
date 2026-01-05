use std::{
    io::Write,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::Duration,
};

use tabled::{
    builder::Builder,
    settings::{Style, object::Rows, style::LineText, width::MinWidth},
};

use crate::error::{CliError, CliResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

    const PREVIEW_LABEL: &str = "Preview output";
    const PREVIEW_OFFSET: usize = 6;
    let min_width = PREVIEW_OFFSET + PREVIEW_LABEL.len() + 4;

    let mut builder = Builder::default();
    builder.push_record(columns);
    for row in rows {
        builder.push_record(row);
    }

    let mut table = builder.build();

    table.with(Style::rounded());
    table.with(MinWidth::new(min_width));
    table.with(LineText::new(PREVIEW_LABEL, Rows::first()).offset(PREVIEW_OFFSET));
    // LineText re-estimates dimensions, so re-apply MinWidth afterwards.
    table.with(MinWidth::new(min_width));
    table.to_string()
}

pub fn print_query_result(res: &QueryResult, opts: &QueryOpts) -> CliResult<()> {
    let mut stdout = std::io::stdout();
    write_query_result(res, opts, &mut stdout)
}

pub fn render_preview(res: &QueryResult, opts: &QueryOpts) -> Option<String> {
    if !res.preview_rows.is_empty() {
        return Some(render_table(&res.columns, &res.preview_rows));
    }

    if opts.max_rows == 0 && !res.columns.is_empty() {
        return Some(render_table(&res.columns, &[]));
    }

    None
}

pub fn preview_message(res: &QueryResult, opts: &QueryOpts) -> Option<String> {
    if opts.max_rows == 0 && res.total_rows > 0 {
        return Some("(preview suppressed; use --max-rows > 0)".to_string());
    }

    if res.total_rows == 0 {
        return Some("(no rows)".to_string());
    }

    None
}

pub fn write_query_summary<W: Write>(
    res: &QueryResult,
    opts: &QueryOpts,
    out: &mut W,
) -> CliResult<()> {
    let mut write_err = |e: std::io::Error| CliError::PathInvariantNoSource {
        message: format!("failed to write output: {e}"),
        path: None,
    };

    writeln!(out, "total_rows: {}", res.total_rows).map_err(&mut write_err)?;

    if let Some(d) = res.elapsed {
        writeln!(out, "elapsed_ms: {}", d.as_millis()).map_err(&mut write_err)?;
    }

    if let Some(path) = &opts.output {
        writeln!(out, "wrote: {} ({:?})", path.display(), opts.format)
            .map_err(&mut write_err)?;
    }

    Ok(())
}

pub fn write_query_result<W: Write>(
    res: &QueryResult,
    opts: &QueryOpts,
    out: &mut W,
) -> CliResult<()> {
    let mut write_err = |e: std::io::Error| CliError::PathInvariantNoSource {
        message: format!("failed to write output: {e}"),
        path: None,
    };

    if let Some(rendered) = render_preview(res, opts) {
        writeln!(out, "{rendered}").map_err(&mut write_err)?;
    }

    if let Some(message) = preview_message(res, opts) {
        writeln!(out, "{message}").map_err(&mut write_err)?;
    }

    write_query_summary(res, opts, out)?;
    Ok(())
}

pub fn page_output(text: &str) -> CliResult<()> {
    let spawn_err = |e: std::io::Error| CliError::PathInvariantNoSource {
        message: format!("failed to spawn pager: {e}"),
        path: None,
    };
    let io_err = |e: std::io::Error| CliError::PathInvariantNoSource {
        message: format!("failed to write pager output: {e}"),
        path: None,
    };

    let mut child = match Command::new("less").arg("-S").stdin(Stdio::piped()).spawn() {
        Ok(child) => child,
        Err(_) => {
            let mut stdout = std::io::stdout();
            stdout.write_all(text.as_bytes()).map_err(io_err)?;
            return Ok(());
        }
    };

    if let Some(stdin) = child.stdin.as_mut() {
        stdin.write_all(text.as_bytes()).map_err(io_err)?;
    }

    child.wait().map_err(spawn_err)?;
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
    fn render_table_includes_preview_label_for_narrow_tables() {
        let columns = vec!["x".to_string()];
        let rows = vec![vec!["1".to_string()]];

        let rendered = render_table(&columns, &rows);
        assert!(rendered.contains("Preview output"));
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
