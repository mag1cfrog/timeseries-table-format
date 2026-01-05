use std::{
    path::{Path, PathBuf},
    time::Duration,
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

/// Pick a stable, user-friendly SQL table name from the table root path.
/// Fallback is "t".
pub fn default_table_name(table_root: &Path) -> String {
    table_root
        .file_name()
        .map(|s| s.to_string_lossy().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "t".to_string())
}

fn render_table(columns: &[String], rows: &[Vec<String>]) -> String {
    let mut widths: Vec<usize> = columns.iter().map(|c| c.len()).collect();

    for row in rows {
        for (idx, cell) in row.iter().enumerate() {
            if idx >= widths.len() {
                widths.push(cell.len());
            } else if cell.len() > widths[idx] {
                widths[idx] = cell.len();
            }
        }
    }

    let mut out = String::new();

    let write_row = |out: &mut String, row: &[String]| {
        for (idx, width) in widths.iter().enumerate() {
            let cell = row.get(idx).map(String::as_str).unwrap_or("");
            let padding = width.saturating_sub(cell.len());
            out.push_str(cell);
            if padding > 0 {
                out.push_str(&" ".repeat(padding));
            }
            if idx + 1 < widths.len() {
                out.push_str(" | ");
            }
        }
    };

    write_row(&mut out, columns);
    out.push('\n');

    for (idx, width) in widths.iter().enumerate() {
        out.push_str(&"-".repeat(*width));
        if idx + 1 < widths.len() {
            out.push_str("-+-");
        }
    }

    for row in rows {
        out.push('\n');
        write_row(&mut out, row);
    }

    out
}

pub fn print_query_result(res: &QueryResult, opts: &QueryOpts) -> CliResult<()> {
    if !res.preview_rows.is_empty() {
        let rendered = render_table(&res.columns, &res.preview_rows);
        println!("{rendered}");
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
    use super::render_table;

    #[test]
    fn render_table_aligns_columns() {
        let columns = vec!["col1".to_string(), "longer".to_string()];
        let rows = vec![
            vec!["a".to_string(), "value".to_string()],
            vec!["bb".to_string(), "x".to_string()],
        ];

        let rendered = render_table(&columns, &rows);
        let lines: Vec<&str> = rendered.lines().collect();

        assert!(lines[0].contains("col1"));
        assert!(lines[0].contains("longer"));
        assert!(lines[1].contains("-+-"));
        assert!(lines[2].contains("a"));
        assert!(lines[3].contains("bb"));
    }
}
