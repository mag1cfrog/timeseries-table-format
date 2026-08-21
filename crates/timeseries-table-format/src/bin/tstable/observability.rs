use std::env::VarError;
use std::io::IsTerminal;

use tracing_subscriber::EnvFilter;

const DEFAULT_FILTER: &str = "warn";

pub(crate) fn init() -> Result<(), String> {
    let filter = match std::env::var(EnvFilter::DEFAULT_ENV) {
        Ok(value) => match EnvFilter::try_new(&value) {
            Ok(filter) => filter,
            Err(error) => {
                eprintln!(
                    "warning: invalid RUST_LOG filter {value:?}: {error}; using warning-level default"
                );
                EnvFilter::new(DEFAULT_FILTER)
            }
        },
        Err(VarError::NotPresent) => EnvFilter::new(DEFAULT_FILTER),
        Err(VarError::NotUnicode(_)) => {
            eprintln!("warning: invalid non-Unicode RUST_LOG filter; using warning-level default");
            EnvFilter::new(DEFAULT_FILTER)
        }
    };

    tracing_subscriber::fmt()
        .compact()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .with_ansi(std::io::stderr().is_terminal())
        .try_init()
        .map_err(|error| error.to_string())
}
