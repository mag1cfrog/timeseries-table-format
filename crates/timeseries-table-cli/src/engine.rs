use std::future::Future;

#[allow(dead_code)]
/// Minimal engine trait.
pub trait Engine: Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    type ExecuteFut<'a>: Future<Output = Result<(), Self::Error>> + Send + 'a
    where
        Self: 'a;

    fn execute<'a>(&'a self, sql: &'a str) -> Self::ExecuteFut<'a>;
}
