//! Write public BTCUSDT aggregate trades, with REST replay after reconnect or restart.
//!
//! Build and run with:
//! `cargo run -p timeseries-table-format --no-default-features --example live_binance_writer -- /tmp/btc-trades`
//! The default run makes three commits of 50 rows. Reuse the same directory to
//! resume from committed coverage. Prices and quantities retain their decimal
//! strings; the UInt64 aggregate-trade ID is the index, with granularity one.
//!
//! `--queue N` bounds the producer's event queue. `--delay-ms N` slows each
//! consumer batch, and `--disconnect-after N` abruptly closes TCP once after N
//! live trades. JSON lines report pending rows, commits, reconnects and blocked
//! sends. REST pages, WebSocket messages, Arrow batches and Parquet row groups
//! also have explicit size or row limits.
//! The queue limit is not a bound on total process memory or segment metadata.
//! Finite runs leave queued read-ahead for REST replay on the next run.
//!
//! This example owns one contiguous BTCUSDT history range. Startup rejects
//! internal coverage gaps instead of skipping them. Commits retry compatible,
//! non-overlapping concurrent appends up to three times. A terminal commit error
//! ends the run.
//! Uncommitted data is replayed after restart, subject to source availability.
//! These unauthenticated endpoints expose public market data only:
//! <https://github.com/binance/binance-spot-api-docs/blob/master/faqs/market_data_only.md>.

use std::{
    collections::HashMap,
    error::Error,
    io::{self, Write},
    net::{Shutdown, TcpStream, ToSocketAddrs},
    num::NonZeroU64,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, SyncSender, TrySendError},
    },
    thread,
    time::{Duration, Instant},
};

use arrow::{
    array::{ArrayRef, BooleanArray, StringArray, TimestampMillisecondArray, UInt64Array},
    datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit},
    record_batch::RecordBatch,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use timeseries_table_format::{
    AppendWriterOptions, IndexKind, IndexSpec, IndexValue, LogicalSchema, TableLocation, TableMeta,
    TimeSeriesTable,
};
use tungstenite::{Message, WebSocket, stream::MaybeTlsStream};

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
type Socket = WebSocket<MaybeTlsStream<TcpStream>>;
const REST: &str = "https://data-api.binance.vision/api/v3/aggTrades";
const STREAM: &str = "wss://data-stream.binance.vision:443/ws/btcusdt@aggTrade";
const BATCH_ROWS: usize = 10;

#[derive(Clone)]
struct MarketSource {
    rest: String,
    stream: String,
    timeout: Duration,
}

impl Default for MarketSource {
    fn default() -> Self {
        Self {
            rest: REST.into(),
            stream: STREAM.into(),
            timeout: Duration::from_secs(10),
        }
    }
}

#[derive(Clone)]
struct Options {
    root: PathBuf,
    commits: usize,
    rows: usize,
    queue: usize,
    delay_ms: u64,
    disconnect_after: u64,
}

impl Options {
    fn parse() -> Result<Self> {
        let mut args = std::env::args().skip(1);
        let mut options = Self {
            root: args.next().filter(|root| !root.is_empty() && !root.starts_with('-'))
                .ok_or("usage: live_binance_writer TABLE_ROOT [--commits N] [--rows N] [--queue N] [--delay-ms N] [--disconnect-after N]")?.into(),
            commits: 3, rows: 50, queue: 64, delay_ms: 0, disconnect_after: 0,
        };
        while let Some(flag) = args.next() {
            let value = args.next().ok_or("option needs a numeric value")?;
            match flag.as_str() {
                "--commits" => options.commits = value.parse()?,
                "--rows" => options.rows = value.parse()?,
                "--queue" => options.queue = value.parse()?,
                "--delay-ms" => options.delay_ms = value.parse()?,
                "--disconnect-after" => options.disconnect_after = value.parse()?,
                _ => return Err(format!("unknown option: {flag}").into()),
            }
        }
        if options.commits == 0 || options.rows == 0 || options.queue == 0 {
            return Err("commits, rows and queue must be positive".into());
        }
        Ok(options)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
struct Trade {
    #[serde(rename = "a")]
    id: u64,
    #[serde(rename = "T")]
    time: i64,
    #[serde(rename = "p")]
    price: String,
    #[serde(rename = "q")]
    quantity: String,
    #[serde(rename = "m")]
    buyer_is_maker: bool,
}

#[derive(Default, Serialize)]
struct SourceStats {
    live_rows: u64,
    replayed_rows: u64,
    reconnects: u64,
    backpressure_events: u64,
    blocked_ms: u64,
}

fn emit(value: serde_json::Value) -> Result<()> {
    let mut out = io::stdout().lock();
    serde_json::to_writer(&mut out, &value)?;
    writeln!(out)?;
    out.flush()?;
    Ok(())
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("trade_id", DataType::UInt64, false),
            Field::new(
                "trade_time",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("price", DataType::Utf8, false),
            Field::new("quantity", DataType::Utf8, false),
            Field::new("buyer_is_maker", DataType::Boolean, false),
        ],
        HashMap::from([("source".into(), "binance:BTCUSDT:aggTrade".into())]),
    ))
}

fn batch(trades: &[Trade]) -> Result<RecordBatch> {
    let columns: Vec<ArrayRef> = vec![
        Arc::new(UInt64Array::from_iter_values(trades.iter().map(|t| t.id))),
        Arc::new(TimestampMillisecondArray::from_iter_values(
            trades.iter().map(|t| t.time),
        )),
        Arc::new(StringArray::from_iter_values(
            trades.iter().map(|t| t.price.as_str()),
        )),
        Arc::new(StringArray::from_iter_values(
            trades.iter().map(|t| t.quantity.as_str()),
        )),
        Arc::new(BooleanArray::from_iter(
            trades.iter().map(|t| Some(t.buyer_is_maker)),
        )),
    ];
    Ok(RecordBatch::try_new(schema(), columns)?)
}

async fn resume_from(table: &TimeSeriesTable) -> Result<Option<u64>> {
    let mut bounds = None;
    for segment in table.state().segments.values() {
        let (IndexValue::UInt64(first), IndexValue::UInt64(last)) =
            (&segment.index_min, &segment.index_max)
        else {
            return Err("expected an unsigned trade ID index".into());
        };
        bounds = Some(match bounds {
            None => (*first, *last),
            Some((min, max)) => (u64::min(min, *first), u64::max(max, *last)),
        });
    }
    let Some((first, last)) = bounds else {
        return Ok(None);
    };
    let next = last.checked_add(1).ok_or("trade ID overflow")?;
    // This example writes a contiguous tail. Refuse holes rather than skip missing data.
    if table.max_gap_len_for_range(first, next).await? != 0 {
        return Err(
            "table has internal coverage gaps; repair them before resuming this example".into(),
        );
    }
    Ok(Some(next))
}

fn tcp(socket: &mut Socket) -> Result<&mut TcpStream> {
    match socket.get_mut() {
        MaybeTlsStream::Plain(stream) => Ok(stream),
        MaybeTlsStream::Rustls(stream) => Ok(&mut stream.sock),
        _ => Err("unsupported TLS stream".into()),
    }
}

fn connect(source: &MarketSource) -> Result<Socket> {
    let uri: tungstenite::http::Uri = source.stream.parse()?;
    let host = uri.host().ok_or("market stream URL has no host")?;
    let port = uri
        .port_u16()
        .unwrap_or(if uri.scheme_str() == Some("wss") {
            443
        } else {
            80
        });
    let mut last_error = io::Error::other("market host resolved to no addresses");
    for address in (host, port).to_socket_addrs()? {
        let stream = match TcpStream::connect_timeout(
            &address,
            source.timeout.min(Duration::from_secs(5)),
        ) {
            Ok(stream) => stream,
            Err(error) => {
                last_error = error;
                continue;
            }
        };
        stream.set_read_timeout(Some(source.timeout))?;
        stream.set_write_timeout(Some(source.timeout))?;
        let config = tungstenite::protocol::WebSocketConfig::default()
            .max_message_size(Some(64 * 1024))
            .max_frame_size(Some(64 * 1024));
        return Ok(tungstenite::client_tls_with_config(
            source.stream.as_str(),
            stream,
            Some(config),
            None,
        )?
        .0);
    }
    Err(last_error.into())
}

fn read_trade(socket: &mut Socket) -> Result<Trade> {
    loop {
        match socket.read()? {
            Message::Text(text) => {
                let value: serde_json::Value = serde_json::from_str(&text)?;
                if value["e"] != "aggTrade" || value["s"] != "BTCUSDT" {
                    return Err("unexpected market stream message; reconnecting".into());
                }
                return Ok(serde_json::from_value(value)?);
            }
            Message::Ping(_) => socket.flush()?,
            Message::Pong(_) => {}
            Message::Close(_) => return Err("market stream closed".into()),
            _ => return Err("unexpected binary market stream message".into()),
        }
    }
}

fn send_trade(sender: &SyncSender<Trade>, trade: Trade, stats: &mut SourceStats) -> Result<()> {
    match sender.try_send(trade) {
        Ok(()) => Ok(()),
        Err(TrySendError::Full(trade)) => {
            stats.backpressure_events += 1;
            let started = Instant::now();
            sender.send(trade)?;
            stats.blocked_ms += started.elapsed().as_millis() as u64;
            Ok(())
        }
        Err(TrySendError::Disconnected(_)) => Err("consumer stopped".into()),
    }
}

fn replay_until(
    source: &MarketSource,
    agent: &ureq::Agent,
    next: &mut u64,
    end: u64,
    sender: &SyncSender<Trade>,
    stats: &mut SourceStats,
    stop: &AtomicBool,
) -> Result<()> {
    let mut empty_responses = 0;
    while *next < end && !stop.load(Ordering::Relaxed) {
        let limit = (end - *next).min(1000);
        let mut response = agent
            .get(format!(
                "{}?symbol=BTCUSDT&fromId={next}&limit={limit}",
                source.rest
            ))
            .call()?;
        let trades: Vec<Trade> = response
            .body_mut()
            .with_config()
            .limit(2 * 1024 * 1024)
            .read_json()?;
        if trades.is_empty() {
            empty_responses += 1;
            if empty_responses >= 10 {
                return Err("historical API did not catch up to live stream".into());
            }
            thread::sleep(Duration::from_millis(200));
            continue;
        }
        empty_responses = 0;
        for trade in trades {
            if trade.id != *next || trade.id >= end {
                return Err(
                    format!("historical API gap: expected {next}, got {}", trade.id).into(),
                );
            }
            send_trade(sender, trade, stats)?;
            *next = next.checked_add(1).ok_or("trade ID overflow")?;
            stats.replayed_rows += 1;
            if stop.load(Ordering::Relaxed) {
                return Ok(());
            }
        }
        thread::sleep(Duration::from_millis(100));
    }
    Ok(())
}

fn produce(
    options: &Options,
    source: &MarketSource,
    mut next: Option<u64>,
    sender: SyncSender<Trade>,
    stop: Arc<AtomicBool>,
) -> Result<SourceStats> {
    let agent: ureq::Agent = ureq::Agent::config_builder()
        .timeout_global(Some(source.timeout))
        .build()
        .into();
    let mut stats = SourceStats::default();
    let mut forced_disconnect = options.disconnect_after;
    let mut failures = 0;
    while !stop.load(Ordering::Relaxed) {
        let before = stats.live_rows + stats.replayed_rows;
        let session: Result<()> = (|| {
            let mut socket = connect(source)?;
            emit(json!({"event":"connected", "resume_from":next}))?;
            while !stop.load(Ordering::Relaxed) {
                let trade = read_trade(&mut socket)?;
                let expected = next.get_or_insert(trade.id);
                if trade.id < *expected {
                    continue;
                }
                replay_until(
                    source, &agent, expected, trade.id, &sender, &mut stats, &stop,
                )?;
                if stop.load(Ordering::Relaxed) {
                    return Ok(());
                }
                send_trade(&sender, trade, &mut stats)?;
                *expected = expected.checked_add(1).ok_or("trade ID overflow")?;
                stats.live_rows += 1;
                if forced_disconnect != 0 && stats.live_rows >= forced_disconnect {
                    forced_disconnect = 0;
                    tcp(&mut socket)?.shutdown(Shutdown::Both)?;
                    return Err("injected TCP disconnect".into());
                }
            }
            Ok(())
        })();
        if stop.load(Ordering::Relaxed) {
            break;
        }
        if let Err(error) = session {
            stats.reconnects += 1;
            failures = if before == stats.live_rows + stats.replayed_rows {
                failures + 1
            } else {
                0
            };
            emit(json!({"event":"reconnect", "reason":error.to_string(), "next_id":next}))?;
            if failures >= 5 {
                return Err(error);
            }
            thread::sleep(Duration::from_secs(1));
        }
    }
    Ok(stats)
}

async fn run(options: Options, source: MarketSource) -> Result<SourceStats> {
    let location = TableLocation::local(&options.root);
    let logical = LogicalSchema::try_from_arrow_schema(schema().as_ref())?;
    let index = IndexSpec {
        column: "trade_id".into(),
        entity_columns: vec![],
        kind: IndexKind::UInt64 {
            index_granularity: NonZeroU64::MIN,
        },
    };
    let mut table = if options.root.join("_timeseries_log/CURRENT").exists() {
        TimeSeriesTable::open(location.clone()).await?
    } else {
        TimeSeriesTable::create(
            location.clone(),
            TableMeta::new_time_series_with_schema(index.clone(), logical.clone()),
        )
        .await?
    };
    if table.index_spec() != &index || table.state().table_meta.logical_schema() != Some(&logical) {
        return Err("table does not belong to this BTCUSDT example".into());
    }
    let next = resume_from(&table).await?;
    emit(
        json!({"event":"started", "resume_from":next, "version":table.state().version, "queue_capacity":options.queue}),
    )?;
    let mut writer = table.writer_with(
        schema(),
        AppendWriterOptions {
            max_rows_per_row_group: 1024,
            max_bytes_per_row_group: 1024 * 1024,
            ..Default::default()
        },
    )?;
    let (sender, receiver) = mpsc::sync_channel(options.queue);
    let stop = Arc::new(AtomicBool::new(false));
    let producer_options = options.clone();
    let producer_stop = Arc::clone(&stop);
    let producer =
        thread::spawn(move || produce(&producer_options, &source, next, sender, producer_stop));
    let outcome: Result<()> = async {
        let mut committed = 0;
        while committed < options.commits {
            let count = BATCH_ROWS.min(options.rows - usize::try_from(writer.pending_rows())?);
            let mut trades = Vec::with_capacity(count);
            for _ in 0..count { trades.push(receiver.recv_timeout(Duration::from_secs(30))?); }
            writer.write(&batch(&trades)?).await?;
            emit(json!({"event":"pending", "rows":writer.pending_rows(), "last_id":trades.last().map(|t| t.id)}))?;
            thread::sleep(Duration::from_millis(options.delay_ms));
            if writer.pending_rows() == options.rows as u64 {
                let report = writer.commit().await?.ok_or("non-empty commit returned no report")?;
                emit(json!({"event":"committed", "version":report.committed_version, "rows":report.row_count, "segment":report.segment_path}))?;
                committed += 1;
            }
        }
        Ok(())
    }.await;
    stop.store(true, Ordering::Relaxed);
    drop(receiver);
    let source_result = producer
        .join()
        .map_err(|_| io::Error::other("source thread panicked"))?;
    let stats = source_result?;
    outcome?;
    writer.abort().await?;
    let reopened = TimeSeriesTable::open(location).await?;
    let next = resume_from(&reopened).await?;
    emit(
        json!({"event":"finished", "next_id":next, "version":reopened.state().version,
        "segments":reopened.state().segments.len(), "rows":reopened.state().segments.values().map(|s|s.row_count).sum::<u64>(),
        "source":stats}),
    )?;
    Ok(stats)
}

fn main() -> Result<()> {
    let options = Options::parse()?;
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(run(options, MarketSource::default()))
        .map(|_| ())
}

#[cfg(test)]
#[path = "live_binance_writer/tests.rs"]
mod tests;
